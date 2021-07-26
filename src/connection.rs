use std::{collections::HashMap, io::Cursor, net::SocketAddr, str, sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}}, time::Duration};

use serde::de::DeserializeOwned;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpSocket, TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}}, sync::{Mutex, mpsc, oneshot}};

use crate::iproto::{request::{self, Call, Request, Select}, response::{ErrorBody, Response, TupleBody}, types::Error};



#[derive(Debug)]
pub struct Connection {
  addr: SocketAddr,
  connect_timeout: Option<tokio::time::Duration>,

  current_sync: AtomicU64,

  req_chan_sender: mpsc::Sender<Request>,
  req_chan_reader: Mutex<mpsc::Receiver<Request>>,

  resp_chans: Mutex<
    HashMap<u64, oneshot::Sender<Response>>
  >,

  closed: AtomicBool,
}

impl Connection {
  pub async fn make_request(&self, req: Request) -> Response {
    let mut req = req;
    req.header.sync = self.get_sync();

    let (sender, receiver) = oneshot::channel::<Response>();

    if let Some(_) = self.resp_chans.lock().await
      .insert(req.header.sync, sender) {
      println!("sync seems to be overflowed with {}", req.header.sync);
    }

    let _ = self.req_chan_sender.send(req).await;

    receiver.await.unwrap()
  }

  pub async fn select<T>(&self, body: Select) -> Result<T, Error>
    where T: DeserializeOwned
  {
    let req = request::select(body);

    let resp: Response = self.make_request(req).await;

    match resp.header.code.is_err() {
      false => resp.unpack_body::<TupleBody<T>>(),
      true => Err(unpack_error(resp)),
    }
  }

  pub async fn call<T>(&self, body: Call) -> Result<T, Error>
    where T: DeserializeOwned
  {
    let req = request::call(body);

    let resp: Response = self.make_request(req).await;

    match resp.header.code.is_err() {
      false => resp.unpack_body::<TupleBody<T>>(),
      true => Err(unpack_error(resp)),
    }
  }

  pub fn new(addr: SocketAddr) -> Connection {
    let (sender, reader) = mpsc::channel(1000);

    let g = LogOnExit("new connection done".into());

    Connection {
      addr, connect_timeout: None,

      req_chan_sender: sender,
      req_chan_reader: reader.into(),
      resp_chans: HashMap::new().into(),

      current_sync: 0.into(),
      closed: false.into(),
    }
  }

  pub fn with_connect_timeout(mut self, timeout: Duration) -> Connection {
    self.connect_timeout = Some(timeout);
    self
  }

  pub async fn connect(self) -> Result<Arc<Connection>, tokio::io::Error> {
    let conn = Arc::new(self);
    let mut serve_conn = conn.clone();

    let stream = conn.new_connection().await?;

    tokio::spawn(async move {
      let mut stream = Some(stream);

      while !serve_conn.closed.load(Ordering::SeqCst) {
        if let Err(err) = Self::serve_connection(serve_conn.clone(), stream).await {
          println!("retrying on error while serving connection: {}", err);
        }
        stream = None;
      }
    });

    Ok(conn)
  }

  async fn new_connection(&self) -> Result<TcpStream, std::io::Error> {
    let sock = match self.addr.is_ipv4() {
      true => TcpSocket::new_v4(),
      false => TcpSocket::new_v6(),
    }?;

    let mut conn: TcpStream = match self.connect_timeout {
      None => sock.connect(self.addr).await?,
      Some(timeout) => match tokio::time::timeout(
        timeout, sock.connect(self.addr)
      ).await {
        Ok(Ok(conn)) => conn,
        Ok(Err(e)) => return Err(e.into()),
        Err(_) => return Err(std::io::Error::new(
          std::io::ErrorKind::TimedOut,
          "connect timeout",
        )),
      },
    };

    let mut start_buf: [u8; 128] = [0; 128];

    conn.read_exact(&mut start_buf).await?;

    println!("start message: {}", str::from_utf8(&start_buf).expect("bad buf"));

    Ok(conn)
  }

  async fn serve_connection(mut conn: Arc<Connection>, stream: Option<TcpStream>) -> Result<(), std::io::Error> {
    let stream = match stream {
      Some(s) => s,
      None => conn.new_connection().await?,
    };

    let (read_stream, write_stream) = stream.into_split();

    let reader_conn = conn.clone();

    let reader_job = tokio::spawn(async move { reader_conn.reader(read_stream).await });
    conn.writer(write_stream).await;

    reader_job.await;

    Ok(())
  }

  fn get_sync(&self) -> u64 {
    self.current_sync
      .fetch_add(1, Ordering::SeqCst)
  }

  async fn writer(&self, write: OwnedWriteHalf) {
    let mut write = write;
    let mut write_buf: Vec<u8> = Vec::new();
    let mut req_chan = self.req_chan_reader.lock().await;

    println!("writer start");
    let guard = LogOnExit("writer exited".into());

    while !self.closed.load(Ordering::SeqCst) {
      write_buf.resize(0, 0);

      let req: Request = match req_chan.recv().await {
        Some(req) => req,
        None => return,
      };

      if let Err(err) = req.pack(&mut write_buf) {
        println!("error while packing request err: {}, req: {:?}", err, req);
        continue;
      }

      if let Err(err) = write.write_all(&write_buf).await {
        println!("error while writing request: {}", err);
        return;
      }
    }
  }

  fn close(&self) {
    self.closed.store(false, Ordering::SeqCst)
  }

  async fn reader(&self, read: OwnedReadHalf) {
    let mut read = read;
    let mut buf = [0; 9];
    let mut req_buf: Vec<u8> = Vec::new();

    while !self.closed.load(Ordering::SeqCst) {
      req_buf.resize(0, 0);

      if let Err(err) = read.read_exact(&mut buf).await {
        println!("reconnecting on error while reading: {}", err);
        return;
      }

      let mut cur = Cursor::new(&buf);

      let size = match rmp::decode::read_int::<u64, _>(&mut cur) {
        Ok(size) => size,
        Err(err) => {
          println!("error while parsing request size: {}", err);
          return;
        },
      };

      let required_buf_size = size as usize + cur.position() as usize;

      req_buf.extend_from_slice(&buf);
      req_buf.resize(required_buf_size, 0);

      if let Err(err) = read.read_exact(&mut req_buf[9..]).await {
        println!("error while reading: {}", err);
        return;
      }

      let mut req_cur = Cursor::new(&req_buf);

      let resp = match Response::parse(&mut req_cur) {
        Ok(resp) => resp,
        Err(err) => {
          println!("error while parsing response header: {}", err);
          continue;
        },
      };

      if let Some(resp_chan) = self.resp_chans.lock().await.remove(&resp.header.sync) {
        if resp_chan.is_closed() { continue; }
        let _ = resp_chan.send(resp);
      }
    }
  }
}

impl Drop for Connection {
  fn drop(&mut self) {
    self.close();
    println!("conn closed");
  }
}

fn unpack_error(resp: Response) -> Error {
  match resp.unpack_body::<ErrorBody>() {
    Ok(err) => Error::TarantoolError(resp.header.code, err),
    Err(err) => err,
  }
}

struct LogOnExit(String);

impl Drop for LogOnExit {
    fn drop(&mut self) {
        println!("{}", self.0)
    }
}


#[cfg(test)]
mod tests {
  use crate::iproto::{constants::Iterator, request::Value};

use super::*;

  #[tokio::test]
  async fn my_test() {
    let addr = "127.0.0.1:3301".parse().unwrap();

    let conn = Connection::new(addr)
      .connect().await.unwrap();

    let res: Vec<(u32, u32, u32)> = conn.select(Select {
        space_id: 512, index_id: 0,
        limit: 100, offset: 0,
        iterator: Iterator::Eq,
        keys: vec![ Value::UInt(1) ],
    }).await.expect("bad query");

    dbg!(res);
  }

}
