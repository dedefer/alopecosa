use std::{collections::HashMap, io::Cursor, net::SocketAddr, str, sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}}, time::Duration};

use base64::decode;
use serde::de::DeserializeOwned;
use sha1::{Digest, Sha1, digest::FixedOutput};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpSocket, TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}}, sync::{Mutex, mpsc, oneshot}};

use crate::iproto::{request::{self, Auth, Call, Request, Select}, response::{ErrorBody, Response, TupleBody}, types::Error};

#[derive(Debug)]
pub struct Connector {
  addr: SocketAddr,
  connect_timeout: Option<tokio::time::Duration>,
  credentials: Option<(String, String)>,
}

impl Connector {
  pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
    self.connect_timeout = Some(timeout);
    self
  }

  pub fn with_auth(mut self, user: String, password: String) -> Self {
    self.credentials = Some((user, password));
    self
  }

  pub async fn connect(self) -> Result<Arc<Connection>, tokio::io::Error> {
    let stream = self.new_connection().await?;

    let (sender, reader) = mpsc::channel(1000);

    let conn = Arc::new(Connection {
        connector: self,

        req_chan_sender: sender,
        req_chan_reader: reader.into(),
        resp_chans: HashMap::new().into(),

        current_sync: 1.into(),
        closed: false.into(),
    });

    let serve_conn = conn.clone();

    tokio::spawn(async move {
      let mut stream = Some(stream);

      while !serve_conn.closed.load(Ordering::SeqCst) {
        if let Err(err) = Connection::serve(serve_conn.clone(), stream).await {
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

    if let Some((user, password)) = &self.credentials {

      let salt = match decode(&start_buf[64..(64 + 44)]) {
        Ok(salt) => salt,
        Err(_) => Err(std::io::Error::new(
          std::io::ErrorKind::InvalidData,
          "bad salt",
        ))?,
      };

      let mut buf: Vec<u8> = Vec::new();
      request::auth(Auth {
        user: user.clone(),
        scramble: self.auth_scramble(&salt, password),
      }).pack(&mut buf)
        .map_err(|_| std::io::Error::new(
          std::io::ErrorKind::Other,
          "auth pack error",
        ))?;

      conn.write_all(&buf).await?;

      buf.resize(128, 0);

      let readed = conn.read(&mut buf).await?;

      let req = Response::parse(&buf[0..readed])
        .map_err(|_| std::io::Error::new(
          std::io::ErrorKind::Other,
          "auth pack error",
        ))?;

      if req.header.sync != 0 || req.header.code.is_err() {
        return Err(std::io::Error::new(
          std::io::ErrorKind::InvalidInput,
          "auth error",
        ));
      }
    }

    Ok(conn)
  }

  fn auth_scramble(&self, salt: &[u8], password: &str) -> Vec<u8> {

    let mut hasher = Sha1::default();
    Digest::update(&mut hasher, password);
    let hash1 = hasher.finalize_fixed();

    let mut hasher = Sha1::default();
    Digest::update(&mut hasher, &hash1[0..20]);
    let hash2 = hasher.finalize();

    let mut hasher = Sha1::default();
    Digest::update(&mut hasher, &salt[0..20]);
    Digest::update(&mut hasher, &hash2[0..20]);
    let almost_final = hasher.finalize();

    almost_final.iter()
      .zip(&hash1[0..20])
      .map(|(&a, &b)| { a ^ b }).collect()
  }
}

#[derive(Debug)]
pub struct Connection {
  connector: Connector,

  current_sync: AtomicU64,

  req_chan_sender: mpsc::Sender<Request>,
  req_chan_reader: Mutex<mpsc::Receiver<Request>>,

  resp_chans: Mutex<
    HashMap<u64, oneshot::Sender<Response>>
  >,

  closed: AtomicBool,
}

impl Connection {
  pub fn new(addr: SocketAddr) -> Connector {
    Connector {
      addr, credentials: None,
      connect_timeout: None,
    }
  }

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

  async fn serve(conn: Arc<Connection>, stream: Option<TcpStream>) -> Result<(), std::io::Error> {
    let stream = match stream {
      Some(s) => s,
      None => conn.connector.new_connection().await?,
    };

    let (read_stream, write_stream) = stream.into_split();

    let reader_conn = conn.clone();

    let reader_job = tokio::spawn(async move { reader_conn.reader(read_stream).await });
    conn.writer(write_stream).await;

    let _ = reader_job.await;

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

  fn close(&self) {
    self.closed.store(false, Ordering::SeqCst)
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
      .with_auth("em".into(), "em".into())
      .connect().await.unwrap();

    // let res: Vec<(u32, u32, u32)> = conn.select(Select {
    //     space_id: 512, index_id: 0,
    //     limit: 100, offset: 0,
    //     iterator: Iterator::Eq,
    //     keys: vec![ Value::UInt(1) ],
    // }).await.expect("bad query");

    let res: (u32, u32) = conn.call(Call {
      function: "test".into(),
      args: vec![ Value::UInt(1) ],
  }).await.expect("bad query");

    dbg!(res);
  }

  // #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
  // async fn my_test2() {
  //   let addr = "127.0.0.1:3301".parse().unwrap();

  //   let conn = Connection::new(addr)
  //     .connect().await.unwrap();

  //   let conn2 = conn.clone();
  //   let conn3 = conn.clone();

  //   let kk = tokio::spawn(async move {
  //     for i in 0..100_000u32 {
  //       let res: (u32, u32) = conn.call(Call {
  //         function: "test".into(),
  //         args: vec![ Value::UInt(i as u64) ],
  //       }).await.expect("all ok");

  //       assert_eq!(res, (i, i+1));
  //     }
  //   });

  //   let kk2 = tokio::spawn(async move {
  //     let req  = Select {
  //       space_id: 512, index_id: 0,
  //       limit: 100, offset: 0,
  //       iterator: Iterator::Eq,
  //       keys: vec![ Value::UInt(1) ],
  //     };

  //     for _ in 0..100_000u32 {
  //       let res: Vec<(u32, u32, u32)> = conn2.select(req.clone())
  //         .await.expect("all ok");
  //       assert_eq!(res[0], (1, 2, 3));
  //     }
  //   });

  //   let kk3 = tokio::spawn(async move {
  //     let req  = Select {
  //       space_id: 512, index_id: 0,
  //       limit: 100, offset: 0,
  //       iterator: Iterator::Eq,
  //       keys: vec![ Value::UInt(1) ],
  //     };

  //     for _ in 0..100_000u32 {
  //       let res: Vec<(u32, u32, u32)> = conn3.select(req.clone())
  //         .await.expect("all ok");
  //       assert_eq!(res[0], (1, 2, 3));
  //     }
  //   });


  //   let _ = tokio::join!(kk, kk2, kk3);
  // }

}
