use std::{
  io::Cursor,
  sync::{Arc, atomic::{AtomicBool, Ordering}},
};

use tokio::{
  io::{AsyncReadExt, AsyncWriteExt},
  net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}},
  sync::mpsc,
};

use crate::iproto::{request::Request, response::Response};

use super::{connection::RespChans, connector::Connector};



pub(crate) struct ConnectionServer {
  pub(crate) connector: Connector,

  pub(crate) req_chan_reader: mpsc::Receiver<Request>,

  pub(crate) resp_chans: RespChans,

  pub(crate) closed: Arc<AtomicBool>,
}

impl ConnectionServer {
  pub(crate) async fn serve_cycle(mut self, stream: TcpStream) {
    let mut stream = Some(stream);

    while !self.closed.load(Ordering::SeqCst) {
      if let Err(err) = self.serve(stream).await {
        println!("retrying on error while serving connection: {}", err);
      }
      stream = None;
    }
  }

  async fn serve(&mut self, stream: Option<TcpStream>) -> Result<(), std::io::Error> {
    let stream = match stream {
      Some(s) => s,
      None => self.connector.new_connection().await?,
    };

    let (read_stream, write_stream) = stream.into_split();

    let reader_fut = Self::reader(read_stream, self.resp_chans.clone(), self.closed.clone());
    let writer_fut = self.writer(write_stream);

    tokio::select! {
      _ = reader_fut => return Ok(()),
      _ = writer_fut => return Ok(()),
    }
  }

  async fn writer(&mut self, mut write: OwnedWriteHalf) {
    let mut write_buf: Vec<u8> = Vec::new();

    while !self.closed.load(Ordering::SeqCst) {
      write_buf.resize(0, 0);

      let req: Request = match self.req_chan_reader.recv().await {
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

  async fn reader(
    mut read: OwnedReadHalf,
    resp_chans: RespChans,
    closed: Arc<AtomicBool>,
  ) {
    let mut buf = [0; 9];
    let mut req_buf: Vec<u8> = Vec::new();

    while !closed.load(Ordering::SeqCst) {
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

      if let Some((_, resp_chan)) = resp_chans.remove(&resp.header.sync) {
        if resp_chan.is_closed() { continue; }
        let _ = resp_chan.send(resp);
      }
    }
  }
}
