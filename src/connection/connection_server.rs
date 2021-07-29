use std::{io::Cursor, net::SocketAddr, sync::{Arc, atomic::{AtomicBool, Ordering}}};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}}, sync::mpsc};

use crate::iproto::{request::Request, response::Response};

use super::{RespChans, connector::Connector};



pub(crate) struct ConnectionServer {
  pub(crate) connector: Connector,

  pub(crate) req_chan_reader: mpsc::Receiver<Request>,

  pub(crate) resp_chans: RespChans,

  pub(crate) closed: Arc<AtomicBool>,
}

impl ConnectionServer {
  pub(crate) async fn serve_loop(mut self, stream: TcpStream) {
    let mut stream = Some(stream);

    while !self.closed.load(Ordering::SeqCst) {
      if let Err(err) = self.serve(stream).await {
        if let Some(interval) = self.connector.reconnect_interval {
          log::error!(
            "[{}] reconnecting in {:?} on error while serving connection: {}",
            &self.connector.addr, interval, err,
          );
          tokio::time::sleep(interval).await;
        } else {
          log::error!(
            "[{}] reconnecting on error while serving connection: {}",
            &self.connector.addr, err,
          );
        }
      }
      stream = None;
    }
  }

  async fn serve(&mut self, stream: Option<TcpStream>) -> Result<(), std::io::Error> {
    let stream = match stream {
      Some(s) => s,
      None => self.connector.new_connection()
          .await.map(|(s, _)| s)?,
    };

    let (read_stream, write_stream) = stream.into_split();

    let reader_fut = Self::reader(
      self.connector.addr, read_stream,
      self.resp_chans.clone(), self.closed.clone());
    let writer_fut = self.writer(write_stream);

    tokio::select! {
      res = reader_fut => res,
      res = writer_fut => res,
    }
  }

  async fn writer(&mut self, mut write: OwnedWriteHalf) -> Result<(), std::io::Error> {
    log::debug!("[{}] writer start", &self.connector.addr);

    #[allow(unused_variables)]
    let on_exit = OnExit(self.connector.addr, "writer");

    let mut write_buf: Vec<u8> = Vec::new();

    while !self.closed.load(Ordering::SeqCst) {
      write_buf.clear();

      let req: Request = match self.req_chan_reader.recv().await {
        Some(req) => req,
        None => {
          log::debug!(
            "[{}] request channel closed, seems Connection dropped",
            &self.connector.addr,
          );
          return Ok(())
        },
      };

      if let Err(err) = req.pack(&mut write_buf) {
        log::error!(
          "[{}] error while packing request err: {}, req: {:?}",
          &self.connector.addr, err, req,
        );
        continue;
      }

      match self.connector.send_request_timeout {
        Some(timeout) => {
          tokio::time::timeout(
            timeout, write.write_all(&write_buf),
          ).await??;
        },
        None => { write.write_all(&write_buf).await?; }
      }
    }

    Ok(())
  }

  async fn reader(
    addr: SocketAddr,
    mut read: OwnedReadHalf,
    resp_chans: RespChans,
    closed: Arc<AtomicBool>,
  ) -> Result<(), std::io::Error> {
    log::debug!("[{}] reader start", addr);

    #[allow(unused_variables)]
    let on_exit = OnExit(addr, "reader");

    const REQUEST_LEN_LEN: usize = 9;
    let mut buf = [0; REQUEST_LEN_LEN];
    let mut req_buf: Vec<u8> = Vec::new();

    { // drop closed channels after reconnect, preventing memory leak
      let closed_resp_chans: Vec<_> = resp_chans.iter()
        .filter(|v| v.is_closed())
        .map(|pair| *pair.key())
        .collect();

      closed_resp_chans.iter()
        .for_each(|sync| { resp_chans.remove(sync); });
    }

    while !closed.load(Ordering::SeqCst) {
      req_buf.clear();

      read.read_exact(&mut buf).await?;

      let mut cur = Cursor::new(&buf);

      let size = match rmp::decode::read_int::<u64, _>(&mut cur) {
        Ok(size) => size,
        Err(err) => {
          return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("error while parsing request size: {:?}", err),
          ));
        },
      };

      let required_buf_size = size as usize + cur.position() as usize;

      req_buf.extend_from_slice(&buf);
      req_buf.resize(required_buf_size, 0);

      read.read_exact(&mut req_buf[REQUEST_LEN_LEN..]).await?;

      let mut req_cur = Cursor::new(&req_buf);

      let resp = match Response::parse(&mut req_cur) {
        Ok(resp) => resp,
        Err(err) => {
          log::error!(
            "[{}] error while parsing response header: {}, resp: {:?}",
            addr, err, &req_buf,
          );
          continue;
        },
      };

      if let Some((_, resp_chan)) = resp_chans.remove(&resp.header.sync) {
        if resp_chan.is_closed() {
          log::debug!(
            "[{}] can't find resp channel for {}",
            addr, resp.header.sync,
          );
          continue;
        }
        if let Err(resp) = resp_chan.send(resp) {
          log::debug!(
            "[{}] resp channel closed for {}",
            addr, resp.header.sync,
          );
        }
      }
    }

    Ok(())
  }
}

struct OnExit(SocketAddr, &'static str);

impl Drop for OnExit {
  fn drop(&mut self) {
    log::debug!("[{}] {} closed", self.0, self.1);
  }
}
