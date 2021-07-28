use std::{
  collections::HashMap,
  net::SocketAddr,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
};

use dashmap::DashMap;
use serde::de::DeserializeOwned;
use tokio::sync::{mpsc, oneshot};

use crate::iproto::{request::{self, Call, Delete, Eval, Execute, Insert, Prepare, Replace, Request, Select, Update, Upsert}, response::{
    ErrorBody, Response, SQLBody,
    SQLResponse, TupleBody,
  }, types::Error};

use super::connector::Connector;

macro_rules! request_method {
  ($func:ident, $body:ident) => {
    pub async fn $func<T>(&self, body: $body) -> Result<T, Error>
      where T: DeserializeOwned
    {
      let req = request::$func(body);

      let resp: Response = self.make_request(req).await;

      match resp.header.code.is_err() {
        false => resp.unpack_body::<TupleBody<T>>(),
        true => match resp.unpack_body::<ErrorBody>() {
          Ok(err) => Err(Error::TarantoolError(resp.header.code, err)),
          Err(err) => Err(err),
        },
      }
    }
  };
}

macro_rules! request_sql_method {
  ($func:ident, $body:ident) => {
    pub async fn $func(&self, body: $body) -> Result<SQLResponse, Error> {
      let req = request::$func(body);

      let resp: Response = self.make_request(req).await;

      match resp.header.code.is_err() {
        false => resp.unpack_body::<SQLBody>(),
        true => match resp.unpack_body::<ErrorBody>() {
          Ok(err) => Err(Error::TarantoolError(resp.header.code, err)),
          Err(err) => Err(err),
        },
      }
    }
  };
}

pub(crate) type RespChans = Arc<DashMap<u64, oneshot::Sender<Response>>>;

#[derive(Debug)]
pub struct Connection {
  pub(crate) sync: AtomicU64,
  pub(crate) req_chan_sender: mpsc::Sender<Request>,
  pub(crate) resp_chans: RespChans,
  pub(crate) closed: Arc<AtomicBool>,
}

impl Connection {
  pub fn new(addr: SocketAddr) -> Connector {
    Connector {
      addr, credentials: None,
      connect_timeout: None,
    }
  }

  pub fn close(&self) {
    self.closed.store(true, Ordering::SeqCst)
  }

  request_method!(select, Select);
  request_method!(call, Call);
  request_method!(insert, Insert);
  request_method!(replace, Replace);
  request_method!(update, Update);
  request_method!(delete, Delete);
  request_method!(eval, Eval);

  request_sql_method!(prepare, Prepare);
  request_sql_method!(execute, Execute);

  pub async fn upsert(&self, body: Upsert) -> Result<(), Error> {
    let req = request::upsert(body);

    let resp: Response = self.make_request(req).await;

    match resp.header.code.is_err() {
      false => Ok(()),
      true => match resp.unpack_body::<ErrorBody>() {
        Ok(err) => Err(Error::TarantoolError(resp.header.code, err)),
        Err(err) => Err(err),
      },
    }
  }

  pub async fn ping(&self) -> Result<(), Error> {
    let req = request::ping();

    let resp: Response = self.make_request(req).await;

    match resp.header.code.is_err() {
      false => Ok(()),
      true => match resp.unpack_body::<ErrorBody>() {
        Ok(err) => Err(Error::TarantoolError(resp.header.code, err)),
        Err(err) => Err(err),
      },
    }
  }

  fn new_sync(&self) -> u64 {
    self.sync.fetch_add(1, Ordering::SeqCst)
  }

  async fn make_request(&self, mut req: Request) -> Response {
    if self.closed.load(Ordering::SeqCst) {
      panic!("request to closed connection");
    }

    let (sender, receiver) = oneshot::channel::<Response>();
    req.header.sync = self.new_sync();

    if let Some(_) = self.resp_chans.insert(req.header.sync, sender) {
      println!("sync seems to be overflowed with {}", req.header.sync);
    }

    let _ = self.req_chan_sender.send(req).await;

    receiver.await.unwrap()
  }
}

impl Drop for Connection {
  fn drop(&mut self) { self.close(); }
}
