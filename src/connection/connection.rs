use std::{
  collections::HashMap,
  net::SocketAddr,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
};

use serde::de::DeserializeOwned;
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::iproto::{
  request::{self, Call, Request, Select},
  response::{ErrorBody, Response, TupleBody},
  types::Error,
};

use super::connector::Connector;



#[derive(Debug)]
pub struct Connection {
  pub(crate) sync: AtomicU64,
  pub(crate) req_chan_sender: mpsc::Sender<Request>,
  pub(crate) resp_chans: Arc<Mutex<
    HashMap<u64, oneshot::Sender<Response>>
  >>,
  pub(crate) closed: Arc<AtomicBool>,
}

impl Connection {
  pub fn new(addr: SocketAddr) -> Connector {
    Connector {
      addr, credentials: None,
      connect_timeout: None,
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

  pub fn close(&self) {
    self.closed.store(true, Ordering::SeqCst)
  }
}

impl Drop for Connection {
  fn drop(&mut self) { self.close(); }
}

fn unpack_error(resp: Response) -> Error {
  match resp.unpack_body::<ErrorBody>() {
    Ok(err) => Error::TarantoolError(resp.header.code, err),
    Err(err) => err,
  }
}
