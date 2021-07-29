pub mod connector;
mod connection_server;

use std::sync::{
  Arc, atomic::{AtomicBool, AtomicU64, Ordering},
};

use dashmap::DashMap;
use serde::de::DeserializeOwned;
use tokio::sync::{mpsc, oneshot};

use crate::iproto::{
  request::{
    self, Call, Delete, Eval, Execute, Insert, Prepare,
    Replace, Request, Select, Update, Upsert,
  },
  response::{
    ErrorBody, Response,
    SQLBody, SQLBodyDecoder,
    TupleBody,
  },
  types::Error,
};

macro_rules! request_method {
  ($func:ident, $body:ident) => {
    #[allow(dead_code)]
    pub async fn $func<T>(&self, body: $body) -> Result<T, Error>
      where T: DeserializeOwned
    {
      let req = request::$func(body);

      let resp: Response = self.perform(req).await?;

      resp.unpack_body::<TupleBody<T>>()
    }
  };
}

macro_rules! request_sql_method {
  ($func:ident, $body:ident) => {
    #[allow(dead_code)]
    pub async fn $func(&self, body: $body) -> Result<SQLBody, Error> {
      let req = request::$func(body);

      let resp: Response = self.perform(req).await?;

      resp.unpack_body::<SQLBodyDecoder>()
    }
  };
}

pub(crate) type RespChans = Arc<DashMap<u64, oneshot::Sender<Response>>>;

/**
  This is user part of connection,
  allows you to perform requests to tarantool.

  You usualy will work with Arc<Connection>, so it is async-safe.

  Example:
  ```rust
    let conn: Arc<Connection> = Connector::new("127.0.0.1:3301".parse().unwrap())
      .connect().await.unwrap();

    let eval_resp: (u32, u32) = conn.eval(Eval {
      expr: "return 1, 2".into(),
      args: ().into_tuple(),
    }).await.unwrap();

    let select_resp: Vec<(u32, u32, u32)> = conn.select(Select {
        space_id: 512, index_id: 0,
        limit: 100, offset: 0,
        iterator: Iterator::Ge,
        keys: ( 1u64, ).into_tuple(),
    }).await.unwrap();

    let (resp_with_timeout,): (i32,) = tokio::time::timeout(
      Duration::from_secs(1),
      conn.call(Call {
        function: "echo".into(),
        args: ( 123, ).into_tuple(),
      })
    ).await.unwrap().unwrap();
  ```
*/
#[derive(Debug)]
pub struct Connection {
  pub(crate) version: String,
  pub(crate) sync: AtomicU64,
  pub(crate) req_chan_sender: mpsc::Sender<Request>,
  pub(crate) resp_chans: RespChans,
  pub(crate) closed: Arc<AtomicBool>,
}

#[allow(dead_code)]
impl Connection {
  /**
    version of connected tarantool

    Note: if tarantool was updated while reconnectng
    this field be outdated
  */
  pub fn tarantool_version(&self) -> &str {
    &self.version
  }

  /**
    schedules connection for close

    Note: this method is called on drop
  */
  pub fn close(&self) {
    self.closed.store(true, Ordering::SeqCst)
  }

  pub async fn perform(&self, req: Request) -> Result<Response, Error> {
    let resp: Response = self.make_request(req).await;

    match resp.header.code.is_err() {
      false => Ok(resp),
      true => match resp.unpack_body::<ErrorBody>() {
        Ok(err) => Err(Error::TarantoolError(resp.header.code, err)),
        Err(err) => Err(err),
      },
    }
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

    if self.resp_chans.insert(req.header.sync, sender).is_some() {
      log::error!("sync seems to be overflowed with {}", req.header.sync);
    }

    let _ = self.req_chan_sender.send(req).await;

    receiver.await.unwrap()
  }
}

impl Drop for Connection {
  fn drop(&mut self) { self.close(); }
}



#[cfg(test)]
mod tests {
  use std::{sync::Arc, time::Duration};

  use crate::{Connector, IntoTuple, Value, iproto::{
    constants::{Field, Iterator},
    request::{
      Call, Delete, Eval, Execute,
      Insert, Prepare, Replace, Select,
      Update, Upsert,
    }
  }};

  use super::*;

  #[tokio::test]
  async fn test_tnt_queries() {
    let addr = "127.0.0.1:3301".parse().unwrap();

    let conn: Arc<Connection> = Connector::new(addr)
      .connect().await.unwrap();

    let (res,): ((u32, u32, u32),) = conn.select(Select {
        space_id: 512, index_id: 0,
        limit: 100, offset: 0,
        iterator: Iterator::Eq,
        keys: [ 1u64 ].into_tuple(),
    }).await.expect("bad query");
    assert_eq!(res, (1, 2, 3));

    let res: (u32, u32) = conn.call(Call {
      function: "test".into(),
      args: [ 1u64 ].into_tuple(),
    }).await.expect("bad query");
    assert_eq!(res, (1, 2));

    let (res,): ((u32, u32, u32),) = conn.insert(Insert {
      space_id: 512,
      tuple: [ 5u64, 6, 7 ].into_tuple(),
    }).await.expect("bad query");
    assert_eq!(res, (5, 6, 7));

    let (res,): ((u32, u32, u32),) = conn.replace(Replace {
      space_id: 512,
      tuple: [ 2u64, 3, 4 ].into_tuple(),
    }).await.expect("bad query");
    assert_eq!(res, (2, 3, 4));

    let (res,): ((u32, u32, u32),) = conn.update(Update {
      space_id: 512, index_id: 0,
      key: [ 5u64 ].into_tuple(),
      tuple: vec![ ( "=", 2u32, 10u64 ).into_tuple() ],
    }).await.expect("bad query");
    assert_eq!(res, (5, 6, 10));

    conn.upsert(Upsert {
      space_id: 512, index_base: 0,
      tuple: [ 5u64, 5, 5 ].into_tuple(),
      ops: vec![ ( "=", 2u32, 11u64 ).into_tuple() ],
    }).await.expect("bad query");

    let (res,): ((u32, u32, u32),) = conn.delete(Delete {
      space_id: 512, index_id: 0,
      key: [ 5u64 ].into_tuple(),
    }).await.expect("bad query");
    assert_eq!(res, (5, 6, 11));

    let (res,): (u64,) = conn.eval(Eval {
      expr: "return 5".into(),
      args: ().into_tuple(),
    }).await.expect("bad query");
    assert_eq!(res, 5);

    conn.ping()
      .await.expect("bad query");

    if conn.tarantool_version().starts_with("1.") {
      println!(
        "skip tests of sql binary protocol due to version({}) < 2",
        conn.tarantool_version(),
      );

      return;
    }

    let res = conn.prepare(
      Prepare::SQL("VALUES (?, ?, ?, ?, ?, ?);".into())
    ).await.expect("bad query");
    assert!(res[&Field::StmtID].is_i64());

    let stmt = res[&Field::StmtID].as_i64().unwrap();

    let res = conn.execute(Execute {
      expr: Prepare::StatementID(stmt),
      sql_bind: ( 1, "123", 1.0f32, 1.0f64, true, Value::Null ).into_tuple(),
      options: ().into_tuple(),
    }).await.expect("bad query");
    let tuple = &res[&Field::Data][0];
    let tuple = (
      tuple[0].as_u64().unwrap(),
      tuple[1].as_str().unwrap(),
      tuple[2].as_f64().unwrap(),
      tuple[3].as_f64().unwrap(),
      tuple[4].as_bool().unwrap(),
      tuple[5].is_nil(),
    );
    assert_eq!(tuple, (1, "123", 1.0, 1.0, true, true));
  }

  #[tokio::test]
  async fn test_tnt_auth_select() {
    let addr = "127.0.0.1:3301".parse().unwrap();

    let conn: Arc<Connection> = Connector::new(addr)
      .with_connect_timeout(Duration::from_secs(1))
      .with_auth("em".into(), "em".into())
      .connect().await.unwrap();

    conn.select::<Vec<(u32, u32, u32)>>(Select {
        space_id: 512, index_id: 0,
        limit: 100, offset: 0,
        iterator: Iterator::Eq,
        keys: [ 1u64 ].into_tuple(),
    }).await.expect_err("auth dont work");

    let res: (u32, u32) = conn.call(Call {
      function: "test".into(),
      args: [ 1u64 ].into_tuple(),
    }).await.expect("bad query");

    assert_eq!(res, (1, 2));
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
  async fn test_tnt_30k_async() {
    let addr = "127.0.0.1:3301".parse().unwrap();

    let conn = Connector::new(addr)
      .connect().await.unwrap();

    let conn_calls = conn.clone();
    let calls = tokio::spawn(async move {
      for i in 0..10_000u32 {
        let res: (u32, u32) = conn_calls.call(Call {
          function: "test".into(),
          args: [ i ].into_tuple(),
        }).await.expect("all ok");

        assert_eq!(res, (i, i+1));
      }
    });

    let conn_selects = conn.clone();
    let selects = tokio::spawn(async move {
      let req  = Select {
        space_id: 512, index_id: 0,
        limit: 100, offset: 0,
        iterator: Iterator::Eq,
        keys: [ 1u64 ].into_tuple(),
      };

      for _ in 0..10_000u32 {
        let (res,): ((u32, u32, u32),) = conn_selects.select(req.clone())
          .await.expect("all ok");
        assert_eq!(res, (1, 2, 3));
      }
    });

    let conn_selects_empty = conn.clone();
    let selects_empty = tokio::spawn(async move {
      let req  = Select {
        space_id: 512, index_id: 0,
        limit: 100, offset: 0,
        iterator: Iterator::Eq,
        keys: [ 100500u64 ].into_tuple(),
      };

      for _ in 0..10_000u32 {
        let res: Vec<(u32, u32, u32)> = conn_selects_empty.select(req.clone())
          .await.expect("all ok");
        assert!(res.is_empty());
      }
    });


    let _ = tokio::join!(calls, selects, selects_empty);
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
  async fn test_tnt_reconnect() {

    let addr = "127.0.0.1:3302".parse().unwrap();

    let conn = Connector::new(addr)
      .with_send_request_timeout(Duration::from_secs(5))
      .with_connect_timeout(Duration::from_secs(1))
      .with_reconnect_interval(Duration::from_secs(1))
      .connect().await.unwrap();

    let res = tokio::time::timeout(
      Duration::from_secs(1), conn.eval::<(u32, u32)>(Eval {
      expr: "return 1, 1".into(),
      args: ().into_tuple(),
    })).await.unwrap().unwrap();

    assert_eq!(res, (1, 1));

    let res = tokio::time::timeout(
      Duration::from_secs(1), conn.call::<()>(Call {
      function: "self_close".into(),
      args: ().into_tuple(),
    })).await;

    assert!(res.is_err());

    let res = tokio::time::timeout(
      Duration::from_secs(2), conn.eval::<(u32, u32)>(Eval {
      expr: "return 1, 1".into(),
      args: ().into_tuple(),
    })).await.unwrap().unwrap();

    assert_eq!(res, (1, 1));

    drop(conn);

    tokio::time::sleep(Duration::from_secs(1)).await;
  }

}
