mod connection;
mod connector;
mod connection_server;

pub use connection::Connection;
pub use connector::Connector;


#[cfg(test)]
mod tests {
  use std::{sync::Arc, time::Duration};

  use crate::iproto::{constants::{Field, Iterator}, request::{Call, Delete, Eval, Execute, Insert, Prepare, Replace, Select, Update, Upsert, Value}};

  use super::*;

  #[tokio::test]
  async fn test_tnt_queries() {
    let addr = "127.0.0.1:3301".parse().unwrap();

    let conn: Arc<Connection> = Connection::new(addr)
      .connect().await.unwrap();

    let res: Vec<(u32, u32, u32)> = conn.select(Select {
        space_id: 512, index_id: 0,
        limit: 100, offset: 0,
        iterator: Iterator::Eq,
        keys: vec![ Value::UInt(1) ],
    }).await.expect("bad query");
    assert_eq!(res, &[(1, 2, 3)]);

    let res: (u32, u32) = conn.call(Call {
      function: "test".into(),
      args: vec![ Value::UInt(1) ],
    }).await.expect("bad query");
    assert_eq!(res, (1, 2));

    let res: Vec<(u32, u32, u32)> = conn.insert(Insert {
      space_id: 512,
      tuple: vec![ Value::UInt(5), Value::UInt(6), Value::UInt(7) ],
    }).await.expect("bad query");
    assert_eq!(res, &[(5, 6, 7)]);

    let res: Vec<(u32, u32, u32)> = conn.replace(Replace {
      space_id: 512,
      tuple: vec![ Value::UInt(2), Value::UInt(3), Value::UInt(4) ],
    }).await.expect("bad query");
    assert_eq!(res, &[(2, 3, 4)]);

    let res: Vec<(u32, u32, u32)> = conn.update(Update {
      space_id: 512, index_id: 0,
      key: vec![ Value::UInt(5) ],
      tuple: vec![ vec![ Value::Str("=".into()), Value::UInt(2), Value::UInt(10) ] ],
    }).await.expect("bad query");
    assert_eq!(res, &[(5, 6, 10)]);

    conn.upsert(Upsert {
      space_id: 512, index_base: 0,
      tuple: vec![ Value::UInt(5), Value::UInt(5), Value::UInt(5) ],
      ops: vec![ vec![ Value::Str("=".into()), Value::UInt(2), Value::UInt(11) ] ],
    }).await.expect("bad query");

    let res: Vec<(u32, u32, u32)> = conn.delete(Delete {
      space_id: 512, index_id: 0,
      key: vec![ Value::UInt(5) ],
    }).await.expect("bad query");
    assert_eq!(res, &[(5, 6, 11)]);

    let (res,): (u64,) = conn.eval(Eval {
      expr: "return 5".into(),
      args: Vec::new(),
    }).await.expect("bad query");
    assert_eq!(res, 5);

    let res = conn.prepare(
      Prepare::SQL("VALUES (?, ?);".into())
    ).await.expect("bad query");
    assert!(res[&Field::StmtID].is_i64());

    let stmt = res[&Field::StmtID].as_i64().unwrap();

    let res = conn.execute(Execute {
      expr: Prepare::StatementID(stmt),
      sql_bind: vec![ Value::UInt(1), Value::UInt(2) ],
      options: Vec::new(),
    }).await.expect("bad query");
    let tuple = &res[&Field::Data][0];
    let tuple = (
      tuple[0].as_u64().unwrap(),
      tuple[1].as_u64().unwrap(),
    );
    assert_eq!(tuple, (1, 2));
  }

  #[tokio::test]
  async fn test_tnt_auth_select() {
    let addr = "127.0.0.1:3301".parse().unwrap();

    let conn: Arc<Connection> = Connection::new(addr)
      .with_connect_timeout(Duration::from_secs(1))
      .with_auth("em".into(), "em".into())
      .connect().await.unwrap();

    conn.select::<Vec<(u32, u32, u32)>>(Select {
        space_id: 512, index_id: 0,
        limit: 100, offset: 0,
        iterator: Iterator::Eq,
        keys: vec![ Value::UInt(1) ],
    }).await.expect_err("auth dont work");

    let res: (u32, u32) = conn.call(Call {
      function: "test".into(),
      args: vec![ Value::UInt(1) ],
    }).await.expect("bad query");

    assert_eq!(res, (1, 2));
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
  async fn test_tnt_30k_async() {
    let addr = "127.0.0.1:3301".parse().unwrap();

    let conn = Connection::new(addr)
      .connect().await.unwrap();

    let conn_calls = conn.clone();
    let calls = tokio::spawn(async move {
      for i in 0..10_000u32 {
        let res: (u32, u32) = conn_calls.call(Call {
          function: "test".into(),
          args: vec![ Value::UInt(i as u64) ],
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
        keys: vec![ Value::UInt(1) ],
      };

      for _ in 0..10_000u32 {
        let res: Vec<(u32, u32, u32)> = conn_selects.select(req.clone())
          .await.expect("all ok");
        assert_eq!(res[0], (1, 2, 3));
      }
    });

    let conn_selects_empty = conn.clone();
    let selects_empty = tokio::spawn(async move {
      let req  = Select {
        space_id: 512, index_id: 0,
        limit: 100, offset: 0,
        iterator: Iterator::Eq,
        keys: vec![ Value::UInt(100500) ],
      };

      for _ in 0..10_000u32 {
        let res: Vec<(u32, u32, u32)> = conn_selects_empty.select(req.clone())
          .await.expect("all ok");
        assert_eq!(&res, &[]);
      }
    });


    let _ = tokio::join!(calls, selects, selects_empty);
  }

}
