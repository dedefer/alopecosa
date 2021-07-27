mod connection;
mod connector;
mod connection_server;

pub use connection::Connection;
pub use connector::Connector;


#[cfg(test)]
mod tests {
  use std::{sync::Arc, time::Duration};

  use crate::iproto::{constants::Iterator, request::{Call, Select, Value}};

  use super::*;

  #[tokio::test]
  async fn my_test() {
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
  async fn my_test2() {
    let addr = "127.0.0.1:3301".parse().unwrap();

    let conn = Connection::new(addr)
      .connect().await.unwrap();

    let conn2 = conn.clone();
    let conn3 = conn.clone();

    let kk = tokio::spawn(async move {
      for i in 0..1000u32 {
        let res: (u32, u32) = conn.call(Call {
          function: "test".into(),
          args: vec![ Value::UInt(i as u64) ],
        }).await.expect("all ok");

        assert_eq!(res, (i, i+1));
      }
    });

    let kk2 = tokio::spawn(async move {
      let req  = Select {
        space_id: 512, index_id: 0,
        limit: 100, offset: 0,
        iterator: Iterator::Eq,
        keys: vec![ Value::UInt(1) ],
      };

      for _ in 0..1000u32 {
        let res: Vec<(u32, u32, u32)> = conn2.select(req.clone())
          .await.expect("all ok");
        assert_eq!(res[0], (1, 2, 3));
      }
    });

    let kk3 = tokio::spawn(async move {
      let req  = Select {
        space_id: 512, index_id: 0,
        limit: 100, offset: 0,
        iterator: Iterator::Eq,
        keys: vec![ Value::UInt(1) ],
      };

      for _ in 0..1000u32 {
        let res: Vec<(u32, u32, u32)> = conn3.select(req.clone())
          .await.expect("all ok");
        assert_eq!(res[0], (1, 2, 3));
      }
    });


    let _ = tokio::join!(kk, kk2, kk3);
  }

}
