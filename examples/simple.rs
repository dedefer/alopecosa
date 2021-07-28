use std::{
  error::Error,
  time::Duration,
  sync::Arc,
};
use alopecosa::{
  Connection, Connector, IntoTuple,
  Call, Eval, Select, Iterator
};
use tokio::time::timeout;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  let addr = "127.0.0.1:3301".parse()?;
  let conn: Arc<Connection> = Connector::new(addr)
    .connect().await?;

  let eval_resp: (u32, u32) = conn.eval(Eval {
    expr: "return 1, 2".into(),
    args: ().into_tuple(),
  }).await?;

  let select_resp: Vec<(u32, u32, u32)> = conn.select(Select {
    space_id: 512, index_id: 0,
    limit: 100, offset: 0,
    iterator: Iterator::Ge,
    keys: ( 1u64, ).into_tuple(),
  }).await?;

  let (resp_with_timeout,): (i32,) = timeout(
    Duration::from_secs(1),
    conn.call(Call {
      function: "echo".into(),
      args: ( 123, ).into_tuple(),
    })
  ).await??;

  Ok(())
}
