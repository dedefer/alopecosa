![MIT licensed](https://img.shields.io/github/license/dedefer/alopecosa?style=for-the-badge)
[![Version](https://img.shields.io/crates/v/alopecosa?style=for-the-badge)](https://crates.io/crates/alopecosa/)
![Downloads](https://img.shields.io/crates/d/alopecosa?style=for-the-badge)

# Alopecosa

Alopecosa is a convenient async pure-rust [Tarantool 1.6+](www.tarantool.io) connector built on tokio (version 1).

[Documentation link](https://docs.rs/alopecosa/)

[Crates.io link](https://crates.io/crates/alopecosa/)

## Example

```rust
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

```

## Auth

```rust
let addr = "127.0.0.1:3301".parse()?;
let conn: Arc<Connection> = Connector::new(addr)
  .with_auth("user".into(), "password".into())
  .connect().await?;
```

## Connection tuning

```rust
let addr = "127.0.0.1:3301".parse()?;
let conn: Arc<Connection> = Connector::new(addr)
  .with_connect_timeout(Duration::from_secs(10))
  .with_reconnect_interval(Duration::from_secs(1))
  .with_send_request_timeout(Duration::from_secs(10))
  .connect().await?;
```
