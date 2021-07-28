use std::{
  str,
  net::SocketAddr,
  sync::{Arc, atomic::AtomicBool},
  time::Duration,
};

use base64::decode;
use dashmap::DashMap;
use sha1::{Digest, Sha1};
use tokio::{
  io::{AsyncReadExt, AsyncWriteExt},
  net::{TcpSocket, TcpStream},
  sync::mpsc,
};

use crate::iproto::{
  request::{self, Auth},
  response::Response,
};

use super::{Connection, connection_server::ConnectionServer};


/**
  This is the only struct that allows you to connect to tarantool.
  It also allows you to tune connection.

  Example:
  ```rust
    let addr = "127.0.0.1:3301".parse().unwrap();

    let conn: Arc<Connection> = Connector::new(addr)
      .with_auth("guest".into(), "guest".into())
      .with_connect_timeout(Duration::from_secs(1))
      .with_reconnect_interval(Duration::from_secs(1))
      .with_send_request_timeout(Duration::from_secs(10))
      .connect().await.unwrap();
  ```
*/
#[derive(Debug, Clone)]
pub struct Connector {
  pub(crate) addr: SocketAddr,
  pub(crate) reconnect_interval: Option<tokio::time::Duration>,
  pub(crate) connect_timeout: Option<tokio::time::Duration>,
  pub(crate) send_request_timeout: Option<tokio::time::Duration>,
  pub(crate) credentials: Option<(String, String)>,
}

#[allow(dead_code)]
impl Connector {
  pub fn new(addr: SocketAddr) -> Connector {
    Connector {
      addr, credentials: None,
      connect_timeout: None,
      reconnect_interval: None,
      send_request_timeout: None,
    }
  }

  pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
    self.connect_timeout = Some(timeout);
    self
  }

  pub fn with_auth(mut self, user: String, password: String) -> Self {
    self.credentials = Some((user, password));
    self
  }

  pub fn with_reconnect_interval(mut self, interval: Duration) -> Self {
    self.reconnect_interval = Some(interval);
    self
  }

  /// set timeout for sending request to connection
  pub fn with_send_request_timeout(mut self, timeout: Duration) -> Self {
    self.send_request_timeout = Some(timeout);
    self
  }

  /// perform connection to tarantool
  pub async fn connect(self) -> Result<Arc<Connection>, tokio::io::Error> {
    let (stream, version) = self.new_connection().await?;

    let (sender, reader) = mpsc::channel(1000);

    let resp_chans = Arc::new(DashMap::new());

    let closed = Arc::new(AtomicBool::new(false));

    let conn = Arc::new(Connection {
        version, sync: 1.into(),
        req_chan_sender: sender,
        closed: closed.clone(),
        resp_chans: resp_chans.clone(),
    });

    let conn_server = ConnectionServer {
      connector: self, req_chan_reader: reader,
      resp_chans, closed,
    };

    tokio::spawn(conn_server.serve_loop(stream));

    Ok(conn)
  }

  pub(crate) async fn new_connection(&self) -> Result<(TcpStream, String), std::io::Error> {
    let sock = match self.addr.is_ipv4() {
      true => TcpSocket::new_v4(),
      false => TcpSocket::new_v6(),
    }?;

    let (conn, ver): (TcpStream, String) = match self.connect_timeout {
      None => self.connect_and_greet(sock).await?,
      Some(timeout) =>
        match tokio::time::timeout(timeout, self.connect_and_greet(sock)).await {
          Ok(Ok(res)) => res,
          Ok(err) => return err,
          Err(elapsed) => return Err(elapsed.into()),
        },
    };

    Ok((conn, ver))
  }

  async fn connect_and_greet(&self, sock: TcpSocket) -> Result<(TcpStream, String), std::io::Error> {
    let mut conn = sock.connect(self.addr).await?;
    let version = self.handle_greating_and_auth(&mut conn).await?;
    Ok((conn, version))
  }

  async fn handle_greating_and_auth(
    &self, conn: &mut TcpStream,
  ) -> Result<String, std::io::Error> {

    let mut greeting_buf = [0u8; 128];

    conn.read_exact(&mut greeting_buf).await?;

    let (version, salt) = self.parse_greeting(&greeting_buf)
      .ok_or_else(|| std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "bad greeting",
      ))?;

    let (user, password) = match &self.credentials {
      Some(creds) => creds,
      None => return Ok(version.into()),
    };

    let salt = match decode(salt) {
      Ok(salt) => salt,
      Err(_) => return Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "bad salt",
      )),
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
        "auth unpack resp error",
      ))?;

    if req.header.sync != 0 || req.header.code.is_err() {
      return Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!("auth error: {:?}", req.header.code),
      ));
    }

    Ok(version.into())
  }

  fn parse_greeting<'g>(&self, greeting: &'g [u8; 128]) -> Option<(&'g str, &'g [u8])> {
    const START: &str = "Tarantool ";
    const PROTO: &str = " (Binary) ";
    const SEP: &str = "\n";
    const SALT_LEN: usize = 44;

    let greeting = match str::from_utf8(&greeting[..]) {
      Ok(g) => g,
      Err(_) => return None,
    };

    let start_pos = greeting.find(START)? + START.len();
    let proto_pos = greeting.find(PROTO)?;

    let version = &greeting[start_pos..proto_pos];

    let salt_start = greeting.find(SEP)? + SEP.len();

    let salt = greeting[salt_start..(salt_start+SALT_LEN)].as_bytes();

    Some((version, salt))
  }

  fn auth_scramble(&self, salt: &[u8], password: &str) -> Vec<u8> {
    let mut hasher = Sha1::default();
    Digest::update(&mut hasher, password);
    let hash1 = hasher.finalize();

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
