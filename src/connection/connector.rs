use std::{
  collections::HashMap, net::SocketAddr,
  sync::{Arc, atomic::AtomicBool},
  time::Duration,
};

use base64::decode;
use dashmap::DashMap;
use sha1::{Digest, Sha1};
use tokio::{
  io::{AsyncReadExt, AsyncWriteExt},
  net::{TcpSocket, TcpStream},
  sync::{Mutex, mpsc},
};

use crate::iproto::{
  request::{self, Auth},
  response::Response,
};

use super::{
  connection::Connection,
  connection_server::ConnectionServer,
};



#[derive(Debug)]
pub struct Connector {
  pub(crate) addr: SocketAddr,
  pub(crate) connect_timeout: Option<tokio::time::Duration>,
  pub(crate) credentials: Option<(String, String)>,
}

impl Connector {
  pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
    self.connect_timeout = Some(timeout);
    self
  }

  pub fn with_auth(mut self, user: String, password: String) -> Self {
    self.credentials = Some((user, password));
    self
  }

  pub async fn connect(self) -> Result<Arc<Connection>, tokio::io::Error> {
    let stream = self.new_connection().await?;

    let (sender, reader) = mpsc::channel(1000);

    let resp_chans = Arc::new(DashMap::new());

    let closed = Arc::new(AtomicBool::new(false));

    let conn = Arc::new(Connection {
        sync: 1.into(),
        req_chan_sender: sender,
        closed: closed.clone(),
        resp_chans: resp_chans.clone(),
    });

    let conn_server = ConnectionServer {
      connector: self, req_chan_reader: reader,
      resp_chans: resp_chans.clone(),
      closed: closed.clone(),
    };

    tokio::spawn(conn_server.serve_cycle(stream));

    Ok(conn)
  }

  pub(crate) async fn new_connection(&self) -> Result<TcpStream, std::io::Error> {
    let sock = match self.addr.is_ipv4() {
      true => TcpSocket::new_v4(),
      false => TcpSocket::new_v6(),
    }?;

    let conn: TcpStream = match self.connect_timeout {
      None => self.connect_and_greet(sock).await?,
      Some(timeout) =>
        match tokio::time::timeout(timeout, self.connect_and_greet(sock)).await {
          Ok(Ok(conn)) => conn,
          Ok(Err(e)) => return Err(e.into()),
          Err(elapsed) => return Err(elapsed.into()),
        },
    };

    Ok(conn)
  }

  async fn connect_and_greet(&self, sock: TcpSocket) -> Result<TcpStream, std::io::Error> {
    let mut conn = sock.connect(self.addr).await?;
    self.handle_greating_and_auth(&mut conn).await?;
    Ok(conn)
  }

  async fn handle_greating_and_auth(
    &self, conn: &mut TcpStream,
  ) -> Result<(), std::io::Error> {

    let mut start_buf: [u8; 128] = [0; 128];

    conn.read_exact(&mut start_buf).await?;

    let (user, password) = match &self.credentials {
      Some(cread) => cread,
      None => return Ok(()),
    };

    let salt = match decode(&start_buf[64..(64 + 44)]) {
      Ok(salt) => salt,
      Err(_) => Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "bad salt",
      ))?,
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

    Ok(())
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
