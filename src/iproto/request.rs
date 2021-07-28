/*!
  This module contains structs for requests.
*/

use std::io::Write;

use super::{
  constants::{Field, RequestType, Iterator},
  types::Error,
};
use num_traits::ToPrimitive;
use rmp::encode::{
  write_array_len, write_map_len, write_sint,
  write_str, write_str_len, write_uint,
};

macro_rules! req_func {
  ( $func:ident, $body:ident ) => {
    #[allow(dead_code)]
    pub fn $func(body: $body) -> Request {
      Request::new(RequestType::$body, body)
    }
  };
}

req_func!(auth, Auth);
req_func!(select, Select);
req_func!(call, Call);
req_func!(insert, Insert);
req_func!(replace, Replace);
req_func!(update, Update);
req_func!(delete, Delete);
req_func!(eval, Eval);
req_func!(upsert, Upsert);
req_func!(prepare, Prepare);
req_func!(execute, Execute);

#[allow(dead_code)]
pub fn ping() -> Request {
  Request {
    header: Header::new(RequestType::Ping),
    body: Box::new(Ping),
  }
}

/**
  This trait represents tarantool query body.

  If you want to make custom request body, you should implement it.
*/
pub trait Body: std::fmt::Debug + Send {
  fn pack(&self) -> Result<Vec<u8>, Error>;
}

/**
  This is representation of request.
*/
#[derive(Debug)]
pub struct Request {
  pub header: Header,
  body: Box<dyn Body>,
}

#[allow(dead_code)]
impl Request {

  /// Allows you to construct request.
  pub fn new<B: Body + 'static>(request: RequestType, body: B) -> Request {
    Request {
      header: Header::new(request),
      body: Box::new(body),
    }
  }

  /// Allows you to pack request.
  pub fn pack<W>(&self, w: &mut W) -> Result<(), Error>
    where W: Write
  {

    let header = self.header.pack()?;
    let body = self.body.pack()?;

    let size = header.len() + body.len();

    rmp::encode::write_uint(w, size as u64)?;

    w.write_all(header.as_slice())?;
    w.write_all(body.as_slice())?;

    Ok(())
  }
}

/// This represents header of request.
#[derive(Debug, Clone)]
pub struct Header {
  pub request: RequestType,
  pub sync: u64,
}

#[allow(dead_code)]
impl Header {
  /// Allows you to construct header.
  fn new(request: RequestType) -> Header {
    Header { request, sync: 0 }
  }

  /// Allows you to pack header.
  fn pack(&self) -> Result<Vec<u8>, Error> {
    // think that request will be u32 and sync u64
    let mut buf: Vec<u8> = Vec::with_capacity(18);

    write_map_len(&mut buf, 2)?;

    write_uint(&mut buf, Field::RequestType.to_u64().unwrap())?;
    write_uint(&mut buf, self.request.to_u64().unwrap())?;

    write_uint(&mut buf, Field::Sync.to_u64().unwrap())?;
    write_uint(&mut buf, self.sync)?;

    Ok(buf)
  }
}

/**
  This represents types allowed in tuple.

  It implementst From for std types.
*/
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum Value {
  Int(i64), UInt(u64),
  F32(f32), F64(f64),
  Bool(bool), Null,
  Str(String), Bin(Vec<u8>),
  Array(Vec<Value>),
}

macro_rules! impl_value_from_as {
  ($value:ident, $type:ident, $as:ident) => {
    impl From<$type> for Value {
      fn from(value: $type) -> Self {
        Value::$value(value as $as)
      }
    }
  };
}

impl_value_from_as!(UInt, u64, u64);
impl_value_from_as!(UInt, usize, u64);
impl_value_from_as!(UInt, u32, u64);
impl_value_from_as!(UInt, u16, u64);

impl_value_from_as!(Int, i64, i64);
impl_value_from_as!(Int, isize, i64);
impl_value_from_as!(Int, i32, i64);
impl_value_from_as!(Int, i16, i64);
impl_value_from_as!(Int, i8, i64);

impl_value_from_as!(F32, f32, f32);
impl_value_from_as!(F64, f64, f64);

impl From<bool> for Value {
  fn from(value: bool) -> Self {
    Value::Bool(value)
  }
}

impl From<String> for Value {
  fn from(value: String) -> Self {
    Value::Str(value)
  }
}

impl From<&str> for Value {
  fn from(value: &str) -> Self {
    Value::Str(value.into())
  }
}

impl From<Vec<u8>> for Value {
  fn from(value: Vec<u8>) -> Self {
    Value::Bin(value)
  }
}

impl From<&[u8]> for Value {
  fn from(value: &[u8]) -> Self {
    Value::Bin(value.into())
  }
}

impl<T: Into<Value>> From<Option<T>> for Value {
  fn from(value: Option<T>) -> Self {
    match value {
      Some(value) => value.into(),
      None => Value::Null,
    }
  }
}

impl<T: Into<Value>> From<Vec<T>> for Value {
  fn from(mut value: Vec<T>) -> Self {
    let mut new_vec = Vec::with_capacity(value.len());
    for i in 0..value.len() {
      new_vec.push(value.remove(i).into());
    }
    Value::Array(new_vec)
  }
}

impl<T> From<&[T]> for Value
  where T: Into<Value> + Clone
{
  fn from(value: &[T]) -> Self {
    Value::Array(value.iter()
      .map(|v| v.clone().into())
      .collect()
    )
  }
}

/**
  This trait provides shortcuts for Vec<Value>.

  instead of
  ```rust
    vec![ Value::Int(1), Value::Str("test") ]
  ```
  you can use
  ```rust
    ( 1, "test" ).to_tuple()
  ```

  It works for slice, vec, and tuples with up to 10 elements.
*/
pub trait IntoTuple {
  fn into_tuple(self) -> Vec<Value>;
}

impl<T> IntoTuple for Vec<T> where T: Into<Value> {
  fn into_tuple(mut self) -> Vec<Value> {
    let mut new_vec = Vec::with_capacity(self.len());
    for i in 0..self.len() {
      new_vec.push(self.remove(i).into());
    }
    new_vec
  }
}

impl<T> IntoTuple for &[T]
  where T: Into<Value> + Clone
{
  fn into_tuple(self) -> Vec<Value> {
    self.iter().map(|v| v.clone().into()).collect()
  }
}

impl IntoTuple for ()
{
  fn into_tuple(self) -> Vec<Value> {
    Vec::new()
  }
}

impl<T1> IntoTuple for (T1,)
  where T1: Into<Value>
{
  fn into_tuple(self) -> Vec<Value> {
    vec![ self.0.into() ]
  }
}

impl<T1, T2> IntoTuple for (T1, T2)
  where
    T1: Into<Value>,
    T2: Into<Value>,
{
  fn into_tuple(self) -> Vec<Value> {
    vec![ self.0.into(), self.1.into() ]
  }
}

impl<T1, T2, T3> IntoTuple for (T1, T2, T3)
  where
    T1: Into<Value>,
    T2: Into<Value>,
    T3: Into<Value>,
{
  fn into_tuple(self) -> Vec<Value> {
    vec![ self.0.into(), self.1.into(), self.2.into() ]
  }
}

impl<T1, T2, T3, T4> IntoTuple for (T1, T2, T3, T4)
  where
    T1: Into<Value>,
    T2: Into<Value>,
    T3: Into<Value>,
    T4: Into<Value>,
{
  fn into_tuple(self) -> Vec<Value> {
    vec![
      self.0.into(),
      self.1.into(),
      self.2.into(),
      self.3.into(),
    ]
  }
}


impl<T1, T2, T3, T4, T5> IntoTuple for (T1, T2, T3, T4, T5)
  where
    T1: Into<Value>,
    T2: Into<Value>,
    T3: Into<Value>,
    T4: Into<Value>,
    T5: Into<Value>,
{
  fn into_tuple(self) -> Vec<Value> {
    vec![
      self.0.into(),
      self.1.into(),
      self.2.into(),
      self.3.into(),
      self.4.into(),
    ]
  }
}

impl<T1, T2, T3, T4, T5, T6> IntoTuple for (T1, T2, T3, T4, T5, T6)
  where
    T1: Into<Value>,
    T2: Into<Value>,
    T3: Into<Value>,
    T4: Into<Value>,
    T5: Into<Value>,
    T6: Into<Value>,
{
  fn into_tuple(self) -> Vec<Value> {
    vec![
      self.0.into(),
      self.1.into(),
      self.2.into(),
      self.3.into(),
      self.4.into(),
      self.5.into(),
    ]
  }
}

impl<T1, T2, T3, T4, T5, T6, T7> IntoTuple for (T1, T2, T3, T4, T5, T6, T7)
  where
    T1: Into<Value>,
    T2: Into<Value>,
    T3: Into<Value>,
    T4: Into<Value>,
    T5: Into<Value>,
    T6: Into<Value>,
    T7: Into<Value>,
{
  fn into_tuple(self) -> Vec<Value> {
    vec![
      self.0.into(),
      self.1.into(),
      self.2.into(),
      self.3.into(),
      self.4.into(),
      self.5.into(),
      self.6.into(),
    ]
  }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8> IntoTuple for (T1, T2, T3, T4, T5, T6, T7, T8)
  where
    T1: Into<Value>,
    T2: Into<Value>,
    T3: Into<Value>,
    T4: Into<Value>,
    T5: Into<Value>,
    T6: Into<Value>,
    T7: Into<Value>,
    T8: Into<Value>,
{
  fn into_tuple(self) -> Vec<Value> {
    vec![
      self.0.into(),
      self.1.into(),
      self.2.into(),
      self.3.into(),
      self.4.into(),
      self.5.into(),
      self.6.into(),
      self.7.into(),
    ]
  }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8, T9> IntoTuple for (T1, T2, T3, T4, T5, T6, T7, T8, T9)
  where
    T1: Into<Value>,
    T2: Into<Value>,
    T3: Into<Value>,
    T4: Into<Value>,
    T5: Into<Value>,
    T6: Into<Value>,
    T7: Into<Value>,
    T8: Into<Value>,
    T9: Into<Value>,
{
  fn into_tuple(self) -> Vec<Value> {
    vec![
      self.0.into(),
      self.1.into(),
      self.2.into(),
      self.3.into(),
      self.4.into(),
      self.5.into(),
      self.6.into(),
      self.7.into(),
      self.8.into(),
    ]
  }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> IntoTuple for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)
  where
    T1: Into<Value>,
    T2: Into<Value>,
    T3: Into<Value>,
    T4: Into<Value>,
    T5: Into<Value>,
    T6: Into<Value>,
    T7: Into<Value>,
    T8: Into<Value>,
    T9: Into<Value>,
    T10: Into<Value>,
{
  fn into_tuple(self) -> Vec<Value> {
    vec![
      self.0.into(),
      self.1.into(),
      self.2.into(),
      self.3.into(),
      self.4.into(),
      self.5.into(),
      self.6.into(),
      self.7.into(),
      self.8.into(),
      self.9.into(),
    ]
  }
}

impl Value {
  fn pack<W>(&self, w: &mut W) -> Result<(), Error>
    where W: Write,
  {
    match self {
      &Value::Int(val) => { rmp::encode::write_sint(w, val)?; },
      &Value::UInt(val) => { rmp::encode::write_uint(w, val)?; },
      &Value::F32(val) => { rmp::encode::write_f32(w, val)?; },
      &Value::F64(val) => { rmp::encode::write_f64(w, val)?; },
      &Value::Bool(val) => { rmp::encode::write_bool(w, val)?; },
      Value::Null => { rmp::encode::write_nil(w)?; },
      Value::Str(val) => { rmp::encode::write_str(w, val.as_str())?; },
      Value::Bin(val) => { rmp::encode::write_bin(w, val.as_slice())?; },
      Value::Array(vals) => {
        rmp::encode::write_array_len(w, vals.len() as u32)?;
        for val in vals.iter() { val.pack(w)?; }
      },
    };

    Ok(())
  }
}

#[derive(Debug, Clone)]
pub struct Select {
  pub space_id: u64,
  pub index_id: u64,
  pub limit: u32,
  pub offset: u32,
  pub iterator: Iterator,
  pub keys: Vec<Value>,
}

impl Body for Select {
  fn pack(&self) -> Result<Vec<u8>, Error> {
    let mut data: Vec<u8> = Vec::with_capacity(
      1 + 6 + (5 * 5) +
      (1 + self.keys.len() * 5)
    );
    let buf = &mut data;

    write_map_len(buf, 6)?;

    write_uint(buf, Field::SpaceID.to_u64().unwrap())?;
    write_uint(buf, self.space_id)?;

    write_uint(buf, Field::IndexID.to_u64().unwrap())?;
    write_uint(buf, self.index_id)?;

    write_uint(buf, Field::Limit.to_u64().unwrap())?;
    write_uint(buf, self.limit as u64)?;

    write_uint(buf, Field::Offset.to_u64().unwrap())?;
    write_uint(buf, self.offset as u64)?;

    write_uint(buf, Field::Iterator.to_u64().unwrap())?;
    write_uint(buf, self.iterator.to_u64().unwrap())?;

    write_uint(buf, Field::Key.to_u64().unwrap())?;
    write_array_len(buf, self.keys.len() as u32)?;
    for key in self.keys.iter() { key.pack(buf)?; }

    Ok(data)
  }
}

#[derive(Debug, Clone)]
pub struct Call {
  pub function: String,
  pub args: Vec<Value>,
}

impl Body for Call {
  fn pack(&self) -> Result<Vec<u8>, Error> {
    let mut data: Vec<u8> = Vec::with_capacity(
      1 + 2 +
      (1 + self.function.len()) +
      (1 + self.args.len() * 5)
    );
    let buf = &mut data;

    write_map_len(buf, 2)?;

    write_uint(buf, Field::FunctionName.to_u64().unwrap())?;
    write_str(buf, self.function.as_str())?;

    write_uint(buf, Field::Tuple.to_u64().unwrap())?;
    write_array_len(buf, self.args.len() as u32)?;
    for arg in self.args.iter() { arg.pack(buf)?; }

    Ok(data)
  }
}

#[derive(Debug, Clone)]
pub struct Auth {
  pub user: String,
  pub scramble: Vec<u8>,
}

impl Body for Auth {
  fn pack(&self) -> Result<Vec<u8>, Error> {
    let mut data: Vec<u8> = Vec::with_capacity(
      1 + 2 +
      (1 + self.user.len()) +
      (1 + self.scramble.len())
    );
    let buf = &mut data;

    write_map_len(buf, 2)?;

    write_uint(buf, Field::UserName.to_u64().unwrap())?;
    write_str(buf, self.user.as_str())?;

    write_uint(buf, Field::Tuple.to_u64().unwrap())?;
    write_array_len(buf, 2)?;
    write_str(buf, "chap-sha1")?;
    write_str_len(buf, self.scramble.len() as u32)?;
    data.extend_from_slice(&self.scramble);

    Ok(data)
  }
}

#[derive(Debug, Clone)]
pub struct Insert {
  pub space_id: u64,
  pub tuple: Vec<Value>,
}

impl Body for Insert {
  fn pack(&self) -> Result<Vec<u8>, Error> {
    let mut data: Vec<u8> = Vec::with_capacity(
      1 + 2 + 5 +
      (1 + self.tuple.len() * 5)
    );
    let buf = &mut data;

    write_map_len(buf, 2)?;

    write_uint(buf, Field::SpaceID.to_u64().unwrap())?;
    write_uint(buf, self.space_id)?;

    write_uint(buf, Field::Tuple.to_u64().unwrap())?;
    write_array_len(buf, self.tuple.len() as u32)?;
    for v in self.tuple.iter() { v.pack(buf)?; }

    Ok(data)
  }
}

#[allow(dead_code)]
pub type Replace = Insert;

#[derive(Debug, Clone)]
pub struct Update {
  pub space_id: u64,
  pub index_id: u64,
  pub key: Vec<Value>,
  pub tuple: Vec<Vec<Value>>,
}

impl Body for Update {
  fn pack(&self) -> Result<Vec<u8>, Error> {
    let mut data: Vec<u8> = Vec::with_capacity(
      1 + 4 + (5 * 2) +
      (1 + self.key.len() * 5) +
      (1 + self.tuple.len() * (1 + 5 * 3))
    );
    let buf = &mut data;

    write_map_len(buf, 4)?;

    write_uint(buf, Field::SpaceID.to_u64().unwrap())?;
    write_uint(buf, self.space_id)?;

    write_uint(buf, Field::IndexID.to_u64().unwrap())?;
    write_uint(buf, self.index_id)?;

    write_uint(buf, Field::Key.to_u64().unwrap())?;
    write_array_len(buf, self.key.len() as u32)?;
    for v in self.key.iter() { v.pack(buf)?; }

    write_uint(buf, Field::Tuple.to_u64().unwrap())?;
    write_array_len(buf, self.tuple.len() as u32)?;
    for update in self.tuple.iter() {
      write_array_len(buf, update.len() as u32)?;
      for v in update.iter() { v.pack(buf)?; }
    }

    Ok(data)
  }
}

#[derive(Debug, Clone)]
pub struct Delete {
  pub space_id: u64,
  pub index_id: u64,
  pub key: Vec<Value>,
}

impl Body for Delete {
  fn pack(&self) -> Result<Vec<u8>, Error> {
    let mut data: Vec<u8> = Vec::with_capacity(
      1 + 3 + (5 * 2) + (1 + self.key.len() * 5)
    );
    let buf = &mut data;

    write_map_len(buf, 3)?;

    write_uint(buf, Field::SpaceID.to_u64().unwrap())?;
    write_uint(buf, self.space_id)?;

    write_uint(buf, Field::IndexID.to_u64().unwrap())?;
    write_uint(buf, self.index_id)?;

    write_uint(buf, Field::Key.to_u64().unwrap())?;
    write_array_len(buf, self.key.len() as u32)?;
    for v in self.key.iter() { v.pack(buf)?; }

    Ok(data)
  }
}

#[derive(Debug, Clone)]
pub struct Eval {
  pub expr: String,
  pub args: Vec<Value>,
}

impl Body for Eval {
  fn pack(&self) -> Result<Vec<u8>, Error> {
    let mut data: Vec<u8> = Vec::with_capacity(
      1 + 2 +
      (1 + self.expr.len()) +
      (1 + self.args.len() * 5)
    );
    let buf = &mut data;

    write_map_len(buf, 2)?;

    write_uint(buf, Field::Expr.to_u64().unwrap())?;
    write_str(buf, &self.expr)?;

    write_uint(buf, Field::Tuple.to_u64().unwrap())?;
    write_array_len(buf, self.args.len() as u32)?;
    for v in self.args.iter() { v.pack(buf)?; }

    Ok(data)
  }
}

#[derive(Debug, Clone)]
pub struct Upsert {
  pub space_id: u64,
  pub index_base: u64,
  pub ops: Vec<Vec<Value>>,
  pub tuple: Vec<Value>,
}

impl Body for Upsert {
  fn pack(&self) -> Result<Vec<u8>, Error> {
    let mut data: Vec<u8> = Vec::with_capacity(
      1 + 4 +
      (1 + self.tuple.len() * 5) +
      (1 + self.ops.len() * (1 + 5 * 3))
    );
    let buf = &mut data;

    write_map_len(buf, 4)?;

    write_uint(buf, Field::SpaceID.to_u64().unwrap())?;
    write_uint(buf, self.space_id)?;

    write_uint(buf, Field::IndexBase.to_u64().unwrap())?;
    write_uint(buf, self.index_base)?;

    write_uint(buf, Field::Ops.to_u64().unwrap())?;
    write_array_len(buf, self.ops.len() as u32)?;
    for update in self.ops.iter() {
      write_array_len(buf, update.len() as u32)?;
      for v in update.iter() { v.pack(buf)?; }
    }

    write_uint(buf, Field::Tuple.to_u64().unwrap())?;
    write_array_len(buf, self.tuple.len() as u32)?;
    for v in self.tuple.iter() { v.pack(buf)?; }

    Ok(data)
  }
}

#[derive(Debug, Clone)]
pub struct Ping;

impl Body for Ping {
  fn pack(&self) -> Result<Vec<u8>, Error> {
    Ok(Vec::new())
  }
}


#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum Prepare {
  StatementID(i64),
  SQL(String),
}

impl Prepare {
  fn pack_pair<W>(&self, w: &mut W) -> Result<(), Error>
    where W: Write
  {
    match self {
      &Self::StatementID(id) => {
        write_uint(w, Field::StmtID.to_u64().unwrap())?;
        write_sint(w, id)?;
      },
      Self::SQL(stmt) => {
        write_uint(w, Field::SqlText.to_u64().unwrap())?;
        write_str(w, &stmt)?;
      },
    };

    Ok(())
  }

  fn pair_size_hint(&self) -> usize {
    match self {
      &Self::StatementID(_) => 1 + 5,
      Self::SQL(stmt) => 1 + (1 + stmt.len()),
    }
  }
}

impl Body for Prepare {
  fn pack(&self) -> Result<Vec<u8>, Error> {
    let mut data: Vec<u8> = Vec::with_capacity(1 + self.pair_size_hint());

    let buf = &mut data;

    write_map_len(buf, 1)?;

    self.pack_pair(buf)?;

    Ok(data)
  }
}

#[derive(Debug, Clone)]
pub struct Execute {
  pub expr: Prepare,
  pub sql_bind: Vec<Value>,
  pub options: Vec<Value>,
}

impl Body for Execute {
  fn pack(&self) -> Result<Vec<u8>, Error> {
    let mut data: Vec<u8> = Vec::with_capacity(
      1 + self.expr.pair_size_hint() +
      (1 + 1 + 5 * self.sql_bind.len()) +
      (1 + 1 + 5 * self.options.len())
    );

    let buf = &mut data;

    write_map_len(buf, 3)?;

    self.expr.pack_pair(buf)?;

    write_uint(buf, Field::SqlBind.to_u64().unwrap())?;
    write_array_len(buf, self.sql_bind.len() as u32)?;
    for v in self.sql_bind.iter() { v.pack(buf)?; }

    write_uint(buf, Field::Options.to_u64().unwrap())?;
    write_array_len(buf, self.options.len() as u32)?;
    for v in self.options.iter() { v.pack(buf)?; }

    Ok(data)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_select() {
    let mut req = select(Select {
        space_id: 512,
        index_id: 0,
        limit: 123,
        offset: 0,
        iterator: Iterator::Eq,
        keys: vec![Value::UInt(1)],
    });

    req.header.sync = u32::MAX as u64 + 100;

    let mut buf: Vec<u8> = Vec::new();

    req.pack(&mut buf).expect("pack error");

    assert_eq!(
      &buf,
      &[
        29, 130, 0, 1, 1, 207, 0, 0, 0, 1, 0, 0, 0,
        99, 134, 16, 205, 2, 0, 17, 0, 18, 123, 19,
        0, 20, 0, 32, 145, 1,
      ],
    );

  }

  #[test]
  fn test_call() {
    let mut req = call(Call {
      function: "test".into(),
      args: vec![ Value::UInt(123) ],
    });

    req.header.sync = u32::MAX as u64 + 100;

    let mut buf: Vec<u8> = Vec::new();

    req.pack(&mut buf).unwrap();

    assert_eq!(
      &buf,
      &[
        23, 130, 0, 10, 1, 207, 0, 0, 0, 1, 0, 0, 0, 99,
        130, 34, 164, 116, 101, 115, 116, 33, 145, 123,
      ],
    )
  }

  #[test]
  fn test_insert() {
    let req = insert(Insert {
      space_id: 512,
      tuple: vec![ Value::UInt(2) ],
    });

    let mut buf: Vec<u8> = Vec::new();

    req.pack(&mut buf).unwrap();

    assert_eq!(&buf, &[13, 130, 0, 2, 1, 0, 130, 16, 205, 2, 0, 33, 145, 2]);
  }

}
