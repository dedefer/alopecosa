use std::io::Write;

use super::{constants::{Field, RequestType, Iterator}, types::Error};
use num_traits::ToPrimitive;
use rmp::encode::{write_array_len, write_map_len, write_str, write_str_len, write_uint};

pub trait Pack {
  fn pack(&self) -> Result<Vec<u8>, Error>;
}

pub fn auth(body: Auth) -> Request {
  Request {
    header: Header { request: RequestType::Auth, sync: 0 },
    body: Box::new(body),
  }
}

pub fn select(body: Select) -> Request {
  Request {
    header: Header { request: RequestType::Select, sync: 0 },
    body: Box::new(body),
  }
}

pub fn call(body: Call) -> Request {
  Request {
    header: Header { request: RequestType::Call, sync: 0 },
    body: Box::new(body),
  }
}

pub trait Body: Pack + std::fmt::Debug + Send {}

#[derive(Debug)]
pub struct Request {
  pub header: Header,
  body: Box<dyn Body>,
}

impl Request {
  pub fn pack<W>(&self, w: &mut W) -> Result<(), Error>
    where W: Write
  {

    let header = self.header.pack()?;
    let body = self.body.pack()?;

    let size = header.len() + body.len();

    rmp::encode::write_uint(w, size as u64)?;

    w.write(header.as_slice())?;
    w.write(body.as_slice())?;

    Ok(())
  }
}

#[derive(Debug)]
pub struct Header {
  pub request: RequestType,
  pub sync: u64,
}

impl Pack for Header {
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

#[derive(Debug, Clone)]
pub enum Value {
  Int(i64), UInt(u64),
  F32(f32), F64(f64),
  Bool(bool), Null,
  Str(String), Bin(Vec<u8>),
  Array(Vec<Value>),
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

impl Body for Select {}

impl Pack for Select {
  fn pack(&self) -> Result<Vec<u8>, Error> {
    let mut data: Vec<u8> = Vec::with_capacity(
      1 + (5 * 5) + (1 + self.keys.len() * 5),
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

#[derive(Debug)]
pub struct Call {
  pub function: String,
  pub args: Vec<Value>,
}

impl Body for Call {}

impl Pack for Call {
  fn pack(&self) -> Result<Vec<u8>, Error> {
    let mut data: Vec<u8> = Vec::with_capacity(
      1 + (1 + self.function.len()) + (1 + self.args.len() * 5),
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

#[derive(Debug)]
pub struct Auth {
  pub user: String,
  pub scramble: Vec<u8>,
}

impl Body for Auth {}

impl Pack for Auth {
  fn pack(&self) -> Result<Vec<u8>, Error> {
    let mut data: Vec<u8> = Vec::with_capacity(26 + self.user.len());
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



#[cfg(test)]
mod tests {
  use std::io::Cursor;

  use super::*;

  // #[test]
  // fn it_works() {
  //   let header = Header {
  //     request: RequestType::Select, sync: 123,
  //   };
  //   dbg!(&header);

  //   let packed = header.pack().unwrap();
  //   dbg!(dbg!(packed).len());
  // }


  // #[test]
  // fn it_works3() {
  //   let mut req = select(Select {
  //       space_id: 512,
  //       index_id: 0,
  //       limit: 123,
  //       offset: 0,
  //       iterator: Iterator::Eq,
  //       keys: vec![Value::UInt(1)],
  //   });

  //   req.header.sync = u32::MAX as u64 + 100;

  //   let mut buf: Vec<u8> = Vec::new();

  //   req.pack(&mut buf).unwrap();
  // }


  //   dbg!(&req);
  //   dbg!(&buf);
  //   dbg!(buf.len());
  // }

  // #[test]
  // fn test_call() {
  //   let mut req = call(Call {
  //     function: "test".into(),
  //     args: vec![ Value::UInt(123) ],
  //   });

  //   req.header.sync = u32::MAX as u64 + 100;

  //   let mut buf: Vec<u8> = Vec::new();

  //   req.pack(&mut buf).unwrap();

  //   dbg!(&req);
  //   dbg!(&buf);
  //   dbg!(buf.len());
  // }

  // #[test]
  // fn it_works4() {
  //   let mut buf = [23, 131, 0, 1, 1, 0, 5, 80, 134, 16, 205, 2, 0, 17, 0, 19, 0, 18, 12, 20, 0, 32, 145, 1];
  //   let mut buf_my = [23, 130, 0, 1, 1, 0, 5, 80, 130, 16, 205, 2, 0, 17, 0, 18, 123, 19, 0, 20, 0, 32, 145, 1];

  //   let mut cur = Cursor::new(&mut buf);
  //   let mut cur_my = Cursor::new(&mut buf_my);

  //   let cur = &mut cur;
  //   let cur_my = &mut cur_my;


  //   dbg!(rmp::decode::read_int::<u64, _>(cur));
  //   dbg!(rmp::decode::read_int::<u64, _>(cur_my));

  //   dbg!("======================================================");

  //   dbg!(rmpv::decode::read_value(cur));
  //   dbg!(rmpv::decode::read_value(cur_my));

  //   dbg!("======================================================");

  //   dbg!(rmpv::decode::read_value(cur));
  //   dbg!(rmpv::decode::read_value(cur_my));

  //   // dbg!(rmp::decode::read_map_len(cur));
  //   // dbg!(cur.position());

  //   // dbg!(rmp::decode::read_int::<u64, _>(cur).unwrap());
  //   // dbg!(cur.position());
  //   // dbg!(rmp::decode::read_int::<u64, _>(cur).unwrap());
  //   // dbg!(cur.position());

  //   // dbg!(rmp::decode::read_int::<u64, _>(cur).unwrap());
  //   // dbg!(cur.position());
  //   // dbg!(rmp::decode::read_int::<u64, _>(cur).unwrap());
  //   // dbg!(cur.position());

  //   // dbg!(rmp::decode::read_int::<u64, _>(cur).unwrap());
  //   // dbg!(cur.position());
  //   // dbg!(rmp::decode::read_int::<u64, _>(cur).unwrap());
  //   // dbg!(cur.position());
  // }

}
