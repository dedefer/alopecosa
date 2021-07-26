use std::{collections::HashMap, io::{self, Cursor, Read}, marker::PhantomData, usize};

use super::{constants::{Code, Field, RequestType}, types::Error};

use num_traits::FromPrimitive;
use rmp::decode::{NumValueReadError, ValueReadError, read_array_len, read_int, read_map_len};
use rmpv::{Value, decode::read_value};
use serde::de::DeserializeOwned;

pub trait Parse<R: Read>: Sized {
  fn parse(bytes: &mut R) -> Result<Self, Error>;
}

#[derive(Debug)]
pub struct RequestHeader {
  pub request_type: RequestType,
  pub sync: u64,
}

#[derive(Debug, Default)]
pub struct Header {
  pub code: Code,
  pub sync: u64,
  pub schema: u64,
}

impl<R: Read> Parse<R> for Header {
  fn parse(reader: &mut R) -> Result<Self, Error> {
    let mut header = Header::default();

    let mut map_len = read_map_len(reader)?;

    for _ in 0..map_len {
      let raw_field: u64 = read_int(reader)?;
      let field: Field = FromPrimitive::from_u64(raw_field)
        .ok_or(Error::UnexpectedField(raw_field))?;

      match field {
        Field::RequestType => {
          let value = read_int(reader)?;
          header.code = FromPrimitive::from_u64(value)
            .ok_or(Error::UnexpectedValue(field))?;
        },
        Field::Sync => { header.sync = read_int(reader)? },
        Field::SchemaVersion => { header.schema = read_int(reader)? },
        _ => {
          log::debug!("skipping value due to unexpected field {:?}", field);
          read_value(reader)?;
        },
      }
    }

    Ok(header)
  }
}

pub trait BodyDecoder {
  type Result;
  fn unpack(body: &Vec<u8>) -> Result<Self::Result, Error>;
}

#[derive(Debug, Default)]
pub struct StackRecord {
  pub err_type: String,
  pub file: String,
  pub line: u64,
  pub message: String,
  pub errno: u64,
  pub errcode: u64,
}

#[derive(Debug)]
pub struct TarantoolError {
  pub message: String,
  pub stack: Vec<StackRecord>,
}

pub struct ErrorBody;

impl BodyDecoder for ErrorBody {
  type Result = TarantoolError;

  fn unpack(body: &Vec<u8>) -> Result<Self::Result, Error> {
    let mut reader = Cursor::new(body);
    let reader = &mut reader;

    let map_len = read_map_len(reader)?;

    let mut body = TarantoolError {
      message: String::new(), stack: Vec::new(),
    };

    let read_string = |reader: &mut Cursor<&Vec<u8>>| -> Result<String, Error> {
      let str_len = rmp::decode::read_str_len(reader)?;
      let mut buf: Vec<u8> = Vec::new();
      buf.resize(str_len as usize, 0);
      reader.read_exact(&mut buf)?;
      String::from_utf8(buf).map_err(|_| io::Error::new(
        io::ErrorKind::InvalidInput,
        "invalid ut8 string",
      ).into())
    };

    for _ in 0..map_len {
      let raw_field: u64 = read_int(reader)?;
      let field: Field = FromPrimitive::from_u64(raw_field)
        .ok_or(Error::UnexpectedField(raw_field))?;

      match field {
        Field::Error24 => { body.message = read_string(reader)? },

        // see more here
        // https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/#msgpack-ext-error
        Field::Error => {
          let map_len = read_map_len(reader)?;
          for _ in 0..map_len {
            if read_int::<u64, _>(reader)? != 0 { // field stack
              read_value(reader)?;
              continue;
            }

            let stack_len = read_array_len(reader)?;

            let mut stack: Vec<StackRecord> = Vec::with_capacity(stack_len as usize);

            for _ in 0..stack_len {
              let mut stack_record = StackRecord::default();

              for _ in 0..read_map_len(reader)? {
                match read_int::<u64, _>(reader)? {
                  0 => { stack_record.err_type = read_string(reader)?; },
                  1 => { stack_record.file = read_string(reader)?; },
                  2 => { stack_record.line = read_int(reader)?; },
                  3 => { stack_record.message = read_string(reader)?; },
                  4 => { stack_record.errno = read_int(reader)?; }
                  5 => { stack_record.errcode = read_int(reader)?; }
                  _ => { read_value(reader)?; },
                }
              }

              stack.push(stack_record);
            }

            body.stack = stack;
          }
        },

        _ => {
          log::debug!("skipping value due to unexpected field {:?}", field);
          read_value(reader)?;
        },
      };
    }

    Ok(body)
  }
}

pub struct TupleBody<T>(PhantomData<T>)
  where T: DeserializeOwned;

impl<T> BodyDecoder for TupleBody<T>
  where T: DeserializeOwned
{
  type Result = T;

  fn unpack(body: &Vec<u8>) -> Result<T, Error> {
    let mut cur = Cursor::new(body);

    let map_len = read_map_len(&mut cur)?;

    if map_len != 1 {
      return Err(io::Error::new(
        io::ErrorKind::Other, "expected 1 field",
      ).into());
    }

    let raw_field: u64 = read_int(&mut cur)?;
    let field: Field = FromPrimitive::from_u64(raw_field)
      .ok_or(Error::UnexpectedField(raw_field))?;

    match field {
      Field::Data =>
        rmp_serde::decode::from_read::<_, T>(cur)
          .map_err(|e| Error::ParseError(e)),
      _ => Err(Error::UnexpectedField(raw_field))?,
    }
  }
}

#[derive(Debug)]
pub struct Response {
  pub header: Header,
  pub body: Option<Vec<u8>>,
}

impl Response {
  pub fn parse<R>(mut reader: R) -> Result<Self, Error>
    where R: Read
  {
    let size: u64 = read_int(&mut reader)?;
    let mut reader = reader.take(size);

    let header: Header = Parse::parse(&mut  reader)?;

    let mut body: Vec<u8> = Vec::with_capacity(size as usize);

    reader.read_to_end(&mut body)?;

    if body.len() == 0 {
      return Ok(Response { header, body: None });
    }

    body.shrink_to_fit();

    Ok(Response { header, body: Some(body) })
  }

  pub fn unpack_body<B>(&self) -> Result<B::Result, Error>
    where B: BodyDecoder,
  {
    match &self.body {
      Some(body) => B::unpack(body),
      None => Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "body is empty",
      ).into()),
    }
  }
}

#[cfg(test)]
mod tests {
    use super::*;

    // #[test]
    // fn it_works() {
    //   let buf = [
    //     206, 0, 0, 0, 34, 131, 0, 206, 0, 0, 0, 0, 1,
    //     207, 0, 0, 0, 0, 0, 0, 0, 0, 5, 206, 0, 0, 0,
    //     80, 129, 48, 221, 0, 0, 0, 1, 147, 1, 2, 3,
    //   ];
    //   let resp = Response::parse(&buf[..]).unwrap();
    //   let tuple: Vec<(u64, u64, u64)> = dbg!(resp).unpack_body::<TupleBody<_>>().unwrap();
    //   dbg!(tuple);
    // }


    // #[test]
    // fn it_works_call() {
    //   let buf = [
    //     206, 0, 0, 0, 32, 131, 0, 206, 0, 0, 0, 0, 1, 207,
    //     0, 0, 0, 1, 0, 0, 0, 99, 5, 206, 0, 0, 0, 80,
    //     129, 48, 221, 0, 0, 0, 2, 123, 124,
    //   ];
    //   let mut cur = Cursor::new(&buf);

    //   let resp = Response::parse(&mut cur).unwrap();
    //   let tuple: (u64, u64) = dbg!(resp).unpack_body::<TupleBody<_>>().unwrap();
    //   dbg!(tuple);
    // }

    // #[test]
    // fn it_works_err() {

    //   let buf = [
    //     206, 0, 0, 0, 147, // len
    //     131, 0, 206, 0, 0, 128, 20, 1, 207, 0, 0, 0, 0, 0, 0, 0, 0, 5, 206, 0, 0, 0, 80, // header
    //     130, 49, 189, 73, 110, 118, 97, 108, 105, 100, 32, 77, 115, 103, 80, 97, 99,
    //     107, 32, 45, 32, 112, 97, 99, 107, 101, 116, 32, 98, 111, 100, 121, 82, 129,
    //     0, 145, 134, 0, 171, 67, 108, 105, 101, 110, 116, 69, 114, 114, 111, 114, 2,
    //     204, 216, 1, 217, 33, 47, 117, 115, 114, 47, 115, 114, 99, 47, 116, 97, 114,
    //     97, 110, 116, 111, 111, 108, 47, 115, 114, 99, 47, 98, 111, 120, 47, 120, 114,
    //     111, 119, 46, 99, 3, 189, 73, 110, 118, 97, 108, 105, 100, 32, 77, 115, 103,
    //     80, 97, 99, 107, 32, 45, 32, 112, 97, 99, 107, 101, 116, 32, 98, 111, 100, 121, 4, 0, 5, 20,
    //   ];

    //   let resp = Response::parse(&buf[..]).unwrap();

    //   assert!(dbg!(resp.header.code).is_err());

    //   dbg!(resp.unpack_body::<ErrorBody>().unwrap());
    // }
}
