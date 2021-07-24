use std::{error, fmt::{Display, Debug}, io::{self, Cursor, Read, Take}, marker::PhantomData};
use rmp::decode::{NumValueReadError, ValueReadError};
use serde::{Serialize, Deserialize, de::DeserializeOwned};
use crate::iproto::constants::{Request, Code, Field};
use num_traits::{ToPrimitive, FromPrimitive};

pub enum Error {
  ParseError(NumValueReadError),
  UnexpectedField(u64),
  UnexpectedValue(Field),
  TarantoolError(Code, String),
}

impl error::Error for Error {}

impl Display for Error {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::ParseError(err) =>
        Display::fmt(err, f),
      &Self::UnexpectedValue(field) =>
        write!(f, "unexpected value for field {:?}", field),
      &Self::UnexpectedField(field) =>
        write!(f, "unexpected field {}", field),
      Self::TarantoolError(code, message) =>
        write!(f, "TarantoolError(code={:?}, message={:?})", code, message),
    }
  }
}

impl Debug for Error {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    Display::fmt(&self, f)
  }
}

impl From<NumValueReadError> for Error {
    fn from(err: NumValueReadError) -> Self {
        Error::ParseError(err)
    }
}

impl From<ValueReadError> for Error {
  fn from(err: ValueReadError) -> Self {
      Error::ParseError(err.into())
  }
}

impl From<io::Error> for Error {
  fn from(err: io::Error) -> Self {
      Error::ParseError(NumValueReadError::InvalidDataRead(err))
  }
}

pub trait Parse<R: Read>: Sized {
  fn parse(bytes: &mut R) -> Result<Self, Error>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Size(u64);

#[derive(Debug)]
pub struct RequestHeader {
  pub request_type: Request,
  pub sync: u64,
}

#[derive(Debug, Default)]
pub struct ResponseHeader {
  pub code: Code,
  pub sync: u64,
  pub schema: u64,
}

impl<R: Read> Parse<R> for ResponseHeader {
  fn parse(reader: &mut R) -> Result<Self, Error> {
    let mut header = ResponseHeader::default();

    let mut map_len = rmp::decode::read_map_len(reader)?;

    while map_len > 0 {

      let raw_field: u64 = rmp::decode::read_int(reader)?;
      let field: Field = FromPrimitive::from_u64(raw_field)
        .ok_or(Error::UnexpectedField(raw_field))?;

      let value: u64 = rmp::decode::read_int(reader)?;

      match field {
        Field::RequestType => {
          header.code = FromPrimitive::from_u64(value)
            .ok_or(Error::UnexpectedValue(field))?;
        },
        Field::Sync => { header.sync = value },
        Field::SchemaVersion => { header.schema = value },
        _ => return Err(Error::UnexpectedField(raw_field)),
      }

      map_len -= 1;
    }

    Ok(header)
  }
}

pub trait ResponseBody<R: Read>: Parse<R> {}

impl<R: Read> ResponseBody<R> for BodyError {}

impl<R: Read> ResponseBody<R> for SimpleBody {}

#[derive(Debug)]
pub struct BodyError(String);

impl<R> Parse<R> for BodyError where R: Read {
  fn parse(reader: &mut R) -> Result<Self, Error> {
    let mut body = BodyError(String::new());

    let mut map_len = rmp::decode::read_map_len(reader)?;

    while map_len > 0 {

      let raw_field: u64 = rmp::decode::read_int(reader)?;
      let field: Field = FromPrimitive::from_u64(raw_field)
        .ok_or(Error::UnexpectedField(raw_field))?;

      match field {
        Field::Error => {
          let str_len = rmp::decode::read_str_len(reader)?;
          body.0 = String::with_capacity(str_len as usize);
          reader.read_to_string(&mut body.0)?;
        },
        _ => return Err(Error::UnexpectedField(raw_field)),
      }

        map_len -= 1;
    }

    Ok(body)
  }
}

#[derive(Debug)]
pub struct SimpleBody {
  pub data: Vec<u8>,
}

impl SimpleBody {
  fn unpack<T: DeserializeOwned>(self) -> Result<T, rmp_serde::decode::Error> {
    let cursor = Cursor::new(self.data.as_slice());
    rmp_serde::decode::from_read(cursor)
  }
}

impl<R> Parse<R> for SimpleBody where R: Read {
  fn parse(reader: &mut R) -> Result<Self, Error> {
    let mut data =  Vec::new();

    let map_len = rmp::decode::read_map_len(reader)?;

    if map_len != 1 {
      return Err(Error::ParseError(NumValueReadError::InvalidDataRead(
        io::Error::new(io::ErrorKind::Other, "expected 1 field")
      )));
    }

    let raw_field: u64 = rmp::decode::read_int(reader)?;
    let field: Field = FromPrimitive::from_u64(raw_field)
      .ok_or(Error::UnexpectedField(raw_field))?;

    match field {
      Field::Data => {
        reader.read_to_end(&mut data)?;
      },
      _ => return Err(Error::UnexpectedField(raw_field)),
    }

    Ok(SimpleBody { data })
  }
}

#[derive(Debug)]
pub struct Response<Body, R>
  where
    R: Read,
    Body: ResponseBody<R>,
{
  pub header: ResponseHeader,
  pub body: Body,

  phantom: PhantomData<R>,
}

impl<Body, R> Response<Body, Take<R>>
  where
    R: Read,
    Body: ResponseBody<Take<R>>,
{
  fn parse(mut reader: R) -> Result<Self, Error> {
    let size: u64 = rmp::decode::read_int(&mut reader)?;
    let mut reader = reader.take(size);

    let header: ResponseHeader = Parse::parse(&mut reader)?;

    if header.code.is_err() {
      let err: BodyError = Parse::parse(&mut reader)?;
      return Err(Error::TarantoolError(header.code, err.0));
    }

    let body: Body = Parse::parse(&mut reader)?;

    Ok(Response { header, body, phantom: PhantomData })
  }
}

pub type SimpleResponse<R> = Response<SimpleBody, R>;


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
      let buf = [12, 131, 0, 0, 1, 123, 5, 123, 129, 48, 145, 145, 1];
      let resp = SimpleResponse::parse(&buf[..]).unwrap();
      let tuple: Vec<Vec<u64>> = dbg!(resp).body.unpack().unwrap();
      dbg!(tuple);
    }

    #[test]
    fn it_works_err() {
      let buf = [
        31, 131, 0, 205, 128, 0, 1, 123, 5, 123, 129, 82, 179, 115, 111, 109, 101,
        32, 101, 114, 114, 111, 114, 32, 111, 99, 99, 117, 114, 114, 101, 100,
      ];
      let err = SimpleResponse::parse(&buf[..]).err().unwrap();
      dbg!(err);
    }
}
