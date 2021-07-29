use std::{error, fmt::{Display, Debug}, io};
use rmp::{decode::{NumValueReadError, ValueReadError}, encode::ValueWriteError};
use crate::iproto::constants::Field;

use super::{constants::Code, response::TarantoolError};

/// pack/unpack and tarantool errors.
#[allow(dead_code)]
pub enum Error {
  ParseError(rmp_serde::decode::Error),
  PackError(ValueWriteError),
  UnexpectedField(u64),
  UnexpectedValue(Field),
  TarantoolError(Code, TarantoolError),
}

impl error::Error for Error {}

impl Display for Error {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::ParseError(err) =>
        Display::fmt(err, f),
      Self::PackError(err) =>
        Display::fmt(err, f),
      &Self::UnexpectedValue(field) =>
        write!(f, "unexpected value for field {:?}", field),
      &Self::UnexpectedField(field) =>
        write!(f, "unexpected field {}", field),
      Self::TarantoolError(code, err) =>
        write!(f, "TarantoolError(code={:?}, err={:?})", code, err),
    }
  }
}

impl Debug for Error {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    Display::fmt(&self, f)
  }
}

impl From<io::Error> for Error {
  fn from(err: io::Error) -> Self {
      Error::ParseError(rmp_serde::decode::Error::InvalidDataRead(err))
  }
}

impl From<NumValueReadError> for Error {
    fn from(err: NumValueReadError) -> Self {
        Error::ParseError(err.into())
    }
}

impl From<ValueReadError> for Error {
  fn from(err: ValueReadError) -> Self {
      Error::ParseError(err.into())
  }
}

impl From<rmpv::decode::Error> for Error {
  fn from(err: rmpv::decode::Error) -> Self {
      let err: io::Error = err.into();
      err.into()
  }
}

impl From<ValueWriteError> for Error {
  fn from(err: ValueWriteError) -> Self {
      Error::PackError(err)
  }
}


#[cfg(test)]
mod test {
  use super::*;

  #[test]
  fn test_display() {
    let err = Error::ParseError(
      rmp_serde::decode::Error::DepthLimitExceeded
    );
    assert_eq!(err.to_string(), "depth limit exceeded");

    let err: Error = ValueWriteError::InvalidDataWrite(std::io::Error::new(
      std::io::ErrorKind::Other,
      "some error",
    )).into();
    assert!(err.to_string().contains("error while writing"));

    let err: Error = Error::UnexpectedField(123);
    assert!(err.to_string().contains("unexpected field"));

    let err: Error = Error::UnexpectedValue(Field::Data);
    assert!(err.to_string().contains("unexpected value for field"));

    let err: Error = Error::TarantoolError(Code::ErrorAccessDenied, TarantoolError{
        message: "error".into(),
        stack: Vec::new(),
    });
    assert!(err.to_string().contains("ErrorAccessDenied"));
  }
}
