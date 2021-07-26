use std::{error, fmt::{Display, Debug}, io};
use rmp::{decode::{NumValueReadError, ValueReadError}, encode::ValueWriteError};
use crate::iproto::constants::Field;

use super::{Code, response::TarantoolError};

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
