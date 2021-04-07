use quick_xml::Error as QxmlError;
use reqwest::header::InvalidHeaderName as HttpInvalidHeaderNameError;
use reqwest::header::InvalidHeaderValue as HttpInvalidHeaderValueError;
use reqwest::header::ToStrError;
use reqwest::Error as ReqwestError;
use std::io::Error as IoError;
use std::string::FromUtf8Error;
use std::{error::Error as StdError, str::ParseBoolError};

#[derive(Debug, Display)]
pub enum Error {
    Convert(ToStrError),
    Object(ObjectError),
    Io(IoError),
    String(FromUtf8Error),
    Reqwest(ReqwestError),
    Qxml(QxmlError),
    Http(HttpError),
    ParseBool(ParseBoolError),
}

#[derive(Debug, Display)]
pub enum HttpError {
    HttpInvalidHeaderValue(HttpInvalidHeaderValueError),
    HttpInvalidHeaderName(HttpInvalidHeaderNameError),
}

impl From<QxmlError> for Error {
    fn from(e: QxmlError) -> Error {
        Error::Qxml(e)
    }
}

impl From<IoError> for Error {
    fn from(e: IoError) -> Error {
        Error::Io(e)
    }
}

impl From<ReqwestError> for Error {
    fn from(e: ReqwestError) -> Error {
        Error::Reqwest(e)
    }
}

impl From<HttpInvalidHeaderValueError> for Error {
    fn from(e: HttpInvalidHeaderValueError) -> Error {
        Error::Http(HttpError::HttpInvalidHeaderValue(e))
    }
}

impl From<HttpInvalidHeaderNameError> for Error {
    fn from(e: HttpInvalidHeaderNameError) -> Error {
        Error::Http(HttpError::HttpInvalidHeaderName(e))
    }
}

impl From<FromUtf8Error> for Error {
    fn from(e: FromUtf8Error) -> Error {
        Error::String(e)
    }
}

impl From<ToStrError> for Error {
    fn from(e: ToStrError) -> Error {
        Error::Convert(e)
    }
}

impl From<ParseBoolError> for Error {
    fn from(e: ParseBoolError) -> Error {
        Error::ParseBool(e)
    }
}

#[derive(Debug, Display)]
pub enum ObjectError {
    #[display(fmt = "PUT ERROR: {}", msg)]
    PutError { msg: String },
    #[display(fmt = "GET ERROR: {}", msg)]
    GetError { msg: String },
    #[display(fmt = "COPY ERROR: {}", msg)]
    CopyError { msg: String },
    #[display(fmt = "DELETE ERROR: {}", msg)]
    DeleteError { msg: String },
    #[display(fmt = "HEAD ERROR: {}", msg)]
    HeadError { msg: String },
}

impl StdError for Error {}
