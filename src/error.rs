use std::string::ToString;

#[derive(Debug)]
pub struct Error(pub String);

pub fn _err<R>(str: &str) -> Result<R, Error> {
    Err(Error(str.into()))
}

pub fn fn_err(str: &str) -> impl Fn() -> Error + '_ {
    move || Error(str.into())
}

pub fn _io_err(str: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, str.to_string())
}

impl From<String> for Error {
    fn from(e: String) -> Error {
        Error(e)
    }
}

impl From<electrum_client::types::Error> for Error {
    fn from(_: electrum_client::types::Error) -> Error {
        Error("electrum_client::types::Error".to_string())
    }
}

macro_rules! impl_error {
    ( $from:ty ) => {
        impl std::convert::From<$from> for Error {
            fn from(err: $from) -> Self {
                Error(err.to_string())
            }
        }
    };
}

impl_error!(&str);
impl_error!(bitcoin::util::base58::Error);
impl_error!(sled::Error);
impl_error!(bitcoin::hashes::error::Error);
impl_error!(bitcoin::consensus::encode::Error);
impl_error!(bitcoin::util::bip32::Error);
impl_error!(std::array::TryFromSliceError);
