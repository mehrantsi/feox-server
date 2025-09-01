use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("FeOxDB error: {0}")]
    Database(#[from] feoxdb::FeoxError),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Connection closed")]
    ConnectionClosed,

    #[error("Buffer overflow")]
    BufferOverflow,

    #[error("Invalid command: {0}")]
    InvalidCommand(String),

    #[error("Wrong number of arguments for '{0}' command")]
    WrongArity(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Server shutting down")]
    Shutdown,

    #[error("System error: {0}")]
    System(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Convert error to Redis error response
    pub fn to_resp(&self) -> Vec<u8> {
        match self {
            Error::Database(feoxdb::FeoxError::KeyNotFound) => {
                b"$-1\r\n".to_vec() // Redis nil response
            }
            Error::WrongArity(cmd) => {
                format!("-ERR wrong number of arguments for '{}' command\r\n", cmd).into_bytes()
            }
            Error::InvalidCommand(cmd) => {
                format!("-ERR unknown command '{}'\r\n", cmd).into_bytes()
            }
            _ => format!("-ERR {}\r\n", self).into_bytes(),
        }
    }
}
