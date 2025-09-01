use crate::protocol::resp::RespValue;
use bytes::Bytes;

mod executor;
mod parser;

pub use executor::CommandExecutor;

#[derive(Debug, Clone)]
pub enum Command {
    // Basic commands
    Get(Vec<u8>),
    Set {
        key: Vec<u8>,
        value: Bytes,
        ex: Option<u64>,
        px: Option<u64>,
    },
    Del(Vec<Vec<u8>>),
    Exists(Vec<Vec<u8>>),

    // Atomic operations
    Incr(Vec<u8>),
    IncrBy {
        key: Vec<u8>,
        delta: i64,
    },
    Decr(Vec<u8>),
    DecrBy {
        key: Vec<u8>,
        delta: i64,
    },

    // TTL commands
    Expire {
        key: Vec<u8>,
        seconds: u64,
    },
    PExpire {
        key: Vec<u8>,
        milliseconds: u64,
    },
    Ttl(Vec<u8>),
    PTtl(Vec<u8>),
    Persist(Vec<u8>),

    // Bulk operations
    MGet(Vec<Vec<u8>>),
    MSet(Vec<(Vec<u8>, Bytes)>),

    // Server commands
    Ping(Option<Bytes>),
    Echo(Bytes),
    Info(Option<String>),
    Config {
        action: String,
        args: Vec<Bytes>,
    },
    Command,
    Quit,
    FlushDb,

    // Key scanning
    Keys(String), // Pattern
    Scan {
        cursor: Vec<u8>,
        count: usize,
        pattern: Option<String>,
    },

    // FeOx-specific
    JsonPatch {
        key: Vec<u8>,
        patch: Bytes,
    },
    Cas {
        key: Vec<u8>,
        expected: Bytes,
        new_value: Bytes,
    },
}

impl Command {
    /// Parse command from RESP array
    #[inline(always)]
    pub fn from_resp(value: RespValue) -> Result<Self, String> {
        parser::parse_command(value)
    }
}
