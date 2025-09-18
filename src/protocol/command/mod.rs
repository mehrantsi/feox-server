use crate::protocol::resp::RespValue;
use bytes::Bytes;

mod client;
mod executor;
mod hash;
mod list;
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

    // Authentication
    Auth(Vec<u8>),

    // List commands
    LPush {
        key: Vec<u8>,
        values: Vec<Bytes>,
    },
    RPush {
        key: Vec<u8>,
        values: Vec<Bytes>,
    },
    LPop {
        key: Vec<u8>,
        count: Option<usize>,
    },
    RPop {
        key: Vec<u8>,
        count: Option<usize>,
    },
    LLen(Vec<u8>),
    LRange {
        key: Vec<u8>,
        start: i64,
        stop: i64,
    },
    LIndex {
        key: Vec<u8>,
        index: i64,
    },

    Subscribe(Vec<Vec<u8>>),
    Unsubscribe(Option<Vec<Vec<u8>>>),
    PSubscribe(Vec<Vec<u8>>),
    PUnsubscribe(Option<Vec<Vec<u8>>>),
    Publish {
        channel: Vec<u8>,
        message: Vec<u8>,
    },
    PubSub {
        subcommand: String,
        args: Vec<Vec<u8>>,
    },

    // Client management
    Client {
        subcommand: String,
        args: Vec<Vec<u8>>,
    },

    // Transaction commands
    Multi,
    Exec,
    Discard,
    Watch(Vec<Vec<u8>>),
    Unwatch,

    // Hash commands
    HSet {
        key: Vec<u8>,
        fields: Vec<(Vec<u8>, Bytes)>,
    },
    HGet {
        key: Vec<u8>,
        field: Vec<u8>,
    },
    HMGet {
        key: Vec<u8>,
        fields: Vec<Vec<u8>>,
    },
    HDel {
        key: Vec<u8>,
        fields: Vec<Vec<u8>>,
    },
    HExists {
        key: Vec<u8>,
        field: Vec<u8>,
    },
    HGetAll(Vec<u8>),
    HLen(Vec<u8>),
    HKeys(Vec<u8>),
    HVals(Vec<u8>),
    HIncrBy {
        key: Vec<u8>,
        field: Vec<u8>,
        delta: i64,
    },
}

impl Command {
    /// Parse command from RESP array
    #[inline(always)]
    pub fn from_resp(value: RespValue) -> Result<Self, String> {
        parser::parse_command(value)
    }

    /// Check if this is a pub/sub command
    pub fn is_pubsub_command(&self) -> bool {
        matches!(
            self,
            Command::Subscribe(_)
                | Command::Unsubscribe(_)
                | Command::PSubscribe(_)
                | Command::PUnsubscribe(_)
                | Command::Publish { .. }
                | Command::PubSub { .. }
        )
    }

    /// Check if this command is allowed in pub/sub mode
    pub fn is_allowed_in_pubsub_mode(&self) -> bool {
        matches!(
            self,
            Command::Subscribe(_)
                | Command::Unsubscribe(_)
                | Command::PSubscribe(_)
                | Command::PUnsubscribe(_)
                | Command::Ping(_)
                | Command::Quit
        )
    }

    /// Convert to PubSubOp if this is a pub/sub command
    pub fn to_pubsub_op(self) -> Option<crate::network::PubSubOp> {
        match self {
            Command::Subscribe(channels) => Some(crate::network::PubSubOp::Subscribe(channels)),
            Command::Unsubscribe(channels) => Some(crate::network::PubSubOp::Unsubscribe(channels)),
            Command::PSubscribe(patterns) => Some(crate::network::PubSubOp::PSubscribe(patterns)),
            Command::PUnsubscribe(patterns) => {
                Some(crate::network::PubSubOp::PUnsubscribe(patterns))
            }
            Command::Publish { channel, message } => {
                Some(crate::network::PubSubOp::Publish { channel, message })
            }
            Command::PubSub { subcommand, args } => match subcommand.to_uppercase().as_str() {
                "CHANNELS" => Some(crate::network::PubSubOp::PubSubChannels {
                    pattern: args.first().cloned(),
                }),
                "NUMSUB" => Some(crate::network::PubSubOp::PubSubNumSub { channels: args }),
                "NUMPAT" => Some(crate::network::PubSubOp::PubSubNumPat),
                _ => None,
            },
            _ => None,
        }
    }
}
