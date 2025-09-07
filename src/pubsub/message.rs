use super::ThreadId;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub enum BroadcastMsg {
    Publish {
        channel: Vec<u8>,
        message: Bytes,
        exclude_thread: Option<ThreadId>,
    },
    PatternPublish {
        channel: Vec<u8>,
        message: Bytes,
        exclude_thread: Option<ThreadId>,
    },
}

#[derive(Debug, Clone)]
pub enum PubSubMessage {
    Message {
        channel: Vec<u8>,
        payload: Bytes,
    },
    PatternMessage {
        pattern: Vec<u8>,
        channel: Vec<u8>,
        payload: Bytes,
    },
    Subscribe {
        channel: Vec<u8>,
        count: usize,
    },
    Unsubscribe {
        channel: Option<Vec<u8>>,
        count: usize,
    },
    PSubscribe {
        pattern: Vec<u8>,
        count: usize,
    },
    PUnsubscribe {
        pattern: Option<Vec<u8>>,
        count: usize,
    },
}

impl PubSubMessage {
    pub fn to_resp(&self) -> crate::protocol::resp::RespValue {
        use crate::protocol::resp::RespValue;

        match self {
            PubSubMessage::Message { channel, payload } => RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"message"))),
                RespValue::BulkString(Some(Bytes::from(channel.clone()))),
                RespValue::BulkString(Some(payload.clone())),
            ])),
            PubSubMessage::PatternMessage {
                pattern,
                channel,
                payload,
            } => RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"pmessage"))),
                RespValue::BulkString(Some(Bytes::from(pattern.clone()))),
                RespValue::BulkString(Some(Bytes::from(channel.clone()))),
                RespValue::BulkString(Some(payload.clone())),
            ])),
            PubSubMessage::Subscribe { channel, count } => RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"subscribe"))),
                RespValue::BulkString(Some(Bytes::from(channel.clone()))),
                RespValue::Integer(*count as i64),
            ])),
            PubSubMessage::Unsubscribe { channel, count } => RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"unsubscribe"))),
                RespValue::BulkString(channel.as_ref().map(|c| Bytes::from(c.clone()))),
                RespValue::Integer(*count as i64),
            ])),
            PubSubMessage::PSubscribe { pattern, count } => RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"psubscribe"))),
                RespValue::BulkString(Some(Bytes::from(pattern.clone()))),
                RespValue::Integer(*count as i64),
            ])),
            PubSubMessage::PUnsubscribe { pattern, count } => RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"punsubscribe"))),
                RespValue::BulkString(pattern.as_ref().map(|p| Bytes::from(p.clone()))),
                RespValue::Integer(*count as i64),
            ])),
        }
    }
}
