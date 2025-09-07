mod handler;
mod manager;
mod message;
mod pattern;
mod registry;

pub use handler::handle_pubsub_operation;
pub use manager::ThreadLocalPubSub;
pub use message::{BroadcastMsg, PubSubMessage};
pub use pattern::PatternTrie;
pub use registry::GlobalRegistry;

use std::sync::atomic::AtomicUsize;

pub type ThreadId = usize;
pub type ConnectionId = usize;

#[derive(Debug, Clone)]
pub enum SubscriptionType {
    Channel,
    Pattern,
}

pub struct PubSubStats {
    pub total_channels: AtomicUsize,
    pub total_patterns: AtomicUsize,
    pub total_messages: AtomicUsize,
}

impl PubSubStats {
    pub fn new() -> Self {
        Self {
            total_channels: AtomicUsize::new(0),
            total_patterns: AtomicUsize::new(0),
            total_messages: AtomicUsize::new(0),
        }
    }
}

impl Default for PubSubStats {
    fn default() -> Self {
        Self::new()
    }
}
