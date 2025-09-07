use super::{BroadcastMsg, ConnectionId, GlobalRegistry, PatternTrie, PubSubMessage, ThreadId};
use bytes::Bytes;
use crossbeam_channel::Receiver;
use std::collections::HashMap;
use std::sync::Arc;

pub struct ThreadLocalPubSub {
    thread_id: ThreadId,
    exact_subs: HashMap<Vec<u8>, Vec<ConnectionId>>,
    pattern_trie: PatternTrie,
    pattern_subs: HashMap<Vec<u8>, Vec<ConnectionId>>,
    inbox: Receiver<BroadcastMsg>,
    registry: Arc<GlobalRegistry>,
    connection_channels: HashMap<ConnectionId, Vec<Vec<u8>>>,
    connection_patterns: HashMap<ConnectionId, Vec<Vec<u8>>>,
}

impl ThreadLocalPubSub {
    pub fn new(
        thread_id: ThreadId,
        inbox: Receiver<BroadcastMsg>,
        registry: Arc<GlobalRegistry>,
    ) -> Self {
        Self {
            thread_id,
            exact_subs: HashMap::new(),
            pattern_trie: PatternTrie::new(),
            pattern_subs: HashMap::new(),
            inbox,
            registry,
            connection_channels: HashMap::new(),
            connection_patterns: HashMap::new(),
        }
    }

    pub fn subscribe(
        &mut self,
        conn_id: ConnectionId,
        channels: Vec<Vec<u8>>,
    ) -> Vec<PubSubMessage> {
        let mut messages = Vec::new();

        for channel in channels {
            let is_new_channel = !self.exact_subs.contains_key(&channel);
            let already_subscribed = self
                .exact_subs
                .get(&channel)
                .map(|subs| subs.contains(&conn_id))
                .unwrap_or(false);

            if !already_subscribed {
                self.exact_subs
                    .entry(channel.clone())
                    .or_default()
                    .push(conn_id);
                self.connection_channels
                    .entry(conn_id)
                    .or_default()
                    .push(channel.clone());

                // Increment global subscriber count
                self.registry.increment_channel_subscribers(&channel);
            }

            if is_new_channel {
                self.registry
                    .add_channel_interest(channel.clone(), self.thread_id);
            }

            let channel_count = self
                .connection_channels
                .get(&conn_id)
                .map(|c| c.len())
                .unwrap_or(0);
            let pattern_count = self
                .connection_patterns
                .get(&conn_id)
                .map(|p| p.len())
                .unwrap_or(0);
            let total_count = channel_count + pattern_count;

            messages.push(PubSubMessage::Subscribe {
                channel,
                count: total_count,
            });
        }

        messages
    }

    pub fn unsubscribe(
        &mut self,
        conn_id: ConnectionId,
        channels: Option<Vec<Vec<u8>>>,
    ) -> Vec<PubSubMessage> {
        let mut messages = Vec::new();

        if let Some(channels) = channels {
            for channel in channels {
                let mut should_remove_interest = false;

                if let Some(subs) = self.exact_subs.get_mut(&channel) {
                    let was_subscribed = subs.contains(&conn_id);
                    subs.retain(|&id| id != conn_id);
                    if was_subscribed {
                        self.registry.decrement_channel_subscribers(&channel);
                    }
                    if subs.is_empty() {
                        self.exact_subs.remove(&channel);
                        should_remove_interest = true;
                    }
                }

                if let Some(conn_channels) = self.connection_channels.get_mut(&conn_id) {
                    conn_channels.retain(|c| c != &channel);
                }

                if should_remove_interest {
                    self.registry
                        .remove_channel_interest(&channel, self.thread_id);
                }

                let total_count = self.get_connection_subscription_count(conn_id);
                messages.push(PubSubMessage::Unsubscribe {
                    channel: Some(channel),
                    count: total_count,
                });
            }
        } else if let Some(conn_channels) = self.connection_channels.remove(&conn_id) {
            for channel in conn_channels {
                if let Some(subs) = self.exact_subs.get_mut(&channel) {
                    let was_subscribed = subs.contains(&conn_id);
                    subs.retain(|&id| id != conn_id);
                    if was_subscribed {
                        self.registry.decrement_channel_subscribers(&channel);
                    }
                    if subs.is_empty() {
                        self.exact_subs.remove(&channel);
                        self.registry
                            .remove_channel_interest(&channel, self.thread_id);
                    }
                }

                let total_count = self.get_connection_subscription_count(conn_id);
                messages.push(PubSubMessage::Unsubscribe {
                    channel: Some(channel),
                    count: total_count,
                });
            }
        }

        messages
    }

    pub fn psubscribe(
        &mut self,
        conn_id: ConnectionId,
        patterns: Vec<Vec<u8>>,
    ) -> Vec<PubSubMessage> {
        let mut messages = Vec::new();

        for pattern in patterns {
            let is_new_pattern = !self.pattern_subs.contains_key(&pattern);
            let already_subscribed = self
                .pattern_subs
                .get(&pattern)
                .map(|subs| subs.contains(&conn_id))
                .unwrap_or(false);

            if !already_subscribed {
                self.pattern_trie.insert(&pattern, conn_id);
                self.pattern_subs
                    .entry(pattern.clone())
                    .or_default()
                    .push(conn_id);
                self.connection_patterns
                    .entry(conn_id)
                    .or_default()
                    .push(pattern.clone());

                // Increment global pattern subscriber count
                self.registry.increment_pattern_subscribers(&pattern);
            }

            if is_new_pattern {
                self.registry
                    .add_pattern_interest(pattern.clone(), self.thread_id);
            }

            let channel_count = self
                .connection_channels
                .get(&conn_id)
                .map(|c| c.len())
                .unwrap_or(0);
            let pattern_count = self
                .connection_patterns
                .get(&conn_id)
                .map(|p| p.len())
                .unwrap_or(0);
            let total_count = channel_count + pattern_count;

            messages.push(PubSubMessage::PSubscribe {
                pattern,
                count: total_count,
            });
        }

        messages
    }

    pub fn punsubscribe(
        &mut self,
        conn_id: ConnectionId,
        patterns: Option<Vec<Vec<u8>>>,
    ) -> Vec<PubSubMessage> {
        let mut messages = Vec::new();

        if let Some(patterns) = patterns {
            for pattern in patterns {
                let mut should_remove_interest = false;

                self.pattern_trie.remove(&pattern, conn_id);

                if let Some(subs) = self.pattern_subs.get_mut(&pattern) {
                    let was_subscribed = subs.contains(&conn_id);
                    subs.retain(|&id| id != conn_id);
                    if was_subscribed {
                        self.registry.decrement_pattern_subscribers(&pattern);
                    }
                    if subs.is_empty() {
                        self.pattern_subs.remove(&pattern);
                        should_remove_interest = true;
                    }
                }

                if let Some(conn_patterns) = self.connection_patterns.get_mut(&conn_id) {
                    conn_patterns.retain(|p| p != &pattern);
                }

                if should_remove_interest {
                    self.registry
                        .remove_pattern_interest(&pattern, self.thread_id);
                }

                let total_count = self.get_connection_subscription_count(conn_id);
                messages.push(PubSubMessage::PUnsubscribe {
                    pattern: Some(pattern),
                    count: total_count,
                });
            }
        } else if let Some(conn_patterns) = self.connection_patterns.remove(&conn_id) {
            for pattern in conn_patterns {
                self.pattern_trie.remove(&pattern, conn_id);

                if let Some(subs) = self.pattern_subs.get_mut(&pattern) {
                    let was_subscribed = subs.contains(&conn_id);
                    subs.retain(|&id| id != conn_id);
                    if was_subscribed {
                        self.registry.decrement_pattern_subscribers(&pattern);
                    }
                    if subs.is_empty() {
                        self.pattern_subs.remove(&pattern);
                        self.registry
                            .remove_pattern_interest(&pattern, self.thread_id);
                    }
                }

                let total_count = self.get_connection_subscription_count(conn_id);
                messages.push(PubSubMessage::PUnsubscribe {
                    pattern: Some(pattern),
                    count: total_count,
                });
            }
        }

        messages
    }

    pub fn publish_local(
        &self,
        channel: &[u8],
        message: &Bytes,
    ) -> Vec<(ConnectionId, PubSubMessage)> {
        let mut deliveries = Vec::new();

        if let Some(subs) = self.exact_subs.get(channel) {
            for &conn_id in subs {
                deliveries.push((
                    conn_id,
                    PubSubMessage::Message {
                        channel: channel.to_vec(),
                        payload: message.clone(),
                    },
                ));
            }
        }

        let pattern_matches = self.pattern_trie.find_matches(channel);
        for (pattern, conn_id) in pattern_matches {
            deliveries.push((
                conn_id,
                PubSubMessage::PatternMessage {
                    pattern,
                    channel: channel.to_vec(),
                    payload: message.clone(),
                },
            ));
        }

        deliveries
    }

    pub fn process_inbox(&mut self) -> Vec<(ConnectionId, PubSubMessage)> {
        let mut deliveries = Vec::new();

        while let Ok(msg) = self.inbox.try_recv() {
            match msg {
                BroadcastMsg::Publish {
                    channel,
                    message,
                    exclude_thread,
                } => {
                    if Some(self.thread_id) != exclude_thread {
                        let local_deliveries = self.publish_local(&channel, &message);
                        deliveries.extend(local_deliveries);
                    }
                }
                BroadcastMsg::PatternPublish {
                    channel,
                    message,
                    exclude_thread,
                } => {
                    if Some(self.thread_id) != exclude_thread {
                        let pattern_matches = self.pattern_trie.find_matches(&channel);
                        for (pattern, conn_id) in pattern_matches {
                            deliveries.push((
                                conn_id,
                                PubSubMessage::PatternMessage {
                                    pattern,
                                    channel: channel.clone(),
                                    payload: message.clone(),
                                },
                            ));
                        }
                    }
                }
            }
        }

        deliveries
    }

    pub fn connection_dropped(&mut self, conn_id: ConnectionId) {
        self.unsubscribe(conn_id, None);
        self.punsubscribe(conn_id, None);
    }

    pub fn get_connection_subscription_count(&self, conn_id: ConnectionId) -> usize {
        let channel_count = self
            .connection_channels
            .get(&conn_id)
            .map(|c| c.len())
            .unwrap_or(0);
        let pattern_count = self
            .connection_patterns
            .get(&conn_id)
            .map(|p| p.len())
            .unwrap_or(0);
        channel_count + pattern_count
    }

    pub fn is_connection_subscribed(&self, conn_id: ConnectionId) -> bool {
        self.connection_channels.contains_key(&conn_id)
            || self.connection_patterns.contains_key(&conn_id)
    }

    pub fn get_all_channels(&self) -> Vec<Vec<u8>> {
        self.exact_subs.keys().cloned().collect()
    }

    pub fn get_all_patterns(&self) -> Vec<Vec<u8>> {
        self.pattern_subs.keys().cloned().collect()
    }
}
