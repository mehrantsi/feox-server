use super::{BroadcastMsg, PubSubStats, ThreadId};
use crossbeam_channel::{bounded, Receiver, Sender};
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct GlobalRegistry {
    channel_to_threads: DashMap<Vec<u8>, HashSet<ThreadId>>,
    pattern_to_threads: DashMap<Vec<u8>, HashSet<ThreadId>>,
    channel_subscriber_counts: DashMap<Vec<u8>, usize>,
    pattern_subscriber_counts: DashMap<Vec<u8>, usize>,
    thread_channels: Vec<Sender<BroadcastMsg>>,
    pub stats: Arc<PubSubStats>,
}

impl GlobalRegistry {
    pub fn new(num_threads: usize) -> (Arc<Self>, Vec<Receiver<BroadcastMsg>>) {
        let mut senders = Vec::with_capacity(num_threads);
        let mut receivers = Vec::with_capacity(num_threads);

        for _ in 0..num_threads {
            let (tx, rx) = bounded(1024);
            senders.push(tx);
            receivers.push(rx);
        }

        let registry = Arc::new(Self {
            channel_to_threads: DashMap::new(),
            pattern_to_threads: DashMap::new(),
            channel_subscriber_counts: DashMap::new(),
            pattern_subscriber_counts: DashMap::new(),
            thread_channels: senders,
            stats: Arc::new(PubSubStats::new()),
        });

        (registry, receivers)
    }

    pub fn add_channel_interest(&self, channel: Vec<u8>, thread_id: ThreadId) {
        let mut entry = self.channel_to_threads.entry(channel).or_default();
        if entry.insert(thread_id) {
            self.stats.total_channels.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn remove_channel_interest(&self, channel: &[u8], thread_id: ThreadId) {
        if let Some(mut entry) = self.channel_to_threads.get_mut(channel) {
            if entry.remove(&thread_id) {
                self.stats.total_channels.fetch_sub(1, Ordering::Relaxed);
            }
            if entry.is_empty() {
                drop(entry);
                self.channel_to_threads.remove(channel);
            }
        }
    }

    pub fn add_pattern_interest(&self, pattern: Vec<u8>, thread_id: ThreadId) {
        let mut entry = self.pattern_to_threads.entry(pattern).or_default();
        if entry.insert(thread_id) {
            self.stats.total_patterns.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn remove_pattern_interest(&self, pattern: &[u8], thread_id: ThreadId) {
        if let Some(mut entry) = self.pattern_to_threads.get_mut(pattern) {
            if entry.remove(&thread_id) {
                self.stats.total_patterns.fetch_sub(1, Ordering::Relaxed);
            }
            if entry.is_empty() {
                drop(entry);
                self.pattern_to_threads.remove(pattern);
            }
        }
    }

    pub fn get_channel_threads(&self, channel: &[u8]) -> Vec<ThreadId> {
        self.channel_to_threads
            .get(channel)
            .map(|entry| entry.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn get_all_pattern_threads(&self) -> HashSet<ThreadId> {
        let mut threads = HashSet::new();
        for entry in self.pattern_to_threads.iter() {
            threads.extend(entry.value().iter().copied());
        }
        threads
    }

    pub fn broadcast_to_threads(&self, msg: BroadcastMsg, thread_ids: &[ThreadId]) {
        self.stats.total_messages.fetch_add(1, Ordering::Relaxed);

        for &thread_id in thread_ids {
            if thread_id < self.thread_channels.len() {
                let _ = self.thread_channels[thread_id].try_send(msg.clone());
            }
        }
    }

    pub fn broadcast_to_all_threads(&self, msg: BroadcastMsg, exclude: Option<ThreadId>) {
        self.stats.total_messages.fetch_add(1, Ordering::Relaxed);

        for (thread_id, sender) in self.thread_channels.iter().enumerate() {
            if Some(thread_id) != exclude {
                let _ = sender.try_send(msg.clone());
            }
        }
    }

    pub fn get_all_channels(&self) -> Vec<Vec<u8>> {
        self.channel_to_threads
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    pub fn get_channel_subscriber_count(&self, channel: &[u8]) -> usize {
        self.channel_subscriber_counts
            .get(channel)
            .map(|entry| *entry)
            .unwrap_or(0)
    }

    pub fn increment_channel_subscribers(&self, channel: &[u8]) {
        *self
            .channel_subscriber_counts
            .entry(channel.to_vec())
            .or_insert(0) += 1;
    }

    pub fn decrement_channel_subscribers(&self, channel: &[u8]) {
        if let Some(mut count) = self.channel_subscriber_counts.get_mut(channel) {
            if *count > 0 {
                *count -= 1;
            }
            if *count == 0 {
                drop(count);
                self.channel_subscriber_counts.remove(channel);
            }
        }
    }

    pub fn increment_pattern_subscribers(&self, pattern: &[u8]) {
        *self
            .pattern_subscriber_counts
            .entry(pattern.to_vec())
            .or_insert(0) += 1;
    }

    pub fn decrement_pattern_subscribers(&self, pattern: &[u8]) {
        if let Some(mut count) = self.pattern_subscriber_counts.get_mut(pattern) {
            if *count > 0 {
                *count -= 1;
            }
            if *count == 0 {
                drop(count);
                self.pattern_subscriber_counts.remove(pattern);
            }
        }
    }

    pub fn get_pattern_subscriber_count(&self, pattern: &[u8]) -> usize {
        self.pattern_subscriber_counts
            .get(pattern)
            .map(|entry| *entry)
            .unwrap_or(0)
    }

    pub fn get_total_pattern_matches(&self, channel: &[u8]) -> usize {
        // Count all pattern subscribers that might match this channel
        // For simplicity, count all pattern subscribers since we can't check patterns here
        let mut total = 0;
        for entry in self.pattern_subscriber_counts.iter() {
            // Check if pattern matches channel using glob-style matching
            if Self::glob_match(entry.key(), channel) {
                total += *entry.value();
            }
        }
        total
    }

    pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
        let mut p = 0;
        let mut t = 0;
        let mut star_idx = None;
        let mut match_idx = 0;

        while t < text.len() {
            if p < pattern.len() && (pattern[p] == text[t] || pattern[p] == b'?') {
                p += 1;
                t += 1;
            } else if p < pattern.len() && pattern[p] == b'*' {
                star_idx = Some(p);
                match_idx = t;
                p += 1;
            } else if let Some(idx) = star_idx {
                p = idx + 1;
                match_idx += 1;
                t = match_idx;
            } else {
                return false;
            }
        }

        while p < pattern.len() && pattern[p] == b'*' {
            p += 1;
        }

        p == pattern.len()
    }

    pub fn get_pattern_count(&self) -> usize {
        self.pattern_to_threads.len()
    }
}
