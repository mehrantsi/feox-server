use crate::error::{Error, Result};
use bytes::Bytes;
use feoxdb::FeoxStore;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use once_cell::sync::Lazy;

struct MetadataTracker {
    pending_updates: HashMap<Vec<u8>, i64>,
    last_flush: Instant,
    flush_interval: Duration,
    max_batch_size: usize,
}

impl MetadataTracker {
    fn new() -> Self {
        Self {
            pending_updates: HashMap::new(),
            last_flush: Instant::now(),
            flush_interval: Duration::from_millis(100),
            max_batch_size: 1000,
        }
    }

    fn add_update(&mut self, key: Vec<u8>, delta: i64) {
        *self.pending_updates.entry(key).or_insert(0) += delta;
    }

    fn should_flush(&self) -> bool {
        self.pending_updates.len() >= self.max_batch_size
            || self.last_flush.elapsed() >= self.flush_interval
    }

    fn take_updates(&mut self) -> HashMap<Vec<u8>, i64> {
        self.last_flush = Instant::now();
        std::mem::take(&mut self.pending_updates)
    }
}

static GLOBAL_METADATA_TRACKER: Lazy<Arc<RwLock<MetadataTracker>>> = Lazy::new(|| {
    Arc::new(RwLock::new(MetadataTracker::new()))
});

#[derive(Clone)]
pub struct HashOperations {
    store: Arc<FeoxStore>,
}

impl HashOperations {
    pub fn new(store: Arc<FeoxStore>) -> Self {
        Self { store }
    }

    fn flush_metadata(&self) {
        let mut tracker = GLOBAL_METADATA_TRACKER.write().unwrap();
        let updates = tracker.take_updates();

        for (meta_key, delta) in updates {
            if delta != 0 {
                self.store.atomic_increment(&meta_key, delta).ok();
            }
        }
    }

    fn maybe_flush_metadata(&self) {
        let should_flush = GLOBAL_METADATA_TRACKER.read().unwrap().should_flush();
        if should_flush {
            self.flush_metadata();
        }
    }

    fn parse_metadata(data: &[u8]) -> i64 {
        if data.len() < 8 {
            return 0;
        }
        i64::from_le_bytes(data[0..8].try_into().unwrap())
    }

    pub fn hset<'a>(&self, key: &[u8], fields: impl Iterator<Item = (&'a [u8], Bytes)>) -> Result<i64> {
        let mut new_fields_count = 0i64;
        let mut prefix = Vec::with_capacity(key.len() + 5);
        prefix.extend_from_slice(b"H:");
        prefix.extend_from_slice(key);
        prefix.extend_from_slice(b":f:");
        let prefix_len = prefix.len();

        for (field, value) in fields {
            prefix.truncate(prefix_len);
            prefix.extend_from_slice(field);

            let is_new = self.store.insert_bytes(&prefix, value)?;
            if is_new {
                new_fields_count += 1;
            }
        }

        if new_fields_count > 0 {
            let mut meta_key = Vec::with_capacity(key.len() + 7);
            meta_key.extend_from_slice(b"H:");
            meta_key.extend_from_slice(key);
            meta_key.extend_from_slice(b":meta");

            GLOBAL_METADATA_TRACKER.write().unwrap().add_update(meta_key, new_fields_count);
            self.maybe_flush_metadata();
        }

        Ok(new_fields_count)
    }

    pub fn hget(&self, key: &[u8], field: &[u8]) -> Result<Option<Bytes>> {
        let mut field_key = Vec::with_capacity(key.len() + field.len() + 5);
        field_key.extend_from_slice(b"H:");
        field_key.extend_from_slice(key);
        field_key.extend_from_slice(b":f:");
        field_key.extend_from_slice(field);

        match self.store.get_bytes(&field_key) {
            Ok(value) => Ok(Some(value)),
            Err(_) => Ok(None),
        }
    }

    pub fn hmget(&self, key: &[u8], fields: Vec<Vec<u8>>) -> Result<Vec<Option<Bytes>>> {
        let mut results = Vec::with_capacity(fields.len());

        for field in fields {
            results.push(self.hget(key, &field)?);
        }

        Ok(results)
    }

    pub fn hdel(&self, key: &[u8], fields: Vec<Vec<u8>>) -> Result<i64> {
        if fields.is_empty() {
            return Ok(0);
        }

        let mut meta_key = Vec::with_capacity(key.len() + 7);
        meta_key.extend_from_slice(b"H:");
        meta_key.extend_from_slice(key);
        meta_key.extend_from_slice(b":meta");

        let mut deleted_count = 0i64;
        let mut field_key = Vec::with_capacity(key.len() + 37);

        for field in fields {
            field_key.clear();
            field_key.extend_from_slice(b"H:");
            field_key.extend_from_slice(key);
            field_key.extend_from_slice(b":f:");
            field_key.extend_from_slice(&field);

            if self.store.delete(&field_key).is_ok() {
                deleted_count += 1;
            }
        }

        if deleted_count > 0 {
            GLOBAL_METADATA_TRACKER.write().unwrap().add_update(meta_key, -deleted_count);
            self.maybe_flush_metadata();
        }

        Ok(deleted_count)
    }

    pub fn hexists(&self, key: &[u8], field: &[u8]) -> Result<bool> {
        let mut field_key = Vec::with_capacity(key.len() + field.len() + 5);
        field_key.extend_from_slice(b"H:");
        field_key.extend_from_slice(key);
        field_key.extend_from_slice(b":f:");
        field_key.extend_from_slice(field);
        Ok(self.store.contains_key(&field_key))
    }

    pub fn hgetall(&self, key: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let mut prefix = Vec::with_capacity(key.len() + 5);
        prefix.extend_from_slice(b"H:");
        prefix.extend_from_slice(key);
        prefix.extend_from_slice(b":f:");
        let prefix_len = prefix.len();

        let start_key = prefix.clone();
        let mut end_key = prefix.clone();
        end_key.push(255);

        let mut results = Vec::new();

        match self.store.range_query(&start_key, &end_key, 10000) {
            Ok(pairs) => {
                for (field_key, value) in pairs {
                    if field_key.starts_with(&prefix) {
                        let field_name = field_key[prefix_len..].to_vec();
                        results.push((field_name, Bytes::from(value)));
                    }
                }
                Ok(results)
            }
            Err(e) => Err(Error::Database(e)),
        }
    }

    pub fn hlen(&self, key: &[u8]) -> Result<i64> {
        self.flush_metadata();

        let mut meta_key = Vec::with_capacity(key.len() + 7);
        meta_key.extend_from_slice(b"H:");
        meta_key.extend_from_slice(key);
        meta_key.extend_from_slice(b":meta");

        match self.store.get_bytes(&meta_key) {
            Ok(meta_bytes) => Ok(Self::parse_metadata(&meta_bytes)),
            Err(_) => Ok(0),
        }
    }

    pub fn hkeys(&self, key: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mut prefix = Vec::with_capacity(key.len() + 5);
        prefix.extend_from_slice(b"H:");
        prefix.extend_from_slice(key);
        prefix.extend_from_slice(b":f:");
        let prefix_len = prefix.len();

        let start_key = prefix.clone();
        let mut end_key = prefix.clone();
        end_key.push(255);

        let mut results = Vec::new();

        match self.store.range_query(&start_key, &end_key, 10000) {
            Ok(pairs) => {
                for (field_key, _) in pairs {
                    if field_key.starts_with(&prefix) {
                        let field_name = field_key[prefix_len..].to_vec();
                        results.push(field_name);
                    }
                }
                Ok(results)
            }
            Err(e) => Err(Error::Database(e)),
        }
    }

    pub fn hvals(&self, key: &[u8]) -> Result<Vec<Bytes>> {
        let mut prefix = Vec::with_capacity(key.len() + 5);
        prefix.extend_from_slice(b"H:");
        prefix.extend_from_slice(key);
        prefix.extend_from_slice(b":f:");

        let start_key = prefix.clone();
        let mut end_key = prefix.clone();
        end_key.push(255);

        let mut results = Vec::new();

        match self.store.range_query(&start_key, &end_key, 10000) {
            Ok(pairs) => {
                for (field_key, value) in pairs {
                    if field_key.starts_with(&prefix) {
                        results.push(Bytes::from(value));
                    }
                }
                Ok(results)
            }
            Err(e) => Err(Error::Database(e)),
        }
    }

    pub fn hincrby(&self, key: &[u8], field: &[u8], delta: i64) -> Result<i64> {
        let mut field_key = Vec::with_capacity(key.len() + field.len() + 5);
        field_key.extend_from_slice(b"H:");
        field_key.extend_from_slice(key);
        field_key.extend_from_slice(b":f:");
        field_key.extend_from_slice(field);

        let mut meta_key = Vec::with_capacity(key.len() + 7);
        meta_key.extend_from_slice(b"H:");
        meta_key.extend_from_slice(key);
        meta_key.extend_from_slice(b":meta");

        let field_exists = self.store.contains_key(&field_key);

        let new_value = if field_exists {
            match self.store.get_bytes(&field_key) {
                Ok(bytes) => {
                    // Try to parse as string integer first (Redis compatibility)
                    match std::str::from_utf8(&bytes) {
                        Ok(s) => match s.parse::<i64>() {
                            Ok(current) => current.saturating_add(delta),
                            Err(_) => {
                                return Err(Error::Protocol(
                                    "hash value is not an integer".to_string(),
                                ))
                            }
                        },
                        Err(_) => {
                            // Try as binary i64
                            if bytes.len() == 8 {
                                let current = i64::from_le_bytes(bytes[..8].try_into().unwrap());
                                current.saturating_add(delta)
                            } else {
                                return Err(Error::Protocol(
                                    "hash value is not an integer".to_string(),
                                ));
                            }
                        }
                    }
                }
                Err(_) => delta,
            }
        } else {
            GLOBAL_METADATA_TRACKER.write().unwrap().add_update(meta_key, 1);
            self.maybe_flush_metadata();
            delta
        };

        self.store
            .insert(&field_key, new_value.to_string().as_bytes())?;

        Ok(new_value)
    }
}
