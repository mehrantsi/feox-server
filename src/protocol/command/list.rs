use crate::error::{Error, Result};
use bytes::Bytes;
use feoxdb::FeoxStore;
use std::sync::Arc;

const INITIAL_POSITION: i64 = 1_000_000_000;
const MAX_RETRIES: usize = 10;

pub struct ListOperations {
    store: Arc<FeoxStore>,
}

impl ListOperations {
    pub fn new(store: Arc<FeoxStore>) -> Self {
        Self { store }
    }

    fn parse_metadata(data: &[u8]) -> (i64, i64, u64) {
        if data.len() < 24 {
            return (INITIAL_POSITION, INITIAL_POSITION, 0);
        }
        let head = i64::from_le_bytes(data[0..8].try_into().unwrap());
        let tail = i64::from_le_bytes(data[8..16].try_into().unwrap());
        let count = u64::from_le_bytes(data[16..24].try_into().unwrap());
        (head, tail, count)
    }

    fn build_metadata(head: i64, tail: i64, count: u64) -> Vec<u8> {
        let mut meta = Vec::with_capacity(24);
        meta.extend_from_slice(&head.to_le_bytes());
        meta.extend_from_slice(&tail.to_le_bytes());
        meta.extend_from_slice(&count.to_le_bytes());
        meta
    }

    pub fn lpush(&self, key: &[u8], values: Vec<Bytes>) -> Result<i64> {
        let meta_key = format!("L:{}:meta", String::from_utf8_lossy(key));

        // Process values in order: LPUSH key a b c pushes a, then b, then c
        // Result should be [c, b, a] when read from head to tail
        for value in values {
            let mut retries = 0;
            loop {
                let meta_result = self.store.get_bytes(meta_key.as_bytes());
                let (meta_bytes, is_new) = match meta_result {
                    Ok(bytes) => (bytes, false),
                    Err(_) => {
                        // List doesn't exist, try to create it
                        let initial_meta =
                            Self::build_metadata(INITIAL_POSITION, INITIAL_POSITION, 0);
                        // Try to insert the initial metadata
                        match self.store.insert(meta_key.as_bytes(), &initial_meta) {
                            Ok(_) => (Bytes::from(initial_meta), true),
                            Err(_) => {
                                // Someone else created it, retry
                                continue;
                            }
                        }
                    }
                };

                let (head, tail, count) = Self::parse_metadata(&meta_bytes);
                let new_head = head - 1;
                let new_tail = if count == 0 { head } else { tail }; // For first item, tail = head
                let new_count = count + 1;
                let new_meta = Self::build_metadata(new_head, new_tail, new_count);

                // Try CAS update
                let cas_success = if is_new {
                    // For new list, just update directly since we just created it
                    self.store.insert(meta_key.as_bytes(), &new_meta).is_ok()
                } else {
                    self.store
                        .compare_and_swap(meta_key.as_bytes(), &meta_bytes, &new_meta)?
                };

                if cas_success {
                    // Now insert the value
                    let value_key = format!("L:{}:{}", String::from_utf8_lossy(key), new_head);
                    self.store.insert(value_key.as_bytes(), &value)?;
                    break;
                }

                // CAS failed, retry with limit
                retries += 1;
                if retries >= MAX_RETRIES {
                    return Err(Error::System(
                        "Operation failed due to contention".to_string(),
                    ));
                }
                std::thread::yield_now();
            }
        }

        // Return final count
        let meta_bytes = self.store.get_bytes(meta_key.as_bytes())?;
        let (_, _, count) = Self::parse_metadata(&meta_bytes);
        Ok(count as i64)
    }

    pub fn rpush(&self, key: &[u8], values: Vec<Bytes>) -> Result<i64> {
        let meta_key = format!("L:{}:meta", String::from_utf8_lossy(key));

        for value in values {
            let mut retries = 0;
            loop {
                let meta_result = self.store.get_bytes(meta_key.as_bytes());
                let (meta_bytes, is_new) = match meta_result {
                    Ok(bytes) => (bytes, false),
                    Err(_) => {
                        // List doesn't exist, try to create it
                        let initial_meta =
                            Self::build_metadata(INITIAL_POSITION, INITIAL_POSITION, 0);
                        // Try to insert the initial metadata
                        match self.store.insert(meta_key.as_bytes(), &initial_meta) {
                            Ok(_) => (Bytes::from(initial_meta), true),
                            Err(_) => {
                                // Someone else created it, retry
                                continue;
                            }
                        }
                    }
                };

                let (head, tail, count) = Self::parse_metadata(&meta_bytes);
                let insert_pos = tail;
                let new_head = if count == 0 { tail } else { head }; // For first item, head = tail
                let new_tail = tail + 1;
                let new_count = count + 1;
                let new_meta = Self::build_metadata(new_head, new_tail, new_count);

                // Try CAS update
                let cas_success = if is_new {
                    // For new list, just update directly since we just created it
                    self.store.insert(meta_key.as_bytes(), &new_meta).is_ok()
                } else {
                    self.store
                        .compare_and_swap(meta_key.as_bytes(), &meta_bytes, &new_meta)?
                };

                if cas_success {
                    // Insert at the position
                    let value_key = format!("L:{}:{}", String::from_utf8_lossy(key), insert_pos);
                    self.store.insert(value_key.as_bytes(), &value)?;
                    break;
                }

                retries += 1;
                if retries >= MAX_RETRIES {
                    return Err(Error::System(
                        "Operation failed due to contention".to_string(),
                    ));
                }
                std::thread::yield_now();
            }
        }

        let meta_bytes = self.store.get_bytes(meta_key.as_bytes())?;
        let (_, _, count) = Self::parse_metadata(&meta_bytes);
        Ok(count as i64)
    }

    pub fn lpop(&self, key: &[u8], count: Option<usize>) -> Result<Vec<Bytes>> {
        let meta_key = format!("L:{}:meta", String::from_utf8_lossy(key));
        let count = count.unwrap_or(1);
        let mut popped = Vec::new();

        for _ in 0..count {
            let mut retries = 0;
            loop {
                let meta_bytes = match self.store.get_bytes(meta_key.as_bytes()) {
                    Ok(bytes) => bytes,
                    Err(_) => return Ok(popped), // List doesn't exist
                };

                let (head, tail, list_count) = Self::parse_metadata(&meta_bytes);

                if list_count == 0 || head >= tail {
                    return Ok(popped); // List is empty
                }

                let new_head = head + 1;
                let new_count = list_count - 1;
                let new_meta = Self::build_metadata(new_head, tail, new_count);

                if self
                    .store
                    .compare_and_swap(meta_key.as_bytes(), &meta_bytes, &new_meta)?
                {
                    let value_key = format!("L:{}:{}", String::from_utf8_lossy(key), head);

                    match self.store.get_bytes(value_key.as_bytes()) {
                        Ok(value) => {
                            self.store.delete(value_key.as_bytes())?;
                            popped.push(value);
                            break;
                        }
                        Err(_) => {
                            // Gap detected, continue to next position
                            continue;
                        }
                    }
                }

                retries += 1;
                if retries >= MAX_RETRIES {
                    return Ok(popped); // Return what we got so far
                }
                std::thread::yield_now();
            }
        }

        Ok(popped)
    }

    pub fn rpop(&self, key: &[u8], count: Option<usize>) -> Result<Vec<Bytes>> {
        let meta_key = format!("L:{}:meta", String::from_utf8_lossy(key));
        let count = count.unwrap_or(1);
        let mut popped = Vec::new();

        for _ in 0..count {
            let mut retries = 0;
            loop {
                let meta_bytes = match self.store.get_bytes(meta_key.as_bytes()) {
                    Ok(bytes) => bytes,
                    Err(_) => return Ok(popped),
                };

                let (head, tail, list_count) = Self::parse_metadata(&meta_bytes);

                if list_count == 0 || head >= tail {
                    return Ok(popped);
                }

                let new_tail = tail - 1;
                let new_count = list_count - 1;
                let new_meta = Self::build_metadata(head, new_tail, new_count);

                if self
                    .store
                    .compare_and_swap(meta_key.as_bytes(), &meta_bytes, &new_meta)?
                {
                    let value_key = format!("L:{}:{}", String::from_utf8_lossy(key), new_tail);

                    match self.store.get_bytes(value_key.as_bytes()) {
                        Ok(value) => {
                            self.store.delete(value_key.as_bytes())?;
                            popped.push(value);
                            break;
                        }
                        Err(_) => continue,
                    }
                }

                retries += 1;
                if retries >= MAX_RETRIES {
                    return Ok(popped);
                }
                std::thread::yield_now();
            }
        }

        Ok(popped)
    }

    pub fn llen(&self, key: &[u8]) -> Result<i64> {
        let meta_key = format!("L:{}:meta", String::from_utf8_lossy(key));

        match self.store.get_bytes(meta_key.as_bytes()) {
            Ok(meta_bytes) => {
                let (_, _, count) = Self::parse_metadata(&meta_bytes);
                Ok(count as i64)
            }
            Err(_) => Ok(0),
        }
    }

    pub fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Result<Vec<Bytes>> {
        let meta_key = format!("L:{}:meta", String::from_utf8_lossy(key));

        let meta_bytes = match self.store.get_bytes(meta_key.as_bytes()) {
            Ok(bytes) => bytes,
            Err(_) => return Ok(vec![]),
        };

        let (head, tail, count) = Self::parse_metadata(&meta_bytes);

        if count == 0 {
            return Ok(vec![]);
        }

        let len = tail - head;

        // Convert negative indices to positive
        let start = if start < 0 {
            (len + start).max(0)
        } else {
            start
        };

        let stop = if stop < 0 { len + stop } else { stop };

        // Check if completely out of bounds
        if start >= len || stop < 0 {
            return Ok(vec![]);
        }

        // Clamp to valid range
        let start = start.max(0).min(len - 1);
        let stop = stop.max(0).min(len - 1);

        if start > stop {
            return Ok(vec![]);
        }

        let mut results = Vec::new();
        for i in start..=stop {
            let pos = head + i;
            let value_key = format!("L:{}:{}", String::from_utf8_lossy(key), pos);

            if let Ok(value) = self.store.get_bytes(value_key.as_bytes()) {
                results.push(value);
            }
            // Skip gaps silently
        }

        Ok(results)
    }

    pub fn lindex(&self, key: &[u8], index: i64) -> Result<Option<Bytes>> {
        let meta_key = format!("L:{}:meta", String::from_utf8_lossy(key));

        let meta_bytes = match self.store.get_bytes(meta_key.as_bytes()) {
            Ok(bytes) => bytes,
            Err(_) => return Ok(None),
        };

        let (head, tail, count) = Self::parse_metadata(&meta_bytes);

        if count == 0 {
            return Ok(None);
        }

        let len = tail - head;
        let actual_index = if index < 0 {
            let positive_index = len + index;
            if positive_index < 0 {
                return Ok(None);
            }
            positive_index
        } else {
            if index >= len {
                return Ok(None);
            }
            index
        };

        let pos = head + actual_index;
        let value_key = format!("L:{}:{}", String::from_utf8_lossy(key), pos);

        match self.store.get_bytes(value_key.as_bytes()) {
            Ok(value) => Ok(Some(value)),
            Err(_) => Ok(None), // Gap
        }
    }
}
