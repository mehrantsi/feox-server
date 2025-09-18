use super::client::ClientOperations;
use super::hash::HashOperations;
use super::list::ListOperations;
use super::Command;
use crate::client_registry::ClientRegistry;
use crate::config::Config;
use crate::protocol::resp::RespValue;
use bytes::Bytes;
use feoxdb::FeoxStore;
use std::sync::Arc;

/// Match a key against a glob pattern
fn match_pattern(key: &[u8], pattern: &str) -> bool {
    let key_str = String::from_utf8_lossy(key);
    glob_match(pattern, &key_str)
}

/// Simple glob pattern matching (* and ? support)
fn glob_match(pattern: &str, text: &str) -> bool {
    let mut p_idx = 0;
    let mut t_idx = 0;
    let mut star_idx = None;
    let mut star_match = None;

    let pattern_bytes = pattern.as_bytes();
    let text_bytes = text.as_bytes();

    while t_idx < text_bytes.len() {
        if p_idx < pattern_bytes.len() {
            match pattern_bytes[p_idx] {
                b'*' => {
                    star_idx = Some(p_idx);
                    star_match = Some(t_idx);
                    p_idx += 1;
                }
                b'?' => {
                    p_idx += 1;
                    t_idx += 1;
                }
                _ => {
                    if pattern_bytes[p_idx] == text_bytes[t_idx] {
                        p_idx += 1;
                        t_idx += 1;
                    } else if let Some(star) = star_idx {
                        p_idx = star + 1;
                        star_match = Some(star_match.unwrap() + 1);
                        t_idx = star_match.unwrap();
                    } else {
                        return false;
                    }
                }
            }
        } else if let Some(star) = star_idx {
            p_idx = star + 1;
            star_match = Some(star_match.unwrap() + 1);
            t_idx = star_match.unwrap();
        } else {
            return false;
        }
    }

    // Check remaining pattern characters (should only be *)
    while p_idx < pattern_bytes.len() && pattern_bytes[p_idx] == b'*' {
        p_idx += 1;
    }

    p_idx == pattern_bytes.len()
}

/// Extract prefix from a pattern (everything before the first wildcard)
fn extract_prefix(pattern: &str) -> &str {
    for (i, ch) in pattern.char_indices() {
        if ch == '*' || ch == '?' || ch == '[' {
            return &pattern[..i];
        }
    }
    pattern
}

/// Executes parsed Redis commands against a FeoxStore
///
/// Translates between Redis protocol semantics and FeOx operations.
#[derive(Clone)]
pub struct CommandExecutor {
    store: Arc<FeoxStore>,
    list_ops: ListOperations,
    hash_ops: HashOperations,
    client_ops: ClientOperations,
    config: Config, // Store config for auth checking
    start_time: std::time::Instant,
    commands_processed: Arc<std::sync::atomic::AtomicU64>,
    connection_id: Option<usize>,
}

impl CommandExecutor {
    /// Create a new command executor with the given store and config
    pub fn new(store: Arc<FeoxStore>, config: &Config) -> Self {
        let list_ops = ListOperations::new(Arc::clone(&store));
        let hash_ops = HashOperations::new(Arc::clone(&store));
        Self {
            store,
            list_ops,
            hash_ops,
            client_ops: ClientOperations::new(),
            config: config.clone(),
            start_time: std::time::Instant::now(),
            commands_processed: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            connection_id: None,
        }
    }

    /// Set the client registry and connection ID for CLIENT command support
    pub fn with_client_info(mut self, registry: Arc<ClientRegistry>, connection_id: usize) -> Self {
        self.client_ops = ClientOperations::with_registry(registry);
        self.connection_id = Some(connection_id);
        self
    }

    /// Check if password is correct
    pub fn check_auth(&self, password: &str) -> bool {
        self.config.check_password(password)
    }

    // Fast-path SET operation
    #[inline(always)]
    pub fn fast_set(&self, key: &[u8], value: &[u8]) -> Result<(), feoxdb::FeoxError> {
        self.store.insert_with_timestamp(key, value, None)?;
        Ok(())
    }

    // Fast-path SET operation with Bytes
    #[inline(always)]
    pub fn fast_set_bytes(&self, key: &[u8], value: bytes::Bytes) -> Result<(), feoxdb::FeoxError> {
        self.store.insert_bytes_with_timestamp(key, value, None)?;
        Ok(())
    }

    // Fast-path GET operation
    #[inline(always)]
    pub fn fast_get(&self, key: &[u8]) -> Result<bytes::Bytes, feoxdb::FeoxError> {
        self.store.get_bytes(key)
    }

    /// Execute a command and return RESP response
    #[inline]
    pub fn execute(&self, cmd: Command) -> RespValue {
        use std::sync::atomic::Ordering;

        // Increment command counter
        self.commands_processed.fetch_add(1, Ordering::Relaxed);

        match cmd {
            Command::Get(key) => match self.store.get_bytes(&key) {
                Ok(value) => RespValue::BulkString(Some(value)),
                Err(feoxdb::FeoxError::KeyNotFound) => RespValue::BulkString(None),
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::Set { key, value, ex, px } => {
                let result = if let Some(seconds) = ex {
                    self.store
                        .insert_bytes_with_ttl_and_timestamp(&key, value, seconds, None)
                } else if let Some(millis) = px {
                    self.store
                        .insert_bytes_with_ttl_and_timestamp(&key, value, millis / 1000, None)
                } else {
                    self.store.insert_bytes_with_timestamp(&key, value, None)
                };

                match result {
                    Ok(_) => RespValue::SimpleString(Bytes::from_static(b"OK")),
                    Err(e) => RespValue::Error(format!("ERR {}", e)),
                }
            }

            Command::Del(keys) => {
                let mut count = 0i64;
                for key in keys {
                    if self.store.delete(&key).is_ok() {
                        count += 1;
                    }
                }
                RespValue::Integer(count)
            }

            Command::Exists(keys) => {
                let count = keys
                    .iter()
                    .filter(|key| self.store.contains_key(key))
                    .count() as i64;
                RespValue::Integer(count)
            }

            Command::Incr(key) => match self.store.atomic_increment(&key, 1) {
                Ok(val) => RespValue::Integer(val),
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::IncrBy { key, delta } => match self.store.atomic_increment(&key, delta) {
                Ok(val) => RespValue::Integer(val),
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::Decr(key) => match self.store.atomic_increment(&key, -1) {
                Ok(val) => RespValue::Integer(val),
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::DecrBy { key, delta } => match self.store.atomic_increment(&key, -delta) {
                Ok(val) => RespValue::Integer(val),
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::Expire { key, seconds } => match self.store.update_ttl(&key, seconds) {
                Ok(_) => RespValue::Integer(1),
                Err(feoxdb::FeoxError::KeyNotFound) => RespValue::Integer(0),
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::PExpire { key, milliseconds } => {
                match self.store.update_ttl(&key, milliseconds / 1000) {
                    Ok(_) => RespValue::Integer(1),
                    Err(feoxdb::FeoxError::KeyNotFound) => RespValue::Integer(0),
                    Err(e) => RespValue::Error(format!("ERR {}", e)),
                }
            }

            Command::Ttl(key) => {
                match self.store.get_ttl(&key) {
                    Ok(Some(ttl)) => RespValue::Integer(ttl as i64),
                    Ok(None) => RespValue::Integer(-1), // No TTL
                    Err(feoxdb::FeoxError::KeyNotFound) => RespValue::Integer(-2),
                    Err(e) => RespValue::Error(format!("ERR {}", e)),
                }
            }

            Command::PTtl(key) => {
                match self.store.get_ttl(&key) {
                    Ok(Some(ttl)) => RespValue::Integer((ttl * 1000) as i64),
                    Ok(None) => RespValue::Integer(-1), // No TTL
                    Err(feoxdb::FeoxError::KeyNotFound) => RespValue::Integer(-2),
                    Err(e) => RespValue::Error(format!("ERR {}", e)),
                }
            }

            Command::Persist(key) => match self.store.persist(&key) {
                Ok(_) => RespValue::Integer(1),
                Err(feoxdb::FeoxError::KeyNotFound) => RespValue::Integer(0),
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::MGet(keys) => {
                let values: Vec<RespValue> = keys
                    .into_iter()
                    .map(|key| match self.store.get_bytes(&key) {
                        Ok(value) => RespValue::BulkString(Some(value)),
                        Err(_) => RespValue::BulkString(None),
                    })
                    .collect();
                RespValue::Array(Some(values))
            }

            Command::MSet(pairs) => {
                for (key, value) in pairs {
                    // Pass None to let FeOx generate a new timestamp
                    if let Err(e) = self.store.insert_with_timestamp(&key, &value, None) {
                        return RespValue::Error(format!("ERR {}", e));
                    }
                }
                RespValue::SimpleString(Bytes::from_static(b"OK"))
            }

            Command::Ping(msg) => match msg {
                Some(m) => RespValue::BulkString(Some(m)),
                None => RespValue::SimpleString(Bytes::from_static(b"PONG")),
            },

            Command::Echo(msg) => RespValue::BulkString(Some(msg)),

            Command::Config { action, args } => {
                match action.to_uppercase().as_str() {
                    "GET" => {
                        // Return empty config for compatibility with redis-benchmark
                        if args.is_empty() {
                            RespValue::Array(Some(vec![]))
                        } else {
                            // Return nil for any specific config request
                            let mut results = Vec::new();
                            for arg in args {
                                results.push(RespValue::BulkString(Some(arg)));
                                results.push(RespValue::BulkString(None)); // nil value
                            }
                            RespValue::Array(Some(results))
                        }
                    }
                    "SET" => {
                        // Pretend to set config successfully
                        RespValue::SimpleString(Bytes::from_static(b"OK"))
                    }
                    _ => RespValue::Error(format!("ERR Unknown CONFIG subcommand '{}'", action)),
                }
            }

            Command::Command => {
                // Return supported commands in Redis COMMAND format
                // Each command entry: [name, arity, flags, first_key, last_key, step]
                let commands = vec![
                    // Basic commands
                    vec![
                        RespValue::BulkString(Some(Bytes::from_static(b"GET"))),
                        RespValue::Integer(2), // arity (command + 1 key)
                        RespValue::Array(Some(vec![RespValue::BulkString(Some(
                            Bytes::from_static(b"readonly"),
                        ))])),
                        RespValue::Integer(1), // first key position
                        RespValue::Integer(1), // last key position
                        RespValue::Integer(1), // step
                    ],
                    vec![
                        RespValue::BulkString(Some(Bytes::from_static(b"SET"))),
                        RespValue::Integer(-3), // arity (variable, min 3)
                        RespValue::Array(Some(vec![RespValue::BulkString(Some(
                            Bytes::from_static(b"write"),
                        ))])),
                        RespValue::Integer(1),
                        RespValue::Integer(1),
                        RespValue::Integer(1),
                    ],
                    vec![
                        RespValue::BulkString(Some(Bytes::from_static(b"DEL"))),
                        RespValue::Integer(-2), // arity (variable, min 2)
                        RespValue::Array(Some(vec![RespValue::BulkString(Some(
                            Bytes::from_static(b"write"),
                        ))])),
                        RespValue::Integer(1),
                        RespValue::Integer(-1), // all args are keys
                        RespValue::Integer(1),
                    ],
                    vec![
                        RespValue::BulkString(Some(Bytes::from_static(b"EXISTS"))),
                        RespValue::Integer(-2),
                        RespValue::Array(Some(vec![RespValue::BulkString(Some(
                            Bytes::from_static(b"readonly"),
                        ))])),
                        RespValue::Integer(1),
                        RespValue::Integer(-1),
                        RespValue::Integer(1),
                    ],
                    // Atomic operations
                    vec![
                        RespValue::BulkString(Some(Bytes::from_static(b"INCR"))),
                        RespValue::Integer(2),
                        RespValue::Array(Some(vec![RespValue::BulkString(Some(
                            Bytes::from_static(b"write"),
                        ))])),
                        RespValue::Integer(1),
                        RespValue::Integer(1),
                        RespValue::Integer(1),
                    ],
                    vec![
                        RespValue::BulkString(Some(Bytes::from_static(b"DECR"))),
                        RespValue::Integer(2),
                        RespValue::Array(Some(vec![RespValue::BulkString(Some(
                            Bytes::from_static(b"write"),
                        ))])),
                        RespValue::Integer(1),
                        RespValue::Integer(1),
                        RespValue::Integer(1),
                    ],
                    // TTL commands
                    vec![
                        RespValue::BulkString(Some(Bytes::from_static(b"EXPIRE"))),
                        RespValue::Integer(3),
                        RespValue::Array(Some(vec![RespValue::BulkString(Some(
                            Bytes::from_static(b"write"),
                        ))])),
                        RespValue::Integer(1),
                        RespValue::Integer(1),
                        RespValue::Integer(1),
                    ],
                    vec![
                        RespValue::BulkString(Some(Bytes::from_static(b"TTL"))),
                        RespValue::Integer(2),
                        RespValue::Array(Some(vec![RespValue::BulkString(Some(
                            Bytes::from_static(b"readonly"),
                        ))])),
                        RespValue::Integer(1),
                        RespValue::Integer(1),
                        RespValue::Integer(1),
                    ],
                    // Bulk operations
                    vec![
                        RespValue::BulkString(Some(Bytes::from_static(b"MGET"))),
                        RespValue::Integer(-2),
                        RespValue::Array(Some(vec![RespValue::BulkString(Some(
                            Bytes::from_static(b"readonly"),
                        ))])),
                        RespValue::Integer(1),
                        RespValue::Integer(-1),
                        RespValue::Integer(1),
                    ],
                    vec![
                        RespValue::BulkString(Some(Bytes::from_static(b"MSET"))),
                        RespValue::Integer(-3),
                        RespValue::Array(Some(vec![RespValue::BulkString(Some(
                            Bytes::from_static(b"write"),
                        ))])),
                        RespValue::Integer(1),
                        RespValue::Integer(-1),
                        RespValue::Integer(2), // key-value pairs
                    ],
                    // Server commands
                    vec![
                        RespValue::BulkString(Some(Bytes::from_static(b"PING"))),
                        RespValue::Integer(-1),
                        RespValue::Array(Some(vec![RespValue::BulkString(Some(
                            Bytes::from_static(b"fast"),
                        ))])),
                        RespValue::Integer(0),
                        RespValue::Integer(0),
                        RespValue::Integer(0),
                    ],
                    // FeOx-specific
                    vec![
                        RespValue::BulkString(Some(Bytes::from_static(b"JSONPATCH"))),
                        RespValue::Integer(3),
                        RespValue::Array(Some(vec![RespValue::BulkString(Some(
                            Bytes::from_static(b"write"),
                        ))])),
                        RespValue::Integer(1),
                        RespValue::Integer(1),
                        RespValue::Integer(1),
                    ],
                    vec![
                        RespValue::BulkString(Some(Bytes::from_static(b"CAS"))),
                        RespValue::Integer(4),
                        RespValue::Array(Some(vec![RespValue::BulkString(Some(
                            Bytes::from_static(b"write"),
                        ))])),
                        RespValue::Integer(1),
                        RespValue::Integer(1),
                        RespValue::Integer(1),
                    ],
                ];

                RespValue::Array(Some(
                    commands
                        .into_iter()
                        .map(|cmd| RespValue::Array(Some(cmd)))
                        .collect(),
                ))
            }

            Command::Quit => RespValue::SimpleString(Bytes::from_static(b"OK")),

            Command::FlushDb => {
                // FeOx doesn't have a direct flush method
                // For in-memory mode: would need to recreate the store
                // For persistent mode: would need to delete files and recreate
                // Since we can't recreate the store from here, return error
                RespValue::Error("ERR FLUSHDB requires server restart. For persistent mode, also delete data files.".to_string())
            }

            Command::Keys(pattern) => {
                // Use range_query to get all keys, then filter by pattern
                let prefix = extract_prefix(&pattern);

                // Calculate end key for prefix scan
                let (start_key, end_key) = if prefix.is_empty() {
                    // Scan all keys
                    (vec![], vec![0xFF; 255])
                } else if pattern == prefix {
                    // Exact match, no wildcards
                    return match self.store.get_bytes(prefix.as_bytes()) {
                        Ok(_) => {
                            let keys =
                                vec![RespValue::BulkString(Some(Bytes::from(prefix.to_string())))];
                            RespValue::Array(Some(keys))
                        }
                        Err(_) => RespValue::Array(Some(vec![])),
                    };
                } else {
                    // Prefix scan with pattern matching
                    let mut end = prefix.as_bytes().to_vec();
                    end.push(b'~'); // Use tilde as upper bound
                    (prefix.as_bytes().to_vec(), end)
                };

                // Get keys using range_query
                match self.store.range_query(&start_key, &end_key, 100000) {
                    Ok(pairs) => {
                        let keys: Vec<RespValue> = pairs
                            .into_iter()
                            .filter(|(key, _)| match_pattern(key, &pattern))
                            .map(|(key, _)| RespValue::BulkString(Some(Bytes::from(key))))
                            .collect();
                        RespValue::Array(Some(keys))
                    }
                    Err(e) => RespValue::Error(format!("ERR {}", e)),
                }
            }

            Command::Scan {
                cursor,
                count,
                pattern,
            } => {
                // Parse cursor (empty or "0" means start from beginning)
                let start_key = if cursor.is_empty() || cursor == b"0" {
                    vec![]
                } else {
                    cursor.clone()
                };

                // For prefix patterns, optimize the scan range
                let (scan_start, scan_end) = if let Some(ref pat) = pattern {
                    let prefix = extract_prefix(pat);
                    if !prefix.is_empty() && pat.starts_with(prefix) && pat.contains('*') {
                        // Optimize for prefix patterns like "user:*"
                        let mut end = prefix.as_bytes().to_vec();
                        end.push(b'~');

                        // Adjust start if cursor is past the prefix
                        let actual_start = if start_key.len() > prefix.len()
                            && start_key.starts_with(prefix.as_bytes())
                        {
                            start_key
                        } else if start_key.is_empty() {
                            prefix.as_bytes().to_vec()
                        } else {
                            start_key
                        };

                        (actual_start, end)
                    } else {
                        (start_key, vec![0xFF; 255])
                    }
                } else {
                    (start_key, vec![0xFF; 255])
                };

                // Get keys using range_query (get a bit more than requested to ensure we have enough after filtering)
                let fetch_count = if pattern.is_some() { count * 2 } else { count };
                match self
                    .store
                    .range_query(&scan_start, &scan_end, fetch_count + 1)
                {
                    Ok(pairs) => {
                        let mut keys = Vec::new();
                        let mut next_cursor = None;

                        for (key, _) in pairs.into_iter() {
                            // Skip if we've collected enough
                            if keys.len() >= count {
                                next_cursor = Some(key.clone());
                                break;
                            }

                            // Apply pattern filter if specified
                            if let Some(ref pat) = pattern {
                                if !match_pattern(&key, pat) {
                                    continue;
                                }
                            }

                            keys.push(RespValue::BulkString(Some(Bytes::from(key.clone()))));
                        }

                        // Format response: [cursor, [keys...]]
                        let cursor_str = if let Some(next) = next_cursor {
                            Bytes::from(next)
                        } else {
                            Bytes::from_static(b"0") // End of iteration
                        };

                        RespValue::Array(Some(vec![
                            RespValue::BulkString(Some(cursor_str)),
                            RespValue::Array(Some(keys)),
                        ]))
                    }
                    Err(e) => RespValue::Error(format!("ERR {}", e)),
                }
            }

            Command::Info(section) => {
                use std::sync::atomic::Ordering;

                // Get actual stats
                let uptime = self.start_time.elapsed().as_secs();
                let commands = self.commands_processed.load(Ordering::Relaxed);
                let stats = self.store.stats();

                // Format memory size
                let format_bytes = |bytes: usize| -> String {
                    if bytes < 1024 {
                        format!("{}B", bytes)
                    } else if bytes < 1024 * 1024 {
                        format!("{:.1}K", bytes as f64 / 1024.0)
                    } else if bytes < 1024 * 1024 * 1024 {
                        format!("{:.1}M", bytes as f64 / (1024.0 * 1024.0))
                    } else {
                        format!("{:.1}G", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
                    }
                };

                let mut info = String::new();

                // Server section
                if section.is_none()
                    || section
                        .as_ref()
                        .map(|s| s.eq_ignore_ascii_case("server"))
                        .unwrap_or(false)
                {
                    info.push_str(&format!(
                        "# Server\r\n\
                        redis_version:feox-{}\r\n\
                        redis_mode:standalone\r\n\
                        process_id:{}\r\n\
                        tcp_port:6379\r\n\
                        uptime_in_seconds:{}\r\n",
                        env!("CARGO_PKG_VERSION"),
                        std::process::id(),
                        uptime
                    ));
                }

                // Memory section
                if section.is_none()
                    || section
                        .as_ref()
                        .map(|s| s.eq_ignore_ascii_case("memory"))
                        .unwrap_or(false)
                {
                    info.push_str(&format!(
                        "# Memory\r\n\
                        used_memory:{}\r\n\
                        used_memory_human:{}\r\n\
                        used_memory_cache:{}\r\n\
                        used_memory_cache_human:{}\r\n",
                        stats.memory_usage,
                        format_bytes(stats.memory_usage),
                        stats.cache_memory,
                        format_bytes(stats.cache_memory)
                    ));
                }

                // Stats section
                if section.is_none()
                    || section
                        .as_ref()
                        .map(|s| s.eq_ignore_ascii_case("stats"))
                        .unwrap_or(false)
                {
                    info.push_str(&format!(
                        "# Stats\r\n\
                        total_connections_received:0\r\n\
                        total_commands_processed:{}\r\n\
                        instantaneous_ops_per_sec:0\r\n\
                        total_net_input_bytes:0\r\n\
                        total_net_output_bytes:0\r\n\
                        total_operations:{}\r\n\
                        total_gets:{}\r\n\
                        total_inserts:{}\r\n\
                        keyspace_hits:{}\r\n\
                        keyspace_misses:{}\r\n\
                        cache_hit_rate:{:.2}\r\n",
                        commands,
                        stats.total_operations,
                        stats.total_gets,
                        stats.total_inserts,
                        stats.cache_hits,
                        stats.cache_misses,
                        stats.cache_hit_rate * 100.0
                    ));
                }

                // Keyspace section
                if section.is_none()
                    || section
                        .as_ref()
                        .map(|s| s.eq_ignore_ascii_case("keyspace"))
                        .unwrap_or(false)
                {
                    info.push_str(&format!(
                        "# Keyspace\r\n\
                        db0:keys={},expires=0,avg_ttl=0\r\n",
                        stats.record_count
                    ));
                }

                RespValue::BulkString(Some(Bytes::from(info)))
            }

            Command::JsonPatch { key, patch } => {
                // Use FeOx's native json_patch method
                match self.store.json_patch(&key, &patch) {
                    Ok(_) => RespValue::SimpleString(Bytes::from_static(b"OK")),
                    Err(e) => RespValue::Error(format!("ERR {}", e)),
                }
            }

            Command::Cas {
                key,
                expected,
                new_value,
            } => {
                // Use FeOx's native compare_and_swap method
                match self.store.compare_and_swap(&key, &expected, &new_value) {
                    Ok(swapped) => RespValue::Integer(if swapped { 1 } else { 0 }),
                    Err(e) => RespValue::Error(format!("ERR {}", e)),
                }
            }

            Command::LPush { key, values } => match self.list_ops.lpush(&key, values) {
                Ok(count) => RespValue::Integer(count),
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::RPush { key, values } => match self.list_ops.rpush(&key, values) {
                Ok(count) => RespValue::Integer(count),
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::LPop { key, count } => match self.list_ops.lpop(&key, count) {
                Ok(values) => {
                    if values.is_empty() {
                        RespValue::BulkString(None)
                    } else if values.len() == 1 {
                        RespValue::BulkString(Some(values.into_iter().next().unwrap()))
                    } else {
                        RespValue::Array(Some(
                            values
                                .into_iter()
                                .map(|v| RespValue::BulkString(Some(v)))
                                .collect(),
                        ))
                    }
                }
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::RPop { key, count } => match self.list_ops.rpop(&key, count) {
                Ok(values) => {
                    if values.is_empty() {
                        RespValue::BulkString(None)
                    } else if values.len() == 1 {
                        RespValue::BulkString(Some(values.into_iter().next().unwrap()))
                    } else {
                        RespValue::Array(Some(
                            values
                                .into_iter()
                                .map(|v| RespValue::BulkString(Some(v)))
                                .collect(),
                        ))
                    }
                }
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::LLen(key) => match self.list_ops.llen(&key) {
                Ok(count) => RespValue::Integer(count),
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::LRange { key, start, stop } => match self.list_ops.lrange(&key, start, stop) {
                Ok(values) => RespValue::Array(Some(
                    values
                        .into_iter()
                        .map(|v| RespValue::BulkString(Some(v)))
                        .collect(),
                )),
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::LIndex { key, index } => match self.list_ops.lindex(&key, index) {
                Ok(Some(value)) => RespValue::BulkString(Some(value)),
                Ok(None) => RespValue::BulkString(None),
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::HSet { key, fields } => {
                let field_refs = fields.iter().map(|(f, v)| (f.as_slice(), v.clone()));
                match self.hash_ops.hset(&key, field_refs) {
                    Ok(count) => RespValue::Integer(count),
                    Err(e) => RespValue::Error(format!("ERR {}", e)),
                }
            }

            Command::HGet { key, field } => match self.hash_ops.hget(&key, &field) {
                Ok(Some(value)) => RespValue::BulkString(Some(value)),
                Ok(None) => RespValue::BulkString(None),
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::HMGet { key, fields } => match self.hash_ops.hmget(&key, fields) {
                Ok(values) => RespValue::Array(Some(
                    values
                        .into_iter()
                        .map(|v| match v {
                            Some(val) => RespValue::BulkString(Some(val)),
                            None => RespValue::BulkString(None),
                        })
                        .collect(),
                )),
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::HDel { key, fields } => match self.hash_ops.hdel(&key, fields) {
                Ok(count) => RespValue::Integer(count),
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::HExists { key, field } => match self.hash_ops.hexists(&key, &field) {
                Ok(exists) => RespValue::Integer(if exists { 1 } else { 0 }),
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::HGetAll(key) => match self.hash_ops.hgetall(&key) {
                Ok(pairs) => {
                    let mut result = Vec::new();
                    for (field, value) in pairs {
                        result.push(RespValue::BulkString(Some(Bytes::from(field))));
                        result.push(RespValue::BulkString(Some(value)));
                    }
                    RespValue::Array(Some(result))
                }
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::HLen(key) => match self.hash_ops.hlen(&key) {
                Ok(count) => RespValue::Integer(count),
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::HKeys(key) => match self.hash_ops.hkeys(&key) {
                Ok(keys) => RespValue::Array(Some(
                    keys.into_iter()
                        .map(|k| RespValue::BulkString(Some(Bytes::from(k))))
                        .collect(),
                )),
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::HVals(key) => match self.hash_ops.hvals(&key) {
                Ok(vals) => RespValue::Array(Some(
                    vals.into_iter()
                        .map(|v| RespValue::BulkString(Some(v)))
                        .collect(),
                )),
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },

            Command::HIncrBy { key, field, delta } => {
                match self.hash_ops.hincrby(&key, &field, delta) {
                    Ok(new_value) => RespValue::Integer(new_value),
                    Err(e) => RespValue::Error(format!("ERR {}", e)),
                }
            }

            Command::Auth(_) => {
                // This should be handled in connection.rs
                // If we get here, it means auth is not configured
                if self.config.requirepass.is_none() {
                    RespValue::Error("-ERR Client sent AUTH, but no password is set".to_string())
                } else {
                    // Should not reach here
                    RespValue::Error("-ERR AUTH failed".to_string())
                }
            }

            Command::Client {
                ref subcommand,
                ref args,
            } => self
                .client_ops
                .execute(subcommand, args, self.connection_id),

            // Pub/Sub commands are handled in connection.rs
            Command::Subscribe(_)
            | Command::Unsubscribe(_)
            | Command::PSubscribe(_)
            | Command::PUnsubscribe(_)
            | Command::Publish { .. }
            | Command::PubSub { .. } => RespValue::Error(
                "-ERR Pub/Sub commands should be handled in connection layer".to_string(),
            ),

            // Transaction commands are handled in connection.rs
            Command::Multi
            | Command::Exec
            | Command::Discard
            | Command::Watch(_)
            | Command::Unwatch => RespValue::Error(
                "-ERR Transaction commands should be handled in connection layer".to_string(),
            ),
        }
    }
}
