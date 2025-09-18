use crate::config::Config;
use crate::protocol::resp::{write_resp_value, RespValue};
use crate::protocol::{Command, CommandExecutor, RespParser};
use crate::pubsub::PubSubMessage;
use bytes::Bytes;
use feoxdb::FeoxStore;
use std::collections::{HashSet, VecDeque};
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, PartialEq)]
enum TransactionState {
    None,
    Queuing,
}

#[derive(Debug)]
pub enum PubSubOp {
    Subscribe(Vec<Vec<u8>>),
    Unsubscribe(Option<Vec<Vec<u8>>>),
    PSubscribe(Vec<Vec<u8>>),
    PUnsubscribe(Option<Vec<Vec<u8>>>),
    Publish { channel: Vec<u8>, message: Vec<u8> },
    PubSubChannels { pattern: Option<Vec<u8>> },
    PubSubNumSub { channels: Vec<Vec<u8>> },
    PubSubNumPat,
}

/// Manages a client connection with RESP protocol handling
///
/// Handles command parsing, execution, and response buffering.
pub struct Connection {
    fd: RawFd,

    // Protocol parser
    parser: RespParser,
    executor: CommandExecutor,

    // Authentication state
    authenticated: bool,
    auth_required: bool,

    // Single consolidated write buffer for better performance
    pub write_buffer: Vec<u8>,
    write_position: usize,

    // Pipeline tracking
    pipeline_depth: usize,

    // Connection state
    closed: bool,

    // Pub/Sub state
    pub connection_id: usize,
    pub subscription_count: usize,
    pending_pubsub_messages: VecDeque<PubSubMessage>,

    // Client metadata
    pub client_name: Option<String>,
    pub client_addr: Option<SocketAddr>,
    pub connected_at: u64, // Unix timestamp in seconds
    pub commands_processed: u64,
    pub flags: Vec<String>, // Client flags (e.g., "pubsub", "master", "replica")

    // Transaction state
    transaction_state: TransactionState,
    queued_commands: Vec<Command>,
    watched_keys: HashSet<Vec<u8>>,
}

impl Connection {
    /// Create a new connection handler
    pub fn new(fd: RawFd, buffer_size: usize, store: Arc<FeoxStore>, config: &Config) -> Self {
        Self::new_with_addr(fd, buffer_size, store, config, None)
    }

    /// Set the executor with client registry info
    pub fn set_client_registry(&mut self, registry: Arc<crate::client_registry::ClientRegistry>) {
        self.executor = self
            .executor
            .clone()
            .with_client_info(registry, self.connection_id);
    }

    /// Create a new connection handler with address
    pub fn new_with_addr(
        fd: RawFd,
        buffer_size: usize,
        store: Arc<FeoxStore>,
        config: &Config,
        addr: Option<SocketAddr>,
    ) -> Self {
        let executor = CommandExecutor::new(store, config);
        let auth_required = config.auth_required();

        static CONNECTION_ID: std::sync::atomic::AtomicUsize =
            std::sync::atomic::AtomicUsize::new(0);
        let connection_id = CONNECTION_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            fd,
            parser: RespParser::new(),
            executor,
            authenticated: !auth_required, // If no auth required, consider authenticated
            auth_required,
            write_buffer: Vec::with_capacity(buffer_size),
            write_position: 0,
            pipeline_depth: 0,
            closed: false,
            connection_id,
            subscription_count: 0,
            pending_pubsub_messages: VecDeque::new(),
            client_name: None,
            client_addr: addr,
            connected_at: now,
            commands_processed: 0,
            flags: Vec::new(),
            transaction_state: TransactionState::None,
            queued_commands: Vec::new(),
            watched_keys: HashSet::new(),
        }
    }

    pub fn fd(&self) -> RawFd {
        self.fd
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn close(&mut self) {
        if !self.closed {
            self.closed = true;
        }
    }

    /// Set authentication status
    pub fn set_authenticated(&mut self, authenticated: bool) {
        self.authenticated = authenticated;
    }

    /// Check if connection is authenticated
    pub fn is_authenticated(&self) -> bool {
        !self.auth_required || self.authenticated
    }

    /// Process incoming data with inline execution
    /// Returns pub/sub operations that need to be executed
    pub fn process_read(&mut self, data: &[u8]) -> crate::error::Result<Vec<PubSubOp>> {
        let mut pubsub_ops = Vec::new();

        // Feed data to parser
        self.parser.feed(data);

        // Only clear buffer if all previous writes have been consumed
        if self.write_position >= self.write_buffer.len() {
            self.write_buffer.clear();
            self.write_position = 0;
        }

        // Parse and execute commands inline
        while let Some(resp_value) = self
            .parser
            .parse_next()
            .map_err(crate::error::Error::Protocol)?
        {
            // Update command counter
            self.commands_processed += 1;

            // Fast-path for common commands (SET/GET) if not in transaction
            if self.transaction_state == TransactionState::None && self.try_fast_path(&resp_value) {
                self.pipeline_depth += 1;
                continue;
            }

            // Parse command (slow path)
            let command = Command::from_resp(resp_value).map_err(crate::error::Error::Protocol)?;

            // Check for quit
            if matches!(command, Command::Quit) {
                self.closed = true;
                self.write_buffer.extend_from_slice(b"+OK\r\n");
                return Ok(pubsub_ops);
            }

            // Check if in pub/sub mode and restrict commands
            if self.is_in_pubsub_mode() && !command.is_allowed_in_pubsub_mode() {
                write_resp_value(
                    &mut self.write_buffer,
                    &RespValue::Error(
                        "-ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context".to_string(),
                    ),
                );
                continue;
            }

            // Special handling for CLIENT SETNAME - update connection metadata
            if let Command::Client {
                ref subcommand,
                ref args,
            } = command
            {
                if subcommand.to_uppercase() == "SETNAME" && !args.is_empty() {
                    self.client_name = Some(String::from_utf8_lossy(&args[0]).to_string());
                }
            }

            // Handle transaction commands
            match command {
                Command::Multi => {
                    if self.transaction_state == TransactionState::Queuing {
                        write_resp_value(
                            &mut self.write_buffer,
                            &RespValue::Error("-ERR MULTI calls can not be nested".to_string()),
                        );
                        continue;
                    }
                    self.transaction_state = TransactionState::Queuing;
                    self.queued_commands.clear();
                    write_resp_value(
                        &mut self.write_buffer,
                        &RespValue::SimpleString(Bytes::from_static(b"OK")),
                    );
                    continue;
                }
                Command::Exec => {
                    if self.transaction_state != TransactionState::Queuing {
                        write_resp_value(
                            &mut self.write_buffer,
                            &RespValue::Error("-ERR EXEC without MULTI".to_string()),
                        );
                        continue;
                    }

                    // Execute all queued commands
                    let mut results = Vec::new();
                    for queued_cmd in self.queued_commands.drain(..) {
                        results.push(self.executor.execute(queued_cmd));
                    }

                    self.transaction_state = TransactionState::None;
                    self.watched_keys.clear();

                    write_resp_value(&mut self.write_buffer, &RespValue::Array(Some(results)));
                    continue;
                }
                Command::Discard => {
                    if self.transaction_state != TransactionState::Queuing {
                        write_resp_value(
                            &mut self.write_buffer,
                            &RespValue::Error("-ERR DISCARD without MULTI".to_string()),
                        );
                        continue;
                    }

                    self.transaction_state = TransactionState::None;
                    self.queued_commands.clear();
                    self.watched_keys.clear();

                    write_resp_value(
                        &mut self.write_buffer,
                        &RespValue::SimpleString(Bytes::from_static(b"OK")),
                    );
                    continue;
                }
                Command::Watch(ref keys) => {
                    if self.transaction_state == TransactionState::Queuing {
                        write_resp_value(
                            &mut self.write_buffer,
                            &RespValue::Error("-ERR WATCH inside MULTI is not allowed".to_string()),
                        );
                        continue;
                    }
                    for key in keys {
                        self.watched_keys.insert(key.clone());
                    }
                    write_resp_value(
                        &mut self.write_buffer,
                        &RespValue::SimpleString(Bytes::from_static(b"OK")),
                    );
                    continue;
                }
                Command::Unwatch => {
                    self.watched_keys.clear();
                    write_resp_value(
                        &mut self.write_buffer,
                        &RespValue::SimpleString(Bytes::from_static(b"OK")),
                    );
                    continue;
                }
                _ => {}
            }

            // If in transaction, queue the command
            if self.transaction_state == TransactionState::Queuing {
                self.queued_commands.push(command);
                write_resp_value(
                    &mut self.write_buffer,
                    &RespValue::SimpleString(Bytes::from_static(b"QUEUED")),
                );
                continue;
            }

            // Check authentication for non-AUTH commands
            let response = if !self.authenticated && !matches!(command, Command::Auth(_)) {
                // Allow PING without auth (Redis-compatible)
                if matches!(command, Command::Ping(_)) {
                    self.executor.execute(command)
                } else {
                    RespValue::Error("-NOAUTH Authentication required.".to_string())
                }
            } else {
                // Special handling for AUTH command
                if let Command::Auth(password) = &command {
                    // Check if password is configured
                    if !self.auth_required {
                        RespValue::Error(
                            "-ERR Client sent AUTH, but no password is set".to_string(),
                        )
                    } else {
                        let password_str = String::from_utf8_lossy(password);
                        if self.executor.check_auth(&password_str) {
                            self.set_authenticated(true);
                            RespValue::SimpleString(Bytes::from_static(b"OK"))
                        } else {
                            RespValue::Error("-ERR invalid password".to_string())
                        }
                    }
                } else if command.is_pubsub_command() {
                    // Capture subcommand for error message if needed
                    let subcommand_str = if let Command::PubSub { ref subcommand, .. } = command {
                        Some(subcommand.clone())
                    } else {
                        None
                    };

                    if let Some(pubsub_op) = command.to_pubsub_op() {
                        pubsub_ops.push(pubsub_op);
                        // Response will be sent after processing by pub/sub manager
                        continue;
                    } else if let Some(subcommand) = subcommand_str {
                        // Unknown PUBSUB subcommand
                        RespValue::Error(format!("ERR Unknown PUBSUB subcommand '{}'", subcommand))
                    } else {
                        RespValue::Error("ERR Failed to process pub/sub command".to_string())
                    }
                } else {
                    self.executor.execute(command)
                }
            };

            write_resp_value(&mut self.write_buffer, &response);

            self.pipeline_depth += 1;
        }

        Ok(pubsub_ops)
    }

    /// Get pending write data as a single buffer slice
    pub fn pending_writes(&mut self) -> Option<&[u8]> {
        if self.write_position < self.write_buffer.len() {
            Some(&self.write_buffer[self.write_position..])
        } else {
            None
        }
    }

    /// Mark bytes as written
    pub fn consume_writes(&mut self, n: usize) {
        self.write_position += n;
    }

    /// Add a pub/sub message to the pending queue
    pub fn queue_pubsub_message(&mut self, message: PubSubMessage) {
        self.pending_pubsub_messages.push_back(message);
    }

    /// Check if connection is in pub/sub mode
    pub fn is_in_pubsub_mode(&self) -> bool {
        self.subscription_count > 0
    }

    /// Update subscription count
    pub fn set_subscription_count(&mut self, count: usize) {
        self.subscription_count = count;
        // Update flags based on subscription status
        if count > 0 && !self.flags.contains(&"pubsub".to_string()) {
            self.flags.push("pubsub".to_string());
        } else if count == 0 {
            self.flags.retain(|f| f != "pubsub");
        }
    }

    /// Process pending pub/sub messages
    pub fn process_pubsub_messages(&mut self) {
        while let Some(message) = self.pending_pubsub_messages.pop_front() {
            let resp = message.to_resp();
            write_resp_value(&mut self.write_buffer, &resp);
        }
    }

    /// Try to handle common commands (SET/GET) via fast path
    /// Returns true if handled, false otherwise
    #[inline(always)]
    fn try_fast_path(&mut self, resp_value: &RespValue) -> bool {
        // Static responses
        const OK_RESPONSE: &[u8] = b"+OK\r\n";
        const NIL_RESPONSE: &[u8] = b"$-1\r\n";

        // Must be an array with at least 2 elements
        let args = match resp_value {
            RespValue::Array(Some(args)) if args.len() >= 2 => args,
            _ => return false,
        };

        // Get command name
        let cmd = match &args[0] {
            RespValue::BulkString(Some(cmd)) => cmd,
            _ => return false,
        };

        // Check for SET command (3 args minimum: SET key value)
        if cmd.len() == 3 && cmd.eq_ignore_ascii_case(b"SET") && args.len() >= 3 {
            // Extract key and value
            let (key, value_bytes) = match (&args[1], &args[2]) {
                (RespValue::BulkString(Some(k)), RespValue::BulkString(Some(v))) => {
                    (k.as_ref(), v.clone())
                }
                _ => return false,
            };

            // Simple SET without options
            if args.len() == 3 {
                match self.executor.fast_set_bytes(key, value_bytes) {
                    Ok(_) => {
                        self.write_buffer.extend_from_slice(OK_RESPONSE);
                        return true;
                    }
                    Err(e) => {
                        self.write_buffer.extend_from_slice(b"-ERR ");
                        self.write_buffer
                            .extend_from_slice(e.to_string().as_bytes());
                        self.write_buffer.extend_from_slice(b"\r\n");
                        return true;
                    }
                }
            }
        }

        // Check for GET command (2 args: GET key)
        if cmd.len() == 3 && cmd.eq_ignore_ascii_case(b"GET") && args.len() == 2 {
            let key = match &args[1] {
                RespValue::BulkString(Some(k)) => k,
                _ => return false,
            };

            match self.executor.fast_get(key) {
                Ok(value) => {
                    let mut num_buf = itoa::Buffer::new();
                    let len_str = num_buf.format(value.len());

                    self.write_buffer.push(b'$');
                    self.write_buffer.extend_from_slice(len_str.as_bytes());
                    self.write_buffer.extend_from_slice(b"\r\n");
                    self.write_buffer.extend_from_slice(&value);
                    self.write_buffer.extend_from_slice(b"\r\n");
                    return true;
                }
                Err(feoxdb::FeoxError::KeyNotFound) => {
                    self.write_buffer.extend_from_slice(NIL_RESPONSE);
                    return true;
                }
                Err(e) => {
                    self.write_buffer.extend_from_slice(b"-ERR ");
                    self.write_buffer
                        .extend_from_slice(e.to_string().as_bytes());
                    self.write_buffer.extend_from_slice(b"\r\n");
                    return true;
                }
            }
        }

        false // Not a fast-path command
    }
}
