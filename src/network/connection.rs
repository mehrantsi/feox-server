use crate::config::Config;
use crate::protocol::resp::{write_resp_value, RespValue};
use crate::protocol::{Command, CommandExecutor, RespParser};
use crate::pubsub::PubSubMessage;
use bytes::Bytes;
use feoxdb::FeoxStore;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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
    pub connected_at: u64,    // Unix timestamp in seconds
    pub last_command_at: u64, // Unix timestamp in seconds
    pub commands_processed: u64,
    pub flags: Vec<String>, // Client flags (e.g., "pubsub", "master", "replica")
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
            last_command_at: now,
            commands_processed: 0,
            flags: Vec::new(),
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
            unsafe {
                libc::close(self.fd);
            }
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

        // Clear buffer for new batch of responses
        self.write_buffer.clear();
        self.write_position = 0;

        // Parse and execute commands inline
        while let Some(resp_value) = self
            .parser
            .parse_next()
            .map_err(crate::error::Error::Protocol)?
        {
            // Update last command timestamp
            self.last_command_at = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            self.commands_processed += 1;
            // Parse command
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
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.close();
    }
}
