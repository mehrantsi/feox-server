use crate::client_registry::ClientRegistry;
use crate::pubsub::{handle_pubsub_operation, GlobalRegistry, ThreadLocalPubSub};
use crate::{config::Config, error::Result, network::Connection};
use feoxdb::FeoxStore;
use std::net::TcpListener;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use tracing::{debug, error, info};

/// High-performance Redis-compatible server
pub struct Server {
    config: Config,
    store: Arc<FeoxStore>,
    shutdown: AtomicBool,
    active_connections: AtomicUsize,
    pubsub_registry: Arc<GlobalRegistry>,
    client_registry: Arc<ClientRegistry>,
}

impl Server {
    /// Create a new server with the given configuration
    pub fn new(config: Config) -> Result<Self> {
        config.validate()?;

        // Create a single shared FeoxStore instance
        let store = if let Some(ref data_path) = config.data_path {
            let mut builder = FeoxStore::builder()
                .device_path(data_path.clone())
                .max_memory(config.max_memory_per_shard.unwrap_or(1024 * 1024 * 1024))
                .enable_ttl(config.enable_ttl);

            // Set file size if configured
            if let Some(file_size) = config.file_size {
                builder = builder.file_size(file_size);
            }

            Arc::new(builder.build()?)
        } else {
            Arc::new(
                FeoxStore::builder()
                    .max_memory(config.max_memory_per_shard.unwrap_or(1024 * 1024 * 1024))
                    .enable_ttl(config.enable_ttl)
                    .build()?,
            )
        };

        let (pubsub_registry, _receivers) = GlobalRegistry::new(config.threads);
        let client_registry = Arc::new(ClientRegistry::new());

        Ok(Self {
            config,
            store,
            shutdown: AtomicBool::new(false),
            active_connections: AtomicUsize::new(0),
            pubsub_registry,
            client_registry,
        })
    }

    /// Run the server, spawning worker threads
    ///
    /// This method blocks until the server is shut down.
    pub fn run(self: Arc<Self>) -> Result<()> {
        // Create TCP listener
        let listener =
            TcpListener::bind(format!("{}:{}", self.config.bind_addr, self.config.port))?;

        listener.set_nonblocking(true)?;
        let listener_fd = listener.as_raw_fd();

        info!(
            "Server listening on {}:{}",
            self.config.bind_addr, self.config.port
        );

        // Create pub/sub receivers for each thread
        let (_, mut pubsub_receivers) = GlobalRegistry::new(self.config.threads);

        // Spawn worker threads
        let mut handles = Vec::new();

        for thread_id in 0..self.config.threads {
            let server = Arc::clone(&self);
            let store = Arc::clone(&self.store);
            let pubsub_registry = Arc::clone(&self.pubsub_registry);
            let pubsub_receiver = pubsub_receivers.remove(0);
            let client_registry = Arc::clone(&self.client_registry);

            let handle = thread::spawn(move || {
                if let Err(e) = server.run_worker(
                    thread_id,
                    listener_fd,
                    store,
                    pubsub_registry,
                    pubsub_receiver,
                    client_registry,
                ) {
                    error!("Worker {} failed: {}", thread_id, e);
                }
            });
            handles.push(handle);
        }

        // Wait for all workers to finish
        for handle in handles {
            let _ = handle.join();
        }

        Ok(())
    }

    /// Signal the server to shut down gracefully
    pub fn shutdown(&self) {
        info!("Initiating server shutdown");
        self.shutdown.store(true, Ordering::Release);
    }

    /// Get the number of active client connections
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Acquire)
    }

    fn run_worker(
        self: &Arc<Self>,
        thread_id: usize,
        listener_fd: RawFd,
        store: Arc<FeoxStore>,
        pubsub_registry: Arc<GlobalRegistry>,
        pubsub_receiver: crossbeam_channel::Receiver<crate::pubsub::BroadcastMsg>,
        client_registry: Arc<ClientRegistry>,
    ) -> Result<()> {
        use mio::net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream};
        use mio::{Events, Interest, Poll, Token};
        use std::collections::HashMap;
        use std::io::{ErrorKind, Read, Write};
        use std::os::fd::FromRawFd;

        // Create mio Poll instance
        let mut poll = Poll::new()?;
        let mut events = Events::with_capacity(1024);

        // Convert raw fd to mio listener
        let std_listener = unsafe { TcpListener::from_raw_fd(listener_fd) };
        std_listener.set_nonblocking(true)?;
        let mut listener = MioTcpListener::from_std(std_listener);

        // Register listener
        const SERVER: Token = Token(0);
        poll.registry()
            .register(&mut listener, SERVER, Interest::READABLE)?;

        // Connection tracking
        let mut connections: HashMap<Token, (MioTcpStream, Connection)> = HashMap::new();
        let mut next_token = 1usize;

        // Initialize thread-local pub/sub
        let mut pubsub_manager =
            ThreadLocalPubSub::new(thread_id, pubsub_receiver, pubsub_registry.clone());

        info!("Worker {} started", thread_id);

        // Event loop
        while !self.shutdown.load(Ordering::Acquire) {
            // Process incoming pub/sub messages
            let pubsub_deliveries = pubsub_manager.process_inbox();
            for (conn_id, message) in pubsub_deliveries {
                // Find connection by ID and queue message
                for (_token, (stream, connection)) in connections.iter_mut() {
                    if connection.connection_id == conn_id {
                        connection.queue_pubsub_message(message);
                        connection.process_pubsub_messages();

                        // Write any pending data immediately
                        while let Some(data) = connection.pending_writes() {
                            let data_len = data.len();
                            match stream.write(data) {
                                Ok(n) => {
                                    connection.consume_writes(n);
                                    if n < data_len {
                                        break;
                                    }
                                }
                                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                                Err(_) => break,
                            }
                        }
                        break;
                    }
                }
            }

            // Poll for events with 100ms timeout
            poll.poll(&mut events, Some(std::time::Duration::from_millis(100)))?;

            for event in events.iter() {
                match event.token() {
                    SERVER => {
                        // Accept new connections
                        loop {
                            match listener.accept() {
                                Ok((mut stream, addr)) => {
                                    debug!("New connection from {:?}", addr);

                                    // Configure socket
                                    stream.set_nodelay(self.config.tcp_nodelay)?;

                                    let token = Token(next_token);
                                    next_token += 1;

                                    // Register for read events
                                    poll.registry().register(
                                        &mut stream,
                                        token,
                                        Interest::READABLE,
                                    )?;

                                    let mut connection = Connection::new_with_addr(
                                        0, // fd not used in this path
                                        self.config.connection_buffer_size,
                                        Arc::clone(&store),
                                        &self.config, // Pass config here
                                        Some(addr),
                                    );

                                    // Set client registry for CLIENT command support
                                    connection.set_client_registry(Arc::clone(&client_registry));

                                    // Register client in registry
                                    client_registry.register(&connection, thread_id);

                                    connections.insert(token, (stream, connection));
                                    self.active_connections.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                                Err(e) => {
                                    error!("Error accepting connection: {}", e);
                                    break;
                                }
                            }
                        }
                    }
                    token => {
                        // First, collect any deliveries that need to be made
                        let mut deliveries_to_make = Vec::new();

                        // Handle client connection
                        let should_close =
                            if let Some((stream, connection)) = connections.get_mut(&token) {
                                let mut should_close = false;

                                if event.is_readable() {
                                    // Use a simple buffer (optimize with pool later if needed)
                                    let mut buffer = vec![0u8; 8192];

                                    match stream.read(&mut buffer) {
                                        Ok(0) => {
                                            // Connection closed
                                            should_close = true;
                                        }
                                        Ok(n) => {
                                            // Process commands inline and get pub/sub operations
                                            match connection.process_read(&buffer[..n]) {
                                                Ok(pubsub_ops) => {
                                                    // Process pub/sub operations
                                                    for op in pubsub_ops {
                                                        let deliveries = handle_pubsub_operation(
                                                            &mut pubsub_manager,
                                                            &pubsub_registry,
                                                            connection.connection_id,
                                                            op,
                                                            connection,
                                                            thread_id,
                                                        );
                                                        deliveries_to_make.extend(deliveries);
                                                    }

                                                    // Process any queued pub/sub messages
                                                    connection.process_pubsub_messages();

                                                    // Update client info in registry if needed
                                                    client_registry.update(connection);

                                                    // Write response immediately
                                                    while let Some(response_data) =
                                                        connection.pending_writes()
                                                    {
                                                        let response_len = response_data.len();
                                                        match stream.write(response_data) {
                                                            Ok(n) => {
                                                                connection.consume_writes(n);
                                                                if n < response_len {
                                                                    // Partial write, would block
                                                                    break;
                                                                }
                                                            }
                                                            Err(e)
                                                                if e.kind()
                                                                    == ErrorKind::WouldBlock =>
                                                            {
                                                                break;
                                                            }
                                                            Err(e) => {
                                                                error!("Error writing: {}", e);
                                                                should_close = true;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    error!("Error processing read: {}", e);
                                                    should_close = true;
                                                }
                                            }

                                            if connection.is_closed() {
                                                should_close = true;
                                            }
                                        }
                                        Err(e) if e.kind() != ErrorKind::WouldBlock => {
                                            if e.kind() != ErrorKind::ConnectionReset {
                                                error!("Error reading: {}", e);
                                            }
                                            should_close = true;
                                        }
                                        Err(_) => {} // WouldBlock - ignore
                                    }
                                }

                                should_close
                            } else {
                                false
                            };

                        if should_close {
                            if let Some((mut stream, mut connection)) = connections.remove(&token) {
                                let _ = poll.registry().deregister(&mut stream);

                                // Clean up pub/sub subscriptions
                                pubsub_manager.connection_dropped(connection.connection_id);

                                // Unregister from client registry
                                client_registry.unregister(connection.connection_id);

                                connection.close();
                                self.active_connections.fetch_sub(1, Ordering::Relaxed);
                            }
                        }

                        // Now deliver any pub/sub messages to local connections
                        for (delivery_conn_id, msg) in deliveries_to_make {
                            for (_, (_, conn)) in connections.iter_mut() {
                                if conn.connection_id == delivery_conn_id {
                                    conn.queue_pubsub_message(msg);
                                    conn.process_pubsub_messages();
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Cleanup
        for (_, (mut stream, mut connection)) in connections {
            let _ = poll.registry().deregister(&mut stream);
            pubsub_manager.connection_dropped(connection.connection_id);
            client_registry.unregister(connection.connection_id);
            connection.close();
        }

        info!("Worker {} shutting down", thread_id);
        Ok(())
    }
}
