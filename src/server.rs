use crate::client_registry::ClientRegistry;
use crate::pubsub::{handle_pubsub_operation, GlobalRegistry, ThreadLocalPubSub};
use crate::{config::Config, error::Result, network::Connection};
use feoxdb::FeoxStore;
use std::fs::File;
use std::io::Read;
use std::net::TcpListener;
use std::path::Path;
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
    client_registry: Arc<ClientRegistry>,
}

impl Server {
    /// Create a new server with the given configuration
    pub fn new(config: Config) -> Result<Self> {
        config.validate()?;

        // Older server versions never finalized FeoxDB metadata because the
        // process-wide signal handler retained the store. Recover those files by
        // writing the missing metadata header, then reopening so FeoxDB scans the
        // existing records immediately (rather than only after another restart).
        if let Some(ref data_path) = config.data_path {
            if Self::has_uninitialized_metadata(data_path)? {
                info!(
                    "Persistent store metadata is uninitialized; rebuilding {}",
                    data_path
                );
                let bootstrap_store = Self::build_store(&config)?;
                bootstrap_store.flush()?;
                drop(bootstrap_store);
            }
        }

        // Create a single shared FeoxStore instance.
        let store = Arc::new(Self::build_store(&config)?);

        // Initialize metadata as soon as a new persistent database is created.
        // This makes records written by FeoxDB's background writer recoverable
        // even if the machine goes down before the first graceful shutdown.
        if config.data_path.is_some() {
            store.flush()?;
        }

        let client_registry = Arc::new(ClientRegistry::new());

        Ok(Self {
            config,
            store,
            shutdown: AtomicBool::new(false),
            active_connections: AtomicUsize::new(0),
            client_registry,
        })
    }

    fn build_store(config: &Config) -> Result<FeoxStore> {
        let mut builder = FeoxStore::builder()
            .max_memory(config.max_memory_per_shard.unwrap_or(1024 * 1024 * 1024))
            .enable_ttl(config.enable_ttl);

        if let Some(ref data_path) = config.data_path {
            builder = builder.device_path(data_path.clone());
            if let Some(file_size) = config.file_size {
                builder = builder.file_size(file_size);
            }
        }

        Ok(builder.build()?)
    }

    fn has_uninitialized_metadata<P: AsRef<Path>>(path: P) -> std::io::Result<bool> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(false);
        }

        let file_len = path.metadata()?.len();
        if file_len < feoxdb::constants::FEOX_SIGNATURE_SIZE as u64 {
            return Ok(false);
        }

        let mut file = File::open(path)?;
        let mut signature = [0u8; feoxdb::constants::FEOX_SIGNATURE_SIZE];
        file.read_exact(&mut signature)?;

        // FeoxDB preallocates new stores with zero-filled metadata. Do not alter
        // non-Feox files whose metadata starts with some other nonzero value.
        Ok(signature.iter().all(|byte| *byte == 0))
    }

    /// Run the server, spawning worker threads
    ///
    /// This method blocks until the server is shut down.
    pub fn run(self: Arc<Self>) -> Result<()> {
        // Create TCP listener
        let listener =
            TcpListener::bind(format!("{}:{}", self.config.bind_addr, self.config.port))?;

        listener.set_nonblocking(true)?;

        info!(
            "Server listening on {}:{}",
            self.config.bind_addr, self.config.port
        );

        // Create pub/sub registry and receivers
        let (pubsub_registry, mut pubsub_receivers) = GlobalRegistry::new(self.config.threads);

        // Spawn worker threads
        let mut handles = Vec::new();

        for thread_id in 0..self.config.threads {
            let server = Arc::clone(&self);
            let store = Arc::clone(&self.store);
            let pubsub_registry = Arc::clone(&pubsub_registry);
            let pubsub_receiver = pubsub_receivers.remove(0);
            let client_registry = Arc::clone(&self.client_registry);
            // Each worker must own a distinct cloned descriptor. Constructing
            // multiple TcpListeners from the same raw fd causes double-close and
            // can abort during graceful shutdown.
            let worker_listener = listener.try_clone()?;

            let handle = thread::spawn(move || {
                if let Err(e) = server.run_worker(
                    thread_id,
                    worker_listener,
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

        // Wait for all workers to finish. No commands can mutate the store after
        // this point, so it is safe to create a durable shutdown checkpoint.
        for handle in handles {
            let _ = handle.join();
        }

        self.flush()?;
        Ok(())
    }

    /// Signal the server to shut down gracefully.
    ///
    /// [`Server::run`] flushes persistent data after all workers have stopped.
    pub fn shutdown(&self) {
        info!("Initiating server shutdown");
        self.shutdown.store(true, Ordering::Release);
    }

    /// Flush all pending writes to durable storage.
    ///
    /// This is a no-op when the server is configured for memory-only storage.
    pub fn flush(&self) -> Result<()> {
        crate::protocol::flush_hash_metadata(&self.store);
        self.store.flush()?;
        info!("Persistent data flushed to disk");
        Ok(())
    }

    /// Get the number of active client connections
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Acquire)
    }

    fn run_worker(
        self: &Arc<Self>,
        thread_id: usize,
        listener: TcpListener,
        store: Arc<FeoxStore>,
        pubsub_registry: Arc<GlobalRegistry>,
        pubsub_receiver: crossbeam_channel::Receiver<crate::pubsub::BroadcastMsg>,
        client_registry: Arc<ClientRegistry>,
    ) -> Result<()> {
        use mio::net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream};
        use mio::{Events, Interest, Poll, Token};
        use std::collections::HashMap;
        use std::io::{ErrorKind, Read, Write};

        // Create mio Poll instance
        let mut poll = Poll::new()?;
        let mut events = Events::with_capacity(1024);

        // Convert this worker's owned listener clone to a mio listener.
        listener.set_nonblocking(true)?;
        let mut listener = MioTcpListener::from_std(listener);

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
                        let should_close = if let Some((stream, connection)) =
                            connections.get_mut(&token)
                        {
                            let mut should_close = false;

                            // Handle writable event - continue writing pending data
                            if event.is_writable() {
                                while let Some(response_data) = connection.pending_writes() {
                                    let response_len = response_data.len();
                                    match stream.write(response_data) {
                                        Ok(n) => {
                                            connection.consume_writes(n);
                                            if n < response_len {
                                                break;
                                            }
                                        }
                                        Err(e) if e.kind() == ErrorKind::WouldBlock => {
                                            break;
                                        }
                                        Err(e) => {
                                            error!("Error writing: {}", e);
                                            should_close = true;
                                            break;
                                        }
                                    }
                                }

                                // If all data written, switch back to READABLE only
                                if connection.pending_writes().is_none() {
                                    let _ = poll.registry().reregister(
                                        stream,
                                        token,
                                        Interest::READABLE,
                                    );
                                }
                            }

                            if event.is_readable() {
                                // Loop to read all available data (important for edge-triggered kqueue)
                                let mut buffer = vec![0u8; 65536];
                                loop {
                                    match stream.read(&mut buffer) {
                                        Ok(0) => {
                                            // Connection closed
                                            should_close = true;
                                            break;
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

                                                    // If there's still data to write, register for WRITABLE
                                                    if connection.pending_writes().is_some() {
                                                        let _ = poll.registry().reregister(
                                                            stream,
                                                            token,
                                                            Interest::READABLE | Interest::WRITABLE,
                                                        );
                                                    }
                                                }
                                                Err(e) => {
                                                    error!("Error processing read: {}", e);
                                                    should_close = true;
                                                    break;
                                                }
                                            }

                                            if connection.is_closed() {
                                                should_close = true;
                                                break;
                                            }
                                        }
                                        Err(e) if e.kind() == ErrorKind::WouldBlock => {
                                            // No more data available, exit read loop
                                            break;
                                        }
                                        Err(e) => {
                                            if e.kind() != ErrorKind::ConnectionReset {
                                                error!("Error reading: {}", e);
                                            }
                                            should_close = true;
                                            break;
                                        }
                                    }
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
                            for (_, (stream, conn)) in connections.iter_mut() {
                                if conn.connection_id == delivery_conn_id {
                                    conn.queue_pubsub_message(msg);
                                    conn.process_pubsub_messages();

                                    // Write the queued messages to the socket
                                    while let Some(response_data) = conn.pending_writes() {
                                        let response_len = response_data.len();
                                        match stream.write(response_data) {
                                            Ok(n) => {
                                                conn.consume_writes(n);
                                                if n < response_len {
                                                    break;
                                                }
                                            }
                                            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                                                break;
                                            }
                                            Err(e) => {
                                                error!("Error writing pub/sub message: {}", e);
                                                break;
                                            }
                                        }
                                    }
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
