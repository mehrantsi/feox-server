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

        Ok(Self {
            config,
            store,
            shutdown: AtomicBool::new(false),
            active_connections: AtomicUsize::new(0),
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

        // Spawn worker threads
        let mut handles = Vec::new();

        for thread_id in 0..self.config.threads {
            let server = Arc::clone(&self);
            let store = Arc::clone(&self.store);
            let handle = thread::spawn(move || {
                if let Err(e) = server.run_worker(thread_id, listener_fd, store) {
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

        info!("Worker {} started", thread_id);

        // Event loop
        while !self.shutdown.load(Ordering::Acquire) {
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

                                    let connection = Connection::new(
                                        0, // fd not used in this path
                                        self.config.connection_buffer_size,
                                        Arc::clone(&store),
                                    );

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
                        // Handle client connection
                        let should_close = if let Some((stream, connection)) =
                            connections.get_mut(&token)
                        {
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
                                        // Process commands inline
                                        if let Err(e) = connection.process_read(&buffer[..n]) {
                                            error!("Error processing read: {}", e);
                                            should_close = true;
                                        } else {
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
                                connection.close();
                                self.active_connections.fetch_sub(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        }

        // Cleanup
        for (_, (mut stream, mut connection)) in connections {
            let _ = poll.registry().deregister(&mut stream);
            connection.close();
        }

        info!("Worker {} shutting down", thread_id);
        Ok(())
    }
}
