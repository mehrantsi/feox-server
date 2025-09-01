use crate::protocol::resp::write_resp_value;
use crate::protocol::{Command, CommandExecutor, RespParser};
use feoxdb::FeoxStore;
use std::os::fd::RawFd;
use std::sync::Arc;

/// Manages a client connection with RESP protocol handling
///
/// Handles command parsing, execution, and response buffering.
pub struct Connection {
    fd: RawFd,

    // Protocol parser
    parser: RespParser,
    executor: CommandExecutor,

    // Single consolidated write buffer for better performance
    write_buffer: Vec<u8>,
    write_position: usize,

    // Pipeline tracking
    pipeline_depth: usize,

    // Connection state
    closed: bool,
}

impl Connection {
    /// Create a new connection handler
    pub fn new(fd: RawFd, buffer_size: usize, store: Arc<FeoxStore>) -> Self {
        let executor = CommandExecutor::new(store);
        Self {
            fd,
            parser: RespParser::new(),
            executor,
            write_buffer: Vec::with_capacity(buffer_size),
            write_position: 0,
            pipeline_depth: 0,
            closed: false,
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

    /// Process incoming data with inline execution
    pub fn process_read(&mut self, data: &[u8]) -> crate::error::Result<()> {
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
            // Parse command
            let command = Command::from_resp(resp_value).map_err(crate::error::Error::Protocol)?;

            // Check for quit
            if matches!(command, Command::Quit) {
                self.closed = true;
                self.write_buffer.extend_from_slice(b"+OK\r\n");
                return Ok(());
            }

            // Execute command and write response directly to buffer
            let response = self.executor.execute(command);
            write_resp_value(&mut self.write_buffer, &response);

            self.pipeline_depth += 1;
        }

        Ok(())
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
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.close();
    }
}
