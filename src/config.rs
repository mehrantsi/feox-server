use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

/// Server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Bind address
    pub bind_addr: String,

    /// Port to listen on
    pub port: u16,

    /// Number of worker threads
    pub threads: usize,

    /// Path to FeOx data file (None for memory-only)
    pub data_path: Option<String>,

    /// Maximum connections per thread
    pub max_connections_per_thread: usize,

    /// Connection buffer size (per connection)
    pub connection_buffer_size: usize,

    /// TCP nodelay
    pub tcp_nodelay: bool,

    /// Pipeline queue depth
    pub max_pipeline_depth: usize,

    /// Enable NUMA awareness
    pub numa_aware: bool,

    /// Maximum memory for FeOx store (per shard)
    pub max_memory_per_shard: Option<usize>,

    /// Enable TTL support
    pub enable_ttl: bool,

    /// File size for persistent storage (in bytes)
    /// Only used when data_path is set
    pub file_size: Option<u64>,

    /// Log level
    pub log_level: String,

    /// Password for AUTH command (Redis-compatible)
    /// None means no authentication required
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requirepass: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1".to_string(),
            port: 6379,
            threads: num_cpus::get(),
            data_path: None,
            max_connections_per_thread: 10000,
            connection_buffer_size: 16 * 1024, // 16KB
            tcp_nodelay: true,
            max_pipeline_depth: 1000,
            numa_aware: false,
            max_memory_per_shard: Some(1024 * 1024 * 1024), // 1GB per shard
            enable_ttl: true,
            file_size: Some(10 * 1024 * 1024 * 1024), // 10GB default for persistent storage
            log_level: "info".to_string(),
            requirepass: None,
        }
    }
}

impl Config {
    /// Load configuration from a TOML file
    ///
    /// # Example
    ///
    /// ```no_run
    /// use feox_server::Config;
    ///
    /// # fn main() -> anyhow::Result<()> {
    /// let config = Config::from_file("config.toml")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let contents = fs::read_to_string(path)?;
        let mut config: Config = toml::from_str(&contents)?;

        // Load password from environment if not in config file
        if config.requirepass.is_none() {
            if let Ok(password) = std::env::var("FEOX_AUTH_PASSWORD") {
                config.requirepass = Some(password);
                // Clear from environment after reading
                std::env::remove_var("FEOX_AUTH_PASSWORD");
            }
        }

        Ok(config)
    }

    /// Save configuration to a TOML file
    pub fn to_file<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        let contents = toml::to_string_pretty(self)?;
        fs::write(path, contents)?;
        Ok(())
    }

    /// Validate configuration
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.threads == 0 {
            anyhow::bail!("threads must be > 0");
        }

        if self.port == 0 {
            anyhow::bail!("port must be > 0");
        }

        if self.connection_buffer_size < 1024 {
            anyhow::bail!("connection_buffer_size must be >= 1024");
        }

        Ok(())
    }

    /// Check if authentication is required
    pub fn auth_required(&self) -> bool {
        self.requirepass.is_some()
    }

    /// Validate password (constant-time comparison)
    pub fn check_password(&self, password: &str) -> bool {
        match &self.requirepass {
            Some(correct) => constant_time_eq(password.as_bytes(), correct.as_bytes()),
            None => false,
        }
    }
}

/// Constant-time string comparison to prevent timing attacks
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let mut result = 0u8;
    for (byte_a, byte_b) in a.iter().zip(b.iter()) {
        result |= byte_a ^ byte_b;
    }
    result == 0
}
