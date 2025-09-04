use clap::Parser;
use feox_server::{Config, Server};
use std::sync::Arc;
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Port to listen on
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    /// Bind address
    #[arg(short, long, default_value = "127.0.0.1")]
    bind: String,

    /// Number of worker threads (0 = number of CPUs)
    #[arg(short = 't', long, default_value_t = 0)]
    threads: usize,

    /// Path to FeOx data file (memory-only if not specified)
    #[arg(short = 'd', long)]
    data_path: Option<String>,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,

    /// Config file path
    #[arg(short, long)]
    config: Option<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Password for AUTH command
    #[arg(long)]
    requirepass: Option<String>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Initialize tracing
    let log_level = if args.verbose {
        "debug"
    } else {
        &args.log_level
    };
    tracing_subscriber::fmt()
        .with_env_filter(format!("feox_server={},feoxdb=info", log_level))
        .init();

    info!(
        "Starting FeOx Server v{} on {}:{}",
        env!("CARGO_PKG_VERSION"),
        args.bind,
        args.port
    );

    // Detect CPU configuration
    let num_cpus = num_cpus::get();
    let threads = if args.threads == 0 {
        num_cpus
    } else {
        args.threads
    };

    info!(
        "Detected {} CPUs, using {} worker threads",
        num_cpus, threads
    );

    // Create configuration
    let config = if let Some(config_path) = args.config {
        Config::from_file(&config_path)?
    } else {
        let mut config = Config {
            bind_addr: args.bind,
            port: args.port,
            threads,
            data_path: args.data_path,
            ..Default::default()
        };

        // Set password from command line or environment
        if let Some(password) = args.requirepass {
            if password.is_empty() {
                warn!("Empty password provided, authentication disabled");
            } else {
                info!("Authentication enabled (password configured)");
                config.requirepass = Some(password);
            }
        }

        config
    };

    // Security warning
    if config.requirepass.is_some()
        && config.bind_addr != "127.0.0.1"
        && config.bind_addr != "localhost"
    {
        warn!(
            "WARNING: Authentication is enabled but server is bound to {}. \
            AUTH credentials will be sent in PLAINTEXT over the network. \
            Consider binding to localhost only or using SSH tunnels for remote access.",
            config.bind_addr
        );
    }

    // Create and run server
    let server = Arc::new(Server::new(config)?);

    // Setup signal handlers for graceful shutdown
    let server_clone = Arc::clone(&server);
    ctrlc::set_handler(move || {
        info!("Received shutdown signal, shutting down gracefully...");
        server_clone.shutdown();
    })?;

    // Run the server
    if let Err(e) = server.run() {
        error!("Server error: {}", e);
        return Err(e.into());
    }

    info!("Server shutdown complete");
    Ok(())
}
