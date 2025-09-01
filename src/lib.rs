//! FeOx-server: High-performance Redis-compatible server for FeOxDB
//!
//! This crate provides a Redis protocol-compatible server that uses FeOxDB
//! as its storage engine, achieving sub-microsecond latencies.
//!
//! # Architecture
//!
//! - Thread-per-core model with CPU affinity
//! - mio-based event loop for cross-platform async I/O
//! - Zero-copy RESP protocol implementation
//! - Lock-free data structures where possible

/// Configuration management for the server
pub mod config;

/// Error types and result aliases
pub mod error;

/// I/O utilities including buffer pooling
pub mod io;

/// Network layer for connection management
pub mod network;

/// Redis protocol (RESP) implementation
pub mod protocol;

/// Core server implementation
pub mod server;

pub use config::Config;
pub use error::{Error, Result};
pub use server::Server;
