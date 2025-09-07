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

/// Client registry for connection management
pub mod client_registry;

/// Configuration management for the server
pub mod config;

/// Error types and result aliases
pub mod error;

/// Network layer for connection management
pub mod network;

/// Redis protocol (RESP) implementation
pub mod protocol;

/// Pub/Sub implementation
pub mod pubsub;

/// Core server implementation
pub mod server;

pub use client_registry::ClientRegistry;
pub use config::Config;
pub use error::{Error, Result};
pub use server::Server;
