# FeOx Server

Ultra-fast Redis-compatible server powered by FeOxDB with sub-microsecond latency.

[📚 FeOxDB Documentation](https://feoxdb.com) | [💬 Issues](https://github.com/mehrantoosi/feox/issues)

## Features

- **Extreme Performance**: 3.7M+ SET/s, 5M+ GET/s with pipelining
- **Redis Protocol Compatible**: Drop-in replacement for Redis workloads
- **Sub-microsecond Latency**: Inherits FeOxDB's <200ns GET operations
- **Thread-per-Core Architecture**: Scales linearly with CPU cores
- **Lock-Free Operations**: Built on FeOxDB's lock-free data structures

## Performance

Benchmarked on a standard development machine with 16 cores:

| Operation | Requests/sec | Latency (p50) |
|-----------|-------------|---------------|
| SET       | 3,770,000   | 0.62ms        |
| GET       | 5,000,000   | 0.35ms        |

*Using redis-benchmark with 50 clients, pipeline depth 64, and 1M random keys*

## Quick Start

### Installation

```bash
# Install from crates.io
cargo install feox-server

# Or build from source
git clone https://github.com/mehrantoosi/feox-server
cd feox-server
cargo build --release
```

### Basic Usage

```bash
# Start server on default port (6379)
feox-server

# Or if built from source
./target/release/feox-server

# Custom configuration
feox-server \
  --port 6380 \
  --bind 0.0.0.0 \
  --threads 8 \
  --data-path /var/lib/feox/data.db
```

### Testing with Redis CLI

```bash
# Connect with redis-cli
redis-cli -p 6379

# Basic operations
SET key value
GET key
INCR counter
EXPIRE key 60
```

## Supported Commands

### Basic Operations
- `GET key` - Get value by key
- `SET key value [EX seconds]` - Set key with optional expiry
- `DEL key [key ...]` - Delete one or more keys
- `EXISTS key [key ...]` - Check if keys exist

### List Operations
- `LPUSH key value [value ...]` - Push values to the head of list
- `RPUSH key value [value ...]` - Push values to the tail of list
- `LPOP key [count]` - Pop values from the head of list
- `RPOP key [count]` - Pop values from the tail of list
- `LLEN key` - Get the length of a list
- `LRANGE key start stop` - Get a range of elements from a list
- `LINDEX key index` - Get an element from a list by index

### Atomic Operations
- `INCR key` - Increment integer value
- `INCRBY key delta` - Increment by specific amount
- `DECR key` - Decrement integer value
- `DECRBY key delta` - Decrement by specific amount

### TTL Operations
- `EXPIRE key seconds` - Set expiration in seconds
- `TTL key` - Get remaining TTL in seconds
- `PERSIST key` - Remove expiration

### Bulk Operations
- `MGET key [key ...]` - Get multiple values
- `MSET key value [key value ...]` - Set multiple key-value pairs

### Server Commands
- `AUTH password` - Authenticate connection
- `PING [message]` - Test connection
- `INFO [section]` - Server information
- `CONFIG GET/SET` - Configuration management
- `KEYS pattern` - Find keys by pattern
- `SCAN cursor [MATCH pattern] [COUNT count]` - Incremental key iteration

### FeOx-Specific
- `JSONPATCH key patch` - Apply JSON Patch (RFC 6902)
- `CAS key expected new_value` - Compare-and-swap operation

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `--port` | 6379 | Port to listen on |
| `--bind` | 127.0.0.1 | Bind address |
| `--threads` | CPU count | Number of worker threads |
| `--data-path` | None | Path to persistent storage (memory-only if not set) |
| `--log-level` | info | Logging level (trace/debug/info/warn/error) |
| `--requirepass` | None | Password for AUTH command |

## Authentication

FeOx-server supports Redis-compatible AUTH command for basic access control.

### Configuration

```bash
# Via command line
./feox-server --requirepass yourpassword

# Via environment variable
FEOX_AUTH_PASSWORD=yourpassword ./feox-server

# Via config file (config.toml)
requirepass = "yourpassword"
```

### Using with Redis Clients

```bash
# Authenticate with redis-cli
redis-cli -p 6379
> AUTH yourpassword
OK

# Or use -a flag
redis-cli -p 6379 -a yourpassword
```

### Security Warning ⚠️

**AUTH credentials are sent in PLAINTEXT** over the network, exactly like Redis.

For production use:
1. **Bind to localhost only** (`--bind 127.0.0.1`)
2. **Use SSH tunnels** for remote access
3. **Use firewalls** to restrict network access

## Building from Source

### Requirements

- Rust 1.70 or later
- Linux/macOS (Windows support experimental)
- For best performance: Linux with io_uring support (for Feox persistence if enabled)

## Benchmarking

```bash
# Using redis-benchmark
redis-benchmark -n 1000000 -r 1000000 -c 50 -P 64 -t SET,GET
```

## Known Limitations

### Concurrent Updates to Same Key in macOS and Windows
When multiple clients rapidly update the same key simultaneously, you may encounter "Timestamp is older than existing record" errors. This is by design - FeOx uses timestamp-based optimistic concurrency control for consistency.

**Impact:**
- Only affects concurrent updates to the same ke, sent in less than 1μs apart
- Normal usage with different keys is unaffected
- Performance with random keys: 3.7M SET/s, 5.0M GET/s

For benchmarking, use the `-r` flag with redis-benchmark to test with random keys

This is a limitation of the said operating systems on system time resolution in user space.

### Currently Not Supported (compared to Redis)
- Sets (SADD, SMEMBERS, etc.)
- Sorted Sets (ZADD, ZRANGE, etc.)
- Hashes (HSET, HGET, etc.)
- Pub/Sub (PUBLISH, SUBSCRIBE, etc.)
- Transactions (MULTI, EXEC)
- Lua scripting
- Some list operations (LINSERT, LREM, LSET, LTRIM, BLPOP, BRPOP, etc.)

## License

Apache License 2.0 - See [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Security

For security issues, please see [SECURITY.md](SECURITY.md).
