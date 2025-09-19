# FeOx Server

Ultra-fast Redis-compatible server powered by [Feox DB](https://feoxdb.com).

## Features

- **High Performance**: 3.8M+ SET/s, 5M+ GET/s (redis-benchmark with 50 clients and pipeline depth 64)
- **Redis Protocol Compatible**: Drop-in replacement for Redis workloads
- **Thread-per-Core Architecture**: Scales linearly with CPU cores
- **Lock-Free Operations**: Built on FeOxDB's lock-free data structures

## Performance

Testing with `memtier_benchmark` on macOS (16 cores, 50 clients, 100K requests, pipeline 16):

| Workload | FeOx (ops/sec) | Redis (ops/sec) | Speedup | FeOx p50 | Redis p50 |
|----------|----------------|-----------------|---------|----------|-----------|
| **Cache Simulation** (50% SET, 50% GET) | 2,980,228 | 1,492,622 | **2.0x** | 1.09ms | 2.08ms |
| **Session Store** (25% SET, 75% GET) | 3,002,196 | 1,601,475 | **1.9x** | 1.06ms | 1.91ms |
| **Content Cache** (10% SET, 90% GET) | 2,785,080 | 1,370,185 | **2.0x** | 1.13ms | 2.27ms |
| **Pub/Sub** (PUBLISH only) | 138,000 | 142,857 | 0.97x | 0.047ms | 0.039ms |

Testing with `redis-benchmark` (1M random keys, 50 clients, pipeline 64):

| Command | FeOx (ops/sec) | Redis (ops/sec) | Speedup |
|---------|----------------|-----------------|---------|
| **SET** | 3,952,569 | 1,538,461 | **2.6x** |
| **GET** | 5,025,125 | 2,004,008 | **2.5x** |

### Highlights

- **2x faster** for typical cache workloads with **45-50% lower latency**
- **Consistent performance** across different read/write ratios
- **Better p99 latency** consistency under load

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

### Hash Operations
- `HSET key field value [field value ...]` - Set hash field(s)
- `HGET key field` - Get value of a hash field
- `HMGET key field [field ...]` - Get values of multiple hash fields
- `HDEL key field [field ...]` - Delete one or more hash fields
- `HEXISTS key field` - Check if a hash field exists
- `HGETALL key` - Get all fields and values in a hash
- `HLEN key` - Get the number of fields in a hash
- `HKEYS key` - Get all field names in a hash
- `HVALS key` - Get all values in a hash
- `HINCRBY key field increment` - Increment the integer value of a hash field

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

### Pub/Sub Operations
- `SUBSCRIBE channel [channel ...]` - Subscribe to channels
- `UNSUBSCRIBE [channel ...]` - Unsubscribe from channels
- `PSUBSCRIBE pattern [pattern ...]` - Subscribe to channel patterns
- `PUNSUBSCRIBE [pattern ...]` - Unsubscribe from patterns
- `PUBLISH channel message` - Publish message to channel
- `PUBSUB CHANNELS [pattern]` - List active channels
- `PUBSUB NUMSUB [channel ...]` - Get subscriber count for channels
- `PUBSUB NUMPAT` - Get pattern subscriber count

### Server Commands
- `AUTH password` - Authenticate connection
- `PING [message]` - Test connection
- `INFO [section]` - Server information
- `CONFIG GET/SET` - Configuration management
- `KEYS pattern` - Find keys by pattern
- `SCAN cursor [MATCH pattern] [COUNT count]` - Incremental key iteration

### Client Management Commands
- `CLIENT ID` - Returns the current connection ID
- `CLIENT LIST` - Lists all connected clients with detailed information
- `CLIENT INFO` - Returns information about the current connection
- `CLIENT SETNAME name` - Sets a name for the current connection
- `CLIENT GETNAME` - Returns the name of the current connection
- `CLIENT KILL [ID id] [ADDR addr] [TYPE type]` - Terminates client connections
- `CLIENT PAUSE timeout` - Suspends command processing for all clients
- `CLIENT UNPAUSE` - Resumes command processing for all clients

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

This is a limitation of the said OS on system time resolution in user space.

### Currently Not Supported (compared to Redis)
- Sets (SADD, SMEMBERS, etc.)
- Sorted Sets (ZADD, ZRANGE, etc.)
- Transactions (MULTI, EXEC)
- Lua scripting
- Additional hash operations (HINCRBYFLOAT, HSETNX, HSTRLEN, HSCAN, etc.)
- Some list operations (LINSERT, LREM, LSET, LTRIM, BLPOP, BRPOP, etc.)
- Some client operations (CLIENT CACHING, CLIENT TRACKING, CLIENT GETREDIR, etc.)

## License

Apache License 2.0 - See [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Security

For security issues, please see [SECURITY.md](SECURITY.md).
