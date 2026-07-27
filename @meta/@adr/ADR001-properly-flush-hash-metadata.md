# ADR001: Properly Flush Persistent and Hash Metadata

- **Status:** Accepted
- **Date:** 2026-07-27

## Context

When `feox-server` was started with `--data-path`, FeoxDB created the configured data file and accepted reads and writes, but data was unavailable after stopping and restarting the server.

The failure had several related causes:

1. The process-wide signal handler retained a strong `Arc<Server>`, preventing `FeoxStore` cleanup from running before process exit.
2. Graceful shutdown stopped network workers but did not explicitly flush FeoxDB's write buffer and metadata.
3. Batched hash-length metadata could remain pending when the underlying store was flushed, causing hash fields and `HLEN` metadata to disagree after recovery.
4. A newly created persistent store did not immediately receive a valid metadata header. Although FeoxDB could background-flush records, an abrupt outage before graceful shutdown left no valid header from which recovery could begin.
5. Files created by affected server versions could contain valid records while retaining a zero-filled metadata header.
6. Worker threads constructed multiple owning `TcpListener` instances from the same raw file descriptor, risking double-close failures during graceful shutdown.

FeoxDB intentionally uses write-behind persistence with an approximately 100ms durability window. The server must preserve this performance model while ensuring all writes outside that window are recoverable and graceful shutdown is fully durable.

## Decision

The server will use the following persistence lifecycle:

1. **Initialize persistent metadata at startup.** After opening or creating a persistent `FeoxStore`, synchronously call `FeoxStore::flush()` so a valid recovery header exists before serving clients.
2. **Recover legacy uninitialized files.** If an existing data file has a zero-filled FeoxDB metadata signature, open it, write the missing metadata, close it, and reopen it so FeoxDB immediately scans and rebuilds indexes from existing records.
3. **Flush after command processing stops.** `Server::run` waits for all network workers to finish and then creates a synchronous durable checkpoint with `Server::flush()`.
4. **Flush hash metadata first.** Before flushing FeoxDB, apply all pending batched hash metadata updates so persisted hash fields and hash lengths remain consistent.
5. **Do not retain the server from the signal handler.** The process-wide Ctrl-C/SIGTERM handler keeps a `Weak<Server>` rather than a strong `Arc<Server>`, allowing normal store cleanup.
6. **Give each worker an owned listener clone.** Workers receive descriptors created with `TcpListener::try_clone()` instead of constructing multiple owners from one raw descriptor.
7. **Retain FeoxDB's write-behind model.** Normal commands are not synchronously fsynced. Abrupt downtime may lose writes still within FeoxDB's documented write-behind window, but background-flushed records remain recoverable.

## Consequences

### Positive

- Data written with `--data-path` survives graceful server restarts.
- Background-flushed data survives abrupt process or machine downtime.
- Existing affected data files are recovered automatically on their first startup with the fixed server.
- Hash field data and hash-length metadata are checkpointed together.
- Graceful shutdown no longer depends solely on destructor timing.
- Listener ownership is memory- and descriptor-safe during shutdown.

### Negative

- Persistent startup performs an additional synchronous metadata flush.
- The first startup of a legacy uninitialized file requires an extra open/close/reopen cycle and a full FeoxDB recovery scan.
- Graceful shutdown waits for pending writes and metadata to reach durable storage.
- As required by FeoxDB's design, writes inside the final write-behind window can still be lost during an abrupt outage.

## Validation

The decision is covered by `tests/persistence.rs`, including:

- String and hash data surviving SIGINT shutdown and restart.
- Pending hash-length metadata surviving restart.
- Background-flushed data surviving SIGKILL and restart.

It was additionally validated with Bun.js Redis-protocol scripts and by recovering a legacy data file containing records under a zero-filled metadata header.
