# ADR002: Integrate Feox Commands with COMMAND and WATCH

- **Status:** Accepted
- **Date:** 2026-07-27

## Context

`feox-server` exposes FeoxDB's native JSON Patch and compare-and-swap operations as custom RESP commands:

- `JSONPATCH key patch`
- `CAS key expected replacement`

The commands are parsed and executed, but they are absent from the centralized Redis `COMMAND` metadata table. Consequently, client discovery through `COMMAND`, `COMMAND LIST`, `COMMAND COUNT`, and `COMMAND INFO` does not accurately describe the server's capabilities or key positions.

Successful `JSONPATCH` and `CAS` operations also mutate keys without notifying the global `WatchRegistry`. A client watching one of those keys can therefore execute a transaction even though another connection changed the watched value. This violates Redis `WATCH` semantics.

A failed CAS does not modify data and must not invalidate a watched transaction.

## Decision

1. Register `jsonpatch` and `cas` in `COMMAND_TABLE` with their exact arities and first-key metadata.
2. Classify both commands as write operations that can allocate memory:
   - flags: `write`, `denyoom`
   - key position: first key `1`, last key `1`, step `1`
   - ACL categories: `@write`, `@string`, `@slow`
3. Notify `WatchRegistry` after a successful JSON Patch.
4. Notify `WatchRegistry` after CAS only when FeoxDB reports that the swap occurred.
5. Do not notify watchers when JSON Patch returns an error or CAS returns `0`.
6. Add integration tests for command discovery and cross-connection `WATCH` behavior.

## Consequences

### Positive

- Redis clients can discover both Feox-specific commands and their key positions.
- `COMMAND COUNT`, `COMMAND LIST`, and `COMMAND INFO` remain consistent with executable commands.
- Transactions correctly abort when another connection changes a watched key with `JSONPATCH` or a successful `CAS`.
- Failed CAS attempts do not create false transaction conflicts.

### Negative

- These remain non-standard Redis commands, so clients must invoke them through their custom/raw command API.
- Existing clients that assert an exact `COMMAND COUNT` must account for two additional commands.

## Validation

Integration tests verify that:

- `COMMAND LIST` includes `jsonpatch` and `cas`.
- `COMMAND INFO JSONPATCH CAS` returns their names, arities, write flags, and first-key metadata.
- Successful JSON Patch and CAS operations invalidate another connection's watched transaction.
- A CAS mismatch does not invalidate a watched transaction.
