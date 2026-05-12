# ADR 0005: Per-Request Storage Options

## Status

Accepted

## Context

Client operations accept `StorageConfig` and/or `GenericStorageOptions` so callers can provide credentials and object-store settings per call.

## Problem

Delta integrations often access multiple storage accounts or rotate credentials. Global process state would make that difficult and error-prone.

## Decision

Prefer per-request storage options instead of global mutable storage configuration.

## Rationale

- Credentials stay scoped to individual operations.
- Multi-account workflows are possible in one process.
- Tests can pass explicit storage settings without hidden global setup.

## Consequences

Positive:

- Better credential isolation.
- Clearer call-site behavior.
- Easier multi-storage integrations.

Negative:

- Method overloads become verbose.
- Agents may mix `StorageConfig` and `GenericStorageOptions` without explaining why.
- Secret-bearing dictionaries require careful logging discipline.

## Alternatives Considered

- Use global environment variables only. Rejected because it hides state and complicates multi-account usage.
- Require a single connection-level storage config. Rejected because the client supports per-operation table paths and credentials.
