# ADR 0002: Arrow-First Data Model

## Status

Accepted

## Context

The client SDK, Flight backends, native Rust interop, and ADBC driver all exchange data through Apache Arrow concepts.

Primary public read APIs expose `IAsyncEnumerable<RecordBatch>` or `IArrowArrayStream`. Row-oriented APIs such as `DbDataReader` are adapters.

## Problem

The SDK needs to support large Delta tables efficiently while still integrating with .NET consumers that expect rows or `DataTable` objects.

## Decision

Use Arrow batches and streams as the canonical data model. Provide row and materialization helpers as opt-in convenience layers.

## Rationale

- Arrow is the common format across Flight, native interop, and ADBC.
- Streaming batches avoid full-table buffering.
- Arrow preserves columnar semantics for analytics workloads.

## Consequences

Positive:

- Efficient large-table reads.
- Natural ADBC and Arrow-native integration.
- Consistent data model across backends.

Negative:

- Consumers must understand streaming and columnar processing.
- Materialization helpers can be misused on large data.
- Decimal and complex Arrow type conversion requires explicit handling.

## Alternatives Considered

- Make `DataTable` the default result. Rejected because it encourages buffering and weakens Arrow-native integration.
- Expose only Arrow streams. Rejected because common .NET integrations still need row adapters.
