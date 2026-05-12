# ADR 0003: ADBC Read-Only Single-Table Scope

## Status

Accepted

## Context

`Microsoft.DI.DeltaTableService.Adbc` exposes Delta tables to ADBC consumers. The implementation is backed by the V3 Rust path and maps one Delta table path to a synthetic logical table.

## Problem

ADBC can imply database-like capabilities such as writes, transactions, prepared statements, parameter binding, and catalog discovery. The current repository does not implement those semantics.

## Decision

Keep ADBC read-only and path-scoped. A connection represents one Delta table, commonly exposed as `delta_table`. Unsupported database features fail explicitly.

## Rationale

- The strongest current value is Arrow-native read access.
- Single-table scope maps directly to Delta table path semantics.
- Avoiding writes and transactions keeps correctness boundaries clear.

## Consequences

Positive:

- Clear MVP surface.
- Lower risk of incorrect transaction semantics.
- Good fit for read-oriented Arrow consumers.

Negative:

- Generic database code may expect unsupported features.
- Multi-table catalog workflows are out of scope.
- Write-heavy ADBC consumers must use another API.

## Alternatives Considered

- Implement generic database semantics. Rejected because the underlying table-path model does not provide a real catalog.
- Add writes to ADBC immediately. Rejected to keep the initial driver scope reliable and read-focused.
