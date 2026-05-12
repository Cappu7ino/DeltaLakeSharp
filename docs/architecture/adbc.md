# ADBC Architecture

## Summary

`Microsoft.DI.DeltaTableService.Adbc` is a read-only, path-scoped ADBC driver backed by the V3 Rust client path. It is optimized for Arrow-native read consumers, not database administration or write orchestration.

## Main Types

| Type | Role |
| --- | --- |
| `DeltaAdbcDriver` | Driver entry point and connection factory. |
| `DeltaAdbcConnection` | Holds connection options and creates statements. |
| `DeltaAdbcStatement` | Executes direct reads, SQL reads, partition reads, and CDF reads. |
| `DeltaAdbcConnectOptions` | Parses connection-level options. |
| `DeltaAdbcStatementOptions` | Holds effective per-statement table/version/storage/CDF options. |
| `DeltaAdbcClientAdapter` | Bridges ADBC statements to `DeltaTableServiceClient(ServiceMode.V3_Rust)`. |

Sources:

- [../../src/DeltaTableService.Adbc/DeltaAdbcDriver.cs](../../src/DeltaTableService.Adbc/DeltaAdbcDriver.cs)
- [../../src/DeltaTableService.Adbc/DeltaAdbcConnection.cs](../../src/DeltaTableService.Adbc/DeltaAdbcConnection.cs)
- [../../src/DeltaTableService.Adbc/DeltaAdbcStatement.cs](../../src/DeltaTableService.Adbc/DeltaAdbcStatement.cs)
- [../../src/DeltaTableService.Adbc/Internal/DeltaAdbcClientAdapter.cs](../../src/DeltaTableService.Adbc/Internal/DeltaAdbcClientAdapter.cs)

## Runtime Dependency

ADBC always uses V3 Rust through `DeltaAdbcClientAdapter`. It does not connect to V1 or V2 Flight services.

Implications:

- Native runtime assets are required.
- CDF and partitioned reads use V3 behavior.
- ADBC docs should describe it as a native Rust offering.

## Logical Table Model

ADBC exposes one Delta table path as a synthetic table, commonly `delta_table`.

Scope:

- no real multi-table catalog
- no real schema namespace
- no cross-table joins through catalog discovery
- table path supplied through connection or statement options

## Supported Read Paths

- direct table scan
- SQL query against the path-scoped table
- schema discovery
- versioned reads
- partitioned execution
- CDF direct reads
- CDF SQL queries using `_cdf`

## Explicit Non-Goals

- writes
- prepared statements
- parameter binding
- transactions
- statistics
- real catalog discovery

## Option Constraints

The driver rejects ambiguous option combinations:

- `MaxRows` is for direct table reads, not SQL queries.
- Partitioned execution is incompatible with CDF mode.
- Partitioned execution is incompatible with `MaxRows`.
- Some deletion-vector layouts are not supported by partitioned execution.

## Agent Guidance

Generate ADBC code only for read-only Arrow workflows. For mutations, use `DeltaTableServiceClient` directly.
