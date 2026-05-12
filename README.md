# DeltaLakeSharp

DeltaLakeSharp is an experimental, incubating Delta Lake SDK for .NET with streaming Arrow reads and writes, SQL operations, merge support, protocol upgrades, and interchangeable Spark, Flight, and native Rust backends.

The library is designed for application code, data tooling, automation, and integration scenarios. The V3 native Rust runtime is the recommended path for external SDK and ADBC consumers. The container-based harnesses and test projects in this repository are supporting assets, not the primary purpose of the library.

Planned public package family:

- `DeltaLakeSharp.Client` - primary C# SDK for Delta table reads, writes, SQL, DML, CDF, partitioned reads, schema operations, and protocol upgrades.
- `DeltaLakeSharp.Adbc` - read-only Arrow Database Connectivity driver backed by the V3 native Rust path.
- `DeltaLakeSharp.Testing` - test harness support for repository and downstream validation scenarios.

The planned public repository location is `https://github.com/Cappu7ino/DeltaLakeSharp`.

## Target Frameworks

The client library currently targets:

- `net8.0`
- `net472`
- `netstandard2.0`

The native `V3_Rust` backend is supported when the V3 native runtime asset is available to the host application. The `net472` and `netstandard2.0` targets are validated with focused compatibility coverage, while the Docker-backed V1/V2 test paths still depend on local Docker/Testcontainers availability.

## Examples And AI Integration Docs

Compileable SDK consumer examples are available in [examples/DeltaLakeSharp.Client.Examples](examples/DeltaLakeSharp.Client.Examples). The example project is a `net8.0` host that intentionally references the client library's `netstandard2.0` asset, which helps keep generated and copied integration code compatible with the broadest public client surface.

AI-friendly integration artifacts are indexed in [docs/ai/README.md](docs/ai/README.md). They include capability matrices, common patterns, anti-patterns, architecture notes, how-to guides, ADRs, and semantic public API metadata for downstream retrieval and agentic coding workflows.

Executable V3 SDK scenario tests live in [tests/DeltaLakeSharp.Tests/IntegrationScenarios](tests/DeltaLakeSharp.Tests/IntegrationScenarios). They cover the recommended native client path across streaming Arrow reads, `DbDataReader`, SQL queries, partition planning, and Change Data Feed.

## Architecture

The repository ships with **three interchangeable backends**, each exposing the same C# client API:

| Backend | Engine | Protocol | Dockerfile | Base Image |
|---------|--------|----------|------------|------------|
| **V1** | Apache Spark (PySpark) | Arrow Flight (port 8815) | `v1/Dockerfile` | `apache/spark:3.5.5` |
| **V2** | DataFusion + delta-rs | Arrow Flight (port 8815) | `v2/Dockerfile` | `python:3.11-slim` |
| **V3** | Native Rust + delta-rs | In-process native interop | N/A | N/A |

All backends are accessed through the same `DeltaTableServiceClient` class in the `DeltaLakeSharp.Client` namespace.
The backend is selected at construction time via the `ServiceMode` enum and is
transparent to the caller.

## Client API

### Quick Start

```csharp
using DeltaLakeSharp.Client;
using DeltaLakeSharp.Client.Models;

// 1. Create a client. V3 native mode runs in-process and does not require a server URI.
using var client = new DeltaTableServiceClient(ServiceMode.V3_Rust);

// 2. Write a Delta table from a CSV string.
var batch = ArrowConverter.FromCsv("name,age\nAlice,30\nBob,25");
await client.InsertAsync("/data/people", batch.Schema, ArrowConverter.ToAsyncEnumerable(batch));

// 3. Read it back as a DataTable.
DataTable dt = await client.ReadTableAsync("/data/people").ToDataTableAsync();

// 4. Run SQL against the table.
DataTable result = await client.ExecuteQueryAsync(
    "SELECT * FROM people WHERE age > 28",
    tablePath: "/data/people",
    tableName: "people")
    .ToDataTableAsync();

// 5. Delete rows using the DML API.
ExecuteResult deleteResult = await client.DeleteAsync(
    "DELETE FROM people WHERE age < 28",
    tablePath: "/data/people",
    tableName: "people");

// 6. Stream source data and merge it into an existing table.
var arrowSchema = new Apache.Arrow.Schema.Builder()
    .Field(f => f.Name("name").DataType(StringType.Default).Nullable(false))
    .Field(f => f.Name("age").DataType(Int32Type.Default).Nullable(true))
    .Build();

var mergeOptions = new MergeOptions(
    predicate: "target.name = source.name",
    sourceAlias: "source",
    targetAlias: "target")
{
    WhenMatchedUpdateAll = true,
    WhenNotMatchedInsertAll = true,
};

ExecuteResult mergeResult = await client.MergeDataAsync(
    "/data/people",
    arrowSchema,
    GetSourceBatches(),   // IAsyncEnumerable<RecordBatch>
    mergeOptions);
```

For V1 and V2 containerized backends, `DeltaLakeSharp.Testing.DeltaTableContainer` remains available as a convenience for local development, compatibility testing, and integration scenarios.

### DeltaTableServiceClient

The primary public SDK entry point. Wraps Arrow, Flight, and native runtime details behind standard
.NET types (`DataTable`, `IAsyncEnumerable<RecordBatch>`, etc.).

| Method | Returns | Description |
|--------|---------|-------------|
| `HealthCheckAsync()` | `Task<bool>` | Checks if the server is healthy and responsive. |
| `ReadTableAsync(path, storageConfig?)` | `IAsyncEnumerable<RecordBatch>` | Streams raw Arrow `RecordBatch` objects for the entire table. Zero-copy columnar access with true streaming semantics. Use `.ToDataTableAsync()` to materialize as a `DataTable`, or `.ToListAsync()` to buffer all batches. |
| `ReadChangeDataAsync(path, startingVersion, endingVersion?, storageConfig?)` | `IAsyncEnumerable<RecordBatch>` | Streams Delta Change Data Feed rows as Arrow batches. Currently supported only by the native V3 backend. |
| `ExecuteChangeDataQueryAsync(sql, path, startingVersion, endingVersion?, storageConfig?)` | `IAsyncEnumerable<RecordBatch>` | Executes a SQL query against Change Data Feed rows exposed as the fixed `_cdf` relation. Currently supported only by the native V3 backend. |
| `GetSchemaAsync(path, storageConfig?)` | `Task<TableSchema>` | Returns the schema of a Delta table. |
| `ExecuteQueryAsync(sql, tablePath?, tableName?, storageConfig?)` | `IAsyncEnumerable<RecordBatch>` | Executes a read-oriented SQL query (SELECT, SHOW, DESCRIBE, etc.) and streams results as Arrow batches. When `tablePath`/`tableName` are provided, the table is registered first (required for stateless engines like V2). Use `.ToDataTableAsync()` to materialize as a `DataTable`. |
| `CreateTableAsync(path, schema, configuration?, storageConfig?)` | `Task<ExecuteResult>` | Creates an empty Delta table with the given schema and optional Delta configuration (DDL). |
| `InsertAsync(path, schema, batches, mode?, schemaMode?, storageConfig?, partitionBy?)` | `Task` | Streams `IAsyncEnumerable<RecordBatch>` to a Delta table. V3 creates the table implicitly on first write; `partitionBy` applies on create and is validated on later writes. `schemaMode` currently supports schema overwrite on the native V3 backend. |
| `DeleteAsync(sql, tablePath, tableName, storageConfig?)` | `Task<ExecuteResult>` | Executes a DELETE statement. SQL must start with "DELETE". Backend auto-registers the table. |
| `UpdateAsync(sql, tablePath, tableName, storageConfig?)` | `Task<ExecuteResult>` | Executes an UPDATE statement. SQL must start with "UPDATE". Backend auto-registers the table. |
| `MergeAsync(sql, tablePath, tableName, storageConfig?)` | `Task<ExecuteResult>` | Executes a MERGE statement. SQL must start with "MERGE". Backend auto-registers the table. |
| `MergeDataAsync(path, schema, batches, mergeOptions, storageConfig?)` | `Task<ExecuteResult>` | Streams source data via Arrow Flight DoPut and performs a MERGE INTO operation on the target Delta table. Unlike `MergeAsync` (which takes a SQL string), this method sends actual data from the client. Returns merge metrics (rows inserted, updated, deleted). |
| `UpgradeTableProtocolAsync(tablePath, readerVersion, writerVersion, readerFeatures?, writerFeatures?, storageConfig?)` | `Task<ExecuteResult>` | Upgrades a Delta table protocol version and optionally enables reader/writer features such as Change Data Feed support. |

### DeltaTableContainer

Optional helper that manages the lifecycle of Dockerized V1/V2 service backends for local development, compatibility validation, and integration testing.

| Method / Property | Description |
|-------------------|-------------|
| `BuildAndStartAsync(dockerfilePath, mode?, ...)` | Builds a Docker image from the local Dockerfile and starts the container. Returns `this` for fluent chaining. |
| `PullAndStartAsync(imageName, mode?, ...)` | Pulls a pre-built image from a registry and starts the container. |
| `GetServiceUri()` | Returns the URI for the active backend (Arrow Flight). |
| `GetFlightUri()` | Returns the Arrow Flight URI for containerized backends. |
| `Mode` | The `ServiceMode` this container was started with. |
| `MappedPort` | The host-mapped port for the active service. |
| `DisposeAsync()` | Stops the container and cleans up the Docker image. |

### ServiceMode Enum

```csharp
public enum ServiceMode
{
    V1_Spark,      // PySpark + Arrow Flight
    V2_DataFusion,  // DataFusion + delta-rs + Arrow Flight (no JVM)
    V3_Rust,        // Native Rust + delta-rs (in-process)
}
```

### SaveMode Enum

Used by `InsertAsync` to control how data is written to the Delta table.

```csharp
public enum SaveMode
{
    Overwrite,  // Replace existing table data (default)
    Append,     // Add new data without removing existing rows
}
```

### WriteSchemaMode Enum

Used by `InsertAsync` to control how schema differences are handled during write operations.

```csharp
public enum WriteSchemaMode
{
  Merge,      // Merge incoming columns into the existing table schema
  Overwrite,  // Replace the existing table schema during overwrite writes
}
```

Currently these schema modes are supported only by the native V3 backend.

### Model Types

**`StorageConfig`** -- Per-request ABFSS/OneLake credentials.

```csharp
new StorageConfig(storageAccount: "onelake", sasToken: "sv=2022-...");
```

**`TableSchema` / `ColumnDefinition`** -- Schema definition for creating tables.

```csharp
var schema = new TableSchema(new[]
{
    new ColumnDefinition("id", "int64", nullable: false),
    new ColumnDefinition("name", "string"),
    new ColumnDefinition("active", "boolean"),
});
```

Supported data types: `string`, `int32`, `int64`, `float`, `double`, `boolean`, `timestamp`.

**`ExecuteResult`** -- Result of DDL/DML operations.

| Property | Type | Description |
|----------|------|-------------|
| `Success` | `bool` | Whether the operation succeeded. |
| `Message` | `string` | Status or error message from the server. |
| `Result` | `IReadOnlyList<Dictionary<string, object>>` | Optional result rows. |

**`MergeOptions`** -- Configuration for `MergeDataAsync()`. Semantically equivalent to a SQL MERGE INTO statement.

```csharp
var options = new MergeOptions(
    predicate: "target.id = source.id",
    sourceAlias: "source",   // default
    targetAlias: "target")   // default
{
    // WHEN MATCHED -- choose one:
    WhenMatchedUpdateAll = true,                           // UPDATE SET *
    // WhenMatchedUpdateSet = new() { ["col"] = "source.col" },  // UPDATE SET col = expr
    // WhenMatchedDeletePredicate = "source.deleted = true",     // DELETE (with condition)

    // WHEN NOT MATCHED (source rows with no target match) -- choose one:
    WhenNotMatchedInsertAll = true,                        // INSERT *
    // WhenNotMatchedInsertSet = new() { ["col"] = "source.col" },  // INSERT (col) VALUES (expr)

    // WHEN NOT MATCHED BY SOURCE (orphaned target rows):
    // WhenNotMatchedBySourceDeletePredicate = "true",     // DELETE all orphans
    // WhenNotMatchedBySourceUpdateSet = new() { ["active"] = "'false'" },
    // WhenNotMatchedBySourceUpdatePredicate = "target.active = true",
};
```

| Property | Type | Description |
|----------|------|-------------|
| `Predicate` | `string` | **Required.** Join condition, e.g. `"target.id = source.id"`. |
| `SourceAlias` | `string` | Alias for the source data stream (default `"source"`). |
| `TargetAlias` | `string` | Alias for the target Delta table (default `"target"`). |
| `WhenMatchedUpdateAll` | `bool` | Update all columns on matched rows (`UPDATE SET *`). |
| `WhenMatchedUpdateSet` | `Dictionary<string, string>` | Explicit column assignments for matched rows. Ignored when `WhenMatchedUpdateAll` is true. |
| `WhenMatchedDeletePredicate` | `string` | Delete matched rows that satisfy this condition. Use `"true"` for unconditional delete. |
| `WhenNotMatchedInsertAll` | `bool` | Insert all columns for unmatched source rows (`INSERT *`). |
| `WhenNotMatchedInsertSet` | `Dictionary<string, string>` | Explicit column assignments for unmatched source inserts. Ignored when `WhenNotMatchedInsertAll` is true. |
| `WhenNotMatchedBySourceDeletePredicate` | `string` | Delete orphaned target rows (no source match). Use `"true"` to delete all. |
| `WhenNotMatchedBySourceUpdateSet` | `Dictionary<string, string>` | Update orphaned target rows with these assignments. |
| `WhenNotMatchedBySourceUpdatePredicate` | `string` | Optional guard condition for the not-matched-by-source update clause. |

### Extension Methods

**`ReadStreamExtensions.ToDataTableAsync()`** -- Materializes an `IAsyncEnumerable<RecordBatch>` into a `DataTable`.

```csharp
DataTable dt = await client.ReadTableAsync("/data/t").ToDataTableAsync();
```

**`ReadStreamExtensions.ToListAsync()`** -- Buffers an `IAsyncEnumerable<RecordBatch>` into a `List<RecordBatch>`.

```csharp
List<RecordBatch> batches = await client.ReadTableAsync("/data/t").ToListAsync();
```

## Benchmark Dataset Generator

The benchmark project includes a CLI generator for creating deterministic Delta datasets for full-table read and full-table Change Data Feed benchmarks.

The generator uses the native V3 client to create and populate local Delta tables with these schema variants:

- default schema
  - `id`
  - `tenant_id`
  - `event_ts`
  - `region`
  - `category`
  - `amount`
  - `quantity`
  - `is_active`
  - `note`
- decimal schema
  - `id`
  - `tenant_id`
  - `event_ts`
  - `region`
  - `category`
  - `amount`
  - `unit_price`
  - `quantity`
  - `is_active`
  - `note`

### CLI Usage

```bash
dotnet run --project benchmarks/DeltaLakeSharp.Benchmark --framework net8.0 -- generate-dataset --kind <full-read|full-cdf> --output <path> [options]
```

Options:

- `--schema <default|decimal>` schema variant, default `default`
- `--rows <n>` initial row count, default `1000000`
- `--batch-size <n>` rows per insert batch/file target, default `100000`
- `--versions <n>` additional CDF versions to generate, default `20`
- `--rows-per-version <n>` rows appended in each extra CDF version, default `50000`
- `--seed <n>` deterministic random seed, default `42`
- `--overwrite` delete the output path first if it already exists

### Scripted Dataset Generation

Generate the default snapshot dataset matrix used by the benchmark project:

```powershell
pwsh -NoLogo -NoProfile -File benchmarks/DeltaLakeSharp.Benchmark/Generate-BenchmarkDatasets.ps1 -Overwrite
```

This creates:

- `benchmarks/DeltaLakeSharp.Benchmark/TestData/delta-full-read/1m`
- `benchmarks/DeltaLakeSharp.Benchmark/TestData/delta-full-read/2m`
- `benchmarks/DeltaLakeSharp.Benchmark/TestData/delta-full-read/5m`
- `benchmarks/DeltaLakeSharp.Benchmark/TestData/delta-full-read/10m`

Generate the decimal snapshot matrix plus decimal CDF dataset:

```powershell
pwsh -NoLogo -NoProfile -File benchmarks/DeltaLakeSharp.Benchmark/Generate-DecimalBenchmarkDatasets.ps1 -Overwrite
```

This creates:

- `benchmarks/DeltaLakeSharp.Benchmark/TestData/delta-full-read-decimal/1m`
- `benchmarks/DeltaLakeSharp.Benchmark/TestData/delta-full-read-decimal/2m`
- `benchmarks/DeltaLakeSharp.Benchmark/TestData/delta-full-read-decimal/5m`
- `benchmarks/DeltaLakeSharp.Benchmark/TestData/delta-full-read-decimal/10m`
- `benchmarks/DeltaLakeSharp.Benchmark/TestData/delta-full-cdf-decimal`

To generate a default CDF dataset directly with the CLI:

```bash
dotnet run --project benchmarks/DeltaLakeSharp.Benchmark --framework net8.0 -- generate-dataset --kind full-cdf --schema default --output benchmarks/DeltaLakeSharp.Benchmark/TestData/delta-full-cdf --rows 1000000 --batch-size 250000 --versions 20 --rows-per-version 50000 --overwrite
```

### Examples

Generate a full-read dataset:

```bash
dotnet run --project benchmarks/DeltaLakeSharp.Benchmark --framework net8.0 -- generate-dataset --kind full-read --schema default --output C:\data\delta-full-read --rows 10000000 --batch-size 250000 --overwrite
```

Generate a full-CDF dataset with version history:

```bash
dotnet run --project benchmarks/DeltaLakeSharp.Benchmark --framework net8.0 -- generate-dataset --kind full-cdf --schema default --output C:\data\delta-full-cdf --rows 5000000 --batch-size 100000 --versions 24 --rows-per-version 25000 --overwrite
```

Generate a decimal full-read dataset:

```bash
dotnet run --project benchmarks/DeltaLakeSharp.Benchmark --framework net8.0 -- generate-dataset --kind full-read --schema decimal --output C:\data\delta-full-read-decimal --rows 2000000 --batch-size 500000 --overwrite
```

Notes:

- `full-read` creates a large latest-snapshot dataset for whole-table scan benchmarks
- `full-cdf` enables Change Data Feed and adds append/update/delete history across versions
- `decimal` adds a `unit_price decimal(18,2)` column to the benchmark table schema
- the generator requires the native V3 backend to be available on the local machine

Benchmark datasets and BenchmarkDotNet result reports are generated locally and are not tracked in public HEAD. Regenerate them from the scripts above when collecting fresh performance data.

## Project Structure

```
DeltaLakeSharp/
  src/
    DeltaLakeSharp.Server/       # Python servers (Docker build context)
      app/                           # Shared Python package root
        __init__.py
        config.py                    # Shared config (ports, hosts)
        v1/                          # PySpark backend modules
          delta_operations.py
          flight_server.py
          spark_manager.py
        v2/                          # DataFusion backend modules
          datafusion_operations.py
          flight_server.py
      v1/                            # V1 entrypoint + Dockerfile
        run.py
        Dockerfile
      v2/                            # V2 entrypoint + Dockerfile
        run.py
        Dockerfile
      v3/                            # Native Rust backend
        Cargo.toml
        src/
      requirements.txt               # V1 Python deps
      requirements_v2.txt            # V2 Python deps
    DeltaLakeSharp.Client/        # Primary .NET Delta Lake client library
  tests/
    DeltaLakeSharp.Tests/         # MSTest unit + integration coverage
  benchmarks/
    DeltaLakeSharp.Benchmark/     # BenchmarkDotNet performance coverage
```

## Building

From the repository root:

```bash
dotnet build DeltaLakeSharp.sln /p:SkipRustBuild=true -m:1
```

## Running Tests

```bash
# Unit tests only (no Docker required)
dotnet test tests/DeltaLakeSharp.Tests/DeltaLakeSharp.Tests.csproj --filter "TestCategory!=Integration"

# Integration tests for a specific backend
dotnet test tests/DeltaLakeSharp.Tests/DeltaLakeSharp.Tests.csproj --filter "TestCategory=V1"
dotnet test tests/DeltaLakeSharp.Tests/DeltaLakeSharp.Tests.csproj --filter "TestCategory=V2"

# Focused V3/native test suites
dotnet test tests/DeltaLakeSharp.Tests/DeltaLakeSharp.Tests.csproj --filter "FullyQualifiedName~DeltaLakeSharpV3IntegrationTests|FullyQualifiedName~NativeRustBackendTests"

# All tests
dotnet test tests/DeltaLakeSharp.Tests/DeltaLakeSharp.Tests.csproj
```

Container-based integration tests require Docker Desktop to be running. The focused native V3 test suites run in-process without Docker. Each containerized backend image is built automatically by `DeltaLakeSharp.Testing.DeltaTableContainer.BuildAndStartAsync` when those compatibility/integration paths are used.

Fabric and OneLake integration tests are opt-in and read their environment-specific values from environment variables rather than checked-in constants. Set the relevant `DELTALAKESHARP_ONELAKE_*` and `DELTALAKESHARP_SQL_ENDPOINT_*` variables before running `TestCategory=OneLake`, `TestCategory=Fabric`, or `TestCategory=SqlEndpoint` suites.

## Docker Images

Each Dockerfile uses `DeltaLakeSharp.Server/` as the build context. These images are optional and primarily used for the containerized compatibility/integration backends. To build manually:

```bash
cd src/DeltaLakeSharp.Server

# V1 (PySpark + Arrow Flight)
docker build -f v1/Dockerfile -t delta-table-service:test .

# V2 (DataFusion + delta-rs)
docker build -f v2/Dockerfile -t delta-table-service-v2:test .
```

## V1 vs V2 -- Architecture & Performance Comparison

The comparison below covers the two containerized Flight backends. The native V3 backend uses in-process Rust interop instead of Docker + Arrow Flight and is the most direct option when you want a native .NET + Rust client experience.

The two backends expose an identical Arrow Flight RPC protocol but differ
significantly in how they process data internally. This section explains
the trade-offs to help library consumers choose the right backend.

### Data Flow Overview

| Operation | V1 (PySpark) | V2 (DataFusion) |
|-----------|-------------|-----------------|
| **DoGet (reads)** | Full materialisation via `toPandas()` -> `pa.Table` | True streaming via `execute_stream()` -> `RecordBatchReader` |
| **DoPut (writes)** | Full materialisation via `reader.read_all()` -> `pa.Table` | Per-batch IPC alignment -> streaming `RecordBatchReader` |
| **DoPut (merge)** | Full materialisation -> Spark DataFrame -> `MERGE INTO` SQL | Per-batch IPC alignment -> streaming `RecordBatchReader` -> `DeltaTable.merge()` |
| **Schema handling** | PySpark schema -> explicit Arrow type mapping | delta-rs native types (`large_utf8`/`large_binary`) pass through unchanged; C# client handles them natively |

### DoGet -- Read Path

**V1 (PySpark):**
The Spark DataFrame is collected to the driver via `toPandas()`, which
crosses the JVM -> Python boundary and produces a pandas DataFrame.
`pa.Table.from_pandas()` converts to Arrow, then an explicit `.cast()`
fixes nullable-integer columns that pandas widens to `float64`.  The
resulting `pa.Table` (entire dataset in memory) is passed to
`flight.RecordBatchStream(table)`.

- Peak memory ~ **2x dataset size** (JVM DataFrame + Python `pa.Table`
  coexist during the `toPandas()` conversion).
- The Arrow Flight C++ layer serialises the table to IPC -- but the data
  is already fully materialised before streaming begins.

**V2 (DataFusion):**
DataFusion's `execute_stream()` returns a lazy iterator of RecordBatches
from the Rust engine.  A peek-wrap helper (`_stream_to_reader`) reads
the first batch to derive the schema, then wraps everything in a
`pa.RecordBatchReader`.  Delta-rs native types (`large_utf8`,
`large_binary`) pass through unchanged -- the C# client handles them
natively via `ArrowConverter`.  This reader is passed to
`flight.RecordBatchStream(reader)`, which serialises to IPC in C++
**without acquiring the Python GIL per batch**.

- Peak memory ~ **1 batch** -- no `pa.Table` is ever constructed.
- DataFusion pushes predicates and limits down to Parquet, so only the
  required data leaves Rust.

### DoPut -- Write Path

**V1 (PySpark):**
`reader.read_all()` collects every incoming Flight batch into a single
`pa.Table`.  This is converted to pandas, then to a Spark DataFrame, and
finally written via `.write.format("delta").save()`.

- Peak memory ~ **2x dataset size** (Arrow table + Spark DataFrame).
- The full dataset must fit in driver memory.

**V2 (DataFusion / delta-rs):**
Incoming batches are consumed one at a time.  Each batch undergoes an
IPC round-trip to force buffer alignment (required because Arrow Flight
may deliver unaligned buffers that cause Rust panics in delta-rs).  The
aligned batches are wrapped in a `pa.RecordBatchReader` and streamed
directly into `write_deltalake()`, which consumes them incrementally.

- Peak memory ~ **1 batch** -- the full dataset is never materialised.
- The IPC alignment step adds a small per-batch overhead but avoids
  Rust FFI alignment panics.

### DoPut -- Merge Path (`MergeDataAsync`)

Both backends receive source data via the same Arrow Flight DoPut
channel.  The Flight descriptor carries a JSON command with
`"operation": "merge"` plus the merge predicate, aliases, and clause
configuration (update/insert/delete rules).

**V1 (PySpark):**
`reader.read_all()` materialises all incoming Flight batches into a
`pa.Table`.  The table is converted to a Spark DataFrame via
`spark.createDataFrame()`.  The target Delta table is opened with
`DeltaTable.forPath(spark, path).alias(target).merge(source_df.alias(source), predicate)`.
Clause methods (`.whenMatchedUpdateAll()`, `.whenNotMatchedInsertAll()`,
etc.) are chained based on the command JSON, then `.execute()` runs the
merge.

- Peak memory ~ **2x dataset size** (Arrow table + Spark DataFrame).
- Full Spark SQL MERGE semantics -- supports all clause combinations.

**V2 (DataFusion / delta-rs):**
Incoming batches are IPC-aligned (same workaround as the write path)
and wrapped in a `pa.RecordBatchReader` -- **no materialisation**.  The
reader is passed directly to `DeltaTable.merge(source=reader,
predicate=..., streamed_exec=True)`, which builds a Rust-side
`LazyMemoryExec` plan.  Clause methods (`.when_matched_update_all()`,
`.when_not_matched_insert_all()`, etc.) are chained based on the
command JSON, then `.execute()` runs the merge and returns metrics.

- Peak memory ~ **1 batch** -- delta-rs consumes the stream lazily.
- Returns detailed metrics: rows inserted, updated, deleted, copied,
  files added/removed, execution time.

### Resource Footprint

| Metric | V1 (PySpark) | V2 (DataFusion) |
|--------|-------------|-----------------|
| Docker image size | ~2 GB (`apache/spark:3.5.5`) | ~200 MB (`python:3.11-slim`) |
| JVM baseline memory | ~500 MB | None |
| Startup time | ~15-30 s (Spark session init) | ~1-2 s |
| Peak memory (read N rows) | ~2x dataset | ~1 batch |
| Peak memory (write N rows) | ~2x dataset | ~1 batch |
| Peak memory (merge N source rows) | ~2x source dataset | ~1 batch |
| GIL contention during Flight streaming | N/A (data already materialised) | None (`RecordBatchStream` + `RecordBatchReader` stay in C++) |

### When to Use Each Backend

**Choose V1 (PySpark) when:**
- Tests require full Delta Lake feature support -- column mapping,
  deletion vectors, or other advanced Delta operations not yet in delta-rs.
- Tests depend on Spark SQL semantics or functions not available in
  DataFusion.
- Dataset size is small enough that full materialisation is acceptable.

**Choose V2 (DataFusion) when:**
- Tests need fast startup and low memory overhead (CI pipelines, local
  dev loops).
- Tests exercise read, write, delete, merge, and SQL query operations
  against standard Delta tables.
- Streaming behaviour matters -- V2 never materialises the full dataset
  (including during merge), making it suitable for large-table tests
  without out-of-memory risk.
- Docker image size or pull time is a constraint (~200 MB vs ~2 GB).

## Known Limitations

### V2 Backend (DataFusion + delta-rs)

The V2 backend uses [delta-rs](https://delta-io.github.io/delta-rs/) (version 1.4.x) which does not support all Delta Lake features. These limitations are documented here to help consumers choose the appropriate backend.

#### Column Mapping

| Operation | Supported? | Notes |
|-----------|------------|-------|
| **Write** with `delta.columnMapping.mode` | Partial | Configuration is stored in metadata but **not implemented**. Protocol stays at (1, 2), no column mapping annotations added. Created tables are standard Delta tables. |
| **Read** Spark column-mapped tables | No | Fails with "minimum reader version is 2 but deltalake only supports version 1 or 3" or "reader features: {'columnMapping'} not yet supported". |

#### Deletion Vectors

| Operation | Supported? | Notes |
|-----------|------------|-------|
| **Write** with `delta.enableDeletionVectors` | **No (Dangerous!)** | Configuration is stored, protocol upgraded to (3, 7) with `deletionVectors` feature, but **deletion vectors are NOT implemented**. DELETE operations still use copy-on-write (rewrite parquet files). |
| **Read** tables with DV config after DELETE | **No** | Fails with "reader features: {'deletionVectors'} not yet supported by the deltalake reader". |

> **WARNING:** Do not use `delta.enableDeletionVectors=true` with V2 backend. The configuration upgrades the protocol but does not implement DVs. After any DELETE operation, the table becomes **unreadable** by delta-rs because the protocol declares a feature the reader doesn't support. This is worse than column mapping (which at least leaves the table readable).

#### Recommendations

- Use **V1 (PySpark)** for tests requiring full Delta Lake feature support (column mapping, deletion vectors, etc.)
- Use **V2 (DataFusion)** for fast, lightweight execution of basic Delta operations when the containerized Flight backend is preferred
- **Do NOT** use `delta.enableDeletionVectors=true` with V2 -- it will make tables unreadable
- The `CreateTableAsync()` API accepts an optional `configuration` parameter on all backends, but feature support varies significantly (see above)
