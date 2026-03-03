# Delta Table Service

Test infrastructure for running Delta Lake operations inside Docker containers and exercising them from C# integration tests.

## Architecture

The service ships with **two interchangeable backends**, each exposing the same C# client API:

| Backend | Engine | Protocol | Dockerfile | Base Image |
|---------|--------|----------|------------|------------|
| **V1** | Apache Spark (PySpark) | Arrow Flight (port 8815) | `v1/Dockerfile` | `apache/spark:3.5.5` |
| **V2** | DataFusion + delta-rs | Arrow Flight (port 8815) | `v2/Dockerfile` | `python:3.11-slim` |

Both backends are accessed through the same `DeltaTableServiceClient` class.
The backend is selected at construction time via the `ServiceMode` enum and is
transparent to the caller.

## Client API

### Quick Start

```csharp
// 1. Start a Docker container (picks the backend automatically).
await using var container = await new DeltaTableContainer()
    .BuildAndStartAsync(dockerfilePath, ServiceMode.V2_DataFusion);

// 2. Create a client pointing at the running container.
using var client = new DeltaTableServiceClient(
    container.GetServiceUri(), container.Mode);

// 3. Write a Delta table from a CSV string.
var batch = ArrowConverter.FromCsv("name,age\nAlice,30\nBob,25");
await client.InsertAsync("/data/people", batch.Schema, ArrowConverter.ToAsyncEnumerable(batch));

// 4. Read it back as a DataTable.
DataTable dt = await client.ReadTableAsync("/data/people").ToDataTableAsync();

// 5. Run SQL against the table.
DataTable result = await client.ExecuteQueryAsync(
    "SELECT * FROM people WHERE age > 28",
    tablePath: "/data/people",
    tableName: "people")
    .ToDataTableAsync();

// 6. Delete rows using the DML API.
ExecuteResult deleteResult = await client.DeleteAsync(
    "DELETE FROM people WHERE age < 28",
    tablePath: "/data/people",
    tableName: "people");

// 7. Stream source data and merge it into an existing table.
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

### DeltaTableServiceClient

The public entry point. Wraps all Arrow/gRPC protocol details behind standard
.NET types (`DataTable`, `IAsyncEnumerable<RecordBatch>`, etc.).

| Method | Returns | Description |
|--------|---------|-------------|
| `HealthCheckAsync()` | `Task<bool>` | Checks if the server is healthy and responsive. |
| `ReadTableAsync(path, storageConfig?)` | `IAsyncEnumerable<RecordBatch>` | Streams raw Arrow `RecordBatch` objects for the entire table. Zero-copy columnar access with true streaming semantics. Use `.ToDataTableAsync()` to materialize as a `DataTable`, or `.ToListAsync()` to buffer all batches. |
| `GetSchemaAsync(path, storageConfig?)` | `Task<TableSchema>` | Returns the schema of a Delta table. |
| `ExecuteQueryAsync(sql, tablePath?, tableName?, storageConfig?)` | `IAsyncEnumerable<RecordBatch>` | Executes a read-oriented SQL query (SELECT, SHOW, DESCRIBE, etc.) and streams results as Arrow batches. When `tablePath`/`tableName` are provided, the table is registered first (required for stateless engines like V2). Use `.ToDataTableAsync()` to materialize as a `DataTable`. |
| `CreateTableAsync(path, schema, configuration?, storageConfig?)` | `Task<ExecuteResult>` | Creates an empty Delta table with the given schema and optional Delta configuration (DDL). |
| `InsertAsync(path, schema, batches, mode?, storageConfig?)` | `Task` | Streams `IAsyncEnumerable<RecordBatch>` to a Delta table (creates it if needed). Use `ArrowConverter.FromRows()`, `ArrowConverter.FromDataTable()`, `ArrowConverter.FromCsv()` to convert .NET data to `RecordBatch`, and `ArrowConverter.ToAsyncEnumerable()` to wrap it for streaming. |
| `DeleteAsync(sql, tablePath, tableName, storageConfig?)` | `Task<ExecuteResult>` | Executes a DELETE statement. SQL must start with "DELETE". Backend auto-registers the table. |
| `UpdateAsync(sql, tablePath, tableName, storageConfig?)` | `Task<ExecuteResult>` | Executes an UPDATE statement. SQL must start with "UPDATE". Backend auto-registers the table. |
| `MergeAsync(sql, tablePath, tableName, storageConfig?)` | `Task<ExecuteResult>` | Executes a MERGE statement. SQL must start with "MERGE". Backend auto-registers the table. |
| `MergeDataAsync(path, schema, batches, mergeOptions, storageConfig?)` | `Task<ExecuteResult>` | Streams source data via Arrow Flight DoPut and performs a MERGE INTO operation on the target Delta table. Unlike `MergeAsync` (which takes a SQL string), this method sends actual data from the client. Returns merge metrics (rows inserted, updated, deleted). |

### DeltaTableContainer

Manages the lifecycle of the Docker container running the Delta Table Service.

| Method / Property | Description |
|-------------------|-------------|
| `BuildAndStartAsync(dockerfilePath, mode?, ...)` | Builds a Docker image from the local Dockerfile and starts the container. Returns `this` for fluent chaining. |
| `PullAndStartAsync(imageName, mode?, ...)` | Pulls a pre-built image from a registry and starts the container. |
| `GetServiceUri()` | Returns the URI for the active backend (Arrow Flight). |
| `GetFlightUri()` | Returns the Arrow Flight URI. |
| `Mode` | The `ServiceMode` this container was started with. |
| `MappedPort` | The host-mapped port for the active service. |
| `DisposeAsync()` | Stops the container and cleans up the Docker image. |

### ServiceMode Enum

```csharp
public enum ServiceMode
{
    V1_Spark,      // PySpark + Arrow Flight
    V2_DataFusion,  // DataFusion + delta-rs + Arrow Flight (no JVM)
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

## Project Structure

```
DeltaLakeExperimental/
  src/
    DeltaTableService.Server/       # Python servers (Docker build context)
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
      requirements.txt               # V1 Python deps
      requirements_v2.txt            # V2 Python deps
    DeltaTableService.Client/        # C# client library
  tests/
    DeltaTableService.Tests/         # MSTest unit + integration tests
  benchmarks/
    DeltaTableService.Benchmark/     # BenchmarkDotNet performance tests
```

## Building

From the repository root:

```bash
dotnet build DeltaTableService.sln
```

## Running Tests

```bash
# Unit tests only (no Docker required)
dotnet test tests/DeltaTableService.Tests/DeltaTableService.Tests.csproj --filter "TestCategory!=Integration"

# Integration tests for a specific backend
dotnet test tests/DeltaTableService.Tests/DeltaTableService.Tests.csproj --filter "TestCategory=V1"
dotnet test tests/DeltaTableService.Tests/DeltaTableService.Tests.csproj --filter "TestCategory=V2"

# All tests
dotnet test tests/DeltaTableService.Tests/DeltaTableService.Tests.csproj
```

Integration tests require Docker Desktop to be running. Each backend's Docker image is built automatically by `DeltaTableContainer.BuildAndStartAsync`.

## Docker Images

Each Dockerfile uses `DeltaTableService.Server/` as the build context. The C# test infrastructure resolves this automatically. To build manually:

```bash
cd src/DeltaTableService.Server

# V1 (PySpark + Arrow Flight)
docker build -f v1/Dockerfile -t delta-table-service:test .

# V2 (DataFusion + delta-rs)
docker build -f v2/Dockerfile -t delta-table-service-v2:test .
```

## V1 vs V2 -- Architecture & Performance Comparison

The two backends expose an identical Arrow Flight RPC protocol but differ
significantly in how they process data internally.  This section explains
the trade-offs to help test authors choose the right backend.

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

The V2 backend uses [delta-rs](https://delta-io.github.io/delta-rs/) (version 1.4.x) which does not support all Delta Lake features. These limitations are documented here to help test authors choose the appropriate backend.

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
- Use **V2 (DataFusion)** for fast, lightweight testing of basic Delta operations only
- **Do NOT** use `delta.enableDeletionVectors=true` with V2 -- it will make tables unreadable
- The `CreateTableAsync()` API accepts an optional `configuration` parameter on all backends, but feature support varies significantly (see above)
