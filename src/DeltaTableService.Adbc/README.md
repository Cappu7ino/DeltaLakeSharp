# DeltaTableService ADBC (.NET)

`Microsoft.DI.DeltaTableService.Adbc` is a read-only ADBC driver for .NET consumers backed by the DeltaTableService `V3_Rust` path.

## Current MVP scope

- Direct reads from a Delta table path
- SQL reads over the same Delta table path
- Arrow-native result streaming
- Basic metadata discovery for the single logical Delta table exposed by the connection

## Connection options

- Required: `delta.table_uri`
- Optional: `delta.version`
- Optional: `delta.max_rows`
- Generic storage pass-through: `delta.storage.option.<key>`
- Azure convenience aliases:
  - `delta.azure.storage_account`
  - `delta.azure.sas_token`

## Synthetic metadata contract

This driver is path-scoped. A connection represents exactly one Delta table path, not a multi-catalog database.

To make ADBC metadata APIs usable for consumers that expect table discovery, the driver exposes a synthetic namespace:

- Catalog name: empty string `""`
- Schema name: empty string `""`
- Logical table name: `delta_table`
- Table type: `TABLE`

The logical table name is also the SQL alias used by the driver. SQL queries should reference:

```sql
SELECT * FROM delta_table
```

## Metadata API behavior

- `GetTableSchema(...)`
  - supports only table name `delta_table`
  - rejects non-empty catalog and schema values
  - returns the actual Arrow schema for the Delta table
- `GetTableTypes()`
  - returns a single row with `TABLE`
- `GetInfo()`
  - returns driver/vendor metadata for this ADBC implementation
- `GetObjects()`
  - returns the synthetic catalog/schema/table hierarchy above
  - derives columns from the actual Delta schema
  - supports filtering by table name pattern, column name pattern, depth, and table types

## Consumer guidance

- Treat this driver as a single-table connection, not a relational catalog explorer.
- Do not expect discovered catalog or schema names to map to real storage namespaces.
- Use `delta_table` in SQL rather than the raw path.
- If your tooling displays ADBC metadata, expect the empty catalog/schema values to be intentional for the MVP.

## Not in the current MVP

- Writes
- Prepared statements
- Parameter binding
- Transactions
- Statistics and richer namespace discovery
