// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Read-path handlers: GetFlightInfo, DoGet, and GetSchema.
//!
//! Follows the V2 protocol exactly:
//! - Ticket = verbatim JSON command bytes (echoed from FlightDescriptor.command)
//! - Read-table: GetFlightInfo returns real schema; DoGet streams all (or limited) rows
//! - SQL: GetFlightInfo returns empty schema; DoGet registers table + executes SQL
//! - GetSchema: returns Arrow schema from Delta table metadata (no data scan)

use std::pin::Pin;
use std::sync::Arc;

use arrow::error::ArrowError;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::ipc::writer::IpcWriteOptions;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::{
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, SchemaAsIpc, SchemaResult, Ticket,
};
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{Stream, StreamExt, TryStreamExt};
use tonic::Status;
use tracing::{debug, info};

use super::commands::{Command, ReadCommand, SqlCommand};
use super::helpers::{open_delta_table, register_delta_table};
use crate::error::ServiceError;

/// Resolves the Arrow schema for a read/query command without committing to any
/// particular transport representation.
///
/// This is the transport-neutral schema path used by both the legacy Flight
/// adapter and the new native in-process ABI.
pub async fn resolve_schema_from_command_bytes(
    cmd_bytes: &[u8],
) -> Result<SchemaRef, ServiceError> {
    let command = Command::parse(cmd_bytes)?;

    match command {
        Command::Read(read_cmd) => {
            info!(path = %read_cmd.path, "Resolving schema: read-table mode");
            get_delta_schema(&read_cmd).await
        }
        Command::Sql(_) => {
            // Match V2/V3 Flight behavior for SQL commands: schema is not known
            // until execution, so the transport-neutral schema path returns an
            // empty schema rather than forcing SQL execution.
            Ok(Arc::new(Schema::empty()))
        }
    }
}

/// Resolves a command payload into a transport-neutral Arrow batch reader.
///
/// The returned reader yields batches lazily and can therefore be exported over
/// the Arrow C Stream interface without materializing the full result set.
pub async fn resolve_batch_reader_from_command_bytes(
    cmd_bytes: &[u8],
    runtime_handle: tokio::runtime::Handle,
) -> Result<Box<dyn RecordBatchReader<Item = Result<RecordBatch, ArrowError>> + Send>, ServiceError>
{
    let command = Command::parse(cmd_bytes)?;

    match command {
        Command::Read(read_cmd) => {
            info!(path = %read_cmd.path, "Resolving batch reader: read-table mode");
            read_table_batch_reader(read_cmd, runtime_handle).await
        }
        Command::Sql(sql_cmd) => {
            info!(sql = %sql_cmd.sql, "Resolving batch reader: SQL mode");
            sql_batch_reader(sql_cmd, runtime_handle).await
        }
    }
}

/// Boxed Flight data stream — the concrete return type for DoGet.
type BoxedFlightStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

// -------------------------------------------------------------------------- //
//  GetFlightInfo
// -------------------------------------------------------------------------- //

/// Handles `GetFlightInfo` — returns flight metadata with schema and ticket.
///
/// - **Read-table mode**: Opens the Delta table, reads its Arrow schema from
///   metadata (no data scan), and returns it in FlightInfo.
/// - **SQL mode**: Returns an empty schema (the real schema is discovered
///   lazily during DoGet, matching V2 behavior).
///
/// In both modes, the ticket is the verbatim JSON command bytes.
pub async fn handle_get_flight_info(
    descriptor: FlightDescriptor,
) -> Result<FlightInfo, ServiceError> {
    let cmd_bytes = &descriptor.cmd;
    let schema = resolve_schema_from_command_bytes(cmd_bytes).await?;

    // Ticket = verbatim command JSON (matching V2 protocol).
    let ticket = Ticket::new(cmd_bytes.clone());
    let endpoint = FlightEndpoint::new().with_ticket(ticket);

    let info = FlightInfo::new()
        .try_with_schema(schema.as_ref())
        .map_err(ServiceError::Arrow)?
        .with_descriptor(descriptor)
        .with_endpoint(endpoint)
        .with_total_records(-1)
        .with_total_bytes(-1);

    Ok(info)
}

// -------------------------------------------------------------------------- //
//  GetSchema
// -------------------------------------------------------------------------- //

/// Handles `GetSchema` — returns the Arrow schema from Delta table metadata.
///
/// Only read-table commands are valid here (SQL mode does not have a
/// meaningful schema until execution).
pub async fn handle_get_schema(descriptor: FlightDescriptor) -> Result<SchemaResult, ServiceError> {
    let cmd_bytes = &descriptor.cmd;
    let schema = resolve_schema_from_command_bytes(cmd_bytes).await?;

    // Convert Schema -> SchemaResult via SchemaAsIpc.
    let options = IpcWriteOptions::default();
    let result: SchemaResult = SchemaAsIpc::new(schema.as_ref(), &options)
        .try_into()
        .map_err(ServiceError::Arrow)?;
    Ok(result)
}

// -------------------------------------------------------------------------- //
//  DoGet
// -------------------------------------------------------------------------- //

/// Handles `DoGet` — streams RecordBatches back to the client.
///
/// - **Read-table mode**: Opens the Delta table, registers it in DataFusion,
///   executes `SELECT * FROM _tbl [LIMIT n]`, and streams batches.
/// - **SQL mode**: Optionally registers a Delta table under a logical name,
///   then executes the user-supplied SQL.
///
/// Returns a boxed stream so both branches produce the same concrete type.
pub async fn handle_do_get(ticket: Ticket) -> Result<BoxedFlightStream, ServiceError> {
    let cmd_bytes = &ticket.ticket;
    let command = Command::parse(cmd_bytes)?;

    match command {
        Command::Read(read_cmd) => {
            info!(path = %read_cmd.path, num_rows = ?read_cmd.num_rows, "DoGet: read-table mode");
            do_get_read_table(read_cmd).await
        }
        Command::Sql(sql_cmd) => {
            info!(sql = %sql_cmd.sql, "DoGet: SQL mode");
            do_get_sql(sql_cmd).await
        }
    }
}

// -------------------------------------------------------------------------- //
//  Internal helpers
// -------------------------------------------------------------------------- //

/// Opens a Delta table and returns its Arrow schema (metadata only, no data scan).
async fn get_delta_schema(cmd: &ReadCommand) -> Result<SchemaRef, ServiceError> {
    let table = open_delta_table(
        &cmd.path,
        cmd.storage_account.as_deref(),
        cmd.sas_token.as_deref(),
        cmd.version,
    )
    .await?;

    // DeltaTable → snapshot() → EagerSnapshot → arrow_schema()
    let schema = table
        .snapshot()
        .map_err(ServiceError::Delta)?
        .snapshot()
        .arrow_schema();
    Ok(schema)
}

/// Executes a read-table DoGet: registers the table as `_tbl`, runs
/// `SELECT * FROM _tbl [LIMIT n]`, and returns a boxed stream of FlightData.
async fn do_get_read_table(cmd: ReadCommand) -> Result<BoxedFlightStream, ServiceError> {
    let (schema, batch_stream) = execute_read_table_stream(cmd).await?;
    Ok(build_flight_stream(schema, batch_stream))
}

/// Executes a SQL DoGet: optionally registers a table, runs the user SQL,
/// and returns a boxed stream of FlightData.
async fn do_get_sql(cmd: SqlCommand) -> Result<BoxedFlightStream, ServiceError> {
    let (schema, batch_stream) = execute_sql_stream(cmd).await?;
    Ok(build_flight_stream(schema, batch_stream))
}

/// Adapts a DataFusion async batch stream into a blocking `RecordBatchReader`.
///
/// The Arrow C Stream interface is pull-based and synchronous at the ABI
/// boundary. DataFusion, on the other hand, yields batches asynchronously. This
/// adapter bridges the two by blocking on exactly one batch at a time using the
/// provided Tokio runtime handle, preserving the streaming-first memory profile.
struct AsyncRecordBatchReader {
    schema: SchemaRef,
    runtime_handle: tokio::runtime::Handle,
    stream: SendableRecordBatchStream,
}

impl Iterator for AsyncRecordBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.runtime_handle.block_on(self.stream.next()).map(|result| {
            result.map_err(|error| ArrowError::ExternalError(Box::new(error)))
        })
    }
}

impl RecordBatchReader for AsyncRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

async fn read_table_batch_reader(
    cmd: ReadCommand,
    runtime_handle: tokio::runtime::Handle,
) -> Result<Box<dyn RecordBatchReader<Item = Result<RecordBatch, ArrowError>> + Send>, ServiceError>
{
    let (schema, stream) = execute_read_table_stream(cmd).await?;
    Ok(Box::new(AsyncRecordBatchReader {
        schema,
        runtime_handle,
        stream,
    }))
}

async fn sql_batch_reader(
    cmd: SqlCommand,
    runtime_handle: tokio::runtime::Handle,
) -> Result<Box<dyn RecordBatchReader<Item = Result<RecordBatch, ArrowError>> + Send>, ServiceError>
{
    let (schema, stream) = execute_sql_stream(cmd).await?;
    Ok(Box::new(AsyncRecordBatchReader {
        schema,
        runtime_handle,
        stream,
    }))
}

async fn execute_read_table_stream(
    cmd: ReadCommand,
) -> Result<(SchemaRef, SendableRecordBatchStream), ServiceError> {
    let ctx = SessionContext::new();
    register_delta_table(
        &ctx,
        "_tbl",
        &cmd.path,
        cmd.storage_account.as_deref(),
        cmd.sas_token.as_deref(),
        cmd.version,
    )
    .await?;

    let sql = match cmd.num_rows {
        Some(n) => format!("SELECT * FROM _tbl LIMIT {n}"),
        None => "SELECT * FROM _tbl".to_string(),
    };
    debug!(sql = %sql, "Executing read-table query");

    let df = ctx.sql(&sql).await.map_err(ServiceError::DataFusion)?;
    let schema: SchemaRef = Arc::clone(df.schema().inner());
    let batch_stream = df.execute_stream().await.map_err(ServiceError::DataFusion)?;
    Ok((schema, batch_stream))
}

async fn execute_sql_stream(
    cmd: SqlCommand,
) -> Result<(SchemaRef, SendableRecordBatchStream), ServiceError> {
    let ctx = SessionContext::new();

    if let (Some(table_path), Some(table_name)) = (&cmd.table_path, &cmd.table_name) {
        register_delta_table(
            &ctx,
            table_name,
            table_path,
            cmd.storage_account.as_deref(),
            cmd.sas_token.as_deref(),
            cmd.version,
        )
        .await?;
    }

    debug!(sql = %cmd.sql, "Executing SQL query");
    let df = ctx.sql(&cmd.sql).await.map_err(ServiceError::DataFusion)?;
    let schema: SchemaRef = Arc::clone(df.schema().inner());
    let batch_stream = df.execute_stream().await.map_err(ServiceError::DataFusion)?;
    Ok((schema, batch_stream))
}

/// Converts a DataFusion `SendableRecordBatchStream` into a boxed
/// `Stream<Item = Result<FlightData, Status>>`.
///
/// Uses [`FlightDataEncoderBuilder`] which handles:
/// - Emitting the schema as the first FlightData message
/// - IPC-encoding each RecordBatch
/// - Dictionary encoding when needed
fn build_flight_stream(
    schema: SchemaRef,
    batch_stream: datafusion::physical_plan::SendableRecordBatchStream,
) -> BoxedFlightStream {
    // FlightDataEncoderBuilder expects Stream<Item = Result<RecordBatch, FlightError>>.
    let mapped = batch_stream.map_err(|e| {
        arrow_flight::error::FlightError::ExternalError(Box::new(e))
    });

    let flight_stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(mapped);

    // Final mapping: FlightError → tonic::Status.
    let status_stream = flight_stream.map_err(|e| Status::internal(format!("{e}")));
    Box::pin(status_stream)
}

// -------------------------------------------------------------------------- //
//  Tests
// -------------------------------------------------------------------------- //

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use arrow::record_batch::RecordBatch;
    use arrow_flight::utils::flight_data_to_batches;
    use futures::StreamExt;
    use url::Url;

    /// Creates a temp directory containing a Delta table with 3 rows:
    ///   id: [1, 2, 3], name: ["a", "b", "c"]
    ///
    /// Returns (path_string, _temp_dir_guard).  The guard must be kept alive
    /// for the duration of the test so the directory is not deleted.
    async fn create_test_delta_table() -> (String, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("failed to create temp dir");
        let table_path = tmp.path().join("test_table");
        std::fs::create_dir(&table_path).expect("failed to create table dir");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .expect("failed to create RecordBatch");

        let url = Url::from_file_path(&table_path).expect("failed to convert path to URL");

        // Create table + write data using DeltaTable API.
        // try_from_url loads the table (or returns an uninitialized table for new locations).
        let table: deltalake::DeltaTable =
            deltalake::DeltaTable::try_from_url(url).await.expect("DeltaTable::try_from_url failed");
        let _table: deltalake::DeltaTable = table
            .write(vec![batch])
            .await
            .expect("write to delta table failed");

        let path_str = table_path.to_str().expect("non-UTF8 path");
        (path_str.to_string(), tmp)
    }

    /// Helper to build a FlightDescriptor with a read-table command JSON.
    /// Uses serde_json to ensure paths with backslashes (Windows) are properly escaped.
    fn read_descriptor(path: &str, num_rows: Option<u64>) -> FlightDescriptor {
        let mut map = serde_json::Map::new();
        map.insert("path".to_string(), serde_json::Value::String(path.to_string()));
        if let Some(n) = num_rows {
            map.insert("num_rows".to_string(), serde_json::Value::Number(n.into()));
        }
        let json = serde_json::to_string(&map).expect("failed to serialize read command");
        FlightDescriptor::new_cmd(json.into_bytes())
    }

    /// Helper to build a FlightDescriptor with a SQL command JSON.
    /// Uses serde_json to ensure paths with backslashes (Windows) are properly escaped.
    fn sql_descriptor(sql: &str, table_path: Option<&str>, table_name: Option<&str>) -> FlightDescriptor {
        let mut map = serde_json::Map::new();
        map.insert("sql".to_string(), serde_json::Value::String(sql.to_string()));
        if let Some(tp) = table_path {
            map.insert("table_path".to_string(), serde_json::Value::String(tp.to_string()));
        }
        if let Some(tn) = table_name {
            map.insert("table_name".to_string(), serde_json::Value::String(tn.to_string()));
        }
        let json = serde_json::to_string(&map).expect("failed to serialize sql command");
        FlightDescriptor::new_cmd(json.into_bytes())
    }

    // -- GetFlightInfo tests -----------------------------------------------

    #[tokio::test]
    async fn get_flight_info_read_table_returns_schema() {
        let (path, _guard) = create_test_delta_table().await;
        let descriptor = read_descriptor(&path, None);

        let info = handle_get_flight_info(descriptor).await.unwrap();

        // Schema should have 2 fields: id (Int32) and name (Utf8).
        let schema = info.clone().try_decode_schema().unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");

        // Should have exactly one endpoint with a ticket.
        assert_eq!(info.endpoint.len(), 1);
        assert!(info.endpoint[0].ticket.is_some());

        // Totals are unknown.
        assert_eq!(info.total_records, -1);
        assert_eq!(info.total_bytes, -1);
    }

    #[tokio::test]
    async fn get_flight_info_sql_returns_empty_schema() {
        let descriptor = sql_descriptor("SELECT 1 AS x", None, None);

        let info = handle_get_flight_info(descriptor).await.unwrap();

        let schema = info.try_decode_schema().unwrap();
        assert_eq!(schema.fields().len(), 0, "SQL mode should return empty schema");
    }

    // -- GetSchema tests ---------------------------------------------------

    #[tokio::test]
    async fn get_schema_returns_delta_table_schema() {
        let (path, _guard) = create_test_delta_table().await;
        let descriptor = read_descriptor(&path, None);

        let result = handle_get_schema(descriptor).await.unwrap();

        // Decode the SchemaResult into an Arrow Schema.
        let schema: Schema = (&result).try_into().unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(*schema.field(0).data_type(), DataType::Int32);
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
    }

    // -- DoGet tests -------------------------------------------------------

    /// Helper to build a Ticket with a read-table command JSON (properly escaped).
    fn read_ticket(path: &str, num_rows: Option<u64>) -> Ticket {
        let mut map = serde_json::Map::new();
        map.insert("path".to_string(), serde_json::Value::String(path.to_string()));
        if let Some(n) = num_rows {
            map.insert("num_rows".to_string(), serde_json::Value::Number(n.into()));
        }
        let json = serde_json::to_string(&map).expect("failed to serialize read ticket");
        Ticket::new(json)
    }

    /// Helper to build a Ticket with a SQL command JSON (properly escaped).
    fn sql_ticket(sql: &str, table_path: Option<&str>, table_name: Option<&str>) -> Ticket {
        let mut map = serde_json::Map::new();
        map.insert("sql".to_string(), serde_json::Value::String(sql.to_string()));
        if let Some(tp) = table_path {
            map.insert("table_path".to_string(), serde_json::Value::String(tp.to_string()));
        }
        if let Some(tn) = table_name {
            map.insert("table_name".to_string(), serde_json::Value::String(tn.to_string()));
        }
        let json = serde_json::to_string(&map).expect("failed to serialize sql ticket");
        Ticket::new(json)
    }

    /// Collects all FlightData from a DoGet stream and decodes into RecordBatches.
    async fn collect_do_get(ticket: Ticket) -> Vec<RecordBatch> {
        let stream = handle_do_get(ticket).await.unwrap();
        let flight_data: Vec<FlightData> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        flight_data_to_batches(&flight_data).unwrap()
    }

    #[tokio::test]
    async fn do_get_read_table_returns_all_rows() {
        let (path, _guard) = create_test_delta_table().await;
        let ticket = read_ticket(&path, None);

        let batches = collect_do_get(ticket).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        // Verify schema is correct.
        if let Some(batch) = batches.first() {
            assert_eq!(batch.schema().fields().len(), 2);
            assert_eq!(batch.schema().field(0).name(), "id");
        }
    }

    #[tokio::test]
    async fn do_get_read_table_with_limit() {
        let (path, _guard) = create_test_delta_table().await;
        let ticket = read_ticket(&path, Some(2));

        let batches = collect_do_get(ticket).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn do_get_sql_select_literal() {
        // SQL mode without registering a table — just execute a literal query.
        let ticket = sql_ticket("SELECT 42 AS answer", None, None);

        let batches = collect_do_get(ticket).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);

        let batch = &batches[0];
        assert_eq!(batch.schema().field(0).name(), "answer");
    }

    #[tokio::test]
    async fn do_get_sql_with_registered_table() {
        let (path, _guard) = create_test_delta_table().await;
        let ticket = sql_ticket(
            "SELECT id FROM tbl WHERE id > 1",
            Some(&path),
            Some("tbl"),
        );

        let batches = collect_do_get(ticket).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2); // rows with id=2 and id=3

        let batch = &batches[0];
        assert_eq!(batch.schema().fields().len(), 1); // only "id" column
    }

    // -- Error case tests --------------------------------------------------

    #[tokio::test]
    async fn get_flight_info_invalid_path_returns_error() {
        let descriptor = read_descriptor("/nonexistent/path/to/nowhere", None);
        let result = handle_get_flight_info(descriptor).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn do_get_invalid_json_returns_error() {
        let ticket = Ticket::new("not valid json");
        let result = handle_do_get(ticket).await;
        assert!(result.is_err());
    }

    // -- Time-travel (versioned read) tests --------------------------------

    /// Creates a Delta table with two versions:
    /// - Version 0: 2 rows — (1,"v0_a"), (2,"v0_b")
    /// - Version 1: 4 rows — appends (3,"v1_c"), (4,"v1_d")
    async fn create_time_travel_table() -> (String, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("failed to create temp dir");
        let table_path = tmp.path().join("tt_table");
        std::fs::create_dir(&table_path).expect("failed to create table dir");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Version 0: 2 rows.
        let batch0 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["v0_a", "v0_b"])),
            ],
        )
        .expect("batch0");

        let url = Url::from_file_path(&table_path).expect("url");
        let table: deltalake::DeltaTable =
            deltalake::DeltaTable::try_from_url(url).await.expect("try_from_url");
        let table: deltalake::DeltaTable = table.write(vec![batch0]).await.expect("write v0");

        // Version 1: append 2 more rows.
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![3, 4])),
                Arc::new(StringArray::from(vec!["v1_c", "v1_d"])),
            ],
        )
        .expect("batch1");

        let _table: deltalake::DeltaTable = table
            .write(vec![batch1])
            .with_save_mode(deltalake::protocol::SaveMode::Append)
            .await
            .expect("write v1");

        let path_str = table_path.to_str().expect("non-UTF8 path");
        (path_str.to_string(), tmp)
    }

    /// Helper to build a read-table ticket with a version field.
    fn read_ticket_versioned(path: &str, version: i64) -> Ticket {
        let mut map = serde_json::Map::new();
        map.insert("path".to_string(), serde_json::Value::String(path.to_string()));
        map.insert("version".to_string(), serde_json::Value::Number(version.into()));
        let json = serde_json::to_string(&map).expect("serialize");
        Ticket::new(json)
    }

    #[tokio::test]
    async fn do_get_time_travel_version_0_returns_2_rows() {
        let (path, _guard) = create_time_travel_table().await;
        let ticket = read_ticket_versioned(&path, 0);

        let batches = collect_do_get(ticket).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2, "Version 0 should have 2 rows");
    }

    #[tokio::test]
    async fn do_get_time_travel_version_1_returns_4_rows() {
        let (path, _guard) = create_time_travel_table().await;
        let ticket = read_ticket_versioned(&path, 1);

        let batches = collect_do_get(ticket).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4, "Version 1 should have 4 rows");
    }

    #[tokio::test]
    async fn do_get_time_travel_latest_returns_4_rows() {
        let (path, _guard) = create_time_travel_table().await;
        // No version → latest (version 1).
        let ticket = read_ticket(&path, None);

        let batches = collect_do_get(ticket).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4, "Latest version should have 4 rows");
    }

    // -- Partitioned table tests -------------------------------------------

    /// Creates a partitioned Delta table with partition column `region`.
    /// 5 rows: (1,"a","us"), (2,"b","eu"), (3,"c","us"), (4,"d","eu"), (5,"e","us")
    async fn create_partitioned_table() -> (String, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("temp dir");
        let table_path = tmp.path().join("part_table");
        std::fs::create_dir(&table_path).expect("create table dir");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("region", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
                Arc::new(StringArray::from(vec!["us", "eu", "us", "eu", "us"])),
            ],
        )
        .expect("batch");

        let url = Url::from_file_path(&table_path).expect("url");
        let table: deltalake::DeltaTable =
            deltalake::DeltaTable::try_from_url(url).await.expect("try_from_url");
        let _table: deltalake::DeltaTable = table
            .write(vec![batch])
            .with_partition_columns(vec!["region"])
            .await
            .expect("write partitioned");

        let path_str = table_path.to_str().expect("non-UTF8 path");
        (path_str.to_string(), tmp)
    }

    #[tokio::test]
    async fn do_get_partitioned_table_returns_all_rows() {
        let (path, _guard) = create_partitioned_table().await;
        let ticket = read_ticket(&path, None);

        let batches = collect_do_get(ticket).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5, "Partitioned table should have all 5 rows");

        // Schema should include the partition column.
        if let Some(batch) = batches.first() {
            let schema = batch.schema();
            let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert!(field_names.contains(&"region"), "Schema should include partition column 'region'");
        }
    }

    #[tokio::test]
    async fn do_get_partitioned_table_sql_filter_on_partition() {
        let (path, _guard) = create_partitioned_table().await;
        let ticket = sql_ticket(
            "SELECT id, name FROM tbl WHERE region = 'us'",
            Some(&path),
            Some("tbl"),
        );

        let batches = collect_do_get(ticket).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "WHERE region='us' should return 3 rows");
    }

    // -- Checked-in fixture tests: Column Mapping --------------------------

    /// Returns the absolute path to a checked-in test fixture under
    /// `tests/DeltaTableService.Tests/data/<name>`.
    ///
    /// The Rust project is at `src/DeltaTableService.Server/v3/`, so we
    /// navigate up 3 levels from `CARGO_MANIFEST_DIR` to the repo root.
    fn fixture_path(name: &str) -> String {
        let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let repo_root = manifest
            .parent()  // src/DeltaTableService.Server
            .and_then(|p| p.parent())  // src
            .and_then(|p| p.parent())  // repo root
            .expect("Cannot resolve repo root from CARGO_MANIFEST_DIR");
        let path = repo_root
            .join("tests")
            .join("DeltaTableService.Tests")
            .join("data")
            .join(name);
        assert!(
            path.exists(),
            "Fixture not found at {}",
            path.display()
        );
        path.to_str().expect("non-UTF8 fixture path").to_string()
    }

    #[tokio::test]
    async fn fixture_column_mapping_get_schema_returns_logical_names() {
        let path = fixture_path("delta_test_column_mapping_name");
        let descriptor = read_descriptor(&path, None);

        let result = handle_get_schema(descriptor).await.unwrap();
        let schema: Schema = (&result).try_into().unwrap();

        // The schema should expose logical column names (not physical UUIDs).
        assert_eq!(schema.fields().len(), 2, "Expected 2 columns");
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(*schema.field(0).data_type(), DataType::Int32);
        assert_eq!(schema.field(1).name(), "city");
        assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
    }

    #[tokio::test]
    async fn fixture_column_mapping_do_get_returns_3_rows() {
        let path = fixture_path("delta_test_column_mapping_name");
        let ticket = read_ticket(&path, None);

        let batches = collect_do_get(ticket).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "Column mapping fixture should have 3 rows");

        // Verify the schema uses logical names.
        if let Some(batch) = batches.first() {
            assert_eq!(batch.schema().field(0).name(), "id");
            assert_eq!(batch.schema().field(1).name(), "city");
        }
    }

    #[tokio::test]
    async fn fixture_column_mapping_do_get_returns_correct_data() {
        let path = fixture_path("delta_test_column_mapping_name");
        let ticket = read_ticket(&path, None);

        let batches = collect_do_get(ticket).await;

        // Collect all rows: (id, city).
        let mut rows: Vec<(i32, String)> = Vec::new();
        for batch in &batches {
            let ids = batch.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id column should be Int32Array");
            // delta-rs / PySpark may write Utf8 or Utf8View; handle both.
            let cities: Vec<String> = (0..batch.num_rows())
                .map(|i| {
                    let col = batch.column(1);
                    if let Some(sa) = col.as_any().downcast_ref::<StringArray>() {
                        sa.value(i).to_string()
                    } else if let Some(sva) = col.as_any().downcast_ref::<arrow::array::StringViewArray>() {
                        sva.value(i).to_string()
                    } else {
                        panic!("Unexpected array type for city column: {:?}", col.data_type());
                    }
                })
                .collect();

            for i in 0..batch.num_rows() {
                rows.push((ids.value(i), cities[i].clone()));
            }
        }

        rows.sort_by_key(|(id, _)| *id);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], (1, "Seattle".to_string()));
        assert_eq!(rows[1], (2, "Portland".to_string()));
        assert_eq!(rows[2], (3, "Denver".to_string()));
    }

    #[tokio::test]
    async fn fixture_column_mapping_sql_query_works() {
        let path = fixture_path("delta_test_column_mapping_name");
        let ticket = sql_ticket(
            "SELECT id, city FROM tbl WHERE id >= 2 ORDER BY id",
            Some(&path),
            Some("tbl"),
        );

        let batches = collect_do_get(ticket).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2, "WHERE id >= 2 should return 2 rows");
    }

    // -- Checked-in fixture tests: Deletion Vectors ------------------------

    #[tokio::test]
    async fn fixture_deletion_vector_get_schema() {
        let path = fixture_path("delta_test_deletion_vector");
        let descriptor = read_descriptor(&path, None);

        let result = handle_get_schema(descriptor).await.unwrap();
        let schema: Schema = (&result).try_into().unwrap();

        assert_eq!(schema.fields().len(), 2, "Expected 2 columns");
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(*schema.field(0).data_type(), DataType::Int32);
        assert_eq!(schema.field(1).name(), "value");
        assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
    }

    #[tokio::test]
    async fn fixture_deletion_vector_do_get_returns_4_rows() {
        let path = fixture_path("delta_test_deletion_vector");
        let ticket = read_ticket(&path, None);

        let batches = collect_do_get(ticket).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 4,
            "Deletion vector fixture should have 4 rows (id=3 deleted)"
        );
    }

    #[tokio::test]
    async fn fixture_deletion_vector_do_get_excludes_deleted_row() {
        let path = fixture_path("delta_test_deletion_vector");
        let ticket = read_ticket(&path, None);

        let batches = collect_do_get(ticket).await;

        // Collect all ids.
        let mut ids: Vec<i32> = Vec::new();
        for batch in &batches {
            let id_col = batch.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id column should be Int32Array");
            for i in 0..batch.num_rows() {
                ids.push(id_col.value(i));
            }
        }

        ids.sort();
        assert_eq!(ids, vec![1, 2, 4, 5], "id=3 should be excluded by deletion");
    }

    #[tokio::test]
    async fn fixture_deletion_vector_do_get_correct_data() {
        let path = fixture_path("delta_test_deletion_vector");
        let ticket = read_ticket(&path, None);

        let batches = collect_do_get(ticket).await;

        let mut rows: Vec<(i32, String)> = Vec::new();
        for batch in &batches {
            let ids = batch.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id column should be Int32Array");
            for i in 0..batch.num_rows() {
                let col = batch.column(1);
                let value = if let Some(sa) = col.as_any().downcast_ref::<StringArray>() {
                    sa.value(i).to_string()
                } else if let Some(sva) = col.as_any().downcast_ref::<arrow::array::StringViewArray>() {
                    sva.value(i).to_string()
                } else {
                    panic!("Unexpected array type for value column: {:?}", col.data_type());
                };
                rows.push((ids.value(i), value));
            }
        }

        rows.sort_by_key(|(id, _)| *id);
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0], (1, "one".to_string()));
        assert_eq!(rows[1], (2, "two".to_string()));
        assert_eq!(rows[2], (4, "four".to_string()));
        assert_eq!(rows[3], (5, "five".to_string()));
    }

    #[tokio::test]
    async fn fixture_deletion_vector_sql_query_works() {
        let path = fixture_path("delta_test_deletion_vector");
        let ticket = sql_ticket(
            "SELECT id, value FROM tbl WHERE id > 2 ORDER BY id",
            Some(&path),
            Some("tbl"),
        );

        let batches = collect_do_get(ticket).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // id=3 deleted, so only id=4 and id=5 match id > 2.
        assert_eq!(total_rows, 2, "WHERE id > 2 should return 2 rows (id=3 deleted)");
    }
}
