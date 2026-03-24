// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Transport-neutral read/query helpers for the Delta Table Service V3.

use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use deltalake::delta_datafusion::DeltaCdfTableProvider;
use futures::StreamExt;
use tracing::{debug, info};

use super::helpers::{open_delta_table, open_delta_table_for_datafusion, register_delta_table};
use super::request::{Command, ReadChangeDataCommand, ReadCommand, SqlCommand};
use crate::error::ServiceError;

/// Resolves the Arrow schema for a read/query command without committing to any
/// transport representation.
pub async fn resolve_schema_from_command_bytes(
    cmd_bytes: &[u8],
) -> Result<SchemaRef, ServiceError> {
    let command = Command::parse(cmd_bytes)?;

    match command {
        Command::Read(read_cmd) => {
            info!(path = %read_cmd.path, "Resolving schema: read-table mode");
            get_delta_schema(&read_cmd).await
        }
        Command::Sql(_) => Ok(Arc::new(Schema::empty())),
    }
}

/// Resolves a command payload into a transport-neutral Arrow batch reader.
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

pub async fn resolve_change_data_reader_from_command_bytes(
    cmd_bytes: &[u8],
    runtime_handle: tokio::runtime::Handle,
) -> Result<Box<dyn RecordBatchReader<Item = Result<RecordBatch, ArrowError>> + Send>, ServiceError>
{
    let cmd: ReadChangeDataCommand = serde_json::from_slice(cmd_bytes).map_err(ServiceError::Json)?;
    info!(
        path = %cmd.path,
        starting_version = cmd.starting_version,
        ending_version = ?cmd.ending_version,
        sql = ?cmd.sql,
        "Resolving batch reader: change-data-feed mode"
    );
    change_data_batch_reader(cmd, runtime_handle).await
}

async fn get_delta_schema(cmd: &ReadCommand) -> Result<SchemaRef, ServiceError> {
    let table = open_delta_table(
        &cmd.path,
        cmd.storage_account.as_deref(),
        cmd.sas_token.as_deref(),
        cmd.storage_options.as_ref(),
        cmd.version,
    )
    .await?;

    let schema = table
        .snapshot()
        .map_err(ServiceError::Delta)?
        .snapshot()
        .arrow_schema();
    Ok(schema)
}

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

async fn change_data_batch_reader(
    cmd: ReadChangeDataCommand,
    runtime_handle: tokio::runtime::Handle,
) -> Result<Box<dyn RecordBatchReader<Item = Result<RecordBatch, ArrowError>> + Send>, ServiceError>
{
    let (schema, stream) = execute_change_data_stream(cmd).await?;
    Ok(Box::new(AsyncRecordBatchReader {
        schema,
        runtime_handle,
        stream,
    }))
}

async fn execute_read_table_stream(
    cmd: ReadCommand,
) -> Result<(SchemaRef, SendableRecordBatchStream), ServiceError> {
    let ctx = create_session_context(cmd.batch_size)?;
    register_delta_table(
        &ctx,
        "_tbl",
        &cmd.path,
        cmd.storage_account.as_deref(),
        cmd.sas_token.as_deref(),
        cmd.storage_options.as_ref(),
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
    let ctx = create_session_context(cmd.batch_size)?;

    if let (Some(table_path), Some(table_name)) = (&cmd.table_path, &cmd.table_name) {
        register_delta_table(
            &ctx,
            table_name,
            table_path,
            cmd.storage_account.as_deref(),
            cmd.sas_token.as_deref(),
            cmd.storage_options.as_ref(),
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

async fn execute_change_data_stream(
    cmd: ReadChangeDataCommand,
) -> Result<(SchemaRef, SendableRecordBatchStream), ServiceError> {
    let ctx = create_session_context(cmd.batch_size)?;
    let table = open_delta_table_for_datafusion(
        &ctx,
        &cmd.path,
        cmd.storage_account.as_deref(),
        cmd.sas_token.as_deref(),
        cmd.storage_options.as_ref(),
        None,
    )
    .await?;

    let mut builder = table
        .scan_cdf()
        .with_session_state(Arc::new(ctx.state()))
        .with_starting_version(cmd.starting_version);
    if let Some(ending_version) = cmd.ending_version {
        builder = builder.with_ending_version(ending_version);
    }

    let provider = Arc::new(DeltaCdfTableProvider::try_new(builder).map_err(ServiceError::Delta)?);
    let df = if let Some(sql) = cmd.sql {
        ctx.register_table("_cdf", provider)
            .map_err(ServiceError::DataFusion)?;
        debug!(sql = %sql, "Executing change-data SQL query");
        ctx.sql(&sql).await.map_err(ServiceError::DataFusion)?
    } else {
        ctx.read_table(provider).map_err(ServiceError::DataFusion)?
    };
    let schema: SchemaRef = Arc::clone(df.schema().inner());
    let batch_stream = df.execute_stream().await.map_err(ServiceError::DataFusion)?;
    Ok((schema, batch_stream))
}

fn create_session_context(batch_size: Option<usize>) -> Result<SessionContext, ServiceError> {
    match batch_size {
        Some(0) => Err(ServiceError::InvalidRequest(
            "batch_size must be greater than zero".to_string(),
        )),
        Some(size) => {
            let config = SessionConfig::new().with_batch_size(size);
            Ok(SessionContext::new_with_config(config))
        }
        None => Ok(SessionContext::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray, StringViewArray};
    use arrow::datatypes::{DataType, Field};
    use url::Url;

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
        let table: deltalake::DeltaTable = deltalake::DeltaTable::try_from_url(url)
            .await
            .expect("DeltaTable::try_from_url failed");
        let _table: deltalake::DeltaTable = table
            .write(vec![batch])
            .await
            .expect("write to delta table failed");

        let path_str = table_path.to_str().expect("non-UTF8 path");
        (path_str.to_string(), tmp)
    }

    fn read_command_bytes(path: &str, num_rows: Option<u64>) -> Vec<u8> {
        let mut map = serde_json::Map::new();
        map.insert("path".to_string(), serde_json::Value::String(path.to_string()));
        if let Some(n) = num_rows {
            map.insert("num_rows".to_string(), serde_json::Value::Number(n.into()));
        }
        serde_json::to_vec(&map).expect("failed to serialize read command")
    }

    fn sql_command_bytes(sql: &str, table_path: Option<&str>, table_name: Option<&str>) -> Vec<u8> {
        let mut map = serde_json::Map::new();
        map.insert("sql".to_string(), serde_json::Value::String(sql.to_string()));
        if let Some(tp) = table_path {
            map.insert("table_path".to_string(), serde_json::Value::String(tp.to_string()));
        }
        if let Some(tn) = table_name {
            map.insert("table_name".to_string(), serde_json::Value::String(tn.to_string()));
        }
        serde_json::to_vec(&map).expect("failed to serialize sql command")
    }

    fn sql_command_bytes_with_batch_size(
        sql: &str,
        table_path: Option<&str>,
        table_name: Option<&str>,
        batch_size: usize,
    ) -> Vec<u8> {
        let mut map = serde_json::Map::new();
        map.insert("sql".to_string(), serde_json::Value::String(sql.to_string()));
        map.insert(
            "batch_size".to_string(),
            serde_json::Value::Number(serde_json::Number::from(batch_size as u64)),
        );
        if let Some(tp) = table_path {
            map.insert("table_path".to_string(), serde_json::Value::String(tp.to_string()));
        }
        if let Some(tn) = table_name {
            map.insert("table_name".to_string(), serde_json::Value::String(tn.to_string()));
        }
        serde_json::to_vec(&map).expect("failed to serialize sql command")
    }

    async fn collect_batches(command: &[u8]) -> Vec<RecordBatch> {
        let parsed = Command::parse(command).unwrap();
        let (_schema, mut stream) = match parsed {
            Command::Read(read_cmd) => execute_read_table_stream(read_cmd).await.unwrap(),
            Command::Sql(sql_cmd) => execute_sql_stream(sql_cmd).await.unwrap(),
        };

        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.unwrap());
        }
        batches
    }

    #[tokio::test]
    async fn resolve_schema_read_table_returns_schema() {
        let (path, _guard) = create_test_delta_table().await;
        let schema = resolve_schema_from_command_bytes(&read_command_bytes(&path, None))
            .await
            .unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[tokio::test]
    async fn resolve_schema_sql_returns_empty_schema() {
        let schema = resolve_schema_from_command_bytes(&sql_command_bytes("SELECT 1 AS x", None, None))
            .await
            .unwrap();
        assert_eq!(schema.fields().len(), 0);
    }

    #[tokio::test]
    async fn read_batches_read_table_returns_all_rows() {
        let (path, _guard) = create_test_delta_table().await;
        let batches = collect_batches(&read_command_bytes(&path, None)).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn read_batches_read_table_with_limit() {
        let (path, _guard) = create_test_delta_table().await;
        let batches = collect_batches(&read_command_bytes(&path, Some(2))).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn read_batches_sql_select_literal() {
        let batches = collect_batches(&sql_command_bytes("SELECT 42 AS answer", None, None)).await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "answer");
    }

    #[tokio::test]
    async fn read_batches_sql_with_registered_table() {
        let (path, _guard) = create_test_delta_table().await;
        let batches = collect_batches(&sql_command_bytes(
            "SELECT id FROM tbl WHERE id > 1",
            Some(&path),
            Some("tbl"),
        ))
        .await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        assert_eq!(batches[0].schema().fields().len(), 1);
    }

    #[tokio::test]
    async fn read_batches_sql_with_batch_size_honors_max_batch_length() {
        let (path, _guard) = create_test_delta_table().await;
        let batches = collect_batches(&sql_command_bytes_with_batch_size(
            "SELECT id, name FROM tbl ORDER BY id",
            Some(&path),
            Some("tbl"),
            1,
        ))
        .await;

        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        assert!(batches.len() >= 3, "Expected one-row batches when batch_size=1.");
        assert!(batches.iter().all(|b| b.num_rows() <= 1));
    }

    #[tokio::test]
    async fn resolve_schema_invalid_path_returns_error() {
        let result = resolve_schema_from_command_bytes(&read_command_bytes("/nonexistent/path/to/nowhere", None)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn read_batches_invalid_json_returns_error() {
        let runtime = tokio::runtime::Handle::current();
        let result = resolve_batch_reader_from_command_bytes(b"not valid json", runtime).await;
        assert!(result.is_err());
    }

    async fn create_time_travel_table() -> (String, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("failed to create temp dir");
        let table_path = tmp.path().join("tt_table");
        std::fs::create_dir(&table_path).expect("failed to create table dir");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch0 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["v0_a", "v0_b"])),
            ],
        )
        .expect("batch0");

        let url = Url::from_file_path(&table_path).expect("url");
        let table: deltalake::DeltaTable = deltalake::DeltaTable::try_from_url(url).await.expect("try_from_url");
        let table: deltalake::DeltaTable = table.write(vec![batch0]).await.expect("write v0");

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

    fn read_command_bytes_versioned(path: &str, version: i64) -> Vec<u8> {
        let mut map = serde_json::Map::new();
        map.insert("path".to_string(), serde_json::Value::String(path.to_string()));
        map.insert("version".to_string(), serde_json::Value::Number(version.into()));
        serde_json::to_vec(&map).expect("serialize")
    }

    #[tokio::test]
    async fn read_batches_time_travel_version_0_returns_2_rows() {
        let (path, _guard) = create_time_travel_table().await;
        let batches = collect_batches(&read_command_bytes_versioned(&path, 0)).await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    }

    #[tokio::test]
    async fn read_batches_time_travel_version_1_returns_4_rows() {
        let (path, _guard) = create_time_travel_table().await;
        let batches = collect_batches(&read_command_bytes_versioned(&path, 1)).await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);
    }

    #[tokio::test]
    async fn read_batches_time_travel_latest_returns_4_rows() {
        let (path, _guard) = create_time_travel_table().await;
        let batches = collect_batches(&read_command_bytes(&path, None)).await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);
    }

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
        let table: deltalake::DeltaTable = deltalake::DeltaTable::try_from_url(url).await.expect("try_from_url");
        let _table: deltalake::DeltaTable = table
            .write(vec![batch])
            .with_partition_columns(vec!["region"])
            .await
            .expect("write partitioned");

        let path_str = table_path.to_str().expect("non-UTF8 path");
        (path_str.to_string(), tmp)
    }

    #[tokio::test]
    async fn read_batches_partitioned_table_returns_all_rows() {
        let (path, _guard) = create_partitioned_table().await;
        let batches = collect_batches(&read_command_bytes(&path, None)).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);

        if let Some(batch) = batches.first() {
            let schema = batch.schema();
            let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert!(field_names.contains(&"region"));
        }
    }

    #[tokio::test]
    async fn read_batches_partitioned_table_sql_filter_on_partition() {
        let (path, _guard) = create_partitioned_table().await;
        let batches = collect_batches(&sql_command_bytes(
            "SELECT id, name FROM tbl WHERE region = 'us'",
            Some(&path),
            Some("tbl"),
        ))
        .await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    }

    fn fixture_path(name: &str) -> String {
        let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let repo_root = manifest
            .parent()
            .and_then(|p| p.parent())
            .and_then(|p| p.parent())
            .expect("Cannot resolve repo root from CARGO_MANIFEST_DIR");
        let path = repo_root
            .join("tests")
            .join("DeltaTableService.Tests")
            .join("data")
            .join(name);
        assert!(path.exists(), "Fixture not found at {}", path.display());
        path.to_str().expect("non-UTF8 fixture path").to_string()
    }

    #[tokio::test]
    async fn fixture_column_mapping_get_schema_returns_logical_names() {
        let path = fixture_path("delta_test_column_mapping_name");
        let schema = resolve_schema_from_command_bytes(&read_command_bytes(&path, None))
            .await
            .unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(*schema.field(0).data_type(), DataType::Int32);
        assert_eq!(schema.field(1).name(), "city");
        assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
    }

    #[tokio::test]
    async fn fixture_column_mapping_read_returns_3_rows() {
        let path = fixture_path("delta_test_column_mapping_name");
        let batches = collect_batches(&read_command_bytes(&path, None)).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        if let Some(batch) = batches.first() {
            assert_eq!(batch.schema().field(0).name(), "id");
            assert_eq!(batch.schema().field(1).name(), "city");
        }
    }

    #[tokio::test]
    async fn fixture_column_mapping_read_returns_correct_data() {
        let path = fixture_path("delta_test_column_mapping_name");
        let batches = collect_batches(&read_command_bytes(&path, None)).await;

        let mut rows: Vec<(i32, String)> = Vec::new();
        for batch in &batches {
            let ids = batch.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id column should be Int32Array");
            let cities: Vec<String> = (0..batch.num_rows())
                .map(|i| {
                    let col = batch.column(1);
                    if let Some(sa) = col.as_any().downcast_ref::<StringArray>() {
                        sa.value(i).to_string()
                    } else if let Some(sva) = col.as_any().downcast_ref::<StringViewArray>() {
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
        let batches = collect_batches(&sql_command_bytes(
            "SELECT id, city FROM tbl WHERE id >= 2 ORDER BY id",
            Some(&path),
            Some("tbl"),
        ))
        .await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    }

    #[tokio::test]
    async fn fixture_deletion_vector_get_schema() {
        let path = fixture_path("delta_test_deletion_vector");
        let schema = resolve_schema_from_command_bytes(&read_command_bytes(&path, None))
            .await
            .unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(*schema.field(0).data_type(), DataType::Int32);
        assert_eq!(schema.field(1).name(), "value");
        assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
    }

    #[tokio::test]
    async fn fixture_deletion_vector_read_returns_4_rows() {
        let path = fixture_path("delta_test_deletion_vector");
        let batches = collect_batches(&read_command_bytes(&path, None)).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4);
    }

    #[tokio::test]
    async fn fixture_deletion_vector_read_excludes_deleted_row() {
        let path = fixture_path("delta_test_deletion_vector");
        let batches = collect_batches(&read_command_bytes(&path, None)).await;

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
        assert_eq!(ids, vec![1, 2, 4, 5]);
    }

    #[tokio::test]
    async fn fixture_deletion_vector_read_correct_data() {
        let path = fixture_path("delta_test_deletion_vector");
        let batches = collect_batches(&read_command_bytes(&path, None)).await;

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
                } else if let Some(sva) = col.as_any().downcast_ref::<StringViewArray>() {
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
        let batches = collect_batches(&sql_command_bytes(
            "SELECT id, value FROM tbl WHERE id > 2 ORDER BY id",
            Some(&path),
            Some("tbl"),
        ))
        .await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    }
}
