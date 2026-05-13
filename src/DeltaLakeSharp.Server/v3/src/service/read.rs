// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Transport-neutral read/query helpers for the Delta Table Service V3.

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute::cast;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::SendableRecordBatchStream;
use deltalake::delta_datafusion::DeltaCdfTableProvider;
use futures::StreamExt;
use tracing::{debug, info};

mod partitioning;
#[cfg(test)]
mod tests;

use super::helpers::{
    open_delta_table, open_delta_table_for_datafusion, register_delta_table,
    register_delta_table_with_files, request_version_to_delta, success_response_with_result,
};
use super::request::{
    Command, PartitionDescriptorMode, ReadChangeDataCommand, ReadCommand, SqlCommand,
};
use partitioning::{
    PlannedPartitionMode, apply_partition_predicate_filter, decode_partition_token,
    encode_partition_token, plan_read_partitions, resolve_partition_files,
    resolve_partition_token_version,
};
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

pub async fn plan_read_partitions_from_command_bytes(
    cmd_bytes: &[u8],
) -> Result<serde_json::Value, ServiceError> {
    let command = Command::parse(cmd_bytes)?;
    let Command::Read(read_cmd) = command else {
        return Err(ServiceError::InvalidRequest(
            "partition planning only supports read-table commands".to_string(),
        ));
    };

    let partitions = plan_read_partitions(read_cmd).await?;
    let result = partitions
        .iter()
        .enumerate()
        .map(|(ordinal, partition)| {
            let token_payload = super::request::PartitionDescriptorPayload {
                version: partition.version,
                ordinal,
                total_partitions: partitions.len(),
                mode: match &partition.mode {
                    PlannedPartitionMode::FileSubset { files } => PartitionDescriptorMode::FileSubset {
                        file_paths: files.iter().map(|file| file.path.clone()).collect(),
                    },
                    PlannedPartitionMode::PartitionPredicate { keys } => {
                        PartitionDescriptorMode::PartitionPredicate { keys: keys.clone() }
                    }
                },
            };

            Ok(serde_json::json!({
                "token": encode_partition_token(&token_payload)?,
                "version": token_payload.version,
                "ordinal": ordinal,
                "totalPartitions": partitions.len(),
                "fileCount": match &partition.mode {
                    PlannedPartitionMode::FileSubset { files } => files.len(),
                    PlannedPartitionMode::PartitionPredicate { keys } => keys.len(),
                },
            }))
        })
        .collect::<Result<Vec<_>, ServiceError>>()?;

    Ok(success_response_with_result(
        "Planned read partitions successfully.",
        result,
    ))
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

    if let Some(token) = cmd.partition_token.as_deref() {
        let descriptor = decode_partition_token(token)?;
        match descriptor.mode {
            PartitionDescriptorMode::FileSubset { .. } => {
                let (version, files) = resolve_partition_files(&cmd, &descriptor).await?;
                let table = open_delta_table(
                    &cmd.path,
                    cmd.storage_account.as_deref(),
                    cmd.sas_token.as_deref(),
                    cmd.storage_options.as_ref(),
                    Some(version),
                )
                .await?;
                register_delta_table_with_files(&ctx, "_tbl", &table, files).await?;
            }
            PartitionDescriptorMode::PartitionPredicate { keys } => {
                let version = resolve_partition_token_version(&cmd, descriptor.version)?;
                register_delta_table(
                    &ctx,
                    "_tbl",
                    &cmd.path,
                    cmd.storage_account.as_deref(),
                    cmd.sas_token.as_deref(),
                    cmd.storage_options.as_ref(),
                    Some(version),
                )
                .await?;

                let df = ctx.table("_tbl").await.map_err(ServiceError::DataFusion)?;
                let df = apply_partition_predicate_filter(df, &keys).map_err(ServiceError::DataFusion)?;
                let df = if let Some(n) = cmd.num_rows {
                    df.limit(0, Some(n as usize)).map_err(ServiceError::DataFusion)?
                } else {
                    df
                };
                let schema: SchemaRef = Arc::clone(df.schema().inner());
                let batch_stream = df.execute_stream().await.map_err(ServiceError::DataFusion)?;
                let batch_stream = normalize_stream_to_schema(batch_stream, Arc::clone(&schema));
                return Ok((schema, batch_stream));
            }
        }
    } else {
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
    }

    let sql = match cmd.num_rows {
        Some(n) => format!("SELECT * FROM _tbl LIMIT {n}"),
        None => "SELECT * FROM _tbl".to_string(),
    };
    debug!(sql = %sql, "Executing read-table query");

    let df = ctx.sql(&sql).await.map_err(ServiceError::DataFusion)?;
    let schema: SchemaRef = Arc::clone(df.schema().inner());
    let batch_stream = df.execute_stream().await.map_err(ServiceError::DataFusion)?;
    let batch_stream = normalize_stream_to_schema(batch_stream, Arc::clone(&schema));
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

    let starting_version = request_version_to_delta(cmd.starting_version, "starting_version")?;
    let mut builder = table
        .scan_cdf()
        .with_session_state(Arc::new(ctx.state()))
        .with_starting_version(starting_version);
    if let Some(ending_version) = cmd.ending_version {
        builder = builder.with_ending_version(request_version_to_delta(
            ending_version,
            "ending_version",
        )?);
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

fn normalize_stream_to_schema(
    stream: SendableRecordBatchStream,
    schema: SchemaRef,
) -> SendableRecordBatchStream {
    let normalized = stream.map({
        let schema = Arc::clone(&schema);
        move |batch_result| match batch_result {
            Ok(batch) => normalize_batch_to_schema(batch, &schema)
                .map_err(datafusion::error::DataFusionError::from),
            Err(error) => Err(error),
        }
    });

    Box::pin(RecordBatchStreamAdapter::new(schema, normalized))
}

fn normalize_batch_to_schema(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, ArrowError> {
    if batch.schema().as_ref() == schema.as_ref() {
        return Ok(batch);
    }

    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields().iter())
        .map(|(column, field)| normalize_array_to_type(Arc::clone(column), field.data_type()))
        .collect::<Result<Vec<ArrayRef>, ArrowError>>()?;

    RecordBatch::try_new(Arc::clone(schema), columns)
}

fn normalize_array_to_type(array: ArrayRef, target_type: &arrow::datatypes::DataType) -> Result<ArrayRef, ArrowError> {
    if array.data_type() == target_type {
        Ok(array)
    } else {
        cast(&array, target_type)
    }
}
