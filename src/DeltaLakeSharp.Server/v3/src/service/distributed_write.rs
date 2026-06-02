use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::datatypes::{Field, Schema};
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::{Action, Add};
use deltalake::logstore::object_store::path::Path;
use deltalake::logstore::object_store::{ObjectMeta, ObjectStoreExt};
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::table::state::DeltaTableState;
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

use crate::error::ServiceError;

use super::helpers::{open_delta_table, open_or_initialize_delta_table, path_to_url, storage_options};
use super::request::{
    arrow_type_from_str, AbortDistributedWriteCommand, BeginDistributedWriteCommand,
    ColumnDef, CommitDistributedWriteCommand, StageDistributedWriteCommand,
};

const DEFAULT_STAGING_PREFIX: &str = "_staging";
const ADDS_DIRECTORY: &str = "adds";
const DEFAULT_MAX_BUFFERED_RECORD_BATCHES: usize = 16;

pub async fn begin_distributed_write(body: &[u8]) -> Result<serde_json::Value, ServiceError> {
    let cmd: BeginDistributedWriteCommand = serde_json::from_slice(body)?;
    let run_id = cmd.run_id.ok_or_else(|| {
        ServiceError::InvalidRequest("distributed write run_id must be provided".to_string())
    })?;
    validate_uuid(&run_id, "run_id")?;
    let mode = validate_mode(&cmd.mode)?;
    let schema_mode = match cmd.schema_mode {
        Some(value) => Some(validate_schema_mode(&value)?.to_string()),
        None => None,
    };
    validate_supported_append_mode(mode)?;
    let overwrite_scope =
        validate_overwrite_scope(cmd.overwrite_scope.as_deref().unwrap_or("fullTable"))?;
    let adds_prefix = staging_adds_prefix(cmd.staging_prefix.as_deref(), &run_id)?;
    let staging_prefix = cmd
        .staging_prefix
        .unwrap_or_else(|| DEFAULT_STAGING_PREFIX.to_string());

    Ok(json!({
        "success": true,
        "message": "Distributed write run initialized.",
        "result": [{
            "runId": run_id,
            "tablePath": cmd.path,
            "mode": mode,
            "schemaMode": schema_mode,
            "overwriteScope": overwrite_scope,
            "stagingPrefix": staging_prefix,
            "addsPrefix": adds_prefix,
            "schema": cmd.schema.unwrap_or_default(),
            "configuration": cmd.configuration.unwrap_or_default(),
            "partitionBy": cmd.partition_by.unwrap_or_default(),
            "maxBufferedBytes": cmd.max_buffered_bytes,
            "maxBufferedRecordBatches": cmd.max_buffered_record_batches
        }]
    }))
}

pub async fn stage_distributed_write(
    cmd: StageDistributedWriteCommand,
    mut reader: ArrowArrayStreamReader,
) -> Result<serde_json::Value, ServiceError> {
    validate_uuid(&cmd.run_id, "run_id")?;
    validate_supported_append_mode(&cmd.mode)?;
    validate_optional_schema_mode(cmd.schema_mode.as_deref())?;
    validate_overwrite_scope(cmd.overwrite_scope.as_deref().unwrap_or("fullTable"))?;

    let partition_columns;
    let object_store;
    let mut writer;
    let mut table = open_or_initialize_delta_table(
        &cmd.path,
        cmd.storage_account.as_deref(),
        cmd.sas_token.as_deref(),
        cmd.storage_options.as_ref(),
    )
    .await?;
    object_store = table.object_store();
    if table.log_store().is_delta_table_location().await.map_err(ServiceError::Delta)? {
        table.load().await.map_err(ServiceError::Delta)?;
        validate_optional_schema_matches_existing_table(cmd.schema.as_ref(), table.snapshot().map_err(ServiceError::Delta)?)?;
        partition_columns = table
            .snapshot()
            .map_err(ServiceError::Delta)?
            .metadata()
            .partition_columns()
            .to_vec();
        validate_requested_partition_columns(cmd.partition_by.as_ref(), &partition_columns)?;
        writer = RecordBatchWriter::for_table(&table).map_err(ServiceError::Delta)?;
    } else {
        partition_columns = cmd.partition_by.clone().unwrap_or_default();
        validate_new_table_schema_partition_columns(cmd.schema.as_ref(), &partition_columns)?;
        let schema = arrow_schema_from_columns(cmd.schema.as_ref().expect("schema validated"));
        let table_url = path_to_url(&cmd.path)?;
        let opts = storage_options(
            cmd.storage_account.as_deref(),
            cmd.sas_token.as_deref(),
            cmd.storage_options.as_ref(),
        );
        writer = RecordBatchWriter::try_new(
            table_url.as_str(),
            Arc::new(schema),
            Some(partition_columns.clone()),
            Some(opts),
        )
        .map_err(ServiceError::Delta)?;
    }
    let adds_prefix = staging_adds_prefix(cmd.staging_prefix.as_deref(), &cmd.run_id)?;
    let max_buffered_record_batches = cmd
        .max_buffered_record_batches
        .unwrap_or(DEFAULT_MAX_BUFFERED_RECORD_BATCHES)
        .max(1);
    let max_buffered_bytes = cmd.max_buffered_bytes.unwrap_or(u64::MAX) as usize;

    let mut artifact_count = 0_usize;
    let mut added_file_count = 0_i64;
    let mut total_data_file_bytes = 0_i64;

    for batch_result in reader.by_ref() {
        let batch = batch_result.map_err(ServiceError::Arrow)?;
        writer.write(batch).await.map_err(ServiceError::Delta)?;

        if writer.buffer_len() >= max_buffered_bytes
            || writer.buffered_record_batch_count() >= max_buffered_record_batches
        {
            let adds = writer.flush().await.map_err(ServiceError::Delta)?;
            let stats = write_add_artifact(&object_store, &adds_prefix, adds).await?;
            artifact_count += stats.artifact_count;
            added_file_count += stats.added_file_count;
            total_data_file_bytes += stats.total_data_file_bytes;
        }
    }

    let adds = writer.flush().await.map_err(ServiceError::Delta)?;
    let stats = write_add_artifact(&object_store, &adds_prefix, adds).await?;
    artifact_count += stats.artifact_count;
    added_file_count += stats.added_file_count;
    total_data_file_bytes += stats.total_data_file_bytes;

    info!(
        run_id = %cmd.run_id,
        artifact_count,
        added_file_count,
        total_data_file_bytes,
        "staged distributed write artifacts"
    );

    Ok(json!({
        "success": true,
        "message": "Distributed write data staged.",
        "result": [{
            "runId": cmd.run_id,
            "stagingPrefix": cmd.staging_prefix.unwrap_or_else(|| DEFAULT_STAGING_PREFIX.to_string()),
            "artifactCount": artifact_count,
            "addedFileCount": added_file_count,
            "totalDataFileBytes": total_data_file_bytes
        }]
    }))
}

pub async fn commit_distributed_write(body: &[u8]) -> Result<serde_json::Value, ServiceError> {
    let cmd: CommitDistributedWriteCommand = serde_json::from_slice(body)?;
    validate_uuid(&cmd.run_id, "run_id")?;
    validate_supported_append_mode(&cmd.mode)?;
    validate_optional_schema_mode(cmd.schema_mode.as_deref())?;
    validate_overwrite_scope(cmd.overwrite_scope.as_deref().unwrap_or("fullTable"))?;

    commit_distributed_append_or_create(cmd).await
}

async fn commit_distributed_append_or_create(
    cmd: CommitDistributedWriteCommand,
) -> Result<serde_json::Value, ServiceError> {
    let mut table = open_or_initialize_delta_table(
        &cmd.path,
        cmd.storage_account.as_deref(),
        cmd.sas_token.as_deref(),
        cmd.storage_options.as_ref(),
    )
    .await?;

    if table.log_store().is_delta_table_location().await.map_err(ServiceError::Delta)? {
        table.load().await.map_err(ServiceError::Delta)?;
        let snapshot = table.snapshot().map_err(ServiceError::Delta)?.clone();
        validate_optional_schema_matches_existing_table(cmd.schema.as_ref(), &snapshot)?;
        let partition_columns = snapshot.metadata().partition_columns().to_vec();
        validate_requested_partition_columns(cmd.partition_by.as_ref(), &partition_columns)?;
        return commit_existing_table_append(cmd, table, snapshot, partition_columns).await;
    }

    let partition_columns = cmd.partition_by.clone().unwrap_or_default();
    validate_new_table_schema_partition_columns(cmd.schema.as_ref(), &partition_columns)?;

    let object_store = table.object_store();
    let adds_prefix = staging_adds_prefix(cmd.staging_prefix.as_deref(), &cmd.run_id)?;
    let artifacts = list_add_artifacts(&object_store, &adds_prefix).await?;
    if artifacts.is_empty() {
        return Err(ServiceError::InvalidRequest(format!(
            "no distributed write artifacts found for run_id {}",
            cmd.run_id
        )));
    }

    let mut actions = Vec::new();
    let mut added_file_count = 0_i64;
    let mut total_data_file_bytes = 0_i64;
    let validate_staged_data_files = cmd.validate_staged_data_files.unwrap_or(false);
    for artifact in &artifacts {
        let entries = read_add_artifact(&object_store, &artifact.location).await?;
        for entry in entries {
            validate_staged_add_artifact_entry(&entry, &partition_columns)?;
            if validate_staged_data_files {
                validate_staged_data_file(&object_store, &entry.add).await?;
            }

            let add = entry.add;
            total_data_file_bytes += add.size;
            added_file_count += 1;
            actions.push(Action::Add(add));
        }
    }

    if actions.is_empty() {
        return Err(ServiceError::InvalidRequest(format!(
            "distributed write run {} has no staged Add actions",
            cmd.run_id
        )));
    }

    let delta_columns = delta_columns_from_schema(cmd.schema.as_ref().expect("schema validated"));
    let mut create_builder = CreateBuilder::new()
        .with_location(path_to_url(&cmd.path)?.to_string())
        .with_save_mode(SaveMode::ErrorIfExists)
        .with_columns(delta_columns)
        .with_commit_properties(CommitProperties::default().with_metadata(distributed_commit_metadata(
            &cmd.run_id,
            artifacts.len(),
            added_file_count,
        )))
        .with_actions(actions);

    if !partition_columns.is_empty() {
        create_builder = create_builder.with_partition_columns(partition_columns);
    }

    if let Some(config) = &cmd.configuration {
        let config_pairs = config
            .iter()
            .map(|(key, value)| (key.clone(), Some(value.clone())))
            .collect::<Vec<_>>();
        create_builder = create_builder.with_configuration(config_pairs);
    }

    let opts = storage_options(
        cmd.storage_account.as_deref(),
        cmd.sas_token.as_deref(),
        cmd.storage_options.as_ref(),
    );
    if !opts.is_empty() {
        create_builder = create_builder.with_storage_options(opts);
    }

    let table = create_builder.await.map_err(ServiceError::Delta)?;
    let version = table.version();

    let mut staging_cleanup_failed = false;
    if cmd.cleanup_staging_artifacts.unwrap_or(true) {
        let artifact_paths = artifacts
            .iter()
            .map(|artifact| artifact.location.clone())
            .collect::<Vec<_>>();
        if let Err(error) = delete_artifacts(&object_store, artifact_paths).await {
            staging_cleanup_failed = true;
            warn!(
                run_id = %cmd.run_id,
                error = %error,
                "distributed write create-if-missing commit succeeded but staging artifact cleanup failed"
            );
        }
    }

    Ok(json!({
        "success": true,
        "message": "Distributed write committed.",
        "result": [{
            "runId": cmd.run_id,
            "version": version,
            "artifactCount": artifacts.len(),
            "addedFileCount": added_file_count,
            "totalDataFileBytes": total_data_file_bytes,
            "stagingCleanupFailed": staging_cleanup_failed
        }]
    }))
}

fn distributed_commit_metadata(
    run_id: &str,
    artifact_count: usize,
    added_file_count: i64,
) -> HashMap<String, serde_json::Value> {
    HashMap::from([
        ("distributedWrite".to_string(), json!(true)),
        ("runId".to_string(), json!(run_id)),
        ("artifactCount".to_string(), json!(artifact_count)),
        ("addedFileCount".to_string(), json!(added_file_count)),
    ])
}

async fn commit_existing_table_append(
    cmd: CommitDistributedWriteCommand,
    mut table: deltalake::DeltaTable,
    snapshot: DeltaTableState,
    partition_columns: Vec<String>,
) -> Result<serde_json::Value, ServiceError> {
    let adds_prefix = staging_adds_prefix(cmd.staging_prefix.as_deref(), &cmd.run_id)?;
    let object_store = table.object_store();
    let artifacts = list_add_artifacts(&object_store, &adds_prefix).await?;
    if artifacts.is_empty() {
        return Err(ServiceError::InvalidRequest(format!(
            "no distributed write artifacts found for run_id {}",
            cmd.run_id
        )));
    }

    let mut actions = Vec::new();
    let mut added_file_count = 0_i64;
    let mut total_data_file_bytes = 0_i64;
    let validate_staged_data_files = cmd.validate_staged_data_files.unwrap_or(false);
    for artifact in &artifacts {
        let entries = read_add_artifact(&object_store, &artifact.location).await?;
        for entry in entries {
            validate_staged_add_artifact_entry(&entry, &partition_columns)?;
            if validate_staged_data_files {
                validate_staged_data_file(&object_store, &entry.add).await?;
            }

            let add = entry.add;
            total_data_file_bytes += add.size;
            added_file_count += 1;
            actions.push(Action::Add(add));
        }
    }

    if actions.is_empty() {
        return Err(ServiceError::InvalidRequest(format!(
            "distributed write run {} has no staged Add actions",
            cmd.run_id
        )));
    }

    let operation = DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by: if partition_columns.is_empty() {
            None
        } else {
            Some(partition_columns)
        },
        predicate: None,
    };
    let finalized = CommitBuilder::default()
        .with_app_metadata(distributed_commit_metadata(
            &cmd.run_id,
            artifacts.len(),
            added_file_count,
        ))
        .with_actions(actions)
        .build(Some(&snapshot), table.log_store(), operation)
        .await
        .map_err(ServiceError::Delta)?;
    let version = finalized.version();
    table.update_state().await.map_err(ServiceError::Delta)?;

    let mut staging_cleanup_failed = false;
    if cmd.cleanup_staging_artifacts.unwrap_or(true) {
        let artifact_paths = artifacts
            .iter()
            .map(|artifact| artifact.location.clone())
            .collect::<Vec<_>>();
        if let Err(error) = delete_artifacts(&object_store, artifact_paths).await {
            staging_cleanup_failed = true;
            warn!(
                run_id = %cmd.run_id,
                error = %error,
                "distributed write commit succeeded but staging artifact cleanup failed"
            );
        }
    }

    Ok(json!({
        "success": true,
        "message": "Distributed write committed.",
        "result": [{
            "runId": cmd.run_id,
            "version": version,
            "artifactCount": artifacts.len(),
            "addedFileCount": added_file_count,
            "totalDataFileBytes": total_data_file_bytes,
            "stagingCleanupFailed": staging_cleanup_failed
        }]
    }))
}

pub async fn abort_distributed_write(body: &[u8]) -> Result<serde_json::Value, ServiceError> {
    let cmd: AbortDistributedWriteCommand = serde_json::from_slice(body)?;
    validate_uuid(&cmd.run_id, "run_id")?;
    let table = open_delta_table(
        &cmd.path,
        cmd.storage_account.as_deref(),
        cmd.sas_token.as_deref(),
        cmd.storage_options.as_ref(),
        None,
    )
    .await?;
    let object_store = table.object_store();
    let adds_prefix = staging_adds_prefix(cmd.staging_prefix.as_deref(), &cmd.run_id)?;
    let artifacts = list_add_artifacts(&object_store, &adds_prefix).await?;
    let deleted_count = artifacts.len();
    let artifact_paths = artifacts
        .into_iter()
        .map(|artifact| artifact.location)
        .collect::<Vec<_>>();
    delete_artifacts(&object_store, artifact_paths).await?;

    Ok(json!({
        "success": true,
        "message": "Distributed write staging artifacts deleted.",
        "result": [{
            "runId": cmd.run_id,
            "deletedArtifactCount": deleted_count
        }]
    }))
}

pub(super) fn staging_adds_prefix(
    staging_prefix: Option<&str>,
    run_id: &str,
) -> Result<String, ServiceError> {
    let staging_prefix = staging_prefix.unwrap_or(DEFAULT_STAGING_PREFIX);
    validate_safe_path_segment(staging_prefix, "staging_prefix")?;
    validate_safe_path_segment(run_id, "run_id")?;

    Ok(format!("{staging_prefix}/{run_id}/{ADDS_DIRECTORY}"))
}

fn validate_safe_path_segment(value: &str, field_name: &str) -> Result<(), ServiceError> {
    if value.is_empty() || value == "." || value == ".." {
        return Err(ServiceError::InvalidRequest(format!(
            "{field_name} must be a non-empty safe path segment"
        )));
    }

    if value == "_delta_log" || value.contains('/') || value.contains('\\') {
        return Err(ServiceError::InvalidRequest(format!(
            "{field_name} must not contain path separators or target _delta_log"
        )));
    }

    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(ServiceError::InvalidRequest(format!(
            "{field_name} contains unsupported characters"
        )));
    }

    Ok(())
}

fn validate_uuid(value: &str, field_name: &str) -> Result<(), ServiceError> {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return Err(ServiceError::InvalidRequest(format!(
            "{field_name} must be a UUID in canonical format"
        )));
    }

    for (index, byte) in bytes.iter().copied().enumerate() {
        let is_hyphen_position = matches!(index, 8 | 13 | 18 | 23);
        if is_hyphen_position {
            if byte != b'-' {
                return Err(ServiceError::InvalidRequest(format!(
                    "{field_name} must be a UUID in canonical format"
                )));
            }
        } else if !byte.is_ascii_hexdigit() {
            return Err(ServiceError::InvalidRequest(format!(
                "{field_name} must be a UUID in canonical format"
            )));
        }
    }

    Ok(())
}

fn validate_mode(value: &str) -> Result<&str, ServiceError> {
    match value {
        "append" | "overwrite" => Ok(value),
        other => Err(ServiceError::InvalidRequest(format!(
            "unsupported distributed write mode '{other}'"
        ))),
    }
}

fn validate_schema_mode(value: &str) -> Result<&str, ServiceError> {
    match value {
        "merge" | "overwrite" => Ok(value),
        other => Err(ServiceError::InvalidRequest(format!(
            "unsupported distributed write schema_mode '{other}'"
        ))),
    }
}

fn validate_overwrite_scope(value: &str) -> Result<&str, ServiceError> {
    match value {
        "fullTable" | "touchedPartitions" => Ok(value),
        other => Err(ServiceError::InvalidRequest(format!(
            "unsupported distributed write overwrite_scope '{other}'"
        ))),
    }
}

fn validate_supported_append_mode(mode: &str) -> Result<(), ServiceError> {
    if validate_mode(mode)? != "append" {
        return Err(ServiceError::InvalidRequest(
            "distributed write scaffold currently supports append only".to_string(),
        ));
    }

    Ok(())
}

fn validate_requested_partition_columns(
    requested_partition_by: Option<&Vec<String>>,
    partition_columns: &[String],
) -> Result<(), ServiceError> {
    if let Some(requested_partition_by) = requested_partition_by {
        if requested_partition_by != partition_columns {
            return Err(ServiceError::InvalidRequest(format!(
                "distributed append partition columns {:?} do not match table partition columns {:?}",
                requested_partition_by, partition_columns
            )));
        }
    }

    Ok(())
}

fn validate_optional_schema_matches_existing_table(
    schema: Option<&Vec<ColumnDef>>,
    table_state: &DeltaTableState,
) -> Result<(), ServiceError> {
    let Some(schema) = schema else {
        return Ok(());
    };

    let requested_schema = deltalake::kernel::StructType::try_new(delta_columns_from_schema(schema))
        .map_err(|error| {
            ServiceError::InvalidRequest(format!(
                "distributed create-if-missing schema is invalid: {error}"
            ))
        })?;
    let table_schema = table_state
        .metadata()
        .parse_schema()
        .map_err(|error| {
            ServiceError::InvalidRequest(format!(
                "existing table schema could not be parsed for distributed create-if-missing compatibility: {error}"
            ))
        })?;
    if requested_schema != table_schema {
        return Err(ServiceError::InvalidRequest(
            "distributed create-if-missing schema does not match the existing table schema; restage against the existing table".to_string(),
        ));
    }

    Ok(())
}

fn validate_new_table_schema_partition_columns(
    schema: Option<&Vec<ColumnDef>>,
    partition_columns: &[String],
) -> Result<(), ServiceError> {
    let schema = schema.ok_or_else(|| {
        ServiceError::InvalidRequest(
            "distributed create-if-missing writes require a table schema".to_string(),
        )
    })?;

    for partition_column in partition_columns {
        if !schema.iter().any(|column| column.name == *partition_column) {
            return Err(ServiceError::InvalidRequest(format!(
                "partition column '{}' is not present in the table schema",
                partition_column
            )));
        }
    }

    Ok(())
}

fn arrow_schema_from_columns(columns: &[ColumnDef]) -> Schema {
    let fields = columns
        .iter()
        .map(|column| Field::new(&column.name, arrow_type_from_str(&column.data_type), column.nullable))
        .collect::<Vec<_>>();
    Schema::new(fields)
}

fn delta_columns_from_schema(columns: &[ColumnDef]) -> Vec<deltalake::kernel::StructField> {
    arrow_schema_from_columns(columns)
        .fields()
        .iter()
        .map(|field| {
            field
                .as_ref()
                .try_into_kernel()
                .expect("Arrow field to Delta StructField conversion should not fail")
        })
        .collect()
}

fn validate_optional_schema_mode(schema_mode: Option<&str>) -> Result<(), ServiceError> {
    if let Some(schema_mode) = schema_mode {
        validate_schema_mode(schema_mode)?;
        return Err(ServiceError::InvalidRequest(
            "distributed write scaffold does not support schema evolution yet".to_string(),
        ));
    }

    Ok(())
}

#[derive(Default)]
struct ArtifactStats {
    artifact_count: usize,
    added_file_count: i64,
    total_data_file_bytes: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StagedAddArtifactEntry {
    add: Add,
    verified_size: i64,
    #[serde(default)]
    e_tag: Option<String>,
    #[serde(default)]
    object_version: Option<String>,
}

#[derive(Debug, Clone)]
struct VerifiedDataFileMetadata {
    verified_size: i64,
    e_tag: Option<String>,
    object_version: Option<String>,
}

async fn write_add_artifact(
    object_store: &std::sync::Arc<dyn deltalake::logstore::object_store::ObjectStore>,
    adds_prefix: &str,
    adds: Vec<Add>,
) -> Result<ArtifactStats, ServiceError> {
    if adds.is_empty() {
        return Ok(ArtifactStats::default());
    }

    let mut payload = String::new();
    let stats = ArtifactStats {
        artifact_count: 1,
        added_file_count: adds.len() as i64,
        total_data_file_bytes: adds.iter().map(|add| add.size).sum(),
    };

    for add in adds {
        let verified_metadata = inspect_staged_data_file(object_store, &add).await?;
        let artifact_entry = StagedAddArtifactEntry {
            add,
            verified_size: verified_metadata.verified_size,
            e_tag: verified_metadata.e_tag,
            object_version: verified_metadata.object_version,
        };

        payload.push_str(&serde_json::to_string(&artifact_entry).map_err(ServiceError::Json)?);
        payload.push('\n');
    }

    let artifact_path = Path::from(format!("{adds_prefix}/{}.jsonl", Uuid::new_v4()));
    object_store
        .put(&artifact_path, payload.into())
        .await
        .map_err(|error| {
            ServiceError::Internal(format!("failed to write staging artifact: {error}"))
        })?;

    Ok(stats)
}

async fn list_add_artifacts(
    object_store: &std::sync::Arc<dyn deltalake::logstore::object_store::ObjectStore>,
    adds_prefix: &str,
) -> Result<Vec<ObjectMeta>, ServiceError> {
    let prefix = Path::from(adds_prefix.to_string());
    let mut artifacts = object_store
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| {
            ServiceError::Internal(format!("failed to list staging artifacts: {error}"))
        })?;
    artifacts.retain(|artifact| {
        artifact.location.as_ref().ends_with(".jsonl")
            && artifact
                .location
                .parent()
                .map(|parent| parent == prefix)
                .unwrap_or(false)
    });
    artifacts.sort_by(|left, right| left.location.cmp(&right.location));
    Ok(artifacts)
}

async fn read_add_artifact(
    object_store: &std::sync::Arc<dyn deltalake::logstore::object_store::ObjectStore>,
    path: &Path,
) -> Result<Vec<StagedAddArtifactEntry>, ServiceError> {
    let bytes = object_store
        .get(path)
        .await
        .map_err(|error| {
            ServiceError::Internal(format!("failed to read staging artifact: {error}"))
        })?
        .bytes()
        .await
        .map_err(|error| {
            ServiceError::Internal(format!("failed to read staging artifact bytes: {error}"))
        })?;
    let text = String::from_utf8(bytes.to_vec()).map_err(|error| {
        ServiceError::InvalidRequest(format!("staging artifact is not valid UTF-8: {error}"))
    })?;
    let mut entries = Vec::new();
    for (line_number, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }

        let entry = serde_json::from_str::<StagedAddArtifactEntry>(line).map_err(|error| {
            ServiceError::InvalidRequest(format!(
                "invalid staged Add artifact entry at line {}: {error}",
                line_number + 1
            ))
        })?;
        entries.push(entry);
    }

    Ok(entries)
}

fn validate_staged_add(add: &Add, partition_columns: &[String]) -> Result<(), ServiceError> {
    if !is_safe_table_relative_data_path(&add.path) {
        return Err(ServiceError::InvalidRequest(format!(
            "staged Add path '{}' is not a safe table-relative data path",
            add.path
        )));
    }

    if !add.data_change {
        return Err(ServiceError::InvalidRequest(format!(
            "staged Add path '{}' must have data_change=true for append commits",
            add.path
        )));
    }

    if add.partition_values.len() != partition_columns.len() {
        return Err(ServiceError::InvalidRequest(format!(
            "staged Add path '{}' partition value keys {:?} do not match table partition columns {:?}",
            add.path,
            add.partition_values.keys().collect::<Vec<_>>(),
            partition_columns
        )));
    }

    for column in partition_columns {
        if !add.partition_values.contains_key(column) {
            return Err(ServiceError::InvalidRequest(format!(
                "staged Add path '{}' is missing partition value for '{}'",
                add.path, column
            )));
        }
    }

    Ok(())
}

fn validate_staged_add_artifact_entry(
    entry: &StagedAddArtifactEntry,
    partition_columns: &[String],
) -> Result<(), ServiceError> {
    validate_staged_add(&entry.add, partition_columns)?;

    if entry.verified_size != entry.add.size {
        return Err(ServiceError::InvalidRequest(format!(
            "staged Add path '{}' verified size {} does not match Add size {}",
            entry.add.path, entry.verified_size, entry.add.size
        )));
    }

    Ok(())
}

async fn validate_staged_data_file(
    object_store: &std::sync::Arc<dyn deltalake::logstore::object_store::ObjectStore>,
    add: &Add,
) -> Result<(), ServiceError> {
    inspect_staged_data_file(object_store, add).await.map(|_| ())
}

async fn inspect_staged_data_file(
    object_store: &std::sync::Arc<dyn deltalake::logstore::object_store::ObjectStore>,
    add: &Add,
) -> Result<VerifiedDataFileMetadata, ServiceError> {
    let data_path = Path::from(add.path.clone());
    let metadata = object_store.head(&data_path).await.map_err(|error| {
        ServiceError::InvalidRequest(format!(
            "staged Add path '{}' does not reference an accessible data file: {error}",
            add.path
        ))
    })?;
    let object_size = i64::try_from(metadata.size).map_err(|_| {
        ServiceError::InvalidRequest(format!(
            "staged Add path '{}' has object size that exceeds supported range",
            add.path
        ))
    })?;
    if object_size != add.size {
        return Err(ServiceError::InvalidRequest(format!(
            "staged Add path '{}' size {} does not match object size {}",
            add.path, add.size, object_size
        )));
    }

    Ok(VerifiedDataFileMetadata {
        verified_size: object_size,
        e_tag: metadata.e_tag.clone(),
        object_version: metadata.version.clone(),
    })
}

fn is_safe_table_relative_data_path(path: &str) -> bool {
    if path.is_empty() || path.starts_with('/') || path.contains("://") {
        return false;
    }

    let mut segments = path.split('/');
    let Some(first_segment) = segments.next() else {
        return false;
    };
    if first_segment.is_empty() || matches!(first_segment, "." | ".." | "_delta_log" | "_staging") {
        return false;
    }

    segments.all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}

async fn delete_artifacts(
    object_store: &std::sync::Arc<dyn deltalake::logstore::object_store::ObjectStore>,
    paths: Vec<Path>,
) -> Result<(), ServiceError> {
    for path in paths {
        object_store.delete(&path).await.map_err(|error| {
            ServiceError::Internal(format!("failed to delete staging artifact: {error}"))
        })?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::logstore::object_store::memory::InMemory;
    use deltalake::logstore::object_store::ObjectStore;
    use deltalake::logstore::object_store::PutPayload;

    #[test]
    fn staging_adds_prefix_uses_run_id_only() {
        let run_id = "123e4567-e89b-12d3-a456-426614174000";
        let prefix = staging_adds_prefix(None, run_id).expect("safe run id");
        assert_eq!("_staging/123e4567-e89b-12d3-a456-426614174000/adds", prefix);
    }

    #[test]
    fn staging_adds_prefix_allows_custom_safe_prefix() {
        let run_id = "123e4567-e89b-12d3-a456-426614174000";
        let prefix = staging_adds_prefix(Some("staging.tmp"), run_id).expect("safe prefix");
        assert_eq!(
            "staging.tmp/123e4567-e89b-12d3-a456-426614174000/adds",
            prefix
        );
    }

    #[test]
    fn staging_adds_prefix_rejects_path_traversal() {
        assert!(staging_adds_prefix(None, "../run").is_err());
        assert!(staging_adds_prefix(Some("_delta_log"), "run").is_err());
        assert!(staging_adds_prefix(Some("a/b"), "run").is_err());
    }

    #[test]
    fn validate_uuid_accepts_canonical_uuid() {
        assert!(validate_uuid("123e4567-e89b-12d3-a456-426614174000", "run_id").is_ok());
    }

    #[test]
    fn validate_uuid_rejects_non_uuid_run_id() {
        assert!(validate_uuid("run-123", "run_id").is_err());
        assert!(validate_uuid("123e4567e89b12d3a456426614174000", "run_id").is_err());
    }

    #[tokio::test]
    async fn begin_distributed_write_requires_run_id() {
        let body = serde_json::json!({
            "path": "/tmp/table"
        });

        let error = begin_distributed_write(body.to_string().as_bytes())
            .await
            .expect_err("missing run_id should fail");

        assert!(matches!(error, ServiceError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn begin_distributed_write_rejects_non_uuid_run_id() {
        let body = serde_json::json!({
            "path": "/tmp/table",
            "run_id": "run-123"
        });

        let error = begin_distributed_write(body.to_string().as_bytes())
            .await
            .expect_err("invalid run_id should fail");

        assert!(matches!(error, ServiceError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn begin_distributed_write_rejects_unknown_mode() {
        let body = serde_json::json!({
            "path": "/tmp/table",
            "run_id": "123e4567-e89b-12d3-a456-426614174000",
            "mode": "merge"
        });

        let error = begin_distributed_write(body.to_string().as_bytes())
            .await
            .expect_err("unknown mode should fail");

        assert!(matches!(error, ServiceError::InvalidRequest(_)));
    }

    #[test]
    fn staged_add_path_validation_rejects_unsafe_segments() {
        assert!(is_safe_table_relative_data_path("part-000.parquet"));
        assert!(is_safe_table_relative_data_path("region=us/part-000.parquet"));
        assert!(!is_safe_table_relative_data_path(""));
        assert!(!is_safe_table_relative_data_path("/part-000.parquet"));
        assert!(!is_safe_table_relative_data_path("../part-000.parquet"));
        assert!(!is_safe_table_relative_data_path("region=us/../part-000.parquet"));
        assert!(!is_safe_table_relative_data_path("region=us/.."));
        assert!(!is_safe_table_relative_data_path("_delta_log/000.json"));
        assert!(!is_safe_table_relative_data_path("_staging/run/adds/a.jsonl"));
    }

    #[test]
    fn validate_staged_add_rejects_non_data_change_or_extra_partition_keys() {
        let non_data_change = Add {
            path: "part-000.parquet".to_string(),
            size: 3,
            data_change: false,
            ..Default::default()
        };
        assert!(validate_staged_add(&non_data_change, &[]).is_err());

        let extra_partition_keys = Add {
            path: "region=us/part-000.parquet".to_string(),
            size: 3,
            data_change: true,
            partition_values: [(
                "region".to_string(),
                Some("us".to_string()),
            ), (
                "unexpected".to_string(),
                Some("x".to_string()),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        assert!(validate_staged_add(&extra_partition_keys, &["region".to_string()]).is_err());
    }

    #[tokio::test]
    async fn validate_staged_data_file_checks_existence_and_size() {
        let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
        let path = Path::from("part-000.parquet");
        store
            .put(&path, PutPayload::from(vec![1_u8, 2, 3]))
            .await
            .expect("put test object");

        let matching = Add {
            path: "part-000.parquet".to_string(),
            size: 3,
            data_change: true,
            ..Default::default()
        };
        validate_staged_data_file(&store, &matching)
            .await
            .expect("matching file should validate");

        let wrong_size = Add {
            size: 4,
            ..matching.clone()
        };
        assert!(validate_staged_data_file(&store, &wrong_size).await.is_err());

        let missing = Add {
            path: "missing.parquet".to_string(),
            size: 3,
            data_change: true,
            ..Default::default()
        };
        assert!(validate_staged_data_file(&store, &missing).await.is_err());
    }

    #[tokio::test]
    async fn read_add_artifact_ignores_empty_lines() {
        let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
        let path = Path::from("_staging/123e4567-e89b-12d3-a456-426614174000/adds/empty.jsonl");
        store
            .put(&path, PutPayload::from("\n  \n".to_string()))
            .await
            .expect("put empty artifact");

        let adds = read_add_artifact(&store, &path).await.expect("read artifact");
        assert!(adds.is_empty());
    }

    #[tokio::test]
    async fn list_add_artifacts_only_returns_immediate_jsonl_children() {
        let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
        let prefix = "_staging/123e4567-e89b-12d3-a456-426614174000/adds";
        let valid = Path::from(format!("{prefix}/a.jsonl"));
        let nested = Path::from(format!("{prefix}/nested/b.jsonl"));
        let other_extension = Path::from(format!("{prefix}/c.txt"));

        store
            .put(&valid, PutPayload::from("{}\n".to_string()))
            .await
            .expect("put immediate jsonl artifact");
        store
            .put(&nested, PutPayload::from("{}\n".to_string()))
            .await
            .expect("put nested jsonl artifact");
        store
            .put(&other_extension, PutPayload::from("{}\n".to_string()))
            .await
            .expect("put non-jsonl artifact");

        let artifacts = list_add_artifacts(&store, prefix)
            .await
            .expect("list artifacts");
        assert_eq!(1, artifacts.len());
        assert_eq!(valid, artifacts[0].location);
    }
}
