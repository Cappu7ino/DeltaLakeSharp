// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Write-path handlers for transport-neutral V3 operations.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use tracing::{debug, info};

use super::helpers::{
    open_delta_table, path_to_url, storage_options, success_response,
    success_response_with_result,
};
use super::request::{
    arrow_type_from_str, CreateTableCommand, ExecuteDmlCommand, WriteCommand,
    UpgradeProtocolCommand,
};
use crate::error::ServiceError;

// ========================================================================== //
//  Create table
// ========================================================================== //

/// Handles table creation.
///
/// Creates a new Delta table at the specified path with the given schema.
/// Matches V2 behavior: writes an empty batch with the correct schema to
/// initialise the Delta log.
pub async fn handle_create_table(body: &[u8]) -> Result<serde_json::Value, ServiceError> {
    let cmd: CreateTableCommand = serde_json::from_slice(body)?;
    info!(path = %cmd.path, columns = cmd.schema.len(), "Creating Delta table");

    // Build Arrow schema from the column definitions.
    let fields: Vec<Field> = cmd
        .schema
        .iter()
        .map(|col| {
            let dt = arrow_type_from_str(&col.data_type);
            Field::new(&col.name, dt, true) // nullable=true matches V2
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));

    // Create the Delta table at the given path.
    // For local filesystem paths, ensure the directory exists first —
    // delta-rs requires the directory to be present before try_from_url.
    if !cmd.path.contains("://") {
        let path = std::path::Path::new(&cmd.path);
        if !path.exists() {
            std::fs::create_dir_all(path).map_err(|e| {
                ServiceError::InvalidRequest(format!(
                    "Failed to create directory '{}': {e}",
                    cmd.path
                ))
            })?;
        }
    }

    let url = path_to_url(&cmd.path)?;
    let opts = storage_options(
        cmd.storage_account.as_deref(),
        cmd.sas_token.as_deref(),
    );

    let table = deltalake::DeltaTable::try_from_url(url)
        .await
        .map_err(ServiceError::Delta)?;

    // Convert Arrow schema fields to delta-rs StructField via TryIntoKernel.
    let delta_columns: Vec<deltalake::kernel::StructField> = schema
        .fields()
        .iter()
        .map(|f| {
            f.as_ref()
                .try_into_kernel()
                .expect("Arrow field to Delta StructField conversion should not fail")
        })
        .collect();

    let mut create_builder = table.create().with_columns(delta_columns);

    if let Some(partition_cols) = &cmd.partition_by {
        if !partition_cols.is_empty() {
            create_builder = create_builder.with_partition_columns(partition_cols.clone());
        }
    }

    if let Some(config) = &cmd.configuration {
        let config_pairs: Vec<(String, Option<String>)> = config
            .iter()
            .map(|(k, v)| (k.clone(), Some(v.clone())))
            .collect();
        create_builder = create_builder.with_configuration(config_pairs);
    }

    if !opts.is_empty() {
        create_builder = create_builder.with_storage_options(opts);
    }

    create_builder.await.map_err(ServiceError::Delta)?;

    let msg = format!("Delta table created at {}.", cmd.path);
    info!("{}", msg);
    Ok(success_response(&msg))
}

// ========================================================================== //
//  Execute DML
// ========================================================================== //

/// Handles DML execution.
///
/// Opens the Delta table and executes the DML statement. DELETE uses
/// delta-rs's native `DeleteBuilder` and UPDATE uses `UpdateBuilder`.
/// MERGE via SQL is still not supported and returns an error, matching the
/// current V3 behavior until the SQL merge path is implemented separately.
pub async fn handle_execute_dml(body: &[u8]) -> Result<serde_json::Value, ServiceError> {
    let cmd: ExecuteDmlCommand = serde_json::from_slice(body)?;
    info!(sql = %cmd.sql, table = %cmd.table_name, "Executing DML");

    let sql_upper = cmd.sql.trim().to_uppercase();

    if sql_upper.starts_with("DELETE") {
        execute_delete(&cmd).await
    } else if sql_upper.starts_with("UPDATE") {
        execute_update(&cmd).await
    } else {
        Err(ServiceError::InvalidRequest(format!(
            "Unsupported DML statement. Only DELETE and UPDATE are supported. Got: {}",
            &cmd.sql[..cmd.sql.len().min(80)]
        )))
    }
}

/// Execute a DELETE statement using delta-rs's native `DeleteBuilder`.
///
/// Parses the SQL to extract the optional WHERE predicate, then calls
/// `table.delete().with_predicate(...)`. This avoids the DataFusion
/// `TableProvider` path which does not support DELETE for Delta tables.
async fn execute_delete(cmd: &ExecuteDmlCommand) -> Result<serde_json::Value, ServiceError> {
    let table = open_delta_table(
        &cmd.table_path,
        cmd.storage_account.as_deref(),
        cmd.sas_token.as_deref(),
        None,
    )
    .await?;

    // Extract the WHERE clause predicate from the SQL string.
    let predicate = extract_where_clause(&cmd.sql);

    let mut builder = table.delete();
    if let Some(pred) = &predicate {
        debug!(predicate = %pred, "DELETE with predicate");
        builder = builder.with_predicate(pred.as_str());
    } else {
        debug!("DELETE all rows (no predicate)");
    }

    let (_table, metrics) = builder.await?;

    // Return metrics as a single JSON row (matching V2 envelope format).
    let metrics_row = serde_json::json!({
        "num_added_files": metrics.num_added_files,
        "num_removed_files": metrics.num_removed_files,
        "num_deleted_rows": metrics.num_deleted_rows,
        "num_copied_rows": metrics.num_copied_rows,
        "execution_time_ms": metrics.execution_time_ms,
        "scan_time_ms": metrics.scan_time_ms,
        "rewrite_time_ms": metrics.rewrite_time_ms,
    });

    info!(
        deleted_rows = metrics.num_deleted_rows,
        "DELETE executed successfully"
    );
    Ok(success_response_with_result(
        "DML executed successfully.",
        vec![metrics_row],
    ))
}

/// Extract the WHERE clause from a DELETE SQL statement.
///
/// Handles forms like:
/// - `DELETE FROM tbl`  → None (delete all)
/// - `DELETE FROM tbl WHERE id > 1`  → Some("id > 1")
/// - `DELETE FROM tbl WHERE true`  → Some("true")
///
/// The matching is case-insensitive for the WHERE keyword.
fn extract_where_clause(sql: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    if let Some(idx) = upper.find(" WHERE ") {
        let predicate = sql[idx + " WHERE ".len()..].trim();
        if predicate.is_empty() {
            None
        } else {
            Some(predicate.to_string())
        }
    } else {
        None
    }
}

#[derive(Debug, Clone)]
struct ParsedUpdate {
    assignments: Vec<(String, String)>,
    predicate: Option<String>,
}

/// Execute an UPDATE statement using delta-rs's native `UpdateBuilder`.
async fn execute_update(cmd: &ExecuteDmlCommand) -> Result<serde_json::Value, ServiceError> {
    let parsed = parse_update_statement(&cmd.sql)?;
    let table = open_delta_table(
        &cmd.table_path,
        cmd.storage_account.as_deref(),
        cmd.sas_token.as_deref(),
        None,
    )
    .await?;

    let mut builder = table.update();
    if let Some(pred) = &parsed.predicate {
        debug!(predicate = %pred, "UPDATE with predicate");
        builder = builder.with_predicate(pred.as_str());
    } else {
        debug!("UPDATE all rows (no predicate)");
    }

    for (column, expression) in &parsed.assignments {
        builder = builder.with_update(column.as_str(), expression.as_str());
    }

    let (_table, metrics) = builder.await?;

    let metrics_row = serde_json::json!({
        "num_added_files": metrics.num_added_files,
        "num_removed_files": metrics.num_removed_files,
        "num_updated_rows": metrics.num_updated_rows,
        "num_copied_rows": metrics.num_copied_rows,
        "execution_time_ms": metrics.execution_time_ms,
        "scan_time_ms": metrics.scan_time_ms,
    });

    info!(updated_rows = metrics.num_updated_rows, "UPDATE executed successfully");
    Ok(success_response_with_result(
        "DML executed successfully.",
        vec![metrics_row],
    ))
}

/// Parses the subset of SQL UPDATE syntax supported by the current V3 service.
fn parse_update_statement(sql: &str) -> Result<ParsedUpdate, ServiceError> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    let set_idx = upper.find(" SET ").ok_or_else(|| {
        ServiceError::InvalidRequest("UPDATE statement must contain SET clause.".into())
    })?;

    let after_set = &trimmed[set_idx + " SET ".len()..];
    let after_set_upper = after_set.to_uppercase();
    let (assignments_part, predicate) = if let Some(where_idx) = after_set_upper.find(" WHERE ") {
        (
            &after_set[..where_idx],
            Some(after_set[where_idx + " WHERE ".len()..].trim().to_string()),
        )
    } else {
        (after_set, None)
    };

    let assignments = split_assignments(assignments_part)?;
    if assignments.is_empty() {
        return Err(ServiceError::InvalidRequest(
            "UPDATE statement must contain at least one assignment.".into(),
        ));
    }

    Ok(ParsedUpdate {
        assignments,
        predicate,
    })
}

fn split_assignments(input: &str) -> Result<Vec<(String, String)>, ServiceError> {
    let mut assignments = Vec::new();
    let mut current = String::new();
    let mut in_single_quote = false;

    for ch in input.chars() {
        match ch {
            '\'' => {
                in_single_quote = !in_single_quote;
                current.push(ch);
            }
            ',' if !in_single_quote => {
                push_assignment(&mut assignments, &current)?;
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    push_assignment(&mut assignments, &current)?;
    Ok(assignments)
}

fn push_assignment(
    assignments: &mut Vec<(String, String)>,
    assignment: &str,
) -> Result<(), ServiceError> {
    let trimmed = assignment.trim();
    if trimmed.is_empty() {
        return Ok(());
    }

    let (column, expression) = trimmed.split_once('=').ok_or_else(|| {
        ServiceError::InvalidRequest(format!(
            "Invalid UPDATE assignment: '{trimmed}'. Expected 'column = expression'."
        ))
    })?;

    let column = column.trim();
    let expression = expression.trim();
    if column.is_empty() || expression.is_empty() {
        return Err(ServiceError::InvalidRequest(format!(
            "Invalid UPDATE assignment: '{trimmed}'. Expected non-empty column and expression."
        )));
    }

    assignments.push((column.to_string(), expression.to_string()));
    Ok(())
}

// ========================================================================== //
//  Upgrade protocol
// ========================================================================== //

/// Maps a camelCase feature name (from the C# client) to a delta-rs `TableFeatures` variant.
fn lookup_table_feature(
    name: &str,
) -> Result<deltalake::kernel::TableFeatures, ServiceError> {
    match name {
        "appendOnly" => Ok(deltalake::kernel::TableFeatures::AppendOnly),
        "changeDataFeed" => Ok(deltalake::kernel::TableFeatures::ChangeDataFeed),
        "checkConstraints" => Ok(deltalake::kernel::TableFeatures::CheckConstraints),
        "columnMapping" => Ok(deltalake::kernel::TableFeatures::ColumnMapping),
        "deletionVectors" => Ok(deltalake::kernel::TableFeatures::DeletionVectors),
        "domainMetadata" => Ok(deltalake::kernel::TableFeatures::DomainMetadata),
        "generatedColumns" => Ok(deltalake::kernel::TableFeatures::GeneratedColumns),
        "identityColumns" => Ok(deltalake::kernel::TableFeatures::IdentityColumns),
        "invariants" => Ok(deltalake::kernel::TableFeatures::Invariants),
        "rowTracking" => Ok(deltalake::kernel::TableFeatures::RowTracking),
        "timestampNtz" => Ok(deltalake::kernel::TableFeatures::TimestampWithoutTimezone),
        "v2Checkpoint" => Ok(deltalake::kernel::TableFeatures::V2Checkpoint),
        _ => Err(ServiceError::InvalidRequest(format!(
            "Unknown table feature: '{name}'"
        ))),
    }
}

/// Returns companion table properties that must be set when enabling a feature.
fn feature_companion_properties(
    feature: &deltalake::kernel::TableFeatures,
) -> Option<(&'static str, &'static str)> {
    match feature {
        deltalake::kernel::TableFeatures::AppendOnly => {
            Some(("delta.appendOnly", "true"))
        }
        deltalake::kernel::TableFeatures::ChangeDataFeed => {
            Some(("delta.enableChangeDataFeed", "true"))
        }
        deltalake::kernel::TableFeatures::ColumnMapping => {
            Some(("delta.columnMapping.mode", "name"))
        }
        deltalake::kernel::TableFeatures::DeletionVectors => {
            Some(("delta.enableDeletionVectors", "true"))
        }
        deltalake::kernel::TableFeatures::RowTracking => {
            Some(("delta.enableRowTracking", "true"))
        }
        _ => None,
    }
}

/// Handles protocol upgrades.
///
/// Enables table features and bumps protocol versions:
/// - If features are requested, uses `add_feature()` (which auto-bumps versions).
/// - If no features, does a simple version bump via table properties.
/// - Sets companion properties for features that need them.
pub async fn handle_upgrade_protocol(body: &[u8]) -> Result<serde_json::Value, ServiceError> {
    let cmd: UpgradeProtocolCommand = serde_json::from_slice(body)?;
    info!(
        path = %cmd.path,
        reader_version = cmd.reader_version,
        writer_version = cmd.writer_version,
        "Upgrading Delta table protocol"
    );

    let mut table = open_delta_table(
        &cmd.path,
        cmd.storage_account.as_deref(),
        cmd.sas_token.as_deref(),
        None,
    )
    .await?;

    // Collect all requested features (dedup).
    let mut all_features = Vec::new();
    let reader_features = cmd.reader_features.unwrap_or_default();
    let writer_features = cmd.writer_features.unwrap_or_default();

    let mut seen = std::collections::HashSet::new();
    for name in reader_features.iter().chain(writer_features.iter()) {
        if seen.insert(name.as_str()) {
            let feature = lookup_table_feature(name)?;
            all_features.push(feature);
        }
    }

    if !all_features.is_empty() {
        // Add features — this auto-bumps protocol versions as needed.
        for feature in &all_features {
            table = table
                .add_feature()
                .with_feature(feature.clone())
                .with_allow_protocol_versions_increase(true)
                .await
                .map_err(ServiceError::Delta)?;
        }

        // Set companion table properties for features that require them.
        let mut companion_props: HashMap<String, String> = HashMap::new();
        for feature in &all_features {
            if let Some((key, value)) = feature_companion_properties(feature) {
                companion_props.insert(key.to_string(), value.to_string());
            }
        }
        if !companion_props.is_empty() {
            let set_props_builder = table.set_tbl_properties().with_properties(companion_props);
            table = set_props_builder.await.map_err(ServiceError::Delta)?;
        }
    } else {
        // Simple version bump (no features) — set via table properties.
        let mut props = HashMap::new();
        props.insert(
            "delta.minReaderVersion".to_string(),
            cmd.reader_version.to_string(),
        );
        props.insert(
            "delta.minWriterVersion".to_string(),
            cmd.writer_version.to_string(),
        );
        table = table
            .set_tbl_properties()
            .with_properties(props)
            .await
            .map_err(ServiceError::Delta)?;
    }

    // Read back the resulting protocol.
    let protocol = table
        .snapshot()
        .map_err(ServiceError::Delta)?
        .snapshot()
        .protocol();

    let mut result_obj = serde_json::json!({
        "minReaderVersion": protocol.min_reader_version(),
        "minWriterVersion": protocol.min_writer_version(),
    });

    if let Some(rf) = protocol.reader_features() {
        let features: Vec<String> = rf.iter().map(|f| format!("{f:?}")).collect();
        result_obj["readerFeatures"] = serde_json::json!(features);
    }
    if let Some(wf) = protocol.writer_features() {
        let features: Vec<String> = wf.iter().map(|f| format!("{f:?}")).collect();
        result_obj["writerFeatures"] = serde_json::json!(features);
    }

    info!(
        path = %cmd.path,
        reader = protocol.min_reader_version(),
        writer = protocol.min_writer_version(),
        "Protocol upgraded"
    );

    Ok(success_response_with_result(
        "Protocol upgraded.",
        vec![result_obj],
    ))
}

// ========================================================================== //
//  Write / merge batch operations
// ========================================================================== //

/// Handles a write (insert/overwrite) operation.
async fn write_batches(
    cmd: WriteCommand,
    batches: Vec<RecordBatch>,
) -> Result<serde_json::Value, ServiceError> {
    info!(
        path = %cmd.path,
        mode = %cmd.mode,
        batch_count = batches.len(),
        "Writing batches to Delta table"
    );

    let table = open_delta_table(
        &cmd.path,
        cmd.storage_account.as_deref(),
        cmd.sas_token.as_deref(),
        None,
    )
    .await?;

    let save_mode = match cmd.mode.as_str() {
        "append" => deltalake::protocol::SaveMode::Append,
        _ => deltalake::protocol::SaveMode::Overwrite,
    };

    let mut write_builder = table.write(batches).with_save_mode(save_mode);

    if let Some(partition_cols) = &cmd.partition_by {
        if !partition_cols.is_empty() {
            write_builder = write_builder.with_partition_columns(partition_cols.clone());
        }
    }

    if let Some(config) = &cmd.configuration {
        let config_pairs: Vec<(String, Option<String>)> = config
            .iter()
            .map(|(k, v)| (k.clone(), Some(v.clone())))
            .collect();
        write_builder = write_builder.with_configuration(config_pairs);
    }

    // write() consumes self and returns DeltaTable.
    let _table = write_builder.await.map_err(ServiceError::Delta)?;

    let msg = format!("Wrote batches to {}.", cmd.path);
    info!("{}", msg);
    Ok(success_response(&msg))
}

/// Transport-neutral insert entrypoint used by the native in-process backend.
pub async fn handle_native_insert(
    cmd: WriteCommand,
    batches: Vec<RecordBatch>,
) -> Result<serde_json::Value, ServiceError> {
    write_batches(cmd, batches).await
}

/// Transport-neutral merge entrypoint used by the native in-process backend.
pub async fn handle_native_merge(
    cmd: WriteCommand,
    batches: Vec<RecordBatch>,
) -> Result<serde_json::Value, ServiceError> {
    merge_batches(cmd, batches).await
}

/// Handles a merge operation.
///
/// The incoming record batches are the merge source data. They are
/// registered as a DataFrame in DataFusion, then merged into the target
/// Delta table using the programmatic merge API.
async fn merge_batches(
    cmd: WriteCommand,
    batches: Vec<RecordBatch>,
) -> Result<serde_json::Value, ServiceError> {
    let predicate = cmd.predicate.as_deref().ok_or_else(|| {
        ServiceError::InvalidRequest("Merge operation requires a 'predicate' field".into())
    })?;
    let source_alias = cmd.source_alias.as_deref().unwrap_or("source");
    let target_alias = cmd.target_alias.as_deref().unwrap_or("target");

    info!(
        path = %cmd.path,
        predicate = %predicate,
        batch_count = batches.len(),
        "Merging into Delta table"
    );

    // Open the target Delta table.
    let table = open_delta_table(
        &cmd.path,
        cmd.storage_account.as_deref(),
        cmd.sas_token.as_deref(),
        None,
    )
    .await?;

    // Convert source batches into a DataFrame via MemTable.
    let schema: SchemaRef = batches[0].schema();
    let source_table = MemTable::try_new(Arc::clone(&schema), vec![batches])
        .map_err(ServiceError::DataFusion)?;
    let ctx = SessionContext::new();
    ctx.register_table("_merge_source", Arc::new(source_table))
        .map_err(ServiceError::DataFusion)?;
    let source_df = ctx
        .sql("SELECT * FROM _merge_source")
        .await
        .map_err(ServiceError::DataFusion)?;

    // Build the merge operation.
    let mut merger = table
        .merge(source_df, predicate)
        .with_source_alias(source_alias)
        .with_target_alias(target_alias);

    // WHEN MATCHED clauses.
    if cmd.when_matched_update_all == Some(true) {
        // No update_all() in delta-rs 0.31 — iterate source columns explicitly.
        let field_names: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        merger = merger
            .when_matched_update(|mut update| {
                for name in &field_names {
                    update = update.update(
                        name.as_str(),
                        format!("{source_alias}.{name}"),
                    );
                }
                update
            })
            .map_err(ServiceError::Delta)?;
    } else if let Some(update_set) = &cmd.when_matched_update_set {
        merger = merger
            .when_matched_update(|mut update| {
                for (col, expr) in update_set {
                    update = update.update(col.as_str(), expr.as_str());
                }
                update
            })
            .map_err(ServiceError::Delta)?;
    }

    if let Some(delete_predicate) = &cmd.when_matched_delete_predicate {
        merger = merger
            .when_matched_delete(|delete| delete.predicate(delete_predicate.as_str()))
            .map_err(ServiceError::Delta)?;
    }

    // WHEN NOT MATCHED clauses.
    if cmd.when_not_matched_insert_all == Some(true) {
        // No insert_all() in delta-rs 0.31 — iterate source columns explicitly.
        let field_names: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        merger = merger
            .when_not_matched_insert(|mut insert| {
                for name in &field_names {
                    insert = insert.set(
                        name.as_str(),
                        format!("{source_alias}.{name}"),
                    );
                }
                insert
            })
            .map_err(ServiceError::Delta)?;
    } else if let Some(insert_set) = &cmd.when_not_matched_insert_set {
        merger = merger
            .when_not_matched_insert(|mut insert| {
                for (col, expr) in insert_set {
                    insert = insert.set(col.as_str(), expr.as_str());
                }
                insert
            })
            .map_err(ServiceError::Delta)?;
    }

    // WHEN NOT MATCHED BY SOURCE clauses.
    if let Some(delete_predicate) = &cmd.when_not_matched_by_source_delete_predicate {
        merger = merger
            .when_not_matched_by_source_delete(|delete| {
                delete.predicate(delete_predicate.as_str())
            })
            .map_err(ServiceError::Delta)?;
    }

    if let Some(update_set) = &cmd.when_not_matched_by_source_update_set {
        merger = merger
            .when_not_matched_by_source_update(|mut update| {
                if let Some(pred) = &cmd.when_not_matched_by_source_update_predicate {
                    update = update.predicate(pred.as_str());
                }
                for (col, expr) in update_set {
                    update = update.update(col.as_str(), expr.as_str());
                }
                update
            })
            .map_err(ServiceError::Delta)?;
    }

    // Execute the merge.
    let (_, metrics) = merger.await.map_err(ServiceError::Delta)?;

    let metrics_json = serde_json::json!({
        "num_source_rows": metrics.num_source_rows,
        "num_target_rows_inserted": metrics.num_target_rows_inserted,
        "num_target_rows_updated": metrics.num_target_rows_updated,
        "num_target_rows_deleted": metrics.num_target_rows_deleted,
        "num_target_rows_copied": metrics.num_target_rows_copied,
        "num_output_rows": metrics.num_output_rows,
        "num_target_files_added": metrics.num_target_files_added,
        "num_target_files_removed": metrics.num_target_files_removed,
    });

    info!(
        path = %cmd.path,
        source_rows = metrics.num_source_rows,
        inserted = metrics.num_target_rows_inserted,
        updated = metrics.num_target_rows_updated,
        deleted = metrics.num_target_rows_deleted,
        "Merge completed"
    );

    Ok(success_response_with_result(
        "Merge completed.",
        vec![metrics_json],
    ))
}

// ========================================================================== //
//  Tests
// ========================================================================== //

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use url::Url;

    /// Creates a temp directory and returns (path_string, _temp_dir_guard).
    fn temp_table_path() -> (String, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("failed to create temp dir");
        let table_path = tmp.path().join("test_table");
        std::fs::create_dir(&table_path).expect("failed to create table dir");
        (table_path.to_str().unwrap().to_string(), tmp)
    }

    /// Creates a simple test Delta table with schema: id (Int32), name (Utf8).
    async fn create_simple_test_table(path: &str) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let url = Url::from_file_path(path).unwrap();
        let table = deltalake::DeltaTable::try_from_url(url)
            .await
            .expect("try_from_url");

        let delta_columns: Vec<deltalake::kernel::StructField> = schema
            .fields()
            .iter()
            .map(|f| {
                f.as_ref()
                    .try_into_kernel()
                    .unwrap()
            })
            .collect();

        let table = table
            .create()
            .with_columns(delta_columns)
            .await
            .expect("create");

        table.write(vec![batch]).await.expect("write");
    }

    // ------------------------------------------------------------------ //
    //  create_table tests
    // ------------------------------------------------------------------ //

    #[tokio::test]
    async fn create_table_basic() {
        let (path, _tmp) = temp_table_path();
        let body = serde_json::json!({
            "path": path,
            "schema": [
                {"name": "id", "type": "int32"},
                {"name": "value", "type": "string"}
            ]
        });
        let body_bytes = serde_json::to_vec(&body).unwrap();

        let result = handle_create_table(&body_bytes).await.unwrap();
        assert_eq!(result["success"], true);
        assert!(result["message"].as_str().unwrap().contains("created"));

        // Verify the table exists and has the correct schema.
        let table = open_delta_table(&path, None, None, None).await.unwrap();
        let schema = table.snapshot().unwrap().snapshot().arrow_schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "value");
    }

    #[tokio::test]
    async fn create_table_with_partition() {
        let (path, _tmp) = temp_table_path();
        let body = serde_json::json!({
            "path": path,
            "schema": [
                {"name": "id", "type": "int32"},
                {"name": "region", "type": "string"}
            ],
            "partition_by": ["region"]
        });
        let body_bytes = serde_json::to_vec(&body).unwrap();

        let result = handle_create_table(&body_bytes).await.unwrap();
        assert_eq!(result["success"], true);

        // Verify the table exists.
        let table = open_delta_table(&path, None, None, None).await.unwrap();
        let schema = table.snapshot().unwrap().snapshot().arrow_schema();
        assert_eq!(schema.fields().len(), 2);
    }

    #[tokio::test]
    async fn create_table_all_types() {
        let (path, _tmp) = temp_table_path();
        let body = serde_json::json!({
            "path": path,
            "schema": [
                {"name": "c_string", "type": "string"},
                {"name": "c_int32", "type": "int32"},
                {"name": "c_int64", "type": "int64"},
                {"name": "c_int16", "type": "int16"},
                {"name": "c_int8", "type": "int8"},
                {"name": "c_float", "type": "float32"},
                {"name": "c_double", "type": "float64"},
                {"name": "c_bool", "type": "boolean"},
                {"name": "c_date", "type": "date32"},
                {"name": "c_ts", "type": "timestamp"},
                {"name": "c_ts_ntz", "type": "timestamp_ntz"},
                {"name": "c_binary", "type": "binary"}
            ]
        });
        let body_bytes = serde_json::to_vec(&body).unwrap();
        let result = handle_create_table(&body_bytes).await.unwrap();
        assert_eq!(result["success"], true);

        let table = open_delta_table(&path, None, None, None).await.unwrap();
        let schema = table.snapshot().unwrap().snapshot().arrow_schema();
        assert_eq!(schema.fields().len(), 12);
    }

    // ------------------------------------------------------------------ //
    //  execute_dml tests
    // ------------------------------------------------------------------ //

    #[tokio::test]
    async fn execute_dml_delete_all() {
        let (path, _tmp) = temp_table_path();
        create_simple_test_table(&path).await;

        let body = serde_json::json!({
            "sql": "DELETE FROM test_table WHERE true",
            "table_path": path,
            "table_name": "test_table",
        });
        let body_bytes = serde_json::to_vec(&body).unwrap();

        let result = handle_execute_dml(&body_bytes).await.unwrap();
        assert_eq!(result["success"], true);
        assert_eq!(result["message"], "DML executed successfully.");
    }

    #[tokio::test]
    async fn execute_dml_delete_with_predicate() {
        let (path, _tmp) = temp_table_path();
        create_simple_test_table(&path).await;

        let body = serde_json::json!({
            "sql": "DELETE FROM test_table WHERE id > 1",
            "table_path": path,
            "table_name": "test_table",
        });
        let body_bytes = serde_json::to_vec(&body).unwrap();

        let result = handle_execute_dml(&body_bytes).await.unwrap();
        assert_eq!(result["success"], true);

        // Verify only 1 row remains.
        let table = open_delta_table(&path, None, None, None).await.unwrap();
        let ctx = SessionContext::new();
        let provider = table.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let df = ctx.sql("SELECT COUNT(*) AS cnt FROM t").await.unwrap();
        let batches = df.collect().await.unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn execute_dml_update_with_predicate() {
        let (path, _tmp) = temp_table_path();
        create_simple_test_table(&path).await;

        let body = serde_json::json!({
            "sql": "UPDATE test_table SET name = 'updated' WHERE id <= 2",
            "table_path": path,
            "table_name": "test_table",
        });
        let body_bytes = serde_json::to_vec(&body).unwrap();

        let result = handle_execute_dml(&body_bytes).await.unwrap();
        assert_eq!(result["success"], true);

        let table = open_delta_table(&path, None, None, None).await.unwrap();
        let ctx = SessionContext::new();
        let provider = table.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let df = ctx.sql("SELECT id, name FROM t ORDER BY id").await.unwrap();
        let batches = df.collect().await.unwrap();

        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        let values = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .map(|arr| vec![arr.value(0), arr.value(1), arr.value(2)])
            .unwrap_or_else(|| {
                let arr = batches[0]
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();
                vec![arr.value(0), arr.value(1), arr.value(2)]
            });

        assert_eq!(1, ids.value(0));
        assert_eq!(2, ids.value(1));
        assert_eq!(3, ids.value(2));
        assert_eq!(vec!["updated", "updated", "c"], values);
    }

    // ------------------------------------------------------------------ //
    //  upgrade_protocol tests
    // ------------------------------------------------------------------ //

    #[tokio::test]
    async fn upgrade_protocol_simple_version_bump() {
        let (path, _tmp) = temp_table_path();
        // Create a basic table first.
        let body = serde_json::json!({
            "path": path,
            "schema": [{"name": "id", "type": "int32"}]
        });
        handle_create_table(&serde_json::to_vec(&body).unwrap())
            .await
            .unwrap();

        // Upgrade protocol versions.
        let upgrade_body = serde_json::json!({
            "path": path,
            "reader_version": 2,
            "writer_version": 5,
        });
        let result =
            handle_upgrade_protocol(&serde_json::to_vec(&upgrade_body).unwrap())
                .await
                .unwrap();

        assert_eq!(result["success"], true);
        assert_eq!(result["message"], "Protocol upgraded.");
        let proto_result = &result["result"][0];
        assert!(proto_result["minReaderVersion"].as_i64().unwrap() >= 2);
        assert!(proto_result["minWriterVersion"].as_i64().unwrap() >= 5);
    }

    #[tokio::test]
    async fn upgrade_protocol_with_features() {
        let (path, _tmp) = temp_table_path();
        let body = serde_json::json!({
            "path": path,
            "schema": [{"name": "id", "type": "int32"}]
        });
        handle_create_table(&serde_json::to_vec(&body).unwrap())
            .await
            .unwrap();

        let upgrade_body = serde_json::json!({
            "path": path,
            "reader_version": 3,
            "writer_version": 7,
            "writer_features": ["changeDataFeed"],
        });
        let result =
            handle_upgrade_protocol(&serde_json::to_vec(&upgrade_body).unwrap())
                .await
                .unwrap();

        assert_eq!(result["success"], true);
        let proto_result = &result["result"][0];
        assert!(proto_result["minWriterVersion"].as_i64().unwrap() >= 7);
    }

    #[tokio::test]
    async fn upgrade_protocol_unknown_feature_returns_error() {
        let (path, _tmp) = temp_table_path();
        let body = serde_json::json!({
            "path": path,
            "schema": [{"name": "id", "type": "int32"}]
        });
        handle_create_table(&serde_json::to_vec(&body).unwrap())
            .await
            .unwrap();

        let upgrade_body = serde_json::json!({
            "path": path,
            "reader_version": 3,
            "writer_version": 7,
            "writer_features": ["nonExistentFeature"],
        });
        let result =
            handle_upgrade_protocol(&serde_json::to_vec(&upgrade_body).unwrap()).await;
        assert!(result.is_err());
    }

    // ------------------------------------------------------------------ //
    //  Write tests
    // ------------------------------------------------------------------ //

    #[tokio::test]
    async fn do_put_write_overwrite() {
        let (path, _tmp) = temp_table_path();
        create_simple_test_table(&path).await;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();

        let cmd = WriteCommand {
            path: path.clone(),
            mode: "overwrite".to_string(),
            operation: "write".to_string(),
            storage_account: None,
            sas_token: None,
            configuration: None,
            partition_by: None,
            predicate: None,
            source_alias: None,
            target_alias: None,
            when_matched_update_all: None,
            when_matched_update_set: None,
            when_matched_delete_predicate: None,
            when_not_matched_insert_all: None,
            when_not_matched_insert_set: None,
            when_not_matched_by_source_delete_predicate: None,
            when_not_matched_by_source_update_set: None,
            when_not_matched_by_source_update_predicate: None,
        };

        let result = write_batches(cmd, vec![batch]).await.unwrap();
        assert_eq!(result["success"], true);

        // Verify overwrite: should have exactly 2 rows.
        let table = open_delta_table(&path, None, None, None).await.unwrap();
        let ctx = SessionContext::new();
        let provider = table.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let df = ctx.sql("SELECT COUNT(*) AS cnt FROM t").await.unwrap();
        let batches = df.collect().await.unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn do_put_write_append() {
        let (path, _tmp) = temp_table_path();
        create_simple_test_table(&path).await; // 3 rows

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![4, 5])),
                Arc::new(StringArray::from(vec!["d", "e"])),
            ],
        )
        .unwrap();

        let cmd = WriteCommand {
            path: path.clone(),
            mode: "append".to_string(),
            operation: "write".to_string(),
            storage_account: None,
            sas_token: None,
            configuration: None,
            partition_by: None,
            predicate: None,
            source_alias: None,
            target_alias: None,
            when_matched_update_all: None,
            when_matched_update_set: None,
            when_matched_delete_predicate: None,
            when_not_matched_insert_all: None,
            when_not_matched_insert_set: None,
            when_not_matched_by_source_delete_predicate: None,
            when_not_matched_by_source_update_set: None,
            when_not_matched_by_source_update_predicate: None,
        };

        let result = write_batches(cmd, vec![batch]).await.unwrap();
        assert_eq!(result["success"], true);

        // Verify append: should have 3 + 2 = 5 rows.
        let table = open_delta_table(&path, None, None, None).await.unwrap();
        let ctx = SessionContext::new();
        let provider = table.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let df = ctx.sql("SELECT COUNT(*) AS cnt FROM t").await.unwrap();
        let batches = df.collect().await.unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 5);
    }

    // ------------------------------------------------------------------ //
    //  Merge tests
    // ------------------------------------------------------------------ //

    #[tokio::test]
    async fn do_put_merge_upsert_all() {
        let (path, _tmp) = temp_table_path();
        create_simple_test_table(&path).await; // rows: (1,a), (2,b), (3,c)

        // Source: (2, "B_updated"), (4, "d_new")
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let source_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![2, 4])),
                Arc::new(StringArray::from(vec!["B_updated", "d_new"])),
            ],
        )
        .unwrap();

        let cmd = WriteCommand {
            path: path.clone(),
            mode: "overwrite".to_string(),
            operation: "merge".to_string(),
            storage_account: None,
            sas_token: None,
            configuration: None,
            partition_by: None,
            predicate: Some("target.id = source.id".to_string()),
            source_alias: Some("source".to_string()),
            target_alias: Some("target".to_string()),
            when_matched_update_all: Some(true),
            when_matched_update_set: None,
            when_matched_delete_predicate: None,
            when_not_matched_insert_all: Some(true),
            when_not_matched_insert_set: None,
            when_not_matched_by_source_delete_predicate: None,
            when_not_matched_by_source_update_set: None,
            when_not_matched_by_source_update_predicate: None,
        };

        let result = merge_batches(cmd, vec![source_batch]).await.unwrap();
        assert_eq!(result["success"], true);
        assert_eq!(result["message"], "Merge completed.");
        assert!(result["result"][0]["num_source_rows"].as_i64().unwrap() > 0);

        // Verify: 4 rows (1,a), (2,B_updated), (3,c), (4,d_new)
        let table = open_delta_table(&path, None, None, None).await.unwrap();
        let ctx = SessionContext::new();
        let provider = table.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let df = ctx
            .sql("SELECT COUNT(*) AS cnt FROM t")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 4);
    }

    #[tokio::test]
    async fn do_put_merge_matched_delete() {
        let (path, _tmp) = temp_table_path();
        create_simple_test_table(&path).await; // rows: (1,a), (2,b), (3,c)

        // Source: id=2 — should delete the matched row.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let source_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![2])),
                Arc::new(StringArray::from(vec!["ignored"])),
            ],
        )
        .unwrap();

        let cmd = WriteCommand {
            path: path.clone(),
            mode: "overwrite".to_string(),
            operation: "merge".to_string(),
            storage_account: None,
            sas_token: None,
            configuration: None,
            partition_by: None,
            predicate: Some("target.id = source.id".to_string()),
            source_alias: Some("source".to_string()),
            target_alias: Some("target".to_string()),
            when_matched_update_all: None,
            when_matched_update_set: None,
            when_matched_delete_predicate: Some("true".to_string()),
            when_not_matched_insert_all: None,
            when_not_matched_insert_set: None,
            when_not_matched_by_source_delete_predicate: None,
            when_not_matched_by_source_update_set: None,
            when_not_matched_by_source_update_predicate: None,
        };

        let result = merge_batches(cmd, vec![source_batch]).await.unwrap();
        assert_eq!(result["success"], true);

        // Verify: 2 rows remain (1,a), (3,c)
        let table = open_delta_table(&path, None, None, None).await.unwrap();
        let ctx = SessionContext::new();
        let provider = table.table_provider().await.unwrap();
        ctx.register_table("t", provider).unwrap();
        let df = ctx
            .sql("SELECT COUNT(*) AS cnt FROM t")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 2);
    }
}
