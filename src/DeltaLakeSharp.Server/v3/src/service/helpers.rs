// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Shared helpers for Delta table operations.
//!
//! These utilities are used by both the read and write handler modules.

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use datafusion::execution::context::SessionContext;
use deltalake::kernel::Add;
use deltalake::kernel::Protocol;
use deltalake::kernel::scalars::ScalarExt;
use tracing::{debug, warn};
use url::Url;

use crate::error::ServiceError;

/// Parses a table path into a URL.  Local filesystem paths are converted to
/// `file://` URLs; paths that already contain a scheme are parsed directly.
pub fn path_to_url(path: &str) -> Result<Url, ServiceError> {
    if path.contains("://") {
        Url::parse(path).map_err(|e| {
            ServiceError::InvalidRequest(format!("Invalid table URL '{path}': {e}"))
        })
    } else {
        // Local filesystem path → file:// URL.
        Url::from_file_path(std::path::Path::new(path)).map_err(|()| {
            ServiceError::InvalidRequest(format!(
                "Cannot convert path to file URL: '{path}'"
            ))
        })
    }
}

/// Builds storage options HashMap for delta-rs from optional account/token.
pub fn storage_options(
    storage_account: Option<&str>,
    sas_token: Option<&str>,
    additional_storage_options: Option<&HashMap<String, String>>,
) -> HashMap<String, String> {
    let mut opts = HashMap::new();
    if let Some(account) = storage_account {
        opts.insert("account_name".to_string(), account.to_string());
        if account == "onelake" {
            opts.insert("use_fabric_endpoint".to_string(), "true".to_string());
        }
    }
    if let Some(token) = sas_token {
        opts.insert("sas_token".to_string(), token.to_string());
    }

    if let Some(extra) = additional_storage_options {
        for (key, value) in extra {
            opts.insert(key.clone(), value.clone());
        }
    }

    let mut sanitized = opts.clone();
    if sanitized.contains_key("sas_token") {
        sanitized.insert("sas_token".to_string(), "REDACTED".to_string());
    }
    if sanitized.contains_key("token") {
        sanitized.insert("token".to_string(), "REDACTED".to_string());
    }
    debug!(storage_account = ?storage_account, storage_options = ?sanitized, "Resolved delta-rs storage options");

    opts
}

pub fn request_version_to_delta(version: i64, field_name: &str) -> Result<u64, ServiceError> {
    u64::try_from(version).map_err(|_| {
        ServiceError::InvalidRequest(format!("{field_name} must be non-negative: {version}"))
    })
}

pub fn delta_version_to_request(version: u64, context: &str) -> Result<i64, ServiceError> {
    i64::try_from(version).map_err(|_| {
        ServiceError::Internal(format!("{context} version {version} exceeds supported range"))
    })
}

/// Opens a Delta table at the given path with optional storage configuration.
/// When `version` is `Some(v)`, opens the table at that specific historical version.
pub async fn open_delta_table(
    path: &str,
    storage_account: Option<&str>,
    sas_token: Option<&str>,
    additional_storage_options: Option<&HashMap<String, String>>,
    version: Option<i64>,
) -> Result<deltalake::DeltaTable, ServiceError> {
    let url = path_to_url(path)?;
    let opts = storage_options(storage_account, sas_token, additional_storage_options);

    debug!(path = %path, url = %url, version = ?version, "Opening Delta table");
    let table = match version {
        Some(v) => {
            let delta_version = request_version_to_delta(v, "version")?;
            deltalake::DeltaTableBuilder::from_url(url.clone())
                .map_err(ServiceError::Delta)?
                .with_storage_options(opts)
                .with_version(delta_version)
                .load()
                .await
                .map_err(|error| {
                    warn!(path = %path, url = %url, version = v, error = %error, "Failed to open versioned Delta table");
                    ServiceError::Delta(error)
                })?
        }
        None => {
            deltalake::open_table_with_storage_options(url.clone(), opts)
                .await
                .map_err(|error| {
                    warn!(path = %path, url = %url, error = %error, "Failed to open Delta table");
                    ServiceError::Delta(error)
                })?
        }
    };
    debug!(path = %path, version = table.version(), "Delta table opened");
    Ok(table)
}

/// Opens a Delta table and registers its object store with a specific
/// DataFusion session.
pub async fn open_delta_table_for_datafusion(
    ctx: &SessionContext,
    path: &str,
    storage_account: Option<&str>,
    sas_token: Option<&str>,
    additional_storage_options: Option<&HashMap<String, String>>,
    version: Option<i64>,
) -> Result<deltalake::DeltaTable, ServiceError> {
    let table = open_delta_table(
        path,
        storage_account,
        sas_token,
        additional_storage_options,
        version,
    )
    .await?;

    table
        .update_datafusion_session(&ctx.state())
        .map_err(ServiceError::Delta)?;

    Ok(table)
}

/// Builds a Delta table handle that may point to an uninitialized location.
///
/// Intended for write paths that should create a Delta table on the first
/// successful commit when `_delta_log` is not present yet.
///
/// For local paths, the target directory must exist before delta-rs can infer
/// the file-backed log store, so this helper creates the directory eagerly.
pub async fn open_or_initialize_delta_table(
    path: &str,
    storage_account: Option<&str>,
    sas_token: Option<&str>,
    additional_storage_options: Option<&HashMap<String, String>>,
) -> Result<deltalake::DeltaTable, ServiceError> {
    ensure_local_table_directory_exists(path)?;

    let url = path_to_url(path)?;
    let opts = storage_options(storage_account, sas_token, additional_storage_options);

    debug!(path = %path, "Opening Delta table handle for create-on-write");
    deltalake::DeltaTable::try_from_url_with_storage_options(url, opts)
        .await
        .map_err(ServiceError::Delta)
}

fn ensure_local_table_directory_exists(path: &str) -> Result<(), ServiceError> {
    if path.contains("://") {
        let url = Url::parse(path)
            .map_err(|e| ServiceError::InvalidRequest(format!("Invalid table URL '{path}': {e}")))?;
        if url.scheme() != "file" {
            return Ok(());
        }

        let file_path = url.to_file_path().map_err(|_| {
            ServiceError::InvalidRequest(format!("Cannot convert file URL to path: '{path}'"))
        })?;
        fs::create_dir_all(file_path).map_err(|e| {
            ServiceError::Internal(format!("Failed to create local table directory '{path}': {e}"))
        })?;
        return Ok(());
    }

    fs::create_dir_all(Path::new(path)).map_err(|e| {
        ServiceError::Internal(format!("Failed to create local table directory '{path}': {e}"))
    })
}

/// Registers a Delta table in a DataFusion `SessionContext`.
///
/// Uses `DeltaTable::table_provider()` which returns `Arc<dyn TableProvider>`
/// (via `DeltaScanNext`), since `DeltaTable` itself does not implement
/// `TableProvider`.
pub async fn register_delta_table(
    ctx: &SessionContext,
    table_name: &str,
    path: &str,
    storage_account: Option<&str>,
    sas_token: Option<&str>,
    additional_storage_options: Option<&HashMap<String, String>>,
    version: Option<i64>,
) -> Result<(), ServiceError> {
    let table = open_delta_table_for_datafusion(
        ctx,
        path,
        storage_account,
        sas_token,
        additional_storage_options,
        version,
    )
    .await?;
    let provider = table
        .table_provider()
        .await
        .map_err(ServiceError::DataFusion)?;
    ctx.register_table(table_name, provider)
        .map_err(ServiceError::DataFusion)?;
    Ok(())
}

pub async fn get_active_add_actions(
    path: &str,
    storage_account: Option<&str>,
    sas_token: Option<&str>,
    additional_storage_options: Option<&HashMap<String, String>>,
    version: Option<i64>,
) -> Result<(deltalake::DeltaTable, Vec<Add>), ServiceError> {
    let table = open_delta_table(
        path,
        storage_account,
        sas_token,
        additional_storage_options,
        version,
    )
    .await?;

    let adds = table
        .snapshot()
        .map_err(ServiceError::Delta)?
        .log_data()
        .into_iter()
        .map(|file| Add {
            path: file.path().to_string(),
            partition_values: file
                .partition_values()
                .map(|data| {
                    data.fields()
                        .iter()
                        .zip(data.values().iter())
                        .map(|(field, value)| {
                            (
                                field.name().to_string(),
                                if value.is_null() {
                                    None
                                } else {
                                    Some(value.serialize())
                                },
                            )
                        })
                        .collect()
                })
                .unwrap_or_default(),
            size: file.size(),
            modification_time: file.modification_time(),
            // This reconstructed Add is used only as a read descriptor for subset scans.
            // Snapshot file views do not preserve the original log action's dataChange bit,
            // and delta-rs's own file_view.add_action() helper also hard-codes true here.
            data_change: true,
            stats: file.stats(),
            tags: None,
            deletion_vector: file.deletion_vector_descriptor(),
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        })
        .collect::<Vec<_>>();

    Ok((table, adds))
}

pub fn table_protocol(table: &deltalake::DeltaTable) -> Result<Protocol, ServiceError> {
    Ok(table
        .snapshot()
        .map_err(ServiceError::Delta)?
        .protocol()
        .clone())
}

pub fn has_reader_feature(protocol: &Protocol, feature_name: &str) -> bool {
    format!("{protocol:?}").contains(feature_name)
}

pub async fn register_delta_table_with_files(
    ctx: &SessionContext,
    table_name: &str,
    table: &deltalake::DeltaTable,
    files: Vec<Add>,
) -> Result<(), ServiceError> {
    table
        .update_datafusion_session(&ctx.state())
        .map_err(ServiceError::Delta)?;

    let provider = deltalake::delta_datafusion::DeltaTableProvider::try_new(
        table.snapshot().map_err(ServiceError::Delta)?.snapshot().clone(),
        table.log_store(),
        deltalake::delta_datafusion::DeltaScanConfig::new_from_session(&ctx.state()),
    )
    .map_err(ServiceError::Delta)?
    .with_files(files);

    ctx.register_table(table_name, Arc::new(provider))
        .map_err(ServiceError::DataFusion)?;
    Ok(())
}

/// Builds a JSON success response envelope (no result array).
///
/// ```json
/// {"success": true, "message": "..."}
/// ```
pub fn success_response(message: &str) -> serde_json::Value {
    serde_json::json!({
        "success": true,
        "message": message,
    })
}

/// Builds a JSON success response envelope with a result array.
///
/// ```json
/// {"success": true, "message": "...", "result": [...]}
/// ```
pub fn success_response_with_result(
    message: &str,
    result: Vec<serde_json::Value>,
) -> serde_json::Value {
    serde_json::json!({
        "success": true,
        "message": message,
        "result": result,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_version_to_delta_rejects_negative_versions() {
        let error = request_version_to_delta(-1, "version").expect_err("negative version");
        assert!(matches!(error, ServiceError::InvalidRequest(_)));
    }

    #[test]
    fn request_version_to_delta_accepts_zero_and_positive_versions() {
        assert_eq!(0, request_version_to_delta(0, "version").unwrap());
        assert_eq!(42, request_version_to_delta(42, "version").unwrap());
    }

    #[test]
    fn delta_version_to_request_rejects_out_of_range_versions() {
        let error = delta_version_to_request(u64::MAX, "test").expect_err("out of range");
        assert!(matches!(error, ServiceError::Internal(_)));
    }
}

