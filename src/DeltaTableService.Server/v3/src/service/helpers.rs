// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Shared helpers for Delta table operations.
//!
//! These utilities are used by both the read and write handler modules.

use std::collections::HashMap;

use datafusion::execution::context::SessionContext;
use tracing::debug;
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
    opts
}

/// Opens a Delta table at the given path with optional storage configuration.
/// When `version` is `Some(v)`, opens the table at that specific historical version.
pub async fn open_delta_table(
    path: &str,
    storage_account: Option<&str>,
    sas_token: Option<&str>,
    version: Option<i64>,
) -> Result<deltalake::DeltaTable, ServiceError> {
    let url = path_to_url(path)?;
    let opts = storage_options(storage_account, sas_token);

    debug!(path = %path, version = ?version, "Opening Delta table");
    let table = match version {
        Some(v) => {
            deltalake::DeltaTableBuilder::from_url(url)
                .map_err(ServiceError::Delta)?
                .with_storage_options(opts)
                .with_version(v)
                .load()
                .await
                .map_err(ServiceError::Delta)?
        }
        None => {
            deltalake::open_table_with_storage_options(url, opts)
                .await
                .map_err(ServiceError::Delta)?
        }
    };
    debug!(path = %path, version = table.version(), "Delta table opened");
    Ok(table)
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
    version: Option<i64>,
) -> Result<(), ServiceError> {
    let table = open_delta_table(path, storage_account, sas_token, version).await?;
    let provider = table
        .table_provider()
        .await
        .map_err(ServiceError::DataFusion)?;
    ctx.register_table(table_name, provider)
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

