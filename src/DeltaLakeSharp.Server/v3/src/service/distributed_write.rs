use serde_json::json;

use crate::error::ServiceError;

use super::request::BeginDistributedWriteCommand;

const DEFAULT_STAGING_PREFIX: &str = "_staging";
const ADDS_DIRECTORY: &str = "adds";

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
    let table_disposition = validate_table_disposition(
        cmd.table_disposition
            .as_deref()
            .unwrap_or("existingTable"),
    )?;
    let overwrite_scope = validate_overwrite_scope(
        cmd.overwrite_scope.as_deref().unwrap_or("fullTable"),
    )?;
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
            "tableDisposition": table_disposition,
            "overwriteScope": overwrite_scope,
            "stagingPrefix": staging_prefix,
            "addsPrefix": adds_prefix,
            "partitionBy": cmd.partition_by.unwrap_or_default()
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

fn validate_table_disposition(value: &str) -> Result<&str, ServiceError> {
    match value {
        "existingTable" | "createIfMissing" | "createOrReplace" => Ok(value),
        other => Err(ServiceError::InvalidRequest(format!(
            "unsupported distributed write table_disposition '{other}'"
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

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!("staging.tmp/123e4567-e89b-12d3-a456-426614174000/adds", prefix);
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
}
