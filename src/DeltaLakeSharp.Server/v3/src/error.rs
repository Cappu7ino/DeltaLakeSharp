// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Unified error type for the Delta Table Service V3.
//!
//! All internal errors are funnelled through [`ServiceError`], which is shared
//! by the transport-neutral Rust core and the native ABI layer.

use std::fmt;

/// Stable service error codes exposed through the native ABI.
#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceErrorCode {
    Ok = 0,
    InvalidRequest = 1,
    TableNotFound = 2,
    Delta = 3,
    DataFusion = 4,
    Arrow = 5,
    Json = 6,
    Internal = 7,
    Cancelled = 8,
}

/// Enumerates all error kinds that can occur in the service.
#[derive(Debug)]
pub enum ServiceError {
    /// The request was malformed or missing required fields.
    InvalidRequest(String),

    /// The requested Delta table was not found at the given path.
    TableNotFound(String),

    /// An error occurred while interacting with the Delta table (delta-rs).
    Delta(deltalake::DeltaTableError),

    /// An error occurred in the DataFusion query engine.
    DataFusion(datafusion::error::DataFusionError),

    /// An error during Arrow IPC serialization / deserialization.
    Arrow(arrow::error::ArrowError),

    /// JSON (de)serialization error for command payloads.
    Json(serde_json::Error),

    /// An internal / unexpected error.
    Internal(String),
}

impl ServiceError {
    /// Returns the stable native ABI code for this error.
    pub fn code(&self) -> ServiceErrorCode {
        match self {
            Self::InvalidRequest(_) => ServiceErrorCode::InvalidRequest,
            Self::TableNotFound(_) => ServiceErrorCode::TableNotFound,
            Self::Delta(_) => ServiceErrorCode::Delta,
            Self::DataFusion(_) => ServiceErrorCode::DataFusion,
            Self::Arrow(_) => ServiceErrorCode::Arrow,
            Self::Json(_) => ServiceErrorCode::Json,
            Self::Internal(_) => ServiceErrorCode::Internal,
        }
    }
}

impl fmt::Display for ServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest(msg) => write!(f, "invalid request: {msg}"),
            Self::TableNotFound(path) => write!(f, "table not found: {path}"),
            Self::Delta(e) => write!(f, "delta error: {e}"),
            Self::DataFusion(e) => write!(f, "datafusion error: {e}"),
            Self::Arrow(e) => write!(f, "arrow error: {e}"),
            Self::Json(e) => write!(f, "json error: {e}"),
            Self::Internal(msg) => write!(f, "internal error: {msg}"),
        }
    }
}

impl std::error::Error for ServiceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Delta(e) => Some(e),
            Self::DataFusion(e) => Some(e),
            Self::Arrow(e) => Some(e),
            Self::Json(e) => Some(e),
            _ => None,
        }
    }
}

// ---- Conversions from underlying error types --------------------------------

impl From<deltalake::DeltaTableError> for ServiceError {
    fn from(e: deltalake::DeltaTableError) -> Self {
        Self::Delta(e)
    }
}

impl From<datafusion::error::DataFusionError> for ServiceError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        Self::DataFusion(e)
    }
}

impl From<arrow::error::ArrowError> for ServiceError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Self::Arrow(e)
    }
}

impl From<serde_json::Error> for ServiceError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_error_code_numeric_values_are_stable() {
        assert_eq!(0, ServiceErrorCode::Ok as i32);
        assert_eq!(1, ServiceErrorCode::InvalidRequest as i32);
        assert_eq!(2, ServiceErrorCode::TableNotFound as i32);
        assert_eq!(3, ServiceErrorCode::Delta as i32);
        assert_eq!(4, ServiceErrorCode::DataFusion as i32);
        assert_eq!(5, ServiceErrorCode::Arrow as i32);
        assert_eq!(6, ServiceErrorCode::Json as i32);
        assert_eq!(7, ServiceErrorCode::Internal as i32);
        assert_eq!(8, ServiceErrorCode::Cancelled as i32);
    }

    #[test]
    fn service_error_maps_to_stable_codes() {
        assert_eq!(
            ServiceErrorCode::InvalidRequest,
            ServiceError::InvalidRequest("missing path".to_string()).code()
        );
        assert_eq!(
            ServiceErrorCode::TableNotFound,
            ServiceError::TableNotFound("/tmp/table".to_string()).code()
        );
        assert_eq!(
            ServiceErrorCode::Delta,
            ServiceError::Delta(deltalake::DeltaTableError::Generic(
                "delta failure".to_string()
            ))
            .code()
        );
        assert_eq!(
            ServiceErrorCode::DataFusion,
            ServiceError::DataFusion(datafusion::error::DataFusionError::Execution(
                "datafusion failure".to_string()
            ))
            .code()
        );
        assert_eq!(
            ServiceErrorCode::Arrow,
            ServiceError::Arrow(arrow::error::ArrowError::ExternalError(Box::new(
                std::io::Error::other("arrow failure")
            )))
            .code()
        );
        assert_eq!(
            ServiceErrorCode::Json,
            ServiceError::Json(serde_json::from_str::<serde_json::Value>("{").unwrap_err()).code()
        );
        assert_eq!(
            ServiceErrorCode::Internal,
            ServiceError::Internal("unexpected".to_string()).code()
        );
    }
}
