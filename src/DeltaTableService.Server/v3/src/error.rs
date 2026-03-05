// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Unified error type for the Delta Table Service V3.
//!
//! All internal errors are funnelled through [`ServiceError`], which implements
//! conversion to [`tonic::Status`] so that Flight RPC handlers can use `?`
//! ergonomically and the correct gRPC status code is returned to the client.

use std::fmt;

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

// ---- Conversion to tonic::Status --------------------------------------------

impl From<ServiceError> for tonic::Status {
    fn from(e: ServiceError) -> Self {
        match &e {
            ServiceError::InvalidRequest(_) => tonic::Status::invalid_argument(e.to_string()),
            ServiceError::TableNotFound(_) => tonic::Status::not_found(e.to_string()),
            ServiceError::Json(_) => tonic::Status::invalid_argument(e.to_string()),
            ServiceError::Delta(_)
            | ServiceError::DataFusion(_)
            | ServiceError::Arrow(_)
            | ServiceError::Internal(_) => tonic::Status::internal(e.to_string()),
        }
    }
}
