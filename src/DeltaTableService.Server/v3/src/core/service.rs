// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Transport-neutral service facade.
//!
//! This is the initial extraction point for moving V3 away from Flight-centric
//! request handling.  The first step intentionally keeps the existing handler
//! implementations as the source of truth while exposing a plain Rust service
//! surface that native interop can grow around.

use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use serde_json::json;

use crate::error::ServiceError;

/// Stateless facade over the existing V3 behavior.
#[derive(Debug, Default, Clone)]
pub struct DeltaService;

impl DeltaService {
    /// Creates a new service facade.
    pub fn new() -> Self {
        Self
    }

    /// Returns the standard V3 health payload.
    pub fn health_json(&self) -> serde_json::Value {
        json!({
            "status": "healthy",
            "engine": "datafusion + delta-rs (Rust native)",
            "version": "v3"
        })
    }

    /// Returns the standard V3 shutdown acknowledgement payload.
    pub fn shutdown_json(&self) -> serde_json::Value {
        json!({
            "status": "shutting_down",
            "message": "Server is shutting down"
        })
    }

    /// Resolves the Arrow schema for a command payload.
    pub async fn get_schema(&self, body: &[u8]) -> Result<Schema, ServiceError> {
        let schema = crate::handlers::read::resolve_schema_from_command_bytes(body).await?;
        Ok(schema.as_ref().clone())
    }

    /// Resolves a command payload into a streaming batch reader suitable for the
    /// Arrow C Stream interface.
    pub async fn read_batches(
        &self,
        body: &[u8],
        runtime_handle: tokio::runtime::Handle,
    ) -> Result<Box<dyn RecordBatchReader<Item = Result<RecordBatch, ArrowError>> + Send>, ServiceError>
    {
        crate::handlers::read::resolve_batch_reader_from_command_bytes(body, runtime_handle).await
    }

    /// Executes a SQL/read command and returns a transport-neutral streaming
    /// reader. The command format is intentionally the same as the existing V3
    /// transport contract so that query semantics remain stable during the
    /// migration away from Flight.
    pub async fn execute_query_reader(
        &self,
        body: &[u8],
        runtime_handle: tokio::runtime::Handle,
    ) -> Result<Box<dyn RecordBatchReader<Item = Result<RecordBatch, ArrowError>> + Send>, ServiceError>
    {
        crate::handlers::read::resolve_batch_reader_from_command_bytes(body, runtime_handle).await
    }

    /// Delegates table creation to the existing write handler.
    pub async fn create_table(&self, body: &[u8]) -> Result<serde_json::Value, ServiceError> {
        crate::handlers::write::handle_create_table(body).await
    }

    /// Consumes an imported Arrow C Stream reader and writes it to the target
    /// Delta table using the existing V3 write semantics.
    pub async fn insert_reader(
        &self,
        body: &[u8],
        mut reader: ArrowArrayStreamReader,
    ) -> Result<serde_json::Value, ServiceError> {
        let cmd = serde_json::from_slice::<crate::handlers::commands::DoPutCommand>(body)
            .map_err(ServiceError::Json)?;
        let batches = reader
            .by_ref()
            .collect::<Result<Vec<_>, _>>()
            .map_err(ServiceError::Arrow)?;
        crate::handlers::write::handle_native_insert(cmd, batches).await
    }

    /// Consumes an imported Arrow C Stream reader and merges it into the target
    /// Delta table using the existing V3 merge semantics.
    pub async fn merge_reader(
        &self,
        body: &[u8],
        mut reader: ArrowArrayStreamReader,
    ) -> Result<serde_json::Value, ServiceError> {
        let cmd = serde_json::from_slice::<crate::handlers::commands::DoPutCommand>(body)
            .map_err(ServiceError::Json)?;
        let batches = reader
            .by_ref()
            .collect::<Result<Vec<_>, _>>()
            .map_err(ServiceError::Arrow)?;
        crate::handlers::write::handle_native_merge(cmd, batches).await
    }

    /// Delegates DML execution to the existing write handler.
    pub async fn execute_dml(&self, body: &[u8]) -> Result<serde_json::Value, ServiceError> {
        crate::handlers::write::handle_execute_dml(body).await
    }

    /// Delegates protocol upgrade to the existing write handler.
    pub async fn upgrade_protocol(
        &self,
        body: &[u8],
    ) -> Result<serde_json::Value, ServiceError> {
        crate::handlers::write::handle_upgrade_protocol(body).await
    }
}
