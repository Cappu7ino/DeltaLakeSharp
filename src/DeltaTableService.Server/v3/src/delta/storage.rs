// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Storage configuration helpers for local filesystem and cloud storage.
//!
//! Stub module for Phase 1. Will be extended in later phases to support
//! Azure Blob Storage (abfss://) and OneLake endpoints.

/// Storage configuration extracted from the JSON command payload.
///
/// For Phase 1, only local filesystem paths are supported.
/// Cloud storage fields (`storage_account`, `sas_token`) are parsed
/// but not yet wired to `object_store`.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct StorageConfig {
    /// Azure storage account name (e.g. "mystorageaccount").
    #[serde(default)]
    pub storage_account: Option<String>,

    /// SAS token for Azure Blob / ADLS Gen2 access.
    #[serde(default)]
    pub sas_token: Option<String>,
}
