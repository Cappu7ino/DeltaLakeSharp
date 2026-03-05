// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Delta table lifecycle management — open, create, read, write.
//!
//! Stub module for Phase 1. Will be implemented in Phase 2 (read) and Phase 3 (write).

/// Placeholder for the table manager that will cache DataFusion sessions
/// and Delta table handles.
pub struct TableManager {
    // TODO(phase2): Add `SessionContext` + table registry.
}

impl TableManager {
    /// Creates a new table manager.
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for TableManager {
    fn default() -> Self {
        Self::new()
    }
}
