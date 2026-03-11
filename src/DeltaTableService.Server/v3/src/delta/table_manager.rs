// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Delta table lifecycle management.
//!
//! Table open/create/read/write operations are handled directly in
//! `handlers::helpers` and `handlers::write`.  This module is retained as a
//! namespace for future session-pooling or caching extensions.
