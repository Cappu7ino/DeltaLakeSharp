// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Minimal native ABI scaffold for the in-process V3 architecture.
//!
//! This file intentionally starts small: lifecycle, health, and error plumbing.
//! Arrow C Data / C Stream entrypoints will be layered on top of this engine in
//! subsequent changes.

use std::ffi::{c_char, CStr, CString};
use std::ptr;
use std::sync::Once;
use std::sync::Mutex;

use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use tracing_subscriber::EnvFilter;

use crate::service::DeltaService;

static INIT_TRACING: Once = Once::new();

/// Opaque native engine handle owned by the consumer.
pub struct DeltaServiceEngine {
    service: DeltaService,
    last_error: Mutex<Option<CString>>,
    runtime: tokio::runtime::Runtime,
}

impl DeltaServiceEngine {
    fn new() -> Self {
        INIT_TRACING.call_once(|| {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| EnvFilter::new("info,object_store=debug,deltalake=debug,delta_table_service_v3=debug")),
                )
                .try_init();
        });

        Self {
            service: DeltaService::new(),
            last_error: Mutex::new(None),
            runtime: tokio::runtime::Runtime::new()
                .expect("creating Tokio runtime for native V3 engine should succeed"),
        }
    }

    fn set_last_error_message(&self, message: String) {
        if let Ok(mut slot) = self.last_error.lock() {
            *slot = CString::new(message).ok();
        }
    }

    fn clear_last_error(&self) {
        if let Ok(mut slot) = self.last_error.lock() {
            *slot = None;
        }
    }

    fn last_error_ptr(&self) -> *const c_char {
        self.last_error
            .lock()
            .ok()
            .and_then(|slot| slot.as_ref().map(|msg| msg.as_ptr()))
            .unwrap_or(ptr::null())
    }

    fn block_on<F, T>(&self, future: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        self.runtime.block_on(future)
    }

    fn runtime_handle(&self) -> tokio::runtime::Handle {
        self.runtime.handle().clone()
    }
}

fn with_engine<T>(
    engine: *mut DeltaServiceEngine,
    f: impl FnOnce(&DeltaServiceEngine) -> T,
) -> Option<T> {
    if engine.is_null() {
        None
    } else {
        // SAFETY: caller provides an engine pointer originally produced by
        // `dts_create_engine`; null is checked above.
        let engine_ref = unsafe { &*engine };
        Some(f(engine_ref))
    }
}

/// Creates a new native service engine.
#[unsafe(no_mangle)]
pub extern "C" fn dts_create_engine() -> *mut DeltaServiceEngine {
    Box::into_raw(Box::new(DeltaServiceEngine::new()))
}

/// Destroys a previously created native service engine.
#[unsafe(no_mangle)]
pub extern "C" fn dts_destroy_engine(engine: *mut DeltaServiceEngine) {
    if engine.is_null() {
        return;
    }

    // SAFETY: `engine` was allocated by `Box::into_raw` in `dts_create_engine`
    // and is consumed exactly once here.
    unsafe {
        drop(Box::from_raw(engine));
    }
}

/// Returns 1 when the engine is healthy, 0 otherwise.
#[unsafe(no_mangle)]
pub extern "C" fn dts_health_check(engine: *mut DeltaServiceEngine) -> i32 {
    with_engine(engine, |engine_ref| {
        engine_ref.clear_last_error();
        let body = engine_ref.service.health_json();
        if body.get("status").and_then(|v| v.as_str()) == Some("healthy") {
            1
        } else {
            engine_ref
                .set_last_error_message("Health check did not return healthy status.".to_string());
            0
        }
    })
    .unwrap_or(0)
}

/// Returns the last engine error as a borrowed UTF-8 string.
#[unsafe(no_mangle)]
pub extern "C" fn dts_get_last_error(engine: *mut DeltaServiceEngine) -> *const c_char {
    with_engine(engine, |engine_ref| engine_ref.last_error_ptr()).unwrap_or(ptr::null())
}

/// Frees a UTF-8 string allocated by the native layer.
#[unsafe(no_mangle)]
pub extern "C" fn dts_free_string(value: *mut c_char) {
    if value.is_null() {
        return;
    }

    // SAFETY: `value` must have been allocated by `CString::into_raw` on the
    // Rust side. This helper is provided for future result-returning entrypoints.
    unsafe {
        drop(CString::from_raw(value));
    }
}

/// Resolves the Arrow schema for a JSON command and exports it via the Arrow C
/// Data Interface.
///
/// Returns 1 on success and 0 on failure. On failure, callers can retrieve the
/// message with `dts_get_last_error`.
#[unsafe(no_mangle)]
pub extern "C" fn dts_get_schema(
    engine: *mut DeltaServiceEngine,
    command_json: *const c_char,
    out_schema: *mut FFI_ArrowSchema,
) -> i32 {
    with_engine(engine, |engine_ref| {
        engine_ref.clear_last_error();

        if command_json.is_null() {
            engine_ref.set_last_error_message("command_json must not be null.".to_string());
            return 0;
        }

        if out_schema.is_null() {
            engine_ref.set_last_error_message("out_schema must not be null.".to_string());
            return 0;
        }

        let command = match unsafe { CStr::from_ptr(command_json) }.to_bytes() {
            bytes => bytes,
        };

        let schema = match engine_ref.block_on(engine_ref.service.get_schema(command)) {
            Ok(schema) => schema,
            Err(error) => {
                engine_ref.set_last_error_message(error.to_string());
                return 0;
            }
        };

        let ffi_schema = match FFI_ArrowSchema::try_from(&schema) {
            Ok(schema) => schema,
            Err(error) => {
                engine_ref.set_last_error_message(error.to_string());
                return 0;
            }
        };

        // SAFETY: `out_schema` is validated non-null above and points to caller-
        // allocated writable memory for a single `FFI_ArrowSchema` value.
        unsafe {
            ptr::write(out_schema, ffi_schema);
        }

        1
    })
    .unwrap_or(0)
}

/// Resolves a read/query command and exports the resulting batch stream via the
/// Arrow C Stream interface.
#[unsafe(no_mangle)]
pub extern "C" fn dts_read_table(
    engine: *mut DeltaServiceEngine,
    command_json: *const c_char,
    out_stream: *mut FFI_ArrowArrayStream,
) -> i32 {
    with_engine(engine, |engine_ref| {
        engine_ref.clear_last_error();

        if command_json.is_null() {
            engine_ref.set_last_error_message("command_json must not be null.".to_string());
            return 0;
        }

        if out_stream.is_null() {
            engine_ref.set_last_error_message("out_stream must not be null.".to_string());
            return 0;
        }

        let command = unsafe { CStr::from_ptr(command_json) }.to_bytes();
        let reader = match engine_ref.block_on(
            engine_ref
                .service
                .read_batches(command, engine_ref.runtime_handle()),
        ) {
            Ok(reader) => reader,
            Err(error) => {
                engine_ref.set_last_error_message(error.to_string());
                return 0;
            }
        };

        let ffi_stream = FFI_ArrowArrayStream::new(reader);

        unsafe {
            ptr::write(out_stream, ffi_stream);
        }

        1
    })
    .unwrap_or(0)
}

/// Plans opaque read partitions for a pinned Delta snapshot and returns the JSON
/// result payload as an owned UTF-8 string.
#[unsafe(no_mangle)]
pub extern "C" fn dts_plan_read_partitions(
    engine: *mut DeltaServiceEngine,
    command_json: *const c_char,
) -> *mut c_char {
    with_engine(engine, |engine_ref| {
        engine_ref.clear_last_error();

        if command_json.is_null() {
            engine_ref.set_last_error_message("command_json must not be null.".to_string());
            return ptr::null_mut();
        }

        let command = unsafe { CStr::from_ptr(command_json) }.to_bytes();
        let result = match engine_ref.block_on(engine_ref.service.plan_read_partitions(command)) {
            Ok(result) => result,
            Err(error) => {
                engine_ref.set_last_error_message(error.to_string());
                return ptr::null_mut();
            }
        };

        match CString::new(result.to_string()) {
            Ok(result) => result.into_raw(),
            Err(error) => {
                engine_ref.set_last_error_message(error.to_string());
                ptr::null_mut()
            }
        }
    })
    .unwrap_or(ptr::null_mut())
}

/// Resolves a partition-scoped read command and exports the resulting batch
/// stream via the Arrow C Stream interface.
#[unsafe(no_mangle)]
pub extern "C" fn dts_read_table_partition(
    engine: *mut DeltaServiceEngine,
    command_json: *const c_char,
    out_stream: *mut FFI_ArrowArrayStream,
) -> i32 {
    dts_read_table(engine, command_json, out_stream)
}

/// Resolves a SQL/read command and exports the resulting batch stream via the
/// Arrow C Stream interface.
#[unsafe(no_mangle)]
pub extern "C" fn dts_execute_query(
    engine: *mut DeltaServiceEngine,
    command_json: *const c_char,
    out_stream: *mut FFI_ArrowArrayStream,
) -> i32 {
    with_engine(engine, |engine_ref| {
        engine_ref.clear_last_error();

        if command_json.is_null() {
            engine_ref.set_last_error_message("command_json must not be null.".to_string());
            return 0;
        }

        if out_stream.is_null() {
            engine_ref.set_last_error_message("out_stream must not be null.".to_string());
            return 0;
        }

        let command = unsafe { CStr::from_ptr(command_json) }.to_bytes();
        let reader = match engine_ref.block_on(
            engine_ref
                .service
                .execute_query_reader(command, engine_ref.runtime_handle()),
        ) {
            Ok(reader) => reader,
            Err(error) => {
                engine_ref.set_last_error_message(error.to_string());
                return 0;
            }
        };

        let ffi_stream = FFI_ArrowArrayStream::new(reader);

        unsafe {
            ptr::write(out_stream, ffi_stream);
        }

        1
    })
    .unwrap_or(0)
}

/// Resolves a change-data-feed command and exports the resulting batch stream
/// via the Arrow C Stream interface.
#[unsafe(no_mangle)]
pub extern "C" fn dts_read_change_data(
    engine: *mut DeltaServiceEngine,
    command_json: *const c_char,
    out_stream: *mut FFI_ArrowArrayStream,
) -> i32 {
    with_engine(engine, |engine_ref| {
        engine_ref.clear_last_error();

        if command_json.is_null() {
            engine_ref.set_last_error_message("command_json must not be null.".to_string());
            return 0;
        }

        if out_stream.is_null() {
            engine_ref.set_last_error_message("out_stream must not be null.".to_string());
            return 0;
        }

        let command = unsafe { CStr::from_ptr(command_json) }.to_bytes();
        let reader = match engine_ref.block_on(
            engine_ref
                .service
                .read_change_data_batches(command, engine_ref.runtime_handle()),
        ) {
            Ok(reader) => reader,
            Err(error) => {
                engine_ref.set_last_error_message(error.to_string());
                return 0;
            }
        };

        let ffi_stream = FFI_ArrowArrayStream::new(reader);

        unsafe {
            ptr::write(out_stream, ffi_stream);
        }

        1
    })
    .unwrap_or(0)
}

/// Imports a source Arrow C Stream from the caller and writes it into a Delta
/// table using the existing V3 insert semantics.
#[unsafe(no_mangle)]
pub extern "C" fn dts_insert(
    engine: *mut DeltaServiceEngine,
    command_json: *const c_char,
    source_stream: *mut FFI_ArrowArrayStream,
) -> i32 {
    with_engine(engine, |engine_ref| {
        engine_ref.clear_last_error();

        if command_json.is_null() {
            engine_ref.set_last_error_message("command_json must not be null.".to_string());
            return 0;
        }

        if source_stream.is_null() {
            engine_ref.set_last_error_message("source_stream must not be null.".to_string());
            return 0;
        }

        let command = unsafe { CStr::from_ptr(command_json) }.to_bytes();
        let reader = match unsafe { ArrowArrayStreamReader::from_raw(source_stream) } {
            Ok(reader) => reader,
            Err(error) => {
                engine_ref.set_last_error_message(error.to_string());
                return 0;
            }
        };

        match engine_ref.block_on(engine_ref.service.insert_reader(command, reader)) {
            Ok(_) => 1,
            Err(error) => {
                engine_ref.set_last_error_message(error.to_string());
                0
            }
        }
    })
    .unwrap_or(0)
}

/// Imports a source Arrow C Stream and performs a streaming merge against the
/// target Delta table using the existing V3 merge semantics.
#[unsafe(no_mangle)]
pub extern "C" fn dts_merge_stream(
    engine: *mut DeltaServiceEngine,
    command_json: *const c_char,
    source_stream: *mut FFI_ArrowArrayStream,
) -> *mut c_char {
    with_engine(engine, |engine_ref| {
        engine_ref.clear_last_error();

        if command_json.is_null() {
            engine_ref.set_last_error_message("command_json must not be null.".to_string());
            return ptr::null_mut();
        }

        if source_stream.is_null() {
            engine_ref.set_last_error_message("source_stream must not be null.".to_string());
            return ptr::null_mut();
        }

        let command = unsafe { CStr::from_ptr(command_json) }.to_bytes();
        let reader = match unsafe { ArrowArrayStreamReader::from_raw(source_stream) } {
            Ok(reader) => reader,
            Err(error) => {
                engine_ref.set_last_error_message(error.to_string());
                return ptr::null_mut();
            }
        };

        let result = match engine_ref.block_on(engine_ref.service.merge_reader(command, reader)) {
            Ok(result) => result,
            Err(error) => {
                engine_ref.set_last_error_message(error.to_string());
                return ptr::null_mut();
            }
        };

        match CString::new(result.to_string()) {
            Ok(result) => result.into_raw(),
            Err(error) => {
                engine_ref.set_last_error_message(error.to_string());
                ptr::null_mut()
            }
        }
    })
    .unwrap_or(ptr::null_mut())
}

/// Executes a create-table command using the transport-neutral V3 core and
/// returns the standard JSON result payload as an owned UTF-8 string.
#[unsafe(no_mangle)]
pub extern "C" fn dts_create_table(
    engine: *mut DeltaServiceEngine,
    command_json: *const c_char,
) -> *mut c_char {
    with_engine(engine, |engine_ref| {
        engine_ref.clear_last_error();

        if command_json.is_null() {
            engine_ref.set_last_error_message("command_json must not be null.".to_string());
            return ptr::null_mut();
        }

        let command = unsafe { CStr::from_ptr(command_json) }.to_bytes();
        let result = match engine_ref.block_on(engine_ref.service.create_table(command)) {
            Ok(result) => result,
            Err(error) => {
                engine_ref.set_last_error_message(error.to_string());
                return ptr::null_mut();
            }
        };

        match CString::new(result.to_string()) {
            Ok(result) => result.into_raw(),
            Err(error) => {
                engine_ref.set_last_error_message(error.to_string());
                ptr::null_mut()
            }
        }
    })
    .unwrap_or(ptr::null_mut())
}

/// Executes an upgrade-protocol command using the transport-neutral V3 core and
/// returns the standard JSON result payload as an owned UTF-8 string.
#[unsafe(no_mangle)]
pub extern "C" fn dts_upgrade_protocol(
    engine: *mut DeltaServiceEngine,
    command_json: *const c_char,
) -> *mut c_char {
    with_engine(engine, |engine_ref| {
        engine_ref.clear_last_error();

        if command_json.is_null() {
            engine_ref.set_last_error_message("command_json must not be null.".to_string());
            return ptr::null_mut();
        }

        let command = unsafe { CStr::from_ptr(command_json) }.to_bytes();
        let result = match engine_ref.block_on(engine_ref.service.upgrade_protocol(command)) {
            Ok(result) => result,
            Err(error) => {
                engine_ref.set_last_error_message(error.to_string());
                return ptr::null_mut();
            }
        };

        match CString::new(result.to_string()) {
            Ok(result) => result.into_raw(),
            Err(error) => {
                engine_ref.set_last_error_message(error.to_string());
                ptr::null_mut()
            }
        }
    })
    .unwrap_or(ptr::null_mut())
}

/// Executes a DML command using the transport-neutral V3 core and returns the
/// standard JSON result payload as an owned UTF-8 string.
#[unsafe(no_mangle)]
pub extern "C" fn dts_execute_dml(
    engine: *mut DeltaServiceEngine,
    command_json: *const c_char,
) -> *mut c_char {
    with_engine(engine, |engine_ref| {
        engine_ref.clear_last_error();

        if command_json.is_null() {
            engine_ref.set_last_error_message("command_json must not be null.".to_string());
            return ptr::null_mut();
        }

        let command = unsafe { CStr::from_ptr(command_json) }.to_bytes();
        let result = match engine_ref.block_on(engine_ref.service.execute_dml(command)) {
            Ok(result) => result,
            Err(error) => {
                engine_ref.set_last_error_message(error.to_string());
                return ptr::null_mut();
            }
        };

        match CString::new(result.to_string()) {
            Ok(result) => result.into_raw(),
            Err(error) => {
                engine_ref.set_last_error_message(error.to_string());
                ptr::null_mut()
            }
        }
    })
    .unwrap_or(ptr::null_mut())
}

/// Copies a borrowed error string into an owned allocation for FFI callers that
/// need a stable buffer independent of engine mutation.
pub fn clone_c_string(ptr: *const c_char) -> Option<CString> {
    if ptr.is_null() {
        return None;
    }

    // SAFETY: caller guarantees `ptr` points to a valid null-terminated string.
    let borrowed = unsafe { CStr::from_ptr(ptr) };
    Some(CString::new(borrowed.to_bytes()).ok()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ffi::FFI_ArrowSchema;
    use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
    use arrow::record_batch::RecordBatch;

    /// Creates a small local Delta table used by native ABI tests.
    async fn create_native_test_table() -> (String, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("temp dir");
        let table_path = tmp.path().join("native_table");
        std::fs::create_dir(&table_path).expect("create table dir");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .expect("record batch");

        let url = url::Url::from_file_path(&table_path).expect("url");
        let table = deltalake::DeltaTable::try_from_url(url)
            .await
            .expect("try_from_url");
        table.write(vec![batch]).await.expect("write");

        (table_path.to_string_lossy().to_string(), tmp)
    }

    #[test]
    fn engine_health_check_returns_healthy() {
        let engine = dts_create_engine();
        assert_eq!(1, dts_health_check(engine));
        assert!(dts_get_last_error(engine).is_null());
        dts_destroy_engine(engine);
    }

    #[test]
    fn get_schema_rejects_null_arguments() {
        let engine = dts_create_engine();
        assert_eq!(0, dts_get_schema(engine, ptr::null(), ptr::null_mut()));
        assert!(!dts_get_last_error(engine).is_null());
        dts_destroy_engine(engine);
    }

    #[test]
    fn read_table_rejects_null_arguments() {
        let engine = dts_create_engine();
        assert_eq!(0, dts_read_table(engine, ptr::null(), ptr::null_mut()));
        assert!(!dts_get_last_error(engine).is_null());
        dts_destroy_engine(engine);
    }

    #[test]
    fn execute_query_rejects_null_arguments() {
        let engine = dts_create_engine();
        assert_eq!(0, dts_execute_query(engine, ptr::null(), ptr::null_mut()));
        assert!(!dts_get_last_error(engine).is_null());
        dts_destroy_engine(engine);
    }

    #[test]
    fn insert_rejects_null_arguments() {
        let engine = dts_create_engine();
        assert_eq!(0, dts_insert(engine, ptr::null(), ptr::null_mut()));
        assert!(!dts_get_last_error(engine).is_null());
        dts_destroy_engine(engine);
    }

    #[test]
    fn merge_stream_rejects_null_arguments() {
        let engine = dts_create_engine();
        assert!(dts_merge_stream(engine, ptr::null(), ptr::null_mut()).is_null());
        assert!(!dts_get_last_error(engine).is_null());
        dts_destroy_engine(engine);
    }

    #[test]
    fn create_table_rejects_null_arguments() {
        let engine = dts_create_engine();
        assert!(dts_create_table(engine, ptr::null()).is_null());
        assert!(!dts_get_last_error(engine).is_null());
        dts_destroy_engine(engine);
    }

    #[test]
    fn upgrade_protocol_rejects_null_arguments() {
        let engine = dts_create_engine();
        assert!(dts_upgrade_protocol(engine, ptr::null()).is_null());
        assert!(!dts_get_last_error(engine).is_null());
        dts_destroy_engine(engine);
    }

    #[test]
    fn execute_dml_rejects_null_arguments() {
        let engine = dts_create_engine();
        assert!(dts_execute_dml(engine, ptr::null()).is_null());
        assert!(!dts_get_last_error(engine).is_null());
        dts_destroy_engine(engine);
    }

    #[test]
    fn get_schema_exports_arrow_schema() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let (path, _guard) = runtime.block_on(create_native_test_table());

        let command = CString::new(
            serde_json::json!({
                "path": path,
            })
            .to_string(),
        )
        .expect("command json");

        let engine = dts_create_engine();
        let mut ffi_schema = FFI_ArrowSchema::empty();

        let status = dts_get_schema(engine, command.as_ptr(), &mut ffi_schema);
        assert_eq!(1, status, "native get_schema should succeed");

        let schema = Schema::try_from(&ffi_schema).expect("import schema");
        assert_eq!(2, schema.fields().len());
        assert_eq!("id", schema.field(0).name());
        assert_eq!(&DataType::Int32, schema.field(0).data_type());
        assert_eq!("name", schema.field(1).name());
        assert_eq!(&DataType::Utf8, schema.field(1).data_type());

        dts_destroy_engine(engine);
    }

    #[test]
    fn read_table_exports_arrow_array_stream() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let (path, _guard) = runtime.block_on(create_native_test_table());

        let command = CString::new(
            serde_json::json!({
                "path": path,
            })
            .to_string(),
        )
        .expect("command json");

        let engine = dts_create_engine();
        let mut ffi_stream = FFI_ArrowArrayStream::empty();

        let status = dts_read_table(engine, command.as_ptr(), &mut ffi_stream);
        assert_eq!(1, status, "native read_table should succeed");

        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut ffi_stream) }
            .expect("import stream");
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .expect("collect batches");

        assert_eq!(1, batches.len());
        assert_eq!(3, batches[0].num_rows());
        assert_eq!(2, batches[0].num_columns());

        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 ids");
        assert_eq!(1, ids.value(0));
        assert_eq!(2, ids.value(1));
        assert_eq!(3, ids.value(2));

        dts_destroy_engine(engine);
    }


    #[test]
    fn execute_query_exports_arrow_array_stream() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let (path, _guard) = runtime.block_on(create_native_test_table());

        let command = CString::new(
            serde_json::json!({
                "sql": "SELECT name FROM tbl WHERE id >= 2",
                "table_path": path,
                "table_name": "tbl"
            })
            .to_string(),
        )
        .expect("command json");

        let engine = dts_create_engine();
        let mut ffi_stream = FFI_ArrowArrayStream::empty();

        let status = dts_execute_query(engine, command.as_ptr(), &mut ffi_stream);
        assert_eq!(1, status, "native execute_query should succeed");

        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut ffi_stream) }
            .expect("import stream");
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .expect("collect batches");

        assert_eq!(1, batches.len());
        assert_eq!(2, batches[0].num_rows());
        assert_eq!(1, batches[0].num_columns());

        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|arr| vec![arr.value(0), arr.value(1)])
            .unwrap_or_else(|| {
                let arr = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::StringViewArray>()
                    .expect("string view names");
                vec![arr.value(0), arr.value(1)]
            });
        assert_eq!(vec!["Bob", "Charlie"], names);

        dts_destroy_engine(engine);
    }

    #[test]
    fn insert_imports_arrow_array_stream() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let tmp = tempfile::tempdir().expect("temp dir");
        let table_path = tmp.path().join("native_insert_table");
        std::fs::create_dir(&table_path).expect("create table dir");

        let path = table_path.to_string_lossy().to_string();
        let create_body = serde_json::json!({
            "path": path,
            "schema": [
                {"name": "id", "type": "int32"},
                {"name": "name", "type": "string"}
            ]
        });
        let create_body_bytes = serde_json::to_vec(&create_body).unwrap();

        let engine = dts_create_engine();
        let create_status = runtime.block_on(unsafe {
            (*engine)
                .service
                .create_table(create_body_bytes.as_slice())
        });
        assert!(create_status.is_ok(), "create table should succeed");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec!["ten", "twenty"])),
            ],
        )
        .expect("record batch");

        let reader = arrow::record_batch::RecordBatchIterator::new(
            vec![Ok(batch)].into_iter(),
            schema,
        );
        let mut ffi_stream = FFI_ArrowArrayStream::new(Box::new(reader));

        let insert_command = CString::new(
            serde_json::json!({
                "path": path,
                "mode": "append"
            })
            .to_string(),
        )
        .expect("insert json");

        let status = dts_insert(engine, insert_command.as_ptr(), &mut ffi_stream);
        assert_eq!(1, status, "native insert should succeed");

        let read_command = CString::new(
            serde_json::json!({
                "path": path,
            })
            .to_string(),
        )
        .expect("read json");
        let mut read_stream = FFI_ArrowArrayStream::empty();
        assert_eq!(1, dts_read_table(engine, read_command.as_ptr(), &mut read_stream));
        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut read_stream) }
            .expect("import read stream");
        let batches = reader.collect::<Result<Vec<_>, _>>().expect("collect read batches");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(2, total_rows);

        dts_destroy_engine(engine);
    }

    #[test]
    fn merge_stream_imports_arrow_array_stream() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let tmp = tempfile::tempdir().expect("temp dir");
        let table_path = tmp.path().join("native_merge_table");
        std::fs::create_dir(&table_path).expect("create table dir");

        let path = table_path.to_string_lossy().to_string();
        let create_body = serde_json::json!({
            "path": path,
            "schema": [
                {"name": "id", "type": "int32"},
                {"name": "name", "type": "string"}
            ]
        });
        let create_body_bytes = serde_json::to_vec(&create_body).unwrap();

        let engine = dts_create_engine();
        let create_status = runtime.block_on(unsafe {
            (*engine)
                .service
                .create_table(create_body_bytes.as_slice())
        });
        assert!(create_status.is_ok(), "create table should succeed");

        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let target_batch = RecordBatch::try_new(
            Arc::clone(&target_schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .expect("target batch");
        let target_reader = arrow::record_batch::RecordBatchIterator::new(
            vec![Ok(target_batch)].into_iter(),
            Arc::clone(&target_schema),
        );
        let mut target_stream = FFI_ArrowArrayStream::new(Box::new(target_reader));

        let insert_command = CString::new(
            serde_json::json!({
                "path": path,
                "mode": "append"
            })
            .to_string(),
        )
        .expect("insert json");
        assert_eq!(1, dts_insert(engine, insert_command.as_ptr(), &mut target_stream));

        let merge_source_batch = RecordBatch::try_new(
            target_schema,
            vec![
                Arc::new(Int32Array::from(vec![2, 4])),
                Arc::new(StringArray::from(vec!["updated_b", "d"])),
            ],
        )
        .expect("merge source batch");
        let merge_source_reader = arrow::record_batch::RecordBatchIterator::new(
            vec![Ok(merge_source_batch)].into_iter(),
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
            ])),
        );
        let mut merge_stream = FFI_ArrowArrayStream::new(Box::new(merge_source_reader));

        let merge_command = CString::new(
            serde_json::json!({
                "operation": "merge",
                "path": path,
                "predicate": "target.id = source.id",
                "source_alias": "source",
                "target_alias": "target",
                "when_matched_update_all": true,
                "when_not_matched_insert_all": true
            })
            .to_string(),
        )
        .expect("merge json");

        let result_ptr = dts_merge_stream(engine, merge_command.as_ptr(), &mut merge_stream);
        assert!(!result_ptr.is_null(), "native merge_stream should succeed");
        dts_free_string(result_ptr);

        let read_command = CString::new(
            serde_json::json!({
                "path": path,
            })
            .to_string(),
        )
        .expect("read json");
        let mut read_stream = FFI_ArrowArrayStream::empty();
        assert_eq!(1, dts_read_table(engine, read_command.as_ptr(), &mut read_stream));
        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut read_stream) }
            .expect("import read stream");
        let batches = reader.collect::<Result<Vec<_>, _>>().expect("collect read batches");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(4, total_rows);

        dts_destroy_engine(engine);
    }

    #[test]
    fn create_table_returns_success_json() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let table_path = tmp.path().join("native_create_table");
        std::fs::create_dir(&table_path).expect("create table dir");

        let command = CString::new(
            serde_json::json!({
                "path": table_path.to_string_lossy().to_string(),
                "schema": [
                    {"name": "id", "type": "int32"},
                    {"name": "value", "type": "string"}
                ]
            })
            .to_string(),
        )
        .expect("create table json");

        let engine = dts_create_engine();
        let result_ptr = dts_create_table(engine, command.as_ptr());
        assert!(!result_ptr.is_null(), "native create_table should succeed");

        let result = unsafe { CStr::from_ptr(result_ptr) }
            .to_str()
            .expect("utf8 json")
            .to_string();
        dts_free_string(result_ptr);

        let json: serde_json::Value = serde_json::from_str(&result).expect("parse json");
        assert_eq!(json["success"], true);
        assert!(json["message"].as_str().unwrap().contains("created"));

        dts_destroy_engine(engine);
    }

    #[test]
    fn upgrade_protocol_returns_success_json() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let table_path = tmp.path().join("native_upgrade_table");
        std::fs::create_dir(&table_path).expect("create table dir");

        let create_command = CString::new(
            serde_json::json!({
                "path": table_path.to_string_lossy().to_string(),
                "schema": [
                    {"name": "id", "type": "int32"}
                ]
            })
            .to_string(),
        )
        .expect("create table json");

        let engine = dts_create_engine();
        let create_result_ptr = dts_create_table(engine, create_command.as_ptr());
        assert!(!create_result_ptr.is_null(), "native create_table should succeed");
        dts_free_string(create_result_ptr);

        let upgrade_command = CString::new(
            serde_json::json!({
                "path": table_path.to_string_lossy().to_string(),
                "reader_version": 1,
                "writer_version": 5,
                "writer_features": ["changeDataFeed"]
            })
            .to_string(),
        )
        .expect("upgrade json");

        let result_ptr = dts_upgrade_protocol(engine, upgrade_command.as_ptr());
        assert!(!result_ptr.is_null(), "native upgrade_protocol should succeed");

        let result = unsafe { CStr::from_ptr(result_ptr) }
            .to_str()
            .expect("utf8 json")
            .to_string();
        dts_free_string(result_ptr);

        let json: serde_json::Value = serde_json::from_str(&result).expect("parse json");
        assert_eq!(json["success"], true);
        assert!(json["message"].as_str().unwrap().contains("upgraded"));

        dts_destroy_engine(engine);
    }

    #[test]
    fn execute_dml_returns_success_json() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let (path, _guard) = runtime.block_on(create_native_test_table());

        let command = CString::new(
            serde_json::json!({
                "sql": "DELETE FROM native_tbl WHERE id = 2",
                "table_path": path,
                "table_name": "native_tbl"
            })
            .to_string(),
        )
        .expect("execute dml json");

        let engine = dts_create_engine();
        let result_ptr = dts_execute_dml(engine, command.as_ptr());
        assert!(!result_ptr.is_null(), "native execute_dml should succeed");

        let result = unsafe { CStr::from_ptr(result_ptr) }
            .to_str()
            .expect("utf8 json")
            .to_string();
        dts_free_string(result_ptr);

        let json: serde_json::Value = serde_json::from_str(&result).expect("parse json");
        assert_eq!(json["success"], true);
        assert!(json["message"].as_str().unwrap().contains("success"));

        let read_command = CString::new(
            serde_json::json!({
                "path": path,
            })
            .to_string(),
        )
        .expect("read json");
        let mut read_stream = FFI_ArrowArrayStream::empty();
        assert_eq!(1, dts_read_table(engine, read_command.as_ptr(), &mut read_stream));
        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut read_stream) }
            .expect("import read stream");
        let batches = reader.collect::<Result<Vec<_>, _>>().expect("collect read batches");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(2, total_rows);

        dts_destroy_engine(engine);
    }

    #[test]
    fn clone_c_string_handles_null() {
        assert!(clone_c_string(ptr::null()).is_none());
    }
}
