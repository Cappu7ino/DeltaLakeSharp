// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Minimal native ABI scaffold for the in-process V3 architecture.
//!
//! This file intentionally starts small: lifecycle, health, and error plumbing.
//! Arrow C Data / C Stream entrypoints will be layered on top of this engine in
//! subsequent changes.

use std::ffi::{CStr, CString, c_char, c_void};
use std::future::Future;
use std::ptr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Once;
use std::sync::OnceLock;

use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use tracing_subscriber::EnvFilter;

use crate::error::ServiceError;
use crate::service::DeltaService;

static INIT_TRACING: Once = Once::new();
static SHARED_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn shared_runtime() -> &'static tokio::runtime::Runtime {
    SHARED_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_stack_size(8 * 1024 * 1024)
            .build()
            .expect("creating Tokio runtime for native V3 engine should succeed")
    })
}

/// Opaque native engine handle owned by the consumer.
pub struct DeltaServiceEngine {
    service: DeltaService,
    last_error: Mutex<Option<CString>>,
    runtime_handle: tokio::runtime::Handle,
}

const ASYNC_OPERATION_PENDING: i32 = 0;
const ASYNC_OPERATION_SUCCEEDED: i32 = 1;
const ASYNC_OPERATION_FAILED: i32 = 2;
const ASYNC_OPERATION_CANCELLED: i32 = 3;

pub struct DeltaAsyncOperation {
    shared: Arc<AsyncOperationShared>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

struct AsyncOperationShared {
    state: Mutex<AsyncOperationState>,
    completion: AsyncOperationCompletion,
    operation_ptr: Mutex<usize>,
}

type AsyncOperationCompletedCallback = unsafe extern "C" fn(*mut DeltaAsyncOperation, *mut c_void);

#[derive(Clone, Copy)]
struct AsyncOperationCompletion {
    callback: Option<AsyncOperationCompletedCallback>,
    user_data: *mut c_void,
}

unsafe impl Send for AsyncOperationCompletion {}
unsafe impl Sync for AsyncOperationCompletion {}

impl AsyncOperationCompletion {
    fn none() -> Self {
        Self {
            callback: None,
            user_data: ptr::null_mut(),
        }
    }

    fn notify(self, operation: *mut DeltaAsyncOperation) {
        if let Some(callback) = self.callback {
            unsafe {
                callback(operation, self.user_data);
            }
        }
    }
}

enum AsyncOperationState {
    Pending,
    Succeeded(Option<CString>),
    Failed(CString),
    Cancelled(CString),
}

impl DeltaAsyncOperation {
    fn new(completion: AsyncOperationCompletion) -> Self {
        Self {
            shared: Arc::new(AsyncOperationShared {
                state: Mutex::new(AsyncOperationState::Pending),
                completion,
                operation_ptr: Mutex::new(0),
            }),
            task: Mutex::new(None),
        }
    }

    fn set_operation_ptr(&self, operation: *mut DeltaAsyncOperation) {
        if let Ok(mut slot) = self.shared.operation_ptr.lock() {
            *slot = operation as usize;
        }
    }

    fn set_task(&self, task: tokio::task::JoinHandle<()>) {
        if let Ok(mut slot) = self.task.lock() {
            *slot = Some(task);
        }
    }
}

fn set_async_operation_terminal_state(
    state: &Mutex<AsyncOperationState>,
    next_state: AsyncOperationState,
) -> bool {
    if let Ok(mut slot) = state.lock() {
        if matches!(*slot, AsyncOperationState::Pending) {
            *slot = next_state;
            return true;
        }
    }

    false
}

fn notify_async_operation_completion(shared: &AsyncOperationShared) {
    if let Ok(slot) = shared.operation_ptr.lock() {
        let operation = *slot as *mut DeltaAsyncOperation;
        if !operation.is_null() {
            shared.completion.notify(operation);
        }
    }
}

fn async_operation_failed(error: impl ToString) -> AsyncOperationState {
    AsyncOperationState::Failed(
        CString::new(error.to_string())
            .unwrap_or_else(|_| CString::new("native async operation failed").unwrap()),
    )
}

fn async_operation_succeeded(result: serde_json::Value) -> AsyncOperationState {
    match CString::new(result.to_string()) {
        Ok(result) => AsyncOperationState::Succeeded(Some(result)),
        Err(error) => async_operation_failed(error),
    }
}

impl DeltaServiceEngine {
    fn new() -> Self {
        INIT_TRACING.call_once(|| {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                    EnvFilter::new(
                        "info,object_store=debug,deltalake=debug,delta_table_service_v3=debug",
                    )
                }))
                .try_init();
        });

        Self {
            service: DeltaService::new(),
            last_error: Mutex::new(None),
            runtime_handle: shared_runtime().handle().clone(),
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
        self.runtime_handle.block_on(future)
    }

    fn block_on_spawn<F, T>(&self, future: F) -> Result<T, tokio::task::JoinError>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.runtime_handle
            .block_on(self.runtime_handle.spawn(future))
    }

    fn runtime_handle(&self) -> tokio::runtime::Handle {
        self.runtime_handle.clone()
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

/// Starts partition planning on the shared native runtime and returns an opaque
/// operation handle that can be polled from managed code.
#[unsafe(no_mangle)]
pub extern "C" fn dts_plan_read_partitions_async(
    engine: *mut DeltaServiceEngine,
    command_json: *const c_char,
) -> *mut DeltaAsyncOperation {
    start_plan_read_partitions_async(engine, command_json, AsyncOperationCompletion::none())
}

/// Starts partition planning and invokes `callback` after terminal state is stored.
#[unsafe(no_mangle)]
pub extern "C" fn dts_plan_read_partitions_async_with_callback(
    engine: *mut DeltaServiceEngine,
    command_json: *const c_char,
    callback: Option<AsyncOperationCompletedCallback>,
    user_data: *mut c_void,
) -> *mut DeltaAsyncOperation {
    start_plan_read_partitions_async(
        engine,
        command_json,
        AsyncOperationCompletion {
            callback,
            user_data,
        },
    )
}

fn start_plan_read_partitions_async(
    engine: *mut DeltaServiceEngine,
    command_json: *const c_char,
    completion: AsyncOperationCompletion,
) -> *mut DeltaAsyncOperation {
    start_json_async_operation(
        engine,
        command_json,
        completion,
        |service, command| async move { service.plan_read_partitions(command.as_slice()).await },
    )
}

/// Starts table creation and invokes `callback` after terminal state is stored.
#[unsafe(no_mangle)]
pub extern "C" fn dts_create_table_async_with_callback(
    engine: *mut DeltaServiceEngine,
    command_json: *const c_char,
    callback: Option<AsyncOperationCompletedCallback>,
    user_data: *mut c_void,
) -> *mut DeltaAsyncOperation {
    start_json_async_operation(
        engine,
        command_json,
        AsyncOperationCompletion {
            callback,
            user_data,
        },
        |service, command| async move { service.create_table(command.as_slice()).await },
    )
}

/// Starts protocol upgrade and invokes `callback` after terminal state is stored.
#[unsafe(no_mangle)]
pub extern "C" fn dts_upgrade_protocol_async_with_callback(
    engine: *mut DeltaServiceEngine,
    command_json: *const c_char,
    callback: Option<AsyncOperationCompletedCallback>,
    user_data: *mut c_void,
) -> *mut DeltaAsyncOperation {
    start_json_async_operation(
        engine,
        command_json,
        AsyncOperationCompletion {
            callback,
            user_data,
        },
        |service, command| async move { service.upgrade_protocol(command.as_slice()).await },
    )
}

/// Starts SQL DML execution and invokes `callback` after terminal state is stored.
#[unsafe(no_mangle)]
pub extern "C" fn dts_execute_dml_async_with_callback(
    engine: *mut DeltaServiceEngine,
    command_json: *const c_char,
    callback: Option<AsyncOperationCompletedCallback>,
    user_data: *mut c_void,
) -> *mut DeltaAsyncOperation {
    start_json_async_operation(
        engine,
        command_json,
        AsyncOperationCompletion {
            callback,
            user_data,
        },
        |service, command| async move { service.execute_dml(command.as_slice()).await },
    )
}

fn start_json_async_operation<F, Fut>(
    engine: *mut DeltaServiceEngine,
    command_json: *const c_char,
    completion: AsyncOperationCompletion,
    action: F,
) -> *mut DeltaAsyncOperation
where
    F: FnOnce(DeltaService, Vec<u8>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<serde_json::Value, ServiceError>> + Send + 'static,
{
    with_engine(engine, |engine_ref| {
        engine_ref.clear_last_error();

        if command_json.is_null() {
            engine_ref.set_last_error_message("command_json must not be null.".to_string());
            return ptr::null_mut();
        }

        let command = unsafe { CStr::from_ptr(command_json) }.to_bytes().to_vec();
        let service = engine_ref.service.clone();
        let operation = Box::new(DeltaAsyncOperation::new(completion));
        let task_shared = Arc::clone(&operation.shared);
        let operation_ptr = Box::into_raw(operation);
        unsafe {
            (*operation_ptr).set_operation_ptr(operation_ptr);
        }
        let task = engine_ref.runtime_handle.spawn(async move {
            let next_state = match action(service, command).await {
                Ok(result) => async_operation_succeeded(result),
                Err(error) => async_operation_failed(error),
            };

            if set_async_operation_terminal_state(&task_shared.state, next_state) {
                notify_async_operation_completion(&task_shared);
            }
        });

        unsafe {
            (*operation_ptr).set_task(task);
        }

        operation_ptr
    })
    .unwrap_or(ptr::null_mut())
}

/// Returns the current status for an async operation.
#[unsafe(no_mangle)]
pub extern "C" fn dts_async_operation_status(operation: *mut DeltaAsyncOperation) -> i32 {
    if operation.is_null() {
        return ASYNC_OPERATION_FAILED;
    }

    let operation_ref = unsafe { &*operation };
    let finished_without_result = operation_ref
        .task
        .lock()
        .ok()
        .and_then(|task| task.as_ref().map(tokio::task::JoinHandle::is_finished))
        .unwrap_or(false);

    operation_ref
        .shared
        .state
        .lock()
        .map(|mut state| match &*state {
            AsyncOperationState::Pending if finished_without_result => {
                *state = AsyncOperationState::Failed(
                    CString::new("Native async operation finished without a result.").unwrap(),
                );
                ASYNC_OPERATION_FAILED
            }
            AsyncOperationState::Pending => ASYNC_OPERATION_PENDING,
            AsyncOperationState::Succeeded(_) => ASYNC_OPERATION_SUCCEEDED,
            AsyncOperationState::Failed(_) => ASYNC_OPERATION_FAILED,
            AsyncOperationState::Cancelled(_) => ASYNC_OPERATION_CANCELLED,
        })
        .unwrap_or(ASYNC_OPERATION_FAILED)
}

/// Takes the successful result payload from an async operation as an owned UTF-8 string.
#[unsafe(no_mangle)]
pub extern "C" fn dts_async_operation_take_result(
    operation: *mut DeltaAsyncOperation,
) -> *mut c_char {
    if operation.is_null() {
        return ptr::null_mut();
    }

    let operation_ref = unsafe { &*operation };
    operation_ref
        .shared
        .state
        .lock()
        .ok()
        .and_then(|mut state| match &mut *state {
            AsyncOperationState::Succeeded(result) => result.take().map(CString::into_raw),
            _ => None,
        })
        .unwrap_or(ptr::null_mut())
}

/// Returns a borrowed UTF-8 error string for failed or cancelled operations.
#[unsafe(no_mangle)]
pub extern "C" fn dts_async_operation_get_error(
    operation: *mut DeltaAsyncOperation,
) -> *const c_char {
    if operation.is_null() {
        return ptr::null();
    }

    let operation_ref = unsafe { &*operation };
    operation_ref
        .shared
        .state
        .lock()
        .ok()
        .and_then(|state| match &*state {
            AsyncOperationState::Failed(error) | AsyncOperationState::Cancelled(error) => {
                Some(error.as_ptr())
            }
            _ => None,
        })
        .unwrap_or(ptr::null())
}

/// Requests cancellation of an async operation.
#[unsafe(no_mangle)]
pub extern "C" fn dts_async_operation_cancel(operation: *mut DeltaAsyncOperation) {
    if operation.is_null() {
        return;
    }

    let operation_ref = unsafe { &*operation };
    if let Ok(mut task) = operation_ref.task.lock() {
        if let Some(handle) = task.take() {
            handle.abort();
        }
    }

    if set_async_operation_terminal_state(
        &operation_ref.shared.state,
        AsyncOperationState::Cancelled(
            CString::new("Native async operation was cancelled.").unwrap(),
        ),
    ) {
        notify_async_operation_completion(&operation_ref.shared);
    }
}

/// Destroys a native async operation handle.
#[unsafe(no_mangle)]
pub extern "C" fn dts_async_operation_destroy(operation: *mut DeltaAsyncOperation) {
    if operation.is_null() {
        return;
    }

    let operation = unsafe { Box::from_raw(operation) };
    if let Ok(mut slot) = operation.shared.operation_ptr.lock() {
        *slot = 0;
    }
    if let Ok(mut task) = operation.task.lock() {
        if let Some(handle) = task.take() {
            handle.abort();
        }
    }
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

        let command = unsafe { CStr::from_ptr(command_json) }.to_bytes().to_vec();
        let reader = match unsafe { ArrowArrayStreamReader::from_raw(source_stream) } {
            Ok(reader) => reader,
            Err(error) => {
                engine_ref.set_last_error_message(error.to_string());
                return ptr::null_mut();
            }
        };

        let service = engine_ref.service.clone();
        let result = match engine_ref
            .block_on_spawn(async move { service.merge_reader(&command, reader).await })
        {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => {
                engine_ref.set_last_error_message(error.to_string());
                return ptr::null_mut();
            }
            Err(error) => {
                engine_ref.set_last_error_message(format!("Native merge task failed: {error}"));
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
    use std::sync::atomic::{AtomicBool, Ordering};

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
    fn multiple_engines_share_runtime_and_remain_healthy() {
        let engines: Vec<*mut DeltaServiceEngine> = (0..16).map(|_| dts_create_engine()).collect();

        for &engine in &engines {
            assert_eq!(1, dts_health_check(engine));
            assert!(dts_get_last_error(engine).is_null());
        }

        for engine in engines {
            dts_destroy_engine(engine);
        }
    }

    #[test]
    fn destroying_one_engine_does_not_invalidate_another() {
        let first = dts_create_engine();
        let second = dts_create_engine();

        assert_eq!(1, dts_health_check(first));
        assert_eq!(1, dts_health_check(second));

        dts_destroy_engine(first);

        assert_eq!(1, dts_health_check(second));
        assert!(dts_get_last_error(second).is_null());

        dts_destroy_engine(second);
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

    fn wait_for_async_operation(operation: *mut DeltaAsyncOperation) -> i32 {
        for _ in 0..200 {
            let status = dts_async_operation_status(operation);
            if status != ASYNC_OPERATION_PENDING {
                return status;
            }

            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        dts_async_operation_status(operation)
    }

    #[test]
    fn async_plan_read_partitions_rejects_null_command() {
        let engine = dts_create_engine();
        let operation = dts_plan_read_partitions_async(engine, ptr::null());
        assert!(operation.is_null());
        assert!(!dts_get_last_error(engine).is_null());
        dts_destroy_engine(engine);
    }

    #[test]
    fn async_plan_read_partitions_returns_result_json() {
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
        let operation = dts_plan_read_partitions_async(engine, command.as_ptr());
        assert!(!operation.is_null(), "async operation should be created");

        assert_eq!(
            ASYNC_OPERATION_SUCCEEDED,
            wait_for_async_operation(operation)
        );
        let result_ptr = dts_async_operation_take_result(operation);
        assert!(!result_ptr.is_null(), "async result should be available");
        assert!(
            dts_async_operation_take_result(operation).is_null(),
            "result is single-use"
        );

        let result = unsafe { CStr::from_ptr(result_ptr) }
            .to_str()
            .expect("utf8 json")
            .to_string();
        dts_free_string(result_ptr);

        let json: serde_json::Value = serde_json::from_str(&result).expect("parse json");
        assert_eq!(json["success"], true);
        assert_eq!(1, json["result"].as_array().expect("result array").len());

        dts_async_operation_destroy(operation);
        dts_destroy_engine(engine);
    }

    #[test]
    fn async_plan_read_partitions_failure_exposes_operation_error() {
        let command = CString::new(
            serde_json::json!({
                "path": "/definitely/missing/native/async/table",
            })
            .to_string(),
        )
        .expect("command json");

        let engine = dts_create_engine();
        let operation = dts_plan_read_partitions_async(engine, command.as_ptr());
        assert!(!operation.is_null(), "async operation should be created");

        assert_eq!(ASYNC_OPERATION_FAILED, wait_for_async_operation(operation));
        assert!(!dts_async_operation_get_error(operation).is_null());
        assert!(dts_async_operation_take_result(operation).is_null());

        dts_async_operation_destroy(operation);
        dts_destroy_engine(engine);
    }

    unsafe extern "C" fn mark_async_operation_completed(
        operation: *mut DeltaAsyncOperation,
        user_data: *mut c_void,
    ) {
        assert!(!operation.is_null());
        assert_eq!(
            ASYNC_OPERATION_SUCCEEDED,
            dts_async_operation_status(operation)
        );
        let notified = unsafe { &*(user_data as *const AtomicBool) };
        notified.store(true, Ordering::SeqCst);
    }

    unsafe extern "C" fn mark_async_operation_notified(
        _operation: *mut DeltaAsyncOperation,
        user_data: *mut c_void,
    ) {
        let notified = unsafe { &*(user_data as *const AtomicBool) };
        notified.store(true, Ordering::SeqCst);
    }

    #[test]
    fn async_plan_read_partitions_callback_notifies_after_success() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let (path, _guard) = runtime.block_on(create_native_test_table());
        let notified = AtomicBool::new(false);

        let command = CString::new(
            serde_json::json!({
                "path": path,
            })
            .to_string(),
        )
        .expect("command json");

        let engine = dts_create_engine();
        let operation = dts_plan_read_partitions_async_with_callback(
            engine,
            command.as_ptr(),
            Some(mark_async_operation_completed),
            &notified as *const AtomicBool as *mut c_void,
        );
        assert!(!operation.is_null(), "async operation should be created");

        assert_eq!(
            ASYNC_OPERATION_SUCCEEDED,
            wait_for_async_operation(operation)
        );
        for _ in 0..200 {
            if notified.load(Ordering::SeqCst) {
                break;
            }

            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(notified.load(Ordering::SeqCst));

        dts_async_operation_destroy(operation);
        dts_destroy_engine(engine);
    }

    #[test]
    fn async_operation_cancel_marks_pending_operation_cancelled() {
        let task = shared_runtime().handle().spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        });
        let operation = Box::into_raw(Box::new(DeltaAsyncOperation::new(
            AsyncOperationCompletion::none(),
        )));
        unsafe {
            (*operation).set_operation_ptr(operation);
            (*operation).set_task(task);
        }

        dts_async_operation_cancel(operation);

        assert_eq!(
            ASYNC_OPERATION_CANCELLED,
            dts_async_operation_status(operation)
        );
        assert!(!dts_async_operation_get_error(operation).is_null());
        dts_async_operation_destroy(operation);
    }

    #[test]
    fn async_operation_cancel_notifies_real_pending_json_operation() {
        let notified = AtomicBool::new(false);
        let command = CString::new("{}").expect("command json");
        let engine = dts_create_engine();
        let operation = start_json_async_operation(
            engine,
            command.as_ptr(),
            AsyncOperationCompletion {
                callback: Some(mark_async_operation_notified),
                user_data: &notified as *const AtomicBool as *mut c_void,
            },
            |_service, _command| async move {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                Ok(serde_json::json!({ "success": true }))
            },
        );
        assert!(!operation.is_null(), "async operation should be created");

        dts_async_operation_cancel(operation);

        assert_eq!(
            ASYNC_OPERATION_CANCELLED,
            dts_async_operation_status(operation)
        );
        assert!(!dts_async_operation_get_error(operation).is_null());
        assert!(dts_async_operation_take_result(operation).is_null());
        assert!(notified.load(Ordering::SeqCst));

        dts_async_operation_destroy(operation);
        dts_destroy_engine(engine);
    }

    #[test]
    fn async_operation_destroy_aborts_pending_operation() {
        let task = shared_runtime().handle().spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        });
        let operation = Box::into_raw(Box::new(DeltaAsyncOperation::new(
            AsyncOperationCompletion::none(),
        )));
        unsafe {
            (*operation).set_operation_ptr(operation);
            (*operation).set_task(task);
        }

        dts_async_operation_destroy(operation);
    }

    #[test]
    fn async_operation_destroy_suppresses_late_completion_callback() {
        let notified = AtomicBool::new(false);
        let command = CString::new("{}").expect("command json");
        let engine = dts_create_engine();
        let operation = start_json_async_operation(
            engine,
            command.as_ptr(),
            AsyncOperationCompletion {
                callback: Some(mark_async_operation_notified),
                user_data: &notified as *const AtomicBool as *mut c_void,
            },
            |_service, _command| async move {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                Ok(serde_json::json!({ "success": true }))
            },
        );
        assert!(!operation.is_null(), "async operation should be created");

        dts_async_operation_destroy(operation);
        std::thread::sleep(std::time::Duration::from_millis(100));

        assert!(!notified.load(Ordering::SeqCst));
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
                "batch_size": 1,
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

        let status = dts_execute_query(engine, command.as_ptr(), &mut ffi_stream);
        assert_eq!(1, status, "native execute_query should succeed");

        let reader =
            unsafe { ArrowArrayStreamReader::from_raw(&mut ffi_stream) }.expect("import stream");
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
    fn execute_query_arrow_array_stream_can_be_released_early() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let (path, _guard) = runtime.block_on(create_native_test_table());

        let command = CString::new(
            serde_json::json!({
                "sql": "SELECT id, name FROM tbl ORDER BY id",
                "table_path": path,
                "table_name": "tbl",
                "batch_size": 1,
            })
            .to_string(),
        )
        .expect("command json");

        let engine = dts_create_engine();
        let mut ffi_stream = FFI_ArrowArrayStream::empty();

        let status = dts_execute_query(engine, command.as_ptr(), &mut ffi_stream);
        assert_eq!(1, status, "native execute_query should succeed");

        let mut reader =
            unsafe { ArrowArrayStreamReader::from_raw(&mut ffi_stream) }.expect("import stream");
        let first_batch = reader
            .next()
            .expect("stream should produce first batch")
            .expect("first batch should be readable");
        assert_eq!(1, first_batch.num_rows());
        drop(reader);

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

        let reader =
            unsafe { ArrowArrayStreamReader::from_raw(&mut ffi_stream) }.expect("import stream");
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
        let create_status = runtime
            .block_on(unsafe { (*engine).service.create_table(create_body_bytes.as_slice()) });
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

        let reader =
            arrow::record_batch::RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
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
        assert_eq!(
            1,
            dts_read_table(engine, read_command.as_ptr(), &mut read_stream)
        );
        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut read_stream) }
            .expect("import read stream");
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .expect("collect read batches");
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
        let create_status = runtime
            .block_on(unsafe { (*engine).service.create_table(create_body_bytes.as_slice()) });
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
        assert_eq!(
            1,
            dts_insert(engine, insert_command.as_ptr(), &mut target_stream)
        );

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
        assert_eq!(
            1,
            dts_read_table(engine, read_command.as_ptr(), &mut read_stream)
        );
        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut read_stream) }
            .expect("import read stream");
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .expect("collect read batches");
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
        assert!(
            !create_result_ptr.is_null(),
            "native create_table should succeed"
        );
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
        assert!(
            !result_ptr.is_null(),
            "native upgrade_protocol should succeed"
        );

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
        assert_eq!(
            1,
            dts_read_table(engine, read_command.as_ptr(), &mut read_stream)
        );
        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut read_stream) }
            .expect("import read stream");
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .expect("collect read batches");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(2, total_rows);

        dts_destroy_engine(engine);
    }

    #[test]
    fn clone_c_string_handles_null() {
        assert!(clone_c_string(ptr::null()).is_none());
    }
}
