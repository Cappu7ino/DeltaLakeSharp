// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use super::partitioning::{
    decode_partition_token, encode_partition_token, resolve_partition_files,
};
use super::*;
use crate::service::request::PartitionPredicateKey;
use arrow::array::{Int32Array, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field};
use arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use url::Url;

async fn create_test_delta_table() -> (String, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("failed to create temp dir");
    let table_path = tmp.path().join("test_table");
    std::fs::create_dir(&table_path).expect("failed to create table dir");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .expect("failed to create RecordBatch");

    let url = Url::from_file_path(&table_path).expect("failed to convert path to URL");
    let table: deltalake::DeltaTable = deltalake::DeltaTable::try_from_url(url)
        .await
        .expect("DeltaTable::try_from_url failed");
    let _table: deltalake::DeltaTable = table
        .write(vec![batch])
        .await
        .expect("write to delta table failed");

    let path_str = table_path.to_str().expect("non-UTF8 path");
    (path_str.to_string(), tmp)
}

async fn create_multi_file_non_partitioned_table() -> (String, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("failed to create temp dir");
    let table_path = tmp.path().join("multi_file_table");
    std::fs::create_dir(&table_path).expect("failed to create table dir");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let url = Url::from_file_path(&table_path).expect("failed to convert path to URL");
    let mut table: deltalake::DeltaTable = deltalake::DeltaTable::try_from_url(url)
        .await
        .expect("DeltaTable::try_from_url failed");

    for i in 1..=8 {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![i])),
                Arc::new(StringArray::from(vec![format!("row_{i}")])),
            ],
        )
        .expect("failed to create RecordBatch");

        table = table
            .write(vec![batch])
            .await
            .expect("write to delta table failed");
    }

    let path_str = table_path.to_str().expect("non-UTF8 path");
    (path_str.to_string(), tmp)
}

fn read_command_bytes(path: &str, num_rows: Option<u64>) -> Vec<u8> {
    let mut map = serde_json::Map::new();
    map.insert(
        "path".to_string(),
        serde_json::Value::String(path.to_string()),
    );
    if let Some(n) = num_rows {
        map.insert("num_rows".to_string(), serde_json::Value::Number(n.into()));
    }
    serde_json::to_vec(&map).expect("failed to serialize read command")
}

fn sql_command_bytes(sql: &str, table_path: Option<&str>, table_name: Option<&str>) -> Vec<u8> {
    let mut map = serde_json::Map::new();
    map.insert(
        "sql".to_string(),
        serde_json::Value::String(sql.to_string()),
    );
    if let Some(tp) = table_path {
        map.insert(
            "table_path".to_string(),
            serde_json::Value::String(tp.to_string()),
        );
    }
    if let Some(tn) = table_name {
        map.insert(
            "table_name".to_string(),
            serde_json::Value::String(tn.to_string()),
        );
    }
    serde_json::to_vec(&map).expect("failed to serialize sql command")
}

fn sql_command_bytes_with_batch_size(
    sql: &str,
    table_path: Option<&str>,
    table_name: Option<&str>,
    batch_size: usize,
) -> Vec<u8> {
    let mut map = serde_json::Map::new();
    map.insert(
        "sql".to_string(),
        serde_json::Value::String(sql.to_string()),
    );
    map.insert(
        "batch_size".to_string(),
        serde_json::Value::Number(serde_json::Number::from(batch_size as u64)),
    );
    if let Some(tp) = table_path {
        map.insert(
            "table_path".to_string(),
            serde_json::Value::String(tp.to_string()),
        );
    }
    if let Some(tn) = table_name {
        map.insert(
            "table_name".to_string(),
            serde_json::Value::String(tn.to_string()),
        );
    }
    serde_json::to_vec(&map).expect("failed to serialize sql command")
}

async fn collect_batches(command: &[u8]) -> Vec<RecordBatch> {
    let parsed = Command::parse(command).unwrap();
    let (_schema, mut stream) = match parsed {
        Command::Read(read_cmd) => execute_read_table_stream(read_cmd).await.unwrap(),
        Command::Sql(sql_cmd) => execute_sql_stream(sql_cmd).await.unwrap(),
    };

    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch.unwrap());
    }
    batches
}

async fn collect_read_batches(cmd: ReadCommand) -> Vec<RecordBatch> {
    let (_schema, mut stream) = execute_read_table_stream(cmd).await.unwrap();

    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch.unwrap());
    }
    batches
}

fn ids_from_batches(batches: &[RecordBatch]) -> Vec<i32> {
    let mut ids = Vec::new();
    for batch in batches {
        let col = batch.column(0);
        if let Some(id_col) = col.as_any().downcast_ref::<Int32Array>() {
            for i in 0..batch.num_rows() {
                ids.push(id_col.value(i));
            }
        } else if let Some(id_col) = col.as_any().downcast_ref::<Int64Array>() {
            for i in 0..batch.num_rows() {
                ids.push(i32::try_from(id_col.value(i)).expect("id should fit into i32"));
            }
        } else {
            panic!(
                "id column should be Int32Array or Int64Array, got {:?}",
                col.data_type()
            );
        }
    }
    ids.sort();
    ids
}

fn predicate_partition_token(version: i64, values: &[(&str, Option<&str>)]) -> String {
    let descriptor = crate::service::request::PartitionDescriptorPayload {
        version,
        ordinal: 0,
        total_partitions: 1,
        mode: PartitionDescriptorMode::PartitionPredicate {
            keys: vec![PartitionPredicateKey {
                values: values
                    .iter()
                    .map(|(key, value)| ((*key).to_string(), value.map(|value| value.to_string())))
                    .collect::<HashMap<_, _>>(),
            }],
        },
    };

    encode_partition_token(&descriptor).expect("predicate token should encode")
}

fn single_id_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
}

fn id_batch(values: Vec<i32>) -> RecordBatch {
    RecordBatch::try_new(single_id_schema(), vec![Arc::new(Int32Array::from(values))])
        .expect("id batch should be valid")
}

fn prefetch_test_stream(
    schema: SchemaRef,
    items: Vec<Result<RecordBatch, DataFusionError>>,
) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(items),
    ))
}

fn pending_after_batch_stream(schema: SchemaRef, batch: RecordBatch) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(vec![Ok(batch)]).chain(futures::stream::pending()),
    ))
}

async fn wait_for_available_permits(limit: &Semaphore, expected: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while limit.available_permits() != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("semaphore permit count should reach expected value");
}

async fn wait_for_prefetched_batches(reader: &PrefetchingRecordBatchReader, expected: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while reader.batches.len() != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reader should reach expected prefetch queue depth");
}

#[tokio::test]
async fn record_batch_reader_direct_mode_drains_batches_in_order() {
    let schema = single_id_schema();
    let stream = prefetch_test_stream(
        Arc::clone(&schema),
        vec![Ok(id_batch(vec![1, 2])), Ok(id_batch(vec![3]))],
    );
    let reader = create_record_batch_reader_with_prefetch(
        schema,
        tokio::runtime::Handle::current(),
        stream,
        false,
    );

    let batches = tokio::task::spawn_blocking(move || reader.collect::<Result<Vec<_>, _>>())
        .await
        .expect("blocking reader task should complete")
        .expect("direct reader should drain without errors");

    assert_eq!(ids_from_batches(&batches), vec![1, 2, 3]);
}

#[tokio::test]
async fn prefetch_reader_drains_batches_in_order() {
    let schema = single_id_schema();
    let stream = prefetch_test_stream(
        Arc::clone(&schema),
        vec![Ok(id_batch(vec![1, 2])), Ok(id_batch(vec![3]))],
    );
    let reader = PrefetchingRecordBatchReader::new_with_limit(
        schema,
        tokio::runtime::Handle::current(),
        stream,
        Arc::new(Semaphore::new(1)),
    );

    let batches = tokio::task::spawn_blocking(move || reader.collect::<Result<Vec<_>, _>>())
        .await
        .expect("blocking reader task should complete")
        .expect("prefetch reader should drain without errors");

    assert_eq!(ids_from_batches(&batches), vec![1, 2, 3]);
}

#[tokio::test]
async fn prefetch_reader_propagates_producer_error() {
    let schema = single_id_schema();
    let stream = prefetch_test_stream(
        Arc::clone(&schema),
        vec![Err(DataFusionError::Execution(
            "planned producer failure".to_string(),
        ))],
    );
    let reader = PrefetchingRecordBatchReader::new_with_limit(
        schema,
        tokio::runtime::Handle::current(),
        stream,
        Arc::new(Semaphore::new(1)),
    );

    let error = tokio::task::spawn_blocking(move || reader.collect::<Result<Vec<_>, _>>())
        .await
        .expect("blocking reader task should complete")
        .expect_err("prefetch reader should surface producer errors");

    match error {
        ArrowError::ExternalError(error) => {
            assert!(error.to_string().contains("planned producer failure"));
        }
        other => panic!("expected external producer error, got {other:?}"),
    }
}

#[tokio::test]
async fn prefetch_reader_drop_releases_producer_permit() {
    let schema = single_id_schema();
    let limit = Arc::new(Semaphore::new(1));
    let stream = Box::pin(RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        futures::stream::pending::<Result<RecordBatch, DataFusionError>>(),
    ));
    let reader = PrefetchingRecordBatchReader::new_with_limit(
        schema,
        tokio::runtime::Handle::current(),
        stream,
        Arc::clone(&limit),
    );

    wait_for_available_permits(&limit, 0).await;
    drop(reader);

    let permit = tokio::time::timeout(Duration::from_secs(1), limit.acquire_owned())
        .await
        .expect("producer permit should be released after reader drop")
        .expect("test semaphore should remain open");
    drop(permit);
}

#[tokio::test]
async fn prefetch_reader_full_queue_does_not_starve_later_stream() {
    let schema = single_id_schema();
    let limit = Arc::new(Semaphore::new(1));
    let first_stream = prefetch_test_stream(
        Arc::clone(&schema),
        vec![
            Ok(id_batch(vec![1])),
            Ok(id_batch(vec![2])),
            Ok(id_batch(vec![3])),
        ],
    );
    let first_reader = PrefetchingRecordBatchReader::new_with_limit(
        Arc::clone(&schema),
        tokio::runtime::Handle::current(),
        first_stream,
        Arc::clone(&limit),
    );

    wait_for_prefetched_batches(&first_reader, DEFAULT_RECORD_BATCH_PREFETCH).await;
    wait_for_available_permits(&limit, 1).await;

    let second_stream = prefetch_test_stream(Arc::clone(&schema), vec![Ok(id_batch(vec![42]))]);
    let second_reader = PrefetchingRecordBatchReader::new_with_limit(
        schema,
        tokio::runtime::Handle::current(),
        second_stream,
        Arc::clone(&limit),
    );

    let batches = tokio::time::timeout(
        Duration::from_secs(1),
        tokio::task::spawn_blocking(move || second_reader.collect::<Result<Vec<_>, _>>()),
    )
    .await
    .expect("later stream should make progress while earlier queue is full")
    .expect("blocking reader task should complete")
    .expect("later stream should drain without errors");

    assert_eq!(ids_from_batches(&batches), vec![42]);
    drop(first_reader);
}

#[tokio::test]
async fn prefetch_reader_concurrent_streams_drain_with_shared_limit() {
    let schema = single_id_schema();
    let limit = Arc::new(Semaphore::new(2));
    let mut tasks = Vec::new();

    for stream_index in 0..8 {
        let first_id = stream_index * 10 + 1;
        let stream = prefetch_test_stream(
            Arc::clone(&schema),
            vec![
                Ok(id_batch(vec![first_id])),
                Ok(id_batch(vec![first_id + 1])),
            ],
        );
        let reader = PrefetchingRecordBatchReader::new_with_limit(
            Arc::clone(&schema),
            tokio::runtime::Handle::current(),
            stream,
            Arc::clone(&limit),
        );

        tasks.push(tokio::task::spawn_blocking(move || {
            reader.collect::<Result<Vec<_>, _>>()
        }));
    }

    let mut ids = Vec::new();
    for task in tasks {
        let batches = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("concurrent reader should not hang")
            .expect("blocking reader task should complete")
            .expect("concurrent reader should drain successfully");
        ids.extend(ids_from_batches(&batches));
    }

    ids.sort();
    assert_eq!(
        ids,
        vec![1, 2, 11, 12, 21, 22, 31, 32, 41, 42, 51, 52, 61, 62, 71, 72]
    );
    wait_for_available_permits(&limit, 2).await;
}

#[tokio::test]
async fn prefetch_reader_drop_after_first_batch_stops_pending_producer() {
    let schema = single_id_schema();
    let limit = Arc::new(Semaphore::new(1));
    let stream = pending_after_batch_stream(Arc::clone(&schema), id_batch(vec![7]));
    let reader = PrefetchingRecordBatchReader::new_with_limit(
        schema,
        tokio::runtime::Handle::current(),
        stream,
        Arc::clone(&limit),
    );

    let ids = tokio::time::timeout(
        Duration::from_secs(1),
        tokio::task::spawn_blocking(move || {
            let batch = reader
                .take(1)
                .collect::<Result<Vec<_>, _>>()
                .expect("first batch should be readable");
            ids_from_batches(&batch)
        }),
    )
    .await
    .expect("partial reader should not hang")
    .expect("blocking reader task should complete");

    assert_eq!(ids, vec![7]);
    let permit = tokio::time::timeout(Duration::from_secs(1), limit.acquire_owned())
        .await
        .expect("producer permit should be available after partial drop")
        .expect("test semaphore should remain open");
    drop(permit);
}

#[tokio::test]
async fn prefetch_reader_error_does_not_block_other_streams() {
    let schema = single_id_schema();
    let limit = Arc::new(Semaphore::new(1));
    let success_stream = prefetch_test_stream(Arc::clone(&schema), vec![Ok(id_batch(vec![10]))]);
    let error_stream = prefetch_test_stream(
        Arc::clone(&schema),
        vec![Err(DataFusionError::Execution(
            "isolated producer failure".to_string(),
        ))],
    );
    let later_success_stream =
        prefetch_test_stream(Arc::clone(&schema), vec![Ok(id_batch(vec![20]))]);

    let success_reader = PrefetchingRecordBatchReader::new_with_limit(
        Arc::clone(&schema),
        tokio::runtime::Handle::current(),
        success_stream,
        Arc::clone(&limit),
    );
    let error_reader = PrefetchingRecordBatchReader::new_with_limit(
        Arc::clone(&schema),
        tokio::runtime::Handle::current(),
        error_stream,
        Arc::clone(&limit),
    );
    let later_success_reader = PrefetchingRecordBatchReader::new_with_limit(
        schema,
        tokio::runtime::Handle::current(),
        later_success_stream,
        Arc::clone(&limit),
    );

    let success_task =
        tokio::task::spawn_blocking(move || success_reader.collect::<Result<Vec<_>, _>>());
    let error_task =
        tokio::task::spawn_blocking(move || error_reader.collect::<Result<Vec<_>, _>>());
    let later_success_task =
        tokio::task::spawn_blocking(move || later_success_reader.collect::<Result<Vec<_>, _>>());

    let success_batches = tokio::time::timeout(Duration::from_secs(2), success_task)
        .await
        .expect("success reader should not hang")
        .expect("success reader task should complete")
        .expect("success reader should drain");
    let error = tokio::time::timeout(Duration::from_secs(2), error_task)
        .await
        .expect("error reader should not hang")
        .expect("error reader task should complete")
        .expect_err("error reader should surface its producer error");
    let later_success_batches = tokio::time::timeout(Duration::from_secs(2), later_success_task)
        .await
        .expect("later success reader should not hang")
        .expect("later success reader task should complete")
        .expect("later success reader should drain");

    assert_eq!(ids_from_batches(&success_batches), vec![10]);
    assert_eq!(ids_from_batches(&later_success_batches), vec![20]);
    match error {
        ArrowError::ExternalError(error) => {
            assert!(error.to_string().contains("isolated producer failure"));
        }
        other => panic!("expected external producer error, got {other:?}"),
    }
    wait_for_available_permits(&limit, 1).await;
}

#[tokio::test]
async fn resolve_schema_read_table_returns_schema() {
    let (path, _guard) = create_test_delta_table().await;
    let schema = resolve_schema_from_command_bytes(&read_command_bytes(&path, None))
        .await
        .unwrap();

    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "name");
}

#[tokio::test]
async fn resolve_schema_sql_returns_empty_schema() {
    let schema = resolve_schema_from_command_bytes(&sql_command_bytes("SELECT 1 AS x", None, None))
        .await
        .unwrap();
    assert_eq!(schema.fields().len(), 0);
}

#[tokio::test]
async fn read_batches_read_table_returns_all_rows() {
    let (path, _guard) = create_test_delta_table().await;
    let batches = collect_batches(&read_command_bytes(&path, None)).await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn read_batches_read_table_with_limit() {
    let (path, _guard) = create_test_delta_table().await;
    let batches = collect_batches(&read_command_bytes(&path, Some(2))).await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn read_batches_sql_select_literal() {
    let batches = collect_batches(&sql_command_bytes("SELECT 42 AS answer", None, None)).await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert_eq!(batches[0].schema().field(0).name(), "answer");
}

#[tokio::test]
async fn read_batches_sql_with_registered_table() {
    let (path, _guard) = create_test_delta_table().await;
    let batches = collect_batches(&sql_command_bytes(
        "SELECT id FROM tbl WHERE id > 1",
        Some(&path),
        Some("tbl"),
    ))
    .await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    assert_eq!(batches[0].schema().fields().len(), 1);
}

#[tokio::test]
async fn read_batches_sql_with_batch_size_honors_max_batch_length() {
    let (path, _guard) = create_test_delta_table().await;
    let batches = collect_batches(&sql_command_bytes_with_batch_size(
        "SELECT id, name FROM tbl ORDER BY id",
        Some(&path),
        Some("tbl"),
        1,
    ))
    .await;

    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    assert!(
        batches.len() >= 3,
        "Expected one-row batches when batch_size=1."
    );
    assert!(batches.iter().all(|b| b.num_rows() <= 1));
}

#[tokio::test]
async fn resolve_schema_invalid_path_returns_error() {
    let result = resolve_schema_from_command_bytes(&read_command_bytes(
        "/nonexistent/path/to/nowhere",
        None,
    ))
    .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn read_batches_invalid_json_returns_error() {
    let runtime = tokio::runtime::Handle::current();
    let result = resolve_batch_reader_from_command_bytes(b"not valid json", runtime).await;
    assert!(result.is_err());
}

async fn create_time_travel_table() -> (String, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("failed to create temp dir");
    let table_path = tmp.path().join("tt_table");
    std::fs::create_dir(&table_path).expect("failed to create table dir");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let batch0 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["v0_a", "v0_b"])),
        ],
    )
    .expect("batch0");

    let url = Url::from_file_path(&table_path).expect("url");
    let table: deltalake::DeltaTable = deltalake::DeltaTable::try_from_url(url)
        .await
        .expect("try_from_url");
    let table: deltalake::DeltaTable = table.write(vec![batch0]).await.expect("write v0");

    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["v1_c", "v1_d"])),
        ],
    )
    .expect("batch1");

    let _table: deltalake::DeltaTable = table
        .write(vec![batch1])
        .with_save_mode(deltalake::protocol::SaveMode::Append)
        .await
        .expect("write v1");

    let path_str = table_path.to_str().expect("non-UTF8 path");
    (path_str.to_string(), tmp)
}

fn read_command_bytes_versioned(path: &str, version: i64) -> Vec<u8> {
    let mut map = serde_json::Map::new();
    map.insert(
        "path".to_string(),
        serde_json::Value::String(path.to_string()),
    );
    map.insert(
        "version".to_string(),
        serde_json::Value::Number(version.into()),
    );
    serde_json::to_vec(&map).expect("serialize")
}

#[tokio::test]
async fn read_batches_time_travel_version_0_returns_2_rows() {
    let (path, _guard) = create_time_travel_table().await;
    let batches = collect_batches(&read_command_bytes_versioned(&path, 0)).await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

#[tokio::test]
async fn read_batches_time_travel_version_1_returns_4_rows() {
    let (path, _guard) = create_time_travel_table().await;
    let batches = collect_batches(&read_command_bytes_versioned(&path, 1)).await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);
}

#[tokio::test]
async fn read_batches_time_travel_latest_returns_4_rows() {
    let (path, _guard) = create_time_travel_table().await;
    let batches = collect_batches(&read_command_bytes(&path, None)).await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);
}

async fn create_partitioned_table() -> (String, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("temp dir");
    let table_path = tmp.path().join("part_table");
    std::fs::create_dir(&table_path).expect("create table dir");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("region", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            Arc::new(StringArray::from(vec!["us", "eu", "us", "eu", "us"])),
        ],
    )
    .expect("batch");

    let url = Url::from_file_path(&table_path).expect("url");
    let table: deltalake::DeltaTable = deltalake::DeltaTable::try_from_url(url)
        .await
        .expect("try_from_url");
    let _table: deltalake::DeltaTable = table
        .write(vec![batch])
        .with_partition_columns(vec!["region"])
        .await
        .expect("write partitioned");

    let path_str = table_path.to_str().expect("non-UTF8 path");
    (path_str.to_string(), tmp)
}

async fn create_skewed_partitioned_table() -> (String, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("temp dir");
    let table_path = tmp.path().join("skewed_part_table");
    std::fs::create_dir(&table_path).expect("create table dir");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("region", DataType::Utf8, false),
    ]));

    let url = Url::from_file_path(&table_path).expect("url");
    let mut table: deltalake::DeltaTable = deltalake::DeltaTable::try_from_url(url)
        .await
        .expect("try_from_url");

    for id in 1..=10 {
        let region = if id <= 9 { "us" } else { "eu" };
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![id])),
                Arc::new(StringArray::from(vec![format!("row_{id}")])),
                Arc::new(StringArray::from(vec![region])),
            ],
        )
        .expect("batch");

        let writer = table
            .write(vec![batch])
            .with_partition_columns(vec!["region"]);
        table = if id == 1 {
            writer.await.expect("write initial partitioned row")
        } else {
            writer
                .with_save_mode(deltalake::protocol::SaveMode::Append)
                .await
                .expect("append partitioned row")
        };
    }

    let path_str = table_path.to_str().expect("non-UTF8 path");
    (path_str.to_string(), tmp)
}

#[tokio::test]
async fn read_batches_partitioned_table_returns_all_rows() {
    let (path, _guard) = create_partitioned_table().await;
    let batches = collect_batches(&read_command_bytes(&path, None)).await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);

    if let Some(batch) = batches.first() {
        let schema = batch.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"region"));
    }
}

#[tokio::test]
async fn read_batches_partitioned_table_sql_filter_on_partition() {
    let (path, _guard) = create_partitioned_table().await;
    let batches = collect_batches(&sql_command_bytes(
        "SELECT id, name FROM tbl WHERE region = 'us'",
        Some(&path),
        Some("tbl"),
    ))
    .await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
}

fn fixture_path(name: &str) -> String {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.parent())
        .expect("Cannot resolve repo root from CARGO_MANIFEST_DIR");
    let path = repo_root
        .join("tests")
        .join("DeltaLakeSharp.Tests")
        .join("data")
        .join(name);
    assert!(path.exists(), "Fixture not found at {}", path.display());
    path.to_str().expect("non-UTF8 fixture path").to_string()
}

#[tokio::test]
async fn fixture_column_mapping_get_schema_returns_logical_names() {
    let path = fixture_path("delta_test_column_mapping_name");
    let schema = resolve_schema_from_command_bytes(&read_command_bytes(&path, None))
        .await
        .unwrap();

    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(*schema.field(0).data_type(), DataType::Int32);
    assert_eq!(schema.field(1).name(), "city");
    assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
}

#[tokio::test]
async fn fixture_column_mapping_read_returns_3_rows() {
    let path = fixture_path("delta_test_column_mapping_name");
    let batches = collect_batches(&read_command_bytes(&path, None)).await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    if let Some(batch) = batches.first() {
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(1).name(), "city");
    }
}

#[tokio::test]
async fn fixture_column_mapping_read_returns_correct_data() {
    let path = fixture_path("delta_test_column_mapping_name");
    let batches = collect_batches(&read_command_bytes(&path, None)).await;

    let mut rows: Vec<(i32, String)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column should be Int32Array");
        let cities: Vec<String> = (0..batch.num_rows())
            .map(|i| {
                let col = batch.column(1);
                if let Some(sa) = col.as_any().downcast_ref::<StringArray>() {
                    sa.value(i).to_string()
                } else if let Some(sva) = col.as_any().downcast_ref::<StringViewArray>() {
                    sva.value(i).to_string()
                } else {
                    panic!(
                        "Unexpected array type for city column: {:?}",
                        col.data_type()
                    );
                }
            })
            .collect();

        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), cities[i].clone()));
        }
    }

    rows.sort_by_key(|(id, _)| *id);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (1, "Seattle".to_string()));
    assert_eq!(rows[1], (2, "Portland".to_string()));
    assert_eq!(rows[2], (3, "Denver".to_string()));
}

#[tokio::test]
async fn fixture_column_mapping_sql_query_works() {
    let path = fixture_path("delta_test_column_mapping_name");
    let batches = collect_batches(&sql_command_bytes(
        "SELECT id, city FROM tbl WHERE id >= 2 ORDER BY id",
        Some(&path),
        Some("tbl"),
    ))
    .await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

#[tokio::test]
async fn fixture_deletion_vector_get_schema() {
    let path = fixture_path("delta_test_deletion_vector");
    let schema = resolve_schema_from_command_bytes(&read_command_bytes(&path, None))
        .await
        .unwrap();

    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(*schema.field(0).data_type(), DataType::Int32);
    assert_eq!(schema.field(1).name(), "value");
    assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
}

#[tokio::test]
async fn fixture_deletion_vector_read_returns_4_rows() {
    let path = fixture_path("delta_test_deletion_vector");
    let batches = collect_batches(&read_command_bytes(&path, None)).await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4);
}

#[tokio::test]
async fn fixture_deletion_vector_read_excludes_deleted_row() {
    let path = fixture_path("delta_test_deletion_vector");
    let batches = collect_batches(&read_command_bytes(&path, None)).await;

    let mut ids: Vec<i32> = Vec::new();
    for batch in &batches {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column should be Int32Array");
        for i in 0..batch.num_rows() {
            ids.push(id_col.value(i));
        }
    }

    ids.sort();
    assert_eq!(ids, vec![1, 2, 4, 5]);
}

#[tokio::test]
async fn fixture_deletion_vector_read_correct_data() {
    let path = fixture_path("delta_test_deletion_vector");
    let batches = collect_batches(&read_command_bytes(&path, None)).await;

    let mut rows: Vec<(i32, String)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column should be Int32Array");
        for i in 0..batch.num_rows() {
            let col = batch.column(1);
            let value = if let Some(sa) = col.as_any().downcast_ref::<StringArray>() {
                sa.value(i).to_string()
            } else if let Some(sva) = col.as_any().downcast_ref::<StringViewArray>() {
                sva.value(i).to_string()
            } else {
                panic!(
                    "Unexpected array type for value column: {:?}",
                    col.data_type()
                );
            };
            rows.push((ids.value(i), value));
        }
    }

    rows.sort_by_key(|(id, _)| *id);
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0], (1, "one".to_string()));
    assert_eq!(rows[1], (2, "two".to_string()));
    assert_eq!(rows[2], (4, "four".to_string()));
    assert_eq!(rows[3], (5, "five".to_string()));
}

#[tokio::test]
async fn fixture_deletion_vector_sql_query_works() {
    let path = fixture_path("delta_test_deletion_vector");
    let batches = collect_batches(&sql_command_bytes(
        "SELECT id, value FROM tbl WHERE id > 2 ORDER BY id",
        Some(&path),
        Some("tbl"),
    ))
    .await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

#[tokio::test]
async fn fixture_partitioned_deletion_vector_full_read_excludes_deleted_row() {
    let path = fixture_path("delta_test_partitioned_deletion_vector");
    let batches = collect_batches(&read_command_bytes(&path, None)).await;
    assert_eq!(ids_from_batches(&batches), vec![1, 2, 4, 5]);
}

#[tokio::test]
async fn fixture_partitioned_deletion_vector_predicate_token_us_partition_excludes_deleted_row() {
    let path = fixture_path("delta_test_partitioned_deletion_vector");
    let token = predicate_partition_token(1, &[("region", Some("us"))]);
    let batches = collect_read_batches(ReadCommand {
        path,
        num_rows: None,
        batch_size: None,
        storage_account: None,
        sas_token: None,
        storage_options: None,
        version: None,
        partition_token: Some(token),
        read_prefetch: None,
    })
    .await;

    assert_eq!(ids_from_batches(&batches), vec![1]);
}

#[tokio::test]
async fn fixture_partitioned_deletion_vector_predicate_tokens_match_full_read() {
    let path = fixture_path("delta_test_partitioned_deletion_vector");
    let mut ids = Vec::new();

    for token in [
        predicate_partition_token(1, &[("region", Some("us"))]),
        predicate_partition_token(1, &[("region", Some("eu"))]),
        predicate_partition_token(1, &[("region", Some("apac"))]),
    ] {
        let batches = collect_read_batches(ReadCommand {
            path: path.clone(),
            num_rows: None,
            batch_size: None,
            storage_account: None,
            sas_token: None,
            storage_options: None,
            version: None,
            partition_token: Some(token),
            read_prefetch: None,
        })
        .await;
        ids.extend(ids_from_batches(&batches));
    }

    ids.sort();
    assert_eq!(ids, vec![1, 2, 4, 5]);
}

#[tokio::test]
async fn plan_read_partitions_partitioned_table_returns_opaque_tokens() {
    let (path, _guard) = create_partitioned_table().await;
    let json = plan_read_partitions_from_command_bytes(&read_command_bytes(&path, None))
        .await
        .expect("partition planning should succeed");

    let _debug = serde_json::to_string_pretty(&json).unwrap(); // ← add this

    let result = json["result"].as_array().expect("result array");
    assert!(
        !result.is_empty(),
        "expected at least one planned partition"
    );

    let total_partitions = result.len();
    let mut ordinals = HashSet::new();

    for item in result {
        let token = item["token"].as_str().expect("token");
        assert!(!token.is_empty(), "expected opaque token");

        let ordinal = item["ordinal"].as_u64().expect("ordinal") as usize;
        assert!(ordinals.insert(ordinal), "ordinal should be unique");
        assert_eq!(
            item["totalPartitions"].as_u64().expect("total partitions") as usize,
            total_partitions
        );
        assert!(item["fileCount"].as_u64().expect("file count") >= 1);
    }
}

#[tokio::test]
async fn plan_read_partitions_non_partitioned_table_returns_opaque_tokens() {
    let (path, _guard) = create_test_delta_table().await;
    let json = plan_read_partitions_from_command_bytes(&read_command_bytes(&path, None))
        .await
        .expect("partition planning should succeed");

    let result = json["result"].as_array().expect("result array");
    assert!(
        !result.is_empty(),
        "expected at least one planned partition"
    );

    let total_partitions = result.len();
    let mut ordinals = HashSet::new();

    for item in result {
        let token = item["token"].as_str().expect("token");
        assert!(!token.is_empty(), "expected opaque token");

        let ordinal = item["ordinal"].as_u64().expect("ordinal") as usize;
        assert!(ordinals.insert(ordinal), "ordinal should be unique");
        assert_eq!(
            item["totalPartitions"].as_u64().expect("total partitions") as usize,
            total_partitions
        );
        assert!(item["fileCount"].as_u64().expect("file count") >= 1);
    }
}

#[tokio::test]
async fn plan_read_partitions_non_partitioned_multi_file_table_splits_evenly() {
    let (path, _guard) = create_multi_file_non_partitioned_table().await;
    let json = plan_read_partitions_from_command_bytes(&read_command_bytes(&path, None))
        .await
        .expect("partition planning should succeed");

    let result = json["result"].as_array().expect("result array");
    assert!(
        result.len() > 1,
        "expected planner to create multiple partitions"
    );

    let file_counts = result
        .iter()
        .map(|item| item["fileCount"].as_u64().expect("file count"))
        .collect::<Vec<_>>();
    let min_count = file_counts.iter().min().copied().expect("min file count");
    let max_count = file_counts.iter().max().copied().expect("max file count");

    assert_eq!(
        file_counts.iter().sum::<u64>(),
        8,
        "expected all files to be assigned"
    );
    assert!(
        max_count - min_count <= 1,
        "expected reasonably balanced file counts, got {:?}",
        file_counts
    );
}

#[tokio::test]
async fn file_subset_partition_token_embeds_add_metadata() {
    let (path, _guard) = create_multi_file_non_partitioned_table().await;
    let planned = plan_read_partitions_from_command_bytes(&read_command_bytes(&path, None))
        .await
        .expect("partition planning should succeed");
    let partition = planned["result"]
        .as_array()
        .and_then(|items| items.first())
        .expect("first partition");
    let token = partition["token"].as_str().expect("token");

    let decoded = decode_partition_token(token).expect("token should decode");
    let PartitionDescriptorMode::FileSubset { files } = decoded.mode else {
        panic!("expected file-subset token");
    };

    assert!(!files.is_empty(), "expected embedded Add metadata");
    assert!(files.iter().all(|file| !file.path.is_empty()));
    assert!(files.iter().all(|file| file.size > 0));
    assert!(
        files.iter().all(|file| file.stats.is_none()),
        "file statistics should be stripped from short-lived partition tokens"
    );
}

#[tokio::test]
async fn resolve_partition_files_returns_open_table_and_embedded_files() {
    let (path, _guard) = create_multi_file_non_partitioned_table().await;
    let planned = plan_read_partitions_from_command_bytes(&read_command_bytes(&path, None))
        .await
        .expect("partition planning should succeed");
    let partition = planned["result"]
        .as_array()
        .and_then(|items| items.first())
        .expect("first partition");
    let token = partition["token"].as_str().expect("token");
    let descriptor = decode_partition_token(token).expect("token should decode");
    let version = descriptor.version;
    let PartitionDescriptorMode::FileSubset { files: token_files } = descriptor.mode else {
        panic!("expected file-subset token");
    };

    let cmd = ReadCommand {
        path,
        num_rows: None,
        batch_size: None,
        storage_account: None,
        sas_token: None,
        storage_options: None,
        version: None,
        partition_token: Some(token.to_string()),
        read_prefetch: None,
    };

    let (table, files) = resolve_partition_files(&cmd, version, token_files)
        .await
        .expect("file-subset token should resolve");

    assert_eq!(table.version(), Some(version as u64));
    assert_eq!(
        files.len(),
        partition["fileCount"].as_u64().expect("fileCount") as usize
    );
    assert!(files.iter().all(|file| !file.path.is_empty()));
}

#[tokio::test]
async fn resolve_partition_files_rejects_version_mismatch() {
    let (path, _guard) = create_multi_file_non_partitioned_table().await;
    let planned = plan_read_partitions_from_command_bytes(&read_command_bytes(&path, None))
        .await
        .expect("partition planning should succeed");
    let partition = planned["result"]
        .as_array()
        .and_then(|items| items.first())
        .expect("first partition");
    let token = partition["token"].as_str().expect("token");
    let descriptor = decode_partition_token(token).expect("token should decode");
    let version = descriptor.version;
    let PartitionDescriptorMode::FileSubset { files: token_files } = descriptor.mode else {
        panic!("expected file-subset token");
    };

    let cmd = ReadCommand {
        path,
        num_rows: None,
        batch_size: None,
        storage_account: None,
        sas_token: None,
        storage_options: None,
        version: Some(version + 1),
        partition_token: Some(token.to_string()),
        read_prefetch: None,
    };

    let error = resolve_partition_files(&cmd, version, token_files)
        .await
        .expect_err("mismatched version should fail");
    assert!(
        error
            .to_string()
            .contains("does not match requested version")
    );
}

#[tokio::test]
async fn resolve_partition_files_trusts_embedded_token_metadata() {
    let (path, _guard) = create_multi_file_non_partitioned_table().await;
    let planned = plan_read_partitions_from_command_bytes(&read_command_bytes(&path, None))
        .await
        .expect("partition planning should succeed");
    let partition = planned["result"]
        .as_array()
        .and_then(|items| items.first())
        .expect("first partition");
    let token = partition["token"].as_str().expect("token");
    let mut descriptor = decode_partition_token(token).expect("token should decode");
    let version = descriptor.version;
    let PartitionDescriptorMode::FileSubset { files } = &mut descriptor.mode else {
        panic!("expected file-subset token");
    };
    let original_size = files[0].size;
    files[0].size = original_size + 1024;

    let cmd = ReadCommand {
        path,
        num_rows: None,
        batch_size: None,
        storage_account: None,
        sas_token: None,
        storage_options: None,
        version: None,
        partition_token: Some(token.to_string()),
        read_prefetch: None,
    };

    let PartitionDescriptorMode::FileSubset { files: token_files } = descriptor.mode else {
        panic!("expected file-subset token");
    };
    let (_table, resolved_files) = resolve_partition_files(&cmd, version, token_files)
        .await
        .expect("trusted token metadata should resolve");
    assert_eq!(resolved_files[0].size, original_size + 1024);
    assert!(resolved_files.iter().all(|file| file.stats.is_none()));
}

#[tokio::test]
async fn plan_read_partitions_downgrades_skewed_partition() {
    let (path, _guard) = create_skewed_partitioned_table().await;
    let planned = plan_read_partitions_from_command_bytes(&read_command_bytes(&path, None))
        .await
        .expect("partition planning should succeed");
    let partitions = planned["result"].as_array().expect("result array");

    let mut has_file_subset = false;
    let mut has_partition_predicate = false;
    for partition in partitions {
        let token = partition["token"].as_str().expect("token");
        match decode_partition_token(token)
            .expect("token should decode")
            .mode
        {
            PartitionDescriptorMode::FileSubset { .. } => has_file_subset = true,
            PartitionDescriptorMode::PartitionPredicate { .. } => has_partition_predicate = true,
        }
    }

    assert!(
        has_file_subset,
        "expected hot partition to be downgraded to FileSubset"
    );
    assert!(
        has_partition_predicate,
        "expected small partition to remain predicate-based"
    );
}

#[tokio::test]
async fn plan_read_partitions_partitioned_deletion_vector_table_stays_predicate_based() {
    let path = fixture_path("delta_test_partitioned_deletion_vector");
    let planned = plan_read_partitions_from_command_bytes(&read_command_bytes(&path, None))
        .await
        .expect("partitioned DV table should plan predicate partitions");
    let partitions = planned["result"].as_array().expect("result array");
    assert!(!partitions.is_empty());

    for partition in partitions {
        let token = partition["token"].as_str().expect("token");
        let descriptor = decode_partition_token(token).expect("token should decode");
        assert!(
            matches!(
                descriptor.mode,
                PartitionDescriptorMode::PartitionPredicate { .. }
            ),
            "partitioned DV tables must not plan FileSubset descriptors"
        );
    }
}

#[tokio::test]
async fn partition_reads_cover_skewed_partitioned_table() {
    let (path, _guard) = create_skewed_partitioned_table().await;
    let planned = plan_read_partitions_from_command_bytes(&read_command_bytes(&path, None))
        .await
        .expect("partition planning should succeed");
    let partitions = planned["result"].as_array().expect("result array");

    let mut ids = Vec::new();
    for partition in partitions {
        let token = partition["token"].as_str().expect("token");
        let batches = collect_read_batches(ReadCommand {
            path: path.clone(),
            num_rows: None,
            batch_size: None,
            storage_account: None,
            sas_token: None,
            storage_options: None,
            version: None,
            partition_token: Some(token.to_string()),
            read_prefetch: None,
        })
        .await;
        ids.extend(ids_from_batches(&batches));
    }

    ids.sort();
    assert_eq!(ids, (1..=10).collect::<Vec<_>>());
}

#[tokio::test]
async fn partition_read_returns_subset_of_partitioned_table() {
    let (path, _guard) = create_partitioned_table().await;
    let planned = plan_read_partitions_from_command_bytes(&read_command_bytes(&path, None))
        .await
        .expect("partition planning should succeed");
    let first_partition = planned["result"]
        .as_array()
        .and_then(|items| items.first())
        .expect("first partition");
    let token = first_partition["token"].as_str().expect("token");

    let mut map = serde_json::Map::new();
    map.insert("path".to_string(), serde_json::Value::String(path));
    map.insert(
        "partition_token".to_string(),
        serde_json::Value::String(token.to_string()),
    );

    let command = serde_json::to_vec(&map).expect("serialize command");
    let parsed = Command::parse(&command).expect("parse read command");
    let Command::Read(read_cmd) = parsed else {
        panic!("expected read command");
    };

    let (_schema, mut stream) = execute_read_table_stream(read_cmd)
        .await
        .expect("partition read should succeed");

    let mut total_rows = 0usize;
    while let Some(batch) = stream.next().await {
        total_rows += batch.expect("batch").num_rows();
    }

    assert!(
        total_rows >= 1 && total_rows < 5,
        "expected subset of table rows"
    );
}

#[tokio::test]
async fn partition_reads_cover_non_partitioned_table() {
    let (path, _guard) = create_test_delta_table().await;
    let planned = plan_read_partitions_from_command_bytes(&read_command_bytes(&path, None))
        .await
        .expect("partition planning should succeed");
    let partitions = planned["result"].as_array().expect("result array");

    let mut total_rows = 0usize;
    for partition in partitions {
        let token = partition["token"].as_str().expect("token");

        let mut map = serde_json::Map::new();
        map.insert("path".to_string(), serde_json::Value::String(path.clone()));
        map.insert(
            "partition_token".to_string(),
            serde_json::Value::String(token.to_string()),
        );

        let command = serde_json::to_vec(&map).expect("serialize command");
        let parsed = Command::parse(&command).expect("parse read command");
        let Command::Read(read_cmd) = parsed else {
            panic!("expected read command");
        };

        let (_schema, mut stream) = execute_read_table_stream(read_cmd)
            .await
            .expect("partition read should succeed");

        while let Some(batch) = stream.next().await {
            total_rows += batch.expect("batch").num_rows();
        }
    }

    assert_eq!(
        total_rows, 3,
        "expected all table rows across planned partitions"
    );
}

#[tokio::test]
async fn plan_read_partitions_deletion_vector_table_returns_error() {
    let path = fixture_path("delta_test_deletion_vector");
    let error = plan_read_partitions_from_command_bytes(&read_command_bytes(&path, None))
        .await
        .expect_err("DV partition planning should fail fast");

    let message = error.to_string();
    assert!(
        message.contains("partitioned reads are not yet supported")
            && message.contains("deletion vectors"),
        "unexpected error message: {message}"
    );
}
