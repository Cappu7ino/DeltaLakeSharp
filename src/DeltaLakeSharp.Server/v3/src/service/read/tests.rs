// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use super::*;
use super::partitioning::{
    decode_partition_token, encode_partition_token, resolve_partition_files,
};
use arrow::array::{Int32Array, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field};
use crate::service::request::{
    PartitionDescriptorMode, PartitionDescriptorPayload, PartitionPredicateKey,
};
use deltalake::kernel::Add;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
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
    map.insert("path".to_string(), serde_json::Value::String(path.to_string()));
    if let Some(n) = num_rows {
        map.insert("num_rows".to_string(), serde_json::Value::Number(n.into()));
    }
    serde_json::to_vec(&map).expect("failed to serialize read command")
}

fn sql_command_bytes(sql: &str, table_path: Option<&str>, table_name: Option<&str>) -> Vec<u8> {
    let mut map = serde_json::Map::new();
    map.insert("sql".to_string(), serde_json::Value::String(sql.to_string()));
    if let Some(tp) = table_path {
        map.insert("table_path".to_string(), serde_json::Value::String(tp.to_string()));
    }
    if let Some(tn) = table_name {
        map.insert("table_name".to_string(), serde_json::Value::String(tn.to_string()));
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
    map.insert("sql".to_string(), serde_json::Value::String(sql.to_string()));
    map.insert(
        "batch_size".to_string(),
        serde_json::Value::Number(serde_json::Number::from(batch_size as u64)),
    );
    if let Some(tp) = table_path {
        map.insert("table_path".to_string(), serde_json::Value::String(tp.to_string()));
    }
    if let Some(tn) = table_name {
        map.insert("table_name".to_string(), serde_json::Value::String(tn.to_string()));
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
            panic!("id column should be Int32Array or Int64Array, got {:?}", col.data_type());
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
    assert!(batches.len() >= 3, "Expected one-row batches when batch_size=1.");
    assert!(batches.iter().all(|b| b.num_rows() <= 1));
}

#[tokio::test]
async fn resolve_schema_invalid_path_returns_error() {
    let result = resolve_schema_from_command_bytes(&read_command_bytes("/nonexistent/path/to/nowhere", None)).await;
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
    let table: deltalake::DeltaTable = deltalake::DeltaTable::try_from_url(url).await.expect("try_from_url");
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
    map.insert("path".to_string(), serde_json::Value::String(path.to_string()));
    map.insert("version".to_string(), serde_json::Value::Number(version.into()));
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
    let table: deltalake::DeltaTable = deltalake::DeltaTable::try_from_url(url).await.expect("try_from_url");
    let _table: deltalake::DeltaTable = table
        .write(vec![batch])
        .with_partition_columns(vec!["region"])
        .await
        .expect("write partitioned");

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

async fn create_skewed_partitioned_table() -> (String, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("temp dir");
    let table_path = tmp.path().join("skewed_table");
    std::fs::create_dir(&table_path).expect("create table dir");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("region", DataType::Utf8, false),
    ]));

    let url = Url::from_file_path(&table_path).expect("url");
    let table: deltalake::DeltaTable = deltalake::DeltaTable::try_from_url(url)
        .await
        .expect("try_from_url");
    let mut table = table
        .write(vec![RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["eu"])),
            ],
        )
        .expect("batch")])
        .with_partition_columns(vec!["region".to_string()])
        .await
        .expect("create partitioned table");

    // Write multiple batches for the "us" partition so it becomes much larger
    // than "eu" — this creates the skew we test for.
    for i in 2..=10 {
        table = table
            .write(vec![RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(vec![i])),
                    Arc::new(StringArray::from(vec!["us"])),
                ],
            )
            .expect("batch")])
            .await
            .expect("write us batch");
    }

    let path_str = table_path.to_str().expect("non-UTF8 path");
    (path_str.to_string(), tmp)
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
        let ids = batch.column(0)
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
                    panic!("Unexpected array type for city column: {:?}", col.data_type());
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
        let id_col = batch.column(0)
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
        let ids = batch.column(0)
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
                panic!("Unexpected array type for value column: {:?}", col.data_type());
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
    assert!(!result.is_empty(), "expected at least one planned partition");

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
    assert!(!result.is_empty(), "expected at least one planned partition");

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
    assert!(result.len() > 1, "expected planner to create multiple partitions");

    let file_counts = result
        .iter()
        .map(|item| item["fileCount"].as_u64().expect("file count"))
        .collect::<Vec<_>>();
    let min_count = file_counts.iter().min().copied().expect("min file count");
    let max_count = file_counts.iter().max().copied().expect("max file count");

    assert_eq!(file_counts.iter().sum::<u64>(), 8, "expected all files to be assigned");
    assert!(
        max_count - min_count <= 1,
        "expected reasonably balanced file counts, got {:?}",
        file_counts
    );
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

    assert!(total_rows >= 1 && total_rows < 5, "expected subset of table rows");
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

    assert_eq!(total_rows, 3, "expected all table rows across planned partitions");
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

#[test]
fn file_subset_token_round_trip() {
    let add = Add {
        path: "part-00000.snappy.parquet".to_string(),
        partition_values: HashMap::new(),
        size: 1024,
        modification_time: 1_700_000_000_000,
        data_change: true,
        stats: Some(r#"{"numRecords":100}"#.to_string()),
        tags: None,
        deletion_vector: None,
        base_row_id: None,
        default_row_commit_version: None,
        clustering_provider: None,
    };

    let payload = PartitionDescriptorPayload {
        version: 5,
        ordinal: 0,
        total_partitions: 4,
        mode: PartitionDescriptorMode::FileSubset {
            files: vec![add.clone(), add.clone()],
        },
    };

    let token = encode_partition_token(&payload).expect("should encode");
    let decoded = decode_partition_token(&token).expect("should decode");

    assert_eq!(decoded.version, 5);
    assert_eq!(decoded.ordinal, 0);
    assert_eq!(decoded.total_partitions, 4);

    let PartitionDescriptorMode::FileSubset { files } = &decoded.mode else {
        panic!("expected FileSubset mode");
    };
    assert_eq!(files.len(), 2);
    assert_eq!(files[0].path, "part-00000.snappy.parquet");
    assert_eq!(files[0].size, 1024);
    assert_eq!(files[0].stats.as_deref(), Some(r#"{"numRecords":100}"#));
}

#[tokio::test]
async fn resolve_partition_files_rejects_partition_predicate_token() {
    let (path, _guard) = create_test_delta_table().await;

    let payload = PartitionDescriptorPayload {
        version: 0,
        ordinal: 0,
        total_partitions: 1,
        mode: PartitionDescriptorMode::PartitionPredicate {
            keys: vec![PartitionPredicateKey {
                values: HashMap::from([("id".to_string(), Some("1".to_string()))]),
            }],
        },
    };

    let cmd = ReadCommand {
        path,
        num_rows: None,
        batch_size: None,
        storage_account: None,
        sas_token: None,
        storage_options: None,
        version: None,
        partition_token: Some(encode_partition_token(&payload).expect("should encode")),
    };

    let error = resolve_partition_files(&cmd, &payload)
        .await
        .expect_err("should reject non-FileSubset token");
    assert!(
        error.to_string().contains("expected file-subset partition token"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn resolve_partition_files_returns_table_and_files() {
    let (path, _guard) = create_test_delta_table().await;

    let planned = plan_read_partitions_from_command_bytes(&read_command_bytes(&path, None))
        .await
        .expect("should plan");
    let partitions = planned["result"].as_array().expect("result array");
    assert!(!partitions.is_empty(), "should have at least one partition");

    let token = partitions[0]["token"].as_str().expect("token").to_string();
    let descriptor = decode_partition_token(&token).expect("should decode");

    let cmd = ReadCommand {
        path,
        num_rows: None,
        batch_size: None,
        storage_account: None,
        sas_token: None,
        storage_options: None,
        version: None,
        partition_token: Some(token),
    };

    let (table, files) = resolve_partition_files(&cmd, &descriptor)
        .await
        .expect("should resolve");

    assert!(!files.is_empty(), "should have files");
    assert_eq!(
        table.version().map(|v| v as i64),
        Some(descriptor.version),
        "table version should match token version"
    );
}

#[tokio::test]
async fn plan_read_partitions_downgrades_skewed_partition() {
    let (path, _guard) = create_skewed_partitioned_table().await;
    let planned = plan_read_partitions_from_command_bytes(&read_command_bytes(&path, None))
        .await
        .expect("planning should succeed");
    let partitions = planned["result"].as_array().expect("result array");

    // A skewed table: 9 rows in "us" and 1 in "eu".
    // The "us" partition should be downgraded to FileSubset.
    let has_file_subset = partitions.iter().any(|p| {
        let token = p["token"].as_str().expect("token");
        let descriptor = decode_partition_token(token).expect("decode");
        matches!(descriptor.mode, PartitionDescriptorMode::FileSubset { .. })
    });
    let has_predicate = partitions.iter().any(|p| {
        let token = p["token"].as_str().expect("token");
        let descriptor = decode_partition_token(token).expect("decode");
        matches!(descriptor.mode, PartitionDescriptorMode::PartitionPredicate { .. })
    });

    assert!(has_file_subset, "skewed 'us' partition should be downgraded to FileSubset");
    assert!(has_predicate, "small 'eu' partition should remain as PartitionPredicate");
}

#[tokio::test]
async fn partition_reads_cover_skewed_partitioned_table() {
    let (path, _guard) = create_skewed_partitioned_table().await;
    let planned = plan_read_partitions_from_command_bytes(&read_command_bytes(&path, None))
        .await
        .expect("planning should succeed");
    let partitions = planned["result"].as_array().expect("result array");
    assert!(!partitions.is_empty(), "should have partitions");

    let mut ids = Vec::new();
    for partition in partitions {
        let token = partition["token"].as_str().expect("token");

        let mut map = serde_json::Map::new();
        map.insert("path".to_string(), serde_json::Value::String(path.clone()));
        map.insert(
            "partition_token".to_string(),
            serde_json::Value::String(token.to_string()),
        );
        let command = serde_json::to_vec(&map).expect("serialize");
        let parsed = Command::parse(&command).expect("parse");
        let Command::Read(read_cmd) = parsed else {
            panic!("expected read command");
        };

        let (_schema, mut stream) = execute_read_table_stream(read_cmd)
            .await
            .expect("read should succeed");
        while let Some(batch) = stream.next().await {
            let batch = batch.expect("batch");
            let col = batch.column(0);
            if let Some(id_col) = col.as_any().downcast_ref::<Int32Array>() {
                for idx in 0..batch.num_rows() {
                    ids.push(id_col.value(idx));
                }
            }
        }
    }

    ids.sort();
    assert_eq!(ids.len(), 10, "should cover all 10 rows across partitions");
    assert_eq!(ids[0], 1, "first id should be 1");
    assert_eq!(ids[9], 10, "last id should be 10");
}
