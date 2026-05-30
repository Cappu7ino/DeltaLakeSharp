//! Fixture-generation utility for Delta Table Service V3 tests.
//!
//! The V3 runtime is now in-process via the native ABI, but integration tests
//! still benefit from a small Rust utility that can create local Delta fixtures
//! with the same delta-rs write semantics as the engine.

use std::sync::Arc;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

/// CLI arguments for the V3 fixture utility.
#[derive(Parser, Debug)]
#[command(name = "delta-table-service-v3-fixture")]
#[command(about = "Creates local Delta test fixtures for V3 integration tests")]
struct Args {
    #[command(subcommand)]
    command: SubCmd,
}

#[derive(Subcommand, Debug)]
enum SubCmd {
    /// Create a Delta table fixture for integration tests.
    Create {
        /// Directory path where the Delta table will be created.
        path: String,

        /// Fixture type: basic, partitioned, or time-travel.
        #[arg(long, default_value = "basic")]
        fixture_type: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .with_level(true)
        .init();

    let args = Args::parse();

    match args.command {
        SubCmd::Create { path, fixture_type } => match fixture_type.as_str() {
            "basic" => create_test_fixture(&path).await,
            "partitioned" => create_partitioned_fixture(&path).await,
            "time-travel" => create_time_travel_fixture(&path).await,
            _ => Err(format!(
                "Unknown fixture type: {fixture_type}. Use: basic, partitioned, time-travel"
            )
            .into()),
        },
    }
}

async fn create_test_fixture(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let table_path = std::path::Path::new(path);
    if !table_path.exists() {
        std::fs::create_dir_all(table_path)?;
    }

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
    )?;

    let url = url::Url::from_file_path(table_path)
        .map_err(|()| format!("Failed to convert path to URL: {path}"))?;

    let table: deltalake::DeltaTable = deltalake::DeltaTable::try_from_url(url).await?;
    let _table: deltalake::DeltaTable = table.write(vec![batch]).await?;

    println!("TEST_FIXTURE_CREATED {path}");
    Ok(())
}

async fn create_partitioned_fixture(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let table_path = std::path::Path::new(path);
    if !table_path.exists() {
        std::fs::create_dir_all(table_path)?;
    }

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
            Arc::new(StringArray::from(vec!["us", "eu", "us", "eu", "apac"])),
        ],
    )?;

    let url = url::Url::from_file_path(table_path)
        .map_err(|()| format!("Failed to convert path to URL: {path}"))?;

    let table: deltalake::DeltaTable = deltalake::DeltaTable::try_from_url(url).await?;
    let _table: deltalake::DeltaTable = table
        .write(vec![batch])
        .with_partition_columns(vec!["region"])
        .await?;

    println!("TEST_FIXTURE_CREATED {path}");
    Ok(())
}

async fn create_time_travel_fixture(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let table_path = std::path::Path::new(path);
    if !table_path.exists() {
        std::fs::create_dir_all(table_path)?;
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let batch_v0 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["v0_a", "v0_b"])),
        ],
    )?;

    let url = url::Url::from_file_path(table_path)
        .map_err(|()| format!("Failed to convert path to URL: {path}"))?;

    let table: deltalake::DeltaTable = deltalake::DeltaTable::try_from_url(url).await?;
    let table: deltalake::DeltaTable = table.write(vec![batch_v0]).await?;

    let batch_v1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["v1_c", "v1_d"])),
        ],
    )?;

    let _table: deltalake::DeltaTable = table
        .write(vec![batch_v1])
        .with_save_mode(deltalake::protocol::SaveMode::Append)
        .await?;

    println!("TEST_FIXTURE_CREATED {path}");
    Ok(())
}
