// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Delta Table Service V3 — Arrow Flight server backed by DataFusion + delta-rs.
//!
//! This binary is spawned as a child process by the C# `DeltaTableProcess` class.
//! It binds to a TCP port (0 = OS-assigned), prints a sentinel line
//! `LISTENING ON PORT {N}` to stdout so the parent can discover the port,
//! then serves Arrow Flight RPCs until shutdown.

mod delta;
mod error;
mod flight_service;
mod handlers;

use std::sync::Arc;

use clap::{Parser, Subcommand};
use flight_service::DeltaFlightService;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// CLI arguments for the Delta Table Service V3 server.
#[derive(Parser, Debug)]
#[command(name = "delta-table-service-v3")]
#[command(about = "Delta Table Service V3 — Arrow Flight server (DataFusion + delta-rs)")]
struct Args {
    #[command(subcommand)]
    command: Option<SubCmd>,

    /// Host address to bind to.
    #[arg(long, default_value = "0.0.0.0")]
    host: String,

    /// Port to listen on. Use 0 for OS-assigned port.
    #[arg(long, default_value_t = 0)]
    port: u16,
}

#[derive(Subcommand, Debug)]
enum SubCmd {
    /// Create a test Delta table fixture for integration tests.
    /// The fixture type determines the table schema and contents:
    ///   basic      — (id: Int32, name: Utf8), 3 rows
    ///   partitioned — (id: Int32, name: Utf8, region: Utf8), partitioned by region
    ///   time-travel — (id: Int32, name: Utf8) with 2 versions (v0: 2 rows, v1: 4 rows)
    ///
    /// Note: column-mapping and deletion-vector fixtures must be created via
    /// PySpark since delta-rs 0.31 does not support writing these features.
    CreateTestFixture {
        /// Directory path where the Delta table will be created.
        path: String,

        /// Type of fixture to create. Defaults to "basic".
        #[arg(long, default_value = "basic")]
        fixture_type: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing (respects RUST_LOG env var, defaults to INFO).
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .with_level(true)
        .init();

    let args = Args::parse();

    // Handle subcommands.
    if let Some(SubCmd::CreateTestFixture { path, fixture_type }) = args.command {
        return match fixture_type.as_str() {
            "basic" => create_test_fixture(&path).await,
            "partitioned" => create_partitioned_fixture(&path).await,
            "time-travel" => create_time_travel_fixture(&path).await,
            _ => {
                eprintln!("Unknown fixture type: {fixture_type}. Use: basic, partitioned, time-travel");
                std::process::exit(1);
            }
        };
    }

    // Default: run the Flight server.
    run_server(args).await
}

/// Creates a test Delta table fixture at the given path.
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

/// Creates a partitioned Delta table fixture at the given path.
///
/// Schema: (id: Int32, name: Utf8, region: Utf8), partitioned by `region`.
/// Data: (1,"a","us"), (2,"b","eu"), (3,"c","us"), (4,"d","eu"), (5,"e","apac")
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

/// Creates a time-travel Delta table fixture at the given path.
///
/// Schema: (id: Int32, name: Utf8).
/// - Version 0: 2 rows — (1,"v0_a"), (2,"v0_b")
/// - Version 1: 4 rows total — appends (3,"v1_c"), (4,"v1_d")
///
/// Tests can read at version 0 (2 rows) vs latest (4 rows) to verify time travel.
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

    // Version 0: initial write with 2 rows.
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

    // Version 1: append 2 more rows (total = 4).
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

async fn run_server(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    // Bind to the requested address. Port 0 lets the OS pick a free port.
    let addr = format!("{}:{}", args.host, args.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let local_addr = listener.local_addr()?;

    // Print the sentinel line that the C# process manager watches for.
    // This MUST be printed to stdout (not stderr) and match the exact format.
    println!("LISTENING ON PORT {}", local_addr.port());

    info!(
        "Delta Table Service V3 listening on {}:{}",
        local_addr.ip(),
        local_addr.port()
    );

    // Build the Flight service.
    let flight_service = DeltaFlightService::new();
    let shutdown_handle = flight_service.shutdown_handle();
    let svc = arrow_flight::flight_service_server::FlightServiceServer::new(flight_service);

    // Serve until shutdown signal (Ctrl+C, SIGTERM, or DoAction("shutdown")).
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    Server::builder()
        .add_service(svc)
        .serve_with_incoming_shutdown(incoming, combined_shutdown(shutdown_handle))
        .await?;

    info!("Server shut down gracefully.");
    Ok(())
}

/// Waits for any shutdown trigger: Ctrl+C, SIGTERM, or `DoAction("shutdown")`.
async fn combined_shutdown(notify: Arc<tokio::sync::Notify>) {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => { info!("Received Ctrl+C, shutting down..."); }
            _ = sigterm.recv() => { info!("Received SIGTERM, shutting down..."); }
            _ = notify.notified() => { info!("Received shutdown action, shutting down..."); }
        }
    }

    #[cfg(not(unix))]
    {
        tokio::select! {
            result = ctrl_c => {
                result.expect("failed to listen for Ctrl+C");
                info!("Received Ctrl+C, shutting down...");
            }
            _ = notify.notified() => {
                info!("Received shutdown action, shutting down...");
            }
        }
    }
}
