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

use clap::Parser;
use flight_service::DeltaFlightService;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// CLI arguments for the Delta Table Service V3 server.
#[derive(Parser, Debug)]
#[command(name = "delta-table-service-v3")]
#[command(about = "Delta Table Service V3 — Arrow Flight server (DataFusion + delta-rs)")]
struct Args {
    /// Host address to bind to.
    #[arg(long, default_value = "0.0.0.0")]
    host: String,

    /// Port to listen on. Use 0 for OS-assigned port.
    #[arg(long, default_value_t = 0)]
    port: u16,
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
