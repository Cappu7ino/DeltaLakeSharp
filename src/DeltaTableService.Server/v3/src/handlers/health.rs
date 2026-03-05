// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Health check and shutdown action handlers.
//!
//! - `DoAction("health")` — returns `{"status": "healthy", ...}`.
//! - `DoAction("shutdown")` — triggers graceful server shutdown.

use serde_json::json;
use tracing::info;

/// Handles the `health` action.
///
/// Returns a JSON payload matching the format expected by the C# client
/// (`FlightClientWrapper.HealthCheckAsync` checks for `"status": "healthy"`).
pub fn handle_health() -> arrow_flight::Result {
    let body = json!({
        "status": "healthy",
        "engine": "datafusion + delta-rs (Rust native)",
        "version": "v3"
    });
    arrow_flight::Result {
        body: body.to_string().into(),
    }
}

/// Handles the `shutdown` action. Sends a JSON acknowledgement and
/// returns the shutdown token so the caller can trigger graceful shutdown.
///
/// The actual shutdown is coordinated by the caller (flight_service.rs)
/// via a `tokio::sync::Notify`.
pub fn handle_shutdown() -> arrow_flight::Result {
    info!("Shutdown requested via DoAction");
    let body = json!({
        "status": "shutting_down",
        "message": "Server is shutting down"
    });
    arrow_flight::Result {
        body: body.to_string().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_returns_healthy_status() {
        let result = handle_health();
        let body: serde_json::Value = serde_json::from_slice(&result.body).unwrap();
        assert_eq!(body["status"], "healthy");
    }

    #[test]
    fn shutdown_returns_success() {
        let result = handle_shutdown();
        let body: serde_json::Value = serde_json::from_slice(&result.body).unwrap();
        assert_eq!(body["status"], "shutting_down");
    }
}
