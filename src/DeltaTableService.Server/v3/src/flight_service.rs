// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Arrow Flight service implementation for Delta Table Service V3.
//!
//! This is the central dispatch layer: each Flight RPC method is routed to
//! the appropriate handler module. Read path (Phase 2) and write path
//! (Phase 3) are fully wired up.

use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::Stream;
use tokio::sync::Notify;
use tonic::{Request, Response, Status, Streaming};
use tracing::warn;

use crate::handlers;

/// The Flight service implementation for V3.
pub struct DeltaFlightService {
    /// Shared shutdown signal. When notified, the server initiates graceful shutdown.
    shutdown: Arc<Notify>,
}

impl DeltaFlightService {
    /// Creates a new Flight service instance.
    pub fn new() -> Self {
        Self {
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Returns a clone of the shutdown notifier for use in the server's
    /// shutdown future.
    #[allow(dead_code)]
    pub fn shutdown_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.shutdown)
    }
}

// Convenience type alias for the streaming response types that Flight requires.
type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for DeltaFlightService {
    // ---- Handshake (not used — no auth) ----------------------------------

    type HandshakeStream = BoxStream<HandshakeResponse>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Handshake is not supported"))
    }

    // ---- ListFlights (not used) ------------------------------------------

    type ListFlightsStream = BoxStream<FlightInfo>;

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("ListFlights is not supported"))
    }

    // ---- GetFlightInfo (Phase 2) -----------------------------------------

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let info = handlers::read::handle_get_flight_info(descriptor)
            .await
            .map_err(tonic::Status::from)?;
        Ok(Response::new(info))
    }

    // ---- PollFlightInfo (not used) ---------------------------------------

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("PollFlightInfo is not supported"))
    }

    // ---- GetSchema (Phase 2) ---------------------------------------------

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();
        let result = handlers::read::handle_get_schema(descriptor)
            .await
            .map_err(tonic::Status::from)?;
        Ok(Response::new(result))
    }

    // ---- DoGet (Phase 2) -------------------------------------------------

    type DoGetStream = BoxStream<FlightData>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let stream = handlers::read::handle_do_get(ticket)
            .await
            .map_err(tonic::Status::from)?;
        Ok(Response::new(stream))
    }

    // ---- DoPut (Phase 3: write + merge) ------------------------------------

    type DoPutStream = BoxStream<PutResult>;

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let stream = request.into_inner();
        let put_result = handlers::write::handle_do_put(stream)
            .await
            .map_err(tonic::Status::from)?;
        let response_stream = futures::stream::once(async { Ok(put_result) });
        Ok(Response::new(Box::pin(response_stream)))
    }

    // ---- DoExchange (not used) -------------------------------------------

    type DoExchangeStream = BoxStream<FlightData>;

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("DoExchange is not supported"))
    }

    // ---- DoAction (Phase 1: health + shutdown; Phase 3: DML + create) ----

    type DoActionStream = BoxStream<arrow_flight::Result>;

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        let action_type = action.r#type.as_str();

        match action_type {
            "health" => {
                let result = handlers::health::handle_health();
                let stream = futures::stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }
            "shutdown" => {
                let result = handlers::health::handle_shutdown();
                // Notify the shutdown signal so the server can stop.
                let shutdown = Arc::clone(&self.shutdown);
                let stream = futures::stream::once(async move {
                    // Trigger shutdown after sending the response.
                    shutdown.notify_one();
                    Ok(result)
                });
                Ok(Response::new(Box::pin(stream)))
            }
            "create_table" | "execute_dml" | "upgrade_protocol" => {
                let json_value = match action_type {
                    "create_table" => {
                        handlers::write::handle_create_table(&action.body).await
                    }
                    "execute_dml" => {
                        handlers::write::handle_execute_dml(&action.body).await
                    }
                    "upgrade_protocol" => {
                        handlers::write::handle_upgrade_protocol(&action.body).await
                    }
                    _ => unreachable!(),
                }
                .map_err(tonic::Status::from)?;

                let json_bytes = serde_json::to_vec(&json_value)
                    .map_err(|e| Status::internal(format!("JSON serialization error: {e}")))?;
                let result = arrow_flight::Result {
                    body: json_bytes.into(),
                };
                let stream = futures::stream::once(async { Ok(result) });
                Ok(Response::new(Box::pin(stream)))
            }
            _ => {
                warn!("Unknown action type: {}", action_type);
                Err(Status::invalid_argument(format!(
                    "Unknown action type: {action_type}"
                )))
            }
        }
    }

    // ---- ListActions (Phase 1) -------------------------------------------

    type ListActionsStream = BoxStream<ActionType>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = handlers::actions::list_actions();
        let stream = futures::stream::iter(actions.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }
}
