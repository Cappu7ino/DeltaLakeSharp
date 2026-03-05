// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Arrow Flight service implementation for Delta Table Service V3.
//!
//! This is the central dispatch layer: each Flight RPC method is routed to
//! the appropriate handler module. Phase 1 implements health/shutdown and
//! stubs all other endpoints as `Unimplemented`.

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

use crate::delta::table_manager::TableManager;
use crate::handlers;

/// The Flight service implementation for V3.
pub struct DeltaFlightService {
    /// Shared shutdown signal. When notified, the server initiates graceful shutdown.
    shutdown: Arc<Notify>,

    /// Delta table manager (placeholder for Phase 2+).
    #[allow(dead_code)]
    table_manager: TableManager,
}

impl DeltaFlightService {
    /// Creates a new Flight service instance.
    pub fn new() -> Self {
        Self {
            shutdown: Arc::new(Notify::new()),
            table_manager: TableManager::new(),
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
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // TODO(phase2): Parse command JSON, open Delta table, return FlightInfo.
        Err(Status::unimplemented(
            "GetFlightInfo will be implemented in Phase 2",
        ))
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
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        // TODO(phase2): Parse command JSON, open Delta table, return schema.
        Err(Status::unimplemented(
            "GetSchema will be implemented in Phase 2",
        ))
    }

    // ---- DoGet (Phase 2) -------------------------------------------------

    type DoGetStream = BoxStream<FlightData>;

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        // TODO(phase2): Decode ticket, execute scan/query, stream batches.
        Err(Status::unimplemented(
            "DoGet will be implemented in Phase 2",
        ))
    }

    // ---- DoPut (Phase 3) -------------------------------------------------

    type DoPutStream = BoxStream<PutResult>;

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        // TODO(phase3): Parse descriptor, receive batches, write to Delta table.
        Err(Status::unimplemented(
            "DoPut will be implemented in Phase 3",
        ))
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
            "create_table" => {
                // TODO(phase3): Implement create_table action.
                Err(Status::unimplemented(
                    "create_table will be implemented in Phase 3",
                ))
            }
            "execute_dml" => {
                // TODO(phase3): Implement execute_dml action (DELETE/UPDATE/MERGE via SQL).
                Err(Status::unimplemented(
                    "execute_dml will be implemented in Phase 3",
                ))
            }
            "upgrade_protocol" => {
                // TODO(phase3): Implement upgrade_protocol action.
                Err(Status::unimplemented(
                    "upgrade_protocol will be implemented in Phase 3",
                ))
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
