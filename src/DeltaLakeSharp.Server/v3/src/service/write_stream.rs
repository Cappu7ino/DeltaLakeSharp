use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use arrow::record_batch::RecordBatchReader;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ffi_stream::ArrowArrayStreamReader;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, UNNAMED_TABLE};
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use datafusion::datasource::provider_as_source;
use futures::StreamExt;

use crate::error::ServiceError;

#[derive(Debug)]
pub(super) struct NativeWriteStreamState {
    reader: Option<ArrowArrayStreamReader>,
}

impl NativeWriteStreamState {
    pub(super) fn new(reader: ArrowArrayStreamReader) -> Self {
        Self {
            reader: Some(reader),
        }
    }

    fn take_reader(&mut self) -> Option<ArrowArrayStreamReader> {
        self.reader.take()
    }
}

#[derive(Clone)]
pub(super) struct NativeWritePartitionStream {
    schema: SchemaRef,
    state: Arc<Mutex<NativeWriteStreamState>>,
}

impl NativeWritePartitionStream {
    pub(super) fn new(
        schema: SchemaRef,
        state: Arc<Mutex<NativeWriteStreamState>>,
    ) -> Self {
        Self { schema, state }
    }
}

impl fmt::Debug for NativeWritePartitionStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeWritePartitionStream")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl PartitionStream for NativeWritePartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<datafusion::execution::TaskContext>) -> SendableRecordBatchStream {
        let reader = match self.state.lock() {
            Ok(mut guard) => match guard.take_reader() {
                Some(reader) => reader,
                None => {
                    let error = ArrowError::ExternalError(Box::new(std::io::Error::other(
                        "native V3 write stream can only be consumed once",
                    )));
                    return Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&self.schema), futures::stream::once(async {
                        Err(DataFusionError::ArrowError(Box::new(error), Some(DataFusionError::get_back_trace())))
                    })));
                }
            },
            Err(_) => {
                let error = ArrowError::ExternalError(Box::new(std::io::Error::other(
                    "native V3 write stream state lock is poisoned",
                )));
                return Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&self.schema), futures::stream::once(async {
                    Err(DataFusionError::ArrowError(Box::new(error), Some(DataFusionError::get_back_trace())))
                })));
            }
        };

        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::iter(reader).map(|result| result.map_err(|error| {
                DataFusionError::ArrowError(Box::new(error), Some(DataFusionError::get_back_trace()))
            })),
        ))
    }
}

pub(super) fn build_streaming_input_plan(
    _ctx: &SessionContext,
    reader: ArrowArrayStreamReader,
) -> Result<LogicalPlan, ServiceError> {
    let schema = reader.schema();
    let partition = Arc::new(NativeWritePartitionStream::new(
        Arc::clone(&schema),
        Arc::new(Mutex::new(NativeWriteStreamState::new(reader))),
    ));
    let table = StreamingTable::try_new(schema, vec![partition]).map_err(ServiceError::DataFusion)?;
    LogicalPlanBuilder::scan(UNNAMED_TABLE, provider_as_source(Arc::new(table)), None)
        .and_then(|builder| builder.build())
        .map_err(ServiceError::DataFusion)
}
