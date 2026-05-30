"""Arrow Flight server for Delta table operations using DataFusion (V2).

This server exposes the exact same Arrow Flight RPC protocol as the V1
server (``flight_server.py``) so the C# ``FlightClientWrapper`` can be
reused without changes.  The only difference is the backend: DataFusion
+ delta-rs instead of PySpark + delta-spark.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import pyarrow as pa
import pyarrow.flight as flight

from app.utils import parse_json, to_bytes
from app.v2 import datafusion_operations as ops

logger = logging.getLogger(__name__)


class DeltaFlightServerV2(flight.FlightServerBase):
    """Arrow Flight server backed by DataFusion + delta-rs.

    Flight RPC mapping (identical to V1):
        GetFlightInfo + DoGet  -> read Delta table or execute SQL query,
                                  stream RecordBatches
        GetSchema              -> return Delta table Arrow schema
        DoPut                  -> receive Arrow data, write as Delta table
        DoAction("health")     -> health check
        DoAction("create_table") -> create empty Delta table with schema
        DoAction("execute_dml")  -> execute DML (DELETE/UPDATE/MERGE)
    """

    def __init__(
        self,
        location: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(location, **kwargs)
        logger.info("DeltaFlightServerV2 (DataFusion) initialized at %s", location)

    # ------------------------------------------------------------------ #
    #  GetFlightInfo — describes a dataset (read request)
    # ------------------------------------------------------------------ #

    # ------------------------------------------------------------------ #
    #  Command detection helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _is_sql_query(cmd: dict[str, Any]) -> bool:
        """Return ``True`` if *cmd* is a SQL query request."""
        return "sql" in cmd

    def get_flight_info(
        self,
        context: flight.ServerCallContext,
        descriptor: flight.FlightDescriptor,
    ) -> flight.FlightInfo:
        """Return FlightInfo for a read or SQL query request.

        Two request shapes are recognised:

        1. **Read request** — ``{ "path": "...", "num_rows": 100 }``
           Returns schema from the Delta log.

        2. **SQL query** — ``{ "sql": "...", "table_path": "...",
           "table_name": "..." }``  (table_path/table_name are optional)
           Returns an empty schema with a ticket; actual execution and
           schema discovery happen in ``do_get``.
        """
        try:
            cmd = parse_json(descriptor.command)

            if self._is_sql_query(cmd):
                return self._get_flight_info_sql(cmd, descriptor)

            # --- Default: read table ---
            path = cmd["path"]
            num_rows = cmd.get("num_rows")
            storage_account = cmd.get("storage_account")
            sas_token = cmd.get("sas_token")

            logger.info("GetFlightInfo for path=%s, num_rows=%s", path, num_rows)

            # Read only the schema — no data is scanned.
            schema = ops.get_schema(path, storage_account, sas_token)

            ticket = flight.Ticket(descriptor.command)
            endpoint = flight.FlightEndpoint(ticket, [])

            return flight.FlightInfo(
                schema,
                descriptor,
                [endpoint],
                -1,  # total_records unknown (streaming)
                -1,  # total_bytes unknown (streaming)
            )
        except Exception as e:
            logger.error("GetFlightInfo failed: %s", e, exc_info=True)
            raise flight.FlightInternalError(str(e))

    def _get_flight_info_sql(
        self,
        cmd: dict[str, Any],
        descriptor: flight.FlightDescriptor,
    ) -> flight.FlightInfo:
        """Build FlightInfo for a SQL query request.

        Returns an empty schema with a ticket.  Actual execution and
        schema discovery happen in ``do_get``.
        """
        logger.info(
            "GetFlightInfo (SQL): sql=%s", cmd["sql"][:200]
        )

        # Build a ticket that carries the full command so do_get can replay.
        ticket = flight.Ticket(descriptor.command)
        endpoint = flight.FlightEndpoint(ticket, [])

        return flight.FlightInfo(
            pa.schema([]),
            descriptor,
            [endpoint],
            -1,
            -1,
        )

    # ------------------------------------------------------------------ #
    #  DoGet — stream RecordBatches to the client
    # ------------------------------------------------------------------ #

    def do_get(
        self,
        context: flight.ServerCallContext,
        ticket: flight.Ticket,
    ) -> flight.FlightDataStream:
        """Stream Delta table data as Arrow RecordBatches.

        Two request shapes are recognised (mirroring ``get_flight_info``):

        1. **Read request** — ``{ "path": "...", "num_rows": 100 }``
        2. **SQL query** — ``{ "sql": "...", "table_path": "...",
           "table_name": "..." }``  (table_path/table_name optional)

        Both return a ``flight.RecordBatchStream`` backed by a
        ``pa.RecordBatchReader`` — the Flight C++ layer handles IPC
        serialization without acquiring the Python GIL per batch.
        """
        try:
            cmd = parse_json(ticket.ticket)

            if self._is_sql_query(cmd):
                return self._do_get_sql(cmd)

            # --- Default: read table ---
            reader = ops.read_table(
                cmd["path"],
                cmd.get("num_rows"),
                cmd.get("storage_account"),
                cmd.get("sas_token"),
            )

            logger.info("DoGet streaming batches for path=%s", cmd["path"])

            return flight.RecordBatchStream(reader)
        except Exception as e:
            logger.error("DoGet failed: %s", e, exc_info=True)
            raise flight.FlightInternalError(str(e))

    def _do_get_sql(
        self,
        cmd: dict[str, Any],
    ) -> flight.FlightDataStream:
        """Execute a SQL query and stream the result as RecordBatches.

        Delegates to ``ops.read_table()`` in SQL mode, passing through
        optional ``table_path`` (as *path*) and ``table_name`` for table
        registration.
        """
        sql = cmd["sql"]
        table_path = cmd.get("table_path")
        table_name = cmd.get("table_name")

        logger.info(
            "DoGet (SQL): sql=%s, table_path=%s, table_name=%s",
            sql[:200],
            table_path,
            table_name,
        )

        reader = ops.read_table(
            table_path,
            sql=sql,
            table_name=table_name,
            storage_account=cmd.get("storage_account"),
            sas_token=cmd.get("sas_token"),
        )

        return flight.RecordBatchStream(reader)

    # ------------------------------------------------------------------ #
    #  GetSchema — return table schema without data
    # ------------------------------------------------------------------ #

    def get_schema(
        self,
        context: flight.ServerCallContext,
        descriptor: flight.FlightDescriptor,
    ) -> flight.SchemaResult:
        """Return the Arrow schema of a Delta table."""
        try:
            cmd = parse_json(descriptor.command)
            schema = ops.get_schema(
                cmd["path"],
                cmd.get("storage_account"),
                cmd.get("sas_token"),
            )
            logger.info(
                "GetSchema returning %d fields: %s",
                len(schema),
                [(f.name, str(f.type)) for f in schema],
            )
            return flight.SchemaResult(schema)
        except Exception as e:
            logger.error("GetSchema failed: %s", e, exc_info=True)
            raise flight.FlightInternalError(str(e))

    # ------------------------------------------------------------------ #
    #  DoPut — receive Arrow data from client, write as Delta table
    # ------------------------------------------------------------------ #

    def do_put(
        self,
        context: flight.ServerCallContext,
        descriptor: flight.FlightDescriptor,
        reader: flight.MetadataRecordBatchReader,
        writer: flight.FlightMetadataWriter,
    ) -> None:
        """Receive Arrow RecordBatches and write them as a Delta table.

        Batches are processed one at a time — the full dataset is never
        materialised into a single ``pa.Table``.  Each incoming batch
        undergoes an IPC round-trip to force 64-byte buffer alignment
        before being passed to delta-rs.

        **Why this is needed (empirically verified Feb 2026):**

        Arrow Flight's C++ gRPC layer allocates buffers with only 8-byte
        alignment.  When these buffers cross the Python→Rust FFI boundary
        into delta-rs's ``write_deltalake``, Rust's arrow-rs panics on
        the misalignment.  See:

        - https://github.com/apache/arrow-rs/issues/6471
        - https://github.com/apache/arrow-rs/pull/6472  (fix in arrow-rs 53.1.0)
        - https://github.com/delta-io/delta-rs/issues/3407

        The arrow-rs 53.1.0 fix only applies to the ``FromPyArrow`` trait
        path.  When batches flow through ``RecordBatchReader`` into
        ``write_deltalake``, they bypass ``FromPyArrow`` and the Rust code
        still receives unaligned buffers.  Removing this IPC round-trip
        causes the server to crash (tested with deltalake 1.4.2 /
        pyarrow 23.0.1).

        The aligned batches are wrapped in a ``pa.RecordBatchReader`` and
        streamed directly into ``write_deltalake``, which consumes them
        incrementally.
        """
        try:
            cmd = parse_json(descriptor.command)
            path = cmd["path"]
            operation = cmd.get("operation", "write")

            schema = reader.schema

            def _aligned_batches():
                """Yield IPC-realigned batches from the Flight reader.

                Each batch is serialised to an in-memory IPC stream and
                read back, forcing PyArrow to allocate new 64-byte
                aligned buffers.  This adds ~1 memcpy per batch but
                avoids Rust alignment panics in delta-rs.
                """
                for chunk in reader:
                    batch = chunk.data  # FlightStreamChunk → RecordBatch
                    sink = pa.BufferOutputStream()
                    ipc_writer = pa.ipc.new_stream(sink, schema)
                    ipc_writer.write_batch(batch)
                    ipc_writer.close()
                    ipc_reader = pa.ipc.open_stream(sink.getvalue())
                    yield ipc_reader.read_next_batch()

            batch_reader = pa.RecordBatchReader.from_batches(
                schema, _aligned_batches()
            )

            if operation == "merge":
                logger.info(
                    "DoPut MERGE for path=%s, predicate=%s",
                    path,
                    cmd.get("predicate"),
                )
                result = ops.merge_arrow_stream(
                    path,
                    batch_reader,
                    cmd,
                    cmd.get("storage_account"),
                    cmd.get("sas_token"),
                )
            else:
                mode = cmd.get("mode", "overwrite")
                logger.info("DoPut receiving data for path=%s, mode=%s", path, mode)
                result = ops.write_arrow_batches(
                    path,
                    batch_reader,
                    mode,
                    cmd.get("storage_account"),
                    cmd.get("sas_token"),
                    cmd.get("configuration"),
                    partition_by=cmd.get("partition_by"),
                )

            if not result.get("success", False):
                raise flight.FlightInternalError(
                    f"Failed to {operation} Delta table: {result.get('message', 'Unknown error')}"
                )

            # Send result metadata back to the client.
            writer.write(json.dumps(result).encode("utf-8"))

            logger.info("DoPut %s result: %s", operation, result)
        except flight.FlightInternalError:
            raise
        except Exception as e:
            logger.error("DoPut failed: %s", e, exc_info=True)
            raise flight.FlightInternalError(str(e))

    # ------------------------------------------------------------------ #
    #  DoAction — execute named actions
    # ------------------------------------------------------------------ #

    def do_action(
        self,
        context: flight.ServerCallContext,
        action: flight.Action,
    ):
        """Dispatch named actions.

        Supported action types:
            "health"               — returns server health and DataFusion version.
            "create_table"         — creates an empty Delta table with the given schema.
            "execute_dml"          — executes a DML statement (DELETE/UPDATE/MERGE).
            "upgrade_protocol"     — upgrades the Delta table protocol version.

        Note: SQL queries are served via GetFlightInfo + DoGet (streaming
        RecordBatches) rather than DoAction.
        """
        action_type = action.type
        logger.info("DoAction: %s", action_type)

        try:
            if action_type == "health":
                yield from self._action_health()
            elif action_type == "create_table":
                yield from self._action_create_table(action.body.to_pybytes())
            elif action_type == "execute_dml":
                yield from self._action_execute_dml(action.body.to_pybytes())
            elif action_type == "upgrade_protocol":
                yield from self._action_upgrade_protocol(action.body.to_pybytes())
            else:
                raise flight.FlightUnimplementedError(
                    f"Unknown action: {action_type}"
                )
        except (flight.FlightUnimplementedError, flight.FlightInternalError):
            raise
        except Exception as e:
            logger.error("DoAction '%s' failed: %s", action_type, e, exc_info=True)
            raise flight.FlightInternalError(str(e))

    def list_actions(
        self, context: flight.ServerCallContext
    ) -> list[tuple[str, str]]:
        """Return the list of supported actions."""
        return [
            ("health", "Health check — returns server and DataFusion version info."),
            (
                "create_table",
                "Create an empty Delta table with a specified schema.",
            ),
            (
                "execute_dml",
                "Execute a DML statement (DELETE/UPDATE/MERGE) against a Delta table.",
            ),
            (
                "upgrade_protocol",
                "Upgrade the Delta table protocol (reader/writer versions and features).",
            ),
        ]

    # ------------------------------------------------------------------ #
    #  Action handlers
    # ------------------------------------------------------------------ #

    def _action_health(self):
        """Return health status with DataFusion engine info."""
        info = ops.health_check()
        yield flight.Result(to_bytes(info))

    def _action_create_table(self, body: bytes):
        """Create an empty Delta table with the given schema."""
        cmd = parse_json(body)
        result = ops.create_table(
            cmd["path"],
            cmd["schema"],
            cmd.get("storage_account"),
            cmd.get("sas_token"),
            cmd.get("configuration"),
            partition_by=cmd.get("partition_by"),
        )
        yield flight.Result(to_bytes(result))

    def _action_execute_dml(self, body: bytes):
        """Execute a DML statement (DELETE/UPDATE/MERGE) against a Delta table.

        Body JSON: {
            "sql": "DELETE FROM myTable WHERE ...",
            "table_path": "/path/to/delta/table",
            "table_name": "myTable",
            "storage_account": "...",   # optional
            "sas_token": "..."          # optional
        }
        """
        cmd = parse_json(body)
        result = ops.execute_dml(
            cmd["sql"],
            cmd["table_path"],
            cmd["table_name"],
            cmd.get("storage_account"),
            cmd.get("sas_token"),
        )
        yield flight.Result(to_bytes(result))

    def _action_upgrade_protocol(self, body: bytes):
        """Upgrade the Delta table protocol version and optionally enable features.

        Body JSON: {
            "path": "/path/to/delta/table",
            "reader_version": 2,
            "writer_version": 5,
            "reader_features": ["timestampNtz"],   # optional
            "writer_features": ["appendOnly"],      # optional
            "storage_account": "...",               # optional
            "sas_token": "..."                      # optional
        }
        """
        cmd = parse_json(body)
        result = ops.upgrade_table_protocol(
            cmd["path"],
            cmd["reader_version"],
            cmd["writer_version"],
            reader_features=cmd.get("reader_features"),
            writer_features=cmd.get("writer_features"),
            storage_account=cmd.get("storage_account"),
            sas_token=cmd.get("sas_token"),
        )
        yield flight.Result(to_bytes(result))
