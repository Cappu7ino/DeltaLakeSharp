"""Arrow Flight server for Delta table operations.

Architecture notes:
    This server is **stateless** — no per-request data is cached between
    RPC calls.  ``GetFlightInfo`` returns lightweight metadata (schema only)
    and ``DoGet`` always reads fresh data from the Delta table.  This design
    is concurrency-safe, avoids stale-data bugs, and keeps memory usage
    proportional to the data being streamed rather than accumulated.

    For streaming without full materialisation, use V2 (DataFusion).
    PySpark's ``DataFrame`` API requires full collection
    via ``toPandas()``, so V1 materialises the result set in ``DoGet``.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import pyarrow as pa
import pyarrow.flight as flight

from app.utils import parse_json, to_bytes
from app.v1 import delta_operations as ops
from app.v1.spark_manager import SparkManager

logger = logging.getLogger(__name__)


class DeltaFlightServer(flight.FlightServerBase):
    """Arrow Flight server that exposes Delta table operations via PySpark.

    Flight RPC mapping:
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
        spark_manager: SparkManager,
        location: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(location, **kwargs)
        self._spark_manager = spark_manager
        logger.info("DeltaFlightServer initialized at %s", location)

    # ------------------------------------------------------------------ #
    #  GetFlightInfo — describes a dataset (read request)
    # ------------------------------------------------------------------ #

    def get_flight_info(
        self,
        context: flight.ServerCallContext,
        descriptor: flight.FlightDescriptor,
    ) -> flight.FlightInfo:
        """Return FlightInfo for a read request.

        This is a **metadata-only** call — no data is scanned.  The schema
        is read from the Delta log via PySpark and returned with unknown
        row count / byte size (-1).  Actual data is streamed in ``do_get``.

        Two request shapes are recognised:

        1. **Read request** — ``{ "path": "...", "num_rows": 100 }``
           Returns schema from the Delta log.

        2. **SQL query** — ``{ "sql": "...", "table_path": "...",
           "table_name": "..." }``  (table_path/table_name are optional)
           Returns an empty schema with a ticket; actual execution and
           schema discovery happen in ``do_get``.

        The descriptor command is a JSON-encoded dict.
        """
        try:
            cmd = parse_json(descriptor.command)

            if "sql" in cmd:
                # SQL query path — return empty schema with a ticket.
                # Avoid executing the SQL eagerly; let do_get handle it.
                logger.info(
                    "GetFlightInfo (SQL): sql=%s", cmd["sql"][:200]
                )
                ticket = flight.Ticket(descriptor.command)
                endpoint = flight.FlightEndpoint(ticket, [])
                return flight.FlightInfo(
                    pa.schema([]),
                    descriptor,
                    [endpoint],
                    -1,  # total_records unknown
                    -1,  # total_bytes unknown
                )

            # --- Default: read table ---
            path = cmd["path"]

            logger.info("GetFlightInfo for path=%s", path)

            # Read only the schema — no data is scanned.
            schema = ops.get_schema(
                self._spark_manager,
                path,
                cmd.get("storage_account"),
                cmd.get("sas_token"),
                evict_fs_cache=cmd.get("evict_fs_cache", True),
            )

            ticket = flight.Ticket(descriptor.command)
            endpoint = flight.FlightEndpoint(ticket, [])

            return flight.FlightInfo(
                schema,
                descriptor,
                [endpoint],
                -1,  # total_records unknown
                -1,  # total_bytes unknown
            )
        except Exception as e:
            logger.error("GetFlightInfo failed: %s", e, exc_info=True)
            raise flight.FlightInternalError(str(e))

    # ------------------------------------------------------------------ #
    #  DoGet — stream RecordBatches to the client
    # ------------------------------------------------------------------ #

    def do_get(
        self,
        context: flight.ServerCallContext,
        ticket: flight.Ticket,
    ) -> flight.RecordBatchStream:
        """Stream Delta table data as Arrow RecordBatches.

        Two request shapes are recognised (mirroring ``get_flight_info``):

        1. **Read request** — ``{ "path": "..." }``
        2. **SQL query** — ``{ "sql": "...", "table_path": "...",
           "table_name": "..." }``  (table_path/table_name optional)

        Always reads fresh data — no caching.  The ticket contains the
        same JSON command used in GetFlightInfo.
        """
        try:
            cmd = parse_json(ticket.ticket)

            if "sql" in cmd:
                # SQL query path.
                arrow_table = ops.execute_sql_to_arrow(
                    self._spark_manager,
                    cmd["sql"],
                    cmd.get("table_path"),
                    cmd.get("table_name"),
                    cmd.get("storage_account"),
                    cmd.get("sas_token"),
                    evict_fs_cache=cmd.get("evict_fs_cache", True),
                )
                logger.info(
                    "DoGet (SQL) streaming %d rows, %d columns",
                    arrow_table.num_rows,
                    arrow_table.num_columns,
                )
                return flight.RecordBatchStream(arrow_table)

            # --- Default: read table ---
            arrow_table = ops.read_table(
                self._spark_manager,
                cmd["path"],
                cmd.get("num_rows"),
                cmd.get("storage_account"),
                cmd.get("sas_token"),
                evict_fs_cache=cmd.get("evict_fs_cache", True),
            )

            logger.info(
                "DoGet streaming %d rows, %d columns",
                arrow_table.num_rows,
                arrow_table.num_columns,
            )

            return flight.RecordBatchStream(arrow_table)
        except Exception as e:
            logger.error("DoGet failed: %s", e, exc_info=True)
            raise flight.FlightInternalError(str(e))

    # ------------------------------------------------------------------ #
    #  GetSchema — return table schema without data
    # ------------------------------------------------------------------ #

    def get_schema(
        self,
        context: flight.ServerCallContext,
        descriptor: flight.FlightDescriptor,
    ) -> flight.SchemaResult:
        """Return the Arrow schema of a Delta table.

        The descriptor command is a JSON-encoded dict:
            {
                "path": "<delta table path>",
                "storage_account": "...",      # optional
                "sas_token": "..."             # optional
            }
        """
        try:
            cmd = parse_json(descriptor.command)
            schema = ops.get_schema(
                self._spark_manager,
                cmd["path"],
                cmd.get("storage_account"),
                cmd.get("sas_token"),
                evict_fs_cache=cmd.get("evict_fs_cache", True),
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
        """Receive Arrow RecordBatches and write or merge them as a Delta table.

        Dispatches based on ``cmd["operation"]``:

        - ``"write"`` (default) — write data to a Delta table.
        - ``"merge"`` — merge streamed source data into an existing
          Delta table using Spark SQL ``MERGE INTO``.

        The descriptor command is a JSON-encoded dict.  For write::

            {
                "path": "<delta table path>",
                "mode": "overwrite",
                "storage_account": "...",
                "sas_token": "...",
                "configuration": {...}
            }

        For merge::

            {
                "operation": "merge",
                "path": "<delta table path>",
                "predicate": "target.id = source.id",
                "source_alias": "source",
                "target_alias": "target",
                "when_matched_update_all": true,
                "when_not_matched_insert_all": true,
                ...
            }
        """
        try:
            cmd = parse_json(descriptor.command)
            path = cmd["path"]
            operation = cmd.get("operation", "write")

            # PySpark requires full materialisation — read all batches.
            arrow_table = reader.read_all()

            if operation == "merge":
                logger.info(
                    "DoPut MERGE for path=%s, predicate=%s",
                    path,
                    cmd.get("predicate"),
                )
                result = ops.merge_arrow_table(
                    self._spark_manager,
                    path,
                    arrow_table,
                    cmd,
                    cmd.get("storage_account"),
                    cmd.get("sas_token"),
                    evict_fs_cache=cmd.get("evict_fs_cache", True),
                )
            else:
                mode = cmd.get("mode", "overwrite")
                logger.info("DoPut receiving data for path=%s, mode=%s", path, mode)
                result = ops.write_arrow_table(
                    self._spark_manager,
                    path,
                    arrow_table,
                    mode,
                    cmd.get("storage_account"),
                    cmd.get("sas_token"),
                    cmd.get("configuration"),
                    evict_fs_cache=cmd.get("evict_fs_cache", True),
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
    #  DoAction — execute named actions (health, create_table, execute_dml)
    # ------------------------------------------------------------------ #

    def do_action(
        self,
        context: flight.ServerCallContext,
        action: flight.Action,
    ):
        """Dispatch named actions.

        Supported action types:
            "health"       — returns server health and Spark version info.
            "create_table" — creates an empty Delta table with the given schema.
                Body JSON: {
                    "path": "...",
                    "schema": [{"name": "...", "type": "..."}, ...],
                    "storage_account": "...",
                    "sas_token": "...",
                    "configuration": {...}
                    "partition_by": ["col1", ...]
                }
            "execute_dml"  — executes a DML statement (DELETE/UPDATE/MERGE).
                Body JSON: {
                    "sql": "...",
                    "table_path": "...",
                    "table_name": "...",
                    "storage_account": "...",
                    "sas_token": "..."
                }
            "upgrade_protocol"  — upgrades the Delta table protocol version.
                Body JSON: {
                    "path": "...",
                    "reader_version": 2,
                    "writer_version": 5,
                    "reader_features": ["..."],
                    "writer_features": ["..."],
                    "storage_account": "...",
                    "sas_token": "..."
                }
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
            ("health", "Health check — returns server and Spark version info."),
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
        """Return health status including Spark and Delta version info."""
        spark = self._spark_manager.spark
        info = {
            "status": "healthy",
            "spark_version": spark.version,
        }
        yield flight.Result(to_bytes(info))

    def _action_create_table(self, body: bytes):
        """Create an empty Delta table with the given schema."""
        cmd = parse_json(body)
        result = ops.create_table(
            self._spark_manager,
            cmd["path"],
            cmd["schema"],
            cmd.get("storage_account"),
            cmd.get("sas_token"),
            cmd.get("configuration"),
            evict_fs_cache=cmd.get("evict_fs_cache", True),
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
            self._spark_manager,
            cmd["sql"],
            cmd["table_path"],
            cmd["table_name"],
            cmd.get("storage_account"),
            cmd.get("sas_token"),
            evict_fs_cache=cmd.get("evict_fs_cache", True),
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
            self._spark_manager,
            cmd["path"],
            cmd["reader_version"],
            cmd["writer_version"],
            reader_features=cmd.get("reader_features"),
            writer_features=cmd.get("writer_features"),
            storage_account=cmd.get("storage_account"),
            sas_token=cmd.get("sas_token"),
            evict_fs_cache=cmd.get("evict_fs_cache", True),
        )
        yield flight.Result(to_bytes(result))
