"""Delta table operations: read, schema, execute SQL, create table, merge."""

from __future__ import annotations

import csv
import io
import json
import logging
from typing import Any

import pyarrow as pa
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

from app.v1.spark_manager import SparkManager

logger = logging.getLogger(__name__)

# Cache of registered temp views: table_name → table_path.
# Avoids redundant CREATE OR REPLACE TEMPORARY VIEW calls when the
# same (name, path) pair is requested repeatedly on the singleton
# SparkSession.
_registered_views: dict[str, str] = {}

# Mapping from simple type name strings to PySpark types.
# The keys must cover every alias that the C# client may send, including
# Arrow-style names (int8, int16, int32, int64, uint*) and SQL-style names.
_TYPE_MAP: dict[str, Any] = {
    "string": StringType(),
    "utf8": StringType(),
    "large_utf8": StringType(),
    "large_string": StringType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "int32": IntegerType(),
    "long": LongType(),
    "bigint": LongType(),
    "int64": LongType(),
    "short": ShortType(),
    "smallint": ShortType(),
    "int16": ShortType(),
    "byte": ByteType(),
    "tinyint": ByteType(),
    "int8": ByteType(),
    "float": FloatType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "bool": BooleanType(),
    "date": DateType(),
    "timestamp": TimestampType(),
    "timestamp_ntz": TimestampNTZType(),
}


def _configure_if_needed(
    spark_manager: SparkManager,
    storage_account: str | None,
    sas_token: str | None,
    path: str | None = None,
    evict_fs_cache: bool = True,
) -> None:
    """Configure storage authentication if credentials are provided.

    When *path* is an ``abfss://`` URI the container name (the part before
    ``@`` in the authority) is extracted and forwarded to
    ``SparkManager.configure_storage`` so that Hadoop's cached
    ``FileSystem`` instance for that exact authority can be evicted.
    Without this, a stale cached instance (created by an earlier operation
    with a different SAS scope) would keep using the old token.

    Args:
        evict_fs_cache: Whether to evict Hadoop's cached ``FileSystem``
            instance before configuring storage.  Defaults to ``True``
            (current behaviour).  Set to ``False`` for benchmark
            scenarios where the same table is read repeatedly and stale
            cache is not a concern.
    """
    if storage_account and sas_token:
        container: str | None = None
        if path and path.startswith("abfss://"):
            # abfss://{container}@{account}.{dfs_host}/{rest}
            try:
                authority = path.split("//", 1)[1].split("/", 1)[0]
                container = authority.split("@", 1)[0]
            except (IndexError, ValueError):
                container = None
        spark_manager.configure_storage(
            storage_account, sas_token, container=container,
            evict_fs_cache=evict_fs_cache,
        )


def read_table(
    spark_manager: SparkManager,
    path: str,
    num_rows: int | None = None,
    storage_account: str | None = None,
    sas_token: str | None = None,
    evict_fs_cache: bool = True,
) -> pa.Table:
    """Read a Delta table and return the result as a PyArrow Table.

    The conversion uses the PySpark schema to build a target Arrow schema,
    then casts the result of ``pa.Table.from_pandas()`` to that schema.

    **Why the cast is required** (empirically verified with PySpark 3.5.5,
    PyArrow 17.0.0, Pandas 2.0.3):

    * When a column has **no nulls**, ``toPandas()`` preserves the original
      numpy dtype (e.g. ``int32``) and ``from_pandas()`` produces the
      matching Arrow type.  No cast needed in this case.
    * When a column has **nulls**, numpy has no nullable integer type, so
      pandas widens to ``float64`` to represent ``NaN``.  ``from_pandas()``
      then faithfully produces Arrow ``double`` instead of the original
      ``int32`` / ``int64`` / ``int16`` / ``int8``.  The explicit cast
      corrects this back to the original integer type.

    Without the cast, the C# Arrow Flight client would see ``double``
    columns where it expects integer columns for any table that contains
    NULL values in integer fields — a very common scenario for Delta tables.

    Args:
        spark_manager: The SparkManager instance.
        path: Path to the Delta table (local or abfss://).
        num_rows: Maximum number of rows to return, or ``None`` to read
            all rows.
        storage_account: Optional storage account name for ABFSS auth.
        sas_token: Optional SAS token for ABFSS auth.
        evict_fs_cache: Whether to evict the Hadoop FileSystem cache.

    Returns:
        A PyArrow Table containing the requested rows.
    """
    _configure_if_needed(spark_manager, storage_account, sas_token, path,
                         evict_fs_cache=evict_fs_cache)
    spark = spark_manager.spark

    df = spark.read.format("delta").load(path)
    if num_rows is not None:
        logger.info("Reading Delta table at: %s (limit=%d)", path, num_rows)
        df = df.limit(num_rows)
    else:
        logger.info("Reading Delta table at: %s (all rows)", path)
    spark_schema = df.schema

    # Build the target Arrow schema from PySpark's schema, which preserves
    # the original Delta column types (e.g. int32 stays int32, not double).
    target_fields = []
    for f in spark_schema.fields:
        target_fields.append(
            pa.field(f.name, _spark_type_to_arrow(f.dataType), nullable=f.nullable)
        )
    target_schema = pa.schema(target_fields)

    # Convert via pandas, then cast to the target schema.  This is required
    # because nullable integer columns become float64 in pandas (numpy has
    # no nullable int), causing from_pandas() to produce Arrow double
    # instead of the correct integer type.
    arrow_table = pa.Table.from_pandas(df.toPandas())
    try:
        arrow_table = arrow_table.cast(target_schema)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as cast_err:
        logger.warning(
            "Could not cast Arrow table to target schema, returning as-is: %s",
            cast_err,
        )

    logger.info(
        "Read %d rows, %d columns from %s",
        arrow_table.num_rows,
        arrow_table.num_columns,
        path,
    )
    return arrow_table


def _arrow_type_to_spark(arrow_type: pa.DataType):
    """Map a PyArrow DataType to a PySpark DataType.

    This is the reverse of ``_spark_type_to_arrow`` and is used by
    ``write_arrow_table`` to build an explicit PySpark schema from the
    incoming Arrow schema.  Without it, ``spark.createDataFrame(pdf)``
    infers the schema from pandas, which loses the distinction between
    ``TimestampType`` (tz-aware) and ``TimestampNTZType`` (tz-naive) —
    pandas tz-naive timestamps are always inferred as ``TimestampType``.
    """
    if pa.types.is_timestamp(arrow_type):
        if arrow_type.tz is not None:
            return TimestampType()
        return TimestampNTZType()
    if pa.types.is_decimal(arrow_type):
        return DecimalType(arrow_type.precision, arrow_type.scale)

    mapping = {
        pa.utf8(): StringType(),
        pa.large_utf8(): StringType(),
        pa.large_string(): StringType(),
        pa.int8(): ByteType(),
        pa.int16(): ShortType(),
        pa.int32(): IntegerType(),
        pa.int64(): LongType(),
        pa.float32(): FloatType(),
        pa.float64(): DoubleType(),
        pa.bool_(): BooleanType(),
        pa.date32(): DateType(),
    }
    return mapping.get(arrow_type, StringType())


def _arrow_schema_to_spark(arrow_schema: pa.Schema) -> StructType:
    """Convert a PyArrow Schema to a PySpark StructType.

    Uses ``_arrow_type_to_spark`` for each field, preserving nullable flags.
    """
    fields = []
    for field in arrow_schema:
        spark_type = _arrow_type_to_spark(field.type)
        fields.append(StructField(field.name, spark_type, field.nullable))
    return StructType(fields)


def _spark_type_to_arrow(spark_type) -> pa.DataType:
    """Map a PySpark DataType to a standard PyArrow type.

    This avoids going through pandas (which can produce large_utf8 or object
    types that the C# Apache.Arrow 18.0.0 GetSchema IPC path deserializes
    as NullType).
    """
    type_name = type(spark_type).__name__

    # DecimalType carries per-instance precision and scale, so it must be
    # handled before the static lookup table.
    if type_name == "DecimalType":
        return pa.decimal128(spark_type.precision, spark_type.scale)

    mapping = {
        "StringType": pa.utf8(),
        "IntegerType": pa.int32(),
        "LongType": pa.int64(),
        "DoubleType": pa.float64(),
        "FloatType": pa.float32(),
        "BooleanType": pa.bool_(),
        "DateType": pa.date32(),
        "TimestampType": pa.timestamp("us", tz="UTC"),
        "TimestampNTZType": pa.timestamp("us"),
        "BinaryType": pa.binary(),
        "ShortType": pa.int16(),
        "ByteType": pa.int8(),
    }
    return mapping.get(type_name, pa.utf8())


def get_schema(
    spark_manager: SparkManager,
    path: str,
    storage_account: str | None = None,
    sas_token: str | None = None,
    evict_fs_cache: bool = True,
) -> pa.Schema:
    """Return the Arrow schema of a Delta table.

    Builds the PyArrow schema directly from PySpark's StructType schema
    instead of going through ``from_pandas()``, which can produce types
    (e.g. ``large_utf8``) that the C# Arrow Flight ``GetSchema`` IPC
    deserializer does not handle correctly.

    Args:
        spark_manager: The SparkManager instance.
        path: Path to the Delta table.
        storage_account: Optional storage account name for ABFSS auth.
        sas_token: Optional SAS token for ABFSS auth.
        evict_fs_cache: Whether to evict the Hadoop FileSystem cache.

    Returns:
        A PyArrow Schema describing the table columns and types.
    """
    _configure_if_needed(spark_manager, storage_account, sas_token, path,
                         evict_fs_cache=evict_fs_cache)
    spark = spark_manager.spark

    logger.info("Getting schema for Delta table at: %s", path)
    df = spark.read.format("delta").load(path)
    spark_schema = df.schema

    fields = []
    for f in spark_schema.fields:
        arrow_type = _spark_type_to_arrow(f.dataType)
        fields.append(pa.field(f.name, arrow_type, nullable=f.nullable))

    schema = pa.schema(fields)
    logger.info(
        "Schema for %s: %s",
        path,
        [(f.name, str(f.type)) for f in schema],
    )
    return schema


def _ensure_temp_view_registered(
    spark: SparkSession, table_name: str, table_path: str
) -> None:
    """Register a Delta table as a temp view, skipping if already cached.

    Checks the module-level ``_registered_views`` cache first.  If
    *table_name* is already mapped to the same *table_path* the
    registration SQL is skipped entirely.  Otherwise the view is
    (re-)created and the cache updated.

    Temp views are session-scoped and unrelated to the Spark catalog,
    so ``spark.catalog.tableExists`` is intentionally **not** used here.
    The ``_registered_views`` dict is the authoritative source of truth
    for views created on the singleton SparkSession.

    This avoids the overhead of ``CREATE OR REPLACE TEMPORARY VIEW`` on
    every request when the same table is queried repeatedly.
    """
    cached_path = _registered_views.get(table_name)
    if cached_path == table_path:
        logger.debug(
            "Temp view '%s' already registered for path '%s', skipping.",
            table_name,
            table_path,
        )
        return

    register_sql = (
        f"CREATE OR REPLACE TEMPORARY VIEW {table_name} "
        f"USING delta OPTIONS (path '{table_path}')"
    )
    logger.info("Registering temp view: %s", register_sql)
    spark.sql(register_sql)
    _registered_views[table_name] = table_path


def execute_sql_to_arrow(
    spark_manager: SparkManager,
    sql: str,
    table_path: str | None = None,
    table_name: str | None = None,
    storage_account: str | None = None,
    sas_token: str | None = None,
    evict_fs_cache: bool = True,
) -> pa.Table:
    """Execute a read-oriented SQL query and return the result as a PyArrow Table.

    When *table_path* and *table_name* are provided, a temporary view is
    registered first (``CREATE OR REPLACE TEMPORARY VIEW {table_name} USING
    delta OPTIONS (path '{table_path}')``), then *sql* is executed.  When
    omitted, the SQL is executed directly via ``spark.sql()``.

    The conversion uses the same PySpark → Arrow schema cast approach as
    ``read_table`` to ensure nullable integer columns are not widened to
    ``float64`` by the pandas round-trip.

    Args:
        spark_manager: The SparkManager instance.
        sql: The SQL query to execute (SELECT, SHOW, DESCRIBE, etc.).
        table_path: Optional path to a Delta table to register before executing.
        table_name: Optional logical table name to use in the SQL query.
        storage_account: Optional storage account name for ABFSS auth.
        sas_token: Optional SAS token for ABFSS auth.
        evict_fs_cache: Whether to evict the Hadoop FileSystem cache.

    Returns:
        A PyArrow Table containing the query results.
    """
    _configure_if_needed(
        spark_manager, storage_account, sas_token, table_path,
        evict_fs_cache=evict_fs_cache,
    )
    spark = spark_manager.spark

    # Optionally register the Delta table as a temp view so the SQL can
    # reference it by name.  Uses a cache to skip re-registration when
    # the same (name, path) pair was already registered on this session.
    if table_path is not None and table_name is not None:
        _ensure_temp_view_registered(spark, table_name, table_path)
    elif table_path is None and table_name is not None:
        # No path to register — the caller expects table_name to already
        # exist in the Spark catalog.  Validate up-front so errors are
        # clear rather than letting Spark throw a cryptic AnalysisException.
        if not spark.catalog.tableExists(table_name):
            raise ValueError(
                f"Table '{table_name}' does not exist in the Spark catalog "
                f"and no table_path was provided to register it."
            )

    logger.info("Executing SQL (streaming): %s", sql[:200])
    result_df = spark.sql(sql)
    spark_schema = result_df.schema

    # Build the target Arrow schema from PySpark's schema.
    target_fields = []
    for f in spark_schema.fields:
        target_fields.append(
            pa.field(f.name, _spark_type_to_arrow(f.dataType), nullable=f.nullable)
        )
    target_schema = pa.schema(target_fields)

    # Convert via pandas, then cast to the target schema.
    arrow_table = pa.Table.from_pandas(result_df.toPandas())
    try:
        arrow_table = arrow_table.cast(target_schema)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as cast_err:
        logger.warning(
            "Could not cast Arrow table to target schema, returning as-is: %s",
            cast_err,
        )

    logger.info(
        "SQL query returned %d rows, %d columns",
        arrow_table.num_rows,
        arrow_table.num_columns,
    )
    return arrow_table


def create_table(
    spark_manager: SparkManager,
    path: str,
    schema_fields: list[dict[str, str]],
    storage_account: str | None = None,
    sas_token: str | None = None,
    configuration: dict[str, str] | None = None,
    evict_fs_cache: bool = True,
    partition_by: list[str] | None = None,
) -> dict[str, Any]:
    """Create an empty Delta table with the given schema.

    Args:
        spark_manager: The SparkManager instance.
        path: Path where the Delta table will be created.
        schema_fields: List of dicts with 'name' and 'type' keys,
            e.g. [{'name': 'id', 'type': 'int'}, {'name': 'value', 'type': 'string'}].
        storage_account: Optional storage account name for ABFSS auth.
        sas_token: Optional SAS token for ABFSS auth.
        configuration: Optional Delta table configuration properties,
            e.g. {"delta.columnMapping.mode": "name"}.  PySpark's Delta
            Lake fully supports these (unlike delta-rs in the V2 backend).
        evict_fs_cache: Whether to evict the Hadoop FileSystem cache.
        partition_by: Optional list of column names to partition by.

    Returns:
        A dict with 'success' (bool) and 'message' (str).
    """
    _configure_if_needed(spark_manager, storage_account, sas_token, path,
                         evict_fs_cache=evict_fs_cache)
    spark = spark_manager.spark

    logger.info("Creating Delta table at: %s (config=%s, partition_by=%s)", path, configuration, partition_by)
    try:
        spark_schema = StructType(
            [
                StructField(
                    f["name"],
                    _TYPE_MAP.get(f["type"].lower(), StringType()),
                    True,
                )
                for f in schema_fields
            ]
        )
        empty_df = spark.createDataFrame([], spark_schema)

        writer = empty_df.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if configuration:
            for key, value in configuration.items():
                writer = writer.option(key, value)
        writer.save(path)

        return {
            "success": True,
            "message": f"Delta table created at {path}.",
        }
    except Exception as e:
        logger.error("Failed to create Delta table: %s", e)
        return {
            "success": False,
            "message": str(e),
        }


def _try_read_existing_schema(
    spark: "SparkSession",
    path: str,
) -> StructType | None:
    """Attempt to read the PySpark schema of an existing Delta table.

    Returns ``None`` if the table does not exist or cannot be read (e.g.
    first write to a new path).  This is used by ``write_arrow_table``
    to cast the incoming DataFrame to the target schema so that type
    mismatches caused by the Arrow → pandas → PySpark round-trip (e.g.
    int32 widened to int64) do not trigger ``DELTA_FAILED_TO_MERGE_FIELDS``
    errors, especially on tables with elevated protocol versions (column
    mapping, deletion vectors).
    """
    try:
        return spark.read.format("delta").load(path).schema
    except Exception:
        return None


def write_arrow_table(
    spark_manager: SparkManager,
    path: str,
    arrow_table: pa.Table,
    mode: str = "overwrite",
    storage_account: str | None = None,
    sas_token: str | None = None,
    configuration: dict[str, str] | None = None,
    evict_fs_cache: bool = True,
    partition_by: list[str] | None = None,
) -> dict[str, Any]:
    """Write a PyArrow Table as a Delta table.

    This is used by DoPut to receive data from the client and persist it.

    When appending to an existing table, the function reads the target
    table's PySpark schema and uses it to create the DataFrame.  This
    avoids type mismatches caused by the Arrow → pandas → PySpark
    round-trip (e.g. ``int32`` silently widened to ``int64`` by pandas),
    which would otherwise trigger ``DELTA_FAILED_TO_MERGE_FIELDS`` errors
    on tables with elevated protocol versions (column mapping, deletion
    vectors).

    Args:
        spark_manager: The SparkManager instance.
        path: Path where the Delta table will be written.
        arrow_table: The PyArrow Table containing the data to write.
        mode: Spark write mode ('overwrite', 'append', etc.).
        storage_account: Optional storage account name for ABFSS auth.
        sas_token: Optional SAS token for ABFSS auth.
        configuration: Optional Delta table configuration properties,
            e.g. {"delta.columnMapping.mode": "name"}.  Applied as writer
            options so that PySpark's Delta writer honours them.
        evict_fs_cache: Whether to evict the Hadoop FileSystem cache.
        partition_by: Optional list of column names to partition by.

    Returns:
        A dict with 'success' (bool), 'message' (str), and 'rows_written' (int).
    """
    _configure_if_needed(spark_manager, storage_account, sas_token, path,
                         evict_fs_cache=evict_fs_cache)
    spark = spark_manager.spark

    logger.info(
        "Writing %d rows to Delta table at: %s (mode=%s, config=%s)",
        arrow_table.num_rows,
        path,
        mode,
        configuration,
    )
    try:
        pdf = arrow_table.to_pandas()

        # When appending to an existing table, read its schema and use it
        # to create the DataFrame.  This ensures exact type matching and
        # avoids pandas-induced type widening (int32 → int64) that causes
        # DELTA_FAILED_TO_MERGE_FIELDS on tables with column mapping or
        # deletion vectors (elevated protocol versions).
        existing_schema = None
        if mode == "append":
            existing_schema = _try_read_existing_schema(spark, path)
            if existing_schema is not None:
                logger.info(
                    "Using existing table schema for append: %s",
                    existing_schema.simpleString(),
                )

        if existing_schema is not None:
            df = spark.createDataFrame(pdf, schema=existing_schema)
        else:
            # Build an explicit PySpark schema from the Arrow schema so
            # that type distinctions lost in the Arrow → pandas round-trip
            # are preserved.  In particular, pandas tz-naive timestamps are
            # inferred as TimestampType by PySpark, but the Arrow schema
            # may specify timestamp(us) (no tz) which should map to
            # TimestampNTZType.  Without this, Delta tables created via
            # overwrite would always get TimestampType columns even when
            # the client sent timestamp_ntz.
            inferred_schema = _arrow_schema_to_spark(arrow_table.schema)
            logger.info(
                "Using Arrow-derived schema for write: %s",
                inferred_schema.simpleString(),
            )
            df = spark.createDataFrame(pdf, schema=inferred_schema)

        writer = df.write.format("delta").mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if configuration:
            for key, value in configuration.items():
                writer = writer.option(key, value)
        writer.save(path)

        return {
            "success": True,
            "message": f"Wrote {arrow_table.num_rows} rows to {path}.",
            "rows_written": arrow_table.num_rows,
        }
    except Exception as e:
        logger.error("Failed to write Delta table: %s", e)
        return {
            "success": False,
            "message": str(e),
        }


def execute_dml(
    spark_manager: SparkManager,
    sql: str,
    table_path: str,
    table_name: str,
    storage_account: str | None = None,
    sas_token: str | None = None,
    evict_fs_cache: bool = True,
) -> dict[str, Any]:
    """Execute a DML statement (DELETE, UPDATE, MERGE) against a Delta table.

    This function encapsulates the two-step pattern that Spark requires for
    DML on path-based Delta tables:

    1. Register a temporary view that points at the Delta table path.
    2. Execute the DML SQL statement against the temporary view.

    The caller passes the logical ``table_name`` used in the SQL statement
    and the physical ``table_path`` on disk / ABFSS.  The temp view is
    created with ``CREATE OR REPLACE TEMPORARY VIEW {table_name} USING
    delta OPTIONS (path '{table_path}')``.

    Args:
        spark_manager: The SparkManager instance.
        sql: The DML SQL statement (DELETE, UPDATE, or MERGE).
        table_path: Physical path to the Delta table (local or abfss://).
        table_name: Logical table name referenced in *sql*.
        storage_account: Optional storage account name for ABFSS auth.
        sas_token: Optional SAS token for ABFSS auth.
        evict_fs_cache: Whether to evict Hadoop FileSystem cache before ops.

    Returns:
        A dict with 'success' (bool), 'message' (str), and optionally
        'result' (list of row dicts from Spark's DML output).
    """
    _configure_if_needed(
        spark_manager, storage_account, sas_token, table_path,
        evict_fs_cache=evict_fs_cache,
    )
    spark = spark_manager.spark

    logger.info(
        "Executing DML: table_path=%s, table_name=%s, sql=%s",
        table_path,
        table_name,
        sql[:200],
    )
    try:
        # Step 1: Register the Delta table as a temporary view (cached).
        _ensure_temp_view_registered(spark, table_name, table_path)

        # Step 2: Execute the DML statement.
        result_df = spark.sql(sql)
        rows = result_df.collect()

        return {
            "success": True,
            "message": "DML executed successfully.",
            "result": [row.asDict() for row in rows] if rows else [],
        }
    except Exception as e:
        logger.error("DML execution failed: %s", e)
        return {
            "success": False,
            "message": str(e),
        }


def merge_arrow_table(
    spark_manager: SparkManager,
    path: str,
    arrow_table: pa.Table,
    cmd: dict[str, Any],
    storage_account: str | None = None,
    sas_token: str | None = None,
    evict_fs_cache: bool = True,
) -> dict[str, Any]:
    """Merge source data (Arrow Table) into an existing Delta table.

    Uses the delta-spark ``DeltaTable.merge()`` builder API to perform
    the merge programmatically (no raw SQL assembly).  The source data
    is converted to a Spark DataFrame and passed directly to
    ``DeltaTable.forPath(spark, path).alias(target).merge(
    source_df.alias(source), predicate)``.

    PySpark inherently requires full materialisation of the source data
    (no streaming merge support), which is why this accepts a
    ``pa.Table`` rather than a ``RecordBatchReader``.

    The merge behaviour is controlled by keys in *cmd*:

    =============================================  ===============================
    Key                                            Effect
    =============================================  ===============================
    ``predicate`` *(required)*                     Join predicate, e.g.
                                                   ``"target.id = source.id"``
    ``source_alias``                               Alias for source (default
                                                   ``"source"``)
    ``target_alias``                               Alias for target (default
                                                   ``"target"``)
    ``when_matched_update_all``                    ``True`` → update all columns
    ``when_matched_update_set``                    ``dict`` of column assignments
    ``when_matched_delete_predicate``              Predicate string for matched
                                                   delete
    ``when_not_matched_insert_all``                ``True`` → insert all columns
    ``when_not_matched_insert_set``                ``dict`` of column assignments
    ``when_not_matched_by_source_delete_predicate``  Predicate string
    ``when_not_matched_by_source_update_set``      ``dict`` of column assignments
    ``when_not_matched_by_source_update_predicate``  Predicate string (required
                                                   with above)
    =============================================  ===============================

    Args:
        spark_manager: The SparkManager instance.
        path: Physical path to the target Delta table.
        arrow_table: The source data as a PyArrow Table.
        cmd: Parsed command dict with merge parameters.
        storage_account: Optional storage account name for ABFSS auth.
        sas_token: Optional SAS token for ABFSS auth.
        evict_fs_cache: Whether to evict Hadoop FileSystem cache before ops.

    Returns:
        A dict with ``success``, ``message``, and ``result`` (empty
        list — delta-spark's ``execute()`` returns ``None``).
    """
    _configure_if_needed(
        spark_manager, storage_account, sas_token, path,
        evict_fs_cache=evict_fs_cache,
    )
    spark = spark_manager.spark

    predicate = cmd["predicate"]
    source_alias = cmd.get("source_alias", "source")
    target_alias = cmd.get("target_alias", "target")

    logger.info(
        "Merging %d source rows into Delta table at: %s "
        "(predicate=%s, source_alias=%s, target_alias=%s)",
        arrow_table.num_rows, path, predicate, source_alias, target_alias,
    )
    try:
        # Step 1: Convert Arrow source data to Spark DataFrame.
        pdf = arrow_table.to_pandas()
        source_schema = _arrow_schema_to_spark(arrow_table.schema)
        source_df = spark.createDataFrame(pdf, schema=source_schema)

        # Step 2: Open the target Delta table via delta-spark API.
        dt = DeltaTable.forPath(spark, path)

        # Step 3: Build the merge using DeltaMergeBuilder.
        merger = dt.alias(target_alias).merge(
            source=source_df.alias(source_alias),
            condition=predicate,
        )

        # -- WHEN MATCHED clauses ----------------------------------------
        if cmd.get("when_matched_update_all"):
            merger = merger.whenMatchedUpdateAll()
        elif cmd.get("when_matched_update_set"):
            merger = merger.whenMatchedUpdate(
                set=cmd["when_matched_update_set"],
            )

        if cmd.get("when_matched_delete_predicate"):
            merger = merger.whenMatchedDelete(
                condition=cmd["when_matched_delete_predicate"],
            )

        # -- WHEN NOT MATCHED clauses ------------------------------------
        if cmd.get("when_not_matched_insert_all"):
            merger = merger.whenNotMatchedInsertAll()
        elif cmd.get("when_not_matched_insert_set"):
            merger = merger.whenNotMatchedInsert(
                values=cmd["when_not_matched_insert_set"],
            )

        # -- WHEN NOT MATCHED BY SOURCE clauses --------------------------
        if cmd.get("when_not_matched_by_source_delete_predicate"):
            merger = merger.whenNotMatchedBySourceDelete(
                condition=cmd["when_not_matched_by_source_delete_predicate"],
            )

        if cmd.get("when_not_matched_by_source_update_set"):
            merger = merger.whenNotMatchedBySourceUpdate(
                condition=cmd.get(
                    "when_not_matched_by_source_update_predicate"
                ),
                set=cmd["when_not_matched_by_source_update_set"],
            )

        # Step 4: Execute the merge.
        merger.execute()

        logger.info("Merge completed for table at: %s", path)
        return {
            "success": True,
            "message": "Merge completed.",
            "result": [],
        }
    except Exception as e:
        logger.error("Merge failed: %s", e)
        return {
            "success": False,
            "message": str(e),
        }


# Mapping from Delta protocol feature names to the table property that
# *activates* the feature.  Features listed here have a companion property
# that must be set via ``ALTER TABLE ... SET TBLPROPERTIES`` — merely
# adding the feature to the protocol (``delta.feature.X = 'supported'``)
# marks the feature as *supported* but does NOT make it *active*.
#
# Features NOT in this map (e.g. ``timestampNtz``, ``invariants``,
# ``domainMetadata``) have no companion property and are correctly enabled
# via the ``delta.feature.{name} = 'supported'`` convention.
_FEATURE_TBLPROPERTIES: dict[str, dict[str, str]] = {
    "appendOnly":      {"delta.appendOnly": "true"},
    "changeDataFeed":  {"delta.enableChangeDataFeed": "true"},
    "columnMapping":   {"delta.columnMapping.mode": "name"},
    "deletionVectors": {"delta.enableDeletionVectors": "true"},
    "rowTracking":     {"delta.enableRowTracking": "true"},
}


def upgrade_table_protocol(
    spark_manager: SparkManager,
    path: str,
    reader_version: int,
    writer_version: int,
    reader_features: list[str] | None = None,
    writer_features: list[str] | None = None,
    storage_account: str | None = None,
    sas_token: str | None = None,
    evict_fs_cache: bool = True,
) -> dict[str, Any]:
    """Upgrade the Delta protocol version of an existing table.

    Uses ``DeltaTable.forPath(spark, path).upgradeTableProtocol(rv, wv)``
    to bump the reader and writer versions.

    Individual table features are enabled via
    ``ALTER TABLE ... SET TBLPROPERTIES`` using the correct activation
    property for each feature.  Features that have a companion property
    (e.g. ``delta.columnMapping.mode`` for *columnMapping*,
    ``delta.enableChangeDataFeed`` for *changeDataFeed*) are set through
    that property — Spark automatically upgrades the protocol as a
    side-effect.  Features without a companion property (e.g.
    ``timestampNtz``) use the ``delta.feature.{name} = 'supported'``
    convention.

    After the upgrade the current protocol is read back via
    ``DESCRIBE DETAIL`` and returned in the result.

    Args:
        spark_manager: The SparkManager instance.
        path: Physical path to the Delta table (local or abfss://).
        reader_version: Target minimum reader version (>= 1).
        writer_version: Target minimum writer version (>= 1).
        reader_features: Optional list of reader features to enable.
        writer_features: Optional list of writer features to enable.
        storage_account: Optional storage account name for ABFSS auth.
        sas_token: Optional SAS token for ABFSS auth.
        evict_fs_cache: Whether to evict Hadoop FileSystem cache before ops.

    Returns:
        A dict with 'success', 'message', and 'result' containing the
        protocol versions after upgrade.
    """
    _configure_if_needed(
        spark_manager, storage_account, sas_token, path,
        evict_fs_cache=evict_fs_cache,
    )
    spark = spark_manager.spark

    logger.info(
        "Upgrading table protocol: path=%s, reader=%d, writer=%d, "
        "reader_features=%s, writer_features=%s",
        path, reader_version, writer_version,
        reader_features, writer_features,
    )
    try:
        # Step 1: Bump reader/writer version.
        dt = DeltaTable.forPath(spark, path)
        dt.upgradeTableProtocol(reader_version, writer_version)

        # Step 2: Enable individual features if requested.
        all_features: list[str] = []
        if reader_features:
            all_features.extend(reader_features)
        if writer_features:
            all_features.extend(writer_features)

        if all_features:
            # Build a single TBLPROPERTIES dict for all requested features.
            # Features with a companion property use that property (which
            # also auto-upgrades the protocol in Spark); features without
            # one use the delta.feature.{name} = 'supported' convention.
            props: dict[str, str] = {}
            seen: set[str] = set()
            for feature in all_features:
                if feature in seen:
                    continue
                seen.add(feature)
                companion = _FEATURE_TBLPROPERTIES.get(feature)
                if companion:
                    props.update(companion)
                else:
                    props[f"delta.feature.{feature}"] = "supported"

            # Use the Delta path directly — temp views cannot be altered
            # with SET TBLPROPERTIES in Spark.
            prop_pairs = ", ".join(
                f"'{k}' = '{v}'" for k, v in props.items()
            )
            spark.sql(
                f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES "
                f"({prop_pairs})"
            )

        # Step 3: Read back the current protocol via DESCRIBE DETAIL.
        detail_df = spark.sql(f"DESCRIBE DETAIL delta.`{path}`")
        detail_row = detail_df.collect()[0]
        result_info = {
            "minReaderVersion": detail_row["minReaderVersion"],
            "minWriterVersion": detail_row["minWriterVersion"],
        }

        # tableFeatures — combined reader+writer list from DESCRIBE DETAIL.
        table_features = detail_row["tableFeatures"]
        if table_features is not None:
            result_info["tableFeatures"] = list(table_features)

        # configuration — table properties from DESCRIBE DETAIL.
        properties = detail_row["properties"]
        if properties:
            result_info["metadata.configuration"] = dict(properties)

        logger.info("Protocol upgraded for table at: %s", path)
        return {
            "success": True,
            "message": "Protocol upgraded.",
            "result": [result_info],
        }
    except Exception as e:
        logger.error("Protocol upgrade failed: %s", e)
        return {
            "success": False,
            "message": str(e),
        }
