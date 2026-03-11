// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! JSON command structures for Flight RPC request payloads.
//!
//! The C# client sends JSON-encoded commands in `FlightDescriptor.command`
//! (for GetFlightInfo / GetSchema) and `Ticket.ticket` (for DoGet).
//! These structures match the V2 Python server's protocol exactly.

use std::collections::HashMap;

use arrow::datatypes::DataType;
use serde::Deserialize;

/// A command for reading an entire Delta table (or a limited number of rows).
///
/// JSON example:
/// ```json
/// {"path": "/data/my_table", "num_rows": 100, "storage_account": "onelake", "sas_token": "..."}
/// ```
#[derive(Debug, Deserialize)]
pub struct ReadCommand {
    /// Path to the Delta table (local or `abfss://` URI).
    pub path: String,

    /// Optional row limit. If present, `SELECT * FROM _tbl LIMIT {num_rows}` is executed.
    #[serde(default)]
    pub num_rows: Option<u64>,

    /// Azure storage account name (e.g. "onelake").
    #[serde(default)]
    pub storage_account: Option<String>,

    /// SAS token for Azure Blob / ADLS Gen2 access.
    #[serde(default)]
    pub sas_token: Option<String>,

    /// Optional Delta table version for time travel.
    /// When set, reads the table at a specific historical version.
    #[serde(default)]
    pub version: Option<i64>,
}

/// A command for executing a SQL query against a Delta table.
///
/// JSON example:
/// ```json
/// {"sql": "SELECT col1 FROM myTable WHERE id > 5", "table_path": "/data/my_table", "table_name": "myTable"}
/// ```
#[derive(Debug, Deserialize)]
pub struct SqlCommand {
    /// The SQL query text.
    pub sql: String,

    /// Path to the Delta table to register before execution.
    /// If `None`, no table is registered (SQL must be self-contained).
    #[serde(default)]
    pub table_path: Option<String>,

    /// Logical name under which the table is registered in DataFusion.
    #[serde(default)]
    pub table_name: Option<String>,

    /// Azure storage account name.
    #[serde(default)]
    pub storage_account: Option<String>,

    /// SAS token for Azure Blob / ADLS Gen2 access.
    #[serde(default)]
    pub sas_token: Option<String>,

    /// Optional Delta table version for time travel.
    /// When set, reads the table at a specific historical version.
    #[serde(default)]
    pub version: Option<i64>,
}

/// Discriminated union over the two command types.
///
/// The discriminator is the presence of the `"sql"` key in the JSON payload
/// (matching V2 behavior in `_is_sql_query()`).
#[derive(Debug)]
pub enum Command {
    Read(ReadCommand),
    Sql(SqlCommand),
}

impl Command {
    /// Parses a JSON byte slice into a `Command`.
    ///
    /// If the JSON contains a `"sql"` key, it is treated as a SQL command;
    /// otherwise, it is treated as a read-table command.
    pub fn parse(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        // Peek at the JSON to check for the "sql" key.
        let value: serde_json::Value = serde_json::from_slice(bytes)?;
        if value.get("sql").is_some() {
            let cmd: SqlCommand = serde_json::from_value(value)?;
            Ok(Command::Sql(cmd))
        } else {
            let cmd: ReadCommand = serde_json::from_value(value)?;
            Ok(Command::Read(cmd))
        }
    }
}

// ========================================================================== //
//  Write-path commands (Phase 3)
// ========================================================================== //

/// A single column definition in a `create_table` schema.
///
/// JSON example: `{"name": "id", "type": "int32"}`
#[derive(Debug, Deserialize)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// Type string — one of the V2-compatible type aliases (see [`arrow_type_from_str`]).
    #[serde(rename = "type")]
    pub data_type: String,
}

/// Command for the `create_table` DoAction.
///
/// JSON example:
/// ```json
/// {
///   "path": "/data/my_table",
///   "schema": [{"name": "id", "type": "int32"}, {"name": "value", "type": "string"}],
///   "configuration": {"delta.key": "val"},
///   "partition_by": ["col1"]
/// }
/// ```
#[derive(Debug, Deserialize)]
pub struct CreateTableCommand {
    /// Path to the Delta table to create.
    pub path: String,

    /// Column definitions for the new table.
    pub schema: Vec<ColumnDef>,

    /// Azure storage account name.
    #[serde(default)]
    pub storage_account: Option<String>,

    /// SAS token.
    #[serde(default)]
    pub sas_token: Option<String>,

    /// Optional Delta table configuration properties.
    #[serde(default)]
    pub configuration: Option<HashMap<String, String>>,

    /// Optional partition columns.
    #[serde(default)]
    pub partition_by: Option<Vec<String>>,
}

/// Command for the `execute_dml` DoAction.
///
/// JSON example:
/// ```json
/// {
///   "sql": "DELETE FROM myTable WHERE id > 5",
///   "table_path": "/data/my_table",
///   "table_name": "myTable"
/// }
/// ```
#[derive(Debug, Deserialize)]
pub struct ExecuteDmlCommand {
    /// The DML SQL statement (DELETE, UPDATE, or MERGE).
    pub sql: String,

    /// Path to the Delta table.
    pub table_path: String,

    /// Logical name under which the table is registered in DataFusion.
    pub table_name: String,

    /// Azure storage account name.
    #[serde(default)]
    pub storage_account: Option<String>,

    /// SAS token.
    #[serde(default)]
    pub sas_token: Option<String>,
}

/// Command for the `upgrade_protocol` DoAction.
///
/// JSON example:
/// ```json
/// {
///   "path": "/data/my_table",
///   "reader_version": 3,
///   "writer_version": 7,
///   "reader_features": ["timestampNtz", "columnMapping"],
///   "writer_features": ["appendOnly", "changeDataFeed"]
/// }
/// ```
#[derive(Debug, Deserialize)]
pub struct UpgradeProtocolCommand {
    /// Path to the Delta table.
    pub path: String,

    /// Target minimum reader protocol version.
    pub reader_version: i32,

    /// Target minimum writer protocol version.
    pub writer_version: i32,

    /// Reader features to enable (camelCase names).
    #[serde(default)]
    pub reader_features: Option<Vec<String>>,

    /// Writer features to enable (camelCase names).
    #[serde(default)]
    pub writer_features: Option<Vec<String>>,

    /// Azure storage account name.
    #[serde(default)]
    pub storage_account: Option<String>,

    /// SAS token.
    #[serde(default)]
    pub sas_token: Option<String>,
}

/// Command embedded in the `FlightDescriptor.command` of a DoPut request.
///
/// The `operation` field discriminates between write (insert/overwrite) and
/// merge operations.  For `operation = "write"` (the default), the batches
/// are written to the Delta table.  For `operation = "merge"`, the batches
/// are the source data for a MERGE INTO operation.
///
/// JSON example (write):
/// ```json
/// {
///   "path": "/data/my_table",
///   "mode": "overwrite",
///   "partition_by": ["col1"]
/// }
/// ```
///
/// JSON example (merge):
/// ```json
/// {
///   "path": "/data/my_table",
///   "operation": "merge",
///   "predicate": "target.id = source.id",
///   "source_alias": "source",
///   "target_alias": "target",
///   "when_matched_update_all": true,
///   "when_not_matched_insert_all": true
/// }
/// ```
#[derive(Debug, Deserialize)]
pub struct DoPutCommand {
    /// Path to the Delta table.
    pub path: String,

    /// Write mode: `"overwrite"` or `"append"`.  Defaults to `"overwrite"`.
    #[serde(default = "default_write_mode")]
    pub mode: String,

    /// Operation type: `"write"` (default) or `"merge"`.
    #[serde(default = "default_operation")]
    pub operation: String,

    /// Azure storage account name.
    #[serde(default)]
    pub storage_account: Option<String>,

    /// SAS token.
    #[serde(default)]
    pub sas_token: Option<String>,

    /// Optional Delta table configuration properties (write mode only).
    #[serde(default)]
    pub configuration: Option<HashMap<String, String>>,

    /// Optional partition columns (write mode only).
    #[serde(default)]
    pub partition_by: Option<Vec<String>>,

    // ------------------------------------------------------------------ //
    //  Merge-specific fields (only used when operation == "merge")
    // ------------------------------------------------------------------ //
    /// Join predicate for MERGE, e.g. `"target.id = source.id"`.
    #[serde(default)]
    pub predicate: Option<String>,

    /// Alias for the source data (default `"source"`).
    #[serde(default)]
    pub source_alias: Option<String>,

    /// Alias for the target Delta table (default `"target"`).
    #[serde(default)]
    pub target_alias: Option<String>,

    /// WHEN MATCHED THEN UPDATE SET * (all columns from source).
    #[serde(default)]
    pub when_matched_update_all: Option<bool>,

    /// WHEN MATCHED THEN UPDATE SET col = expr.
    #[serde(default)]
    pub when_matched_update_set: Option<HashMap<String, String>>,

    /// WHEN MATCHED AND predicate THEN DELETE.
    #[serde(default)]
    pub when_matched_delete_predicate: Option<String>,

    /// WHEN NOT MATCHED THEN INSERT * (all columns).
    #[serde(default)]
    pub when_not_matched_insert_all: Option<bool>,

    /// WHEN NOT MATCHED THEN INSERT (col) VALUES (expr).
    #[serde(default)]
    pub when_not_matched_insert_set: Option<HashMap<String, String>>,

    /// WHEN NOT MATCHED BY SOURCE [AND predicate] THEN DELETE.
    #[serde(default)]
    pub when_not_matched_by_source_delete_predicate: Option<String>,

    /// WHEN NOT MATCHED BY SOURCE THEN UPDATE SET col = expr.
    #[serde(default)]
    pub when_not_matched_by_source_update_set: Option<HashMap<String, String>>,

    /// Predicate gating the WHEN NOT MATCHED BY SOURCE UPDATE clause.
    #[serde(default)]
    pub when_not_matched_by_source_update_predicate: Option<String>,
}

fn default_write_mode() -> String {
    "overwrite".to_string()
}

fn default_operation() -> String {
    "write".to_string()
}

// ========================================================================== //
//  Arrow type mapping (used by create_table)
// ========================================================================== //

/// Maps a V2-compatible type string to an Arrow `DataType`.
///
/// The supported type strings match the V2 Python `_ARROW_TYPE_MAP` exactly.
/// Unknown types fall back to `DataType::Utf8` (matching V2 behavior).
pub fn arrow_type_from_str(type_str: &str) -> DataType {
    match type_str.to_lowercase().as_str() {
        "string" | "utf8" => DataType::Utf8,
        "int" | "int32" | "integer" => DataType::Int32,
        "long" | "int64" | "bigint" => DataType::Int64,
        "short" | "int16" | "smallint" => DataType::Int16,
        "byte" | "int8" | "tinyint" => DataType::Int8,
        "float" | "float32" => DataType::Float32,
        "double" | "float64" => DataType::Float64,
        "boolean" | "bool" => DataType::Boolean,
        "date" | "date32" => DataType::Date32,
        "timestamp" => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        }
        "timestamp_ntz" => DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        "binary" => DataType::Binary,
        _ => DataType::Utf8, // Unknown types fall back to Utf8 (matching V2).
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_read_command_minimal() {
        let json = br#"{"path": "/data/test"}"#;
        let cmd = Command::parse(json).unwrap();
        match cmd {
            Command::Read(r) => {
                assert_eq!(r.path, "/data/test");
                assert!(r.num_rows.is_none());
                assert!(r.storage_account.is_none());
            }
            _ => panic!("Expected Read command"),
        }
    }

    #[test]
    fn parse_read_command_with_num_rows() {
        let json = br#"{"path": "/data/test", "num_rows": 42}"#;
        let cmd = Command::parse(json).unwrap();
        match cmd {
            Command::Read(r) => {
                assert_eq!(r.path, "/data/test");
                assert_eq!(r.num_rows, Some(42));
            }
            _ => panic!("Expected Read command"),
        }
    }

    #[test]
    fn parse_sql_command() {
        let json = br#"{"sql": "SELECT 1", "table_path": "/data/t", "table_name": "t"}"#;
        let cmd = Command::parse(json).unwrap();
        match cmd {
            Command::Sql(s) => {
                assert_eq!(s.sql, "SELECT 1");
                assert_eq!(s.table_path.as_deref(), Some("/data/t"));
                assert_eq!(s.table_name.as_deref(), Some("t"));
            }
            _ => panic!("Expected Sql command"),
        }
    }

    #[test]
    fn parse_sql_command_without_table() {
        let json = br#"{"sql": "SELECT 1"}"#;
        let cmd = Command::parse(json).unwrap();
        match cmd {
            Command::Sql(s) => {
                assert_eq!(s.sql, "SELECT 1");
                assert!(s.table_path.is_none());
                assert!(s.table_name.is_none());
            }
            _ => panic!("Expected Sql command"),
        }
    }

    #[test]
    fn parse_invalid_json_returns_error() {
        let json = b"not json";
        assert!(Command::parse(json).is_err());
    }

    #[test]
    fn parse_read_missing_path_returns_error() {
        let json = br#"{"num_rows": 10}"#;
        // "path" is required for ReadCommand, so this should fail deserialization.
        assert!(Command::parse(json).is_err());
    }

    #[test]
    fn parse_read_command_with_version() {
        let json = br#"{"path": "/data/test", "version": 3}"#;
        let cmd = Command::parse(json).unwrap();
        match cmd {
            Command::Read(r) => {
                assert_eq!(r.path, "/data/test");
                assert_eq!(r.version, Some(3));
            }
            _ => panic!("Expected Read command"),
        }
    }

    #[test]
    fn parse_read_command_without_version_defaults_to_none() {
        let json = br#"{"path": "/data/test"}"#;
        let cmd = Command::parse(json).unwrap();
        match cmd {
            Command::Read(r) => {
                assert!(r.version.is_none());
            }
            _ => panic!("Expected Read command"),
        }
    }

    #[test]
    fn parse_sql_command_with_version() {
        let json =
            br#"{"sql": "SELECT 1", "table_path": "/data/t", "table_name": "t", "version": 5}"#;
        let cmd = Command::parse(json).unwrap();
        match cmd {
            Command::Sql(s) => {
                assert_eq!(s.sql, "SELECT 1");
                assert_eq!(s.version, Some(5));
            }
            _ => panic!("Expected Sql command"),
        }
    }

    // ------------------------------------------------------------------ //
    //  CreateTableCommand tests
    // ------------------------------------------------------------------ //

    #[test]
    fn parse_create_table_minimal() {
        let json = br#"{
            "path": "/data/new_table",
            "schema": [{"name": "id", "type": "int32"}, {"name": "value", "type": "string"}]
        }"#;
        let cmd: CreateTableCommand = serde_json::from_slice(json).unwrap();
        assert_eq!(cmd.path, "/data/new_table");
        assert_eq!(cmd.schema.len(), 2);
        assert_eq!(cmd.schema[0].name, "id");
        assert_eq!(cmd.schema[0].data_type, "int32");
        assert_eq!(cmd.schema[1].name, "value");
        assert_eq!(cmd.schema[1].data_type, "string");
        assert!(cmd.configuration.is_none());
        assert!(cmd.partition_by.is_none());
        assert!(cmd.storage_account.is_none());
    }

    #[test]
    fn parse_create_table_with_options() {
        let json = br#"{
            "path": "/data/new_table",
            "schema": [{"name": "id", "type": "int32"}],
            "configuration": {"delta.appendOnly": "true"},
            "partition_by": ["id"],
            "storage_account": "onelake",
            "sas_token": "tok"
        }"#;
        let cmd: CreateTableCommand = serde_json::from_slice(json).unwrap();
        assert_eq!(cmd.path, "/data/new_table");
        assert_eq!(
            cmd.configuration.as_ref().unwrap()["delta.appendOnly"],
            "true"
        );
        assert_eq!(cmd.partition_by.as_ref().unwrap(), &vec!["id".to_string()]);
        assert_eq!(cmd.storage_account.as_deref(), Some("onelake"));
        assert_eq!(cmd.sas_token.as_deref(), Some("tok"));
    }

    #[test]
    fn parse_create_table_missing_schema_returns_error() {
        let json = br#"{"path": "/data/new_table"}"#;
        assert!(serde_json::from_slice::<CreateTableCommand>(json).is_err());
    }

    // ------------------------------------------------------------------ //
    //  ExecuteDmlCommand tests
    // ------------------------------------------------------------------ //

    #[test]
    fn parse_execute_dml() {
        let json = br#"{
            "sql": "DELETE FROM myTable WHERE id > 5",
            "table_path": "/data/my_table",
            "table_name": "myTable"
        }"#;
        let cmd: ExecuteDmlCommand = serde_json::from_slice(json).unwrap();
        assert_eq!(cmd.sql, "DELETE FROM myTable WHERE id > 5");
        assert_eq!(cmd.table_path, "/data/my_table");
        assert_eq!(cmd.table_name, "myTable");
        assert!(cmd.storage_account.is_none());
    }

    #[test]
    fn parse_execute_dml_with_storage() {
        let json = br#"{
            "sql": "DELETE FROM t WHERE id = 1",
            "table_path": "/data/t",
            "table_name": "t",
            "storage_account": "acct",
            "sas_token": "tok"
        }"#;
        let cmd: ExecuteDmlCommand = serde_json::from_slice(json).unwrap();
        assert_eq!(cmd.storage_account.as_deref(), Some("acct"));
        assert_eq!(cmd.sas_token.as_deref(), Some("tok"));
    }

    #[test]
    fn parse_execute_dml_missing_sql_returns_error() {
        let json = br#"{"table_path": "/data/t", "table_name": "t"}"#;
        assert!(serde_json::from_slice::<ExecuteDmlCommand>(json).is_err());
    }

    #[test]
    fn parse_execute_dml_missing_table_path_returns_error() {
        let json = br#"{"sql": "DELETE FROM t", "table_name": "t"}"#;
        assert!(serde_json::from_slice::<ExecuteDmlCommand>(json).is_err());
    }

    // ------------------------------------------------------------------ //
    //  UpgradeProtocolCommand tests
    // ------------------------------------------------------------------ //

    #[test]
    fn parse_upgrade_protocol_minimal() {
        let json = br#"{
            "path": "/data/t",
            "reader_version": 2,
            "writer_version": 5
        }"#;
        let cmd: UpgradeProtocolCommand = serde_json::from_slice(json).unwrap();
        assert_eq!(cmd.path, "/data/t");
        assert_eq!(cmd.reader_version, 2);
        assert_eq!(cmd.writer_version, 5);
        assert!(cmd.reader_features.is_none());
        assert!(cmd.writer_features.is_none());
    }

    #[test]
    fn parse_upgrade_protocol_with_features() {
        let json = br#"{
            "path": "/data/t",
            "reader_version": 3,
            "writer_version": 7,
            "reader_features": ["timestampNtz", "columnMapping"],
            "writer_features": ["appendOnly", "changeDataFeed"]
        }"#;
        let cmd: UpgradeProtocolCommand = serde_json::from_slice(json).unwrap();
        assert_eq!(cmd.reader_features.as_ref().unwrap().len(), 2);
        assert_eq!(cmd.writer_features.as_ref().unwrap().len(), 2);
        assert_eq!(cmd.reader_features.as_ref().unwrap()[0], "timestampNtz");
    }

    // ------------------------------------------------------------------ //
    //  DoPutCommand tests
    // ------------------------------------------------------------------ //

    #[test]
    fn parse_do_put_write_defaults() {
        let json = br#"{"path": "/data/t"}"#;
        let cmd: DoPutCommand = serde_json::from_slice(json).unwrap();
        assert_eq!(cmd.path, "/data/t");
        assert_eq!(cmd.mode, "overwrite");
        assert_eq!(cmd.operation, "write");
        assert!(cmd.predicate.is_none());
    }

    #[test]
    fn parse_do_put_write_append() {
        let json = br#"{"path": "/data/t", "mode": "append"}"#;
        let cmd: DoPutCommand = serde_json::from_slice(json).unwrap();
        assert_eq!(cmd.mode, "append");
        assert_eq!(cmd.operation, "write");
    }

    #[test]
    fn parse_do_put_write_with_partition_and_config() {
        let json = br#"{
            "path": "/data/t",
            "mode": "overwrite",
            "partition_by": ["year", "month"],
            "configuration": {"delta.appendOnly": "true"}
        }"#;
        let cmd: DoPutCommand = serde_json::from_slice(json).unwrap();
        assert_eq!(
            cmd.partition_by.as_ref().unwrap(),
            &vec!["year".to_string(), "month".to_string()]
        );
        assert_eq!(
            cmd.configuration.as_ref().unwrap()["delta.appendOnly"],
            "true"
        );
    }

    #[test]
    fn parse_do_put_merge() {
        let json = br#"{
            "path": "/data/t",
            "operation": "merge",
            "predicate": "target.id = source.id",
            "source_alias": "source",
            "target_alias": "target",
            "when_matched_update_all": true,
            "when_not_matched_insert_all": true
        }"#;
        let cmd: DoPutCommand = serde_json::from_slice(json).unwrap();
        assert_eq!(cmd.operation, "merge");
        assert_eq!(cmd.predicate.as_deref(), Some("target.id = source.id"));
        assert_eq!(cmd.source_alias.as_deref(), Some("source"));
        assert_eq!(cmd.target_alias.as_deref(), Some("target"));
        assert_eq!(cmd.when_matched_update_all, Some(true));
        assert_eq!(cmd.when_not_matched_insert_all, Some(true));
    }

    #[test]
    fn parse_do_put_merge_with_update_set() {
        let json = br#"{
            "path": "/data/t",
            "operation": "merge",
            "predicate": "target.id = source.id",
            "when_matched_update_set": {"col1": "source.col1", "col2": "source.col2 + 1"}
        }"#;
        let cmd: DoPutCommand = serde_json::from_slice(json).unwrap();
        let update_set = cmd.when_matched_update_set.as_ref().unwrap();
        assert_eq!(update_set["col1"], "source.col1");
        assert_eq!(update_set["col2"], "source.col2 + 1");
    }

    #[test]
    fn parse_do_put_merge_with_delete_and_not_matched_by_source() {
        let json = br#"{
            "path": "/data/t",
            "operation": "merge",
            "predicate": "target.id = source.id",
            "when_matched_delete_predicate": "source.deleted = true",
            "when_not_matched_by_source_delete_predicate": "true",
            "when_not_matched_by_source_update_set": {"active": "'false'"},
            "when_not_matched_by_source_update_predicate": "target.active = true"
        }"#;
        let cmd: DoPutCommand = serde_json::from_slice(json).unwrap();
        assert_eq!(
            cmd.when_matched_delete_predicate.as_deref(),
            Some("source.deleted = true")
        );
        assert_eq!(
            cmd.when_not_matched_by_source_delete_predicate.as_deref(),
            Some("true")
        );
        let update_set = cmd.when_not_matched_by_source_update_set.as_ref().unwrap();
        assert_eq!(update_set["active"], "'false'");
        assert_eq!(
            cmd.when_not_matched_by_source_update_predicate.as_deref(),
            Some("target.active = true")
        );
    }

    // ------------------------------------------------------------------ //
    //  Arrow type mapping tests
    // ------------------------------------------------------------------ //

    #[test]
    fn arrow_type_string_variants() {
        assert_eq!(arrow_type_from_str("string"), DataType::Utf8);
        assert_eq!(arrow_type_from_str("utf8"), DataType::Utf8);
        assert_eq!(arrow_type_from_str("STRING"), DataType::Utf8); // case-insensitive
    }

    #[test]
    fn arrow_type_integer_variants() {
        assert_eq!(arrow_type_from_str("int"), DataType::Int32);
        assert_eq!(arrow_type_from_str("int32"), DataType::Int32);
        assert_eq!(arrow_type_from_str("integer"), DataType::Int32);
        assert_eq!(arrow_type_from_str("long"), DataType::Int64);
        assert_eq!(arrow_type_from_str("int64"), DataType::Int64);
        assert_eq!(arrow_type_from_str("bigint"), DataType::Int64);
        assert_eq!(arrow_type_from_str("short"), DataType::Int16);
        assert_eq!(arrow_type_from_str("int16"), DataType::Int16);
        assert_eq!(arrow_type_from_str("smallint"), DataType::Int16);
        assert_eq!(arrow_type_from_str("byte"), DataType::Int8);
        assert_eq!(arrow_type_from_str("int8"), DataType::Int8);
        assert_eq!(arrow_type_from_str("tinyint"), DataType::Int8);
    }

    #[test]
    fn arrow_type_float_variants() {
        assert_eq!(arrow_type_from_str("float"), DataType::Float32);
        assert_eq!(arrow_type_from_str("float32"), DataType::Float32);
        assert_eq!(arrow_type_from_str("double"), DataType::Float64);
        assert_eq!(arrow_type_from_str("float64"), DataType::Float64);
    }

    #[test]
    fn arrow_type_boolean_variants() {
        assert_eq!(arrow_type_from_str("boolean"), DataType::Boolean);
        assert_eq!(arrow_type_from_str("bool"), DataType::Boolean);
    }

    #[test]
    fn arrow_type_date_and_timestamp() {
        assert_eq!(arrow_type_from_str("date"), DataType::Date32);
        assert_eq!(arrow_type_from_str("date32"), DataType::Date32);
        assert_eq!(
            arrow_type_from_str("timestamp"),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            arrow_type_from_str("timestamp_ntz"),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn arrow_type_binary() {
        assert_eq!(arrow_type_from_str("binary"), DataType::Binary);
    }

    #[test]
    fn arrow_type_unknown_falls_back_to_utf8() {
        assert_eq!(arrow_type_from_str("unknown_type"), DataType::Utf8);
        assert_eq!(arrow_type_from_str(""), DataType::Utf8);
    }
}
