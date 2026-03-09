// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! JSON command structures for Flight RPC request payloads.
//!
//! The C# client sends JSON-encoded commands in `FlightDescriptor.command`
//! (for GetFlightInfo / GetSchema) and `Ticket.ticket` (for DoGet).
//! These structures match the V2 Python server's protocol exactly.

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
}
