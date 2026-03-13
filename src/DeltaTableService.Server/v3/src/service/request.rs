// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! JSON command structures for transport-neutral V3 request payloads.

use std::collections::HashMap;

use arrow::datatypes::DataType;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ReadCommand {
    pub path: String,
    #[serde(default)]
    pub num_rows: Option<u64>,
    #[serde(default)]
    pub storage_account: Option<String>,
    #[serde(default)]
    pub sas_token: Option<String>,
    #[serde(default)]
    pub version: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct SqlCommand {
    pub sql: String,
    #[serde(default)]
    pub table_path: Option<String>,
    #[serde(default)]
    pub table_name: Option<String>,
    #[serde(default)]
    pub storage_account: Option<String>,
    #[serde(default)]
    pub sas_token: Option<String>,
    #[serde(default)]
    pub version: Option<i64>,
}

#[derive(Debug)]
pub enum Command {
    Read(ReadCommand),
    Sql(SqlCommand),
}

impl Command {
    pub fn parse(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        let value: serde_json::Value = serde_json::from_slice(bytes)?;
        if value.get("sql").is_some() {
            Ok(Command::Sql(serde_json::from_value(value)?))
        } else {
            Ok(Command::Read(serde_json::from_value(value)?))
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateTableCommand {
    pub path: String,
    pub schema: Vec<ColumnDef>,
    #[serde(default)]
    pub storage_account: Option<String>,
    #[serde(default)]
    pub sas_token: Option<String>,
    #[serde(default)]
    pub configuration: Option<HashMap<String, String>>,
    #[serde(default)]
    pub partition_by: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct ExecuteDmlCommand {
    pub sql: String,
    pub table_path: String,
    pub table_name: String,
    #[serde(default)]
    pub storage_account: Option<String>,
    #[serde(default)]
    pub sas_token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpgradeProtocolCommand {
    pub path: String,
    pub reader_version: i32,
    pub writer_version: i32,
    #[serde(default)]
    pub reader_features: Option<Vec<String>>,
    #[serde(default)]
    pub writer_features: Option<Vec<String>>,
    #[serde(default)]
    pub storage_account: Option<String>,
    #[serde(default)]
    pub sas_token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WriteCommand {
    pub path: String,
    #[serde(default = "default_write_mode")]
    pub mode: String,
    #[serde(default = "default_operation")]
    pub operation: String,
    #[serde(default)]
    pub storage_account: Option<String>,
    #[serde(default)]
    pub sas_token: Option<String>,
    #[serde(default)]
    pub configuration: Option<HashMap<String, String>>,
    #[serde(default)]
    pub partition_by: Option<Vec<String>>,
    #[serde(default)]
    pub predicate: Option<String>,
    #[serde(default)]
    pub source_alias: Option<String>,
    #[serde(default)]
    pub target_alias: Option<String>,
    #[serde(default)]
    pub when_matched_update_all: Option<bool>,
    #[serde(default)]
    pub when_matched_update_set: Option<HashMap<String, String>>,
    #[serde(default)]
    pub when_matched_delete_predicate: Option<String>,
    #[serde(default)]
    pub when_not_matched_insert_all: Option<bool>,
    #[serde(default)]
    pub when_not_matched_insert_set: Option<HashMap<String, String>>,
    #[serde(default)]
    pub when_not_matched_by_source_delete_predicate: Option<String>,
    #[serde(default)]
    pub when_not_matched_by_source_update_set: Option<HashMap<String, String>>,
    #[serde(default)]
    pub when_not_matched_by_source_update_predicate: Option<String>,
}

fn default_write_mode() -> String {
    "overwrite".to_string()
}

fn default_operation() -> String {
    "write".to_string()
}

pub fn arrow_type_from_str(s: &str) -> DataType {
    match s.to_ascii_lowercase().as_str() {
        "string" | "str" | "utf8" => DataType::Utf8,
        "int64" | "long" | "bigint" => DataType::Int64,
        "int32" | "int" | "integer" => DataType::Int32,
        "int16" | "short" | "smallint" => DataType::Int16,
        "int8" | "byte" | "tinyint" => DataType::Int8,
        "float64" | "double" => DataType::Float64,
        "float32" | "float" => DataType::Float32,
        "bool" | "boolean" => DataType::Boolean,
        "date32" | "date" => DataType::Date32,
        "timestamp" => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
        }
        "timestamp_ntz" => DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        "binary" => DataType::Binary,
        _ => DataType::Utf8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrow_type_string_variants() {
        assert_eq!(arrow_type_from_str("string"), DataType::Utf8);
        assert_eq!(arrow_type_from_str("utf8"), DataType::Utf8);
    }

    #[test]
    fn arrow_type_integer_variants() {
        assert_eq!(arrow_type_from_str("int32"), DataType::Int32);
        assert_eq!(arrow_type_from_str("integer"), DataType::Int32);
        assert_eq!(arrow_type_from_str("int64"), DataType::Int64);
        assert_eq!(arrow_type_from_str("int16"), DataType::Int16);
        assert_eq!(arrow_type_from_str("int8"), DataType::Int8);
    }

    #[test]
    fn arrow_type_float_variants() {
        assert_eq!(arrow_type_from_str("float32"), DataType::Float32);
        assert_eq!(arrow_type_from_str("double"), DataType::Float64);
    }

    #[test]
    fn arrow_type_boolean_variants() {
        assert_eq!(arrow_type_from_str("boolean"), DataType::Boolean);
        assert_eq!(arrow_type_from_str("bool"), DataType::Boolean);
    }

    #[test]
    fn arrow_type_date_and_timestamp() {
        assert_eq!(arrow_type_from_str("date"), DataType::Date32);
        assert_eq!(arrow_type_from_str("binary"), DataType::Binary);
    }

    #[test]
    fn arrow_type_binary() {
        assert_eq!(arrow_type_from_str("binary"), DataType::Binary);
    }

    #[test]
    fn arrow_type_unknown_falls_back_to_utf8() {
        assert_eq!(arrow_type_from_str("something_new"), DataType::Utf8);
    }

    #[test]
    fn parse_invalid_json_returns_error() {
        assert!(Command::parse(br#"{"#).is_err());
    }

    #[test]
    fn parse_read_command_minimal() {
        let cmd = Command::parse(br#"{"path":"/tmp/t"}"#).unwrap();
        match cmd {
            Command::Read(read) => {
                assert_eq!(read.path, "/tmp/t");
                assert_eq!(read.num_rows, None);
            }
            _ => panic!("expected read command"),
        }
    }

    #[test]
    fn parse_read_command_with_num_rows() {
        let cmd = Command::parse(br#"{"path":"/tmp/t","num_rows":5}"#).unwrap();
        match cmd {
            Command::Read(read) => assert_eq!(read.num_rows, Some(5)),
            _ => panic!("expected read command"),
        }
    }

    #[test]
    fn parse_read_command_with_version() {
        let cmd = Command::parse(br#"{"path":"/tmp/t","version":3}"#).unwrap();
        match cmd {
            Command::Read(read) => assert_eq!(read.version, Some(3)),
            _ => panic!("expected read command"),
        }
    }

    #[test]
    fn parse_read_command_without_version_defaults_to_none() {
        let cmd = Command::parse(br#"{"path":"/tmp/t"}"#).unwrap();
        match cmd {
            Command::Read(read) => assert_eq!(read.version, None),
            _ => panic!("expected read command"),
        }
    }

    #[test]
    fn parse_read_missing_path_returns_error() {
        assert!(Command::parse(br#"{"num_rows":1}"#).is_err());
    }

    #[test]
    fn parse_sql_command() {
        let cmd =
            Command::parse(br#"{"sql":"SELECT * FROM t","table_path":"/tmp/t","table_name":"t"}"#)
                .unwrap();
        match cmd {
            Command::Sql(sql) => {
                assert_eq!(sql.sql, "SELECT * FROM t");
                assert_eq!(sql.table_path.as_deref(), Some("/tmp/t"));
                assert_eq!(sql.table_name.as_deref(), Some("t"));
            }
            _ => panic!("expected sql command"),
        }
    }

    #[test]
    fn parse_sql_command_without_table() {
        let cmd = Command::parse(br#"{"sql":"SELECT 1"}"#).unwrap();
        match cmd {
            Command::Sql(sql) => {
                assert_eq!(sql.sql, "SELECT 1");
                assert_eq!(sql.table_path, None);
                assert_eq!(sql.table_name, None);
            }
            _ => panic!("expected sql command"),
        }
    }

    #[test]
    fn parse_sql_command_with_version() {
        let cmd = Command::parse(
            br#"{"sql":"SELECT * FROM t","table_path":"/tmp/t","table_name":"t","version":7}"#,
        )
        .unwrap();
        match cmd {
            Command::Sql(sql) => assert_eq!(sql.version, Some(7)),
            _ => panic!("expected sql command"),
        }
    }

    #[test]
    fn parse_create_table_minimal() {
        let json = br#"{"path":"/tmp/t","schema":[{"name":"id","type":"int32"}]}"#;
        let cmd: CreateTableCommand = serde_json::from_slice(json).unwrap();
        assert_eq!(cmd.path, "/tmp/t");
        assert_eq!(cmd.schema.len(), 1);
        assert_eq!(cmd.schema[0].name, "id");
        assert_eq!(cmd.schema[0].data_type, "int32");
    }

    #[test]
    fn parse_create_table_with_options() {
        let json = br#"{"path":"/tmp/t","schema":[{"name":"id","type":"int32"}],"partition_by":["p"],"configuration":{"delta.enableChangeDataFeed":"true"}}"#;
        let cmd: CreateTableCommand = serde_json::from_slice(json).unwrap();
        assert_eq!(cmd.partition_by.unwrap(), vec!["p"]);
        assert_eq!(
            cmd.configuration.unwrap()["delta.enableChangeDataFeed"],
            "true"
        );
    }

    #[test]
    fn parse_create_table_missing_schema_returns_error() {
        assert!(serde_json::from_slice::<CreateTableCommand>(br#"{"path":"/tmp/t"}"#).is_err());
    }

    #[test]
    fn parse_execute_dml() {
        let json =
            br#"{"sql":"DELETE FROM t WHERE id > 1","table_path":"/tmp/t","table_name":"t"}"#;
        let cmd: ExecuteDmlCommand = serde_json::from_slice(json).unwrap();
        assert_eq!(cmd.sql, "DELETE FROM t WHERE id > 1");
        assert_eq!(cmd.table_path, "/tmp/t");
        assert_eq!(cmd.table_name, "t");
    }

    #[test]
    fn parse_execute_dml_with_storage() {
        let json = br#"{"sql":"DELETE FROM t","table_path":"abfss://x","table_name":"t","storage_account":"acct","sas_token":"?sig=abc"}"#;
        let cmd: ExecuteDmlCommand = serde_json::from_slice(json).unwrap();
        assert_eq!(cmd.storage_account.as_deref(), Some("acct"));
        assert_eq!(cmd.sas_token.as_deref(), Some("?sig=abc"));
    }

    #[test]
    fn parse_execute_dml_missing_sql_returns_error() {
        assert!(serde_json::from_slice::<ExecuteDmlCommand>(
            br#"{"table_path":"/tmp/t","table_name":"t"}"#
        )
        .is_err());
    }

    #[test]
    fn parse_execute_dml_missing_table_path_returns_error() {
        assert!(serde_json::from_slice::<ExecuteDmlCommand>(
            br#"{"sql":"DELETE FROM t","table_name":"t"}"#
        )
        .is_err());
    }

    #[test]
    fn parse_upgrade_protocol_minimal() {
        let json = br#"{"path":"/tmp/t","reader_version":2,"writer_version":5}"#;
        let cmd: UpgradeProtocolCommand = serde_json::from_slice(json).unwrap();
        assert_eq!(cmd.reader_version, 2);
        assert_eq!(cmd.writer_version, 5);
    }

    #[test]
    fn parse_upgrade_protocol_with_features() {
        let json = br#"{"path":"/tmp/t","reader_version":3,"writer_version":7,"reader_features":["columnMapping"],"writer_features":["changeDataFeed"]}"#;
        let cmd: UpgradeProtocolCommand = serde_json::from_slice(json).unwrap();
        assert_eq!(cmd.reader_features.unwrap(), vec!["columnMapping"]);
        assert_eq!(cmd.writer_features.unwrap(), vec!["changeDataFeed"]);
    }

    #[test]
    fn parse_do_put_write_defaults() {
        let json = br#"{"path":"/data/t"}"#;
        let cmd: WriteCommand = serde_json::from_slice(json).unwrap();
        assert_eq!(cmd.path, "/data/t");
        assert_eq!(cmd.mode, "overwrite");
        assert_eq!(cmd.operation, "write");
    }

    #[test]
    fn parse_do_put_write_append() {
        let json = br#"{"path":"/data/t","mode":"append"}"#;
        let cmd: WriteCommand = serde_json::from_slice(json).unwrap();
        assert_eq!(cmd.mode, "append");
        assert_eq!(cmd.operation, "write");
    }

    #[test]
    fn parse_do_put_write_with_partition_and_config() {
        let json = br#"{"path":"/data/t","mode":"overwrite","partition_by":["p1","p2"],"configuration":{"delta.enableChangeDataFeed":"true"}}"#;
        let cmd: WriteCommand = serde_json::from_slice(json).unwrap();
        assert_eq!(cmd.partition_by.unwrap(), vec!["p1", "p2"]);
        assert_eq!(
            cmd.configuration.unwrap()["delta.enableChangeDataFeed"],
            "true"
        );
    }

    #[test]
    fn parse_do_put_merge() {
        let json = br#"{"path":"/data/t","operation":"merge","predicate":"target.id = source.id","source_alias":"source","target_alias":"target","when_matched_update_all":true,"when_not_matched_insert_all":true}"#;
        let cmd: WriteCommand = serde_json::from_slice(json).unwrap();
        assert_eq!(cmd.operation, "merge");
        assert_eq!(cmd.predicate.as_deref(), Some("target.id = source.id"));
        assert_eq!(cmd.source_alias.as_deref(), Some("source"));
        assert_eq!(cmd.target_alias.as_deref(), Some("target"));
        assert_eq!(cmd.when_matched_update_all, Some(true));
        assert_eq!(cmd.when_not_matched_insert_all, Some(true));
    }

    #[test]
    fn parse_do_put_merge_with_update_set() {
        let json = br#"{"path":"/data/t","operation":"merge","predicate":"target.id = source.id","when_matched_update_set":{"col1":"source.col1","col2":"source.col2 + 1"}}"#;
        let cmd: WriteCommand = serde_json::from_slice(json).unwrap();
        let update_set = cmd.when_matched_update_set.as_ref().unwrap();
        assert_eq!(update_set["col1"], "source.col1");
        assert_eq!(update_set["col2"], "source.col2 + 1");
    }

    #[test]
    fn parse_do_put_merge_with_delete_and_not_matched_by_source() {
        let json = br#"{"path":"/data/t","operation":"merge","predicate":"target.id = source.id","when_matched_delete_predicate":"source.deleted = true","when_not_matched_by_source_delete_predicate":"true","when_not_matched_by_source_update_set":{"active":"'false'"},"when_not_matched_by_source_update_predicate":"target.active = true"}"#;
        let cmd: WriteCommand = serde_json::from_slice(json).unwrap();
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
}
