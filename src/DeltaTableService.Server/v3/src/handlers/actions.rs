// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! `ListActions` handler — returns the catalogue of supported DoAction types.

use arrow_flight::ActionType;

/// Returns the list of all supported actions for `ListActions`.
pub fn list_actions() -> Vec<ActionType> {
    vec![
        ActionType {
            r#type: "health".into(),
            description: "Health check — returns {\"success\": true} if the server is alive."
                .into(),
        },
        ActionType {
            r#type: "shutdown".into(),
            description: "Gracefully shuts down the server.".into(),
        },
        ActionType {
            r#type: "create_table".into(),
            description: "Creates an empty Delta table with the given schema and configuration."
                .into(),
        },
        ActionType {
            r#type: "execute_dml".into(),
            description: "Executes a DML statement (DELETE, UPDATE, MERGE) against a Delta table."
                .into(),
        },
        ActionType {
            r#type: "upgrade_protocol".into(),
            description: "Upgrades the Delta protocol version and enables table features.".into(),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_actions_contains_health() {
        let actions = list_actions();
        assert!(actions.iter().any(|a| a.r#type == "health"));
    }

    #[test]
    fn list_actions_contains_all_expected() {
        let actions = list_actions();
        let types: Vec<&str> = actions.iter().map(|a| a.r#type.as_str()).collect();
        assert!(types.contains(&"health"));
        assert!(types.contains(&"shutdown"));
        assert!(types.contains(&"create_table"));
        assert!(types.contains(&"execute_dml"));
        assert!(types.contains(&"upgrade_protocol"));
    }
}
