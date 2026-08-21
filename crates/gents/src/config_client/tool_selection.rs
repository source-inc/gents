use crate::graphql::escape_graphql_string;
use crate::ToolSelectionDocument;
use anyhow::Result;

use super::{mint_recreate_identity_timestamp, ConfigAccess};
use gents_protocol::graphql::{optional_bool_field, optional_string_field, string_list_field};

pub async fn write_tool_selection_document(
    access: &ConfigAccess,
    selection: &ToolSelectionDocument,
) -> Result<String> {
    write_tool_selection_document_with_clear_fields(access, selection, &[]).await
}

pub async fn write_tool_selection_document_with_clear_fields(
    access: &ConfigAccess,
    selection: &ToolSelectionDocument,
    clear_update_fields: &[&str],
) -> Result<String> {
    let add_fields = format!(
        "{},\n                    updated_at: \"{}\"",
        tool_selection_fields(selection, true),
        escape_graphql_string(&mint_recreate_identity_timestamp()),
    );
    let mut update_fields = tool_selection_fields(selection, false);
    if !clear_update_fields.is_empty() {
        if !update_fields.is_empty() {
            update_fields.push_str(",\n                    ");
        }
        update_fields.push_str(
            &clear_update_fields
                .iter()
                .map(|field| format!("{field}: null"))
                .collect::<Vec<_>>()
                .join(",\n                    "),
        );
    }
    let mutation = format!(
        r#"mutation {{
            upsert_ToolSelection(
                filter: {{ selection_id: {{ _eq: "{selection_id}" }} }},
                add: {{
                    {add_fields}
                }},
                update: {{
                    {update_fields}
                }}
            ) {{ _docID }}
        }}"#,
        selection_id = escape_graphql_string(&selection.selection_id),
        add_fields = add_fields,
        update_fields = update_fields,
    );
    let response = access
        .execute_mutation(&mutation, "upsert ToolSelection")
        .await?;
    gents_protocol::graphql::extract_mutation_doc_id(&response, "ToolSelection")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ensure_runtime_schemas, load_tool_selection};
    use anyhow::Result;
    use defra_node::{EmbeddedNode, StorageBackend};

    /// Round-trip test: write a `ToolSelectionDocument` with subagent
    /// enablement fields set, then read it back and assert every field persisted.
    ///
    /// This test will FAIL before the fix because `tool_selection_fields()` does
    /// not emit the subagent fields.
    #[tokio::test]
    async fn write_tool_selection_persists_subagent_enablement_fields() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let data_dir = tempdir.path().join("data");
        let node = EmbeddedNode::builder()
            .data_path(&data_dir)
            .with_storage_backend(StorageBackend::Lark)
            .build()
            .await?;
        ensure_runtime_schemas(&node).await?;

        // A valid `subagent_targets` entry is the JSON serialization of a named
        // SubagentTarget, not a bare behavior id.
        let target = crate::subagent_target_entry(
            "amy-research",
            "did:test:subagent-enablement",
            "amy-research",
            None,
        );
        let selection = ToolSelectionDocument {
            selection_id: "test-subagent-fields".to_string(),
            agent_did: "did:test:subagent-enablement".to_string(),
            subagent_spawn_enabled: Some(true),
            subagent_targets: Some(vec![target.clone()]),
            subagent_steering_enabled: Some(true),
            subagent_background_enabled: Some(true),
            subagent_default_await_mode: Some("background".to_string()),
            subagent_allow_cross_deployment: Some(true),
            cross_deployment_spawn_timeout_seconds: Some(90),
            ..Default::default()
        };

        let access = ConfigAccess::Local(std::sync::Arc::new(node));
        write_tool_selection_document(&access, &selection).await?;

        let node = match &access {
            ConfigAccess::Local(n) => n,
            ConfigAccess::Graphql(_) => unreachable!(),
        };

        let loaded = load_tool_selection(node, "test-subagent-fields")
            .await?
            .expect("ToolSelection should exist after write");

        assert_eq!(
            loaded.subagent_spawn_enabled,
            Some(true),
            "subagent_spawn_enabled must persist"
        );
        assert_eq!(
            loaded.subagent_targets,
            Some(vec![target.clone()]),
            "subagent_targets must persist"
        );
        assert_eq!(
            loaded.subagent_steering_enabled,
            Some(true),
            "subagent_steering_enabled must persist"
        );
        assert_eq!(
            loaded.subagent_background_enabled,
            Some(true),
            "subagent_background_enabled must persist"
        );
        assert_eq!(
            loaded.subagent_default_await_mode.as_deref(),
            Some("background"),
            "subagent_default_await_mode must persist"
        );
        assert_eq!(
            loaded.subagent_allow_cross_deployment,
            Some(true),
            "subagent_allow_cross_deployment must persist"
        );
        assert_eq!(
            loaded.cross_deployment_spawn_timeout_seconds,
            Some(90),
            "cross_deployment_spawn_timeout_seconds must persist"
        );

        Ok(())
    }

    /// Regression for the `config tools set` clobbering bug: an update that
    /// leaves the subagent enablement fields `None` (as the imperative command
    /// does — it exposes no flags for them) MUST NOT overwrite an existing
    /// apply-managed subagent config. The writer omits `None` fields from the
    /// `update` clause, so DefraDB preserves the stored values.
    #[tokio::test]
    async fn update_with_none_subagent_fields_preserves_existing_config() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let data_dir = tempdir.path().join("data");
        let node = EmbeddedNode::builder()
            .data_path(&data_dir)
            .with_storage_backend(StorageBackend::Lark)
            .build()
            .await?;
        ensure_runtime_schemas(&node).await?;
        let access = ConfigAccess::Local(std::sync::Arc::new(node));

        // A valid `subagent_targets` entry is the JSON serialization of a named
        // SubagentTarget, not a bare behavior id.
        let target =
            crate::subagent_target_entry("amy-research", "did:test:clobber", "amy-research", None);

        // Step 1: an apply-style write enables subagents.
        let applied = ToolSelectionDocument {
            selection_id: "test-clobber".to_string(),
            agent_did: "did:test:clobber".to_string(),
            display_name: Some("Original".to_string()),
            subagent_spawn_enabled: Some(true),
            subagent_targets: Some(vec![target.clone()]),
            subagent_background_enabled: Some(true),
            subagent_default_await_mode: Some("background".to_string()),
            subagent_allow_cross_deployment: Some(true),
            cross_deployment_spawn_timeout_seconds: Some(90),
            ..Default::default()
        };
        write_tool_selection_document(&access, &applied).await?;

        // Step 2: an imperative `tools set`-style update touches only its own
        // fields and leaves every subagent field `None`.
        let imperative = ToolSelectionDocument {
            selection_id: "test-clobber".to_string(),
            agent_did: "did:test:clobber".to_string(),
            display_name: Some("Updated".to_string()),
            subagent_targets: None,
            subagent_spawn_enabled: None,
            subagent_steering_enabled: None,
            subagent_background_enabled: None,
            subagent_default_await_mode: None,
            subagent_allow_cross_deployment: None,
            cross_deployment_spawn_timeout_seconds: None,
            ..Default::default()
        };
        write_tool_selection_document(&access, &imperative).await?;

        let node = match &access {
            ConfigAccess::Local(n) => n,
            ConfigAccess::Graphql(_) => unreachable!(),
        };
        let loaded = load_tool_selection(node, "test-clobber")
            .await?
            .expect("ToolSelection should exist after update");

        // The imperative field changed...
        assert_eq!(
            loaded.display_name.as_deref(),
            Some("Updated"),
            "imperative-owned field should update"
        );
        // ...but the apply-managed subagent config is preserved, not clobbered.
        assert_eq!(
            loaded.subagent_spawn_enabled,
            Some(true),
            "subagent_spawn_enabled must NOT be clobbered by a None update"
        );
        assert_eq!(
            loaded.subagent_targets,
            Some(vec![target.clone()]),
            "subagent_targets must NOT be clobbered by a None update"
        );
        assert_eq!(
            loaded.subagent_background_enabled,
            Some(true),
            "subagent_background_enabled must NOT be clobbered by a None update"
        );
        assert_eq!(
            loaded.subagent_default_await_mode.as_deref(),
            Some("background"),
            "subagent_default_await_mode must NOT be clobbered by a None update"
        );
        assert_eq!(
            loaded.subagent_allow_cross_deployment,
            Some(true),
            "subagent_allow_cross_deployment must NOT be clobbered by a None update"
        );
        assert_eq!(
            loaded.cross_deployment_spawn_timeout_seconds,
            Some(90),
            "cross_deployment_spawn_timeout_seconds must NOT be clobbered by a None update"
        );

        Ok(())
    }

    #[tokio::test]
    async fn update_with_clear_fields_nulls_nullable_config() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let data_dir = tempdir.path().join("data");
        let node = EmbeddedNode::builder()
            .data_path(&data_dir)
            .with_storage_backend(StorageBackend::Lark)
            .build()
            .await?;
        ensure_runtime_schemas(&node).await?;
        let access = ConfigAccess::Local(std::sync::Arc::new(node));

        let applied = ToolSelectionDocument {
            selection_id: "test-clear-fields".to_string(),
            agent_did: "did:test:clear-fields".to_string(),
            display_name: Some("Original".to_string()),
            file_tool_root: Some("/tmp/workspace".to_string()),
            command_execution_policy: Some("read_only".to_string()),
            command_network_mode: Some("disabled".to_string()),
            allowed_mcp_service_ids: Some(vec!["observability".to_string()]),
            backgroundable_tool_names: Some(vec!["bash_unrestricted".to_string()]),
            cli_tool_names: Some(vec!["rg".to_string()]),
            defra_query_collections: Some(vec!["AgentRequest".to_string()]),
            cross_deployment_spawn_timeout_seconds: Some(90),
            ..Default::default()
        };
        write_tool_selection_document(&access, &applied).await?;

        let clear_update = ToolSelectionDocument {
            selection_id: "test-clear-fields".to_string(),
            agent_did: "did:test:clear-fields".to_string(),
            allowed_mcp_service_ids: Some(Vec::new()),
            backgroundable_tool_names: Some(Vec::new()),
            cli_tool_names: Some(Vec::new()),
            defra_query_collections: Some(Vec::new()),
            ..Default::default()
        };
        write_tool_selection_document_with_clear_fields(
            &access,
            &clear_update,
            &[
                "display_name",
                "file_tool_root",
                "command_execution_policy",
                "command_network_mode",
                "cross_deployment_spawn_timeout_seconds",
            ],
        )
        .await?;

        let node = match &access {
            ConfigAccess::Local(n) => n,
            ConfigAccess::Graphql(_) => unreachable!(),
        };
        let loaded = load_tool_selection(node, "test-clear-fields")
            .await?
            .expect("ToolSelection should exist after update");

        assert_eq!(loaded.display_name, None);
        assert_eq!(loaded.file_tool_root, None);
        assert_eq!(loaded.command_execution_policy, None);
        assert_eq!(loaded.command_network_mode, None);
        assert!(loaded
            .allowed_mcp_service_ids
            .unwrap_or_default()
            .is_empty());
        assert!(loaded
            .backgroundable_tool_names
            .unwrap_or_default()
            .is_empty());
        assert!(loaded.cli_tool_names.unwrap_or_default().is_empty());
        assert!(loaded
            .defra_query_collections
            .unwrap_or_default()
            .is_empty());
        assert_eq!(loaded.cross_deployment_spawn_timeout_seconds, None);

        Ok(())
    }
}

fn tool_selection_fields(selection: &ToolSelectionDocument, include_id: bool) -> String {
    let mut fields = Vec::new();
    if include_id {
        fields.push(format!(
            r#"selection_id: "{}""#,
            escape_graphql_string(&selection.selection_id)
        ));
    }
    fields.push(format!(
        r#"agent_did: "{}""#,
        escape_graphql_string(&selection.agent_did)
    ));
    fields.extend(
        [
            optional_string_field("display_name", selection.display_name.as_deref()),
            // Emitted from the loaded/desired value only; the imperative builder
            // leaves it `None`, so `optional_string_field` omits it and the stored
            // version is preserved on update (the version is backfill-owned, never
            // set by an imperative flag).
            optional_string_field(
                "tool_policy_version",
                selection.tool_policy_version.as_deref(),
            ),
            optional_bool_field("enable_file_tools", selection.enable_file_tools),
            optional_string_field("file_tools_mode", selection.file_tools_mode.as_deref()),
            optional_string_field("file_tool_root", selection.file_tool_root.as_deref()),
            optional_bool_field("enable_bash", selection.enable_bash),
            optional_string_field("bash_mode", selection.bash_mode.as_deref()),
            optional_string_field(
                "command_execution_policy",
                selection.command_execution_policy.as_deref(),
            ),
            selection
                .command_allowed_argv_prefixes
                .as_ref()
                .and_then(|values| string_list_field("command_allowed_argv_prefixes", values)),
            selection
                .command_forbidden_argv_prefixes
                .as_ref()
                .and_then(|values| string_list_field("command_forbidden_argv_prefixes", values)),
            selection
                .read_only_command_allowlist
                .as_ref()
                .and_then(|values| string_list_field("read_only_command_allowlist", values)),
            optional_string_field(
                "command_network_mode",
                selection.command_network_mode.as_deref(),
            ),
            selection
                .cli_tool_names
                .as_ref()
                .and_then(|values| string_list_field("cli_tool_names", values)),
            optional_bool_field("enable_meta_tools", selection.enable_meta_tools),
            selection
                .allowed_mcp_service_ids
                .as_ref()
                .and_then(|values| string_list_field("allowed_mcp_service_ids", values)),
            selection
                .backgroundable_tool_names
                .as_ref()
                .and_then(|values| string_list_field("backgroundable_tool_names", values)),
            selection
                .approval_required_tools
                .as_ref()
                .and_then(|values| string_list_field("approval_required_tools", values)),
            optional_bool_field("enable_memory", selection.enable_memory),
            optional_bool_field(
                "enable_session_history_tool",
                selection.enable_session_history_tool,
            ),
            optional_bool_field("enable_context_budget", selection.enable_context_budget),
            optional_bool_field("enable_defra_query", selection.enable_defra_query),
            selection
                .defra_query_collections
                .as_ref()
                .and_then(|values| string_list_field("defra_query_collections", values)),
            selection
                .subagent_targets
                .as_ref()
                .and_then(|values| string_list_field("subagent_targets", values)),
            optional_bool_field("subagent_spawn_enabled", selection.subagent_spawn_enabled),
            optional_bool_field(
                "subagent_steering_enabled",
                selection.subagent_steering_enabled,
            ),
            optional_bool_field(
                "subagent_background_enabled",
                selection.subagent_background_enabled,
            ),
            optional_string_field(
                "subagent_default_await_mode",
                selection.subagent_default_await_mode.as_deref(),
            ),
            optional_bool_field(
                "subagent_allow_cross_deployment",
                selection.subagent_allow_cross_deployment,
            ),
            selection
                .cross_deployment_spawn_timeout_seconds
                .map(|value| format!("cross_deployment_spawn_timeout_seconds: {value}")),
            optional_bool_field("enable_self_config", selection.enable_self_config),
            selection
                .self_config_categories
                .as_ref()
                .and_then(|values| string_list_field("self_config_categories", values)),
            optional_bool_field("self_config_no_lockout", selection.self_config_no_lockout),
            optional_bool_field("self_config_dry_run", selection.self_config_dry_run),
            // NOTE: `write_tools` is deliberately NOT encoded here. The
            // imperative path always sets `write_tools: None` (it is
            // apply-managed only), so there is nothing to render.
        ]
        .into_iter()
        .flatten(),
    );
    fields.join(",\n                    ")
}
