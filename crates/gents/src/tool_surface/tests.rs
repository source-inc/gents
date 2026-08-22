use super::*;

fn temp_root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("{name}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&path).unwrap();
    path
}

#[test]
fn selection_file_tool_root_clamps_within_operator_root() {
    let operator_root = temp_root("gents-operator-root");
    let scoped_root = operator_root.join("bench").join("results");
    std::fs::create_dir_all(&scoped_root).unwrap();

    let config = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            file_tools: FileToolMode::ReadWrite,
            file_tool_root: Some(scoped_root.clone()),
            bash: BashMode::Unrestricted,
            command_policy: None,
            cli_tool_names: Vec::new(),
            enable_meta_tools: false,
            allowed_mcp_service_ids: Vec::new(),
            backgroundable_tool_names: Vec::new(),
            approval_required_tools: Vec::new(),
            enable_memory: false,
            enable_session_history_tool: false,
            enable_context_budget: true,
            enable_defra_query: false,
            defra_query_collections: Vec::new(),
            write_tools: Vec::new(),
            query_tools: Vec::new(),
            enable_self_config: false,
            self_config_categories: None,
            self_config_no_lockout: false,
            self_config_dry_run: false,
            enable_lsp: false,
            enable_graph_dsl: false,
            lsp_config: None,
        },
        &ToolCeiling::readwrite(operator_root.clone()),
        Vec::new(),
    )
    .unwrap();

    let canonical_scoped_root = std::fs::canonicalize(&scoped_root).unwrap();
    let native_tools = config.host_tools().native_tools();
    assert!(matches!(
        native_tools[0],
        crate::toolset::NativeTool::ListFiles { .. }
    ));
    assert!(matches!(
        native_tools[1],
        crate::toolset::NativeTool::ReadFile { .. }
    ));
    assert!(matches!(
        native_tools[2],
        crate::toolset::NativeTool::Glob { .. }
    ));
    assert!(matches!(
        native_tools[3],
        crate::toolset::NativeTool::Grep { .. }
    ));
    assert!(matches!(
        native_tools[4],
        crate::toolset::NativeTool::WriteFile { ref root } if root == &canonical_scoped_root
    ));
    assert!(matches!(
        native_tools[5],
        crate::toolset::NativeTool::EditFile { ref root } if root == &canonical_scoped_root
    ));
    assert!(matches!(
        native_tools[6],
        crate::toolset::NativeTool::BashUnrestricted { ref root, .. } if root == &canonical_scoped_root
    ));
}

#[test]
fn command_timeout_ceiling_reaches_selected_bash_tool() {
    let operator_root = temp_root("gents-command-timeout-root");
    let ceiling = ToolCeiling::readonly_at(&operator_root).with_command_timeout_secs(120);
    let config = BehaviorToolConfig::from_selection(
        "classifier",
        ToolSelection {
            file_tools: FileToolMode::Off,
            file_tool_root: None,
            bash: BashMode::ReadOnly,
            command_policy: None,
            cli_tool_names: Vec::new(),
            enable_meta_tools: false,
            allowed_mcp_service_ids: Vec::new(),
            backgroundable_tool_names: Vec::new(),
            approval_required_tools: Vec::new(),
            enable_memory: false,
            enable_session_history_tool: false,
            enable_context_budget: false,
            enable_defra_query: false,
            defra_query_collections: Vec::new(),
            write_tools: Vec::new(),
            query_tools: Vec::new(),
            enable_self_config: false,
            self_config_categories: None,
            self_config_no_lockout: false,
            self_config_dry_run: false,
            enable_lsp: false,
            enable_graph_dsl: false,
            lsp_config: None,
        },
        &ceiling,
        Vec::new(),
    )
    .unwrap();

    assert!(matches!(
        config.host_tools().native_tools(),
        [crate::toolset::NativeTool::BashReadOnly { timeout, .. }]
            if timeout.as_secs() == 120
    ));
}

#[test]
fn command_timeout_max_ceiling_reaches_selected_bash_tool() {
    let operator_root = temp_root("gents-command-timeout-max-root");
    let ceiling = ToolCeiling::readonly_at(&operator_root)
        .with_command_timeout_secs(600)
        .with_command_timeout_max_secs(3_600);
    let config = BehaviorToolConfig::from_selection(
        "classifier",
        ToolSelection {
            file_tools: FileToolMode::Off,
            file_tool_root: None,
            bash: BashMode::ReadOnly,
            command_policy: None,
            cli_tool_names: Vec::new(),
            enable_meta_tools: false,
            allowed_mcp_service_ids: Vec::new(),
            backgroundable_tool_names: Vec::new(),
            approval_required_tools: Vec::new(),
            enable_memory: false,
            enable_session_history_tool: false,
            enable_context_budget: false,
            enable_defra_query: false,
            defra_query_collections: Vec::new(),
            write_tools: Vec::new(),
            query_tools: Vec::new(),
            enable_self_config: false,
            self_config_categories: None,
            self_config_no_lockout: false,
            self_config_dry_run: false,
            enable_lsp: false,
            enable_graph_dsl: false,
            lsp_config: None,
        },
        &ceiling,
        Vec::new(),
    )
    .unwrap();

    assert!(matches!(
        config.host_tools().native_tools(),
        [crate::toolset::NativeTool::BashReadOnly { timeout, timeout_max, .. }]
            if timeout.as_secs() == 600 && timeout_max.as_secs() == 3_600
    ));
}

#[test]
fn selection_file_tool_root_rejects_escape_outside_operator_root() {
    let operator_root = temp_root("gents-operator-root");
    let outside_root = temp_root("gents-outside-root");

    let error = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            file_tools: FileToolMode::ReadOnly,
            file_tool_root: Some(outside_root),
            bash: BashMode::Off,
            command_policy: None,
            cli_tool_names: Vec::new(),
            enable_meta_tools: false,
            allowed_mcp_service_ids: Vec::new(),
            backgroundable_tool_names: Vec::new(),
            approval_required_tools: Vec::new(),
            enable_memory: false,
            enable_session_history_tool: false,
            enable_context_budget: true,
            enable_defra_query: false,
            defra_query_collections: Vec::new(),
            write_tools: Vec::new(),
            query_tools: Vec::new(),
            enable_self_config: false,
            self_config_categories: None,
            self_config_no_lockout: false,
            self_config_dry_run: false,
            enable_lsp: false,
            enable_graph_dsl: false,
            lsp_config: None,
        },
        &ToolCeiling::readwrite(operator_root),
        Vec::new(),
    )
    .expect_err("selection root outside operator ceiling should fail");

    assert!(
        error.to_string().contains("escapes operator tool root"),
        "{error:#}"
    );
}

#[test]
fn readonly_selection_file_tool_root_rejects_escape_outside_operator_root() {
    let operator_root = temp_root("gents-operator-root");
    let outside_root = temp_root("gents-outside-root");

    let error = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            file_tools: FileToolMode::ReadOnly,
            file_tool_root: Some(outside_root),
            bash: BashMode::ReadOnly,
            command_policy: None,
            cli_tool_names: Vec::new(),
            enable_meta_tools: false,
            allowed_mcp_service_ids: Vec::new(),
            backgroundable_tool_names: Vec::new(),
            approval_required_tools: Vec::new(),
            enable_memory: false,
            enable_session_history_tool: false,
            enable_context_budget: true,
            enable_defra_query: false,
            defra_query_collections: Vec::new(),
            write_tools: Vec::new(),
            query_tools: Vec::new(),
            enable_self_config: false,
            self_config_categories: None,
            self_config_no_lockout: false,
            self_config_dry_run: false,
            enable_lsp: false,
            enable_graph_dsl: false,
            lsp_config: None,
        },
        &ToolCeiling::readonly_at(operator_root),
        Vec::new(),
    )
    .expect_err("readonly selection root outside operator ceiling should fail");

    assert!(
        error.to_string().contains("escapes operator tool root"),
        "{error:#}"
    );
}

#[test]
fn downgraded_off_selection_ignores_stale_file_tool_root() {
    let stale_root =
        std::env::temp_dir().join(format!("gents-stale-root-{}", uuid::Uuid::new_v4()));

    let config = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            file_tools: FileToolMode::ReadOnly,
            file_tool_root: Some(stale_root),
            bash: BashMode::ReadOnly,
            command_policy: None,
            cli_tool_names: Vec::new(),
            enable_meta_tools: false,
            allowed_mcp_service_ids: Vec::new(),
            backgroundable_tool_names: Vec::new(),
            approval_required_tools: Vec::new(),
            enable_memory: false,
            enable_session_history_tool: false,
            enable_context_budget: true,
            enable_defra_query: false,
            defra_query_collections: Vec::new(),
            write_tools: Vec::new(),
            query_tools: Vec::new(),
            enable_self_config: false,
            self_config_categories: None,
            self_config_no_lockout: false,
            self_config_dry_run: false,
            enable_lsp: false,
            enable_graph_dsl: false,
            lsp_config: None,
        },
        &ToolCeiling::meta_only(),
        Vec::new(),
    )
    .unwrap();

    assert!(config.host_tools().native_tools().is_empty());
    assert!(config.host_tools().build_native_tools().unwrap().is_empty());
}

#[test]
fn readonly_ceiling_clamps_unrestricted_bash_policy() {
    let config = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            file_tools: FileToolMode::ReadWrite,
            file_tool_root: None,
            bash: BashMode::Unrestricted,
            command_policy: Some(
                crate::toolset::CommandExecutionPolicy::write_capable()
                    .with_mode(crate::toolset::CommandExecutionMode::Unrestricted),
            ),
            cli_tool_names: Vec::new(),
            enable_meta_tools: false,
            allowed_mcp_service_ids: Vec::new(),
            backgroundable_tool_names: Vec::new(),
            approval_required_tools: Vec::new(),
            enable_memory: false,
            enable_session_history_tool: false,
            enable_context_budget: true,
            enable_defra_query: false,
            defra_query_collections: Vec::new(),
            write_tools: Vec::new(),
            query_tools: Vec::new(),
            enable_self_config: false,
            self_config_categories: None,
            self_config_no_lockout: false,
            self_config_dry_run: false,
            enable_lsp: false,
            enable_graph_dsl: false,
            lsp_config: None,
        },
        &ToolCeiling::readonly(),
        Vec::new(),
    )
    .unwrap();

    assert_eq!(config.host_tools(), &crate::toolset::ToolSet::readonly());
}

#[test]
fn selection_without_root_inherits_operator_root() {
    let operator_root = temp_root("gents-operator-root");

    let config = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            file_tools: FileToolMode::ReadWrite,
            file_tool_root: None,
            bash: BashMode::Unrestricted,
            command_policy: None,
            cli_tool_names: Vec::new(),
            enable_meta_tools: false,
            allowed_mcp_service_ids: Vec::new(),
            backgroundable_tool_names: Vec::new(),
            approval_required_tools: Vec::new(),
            enable_memory: false,
            enable_session_history_tool: false,
            enable_context_budget: true,
            enable_defra_query: false,
            defra_query_collections: Vec::new(),
            write_tools: Vec::new(),
            query_tools: Vec::new(),
            enable_self_config: false,
            self_config_categories: None,
            self_config_no_lockout: false,
            self_config_dry_run: false,
            enable_lsp: false,
            enable_graph_dsl: false,
            lsp_config: None,
        },
        &ToolCeiling::readwrite(operator_root.clone()),
        Vec::new(),
    )
    .unwrap();

    let canonical_operator_root = std::fs::canonicalize(&operator_root).unwrap();
    let native_tools = config.host_tools().native_tools();
    assert!(matches!(
        native_tools[4],
        crate::toolset::NativeTool::WriteFile { ref root } if root == &canonical_operator_root
    ));
    assert!(matches!(
        native_tools[5],
        crate::toolset::NativeTool::EditFile { ref root } if root == &canonical_operator_root
    ));
    assert!(matches!(
        native_tools[6],
        crate::toolset::NativeTool::BashUnrestricted { ref root, .. } if root == &canonical_operator_root
    ));
}

#[test]
fn selection_cli_tools_require_ceiling_entries() {
    let operator_root = temp_root("gents-operator-root");

    let config = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            file_tools: FileToolMode::Off,
            file_tool_root: None,
            bash: BashMode::Off,
            command_policy: None,
            cli_tool_names: vec!["rg".to_string()],
            enable_meta_tools: false,
            allowed_mcp_service_ids: Vec::new(),
            backgroundable_tool_names: Vec::new(),
            approval_required_tools: Vec::new(),
            enable_memory: false,
            enable_session_history_tool: false,
            enable_context_budget: true,
            enable_defra_query: false,
            defra_query_collections: Vec::new(),
            write_tools: Vec::new(),
            query_tools: Vec::new(),
            enable_self_config: false,
            self_config_categories: None,
            self_config_no_lockout: false,
            self_config_dry_run: false,
            enable_lsp: false,
            enable_graph_dsl: false,
            lsp_config: None,
        },
        &ToolCeiling::readwrite(operator_root),
        Vec::new(),
    )
    .unwrap();

    assert!(!config.host_tools().tool_names().contains(&"rg".to_string()));
    assert!(config
        .host_tools()
        .native_tools()
        .iter()
        .all(|tool| !matches!(tool, crate::toolset::NativeTool::Cli(_))));
}

#[test]
fn selection_cli_tools_expose_only_ceiling_entries() {
    let operator_root = temp_root("gents-operator-root");
    let ceiling = ToolCeiling::readwrite(operator_root).with_cli_tool(cli_tool(
        "rg",
        "/usr/bin/rg",
        "Search files with ripgrep",
    ));

    let config = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            file_tools: FileToolMode::Off,
            file_tool_root: None,
            bash: BashMode::Off,
            command_policy: None,
            cli_tool_names: vec!["rg".to_string(), "cargo".to_string()],
            enable_meta_tools: false,
            allowed_mcp_service_ids: Vec::new(),
            backgroundable_tool_names: Vec::new(),
            approval_required_tools: Vec::new(),
            enable_memory: false,
            enable_session_history_tool: false,
            enable_context_budget: true,
            enable_defra_query: false,
            defra_query_collections: Vec::new(),
            write_tools: Vec::new(),
            query_tools: Vec::new(),
            enable_self_config: false,
            self_config_categories: None,
            self_config_no_lockout: false,
            self_config_dry_run: false,
            enable_lsp: false,
            enable_graph_dsl: false,
            lsp_config: None,
        },
        &ceiling,
        Vec::new(),
    )
    .unwrap();

    let tool_names = config.host_tools().tool_names();
    assert!(tool_names.contains(&"rg".to_string()));
    assert!(!tool_names.contains(&"cargo".to_string()));
    assert_eq!(
        config
            .host_tools()
            .native_tools()
            .iter()
            .filter(|tool| matches!(tool, crate::toolset::NativeTool::Cli(_)))
            .count(),
        1
    );
}

#[test]
fn selection_mcp_service_allowlist_is_deduped() {
    let config = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            file_tools: FileToolMode::Off,
            file_tool_root: None,
            bash: BashMode::Off,
            command_policy: None,
            cli_tool_names: Vec::new(),
            enable_meta_tools: true,
            allowed_mcp_service_ids: vec![
                "x-data".to_string(),
                "x-data".to_string(),
                "observability-mcp".to_string(),
            ],
            backgroundable_tool_names: Vec::new(),
            approval_required_tools: Vec::new(),
            enable_memory: false,
            enable_session_history_tool: false,
            enable_context_budget: true,
            enable_defra_query: false,
            defra_query_collections: Vec::new(),
            write_tools: Vec::new(),
            query_tools: Vec::new(),
            enable_self_config: false,
            self_config_categories: None,
            self_config_no_lockout: false,
            self_config_dry_run: false,
            enable_lsp: false,
            enable_graph_dsl: false,
            lsp_config: None,
        },
        &ToolCeiling::meta_only(),
        Vec::new(),
    )
    .unwrap();

    assert_eq!(
        config.allowed_mcp_service_ids(),
        &["x-data".to_string(), "observability-mcp".to_string()]
    );
}

#[test]
fn background_tool_allowlist_registers_r6_tools() {
    let config = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            file_tools: FileToolMode::ReadOnly,
            file_tool_root: None,
            bash: BashMode::ReadOnly,
            command_policy: None,
            cli_tool_names: Vec::new(),
            enable_meta_tools: false,
            allowed_mcp_service_ids: Vec::new(),
            backgroundable_tool_names: vec!["bash".to_string(), "bash".to_string()],
            approval_required_tools: Vec::new(),
            enable_memory: false,
            enable_session_history_tool: false,
            enable_context_budget: true,
            enable_defra_query: false,
            defra_query_collections: Vec::new(),
            write_tools: Vec::new(),
            query_tools: Vec::new(),
            enable_self_config: false,
            self_config_categories: None,
            self_config_no_lockout: false,
            self_config_dry_run: false,
            enable_lsp: false,
            enable_graph_dsl: false,
            lsp_config: None,
        },
        &ToolCeiling::readonly(),
        Vec::new(),
    )
    .unwrap();

    assert_eq!(
        config.background_tools().allowlist,
        vec!["bash".to_string()]
    );
}

#[test]
fn background_tool_allowlist_rejects_non_backgroundable_tools() {
    let error = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            file_tools: FileToolMode::ReadOnly,
            file_tool_root: None,
            bash: BashMode::ReadOnly,
            command_policy: None,
            cli_tool_names: Vec::new(),
            enable_meta_tools: false,
            allowed_mcp_service_ids: Vec::new(),
            backgroundable_tool_names: vec!["read_file".to_string()],
            approval_required_tools: Vec::new(),
            enable_memory: false,
            enable_session_history_tool: false,
            enable_context_budget: true,
            enable_defra_query: false,
            defra_query_collections: Vec::new(),
            write_tools: Vec::new(),
            query_tools: Vec::new(),
            enable_self_config: false,
            self_config_categories: None,
            self_config_no_lockout: false,
            self_config_dry_run: false,
            enable_lsp: false,
            enable_graph_dsl: false,
            lsp_config: None,
        },
        &ToolCeiling::readonly(),
        Vec::new(),
    )
    .expect_err("read_file is intentionally not backgroundable");

    assert!(
        error
            .to_string()
            .contains("not a registered backgroundable tool"),
        "{error:#}"
    );
}

#[cfg(unix)]
#[test]
fn selection_file_tool_root_rejects_symlink_escape_for_missing_child() {
    let operator_root = temp_root("gents-operator-root");
    let outside_root = temp_root("gents-outside-root");
    let symlink_path = operator_root.join("link-out");
    std::os::unix::fs::symlink(&outside_root, &symlink_path).unwrap();

    let error = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            file_tools: FileToolMode::ReadWrite,
            file_tool_root: Some(symlink_path.join("workspace")),
            bash: BashMode::Unrestricted,
            command_policy: None,
            cli_tool_names: Vec::new(),
            enable_meta_tools: false,
            allowed_mcp_service_ids: Vec::new(),
            backgroundable_tool_names: Vec::new(),
            approval_required_tools: Vec::new(),
            enable_memory: false,
            enable_session_history_tool: false,
            enable_context_budget: true,
            enable_defra_query: false,
            defra_query_collections: Vec::new(),
            write_tools: Vec::new(),
            query_tools: Vec::new(),
            enable_self_config: false,
            self_config_categories: None,
            self_config_no_lockout: false,
            self_config_dry_run: false,
            enable_lsp: false,
            enable_graph_dsl: false,
            lsp_config: None,
        },
        &ToolCeiling::readwrite(operator_root),
        Vec::new(),
    )
    .expect_err("selection root through symlink should fail");

    assert!(
        error.to_string().contains("escapes operator tool root"),
        "{error:#}"
    );
}

#[tokio::test]
async fn defra_query_tool_gated_by_selection() {
    let node = defra_node::EmbeddedNode::builder().build().await.unwrap();
    crate::ensure_runtime_schemas(&node).await.unwrap();

    let enabled = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            enable_defra_query: true,
            ..Default::default()
        },
        &ToolCeiling::meta_only(),
        Vec::new(),
    )
    .unwrap()
    .resolve(&node)
    .await
    .unwrap();
    assert!(enabled.tool_names().contains(&"defra_query".to_string()));

    let disabled = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            enable_defra_query: false,
            ..Default::default()
        },
        &ToolCeiling::meta_only(),
        Vec::new(),
    )
    .unwrap()
    .resolve(&node)
    .await
    .unwrap();
    assert!(!disabled.tool_names().contains(&"defra_query".to_string()));
}

#[tokio::test]
async fn context_budget_tool_gated_by_selection() {
    let node = defra_node::EmbeddedNode::builder().build().await.unwrap();
    crate::ensure_runtime_schemas(&node).await.unwrap();

    let enabled = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            enable_context_budget: true,
            ..Default::default()
        },
        &ToolCeiling::meta_only(),
        Vec::new(),
    )
    .unwrap()
    .resolve(&node)
    .await
    .unwrap();
    assert!(enabled.tool_names().contains(&"context_budget".to_string()));

    let disabled = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            enable_context_budget: false,
            ..Default::default()
        },
        &ToolCeiling::meta_only(),
        Vec::new(),
    )
    .unwrap()
    .resolve(&node)
    .await
    .unwrap();
    assert!(!disabled
        .tool_names()
        .contains(&"context_budget".to_string()));

    let runtime = ToolRuntimeContext::oneshot(std::sync::Arc::new(node));
    let built = disabled.build_tools(&runtime).unwrap();
    assert!(!built.iter().any(|tool| tool.name() == "context_budget"));
}

#[tokio::test]
async fn write_tools_register_under_declared_names() {
    use crate::document_config::{WriteToolDecl, WriteToolField};

    let node = defra_node::EmbeddedNode::builder().build().await.unwrap();
    crate::ensure_runtime_schemas(&node).await.unwrap();

    let surface = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            enable_defra_query: false,
            write_tools: vec![WriteToolDecl {
                tool_name: "request_action".to_string(),
                collection: "ActionRequest".to_string(),
                description: "Request an action".to_string(),
                fields: vec![WriteToolField {
                    name: "summary".to_string(),
                    required: true,
                    fill: None,
                }],
                output_obligation: None,
            }],
            ..Default::default()
        },
        &ToolCeiling::meta_only(),
        Vec::new(),
    )
    .unwrap()
    .resolve(&node)
    .await
    .unwrap();

    let names = surface.tool_names();
    assert!(
        names.contains(&"request_action".to_string()),
        "declared write tool should be advertised under its tool_name; got {names:?}"
    );

    // The built dynamic tools must carry the per-decl name too.
    let runtime = ToolRuntimeContext::oneshot(std::sync::Arc::new(node));
    let built = surface.build_tools(&runtime).unwrap();
    assert!(
        built.iter().any(|tool| tool.name() == "request_action"),
        "registered dynamic tool should advertise decl.tool_name"
    );
}

#[tokio::test]
async fn query_tool_is_advertised_and_registered() {
    use crate::document_config::{QueryToolDecl, WriteToolField, WriteToolFieldFill};

    let node = defra_node::EmbeddedNode::builder().build().await.unwrap();
    crate::ensure_runtime_schemas(&node).await.unwrap();

    let surface = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            enable_defra_query: false,
            query_tools: vec![QueryToolDecl {
                tool_name: "query_candidate_finding".to_string(),
                collection: "CandidateFinding".to_string(),
                description: "Load candidates".to_string(),
                fields: vec!["finding_id".to_string()],
                filter_fields: vec![WriteToolField {
                    name: "run_id".to_string(),
                    required: false,
                    fill: Some(WriteToolFieldFill::Correlation),
                }],
            }],
            ..Default::default()
        },
        &ToolCeiling::meta_only(),
        Vec::new(),
    )
    .unwrap()
    .resolve(&node)
    .await
    .unwrap();

    let names = surface.tool_names();
    assert!(
        names.contains(&"query_candidate_finding".to_string()),
        "declared query tool should be advertised; got {names:?}"
    );
    assert!(
        !names.contains(&"defra_query".to_string()),
        "bound query tools must not imply the generic defra_query console"
    );

    let runtime = ToolRuntimeContext::oneshot(std::sync::Arc::new(node));
    let built = surface.build_tools(&runtime).unwrap();
    assert!(built
        .iter()
        .any(|tool| tool.name() == "query_candidate_finding"));
}

#[test]
fn malformed_write_tool_is_rejected_during_configuration() {
    use crate::document_config::WriteToolDecl;

    let error = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            write_tools: vec![WriteToolDecl {
                tool_name: "broken_tool".to_string(),
                collection: "  ".to_string(),
                description: String::new(),
                fields: Vec::new(),
                output_obligation: None,
            }],
            ..Default::default()
        },
        &ToolCeiling::meta_only(),
        Vec::new(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("malformed"));
}

#[test]
fn write_tool_colliding_with_builtin_is_rejected_during_configuration() {
    use crate::document_config::{WriteToolDecl, WriteToolField};

    let error = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            enable_defra_query: false,
            write_tools: vec![WriteToolDecl {
                tool_name: "context_budget".to_string(),
                collection: "ActionRequest".to_string(),
                description: String::new(),
                fields: vec![WriteToolField {
                    name: "summary".to_string(),
                    required: false,
                    fill: None,
                }],
                output_obligation: None,
            }],
            ..Default::default()
        },
        &ToolCeiling::meta_only(),
        Vec::new(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("collides with a built-in"));
}

#[test]
fn memory_tool_defaults_disabled() {
    assert!(!ToolSelection::default().enable_memory);
    assert!(!ToolSelection::default().enable_session_history_tool);
}

#[tokio::test]
async fn disabling_meta_tools_removes_goal_tools_from_names_and_runtime() {
    let node = defra_node::EmbeddedNode::builder().build().await.unwrap();
    crate::ensure_runtime_schemas(&node).await.unwrap();
    let surface = BehaviorToolConfig::from_selection(
        "benchmark",
        ToolSelection {
            enable_meta_tools: false,
            enable_context_budget: false,
            ..Default::default()
        },
        &ToolCeiling::meta_only(),
        Vec::new(),
    )
    .unwrap()
    .resolve(&node)
    .await
    .unwrap();

    let names = surface.tool_names();
    assert!(!names.contains(&crate::goal::GET_GOAL_TOOL_NAME.to_string()));
    assert!(!names.contains(&crate::goal::UPDATE_GOAL_TOOL_NAME.to_string()));

    let runtime = ToolRuntimeContext::oneshot(std::sync::Arc::new(node));
    let built_names = surface
        .build_tools(&runtime)
        .unwrap()
        .into_iter()
        .map(|tool| tool.name())
        .collect::<Vec<_>>();
    assert!(!built_names.contains(&crate::goal::GET_GOAL_TOOL_NAME.to_string()));
    assert!(!built_names.contains(&crate::goal::UPDATE_GOAL_TOOL_NAME.to_string()));
}

#[test]
fn defra_query_deny_all_collection_scope_gates_tool_off() {
    use super::policy::{EndpointScope, ToolPolicySurface};

    // Behavior permits collection `a`; ceiling permits only `b`. The disjoint
    // `Only ∩ Only` meet yields `Only(∅)` — a deny-all the projection must not
    // collapse into allow-all (the `Only(∅) ≠ All` trap).
    let mut behavior = ToolPolicySurface::runtime_all();
    behavior.defra_collections = EndpointScope::<String, ()>::only_units(["a".to_string()]);
    let mut ceiling = ToolPolicySurface::runtime_all();
    ceiling.defra_collections = EndpointScope::<String, ()>::only_units(["b".to_string()]);

    let effective = behavior.meet(&ceiling);
    assert!(
        effective.defra_collections.is_deny_all(),
        "disjoint collection scopes must meet to Only(empty) = deny-all"
    );
    // The capability bit alone is still on; only the keyed scope is empty.
    assert!(effective.defra_query);
    assert!(
        !effective.include_defra_query(),
        "deny-all collection scope must gate the defra_query tool off, not surface it as allow-all"
    );
    assert!(
        effective.defra_query_collections_for_runtime().is_empty(),
        "deny-all and all both project to an empty list; the gate is what distinguishes them"
    );

    // Sanity: an All ceiling leaves the behavior's own allowlist intact and the
    // tool surfaced.
    let mut all_ceiling = ToolPolicySurface::runtime_all();
    all_ceiling.defra_collections = EndpointScope::all();
    let permissive = behavior.meet(&all_ceiling);
    assert!(permissive.include_defra_query());
    assert_eq!(
        permissive.defra_query_collections_for_runtime(),
        vec!["a".to_string()]
    );
}

#[test]
fn query_decls_for_runtime_drops_tool_when_filled_filter_would_be_stripped() {
    use std::collections::{BTreeMap, BTreeSet};

    use super::policy::{EndpointScope, ToolPolicySurface};
    use crate::document_config::{QueryToolDecl, WriteToolField, WriteToolFieldFill};

    let decl = QueryToolDecl {
        tool_name: "query_candidate_finding".to_string(),
        collection: "CandidateFinding".to_string(),
        description: "Load candidates".to_string(),
        fields: vec!["finding_id".to_string(), "title".to_string()],
        filter_fields: vec![WriteToolField {
            name: "run_id".to_string(),
            required: false,
            fill: Some(WriteToolFieldFill::Correlation),
        }],
    };
    let mut policy = ToolPolicySurface::runtime_all();
    let mut grants = BTreeMap::new();
    grants.insert(
        (
            "query_candidate_finding".to_string(),
            "CandidateFinding".to_string(),
        ),
        BTreeSet::from(["finding_id".to_string(), "title".to_string()]),
    );
    policy.query_tools = EndpointScope::Only(grants);

    assert!(
        policy.query_decls_for_runtime(&[decl.clone()]).is_empty(),
        "narrowing away a correlation filter must drop the tool, not un-scope the read"
    );

    let mut lossless = ToolPolicySurface::runtime_all();
    let mut full = BTreeMap::new();
    full.insert(
        (
            "query_candidate_finding".to_string(),
            "CandidateFinding".to_string(),
        ),
        BTreeSet::from([
            "finding_id".to_string(),
            "title".to_string(),
            "run_id".to_string(),
        ]),
    );
    lossless.query_tools = EndpointScope::Only(full);
    let kept = lossless.query_decls_for_runtime(&[decl.clone()]);
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].filter_fields.len(), 1);

    let mut write_all = ToolPolicySurface::runtime_all();
    write_all.write_tools = EndpointScope::all();
    write_all.query_tools = EndpointScope::none();
    assert!(
        write_all.query_decls_for_runtime(&[decl]).is_empty(),
        "write_tools: All must not grant bound read tools"
    );
}

#[test]
fn from_document_read_only_allowlist_override_and_fallback() {
    // (a) A doc that sets ONLY read_only_command_allowlist (no other command_*
    // field) with enable_bash + ReadOnly must materialize a Some(command_policy)
    // whose read_only_allowlist is the custom list — proving the has_policy gate
    // fires and the override applies.
    let mut doc = init_like_tool_selection_document(
        "readonly-custom",
        false,
        "Off",
        true,
        "ReadOnly",
        false,
        Vec::new(),
        false,
    );
    doc.read_only_command_allowlist = Some(vec!["cat".to_string()]);
    let selection = ToolSelection::from_document(&doc).unwrap();
    let policy = selection
        .command_policy
        .expect("custom read-only allowlist must produce Some(command_policy)");
    assert_eq!(policy.read_only_allowlist(), ["cat".to_string()]);

    // (b) The same doc with an absent / empty allowlist yields the historical
    // default: command_policy resolves to None (builder applies the hardcoded
    // default_read_only_command_policy).
    let mut absent = doc.clone();
    absent.read_only_command_allowlist = None;
    assert!(ToolSelection::from_document(&absent)
        .unwrap()
        .command_policy
        .is_none());

    let mut empty = doc;
    empty.read_only_command_allowlist = Some(Vec::new());
    assert!(ToolSelection::from_document(&empty)
        .unwrap()
        .command_policy
        .is_none());
}

#[test]
fn tool_policy_version_controls_nullable_default_decode() {
    let legacy_doc = crate::document_config::ToolSelectionDocument {
        selection_id: "legacy-tools".to_string(),
        agent_did: "did:test:test".to_string(),
        ..Default::default()
    };
    let legacy = ToolSelection::from_document(&legacy_doc).unwrap();
    assert!(legacy.enable_meta_tools);
    // defra_query is opt-in for every policy version (#592): legacy docs are
    // NOT grandfathered; only the backfill's materialized `true` (below)
    // carries the historical permissive surface forward.
    assert!(!legacy.enable_defra_query);

    let backfilled = legacy_doc.with_legacy_policy_defaults_backfilled();
    assert_eq!(
        backfilled.tool_policy_version,
        Some(TOOL_POLICY_V1.to_string())
    );
    assert_eq!(backfilled.enable_meta_tools, Some(true));
    assert_eq!(backfilled.enable_defra_query, Some(true));
    let decoded_backfill = ToolSelection::from_document(&backfilled).unwrap();
    assert!(decoded_backfill.enable_meta_tools);
    assert!(decoded_backfill.enable_defra_query);

    let versioned_doc = crate::document_config::ToolSelectionDocument {
        tool_policy_version: Some(TOOL_POLICY_V1.to_string()),
        ..legacy_doc
    };
    let versioned = ToolSelection::from_document(&versioned_doc).unwrap();
    assert!(!versioned.enable_meta_tools);
    assert!(!versioned.enable_defra_query);
}

#[tokio::test]
async fn session_history_tool_requires_selection_opt_in() {
    let node = defra_node::EmbeddedNode::builder().build().await.unwrap();
    crate::ensure_runtime_schemas(&node).await.unwrap();

    let disabled = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            enable_session_history_tool: false,
            enable_context_budget: true,
            ..Default::default()
        },
        &ToolCeiling::meta_only(),
        Vec::new(),
    )
    .unwrap()
    .resolve(&node)
    .await
    .unwrap();
    assert!(!disabled
        .tool_names()
        .contains(&crate::toolset::SESSION_HISTORY_TOOL_NAME.to_string()));

    let enabled = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            enable_session_history_tool: true,
            ..Default::default()
        },
        &ToolCeiling::meta_only(),
        Vec::new(),
    )
    .unwrap()
    .resolve(&node)
    .await
    .unwrap();
    assert!(enabled
        .tool_names()
        .contains(&crate::toolset::SESSION_HISTORY_TOOL_NAME.to_string()));
}

fn init_like_tool_selection_document(
    package_name: &str,
    enable_file_tools: bool,
    file_tools_mode: &str,
    enable_bash: bool,
    bash_mode: &str,
    enable_meta_tools: bool,
    backgroundable_tool_names: Vec<String>,
    enable_defra_query: bool,
) -> crate::document_config::ToolSelectionDocument {
    crate::document_config::ToolSelectionDocument {
        selection_id: format!("{package_name}-tools"),
        agent_did: "did:test:test".to_string(),
        display_name: Some(package_name.to_string()),
        tool_policy_version: None,
        enable_file_tools: Some(enable_file_tools),
        file_tools_mode: Some(file_tools_mode.to_string()),
        file_tool_root: None,
        enable_bash: Some(enable_bash),
        bash_mode: Some(bash_mode.to_string()),
        command_execution_policy: matches!(bash_mode, "Unrestricted")
            .then(|| "unrestricted".to_string()),
        command_allowed_argv_prefixes: Some(Vec::new()),
        command_forbidden_argv_prefixes: Some(Vec::new()),
        read_only_command_allowlist: None,
        command_network_mode: None,
        cli_tool_names: Some(Vec::new()),
        enable_meta_tools: Some(enable_meta_tools),
        allowed_mcp_service_ids: Some(Vec::new()),
        backgroundable_tool_names: Some(backgroundable_tool_names),
        approval_required_tools: None,
        subagent_targets: Some(Vec::new()),
        subagent_spawn_enabled: Some(false),
        subagent_steering_enabled: Some(false),
        subagent_background_enabled: Some(false),
        subagent_default_await_mode: None,
        subagent_allow_cross_deployment: Some(false),
        cross_deployment_spawn_timeout_seconds: None,
        enable_memory: Some(false),
        enable_session_history_tool: Some(false),
        enable_context_budget: Some(true),
        enable_defra_query: Some(enable_defra_query),
        defra_query_collections: Some(Vec::new()),
        write_tools: None,
        datastore_tool_surface_ids: None,
        enable_self_config: None,
        self_config_categories: None,
        self_config_no_lockout: None,
        self_config_dry_run: None,
        enable_lsp: None,
        lsp_config: None,
        enable_graph_dsl: None,
    }
}

fn explanation_has_warning(explanation: &ToolSurfaceExplanation, code: &str) -> bool {
    explanation
        .warnings
        .iter()
        .any(|warning| warning.code == code)
}

fn explanation_category_contains(
    map: &std::collections::BTreeMap<String, Vec<String>>,
    category: &str,
    name: &str,
) -> bool {
    map.get(category)
        .is_some_and(|names| names.contains(&name.to_string()))
}

#[test]
fn explain_init_package_document_matrix_resolves_expected_surfaces() {
    struct Case {
        name: &'static str,
        selection: crate::document_config::ToolSelectionDocument,
        ceiling: ToolCeiling,
        mcp_services_online: bool,
        expected_tool_names: Vec<&'static str>,
        absent_tool_names: Vec<&'static str>,
        expected_warnings: Vec<&'static str>,
        host_ceiling_warning: bool,
    }

    let readonly_root = temp_root("gents-readonly-package-root");
    let write_root = temp_root("gents-write-package-root");
    let cases = vec![
        Case {
            name: "minimal",
            selection: init_like_tool_selection_document(
                "minimal",
                false,
                "Off",
                false,
                "Off",
                false,
                Vec::new(),
                false,
            ),
            ceiling: ToolCeiling::meta_only(),
            mcp_services_online: false,
            expected_tool_names: vec!["context_budget"],
            absent_tool_names: vec!["call_tool", "defra_query", "read_file", "spawn_process"],
            expected_warnings: vec!["host_ceiling_not_global"],
            host_ceiling_warning: true,
        },
        Case {
            name: "introspection-offline",
            selection: init_like_tool_selection_document(
                "introspection",
                false,
                "Off",
                false,
                "Off",
                true,
                Vec::new(),
                true,
            ),
            ceiling: ToolCeiling::meta_only(),
            mcp_services_online: false,
            expected_tool_names: vec!["context_budget", "defra_query"],
            absent_tool_names: vec!["call_tool", "read_file", "spawn_process"],
            expected_warnings: vec![
                "host_ceiling_not_global",
                "meta_requested_no_online_mcp",
                "defra_query_empty_scope_all",
            ],
            host_ceiling_warning: true,
        },
        Case {
            name: "introspection-online",
            selection: init_like_tool_selection_document(
                "introspection",
                false,
                "Off",
                false,
                "Off",
                true,
                Vec::new(),
                true,
            ),
            ceiling: ToolCeiling::meta_only(),
            mcp_services_online: true,
            expected_tool_names: vec![
                "discover_tools",
                "call_tool",
                "context_budget",
                "defra_query",
            ],
            absent_tool_names: vec!["read_file", "spawn_process", "spawn_subagent"],
            expected_warnings: vec![
                "host_ceiling_not_global",
                "mcp_empty_allowlist_all",
                "defra_query_empty_scope_all",
            ],
            host_ceiling_warning: true,
        },
        // readonly/write mirror the init packages, which no longer enable
        // defra_query (#592: opt-in only; introspection is the package that
        // turns it on).
        Case {
            name: "readonly-online",
            selection: init_like_tool_selection_document(
                "readonly",
                true,
                "ReadOnly",
                true,
                "ReadOnly",
                true,
                Vec::new(),
                false,
            ),
            ceiling: ToolCeiling::readonly_at(readonly_root),
            mcp_services_online: true,
            expected_tool_names: vec![
                "list_files",
                "read_file",
                "glob",
                "grep",
                "bash",
                "discover_tools",
                "call_tool",
                "context_budget",
            ],
            absent_tool_names: vec![
                "write_file",
                "bash_unrestricted",
                "spawn_process",
                "defra_query",
            ],
            expected_warnings: vec!["mcp_empty_allowlist_all"],
            host_ceiling_warning: false,
        },
        Case {
            name: "write-online",
            selection: init_like_tool_selection_document(
                "write",
                true,
                "ReadWrite",
                true,
                "Unrestricted",
                true,
                vec!["bash_unrestricted".to_string()],
                false,
            ),
            ceiling: ToolCeiling::readwrite(write_root),
            mcp_services_online: true,
            expected_tool_names: vec![
                "list_files",
                "read_file",
                "glob",
                "grep",
                "write_file",
                "edit_file",
                "bash_unrestricted",
                "discover_tools",
                "call_tool",
                "spawn_process",
                "wait_process",
                "context_budget",
            ],
            absent_tool_names: vec!["bash", "spawn_subagent", "defra_query"],
            expected_warnings: vec!["mcp_empty_allowlist_all"],
            host_ceiling_warning: false,
        },
    ];

    for case in cases {
        let config = BehaviorToolConfig::from_tool_selection_document(
            case.name,
            &case.selection,
            &case.ceiling,
            Vec::new(),
        )
        .unwrap();
        let explanation = config.explain_with_runtime(
            case.mcp_services_online,
            "did:test:test",
            &std::collections::HashSet::new(),
        );

        for name in case.expected_tool_names {
            assert!(
                explanation.tool_names.contains(&name.to_string()),
                "{} should expose {name}; got {:?}",
                case.name,
                explanation.tool_names
            );
        }
        for name in case.absent_tool_names {
            assert!(
                !explanation.tool_names.contains(&name.to_string()),
                "{} should not expose {name}; got {:?}",
                case.name,
                explanation.tool_names
            );
        }
        for code in case.expected_warnings {
            assert!(
                explanation_has_warning(&explanation, code),
                "{} should warn {code}; got {:?}",
                case.name,
                explanation.warnings
            );
        }
        assert_eq!(
            explanation_has_warning(&explanation, "host_ceiling_not_global"),
            case.host_ceiling_warning,
            "{} host ceiling warning should match whether only built-in read tools survived",
            case.name
        );
    }
}

#[test]
fn explain_complex_document_combination_filters_subagents_and_groups_surface() {
    let own_agent_did = "did:test:local";
    let mut selection = crate::document_config::ToolSelectionDocument {
        selection_id: "complex-tools".to_string(),
        agent_did: own_agent_did.to_string(),
        display_name: Some("Complex Tools".to_string()),
        tool_policy_version: None,
        enable_file_tools: Some(true),
        file_tools_mode: Some("ReadOnly".to_string()),
        file_tool_root: None,
        enable_bash: Some(true),
        bash_mode: Some("ReadOnly".to_string()),
        command_execution_policy: None,
        command_allowed_argv_prefixes: Some(Vec::new()),
        command_forbidden_argv_prefixes: Some(Vec::new()),
        read_only_command_allowlist: None,
        command_network_mode: None,
        cli_tool_names: Some(Vec::new()),
        enable_meta_tools: Some(true),
        allowed_mcp_service_ids: Some(vec![
            "registry".to_string(),
            "registry".to_string(),
            "observability".to_string(),
        ]),
        backgroundable_tool_names: Some(vec!["bash".to_string(), "bash".to_string()]),
        approval_required_tools: None,
        subagent_targets: Some(vec![
            crate::document_config::subagent_target_entry(
                "worker",
                own_agent_did,
                "worker",
                Some("local worker".to_string()),
            ),
            crate::document_config::subagent_target_entry(
                "inactive",
                own_agent_did,
                "inactive",
                Some("inactive local worker".to_string()),
            ),
            crate::document_config::subagent_target_entry(
                "remote",
                "did:test:remote",
                "remote-worker",
                Some("remote worker".to_string()),
            ),
        ]),
        subagent_spawn_enabled: Some(true),
        subagent_steering_enabled: Some(true),
        subagent_background_enabled: Some(true),
        subagent_default_await_mode: None,
        subagent_allow_cross_deployment: Some(false),
        cross_deployment_spawn_timeout_seconds: Some(120),
        enable_memory: Some(true),
        enable_session_history_tool: Some(false),
        enable_context_budget: Some(true),
        enable_defra_query: Some(true),
        defra_query_collections: Some(vec![
            "AgentRequest".to_string(),
            "AgentRequest".to_string(),
            " AgentResponse ".to_string(),
        ]),
        write_tools: None,
        datastore_tool_surface_ids: None,
        enable_self_config: None,
        self_config_categories: None,
        self_config_no_lockout: None,
        self_config_dry_run: None,
        enable_lsp: None,
        lsp_config: None,
        enable_graph_dsl: None,
    };
    let ceiling = ToolCeiling::readonly_at(temp_root("gents-complex-package-root"));
    let config = BehaviorToolConfig::from_tool_selection_document(
        "complex",
        &selection,
        &ceiling,
        Vec::new(),
    )
    .unwrap();
    assert_eq!(
        config.allowed_mcp_service_ids(),
        &["registry".to_string(), "observability".to_string()]
    );

    let active_behavior_ids = std::collections::HashSet::from(["worker".to_string()]);
    let surface = config.resolve_with_available_subagent_targets_for_mcp_presence(
        true,
        own_agent_did,
        &active_behavior_ids,
    );
    assert_eq!(
        resolve_subagent_target_descriptions(&surface),
        vec![("worker".to_string(), "local worker".to_string())]
    );
    let explanation = ToolSurfaceExplanation::from_resolved(&config, &surface);

    for name in [
        "read_file",
        "bash",
        "call_tool",
        "spawn_process",
        "spawn_subagent",
        "steer_subagent",
        "defra_query",
    ] {
        assert!(
            explanation.tool_names.contains(&name.to_string()),
            "complex surface should expose {name}; got {:?}",
            explanation.tool_names
        );
    }
    assert!(!explanation_has_warning(
        &explanation,
        "defra_query_empty_scope_all"
    ));
    assert!(explanation_category_contains(
        &explanation.included,
        "subagent",
        "spawn_subagent"
    ));
    assert!(explanation_category_contains(
        &explanation.included,
        "background_process",
        "spawn_process"
    ));
    assert!(explanation_category_contains(
        &explanation.included,
        "meta_mcp",
        "call_tool"
    ));

    #[cfg(feature = "agent-memory")]
    assert!(explanation_category_contains(
        &explanation.included,
        "built_in_memory",
        "memory"
    ));
    #[cfg(not(feature = "agent-memory"))]
    {
        assert!(explanation_category_contains(
            &explanation.unavailable,
            "built_in_memory",
            "memory"
        ));
        assert!(explanation_has_warning(
            &explanation,
            "memory_requested_compiled_out"
        ));
    }

    selection.subagent_allow_cross_deployment = Some(true);
    let config = BehaviorToolConfig::from_tool_selection_document(
        "complex-cross-deployment",
        &selection,
        &ceiling,
        Vec::new(),
    )
    .unwrap();
    let surface = config.resolve_with_available_subagent_targets_for_mcp_presence(
        true,
        own_agent_did,
        &active_behavior_ids,
    );
    assert_eq!(
        resolve_subagent_target_descriptions(&surface),
        vec![
            ("worker".to_string(), "local worker".to_string()),
            ("remote".to_string(), "remote worker".to_string())
        ]
    );
}

#[test]
fn explain_default_surface_calls_out_builtin_reads_and_defra_query_scope() {
    let config = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection::default(),
        &ToolCeiling::meta_only(),
        Vec::new(),
    )
    .unwrap();
    let explanation =
        config.explain_with_runtime(false, "did:test:test", &std::collections::HashSet::new());

    assert!(explanation
        .tool_names
        .contains(&crate::toolset::CONTEXT_BUDGET_TOOL_NAME.to_string()));
    // `sessions` is opt-in (enable_session_history_tool, default off), so the
    // default surface excludes it rather than listing it as callable.
    assert!(!explanation
        .tool_names
        .contains(&crate::toolset::SESSION_HISTORY_TOOL_NAME.to_string()));
    assert!(explanation.excluded.get("built_in_read").is_some_and(
        |names| names.contains(&crate::toolset::SESSION_HISTORY_TOOL_NAME.to_string())
    ));
    // defra_query is opt-in (#592): the default surface excludes it, and with
    // the tool off the empty-scope warning has nothing to warn about.
    assert!(!explanation
        .tool_names
        .contains(&crate::defra_query::DEFRA_QUERY_TOOL_NAME.to_string()));
    assert!(!explanation
        .warnings
        .iter()
        .any(|warning| warning.code == "defra_query_empty_scope_all"));
    assert!(
        explanation
            .policy
            .effective
            .get("built_in_read")
            .is_some_and(
                |names| names.contains(&crate::toolset::CONTEXT_BUDGET_TOOL_NAME.to_string())
            )
    );
    assert!(explanation
        .unavailable
        .get("meta_mcp")
        .is_some_and(|names| names.contains(&"discover_tools".to_string())));
}

#[test]
fn category_complete_ceiling_clamps_builtin_reads() {
    let mut ceiling_policy =
        ToolPolicySurface::legacy_non_host_wide(FileToolMode::Off, BashMode::Off);
    ceiling_policy.context_budget = false;
    ceiling_policy.defra_query = false;
    ceiling_policy.defra_collections = EndpointScope::None;

    let config = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection::default(),
        &ToolCeiling::meta_only().with_policy(ceiling_policy),
        Vec::new(),
    )
    .unwrap();
    let explanation =
        config.explain_with_runtime(false, "did:test:test", &std::collections::HashSet::new());

    assert!(!explanation
        .tool_names
        .contains(&crate::toolset::CONTEXT_BUDGET_TOOL_NAME.to_string()));
    assert!(!explanation
        .tool_names
        .contains(&crate::defra_query::DEFRA_QUERY_TOOL_NAME.to_string()));
    assert!(
        explanation.excluded.get("built_in_read").is_some_and(
            |names| names.contains(&crate::toolset::CONTEXT_BUDGET_TOOL_NAME.to_string())
        )
    );
}

#[test]
fn explain_mcp_empty_allowlist_reports_all_services_when_online() {
    let config = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            enable_meta_tools: true,
            allowed_mcp_service_ids: Vec::new(),
            enable_defra_query: false,
            ..Default::default()
        },
        &ToolCeiling::meta_only(),
        Vec::new(),
    )
    .unwrap();
    let explanation =
        config.explain_with_runtime(true, "did:test:test", &std::collections::HashSet::new());

    assert!(explanation.tool_names.contains(&"call_tool".to_string()));
    assert!(explanation
        .warnings
        .iter()
        .any(|warning| warning.code == "mcp_empty_allowlist_all"));
    assert!(explanation
        .included
        .get("meta_mcp")
        .is_some_and(|names| names.contains(&"call_tool".to_string())));
}

#[cfg(feature = "agent-memory")]
#[tokio::test]
async fn memory_tool_requires_selection_opt_in() {
    let node = defra_node::EmbeddedNode::builder().build().await.unwrap();
    crate::ensure_runtime_schemas(&node).await.unwrap();

    let disabled = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            enable_memory: false,
            ..Default::default()
        },
        &ToolCeiling::meta_only(),
        Vec::new(),
    )
    .unwrap()
    .resolve(&node)
    .await
    .unwrap();
    assert!(!disabled
        .tool_names()
        .contains(&crate::toolset::MEMORY_TOOL_NAME.to_string()));

    let enabled = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            enable_memory: true,
            ..Default::default()
        },
        &ToolCeiling::meta_only(),
        Vec::new(),
    )
    .unwrap()
    .resolve(&node)
    .await
    .unwrap();
    assert!(enabled
        .tool_names()
        .contains(&crate::toolset::MEMORY_TOOL_NAME.to_string()));
}

/// #592: `defra_query` must be OFF unless explicitly enabled. The programmatic
/// default, the legacy document decode (no `tool_policy_version`), and the
/// meta-only baseline all exclude it; only an explicit
/// `enable_defra_query: true` (or the wide-open preset, which materializes one)
/// surfaces the tool.
#[tokio::test]
async fn defra_query_is_off_by_default() {
    let node = defra_node::EmbeddedNode::builder().build().await.unwrap();
    crate::ensure_runtime_schemas(&node).await.unwrap();

    // Programmatic default surface.
    let default_surface = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection::default(),
        &ToolCeiling::meta_only(),
        Vec::new(),
    )
    .unwrap()
    .resolve(&node)
    .await
    .unwrap();
    assert!(
        !default_surface
            .tool_names()
            .contains(&"defra_query".to_string()),
        "default ToolSelection must not surface defra_query: {:?}",
        default_surface.tool_names()
    );

    // Legacy documents (no tool_policy_version, field absent) are NOT
    // grandfathered into defra_query.
    let legacy = crate::document_config::ToolSelectionDocument::default();
    assert!(
        !ToolSelection::from_document(&legacy)
            .unwrap()
            .enable_defra_query
    );

    // Explicit opt-in still decodes to enabled.
    let explicit = crate::document_config::ToolSelectionDocument {
        enable_defra_query: Some(true),
        ..Default::default()
    };
    assert!(
        ToolSelection::from_document(&explicit)
            .unwrap()
            .enable_defra_query
    );

    // The meta-only baseline excludes it too.
    let meta_only = BehaviorToolConfig::meta_only()
        .resolve(&node)
        .await
        .unwrap();
    assert!(
        !meta_only.tool_names().contains(&"defra_query".to_string()),
        "meta_only baseline must not surface defra_query: {:?}",
        meta_only.tool_names()
    );
}

/// The `agent-config` alias in `defra_query_collections` expands to the
/// configuration read surface: config collections are allowed, conversation
/// content stays denied, and the alias itself is never treated as a literal
/// collection name.
#[tokio::test]
async fn agent_config_alias_expands_to_config_scope() {
    let node = defra_node::EmbeddedNode::builder().build().await.unwrap();
    crate::ensure_runtime_schemas(&node).await.unwrap();

    let surface = BehaviorToolConfig::from_selection(
        "ops",
        ToolSelection {
            enable_defra_query: true,
            defra_query_collections: vec!["agent-config".to_string()],
            ..Default::default()
        },
        &ToolCeiling::meta_only(),
        Vec::new(),
    )
    .unwrap()
    .resolve(&node)
    .await
    .unwrap();

    assert!(surface.tool_names().contains(&"defra_query".to_string()));
    for allowed in ["AgentBehavior", "ToolSelection", "Schedule", "AgentRuntime"] {
        assert!(
            surface.defra_query_scope.ensure_allowed(allowed).is_ok(),
            "{allowed} must be readable under the agent-config preset"
        );
    }
    for denied in [
        "AgentRequest",
        "AgentMessage",
        "OAuthCredential",
        "agent-config",
    ] {
        assert!(
            surface.defra_query_scope.ensure_allowed(denied).is_err(),
            "{denied} must stay outside the agent-config preset"
        );
    }
}
