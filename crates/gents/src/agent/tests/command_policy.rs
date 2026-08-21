use crate::agent::tool_selection_from_document;
use crate::document_config::ToolSelectionDocument;
use crate::toolset::CommandExecutionMode;

fn tool_selection_doc(bash_mode: &str) -> ToolSelectionDocument {
    ToolSelectionDocument {
        selection_id: "tools".to_string(),
        agent_did: "did:key:zAgent".to_string(),
        display_name: Some("Tools".to_string()),
        tool_policy_version: None,
        enable_file_tools: Some(false),
        file_tools_mode: Some("Off".to_string()),
        file_tool_root: None,
        enable_bash: Some(true),
        bash_mode: Some(bash_mode.to_string()),
        command_execution_policy: None,
        command_allowed_argv_prefixes: Some(Vec::new()),
        command_forbidden_argv_prefixes: Some(Vec::new()),
        read_only_command_allowlist: None,
        command_network_mode: None,
        cli_tool_names: Some(Vec::new()),
        enable_meta_tools: Some(false),
        allowed_mcp_service_ids: Some(Vec::new()),
        backgroundable_tool_names: Some(Vec::new()),
        approval_required_tools: None,
        subagent_targets: Some(Vec::new()),
        subagent_spawn_enabled: Some(false),
        subagent_steering_enabled: Some(false),
        subagent_background_enabled: Some(false),
        subagent_default_await_mode: Some("foreground".to_string()),
        subagent_allow_cross_deployment: Some(false),
        cross_deployment_spawn_timeout_seconds: None,
        enable_memory: None,
        enable_session_history_tool: None,
        enable_context_budget: None,
        enable_defra_query: None,
        defra_query_collections: None,
        write_tools: None,
        datastore_tool_surface_ids: None,
        enable_self_config: None,
        self_config_categories: None,
        self_config_no_lockout: None,
        self_config_dry_run: None,
        enable_lsp: None,
        lsp_config: None,
    }
}

#[test]
fn unrestricted_bash_mode_defaults_to_unrestricted_command_policy() {
    let selection = tool_selection_from_document(&tool_selection_doc("Unrestricted")).unwrap();

    assert_eq!(
        selection.command_policy.unwrap().mode,
        CommandExecutionMode::Unrestricted
    );
}

#[test]
fn unrestricted_bash_mode_can_request_workspace_write_command_policy() {
    let mut doc = tool_selection_doc("Unrestricted");
    doc.command_execution_policy = Some("workspace_write".to_string());

    let selection = tool_selection_from_document(&doc).unwrap();

    assert_eq!(
        selection.command_policy.unwrap().mode,
        CommandExecutionMode::WorkspaceWrite
    );
}

#[test]
fn readonly_bash_mode_uses_builder_default_policy() {
    let selection = tool_selection_from_document(&tool_selection_doc("ReadOnly")).unwrap();

    assert!(selection.command_policy.is_none());
}

#[test]
fn readonly_bash_mode_custom_allowlist_flows_through_to_policy() {
    let mut doc = tool_selection_doc("ReadOnly");
    doc.read_only_command_allowlist = Some(vec!["cat".to_string(), "journalctl".to_string()]);

    let selection = tool_selection_from_document(&doc).unwrap();

    let policy = selection
        .command_policy
        .expect("a non-empty custom read-only allowlist must materialize a Some(command_policy)");
    assert_eq!(policy.mode, CommandExecutionMode::ReadOnly);
    assert_eq!(
        policy.read_only_allowlist(),
        ["cat".to_string(), "journalctl".to_string()]
    );
}

#[test]
fn readonly_bash_mode_empty_allowlist_falls_back_to_default() {
    // An explicitly-empty allowlist must behave identically to absent: no
    // override, so command_policy stays None and the builder applies the
    // hardcoded default_read_only_commands() list (never a deny-all surface).
    let mut doc = tool_selection_doc("ReadOnly");
    doc.read_only_command_allowlist = Some(Vec::new());

    let selection = tool_selection_from_document(&doc).unwrap();

    assert!(selection.command_policy.is_none());
}
