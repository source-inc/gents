use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use gents::graphql::escape_graphql_string;
use gents::{
    cli_tool, default_behavior_id_for_agent, default_inference_profile_id_for_behavior,
    default_tool_selection_id_for_behavior, ensure_agent_principal, load_agent_behavior,
    subagent_target_entry, upsert_agent_behavior, AgentIdentity, DocumentRuntimeOptions, Gents,
    KeyIdentity, ToolCeiling,
};
use gents_desktop_core::client::ClientCore;
use gents_protocol::row::{AgentBehaviorRow, InferenceProfileRow, ToolSelectionRow};
use serde_json::Value;
use tokio::sync::watch;
use tracing::Instrument;

use super::backend::AgentBackendConfig;
use super::workspace::seed_repo_workspace;
use super::DEFAULT_DEPLOYMENT_LABEL;

#[derive(Debug, Clone)]
pub(crate) struct LiveAgentDocs {
    pub(crate) behavior_id: String,
    pub(crate) subagent_behavior_id: String,
    pub(crate) backend_id: String,
    pub(crate) subagent_backend_id: String,
    pub(crate) tool_selection_id: String,
    pub(crate) subagent_tool_selection_id: String,
    pub(crate) inference_profile_id: String,
}

pub(crate) struct RunningAgent {
    pub(crate) did: String,
    shutdown_tx: watch::Sender<bool>,
    run_task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl RunningAgent {
    pub(crate) async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown_tx.send(true);
        self.run_task.await??;
        Ok(())
    }
}

pub(super) async fn spawn_live_agent(
    node_owner: Arc<ClientCore>,
    key_path: PathBuf,
    name: &str,
    backend: &AgentBackendConfig,
    subagent_backend: Option<&AgentBackendConfig>,
) -> Result<(RunningAgent, LiveAgentDocs, PathBuf)> {
    let tool_root = key_path
        .parent()
        .map(|parent| parent.join("tool-root"))
        .unwrap_or_else(|| std::env::temp_dir().join(format!("gents-tools-{name}")));
    std::fs::create_dir_all(&tool_root)
        .with_context(|| format!("creating live tool root {}", tool_root.display()))?;
    seed_repo_workspace(&tool_root)?;

    let identity = Arc::new(KeyIdentity::load_or_create(key_path, None)?);
    let did = identity.did().to_string();
    let docs =
        seed_live_behavior_documents(node_owner.as_ref(), &did, name, backend, subagent_backend)
            .await?;
    let agent = Gents::from_default_behavior_documents(
        node_owner.node_arc(),
        identity,
        DocumentRuntimeOptions {
            tool_ceiling: ToolCeiling::readwrite(tool_root.clone())
                .with_command_timeout_secs(30)
                .with_cli_tool(cli_tool("rg", "rg", "Search files with ripgrep")),
            ..Default::default()
        },
    )
    .await?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let run_task = tokio::spawn(agent.run(shutdown_rx).instrument(tracing::info_span!(
        "live_bridge_agent",
        deployment_label = %DEFAULT_DEPLOYMENT_LABEL,
        agent_did = %did
    )));
    wait_for_runtime_process_state(node_owner.node(), &did, "ready").await?;

    Ok((
        RunningAgent {
            did,
            shutdown_tx,
            run_task,
        },
        docs,
        tool_root,
    ))
}

async fn seed_live_behavior_documents(
    core: &ClientCore,
    agent_did: &str,
    agent_name: &str,
    backend: &AgentBackendConfig,
    subagent_backend: Option<&AgentBackendConfig>,
) -> Result<LiveAgentDocs> {
    let behavior_id = default_behavior_id_for_agent(agent_did);
    let subagent_behavior_id = format!("{agent_did}:live-repo-audit-subagent");
    let backend_id = format!("{agent_name}-backend");
    let subagent_backend_id = if subagent_backend.is_some() {
        format!("{agent_name}-subagent-backend")
    } else {
        backend_id.clone()
    };
    let tool_selection_id = default_tool_selection_id_for_behavior(&behavior_id);
    let subagent_tool_selection_id = default_tool_selection_id_for_behavior(&subagent_behavior_id);
    let inference_profile_id = default_inference_profile_id_for_behavior(&behavior_id);

    bind_default_behavior_backend(core.node(), agent_did, &backend_id, backend).await?;
    if let Some(sub) = subagent_backend {
        upsert_inference_backend(core.node(), &subagent_backend_id, sub).await?;
    }

    core.save_tool_selection(&ToolSelectionRow {
        selection_id: tool_selection_id.clone(),
        agent_did: Some(agent_did.to_string()),
        display_name: Some("Live Repo Audit Tools".to_string()),
        tool_policy_version: None,
        subagent_default_await_mode: None,
        write_tools: Vec::new(),
        enable_self_config: None,
        self_config_categories: Vec::new(),
        self_config_no_lockout: None,
        self_config_dry_run: None,
        enable_lsp: None,
        lsp_config: None,
        enable_file_tools: Some(true),
        file_tools_mode: Some("ReadOnly".to_string()),
        file_tool_root: None,
        // Keep a real native-background lane in the live desktop fixture.  The
        // Operations rail E2E uses it to observe a `spawn_process` call while
        // its underlying command is still running.
        enable_bash: Some(true),
        bash_mode: Some("Unrestricted".to_string()),
        command_execution_policy: None,
        read_only_command_allowlist: Vec::new(),
        command_allowed_argv_prefixes: Vec::new(),
        command_forbidden_argv_prefixes: Vec::new(),
        command_network_mode: None,
        cli_tool_names: Vec::new(),
        enable_meta_tools: Some(false),
        allowed_mcp_service_ids: Vec::new(),
        delegate_to: vec![],
        backgroundable_tool_names: vec!["bash_unrestricted".to_string()],
        enable_memory: Some(false),
        enable_session_history_tool: Some(false),
        enable_context_budget: Some(true),
        subagent_targets: vec![subagent_target_entry(
            "repo-audit-subagent",
            agent_did,
            &subagent_behavior_id,
            Some("Local repository audit subagent for the desktop live fixture".to_string()),
        )],
        subagent_spawn_enabled: Some(true),
        subagent_steering_enabled: Some(true),
        subagent_background_enabled: Some(true),
        subagent_allow_cross_deployment: Some(false),
        cross_deployment_spawn_timeout_seconds: Some(60),
        enable_defra_query: Some(false),
        defra_query_collections: Vec::new(),
    })
    .await?;
    core.save_tool_selection(&ToolSelectionRow {
        selection_id: subagent_tool_selection_id.clone(),
        agent_did: Some(agent_did.to_string()),
        display_name: Some("Live Repo Audit Subagent Tools".to_string()),
        tool_policy_version: None,
        subagent_default_await_mode: None,
        write_tools: Vec::new(),
        enable_self_config: None,
        self_config_categories: Vec::new(),
        self_config_no_lockout: None,
        self_config_dry_run: None,
        enable_lsp: None,
        lsp_config: None,
        enable_file_tools: Some(true),
        file_tools_mode: Some("ReadOnly".to_string()),
        file_tool_root: None,
        enable_bash: Some(false),
        bash_mode: Some("ReadOnly".to_string()),
        command_execution_policy: None,
        read_only_command_allowlist: Vec::new(),
        command_allowed_argv_prefixes: Vec::new(),
        command_forbidden_argv_prefixes: Vec::new(),
        command_network_mode: None,
        cli_tool_names: Vec::new(),
        enable_meta_tools: Some(false),
        allowed_mcp_service_ids: Vec::new(),
        delegate_to: vec![],
        backgroundable_tool_names: Vec::new(),
        enable_memory: Some(false),
        enable_session_history_tool: Some(false),
        enable_context_budget: Some(true),
        subagent_targets: Vec::new(),
        subagent_spawn_enabled: Some(false),
        subagent_steering_enabled: Some(false),
        subagent_background_enabled: Some(false),
        subagent_allow_cross_deployment: Some(false),
        cross_deployment_spawn_timeout_seconds: None,
        enable_defra_query: Some(false),
        defra_query_collections: Vec::new(),
    })
    .await?;
    core.save_inference_profile(&InferenceProfileRow {
        profile_id: inference_profile_id.clone(),
        display_name: Some("Live Repo Audit Profile".to_string()),
        context_window: Some(131_072),
        max_output_tokens: Some(1_024),
        max_turns: Some(20),
        temperature: Some(0.0),
        stream_batch_ms: Some(250),
        stream_liveness_timeout_secs: Some(300),
        deadline_duration_secs: Some(300),
        retry_max_transport: None,
        retry_backoff_ms: None,
        retry_max_resample: None,
        retry_allow_repair: None,
        retry_interactive_max: None,
        top_p: None,
        top_k: None,
        seed: None,
        min_p: None,
        frequency_penalty: None,
        presence_penalty: None,
        repetition_penalty: None,
        reasoning_effort: None,
    })
    .await?;
    core.save_behavior(&AgentBehaviorRow {
        behavior_id: behavior_id.clone(),
        agent_did: Some(agent_did.to_string()),
        display_name: Some("Live Repo Audit Default".to_string()),
        system_prompt: Some(
            "You are Amy, a repository analysis agent operating inside a live desktop integration test. Keep answers concise. Use only the exact files requested by the user, and do not explore the wider repository unless explicitly asked. When the user explicitly asks you to use the local subagent, call spawn_subagent with name \"repo-audit-subagent\" and await_mode \"background\", then call wait_subagent with the returned child_request_id to retrieve the child's result before you reply to the user. When the user explicitly asks you to launch a native background process, call spawn_process with tool_name \"bash_unrestricted\" and the exact requested arguments. Do not call wait_process, read_process, list_processes, or cancel_process unless the user explicitly asks."
                .to_string(),
        ),
        backend_id: Some(backend_id.clone()),
        model_name: Some(backend.model_name.clone()),
        tool_selection_id: Some(tool_selection_id.clone()),
        inference_profile_id: Some(inference_profile_id.clone()),
        compaction_strategy: Some("StripThenSummarize".to_string()),
        compaction_threshold: Some(0.95),
        enabled: Some(true),
        skill_refs: Vec::new(),
        skill_excludes: Vec::new(),
        created_at: Some(Utc::now().to_rfc3339()),
    })
    .await?;
    let subagent_model_name = subagent_backend
        .map(|s| s.model_name.clone())
        .unwrap_or_else(|| backend.model_name.clone());
    core.save_behavior(&AgentBehaviorRow {
        behavior_id: subagent_behavior_id.clone(),
        agent_did: Some(agent_did.to_string()),
        display_name: Some("Live Repo Audit Subagent".to_string()),
        system_prompt: Some(
            "You are Amy's local repo audit subagent inside a live desktop integration test. Read only the exact files requested by the parent and return concise findings."
                .to_string(),
        ),
        backend_id: Some(subagent_backend_id.clone()),
        model_name: Some(subagent_model_name),
        tool_selection_id: Some(subagent_tool_selection_id.clone()),
        inference_profile_id: Some(inference_profile_id.clone()),
        compaction_strategy: Some("StripThenSummarize".to_string()),
        compaction_threshold: Some(0.95),
        enabled: Some(true),
        skill_refs: Vec::new(),
        skill_excludes: Vec::new(),
        created_at: Some(Utc::now().to_rfc3339()),
    })
    .await?;
    core.refresh_store().await?;

    Ok(LiveAgentDocs {
        behavior_id,
        subagent_behavior_id,
        backend_id,
        subagent_backend_id,
        tool_selection_id,
        subagent_tool_selection_id,
        inference_profile_id,
    })
}

async fn upsert_inference_backend(
    node: &gents::defra_node::EmbeddedNode,
    backend_id: &str,
    backend: &AgentBackendConfig,
) -> Result<()> {
    let escaped_backend_id = escape_graphql_string(backend_id);
    let escaped_endpoint = escape_graphql_string(&backend.endpoint);
    let escaped_provider_kind = escape_graphql_string(backend.provider_kind.as_str());
    let escaped_model_name = escape_graphql_string(&backend.model_name);
    let api_key_field = graphql_optional_string_field("api_key", backend.api_key.as_deref());
    let api_key_env_var_field =
        graphql_optional_string_field("api_key_env_var", backend.api_key_env_var.as_deref());
    let mutation = format!(
        r#"mutation {{
            upsert_InferenceBackend(
                filter: {{ backend_id: {{ _eq: "{escaped_backend_id}" }} }},
                add: {{
                    backend_id: "{escaped_backend_id}",
                    name: "{escaped_backend_id}",
                    provider_kind: "{escaped_provider_kind}",
                    endpoint: "{escaped_endpoint}",
                    {api_key_field}
                    {api_key_env_var_field}
                    max_concurrent: 2,
                    max_queue_depth: 100,
                    enabled: true,
                    models: ["{escaped_model_name}"],
                    probe_status: "healthy"
                }},
                update: {{
                    name: "{escaped_backend_id}",
                    provider_kind: "{escaped_provider_kind}",
                    endpoint: "{escaped_endpoint}",
                    {api_key_field}
                    {api_key_env_var_field}
                    max_concurrent: 2,
                    max_queue_depth: 100,
                    enabled: true,
                    models: ["{escaped_model_name}"],
                    probe_status: "healthy"
                }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    if response.has_errors() {
        anyhow::bail!("upsert inference backend failed: {:?}", response.errors);
    }
    Ok(())
}

async fn bind_default_behavior_backend(
    node: &gents::defra_node::EmbeddedNode,
    agent_did: &str,
    backend_id: &str,
    backend: &AgentBackendConfig,
) -> Result<()> {
    let bootstrap = ensure_agent_principal(node, agent_did).await?;
    upsert_inference_backend(node, backend_id, backend).await?;
    let mut default_behavior = load_agent_behavior(node, &bootstrap.default_behavior.behavior_id)
        .await?
        .expect("default behavior document");
    default_behavior.backend_id = Some(backend_id.to_string());
    default_behavior.model_name = Some(backend.model_name.clone());
    upsert_agent_behavior(node, &default_behavior).await?;
    Ok(())
}

fn graphql_optional_string_field(name: &str, value: Option<&str>) -> String {
    value
        .filter(|value| !value.trim().is_empty())
        .map(|value| format!(r#"{name}: "{}","#, escape_graphql_string(value)))
        .unwrap_or_default()
}

async fn wait_for_runtime_process_state(
    node: &gents::defra_node::EmbeddedNode,
    agent_did: &str,
    expected_process_state: &str,
) -> Result<()> {
    let escaped_agent_did = escape_graphql_string(agent_did);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let query = format!(
            r#"{{
                AgentRuntime(
                    filter: {{ agent_did: {{ _eq: "{escaped_agent_did}" }} }},
                    limit: 1
                ) {{
                    process_state
                }}
            }}"#
        );
        let response = node.execute(&query).await;
        if response.has_errors() {
            anyhow::bail!("AgentRuntime query failed: {:?}", response.errors);
        }
        let process_state = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRuntime"))
            .and_then(Value::as_array)
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("process_state"))
            .and_then(Value::as_str);
        if process_state == Some(expected_process_state) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for AgentRuntime {agent_did} to reach process_state={expected_process_state}; last={process_state:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
