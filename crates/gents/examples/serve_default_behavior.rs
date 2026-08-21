use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use gents::defra_node::{EmbeddedNode, HttpConfig};
use gents::graphql::escape_graphql_string;
use gents::{
    ensure_agent_principal, ensure_runtime_schemas, upsert_agent_behavior,
    upsert_inference_profile, upsert_tool_selection, AgentBehaviorDocument, AgentIdentity,
    DocumentRuntimeOptions, Gents, InferenceProfile, KeyIdentity, McpPool, ToolCeiling,
    ToolSelectionDocument, DEFAULT_MAX_TURNS,
};
use tokio::sync::watch;

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_or_u16(name: &str, default: u16) -> u16 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_or_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<()> {
    let data_dir = PathBuf::from(env_or("GENTS_DATA_DIR", "./var/defradb"));
    let http_port = env_or_u16("GENTS_HTTP_PORT", 9191);
    let agent_name = env_or("GENTS_NAME", "demo");
    let backend_id = env_or("GENTS_BACKEND_ID", "demo-backend");
    let model_endpoint = env_or("GENTS_MODEL_ENDPOINT", "http://127.0.0.1:8000/v1");
    let model_name = env_or("GENTS_MODEL_NAME", "default");
    let system_prompt = std::env::var("GENTS_SYSTEM_PROMPT").unwrap_or_default();
    let deadline_secs = env_or_u64("GENTS_DEADLINE_SECS", 900);
    let key_path = std::env::var("GENTS_KEY_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| data_dir.join("keys").join(format!("{agent_name}.key")));

    let http_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), http_port);
    let identity = Arc::new(
        KeyIdentity::load_or_create(key_path, None)
            .context("creating or loading agent identity key")?,
    );
    let node = Arc::new(
        EmbeddedNode::builder()
            .data_path(&data_dir)
            .with_http(HttpConfig::with_addr(http_addr))
            .with_node_identity_did(identity.did())
            .build()
            .await
            .context("building embedded DefraDB node")?,
    );
    ensure_runtime_schemas(node.as_ref()).await?;
    seed_demo_documents(
        node.as_ref(),
        identity.did(),
        &backend_id,
        &model_endpoint,
        &model_name,
        &system_prompt,
        deadline_secs,
    )
    .await?;

    let agent = Gents::from_default_behavior_documents(
        node,
        identity.clone(),
        DocumentRuntimeOptions {
            mcp_pool: McpPool::new(),
            local_hostname: Some("localhost".to_string()),
            tool_ceiling: ToolCeiling::readonly(),
            ..Default::default()
        },
    )
    .await?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = shutdown_tx.send(true);
        }
    });

    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "status": "serving",
            "agent_name": agent_name,
            "agent_did": agent.agent_did(),
            "graphql": format!("http://127.0.0.1:{http_port}/api/v0/graphql"),
            "backend_id": backend_id,
        }))?
    );

    agent.run(shutdown_rx).await
}

async fn seed_demo_documents(
    node: &EmbeddedNode,
    agent_did: &str,
    backend_id: &str,
    model_endpoint: &str,
    model_name: &str,
    system_prompt: &str,
    deadline_secs: u64,
) -> Result<()> {
    let bootstrap = ensure_agent_principal(node, agent_did).await?;
    let inference_profile_id = format!("{}:demo-profile", bootstrap.default_behavior.behavior_id);
    let tool_selection_id = format!("{}:demo-tools", bootstrap.default_behavior.behavior_id);

    upsert_demo_backend(node, backend_id, model_endpoint).await?;
    upsert_tool_selection(
        node,
        &ToolSelectionDocument {
            selection_id: tool_selection_id.clone(),
            agent_did: agent_did.to_string(),
            display_name: Some("Demo Tools".to_string()),
            tool_policy_version: None,
            enable_file_tools: Some(true),
            file_tools_mode: Some("ReadOnly".to_string()),
            file_tool_root: None,
            enable_bash: Some(true),
            bash_mode: Some("ReadOnly".to_string()),
            command_execution_policy: None,
            read_only_command_allowlist: None,
            command_allowed_argv_prefixes: Some(Vec::new()),
            command_forbidden_argv_prefixes: Some(Vec::new()),
            command_network_mode: None,
            cli_tool_names: Some(Vec::new()),
            enable_meta_tools: Some(true),
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
        },
    )
    .await?;
    upsert_inference_profile(
        node,
        &InferenceProfile {
            profile_id: inference_profile_id.clone(),
            display_name: Some("Demo".to_string()),
            context_window: Some(131_072),
            max_output_tokens: Some(32_768),
            max_turns: Some(DEFAULT_MAX_TURNS as i64),
            temperature: None,
            stream_batch_ms: Some(1_000),
            stream_liveness_timeout_secs: None,
            deadline_duration_secs: Some(deadline_secs as i64),
            retry_max_transport: None,
            retry_backoff_ms: None,
            retry_max_resample: None,
            retry_allow_repair: None,
            retry_interactive_max: None,
            ..Default::default()
        },
    )
    .await?;
    upsert_agent_behavior(
        node,
        &AgentBehaviorDocument {
            behavior_id: bootstrap.default_behavior.behavior_id,
            agent_did: agent_did.to_string(),
            display_name: Some("Default".to_string()),
            description: None,
            summary: None,
            system_prompt: Some(system_prompt.to_string()),
            request_context_template: None,
            backend_id: Some(backend_id.to_string()),
            model_name: Some(model_name.to_string()),
            tool_selection_id: Some(tool_selection_id),
            inference_profile_id: Some(inference_profile_id),
            compaction_strategy: Some("StripThenSummarize".to_string()),
            compaction_threshold: Some(0.75),
            enabled: true,
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            created_at: bootstrap.default_behavior.created_at,
        },
    )
    .await?;
    Ok(())
}

async fn upsert_demo_backend(node: &EmbeddedNode, backend_id: &str, endpoint: &str) -> Result<()> {
    let mutation = format!(
        r#"mutation {{
            upsert_InferenceBackend(
                filter: {{ backend_id: {{ _eq: "{backend_id}" }} }},
                add: {{
                    backend_id: "{backend_id}",
                    name: "{backend_id}",
                    endpoint: "{endpoint}",
                    max_concurrent: 2,
                    enabled: true,
                    models: ["default"],
                    probe_status: "healthy"
                }},
                update: {{
                    name: "{backend_id}",
                    endpoint: "{endpoint}",
                    max_concurrent: 2,
                    enabled: true,
                    probe_status: "healthy"
                }}
            ) {{ _docID }}
        }}"#,
        backend_id = escape_graphql_string(backend_id),
        endpoint = escape_graphql_string(endpoint),
    );
    let response = node.execute(&mutation).await;
    if response.has_errors() {
        anyhow::bail!("upsert demo backend failed: {:?}", response.errors);
    }
    Ok(())
}
