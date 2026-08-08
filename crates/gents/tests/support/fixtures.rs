use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use gents::__test_internals::run_subagent_source_for_test_with_ready;
use gents::compaction::CompactionStrategy;
use gents::defra_node::EmbeddedNode;
use gents::graphql::escape_graphql_string;
use gents::{
    ensure_agent_principal, load_agent_behavior, upsert_agent_behavior, ActiveRuntimeSnapshot,
    AgentBehavior, AgentIdentity, AgentPrincipal, BackendProviderKind, BehaviorToolConfig,
    KeyIdentity,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

pub struct SubagentSourceGuard {
    cancel: CancellationToken,
    handle: Option<tokio::task::JoinHandle<()>>,
    _snapshot_tx: watch::Sender<Arc<ActiveRuntimeSnapshot>>,
    ready: Option<tokio::sync::oneshot::Receiver<()>>,
}

impl SubagentSourceGuard {
    pub async fn wait_ready(&mut self) {
        let ready = self
            .ready
            .take()
            .expect("subagent source readiness may only be awaited once");
        ready
            .await
            .expect("subagent source task exited before opening its subscription");
        assert!(
            self.handle
                .as_ref()
                .is_some_and(|handle| !handle.is_finished()),
            "subagent source task exited immediately after readiness"
        );
    }
}

impl Drop for SubagentSourceGuard {
    fn drop(&mut self) {
        if !std::thread::panicking()
            && self
                .handle
                .as_ref()
                .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            panic!("subagent source task exited before its fixture guard was dropped");
        }
        self.cancel.cancel();
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

pub fn spawn_subagent_source(
    node: Arc<EmbeddedNode>,
    agent_did: &str,
    parent_behavior_id: &str,
    child_behavior_id: &str,
) -> SubagentSourceGuard {
    spawn_subagent_source_with_paired_peers(
        node,
        agent_did,
        parent_behavior_id,
        child_behavior_id,
        HashSet::new(),
    )
}

pub fn spawn_subagent_source_with_paired_peers(
    node: Arc<EmbeddedNode>,
    agent_did: &str,
    parent_behavior_id: &str,
    child_behavior_id: &str,
    paired_peer_dids: HashSet<String>,
) -> SubagentSourceGuard {
    let identity: Arc<dyn AgentIdentity> = Arc::new(test_identity("subagent-source-principal"));
    let principal = test_principal_for(identity, parent_behavior_id);
    let mut child = test_behavior_for_principal(child_behavior_id, principal.clone());
    child.principal = Arc::new(AgentPrincipal {
        agent_did: agent_did.to_string(),
        identity: principal.identity.clone(),
        default_behavior_id: parent_behavior_id.to_string(),
        display_name: None,
        enabled: true,
    });
    let mut behaviors = HashMap::new();
    behaviors.insert(child_behavior_id.to_string(), Arc::new(child));
    let snapshot = ActiveRuntimeSnapshot {
        generation: 1,
        principal: None,
        local_did: agent_did.to_string(),
        paired_peer_dids,
        default_behavior_id: parent_behavior_id.to_string(),
        behaviors,
        config_provenance_scope: gents::rendered_request::ConfigProvenanceScope::StaticOrOneShot,
        behavior_config_provenance: HashMap::new(),
        tool_surfaces: HashMap::new(),
        backend_admission_configs: HashMap::new(),
        unavailable_behaviors: HashMap::new(),
        active_schedules: HashMap::new(),
        unavailable_schedules: HashSet::new(),
        active_event_triggers: HashMap::new(),
        unavailable_event_triggers: HashSet::new(),
        active_tasks: HashMap::new(),
        dispatchers: HashMap::new(),
        behavior_executor_capacities: HashMap::new(),
        behavior_executor_queue_capacities: HashMap::new(),
    };
    let (snapshot_tx, snapshot_rx) = watch::channel(Arc::new(snapshot));
    let cancel = CancellationToken::new();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(run_subagent_source_for_test_with_ready(
        node,
        snapshot_rx,
        cancel.clone(),
        ready_tx,
    ));
    SubagentSourceGuard {
        cancel,
        handle: Some(handle),
        _snapshot_tx: snapshot_tx,
        ready: Some(ready_rx),
    }
}

pub fn test_identity(name: &str) -> KeyIdentity {
    let path = std::env::temp_dir().join(format!("{name}-{}.key", uuid::Uuid::new_v4()));
    KeyIdentity::load_or_create(path, None).unwrap()
}

pub fn test_principal_for(
    identity: Arc<dyn gents::AgentIdentity>,
    default_behavior_id: impl Into<String>,
) -> Arc<AgentPrincipal> {
    Arc::new(AgentPrincipal {
        agent_did: identity.did().to_string(),
        identity,
        default_behavior_id: default_behavior_id.into(),
        display_name: None,
        enabled: true,
    })
}

pub fn test_behavior(
    name: &str,
    backend_id: &str,
    backend_api_key_env_var: Option<&str>,
) -> AgentBehavior {
    let identity: Arc<dyn gents::AgentIdentity> = Arc::new(test_identity(name));
    let principal = test_principal_for(identity, name);
    AgentBehavior {
        skills: Vec::new(),
        behavior_id: name.to_string(),
        principal,
        backend_id: Some(backend_id.to_string()),
        backend_provider_kind: BackendProviderKind::OpenAiCompatible,
        openai_wire_api: gents::OpenAiWireApi::ChatCompletions,
        backend_endpoint: "http://localhost:8000/v1".to_string(),
        backend_api_key: None,
        backend_api_key_env_var: backend_api_key_env_var.map(ToOwned::to_owned),
        model_name: gents::config::DEFAULT_MODEL_NAME.to_string(),
        context_window: gents::config::DEFAULT_CONTEXT_WINDOW,
        max_output_tokens: gents::config::DEFAULT_MAX_OUTPUT_TOKENS,
        max_turns: gents::config::DEFAULT_MAX_TURNS,
        system_prompt: String::new(),
        request_context_template: None,
        tools: BehaviorToolConfig::default(),
        compaction_threshold: gents::config::DEFAULT_COMPACTION_THRESHOLD,
        compaction_strategy: CompactionStrategy::StripThenSummarize,
        stream_batch_ms: gents::config::DEFAULT_STREAM_BATCH_MS,
        stream_liveness_timeout: Duration::from_secs(
            gents::config::DEFAULT_STREAM_LIVENESS_TIMEOUT_SECS,
        ),
        deadline_duration: Duration::from_secs(gents::config::DEFAULT_DEADLINE_DURATION_SECS),
        completion_retry: gents::agent::completion_retry::CompletionRetryProfileFields::default(),
        sampling: gents::config::SamplingConfig::default(),
    }
}

pub fn test_behavior_for_principal(
    behavior_id: impl Into<String>,
    principal: Arc<AgentPrincipal>,
) -> AgentBehavior {
    let behavior_id = behavior_id.into();
    AgentBehavior {
        skills: Vec::new(),
        behavior_id,
        principal,
        backend_id: None,
        backend_provider_kind: BackendProviderKind::OpenAiCompatible,
        openai_wire_api: gents::OpenAiWireApi::ChatCompletions,
        backend_endpoint: "http://localhost:8000/v1".to_string(),
        backend_api_key: None,
        backend_api_key_env_var: None,
        model_name: gents::config::DEFAULT_MODEL_NAME.to_string(),
        context_window: gents::config::DEFAULT_CONTEXT_WINDOW,
        max_output_tokens: gents::config::DEFAULT_MAX_OUTPUT_TOKENS,
        max_turns: gents::config::DEFAULT_MAX_TURNS,
        system_prompt: String::new(),
        request_context_template: None,
        tools: BehaviorToolConfig::default(),
        compaction_threshold: gents::config::DEFAULT_COMPACTION_THRESHOLD,
        compaction_strategy: CompactionStrategy::StripThenSummarize,
        stream_batch_ms: gents::config::DEFAULT_STREAM_BATCH_MS,
        stream_liveness_timeout: Duration::from_secs(
            gents::config::DEFAULT_STREAM_LIVENESS_TIMEOUT_SECS,
        ),
        deadline_duration: Duration::from_secs(gents::config::DEFAULT_DEADLINE_DURATION_SECS),
        completion_retry: gents::agent::completion_retry::CompletionRetryProfileFields::default(),
        sampling: gents::config::SamplingConfig::default(),
    }
}

pub async fn bind_default_behavior_backend(
    node: &EmbeddedNode,
    agent_did: &str,
    backend_id: &str,
    endpoint: &str,
) {
    let bootstrap = ensure_agent_principal(node, agent_did).await.unwrap();
    let escaped_backend_id = escape_graphql_string(backend_id);
    let escaped_endpoint = escape_graphql_string(endpoint);
    let mutation = format!(
        r#"mutation {{
            upsert_InferenceBackend(
                filter: {{ backend_id: {{ _eq: "{escaped_backend_id}" }} }},
                add: {{
                    backend_id: "{escaped_backend_id}",
                    name: "{escaped_backend_id}",
                    endpoint: "{escaped_endpoint}",
                    max_concurrent: 1,
                    enabled: true,
                    models: ["default"],
                    probe_status: "healthy"
                }},
                update: {{
                    name: "{escaped_backend_id}",
                    endpoint: "{escaped_endpoint}",
                    max_concurrent: 1,
                    enabled: true,
                    probe_status: "healthy"
                }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(!response.has_errors(), "{:?}", response.errors);

    let mut default_behavior = load_agent_behavior(node, &bootstrap.default_behavior.behavior_id)
        .await
        .unwrap()
        .expect("default behavior document");
    default_behavior.backend_id = Some(backend_id.to_string());
    upsert_agent_behavior(node, &default_behavior)
        .await
        .unwrap();
}
