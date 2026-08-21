use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use defra_node::EmbeddedNode;
use tokio::sync::{watch, OnceCell};

use crate::backend_health::{BackendHealthMap, BackendProberOptions};
use crate::compaction::CompactionStrategy;
use crate::config::{
    AgentBehavior, SamplingConfig, DEFAULT_COMPACTION_THRESHOLD, DEFAULT_CONTEXT_WINDOW,
    DEFAULT_DEADLINE_DURATION_SECS, DEFAULT_MAX_OUTPUT_TOKENS, DEFAULT_MAX_TURNS,
    DEFAULT_MODEL_NAME, DEFAULT_STREAM_BATCH_MS, DEFAULT_STREAM_LIVENESS_TIMEOUT_SECS,
};
use crate::health_checker::HealthCheckerOptions;
use crate::hook::{BackgroundExecutionRegistry, FailurePolicy};
use crate::identity::{AgentIdentity, AgentPrincipal};
use crate::mcp_pool::McpPool;
use crate::migration;
use crate::retry::RetryPolicy;
use crate::runtime_snapshot::ResolvedRuntimeSnapshot;
use crate::tool_surface::{BehaviorToolConfig, SubagentToolConfig, ToolCeiling, ToolSelection};
use crate::trigger_engine::manual_source::ManualTriggerHandle;

mod builder;
pub mod completion_retry;
mod daemon;
pub mod directory_projection;
mod document_view;
pub(crate) mod loop_stream;
pub(crate) mod output_obligation;
pub mod p2p_reconcile;
pub mod persona_ops;
pub mod persona_presets;
pub(crate) mod principal_assembly;
mod reconcile;
mod runtime;
mod stream_processor;
#[cfg(test)]
mod supervision;
#[cfg(test)]
mod tests;

pub(crate) use principal_assembly::assemble_principal_and_behaviors;
pub(crate) use principal_assembly::BehaviorBuildError;

#[cfg(test)]
pub(crate) use builder::PendingAgentBehavior;
pub use builder::{BehaviorBuilder, GentsBuilder};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessLifecycleState {
    Uninitialized,
    Recovering,
    Ready,
    ShuttingDown,
    Shutdown,
}

impl ProcessLifecycleState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Uninitialized => "uninitialized",
            Self::Recovering => "recovering",
            Self::Ready => "ready",
            Self::ShuttingDown => "shuttingDown",
            Self::Shutdown => "shutdown",
        }
    }
}

pub trait ProcessLifecycleObserver: Send + Sync {
    fn on_process_state_change(&self, state: ProcessLifecycleState);
}

pub trait RuntimeSnapshotObserver: Send + Sync {
    fn on_generation_published(&self, generation: u64, runnable_behavior_ids: &[String]);
}

#[derive(Default)]
pub struct DocumentRuntimeOptions {
    pub tool_ceiling: ToolCeiling,
    pub mcp_pool: McpPool,
    pub local_hostname: Option<String>,
    pub local_subnet: Option<String>,
    pub retry_policy: RetryPolicy,
    pub hook_failure_policy: FailurePolicy,
    pub health_checker_options: HealthCheckerOptions,
    pub backend_prober_options: BackendProberOptions,
    pub backend_health: Option<BackendHealthMap>,
    pub process_state_observer: Option<Arc<dyn ProcessLifecycleObserver>>,
    pub runtime_snapshot_observer: Option<Arc<dyn RuntimeSnapshotObserver>>,
    pub startup_readiness: crate::startup_readiness::StartupReadinessOptions,
}

#[derive(Clone)]
pub(crate) struct DocumentResolveContext {
    pub(crate) identity: Arc<dyn AgentIdentity>,
    pub(crate) tool_ceiling: ToolCeiling,
    pub(crate) backend_health: BackendHealthMap,
}

#[derive(Clone)]
pub struct Gents {
    node: Arc<EmbeddedNode>,
    principal: Arc<AgentPrincipal>,
    behaviors: Vec<Arc<AgentBehavior>>,
    unavailable_behaviors: HashMap<String, String>,
    document_runtime_context: Option<DocumentResolveContext>,
    mcp_pool: McpPool,
    local_hostname: String,
    local_subnet: Option<String>,
    retry_policy: RetryPolicy,
    hook_failure_policy: FailurePolicy,
    background_execution_registry: BackgroundExecutionRegistry,
    health_checker_options: HealthCheckerOptions,
    backend_prober_options: BackendProberOptions,
    backend_health: BackendHealthMap,
    process_state_observer: Option<Arc<dyn ProcessLifecycleObserver>>,
    runtime_snapshot_observer: Option<Arc<dyn RuntimeSnapshotObserver>>,
    startup_readiness: crate::startup_readiness::StartupReadinessOptions,
    rendered_request_capture_factory:
        Option<crate::rendered_request::RenderedRequestCaptureFactory>,
    pub(crate) manual_trigger_handle: Arc<OnceCell<ManualTriggerHandle>>,
    operator_tool_root: Option<PathBuf>,
}

impl Gents {
    pub fn builder() -> GentsBuilder {
        GentsBuilder::new()
    }

    pub async fn from_default_behavior_documents(
        node: Arc<EmbeddedNode>,
        identity: Arc<dyn AgentIdentity>,
        options: DocumentRuntimeOptions,
    ) -> anyhow::Result<Self> {
        if node.node_identity_did().is_none() {
            anyhow::bail!(
                "Gents runtime requires an EmbeddedNode configured with a node signing DID"
            );
        }
        // Run the AgentBehavior migration before any behavior read so that
        // desktops, embedders, and CLI serve paths all see description/summary
        // even when the DB was created before branch #377. This is idempotent
        // (field-presence-checked) and cheap on already-migrated DBs.
        migration::ensure_agent_behavior_migrations(node.clone()).await?;
        let backend_health = options.backend_health.clone().unwrap_or_default();
        let document_runtime_context = DocumentResolveContext {
            identity: identity.clone(),
            tool_ceiling: options.tool_ceiling.clone(),
            backend_health: backend_health.clone(),
        };
        let resolved_snapshot =
            resolve_document_runtime_snapshot(node.as_ref(), &document_runtime_context).await?;
        debug_assert!(
            resolved_snapshot.principal.is_some(),
            "from_default_behavior_documents called with a snapshot lacking a principal; \
             the production loader always sets principal: Some(...) — a None snapshot \
             means a non-production path bypassed the loader and would produce a \
             Gents.principal that's NOT Arc::ptr_eq to the snapshot's behavior principals",
        );
        let principal = resolved_snapshot.principal.clone().unwrap_or_else(|| {
            let default_behavior_id = resolved_snapshot.default_behavior_id.clone();
            Arc::new(AgentPrincipal {
                agent_did: identity.did().to_string(),
                identity: identity.clone(),
                default_behavior_id,
                display_name: None,
                enabled: true,
            })
        });
        let default_behavior_id = principal.default_behavior_id.clone();
        let mut behaviors = resolved_snapshot
            .behaviors
            .values()
            .cloned()
            .collect::<Vec<_>>();
        behaviors.sort_by(|left, right| {
            let left_is_default = left.behavior_id == default_behavior_id;
            let right_is_default = right.behavior_id == default_behavior_id;
            right_is_default
                .cmp(&left_is_default)
                .then_with(|| left.behavior_id.cmp(&right.behavior_id))
        });

        let rendered_request_capture_factory =
            crate::rendered_request::defra_rendered_request_capture_factory(node.clone());

        Ok(Self {
            node,
            principal,
            behaviors,
            unavailable_behaviors: resolved_snapshot.unavailable_behaviors,
            document_runtime_context: Some(document_runtime_context),
            mcp_pool: options.mcp_pool,
            local_hostname: options
                .local_hostname
                .unwrap_or_else(runtime::default_hostname),
            local_subnet: options.local_subnet,
            retry_policy: options.retry_policy,
            hook_failure_policy: options.hook_failure_policy,
            background_execution_registry: BackgroundExecutionRegistry::default(),
            health_checker_options: options.health_checker_options,
            backend_prober_options: options.backend_prober_options,
            backend_health,
            process_state_observer: options.process_state_observer,
            runtime_snapshot_observer: options.runtime_snapshot_observer,
            startup_readiness: options.startup_readiness,
            // Capture is mandatory (#840): a provider call with no durable
            // rendered input is a log entry, not a fact record. The public
            // builder exposes only a fail-closed fault-injection hook, never an
            // arbitrary sink that could acknowledge without persisting.
            rendered_request_capture_factory: Some(rendered_request_capture_factory),
            manual_trigger_handle: Arc::new(OnceCell::new()),
            operator_tool_root: options.tool_ceiling.root().map(PathBuf::from),
        })
    }

    /// The local prober's measured backend health — the truthful signal
    /// behind effective availability. Exposed for embedders and for the
    /// completion-retry path (#631) to make fail-fast-vs-backoff decisions.
    pub fn backend_health(&self) -> BackendHealthMap {
        self.backend_health.clone()
    }

    pub fn behaviors(&self) -> &[Arc<AgentBehavior>] {
        &self.behaviors
    }

    /// Returns the deployment principal record.
    ///
    /// DefraDB ops issued by this `Gents` are signed by the node identity
    /// configured on its `EmbeddedNode`. That signer may differ from the
    /// deployment principal identity: the node is the durable write author,
    /// while the principal remains the permission boundary for its behaviors.
    pub fn principal(&self) -> &AgentPrincipal {
        &self.principal
    }

    pub(crate) fn principal_arc(&self) -> Arc<AgentPrincipal> {
        Arc::clone(&self.principal)
    }

    pub fn agent_did(&self) -> &str {
        &self.principal.agent_did
    }

    pub fn default_behavior_id(&self) -> &str {
        &self.principal.default_behavior_id
    }

    pub fn unavailable_behaviors(&self) -> &HashMap<String, String> {
        &self.unavailable_behaviors
    }

    pub fn background_execution_registry(&self) -> BackgroundExecutionRegistry {
        self.background_execution_registry.clone()
    }

    pub(crate) fn document_runtime_context(&self) -> Option<&DocumentResolveContext> {
        self.document_runtime_context.as_ref()
    }

    pub(crate) fn operator_tool_root(&self) -> Option<&std::path::Path> {
        self.operator_tool_root.as_deref()
    }

    #[allow(dead_code)]
    pub(crate) fn manual_trigger_handle(&self) -> Option<&ManualTriggerHandle> {
        self.manual_trigger_handle.get()
    }

    pub async fn run(self, shutdown: watch::Receiver<bool>) -> anyhow::Result<()> {
        runtime::run_agent(self, shutdown).await
    }
}

pub(crate) async fn resolve_document_runtime_snapshot(
    node: &EmbeddedNode,
    context: &DocumentResolveContext,
) -> anyhow::Result<ResolvedRuntimeSnapshot> {
    let view = document_view::load_document_runtime_view(node, context.identity.did()).await?;
    document_view::resolve_document_runtime_snapshot_from_view(node, context, &view).await
}

pub(crate) fn behavior_config_from_documents(
    principal: Arc<AgentPrincipal>,
    behavior: &crate::document_config::AgentBehavior,
    backend: &crate::backend_registry::InferenceBackend,
    inference_profile: &crate::document_config::InferenceProfile,
    tool_selection: ToolSelection,
    subagent_tools: SubagentToolConfig,
    tool_ceiling: &ToolCeiling,
    skills: Vec<crate::skills::Skill>,
) -> anyhow::Result<AgentBehavior> {
    let compaction_strategy = parse_compaction_strategy(behavior.compaction_strategy.as_deref())?;
    let stream_batch_ms = inference_profile
        .stream_batch_ms
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(DEFAULT_STREAM_BATCH_MS);
    let deadline_duration_secs = inference_profile
        .deadline_duration_secs
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(DEFAULT_DEADLINE_DURATION_SECS);
    let stream_liveness_timeout_secs = positive_duration_secs_or_default(
        inference_profile.stream_liveness_timeout_secs,
        "stream_liveness_timeout_secs",
        DEFAULT_STREAM_LIVENESS_TIMEOUT_SECS,
    )?
    .min(deadline_duration_secs.max(1));
    let profile_max_tokens = inference_profile
        .max_output_tokens
        .and_then(|value| u64::try_from(value).ok());

    let raw_system_prompt = behavior.system_prompt.clone().unwrap_or_default();
    let rendered_system_prompt = crate::template::render_system_prompt(
        &raw_system_prompt,
        serde_json::json!({
            "node_did": principal.agent_did.as_str(),
            "behavior_id": behavior.behavior_id.as_str(),
        }),
        &crate::template::catalog::default_catalog(),
    )?;
    let openai_wire_api = crate::OpenAiWireApi::effective_for_provider(
        backend.provider_kind,
        backend.openai_wire_api,
        &backend.backend_id,
    );
    let sampling = SamplingConfig {
        temperature: inference_profile.temperature,
        top_p: inference_profile.top_p,
        top_k: inference_profile.top_k,
        seed: inference_profile.seed,
        min_p: inference_profile.min_p,
        frequency_penalty: inference_profile.frequency_penalty,
        presence_penalty: inference_profile.presence_penalty,
        repetition_penalty: inference_profile.repetition_penalty,
        reasoning_effort: inference_profile
            .reasoning_effort
            .as_deref()
            // Older/default Defra rows may materialize nullable strings as an
            // empty value. That is the wire equivalent of an unset profile
            // field, not an invalid reasoning level.
            .filter(|value| !value.trim().is_empty())
            .map(crate::config::ReasoningEffort::parse)
            .transpose()?,
        max_tokens: profile_max_tokens,
    };
    sampling.validate_for_provider(backend.provider_kind, openai_wire_api)?;

    Ok(AgentBehavior {
        behavior_id: behavior.behavior_id.clone(),
        principal,
        backend_id: Some(backend.backend_id.clone()),
        backend_provider_kind: backend.provider_kind,
        openai_wire_api,
        backend_endpoint: backend.endpoint.clone(),
        backend_api_key: backend.api_key.clone(),
        backend_api_key_env_var: backend.api_key_env_var.clone(),
        model_name: normalize_optional_string(behavior.model_name.as_deref())
            .unwrap_or(DEFAULT_MODEL_NAME)
            .to_string(),
        context_window: inference_profile
            .context_window
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(DEFAULT_CONTEXT_WINDOW),
        max_output_tokens: inference_profile
            .max_output_tokens
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(DEFAULT_MAX_OUTPUT_TOKENS),
        max_turns: inference_profile
            .max_turns
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(DEFAULT_MAX_TURNS),
        system_prompt: rendered_system_prompt,
        request_context_template: behavior.request_context_template.clone(),
        tools: BehaviorToolConfig::from_selection_with_subagent_tools(
            &behavior.behavior_id,
            tool_selection,
            tool_ceiling,
            subagent_tools,
            Vec::new(),
        )?,
        compaction_threshold: behavior
            .compaction_threshold
            .unwrap_or(DEFAULT_COMPACTION_THRESHOLD),
        compaction_strategy,
        stream_batch_ms,
        stream_liveness_timeout: Duration::from_secs(stream_liveness_timeout_secs),
        deadline_duration: Duration::from_secs(deadline_duration_secs),
        completion_retry: completion_retry_fields_from_profile(inference_profile),
        sampling,
        skills,
    })
}

fn completion_retry_fields_from_profile(
    inference_profile: &crate::document_config::InferenceProfile,
) -> completion_retry::CompletionRetryProfileFields {
    completion_retry::CompletionRetryProfileFields {
        retry_max_transport: inference_profile.retry_max_transport,
        retry_backoff_ms: inference_profile.retry_backoff_ms.clone(),
        retry_max_resample: inference_profile.retry_max_resample,
        retry_allow_repair: inference_profile.retry_allow_repair,
        retry_interactive_max: inference_profile.retry_interactive_max,
    }
}

fn positive_duration_secs_or_default(
    value: Option<i64>,
    field_name: &str,
    default_secs: u64,
) -> anyhow::Result<u64> {
    match value {
        None => Ok(default_secs),
        Some(value) if value > 0 => u64::try_from(value)
            .map_err(|_| anyhow::anyhow!("{field_name} value {value} is out of range")),
        Some(value) => anyhow::bail!("{field_name} must be positive; got {value}"),
    }
}

fn parse_compaction_strategy(value: Option<&str>) -> anyhow::Result<CompactionStrategy> {
    match normalize_optional_string(value) {
        None => Ok(CompactionStrategy::StripThenSummarize),
        Some("StripToolResults") => Ok(CompactionStrategy::StripToolResults),
        Some("Summarize") => Ok(CompactionStrategy::Summarize),
        Some("StripThenSummarize") => Ok(CompactionStrategy::StripThenSummarize),
        Some(other) => anyhow::bail!("unknown compaction strategy {other}"),
    }
}

fn normalize_optional_string(value: Option<&str>) -> Option<&str> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then_some(trimmed)
    })
}

pub(crate) fn tool_selection_from_document(
    selection: &crate::document_config::ToolSelectionDocument,
) -> anyhow::Result<ToolSelection> {
    ToolSelection::from_document(selection)
}

pub(crate) fn subagent_tool_config_from_document(
    selection: &crate::document_config::ToolSelectionDocument,
) -> SubagentToolConfig {
    SubagentToolConfig::from_document(selection)
}
