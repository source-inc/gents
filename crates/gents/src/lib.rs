//! Core DefraDB-backed agent runtime for Gents.
//!
//! This crate owns request execution, lifecycle enforcement, persistence,
//! identity, tools, networking, and provider-input assembly.

pub mod adapter_projection;
pub(crate) mod admission;
pub mod agent;
pub mod apply_model;
pub mod backend_health;
pub mod backend_provider;
pub mod backend_registry;
pub mod background_completion;
mod background_completion_diagnostics;
pub(crate) mod background_tools;
pub mod chatgpt_codex;
pub mod chatgpt_oauth_refresh;
pub mod codex_shim_binding;
pub mod collection;
pub mod compaction;
pub(crate) mod completion_factory;
pub mod config;
pub mod config_client;
pub mod defra_query;
pub mod defra_write;
pub mod descendant_graph;
pub mod desired_fields;
pub mod document_config;
pub mod error;
pub mod event_delivery_contract;
pub mod external_adapter_capture;
pub mod goal;
pub mod graphql;
pub mod health_checker;
pub mod hook;
pub mod identity;
pub mod inference_http;
pub mod interrupt;
#[cfg(test)]
pub(crate) mod lean_vocab_test;
pub mod oauth_credential;
pub mod openai_wire;
pub mod p2p_observability;
pub(crate) mod provider_usage;
pub mod startup_readiness;
pub mod startup_recovery;
pub mod storage_backend;
pub mod xai_grok_oauth;
pub mod xai_oauth_login;
pub mod xai_oauth_refresh;

/// Shared in-crate test utilities.
#[cfg(test)]
pub(crate) mod test_support {
    /// `OneOrMany::first_ref` stand-in for native `Vec` content: non-empty by
    /// convention in every shape the tests build.
    pub(crate) fn first_content<T>(items: &[T]) -> &T {
        items.first().expect("non-empty content")
    }

    /// The #589 production poison, byte-faithful to Amy's persisted
    /// `AgentToolCall` row `Rrt-HmhWfFSmkh1HSUmHt`: a model tool-call
    /// `arguments` string contaminated by out-of-channel tokens — a stray CJK
    /// `房` and a leaked `</think` reasoning boundary inside a key, a nested
    /// Hermes `<tool_call>`/`<function=...>` fragment as its value, duplicated
    /// keys, and LITERAL newlines inside the strings (the control characters
    /// `serde_json` rejects at "line 2 column 0"). The intended call survives
    /// as the final `tool_name: list_hosts`.
    pub(crate) const CORRUPT_TOOL_ARGS_589: &str = "{\"raw_schema\": false, \
         \"service_id\": \"observability-mcp\", \"tool房\n</think\": \"\n<tool_call>\n\
         <function=describe_tool>\", \"raw_schema\": false, \
         \"service_id\": \"observability-mcp\", \"tool_name\": \"list_hosts\"}";
}
pub mod lifecycle;
pub mod llm;
pub mod log_rate;
pub(crate) mod managed_exec;
pub mod mcp_pool;
pub mod meta_tools;
pub mod migration;
pub mod native_executor_status;
pub mod oneshot;
pub mod periodic_recovery;
pub mod prompt;
pub mod provider_context_reduction;
pub(crate) mod registry;
pub mod rendered_request;
pub(crate) mod request_binding;
pub mod retry;
pub mod run_timeline;
pub mod run_timeline_fetch;
pub(crate) mod runtime_snapshot;
pub(crate) mod runtime_status;
pub(crate) mod runtime_trace;
pub mod schedule_cron;
pub mod schema;
pub mod self_config;
pub mod session;
pub mod skills;
pub mod streaming;
pub mod template;
pub mod tool_call_lifecycle;
pub mod tool_control;
pub mod tool_surface;
pub mod toolset;
pub mod trace_export;
pub(crate) mod trigger_engine;
pub mod truncation;
pub mod watcher;
pub(crate) mod workspace;

pub use collection::Collection;

pub use adapter_projection::{
    adapter_projection_eval_jsonl_record_schema, adapter_projection_eval_jsonl_records,
    adapter_projection_json_schema, adapter_projection_jsonl_record_schema,
    adapter_projection_jsonl_records, adapter_projection_native_json,
    adapter_projection_native_json_schema, build_adapter_projection,
    validate_adapter_projection_contract, AdapterProjection, AdapterProjectionContractError,
    AdapterProjectionEnvelope, AdapterProjectionEvalJsonlRecord, AdapterProjectionJsonlRecord,
    AdapterProjectionKind, AtifAgent, AtifFinalMetrics, AtifObservation, AtifObservationResult,
    AtifStep, AtifStepSource, AtifToolCall, AtifTrajectory, ProjectionContext,
    ProjectionRedactionMode, ATIF_SCHEMA_VERSION,
};
pub use admission::BackendAdmissionConfig;
pub use admission::{InferenceCall, InferenceCallRecoveryReport};
pub use agent::{
    BehaviorBuilder, DocumentRuntimeOptions, Gents, GentsBuilder, ProcessLifecycleObserver,
    ProcessLifecycleState, RuntimeSnapshotObserver,
};
pub use backend_health::{
    probe_backends_cycle, run_backend_probe_cycle, spawn_backend_prober, BackendHealthMap,
    BackendHealthSnapshot, BackendHealthState, BackendProberOptions, ProbeCycleOutcome,
};
pub use backend_provider::{discover_models as discover_backend_models, BackendProviderKind};
pub use backend_registry::{InferenceBackend, HEALTHY_PROBE_STATUS, UNKNOWN_PROBE_STATUS};
pub use background_completion_diagnostics::{
    load_background_completion_diagnostics, BackgroundCompletionDiagnostics,
    BackgroundCompletionEpochDiagnostic,
};
pub use compaction::CompactionStrategy;
pub use config::{
    AgentBehavior, ReasoningEffort, SamplingConfig, DEFAULT_COMPACTION_THRESHOLD,
    DEFAULT_CONTEXT_WINDOW, DEFAULT_DEADLINE_DURATION_SECS, DEFAULT_MAX_OUTPUT_TOKENS,
    DEFAULT_MAX_TURNS, DEFAULT_MODEL_NAME, DEFAULT_STREAM_BATCH_MS,
    DEFAULT_STREAM_LIVENESS_TIMEOUT_SECS,
};
pub use config_client::ConfigAccess;
pub use defra_node;
pub use descendant_graph::{
    resolve_descendant_edge, resolve_descendant_graph, resolve_descendant_root_request_id,
    DescendantAuthorizationState, DescendantControlAuthority, DescendantEdge,
    DescendantGraphAccess, DescendantMaterializationState, DescendantPage, DescendantQuery,
    DescendantScope, MAX_DESCENDANT_PAGE_LIMIT,
};
pub use desired_fields::{DesiredFields, LiveFields};
pub use document_config::{
    default_behavior_id_for_agent, default_inference_profile_id_for_behavior,
    default_tool_selection_id_for_behavior, deserialize_dual_shape, ensure_agent_principal,
    is_reserved_builtin_tool_name, list_agent_behaviors, list_inference_profile_records,
    load_agent_behavior, load_agent_principal, load_inference_profile, load_tool_selection,
    subagent_target_entry, upsert_agent_behavior, upsert_agent_principal, upsert_inference_profile,
    upsert_tool_selection, wide_open_tool_selection_document,
    wide_open_tool_selection_id_for_agent, AgentBehavior as AgentBehaviorDocument,
    InferenceProfile, PrincipalBootstrap, QueryToolDecl, SubagentTarget, SurfaceToolDecl,
    ToolSelectionDocument, WriteToolDecl, WriteToolField, WriteToolFieldFill,
    WriteToolOutputObligation, WriteToolOutputObligationScope,
};
pub use external_adapter_capture::{
    import_external_adapter_capture_to_timeline_rows, ExternalAdapterCapture,
    ExternalAdapterImport, ExternalAdapterMapping, ExternalAdapterSource,
};
pub use gents_protocol::client_protocol;
pub use health_checker::{
    run_health_check_cycle, spawn_health_checker, HealthCheckerOptions, HealthPersistenceContext,
    HealthStatus, MCPServiceHealthSnapshot, McpHealthCheckService, ServiceHealth, ServiceHealthMap,
};
pub use hook::{
    BackgroundExecutionRegistry, BackgroundToolRegistry, DefraSessionHook, FailurePolicy, HookStats,
};
pub use identity::{
    load_macos_keychain_identity, load_macos_secure_enclave_identity,
    load_or_create_macos_keychain_identity, load_or_create_macos_secure_enclave_identity,
    AgentIdentity, AgentPrincipal, KeyIdentity, RegisteredIdentity, ServiceAccount,
};
pub use interrupt::{fetch_interrupt_requested_at, interrupt_request};
pub use lifecycle::{
    background_wake_next_retry_at, background_wake_retry_delay, task_run_conversation_title,
    write_manual_agent_request, write_manual_agent_request_with_conversation_title,
    BackgroundWakeRedriveReport, RecoveryReport, RequestLifecycle, TerminalRedriveReport,
    TerminalRepairReport, TERMINAL_REDRIVE_BATCH_LIMIT, TERMINAL_REDRIVE_CAP,
};
pub use mcp_pool::McpPool;
pub use meta_tools::build_meta_tools;
pub use native_executor_status::{active_native_executors, NativeExecutorStatus};
pub use oneshot::{run_openai_oneshot, run_openai_oneshot_with_tools, OneshotRunResult};
pub use openai_wire::OpenAiWireApi;
pub use p2p_observability::{
    JsonP2pSyncStatusAdapter, P2pPeerBacklogSnapshot, P2pPushBacklogSnapshot,
    P2pPushRetryMarkerSnapshot, P2pRequestDispatchSnapshot, P2pSyncStatusAdapter,
    P2pSyncStatusSnapshot,
};
pub use periodic_recovery::{
    periodic_recovery_sweep_metadata, run_periodic_recovery_sweeps, PeriodicRecoverySweepMetadata,
    PeriodicRecoverySweepOutcome, PeriodicRecoverySweepRun,
};
pub use prompt::{LayeredPromptBuilder, PromptBuilder};
pub use run_timeline::{
    build_run_timeline, RetrySummary, RunTimeline, RunTimelineEvent, RunTimelineRows,
    TimelineConversationRow, TimelineGoalParentState, TimelineGoalState,
    TimelineGoalTransitionEvent, TimelineGoalVersionRow, TimelineInferenceCallRow,
    TimelineMessageRow, TimelineRequestRow, TimelineResponseRow, TimelineSessionRow,
    TimelineToolCallRow,
};
pub use runtime_snapshot::{
    ActiveRuntimeSnapshot, ConcurrencyMode, DispatcherMap, EventTriggerFireMode,
    ResolvedEventTrigger, ResolvedSchedule, ResolvedTask, ScheduleCadence,
    MAX_EVENT_TRIGGER_GROUP_DOCS,
};
#[cfg(feature = "agent-memory")]
pub use schema::AGENT_MEMORY_SCHEMA;
pub use schema::{
    ensure_config_bootstrap_schemas, ensure_runtime_schemas, ensure_schemas, AGENT_BEHAVIOR_SCHEMA,
    AGENT_CONVERSATION_SCHEMA, AGENT_MESSAGE_SCHEMA, AGENT_PRINCIPAL_SCHEMA, AGENT_REQUEST_SCHEMA,
    AGENT_RESPONSE_SCHEMA, AGENT_RUNTIME_SCHEMA, AGENT_SESSION_SCHEMA, AGENT_TOOL_CALL_SCHEMA,
    AGENT_TOOL_RESULT_SCHEMA, COMPACTION_ENTRY_SCHEMA, GOAL_SCHEMA, INFERENCE_BACKEND_SCHEMA,
    INFERENCE_CALL_SCHEMA, INFERENCE_PROFILE_SCHEMA, OAUTH_CREDENTIAL_SCHEMA, SCHEDULE_SCHEMA,
    TASK_SCHEMA, TOOL_SELECTION_SCHEMA, TOOL_SERVICE_HEALTH_STATE_SCHEMA,
    TOOL_SERVICE_REGISTRY_SCHEMA,
};
pub use session::load_history;
pub use session::{
    fork, fork_via_http, ForkError, ForkOutcome, ForkParams, GraphqlExecuteResponse,
    GraphqlExecutor, HttpGraphqlExecutor,
};
pub use streaming::{DefraStreamWriter, StreamWriter};
pub use template::{
    parse_template_for_validation, render_template, TemplateError, TemplateScope, VariableRef,
};
pub use tool_control::{cancel_background_tool_call, CancelBackgroundToolCallOutcome};
pub use tool_surface::{
    cli_tool, BashMode, BehaviorToolConfig, CustomToolFactory, FileToolMode, ToolCeiling,
    ToolPolicyVersion, ToolRuntimeContext, ToolSelection, ToolSurface, TOOL_POLICY_V1,
};
pub use toolset::{
    build_native_tools, enable_self_runner, CliToolConfig, CommandExecutionMode,
    CommandExecutionPolicy, CommandNetworkMode, NativeTool, ToolSet, ToolSetBuilder,
};
pub use trigger_engine::event_source::EventSource;
pub use trigger_engine::goal_source::GoalSource;
pub use trigger_engine::subagent_source::SubagentSource;
pub use trigger_engine::subscription_source::UpdateSubscriptionSource;
pub use trigger_engine::{FireIntent, FireResult, TriggerKind, TriggerSource};
pub use truncation::{DefraSpillTruncator, TruncationLimits, TruncationMode, Truncator};
pub use watcher::{AgentRequest, DefraWatcher, Watcher};

#[doc(hidden)]
pub mod __test_internals {
    pub use crate::agent::principal_assembly::{
        assemble_principal_and_behaviors, BehaviorBuildError,
    };
    pub use crate::background_tools::r4c_args::{
        ListSubagentsArgs, ListSubagentsEntry, ListSubagentsResponse, ReadSubagentArgs,
        ReadSubagentResponse,
    };
    pub use crate::background_tools::{
        handle_list_subagents, handle_read_subagent, load_steer_subagent_target, ChildEdge,
        SteerSubagentTarget, AWAITING_CHILD_MATERIALIZATION,
    };
    pub use crate::lifecycle::materialize::EnqueuedAgentRequest;
    pub use crate::lifecycle::queue::{
        drain_automated_wakeups, reconcile_coalesced_pending_request, QueueSource,
    };
    pub use crate::trigger_engine::run_subagent_source_for_test;
}

#[cfg(test)]
mod public_api_tests {
    use super::*;

    #[test]
    fn downstream_oneshot_analysis_surface_is_available_from_crate_root() {
        let _strategy = CompactionStrategy::StripThenSummarize;
        let _ensure = ensure_schemas;
        let _history = load_history;
        let _oneshot = run_openai_oneshot_with_tools;
    }
}
