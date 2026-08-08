use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::{Duration, Instant};

use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
use tokio::sync::{mpsc, watch, Notify};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::compaction::CompactionStrategy;
use crate::config::{AgentBehavior, SamplingConfig};
use crate::document_config::{
    list_event_trigger_records, list_schedule_records, load_schedule_next_run_at,
};
use crate::ensure_runtime_schemas;
use crate::graphql::escape_graphql_string;
use crate::identity::{AgentIdentity, AgentPrincipal, KeyIdentity};
use crate::lean_vocab_test::{
    assert_lean_to_defradb_vocabulary_matches, lean_trigger_dispatch_case_count,
    lean_trigger_dispatch_cases, LeanTriggerDispatchCase, LeanTriggerKeyContract, LeanVocabulary,
};
use crate::runtime_snapshot::{
    ActiveRuntimeSnapshot, ConcurrencyMode, ResolvedEventTrigger, ResolvedRuntimeSnapshot,
    ResolvedSchedule, ResolvedTask, ScheduleCadence,
};
use crate::tool_surface::BehaviorToolConfig;
use crate::trigger_engine::event_source::EventSource;
use crate::trigger_engine::manual_source::ManualSource;
use crate::trigger_engine::production_materializer::{
    execution_origin_for_trigger_kind, ProductionMaterializer,
};
use crate::trigger_engine::schedule_source::ScheduleSource;
use crate::BackendProviderKind;

/// Recorded `materialize` invocation: `(trigger_id, trigger_kind, rendered_prompt)`.
type MaterializeCall = (Option<String>, TriggerKind, String);

/// Recorded `supersede` invocation: `(trigger_id, trigger_kind)`.
type SupersedeCall = (String, TriggerKind);

type NonterminalRequests = Arc<Mutex<HashMap<(String, TriggerKind), Vec<String>>>>;

/// Build a minimal `Arc<AgentPrincipal>` for tests that need to satisfy the
/// principal invariant enforced by `ResolvedRuntimeSnapshot::activate`'s
/// `debug_assert!`. Does not exercise signing.
fn stub_principal() -> Arc<crate::identity::AgentPrincipal> {
    let identity: Arc<dyn crate::identity::AgentIdentity> = Arc::new(
        KeyIdentity::load_or_create(
            std::env::temp_dir().join(format!("stub-principal-{}.key", uuid::Uuid::new_v4())),
            None,
        )
        .unwrap(),
    );
    Arc::new(crate::identity::AgentPrincipal {
        agent_did: identity.did().to_string(),
        identity,
        default_behavior_id: String::new(),
        display_name: None,
        enabled: true,
    })
}

async fn signed_test_node(name: &str) -> Arc<defra_node::EmbeddedNode> {
    let identity = KeyIdentity::load_or_create(
        std::env::temp_dir().join(format!("{name}-{}.key", uuid::Uuid::new_v4())),
        None,
    )
    .unwrap();
    Arc::new(
        defra_node::EmbeddedNode::builder()
            .with_node_identity_did(identity.did())
            .build()
            .await
            .unwrap(),
    )
}

const LEAN_TRIGGER_TYPES_MODEL: &str = include_str!("../../../proofs/Proofs/Triggers/Types.lean");
const LEAN_TRIGGER_TYPES_FILE: &str = "crates/gents/proofs/Proofs/Triggers/Types.lean";

#[test]
fn rust_trigger_kind_vocabulary_matches_lean_model() {
    let rust_kinds = vec![
        TriggerKind::Schedule.as_str(),
        TriggerKind::Event.as_str(),
        TriggerKind::Manual.as_str(),
    ];
    assert_lean_to_defradb_vocabulary_matches(LeanVocabulary {
        lean_file: LEAN_TRIGGER_TYPES_FILE,
        model: LEAN_TRIGGER_TYPES_MODEL,
        namespace: "TriggerKind",
        rust_source: "TriggerKind::{Schedule, Event, Manual}",
        rust_values: &rust_kinds,
    });
}

#[derive(Clone)]
struct MaterializeGate {
    entered_tx: mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
}

/// Spy `MaterializerHandle` used by the engine tests. Records every
/// `materialize` call it sees and hands back sequentially-numbered request ids
/// so assertions can check both the call count and the rendered prompt that
/// reached the materializer.
///
/// `nonterminal_for` stores the concrete request ids for `(trigger_id,
/// trigger_kind)` tuples that `has_active_runtime_request_for_trigger` should
/// report as in-flight. Tests can pre-populate it to simulate prior fires.
/// Lean contract tests can opt into adding successful materializations as new
/// non-terminal requests, which mirrors production persistence without
/// changing the default spy behavior expected by local unit tests.
///
/// `materialize_delay` optionally pauses inside `materialize` before recording
/// the call. Used by the `LatestOnly` serialization tests to widen the window
/// during which the per-trigger lock is held so parallel fires can be observed
/// to queue.
struct SpyMaterializer {
    materialize_calls: Arc<Mutex<Vec<MaterializeCall>>>,
    next_request_id: AtomicUsize,
    nonterminal_for: NonterminalRequests,
    /// DIDs the engine passed to the concurrency gate / supersede, in call
    /// order. The gate's DID scope is the subject of #605.
    gate_dids: Arc<Mutex<Vec<String>>>,
    supersede_dids: Arc<Mutex<Vec<String>>>,
    supersede_calls: Arc<Mutex<Vec<SupersedeCall>>>,
    superseded_request_ids: Arc<Mutex<Vec<String>>>,
    materialize_delay: Mutex<Option<Duration>>,
    materialize_gate: Mutex<Option<MaterializeGate>>,
    track_materialized_nonterminal: AtomicBool,
}

impl SpyMaterializer {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            materialize_calls: Arc::new(Mutex::new(Vec::new())),
            next_request_id: AtomicUsize::new(0),
            nonterminal_for: Arc::new(Mutex::new(HashMap::new())),
            gate_dids: Arc::new(Mutex::new(Vec::new())),
            supersede_dids: Arc::new(Mutex::new(Vec::new())),
            supersede_calls: Arc::new(Mutex::new(Vec::new())),
            superseded_request_ids: Arc::new(Mutex::new(Vec::new())),
            materialize_delay: Mutex::new(None),
            materialize_gate: Mutex::new(None),
            track_materialized_nonterminal: AtomicBool::new(false),
        })
    }

    fn calls(&self) -> Vec<MaterializeCall> {
        self.materialize_calls.lock().unwrap().clone()
    }

    fn supersede_calls(&self) -> Vec<SupersedeCall> {
        self.supersede_calls.lock().unwrap().clone()
    }

    fn gate_dids(&self) -> Vec<String> {
        self.gate_dids.lock().unwrap().clone()
    }

    fn supersede_dids(&self) -> Vec<String> {
        self.supersede_dids.lock().unwrap().clone()
    }

    fn superseded_request_ids(&self) -> Vec<String> {
        self.superseded_request_ids.lock().unwrap().clone()
    }

    fn nonterminal_count_for(&self, trigger_id: &str, trigger_kind: TriggerKind) -> usize {
        self.nonterminal_for
            .lock()
            .unwrap()
            .get(&(trigger_id.to_owned(), trigger_kind))
            .map(Vec::len)
            .unwrap_or(0)
    }

    /// Pre-populate the in-flight set with `(trigger_id, trigger_kind)` so the
    /// next `has_active_runtime_request_for_trigger` call returns `true` for the
    /// matching tuple. Also makes `supersede_active_runtime_requests_for_trigger`
    /// report the tuple count (and clears it, mirroring real terminal
    /// transitions) so LatestOnly tests can assert the count plumbed through.
    fn mark_nonterminal(&self, trigger_id: &str, trigger_kind: TriggerKind) {
        let next_index = self.nonterminal_count_for(trigger_id, trigger_kind);
        let request_id = format!("spy-prior-{}-{next_index}", trigger_kind.as_str());
        self.mark_nonterminal_request(trigger_id, trigger_kind, request_id);
    }

    fn mark_nonterminal_request(
        &self,
        trigger_id: &str,
        trigger_kind: TriggerKind,
        request_id: impl Into<String>,
    ) {
        self.nonterminal_for
            .lock()
            .unwrap()
            .entry((trigger_id.to_owned(), trigger_kind))
            .or_default()
            .push(request_id.into());
    }

    /// Make successful materializations increment the in-flight tuple count.
    /// The Lean-generated conformance cases use this to compare post-dispatch
    /// non-terminal counts; ordinary unit tests keep the older explicit
    /// `mark_nonterminal` behavior.
    fn track_materialized_nonterminal(&self) {
        self.track_materialized_nonterminal
            .store(true, Ordering::SeqCst);
    }

    /// Install a delay that `materialize` will sleep for before recording its
    /// call. Used to widen the critical section so parallel `LatestOnly`
    /// dispatches can be observed to serialize on the per-trigger lock.
    fn set_materialize_delay(&self, delay: Duration) {
        *self.materialize_delay.lock().unwrap() = Some(delay);
    }

    /// Block materialization until `release` is notified, sending one message
    /// on `entered_tx` each time a materialize call reaches the gate.
    fn set_materialize_gate(&self, entered_tx: mpsc::UnboundedSender<()>, release: Arc<Notify>) {
        *self.materialize_gate.lock().unwrap() = Some(MaterializeGate {
            entered_tx,
            release,
        });
    }
}

impl MaterializerHandle for SpyMaterializer {
    fn materialize(
        &self,
        _task: &ResolvedTask,
        trigger_id: Option<&str>,
        trigger_kind: TriggerKind,
        rendered_prompt: &str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<String>> + Send + '_>> {
        let entry = (
            trigger_id.map(str::to_owned),
            trigger_kind,
            rendered_prompt.to_owned(),
        );
        let calls = self.materialize_calls.clone();
        let nonterminal_for = self.nonterminal_for.clone();
        let nonterminal_key = trigger_id.map(|id| (id.to_owned(), trigger_kind));
        let track_materialized_nonterminal =
            self.track_materialized_nonterminal.load(Ordering::SeqCst);
        let id = self.next_request_id.fetch_add(1, Ordering::SeqCst);
        let request_id = format!("req-{id}");
        let delay = *self.materialize_delay.lock().unwrap();
        let gate = self.materialize_gate.lock().unwrap().clone();
        Box::pin(async move {
            if let Some(gate) = gate {
                let _ = gate.entered_tx.send(());
                gate.release.notified().await;
            }
            if let Some(d) = delay {
                tokio::time::sleep(d).await;
            }
            calls.lock().unwrap().push(entry);
            if let (true, Some(key)) = (track_materialized_nonterminal, nonterminal_key) {
                nonterminal_for
                    .lock()
                    .unwrap()
                    .entry(key)
                    .or_default()
                    .push(request_id.clone());
            }
            Ok(request_id)
        })
    }

    fn has_active_runtime_request_for_trigger(
        &self,
        agent_did: &str,
        trigger_id: &str,
        trigger_kind: TriggerKind,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + '_>> {
        let set = self.nonterminal_for.clone();
        let gate_dids = self.gate_dids.clone();
        let agent_did = agent_did.to_owned();
        let key = (trigger_id.to_owned(), trigger_kind);
        Box::pin(async move {
            gate_dids.lock().unwrap().push(agent_did);
            Ok(set
                .lock()
                .unwrap()
                .get(&key)
                .map(|request_ids| !request_ids.is_empty())
                .unwrap_or(false))
        })
    }

    fn supersede_active_runtime_requests_for_trigger(
        &self,
        agent_did: &str,
        trigger_id: &str,
        trigger_kind: TriggerKind,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<usize>> + Send + '_>> {
        let nonterm = self.nonterminal_for.clone();
        let supersede_calls = self.supersede_calls.clone();
        let supersede_dids = self.supersede_dids.clone();
        let superseded_request_ids = self.superseded_request_ids.clone();
        let agent_did = agent_did.to_owned();
        let key = (trigger_id.to_owned(), trigger_kind);
        Box::pin(async move {
            supersede_dids.lock().unwrap().push(agent_did);
            supersede_calls.lock().unwrap().push(key.clone());
            // Mirror a real terminal transition: the tuple is no longer
            // in-flight after supersede.
            let removed = nonterm.lock().unwrap().remove(&key).unwrap_or_default();
            let count = removed.len();
            superseded_request_ids.lock().unwrap().extend(removed);
            Ok(count)
        })
    }
}

/// Build an `ActiveRuntimeSnapshot` with the supplied active schedules and no
/// other live state. Matches the empty-defaults pattern used by
/// `runtime_snapshot::tests`.
fn snapshot_with_schedules(
    schedules: HashMap<String, ResolvedSchedule>,
) -> Arc<ActiveRuntimeSnapshot> {
    // The "general" behavior must resolve: the concurrency gate scopes
    // serial/latestOnly coordination by the behavior's agent DID (#605).
    let resolved = ResolvedRuntimeSnapshot::from_parts_with_admission_configs(
        "general".to_string(),
        vec![integration_test_behavior("general")],
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
    )
    .with_schedules(schedules, HashSet::new())
    .with_principal(stub_principal());
    Arc::new(resolved.activate(1, HashMap::new()))
}

fn resolved_task(prompt_template: &str) -> ResolvedTask {
    ResolvedTask {
        task_id: "t1".to_string(),
        name: None,
        behavior_id: "general".to_string(),
        prompt_template: prompt_template.to_string(),
        output_schema_ref: None,
    }
}

fn resolved_schedule(schedule_id: &str, task: ResolvedTask) -> ResolvedSchedule {
    ResolvedSchedule {
        schedule_id: schedule_id.to_string(),
        task_id: task.task_id.clone(),
        task,
        cadence: ScheduleCadence::Interval { interval_secs: 60 },
        enabled: true,
        concurrency: ConcurrencyMode::Serial,
    }
}

fn resolved_schedule_with_concurrency(
    schedule_id: &str,
    task: ResolvedTask,
    concurrency: ConcurrencyMode,
) -> ResolvedSchedule {
    ResolvedSchedule {
        schedule_id: schedule_id.to_string(),
        task_id: task.task_id.clone(),
        task,
        cadence: ScheduleCadence::Interval { interval_secs: 60 },
        enabled: true,
        concurrency,
    }
}

fn resolved_event_trigger_with_concurrency(
    trigger_id: &str,
    task: ResolvedTask,
    concurrency: ConcurrencyMode,
) -> ResolvedEventTrigger {
    ResolvedEventTrigger {
        trigger_id: trigger_id.to_string(),
        task_id: task.task_id.clone(),
        task,
        source_collection: "WebhookEvent".to_string(),
        event_kind: "created".to_string(),
        filter: None,
        enabled: true,
        concurrency,
    }
}

fn trigger_kind_from_lean(value: &str) -> TriggerKind {
    match value {
        "schedule" => TriggerKind::Schedule,
        "event" => TriggerKind::Event,
        "manual" => TriggerKind::Manual,
        other => panic!("unknown Lean trigger kind {other:?}"),
    }
}

fn concurrency_from_lean(value: &str) -> ConcurrencyMode {
    ConcurrencyMode::parse(value)
        .unwrap_or_else(|| panic!("unknown Lean concurrency mode {value:?}"))
}

fn trigger_key_from_lean(key: &LeanTriggerKeyContract) -> (String, TriggerKind) {
    (
        key.trigger_id.clone(),
        trigger_kind_from_lean(&key.trigger_kind),
    )
}

fn snapshot_from_trigger_contract(
    case: &LeanTriggerDispatchCase,
    task: &ResolvedTask,
    concurrency: ConcurrencyMode,
) -> Arc<ActiveRuntimeSnapshot> {
    let active_schedules = case
        .active_schedule_ids
        .iter()
        .map(|id| {
            (
                id.clone(),
                resolved_schedule_with_concurrency(id, task.clone(), concurrency),
            )
        })
        .collect();
    let active_event_triggers = case
        .active_event_trigger_ids
        .iter()
        .map(|id| {
            (
                id.clone(),
                resolved_event_trigger_with_concurrency(id, task.clone(), concurrency),
            )
        })
        .collect();
    let resolved = ResolvedRuntimeSnapshot::from_parts_with_admission_configs(
        "general".to_string(),
        vec![integration_test_behavior("general")],
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
    )
    .with_schedules(active_schedules, HashSet::new())
    .with_event_triggers(active_event_triggers, HashSet::new())
    .with_principal(stub_principal());
    Arc::new(resolved.activate(1, HashMap::new()))
}

/// Build a minimal `AgentBehavior` suitable for the production materializer
/// integration test. The behavior has a backend binding (required — the
/// materializer rejects tasks whose behavior is not backend-bound) but does
/// not drive any inference: the integration test asserts lineage on the
/// persisted `AgentRequest` doc only, not execution.
fn integration_test_behavior(behavior_name: &str) -> Arc<AgentBehavior> {
    let identity: Arc<dyn crate::identity::AgentIdentity> = Arc::new(
        KeyIdentity::load_or_create(
            std::env::temp_dir().join(format!("{behavior_name}-{}.key", uuid::Uuid::new_v4())),
            None,
        )
        .unwrap(),
    );
    let principal = Arc::new(AgentPrincipal {
        agent_did: identity.did().to_string(),
        identity,
        default_behavior_id: String::new(),
        display_name: None,
        enabled: true,
    });
    Arc::new(AgentBehavior {
        skills: Vec::new(),
        behavior_id: behavior_name.to_string(),
        principal,
        backend_id: Some("backend-it".to_string()),
        backend_provider_kind: BackendProviderKind::OpenAiCompatible,
        openai_wire_api: crate::OpenAiWireApi::ChatCompletions,
        backend_endpoint: "http://localhost:0/v1".to_string(),
        backend_api_key: None,
        backend_api_key_env_var: None,
        model_name: crate::config::DEFAULT_MODEL_NAME.to_string(),
        context_window: crate::config::DEFAULT_CONTEXT_WINDOW,
        max_output_tokens: crate::config::DEFAULT_MAX_OUTPUT_TOKENS,
        max_turns: crate::config::DEFAULT_MAX_TURNS,
        system_prompt: String::new(),
        request_context_template: None,
        tools: BehaviorToolConfig::default(),
        compaction_threshold: crate::config::DEFAULT_COMPACTION_THRESHOLD,
        compaction_strategy: CompactionStrategy::StripThenSummarize,
        stream_batch_ms: crate::config::DEFAULT_STREAM_BATCH_MS,
        stream_liveness_timeout: Duration::from_secs(
            crate::config::DEFAULT_STREAM_LIVENESS_TIMEOUT_SECS,
        ),
        deadline_duration: Duration::from_secs(crate::config::DEFAULT_DEADLINE_DURATION_SECS),
        completion_retry: crate::agent::completion_retry::CompletionRetryProfileFields::default(),
        sampling: SamplingConfig::default(),
    })
}

/// Build an `ActiveRuntimeSnapshot` containing the given behavior as loaded
/// and the supplied active schedules. Used by the integration test below to
/// hand the ProductionMaterializer a snapshot where `behavior_id` resolution
/// succeeds.
fn snapshot_with_behavior_and_schedules(
    behavior: Arc<AgentBehavior>,
    schedules: HashMap<String, ResolvedSchedule>,
) -> Arc<ActiveRuntimeSnapshot> {
    let resolved = ResolvedRuntimeSnapshot::from_parts_with_admission_configs(
        behavior.behavior_id.clone(),
        vec![behavior],
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
    )
    .with_schedules(schedules, HashSet::new())
    .with_principal(stub_principal());
    Arc::new(resolved.activate(1, HashMap::new()))
}

mod dispatch;
mod dispatch_contract;
mod event_source;
mod manual_source;
mod schedule_source;

#[tokio::test]
async fn trigger_engine_dispatch_matches_lean_generated_contract_cases() {
    dispatch_contract::trigger_engine_dispatch_matches_lean_generated_contract_cases().await;
}
