use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crate::llm::tool::ToolDyn;
use crate::llm::{HookAction, ToolCallHookAction};
use chrono::{DateTime, Utc};
use defra_node::EmbeddedNode;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::background_tools::LiveToolOutputRegistry;
use crate::session;
use crate::tool_call_lifecycle::{
    AwaitMode, CancelCause, CascadeDispatch, ChildTerminal, ToolCallLifecycle,
};
use crate::truncation::TruncationLimits;

pub(crate) mod persistence;
#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FailurePolicy {
    FailOpen,
    #[default]
    FailClosed,
}

#[derive(Debug)]
pub struct HookStats {
    pub persistence_failures: u64,
    pub persistence_successes: u64,
}

struct HookCounters {
    failures: AtomicU64,
    successes: AtomicU64,
}

#[derive(Clone, Default)]
pub struct BackgroundToolRegistry {
    inner: Arc<BackgroundToolRegistryInner>,
}

#[derive(Default)]
struct BackgroundToolRegistryInner {
    tools: HashMap<String, Arc<dyn ToolDyn>>,
    allowlist: Vec<String>,
}

impl BackgroundToolRegistry {
    pub fn from_tools(tools: Vec<Box<dyn ToolDyn>>, allowlist: &[String]) -> Self {
        let allowed = allowlist
            .iter()
            .map(|name| name.trim())
            .filter(|name| !name.is_empty())
            .map(str::to_string)
            .collect::<HashSet<_>>();
        let mut registry_tools = HashMap::new();
        for tool in tools {
            let name = tool.name();
            if allowed.contains(&name) {
                registry_tools.insert(name, Arc::from(tool));
            }
        }
        let mut allowlist = allowed.into_iter().collect::<Vec<_>>();
        allowlist.sort();
        Self {
            inner: Arc::new(BackgroundToolRegistryInner {
                tools: registry_tools,
                allowlist,
            }),
        }
    }

    pub(crate) fn get(&self, tool_name: &str) -> Option<Arc<dyn ToolDyn>> {
        self.inner.tools.get(tool_name).cloned()
    }

    pub(crate) fn allowlist(&self) -> Vec<String> {
        self.inner.allowlist.clone()
    }
}

#[derive(Clone)]
struct BackgroundExecution {
    cancellation_token: CancellationToken,
}

#[derive(Clone, Default)]
struct BackgroundLiveOutputState {
    registry: LiveToolOutputRegistry,
    row_targets: Arc<Mutex<HashMap<String, LiveOutputRowTarget>>>,
    flushed_seq: Arc<Mutex<HashMap<String, i64>>>,
    flusher_running: Arc<AtomicBool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveOutputRowTarget {
    doc_id: String,
    request_id: String,
    session_id: String,
    agent_did: String,
}

impl LiveOutputRowTarget {
    fn from_lifecycle(lifecycle: &ToolCallLifecycle) -> anyhow::Result<Self> {
        let doc_id = lifecycle.doc_id().ok_or_else(|| {
            anyhow::anyhow!("live output requested before AgentToolCall persisted")
        })?;
        Ok(Self {
            doc_id: doc_id.to_string(),
            request_id: lifecycle.request_id().to_string(),
            session_id: lifecycle.session_id().to_string(),
            agent_did: lifecycle.agent_did().to_string(),
        })
    }
}

impl BackgroundLiveOutputState {
    async fn writer_for(
        &self,
        tool_call_id: impl Into<String>,
        row_target: LiveOutputRowTarget,
    ) -> anyhow::Result<crate::background_tools::LiveToolOutputWriter> {
        let tool_call_id = tool_call_id.into();
        let mut row_targets = self.row_targets.lock().await;
        if let Some(existing) = row_targets.get(&tool_call_id) {
            anyhow::ensure!(
                existing == &row_target,
                "live output registry identity conflict for tool_call_id={tool_call_id}: existing _docID={} new _docID={}",
                existing.doc_id,
                row_target.doc_id
            );
        } else {
            row_targets.insert(tool_call_id.clone(), row_target);
        }
        drop(row_targets);
        Ok(self.registry.writer_for(tool_call_id).await)
    }

    async fn remove(&self, tool_call_id: &str) {
        self.registry.remove(tool_call_id).await;
        self.row_targets.lock().await.remove(tool_call_id);
        self.flushed_seq.lock().await.remove(tool_call_id);
    }

    async fn record_flushed_seq_if_live(&self, tool_call_id: &str, seq: i64) {
        self.flushed_seq
            .lock()
            .await
            .insert(tool_call_id.to_string(), seq);
        // Close the mutation/worker-cleanup race: if cleanup removed the live
        // buffer while the durable write was in flight, do not resurrect its
        // sequence marker after cleanup has already run.
        if self.registry.snapshot(tool_call_id).await.is_none() {
            self.flushed_seq.lock().await.remove(tool_call_id);
        }
    }
}

/// Process-wide volatile state for ordinary background tool calls.
///
/// Sharing this registry across request hooks keeps both cancellation tokens
/// and live output buffers reachable until each process becomes terminal.
#[derive(Clone, Default)]
pub struct BackgroundExecutionRegistry {
    inner: Arc<std::sync::Mutex<HashMap<String, BackgroundExecution>>>,
    live_outputs: BackgroundLiveOutputState,
}

impl BackgroundExecutionRegistry {
    pub async fn cancel(&self, tool_call_id: &str) -> bool {
        let Some(execution) = self.lock_executions().get(tool_call_id).cloned() else {
            return false;
        };
        execution.cancellation_token.cancel();
        true
    }

    pub(crate) fn reserve(
        &self,
        tool_call_id: String,
        cancellation_token: CancellationToken,
    ) -> BackgroundExecutionReservation {
        self.lock_executions().insert(
            tool_call_id.clone(),
            BackgroundExecution { cancellation_token },
        );
        BackgroundExecutionReservation {
            registry: self.clone(),
            tool_call_id,
            armed: true,
        }
    }

    pub(crate) async fn remove(&self, tool_call_id: &str) {
        self.remove_now(tool_call_id);
    }

    pub(crate) async fn contains(&self, tool_call_id: &str) -> bool {
        self.lock_executions().contains_key(tool_call_id)
    }

    fn remove_now(&self, tool_call_id: &str) {
        self.lock_executions().remove(tool_call_id);
    }

    fn lock_executions(&self) -> std::sync::MutexGuard<'_, HashMap<String, BackgroundExecution>> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Cancellation-safe ownership reservation for the durable-running handoff.
/// Dropping the caller future before `tokio::spawn` transfers ownership removes
/// the volatile entry synchronously, so periodic recovery can see the orphan.
pub(crate) struct BackgroundExecutionReservation {
    registry: BackgroundExecutionRegistry,
    tool_call_id: String,
    armed: bool,
}

impl BackgroundExecutionReservation {
    pub(crate) fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for BackgroundExecutionReservation {
    fn drop(&mut self) {
        if self.armed {
            self.registry.remove_now(&self.tool_call_id);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TranscriptTurnState {
    Idle,
    AssistantBuilding { sequence: u32 },
    AssistantPersisted { sequence: u32 },
}

#[derive(Debug, Clone, Default)]
struct ToolResultIdentity {
    result_id: Option<String>,
    call_id: Option<String>,
}

struct SessionState {
    session_id: Option<String>,
    current_request_id: Option<String>,
    current_requester_did: Option<String>,
    request_deadline_at: Option<DateTime<Utc>>,
    approval_required_tools: Vec<String>,
    agent_name: String,
    sequence: u32,
    transcript_turn: TranscriptTurnState,
    persisted_tool_result_keys: HashSet<String>,
    persisted_tool_result_message_sequences: HashMap<String, u32>,
    tool_result_identities: HashMap<String, ToolResultIdentity>,
    initialized: bool,
}

impl SessionState {
    /// Reset on a genuine user message only. Tool-result messages must NOT
    /// reset the turn state: with several parallel tool calls accumulated in
    /// one persisted assistant turn, the first streamed result's user message
    /// would otherwise revoke the persisted-turn gate the remaining results
    /// still need (Lean: `Transcript.parallel_results_complete_independently`;
    /// `completeToolWithResult` never removes a persisted reservation).
    fn reset_after_user_message(&mut self) {
        self.transcript_turn = TranscriptTurnState::Idle;
    }

    fn begin_or_continue_assistant_turn(&mut self) -> u32 {
        match self.transcript_turn {
            TranscriptTurnState::AssistantBuilding { sequence } => sequence,
            TranscriptTurnState::Idle | TranscriptTurnState::AssistantPersisted { .. } => {
                self.sequence += 1;
                let sequence = self.sequence;
                self.transcript_turn = TranscriptTurnState::AssistantBuilding { sequence };
                sequence
            }
        }
    }

    fn persist_assistant_turn(&mut self) -> u32 {
        let sequence = match self.transcript_turn {
            TranscriptTurnState::AssistantBuilding { sequence } => sequence,
            TranscriptTurnState::Idle | TranscriptTurnState::AssistantPersisted { .. } => {
                self.sequence += 1;
                self.sequence
            }
        };
        self.transcript_turn = TranscriptTurnState::AssistantPersisted { sequence };
        sequence
    }

    fn register_tool_result_identity(
        &mut self,
        internal_call_id: &str,
        result_id: Option<&str>,
        call_id: Option<&str>,
    ) {
        let identity = self
            .tool_result_identities
            .entry(internal_call_id.to_string())
            .or_default();
        if let Some(result_id) = non_empty(result_id) {
            identity.result_id = Some(result_id.to_string());
        }
        if let Some(call_id) = non_empty(call_id) {
            identity.call_id = Some(call_id.to_string());
        }
    }

    fn tool_result_message_identity(
        &self,
        internal_call_id: &str,
        call_id: Option<&str>,
    ) -> (String, Option<String>) {
        let registered = self.tool_result_identities.get(internal_call_id);
        let result_id = registered
            .and_then(|identity| identity.result_id.clone())
            .or_else(|| non_empty(call_id).map(ToOwned::to_owned))
            .unwrap_or_else(|| internal_call_id.to_string());
        let call_id = registered
            .and_then(|identity| identity.call_id.clone())
            .or_else(|| non_empty(call_id).map(ToOwned::to_owned));

        (result_id, call_id)
    }

    fn mark_tool_result_seen_for_persisted_turn(
        &mut self,
        internal_call_id: &str,
        result_id: Option<&str>,
        call_id: Option<&str>,
    ) -> bool {
        self.register_tool_result_identity(internal_call_id, result_id, call_id);
        if !matches!(
            self.transcript_turn,
            TranscriptTurnState::AssistantPersisted { .. }
        ) {
            return false;
        }

        let keys = self.tool_result_dedupe_keys(internal_call_id, result_id, call_id);
        self.mark_tool_result_keys_seen(keys)
    }

    fn mark_stream_tool_result_seen(
        &mut self,
        internal_call_id: &str,
        result_id: &str,
        call_id: Option<&str>,
    ) -> anyhow::Result<bool> {
        self.register_tool_result_identity(internal_call_id, Some(result_id), call_id);
        let keys = self.tool_result_dedupe_keys(internal_call_id, Some(result_id), call_id);
        if !matches!(
            self.transcript_turn,
            TranscriptTurnState::AssistantPersisted { .. }
        ) {
            if self.tool_result_keys_already_seen(&keys) {
                self.persist_tool_result_keys(keys);
                return Ok(false);
            }
            anyhow::bail!(
                "cannot persist streamed tool result before its assistant turn is persisted"
            );
        }
        Ok(self.mark_tool_result_keys_seen(keys))
    }

    fn assistant_turn_persisted(&self) -> bool {
        matches!(
            self.transcript_turn,
            TranscriptTurnState::AssistantPersisted { .. }
        )
    }

    fn tool_result_dedupe_keys(
        &self,
        internal_call_id: &str,
        result_id: Option<&str>,
        call_id: Option<&str>,
    ) -> Vec<String> {
        let mut keys = Vec::new();
        push_tool_result_key(&mut keys, "internal", Some(internal_call_id));

        if let Some(identity) = self.tool_result_identities.get(internal_call_id) {
            push_tool_result_key(&mut keys, "result", identity.result_id.as_deref());
            push_tool_result_key(&mut keys, "call", identity.call_id.as_deref());
        }
        push_tool_result_key(&mut keys, "result", result_id);
        push_tool_result_key(&mut keys, "call", call_id);

        keys
    }

    fn mark_tool_result_keys_seen(&mut self, keys: Vec<String>) -> bool {
        let already_seen = self.tool_result_keys_already_seen(&keys);
        self.persist_tool_result_keys(keys);
        !already_seen
    }

    fn tool_result_keys_already_seen(&self, keys: &[String]) -> bool {
        keys.iter()
            .any(|key| self.persisted_tool_result_keys.contains(key))
    }

    fn persist_tool_result_keys(&mut self, keys: Vec<String>) {
        self.persisted_tool_result_keys.extend(keys);
    }
}

fn push_tool_result_key(keys: &mut Vec<String>, namespace: &str, value: Option<&str>) {
    let Some(value) = non_empty(value) else {
        return;
    };
    let key = format!("{namespace}:{value}");
    if !keys.iter().any(|existing| existing == &key) {
        keys.push(key);
    }
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.and_then(|value| (!value.is_empty()).then_some(value))
}

fn mutation_doc_ids(data: Option<&serde_json::Value>, field: &str) -> Vec<String> {
    let Some(value) = data.and_then(|data| data.get(field)) else {
        return Vec::new();
    };
    if let Some(doc_id) = value.get("_docID").and_then(serde_json::Value::as_str) {
        return vec![doc_id.to_string()];
    }
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|row| row.get("_docID").and_then(serde_json::Value::as_str))
        .map(str::to_string)
        .collect()
}

#[derive(Clone)]
pub struct DefraSessionHook {
    node: Arc<EmbeddedNode>,
    agent_did: String,
    truncation_limits: TruncationLimits,
    failure_policy: FailurePolicy,
    counters: Arc<HookCounters>,
    state: Arc<Mutex<SessionState>>,
    in_flight_lifecycles: Arc<Mutex<HashMap<String, ToolCallLifecycle>>>,
    background_tool_registry: BackgroundToolRegistry,
    background_executions: BackgroundExecutionRegistry,
    background_live_outputs: BackgroundLiveOutputState,
}

enum PolicyDecision {
    Continue,
    Terminate(String),
}

impl DefraSessionHook {
    pub fn with_identity(
        node: Arc<EmbeddedNode>,
        agent_name: &str,
        agent_did: &str,
        failure_policy: FailurePolicy,
    ) -> Self {
        let background_executions = BackgroundExecutionRegistry::default();
        let background_live_outputs = background_executions.live_outputs.clone();
        Self {
            node,
            agent_did: agent_did.to_string(),
            truncation_limits: TruncationLimits::default(),
            failure_policy,
            counters: Arc::new(HookCounters {
                failures: AtomicU64::new(0),
                successes: AtomicU64::new(0),
            }),
            state: Arc::new(Mutex::new(SessionState {
                session_id: None,
                current_request_id: None,
                current_requester_did: None,
                request_deadline_at: None,
                approval_required_tools: Vec::new(),
                agent_name: agent_name.to_string(),
                sequence: 0,
                transcript_turn: TranscriptTurnState::Idle,
                persisted_tool_result_keys: HashSet::new(),
                persisted_tool_result_message_sequences: HashMap::new(),
                tool_result_identities: HashMap::new(),
                initialized: false,
            })),
            in_flight_lifecycles: Arc::new(Mutex::new(HashMap::new())),
            background_tool_registry: BackgroundToolRegistry::default(),
            background_executions,
            background_live_outputs,
        }
    }

    pub async fn resume_with_identity_policy(
        node: Arc<EmbeddedNode>,
        session_id: &str,
        agent_name: &str,
        agent_did: &str,
        failure_policy: FailurePolicy,
    ) -> anyhow::Result<Self> {
        session::ensure_session(&node, session_id, agent_name, agent_did).await?;
        let max_seq = session::max_sequence(&node, session_id).await?;
        let background_executions = BackgroundExecutionRegistry::default();
        let background_live_outputs = background_executions.live_outputs.clone();

        Ok(Self {
            node,
            agent_did: agent_did.to_string(),
            truncation_limits: TruncationLimits::default(),
            failure_policy,
            counters: Arc::new(HookCounters {
                failures: AtomicU64::new(0),
                successes: AtomicU64::new(0),
            }),
            state: Arc::new(Mutex::new(SessionState {
                session_id: Some(session_id.to_string()),
                current_request_id: None,
                current_requester_did: None,
                request_deadline_at: None,
                approval_required_tools: Vec::new(),
                agent_name: agent_name.to_string(),
                sequence: max_seq,
                transcript_turn: TranscriptTurnState::Idle,
                persisted_tool_result_keys: HashSet::new(),
                persisted_tool_result_message_sequences: HashMap::new(),
                tool_result_identities: HashMap::new(),
                initialized: true,
            })),
            in_flight_lifecycles: Arc::new(Mutex::new(HashMap::new())),
            background_tool_registry: BackgroundToolRegistry::default(),
            background_executions,
            background_live_outputs,
        })
    }

    pub fn with_background_tool_registry(mut self, registry: BackgroundToolRegistry) -> Self {
        self.background_tool_registry = registry;
        self
    }

    pub fn with_background_execution_registry(
        mut self,
        registry: BackgroundExecutionRegistry,
    ) -> Self {
        self.background_live_outputs = registry.live_outputs.clone();
        self.background_executions = registry;
        self
    }

    pub fn stats(&self) -> HookStats {
        HookStats {
            persistence_failures: self.counters.failures.load(Ordering::Relaxed),
            persistence_successes: self.counters.successes.load(Ordering::Relaxed),
        }
    }

    fn record_success(&self) {
        self.counters.successes.fetch_add(1, Ordering::Relaxed);
    }

    fn decide_persistence_outcome(&self, context: &str, error: &anyhow::Error) -> PolicyDecision {
        decide_persistence_outcome(self.failure_policy, &self.counters, context, error)
    }

    fn on_persistence_error(&self, context: &str, error: &anyhow::Error) -> HookAction {
        match self.decide_persistence_outcome(context, error) {
            PolicyDecision::Continue => HookAction::Continue,
            PolicyDecision::Terminate(reason) => HookAction::Terminate { reason },
        }
    }

    fn on_tool_persistence_error(
        &self,
        context: &str,
        error: &anyhow::Error,
    ) -> ToolCallHookAction {
        match self.decide_persistence_outcome(context, error) {
            PolicyDecision::Continue => ToolCallHookAction::Continue,
            PolicyDecision::Terminate(reason) => ToolCallHookAction::Terminate { reason },
        }
    }

    pub async fn resume_or_create_with_identity_policy(
        node: Arc<EmbeddedNode>,
        session_id: &str,
        agent_name: &str,
        agent_did: &str,
        failure_policy: FailurePolicy,
    ) -> anyhow::Result<Self> {
        Self::resume_with_identity_policy(node, session_id, agent_name, agent_did, failure_policy)
            .await
    }

    pub async fn session_id(&self) -> Option<String> {
        self.state.lock().await.session_id.clone()
    }

    pub async fn set_active_request_id(&self, request_id: Option<String>) {
        self.set_active_request_lineage(request_id, None).await;
    }

    pub async fn set_active_request_lineage(
        &self,
        request_id: Option<String>,
        requester_did: Option<String>,
    ) {
        let mut state = self.state.lock().await;
        state.current_request_id = request_id;
        state.current_requester_did = requester_did;
    }

    async fn active_requester_did(&self) -> Option<String> {
        self.state.lock().await.current_requester_did.clone()
    }

    pub(crate) async fn register_stream_tool_call_identity(
        &self,
        internal_call_id: &str,
        result_id: &str,
        call_id: Option<&str>,
    ) {
        self.state.lock().await.register_tool_result_identity(
            internal_call_id,
            Some(result_id),
            call_id,
        );
    }

    pub async fn set_request_deadline_at(&self, deadline_at: Option<DateTime<Utc>>) {
        self.state.lock().await.request_deadline_at = deadline_at;
    }

    pub async fn set_approval_required_tools(&self, tools: Vec<String>) {
        self.state.lock().await.approval_required_tools = tools;
    }

    pub(crate) async fn approval_required_for(&self, tool_name: &str) -> bool {
        self.state
            .lock()
            .await
            .approval_required_tools
            .iter()
            .any(|name| name == tool_name)
    }

    pub(crate) async fn foreground_live_output_writer(
        &self,
        internal_call_id: &str,
    ) -> Option<crate::background_tools::LiveToolOutputWriter> {
        let row_target = {
            let lifecycles = self.in_flight_lifecycles.lock().await;
            let Some(lifecycle) = lifecycles.get(internal_call_id) else {
                tracing::warn!(
                    tool_call_id = %internal_call_id,
                    "live output disabled because the AgentToolCall lifecycle is not registered"
                );
                return None;
            };
            match LiveOutputRowTarget::from_lifecycle(lifecycle) {
                Ok(target) => target,
                Err(error) => {
                    tracing::warn!(
                        tool_call_id = %internal_call_id,
                        error = %error,
                        "live output disabled because the AgentToolCall physical identity is unavailable"
                    );
                    return None;
                }
            }
        };
        let writer = match self
            .background_live_outputs
            .writer_for(internal_call_id, row_target)
            .await
        {
            Ok(writer) => writer,
            Err(error) => {
                tracing::warn!(
                    tool_call_id = %internal_call_id,
                    error = %error,
                    "live output disabled because its registry identity conflicts"
                );
                return None;
            }
        };
        self.ensure_live_output_flusher();
        Some(writer)
    }

    pub(crate) async fn release_live_output(&self, tool_call_id: &str) {
        self.background_live_outputs.remove(tool_call_id).await;
    }

    pub(crate) fn ensure_live_output_flusher(&self) {
        use std::sync::atomic::Ordering;
        if self
            .background_live_outputs
            .flusher_running
            .swap(true, Ordering::AcqRel)
        {
            return;
        }
        let hook = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(2));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                match hook.flush_live_output_tails().await {
                    Ok(_) => {}
                    Err(error) => {
                        tracing::debug!(error = %error, "live output flush failed");
                    }
                }
                if hook
                    .background_live_outputs
                    .registry
                    .live_ids()
                    .await
                    .is_empty()
                {
                    break;
                }
            }
            hook.background_live_outputs
                .flusher_running
                .store(false, std::sync::atomic::Ordering::Release);
            // Close the empty-check/stop race: a writer that arrived after
            // the loop's last check saw the guard as running and did not spawn
            // a replacement, so re-check after releasing the shared guard.
            if !hook
                .background_live_outputs
                .registry
                .live_ids()
                .await
                .is_empty()
            {
                hook.ensure_live_output_flusher();
            }
        });
    }

    pub(crate) async fn flush_live_output_tails(&self) -> anyhow::Result<usize> {
        const TAIL_PERSIST_BYTES: usize = 4096;

        let live_ids = self.background_live_outputs.registry.live_ids().await;
        {
            let mut flushed = self.background_live_outputs.flushed_seq.lock().await;
            flushed.retain(|id, _| live_ids.contains(id));
        }

        let mut count = 0usize;
        for tool_call_id in live_ids {
            let Some(row_target) = self
                .background_live_outputs
                .row_targets
                .lock()
                .await
                .get(&tool_call_id)
                .cloned()
            else {
                tracing::warn!(
                    tool_call_id = %tool_call_id,
                    "live output buffer has no physical AgentToolCall identity; skipping flush"
                );
                continue;
            };
            let Some(snapshot) = self
                .background_live_outputs
                .registry
                .snapshot(&tool_call_id)
                .await
            else {
                continue;
            };
            let seq = snapshot.combined.total_bytes_seen as i64;
            if seq == 0 {
                continue;
            }
            if self
                .background_live_outputs
                .flushed_seq
                .lock()
                .await
                .get(&tool_call_id)
                .copied()
                == Some(seq)
            {
                continue;
            }
            let bytes = &snapshot.combined.bytes;
            let start = bytes.len().saturating_sub(TAIL_PERSIST_BYTES);
            let tail = String::from_utf8_lossy(&bytes[start..]).to_string();

            let escaped_doc_id = crate::graphql::escape_graphql_string(&row_target.doc_id);
            let escaped_request_id = crate::graphql::escape_graphql_string(&row_target.request_id);
            let escaped_session_id = crate::graphql::escape_graphql_string(&row_target.session_id);
            let escaped_agent_did = crate::graphql::escape_graphql_string(&row_target.agent_did);
            let row_query = format!(
                r#"{{
                    AgentToolCall(
                        filter: {{
                            _and: [
                                {{ _docID: {{ _eq: "{doc_id}" }} }},
                                {{ request_id: {{ _eq: "{request_id}" }} }},
                                {{ session_id: {{ _eq: "{session_id}" }} }},
                                {{ agent_did: {{ _eq: "{agent_did}" }} }}
                            ]
                        }},
                        limit: 2
                    ) {{
                        _docID
                        lifecycle_state
                        started_at
                        deadline_at
                        completed_at
                        unclaimed_deadline_at
                        cancel_cascade_intent_at
                        stuck_since
                    }}
                }}"#,
                doc_id = escaped_doc_id,
                request_id = escaped_request_id,
                session_id = escaped_session_id,
                agent_did = escaped_agent_did,
            );
            let row_response = self.node.execute(&row_query).await;
            if row_response.has_errors() {
                tracing::debug!(
                    tool_call_id = %tool_call_id,
                    doc_id = %row_target.doc_id,
                    errors = ?row_response.errors,
                    "live output exact-row reload failed; will retry next tick"
                );
                continue;
            }
            let row_values = row_response
                .data
                .as_ref()
                .and_then(|data| data.get("AgentToolCall"))
                .and_then(|value| value.as_array())
                .cloned()
                .unwrap_or_default();
            let [row_value] = row_values.as_slice() else {
                tracing::warn!(
                    tool_call_id = %tool_call_id,
                    doc_id = %row_target.doc_id,
                    matched_rows = row_values.len(),
                    "live output exact-row reload found stale or conflicting physical identity"
                );
                continue;
            };
            if row_value.get("lifecycle_state").and_then(|v| v.as_str()) != Some("running") {
                tracing::debug!(
                    tool_call_id = %tool_call_id,
                    doc_id = %row_target.doc_id,
                    observed_state = ?row_value.get("lifecycle_state"),
                    "live output flush lost its running-state compare"
                );
                continue;
            }
            let datetime_row: crate::background_completion::AgentToolCallDateTimeRow =
                serde_json::from_value(row_value.clone()).unwrap_or_default();
            let mut datetime_fields = Vec::new();
            crate::background_completion::push_datetime_field(
                &mut datetime_fields,
                &[],
                "started_at",
                datetime_row.started_at.as_deref(),
            );
            crate::background_completion::push_datetime_field(
                &mut datetime_fields,
                &[],
                "deadline_at",
                datetime_row.deadline_at.as_deref(),
            );
            crate::background_completion::push_datetime_field(
                &mut datetime_fields,
                &[],
                "completed_at",
                datetime_row.completed_at.as_deref(),
            );
            crate::background_completion::push_datetime_field(
                &mut datetime_fields,
                &[],
                "unclaimed_deadline_at",
                datetime_row.unclaimed_deadline_at.as_deref(),
            );
            crate::background_completion::push_datetime_field(
                &mut datetime_fields,
                &[],
                "cancel_cascade_intent_at",
                datetime_row.cancel_cascade_intent_at.as_deref(),
            );
            crate::background_completion::push_datetime_field(
                &mut datetime_fields,
                &[],
                "stuck_since",
                datetime_row.stuck_since.as_deref(),
            );
            let datetime_fragment = if datetime_fields.is_empty() {
                String::new()
            } else {
                format!(", {}", datetime_fields.join(", "))
            };
            // CAS on running: a straggler tick must never stamp telemetry
            // onto a terminal row.
            let mutation = format!(
                r#"mutation {{
                    update_AgentToolCall(
                        filter: {{
                            _and: [
                                {{ _docID: {{ _eq: "{doc_id}" }} }},
                                {{ request_id: {{ _eq: "{request_id}" }} }},
                                {{ session_id: {{ _eq: "{session_id}" }} }},
                                {{ agent_did: {{ _eq: "{agent_did}" }} }},
                                {{ lifecycle_state: {{ _eq: "running" }} }}
                            ]
                        }},
                        input: {{ partial_output_tail: "{tail}", partial_output_seq: {seq}{datetimes} }}
                    ) {{ _docID }}
                }}"#,
                doc_id = escaped_doc_id,
                request_id = escaped_request_id,
                session_id = escaped_session_id,
                agent_did = escaped_agent_did,
                tail = crate::graphql::escape_graphql_string(&tail),
                datetimes = datetime_fragment,
            );
            let response = self.node.execute(&mutation).await;
            if response.has_errors() {
                tracing::debug!(
                    tool_call_id = %tool_call_id,
                    errors = ?response.errors,
                    "live output tail flush failed; will retry next tick"
                );
            } else {
                let returned_doc_ids =
                    mutation_doc_ids(response.data.as_ref(), "update_AgentToolCall");
                match returned_doc_ids.as_slice() {
                    [updated_doc_id] if updated_doc_id == &row_target.doc_id => {
                        self.background_live_outputs
                            .record_flushed_seq_if_live(&tool_call_id, seq)
                            .await;
                        count += 1;
                    }
                    [] => {
                        tracing::warn!(
                            tool_call_id = %tool_call_id,
                            doc_id = %row_target.doc_id,
                            "live output tail flush lost its exact running-state compare; will retry while live"
                        );
                    }
                    _ => {
                        tracing::error!(
                            tool_call_id = %tool_call_id,
                            doc_id = %row_target.doc_id,
                            returned_doc_ids = ?returned_doc_ids,
                            "live output tail flush returned an unexpected physical row set"
                        );
                    }
                }
            }
        }
        Ok(count)
    }

    pub(crate) async fn timeout_expired_tool_calls(&self) -> anyhow::Result<usize> {
        let lifecycles = {
            let now = Utc::now();
            let mut map = self.in_flight_lifecycles.lock().await;
            let expired_ids = map
                .iter()
                .filter_map(|(id, lifecycle)| (lifecycle.deadline_at() <= now).then(|| id.clone()))
                .collect::<Vec<_>>();

            expired_ids
                .into_iter()
                .filter_map(|id| map.remove(&id))
                .collect::<Vec<_>>()
        };

        let count = lifecycles.len();
        for mut lifecycle in lifecycles {
            if lifecycle.is_subagent_bridge() && lifecycle.await_mode() != AwaitMode::Foreground {
                tracing::debug!(
                    "leaving background subagent bridge running after parent deadline sweep"
                );
            } else if lifecycle.state()
                == crate::tool_call_lifecycle::ToolCallState::AwaitingApproval
            {
                lifecycle.timeout_while_held().await?;
            } else {
                // Foreground subagent bridges take the same deadline
                // transition as native tools: `timedOut`, never a fabricated
                // `ChildTerminal::Dead` — the child may still be live, and its
                // terminalization belongs to the subagent-liveness sweep
                // (#1002; Lean `ToolExecution.Transition.timeout`).
                let _ = lifecycle.timeout().await?;
            }
        }
        Ok(count)
    }

    pub async fn cancel_in_flight_tool_calls(&self) -> anyhow::Result<usize> {
        let lifecycles = {
            let mut map = self.in_flight_lifecycles.lock().await;
            map.drain()
                .map(|(_, lifecycle)| lifecycle)
                .collect::<Vec<_>>()
        };

        let count = lifecycles.len();
        for mut lifecycle in lifecycles {
            if lifecycle.state() == crate::tool_call_lifecycle::ToolCallState::AwaitingApproval {
                lifecycle
                    .cancel_while_held(CancelCause::Interrupted)
                    .await?;
                continue;
            }
            let dispatch = lifecycle
                .cancel_during_run_with_cascade_dispatch(CancelCause::Interrupted, &self.agent_did)
                .await?;
            if lifecycle.is_cancelled() {
                if let Some(dispatch) = dispatch {
                    if let CascadeDispatch::Local(intent) = dispatch {
                        if let Err(error) = crate::interrupt::interrupt_request(
                            &self.node,
                            &intent.child_request_id,
                        )
                        .await
                        {
                            tracing::warn!(
                                child_request_id = %intent.child_request_id,
                                error = %error,
                                "failed to cascade live tool-call cancellation to child request"
                            );
                        }
                    }
                }
            }
        }
        Ok(count)
    }

    pub(crate) async fn fail_in_flight_tool_calls(
        &self,
        result: &str,
        failure_class: crate::tool_call_lifecycle::FailureClass,
    ) -> anyhow::Result<usize> {
        let lifecycles = {
            let mut map = self.in_flight_lifecycles.lock().await;
            map.drain()
                .map(|(_, lifecycle)| lifecycle)
                .collect::<Vec<_>>()
        };

        let count = lifecycles.len();
        for mut lifecycle in lifecycles {
            if lifecycle.is_subagent_bridge() {
                lifecycle
                    .bridge_failure(ChildTerminal::Failed {
                        reason: result.to_string(),
                        failure_class,
                    })
                    .await?;
            } else {
                lifecycle.fail(result, failure_class).await?;
            }
        }
        Ok(count)
    }

    pub async fn mark_current_response_materialized(&self, sequence: u32) -> anyhow::Result<()> {
        let request_id = self.state.lock().await.current_request_id.clone();
        let Some(request_id) = request_id.as_deref() else {
            return Ok(());
        };
        session::mark_response_materialized(&self.node, request_id, sequence).await
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        let session_id = self.state.lock().await.session_id.clone();
        if let Some(id) = session_id {
            session::close_session(&self.node, &id).await?;
        }
        Ok(())
    }

    pub fn apply_persistence_policy(
        &self,
        result: anyhow::Result<()>,
        context: &str,
    ) -> anyhow::Result<()> {
        match result {
            Ok(()) => {
                self.record_success();
                Ok(())
            }
            Err(e) => match self.decide_persistence_outcome(context, &e) {
                PolicyDecision::Continue => Ok(()),
                PolicyDecision::Terminate(_) => Err(e),
            },
        }
    }
}

fn decide_persistence_outcome(
    failure_policy: FailurePolicy,
    counters: &HookCounters,
    context: &str,
    error: &anyhow::Error,
) -> PolicyDecision {
    counters.failures.fetch_add(1, Ordering::Relaxed);
    match failure_policy {
        FailurePolicy::FailOpen => {
            tracing::warn!(error = %error, context = %context, "persistence failed (fail-open)");
            PolicyDecision::Continue
        }
        FailurePolicy::FailClosed => {
            tracing::error!(error = %error, context = %context, "persistence failed (fail-closed) — terminating");
            PolicyDecision::Terminate(format!("persistence failed: {error}"))
        }
    }
}
