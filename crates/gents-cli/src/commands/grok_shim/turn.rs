//! Grok shim prompt/cancel turn manager.
//!
//! This leaf owns the connection-scoped pending prompt lifecycle for the Grok
//! pager's `session/prompt` and `session/cancel` wire methods.
//!
//! `session/prompt` parses the audited wire shape (sessionId, prompt blocks
//! with per-block `meta.skillTokenRanges` / PromptBlockMeta::bash, and
//! `_meta.promptId` / `_meta.screenMode` / `_meta.sendNow`), echoes the user
//! message back as a `user_message_chunk` `session/update` notification, and
//! then defers the JSON-RPC response until the durable request terminalizes.
//! The response result is a `stopReason` projection of the durable lifecycle —
//! never a persisted field.
//!
//! `session/cancel` parses the audited notification shape (sessionId plus
//! `_meta.cancelSubagents` / `_meta.cancelTrigger` / `_meta.rewindIfNoOutput`
//! / `_meta.rewindIfPristine` / `_meta.promptId`), is a notification (never
//! responded to), and interrupts the pending request through
//! [`gents::interrupt_request`]. `cancelSubagents=true` also interrupts
//! runtime child `AgentRequest` rows linked by `caused_by_parent_request_id`;
//! static `Task` configuration rows are never queried or mutated as runtime
//! state.
//!
//! Ordering contract: the returned Gents request id is registered on the
//! pending entry *before* the first fallible outbound send, so a send failure
//! after submission interrupts the durable request instead of leaking it.
//! Cancel/disconnect may fire before the request id is even known — in that
//! window they drain the pending entry, resolve the connected prompt with
//! `stopReason="cancelled"`, and cancel any future submission of that prompt,
//! so the session immediately accepts the next prompt. Submission failures
//! are classified in the same critical section that observes connection
//! closure and the pending entry, so a disconnect that has already published
//! its closed+drained state always resolves `stopReason="cancelled"` — even
//! before the disconnect has published the drained entry's cancel-before-id
//! latch. While `submit_request` is awaited, only an explicit cancel or a
//! disconnect can remove the pending entry (every other removal happens in
//! the submitter's own task after the request id is known), so a submission
//! failure that observes its entry already removed also resolves
//! `stopReason="cancelled"` — even before the cancel has published the
//! drained entry's latch.
//!
//! One pending prompt per session: a second `session/prompt` for the same
//! session while one is live is rejected and does not disturb the live turn.
//! Pending prompts are keyed by (session id, prompt id) inside this
//! connection-scoped manager.
//!
//! All durable reads/writes go through the in-process embedded node
//! (`node.execute(&query).await`) with every interpolated value escaped by
//! `gents::graphql::escape_graphql_string`; no HTTP GraphQL helper is used
//! except the `create_agent_request` seam, which takes the bound GraphQL
//! endpoint. The turn streams durable `AgentMessage`/`AgentToolCall`/
//! subagent projection live: each watch cycle runs the projection engine's
//! request-scoped poll and sends every novel `session/update` through the
//! sender (deterministic tools → subagents → messages order), with a final
//! flush before the deferred response. The payload shapes are owned by the
//! projection leaves; the turn only polls, dedupes by durable identity
//! (advancing the request-local cursor only after a successful send), and
//! delivers.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use defra_node::EmbeddedNode;
use gents::graphql::{ensure_no_errors, escape_graphql_string};
use serde_json::{json, Value};
use tokio::sync::{oneshot, Mutex};

use super::projection::{
    AsyncCommit, AsyncSendLine, CursorAdvance, NovelProjectionEvent, ProjectionEngine,
    RequestCursor, SessionUpdateChannel, UpdateTimestamps, SESSION_UPDATE_METHOD,
};
use super::server::AcpOutbound;
use crate::request_helpers::{
    graphql_error_is_transient, transient_graphql_retry_delay, MAX_TRANSIENT_GRAPHQL_RETRIES,
};

/// Poll cadence for watching the durable request terminalize. The embedded
/// node exposes no subscription seam to the shim, so terminalization is
/// observed by bounded polling; the pager expects the deferred response
/// promptly after terminalization, so the interval is short.
const TERMINAL_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Wire values for `screenMode` accepted on `session/prompt` `_meta`.
pub(super) const SCREEN_MODES: [&str; 3] = ["fullscreen", "inline", "minimal"];

/// A single prompt content block as sent by the pager.
///
/// The audited wire carries `prompt: [{"type":"text","text":"...", ...}]` with
/// an optional block `meta` containing `skillTokenRanges` (array of
/// [start,end] pairs) or a PromptBlockMeta::bash command stamp.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct PromptBlock {
    pub kind: String,
    pub text: String,
    pub meta: Option<Value>,
}

impl PromptBlock {
    /// The `meta.bash.command` stamp of the bash variant, when present.
    /// Test observation helper: the echo loop forwards the whole block
    /// verbatim and never discriminates by bash meta.
    #[cfg(test)]
    pub(super) fn bash_command(&self) -> Option<String> {
        let meta = self.meta.as_ref()?;
        let command = meta
            .get("bash")
            .and_then(|bash| bash.get("command"))
            .and_then(Value::as_str)?;
        (!command.is_empty()).then(|| command.to_string())
    }

    /// Whether this block carries non-empty `skillTokenRanges`. Test
    /// observation helper: the echo loop forwards block meta verbatim.
    #[cfg(test)]
    pub(super) fn has_skill_token_ranges(&self) -> bool {
        self.meta
            .as_ref()
            .and_then(|meta| meta.get("skillTokenRanges"))
            .and_then(Value::as_array)
            .is_some_and(|ranges| !ranges.is_empty())
    }
}

/// The audited `session/prompt` request shape.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct PromptRequest {
    pub session_id: String,
    pub prompt: Vec<PromptBlock>,
    pub prompt_id: Option<String>,
    pub screen_mode: Option<String>,
    pub send_now: bool,
    pub id: Option<Value>,
}

/// The audited `session/cancel` notification shape.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct CancelNotification {
    pub session_id: String,
    pub cancel_subagents: bool,
    pub cancel_trigger: Option<String>,
    pub rewind_if_no_output: bool,
    pub rewind_if_pristine: bool,
    pub prompt_id: Option<String>,
}

impl CancelNotification {
    /// Build the audited `_meta` payload the pager sends with a cancel.
    /// Test observation helper: the cancel handler reads the notification
    /// fields directly and never re-serializes the meta.
    #[cfg(test)]
    pub(super) fn meta(&self) -> Value {
        let mut meta = json!({
            "cancelSubagents": self.cancel_subagents,
            "rewindIfNoOutput": self.rewind_if_no_output,
            "rewindIfPristine": self.rewind_if_pristine,
        });
        if let Some(trigger) = self.cancel_trigger.as_deref() {
            meta["cancelTrigger"] = json!(trigger);
        }
        if let Some(prompt_id) = self.prompt_id.as_deref() {
            meta["promptId"] = json!(prompt_id);
        }
        meta
    }
}

/// Projection of the durable terminal state into a wire `stopReason`.
///
/// `stopReason` is an adapter projection, not a persisted field: the durable
/// source is `AgentRequest.lifecycle_state` plus the `AgentResponse` status
/// vocabulary and its `interrupted_at` marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StopReason {
    EndTurn,
    Cancelled,
    Refusal,
    Error,
}

impl StopReason {
    pub(super) fn wire_name(self) -> &'static str {
        match self {
            StopReason::EndTurn => "end_turn",
            StopReason::Cancelled => "cancelled",
            StopReason::Refusal => "refusal",
            StopReason::Error => "error",
        }
    }
}

/// How a turn sends notifications to the connected client.
///
/// Cloning shares the underlying channel/buffer, which lets a caller keep a
/// copy while a spawned task drives one turn. Every variant carries the
/// connection's common session-update send path: all three `session/update`
/// families (set-mode updates, the user echo, and the durable projected
/// updates) allocate and enqueue their event ids through it, so per-session
/// allocation order equals enqueue order.
#[derive(Clone)]
pub(super) enum PromptSender {
    /// Sends one JSON-RPC notification line to the live client through the
    /// connection's [`AcpOutbound`]. The sender is fallible: a closed
    /// channel must interrupt the submitted request. Live turns send every
    /// notification through this handle as it is produced — the user echo,
    /// then each novel durable projection line — so the pager sees the turn
    /// stream in real time; only the deferred `session/prompt` response is
    /// written by the dispatcher after the turn resolves.
    Live { outbound: AcpOutbound },
    /// Historical delivery through the same send-success path. This only
    /// marks wire observations; it never changes persisted runtime state.
    Replay { inner: Arc<PromptSender> },
    /// Collects serialized notification lines in memory (tests, headless
    /// capture). The buffer never fails, so a test can simulate a send
    /// failure only through the Live variant.
    #[cfg(test)]
    Buffer { buffer: Arc<Mutex<Vec<String>>> },
}

impl PromptSender {
    /// Send one already-serialized JSON-RPC line. The first fallible send
    /// after the request id is registered; a failure here must interrupt
    /// the submitted request.
    pub(super) async fn send_line(&self, line: String) -> Result<()> {
        match self {
            PromptSender::Live { outbound } => outbound.send(line).await,
            PromptSender::Replay { inner } => {
                let mut value: Value = serde_json::from_str(&line)?;
                value["params"]["_meta"]["isReplay"] = Value::Bool(true);
                Box::pin(inner.send_line(serde_json::to_string(&value)?)).await
            }
            #[cfg(test)]
            PromptSender::Buffer { buffer } => {
                buffer.lock().await.push(line);
                Ok(())
            }
        }
    }

    /// Drain every notification line collected so far. A `Buffer` yields
    /// its lines (the headless/test path converts them into dispatch
    /// notifications); a `Live` sender yields nothing — every line was
    /// already delivered to the client as it was produced.
    pub(super) async fn take_lines(&self) -> Vec<String> {
        match self {
            PromptSender::Live { .. } => Vec::new(),
            PromptSender::Replay { inner } => Box::pin(inner.take_lines()).await,
            #[cfg(test)]
            PromptSender::Buffer { buffer } => {
                let mut buffer = buffer.lock().await;
                std::mem::take(&mut *buffer)
            }
        }
    }

    /// Send the synthetic `user_message_chunk` echo of one prompt block
    /// through the connection's common session-update send path: the
    /// per-session send lock is held across reserve → stamp → send → commit,
    /// so a failed echo does not consume an id and the echo shares the
    /// pager's `"{sessionId}-{counter}"` dedup space with the projected
    /// updates. `totalTokens` is the last known context occupancy read under the
    /// same lock (zero before any projected observation has applied).
    pub(super) async fn send_user_message_chunk(
        &self,
        session_updates: &SessionUpdateChannel,
        session_id: &str,
        prompt_id: &str,
        block: &PromptBlock,
        prompt_index: usize,
    ) -> Result<()> {
        let session_id = session_id.to_string();
        let prompt_id = prompt_id.to_string();
        let block = block.clone();
        let session_for_lock = session_id.clone();
        session_updates
            .send(
                &session_for_lock,
                move |event_id, total_tokens| {
                    let meta = json!({
                        "promptId": prompt_id,
                        "isReplay": false,
                        "eventId": event_id,
                        "totalTokens": total_tokens,
                        "agentTimestampMs": chrono::Utc::now().timestamp_millis(),
                    });
                    let params = json!({
                        "sessionId": session_id,
                        "update": {
                            "sessionUpdate": "user_message_chunk",
                            "content": {
                                "type": "text",
                                "text": block.text,
                            },
                            "_meta": {
                                "promptIndex": prompt_index,
                                "hideFromScrollback": false,
                            },
                        },
                        "_meta": meta,
                    });
                    Ok(json!({
                        "jsonrpc": "2.0",
                        "method": SESSION_UPDATE_METHOD,
                        "params": params,
                    }))
                },
                PromptSenderLine(self),
            )
            .await
            .map(|_| ())
    }
}

/// Adapter that lets a borrowed [`PromptSender`] drive the common send
/// path's enqueue step.
pub(super) struct PromptSenderLine<'a>(pub(super) &'a PromptSender);

impl AsyncSendLine for PromptSenderLine<'_> {
    async fn send_line(&self, line: String) -> Result<()> {
        self.0.send_line(line).await
    }
}

/// Commit one projection cursor advance inside the common session send
/// critical section, immediately after the corresponding line is enqueued.
struct ProjectionCursorCommit<'a> {
    cursor: &'a tokio::sync::Mutex<RequestCursor>,
    advance: CursorAdvance,
}

/// Request-local realization of projection segment identities into the
/// pager's timestamp vocabulary. Segment starts remain stable across polls
/// and retries, and are forced strictly increasing because the pager uses
/// `streamStartMs` inequality as the model-generation boundary signal.
struct RequestUpdateTiming {
    turn_start_ms: i64,
    active_segment: Option<String>,
    starts: BTreeMap<String, i64>,
    last_start_ms: Option<i64>,
}

impl RequestUpdateTiming {
    fn new(turn_start_ms: i64) -> Self {
        Self {
            turn_start_ms,
            active_segment: None,
            starts: BTreeMap::new(),
            last_start_ms: None,
        }
    }

    fn resolve(
        &mut self,
        timing: Option<&super::projection::ProjectionEventTiming>,
        now_ms: i64,
    ) -> UpdateTimestamps {
        if let Some(timing) = timing {
            let start = if let Some(start) = self.starts.get(&timing.segment_key) {
                *start
            } else {
                let candidate = timing.stream_start_candidate_ms.unwrap_or(now_ms);
                let start = self.last_start_ms.map_or(candidate, |previous| {
                    candidate.max(previous.saturating_add(1))
                });
                self.starts.insert(timing.segment_key.clone(), start);
                self.last_start_ms = Some(start);
                start
            };
            self.active_segment = Some(timing.segment_key.clone());
            let agent_timestamp_ms = timing
                .agent_timestamp_candidate_ms
                .unwrap_or(now_ms)
                .max(start);
            return UpdateTimestamps {
                agent_timestamp_ms: Some(agent_timestamp_ms),
                stream_start_ms: Some(start),
                turn_start_ms: Some(self.turn_start_ms),
            };
        }
        UpdateTimestamps {
            agent_timestamp_ms: Some(now_ms),
            stream_start_ms: self
                .active_segment
                .as_ref()
                .and_then(|segment| self.starts.get(segment).copied()),
            turn_start_ms: Some(self.turn_start_ms),
        }
    }
}

impl AsyncCommit for ProjectionCursorCommit<'_> {
    async fn commit(&self) {
        self.cursor.lock().await.record(self.advance.clone());
    }
}

/// Configuration the ACP service binds into the turn manager.
#[derive(Clone, Debug)]
pub(super) struct TurnManagerConfig {
    /// The agent did every request is submitted under.
    pub agent_did: String,
    /// The behavior id the serving shim is bound to.
    pub behavior_id: String,
    /// GraphQL endpoint string accepted by `create_agent_request`; the
    /// in-process embedded node is authoritative for reads.
    pub graphql: String,
}

/// Shared latch recorded the moment a prompt is cancelled before its Gents
/// request id is known. The submitter checks it after registration so the
/// cancel-before-id race resolves deterministically.
#[derive(Debug, Default)]
struct CancelBeforeIdLatch {
    cancelled: bool,
}

impl CancelBeforeIdLatch {
    fn cancel(&mut self) -> bool {
        let was_cancelled = self.cancelled;
        self.cancelled = true;
        !was_cancelled
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled
    }
}

/// State of one connection-scoped pending prompt. The (session id, prompt id)
/// key carries the correlation ids; this struct carries only the response
/// plumbing and the submission state.
struct PendingPrompt {
    /// Resolves the deferred `session/prompt` response.
    response_tx: Option<oneshot::Sender<Result<Value>>>,
    /// The Gents request id once submission succeeded; registered here
    /// *before* the first fallible outbound send.
    request_id: Option<String>,
    /// Latch for the cancel-before-request-id window.
    cancel_before_id: Arc<Mutex<CancelBeforeIdLatch>>,
    /// Whether cancel/disconnect already drained this entry.
    drained: bool,
}

impl PendingPrompt {
    fn resolve(&mut self, result: Result<Value>) {
        if let Some(tx) = self.response_tx.take() {
            let _ = tx.send(result);
        }
        self.drained = true;
    }
}

/// Connection-scoped turn state under a single mutex: the irreversible
/// `closed` latch set by disconnect, and the connection's pending prompts.
/// The latch is checked in the same critical section as insertion, so no
/// prompt task spawned before a disconnect can insert (and therefore submit)
/// after the disconnect drained. There is no reopen: a `TurnManager` belongs
/// to exactly one connection lifetime.
#[derive(Default)]
struct ConnectionState {
    closed: bool,
    entries: HashMap<(String, String), PendingPrompt>,
}

/// Delivery progress only. Lifecycle and content are always re-read from the
/// database. The foreground watcher holds this lock until its final flush;
/// afterwards the session observer continues with the very same cursor.
struct ObservedRequest {
    prompt_id: Option<String>,
    cursor: Mutex<RequestCursor>,
    timing: RequestUpdateTiming,
    completion_sent: bool,
    echo_sent: bool,
}

impl ObservedRequest {
    fn new(prompt_id: String, started_at: i64, foreground: bool) -> Self {
        Self {
            prompt_id: (!foreground).then_some(prompt_id),
            cursor: Mutex::new(RequestCursor::new()),
            timing: RequestUpdateTiming::new(started_at),
            completion_sent: foreground,
            echo_sent: true,
        }
    }
}

type ObservedRequests = BTreeMap<(String, String), Arc<Mutex<ObservedRequest>>>;

type ForegroundDeliveries = Arc<std::sync::Mutex<HashMap<String, usize>>>;

/// Holds observer output until the foreground response has been enqueued.
/// Synchronous Drop also releases it on an outbound error or task abort.
pub(super) struct ForegroundDelivery {
    sessions: ForegroundDeliveries,
    session_id: String,
}

impl Drop for ForegroundDelivery {
    fn drop(&mut self) {
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = sessions.get_mut(&self.session_id) {
            *count -= 1;
            if *count == 0 {
                sessions.remove(&self.session_id);
            }
        }
    }
}

impl ConnectionState {
    fn is_closed(&self) -> bool {
        self.closed
    }
}

/// Owns the connection-scoped pending prompts and exposes the prompt, cancel,
/// and disconnect operations.
pub(super) struct TurnManager {
    node: Arc<EmbeddedNode>,
    config: TurnManagerConfig,
    state: Mutex<ConnectionState>,
    /// Serializes observer sends with foreground admission. Disconnect never
    /// acquires this gate, so a blocked outbound send cannot block shutdown.
    delivery_gate: Mutex<()>,
    foreground_deliveries: ForegroundDeliveries,
    /// Current autonomous wire turn, retained until its terminal notification
    /// is delivered. This is a transport boundary, not a runtime lifecycle.
    autonomous_delivery: Mutex<HashMap<String, String>>,
    observed: Mutex<ObservedRequests>,
    observers: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
    /// Parks `handle_prompt` before the insertion critical section.
    #[cfg(test)]
    insertion_gate: Mutex<Option<TestGate>>,
    /// Parks disconnect after publishing closed+drained state.
    #[cfg(test)]
    disconnect_gate: Mutex<Option<TestGate>>,
    /// Parks explicit cancel after removing the pending entry.
    #[cfg(test)]
    cancel_drain_gate: Mutex<Option<TestGate>>,
    /// Parks cancel after snapshotting matching entry generations.
    #[cfg(test)]
    cancel_selection_gate: Mutex<Option<TestGate>>,
}

/// Reusable deterministic test seam with explicit arrival and release phases.
#[cfg(test)]
#[derive(Debug, Clone)]
struct TestGate {
    arrived: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl TurnManager {
    pub(super) fn new(node: Arc<EmbeddedNode>, config: TurnManagerConfig) -> Self {
        TurnManager {
            node,
            config,
            state: Mutex::new(ConnectionState::default()),
            delivery_gate: Mutex::new(()),
            foreground_deliveries: Arc::new(std::sync::Mutex::new(HashMap::new())),
            autonomous_delivery: Mutex::new(HashMap::new()),
            observed: Mutex::new(BTreeMap::new()),
            observers: Mutex::new(HashMap::new()),
            #[cfg(test)]
            insertion_gate: Mutex::new(None),
            #[cfg(test)]
            disconnect_gate: Mutex::new(None),
            #[cfg(test)]
            cancel_drain_gate: Mutex::new(None),
            #[cfg(test)]
            cancel_selection_gate: Mutex::new(None),
        }
    }

    pub(super) async fn begin_foreground_delivery(&self, session_id: &str) -> ForegroundDelivery {
        let _delivery = self.delivery_gate.lock().await;
        *self
            .foreground_deliveries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(session_id.to_owned())
            .or_default() += 1;
        ForegroundDelivery {
            sessions: self.foreground_deliveries.clone(),
            session_id: session_id.to_owned(),
        }
    }

    /// Observe this connection's session after the foreground RPC completes.
    /// Each tick discovers one bounded page, then advances retained delivery
    /// cursors. A new sweep revisits IDs below the previous page, so concurrent
    /// inserts cannot fall permanently behind a pagination watermark.
    pub(super) async fn observe_session(
        self: &Arc<Self>,
        session_id: &str,
        sender: PromptSender,
        projections: Arc<ProjectionEngine>,
    ) {
        self.observe_session_since(
            session_id,
            chrono::Utc::now().to_rfc3339(),
            sender,
            projections,
        )
        .await;
    }

    /// Resume uses the attachment time captured before reading history, so
    /// requests created during replay cannot fall into a discovery gap.
    pub(super) async fn observe_session_since(
        self: &Arc<Self>,
        session_id: &str,
        attached_at: String,
        sender: PromptSender,
        projections: Arc<ProjectionEngine>,
    ) {
        let mut observers = self.observers.lock().await;
        if observers.contains_key(session_id) || self.state.lock().await.closed {
            return;
        }
        let session_id = session_id.to_owned();
        let manager = Arc::downgrade(self);
        let observed_session = session_id.clone();
        observers.insert(
            session_id,
            tokio::spawn(async move {
                let mut after = String::new();
                let mut delivery_after = String::new();
                let mut goal_cursor = super::goals::GoalCursor::default();
                let mut goal_refresh_at = tokio::time::Instant::now();
                loop {
                    tokio::time::sleep(TERMINAL_POLL_INTERVAL).await;
                    let Some(manager) = manager.upgrade() else {
                        break;
                    };
                    if manager.state.lock().await.closed {
                        break;
                    }
                    // Goal accounting scans the runtime ledger. Its panel
                    // does not require token-stream polling frequency.
                    if tokio::time::Instant::now() >= goal_refresh_at {
                        goal_refresh_at = tokio::time::Instant::now() + Duration::from_secs(1);
                        if let Err(error) = goal_cursor
                            .refresh(
                                &manager.node,
                                &manager.config.agent_did,
                                &observed_session,
                                &sender,
                                &projections,
                            )
                            .await
                        {
                            tracing::warn!(%error, session_id = %observed_session,
                            "Grok goal observation failed; retrying");
                        }
                    }
                    if let Err(error) = manager
                        .observe_session_tick(
                            &observed_session,
                            &attached_at,
                            &mut after,
                            &mut delivery_after,
                            &sender,
                            &projections,
                        )
                        .await
                    {
                        // Observation failures never change durable execution.
                        // The cursor commits only delivered events, so retrying
                        // after a database read conflict cannot lose an update.
                        tracing::warn!(%error, session_id = %observed_session,
                        "Grok shim session observation failed; retrying");
                    }
                }
            }),
        );
    }

    /// Reuse the live projection and its delivery cursors for persisted
    /// history. The caller starts observation only after the load response
    /// has been enqueued. No new request is submitted by this operation.
    pub(super) async fn replay_session(
        &self,
        session_id: &str,
        rows: &[gents_protocol::row::AgentRequestRow],
        sender: &PromptSender,
        projections: &ProjectionEngine,
    ) -> Result<Option<String>> {
        let replay = PromptSender::Replay {
            inner: Arc::new(sender.clone()),
        };
        let mut running = None;
        for row in rows {
            let started_at = row
                .created_at
                .as_deref()
                .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
                .map(|time| time.timestamp_millis())
                .unwrap_or(0);
            let live_prompt_id = format!("notifications-{}", row.request_id);
            // The stock client hides auto-wake prompts by their ID family.
            // Preserve the submitted identity when replaying a human turn;
            // labeling every historical prompt notifications-* hides history.
            let metadata = row
                .metadata
                .as_deref()
                .and_then(|raw| serde_json::from_str::<Value>(raw).ok());
            let prompt_id = metadata
                .as_ref()
                .and_then(|meta| meta.get("promptId"))
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    if row.runtime_source_kind.is_some() {
                        live_prompt_id.clone()
                    } else {
                        row.request_id.clone()
                    }
                });
            let mut progress = ObservedRequest::new(live_prompt_id.clone(), started_at, false);
            if let Some(content) = row.content.as_deref().filter(|value| !value.is_empty()) {
                projections.session_updates().send(session_id, |event_id, total_tokens| {
                    Ok(super::projection::session_notification_for_method(SESSION_UPDATE_METHOD, session_id,
                        json!({"sessionUpdate":"user_message_chunk", "content":{"type":"text", "text":content},
                            "_meta":{"hideFromScrollback":row.runtime_source_kind.is_some()}}),
                        super::projection::stamp_update_meta(event_id, total_tokens, Some(&prompt_id), Some(true),
                            UpdateTimestamps { agent_timestamp_ms: Some(started_at), ..Default::default() }),
                    ))
                }, PromptSenderLine(&replay)).await?;
            }
            // Read terminal state before flushing, as in live delivery:
            // completion must never overtake final persisted output.
            let terminal = self.request_stop_reason(&row.request_id).await?;
            self.stream_projection_updates(
                session_id,
                &row.request_id,
                Some(&prompt_id),
                &replay,
                projections,
                &progress.cursor,
                &mut progress.timing,
                true,
                0,
            )
            .await?;
            if let Some(reason) = terminal {
                projections.session_updates().send(session_id, |event_id, total_tokens| {
                    Ok(super::projection::session_notification_for_method("x.ai/session_notification", session_id,
                        json!({"sessionUpdate":"turn_completed", "prompt_id":prompt_id, "stop_reason":reason.wire_name()}),
                        super::projection::stamp_update_meta(event_id, total_tokens, Some(&prompt_id), Some(true), UpdateTimestamps::default()),
                    ))
                }, PromptSenderLine(&replay)).await?;
                progress.completion_sent = true;
                progress.prompt_id = None;
            } else if running.is_none() {
                running = Some((row.request_id.clone(), live_prompt_id));
            }
            self.observed.lock().await.insert(
                (session_id.to_owned(), row.request_id.clone()),
                Arc::new(Mutex::new(progress)),
            );
        }
        if let Some((request_id, _)) = &running {
            self.autonomous_delivery
                .lock()
                .await
                .insert(session_id.to_owned(), request_id.clone());
        }
        Ok(running.map(|(_, prompt)| prompt))
    }

    async fn observe_session_tick(
        &self,
        session_id: &str,
        attached_at: &str,
        after: &mut String,
        delivery_after: &mut String,
        sender: &PromptSender,
        projections: &ProjectionEngine,
    ) -> Result<()> {
        const PAGE_SIZE: usize = 128;
        // Request submission persists second-precision timestamps. An
        // attachment with fractional seconds must include its whole second
        // or it can miss a request submitted later in that same second.
        let attached_at = chrono::DateTime::parse_from_rfc3339(attached_at)?
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        // Session IDs are grouping keys, not authorization. Root sessions
        // use the bound principal as requester, just like submission and
        // AgentSession creation; foreign rows must never become UI turns.
        let principal = escape_graphql_string(&self.config.agent_did);
        let query = format!(
            r#"{{ AgentRequest(filter: {{ session_id: {{ _eq: "{}" }},
                agent_did: {{ _eq: "{principal}" }}, requester_did: {{ _eq: "{principal}" }},
                created_at: {{ _gte: "{}" }}, request_id: {{ _gt: "{}" }} }},
                order: {{ request_id: ASC }}, limit: {PAGE_SIZE}) {{ request_id created_at }} }}"#,
            escape_graphql_string(session_id),
            escape_graphql_string(&attached_at),
            escape_graphql_string(after),
        );
        let response = self.node.execute(&query).await;
        ensure_no_errors(&response, "Grok shim session request discovery")?;
        let rows = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(Value::as_array)
            .context("missing session request discovery rows")?;
        for row in rows {
            let request_id = row
                .get("request_id")
                .and_then(Value::as_str)
                .context("session request missing request_id")?;
            // Submission installs its pending entry before the DB mutation.
            // Keep this check and registration in one critical section: even
            // a request whose ID has not returned yet cannot be double-owned.
            let state = self.state.lock().await;
            if state.entries.iter().any(|((session, _), pending)| {
                session == session_id
                    && pending
                        .request_id
                        .as_deref()
                        .is_none_or(|id| id == request_id)
            }) {
                continue;
            }
            let started_at = row
                .get("created_at")
                .and_then(Value::as_str)
                .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
                .map(|value| value.timestamp_millis())
                .unwrap_or(0);
            self.observed
                .lock()
                .await
                .entry((session_id.to_owned(), request_id.to_owned()))
                .or_insert_with(|| {
                    let mut progress = ObservedRequest::new(
                        format!("notifications-{request_id}"),
                        started_at,
                        false,
                    );
                    progress.echo_sent = false;
                    Arc::new(Mutex::new(progress))
                });
        }
        *after = if rows.len() == PAGE_SIZE {
            rows.last()
                .and_then(|row| row.get("request_id"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        } else {
            String::new()
        };

        // Retain cursors for the connection lifetime: a durable wake notice
        // may be appended even after its tool and parent have terminalized.
        // Bound database work per tick while sweeping those cursors fairly.
        const DELIVERY_PAGE_SIZE: usize = 32;
        let mut candidates: Vec<_> = self
            .observed
            .lock()
            .await
            .iter()
            .filter(|((session, _), _)| session == session_id)
            .map(|((_, id), progress)| {
                let autonomous = progress
                    .try_lock()
                    .map(|p| p.prompt_id.is_some())
                    .unwrap_or(false);
                (
                    format!("{}:{id}", u8::from(autonomous)),
                    id.clone(),
                    progress.clone(),
                )
            })
            .collect();
        candidates.sort_by(|a, b| a.0.cmp(&b.0));
        candidates.retain(|(key, _, _)| key > &*delivery_after);
        candidates.truncate(DELIVERY_PAGE_SIZE);
        *delivery_after = if candidates.len() == DELIVERY_PAGE_SIZE {
            candidates
                .last()
                .map(|(key, _, _)| key.clone())
                .unwrap_or_default()
        } else {
            String::new()
        };
        // Sweep all root notices before admitting the next autonomous turn,
        // including sessions larger than one delivery page.
        let mut requests: Vec<_> = candidates
            .into_iter()
            .map(|(_, id, progress)| (id, progress))
            .collect();
        if let Some(active) = self
            .autonomous_delivery
            .lock()
            .await
            .get(session_id)
            .cloned()
        {
            if !requests.iter().any(|(id, _)| id == &active) {
                if let Some(progress) = self
                    .observed
                    .lock()
                    .await
                    .get(&(session_id.to_owned(), active.clone()))
                    .cloned()
                {
                    requests.push((active, progress));
                }
            }
        }
        for (request_id, progress) in requests {
            let _delivery = self.delivery_gate.lock().await;
            let foreground_busy = self
                .foreground_deliveries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(session_id);
            // The foreground owns this lock through its final flush. Never
            // wait behind it: other completed requests may have late events.
            let Ok(mut progress) = progress.try_lock() else {
                continue;
            };
            // The pager treats a user notification as a turn boundary and
            // accepts autonomous prompts only while idle. Leave all cursors
            // untouched while a foreground request owns the session.
            let request_busy = {
                let state = self.state.lock().await;
                if state.closed {
                    continue;
                }
                state
                    .entries
                    .keys()
                    .any(|(session, _)| session == session_id)
            };
            let active = self
                .autonomous_delivery
                .lock()
                .await
                .get(session_id)
                .cloned();
            if foreground_busy
                || request_busy
                || active.as_deref().is_some_and(|id| id != request_id)
            {
                let ObservedRequest {
                    prompt_id,
                    cursor,
                    timing,
                    ..
                } = &mut *progress;
                self.stream_projection_updates_mode(
                    session_id,
                    &request_id,
                    prompt_id.as_deref(),
                    sender,
                    projections,
                    cursor,
                    timing,
                    false,
                    0,
                    true,
                )
                .await?;
                continue;
            }
            let continuing_autonomous = active.is_some();
            // Install the cancellation target before any visible echo can
            // let a viewer adopt (and immediately cancel) this request.
            if progress.prompt_id.is_some() && !continuing_autonomous {
                self.autonomous_delivery
                    .lock()
                    .await
                    .insert(session_id.to_owned(), request_id.clone());
            }
            if !progress.echo_sent {
                if let Some((prompt_id, content)) =
                    self.observed_human_prompt(session_id, &request_id).await?
                {
                    projections.session_updates().send(session_id, |event_id, total_tokens| {
                        Ok(super::projection::session_notification_for_method(SESSION_UPDATE_METHOD, session_id,
                            json!({"sessionUpdate":"user_message_chunk", "content":{"type":"text","text":content},
                                "_meta":{"hideFromScrollback":false,"promptIndex":0}}),
                            super::projection::stamp_update_meta(event_id, total_tokens, Some(&prompt_id), Some(false),
                                UpdateTimestamps { agent_timestamp_ms:Some(progress.timing.turn_start_ms),
                                    turn_start_ms:Some(progress.timing.turn_start_ms), ..Default::default() }),
                        ))
                    }, PromptSenderLine(sender)).await?;
                    progress.prompt_id = Some(prompt_id);
                }
                progress.echo_sent = true;
            }
            // Read terminal state first, then flush. Completion must never
            // overtake the final content persisted before terminalization.
            let terminal = self.request_stop_reason(&request_id).await?;
            let ObservedRequest {
                prompt_id,
                cursor,
                timing,
                ..
            } = &mut *progress;
            self.stream_projection_updates(
                session_id,
                &request_id,
                prompt_id.as_deref(),
                sender,
                projections,
                cursor,
                timing,
                !continuing_autonomous,
                0,
            )
            .await?;
            if !progress.completion_sent {
                if let Some(reason) = terminal {
                    let prompt_id = progress
                        .prompt_id
                        .clone()
                        .context("missing autonomous prompt ID")?;
                    projections.session_updates().send(session_id, move |event_id, total_tokens| {
                        Ok(super::projection::session_notification_for_method(
                            "x.ai/session_notification", session_id,
                            json!({"sessionUpdate": "turn_completed", "prompt_id": prompt_id,
                                "stop_reason": reason.wire_name()}),
                            super::projection::stamp_update_meta(event_id, total_tokens,
                                Some(&prompt_id), None, UpdateTimestamps::default()),
                        ))
                    }, PromptSenderLine(sender)).await?;
                    progress.completion_sent = true;
                    progress.prompt_id = None;
                    self.autonomous_delivery.lock().await.remove(session_id);
                    // The next tick first delivers deferred notices, then
                    // admits the next wake. Do not jump over that boundary.
                    delivery_after.clear();
                    break;
                }
            }
        }
        Ok(())
    }

    /// Handle a `session/prompt` request.
    ///
    /// Returns the deferred `session/prompt` result value (`stopReason`).
    /// The ACP service wraps it in the JSON-RPC response envelope.
    ///
    /// `projections` is the connection's projection engine; it is passed per
    /// prompt (rather than held by the manager) so the assembly slice
    /// constructs the `TurnManager` and `ProjectionEngine` independently and
    /// the two are joined exactly where the live turn needs them. A
    /// `Buffer`-sender headless turn still runs the same projection pass —
    /// the poll is what streams novel durable events, and the sender only
    /// decides where they go — so there is no code path that skips
    /// projection semantics.
    pub(super) async fn handle_prompt(
        &self,
        request: PromptRequest,
        sender: &PromptSender,
        projections: &ProjectionEngine,
    ) -> Result<Value> {
        if request.prompt.is_empty() {
            anyhow::bail!("session/prompt requires at least one prompt block");
        }
        let turn_start_ms = chrono::Utc::now().timestamp_millis();
        let prompt_id = request
            .prompt_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let key = (request.session_id.clone(), prompt_id.clone());
        let (response_tx, response_rx) = oneshot::channel::<Result<Value>>();
        let cancel_before_id = Arc::new(Mutex::new(CancelBeforeIdLatch::default()));
        #[cfg(test)]
        {
            // Test seam: park deterministically right before the insertion
            // critical section so a concurrent disconnect can be observed in
            // the exact race window that used to leak requests.
            let gate = self.insertion_gate.lock().await.clone();
            if let Some(gate) = gate {
                gate.arrived.notify_one();
                gate.release.notified().await;
            }
        }
        {
            let _admission = self.delivery_gate.lock().await;
            let mut state = self.state.lock().await;
            if state.is_closed() {
                // The connection already disconnected. Reject before any
                // durable submission: the disconnected connection must never
                // mint another request.
                anyhow::bail!("connection already disconnected");
            }
            if state
                .entries
                .keys()
                .any(|(session, _)| session == &request.session_id)
            {
                anyhow::bail!("session already has a live prompt");
            }
            state.entries.insert(
                key.clone(),
                PendingPrompt {
                    response_tx: Some(response_tx),
                    request_id: None,
                    cancel_before_id: cancel_before_id.clone(),
                    drained: false,
                },
            );
        }

        // Submit the durable request first. The returned request id is
        // registered on the pending entry *before* the first fallible
        // outbound send (the user echo below), so a send failure after
        // submission interrupts the request rather than leaking it.
        tracing::debug!(session_id = %request.session_id, prompt_id = %prompt_id, "Grok shim submitting prompt request");
        let progress = Arc::new(Mutex::new(ObservedRequest::new(
            prompt_id.clone(),
            turn_start_ms,
            true,
        )));
        let mut progress_guard = progress.lock().await;
        let submission = self.submit_request(&request, &prompt_id).await;
        let request_id = match submission {
            Ok(request_id) => {
                let registered_own_entry = {
                    let mut state = self.state.lock().await;
                    match state.entries.get_mut(&key) {
                        Some(entry) if Arc::ptr_eq(&entry.cancel_before_id, &cancel_before_id) => {
                            entry.request_id = Some(request_id.clone());
                            // Install delivery ownership before cancellation
                            // can remove the pending entry or the first echo
                            // can yield. Discovery checks under this same lock.
                            self.observed.lock().await.insert(
                                (request.session_id.clone(), request_id.clone()),
                                progress.clone(),
                            );
                            true
                        }
                        _ => false,
                    }
                };
                let cancelled_before_id = cancel_before_id.lock().await.is_cancelled();
                if !registered_own_entry || cancelled_before_id {
                    // The original entry was drained while submission was in
                    // flight. A later prompt may already have reused the same
                    // peer-supplied (session, promptId) key, so entry presence
                    // alone is not proof that this submission still owns it.
                    // The per-instance latch/Arc identity is the generation
                    // token: never stamp or stream through a replacement.
                    self.interrupt_submitted(&request_id).await;
                    drop(response_rx);
                    tracing::info!(
                        session_id = %request.session_id,
                        prompt_id = %prompt_id,
                        request_id = %request_id,
                        registered_own_entry,
                        cancelled_before_id,
                        "Grok shim prompt was drained during submission; interrupting submitted request"
                    );
                    return Ok(json!({"stopReason": StopReason::Cancelled.wire_name()}));
                }
                request_id
            }
            Err(error) => {
                // Submission failed before any request id existed. The
                // classification must linearize with connection closure:
                // observe `closed` and remove/check the entry in the same
                // critical section, then combine that with the explicit
                // cancel-before-id latch. A disconnect that has already
                // published its closed+drained state must always resolve
                // cancelled — even when it has not published the drained
                // entry's latch yet — and so must an explicit cancel: while
                // `submit_request` is awaited, only cancel or disconnect can
                // remove this generation (every other removal runs in the
                // submitter's own task after the request id is known), so a
                // missing or replacement generation means this turn was
                // cancelled. Removal is guarded by the Arc generation token:
                // a peer may already have reused the exact key, and the old
                // failed submitter must never delete that replacement.
                // Ordinary
                // non-disconnect GraphQL failures (state open, entry present,
                // latch unset) stay surfaced as errors.
                let (own_entry, state_closed) =
                    self.take_entry_if_generation(&key, &cancel_before_id).await;
                let own_entry_was_present = own_entry.is_some();
                let cancelled_by_latch = cancel_before_id.lock().await.is_cancelled();
                let cancelled_by_disconnect = state_closed && !own_entry_was_present;
                let cancelled_by_drain = !own_entry_was_present;
                drop(response_rx);
                if cancelled_by_disconnect || cancelled_by_drain || cancelled_by_latch {
                    tracing::info!(
                        %error,
                        session_id = %request.session_id,
                        prompt_id = %prompt_id,
                        state_closed,
                        own_entry_was_present,
                        cancelled_by_latch,
                        "Grok shim prompt submission failed into a cancelled/disconnected turn; resolving cancelled"
                    );
                    return Ok(json!({"stopReason": StopReason::Cancelled.wire_name()}));
                }
                tracing::warn!(
                    %error,
                    session_id = %request.session_id,
                    prompt_id = %prompt_id,
                    "Grok shim prompt submission failed"
                );
                return Err(error);
            }
        };

        // First fallible outbound send after registration: the user echo. If
        // cancel/disconnect drained the entry in the meantime, or the send
        // itself fails, interrupt immediately and resolve cancelled.
        let drained_during_submission = {
            let state = self.state.lock().await;
            state.entries.get(&key).is_none_or(|entry| {
                !Arc::ptr_eq(&entry.cancel_before_id, &cancel_before_id) || entry.drained
            })
        };
        if drained_during_submission {
            self.interrupt_submitted(&request_id).await;
            drop(response_rx);
            tracing::info!(
                session_id = %request.session_id,
                prompt_id = %prompt_id,
                request_id = %request_id,
                "Grok shim prompt drained before user echo; interrupting submitted request"
            );
            return Ok(json!({"stopReason": StopReason::Cancelled.wire_name()}));
        }
        for (index, block) in request.prompt.iter().enumerate() {
            // The echo rides the connection's common session-update send
            // path: the per-session lock, the reserve/send/commit envelope,
            // and the shared event-id space are all owned by that path, so
            // a failed echo never consumes an id and can never reorder
            // against a concurrent session update on the same session.
            if let Err(error) = sender
                .send_user_message_chunk(
                    projections.session_updates(),
                    &request.session_id,
                    &prompt_id,
                    block,
                    index,
                )
                .await
            {
                // Send failure after submission: interrupt the durable
                // request, drain the entry, and surface the failure. The
                // common send path already rolled the reservation back, so
                // the failed echo consumed no event id.
                self.interrupt_and_drain(&key, &request_id, &cancel_before_id)
                    .await;
                drop(response_rx);
                tracing::warn!(
                    %error,
                    session_id = %request.session_id,
                    prompt_id = %prompt_id,
                    request_id = %request_id,
                    "Grok shim user echo send failed; interrupted submitted request"
                );
                return Err(error);
            }
        }

        // Deferred response: watch the durable request until it terminalizes
        // or the pending entry is drained by cancel/disconnect. Each watch
        // cycle runs the durable projection pass first, so novel
        // tool/subagent/message updates stream live and the final pass
        // precedes the terminal response.
        let outcome = self
            .watch_terminal(
                &key,
                &request_id,
                &request.session_id,
                &prompt_id,
                &cancel_before_id,
                sender,
                projections,
                &mut progress_guard,
                response_rx,
            )
            .await;
        // Transfer ownership while holding the delivery lock. Late updates
        // refine existing tool/child cards without opening another prompt;
        // actual durable continuation requests receive their own identity.
        let outcome = outcome?;
        Ok(json!({"stopReason": outcome.wire_name()}))
    }

    /// Re-read immutable submission data for an observed human turn. No
    /// prompt/content cache or request ownership is created by the viewer.
    async fn observed_human_prompt(
        &self,
        session: &str,
        request: &str,
    ) -> Result<Option<(String, String)>> {
        let principal = escape_graphql_string(&self.config.agent_did);
        let query = format!(
            r#"{{AgentRequest(filter:{{request_id:{{_eq:"{}"}},session_id:{{_eq:"{}"}},
            agent_did:{{_eq:"{principal}"}},requester_did:{{_eq:"{principal}"}}}})
            {{metadata content runtime_source_kind}}}}"#,
            escape_graphql_string(request),
            escape_graphql_string(session)
        );
        let response = self.node.execute(&query).await;
        ensure_no_errors(&response, "read observed human prompt")?;
        let rows = response
            .data
            .as_ref()
            .and_then(|data| data["AgentRequest"].as_array())
            .context("missing observed request rows")?;
        anyhow::ensure!(
            rows.len() == 1,
            "observed request ownership is missing or ambiguous"
        );
        let row = &rows[0];
        if row["runtime_source_kind"].as_str().is_some() {
            return Ok(None);
        }
        let metadata = row["metadata"]
            .as_str()
            .and_then(|raw| serde_json::from_str::<Value>(raw).ok());
        let prompt_id = metadata
            .as_ref()
            .and_then(|meta| meta["promptId"].as_str())
            .filter(|value| !value.is_empty())
            .unwrap_or(request)
            .to_owned();
        Ok(Some((
            prompt_id,
            row["content"].as_str().unwrap_or_default().to_owned(),
        )))
    }

    /// Handle a `session/cancel` notification. Never sends a response.
    pub(super) async fn handle_cancel(&self, notification: CancelNotification) -> Result<()> {
        tracing::info!(
            session_id = %notification.session_id,
            cancel_subagents = notification.cancel_subagents,
            prompt_id = notification.prompt_id.as_deref().unwrap_or(""),
            "Grok shim received session/cancel"
        );
        let target_prompt_id = notification.prompt_id.clone();
        let targets: Vec<((String, String), Arc<Mutex<CancelBeforeIdLatch>>)> = {
            let state = self.state.lock().await;
            state
                .entries
                .iter()
                .filter(|((session, _), _)| session == &notification.session_id)
                .filter(|((_, prompt_id), _)| {
                    target_prompt_id
                        .as_deref()
                        .is_none_or(|expected| expected == prompt_id.as_str())
                })
                .map(|(key, entry)| (key.clone(), entry.cancel_before_id.clone()))
                .collect()
        };
        #[cfg(test)]
        {
            let gate = self.cancel_selection_gate.lock().await.clone();
            if let Some(gate) = gate {
                gate.arrived.notify_one();
                gate.release.notified().await;
            }
        }
        let had_foreground_target = !targets.is_empty();
        for (key, generation) in targets {
            let request_id = self
                .drain_entry(&key, &generation, StopReason::Cancelled)
                .await;
            if let Some(request_id) = request_id {
                self.interrupt_submitted(&request_id).await;
                if notification.cancel_subagents {
                    self.interrupt_child_requests(&request_id).await;
                }
            }
        }
        let autonomous_target = match target_prompt_id.as_deref() {
            Some(id) if id.starts_with("notifications-") => {
                id.strip_prefix("notifications-").map(str::to_owned)
            }
            Some(id) => {
                let active = self
                    .autonomous_delivery
                    .lock()
                    .await
                    .get(&notification.session_id)
                    .cloned();
                match active {
                    Some(request)
                        if self
                            .observed_human_prompt(&notification.session_id, &request)
                            .await?
                            .is_some_and(|(prompt, _)| prompt == id) =>
                    {
                        Some(request)
                    }
                    _ => None,
                }
            }
            None if !had_foreground_target => self
                .autonomous_delivery
                .lock()
                .await
                .get(&notification.session_id)
                .cloned(),
            None => None,
        };
        if let Some(request_id) = autonomous_target.as_deref() {
            // Autonomous prompt identities are transport projections of a
            // durable request already observed in this session. Cancellation
            // uses the runtime's ordinary interrupt transition, never cursor
            // state, and cannot block behind an outbound projection send.
            let observed = self
                .observed
                .lock()
                .await
                .contains_key(&(notification.session_id.clone(), request_id.to_owned()));
            if observed && self.request_stop_reason(request_id).await?.is_none() {
                self.interrupt_submitted(request_id).await;
                if notification.cancel_subagents {
                    self.interrupt_child_requests(request_id).await;
                }
            }
        }
        Ok(())
    }

    /// Handle connection teardown: atomically latch `closed` and drain every
    /// pending prompt under the same lock, then interrupt their submitted
    /// requests outside it. No response is ever sent (the channel is gone);
    /// the deferred response is resolved so a concurrently awaited prompt
    /// future observes the drain. The latch makes duplicate/concurrent
    /// disconnects idempotent: the second one finds no entries (the first
    /// already drained them) and a closed state, so it does nothing, and any
    /// prompt that arrives after the first disconnect is rejected before it
    /// can insert or submit a durable request.
    pub(super) async fn handle_disconnect(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        state.closed = true;
        let drained: Vec<(
            (String, String),
            Arc<Mutex<CancelBeforeIdLatch>>,
            Option<String>,
        )> = std::mem::take(&mut state.entries)
            .into_iter()
            .map(|(key, mut entry)| {
                entry.resolve(Ok(json!({
                    "stopReason": StopReason::Cancelled.wire_name(),
                })));
                (
                    key,
                    entry.cancel_before_id.clone(),
                    entry.request_id.clone(),
                )
            })
            .collect();
        drop(state);
        let observers = std::mem::take(&mut *self.observers.lock().await);
        for observer in observers.values() {
            observer.abort();
        }
        for (_, observer) in observers {
            let _ = observer.await;
        }
        #[cfg(test)]
        {
            // Test seam: park deterministically after the closed+drained
            // state is published but before the drained entries are
            // latched, so a concurrent submission failure can be observed
            // in the exact window where the disconnect has not published
            // the cancel-before-id latch yet.
            let gate = self.disconnect_gate.lock().await.clone();
            if let Some(gate) = gate {
                gate.arrived.notify_one();
                gate.release.notified().await;
            }
        }
        for ((_session_id, prompt_id), latch, request_id) in drained {
            // Latch the cancel-before-id window for submitters that are still
            // inside `create_agent_request` and have not registered a request
            // id yet; they observe the latch and resolve cancelled.
            let _first_cancel = latch.lock().await.cancel();
            if let Some(request_id) = request_id {
                if let Err(error) = gents::interrupt_request(self.node.as_ref(), &request_id).await
                {
                    tracing::warn!(
                        %error,
                        prompt_id = %prompt_id,
                        request_id = %request_id,
                        "Grok shim failed to interrupt request after disconnect"
                    );
                }
            }
        }
        Ok(())
    }

    /// Drain one pending entry only when it still belongs to the generation
    /// selected by the caller: resolve its deferred response with the given
    /// stop reason and return the submitted request id (if any) so the caller
    /// can interrupt it. A cancel that fires before the request id is known
    /// still latches `cancel_before_id`, which the submitter observes.
    async fn drain_entry(
        &self,
        key: &(String, String),
        generation: &Arc<Mutex<CancelBeforeIdLatch>>,
        stop_reason: StopReason,
    ) -> Option<String> {
        let (entry, _) = self.take_entry_if_generation(key, generation).await;
        let mut entry = entry?;
        #[cfg(test)]
        {
            // Test seam: park deterministically after the entry is removed
            // from the connection state but before its cancel-before-id
            // latch is set, so a concurrently failing submission can be
            // observed in the exact race window where the entry is gone and
            // the latch is not published yet.
            let gate = self.cancel_drain_gate.lock().await.clone();
            if let Some(gate) = gate {
                gate.arrived.notify_one();
                gate.release.notified().await;
            }
        }
        let _first_cancel = entry.cancel_before_id.lock().await.cancel();
        entry.resolve(Ok(json!({
            "stopReason": stop_reason.wire_name(),
        })));
        entry.request_id.clone()
    }

    /// Interrupt the submitted request and drain only its pending generation.
    async fn interrupt_and_drain(
        &self,
        key: &(String, String),
        request_id: &str,
        generation: &Arc<Mutex<CancelBeforeIdLatch>>,
    ) {
        if let (Some(mut entry), _) = self.take_entry_if_generation(key, generation).await {
            let _first_cancel = entry.cancel_before_id.lock().await.cancel();
            entry.resolve(Ok(json!({
                "stopReason": StopReason::Cancelled.wire_name(),
            })));
        }
        self.interrupt_submitted(request_id).await;
    }

    /// Atomically take only the caller's pending-entry generation and observe
    /// the connection latch under the same state lock. A peer can reuse an
    /// exact `(sessionId, promptId)` key immediately after cancel, so key
    /// equality alone is never authority for submitter-owned cleanup.
    async fn take_entry_if_generation(
        &self,
        key: &(String, String),
        generation: &Arc<Mutex<CancelBeforeIdLatch>>,
    ) -> (Option<PendingPrompt>, bool) {
        let mut state = self.state.lock().await;
        let owns_current_generation = state
            .entries
            .get(key)
            .is_some_and(|entry| Arc::ptr_eq(&entry.cancel_before_id, generation));
        let entry = if owns_current_generation {
            state.entries.remove(key)
        } else {
            None
        };
        (entry, state.is_closed())
    }

    async fn interrupt_submitted(&self, request_id: &str) {
        if let Err(error) = gents::interrupt_request(self.node.as_ref(), request_id).await {
            tracing::warn!(
                %error,
                request_id,
                "Grok shim failed to interrupt submitted request"
            );
        }
    }

    /// Interrupt runtime child `AgentRequest` rows linked to the parent by
    /// `caused_by_parent_request_id`. Static `Task` rows are never queried or
    /// mutated as runtime state.
    async fn interrupt_child_requests(&self, parent_request_id: &str) {
        let escaped_parent = escape_graphql_string(parent_request_id);
        let query = format!(
            r#"{{
                AgentRequest(
                    filter: {{
                        caused_by_parent_request_id: {{ _eq: "{escaped_parent}" }}
                    }}
                ) {{
                    request_id
                    lifecycle_state
                }}
            }}"#
        );
        let response = self.node.execute(&query).await;
        if let Err(error) = ensure_no_errors(&response, "grok shim child request query") {
            tracing::warn!(
                %error,
                parent_request_id,
                "Grok shim failed to load child requests for cancelSubagents"
            );
            return;
        }
        let rows = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for row in rows {
            let Some(request_id) = row.get("request_id").and_then(Value::as_str) else {
                continue;
            };
            let lifecycle_state = row
                .get("lifecycle_state")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if is_terminal_lifecycle_state(lifecycle_state) {
                continue;
            }
            if let Err(error) = gents::interrupt_request(self.node.as_ref(), request_id).await {
                tracing::warn!(
                    %error,
                    request_id,
                    parent_request_id,
                    "Grok shim failed to interrupt child request after cancelSubagents"
                );
            }
        }
    }

    /// Submit the durable Gents request for the prompt and return its request
    /// id. The caller registers the id on the pending entry before the first
    /// fallible outbound send.
    async fn submit_request(&self, request: &PromptRequest, prompt_id: &str) -> Result<String> {
        let content = prompt_text(request);
        let mut metadata = json!({
            "promptId": prompt_id,
        });
        if let Some(screen_mode) = request.screen_mode.as_deref() {
            metadata["screenMode"] = json!(screen_mode);
        }
        if request.send_now {
            metadata["sendNow"] = json!(true);
        }
        let stable_request_id = uuid::Uuid::new_v4().to_string();
        let options = crate::RequestSubmitOptions {
            metadata: Some(metadata.to_string()),
            ..Default::default()
        };
        let submitted = if let Some(super::goals::GoalCommand::Create {
            objective,
            token_budget,
        }) = super::goals::GoalCommand::from_prompt(request)?
        {
            crate::create_goal_backed_agent_request_local(
                &self.node,
                self.config.graphql.as_ref(),
                &self.config.agent_did,
                &objective,
                token_budget,
                &request.session_id,
                &self.config.behavior_id,
                stable_request_id.clone(),
                options,
            )
            .await
        } else {
            crate::create_agent_request_retrying_transient(
                self.config.graphql.as_ref(),
                self.config.agent_did.as_str(),
                &content,
                Some(request.session_id.as_str()),
                Some(self.config.behavior_id.as_str()),
                stable_request_id.clone(),
                options,
            )
            .await
        };
        let submitted = match submitted {
            Ok(submitted) => submitted,
            Err(error) => {
                self.interrupt_submitted(&stable_request_id).await;
                return Err(error);
            }
        };
        Ok(submitted.request_id)
    }

    /// Watch the durable request until it terminalizes or the pending entry
    /// is drained by cancel/disconnect. The drain resolves `response_rx`
    /// first, so the watch returns `cancelled` without another poll.
    ///
    /// Each cycle first polls the request's terminal state, then runs the
    /// durable projection pass and streams every novel event live (tools,
    /// then subagents, then messages), and only then sleeps. When the
    /// request has terminalized, the final projection pass still runs —
    /// and its sends complete — before the pending entry is removed and the
    /// terminal `stopReason` response is produced, so the pager observes
    /// the full stream strictly before the response.
    ///
    /// Outbound failures and non-transient read failures interrupt and drain
    /// the submitted request. DefraDB contention errors from the terminal or
    /// projection reads receive a bounded number of consecutive retries; a
    /// complete successful poll resets that budget, so an otherwise healthy
    /// request is never interrupted by an isolated read conflict.
    #[allow(clippy::too_many_arguments)]
    async fn watch_terminal(
        &self,
        key: &(String, String),
        request_id: &str,
        session_id: &str,
        prompt_id: &str,
        generation: &Arc<Mutex<CancelBeforeIdLatch>>,
        sender: &PromptSender,
        projections: &ProjectionEngine,
        progress: &mut ObservedRequest,
        mut response_rx: oneshot::Receiver<Result<Value>>,
    ) -> Result<StopReason> {
        // Request-local token-observation high-water: one per pending
        // request, so sequential requests accumulate per-request deltas into
        // the session total without double-counting and a retry-replaced
        // (smaller) observation can never decrease it.
        let ObservedRequest {
            cursor,
            timing: update_timing,
            ..
        } = progress;
        let mut consecutive_transient_read_failures = 0usize;
        loop {
            // A cancel/disconnect that drained the entry resolves the
            // response before (or between) terminalization polls.
            if let Ok(result) = response_rx.try_recv() {
                // The drain always resolves `cancelled` today; keep the
                // branch total in case future drains resolve other reasons.
                let value = result?;
                let stop_reason = value
                    .get("stopReason")
                    .and_then(Value::as_str)
                    .unwrap_or(StopReason::Cancelled.wire_name());
                return Ok(stop_reason_from_wire(stop_reason));
            }
            let terminal = match self.request_stop_reason(request_id).await {
                Ok(terminal) => terminal,
                Err(error)
                    if register_transient_read_retry(
                        &error,
                        &mut consecutive_transient_read_failures,
                    ) =>
                {
                    tracing::warn!(
                        %error,
                        session_id,
                        prompt_id,
                        request_id,
                        retry = consecutive_transient_read_failures,
                        "retrying transient Grok shim terminal query failure"
                    );
                    if let Some(stop_reason) = wait_for_retry_or_cancel(
                        &mut response_rx,
                        transient_graphql_retry_delay(consecutive_transient_read_failures),
                    )
                    .await?
                    {
                        return Ok(stop_reason);
                    }
                    continue;
                }
                Err(error) => {
                    // A terminal query failure after submission must not
                    // leak the submitted request.
                    self.interrupt_and_drain(key, request_id, generation).await;
                    drop(response_rx);
                    tracing::warn!(
                        %error,
                        session_id,
                        prompt_id,
                        request_id,
                        "Grok shim terminal query failed; interrupted submitted request"
                    );
                    return Err(error);
                }
            };
            // The projection pass runs every cycle, including the terminal
            // one: novel durable events stream live, and the final pass
            // flushes everything before the response below.
            match self
                .stream_projection_updates(
                    session_id,
                    request_id,
                    Some(prompt_id),
                    sender,
                    projections,
                    cursor,
                    update_timing,
                    false,
                    0,
                )
                .await
            {
                Ok(()) => {}
                Err(error)
                    if register_transient_read_retry(
                        &error,
                        &mut consecutive_transient_read_failures,
                    ) =>
                {
                    tracing::warn!(
                        %error,
                        session_id,
                        prompt_id,
                        request_id,
                        retry = consecutive_transient_read_failures,
                        "retrying transient Grok shim projection read failure"
                    );
                    if let Some(stop_reason) = wait_for_retry_or_cancel(
                        &mut response_rx,
                        transient_graphql_retry_delay(consecutive_transient_read_failures),
                    )
                    .await?
                    {
                        return Ok(stop_reason);
                    }
                    continue;
                }
                Err(error) => {
                    // A send failure or projection query failure after
                    // submission must not leak the submitted request.
                    self.interrupt_and_drain(key, request_id, generation).await;
                    drop(response_rx);
                    tracing::warn!(
                        %error,
                        session_id,
                        prompt_id,
                        request_id,
                        "Grok shim live projection failed; interrupted submitted request"
                    );
                    return Err(error);
                }
            }
            consecutive_transient_read_failures = 0;
            if let Some(stop_reason) = terminal {
                // Terminalized: the final projection pass above already
                // flushed the stream. Remove the pending entry; the response
                // value is built by the caller from the returned stop reason.
                self.take_entry_if_generation(key, generation).await;
                return Ok(stop_reason);
            }
            tokio::select! {
                _ = tokio::time::sleep(TERMINAL_POLL_INTERVAL) => {}
                result = &mut response_rx => {
                    if let Ok(result) = result {
                        let value = result?;
                        let stop_reason = value
                            .get("stopReason")
                            .and_then(Value::as_str)
                            .unwrap_or(StopReason::Cancelled.wire_name());
                        return Ok(stop_reason_from_wire(stop_reason));
                    }
                    // Sender dropped without resolving: treat as cancelled.
                    return Ok(StopReason::Cancelled);
                }
            }
        }
    }

    /// Run one durable projection pass and stream every novel event through
    /// the connection's common session-update send path. Each send holds the
    /// session's send lock across reserve → stamp → send → commit, so the
    /// event id is consumed only by a successful enqueue. The cursor
    /// likewise advances only after each line was successfully sent, so a
    /// send failure replays the same events on the next poll and never
    /// duplicates an id.
    async fn stream_projection_updates(
        &self,
        session_id: &str,
        request_id: &str,
        prompt_id: Option<&str>,
        sender: &PromptSender,
        projections: &ProjectionEngine,
        cursor: &tokio::sync::Mutex<RequestCursor>,
        update_timing: &mut RequestUpdateTiming,
        include_notifications: bool,
        descendant_depth: usize,
    ) -> Result<()> {
        self.stream_projection_updates_mode(
            session_id,
            request_id,
            prompt_id,
            sender,
            projections,
            cursor,
            update_timing,
            include_notifications,
            descendant_depth,
            false,
        )
        .await
    }

    /// Background activity is independent of parent conversation delivery.
    /// Suppressed text and its cursor advances stay pending until that lane
    /// is free; children retain their own session-scoped delivery cursors.
    async fn stream_projection_updates_mode(
        &self,
        session_id: &str,
        request_id: &str,
        prompt_id: Option<&str>,
        sender: &PromptSender,
        projections: &ProjectionEngine,
        cursor: &tokio::sync::Mutex<RequestCursor>,
        update_timing: &mut RequestUpdateTiming,
        include_notifications: bool,
        descendant_depth: usize,
        activity_only: bool,
    ) -> Result<()> {
        // An old prompt ID on parent activity makes the stock pager adopt
        // that old turn as a viewer. Activity has no conversation ownership.
        let prompt_id = if activity_only { None } else { prompt_id };
        let batch = {
            let mut cursor = cursor.lock().await;
            projections
                .project_request_updates(session_id, request_id, &mut cursor)
                .await?
        };
        let mut deferred_notifications = false;
        let mut child_finishes = Vec::new();
        for event in batch.events {
            let event = if descendant_depth > 0 {
                cursor.lock().await.child_output_event(event)
            } else {
                event
            };
            if activity_only && !is_background_activity(&event.payload) {
                continue;
            }
            if event.payload.get("sessionUpdate").and_then(Value::as_str)
                == Some("subagent_finished")
            {
                child_finishes.push(event);
                continue;
            }
            if !include_notifications
                && event.payload.get("sessionUpdate").and_then(Value::as_str)
                    == Some("user_message_chunk")
            {
                // Do not commit this advance: the session observer delivers
                // the notice after the foreground RPC has finished.
                deferred_notifications = true;
                continue;
            }
            self.send_projection_event(
                session_id,
                prompt_id,
                event,
                sender,
                projections,
                cursor,
                update_timing,
                activity_only,
            )
            .await?;
        }
        // The pager creates child panes on spawned and finalizes them on
        // finished. Deliver their transcript/tools between those boundaries.
        let deferred_children = self
            .stream_readable_child_updates(
                session_id,
                request_id,
                sender,
                projections,
                cursor,
                descendant_depth,
            )
            .await?;
        for event in child_finishes {
            if event
                .payload
                .get("child_session_id")
                .and_then(Value::as_str)
                .is_some_and(|session| deferred_children.contains(session))
            {
                continue;
            }
            self.send_projection_event(
                session_id,
                prompt_id,
                event,
                sender,
                projections,
                cursor,
                update_timing,
                activity_only,
            )
            .await?;
        }
        // No-wire observations (tail resets/no-op commits) form the suffix
        // of the same delivery prefix. They become durable cursor state only
        // after every byte-carrying event above was enqueued successfully.
        if !activity_only && !batch.trailing_advances.is_empty() {
            let mut cursor = cursor.lock().await;
            for advance in batch.trailing_advances {
                if deferred_notifications
                    && matches!(advance, CursorAdvance::MessageHighWater { .. })
                {
                    // Keep incremental discovery behind the undelivered
                    // notice, while other event cursors still deduplicate.
                    continue;
                }
                cursor.record(advance);
            }
        }
        Ok(())
    }

    async fn send_projection_event(
        &self,
        session_id: &str,
        prompt_id: Option<&str>,
        event: NovelProjectionEvent,
        sender: &PromptSender,
        projections: &ProjectionEngine,
        cursor: &Mutex<RequestCursor>,
        timing: &mut RequestUpdateTiming,
        activity_only: bool,
    ) -> Result<()> {
        let timestamps = if activity_only {
            UpdateTimestamps::default()
        } else {
            timing.resolve(event.timing.as_ref(), chrono::Utc::now().timestamp_millis())
        };
        let commit = ProjectionCursorCommit {
            cursor,
            advance: event.advance,
        };
        projections
            .session_updates()
            .send_with_commit(
                session_id,
                move |event_id, total_tokens| {
                    let meta = super::projection::stamp_update_meta(
                        event_id,
                        total_tokens,
                        prompt_id,
                        None,
                        timestamps,
                    );
                    Ok(super::projection::session_notification_for_method(
                        event.method,
                        session_id,
                        event.payload,
                        meta,
                    ))
                },
                PromptSenderLine(sender),
                commit,
            )
            .await?;
        Ok(())
    }

    async fn stream_readable_child_updates(
        &self,
        parent_session_id: &str,
        parent_request_id: &str,
        sender: &PromptSender,
        projections: &ProjectionEngine,
        parent_cursor: &Mutex<RequestCursor>,
        depth: usize,
    ) -> Result<BTreeSet<String>> {
        let mut deferred = BTreeSet::new();
        if depth >= gents::tool_call_lifecycle::MAX_SUBAGENT_DEPTH as usize {
            return Ok(deferred);
        }
        let mut query = gents::DescendantQuery::direct(parent_request_id);
        query.limit = gents::MAX_DESCENDANT_PAGE_LIMIT;
        loop {
            let page = gents::resolve_descendant_graph(
                gents::DescendantGraphAccess::Local(&self.node),
                &query,
            )
            .await?;
            for child in page.edges.into_iter().filter(|edge| edge.readable()) {
                let Some(session_id) = child.child_session_id.as_deref() else {
                    continue;
                };
                if session_id == parent_session_id
                    || !parent_cursor
                        .lock()
                        .await
                        .subagent_spawn_was_delivered(session_id)
                {
                    continue;
                }
                for (request_id, started_at) in self
                    .readable_child_session_requests(session_id, &child.child_request_id)
                    .await?
                {
                    let progress = self
                        .observed
                        .lock()
                        .await
                        .entry((session_id.to_owned(), request_id.clone()))
                        .or_insert_with(|| {
                            Arc::new(Mutex::new(ObservedRequest::new(
                                request_id.clone(),
                                started_at,
                                true,
                            )))
                        })
                        .clone();
                    // Ownership contention must defer the parent's finish too:
                    // another sender may still be flushing the child's tail.
                    let Ok(mut progress) = progress.try_lock() else {
                        deferred.insert(session_id.to_owned());
                        continue;
                    };
                    let terminal = self.request_stop_reason(&request_id).await?;
                    if terminal.is_none() {
                        deferred.insert(session_id.to_owned());
                    }
                    let ObservedRequest { cursor, timing, .. } = &mut *progress;
                    Box::pin(self.stream_projection_updates(
                        session_id,
                        &request_id,
                        None,
                        sender,
                        projections,
                        cursor,
                        timing,
                        terminal.is_some() || matches!(sender, PromptSender::Replay { .. }),
                        depth + 1,
                    ))
                    .await?;
                }
            }
            if !page.has_more {
                break;
            }
            query.after = page.next_cursor;
        }
        Ok(deferred)
    }

    /// A canonical edge authorizes its child session, but not foreign
    /// principal rows sharing that label. Followups must preserve the exact
    /// child agent/requester identity, including absent requester identity.
    async fn readable_child_session_requests(
        &self,
        session_id: &str,
        child_request_id: &str,
    ) -> Result<Vec<(String, i64)>> {
        let response = self
            .node
            .execute(&format!(
                r#"{{ AgentRequest(filter: {{ request_id: {{ _eq: "{}" }} }}, limit: 2)
            {{ request_id session_id agent_did requester_did }} }}"#,
                escape_graphql_string(child_request_id)
            ))
            .await;
        ensure_no_errors(&response, "load canonical child session principal")?;
        let rows = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(Value::as_array)
            .context("missing canonical child request rows")?;
        let [owner] = rows.as_slice() else {
            anyhow::bail!("canonical child request is missing or ambiguous")
        };
        let agent = owner
            .get("agent_did")
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty())
            .context("canonical child principal is missing")?;
        anyhow::ensure!(
            owner.get("session_id").and_then(Value::as_str) == Some(session_id),
            "canonical child session changed"
        );
        let requester = owner.get("requester_did").and_then(Value::as_str);
        let mut after = String::new();
        let mut requests = Vec::new();
        loop {
            let response = self.node.execute(&format!(r#"{{ AgentRequest(filter: {{ session_id: {{ _eq: "{}" }},
                agent_did: {{ _eq: "{}" }}, request_id: {{ _gt: "{}" }} }}, order: {{ request_id: ASC }}, limit: 128)
                {{ request_id requester_did created_at }} }}"#,
                escape_graphql_string(session_id), escape_graphql_string(agent), escape_graphql_string(&after))).await;
            ensure_no_errors(&response, "discover readable child session requests")?;
            let rows = response
                .data
                .as_ref()
                .and_then(|data| data.get("AgentRequest"))
                .and_then(Value::as_array)
                .context("missing child session request rows")?;
            for row in rows {
                if row.get("requester_did").and_then(Value::as_str) != requester {
                    continue;
                }
                let id = row
                    .get("request_id")
                    .and_then(Value::as_str)
                    .context("child session request missing ID")?;
                let started_at = row
                    .get("created_at")
                    .and_then(Value::as_str)
                    .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
                    .map(|value| value.timestamp_millis())
                    .unwrap_or(0);
                requests.push((id.to_owned(), started_at));
            }
            if rows.len() < 128 {
                break;
            }
            after = rows
                .last()
                .and_then(|row| row.get("request_id"))
                .and_then(Value::as_str)
                .context("child session discovery missing pagination ID")?
                .to_owned();
        }
        requests.sort_by(|a, b| (a.1, &a.0).cmp(&(b.1, &b.0)));
        Ok(requests)
    }

    /// Query the durable request's terminal state and project a `stopReason`.
    /// Returns `None` while the request is still non-terminal.
    async fn request_stop_reason(&self, request_id: &str) -> Result<Option<StopReason>> {
        let escaped_request_id = escape_graphql_string(request_id);
        let query = format!(
            r#"{{
                AgentRequest(
                    filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                    order: {{ created_at: DESC }},
                    limit: 1
                ) {{
                    request_id
                    lifecycle_state
                    interrupt_requested_at
                }}
                AgentResponse(
                    filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                    order: {{ created_at: DESC }},
                    limit: 1
                ) {{
                    request_id
                    status
                    interrupted_at
                }}
            }}"#
        );
        let response = self.node.execute(&query).await;
        ensure_no_errors(&response, "grok shim turn terminal query")?;
        let request_row = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(Value::as_array)
            .and_then(|rows| rows.first())
            .cloned()
            .unwrap_or(Value::Null);
        if request_row.is_null() {
            // The request row has not been durably observed yet.
            return Ok(None);
        }
        let response_row = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentResponse"))
            .and_then(Value::as_array)
            .and_then(|rows| rows.first())
            .cloned()
            .unwrap_or(Value::Null);
        let lifecycle_state = request_row
            .get("lifecycle_state")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let response_status = response_row
            .get("status")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let interrupted_at = response_row
            .get("interrupted_at")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        Ok(stop_reason_from_rows(
            lifecycle_state,
            response_status.as_deref(),
            interrupted_at.as_deref(),
        ))
    }
}

/// Wait between transient read attempts without delaying an explicit cancel
/// or disconnect that already resolved the pending turn.
async fn wait_for_retry_or_cancel(
    response_rx: &mut oneshot::Receiver<Result<Value>>,
    delay: Duration,
) -> Result<Option<StopReason>> {
    tokio::select! {
        _ = tokio::time::sleep(delay) => Ok(None),
        result = response_rx => {
            let value = match result {
                Ok(result) => result?,
                Err(_) => return Ok(Some(StopReason::Cancelled)),
            };
            let stop_reason = value
                .get("stopReason")
                .and_then(Value::as_str)
                .unwrap_or(StopReason::Cancelled.wire_name());
            Ok(Some(stop_reason_from_wire(stop_reason)))
        }
    }
}

fn register_transient_read_retry(error: &anyhow::Error, consecutive: &mut usize) -> bool {
    if !graphql_error_is_transient(error) || *consecutive >= MAX_TRANSIENT_GRAPHQL_RETRIES {
        return false;
    }
    *consecutive += 1;
    true
}

/// Map a wire `stopReason` back onto the enum (used by the drain branch).
fn stop_reason_from_wire(name: &str) -> StopReason {
    match name {
        "end_turn" => StopReason::EndTurn,
        "refusal" => StopReason::Refusal,
        "error" => StopReason::Error,
        _ => StopReason::Cancelled,
    }
}

/// Whether a durable request lifecycle state is terminal.
fn is_background_activity(payload: &Value) -> bool {
    matches!(
        payload["sessionUpdate"].as_str(),
        Some(
            "tool_call"
                | "tool_call_update"
                | "task_backgrounded"
                | "task_completed"
                | "monitor_event"
                | "subagent_spawned"
                | "subagent_progress"
                | "subagent_finished"
        )
    )
}

/// Whether a durable request lifecycle state is terminal.
pub(super) fn is_terminal_lifecycle_state(state: &str) -> bool {
    gents_protocol::request_lifecycle::RequestLifecycleState::is_terminal_str(Some(state))
}

/// Project the durable terminal state into a wire `stopReason`.
///
/// The durable source is `AgentRequest.lifecycle_state`; an `interrupted`
/// request projects `cancelled`. Response markers may explain a terminal
/// outcome, but cannot finish a still-active request ahead of its owner.
pub(super) fn stop_reason_from_rows(
    lifecycle_state: &str,
    response_status: Option<&str>,
    interrupted_at: Option<&str>,
) -> Option<StopReason> {
    let interrupted_at_nonempty = interrupted_at
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);
    match lifecycle_state {
        "interrupted" => Some(StopReason::Cancelled),
        "completed" => match response_status {
            Some("refusal") => Some(StopReason::Refusal),
            Some("error") => Some(StopReason::Error),
            _ => Some(StopReason::EndTurn),
        },
        "failed" => Some(StopReason::Error),
        "superseded" | "dead" => {
            if interrupted_at_nonempty {
                Some(StopReason::Cancelled)
            } else {
                Some(StopReason::Error)
            }
        }
        _ => None,
    }
}

/// Flatten the prompt blocks into the single text content submitted to the
/// Gents runtime. Text blocks are joined with newlines; non-text blocks are
/// serialized so their payload is preserved verbatim in the request content.
pub(super) fn prompt_text(request: &PromptRequest) -> String {
    request
        .prompt
        .iter()
        .map(|block| {
            if block.kind == "text" {
                block.text.clone()
            } else {
                serde_json::to_string(&json!({
                    "type": block.kind,
                    "text": block.text,
                    "meta": block.meta,
                }))
                .unwrap_or_default()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Parse the audited `session/prompt` params.
pub(super) fn parse_prompt_request(params: &Value, id: Option<Value>) -> Result<PromptRequest> {
    let session_id = params
        .get("sessionId")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .context("session/prompt requires sessionId")?
        .to_string();
    let prompt_rows = params
        .get("prompt")
        .and_then(Value::as_array)
        .context("session/prompt requires a prompt array")?;
    if prompt_rows.is_empty() {
        anyhow::bail!("session/prompt requires at least one prompt block");
    }
    let mut prompt = Vec::with_capacity(prompt_rows.len());
    for row in prompt_rows {
        let kind = row
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("text")
            .to_string();
        let text = row
            .get("text")
            .and_then(Value::as_str)
            .context("session/prompt prompt block missing text")?
            .to_string();
        let meta = row.get("meta").cloned().filter(|meta| !meta.is_null());
        prompt.push(PromptBlock { kind, text, meta });
    }
    let meta = params.get("_meta");
    let prompt_id = meta
        .and_then(|meta| meta.get("promptId"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let screen_mode = meta
        .and_then(|meta| meta.get("screenMode"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    if let Some(screen_mode) = screen_mode.as_deref() {
        if !SCREEN_MODES.contains(&screen_mode) {
            anyhow::bail!("unknown screenMode {screen_mode:?}");
        }
    }
    let send_now = meta
        .and_then(|meta| meta.get("sendNow"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Ok(PromptRequest {
        session_id,
        prompt,
        prompt_id,
        screen_mode,
        send_now,
        id,
    })
}

/// Parse the audited `session/cancel` notification params.
pub(super) fn parse_cancel_notification(params: &Value) -> Result<CancelNotification> {
    let session_id = params
        .get("sessionId")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .context("session/cancel requires sessionId")?
        .to_string();
    let meta = params.get("_meta");
    let cancel_subagents = meta
        .and_then(|meta| meta.get("cancelSubagents"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let cancel_trigger = meta
        .and_then(|meta| meta.get("cancelTrigger"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let rewind_if_no_output = meta
        .and_then(|meta| meta.get("rewindIfNoOutput"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let rewind_if_pristine = meta
        .and_then(|meta| meta.get("rewindIfPristine"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let prompt_id = meta
        .and_then(|meta| meta.get("promptId"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    Ok(CancelNotification {
        session_id,
        cancel_subagents,
        cancel_trigger,
        rewind_if_no_output,
        rewind_if_pristine,
        prompt_id,
    })
}

#[cfg(test)]
mod tests {
    use super::super::projection::ProjectionSequencer;
    use super::*;

    #[test]
    fn request_update_timing_is_stable_per_segment_and_strict_across_same_ms_resets() {
        let mut timing = RequestUpdateTiming::new(90);
        let first = super::super::projection::ProjectionEventTiming {
            segment_key: "response:commit-a".to_string(),
            stream_start_candidate_ms: Some(100),
            agent_timestamp_candidate_ms: None,
        };
        let second = super::super::projection::ProjectionEventTiming {
            segment_key: "response:commit-b".to_string(),
            stream_start_candidate_ms: Some(100),
            agent_timestamp_candidate_ms: None,
        };

        let first_chunk = timing.resolve(Some(&first), 250);
        let first_retry = timing.resolve(Some(&first), 300);
        let second_chunk = timing.resolve(Some(&second), 300);

        assert_eq!(first_chunk.stream_start_ms, Some(100));
        assert_eq!(first_retry.stream_start_ms, Some(100));
        assert_eq!(first_retry.agent_timestamp_ms, Some(300));
        assert_eq!(second_chunk.stream_start_ms, Some(101));
        assert_eq!(second_chunk.turn_start_ms, Some(90));
    }

    #[test]
    fn request_update_timing_clamps_durable_event_time_to_its_stream_start() {
        let mut timing = RequestUpdateTiming::new(90);
        let event = super::super::projection::ProjectionEventTiming {
            segment_key: "response:commit-a".to_string(),
            stream_start_candidate_ms: Some(200),
            agent_timestamp_candidate_ms: Some(150),
        };
        let resolved = timing.resolve(Some(&event), 175);
        assert_eq!(resolved.agent_timestamp_ms, Some(200));
        assert_eq!(resolved.stream_start_ms, Some(200));
    }

    fn text_block(text: &str) -> Value {
        json!({"type": "text", "text": text})
    }

    #[test]
    fn parse_prompt_request_reads_audited_meta() {
        let params = json!({
            "sessionId": "session-1",
            "prompt": [
                {
                    "type": "text",
                    "text": "hello",
                    "meta": {"skillTokenRanges": [[0, 5]]},
                },
            ],
            "_meta": {
                "promptId": "prompt-1",
                "screenMode": "fullscreen",
                "sendNow": true,
            },
        });
        let request = parse_prompt_request(&params, Some(json!(7))).unwrap();
        assert_eq!(request.session_id, "session-1");
        assert_eq!(request.prompt_id.as_deref(), Some("prompt-1"));
        assert_eq!(request.screen_mode.as_deref(), Some("fullscreen"));
        assert!(request.send_now);
        assert_eq!(request.id, Some(json!(7)));
        assert_eq!(request.prompt.len(), 1);
        assert!(request.prompt[0].has_skill_token_ranges());
        assert_eq!(request.prompt[0].bash_command(), None);
    }

    #[test]
    fn parse_prompt_request_defaults_optional_meta() {
        let params = json!({
            "sessionId": "session-1",
            "prompt": [text_block("hi")],
        });
        let request = parse_prompt_request(&params, None).unwrap();
        assert_eq!(request.prompt_id, None);
        assert_eq!(request.screen_mode, None);
        assert!(!request.send_now);
        assert!(!request.prompt[0].has_skill_token_ranges());
    }

    #[test]
    fn parse_prompt_request_rejects_missing_session_id() {
        let params = json!({"prompt": [text_block("hi")]});
        assert!(parse_prompt_request(&params, None).is_err());
    }

    #[test]
    fn parse_prompt_request_rejects_unknown_screen_mode() {
        let params = json!({
            "sessionId": "session-1",
            "prompt": [text_block("hi")],
            "_meta": {"screenMode": "sideways"},
        });
        assert!(parse_prompt_request(&params, None).is_err());
    }

    #[test]
    fn parse_prompt_request_rejects_empty_prompt() {
        let params = json!({"sessionId": "session-1", "prompt": []});
        assert!(parse_prompt_request(&params, None).is_err());
    }

    #[test]
    fn parse_prompt_request_rejects_block_without_text() {
        let params = json!({"sessionId": "session-1", "prompt": [{"type": "text"}]});
        assert!(parse_prompt_request(&params, None).is_err());
    }

    #[test]
    fn parse_cancel_notification_reads_audited_meta() {
        let params = json!({
            "sessionId": "session-1",
            "_meta": {
                "cancelSubagents": true,
                "cancelTrigger": "user",
                "rewindIfNoOutput": true,
                "rewindIfPristine": true,
                "promptId": "prompt-1",
            },
        });
        let notification = parse_cancel_notification(&params).unwrap();
        assert_eq!(notification.session_id, "session-1");
        assert!(notification.cancel_subagents);
        assert_eq!(notification.cancel_trigger.as_deref(), Some("user"));
        assert!(notification.rewind_if_no_output);
        assert!(notification.rewind_if_pristine);
        assert_eq!(notification.prompt_id.as_deref(), Some("prompt-1"));
        let meta = notification.meta();
        assert_eq!(meta["cancelSubagents"], json!(true));
        assert_eq!(meta["cancelTrigger"], json!("user"));
        assert_eq!(meta["rewindIfNoOutput"], json!(true));
        assert_eq!(meta["rewindIfPristine"], json!(true));
        assert_eq!(meta["promptId"], json!("prompt-1"));
    }

    #[test]
    fn parse_cancel_notification_omits_absent_optional_keys() {
        let params = json!({"sessionId": "session-1", "_meta": {"cancelSubagents": false}});
        let notification = parse_cancel_notification(&params).unwrap();
        assert!(!notification.cancel_subagents);
        assert_eq!(notification.cancel_trigger, None);
        assert!(!notification.rewind_if_no_output);
        assert!(!notification.rewind_if_pristine);
        assert_eq!(notification.prompt_id, None);
        let meta = notification.meta();
        assert!(meta.get("cancelTrigger").is_none());
        assert!(meta.get("promptId").is_none());
    }

    #[test]
    fn parse_cancel_notification_requires_session_id() {
        assert!(parse_cancel_notification(&json!({"_meta": {}})).is_err());
    }

    #[test]
    fn stop_reason_projection_prefers_interrupted_lifecycle() {
        assert_eq!(
            stop_reason_from_rows("interrupted", Some("complete"), None),
            Some(StopReason::Cancelled)
        );
        assert_eq!(
            stop_reason_from_rows("completed", Some("complete"), None),
            Some(StopReason::EndTurn)
        );
        assert_eq!(
            stop_reason_from_rows("completed", Some("refusal"), None),
            Some(StopReason::Refusal)
        );
        assert_eq!(
            stop_reason_from_rows("completed", Some("error"), None),
            Some(StopReason::Error)
        );
        assert_eq!(
            stop_reason_from_rows("failed", Some("error"), None),
            Some(StopReason::Error)
        );
        assert_eq!(stop_reason_from_rows("processing", None, None), None);
    }

    #[test]
    fn stop_reason_projection_maps_interrupted_at_marker() {
        assert_eq!(
            stop_reason_from_rows("processing", None, Some("2026-01-01T00:00:00Z")),
            None
        );
        assert_eq!(stop_reason_from_rows("processing", None, Some("  ")), None);
        assert_eq!(
            stop_reason_from_rows("superseded", None, Some("2026-01-01T00:00:00Z")),
            Some(StopReason::Cancelled)
        );
        assert_eq!(
            stop_reason_from_rows("superseded", None, None),
            Some(StopReason::Error)
        );
    }

    #[test]
    fn stop_reason_wire_names_are_audited_values() {
        assert_eq!(StopReason::EndTurn.wire_name(), "end_turn");
        assert_eq!(StopReason::Cancelled.wire_name(), "cancelled");
        assert_eq!(StopReason::Refusal.wire_name(), "refusal");
        assert_eq!(StopReason::Error.wire_name(), "error");
        assert_eq!(stop_reason_from_wire("cancelled"), StopReason::Cancelled);
        assert_eq!(stop_reason_from_wire("end_turn"), StopReason::EndTurn);
        assert_eq!(stop_reason_from_wire("nonsense"), StopReason::Cancelled);
    }

    #[test]
    fn prompt_text_joins_text_blocks_and_preserves_other_kinds() {
        let request = PromptRequest {
            session_id: "s".to_string(),
            prompt: vec![
                PromptBlock {
                    kind: "text".to_string(),
                    text: "one".to_string(),
                    meta: None,
                },
                PromptBlock {
                    kind: "image".to_string(),
                    text: "binary".to_string(),
                    meta: Some(json!({"mime": "image/png"})),
                },
            ],
            prompt_id: None,
            screen_mode: None,
            send_now: false,
            id: None,
        };
        let text = prompt_text(&request);
        assert!(text.starts_with("one\n"));
        assert!(text.contains("\"type\":\"image\""));
    }

    #[test]
    fn bash_block_meta_stamp_is_recognized() {
        let block = PromptBlock {
            kind: "text".to_string(),
            text: "$ run".to_string(),
            meta: Some(json!({"bash": {"command": "echo hi"}})),
        };
        assert_eq!(block.bash_command().as_deref(), Some("echo hi"));
        assert!(!block.has_skill_token_ranges());
    }

    #[test]
    fn terminal_lifecycle_states_are_audited() {
        for state in ["completed", "failed", "superseded", "dead", "interrupted"] {
            assert!(
                is_terminal_lifecycle_state(state),
                "{state} must be terminal"
            );
        }
        for state in ["pending", "processing", "queued", ""] {
            assert!(
                !is_terminal_lifecycle_state(state),
                "{state} must not be terminal"
            );
        }
    }

    /// The cancel-before-id latch records the first cancel and stays set.
    #[test]
    fn cancel_before_id_latch_is_idempotent() {
        let mut latch = CancelBeforeIdLatch::default();
        assert!(!latch.is_cancelled());
        assert!(latch.cancel());
        assert!(latch.is_cancelled());
        assert!(!latch.cancel());
        assert!(latch.is_cancelled());
    }

    #[test]
    fn transient_read_retry_budget_is_consecutive_and_bounded() {
        let transient = anyhow::anyhow!("database is locked");
        let fatal = anyhow::anyhow!("invalid query");
        let mut consecutive = 0;
        for expected in 1..=MAX_TRANSIENT_GRAPHQL_RETRIES {
            assert!(register_transient_read_retry(&transient, &mut consecutive));
            assert_eq!(consecutive, expected);
        }
        assert!(!register_transient_read_retry(&transient, &mut consecutive));
        consecutive = 0; // the production loop resets after a complete poll
        assert!(register_transient_read_retry(&transient, &mut consecutive));
        assert!(!register_transient_read_retry(&fatal, &mut consecutive));
    }

    /// A pending entry resolves its deferred response exactly once.
    #[tokio::test]
    async fn pending_prompt_resolves_once() {
        let (tx, mut rx) = oneshot::channel::<Result<Value>>();
        let mut entry = PendingPrompt {
            response_tx: Some(tx),
            request_id: None,
            cancel_before_id: Arc::new(Mutex::new(CancelBeforeIdLatch::default())),
            drained: false,
        };
        entry.resolve(Ok(json!({"stopReason": "cancelled"})));
        assert!(entry.drained);
        let first = rx.try_recv().expect("first resolve delivers");
        assert_eq!(first.unwrap()["stopReason"], json!("cancelled"));
        // A second resolve is a no-op: the receiver is exhausted.
        entry.resolve(Ok(json!({"stopReason": "end_turn"})));
        assert!(rx.try_recv().is_err());
    }

    /// The user echo notification carries the audited shape: content field
    /// name (not contentBlock), promptIndex/hideFromScrollback block meta,
    /// and _meta.promptId.
    #[tokio::test]
    async fn user_echo_uses_audited_chunk_shape() {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let sender = PromptSender::Buffer {
            buffer: buffer.clone(),
        };
        let session_updates = SessionUpdateChannel::new(Arc::new(ProjectionSequencer::new()));
        sender
            .send_user_message_chunk(
                &session_updates,
                "session-1",
                "prompt-1",
                &PromptBlock {
                    kind: "text".to_string(),
                    text: "hello".to_string(),
                    meta: None,
                },
                0,
            )
            .await
            .unwrap();
        let lines = buffer.lock().await;
        assert_eq!(lines.len(), 1);
        let value: Value = serde_json::from_str(&lines[0]).unwrap();
        assert_eq!(value["method"], json!("session/update"));
        assert_eq!(value["params"]["sessionId"], json!("session-1"));
        assert_eq!(
            value["params"]["update"]["sessionUpdate"],
            json!("user_message_chunk")
        );
        // The Grok decoder expects the chunk field name `content`.
        assert_eq!(value["params"]["update"]["content"]["type"], json!("text"));
        assert_eq!(value["params"]["update"]["content"]["text"], json!("hello"));
        assert_eq!(value["params"]["update"]["_meta"]["promptIndex"], json!(0));
        assert_eq!(
            value["params"]["update"]["_meta"]["hideFromScrollback"],
            json!(false)
        );
        assert!(value["params"]["update"]["content"].get("meta").is_none());
        assert_eq!(value["params"]["_meta"]["promptId"], json!("prompt-1"));
        assert_eq!(value["params"]["_meta"]["isReplay"], json!(false));
        // The echo reports the last observed context (zero on the first
        // turn, before any projected observation has applied).
        assert_eq!(value["params"]["_meta"]["totalTokens"], json!(0));
        // The echo shares the pager's eventId dedup space with the projected
        // updates.
        assert_eq!(value["params"]["_meta"]["eventId"], json!("session-1-1"));
    }

    /// A closed outbound channel makes the user echo send fail, which is the
    /// send-failure-after-submission path: the caller must interrupt. The
    /// failed send consumes neither the event id nor the session's counter,
    /// so the next successful sender receives the same expected next id.
    #[tokio::test]
    async fn closed_outbound_channel_fails_the_echo_send() {
        let (frames_tx, frames_rx) = tokio::sync::mpsc::unbounded_channel::<
            crate::commands::grok_shim::protocol::ServerEnvelope,
        >();
        drop(frames_rx);
        let sender = PromptSender::Live {
            outbound: AcpOutbound::for_frames(frames_tx),
        };
        let session_updates = SessionUpdateChannel::new(Arc::new(ProjectionSequencer::new()));
        let result = sender
            .send_user_message_chunk(
                &session_updates,
                "session-1",
                "prompt-1",
                &PromptBlock {
                    kind: "text".to_string(),
                    text: "hello".to_string(),
                    meta: None,
                },
                0,
            )
            .await;
        assert!(result.is_err(), "closed channel must fail the send");
        assert_eq!(
            session_updates.sequencer().event_counter("session-1"),
            0,
            "the failed send must roll the reservation back"
        );

        // A fresh sender on the same session draws the id the failed send
        // would have taken: no id was consumed, no gap was left.
        let (fresh_tx, mut fresh_rx) = tokio::sync::mpsc::unbounded_channel::<
            crate::commands::grok_shim::protocol::ServerEnvelope,
        >();
        let fresh = PromptSender::Live {
            outbound: AcpOutbound::for_frames(fresh_tx),
        };
        fresh
            .send_user_message_chunk(
                &session_updates,
                "session-1",
                "prompt-1",
                &PromptBlock {
                    kind: "text".to_string(),
                    text: "hello".to_string(),
                    meta: None,
                },
                0,
            )
            .await
            .expect("fresh sender delivers");
        let envelope = fresh_rx.try_recv().expect("fresh line");
        let payload = match envelope {
            crate::commands::grok_shim::protocol::ServerEnvelope::Acp { payload } => payload,
            other => panic!("expected an ACP envelope, got {other:?}"),
        };
        let notification: Value = serde_json::from_str(&payload).expect("notification json");
        assert_eq!(
            notification["params"]["_meta"]["eventId"],
            json!("session-1-1"),
            "the next successful send must receive the same expected next id"
        );
        assert_eq!(session_updates.sequencer().event_counter("session-1"), 1);
    }

    /// The wire-escape helper is applied to every interpolated GraphQL value:
    /// a request id containing quotes and backslashes must round-trip safely
    /// through the terminal query string.
    #[test]
    fn terminal_query_escapes_interpolated_values() {
        let request_id = "req-\"quoted\"\\slash";
        let escaped = escape_graphql_string(request_id);
        let query = format!(r#"AgentRequest(filter: {{ request_id: {{ _eq: "{escaped}" }} }})"#);
        assert!(!query.contains(r#""req-""#), "raw quote must be escaped");
        assert!(query.contains(&escaped));
    }

    /// One pending prompt per session: the live-prompt check treats any
    /// pending entry for the same session as a conflict.
    #[tokio::test]
    async fn one_pending_prompt_per_session_is_enforced_by_key_scan() {
        let pending = Mutex::new(HashMap::new());
        let (tx, _rx) = oneshot::channel::<Result<Value>>();
        pending.lock().await.insert(
            ("session-1".to_string(), "prompt-live".to_string()),
            PendingPrompt {
                response_tx: Some(tx),
                request_id: None,
                cancel_before_id: Arc::new(Mutex::new(CancelBeforeIdLatch::default())),
                drained: false,
            },
        );
        let has_live = pending
            .lock()
            .await
            .keys()
            .any(|(session, _)| session == "session-1");
        assert!(has_live);
        let other_session_live = pending
            .lock()
            .await
            .keys()
            .any(|(session, _)| session == "session-2");
        assert!(!other_session_live);
    }

    // ----- Integration tests: real embedded node + mock GraphQL endpoint -----

    use axum::{extract::State, routing::post, Json, Router};

    /// Shared mock-endpoint state: the embedded node plus an optional
    /// one-shot submission gate. While armed, the endpoint signals that the
    /// `create_AgentRequest` mutation has arrived and then parks until the
    /// test releases it, so a cancel/disconnect can deterministically land
    /// inside the before-request-id window. The gate disarms itself after
    /// the first gated submission so later submissions pass through. When
    /// `submission_fail` is armed, the released gated submission returns a
    /// non-retryable GraphQL error response instead of forwarding to the
    /// node, so a submission failure can be produced deterministically.
    #[derive(Clone)]
    struct MockGraphqlState {
        node: Arc<EmbeddedNode>,
        gate_armed: Option<Arc<std::sync::atomic::AtomicBool>>,
        submission_arrived: Option<Arc<tokio::sync::Notify>>,
        submission_release: Option<Arc<tokio::sync::Notify>>,
        submission_fail: Option<Arc<std::sync::atomic::AtomicBool>>,
    }

    /// A mock GraphQL endpoint that forwards mutations to the embedded node
    /// so `create_agent_request` writes real durable rows. Every response is
    /// the node's own, so the whole submission path is exercised.
    async fn mock_graphql(
        State(state): State<MockGraphqlState>,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        let query = body
            .get("query")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let is_create_request = query.contains("create_AgentRequest");
        tracing::debug!(
            target: "grok_shim::mock",
            query_len = query.len(),
            is_create_request,
            "mock handler forwarding query"
        );
        if is_create_request {
            let first_gated_submission = state
                .gate_armed
                .as_ref()
                .is_some_and(|armed| armed.swap(false, std::sync::atomic::Ordering::SeqCst));
            if first_gated_submission {
                if let Some(arrived) = state.submission_arrived.as_ref() {
                    // notify_one stores a permit when no test waiter is
                    // parked yet, so the arrival signal cannot be lost.
                    arrived.notify_one();
                }
                if let Some(release) = state.submission_release.as_ref() {
                    release.notified().await;
                }
                if let Some(fail) = state.submission_fail.as_ref() {
                    if fail.swap(false, std::sync::atomic::Ordering::SeqCst) {
                        // A non-retryable GraphQL error response: the same
                        // class an ordinary failed mutation produces.
                        return Json(json!({
                            "errors": [{"message": "injected submission failure"}],
                        }));
                    }
                }
            }
        }
        let response = state.node.execute(&query).await;
        Json(serde_json::to_value(&response).unwrap_or_default())
    }

    /// Spawn a mock GraphQL endpoint bound to the node and return its URL.
    /// The endpoint forwards to the node directly; no gating is applied.
    async fn spawn_mock_graphql(node: Arc<EmbeddedNode>) -> String {
        spawn_gated_mock_graphql(node, None).await
    }

    /// Spawn a mock GraphQL endpoint that one-shot gates the first
    /// `create_AgentRequest`: the returned `Notify` fires when the submission
    /// mutation arrives, and the endpoint parks the mutation until
    /// `submission_release` is notified. Later submissions pass through.
    /// `submission_fail` optionally makes the released gated submission
    /// return a non-retryable GraphQL error response instead of forwarding.
    async fn spawn_gated_mock_graphql(
        node: Arc<EmbeddedNode>,
        submission_gate: Option<(
            Arc<tokio::sync::Notify>,
            Arc<tokio::sync::Notify>,
            Arc<std::sync::atomic::AtomicBool>,
        )>,
    ) -> String {
        spawn_gated_mock_graphql_with_failure(node, submission_gate, None).await
    }

    /// The gated mock endpoint with an optional one-shot submission-failure
    /// injection for the released gated submission.
    async fn spawn_gated_mock_graphql_with_failure(
        node: Arc<EmbeddedNode>,
        submission_gate: Option<(
            Arc<tokio::sync::Notify>,
            Arc<tokio::sync::Notify>,
            Arc<std::sync::atomic::AtomicBool>,
        )>,
        submission_fail: Option<Arc<std::sync::atomic::AtomicBool>>,
    ) -> String {
        let (submission_arrived, submission_release, gate_armed) = match submission_gate {
            Some((arrived, release, armed)) => (Some(arrived), Some(release), Some(armed)),
            None => (None, None, None),
        };
        let state = MockGraphqlState {
            node,
            gate_armed,
            submission_arrived,
            submission_release,
            submission_fail,
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock graphql listener");
        let addr = listener
            .local_addr()
            .expect("mock graphql listener address");
        let router = Router::new()
            .route("/", post(mock_graphql))
            .with_state(state);
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        format!("http://{addr}/")
    }

    /// Build an embedded node with runtime schemas and an admitted test
    /// principal/behavior, matching the production request boundary.
    async fn test_node() -> (tempfile::TempDir, Arc<EmbeddedNode>, String) {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let identity = gents::KeyIdentity::load_or_create(tempdir.path().join("agent.key"), None)
            .expect("test signing identity");
        let agent_did = gents::AgentIdentity::did(&identity).to_string();
        let behavior_id = gents::default_behavior_id_for_agent(&agent_did);
        let node = Arc::new(
            EmbeddedNode::builder()
                .data_path(tempdir.path().join("node"))
                .with_storage_backend(gents::defra_node::StorageBackend::Regolith)
                .build()
                .await
                .expect("embedded node"),
        );
        gents::schema::ensure_runtime_schemas(&node)
            .await
            .expect("runtime schemas");
        let response = node
            .execute(&format!(
                r#"mutation {{
                    create_AgentPrincipal(input: {{
                        agent_did: "{agent_did}"
                        display_name: "Grok shim test"
                        default_behavior_id: "{behavior_id}"
                        enabled: true
                    }}) {{ _docID }}
                    create_AgentBehavior(input: {{
                        behavior_id: "{behavior_id}"
                        agent_did: "{agent_did}"
                        display_name: "Grok shim test"
                        enabled: true
                    }}) {{ _docID }}
                }}"#,
            ))
            .await;
        gents::graphql::ensure_no_errors(&response, "seed admitted test behavior")
            .expect("seed admitted test behavior");
        (tempdir, node, agent_did)
    }

    fn test_config(graphql: String, agent_did: &str) -> TurnManagerConfig {
        TurnManagerConfig {
            agent_did: agent_did.to_string(),
            behavior_id: gents::default_behavior_id_for_agent(agent_did),
            graphql,
        }
    }

    /// A projection engine over the same embedded node the turn manager
    /// uses, with a plain test bound context.
    fn test_engine(node: Arc<EmbeddedNode>) -> Arc<ProjectionEngine> {
        Arc::new(ProjectionEngine::new(
            node,
            super::super::projection::BoundModelContext::new(
                "GLM-5.3-NVFP4".to_string(),
                "GLM-5.3-NVFP4".to_string(),
                262_144,
            ),
        ))
    }

    async fn seed_runtime_wake(node: &EmbeddedNode, principal: &str, content: &str) -> String {
        // Source identity is immutable: seed a runtime-shaped request at
        // creation, never relabel a human submission after admission.
        let request = uuid::Uuid::new_v4().to_string();
        let result = node
            .execute(&format!(
                r#"mutation {{create_AgentRequest(input:{{
            request_id:"{request}",agent_did:"{principal}",requester_did:"{principal}",
            session_id:"session-1",runtime_source_kind:"local-control",lifecycle_state:"pending",
            content:"{}",created_at:"{}"}}) {{_docID}}}}"#,
                escape_graphql_string(content),
                chrono::Utc::now().to_rfc3339(),
                principal = escape_graphql_string(principal)
            ))
            .await;
        ensure_no_errors(&result, "seed runtime wake fixture").unwrap();
        request
    }

    fn buffer_sender() -> (Arc<Mutex<Vec<String>>>, PromptSender) {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        (buffer.clone(), PromptSender::Buffer { buffer })
    }

    /// Terminalize a request durably in one atomic mutation: the given
    /// lifecycle state plus a response row carrying the audited fields, as
    /// the runtime does. A single mutation avoids the watch observing a
    /// transient intermediate state between two writes.
    async fn terminalize_request(
        node: &Arc<EmbeddedNode>,
        request_id: &str,
        lifecycle_state: &str,
    ) {
        let escaped = escape_graphql_string(request_id);
        let escaped_state = escape_graphql_string(lifecycle_state);
        let now = escape_graphql_string(&chrono::Utc::now().to_rfc3339());
        let mutation = format!(
            r#"mutation {{
                update_AgentRequest(
                    filter: {{ request_id: {{ _eq: "{escaped}" }} }},
                    input: {{ lifecycle_state: "{escaped_state}" }}
                ) {{ _docID }}
                create_AgentResponse(input: {{
                    response_key: "{escaped}"
                    request_id: "{escaped}"
                    agent_did: "did:test:grok-shim"
                    behavior_id: "did:test:grok-shim:default"
                    session_id: "session-1"
                    content: ""
                    reasoning: ""
                    status: "complete"
                    error_message: ""
                    token_count: 0
                    progress_seq: 0
                    created_at: "{now}"
                    completed_at: "{now}"
                }}) {{ _docID }}
            }}"#
        );
        let response = node.execute(&mutation).await;
        ensure_no_errors(&response, "test terminalize request").expect("terminalize");
    }

    /// Seed one durable `AgentMessage` assistant row for the request, the
    /// way the runtime materializes a finished assistant turn: a role-tagged
    /// persisted envelope decoded by the message leaf.
    async fn seed_assistant_message(
        node: &Arc<EmbeddedNode>,
        request_id: &str,
        sequence: i64,
        text: &str,
    ) {
        let message = serde_json::to_string(&gents_protocol::message::Message::assistant(text))
            .expect("serialize assistant message");
        seed_message_row(node, request_id, sequence, "assistant", &message).await;
    }

    /// Seed one durable `AgentMessage` row with an explicit serialized
    /// content blob and role.
    async fn seed_message_row(
        node: &Arc<EmbeddedNode>,
        request_id: &str,
        sequence: i64,
        role: &str,
        content: &str,
    ) {
        let escaped_request = escape_graphql_string(request_id);
        let escaped_content = escape_graphql_string(content);
        let escaped_role = escape_graphql_string(role);
        let mutation = format!(
            r#"mutation {{
                create_AgentMessage(input: {{
                    message_key: "{escaped_request}:{sequence}"
                    session_id: "session-1"
                    agent_did: "did:test:grok-shim"
                    requester_did: "did:test:grok-shim"
                    request_id: "{escaped_request}"
                    sequence: {sequence}
                    role: "{escaped_role}"
                    content: "{escaped_content}"
                }}) {{ _docID }}
            }}"#
        );
        let response = node.execute(&mutation).await;
        ensure_no_errors(&response, "test seed message").expect("seed message");
    }

    /// Seed one durable `AgentToolCall` row for the request with the given
    /// authoritative lifecycle state.
    async fn seed_tool_call(
        node: &Arc<EmbeddedNode>,
        request_id: &str,
        tool_call_id: &str,
        tool_name: &str,
        lifecycle_state: &str,
        result: &str,
        child_request_id: Option<&str>,
    ) {
        let escaped_request = escape_graphql_string(request_id);
        let escaped_id = escape_graphql_string(tool_call_id);
        let escaped_name = escape_graphql_string(tool_name);
        let escaped_state = escape_graphql_string(lifecycle_state);
        let escaped_result = escape_graphql_string(result);
        let parent = node
            .execute(&format!(
                r#"{{ AgentRequest(filter: {{ request_id: {{ _eq: "{escaped_request}" }} }}, limit: 1) {{ _docID }} }}"#
            ))
            .await;
        ensure_no_errors(&parent, "test load parent doc id").expect("parent doc id query");
        let parent_doc_id = parent
            .data
            .as_ref()
            .and_then(|data| data.pointer("/AgentRequest/0/_docID"))
            .and_then(Value::as_str)
            .expect("parent doc id");
        let escaped_parent_doc = escape_graphql_string(parent_doc_id);
        let child_field = child_request_id.map_or_else(String::new, |child_request_id| {
            format!(
                "child_request_id: \"{}\"",
                escape_graphql_string(child_request_id)
            )
        });
        let mutation = format!(
            r#"mutation {{
                create_AgentToolCall(input: {{
                    tool_call_key: "session-1:{escaped_id}"
                    request_id: "{escaped_request}"
                    session_id: "session-1"
                    agent_did: "did:test:grok-shim"
                    requester_did: "did:test:grok-shim"
                    request_doc_id: "{escaped_parent_doc}"
                    tool_call_id: "{escaped_id}"
                    tool_name: "{escaped_name}"
                    lifecycle_state: "{escaped_state}"
                    result: "{escaped_result}"
                    {child_field}
                }}) {{ _docID }}
            }}"#
        );
        let response = node.execute(&mutation).await;
        ensure_no_errors(&response, "test seed tool call").expect("seed tool call");
    }

    /// Seed one runtime child `AgentRequest` row linked to the parent
    /// request, the durable shape the subagent projection observes.
    async fn seed_child_request(
        node: &Arc<EmbeddedNode>,
        parent_request_id: &str,
        child_request_id: &str,
        lifecycle_state: &str,
    ) {
        let escaped_parent = escape_graphql_string(parent_request_id);
        let escaped_child = escape_graphql_string(child_request_id);
        let escaped_state = escape_graphql_string(lifecycle_state);
        let bridge = node
            .execute(&format!(
                r#"{{
                    parent: AgentRequest(filter: {{ request_id: {{ _eq: "{escaped_parent}" }} }}, limit: 1) {{ _docID }}
                    tool: AgentToolCall(filter: {{ request_id: {{ _eq: "{escaped_parent}" }}, tool_call_id: {{ _eq: "call-1" }} }}, limit: 1) {{ _docID }}
                }}"#
            ))
            .await;
        ensure_no_errors(&bridge, "test load spawn bridge ids").expect("spawn bridge ids");
        let parent_doc_id = bridge
            .data
            .as_ref()
            .and_then(|data| data.pointer("/parent/0/_docID"))
            .and_then(Value::as_str)
            .expect("parent doc id");
        let tool_doc_id = bridge
            .data
            .as_ref()
            .and_then(|data| data.pointer("/tool/0/_docID"))
            .and_then(Value::as_str)
            .expect("tool doc id");
        let escaped_parent_doc = escape_graphql_string(parent_doc_id);
        let escaped_tool_doc = escape_graphql_string(tool_doc_id);
        let now = chrono::Utc::now().to_rfc3339();
        let escaped_now = escape_graphql_string(&now);
        let mutation = format!(
            r#"mutation {{
                create_AgentRequest(input: {{
                    request_id: "{escaped_child}"
                    agent_did: "did:test:grok-shim"
                    session_id: "session-1-child"
                    caused_by_parent_request_id: "{escaped_parent}"
                    caused_by_parent_request_doc_id: "{escaped_parent_doc}"
                    caused_by_parent_tool_call_id: "call-1"
                    caused_by_parent_tool_call_doc_id: "{escaped_tool_doc}"
                    content: "child work"
                    lifecycle_state: "{escaped_state}"
                    backend_id: ""
                    execution_origin: "interactive"
                    failure_reason: ""
                    created_at: "{escaped_now}"
                    retry_count: 0
                    max_retries: 3
                }}) {{ _docID }}
            }}"#
        );
        let response = node.execute(&mutation).await;
        ensure_no_errors(&response, "test seed child request").expect("seed child request");
    }

    /// Transition a seeded tool call to its terminal completed state with a
    /// recorded result, the way the runtime finalizes a tool call.
    async fn complete_tool_call(node: &Arc<EmbeddedNode>, tool_call_id: &str, result: &str) {
        let escaped_id = escape_graphql_string(tool_call_id);
        let escaped_state = escape_graphql_string("completed");
        let escaped_result = escape_graphql_string(result);
        let mutation = format!(
            r#"mutation {{
                update_AgentToolCall(
                    filter: {{ tool_call_id: {{ _eq: "{escaped_id}" }} }},
                    input: {{
                        lifecycle_state: "{escaped_state}"
                        result: "{escaped_result}"
                    }}
                ) {{ _docID }}
            }}"#
        );
        let response = node.execute(&mutation).await;
        ensure_no_errors(&response, "test complete tool call").expect("complete tool call");
    }

    /// Transition a seeded child request to its terminal completed state,
    /// the durable edge the subagent projection finishes on.
    async fn complete_child_request(node: &Arc<EmbeddedNode>, child_request_id: &str) {
        let escaped_child = escape_graphql_string(child_request_id);
        let escaped_state = escape_graphql_string("completed");
        let now = escape_graphql_string(&chrono::Utc::now().to_rfc3339());
        let mutation = format!(
            r#"mutation {{
                update_AgentRequest(
                    filter: {{ request_id: {{ _eq: "{escaped_child}" }} }},
                    input: {{
                        lifecycle_state: "{escaped_state}"
                        terminalized_at: "{now}"
                    }}
                ) {{ _docID }}
            }}"#
        );
        let response = node.execute(&mutation).await;
        ensure_no_errors(&response, "test complete child request").expect("complete child request");
    }

    /// The parsed `session/update` notification values in a buffer of
    /// serialized lines.
    async fn parse_buffered_updates(buffer: &Arc<Mutex<Vec<String>>>) -> Vec<Value> {
        let lines = buffer.lock().await.clone();
        lines
            .iter()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .collect()
    }

    /// The `sessionUpdate` discriminators of the buffered notifications, in
    /// order, for ordering assertions.
    async fn buffered_update_kinds(buffer: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
        parse_buffered_updates(buffer)
            .await
            .iter()
            .map(|value| {
                value["params"]["update"]["sessionUpdate"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string()
            })
            .collect()
    }

    /// A prompt that terminalizes normally resolves `stopReason=end_turn`.
    #[tokio::test]
    async fn prompt_resolves_end_turn_after_terminalization() {
        let (_tempdir, node, agent_did) = test_node().await;
        let graphql = spawn_mock_graphql(node.clone()).await;
        let manager = TurnManager::new(node.clone(), test_config(graphql, &agent_did));
        let engine = test_engine(node.clone());
        let (_buffer, sender) = buffer_sender();

        let prompt = parse_prompt_request(
            &json!({
                "sessionId": "session-1",
                "prompt": [text_block("hello")],
                "_meta": {"promptId": "prompt-1"},
            }),
            Some(json!(1)),
        )
        .unwrap();

        let node_for_terminalize = node.clone();
        let handle = tokio::spawn(async move {
            // Wait for the request row to exist, then terminalize it.
            loop {
                let query = r#"{ AgentRequest(filter: { lifecycle_state: { _eq: "pending" } }) { request_id } }"#;
                let response = node_for_terminalize.execute(query).await;
                let rows = response
                    .data
                    .as_ref()
                    .and_then(|data| data.get("AgentRequest"))
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                if let Some(row) = rows.first() {
                    let request_id = row.get("request_id").and_then(Value::as_str).unwrap();
                    // A completed lifecycle with a `complete` response status
                    // projects `end_turn`; the single atomic mutation avoids
                    // the watch observing a transient `interrupted` state.
                    terminalize_request(&node_for_terminalize, request_id, "completed").await;
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });

        let result = tokio::time::timeout(
            Duration::from_secs(30),
            manager.handle_prompt(prompt, &sender, &engine),
        )
        .await
        .expect("prompt should resolve within timeout")
        .expect("prompt should succeed");
        assert_eq!(result["stopReason"], json!("end_turn"));
        handle.await.expect("terminalize task");
    }

    #[tokio::test]
    async fn goal_prompt_atomically_submits_scoped_goal_and_signed_request() {
        let (_dir, node, principal) = test_node().await;
        let graphql = spawn_mock_graphql(node.clone()).await;
        let manager = TurnManager::new(node.clone(), test_config(graphql, &principal));
        let mut prompt = parse_prompt_request(
            &json!({
                "sessionId":"goal-submit-session",
                "prompt":[text_block("/goal Explain the architecture --budget 100000")],
                "_meta":{"screenMode":"inline", "sendNow":true}
            }),
            None,
        )
        .unwrap();
        let id = manager
            .submit_request(&prompt, "goal-prompt-id")
            .await
            .unwrap();
        let goal = gents::goal::load_canonical_goal(&node, &principal, &prompt.session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(goal.objective, "Explain the architecture");
        assert_eq!(goal.status, "active");
        assert_eq!(goal.token_budget, Some(100000));
        let response = node.execute(&format!(
            "{{AgentRequest(filter:{{request_id:{{_eq:\"{id}\"}}}}){{request_id agent_did session_id content metadata retry_key admission_signer_did}}}}"
        )).await;
        gents::graphql::ensure_no_errors(&response, "goal submission").unwrap();
        let row = &response.data.as_ref().unwrap()["AgentRequest"][0];
        assert_eq!(row["content"], "Explain the architecture");
        assert_eq!(row["agent_did"], principal);
        assert_eq!(row["admission_signer_did"], principal);
        assert_eq!(row["session_id"], prompt.session_id);
        assert_eq!(row["retry_key"], format!("goal-request:{id}"));
        let metadata: Value = serde_json::from_str(row["metadata"].as_str().unwrap()).unwrap();
        assert_eq!(metadata["promptId"], "goal-prompt-id");
        assert_eq!(metadata["screenMode"], "inline");
        assert_eq!(metadata["sendNow"], true);

        prompt.prompt[0].text = "/goal Conflicting objective".into();
        assert!(manager.submit_request(&prompt, "conflict").await.is_err());
        let response = node
            .execute("{AgentRequest{request_id} Goal{goal_id} GoalCreationClaim{creation_key}}")
            .await;
        gents::graphql::ensure_no_errors(&response, "atomic rejection").unwrap();
        let data = response.data.as_ref().unwrap();
        for collection in ["AgentRequest", "Goal", "GoalCreationClaim"] {
            assert_eq!(
                data[collection].as_array().unwrap().len(),
                1,
                "{collection}"
            );
        }
        assert_eq!(
            gents::goal::load_canonical_goal(&node, &principal, &prompt.session_id)
                .await
                .unwrap()
                .unwrap()
                .objective,
            "Explain the architecture"
        );
        // Clear/recreate must admit a genuinely new request, not recover the
        // prior incarnation's request by a session-only retry key.
        gents::goal::delete_goals_for_session(&node, &principal, &prompt.session_id)
            .await
            .unwrap();
        let next = manager
            .submit_request(&prompt, "new-incarnation")
            .await
            .unwrap();
        assert_ne!(id, next);
    }

    /// Only one autonomous turn can own the pager's live cards. Notices and
    /// later wakes wait for its terminal delivery; ordinary Esc cancels it.
    #[tokio::test]
    async fn resume_hands_off_active_and_during_replay_requests_without_repeating_history() {
        let (_dir, node, principal) = test_node().await;
        let graphql = spawn_mock_graphql(node.clone()).await;
        let manager = TurnManager::new(node.clone(), test_config(graphql, &principal));
        let engine = test_engine(node.clone());
        let (buffer, sender) = buffer_sender();
        let prompt = parse_prompt_request(
            &json!({"sessionId":"session-1", "prompt":[text_block("Original prompt")]}),
            None,
        )
        .unwrap();
        let first = manager.submit_request(&prompt, "first").await.unwrap();
        seed_assistant_message(&node, &first, 1, "Before reconnect.").await;
        let attached = chrono::Utc::now().to_rfc3339();
        let rows = super::super::sessions::requests(&node, &principal, Some("session-1"))
            .await
            .unwrap();
        let running = manager
            .replay_session("session-1", &rows, &sender, &engine)
            .await
            .unwrap();
        assert_eq!(running, Some(format!("notifications-{first}")));
        let historical = parse_buffered_updates(&buffer).await;
        assert!(historical
            .iter()
            .all(|event| event["params"]["_meta"]["isReplay"] == true));
        buffer.lock().await.clear();
        seed_assistant_message(&node, &first, 2, "After reconnect.").await;
        terminalize_request(&node, &first, "completed").await;
        // This request was not in the replay manifest. Discovery must begin
        // at the pre-replay attachment time, not the later response time.
        let second = manager.submit_request(&prompt, "second").await.unwrap();
        seed_assistant_message(&node, &second, 3, "Created during replay.").await;
        terminalize_request(&node, &second, "completed").await;
        let mut after = String::new();
        let mut delivery_after = String::new();
        for _ in 0..3 {
            manager
                .observe_session_tick(
                    "session-1",
                    &attached,
                    &mut after,
                    &mut delivery_after,
                    &sender,
                    &engine,
                )
                .await
                .unwrap();
        }
        let updates = parse_buffered_updates(&buffer).await;
        let text = updates
            .iter()
            .filter_map(|event| {
                event
                    .pointer("/params/update/content/text")
                    .and_then(Value::as_str)
            })
            .collect::<String>();
        assert!(!text.contains("Before reconnect."));
        assert_eq!(text.matches("After reconnect.").count(), 1);
        assert_eq!(text.matches("Created during replay.").count(), 1);
        assert!(updates
            .iter()
            .all(|event| event["params"]["_meta"]["isReplay"] != true));
        assert!(manager.autonomous_delivery.lock().await.is_empty());
    }

    #[tokio::test]
    async fn session_discovery_excludes_foreign_principals_and_requesters() {
        let (_tempdir, node, agent_did) = test_node().await;
        let graphql = spawn_mock_graphql(node.clone()).await;
        let manager = TurnManager::new(node.clone(), test_config(graphql, &agent_did));
        let engine = test_engine(node.clone());
        let (_, sender) = buffer_sender();
        for (id, agent, requester) in [
            (
                "foreign-agent",
                "did:test:foreign",
                Some(agent_did.as_str()),
            ),
            (
                "foreign-requester",
                agent_did.as_str(),
                Some("did:test:foreign"),
            ),
            ("missing-requester", agent_did.as_str(), None),
            ("owned", agent_did.as_str(), Some(agent_did.as_str())),
        ] {
            let requester = requester
                .map(|did| format!("\"{}\"", escape_graphql_string(did)))
                .unwrap_or_else(|| "null".into());
            let result = node
                .execute(&format!(
                    r#"mutation {{ create_AgentRequest(input: {{
                request_id: "{id}", session_id: "session-1", agent_did: "{}",
                requester_did: {requester}, lifecycle_state: "completed",
                created_at: "2026-06-04T12:00:00Z"
            }}) {{ _docID }} }}"#,
                    escape_graphql_string(agent)
                ))
                .await;
            ensure_no_errors(&result, "seed scoped session discovery").unwrap();
        }
        manager
            .observe_session_tick(
                "session-1",
                "1970-01-01T00:00:00Z",
                &mut String::new(),
                &mut String::new(),
                &sender,
                &engine,
            )
            .await
            .unwrap();
        let observed = manager.observed.lock().await;
        assert_eq!(observed.len(), 1);
        assert!(observed.contains_key(&("session-1".into(), "owned".into())));
    }

    #[tokio::test]
    async fn observed_human_turn_keeps_prompt_identity_echo_and_cancel_target() {
        let (_tempdir, node, agent_did) = test_node().await;
        let graphql = spawn_mock_graphql(node.clone()).await;
        let manager = TurnManager::new(node.clone(), test_config(graphql, &agent_did));
        let engine = test_engine(node.clone());
        let (buffer, sender) = buffer_sender();
        let prompt = parse_prompt_request(
            &json!({"sessionId":"session-1", "prompt":[text_block("Human from another client")]}),
            None,
        )
        .unwrap();
        let request = manager.submit_request(&prompt, "peer-human").await.unwrap();
        seed_assistant_message(&node, &request, 1, "Peer answer").await;
        let mut after = String::new();
        let mut delivery_after = String::new();
        manager
            .observe_session_tick(
                "session-1",
                "1970-01-01T00:00:00Z",
                &mut after,
                &mut delivery_after,
                &sender,
                &engine,
            )
            .await
            .unwrap();
        let events: Vec<Value> = buffer
            .lock()
            .await
            .iter()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let echo = events
            .iter()
            .find(|event| event["params"]["update"]["sessionUpdate"] == "user_message_chunk")
            .unwrap();
        assert_eq!(echo["params"]["_meta"]["promptId"], "peer-human");
        assert_eq!(
            echo["params"]["update"]["_meta"]["hideFromScrollback"],
            false
        );
        assert_eq!(
            echo["params"]["update"]["content"]["text"],
            "Human from another client"
        );
        for target in ["different-prompt", "peer-human"] {
            manager
                .handle_cancel(
                    parse_cancel_notification(
                        &json!({"sessionId":"session-1", "_meta":{"promptId":target}}),
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            let interrupted = node.execute(&format!(r#"{{AgentRequest(filter:{{request_id:{{_eq:"{}"}}}}){{interrupt_requested_at}}}}"#, escape_graphql_string(&request))).await;
            ensure_no_errors(&interrupted, "read viewer cancel intent").unwrap();
            assert_eq!(
                interrupted.data.as_ref().unwrap()["AgentRequest"][0]["interrupt_requested_at"]
                    .as_str()
                    .is_some(),
                target == "peer-human"
            );
        }
        // The runtime, not the viewer, acknowledges cancellation.
        terminalize_request(&node, &request, "interrupted").await;
        manager
            .observe_session_tick(
                "session-1",
                "1970-01-01T00:00:00Z",
                &mut after,
                &mut delivery_after,
                &sender,
                &engine,
            )
            .await
            .unwrap();
        let events: Vec<Value> = buffer
            .lock()
            .await
            .iter()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            events
                .iter()
                .filter(|event| event["params"]["update"]["sessionUpdate"] == "user_message_chunk")
                .count(),
            1
        );
        let completed = events
            .iter()
            .find(|event| event["params"]["update"]["sessionUpdate"] == "turn_completed")
            .unwrap();
        assert_eq!(completed["params"]["update"]["prompt_id"], "peer-human");
        assert_eq!(completed["params"]["update"]["stop_reason"], "cancelled");
    }

    #[tokio::test]
    async fn autonomous_turn_defers_late_notices_and_plain_escape_cancels_its_owner() {
        let (_tempdir, node, agent_did) = test_node().await;
        let graphql = spawn_mock_graphql(node.clone()).await;
        let manager = TurnManager::new(node.clone(), test_config(graphql, &agent_did));
        let engine = test_engine(node.clone());
        let (buffer, sender) = buffer_sender();
        let prompt = parse_prompt_request(
            &json!({"sessionId": "session-1",
            "prompt": [text_block("internal wake instruction")]}),
            None,
        )
        .unwrap();
        let root = manager.submit_request(&prompt, "root").await.unwrap();
        terminalize_request(&node, &root, "completed").await;
        manager.observed.lock().await.insert(
            ("session-1".into(), root.clone()),
            Arc::new(Mutex::new(ObservedRequest::new("root".into(), 0, true))),
        );
        let first = seed_runtime_wake(&node, &agent_did, "internal wake instruction").await;
        seed_assistant_message(&node, &first, 1, "Wake A is working.").await;
        seed_tool_call(&node, &first, "wake-a-tool", "bash", "running", "", None).await;
        let mut after = String::new();
        let mut delivery_after = String::new();
        manager
            .observe_session_tick(
                "session-1",
                "1970-01-01T00:00:00Z",
                &mut after,
                &mut delivery_after,
                &sender,
                &engine,
            )
            .await
            .unwrap();
        assert_eq!(
            manager.autonomous_delivery.lock().await.get("session-1"),
            Some(&first)
        );

        // Completion notices on both the older root and the active wake must
        // wait; the pager treats either user chunk as a destructive boundary.
        for (request, sequence, text) in [
            (&root, 2, "Root task finished."),
            (&first, 3, "Wake task finished."),
        ] {
            let result = node
                .execute(&format!(
                    r#"mutation {{ create_AgentMessage(input: {{
                message_key: "background-completion-notification:{sequence}:tool",
                session_id: "session-1", request_id: "{}", sequence: {sequence},
                agent_did: "did:test:grok-shim", requester_did: "did:test:grok-shim",
                role: "user", content: "{}" }}) {{ _docID }} }}"#,
                    escape_graphql_string(request),
                    escape_graphql_string(text)
                ))
                .await;
            ensure_no_errors(&result, "seed delayed completion notice").unwrap();
        }
        let second = seed_runtime_wake(&node, &agent_did, "internal wake instruction").await;
        seed_assistant_message(&node, &second, 4, "Wake B response.").await;
        terminalize_request(&node, &second, "completed").await;
        seed_assistant_message(&node, &first, 5, "Wake A continues.").await;
        manager
            .observe_session_tick(
                "session-1",
                "1970-01-01T00:00:00Z",
                &mut after,
                &mut delivery_after,
                &sender,
                &engine,
            )
            .await
            .unwrap();
        let updates = parse_buffered_updates(&buffer).await;
        let text: Vec<_> = updates
            .iter()
            .filter_map(|event| {
                event
                    .pointer("/params/update/content/text")
                    .and_then(Value::as_str)
            })
            .collect();
        assert!(text.contains(&"Wake A continues."));
        assert!(!text.contains(&"Root task finished."));
        assert!(!text.contains(&"Wake task finished."));
        assert!(!text.contains(&"Wake B response."));

        // Normal Esc carries no promptId. It must interrupt the visible
        // autonomous request, even though no foreground RPC is pending.
        manager
            .handle_cancel(parse_cancel_notification(&json!({"sessionId": "session-1"})).unwrap())
            .await
            .unwrap();
        let interrupted = node
            .execute(&format!(
                r#"{{ AgentRequest(filter: {{ request_id: {{ _eq: "{}" }} }})
            {{ interrupt_requested_at }} }}"#,
                escape_graphql_string(&first)
            ))
            .await;
        ensure_no_errors(&interrupted, "read autonomous interrupt").unwrap();
        assert!(interrupted
            .data
            .as_ref()
            .and_then(|data| data.pointer("/AgentRequest/0/interrupt_requested_at"))
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty()));
        terminalize_request(&node, &first, "interrupted").await;
        for _ in 0..3 {
            manager
                .observe_session_tick(
                    "session-1",
                    "1970-01-01T00:00:00Z",
                    &mut after,
                    &mut delivery_after,
                    &sender,
                    &engine,
                )
                .await
                .unwrap();
        }
        let updates = parse_buffered_updates(&buffer).await;
        let first_complete = updates
            .iter()
            .position(|event| {
                event
                    .pointer("/params/update/prompt_id")
                    .and_then(Value::as_str)
                    == Some(format!("notifications-{first}").as_str())
            })
            .unwrap();
        let second_start = updates
            .iter()
            .position(|event| {
                event
                    .pointer("/params/update/content/text")
                    .and_then(Value::as_str)
                    == Some("Wake B response.")
            })
            .unwrap();
        for notice in ["Root task finished.", "Wake task finished."] {
            let positions: Vec<_> = updates
                .iter()
                .enumerate()
                .filter_map(|(index, event)| {
                    (event
                        .pointer("/params/update/content/text")
                        .and_then(Value::as_str)
                        == Some(notice))
                    .then_some(index)
                })
                .collect();
            assert_eq!(
                positions.len(),
                1,
                "each deferred notice survives the message high-water and is delivered once"
            );
            assert!(first_complete < positions[0] && positions[0] < second_start);
        }
        assert!(manager.autonomous_delivery.lock().await.is_empty());
    }

    #[tokio::test]
    async fn child_pane_streams_followups_before_finish_and_excludes_foreign_requester() {
        let (_tempdir, node, agent_did) = test_node().await;
        let manager = TurnManager::new(node.clone(), test_config(String::new(), &agent_did));
        let engine = test_engine(node.clone());
        let (buffer, sender) = buffer_sender();
        let response = node
            .execute(
                r#"mutation { create_AgentRequest(input: {
            request_id: "pane-root", session_id: "session-1", agent_did: "did:test:grok-shim",
            requester_did: "did:test:grok-shim", lifecycle_state: "processing"
        }) { _docID } }"#,
            )
            .await;
        ensure_no_errors(&response, "seed pane root").unwrap();
        seed_tool_call(
            &node,
            "pane-root",
            "call-1",
            "spawn_subagent",
            "running",
            "",
            Some("pane-child"),
        )
        .await;
        seed_child_request(&node, "pane-root", "pane-child", "processing").await;
        for (id, requester, text) in [
            ("pane-child", None, "Original child output"),
            ("pane-followup", None, "Steered child output"),
            ("pane-foreign", Some("did:foreign"), "MUST NOT LEAK"),
        ] {
            if id != "pane-child" {
                let requester = requester
                    .map(|did| format!("requester_did: \"{did}\""))
                    .unwrap_or_default();
                let response = node.execute(&format!(r#"mutation {{ create_AgentRequest(input: {{
                    request_id: "{id}", session_id: "session-1-child", agent_did: "did:test:grok-shim",
                    {requester}, lifecycle_state: "processing"
                }}) {{ _docID }} }}"#)).await;
                ensure_no_errors(&response, "seed child followup").unwrap();
            }
            let content = escape_graphql_string(
                &serde_json::to_string(&gents_protocol::message::Message::assistant(text)).unwrap(),
            );
            let response = node.execute(&format!(r#"mutation {{ create_AgentMessage(input: {{
                message_key: "{id}:1", request_id: "{id}", session_id: "session-1-child",
                agent_did: "did:test:grok-shim", sequence: 1, role: "assistant", content: "{content}"
            }}) {{ _docID }} }}"#)).await;
            ensure_no_errors(&response, "seed child transcript").unwrap();
        }
        let response = node
            .execute(
                r#"mutation { create_AgentToolCall(input: {
            tool_call_key: "session-1-child:child-bash", tool_call_id: "child-bash",
            request_id: "pane-child", session_id: "session-1-child",
            agent_did: "did:test:grok-shim", tool_name: "bash", lifecycle_state: "running",
            await_mode: "background", started_at: "2026-01-01T00:00:00Z",
            args: "{\"command\":\"echo CHILD_BG_OUTPUT\"}",
            partial_output_tail: "CHILD_BG_OUTPUT"
        }) { _docID } }"#,
            )
            .await;
        ensure_no_errors(&response, "scope child tool").unwrap();
        seed_assistant_message(&node, "pane-root", 7, "PARENT MUST WAIT").await;
        let cursor = Mutex::new(RequestCursor::new());
        let mut timing = RequestUpdateTiming::new(0);
        manager
            .stream_projection_updates_mode(
                "session-1",
                "pane-root",
                Some("parent-prompt"),
                &sender,
                &engine,
                &cursor,
                &mut timing,
                false,
                0,
                true,
            )
            .await
            .unwrap();
        let activity = parse_buffered_updates(&buffer).await;
        let task_start = activity
            .iter()
            .position(|row| {
                row["params"]["update"]["sessionUpdate"] == "task_backgrounded"
                    && row["params"]["sessionId"] == "session-1-child"
            })
            .unwrap();
        let output = activity
            .iter()
            .position(|row| {
                row["params"]["update"]["sessionUpdate"] == "monitor_event"
                    && row["params"]["sessionId"] == "session-1-child"
            })
            .unwrap();
        assert!(task_start < output);
        assert_eq!(
            activity[output]["params"]["update"]["event_text"],
            "CHILD_BG_OUTPUT"
        );
        assert!(activity
            .iter()
            .any(|row| row["params"]["sessionId"] == "session-1-child"
                && row["params"]["update"]["toolCallId"] == "child-bash"));
        assert!(!activity.iter().any(|row| row
            .pointer("/params/update/content/text")
            .and_then(Value::as_str)
            == Some("PARENT MUST WAIT")));
        for row in activity
            .iter()
            .filter(|row| row["params"]["sessionId"] == "session-1")
        {
            assert!(row.pointer("/params/_meta/promptId").is_none());
            assert!(row.pointer("/params/_meta/turnStartMs").is_none());
        }
        complete_child_request(&node, "pane-child").await;
        complete_tool_call(&node, "child-bash", "child tool done").await;
        complete_tool_call(&node, "call-1", "done").await;
        manager
            .stream_projection_updates(
                "session-1",
                "pane-root",
                None,
                &sender,
                &engine,
                &cursor,
                &mut timing,
                true,
                0,
            )
            .await
            .unwrap();
        assert!(!buffered_update_kinds(&buffer)
            .await
            .iter()
            .any(|kind| kind == "subagent_finished"));
        complete_child_request(&node, "pane-followup").await;
        manager
            .stream_projection_updates(
                "session-1",
                "pane-root",
                None,
                &sender,
                &engine,
                &cursor,
                &mut timing,
                true,
                0,
            )
            .await
            .unwrap();
        let updates = parse_buffered_updates(&buffer).await;
        let position = |kind: &str| {
            updates
                .iter()
                .position(|row| row["params"]["update"]["sessionUpdate"] == kind)
                .unwrap()
        };
        let spawned = position("subagent_spawned");
        let finished = position("subagent_finished");
        let child_tools: Vec<_> = updates
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                row["params"]["sessionId"] == "session-1-child"
                    && row["params"]["update"]["toolCallId"] == "child-bash"
            })
            .collect();
        assert!(
            !child_tools.is_empty(),
            "child pane receives native tool frames"
        );
        assert!(child_tools
            .iter()
            .all(|(index, _)| spawned < *index && *index < finished));
        for text in ["Original child output", "Steered child output"] {
            let matches: Vec<_> = updates
                .iter()
                .enumerate()
                .filter(|(_, row)| row["params"]["update"]["content"]["text"] == text)
                .collect();
            assert_eq!(matches.len(), 1, "child cursor must not replay {text}");
            assert!(spawned < matches[0].0 && matches[0].0 < finished);
            assert_eq!(matches[0].1["params"]["sessionId"], "session-1-child");
        }
        assert!(!updates
            .iter()
            .any(|row| row.to_string().contains("MUST NOT LEAK")));
    }

    /// Foreground completion transfers its exact cursor; late tool updates
    /// and durable wake results remain visible without replaying old output.
    #[tokio::test]
    async fn session_observer_preserves_handoff_and_delivers_late_wake() {
        let (_tempdir, node, agent_did) = test_node().await;
        let graphql = spawn_mock_graphql(node.clone()).await;
        let manager = Arc::new(TurnManager::new(
            node.clone(),
            test_config(graphql, &agent_did),
        ));
        let engine = test_engine(node.clone());
        let (buffer, sender) = buffer_sender();
        let prompt = parse_prompt_request(
            &json!({
                "sessionId": "session-1", "prompt": [text_block("start")],
                "_meta": {"promptId": "foreground-1"},
            }),
            Some(json!(1)),
        )
        .unwrap();
        let turn = {
            let manager = manager.clone();
            let engine = engine.clone();
            let sender = sender.clone();
            tokio::spawn(async move { manager.handle_prompt(prompt, &sender, &engine).await })
        };
        let root = wait_for_pending_request(&node).await;
        seed_tool_call(&node, &root, "late-bash", "bash", "running", "", None).await;
        seed_assistant_message(&node, &root, 1, "Root response.").await;
        let mut after = String::new();
        let mut delivery_after = String::new();
        // A discovery poll while foreground ownership is installed cannot
        // acquire another cursor for the submitted request.
        manager
            .observe_session_tick(
                "session-1",
                "1970-01-01T00:00:00Z",
                &mut after,
                &mut delivery_after,
                &sender,
                &engine,
            )
            .await
            .unwrap();
        terminalize_request(&node, &root, "completed").await;
        let result = tokio::time::timeout(Duration::from_secs(30), turn)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(result["stopReason"], "end_turn");

        let update = node
            .execute(
                r#"mutation { update_AgentToolCall(
            filter: { tool_call_id: { _eq: "late-bash" } },
            input: { lifecycle_state: "failed", result: "exit 7" }) { _docID } }"#,
            )
            .await;
        ensure_no_errors(&update, "late tool finish").unwrap();
        let wake = seed_runtime_wake(&node, &agent_did, "internal notification instruction").await;
        seed_assistant_message(&node, &wake, 2, "Background work finished.").await;
        terminalize_request(&node, &wake, "completed").await;
        let (pending_tx, _pending_rx) = oneshot::channel();
        let foreground_key = ("session-1".to_owned(), "new-foreground".to_owned());
        manager.state.lock().await.entries.insert(
            foreground_key.clone(),
            PendingPrompt {
                response_tx: Some(pending_tx),
                request_id: Some("new-foreground-request".to_owned()),
                cancel_before_id: Arc::new(Mutex::new(CancelBeforeIdLatch::default())),
                drained: false,
            },
        );
        manager
            .observe_session_tick(
                "session-1",
                "1970-01-01T00:00:00Z",
                &mut after,
                &mut delivery_after,
                &sender,
                &engine,
            )
            .await
            .unwrap();
        assert!(
            !parse_buffered_updates(&buffer)
                .await
                .iter()
                .any(|event| event
                    .pointer("/params/update/content/text")
                    .and_then(Value::as_str)
                    == Some("Background work finished.")),
            "wake output must remain undelivered while the pager has a foreground prompt"
        );
        assert!(
            parse_buffered_updates(&buffer).await.iter().any(|event| {
                event
                    .pointer("/params/update/toolCallId")
                    .and_then(Value::as_str)
                    == Some("late-bash")
                    && event
                        .pointer("/params/update/status")
                        .and_then(Value::as_str)
                        == Some("failed")
            }),
            "background terminal updates must flow while a new foreground is pending"
        );
        manager.state.lock().await.entries.remove(&foreground_key);
        // Pending removal precedes response enqueue in the live delegate.
        // Its RAII transport guard must cover this remaining delivery gap.
        let response_delivery = manager.begin_foreground_delivery("session-1").await;
        manager
            .observe_session_tick(
                "session-1",
                "1970-01-01T00:00:00Z",
                &mut after,
                &mut delivery_after,
                &sender,
                &engine,
            )
            .await
            .unwrap();
        assert!(!parse_buffered_updates(&buffer).await.iter().any(|event| {
            event
                .pointer("/params/update/content/text")
                .and_then(Value::as_str)
                == Some("Background work finished.")
        }));
        drop(response_delivery);
        for _ in 0..2 {
            manager
                .observe_session_tick(
                    "session-1",
                    "1970-01-01T00:00:00Z",
                    &mut after,
                    &mut delivery_after,
                    &sender,
                    &engine,
                )
                .await
                .unwrap();
        }
        let updates = parse_buffered_updates(&buffer).await;
        let text: Vec<_> = updates
            .iter()
            .filter_map(|event| {
                event
                    .pointer("/params/update/content/text")
                    .and_then(Value::as_str)
            })
            .collect();
        assert_eq!(
            text.iter()
                .filter(|value| **value == "Root response.")
                .count(),
            1
        );
        assert_eq!(
            text.iter()
                .filter(|value| **value == "Background work finished.")
                .count(),
            1
        );
        assert!(!text.contains(&"internal notification instruction"));
        let failed = updates
            .iter()
            .find(|event| {
                event
                    .pointer("/params/update/status")
                    .and_then(Value::as_str)
                    == Some("failed")
            })
            .unwrap();
        assert!(
            failed.pointer("/params/_meta/promptId").is_none(),
            "late root update must pass the pager's idle prompt gate"
        );
        let completions: Vec<_> = updates
            .iter()
            .enumerate()
            .filter(|(_, event)| {
                event
                    .pointer("/params/update/sessionUpdate")
                    .and_then(Value::as_str)
                    == Some("turn_completed")
            })
            .collect();
        assert_eq!(completions.len(), 1);
        let (completion_index, completion) = completions[0];
        assert_eq!(completion["method"], "_x.ai/session_notification");
        assert_eq!(
            completion["params"]["update"]["prompt_id"],
            format!("notifications-{wake}")
        );
        assert_eq!(
            completion["params"]["_meta"]["promptId"],
            format!("notifications-{wake}")
        );
        assert!(updates[..completion_index].iter().any(|event| {
            event
                .pointer("/params/update/content/text")
                .and_then(Value::as_str)
                == Some("Background work finished.")
        }));

        manager
            .observe_session("session-1", sender.clone(), engine.clone())
            .await;
        // Model an observer blocked on outbound delivery: admission must wait
        // on its independent gate, while disconnect can still close/drain.
        let delivery = manager.delivery_gate.lock().await;
        let prompt = parse_prompt_request(
            &json!({
                "sessionId": "session-1", "prompt": [text_block("after wake")],
            }),
            None,
        )
        .unwrap();
        let mut admission = Box::pin(manager.handle_prompt(prompt, &sender, &engine));
        tokio::select! {
            biased;
            result = &mut admission => panic!("admission crossed delivery gate: {result:?}"),
            _ = tokio::task::yield_now() => {}
        }
        assert!(manager.state.lock().await.entries.is_empty());
        tokio::time::timeout(Duration::from_secs(5), manager.handle_disconnect())
            .await
            .unwrap()
            .unwrap();
        assert!(manager.observers.lock().await.is_empty());
        drop(delivery);
        assert!(admission
            .await
            .unwrap_err()
            .to_string()
            .contains("disconnected"));
    }

    /// A live turn streams every novel durable projection update before the
    /// deferred response: the tool call registers, the subagent lifecycle
    /// appears, and the assistant message chunk streams — each exactly once,
    /// in tools → subagents → messages order within a poll — with no
    /// duplicates even though the watch polls many times.
    #[tokio::test]
    async fn live_turn_streams_tool_subagent_and_message_updates_once_each() {
        let (_tempdir, node, agent_did) = test_node().await;
        let graphql = spawn_mock_graphql(node.clone()).await;
        let manager = Arc::new(TurnManager::new(
            node.clone(),
            test_config(graphql, &agent_did),
        ));
        let engine = test_engine(node.clone());
        let (buffer, sender) = buffer_sender();

        let prompt = parse_prompt_request(
            &json!({
                "sessionId": "session-1",
                "prompt": [text_block("hello")],
                "_meta": {"promptId": "prompt-1"},
            }),
            Some(json!(1)),
        )
        .unwrap();

        // Seeding task: wait for the submitted request row, then materialize
        // the turn's durable output in stages so at least one poll observes
        // the intermediate (non-terminal) state, and finish with a
        // terminalization.
        let node_for_seed = node.clone();
        let seed_handle = tokio::spawn(async move {
            let request_id = wait_for_pending_request(&node_for_seed).await;
            // Stage 1: an in-flight tool call and a running child request —
            // observed by at least one non-terminal poll.
            seed_tool_call(
                &node_for_seed,
                &request_id,
                "call-1",
                "read_file",
                "running",
                "",
                Some("child-1"),
            )
            .await;
            seed_child_request(&node_for_seed, &request_id, "child-1", "processing").await;
            tokio::time::sleep(Duration::from_millis(600)).await;
            // Stage 2: the terminal tool status, the finished child, the
            // assistant output, and the request's terminal state.
            complete_tool_call(&node_for_seed, "call-1", "file contents").await;
            complete_child_request(&node_for_seed, "child-1").await;
            seed_assistant_message(&node_for_seed, &request_id, 1, "the answer").await;
            terminalize_request(&node_for_seed, &request_id, "completed").await;
        });

        let result = tokio::time::timeout(
            Duration::from_secs(30),
            manager.handle_prompt(prompt, &sender, &engine),
        )
        .await
        .expect("prompt should resolve within timeout")
        .expect("prompt should succeed");
        assert_eq!(result["stopReason"], json!("end_turn"));
        seed_handle.await.expect("seed task");

        let kinds = buffered_update_kinds(&buffer).await;
        // The user echo always streams first.
        assert_eq!(
            kinds.first().map(String::as_str),
            Some("user_message_chunk")
        );
        // Every projection family streamed: tool call (base registration,
        // status revision, commands), subagent (spawned, progress,
        // finished), and the assistant message chunk.
        for expected in [
            "tool_call",
            "tool_call_update",
            "available_commands_update",
            "subagent_spawned",
            "subagent_progress",
            "subagent_finished",
            "agent_message_chunk",
        ] {
            assert!(
                kinds.iter().any(|kind| kind == expected),
                "expected a {expected} notification, got: {kinds:?}"
            );
        }
        // No duplicates: each projected event streamed exactly once.
        for kind in [
            "tool_call",
            "available_commands_update",
            "subagent_spawned",
            "subagent_finished",
            "agent_message_chunk",
        ] {
            let count = kinds.iter().filter(|k| k == &kind).count();
            assert_eq!(count, 1, "{kind} must stream exactly once, got {count}");
        }
        // Every notification is well-formed and carries the turn's promptId.
        let updates = parse_buffered_updates(&buffer).await;
        // Output refinement and lifecycle completion are separate deliveries:
        // the pager receives the result first, then a dedicated status-only
        // update for the same card. Neither repeats on subsequent polls.
        let revisions: Vec<_> = updates
            .iter()
            .filter_map(|event| {
                let update = event.pointer("/params/update")?;
                (update.get("sessionUpdate").and_then(Value::as_str) == Some("tool_call_update"))
                    .then_some(update)
            })
            .collect();
        assert_eq!(
            revisions,
            vec![
                &json!({
                    "sessionUpdate": "tool_call_update", "toolCallId": "call-1",
                    "content": [{"type": "content", "content": {"type": "text", "text": "file contents"}}],
                    "rawOutput": {"output": "file contents"},
                }),
                &json!({
                    "sessionUpdate": "tool_call_update", "toolCallId": "call-1",
                    "status": "completed",
                }),
            ]
        );
        assert!(
            updates.len() >= 8,
            "expected the full stream, got {updates:?}"
        );
        for update in &updates {
            let kind = update["params"]["update"]["sessionUpdate"]
                .as_str()
                .expect("sessionUpdate kind");
            let expected_method = if kind.starts_with("subagent_") {
                "_x.ai/session_notification"
            } else {
                "session/update"
            };
            assert_eq!(update["method"], expected_method);
            assert_eq!(update["params"]["sessionId"], "session-1");
            assert_eq!(update["params"]["_meta"]["promptId"], "prompt-1");
            assert!(
                update["params"]["_meta"]["eventId"].as_str().is_some(),
                "every notification carries an eventId"
            );
        }
    }

    /// Distinct durable rows with identical text both stream: the message
    /// dedup is by durable row key, never by content.
    #[tokio::test]
    async fn identical_text_distinct_rows_both_stream() {
        let (_tempdir, node, agent_did) = test_node().await;
        let graphql = spawn_mock_graphql(node.clone()).await;
        let manager = TurnManager::new(node.clone(), test_config(graphql, &agent_did));
        let engine = test_engine(node.clone());
        let (buffer, sender) = buffer_sender();

        let prompt = parse_prompt_request(
            &json!({
                "sessionId": "session-1",
                "prompt": [text_block("hello")],
                "_meta": {"promptId": "prompt-1"},
            }),
            Some(json!(1)),
        )
        .unwrap();

        let node_for_seed = node.clone();
        let seed_handle = tokio::spawn(async move {
            let request_id = wait_for_pending_request(&node_for_seed).await;
            // Two distinct assistant rows carrying the same text.
            seed_assistant_message(&node_for_seed, &request_id, 1, "same text").await;
            seed_assistant_message(&node_for_seed, &request_id, 2, "same text").await;
            terminalize_request(&node_for_seed, &request_id, "completed").await;
        });

        let result = tokio::time::timeout(
            Duration::from_secs(30),
            manager.handle_prompt(prompt, &sender, &engine),
        )
        .await
        .expect("prompt should resolve within timeout")
        .expect("prompt should succeed");
        assert_eq!(result["stopReason"], json!("end_turn"));
        seed_handle.await.expect("seed task");

        let updates = parse_buffered_updates(&buffer).await;
        let chunks: Vec<&str> = updates
            .iter()
            .filter(|update| update["params"]["update"]["sessionUpdate"] == "agent_message_chunk")
            .filter_map(|update| update["params"]["update"]["content"]["text"].as_str())
            .collect();
        assert_eq!(
            chunks,
            vec!["same text", "same text"],
            "both distinct rows must stream even with identical text"
        );
    }

    /// An outbound that closes after submission (mid-turn) interrupts the
    /// submitted request and drains the pending entry, so the session
    /// accepts the next prompt immediately.
    #[tokio::test]
    async fn outbound_close_after_submission_interrupts_and_drains() {
        let (_tempdir, node, agent_did) = test_node().await;
        let graphql = spawn_mock_graphql(node.clone()).await;
        let manager = Arc::new(TurnManager::new(
            node.clone(),
            test_config(graphql, &agent_did),
        ));
        let engine = test_engine(node.clone());
        let (frames_tx, frames_rx) = tokio::sync::mpsc::unbounded_channel::<
            crate::commands::grok_shim::protocol::ServerEnvelope,
        >();
        let sender = PromptSender::Live {
            outbound: AcpOutbound::for_frames(frames_tx),
        };

        let prompt = parse_prompt_request(
            &json!({
                "sessionId": "session-1",
                "prompt": [text_block("hello")],
                "_meta": {"promptId": "prompt-1"},
            }),
            Some(json!(1)),
        )
        .unwrap();

        // Close the outbound only after the user echo was delivered, and
        // concurrently materialize a durable tool row so a later poll has a
        // novel event to send: that projection send then fails against the
        // closed outbound, which must interrupt the request and drain the
        // entry.
        let mut frames_rx = frames_rx;
        let outbound_closed = Arc::new(tokio::sync::Notify::new());
        let closed_signal = outbound_closed.clone();
        let closer = tokio::spawn(async move {
            // Wait for the echo frame, then close the channel.
            let _ = frames_rx.recv().await;
            drop(frames_rx);
            closed_signal.notify_one();
        });
        let node_for_seed = node.clone();
        let seed_handle = tokio::spawn(async move {
            let request_id = wait_for_pending_request(&node_for_seed).await;
            outbound_closed.notified().await;
            seed_tool_call(
                &node_for_seed,
                &request_id,
                "call-1",
                "read_file",
                "running",
                "",
                None,
            )
            .await;
        });

        let result = tokio::time::timeout(
            Duration::from_secs(30),
            manager.handle_prompt(prompt, &sender, &engine),
        )
        .await
        .expect("prompt should resolve within timeout");
        assert!(result.is_err(), "a closed outbound must fail the turn");
        closer.await.expect("closer task");
        seed_handle.await.expect("seed task");

        // The submitted request was interrupted.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let query = r#"{ AgentRequest { request_id interrupt_requested_at } }"#;
        let response = node.execute(query).await;
        ensure_no_errors(&response, "test request query").expect("query");
        let rows = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        assert!(
            rows.iter().any(|row| row
                .get("interrupt_requested_at")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty())),
            "the submitted request must be interrupted after the mid-turn send failure"
        );

        // The pending entry was drained: the session accepts a new prompt
        // (it terminalizes immediately through the mock endpoint's real
        // submission and a terminalizer).
        let second = parse_prompt_request(
            &json!({
                "sessionId": "session-1",
                "prompt": [text_block("again")],
                "_meta": {"promptId": "prompt-2"},
            }),
            Some(json!(2)),
        )
        .unwrap();
        let node_for_terminalize = node.clone();
        let terminalize_handle = tokio::spawn(async move {
            loop {
                let query = r#"{ AgentRequest(filter: { lifecycle_state: { _eq: "pending" } }) { request_id } }"#;
                let response = node_for_terminalize.execute(query).await;
                let rows = response
                    .data
                    .as_ref()
                    .and_then(|data| data.get("AgentRequest"))
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                for row in &rows {
                    let request_id = row.get("request_id").and_then(Value::as_str).unwrap();
                    terminalize_request(&node_for_terminalize, request_id, "interrupted").await;
                }
                if rows.is_empty() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        });
        let (buffer2, sender2) = buffer_sender();
        let second_result = tokio::time::timeout(
            Duration::from_secs(30),
            manager.handle_prompt(second, &sender2, &engine),
        )
        .await
        .expect("second prompt should resolve within timeout")
        .expect("second prompt must be accepted after the drain");
        drop(buffer2);
        terminalize_handle.abort();
        assert_eq!(second_result["stopReason"], json!("cancelled"));
    }

    /// Wait for the first pending `AgentRequest` row and return its id.
    async fn wait_for_pending_request(node: &Arc<EmbeddedNode>) -> String {
        loop {
            let query = r#"{ AgentRequest(filter: { lifecycle_state: { _eq: "pending" } }) { request_id } }"#;
            let response = node.execute(query).await;
            let rows = response
                .data
                .as_ref()
                .and_then(|data| data.get("AgentRequest"))
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if let Some(row) = rows.first() {
                return row
                    .get("request_id")
                    .and_then(Value::as_str)
                    .unwrap()
                    .to_string();
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Cancel before the request id is registered: the entry is drained, the
    /// connected prompt resolves `stopReason=cancelled`, and the next prompt
    /// for the session is accepted (reuse). The gated mock endpoint holds the
    /// submitter inside `create_agent_request`, so the cancel deterministically
    /// lands in the before-request-id window.
    #[tokio::test]
    async fn cancel_before_request_id_resolves_cancelled_and_permits_reuse() {
        let (_tempdir, node, agent_did) = test_node().await;
        let submission_arrived = Arc::new(tokio::sync::Notify::new());
        let submission_release = Arc::new(tokio::sync::Notify::new());
        let gate_armed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let graphql = spawn_gated_mock_graphql(
            node.clone(),
            Some((
                submission_arrived.clone(),
                submission_release.clone(),
                gate_armed,
            )),
        )
        .await;
        let manager = Arc::new(TurnManager::new(
            node.clone(),
            test_config(graphql, &agent_did),
        ));
        let engine = test_engine(node.clone());
        let (_buffer, sender) = buffer_sender();

        let prompt = parse_prompt_request(
            &json!({
                "sessionId": "session-1",
                "prompt": [text_block("hello")],
                "_meta": {"promptId": "prompt-1"},
            }),
            Some(json!(1)),
        )
        .unwrap();

        // Run the prompt; it parks inside the gated submission with its
        // pending entry inserted and no request id registered yet.
        let prompt_handle = tokio::spawn({
            let manager = manager.clone();
            let sender = sender.clone();
            let engine = engine.clone();
            async move { manager.handle_prompt(prompt, &sender, &engine).await }
        });

        // Deterministically wait for the submission mutation to arrive: the
        // submitter is now inside create_agent_request, strictly before the
        // request id is registered.
        tokio::time::timeout(Duration::from_secs(30), submission_arrived.notified())
            .await
            .expect("submission should arrive at the gated endpoint");

        // Cancel while the submitter is parked: this drains the pending entry
        // and latches cancel-before-id.
        let cancel = parse_cancel_notification(&json!({
            "sessionId": "session-1",
            "_meta": {"cancelSubagents": true, "promptId": "prompt-1"},
        }))
        .unwrap();
        manager
            .handle_cancel(cancel)
            .await
            .expect("cancel should succeed");

        // Release the submission; the submitter observes the latch and
        // resolves the prompt with `stopReason=cancelled`.
        submission_release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(30), prompt_handle)
            .await
            .expect("prompt should resolve within timeout")
            .expect("prompt task")
            .expect("cancel-before-id must resolve cancelled, not error");

        assert_eq!(result["stopReason"], json!("cancelled"));

        // Reuse: the session accepts the next prompt. Run the second prompt
        // and a terminalizer concurrently: any request still pending (the
        // orphaned first submission and/or the second prompt's fresh one) is
        // terminalized as interrupted so the second prompt resolves.
        let second = parse_prompt_request(
            &json!({
                "sessionId": "session-1",
                "prompt": [text_block("again")],
                "_meta": {"promptId": "prompt-2"},
            }),
            Some(json!(2)),
        )
        .unwrap();
        let node_for_terminalize = node.clone();
        let terminalize_handle = tokio::spawn(async move {
            loop {
                let query = r#"{ AgentRequest(filter: { lifecycle_state: { _eq: "pending" } }) { request_id } }"#;
                let response = node_for_terminalize.execute(query).await;
                let rows = response
                    .data
                    .as_ref()
                    .and_then(|data| data.get("AgentRequest"))
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                for row in &rows {
                    let request_id = row.get("request_id").and_then(Value::as_str).unwrap();
                    terminalize_request(&node_for_terminalize, request_id, "interrupted").await;
                }
                if rows.is_empty() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        });
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            manager.handle_prompt(second, &sender, &engine),
        )
        .await
        .expect("second prompt should resolve within timeout")
        .expect("second prompt should succeed");
        assert_eq!(result["stopReason"], json!("cancelled"));
        terminalize_handle.abort();
    }

    /// A peer may reuse the exact `(sessionId, promptId)` immediately after
    /// cancelling a submission whose request id is still unknown. When the old
    /// submission later succeeds, its Arc latch identity must not register the
    /// old request id on, drain, or stream through the replacement entry.
    #[tokio::test]
    async fn cancelled_submission_cannot_capture_an_exact_key_replacement() {
        let (_tempdir, node, agent_did) = test_node().await;
        let submission_arrived = Arc::new(tokio::sync::Notify::new());
        let submission_release = Arc::new(tokio::sync::Notify::new());
        let gate_armed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let graphql = spawn_gated_mock_graphql(
            node.clone(),
            Some((
                submission_arrived.clone(),
                submission_release.clone(),
                gate_armed,
            )),
        )
        .await;
        let manager = Arc::new(TurnManager::new(
            node.clone(),
            test_config(graphql, &agent_did),
        ));
        let engine = test_engine(node.clone());
        let (_buffer, sender) = buffer_sender();

        let prompt = || {
            parse_prompt_request(
                &json!({
                    "sessionId": "session-reuse",
                    "prompt": [text_block("hello")],
                    "_meta": {"promptId": "same-prompt"},
                }),
                Some(json!(1)),
            )
            .expect("prompt")
        };
        let first = tokio::spawn({
            let manager = manager.clone();
            let sender = sender.clone();
            let engine = engine.clone();
            let prompt = prompt();
            async move { manager.handle_prompt(prompt, &sender, &engine).await }
        });
        tokio::time::timeout(Duration::from_secs(30), submission_arrived.notified())
            .await
            .expect("first submission reached gate");

        let cancel = parse_cancel_notification(&json!({
            "sessionId": "session-reuse",
            "_meta": {"promptId": "same-prompt"},
        }))
        .expect("cancel");
        manager.handle_cancel(cancel).await.expect("cancel first");

        let replacement = tokio::spawn({
            let manager = manager.clone();
            let sender = sender.clone();
            let engine = engine.clone();
            let prompt = prompt();
            async move { manager.handle_prompt(prompt, &sender, &engine).await }
        });
        let replacement_request_id = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let request_id = manager
                    .state
                    .lock()
                    .await
                    .entries
                    .get(&("session-reuse".to_string(), "same-prompt".to_string()))
                    .and_then(|entry| entry.request_id.clone());
                if let Some(request_id) = request_id {
                    break request_id;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("replacement request id registered");

        submission_release.notify_one();
        let first_result = tokio::time::timeout(Duration::from_secs(30), first)
            .await
            .expect("old prompt resolves")
            .expect("old prompt task")
            .expect("old prompt result");
        assert_eq!(first_result["stopReason"], json!("cancelled"));
        assert_eq!(
            manager
                .state
                .lock()
                .await
                .entries
                .get(&("session-reuse".to_string(), "same-prompt".to_string()))
                .and_then(|entry| entry.request_id.as_deref()),
            Some(replacement_request_id.as_str()),
            "the old generation must not overwrite the exact-key replacement"
        );

        let cancel_replacement = parse_cancel_notification(&json!({
            "sessionId": "session-reuse",
            "_meta": {"promptId": "same-prompt"},
        }))
        .expect("cancel replacement");
        manager
            .handle_cancel(cancel_replacement)
            .await
            .expect("cancel replacement");
        let replacement_result = tokio::time::timeout(Duration::from_secs(30), replacement)
            .await
            .expect("replacement resolves")
            .expect("replacement task")
            .expect("replacement result");
        assert_eq!(replacement_result["stopReason"], json!("cancelled"));
    }

    /// The failure half of the exact-key generation race: after cancellation
    /// removes the first entry, a peer may immediately reuse the same key.
    /// When the old gated submission then fails, its cleanup must recognize
    /// the Arc generation mismatch and leave the replacement entry intact.
    #[tokio::test]
    async fn failed_submission_cannot_remove_an_exact_key_replacement() {
        let (_tempdir, node, agent_did) = test_node().await;
        let submission_arrived = Arc::new(tokio::sync::Notify::new());
        let submission_release = Arc::new(tokio::sync::Notify::new());
        let gate_armed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let submission_fail = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let graphql = spawn_gated_mock_graphql_with_failure(
            node.clone(),
            Some((
                submission_arrived.clone(),
                submission_release.clone(),
                gate_armed,
            )),
            Some(submission_fail),
        )
        .await;
        let manager = Arc::new(TurnManager::new(
            node.clone(),
            test_config(graphql, &agent_did),
        ));
        let engine = test_engine(node.clone());
        let (_buffer, sender) = buffer_sender();
        let prompt = || {
            parse_prompt_request(
                &json!({
                    "sessionId": "session-failed-reuse",
                    "prompt": [text_block("hello")],
                    "_meta": {"promptId": "same-prompt"},
                }),
                Some(json!(1)),
            )
            .expect("prompt")
        };

        let first = tokio::spawn({
            let manager = manager.clone();
            let sender = sender.clone();
            let engine = engine.clone();
            let prompt = prompt();
            async move { manager.handle_prompt(prompt, &sender, &engine).await }
        });
        tokio::time::timeout(Duration::from_secs(30), submission_arrived.notified())
            .await
            .expect("first submission reached gate");

        manager
            .handle_cancel(
                parse_cancel_notification(&json!({
                    "sessionId": "session-failed-reuse",
                    "_meta": {"promptId": "same-prompt"},
                }))
                .expect("cancel first"),
            )
            .await
            .expect("cancel first");

        let replacement = tokio::spawn({
            let manager = manager.clone();
            let sender = sender.clone();
            let engine = engine.clone();
            let prompt = prompt();
            async move { manager.handle_prompt(prompt, &sender, &engine).await }
        });
        let key = (
            "session-failed-reuse".to_string(),
            "same-prompt".to_string(),
        );
        let replacement_request_id = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(request_id) = manager
                    .state
                    .lock()
                    .await
                    .entries
                    .get(&key)
                    .and_then(|entry| entry.request_id.clone())
                {
                    break request_id;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("replacement request id registered");

        submission_release.notify_one();
        let first_result = tokio::time::timeout(Duration::from_secs(30), first)
            .await
            .expect("old prompt resolves")
            .expect("old prompt task")
            .expect("old failed submission resolves as cancelled");
        assert_eq!(first_result["stopReason"], json!("cancelled"));
        assert_eq!(
            manager
                .state
                .lock()
                .await
                .entries
                .get(&key)
                .and_then(|entry| entry.request_id.as_deref()),
            Some(replacement_request_id.as_str()),
            "the failed old generation must not remove its replacement"
        );

        manager
            .handle_cancel(
                parse_cancel_notification(&json!({
                    "sessionId": "session-failed-reuse",
                    "_meta": {"promptId": "same-prompt"},
                }))
                .expect("cancel replacement"),
            )
            .await
            .expect("cancel replacement");
        let replacement_result = tokio::time::timeout(Duration::from_secs(30), replacement)
            .await
            .expect("replacement resolves")
            .expect("replacement task")
            .expect("replacement result");
        assert_eq!(replacement_result["stopReason"], json!("cancelled"));
    }

    /// An explicit cancel snapshots an entry, then yields before draining it.
    /// If the old generation terminalizes and a peer reuses the exact key in
    /// that window, the stale cancel must not resolve or interrupt the
    /// replacement generation.
    #[tokio::test]
    async fn cancel_selection_cannot_drain_an_exact_key_replacement() {
        let (_tempdir, node, agent_did) = test_node().await;
        let manager = Arc::new(TurnManager::new(
            node,
            test_config("http://127.0.0.1:1/".to_string(), &agent_did),
        ));
        let key = ("session-reuse".to_string(), "same-prompt".to_string());
        let old_generation = Arc::new(Mutex::new(CancelBeforeIdLatch::default()));
        let (old_tx, mut old_rx) = oneshot::channel::<Result<Value>>();
        manager.state.lock().await.entries.insert(
            key.clone(),
            PendingPrompt {
                response_tx: Some(old_tx),
                request_id: Some("old-request".to_string()),
                cancel_before_id: old_generation,
                drained: false,
            },
        );

        let gate = TestGate {
            arrived: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
        };
        *manager.cancel_selection_gate.lock().await = Some(gate.clone());
        let cancel = parse_cancel_notification(&json!({
            "sessionId": "session-reuse",
            "_meta": {"promptId": "same-prompt"},
        }))
        .expect("cancel");
        let cancel_handle = tokio::spawn({
            let manager = manager.clone();
            async move { manager.handle_cancel(cancel).await }
        });
        tokio::time::timeout(Duration::from_secs(30), gate.arrived.notified())
            .await
            .expect("cancel should snapshot the old generation");

        let replacement_generation = Arc::new(Mutex::new(CancelBeforeIdLatch::default()));
        let (replacement_tx, mut replacement_rx) = oneshot::channel::<Result<Value>>();
        {
            let mut state = manager.state.lock().await;
            let mut old = state.entries.remove(&key).expect("old generation");
            old.resolve(Ok(json!({"stopReason": "end_turn"})));
            state.entries.insert(
                key.clone(),
                PendingPrompt {
                    response_tx: Some(replacement_tx),
                    request_id: Some("replacement-request".to_string()),
                    cancel_before_id: replacement_generation.clone(),
                    drained: false,
                },
            );
        }
        assert_eq!(
            old_rx
                .try_recv()
                .expect("old generation terminal response")
                .expect("old generation result")["stopReason"],
            json!("end_turn")
        );

        gate.release.notify_one();
        tokio::time::timeout(Duration::from_secs(30), cancel_handle)
            .await
            .expect("cancel should finish")
            .expect("cancel task")
            .expect("cancel result");

        {
            let state = manager.state.lock().await;
            let replacement = state.entries.get(&key).expect("replacement survives");
            assert!(Arc::ptr_eq(
                &replacement.cancel_before_id,
                &replacement_generation
            ));
            assert_eq!(
                replacement.request_id.as_deref(),
                Some("replacement-request")
            );
            assert!(!replacement.drained);
        }
        assert!(matches!(
            replacement_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));

        let mut replacement = manager
            .state
            .lock()
            .await
            .entries
            .remove(&key)
            .expect("replacement cleanup");
        replacement.resolve(Ok(json!({"stopReason": "cancelled"})));
        assert_eq!(
            replacement_rx
                .try_recv()
                .expect("replacement cleanup response")
                .expect("replacement cleanup result")["stopReason"],
            json!("cancelled")
        );
    }

    /// Disconnect before the request id is registered: the entry is drained
    /// and the prompt resolves `stopReason=cancelled`. The gated mock endpoint
    /// holds the submitter inside `create_agent_request`, so the disconnect
    /// deterministically lands in the before-request-id window.
    #[tokio::test]
    async fn disconnect_before_request_id_resolves_cancelled() {
        let (_tempdir, node, agent_did) = test_node().await;
        let submission_arrived = Arc::new(tokio::sync::Notify::new());
        let submission_release = Arc::new(tokio::sync::Notify::new());
        let gate_armed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let graphql = spawn_gated_mock_graphql(
            node.clone(),
            Some((
                submission_arrived.clone(),
                submission_release.clone(),
                gate_armed,
            )),
        )
        .await;
        let manager = Arc::new(TurnManager::new(
            node.clone(),
            test_config(graphql, &agent_did),
        ));
        let engine = test_engine(node.clone());
        let (_buffer, sender) = buffer_sender();

        let prompt = parse_prompt_request(
            &json!({
                "sessionId": "session-1",
                "prompt": [text_block("hello")],
                "_meta": {"promptId": "prompt-1"},
            }),
            Some(json!(1)),
        )
        .unwrap();

        // Run the prompt; it parks inside the gated submission with its
        // pending entry inserted and no request id registered yet.
        let prompt_handle = tokio::spawn({
            let manager = manager.clone();
            let sender = sender.clone();
            let engine = engine.clone();
            async move { manager.handle_prompt(prompt, &sender, &engine).await }
        });

        // Deterministically wait for the submission mutation to arrive.
        tokio::time::timeout(Duration::from_secs(30), submission_arrived.notified())
            .await
            .expect("submission should arrive at the gated endpoint");

        // Disconnect while the submitter is parked: this drains the pending
        // entry and latches cancel-before-id for the parked submitter.
        manager
            .handle_disconnect()
            .await
            .expect("disconnect should succeed");

        // Release the submission; the submitter observes the latch and
        // resolves the prompt with `stopReason=cancelled`.
        submission_release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(30), prompt_handle)
            .await
            .expect("prompt should resolve within timeout")
            .expect("prompt task")
            .expect("disconnect-before-id must resolve cancelled, not error");
        assert_eq!(result["stopReason"], json!("cancelled"));
    }

    /// The disconnect-vs-submission-error linearization regression. A prompt
    /// whose submission is parked inside `create_agent_request` (and will
    /// fail when released) is drained by a disconnect that parks at the
    /// disconnect seam *after* publishing closed+drained state but *before*
    /// latching the drained entry's cancel-before-id. Releasing the
    /// submission into failure in that window must resolve the prompt with
    /// `stopReason=cancelled` — never the GraphQL error — because the
    /// submission-error classification linearizes with the closed latch.
    /// Releasing the disconnect afterwards verifies the empty-entry/closed
    /// terminal state. Fully deterministic: no sleeps.
    #[tokio::test]
    async fn disconnect_parked_before_latch_with_finishing_submission_failure_resolves_cancelled() {
        let (_tempdir, node, agent_did) = test_node().await;
        let submission_arrived = Arc::new(tokio::sync::Notify::new());
        let submission_release = Arc::new(tokio::sync::Notify::new());
        let gate_armed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let submission_fail = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let graphql = spawn_gated_mock_graphql_with_failure(
            node.clone(),
            Some((
                submission_arrived.clone(),
                submission_release.clone(),
                gate_armed,
            )),
            Some(submission_fail),
        )
        .await;
        let manager = Arc::new(TurnManager::new(
            node.clone(),
            test_config(graphql, &agent_did),
        ));
        let engine = test_engine(node.clone());
        let (_buffer, sender) = buffer_sender();

        let prompt = parse_prompt_request(
            &json!({
                "sessionId": "session-1",
                "prompt": [text_block("hello")],
                "_meta": {"promptId": "prompt-1"},
            }),
            Some(json!(1)),
        )
        .unwrap();

        // Run the prompt; it parks inside the gated submission with its
        // pending entry inserted and no request id registered yet.
        let prompt_handle = tokio::spawn({
            let manager = manager.clone();
            let sender = sender.clone();
            let engine = engine.clone();
            async move { manager.handle_prompt(prompt, &sender, &engine).await }
        });

        // Deterministically wait for the submission mutation to arrive: the
        // submitter is now inside create_agent_request, strictly before the
        // request id is registered.
        tokio::time::timeout(Duration::from_secs(30), submission_arrived.notified())
            .await
            .expect("submission should arrive at the gated endpoint");

        // Arm the disconnect seam and run the disconnect: it atomically
        // latches closed and drains the entry, then parks before it would
        // latch the drained entry's cancel-before-id.
        let gate = TestGate {
            arrived: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
        };
        *manager.disconnect_gate.lock().await = Some(gate.clone());
        let manager_for_disconnect = manager.clone();
        let disconnect_handle = tokio::spawn(async move {
            manager_for_disconnect
                .handle_disconnect()
                .await
                .expect("disconnect should succeed")
        });

        tokio::time::timeout(Duration::from_secs(30), gate.arrived.notified())
            .await
            .expect("disconnect should park at the seam");

        // The closed+drained state is published, but the drained entry's
        // cancel-before-id latch is NOT: this is the exact race window. A
        // plain observer would see the latch as false.
        {
            let state = manager.state.lock().await;
            assert!(state.closed, "the disconnect has published closed");
            assert!(
                state.entries.is_empty(),
                "the disconnect has drained the entry table"
            );
        }

        // Release the parked submission *while the disconnect is parked at
        // the seam*: the endpoint answers with a GraphQL error, and the
        // failing submission must still resolve cancelled.
        submission_release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(30), prompt_handle)
            .await
            .expect("prompt should resolve within timeout")
            .expect("prompt task")
            .expect("submission failure in the closed-drained window must resolve cancelled");
        assert_eq!(
            result["stopReason"],
            json!("cancelled"),
            "the disconnect's required stopReason wins over the GraphQL error"
        );

        // Release the disconnect: it latches the (already-consumed) drained
        // entry and finishes as a no-op interrupt pass.
        gate.release.notify_one();
        tokio::time::timeout(Duration::from_secs(30), disconnect_handle)
            .await
            .expect("disconnect should resolve within timeout")
            .expect("disconnect task");

        // Terminal state: empty entries, closed connection.
        {
            let state = manager.state.lock().await;
            assert!(state.entries.is_empty());
            assert!(state.closed);
        }

        // Zero durable requests: the failed submission never minted an
        // AgentRequest, and the turn leaked nothing.
        let query = r#"{ AgentRequest { request_id } }"#;
        let response = node.execute(query).await;
        ensure_no_errors(&response, "test request query").expect("query");
        let rows = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        assert!(
            rows.is_empty(),
            "a submission failed into a closed connection must not mint an AgentRequest, got {rows:?}"
        );
    }

    /// The explicit-cancel-vs-submission-error linearization regression. A
    /// prompt whose submission is parked inside `create_agent_request` (and
    /// will fail when released) is drained by an explicit `session/cancel`
    /// that parks at the cancel-drain seam *after* removing the pending
    /// entry from the connection state but *before* latching the removed
    /// entry's cancel-before-id. Releasing the submission into failure in
    /// that window must resolve the prompt with `stopReason=cancelled` —
    /// never the GraphQL error — because the submission-error classification
    /// treats a missing entry as cancellation (only cancel/disconnect can
    /// remove it while `submit_request` is awaited). Releasing the cancel
    /// afterwards verifies the empty pending table and zero durable
    /// `AgentRequest` rows. Fully deterministic: no sleeps.
    #[tokio::test]
    async fn cancel_parked_before_latch_with_finishing_submission_failure_resolves_cancelled() {
        let (_tempdir, node, agent_did) = test_node().await;
        let submission_arrived = Arc::new(tokio::sync::Notify::new());
        let submission_release = Arc::new(tokio::sync::Notify::new());
        let gate_armed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let submission_fail = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let graphql = spawn_gated_mock_graphql_with_failure(
            node.clone(),
            Some((
                submission_arrived.clone(),
                submission_release.clone(),
                gate_armed,
            )),
            Some(submission_fail),
        )
        .await;
        let manager = Arc::new(TurnManager::new(
            node.clone(),
            test_config(graphql, &agent_did),
        ));
        let engine = test_engine(node.clone());
        let (_buffer, sender) = buffer_sender();

        let prompt = parse_prompt_request(
            &json!({
                "sessionId": "session-1",
                "prompt": [text_block("hello")],
                "_meta": {"promptId": "prompt-1"},
            }),
            Some(json!(1)),
        )
        .unwrap();

        // Run the prompt; it parks inside the gated submission with its
        // pending entry inserted and no request id registered yet.
        let prompt_handle = tokio::spawn({
            let manager = manager.clone();
            let sender = sender.clone();
            let engine = engine.clone();
            async move { manager.handle_prompt(prompt, &sender, &engine).await }
        });

        // Deterministically wait for the submission mutation to arrive: the
        // submitter is now inside create_agent_request, strictly before the
        // request id is registered.
        tokio::time::timeout(Duration::from_secs(30), submission_arrived.notified())
            .await
            .expect("submission should arrive at the gated endpoint");

        // Arm the cancel-drain seam and run the explicit cancel: it removes
        // the pending entry under the connection lock, then parks before it
        // would latch the removed entry's cancel-before-id.
        let gate = TestGate {
            arrived: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
        };
        *manager.cancel_drain_gate.lock().await = Some(gate.clone());
        let cancel = parse_cancel_notification(&json!({
            "sessionId": "session-1",
            "_meta": {"cancelSubagents": true, "promptId": "prompt-1"},
        }))
        .unwrap();
        let manager_for_cancel = manager.clone();
        let cancel_handle = tokio::spawn(async move {
            manager_for_cancel
                .handle_cancel(cancel)
                .await
                .expect("cancel should succeed")
        });

        tokio::time::timeout(Duration::from_secs(30), gate.arrived.notified())
            .await
            .expect("cancel should park at the seam");

        // The entry is removed, the connection is still open, and the drained
        // entry's cancel-before-id latch is NOT published: this is the exact
        // race window. A plain three-way observer would see every
        // cancellation marker as false.
        {
            let state = manager.state.lock().await;
            assert!(
                !state.closed,
                "an explicit cancel leaves the connection open"
            );
            assert!(
                state.entries.is_empty(),
                "the cancel has removed the pending entry"
            );
        }

        // Release the parked submission *while the cancel is parked at the
        // seam*: the endpoint answers with a GraphQL error, and the failing
        // submission must still resolve cancelled.
        submission_release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(30), prompt_handle)
            .await
            .expect("prompt should resolve within timeout")
            .expect("prompt task")
            .expect("submission failure in the cancel-drain window must resolve cancelled");
        assert_eq!(
            result["stopReason"],
            json!("cancelled"),
            "the explicit cancel's required stopReason wins over the GraphQL error"
        );

        // Release the cancel: it latches the (already-consumed) removed
        // entry's cancel-before-id and finishes with no request id to
        // interrupt.
        gate.release.notify_one();
        tokio::time::timeout(Duration::from_secs(30), cancel_handle)
            .await
            .expect("cancel should resolve within timeout")
            .expect("cancel task");

        // Terminal state: no pending entry, connection still open.
        {
            let state = manager.state.lock().await;
            assert!(state.entries.is_empty());
            assert!(!state.closed);
        }

        // Zero durable requests: the failed submission never minted an
        // AgentRequest, and the cancelled turn leaked nothing.
        let query = r#"{ AgentRequest { request_id } }"#;
        let response = node.execute(query).await;
        ensure_no_errors(&response, "test request query").expect("query");
        let rows = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        assert!(
            rows.is_empty(),
            "a submission failed into a cancelled turn must not mint an AgentRequest, got {rows:?}"
        );
    }

    /// The deterministic post-disconnect insertion regression. A prompt task
    /// is gated immediately before the insertion critical section — the exact
    /// race window where the production bug leaked a durable request — then
    /// the connection disconnects while the prompt is gated. Releasing the
    /// gate must reject the prompt before any durable submission: it errors,
    /// no pending entry exists, and zero `AgentRequest` documents were
    /// created. A duplicate disconnect stays idempotent.
    #[tokio::test]
    async fn prompt_gated_before_insertion_is_rejected_after_disconnect() {
        let (_tempdir, node, agent_did) = test_node().await;
        let graphql = spawn_mock_graphql(node.clone()).await;
        let manager = Arc::new(TurnManager::new(
            node.clone(),
            test_config(graphql, &agent_did),
        ));
        let engine = test_engine(node.clone());
        let (_buffer, sender) = buffer_sender();

        let prompt = parse_prompt_request(
            &json!({
                "sessionId": "session-1",
                "prompt": [text_block("hello")],
                "_meta": {"promptId": "prompt-1"},
            }),
            Some(json!(1)),
        )
        .unwrap();

        // Arm the insertion gate: the spawned prompt parks immediately before
        // it touches the connection state, so the disconnect below can run
        // while the prompt is still queued.
        let gate = TestGate {
            arrived: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
        };
        *manager.insertion_gate.lock().await = Some(gate.clone());

        let manager_for_prompt = manager.clone();
        let sender_for_prompt = sender.clone();
        let engine_for_prompt = engine.clone();
        let prompt_handle = tokio::spawn(async move {
            manager_for_prompt
                .handle_prompt(prompt, &sender_for_prompt, &engine_for_prompt)
                .await
        });

        // Deterministically wait for the prompt to park at the gate.
        tokio::time::timeout(Duration::from_secs(30), gate.arrived.notified())
            .await
            .expect("prompt should arrive at the insertion gate");

        // Disconnect while the prompt is gated: the closed latch is set and
        // the (empty) entry table is drained under one lock.
        manager
            .handle_disconnect()
            .await
            .expect("disconnect should succeed");

        // Release the gate; the prompt must observe the closed latch in the
        // same critical section as insertion and reject.
        gate.release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(30), prompt_handle)
            .await
            .expect("prompt should resolve within timeout")
            .expect("prompt task");
        let error = result.expect_err("a prompt gated across disconnect must be rejected");
        assert!(
            error.to_string().contains("already disconnected"),
            "rejection must name the closed connection, got: {error:#}"
        );

        // No pending entry was inserted.
        {
            let state = manager.state.lock().await;
            assert!(
                state.entries.is_empty(),
                "no entry may be inserted after the closed latch"
            );
            assert!(state.closed, "the connection stays closed");
        }

        // Zero durable requests: the rejection happened strictly before
        // `create_agent_request`.
        let query = r#"{ AgentRequest { request_id } }"#;
        let response = node.execute(query).await;
        ensure_no_errors(&response, "test request query").expect("query");
        let rows = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        assert!(
            rows.is_empty(),
            "a prompt rejected by the closed latch must never create an AgentRequest, got {rows:?}"
        );

        // Duplicate disconnect stays idempotent: it succeeds and changes
        // nothing.
        manager
            .handle_disconnect()
            .await
            .expect("duplicate disconnect should succeed");
        {
            let state = manager.state.lock().await;
            assert!(state.entries.is_empty());
            assert!(state.closed);
        }
    }

    /// The ordinary already-pending drain: a prompt whose entry is already
    /// inserted (parked inside `create_agent_request` via the gated mock
    /// endpoint) is drained atomically by disconnect, the entry table is
    /// empty immediately, and a second duplicate disconnect is a no-op.
    #[tokio::test]
    async fn disconnect_drains_an_already_pending_entry_and_duplicate_disconnect_is_a_noop() {
        let (_tempdir, node, agent_did) = test_node().await;
        let submission_arrived = Arc::new(tokio::sync::Notify::new());
        let submission_release = Arc::new(tokio::sync::Notify::new());
        let gate_armed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let graphql = spawn_gated_mock_graphql(
            node.clone(),
            Some((
                submission_arrived.clone(),
                submission_release.clone(),
                gate_armed,
            )),
        )
        .await;
        let manager = Arc::new(TurnManager::new(
            node.clone(),
            test_config(graphql, &agent_did),
        ));
        let engine = test_engine(node.clone());
        let (_buffer, sender) = buffer_sender();

        let prompt = parse_prompt_request(
            &json!({
                "sessionId": "session-1",
                "prompt": [text_block("hello")],
                "_meta": {"promptId": "prompt-1"},
            }),
            Some(json!(1)),
        )
        .unwrap();

        let prompt_handle = tokio::spawn({
            let manager = manager.clone();
            let sender = sender.clone();
            let engine = engine.clone();
            async move { manager.handle_prompt(prompt, &sender, &engine).await }
        });

        tokio::time::timeout(Duration::from_secs(30), submission_arrived.notified())
            .await
            .expect("submission should arrive at the gated endpoint");

        // The entry is live and pending; disconnect drains it under the same
        // lock that latches closed.
        {
            let state = manager.state.lock().await;
            assert_eq!(state.entries.len(), 1, "the prompt entry must be pending");
        }
        manager
            .handle_disconnect()
            .await
            .expect("disconnect should succeed");
        {
            let state = manager.state.lock().await;
            assert!(
                state.entries.is_empty(),
                "disconnect must drain every entry"
            );
            assert!(state.closed);
        }

        // Duplicate disconnect: idempotent no-op.
        manager
            .handle_disconnect()
            .await
            .expect("duplicate disconnect should succeed");
        {
            let state = manager.state.lock().await;
            assert!(state.entries.is_empty());
            assert!(state.closed);
        }

        // Release the parked submission: the submitter observes the
        // cancel-before-id latch and resolves cancelled.
        submission_release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(30), prompt_handle)
            .await
            .expect("prompt should resolve within timeout")
            .expect("prompt task")
            .expect("the drained prompt must resolve cancelled, not error");
        assert_eq!(result["stopReason"], json!("cancelled"));
    }

    /// Send failure after submission: the user echo fails against a closed
    /// outbound channel, so the submitted request must be interrupted and the
    /// prompt surface the send failure.
    #[tokio::test]
    async fn send_failure_after_submission_interrupts_the_request() {
        let (_tempdir, node, agent_did) = test_node().await;
        let graphql = spawn_mock_graphql(node.clone()).await;
        let manager = TurnManager::new(node.clone(), test_config(graphql, &agent_did));
        let engine = test_engine(node.clone());
        let (frames_tx, frames_rx) = tokio::sync::mpsc::unbounded_channel::<
            crate::commands::grok_shim::protocol::ServerEnvelope,
        >();
        // Closing the receiver makes every outbound send fail: the user echo
        // fails immediately after submission.
        drop(frames_rx);
        let sender = PromptSender::Live {
            outbound: AcpOutbound::for_frames(frames_tx),
        };

        let prompt = parse_prompt_request(
            &json!({
                "sessionId": "session-1",
                "prompt": [text_block("hello")],
                "_meta": {"promptId": "prompt-1"},
            }),
            Some(json!(1)),
        )
        .unwrap();

        let result = tokio::time::timeout(
            Duration::from_secs(30),
            manager.handle_prompt(prompt, &sender, &engine),
        )
        .await
        .expect("prompt should resolve within timeout");
        assert!(
            result.is_err(),
            "closed outbound must surface a send failure"
        );

        // The submitted request must have been interrupted: its durable row
        // carries a non-empty interrupt_requested_at.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let query = r#"{ AgentRequest { request_id interrupt_requested_at } }"#;
        let response = node.execute(query).await;
        ensure_no_errors(&response, "test request query").expect("query");
        let rows = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        assert!(
            rows.iter().any(|row| row
                .get("interrupt_requested_at")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty())),
            "the submitted request must be interrupted after the send failure"
        );
    }

    /// A second prompt for the same session while one is live is rejected and
    /// does not disturb the live turn (one pending prompt per session).
    #[tokio::test]
    async fn second_prompt_for_live_session_is_rejected() {
        let (_tempdir, node, agent_did) = test_node().await;
        let graphql = spawn_mock_graphql(node.clone()).await;
        let manager = Arc::new(TurnManager::new(
            node.clone(),
            test_config(graphql, &agent_did),
        ));
        let projections = test_engine(node.clone());
        let (buffer, sender) = buffer_sender();

        let first = parse_prompt_request(
            &json!({
                "sessionId": "session-1",
                "prompt": [text_block("first")],
                "_meta": {"promptId": "prompt-1"},
            }),
            Some(json!(1)),
        )
        .unwrap();
        // Run the first prompt to its terminal watch; nothing terminalizes it
        // yet, so it stays pending for the whole rejection check below.
        let manager_for_first = manager.clone();
        let sender_for_first = sender.clone();
        let engine_for_first = projections.clone();
        let first_handle = tokio::spawn(async move {
            manager_for_first
                .handle_prompt(first, &sender_for_first, &engine_for_first)
                .await
        });

        // The first prompt's user echo is sent only after its request id was
        // registered on the pending entry, so a non-empty buffer proves the
        // entry is live.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if buffer.lock().await.len() >= 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "first prompt never echoed; pending entry never went live"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // A second prompt for the same session must be rejected while the
        // first is still pending.
        let second = parse_prompt_request(
            &json!({
                "sessionId": "session-1",
                "prompt": [text_block("second")],
                "_meta": {"promptId": "prompt-2"},
            }),
            Some(json!(2)),
        )
        .unwrap();
        let rejection = manager
            .handle_prompt(second, &sender, &projections)
            .await
            .expect_err("second prompt for a live session must be rejected");
        assert!(
            rejection.to_string().contains("live prompt"),
            "rejection must name the one-pending-per-session rule, got: {rejection}"
        );

        // The rejection must not have disturbed the live turn: terminalize the
        // first prompt's request and confirm it resolves normally.
        let node_for_terminalize = node.clone();
        let terminalize_handle = tokio::spawn(async move {
            loop {
                let query = r#"{ AgentRequest(filter: { lifecycle_state: { _eq: "pending" } }) { request_id } }"#;
                let response = node_for_terminalize.execute(query).await;
                let rows = response
                    .data
                    .as_ref()
                    .and_then(|data| data.get("AgentRequest"))
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                if let Some(row) = rows.first() {
                    let request_id = row.get("request_id").and_then(Value::as_str).unwrap();
                    terminalize_request(&node_for_terminalize, request_id, "completed").await;
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
        let first_result = tokio::time::timeout(Duration::from_secs(30), first_handle)
            .await
            .expect("first prompt should resolve within timeout")
            .expect("first prompt task")
            .expect("first prompt should succeed");
        assert_eq!(first_result["stopReason"], json!("end_turn"));
        terminalize_handle.await.expect("terminalize task");
    }
}
