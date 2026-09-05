//! Grok shim projection engine root.
//!
//! The projection engine owns the connection-local side of the Grok shim: it
//! turns durable Gents rows (`AgentResponse`, `AgentMessage`,
//! `AgentToolCall`/`AgentToolResult`, and runtime child `AgentRequest` rows)
//! into fresh Grok pager `session/update` notification payloads and stamps the
//! per-connection event metadata (`_meta.eventId`, `_meta.promptId`,
//! `_meta.totalTokens`) those payloads require.
//!
//! The engine is deliberately bounded and request-id-scoped:
//! - every projection helper takes an explicit request id and queries only the
//!   rows that request can own (one query per row family, no graph walks
//!   beyond the direct children of the projected request);
//! - projection is read-only: it never replays the session, never duplicates
//!   durable materialization, and never writes a document;
//! - every interpolated GraphQL value passes through
//!   [`gents::graphql::escape_graphql_string`], and every query executes
//!   in-process through [`EmbeddedNode::execute`].
//!
//! The three leaves own the payload shapes:
//! - [`messages`]: agent/user thought and message chunks plus streaming token
//!   and context metadata;
//! - [`tools`]: tool-call lifecycle, command titles/status/content,
//!   available-command updates, and the pager-style terminal `not supported`
//!   stubs;
//! - [`subagents`]: subagent spawned/progress/finished updates from runtime
//!   child `AgentRequest` rows and the shaped not-found ext stubs.
//!
//! Static `Task` configuration rows are never treated as runtime state and no
//! permission or terminal documents are ever fabricated here.

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use defra_node::EmbeddedNode;
use gents::{load_agent_behavior, load_inference_profile};
use serde_json::{json, Map, Value};

mod child_output;
mod context;
pub(crate) mod messages;
pub(crate) mod subagents;
pub(crate) mod tools;

/// Wire name of the ACP `session/update` notification every projection
/// payload is wrapped in.
pub(crate) const SESSION_UPDATE_METHOD: &str = "session/update";
/// Live xAI extension rail used by Grok for subagent lifecycle events.
pub(crate) const SUBAGENT_NOTIFICATION_METHOD: &str = "x.ai/session_notification";

/// Default context window reported when the bound configuration does not
/// supply one. Mirrors the model catalog's `totalContextTokens` default scale
/// (`gents::DEFAULT_CONTEXT_WINDOW`) so a bound behavior that never pinned a
/// window still reports a truthful, bounded value instead of zero.
pub(crate) const DEFAULT_CONTEXT_WINDOW_TOKENS: u64 = gents::DEFAULT_CONTEXT_WINDOW as u64;

/// Normalize a configured context window for every Grok-facing consumer.
/// Older profiles use zero for "unspecified", but the wire catalog and token
/// projection both require the same positive effective value.
pub(crate) fn effective_context_window_tokens(configured: u64) -> u64 {
    if configured == 0 {
        DEFAULT_CONTEXT_WINDOW_TOKENS
    } else {
        configured
    }
}

/// Return a trimmed non-empty string. Projection leaves share this helper so
/// optional identity fields cannot drift subtly by row family.
pub(super) fn nonempty(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}

/// Bound model/context configuration the shim was assembled with.
///
/// Model and context-window values come from the bound `AgentBehavior` and its
/// `InferenceProfile`, not from `AgentSession` (which has no model or
/// context-window fields).
#[derive(Debug, Clone)]
pub(crate) struct BoundModelContext {
    /// Grok `modelId` the pager addresses: the bound behavior's `model_name`
    /// exactly. The backend id stays internal and is never projected.
    pub(crate) model_id: String,
    /// Human display name; falls back to the raw model id when the catalog
    /// has no friendlier entry.
    pub(crate) model_name: String,
    /// `totalContextTokens` reported in the session/new model catalog and
    /// used to bound `_meta.totalTokens`.
    pub(crate) total_context_tokens: u64,
}

impl BoundModelContext {
    pub(crate) fn new(model_id: String, model_name: String, total_context_tokens: u64) -> Self {
        Self {
            model_id,
            model_name,
            total_context_tokens,
        }
    }

    /// Fall back to the catalog default when the bound profile did not pin a
    /// context window.
    pub(crate) fn effective_context_window(&self) -> u64 {
        effective_context_window_tokens(self.total_context_tokens)
    }
}

/// Connection-scoped, session-keyed projection sequencing.
///
/// One sequencer serves one registered pager connection and keys every
/// counter by session id, so two sessions on the same connection never share
/// an event counter or a token total:
/// - event ids are monotonic *per session*, formatted
///   `"{session_id}-{counter}"` and starting at 1, matching the pager's
///   `NotificationMeta` dedup contract (the pager deduplicates non-replay
///   counters by `eventId`, so a repeated id would silently drop a live
///   update);
/// - `totalTokens` is current context occupancy, ordered by persisted
///   inference dispatch; newer context may decrease after compaction.
///
/// Event ids are *reserved*, not simply allocated: a reservation commits only
/// after the notification carrying it was successfully sent, and an
/// uncommitted reservation rolls back on drop, so a failed send never
/// consumes an id. Splitting the counters out keeps the arithmetic and the
/// rollback unit-testable without an embedded node.
#[derive(Debug, Default)]
pub(crate) struct ProjectionSequencer {
    sessions: std::sync::Mutex<BTreeMap<String, SessionSequence>>,
}

/// Per-session counters: the committed event-id high-water mark and the
/// most recent observed context occupancy (not cumulative token spend).
#[derive(Debug, Default)]
struct SessionSequence {
    event_counter: u64,
    total_tokens: u64,
    context_order: Option<context::ContextOrder>,
}

/// One reserved event id.
///
/// Reserving increments the session's counter immediately (the id must be
/// stamped into the payload before it is sent), but the reservation only
/// becomes permanent on [`EventIdReservation::commit`]. Dropping an
/// uncommitted reservation rolls the counter back — and only while the
/// reservation is still the session's most recent id, so a later committed
/// id can never be un-allocated.
pub(crate) struct EventIdReservation {
    sequencer: Arc<ProjectionSequencer>,
    session_id: String,
    value: u64,
    committed: bool,
}

impl EventIdReservation {
    /// The reserved wire event id: `"{sessionId}-{counter}"`.
    pub(crate) fn event_id(&self) -> String {
        format!("{}-{}", self.session_id, self.value)
    }

    /// Keep the reserved id permanently. Called only after the notification
    /// carrying it was successfully sent.
    pub(crate) fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for EventIdReservation {
    fn drop(&mut self) {
        if !self.committed {
            self.sequencer
                .rollback_event_id(&self.session_id, self.value);
        }
    }
}

impl ProjectionSequencer {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Reserve the next monotonic event id for `session_id`.
    ///
    /// The counter is per session and starts at 1; the reservation must be
    /// committed after the send succeeds (otherwise it rolls back on drop),
    /// so a failed send does not consume an id.
    pub(crate) fn reserve_event_id(sequencer: &Arc<Self>, session_id: &str) -> EventIdReservation {
        let value = {
            let mut sessions = sequencer
                .sessions
                .lock()
                .expect("grok shim sequencer lock poisoned");
            let sequence = sessions.entry(session_id.to_string()).or_default();
            sequence.event_counter += 1;
            sequence.event_counter
        };
        EventIdReservation {
            sequencer: sequencer.clone(),
            session_id: session_id.to_string(),
            value,
            committed: false,
        }
    }

    /// Roll back one uncommitted reservation. Only the session's most recent
    /// id can roll back; if a later id was already committed, the failed
    /// reservation leaves a gap instead (gaps are harmless to the pager's
    /// monotonic dedup; duplicates would not be).
    fn rollback_event_id(&self, session_id: &str, value: u64) {
        let mut sessions = self
            .sessions
            .lock()
            .expect("grok shim sequencer lock poisoned");
        if let Some(sequence) = sessions.get_mut(session_id) {
            if sequence.event_counter == value {
                sequence.event_counter = sequence.event_counter.saturating_sub(1);
            }
        }
    }

    /// The number of committed event ids for `session_id`. Test observation
    /// accessor: production send paths always commit inside the common
    /// session-update path.
    #[cfg(test)]
    pub(crate) fn event_counter(&self, session_id: &str) -> u64 {
        self.sessions
            .lock()
            .expect("grok shim sequencer lock poisoned")
            .get(session_id)
            .map(|sequence| sequence.event_counter)
            .unwrap_or(0)
    }

    /// Last known current-context occupancy for `session_id`.
    pub(crate) fn session_total_tokens(&self, session_id: &str) -> u64 {
        self.sessions
            .lock()
            .expect("grok shim sequencer lock poisoned")
            .get(session_id)
            .map(|sequence| sequence.total_tokens)
            .unwrap_or(0)
    }

    /// Replace context only from the newest persisted inference generation.
    /// New calls may lower occupancy after compaction; old background polls
    /// cannot overwrite them. Repeated observations never add token spend.
    fn observe_context(&self, session_id: &str, sample: context::ContextSample) {
        let mut sessions = self
            .sessions
            .lock()
            .expect("grok shim sequencer lock poisoned");
        let sequence = sessions.entry(session_id.to_owned()).or_default();
        match sequence
            .context_order
            .as_ref()
            .map(|order| sample.order.cmp(order))
        {
            Some(std::cmp::Ordering::Less) => return,
            Some(std::cmp::Ordering::Equal) => {
                sequence.total_tokens = sequence.total_tokens.max(sample.used)
            }
            _ => {
                sequence.total_tokens = sample.used;
                sequence.context_order = Some(sample.order);
            }
        }
    }
}

/// Build the `_meta` object stamped on one session/update notification.
///
/// Fields follow the pager's `NotificationMeta`: `eventId` is
/// `"{sessionId}-{counter}"`, `totalTokens` is current context occupancy,
/// and `promptId` correlates the update with its turn. `is_replay` is
/// `None` for fresh updates (the key is omitted entirely) and `Some(false)`
/// for the user echo, which carries the key explicitly.
pub(crate) fn stamp_update_meta(
    event_id: &str,
    total_tokens: u64,
    prompt_id: Option<&str>,
    is_replay: Option<bool>,
    timestamps: UpdateTimestamps,
) -> Value {
    let mut meta = Map::new();
    meta.insert("eventId".to_string(), Value::String(event_id.to_string()));
    meta.insert("totalTokens".to_string(), Value::from(total_tokens));
    if let Some(prompt_id) = prompt_id {
        meta.insert("promptId".to_string(), Value::String(prompt_id.to_string()));
    }
    if let Some(is_replay) = is_replay {
        meta.insert("isReplay".to_string(), Value::Bool(is_replay));
    }
    if let Some(value) = timestamps.agent_timestamp_ms {
        meta.insert("agentTimestampMs".to_string(), Value::from(value));
    }
    if let Some(value) = timestamps.stream_start_ms {
        meta.insert("streamStartMs".to_string(), Value::from(value));
    }
    if let Some(value) = timestamps.turn_start_ms {
        meta.insert("turnStartMs".to_string(), Value::from(value));
    }
    Value::Object(meta)
}

/// Server-side timestamps understood by the Grok pager. `streamStartMs` is
/// also the pager's model-generation boundary key, so it must stay stable
/// within one generation and change across tool-loop generations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct UpdateTimestamps {
    pub(crate) agent_timestamp_ms: Option<i64>,
    pub(crate) stream_start_ms: Option<i64>,
    pub(crate) turn_start_ms: Option<i64>,
}

/// Wrap one projected update payload in a `session/update` notification
/// envelope.
///
/// The Grok decoder expects the chunk field name `content` (not
/// `contentBlock`); the leaves own that shape and this wrapper only adds the
/// session envelope and the stamped `_meta`.
#[cfg(test)]
pub(crate) fn session_update_notification(session_id: &str, update: Value, meta: Value) -> Value {
    session_notification_for_method(SESSION_UPDATE_METHOD, session_id, update, meta)
}

/// Wrap a projected payload on its protocol rail. Standard ACP updates use
/// `session/update`; live subagent lifecycle updates use Grok's
/// `_x.ai/session_notification` wire extension rail (the similarly named
/// `x.ai/session/update` is a replay alias, not the live method).
pub(crate) fn session_notification_for_method(
    method: &str,
    session_id: &str,
    update: Value,
    meta: Value,
) -> Value {
    let mut params = Map::new();
    params.insert(
        "sessionId".to_string(),
        Value::String(session_id.to_string()),
    );
    params.insert("update".to_string(), update);
    params.insert("_meta".to_string(), meta);
    json!({
        "jsonrpc": "2.0",
        "method": super::acp::wire_method(method),
        "params": Value::Object(params),
    })
}

// ---------------------------------------------------------------------------
// Common session-update send path
// ---------------------------------------------------------------------------

/// The connection-scoped common send path for `session/update` notifications.
///
/// One channel serves one registered pager connection and keys its send locks
/// by session id, so two sessions never serialize each other while all sends
/// for one session do. Every `session/update` family the shim emits — the
/// `session/set_mode` `current_mode_update`, the synthetic prompt
/// `user_message_chunk` echo, and the durable projected tool/subagent/message
/// updates — must go through [`SessionUpdateChannel::send`], which is what
/// makes allocation order equal successful enqueue order for every event id
/// on a session.
///
/// The allocation/enqueue invariant: the per-session send lock is held from
/// before the event id is reserved until after the notification was
/// successfully enqueued through the sender and the reservation committed.
/// The pager deduplicates non-replay counters monotonically by `eventId`, so
/// a `session-2` arriving before `session-1` would silently drop the real
/// `session-1` update as stale — uniqueness alone is not enough. A failed
/// send rolls the reservation back (the id is not consumed) and never
/// advances the caller's delivery cursor.
#[derive(Debug, Default)]
pub(crate) struct SessionUpdateChannel {
    /// The connection's projection sequencer: the shared per-session
    /// event-id and token-total counters.
    sequencer: Arc<ProjectionSequencer>,
    /// One async send lock per session id. The inner map is a short
    /// synchronous lock that only guards insertion; each session's lock is
    /// an async mutex held across the (possibly fallible) send await.
    send_locks: std::sync::Mutex<BTreeMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl SessionUpdateChannel {
    /// Build the channel over the connection's sequencer.
    pub(crate) fn new(sequencer: Arc<ProjectionSequencer>) -> Self {
        Self {
            sequencer,
            send_locks: std::sync::Mutex::new(BTreeMap::new()),
        }
    }

    /// The connection's projection sequencer.
    #[cfg(test)]
    pub(crate) fn sequencer(&self) -> &ProjectionSequencer {
        &self.sequencer
    }

    /// The per-session async send lock. Different sessions get independent
    /// locks and stay fully concurrent. The caller acquires the guard itself
    /// so the lock is held across the whole reserve → send → commit span.
    fn session_lock(&self, session_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut send_locks = self
            .send_locks
            .lock()
            .expect("grok shim send-lock map poisoned");
        send_locks
            .entry(session_id.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Acquire the session's send lock as an *owned* guard.
    ///
    /// `lock_owned` requires an owned `Arc` handle to the mutex and returns an
    /// [`OwnedMutexGuard`] that is a real named binding the caller holds —
    /// never a temporary guard dropped at the end of the acquiring statement.
    /// This is the exact shape the per-session ordering invariant needs: the
    /// guard stays alive from before the event id is reserved until after the
    /// notification was enqueued and the reservation committed. Returning the
    /// plain `Arc<Mutex<()>>` after `lock.lock().await;` would silently drop
    /// the temporary guard and let a racing same-session send interleave.
    async fn session_send_guard(&self, session_id: &str) -> tokio::sync::OwnedMutexGuard<()> {
        self.session_lock(session_id).lock_owned().await
    }

    /// Send one `session/update` notification through the common path.
    ///
    /// While holding the session's send lock: reads the current session
    /// token total, reserves the next event id, lets `build_notification`
    /// stamp the final notification value (the reserved event id and token
    /// total are handed in), enqueues the serialized line through
    /// `send_line`, and commits the reservation only after the send
    /// succeeded. A failed send returns the error; the uncommitted
    /// reservation rolls back on drop, so the id is not consumed and the
    /// next successful send on the session receives the immediately
    /// expected next id.
    ///
    /// Returns the serialized notification line that was delivered.
    pub(crate) async fn send(
        &self,
        session_id: &str,
        build_notification: impl FnOnce(&str, u64) -> Result<Value>,
        send_line: impl AsyncSendLine,
    ) -> Result<String> {
        self.send_with_commit(session_id, build_notification, send_line, NoCommit)
            .await
    }

    /// Send one `session/update` notification through the common path with a
    /// state-commit hook.
    ///
    /// Identical ordering and rollback semantics to [`SessionUpdateChannel::send`],
    /// plus one atomicity guarantee callers with side effects need: `commit`
    /// runs while the session's send lock is still held, immediately after
    /// the line was successfully enqueued and the event-id reservation was
    /// committed. A send failure skips `commit` entirely — so a caller like
    /// `session/set_mode` can record its mode change *inside* the hook and
    /// be certain the mode state mutates if and only if the corresponding
    /// notification was enqueued, with no window in which a concurrent
    /// same-session send can interleave between the enqueue and the state
    /// change. The hook is infallible by construction: it only ever records
    /// connection-local state, and committing the reservation before it runs
    /// is what guarantees an already-delivered event id is never reused.
    pub(crate) async fn send_with_commit(
        &self,
        session_id: &str,
        build_notification: impl FnOnce(&str, u64) -> Result<Value>,
        send_line: impl AsyncSendLine,
        commit: impl AsyncCommit,
    ) -> Result<String> {
        // Hold the session's async send lock across reserve → stamp →
        // enqueue → commit: allocation order equals enqueue order. The guard
        // is an owned guard bound here — it stays alive through the whole
        // reserve → send → commit span below (and, on the fallible paths,
        // is dropped only after the reservation rolled back).
        let _send_guard = self.session_send_guard(session_id).await;
        let reservation = ProjectionSequencer::reserve_event_id(&self.sequencer, session_id);
        let total_tokens = self.sequencer.session_total_tokens(session_id);
        let notification = build_notification(&reservation.event_id(), total_tokens)?;
        let line = serde_json::to_string(&notification)
            .context("serialize session/update notification")?;
        send_line.send_line(line.clone()).await?;
        // Commit the reservation immediately after the successful enqueue —
        // never after a further fallible operation — so the id that was
        // just delivered can never be handed to a later send. Only then run
        // the (infallible) local state hook, still inside the per-session
        // critical section so state and delivery stay coherent.
        reservation.commit();
        commit.commit().await;
        Ok(line)
    }
}

/// One infallible local state commit performed after a notification was
/// successfully enqueued, while the session's send lock is still held.
/// Implemented by callers whose session state must change exactly when the
/// corresponding notification was delivered (`session/set_mode`). The hook
/// only ever records connection-local state, so it cannot fail; the
/// reservation is committed before the hook runs, which is what guarantees
/// an already-delivered event id is never reused.
pub(crate) trait AsyncCommit: Send + Sync {
    async fn commit(&self);
}

impl<T: AsyncCommit + ?Sized> AsyncCommit for &T {
    async fn commit(&self) {
        (**self).commit().await
    }
}

/// The no-op commit used by plain [`SessionUpdateChannel::send`].
struct NoCommit;

impl AsyncCommit for NoCommit {
    async fn commit(&self) {}
}

/// One fallible enqueue of an already-serialized JSON-RPC line. Implemented
/// by the prompt sender (live outbound or test buffer); the exact commit
/// point is the successful send itself.
pub(crate) trait AsyncSendLine: Send + Sync {
    async fn send_line(&self, line: String) -> Result<()>;
}

impl<T: AsyncSendLine + ?Sized> AsyncSendLine for &T {
    async fn send_line(&self, line: String) -> Result<()> {
        (**self).send_line(line).await
    }
}

impl<T: AsyncSendLine + ?Sized> AsyncSendLine for Arc<T> {
    async fn send_line(&self, line: String) -> Result<()> {
        (**self).send_line(line).await
    }
}

/// Connection-scoped projection engine.
///
/// One engine instance serves one registered pager connection: it holds the
/// in-process node every projection query executes against, the bound
/// model/context configuration, and the connection's projection sequencer.
pub(crate) struct ProjectionEngine {
    pub(crate) background_executions: gents::hook::BackgroundExecutionRegistry,
    node: Arc<EmbeddedNode>,
    bound: BoundModelContext,
    sequencer: Arc<ProjectionSequencer>,
    /// The connection-scoped common send path every `session/update`
    /// notification must go through (per-session send lock + reserve/send/
    /// commit), so allocation order equals enqueue order per session.
    channel: SessionUpdateChannel,
}

impl ProjectionEngine {
    pub(crate) fn new(node: Arc<EmbeddedNode>, bound: BoundModelContext) -> Self {
        let sequencer = Arc::new(ProjectionSequencer::new());
        Self {
            node,
            bound,
            background_executions: Default::default(),
            channel: SessionUpdateChannel::new(sequencer.clone()),
            sequencer,
        }
    }

    /// The connection's common session-update send path. Every
    /// `session/update` family (set-mode updates, the prompt echo, and the
    /// durable projected updates) sends through this so per-session
    /// allocation order equals enqueue order.
    pub(crate) fn session_updates(&self) -> &SessionUpdateChannel {
        &self.channel
    }

    pub(crate) fn with_background_executions(
        mut self,
        executions: gents::hook::BackgroundExecutionRegistry,
    ) -> Self {
        self.background_executions = executions;
        self
    }

    /// The connection's projection sequencer as a shared handle, for tests
    /// that inspect per-session counters.
    #[cfg(test)]
    pub(crate) fn sequencer_arc(&self) -> Arc<ProjectionSequencer> {
        self.sequencer.clone()
    }

    /// Poll the durable request-scoped projections and return only the
    /// *novel* events this cursor has not emitted yet, merged across
    /// families into durable transcript chronology (see step 5 below).
    ///
    /// The poll itself is read-only: it observes every projection leaf, picks
    /// the events whose durable identity is new or changed relative to this
    /// cursor, and returns each together with the cursor advance that
    /// records it. **The cursor is not mutated here** — the caller records
    /// each advance only after the corresponding line was successfully sent,
    /// so a send failure never marks a novel event as delivered. Event ids
    /// are likewise *reserved* by the caller (see
    /// [`ProjectionSequencer::reserve_event_id`]) and committed only after a
    /// successful send.
    ///
    /// Ordering and identity rules:
    /// - live tails: the `AgentResponse` live `content`/`reasoning` tails
    ///   (the streaming snapshot of the *current* assistant segment) plan
    ///   deltas against a shadow copy of the live cursors, so several
    ///   candidates in one poll each see the preceding planned advances and
    ///   a failed send re-plans the identical candidate next poll. The rails
    ///   differ (see [`LiveSegmentCursor::plan`]): on the reasoning rail an
    ///   identical observation with an advanced `reasoning_progress_seq` is
    ///   a genuine later identical rewrite that streams in full, and a
    ///   non-prefix change continues the segment only when it is a proven
    ///   bounded-window rollover with an advanced seq; on the content rail
    ///   an identical observation with an advanced `progress_seq` is an
    ///   ordinary lifecycle boundary (nothing new — the counter is
    ///   boundary-scoped, not per-write), and any non-prefix change closes
    ///   the segment and re-emits the whole new snapshot. See
    ///   [`LiveSegmentCursor`] for the documented append-only / no-loss
    ///   policy on divergence.
    /// - tool calls: the first observation of a `tool_call` base emits the
    ///   full tracker registration; a later change to the tracked fields
    ///   (`title`/`kind`/`status`/`content`/`rawInput`/`rawOutput`/`meta`)
    ///   emits a `tool_call_update` carrying exactly the changed fields. The
    ///   terminal status has a dedicated status-only update whose delivery
    ///   is tracked separately from content refinements.
    ///   `available_commands_update` emits once per distinct visible tool
    ///   list.
    /// - subagents: one event per distinct payload per
    ///   `<sessionUpdate kind>:<subagentId>`; a still-running child's
    ///   `durationMs` is 0 (the elapsed computation needs a terminal bound),
    ///   so running progress payloads are stable across polls.
    /// - durable messages: each `AgentMessage`-derived chunk keeps a
    ///   delivered-length state keyed by `(message_key, update kind,
    ///   ordinal)`; an upserted/grown row re-projects and emits only the
    ///   newly proven suffix, never "seen forever" after its first
    ///   observation. The durable view reconciles against the live view: a
    ///   durable final row bound by `materialized_message_sequence` emits
    ///   only the bytes the live cursor has not already sent of the same
    ///   logical segment, and live bytes already covering a row suppress its
    ///   replay. The synthetic `user_message_chunk` echo of the current
    ///   prompt's user row is skipped — the turn already echoed the prompt
    ///   blocks directly.
    ///
    /// Context metadata comes from the newest physically owned inference
    /// accounting observation, not generated-token totals. Re-observation
    /// after a failed send is idempotent; older requests cannot replace it.
    pub(crate) async fn project_request_updates(
        &self,
        session_id: &str,
        request_id: &str,
        cursor: &mut RequestCursor,
    ) -> Result<ProjectionBatch> {
        // Each family projects independently (one bounded query set per
        // leaf), then the novel events merge into one chronology below.
        let mut merged: Vec<MergedEvent> = Vec::new();

        // 1. Messages leaf query (live tail + durable rows). The live tails
        //    plan against a shadow copy of the live cursors; the durable
        //    rows reconcile against that planned state below.
        let floor = earliest_live_anchor(&cursor.history_observation, &cursor.live_cursors);
        cursor.history_observation.retain_from(floor.as_deref());
        let message_sequence_high_water = cursor.message_sequence_high_water;
        let messages = messages::project_messages(
            &self.node,
            &mut cursor.history_observation,
            message_sequence_high_water,
            request_id,
            self.bound.effective_context_window(),
        )
        .await?;
        cursor.observe_timestamps(&messages);
        if let Some(sample) = context::load(&self.node, session_id, request_id).await? {
            self.sequencer.observe_context(session_id, sample);
        }

        let durable_rows = durable_row_views(&messages);
        let history_rows = messages
            .history
            .as_deref()
            .map(|history| infer_history_row_bindings(history, &durable_rows))
            .unwrap_or_default();

        // 2. Replay every validated response snapshot after each rail's
        // send-success anchor. Observation may already be at the newest
        // durable head; delivery remains an independent sequential prefix.
        let response_doc_changed = known_response_changed(
            messages.response_doc_id.as_deref(),
            cursor.delivered_response_doc.as_deref(),
        );
        let mut planned_live = if response_doc_changed {
            LiveCursorPair::default()
        } else {
            cursor.live_cursors.clone()
        };
        if let Some(history) = messages.history.as_deref() {
            let mut active_segment_key: Option<String> = None;
            let history_segment_keys: Vec<Option<String>> = history
                .iter()
                .map(|snapshot| {
                    if snapshot.content().is_empty() && snapshot.reasoning().is_empty() {
                        active_segment_key = None;
                    } else if active_segment_key.is_none() {
                        active_segment_key = Some(format!(
                            "{}:{}",
                            messages.response_doc_id.as_deref().unwrap_or(request_id),
                            snapshot.cid
                        ));
                    }
                    active_segment_key.clone()
                })
                .collect();
            let reasoning_start =
                replay_start(history, planned_live.reasoning.absorbed_commit.as_deref());
            let content_start =
                replay_start(history, planned_live.content.absorbed_commit.as_deref());
            for (snapshot_index, snapshot) in history.iter().enumerate() {
                let inferred_row = history_rows.get(&snapshot_index).cloned();
                for (kind, observed, progress_seq, is_reasoning) in [
                    (
                        messages::AGENT_THOUGHT_CHUNK,
                        snapshot.reasoning(),
                        snapshot.reasoning_progress_seq(),
                        true,
                    ),
                    (
                        messages::AGENT_MESSAGE_CHUNK,
                        snapshot.content(),
                        snapshot.progress_seq(),
                        false,
                    ),
                ] {
                    let live = if is_reasoning {
                        &mut planned_live.reasoning
                    } else {
                        &mut planned_live.content
                    };
                    let start = if is_reasoning {
                        reasoning_start
                    } else {
                        content_start
                    };
                    let Some(start) = start else {
                        // A same-document delivery anchor absent from the
                        // retained chain is unprovable. Leave the rail
                        // untouched; a later sound observation may recover.
                        continue;
                    };
                    if snapshot_index < start {
                        continue;
                    }
                    let before = live.clone();
                    let (delta, mut plan) = live
                        .plan(observed, progress_seq, is_reasoning, true)
                        .unwrap_or_else(|| (String::new(), live.anchor_plan(&snapshot.cid)));
                    plan.absorbed_commit = Some(snapshot.cid.clone());
                    if plan.segment_key.is_none() && !plan.sent_bytes.is_empty() {
                        plan.segment_key = history_segment_keys[snapshot_index].clone();
                    }

                    // A reset/divergence closes exactly the preceding
                    // delivered segment. Preserve it in order for later
                    // exact durable-row reconciliation.
                    if !before.sent_bytes.is_empty()
                        && plan.completed_sent_bytes == before.sent_bytes
                        && (plan.observed.is_empty()
                            || !plan.sent_bytes.starts_with(&before.sent_bytes))
                    {
                        plan.closed_evidence.push(ClosedEvidence {
                            sent_bytes: before.sent_bytes.clone(),
                            materialized_sequence: snapshot.materialized_message_sequence(),
                            bound_row: row_for_materialized_sequence(
                                snapshot.materialized_message_sequence(),
                                &durable_rows,
                            )
                            .or_else(|| inferred_row.clone()),
                            history_height: snapshot.height,
                            segment_key: before.segment_key.clone(),
                        });
                    }
                    // A final/interrupted materialization stamp can bind the
                    // still-open segment before the following reset commit.
                    if let Some(sequence) = snapshot.materialized_message_sequence() {
                        if !plan.sent_bytes.is_empty()
                            && !plan.closed_evidence.iter().any(|evidence| {
                                evidence.materialized_sequence == Some(sequence)
                                    && evidence.sent_bytes == plan.sent_bytes
                            })
                        {
                            plan.closed_evidence.push(ClosedEvidence {
                                sent_bytes: plan.sent_bytes.clone(),
                                materialized_sequence: Some(sequence),
                                bound_row: row_for_materialized_sequence(
                                    Some(sequence),
                                    &durable_rows,
                                ),
                                history_height: snapshot.height,
                                segment_key: plan.segment_key.clone(),
                            });
                        }
                    }
                    // An in-flight assistant row is persisted before tool
                    // execution, while the response tail is still open and
                    // carries no materialization stamp. A globally unique,
                    // order-preserving segment-to-row match is sufficient to
                    // bind that open evidence. The binding travels only in
                    // this plan/trailing advance, so it cannot suppress the
                    // durable row unless the live send succeeded.
                    if let Some(row) = inferred_row.clone() {
                        if !plan.sent_bytes.is_empty() {
                            upsert_bound_evidence(
                                &mut plan.closed_evidence,
                                plan.sent_bytes.clone(),
                                snapshot.materialized_message_sequence(),
                                row,
                                snapshot.height,
                                plan.segment_key.clone(),
                            );
                        }
                    }
                    live.commit(plan.clone(), progress_seq);
                    if delta.is_empty() {
                        continue;
                    }
                    let chronology = inferred_row
                        .as_ref()
                        .map(|row| row.sequence)
                        .or(snapshot.materialized_message_sequence())
                        // Only the current open tip may use the latest
                        // assistant-row position. Applying this fallback to
                        // older retained snapshots would reorder historical
                        // segments around intervening tools.
                        .or_else(|| {
                            (snapshot_index + 1 == history.len())
                                .then_some(messages.live_tail.assistant_sequence)
                                .flatten()
                        });
                    let timing = plan
                        .segment_key
                        .clone()
                        .map(|segment_key| cursor.timing_for_segment(segment_key, chronology));
                    let advance = if is_reasoning {
                        CursorAdvance::LiveReasoning { plan, progress_seq }
                    } else {
                        CursorAdvance::LiveContent { plan, progress_seq }
                    };
                    merged.push(MergedEvent {
                        event: NovelProjectionEvent {
                            method: SESSION_UPDATE_METHOD,
                            payload: messages::MessageUpdate::chunk_payload(kind, delta),
                            timing,
                            advance,
                        },
                        chronology,
                        family_rank: FAMILY_RANK_MESSAGE,
                        family_ordinal: merged
                            .iter()
                            .filter(|item| item.family_rank == FAMILY_RANK_MESSAGE)
                            .count(),
                    });
                }
            }
            // Cross-document race: the runtime persists an assistant
            // AgentMessage before it stamps/resets AgentResponse. If that row
            // appears after this rail already absorbed the unchanged history
            // tip, replay_start has no snapshots to revisit. Bind the proven
            // tip segment to its uniquely inferred row here so the durable
            // pass cannot replay bytes already delivered live.
            if let Some((tip_index, tip)) = history
                .len()
                .checked_sub(1)
                .map(|index| (index, &history[index]))
            {
                if let Some(row) = history_rows.get(&tip_index) {
                    bind_open_tip_evidence(
                        &mut planned_live.reasoning,
                        row,
                        EvidenceRail::Reasoning,
                        &durable_rows,
                        tip.height,
                    );
                    bind_open_tip_evidence(
                        &mut planned_live.content,
                        row,
                        EvidenceRail::Content,
                        &durable_rows,
                        tip.height,
                    );
                }
            }
        }
        // 4. Tools (lifecycle of the request's tool calls).
        let tools = tools::project_tools(
            &self.node,
            request_id,
            session_id,
            &self.background_executions,
        )
        .await?;
        for (index, update) in tools.updates.iter().enumerate() {
            let chronology = tools.chronology.get(index).copied().flatten();
            match update {
                tools::ToolUpdate::ToolCall(base) => {
                    let payload = base.to_payload();
                    let Some((emitted, advance)) =
                        cursor.tool_base_novel(&base.tool_call_id, &payload)
                    else {
                        continue;
                    };
                    merged.push(MergedEvent {
                        event: NovelProjectionEvent {
                            method: SESSION_UPDATE_METHOD,
                            payload: emitted,
                            timing: None,
                            advance,
                        },
                        chronology,
                        family_rank: FAMILY_RANK_TOOL,
                        family_ordinal: merged
                            .iter()
                            .filter(|item| item.family_rank == FAMILY_RANK_TOOL)
                            .count(),
                    });
                }
                tools::ToolUpdate::ToolCallUpdate(update) => {
                    let status = update
                        .fields
                        .get("status")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let Some(advance) = cursor.tool_terminal_novel(&update.tool_call_id, status)
                    else {
                        continue;
                    };
                    merged.push(MergedEvent {
                        event: NovelProjectionEvent {
                            method: SESSION_UPDATE_METHOD,
                            payload: tools::tool_call_update_payload(
                                &update.tool_call_id,
                                &update.fields,
                            ),
                            timing: None,
                            advance,
                        },
                        chronology,
                        family_rank: FAMILY_RANK_TOOL,
                        family_ordinal: merged
                            .iter()
                            .filter(|item| item.family_rank == FAMILY_RANK_TOOL)
                            .count(),
                    });
                }
                tools::ToolUpdate::BackgroundTask(update) => {
                    let advance = if update.kind == "tool_call_update" {
                        let fingerprint =
                            payload_fingerprint(&json!([update.payload, update.output_start]));
                        (cursor.background_outputs.get(&update.key) != Some(&fingerprint)).then(
                            || CursorAdvance::BackgroundOutput {
                                key: update.key.clone(),
                                fingerprint,
                                output_start: update.output_start,
                            },
                        )
                    } else {
                        cursor.background_task_novel(&update.key)
                    };
                    let Some(advance) = advance else {
                        continue;
                    };
                    merged.push(MergedEvent {
                        event: NovelProjectionEvent {
                            method: update.method,
                            payload: update.payload.clone(),
                            timing: None,
                            advance,
                        },
                        chronology,
                        family_rank: FAMILY_RANK_TOOL,
                        family_ordinal: merged
                            .iter()
                            .filter(|item| item.family_rank == FAMILY_RANK_TOOL)
                            .count(),
                    });
                }
                tools::ToolUpdate::AvailableCommands(update) => {
                    let payload = update.to_payload();
                    let fingerprint = payload_fingerprint(&payload);
                    let Some(advance) = cursor.commands_changed(fingerprint) else {
                        continue;
                    };
                    merged.push(MergedEvent {
                        event: NovelProjectionEvent {
                            method: SESSION_UPDATE_METHOD,
                            payload,
                            timing: None,
                            advance,
                        },
                        chronology,
                        family_rank: FAMILY_RANK_TOOL,
                        family_ordinal: merged
                            .iter()
                            .filter(|item| item.family_rank == FAMILY_RANK_TOOL)
                            .count(),
                    });
                }
            }
        }

        // 2. Subagents (runtime child requests).
        let subagents = subagents::project_subagents(
            self.node.as_ref(),
            request_id,
            session_id,
            self.bound.effective_context_window(),
        )
        .await?;
        for (index, update) in subagents.updates.iter().enumerate() {
            let chronology = subagents.chronology.get(index).copied().flatten();
            let payload = update.to_payload();
            let key = format!("{}:{}", update.session_update_kind(), update.subagent_id());
            let fingerprint = payload_fingerprint(&payload);
            let Some(advance) = cursor.subagent_changed(&key, fingerprint) else {
                continue;
            };
            merged.push(MergedEvent {
                event: NovelProjectionEvent {
                    method: SUBAGENT_NOTIFICATION_METHOD,
                    payload,
                    timing: None,
                    advance,
                },
                chronology,
                family_rank: FAMILY_RANK_SUBAGENT,
                family_ordinal: merged
                    .iter()
                    .filter(|item| item.family_rank == FAMILY_RANK_SUBAGENT)
                    .count(),
            });
        }

        // 5. Durable messages (assistant transcript chunks), reconciled
        //    against the live view planned above. The user echo of the
        //    current prompt is skipped: the turn already sent it directly.
        //    The durable pass plans against the same shadow state the live
        //    pass advanced (plus a shadow of the durable chunk states), so
        //    live and durable observations of the same logical segment never
        //    duplicate and a failed send replans the identical candidates.
        //    Intermediate rows (without the materialization pointer) are
        //    reconsidered for upsert growth in transcript order.
        let mut durable_trailing = Vec::new();
        {
            let mut planned_durable = cursor.durable_chunks.clone();
            // Aggregate each durable row/rail before applying live evidence:
            // one live segment spans all text blocks of that rail, so
            // comparing evidence independently to each block would duplicate
            // multi-block rows.
            let mut row_texts: BTreeMap<(DurableRowIdentity, EvidenceRail), String> =
                BTreeMap::new();
            for (index, update) in messages.updates.iter().enumerate() {
                let Some(sequence) = messages.chronology.get(index).copied().flatten() else {
                    continue;
                };
                let (rail, text) = match update {
                    messages::MessageUpdate::AgentMessageChunk { text } => {
                        (EvidenceRail::Content, text)
                    }
                    messages::MessageUpdate::AgentThoughtChunk { text } => {
                        (EvidenceRail::Reasoning, text)
                    }
                    messages::MessageUpdate::UserMessageChunk { .. } => continue,
                };
                let Some(key) = messages.update_keys.get(index) else {
                    continue;
                };
                let Some(message_key) = durable_message_key(key, update.session_update_kind())
                else {
                    continue;
                };
                row_texts
                    .entry((
                        DurableRowIdentity {
                            sequence,
                            message_key: message_key.to_string(),
                        },
                        rail,
                    ))
                    .or_default()
                    .push_str(text);
            }

            // Bind exact materialization stamps first. For unstamped
            // intermediate rows, accept only a globally unique assignment
            // whose row identities increase with the history order. A
            // locally unique match is insufficient: A/B evidence against
            // rows B/A would otherwise cross-bind and hide durable order.
            bind_durable_evidence(
                &mut planned_live.content,
                EvidenceRail::Content,
                &durable_rows,
            );
            bind_durable_evidence(
                &mut planned_live.reasoning,
                EvidenceRail::Reasoning,
                &durable_rows,
            );

            let mut row_offsets: BTreeMap<(DurableRowIdentity, EvidenceRail), usize> =
                BTreeMap::new();
            for (index, update) in messages.updates.iter().enumerate() {
                let Some(key) = messages.update_keys.get(index) else {
                    continue;
                };
                if key.trim().is_empty() {
                    continue;
                }
                let chronology = messages.chronology.get(index).copied().flatten();
                if let messages::MessageUpdate::UserMessageChunk { text } = update {
                    // Keep the exact durable wakeup echo, but tag it as
                    // runtime input using Grok's native hidden-echo metadata.
                    // Lifecycle events surface the completion in the UI.
                    let is_notification = durable_message_key(key, update.session_update_kind())
                        .is_some_and(gents::background_completion::is_background_completion_notification_message_key);
                    if is_notification {
                        let planned = planned_durable.entry(key.clone()).or_default();
                        let sent_len = text
                            .strip_prefix(&planned.sent_text)
                            .map(|suffix| text.len() - suffix.len())
                            .unwrap_or(0);
                        if sent_len < text.len() {
                            merged.push(MergedEvent {
                                event: NovelProjectionEvent {
                                    method: SESSION_UPDATE_METHOD,
                                    payload: messages::MessageUpdate::background_completion_payload(
                                        &text[sent_len..],
                                    ),
                                    timing: None,
                                    advance: CursorAdvance::DurableChunk {
                                        message_key: key.clone(),
                                        sent_text: text.clone(),
                                    },
                                },
                                chronology,
                                family_rank: FAMILY_RANK_MESSAGE,
                                family_ordinal: index,
                            });
                            planned.sent_text = text.clone();
                        }
                    }
                    continue;
                }
                let (rail, text) = match update {
                    messages::MessageUpdate::AgentMessageChunk { text } => {
                        (EvidenceRail::Content, text)
                    }
                    messages::MessageUpdate::AgentThoughtChunk { text } => {
                        (EvidenceRail::Reasoning, text)
                    }
                    messages::MessageUpdate::UserMessageChunk { .. } => continue,
                };
                // Reconcile live against durable. A final row bound by
                // `materialized_message_sequence` is the same logical segment
                // the live tail streamed: the bytes the live cursor already
                // sent of that segment are a prefix of the row's text, and
                // only the remaining suffix is durable-novel. Intermediate
                // rows are durable observations of already-completed segments
                // (persisted before the tail reset), so live bytes of a
                // *later* segment never suppress them; their delivered state
                // is purely the per-chunk cursor.
                let live = match rail {
                    EvidenceRail::Reasoning => &planned_live.reasoning,
                    EvidenceRail::Content => &planned_live.content,
                };
                let row_identity = chronology.and_then(|sequence| {
                    durable_message_key(key, update.session_update_kind()).map(|message_key| {
                        DurableRowIdentity {
                            sequence,
                            message_key: message_key.to_string(),
                        }
                    })
                });
                let bound_evidence = chronology.and_then(|_| {
                    live.closed_evidence
                        .iter()
                        .find(|evidence| evidence.bound_row.as_ref() == row_identity.as_ref())
                });
                let evidence = bound_evidence
                    .map(|evidence| evidence.sent_bytes.as_str())
                    .unwrap_or_default();
                let row_offset = row_identity
                    .clone()
                    .map(|identity| {
                        let offset = row_offsets.entry((identity, rail)).or_default();
                        let current = *offset;
                        *offset = offset.saturating_add(text.len());
                        current
                    })
                    .unwrap_or_default();
                let evidence_covered = evidence.len().saturating_sub(row_offset).min(text.len());
                let planned = planned_durable.entry(key.clone()).or_default();
                let mut sent_len = if text.starts_with(&planned.sent_text) {
                    planned.sent_text.len()
                } else {
                    // A replacement/shrink/divergence is not growth. Never
                    // slice an unrelated UTF-8 value at a stale byte offset;
                    // project the new authoritative value in full.
                    0
                };
                if evidence_covered > sent_len
                    && text.is_char_boundary(evidence_covered)
                    && row_identity
                        .as_ref()
                        .and_then(|identity| row_texts.get(&(identity.clone(), rail)))
                        .is_some_and(|row_text| row_text.starts_with(evidence))
                {
                    sent_len = evidence_covered;
                }
                if sent_len >= text.len() {
                    planned.sent_text = text.clone();
                    durable_trailing.push(CursorAdvance::DurableChunk {
                        message_key: key.clone(),
                        sent_text: text.clone(),
                    });
                    continue;
                }
                // UTF-8 safety: `sent_len` is either zero, a previously
                // observed chunk-text length of this same row, or a
                // live-prefix length of the same logical segment's bytes —
                // all char boundaries of `text`.
                let suffix = text[sent_len..].to_string();
                let payload_kind = update.session_update_kind();
                let segment_key = bound_evidence
                    .and_then(|evidence| evidence.segment_key.clone())
                    .or_else(|| {
                        row_identity.as_ref().map(|identity| {
                            format!("message:{}:{}", identity.sequence, identity.message_key)
                        })
                    });
                let timing = segment_key
                    .map(|segment_key| cursor.timing_for_segment(segment_key, chronology));
                merged.push(MergedEvent {
                    event: NovelProjectionEvent {
                        method: SESSION_UPDATE_METHOD,
                        payload: messages::MessageUpdate::chunk_payload(payload_kind, suffix),
                        timing,
                        advance: CursorAdvance::DurableChunk {
                            message_key: key.clone(),
                            sent_text: text.clone(),
                        },
                    },
                    chronology,
                    family_rank: FAMILY_RANK_MESSAGE,
                    family_ordinal: merged
                        .iter()
                        .filter(|item| item.family_rank == FAMILY_RANK_MESSAGE)
                        .count(),
                });
                planned.sent_text = text.clone();
            }
        }

        // 6. Cross-family merge: emit in durable chronology order, never
        // family-batched. The primary key is the durable transcript position
        // each family shares (tool `message_sequence`, message `sequence`,
        // and the subagent's spawn-tool `message_sequence` all allocate from
        // the same session transcript sequence space), so a client replaying
        // the stream observes tool calls, subagent lifecycles, and message
        // chunks in the order the transcript recorded them. Ties break by
        // family rank: message chunks of an assistant turn precede the tool
        // call that turn issued (thought-before-text precedes the call), and
        // a `subagent_spawned` follows its spawn tool call. Within a family,
        // equal positions break by the durable stable identity each family's
        // decoded rows were sorted by (the tool call's stable id, the spawn
        // row's tool call id, the child's request id), so the merged wire
        // order is a pure function of the durable rows and never of query
        // iteration order. Positionless events
        // (`available_commands_update`, rows without a sequence, and
        // subagents without a spawn row) sort after every positioned event
        // of their family, preserving each family's own emission order.
        merged.sort_by(|a, b| family_sort_key(a).cmp(&family_sort_key(b)));
        let mut events: Vec<NovelProjectionEvent> =
            merged.into_iter().map(|item| item.event).collect();
        let mut trailing_advances = durable_trailing;
        if let Some(sequence) = messages.message_sequence_high_water {
            trailing_advances.push(CursorAdvance::MessageHighWater { sequence });
        }
        // The final shadow rail states include every no-byte commit and every
        // evidence binding. They are the batch suffix and commit only after
        // all wire events succeed.
        if let Some(cid) = planned_live.reasoning.absorbed_commit.clone() {
            trailing_advances.push(CursorAdvance::LiveReasoning {
                plan: planned_live.reasoning.anchor_plan(&cid),
                progress_seq: planned_live.reasoning.progress_seq,
            });
        }
        if let Some(cid) = planned_live.content.absorbed_commit.clone() {
            trailing_advances.push(CursorAdvance::LiveContent {
                plan: planned_live.content.anchor_plan(&cid),
                progress_seq: planned_live.content.progress_seq,
            });
        }
        if response_doc_changed {
            if let Some(doc_id) = messages.response_doc_id.clone() {
                let reset = CursorAdvance::ResponseDocument { doc_id };
                if let Some(first) = events.first_mut() {
                    first.advance = CursorAdvance::Many(vec![reset, first.advance.clone()]);
                } else {
                    trailing_advances.insert(0, reset);
                }
            }
        }
        Ok(ProjectionBatch {
            events,
            trailing_advances,
        })
    }
}

/// Family ranks for the cross-family merge at equal chronology. Lower rank
/// emits first: message chunks (reasoning precedes the assistant turn's tool
/// call), then the tool call, then the subagent that spawn tool created.
const FAMILY_RANK_MESSAGE: u8 = 0;
const FAMILY_RANK_TOOL: u8 = 1;
const FAMILY_RANK_SUBAGENT: u8 = 2;

/// One novel event tagged with its durable chronology key and merge tiebreak
/// data. Internal to [`ProjectionEngine::project_request_updates`].
struct MergedEvent {
    event: NovelProjectionEvent,
    /// Durable transcript position (`None` = positionless).
    chronology: Option<i64>,
    /// Family rank for ties at the same chronology.
    family_rank: u8,
    /// Zero-based emission ordinal within this poll's family stream, keeping
    /// each family's own order for positionless tails.
    family_ordinal: usize,
}

/// The full sort key of one merged event: `(position, family rank,
/// family ordinal)`. Positionless events sort last within their family by
/// using a sentinel position of `i64::MAX`.
fn family_sort_key(event: &MergedEvent) -> (i64, u8, usize) {
    (
        event.chronology.unwrap_or(i64::MAX),
        event.family_rank,
        event.family_ordinal,
    )
}

/// One novel projection event: the update payload to send plus the cursor
/// advance that records its durable identity once the send succeeds.
#[derive(Debug, Clone)]
pub(crate) struct NovelProjectionEvent {
    /// JSON-RPC notification method for this event's protocol family.
    pub(crate) method: &'static str,
    /// The `session/update` payload (`sessionUpdate` object) to wrap and
    /// send.
    pub(crate) payload: Value,
    /// Stable logical model-generation identity and its best durable start
    /// candidate. The turn sender resolves this into one request-local,
    /// strictly increasing `streamStartMs` and reuses it on every later
    /// chunk/retry of the same segment.
    pub(crate) timing: Option<ProjectionEventTiming>,
    /// The advance that records this event as delivered once it is sent.
    pub(crate) advance: CursorAdvance,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectionEventTiming {
    pub(crate) segment_key: String,
    pub(crate) stream_start_candidate_ms: Option<i64>,
    pub(crate) agent_timestamp_candidate_ms: Option<i64>,
}

/// One fully planned projection poll.
///
/// Byte-carrying advances travel with their outbound event. State changes
/// which intentionally carry no wire bytes (for example a tail-reset
/// snapshot) are held in `trailing_advances` and committed only after every
/// event in this batch was sent successfully. This keeps an unsent earlier
/// byte event from being leapfrogged by a later reset/no-op observation.
#[derive(Debug, Default)]
pub(crate) struct ProjectionBatch {
    pub(crate) events: Vec<NovelProjectionEvent>,
    pub(crate) trailing_advances: Vec<CursorAdvance>,
}

impl std::ops::Deref for ProjectionBatch {
    type Target = [NovelProjectionEvent];

    fn deref(&self) -> &Self::Target {
        &self.events
    }
}

/// The recorded identity of one novel projection event. Recorded only after
/// the corresponding notification line was successfully sent.
#[derive(Debug, Clone)]
pub(crate) enum CursorAdvance {
    /// Apply several infallible cursor transitions in order at one delivery
    /// commit point.
    Many(Vec<CursorAdvance>),
    /// Adopt a replacement response document generation before applying
    /// live-tail advances from it.
    ResponseDocument { doc_id: String },
    /// The full base payload of a tool call was observed (first time or
    /// changed tracked fields).
    ToolBase {
        tool_call_id: String,
        payload: Value,
    },
    /// A terminal same-id tool update was delivered (needed separately from
    /// the base registration so a first-observed-terminal task clears the
    /// pager's foreground wait).
    ToolTerminal {
        tool_call_id: String,
        status: String,
    },
    /// A distinct visible tool list was observed.
    Commands { fingerprint: u64 },
    /// A distinct subagent payload was observed for its key.
    Subagent { key: String, fingerprint: u64 },
    /// One native background task lifecycle notification was delivered.
    BackgroundTask { key: String },
    BackgroundOutput {
        key: String,
        fingerprint: u64,
        output_start: Option<u64>,
    },
    ChildOutput {
        key: String,
        receipt: child_output::OutputReceipt,
    },
    /// A live response tail delta was planned and sent. The `plan` is the
    /// post-send cursor state (including the history anchor it absorbed);
    /// `progress_seq` is the durable progress counter observed with this
    /// snapshot, recorded so an identical later rewrite (advanced seq) is
    /// distinguishable from a stale identical read (unchanged seq).
    LiveContent {
        plan: LiveSegmentPlan,
        progress_seq: Option<u64>,
    },
    /// Same as [`CursorAdvance::LiveContent`] for the reasoning tail.
    LiveReasoning {
        plan: LiveSegmentPlan,
        progress_seq: Option<u64>,
    },
    /// A durable message chunk's exact delivered text advanced after send.
    DurableChunk {
        message_key: String,
        sent_text: String,
    },
    /// Inclusive durable transcript query cursor. This advances only after
    /// the complete projection batch succeeds, so a leaf/send failure re-reads
    /// every still-undelivered row.
    MessageHighWater { sequence: i64 },
}

/// The post-send state of one live tail cursor, carried inside a
/// [`CursorAdvance`] so the send-success `record` path is the only mutator of
/// the real cursor's *delivered* state.
#[derive(Clone, Debug, Default)]
pub(crate) struct LiveSegmentPlan {
    /// Stable identity of the current logical model generation. Assigned
    /// from the first response-history commit that carries bytes after a
    /// reset and retained across later prefix-growth commits.
    segment_key: Option<String>,
    /// The observed tail snapshot after this send.
    observed: String,
    /// How many bytes of the current segment's logical stream were
    /// successfully sent (including this delta).
    sent_len: usize,
    /// The exact bytes of the current segment already sent, retained so the
    /// durable reconciliation can prove a live prefix covers a durable row's
    /// start even after the observed window rolls or the tail resets.
    sent_bytes: String,
    /// The exact bytes already sent of the most recently *closed* segment.
    /// A tail reset (or a divergence close) ends the current segment while
    /// its delivered bytes stay delivered: they are retained here as the
    /// reconciliation evidence for the closed segment's materialized row,
    /// which is exactly the row `materialized_message_sequence` binds.
    completed_sent_bytes: String,
    /// Delivered closed segments awaiting (or carrying) an exact durable-row
    /// binding. This queue is never arbitrarily capped: dropping an older
    /// entry could make a later durable row duplicate bytes already sent.
    closed_evidence: Vec<ClosedEvidence>,
    /// The reasoning preview advanced without any overlap with the preceding
    /// persisted window. Bytes between the windows are not reconstructable
    /// from response history, so this rail stays deferred until the durable
    /// assistant row supplies the authoritative full reasoning text.
    unproven_gap: bool,
    /// The composite commit tip this plan absorbed: recorded into the
    /// cursor's `absorbed_commit` only when the send succeeds, so the
    /// next poll proves continuity against the exact history slice past
    /// that commit. `None` when the observation carried no readable
    /// history (the cursor's absorbed commit then stands unchanged — never
    /// regressed).
    absorbed_commit: Option<String>,
}

/// A request-local live response tail cursor for one logical byte stream
/// (`content` or `reasoning` of the current `AgentResponse`).
///
/// The `AgentResponse` live tails are *segment-local*, not
/// request-cumulative: the runtime clears them on ToolResult,
/// FinalResponse materialization, TurnRetracted, OutputObligationPending,
/// and interrupted/error partial persistence. Within one segment the tail
/// grows by exact prefix append. This cursor tracks:
///
/// - `observed`: the most recent snapshot of the tail (for reasoning, the
///   rolling 64-KiB preview — the *preview window*, not the logical stream);
/// - `sent_len` / `sent_bytes`: how many bytes of the current segment's
///   logical stream have been *successfully sent*, and their exact bytes;
/// - `progress_seq`: the durable progress counter observed with the last
///   snapshot, used to distinguish a stale identical read from a genuine
///   later identical rewrite.
///
/// ## Append-only / no-loss policy (documented contract)
///
/// ACP chunks are append-only: bytes that were already sent can never be
/// retracted. A divergence — the freshly observed tail no longer starts with
/// the previously observed snapshot (a TurnRetracted, a retracted turn, or a
/// racing retry-replaced response row) — therefore never slices into the
/// sent prefix and never pretends already-sent bytes can be taken back.
/// Instead the divergence *closes* the current segment and the whole freshly
/// observed snapshot opens a new segment: the un-sent remainder of the old
/// segment is deliberately dropped (the runtime retracted it), while the new
/// observation is streamed in full — no part of what the durable row now
/// shows is ever lost.
#[derive(Clone, Debug, Default)]
pub(crate) struct LiveSegmentCursor {
    /// See [`LiveSegmentPlan::segment_key`].
    segment_key: Option<String>,
    /// The most recently observed snapshot of the live tail.
    observed: String,
    /// How many bytes of the current segment's logical stream were already
    /// successfully sent.
    sent_len: usize,
    /// The exact already-sent bytes of the current segment (evidence for
    /// live/durable reconciliation after a window roll or tail reset).
    sent_bytes: String,
    /// The exact already-sent bytes of the most recently closed segment
    /// (reconciliation evidence: the closed segment's materialized row is
    /// the row `materialized_message_sequence` binds).
    completed_sent_bytes: String,
    /// Ordered delivered evidence for every closed logical segment.
    closed_evidence: Vec<ClosedEvidence>,
    /// See [`LiveSegmentPlan::unproven_gap`]. While set, later live previews
    /// only advance the history anchor; none can prove the missing bytes.
    unproven_gap: bool,
    /// The durable progress counter observed with the last snapshot.
    progress_seq: Option<u64>,
    /// The CID of the last composite commit of the response document's
    /// history whose snapshot this cursor has fully absorbed (planned *and*
    /// recorded — send-success). Continuity of the next observation is
    /// proven against the history slice past this commit: any intervening
    /// tail-reset commit (empty tail, unchanged seqs) breaks continuity and
    /// forces a fresh segment, while intervening no-op rewrites extend it.
    /// `None` before the first observation of the turn.
    absorbed_commit: Option<String>,
}

/// The request-local pair of live tail cursors: reasoning and content.
#[derive(Clone, Debug, Default)]
struct LiveCursorPair {
    reasoning: LiveSegmentCursor,
    content: LiveSegmentCursor,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
struct DurableRowIdentity {
    sequence: i64,
    message_key: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ClosedEvidence {
    sent_bytes: String,
    materialized_sequence: Option<i64>,
    bound_row: Option<DurableRowIdentity>,
    history_height: i64,
    segment_key: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct DurableRowView {
    identity: DurableRowIdentity,
    content: String,
    reasoning: String,
}

fn durable_message_key<'a>(update_key: &'a str, kind: &str) -> Option<&'a str> {
    let (prefix, ordinal) = update_key.rsplit_once(':')?;
    ordinal.parse::<u64>().ok()?;
    prefix.strip_suffix(&format!(":{kind}"))
}

fn durable_row_views(messages: &messages::MessageProjection) -> Vec<DurableRowView> {
    let mut rows: BTreeMap<DurableRowIdentity, DurableRowView> = BTreeMap::new();
    for (index, update) in messages.updates.iter().enumerate() {
        let Some(sequence) = messages.chronology.get(index).copied().flatten() else {
            continue;
        };
        let Some(update_key) = messages.update_keys.get(index) else {
            continue;
        };
        let kind = update.session_update_kind();
        let Some(message_key) = durable_message_key(update_key, kind) else {
            continue;
        };
        let identity = DurableRowIdentity {
            sequence,
            message_key: message_key.to_string(),
        };
        let row = rows
            .entry(identity.clone())
            .or_insert_with(|| DurableRowView {
                identity,
                ..DurableRowView::default()
            });
        match update {
            messages::MessageUpdate::AgentMessageChunk { text } => row.content.push_str(text),
            messages::MessageUpdate::AgentThoughtChunk { text } => row.reasoning.push_str(text),
            messages::MessageUpdate::UserMessageChunk { .. } => {}
        }
    }
    rows.into_values().collect()
}

fn row_for_materialized_sequence(
    sequence: Option<i64>,
    rows: &[DurableRowView],
) -> Option<DurableRowIdentity> {
    let sequence = sequence?;
    let mut matches = rows
        .iter()
        .filter(|row| row.identity.sequence == sequence)
        .map(|row| row.identity.clone());
    let only = matches.next()?;
    matches.next().is_none().then_some(only)
}

#[derive(Default)]
struct HistorySegmentSketch {
    snapshot_indices: Vec<usize>,
    content: Option<String>,
    reasoning: Option<String>,
    reasoning_window: String,
    reasoning_seq: Option<u64>,
}

/// Infer exact row identities for history segments only when the complete
/// segment-to-row assignment is unique and strictly increasing (an unmatched,
/// provably open tip may remain unbound). Any remaining
/// ambiguity or inversion returns no inferred bindings; durable projection
/// then remains authoritative and may duplicate rather than hide bytes.
fn infer_history_row_bindings(
    history: &[messages::CompositeSnapshot],
    rows: &[DurableRowView],
) -> BTreeMap<usize, DurableRowIdentity> {
    let mut segments: Vec<HistorySegmentSketch> = Vec::new();
    let mut current: Option<usize> = None;
    for (index, snapshot) in history.iter().enumerate() {
        let content = snapshot.content();
        let reasoning = snapshot.reasoning();
        if content.is_empty() && reasoning.is_empty() {
            if let Some(segment) = current.take() {
                segments[segment].snapshot_indices.push(index);
            }
            continue;
        }
        let segment = *current.get_or_insert_with(|| {
            segments.push(HistorySegmentSketch {
                content: Some(String::new()),
                reasoning: Some(String::new()),
                ..HistorySegmentSketch::default()
            });
            segments.len() - 1
        });
        let sketch = &mut segments[segment];
        sketch.snapshot_indices.push(index);
        if !content.is_empty() {
            sketch.content = match sketch.content.take() {
                Some(previous) if content.starts_with(&previous) => Some(content.to_string()),
                Some(previous) if previous.is_empty() => Some(content.to_string()),
                _ => None,
            };
        }
        if !reasoning.is_empty() {
            let next_seq = snapshot.reasoning_progress_seq();
            sketch.reasoning = match sketch.reasoning.take() {
                Some(mut known) if sketch.reasoning_window.is_empty() => {
                    known.push_str(reasoning);
                    Some(known)
                }
                Some(mut known) if reasoning.starts_with(&sketch.reasoning_window) => {
                    known.push_str(&reasoning[sketch.reasoning_window.len()..]);
                    Some(known)
                }
                Some(mut known)
                    if matches!((sketch.reasoning_seq, next_seq), (Some(old), Some(new)) if new > old)
                        && proven_reasoning_rollover(&sketch.reasoning_window, reasoning)
                            .is_some() =>
                {
                    let overlap = proven_reasoning_rollover(&sketch.reasoning_window, reasoning)
                        .expect("checked above");
                    known.push_str(&reasoning[overlap..]);
                    Some(known)
                }
                _ => None,
            };
            sketch.reasoning_window = reasoning.to_string();
            sketch.reasoning_seq = next_seq;
        }
    }

    let mut candidate_rows = Vec::new();
    for segment in &segments {
        let candidates: Vec<_> = rows
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                let content_matches = segment
                    .content
                    .as_deref()
                    .filter(|value| !value.is_empty())
                    .map_or(true, |value| row.content.starts_with(value));
                let reasoning_matches = segment
                    .reasoning
                    .as_deref()
                    .filter(|value| !value.is_empty())
                    .map_or(true, |value| row.reasoning.starts_with(value));
                let has_evidence = segment
                    .content
                    .as_deref()
                    .is_some_and(|value| !value.is_empty())
                    || segment
                        .reasoning
                        .as_deref()
                        .is_some_and(|value| !value.is_empty());
                has_evidence && content_matches && reasoning_matches
            })
            .map(|(index, _)| index)
            .collect();
        candidate_rows.push(candidates);
    }
    // A provably open, not-yet-materialized tip has no durable candidate.
    // Its absence must not invalidate a uniquely identified closed prefix.
    // Do not relax any missing/interior, divergent, or stamped segment.
    let open_unmaterialized_tip = current.is_some_and(|index| index + 1 == segments.len())
        && candidate_rows.last().is_some_and(Vec::is_empty)
        && segments
            .last()
            .is_some_and(|segment| segment.content.is_some() && segment.reasoning.is_some())
        && history
            .last()
            .is_some_and(|snapshot| snapshot.materialized_message_sequence().is_none());
    let bound_count = candidate_rows.len() - usize::from(open_unmaterialized_tip);
    let Some(assignment) = unique_increasing_assignment(&candidate_rows[..bound_count]) else {
        return BTreeMap::new();
    };
    let mut bindings = BTreeMap::new();
    for (segment, row_index) in segments.iter().zip(assignment) {
        for index in &segment.snapshot_indices {
            bindings.insert(*index, rows[row_index].identity.clone());
        }
    }
    bindings
}

/// Sorted candidate indices admit a unique ordered assignment exactly when
/// the earliest and latest feasible assignments coincide. A repeated prefix
/// can be ambiguous locally but unambiguous in the complete transcript.
fn unique_increasing_assignment(candidates: &[Vec<usize>]) -> Option<Vec<usize>> {
    let mut earliest = Vec::with_capacity(candidates.len());
    let mut previous = None;
    for choices in candidates {
        let next = choices
            .iter()
            .copied()
            .find(|index| previous.is_none_or(|p| *index > p))?;
        earliest.push(next);
        previous = Some(next);
    }
    let mut following = None;
    for (choices, earliest) in candidates.iter().zip(&earliest).rev() {
        let latest = choices
            .iter()
            .rev()
            .copied()
            .find(|index| following.is_none_or(|p| *index < p))?;
        if latest != *earliest {
            return None;
        }
        following = Some(latest);
    }
    Some(earliest)
}

fn upsert_bound_evidence(
    evidence: &mut Vec<ClosedEvidence>,
    sent_bytes: String,
    materialized_sequence: Option<i64>,
    row: DurableRowIdentity,
    history_height: i64,
    segment_key: Option<String>,
) {
    if let Some(existing) = evidence
        .iter_mut()
        .find(|item| item.bound_row.as_ref() == Some(&row))
    {
        if sent_bytes.starts_with(&existing.sent_bytes) {
            existing.sent_bytes = sent_bytes;
        }
        existing.materialized_sequence = existing.materialized_sequence.or(materialized_sequence);
        existing.segment_key = existing.segment_key.clone().or(segment_key);
        return;
    }
    evidence.push(ClosedEvidence {
        sent_bytes,
        materialized_sequence,
        bound_row: Some(row),
        history_height,
        segment_key,
    });
}

fn bind_open_tip_evidence(
    live: &mut LiveSegmentCursor,
    row: &DurableRowIdentity,
    rail: EvidenceRail,
    rows: &[DurableRowView],
    history_height: i64,
) {
    if live.sent_bytes.is_empty() {
        return;
    }
    let Some(view) = rows.iter().find(|view| &view.identity == row) else {
        return;
    };
    let durable = match rail {
        EvidenceRail::Content => &view.content,
        EvidenceRail::Reasoning => &view.reasoning,
    };
    if !durable.starts_with(&live.sent_bytes) {
        return;
    }
    let sent_bytes = live.sent_bytes.clone();
    let segment_key = live.segment_key.clone();
    upsert_bound_evidence(
        &mut live.closed_evidence,
        sent_bytes,
        None,
        row.clone(),
        history_height,
        segment_key,
    );
}

fn bind_durable_evidence(
    live: &mut LiveSegmentCursor,
    rail: EvidenceRail,
    rows: &[DurableRowView],
) {
    for evidence in &mut live.closed_evidence {
        if evidence.bound_row.is_none() {
            evidence.bound_row =
                row_for_materialized_sequence(evidence.materialized_sequence, rows);
        }
    }
    let unbound: Vec<usize> = live
        .closed_evidence
        .iter()
        .enumerate()
        .filter_map(|(index, evidence)| evidence.bound_row.is_none().then_some(index))
        .collect();
    let already_bound: std::collections::BTreeSet<_> = live
        .closed_evidence
        .iter()
        .filter_map(|evidence| evidence.bound_row.clone())
        .collect();
    let mut tentative = Vec::new();
    for index in &unbound {
        let evidence = &live.closed_evidence[*index];
        let candidates: Vec<_> = rows
            .iter()
            .filter(|row| !already_bound.contains(&row.identity))
            .filter(|row| {
                let text = match rail {
                    EvidenceRail::Content => &row.content,
                    EvidenceRail::Reasoning => &row.reasoning,
                };
                !evidence.sent_bytes.is_empty() && text.starts_with(&evidence.sent_bytes)
            })
            .map(|row| row.identity.clone())
            .collect();
        let [row] = candidates.as_slice() else {
            return;
        };
        tentative.push((*index, row.clone()));
    }
    let mut ordered: Vec<_> = live
        .closed_evidence
        .iter()
        .enumerate()
        .filter_map(|(index, evidence)| {
            evidence
                .bound_row
                .clone()
                .map(|row| (evidence.history_height, index, row))
        })
        .chain(tentative.iter().map(|(index, row)| {
            (
                live.closed_evidence[*index].history_height,
                *index,
                row.clone(),
            )
        }))
        .collect();
    ordered.sort_by_key(|(height, index, _)| (*height, *index));
    if ordered.windows(2).any(|pair| pair[0].2 >= pair[1].2) {
        return;
    }
    for (index, row) in tentative {
        live.closed_evidence[index].bound_row = Some(row);
    }
}

impl LiveSegmentCursor {
    /// Plan the novel delta of one freshly observed tail snapshot.
    ///
    /// Planning is a pure function of `(self, observed, progress_seq, rail,
    /// history_continuity)`, so a failed send (plan never recorded)
    /// re-plans the byte-identical candidate on the next poll. The poll
    /// plans each candidate against a shadow copy of the cursor advanced
    /// by the preceding planned candidates of the same poll.
    ///
    /// Returns the delta to emit plus the post-send cursor state.
    /// `None` means nothing novel and no state change.
    ///
    /// `history_continuity` is the composite-history gate: `true` when no
    /// tail-reset commit (an empty-tail snapshot with unchanged progress
    /// counters) intervenes in the response document's commit history
    /// between this cursor's `absorbed_commit` and the tip this poll
    /// observed — or when the tail is being observed for the first time.
    /// `false` when a reset intervened (a missed poll window: the segment
    /// closed and a new one began) or the history could not be read. It is
    /// the *only* sound discriminator between a no-op identical rewrite
    /// (continuity: absorb, emit nothing) and a missed reset followed by a
    /// byte-identical new segment (no continuity: re-emit in full) — the
    /// progress counters alone cannot separate them, because a no-op
    /// `write_reasoning` bumps its counter without changing bytes.
    ///
    /// The rails differ, and `plan` is rail-aware through the
    /// `reasoning_tail` flag:
    ///
    /// - **reasoning** (`reasoning_tail = true`): `reasoning_progress_seq`
    ///   advances on *every* `write_reasoning` append (including a no-op
    ///   append of empty bytes). With continuity, an advance on
    ///   byte-identical bytes is therefore a no-op rewrite: the bytes were
    ///   already delivered and must not duplicate on the wire — the commit
    ///   is absorbed and nothing streams. Without continuity the identical
    ///   bytes are a new segment's and stream in full. The reasoning tail
    ///   is also a bounded rolling preview (`MAX_LIVE_REASONING_BYTES`): a
    ///   non-prefix change is accepted as continuity only when it is
    ///   *proven* a window rollover — the previous observation was
    ///   at/near the bound, the new observation is bounded by it, the new
    ///   observation starts with the longest retained suffix of the
    ///   previous window, *and* the history continuity holds. Any other
    ///   divergence closes the segment and re-emits the whole new snapshot.
    /// - **content** (`reasoning_tail = false`): `progress_seq` is a
    ///   *lifecycle boundary* counter (first visible text, tool call, tool
    ///   result, final response), so an advance on byte-identical bytes is
    ///   the ordinary same-segment case (a boundary landed that did not
    ///   touch the tail) — nothing new, never a re-emit. The content tail
    ///   is segment-cumulative, never windowed: only exact prefix growth
    ///   (with continuity) continues a segment; any shrink, non-prefix
    ///   growth, divergence, or broken continuity closes the segment and
    ///   re-emits the whole new snapshot.
    ///
    /// The fresh-segment plan for one whole observed snapshot: the new
    /// segment's logical text starts at the snapshot, the previous
    /// segment's delivered bytes close into `completed_sent_bytes` (they
    /// stay delivered as that segment's reconciliation evidence), and the
    /// history absorption is left to the caller (the tip commit it planned
    /// against — every rebase must re-stamp it, never inherit a stale one).
    fn fresh_segment_plan(&self, observed: &str) -> LiveSegmentPlan {
        LiveSegmentPlan {
            segment_key: None,
            observed: observed.to_string(),
            sent_len: observed.len(),
            sent_bytes: observed.to_string(),
            completed_sent_bytes: self.sent_bytes.clone(),
            closed_evidence: self.closed_evidence.clone(),
            unproven_gap: false,
            absorbed_commit: None,
        }
    }

    fn plan(
        &self,
        observed: &str,
        progress_seq: Option<u64>,
        reasoning_tail: bool,
        history_continuity: bool,
    ) -> Option<(String, LiveSegmentPlan)> {
        let seq_advanced = match (self.progress_seq, progress_seq) {
            (Some(previous), Some(current)) => current > previous,
            _ => false,
        };
        // The reset observation and every divergence/close rebase the
        // segment: the plan's post-send `absorbed_commit` is set by the
        // caller (the history tip it planned against); no plan here may
        // leave a stale absorbed commit from an earlier segment.
        let plan_absorbs = |observed: String,
                            sent_len: usize,
                            sent_bytes: String,
                            completed: String|
         -> LiveSegmentPlan {
            LiveSegmentPlan {
                segment_key: self.segment_key.clone(),
                observed,
                sent_len,
                sent_bytes,
                completed_sent_bytes: completed,
                closed_evidence: self.closed_evidence.clone(),
                unproven_gap: self.unproven_gap,
                absorbed_commit: None,
            }
        };

        // An observed empty tail after a non-empty observation is the
        // runtime's reset-tail write: it closes the current segment (the
        // rebase for the next observation) and emits nothing. The plan is
        // still returned so the reset is recorded — without it, the next
        // poll would replay the closed segment's bytes as a fresh prefix.
        if observed.is_empty() {
            if self.observed.is_empty()
                && self.sent_bytes.is_empty()
                && self.completed_sent_bytes.is_empty()
            {
                return None;
            }
            // The closed segment's delivered bytes stay delivered: retained
            // as the reconciliation evidence for its materialized row.
            return Some((
                String::new(),
                LiveSegmentPlan {
                    segment_key: None,
                    observed: String::new(),
                    sent_len: 0,
                    sent_bytes: String::new(),
                    completed_sent_bytes: self.sent_bytes.clone(),
                    closed_evidence: self.closed_evidence.clone(),
                    unproven_gap: false,
                    absorbed_commit: None,
                },
            ));
        }

        if self.observed.is_empty() {
            // Fresh segment (or the first observation of the turn): the whole
            // snapshot is the segment's new logical text. An already-closed
            // earlier segment's evidence stays retained.
            return Some((observed.to_string(), self.fresh_segment_plan(observed)));
        }

        // Once a persisted reasoning preview jumps beyond the overlap that
        // history can prove, subsequent windows cannot repair the missing
        // middle. Absorb their commits without emitting bytes; the durable
        // assistant row is the only authoritative recovery source.
        if reasoning_tail && self.unproven_gap {
            return Some((
                String::new(),
                LiveSegmentPlan {
                    segment_key: self.segment_key.clone(),
                    observed: observed.to_string(),
                    sent_len: self.sent_len,
                    sent_bytes: self.sent_bytes.clone(),
                    completed_sent_bytes: self.completed_sent_bytes.clone(),
                    closed_evidence: self.closed_evidence.clone(),
                    unproven_gap: true,
                    absorbed_commit: None,
                },
            ));
        }

        if let Some(suffix) = observed.strip_prefix(self.observed.as_str()) {
            if suffix.is_empty() {
                // Identical bytes. Three cases, resolved by history:
                //
                // 1. *No continuity* (a tail-reset commit intervened since
                //    the cursor's absorbed commit, or the history could not
                //    be read): the identical bytes are a *new segment's*
                //    bytes that happen to equal the old segment's — a
                //    durably-recoverable rewrite that must stream in full
                //    (Blocker: never suppress a recoverable byte).
                // 2. *Continuity, unchanged seq*: a stale identical read —
                //    nothing novel.
                // 3. *Continuity, advanced seq*: a genuine no-op identical
                //    rewrite (the runtime's `write_reasoning("")` bumps
                //    `reasoning_progress_seq` but writes the same bytes).
                //    The bytes were already delivered; re-emitting would
                //    duplicate them. The rewrite is absorbed (the commit
                //    tip advances) and nothing streams.
                if !history_continuity {
                    return Some((observed.to_string(), self.fresh_segment_plan(observed)));
                }
                if !seq_advanced {
                    return None;
                }
                return Some((
                    String::new(),
                    LiveSegmentPlan {
                        segment_key: self.segment_key.clone(),
                        observed: observed.to_string(),
                        sent_len: self.sent_len,
                        sent_bytes: self.sent_bytes.clone(),
                        completed_sent_bytes: self.completed_sent_bytes.clone(),
                        closed_evidence: self.closed_evidence.clone(),
                        unproven_gap: self.unproven_gap,
                        absorbed_commit: None,
                    },
                ));
            }
            // Exact prefix growth within the segment: only the suffix is new.
            // A broken history continuity (an intervening reset commit) means
            // even prefix-shaped growth is a *new segment's* coincidence: the
            // old segment closed and this observation starts fresh — stream
            // the whole snapshot.
            if !history_continuity {
                return Some((observed.to_string(), self.fresh_segment_plan(observed)));
            }
            let mut sent_bytes = self.sent_bytes.clone();
            sent_bytes.push_str(suffix);
            return Some((
                suffix.to_string(),
                plan_absorbs(
                    observed.to_string(),
                    self.sent_len + suffix.len(),
                    sent_bytes,
                    self.completed_sent_bytes.clone(),
                ),
            ));
        }

        // Non-prefix change. On the content rail this is always a divergence
        // (the content tail is segment-cumulative, never windowed): even an
        // accidental byte overlap — 'abc' → 'cdef' — must not slice into it;
        // the segment closes and the whole new snapshot streams in full.
        if !reasoning_tail {
            return Some((observed.to_string(), self.fresh_segment_plan(observed)));
        }

        // Reasoning rail: a non-prefix change continues the segment only when
        // it is *proven* a bounded-window rollover — the runtime's
        // `MAX_LIVE_REASONING_BYTES` rolling preview dropped head bytes and
        // appended a suffix — *and* the `reasoning_progress_seq` advanced
        // (every rollover is caused by a `write_reasoning` append, which
        // bumps the counter before the flush; a non-prefix change with an
        // unchanged seq cannot be an append and must be treated as a
        // divergence) — *and* the history continuity holds (an intervening
        // reset commit means the observation is a new segment, not a
        // rollover of the old one). Anything else (a diverging rewrite, a
        // shrink that is not a window trim, a mid-window corruption, an
        // accidental byte coincidence) closes the segment and re-emits the
        // whole snapshot.
        if seq_advanced && history_continuity {
            if let Some(overlap) = proven_reasoning_rollover(&self.observed, observed) {
                let delta = observed[overlap..].to_string();
                let mut sent_bytes = self.sent_bytes.clone();
                sent_bytes.push_str(&delta);
                return Some((
                    delta,
                    plan_absorbs(
                        observed.to_string(),
                        self.sent_len + observed.len() - overlap,
                        sent_bytes,
                        self.completed_sent_bytes.clone(),
                    ),
                ));
            }
            if reasoning_window_is_saturated(observed) {
                // This is still the same history segment (no reset
                // intervened), and a full preview with no overlap proves at
                // least one whole window vanished between snapshots.
                // Re-emitting it would omit that gap and later duplicate its
                // tail when the durable row repairs it.
                return Some((
                    String::new(),
                    LiveSegmentPlan {
                        segment_key: self.segment_key.clone(),
                        observed: observed.to_string(),
                        sent_len: self.sent_len,
                        sent_bytes: self.sent_bytes.clone(),
                        completed_sent_bytes: self.completed_sent_bytes.clone(),
                        closed_evidence: self.closed_evidence.clone(),
                        unproven_gap: true,
                        absorbed_commit: None,
                    },
                ));
            }
        }
        // Shrink or divergence: close the segment and rebase onto the whole
        // new snapshot. This is the documented append-only/no-loss policy
        // (see the type docs) — already-sent bytes are never sliced or
        // retracted, and the new observation streams in full on a fresh
        // segment.
        Some((observed.to_string(), self.fresh_segment_plan(observed)))
    }

    /// Commit one planned (and successfully sent) state into this cursor.
    fn commit(&mut self, plan: LiveSegmentPlan, progress_seq: Option<u64>) {
        self.segment_key = plan.segment_key;
        self.observed = plan.observed;
        self.sent_len = plan.sent_len;
        self.sent_bytes = plan.sent_bytes;
        self.completed_sent_bytes = plan.completed_sent_bytes;
        self.closed_evidence = plan.closed_evidence;
        self.unproven_gap = plan.unproven_gap;
        self.progress_seq = progress_seq;
        // Absorb the history tip the plan carried: the cursor's continuity
        // anchor advances exactly with delivery. `None` leaves the anchor
        // standing (an observation without a readable history never
        // regresses it), so the next poll re-proves continuity against the
        // same anchor.
        if let Some(cid) = plan.absorbed_commit {
            self.absorbed_commit = Some(cid);
        }
    }

    /// Full cursor state as a no-byte plan, used to advance a validated
    /// composite anchor without inventing a wire update.
    fn anchor_plan(&self, cid: &str) -> LiveSegmentPlan {
        LiveSegmentPlan {
            segment_key: self.segment_key.clone(),
            observed: self.observed.clone(),
            sent_len: self.sent_len,
            sent_bytes: self.sent_bytes.clone(),
            completed_sent_bytes: self.completed_sent_bytes.clone(),
            closed_evidence: self.closed_evidence.clone(),
            unproven_gap: self.unproven_gap,
            absorbed_commit: Some(cid.to_string()),
        }
    }
}

/// First retained snapshot not yet absorbed by one delivery rail. `None`
/// means a nonempty anchor was absent from this same-document chain.
fn replay_start(history: &[messages::CompositeSnapshot], anchor: Option<&str>) -> Option<usize> {
    match anchor {
        None => Some(0),
        Some(anchor) => history
            .iter()
            .position(|snapshot| snapshot.cid == anchor)
            .map(|index| index + 1),
    }
}

/// Earliest send-success rail anchor retained for the next observation.
/// Both rails must have an anchor before old snapshots can be discarded.
fn earliest_live_anchor(
    observation: &messages::HistoryObservation,
    cursors: &LiveCursorPair,
) -> Option<String> {
    let content = cursors.content.absorbed_commit.as_deref()?;
    let reasoning = cursors.reasoning.absorbed_commit.as_deref()?;
    let chain = observation.retained_chain();
    let content_index = chain.iter().position(|snapshot| snapshot.cid == content)?;
    let reasoning_index = chain
        .iter()
        .position(|snapshot| snapshot.cid == reasoning)?;
    Some(chain[content_index.min(reasoning_index)].cid.clone())
}

/// The runtime's live reasoning preview bound (`MAX_LIVE_REASONING_BYTES` in
/// `gents::streaming`): the durable `AgentResponse.reasoning` tail is a
/// rolling window that never exceeds this many bytes. Duplicated here
/// because the projection must prove a rollover against the same bound the
/// runtime trims to; the two constants must move together.
const MAX_LIVE_REASONING_WINDOW_BYTES: usize = 64 * 1024;

/// `tail_window` advances a cut inside a four-byte scalar by at most three
/// bytes, so a saturated UTF-8 preview may be `MAX-3..=MAX` bytes long.
fn reasoning_window_is_saturated(value: &str) -> bool {
    (MAX_LIVE_REASONING_WINDOW_BYTES.saturating_sub(3)..=MAX_LIVE_REASONING_WINDOW_BYTES)
        .contains(&value.len())
}

/// Proof that `(previous, current)` is a bounded-window rollover of the
/// runtime's live reasoning preview: the runtime's
/// `append_live_reasoning_preview` drops head bytes only when
/// `previous.len() + appended > MAX_LIVE_REASONING_WINDOW_BYTES`, and then
/// the new window is exactly the retained suffix plus the appended bytes.
///
/// The proof therefore requires every fact of that shape:
///
/// - both windows are bounded by the runtime constant (a larger observation
///   is corruption, not a window);
/// - the head actually dropped (`overlap < previous.len()` — this is the
///   near-bound condition: `dropped = previous.len() + appended - MAX ≥ 1`);
/// - bytes actually appended (`current.len() > overlap` — the runtime never
///   shrinks the window without an append);
/// - `current` starts with the longest UTF-8-safe suffix of `previous`
///   (the retained overlap after the head drop).
///
/// Returns the retained overlap length, or `None` when the pair is not a
/// proven rollover. The caller must additionally require an advanced
/// `reasoning_progress_seq`: a rollover is always caused by a
/// `write_reasoning` append, which bumps the counter before the flush, so a
/// genuine rollover observed across two polls always carries an advanced
/// seq. An unproven non-prefix change could be a diverging rewrite or a
/// mid-window corruption; slicing into unproven continuity would silently
/// drop bytes that were never sent (Blocker: overlap must be gated on
/// proven rollover, never on an accidental byte coincidence).
fn proven_reasoning_rollover(previous: &str, current: &str) -> Option<usize> {
    if previous.len() > MAX_LIVE_REASONING_WINDOW_BYTES
        || current.len() > MAX_LIVE_REASONING_WINDOW_BYTES
    {
        return None;
    }
    // The retained-overlap lower bound: the runtime's trim keeps exactly
    // `previous.len() + appended - MAX` bytes of the old window (where
    // `appended = current.len() - overlap`), so an overlap shorter than the
    // genuine trim can never be the transform's retained window (an
    // `abc` -> `cdef` coincidence with a 1-byte "overlap" would emit only
    // `def` and lose bytes that were never delivered — see the unit tests).
    // Computed as `previous.len() - tail_keep` in usize so a pair that
    // never crossed the bound cannot underflow.
    let overlap = suffix_prefix_overlap(previous, current);
    if overlap == 0 || overlap >= previous.len() || overlap >= current.len() {
        return None;
    }
    let appended = current.len() - overlap;
    let kept = previous.len() + appended;
    if kept <= MAX_LIVE_REASONING_WINDOW_BYTES {
        // The pair never crossed the bound: the runtime appends without
        // trimming, so a non-prefix change here is a divergence, not a
        // rollover.
        return None;
    }
    if !current.is_char_boundary(overlap) {
        return None;
    }
    // Exact-transform proof. The runtime's append is exactly
    // `new_window = tail(previous, MAX - |appended|) ++ appended`, so the
    // pair is a proven rollover if and only if taking `appended` as the
    // bytes past the retained overlap and running the runtime's own
    // transform over `previous` reproduces `current` byte for byte. An
    // `abc -> cdef` coincidence, a diverging rewrite, or a mid-window
    // corruption fails here and the caller closes the segment instead of
    // slicing into unproven continuity.
    let appended_text = &current[overlap..];
    let expected = format!(
        "{}{}",
        tail_bytes(
            previous,
            MAX_LIVE_REASONING_WINDOW_BYTES - appended_text.len()
        ),
        appended_text
    );
    if expected != current {
        return None;
    }
    Some(overlap)
}

/// The runtime's UTF-8-safe tail window (`tail_window` in
/// `gents::streaming`): the last `max_bytes` bytes of `value`, advanced to
/// the nearest character boundary when the cut lands mid-character.
/// Duplicated here because the rollover proof must run the exact transform
/// the runtime applies; the two implementations must move together.
fn tail_bytes(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut start = value.len() - max_bytes;
    while !value.is_char_boundary(start) {
        start += 1;
    }
    &value[start..]
}

/// The UTF-8-safe length of the longest suffix of `previous` that is a
/// prefix of `current` (linear KMP over the combined byte string; the
/// sentinel byte cannot occur in either UTF-8 text). Used to advance through
/// a rolling bounded preview without re-streaming already-delivered bytes.
fn suffix_prefix_overlap(previous: &str, current: &str) -> usize {
    if previous.is_empty() || current.is_empty() {
        return 0;
    }
    let mut combined = Vec::with_capacity(current.len() + 1 + previous.len());
    combined.extend_from_slice(current.as_bytes());
    combined.push(0xff);
    combined.extend_from_slice(previous.as_bytes());
    let mut prefix = vec![0usize; combined.len()];
    for index in 1..combined.len() {
        let mut candidate = prefix[index - 1];
        while candidate > 0 && combined[index] != combined[candidate] {
            candidate = prefix[candidate - 1];
        }
        if combined[index] == combined[candidate] {
            candidate += 1;
        }
        prefix[index] = candidate.min(current.len());
    }
    prefix.last().copied().unwrap_or_default()
}

/// Request-local dedup cursor for one live turn's projection poll.
///
/// One cursor serves exactly one (session id, prompt id) turn: it is created
/// when the turn's watch loop starts and dropped when the turn resolves, so
/// it is never shared across prompts and never outlives its request. It
/// tracks the last-sent durable identity of every projection event family:
///
/// - tool calls: the last-sent base payload per `toolCallId`; a later
///   change to a tracked field emits a `tool_call_update` with exactly the
///   changed fields;
/// - available commands: the last-sent tool-list fingerprint;
/// - subagents: the last-sent payload fingerprint per
///   `<sessionUpdate kind>:<subagentId>`;
/// - live tails: one [`LiveSegmentCursor`] per stream (content, reasoning);
/// - durable message chunks: a delivered-length state per
///   `(message_key, update kind, ordinal)` chunk key, so an upserted/grown
///   row re-projects and emits only the newly proven suffix.
///
/// The poll computes novel events against *shadow copies* of the live and
/// durable chunk states (never mutating the cursor); each shadow advance is
/// carried inside its [`CursorAdvance`] and the caller records it only after
/// the corresponding send succeeded, so a send failure replays the identical
/// candidates on the next poll instead of dropping or duplicating them.
#[derive(Debug, Default)]
pub(crate) struct RequestCursor {
    /// Validated, request-local response history cache. Observation advances
    /// independently of outbound delivery; the per-rail live cursors below
    /// remain the send-success anchors into this retained chain.
    history_observation: messages::HistoryObservation,
    /// Response document generation whose live delivery cursors are active.
    delivered_response_doc: Option<String>,
    /// Last-sent base payload per tool call id.
    tool_bases: BTreeMap<String, Value>,
    /// Last delivered terminal status update per tool call.
    tool_terminal_states: BTreeMap<String, String>,
    /// Last-sent visible tool list fingerprint.
    commands_state: Option<u64>,
    /// Last-sent payload fingerprint per subagent key.
    subagent_states: BTreeMap<String, u64>,
    /// Delivery receipts only; process lifecycle remains in AgentToolCall.
    background_task_events: std::collections::BTreeSet<String>,
    background_outputs: BTreeMap<String, u64>,
    child_outputs: BTreeMap<String, child_output::OutputReceipt>,
    /// The committed (send-success) state of the live tail cursors.
    live_cursors: LiveCursorPair,
    /// The committed (send-success) delivered length per durable chunk key.
    /// The durable pass plans against a per-poll shadow of this map; an
    /// advance is promoted into it only through `record` after the
    /// corresponding send succeeded, so a failed send replans the identical
    /// durable candidate on the next poll.
    durable_chunks: BTreeMap<String, DurableChunkState>,
    /// Last transcript sequence whose complete projection batch succeeded.
    /// Queries include this row because the current assistant row may grow.
    message_sequence_high_water: Option<i64>,
    /// Timestamp evidence is observation state, not delivery state. Retain
    /// it across incremental pages so a growing current assistant row keeps
    /// the same start derived from its preceding tool-result/input row.
    timing_response_doc: Option<String>,
    response_started_at_ms: Option<i64>,
    response_ended_at_ms: Option<i64>,
    message_timestamps: BTreeMap<i64, (String, Option<i64>)>,
}

/// The rail one row's live evidence streamed on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum EvidenceRail {
    Content,
    Reasoning,
}

fn known_response_changed(observed: Option<&str>, delivered: Option<&str>) -> bool {
    observed.is_some_and(|identity| Some(identity) != delivered)
}

impl RequestCursor {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn observe_timestamps(&mut self, messages: &messages::MessageProjection) {
        // Unprovable history is unknown, not evidence of a replacement.
        // Keep timestamps whose rows the incremental high water has passed.
        if messages.response_doc_id.is_none() {
            return;
        }
        self.response_ended_at_ms = messages.response_ended_at_ms;
        if self.timing_response_doc != messages.response_doc_id {
            self.timing_response_doc = messages.response_doc_id.clone();
            self.response_started_at_ms = messages.response_started_at_ms;
            self.message_timestamps.clear();
        } else if self.response_started_at_ms.is_none() {
            self.response_started_at_ms = messages.response_started_at_ms;
        }
        for row in &messages.timeline {
            let entry = self
                .message_timestamps
                .entry(row.sequence)
                .or_insert_with(|| (row.message_key.clone(), row.timestamp_ms));
            if entry.0 == row.message_key {
                entry.1 = match (entry.1, row.timestamp_ms) {
                    (Some(old), Some(new)) => Some(old.min(new)),
                    (old, new) => old.or(new),
                };
            }
        }
    }

    fn timing_for_segment(
        &self,
        segment_key: String,
        chronology: Option<i64>,
    ) -> ProjectionEventTiming {
        let stream_start_candidate_ms = chronology
            .and_then(|sequence| {
                self.message_timestamps
                    .range(..sequence)
                    .rev()
                    .find_map(|(_, (_, timestamp))| *timestamp)
            })
            .or_else(|| {
                chronology
                    .is_none()
                    .then(|| {
                        self.message_timestamps
                            .iter()
                            .rev()
                            .find_map(|(_, (_, timestamp))| *timestamp)
                    })
                    .flatten()
            })
            .or(self.response_started_at_ms);
        let agent_timestamp_candidate_ms = chronology
            .and_then(|sequence| {
                self.message_timestamps
                    .get(&sequence)
                    .and_then(|(_, timestamp)| *timestamp)
            })
            .or(self.response_ended_at_ms);
        ProjectionEventTiming {
            segment_key,
            stream_start_candidate_ms,
            agent_timestamp_candidate_ms,
        }
    }

    /// The novel event for one tool call's base payload: the full
    /// `tool_call` registration on first observation, or a
    /// `tool_call_update` carrying exactly the tracked fields that changed
    /// since the last sent base. `None` means nothing tracked changed.
    fn tool_base_novel(
        &mut self,
        tool_call_id: &str,
        payload: &Value,
    ) -> Option<(Value, CursorAdvance)> {
        let advance = CursorAdvance::ToolBase {
            tool_call_id: tool_call_id.to_string(),
            payload: payload.clone(),
        };
        match self.tool_bases.get(tool_call_id) {
            None => Some((payload.clone(), advance)),
            Some(last_sent) => {
                let mut fields = changed_tool_fields(last_sent, payload)?;
                // Lifecycle completion has its own status-only event and
                // send-success cursor. A content refinement must not consume
                // that event if delivery stops between the two sends.
                if matches!(
                    payload.get("status").and_then(Value::as_str),
                    Some("completed" | "failed")
                ) {
                    fields.as_object_mut()?.remove("status");
                    if fields.as_object()?.is_empty() {
                        return None;
                    }
                }
                Some((
                    tools::tool_call_update_payload(tool_call_id, &fields),
                    advance,
                ))
            }
        }
    }

    /// Whether the visible tool list is novel.
    fn commands_changed(&mut self, fingerprint: u64) -> Option<CursorAdvance> {
        if self.commands_state == Some(fingerprint) {
            return None;
        }
        Some(CursorAdvance::Commands { fingerprint })
    }

    fn tool_terminal_novel(&self, tool_call_id: &str, status: &str) -> Option<CursorAdvance> {
        if status.is_empty()
            || self
                .tool_terminal_states
                .get(tool_call_id)
                .map(String::as_str)
                == Some(status)
        {
            return None;
        }
        Some(CursorAdvance::ToolTerminal {
            tool_call_id: tool_call_id.to_string(),
            status: status.to_string(),
        })
    }

    /// Whether the subagent payload is novel for its key.
    pub(super) fn subagent_spawn_was_delivered(&self, child_session_id: &str) -> bool {
        self.subagent_states
            .contains_key(&format!("subagent_spawned:{child_session_id}"))
    }

    fn subagent_changed(&mut self, key: &str, fingerprint: u64) -> Option<CursorAdvance> {
        if self.subagent_states.get(key) == Some(&fingerprint) {
            return None;
        }
        Some(CursorAdvance::Subagent {
            key: key.to_string(),
            fingerprint,
        })
    }

    fn background_task_novel(&self, key: &str) -> Option<CursorAdvance> {
        (!self.background_task_events.contains(key)).then(|| CursorAdvance::BackgroundTask {
            key: key.to_string(),
        })
    }

    /// Record one delivered event after its send succeeded.
    pub(crate) fn record(&mut self, advance: CursorAdvance) {
        match advance {
            CursorAdvance::Many(advances) => {
                for advance in advances {
                    self.record(advance);
                }
            }
            CursorAdvance::ResponseDocument { doc_id } => {
                if self.delivered_response_doc.as_deref() != Some(&doc_id) {
                    self.delivered_response_doc = Some(doc_id);
                    self.live_cursors = LiveCursorPair::default();
                }
            }
            CursorAdvance::ToolBase {
                tool_call_id,
                payload,
            } => {
                self.tool_bases.insert(tool_call_id, payload);
            }
            CursorAdvance::ToolTerminal {
                tool_call_id,
                status,
            } => {
                self.tool_terminal_states.insert(tool_call_id, status);
            }
            CursorAdvance::Commands { fingerprint } => {
                self.commands_state = Some(fingerprint);
            }
            CursorAdvance::Subagent { key, fingerprint } => {
                self.subagent_states.insert(key, fingerprint);
            }
            CursorAdvance::BackgroundTask { key } => {
                self.background_task_events.insert(key);
            }
            CursorAdvance::BackgroundOutput {
                key, fingerprint, ..
            } => {
                self.background_outputs.insert(key, fingerprint);
            }
            CursorAdvance::ChildOutput { key, receipt } => {
                self.child_outputs.insert(key, receipt);
            }
            CursorAdvance::LiveContent { plan, progress_seq } => {
                self.live_cursors.content.commit(plan, progress_seq);
            }
            CursorAdvance::LiveReasoning { plan, progress_seq } => {
                self.live_cursors.reasoning.commit(plan, progress_seq);
            }
            CursorAdvance::DurableChunk {
                message_key,
                sent_text,
            } => {
                self.durable_chunks
                    .entry(message_key)
                    .or_default()
                    .sent_text = sent_text;
            }
            CursorAdvance::MessageHighWater { sequence } => {
                self.message_sequence_high_water = Some(
                    self.message_sequence_high_water
                        .map_or(sequence, |high| high.max(sequence)),
                );
            }
        }
    }
}

/// A per durable chunk delivery state: how much of the chunk's logical
/// text has already been *successfully sent*.
///
/// A durable `AgentMessage` row can appear before the request
/// terminalizes and be upserted (same `message_key`/sequence, growing
/// content), so the cursor must never mark a row "seen forever" after its
/// first observation. Instead each chunk keeps the exact delivered length
/// of its text and a re-projection emits only the newly proven suffix.
#[derive(Clone, Debug, Default)]
struct DurableChunkState {
    /// Exact logical text of this chunk already successfully sent. Prefix
    /// equality, not length alone, proves a later observation is growth.
    sent_text: String,
}

/// The tool-call fields the live poll tracks for diffs. A change to any of
/// these emits a `tool_call_update` carrying exactly the changed fields; a
/// first observation emits the full `tool_call` registration.
const TRACKED_TOOL_FIELDS: [&str; 7] = [
    "title",
    "kind",
    "status",
    "content",
    "rawInput",
    "rawOutput",
    "_meta",
];

/// The tracked tool-call fields that differ between the last-sent base and
/// the freshly observed payload, as a JSON object for a `tool_call_update`.
/// `None` when nothing tracked changed.
fn changed_tool_fields(last_sent: &Value, observed: &Value) -> Option<Value> {
    let last = last_sent.as_object()?;
    let fresh = observed.as_object()?;
    let mut fields = Map::new();
    for key in TRACKED_TOOL_FIELDS {
        let fresh_value = fresh.get(key);
        if fresh_value != last.get(key) {
            match fresh_value {
                Some(value) => {
                    fields.insert(key.to_string(), value.clone());
                }
                None => {
                    fields.insert(key.to_string(), Value::Null);
                }
            }
        }
    }
    if fields.is_empty() {
        None
    } else {
        Some(Value::Object(fields))
    }
}

/// A stable fingerprint of one projection payload: order-insensitive over
/// JSON object keys (a serialized `serde_json::Value` iterates object keys
/// in sorted order, so two payloads that differ only in key insertion order
/// hash identically) while remaining sensitive to every value and array
/// order.
fn payload_fingerprint(payload: &Value) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hash_value(&mut hasher, payload);
    hasher.finish()
}

fn hash_value<H: Hasher>(hasher: &mut H, value: &Value) {
    match value {
        Value::Null => 0u8.hash(hasher),
        Value::Bool(value) => {
            1u8.hash(hasher);
            value.hash(hasher);
        }
        Value::Number(value) => {
            2u8.hash(hasher);
            value.to_string().hash(hasher);
        }
        Value::String(value) => {
            3u8.hash(hasher);
            value.hash(hasher);
        }
        Value::Array(values) => {
            4u8.hash(hasher);
            for value in values {
                hash_value(hasher, value);
            }
        }
        Value::Object(fields) => {
            5u8.hash(hasher);
            // serde_json preserves insertion order, so iterate sorted to
            // make the fingerprint insensitive to key order.
            for (key, value) in fields.iter().collect::<BTreeMap<_, _>>() {
                key.hash(hasher);
                hash_value(hasher, value);
            }
        }
    }
}

/// Resolve the bound model/context configuration for the shim from the bound
/// behavior's `AgentBehavior` and `InferenceProfile` documents.
///
/// `AgentBehavior` selects `model_name` and `backend_id`; `InferenceProfile`
/// owns the context window. `AgentSession` has no model or context-window
/// fields and is never consulted here. Failures are surfaced as errors instead
/// of being papered over with a synthetic catalog entry.
pub(crate) async fn resolve_bound_model_context(
    node: &EmbeddedNode,
    behavior_id: &str,
) -> Result<BoundModelContext> {
    let behavior = load_agent_behavior(node, behavior_id)
        .await
        .with_context(|| format!("loading AgentBehavior {behavior_id:?} for the Grok shim"))?
        .ok_or_else(|| {
            anyhow!(
                "Grok shim is bound to behavior {behavior_id:?}, but no AgentBehavior document \
                 with that behavior_id exists"
            )
        })?;
    let model_name = behavior
        .model_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            anyhow!(
                "Grok shim is bound to behavior {behavior_id:?}, but that behavior has no \
                 model_name set, so no Grok modelId can be projected"
            )
        })?;
    // The backend selection is still validated (a bound behavior without a
    // backend cannot serve) but stays internal: it is a Gents routing
    // detail and never leaks into the wire-facing model identity.
    behavior
        .backend_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow!(
                "Grok shim is bound to behavior {behavior_id:?}, but that behavior has no \
                 backend_id set, so no Grok modelId can be projected"
            )
        })?;
    let context_window = match behavior.inference_profile_id.as_deref().map(str::trim) {
        Some(profile_id) if !profile_id.is_empty() => {
            let profile = load_inference_profile(node, profile_id)
                .await
                .with_context(|| {
                    format!("loading InferenceProfile {profile_id:?} for the Grok shim")
                })?
                .ok_or_else(|| {
                    anyhow!(
                        "Grok shim is bound to behavior {behavior_id:?}, which references \
                         inference_profile_id {profile_id:?}, but no InferenceProfile document \
                         with that id exists"
                    )
                })?;
            profile
                .context_window
                .and_then(|value| u64::try_from(value.max(0)).ok())
                .unwrap_or(DEFAULT_CONTEXT_WINDOW_TOKENS)
        }
        _ => DEFAULT_CONTEXT_WINDOW_TOKENS,
    };

    // The pager addresses models by their `modelId` — the bound behavior's
    // `model_name` exactly. The `backend_id` stays internal: it is a Gents
    // routing detail and never leaks into the wire-facing model identity.
    Ok(BoundModelContext::new(
        model_name.clone(),
        model_name,
        context_window,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use gents::graphql::ensure_no_errors;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;

    #[test]
    fn unknown_response_identity_preserves_delivered_timing_evidence() {
        assert!(!known_response_changed(None, Some("old")));
        assert!(!known_response_changed(Some("old"), Some("old")));
        assert!(known_response_changed(Some("new"), Some("old")));
        assert!(known_response_changed(Some("first"), None));
        let mut cursor = RequestCursor::default();
        cursor.timing_response_doc = Some("old".into());
        cursor.response_started_at_ms = Some(1000);
        cursor.response_ended_at_ms = Some(2000);
        cursor
            .message_timestamps
            .insert(1, ("row".into(), Some(1500)));
        let mut observation = messages::MessageProjection {
            updates: vec![],
            chronology: vec![],
            update_keys: vec![],
            total_tokens: 0,
            terminal: false,
            stop_reason: None,
            context_window_tokens: 1000,
            live_tail: Default::default(),
            history: None,
            response_doc_id: None,
            message_sequence_high_water: Some(1),
            response_started_at_ms: None,
            response_ended_at_ms: None,
            timeline: vec![],
        };
        cursor.observe_timestamps(&observation);
        assert_eq!(cursor.timing_response_doc.as_deref(), Some("old"));
        assert_eq!(cursor.response_started_at_ms, Some(1000));
        assert_eq!(cursor.response_ended_at_ms, Some(2000));
        assert_eq!(cursor.message_timestamps[&1].1, Some(1500));
        observation.response_doc_id = Some("new".into());
        observation.response_started_at_ms = Some(3000);
        cursor.observe_timestamps(&observation);
        assert_eq!(cursor.timing_response_doc.as_deref(), Some("new"));
        assert_eq!(cursor.response_started_at_ms, Some(3000));
        assert_eq!(cursor.response_ended_at_ms, None);
        assert!(cursor.message_timestamps.is_empty());
    }

    #[test]
    fn repeated_prefixes_bind_only_when_transcript_order_proves_identity() {
        assert_eq!(
            unique_increasing_assignment(&[vec![0], vec![1, 2], vec![2]]),
            Some(vec![0, 1, 2])
        );
        assert_eq!(
            unique_increasing_assignment(&[vec![0, 1], vec![0, 1]]),
            Some(vec![0, 1])
        );
        assert_eq!(unique_increasing_assignment(&[vec![0, 1], vec![2]]), None);
        assert_eq!(unique_increasing_assignment(&[vec![1], vec![0]]), None);
        assert_eq!(unique_increasing_assignment(&[vec![]]), None);
    }

    #[test]
    fn ordered_assignment_matches_exhaustive_small_transcripts() {
        for first in 0u8..16 {
            for second in 0u8..16 {
                for third in 0u8..16 {
                    let candidates: Vec<Vec<usize>> = [first, second, third]
                        .iter()
                        .map(|mask| (0..4).filter(|index| mask & (1 << index) != 0).collect())
                        .collect();
                    let mut solutions = Vec::new();
                    for &a in &candidates[0] {
                        for &b in &candidates[1] {
                            for &c in &candidates[2] {
                                if a < b && b < c {
                                    solutions.push(vec![a, b, c]);
                                }
                            }
                        }
                    }
                    let expected = (solutions.len() == 1).then(|| solutions[0].clone());
                    assert_eq!(
                        unique_increasing_assignment(&candidates),
                        expected,
                        "{candidates:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn terminal_tail_uses_persisted_end_not_replay_arrival() {
        let mut cursor = RequestCursor::default();
        cursor.response_started_at_ms = Some(1000);
        cursor.response_ended_at_ms = Some(2500);
        let tail = cursor.timing_for_segment("tail".into(), None);
        assert_eq!(tail.stream_start_candidate_ms, Some(1000));
        assert_eq!(tail.agent_timestamp_candidate_ms, Some(2500));
        cursor
            .message_timestamps
            .insert(1, ("message".into(), Some(1500)));
        let durable = cursor.timing_for_segment("durable".into(), Some(1));
        assert_eq!(durable.agent_timestamp_candidate_ms, Some(1500));
        cursor.response_ended_at_ms = None;
        assert_eq!(
            cursor
                .timing_for_segment("live".into(), None)
                .agent_timestamp_candidate_ms,
            None
        );
    }

    #[test]
    fn background_task_receipts_advance_only_after_delivery() {
        let mut cursor = RequestCursor::default();
        let started = "task_backgrounded:call";
        let done = "task_completed:call";
        assert!(cursor.background_task_novel(started).is_some());
        // A planned but failed send remains retryable.
        let retry = cursor.background_task_novel(started).unwrap();
        cursor.record(retry);
        assert!(cursor.background_task_novel(started).is_none());
        let completion = cursor.background_task_novel(done).unwrap();
        assert!(cursor.background_task_novel(done).is_some());
        cursor.record(completion);
        assert!(cursor.background_task_novel(done).is_none());
        assert!(cursor
            .background_task_novel("task_backgrounded:other")
            .is_some());
    }

    /// A deterministic sender that records wire-enqueue order and can delay
    /// or fail sends: exactly the shape a closed/failing live outbound has.
    struct RecordingSender {
        lines: StdMutex<Vec<String>>,
        first_send_delay: tokio::sync::Notify,
        delay_armed: AtomicBool,
        fail_all: AtomicBool,
        /// Completed (not merely attempted) sends. Only incremented after a
        /// send finished enqueueing or failing.
        sends: AtomicUsize,
        /// Sends that have parked inside their delay. Incremented *before*
        /// the send awaits the release notification, so a test can wait on
        /// it without deadlocking against the parked send itself.
        parked: AtomicUsize,
    }

    impl RecordingSender {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                lines: StdMutex::new(Vec::new()),
                first_send_delay: tokio::sync::Notify::new(),
                delay_armed: AtomicBool::new(false),
                fail_all: AtomicBool::new(false),
                sends: AtomicUsize::new(0),
                parked: AtomicUsize::new(0),
            })
        }

        fn recorded_lines(&self) -> Vec<String> {
            self.lines.lock().expect("lines").clone()
        }
    }

    impl AsyncSendLine for RecordingSender {
        async fn send_line(&self, line: String) -> Result<()> {
            if self.delay_armed.swap(false, Ordering::SeqCst) {
                // The first send parks until the test releases it, so a
                // racing second send deterministically arrives while the
                // first still holds the session's send lock. `parked` is
                // counted before the await so the test has an observable
                // "has parked" signal that the parked send itself cannot
                // miss.
                self.parked.fetch_add(1, Ordering::SeqCst);
                self.first_send_delay.notified().await;
            }
            self.sends.fetch_add(1, Ordering::SeqCst);
            if self.fail_all.load(Ordering::SeqCst) {
                anyhow::bail!("sender closed");
            }
            self.lines.lock().expect("lines").push(line);
            Ok(())
        }
    }

    /// A no-op payload builder: the notification body is irrelevant to the
    /// ordering assertions; the `_meta.eventId` is what the tests read.
    fn plain_update(event_id: &str, _total_tokens: u64) -> Result<Value> {
        Ok(json!({ "eventId": event_id }))
    }

    /// The pager's `NotificationMeta` read: `_meta.eventId` of one recorded
    /// line.
    fn recorded_event_ids(lines: &[String]) -> Vec<String> {
        lines
            .iter()
            .map(|line| {
                serde_json::from_str::<Value>(line)
                    .expect("recorded line is JSON")
                    .get("eventId")
                    .and_then(Value::as_str)
                    .expect("eventId")
                    .to_string()
            })
            .collect()
    }

    /// Gate 1/3: a deliberately delayed first same-session send and a
    /// racing second send. The wire enqueue order must still be the strictly
    /// increasing event-id allocation order — the second send cannot
    /// overtake the first even though the first parked inside its enqueue.
    #[tokio::test]
    async fn a_delayed_first_send_is_not_overtaken_by_a_racing_second_send() {
        let sequencer = Arc::new(ProjectionSequencer::new());
        let channel = Arc::new(SessionUpdateChannel::new(sequencer.clone()));
        let sender = RecordingSender::new();

        // Arm the delay, start the first send, and let it acquire the
        // session lock and park inside its enqueue.
        sender.delay_armed.store(true, Ordering::SeqCst);
        let first_sender = sender.clone();
        let first_channel = channel.clone();
        let first = tokio::spawn(async move {
            first_channel
                .send("s", plain_update, first_sender)
                .await
                .expect("first send")
        });
        // Yield until the first send has actually parked inside its enqueue;
        // this makes the race deterministic. Waiting on `parked` (not
        // `sends`) is what makes the wait sound: the parked send increments
        // it before awaiting, so the signal can never be lost to the very
        // delay the test is about to release.
        while sender.parked.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        // The racing second send must block on the session's send lock
        // until the first released it — it cannot enqueue before the first.
        let second_sender = sender.clone();
        let second_channel = channel.clone();
        let second = tokio::spawn(async move {
            second_channel
                .send("s", plain_update, second_sender)
                .await
                .expect("second send")
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Release the delayed first send; both sends now complete.
        sender.first_send_delay.notify_one();
        let (first_line, second_line) = tokio::join!(first, second);
        let ids = recorded_event_ids(&[first_line.unwrap(), second_line.unwrap()]);
        assert_eq!(
            ids,
            vec!["s-1".to_string(), "s-2".to_string()],
            "same-session wire enqueue order must equal allocation order"
        );
        assert_eq!(sequencer.event_counter("s"), 2);
    }

    /// Gate 3: two sessions both start at event id 1 and are *not*
    /// serialized behind one another — a parked send on session A does not
    /// block a concurrent send on session B.
    #[tokio::test]
    async fn two_sessions_start_at_one_and_stay_independently_concurrent() {
        let sequencer = Arc::new(ProjectionSequencer::new());
        let channel = Arc::new(SessionUpdateChannel::new(sequencer.clone()));
        let sender = RecordingSender::new();

        // Park session A's first send inside its enqueue.
        sender.delay_armed.store(true, Ordering::SeqCst);
        let sender_a = sender.clone();
        let parked_channel = channel.clone();
        let parked = tokio::spawn(async move {
            parked_channel
                .send("session-a", plain_update, sender_a)
                .await
                .expect("parked send")
        });
        // Wait until session A's first send has actually parked inside its
        // enqueue (see the `parked` counter rationale above).
        while sender.parked.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        // Session B sends concurrently and must complete *without waiting*
        // for session A's parked send.
        let sender_b = sender.clone();
        let concurrent_channel = channel.clone();
        let concurrent_handle = tokio::spawn(async move {
            concurrent_channel
                .send("session-b", plain_update, sender_b)
                .await
                .expect("concurrent send")
        });
        let concurrent = tokio::time::timeout(std::time::Duration::from_secs(5), concurrent_handle)
            .await
            .expect("session B must not block behind session A's parked send")
            .expect("join");

        // Both sessions started at 1: per-session counters, never shared.
        // Release session A's parked send and collect its line: the
        // assertion that matters already passed above — session B completed
        // while A was still parked.
        sender.first_send_delay.notify_one();
        let parked_line = parked.await.expect("join");
        assert_eq!(
            recorded_event_ids(&[parked_line, concurrent]),
            vec!["session-a-1".to_string(), "session-b-1".to_string()]
        );
        assert_eq!(sequencer.event_counter("session-a"), 1);
        assert_eq!(sequencer.event_counter("session-b"), 1);
    }

    /// Gate 2: a deterministic failing sender. A failed send consumes no
    /// event id, and the following successful send receives the expected
    /// next id.
    #[tokio::test]
    async fn a_failed_send_consumes_no_event_id_and_the_next_send_gets_the_expected_id() {
        let sequencer = Arc::new(ProjectionSequencer::new());
        let channel = SessionUpdateChannel::new(sequencer.clone());
        let sender = RecordingSender::new();

        // The sender fails: the send returns an error and nothing is
        // enqueued.
        sender.fail_all.store(true, Ordering::SeqCst);
        let failure = channel
            .send("s", plain_update, sender.clone())
            .await
            .expect_err("the closed sender must fail the send");
        assert!(failure.to_string().contains("sender closed"));
        assert!(sender.recorded_lines().is_empty());
        assert_eq!(
            sequencer.event_counter("s"),
            0,
            "a failed send must consume no event id"
        );

        // Recover the sender: the next successful send receives the
        // immediately expected next id — the failed reservation rolled back.
        sender.fail_all.store(false, Ordering::SeqCst);
        let recovered = channel
            .send("s", plain_update, sender.clone())
            .await
            .expect("the recovered send must succeed");
        assert_eq!(
            recorded_event_ids(&[recovered]),
            vec!["s-1".to_string()],
            "the next successful send must receive the expected next id"
        );
        assert_eq!(sequencer.event_counter("s"), 1);
    }

    /// Gate 2 (state-commit hook): the commit hook runs only after a
    /// successful enqueue — a failed send leaves the recorded state
    /// untouched and the counter at zero. The hook is infallible (it only
    /// records connection-local state), and the reservation is committed
    /// before the hook runs, so an already-delivered id is never reused.
    #[tokio::test]
    async fn a_failed_send_skips_the_state_commit_hook() {
        struct RecordingCommit {
            committed: AtomicUsize,
        }
        impl AsyncCommit for RecordingCommit {
            async fn commit(&self) {
                self.committed.fetch_add(1, Ordering::SeqCst);
            }
        }

        let sequencer = Arc::new(ProjectionSequencer::new());
        let channel = SessionUpdateChannel::new(sequencer.clone());
        let sender = RecordingSender::new();
        let commit = RecordingCommit {
            committed: AtomicUsize::new(0),
        };

        sender.fail_all.store(true, Ordering::SeqCst);
        channel
            .send_with_commit("s", plain_update, sender.clone(), &commit)
            .await
            .expect_err("the closed sender must fail the send");
        assert_eq!(
            commit.committed.load(Ordering::SeqCst),
            0,
            "a failed send must not commit state"
        );
        assert_eq!(sequencer.event_counter("s"), 0);

        sender.fail_all.store(false, Ordering::SeqCst);
        channel
            .send_with_commit("s", plain_update, sender, &commit)
            .await
            .expect("the recovered send must succeed");
        assert_eq!(
            commit.committed.load(Ordering::SeqCst),
            1,
            "a successful send commits the state exactly once"
        );
        assert_eq!(sequencer.event_counter("s"), 1);
    }

    /// Context observation is independent of transport delivery. A failed
    /// send rolls back its event ID; re-observing the same inference sample
    /// neither adds usage nor changes context, and the retry stamps it once.
    #[tokio::test]
    async fn a_failed_send_rolls_back_the_id_but_never_double_counts_tokens() {
        let sequencer = Arc::new(ProjectionSequencer::new());
        let channel = SessionUpdateChannel::new(sequencer.clone());
        let sender = RecordingSender::new();

        // The projection pass observed 100 tokens for the request and
        // applied the delta to the session total at poll time.
        sequencer.observe_context("s", test_context(1, 100));
        assert_eq!(sequencer.session_total_tokens("s"), 100);

        // The send fails: the id rolls back, nothing is enqueued.
        sender.fail_all.store(true, Ordering::SeqCst);
        channel
            .send("s", plain_update, sender.clone())
            .await
            .expect_err("the closed sender must fail the send");
        assert_eq!(sequencer.event_counter("s"), 0);

        // The next poll re-observes the same generation and value.
        sequencer.observe_context("s", test_context(1, 100));
        assert_eq!(sequencer.session_total_tokens("s"), 100);

        // The recovery send stamps the recorded cumulative total (100) with
        // the rolled-back id, in one coherent notification.
        sender.fail_all.store(false, Ordering::SeqCst);
        let recovered = channel
            .send("s", plain_update, sender)
            .await
            .expect("the recovered send must succeed");
        let recovered: Value = serde_json::from_str(&recovered).expect("line is JSON");
        assert_eq!(recovered["eventId"], "s-1");
        assert_eq!(
            sequencer.session_total_tokens("s"),
            100,
            "the recovery send must not re-add the observed delta"
        );
        assert_eq!(sequencer.event_counter("s"), 1);
    }

    #[test]
    fn event_ids_are_session_keyed_and_monotonic() {
        let sequencer = Arc::new(ProjectionSequencer::new());
        let first = ProjectionSequencer::reserve_event_id(&sequencer, "session-1");
        assert_eq!(first.event_id(), "session-1-1");
        let second = ProjectionSequencer::reserve_event_id(&sequencer, "session-1");
        assert_eq!(second.event_id(), "session-1-2");
        second.commit();
        first.commit();
        // A different session starts at 1: counters are per session, never
        // connection-wide.
        let third = ProjectionSequencer::reserve_event_id(&sequencer, "session-2");
        assert_eq!(third.event_id(), "session-2-1");
        third.commit();
        assert_eq!(sequencer.event_counter("session-1"), 2);
        assert_eq!(sequencer.event_counter("session-2"), 1);
    }

    #[test]
    fn a_fresh_sequencer_allocates_no_ids_or_tokens() {
        let sequencer = ProjectionSequencer::new();
        assert_eq!(sequencer.event_counter("s"), 0);
        assert_eq!(sequencer.session_total_tokens("s"), 0);
    }

    #[test]
    fn a_failed_send_rolls_back_the_uncommitted_event_id() {
        let sequencer = Arc::new(ProjectionSequencer::new());
        // Simulate a failed send: reserve, never commit, drop.
        {
            let reservation = ProjectionSequencer::reserve_event_id(&sequencer, "s");
            assert_eq!(reservation.event_id(), "s-1");
            // Dropped without commit: the send failed.
        }
        assert_eq!(sequencer.event_counter("s"), 0, "the id must roll back");
        // The next successful send reuses the rolled-back id.
        let next = ProjectionSequencer::reserve_event_id(&sequencer, "s");
        assert_eq!(next.event_id(), "s-1");
        next.commit();
        assert_eq!(sequencer.event_counter("s"), 1);
    }

    #[test]
    fn an_uncommitted_reservation_leaves_a_gap_when_a_later_id_committed() {
        let sequencer = Arc::new(ProjectionSequencer::new());
        let first = ProjectionSequencer::reserve_event_id(&sequencer, "s");
        let second = ProjectionSequencer::reserve_event_id(&sequencer, "s");
        // The later id sends successfully first; the earlier reservation
        // then fails: it cannot un-allocate the committed id, so it leaves a
        // gap instead.
        second.commit();
        drop(first);
        assert_eq!(sequencer.event_counter("s"), 2);
        let third = ProjectionSequencer::reserve_event_id(&sequencer, "s");
        assert_eq!(third.event_id(), "s-3");
        third.commit();
        assert_eq!(sequencer.event_counter("s"), 3);
    }

    fn test_context(generation: i64, used: u64) -> context::ContextSample {
        context::ContextSample {
            order: (
                chrono::DateTime::from_timestamp(1_700_000_000 + generation, 0).unwrap(),
                generation,
                format!("call-{generation}"),
            ),
            used,
        }
    }

    #[test]
    fn context_observations_replace_spend_and_reject_old_requests() {
        let sequencer = ProjectionSequencer::new();
        sequencer.observe_context("s", test_context(1, 900));
        sequencer.observe_context("s", test_context(1, 900));
        assert_eq!(sequencer.session_total_tokens("s"), 900);
        sequencer.observe_context("s", test_context(2, 300));
        assert_eq!(
            sequencer.session_total_tokens("s"),
            300,
            "compaction can reduce current context"
        );
        sequencer.observe_context("s", test_context(1, 950));
        assert_eq!(
            sequencer.session_total_tokens("s"),
            300,
            "old background polling cannot restore old context"
        );
        sequencer.observe_context("s", test_context(2, 350));
        sequencer.observe_context("s", test_context(2, 300));
        assert_eq!(
            sequencer.session_total_tokens("s"),
            350,
            "same-call stale usage cannot retract completed output"
        );
        sequencer.observe_context("other", test_context(3, 100));
        assert_eq!(sequencer.session_total_tokens("s"), 350);
        assert_eq!(sequencer.session_total_tokens("other"), 100);
    }

    #[test]
    fn observed_context_is_not_clamped_to_hide_budget_overflow() {
        let sequencer = ProjectionSequencer::new();
        sequencer.observe_context("s", test_context(1, 1_500));
        assert_eq!(sequencer.session_total_tokens("s"), 1_500);
    }

    #[test]
    fn stamp_update_meta_carries_event_tokens_prompt_and_replay_keys() {
        let fresh = stamp_update_meta(
            "s-1",
            64,
            Some("prompt-9"),
            None,
            UpdateTimestamps {
                agent_timestamp_ms: Some(1_700_000_003_200),
                stream_start_ms: Some(1_700_000_000_000),
                turn_start_ms: Some(1_699_999_999_000),
            },
        );
        assert_eq!(fresh["eventId"], "s-1");
        assert_eq!(fresh["totalTokens"], 64);
        assert_eq!(fresh["promptId"], "prompt-9");
        assert_eq!(fresh["agentTimestampMs"], 1_700_000_003_200i64);
        assert_eq!(fresh["streamStartMs"], 1_700_000_000_000i64);
        assert_eq!(fresh["turnStartMs"], 1_699_999_999_000i64);
        assert!(
            fresh.get("isReplay").is_none(),
            "fresh updates omit the key"
        );

        let replay = stamp_update_meta("s-2", 64, None, Some(true), UpdateTimestamps::default());
        assert_eq!(replay["isReplay"], true);
        assert!(replay.get("promptId").is_none());

        let echo = stamp_update_meta(
            "s-3",
            0,
            Some("prompt-1"),
            Some(false),
            UpdateTimestamps::default(),
        );
        assert_eq!(echo["isReplay"], false);
        assert_eq!(echo["promptId"], "prompt-1");
        assert_eq!(echo["totalTokens"], 0);
    }

    #[test]
    fn session_update_notification_wraps_payload_with_session_and_meta() {
        let meta = stamp_update_meta(
            "session-1-1",
            64,
            Some("prompt-1"),
            None,
            UpdateTimestamps::default(),
        );
        let notification = session_update_notification(
            "session-1",
            json!({
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": "hi"},
            }),
            meta,
        );
        assert_eq!(notification["jsonrpc"], "2.0");
        assert_eq!(notification["method"], "session/update");
        assert_eq!(notification["params"]["sessionId"], "session-1");
        assert_eq!(
            notification["params"]["update"]["sessionUpdate"],
            "agent_message_chunk"
        );
        // The Grok decoder expects the chunk field name `content`.
        assert_eq!(notification["params"]["update"]["content"]["text"], "hi");
        assert_eq!(notification["params"]["_meta"]["promptId"], "prompt-1");
        assert_eq!(notification["params"]["_meta"]["eventId"], "session-1-1");
        assert_eq!(notification["params"]["_meta"]["totalTokens"], 64);
        let extension = session_notification_for_method(
            SUBAGENT_NOTIFICATION_METHOD,
            "session-1",
            json!({"sessionUpdate": "subagent_spawned"}),
            json!({}),
        );
        assert_eq!(
            extension["method"], "_x.ai/session_notification",
            "ACP SDK requires the extension marker on serialized methods"
        );
    }

    #[test]
    fn bound_context_window_falls_back_to_runtime_default() {
        let zeroed = BoundModelContext::new("b::m".to_string(), "m".to_string(), 0);
        assert_eq!(
            zeroed.effective_context_window(),
            gents::DEFAULT_CONTEXT_WINDOW as u64,
            "the Grok fallback must come from the runtime's single context-window default"
        );
        let pinned = BoundModelContext::new("b::m".to_string(), "m".to_string(), 8_192);
        assert_eq!(pinned.effective_context_window(), 8_192);
    }

    #[test]
    fn bound_model_context_keeps_model_id_and_display_name() {
        let bound = BoundModelContext::new(
            "GLM-5.3-NVFP4".to_string(),
            "GLM-5.3-NVFP4".to_string(),
            262_144,
        );
        assert_eq!(bound.model_id, "GLM-5.3-NVFP4");
        assert_eq!(bound.model_name, "GLM-5.3-NVFP4");
        assert_eq!(bound.total_context_tokens, 262_144);
    }

    #[tokio::test]
    async fn resolve_bound_model_context_projects_the_production_style_catalog() {
        // The production-style bound context: a workstation backend whose
        // behavior selects `GLM-5.3-NVFP4`, pinned to the pack profile with
        // a 262144-token context window. The pager addresses the model by
        // its `model_name` exactly — the backend id never leaks into the
        // wire-facing `modelId`.
        let dir = tempfile::tempdir().expect("tempdir");
        let node = Arc::new(
            defra_node::EmbeddedNode::builder()
                // The staging `TempDir` guard stays in scope (`dir`) for the
                // test's lifetime, so the node's storage directory is
                // deleted when the test ends — never abandoned with
                // `keep()` or leaked with `mem::forget`.
                .data_path(dir.path().join("node"))
                .with_storage_backend(gents::defra_node::StorageBackend::Regolith)
                .build()
                .await
                .expect("embedded node"),
        );
        gents::schema::ensure_runtime_schemas(node.as_ref())
            .await
            .expect("runtime schemas");

        let seed = r#"mutation {
            create_InferenceBackend(input: {
                backend_id: "grok-port-backend-ws1",
                name: "workstation-1",
                endpoint: "http://127.0.0.1:8000/v1",
                max_concurrent: 16,
                max_queue_depth: 64,
                enabled: true
            }) { _docID }
            create_InferenceProfile(input: {
                profile_id: "grok-port-profile",
                display_name: "Grok TUI port profile",
                context_window: 262144
            }) { _docID }
            create_AgentBehavior(input: {
                behavior_id: "port-live",
                agent_did: "did:key:zGrokTuiPortAgentPlaceholder00000000000000000000000",
                display_name: "Live GLM probes through the Grok wire",
                backend_id: "grok-port-backend-ws1",
                model_name: "GLM-5.3-NVFP4",
                inference_profile_id: "grok-port-profile",
                enabled: true
            }) { _docID }
        }"#
        .to_string();
        let response = node.execute(&seed).await;
        assert!(!response.has_errors(), "seed failed: {:?}", response.errors);

        let bound = resolve_bound_model_context(node.as_ref(), "port-live")
            .await
            .expect("bound model context");
        assert_eq!(bound.model_id, "GLM-5.3-NVFP4");
        assert_eq!(bound.model_name, "GLM-5.3-NVFP4");
        assert_eq!(bound.total_context_tokens, 262_144);
        assert_eq!(bound.effective_context_window(), 262_144);
    }

    #[tokio::test]
    async fn resolve_bound_model_context_without_profile_uses_runtime_default() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node = Arc::new(
            defra_node::EmbeddedNode::builder()
                .data_path(dir.path().join("node"))
                .with_storage_backend(gents::defra_node::StorageBackend::Regolith)
                .build()
                .await
                .expect("embedded node"),
        );
        gents::schema::ensure_runtime_schemas(node.as_ref())
            .await
            .expect("runtime schemas");

        let seed = r#"mutation {
            create_AgentBehavior(input: {
                behavior_id: "no-profile"
                agent_did: "did:key:zGrokNoProfilePlaceholder000000000000000000000000"
                display_name: "Grok behavior without an inference profile"
                backend_id: "local-backend"
                model_name: "local-model"
                enabled: true
            }) { _docID }
        }"#;
        let response = node.execute(seed).await;
        assert!(!response.has_errors(), "seed failed: {:?}", response.errors);

        let bound = resolve_bound_model_context(node.as_ref(), "no-profile")
            .await
            .expect("bound model context");
        assert_eq!(bound.model_id, "local-model");
        assert_eq!(
            bound.total_context_tokens,
            gents::DEFAULT_CONTEXT_WINDOW as u64
        );
        assert_eq!(
            bound.effective_context_window(),
            gents::DEFAULT_CONTEXT_WINDOW as u64
        );
    }

    /// One durable assistant row carrying both a reasoning thought and body
    /// text streams as two chunks, and a cursor that only recorded the
    /// thought (its send failed) still emits the text on the next poll —
    /// the chunk-level identity is what makes the retry recover the second
    /// chunk instead of dropping it with the row.
    #[tokio::test]
    async fn chunk_level_identity_recovers_a_partial_row_retry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node = Arc::new(
            EmbeddedNode::builder()
                // The staging `TempDir` guard stays in scope (`dir`) for the
                // test's lifetime, so the node's storage directory is deleted
                // when the test ends — never abandoned with `keep()` or
                // leaked with `mem::forget`.
                .data_path(dir.path().join("node"))
                .with_storage_backend(gents::defra_node::StorageBackend::Regolith)
                .build()
                .await
                .expect("embedded node"),
        );
        gents::schema::ensure_runtime_schemas(node.as_ref())
            .await
            .expect("runtime schemas");

        let request_id = "req-chunk-retry";
        let message = serde_json::to_string(&gents_protocol::message::Message::Assistant {
            id: None,
            content: vec![
                gents_protocol::message::AssistantContent::Reasoning(
                    gents_protocol::message::Reasoning::new("thinking"),
                ),
                gents_protocol::message::AssistantContent::text("answer"),
            ],
        })
        .expect("serialize assistant message");
        let escaped_message = gents::graphql::escape_graphql_string(&message);
        let escaped_request = gents::graphql::escape_graphql_string(request_id);
        let seed = format!(
            r#"mutation {{
                create_AgentMessage(input: {{
                    message_key: "{escaped_request}:1"
                    session_id: "s-chunk"
                    agent_did: "did:test:grok-shim"
                    requester_did: "did:test:grok-shim"
                    request_id: "{escaped_request}"
                    sequence: 1
                    role: "assistant"
                    content: "{escaped_message}"
                }}) {{ _docID }}
            }}"#
        );
        let response = node.execute(&seed).await;
        assert!(!response.has_errors(), "seed failed: {:?}", response.errors);

        let engine = ProjectionEngine::new(
            node,
            BoundModelContext::new(
                "GLM-5.3-NVFP4".to_string(),
                "GLM-5.3-NVFP4".to_string(),
                262_144,
            ),
        );
        let mut cursor = RequestCursor::new();

        // First poll: both chunks of the row are novel.
        let first = engine
            .project_request_updates("s-chunk", request_id, &mut cursor)
            .await
            .expect("first poll");
        assert_eq!(first.len(), 2, "thought plus text both stream");
        let kinds: Vec<&str> = first
            .iter()
            .map(|event| {
                event.payload["sessionUpdate"]
                    .as_str()
                    .expect("sessionUpdate kind")
            })
            .collect();
        assert_eq!(kinds, vec!["agent_thought_chunk", "agent_message_chunk"]);

        // Simulate a partial send failure: only the thought's send
        // succeeded, so only its advance is recorded. The text chunk's
        // identity stays unseen and must be re-emitted by the next poll.
        cursor.record(first[0].advance.clone());
        let second = engine
            .project_request_updates("s-chunk", request_id, &mut cursor)
            .await
            .expect("second poll");
        assert_eq!(
            second.len(),
            1,
            "only the unsent text chunk re-emits; the delivered thought does not duplicate"
        );
        assert_eq!(
            second[0].payload["sessionUpdate"], "agent_message_chunk",
            "the retry recovers the text chunk, not the thought"
        );

        // After the retry's send succeeds, a third poll emits nothing.
        cursor.record(second[0].advance.clone());
        let third = engine
            .project_request_updates("s-chunk", request_id, &mut cursor)
            .await
            .expect("third poll");
        assert!(third.is_empty(), "every chunk is now delivered");
    }

    /// Seed the embedded node with runtime schemas and start a projection
    /// engine, the production shape every embedded chronology test uses.
    async fn embedded_engine() -> (tempfile::TempDir, Arc<ProjectionEngine>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let node = Arc::new(
            EmbeddedNode::builder()
                // The staging `TempDir` guard stays in scope (`dir`) for the
                // test's lifetime, so the node's storage directory is deleted
                // when the test ends — never abandoned with `keep()` or
                // leaked with `mem::forget`.
                .data_path(dir.path().join("node"))
                .with_storage_backend(gents::defra_node::StorageBackend::Regolith)
                .build()
                .await
                .expect("embedded node"),
        );
        gents::schema::ensure_runtime_schemas(node.as_ref())
            .await
            .expect("runtime schemas");
        let engine = ProjectionEngine::new(
            node,
            BoundModelContext::new(
                "GLM-5.3-NVFP4".to_string(),
                "GLM-5.3-NVFP4".to_string(),
                262_144,
            ),
        );
        (dir, Arc::new(engine))
    }

    /// Seed one durable `AgentToolCall` row with an explicit stable id and
    /// transcript sequence.
    async fn seed_tool_call_row(
        engine: &ProjectionEngine,
        session_id: &str,
        request_id: &str,
        request_doc_id: Option<&str>,
        tool_call_id: &str,
        tool_name: &str,
        message_sequence: i64,
        child_request_id: Option<&str>,
    ) -> String {
        let escaped_session = gents::graphql::escape_graphql_string(session_id);
        let escaped_request = gents::graphql::escape_graphql_string(request_id);
        let escaped_id = gents::graphql::escape_graphql_string(tool_call_id);
        let escaped_name = gents::graphql::escape_graphql_string(tool_name);
        let request_doc_field = request_doc_id
            .map(|id| {
                format!(
                    r#"request_doc_id: "{}""#,
                    gents::graphql::escape_graphql_string(id)
                )
            })
            .unwrap_or_default();
        let child_field = child_request_id
            .map(|id| {
                format!(
                    r#"child_request_id: "{}""#,
                    gents::graphql::escape_graphql_string(id)
                )
            })
            .unwrap_or_else(|| r#"child_request_id: """#.to_string());
        let mutation = format!(
            r#"mutation {{
                tool: create_AgentToolCall(input: {{
                    tool_call_key: "{escaped_session}:{escaped_id}"
                    request_id: "{escaped_request}"
                    {request_doc_field}
                    session_id: "{escaped_session}"
                    agent_did: "did:test:grok-shim"
                    requester_did: "did:test:grok-shim"
                    tool_call_id: "{escaped_id}"
                    tool_name: "{escaped_name}"
                    lifecycle_state: "completed"
                    result: "done"
                    message_sequence: {message_sequence}
                    {child_field}
                }}) {{ _docID }}
            }}"#
        );
        let response = engine.node.execute(&mutation).await;
        assert!(
            !response.has_errors(),
            "seed tool call failed: {:?}",
            response.errors
        );
        response
            .data
            .as_ref()
            .and_then(|data| data.pointer("/tool/0/_docID"))
            .and_then(Value::as_str)
            .expect("seeded tool call document id")
            .to_string()
    }

    /// Seed one runtime child `AgentRequest` row linked to the parent
    /// request, with an explicit equal-time `created_at`.
    async fn seed_child_request_row(
        engine: &ProjectionEngine,
        parent_request_id: &str,
        parent_request_doc_id: &str,
        parent_tool_call_id: &str,
        parent_tool_call_doc_id: &str,
        child_request_id: &str,
        created_at: &str,
    ) {
        let escaped_parent = gents::graphql::escape_graphql_string(parent_request_id);
        let escaped_parent_doc = gents::graphql::escape_graphql_string(parent_request_doc_id);
        let escaped_tool_call = gents::graphql::escape_graphql_string(parent_tool_call_id);
        let escaped_tool_doc = gents::graphql::escape_graphql_string(parent_tool_call_doc_id);
        let escaped_child = gents::graphql::escape_graphql_string(child_request_id);
        let escaped_created = gents::graphql::escape_graphql_string(created_at);
        let mutation = format!(
            r#"mutation {{
                create_AgentRequest(input: {{
                    request_id: "{escaped_child}"
                    agent_did: "did:test:grok-shim"
                    session_id: "s-chron-child"
                    caused_by_parent_request_id: "{escaped_parent}"
                    caused_by_parent_request_doc_id: "{escaped_parent_doc}"
                    caused_by_parent_tool_call_id: "{escaped_tool_call}"
                    caused_by_parent_tool_call_doc_id: "{escaped_tool_doc}"
                    content: "child work"
                    lifecycle_state: "processing"
                    backend_id: ""
                    execution_origin: "interactive"
                    failure_reason: ""
                    created_at: "{escaped_created}"
                    retry_count: 0
                    max_retries: 3
                }}) {{ _docID }}
            }}"#
        );
        let response = engine.node.execute(&mutation).await;
        assert!(
            !response.has_errors(),
            "seed child request failed: {:?}",
            response.errors
        );
    }

    /// The `sessionUpdate` kind of one novel event, for order assertions.
    fn update_kind(event: &NovelProjectionEvent) -> String {
        event.payload["sessionUpdate"]
            .as_str()
            .expect("sessionUpdate kind")
            .to_string()
    }

    /// The mixed durable chronology, through the production projection
    /// engine over an embedded node and runtime schemas:
    ///
    /// - one assistant `AgentMessage` row at sequence 3 (a reasoning thought
    ///   plus body text, streamed as two chunks);
    /// - two `AgentToolCall` rows at the *same* `message_sequence` 4, seeded
    ///   in reverse stable-identity order;
    /// - a child `AgentRequest` created by the spawn tool `call-a` (the
    ///   `call-a` row is a spawn row through its `child_request_id`), with an
    ///   `created_at` equal to nothing else deciding order — its position is
    ///   the spawn tool's sequence.
    ///
    /// The wire order must be exactly: thought, text, tool a, tool z,
    /// spawned, with the positionless `available_commands_update` last —
    /// and the same on a re-poll after a failed send, without duplicating
    /// the events whose sends succeeded.
    #[tokio::test]
    async fn persisted_context_hydrates_metadata_without_a_response_token_counter() {
        let (_dir, engine) = embedded_engine().await;
        let response = engine.node.execute(r#"mutation { create_AgentRequest(input: {
            request_id: "context-owner", session_id: "context-session", agent_did: "did:test:grok-shim",
            requester_did: "did:test:requester", lifecycle_state: "processing"
        }) {_docID} }"#).await;
        ensure_no_errors(&response, "context fixture owner").unwrap();
        let response = engine
            .node
            .execute(r#"{ AgentRequest(filter: {request_id: {_eq: "context-owner"}}) {_docID} }"#)
            .await;
        let doc = response.data.as_ref().unwrap()["AgentRequest"][0]["_docID"]
            .as_str()
            .unwrap();
        let accounting = gents_protocol::rendered_request::ContextAccounting {
            accounting_version: 1,
            turn_index: 0,
            attempt: 0,
            estimator: "fixture".into(),
            components: gents_protocol::rendered_request::ContextInputComponents {
                messages: 900,
                documents: 0,
                tool_schemas: 50,
                additional_parameters: 0,
                output_schema: 0,
            },
            estimated_input_tokens: 950,
            context_window: 10_000,
            compaction_threshold_basis_points: 8_000,
            compaction_threshold_tokens: 8_000,
            configured_max_output_tokens: Some(1_000),
            effective_max_output_tokens: Some(1_000),
            compaction_reason:
                gents_protocol::rendered_request::ContextCompactionReason::BelowThreshold,
            pre_compaction_input_tokens: None,
        };
        let encoded =
            gents::graphql::escape_graphql_string(&serde_json::to_string(&accounting).unwrap());
        let doc = gents::graphql::escape_graphql_string(doc);
        let response = engine.node.execute(&format!(r#"mutation {{ create_InferenceCall(input: {{
            call_id: "context-call", request_id: "context-owner", request_doc_id: "{doc}",
            agent_did: "did:test:grok-shim", call_kind: "inference", call_seq: 1,
            queued_at: "2026-09-04T12:00:00Z", completion_tokens: 25, context_accounting_json: "{encoded}"
        }}) {{_docID}} }}"#)).await;
        ensure_no_errors(&response, "context call fixture").unwrap();
        let mut cursor = RequestCursor::new();
        engine
            .project_request_updates("context-session", "context-owner", &mut cursor)
            .await
            .unwrap();
        assert_eq!(
            engine.sequencer.session_total_tokens("context-session"),
            975
        );
        assert!(
            context::load(&engine.node, "foreign-session", "context-owner")
                .await
                .unwrap()
                .is_none()
        );
        engine
            .project_request_updates("context-session", "context-owner", &mut cursor)
            .await
            .unwrap();
        assert_eq!(
            engine.sequencer.session_total_tokens("context-session"),
            975
        );
    }

    #[tokio::test]
    async fn mixed_families_project_in_deterministic_chronology_through_the_embedded_node() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-chron";
        let request_id = "req-chron";

        let seed_parent = format!(
            r#"mutation {{
                parent: create_AgentRequest(input: {{
                    request_id: "{request_id}"
                    agent_did: "did:test:grok-shim"
                    session_id: "{session_id}"
                    content: "parent work"
                    lifecycle_state: "processing"
                    backend_id: ""
                    execution_origin: "interactive"
                    failure_reason: ""
                    created_at: "2026-08-31T22:46:44Z"
                    retry_count: 0
                    max_retries: 3
                }}) {{ _docID }}
            }}"#
        );
        let response = engine.node.execute(&seed_parent).await;
        assert!(
            !response.has_errors(),
            "seed parent failed: {:?}",
            response.errors
        );
        let parent_doc_id = response
            .data
            .as_ref()
            .and_then(|data| data.pointer("/parent/0/_docID"))
            .and_then(Value::as_str)
            .expect("seeded parent document id")
            .to_string();

        // The assistant turn's durable message: reasoning before text.
        let message = serde_json::to_string(&gents_protocol::message::Message::Assistant {
            id: None,
            content: vec![
                gents_protocol::message::AssistantContent::Reasoning(
                    gents_protocol::message::Reasoning::new("thinking"),
                ),
                gents_protocol::message::AssistantContent::text("answer"),
            ],
        })
        .expect("serialize assistant message");
        let escaped_message = gents::graphql::escape_graphql_string(&message);
        let escaped_request = gents::graphql::escape_graphql_string(request_id);
        let seed_message = format!(
            r#"mutation {{
                create_AgentMessage(input: {{
                    message_key: "{escaped_request}:3"
                    session_id: "{session_id}"
                    agent_did: "did:test:grok-shim"
                    requester_did: "did:test:grok-shim"
                    request_id: "{escaped_request}"
                    sequence: 3
                    role: "assistant"
                    content: "{escaped_message}"
                }}) {{ _docID }}
            }}"#
        );
        let response = engine.node.execute(&seed_message).await;
        assert!(
            !response.has_errors(),
            "seed message failed: {:?}",
            response.errors
        );

        // Two same-sequence tool calls seeded in REVERSE stable order: the
        // projection must emit `call-a` before `call-z` by identity. The
        // first is the spawn tool (a recognized spawn verb via its recorded
        // `child_request_id`, not the family-suppressed `task` name), so it
        // keeps its rendered `tool_call` block and links the child.
        seed_tool_call_row(
            &engine,
            session_id,
            request_id,
            Some(&parent_doc_id),
            "call-z",
            "bash",
            4,
            None,
        )
        .await;
        let spawn_tool_doc_id = seed_tool_call_row(
            &engine,
            session_id,
            request_id,
            Some(&parent_doc_id),
            "call-a",
            "spawn_subagent",
            4,
            Some("child-chron"),
        )
        .await;
        // Equal-time children of the parent: the linked child plus an
        // unlinked-by-tool child that shares its timestamp, both tied so
        // only the durable sorts can decide order. Only the spawn-linked
        // child projects (the query filter keeps the family scoped).
        seed_child_request_row(
            &engine,
            request_id,
            &parent_doc_id,
            "call-a",
            &spawn_tool_doc_id,
            "child-chron",
            "2026-08-31T22:46:45Z",
        )
        .await;

        let mut cursor = RequestCursor::new();
        let first = engine
            .project_request_updates(session_id, request_id, &mut cursor)
            .await
            .expect("first poll");
        let kinds: Vec<String> = first.iter().map(update_kind).collect();
        assert_eq!(
            kinds,
            vec![
                "agent_thought_chunk".to_string(),
                "agent_message_chunk".to_string(),
                "tool_call".to_string(),
                "tool_call_update".to_string(),
                "tool_call".to_string(),
                "tool_call_update".to_string(),
                "subagent_spawned".to_string(),
                "subagent_progress".to_string(),
                "available_commands_update".to_string(),
            ],
            "the mixed payload must merge by chronology with family-rank ties and a positionless tail"
        );
        // The same-sequence tools emitted in stable-identity order, not
        // insertion order.
        let tool_ids: Vec<&str> = first
            .iter()
            .filter(|event| event.payload["sessionUpdate"] == "tool_call")
            .map(|event| event.payload["toolCallId"].as_str().expect("toolCallId"))
            .collect();
        assert_eq!(
            tool_ids,
            vec!["call-a", "call-z"],
            "same-sequence tools must emit in stable identity order"
        );
        // The pager routes subagent lifecycle updates by the child session
        // id (the id the ext controls address), never by the spawn tool
        // call id; the payload key is the enum's snake_case field.
        let spawned = first
            .iter()
            .find(|event| event.payload["sessionUpdate"] == "subagent_spawned")
            .expect("spawned event");
        assert_eq!(spawned.payload["subagent_id"], "s-chron-child");

        // Failed later send: record only the first three advances (thought,
        // text, tool-a base). Its terminal update and all later events must
        // reappear in the same deterministic order on the next poll.
        for advance in first.iter().take(3).map(|event| event.advance.clone()) {
            cursor.record(advance);
        }
        let second = engine
            .project_request_updates(session_id, request_id, &mut cursor)
            .await
            .expect("second poll");
        let retry_kinds: Vec<String> = second.iter().map(update_kind).collect();
        assert_eq!(
            retry_kinds,
            vec![
                "tool_call_update".to_string(),
                "tool_call".to_string(),
                "tool_call_update".to_string(),
                "subagent_spawned".to_string(),
                "subagent_progress".to_string(),
                "available_commands_update".to_string(),
            ],
            "the failed events reappear in the same deterministic remaining order; the delivered ones never duplicate"
        );
        let retry_tool_ids: Vec<&str> = second
            .iter()
            .filter(|event| {
                matches!(
                    event.payload["sessionUpdate"].as_str(),
                    Some("tool_call") | Some("tool_call_update")
                )
            })
            .map(|event| event.payload["toolCallId"].as_str().expect("toolCallId"))
            .collect();
        assert_eq!(retry_tool_ids, vec!["call-a", "call-z", "call-z"]);

        // Deliver the rest; a final poll is empty.
        for advance in second.iter().map(|event| event.advance.clone()) {
            cursor.record(advance);
        }
        let third = engine
            .project_request_updates(session_id, request_id, &mut cursor)
            .await
            .expect("third poll");
        assert!(third.is_empty(), "every event is now delivered");
    }

    // -------------------------------------------------------------------
    // Live-tail streaming reconciliation regressions
    // -------------------------------------------------------------------
    //
    // Every test below drives the production path: durable rows seeded
    // into an embedded node with runtime schemas, projected through
    // `ProjectionEngine::project_request_updates` with a real
    // `RequestCursor`, advances recorded exactly as the send loop does
    // (only after a successful send).

    /// One live/terminal `AgentResponse` row for the request: the live tail
    /// snapshot (`content`/`reasoning`), the progress counters, and the
    /// materialization pointer. `status: "streaming"` keeps the request
    /// non-terminal so the live path (not the stop-reason projection) owns
    /// the turn.
    async fn seed_response_row(
        engine: &ProjectionEngine,
        session_id: &str,
        request_id: &str,
        content: &str,
        reasoning: &str,
        progress_seq: i64,
        reasoning_progress_seq: i64,
        materialized_message_sequence: Option<i64>,
    ) {
        let escaped_session = gents::graphql::escape_graphql_string(session_id);
        let escaped_request = gents::graphql::escape_graphql_string(request_id);
        let escaped_content = gents::graphql::escape_graphql_string(content);
        let escaped_reasoning = gents::graphql::escape_graphql_string(reasoning);
        let materialized_field = materialized_message_sequence
            .map(|seq| format!("materialized_message_sequence: {seq}"))
            .unwrap_or_default();
        let mutation = format!(
            r#"mutation {{
                create_AgentResponse(input: {{
                    response_key: "{escaped_request}"
                    request_id: "{escaped_request}"
                    agent_did: "did:test:grok-shim"
                    requester_did: "did:test:grok-shim"
                    session_id: "{escaped_session}"
                    content: "{escaped_content}"
                    reasoning: "{escaped_reasoning}"
                    status: "streaming"
                    token_count: 0
                    progress_seq: {progress_seq}
                    reasoning_progress_seq: {reasoning_progress_seq}
                    created_at: "2026-08-31T23:00:00Z"
                    {materialized_field}
                }}) {{ _docID }}
            }}"#
        );
        let response = engine.node.execute(&mutation).await;
        assert!(
            !response.has_errors(),
            "seed response failed: {:?}",
            response.errors
        );
    }

    /// Replace the seeded response row's live tail and progress counters —
    /// the exact shape of the runtime's streaming flush mutation.
    async fn update_response_tail(
        engine: &ProjectionEngine,
        request_id: &str,
        content: &str,
        reasoning: &str,
        progress_seq: i64,
        reasoning_progress_seq: i64,
    ) {
        let escaped_request = gents::graphql::escape_graphql_string(request_id);
        let escaped_content = gents::graphql::escape_graphql_string(content);
        let escaped_reasoning = gents::graphql::escape_graphql_string(reasoning);
        let mutation = format!(
            r#"mutation {{
                update_AgentResponse(
                    filter: {{ request_id: {{ _eq: "{escaped_request}" }} }},
                    input: {{
                        content: "{escaped_content}"
                        reasoning: "{escaped_reasoning}"
                        progress_seq: {progress_seq}
                        reasoning_progress_seq: {reasoning_progress_seq}
                    }}
                ) {{ _docID }}
            }}"#
        );
        let response = engine.node.execute(&mutation).await;
        assert!(
            !response.has_errors(),
            "update response tail failed: {:?}",
            response.errors
        );
    }

    /// One assistant `AgentMessage` row with a single text block, the exact
    /// envelope shape the runtime persists.
    async fn seed_assistant_text_row(
        engine: &ProjectionEngine,
        session_id: &str,
        request_id: &str,
        sequence: i64,
        text: &str,
    ) {
        let message = serde_json::to_string(&gents_protocol::message::Message::Assistant {
            id: None,
            content: vec![gents_protocol::message::AssistantContent::text(text)],
        })
        .expect("serialize assistant message");
        let escaped_message = gents::graphql::escape_graphql_string(&message);
        let escaped_session = gents::graphql::escape_graphql_string(session_id);
        let escaped_request = gents::graphql::escape_graphql_string(request_id);
        let mutation = format!(
            r#"mutation {{
                create_AgentMessage(input: {{
                    message_key: "{escaped_request}:{sequence}"
                    session_id: "{escaped_session}"
                    agent_did: "did:test:grok-shim"
                    requester_did: "did:test:grok-shim"
                    request_id: "{escaped_request}"
                    sequence: {sequence}
                    role: "assistant"
                    content: "{escaped_message}"
                    timestamp: "2026-08-31T23:00:05Z"
                }}) {{ _docID }}
            }}"#
        );
        let response = engine.node.execute(&mutation).await;
        assert!(
            !response.has_errors(),
            "seed assistant row failed: {:?}",
            response.errors
        );
    }

    /// Grow one seeded assistant row's text in place (an upserted/grown
    /// intermediate row), keeping the same `message_key` and sequence.
    async fn grow_assistant_text_row(
        engine: &ProjectionEngine,
        request_id: &str,
        sequence: i64,
        text: &str,
    ) {
        let message = serde_json::to_string(&gents_protocol::message::Message::Assistant {
            id: None,
            content: vec![gents_protocol::message::AssistantContent::text(text)],
        })
        .expect("serialize assistant message");
        let escaped_message = gents::graphql::escape_graphql_string(&message);
        let escaped_request = gents::graphql::escape_graphql_string(request_id);
        let mutation = format!(
            r#"mutation {{
                update_AgentMessage(
                    filter: {{ message_key: {{ _eq: "{escaped_request}:{sequence}" }} }},
                    input: {{ content: "{escaped_message}" }}
                ) {{ _docID }}
            }}"#
        );
        let response = engine.node.execute(&mutation).await;
        assert!(
            !response.has_errors(),
            "grow assistant row failed: {:?}",
            response.errors
        );
    }

    /// Deliver every event of one poll (the send loop's success path).
    async fn deliver(
        engine: &ProjectionEngine,
        session_id: &str,
        request_id: &str,
        cursor: &mut RequestCursor,
    ) -> Vec<NovelProjectionEvent> {
        let batch = engine
            .project_request_updates(session_id, request_id, cursor)
            .await
            .expect("poll");
        for event in &batch.events {
            cursor.record(event.advance.clone());
        }
        for advance in batch.trailing_advances {
            cursor.record(advance);
        }
        batch.events
    }

    /// `(sessionUpdate kind, text)` of one poll's events.
    fn chunk_texts(events: &[NovelProjectionEvent]) -> Vec<(String, String)> {
        events
            .iter()
            .filter(|event| {
                matches!(
                    event.payload["sessionUpdate"].as_str(),
                    Some("agent_message_chunk") | Some("agent_thought_chunk")
                )
            })
            .map(|event| {
                (
                    event.payload["sessionUpdate"]
                        .as_str()
                        .expect("sessionUpdate")
                        .to_string(),
                    event.payload["content"]["text"]
                        .as_str()
                        .expect("chunk text")
                        .to_string(),
                )
            })
            .collect()
    }

    /// 1. Live content deltas stream incrementally: `Hel` then `lo` before
    /// the request terminalizes, never the whole text replayed per poll.
    #[tokio::test]
    async fn live_content_streams_incremental_deltas_before_terminal() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-live";
        let request_id = "req-live";
        let mut cursor = RequestCursor::new();

        seed_response_row(&engine, session_id, request_id, "Hel", "", 1, 0, None).await;
        let first = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&first),
            vec![("agent_message_chunk".into(), "Hel".into())],
            "the first live tail observation streams the whole snapshot"
        );

        update_response_tail(&engine, request_id, "Hello", "", 2, 0).await;
        let second = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&second),
            vec![("agent_message_chunk".into(), "lo".into())],
            "prefix growth emits only the new suffix"
        );

        // An unchanged tail re-poll emits nothing.
        let third = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert!(third.is_empty(), "an unchanged tail is not novel");
    }

    #[tokio::test]
    async fn reasoning_and_content_in_one_generation_share_a_stream_identity() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-one-stream";
        let request_id = "req-one-stream";
        let mut cursor = RequestCursor::new();

        seed_response_row(&engine, session_id, request_id, "", "thinking", 0, 1, None).await;
        update_response_tail(&engine, request_id, "answer", "thinking", 1, 1).await;
        let events = deliver(&engine, session_id, request_id, &mut cursor).await;
        let keys: Vec<_> = events
            .iter()
            .filter_map(|event| {
                event
                    .timing
                    .as_ref()
                    .map(|timing| timing.segment_key.as_str())
            })
            .collect();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], keys[1]);
    }

    /// 2. A missed poll window (the live tail reset before the final row was
    /// observed) plus the durable final row emits only the never-sent
    /// suffix, never a replay of the already-sent prefix.
    #[tokio::test]
    async fn missed_reset_plus_durable_final_row_emits_only_the_unsent_suffix() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-reset";
        let request_id = "req-reset";
        let mut cursor = RequestCursor::new();

        // Poll 1 sees the live prefix "He" and delivers it.
        seed_response_row(&engine, session_id, request_id, "He", "", 1, 0, None).await;
        let first = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&first),
            vec![("agent_message_chunk".into(), "He".into())]
        );

        // Between polls the runtime reset the tail (empty) and materialized
        // the final row "Hello" — the projection never observed "Hello"
        // live. The materialization pointer binds the row to the live
        // segment whose prefix was delivered, so the durable pass must emit
        // only "llo".
        seed_assistant_text_row(&engine, session_id, request_id, 5, "Hello").await;
        update_response_tail(&engine, request_id, "", "", 1, 0).await;
        update_materialized_sequence(&engine, request_id, 5).await;
        let second = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&second),
            vec![("agent_message_chunk".into(), "llo".into())],
            "the durable final row emits only the bytes the live cursor never sent"
        );

        let third = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert!(third.is_empty(), "the final row is fully delivered");
    }

    /// 3. The whole live text was sent, then the durable final row appears:
    /// nothing replays. The live `sent_bytes` prove the row is covered.
    #[tokio::test]
    async fn fully_sent_live_tail_suppresses_the_durable_final_row_replay() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-full";
        let request_id = "req-full";
        let mut cursor = RequestCursor::new();

        seed_response_row(&engine, session_id, request_id, "Hello", "", 1, 0, None).await;
        let first = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&first),
            vec![("agent_message_chunk".into(), "Hello".into())]
        );

        // Materialization: the tail cleared and the final row carries the
        // same text, bound by materialized_message_sequence.
        seed_assistant_text_row(&engine, session_id, request_id, 5, "Hello").await;
        update_response_tail(&engine, request_id, "", "", 1, 0).await;
        update_materialized_sequence(&engine, request_id, 5).await;
        let second = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert!(
            second.is_empty(),
            "a fully live-sent final row never replays durably"
        );
    }

    #[tokio::test]
    async fn durable_wakeup_notification_projects_once_without_internal_inputs() {
        let (_dir, engine) = embedded_engine().await;
        let mut cursor = RequestCursor::new();
        for (sequence, key, text) in [
            (1, "wake-context", "private context"),
            (
                2,
                "background-completion-notification:call:tool",
                "<tool-completion status=\"completed\"><result>done</result></tool-completion>",
            ),
            (
                3,
                "wake-instruction",
                "Review background results and continue",
            ),
            (
                4,
                "ordinary-user-input",
                "<tool-completion>This text alone does not establish runtime origin</tool-completion>",
            ),
        ] {
            let response = engine
                .node
                .execute(&format!(
                    r#"mutation {{ create_AgentMessage(input: {{
                    message_key: "{}", session_id: "s-wakeup", request_id: "r-wakeup",
                    agent_did: "did:test:grok-shim", requester_did: "did:test:grok-shim",
                    sequence: {sequence}, role: "user", content: "{}"
                }}) {{ _docID }} }}"#,
                    gents::graphql::escape_graphql_string(key),
                    gents::graphql::escape_graphql_string(text)
                ))
                .await;
            assert!(!response.has_errors(), "{:?}", response.errors);
        }
        let unsent = engine
            .project_request_updates("s-wakeup", "r-wakeup", &mut cursor)
            .await
            .unwrap();
        assert_eq!(unsent.len(), 1);
        assert_eq!(unsent[0].payload["sessionUpdate"], "user_message_chunk");
        assert_eq!(unsent[0].payload["_meta"]["hideFromScrollback"], true);
        assert!(unsent[0].payload["content"]["text"]
            .as_str()
            .unwrap()
            .contains("<tool-completion"));
        let sent = deliver(&engine, "s-wakeup", "r-wakeup", &mut cursor).await;
        assert_eq!(sent.len(), 1, "unsent notification retries");
        assert_eq!(sent[0].payload, unsent[0].payload);
        assert!(deliver(&engine, "s-wakeup", "r-wakeup", &mut cursor)
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn failed_tool_terminal_retries_after_content_refinement_delivery() {
        let (_dir, engine) = embedded_engine().await;
        let mut cursor = RequestCursor::new();
        let doc = seed_tool_call_row(
            &engine,
            "s-failed",
            "r-failed",
            None,
            "failed-call",
            "bash",
            2,
            None,
        )
        .await;
        let response = engine.node.execute(&format!(
            r#"mutation {{ update_AgentToolCall(filter: {{ _docID: {{ _eq: "{}" }} }}, input: {{ lifecycle_state: "running", result: null }}) {{ _docID }} }}"#,
            gents::graphql::escape_graphql_string(&doc))).await;
        assert!(!response.has_errors(), "{:?}", response.errors);
        deliver(&engine, "s-failed", "r-failed", &mut cursor).await;
        let response = engine.node.execute(&format!(
            r#"mutation {{ update_AgentToolCall(filter: {{ _docID: {{ _eq: "{}" }} }}, input: {{ lifecycle_state: "failed", result: "exit 7" }}) {{ _docID }} }}"#,
            gents::graphql::escape_graphql_string(&doc))).await;
        assert!(!response.has_errors(), "{:?}", response.errors);
        let batch = engine
            .project_request_updates("s-failed", "r-failed", &mut cursor)
            .await
            .unwrap();
        let updates: Vec<_> = batch
            .iter()
            .filter(|event| event.payload["toolCallId"] == "failed-call")
            .collect();
        assert_eq!(updates.len(), 2);
        assert!(updates[0].payload.get("status").is_none());
        assert_eq!(
            updates[1].payload,
            json!({"sessionUpdate": "tool_call_update", "toolCallId": "failed-call", "status": "failed"})
        );
        // The socket accepts the output refinement then fails before status.
        cursor.record(updates[0].advance.clone());
        let retry = deliver(&engine, "s-failed", "r-failed", &mut cursor).await;
        let terminal: Vec<_> = retry
            .iter()
            .filter(|event| event.payload["toolCallId"] == "failed-call")
            .collect();
        assert_eq!(terminal.len(), 1);
        assert_eq!(terminal[0].payload, updates[1].payload);
        assert!(deliver(&engine, "s-failed", "r-failed", &mut cursor)
            .await
            .is_empty());
    }

    /// Production-order regression: two live prefix polls are followed by
    /// the durable row, materialization stamp, and only then the tail reset.
    /// The durable row must not replay the full value after both live deltas
    /// already reconstructed it on the wire.
    #[tokio::test]
    async fn split_live_growth_then_materialize_before_reset_never_duplicates() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-live-materialize-order";
        let request_id = "req-live-materialize-order";
        let mut cursor = RequestCursor::new();

        seed_response_row(
            &engine,
            session_id,
            request_id,
            "DUPLICATION_SENTIN",
            "",
            1,
            0,
            None,
        )
        .await;
        let first = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&first),
            vec![("agent_message_chunk".into(), "DUPLICATION_SENTIN".into())]
        );

        update_response_tail(&engine, request_id, "DUPLICATION_SENTINEL_9472", "", 2, 0).await;
        let growth = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&growth),
            vec![("agent_message_chunk".into(), "EL_9472".into())]
        );

        seed_assistant_text_row(
            &engine,
            session_id,
            request_id,
            5,
            "DUPLICATION_SENTINEL_9472",
        )
        .await;
        let open_row = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert!(
            chunk_texts(&open_row).is_empty(),
            "a durable row appearing while its fully sent tail is still open must not replay"
        );
        update_materialized_sequence(&engine, request_id, 5).await;
        update_response_tail(&engine, request_id, "", "", 2, 0).await;

        let materialized = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert!(
            chunk_texts(&materialized).is_empty(),
            "fully live-sent bytes must suppress the later durable row"
        );
    }

    /// 4. An inflight durable row grows (the same key/sequence upserted with
    /// longer text): the poll emits exactly the growth suffix.
    #[tokio::test]
    async fn inflight_durable_row_growth_emits_only_the_suffix() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-grow";
        let request_id = "req-grow";
        let mut cursor = RequestCursor::new();

        seed_assistant_text_row(&engine, session_id, request_id, 2, "Hel").await;
        let first = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&first),
            vec![("agent_message_chunk".into(), "Hel".into())]
        );

        grow_assistant_text_row(&engine, request_id, 2, "Hello").await;
        let second = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&second),
            vec![("agent_message_chunk".into(), "lo".into())],
            "row growth emits only the newly proven suffix"
        );

        let third = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert!(third.is_empty(), "the grown row is fully delivered");
    }

    /// A durable row replacement that is not an append cannot reuse a byte
    /// offset from the old value. In particular, a one-byte ASCII value
    /// replaced by multibyte UTF-8 must emit the authoritative replacement
    /// whole and never slice through a code point.
    #[tokio::test]
    async fn durable_non_prefix_utf8_replacement_emits_whole_without_panicking() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-durable-utf8";
        let request_id = "req-durable-utf8";
        let mut cursor = RequestCursor::new();

        seed_assistant_text_row(&engine, session_id, request_id, 2, "a").await;
        let first = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&first),
            vec![("agent_message_chunk".into(), "a".into())]
        );

        grow_assistant_text_row(&engine, request_id, 2, "日x").await;
        let replacement = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&replacement),
            vec![("agent_message_chunk".into(), "日x".into())]
        );
    }

    /// 5. An old segment closed by a reset, then a *new* segment whose text
    /// happens to start with the old segment's text: the new segment streams
    /// in full — live bytes of the old segment never suppress it.
    #[tokio::test]
    async fn new_segment_after_reset_is_not_suppressed_by_an_accidental_prefix() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-seg";
        let request_id = "req-seg";
        let mut cursor = RequestCursor::new();

        // Old segment: "Hello" delivered live.
        seed_response_row(&engine, session_id, request_id, "Hello", "", 1, 0, None).await;
        let first = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&first),
            vec![("agent_message_chunk".into(), "Hello".into())]
        );

        // Reset, then a new segment "Hello world" — its prefix collides with
        // the old segment but it is a *different* logical segment.
        update_response_tail(&engine, request_id, "", "", 1, 0).await;
        let reset = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert!(reset.is_empty(), "the reset itself emits nothing");

        update_response_tail(&engine, request_id, "Hello world", "", 3, 0).await;
        let second = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&second),
            vec![("agent_message_chunk".into(), "Hello world".into())],
            "the new segment streams in full; old-segment bytes never suppress it"
        );
    }

    /// A single poll can observe several durable response snapshots. A
    /// reset between two non-empty segments must replay both segments in
    /// commit order, even when no poll happened at the reset boundary.
    #[tokio::test]
    async fn one_poll_replays_growth_reset_and_new_segment_in_order() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-missed-reset";
        let request_id = "req-missed-reset";
        let mut cursor = RequestCursor::new();

        seed_response_row(&engine, session_id, request_id, "A", "", 1, 0, None).await;
        update_response_tail(&engine, request_id, "", "", 1, 0).await;
        update_response_tail(&engine, request_id, "B", "", 2, 0).await;

        let events = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&events),
            vec![
                ("agent_message_chunk".into(), "A".into()),
                ("agent_message_chunk".into(), "B".into()),
            ]
        );
        let segment_keys: Vec<_> = events
            .iter()
            .filter_map(|event| {
                event
                    .timing
                    .as_ref()
                    .map(|timing| timing.segment_key.as_str())
            })
            .collect();
        assert_eq!(segment_keys.len(), 2);
        assert_ne!(
            segment_keys[0], segment_keys[1],
            "a reset opens a distinct pager stream generation"
        );
        assert!(deliver(&engine, session_id, request_id, &mut cursor)
            .await
            .is_empty());
    }

    /// Identical bytes on either side of a missed reset are two logical
    /// segments. History, not byte equality, preserves the second segment.
    #[tokio::test]
    async fn one_poll_replays_identical_segments_separated_by_reset() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-identical-reset";
        let request_id = "req-identical-reset";
        let mut cursor = RequestCursor::new();

        seed_response_row(&engine, session_id, request_id, "same", "", 1, 0, None).await;
        update_response_tail(&engine, request_id, "", "", 1, 0).await;
        update_response_tail(&engine, request_id, "same", "", 2, 0).await;

        let events = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&events),
            vec![
                ("agent_message_chunk".into(), "same".into()),
                ("agent_message_chunk".into(), "same".into()),
            ]
        );
    }

    /// Historical live segments carry the chronology of the durable row they
    /// materialized into. Replaying two segments around a tool must therefore
    /// preserve text(3) -> tool(4) -> text(6), rather than assigning both
    /// segments the newest assistant sequence.
    #[tokio::test]
    async fn reconnect_with_open_tail_keeps_repeated_closed_segments_once() {
        let (_dir, engine) = embedded_engine().await;
        let session = "s-open-repeat";
        let request = "req-open-repeat";
        seed_response_row(&engine, session, request, "same", "", 1, 0, None).await;
        update_response_tail(&engine, request, "", "", 1, 0).await;
        update_response_tail(&engine, request, "same", "", 2, 0).await;
        update_response_tail(&engine, request, "", "", 2, 0).await;
        update_response_tail(&engine, request, "open", "", 3, 0).await;
        seed_assistant_text_row(&engine, session, request, 3, "same").await;
        seed_assistant_text_row(&engine, session, request, 6, "same").await;
        let mut cursor = RequestCursor::new();
        let events = deliver(&engine, session, request, &mut cursor).await;
        assert_eq!(
            chunk_texts(&events),
            vec![
                ("agent_message_chunk".into(), "same".into()),
                ("agent_message_chunk".into(), "same".into()),
                ("agent_message_chunk".into(), "open".into()),
            ]
        );
        assert!(deliver(&engine, session, request, &mut cursor)
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn retained_segments_sort_around_an_intervening_tool() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-history-order";
        let request_id = "req-history-order";
        let mut cursor = RequestCursor::new();

        seed_response_row(&engine, session_id, request_id, "first", "", 1, 0, None).await;
        update_response_tail(&engine, request_id, "", "", 1, 0).await;
        update_response_tail(&engine, request_id, "second", "", 2, 0).await;
        seed_assistant_text_row(&engine, session_id, request_id, 3, "first").await;
        seed_tool_call_row(
            &engine,
            session_id,
            request_id,
            None,
            "call-middle",
            "bash",
            4,
            None,
        )
        .await;
        seed_assistant_text_row(&engine, session_id, request_id, 6, "second").await;

        let events = deliver(&engine, session_id, request_id, &mut cursor).await;
        let ordered: Vec<_> = events
            .iter()
            .filter_map(|event| match event.payload["sessionUpdate"].as_str() {
                Some("agent_message_chunk") => event.payload["content"]["text"]
                    .as_str()
                    .map(ToOwned::to_owned),
                Some("tool_call") => Some("tool-base".to_string()),
                Some("tool_call_update") => Some("tool-terminal".to_string()),
                _ => None,
            })
            .collect();
        assert_eq!(
            ordered,
            vec!["first", "tool-base", "tool-terminal", "second"]
        );
    }

    /// The runtime can persist an in-flight assistant row while its matching
    /// response tail remains open. The live send is the one wire delivery;
    /// the same row must not immediately replay the text durably.
    #[tokio::test]
    async fn uniquely_matching_open_durable_row_is_suppressed_after_live_delivery() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-open-row";
        let request_id = "req-open-row";
        let mut cursor = RequestCursor::new();

        seed_response_row(&engine, session_id, request_id, "hello", "", 1, 0, None).await;
        seed_assistant_text_row(&engine, session_id, request_id, 3, "hello").await;
        let events = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&events),
            vec![("agent_message_chunk".into(), "hello".into())]
        );
        let timing = events[0].timing.as_ref().expect("message timing");
        assert_eq!(timing.stream_start_candidate_ms, Some(1_788_217_200_000));
        assert_eq!(timing.agent_timestamp_candidate_ms, Some(1_788_217_205_000));
        assert!(deliver(&engine, session_id, request_id, &mut cursor)
            .await
            .is_empty());
    }

    #[test]
    fn unstamped_evidence_never_cross_binds_reversed_rows() {
        let mut live = LiveSegmentCursor {
            closed_evidence: vec![
                ClosedEvidence {
                    sent_bytes: "A".into(),
                    materialized_sequence: None,
                    bound_row: None,
                    history_height: 2,
                    segment_key: None,
                },
                ClosedEvidence {
                    sent_bytes: "B".into(),
                    materialized_sequence: None,
                    bound_row: None,
                    history_height: 4,
                    segment_key: None,
                },
            ],
            ..LiveSegmentCursor::default()
        };
        let rows = vec![
            DurableRowView {
                identity: DurableRowIdentity {
                    sequence: 3,
                    message_key: "row-b".into(),
                },
                content: "B".into(),
                reasoning: String::new(),
            },
            DurableRowView {
                identity: DurableRowIdentity {
                    sequence: 5,
                    message_key: "row-a".into(),
                },
                content: "A".into(),
                reasoning: String::new(),
            },
        ];
        bind_durable_evidence(&mut live, EvidenceRail::Content, &rows);
        assert!(
            live.closed_evidence
                .iter()
                .all(|evidence| evidence.bound_row.is_none()),
            "a locally unique but globally inverted assignment must fail closed"
        );
    }

    /// 6. Identical reasoning with an unchanged `reasoning_progress_seq` is a
    /// stale read (nothing novel); with an advanced seq it is a genuine
    /// later identical rewrite — and since no new bytes exist, still nothing
    /// new streams (the rewrite stands, never re-sent).
    #[tokio::test]
    async fn identical_reasoning_is_stale_without_seq_advance_and_genuine_with_it() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-reason";
        let request_id = "req-reason";
        let mut cursor = RequestCursor::new();

        seed_response_row(&engine, session_id, request_id, "", "thinking", 0, 1, None).await;
        let first = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&first),
            vec![("agent_thought_chunk".into(), "thinking".into())]
        );

        // Stale identical read: same bytes, same seq. Nothing novel.
        let stale = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert!(
            stale.is_empty(),
            "identical bytes with an unchanged seq are stale"
        );

        // Genuine later identical rewrite: same bytes, advanced seq. The
        // rewrite stands on the wire already; no new bytes exist to emit.
        update_response_tail(&engine, request_id, "", "thinking", 0, 2).await;
        let rewrite = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert!(
            rewrite.is_empty(),
            "an identical rewrite carries no new bytes; the sent text stands"
        );

        // A rewrite to *different* bytes does stream: the divergence closes
        // the segment and the new observation streams in full.
        update_response_tail(&engine, request_id, "", "revised", 0, 3).await;
        let diverged = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&diverged),
            vec![("agent_thought_chunk".into(), "revised".into())],
            "a diverging rewrite closes the segment and streams the new snapshot"
        );
    }

    /// 7. Reasoning's bounded 64-KiB rolling preview drops its head: the
    /// rollover emits exactly the newly appended suffix, once, without
    /// re-streaming the window's retained bytes.
    #[tokio::test]
    async fn reasoning_window_rollover_emits_the_new_suffix_once() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-roll";
        let request_id = "req-roll";
        let mut cursor = RequestCursor::new();

        // A full first window with a distinct head that the runtime will
        // trim when 16 new bytes arrive.
        let head = format!("{}{}", "x".repeat(16), "y".repeat(64 * 1024 - 16));
        seed_response_row(&engine, session_id, request_id, "", &head, 0, 1, None).await;
        let first = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&first).len(),
            1,
            "the first window streams once"
        );

        // The runtime appended past the bound: the preview dropped its head
        // but keeps continuity — the suffix past the overlap is the new text.
        let rolled = format!("{}{}", "y".repeat(64 * 1024 - 16), "z".repeat(16));
        update_response_tail(&engine, request_id, "", &rolled, 0, 2).await;
        let second = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&second),
            vec![("agent_thought_chunk".into(), "z".repeat(16))],
            "the rollover emits only the bytes past the retained overlap"
        );

        // Re-observing the same window is not novel.
        let third = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert!(third.is_empty(), "the rolled window is fully delivered");
    }

    #[test]
    fn utf8_short_saturated_reasoning_window_defers_an_unproven_jump() {
        for size in
            MAX_LIVE_REASONING_WINDOW_BYTES.saturating_sub(3)..=MAX_LIVE_REASONING_WINDOW_BYTES
        {
            assert!(reasoning_window_is_saturated(&"x".repeat(size)));
        }
        assert!(!reasoning_window_is_saturated(
            &"x".repeat(MAX_LIVE_REASONING_WINDOW_BYTES - 4)
        ));

        let mut cursor = LiveSegmentCursor::default();
        let first = "a".repeat(MAX_LIVE_REASONING_WINDOW_BYTES);
        let (_, first_plan) = cursor
            .plan(&first, Some(1), true, true)
            .expect("first reasoning window");
        cursor.commit(first_plan, Some(1));

        // The runtime's UTF-8-safe cut begins one byte inside a four-byte
        // scalar and advances three bytes, producing a saturated MAX-3 tail.
        let source = format!("💡{}", "z".repeat(MAX_LIVE_REASONING_WINDOW_BYTES - 3));
        let saturated = tail_bytes(&source, MAX_LIVE_REASONING_WINDOW_BYTES);
        assert_eq!(saturated.len(), MAX_LIVE_REASONING_WINDOW_BYTES - 3);
        let (delta, plan) = cursor
            .plan(saturated, Some(2), true, true)
            .expect("advanced saturated reasoning observation");
        assert!(
            delta.is_empty(),
            "the missing middle is not reconstructable"
        );
        assert!(plan.unproven_gap, "the rail waits for its durable row");
    }

    /// A persisted reasoning preview can jump by more than one whole window
    /// between polls. With no overlap the missing middle is unprovable: do not
    /// emit the new tail as if it were a fresh segment. The later durable row
    /// emits exactly the authoritative suffix after the already-sent prefix.
    #[tokio::test]
    async fn no_overlap_reasoning_jump_defers_to_the_durable_row() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-reason-gap";
        let request_id = "req-reason-gap";
        let mut cursor = RequestCursor::new();
        let first_window = "a".repeat(MAX_LIVE_REASONING_WINDOW_BYTES);
        let latest_window = "z".repeat(MAX_LIVE_REASONING_WINDOW_BYTES);

        seed_response_row(
            &engine,
            session_id,
            request_id,
            "",
            &first_window,
            0,
            1,
            None,
        )
        .await;
        let first = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(chunk_texts(&first)[0].1, first_window);

        update_response_tail(&engine, request_id, "", &latest_window, 0, 2).await;
        let deferred = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert!(
            chunk_texts(&deferred).is_empty(),
            "an unproved window jump must not emit a lossy tail"
        );

        let missing_suffix = format!("{}{}", "middle", latest_window);
        let full_reasoning = format!("{}{}", first_window, missing_suffix);
        seed_assistant_thought_row(&engine, session_id, request_id, 5, &full_reasoning).await;
        update_materialized_sequence(&engine, request_id, 5).await;
        let recovered = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&recovered),
            vec![("agent_thought_chunk".into(), missing_suffix)]
        );
    }

    /// 8. A multibyte UTF-8 chunk appended at a poll boundary never splits a
    /// character: the delta is a whole sequence of chars.
    #[tokio::test]
    async fn multibyte_utf8_appends_never_split_a_character() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-utf8";
        let request_id = "req-utf8";
        let mut cursor = RequestCursor::new();

        seed_response_row(&engine, session_id, request_id, "日", "", 1, 0, None).await;
        let first = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&first),
            vec![("agent_message_chunk".into(), "日".into())]
        );

        update_response_tail(&engine, request_id, "日本語テキスト", "", 2, 0).await;
        let second = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&second),
            vec![("agent_message_chunk".into(), "本語テキスト".into())],
            "the multibyte suffix streams whole, never sliced mid-character"
        );

        // A 4-byte emoji append at the boundary.
        update_response_tail(&engine, request_id, "日本語テキスト🚀", "", 3, 0).await;
        let third = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&third),
            vec![("agent_message_chunk".into(), "🚀".into())]
        );
    }

    /// 9. Whitespace-only growth (a newline between words) streams verbatim:
    /// no trim logic may corrupt the concatenation.
    #[tokio::test]
    async fn whitespace_only_growth_streams_verbatim() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-ws";
        let request_id = "req-ws";
        let mut cursor = RequestCursor::new();

        seed_response_row(&engine, session_id, request_id, "one", "", 1, 0, None).await;
        let first = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&first),
            vec![("agent_message_chunk".into(), "one".into())]
        );

        update_response_tail(&engine, request_id, "one\ntwo", "", 2, 0).await;
        let second = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&second),
            vec![("agent_message_chunk".into(), "\ntwo".into())],
            "the whitespace-only part of the growth streams verbatim"
        );
    }

    /// 10. A failed send never advances the live cursor: the identical delta
    /// re-plans on the next poll, and after it is delivered nothing
    /// duplicates.
    #[tokio::test]
    async fn failed_send_never_advances_the_live_cursor() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-fail";
        let request_id = "req-fail";
        let mut cursor = RequestCursor::new();

        seed_response_row(&engine, session_id, request_id, "Hel", "", 1, 0, None).await;
        // Poll but record nothing (every send failed).
        let first = engine
            .project_request_updates(session_id, request_id, &mut cursor)
            .await
            .expect("poll");
        assert_eq!(
            chunk_texts(&first),
            vec![("agent_message_chunk".into(), "Hel".into())]
        );

        update_response_tail(&engine, request_id, "Hello", "", 2, 0).await;
        // Second poll: the cursor never advanced, so the complete validated
        // history after its anchor is replayed in order: the exact failed
        // "Hel" event followed by the newly observed "lo" suffix.
        let second = engine
            .project_request_updates(session_id, request_id, &mut cursor)
            .await
            .expect("poll");
        assert_eq!(
            chunk_texts(&second),
            vec![
                ("agent_message_chunk".into(), "Hel".into()),
                ("agent_message_chunk".into(), "lo".into()),
            ],
            "a failed send replays the exact failed event, then the ordered catch-up suffix"
        );

        // Deliver it; a final poll is empty.
        let delivered = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&delivered),
            vec![
                ("agent_message_chunk".into(), "Hel".into()),
                ("agent_message_chunk".into(), "lo".into()),
            ]
        );
        let final_poll = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert!(final_poll.is_empty(), "nothing replays after delivery");
    }

    /// 11. Reasoning plans before body text in one poll (thought-before-text
    /// matching the durable row order), and each keeps its own cursor.
    #[tokio::test]
    async fn reasoning_plans_before_body_text_in_one_poll() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-order";
        let request_id = "req-order";
        let mut cursor = RequestCursor::new();

        seed_response_row(
            &engine, session_id, request_id, "answer", "thought", 1, 1, None,
        )
        .await;
        let first = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&first),
            vec![
                ("agent_thought_chunk".into(), "thought".into()),
                ("agent_message_chunk".into(), "answer".into()),
            ],
            "reasoning plans first, then the body, in one poll"
        );

        // Independent growth of each stream emits independent suffixes.
        update_response_tail(&engine, request_id, "answered", "thought through", 2, 2).await;
        let second = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&second),
            vec![
                ("agent_thought_chunk".into(), " through".into()),
                ("agent_message_chunk".into(), "ed".into()),
            ]
        );
    }

    /// 12. Live text sorts before the same-poll tool call by the assistant
    /// row's sequence (cross-family chronology keeps holding for live
    /// events).
    #[tokio::test]
    async fn live_text_sorts_before_the_same_poll_tool_call() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-tool";
        let request_id = "req-tool";
        let mut cursor = RequestCursor::new();

        // The assistant row exists at sequence 3 (the live segment's
        // position) and a tool call at sequence 4.
        seed_assistant_text_row(&engine, session_id, request_id, 3, "").await;
        seed_tool_call_row(
            &engine, session_id, request_id, None, "call-x", "bash", 4, None,
        )
        .await;
        seed_response_row(
            &engine,
            session_id,
            request_id,
            "running a tool",
            "",
            1,
            0,
            None,
        )
        .await;

        let first = deliver(&engine, session_id, request_id, &mut cursor).await;
        let kinds: Vec<String> = first.iter().map(update_kind).collect();
        assert_eq!(
            kinds,
            vec![
                "agent_message_chunk".to_string(),
                "tool_call".to_string(),
                "tool_call_update".to_string(),
                "available_commands_update".to_string(),
            ],
            "live text at the assistant row's sequence precedes the sequence-4 tool call; the positionless commands tail is last"
        );
    }

    /// Durable transcript query progress is a delivery cursor, not an
    /// observation cursor: abandoning a batch (the send-failure case) must
    /// cause every undelivered row to be queried and planned again.
    #[tokio::test]
    async fn durable_message_query_high_water_commits_only_after_the_batch() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-durable-retry";
        let request_id = "req-durable-retry";
        let mut cursor = RequestCursor::new();
        seed_assistant_text_row(&engine, session_id, request_id, 3, "durable").await;

        let abandoned = engine
            .project_request_updates(session_id, request_id, &mut cursor)
            .await
            .expect("first projection");
        assert_eq!(
            chunk_texts(&abandoned.events),
            vec![("agent_message_chunk".into(), "durable".into())]
        );
        assert_eq!(cursor.message_sequence_high_water, None);

        let retry = engine
            .project_request_updates(session_id, request_id, &mut cursor)
            .await
            .expect("retry projection");
        assert_eq!(
            chunk_texts(&retry.events),
            vec![("agent_message_chunk".into(), "durable".into())]
        );
        for event in retry.events {
            cursor.record(event.advance);
        }
        for advance in retry.trailing_advances {
            cursor.record(advance);
        }
        assert_eq!(cursor.message_sequence_high_water, Some(3));
    }

    /// 13. A live reasoning tail and a durable reasoning row of the same
    /// text reconcile: the fully-sent live reasoning suppresses the durable
    /// thought row's replay (the reasoning stream is one logical stream).
    #[tokio::test]
    async fn fully_sent_live_reasoning_suppresses_the_durable_thought_row_replay() {
        let (_dir, engine) = embedded_engine().await;
        let session_id = "s-thought";
        let request_id = "req-thought";
        let mut cursor = RequestCursor::new();

        seed_response_row(&engine, session_id, request_id, "", "thinking", 0, 1, None).await;
        let first = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert_eq!(
            chunk_texts(&first),
            vec![("agent_thought_chunk".into(), "thinking".into())]
        );

        // Materialization: the reasoning tail cleared and the durable row
        // carries the same thought text, bound by
        // materialized_message_sequence.
        seed_assistant_thought_row(&engine, session_id, request_id, 5, "thinking").await;
        update_response_tail(&engine, request_id, "", "", 0, 1).await;
        update_materialized_sequence(&engine, request_id, 5).await;
        let second = deliver(&engine, session_id, request_id, &mut cursor).await;
        assert!(
            second.iter().all(|event| !matches!(
                event.payload["sessionUpdate"].as_str(),
                Some("agent_thought_chunk") | Some("agent_message_chunk")
            )),
            "a fully live-sent thought never replays from the durable row"
        );
    }

    /// Stamp `materialized_message_sequence` on the request's response row.
    async fn update_materialized_sequence(
        engine: &ProjectionEngine,
        request_id: &str,
        sequence: i64,
    ) {
        let escaped_request = gents::graphql::escape_graphql_string(request_id);
        let mutation = format!(
            r#"mutation {{
                update_AgentResponse(
                    filter: {{ request_id: {{ _eq: "{escaped_request}" }} }},
                    input: {{ materialized_message_sequence: {sequence} }}
                ) {{ _docID }}
            }}"#
        );
        let response = engine.node.execute(&mutation).await;
        assert!(
            !response.has_errors(),
            "stamp materialized_message_sequence failed: {:?}",
            response.errors
        );
    }

    /// One assistant `AgentMessage` row with a single reasoning block.
    async fn seed_assistant_thought_row(
        engine: &ProjectionEngine,
        session_id: &str,
        request_id: &str,
        sequence: i64,
        text: &str,
    ) {
        let message = serde_json::to_string(&gents_protocol::message::Message::Assistant {
            id: None,
            content: vec![gents_protocol::message::AssistantContent::Reasoning(
                gents_protocol::message::Reasoning::new(text),
            )],
        })
        .expect("serialize assistant message");
        let escaped_message = gents::graphql::escape_graphql_string(&message);
        let escaped_session = gents::graphql::escape_graphql_string(session_id);
        let escaped_request = gents::graphql::escape_graphql_string(request_id);
        let mutation = format!(
            r#"mutation {{
                create_AgentMessage(input: {{
                    message_key: "{escaped_request}:{sequence}"
                    session_id: "{escaped_session}"
                    agent_did: "did:test:grok-shim"
                    requester_did: "did:test:grok-shim"
                    request_id: "{escaped_request}"
                    sequence: {sequence}
                    role: "assistant"
                    content: "{escaped_message}"
                }}) {{ _docID }}
            }}"#
        );
        let response = engine.node.execute(&mutation).await;
        assert!(
            !response.has_errors(),
            "seed assistant thought row failed: {:?}",
            response.errors
        );
    }
}
