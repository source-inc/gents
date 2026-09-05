//! Grok shim message projection.
//!
//! Projects the durable `AgentResponse`/`AgentMessage` rows of one request id
//! into the Grok pager's streaming `session/update` notification payloads:
//! `agent_message_chunk`, `agent_thought_chunk`, and `user_message_chunk`.
//!
//! Two contracts matter for fidelity:
//!
//! 1. **Persisted envelope decoding.** `AgentMessage.content` is not raw
//!    delta text: the runtime persists `serde_json::to_string(&Message)`
//!    where `Message` is `gents_protocol::message::Message` (tag = "role":
//!    `{"role":"assistant","id":null,"content":[{"text":"..."}]}` for
//!    assistant rows, `{"role":"user","content":[{"type":"text","text":
//!    "..."}]}` for user rows). This leaf decodes that envelope through
//!    `gents_protocol::transcript::decode_persisted_message` (which also
//!    tolerates a bare `Vec<AssistantContent>`/`Vec<UserContent>` array and
//!    falls back to treating the blob as plain text) and streams only the
//!    *text* blocks of the decoded message. A row whose persisted content
//!    fails to decode projects as nothing rather than as JSON noise.
//!
//! 2. **Wire shape.** The Grok decoder expects the chunk field name
//!    `content` (not `contentBlock`): each update payload is
//!    `{"sessionUpdate":"agent_message_chunk","content":{"type":"text",
//!    "text":"<delta>"}}`. `_meta` is stamped by the projection engine
//!    (totalTokens, promptId, isReplay, eventId); this leaf returns the
//!    split update shapes and the engine renders the final notification.
//!
//! The projection is bounded and request-id-scoped: exactly one
//! `AgentResponse` discovery query (the latest row for the request), one
//! composite-history observation of that row's document, and fixed-size
//! `AgentMessage` pages from an inclusive sequence cursor, with no session replay and no durable
//! materialization — issued in that order. The history tip is loaded and
//! fixed *before* the `AgentMessage` rows are read, so a tail reset
//! observed in the history implies the assistant row the runtime persisted
//! before that reset is already durable and present in the rows read
//! after: the live segment's chronology position can never associate a
//! observed reset with a stale row list.
//!
//! Response selection fails closed: only the last snapshot of the
//! *validated* composite history may supply the live tail and response
//! completion observation. When the history cannot be read or proven,
//! the projection exposes no live bytes and `terminal = false`
//! — never falling back to the discovery row — while the durable
//! `AgentMessage` projection continues and polling may retry. A proven
//! request is terminal only when the validated tip's response status is
//! `complete`/`error` or it carries a non-empty `interrupted_at`; anything
//! else is a still-running response. The turn owner separately checks the
//! canonical request lifecycle before resolving a turn. The legacy
//! `token_count` observation is not UI context occupancy: `context.rs`
//! obtains that from the runtime's persisted inference accounting.
//!
//! All queries go through the in-process embedded node with every
//! interpolated value passed through `escape_graphql_string`; no HTTP
//! GraphQL helper is used.

use std::sync::Arc;

use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use defra_node::EmbeddedNode;
use gents::graphql::{ensure_no_errors, escape_graphql_string};
use gents_protocol::message::{AssistantContent, Message, UserContent};
use gents_protocol::transcript::decode_persisted_message;
use serde::Deserialize;
use serde_json::{json, Value};

use super::{effective_context_window_tokens, nonempty};

/// `sessionUpdate` discriminators emitted by this leaf.
pub(super) const AGENT_MESSAGE_CHUNK: &str = "agent_message_chunk";
pub(super) const AGENT_THOUGHT_CHUNK: &str = "agent_thought_chunk";
pub(super) const USER_MESSAGE_CHUNK: &str = "user_message_chunk";

/// One projected streaming chunk, split by kind so the projection engine
/// only needs to stamp `_meta` and wrap it in a `session/update`
/// notification.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum MessageUpdate {
    /// Assistant body text delta → `agent_message_chunk`.
    AgentMessageChunk { text: String },
    /// Assistant reasoning delta → `agent_thought_chunk`.
    AgentThoughtChunk { text: String },
    /// Echoed user prompt text → `user_message_chunk`.
    UserMessageChunk { text: String },
}

impl MessageUpdate {
    /// Echo a persisted runtime completion without presenting it as something
    /// the human typed. This is Grok's native ContentChunk metadata; task and
    /// subagent lifecycle projections provide the visible activity instead.
    /// Call only after classifying the durable message key, never from text.
    pub fn background_completion_payload(text: impl Into<String>) -> Value {
        let mut payload = Self::chunk_payload(USER_MESSAGE_CHUNK, text);
        payload["_meta"] = json!({"hideFromScrollback": true});
        payload
    }

    /// The `sessionUpdate` discriminator for this update.
    pub fn session_update_kind(&self) -> &'static str {
        match self {
            MessageUpdate::AgentMessageChunk { .. } => AGENT_MESSAGE_CHUNK,
            MessageUpdate::AgentThoughtChunk { .. } => AGENT_THOUGHT_CHUNK,
            MessageUpdate::UserMessageChunk { .. } => USER_MESSAGE_CHUNK,
        }
    }

    /// Render the Grok pager payload for this update. The chunk field name is
    /// `content` (the Grok decoder's expected name, not `contentBlock`).
    #[cfg(test)]
    pub fn to_payload(&self) -> Value {
        let text = match self {
            MessageUpdate::AgentMessageChunk { text }
            | MessageUpdate::AgentThoughtChunk { text }
            | MessageUpdate::UserMessageChunk { text } => text,
        };
        Self::chunk_payload(self.session_update_kind(), text)
    }

    /// Build the chunk payload for one `session_update_kind` discriminator
    /// plus delta text, without constructing an intermediate enum value.
    ///
    /// The live/durable reconciliation in the projection engine emits plain
    /// `(kind, delta)` pairs (a kind string observed from the response row
    /// or a durable row's chunk kind, plus the byte-exact suffix to send).
    /// `kind` must be one of
    /// [`AGENT_MESSAGE_CHUNK`]/[`AGENT_THOUGHT_CHUNK`]/[`USER_MESSAGE_CHUNK`]; any other value
    /// falls back to `agent_message_chunk` rather than fabricating an
    /// unknown discriminator on the wire.
    pub fn chunk_payload(kind: &str, text: impl Into<String>) -> Value {
        let kind = match kind {
            AGENT_THOUGHT_CHUNK => AGENT_THOUGHT_CHUNK,
            USER_MESSAGE_CHUNK => USER_MESSAGE_CHUNK,
            _ => AGENT_MESSAGE_CHUNK,
        };
        json!({
            "sessionUpdate": kind,
            "content": {
                "type": "text",
                "text": text.into(),
            },
        })
    }
}

/// The full set of streaming message updates for one request id, in transcript
/// order, plus the projection bookkeeping the engine needs to stamp `_meta`.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct MessageProjection {
    /// Ordered streaming updates: `user_message_chunk` echoes precede
    /// assistant output, and assistant rows project in `sequence` order.
    pub updates: Vec<MessageUpdate>,
    /// Durable chronology key per update, aligned 1:1 with `updates`: the
    /// row's `sequence` in the shared transcript sequence space (the same
    /// space `AgentToolCall.message_sequence` allocates from). `None` when
    /// the row carries no sequence — such updates sort after every
    /// positioned event of the family.
    pub chronology: Vec<Option<i64>>,
    /// Durable chunk identity per update, aligned 1:1 with `updates`:
    /// `"{message_key}:{update kind}:{per-row ordinal of that kind}"`. The
    /// live projection poll deduplicates streamed chunks by these keys, so
    /// two distinct rows carrying identical text both stream *and* one row's
    /// reasoning thought and body text are distinct chunks. An entry is
    /// empty only if the aligned update's row could not be identified (never
    /// happens today: every update comes from a decoded row).
    pub update_keys: Vec<String>,
    /// Legacy per-request generated-token observation, from the latest
    /// `AgentResponse.token_count` (u64, never fabricated). Zero when the
    /// request has no response row yet.
    pub total_tokens: u64,
    /// Whether the projected request is terminal (complete, error, or
    /// non-empty `interrupted_at`). A still-running request is not terminal
    /// and the engine keeps the pending prompt unresolved.
    pub terminal: bool,
    /// Terminal stop reason projection when `terminal` is true. This is an
    /// adapter projection, not a persisted field: `cancelled` for an
    /// interrupted turn, `error` for a failed one, `end_turn` otherwise.
    pub stop_reason: Option<&'static str>,
    /// Context window tokens used to bound `totalTokens`; falls back to the
    /// catalog default when the bound configuration did not supply one.
    pub context_window_tokens: u64,
    /// The live `AgentResponse` streaming tail: the current request's
    /// in-flight `content`/`reasoning` snapshot plus the durable progress
    /// counters and materialization pointers the live/durable
    /// reconciliation needs. All fields are `None`/empty when the request
    /// has no response row yet (or the response row exists but has never
    /// streamed a tail).
    pub live_tail: LiveResponseTail,
    /// Composite history of the response row, when it was readable: the
    /// ordered chain of every `update_AgentResponse` commit (by
    /// `(height, cid)`), each with its reconstructed snapshot. Empty when
    /// the request has no response row; `None` when a row exists but its
    /// history could not be read/validated (the live pass then fails
    /// closed: it never plans a continuation against a history it cannot
    /// prove).
    pub history: Option<Vec<CompositeSnapshot>>,
    /// Document whose validated history supplied this projection. `None`
    /// when no response exists or its history was unavailable.
    pub response_doc_id: Option<String>,
    /// Inclusive durable transcript high-water proved by this projection.
    /// The caller commits it only after every event in the batch sends.
    pub message_sequence_high_water: Option<i64>,
    /// Durable start of the request's response stream. The runtime creates
    /// `AgentResponse` immediately before entering inference, so this is the
    /// best durable start for the first model generation.
    pub response_started_at_ms: Option<i64>,
    /// Durable terminal timestamp bounds the retained streaming tail during
    /// replay. Arrival time is not historical generation time.
    pub response_ended_at_ms: Option<i64>,
    /// Request-local transcript timestamps observed by this bounded read.
    /// Projection retains these across polls to derive the start of later
    /// tool-loop generations from the preceding durable input row.
    pub timeline: Vec<MessageTimelineRow>,
}

/// Timestamp-bearing identity of one request-local transcript row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct MessageTimelineRow {
    pub sequence: i64,
    pub message_key: String,
    pub timestamp_ms: Option<i64>,
}

/// The live streaming tail of the request's `AgentResponse` row.
///
/// `AgentResponse.content`/`reasoning` are the runtime's *live* streaming
/// snapshot of the current assistant segment (cleared on ToolResult,
/// materialization, retraction, interruption, and error), while the
/// per-request `progress_seq`/`reasoning_progress_seq` counters advance on
/// every live tail write. The projection plans its live deltas against
/// exactly these fields: the tail text, the progress counter observed with
/// it (distinguishing a stale identical read from a genuine later identical
/// rewrite), the final row's `materialized_message_sequence` (binding the
/// live stream to the durable `AgentMessage` row it became), and the
/// current assistant row's `sequence` (the durable chronology position of
/// the live segment, so live text that produced a tool call in the same
/// poll sorts before that call).
#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct LiveResponseTail {
    /// Live content tail (`AgentResponse.content`), verbatim.
    pub content: Option<String>,
    /// Live reasoning tail (`AgentResponse.reasoning`), verbatim.
    pub reasoning: Option<String>,
    /// Durable content-progress counter observed with this response row.
    pub progress_seq: Option<u64>,
    /// Durable reasoning-progress counter observed with this response row.
    pub reasoning_progress_seq: Option<u64>,
    /// Sequence of the durable assistant `AgentMessage` row this live
    /// stream materialized into, when materialization happened.
    pub materialized_message_sequence: Option<i64>,
    /// Sequence of the current assistant `AgentMessage` row (the row the
    /// live tail belongs to), when one exists. Used as the live segment's
    /// chronology position.
    pub assistant_sequence: Option<i64>,
    /// Whether a response row exists for the request at all. `false` only
    /// when the request has no `AgentResponse` row yet; a row with an
    /// empty/never-streamed tail still counts as present. The durable-pass
    /// deferral of the unstamped newest assistant row requires a live
    /// segment, and a live segment is only meaningful against a response
    /// row, so deferral is gated on this flag.
    pub response_present: bool,
}

/// One composite commit of the response row's history with the snapshot
/// it reconstructs to: the authoritative per-response event record.
///
/// DefraDB records every `update_AgentResponse` mutation — including a
/// byte-identical no-op rewrite — as a new composite (`fieldName == "_C"`)
/// commit on the document's DAG. That record is the only signal that can
/// separate a no-op identical rewrite (which must not re-emit) from a
/// missed tail reset followed by a byte-identical new segment (which must
/// re-emit in full): both present as identical bytes with an advanced
/// `reasoning_progress_seq`, and only the reset's *own* commit (empty
/// tail, unchanged seqs) distinguishes them.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct CompositeSnapshot {
    /// The composite commit's CID: the exact identity of one history
    /// event, tracked by the projection cursor so progress is keyed by
    /// commit identity, never by height alone (a replaced branch reuses
    /// heights).
    pub cid: String,
    /// Composite height: monotonically increasing along a linear chain.
    pub height: i64,
    /// The snapshot the commit reconstructs to, verbatim.
    snapshot: ResponseRow,
}

impl CompositeSnapshot {
    /// The snapshot's live content tail, empty when absent (the runtime
    /// writes `""` on a tail reset; a missing field decodes as `None` —
    /// both mean the content segment is not open in this snapshot).
    pub fn content(&self) -> &str {
        self.snapshot.content.as_deref().unwrap_or("")
    }

    /// The snapshot's live reasoning tail, empty when absent (same
    /// convention as [`Self::content`]).
    pub fn reasoning(&self) -> &str {
        self.snapshot.reasoning.as_deref().unwrap_or("")
    }

    /// Durable content progress observed at this exact commit.
    pub fn progress_seq(&self) -> Option<u64> {
        self.snapshot
            .progress_seq
            .and_then(|value| u64::try_from(value).ok())
    }

    /// Durable reasoning progress observed at this exact commit.
    pub fn reasoning_progress_seq(&self) -> Option<u64> {
        self.snapshot
            .reasoning_progress_seq
            .and_then(|value| u64::try_from(value).ok())
    }

    /// Sequence of the durable assistant row materialized by this commit.
    pub fn materialized_message_sequence(&self) -> Option<i64> {
        self.snapshot.materialized_message_sequence
    }
}

/// Latest `AgentResponse` row for the request. The runtime writes exactly one
/// response per request; "latest" guards against a retry-replaced row.
#[derive(Clone, Debug, Deserialize, PartialEq)]
struct ResponseRow {
    /// The document id: the key the composite history queries address.
    #[serde(default, rename = "_docID")]
    doc_id: Option<String>,
    request_id: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    error_message: Option<String>,
    #[serde(default)]
    token_count: Option<i64>,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    reasoning: Option<String>,
    #[serde(default)]
    progress_seq: Option<i64>,
    #[serde(default)]
    reasoning_progress_seq: Option<i64>,
    #[serde(default)]
    materialized_at: Option<String>,
    #[serde(default)]
    materialized_message_sequence: Option<i64>,
    #[serde(default)]
    created_at: Option<String>,
    #[serde(default)]
    completed_at: Option<String>,
    #[serde(default)]
    interrupted_at: Option<String>,
}

/// One `AgentMessage` transcript row scoped to the request.
#[derive(Clone, Debug, Deserialize)]
struct MessageRow {
    message_key: String,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    sequence: Option<i64>,
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    reasoning: Option<String>,
    #[serde(default)]
    timestamp: Option<String>,
}

/// The query execution seam this leaf reads through.
///
/// Production always executes through the embedded node. The seam exists so
/// tests can interpose between the loader's reads (discovery → history →
/// `AgentMessage` rows) and prove the read *order*, which no embedded-node
/// hook can observe: the runtime persists an assistant row before resetting
/// the live tail, so once the history tip is loaded and fixed, a materialized
/// row that appears between the history read and the row read is guaranteed
/// to be present in the rows read after. `QuerySink` is internal to this
/// leaf (the public [`project_messages`] entry point keeps the
/// `Arc<EmbeddedNode>` signature); the shared `execute` helper below keeps
/// every read on one seam so ordering regressions cannot hide behind a
/// direct `node.execute` call. The returned future is `Send` so the
/// loader's futures stay `Send` end to end without a proc-macro crate.
pub(super) trait QuerySink: Send + Sync {
    fn execute(
        &self,
        query: &str,
    ) -> impl std::future::Future<Output = defra_node::QueryResponse> + Send;
}

/// The production sink: the embedded node itself.
struct NodeSink<'a> {
    node: &'a Arc<EmbeddedNode>,
}

impl QuerySink for NodeSink<'_> {
    async fn execute(&self, query: &str) -> defra_node::QueryResponse {
        self.node.execute(query).await
    }
}

/// Project the streaming message chunks for one request id.
///
/// Bounded and request-id-scoped: the query set is exactly
/// 1. one `AgentResponse` query for the latest row of this request id,
/// 2. one composite-history load of that row's document (its `_commits`
///    plus one snapshot reconstruction),
/// 3. fixed-size `AgentMessage` pages from the latest mutable sequence,
///
/// issued in that order: the history tip is loaded and fixed *before* the
/// `AgentMessage` rows are read (the runtime persists the assistant row
/// before a tail reset, so a reset observed in the fixed history implies
/// its materialized row is already in the rows read after).
///
/// It never replays the session, never duplicates durable materialization
/// (the projection is read-only), and every payload is a fresh notification
/// value. Returns an empty projection when the request has no rows.
pub(super) async fn project_messages(
    node: &Arc<EmbeddedNode>,
    history_observation: &mut HistoryObservation,
    message_sequence_high_water: Option<i64>,
    request_id: &str,
    context_window_tokens: u64,
) -> Result<MessageProjection> {
    let sink = NodeSink { node };
    project_messages_with_sink(
        &sink,
        history_observation,
        message_sequence_high_water,
        request_id,
        context_window_tokens,
    )
    .await
}

/// The loader body on one query sink; see [`project_messages`] for the
/// contract and [`QuerySink`] for why the seam is separated out.
async fn project_messages_with_sink<S: QuerySink>(
    sink: &S,
    history_observation: &mut HistoryObservation,
    message_sequence_high_water: Option<i64>,
    request_id: &str,
    context_window_tokens: u64,
) -> Result<MessageProjection> {
    // Read order (fail-closed selection depends on it): discover the
    // response row (1), then load and fix the authoritative history tip
    // (2a/2b), and only then read the `AgentMessage` rows (3). The
    // runtime persists the assistant row *before* resetting the tail, so
    // once the tip is fixed, a reset observed in the history implies the
    // materialized row is already durable in the rows read after: the
    // live segment's chronology position can never associate a reset
    // with a stale row list.
    let response = sink.execute(&latest_response_query(request_id)).await;
    ensure_no_errors(&response, "grok shim message response query")?;
    let response_row = decode_response_row(&response);

    // Load and fix the history tip before any `AgentMessage` read.
    let history: Option<Vec<CompositeSnapshot>> = match response_row.as_ref() {
        Some(row) => {
            let doc_id = row.doc_id.as_deref().and_then(nonempty).map(str::to_owned);
            match doc_id {
                Some(doc_id) => {
                    observe_history_with_sink(sink, history_observation, &doc_id, request_id)
                        .await?
                }
                // A row without a readable `_docID` cannot have its history
                // proven: fail closed with `None` (the live pass treats an
                // unprovable history as no history, never guessing).
                None => None,
            }
        }
        // No response row at all: there is nothing to prove, and an empty
        // (not `None`) history records exactly that.
        None => Some(Vec::new()),
    };

    let (rows, message_sequence_high_water) =
        load_incremental_message_rows(sink, message_sequence_high_water, request_id).await?;

    let context_window_tokens = effective_context_window_tokens(context_window_tokens);

    let mut updates = Vec::new();
    let mut update_keys = Vec::new();
    let mut chronology = Vec::new();
    for row in &rows {
        // Defense-in-depth re-check of the request scoping: the query filter
        // already guarantees it, but a widened filter must not leak other
        // requests' transcript rows into this projection.
        if row.request_id.as_deref().and_then(nonempty) != Some(request_id) {
            continue;
        }
        let before = updates.len();
        project_row(row, &mut updates);
        // One durable identity per update that row produced, aligned 1:1
        // with `updates` so the live poll can dedupe by durable identity.
        // The identity is chunk-level — `message_key` plus the update kind
        // plus the per-row ordinal of that kind — because one row can emit
        // more than one chunk (a reasoning thought plus its body text): a
        // row-level key would let the thought mark the body text as already
        // streamed and silently drop it. The kind plus ordinal keeps two
        // distinct rows with identical text emitting both times while still
        // distinguishing a row's thought from its text.
        let mut kinds_seen: std::collections::BTreeMap<&'static str, u64> =
            std::collections::BTreeMap::new();
        for update in &updates[before..] {
            let ordinal = {
                let counter = kinds_seen.entry(update.session_update_kind()).or_default();
                *counter += 1;
                *counter
            };
            update_keys.push(format!(
                "{}:{}:{}",
                row.message_key,
                update.session_update_kind(),
                ordinal
            ));
            chronology.push(row.sequence);
        }
    }

    // Response selection fails closed: only the last snapshot of the
    // *validated* history may supply the live tail, `totalTokens`,
    // terminality, or stop reason. A response row whose history could not
    // be read or proven exposes no live bytes, no tokens, and never
    // terminal state — the durable `AgentMessage` projection continues and
    // polling may retry; the discovery row is never a fallback. With no
    // response row at all there is no live tail either (the default).
    let (live_tail, authoritative_row) = match history.as_ref().and_then(|h| h.last()) {
        Some(tip) => {
            let authoritative = tip.snapshot.clone();
            let live_tail = LiveResponseTail {
                content: authoritative.content.clone(),
                reasoning: authoritative.reasoning.clone(),
                progress_seq: authoritative
                    .progress_seq
                    .and_then(|seq| u64::try_from(seq.max(0)).ok()),
                reasoning_progress_seq: authoritative
                    .reasoning_progress_seq
                    .and_then(|seq| u64::try_from(seq.max(0)).ok()),
                materialized_message_sequence: authoritative.materialized_message_sequence,
                assistant_sequence: rows
                    .iter()
                    .rev()
                    .find(|row| {
                        row.request_id.as_deref().and_then(nonempty) == Some(request_id)
                            && row.role.as_deref().and_then(nonempty) == Some("assistant")
                    })
                    .and_then(|row| row.sequence),
                response_present: true,
            };
            (live_tail, Some(authoritative))
        }
        // No proven tip: either no response row exists (history is
        // `Some(vec![])`, and the default tail is already correct) or a row
        // exists but its history is unprovable (history is `None`), and
        // selection fails closed either way — the discovery row is never a
        // fallback.
        None => (LiveResponseTail::default(), None),
    };

    // Tokens and terminality come from the validated tip only; an
    // unprovable history reports zero tokens and a still-running turn so
    // polling retries instead of resolving the prompt.
    let total_tokens = authoritative_row
        .as_ref()
        .and_then(|row| row.token_count)
        .and_then(|tokens| u64::try_from(tokens.max(0)).ok())
        .unwrap_or(0);

    let (terminal, stop_reason) = match authoritative_row.as_ref() {
        Some(row) if row.is_terminal() => (true, Some(row.stop_reason())),
        _ => (false, None),
    };

    let response_doc_id = authoritative_row
        .as_ref()
        .and_then(|row| row.doc_id.clone());
    let response_started_at_ms = authoritative_row
        .as_ref()
        .and_then(|row| row.created_at.as_deref())
        .and_then(rfc3339_millis);
    let response_ended_at_ms = authoritative_row
        .as_ref()
        .filter(|row| row.is_terminal())
        .and_then(|row| {
            row.completed_at
                .as_deref()
                .and_then(rfc3339_millis)
                .or_else(|| row.interrupted_at.as_deref().and_then(rfc3339_millis))
        });
    let timeline = rows
        .iter()
        .filter_map(|row| {
            Some(MessageTimelineRow {
                sequence: row.sequence?,
                message_key: row.message_key.clone(),
                timestamp_ms: row.timestamp.as_deref().and_then(rfc3339_millis),
            })
        })
        .collect();

    Ok(MessageProjection {
        updates,
        update_keys,
        chronology,
        total_tokens,
        terminal,
        stop_reason,
        context_window_tokens,
        live_tail,
        history,
        response_doc_id,
        message_sequence_high_water,
        response_started_at_ms,
        response_ended_at_ms,
        timeline,
    })
}

fn rfc3339_millis(value: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|timestamp| timestamp.timestamp_millis())
}

/// Project one transcript row into ordered streaming updates.
///
/// The persisted `content` blob is decoded through
/// `decode_persisted_message` (role-aware, with plain-text fallback) and only
/// its text blocks stream. Assistant reasoning streams as
/// `agent_thought_chunk` before the body so the pager sees thought-then-text
/// per row; user rows echo as `user_message_chunk` and never include
/// tool-result blocks (tool results are the tool leaf's domain).
fn project_row(row: &MessageRow, updates: &mut Vec<MessageUpdate>) {
    let role = row.role.as_deref().and_then(nonempty).unwrap_or_default();
    let blob = row
        .content
        .as_deref()
        .and_then(nonempty)
        .unwrap_or_default();
    if blob.is_empty() {
        return;
    }
    let message = decode_persisted_message(role, blob);

    match &message {
        Message::Assistant { content, .. } => {
            // Reasoning deltas first (thought-before-text per row), then body.
            // Chunk text streams verbatim: only whitespace-only blocks are
            // skipped, never trimmed, so the accumulated pager text equals
            // the durable message text exactly.
            for item in content {
                if let AssistantContent::Reasoning(reasoning) = item {
                    for text in reasoning_texts(reasoning) {
                        push_nonempty(updates, MessageUpdate::AgentThoughtChunk { text });
                    }
                }
            }
            // #492: the durable reasoning copy may live only in
            // `AgentMessage.reasoning` when the response tail was cleared on
            // finalize; project it so a finished row still shows its thought.
            // It streams before the body text, matching the established
            // thought-before-text contract for every assistant row.
            if content
                .iter()
                .all(|item| !matches!(item, AssistantContent::Reasoning(_)))
            {
                if let Some(reasoning) = row.reasoning.as_deref().and_then(streamable_owned) {
                    push_nonempty(
                        updates,
                        MessageUpdate::AgentThoughtChunk { text: reasoning },
                    );
                }
            }
            for item in content {
                if let AssistantContent::Text(text) = item {
                    if let Some(text) = streamable_owned(&text.text) {
                        updates.push(MessageUpdate::AgentMessageChunk { text });
                    }
                }
            }
        }
        Message::User { content } => {
            for item in content {
                if let UserContent::Text(text) = item {
                    if let Some(text) = streamable_owned(&text.text) {
                        updates.push(MessageUpdate::UserMessageChunk { text });
                    }
                }
            }
        }
        Message::System { .. } => {
            // System messages are not persisted in session history; a row
            // claiming that role projects as nothing.
        }
    }
}

fn push_nonempty(updates: &mut Vec<MessageUpdate>, update: MessageUpdate) {
    let is_empty = match &update {
        MessageUpdate::AgentMessageChunk { text }
        | MessageUpdate::AgentThoughtChunk { text }
        | MessageUpdate::UserMessageChunk { text } => text.trim().is_empty(),
    };
    if !is_empty {
        updates.push(update);
    }
}

/// Text pieces of a reasoning block, rendered the way the transcript
/// presents them (plain text and summary text stream; encrypted/redacted
/// payloads are opaque and never stream as thought text).
fn reasoning_texts(reasoning: &gents_protocol::message::Reasoning) -> Vec<String> {
    use gents_protocol::message::ReasoningContent;
    reasoning
        .content
        .iter()
        .filter_map(|item| match item {
            ReasoningContent::Text { text, .. } | ReasoningContent::Summary(text) => {
                streamable_owned(text)
            }
            ReasoningContent::Encrypted(_) | ReasoningContent::Redacted { .. } => None,
        })
        .collect()
}

/// Streamable chunk text: verbatim (never trimmed) but whitespace-only
/// blocks are skipped so a blank block does not emit an empty chunk.
fn streamable_owned(value: &str) -> Option<String> {
    (!value.trim().is_empty()).then(|| value.to_string())
}

impl ResponseRow {
    /// A request is terminal only when the response reached a terminal
    /// status (`complete`/`error`) or carries a non-empty `interrupted_at`.
    /// Blank, running, and in-flight statuses are never terminal.
    fn is_terminal(&self) -> bool {
        if self.interrupted_at.as_deref().and_then(nonempty).is_some() {
            return true;
        }
        matches!(
            self.status.as_deref().and_then(nonempty),
            Some("complete") | Some("error")
        )
    }

    /// Adapter-projected stop reason for a terminal response. Never persisted.
    fn stop_reason(&self) -> &'static str {
        if self.interrupted_at.as_deref().and_then(nonempty).is_some() {
            return "cancelled";
        }
        if self.status.as_deref().and_then(nonempty) == Some("error") {
            return "error";
        }
        "end_turn"
    }
}

// ---------------------------------------------------------------------------
// Queries and decoding
// ---------------------------------------------------------------------------

/// Latest `AgentResponse` row for the request id. The runtime writes one
/// response per request; ordering by `created_at` descending with a bound of
/// one row keeps the query bounded even if a retry replaced the row.
fn latest_response_query(request_id: &str) -> String {
    format!(
        r#"{{
            AgentResponse(
                filter: {{ request_id: {{ _eq: "{request_id}" }} }},
                order: {{ created_at: DESC }},
                limit: 1
            ) {{ {RESPONSE_FIELDS} }}
        }}"#,
        request_id = escape_graphql_string(request_id),
    )
}

/// `AgentMessage` rows for the request id in transcript order. Ordered by
/// `sequence` so the streamed chunks follow the durable transcript order
/// (user echo before assistant output).
fn request_messages_query(request_id: &str, min_sequence: i64) -> String {
    format!(
        r#"{{
            AgentMessage(
                filter: {{
                    request_id: {{ _eq: "{request_id}" }},
                    sequence: {{ _gte: {min_sequence} }}
                }},
                order: {{ sequence: ASC }},
                limit: {MESSAGE_BATCH_LIMIT}
            ) {{ {MESSAGE_FIELDS} }}
        }}"#,
        request_id = escape_graphql_string(request_id),
    )
}

const MESSAGE_BATCH_LIMIT: usize = 64;

/// Load the mutable transcript tail plus newly appended rows in fixed pages.
/// The cursor advances only after every page has decoded and validated, so a
/// query/shape failure retries from the same inclusive sequence next poll.
async fn load_incremental_message_rows<S: QuerySink>(
    sink: &S,
    committed_high_water: Option<i64>,
    request_id: &str,
) -> Result<(Vec<MessageRow>, Option<i64>)> {
    let mut floor = committed_high_water.unwrap_or(0);
    let mut rows = Vec::new();
    let mut candidate_high_water = committed_high_water;

    loop {
        let response = sink
            .execute(&request_messages_query(request_id, floor))
            .await;
        ensure_no_errors(&response, "grok shim message rows query")?;
        let values = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentMessage"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if values.len() > MESSAGE_BATCH_LIMIT {
            return Err(anyhow!(
                "grok shim message rows query exceeded its fixed batch limit"
            ));
        }

        let batch_len = values.len();
        let batch = decode_message_rows_values(values)?;
        let mut previous = None;
        for row in &batch {
            if row.request_id.as_deref().and_then(nonempty) != Some(request_id) {
                return Err(anyhow!(
                    "grok shim message rows query returned a different request"
                ));
            }
            let sequence = row
                .sequence
                .ok_or_else(|| anyhow!("grok shim AgentMessage row has no sequence"))?;
            if sequence < floor || previous.is_some_and(|prior| sequence <= prior) {
                return Err(anyhow!(
                    "grok shim message rows query returned an unordered sequence"
                ));
            }
            previous = Some(sequence);
            candidate_high_water =
                Some(candidate_high_water.map_or(sequence, |high| high.max(sequence)));
        }
        rows.extend(batch);

        if batch_len < MESSAGE_BATCH_LIMIT {
            break;
        }
        let last = previous.ok_or_else(|| {
            anyhow!("grok shim full message page contained no decodable sequence")
        })?;
        floor = last
            .checked_add(1)
            .ok_or_else(|| anyhow!("grok shim AgentMessage sequence overflow"))?;
    }

    Ok((rows, candidate_high_water))
}

const RESPONSE_FIELDS: &str = "
    _docID
    request_id
    status
    error_message
    token_count
    content
    reasoning
    progress_seq
    reasoning_progress_seq
    materialized_at
    materialized_message_sequence
    created_at
    completed_at
    interrupted_at
";

const MESSAGE_FIELDS: &str = "
    message_key
    request_id
    sequence
    role
    content
    reasoning
    timestamp
";

fn decode_response_row(response: &defra_node::QueryResponse) -> Option<ResponseRow> {
    let row = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentResponse"))
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .cloned()?;
    match serde_json::from_value::<ResponseRow>(row) {
        Ok(row) => Some(row),
        Err(error) => {
            tracing::debug!(
                %error,
                "grok shim skipped an undecodable AgentResponse row"
            );
            None
        }
    }
}

fn decode_message_rows_values(values: Vec<Value>) -> Result<Vec<MessageRow>> {
    values
        .into_iter()
        .map(|row| {
            serde_json::from_value::<MessageRow>(row)
                .map_err(|error| anyhow!("grok shim AgentMessage row did not decode: {error}"))
        })
        .collect()
}

/// One raw `_commits` row of the response document.
///
/// `raw_value` keeps the exact JSON object the node returned (the row's
/// byte identity for the duplicate-CID rule); the flattened fields are
/// the validated, typed view of it.
#[derive(Clone, Debug, Deserialize, PartialEq)]
struct CompositeCommitRow {
    #[serde(default)]
    cid: String,
    #[serde(default)]
    height: i64,
    #[serde(default, rename = "fieldName")]
    field_name: String,
    #[serde(default)]
    heads: Vec<CompositeCommitHead>,
    #[serde(default)]
    links: Vec<CompositeCommitHead>,
    /// The exact raw JSON object this row was decoded from: the canonical
    /// identity the incremental observer caches and the duplicate-CID rule
    /// compares. Never serialized into queries.
    #[serde(skip)]
    raw_value: Value,
}

/// One parent/field link of a composite commit (`_C` heads carry the
/// composite parent; `links` carry the field blocks the composite ties
/// together). `height` is `None` for a head/link whose block could not be
/// loaded — DefraDB renders such an unresolved reference without a height,
/// and this leaf rejects it rather than guessing at the chain.
#[derive(Clone, Debug, Deserialize, PartialEq)]
struct CompositeCommitHead {
    #[serde(default)]
    cid: String,
    #[serde(default)]
    height: Option<i64>,
    #[serde(default, rename = "fieldName")]
    field_name: String,
}

impl CompositeCommitHead {
    /// A well-formed, resolvable reference: a nonempty CID, a loaded block
    /// height (strictly positive — DefraDB numbers blocks from 1, and a
    /// non-positive or missing height means the block could not be
    /// resolved), and the field name of the block it points at. Anything
    /// else is an unresolved or malformed reference.
    fn is_resolvable(&self) -> bool {
        !self.cid.trim().is_empty()
            && self.height.is_some_and(|height| height > 0)
            && !self.field_name.trim().is_empty()
    }
}

/// The fixed height-window size of the incremental history observer: every
/// `_commits` read addresses at most this many consecutive composite
/// heights (plus its field rows), so an unchanged poll's read work is
/// bounded by the window, never by the age of the history.
///
/// The pinned DefraDB `_commits` API supports indexed height ranges
/// (`filter: { height: { _gte, _lt } }` plus `docID`) and no useful
/// depth/limit knob, so a window is expressed as a half-open height range.
/// Every composite in a window plus its field rows fit in one query reply;
/// the window boundary composite (the height just below the window) is
/// re-read as *boundary evidence* so exact continuity with the previously
/// validated chain can be proven on suffix reads.
pub(super) const HISTORY_WINDOW_HEIGHTS: i64 = 64;

const HISTORY_CID_BATCH: usize = 64;

/// Exact identity of one raw commit row retained as validation evidence.
///
/// `height` and `field_name` are duplicated outside `raw` deliberately:
/// reference resolution is an exact tuple check, while `raw` detects a CID
/// that is later served with any different metadata or ancestry.
#[derive(Clone, Debug, PartialEq)]
struct RawCommitIdentity {
    height: i64,
    field_name: String,
    raw: Value,
}

/// Request-local incremental response/transcript observation state,
/// independent of outbound delivery anchors (spec point 1).
///
/// The observer proves the response document's composite history one fixed
/// height window at a time, fail-closed at every step, and exposes the
/// *ordered* sequence of newly validated snapshots — not only the tip — so
/// the projection can replay every unseen segment between polls (spec
/// point 3). Observation state survives delivery failures by design: a
/// failed send leaves the delivery anchors untouched and the next poll
/// re-derives the identical replay candidates from the same observation
/// (spec point 4).
///
/// Because a composite's head/link references resolve against *field rows
/// of much older heights* (probe-verified against the real embedded node:
/// a mutation's field rows share the composite's height, but their heads
/// point backward at ancient field blocks), the raw-row identity map must
/// persist across windows and polls, not just within one window. Only the
/// *retained chain* gets pruned to the delivery anchors; the raw-row cache
/// keeps every identity the chain still references plus a bounded suffix.
#[derive(Clone, Debug, Default)]
pub(super) struct HistoryObservation {
    /// The response document id this observation proves. A different doc id
    /// (a replaced/recreated response row) resets the observation: the new
    /// chain is replayed from scratch.
    doc_id: Option<String>,
    /// Request identity paired with `doc_id`; prevents accidental reuse of
    /// one request-local observer for another request with a shared row.
    request_id: Option<String>,
    /// Raw `_commits` identities needed by the retained composite frontier.
    /// A later composite may reference an evicted ancient field row; the
    /// observer rehydrates that exact CID before validation.
    raw_identities: BTreeMap<String, RawCommitIdentity>,
    /// The retained validated chain: every composite from the earliest
    /// delivery anchor (or the root) to the observed tip, in height order.
    /// Pruned each poll so an unchanged poll's retained state is bounded
    /// by the anchors' distance to the tip, never by history age.
    chain: Vec<CompositeSnapshot>,
    /// The validated tip: `(cid, height)` of the last chain entry.
    tip: Option<(String, i64)>,
}

impl HistoryObservation {
    /// The retained validated chain, in height order (spec point 3: the
    /// ordered sequence of validated snapshots, not only the tip).
    pub fn retained_chain(&self) -> &[CompositeSnapshot] {
        &self.chain
    }

    /// Whether the observation is pinned to this document (a fresh or
    /// replaced observation is not).
    fn pinned_to(&self, doc_id: &str, request_id: &str) -> bool {
        self.doc_id.as_deref() == Some(doc_id) && self.request_id.as_deref() == Some(request_id)
    }

    /// Drop snapshots strictly before a successfully delivered anchor.
    ///
    /// The caller invokes this only after its delivery cursor commits. Raw
    /// identities are pruned to the retained frontier; future ancient direct
    /// references are rehydrated by CID.
    pub fn retain_from(&mut self, anchor_cid: Option<&str>) {
        let Some(anchor_cid) = anchor_cid else {
            return;
        };
        let Some(index) = self.chain.iter().position(|entry| entry.cid == anchor_cid) else {
            return;
        };
        self.chain.drain(..index);
        prune_raw_identities(self);
    }

    /// Reset the observation to a fresh state for a new document.
    fn reset(&mut self, doc_id: &str, request_id: &str) {
        self.doc_id = Some(doc_id.to_string());
        self.request_id = Some(request_id.to_string());
        self.raw_identities.clear();
        self.chain.clear();
        self.tip = None;
    }
}

/// Observe the response document's composite history incrementally.
///
/// One poll reads exactly one fixed-height `_commits` window per observed
/// growth step plus the snapshot rows of the newly validated composites —
/// never the whole history (spec points 1-2). Fail-closed semantics cover
/// duplicate CIDs, positive reference
/// heights, exact `(cid,height,fieldName)` tuple resolution, parent
/// CID+height, one root, consecutive heights, >=1 field link, snapshot
/// count/doc/request identity): on any failure the observation returns
/// `None` and *keeps its previous validated state standing* (never
/// regressing to a partial view), so a later sound poll can recover
/// (spec point 2: never project from an unproved partial history).
///
/// The window for a fresh observation starts at the root; for a pinned
/// observation it starts one height *below* the validated tip, so the
/// boundary composite is re-read as evidence and its CID (plus its raw
/// row identity) must match the cached tip exactly — the proof that the
/// suffix continues the very chain the observation already validated.
/// A boundary mismatch, a gap, a fork, or a conflicting repeated CID
/// fails the whole observation closed.
#[allow(clippy::too_many_arguments)]
async fn observe_history_with_sink<S: QuerySink>(
    sink: &S,
    observation: &mut HistoryObservation,
    doc_id: &str,
    expected_request_id: &str,
) -> Result<Option<Vec<CompositeSnapshot>>> {
    let escaped_doc_id = escape_graphql_string(doc_id);

    // Pin one unique current composite head before scanning any windows.
    // `depth: 1` makes this query independent of history length; composite
    // selection remains a Rust-side check because DefraDB's filter is not a
    // trustworthy correctness boundary.
    let head_query = format!(
        r#"query {{
            _commits(docID: "{escaped_doc_id}", depth: 1) {{ {COMMIT_FIELDS} }}
        }}"#
    );
    let response = sink.execute(&head_query).await;
    ensure_no_errors(&response, "grok shim response composite head query")?;
    let Some(head_values) = extract_commit_values(&response) else {
        return Ok(None);
    };
    let mut head_probe = HistoryObservation::default();
    let Some(head_rows) = cache_raw_rows(&mut head_probe, head_values.clone(), doc_id) else {
        return Ok(None);
    };
    let composite_heads: Vec<_> = head_rows
        .values()
        .filter(|row| row.field_name == "_C")
        .cloned()
        .collect();
    let [pinned_head] = composite_heads.as_slice() else {
        tracing::debug!(
            doc_id = %doc_id,
            heads = composite_heads.len(),
            "response history does not have one unique composite head"
        );
        return Ok(None);
    };
    let pinned = (pinned_head.cid.clone(), pinned_head.height);

    // The common unchanged poll does only response discovery plus this
    // bounded head read (and the separately bounded message query). Validate
    // the returned head identity against the cache before returning, but do
    // not deep-clone the accumulated observer on this hot path.
    if observation.pinned_to(doc_id, expected_request_id)
        && observation.tip.as_ref() == Some(&pinned)
    {
        if head_probe.raw_identities.iter().any(|(cid, identity)| {
            observation
                .raw_identities
                .get(cid)
                .is_some_and(|cached| cached != identity)
        }) || observation.raw_identities.get(&pinned.0)
            != head_probe.raw_identities.get(&pinned.0)
        {
            tracing::debug!(doc_id = %doc_id, "response history head changed cached identity");
            return Ok(None);
        }
        return Ok(Some(observation.chain.clone()));
    }

    // Growth and replacement are staged in a shadow copy. Query errors and
    // validation failures leave the last proven observation byte-for-byte
    // untouched.
    let mut working = if observation.pinned_to(doc_id, expected_request_id) {
        observation.clone()
    } else {
        let mut fresh = HistoryObservation::default();
        fresh.reset(doc_id, expected_request_id);
        fresh
    };
    if cache_raw_rows(&mut working, head_values, doc_id).is_none() {
        return Ok(None);
    }

    while working.tip.as_ref() != Some(&pinned) {
        let base = working.tip.clone();
        let start_height = base.as_ref().map_or(1, |(_, height)| *height);
        if start_height > pinned.1 {
            tracing::debug!(doc_id = %doc_id, "response history head regressed below validated tip");
            return Ok(None);
        }
        let window_end = start_height.saturating_add(HISTORY_WINDOW_HEIGHTS);
        let window_query = format!(
            r#"query {{
                _commits(
                    docID: "{escaped_doc_id}"
                    filter: {{ height: {{ _gte: {start_height}, _lt: {window_end} }} }}
                ) {{ {COMMIT_FIELDS} }}
            }}"#
        );
        let response = sink.execute(&window_query).await;
        ensure_no_errors(
            &response,
            "grok shim response composite history window query",
        )?;
        let Some(mut values) = extract_commit_values(&response) else {
            return Ok(None);
        };
        // Commits written after the head pin can land in the last fixed
        // window. They belong to the next poll, not this pinned snapshot.
        values.retain(|value| {
            value
                .get("height")
                .and_then(Value::as_i64)
                .is_some_and(|height| height <= pinned.1)
        });
        let Some(rows) = cache_raw_rows(&mut working, values, doc_id) else {
            return Ok(None);
        };
        let mut composites: Vec<_> = rows
            .values()
            .filter(|row| row.field_name == "_C")
            .cloned()
            .collect();
        composites.sort_by(|left, right| (left.height, &left.cid).cmp(&(right.height, &right.cid)));

        // A suffix poll overlaps the exact prior tip height. That boundary
        // must be present once, with the same CID and raw identity; it is
        // evidence only and is filtered before suffix validation/replay.
        if let Some((base_cid, base_height)) = base.as_ref() {
            let boundary: Vec<_> = composites
                .iter()
                .filter(|commit| commit.height == *base_height)
                .collect();
            if boundary.len() != 1 || boundary[0].cid != *base_cid {
                tracing::debug!(doc_id = %doc_id, "response history overlap does not match validated tip");
                return Ok(None);
            }
            composites.retain(|commit| commit.height > *base_height);
        }
        if composites.is_empty() {
            tracing::debug!(doc_id = %doc_id, "response history window made no progress toward pinned head");
            return Ok(None);
        }

        if !hydrate_direct_references(sink, &mut working, doc_id, &composites).await? {
            return Ok(None);
        }
        if !composite_references_resolve(&working, &composites, doc_id) {
            return Ok(None);
        }
        let Some(ordered) = validate_window_chain(&composites, doc_id, base.as_ref()) else {
            return Ok(None);
        };
        let Some(new_snapshots) =
            hydrate_snapshots_bounded(sink, &ordered, doc_id, expected_request_id).await?
        else {
            return Ok(None);
        };
        let Some(last) = new_snapshots.last() else {
            return Ok(None);
        };
        if last.height > pinned.1 {
            return Ok(None);
        }
        working.tip = Some((last.cid.clone(), last.height));
        working.chain.extend(new_snapshots);
    }

    // The scan must terminate at the exact pinned CID, not merely its
    // numeric height (forks may reuse heights).
    if working.tip.as_ref() != Some(&pinned) {
        return Ok(None);
    }
    prune_raw_identities(&mut working);
    *observation = working;
    Ok(Some(observation.chain.clone()))
}

/// Keep only identities required to validate the retained composite suffix.
/// A later composite that reaches farther back is hydrated directly by CID,
/// so correctness does not require an age-sized cache.
fn prune_raw_identities(observation: &mut HistoryObservation) {
    let mut keep = std::collections::BTreeSet::new();
    for snapshot in &observation.chain {
        keep.insert(snapshot.cid.clone());
        let Some(identity) = observation.raw_identities.get(&snapshot.cid) else {
            continue;
        };
        let Ok(commit) = serde_json::from_value::<CompositeCommitRow>(identity.raw.clone()) else {
            continue;
        };
        for reference in commit.heads.iter().chain(commit.links.iter()) {
            keep.insert(reference.cid.clone());
        }
    }
    observation
        .raw_identities
        .retain(|cid, _| keep.contains(cid));
}

const COMMIT_FIELDS: &str = "
    cid
    height
    fieldName
    heads { cid height fieldName }
    links { cid height fieldName }
";

/// Decode a bounded batch and merge every composite and field row into the
/// shadow observation. Conflicting repeats fail the entire poll closed.
fn cache_raw_rows(
    observation: &mut HistoryObservation,
    values: Vec<Value>,
    doc_id: &str,
) -> Option<BTreeMap<String, CompositeCommitRow>> {
    let mut decoded = BTreeMap::<String, CompositeCommitRow>::new();
    for value in values {
        let mut row = match serde_json::from_value::<CompositeCommitRow>(value.clone()) {
            Ok(row) => row,
            Err(error) => {
                tracing::debug!(%error, doc_id = %doc_id, "response history row did not decode");
                return None;
            }
        };
        if row.cid.trim().is_empty() || row.height <= 0 || row.field_name.trim().is_empty() {
            tracing::debug!(doc_id = %doc_id, "response history row has an invalid identity tuple");
            return None;
        }
        row.raw_value = value.clone();
        let identity = RawCommitIdentity {
            height: row.height,
            field_name: row.field_name.clone(),
            raw: value,
        };
        if let Some(cached) = observation.raw_identities.get(&row.cid) {
            if cached != &identity {
                tracing::debug!(doc_id = %doc_id, cid = %row.cid, "response history CID changed identity");
                return None;
            }
        }
        if let Some(previous) = decoded.get(&row.cid) {
            if previous.raw_value != row.raw_value {
                tracing::debug!(doc_id = %doc_id, cid = %row.cid, "response history batch contains conflicting duplicate CID");
                return None;
            }
            continue;
        }
        decoded.insert(row.cid.clone(), row);
    }
    for row in decoded.values() {
        observation.raw_identities.insert(
            row.cid.clone(),
            RawCommitIdentity {
                height: row.height,
                field_name: row.field_name.clone(),
                raw: row.raw_value.clone(),
            },
        );
    }
    Some(decoded)
}

/// Fetch only directly referenced rows absent from the identity cache. CID
/// queries without `depth` return the addressed block itself, so field-row
/// ancestry is intentionally not traversed or required. Batching bounds the
/// query and response sizes even for wide composite commits.
async fn hydrate_direct_references<S: QuerySink>(
    sink: &S,
    observation: &mut HistoryObservation,
    doc_id: &str,
    composites: &[CompositeCommitRow],
) -> Result<bool> {
    let mut missing = std::collections::BTreeSet::<String>::new();
    for commit in composites {
        for reference in commit.heads.iter().chain(commit.links.iter()) {
            if !reference.is_resolvable() {
                return Ok(false);
            }
            if !observation.raw_identities.contains_key(&reference.cid) {
                missing.insert(reference.cid.clone());
            }
        }
    }
    let escaped_doc_id = escape_graphql_string(doc_id);
    let missing: Vec<_> = missing.into_iter().collect();
    for batch in missing.chunks(HISTORY_CID_BATCH) {
        let cid_list = batch
            .iter()
            .map(|cid| format!(r#""{}""#, escape_graphql_string(cid)))
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            r#"query {{
                _commits(docID: "{escaped_doc_id}", cid: [{cid_list}]) {{ {COMMIT_FIELDS} }}
            }}"#
        );
        let response = sink.execute(&query).await;
        ensure_no_errors(&response, "grok shim response referenced commit query")?;
        let Some(values) = extract_commit_values(&response) else {
            return Ok(false);
        };
        let Some(rows) = cache_raw_rows(observation, values, doc_id) else {
            return Ok(false);
        };
        if batch
            .iter()
            .any(|requested| !rows.contains_key(requested.as_str()))
        {
            tracing::debug!(doc_id = %doc_id, "response history CID hydration omitted a requested row");
            return Ok(false);
        }
        if rows.keys().any(|returned| !batch.contains(returned)) {
            tracing::debug!(doc_id = %doc_id, "response history CID hydration returned an unrequested row");
            return Ok(false);
        }
    }
    Ok(true)
}

fn composite_references_resolve(
    observation: &HistoryObservation,
    composites: &[CompositeCommitRow],
    doc_id: &str,
) -> bool {
    for commit in composites {
        for reference in commit.heads.iter().chain(commit.links.iter()) {
            let resolves = reference.is_resolvable()
                && observation
                    .raw_identities
                    .get(&reference.cid)
                    .is_some_and(|row| {
                        row.height == reference.height.unwrap_or_default()
                            && row.field_name == reference.field_name
                    });
            if !resolves {
                tracing::debug!(
                    doc_id = %doc_id,
                    cid = %commit.cid,
                    reference = %reference.cid,
                    "response history direct reference does not resolve exactly"
                );
                return false;
            }
        }
    }
    true
}

/// Reconstruct a suffix in fixed-size CID batches. Every batch preserves the
/// caller's CID order and is independently count/identity checked by
/// `assemble_snapshots` before any result reaches the observation.
async fn hydrate_snapshots_bounded<S: QuerySink>(
    sink: &S,
    commits: &[CompositeCommitRow],
    doc_id: &str,
    expected_request_id: &str,
) -> Result<Option<Vec<CompositeSnapshot>>> {
    let escaped_doc_id = escape_graphql_string(doc_id);
    let mut snapshots = Vec::with_capacity(commits.len());
    for batch in commits.chunks(HISTORY_CID_BATCH) {
        let cid_list = batch
            .iter()
            .map(|commit| format!(r#""{}""#, escape_graphql_string(&commit.cid)))
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            r#"query {{
                AgentResponse(
                    cid: [{cid_list}],
                    docID: "{escaped_doc_id}",
                    showDeleted: true
                ) {{ {RESPONSE_FIELDS} }}
            }}"#
        );
        let response = sink.execute(&query).await;
        ensure_no_errors(&response, "grok shim response history snapshot query")?;
        let values = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentResponse"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let Some(batch_snapshots) = assemble_snapshots(batch, values, doc_id, expected_request_id)
        else {
            return Ok(None);
        };
        snapshots.extend(batch_snapshots);
    }
    Ok(Some(snapshots))
}

/// Pair the reconstructed snapshots with the validated chain, in chain
/// order. The count and order must pair exactly (the runner preserves
/// input CID order), every snapshot must decode, reconstruct the expected
/// `_docID`, and carry `request_id == expected_request_id`; anything else
/// is an integrity failure (`None`).
fn assemble_snapshots(
    commits: &[CompositeCommitRow],
    snapshot_values: Vec<Value>,
    doc_id: &str,
    expected_request_id: &str,
) -> Option<Vec<CompositeSnapshot>> {
    if snapshot_values.len() != commits.len() {
        tracing::debug!(
            commits = commits.len(),
            snapshots = snapshot_values.len(),
            "response history reconstruction returned a mismatched snapshot count"
        );
        return None;
    }
    let mut history = Vec::with_capacity(commits.len());
    for (commit, value) in commits.iter().zip(snapshot_values) {
        let snapshot = match serde_json::from_value::<ResponseRow>(value) {
            Ok(row) => row,
            Err(error) => {
                tracing::debug!(
                    %error,
                    cid = %commit.cid,
                    "grok shim response history snapshot did not decode"
                );
                return None;
            }
        };
        let Some(snapshot_doc) = snapshot.doc_id.as_deref().and_then(nonempty) else {
            tracing::debug!(
                cid = %commit.cid,
                "response history snapshot reconstructed without a document id"
            );
            return None;
        };
        if snapshot_doc != doc_id {
            tracing::debug!(
                cid = %commit.cid,
                "response history snapshot reconstructed a different document"
            );
            return None;
        }
        if snapshot.request_id != expected_request_id {
            tracing::debug!(
                cid = %commit.cid,
                request_id = %snapshot.request_id,
                expected_request_id = %expected_request_id,
                "response history snapshot reconstructed a different request"
            );
            return None;
        }
        history.push(CompositeSnapshot {
            cid: commit.cid.clone(),
            height: commit.height,
            snapshot,
        });
    }
    Some(history)
}

/// Extract the raw `_commits` array from a history query response. The
/// array must be present: a response document that exists but whose commit
/// query returns no array is an integrity failure, never an empty history.
fn extract_commit_values(response: &defra_node::QueryResponse) -> Option<Vec<Value>> {
    response
        .data
        .as_ref()
        .and_then(|data| data.get("_commits"))
        .and_then(Value::as_array)
        .cloned()
}

/// Validate an exact linear `_C` composite chain *suffix* fail-closed and
/// return it ordered by height.
///
/// The window's composites are a suffix of the document's composite chain:
/// for a fresh observation (`base_height == None`) the suffix must be the
/// chain from its root (heights exactly 1, 2, 3, …, consecutive from 1,
/// one root, no sibling, no merge, resolvable field links, linear
/// parentage by CID *and* height); for a pinned observation
/// (`base == Some((tip_cid, tip_height))`) the suffix must continue at
/// `tip_height + 1`, and its first entry's sole composite parent must be that
/// exact cached tip by CID and height.
///
/// Every check is fail-closed (`None`): the observer can only plan
/// against a chronology it can prove end to end.
fn validate_window_chain(
    commits: &[CompositeCommitRow],
    doc_id: &str,
    base: Option<&(String, i64)>,
) -> Option<Vec<CompositeCommitRow>> {
    if commits.is_empty() {
        return None;
    }
    // The suffix's expected starting height: 1 for a fresh observation,
    // one past the validated tip for a pinned one.
    let expected_start = match base {
        Some((_, height)) => height + 1,
        None => 1,
    };
    // No sibling and no merge, before ordering: track each parent's
    // children across the whole suffix. A parent with two children is a
    // replaced branch; a commit with two `_C` parents is a merge.
    let mut children_of_parent: BTreeMap<&str, usize> = BTreeMap::new();
    for commit in commits {
        let parents = composite_parents(commit);
        if parents.len() > 1 {
            tracing::debug!(
                doc_id = %doc_id,
                cid = %commit.cid,
                parents = parents.len(),
                "response window commit carries multiple composite parents (merge)"
            );
            return None;
        }
        // A fresh suffix must have exactly one root (a root inside the
        // window whose parent was already validated is legitimate for a
        // pinned suffix, so the root count is only enforced on fresh
        // suffixes below).
        if let Some(parent) = parents.first() {
            let children = children_of_parent.entry(parent.cid.as_str()).or_default();
            *children += 1;
            if *children > 1 {
                tracing::debug!(
                    doc_id = %doc_id,
                    parent = %parent.cid,
                    "response window chain carries sibling branches"
                );
                return None;
            }
        }
    }
    // A fresh suffix carries exactly one root; a pinned suffix's base is
    // the cached tip, so a root inside it would be a second root of the
    // document's chain (unprovable).
    let roots = commits
        .iter()
        .filter(|commit| composite_parents(commit).is_empty())
        .count();
    let expected_roots = if base.is_some() { 0 } else { 1 };
    if roots != expected_roots {
        tracing::debug!(
            doc_id = %doc_id,
            roots = roots,
            "response window chain does not have the expected single root"
        );
        return None;
    }
    for (index, commit) in commits.iter().enumerate() {
        // Positive and consecutive heights continuing the base exactly.
        let expected_height = expected_start + (index as i64);
        if commit.height != expected_height {
            tracing::debug!(
                doc_id = %doc_id,
                cid = %commit.cid,
                height = commit.height,
                expected = expected_height,
                "response window chain has a non-consecutive or non-continuing height"
            );
            return None;
        }
        let parents = composite_parents(commit);
        if index == 0 {
            if let Some((base_cid, base_height)) = base {
                let [parent] = parents[..] else {
                    tracing::debug!(
                        doc_id = %doc_id,
                        cid = %commit.cid,
                        parents = parents.len(),
                        "response window suffix does not have one cached-tip parent"
                    );
                    return None;
                };
                if parent.cid != *base_cid || parent.height != Some(*base_height) {
                    tracing::debug!(
                        doc_id = %doc_id,
                        cid = %commit.cid,
                        parent = %parent.cid,
                        parent_height = ?parent.height,
                        expected_parent = %base_cid,
                        expected_height = *base_height,
                        "response window suffix does not continue the cached tip"
                    );
                    return None;
                }
            }
            // With no base, the first entry is the single root verified
            // above and therefore carries no composite parent.
        } else {
            let [parent] = parents[..] else {
                tracing::debug!(
                    doc_id = %doc_id,
                    cid = %commit.cid,
                    parents = parents.len(),
                    "response window commit does not have exactly one composite parent"
                );
                return None;
            };
            let preceding = &commits[index - 1];
            // The parent must be the immediately preceding commit by CID
            // *and* the parent tuple's height must equal the preceding
            // commit's own height exactly.
            if parent.cid != preceding.cid {
                tracing::debug!(
                    doc_id = %doc_id,
                    cid = %commit.cid,
                    "response window chain is not linear (parent is not the preceding commit)"
                );
                return None;
            }
            if parent.height != Some(preceding.height) {
                tracing::debug!(
                    doc_id = %doc_id,
                    cid = %commit.cid,
                    parent = %parent.cid,
                    parent_height = ?parent.height,
                    expected_height = preceding.height,
                    "response window parent height does not equal the preceding commit height"
                );
                return None;
            }
        }
        // Required field links: a composite ties field blocks together; one
        // with nothing to compose is malformed, and every link was already
        // proven resolvable and exactly resolved at decode time.
        if commit.links.is_empty() {
            tracing::debug!(
                doc_id = %doc_id,
                cid = %commit.cid,
                "response window commit carries no field links"
            );
            return None;
        }
    }
    Some(commits.to_vec())
}

/// The `_C`-parent heads of one composite commit. A composite's `_C` heads
/// are its parents in the composite chain; heads pointing at other field
/// names are not parents (all references were already proven resolvable at
/// decode time).
fn composite_parents(commit: &CompositeCommitRow) -> Vec<&CompositeCommitHead> {
    commit
        .heads
        .iter()
        .filter(|head| head.field_name == "_C")
        .collect()
}

#[cfg(test)]
impl CompositeSnapshot {
    /// The wrapped `ResponseRow`, for tests asserting the authoritative
    /// snapshot state.
    fn row(&self) -> &ResponseRow {
        &self.snapshot
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn message_row(role: &str, sequence: i64, content: &str) -> MessageRow {
        MessageRow {
            message_key: format!("sess:{sequence}"),
            request_id: Some("req-1".to_string()),
            sequence: Some(sequence),
            role: Some(role.to_string()),
            content: Some(content.to_string()),
            reasoning: None,
            timestamp: None,
        }
    }

    #[test]
    fn update_kinds_use_the_grok_wire_names() {
        assert_eq!(
            MessageUpdate::AgentMessageChunk { text: "hi".into() }.session_update_kind(),
            "agent_message_chunk"
        );
        assert_eq!(
            MessageUpdate::AgentThoughtChunk { text: "hi".into() }.session_update_kind(),
            "agent_thought_chunk"
        );
        assert_eq!(
            MessageUpdate::UserMessageChunk { text: "hi".into() }.session_update_kind(),
            "user_message_chunk"
        );
    }

    #[test]
    fn payload_uses_content_field_name_not_content_block() {
        let payload = MessageUpdate::AgentMessageChunk {
            text: "delta".into(),
        }
        .to_payload();
        assert_eq!(payload["sessionUpdate"], "agent_message_chunk");
        assert_eq!(payload["content"]["type"], "text");
        assert_eq!(payload["content"]["text"], "delta");
        assert!(
            payload.get("contentBlock").is_none(),
            "the Grok decoder expects the chunk field name `content`"
        );
    }

    #[test]
    fn assistant_envelope_decodes_to_message_chunks_not_raw_json() {
        // The persisted blob is serde_json::to_string(&Message), not raw text.
        let blob = serde_json::to_string(&Message::assistant("Hello from Gents!"))
            .expect("serialize assistant message");
        let mut updates = Vec::new();
        project_row(&message_row("assistant", 1, &blob), &mut updates);
        assert_eq!(
            updates,
            vec![MessageUpdate::AgentMessageChunk {
                text: "Hello from Gents!".to_string()
            }],
            "the envelope must be decoded; the raw JSON blob must never stream"
        );
    }

    #[test]
    fn chunk_text_streams_verbatim_without_trimming() {
        // The streamed deltas must concatenate to the durable message text
        // exactly; trimming would corrupt multi-block messages.
        let message = Message::Assistant {
            id: None,
            content: vec![
                AssistantContent::text("  leading and trailing  "),
                AssistantContent::text("\nsecond block\n"),
            ],
        };
        let blob = serde_json::to_string(&message).expect("serialize assistant message");
        let mut updates = Vec::new();
        project_row(&message_row("assistant", 1, &blob), &mut updates);
        assert_eq!(
            updates,
            vec![
                MessageUpdate::AgentMessageChunk {
                    text: "  leading and trailing  ".to_string()
                },
                MessageUpdate::AgentMessageChunk {
                    text: "\nsecond block\n".to_string()
                },
            ],
            "chunk text must stream verbatim, never str::trim filtered"
        );
    }

    #[test]
    fn whitespace_only_blocks_do_not_stream() {
        let message = Message::Assistant {
            id: None,
            content: vec![AssistantContent::text("   \n\t  ")],
        };
        let blob = serde_json::to_string(&message).expect("serialize assistant message");
        let mut updates = Vec::new();
        project_row(&message_row("assistant", 1, &blob), &mut updates);
        assert!(updates.is_empty());
    }

    #[test]
    fn assistant_envelope_with_text_and_reasoning_orders_thought_before_text() {
        let message = Message::Assistant {
            id: None,
            content: vec![
                AssistantContent::Reasoning(gents_protocol::message::Reasoning::new(
                    "thinking hard",
                )),
                AssistantContent::text("the answer"),
            ],
        };
        let blob = serde_json::to_string(&message).expect("serialize assistant message");
        let mut updates = Vec::new();
        project_row(&message_row("assistant", 1, &blob), &mut updates);
        assert_eq!(
            updates,
            vec![
                MessageUpdate::AgentThoughtChunk {
                    text: "thinking hard".to_string()
                },
                MessageUpdate::AgentMessageChunk {
                    text: "the answer".to_string()
                },
            ]
        );
    }

    #[test]
    fn assistant_reasoning_only_row_streams_thought_chunk() {
        let message = Message::Assistant {
            id: None,
            content: vec![AssistantContent::Reasoning(
                gents_protocol::message::Reasoning::new("only a thought"),
            )],
        };
        let blob = serde_json::to_string(&message).expect("serialize assistant message");
        let mut updates = Vec::new();
        project_row(&message_row("assistant", 1, &blob), &mut updates);
        assert_eq!(
            updates,
            vec![MessageUpdate::AgentThoughtChunk {
                text: "only a thought".to_string()
            }]
        );
    }

    #[test]
    fn assistant_tool_call_only_row_streams_no_chunks() {
        // Assistant rows carrying only tool calls stream nothing here; the
        // tool leaf owns tool_call projection.
        let message = Message::Assistant {
            id: None,
            content: vec![AssistantContent::ToolCall(
                gents_protocol::message::ToolCall {
                    id: "call-1".to_string(),
                    call_id: None,
                    function: gents_protocol::message::ToolFunction::new(
                        "read_file".to_string(),
                        serde_json::json!({"path": "README.md"}),
                    ),
                    signature: None,
                    additional_params: None,
                },
            )],
        };
        let blob = serde_json::to_string(&message).expect("serialize assistant message");
        let mut updates = Vec::new();
        project_row(&message_row("assistant", 1, &blob), &mut updates);
        assert!(updates.is_empty());
    }

    #[test]
    fn durable_reasoning_field_streams_when_envelope_has_no_reasoning() {
        // #492: the reasoning copy may live only in AgentMessage.reasoning
        // after the response tail was cleared on finalize. The fallback
        // streams before the body text, matching the thought-before-text
        // contract every other assistant row follows.
        let mut row = message_row(
            "assistant",
            1,
            &serde_json::to_string(&Message::assistant("body text"))
                .expect("serialize assistant message"),
        );
        row.reasoning = Some("late reasoning".to_string());
        let mut updates = Vec::new();
        project_row(&row, &mut updates);
        assert_eq!(
            updates,
            vec![
                MessageUpdate::AgentThoughtChunk {
                    text: "late reasoning".to_string()
                },
                MessageUpdate::AgentMessageChunk {
                    text: "body text".to_string()
                },
            ]
        );
    }

    #[test]
    fn user_envelope_decodes_to_user_message_chunk() {
        let blob = serde_json::to_string(&Message::user("In one sentence, what is Gents?"))
            .expect("serialize user message");
        let mut updates = Vec::new();
        project_row(&message_row("user", 0, &blob), &mut updates);
        assert_eq!(
            updates,
            vec![MessageUpdate::UserMessageChunk {
                text: "In one sentence, what is Gents?".to_string()
            }]
        );
    }

    #[test]
    fn user_tool_result_rows_do_not_stream_as_user_chunks() {
        // Tool results are the tool leaf's domain; they are not message text.
        let message = Message::User {
            content: vec![UserContent::tool_result(
                "result-1",
                vec![gents_protocol::message::ToolResultContent::text(
                    "file contents",
                )],
            )],
        };
        let blob = serde_json::to_string(&message).expect("serialize user message");
        let mut updates = Vec::new();
        project_row(&message_row("user", 2, &blob), &mut updates);
        assert!(updates.is_empty());
    }

    #[test]
    fn legacy_plain_text_row_falls_back_to_a_single_chunk() {
        // decode_persisted_message tolerates legacy rows whose content is
        // plain text rather than a serialized envelope.
        let mut updates = Vec::new();
        project_row(
            &message_row("assistant", 1, "plain legacy text"),
            &mut updates,
        );
        assert_eq!(
            updates,
            vec![MessageUpdate::AgentMessageChunk {
                text: "plain legacy text".to_string()
            }]
        );
        let mut updates = Vec::new();
        project_row(&message_row("user", 0, "legacy user text"), &mut updates);
        assert_eq!(
            updates,
            vec![MessageUpdate::UserMessageChunk {
                text: "legacy user text".to_string()
            }]
        );
    }

    #[test]
    fn undecodable_blob_falls_back_to_plain_text_without_panicking() {
        let mut updates = Vec::new();
        project_row(
            &message_row("assistant", 1, r#"{"role":"assistant","content":"#),
            &mut updates,
        );
        // A malformed blob fails Message decoding, then bare-array decoding,
        // then falls back to plain text — which is the JSON fragment itself.
        // The fragment is not empty, so it streams as the fallback text; the
        // important property is that it never panics and never fabricates an
        // assistant envelope that was not there.
        assert!(!updates.is_empty());
    }

    #[test]
    fn empty_and_whitespace_rows_project_nothing() {
        let mut updates = Vec::new();
        project_row(&message_row("assistant", 1, "   "), &mut updates);
        project_row(&message_row("user", 0, ""), &mut updates);
        project_row(&message_row("assistant", 2, ""), &mut updates);
        assert!(updates.is_empty());
    }

    #[test]
    fn response_row_terminality_requires_complete_error_or_interrupted() {
        let row = |status: Option<&str>, interrupted_at: Option<&str>| ResponseRow {
            doc_id: None,
            request_id: "req-1".to_string(),
            status: status.map(ToOwned::to_owned),
            error_message: None,
            token_count: None,
            content: None,
            reasoning: None,
            progress_seq: None,
            reasoning_progress_seq: None,
            materialized_at: None,
            materialized_message_sequence: None,
            created_at: None,
            completed_at: None,
            interrupted_at: interrupted_at.map(ToOwned::to_owned),
        };

        assert!(!row(None, None).is_terminal());
        assert!(!row(Some(""), None).is_terminal());
        assert!(!row(Some("running"), None).is_terminal());
        assert!(!row(Some("in_progress"), None).is_terminal());
        assert!(row(Some("complete"), None).is_terminal());
        assert!(row(Some("error"), None).is_terminal());
        assert!(row(Some("running"), Some("2026-08-31T00:00:00Z")).is_terminal());
        // A blank interrupted_at is not terminal (the unit contract treats
        // only a non-empty interrupted_at as terminal).
        assert!(!row(Some("running"), Some("")).is_terminal());
    }

    #[test]
    fn stop_reason_projection_covers_cancelled_error_and_end_turn() {
        let cancelled = ResponseRow {
            doc_id: None,
            request_id: "req-1".to_string(),
            status: Some("complete".to_string()),
            error_message: None,
            token_count: Some(120),
            content: None,
            reasoning: None,
            progress_seq: None,
            reasoning_progress_seq: None,
            materialized_at: None,
            materialized_message_sequence: None,
            created_at: None,
            completed_at: None,
            interrupted_at: Some("2026-08-31T00:00:00Z".to_string()),
        };
        assert_eq!(cancelled.stop_reason(), "cancelled");

        let errored = ResponseRow {
            status: Some("error".to_string()),
            interrupted_at: None,
            ..cancelled.clone()
        };
        assert_eq!(errored.stop_reason(), "error");

        let completed = ResponseRow {
            status: Some("complete".to_string()),
            interrupted_at: None,
            ..cancelled.clone()
        };
        assert_eq!(completed.stop_reason(), "end_turn");
    }

    #[test]
    fn queries_escape_the_request_id() {
        let query = latest_response_query("req\"1\\x");
        assert!(
            !query.contains("\"req\"1\\x\""),
            "the interpolated request id must be escaped"
        );
        let messages = request_messages_query("req\"1\\x", 0);
        assert!(
            !messages.contains("\"req\"1\\x\""),
            "the interpolated request id must be escaped"
        );
    }

    #[test]
    fn queries_are_request_scoped_and_bounded() {
        let query = latest_response_query("req-1");
        assert!(query.contains(r#"request_id: { _eq: "req-1" }"#));
        assert!(query.contains("limit: 1"));
        let messages = request_messages_query("req-1", 17);
        assert!(messages.contains(r#"request_id: { _eq: "req-1" }"#));
        assert!(messages.contains("sequence: { _gte: 17 }"));
        assert!(messages.contains("order: { sequence: ASC }"));
        assert!(messages.contains("limit: 64"));
    }

    /// A live/durable reconciliation caller hands `(kind, delta)` pairs whose
    /// kind string was observed from a durable row; an unknown discriminator
    /// must fall back to `agent_message_chunk` rather than fabricate an
    /// unrecognized `sessionUpdate` tag on the wire, while the two known
    /// assistant kinds pass through byte-exact.
    #[test]
    fn chunk_payload_preserves_known_kinds_and_falls_back_for_unknown() {
        let thought = MessageUpdate::chunk_payload(AGENT_THOUGHT_CHUNK, "why");
        assert_eq!(thought["sessionUpdate"], "agent_thought_chunk");
        assert_eq!(thought["content"]["text"], "why");

        let message = MessageUpdate::chunk_payload(AGENT_MESSAGE_CHUNK, "what");
        assert_eq!(message["sessionUpdate"], "agent_message_chunk");
        let user = MessageUpdate::chunk_payload(USER_MESSAGE_CHUNK, "notice");
        assert_eq!(user["sessionUpdate"], "user_message_chunk");
        assert!(
            user.get("_meta").is_none(),
            "ordinary user echoes stay visible"
        );

        let unknown = MessageUpdate::chunk_payload("agent_message_block", "delta");
        assert_eq!(
            unknown["sessionUpdate"], "agent_message_chunk",
            "an unknown kind must fall back to agent_message_chunk, never reach the wire as-is"
        );
        assert_eq!(unknown["content"]["text"], "delta");
        assert!(
            unknown.get("contentBlock").is_none(),
            "the chunk field name is `content`, never `contentBlock`"
        );
    }

    #[test]
    fn background_completion_uses_native_hidden_echo_metadata() {
        let text = "<tool-completion>unchanged durable content</tool-completion>";
        let payload = MessageUpdate::background_completion_payload(text);
        assert_eq!(payload["sessionUpdate"], USER_MESSAGE_CHUNK);
        assert_eq!(payload["content"]["text"], text);
        assert_eq!(payload["_meta"]["hideFromScrollback"], true);
        assert!(payload["content"].get("_meta").is_none());
    }

    /// A sink serving one empty response discovery and one empty transcript
    /// page: the minimal request with no rows at all.
    struct EmptyRequestSink;

    impl QuerySink for EmptyRequestSink {
        async fn execute(&self, query: &str) -> defra_node::QueryResponse {
            assert!(
                query.contains(r#"request_id: { _eq: "req-1" }"#),
                "every query must stay request-scoped: {query}"
            );
            if query.contains("AgentResponse(") {
                return query_response(json!({ "AgentResponse": [] }));
            }
            if query.contains("AgentMessage(") {
                return query_response(json!({ "AgentMessage": [] }));
            }
            panic!("unexpected query for an empty request: {query}");
        }
    }

    /// The projection exposes the effective `totalContextTokens` bound the
    /// engine clamps `_meta.totalTokens` against: a configured window passes
    /// through unchanged, and the unspecified (zero) configuration falls back
    /// to the catalog default so the bound is never zero.
    #[tokio::test]
    async fn projection_binds_context_window_tokens_to_the_bound_catalog() {
        let mut observation = HistoryObservation::default();
        let default_projection =
            project_messages_with_sink(&EmptyRequestSink, &mut observation, None, "req-1", 0)
                .await
                .expect("empty-request projection");
        assert_eq!(
            default_projection.context_window_tokens,
            super::super::DEFAULT_CONTEXT_WINDOW_TOKENS,
            "an unspecified context window must fall back to the catalog default"
        );
        assert_eq!(default_projection.total_tokens, 0);
        assert!(!default_projection.terminal);

        let mut observation = HistoryObservation::default();
        let bound_projection =
            project_messages_with_sink(&EmptyRequestSink, &mut observation, None, "req-1", 524_288)
                .await
                .expect("empty-request projection");
        assert_eq!(
            bound_projection.context_window_tokens, 524_288,
            "a configured window must pass through without modification"
        );
    }

    struct MessagePageSink {
        pages: std::sync::Mutex<std::collections::VecDeque<defra_node::QueryResponse>>,
        queries: std::sync::Mutex<Vec<String>>,
    }

    impl QuerySink for MessagePageSink {
        async fn execute(&self, query: &str) -> defra_node::QueryResponse {
            self.queries
                .lock()
                .expect("queries lock")
                .push(query.to_string());
            self.pages
                .lock()
                .expect("pages lock")
                .pop_front()
                .expect("scripted message page")
        }
    }

    fn message_page(
        range: std::ops::RangeInclusive<i64>,
        suffix: &str,
    ) -> defra_node::QueryResponse {
        let rows: Vec<_> = range
            .map(|sequence| {
                json!({
                    "message_key": format!("sess:{sequence}"),
                    "session_id": "sess",
                    "request_id": "req-1",
                    "sequence": sequence,
                    "role": "assistant",
                    "content": format!("row-{sequence}{suffix}"),
                })
            })
            .collect();
        query_response(json!({ "AgentMessage": rows }))
    }

    #[tokio::test]
    async fn message_observer_pages_cold_history_then_rereads_only_mutable_tail() {
        let sink = MessagePageSink {
            pages: std::sync::Mutex::new(std::collections::VecDeque::from([
                message_page(1..=64, ""),
                message_page(65..=70, ""),
                message_page(70..=70, "-grown"),
            ])),
            queries: std::sync::Mutex::new(Vec::new()),
        };
        let cold = load_incremental_message_rows(&sink, None, "req-1")
            .await
            .expect("cold pages");
        assert_eq!(cold.0.len(), 70);
        assert_eq!(cold.1, Some(70));

        let unchanged = load_incremental_message_rows(&sink, cold.1, "req-1")
            .await
            .expect("tail page");
        assert_eq!(unchanged.0.len(), 1);
        assert_eq!(unchanged.0[0].sequence, Some(70));
        assert_eq!(unchanged.0[0].content.as_deref(), Some("row-70-grown"));

        let queries = sink.queries.lock().expect("queries lock");
        assert_eq!(queries.len(), 3);
        assert!(queries[0].contains("sequence: { _gte: 0 }"));
        assert!(queries[1].contains("sequence: { _gte: 65 }"));
        assert!(queries[2].contains("sequence: { _gte: 70 }"));
        assert!(queries.iter().all(|query| query.contains("limit: 64")));
    }

    #[test]
    fn reasoning_texts_skip_encrypted_and_redacted_payloads() {
        use gents_protocol::message::{Reasoning, ReasoningContent};
        let reasoning = Reasoning {
            id: None,
            content: vec![
                ReasoningContent::Text {
                    text: "visible".to_string(),
                    signature: None,
                },
                ReasoningContent::Encrypted("opaque".to_string()),
                ReasoningContent::Redacted {
                    data: "opaque".to_string(),
                },
                ReasoningContent::Summary("summarized".to_string()),
            ],
        };
        assert_eq!(
            reasoning_texts(&reasoning),
            vec!["visible".to_string(), "summarized".to_string()]
        );
    }

    #[test]
    fn extract_message_reasoning_helper_still_applies_to_decoded_envelopes() {
        use gents_protocol::transcript::extract_message_reasoning;
        let message = Message::Assistant {
            id: None,
            content: vec![
                AssistantContent::Reasoning(gents_protocol::message::Reasoning::new("why")),
                AssistantContent::text("what"),
            ],
        };
        assert_eq!(
            extract_message_reasoning(&message).as_deref(),
            Some("why"),
            "the transcript helper reads the same decoded envelope this leaf streams"
        );
    }

    /// Chunk identity is chunk-level, not row-level: one row's reasoning
    /// thought and body text get distinct keys (a row-level key would let
    /// the thought mark the text as already streamed), while two distinct
    /// rows with identical text both keep their own keys.
    #[test]
    fn update_keys_distinguish_a_rows_thought_from_its_text() {
        let blob = serde_json::to_string(&Message::Assistant {
            id: None,
            content: vec![
                AssistantContent::Reasoning(gents_protocol::message::Reasoning::new("why")),
                AssistantContent::text("what"),
            ],
        })
        .expect("serialize assistant message");
        let mut updates = Vec::new();
        project_row(&message_row("assistant", 1, &blob), &mut updates);
        assert_eq!(
            updates,
            vec![
                MessageUpdate::AgentThoughtChunk {
                    text: "why".to_string()
                },
                MessageUpdate::AgentMessageChunk {
                    text: "what".to_string()
                },
            ]
        );
        // Rebuild the keys exactly as `project_messages` does so the
        // identity scheme itself is what is asserted here.
        let row = message_row("assistant", 1, &blob);
        let mut keys = Vec::new();
        let mut kinds_seen: std::collections::BTreeMap<&'static str, u64> =
            std::collections::BTreeMap::new();
        for update in &updates {
            let counter = kinds_seen.entry(update.session_update_kind()).or_default();
            *counter += 1;
            keys.push(format!(
                "{}:{}:{}",
                row.message_key,
                update.session_update_kind(),
                *counter
            ));
        }
        assert_eq!(
            keys,
            vec![
                "sess:1:agent_thought_chunk:1".to_string(),
                "sess:1:agent_message_chunk:1".to_string(),
            ],
            "the thought and the text of one row must have distinct chunk identities"
        );

        // Two distinct rows carrying identical text keep distinct keys too.
        let same_text = serde_json::to_string(&Message::assistant("twin"))
            .expect("serialize assistant message");
        let mut first = Vec::new();
        let mut second = Vec::new();
        let row_one = message_row("assistant", 1, &same_text);
        let row_two = message_row("assistant", 2, &same_text);
        project_row(&row_one, &mut first);
        project_row(&row_two, &mut second);
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        let key_one = format!(
            "{}:{}:1",
            row_one.message_key,
            first[0].session_update_kind()
        );
        let key_two = format!(
            "{}:{}:1",
            row_two.message_key,
            second[0].session_update_kind()
        );
        assert_ne!(key_one, key_two);
    }
    // -----------------------------------------------------------------
    // Composite-history fixtures shared by the live HistoryObservation tests.
    // -----------------------------------------------------------------

    /// A query response with `data` set, shaped like an embedded-node reply.
    fn query_response(data: serde_json::Value) -> defra_node::QueryResponse {
        defra_node::QueryResponse::success(data)
    }

    /// One raw `_commits` row value.
    fn commit_value(
        cid: &str,
        height: i64,
        field_name: &str,
        heads: serde_json::Value,
        links: serde_json::Value,
    ) -> serde_json::Value {
        json!({
            "cid": cid,
            "height": height,
            "fieldName": field_name,
            "heads": heads,
            "links": links,
        })
    }

    fn head_value(cid: &str, height: i64, field_name: &str) -> serde_json::Value {
        json!({ "cid": cid, "height": height, "fieldName": field_name })
    }

    // -----------------------------------------------------------------
    // Fail-closed response selection and read order, proven through the
    // `QuerySink` seam (the reads are discover → history → rows, and
    // only the validated history tip may supply live state).
    // -----------------------------------------------------------------

    /// An `AgentMessage` reply carrying one assistant row (sequence 2)
    /// whose persisted content envelope holds `durable text`.
    fn message_rows_reply() -> defra_node::QueryResponse {
        let blob =
            serde_json::to_string(&Message::assistant("durable text")).expect("serialize message");
        query_response(json!({ "AgentMessage": [ {
            "message_key": "sess:2",
            "session_id": "sess",
            "request_id": "req-1",
            "sequence": 2,
            "role": "assistant",
            "content": blob,
        } ] }))
    }

    /// A valid two-commit `_commits` reply (create then one update) with
    /// the field rows its links point at.
    fn two_commit_chain_reply() -> defra_node::QueryResponse {
        query_response(json!({ "_commits": [
            commit_value("bafy-c1", 1, "_C", json!([]), json!([head_value("bafy-f1", 1, "content")])),
            commit_value(
                "bafy-c2",
                2,
                "_C",
                json!([head_value("bafy-c1", 1, "_C")]),
                json!([head_value("bafy-f2", 2, "content")]),
            ),
            commit_value("bafy-f1", 1, "content", json!([]), json!([])),
            commit_value("bafy-f2", 2, "content", json!([]), json!([])),
        ] }))
    }

    fn two_commit_head_reply() -> defra_node::QueryResponse {
        query_response(json!({ "_commits": [
            commit_value(
                "bafy-c2",
                2,
                "_C",
                json!([head_value("bafy-c1", 1, "_C")]),
                json!([head_value("bafy-f2", 2, "content")]),
            ),
            commit_value("bafy-f2", 2, "content", json!([]), json!([])),
        ] }))
    }

    /// A sink whose history reads fail for the first two polls (the
    /// `_commits` reply carries no data array) and recover afterwards,
    /// sharing state across polls so one instance can serve a
    /// poll-retry sequence. The discovery row deliberately carries live
    /// bytes, a token count, and a terminal status that must never
    /// surface while the history is unprovable.
    struct RecoveringSink {
        discovery_polls: std::sync::atomic::AtomicU8,
        queries: std::sync::Mutex<Vec<String>>,
    }

    impl QuerySink for RecoveringSink {
        async fn execute(&self, query: &str) -> defra_node::QueryResponse {
            self.queries
                .lock()
                .expect("queries lock")
                .push(query.to_string());
            if query.contains("AgentMessage(") {
                return message_rows_reply();
            }
            if query.contains("_commits(") {
                if self
                    .discovery_polls
                    .load(std::sync::atomic::Ordering::SeqCst)
                    <= 2
                {
                    // The history cannot be read: no `_commits` array.
                    return defra_node::QueryResponse {
                        data: None,
                        errors: Vec::new(),
                    };
                }
                if query.contains("depth: 1") {
                    return two_commit_head_reply();
                }
                return two_commit_chain_reply();
            }
            if query.contains("AgentResponse(") {
                if query.contains("cid:") {
                    // Snapshot reconstruction for the validated chain.
                    return query_response(json!({ "AgentResponse": [
                        {
                            "_docID": "bae-doc",
                            "request_id": "req-1",
                            "content": "abc",
                            "status": "streaming",
                        },
                        {
                            "_docID": "bae-doc",
                            "request_id": "req-1",
                            "content": "tip-validated",
                            "status": "complete",
                            "token_count": 42,
                            "materialized_message_sequence": 2,
                        },
                    ] }));
                }
                // Discovery: bytes/tokens/terminality that must never be
                // selected while the history is unprovable.
                self.discovery_polls
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return query_response(json!({ "AgentResponse": [ {
                    "_docID": "bae-doc",
                    "request_id": "req-1",
                    "status": "error",
                    "content": "discovery-only bytes",
                    "token_count": 777,
                } ] }));
            }
            panic!("recovering sink received an unexpected query: {query}");
        }
    }

    /// Response selection fails closed across unavailable-history polls
    /// and exposes the validated tip exactly once it can be proven: the
    /// discovery row is never a fallback for live bytes, tokens,
    /// terminality, or stop reason, while the durable `AgentMessage`
    /// projection continues on every poll.
    #[tokio::test]
    async fn selection_fails_closed_until_the_history_is_provable() {
        let sink = RecoveringSink {
            discovery_polls: std::sync::atomic::AtomicU8::new(0),
            queries: std::sync::Mutex::new(Vec::new()),
        };
        let mut observation = HistoryObservation::default();
        for poll in 1..=2 {
            let projection = project_messages_with_sink(&sink, &mut observation, None, "req-1", 0)
                .await
                .expect("projection must not error");
            assert!(
                !projection.terminal,
                "unavailable history must never report terminal state (poll {poll})"
            );
            assert_eq!(
                projection.stop_reason, None,
                "unavailable history must never report a stop reason (poll {poll})"
            );
            assert_eq!(
                projection.total_tokens, 0,
                "unavailable history must never report the discovery row's tokens (poll {poll})"
            );
            assert_eq!(
                projection.live_tail,
                LiveResponseTail::default(),
                "unavailable history must expose no live tail, never the discovery row (poll {poll})"
            );
            assert_eq!(
                projection.history, None,
                "an unreadable history is unprovable, not absent (poll {poll})"
            );
            // The durable AgentMessage projection continues regardless.
            assert_eq!(
                projection.updates,
                vec![MessageUpdate::AgentMessageChunk {
                    text: "durable text".to_string()
                }],
                "durable rows still project while the history is unprovable (poll {poll})"
            );
        }

        // Recovery: the third poll proves the history and exposes the
        // validated tip exactly — its bytes, its tokens, its terminality.
        let projection = project_messages_with_sink(&sink, &mut observation, None, "req-1", 0)
            .await
            .expect("projection must not error");
        let history = projection.history.as_ref().expect("proven history");
        assert_eq!(history.len(), 2);
        assert_eq!(history.last().expect("tip").cid, "bafy-c2");
        assert_eq!(
            projection.live_tail.content.as_deref(),
            Some("tip-validated"),
            "only the validated tip may supply the live tail"
        );
        assert!(projection.live_tail.response_present);
        assert_eq!(projection.live_tail.assistant_sequence, Some(2));
        assert_eq!(projection.live_tail.materialized_message_sequence, Some(2));
        assert_eq!(projection.total_tokens, 42);
        assert!(projection.terminal);
        assert_eq!(projection.stop_reason, Some("end_turn"));

        // Read order per poll: discover → bounded head (→ fixed window and
        // snapshots once provable) → rows. Recorded across all three polls.
        let queries = sink.queries.lock().expect("queries lock").clone();
        let kind = |query: &str| {
            if query.contains("AgentMessage(") {
                "rows"
            } else if query.contains("_commits(") {
                "commits"
            } else if query.contains("cid:") {
                "snapshots"
            } else {
                "discovery"
            }
        };
        let order: Vec<&str> = queries.iter().map(|query| kind(query)).collect();
        assert_eq!(
            order,
            vec![
                "discovery",
                "commits",
                "rows", //
                "discovery",
                "commits",
                "rows", //
                "discovery",
                "commits",
                "commits",
                "snapshots",
                "rows",
            ],
            "every poll must read discovery, then history, then rows"
        );
    }

    /// One scripted step of [`ScriptedSink`].
    enum ScriptedStep {
        /// Reply with a fixed response.
        Reply(defra_node::QueryResponse),
        /// Wait until the test releases the barrier, then build the
        /// reply at read time (simulating a row materializing between
        /// the history read and the row read).
        Gated {
            barrier: Arc<tokio::sync::Barrier>,
            make: Box<dyn Fn() -> defra_node::QueryResponse + Send>,
        },
    }

    /// A sink that replays fixed steps in order and records every query
    /// it served, so tests can prove the read order deterministically.
    struct ScriptedSink {
        steps: std::sync::Mutex<std::collections::VecDeque<ScriptedStep>>,
        queries: std::sync::Mutex<Vec<String>>,
    }

    impl QuerySink for ScriptedSink {
        async fn execute(&self, query: &str) -> defra_node::QueryResponse {
            self.queries
                .lock()
                .expect("queries lock")
                .push(query.to_string());
            // Take the step out of the lock before awaiting: the guard
            // must never live across the barrier wait.
            let step = self.steps.lock().expect("steps lock").pop_front();
            match step {
                Some(ScriptedStep::Reply(response)) => response,
                Some(ScriptedStep::Gated { barrier, make }) => {
                    barrier.wait().await;
                    make()
                }
                None => panic!("scripted sink ran out of steps for query: {query}"),
            }
        }
    }

    #[tokio::test]
    async fn observer_rejects_malformed_rows_and_foreign_snapshots() {
        let malformed = ScriptedSink {
            steps: std::sync::Mutex::new(
                vec![ScriptedStep::Reply(query_response(json!({
                    "_commits": [{
                        "cid": "bafy-bad",
                        "height": "not-an-integer",
                        "fieldName": "_C",
                        "heads": [],
                        "links": [],
                    }]
                })))]
                .into_iter()
                .collect(),
            ),
            queries: std::sync::Mutex::new(Vec::new()),
        };
        let mut observation = HistoryObservation::default();
        assert!(
            observe_history_with_sink(&malformed, &mut observation, "bae-doc", "req-1")
                .await
                .expect("malformed observation query")
                .is_none(),
            "the production observer must fail closed on any undecodable commit"
        );
        assert!(observation.tip.is_none());

        let root = commit_value(
            "bafy-c1",
            1,
            "_C",
            json!([]),
            json!([head_value("bafy-f1", 1, "content")]),
        );
        let first_child = commit_value(
            "bafy-c2",
            2,
            "_C",
            json!([head_value("bafy-c1", 1, "_C")]),
            json!([head_value("bafy-f2", 2, "content")]),
        );
        let sibling = commit_value(
            "bafy-c3",
            3,
            "_C",
            json!([head_value("bafy-c1", 1, "_C")]),
            json!([head_value("bafy-f3", 3, "content")]),
        );
        let field1 = commit_value("bafy-f1", 1, "content", json!([]), json!([]));
        let field2 = commit_value("bafy-f2", 2, "content", json!([]), json!([]));
        let field3 = commit_value("bafy-f3", 3, "content", json!([]), json!([]));
        let branched = ScriptedSink {
            steps: std::sync::Mutex::new(
                vec![
                    ScriptedStep::Reply(query_response(json!({
                        "_commits": [sibling.clone(), field3.clone()]
                    }))),
                    ScriptedStep::Reply(query_response(json!({
                        "_commits": [root, first_child, sibling, field1, field2, field3]
                    }))),
                ]
                .into_iter()
                .collect(),
            ),
            queries: std::sync::Mutex::new(Vec::new()),
        };
        assert!(
            observe_history_with_sink(&branched, &mut observation, "bae-doc", "req-1")
                .await
                .expect("branched observation query")
                .is_none(),
            "the live observer must reject sibling branches"
        );
        assert!(observation.tip.is_none());

        let root_rows = linear_history_values(1, 1);
        let foreign_snapshot = ScriptedSink {
            steps: std::sync::Mutex::new(
                vec![
                    ScriptedStep::Reply(query_response(json!({
                        "_commits": root_rows.clone()
                    }))),
                    ScriptedStep::Reply(query_response(json!({
                        "_commits": root_rows
                    }))),
                    ScriptedStep::Reply(query_response(json!({
                        "AgentResponse": [{
                            "_docID": "bae-doc",
                            "request_id": "req-OTHER",
                            "content": "foreign",
                            "status": "streaming",
                        }]
                    }))),
                ]
                .into_iter()
                .collect(),
            ),
            queries: std::sync::Mutex::new(Vec::new()),
        };
        assert!(
            observe_history_with_sink(&foreign_snapshot, &mut observation, "bae-doc", "req-1")
                .await
                .expect("foreign snapshot observation query")
                .is_none(),
            "snapshot/request pairing must be checked on the live observer path"
        );
        assert!(observation.tip.is_none());
    }

    /// The `AgentMessage` rows are read *after* the history tip is
    /// loaded and fixed: the gated row read cannot begin until the
    /// discovery, head, window, and snapshot reads have all been served, and a
    /// row the runtime materialized between the history read and the row
    /// read is present in the rows read after — so the reset observed in
    /// the fixed history binds to the materialized row, never to a stale
    /// row list.
    #[tokio::test]
    async fn message_rows_are_read_after_the_history_tip_is_fixed() {
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let materialized = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let rows_barrier = Arc::clone(&barrier);
        let rows_materialized = Arc::clone(&materialized);
        let make_rows = Box::new(move || {
            // The runtime persists the assistant row before resetting the
            // tail; by the time the row read runs, the row is durable.
            rows_materialized.store(true, std::sync::atomic::Ordering::SeqCst);
            message_rows_reply()
        });
        let sink = Arc::new(ScriptedSink {
            steps: std::sync::Mutex::new(
                vec![
                    // 1. Discovery: live bytes in flight (never selected
                    //    while unprovable, and superseded by the tip).
                    ScriptedStep::Reply(query_response(json!({ "AgentResponse": [ {
                        "_docID": "bae-doc",
                        "request_id": "req-1",
                        "status": "running",
                        "content": "live bytes in flight",
                    } ] }))),
                    // 2. The commit history: create then a tail reset.
                    ScriptedStep::Reply(two_commit_head_reply()),
                    // 3. The fixed window: create then a tail reset.
                    ScriptedStep::Reply(two_commit_chain_reply()),
                    // 4. Snapshot reconstruction: the tip is the reset
                    //    (empty tail, the row materialized as sequence 2).
                    ScriptedStep::Reply(query_response(json!({ "AgentResponse": [
                        {
                            "_docID": "bae-doc",
                            "request_id": "req-1",
                            "content": "abc",
                            "status": "streaming",
                        },
                        {
                            "_docID": "bae-doc",
                            "request_id": "req-1",
                            "content": "",
                            "status": "streaming",
                            "materialized_message_sequence": 2,
                        },
                    ] }))),
                    // 5. The row read: gated on the test's barrier, and
                    //    the assistant row materializes at read time.
                    ScriptedStep::Gated {
                        barrier: rows_barrier,
                        make: make_rows,
                    },
                ]
                .into_iter()
                .collect(),
            ),
            queries: std::sync::Mutex::new(Vec::new()),
        });

        let task_sink = Arc::clone(&sink);
        let task = tokio::spawn(async move {
            let mut observation = HistoryObservation::default();
            project_messages_with_sink(task_sink.as_ref(), &mut observation, None, "req-1", 0)
                .await
                .expect("projection must not error")
        });

        // The barrier fires only when the gated row read begins, which is
        // reachable only after the four history reads were served: at
        // this point the tip is provably already fixed.
        barrier.wait().await;
        let queries = sink.queries.lock().expect("queries lock").clone();
        assert_eq!(
            queries.len(),
            5,
            "the row read must be the fifth and last read"
        );
        assert!(
            queries[0].contains("AgentResponse(") && !queries[0].contains("cid:"),
            "the first read must be the response discovery"
        );
        assert!(
            queries[1].contains("_commits("),
            "the second read must be the commit history"
        );
        assert!(
            queries[2].contains("filter:") && queries[2].contains("height:"),
            "the third read must be the fixed history window"
        );
        assert!(
            queries[3].contains("cid:"),
            "the fourth read must be the snapshot reconstruction"
        );
        assert!(
            queries[4].contains("AgentMessage("),
            "the fifth read must be the message rows"
        );

        let projection = task.await.expect("projection task must not panic");
        assert!(
            materialized.load(std::sync::atomic::Ordering::SeqCst),
            "the row read must have run"
        );
        // The reset observed in the fixed history implies the row read
        // after carries the materialized assistant row: the live segment
        // binds to it, never to a stale row list.
        assert_eq!(projection.live_tail.assistant_sequence, Some(2));
        assert_eq!(projection.live_tail.materialized_message_sequence, Some(2));
        assert_eq!(
            projection.live_tail.content.as_deref(),
            Some(""),
            "the validated reset tip is the live tail, not the discovery row"
        );
        assert!(projection.live_tail.response_present);
        assert_eq!(
            projection.updates,
            vec![MessageUpdate::AgentMessageChunk {
                text: "durable text".to_string()
            }],
            "the row materialized before the row read must project durably"
        );
    }

    fn linear_history_values(start: i64, end: i64) -> Vec<Value> {
        let mut values = Vec::new();
        for height in start..=end {
            let heads = if height == 1 {
                json!([])
            } else {
                json!([head_value(
                    &format!("bafy-c{}", height - 1),
                    height - 1,
                    "_C"
                )])
            };
            values.push(commit_value(
                &format!("bafy-c{height}"),
                height,
                "_C",
                heads,
                json!([head_value(&format!("bafy-f{height}"), height, "content")]),
            ));
            values.push(commit_value(
                &format!("bafy-f{height}"),
                height,
                "content",
                json!([]),
                json!([]),
            ));
        }
        values
    }

    fn numbered_snapshots(start: i64, end: i64, doc_id: &str) -> Vec<Value> {
        (start..=end)
            .map(|height| {
                json!({
                    "_docID": doc_id,
                    "request_id": "req-1",
                    "content": format!("value-{height}"),
                    "progress_seq": height,
                    "status": "streaming",
                })
            })
            .collect()
    }

    #[tokio::test]
    async fn observer_cold_loads_more_than_one_window_then_polls_head_only() {
        let all = linear_history_values(1, 70);
        let first_window: Vec<_> = all
            .iter()
            .filter(|value| value["height"].as_i64().is_some_and(|h| h <= 64))
            .cloned()
            .collect();
        let second_window: Vec<_> = all
            .iter()
            .filter(|value| value["height"].as_i64().is_some_and(|h| h >= 64))
            .cloned()
            .collect();
        let head = query_response(json!({ "_commits": [
            commit_value(
                "bafy-c70", 70, "_C",
                json!([head_value("bafy-c69", 69, "_C")]),
                json!([head_value("bafy-f70", 70, "content")]),
            ),
            commit_value("bafy-f70", 70, "content", json!([]), json!([])),
        ] }));
        let sink = ScriptedSink {
            steps: std::sync::Mutex::new(
                vec![
                    ScriptedStep::Reply(head.clone()),
                    ScriptedStep::Reply(query_response(json!({ "_commits": first_window }))),
                    ScriptedStep::Reply(query_response(json!({
                        "AgentResponse": numbered_snapshots(1, 64, "bae-doc")
                    }))),
                    ScriptedStep::Reply(query_response(json!({ "_commits": second_window }))),
                    ScriptedStep::Reply(query_response(json!({
                        "AgentResponse": numbered_snapshots(65, 70, "bae-doc")
                    }))),
                    ScriptedStep::Reply(head),
                ]
                .into_iter()
                .collect(),
            ),
            queries: std::sync::Mutex::new(Vec::new()),
        };
        let mut observation = HistoryObservation::default();
        let history = observe_history_with_sink(&sink, &mut observation, "bae-doc", "req-1")
            .await
            .expect("cold observation query")
            .expect("cold history is proven");
        assert_eq!(history.len(), 70);
        assert_eq!(history.first().map(|row| row.height), Some(1));
        assert_eq!(history.last().map(|row| row.height), Some(70));
        let unchanged = observe_history_with_sink(&sink, &mut observation, "bae-doc", "req-1")
            .await
            .expect("unchanged observation query")
            .expect("unchanged history stays proven");
        assert_eq!(unchanged, history);
        let queries = sink.queries.lock().expect("queries lock");
        assert_eq!(queries.len(), 6);
        assert!(queries
            .last()
            .is_some_and(|query| query.contains("depth: 1")));
    }

    #[tokio::test]
    async fn observer_batches_direct_far_backward_reference_hydration() {
        let links: Vec<_> = (1..=65)
            .map(|index| head_value(&format!("bafy-field-{index:03}"), 1, "content"))
            .collect();
        let composite = commit_value("bafy-root", 1, "_C", json!([]), Value::Array(links));
        let fields: Vec<_> = (1..=65)
            .map(|index| {
                commit_value(
                    &format!("bafy-field-{index:03}"),
                    1,
                    "content",
                    json!([]),
                    json!([]),
                )
            })
            .collect();
        let sink = ScriptedSink {
            steps: std::sync::Mutex::new(
                vec![
                    ScriptedStep::Reply(query_response(json!({ "_commits": [composite.clone()] }))),
                    ScriptedStep::Reply(query_response(json!({ "_commits": [composite] }))),
                    ScriptedStep::Reply(query_response(json!({ "_commits": fields[..64] }))),
                    ScriptedStep::Reply(query_response(json!({ "_commits": fields[64..] }))),
                    ScriptedStep::Reply(query_response(json!({
                        "AgentResponse": numbered_snapshots(1, 1, "bae-doc")
                    }))),
                ]
                .into_iter()
                .collect(),
            ),
            queries: std::sync::Mutex::new(Vec::new()),
        };
        let mut observation = HistoryObservation::default();
        let history = observe_history_with_sink(&sink, &mut observation, "bae-doc", "req-1")
            .await
            .expect("observation query")
            .expect("direct references resolve");
        assert_eq!(history.len(), 1);
        assert_eq!(observation.raw_identities.len(), 66);
        let queries = sink.queries.lock().expect("queries lock");
        assert_eq!(
            queries
                .iter()
                .filter(|query| query.contains("_commits") && query.contains("cid:"))
                .count(),
            2,
            "65 direct references hydrate in two bounded CID batches"
        );
    }

    #[tokio::test]
    async fn observer_failure_rolls_back_and_later_recovers() {
        let root_rows = linear_history_values(1, 1);
        let root_head = query_response(json!({ "_commits": root_rows.clone() }));
        let initial = ScriptedSink {
            steps: std::sync::Mutex::new(
                vec![
                    ScriptedStep::Reply(root_head),
                    ScriptedStep::Reply(query_response(json!({ "_commits": root_rows.clone() }))),
                    ScriptedStep::Reply(query_response(json!({
                        "AgentResponse": numbered_snapshots(1, 1, "bae-doc")
                    }))),
                ]
                .into_iter()
                .collect(),
            ),
            queries: std::sync::Mutex::new(Vec::new()),
        };
        let mut observation = HistoryObservation::default();
        observe_history_with_sink(&initial, &mut observation, "bae-doc", "req-1")
            .await
            .expect("initial query")
            .expect("initial proof");
        let proven_tip = observation.tip.clone();
        let proven_chain = observation.chain.clone();
        let growth = linear_history_values(1, 2);
        let conflict = commit_value(
            "bafy-c1",
            1,
            "_C",
            json!([]),
            json!([head_value("bafy-conflict", 1, "content")]),
        );
        let failed = ScriptedSink {
            steps: std::sync::Mutex::new(
                vec![
                    ScriptedStep::Reply(query_response(json!({
                        "_commits": growth.iter().filter(|row| row["height"] == 2).cloned().collect::<Vec<_>>()
                    }))),
                    ScriptedStep::Reply(query_response(json!({
                        "_commits": [conflict]
                    }))),
                ]
                .into_iter()
                .collect(),
            ),
            queries: std::sync::Mutex::new(Vec::new()),
        };
        assert!(
            observe_history_with_sink(&failed, &mut observation, "bae-doc", "req-1")
                .await
                .expect("conflicting poll")
                .is_none()
        );
        assert_eq!(observation.tip, proven_tip);
        assert_eq!(observation.chain, proven_chain);

        let recovery = ScriptedSink {
            steps: std::sync::Mutex::new(
                vec![
                    ScriptedStep::Reply(query_response(json!({
                        "_commits": growth.iter().filter(|row| row["height"] == 2).cloned().collect::<Vec<_>>()
                    }))),
                    ScriptedStep::Reply(query_response(json!({ "_commits": growth }))),
                    ScriptedStep::Reply(query_response(json!({
                        "AgentResponse": numbered_snapshots(2, 2, "bae-doc")
                    }))),
                ]
                .into_iter()
                .collect(),
            ),
            queries: std::sync::Mutex::new(Vec::new()),
        };
        let recovered = observe_history_with_sink(&recovery, &mut observation, "bae-doc", "req-1")
            .await
            .expect("recovery query")
            .expect("recovery proof");
        assert_eq!(recovered.len(), 2);
        assert_eq!(
            observation.tip.as_ref().map(|(cid, _)| cid.as_str()),
            Some("bafy-c2")
        );
    }

    /// End-to-end observer test against a real embedded node: seed a
    /// response, update it twice, and require the loader to return the
    /// ordered, validated snapshot chain with the last snapshot
    /// authoritative.
    #[tokio::test]
    async fn embedded_history_observer_returns_ordered_chain_and_bounds_unchanged_poll() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node = Arc::new(
            EmbeddedNode::builder()
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
            create_AgentResponse(input: {
                response_key: "hist-1"
                request_id: "hist-1"
                agent_did: "did:test:grok-shim"
                requester_did: "did:test:grok-shim"
                session_id: "s-hist"
                content: "abc"
                reasoning: ""
                status: "streaming"
                token_count: 0
                progress_seq: 1
                reasoning_progress_seq: 0
                created_at: "2026-08-31T23:00:00Z"
            }) { _docID }
        }"#
        .to_string();
        let response = node.execute(&seed).await;
        assert!(!response.has_errors(), "seed failed: {:?}", response.errors);
        let doc_id = response
            .data
            .as_ref()
            .and_then(|data| data.get("add_AgentResponse"))
            .and_then(|value| value.as_array())
            .and_then(|rows| rows.first())
            .and_then(|value| value.get("_docID"))
            .and_then(|value| value.as_str())
            .expect("doc id")
            .to_string();

        for (content, seq) in [("", 1), ("abc", 3)] {
            let mutation = format!(
                r#"mutation {{
                    update_AgentResponse(
                        filter: {{ request_id: {{ _eq: "hist-1" }} }},
                        input: {{ content: "{content}", progress_seq: {seq} }}
                    ) {{ _docID }}
                }}"#
            );
            let response = node.execute(&mutation).await;
            assert!(
                !response.has_errors(),
                "update failed: {:?}",
                response.errors
            );
        }

        let mut observation = HistoryObservation::default();
        let history = observe_history_with_sink(
            &NodeSink { node: &node },
            &mut observation,
            &doc_id,
            "hist-1",
        )
        .await
        .expect("history query must not error")
        .expect("a live response document must have a provable history");
        assert_eq!(history.len(), 3, "create + two updates = three composites");
        assert_eq!(
            history.iter().map(|s| s.height).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "heights must be ordered and consecutive"
        );
        assert_eq!(history[0].content(), "abc", "the create snapshot");
        assert_eq!(history[1].content(), "", "the reset snapshot");
        assert_eq!(history[2].content(), "abc", "the rewrite snapshot");
        assert_eq!(history[2].row().progress_seq, Some(3));
        for snapshot in &history {
            assert_eq!(snapshot.row().doc_id.as_deref(), Some(doc_id.as_str()));
            assert_eq!(snapshot.row().request_id, "hist-1");
        }

        let unchanged = observe_history_with_sink(
            &NodeSink { node: &node },
            &mut observation,
            &doc_id,
            "hist-1",
        )
        .await
        .expect("unchanged query must not error")
        .expect("unchanged history remains proven");
        assert_eq!(unchanged, history);

        // A request-id mismatch makes the same document unprovable.
        assert!(
            observe_history_with_sink(
                &NodeSink { node: &node },
                &mut observation,
                &doc_id,
                "hist-OTHER",
            )
            .await
            .expect("query must not error")
            .is_none(),
            "a snapshot chain for a different request must be unavailable"
        );
    }
}
