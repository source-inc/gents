//! The durable fact record for one provider call (#840), and the typed trace
//! that makes it explainable (#523).
//!
//! Two things live here and they are deliberately separate:
//!
//! * `RenderedCompletionRequest` is the *capture DTO*. It carries the exact
//!   provider request body plus the identity, routing, and provenance a
//!   `RenderedRequest` row needs. It is built at the **transport seam** — the
//!   last `HttpClientExt` before the network client — and handed to the capture
//!   sink, which must succeed before the body is forwarded.
//! * `AssemblyTrace` is the *leak set*. Prompt assembly reads durable
//!   documents, but four of its inputs are created in memory and never written
//!   anywhere a reconstructor could find them. Those four are enumerated on
//!   `AssemblyTrace` with the citation that proves each one is lost.
//!
//! ## Why the transport seam and not the assembled request
//!
//! Two of the four provider kinds rewrite the body *after* rig serializes it.
//! ChatGPT-Codex hoists the first system text into a top-level `instructions`,
//! strips system items from `input`, sets `store:false` and `stream:true`,
//! deletes `max_output_tokens`/`temperature`/`top_p`, and forces
//! `strict:false` on every tool (`chatgpt_codex::patch_instructions_body`).
//! Grok injects `store:false` (`xai_grok_oauth::patch_store_false`). The
//! OpenAI-compatible Responses stack additionally rewrites prior assistant
//! items (`inference_http::ResponsesNormalizingHttpClient`). Capturing the
//! assembled request would therefore make the equality claim false for most of
//! the fleet, and re-applying those rewrites at capture time would be a second
//! implementation that can drift from the one that actually ran.
//!
//! `crate::rendered_request::transport::RenderedRequestCapturingHttpClient` is
//! installed as the innermost transport wrapper, so it observes the body every
//! outer wrapper has finished editing and nothing can reach the network without
//! passing through it. See `transport.rs` for the fail-closed contract and
//! `scope.rs` for how the loop's per-attempt facts reach it.
//!
//! ## Integrity is the field commit, not a column
//!
//! There is no `request_hash`. A stored digest is self-attested: the same code
//! that chooses the bytes also chooses the digest, so the two always agree and
//! an auditor learns nothing. DefraDB instead writes a per-field commit block
//! for `request_json` whose CID is computed over the value actually stored.
//! That CID is the content address, it replicates with the document, and it is
//! what a future Merkle-DAG proof can attest over.
//!
//! Per-field and composite commit blocks are written for **every** collection;
//! `@branchable` gates only the additional collection-level block. So the field
//! CID this record relies on is not a consequence of `@branchable` — that
//! directive is taken for replication and collection-scoped ACP reasons, which
//! `rendered_request.graphql` explains.
//!
//! `prompt_hash` and `tools_hash` survive only as query indexes — "find every
//! capture sharing this tool surface". Treating either as proof of content is a
//! bug.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::llm::message::{Message, ToolResultContent, UserContent};

pub(crate) mod scope;
pub mod sink;
pub(crate) mod transport;

pub use scope::CaptureScopeKind;
pub(crate) use sink::defra_rendered_request_capture_factory;
pub use sink::DefraRenderedRequestSink;
pub use transport::RenderedRequestCapturingHttpClient;

/// Capture format version stamped onto every row. Bump when the *set of
/// columns* a reader must understand changes.
pub const CAPTURE_VERSION: u32 = 1;

/// Provenance manifest version. Bump when `ProvenanceManifest`'s serialized
/// shape changes. A reader that does not know this number must report
/// `UnsupportedManifest` rather than guessing.
pub const PROVENANCE_MANIFEST_VERSION: u32 = 2;

/// Assembly-trace version. Bump when `AssemblyTrace`'s serialized shape
/// changes. Versioned independently of the manifest so a manifest that later
/// gains pinned config CIDs does not have to re-version the trace.
pub const ASSEMBLY_TRACE_VERSION: u32 = 2;

/// Prefix on every capture key. Bound to the *key derivation*, not to
/// `CAPTURE_VERSION`: adding a column must not silently re-key existing facts.
const CAPTURE_KEY_PREFIX: &str = "rendered:v1";

/// Request paths whose body is a completion request, and the wire shape each
/// one implies. The capturing transport only claims a pending capture for one
/// of these, so a `/models` listing or a token-refresh call issued while a
/// capture is armed passes through untouched instead of consuming — and
/// mis-describing — the turn's fact.
const COMPLETION_REQUEST_PATHS: &[(&str, RenderedRequestSource)] = &[
    ("/responses", RenderedRequestSource::OpenAiResponses),
    (
        "/chat/completions",
        RenderedRequestSource::OpenAiChatCompletions,
    ),
];

pub(crate) type RenderedRequestCaptureSink = Arc<
    dyn Fn(RenderedCompletionRequest) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>
        + Send
        + Sync,
>;

pub(crate) type RenderedRequestCaptureFactory =
    Arc<dyn Fn(RenderedRequestContext) -> RenderedRequestCaptureSink + Send + Sync>;

/// The provider wire shape a captured body was actually sent on.
///
/// Derived from the request path the transport posted to, never from behavior
/// configuration: configuration says what the runtime *intended*, and this
/// column has to say what the provider *received*. The two can disagree — a
/// backend document can be edited between reconcile and send.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RenderedRequestSource {
    #[serde(rename = "openai_responses")]
    OpenAiResponses,
    #[serde(rename = "openai_chat_completions")]
    OpenAiChatCompletions,
}

impl RenderedRequestSource {
    /// Classify by the completion request path. `None` means "not a completion
    /// request", which is the transport's signal to forward without capturing.
    pub(crate) fn for_request_path(path: &str) -> Option<Self> {
        COMPLETION_REQUEST_PATHS
            .iter()
            .find(|(suffix, _)| path.ends_with(suffix))
            .map(|(_, source)| *source)
    }

    /// The body field carrying the provider message list on this wire shape.
    fn messages_field(self) -> &'static str {
        match self {
            Self::OpenAiResponses => "input",
            Self::OpenAiChatCompletions => "messages",
        }
    }
}

/// Which of the owned loop's two request builders produced the captured
/// `CompletionRequest`.
///
/// This is one of the four unrecoverable inputs. `build_budgeted_request`
/// applies `clamp_request_output_budget` before returning
/// (`agent/loop_stream.rs`), but the completion-retry `Repair` directive calls
/// `build_request` directly (`agent/loop_stream.rs:353,447`) and never clamps.
/// A repaired attempt therefore carries the raw configured `max_tokens` while
/// the original attempt for the same turn carries the clamped one. Without this
/// discriminator a reconstructor cannot tell which of the two it should
/// reproduce, and both are legal.
///
/// The clamp *value* is deliberately not stored: it is a pure function of the
/// assembled request plus durable config, and `completion_request_input_estimate`
/// does not read `max_tokens`, so a single reconstruction pass reproduces it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssemblyBuildPath {
    /// `build_budgeted_request`: ordinary assembly, output clamp applied, and
    /// per-turn compaction when the request exceeded the input budget.
    Budgeted,
    /// `build_request` invoked directly by a completion-retry repair. No output
    /// clamp is applied on this path.
    Repair,
}

impl AssemblyBuildPath {
    /// Whether the loop applied `clamp_request_output_budget` on this path.
    pub fn applies_output_clamp(self) -> bool {
        matches!(self, Self::Budgeted)
    }
}

/// A provider-assigned assistant message id, positioned in the effective
/// message list.
///
/// One of the four unrecoverable inputs. `close_streaming_turn` stamps the
/// provider's `MessageId` event onto the threaded assistant message
/// (`agent/loop_stream.rs:802-806`) because OpenAI Responses and ChatGPT Codex
/// follow-up requests reference prior `msg_` ids. The persistence path builds
/// its assistant message with `id: None`
/// (`agent/stream_processor.rs:305`), so the id exists in the provider request
/// and nowhere in the durable transcript.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantMessageId {
    /// Index into `AssemblyTrace::effective_messages`.
    pub message_index: usize,
    pub message_id: String,
}

/// The exact tool-result content threaded back into provider history for one
/// tool call.
///
/// One of the four unrecoverable inputs. The loop threads
/// `truncate_text(outcome.model_facing_text(), tool_result_truncation_mode(name),
/// &TruncationLimits::default())` (`agent/loop_stream.rs:655-658`). Persistence
/// re-derives its text from the stored `AgentToolCall.result` with
/// `TruncationMode::Head`, the hook's own `truncation_limits`, and
/// `model_observation_for_tool_result`
/// (`hook/persistence/message_spawn.rs:296-324`). Those are different functions
/// over different inputs, so replaying from the transcript does not reproduce
/// the bytes the model actually saw.
///
/// `content` is the full threaded `Vec<ToolResultContent>`, not a flattened
/// string: `ToolResultContent::from_tool_output` can split a JSON payload into
/// several parts, and that split is part of what the provider received.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ThreadedToolResult {
    /// Index into `AssemblyTrace::effective_messages`.
    pub message_index: usize,
    /// `ToolResult.id` — rig's locally minted tool-call id.
    pub tool_call_id: String,
    /// `ToolResult.call_id` — the provider-side call id when one exists.
    pub call_id: Option<String>,
    pub content: Vec<ToolResultContent>,
}

/// The genuinely unrecoverable inputs to one rendered provider request.
///
/// Everything else that shapes a request is either durable (transcript rows,
/// behavior/profile/backend/skill documents) or a pure function of durable data.
/// These four are not:
///
/// 1. `assistant_message_ids` — provider-assigned, persisted as `None`.
/// 2. `threaded_tool_results` — the loop and the persistence path derive
///    different text from different sources.
/// 3. `effective_messages` — when a rendered request-context message or
///    per-turn compaction adds ephemeral content. Compaction is a *sticky*
///    mutation
///    (`*history = compacted; *new_messages = vec![compacted_prompt]`,
///    `agent/loop_stream.rs:1350-1351`), so one turn's model-generated summary
///    governs every later turn of the same request, and that summary is never
///    written as an `AgentCompactionEntry`. Re-running the summarizer does not
///    produce the same words.
/// 4. `build_path` — see `AssemblyBuildPath`.
///
/// `assistant_message_ids` and `threaded_tool_results` are projections of the
/// effective message list, derived by the same constructor so they cannot drift
/// from it.
/// They are carried explicitly because a reconstructor rebuilds its message
/// list from `AgentMessage` rows and needs these as an *overlay* keyed by
/// position and call id; `effective_messages` is the oracle it checks itself
/// against. `effective_messages` is present only when that list contains a
/// rendered request-context message, a model-generated per-turn compaction
/// summary, or the result of a repair rewrite. Otherwise the durable transcript
/// plus these overlays reconstructs it exactly.
///
/// ## Size
///
/// Ordinary turns do not retain the full native message list next to the
/// provider-wire copy in `request_json`; that list is reconstructible from
/// durable rows plus the overlays. Once request-context rendering, per-turn
/// compaction, or a repair introduces content no durable row reproduces, the
/// full native list becomes the oracle and is retained on that and every later
/// turn of the request. Compacted lists are bounded by the context window
/// rather than growing with the unabridged session.
///
/// `threaded_tool_results` is carried on **every** turn, compact path included,
/// because rig joins multi-part tool-result content with `"\n"` on the Chat
/// Completions wire — the native `Vec<ToolResultContent>` split is genuinely
/// unrecoverable from `request_json`. So a tool-heavy turn does duplicate its
/// tool-result payloads; only the surrounding message list is elided.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssemblyTrace {
    pub trace_version: u32,
    pub build_path: AssemblyBuildPath,
    /// Number of native messages passed to assembly. This validates positional
    /// overlays even when the reconstructible common-path list is omitted.
    pub effective_message_count: usize,
    /// The full effective provider message list at capture time, retained only
    /// after request-context rendering or per-turn compaction introduces
    /// ephemeral content. Native `Message`s, not provider wire shapes: this is
    /// the *input* to assembly, whereas `request_json` is its output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_messages: Option<Vec<Message>>,
    pub assistant_message_ids: Vec<AssistantMessageId>,
    pub threaded_tool_results: Vec<ThreadedToolResult>,
}

impl AssemblyTrace {
    /// The only constructor that keeps the overlays consistent with
    /// `effective_messages`. Build traces with this, never with a struct
    /// literal.
    pub fn from_effective_messages(
        build_path: AssemblyBuildPath,
        effective_messages: Vec<Message>,
    ) -> Self {
        Self::from_messages(build_path, effective_messages, true)
    }

    /// Build the compact common-path trace when the native list can be rebuilt
    /// from durable transcript/configuration documents plus the overlays.
    pub(crate) fn from_reconstructible_messages(
        build_path: AssemblyBuildPath,
        effective_messages: Vec<Message>,
    ) -> Self {
        Self::from_messages(build_path, effective_messages, false)
    }

    fn from_messages(
        build_path: AssemblyBuildPath,
        effective_messages: Vec<Message>,
        retain_effective_messages: bool,
    ) -> Self {
        let mut assistant_message_ids = Vec::new();
        let mut threaded_tool_results = Vec::new();

        for (message_index, message) in effective_messages.iter().enumerate() {
            match message {
                Message::Assistant {
                    id: Some(message_id),
                    ..
                } => assistant_message_ids.push(AssistantMessageId {
                    message_index,
                    message_id: message_id.clone(),
                }),
                Message::User { content } => {
                    for item in content {
                        if let UserContent::ToolResult(result) = item {
                            threaded_tool_results.push(ThreadedToolResult {
                                message_index,
                                tool_call_id: result.id.clone(),
                                call_id: result.call_id.clone(),
                                content: result.content.clone(),
                            });
                        }
                    }
                }
                _ => {}
            }
        }

        let effective_message_count = effective_messages.len();
        Self {
            trace_version: ASSEMBLY_TRACE_VERSION,
            build_path,
            effective_message_count,
            effective_messages: retain_effective_messages.then_some(effective_messages),
            assistant_message_ids,
            threaded_tool_results,
        }
    }
}

/// How much this capture claims about reconstructibility.
///
/// Explicit, never inferred from an absent field. A manifest that simply omits
/// pinned CIDs must not read as "nothing needed pinning".
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProvenanceStatus {
    /// The rendered request is durable and exact, but no durable source
    /// versions are pinned alongside it, so a reconstruction cannot be
    /// verified against it. This is the only status version 1 emits.
    CapturedOnly,
}

/// Where in the send path the captured bytes were read.
///
/// Recorded positively so a reader never has to infer it. A row that says
/// `TransportBody` is claiming the stronger thing: these are the bytes the HTTP
/// client forwarded, after every provider-specific rewrite.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureSeam {
    /// The last `HttpClientExt` before the network client. The only seam
    /// version 1 emits.
    TransportBody,
}

/// Versioned provenance travelling in the `provenance_json` column.
///
/// Version 1 carries the assembly trace, the seam the bytes came from, the
/// capture scope, and an honest `CapturedOnly` status. Pinned
/// config/transcript CIDs are a later version; when they arrive, a version-1
/// row must still be readable and must still report `CapturedOnly`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProvenanceManifest {
    pub manifest_version: u32,
    pub status: ProvenanceStatus,
    /// Why this row is not `Verified`, in words, so a projection can say so
    /// without the reader reverse-engineering it from missing fields.
    pub status_reason: String,
    pub capture_seam: CaptureSeam,
    /// Mirrors the `capture_scope` column. Duplicated here so a manifest read
    /// in isolation still identifies which completion loop it describes.
    pub capture_scope: String,
    /// Scheme and authority the body was actually posted to, observed at the
    /// seam. `None` only when the URI carried no authority.
    ///
    /// This is the one routing fact that is otherwise lost in time. `model_name`
    /// alone cannot distinguish OpenAI from OpenRouter from a local vLLM from
    /// Grok, and for daemon requests the answer is recoverable only by joining
    /// to `InferenceCall.backend_id` — a join that does not exist for one-shot
    /// runs, which never enter an admission scope. Configuration says where the
    /// bytes were meant to go; this says where they went.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_endpoint: Option<String>,
    pub assembly_trace: AssemblyTrace,
}

impl ProvenanceManifest {
    const CAPTURED_ONLY_REASON: &'static str =
        "this provenance manifest pins no config or transcript versions, so a \
         reconstruction cannot be verified against this capture";

    pub fn captured_only(
        capture_scope: String,
        provider_endpoint: Option<String>,
        assembly_trace: AssemblyTrace,
    ) -> Self {
        Self {
            manifest_version: PROVENANCE_MANIFEST_VERSION,
            status: ProvenanceStatus::CapturedOnly,
            status_reason: Self::CAPTURED_ONLY_REASON.to_string(),
            capture_seam: CaptureSeam::TransportBody,
            capture_scope,
            provider_endpoint,
            assembly_trace,
        }
    }
}

/// Identity and routing for every capture belonging to one request.
///
/// Deliberately carries no provider-wire information: which wire shape was used
/// and which model the body named are read back off the captured body itself,
/// because configuration describes intent and this record describes what
/// happened.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenderedRequestContext {
    /// Exact DefraDB document identity for the request being served. Unlike the
    /// logical `request_id`, this identifies one signed document even if an
    /// invalid duplicate logical id exists in the collection. Empty only for a
    /// one-shot run, which does not author an `AgentRequest` document.
    pub request_doc_id: String,
    pub request_id: String,
    pub agent_did: String,
    /// The requesting principal. Empty when the request has none — an empty DID
    /// is never a participant, so downstream authorization must treat `""` as
    /// "owner only" rather than as a DID.
    pub requester_did: String,
    pub behavior_id: String,
    pub session_id: String,
    /// The behavior's configured model. Used only when the captured body names
    /// no `model`.
    pub model_name: String,
}

impl RenderedRequestContext {
    pub(crate) fn for_request(request: &crate::watcher::AgentRequest, model_name: String) -> Self {
        Self {
            request_doc_id: request.doc_id.clone(),
            request_id: request.request_id.clone(),
            agent_did: request.agent_did.clone(),
            requester_did: request.requester_did.clone().unwrap_or_default(),
            behavior_id: request.behavior_id.clone().unwrap_or_default(),
            session_id: request.session_id.clone(),
            model_name,
        }
    }
}

/// The JSON pieces extracted from one captured provider body. Grouped so the
/// DTO builder keeps a readable arity.
pub(crate) struct RenderedRequestComponents {
    /// The complete provider request. This is the fact record; the four fields
    /// below are query conveniences derived from it.
    pub(crate) request_json: Value,
    pub(crate) messages_json: Value,
    /// Complete provider prompt surface used for `prompt_hash`. Responses
    /// carries system text separately in `instructions`; Chat Completions has
    /// no prompt-bearing field outside `messages`.
    pub(crate) prompt_json: Value,
    pub(crate) tools_json: Value,
    pub(crate) tool_choice_json: Value,
    pub(crate) sampling_json: Value,
}

impl RenderedRequestComponents {
    /// Split a captured provider body into the payload plus the four derived
    /// views the DTO indexes over.
    ///
    /// Every view is read out of the body, never re-derived from the assembled
    /// `CompletionRequest`: on the Codex path the assembled request still
    /// carries `temperature`/`max_output_tokens` that the transport deleted,
    /// and a row that reported those would be describing a request nobody sent.
    pub(crate) fn from_provider_body(request_json: Value, source: RenderedRequestSource) -> Self {
        let field = |name: &str| request_json.get(name).cloned();
        let messages_json =
            field(source.messages_field()).unwrap_or_else(|| Value::Array(Vec::new()));
        let prompt_json = match source {
            RenderedRequestSource::OpenAiResponses => json!({
                "instructions": field("instructions").unwrap_or(Value::Null),
                "input": messages_json.clone(),
            }),
            RenderedRequestSource::OpenAiChatCompletions => messages_json.clone(),
        };
        let tools_json = field("tools").unwrap_or_else(|| Value::Array(Vec::new()));
        let tool_choice_json = field("tool_choice").unwrap_or(Value::Null);
        let sampling_json = json!({
            "temperature": field("temperature").unwrap_or(Value::Null),
            "top_p": field("top_p").unwrap_or(Value::Null),
            // Chat Completions calls it `max_tokens`; Responses calls it
            // `max_output_tokens`. Codex deletes both, and `null` here is the
            // honest report of that.
            "max_tokens": field("max_tokens")
                .or_else(|| field("max_output_tokens"))
                .unwrap_or(Value::Null),
            "reasoning": field("reasoning").unwrap_or(Value::Null),
        });

        Self {
            request_json,
            messages_json,
            prompt_json,
            tools_json,
            tool_choice_json,
            sampling_json,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenderedCompletionRequest {
    /// `capture_key(agent_did, session_id, request_doc_id, capture_scope,
    /// turn_index, attempt)`. The unique index on the durable row and the
    /// idempotency key of the sink.
    pub capture_key: String,
    pub capture_version: u32,
    /// Exact `_docID` of the durable `AgentRequest`. The logical `request_id`
    /// remains alongside it for user-facing correlation and queries. Empty for
    /// a one-shot run, which has no `AgentRequest` document.
    pub request_doc_id: String,
    pub request_id: String,
    /// Which completion loop inside the request issued this call, e.g.
    /// `inference.1` or `compaction.2`. See [`CaptureScopeKind`].
    pub capture_scope: String,
    pub turn_index: usize,
    pub attempt: u32,
    pub agent_did: String,
    /// Empty when the request carried no requester DID.
    pub requester_did: String,
    pub behavior_id: String,
    pub session_id: String,
    pub model_name: String,
    pub source: RenderedRequestSource,
    /// The complete rendered provider request. Retained in full: component
    /// hashes are indexes, not a substitute for the payload.
    pub request_json: Value,
    pub messages_json: Value,
    pub tools_json: Value,
    pub tool_choice_json: Value,
    pub sampling_json: Value,
    /// Query index over the complete provider prompt surface: `messages` for
    /// Chat Completions, and `instructions` plus `input` for Responses. Not an
    /// integrity mechanism.
    pub prompt_hash: String,
    /// Query index over `tools_json`. Not an integrity mechanism.
    pub tools_hash: String,
    pub assembly_trace: AssemblyTrace,
    /// Canonical JSON of the `ProvenanceManifest` built from `assembly_trace`.
    /// Derived by the builder so the column and the typed value cannot
    /// disagree; a reader may deserialize it back into `ProvenanceManifest`.
    pub provenance_json: Value,
}

pub(crate) fn build_rendered_completion_request(
    context: &RenderedRequestContext,
    capture_scope: &str,
    source: RenderedRequestSource,
    provider_endpoint: Option<String>,
    turn_index: usize,
    attempt: u32,
    assembly_trace: AssemblyTrace,
    components: RenderedRequestComponents,
) -> Result<RenderedCompletionRequest> {
    let RenderedRequestComponents {
        request_json,
        messages_json,
        prompt_json,
        tools_json,
        tool_choice_json,
        sampling_json,
    } = components;

    let prompt_hash = sha256_canonical_json(&prompt_json)?;
    let tools_hash = sha256_canonical_json(&tools_json)?;
    let capture_key = capture_key(
        &context.agent_did,
        &context.session_id,
        &context.request_doc_id,
        capture_scope,
        turn_index,
        attempt,
    )?;
    let manifest = ProvenanceManifest::captured_only(
        capture_scope.to_string(),
        provider_endpoint,
        assembly_trace.clone(),
    );
    let provenance_json = canonical_json(
        &serde_json::to_value(&manifest).context("encoding rendered-request provenance")?,
    );
    let model_name = request_json
        .get("model")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| context.model_name.clone());

    Ok(RenderedCompletionRequest {
        capture_key,
        capture_version: CAPTURE_VERSION,
        request_doc_id: context.request_doc_id.clone(),
        request_id: context.request_id.clone(),
        capture_scope: capture_scope.to_string(),
        turn_index,
        attempt,
        agent_did: context.agent_did.clone(),
        requester_did: context.requester_did.clone(),
        behavior_id: context.behavior_id.clone(),
        session_id: context.session_id.clone(),
        model_name,
        source,
        request_json,
        messages_json,
        tools_json,
        tool_choice_json,
        sampling_json,
        prompt_hash,
        tools_hash,
        assembly_trace,
        provenance_json,
    })
}

/// Derive the durable capture key from the five-component identity tuple.
///
/// The tuple is exactly the one `Proofs/RenderedCapture.lean` quantifies over
/// with componentwise equality, and it is encoded as a canonical JSON *array* —
/// never as a delimited concatenation. JSON string escaping keeps the encoding
/// injective, so no component value can be chosen to forge another tuple's key.
/// That matters concretely: `session_id` is caller-controlled and unvalidated
/// (`ChatArgs::session_id` has no `value_parser`), and a `"{a}:{b}"` format
/// would let `("x:y", "z")` and `("x", "y:z")` collide into one fact.
///
/// `request_doc_id` is the DefraDB `_docID` of the durable `AgentRequest`, not its
/// user-facing `request_id`. The latter is an indexed logical correlation id,
/// but the document id is the provenance edge that identifies the exact request
/// fact this capture belongs to.
///
/// ## Why the third component is a pair
///
/// The Lean model's `requestId` names *the provider-call scope inside a
/// request*, and one request runs more than one completion loop: the owned
/// inference loop, the per-turn compaction summarizer (guided, plus a JSON
/// fallback), and conversation title generation. Each of those is a separate
/// `run_loop_stream` whose turn and attempt counters start at zero, so
/// `(request_id, 0, 0)` would name several different provider calls. The scope
/// therefore rides *inside* the third component as the nested JSON array
/// `[request_doc_id, capture_scope]`, which keeps the tuple five components wide
/// and componentwise-injective — a `"{request_doc_id}#{scope}"` string would
/// reintroduce exactly the delimiter collision the array encoding exists to
/// rule out.
pub fn capture_key(
    agent_did: &str,
    session_id: &str,
    request_doc_id: &str,
    capture_scope: &str,
    turn_index: usize,
    attempt: u32,
) -> Result<String> {
    let tuple = json!([
        agent_did,
        session_id,
        [request_doc_id, capture_scope],
        turn_index,
        attempt
    ]);
    let digest = sha256_canonical_json(&tuple)?;
    Ok(format!("{CAPTURE_KEY_PREFIX}:{digest}"))
}

/// The one canonical JSON encoder. Persisted bytes, component hashes, and the
/// capture key all go through it; there is deliberately no second
/// implementation in the sink or the reconstructor.
///
/// Key order is not free: `serde_json::Map` becomes an insertion-ordered
/// `IndexMap` for the whole build whenever any crate in the graph enables
/// `serde_json/preserve_order` — `schemars` does, via `tauri-build`. Without an
/// imposed order the "same" request would hash differently depending on which
/// workspace members were compiled.
pub(crate) fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonical_json).collect()),
        Value::Object(map) => {
            let sorted = map
                .iter()
                .map(|(key, value)| (key.clone(), canonical_json(value)))
                .collect::<BTreeMap<_, _>>();
            Value::Object(sorted.into_iter().collect())
        }
        other => other.clone(),
    }
}

/// The exact UTF-8 bytes to persist for a canonical JSON column, and the exact
/// bytes `sha256_canonical_json` digests.
pub(crate) fn canonical_json_string(value: &Value) -> Result<String> {
    serde_json::to_string(&canonical_json(value)).context("encoding canonical JSON")
}

/// SHA-256 of `canonical_json_string`, lowercase hex.
pub(crate) fn sha256_canonical_json(value: &Value) -> Result<String> {
    let digest = Sha256::digest(canonical_json_string(value)?.as_bytes());
    Ok(format!("{digest:x}"))
}

#[cfg(test)]
mod tests {
    use gents_protocol::message::{AssistantContent, Text, ToolCall, ToolFunction};
    use serde_json::json;

    use super::*;

    fn context() -> RenderedRequestContext {
        RenderedRequestContext {
            request_doc_id: "doc-1".to_string(),
            request_id: "req-1".to_string(),
            agent_did: "did:key:test".to_string(),
            requester_did: "did:key:requester".to_string(),
            behavior_id: "behavior".to_string(),
            session_id: "session".to_string(),
            model_name: "test-model".to_string(),
        }
    }

    fn chat_body() -> Value {
        json!({
            "model": "test-model",
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [{"type": "function", "function": {"name": "read_file"}}],
            "temperature": 0.2,
            "max_tokens": 512,
        })
    }

    fn components() -> RenderedRequestComponents {
        RenderedRequestComponents::from_provider_body(
            chat_body(),
            RenderedRequestSource::OpenAiChatCompletions,
        )
    }

    fn build(
        turn_index: usize,
        attempt: u32,
        trace: AssemblyTrace,
        components: RenderedRequestComponents,
    ) -> RenderedCompletionRequest {
        build_rendered_completion_request(
            &context(),
            "inference.1",
            RenderedRequestSource::OpenAiChatCompletions,
            Some("https://api.example.test".to_string()),
            turn_index,
            attempt,
            trace,
            components,
        )
        .expect("rendered request")
    }

    fn empty_trace() -> AssemblyTrace {
        AssemblyTrace::from_effective_messages(AssemblyBuildPath::Budgeted, Vec::new())
    }

    fn tool_result_message(id: &str, call_id: Option<&str>, text: &str) -> Message {
        Message::User {
            content: vec![match call_id {
                Some(call_id) => UserContent::tool_result_with_call_id(
                    id,
                    call_id.to_string(),
                    vec![ToolResultContent::text(text)],
                ),
                None => UserContent::tool_result(id, vec![ToolResultContent::text(text)]),
            }],
        }
    }

    fn assistant_with_tool_call(id: Option<&str>, tool_call_id: &str) -> Message {
        Message::Assistant {
            id: id.map(str::to_string),
            content: vec![AssistantContent::ToolCall(ToolCall::new(
                tool_call_id.to_string(),
                ToolFunction {
                    name: "read_file".to_string(),
                    arguments: json!({"path": "a.txt"}),
                },
            ))],
        }
    }

    #[test]
    fn canonical_hash_sorts_object_keys() {
        let left = json!({ "b": 1, "a": { "d": 2, "c": 3 } });
        let right = json!({ "a": { "c": 3, "d": 2 }, "b": 1 });

        assert_eq!(
            sha256_canonical_json(&left).unwrap(),
            sha256_canonical_json(&right).unwrap()
        );
    }

    /// The persisted bytes and the digest must come from the same encoder. If a
    /// second serialization ever creeps into the sink, this is the test that
    /// notices the digest no longer describes the stored string.
    #[test]
    fn component_hashes_digest_exactly_the_canonical_bytes() {
        let value = json!({ "b": [3, {"z": 1, "a": 2}], "a": "x" });

        let bytes = canonical_json_string(&value).unwrap();
        assert_eq!(bytes, r#"{"a":"x","b":[3,{"a":2,"z":1}]}"#);

        let digest = Sha256::digest(bytes.as_bytes());
        assert_eq!(
            sha256_canonical_json(&value).unwrap(),
            format!("{digest:x}")
        );
    }

    #[test]
    fn canonical_json_sorts_nested_arrays_of_objects() {
        let value = json!([{ "b": 1, "a": 2 }, { "d": 3, "c": 4 }]);
        assert_eq!(
            canonical_json_string(&value).unwrap(),
            r#"[{"a":2,"b":1},{"c":4,"d":3}]"#
        );
    }

    /// Reconstruction compares the *parsed* persisted string against a freshly
    /// rendered request. That comparison is only meaningful if canonicalization
    /// reorders keys and changes nothing else — array order, numeric form,
    /// nulls, and escapes all have to survive the round trip.
    #[test]
    fn canonical_bytes_parse_back_to_the_same_value() {
        let value = json!({
            "model": "test-model",
            "messages": [
                { "role": "system", "content": "b\u{0007}\"quoted\"\n" },
                { "role": "user", "content": null },
            ],
            "tools": [],
            "temperature": 0.2,
            "max_tokens": 512,
            "nested": { "z": [1, 2, 3], "a": { "deep": true } },
        });

        let parsed: Value = serde_json::from_str(&canonical_json_string(&value).unwrap()).unwrap();
        assert_eq!(parsed, value);
        assert_eq!(parsed, canonical_json(&value));
    }

    /// Absent tools and tool choice must still hash — an empty list is a real
    /// tool surface, not a missing one, and it has to be findable by index.
    #[test]
    fn empty_tools_and_tool_choice_still_produce_component_hashes() {
        let rendered = build(
            0,
            0,
            empty_trace(),
            RenderedRequestComponents::from_provider_body(
                json!({"model": "test-model", "tools": [], "messages": []}),
                RenderedRequestSource::OpenAiChatCompletions,
            ),
        );

        assert_eq!(
            rendered.prompt_hash,
            sha256_canonical_json(&json!([])).unwrap()
        );
        assert_eq!(
            rendered.tools_hash,
            sha256_canonical_json(&json!([])).unwrap()
        );
        assert_eq!(rendered.tool_choice_json, Value::Null);
        assert_eq!(rendered.sampling_json["temperature"], Value::Null);
    }

    /// The wire shape is read off the path the transport posted to, so the
    /// column reports what was sent rather than what was configured.
    #[test]
    fn rendered_source_follows_the_request_path() {
        assert_eq!(
            RenderedRequestSource::for_request_path("/v1/responses"),
            Some(RenderedRequestSource::OpenAiResponses)
        );
        assert_eq!(
            RenderedRequestSource::for_request_path("/backend-api/codex/responses"),
            Some(RenderedRequestSource::OpenAiResponses)
        );
        assert_eq!(
            RenderedRequestSource::for_request_path("/v1/chat/completions"),
            Some(RenderedRequestSource::OpenAiChatCompletions)
        );
        assert_eq!(RenderedRequestSource::for_request_path("/v1/models"), None);
        assert_eq!(RenderedRequestSource::for_request_path("/key"), None);
    }

    /// Every suffix the capture path claims must classify to the wire shape the
    /// table names, and the table must stay the only definition of "completion
    /// request".
    #[test]
    fn completion_path_suffixes_all_classify() {
        for (suffix, source) in COMPLETION_REQUEST_PATHS {
            assert_eq!(
                RenderedRequestSource::for_request_path(suffix),
                Some(*source),
                "{suffix} must classify as a completion path"
            );
        }
    }

    /// A Responses body names its message list `input`, and the derived views
    /// have to follow the body rather than a configured wire API.
    #[test]
    fn responses_bodies_index_the_input_field() {
        let components = RenderedRequestComponents::from_provider_body(
            json!({
                "model": "gpt-5.2",
                "instructions": "hoisted",
                "input": [{"role": "user", "content": "hi"}],
                "max_output_tokens": 4096,
            }),
            RenderedRequestSource::OpenAiResponses,
        );
        assert_eq!(components.messages_json[0]["role"], "user");
        assert_eq!(components.sampling_json["max_tokens"], 4096);
    }

    #[test]
    fn responses_prompt_hash_includes_hoisted_instructions() {
        let build_responses = |instructions: &str| {
            build_rendered_completion_request(
                &context(),
                "inference.1",
                RenderedRequestSource::OpenAiResponses,
                Some("https://api.example.test".to_string()),
                0,
                0,
                empty_trace(),
                RenderedRequestComponents::from_provider_body(
                    json!({
                        "model": "gpt-5.2",
                        "instructions": instructions,
                        "input": [{"role": "user", "content": "same input"}],
                    }),
                    RenderedRequestSource::OpenAiResponses,
                ),
            )
            .expect("rendered Responses request")
        };

        let first = build_responses("first system prompt");
        let second = build_responses("different system prompt");
        assert_eq!(first.messages_json, second.messages_json);
        assert_ne!(first.prompt_hash, second.prompt_hash);
    }

    /// Codex deletes `max_output_tokens`, `temperature`, and `top_p` from the
    /// body. The row has to say `null`, not the value the loop assembled.
    #[test]
    fn a_stripped_sampling_parameter_reads_as_null() {
        let components = RenderedRequestComponents::from_provider_body(
            json!({ "model": "gpt-5.2", "input": [], "store": false }),
            RenderedRequestSource::OpenAiResponses,
        );
        assert_eq!(components.sampling_json["max_tokens"], Value::Null);
        assert_eq!(components.sampling_json["temperature"], Value::Null);
        assert_eq!(components.sampling_json["top_p"], Value::Null);
    }

    #[test]
    fn rendered_completion_request_hashes_prompt_and_tools() {
        let rendered = build(0, 0, empty_trace(), components());

        assert_eq!(rendered.request_id, "req-1");
        assert_eq!(rendered.capture_scope, "inference.1");
        assert_eq!(rendered.turn_index, 0);
        assert_eq!(rendered.attempt, 0);
        assert_eq!(rendered.requester_did, "did:key:requester");
        assert_eq!(rendered.capture_version, CAPTURE_VERSION);
        assert_eq!(
            rendered.source,
            RenderedRequestSource::OpenAiChatCompletions
        );
        assert_eq!(rendered.messages_json[0]["role"], "user");
        assert_eq!(rendered.tools_json[0]["function"]["name"], "read_file");
        assert_eq!(rendered.sampling_json["temperature"], 0.2);
        assert_eq!(rendered.sampling_json["max_tokens"], 512);
        assert_eq!(rendered.prompt_hash.len(), 64);
        assert_eq!(rendered.tools_hash.len(), 64);
    }

    /// The payload survives capture. Component hashes are indexes; they never
    /// replace `request_json`.
    #[test]
    fn rendered_completion_request_retains_the_full_payload() {
        let rendered = build(0, 0, empty_trace(), components());

        assert_eq!(rendered.request_json, chat_body());
    }

    /// The `model_name` column reports the model the provider was asked for.
    /// A behavior document edited between reconcile and send must not make the
    /// row disagree with the body.
    #[test]
    fn model_name_comes_from_the_captured_body() {
        let rendered = build(
            0,
            0,
            empty_trace(),
            RenderedRequestComponents::from_provider_body(
                json!({"model": "wire-model", "messages": []}),
                RenderedRequestSource::OpenAiChatCompletions,
            ),
        );
        assert_eq!(rendered.model_name, "wire-model");

        let bodyless = build(
            0,
            0,
            empty_trace(),
            RenderedRequestComponents::from_provider_body(
                json!({"messages": []}),
                RenderedRequestSource::OpenAiChatCompletions,
            ),
        );
        assert_eq!(bodyless.model_name, "test-model");
    }

    #[test]
    fn capture_key_is_stable_and_prefixed() {
        let key = capture_key("did:key:a", "session-1", "doc-1", "inference.1", 3, 2).unwrap();
        assert!(key.starts_with("rendered:v1:"), "unexpected key {key}");
        assert_eq!(key.len(), "rendered:v1:".len() + 64);
        assert_eq!(
            key,
            capture_key("did:key:a", "session-1", "doc-1", "inference.1", 3, 2).unwrap()
        );
    }

    /// Every component has to move the key. A dropped component silently merges
    /// two provider attempts into one durable fact, which is precisely what
    /// `capture_key_determines_request` forbids.
    #[test]
    fn every_capture_key_component_changes_the_key() {
        let base = capture_key("did:key:a", "session-1", "doc-1", "inference.1", 0, 0).unwrap();

        for varied in [
            capture_key("did:key:b", "session-1", "doc-1", "inference.1", 0, 0).unwrap(),
            capture_key("did:key:a", "session-2", "doc-1", "inference.1", 0, 0).unwrap(),
            capture_key("did:key:a", "session-1", "doc-2", "inference.1", 0, 0).unwrap(),
            capture_key("did:key:a", "session-1", "doc-1", "inference.2", 0, 0).unwrap(),
            capture_key("did:key:a", "session-1", "doc-1", "compaction.1", 0, 0).unwrap(),
            capture_key("did:key:a", "session-1", "doc-1", "inference.1", 1, 0).unwrap(),
            capture_key("did:key:a", "session-1", "doc-1", "inference.1", 0, 1).unwrap(),
        ] {
            assert_ne!(base, varied);
        }
    }

    /// A delimited `"{a}:{b}"` encoding would make these one fact. `session_id`
    /// is caller-supplied and unvalidated, so the encoding — not a convention —
    /// has to rule the collision out. The scope rides inside the third
    /// component as a nested array for exactly the same reason: a
    /// `"{request_doc_id}#{scope}"` string would lose the component boundary.
    #[test]
    fn capture_key_does_not_collide_across_component_boundaries() {
        assert_ne!(
            capture_key("did:key:a", "s:1", "doc-1", "inference.1", 0, 0).unwrap(),
            capture_key("did:key:a:s", "1", "doc-1", "inference.1", 0, 0).unwrap(),
        );
        assert_ne!(
            capture_key("did:key:a", "session", "r:1", "inference.1", 0, 0).unwrap(),
            capture_key("did:key:a", "session:r", "1", "inference.1", 0, 0).unwrap(),
        );
        // A replicated document id appearing under two session contexts must
        // stay two facts rather than erasing the session boundary.
        assert_ne!(
            capture_key("did:key:a", "session-1", "shared", "inference.1", 0, 0).unwrap(),
            capture_key("did:key:a", "session-2", "shared", "inference.1", 0, 0).unwrap(),
        );
        // A document id component must not absorb another scope's boundary.
        assert_ne!(
            capture_key("did:key:a", "s", "req#compaction.1", "inference.1", 0, 0).unwrap(),
            capture_key("did:key:a", "s", "req", "compaction.1", 0, 0).unwrap(),
        );
    }

    /// The concrete collision this component exists to prevent: the request's
    /// first turn and the summarizer's first call are both `(turn 0, attempt
    /// 0)` of the same request in the same session.
    #[test]
    fn the_summarizers_first_call_is_not_the_requests_first_turn() {
        assert_ne!(
            capture_key("did:key:a", "s", "req", "inference.1", 0, 0).unwrap(),
            capture_key("did:key:a", "s", "req", "compaction.1", 0, 0).unwrap(),
        );
    }

    #[test]
    fn assembly_trace_records_assistant_message_ids_by_position() {
        let trace = AssemblyTrace::from_effective_messages(
            AssemblyBuildPath::Budgeted,
            vec![
                Message::user("hi"),
                assistant_with_tool_call(Some("msg_abc"), "call-1"),
                tool_result_message("call-1", Some("fc_1"), "ok"),
                // An assistant turn the provider gave no id for.
                Message::assistant("done"),
            ],
        );

        assert_eq!(
            trace.assistant_message_ids,
            vec![AssistantMessageId {
                message_index: 1,
                message_id: "msg_abc".to_string(),
            }]
        );
    }

    #[test]
    fn assembly_trace_records_threaded_tool_results_by_call_identity() {
        let trace = AssemblyTrace::from_effective_messages(
            AssemblyBuildPath::Budgeted,
            vec![
                assistant_with_tool_call(Some("msg_abc"), "call-1"),
                tool_result_message("call-1", Some("fc_1"), "threaded bytes"),
                tool_result_message("call-2", None, "no provider call id"),
            ],
        );

        assert_eq!(
            trace.threaded_tool_results,
            vec![
                ThreadedToolResult {
                    message_index: 1,
                    tool_call_id: "call-1".to_string(),
                    call_id: Some("fc_1".to_string()),
                    content: vec![ToolResultContent::Text(Text {
                        text: "threaded bytes".to_string()
                    })],
                },
                ThreadedToolResult {
                    message_index: 2,
                    tool_call_id: "call-2".to_string(),
                    call_id: None,
                    content: vec![ToolResultContent::Text(Text {
                        text: "no provider call id".to_string()
                    })],
                },
            ]
        );
    }

    /// The overlays index into `effective_messages`; a projection that reads
    /// them has to land back on the message it came from.
    #[test]
    fn assembly_trace_overlay_indexes_address_the_effective_messages() {
        let messages = vec![
            Message::user("hi"),
            assistant_with_tool_call(Some("msg_abc"), "call-1"),
            tool_result_message("call-1", Some("fc_1"), "threaded bytes"),
        ];
        let trace =
            AssemblyTrace::from_effective_messages(AssemblyBuildPath::Budgeted, messages.clone());

        let effective_messages = trace
            .effective_messages
            .as_ref()
            .expect("explicit oracle trace");
        assert_eq!(effective_messages, &messages);
        for overlay in &trace.assistant_message_ids {
            assert!(matches!(
                &effective_messages[overlay.message_index],
                Message::Assistant { id: Some(id), .. } if *id == overlay.message_id
            ));
        }
        for overlay in &trace.threaded_tool_results {
            assert!(matches!(
                &effective_messages[overlay.message_index],
                Message::User { .. }
            ));
        }
    }

    /// Per-turn compaction rewrites `history` and `new_messages` in place, so
    /// the trace has to describe the *post*-compaction list. Nothing else
    /// records it: the summary is model-generated and never becomes an
    /// `AgentCompactionEntry`.
    #[test]
    fn assembly_trace_carries_the_post_compaction_message_list() {
        let compacted = vec![
            Message::system("<system-reminder>continuation checkpoint</system-reminder>"),
            Message::user("continue"),
        ];
        let trace =
            AssemblyTrace::from_effective_messages(AssemblyBuildPath::Budgeted, compacted.clone());

        assert_eq!(trace.effective_messages, Some(compacted));
        assert_eq!(trace.trace_version, ASSEMBLY_TRACE_VERSION);
    }

    #[test]
    fn reconstructible_trace_does_not_duplicate_message_bodies() {
        let marker = "ordinary-message-body-".repeat(1_000);
        let messages = vec![Message::user(marker.clone()), Message::assistant("done")];
        let compact = AssemblyTrace::from_reconstructible_messages(
            AssemblyBuildPath::Budgeted,
            messages.clone(),
        );
        let oracle = AssemblyTrace::from_effective_messages(AssemblyBuildPath::Budgeted, messages);

        let compact_json = serde_json::to_string(&compact).expect("compact trace");
        let oracle_json = serde_json::to_string(&oracle).expect("oracle trace");
        assert_eq!(compact.effective_message_count, 2);
        assert!(compact.effective_messages.is_none());
        assert!(!compact_json.contains(&marker));
        assert!(oracle_json.len() > compact_json.len() + marker.len());
    }

    #[test]
    fn build_path_records_whether_the_output_clamp_ran() {
        assert!(AssemblyBuildPath::Budgeted.applies_output_clamp());
        assert!(!AssemblyBuildPath::Repair.applies_output_clamp());

        let budgeted = build(0, 0, empty_trace(), components());
        let repaired = build(
            0,
            1,
            AssemblyTrace::from_effective_messages(AssemblyBuildPath::Repair, Vec::new()),
            components(),
        );

        assert_eq!(
            budgeted.assembly_trace.build_path,
            AssemblyBuildPath::Budgeted
        );
        assert_eq!(
            repaired.assembly_trace.build_path,
            AssemblyBuildPath::Repair
        );
        assert_ne!(budgeted.provenance_json, repaired.provenance_json);
    }

    #[test]
    fn provenance_json_round_trips_to_a_captured_only_manifest() {
        let trace = AssemblyTrace::from_effective_messages(
            AssemblyBuildPath::Repair,
            vec![
                assistant_with_tool_call(Some("msg_abc"), "call-1"),
                tool_result_message("call-1", Some("fc_1"), "threaded bytes"),
            ],
        );
        let rendered = build(2, 1, trace.clone(), components());

        let manifest: ProvenanceManifest =
            serde_json::from_value(rendered.provenance_json.clone()).expect("manifest round-trip");

        assert_eq!(manifest.manifest_version, PROVENANCE_MANIFEST_VERSION);
        assert_eq!(manifest.status, ProvenanceStatus::CapturedOnly);
        assert!(!manifest.status_reason.is_empty());
        assert_eq!(manifest.capture_seam, CaptureSeam::TransportBody);
        assert_eq!(manifest.capture_scope, "inference.1");
        assert_eq!(manifest.assembly_trace, trace);
        assert_eq!(rendered.assembly_trace, trace);
    }

    /// `provenance_json` is what lands in the column, so its key order must not
    /// depend on which workspace members turned on `serde_json/preserve_order`.
    #[test]
    fn provenance_json_is_canonically_ordered() {
        let rendered = build(0, 0, empty_trace(), components());

        assert_eq!(
            canonical_json(&rendered.provenance_json),
            rendered.provenance_json
        );
    }

    /// Version 1 declares `captured_only` positively. An absent field is never
    /// the evidence — a reader must be able to see the claim, not infer it.
    #[test]
    fn version_one_provenance_never_claims_verification() {
        let rendered = build(0, 0, empty_trace(), components());

        assert_eq!(rendered.provenance_json["status"], "captured_only");
        assert_eq!(
            rendered.provenance_json["manifest_version"],
            PROVENANCE_MANIFEST_VERSION
        );
        assert!(rendered.provenance_json.get("assembly_trace").is_some());
    }

    /// The seam is recorded positively so a reader never has to guess whether
    /// the bytes predate the ChatGPT-Codex and Grok body rewrites.
    #[test]
    fn provenance_names_the_seam_the_bytes_came_from() {
        let rendered = build(0, 0, empty_trace(), components());

        assert_eq!(rendered.provenance_json["capture_seam"], "transport_body");
        assert_eq!(rendered.provenance_json["capture_scope"], "inference.1");
    }

    fn agent_request() -> crate::watcher::AgentRequest {
        crate::watcher::AgentRequest {
            doc_id: "doc-1".to_string(),
            request_id: "request-1".to_string(),
            agent_did: "did:key:test".to_string(),
            requester_did: None,
            behavior_id: Some("behavior".to_string()),
            session_id: "session".to_string(),
            content: "hi".to_string(),
            temperature: None,
            top_p: None,
            top_k: None,
            max_tokens: None,
            seed: None,
            max_total_tokens: None,
            metadata: None,
            execution_origin: None,
            created_at: String::new(),
            deadline: None,
            subagent_depth: 0,
            caused_by_parent_request_id: None,
            caused_by_parent_tool_call_id: None,
        }
    }

    #[test]
    fn context_for_request_carries_an_absent_requester_as_empty() {
        let mut request = agent_request();
        request.requester_did = None;
        let context = RenderedRequestContext::for_request(&request, "test-model".to_string());
        assert_eq!(context.requester_did, "");

        request.requester_did = Some("did:key:requester".to_string());
        let context = RenderedRequestContext::for_request(&request, "test-model".to_string());
        assert_eq!(context.requester_did, "did:key:requester");
    }
}
