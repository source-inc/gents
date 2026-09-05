//! Shared vocabulary for `RenderedRequest` capture rows (#840, #1066).
//!
//! The producer lives in `gents::rendered_request` — it builds the capture DTO
//! at the transport seam and persists the 18-column row. Every *consumer* (the
//! run timeline, adapter projections, the CLI, a future reconstructor) reads
//! through the types here, so the sharp parts of the format — the
//! `"{kind}.{seq}"` capture scope, the `(scope, turn, attempt)` ordering, and
//! the versioned provenance manifest — are implemented exactly once, next to
//! the vocabulary that defines them. `gents` re-exports everything in this
//! module at its original paths, so the producer and the readers cannot drift.
//!
//! Serialized shapes here are load-bearing: `provenance_json` columns already
//! persisted by #1059 deserialize through these exact serde attributes.

use serde::{Deserialize, Serialize};

use crate::message::{Message, ToolResultContent, UserContent};

/// Capture format version stamped onto every row. Bump when the *set of
/// columns* a reader must understand changes.
pub const CAPTURE_VERSION: u32 = 1;

/// Provenance manifest version. Bump when `ProvenanceManifest`'s serialized
/// shape changes. A reader that does not know this number must report
/// `UnsupportedManifest` rather than guessing.
///
/// v2 (#1059): status, seam, scope, endpoint, assembly trace.
/// v3 (#1066): optional `admission` join to the persisted `InferenceCall`.
pub const PROVENANCE_MANIFEST_VERSION: u32 = 3;

/// Assembly-trace version. Bump when `AssemblyTrace`'s serialized shape
/// changes. Versioned independently of the manifest so a manifest that later
/// gains pinned config CIDs does not have to re-version the trace.
pub const ASSEMBLY_TRACE_VERSION: u32 = 4;

/// Version of the request-bound context accounting payload. The estimator is
/// intentionally named as well as versioned: these are the exact values the
/// runtime used for its dispatch decision, not a claim about a provider's
/// proprietary tokenizer.
///
/// v2: provider-wire projection selected by backend/API, with normalized
/// documents attributed separately and one authoritative whole-body total.
pub const CONTEXT_ACCOUNTING_VERSION: u32 = 2;

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
    ("/messages", RenderedRequestSource::ClaudeCliSubscription),
];

/// The provider wire shape a captured body was actually sent on.
///
/// Derived from the request path the transport posted to, never from behavior
/// configuration: configuration says what the runtime *intended*, and this
/// column has to say what the provider *received*. The two can disagree — a
/// backend document can be edited between reconcile and send.
///
/// `ClaudeCliSubscription` is stamped by the capturing HTTP transport for
/// Anthropic Messages posts to `/v1/messages` — the only Claude wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RenderedRequestSource {
    #[serde(rename = "openai_responses")]
    OpenAiResponses,
    #[serde(rename = "openai_chat_completions")]
    OpenAiChatCompletions,
    #[serde(rename = "claude_cli_subscription")]
    ClaudeCliSubscription,
}

impl RenderedRequestSource {
    /// Classify by the completion request path. `None` means "not a completion
    /// request", which is the transport's signal to forward without capturing.
    pub fn for_request_path(path: &str) -> Option<Self> {
        COMPLETION_REQUEST_PATHS
            .iter()
            .find(|(suffix, _)| path.ends_with(suffix))
            .map(|(_, source)| *source)
    }

    /// The body field carrying the provider message list on this wire shape.
    pub fn messages_field(self) -> &'static str {
        match self {
            Self::OpenAiResponses => "input",
            Self::OpenAiChatCompletions | Self::ClaudeCliSubscription => "messages",
        }
    }
}

/// Which completion loop inside a request is issuing provider calls.
///
/// Declaration order is the ordering `CaptureOrderKey` sorts kinds by; it is a
/// stable identity order, not a temporal one (temporal interleaving across
/// loops comes from `created_at` and the admission join).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum CaptureScopeKind {
    /// The request's own owned completion loop (`agent/loop_stream.rs`).
    Inference,
    /// The guided per-turn compaction summarizer. Its output becomes a durable
    /// request-local `ProviderContextReduction` before it may replace provider
    /// history; it is not a session-prefix `CompactionEntry`.
    Compaction,
    /// The strict-JSON compaction fallback, taken when guided structured output
    /// exhausts its recovery.
    CompactionFallback,
    /// Conversation title generation.
    Title,
    /// The one-shot runner (`oneshot::run_openai_oneshot_with_tools`).
    OneShot,
}

impl CaptureScopeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inference => "inference",
            Self::Compaction => "compaction",
            Self::CompactionFallback => "compaction_fallback",
            Self::Title => "title",
            Self::OneShot => "oneshot",
        }
    }

    /// The inverse of `as_str`. `None` for anything the producer never writes.
    pub fn from_label(label: &str) -> Option<Self> {
        match label {
            "inference" => Some(Self::Inference),
            "compaction" => Some(Self::Compaction),
            "compaction_fallback" => Some(Self::CompactionFallback),
            "title" => Some(Self::Title),
            "oneshot" => Some(Self::OneShot),
            _ => None,
        }
    }
}

impl std::fmt::Display for CaptureScopeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A parsed `capture_scope` column: which loop, and which allocation of that
/// loop within the request. The producer writes `"{kind}.{seq}"` with `seq`
/// starting at 1 per kind (`gents::rendered_request::scope`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CaptureScope {
    pub kind: CaptureScopeKind,
    pub seq: u64,
}

/// Why a `capture_scope` string failed to parse. Malformed scope is an error a
/// consumer must surface, never a default: defaulting would silently merge the
/// row into some other loop's ordering.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CaptureScopeParseError {
    Empty,
    /// No `.` separator, or an empty kind/seq component.
    MissingSeparator(String),
    UnknownKind(String),
    InvalidSeq(String),
}

impl std::fmt::Display for CaptureScopeParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "capture scope is empty"),
            Self::MissingSeparator(scope) => {
                write!(f, "capture scope {scope:?} is not \"{{kind}}.{{seq}}\"")
            }
            Self::UnknownKind(kind) => write!(f, "unknown capture scope kind {kind:?}"),
            Self::InvalidSeq(seq) => write!(f, "capture scope seq {seq:?} is not a u64"),
        }
    }
}

impl std::error::Error for CaptureScopeParseError {}

impl std::str::FromStr for CaptureScope {
    type Err = CaptureScopeParseError;

    fn from_str(label: &str) -> Result<Self, Self::Err> {
        if label.is_empty() {
            return Err(CaptureScopeParseError::Empty);
        }
        // Split on the LAST '.': kind labels contain '_' but never '.', and a
        // future kind must not silently re-shape existing labels.
        let (kind, seq) = label
            .rsplit_once('.')
            .filter(|(kind, seq)| !kind.is_empty() && !seq.is_empty())
            .ok_or_else(|| CaptureScopeParseError::MissingSeparator(label.to_string()))?;
        let kind = CaptureScopeKind::from_label(kind)
            .ok_or_else(|| CaptureScopeParseError::UnknownKind(kind.to_string()))?;
        if !seq.bytes().all(|b| b.is_ascii_digit()) {
            return Err(CaptureScopeParseError::InvalidSeq(seq.to_string()));
        }
        let seq = seq
            .parse::<u64>()
            .map_err(|_| CaptureScopeParseError::InvalidSeq(seq.to_string()))?;
        Ok(Self { kind, seq })
    }
}

impl std::fmt::Display for CaptureScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.kind, self.seq)
    }
}

/// The stable identity ordering of capture rows within one request:
/// `(kind, seq, turn_index, attempt)`, with `seq` compared numerically — a
/// lexical sort of the raw `capture_scope` column would put `inference.10`
/// before `inference.2`.
///
/// This orders rows *within* and *across* loops deterministically; it is not a
/// temporal order across loops (the summarizer's calls interleave with the
/// inference loop's in wall-clock time). Temporal placement comes from
/// `created_at` and the provenance admission join.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CaptureOrderKey {
    pub scope: CaptureScope,
    pub turn_index: i64,
    pub attempt: i64,
}

impl CaptureOrderKey {
    /// A zero-padded rendering whose lexical order equals `Ord`. For sort-key
    /// slots that only carry strings (timeline tiebreaks).
    ///
    /// The leading numeric rank is load-bearing: the kind *labels* sort
    /// lexically as `compaction < inference`, which contradicts declaration
    /// order — the conformance case `kind_rank_dominates_seq` is the fence
    /// that caught exactly that.
    pub fn padded(&self) -> String {
        format!(
            "{:02}.{}.{:010}.{:06}.{:06}",
            self.scope.kind as u8, self.scope.kind, self.scope.seq, self.turn_index, self.attempt
        )
    }
}

/// Why a row could not produce a [`CaptureOrderKey`]. `turn_index` and
/// `attempt` are core facts; a row missing them cannot be ordered, and
/// defaulting them to zero would silently collide with a real first-turn row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CaptureOrderKeyError {
    MissingScope,
    Scope(CaptureScopeParseError),
    MissingTurnIndex,
    MissingAttempt,
}

impl std::fmt::Display for CaptureOrderKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingScope => write!(f, "row has no capture_scope"),
            Self::Scope(err) => write!(f, "{err}"),
            Self::MissingTurnIndex => write!(f, "row has no turn_index"),
            Self::MissingAttempt => write!(f, "row has no attempt"),
        }
    }
}

impl std::error::Error for CaptureOrderKeyError {}

impl From<CaptureScopeParseError> for CaptureOrderKeyError {
    fn from(err: CaptureScopeParseError) -> Self {
        Self::Scope(err)
    }
}

/// Outcome of reading a `provenance_json` column.
#[derive(Clone, Debug, PartialEq)]
pub enum ParsedProvenance {
    Manifest(Box<ProvenanceManifest>),
    /// The row was written by a runtime this reader does not understand. The
    /// row is still real and still listed; only its provenance is opaque.
    Unsupported {
        manifest_version: u32,
    },
}

/// Why a `provenance_json` column failed to read. Distinct from
/// [`ParsedProvenance::Unsupported`]: these are malformed columns, not
/// unknown-but-well-formed versions.
#[derive(Debug)]
pub enum ProvenanceParseError {
    Empty,
    InvalidJson(serde_json::Error),
    /// JSON object with no numeric `manifest_version` — pre-versioned or
    /// corrupt; either way unreadable by contract.
    MissingVersion,
    /// Version in range but the body did not match that version's shape.
    InvalidManifest(serde_json::Error),
}

impl std::fmt::Display for ProvenanceParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "provenance_json is empty"),
            Self::InvalidJson(err) => write!(f, "provenance_json is not JSON: {err}"),
            Self::MissingVersion => {
                write!(f, "provenance_json carries no numeric manifest_version")
            }
            Self::InvalidManifest(err) => {
                write!(
                    f,
                    "provenance_json does not match its declared version: {err}"
                )
            }
        }
    }
}

impl std::error::Error for ProvenanceParseError {}

impl ProvenanceManifest {
    /// Read a `provenance_json` column, gating on `manifest_version` before
    /// committing to a shape.
    pub fn parse(provenance_json: &str) -> Result<ParsedProvenance, ProvenanceParseError> {
        if provenance_json.trim().is_empty() {
            return Err(ProvenanceParseError::Empty);
        }
        let value: serde_json::Value =
            serde_json::from_str(provenance_json).map_err(ProvenanceParseError::InvalidJson)?;
        let manifest_version = value
            .get("manifest_version")
            .and_then(serde_json::Value::as_u64)
            .and_then(|version| u32::try_from(version).ok())
            .ok_or(ProvenanceParseError::MissingVersion)?;
        if manifest_version != PROVENANCE_MANIFEST_VERSION {
            return Ok(ParsedProvenance::Unsupported { manifest_version });
        }
        serde_json::from_value(value)
            .map(|m| ParsedProvenance::Manifest(Box::new(m)))
            .map_err(ProvenanceParseError::InvalidManifest)
    }
}

/// Which of the owned loop's two request builders produced the captured
/// `CompletionRequest`.
///
/// This is one of the four unrecoverable inputs. The completion-retry `Repair`
/// directive rebuilds directly after payload repair, while `Budgeted` performs
/// ordinary assembly and optional per-turn compaction. Every resulting request
/// is provider-projected, recounted, and output-clamped at the final dispatch
/// boundary; this discriminator records the assembly route, not budget policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssemblyBuildPath {
    /// `build_budgeted_request`: ordinary assembly and per-turn compaction when
    /// the request exceeded the input budget.
    Budgeted,
    /// `build_request` invoked directly by a completion-retry repair.
    Repair,
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

/// Why the successful provider dispatch did or did not use per-turn
/// compaction. Session-prefix compaction remains represented by
/// `CompactionEntry`; this describes the complete request assembled at the
/// owned-loop dispatch boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextCompactionReason {
    /// The complete assembled input fit below the configured threshold.
    BelowThreshold,
    /// The input exceeded the threshold and a durable
    /// `ProviderContextReduction` was activated before this dispatch.
    Compacted,
    /// A request-local, non-durable repair (currently completed tool-result
    /// stripping) made the provider view fit without creating a reduction
    /// checkpoint.
    ProviderViewRepaired,
    /// This completion loop had no per-turn compactor. Used by internal and
    /// one-shot loops where compaction is deliberately unavailable.
    CompactorUnavailable,
}

/// Token-estimate components for the exact provider-wire request that was
/// captured. Their saturating sum is `estimated_input_tokens`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextInputComponents {
    pub messages: usize,
    pub documents: usize,
    pub tool_schemas: usize,
    pub additional_parameters: usize,
    pub output_schema: usize,
}

impl ContextInputComponents {
    pub fn estimated_input_tokens(&self) -> usize {
        self.messages
            .saturating_add(self.documents)
            .saturating_add(self.tool_schemas)
            .saturating_add(self.additional_parameters)
            .saturating_add(self.output_schema)
    }
}

/// Request-bound context accounting persisted with the exact rendered request
/// and projected onto its joined, replicated `InferenceCall` row. Every
/// provider dispatch attempt owns a distinct call row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextAccounting {
    pub accounting_version: u32,
    /// Provider-dispatch coordinate within the owning completion loop.
    #[serde(default)]
    pub turn_index: usize,
    #[serde(default)]
    pub attempt: u32,
    pub estimator: String,
    pub components: ContextInputComponents,
    pub estimated_input_tokens: usize,
    pub context_window: usize,
    pub compaction_threshold_basis_points: u64,
    pub compaction_threshold_tokens: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configured_max_output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_max_output_tokens: Option<u64>,
    pub compaction_reason: ContextCompactionReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pre_compaction_input_tokens: Option<usize>,
}

/// The request-local inputs that reconstruction must account for explicitly.
///
/// Everything else that shapes a request is either durable (transcript rows,
/// behavior/profile/backend/skill documents) or a pure function of durable data.
/// These four need an overlay, an oracle, or an explicit durable lineage:
///
/// 1. `assistant_message_ids` — provider-assigned, persisted as `None`.
/// 2. `threaded_tool_results` — the loop and the persistence path derive
///    different text from different sources.
/// 3. `effective_messages` — when a rendered request-context message,
///    repair, or per-turn compaction adds request-local content. Compaction is a *sticky*
///    mutation
///    (`*history = compacted; *new_messages = vec![compacted_prompt]`,
///    `agent/loop_stream.rs:1350-1351`), so one turn's model-generated summary
///    governs every later turn of the same request. Its exact source split and
///    checkpoint are written as a `ProviderContextReduction`; re-running the
///    summarizer is not reconstruction because it need not produce the same words.
/// 4. `build_path` — see `AssemblyBuildPath`.
///
/// `assistant_message_ids` and `threaded_tool_results` are projections of the
/// effective message list, derived by the same constructor so they cannot drift
/// from it.
/// They are carried explicitly because a reconstructor rebuilds its message
/// list from `AgentMessage` rows and needs these as an *overlay* keyed by
/// position and call id; `effective_messages` is the oracle it checks itself
/// against. `effective_messages` is present only when that list contains a
/// rendered request-context message, a durably recorded per-turn compaction
/// summary, or the result of a repair rewrite. Otherwise the durable transcript
/// plus these overlays reconstructs it exactly.
///
/// ## Size
///
/// Ordinary turns do not retain the full native message list next to the
/// provider-wire copy in `request_json`; that list is reconstructible from
/// durable rows plus the overlays. Once request-context rendering, per-turn
/// compaction, or a repair changes the native list, the full list is retained
/// as the reconstruction oracle. Per-turn reductions additionally cite their
/// durable fact keys, so the oracle can be checked against independently
/// addressable checkpoints. Compacted lists are bounded by the context window.
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
    /// request-local content. Native `Message`s, not provider wire shapes: this is
    /// the *input* to assembly, whereas `request_json` is its output.
    pub effective_messages: Option<Vec<Message>>,
    pub assistant_message_ids: Vec<AssistantMessageId>,
    pub threaded_tool_results: Vec<ThreadedToolResult>,
    /// Ordered immutable per-turn reduction facts that causally determine the
    /// sticky provider projection used for this request. Empty on ordinary
    /// assembly.
    pub reduction_keys: Vec<String>,
    /// Exact accounting used at the dispatch gate.
    pub context_accounting: Option<ContextAccounting>,
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
    pub fn from_reconstructible_messages(
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
            reduction_keys: Vec::new(),
            context_accounting: None,
        }
    }

    pub fn with_reduction_keys(mut self, reduction_keys: Vec<String>) -> Self {
        self.reduction_keys = reduction_keys;
        self
    }

    pub fn with_context_accounting(mut self, context_accounting: ContextAccounting) -> Self {
        self.context_accounting = Some(context_accounting);
        self
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

/// The admission identity of the provider call this capture preceded: the
/// `call_id`/`call_seq` the admission registry minted for exactly that call.
///
/// This makes the capture↔`InferenceCall` correspondence a stored key instead
/// of an ordinal guess — an admission rejection consumes no `call_seq`, so
/// counting rows on either side desynchronises; this join does not. Absent for
/// one-shot runs (no admission scope exists) and on v2 rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionJoin {
    pub call_id: String,
    pub call_seq: i64,
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
    /// The admitted call this capture preceded. Present exactly when an
    /// admission scope was live at the seam; a one-shot capture legitimately
    /// carries none, and its absence there is a documented fact, not an error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission: Option<AdmissionJoin>,
    pub assembly_trace: AssemblyTrace,
}

impl ProvenanceManifest {
    const CAPTURED_ONLY_REASON: &'static str =
        "this provenance manifest pins no config or transcript versions, so a \
         reconstruction cannot be verified against this capture";

    pub fn captured_only(
        capture_scope: String,
        provider_endpoint: Option<String>,
        admission: Option<AdmissionJoin>,
        assembly_trace: AssemblyTrace,
    ) -> Self {
        Self {
            manifest_version: PROVENANCE_MANIFEST_VERSION,
            status: ProvenanceStatus::CapturedOnly,
            status_reason: Self::CAPTURED_ONLY_REASON.to_string(),
            capture_seam: CaptureSeam::TransportBody,
            capture_scope,
            provider_endpoint,
            admission,
            assembly_trace,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::message::{AssistantContent, Text, ToolCall, ToolFunction};

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

    #[test]
    fn claude_cli_subscription_classifies_messages_http_urls_only() {
        assert_eq!(
            RenderedRequestSource::ClaudeCliSubscription.messages_field(),
            "messages"
        );
        assert_eq!(
            serde_json::to_value(RenderedRequestSource::ClaudeCliSubscription).unwrap(),
            json!("claude_cli_subscription")
        );
        assert_eq!(
            RenderedRequestSource::for_request_path("/v1/messages"),
            Some(RenderedRequestSource::ClaudeCliSubscription)
        );
        assert_eq!(RenderedRequestSource::for_request_path("/claude"), None);
        assert_eq!(
            RenderedRequestSource::for_request_path("claude-cli://subscription"),
            None
        );
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
    fn capture_scope_round_trips_and_rejects_garbage() {
        for label in [
            "inference.1",
            "compaction.2",
            "compaction_fallback.1",
            "title.1",
            "oneshot.1",
        ] {
            let scope: CaptureScope = label.parse().expect(label);
            assert_eq!(scope.to_string(), label);
        }
        for bad in [
            "",
            "inference",
            "inference.",
            ".1",
            "inference.0x2",
            "inference.+2",
            "inference. 2",
            "mystery.1",
            "inference.1.2",
        ] {
            assert!(
                bad.parse::<CaptureScope>().is_err(),
                "{bad:?} must not parse"
            );
        }
    }

    #[test]
    fn order_key_sorts_seq_numerically_not_lexically() {
        let key = |label: &str, turn_index, attempt| CaptureOrderKey {
            scope: label.parse().unwrap(),
            turn_index,
            attempt,
        };
        let mut keys = [
            key("inference.10", 0, 0),
            key("inference.2", 3, 1),
            key("inference.2", 3, 0),
        ];
        keys.sort();
        assert_eq!(keys[0], key("inference.2", 3, 0));
        assert_eq!(keys[1], key("inference.2", 3, 1));
        assert_eq!(keys[2], key("inference.10", 0, 0));

        // padded() must agree with Ord under lexical sort.
        let padded: Vec<String> = keys.iter().map(CaptureOrderKey::padded).collect();
        let mut lexical = padded.clone();
        lexical.sort();
        assert_eq!(padded, lexical);
    }

    /// Cross-kind ordering is by declaration order — a stable identity order,
    /// pinned so a reordered enum shows up as a test failure and a conscious
    /// decision, not a silent re-sort of every consumer.
    #[test]
    fn order_key_orders_kinds_by_declaration() {
        let scopes: Vec<CaptureScope> = [
            "oneshot.1",
            "title.1",
            "compaction_fallback.1",
            "compaction.1",
            "inference.1",
        ]
        .iter()
        .map(|label| label.parse().unwrap())
        .collect();
        let mut sorted = scopes.clone();
        sorted.sort();
        assert_eq!(
            sorted.iter().map(|s| s.kind.as_str()).collect::<Vec<_>>(),
            vec![
                "inference",
                "compaction",
                "compaction_fallback",
                "title",
                "oneshot"
            ]
        );
    }

    #[test]
    fn manifest_reader_accepts_only_the_current_version() {
        let current = json!({
            "manifest_version": PROVENANCE_MANIFEST_VERSION,
            "status": "captured_only",
            "status_reason": "r",
            "capture_seam": "transport_body",
            "capture_scope": "inference.1",
            "assembly_trace": {
                "trace_version": 4,
                "build_path": "budgeted",
                "effective_message_count": 0,
                "effective_messages": null,
                "assistant_message_ids": [],
                "threaded_tool_results": [],
                "reduction_keys": [],
                "context_accounting": null
            }
        });
        let parsed = ProvenanceManifest::parse(&current.to_string()).expect("current parses");
        match parsed {
            ParsedProvenance::Manifest(manifest) => {
                assert_eq!(manifest.manifest_version, PROVENANCE_MANIFEST_VERSION);
                assert_eq!(manifest.capture_scope, "inference.1");
                assert_eq!(manifest.status, ProvenanceStatus::CapturedOnly);
            }
            other => panic!("expected manifest, got {other:?}"),
        }

        for version in [2, 99] {
            let unsupported = json!({ "manifest_version": version, "anything": true });
            assert_eq!(
                ProvenanceManifest::parse(&unsupported.to_string())
                    .expect("well-formed unknown version"),
                ParsedProvenance::Unsupported {
                    manifest_version: version
                }
            );
        }

        assert!(matches!(
            ProvenanceManifest::parse(""),
            Err(ProvenanceParseError::Empty)
        ));
        assert!(matches!(
            ProvenanceManifest::parse("not json"),
            Err(ProvenanceParseError::InvalidJson(_))
        ));
        assert!(matches!(
            ProvenanceManifest::parse(r#"{"status":"captured_only"}"#),
            Err(ProvenanceParseError::MissingVersion)
        ));
        // Current version whose body does not match the declared shape.
        assert!(matches!(
            ProvenanceManifest::parse(&format!(
                r#"{{"manifest_version":{PROVENANCE_MANIFEST_VERSION}}}"#
            )),
            Err(ProvenanceParseError::InvalidManifest(_))
        ));
    }

    /// What the producer writes, the reader reads — through the version gate,
    /// not around it.
    #[test]
    fn manifest_reader_round_trips_the_producer_shape() {
        let manifest = ProvenanceManifest::captured_only(
            "compaction.2".to_string(),
            Some("https://api.example.test".to_string()),
            Some(AdmissionJoin {
                call_id: "call-7".to_string(),
                call_seq: 7,
            }),
            AssemblyTrace::from_effective_messages(
                AssemblyBuildPath::Repair,
                vec![Message::user("hi")],
            ),
        );
        let serialized = serde_json::to_string(&manifest).expect("serialize manifest");
        assert_eq!(
            ProvenanceManifest::parse(&serialized).expect("reader accepts producer output"),
            ParsedProvenance::Manifest(Box::new(manifest))
        );
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
    fn context_accounting_round_trips_decision_and_components() {
        let accounting = ContextAccounting {
            accounting_version: CONTEXT_ACCOUNTING_VERSION,
            turn_index: 2,
            attempt: 1,
            estimator: "serialized_json_bytes_div_4_v1".to_string(),
            components: ContextInputComponents {
                messages: 120,
                documents: 3,
                tool_schemas: 40,
                additional_parameters: 2,
                output_schema: 5,
            },
            estimated_input_tokens: 170,
            context_window: 480_000,
            compaction_threshold_basis_points: 5_460,
            compaction_threshold_tokens: 262_080,
            configured_max_output_tokens: Some(64_000),
            effective_max_output_tokens: Some(64_000),
            compaction_reason: ContextCompactionReason::Compacted,
            pre_compaction_input_tokens: Some(300_000),
        };
        let trace = AssemblyTrace::from_reconstructible_messages(
            AssemblyBuildPath::Budgeted,
            vec![Message::user("hello")],
        )
        .with_context_accounting(accounting.clone());
        let encoded = serde_json::to_string(&trace).expect("serialize accounting trace");
        let decoded: AssemblyTrace =
            serde_json::from_str(&encoded).expect("deserialize accounting trace");

        assert_eq!(decoded.trace_version, ASSEMBLY_TRACE_VERSION);
        assert_eq!(decoded.context_accounting, Some(accounting));
        assert!(encoded.contains(r#""compaction_reason":"compacted""#));
        assert_eq!(
            decoded
                .context_accounting
                .as_ref()
                .expect("accounting")
                .components
                .estimated_input_tokens(),
            170
        );
    }
}
