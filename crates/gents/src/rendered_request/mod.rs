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

use std::collections::{BTreeMap, BTreeSet};
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

#[cfg(test)]
pub(crate) fn test_static_rendered_request_version() -> crate::SignedDocumentVersionRef {
    crate::SignedDocumentVersionRef::new(
        crate::DocumentVersionRef::new("rendered-doc-test", "bafy-rendered-test"),
        "did:key:test",
    )
}

/// Capture format version stamped onto every row. Bump when the *set of
/// columns* a reader must understand changes.
pub const CAPTURE_VERSION: u32 = 4;

/// Provenance manifest version. Bump when `ProvenanceManifest`'s serialized
/// shape changes. A reader that does not know this number must report
/// `UnsupportedManifest` rather than guessing.
pub const PROVENANCE_MANIFEST_VERSION: u32 = 7;

/// Assembly-trace version. Bump when `AssemblyTrace`'s serialized shape
/// changes. Versioned independently of the manifest: adding exact config
/// provenance to later manifest versions did not change the assembly trace.
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
    dyn Fn(
            RenderedCompletionRequest,
        ) -> Pin<Box<dyn Future<Output = Result<crate::SignedDocumentVersionRef>> + Send>>
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
    /// The rendered request is durable and exact, but not all durable source
    /// versions are pinned alongside it, so a reconstruction cannot yet be
    /// fully verified. This is the only status version 7 emits.
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
    /// version 7 emits.
    TransportBody,
}

/// Declares whether configuration came from the reconciled DefraDB document
/// runtime or from a static/one-shot construction path.
///
/// This is explicit rather than inferred from `config_provenance`: a dropped
/// document-runtime map entry must fail closed instead of masquerading as an
/// intentional static capture.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigProvenanceScope {
    ReconciledDocumentRuntime,
    #[default]
    StaticOrOneShot,
}

/// Whether a capture is required to be bound to a durable running
/// `InferenceCall` admission fact.
///
/// Production document and one-shot runtime contexts always use
/// [`Self::AdmittedProviderCall`]. The static variant is deliberately
/// explicit so transport tests and one-shot builders cannot accidentally
/// claim that the body was admitted by a durable call record.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InferenceCallProvenanceScope {
    AdmittedProviderCall,
    #[default]
    StaticOrTest,
}

/// Versioned provenance travelling in the `provenance_json` column.
///
/// Version 7 carries the assembly trace, the seam, the observed provider
/// endpoint, the complete signed source/claim chain admitted at request ingest,
/// the exact signed running `InferenceCall` that authorized the provider send,
/// the ordered exact transcript snapshot loaded for request assembly, and,
/// when the request uses a reconciled document runtime, the exact signed core
/// configuration versions that produced that resolved behavior generation.
/// The complete skill-candidate set, MCP availability, the host tool ceiling,
/// and compaction source versions remain unpinned, so version 7 still reports
/// `captured_only` rather than claiming full reconstruction.
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
    /// Legacy v3 exact claim snapshot. Version 4 never emits this field; it is
    /// retained only so old captures remain decodable without being promoted
    /// to verified ingest evidence.
    #[serde(default, skip_serializing)]
    pub request_version: Option<crate::DocumentVersionRef>,
    /// Cryptographically admitted source and target-agent claim versions for a
    /// document-backed request. `None` is valid for one-shot runs and legacy
    /// manifests only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_provenance: Option<crate::RequestExecutionProvenance>,
    /// Whether this capture was required to come from the admitted document
    /// runtime or was intentionally produced by a static/test path.
    #[serde(default)]
    pub inference_call_provenance_scope: InferenceCallProvenanceScope,
    /// Exact signed running `InferenceCall` version consumed at the transport
    /// seam. Required for `admitted_provider_call`, absent for
    /// `static_or_test`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inference_call_provenance: Option<crate::SignedDocumentVersionRef>,
    /// Exact durable finalized transcript snapshot loaded before this request's
    /// capture scope was constructed, in strictly increasing message sequence.
    /// Each entry pins both DefraDB physical identity and the signed composite
    /// version consumed by provider-input assembly.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transcript_snapshot: Vec<crate::MessageFactRef>,
    /// Configuration-resolution path. Pre-v6 manifests omit this field and
    /// decode as `static_or_one_shot`.
    #[serde(default)]
    pub config_provenance_scope: ConfigProvenanceScope,
    /// Exact signed core configuration documents retained by the reconciled
    /// behavior generation. Absent for one-shot and legacy/static-builder
    /// contexts that have no document-runtime provenance bundle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_provenance: Option<crate::ResolvedBehaviorConfigProvenance>,
    pub assembly_trace: AssemblyTrace,
}

impl ProvenanceManifest {
    const RECONCILED_CORE_CONFIG_CAPTURED_ONLY_REASON: &'static str =
        "provenance manifest v7 records a reconciled document-runtime capture and pins the signed \
         source/claim chain, exact running inference-call admission, exact loaded transcript snapshot, and exact resolved core config \
         documents, but not the complete skill-candidate set, MCP availability, the host tool \
         ceiling, or compaction source versions, so reconstruction cannot yet be fully verified";
    const STATIC_CORE_CONFIG_CAPTURED_ONLY_REASON: &'static str =
        "provenance manifest v7 records an explicit static/test capture with no durable \
         inference-call admission and pins any supplied source/claim chain, exact loaded transcript snapshot, and supplied exact core config \
         documents, but not the complete skill-candidate set, MCP availability, the host tool \
         ceiling, or compaction source versions, so reconstruction cannot yet be fully verified";
    const STATIC_UNPINNED_CONFIG_CAPTURED_ONLY_REASON: &'static str =
        "provenance manifest v7 records an explicit static/test capture with no durable \
         inference-call admission and no resolved core config provenance; it does not pin MCP \
         availability, the complete skill-candidate set, the host tool ceiling, or compaction \
         source versions, so reconstruction cannot yet be fully verified";
    const UNPINNED_CONFIG_CAPTURED_ONLY_REASON: &'static str =
        "provenance manifest v7 pins the signed source/claim chain and exact running inference-call admission and exact loaded transcript \
         snapshot but has no resolved core config provenance and does not pin MCP availability, \
         the complete skill-candidate set, the host tool ceiling, or compaction source versions, \
         so reconstruction cannot yet be fully verified";
    const ONESHOT_CAPTURED_ONLY_REASON: &'static str =
        "provenance manifest v7 describes a one-shot run with no request document provenance \
         but pins its exact running inference-call admission; it does not pin the complete skill-candidate set, MCP availability, the host tool \
         ceiling, or compaction source versions; resolved core config provenance may also be \
         absent, so reconstruction cannot yet be fully verified";
    const ONESHOT_WITH_CONFIG_CAPTURED_ONLY_REASON: &'static str =
        "provenance manifest v7 records a static or one-shot config path with no request document \
         provenance, pins its exact running inference-call admission and supplied exact core config documents, but not the complete \
         skill-candidate set, MCP availability, the host tool ceiling, or compaction source \
         versions, so reconstruction cannot yet be fully verified";

    fn captured_only_reason(
        request_provenance: Option<&crate::RequestExecutionProvenance>,
        inference_call_provenance_scope: InferenceCallProvenanceScope,
        config_provenance_scope: ConfigProvenanceScope,
        config_provenance: Option<&crate::ResolvedBehaviorConfigProvenance>,
    ) -> &'static str {
        if inference_call_provenance_scope == InferenceCallProvenanceScope::StaticOrTest {
            return if config_provenance.is_some() {
                Self::STATIC_CORE_CONFIG_CAPTURED_ONLY_REASON
            } else {
                Self::STATIC_UNPINNED_CONFIG_CAPTURED_ONLY_REASON
            };
        }
        match (
            request_provenance,
            config_provenance_scope,
            config_provenance,
        ) {
            (Some(_), ConfigProvenanceScope::ReconciledDocumentRuntime, Some(_)) => {
                Self::RECONCILED_CORE_CONFIG_CAPTURED_ONLY_REASON
            }
            (Some(_), ConfigProvenanceScope::StaticOrOneShot, Some(_)) => {
                Self::STATIC_CORE_CONFIG_CAPTURED_ONLY_REASON
            }
            (None, _, Some(_)) => Self::ONESHOT_WITH_CONFIG_CAPTURED_ONLY_REASON,
            (Some(_), _, None) => Self::UNPINNED_CONFIG_CAPTURED_ONLY_REASON,
            (None, _, None) => Self::ONESHOT_CAPTURED_ONLY_REASON,
        }
    }

    pub fn captured_only(
        capture_scope: String,
        provider_endpoint: Option<String>,
        assembly_trace: AssemblyTrace,
    ) -> Self {
        Self::captured_only_with_request_provenance(
            capture_scope,
            provider_endpoint,
            None,
            assembly_trace,
        )
    }

    pub fn captured_only_with_request_provenance(
        capture_scope: String,
        provider_endpoint: Option<String>,
        request_provenance: Option<crate::RequestExecutionProvenance>,
        assembly_trace: AssemblyTrace,
    ) -> Self {
        Self::captured_only_with_request_and_transcript_provenance(
            capture_scope,
            provider_endpoint,
            request_provenance,
            Vec::new(),
            assembly_trace,
        )
    }

    pub fn captured_only_with_request_and_transcript_provenance(
        capture_scope: String,
        provider_endpoint: Option<String>,
        request_provenance: Option<crate::RequestExecutionProvenance>,
        transcript_snapshot: Vec<crate::MessageFactRef>,
        assembly_trace: AssemblyTrace,
    ) -> Self {
        Self::captured_only_with_request_transcript_and_config_provenance(
            capture_scope,
            provider_endpoint,
            request_provenance,
            transcript_snapshot,
            None,
            assembly_trace,
        )
    }

    pub fn captured_only_with_request_transcript_and_config_provenance(
        capture_scope: String,
        provider_endpoint: Option<String>,
        request_provenance: Option<crate::RequestExecutionProvenance>,
        transcript_snapshot: Vec<crate::MessageFactRef>,
        config_provenance: Option<crate::ResolvedBehaviorConfigProvenance>,
        assembly_trace: AssemblyTrace,
    ) -> Self {
        Self::captured_only_with_scoped_config_provenance(
            capture_scope,
            provider_endpoint,
            request_provenance,
            transcript_snapshot,
            ConfigProvenanceScope::StaticOrOneShot,
            config_provenance,
            assembly_trace,
        )
    }

    pub fn captured_only_with_scoped_config_provenance(
        capture_scope: String,
        provider_endpoint: Option<String>,
        request_provenance: Option<crate::RequestExecutionProvenance>,
        transcript_snapshot: Vec<crate::MessageFactRef>,
        config_provenance_scope: ConfigProvenanceScope,
        config_provenance: Option<crate::ResolvedBehaviorConfigProvenance>,
        assembly_trace: AssemblyTrace,
    ) -> Self {
        Self::captured_only_with_admission_and_scoped_config_provenance(
            capture_scope,
            provider_endpoint,
            request_provenance,
            transcript_snapshot,
            InferenceCallProvenanceScope::StaticOrTest,
            None,
            config_provenance_scope,
            config_provenance,
            assembly_trace,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn captured_only_with_admission_and_scoped_config_provenance(
        capture_scope: String,
        provider_endpoint: Option<String>,
        request_provenance: Option<crate::RequestExecutionProvenance>,
        transcript_snapshot: Vec<crate::MessageFactRef>,
        inference_call_provenance_scope: InferenceCallProvenanceScope,
        inference_call_provenance: Option<crate::SignedDocumentVersionRef>,
        config_provenance_scope: ConfigProvenanceScope,
        config_provenance: Option<crate::ResolvedBehaviorConfigProvenance>,
        assembly_trace: AssemblyTrace,
    ) -> Self {
        let status_reason = Self::captured_only_reason(
            request_provenance.as_ref(),
            inference_call_provenance_scope,
            config_provenance_scope,
            config_provenance.as_ref(),
        );
        Self {
            manifest_version: PROVENANCE_MANIFEST_VERSION,
            status: ProvenanceStatus::CapturedOnly,
            status_reason: status_reason.to_string(),
            capture_seam: CaptureSeam::TransportBody,
            capture_scope,
            provider_endpoint,
            request_version: None,
            request_provenance,
            inference_call_provenance_scope,
            inference_call_provenance,
            transcript_snapshot,
            config_provenance_scope,
            config_provenance,
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
    #[serde(default)]
    pub request_provenance: Option<crate::RequestExecutionProvenance>,
    /// Explicitly separates admitted runtime calls from static/unit transport
    /// construction. Production callers must not select the static scope.
    #[serde(default)]
    pub inference_call_provenance_scope: InferenceCallProvenanceScope,
    /// Exact finalized transcript snapshot loaded for every completion loop in
    /// this request-wide capture context.
    #[serde(default)]
    pub transcript_snapshot: Vec<crate::MessageFactRef>,
    /// Explicit origin of the behavior configuration. Static constructors use
    /// the default; the document runtime sets the reconciled variant before
    /// looking up the exact bundle.
    #[serde(default)]
    pub config_provenance_scope: ConfigProvenanceScope,
    /// Exact signed core config sources retained by the active reconciled
    /// behavior generation. Optional for one-shot and legacy/static builders.
    #[serde(default)]
    pub config_provenance: Option<crate::ResolvedBehaviorConfigProvenance>,
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
    pub(crate) fn for_request(
        request: &crate::watcher::AgentRequest,
        request_provenance: crate::RequestExecutionProvenance,
        transcript_snapshot: Vec<crate::MessageFactRef>,
        config_provenance_scope: ConfigProvenanceScope,
        config_provenance: Option<crate::ResolvedBehaviorConfigProvenance>,
        model_name: String,
    ) -> Self {
        Self {
            request_doc_id: request.doc_id.clone(),
            request_provenance: Some(request_provenance),
            inference_call_provenance_scope: InferenceCallProvenanceScope::AdmittedProviderCall,
            transcript_snapshot,
            config_provenance_scope,
            config_provenance,
            request_id: request.request_id.clone(),
            agent_did: request.agent_did.clone(),
            requester_did: request.requester_did.clone().unwrap_or_default(),
            behavior_id: request.behavior_id.clone().unwrap_or_default(),
            session_id: request.session_id.clone(),
            model_name,
        }
    }

    pub(crate) fn without_transcript_snapshot(mut self) -> Self {
        self.transcript_snapshot.clear();
        self
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
    /// Composite commit CID of the exact signed source `AgentRequest` admitted
    /// before claiming. Empty only for one-shot runs.
    pub request_source_commit_cid: String,
    /// DID derived only after cryptographic verification of the source commit.
    pub request_source_signer_did: String,
    /// Composite commit CID of the exact target-agent claim snapshot consumed
    /// by the runtime. Empty only for one-shot runs.
    pub request_claim_commit_cid: String,
    /// DID derived only after cryptographic verification of the claim commit.
    pub request_claim_signer_did: String,
    /// Exact signed running `InferenceCall` snapshot which admitted this send.
    /// Empty only under an explicit static/test capture scope.
    pub inference_call_doc_id: String,
    pub inference_call_composite_commit_cid: String,
    pub inference_call_signer_did: String,
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

fn validate_transcript_snapshot(snapshot: &[crate::MessageFactRef]) -> Result<()> {
    let mut previous_sequence = None;
    let mut doc_ids = BTreeSet::new();
    let mut composite_cids = BTreeSet::new();
    for fact_ref in snapshot {
        if fact_ref.doc_id.trim().is_empty()
            || fact_ref.composite_commit_cid.trim().is_empty()
            || fact_ref.signer_did.trim().is_empty()
        {
            anyhow::bail!(
                "transcript snapshot reference at sequence {} has incomplete DefraDB provenance",
                fact_ref.sequence
            );
        }
        if previous_sequence.is_some_and(|previous| fact_ref.sequence <= previous) {
            anyhow::bail!(
                "transcript snapshot references are not in canonical sequence order: previous={previous_sequence:?} current={}",
                fact_ref.sequence
            );
        }
        if !doc_ids.insert(fact_ref.doc_id.as_str()) {
            anyhow::bail!(
                "transcript snapshot repeats AgentMessage _docID {}",
                fact_ref.doc_id
            );
        }
        if !composite_cids.insert(fact_ref.composite_commit_cid.as_str()) {
            anyhow::bail!(
                "transcript snapshot repeats AgentMessage composite CID {}",
                fact_ref.composite_commit_cid
            );
        }
        previous_sequence = Some(fact_ref.sequence);
    }
    Ok(())
}

fn validate_config_provenance(
    scope: ConfigProvenanceScope,
    provenance: Option<&crate::ResolvedBehaviorConfigProvenance>,
    behavior_id: &str,
    agent_did: &str,
) -> Result<()> {
    if scope == ConfigProvenanceScope::ReconciledDocumentRuntime && provenance.is_none() {
        anyhow::bail!(
            "reconciled document-runtime capture for behavior {behavior_id} has no exact config provenance"
        );
    }
    if let Some(provenance) = provenance {
        provenance.validate_for_behavior(behavior_id, agent_did)?;
    }
    Ok(())
}

fn validate_inference_call_provenance(
    scope: InferenceCallProvenanceScope,
    provenance: Option<&crate::SignedDocumentVersionRef>,
) -> Result<()> {
    match (scope, provenance) {
        (InferenceCallProvenanceScope::AdmittedProviderCall, Some(provenance)) => {
            if provenance.version.doc_id.trim().is_empty()
                || provenance.version.composite_commit_cid.trim().is_empty()
                || provenance.signer_did.trim().is_empty()
            {
                anyhow::bail!(
                    "admitted provider-call capture has incomplete signed InferenceCall provenance"
                );
            }
            Ok(())
        }
        (InferenceCallProvenanceScope::AdmittedProviderCall, None) => anyhow::bail!(
            "admitted provider-call capture has no signed running InferenceCall provenance"
        ),
        (InferenceCallProvenanceScope::StaticOrTest, None) => Ok(()),
        (InferenceCallProvenanceScope::StaticOrTest, Some(_)) => anyhow::bail!(
            "static/test rendered-request capture cannot claim InferenceCall provenance"
        ),
    }
}

pub(crate) fn build_rendered_completion_request(
    context: &RenderedRequestContext,
    inference_call_provenance: Option<crate::SignedDocumentVersionRef>,
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
    match (&context.request_doc_id, &context.request_provenance) {
        (doc_id, Some(provenance)) if !doc_id.is_empty() => provenance
            .validate_for_request(doc_id, &context.agent_did)
            .context("invalid rendered-request source/claim provenance")?,
        (doc_id, None) if doc_id.is_empty() => {}
        (doc_id, Some(_)) if doc_id.is_empty() => {
            anyhow::bail!("one-shot rendered-request context cannot carry request provenance")
        }
        (doc_id, None) => anyhow::bail!(
            "document-backed rendered-request context {doc_id} has no signed source/claim provenance"
        ),
        (doc_id, Some(_)) => anyhow::bail!(
            "rendered-request context document {doc_id} has invalid source/claim provenance"
        ),
    }
    validate_transcript_snapshot(&context.transcript_snapshot)
        .context("invalid rendered-request transcript snapshot provenance")?;
    validate_config_provenance(
        context.config_provenance_scope,
        context.config_provenance.as_ref(),
        &context.behavior_id,
        &context.agent_did,
    )
    .context("invalid rendered-request resolved config provenance")?;
    validate_inference_call_provenance(
        context.inference_call_provenance_scope,
        inference_call_provenance.as_ref(),
    )
    .context("invalid rendered-request inference-call provenance")?;
    let capture_key = capture_key(
        &context.agent_did,
        &context.session_id,
        &context.request_doc_id,
        capture_scope,
        turn_index,
        attempt,
    )?;
    let manifest = ProvenanceManifest::captured_only_with_admission_and_scoped_config_provenance(
        capture_scope.to_string(),
        provider_endpoint,
        context.request_provenance.clone(),
        context.transcript_snapshot.clone(),
        context.inference_call_provenance_scope,
        inference_call_provenance.clone(),
        context.config_provenance_scope,
        context.config_provenance.clone(),
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
        request_source_commit_cid: context
            .request_provenance
            .as_ref()
            .map(|provenance| provenance.source.version.composite_commit_cid.clone())
            .unwrap_or_default(),
        request_source_signer_did: context
            .request_provenance
            .as_ref()
            .map(|provenance| provenance.source.signer_did.clone())
            .unwrap_or_default(),
        request_claim_commit_cid: context
            .request_provenance
            .as_ref()
            .map(|provenance| provenance.claim.version.composite_commit_cid.clone())
            .unwrap_or_default(),
        request_claim_signer_did: context
            .request_provenance
            .as_ref()
            .map(|provenance| provenance.claim.signer_did.clone())
            .unwrap_or_default(),
        inference_call_doc_id: inference_call_provenance
            .as_ref()
            .map(|provenance| provenance.version.doc_id.clone())
            .unwrap_or_default(),
        inference_call_composite_commit_cid: inference_call_provenance
            .as_ref()
            .map(|provenance| provenance.version.composite_commit_cid.clone())
            .unwrap_or_default(),
        inference_call_signer_did: inference_call_provenance
            .as_ref()
            .map(|provenance| provenance.signer_did.clone())
            .unwrap_or_default(),
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

impl RenderedCompletionRequest {
    /// Validate the redundant durable columns and manifest as one canonical
    /// fact before the sink is allowed to create a row. The builder derives all
    /// of these values from one context, but the DTO and sink are public, so the
    /// persistence boundary must not trust callers to keep them coherent.
    pub(super) fn validate_new_capture(&self) -> Result<()> {
        if self.capture_version != CAPTURE_VERSION {
            anyhow::bail!(
                "new rendered-request capture has unsupported capture version {}",
                self.capture_version
            );
        }
        let expected_key = capture_key(
            &self.agent_did,
            &self.session_id,
            &self.request_doc_id,
            &self.capture_scope,
            self.turn_index,
            self.attempt,
        )?;
        if self.capture_key != expected_key {
            anyhow::bail!("rendered-request capture key disagrees with its identity tuple");
        }

        let manifest: ProvenanceManifest = serde_json::from_value(self.provenance_json.clone())
            .context("decoding rendered-request provenance manifest")?;
        if manifest.manifest_version != PROVENANCE_MANIFEST_VERSION {
            anyhow::bail!(
                "new rendered-request capture has unsupported provenance manifest version {}",
                manifest.manifest_version
            );
        }
        if manifest.capture_scope != self.capture_scope {
            anyhow::bail!("rendered-request manifest capture scope disagrees with its row");
        }
        if manifest.assembly_trace != self.assembly_trace {
            anyhow::bail!("rendered-request manifest assembly trace disagrees with its row");
        }
        if manifest.status != ProvenanceStatus::CapturedOnly
            || manifest.capture_seam != CaptureSeam::TransportBody
        {
            anyhow::bail!(
                "new rendered-request manifest must be a transport-body captured-only fact"
            );
        }
        validate_config_provenance(
            manifest.config_provenance_scope,
            manifest.config_provenance.as_ref(),
            &self.behavior_id,
            &self.agent_did,
        )
        .context("invalid rendered-request manifest resolved config provenance")?;
        validate_inference_call_provenance(
            manifest.inference_call_provenance_scope,
            manifest.inference_call_provenance.as_ref(),
        )
        .context("invalid rendered-request manifest inference-call provenance")?;
        match manifest.inference_call_provenance.as_ref() {
            Some(provenance) => {
                let row_provenance = (
                    self.inference_call_doc_id.as_str(),
                    self.inference_call_composite_commit_cid.as_str(),
                    self.inference_call_signer_did.as_str(),
                );
                let manifest_provenance = (
                    provenance.version.doc_id.as_str(),
                    provenance.version.composite_commit_cid.as_str(),
                    provenance.signer_did.as_str(),
                );
                if row_provenance != manifest_provenance {
                    anyhow::bail!(
                        "rendered-request inference-call columns disagree with the provenance manifest"
                    );
                }
            }
            None => {
                if !self.inference_call_doc_id.is_empty()
                    || !self.inference_call_composite_commit_cid.is_empty()
                    || !self.inference_call_signer_did.is_empty()
                {
                    anyhow::bail!(
                        "static/test rendered-request capture cannot carry inference-call columns"
                    );
                }
            }
        }
        let expected_status_reason = ProvenanceManifest::captured_only_reason(
            manifest.request_provenance.as_ref(),
            manifest.inference_call_provenance_scope,
            manifest.config_provenance_scope,
            manifest.config_provenance.as_ref(),
        );
        if manifest.status_reason != expected_status_reason {
            anyhow::bail!(
                "new rendered-request manifest status reason disagrees with its provenance set"
            );
        }

        // Re-encoding with the current serializer rejects unknown or legacy
        // fields on a newly-created v7 manifest. In particular, v3's
        // `request_version` remains deserialize-only and can never leak into a
        // new canonical row.
        let canonical_manifest = canonical_json(
            &serde_json::to_value(&manifest).context("encoding rendered-request manifest")?,
        );
        if canonical_json(&self.provenance_json) != canonical_manifest {
            anyhow::bail!("new rendered-request provenance manifest is not canonical v7");
        }

        validate_transcript_snapshot(&manifest.transcript_snapshot)
            .context("invalid rendered-request manifest transcript snapshot provenance")?;

        match (&self.request_doc_id, &manifest.request_provenance) {
            (doc_id, Some(provenance)) if !doc_id.is_empty() => {
                provenance
                    .validate_for_request(doc_id, &self.agent_did)
                    .context("invalid rendered-request manifest source/claim provenance")?;
                let row_provenance = (
                    self.request_source_commit_cid.as_str(),
                    self.request_source_signer_did.as_str(),
                    self.request_claim_commit_cid.as_str(),
                    self.request_claim_signer_did.as_str(),
                );
                let manifest_provenance = (
                    provenance.source.version.composite_commit_cid.as_str(),
                    provenance.source.signer_did.as_str(),
                    provenance.claim.version.composite_commit_cid.as_str(),
                    provenance.claim.signer_did.as_str(),
                );
                if row_provenance != manifest_provenance {
                    anyhow::bail!(
                        "rendered-request source/claim columns disagree with the provenance manifest"
                    );
                }
            }
            (doc_id, None) if doc_id.is_empty() => {
                if !self.request_source_commit_cid.is_empty()
                    || !self.request_source_signer_did.is_empty()
                    || !self.request_claim_commit_cid.is_empty()
                    || !self.request_claim_signer_did.is_empty()
                {
                    anyhow::bail!(
                        "one-shot rendered-request capture cannot carry source/claim columns"
                    );
                }
            }
            (doc_id, None) => anyhow::bail!(
                "document-backed rendered-request capture {doc_id} has no request provenance"
            ),
            (doc_id, Some(_)) => anyhow::bail!(
                "one-shot rendered-request capture cannot carry request provenance for {doc_id:?}"
            ),
        }
        Ok(())
    }
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

    fn transcript_snapshot() -> Vec<crate::MessageFactRef> {
        vec![
            crate::MessageFactRef {
                sequence: 1,
                doc_id: "message-doc-1".to_string(),
                composite_commit_cid: "bafy-message-1".to_string(),
                signer_did: "did:key:test".to_string(),
            },
            crate::MessageFactRef {
                sequence: 2,
                doc_id: "message-doc-2".to_string(),
                composite_commit_cid: "bafy-message-2".to_string(),
                signer_did: "did:key:test".to_string(),
            },
        ]
    }

    fn config_fact(
        collection: &str,
        logical_id: &str,
        doc_id: &str,
        cid: &str,
    ) -> crate::ConfigFactRef {
        crate::ConfigFactRef::new(
            collection,
            logical_id,
            crate::SignedDocumentVersionRef::new(
                crate::DocumentVersionRef::new(doc_id, cid),
                "did:key:config-author",
            ),
        )
    }

    fn config_provenance() -> crate::ResolvedBehaviorConfigProvenance {
        crate::ResolvedBehaviorConfigProvenance {
            principal: config_fact(
                "AgentPrincipal",
                "did:key:test",
                "principal-doc",
                "bafy-principal",
            ),
            behavior: config_fact("AgentBehavior", "behavior", "behavior-doc", "bafy-behavior"),
            inference_backend: config_fact(
                "InferenceBackend",
                "backend",
                "backend-doc",
                "bafy-backend",
            ),
            inference_profile: config_fact(
                "InferenceProfile",
                "profile",
                "profile-doc",
                "bafy-profile",
            ),
            tool_selection: Some(config_fact(
                "ToolSelection",
                "tools",
                "tools-doc",
                "bafy-tools",
            )),
            skills: vec![
                config_fact("Skill", "review", "skill-doc-1", "bafy-skill-1"),
                config_fact("Skill", "write", "skill-doc-2", "bafy-skill-2"),
            ],
            resolution_algorithm_version: 1,
        }
    }

    fn inference_call_provenance() -> crate::SignedDocumentVersionRef {
        crate::SignedDocumentVersionRef::new(
            crate::DocumentVersionRef::new("inference-call-doc", "bafy-inference-running"),
            "did:key:test",
        )
    }

    fn context() -> RenderedRequestContext {
        RenderedRequestContext {
            request_doc_id: "doc-1".to_string(),
            request_provenance: Some(crate::document_version::test_request_execution_provenance(
                "doc-1",
                "did:key:test",
            )),
            inference_call_provenance_scope: InferenceCallProvenanceScope::StaticOrTest,
            transcript_snapshot: transcript_snapshot(),
            config_provenance_scope: ConfigProvenanceScope::ReconciledDocumentRuntime,
            config_provenance: Some(config_provenance()),
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
            None,
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
                None,
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
        assert_eq!(manifest.request_version, None);
        assert_eq!(
            manifest.inference_call_provenance_scope,
            InferenceCallProvenanceScope::StaticOrTest
        );
        assert_eq!(manifest.inference_call_provenance, None);
        assert!(rendered.inference_call_doc_id.is_empty());
        assert!(rendered.inference_call_composite_commit_cid.is_empty());
        assert!(rendered.inference_call_signer_did.is_empty());
        assert_eq!(
            manifest.request_provenance,
            Some(crate::document_version::test_request_execution_provenance(
                "doc-1",
                "did:key:test",
            ))
        );
        assert_eq!(rendered.request_source_commit_cid, "bafy-source-1");
        assert_eq!(rendered.request_source_signer_did, "did:key:source");
        assert_eq!(rendered.request_claim_commit_cid, "bafy-claim-1");
        assert_eq!(rendered.request_claim_signer_did, "did:key:test");
        assert_eq!(manifest.transcript_snapshot, transcript_snapshot());
        assert_eq!(
            manifest.config_provenance_scope,
            ConfigProvenanceScope::ReconciledDocumentRuntime
        );
        assert_eq!(manifest.config_provenance, Some(config_provenance()));
        assert!(manifest
            .status_reason
            .contains("explicit static/test capture"));
        assert!(manifest.status_reason.contains("MCP availability"));
        assert!(manifest.status_reason.contains("host tool ceiling"));
        assert!(manifest
            .status_reason
            .contains("compaction source versions"));
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

    /// Version 7 declares `captured_only` positively. An absent field is never
    /// the evidence — a reader must be able to see the claim, not infer it.
    #[test]
    fn version_seven_provenance_never_claims_full_reconstruction() {
        let rendered = build(0, 0, empty_trace(), components());

        assert_eq!(rendered.provenance_json["status"], "captured_only");
        assert_eq!(
            rendered.provenance_json["manifest_version"],
            PROVENANCE_MANIFEST_VERSION
        );
        assert!(rendered.provenance_json.get("assembly_trace").is_some());
        assert!(
            rendered.provenance_json.get("request_version").is_none(),
            "v7 must never emit the legacy single-version field"
        );
        assert!(rendered.provenance_json.get("config_provenance").is_some());
    }

    #[test]
    fn admitted_capture_requires_and_pins_exact_running_call_provenance() {
        let mut admitted = context();
        admitted.inference_call_provenance_scope =
            InferenceCallProvenanceScope::AdmittedProviderCall;
        let call = inference_call_provenance();
        let rendered = build_rendered_completion_request(
            &admitted,
            Some(call.clone()),
            "inference.1",
            RenderedRequestSource::OpenAiChatCompletions,
            None,
            0,
            0,
            empty_trace(),
            components(),
        )
        .expect("admitted capture");

        assert_eq!(rendered.inference_call_doc_id, call.version.doc_id);
        assert_eq!(
            rendered.inference_call_composite_commit_cid,
            call.version.composite_commit_cid
        );
        assert_eq!(rendered.inference_call_signer_did, call.signer_did);
        let manifest: ProvenanceManifest =
            serde_json::from_value(rendered.provenance_json.clone()).expect("manifest");
        assert_eq!(
            manifest.inference_call_provenance_scope,
            InferenceCallProvenanceScope::AdmittedProviderCall
        );
        assert_eq!(manifest.inference_call_provenance, Some(call));
        rendered
            .validate_new_capture()
            .expect("canonical admitted capture");
    }

    #[test]
    fn admitted_capture_fails_closed_without_running_call_and_rejects_column_drift() {
        let mut admitted = context();
        admitted.inference_call_provenance_scope =
            InferenceCallProvenanceScope::AdmittedProviderCall;
        let missing = build_rendered_completion_request(
            &admitted,
            None,
            "inference.1",
            RenderedRequestSource::OpenAiChatCompletions,
            None,
            0,
            0,
            empty_trace(),
            components(),
        )
        .expect_err("admitted capture without a running call must fail");
        assert!(format!("{missing:#}").contains("no signed running InferenceCall provenance"));

        let mut rendered = build_rendered_completion_request(
            &admitted,
            Some(inference_call_provenance()),
            "inference.1",
            RenderedRequestSource::OpenAiChatCompletions,
            None,
            0,
            0,
            empty_trace(),
            components(),
        )
        .expect("admitted capture");
        rendered.inference_call_composite_commit_cid = "bafy-forged".to_string();
        let drift = rendered
            .validate_new_capture()
            .expect_err("row/manifest inference-call drift must fail");
        assert!(drift
            .to_string()
            .contains("inference-call columns disagree"));
    }

    #[test]
    fn transcript_snapshot_must_be_canonically_ordered() {
        let mut context = context();
        context.transcript_snapshot.swap(0, 1);

        let error = build_rendered_completion_request(
            &context,
            None,
            "inference.1",
            RenderedRequestSource::OpenAiChatCompletions,
            None,
            0,
            0,
            empty_trace(),
            components(),
        )
        .expect_err("out-of-order transcript facts must be rejected");

        assert!(
            format!("{error:#}").contains("canonical sequence order"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn persisted_manifest_rejects_noncanonical_transcript_snapshot() {
        let mut rendered = build(0, 0, empty_trace(), components());
        rendered.provenance_json["transcript_snapshot"]
            .as_array_mut()
            .expect("transcript snapshot")
            .swap(0, 1);

        let error = rendered
            .validate_new_capture()
            .expect_err("persisted manifest order must be validated independently");
        assert!(
            format!("{error:#}").contains("canonical sequence order"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn version_two_manifest_without_request_version_remains_readable() {
        let mut value = serde_json::to_value(ProvenanceManifest::captured_only(
            "inference.1".to_string(),
            Some("https://provider.example".to_string()),
            empty_trace(),
        ))
        .expect("legacy manifest fixture");
        value["manifest_version"] = json!(2);
        value
            .as_object_mut()
            .expect("manifest object")
            .remove("request_version");

        let decoded: ProvenanceManifest =
            serde_json::from_value(value).expect("version-two manifest remains readable");
        assert_eq!(decoded.manifest_version, 2);
        assert_eq!(decoded.request_version, None);
        assert_eq!(decoded.request_provenance, None);
        assert_eq!(decoded.status, ProvenanceStatus::CapturedOnly);
    }

    #[test]
    fn version_three_request_version_is_decode_only() {
        let mut value = serde_json::to_value(ProvenanceManifest::captured_only(
            "inference.1".to_string(),
            Some("https://provider.example".to_string()),
            empty_trace(),
        ))
        .expect("legacy manifest fixture");
        value["manifest_version"] = json!(3);
        value
            .as_object_mut()
            .expect("manifest object")
            .remove("request_provenance");
        value["request_version"] = json!({
            "doc_id": "doc-legacy",
            "composite_commit_cid": "bafy-legacy-claim"
        });

        let decoded: ProvenanceManifest =
            serde_json::from_value(value).expect("version-three manifest remains readable");
        assert_eq!(decoded.manifest_version, 3);
        assert_eq!(
            decoded.request_version,
            Some(crate::DocumentVersionRef::new(
                "doc-legacy",
                "bafy-legacy-claim"
            ))
        );
        assert_eq!(decoded.request_provenance, None);

        let reencoded = serde_json::to_value(decoded).expect("legacy manifest re-encodes");
        assert!(
            reencoded.get("request_version").is_none(),
            "legacy request_version is accepted for decoding but never emitted"
        );
    }

    #[test]
    fn version_four_manifest_without_transcript_snapshot_remains_readable() {
        let mut value =
            serde_json::to_value(ProvenanceManifest::captured_only_with_request_provenance(
                "inference.1".to_string(),
                Some("https://provider.example".to_string()),
                Some(crate::document_version::test_request_execution_provenance(
                    "doc-1",
                    "did:key:test",
                )),
                empty_trace(),
            ))
            .expect("legacy manifest fixture");
        value["manifest_version"] = json!(4);
        value
            .as_object_mut()
            .expect("manifest object")
            .remove("transcript_snapshot");

        let decoded: ProvenanceManifest =
            serde_json::from_value(value).expect("version-four manifest remains readable");
        assert_eq!(decoded.manifest_version, 4);
        assert!(decoded.transcript_snapshot.is_empty());
        assert!(decoded.request_provenance.is_some());
    }

    #[test]
    fn version_five_manifest_without_config_provenance_remains_readable() {
        let mut value = serde_json::to_value(
            ProvenanceManifest::captured_only_with_request_transcript_and_config_provenance(
                "inference.1".to_string(),
                Some("https://provider.example".to_string()),
                Some(crate::document_version::test_request_execution_provenance(
                    "doc-1",
                    "did:key:test",
                )),
                transcript_snapshot(),
                Some(config_provenance()),
                empty_trace(),
            ),
        )
        .expect("legacy manifest fixture");
        value["manifest_version"] = json!(5);
        value
            .as_object_mut()
            .expect("manifest object")
            .remove("config_provenance");
        value
            .as_object_mut()
            .expect("manifest object")
            .remove("config_provenance_scope");

        let decoded: ProvenanceManifest =
            serde_json::from_value(value).expect("version-five manifest remains readable");
        assert_eq!(decoded.manifest_version, 5);
        assert_eq!(
            decoded.config_provenance_scope,
            ConfigProvenanceScope::StaticOrOneShot
        );
        assert!(decoded.config_provenance.is_none());
        assert_eq!(decoded.transcript_snapshot, transcript_snapshot());
    }

    #[test]
    fn resolved_config_provenance_cannot_be_rebound_to_another_behavior_or_agent() {
        let mut wrong_behavior = context();
        wrong_behavior
            .config_provenance
            .as_mut()
            .expect("config provenance")
            .behavior
            .logical_id = "other-behavior".to_string();
        let error = build_rendered_completion_request(
            &wrong_behavior,
            None,
            "inference.1",
            RenderedRequestSource::OpenAiChatCompletions,
            None,
            0,
            0,
            empty_trace(),
            components(),
        )
        .expect_err("config provenance for another behavior must fail");
        assert!(
            format!("{error:#}").contains("invalid rendered-request resolved config provenance")
        );

        let mut wrong_agent = build(0, 0, empty_trace(), components());
        wrong_agent.provenance_json["config_provenance"]["principal"]["logical_id"] =
            json!("did:key:other-agent");
        let error = wrong_agent
            .validate_new_capture()
            .expect_err("persisted config provenance for another agent must fail");
        assert!(format!("{error:#}")
            .contains("invalid rendered-request manifest resolved config provenance"));
    }

    #[test]
    fn resolved_config_provenance_requires_canonical_skill_order() {
        let mut context = context();
        context
            .config_provenance
            .as_mut()
            .expect("config provenance")
            .skills
            .swap(0, 1);
        let error = build_rendered_completion_request(
            &context,
            None,
            "inference.1",
            RenderedRequestSource::OpenAiChatCompletions,
            None,
            0,
            0,
            empty_trace(),
            components(),
        )
        .expect_err("noncanonical config sources must fail before capture");
        assert!(
            format!("{error:#}").contains("invalid rendered-request resolved config provenance")
        );
    }

    #[test]
    fn reconciled_capture_rejects_missing_config_provenance() {
        let mut missing_at_build = context();
        missing_at_build.config_provenance = None;
        let error = build_rendered_completion_request(
            &missing_at_build,
            None,
            "inference.1",
            RenderedRequestSource::OpenAiChatCompletions,
            None,
            0,
            0,
            empty_trace(),
            components(),
        )
        .expect_err("a reconciled context cannot omit exact config provenance");
        assert!(format!("{error:#}").contains("has no exact config provenance"));

        let mut rendered = build(0, 0, empty_trace(), components());
        rendered
            .provenance_json
            .as_object_mut()
            .expect("manifest object")
            .remove("config_provenance");

        let error = rendered
            .validate_new_capture()
            .expect_err("a reconciled capture cannot lose its exact config bundle");
        assert!(format!("{error:#}").contains("has no exact config provenance"));
    }

    #[test]
    fn explicit_static_test_contexts_may_omit_config_provenance() {
        let mut legacy_static = context();
        legacy_static.config_provenance_scope = ConfigProvenanceScope::StaticOrOneShot;
        legacy_static.config_provenance = None;
        let rendered = build_rendered_completion_request(
            &legacy_static,
            None,
            "inference.1",
            RenderedRequestSource::OpenAiChatCompletions,
            None,
            0,
            0,
            empty_trace(),
            components(),
        )
        .expect("legacy static context remains capturable");
        assert!(rendered.provenance_json.get("config_provenance").is_none());
        assert!(rendered.provenance_json["status_reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("no resolved core config provenance")));

        let mut oneshot = legacy_static;
        oneshot.request_doc_id.clear();
        oneshot.request_provenance = None;
        oneshot.transcript_snapshot.clear();
        let rendered = build_rendered_completion_request(
            &oneshot,
            None,
            "oneshot.1",
            RenderedRequestSource::OpenAiChatCompletions,
            None,
            0,
            0,
            empty_trace(),
            components(),
        )
        .expect("one-shot-shaped static fixture may omit config provenance");
        assert!(rendered.provenance_json.get("config_provenance").is_none());
        assert!(rendered.provenance_json["status_reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("explicit static/test capture")));
    }

    /// The seam is recorded positively so a reader never has to guess whether
    /// the bytes predate the ChatGPT-Codex and Grok body rewrites.
    #[test]
    fn provenance_names_the_seam_the_bytes_came_from() {
        let rendered = build(0, 0, empty_trace(), components());

        assert_eq!(rendered.provenance_json["capture_seam"], "transport_body");
        assert_eq!(rendered.provenance_json["capture_scope"], "inference.1");
    }

    #[test]
    fn document_backed_capture_fails_closed_without_matching_provenance() {
        let mut missing = context();
        missing.request_provenance = None;
        let error = build_rendered_completion_request(
            &missing,
            None,
            "inference.1",
            RenderedRequestSource::OpenAiChatCompletions,
            None,
            0,
            0,
            empty_trace(),
            components(),
        )
        .expect_err("document-backed capture without signed provenance must fail");
        assert!(error
            .to_string()
            .contains("no signed source/claim provenance"));

        let mut mismatched = context();
        mismatched
            .request_provenance
            .as_mut()
            .expect("provenance")
            .source
            .version
            .doc_id = "another-doc".to_string();
        let error = build_rendered_completion_request(
            &mismatched,
            None,
            "inference.1",
            RenderedRequestSource::OpenAiChatCompletions,
            None,
            0,
            0,
            empty_trace(),
            components(),
        )
        .expect_err("source provenance for another document must fail");
        assert!(
            format!("{error:#}").contains("invalid rendered-request source/claim provenance"),
            "unexpected error: {error:#}"
        );

        let mut empty_source_cid = context();
        empty_source_cid
            .request_provenance
            .as_mut()
            .expect("provenance")
            .source
            .version
            .composite_commit_cid
            .clear();
        let error = build_rendered_completion_request(
            &empty_source_cid,
            None,
            "inference.1",
            RenderedRequestSource::OpenAiChatCompletions,
            None,
            0,
            0,
            empty_trace(),
            components(),
        )
        .expect_err("empty source CID must fail");
        assert!(
            format!("{error:#}").contains("source/claim provenance"),
            "unexpected error: {error:#}"
        );

        let mut reused_cid = context();
        let claim_cid = reused_cid
            .request_provenance
            .as_ref()
            .expect("provenance")
            .claim
            .version
            .composite_commit_cid
            .clone();
        reused_cid
            .request_provenance
            .as_mut()
            .expect("provenance")
            .source
            .version
            .composite_commit_cid = claim_cid;
        let error = build_rendered_completion_request(
            &reused_cid,
            None,
            "inference.1",
            RenderedRequestSource::OpenAiChatCompletions,
            None,
            0,
            0,
            empty_trace(),
            components(),
        )
        .expect_err("source and claim must name distinct commits");
        assert!(
            format!("{error:#}").contains("source/claim provenance"),
            "unexpected error: {error:#}"
        );

        let mut wrong_claim_signer = context();
        wrong_claim_signer
            .request_provenance
            .as_mut()
            .expect("provenance")
            .claim
            .signer_did = "did:key:another-agent".to_string();
        let error = build_rendered_completion_request(
            &wrong_claim_signer,
            None,
            "inference.1",
            RenderedRequestSource::OpenAiChatCompletions,
            None,
            0,
            0,
            empty_trace(),
            components(),
        )
        .expect_err("claim signer must match the target agent");
        assert!(
            format!("{error:#}").contains("source/claim provenance"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn new_capture_validation_rejects_manifest_column_drift_and_legacy_fields() {
        let mut drifted = build(0, 0, empty_trace(), components());
        drifted.request_source_signer_did = "did:key:different-source".to_string();
        let error = drifted
            .validate_new_capture()
            .expect_err("row and manifest provenance must agree");
        assert!(error.to_string().contains("columns disagree"));

        let mut legacy_field = build(0, 0, empty_trace(), components());
        legacy_field.provenance_json["request_version"] = json!({
            "doc_id": "doc-1",
            "composite_commit_cid": "bafy-legacy-claim"
        });
        let error = legacy_field
            .validate_new_capture()
            .expect_err("a new v7 manifest cannot carry request_version");
        assert!(error.to_string().contains("not canonical v7"));
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
        let context = RenderedRequestContext::for_request(
            &request,
            crate::document_version::test_request_execution_provenance("doc-1", "did:key:test"),
            transcript_snapshot(),
            ConfigProvenanceScope::ReconciledDocumentRuntime,
            Some(config_provenance()),
            "test-model".to_string(),
        );
        assert_eq!(context.requester_did, "");
        assert_eq!(context.transcript_snapshot, transcript_snapshot());
        assert_eq!(context.config_provenance, Some(config_provenance()));
        let without_transcript = context.clone().without_transcript_snapshot();
        assert!(without_transcript.transcript_snapshot.is_empty());
        assert_eq!(
            without_transcript.config_provenance,
            Some(config_provenance())
        );

        request.requester_did = Some("did:key:requester".to_string());
        let context = RenderedRequestContext::for_request(
            &request,
            crate::document_version::test_request_execution_provenance("doc-1", "did:key:test"),
            Vec::new(),
            ConfigProvenanceScope::ReconciledDocumentRuntime,
            Some(config_provenance()),
            "test-model".to_string(),
        );
        assert_eq!(context.requester_did, "did:key:requester");
    }
}
