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
//! * `AssemblyTrace` is the reconstruction manifest. It records the effective
//!   native messages plus the ordered `ProviderContextReduction` keys that
//!   produced a sticky request-local projection. `RenderedRequest` remains the
//!   exact provider-wire oracle.
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

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub mod commits;
pub(crate) mod scope;
pub mod sink;
pub(crate) mod transport;

pub use gents_protocol::rendered_request::{
    AdmissionJoin, AssemblyBuildPath, AssemblyTrace, AssistantMessageId, CaptureOrderKey,
    CaptureScope, CaptureScopeKind, CaptureSeam, ParsedProvenance, ProvenanceManifest,
    ProvenanceStatus, RenderedRequestSource, ThreadedToolResult, ASSEMBLY_TRACE_VERSION,
    CAPTURE_VERSION, PROVENANCE_MANIFEST_VERSION,
};
pub(crate) use sink::defra_rendered_request_capture_factory;
pub use sink::DefraRenderedRequestSink;
pub use transport::RenderedRequestCapturingHttpClient;

/// Prefix on every capture key. Bound to the *key derivation*, not to
/// `CAPTURE_VERSION`: adding a column must not silently re-key existing facts.
const CAPTURE_KEY_PREFIX: &str = "rendered:v1";

pub(crate) type RenderedRequestCaptureSink = Arc<
    dyn Fn(RenderedCompletionRequest) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>
        + Send
        + Sync,
>;

pub(crate) type RenderedRequestCaptureFactory =
    Arc<dyn Fn(RenderedRequestContext) -> RenderedRequestCaptureSink + Send + Sync>;

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
    /// Exact composite DefraDB commit returned by the mutation that claimed
    /// this request. Empty only for a one-shot run.
    pub request_commit_cid: String,
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
    #[cfg(test)]
    pub(crate) fn for_request(request: &crate::watcher::AgentRequest, model_name: String) -> Self {
        Self::for_request_version(request, "", model_name)
    }

    pub(crate) fn for_claimed_request(
        request: &crate::watcher::AgentRequest,
        request_commit_cid: &str,
        model_name: String,
    ) -> Self {
        Self::for_request_version(request, request_commit_cid, model_name)
    }

    fn for_request_version(
        request: &crate::watcher::AgentRequest,
        request_commit_cid: &str,
        model_name: String,
    ) -> Self {
        Self {
            request_doc_id: request.doc_id.clone(),
            request_commit_cid: request_commit_cid.to_string(),
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
    /// The complete provider request. This is the fact record; the fields below
    /// are typed views used while validating transport conversion.
    pub(crate) request_json: Value,
    pub(crate) messages_json: Value,
    pub(crate) tools_json: Value,
    pub(crate) tool_choice_json: Value,
    pub(crate) sampling_json: Value,
}

impl RenderedRequestComponents {
    /// Split a captured provider body into the payload plus derived views.
    ///
    /// Every view is read out of the body, never re-derived from the assembled
    /// `CompletionRequest`: on the Codex path the assembled request still
    /// carries `temperature`/`max_output_tokens` that the transport deleted,
    /// and a row that reported those would be describing a request nobody sent.
    pub(crate) fn from_provider_body(request_json: Value, source: RenderedRequestSource) -> Self {
        let field = |name: &str| request_json.get(name).cloned();
        let messages_json =
            field(source.messages_field()).unwrap_or_else(|| Value::Array(Vec::new()));
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
    /// Exact composite version of `request_doc_id` that supplied the runtime
    /// input. The CID is DefraDB's native time-travel and integrity reference.
    pub request_commit_cid: String,
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
    /// The complete rendered provider request retained as the durable payload.
    pub request_json: Value,
    pub messages_json: Value,
    pub tools_json: Value,
    pub tool_choice_json: Value,
    pub sampling_json: Value,
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
        tools_json,
        tool_choice_json,
        sampling_json,
    } = components;

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
        admission_join_for_scope(capture_scope),
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
        request_commit_cid: context.request_commit_cid.clone(),
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
        assembly_trace,
        provenance_json,
    })
}

/// The admission identity to stamp into this capture's provenance, if the call
/// in flight on this task belongs to the loop the capture describes.
///
/// The kind guard keeps the task-local admission join honest if a caller wires
/// the wrong capture scope. One-shot captures never join because no admission
/// scope exists there.
fn admission_join_for_scope(capture_scope: &str) -> Option<AdmissionJoin> {
    let join = crate::admission::current_call_join()?;
    let scope_kind = capture_scope.parse::<CaptureScope>().ok()?.kind;
    admission_kind_matches_scope(join.call_kind, scope_kind).then(|| AdmissionJoin {
        call_id: join.call_id,
        call_seq: join.call_seq,
    })
}

/// Which admission [`CallKind`](crate::admission::CallKind) legitimately
/// produces captures of which [`CaptureScopeKind`]. `OneShot` maps to nothing:
/// one-shot runs have no admission scope at all, so a join observed under a
/// oneshot capture could only be another loop's call.
pub(crate) fn admission_kind_matches_scope(
    call_kind: crate::admission::CallKind,
    scope_kind: CaptureScopeKind,
) -> bool {
    use crate::admission::CallKind;

    matches!(
        (call_kind, scope_kind),
        (CallKind::Inference, CaptureScopeKind::Inference)
            | (CallKind::Compaction, CaptureScopeKind::Compaction)
            | (CallKind::Compaction, CaptureScopeKind::CompactionFallback)
            | (CallKind::OneOff, CaptureScopeKind::Title)
    )
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
    use gents_protocol::message::{
        AssistantContent, Message, ToolCall, ToolFunction, ToolResultContent, UserContent,
    };
    use serde_json::json;

    use super::*;

    fn context() -> RenderedRequestContext {
        RenderedRequestContext {
            request_doc_id: "doc-1".to_string(),
            request_commit_cid: "bafy-request-commit".to_string(),
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

    /// Absent tools and tool choice remain explicit typed views of the captured
    /// transport body.
    #[test]
    fn empty_tools_and_tool_choice_remain_explicit() {
        let rendered = build(
            0,
            0,
            empty_trace(),
            RenderedRequestComponents::from_provider_body(
                json!({"model": "test-model", "tools": [], "messages": []}),
                RenderedRequestSource::OpenAiChatCompletions,
            ),
        );

        assert_eq!(rendered.tools_json, json!([]));
        assert_eq!(rendered.tool_choice_json, Value::Null);
        assert_eq!(rendered.sampling_json["temperature"], Value::Null);
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
    fn rendered_completion_request_retains_transport_views() {
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
    }

    /// The payload survives capture as the durable source of truth.
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

    /// Outside an admission scope (the one-shot situation) there is no join,
    /// and its absence is an absent key — never a null or a placeholder.
    #[test]
    fn provenance_without_an_admission_scope_carries_no_join() {
        let rendered = build(0, 0, empty_trace(), components());

        assert!(rendered.provenance_json.get("admission").is_none());
        assert_eq!(
            rendered.provenance_json["manifest_version"],
            PROVENANCE_MANIFEST_VERSION
        );
    }

    /// The kind guard: a join is stamped only when the admitted call's kind
    /// legitimately produces the capture's loop. A wrong join would be worse
    /// than none.
    #[test]
    fn admission_kinds_map_to_their_capture_scopes() {
        use crate::admission::CallKind;

        let cases = [
            (CallKind::Inference, CaptureScopeKind::Inference, true),
            (CallKind::Inference, CaptureScopeKind::Compaction, false),
            (CallKind::Compaction, CaptureScopeKind::Compaction, true),
            (
                CallKind::Compaction,
                CaptureScopeKind::CompactionFallback,
                true,
            ),
            (CallKind::Compaction, CaptureScopeKind::Inference, false),
            (CallKind::OneOff, CaptureScopeKind::Title, true),
            (CallKind::OneOff, CaptureScopeKind::OneShot, false),
            (CallKind::Inference, CaptureScopeKind::OneShot, false),
            (CallKind::Scheduled, CaptureScopeKind::Inference, false),
        ];
        for (call_kind, scope_kind, expected) in cases {
            assert_eq!(
                admission_kind_matches_scope(call_kind, scope_kind),
                expected,
                "{call_kind:?} vs {scope_kind:?}"
            );
        }
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
            caused_by_parent_request_doc_id: None,
            caused_by_parent_tool_call_id: None,
            caused_by_parent_tool_call_doc_id: None,
            caused_by_trigger_id: None,
            caused_by_trigger_kind: None,
            caused_by_source_doc_id: None,
            caused_by_correlation: None,
            caused_by_trigger_context: None,
            workspace_id: None,
            workspace_authority: None,
            workspace_owner_deployment_id: None,
            workspace_seal_hash: None,
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
