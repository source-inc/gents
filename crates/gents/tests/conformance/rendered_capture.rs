//! Conformance fence for persist-before-send at the provider boundary (#840).
//!
//! The model is `Proofs/RenderedCapture.lean`. It proves three things this
//! repository is about to depend on:
//!
//! * `sent_requires_a_capture_step` — no provider send is reachable from an
//!   assembled request without an intervening successful capture of the same
//!   `(capture key, canonical request)`;
//! * `capture_key_determines_request` — one capture key binds at most one
//!   canonical request, for the life of the store;
//! * `capture_failure_blocks_send` — a key already bound to a conflicting
//!   request makes `sent` unreachable, permanently.
//! * `reconciled_document_send_implies_exact_config_provenance` — a reconciled
//!   document-runtime send carries required signed exact refs plus canonical
//!   optional-tool/skill evidence; static/one-shot requests may omit it.
//!
//! The *ordering* half is fenced against the real owned loop in
//! `agent::loop_stream::tests::generated_rendered_capture_cases_fence_persist_before_send`,
//! which drives every generated row through `run_loop_stream` and asserts the
//! provider observed exactly the number of requests the modeled trace permits.
//!
//! This file fences the *identity* half: that the capture key is the
//! five-component tuple the model quantifies over, and that every one of those
//! components is actually carried on the production capture DTO. The model's
//! `requestId` component names the provider-call *scope* inside a request, and
//! production encodes it as the injective JSON pair
//! `[request_doc_id, capture_scope]`; one request runs several completion loops and
//! each starts its turn and attempt counters at zero, so the request document id
//! alone does not identify a provider attempt. A component
//! dropped here does not fail loudly at runtime — it silently merges two
//! provider attempts into one durable fact, which is exactly the failure
//! `capture_key_determines_request` is supposed to make impossible.
//!
//! Two scope notes, both declared as emitted boundaries rather than assumed:
//!
//! * `boundary.rendered-capture.assembled-request-artifact` — **closed in
//!   production, still open in Lean.** The model's `CanonicalRequest` is
//!   opaque, so it is agnostic about which artifact the implementation binds to
//!   the key; the earlier note recorded that production bound the *assembled*
//!   request, which the ChatGPT-Codex and xAI Grok transports then rewrote.
//!   Production now captures at the transport seam
//!   (`rendered_request::transport::RenderedRequestCapturingHttpClient`), so the
//!   bound artifact is the body the provider received. The order property is
//!   unchanged and strictly better placed: capture and send are now the same
//!   function call, in that order.
//! * `boundary.rendered-capture.key-encoding-injectivity` — the model's key is
//!   a tuple with componentwise equality; the durable column is a string, and
//!   the model does not prove the encoder is injective on the tuple. Lean still
//!   does not, but the Rust half is now fenced below: every generated row
//!   asserts the derived `capture_key` string agrees with tuple equality, and
//!   `rendered_request::tests::capture_key_does_not_collide_across_component_boundaries`
//!   covers the delimiter-collision shapes the generated rows do not reach.

use gents::rendered_request::{
    capture_key as derive_capture_key, AssemblyBuildPath, AssemblyTrace, ProvenanceManifest,
    RenderedCompletionRequest, RenderedRequestSource, CAPTURE_VERSION,
};
use gents::{DocumentVersionRef, RequestExecutionProvenance, SignedDocumentVersionRef};
use serde_json::{json, Value};

use crate::lean_vocab_test::{lean_rendered_capture_cases, lean_rendered_capture_key_cases};

/// The model's ids are `Nat` (`boundary.model.nat-typed-ids-time`); production
/// carries strings. The mapping is injective, so tuple equality on either side
/// means the same thing.
fn agent_did(id: u64) -> String {
    format!("did:key:z6Mk-agent-{id}")
}

fn session_id(id: u64) -> String {
    format!("session-{id}")
}

fn request_doc_id(id: u64) -> String {
    format!("bae-request-doc-{id}")
}

/// Build the production capture DTO for one modeled attempt.
///
/// This is a struct literal on purpose: if a future change drops
/// `turn_index`, `attempt`, `agent_did`, `session_id`, or `request_doc_id` from
/// `RenderedCompletionRequest`, this file stops compiling instead of quietly
/// collapsing two capture keys.
fn rendered(
    agent: u64,
    session: u64,
    request: u64,
    turn_index: usize,
    attempt: u32,
    request_json: Value,
) -> RenderedCompletionRequest {
    rendered_in_scope(
        agent,
        session,
        request,
        CAPTURE_SCOPE,
        turn_index,
        attempt,
        request_json,
    )
}

/// The scope every generated row uses. The Lean rows vary `requestId`, which
/// production splits into `(request_doc_id, capture_scope)`; holding the scope fixed
/// keeps each row varying exactly one modeled component.
const CAPTURE_SCOPE: &str = "inference.1";

fn rendered_in_scope(
    agent: u64,
    session: u64,
    request: u64,
    capture_scope: &str,
    turn_index: usize,
    attempt: u32,
    request_json: Value,
) -> RenderedCompletionRequest {
    let agent_did = agent_did(agent);
    let session_id = session_id(session);
    let request_doc_id = request_doc_id(request);
    let source_version = DocumentVersionRef {
        doc_id: request_doc_id.clone(),
        composite_commit_cid: format!("bafy-source-{request}"),
    };
    let claim_version = DocumentVersionRef {
        doc_id: request_doc_id.clone(),
        composite_commit_cid: format!("bafy-claim-{request}"),
    };
    let request_provenance = RequestExecutionProvenance {
        source: SignedDocumentVersionRef {
            version: source_version.clone(),
            signer_did: "did:key:z6Mk-source".to_string(),
        },
        claim: SignedDocumentVersionRef {
            version: claim_version.clone(),
            signer_did: agent_did.clone(),
        },
    };
    let assembly_trace =
        AssemblyTrace::from_effective_messages(AssemblyBuildPath::Budgeted, Vec::new());

    RenderedCompletionRequest {
        capture_key: derive_capture_key(
            &agent_did,
            &session_id,
            &request_doc_id,
            capture_scope,
            turn_index,
            attempt,
        )
        .expect("capture key"),
        capture_version: CAPTURE_VERSION,
        request_doc_id,
        request_source_commit_cid: source_version.composite_commit_cid.clone(),
        request_source_signer_did: "did:key:z6Mk-source".to_string(),
        request_claim_commit_cid: claim_version.composite_commit_cid.clone(),
        request_claim_signer_did: agent_did.clone(),
        inference_call_doc_id: String::new(),
        inference_call_composite_commit_cid: String::new(),
        inference_call_signer_did: String::new(),
        request_id: format!("logical-request-{request}"),
        capture_scope: capture_scope.to_string(),
        turn_index,
        attempt,
        agent_did,
        requester_did: "did:key:z6Mk-requester".to_string(),
        behavior_id: "behavior-1".to_string(),
        session_id,
        model_name: "test-model".to_string(),
        source: RenderedRequestSource::OpenAiChatCompletions,
        request_json,
        messages_json: json!([]),
        tools_json: json!([]),
        tool_choice_json: Value::Null,
        sampling_json: Value::Null,
        prompt_hash: String::new(),
        tools_hash: String::new(),
        provenance_json: serde_json::to_value(
            ProvenanceManifest::captured_only_with_request_provenance(
                capture_scope.to_string(),
                None,
                Some(request_provenance),
                assembly_trace.clone(),
            ),
        )
        .expect("provenance manifest"),
        assembly_trace,
    }
}

/// The five components `RenderedCapture.CaptureKey` is made of, projected off
/// the production DTO. PR2 derives the durable `capture_key` column from
/// exactly this tuple.
fn capture_key(
    rendered: &RenderedCompletionRequest,
) -> (String, String, (String, String), usize, u32) {
    (
        rendered.agent_did.clone(),
        rendered.session_id.clone(),
        (
            rendered.request_doc_id.clone(),
            rendered.capture_scope.clone(),
        ),
        rendered.turn_index,
        rendered.attempt,
    )
}

fn request_provenance(rendered: &RenderedCompletionRequest) -> (String, String, String, String) {
    (
        rendered.request_source_commit_cid.clone(),
        rendered.request_source_signer_did.clone(),
        rendered.request_claim_commit_cid.clone(),
        rendered.request_claim_signer_did.clone(),
    )
}

#[test]
fn generated_rendered_capture_key_cases_pin_the_capture_key_tuple() {
    let cases = lean_rendered_capture_key_cases();
    assert!(
        !cases.is_empty(),
        "Lean emitted no rendered-capture key cases"
    );

    for case in cases {
        let left = rendered(
            case.left_agent_did,
            case.left_session_id,
            case.left_request_id,
            case.left_turn_index,
            case.left_attempt,
            json!({"model": "test-model"}),
        );
        let right = rendered(
            case.right_agent_did,
            case.right_session_id,
            case.right_request_id,
            case.right_turn_index,
            case.right_attempt,
            json!({"model": "test-model"}),
        );

        assert_eq!(
            capture_key(&left) == capture_key(&right),
            case.same_fact,
            "{}: the production capture-key projection disagrees with the Lean \
             model about whether these two provider attempts are one fact",
            case.name
        );

        // `boundary.rendered-capture.key-encoding-injectivity`: the model's key
        // is a tuple with componentwise equality, the durable column is a
        // string, and Lean does not prove the encoder injective. This is the
        // Rust half of that boundary — the derived `capture_key` column must
        // agree with tuple equality on every generated row, so a delimited or
        // otherwise lossy encoding fails here rather than merging two provider
        // attempts into one durable fact.
        assert_eq!(
            left.capture_key == right.capture_key,
            case.same_fact,
            "{}: the derived capture_key column disagrees with componentwise \
             tuple equality",
            case.name
        );

        // Each generated row isolates a single component, so a projection that
        // drops that component would report `same_fact` for a distinct pair.
        let varied = [
            case.left_agent_did != case.right_agent_did,
            case.left_session_id != case.right_session_id,
            case.left_request_id != case.right_request_id,
            case.left_turn_index != case.right_turn_index,
            case.left_attempt != case.right_attempt,
        ]
        .into_iter()
        .filter(|differs| *differs)
        .count();
        assert!(
            varied <= 1,
            "{}: key rows must vary at most one component so a dropped \
             component is attributable",
            case.name
        );
        assert_eq!(
            varied == 0,
            case.same_fact,
            "{}: sameFact must be exactly componentwise equality",
            case.name
        );
    }
}

/// The Lean rows hold `requestId` opaque, so they cannot vary the scope half of
/// production's third component. This is that half: two completion loops inside
/// one request, at the same turn and attempt, must be two facts. It is the
/// concrete case — the request's first inference turn versus the compaction
/// summarizer's first call — that `capture_key_determines_request` would
/// otherwise be violated by, because the sink would be asked to bind one key to
/// two different canonical requests and would (correctly) refuse, taking the
/// agent down.
#[test]
fn the_capture_scope_separates_completion_loops_within_one_request() {
    let inference = rendered_in_scope(1, 1, 1, "inference.1", 0, 0, json!({"model": "m"}));
    let compaction = rendered_in_scope(1, 1, 1, "compaction.1", 0, 0, json!({"model": "m"}));
    let fallback = rendered_in_scope(
        1,
        1,
        1,
        "compaction_fallback.1",
        0,
        0,
        json!({"model": "m"}),
    );
    // A second summarizer run in the same request, e.g. compaction at turn 7
    // after compaction at turn 3.
    let compaction_again = rendered_in_scope(1, 1, 1, "compaction.2", 0, 0, json!({"model": "m"}));

    let keys = [&inference, &compaction, &fallback, &compaction_again];
    for (index, left) in keys.iter().enumerate() {
        for right in keys.iter().skip(index + 1) {
            assert_ne!(
                capture_key(left),
                capture_key(right),
                "scopes {} and {} must be separate facts",
                left.capture_scope,
                right.capture_scope
            );
            assert_ne!(
                left.capture_key, right.capture_key,
                "scopes {} and {} must derive different capture keys",
                left.capture_scope, right.capture_scope
            );
        }
    }
}

/// The scope rides inside the third component as a JSON pair rather than a
/// delimited string. A `"{request_doc_id}#{scope}"` encoding would let one
/// component absorb the boundary of the next and forge another scope's fact.
#[test]
fn a_request_doc_id_cannot_forge_another_scopes_key() {
    let forged = rendered_in_scope(1, 1, 1, "inference.1", 0, 0, json!({"model": "m"}));
    let honest = rendered_in_scope(1, 1, 1, "inference.1", 0, 0, json!({"model": "m"}));
    assert_eq!(forged.capture_key, honest.capture_key);

    let sneaky = derive_capture_key(
        &agent_did(1),
        &session_id(1),
        &format!("{}#compaction.1", request_doc_id(1)),
        "inference.1",
        0,
        0,
    )
    .expect("capture key");
    let target = derive_capture_key(
        &agent_did(1),
        &session_id(1),
        &request_doc_id(1),
        "compaction.1",
        0,
        0,
    )
    .expect("capture key");
    assert_ne!(sneaky, target);
}

/// `RenderedCapture.capture_rejects_request_provenance_rebinding` quantifies
/// over the complete signed source/claim chain, not just the claim snapshot the
/// runtime consumed. Keep every member on the production DTO while confirming
/// that provenance changes do not silently change the provider-attempt key.
#[test]
fn complete_signed_request_provenance_is_part_of_the_capture_fact() {
    let original = rendered(1, 1, 1, 0, 0, json!({"model": "m"}));
    let original_key = capture_key(&original);
    let original_provenance = request_provenance(&original);

    let mut variants = Vec::new();

    let mut source_cid = original.clone();
    source_cid.request_source_commit_cid = "bafy-other-source".to_string();
    variants.push(source_cid);

    let mut source_signer = original.clone();
    source_signer.request_source_signer_did = "did:key:z6Mk-other-source".to_string();
    variants.push(source_signer);

    let mut claim_cid = original.clone();
    claim_cid.request_claim_commit_cid = "bafy-other-claim".to_string();
    variants.push(claim_cid);

    let mut claim_signer = original.clone();
    claim_signer.request_claim_signer_did = "did:key:z6Mk-other-agent".to_string();
    variants.push(claim_signer);

    for variant in variants {
        assert_eq!(
            capture_key(&variant),
            original_key,
            "provenance is canonical fact data, not provider-attempt key data"
        );
        assert_ne!(
            request_provenance(&variant),
            original_provenance,
            "every signed source/claim component must survive on the DTO"
        );
    }
}

/// The emitted rows must never describe a fail-open capture. This guards the
/// oracle itself: if someone relaxes `RenderedCapture.capture` so a rejected
/// delivery still permits a send, the generated data changes and this fails
/// before any sink is written against it.
#[test]
fn generated_rendered_capture_cases_never_permit_an_uncaptured_send() {
    let cases = lean_rendered_capture_cases();
    assert!(!cases.is_empty(), "Lean emitted no rendered-capture cases");

    let mut saw_rejection = false;
    let mut saw_idempotent = false;
    let mut saw_missing_config = false;
    let mut saw_config_rebinding = false;
    let mut saw_static_without_config = false;

    const REQUIRED_CONFIG_CLASSES: [&str; 4] = [
        "principal",
        "behavior",
        "inference_backend",
        "inference_profile",
    ];

    for case in cases {
        let exact = |source: &crate::lean_vocab_test::LeanRenderedConfigSourceRef| {
            source.doc_id != 0 && source.composite_commit_cid != 0 && source.signer_did != 0
        };
        let required_complete = case.config_sources.len() >= REQUIRED_CONFIG_CLASSES.len()
            && case
                .config_sources
                .iter()
                .take(REQUIRED_CONFIG_CLASSES.len())
                .zip(REQUIRED_CONFIG_CLASSES)
                .all(|(source, expected_class)| {
                    source.source_class == expected_class
                        && source.logical_id.is_none()
                        && exact(source)
                });
        let (optional_tool_complete, skills_complete) = if required_complete {
            let mut index = REQUIRED_CONFIG_CLASSES.len();
            let optional_tool_complete = if case
                .config_sources
                .get(index)
                .is_some_and(|source| source.source_class == "tool_selection")
            {
                let tool = &case.config_sources[index];
                index += 1;
                tool.logical_id.is_none() && exact(tool)
            } else {
                true
            };
            let mut previous_skill_id = 0;
            let skills_complete = case.config_sources[index..].iter().all(|skill| {
                let Some(logical_id) = skill.logical_id else {
                    return false;
                };
                let canonical =
                    skill.source_class == "skill" && exact(skill) && previous_skill_id < logical_id;
                previous_skill_id = logical_id;
                canonical
            });
            (optional_tool_complete, skills_complete)
        } else {
            (false, false)
        };
        let computed_config_complete =
            case.config_present && required_complete && optional_tool_complete && skills_complete;
        let computed_config_required = match case.config_scope.as_str() {
            "reconciled_document_runtime" => true,
            "static_or_one_shot" => false,
            other => panic!("{}: unknown config scope {other:?}", case.name),
        };
        let computed_config_admitted = if computed_config_required {
            computed_config_complete
        } else {
            !case.config_present || computed_config_complete
        };
        assert_eq!(
            computed_config_required, case.config_required,
            "{}: config scope/requirement drifted",
            case.name
        );
        assert_eq!(
            computed_config_complete, case.config_complete,
            "{}: the generated config-completeness decision drifted from the \
             required/optional/canonical-skills exact-reference contract",
            case.name
        );
        assert_eq!(
            computed_config_admitted, case.config_admitted,
            "{}: scope-aware config admission drifted",
            case.name
        );

        assert!(
            matches!(
                case.capture_outcome.as_str(),
                "fresh" | "idempotent" | "rejected"
            ),
            "{}: unknown capture outcome {:?}",
            case.name,
            case.capture_outcome
        );
        assert_eq!(
            case.capture_durable,
            case.capture_outcome != "rejected",
            "{}: durability must follow the outcome",
            case.name
        );

        if case.send_permitted {
            assert_eq!(
                case.durable_after,
                Some(case.request),
                "{}: a permitted send must leave this key bound to this request",
                case.name
            );
            assert_eq!(case.post_stage, "durablyCaptured", "{}", case.name);
            assert_eq!(case.final_stage, "sent", "{}", case.name);
            assert_eq!(case.provider_requests_observed, 1, "{}", case.name);
            assert!(case.config_admitted, "{}: send was not admitted", case.name);
            if case.config_required {
                assert!(
                    case.config_complete,
                    "{}: a reconciled document-runtime send must carry complete exact config provenance",
                    case.name
                );
            }
            assert_eq!(
                case.durable_config_sources, case.config_sources,
                "{}: the durable fact must preserve the exact config bundle",
                case.name
            );
        } else {
            let same_canonical_fact = case.durable_after == Some(case.request)
                && case.durable_config_present == case.config_present
                && case.durable_config_sources == case.config_sources;
            assert!(
                !same_canonical_fact,
                "{}: a refused send must not claim this complete canonical fact \
                 is durable",
                case.name,
            );
            assert_eq!(case.post_stage, "assembled", "{}", case.name);
            assert_eq!(case.final_stage, "assembled", "{}", case.name);
            assert_eq!(case.provider_requests_observed, 0, "{}", case.name);
        }

        saw_rejection |= case.capture_outcome == "rejected";
        saw_idempotent |= case.capture_outcome == "idempotent";
        saw_missing_config |= case.name == "reconciled_runtime_missing_config_blocks_send";
        saw_config_rebinding |=
            case.name == "config_provenance_rebinding_is_an_integrity_violation";
        saw_static_without_config |= case.name == "static_or_one_shot_without_config_can_send";
    }

    assert!(
        saw_rejection,
        "the generated rows must include a key rebound to a different canonical \
         request; without it the fail-closed path is untested"
    );
    assert!(
        saw_idempotent,
        "the generated rows must include an identical recapture; without it \
         idempotent redelivery is untested"
    );
    assert!(
        saw_missing_config,
        "the generated rows must include missing config provenance"
    );
    assert!(
        saw_config_rebinding,
        "the generated rows must include equal bytes rebound to another config bundle"
    );
    assert!(
        saw_static_without_config,
        "the generated rows must include a static/one-shot send without config provenance"
    );
}
