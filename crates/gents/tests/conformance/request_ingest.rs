//! Conformance fence for the abstract signed-ingest provenance decision.
//!
//! The generated rows drive the same executable predicate used after DefraDB
//! signature retrieval in the production atomic-claim path.

use crate::lean_vocab_test::{lean_request_ingest_cases, LeanRequestIngestCase};
use gents::lifecycle::ingest_contract::{
    evaluate_request_ingest, RequestIngestEvidence, RequestIngestOutcome,
};

fn production_evidence(case: &LeanRequestIngestCase) -> RequestIngestEvidence<u64> {
    RequestIngestEvidence {
        source_signature_valid: case.source_signature_valid,
        source_signer_did: case.source_signer_did,
        expected_source_signer_did: case.expected_source_signer_did,
        source_claimable: case.source_claimable,
        logical_match_count: case.logical_match_count,
        source_doc_id: case.source_doc_id,
        observed_doc_id: case.observed_doc_id,
        source_head_count: case.source_head_count,
        observed_source_cid: case.observed_source_cid,
        source_cid: case.source_cid,
        claim_signature_valid: case.claim_signature_valid,
        claim_signer_did: case.claim_signer_did,
        target_agent_did: case.target_agent_did,
        claim_parent_cid: Some(case.claim_parent_cid),
        claim_payload_preserved: case.claim_payload == case.source_payload,
    }
}

fn outcome_name(outcome: RequestIngestOutcome) -> &'static str {
    match outcome {
        RequestIngestOutcome::SourceRejected => "sourceRejected",
        RequestIngestOutcome::ClaimRejected => "claimRejected",
        RequestIngestOutcome::Admitted => "admitted",
    }
}

#[test]
fn generated_request_ingest_cases_fence_provenance_invariants() {
    let cases = lean_request_ingest_cases();
    assert_eq!(cases.len(), 15, "the signed-ingest decision table drifted");

    for case in cases {
        let outcome = evaluate_request_ingest(&production_evidence(case));
        assert!(
            matches!(case.origin.as_str(), "external" | "internal"),
            "{} emitted an unknown origin",
            case.name
        );
        assert_eq!(
            outcome != RequestIngestOutcome::SourceRejected,
            case.source_admitted,
            "{} drifted from Lean source admission",
            case.name
        );
        assert_eq!(
            outcome_name(outcome),
            case.outcome,
            "{} drifted from the Lean ingest outcome",
            case.name
        );
    }
}

#[test]
fn generated_cases_reject_replay_restart_and_logical_duplicates() {
    let cases = lean_request_ingest_cases();
    for name in [
        "replayed_or_restarted_claim",
        "duplicate_logical_request_documents",
        "selected_request_document_mismatch",
    ] {
        let case = cases
            .iter()
            .find(|case| case.name == name)
            .unwrap_or_else(|| panic!("Lean must emit {name}"));
        assert!(!case.source_admitted, "{name} must fail source admission");
        assert_eq!(case.outcome, "sourceRejected");
    }
}

#[test]
fn generated_internal_request_keeps_requester_distinct_from_author() {
    let case = lean_request_ingest_cases()
        .iter()
        .find(|case| case.name == "valid_internal_request_with_distinct_requester")
        .expect("Lean must emit the internal attribution witness");

    assert_eq!(case.origin, "internal");
    assert_ne!(case.requester_did, case.source_author_did);
    assert_eq!(case.source_signer_did, case.source_author_did);
    assert_eq!(case.expected_source_signer_did, case.source_author_did);
    assert_eq!(case.claim_signer_did, case.target_agent_did);
    assert!(case.source_admitted);
    assert_eq!(case.outcome, "admitted");
}
