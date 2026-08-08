//! Executable signed-ingest decision shared by production and Lean-generated
//! conformance cases.

/// Evidence available at the atomic request-claim boundary.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestIngestEvidence<I> {
    pub source_signature_valid: bool,
    pub source_signer_did: I,
    pub expected_source_signer_did: I,
    pub source_claimable: bool,
    pub logical_match_count: usize,
    pub source_doc_id: I,
    pub observed_doc_id: I,
    pub source_head_count: usize,
    pub observed_source_cid: I,
    pub source_cid: I,
    pub claim_signature_valid: bool,
    pub claim_signer_did: I,
    pub target_agent_did: I,
    pub claim_parent_cid: Option<I>,
    pub claim_payload_preserved: bool,
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestIngestOutcome {
    SourceRejected,
    ClaimRejected,
    Admitted,
}

/// Evaluate the exact predicate modeled by `Proofs.RequestIngest`.
#[doc(hidden)]
pub fn evaluate_request_ingest<I: PartialEq>(
    evidence: &RequestIngestEvidence<I>,
) -> RequestIngestOutcome {
    let source_admitted = evidence.source_signature_valid
        && evidence.source_signer_did == evidence.expected_source_signer_did
        && evidence.source_claimable
        && evidence.logical_match_count == 1
        && evidence.observed_doc_id == evidence.source_doc_id
        && evidence.source_head_count == 1
        && evidence.observed_source_cid == evidence.source_cid;
    if !source_admitted {
        return RequestIngestOutcome::SourceRejected;
    }

    if evidence.claim_signature_valid
        && evidence.claim_signer_did == evidence.target_agent_did
        && evidence.claim_parent_cid.as_ref() == Some(&evidence.source_cid)
        && evidence.claim_payload_preserved
    {
        RequestIngestOutcome::Admitted
    } else {
        RequestIngestOutcome::ClaimRejected
    }
}
