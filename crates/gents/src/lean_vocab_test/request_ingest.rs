use serde::Deserialize;

/// One provenance decision computed by `Proofs/RequestIngest.lean`.
///
/// The numeric identifiers are opaque model values. Equality is the only
/// meaning they carry: DIDs identify signers, CIDs identify immutable source
/// versions, and payload values detect mutation across the agent claim.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LeanRequestIngestCase {
    pub(crate) name: String,
    pub(crate) origin: String,
    pub(crate) requester_did: u64,
    pub(crate) source_author_did: u64,
    pub(crate) target_agent_did: u64,
    pub(crate) source_signer_did: u64,
    pub(crate) expected_source_signer_did: u64,
    pub(crate) source_signature_valid: bool,
    pub(crate) source_claimable: bool,
    pub(crate) logical_match_count: usize,
    pub(crate) source_doc_id: u64,
    pub(crate) observed_doc_id: u64,
    pub(crate) source_head_count: usize,
    pub(crate) observed_source_cid: u64,
    pub(crate) source_cid: u64,
    pub(crate) source_payload: u64,
    pub(crate) source_admitted: bool,
    pub(crate) claim_signer_did: u64,
    pub(crate) claim_signature_valid: bool,
    pub(crate) claim_parent_cid: u64,
    pub(crate) claim_payload: u64,
    pub(crate) outcome: String,
}
