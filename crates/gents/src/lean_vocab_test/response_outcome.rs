use serde::Deserialize;

/// Generated immutable response-outcome witness (#1075).
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanResponseOutcomeCase {
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) has_final_message: bool,
    pub(crate) final_message_role: Option<String>,
    pub(crate) request_doc_id: usize,
    pub(crate) request_cid: usize,
    pub(crate) final_message_doc_id: Option<usize>,
    pub(crate) final_message_cid: Option<usize>,
    pub(crate) final_message_signer_did: Option<usize>,
    pub(crate) visible_sibling_count: usize,
    pub(crate) publish_outcome: String,
    pub(crate) resulting_fact_count: usize,
}

/// Generated persistence-order witness for message -> outcome -> request -> live.
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanResponsePersistenceCutCase {
    pub(crate) name: String,
    pub(crate) pre_cut: String,
    pub(crate) post_cut: String,
    pub(crate) request_terminal: bool,
    pub(crate) live_stage: String,
    pub(crate) outcome_count: usize,
}

/// Generated crash-cut witness for claim ancestry -> outcome -> request.
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanResponseRecoveryCutCase {
    pub(crate) name: String,
    pub(crate) response_present: bool,
    pub(crate) source_cid: usize,
    pub(crate) claim_cid: usize,
    pub(crate) claim_parent_cid: usize,
    pub(crate) provenance_reconstructed: bool,
    pub(crate) publish_outcome: String,
    pub(crate) terminalized_at_source: String,
    pub(crate) request_terminal: bool,
    pub(crate) outcome_count: usize,
}
