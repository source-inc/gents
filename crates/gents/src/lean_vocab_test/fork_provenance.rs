use serde::Deserialize;

#[derive(Debug, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanForkProvenanceCase {
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) disposition: String,
    pub(crate) visible_logical_twins: usize,
    pub(crate) source_authoritative: bool,
    pub(crate) source_session_id: u64,
    pub(crate) child_session_id: u64,
    pub(crate) source_doc_id: u64,
    pub(crate) source_cid: u64,
    pub(crate) source_signer_did: u64,
    pub(crate) child_doc_id: u64,
    pub(crate) child_cid: u64,
    pub(crate) child_signer_did: u64,
    pub(crate) child_call_required: bool,
    pub(crate) child_call_satisfied: bool,
    pub(crate) exact_source_pinned: bool,
    pub(crate) immutable_noop: bool,
}
