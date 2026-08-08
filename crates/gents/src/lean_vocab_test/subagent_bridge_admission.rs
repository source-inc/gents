use serde::Deserialize;

/// One bridge-admission decision emitted by
/// `Proofs/SubagentBridgeAdmission.lean`.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LeanSubagentBridgeAdmissionCase {
    pub(crate) name: String,
    pub(crate) bridge_signature_valid: bool,
    pub(crate) bridge_signer_did: u64,
    pub(crate) bridge_author_did: u64,
    pub(crate) admitted_parent_did: u64,
    pub(crate) bridge_head_count: usize,
    pub(crate) observed_bridge_cid: u64,
    pub(crate) current_bridge_cid: u64,
    pub(crate) parent_request_matches: bool,
    pub(crate) parent_tool_call_matches: bool,
    pub(crate) child_request_matches: bool,
    pub(crate) admitted: bool,
    pub(crate) outcome: String,
}
