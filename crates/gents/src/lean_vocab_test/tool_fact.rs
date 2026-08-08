use serde::Deserialize;

/// Generated exact durable tool-call/result/approval fact witness (#1073).
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanToolFactCase {
    pub(crate) name: String,
    pub(crate) operation: String,
    pub(crate) disposition: String,
    pub(crate) visible_logical_twins: usize,
    pub(crate) full_output: bool,
    pub(crate) call_doc_id: u64,
    pub(crate) call_cid: u64,
    pub(crate) call_signer_did: u64,
    pub(crate) result_doc_id: u64,
    pub(crate) result_cid: u64,
    pub(crate) result_signer_did: u64,
    pub(crate) approval_doc_id: Option<u64>,
    pub(crate) approval_cid: Option<u64>,
    pub(crate) approval_signer_did: Option<u64>,
    pub(crate) result_durable: bool,
    pub(crate) approval_durable: bool,
    pub(crate) result_pins_exact_call: bool,
    pub(crate) approval_pins_exact_call: bool,
    pub(crate) exact_projection: bool,
    pub(crate) immutable_noop: bool,
}
