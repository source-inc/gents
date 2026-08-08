use serde::Deserialize;

/// One exact source-class snapshot in the bounded resolved-config checkpoint.
#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub(crate) struct LeanRenderedConfigSourceRef {
    pub(crate) source_class: String,
    pub(crate) logical_id: Option<u64>,
    pub(crate) doc_id: u64,
    pub(crate) composite_commit_cid: u64,
    pub(crate) signer_did: u64,
}

/// One capture delivery evaluated by `RenderedCapture.capture`.
///
/// Every expectation below is computed by the Lean model
/// (`Proofs/Conformance/ContractCases/RenderedCapture.lean`), and
/// `RenderedCapture.Scenario.trace_realizes` proves each row is reachable by
/// legal `Step`s from an `assembled` start. Reproducing these rows therefore
/// inherits `sent_implies_durably_captured`, `sent_requires_a_capture_step`,
/// and `capture_failure_blocks_send`.
///
/// `request`, `prior_binding`, and `durable_after` carry the model's *opaque*
/// provider-body identity. The adjacent config fields carry the direct exact
/// source bundle that is also part of canonical-request equality.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LeanRenderedCaptureCase {
    pub(crate) name: String,
    pub(crate) agent_did: u64,
    pub(crate) session_id: u64,
    pub(crate) request_id: u64,
    pub(crate) turn_index: usize,
    pub(crate) attempt: u32,
    pub(crate) request: u64,
    pub(crate) config_scope: String,
    pub(crate) config_required: bool,
    pub(crate) config_present: bool,
    pub(crate) config_sources: Vec<LeanRenderedConfigSourceRef>,
    pub(crate) config_complete: bool,
    pub(crate) config_admitted: bool,
    pub(crate) prior_binding: Option<u64>,
    pub(crate) prior_config_present: bool,
    pub(crate) prior_config_sources: Vec<LeanRenderedConfigSourceRef>,
    pub(crate) capture_outcome: String,
    pub(crate) capture_durable: bool,
    pub(crate) post_stage: String,
    pub(crate) send_permitted: bool,
    pub(crate) provider_requests_observed: usize,
    pub(crate) durable_after: Option<u64>,
    pub(crate) durable_config_present: bool,
    pub(crate) durable_config_sources: Vec<LeanRenderedConfigSourceRef>,
    pub(crate) final_stage: String,
}

/// Two capture keys and whether the model calls them the same fact.
///
/// `same_fact` is `decide (left = right)` over the five-component
/// `RenderedCapture.CaptureKey` tuple, so any component production drops from
/// the key silently merges two provider attempts into one row.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LeanRenderedCaptureKeyCase {
    pub(crate) name: String,
    pub(crate) left_agent_did: u64,
    pub(crate) left_session_id: u64,
    pub(crate) left_request_id: u64,
    pub(crate) left_turn_index: usize,
    pub(crate) left_attempt: u32,
    pub(crate) right_agent_did: u64,
    pub(crate) right_session_id: u64,
    pub(crate) right_request_id: u64,
    pub(crate) right_turn_index: usize,
    pub(crate) right_attempt: u32,
    pub(crate) same_fact: bool,
}
