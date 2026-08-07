use serde::Deserialize;

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
/// canonical-request identity: equal numbers mean equal canonical JSON, and
/// nothing else about them is modeled.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LeanRenderedCaptureCase {
    pub(crate) name: String,
    pub(crate) agent_did: u64,
    pub(crate) session_id: u64,
    pub(crate) request_id: u64,
    pub(crate) turn_index: usize,
    pub(crate) attempt: u32,
    pub(crate) request: u64,
    pub(crate) prior_binding: Option<u64>,
    pub(crate) capture_outcome: String,
    pub(crate) capture_durable: bool,
    pub(crate) post_stage: String,
    pub(crate) send_permitted: bool,
    pub(crate) provider_requests_observed: usize,
    pub(crate) durable_after: Option<u64>,
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

/// One `capture_scope` label vector (#1066): what the shared consumer parser
/// (`gents_protocol::rendered_request::CaptureScope`) must accept — with the
/// kind and numeric seq it must recover — or reject, never default.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LeanCaptureScopeCase {
    pub(crate) label: String,
    pub(crate) kind: String,
    pub(crate) seq: u64,
    pub(crate) valid: bool,
}

/// One ordering verdict over `(kind rank, seq, turn, attempt)`, decided by the
/// Lean comparison. `inference.10` vs `inference.2` is in the list precisely
/// so a lexical implementation cannot pass.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LeanCaptureOrderCase {
    pub(crate) name: String,
    pub(crate) left_label: String,
    pub(crate) left_turn: i64,
    pub(crate) left_attempt: i64,
    pub(crate) right_label: String,
    pub(crate) right_turn: i64,
    pub(crate) right_attempt: i64,
    pub(crate) left_before_right: bool,
}
