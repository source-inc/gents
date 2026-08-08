use serde::Deserialize;

/// One generated witness for the exact inference-call/rendered-request
/// composition fence. Every emitted field is retained so Rust cannot silently
/// weaken the V1 -> R -> V2 -> send -> V3 contract while still parsing it.
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanInferenceRenderedCaptureCase {
    pub(crate) name: String,
    pub(crate) initial_stage: String,
    pub(crate) final_stage: String,
    pub(crate) initial_call_state: String,
    pub(crate) final_call_state: String,
    pub(crate) capture_outcome: String,
    pub(crate) running_call_doc_id: Option<u64>,
    pub(crate) running_call_cid: Option<u64>,
    pub(crate) render_doc_id: Option<u64>,
    pub(crate) render_cid: Option<u64>,
    pub(crate) current_call_cid: u64,
    pub(crate) render_durable: bool,
    pub(crate) render_pins_running: bool,
    pub(crate) call_pins_render: bool,
    pub(crate) http_requests_observed: usize,
    pub(crate) terminal_failed: bool,
    pub(crate) second_send_permitted: bool,
}
