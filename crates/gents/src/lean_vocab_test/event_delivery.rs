use super::*;

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub(crate) struct LeanEventDeliveryWorld {
    pub(crate) persistent_set: Vec<String>,
    pub(crate) subscription_queue: Vec<String>,
    pub(crate) processed_set: Vec<String>,
    pub(crate) handled: Vec<String>,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum LeanEventDeliveryAction {
    Persist { doc: String },
    Depersist { doc: String },
    Enqueue { doc: String },
    Drop { doc: String },
    DeliverFromQueue { doc: String },
    RescanTick,
    Handle { doc: String },
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LeanEventDeliveryTransitionCase {
    pub(crate) name: String,
    pub(crate) pre: LeanEventDeliveryWorld,
    pub(crate) action: LeanEventDeliveryAction,
    pub(crate) post: LeanEventDeliveryWorld,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LeanEventDeliverySourceInstance {
    pub(crate) name: String,
    pub(crate) dedupe_policy: String,
    pub(crate) rescan_bounded_by: u64,
    pub(crate) deviation: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LeanEventDeliveryConvergenceTrace {
    pub(crate) name: String,
    pub(crate) instance_name: String,
    pub(crate) initial_world: LeanEventDeliveryWorld,
    pub(crate) actions: Vec<LeanEventDeliveryAction>,
    pub(crate) final_world: LeanEventDeliveryWorld,
    pub(crate) status: String,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LeanDurableEventAdmissionCase {
    pub(crate) name: String,
    pub(crate) operation: String,
    pub(crate) disposition: String,
    pub(crate) activation_twins: usize,
    pub(crate) delivery_twins: usize,
    pub(crate) baseline_contains_source: bool,
    pub(crate) trigger_cid: u64,
    pub(crate) source_cid: u64,
    pub(crate) durable_activations: usize,
    pub(crate) durable_deliveries: usize,
}
