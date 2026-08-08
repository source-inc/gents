use serde::Deserialize;

#[derive(Debug, Clone)]
pub(super) struct DedupPlan {
    pub(super) is_earliest: bool,
    pub(super) blocking_request_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RequestStatusTransition {
    Updated,
    AlreadyTarget,
    ConflictingTerminal(RequestViewRow),
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct DedupRow {
    #[serde(rename = "_docID")]
    pub(super) doc_id: String,
    pub(super) request_id: String,
    pub(super) status: String,
    pub(super) lifecycle_state: Option<String>,
    #[allow(dead_code)]
    pub(super) created_at: String,
}

impl DedupRow {
    pub(super) fn is_pending(&self) -> bool {
        self.status == "pending" && self.lifecycle_state.as_deref() == Some("pending")
    }

    pub(super) fn is_active_non_pending(&self) -> bool {
        !self.is_pending()
    }
}

#[derive(Deserialize)]
pub(super) struct StatusRow {
    pub(super) status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(super) struct RequestViewRow {
    pub(super) status: String,
    pub(super) lifecycle_state: Option<String>,
    #[allow(dead_code)]
    pub(super) backend_id: Option<String>,
    #[allow(dead_code)]
    pub(super) execution_origin: Option<String>,
}
