use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanDescendantGraphCase {
    pub(crate) name: String,
    pub(crate) root_request_id: usize,
    pub(crate) parent_request_id: usize,
    pub(crate) child_request_id: usize,
    pub(crate) await_mode: String,
    pub(crate) materialization: String,
    pub(crate) lifecycle: String,
    pub(crate) direct: bool,
    pub(crate) visible: bool,
    pub(crate) readable: bool,
    pub(crate) retryable: bool,
    pub(crate) listed_by_default: bool,
    pub(crate) controllable: bool,
    pub(crate) cursor_anchor_survives_terminal: bool,
    pub(crate) caller_session: String,
    pub(crate) caller_agent: String,
    pub(crate) caller_requester: Option<String>,
    pub(crate) session_authorized: bool,
    pub(crate) session_controllable: bool,
}
