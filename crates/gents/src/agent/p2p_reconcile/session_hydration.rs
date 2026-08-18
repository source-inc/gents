//! Pure admission and selection core for `SessionHydrationRequest` (#1142).
//!
//! The background sweep and P2P delivery adapter are intentionally separate:
//! this module decides whether a request is authorized and returns the exact
//! tenant/session-scoped document set. The delivery adapter must consume this
//! set through DefraDB's existing bounded doc-pusher once that primitive is
//! exposed through the embedded node API.

use std::collections::{BTreeMap, BTreeSet};

pub const HYDRATION_COLLECTIONS: &[&str] = &[
    "AgentRequest",
    "AgentResponse",
    "AgentMessage",
    "AgentToolCall",
    "AgentToolResult",
    "AgentToolApproval",
    "CompactionEntry",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HydrationRequest {
    pub request_key: String,
    pub peer_id: String,
    pub requester_did: String,
    pub agent_did: String,
    pub session_id: String,
}

impl HydrationRequest {
    /// Decode the schema's `{peer_id}:{session_id}` key and reject a key whose
    /// session suffix does not match the immutable `session_id` column.
    pub fn from_row(
        request_key: String,
        requester_did: String,
        agent_did: String,
        session_id: String,
    ) -> Result<Self, &'static str> {
        let Some((peer_id, key_session_id)) = request_key.split_once(':') else {
            return Err("request_key must be {peer_id}:{session_id}");
        };
        if peer_id.is_empty() || key_session_id != session_id {
            return Err("request_key does not match peer/session columns");
        }
        let peer_id = peer_id.to_string();
        Ok(Self {
            request_key,
            peer_id,
            requester_did,
            agent_did,
            session_id,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SessionOwner {
    pub session_id: String,
    pub requester_did: String,
    pub agent_did: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct HydrationDocument {
    pub collection: String,
    pub doc_id: String,
    pub requester_did: String,
    pub agent_did: String,
    pub session_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HydrationCatalog {
    pub paired_peer_ids: BTreeSet<String>,
    pub active_member_dids: BTreeSet<String>,
    pub sessions: BTreeSet<SessionOwner>,
    pub documents: BTreeSet<HydrationDocument>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HydrationVerdict {
    Admit(BTreeSet<HydrationDocument>),
    Reject(&'static str),
}

pub fn decide_hydration(
    request: &HydrationRequest,
    catalog: &HydrationCatalog,
) -> HydrationVerdict {
    if !catalog.paired_peer_ids.contains(&request.peer_id) {
        return HydrationVerdict::Reject("peer is not paired");
    }
    if !catalog.active_member_dids.contains(&request.requester_did) {
        return HydrationVerdict::Reject("requester membership is not active");
    }
    let owner = SessionOwner {
        session_id: request.session_id.clone(),
        requester_did: request.requester_did.clone(),
        agent_did: request.agent_did.clone(),
    };
    if !catalog.sessions.contains(&owner) {
        return HydrationVerdict::Reject("session ownership does not match request");
    }

    HydrationVerdict::Admit(
        catalog
            .documents
            .iter()
            .filter(|doc| {
                HYDRATION_COLLECTIONS.contains(&doc.collection.as_str())
                    && doc.requester_did == request.requester_did
                    && doc.agent_did == request.agent_did
                    && doc.session_id == request.session_id
            })
            .cloned()
            .collect(),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HydrationOutcome {
    Served,
    Rejected,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HydrationState {
    pub delivered: BTreeSet<HydrationDocument>,
    pub terminals: BTreeMap<String, (HydrationOutcome, usize)>,
    /// Opaque PairingReconcile-owned state, carried to fence non-interference.
    pub pairing_state: BTreeSet<String>,
}

pub fn apply_hydration_step(
    request: &HydrationRequest,
    catalog: &HydrationCatalog,
    state: &HydrationState,
) -> HydrationState {
    if state.terminals.contains_key(&request.request_key) {
        return state.clone();
    }

    let mut next = state.clone();
    match decide_hydration(request, catalog) {
        HydrationVerdict::Admit(documents) => {
            let count = documents.len();
            next.delivered.extend(documents);
            next.terminals.insert(
                request.request_key.clone(),
                (HydrationOutcome::Served, count),
            );
        }
        HydrationVerdict::Reject(_) => {
            next.terminals
                .insert(request.request_key.clone(), (HydrationOutcome::Rejected, 0));
        }
    }
    next
}
