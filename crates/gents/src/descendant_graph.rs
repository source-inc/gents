//! Canonical, authorization-safe descendant graph projection (#836).
//!
//! The parent-authored `AgentToolCall` document is the durable edge receipt.
//! A materialized child is readable/control-eligible only after both logical
//! identifiers and immutable DefraDB document identifiers corroborate that
//! receipt. Consumers must not rebuild lineage from `AgentRequest` labels.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use anyhow::{Context, Result};
use defra_node::EmbeddedNode;
use gents_protocol::row::AgentRequestRow;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config_client::ConfigAccess;
use crate::graphql::escape_graphql_string;

pub const DEFAULT_DESCENDANT_PAGE_LIMIT: usize = 20;
pub const MAX_DESCENDANT_PAGE_LIMIT: usize = 100;
pub const MAX_DESCENDANT_DEPTH: usize = 32;
pub const AWAITING_CHILD_MATERIALIZATION: &str = "awaiting_child_materialization";
pub const PENDING_CHILD_AUTHORIZATION: &str = "pending_child_authorization";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DescendantScope {
    #[default]
    DirectChildren,
    AllDescendants,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescendantQuery {
    pub root_request_id: String,
    pub scope: DescendantScope,
    pub after: Option<String>,
    pub limit: usize,
    pub include_terminal: bool,
}

impl DescendantQuery {
    pub fn direct(root_request_id: impl Into<String>) -> Self {
        Self {
            root_request_id: root_request_id.into(),
            scope: DescendantScope::DirectChildren,
            after: None,
            limit: DEFAULT_DESCENDANT_PAGE_LIMIT,
            include_terminal: true,
        }
    }

    pub fn all(root_request_id: impl Into<String>) -> Self {
        Self {
            scope: DescendantScope::AllDescendants,
            ..Self::direct(root_request_id)
        }
    }

    fn validated_limit(&self) -> usize {
        self.limit.clamp(1, MAX_DESCENDANT_PAGE_LIMIT)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DescendantMaterializationState {
    AwaitingChild,
    AuthorizationPending,
    MaterializedLocal,
    MaterializedRemote,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DescendantAuthorizationState {
    Authorized,
    PendingMaterialization,
    RejectedPhysicalLineage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DescendantControlAuthority {
    Authorized,
    VisibilityOnly,
    PendingMaterialization,
    RejectedPhysicalLineage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescendantEdge {
    pub cursor: String,
    pub root_request_id: String,
    pub immediate_parent_request_id: String,
    pub immediate_parent_session_id: String,
    pub immediate_parent_tool_call_id: String,
    pub child_request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal_did: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub behavior_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    pub await_mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancel_policy: Option<String>,
    pub lifecycle_state: String,
    pub materialization_state: DescendantMaterializationState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_result_ref: Option<String>,
    pub transcript_cursor: u64,
    pub authorization_state: DescendantAuthorizationState,
    pub control_authority: DescendantControlAuthority,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
    pub depth: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

impl DescendantEdge {
    pub fn is_direct(&self) -> bool {
        self.depth == 1
    }

    pub fn is_terminal(&self) -> bool {
        lifecycle_is_terminal(&self.lifecycle_state)
    }

    pub fn readable(&self) -> bool {
        self.authorization_state == DescendantAuthorizationState::Authorized
            && matches!(
                self.materialization_state,
                DescendantMaterializationState::MaterializedLocal
                    | DescendantMaterializationState::MaterializedRemote
            )
    }

    pub fn controllable(&self) -> bool {
        self.control_authority == DescendantControlAuthority::Authorized
    }

    /// Only a bridge whose child has not materialized can converge through a
    /// retry. A present child with rejected physical lineage is permanent.
    pub fn retryable(&self) -> bool {
        self.authorization_state == DescendantAuthorizationState::PendingMaterialization
            && !self.is_terminal()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescendantPage {
    pub root_request_id: String,
    pub scope: DescendantScope,
    pub edges: Vec<DescendantEdge>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Clone, Copy)]
pub enum DescendantGraphAccess<'a> {
    Local(&'a EmbeddedNode),
    Config(&'a ConfigAccess),
}

impl DescendantGraphAccess<'_> {
    async fn execute(&self, query: &str) -> Result<Value> {
        match self {
            Self::Config(access) => access.execute(query).await,
            Self::Local(node) => {
                let response = node.execute(query).await;
                if response.has_errors() {
                    let errors = response
                        .errors
                        .iter()
                        .map(|error| error.message.as_str())
                        .collect::<Vec<_>>()
                        .join("; ");
                    anyhow::bail!("descendant graph GraphQL returned errors: {errors}");
                }
                Ok(serde_json::json!({
                    "data": response.data.unwrap_or(Value::Null),
                }))
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct BridgeRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    request_id: String,
    request_doc_id: Option<String>,
    session_id: Option<String>,
    agent_did: Option<String>,
    requester_did: Option<String>,
    tool_call_id: String,
    args: Option<String>,
    result: Option<String>,
    status: Option<String>,
    lifecycle_state: Option<String>,
    started_at: Option<String>,
    completed_at: Option<String>,
    await_mode: Option<String>,
    cancel_policy: Option<String>,
    child_request_id: Option<String>,
    spawn_target_did: Option<String>,
    unclaimed_deadline_at: Option<String>,
}

const BRIDGE_FIELDS: &str = r#"
    _docID
    request_id
    request_doc_id
    session_id
    agent_did
    requester_did
    tool_call_id
    args
    result
    status
    lifecycle_state
    started_at
    completed_at
    await_mode
    cancel_policy
    child_request_id
    spawn_target_did
    unclaimed_deadline_at
"#;

#[derive(Debug, Deserialize)]
struct MessageCursorRow {
    session_id: String,
    sequence: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct BridgeArgs {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    agent_did: Option<String>,
    #[serde(default)]
    behavior_id: Option<String>,
}

#[derive(Debug, Clone)]
struct ParentNode {
    row: AgentRequestRow,
    depth: usize,
}

#[derive(Debug, Clone)]
struct RootContext {
    request_id: String,
    doc_id: String,
    agent_did: Option<String>,
    requester_did: Option<String>,
}

impl RootContext {
    fn from_request(row: &AgentRequestRow) -> Self {
        Self {
            request_id: row.request_id.clone(),
            doc_id: request_doc_id(row).to_string(),
            agent_did: clean(row.agent_did.as_deref()),
            requester_did: clean(row.requester_did.as_deref()),
        }
    }
}

struct ProjectedEdge {
    edge: DescendantEdge,
    child: Option<AgentRequestRow>,
}

pub async fn resolve_descendant_graph(
    access: DescendantGraphAccess<'_>,
    query: &DescendantQuery,
) -> Result<DescendantPage> {
    let root_id = nonempty(Some(query.root_request_id.as_str()))
        .context("descendant graph root_request_id is required")?;
    let root = load_unique_request(&access, root_id)
        .await?
        .with_context(|| format!("root AgentRequest {root_id} not found"))?;
    let edges = collect_descendant_edges(&access, root, query.scope).await?;
    page_descendant_edges(query, edges)
}

async fn collect_descendant_edges(
    access: &DescendantGraphAccess<'_>,
    root: AgentRequestRow,
    scope: DescendantScope,
) -> Result<Vec<DescendantEdge>> {
    let root_context = RootContext::from_request(&root);

    let mut frontier = VecDeque::from([ParentNode {
        row: root,
        depth: 0,
    }]);
    let mut visited_parents = BTreeSet::new();
    let mut seen_children = BTreeSet::new();
    let mut edges = Vec::new();

    while !frontier.is_empty() {
        let mut level = Vec::new();
        while let Some(parent) = frontier.pop_front() {
            if parent.depth >= MAX_DESCENDANT_DEPTH
                || !visited_parents.insert(request_doc_id(&parent.row).to_string())
            {
                continue;
            }
            level.push(parent);
        }
        if level.is_empty() {
            break;
        }

        let parent_ids = level
            .iter()
            .map(|parent| parent.row.request_id.clone())
            .collect::<Vec<_>>();
        let bridges = load_bridges(&access, &parent_ids).await?;
        let child_ids = bridges
            .iter()
            .filter_map(|bridge| clean(bridge.child_request_id.as_deref()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let children = load_requests(&access, &child_ids).await?;
        let mut children_by_id = BTreeMap::<String, Vec<AgentRequestRow>>::new();
        for child in children {
            children_by_id
                .entry(child.request_id.clone())
                .or_default()
                .push(child);
        }
        let parents_by_id = level
            .iter()
            .map(|parent| (parent.row.request_id.clone(), parent))
            .collect::<BTreeMap<_, _>>();

        for bridge in bridges {
            let Some(parent) = parents_by_id.get(&bridge.request_id) else {
                continue;
            };
            let Some(child_request_id) = clean(bridge.child_request_id.as_deref()) else {
                continue;
            };
            let candidate_rows = children_by_id
                .get(&child_request_id)
                .map(Vec::as_slice)
                .unwrap_or_default();
            let Some(projected) =
                project_descendant_edge(&root_context, parent, &bridge, candidate_rows)?
            else {
                continue;
            };
            if !seen_children.insert(projected.edge.child_request_id.clone()) {
                anyhow::bail!(
                    "descendant child {child_request_id} is referenced by more than one canonical bridge"
                );
            }

            if scope != DescendantScope::DirectChildren {
                if let Some(child) = projected.child.clone() {
                    frontier.push_back(ParentNode {
                        row: child,
                        depth: projected.edge.depth,
                    });
                }
            }
            edges.push(projected.edge);
        }

        if scope == DescendantScope::DirectChildren {
            break;
        }
    }

    populate_transcript_cursors(&access, &mut edges).await?;
    edges.retain(|edge| match scope {
        DescendantScope::DirectChildren => edge.is_direct(),
        DescendantScope::AllDescendants => true,
    });
    edges.sort_by(|left, right| left.cursor.cmp(&right.cursor));
    Ok(edges)
}

fn page_descendant_edges(
    query: &DescendantQuery,
    mut edges: Vec<DescendantEdge>,
) -> Result<DescendantPage> {
    edges.sort_by(|left, right| left.cursor.cmp(&right.cursor));

    // Resolve the cursor against the stable scoped edge set before applying
    // lifecycle filters. Otherwise an edge that becomes terminal between
    // pages disappears along with the client's valid pagination anchor.
    let start = match query
        .after
        .as_deref()
        .and_then(|value| nonempty(Some(value)))
    {
        None => 0,
        Some(after) => edges
            .iter()
            .position(|edge| edge.cursor == after)
            .map(|index| index + 1)
            .with_context(|| format!("descendant cursor {after:?} is not in this graph scope"))?,
    };
    let limit = query.validated_limit();
    let eligible_edges = edges
        .into_iter()
        .skip(start)
        .filter(|edge| query.include_terminal || !edge.is_terminal())
        .collect::<Vec<_>>();
    let has_more = eligible_edges.len() > limit;
    let page_edges = eligible_edges.into_iter().take(limit).collect::<Vec<_>>();
    let next_cursor = has_more
        .then(|| page_edges.last().map(|edge| edge.cursor.clone()))
        .flatten();

    Ok(DescendantPage {
        root_request_id: query.root_request_id.clone(),
        scope: query.scope,
        edges: page_edges,
        next_cursor,
        has_more,
    })
}

/// The session is an authority boundary, not merely a label for convenient
/// lookup. Match requester absence exactly and never grant cross-turn access
/// to missing agent/session identities. Physical bridges are checked separately.
fn same_session_owner(caller: &AgentRequestRow, owner: &AgentRequestRow) -> bool {
    caller
        .session_id
        .as_deref()
        .is_some_and(|id| !id.trim().is_empty())
        && caller
            .agent_did
            .as_deref()
            .is_some_and(|did| !did.trim().is_empty())
        && caller.session_id == owner.session_id
        && caller.agent_did == owner.agent_did
        && caller.requester_did == owner.requester_did
}

async fn session_bridge_owners(
    access: &DescendantGraphAccess<'_>,
    caller: &AgentRequestRow,
) -> Result<Vec<AgentRequestRow>> {
    if !same_session_owner(caller, caller) {
        return Ok(Vec::new());
    }
    // Enumerate only requests with durable child receipts, not every message
    // or unrelated request in a long-running conversation.
    let session = escape_graphql_string(request_session_id(caller));
    let query = format!(
        r#"{{ AgentToolCall(filter: {{ session_id: {{ _eq: "{session}" }},
        child_request_id: {{ _ne: "" }} }}) {{ request_id }} }}"#
    );
    #[derive(Deserialize)]
    struct OwnerId {
        request_id: String,
    }
    let ids = load_rows::<OwnerId>(access, "AgentToolCall", &query)
        .await?
        .into_iter()
        .map(|row| row.request_id)
        .collect::<BTreeSet<_>>();
    let mut owners = Vec::new();
    // Keep query envelopes bounded without imposing a conversation-size cap.
    for page in ids.into_iter().collect::<Vec<_>>().chunks(128) {
        let rows = load_requests(access, page).await?;
        let mut seen = BTreeSet::new();
        for row in rows {
            if !seen.insert(row.request_id.clone()) {
                anyhow::bail!("ambiguous descendant owner request {}", row.request_id);
            }
            if same_session_owner(caller, &row) {
                owners.push(row);
            }
        }
    }
    Ok(owners)
}

/// Project descendants owned by this caller's conversation and exact principal
/// scope. `query.root_request_id` identifies the caller, not a fabricated parent;
/// every returned edge retains its actual owning request and physical receipt.
pub async fn resolve_session_descendant_graph(
    access: DescendantGraphAccess<'_>,
    query: &DescendantQuery,
) -> Result<DescendantPage> {
    let caller = load_unique_request(&access, &query.root_request_id)
        .await?
        .context("session descendant caller request not found")?;
    let mut edges = Vec::new();
    let mut children = BTreeSet::new();
    for owner in session_bridge_owners(&access, &caller).await? {
        for edge in collect_descendant_edges(&access, owner, query.scope).await? {
            if !children.insert(edge.child_request_id.clone()) {
                anyhow::bail!(
                    "ambiguous child {} across session descendant roots",
                    edge.child_request_id
                );
            }
            edges.push(edge);
        }
    }
    page_descendant_edges(query, edges)
}

/// Exact-handle lookup with the same authority as session enumeration. The
/// canonical resolver still enforces direct-parent control and physical joins.
pub async fn resolve_session_descendant_edge(
    access: DescendantGraphAccess<'_>,
    caller_request_id: &str,
    child_request_id: &str,
) -> Result<Option<DescendantEdge>> {
    let caller = load_unique_request(&access, caller_request_id)
        .await?
        .context("session descendant caller request not found")?;
    if child_request_id.trim().is_empty() {
        return Ok(None);
    }
    let candidates = load_requests(&access, &[child_request_id.to_owned()]).await?;
    let mut matches = Vec::new();
    for bridge in load_bridges_by_child(&access, child_request_id).await? {
        let Some(parent) = load_unique_request(&access, &bridge.request_id).await? else {
            continue;
        };
        let Some((owner, depth)) = trace_parent_to_matching_root(&access, &parent, |candidate| {
            same_session_owner(&caller, candidate)
        })
        .await?
        else {
            continue;
        };
        if let Some(projected) = project_descendant_edge(
            &RootContext::from_request(&owner),
            &ParentNode { row: parent, depth },
            &bridge,
            &candidates,
        )? {
            matches.push(projected.edge);
        }
    }
    match matches.len() {
        0 => Ok(None),
        1 => {
            populate_transcript_cursors(&access, &mut matches).await?;
            Ok(matches.pop())
        }
        _ => anyhow::bail!("ambiguous child {child_request_id} across session descendant roots"),
    }
}

fn project_descendant_edge(
    root: &RootContext,
    parent: &ParentNode,
    bridge: &BridgeRow,
    candidate_rows: &[AgentRequestRow],
) -> Result<Option<ProjectedEdge>> {
    if !bridge_corroborates_parent(parent, bridge) {
        return Ok(None);
    }
    let Some(child_request_id) = clean(bridge.child_request_id.as_deref()) else {
        return Ok(None);
    };
    let matching = candidate_rows
        .iter()
        .filter(|child| child_corroborates(parent, bridge, child))
        .collect::<Vec<_>>();
    if matching.len() > 1 {
        anyhow::bail!(
            "descendant child {child_request_id} is ambiguous across physically corroborated documents"
        );
    }
    let child = matching.first().copied().cloned();
    let rejected_physical_lineage = child.is_none() && !candidate_rows.is_empty();
    let depth = parent.depth + 1;
    let args = parse_bridge_args(bridge.args.as_deref());
    let bridge_state = lifecycle(&bridge.lifecycle_state, &bridge.status, "running");
    // The parent-authored bridge owns edge lifecycle. Child request state is
    // intentionally not substituted: continuations can make one request
    // terminal while the bridge itself is still converging.
    let lifecycle_state = if child.is_some() || bridge_state_is_terminal(&bridge_state) {
        bridge_state.clone()
    } else if rejected_physical_lineage {
        PENDING_CHILD_AUTHORIZATION.to_string()
    } else {
        AWAITING_CHILD_MATERIALIZATION.to_string()
    };
    let materialization_state = match child.as_ref() {
        Some(child) if clean(child.agent_did.as_deref()) == root.agent_did => {
            DescendantMaterializationState::MaterializedLocal
        }
        Some(_) => DescendantMaterializationState::MaterializedRemote,
        None if rejected_physical_lineage => DescendantMaterializationState::AuthorizationPending,
        None => DescendantMaterializationState::AwaitingChild,
    };
    let authorization_state = match child {
        Some(_) => DescendantAuthorizationState::Authorized,
        None if rejected_physical_lineage => DescendantAuthorizationState::RejectedPhysicalLineage,
        None => DescendantAuthorizationState::PendingMaterialization,
    };
    let direct = depth == 1;
    let parent_principal_matches_root = clean(parent.row.agent_did.as_deref()) == root.agent_did
        && (request_doc_id(&parent.row) == root.doc_id
            || clean(parent.row.requester_did.as_deref()) == root.requester_did);
    let control_authority = match authorization_state {
        DescendantAuthorizationState::Authorized if direct && parent_principal_matches_root => {
            DescendantControlAuthority::Authorized
        }
        DescendantAuthorizationState::Authorized => DescendantControlAuthority::VisibilityOnly,
        DescendantAuthorizationState::PendingMaterialization => {
            DescendantControlAuthority::PendingMaterialization
        }
        DescendantAuthorizationState::RejectedPhysicalLineage => {
            DescendantControlAuthority::RejectedPhysicalLineage
        }
    };
    let terminal_result_ref = bridge_state_is_terminal(&lifecycle_state)
        .then(|| clean(bridge.result.as_deref()))
        .flatten()
        .map(|_| format!("AgentToolCall:{}", bridge.doc_id));
    let diagnostic = match authorization_state {
        DescendantAuthorizationState::Authorized => None,
        DescendantAuthorizationState::PendingMaterialization => Some(format!(
            "bridge {} is durable; child {} has no materialized row yet{}",
            bridge.tool_call_id,
            child_request_id,
            bridge
                .unclaimed_deadline_at
                .as_deref()
                .map(|deadline| format!(" (unclaimed deadline {deadline})"))
                .unwrap_or_default()
        )),
        DescendantAuthorizationState::RejectedPhysicalLineage => Some(format!(
            "child {child_request_id} exists but does not corroborate bridge {} physical provenance",
            bridge.tool_call_id
        )),
    };
    let created_at = clean(bridge.started_at.as_deref());
    let updated_at = clean(bridge.completed_at.as_deref()).or_else(|| created_at.clone());
    let cursor = edge_cursor(
        depth,
        created_at.as_deref(),
        &bridge.request_id,
        &bridge.tool_call_id,
        &child_request_id,
    );
    let principal_did = child
        .as_ref()
        .and_then(|row| clean(row.agent_did.as_deref()))
        .or_else(|| clean(bridge.spawn_target_did.as_deref()))
        .or_else(|| clean(args.agent_did.as_deref()));
    let behavior_id = child
        .as_ref()
        .and_then(|row| clean(row.behavior_id.as_deref()))
        .or_else(|| clean(args.behavior_id.as_deref()));

    Ok(Some(ProjectedEdge {
        edge: DescendantEdge {
            cursor,
            root_request_id: root.request_id.clone(),
            immediate_parent_request_id: bridge.request_id.clone(),
            immediate_parent_session_id: request_session_id(&parent.row).to_string(),
            immediate_parent_tool_call_id: bridge.tool_call_id.clone(),
            child_request_id,
            child_session_id: child
                .as_ref()
                .map(|row| request_session_id(row).to_string()),
            principal_did: principal_did.clone(),
            behavior_id,
            deployment_id: principal_did,
            target: clean(args.name.as_deref()),
            await_mode: clean(bridge.await_mode.as_deref())
                .unwrap_or_else(|| "foreground".to_string()),
            cancel_policy: clean(bridge.cancel_policy.as_deref()),
            lifecycle_state,
            materialization_state,
            terminal_result_ref,
            transcript_cursor: 0,
            authorization_state,
            control_authority,
            diagnostic,
            depth,
            created_at,
            updated_at,
        },
        child,
    }))
}

pub async fn resolve_descendant_edge(
    access: DescendantGraphAccess<'_>,
    root_request_id: &str,
    child_request_id: &str,
) -> Result<Option<DescendantEdge>> {
    let Some(root_request_id) = nonempty(Some(root_request_id)) else {
        anyhow::bail!("descendant graph root_request_id is required");
    };
    let Some(child_request_id) = nonempty(Some(child_request_id)) else {
        return Ok(None);
    };

    let root = load_unique_request(&access, root_request_id)
        .await?
        .with_context(|| format!("root AgentRequest {root_request_id} not found"))?;
    let root_context = RootContext::from_request(&root);
    let bridges = load_bridges_by_child(&access, child_request_id).await?;
    if bridges.is_empty() {
        return Ok(None);
    }
    let candidate_rows = load_requests(&access, &[child_request_id.to_string()]).await?;
    let mut matches = Vec::new();

    for bridge in bridges {
        let parent = if bridge.request_id == root.request_id {
            root.clone()
        } else {
            let Some(parent) = load_unique_request(&access, &bridge.request_id).await? else {
                continue;
            };
            parent
        };
        let Some(parent_depth) = trace_parent_to_root(&access, &root, &parent).await? else {
            continue;
        };
        let parent = ParentNode {
            row: parent,
            depth: parent_depth,
        };
        let Some(projected) =
            project_descendant_edge(&root_context, &parent, &bridge, &candidate_rows)?
        else {
            continue;
        };
        matches.push(projected.edge);
    }

    match matches.len() {
        0 => Ok(None),
        1 => {
            populate_transcript_cursors(&access, &mut matches).await?;
            Ok(matches.pop())
        }
        count => anyhow::bail!(
            "descendant child {child_request_id} is referenced by {count} canonical bridges in root {root_request_id}"
        ),
    }
}

/// Walk only the immutable physical parent joins needed to prove that one
/// immediate parent belongs to `root`. This keeps exact-handle resolution
/// O(depth), independent of fan-out size and page count.
async fn trace_parent_to_root(
    access: &DescendantGraphAccess<'_>,
    root: &AgentRequestRow,
    candidate_parent: &AgentRequestRow,
) -> Result<Option<usize>> {
    Ok(
        trace_parent_to_matching_root(access, candidate_parent, |current| {
            request_doc_id(current) == request_doc_id(root) && current.request_id == root.request_id
        })
        .await?
        .map(|(_, depth)| depth),
    )
}

async fn trace_parent_to_matching_root(
    access: &DescendantGraphAccess<'_>,
    candidate_parent: &AgentRequestRow,
    matches: impl Fn(&AgentRequestRow) -> bool,
) -> Result<Option<(AgentRequestRow, usize)>> {
    let mut current = candidate_parent.clone();
    let mut depth = 0usize;
    let mut seen = BTreeSet::new();

    loop {
        if matches(&current) {
            return Ok(Some((current, depth)));
        }
        if depth >= MAX_DESCENDANT_DEPTH || !seen.insert(request_doc_id(&current).to_string()) {
            return Ok(None);
        }
        let (
            Some(parent_request_id),
            Some(parent_request_doc_id),
            Some(parent_tool_call_id),
            Some(parent_tool_call_doc_id),
        ) = (
            clean(current.caused_by_parent_request_id.as_deref()),
            clean(current.caused_by_parent_request_doc_id.as_deref()),
            clean(current.caused_by_parent_tool_call_id.as_deref()),
            clean(current.caused_by_parent_tool_call_doc_id.as_deref()),
        )
        else {
            return Ok(None);
        };
        let Some(parent) = load_unique_request(access, &parent_request_id).await? else {
            return Ok(None);
        };
        if request_doc_id(&parent) != parent_request_doc_id {
            return Ok(None);
        }
        let Some(parent_bridge) =
            load_unique_bridge_by_doc_id(access, &parent_tool_call_doc_id).await?
        else {
            return Ok(None);
        };
        let parent_node = ParentNode {
            row: parent.clone(),
            depth: 0,
        };
        if parent_bridge.tool_call_id != parent_tool_call_id
            || parent_bridge.request_id != parent.request_id
            || !bridge_corroborates_parent(&parent_node, &parent_bridge)
            || !child_corroborates(&parent_node, &parent_bridge, &current)
        {
            return Ok(None);
        }
        current = parent;
        depth += 1;
    }
}

/// Resolve request-only control continuations (steering/background-completion
/// wakes) back to the request that owns the descendant bridges. A subagent
/// spawn edge is never crossed because it has an immediate parent tool-call.
pub async fn resolve_descendant_root_request_id(
    access: DescendantGraphAccess<'_>,
    caller_request_id: &str,
) -> Result<String> {
    let mut current = load_unique_request(&access, caller_request_id)
        .await?
        .with_context(|| format!("caller AgentRequest {caller_request_id} not found"))?;
    let mut seen = BTreeSet::from([request_doc_id(&current).to_string()]);
    loop {
        let parent_request_id = clean(current.caused_by_parent_request_id.as_deref());
        let parent_request_doc_id = clean(current.caused_by_parent_request_doc_id.as_deref());
        let has_tool_edge = clean(current.caused_by_parent_tool_call_id.as_deref()).is_some()
            || clean(current.caused_by_parent_tool_call_doc_id.as_deref()).is_some();
        let (Some(parent_request_id), Some(parent_request_doc_id)) =
            (parent_request_id, parent_request_doc_id)
        else {
            return Ok(current.request_id);
        };
        if has_tool_edge {
            return Ok(current.request_id);
        }
        let parent = load_unique_request(&access, &parent_request_id)
            .await?
            .with_context(|| {
                format!(
                    "request-only continuation {} points to missing parent {parent_request_id}",
                    current.request_id
                )
            })?;
        if request_doc_id(&parent) != parent_request_doc_id
            || request_session_id(&parent) != request_session_id(&current)
            || clean(parent.agent_did.as_deref()) != clean(current.agent_did.as_deref())
            || clean(parent.requester_did.as_deref()) != clean(current.requester_did.as_deref())
        {
            anyhow::bail!(
                "request-only continuation {} crosses its principal/session/physical lineage boundary",
                current.request_id
            );
        }
        if !seen.insert(request_doc_id(&parent).to_string()) {
            anyhow::bail!("request-only continuation lineage contains a cycle");
        }
        current = parent;
    }
}

fn child_corroborates(parent: &ParentNode, bridge: &BridgeRow, child: &AgentRequestRow) -> bool {
    clean(child.caused_by_parent_request_id.as_deref()).as_deref()
        == Some(parent.row.request_id.as_str())
        && clean(child.caused_by_parent_request_doc_id.as_deref()).as_deref()
            == Some(request_doc_id(&parent.row))
        && clean(child.caused_by_parent_tool_call_id.as_deref()).as_deref()
            == Some(bridge.tool_call_id.as_str())
        && clean(child.caused_by_parent_tool_call_doc_id.as_deref()).as_deref()
            == Some(bridge.doc_id.as_str())
        && clean(bridge.child_request_id.as_deref()).as_deref() == Some(child.request_id.as_str())
}

fn bridge_corroborates_parent(parent: &ParentNode, bridge: &BridgeRow) -> bool {
    clean(bridge.request_doc_id.as_deref()).as_deref() == Some(request_doc_id(&parent.row))
        && clean(bridge.session_id.as_deref()).as_deref() == Some(request_session_id(&parent.row))
        && clean(bridge.agent_did.as_deref()) == clean(parent.row.agent_did.as_deref())
        && clean(bridge.requester_did.as_deref()) == clean(parent.row.requester_did.as_deref())
}

async fn load_unique_request(
    access: &DescendantGraphAccess<'_>,
    request_id: &str,
) -> Result<Option<AgentRequestRow>> {
    let rows = load_requests(access, &[request_id.to_string()]).await?;
    match rows.len() {
        0 => Ok(None),
        1 => Ok(rows.into_iter().next()),
        count => anyhow::bail!(
            "request_id {request_id} is ambiguous across {count} AgentRequest documents"
        ),
    }
}

async fn load_requests(
    access: &DescendantGraphAccess<'_>,
    request_ids: &[String],
) -> Result<Vec<AgentRequestRow>> {
    if request_ids.is_empty() {
        return Ok(Vec::new());
    }
    let list = graphql_string_list(request_ids);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _in: [{list}] }} }},
                order: [{{ created_at: ASC }}, {{ request_id: ASC }}]
            ) {{
                _docID
                request_id
                agent_did
                requester_did
                behavior_id
                session_id
                caused_by_parent_request_id
                caused_by_parent_request_doc_id
                caused_by_parent_tool_call_id
                caused_by_parent_tool_call_doc_id
            }}
        }}"#
    );
    let rows: Vec<AgentRequestRow> = load_rows(access, "AgentRequest", &query).await?;
    for row in &rows {
        anyhow::ensure!(
            row.doc_id.is_some(),
            "AgentRequest {} is missing _docID",
            row.request_id
        );
        anyhow::ensure!(
            row.session_id.is_some(),
            "AgentRequest {} is missing session_id",
            row.request_id
        );
    }
    Ok(rows)
}

fn request_doc_id(row: &AgentRequestRow) -> &str {
    row.doc_id
        .as_deref()
        .expect("AgentRequest _docID validated at query boundary")
}

fn request_session_id(row: &AgentRequestRow) -> &str {
    row.session_id
        .as_deref()
        .expect("AgentRequest session_id validated at query boundary")
}

async fn load_bridges(
    access: &DescendantGraphAccess<'_>,
    parent_request_ids: &[String],
) -> Result<Vec<BridgeRow>> {
    if parent_request_ids.is_empty() {
        return Ok(Vec::new());
    }
    let list = graphql_string_list(parent_request_ids);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    request_id: {{ _in: [{list}] }},
                    child_request_id: {{ _ne: "" }}
                }},
                order: [{{ started_at: ASC }}, {{ tool_call_id: ASC }}]
            ) {{ {BRIDGE_FIELDS} }}
        }}"#
    );
    load_rows(access, "AgentToolCall", &query).await
}

async fn load_bridges_by_child(
    access: &DescendantGraphAccess<'_>,
    child_request_id: &str,
) -> Result<Vec<BridgeRow>> {
    let child_request_id = escape_graphql_string(child_request_id);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{ child_request_id: {{ _eq: "{child_request_id}" }} }},
                order: [{{ started_at: ASC }}, {{ tool_call_id: ASC }}]
            ) {{ {BRIDGE_FIELDS} }}
        }}"#
    );
    load_rows(access, "AgentToolCall", &query).await
}

async fn load_unique_bridge_by_doc_id(
    access: &DescendantGraphAccess<'_>,
    doc_id: &str,
) -> Result<Option<BridgeRow>> {
    let doc_id = escape_graphql_string(doc_id);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{ _docID: {{ _eq: "{doc_id}" }} }},
                limit: 2
            ) {{ {BRIDGE_FIELDS} }}
        }}"#
    );
    let rows: Vec<BridgeRow> = load_rows(access, "AgentToolCall", &query).await?;
    match rows.len() {
        0 => Ok(None),
        1 => Ok(rows.into_iter().next()),
        count => anyhow::bail!("AgentToolCall document {doc_id} resolved to {count} rows"),
    }
}

async fn populate_transcript_cursors(
    access: &DescendantGraphAccess<'_>,
    edges: &mut [DescendantEdge],
) -> Result<()> {
    let session_ids = edges
        .iter()
        .filter(|edge| edge.readable())
        .filter_map(|edge| edge.child_session_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if session_ids.is_empty() {
        return Ok(());
    }
    let list = graphql_string_list(&session_ids);
    let query = format!(
        r#"{{
            AgentMessage(
                filter: {{ session_id: {{ _in: [{list}] }} }},
                order: [{{ session_id: ASC }}, {{ sequence: ASC }}]
            ) {{ session_id sequence }}
        }}"#
    );
    let rows: Vec<MessageCursorRow> = load_rows(access, "AgentMessage", &query).await?;
    let mut cursors = BTreeMap::<String, u64>::new();
    for row in rows {
        let next = row.sequence.unwrap_or_default().saturating_add(1);
        cursors
            .entry(row.session_id)
            .and_modify(|cursor| *cursor = (*cursor).max(next))
            .or_insert(next);
    }
    for edge in edges {
        edge.transcript_cursor = edge
            .child_session_id
            .as_ref()
            .and_then(|session_id| cursors.get(session_id))
            .copied()
            .unwrap_or_default();
    }
    Ok(())
}

async fn load_rows<T: for<'de> Deserialize<'de>>(
    access: &DescendantGraphAccess<'_>,
    collection: &str,
    query: &str,
) -> Result<Vec<T>> {
    let response = access.execute(query).await?;
    let value = response
        .pointer(&format!("/data/{collection}"))
        .cloned()
        .unwrap_or_else(|| Value::Array(Vec::new()));
    serde_json::from_value(value)
        .with_context(|| format!("decoding canonical descendant {collection} rows"))
}

fn parse_bridge_args(value: Option<&str>) -> BridgeArgs {
    value
        .and_then(|value| serde_json::from_str(value).ok())
        .unwrap_or_default()
}

fn graphql_string_list(values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("\"{}\"", escape_graphql_string(value)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn lifecycle(primary: &Option<String>, fallback: &Option<String>, default: &str) -> String {
    clean(primary.as_deref())
        .or_else(|| clean(fallback.as_deref()))
        .unwrap_or_else(|| default.to_string())
}

fn edge_cursor(
    depth: usize,
    created_at: Option<&str>,
    parent_request_id: &str,
    tool_call_id: &str,
    child_request_id: &str,
) -> String {
    // Fixed-width depth plus immutable identifiers gives every consumer the
    // same total order. The cursor is intentionally the sort key itself.
    format!(
        "{depth:08}\u{1f}{}\u{1f}{parent_request_id}\u{1f}{tool_call_id}\u{1f}{child_request_id}",
        created_at.unwrap_or_default()
    )
}

fn bridge_state_is_terminal(value: &str) -> bool {
    matches!(
        value.trim(),
        "completed"
            | "complete"
            | "failed"
            | "error"
            | "timedOut"
            | "cancelled"
            | "interrupted"
            | "superseded"
            | "dead"
    )
}

fn lifecycle_is_terminal(value: &str) -> bool {
    bridge_state_is_terminal(value)
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn clean(value: Option<&str>) -> Option<String> {
    nonempty(value).map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lean_vocab_test::lean_descendant_graph_cases;

    #[test]
    fn cursor_order_is_total_and_depth_first() {
        let direct = edge_cursor(1, Some("2026-01-01T00:00:00Z"), "p", "t", "c");
        let nested = edge_cursor(2, Some("2025-01-01T00:00:00Z"), "p2", "t2", "c2");
        assert!(direct < nested);
        assert_eq!(
            direct,
            edge_cursor(1, Some("2026-01-01T00:00:00Z"), "p", "t", "c")
        );
    }

    #[test]
    fn terminal_vocabulary_covers_request_and_bridge_spellings() {
        for value in [
            "completed",
            "failed",
            "timedOut",
            "cancelled",
            "interrupted",
            "superseded",
            "dead",
        ] {
            assert!(lifecycle_is_terminal(value), "{value}");
        }
        assert!(!lifecycle_is_terminal(AWAITING_CHILD_MATERIALIZATION));
        assert!(!lifecycle_is_terminal(PENDING_CHILD_AUTHORIZATION));
    }

    #[test]
    fn generated_descendant_graph_cases_fence_visibility_and_control() {
        let cases = lean_descendant_graph_cases();
        assert_eq!(cases.len(), 20);
        for case in cases {
            let owner = AgentRequestRow {
                doc_id: Some("owner-doc".into()),
                request_id: "owner-turn".into(),
                session_id: Some("conversation".into()),
                agent_did: Some("did:owner".into()),
                requester_did: None,
                behavior_id: None,
                caused_by_parent_request_id: None,
                caused_by_parent_request_doc_id: None,
                caused_by_parent_tool_call_id: None,
                caused_by_parent_tool_call_doc_id: None,
                ..Default::default()
            };
            let caller = AgentRequestRow {
                doc_id: Some("later-doc".into()),
                request_id: "later-turn".into(),
                session_id: Some(case.caller_session.clone()),
                agent_did: Some(case.caller_agent.clone()),
                requester_did: case.caller_requester.clone(),
                ..owner.clone()
            };
            let authorized = same_session_owner(&caller, &owner);
            assert_eq!(authorized, case.session_authorized, "{}", case.name);
            assert_eq!(
                authorized && case.controllable,
                case.session_controllable,
                "{}",
                case.name
            );
            assert!(case.root_request_id > 0, "{}", case.name);
            assert!(case.parent_request_id > 0, "{}", case.name);
            assert!(case.child_request_id > 0, "{}", case.name);
            assert!(
                matches!(case.await_mode.as_str(), "foreground" | "background"),
                "{}",
                case.name
            );
            if !case.visible {
                assert!(!case.controllable, "{}", case.name);
                assert!(!case.readable, "{}", case.name);
                assert!(!case.listed_by_default, "{}", case.name);
            }
            if !case.direct || case.materialization == "pending" {
                assert!(!case.controllable, "{}", case.name);
            }
            assert!(case.cursor_anchor_survives_terminal, "{}", case.name);
            assert_eq!(
                case.retryable,
                case.visible
                    && case.materialization == "pending"
                    && !matches!(
                        case.lifecycle.as_str(),
                        "completed" | "failed" | "cancelled"
                    ),
                "{}",
                case.name
            );
            if case.name == "nested_visible_not_controllable" {
                assert!(case.visible);
                assert!(!case.controllable);
            }
            if case.name.starts_with("unauthorized_") {
                assert!(!case.visible, "{}", case.name);
            }
            if case.name == "uncorroborated_materialized" {
                assert!(case.visible);
                assert!(!case.readable);
                assert!(!case.retryable);
                assert!(case.listed_by_default);
                assert!(!case.controllable);
            }
            if case.name == "terminal_unmaterialized_remote_bridge" {
                assert!(case.visible);
                assert!(!case.readable);
                assert!(!case.retryable);
                assert!(!case.listed_by_default);
                assert!(!case.controllable);
            }
        }
    }
}
