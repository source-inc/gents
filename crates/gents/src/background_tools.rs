#![allow(dead_code)] // R4b lands these helpers one task ahead of their tool integrations.

mod buffer;
pub(crate) mod r4c_args;
mod transcript_render;

use std::collections::{HashMap, HashSet};

use crate::llm::message::{AssistantContent, Message, Text, UserContent};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use defra_node::EmbeddedNode;
use gents_protocol::transcript::{decode_persisted_message, present_persisted_message};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::document_config::SubagentTarget;
use crate::graphql::{escape_graphql_string, response_has_documents};
use crate::lifecycle::queue::{
    drain_automated_wakeups, enqueue_session_request, is_automated_wakeup, QueueHints, QueuePolicy,
    QueueSource,
};
use crate::lifecycle::ExecutionOrigin;
use crate::session;
use crate::session::execute_mutation_with_retry;
use crate::tool_call_lifecycle::{AwaitMode, ChildTerminal, FailureClass};
use crate::watcher::{validate_agent_request, AgentRequest};

pub(crate) use self::buffer::{
    LiveOutputStream, LiveToolOutputRegistry, LiveToolOutputSnapshot, LiveToolOutputWriter,
};
use self::r4c_args::{
    ListBackgroundToolsArgs, ListBackgroundToolsEntry, ListBackgroundToolsResponse,
    ListStatusFilter, ListSubagentsArgs, ListSubagentsEntry, ListSubagentsResponse,
    ReadSubagentArgs, ReadSubagentResponse, ReadToolOutputArgs, ReadToolOutputResponse,
    SteerSubagentResponse,
};
use self::transcript_render::{
    render_transcript, MessageKindView, MessageRoleView, MessageView, RenderOptions,
};

/// #593: projected status served by `list_subagents`/`read_subagent` for a
/// background spawn bridge whose child `AgentRequest` has not materialized
/// yet (spawn convergence #377 creates the child asynchronously via
/// `SubagentSource`; a cross-deployment child appears only after the owning
/// peer claims and replicates it). This is a read-side projection of
/// (bridge `await_mode = background` ∧ bridge non-terminal ∧ child row
/// absent) — it is never persisted as a bridge `lifecycle_state`. Pinned by
/// the Lean witness `r4c.list_subagents.unmaterialized_child_visible`.
pub const AWAITING_CHILD_MATERIALIZATION: &str = "awaiting_child_materialization";

/// Immutable identity boundary used by `list_processes`, `read_process`,
/// `wait_process`, and `cancel_process`. A handle is usable on a later request
/// in the same session when the agent and requester principals still match.
/// Two absent requester identities are the same anonymous principal scope.
/// Exact originating-request ownership remains sufficient when only a legacy
/// owner row predates persisted requester lineage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProcessControlScope {
    pub(crate) request_id: String,
    pub(crate) session_id: String,
    pub(crate) agent_did: String,
    pub(crate) requester_did: Option<String>,
}

impl ProcessControlScope {
    pub(crate) fn authorizes(
        &self,
        owner_request_id: &str,
        owner_session_id: &str,
        owner_agent_did: &str,
        owner_requester_did: Option<&str>,
    ) -> bool {
        self.session_id == owner_session_id
            && self.agent_did == owner_agent_did
            && (self.request_id == owner_request_id
                || self.requester_did.as_deref() == owner_requester_did)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SpawnSubagentArgs {
    /// Friendly, model-facing name of an allowed subagent target. The runtime
    /// maps this name to the target's `(agent_did, behavior_id)`.
    pub name: String,
    pub prompt: String,
    #[serde(default)]
    pub await_mode: Option<AwaitModeArg>,
    #[serde(default)]
    pub deadline: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct WaitSubagentArgs {
    pub child_request_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct CancelSubagentArgs {
    pub child_request_id: String,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BackgroundToolArgs {
    pub tool_name: String,
    #[serde(default)]
    pub args: serde_json::Value,
}

// Bounded-wait defaults for wait_process, aligned with other agent
// frameworks (codex wait_agent defaults to 30s; grok-build caps blocking
// waits at 10 minutes). A wait that times out reports the process as still
// running without cancelling it (#985).
pub(crate) const DEFAULT_WAIT_PROCESS_TIMEOUT_SECS: u64 = 30;
pub(crate) const MAX_WAIT_PROCESS_TIMEOUT_SECS: u64 = 600;

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct WaitToolArgs {
    pub tool_call_id: String,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

impl WaitToolArgs {
    pub(crate) fn validated_wait_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(
            self.timeout_secs
                .unwrap_or(DEFAULT_WAIT_PROCESS_TIMEOUT_SECS)
                .clamp(1, MAX_WAIT_PROCESS_TIMEOUT_SECS),
        )
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct CancelToolArgs {
    pub tool_call_id: String,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum AwaitModeArg {
    #[default]
    Foreground,
    Background,
}

impl AwaitModeArg {
    pub(crate) fn as_await_mode(self) -> AwaitMode {
        match self {
            Self::Foreground => AwaitMode::Foreground,
            Self::Background => AwaitMode::Background,
        }
    }
}

impl<'de> Deserialize<'de> for AwaitModeArg {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        match value.trim() {
            "foreground" => Ok(Self::Foreground),
            "background" => Ok(Self::Background),
            other => Err(serde::de::Error::custom(format!(
                "unsupported await_mode '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParentSubagentContext {
    pub session_id: String,
    pub request_id: String,
    pub behavior_id: String,
    pub subagent_depth: u32,
    pub request_deadline_at: DateTime<Utc>,
    pub allowed_targets: Vec<SubagentTarget>,
    pub subagent_spawn_enabled: bool,
    pub orchestration_enabled: bool,
    pub subagent_background_enabled: bool,
    pub subagent_default_await_mode: AwaitMode,
    /// When false (default), cross-deployment (remote-DID) subagent spawns are
    /// rejected at runtime. Cross-deployment is deferred pending ACP.
    pub subagent_allow_cross_deployment: bool,
    pub cross_deployment_spawn_timeout_seconds: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParentSubagentAuthorization {
    pub behavior_id: String,
    pub allowed_targets: Vec<SubagentTarget>,
    pub spawn_enabled: bool,
    pub background_enabled: bool,
    pub allow_cross_deployment: bool,
    pub cross_deployment_spawn_timeout_seconds: Option<i64>,
}

impl ParentSubagentAuthorization {
    /// Resolve a model-facing target `name` to its configured [`SubagentTarget`].
    pub(crate) fn resolve_target(&self, name: &str) -> Option<&SubagentTarget> {
        self.allowed_targets
            .iter()
            .find(|target| target.name == name)
    }

    pub(crate) fn authorizes_target(&self, name: &str) -> bool {
        self.resolve_target(name).is_some()
    }

    /// Model-facing names the parent is allowed to spawn (for error payloads).
    pub(crate) fn allowed_target_names(&self) -> Vec<String> {
        self.allowed_targets
            .iter()
            .map(|target| target.name.clone())
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubagentAuthorizationDenial {
    pub path: &'static str,
    pub requested: String,
    pub message: String,
}

pub(crate) fn subagent_spawn_denial(
    authorization: &ParentSubagentAuthorization,
    target_name: &str,
    await_mode: AwaitMode,
    tool_name: &str,
    local_did: &str,
) -> Option<SubagentAuthorizationDenial> {
    if !authorization.spawn_enabled {
        return Some(SubagentAuthorizationDenial {
            path: "/",
            requested: tool_name.to_string(),
            message: "subagent spawning is not enabled for this behavior".to_string(),
        });
    }

    if await_mode == AwaitMode::Background && !authorization.background_enabled {
        return Some(SubagentAuthorizationDenial {
            path: "/await_mode",
            requested: "background".to_string(),
            message: "background subagent spawning is not enabled for this behavior".to_string(),
        });
    }

    let Some(target) = authorization.resolve_target(target_name) else {
        return Some(SubagentAuthorizationDenial {
            path: "/name",
            requested: target_name.to_string(),
            message: format!("'{target_name}' is not an allowed subagent target for this behavior"),
        });
    };

    // Cross-deployment (remote-DID) subagent delegation is deferred behind a
    // default-OFF flag (#377). When the resolved target is owned by a DID other
    // than this node's local DID and the parent behavior has not opted in,
    // refuse the spawn. This single gate covers the recovery path and the
    // non-trusted receiver fallback (both call `subagent_spawn_denial`); the
    // trusted-paired-peer receiver branch is gated separately in
    // `subagent_source`.
    let target_did = target.agent_did.trim();
    let local_did = local_did.trim();
    let target_is_remote = local_did.is_empty() || target_did != local_did;
    if target_is_remote && !authorization.allow_cross_deployment {
        return Some(SubagentAuthorizationDenial {
            path: "/name",
            requested: target_name.to_string(),
            message: "cross-deployment subagent delegation is not enabled".to_string(),
        });
    }

    None
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildEdge {
    pub parent_tool_call_id: String,
    pub child_request_id: String,
    pub child_session_id: String,
    /// Owning principal of the child request/session. #664: used to scope
    /// queue drains/interrupts to the child's own DID so a foreign-DID replica
    /// sharing the child session is never drained locally.
    pub child_agent_did: String,
    pub behavior_id: String,
    pub await_mode: AwaitMode,
    pub lifecycle_state: String,
}

#[derive(Debug, Deserialize)]
struct ParentRequestRow {
    request_id: String,
    session_id: String,
    behavior_id: Option<String>,
    subagent_depth: Option<u32>,
    deadline: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ParentAuthorizationRequestRow {
    behavior_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AgentBehaviorToolSelectionRow {
    tool_selection_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ToolSelectionTargetsRow {
    subagent_targets: Option<Vec<String>>,
    subagent_spawn_enabled: Option<bool>,
    orchestration_enabled: Option<bool>,
    subagent_background_enabled: Option<bool>,
    subagent_default_await_mode: Option<String>,
    #[serde(default)]
    subagent_allow_cross_deployment: Option<bool>,
    cross_deployment_spawn_timeout_seconds: Option<i64>,
}

pub(crate) const DEFAULT_CROSS_DEPLOYMENT_SPAWN_TIMEOUT_SECONDS: i64 = 60;

#[derive(Debug, Deserialize)]
struct ListSubagentBridgeRow {
    tool_call_id: String,
    child_request_id: Option<String>,
    lifecycle_state: Option<String>,
    await_mode: Option<String>,
    started_at: Option<String>,
    completed_at: Option<String>,
    unclaimed_deadline_at: Option<String>,
    /// Raw JSON bridge args — we extract the `name` field here.
    args: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ListSubagentChildRow {
    request_id: String,
    session_id: String,
    behavior_id: Option<String>,
    created_at: String,
    subagent_depth: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct ListBackgroundToolRow {
    tool_call_id: String,
    tool_name: String,
    request_id: String,
    session_id: String,
    agent_did: String,
    requester_did: Option<String>,
    await_mode: Option<String>,
    lifecycle_state: Option<String>,
    child_request_id: Option<String>,
    started_at: Option<String>,
    completed_at: Option<String>,
    result: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TranscriptMessageRow {
    sequence: u64,
    role: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct TranscriptToolCallRow {
    message_sequence: u64,
    tool_call_id: String,
    tool_name: String,
    await_mode: Option<String>,
    child_request_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ReadToolOutputRow {
    tool_call_id: String,
    tool_name: String,
    request_id: Option<String>,
    session_id: Option<String>,
    agent_did: Option<String>,
    requester_did: Option<String>,
    await_mode: Option<String>,
    lifecycle_state: Option<String>,
    child_request_id: Option<String>,
    result: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AgentRequestQueueRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    request_id: String,
    agent_did: String,
    requester_did: Option<String>,
    behavior_id: Option<String>,
    session_id: String,
    content: String,
    temperature: Option<f64>,
    top_p: Option<f64>,
    top_k: Option<i64>,
    seed: Option<i64>,
    max_tokens: Option<i64>,
    metadata: Option<String>,
    execution_origin: Option<String>,
    created_at: String,
    deadline: Option<String>,
    subagent_depth: Option<u32>,
    caused_by_parent_request_id: Option<String>,
    caused_by_parent_tool_call_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ActiveSessionRequestRow {
    request_id: String,
}

#[derive(Debug, Deserialize)]
struct PendingWakeupRow {
    request_id: Option<String>,
    execution_origin: Option<String>,
    metadata: Option<String>,
}

pub(crate) enum ReadToolOutputOutcome {
    Found(ReadToolOutputResponse),
    NotAuthorized,
    NotBackgrounded,
}

#[derive(Debug)]
pub enum SteerSubagentTarget {
    Found(ChildEdge),
    NotAuthorized,
    NotBackgrounded,
    /// #593: the caller owns a background spawn bridge for this child, but
    /// the child `AgentRequest` has not materialized yet. The message
    /// explains the bridge state instead of collapsing into not-authorized.
    AwaitingMaterialization {
        message: String,
    },
    Terminal(String),
}

pub async fn handle_list_subagents(
    node: &EmbeddedNode,
    caller_request_id: &str,
    local_deployment_id: &str,
    args: ListSubagentsArgs,
) -> Result<ListSubagentsResponse> {
    let limit = args.validated_limit() as usize;
    let escaped_caller = escape_graphql_string(caller_request_id);
    let escaped_spawn_tool = escape_graphql_string("spawn_subagent");
    let bridge_query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    request_id: {{ _eq: "{escaped_caller}" }},
                    tool_name: {{ _eq: "{escaped_spawn_tool}" }}
                }},
                order: {{ started_at: ASC }}
            ) {{
                tool_call_id
                child_request_id
                lifecycle_state
                await_mode
                started_at
                completed_at
                unclaimed_deadline_at
                args
            }}
        }}"#
    );
    let bridge_response = node.execute(&bridge_query).await;
    if bridge_response.has_errors() {
        anyhow::bail!(
            "list_subagents bridge query failed: {:?}",
            bridge_response.errors
        );
    }
    let bridges: Vec<ListSubagentBridgeRow> = rows(bridge_response.data.as_ref(), "AgentToolCall")?;

    let child_query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    caused_by_parent_request_id: {{ _eq: "{escaped_caller}" }}
                }},
                order: {{ created_at: ASC }}
            ) {{
                request_id
                session_id
                behavior_id
                created_at
                subagent_depth
            }}
        }}"#
    );
    let child_response = node.execute(&child_query).await;
    if child_response.has_errors() {
        anyhow::bail!(
            "list_subagents child query failed: {:?}",
            child_response.errors
        );
    }
    let children_by_request =
        rows::<ListSubagentChildRow>(child_response.data.as_ref(), "AgentRequest")?
            .into_iter()
            .map(|row| (row.request_id.clone(), row))
            .collect::<HashMap<_, _>>();

    let mut entries = Vec::new();
    for bridge in bridges {
        if bridge.await_mode.as_deref() != Some("background") {
            continue;
        }
        let Some(child_request_id) = non_empty_string(bridge.child_request_id.as_deref()) else {
            continue;
        };
        let bridge_status = bridge
            .lifecycle_state
            .as_deref()
            .filter(|state| !state.trim().is_empty())
            .unwrap_or("running");

        // Extract the model-facing `name` (and, for a bridge-level entry, the
        // resolved `behavior_id`) from the bridge args JSON. The named-target
        // redesign (#377) always writes both into the bridge args payload;
        // older or malformed records fall back to empty string.
        let bridge_args = bridge
            .args
            .as_deref()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok());
        let bridge_arg = |key: &str| {
            bridge_args
                .as_ref()
                .and_then(|v| v.get(key).and_then(serde_json::Value::as_str))
                .and_then(|s| non_empty_string(Some(s)))
                .unwrap_or_default()
        };

        let entry = match children_by_request.get(&child_request_id) {
            Some(child) => {
                if !list_subagent_status_matches(args.status, bridge_status) {
                    continue;
                }
                let created_at = parse_rfc3339(Some(&child.created_at)).ok_or_else(|| {
                    anyhow!("child AgentRequest {child_request_id} has invalid created_at")
                })?;
                let last_update = parse_rfc3339(bridge.completed_at.as_deref())
                    .or_else(|| parse_rfc3339(bridge.started_at.as_deref()))
                    .unwrap_or(created_at);
                ListSubagentsEntry {
                    child_request_id,
                    child_session_id: child.session_id.clone(),
                    name: bridge_arg("name"),
                    behavior_id: non_empty_string(child.behavior_id.as_deref()).unwrap_or_default(),
                    deployment_id: local_deployment_id.to_string(),
                    await_mode: "background".to_string(),
                    status: bridge_status.to_string(),
                    created_at,
                    last_update,
                    depth: child.subagent_depth.unwrap_or_default(),
                    diagnostic: None,
                }
            }
            None => {
                // #593: the child `AgentRequest` has not materialized (it is
                // created asynchronously by the claiming deployment; a
                // cross-deployment child appears only after replication). A
                // returned background child id must never disappear from the
                // control plane, so surface the bridge-level handle with a
                // projected status instead of dropping it.
                let status = if bridge_state_is_terminal(bridge_status) {
                    bridge_status.to_string()
                } else {
                    AWAITING_CHILD_MATERIALIZATION.to_string()
                };
                if !list_subagent_status_matches(args.status, &status) {
                    continue;
                }
                let created_at = parse_rfc3339(bridge.started_at.as_deref())
                    .or_else(|| parse_rfc3339(bridge.completed_at.as_deref()))
                    .ok_or_else(|| {
                        anyhow!(
                            "bridge AgentToolCall {} has invalid started_at",
                            bridge.tool_call_id
                        )
                    })?;
                let last_update =
                    parse_rfc3339(bridge.completed_at.as_deref()).unwrap_or(created_at);
                let diagnostic = unmaterialized_child_diagnostic(
                    &bridge.tool_call_id,
                    bridge_status,
                    bridge.unclaimed_deadline_at.as_deref(),
                );
                ListSubagentsEntry {
                    child_request_id,
                    child_session_id: String::new(),
                    name: bridge_arg("name"),
                    behavior_id: bridge_arg("behavior_id"),
                    deployment_id: local_deployment_id.to_string(),
                    await_mode: "background".to_string(),
                    status,
                    created_at,
                    last_update,
                    depth: 0,
                    diagnostic: Some(diagnostic),
                }
            }
        };
        entries.push(entry);
    }

    let truncated = entries.len() > limit;
    entries.truncate(limit);
    Ok(ListSubagentsResponse {
        read_at: Utc::now(),
        truncated,
        entries,
    })
}

pub(crate) async fn handle_list_background_tools(
    node: &EmbeddedNode,
    caller: &ProcessControlScope,
    local_deployment_id: &str,
    live_outputs: &LiveToolOutputRegistry,
    args: ListBackgroundToolsArgs,
) -> Result<ListBackgroundToolsResponse> {
    let limit = args.validated_limit() as usize;
    let escaped_session = escape_graphql_string(&caller.session_id);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    session_id: {{ _eq: "{escaped_session}" }},
                    await_mode: {{ _eq: "background" }}
                }},
                order: {{ started_at: ASC }}
            ) {{
                tool_call_id
                tool_name
                request_id
                session_id
                agent_did
                requester_did
                await_mode
                lifecycle_state
                child_request_id
                started_at
                completed_at
                result
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!("list_background_tools query failed: {:?}", response.errors);
    }
    let rows: Vec<ListBackgroundToolRow> =
        rows_skipping_malformed(response.data.as_ref(), "AgentToolCall")?;

    let mut entries = Vec::new();
    for row in rows {
        if !caller.authorizes(
            &row.request_id,
            &row.session_id,
            &row.agent_did,
            row.requester_did.as_deref(),
        ) {
            continue;
        }
        if non_empty_string(row.child_request_id.as_deref()).is_some() {
            continue;
        }
        if row.await_mode.as_deref() != Some("background") {
            continue;
        }
        let Some(tool_call_id) = non_empty_string(Some(&row.tool_call_id)) else {
            continue;
        };
        let status = row
            .lifecycle_state
            .as_deref()
            .filter(|state| !state.trim().is_empty())
            .unwrap_or("running");
        if !list_status_matches(args.status, status) {
            continue;
        }
        let Some(created_at) = parse_rfc3339(row.started_at.as_deref()) else {
            tracing::warn!(
                tool_call_id,
                started_at = ?row.started_at,
                "skipping malformed background tool call with invalid started_at"
            );
            continue;
        };
        let last_update = parse_rfc3339(row.completed_at.as_deref()).unwrap_or(created_at);
        let (stdout_bytes, stderr_bytes) = if status == "running" {
            live_outputs
                .snapshot(&tool_call_id)
                .await
                .map(|snapshot| (snapshot.stdout_bytes, snapshot.stderr_bytes))
                .unwrap_or((0, 0))
        } else {
            let persisted =
                persisted_tool_output_streams(&row.tool_name, row.result.as_deref().unwrap_or(""));
            (persisted.stdout.len() as u64, persisted.stderr.len() as u64)
        };

        entries.push(ListBackgroundToolsEntry {
            tool_call_id,
            tool_name: row.tool_name,
            deployment_id: local_deployment_id.to_string(),
            await_mode: "background".to_string(),
            status: status.to_string(),
            created_at,
            last_update,
            stdout_bytes,
            stderr_bytes,
        });
    }

    let truncated = entries.len() > limit;
    entries.truncate(limit);
    Ok(ListBackgroundToolsResponse {
        read_at: Utc::now(),
        truncated,
        entries,
    })
}

pub async fn handle_read_subagent(
    node: &EmbeddedNode,
    caller_request_id: &str,
    args: ReadSubagentArgs,
) -> Result<Option<ReadSubagentResponse>> {
    let child_request_id = args.child_request_id.trim();
    let Some(edge) =
        load_readable_background_child_edge(node, caller_request_id, child_request_id).await?
    else {
        // #593: distinguish "no such child edge for this caller" from "the
        // caller owns a background spawn bridge whose child has not
        // materialized". The bridge-level handle stays readable: an empty
        // transcript with the projected (never faked-terminal) state.
        let Some(bridge) = load_spawn_bridge_row(node, caller_request_id, child_request_id).await?
        else {
            return Ok(None);
        };
        if !bridge.is_background() {
            return Ok(None);
        }
        let terminal = bridge.is_terminal();
        let lifecycle_state = if terminal {
            bridge.status().to_string()
        } else {
            AWAITING_CHILD_MATERIALIZATION.to_string()
        };
        let diagnostic = unmaterialized_child_diagnostic(
            &bridge.tool_call_id,
            bridge.status(),
            bridge.unclaimed_deadline_at.as_deref(),
        );
        return Ok(Some(ReadSubagentResponse {
            child_request_id: child_request_id.to_string(),
            child_session_id: String::new(),
            from_sequence: 0,
            through_sequence: 0,
            next_sequence: 0,
            has_more: false,
            terminal,
            lifecycle_state,
            diagnostic: Some(diagnostic),
            transcript: String::new(),
        }));
    };

    // Project the child's terminal status so the model knows whether to keep
    // polling once it has drained the transcript.
    let (terminal, lifecycle_state) =
        match load_child_terminal_row(node, &edge.child_request_id).await? {
            Some(row) => match child_terminal_state_name(&row) {
                Some(state) => (true, state),
                None => (
                    false,
                    row.lifecycle_state
                        .as_deref()
                        .or(row.status.as_deref())
                        .filter(|state| !state.trim().is_empty())
                        .unwrap_or("running")
                        .to_string(),
                ),
            },
            None => (false, "running".to_string()),
        };

    let escaped_session_id = escape_graphql_string(&edge.child_session_id);
    let messages_query = format!(
        r#"{{
            AgentMessage(
                filter: {{ session_id: {{ _eq: "{escaped_session_id}" }} }},
                order: {{ sequence: ASC }}
            ) {{
                sequence
                role
                content
            }}
        }}"#
    );
    let messages_response = node.execute(&messages_query).await;
    if messages_response.has_errors() {
        anyhow::bail!(
            "read_subagent AgentMessage query failed: {:?}",
            messages_response.errors
        );
    }
    let message_rows: Vec<TranscriptMessageRow> =
        rows(messages_response.data.as_ref(), "AgentMessage")?;

    let tool_calls_query = format!(
        r#"{{
            AgentToolCall(
                filter: {{ session_id: {{ _eq: "{escaped_session_id}" }} }}
            ) {{
                message_sequence
                tool_call_id
                tool_name
                await_mode
                child_request_id
            }}
        }}"#
    );
    let tool_calls_response = node.execute(&tool_calls_query).await;
    if tool_calls_response.has_errors() {
        anyhow::bail!(
            "read_subagent AgentToolCall query failed: {:?}",
            tool_calls_response.errors
        );
    }
    let tool_call_rows: Vec<TranscriptToolCallRow> =
        rows(tool_calls_response.data.as_ref(), "AgentToolCall")?;

    let views = decode_transcript_message_views(message_rows, tool_call_rows);
    let rendered = render_transcript(
        &views,
        args.since_sequence,
        RenderOptions {
            include_user_messages: args.include_user_messages,
            include_tool_results: args.include_tool_results,
            limit: args.validated_limit(),
            max_chars: args.validated_max_chars(),
        },
    );

    Ok(Some(ReadSubagentResponse {
        child_request_id: edge.child_request_id,
        child_session_id: edge.child_session_id,
        from_sequence: rendered.from_sequence,
        through_sequence: rendered.through_sequence,
        next_sequence: rendered.next_sequence,
        has_more: rendered.has_more,
        terminal,
        lifecycle_state,
        diagnostic: None,
        transcript: rendered.transcript,
    }))
}

async fn load_readable_background_child_edge(
    node: &EmbeddedNode,
    caller_request_id: &str,
    child_request_id: &str,
) -> Result<Option<ChildEdge>> {
    if child_request_id.is_empty() {
        return Ok(None);
    }
    let parent_context = load_parent_subagent_context(node, caller_request_id).await?;
    match load_authorized_child_edge(node, &parent_context, child_request_id).await {
        Ok(edge) if edge.await_mode == AwaitMode::Background => Ok(Some(edge)),
        Ok(_) => Ok(None),
        Err(error) if authorization_lookup_error(&error, caller_request_id, child_request_id) => {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

/// True for the error class where the child edge could not be RESOLVED for
/// this caller (child row absent or not linked). Deliberately excludes
/// child-present corruption (e.g. missing `behavior_id`) and query failures,
/// so callers gating the #593 bridge fallback on this predicate never mask
/// data/storage problems as materialization lag.
pub(crate) fn authorization_lookup_error(
    error: &anyhow::Error,
    caller_request_id: &str,
    child_request_id: &str,
) -> bool {
    let message = error.to_string();
    message.contains(&format!("child AgentRequest {child_request_id} not found"))
        || message.contains(&format!(
            "child AgentRequest {child_request_id} is not linked to parent request {caller_request_id}"
        ))
        || (message.contains("parent AgentToolCall") && message.contains("not found for child"))
        || message.contains("has no parent tool-call link")
        || message.contains("does not point at child")
}

fn decode_transcript_message_views(
    message_rows: Vec<TranscriptMessageRow>,
    tool_call_rows: Vec<TranscriptToolCallRow>,
) -> Vec<MessageView> {
    let mut bridge_call_ids_by_sequence = HashMap::<u64, HashSet<String>>::new();
    let mut tool_names_by_call_id = HashMap::<String, String>::new();
    for row in tool_call_rows {
        if let Some(tool_call_id) = non_empty_string(Some(&row.tool_call_id)) {
            tool_names_by_call_id.insert(tool_call_id.clone(), row.tool_name);
            let is_background_bridge = non_empty_string(row.child_request_id.as_deref()).is_some()
                || row.await_mode.as_deref().map(str::trim) == Some("background");
            if is_background_bridge {
                bridge_call_ids_by_sequence
                    .entry(row.message_sequence)
                    .or_default()
                    .insert(tool_call_id);
            }
        }
    }

    message_rows
        .into_iter()
        .map(|row| {
            let presentation = present_persisted_message(&row.role, &row.content);
            let message = decode_persisted_message(&row.role, &row.content);
            let role = if row.role == "assistant" {
                MessageRoleView::Assistant
            } else {
                MessageRoleView::User
            };
            let body = presentation.body_markdown;
            let kind = if presentation.has_tool_results {
                let tool_name = tool_result_identities(&message)
                    .into_iter()
                    .find_map(|id| tool_names_by_call_id.get(&id).cloned())
                    .unwrap_or_else(|| "tool".to_string());
                MessageKindView::ToolResult { tool_name, body }
            } else if presentation.has_tool_calls {
                let bridge_call_ids = bridge_call_ids_by_sequence
                    .get(&row.sequence)
                    .map(|ids| ids.iter().cloned().collect::<Vec<_>>())
                    .unwrap_or_default();
                let bridge_set = bridge_call_ids.iter().cloned().collect::<HashSet<_>>();
                let tool_call_identities = assistant_tool_call_identities(&message);
                let non_bridge_tool_call_count = tool_call_identities
                    .iter()
                    .filter(|ids| ids.iter().all(|id| !bridge_set.contains(id)))
                    .count() as u32;
                MessageKindView::AssistantWithToolCalls {
                    body,
                    tool_call_count: tool_call_identities.len() as u32,
                    bridge_call_ids,
                    non_bridge_tool_call_count,
                }
            } else {
                MessageKindView::Ordinary { body }
            };

            MessageView {
                sequence: row.sequence,
                role,
                kind,
            }
        })
        .collect()
}

fn assistant_tool_call_identities(message: &Message) -> Vec<Vec<String>> {
    let Message::Assistant { content, .. } = message else {
        return Vec::new();
    };
    content
        .iter()
        .filter_map(|item| {
            let AssistantContent::ToolCall(tool_call) = item else {
                return None;
            };
            let mut ids = Vec::new();
            if let Some(id) = non_empty_string(Some(&tool_call.id)) {
                ids.push(id);
            }
            if let Some(call_id) = non_empty_string(tool_call.call_id.as_deref()) {
                ids.push(call_id);
            }
            (!ids.is_empty()).then_some(ids)
        })
        .collect()
}

fn tool_result_identities(message: &Message) -> Vec<String> {
    let Message::User { content } = message else {
        return Vec::new();
    };
    let mut ids = Vec::new();
    for item in content.iter() {
        let UserContent::ToolResult(tool_result) = item else {
            continue;
        };
        if let Some(id) = non_empty_string(Some(&tool_result.id)) {
            ids.push(id);
        }
        if let Some(call_id) = non_empty_string(tool_result.call_id.as_deref()) {
            ids.push(call_id);
        }
    }
    ids
}

pub(crate) async fn handle_read_tool_output(
    node: &EmbeddedNode,
    caller: &ProcessControlScope,
    live_outputs: &LiveToolOutputRegistry,
    args: ReadToolOutputArgs,
) -> Result<ReadToolOutputOutcome> {
    let tool_call_id = args.tool_call_id.trim();
    if tool_call_id.is_empty() {
        return Ok(ReadToolOutputOutcome::NotAuthorized);
    }

    let escaped_tool_call_id = escape_graphql_string(tool_call_id);
    let escaped_session = escape_graphql_string(&caller.session_id);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    session_id: {{ _eq: "{escaped_session}" }},
                    tool_call_id: {{ _eq: "{escaped_tool_call_id}" }}
                }},
                limit: 1
            ) {{
                tool_call_id
                tool_name
                request_id
                session_id
                agent_did
                requester_did
                await_mode
                lifecycle_state
                child_request_id
                result
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!("read_tool_output query failed: {:?}", response.errors);
    }
    let Some(row) = first_row::<ReadToolOutputRow>(response.data.as_ref(), "AgentToolCall") else {
        return Ok(ReadToolOutputOutcome::NotAuthorized);
    };
    if !caller.authorizes(
        row.request_id.as_deref().unwrap_or_default(),
        row.session_id.as_deref().unwrap_or_default(),
        row.agent_did.as_deref().unwrap_or_default(),
        row.requester_did.as_deref(),
    ) {
        return Ok(ReadToolOutputOutcome::NotAuthorized);
    }
    if row.await_mode.as_deref() != Some("background")
        || non_empty_string(row.child_request_id.as_deref()).is_some()
    {
        return Ok(ReadToolOutputOutcome::NotBackgrounded);
    }

    let status = row
        .lifecycle_state
        .as_deref()
        .filter(|state| !state.trim().is_empty())
        .unwrap_or("running")
        .to_string();
    let exited = status != "running";
    let max_bytes = args.validated_max_bytes();
    let (slice, exit_code) = if exited {
        let result = row.result.as_deref().unwrap_or_default();
        let persisted = persisted_tool_output_streams(&row.tool_name, result);
        // Merge stdout + stderr into a single logical buffer behind one byte cursor.
        // The capture stores the two streams separately with no preserved interleave
        // order, so combining them with a stable labeled boundary (stdout first,
        // then `STDERR_BOUNDARY`, then stderr) is the cleanest honest single-cursor
        // model: an orchestrator pages through ALL output gap-free from `offset`.
        let combined = combine_output_streams(&persisted.stdout, &persisted.stderr);
        (
            read_combined_output_slice(&combined, args.offset, max_bytes),
            persisted.exit_code,
        )
    } else {
        let slice = live_outputs
            .snapshot(&row.tool_call_id)
            .await
            .map(|snapshot| read_live_output_slice(snapshot, args.offset, max_bytes))
            .unwrap_or_else(|| read_combined_output_slice("", args.offset, max_bytes));
        (slice, None)
    };

    Ok(ReadToolOutputOutcome::Found(ReadToolOutputResponse {
        tool_call_id: row.tool_call_id,
        tool_name: row.tool_name,
        status,
        output: slice.output,
        next_offset: slice.next_offset,
        first_available_offset: slice.first_available_offset,
        total_bytes: slice.total_bytes,
        has_more: slice.has_more,
        exited,
        exit_code,
    }))
}

/// Boundary inserted between captured stdout and stderr in the combined buffer.
/// Only present when BOTH streams have content, so single-stream output is
/// served verbatim.
const STDERR_BOUNDARY: &str = "\n--- stderr ---\n";

/// Concatenate captured stdout and stderr into one logical buffer behind a
/// single byte cursor. stdout first; if both streams are non-empty a labeled
/// boundary separates them; if only one is non-empty it is served verbatim.
fn combine_output_streams(stdout: &str, stderr: &str) -> String {
    match (stdout.is_empty(), stderr.is_empty()) {
        (true, true) => String::new(),
        (false, true) => stdout.to_string(),
        (true, false) => stderr.to_string(),
        (false, false) => format!("{stdout}{STDERR_BOUNDARY}{stderr}"),
    }
}

struct CombinedOutputSlice {
    output: String,
    next_offset: u64,
    /// Earliest byte offset still readable. 0 for terminal/persisted output
    /// (nothing is ever evicted); for a running tool whose live ring buffer
    /// has overflowed, this is > 0 and a caller can detect that bytes in
    /// `[requested offset, first_available_offset)` were dropped.
    first_available_offset: u64,
    total_bytes: u64,
    has_more: bool,
}

/// Read a contiguous byte slice of the combined buffer starting at `offset`,
/// capped at `max_bytes`. Pages are contiguous from the cursor (no head/tail
/// drop): `next_offset = offset + bytes_returned`, `has_more` is true iff
/// `next_offset < total_bytes`. `offset` past the end yields an empty slice
/// with `next_offset == total_bytes` and `has_more == false`.
fn read_combined_output_slice(
    combined: &str,
    offset: u64,
    max_bytes: usize,
) -> CombinedOutputSlice {
    read_retained_output_slice(combined, 0, combined.len() as u64, offset, max_bytes)
}

fn read_live_output_slice(
    snapshot: LiveToolOutputSnapshot,
    offset: u64,
    max_bytes: usize,
) -> CombinedOutputSlice {
    let retained = String::from_utf8_lossy(&snapshot.combined.bytes).into_owned();
    read_retained_output_slice(
        &retained,
        snapshot.combined.first_offset,
        snapshot.combined.total_bytes_seen,
        offset,
        max_bytes,
    )
}

fn read_retained_output_slice(
    combined: &str,
    first_offset: u64,
    total_bytes: u64,
    offset: u64,
    max_bytes: usize,
) -> CombinedOutputSlice {
    let bytes = combined.as_bytes();
    let retained_end = first_offset.saturating_add(bytes.len() as u64);
    let total_bytes = total_bytes.max(retained_end);
    let start_offset = offset.clamp(first_offset, retained_end);
    let start = start_offset.saturating_sub(first_offset) as usize;
    let mut start = start;
    // Snap forward to a UTF-8 char boundary so slicing never splits a codepoint.
    while start < bytes.len() && !combined.is_char_boundary(start) {
        start += 1;
    }
    let mut end = start.saturating_add(max_bytes).min(bytes.len());
    while end > start && !combined.is_char_boundary(end) {
        end -= 1;
    }
    // Progress guard: if snapping back a multi-byte codepoint collapses the
    // slice to empty yet more bytes remain, advance `end` past that one
    // codepoint so every read makes progress.  In practice the 256-byte floor
    // in `validated_max_bytes` makes this unreachable today, but the guard
    // keeps the invariant explicit and safe against future budget changes.
    if end == start && start < bytes.len() {
        end = start + 1;
        while end < bytes.len() && !combined.is_char_boundary(end) {
            end += 1;
        }
    }
    let output = combined[start..end].to_string();
    let next_offset = first_offset.saturating_add(end as u64);
    CombinedOutputSlice {
        output,
        next_offset,
        first_available_offset: first_offset,
        total_bytes,
        has_more: next_offset < total_bytes,
    }
}

pub async fn load_steer_subagent_target(
    node: &EmbeddedNode,
    caller_request_id: &str,
    child_request_id: &str,
) -> Result<SteerSubagentTarget> {
    if child_request_id.trim().is_empty() {
        return Ok(SteerSubagentTarget::NotAuthorized);
    }
    let parent_context = load_parent_subagent_context(node, caller_request_id).await?;
    let edge = match load_authorized_child_edge(node, &parent_context, child_request_id).await {
        Ok(edge) => edge,
        Err(error) if authorization_lookup_error(&error, caller_request_id, child_request_id) => {
            // #593: if the caller owns a spawn bridge for this child, the id
            // is real — report the bridge state instead of not-authorized.
            let Some(bridge) =
                load_spawn_bridge_row(node, caller_request_id, child_request_id).await?
            else {
                return Ok(SteerSubagentTarget::NotAuthorized);
            };
            if !bridge.is_background() {
                return Ok(SteerSubagentTarget::NotBackgrounded);
            }
            if bridge.is_terminal() {
                return Ok(SteerSubagentTarget::Terminal(bridge.status().to_string()));
            }
            let (message, _retryable) = bridge.unmaterialized_child_explanation(child_request_id);
            return Ok(SteerSubagentTarget::AwaitingMaterialization { message });
        }
        Err(error) => return Err(error),
    };
    if edge.await_mode != AwaitMode::Background {
        return Ok(SteerSubagentTarget::NotBackgrounded);
    }
    let Some(terminal_row) = load_child_terminal_row(node, &edge.child_request_id).await? else {
        return Ok(SteerSubagentTarget::NotAuthorized);
    };
    if let Some(state) = child_terminal_state_name(&terminal_row) {
        return Ok(SteerSubagentTarget::Terminal(state));
    }

    Ok(SteerSubagentTarget::Found(edge))
}

pub(crate) async fn append_steering_request(
    node: &EmbeddedNode,
    caller_request_id: &str,
    edge: &ChildEdge,
    message: &str,
    interrupted_request_id: Option<String>,
    drained_wake_up_request_ids: Vec<String>,
) -> Result<SteerSubagentResponse> {
    // Load the child request first so the steering message is stamped with the
    // child session's owning agent_did (the message belongs to the child agent's
    // conversation slice, not the steering caller's).
    let mut child_request = load_agent_request_for_queue(node, &edge.child_request_id)
        .await?
        .ok_or_else(|| anyhow!("child AgentRequest {} not found", edge.child_request_id))?;
    session::append_message_with_requester_did(
        node,
        &edge.child_session_id,
        &child_request.agent_did,
        child_request.requester_did.as_deref(),
        "user",
        message,
        None,
        None,
    )
    .await?;
    if child_request.caused_by_parent_request_id.as_deref() != Some(caller_request_id) {
        anyhow::bail!(
            "child AgentRequest {} no longer links to caller request {caller_request_id}",
            edge.child_request_id
        );
    }
    child_request.caused_by_parent_request_id = Some(caller_request_id.to_string());
    child_request.caused_by_parent_tool_call_id = None;
    let enqueued = enqueue_session_request(
        node,
        &child_request,
        message,
        ExecutionOrigin::Interactive,
        QueueHints {
            source: QueueSource::Steering,
            policy: QueuePolicy::Append,
            key: None,
            queued_after_request_id: None,
            interrupted_request_id: interrupted_request_id.clone(),
        },
    )
    .await?;

    Ok(SteerSubagentResponse {
        child_request_id: edge.child_request_id.clone(),
        child_session_id: edge.child_session_id.clone(),
        queued_request_id: enqueued.request_id,
        interrupted_active_request_id: interrupted_request_id,
        drained_wake_up_request_ids,
    })
}

pub(crate) async fn active_session_request_id(
    node: &EmbeddedNode,
    session_id: &str,
) -> Result<Option<String>> {
    let escaped_session_id = escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    session_id: {{ _eq: "{escaped_session_id}" }},
                    status: {{ _eq: "processing" }},
                    lifecycle_state: {{ _in: ["claimed", "processing"] }}
                }},
                order: [{{ created_at: ASC }}, {{ request_id: ASC }}],
                limit: 1
            ) {{
                request_id
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query active request for session {session_id} failed: {:?}",
            response.errors
        );
    }
    Ok(
        first_row::<ActiveSessionRequestRow>(response.data.as_ref(), "AgentRequest")
            .map(|row| row.request_id),
    )
}

pub(crate) async fn drain_automated_wakeups_returning_ids(
    node: &EmbeddedNode,
    session_id: &str,
    agent_did: &str,
    reason: &str,
) -> Result<Vec<String>> {
    let request_ids = pending_automated_wakeup_request_ids(node, session_id).await?;
    drain_automated_wakeups(node, session_id, agent_did, reason).await?;
    Ok(request_ids)
}

pub(crate) async fn pending_automated_wakeup_request_ids(
    node: &EmbeddedNode,
    session_id: &str,
) -> Result<Vec<String>> {
    let escaped_session_id = escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    session_id: {{ _eq: "{escaped_session_id}" }},
                    status: {{ _eq: "pending" }},
                    lifecycle_state: {{ _eq: "pending" }}
                }},
                order: [{{ created_at: ASC }}, {{ request_id: ASC }}]
            ) {{
                request_id
                execution_origin
                metadata
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query pending automated wake-ups for session {session_id} failed: {:?}",
            response.errors
        );
    }
    let rows: Vec<PendingWakeupRow> = rows(response.data.as_ref(), "AgentRequest")?;
    Ok(rows
        .into_iter()
        .filter(|row| {
            row.execution_origin.as_deref() == Some("scheduled")
                && is_automated_wakeup(row.metadata.as_deref())
        })
        .filter_map(|row| non_empty_string(row.request_id.as_deref()))
        .collect())
}

async fn load_agent_request_for_queue(
    node: &EmbeddedNode,
    request_id: &str,
) -> Result<Option<AgentRequest>> {
    let escaped_request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                limit: 1
            ) {{
                _docID
                request_id
                agent_did
                requester_did
                behavior_id
                session_id
                content
                temperature
                top_p
                top_k
                seed
                max_tokens
                metadata
                execution_origin
                created_at
                deadline
                subagent_depth
                caused_by_parent_request_id
                caused_by_parent_tool_call_id
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query AgentRequest {request_id} for steering queue failed: {:?}",
            response.errors
        );
    }
    let Some(row) = first_row::<AgentRequestQueueRow>(response.data.as_ref(), "AgentRequest")
    else {
        return Ok(None);
    };
    let request = AgentRequest {
        doc_id: row.doc_id,
        request_id: row.request_id,
        agent_did: row.agent_did,
        requester_did: normalize_optional_string(row.requester_did),
        behavior_id: normalize_optional_string(row.behavior_id),
        session_id: row.session_id,
        content: row.content,
        temperature: row.temperature,
        top_p: row.top_p,
        top_k: row.top_k,
        seed: row.seed,
        max_tokens: row.max_tokens,
        metadata: row.metadata,
        execution_origin: normalize_optional_string(row.execution_origin),
        created_at: row.created_at,
        deadline: normalize_optional_string(row.deadline),
        subagent_depth: row.subagent_depth.unwrap_or(0),
        caused_by_parent_request_id: normalize_optional_string(row.caused_by_parent_request_id),
        caused_by_parent_tool_call_id: normalize_optional_string(row.caused_by_parent_tool_call_id),
    };
    validate_agent_request(&request)?;
    Ok(Some(request))
}

fn child_terminal_state_name(row: &ChildRequestTerminalRow) -> Option<String> {
    if child_request_completed(row) {
        return Some(
            row.lifecycle_state
                .as_deref()
                .or(row.status.as_deref())
                .unwrap_or("completed")
                .to_string(),
        );
    }
    project_child_terminal(row).map(|_| {
        row.lifecycle_state
            .as_deref()
            .or(row.status.as_deref())
            .unwrap_or("terminal")
            .to_string()
    })
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

#[derive(Debug, Default)]
struct PersistedToolOutputStreams {
    stdout: String,
    stderr: String,
    exit_code: Option<i32>,
}

fn persisted_tool_output_streams(tool_name: &str, result: &str) -> PersistedToolOutputStreams {
    parse_native_command_output_streams(tool_name, result).unwrap_or_else(|| {
        PersistedToolOutputStreams {
            stdout: result.to_string(),
            ..Default::default()
        }
    })
}

fn parse_native_command_output_streams(
    tool_name: &str,
    result: &str,
) -> Option<PersistedToolOutputStreams> {
    if !matches!(tool_name, "bash" | "bash_unrestricted") {
        return None;
    }

    let trimmed = result.trim_start();
    let (metadata_line, body) = trimmed.split_once('\n')?;
    let metadata = metadata_line.trim().strip_prefix("gents_exec: ")?;
    let metadata = serde_json::from_str::<Value>(metadata).ok()?;
    let body = body.strip_prefix("stdout:\n")?;
    let (stdout, stderr) = body.rsplit_once("\nstderr:\n")?;
    let stdout = persisted_stream_body(stdout);
    let stderr = persisted_stream_body(stderr);
    let exit_code = metadata
        .get("exit_code")
        .and_then(Value::as_i64)
        .and_then(|code| i32::try_from(code).ok());

    Some(PersistedToolOutputStreams {
        stdout,
        stderr,
        exit_code,
    })
}

fn persisted_stream_body(value: &str) -> String {
    if value == "(empty)" {
        String::new()
    } else {
        value.to_string()
    }
}

fn list_subagent_status_matches(filter: ListStatusFilter, status: &str) -> bool {
    match filter {
        // #593: the awaiting-materialization projection is non-terminal, so
        // the default `running` filter must show the handle.
        ListStatusFilter::Running => {
            status == "running" || status == AWAITING_CHILD_MATERIALIZATION
        }
        _ => list_status_matches(filter, status),
    }
}

fn list_status_matches(filter: ListStatusFilter, status: &str) -> bool {
    match filter {
        ListStatusFilter::Running => status == "running",
        ListStatusFilter::Terminal => bridge_state_is_terminal(status),
        ListStatusFilter::All => !status.trim().is_empty(),
    }
}

fn bridge_state_is_terminal(status: &str) -> bool {
    matches!(
        status,
        "completed" | "failed" | "timedOut" | "cancelled" | "dead" | "interrupted" | "superseded"
    )
}

/// #593: parent-facing explanation for a background spawn bridge whose child
/// `AgentRequest` row is absent.
fn unmaterialized_child_diagnostic(
    tool_call_id: &str,
    bridge_status: &str,
    unclaimed_deadline_at: Option<&str>,
) -> String {
    if bridge_state_is_terminal(bridge_status) {
        format!(
            "spawn bridge {tool_call_id} reached terminal state '{bridge_status}' without a \
             materialized child request (e.g. no paired deployment claimed the spawn)"
        )
    } else {
        let deadline_clause = unclaimed_deadline_at
            .filter(|value| !value.trim().is_empty())
            .map(|deadline| {
                format!("; the spawn fails as no_peer_claimed_spawn if unclaimed by {deadline}")
            })
            .unwrap_or_default();
        format!(
            "spawn bridge {tool_call_id} is running but the child request has not materialized \
             yet (it is created asynchronously by the claiming deployment; a cross-deployment \
             child appears only after peer replication){deadline_clause}"
        )
    }
}

pub(crate) async fn load_parent_subagent_context(
    node: &EmbeddedNode,
    parent_request_id: &str,
) -> Result<ParentSubagentContext> {
    let escaped_request_id = escape_graphql_string(parent_request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                limit: 1
            ) {{
                request_id
                session_id
                behavior_id
                subagent_depth
                deadline
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query parent AgentRequest {parent_request_id} failed: {:?}",
            response.errors
        );
    }

    let row: ParentRequestRow = first_row(response.data.as_ref(), "AgentRequest")
        .ok_or_else(|| anyhow!("parent AgentRequest {parent_request_id} not found"))?;
    let behavior_id = row
        .behavior_id
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("parent AgentRequest {parent_request_id} has no behavior_id"))?;
    let deadline = row
        .deadline
        .as_deref()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
        .ok_or_else(|| anyhow!("parent AgentRequest {parent_request_id} has no valid deadline"))?;
    let selection = load_subagent_tool_selection(node, &behavior_id).await?;

    Ok(ParentSubagentContext {
        session_id: row.session_id,
        request_id: row.request_id,
        behavior_id,
        subagent_depth: row.subagent_depth.unwrap_or_default(),
        request_deadline_at: deadline,
        allowed_targets: selection.allowed_targets,
        subagent_spawn_enabled: selection.spawn_enabled,
        orchestration_enabled: selection.orchestration_enabled,
        subagent_background_enabled: selection.background_enabled,
        subagent_default_await_mode: selection.default_await_mode,
        subagent_allow_cross_deployment: selection.allow_cross_deployment,
        cross_deployment_spawn_timeout_seconds: selection.cross_deployment_spawn_timeout_seconds,
    })
}

pub(crate) async fn parent_authorizes_subagent_target(
    node: &EmbeddedNode,
    parent_request_id: &str,
    target_behavior_id: &str,
) -> Result<bool> {
    Ok(load_parent_subagent_authorization(node, parent_request_id)
        .await?
        .authorizes_target(target_behavior_id))
}

pub(crate) async fn load_parent_subagent_authorization(
    node: &EmbeddedNode,
    parent_request_id: &str,
) -> Result<ParentSubagentAuthorization> {
    let escaped_request_id = escape_graphql_string(parent_request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                limit: 1
            ) {{
                behavior_id
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query parent AgentRequest {parent_request_id} authorization failed: {:?}",
            response.errors
        );
    }

    let row: ParentAuthorizationRequestRow = first_row(response.data.as_ref(), "AgentRequest")
        .ok_or_else(|| anyhow!("parent AgentRequest {parent_request_id} not found"))?;
    let behavior_id = row
        .behavior_id
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("parent AgentRequest {parent_request_id} has no behavior_id"))?;
    let selection = load_subagent_tool_selection(node, &behavior_id).await?;

    Ok(ParentSubagentAuthorization {
        behavior_id,
        allowed_targets: selection.allowed_targets,
        spawn_enabled: selection.spawn_enabled,
        background_enabled: selection.background_enabled,
        allow_cross_deployment: selection.allow_cross_deployment,
        cross_deployment_spawn_timeout_seconds: selection.cross_deployment_spawn_timeout_seconds,
    })
}

/// Load the `subagent_allow_cross_deployment` flag for a target behavior on
/// THIS node. Used by the receiver-side trusted-paired-peer claim path (#377)
/// to gate cross-deployment children on the TARGET behavior's opt-in: the
/// trusted-peer branch bypasses `subagent_spawn_denial`, so it must consult the
/// target behavior's flag directly before materializing a cross-deployment
/// child. Returns false (deny) when the behavior or its selection is absent.
pub(crate) async fn load_behavior_allow_cross_deployment(
    node: &EmbeddedNode,
    behavior_id: &str,
) -> Result<bool> {
    Ok(load_subagent_tool_selection(node, behavior_id)
        .await?
        .allow_cross_deployment)
}

pub(crate) fn effective_cross_deployment_spawn_timeout_seconds(
    authorization: &ParentSubagentAuthorization,
) -> i64 {
    authorization
        .cross_deployment_spawn_timeout_seconds
        .unwrap_or(DEFAULT_CROSS_DEPLOYMENT_SPAWN_TIMEOUT_SECONDS)
}

pub(crate) fn effective_context_cross_deployment_spawn_timeout_seconds(
    context: &ParentSubagentContext,
) -> i64 {
    context
        .cross_deployment_spawn_timeout_seconds
        .unwrap_or(DEFAULT_CROSS_DEPLOYMENT_SPAWN_TIMEOUT_SECONDS)
}

/// Resolve a model-facing target `name` to its configured [`SubagentTarget`]
/// within the parent context's allowed set.
pub(crate) fn resolve_context_target<'a>(
    context: &'a ParentSubagentContext,
    name: &str,
) -> Option<&'a SubagentTarget> {
    context
        .allowed_targets
        .iter()
        .find(|target| target.name == name)
}

pub(crate) fn target_is_allowed(context: &ParentSubagentContext, name: &str) -> bool {
    resolve_context_target(context, name).is_some()
}

/// Model-facing target names allowed by the parent context (for error payloads).
pub(crate) fn context_allowed_target_names(context: &ParentSubagentContext) -> Vec<String> {
    context
        .allowed_targets
        .iter()
        .map(|target| target.name.clone())
        .collect()
}

struct SubagentToolSelection {
    allowed_targets: Vec<SubagentTarget>,
    spawn_enabled: bool,
    orchestration_enabled: bool,
    background_enabled: bool,
    default_await_mode: AwaitMode,
    allow_cross_deployment: bool,
    cross_deployment_spawn_timeout_seconds: Option<i64>,
}

/// Parse the `subagent_targets` `[String]` JSON entries into structured
/// targets, deduping by `name` and dropping malformed/invalid entries.
fn parse_subagent_targets(entries: Vec<String>) -> Vec<SubagentTarget> {
    let mut seen = HashSet::new();
    let mut targets = Vec::with_capacity(entries.len());
    for entry in entries {
        match SubagentTarget::parse(&entry) {
            Ok(target) if target.is_structurally_valid() => {
                if seen.insert(target.name.trim().to_string()) {
                    targets.push(target);
                }
            }
            Ok(_) => {
                tracing::warn!(entry = %entry, "skipping structurally invalid subagent target");
            }
            Err(error) => {
                tracing::warn!(entry = %entry, %error, "skipping malformed subagent target entry");
            }
        }
    }
    targets
}

async fn load_subagent_tool_selection(
    node: &EmbeddedNode,
    behavior_id: &str,
) -> Result<SubagentToolSelection> {
    let escaped_behavior_id = escape_graphql_string(behavior_id);
    let behavior_query = format!(
        r#"{{
            AgentBehavior(
                filter: {{ behavior_id: {{ _eq: "{escaped_behavior_id}" }} }},
                limit: 1
            ) {{
                tool_selection_id
            }}
        }}"#
    );
    let behavior_response = node.execute(&behavior_query).await;
    if behavior_response.has_errors() {
        anyhow::bail!(
            "query AgentBehavior {behavior_id} for subagent targets failed: {:?}",
            behavior_response.errors
        );
    }
    let behavior: AgentBehaviorToolSelectionRow =
        first_row(behavior_response.data.as_ref(), "AgentBehavior")
            .ok_or_else(|| anyhow!("AgentBehavior {behavior_id} not found"))?;
    let selection_id = match behavior
        .tool_selection_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(selection_id) => selection_id,
        None => {
            return Ok(SubagentToolSelection {
                allowed_targets: Vec::new(),
                spawn_enabled: false,
                orchestration_enabled: false,
                background_enabled: false,
                default_await_mode: AwaitMode::Foreground,
                allow_cross_deployment: false,
                cross_deployment_spawn_timeout_seconds: None,
            });
        }
    };

    let escaped_selection_id = escape_graphql_string(selection_id);
    let selection_query = format!(
        r#"{{
            ToolSelection(
                filter: {{ selection_id: {{ _eq: "{escaped_selection_id}" }} }},
                limit: 1
            ) {{
                subagent_targets
                subagent_spawn_enabled
                orchestration_enabled
                subagent_background_enabled
                subagent_default_await_mode
                subagent_allow_cross_deployment
                cross_deployment_spawn_timeout_seconds
            }}
        }}"#
    );
    let selection_response = node.execute(&selection_query).await;
    if selection_response.has_errors() {
        anyhow::bail!(
            "query ToolSelection {selection_id} for subagent targets failed: {:?}",
            selection_response.errors
        );
    }
    let Some(selection) =
        first_row::<ToolSelectionTargetsRow>(selection_response.data.as_ref(), "ToolSelection")
    else {
        return Ok(SubagentToolSelection {
            allowed_targets: Vec::new(),
            spawn_enabled: false,
            orchestration_enabled: false,
            background_enabled: false,
            default_await_mode: AwaitMode::Foreground,
            allow_cross_deployment: false,
            cross_deployment_spawn_timeout_seconds: None,
        });
    };

    let background_enabled = selection.subagent_background_enabled.unwrap_or(false);
    let default_await_mode = selection
        .subagent_default_await_mode
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(AwaitMode::from_persisted)
        .filter(|mode| background_enabled || *mode != AwaitMode::Background)
        .unwrap_or(AwaitMode::Foreground);

    Ok(SubagentToolSelection {
        allowed_targets: parse_subagent_targets(selection.subagent_targets.unwrap_or_default()),
        spawn_enabled: selection.subagent_spawn_enabled.unwrap_or(false),
        orchestration_enabled: selection.orchestration_enabled.unwrap_or(false),
        background_enabled,
        default_await_mode,
        allow_cross_deployment: selection.subagent_allow_cross_deployment.unwrap_or(false),
        cross_deployment_spawn_timeout_seconds: selection.cross_deployment_spawn_timeout_seconds,
    })
}

#[cfg(test)]
mod cross_deployment_timeout_tests {
    use super::*;

    fn auth(timeout: Option<i64>) -> ParentSubagentAuthorization {
        ParentSubagentAuthorization {
            behavior_id: "parent".to_string(),
            allowed_targets: vec![SubagentTarget {
                name: "child".to_string(),
                agent_did: "did:key:zParent".to_string(),
                behavior_id: "child".to_string(),
                description: None,
            }],
            spawn_enabled: true,
            background_enabled: true,
            allow_cross_deployment: false,
            cross_deployment_spawn_timeout_seconds: timeout,
        }
    }

    #[test]
    fn override_takes_precedence() {
        assert_eq!(
            effective_cross_deployment_spawn_timeout_seconds(&auth(Some(120))),
            120
        );
    }

    #[test]
    fn default_when_none() {
        assert_eq!(
            effective_cross_deployment_spawn_timeout_seconds(&auth(None)),
            DEFAULT_CROSS_DEPLOYMENT_SPAWN_TIMEOUT_SECONDS
        );
    }

    fn auth_with(target_did: &str, allow_cross_deployment: bool) -> ParentSubagentAuthorization {
        ParentSubagentAuthorization {
            behavior_id: "parent".to_string(),
            allowed_targets: vec![SubagentTarget {
                name: "child".to_string(),
                agent_did: target_did.to_string(),
                behavior_id: "child-behavior".to_string(),
                description: None,
            }],
            spawn_enabled: true,
            background_enabled: true,
            allow_cross_deployment,
            cross_deployment_spawn_timeout_seconds: None,
        }
    }

    #[test]
    fn denial_allows_local_target_with_flag_off() {
        let auth = auth_with("did:key:zLocal", false);
        assert_eq!(
            subagent_spawn_denial(
                &auth,
                "child",
                AwaitMode::Background,
                "spawn_subagent",
                "did:key:zLocal"
            ),
            None
        );
    }

    #[test]
    fn denial_refuses_remote_target_with_flag_off() {
        let auth = auth_with("did:key:zRemote", false);
        let denial = subagent_spawn_denial(
            &auth,
            "child",
            AwaitMode::Background,
            "spawn_subagent",
            "did:key:zLocal",
        )
        .expect("remote target with flag off must be denied");
        assert_eq!(denial.path, "/name");
        assert!(denial.message.contains("cross-deployment"));
    }

    #[test]
    fn denial_allows_remote_target_with_flag_on() {
        let auth = auth_with("did:key:zRemote", true);
        assert_eq!(
            subagent_spawn_denial(
                &auth,
                "child",
                AwaitMode::Background,
                "spawn_subagent",
                "did:key:zLocal"
            ),
            None
        );
    }

    #[test]
    fn denial_refuses_when_local_did_unknown() {
        let auth = auth_with("did:key:zRemote", false);
        let denial =
            subagent_spawn_denial(&auth, "child", AwaitMode::Background, "spawn_subagent", "")
                .expect("empty local DID must be treated as remote and denied");
        assert!(denial.message.contains("cross-deployment"));
    }

    #[test]
    fn unmaterialized_explanation_is_retryable_until_bridge_terminal() {
        let running = SpawnBridgeRow {
            tool_call_id: "tc-running".to_string(),
            lifecycle_state: Some("running".to_string()),
            await_mode: Some("background".to_string()),
            unclaimed_deadline_at: Some("2026-07-01T00:01:00Z".to_string()),
        };
        let (message, retryable) = running.unmaterialized_child_explanation("child-1");
        assert!(retryable, "non-terminal bridge must be retryable");
        assert!(message.contains("child-1"));
        assert!(message.contains("tc-running"));
        assert!(message.contains("no_peer_claimed_spawn"));
        assert!(message.contains("2026-07-01T00:01:00Z"));

        let failed = SpawnBridgeRow {
            tool_call_id: "tc-failed".to_string(),
            lifecycle_state: Some("failed".to_string()),
            await_mode: Some("background".to_string()),
            unclaimed_deadline_at: None,
        };
        let (message, retryable) = failed.unmaterialized_child_explanation("child-1");
        assert!(!retryable, "terminal bridge must not be retryable");
        assert!(message.contains("'failed'"));
    }

    #[test]
    fn authorization_lookup_error_excludes_child_present_corruption() {
        // Resolution failures (child absent / unlinked) are probe-eligible…
        assert!(authorization_lookup_error(
            &anyhow!("child AgentRequest c-1 not found"),
            "p-1",
            "c-1"
        ));
        assert!(authorization_lookup_error(
            &anyhow!("child AgentRequest c-1 is not linked to parent request p-1"),
            "p-1",
            "c-1"
        ));
        // …child-present corruption and query failures are NOT: the #593
        // bridge fallback must never mask them as materialization lag.
        assert!(!authorization_lookup_error(
            &anyhow!("child AgentRequest c-1 has no behavior_id"),
            "p-1",
            "c-1"
        ));
        assert!(!authorization_lookup_error(
            &anyhow!("query child AgentRequest c-1 failed: storage down"),
            "p-1",
            "c-1"
        ));
    }

    #[test]
    fn awaiting_materialization_is_nonterminal_in_list_filters() {
        assert!(list_subagent_status_matches(
            ListStatusFilter::Running,
            AWAITING_CHILD_MATERIALIZATION
        ));
        assert!(list_subagent_status_matches(
            ListStatusFilter::All,
            AWAITING_CHILD_MATERIALIZATION
        ));
        assert!(!list_subagent_status_matches(
            ListStatusFilter::Terminal,
            AWAITING_CHILD_MATERIALIZATION
        ));
        // Unchanged: plain running and terminal bridge states.
        assert!(list_subagent_status_matches(
            ListStatusFilter::Running,
            "running"
        ));
        assert!(!list_subagent_status_matches(
            ListStatusFilter::Running,
            "failed"
        ));
        assert!(list_subagent_status_matches(
            ListStatusFilter::Terminal,
            "failed"
        ));
    }
}

#[derive(Debug, Deserialize)]
struct ChildRequestEdgeRow {
    request_id: String,
    session_id: String,
    agent_did: Option<String>,
    behavior_id: Option<String>,
    caused_by_parent_request_id: Option<String>,
    caused_by_parent_tool_call_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ParentToolCallEdgeRow {
    tool_call_id: String,
    request_id: Option<String>,
    lifecycle_state: Option<String>,
    await_mode: Option<String>,
    child_request_id: Option<String>,
}

/// Tolerant variant of [`load_authorized_child_edge`] used by the foreground
/// subagent wait loop. After the spawn convergence (#377) the child
/// `AgentRequest` is materialized asynchronously by `SubagentSource` rather than
/// synchronously by the hook, so the foreground poller can observe the bridge
/// before the child row exists. This returns `Ok(None)` for the "child not yet
/// materialized" / not-yet-linked cases (using the same `authorization_lookup_error`
/// predicate as `load_readable_background_child_edge`) so the caller can back off
/// and keep polling; all other errors propagate.
pub(crate) async fn try_load_authorized_child_edge(
    node: &EmbeddedNode,
    parent_context: &ParentSubagentContext,
    child_request_id: &str,
) -> Result<Option<ChildEdge>> {
    match load_authorized_child_edge(node, parent_context, child_request_id).await {
        Ok(edge) => Ok(Some(edge)),
        Err(error)
            if authorization_lookup_error(&error, &parent_context.request_id, child_request_id) =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

/// #593: the caller-owned spawn bridge pointing at a child request id,
/// loadable regardless of whether the child `AgentRequest` exists. This is
/// the durable receipt behind a background spawn: when the child row is
/// absent, the bridge is what keeps the returned `child_request_id`
/// observable on the parent control plane.
#[derive(Debug, Deserialize)]
pub(crate) struct SpawnBridgeRow {
    pub(crate) tool_call_id: String,
    pub(crate) lifecycle_state: Option<String>,
    pub(crate) await_mode: Option<String>,
    pub(crate) unclaimed_deadline_at: Option<String>,
}

impl SpawnBridgeRow {
    pub(crate) fn status(&self) -> &str {
        self.lifecycle_state
            .as_deref()
            .filter(|state| !state.trim().is_empty())
            .unwrap_or("running")
    }

    pub(crate) fn is_terminal(&self) -> bool {
        bridge_state_is_terminal(self.status())
    }

    pub(crate) fn is_background(&self) -> bool {
        self.await_mode.as_deref().map(str::trim) == Some("background")
    }

    /// Explanation served by `wait_subagent`/`cancel_subagent` for a child
    /// that has not materialized: `(message, retryable)`. Retryable while the
    /// bridge is non-terminal (the child may still materialize or the
    /// unclaimed projection will fail the bridge); not retryable once the
    /// bridge is terminal.
    pub(crate) fn unmaterialized_child_explanation(
        &self,
        child_request_id: &str,
    ) -> (String, bool) {
        let diagnostic = unmaterialized_child_diagnostic(
            &self.tool_call_id,
            self.status(),
            self.unclaimed_deadline_at.as_deref(),
        );
        (
            format!("child request {child_request_id} has no materialized row yet: {diagnostic}"),
            !self.is_terminal(),
        )
    }
}

/// Load the caller-owned `spawn_subagent` bridge whose `child_request_id`
/// matches, independent of child materialization. Ownership is enforced by
/// filtering on the caller's `request_id`, so a caller can never observe a
/// sibling's bridge through this path.
pub(crate) async fn load_spawn_bridge_row(
    node: &EmbeddedNode,
    caller_request_id: &str,
    child_request_id: &str,
) -> Result<Option<SpawnBridgeRow>> {
    if child_request_id.trim().is_empty() {
        return Ok(None);
    }
    let escaped_caller = escape_graphql_string(caller_request_id);
    let escaped_child = escape_graphql_string(child_request_id);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    request_id: {{ _eq: "{escaped_caller}" }},
                    child_request_id: {{ _eq: "{escaped_child}" }}
                }},
                limit: 1
            ) {{
                tool_call_id
                lifecycle_state
                await_mode
                unclaimed_deadline_at
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query spawn bridge for child {child_request_id} failed: {:?}",
            response.errors
        );
    }
    Ok(first_row(response.data.as_ref(), "AgentToolCall"))
}

pub(crate) async fn load_authorized_child_edge(
    node: &EmbeddedNode,
    parent_context: &ParentSubagentContext,
    child_request_id: &str,
) -> Result<ChildEdge> {
    let escaped_child_request_id = escape_graphql_string(child_request_id);
    let child_query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_child_request_id}" }} }},
                limit: 1
            ) {{
                request_id
                session_id
                agent_did
                behavior_id
                caused_by_parent_request_id
                caused_by_parent_tool_call_id
            }}
        }}"#
    );
    let child_response = node.execute(&child_query).await;
    if child_response.has_errors() {
        anyhow::bail!(
            "query child AgentRequest {child_request_id} failed: {:?}",
            child_response.errors
        );
    }
    let child: ChildRequestEdgeRow = first_row(child_response.data.as_ref(), "AgentRequest")
        .ok_or_else(|| anyhow!("child AgentRequest {child_request_id} not found"))?;
    if child.caused_by_parent_request_id.as_deref() != Some(parent_context.request_id.as_str()) {
        anyhow::bail!(
            "child AgentRequest {child_request_id} is not linked to parent request {}",
            parent_context.request_id
        );
    }
    let parent_tool_call_id = child
        .caused_by_parent_tool_call_id
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            anyhow!("child AgentRequest {child_request_id} has no parent tool-call link")
        })?;
    let behavior_id = child
        .behavior_id
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("child AgentRequest {child_request_id} has no behavior_id"))?;
    let child_agent_did = child
        .agent_did
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("child AgentRequest {child_request_id} has no agent_did"))?;
    let escaped_parent_session_id = escape_graphql_string(&parent_context.session_id);
    let escaped_parent_request_id = escape_graphql_string(&parent_context.request_id);
    let escaped_parent_tool_call_id = escape_graphql_string(&parent_tool_call_id);
    let tool_call_query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    session_id: {{ _eq: "{escaped_parent_session_id}" }},
                    request_id: {{ _eq: "{escaped_parent_request_id}" }},
                    tool_call_id: {{ _eq: "{escaped_parent_tool_call_id}" }}
                }},
                limit: 1
            ) {{
                tool_call_id
                request_id
                lifecycle_state
                await_mode
                child_request_id
            }}
        }}"#
    );
    let tool_call_response = node.execute(&tool_call_query).await;
    if tool_call_response.has_errors() {
        anyhow::bail!(
            "query parent AgentToolCall {parent_tool_call_id} failed: {:?}",
            tool_call_response.errors
        );
    }
    let tool_call: ParentToolCallEdgeRow =
        first_row(tool_call_response.data.as_ref(), "AgentToolCall").ok_or_else(|| {
            anyhow!(
                "parent AgentToolCall {parent_tool_call_id} not found for child {child_request_id}"
            )
        })?;
    if tool_call.request_id.as_deref() != Some(parent_context.request_id.as_str()) {
        anyhow::bail!(
            "parent AgentToolCall {parent_tool_call_id} is not linked to parent request {}",
            parent_context.request_id
        );
    }
    if tool_call.child_request_id.as_deref() != Some(child.request_id.as_str()) {
        anyhow::bail!(
            "parent AgentToolCall {parent_tool_call_id} does not point at child {child_request_id}"
        );
    }
    let await_mode = tool_call
        .await_mode
        .as_deref()
        .and_then(AwaitMode::from_persisted)
        .unwrap_or(AwaitMode::Foreground);

    Ok(ChildEdge {
        parent_tool_call_id: tool_call.tool_call_id,
        child_request_id: child.request_id,
        child_session_id: child.session_id,
        child_agent_did,
        behavior_id,
        await_mode,
        lifecycle_state: tool_call
            .lifecycle_state
            .unwrap_or_else(|| "running".to_string()),
    })
}

#[derive(Debug, Deserialize)]
struct AgentResponseFinalRow {
    materialized_message_sequence: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct AgentMessageContentRow {
    role: String,
    content: String,
}

pub(crate) async fn load_child_final_response(
    node: &EmbeddedNode,
    child_edge: &ChildEdge,
) -> Result<Option<String>> {
    let child_request_id = &child_edge.child_request_id;
    let escaped_child_request_id = escape_graphql_string(child_request_id);
    let response_query = format!(
        r#"{{
            AgentResponse(
                filter: {{ request_id: {{ _eq: "{escaped_child_request_id}" }} }},
                order: {{ created_at: DESC }},
                limit: 1
            ) {{
                materialized_message_sequence
            }}
        }}"#
    );
    let response = node.execute(&response_query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query child AgentResponse {child_request_id} failed: {:?}",
            response.errors
        );
    }
    let Some(response_row) =
        first_row::<AgentResponseFinalRow>(response.data.as_ref(), "AgentResponse")
    else {
        return Ok(None);
    };
    let Some(sequence) = response_row.materialized_message_sequence else {
        return Ok(None);
    };

    let escaped_session_id = escape_graphql_string(&child_edge.child_session_id);
    let message_query = format!(
        r#"{{
            AgentMessage(
                filter: {{
                    session_id: {{ _eq: "{escaped_session_id}" }},
                    sequence: {{ _eq: {sequence} }}
                }},
                limit: 1
            ) {{
                role
                content
            }}
        }}"#
    );
    let message = node.execute(&message_query).await;
    if message.has_errors() {
        anyhow::bail!(
            "query child AgentMessage {child_request_id} sequence {sequence} failed: {:?}",
            message.errors
        );
    }
    let Some(message_row) =
        first_row::<AgentMessageContentRow>(message.data.as_ref(), "AgentMessage")
    else {
        return Ok(None);
    };
    if message_row.role != "assistant" {
        anyhow::bail!(
            "materialized child response {child_request_id} sequence {sequence} is role {}",
            message_row.role
        );
    }

    Ok(Some(render_assistant_message_text(&message_row.content)?))
}

pub(crate) async fn load_child_terminal_row(
    node: &EmbeddedNode,
    child_request_id: &str,
) -> Result<Option<ChildRequestTerminalRow>> {
    let escaped_child_request_id = escape_graphql_string(child_request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_child_request_id}" }} }},
                limit: 1
            ) {{
                status
                lifecycle_state
                failure_reason
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query child AgentRequest {child_request_id} terminal state failed: {:?}",
            response.errors
        );
    }
    Ok(first_row::<ChildRequestTerminalRow>(
        response.data.as_ref(),
        "AgentRequest",
    ))
}

fn render_assistant_message_text(content: &str) -> Result<String> {
    let message = decode_persisted_message("assistant", content);
    let Message::Assistant { content, .. } = message else {
        anyhow::bail!("materialized child response is not an assistant message");
    };

    // A materialized final response handed to a waiting parent (a subagent
    // bridge result, or a workflow fan-out outcome fed to the synthesizer) should
    // be the assistant's ANSWER TEXT — never its chain-of-thought. Render only
    // `Text` content; drop reasoning/tool-call/image items so no provider's
    // reasoning trace can leak into a downstream prompt.
    let text_parts: Vec<String> = content
        .iter()
        .filter_map(|item| match item {
            AssistantContent::Text(Text { text }) => Some(text.clone()),
            _ => None,
        })
        .collect();
    if !text_parts.is_empty() {
        return Ok(text_parts.join("\n"));
    }
    // Rare: a final message with no text content. Fall back to the full
    // serialization rather than returning an empty answer.
    let mut parts = Vec::new();
    for item in content.iter() {
        match item {
            AssistantContent::Text(Text { text }) => parts.push(text.clone()),
            other => parts.push(serde_json::to_string(other)?),
        }
    }
    Ok(parts.join("\n"))
}

#[derive(Debug, Deserialize)]
pub(crate) struct ChildRequestTerminalRow {
    pub status: Option<String>,
    pub lifecycle_state: Option<String>,
    pub failure_reason: Option<String>,
}

pub(crate) fn project_child_terminal(row: &ChildRequestTerminalRow) -> Option<ChildTerminal> {
    let lifecycle_state = row.lifecycle_state.as_deref().unwrap_or_default();
    match lifecycle_state {
        "completed" | "complete" => None,
        "failed" | "error" => Some(ChildTerminal::Failed {
            reason: non_empty_string(row.failure_reason.as_deref())
                .unwrap_or_else(|| "child request failed".to_string()),
            failure_class: FailureClass::External,
        }),
        "dead" | "timedOut" => Some(ChildTerminal::Dead),
        "interrupted" | "cancelled" => Some(ChildTerminal::Interrupted),
        "superseded" => Some(ChildTerminal::Superseded),
        _ => match row.status.as_deref().unwrap_or_default() {
            "complete" | "completed" => None,
            "error" | "failed" => Some(ChildTerminal::Failed {
                reason: non_empty_string(row.failure_reason.as_deref())
                    .unwrap_or_else(|| "child request failed".to_string()),
                failure_class: FailureClass::External,
            }),
            "interrupted" | "cancelled" => Some(ChildTerminal::Interrupted),
            "superseded" => Some(ChildTerminal::Superseded),
            _ => None,
        },
    }
}

pub(crate) fn child_request_completed(row: &ChildRequestTerminalRow) -> bool {
    matches!(
        row.lifecycle_state.as_deref(),
        Some("completed" | "complete")
    ) || matches!(row.status.as_deref(), Some("completed" | "complete"))
}

pub(crate) fn subagent_tool_not_allowed_payload(
    tool_name: &str,
    path: &str,
    requested: &str,
    message: impl Into<String>,
    allowed_targets: &[String],
) -> String {
    serde_json::to_string(&json!({
        "ok": false,
        "failure_class": "tool_not_allowed",
        "path": path,
        "message": message.into(),
        "retryable": false,
        "service_id": "subagent",
        "tool_name": tool_name,
        "requested_tool_name": requested,
        "allowed_subagent_targets": allowed_targets
    }))
    .unwrap_or_else(|_| {
        r#"{"ok":false,"failure_class":"tool_not_allowed","service_id":"subagent"}"#.to_string()
    })
}

pub(crate) async fn fail_running_subagent_tool_call(
    node: &EmbeddedNode,
    doc_id: &str,
    started_at: Option<&str>,
    deadline_at: Option<&str>,
    result: &str,
    failure: FailureClass,
) -> Result<bool> {
    let now = Utc::now();
    let started_at = parse_rfc3339(started_at).unwrap_or(now);
    let latency_ms = (now - started_at).num_milliseconds().max(0);
    let escaped_doc_id = escape_graphql_string(doc_id);
    let escaped_result = escape_graphql_string(result);
    let started_at_str = started_at.to_rfc3339();
    let completed_at_str = now.to_rfc3339();
    let failure_class = failure.as_str();
    let deadline_field = parse_rfc3339(deadline_at)
        .map(|deadline| format!(r#", deadline_at: "{}""#, deadline.to_rfc3339()))
        .unwrap_or_default();

    let mutation = format!(
        r#"mutation {{
            update_AgentToolCall(
                filter: {{
                    _docID: {{ _eq: "{escaped_doc_id}" }},
                    lifecycle_state: {{ _eq: "running" }}
                }},
                input: {{
                    result: "{escaped_result}",
                    status: "completed",
                    lifecycle_state: "failed",
                    started_at: "{started_at_str}"{deadline_field},
                    completed_at: "{completed_at_str}",
                    tool_failure_class: "{failure_class}",
                    latency_ms: {latency_ms}
                }}
            ) {{ _docID }}
        }}"#
    );

    let response =
        execute_mutation_with_retry(node, &mutation, "fail_running_subagent_tool_call").await?;
    Ok(response
        .data
        .as_ref()
        .and_then(|data| data.get("update_AgentToolCall"))
        .is_some_and(response_has_documents))
}

fn parse_rfc3339(value: Option<&str>) -> Option<DateTime<Utc>> {
    value
        .filter(|value| !value.trim().is_empty())
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
}

fn first_row<T>(data: Option<&serde_json::Value>, collection: &str) -> Option<T>
where
    T: for<'de> Deserialize<'de>,
{
    data.and_then(|data| data.get(collection))
        .and_then(|value| serde_json::from_value::<Vec<T>>(value.clone()).ok())
        .and_then(|mut rows| rows.pop())
}

fn rows<T>(data: Option<&serde_json::Value>, collection: &str) -> Result<Vec<T>>
where
    T: for<'de> Deserialize<'de>,
{
    let Some(value) = data.and_then(|data| data.get(collection)) else {
        anyhow::bail!("{collection} field missing from query response");
    };
    serde_json::from_value(value.clone()).map_err(|error| anyhow!("parse {collection}: {error}"))
}

fn rows_skipping_malformed<T>(data: Option<&serde_json::Value>, collection: &str) -> Result<Vec<T>>
where
    T: for<'de> Deserialize<'de>,
{
    let Some(value) = data.and_then(|data| data.get(collection)) else {
        anyhow::bail!("{collection} field missing from query response");
    };
    let Some(values) = value.as_array() else {
        anyhow::bail!("parse {collection}: expected an array");
    };

    let mut parsed = Vec::with_capacity(values.len());
    for (row_index, value) in values.iter().enumerate() {
        match serde_json::from_value(value.clone()) {
            Ok(row) => parsed.push(row),
            Err(error) => {
                tracing::warn!(
                    collection,
                    row_index,
                    error = %error,
                    "skipping malformed control-plane row"
                );
            }
        }
    }
    Ok(parsed)
}

fn dedupe_non_empty(values: Vec<String>) -> Vec<String> {
    let mut deduped = Vec::with_capacity(values.len());
    for value in values {
        let value = value.trim();
        if !value.is_empty() && !deduped.iter().any(|existing| existing == value) {
            deduped.push(value.to_string());
        }
    }
    deduped
}

fn non_empty_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::message::{AssistantContent, Text};

    #[test]
    fn process_control_requester_absence_is_exact_not_empty_string() {
        let owner = ProcessControlScope {
            request_id: "request-1".to_string(),
            session_id: "session-1".to_string(),
            agent_did: "did:agent".to_string(),
            requester_did: None,
        };
        let absent_next_turn = ProcessControlScope {
            request_id: "request-2".to_string(),
            ..owner.clone()
        };
        assert!(absent_next_turn.authorizes(
            &owner.request_id,
            &owner.session_id,
            &owner.agent_did,
            owner.requester_did.as_deref(),
        ));

        let empty_next_turn = ProcessControlScope {
            requester_did: Some(String::new()),
            ..absent_next_turn
        };
        assert!(!empty_next_turn.authorizes(
            &owner.request_id,
            &owner.session_id,
            &owner.agent_did,
            owner.requester_did.as_deref(),
        ));
    }

    #[test]
    fn wait_process_timeout_defaults_and_clamps() {
        let args = |timeout_secs| WaitToolArgs {
            tool_call_id: "call".to_string(),
            timeout_secs,
        };
        assert_eq!(
            args(None).validated_wait_timeout(),
            std::time::Duration::from_secs(DEFAULT_WAIT_PROCESS_TIMEOUT_SECS)
        );
        assert_eq!(
            args(Some(0)).validated_wait_timeout(),
            std::time::Duration::from_secs(1)
        );
        assert_eq!(
            args(Some(5)).validated_wait_timeout(),
            std::time::Duration::from_secs(5)
        );
        assert_eq!(
            args(Some(999_999)).validated_wait_timeout(),
            std::time::Duration::from_secs(MAX_WAIT_PROCESS_TIMEOUT_SECS)
        );
    }

    #[test]
    fn project_child_terminal_maps_child_states() {
        assert_eq!(
            project_child_terminal(&ChildRequestTerminalRow {
                status: Some("error".to_string()),
                lifecycle_state: Some("failed".to_string()),
                failure_reason: Some("bad output".to_string()),
            }),
            Some(ChildTerminal::Failed {
                reason: "bad output".to_string(),
                failure_class: FailureClass::External,
            })
        );
        assert_eq!(
            project_child_terminal(&ChildRequestTerminalRow {
                status: Some("processing".to_string()),
                lifecycle_state: Some("dead".to_string()),
                failure_reason: None,
            }),
            Some(ChildTerminal::Dead)
        );
        assert_eq!(
            project_child_terminal(&ChildRequestTerminalRow {
                status: Some("interrupted".to_string()),
                lifecycle_state: Some("interrupted".to_string()),
                failure_reason: None,
            }),
            Some(ChildTerminal::Interrupted)
        );
        assert_eq!(
            project_child_terminal(&ChildRequestTerminalRow {
                status: Some("superseded".to_string()),
                lifecycle_state: Some("superseded".to_string()),
                failure_reason: None,
            }),
            Some(ChildTerminal::Superseded)
        );
        assert_eq!(
            project_child_terminal(&ChildRequestTerminalRow {
                status: Some("complete".to_string()),
                lifecycle_state: Some("completed".to_string()),
                failure_reason: None,
            }),
            None
        );
    }

    #[test]
    fn render_assistant_message_text_uses_persisted_assistant_message() {
        let message = Message::Assistant {
            id: None,
            content: vec![AssistantContent::Text(Text {
                text: "child final answer".to_string(),
            })],
        };
        let content = serde_json::to_string(&message).unwrap();
        assert_eq!(
            render_assistant_message_text(&content).unwrap(),
            "child final answer"
        );
    }

    #[test]
    fn render_assistant_message_text_uses_legacy_assistant_content() {
        let content = vec![AssistantContent::Text(Text {
            text: "legacy child final answer".to_string(),
        })];
        let persisted = serde_json::to_string(&content).unwrap();
        assert_eq!(
            render_assistant_message_text(&persisted).unwrap(),
            "legacy child final answer"
        );
    }

    #[test]
    fn render_assistant_message_text_uses_plain_text_assistant_content() {
        assert_eq!(
            render_assistant_message_text("plain child final answer").unwrap(),
            "plain child final answer"
        );
    }

    #[test]
    fn dedupe_non_empty_trims_and_preserves_order() {
        assert_eq!(
            dedupe_non_empty(vec![
                " alpha ".to_string(),
                "".to_string(),
                "alpha".to_string(),
                "beta".to_string(),
            ]),
            vec!["alpha".to_string(), "beta".to_string()]
        );
    }

    #[test]
    fn combined_slice_reports_zero_first_available_offset() {
        // Terminal/persisted output is never evicted, so the earliest readable
        // byte is always 0.
        let slice = read_combined_output_slice("hello world", 0, 1024);
        assert_eq!(slice.first_available_offset, 0);
        assert_eq!(slice.output, "hello world");
        assert_eq!(slice.total_bytes, 11);
        assert!(!slice.has_more);
    }

    #[test]
    fn retained_slice_surfaces_dropped_prefix() {
        // Simulate a live ring buffer that produced 1000 bytes but retains only
        // the last 4 (tail): first_offset = 996, total_bytes_seen = 1000.
        let slice = read_retained_output_slice("tail", 996, 1000, 0, 1024);
        // A read from offset 0 is clamped forward to the earliest retained
        // byte; first_available_offset (996) exceeding the requested offset (0)
        // is how a caller detects bytes [0, 996) were produced then evicted.
        assert_eq!(slice.first_available_offset, 996);
        assert_eq!(slice.output, "tail");
        assert_eq!(slice.next_offset, 1000);
        assert_eq!(slice.total_bytes, 1000);
        assert!(!slice.has_more);
    }

    /// Drives the Lean `tool_output_paging_cases` (#937) through the real
    /// `read_retained_output_slice`. The rows are computed from the Lean
    /// `Subagent.ToolOutput.readSlice` model, so paging drift in either
    /// direction (model or implementation) fails here. ASCII payloads keep
    /// byte and UTF-8 character boundaries identical, so the Rust boundary
    /// snapping is inert for these rows.
    #[test]
    fn generated_tool_output_paging_cases_match_slice_function() {
        let cases = crate::lean_vocab_test::lean_tool_output_paging_cases();
        assert_eq!(cases.len(), 5, "Lean tool-output paging family drifted");

        for case in cases {
            let retained = "x".repeat(case.retained_len as usize);
            let slice = read_retained_output_slice(
                &retained,
                case.first_offset,
                case.total_bytes,
                case.offset,
                case.max_bytes as usize,
            );
            assert_eq!(
                slice.output.len() as u64,
                case.slice_len,
                "paging case {} returned the wrong slice length",
                case.name
            );
            assert_eq!(
                slice.next_offset, case.next_offset,
                "paging case {} continuation cursor drifted",
                case.name
            );
            assert_eq!(
                slice.first_available_offset, case.first_available_offset,
                "paging case {} eviction floor drifted",
                case.name
            );
            assert_eq!(
                slice.total_bytes, case.total_bytes_out,
                "paging case {} total drifted",
                case.name
            );
            assert_eq!(
                slice.has_more, case.has_more,
                "paging case {} has_more drifted",
                case.name
            );
            assert_eq!(
                slice.next_offset,
                case.start + case.slice_len,
                "paging case {} pages must be contiguous from the clamped start",
                case.name
            );
        }
    }
}
