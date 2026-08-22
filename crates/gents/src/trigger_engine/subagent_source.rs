//! Subagent-backed `TriggerSource`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use defra_node::EmbeddedNode;
use serde::Deserialize;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::background_tools::{
    fail_running_subagent_tool_call, load_behavior_allow_cross_deployment,
    load_parent_subagent_authorization, subagent_spawn_denial, subagent_tool_not_allowed_payload,
};
use crate::event_delivery_contract::{EventDeliveryRuntimeContract, EventDeliverySourceContract};
use crate::graphql::escape_graphql_string;
use crate::runtime_snapshot::{ActiveRuntimeSnapshot, ConcurrencyMode, ResolvedTask};
use crate::tool_call_lifecycle::subagent_request::{
    create_subagent_request_with_request_id_and_workspace,
    create_subagent_request_with_trusted_parent_request_id_and_workspace,
};
use crate::tool_call_lifecycle::subagent_workspace::{
    complete_lineage_from_bridge, resolve_child_workspace, ParentWorkspaceStamp,
    SpawnWorkspaceError,
};
use crate::tool_call_lifecycle::{
    AwaitMode, CancelPolicy, FailureClass, IllegalToolCallTransition, ToolCallState,
};
use crate::UpdateSubscriptionSource;

use super::{FireIntent, FireResult, TriggerKind, TriggerSource};

const TOOL_CALL_COLLECTION: &str = "AgentToolCall";
const SUBAGENT_SOURCE_RESCAN_INTERVAL: Duration = Duration::from_secs(5);

pub struct SubagentSource {
    snapshot_rx: watch::Receiver<Arc<ActiveRuntimeSnapshot>>,
    node: Arc<EmbeddedNode>,
    subscription_source: Arc<dyn UpdateSubscriptionSource>,
    subscription: Option<events::Subscription>,
    cancel: CancellationToken,
    collection_id_to_name: HashMap<String, String>,
    processed_tool_calls: HashSet<String>,
    warned_incoherent_tool_calls: HashSet<String>,
    rescan_tick: tokio::time::Interval,
}

#[derive(Debug, Deserialize)]
struct ToolCallRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    #[serde(default)]
    tool_call_key: Option<String>,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    request_doc_id: Option<String>,
    #[serde(default)]
    agent_did: Option<String>,
    tool_call_id: String,
    #[serde(default)]
    tool_name: String,
    #[serde(default)]
    args: String,
    #[serde(default)]
    lifecycle_state: Option<String>,
    #[serde(default)]
    started_at: Option<String>,
    #[serde(default)]
    deadline_at: Option<String>,
    #[serde(default)]
    await_mode: Option<String>,
    #[serde(default)]
    cancel_policy: Option<String>,
    #[serde(default)]
    child_request_id: Option<String>,
    #[serde(default)]
    spawn_target_did: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ToolCallDocIdRow {
    #[serde(rename = "_docID")]
    doc_id: String,
}

impl ToolCallRow {
    fn cancel_policy(&self) -> CancelPolicy {
        self.cancel_policy
            .as_deref()
            .and_then(CancelPolicy::from_persisted)
            .unwrap_or(CancelPolicy::Cascade)
    }
}

#[derive(Debug, Deserialize)]
struct ParentRequestRow {
    request_id: String,
    agent_did: String,
    #[serde(default)]
    subagent_depth: Option<i64>,
    #[serde(default)]
    workspace_id: Option<String>,
    #[serde(default)]
    workspace_authority: Option<String>,
    #[serde(default)]
    workspace_owner_deployment_id: Option<String>,
    #[serde(default)]
    workspace_seal_hash: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ParentTerminalRow {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    lifecycle_state: Option<String>,
}

fn parent_reached_cancel_worthy_terminal(row: &ParentTerminalRow) -> bool {
    matches!(
        row.status.as_deref(),
        Some("error" | "superseded" | "dead" | "interrupted")
    ) || matches!(
        row.lifecycle_state.as_deref(),
        Some("failed" | "superseded" | "dead" | "interrupted")
    )
}

#[derive(Debug, Deserialize)]
struct SpawnArgs {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    agent_did: Option<String>,
    #[serde(alias = "target", alias = "target_behavior_id")]
    behavior_id: String,
    #[serde(alias = "message", alias = "content")]
    prompt: String,
    #[serde(default)]
    deadline: Option<String>,
    #[serde(default)]
    parent_subagent_depth: Option<u32>,
    #[serde(default)]
    workspace: Option<crate::background_tools::SpawnWorkspaceArg>,
    #[serde(default)]
    workspace_id: Option<String>,
    #[serde(default)]
    workspace_authority: Option<String>,
    #[serde(default)]
    workspace_owner_deployment_id: Option<String>,
    #[serde(default)]
    workspace_seal_hash: Option<String>,
}

impl SpawnArgs {
    fn target_name(&self) -> &str {
        self.name
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(&self.behavior_id)
    }
}

fn subagent_source_rescan_tick(interval: Duration) -> tokio::time::Interval {
    let interval = if interval.is_zero() {
        SUBAGENT_SOURCE_RESCAN_INTERVAL
    } else {
        interval
    };
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick
}

impl SubagentSource {
    pub fn new(
        snapshot_rx: watch::Receiver<Arc<ActiveRuntimeSnapshot>>,
        node: Arc<EmbeddedNode>,
        cancel: CancellationToken,
    ) -> Self {
        Self::with_subscription_source(node.clone(), snapshot_rx, node, cancel)
    }

    pub fn with_subscription_source(
        subs: Arc<dyn UpdateSubscriptionSource>,
        snapshot_rx: watch::Receiver<Arc<ActiveRuntimeSnapshot>>,
        node: Arc<EmbeddedNode>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            snapshot_rx,
            node,
            subscription_source: subs,
            subscription: None,
            cancel,
            collection_id_to_name: HashMap::new(),
            processed_tool_calls: HashSet::new(),
            warned_incoherent_tool_calls: HashSet::new(),
            rescan_tick: subagent_source_rescan_tick(SUBAGENT_SOURCE_RESCAN_INTERVAL),
        }
    }

    #[doc(hidden)]
    pub fn with_rescan_interval(mut self, interval: Duration) -> Self {
        self.rescan_tick = subagent_source_rescan_tick(interval);
        self
    }

    fn ensure_subscription(&mut self) {
        if self.subscription.is_none() {
            self.subscription = Some(self.subscription_source.subscribe_updates());
            tracing::info!("subagent source opened global Update subscription");
        }
    }

    async fn resolve_collection_name(&mut self, collection_id: &str) -> Option<String> {
        if let Some(name) = self.collection_id_to_name.get(collection_id) {
            return Some(name.clone());
        }

        let names = match self.node.list_collections() {
            Ok(names) => names,
            Err(error) => {
                tracing::warn!(
                    collection_id = %collection_id,
                    %error,
                    "subagent source failed to list collections; dropping event"
                );
                return None;
            }
        };

        for name in names {
            let def = match self.node.get_collection(&name) {
                Ok(Some(def)) => def,
                Ok(None) => continue,
                Err(error) => {
                    tracing::warn!(
                        collection_name = %name,
                        %error,
                        "subagent source failed to fetch collection definition while resolving id",
                    );
                    continue;
                }
            };
            self.collection_id_to_name
                .insert(def.collection_id.clone(), def.name.clone());
        }

        self.collection_id_to_name.get(collection_id).cloned()
    }

    async fn load_tool_call(&self, doc_id: &str) -> anyhow::Result<Option<ToolCallRow>> {
        let escaped_doc_id = escape_graphql_string(doc_id);
        let query = format!(
            r#"{{
                AgentToolCall(
                    filter: {{ _docID: {{ _eq: "{escaped_doc_id}" }} }},
                    limit: 1
                ) {{
                    _docID
                    tool_call_key
                    request_id
                    request_doc_id
                    agent_did
                    tool_call_id
                    tool_name
                    args
                    lifecycle_state
                    started_at
                    deadline_at
                    await_mode
                    cancel_policy
                    child_request_id
                    spawn_target_did
                }}
            }}"#
        );
        let response = self.node.execute(&query).await;
        if response.has_errors() {
            anyhow::bail!(
                "query AgentToolCall for SubagentSource failed: {:?}",
                response.errors
            );
        }
        let rows: Vec<ToolCallRow> = response
            .data
            .as_ref()
            .and_then(|data| data.get(TOOL_CALL_COLLECTION))
            .and_then(|value| serde_json::from_value(value.clone()).ok())
            .unwrap_or_default();
        Ok(rows.into_iter().next())
    }

    async fn load_parent_request(
        &self,
        request_doc_id: &str,
    ) -> anyhow::Result<Option<ParentRequestRow>> {
        let escaped_request_doc_id = escape_graphql_string(request_doc_id);
        let query = format!(
            r#"{{
                AgentRequest(
                    filter: {{ _docID: {{ _eq: "{escaped_request_doc_id}" }} }},
                    limit: 1
                ) {{
                    request_id
                    agent_did
                    subagent_depth
                    workspace_id
                    workspace_authority
                    workspace_owner_deployment_id
                    workspace_seal_hash
                }}
            }}"#
        );
        let response = self.node.execute(&query).await;
        if response.has_errors() {
            anyhow::bail!(
                "query parent AgentRequest for SubagentSource failed: {:?}",
                response.errors
            );
        }
        let rows: Vec<ParentRequestRow> = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(|value| serde_json::from_value(value.clone()).ok())
            .unwrap_or_default();
        Ok(rows.into_iter().next())
    }

    async fn load_parent_terminal(
        &self,
        request_doc_id: &str,
    ) -> anyhow::Result<Option<ParentTerminalRow>> {
        let escaped_request_doc_id = escape_graphql_string(request_doc_id);
        let query = format!(
            r#"{{
                AgentRequest(
                    filter: {{ _docID: {{ _eq: "{escaped_request_doc_id}" }} }},
                    limit: 1
                ) {{
                    status
                    lifecycle_state
                }}
            }}"#
        );
        let response = self.node.execute(&query).await;
        if response.has_errors() {
            anyhow::bail!(
                "query parent AgentRequest terminal state for SubagentSource failed: {:?}",
                response.errors
            );
        }
        let rows: Vec<ParentTerminalRow> = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(|value| serde_json::from_value(value.clone()).ok())
            .unwrap_or_default();
        Ok(rows.into_iter().next())
    }

    async fn child_request_exists(&self, request_id: &str) -> anyhow::Result<bool> {
        let escaped_request_id = escape_graphql_string(request_id);
        let query = format!(
            r#"{{
                AgentRequest(
                    filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                    limit: 1
                ) {{ _docID }}
            }}"#
        );
        let response = self.node.execute(&query).await;
        if response.has_errors() {
            anyhow::bail!(
                "query child AgentRequest for SubagentSource failed: {:?}",
                response.errors
            );
        }
        Ok(response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(serde_json::Value::as_array)
            .is_some_and(|rows| !rows.is_empty()))
    }

    async fn load_running_bridge_doc_ids(&self) -> anyhow::Result<Vec<String>> {
        let query = r#"{
            AgentToolCall(
                filter: {
                    lifecycle_state: { _eq: "running" },
                    child_request_id: { _ne: "" }
                }
            ) { _docID }
        }"#;
        let response = self.node.execute(query).await;
        if response.has_errors() {
            anyhow::bail!(
                "query running AgentToolCall bridge rows for SubagentSource rescan failed: {:?}",
                response.errors
            );
        }
        let rows: Vec<ToolCallDocIdRow> = response
            .data
            .as_ref()
            .and_then(|data| data.get(TOOL_CALL_COLLECTION))
            .and_then(|value| serde_json::from_value(value.clone()).ok())
            .unwrap_or_default();
        Ok(rows.into_iter().map(|row| row.doc_id).collect())
    }

    async fn rescan_running_bridge_rows(&mut self) -> Option<FireIntent> {
        let doc_ids = match self.load_running_bridge_doc_ids().await {
            Ok(doc_ids) => doc_ids,
            Err(error) => {
                tracing::warn!(
                    %error,
                    "subagent source periodic rescan failed to load running bridge rows",
                );
                return None;
            }
        };

        for doc_id in doc_ids {
            match self.build_intent_for_tool_call_doc(&doc_id).await {
                Ok(Some(intent)) => {
                    tracing::info!(
                        doc_id = %doc_id,
                        "subagent source periodic rescan emitted fire intent",
                    );
                    return Some(intent);
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(
                        doc_id = %doc_id,
                        %error,
                        "subagent source periodic rescan failed to process AgentToolCall row",
                    );
                }
            }
        }
        None
    }

    async fn fail_unauthorized_tool_call(
        &self,
        row: &ToolCallRow,
        path: &str,
        requested: &str,
        message: impl Into<String>,
        allowed_targets: &[String],
    ) -> anyhow::Result<bool> {
        let tool_name = non_empty(Some(&row.tool_name)).unwrap_or("spawn_subagent");
        let payload =
            subagent_tool_not_allowed_payload(tool_name, path, requested, message, allowed_targets);
        fail_running_subagent_tool_call(
            &self.node,
            &row.doc_id,
            row.started_at.as_deref(),
            row.deadline_at.as_deref(),
            &payload,
            FailureClass::ServiceUnavailable,
        )
        .await
    }

    async fn fail_workspace_tool_call(
        &self,
        row: &ToolCallRow,
        error: SpawnWorkspaceError,
    ) -> anyhow::Result<bool> {
        fail_running_subagent_tool_call(
            &self.node,
            &row.doc_id,
            row.started_at.as_deref(),
            row.deadline_at.as_deref(),
            &error.payload(),
            error.class,
        )
        .await
    }

    async fn build_intent_for_tool_call_doc(
        &mut self,
        doc_id: &str,
    ) -> anyhow::Result<Option<FireIntent>> {
        let Some(row) = self.load_tool_call(doc_id).await? else {
            return Ok(None);
        };
        let child_request_id = match non_empty(row.child_request_id.as_deref()) {
            Some(value) => value.to_string(),
            None => return Ok(None),
        };
        if row.lifecycle_state.as_deref() != Some("running") {
            return Ok(None);
        }

        let processed_key = row
            .tool_call_key
            .clone()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| row.doc_id.clone());
        if self.processed_tool_calls.contains(&processed_key) {
            return Ok(None);
        }

        let parent_request_id = match non_empty(row.request_id.as_deref()) {
            Some(value) => value.to_string(),
            None => return Ok(None),
        };
        let parent_request_doc_id = match non_empty(row.request_doc_id.as_deref()) {
            Some(value) => value.to_string(),
            None => match crate::request_binding::resolve_request_doc_id(
                self.node.as_ref(),
                &parent_request_id,
            )
            .await
            {
                Ok(Some(doc_id)) => {
                    tracing::warn!(
                        doc_id = %row.doc_id,
                        parent_request_id = %parent_request_id,
                        parent_request_doc_id = %doc_id,
                        tool_call_id = %row.tool_call_id,
                        "subagent source recovered legacy logical-only request binding",
                    );
                    doc_id
                }
                Ok(None) => {
                    if self
                        .warned_incoherent_tool_calls
                        .insert(processed_key.clone())
                    {
                        tracing::warn!(
                            doc_id = %row.doc_id,
                            parent_request_id = %parent_request_id,
                            tool_call_id = %row.tool_call_id,
                            "subagent source quarantined AgentToolCall whose parent request is not visible",
                        );
                    }
                    return Ok(None);
                }
                Err(error) => {
                    if self
                        .warned_incoherent_tool_calls
                        .insert(processed_key.clone())
                    {
                        tracing::warn!(
                            doc_id = %row.doc_id,
                            parent_request_id = %parent_request_id,
                            tool_call_id = %row.tool_call_id,
                            %error,
                            "subagent source could not resolve legacy logical-only request binding",
                        );
                    }
                    return Ok(None);
                }
            },
        };
        let parent_tool_call_id = match non_empty(Some(&row.tool_call_id)) {
            Some(value) => value.to_string(),
            None => return Ok(None),
        };
        let spawn_args: SpawnArgs = serde_json::from_str(&row.args)?;
        let row_spawn_target_did =
            non_empty(row.spawn_target_did.as_deref()).map(ToOwned::to_owned);
        let args_target_did = non_empty(spawn_args.agent_did.as_deref()).map(ToOwned::to_owned);
        if let (Some(row_did), Some(args_did)) = (&row_spawn_target_did, &args_target_did) {
            if row_did != args_did {
                let failed = self
                    .fail_unauthorized_tool_call(
                        &row,
                        "/agent_did",
                        args_did,
                        "subagent target DID args do not match immutable spawn_target_did",
                        &[],
                    )
                    .await?;
                self.processed_tool_calls.insert(processed_key);
                tracing::warn!(
                    parent_request_id = %parent_request_id,
                    parent_tool_call_id = %parent_tool_call_id,
                    spawn_target_did = %row_did,
                    args_agent_did = %args_did,
                    failed_tool_call = failed,
                    "subagent source rejected spawn with mismatched target DID fields",
                );
                return Ok(None);
            }
        }
        let resolved_target_did = row_spawn_target_did.clone().or(args_target_did);
        let await_mode = row
            .await_mode
            .as_deref()
            .and_then(AwaitMode::from_persisted)
            .unwrap_or(AwaitMode::Foreground);

        let parent = self.load_parent_request(&parent_request_doc_id).await?;
        if parent
            .as_ref()
            .is_some_and(|parent| parent.request_id != parent_request_id)
        {
            anyhow::bail!(IllegalToolCallTransition::ParentLinkageIncoherent);
        }
        let snapshot = self.snapshot_rx.borrow().clone();
        // SECURITY (#377): under the current replication-trust posture this
        // self-declared bridge DID is trusted once it names a configured paired
        // peer; ACP signing must eventually bind it to the actual remote author.
        let bridge_authoring_did = non_empty(row.agent_did.as_deref()).map(ToOwned::to_owned);
        if let (Some(bridge_did), Some(parent_did)) = (
            bridge_authoring_did.as_deref(),
            parent.as_ref().map(|parent| parent.agent_did.as_str()),
        ) {
            // A paired-peer bridge is a remote authority boundary, so its DID
            // must agree with any legacy parent replica still present. Local
            // legacy rows predate that invariant and keep using the parent row
            // as their authority.
            if bridge_did != parent_did
                && (snapshot.paired_peer_dids.contains(bridge_did)
                    || snapshot.paired_peer_dids.contains(parent_did))
            {
                anyhow::bail!(IllegalToolCallTransition::ParentLinkageIncoherent);
            }
        }
        let parent_authoring_did = parent
            .as_ref()
            .map(|parent| parent.agent_did.clone())
            .or(bridge_authoring_did)
            .ok_or(IllegalToolCallTransition::ParentLinkageIncoherent)?;
        let trusted_paired_peer = snapshot.paired_peer_dids.contains(&parent_authoring_did);
        if parent.is_none() && !trusted_paired_peer {
            anyhow::bail!(IllegalToolCallTransition::ParentLinkageIncoherent);
        }
        let tool_name = non_empty(Some(&row.tool_name)).unwrap_or("spawn_subagent");
        if trusted_paired_peer {
            let local_did = snapshot.local_did.trim();
            let Some(spawn_target_did) = row_spawn_target_did.as_deref() else {
                tracing::debug!(
                    parent_request_id = %parent_request_id,
                    parent_tool_call_id = %parent_tool_call_id,
                    parent_authoring_did = %parent_authoring_did,
                    local_did = %local_did,
                    "subagent source skipping trusted spawn: immutable spawn target is missing",
                );
                return Ok(None);
            };
            if local_did.is_empty() || spawn_target_did != local_did {
                tracing::debug!(
                    parent_request_id = %parent_request_id,
                    parent_tool_call_id = %parent_tool_call_id,
                    parent_authoring_did = %parent_authoring_did,
                    spawn_target_did = %spawn_target_did,
                    local_did = %local_did,
                    "subagent source skipping trusted spawn: immutable spawn target is not this host DID",
                );
                return Ok(None);
            }
            let allow_cross_deployment = match load_behavior_allow_cross_deployment(
                &self.node,
                &spawn_args.behavior_id,
            )
            .await
            {
                Ok(value) => value,
                Err(error) => {
                    tracing::warn!(
                        parent_request_id = %parent_request_id,
                        parent_authoring_did = %parent_authoring_did,
                        target_behavior_id = %spawn_args.behavior_id,
                        %error,
                        "subagent source could not load target behavior cross-deployment flag; refusing cross-deployment child",
                    );
                    return Ok(None);
                }
            };
            if !allow_cross_deployment {
                tracing::warn!(
                    parent_request_id = %parent_request_id,
                    parent_authoring_did = %parent_authoring_did,
                    target_behavior_id = %spawn_args.behavior_id,
                    "cross-deployment child refused: subagent_allow_cross_deployment is off for target behavior {behavior_id}",
                    behavior_id = spawn_args.behavior_id,
                );
                return Ok(None);
            }
            tracing::debug!(
                parent_request_id = %parent_request_id,
                parent_authoring_did = %parent_authoring_did,
                "subagent source claiming cross-deployment spawn from paired peer",
            );
        } else {
            let authorization = match load_parent_subagent_authorization(
                &self.node,
                &parent_request_id,
            )
            .await
            {
                Ok(authorization) => authorization,
                Err(error) => {
                    let failed = self
                        .fail_unauthorized_tool_call(
                            &row,
                            "/name",
                            spawn_args.target_name(),
                            "subagent authorization could not be verified for this behavior",
                            &[],
                        )
                        .await?;
                    self.processed_tool_calls.insert(processed_key);
                    tracing::warn!(
                        parent_request_id = %parent_request_id,
                        parent_tool_call_id = %parent_tool_call_id,
                        target_name = %spawn_args.target_name(),
                        failed_tool_call = failed,
                        %error,
                        "subagent source could not verify parent subagent authorization; rejecting spawn",
                    );
                    return Ok(None);
                }
            };
            if let Some(denial) = subagent_spawn_denial(
                &authorization,
                spawn_args.target_name(),
                await_mode,
                tool_name,
                snapshot.local_did.as_str(),
            ) {
                let failed = self
                    .fail_unauthorized_tool_call(
                        &row,
                        denial.path,
                        &denial.requested,
                        denial.message,
                        &authorization.allowed_target_names(),
                    )
                    .await?;
                self.processed_tool_calls.insert(processed_key);
                tracing::warn!(
                    parent_request_id = %parent_request_id,
                    parent_behavior_id = %authorization.behavior_id,
                    parent_tool_call_id = %parent_tool_call_id,
                    target_name = %spawn_args.target_name(),
                    await_mode = %await_mode.as_str(),
                    failed_tool_call = failed,
                    "subagent source rejected unauthorized subagent spawn",
                );
                return Ok(None);
            }
        }

        if snapshot.behavior(&spawn_args.behavior_id).is_none() {
            tracing::warn!(
                parent_request_id = %parent_request_id,
                parent_tool_call_id = %parent_tool_call_id,
                target_name = %spawn_args.target_name(),
                target_behavior_id = %spawn_args.behavior_id,
                "subagent source target behavior is not in the active runtime snapshot; skipping spawn",
            );
            return Ok(None);
        }

        if !trusted_paired_peer {
            let local_did = snapshot.local_did.trim();
            let target_owner_did = resolved_target_did
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| parent_authoring_did.trim());
            if local_did.is_empty() || target_owner_did != local_did {
                tracing::debug!(
                    parent_request_id = %parent_request_id,
                    parent_tool_call_id = %parent_tool_call_id,
                    target_name = %spawn_args.target_name(),
                    target_owner_did = %target_owner_did,
                    local_did = %local_did,
                    "subagent source skipping spawn: this node does not own the target DID (single-creator gate)",
                );
                return Ok(None);
            }
        }

        if self.child_request_exists(&child_request_id).await? {
            self.processed_tool_calls.insert(processed_key);
            return Ok(None);
        }

        let row_parent_depth = parent
            .as_ref()
            .and_then(|parent| parent.subagent_depth)
            .and_then(|depth| u32::try_from(depth).ok());
        if let (Some(bridge_depth), Some(row_depth)) =
            (spawn_args.parent_subagent_depth, row_parent_depth)
        {
            if bridge_depth != row_depth {
                anyhow::bail!(IllegalToolCallTransition::ParentLinkageIncoherent);
            }
        }
        let parent_depth = spawn_args
            .parent_subagent_depth
            .or(row_parent_depth)
            .or_else(|| parent.as_ref().map(|_| 0))
            .ok_or(IllegalToolCallTransition::ParentLinkageIncoherent)?;
        let deadline =
            effective_deadline(row.deadline_at.as_deref(), spawn_args.deadline.as_deref());
        let child_agent_did = if trusted_paired_peer && !snapshot.local_did.trim().is_empty() {
            snapshot.local_did.clone()
        } else {
            resolved_target_did.unwrap_or_else(|| parent_authoring_did.clone())
        };
        let parent_workspace = ParentWorkspaceStamp::from_fields(
            parent.as_ref().and_then(|row| row.workspace_id.as_deref()),
            parent
                .as_ref()
                .and_then(|row| row.workspace_authority.as_deref()),
            parent
                .as_ref()
                .and_then(|row| row.workspace_owner_deployment_id.as_deref()),
            parent
                .as_ref()
                .and_then(|row| row.workspace_seal_hash.as_deref()),
        );
        let operator_tool_root = crate::workspace::process_operator_tool_root();
        let workspace = match resolve_child_workspace(
            &self.node,
            &parent_workspace,
            spawn_args.workspace.as_ref(),
            complete_lineage_from_bridge(
                spawn_args.workspace_id.as_deref(),
                spawn_args.workspace_authority.as_deref(),
                spawn_args.workspace_owner_deployment_id.as_deref(),
                spawn_args.workspace_seal_hash.as_deref(),
            ),
            &child_agent_did,
            &parent_tool_call_id,
            &parent_request_id,
            operator_tool_root.as_deref(),
        )
        .await
        {
            Ok(lineage) => lineage,
            Err(error) => {
                let failed = self.fail_workspace_tool_call(&row, error).await?;
                self.processed_tool_calls.insert(processed_key);
                tracing::warn!(
                    parent_request_id = %parent_request_id,
                    parent_tool_call_id = %parent_tool_call_id,
                    failed_tool_call = failed,
                    "subagent source rejected spawn because workspace could not be resolved"
                );
                return Ok(None);
            }
        };
        let request_id = if trusted_paired_peer {
            create_subagent_request_with_trusted_parent_request_id_and_workspace(
                &self.node,
                child_request_id.clone(),
                parent_request_id.clone(),
                parent_request_doc_id.clone(),
                parent_tool_call_id.clone(),
                row.doc_id.clone(),
                parent_depth,
                child_agent_did,
                spawn_args.behavior_id.clone(),
                spawn_args.prompt.clone(),
                deadline,
                parent_authoring_did.clone(),
                workspace,
            )
            .await?
        } else {
            create_subagent_request_with_request_id_and_workspace(
                &self.node,
                child_request_id.clone(),
                parent_request_id.clone(),
                parent_request_doc_id.clone(),
                parent_tool_call_id.clone(),
                row.doc_id.clone(),
                parent_depth,
                child_agent_did,
                spawn_args.behavior_id.clone(),
                spawn_args.prompt.clone(),
                deadline,
                workspace,
            )
            .await?
        };

        // Orphan-child-escapes-cancel race (audit Finding 1). The parent may have
        // been cancelled/interrupted in the window between the spawn hook writing
        // the `running` bridge and this child create. The cascade's
        // `interrupt_request(child_request_id)` would have no-oped because the
        // child did not exist yet, so we re-check AFTER the create and interrupt
        // the just-created child if a genuine cancel signal is present.
        //
        // CRUCIALLY, this re-check must be consistent with the live cascade
        // (`transition/bridge.rs::bridge_cancel_cascade`) and the recovery cascade
        // (`recovery.rs::cascade_child_request_id`): BOTH gate the child interrupt
        // on `cancel_policy == Cascade` and refuse to cascade for detached
        // children (`if self.cancel_policy != CancelPolicy::Cascade { return None }`
        // / `cascade_child_request_id` returns `None` unless Cascade). A
        // DETACHED/background-detached child outlives its parent. So we ONLY
        // interrupt when the bridge policy is Cascade AND a real cancel signal is
        // present. A parent that completed NORMALLY is NOT a cancel signal — a
        // cleanly-completed parent never cascade-cancels its tools anywhere else.
        let bridge_cancel_policy = row.cancel_policy();
        if bridge_cancel_policy != CancelPolicy::Cascade {
            tracing::debug!(
                child_request_id = %request_id,
                parent_request_id = %parent_request_id,
                cancel_policy = bridge_cancel_policy.as_str(),
                "subagent source: detached child, skipping orphan cancel re-check (child outlives parent)",
            );
        } else {
            let bridge_cancelled = match self.load_tool_call(doc_id).await {
                Ok(Some(latest)) => {
                    latest.lifecycle_state.as_deref() == Some(ToolCallState::Cancelled.as_str())
                }
                Ok(None) => true,
                Err(error) => {
                    tracing::warn!(
                        child_request_id = %request_id,
                        %error,
                        "subagent source failed to re-read bridge lifecycle after child create; assuming not cancelled",
                    );
                    false
                }
            };
            let parent_interrupted = if parent.is_some() {
                match crate::interrupt::fetch_interrupt_requested_at(&self.node, &parent_request_id)
                    .await
                {
                    Ok(value) => value.is_some(),
                    Err(error) => {
                        tracing::warn!(
                            parent_request_id = %parent_request_id,
                            %error,
                            "subagent source failed to re-read parent interrupt latch after child create; assuming not interrupted",
                        );
                        false
                    }
                }
            } else {
                false
            };
            let parent_cancel_worthy_terminal = if parent.is_some() {
                match self.load_parent_terminal(&parent_request_doc_id).await {
                    Ok(Some(row)) => parent_reached_cancel_worthy_terminal(&row),
                    Ok(None) => true,
                    Err(error) => {
                        tracing::warn!(
                            parent_request_id = %parent_request_id,
                            %error,
                            "subagent source failed to re-read parent terminal state after child create; assuming not terminal",
                        );
                        false
                    }
                }
            } else {
                false
            };
            if bridge_cancelled || parent_interrupted || parent_cancel_worthy_terminal {
                tracing::info!(
                    child_request_id = %request_id,
                    parent_request_id = %parent_request_id,
                    bridge_cancelled,
                    parent_interrupted,
                    parent_cancel_worthy_terminal,
                    "subagent source: Cascade bridge with real cancel signal in materialize window; interrupting just-created orphan child",
                );
                if let Err(error) =
                    crate::interrupt::interrupt_request(&self.node, &request_id).await
                {
                    tracing::warn!(
                        child_request_id = %request_id,
                        %error,
                        "subagent source failed to interrupt orphaned child after cancel-before-materialize race",
                    );
                }
            }
        }

        self.processed_tool_calls.insert(processed_key);
        let fired_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
        let task = ResolvedTask {
            task_id: format!("subagent:{parent_tool_call_id}"),
            name: Some(format!(
                "Subagent {target}",
                target = spawn_args.behavior_id
            )),
            behavior_id: spawn_args.behavior_id,
            prompt_template: spawn_args.prompt,
            output_schema_ref: None,
        };
        let event_vars = serde_json::json!({
            "fired_at": fired_at,
            "trigger_id": parent_tool_call_id,
            "trigger_kind": "subagent",
            "parent_request_id": parent_request_id,
            "child_request_id": request_id,
        });
        Ok(Some(FireIntent {
            trigger_id: None,
            trigger_kind: TriggerKind::Manual,
            task,
            concurrency: ConcurrencyMode::Parallel,
            event_vars,
            doc_vars: None,
            correlation: None,
            group_vars: None,
            trigger_context: None,
            args_vars: None,
            pre_materialized_request_id: Some(request_id),
            on_result: Box::new(move |result| match result {
                FireResult::Fired { request_id } => {
                    tracing::debug!(
                        child_request_id = %request_id,
                        "subagent source reported pre-materialized child request fired"
                    );
                }
                FireResult::Skipped { reason } => {
                    tracing::warn!(%reason, "subagent source pre-materialized fire skipped");
                }
                FireResult::Errored { error } => {
                    tracing::warn!(%error, "subagent source pre-materialized fire errored");
                }
            }),
        }))
    }
}

impl EventDeliveryRuntimeContract for SubagentSource {
    const EVENT_DELIVERY_CONTRACT: EventDeliverySourceContract = EventDeliverySourceContract {
        name: "SubagentSource",
        dedupe_policy: "monotone_once",
        rescan_bounded_by: 1,
        deviation: None,
    };
}

impl TriggerSource for SubagentSource {
    fn next_fire(
        &mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<FireIntent>> + Send + '_>> {
        Box::pin(async move {
            self.ensure_subscription();
            loop {
                let mut message = None;
                let mut dropped = 0;
                let rescan_due = {
                    let subscription = self
                        .subscription
                        .as_mut()
                        .expect("subagent source subscription opened before polling");
                    let rescan_due = tokio::select! {
                        biased;
                        _ = self.cancel.cancelled() => return None,
                        res = self.snapshot_rx.changed() => {
                            if res.is_err() {
                                return None;
                            }
                            continue;
                        }
                        _ = self.rescan_tick.tick() => true,
                        msg = subscription.recv() => {
                            match msg {
                                Some(received) => {
                                    message = Some(received);
                                    false
                                }
                                None => {
                                    tracing::warn!(
                                        "subagent source subscription channel closed; source exiting",
                                    );
                                    return None;
                                }
                            }
                        }
                    };
                    if !rescan_due {
                        dropped = subscription.check_and_reset_dropped();
                    }
                    rescan_due
                };
                if rescan_due {
                    if let Some(intent) = self.rescan_running_bridge_rows().await {
                        return Some(intent);
                    }
                    continue;
                }
                let message = message.expect("subscription recv branch sets message");

                if dropped > 0 {
                    tracing::warn!(
                        dropped,
                        "subagent source dropped messages; periodic rescan will recover child spawns",
                    );
                }

                let Some(update) = message.as_update() else {
                    continue;
                };
                let collection_id = update.collection_id.clone();
                let doc_id = update.doc_id.clone();
                let Some(collection_name) = self.resolve_collection_name(&collection_id).await
                else {
                    continue;
                };
                if collection_name != TOOL_CALL_COLLECTION {
                    continue;
                }

                match self.build_intent_for_tool_call_doc(&doc_id).await {
                    Ok(Some(intent)) => return Some(intent),
                    Ok(None) => continue,
                    Err(error) => {
                        tracing::warn!(
                            doc_id = %doc_id,
                            %error,
                            "subagent source failed to process AgentToolCall event",
                        );
                        continue;
                    }
                }
            }
        })
    }
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then_some(trimmed)
    })
}

fn parse_deadline(value: Option<&str>) -> Option<DateTime<Utc>> {
    non_empty(value)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
}

fn effective_deadline(
    tool_deadline: Option<&str>,
    args_deadline: Option<&str>,
) -> Option<DateTime<Utc>> {
    match (parse_deadline(tool_deadline), parse_deadline(args_deadline)) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}
