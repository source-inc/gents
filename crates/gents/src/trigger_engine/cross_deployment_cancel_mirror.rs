//! Cross-deployment cascade-cancel mirror observer.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Result;
use defra_node::{EmbeddedNode, EventName};
use serde::Deserialize;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::graphql::escape_graphql_string;
use crate::runtime_snapshot::ActiveRuntimeSnapshot;

const TOOL_CALL_COLLECTION: &str = "AgentToolCall";
const AGENT_REQUEST_COLLECTION: &str = "AgentRequest";

pub(crate) async fn run_cross_deployment_cancel_mirror(
    node: Arc<EmbeddedNode>,
    snapshot_rx: watch::Receiver<Arc<ActiveRuntimeSnapshot>>,
    cancel: CancellationToken,
) -> Result<()> {
    CrossDeploymentCancelMirror::new(node, snapshot_rx, cancel)
        .run()
        .await
}

pub(crate) struct CrossDeploymentCancelMirror {
    node: Arc<EmbeddedNode>,
    snapshot_rx: watch::Receiver<Arc<ActiveRuntimeSnapshot>>,
    subscription: events::Subscription,
    cancel: CancellationToken,
    collection_id_to_name: HashMap<String, String>,
    mirrored: HashSet<String>,
}

impl CrossDeploymentCancelMirror {
    pub(crate) fn new(
        node: Arc<EmbeddedNode>,
        snapshot_rx: watch::Receiver<Arc<ActiveRuntimeSnapshot>>,
        cancel: CancellationToken,
    ) -> Self {
        let subscription = node.subscribe(&[EventName::Update]);
        Self {
            node,
            snapshot_rx,
            subscription,
            cancel,
            collection_id_to_name: HashMap::new(),
            mirrored: HashSet::new(),
        }
    }

    pub(crate) async fn run(mut self) -> Result<()> {
        self.scan_pending_intents().await?;
        let mut retry_tick = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            let message = tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return Ok(()),
                _ = retry_tick.tick() => {
                    self.scan_pending_intents().await?;
                    continue;
                }
                changed = self.snapshot_rx.changed() => {
                    if changed.is_err() {
                        return Ok(());
                    }
                    self.scan_pending_intents().await?;
                    continue;
                }
                msg = self.subscription.recv() => {
                    match msg {
                        Some(message) => message,
                        None => anyhow::bail!("cross-deployment cancel mirror subscription channel closed"),
                    }
                }
            };

            let dropped = self.subscription.check_and_reset_dropped();
            if dropped > 0 {
                tracing::warn!(
                    dropped,
                    "cancel mirror dropped messages; scanning pending cancel intents"
                );
                self.scan_pending_intents().await?;
            }

            let Some(update) = message.as_update() else {
                continue;
            };
            let Some(collection_name) = self.resolve_collection_name(&update.collection_id).await
            else {
                continue;
            };
            match collection_name.as_str() {
                TOOL_CALL_COLLECTION => {
                    if let Err(error) = self.handle_tool_call_doc(&update.doc_id).await {
                        tracing::warn!(
                            doc_id = %update.doc_id,
                            %error,
                            "cancel mirror failed to handle AgentToolCall update"
                        );
                    }
                }
                AGENT_REQUEST_COLLECTION => {
                    self.scan_pending_intents().await?;
                }
                _ => {}
            }
        }
    }

    pub(crate) async fn scan_pending_intents(&mut self) -> Result<()> {
        for doc_id in self.load_pending_intent_doc_ids().await? {
            if let Err(error) = self.handle_tool_call_doc(&doc_id).await {
                tracing::warn!(
                    doc_id = %doc_id,
                    %error,
                    "cancel mirror failed to scan pending cancel intent"
                );
            }
        }
        Ok(())
    }

    async fn handle_tool_call_doc(&mut self, doc_id: &str) -> Result<()> {
        let Some(row) = self.load_bridge_row(doc_id).await? else {
            return Ok(());
        };
        let Some(intent_at) = row
            .cancel_cascade_intent_at
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
        else {
            return Ok(());
        };
        let Some(child_request_id) = row
            .child_request_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
        else {
            return Ok(());
        };

        let dedupe_key = format!("{}:{intent_at}", row.tool_call_id);
        if self.mirrored.contains(&dedupe_key) {
            return Ok(());
        }

        let bridge_authoring_did = non_empty(row.agent_did.as_deref()).map(ToOwned::to_owned);
        let parent_authoring_did = self.load_parent_authoring_did(&row.request_id).await?;
        if let (Some(bridge_did), Some(parent_did)) = (
            bridge_authoring_did.as_deref(),
            parent_authoring_did.as_deref(),
        ) {
            if bridge_did != parent_did {
                tracing::warn!(
                    bridge_agent_did = %bridge_did,
                    parent_agent_did = %parent_did,
                    parent_request_id = %row.request_id,
                    "cancel mirror rejected incoherent bridge author"
                );
                return Ok(());
            }
        }
        let Some(parent_did) = bridge_authoring_did.or(parent_authoring_did) else {
            return Ok(());
        };
        let snapshot = self.snapshot_rx.borrow().clone();
        if !snapshot.paired_peer_dids.contains(&parent_did) {
            return Ok(());
        }

        let Some(child) = self.load_child_request(&child_request_id).await? else {
            return Ok(());
        };
        if child.agent_did.as_deref() != Some(snapshot.local_did.as_str()) {
            return Ok(());
        }
        if is_terminal_state(child.lifecycle_state.as_deref(), child.status.as_deref())
            || child.interrupt_requested_at.is_some()
        {
            self.mirrored.insert(dedupe_key);
            return Ok(());
        }

        write_child_interrupt_requested_at(self.node.as_ref(), &child_request_id, &intent_at)
            .await?;
        self.mirrored.insert(dedupe_key);
        Ok(())
    }

    async fn load_pending_intent_doc_ids(&self) -> Result<Vec<String>> {
        let query = r#"{
            AgentToolCall(filter: { cancel_pending_remote_ack: { _eq: true } }) {
                _docID
            }
        }"#;
        let response = self.node.execute(query).await;
        if response.has_errors() {
            anyhow::bail!(
                "cancel mirror pending intent query failed: {:?}",
                response.errors
            );
        }
        let rows: Vec<DocIdRow> = response
            .data
            .as_ref()
            .and_then(|d| d.get("AgentToolCall"))
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        Ok(rows.into_iter().map(|row| row.doc_id).collect())
    }

    async fn load_bridge_row(&self, doc_id: &str) -> Result<Option<BridgeCancelRow>> {
        let escaped = escape_graphql_string(doc_id);
        let query = format!(
            r#"{{
                AgentToolCall(
                    filter: {{ _docID: {{ _eq: "{escaped}" }} }},
                    limit: 1
                ) {{
                    request_id
                    agent_did
                    tool_call_id
                    child_request_id
                    cancel_cascade_intent_at
                }}
            }}"#
        );
        let response = self.node.execute(&query).await;
        if response.has_errors() {
            anyhow::bail!("cancel mirror bridge load failed: {:?}", response.errors);
        }
        let rows: Vec<BridgeCancelRow> = response
            .data
            .as_ref()
            .and_then(|d| d.get("AgentToolCall"))
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        Ok(rows.into_iter().next())
    }

    async fn load_parent_authoring_did(&self, parent_request_id: &str) -> Result<Option<String>> {
        let escaped = escape_graphql_string(parent_request_id);
        let query = format!(
            r#"{{
                AgentRequest(
                    filter: {{ request_id: {{ _eq: "{escaped}" }} }},
                    limit: 1
                ) {{ agent_did }}
            }}"#
        );
        let response = self.node.execute(&query).await;
        if response.has_errors() {
            anyhow::bail!(
                "cancel mirror parent DID load failed: {:?}",
                response.errors
            );
        }
        Ok(response
            .data
            .as_ref()
            .and_then(|d| d.get("AgentRequest"))
            .and_then(|v| v.as_array())
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("agent_did"))
            .and_then(|v| v.as_str())
            .map(String::from))
    }

    async fn load_child_request(&self, child_request_id: &str) -> Result<Option<ChildRequestRow>> {
        let escaped = escape_graphql_string(child_request_id);
        let query = format!(
            r#"{{
                AgentRequest(
                    filter: {{ request_id: {{ _eq: "{escaped}" }} }},
                    limit: 1
                ) {{
                    agent_did
                    status
                    lifecycle_state
                    interrupt_requested_at
                }}
            }}"#
        );
        let response = self.node.execute(&query).await;
        if response.has_errors() {
            anyhow::bail!("cancel mirror child load failed: {:?}", response.errors);
        }
        let rows: Vec<ChildRequestRow> = response
            .data
            .as_ref()
            .and_then(|d| d.get("AgentRequest"))
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        Ok(rows.into_iter().next())
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
                    "cancel mirror failed to list collections"
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
                        "cancel mirror failed to fetch collection definition",
                    );
                    continue;
                }
            };
            self.collection_id_to_name
                .insert(def.collection_id.clone(), def.name.clone());
        }
        self.collection_id_to_name.get(collection_id).cloned()
    }
}

#[derive(Debug, Deserialize)]
struct DocIdRow {
    #[serde(rename = "_docID")]
    doc_id: String,
}

#[derive(Debug, Deserialize)]
struct BridgeCancelRow {
    request_id: String,
    #[serde(default)]
    agent_did: Option<String>,
    tool_call_id: String,
    child_request_id: Option<String>,
    cancel_cascade_intent_at: Option<String>,
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then_some(trimmed)
    })
}

#[derive(Debug, Deserialize)]
struct ChildRequestRow {
    agent_did: Option<String>,
    status: Option<String>,
    lifecycle_state: Option<String>,
    interrupt_requested_at: Option<String>,
}

fn is_terminal_state(lifecycle_state: Option<&str>, status: Option<&str>) -> bool {
    matches!(
        lifecycle_state,
        Some("completed" | "failed" | "dead" | "interrupted" | "superseded")
    ) || matches!(
        status,
        Some("completed" | "error" | "dead" | "interrupted" | "superseded")
    )
}

async fn write_child_interrupt_requested_at(
    node: &EmbeddedNode,
    child_request_id: &str,
    when: &str,
) -> Result<()> {
    let escaped_id = escape_graphql_string(child_request_id);
    let escaped_when = escape_graphql_string(when);
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_id}" }} }},
                input: {{ interrupt_requested_at: "{escaped_when}" }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    if response.has_errors() {
        anyhow::bail!(
            "cancel mirror child interrupt write failed: {:?}",
            response.errors
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_snapshot::ActiveRuntimeSnapshot;
    use std::collections::{HashMap, HashSet};
    use tokio::sync::watch;

    const PARENT_DID: &str = "did:test:parent";
    const CHILD_DID: &str = "did:test:child";

    async fn test_node() -> Arc<EmbeddedNode> {
        let node = Arc::new(EmbeddedNode::builder().build().await.unwrap());
        crate::ensure_runtime_schemas(node.as_ref()).await.unwrap();
        node
    }

    async fn exec(node: &EmbeddedNode, statement: &str) {
        let response = node.execute(statement).await;
        assert!(
            !response.has_errors(),
            "GraphQL errors: {:?}",
            response.errors
        );
    }

    fn snapshot() -> Arc<ActiveRuntimeSnapshot> {
        Arc::new(ActiveRuntimeSnapshot {
            generation: 1,
            principal: None,
            local_did: CHILD_DID.to_string(),
            paired_peer_dids: HashSet::from([PARENT_DID.to_string()]),
            default_behavior_id: "child-behavior".to_string(),
            behaviors: HashMap::new(),
            config_provenance_scope:
                crate::rendered_request::ConfigProvenanceScope::StaticOrOneShot,
            behavior_config_provenance: HashMap::new(),
            tool_surfaces: HashMap::new(),
            backend_admission_configs: HashMap::new(),
            unavailable_behaviors: HashMap::new(),
            active_schedules: HashMap::new(),
            unavailable_schedules: HashSet::new(),
            active_event_triggers: HashMap::new(),
            unavailable_event_triggers: HashSet::new(),
            active_tasks: HashMap::new(),
            dispatchers: HashMap::new(),
            behavior_executor_capacities: HashMap::new(),
            behavior_executor_queue_capacities: HashMap::new(),
        })
    }

    async fn write_targeted_bridge_without_parent(node: &EmbeddedNode) {
        exec(
            node,
            r#"mutation {
                create_AgentToolCall(input: {
                    tool_call_key: "parent-request:spawn",
                    request_id: "parent-request",
                    session_id: "parent-session",
                    agent_did: "did:test:parent",
                    message_sequence: 1,
                    tool_name: "spawn_subagent",
                    tool_call_id: "spawn",
                    args: "{}",
                    status: "completed",
                    lifecycle_state: "cancelled",
                    cancel_cause: "interrupted",
                    started_at: "2026-05-15T00:00:00Z",
                    deadline_at: "2026-05-15T00:05:00Z",
                    completed_at: "2026-05-15T00:01:00Z",
                    await_mode: "background",
                    cancel_policy: "cascade",
                    child_request_id: "child-request",
                    cancel_cascade_intent_at: "2026-05-15T00:01:00Z",
                    cancel_pending_remote_ack: true
                }) { _docID }
            }"#,
        )
        .await;
    }

    async fn write_child_request(node: &EmbeddedNode) {
        exec(
            node,
            r#"mutation {
                create_AgentRequest(input: {
                    request_id: "child-request",
                    agent_did: "did:test:child",
                    requester_did: "did:test:parent",
                    behavior_id: "child-behavior",
                    session_id: "child-session",
                    content: "child",
                    status: "processing",
                    lifecycle_state: "processing",
                    created_at: "2026-05-15T00:00:30Z",
                    caused_by_parent_request_id: "parent-request",
                    caused_by_parent_tool_call_id: "spawn"
                }) { _docID }
            }"#,
        )
        .await;
    }

    async fn child_interrupt_requested_at(node: &EmbeddedNode) -> Option<String> {
        let response = node
            .execute(
                r#"{
                    AgentRequest(filter: { request_id: { _eq: "child-request" } }, limit: 1) {
                        interrupt_requested_at
                    }
                }"#,
            )
            .await;
        assert!(
            !response.has_errors(),
            "query errors: {:?}",
            response.errors
        );
        response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(|rows| rows.as_array())
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("interrupt_requested_at"))
            .and_then(|value| value.as_str())
            .map(ToOwned::to_owned)
    }

    #[tokio::test]
    async fn pending_intent_is_retried_after_child_request_arrives() {
        let node = test_node().await;
        write_targeted_bridge_without_parent(node.as_ref()).await;
        let (_tx, rx) = watch::channel(snapshot());
        let cancel = CancellationToken::new();
        let mut mirror = CrossDeploymentCancelMirror::new(node.clone(), rx, cancel);

        mirror.scan_pending_intents().await.unwrap();
        assert!(child_interrupt_requested_at(node.as_ref()).await.is_none());

        write_child_request(node.as_ref()).await;
        mirror.scan_pending_intents().await.unwrap();
        assert_eq!(
            child_interrupt_requested_at(node.as_ref()).await.as_deref(),
            Some("2026-05-15T00:01:00Z")
        );
    }
}
