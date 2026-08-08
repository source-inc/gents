use anyhow::{bail, Result};
use gents::background_completion::{
    observe_cancel_cascade_ack, project_background_subagent_completion,
    reconcile_unclaimed_cross_deployment_spawns,
};
use gents::graphql::escape_graphql_string;
use gents::tool_call_lifecycle::{CancelCause, CascadeDispatch, ToolCallLifecycle};
use gents::{
    interrupt_request, upsert_agent_behavior, upsert_tool_selection, AgentBehaviorDocument,
    RequestLifecycle, ToolSelectionDocument,
};
use serde::Deserialize;
use serde_json::json;

use std::sync::Arc;

use crate::support::{first_optional_row, test_db_with_identity, TestDb};

use super::scenario::{Action, NodeId, Scenario};

const NODE_A_DID: &str = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
const NODE_B_DID: &str = "did:key:z6MkfXG2FkNy3u7Eg3jm8e2YQpGz7Z1JqWgHDAP1hLk9r2bR";

pub struct HarnessNode {
    pub id: NodeId,
    pub db: TestDb,
}

pub struct Harness {
    a: HarnessNode,
    b: HarnessNode,
    history: Vec<Observation>,
}

#[derive(Debug, Clone, Default)]
pub struct Observation {
    pub a_bridge_rows: Vec<BridgeObservation>,
    pub b_bridge_rows: Vec<BridgeObservation>,
    pub a_child_requests: Vec<RequestObservation>,
    pub b_child_requests: Vec<RequestObservation>,
    pub subagent_notifications: Vec<String>,
    pub background_wakeup_keys: Vec<String>,
    pub a_process_generation: u64,
    pub b_process_generation: u64,
    pub crashed_node: Option<NodeId>,
}

impl Observation {
    pub fn child_for_bridge(&self, bridge: &BridgeObservation) -> Option<&RequestObservation> {
        let child_id = bridge.child_request_id.as_ref()?;
        self.a_child_requests
            .iter()
            .chain(self.b_child_requests.iter())
            .find(|child| &child.request_id == child_id)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BridgeObservation {
    pub request_id: String,
    pub session_id: String,
    pub tool_call_id: String,
    pub lifecycle_state: String,
    pub child_request_id: Option<String>,
    pub cancel_cause: Option<String>,
    pub cancel_cascade_intent_at: Option<String>,
    pub cancel_pending_remote_ack: Option<bool>,
    pub stuck_since: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RequestObservation {
    pub request_id: String,
    pub agent_did: String,
    pub lifecycle_state: String,
    pub caused_by_parent_tool_call_id: Option<String>,
    pub interrupt_requested_at: Option<String>,
}

impl RequestObservation {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.lifecycle_state.as_str(),
            "completed" | "failed" | "dead" | "interrupted" | "superseded"
        )
    }
}

impl Harness {
    pub async fn start_two_nodes() -> Result<Self> {
        let a_identity: Arc<dyn gents::AgentIdentity> =
            Arc::new(crate::support::fixtures::test_identity("r5-conformance-a"));
        let b_identity: Arc<dyn gents::AgentIdentity> =
            Arc::new(crate::support::fixtures::test_identity("r5-conformance-b"));
        let a = HarnessNode {
            id: "A".to_string(),
            db: test_db_with_identity("r5-conformance-a", a_identity).await,
        };
        let b = HarnessNode {
            id: "B".to_string(),
            db: test_db_with_identity("r5-conformance-b", b_identity).await,
        };
        let mut harness = Self {
            a,
            b,
            history: Vec::new(),
        };
        harness.record_observation().await?;
        Ok(harness)
    }

    pub async fn run(&mut self, scenario: &Scenario) -> Result<()> {
        let _ = &scenario.name;
        for action in &scenario.actions {
            let crashed = match action {
                Action::Crash { node } => Some(node.clone()),
                _ => None,
            };
            self.apply_action(action).await?;
            self.record_observation_after(crashed).await?;
        }
        Ok(())
    }

    pub fn observation_history(&self) -> Vec<Observation> {
        self.history.clone()
    }

    async fn apply_action(&mut self, action: &Action) -> Result<()> {
        match action {
            Action::OperatorWritePairing {
                node,
                peer,
                collections,
            } => write_pairing(self.node(node)?, peer, collections).await?,
            Action::WriteAgentRequest {
                node,
                request_id,
                agent_did,
                behavior_id,
                state,
                caused_by_parent_request_id,
                caused_by_parent_tool_call_id,
            } => {
                write_agent_request(
                    self.node(node)?,
                    request_id,
                    agent_did,
                    behavior_id,
                    state,
                    caused_by_parent_request_id.as_deref(),
                    caused_by_parent_tool_call_id.as_deref(),
                )
                .await?
            }
            Action::WriteParentToolCall {
                node,
                parent_request_id,
                parent_tool_call_id,
                child_request_id,
                behavior_id,
                unclaimed_deadline_at,
            } => {
                write_parent_tool_call(
                    self.node(node)?,
                    parent_request_id,
                    parent_tool_call_id,
                    child_request_id,
                    behavior_id,
                    unclaimed_deadline_at.as_deref(),
                )
                .await?
            }
            Action::ReplicateDoc {
                from,
                to,
                collection,
                doc_id,
            } => {
                let row = export_doc(self.node(from)?, collection, doc_id).await?;
                import_doc(self.node(to)?, collection, &row).await?;
            }
            Action::TerminalizeChildOnB {
                request_id,
                terminal,
                final_response,
            } => {
                terminalize_child_on_b(&self.b, request_id, terminal, final_response.as_deref())
                    .await?
            }
            Action::CancelParentOnA {
                parent_request_id: _,
                parent_tool_call_id,
            } => cancel_parent_on_a(&self.a, parent_tool_call_id).await?,
            Action::RunBackgroundCompletionObserverOnA => {
                run_background_completion_on_a(&self.a).await?
            }
            Action::RunCancelMirrorObserverOnB => run_cancel_mirror_on_b(&self.b).await?,
            Action::RunUnclaimedSpawnReconcilerOnA => {
                let _ =
                    reconcile_unclaimed_cross_deployment_spawns(self.a.db.node.clone(), NODE_A_DID)
                        .await?;
            }
            Action::RunCancelAckObserverOnA => {
                let _ = observe_cancel_cascade_ack(self.a.db.node.clone(), NODE_A_DID).await?;
            }
            Action::RunRecoverySweepOn { node } => {
                let did = if node == "A" { NODE_A_DID } else { NODE_B_DID };
                let _ =
                    ToolCallLifecycle::recover_all(self.node(node)?.db.node.as_ref(), did).await?;
                let _ =
                    RequestLifecycle::recover_all(self.node(node)?.db.node.as_ref(), did).await?;
            }
            Action::Crash { node } => {
                self.crash_node(node).await?;
            }
            Action::AdvanceClockOn { node, seconds } => {
                advance_r5_clock_effects(self.node(node)?, *seconds).await?;
            }
            Action::WaitForConvergence => {
                self.wait_for_convergence().await?;
            }
        }
        Ok(())
    }

    async fn wait_for_convergence(&mut self) -> Result<()> {
        run_background_completion_on_a(&self.a).await?;
        let _ = observe_cancel_cascade_ack(self.a.db.node.clone(), NODE_A_DID).await?;
        run_cancel_mirror_on_b(&self.b).await?;
        Ok(())
    }

    fn node(&self, id: &NodeId) -> Result<&HarnessNode> {
        if id == &self.a.id {
            Ok(&self.a)
        } else if id == &self.b.id {
            Ok(&self.b)
        } else {
            bail!("unknown node {id}")
        }
    }

    fn node_mut(&mut self, id: &NodeId) -> Result<&mut HarnessNode> {
        if id == &self.a.id {
            Ok(&mut self.a)
        } else if id == &self.b.id {
            Ok(&mut self.b)
        } else {
            bail!("unknown node {id}")
        }
    }

    async fn crash_node(&mut self, id: &NodeId) -> Result<()> {
        let node = self.node_mut(id)?;
        let before = node.db.process_generation;
        node.db
            .simulate_process_crash()
            .await
            .map_err(|e| anyhow::anyhow!("Crash({id}) failed: {e}"))?;
        if node.db.process_generation != before + 1 {
            bail!(
                "Crash({id}): process_generation did not advance ({before} -> {})",
                node.db.process_generation
            );
        }
        tracing::info!(
            node = %id,
            process_generation = node.db.process_generation,
            "R5 harness process crash/reopen completed"
        );
        Ok(())
    }

    async fn record_observation(&mut self) -> Result<()> {
        self.record_observation_after(None).await
    }

    async fn record_observation_after(&mut self, crashed_node: Option<NodeId>) -> Result<()> {
        self.history.push(Observation {
            a_bridge_rows: load_bridge_rows(&self.a).await?,
            b_bridge_rows: load_bridge_rows(&self.b).await?,
            a_child_requests: load_child_requests(&self.a).await?,
            b_child_requests: load_child_requests(&self.b).await?,
            subagent_notifications: load_subagent_notifications(&self.a).await?,
            background_wakeup_keys: load_background_wakeup_keys(&self.a).await?,
            a_process_generation: self.a.db.process_generation,
            b_process_generation: self.b.db.process_generation,
            crashed_node,
        });
        Ok(())
    }
}

async fn write_pairing(node: &HarnessNode, peer: &str, collections: &[String]) -> Result<()> {
    let peer_did = if peer == "A" {
        NODE_A_DID
    } else if peer == "B" {
        NODE_B_DID
    } else {
        peer
    };
    let peer_id = escape_graphql_string(peer);
    let peer_did = escape_graphql_string(peer_did);
    let collections = collections
        .iter()
        .map(|value| format!(r#""{}""#, escape_graphql_string(value)))
        .collect::<Vec<_>>()
        .join(", ");
    let now = chrono::Utc::now().to_rfc3339();
    let mutation = format!(
        r#"mutation {{
            upsert_PeerPairingDesired(
                filter: {{ peer_id: {{ _eq: "{peer_id}" }} }},
                add: {{
                    peer_id: "{peer_id}",
                    agent_did: "{peer_did}",
                    collections: [{collections}],
                    replicator_addresses: [],
                    created_at: "{now}",
                    updated_at: "{now}"
                }},
                update: {{
                    agent_did: "{peer_did}",
                    collections: [{collections}],
                    replicator_addresses: [],
                    updated_at: "{now}"
                }}
            ) {{ _docID }}
        }}"#
    );
    exec(node, &mutation, "write PeerPairingDesired").await
}

async fn write_agent_request(
    node: &HarnessNode,
    request_id: &str,
    agent_did: &str,
    behavior_id: &str,
    state: &str,
    parent_request_id: Option<&str>,
    parent_tool_call_id: Option<&str>,
) -> Result<()> {
    ensure_behavior(node, behavior_id, agent_did).await?;

    let request_id = escape_graphql_string(request_id);
    let agent_did = escape_graphql_string(agent_did);
    let behavior_id = escape_graphql_string(behavior_id);
    let session_id = escape_graphql_string(&format!("{request_id}-session"));
    let status = status_for_lifecycle(state);
    let now = chrono::Utc::now().to_rfc3339();
    let deadline = (chrono::Utc::now() + chrono::Duration::minutes(30)).to_rfc3339();
    let parent_request_field = parent_request_id
        .map(|value| {
            format!(
                r#", caused_by_parent_request_id: "{}""#,
                escape_graphql_string(value)
            )
        })
        .unwrap_or_default();
    let parent_tool_field = parent_tool_call_id
        .map(|value| {
            let value = escape_graphql_string(value);
            format!(r#", caused_by_parent_tool_call_id: "{value}", caused_by_trigger_id: "{value}", caused_by_trigger_kind: "subagent""#)
        })
        .unwrap_or_default();
    let mutation = format!(
        r#"mutation {{
            upsert_AgentRequest(
                filter: {{ request_id: {{ _eq: "{request_id}" }} }},
                add: {{
                    request_id: "{request_id}",
                    agent_did: "{agent_did}",
                    behavior_id: "{behavior_id}",
                    session_id: "{session_id}",
                    retry_parent_request: "",
                    retry_root_request: "{request_id}",
                    superseded_by_request: "",
                    content: "r5 scenario request",
                    status: "{status}",
                    lifecycle_state: "{state}",
                    backend_id: "",
                    execution_origin: "interactive",
                    metadata: "",
                    failure_reason: "",
                    created_at: "{now}",
                    deadline: "{deadline}",
                    retry_count: 0,
                    max_retries: 3,
                    subagent_depth: 0
                    {parent_request_field}
                    {parent_tool_field}
                }},
                update: {{
                    agent_did: "{agent_did}",
                    behavior_id: "{behavior_id}",
                    status: "{status}",
                    lifecycle_state: "{state}"
                    {parent_request_field}
                    {parent_tool_field}
                }}
            ) {{ _docID }}
        }}"#
    );
    exec(node, &mutation, "write AgentRequest").await
}

async fn write_parent_tool_call(
    node: &HarnessNode,
    parent_request_id: &str,
    parent_tool_call_id: &str,
    child_request_id: &str,
    behavior_id: &str,
    unclaimed_deadline_at: Option<&str>,
) -> Result<()> {
    let agent_did = load_request(node, parent_request_id)
        .await
        .map(|row| row.agent_did)
        .unwrap_or_else(|_| {
            if node.id == "A" {
                NODE_A_DID.to_string()
            } else {
                NODE_B_DID.to_string()
            }
        });
    let session_id_raw = format!("{parent_request_id}-session");
    let parent_request_id = escape_graphql_string(parent_request_id);
    let parent_tool_call_id = escape_graphql_string(parent_tool_call_id);
    let child_request_id = escape_graphql_string(child_request_id);
    let agent_did = escape_graphql_string(&agent_did);
    let session_id = escape_graphql_string(&session_id_raw);
    let args = escape_graphql_string(
        &json!({
            "behavior_id": behavior_id,
            "prompt": "r5 child prompt",
            "await_mode": "background"
        })
        .to_string(),
    );
    let now = chrono::Utc::now().to_rfc3339();
    let deadline = (chrono::Utc::now() + chrono::Duration::minutes(30)).to_rfc3339();
    let unclaimed = unclaimed_deadline_at
        .map(|value| {
            format!(
                r#", unclaimed_deadline_at: "{}""#,
                escape_graphql_string(value)
            )
        })
        .unwrap_or_default();
    let mutation = format!(
        r#"mutation {{
            upsert_AgentToolCall(
                filter: {{ tool_call_key: {{ _eq: "{session_id}:{parent_tool_call_id}" }} }},
                add: {{
                    tool_call_key: "{session_id}:{parent_tool_call_id}",
                    request_id: "{parent_request_id}",
                    session_id: "{session_id}",
                    agent_did: "{agent_did}",
                    message_sequence: 1,
                    tool_name: "spawn_subagent",
                    tool_call_id: "{parent_tool_call_id}",
                    args: "{args}",
                    result: "",
                    status: "called",
                    lifecycle_state: "running",
                    started_at: "{now}",
                    deadline_at: "{deadline}",
                    await_mode: "background",
                    cancel_policy: "cascade",
                    child_request_id: "{child_request_id}"
                    {unclaimed}
                }},
                update: {{
                    lifecycle_state: "running",
                    await_mode: "background",
                    cancel_policy: "cascade",
                    child_request_id: "{child_request_id}"
                    {unclaimed}
                }}
            ) {{ _docID }}
        }}"#
    );
    exec(node, &mutation, "write AgentToolCall").await
}

async fn export_doc(
    node: &HarnessNode,
    collection: &str,
    doc_id: &str,
) -> Result<serde_json::Value> {
    let filter = match collection {
        "AgentRequest" => format!(
            r#"request_id: {{ _eq: "{}" }}"#,
            escape_graphql_string(doc_id)
        ),
        "AgentToolCall" => format!(
            r#"tool_call_id: {{ _eq: "{}" }}"#,
            escape_graphql_string(doc_id)
        ),
        "AgentResponse" => format!(
            r#"request_id: {{ _eq: "{}" }}"#,
            escape_graphql_string(doc_id)
        ),
        "AgentMessage" => {
            let message_key = if doc_id.contains(':') {
                doc_id.to_string()
            } else {
                format!("{doc_id}-session:1")
            };
            format!(
                r#"message_key: {{ _eq: "{}" }}"#,
                escape_graphql_string(&message_key)
            )
        }
        other => bail!("unsupported replicate collection {other}"),
    };
    let fields = match collection {
        "AgentRequest" => {
            "request_id agent_did behavior_id session_id status lifecycle_state caused_by_parent_request_id caused_by_parent_tool_call_id caused_by_trigger_id caused_by_trigger_kind interrupt_requested_at"
        }
        "AgentToolCall" => {
            "tool_call_key request_id session_id agent_did message_sequence tool_name tool_call_id args result status lifecycle_state started_at deadline_at completed_at tool_failure_class denial_reason denied_argv denied_command denied_argument denied_subcommand denied_prefix policy_mode policy_network cancel_cause latency_ms await_mode cancel_policy child_request_id unclaimed_deadline_at cancel_cascade_intent_at cancel_pending_remote_ack stuck_since"
        }
        "AgentResponse" => {
            "response_key request_id agent_did behavior_id session_id content reasoning status error_message token_count progress_seq materialized_message_sequence materialized_at created_at completed_at"
        }
        "AgentMessage" => "message_key session_id sequence role content timestamp",
        _ => unreachable!(),
    };
    let query = format!(
        r#"{{
            {collection}(filter: {{ {filter} }}, limit: 1) {{ {fields} }}
        }}"#
    );
    let response = node.db.node.execute(&query).await;
    if response.has_errors() {
        bail!("export {collection} failed: {:?}", response.errors);
    }
    response
        .data
        .as_ref()
        .and_then(|d| d.get(collection))
        .and_then(|v| v.as_array())
        .and_then(|rows| rows.first())
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("{collection} {doc_id} not found for replication"))
}

async fn import_doc(node: &HarnessNode, collection: &str, row: &serde_json::Value) -> Result<()> {
    match collection {
        "AgentRequest" => {
            let request_id = str_field(row, "request_id")?;
            write_agent_request(
                node,
                request_id,
                str_field(row, "agent_did")?,
                str_field(row, "behavior_id")?,
                str_field(row, "lifecycle_state")?,
                opt_str_field(row, "caused_by_parent_request_id"),
                opt_str_field(row, "caused_by_parent_tool_call_id"),
            )
            .await?;
            if let Some(interrupt) = opt_str_field(row, "interrupt_requested_at") {
                set_child_interrupt(node, request_id, interrupt).await?;
            }
            Ok(())
        }
        "AgentToolCall" => import_tool_call(node, row).await,
        "AgentResponse" => import_response(node, row).await,
        "AgentMessage" => import_message(node, row).await,
        other => bail!("unsupported import collection {other}"),
    }
}

async fn import_tool_call(node: &HarnessNode, row: &serde_json::Value) -> Result<()> {
    let session_id = str_field(row, "session_id")?;
    let tool_call_id = str_field(row, "tool_call_id")?;
    let tool_call_key = str_field(row, "tool_call_key")?;
    let request_id = str_field(row, "request_id")?;
    let agent_did = opt_str_field(row, "agent_did").unwrap_or(if node.id == "B" {
        NODE_B_DID
    } else {
        NODE_A_DID
    });
    let args = escape_graphql_string(str_field(row, "args")?);
    let result = escape_graphql_string(opt_str_field(row, "result").unwrap_or(""));
    let child_request_id =
        escape_graphql_string(opt_str_field(row, "child_request_id").unwrap_or(""));
    let started_at = opt_str_field(row, "started_at")
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());
    let deadline_at = opt_str_field(row, "deadline_at")
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| (chrono::Utc::now() + chrono::Duration::minutes(30)).to_rfc3339());
    let optional = optional_tool_fields(row);
    let mutation = format!(
        r#"mutation {{
            upsert_AgentToolCall(
                filter: {{ tool_call_key: {{ _eq: "{}" }} }},
                add: {{
                    tool_call_key: "{}",
                    request_id: "{}",
                    session_id: "{}",
                    agent_did: "{}",
                    message_sequence: {},
                    tool_name: "{}",
                    tool_call_id: "{}",
                    args: "{args}",
                    result: "{result}",
                    status: "{}",
                    lifecycle_state: "{}",
                    started_at: "{started_at}",
                    deadline_at: "{deadline_at}",
                    await_mode: "{}",
                    cancel_policy: "{}",
                    child_request_id: "{child_request_id}"
                    {optional}
                }},
                update: {{
                    result: "{result}",
                    status: "{}",
                    lifecycle_state: "{}",
                    started_at: "{started_at}",
                    deadline_at: "{deadline_at}",
                    await_mode: "{}",
                    cancel_policy: "{}",
                    child_request_id: "{child_request_id}"
                    {optional}
                }}
            ) {{ _docID }}
        }}"#,
        escape_graphql_string(tool_call_key),
        escape_graphql_string(tool_call_key),
        escape_graphql_string(request_id),
        escape_graphql_string(session_id),
        escape_graphql_string(agent_did),
        row.get("message_sequence")
            .and_then(|v| v.as_i64())
            .unwrap_or(1),
        escape_graphql_string(str_field(row, "tool_name")?),
        escape_graphql_string(tool_call_id),
        opt_str_field(row, "status").unwrap_or("called"),
        opt_str_field(row, "lifecycle_state").unwrap_or("running"),
        opt_str_field(row, "await_mode").unwrap_or("background"),
        opt_str_field(row, "cancel_policy").unwrap_or("cascade"),
        opt_str_field(row, "status").unwrap_or("called"),
        opt_str_field(row, "lifecycle_state").unwrap_or("running"),
        opt_str_field(row, "await_mode").unwrap_or("background"),
        opt_str_field(row, "cancel_policy").unwrap_or("cascade"),
    );
    exec(node, &mutation, "import AgentToolCall").await
}

async fn import_response(node: &HarnessNode, row: &serde_json::Value) -> Result<()> {
    let request_id = str_field(row, "request_id")?;
    create_agent_response(
        node,
        request_id,
        str_field(row, "agent_did")?,
        str_field(row, "behavior_id")?,
        str_field(row, "session_id")?,
        opt_str_field(row, "content").unwrap_or(""),
    )
    .await
}

async fn import_message(node: &HarnessNode, row: &serde_json::Value) -> Result<()> {
    let message_key = str_field(row, "message_key")?;
    let session_id = str_field(row, "session_id")?;
    let sequence = row.get("sequence").and_then(|v| v.as_i64()).unwrap_or(1);
    let role = opt_str_field(row, "role").unwrap_or("assistant");
    let content = opt_str_field(row, "content").unwrap_or("");
    let timestamp = opt_str_field(row, "timestamp")
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());
    let mutation = format!(
        r#"mutation {{
            create_AgentMessage(input: {{
                message_key: "{}",
                session_id: "{}",
                sequence: {sequence},
                role: "{}",
                content: "{}",
                timestamp: "{timestamp}"
            }}) {{ _docID }}
        }}"#,
        escape_graphql_string(message_key),
        escape_graphql_string(session_id),
        escape_graphql_string(role),
        escape_graphql_string(content),
    );
    exec(node, &mutation, "import AgentMessage").await
}

async fn ensure_behavior(node: &HarnessNode, behavior_id: &str, agent_did: &str) -> Result<()> {
    let selection_id = format!("{behavior_id}-tools");
    upsert_tool_selection(
        node.db.node.as_ref(),
        &ToolSelectionDocument {
            selection_id: selection_id.clone(),
            agent_did: agent_did.to_string(),
            subagent_targets: Some(
                [
                    "child-behavior",
                    "child-behavior-1",
                    "child-behavior-2",
                    behavior_id,
                ]
                .into_iter()
                .map(|target_behavior_id| {
                    gents::subagent_target_entry(
                        target_behavior_id,
                        agent_did,
                        target_behavior_id,
                        None,
                    )
                })
                .collect(),
            ),
            subagent_spawn_enabled: Some(true),
            subagent_background_enabled: Some(true),
            cross_deployment_spawn_timeout_seconds: Some(60),
            enable_defra_query: None,
            defra_query_collections: None,
            ..Default::default()
        },
    )
    .await?;
    upsert_agent_behavior(
        node.db.node.as_ref(),
        &AgentBehaviorDocument {
            behavior_id: behavior_id.to_string(),
            agent_did: agent_did.to_string(),
            display_name: Some(behavior_id.to_string()),
            description: None,
            summary: None,
            system_prompt: None,
            request_context_template: None,
            backend_id: None,
            model_name: None,
            tool_selection_id: Some(selection_id),
            inference_profile_id: None,
            compaction_strategy: None,
            compaction_threshold: None,
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            enabled: true,
            created_at: Some("2026-05-14T00:00:00Z".to_string()),
        },
    )
    .await?;
    Ok(())
}

async fn terminalize_child_on_b(
    node: &HarnessNode,
    request_id: &str,
    terminal: &str,
    final_response: Option<&str>,
) -> Result<()> {
    let status = status_for_lifecycle(terminal);
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ request_id: {{ _eq: "{}" }} }},
                input: {{ status: "{status}", lifecycle_state: "{}" }}
            ) {{ _docID }}
        }}"#,
        escape_graphql_string(request_id),
        escape_graphql_string(terminal)
    );
    exec(node, &mutation, "terminalize child").await?;
    if let Some(final_response) = final_response {
        let child = load_request(node, request_id).await?;
        create_agent_message(node, &child.session_id, final_response).await?;
        create_agent_response(
            node,
            request_id,
            &child.agent_did,
            &child.behavior_id,
            &child.session_id,
            final_response,
        )
        .await?;
    }
    Ok(())
}

async fn cancel_parent_on_a(node: &HarnessNode, parent_tool_call_id: &str) -> Result<()> {
    let session_id = format!(
        "{parent_tool_call_id_parent}-session",
        parent_tool_call_id_parent = parent_request_for_tool(node, parent_tool_call_id).await?
    );
    let mut lifecycle =
        ToolCallLifecycle::load(node.db.node.clone(), &session_id, parent_tool_call_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("bridge {parent_tool_call_id} not found"))?;
    lifecycle
        .cancel_during_run(CancelCause::Interrupted)
        .await?;
    if let Some(dispatch) = lifecycle.bridge_cancel_cascade_dispatch(NODE_A_DID).await? {
        if let CascadeDispatch::Local(intent) = dispatch {
            interrupt_request(node.db.node.as_ref(), &intent.child_request_id).await?;
        }
    }
    Ok(())
}

async fn parent_request_for_tool(node: &HarnessNode, tool_call_id: &str) -> Result<String> {
    let query = format!(
        r#"{{
            AgentToolCall(filter: {{ tool_call_id: {{ _eq: "{}" }} }}, limit: 1) {{ request_id }}
        }}"#,
        escape_graphql_string(tool_call_id)
    );
    let response = node.db.node.execute(&query).await;
    #[derive(Deserialize)]
    struct Row {
        request_id: String,
    }
    first_optional_row::<Row>(&response, "AgentToolCall")
        .map(|row| row.request_id)
        .ok_or_else(|| anyhow::anyhow!("parent tool call {tool_call_id} not found"))
}

async fn run_background_completion_on_a(node: &HarnessNode) -> Result<()> {
    for request_id in terminal_child_request_ids(node).await? {
        let _ =
            project_background_subagent_completion(node.db.node.clone(), &request_id, NODE_A_DID)
                .await?;
    }
    Ok(())
}

async fn run_cancel_mirror_on_b(node: &HarnessNode) -> Result<()> {
    for bridge in load_bridge_rows(node).await? {
        let Some(intent_at) = bridge.cancel_cascade_intent_at.as_deref() else {
            continue;
        };
        let Some(child_request_id) = bridge.child_request_id.as_deref() else {
            continue;
        };
        let Some(child) = load_request_optional(node, child_request_id).await? else {
            continue;
        };
        if child.agent_did == NODE_B_DID
            && !child.is_terminal()
            && child.interrupt_requested_at.is_none()
        {
            set_child_interrupt(node, child_request_id, intent_at).await?;
        }
    }
    Ok(())
}

async fn advance_r5_clock_effects(node: &HarnessNode, seconds: u64) -> Result<()> {
    let past = (chrono::Utc::now() - chrono::Duration::seconds(seconds as i64))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let query = r#"{
        AgentToolCall(filter: { cancel_pending_remote_ack: { _eq: true } }) {
            _docID
            started_at
            deadline_at
            completed_at
            unclaimed_deadline_at
            stuck_since
        }
    }"#;
    let response = node.db.node.execute(query).await;
    if response.has_errors() {
        bail!(
            "query cancel-pending bridge rows failed: {:?}",
            response.errors
        );
    }
    let rows: Vec<AdvanceBridgeRow> = response
        .data
        .as_ref()
        .and_then(|d| d.get("AgentToolCall"))
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();
    for row in rows {
        let doc_id = escape_graphql_string(&row.doc_id);
        let datetime_fields = row.datetime_update_fragment();
        let mutation = format!(
            r#"mutation {{
                update_AgentToolCall(
                    filter: {{ _docID: {{ _eq: "{doc_id}" }} }},
                    input: {{ cancel_cascade_intent_at: "{past}"{datetime_fields} }}
                ) {{ _docID }}
            }}"#
        );
        exec(node, &mutation, "advance R5 clock effects").await?;
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct AdvanceBridgeRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    started_at: Option<String>,
    deadline_at: Option<String>,
    completed_at: Option<String>,
    unclaimed_deadline_at: Option<String>,
    stuck_since: Option<String>,
}

impl AdvanceBridgeRow {
    fn datetime_update_fragment(&self) -> String {
        let mut fields = Vec::new();
        push_runner_datetime_field(&mut fields, "started_at", self.started_at.as_deref());
        push_runner_datetime_field(&mut fields, "deadline_at", self.deadline_at.as_deref());
        push_runner_datetime_field(&mut fields, "completed_at", self.completed_at.as_deref());
        push_runner_datetime_field(
            &mut fields,
            "unclaimed_deadline_at",
            self.unclaimed_deadline_at.as_deref(),
        );
        push_runner_datetime_field(&mut fields, "stuck_since", self.stuck_since.as_deref());
        if fields.is_empty() {
            String::new()
        } else {
            format!(", {}", fields.join(", "))
        }
    }
}

fn push_runner_datetime_field(fields: &mut Vec<String>, field: &'static str, value: Option<&str>) {
    let Some(value) = value.filter(|value| !value.trim().is_empty()) else {
        return;
    };
    let value = chrono::DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|_| value.to_string());
    fields.push(format!(r#"{field}: "{}""#, escape_graphql_string(&value)));
}

async fn load_bridge_rows(node: &HarnessNode) -> Result<Vec<BridgeObservation>> {
    let query = r#"{
        AgentToolCall(filter: { await_mode: { _eq: "background" } }) {
            request_id
            session_id
            tool_call_id
            lifecycle_state
            child_request_id
            cancel_cause
            cancel_cascade_intent_at
            cancel_pending_remote_ack
            stuck_since
        }
    }"#;
    let response = node.db.node.execute(query).await;
    if response.has_errors() {
        bail!("load bridge rows failed: {:?}", response.errors);
    }
    Ok(response
        .data
        .as_ref()
        .and_then(|d| d.get("AgentToolCall"))
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default())
}

async fn load_child_requests(node: &HarnessNode) -> Result<Vec<RequestObservation>> {
    let query = r#"{
        AgentRequest(filter: { caused_by_parent_tool_call_id: { _ne: "" } }) {
            request_id
            agent_did
            lifecycle_state
            caused_by_parent_tool_call_id
            interrupt_requested_at
        }
    }"#;
    let response = node.db.node.execute(query).await;
    if response.has_errors() {
        bail!("load child requests failed: {:?}", response.errors);
    }
    Ok(response
        .data
        .as_ref()
        .and_then(|d| d.get("AgentRequest"))
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default())
}

async fn terminal_child_request_ids(node: &HarnessNode) -> Result<Vec<String>> {
    Ok(load_child_requests(node)
        .await?
        .into_iter()
        .filter(RequestObservation::is_terminal)
        .map(|row| row.request_id)
        .collect())
}

async fn load_subagent_notifications(node: &HarnessNode) -> Result<Vec<String>> {
    let query = r#"{ AgentMessage { content } }"#;
    let response = node.db.node.execute(query).await;
    #[derive(Deserialize)]
    struct Row {
        content: String,
    }
    let rows: Vec<Row> = response
        .data
        .as_ref()
        .and_then(|d| d.get("AgentMessage"))
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();
    Ok(rows
        .into_iter()
        .filter(|row| row.content.contains("<subagent-notification"))
        .map(|row| row.content)
        .collect())
}

async fn load_background_wakeup_keys(node: &HarnessNode) -> Result<Vec<String>> {
    let query =
        r#"{ AgentRequest(filter: { execution_origin: { _eq: "scheduled" } }) { metadata } }"#;
    let response = node.db.node.execute(query).await;
    #[derive(Deserialize)]
    struct Row {
        metadata: Option<String>,
    }
    let rows: Vec<Row> = response
        .data
        .as_ref()
        .and_then(|d| d.get("AgentRequest"))
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();
    Ok(rows
        .into_iter()
        .filter_map(|row| row.metadata)
        .filter(|metadata| metadata.contains("background_completion"))
        .collect())
}

#[derive(Deserialize)]
struct RequestRow {
    request_id: String,
    agent_did: String,
    behavior_id: String,
    session_id: String,
    lifecycle_state: String,
    interrupt_requested_at: Option<String>,
}

impl RequestRow {
    fn is_terminal(&self) -> bool {
        matches!(
            self.lifecycle_state.as_str(),
            "completed" | "failed" | "dead" | "interrupted" | "superseded"
        )
    }
}

async fn load_request(node: &HarnessNode, request_id: &str) -> Result<RequestRow> {
    load_request_optional(node, request_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("AgentRequest {request_id} not found"))
}

async fn load_request_optional(node: &HarnessNode, request_id: &str) -> Result<Option<RequestRow>> {
    let query = format!(
        r#"{{
            AgentRequest(filter: {{ request_id: {{ _eq: "{}" }} }}, limit: 1) {{
                request_id agent_did behavior_id session_id lifecycle_state interrupt_requested_at
            }}
        }}"#,
        escape_graphql_string(request_id)
    );
    let response = node.db.node.execute(&query).await;
    Ok(first_optional_row(&response, "AgentRequest"))
}

async fn set_child_interrupt(node: &HarnessNode, request_id: &str, when: &str) -> Result<()> {
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ request_id: {{ _eq: "{}" }} }},
                input: {{ interrupt_requested_at: "{}" }}
            ) {{ _docID }}
        }}"#,
        escape_graphql_string(request_id),
        escape_graphql_string(when)
    );
    exec(node, &mutation, "set child interrupt").await
}

async fn create_agent_message(node: &HarnessNode, session_id: &str, content: &str) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    let mutation = format!(
        r#"mutation {{
            create_AgentMessage(input: {{
                message_key: "{}:1",
                session_id: "{}",
                sequence: 1,
                role: "assistant",
                content: "{}",
                timestamp: "{now}"
            }}) {{ _docID }}
        }}"#,
        escape_graphql_string(session_id),
        escape_graphql_string(session_id),
        escape_graphql_string(content),
    );
    exec(node, &mutation, "create AgentMessage").await
}

async fn create_agent_response(
    node: &HarnessNode,
    request_id: &str,
    agent_did: &str,
    behavior_id: &str,
    session_id: &str,
    content: &str,
) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    let mutation = format!(
        r#"mutation {{
            upsert_AgentResponse(
                filter: {{ response_key: {{ _eq: "{}" }} }},
                add: {{
                    response_key: "{}",
                    request_id: "{}",
                    agent_did: "{}",
                    behavior_id: "{}",
                    session_id: "{}",
                    content: "{}",
                    reasoning: "",
                    status: "completed",
                    error_message: "",
                    token_count: 0,
                    progress_seq: 0,
                    materialized_message_sequence: 1,
                    materialized_at: "{now}",
                    created_at: "{now}",
                    completed_at: "{now}"
                }},
                update: {{ content: "{}", status: "completed", completed_at: "{now}" }}
            ) {{ _docID }}
        }}"#,
        escape_graphql_string(request_id),
        escape_graphql_string(request_id),
        escape_graphql_string(request_id),
        escape_graphql_string(agent_did),
        escape_graphql_string(behavior_id),
        escape_graphql_string(session_id),
        escape_graphql_string(content),
        escape_graphql_string(content)
    );
    exec(node, &mutation, "create AgentResponse").await
}

fn optional_tool_fields(row: &serde_json::Value) -> String {
    let mut fields = Vec::new();
    for field in [
        "completed_at",
        "tool_failure_class",
        "denial_reason",
        "denied_command",
        "denied_argument",
        "denied_subcommand",
        "policy_mode",
        "policy_network",
        "cancel_cause",
        "unclaimed_deadline_at",
        "cancel_cascade_intent_at",
        "stuck_since",
    ] {
        if let Some(value) = opt_str_field(row, field) {
            fields.push(format!(r#"{field}: "{}""#, escape_graphql_string(value)));
        }
    }
    for field in ["denied_argv", "denied_prefix"] {
        if let Some(value) = opt_string_array_field(row, field) {
            fields.push(format!("{field}: {}", string_array_literal(&value)));
        }
    }
    if let Some(value) = row.get("latency_ms").and_then(|v| v.as_i64()) {
        fields.push(format!("latency_ms: {value}"));
    }
    if let Some(value) = row
        .get("cancel_pending_remote_ack")
        .and_then(|v| v.as_bool())
    {
        fields.push(format!("cancel_pending_remote_ack: {value}"));
    }
    if fields.is_empty() {
        String::new()
    } else {
        format!(", {}", fields.join(", "))
    }
}

fn str_field<'a>(row: &'a serde_json::Value, field: &str) -> Result<&'a str> {
    row.get(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("row missing string field {field}"))
}

fn opt_str_field<'a>(row: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    row.get(field)
        .and_then(|v| v.as_str())
        .filter(|v| !v.is_empty())
}

fn opt_string_array_field(row: &serde_json::Value, field: &str) -> Option<Vec<String>> {
    Some(
        row.get(field)?
            .as_array()?
            .iter()
            .filter_map(|value| value.as_str().map(ToOwned::to_owned))
            .collect(),
    )
}

fn string_array_literal(values: &[String]) -> String {
    let values = values
        .iter()
        .map(|value| format!(r#""{}""#, escape_graphql_string(value)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{values}]")
}

fn status_for_lifecycle(state: &str) -> &str {
    match state {
        "completed" => "completed",
        "failed" | "dead" | "interrupted" => "error",
        "superseded" => "superseded",
        "processing" => "processing",
        _ => state,
    }
}

async fn exec(node: &HarnessNode, mutation: &str, label: &str) -> Result<()> {
    let response = node.db.node.execute(mutation).await;
    if response.has_errors() {
        bail!("{label} failed: {:?}", response.errors);
    }
    Ok(())
}
