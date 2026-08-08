use anyhow::{Context, Result};
use defra_node::EmbeddedNode;
use serde::Deserialize;

use crate::graphql::escape_graphql_string;
use crate::session::execute_mutation_with_retry;

const EXACT_RECOVERY_ATTEMPTS: usize = 3;

#[derive(Debug, Default)]
pub struct InferenceCallRecoveryReport {
    pub calls_recovered: usize,
}

pub struct InferenceCall;

#[derive(Debug, Deserialize)]
struct StaleInferenceCallRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    call_id: String,
    runtime_instance_id: String,
    request_id: String,
    agent_did: String,
    controller_generation: i64,
    call_state: String,
    #[serde(default)]
    failure_reason: Option<String>,
    #[serde(default)]
    ended_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ParentRequestRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    status: String,
    #[serde(default)]
    lifecycle_state: Option<String>,
}

enum InferenceRecoveryOutcome {
    Cancelled,
    Failed,
}

impl InferenceCall {
    pub async fn recover_all(
        node: &EmbeddedNode,
        agent_did: &str,
    ) -> Result<InferenceCallRecoveryReport> {
        let rows = load_stale_inference_calls(node, agent_did).await?;
        let mut calls_recovered = 0;

        for row in rows {
            let Some(parent) = lookup_parent_request(node, agent_did, &row.request_id).await?
            else {
                continue;
            };
            if recovery_outcome(&row, &parent).is_none() {
                continue;
            }

            if let Err(error) = recover_inference_call_row(node, &row, &parent).await {
                tracing::warn!(
                    call_id = %row.call_id,
                    request_id = %row.request_id,
                    call_state = %row.call_state,
                    error = %error,
                    "failed to recover stale inference call"
                );
                continue;
            }

            calls_recovered += 1;
            tracing::info!(
                call_id = %row.call_id,
                request_id = %row.request_id,
                "recovered stale inference call"
            );
        }

        Ok(InferenceCallRecoveryReport { calls_recovered })
    }
}

async fn load_stale_inference_calls(
    node: &EmbeddedNode,
    agent_did: &str,
) -> Result<Vec<StaleInferenceCallRow>> {
    let escaped_agent_did = escape_graphql_string(agent_did);
    let query = format!(
        r#"{{
            InferenceCall(
                filter: {{
                    agent_did: {{ _eq: "{escaped_agent_did}" }},
                    call_state: {{ _in: ["queued", "running"] }}
                }}
            ) {{
                _docID
                call_id
                runtime_instance_id
                request_id
                agent_did
                controller_generation
                call_state
                failure_reason
                ended_at
            }}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("querying stale InferenceCall rows: {:?}", resp.errors);
    }

    let rows = resp
        .data
        .as_ref()
        .and_then(|data| data.get("InferenceCall"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    Ok(rows)
}

async fn lookup_parent_request(
    node: &EmbeddedNode,
    agent_did: &str,
    request_id: &str,
) -> Result<Option<ParentRequestRow>> {
    let escaped_agent_did = escape_graphql_string(agent_did);
    let escaped_request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    agent_did: {{ _eq: "{escaped_agent_did}" }},
                    request_id: {{ _eq: "{escaped_request_id}" }}
                }}
            ) {{
                _docID
                status
                lifecycle_state
            }}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!(
            "querying parent request for inference recovery request_id={request_id}: {:?}",
            resp.errors
        );
    }

    let rows: Vec<ParentRequestRow> = resp
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    select_parent_request(rows, agent_did, request_id)
}

fn select_parent_request(
    rows: Vec<ParentRequestRow>,
    agent_did: &str,
    request_id: &str,
) -> Result<Option<ParentRequestRow>> {
    match rows.as_slice() {
        [] => Ok(None),
        [_] => Ok(rows.into_iter().next()),
        _ => {
            let conflicting_doc_ids = rows
                .iter()
                .map(|row| row.doc_id.as_str())
                .collect::<Vec<_>>();
            anyhow::bail!(
                "ambiguous parent AgentRequest rows for agent_did={agent_did} request_id={request_id}: conflicting _docIDs={conflicting_doc_ids:?}"
            )
        }
    }
}

fn recovery_outcome(
    row: &StaleInferenceCallRow,
    parent: &ParentRequestRow,
) -> Option<InferenceRecoveryOutcome> {
    // Recovery never claims ownership from a live parent. Only a terminal or
    // interrupted parent authorizes terminalizing one of its nonterminal calls.
    if request_is_interrupted(parent) {
        return Some(InferenceRecoveryOutcome::Cancelled);
    }
    if !request_is_terminal(parent) {
        return None;
    }

    match row.call_state.as_str() {
        "queued" => Some(InferenceRecoveryOutcome::Cancelled),
        "running" => Some(InferenceRecoveryOutcome::Failed),
        _ => None,
    }
}

async fn recover_inference_call_row(
    node: &EmbeddedNode,
    row: &StaleInferenceCallRow,
    parent: &ParentRequestRow,
) -> Result<()> {
    let mut observed_state = row.call_state.clone();
    let recovery_ended_at = chrono::Utc::now().to_rfc3339();
    for attempt in 1..=EXACT_RECOVERY_ATTEMPTS {
        let snapshot = StaleInferenceCallRow {
            doc_id: row.doc_id.clone(),
            call_id: row.call_id.clone(),
            runtime_instance_id: row.runtime_instance_id.clone(),
            request_id: row.request_id.clone(),
            agent_did: row.agent_did.clone(),
            controller_generation: row.controller_generation,
            call_state: observed_state.clone(),
            failure_reason: row.failure_reason.clone(),
            ended_at: row.ended_at.clone(),
        };
        let Some(outcome) = recovery_outcome(&snapshot, parent) else {
            anyhow::bail!(
                "InferenceCall recovery no longer has a parent-authorized transition: _docID={} call_id={} observed_state={observed_state}",
                row.doc_id,
                row.call_id
            );
        };
        let (target_state, failure_reason) = match outcome {
            InferenceRecoveryOutcome::Cancelled => ("cancelled", "Cancelled"),
            InferenceRecoveryOutcome::Failed => ("failed", "StreamDroppedBeforeTerminalResponse"),
        };
        let mutation = recovery_mutation(
            row,
            &observed_state,
            target_state,
            failure_reason,
            &recovery_ended_at,
        );
        let response = execute_mutation_with_retry(node, &mutation, "recover_inference_call")
            .await
            .context("recover inference call mutation")?;
        let returned_doc_ids = mutation_doc_ids(response.data.as_ref(), "update_InferenceCall");
        if returned_doc_ids.as_slice() == [row.doc_id.as_str()] {
            return Ok(());
        }
        if !returned_doc_ids.is_empty() {
            anyhow::bail!(
                "InferenceCall recovery returned unexpected document ids: _docID={} call_id={} expected_state={observed_state} target_state={target_state} returned_doc_ids={returned_doc_ids:?}",
                row.doc_id,
                row.call_id
            );
        }

        let Some(reloaded) = reload_exact_call(node, &row.doc_id).await? else {
            anyhow::bail!(
                "InferenceCall recovery matched no document and exact reload found no row: _docID={} call_id={} expected_state={observed_state} target_state={target_state}",
                row.doc_id,
                row.call_id
            );
        };
        if reloaded.call_id != row.call_id
            || reloaded.runtime_instance_id != row.runtime_instance_id
            || reloaded.request_id != row.request_id
            || reloaded.agent_did != row.agent_did
            || reloaded.controller_generation != row.controller_generation
        {
            anyhow::bail!(
                "InferenceCall recovery exact-document ownership conflict: _docID={} expected_call_id={} observed_call_id={} expected_state={observed_state} target_state={target_state} observed_state={}",
                row.doc_id,
                row.call_id,
                reloaded.call_id,
                reloaded.call_state
            );
        }
        let reloaded_state = reloaded.call_state;
        if reloaded_state == target_state {
            if reloaded.failure_reason.as_deref() == Some(failure_reason)
                && reloaded.ended_at.as_deref() == Some(recovery_ended_at.as_str())
            {
                return Ok(());
            }
            anyhow::bail!(
                "InferenceCall recovery reached target state with conflicting terminal facts: _docID={} call_id={} target_state={target_state}",
                row.doc_id,
                row.call_id
            );
        }
        if matches!(
            reloaded_state.as_str(),
            "completed" | "failed" | "cancelled"
        ) {
            anyhow::bail!(
                "InferenceCall recovery terminal conflict: _docID={} call_id={} expected_state={observed_state} target_state={target_state} observed_state={reloaded_state}",
                row.doc_id,
                row.call_id
            );
        }
        if attempt == EXACT_RECOVERY_ATTEMPTS {
            anyhow::bail!(
                "InferenceCall recovery remained nonterminal after {EXACT_RECOVERY_ATTEMPTS} exact attempts: _docID={} call_id={} last_state={reloaded_state}",
                row.doc_id,
                row.call_id
            );
        }
        observed_state = reloaded_state;
        tokio::task::yield_now().await;
    }
    unreachable!("bounded exact recovery loop always returns")
}

fn recovery_mutation(
    row: &StaleInferenceCallRow,
    expected_state: &str,
    target_state: &str,
    failure_reason: &str,
    ended_at: &str,
) -> String {
    let escaped_doc_id = escape_graphql_string(&row.doc_id);
    let escaped_call_id = escape_graphql_string(&row.call_id);
    let escaped_runtime_instance_id = escape_graphql_string(&row.runtime_instance_id);
    let escaped_request_id = escape_graphql_string(&row.request_id);
    let escaped_agent_did = escape_graphql_string(&row.agent_did);
    let escaped_expected_state = escape_graphql_string(expected_state);
    let escaped_target_state = escape_graphql_string(target_state);
    let escaped_failure_reason = escape_graphql_string(failure_reason);
    let ended_at = escape_graphql_string(ended_at);
    format!(
        r#"mutation {{
            update_InferenceCall(
                filter: {{
                    _docID: {{ _eq: "{escaped_doc_id}" }},
                    call_id: {{ _eq: "{escaped_call_id}" }},
                    runtime_instance_id: {{ _eq: "{escaped_runtime_instance_id}" }},
                    request_id: {{ _eq: "{escaped_request_id}" }},
                    agent_did: {{ _eq: "{escaped_agent_did}" }},
                    controller_generation: {{ _eq: {controller_generation} }},
                    call_state: {{ _eq: "{escaped_expected_state}" }}
                }},
                input: {{
                    call_state: "{escaped_target_state}",
                    failure_reason: "{escaped_failure_reason}",
                    ended_at: "{ended_at}"
                }}
            ) {{ _docID }}
        }}"#,
        controller_generation = row.controller_generation,
    )
}

async fn reload_exact_call(
    node: &EmbeddedNode,
    doc_id: &str,
) -> Result<Option<StaleInferenceCallRow>> {
    let escaped_doc_id = escape_graphql_string(doc_id);
    let query = format!(
        r#"{{
            InferenceCall(filter: {{ _docID: {{ _eq: "{escaped_doc_id}" }} }}) {{
                _docID
                call_id
                runtime_instance_id
                request_id
                agent_did
                controller_generation
                call_state
                failure_reason
                ended_at
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "reloading exact InferenceCall _docID={doc_id}: {:?}",
            response.errors
        );
    }
    let row = response
        .data
        .as_ref()
        .and_then(|data| data.get("InferenceCall"))
        .and_then(serde_json::Value::as_array)
        .and_then(|rows| rows.first());
    match row {
        None => Ok(None),
        Some(row) => serde_json::from_value(row.clone())
            .context("deserializing exact InferenceCall recovery reload")
            .map(Some),
    }
}

fn mutation_doc_ids(data: Option<&serde_json::Value>, field: &str) -> Vec<String> {
    let Some(value) = data.and_then(|data| data.get(field)) else {
        return Vec::new();
    };
    if let Some(doc_id) = value.get("_docID").and_then(serde_json::Value::as_str) {
        return vec![doc_id.to_owned()];
    }
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|row| {
            row.get("_docID")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .collect()
}

fn request_is_interrupted(parent: &ParentRequestRow) -> bool {
    parent.status == "interrupted" || parent.lifecycle_state.as_deref() == Some("interrupted")
}

fn request_is_terminal(parent: &ParentRequestRow) -> bool {
    matches!(
        parent.status.as_str(),
        "completed" | "error" | "superseded" | "dead" | "interrupted"
    ) || matches!(
        parent.lifecycle_state.as_deref(),
        Some("completed" | "failed" | "superseded" | "dead" | "interrupted")
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::schema::ensure_schemas;

    async fn test_node() -> Arc<EmbeddedNode> {
        let node = Arc::new(EmbeddedNode::builder().build().await.unwrap());
        ensure_schemas(node.as_ref()).await.unwrap();
        node
    }

    #[tokio::test]
    async fn recovery_rejects_a_stale_snapshot_after_the_exact_call_terminalizes() {
        let node = test_node().await;
        let response = node
            .execute(
                r#"mutation {
                    add_InferenceCall(input: {
                        call_id: "call-stale-recovery"
                        runtime_instance_id: "runtime-stale-recovery"
                        request_id: "request-stale-recovery"
                        call_seq: 1
                        backend_id: "backend-stale-recovery"
                        behavior_id: "default"
                        agent_did: "did:key:stale-recovery"
                        call_kind: "inference"
                        attempt: 1
                        call_state: "completed"
                        priority: 0
                        queue_depth_at_enqueue: 0
                        controller_generation: 1
                        backend_config_fingerprint: "stale-recovery"
                    }) { _docID }
                }"#,
            )
            .await;
        assert!(!response.has_errors(), "{:?}", response.errors);
        let doc_id = mutation_doc_ids(response.data.as_ref(), "add_InferenceCall")
            .into_iter()
            .next()
            .expect("created InferenceCall _docID");
        let stale_row = StaleInferenceCallRow {
            doc_id: doc_id.clone(),
            call_id: "call-stale-recovery".to_owned(),
            runtime_instance_id: "runtime-stale-recovery".to_owned(),
            request_id: "request-stale-recovery".to_owned(),
            agent_did: "did:key:stale-recovery".to_owned(),
            controller_generation: 1,
            call_state: "running".to_owned(),
            failure_reason: None,
            ended_at: None,
        };

        let parent = ParentRequestRow {
            doc_id: "parent-stale-recovery".to_owned(),
            status: "completed".to_owned(),
            lifecycle_state: Some("completed".to_owned()),
        };
        let error = recover_inference_call_row(node.as_ref(), &stale_row, &parent)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("observed_state=completed"),
            "{error:#}"
        );

        let response = node
            .execute(&format!(
                r#"{{
                    InferenceCall(filter: {{ _docID: {{ _eq: "{}" }} }}) {{ call_state }}
                }}"#,
                escape_graphql_string(&doc_id)
            ))
            .await;
        assert!(!response.has_errors(), "{:?}", response.errors);
        assert_eq!(
            response.data.as_ref().unwrap()["InferenceCall"][0]["call_state"],
            "completed"
        );

        let response = node
            .execute(
                r#"mutation {
                    add_InferenceCall(input: {
                        call_id: "call-rederived-recovery"
                        runtime_instance_id: "runtime-rederived-recovery"
                        request_id: "request-rederived-recovery"
                        call_seq: 1
                        backend_id: "backend-rederived-recovery"
                        behavior_id: "default"
                        agent_did: "did:key:stale-recovery"
                        call_kind: "inference"
                        attempt: 1
                        call_state: "running"
                        priority: 0
                        queue_depth_at_enqueue: 0
                        controller_generation: 2
                        backend_config_fingerprint: "rederived-recovery"
                    }) { _docID }
                }"#,
            )
            .await;
        assert!(!response.has_errors(), "{:?}", response.errors);
        let rederived_doc_id = mutation_doc_ids(response.data.as_ref(), "add_InferenceCall")
            .into_iter()
            .next()
            .expect("created re-derived InferenceCall _docID");
        let stale_queued_snapshot = StaleInferenceCallRow {
            doc_id: rederived_doc_id.clone(),
            call_id: "call-rederived-recovery".to_owned(),
            runtime_instance_id: "runtime-rederived-recovery".to_owned(),
            request_id: "request-rederived-recovery".to_owned(),
            agent_did: "did:key:stale-recovery".to_owned(),
            controller_generation: 2,
            call_state: "queued".to_owned(),
            failure_reason: None,
            ended_at: None,
        };
        recover_inference_call_row(node.as_ref(), &stale_queued_snapshot, &parent)
            .await
            .expect("recovery re-observes running and re-derives failed outcome");
        let reloaded = reload_exact_call(node.as_ref(), &rederived_doc_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.call_state, "failed");
        assert_eq!(
            reloaded.failure_reason.as_deref(),
            Some("StreamDroppedBeforeTerminalResponse")
        );
    }

    #[tokio::test]
    async fn parent_selection_rejects_duplicate_logical_requests() {
        let rows = ["parent-first", "parent-second"].map(|doc_id| ParentRequestRow {
            doc_id: doc_id.to_owned(),
            status: "completed".to_owned(),
            lifecycle_state: Some("completed".to_owned()),
        });
        let error = select_parent_request(
            rows.into_iter().collect(),
            "did:key:ambiguous-parent",
            "request-ambiguous-parent",
        )
        .unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("ambiguous parent AgentRequest rows"),
            "{error:#}"
        );
        for doc_id in ["parent-first", "parent-second"] {
            assert!(message.contains(&doc_id), "{error:#}");
        }
    }
}
