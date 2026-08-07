use std::collections::HashSet;

use serde::Deserialize;

use super::{validate_agent_request, AgentRequest, DefraWatcher};

const AGENT_REQUEST_FIELDS: &str = r#"
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
"#;

impl DefraWatcher {
    pub async fn try_fetch_request(&self, doc_id: &str) -> anyhow::Result<Option<AgentRequest>> {
        let query = format!(
            r#"{{
                AgentRequest(
                    filter: {{
                        _docID: {{ _eq: "{doc_id}" }},
                        agent_did: {{ _eq: "{agent_did}" }},
                        status: {{ _eq: "pending" }}
                    }},
                    limit: 1
                ) {{{fields}
                    status
                    lifecycle_state
                    interrupt_requested_at
                    valid_until
                }}
            }}"#,
            doc_id = doc_id,
            agent_did = self.agent_did,
            fields = AGENT_REQUEST_FIELDS,
        );

        let resp = self.node.execute(&query).await;
        if resp.has_errors() {
            anyhow::bail!("watcher query failed: {:?}", resp.errors);
        }

        let Some(row) = active_runtime_rows(resp.data.as_ref())?.into_iter().next() else {
            return Ok(None);
        };
        if row.is_deprecated_background_completion_wakeup() {
            return Ok(None);
        }
        if !self.row_is_claimable(&row).await? {
            return Ok(None);
        }
        row.into_agent_request().map(Some)
    }

    pub(super) async fn pending_requests(&self) -> anyhow::Result<Vec<AgentRequest>> {
        let active_runtime_states = crate::lifecycle::active_runtime_lifecycle_state_graphql_list();
        let query = format!(
            r#"{{
                AgentRequest(
                    filter: {{
                        agent_did: {{ _eq: "{agent_did}" }},
                        status: {{ _in: ["pending", "processing"] }},
                        lifecycle_state: {{ _in: {active_runtime_states} }}
                    }},
                    order: [{{ created_at: ASC }}, {{ request_id: ASC }}]
                ) {{{fields}
                    status
                    lifecycle_state
                    interrupt_requested_at
                    valid_until
                }}
            }}"#,
            agent_did = self.agent_did,
            active_runtime_states = active_runtime_states,
            fields = AGENT_REQUEST_FIELDS,
        );

        let resp = self.node.execute(&query).await;
        if resp.has_errors() {
            anyhow::bail!("watcher pending-request query failed: {:?}", resp.errors);
        }

        claimable_pending_rows(resp.data.as_ref())?
            .into_iter()
            .map(AgentRequestRow::into_agent_request)
            .collect()
    }

    async fn row_is_claimable(&self, row: &AgentRequestRow) -> anyhow::Result<bool> {
        if !row.is_pending() {
            return Ok(false);
        }
        if row.has_preclaim_terminal_signal() {
            return Ok(true);
        }

        let session_id = crate::graphql::escape_graphql_string(&row.session_id);
        let active_runtime_states = crate::lifecycle::active_runtime_lifecycle_state_graphql_list();
        let query = format!(
            r#"{{
                AgentRequest(
                    filter: {{
                        session_id: {{ _eq: "{session_id}" }},
                        status: {{ _in: ["pending", "processing"] }},
                        lifecycle_state: {{ _in: {active_runtime_states} }}
                    }},
                    order: [{{ created_at: ASC }}, {{ request_id: ASC }}]
                ) {{
                    _docID
                    request_id
                    status
                    lifecycle_state
                    execution_origin
                    metadata
                    created_at
                }}
            }}"#,
            session_id = session_id,
            active_runtime_states = active_runtime_states,
        );
        let resp = self.node.execute(&query).await;
        if resp.has_errors() {
            anyhow::bail!("watcher session queue query failed: {:?}", resp.errors);
        }

        let rows: Vec<SessionQueueRow> = resp
            .data
            .as_ref()
            .and_then(|d| d.get("AgentRequest"))
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        let active_blocker = rows
            .iter()
            .filter(|candidate| !candidate.is_deprecated_background_completion_wakeup())
            .any(|candidate| candidate.doc_id != row.doc_id && candidate.is_active_non_pending());
        if active_blocker {
            return Ok(false);
        }

        Ok(rows
            .iter()
            .filter(|candidate| !candidate.is_deprecated_background_completion_wakeup())
            .find(|candidate| candidate.is_pending())
            .is_some_and(|candidate| candidate.doc_id == row.doc_id))
    }
}

fn active_runtime_rows(data: Option<&serde_json::Value>) -> anyhow::Result<Vec<AgentRequestRow>> {
    match data.and_then(|d| d.get("AgentRequest")) {
        Some(value) => Ok(serde_json::from_value(value.clone())?),
        None => Ok(Vec::new()),
    }
}

fn claimable_pending_rows(
    data: Option<&serde_json::Value>,
) -> anyhow::Result<Vec<AgentRequestRow>> {
    let rows = active_runtime_rows(data)?;
    let blocked_sessions = rows
        .iter()
        .filter(|row| !row.is_deprecated_background_completion_wakeup())
        .filter(|row| row.is_active_non_pending())
        .map(|row| row.session_id.clone())
        .collect::<HashSet<_>>();
    let mut seen_pending_sessions = HashSet::new();
    let mut claimable = Vec::new();

    for row in rows {
        if row.is_deprecated_background_completion_wakeup() {
            continue;
        }
        let is_pending = row.is_pending();
        let is_preclaim_terminal = row.has_preclaim_terminal_signal();
        let pending_session_seen = seen_pending_sessions.contains(&row.session_id);
        let session_blocked = blocked_sessions.contains(&row.session_id);

        if is_pending && (is_preclaim_terminal || (!session_blocked && !pending_session_seen)) {
            claimable.push(row.clone());
        }

        if is_pending {
            seen_pending_sessions.insert(row.session_id.clone());
        }
    }

    Ok(claimable)
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

#[derive(Clone, Deserialize)]
struct AgentRequestRow {
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
    status: String,
    lifecycle_state: Option<String>,
    interrupt_requested_at: Option<String>,
    valid_until: Option<String>,
}

#[derive(Deserialize)]
struct SessionQueueRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    status: String,
    lifecycle_state: Option<String>,
    execution_origin: Option<String>,
    metadata: Option<String>,
}

impl SessionQueueRow {
    fn is_pending(&self) -> bool {
        self.status == "pending" && self.lifecycle_state.as_deref() == Some("pending")
    }

    fn is_active_non_pending(&self) -> bool {
        !self.is_pending()
    }

    fn is_deprecated_background_completion_wakeup(&self) -> bool {
        crate::lifecycle::queue::is_deprecated_background_completion_wakeup(
            self.execution_origin.as_deref(),
            self.metadata.as_deref(),
        )
    }
}

impl AgentRequestRow {
    fn is_pending(&self) -> bool {
        self.status == "pending" && self.lifecycle_state.as_deref() == Some("pending")
    }

    fn is_active_non_pending(&self) -> bool {
        !self.is_pending()
    }

    fn has_preclaim_terminal_signal(&self) -> bool {
        if normalize_optional_string(self.interrupt_requested_at.clone()).is_some() {
            return true;
        }
        normalize_optional_string(self.valid_until.clone()).is_some_and(|value| {
            chrono::DateTime::parse_from_rfc3339(&value)
                .map(|dt| chrono::Utc::now() > dt.with_timezone(&chrono::Utc))
                .unwrap_or(false)
        })
    }

    fn is_deprecated_background_completion_wakeup(&self) -> bool {
        crate::lifecycle::queue::is_deprecated_background_completion_wakeup(
            self.execution_origin.as_deref(),
            self.metadata.as_deref(),
        )
    }

    fn into_agent_request(self) -> anyhow::Result<AgentRequest> {
        let req = AgentRequest {
            doc_id: self.doc_id,
            request_id: self.request_id,
            agent_did: self.agent_did,
            requester_did: normalize_optional_string(self.requester_did),
            behavior_id: normalize_optional_string(self.behavior_id),
            session_id: self.session_id,
            content: self.content,
            temperature: self.temperature,
            top_p: self.top_p,
            top_k: self.top_k,
            seed: self.seed,
            max_tokens: self.max_tokens,
            metadata: self.metadata,
            execution_origin: normalize_optional_string(self.execution_origin),
            created_at: self.created_at,
            deadline: normalize_optional_string(self.deadline),
            subagent_depth: self.subagent_depth.unwrap_or(0),
            caused_by_parent_request_id: self.caused_by_parent_request_id,
            caused_by_parent_tool_call_id: self.caused_by_parent_tool_call_id,
        };
        validate_agent_request(&req)?;
        Ok(req)
    }
}

#[cfg(test)]
mod tests {
    use super::claimable_pending_rows;

    #[test]
    fn processing_legacy_wake_does_not_block_interactive_request() {
        let wake_metadata = serde_json::json!({
            "queue": {
                "source": "background_completion",
                "policy": "coalesce",
                "key": "child-1",
                "queued_after_request_id": null
            }
        })
        .to_string();
        let data = serde_json::json!({
            "AgentRequest": [
                {
                    "_docID": "wake-doc",
                    "request_id": "legacy-wake",
                    "agent_did": "did:agent:1",
                    "behavior_id": "default",
                    "session_id": "session-1",
                    "content": "legacy wake",
                    "metadata": wake_metadata,
                    "execution_origin": "scheduled",
                    "created_at": "2026-07-01T00:00:00Z",
                    "status": "processing",
                    "lifecycle_state": "processing"
                },
                {
                    "_docID": "user-doc",
                    "request_id": "interactive",
                    "agent_did": "did:agent:1",
                    "behavior_id": "default",
                    "session_id": "session-1",
                    "content": "hello",
                    "execution_origin": "interactive",
                    "created_at": "2026-07-01T00:00:01Z",
                    "status": "pending",
                    "lifecycle_state": "pending"
                }
            ]
        });

        let claimable = claimable_pending_rows(Some(&data)).expect("claimable rows");
        assert_eq!(claimable.len(), 1);
        assert_eq!(claimable[0].request_id, "interactive");
    }
}
