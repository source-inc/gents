use std::collections::HashSet;

use serde::Deserialize;

use super::{validate_agent_request, AgentRequest, DefraWatcher};

mod rows;
use rows::{AgentRequestRow, SessionQueueRow};

pub(crate) const AGENT_REQUEST_FIELDS: &str = r#"
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
                    max_total_tokens
                    metadata
                    execution_origin
                    created_at
                    deadline
                    subagent_depth
                    caused_by_parent_request_id
                    caused_by_parent_request_doc_id
                    caused_by_parent_tool_call_id
                    caused_by_parent_tool_call_doc_id
                    caused_by_trigger_id
                    caused_by_trigger_kind
                    caused_by_source_doc_id
                    caused_by_correlation
                    caused_by_trigger_context
                    workspace_id
                    workspace_authority
                    workspace_owner_deployment_id
                    workspace_seal_hash
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
        match row.clone().into_agent_request() {
            Ok(request) => {
                if !self.request_is_locally_claimable(&request) {
                    return Ok(None);
                }
                Ok(Some(request))
            }
            Err(error) => {
                self.terminalize_incoherent_pending_request(&row, &error)
                    .await;
                Ok(None)
            }
        }
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

        let rows = active_runtime_rows(resp.data.as_ref())?;
        for row in &rows {
            if row.is_pending() {
                if let Err(error) = row.clone().into_agent_request() {
                    self.terminalize_incoherent_pending_request(row, &error)
                        .await;
                }
            }
        }

        prioritize_aged_background_wakes(claimable_pending_rows_from_rows(rows), chrono::Utc::now())
            .into_iter()
            .map(AgentRequestRow::into_agent_request)
            .filter(|request| {
                request
                    .as_ref()
                    .map(|request| self.request_is_locally_claimable(request))
                    .unwrap_or(true)
            })
            .collect()
    }

    async fn terminalize_incoherent_pending_request(
        &self,
        row: &AgentRequestRow,
        error: &anyhow::Error,
    ) {
        let doc_id = crate::graphql::escape_graphql_string(&row.doc_id);
        let failure_reason = crate::graphql::escape_graphql_string(&format!(
            "request rejected at ingest: incoherent durable lineage ({error})"
        ));
        let agent_did = crate::graphql::escape_graphql_string(&self.agent_did);
        let terminalized_at =
            crate::graphql::escape_graphql_string(&chrono::Utc::now().to_rfc3339());
        let mutation = format!(
            r#"mutation {{
                update_AgentRequest(
                    filter: {{
                        _docID: {{ _eq: "{doc_id}" }},
                        agent_did: {{ _eq: "{agent_did}" }},
                        status: {{ _eq: "pending" }},
                        lifecycle_state: {{ _eq: "pending" }}
                    }},
                    input: {{
                        status: "error",
                        lifecycle_state: "failed",
                        failure_reason: "{failure_reason}",
                        terminalized_at: "{terminalized_at}",
                        terminal_redrive_attempts: 0
                    }}
                ) {{ _docID }}
            }}"#
        );
        if let Err(persist_error) = crate::retry::execute_graphql_with_terminal_persistence_retry(
            self.node.as_ref(),
            &mutation,
            "terminalize_incoherent_pending_request",
        )
        .await
        {
            tracing::error!(
                doc_id = %row.doc_id,
                request_id = %row.request_id,
                error = %persist_error,
                "failed to terminalize incoherent AgentRequest",
            );
            return;
        }
        tracing::warn!(
            doc_id = %row.doc_id,
            request_id = %row.request_id,
            %error,
            "terminalized incoherent AgentRequest at watcher ingest",
        );
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

pub(crate) fn agent_request_from_mutation_response(
    response: &defra_node::QueryResponse,
    field: &str,
) -> anyhow::Result<Option<AgentRequest>> {
    crate::graphql::single_mutation_document(response, field)?
        .cloned()
        .map(serde_json::from_value::<AgentRequestRow>)
        .transpose()?
        .map(AgentRequestRow::into_agent_request)
        .transpose()
}

#[cfg(test)]
fn claimable_pending_rows(
    data: Option<&serde_json::Value>,
) -> anyhow::Result<Vec<AgentRequestRow>> {
    Ok(prioritize_aged_background_wakes(
        claimable_pending_rows_from_rows(active_runtime_rows(data)?),
        chrono::Utc::now(),
    ))
}

fn prioritize_aged_background_wakes(
    mut rows: Vec<AgentRequestRow>,
    now: chrono::DateTime<chrono::Utc>,
) -> Vec<AgentRequestRow> {
    // The query is already FIFO. Stable partitioning preserves that order
    // within both classes while preventing an old completion wake from being
    // perpetually overtaken by ordinary work. Once selected, the wake has at
    // most the bounded executor queue and active workers ahead of it.
    rows.sort_by_key(|row| !row.is_aged_background_completion_wakeup(now));
    rows
}

fn claimable_pending_rows_from_rows(rows: Vec<AgentRequestRow>) -> Vec<AgentRequestRow> {
    // Quarantine malformed pending work without allowing a second claim next
    // to malformed live work in the same session.
    let blocked_sessions = rows
        .iter()
        .filter(|row| !row.is_deprecated_background_completion_wakeup())
        .filter(|row| row.is_active_non_pending())
        .map(|row| row.session_id.clone())
        .collect::<HashSet<_>>();
    let rows = rows
        .into_iter()
        .filter_map(|row| match row.clone().into_agent_request() {
            Ok(_) => Some(row),
            Err(error) => {
                tracing::warn!(
                    doc_id = %row.doc_id,
                    request_id = %row.request_id,
                    %error,
                    "watcher quarantined incoherent AgentRequest row during pending scan",
                );
                None
            }
        })
        .collect::<Vec<_>>();
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

    claimable
}

#[cfg(test)]
mod tests {
    use super::{
        active_runtime_rows, claimable_pending_rows, claimable_pending_rows_from_rows,
        prioritize_aged_background_wakes,
    };

    fn versioned_wake_metadata(session_id: &str) -> String {
        serde_json::json!({
            "queue": {
                "source": "background_completion",
                "policy": "coalesce",
                "key": format!("background_completion:{session_id}"),
                "queued_after_request_id": "parent"
            },
            "background_completion_wake_version": 1
        })
        .to_string()
    }

    fn pending_row(
        request_id: &str,
        session_id: &str,
        created_at: &str,
        metadata: Option<String>,
        execution_origin: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "_docID": format!("doc-{request_id}"),
            "request_id": request_id,
            "agent_did": "did:agent:1",
            "behavior_id": "default",
            "session_id": session_id,
            "content": "work",
            "metadata": metadata,
            "execution_origin": execution_origin,
            "created_at": created_at,
            "status": "pending",
            "lifecycle_state": "pending"
        })
    }

    #[test]
    fn aged_completion_wake_moves_ahead_of_older_descendant() {
        let witness = crate::lean_vocab_test::lean_r6_backgrounding_case(
            "aged_background_wake_precedes_new_descendant",
        );
        assert!(witness.legal);
        assert_eq!(witness.reason.as_deref(), Some("aged_priority"));
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-12T22:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let data = serde_json::json!({
            "AgentRequest": [
                pending_row(
                    "older-descendant",
                    "descendant-session",
                    "2026-08-12T21:00:00Z",
                    None,
                    "interactive",
                ),
                pending_row(
                    "aged-wake",
                    "parent-session",
                    "2026-08-12T21:59:30Z",
                    Some(versioned_wake_metadata("parent-session")),
                    "scheduled",
                )
            ]
        });
        let rows = claimable_pending_rows_from_rows(active_runtime_rows(Some(&data)).unwrap());
        let ranked = prioritize_aged_background_wakes(rows, now);
        assert_eq!(ranked[0].request_id, "aged-wake");
        assert_eq!(ranked[1].request_id, "older-descendant");
    }

    #[test]
    fn fresh_completion_wake_preserves_fifo() {
        let witness = crate::lean_vocab_test::lean_r6_backgrounding_case(
            "fresh_background_wake_preserves_fifo",
        );
        assert!(!witness.legal);
        assert_eq!(witness.reason.as_deref(), Some("fifo"));
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-12T22:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let data = serde_json::json!({
            "AgentRequest": [
                pending_row(
                    "older-descendant",
                    "descendant-session",
                    "2026-08-12T21:59:00Z",
                    None,
                    "interactive",
                ),
                pending_row(
                    "fresh-wake",
                    "parent-session",
                    "2026-08-12T21:59:31Z",
                    Some(versioned_wake_metadata("parent-session")),
                    "scheduled",
                )
            ]
        });
        let rows = claimable_pending_rows_from_rows(active_runtime_rows(Some(&data)).unwrap());
        let ranked = prioritize_aged_background_wakes(rows, now);
        assert_eq!(ranked[0].request_id, "older-descendant");
        assert_eq!(ranked[1].request_id, "fresh-wake");
    }

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
