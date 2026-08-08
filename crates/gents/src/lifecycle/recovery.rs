use std::collections::BTreeMap;

use super::lookup::lookup_request_status_by_request_id;
use super::*;

impl RequestLifecycle {
    pub async fn recover_all(node: &EmbeddedNode, agent_did: &str) -> Result<RecoveryReport> {
        let responses_recovered = recover_stuck_responses(node, agent_did).await?
            + recover_missing_response_documents(node, agent_did).await?;
        let requests_recovered = Self::repair_terminal_requests(node, agent_did)
            .await?
            .repaired;
        let conversations = recover_stuck_conversations(node, agent_did).await?;

        Ok(RecoveryReport {
            responses_recovered,
            requests_recovered,
            conversations_recovered: conversations.recovered,
            conversations_failed: conversations.failed,
            duplicate_conversation_sessions: conversations.duplicate_sessions,
        })
    }

    /// Owner-scoped terminal-convergence re-drive (#664).
    ///
    /// Under `subagent-host` replication a routed `AgentRequest` is replicated
    /// to its `requester_did` peer. Safety already holds (the watcher
    /// `agent_did` filter never lets that peer claim a foreign replica), but
    /// liveness does not: when the owner terminalizes, the terminal delta
    /// reaches the requester via a single one-shot PushLog that can drop, and
    /// there is no per-doc anti-entropy on a running peer (defradb.rs#1074) to
    /// re-request it. This re-drive is the owner side of the fix — periodically
    /// re-asserting the current terminal value of recently-terminalized routed
    /// requests. Local-only requests are excluded because no peer consumes
    /// their request state (#683). A same-value re-write is a genuine
    /// higher-priority CRDT delta (it does not no-op), so it flows through the
    /// normal PushLog path and a lagging requester accepts it (LWW, higher
    /// priority ⇒ applied).
    ///
    /// BOUNDED, NOT CONVERGENCE-OBSERVING. The owner has no back-channel telling
    /// it whether a peer caught up. Each successful re-assert atomically advances
    /// the persisted `terminal_redrive_attempts` counter, and eligibility stops
    /// at [`TERMINAL_REDRIVE_CAP`] across process restarts. Candidate ordering is
    /// `terminalized_at ASC`, not request creation time; exhausted rows leave the
    /// query, so bounded batches eventually cover an arbitrarily old request that
    /// terminalized late. A peer unavailable through the whole budget is repaired
    /// by a bounded full replicator replay when the pairing reconnects; that path
    /// authors no same-value request delta and therefore grows no request history.
    ///
    /// `agent_did` MUST be the runtime's own DID: only the owner re-asserts its
    /// own documents; peers stay passive (a peer-authored delta to a foreign doc
    /// would fork the CRDT, not converge it). `agent_did` itself is never
    /// written (it is `@immutable`); only the mutable terminal `status`,
    /// `lifecycle_state`, and bounded attempt counter are written together in
    /// one document update.
    ///
    pub async fn redrive_terminal_convergence(
        node: &EmbeddedNode,
        agent_did: &str,
    ) -> Result<TerminalRedriveReport> {
        let escaped_agent_did = escape_graphql_string(agent_did);
        let terminal_states = crate::lifecycle::terminal_lifecycle_state_graphql_list();
        let query = format!(
            r#"{{
                AgentRequest(
                    filter: {{
                        agent_did: {{ _eq: "{escaped_agent_did}" }},
                        requester_did: {{ _neq: null }},
                        lifecycle_state: {{ _in: {terminal_states} }},
                        terminal_redrive_attempts: {{ _lt: {cap} }}
                    }},
                    order: [{{ terminalized_at: ASC }}, {{ request_id: ASC }}],
                    limit: {limit}
                ) {{
                    _docID
                    request_id
                    status
                    lifecycle_state
                    terminal_redrive_attempts
                }}
            }}"#,
            limit = TERMINAL_REDRIVE_BATCH_LIMIT,
            cap = TERMINAL_REDRIVE_CAP,
        );

        let resp = node.execute(&query).await;
        if resp.has_errors() {
            anyhow::bail!("querying terminal requests to re-drive: {:?}", resp.errors);
        }

        let rows: Vec<serde_json::Value> = resp
            .data
            .as_ref()
            .and_then(|d| d.get("AgentRequest"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let mut candidates: Vec<(String, String, String, String, u32)> = Vec::new();
        for row in &rows {
            let doc_id = row.get("_docID").and_then(|v| v.as_str()).unwrap_or("");
            let request_id = row.get("request_id").and_then(|v| v.as_str()).unwrap_or("");
            let status = row.get("status").and_then(|v| v.as_str()).unwrap_or("");
            let lifecycle_state = row
                .get("lifecycle_state")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let attempts = row
                .get("terminal_redrive_attempts")
                .and_then(|value| value.as_u64())
                .unwrap_or(TERMINAL_REDRIVE_CAP as u64) as u32;
            if doc_id.is_empty() || status.is_empty() || lifecycle_state.is_empty() {
                continue;
            }
            candidates.push((
                doc_id.to_string(),
                request_id.to_string(),
                status.to_string(),
                lifecycle_state.to_string(),
                attempts,
            ));
        }

        let scanned = candidates.len();
        let mut reasserted = 0usize;
        let mut failed = 0usize;
        for (doc_id, request_id, status, lifecycle_state, attempts) in &candidates {
            let next_attempts = attempts.saturating_add(1);
            let escaped_doc_id = escape_graphql_string(doc_id);
            let escaped_status = escape_graphql_string(status);
            let escaped_lifecycle_state = escape_graphql_string(lifecycle_state);
            // Defense-in-depth: the candidate query is already `agent_did == self`
            // scoped, but keep the mutation itself owner-scoped too, matching the
            // queue.rs seam guards — a re-drive must never touch a foreign replica.
            let mutation = format!(
                r#"mutation {{
                    update_AgentRequest(
                        filter: {{
                            _docID: {{ _eq: "{escaped_doc_id}" }},
                            agent_did: {{ _eq: "{escaped_agent_did}" }},
                            requester_did: {{ _neq: null }},
                            lifecycle_state: {{ _eq: "{escaped_lifecycle_state}" }},
                            terminal_redrive_attempts: {{ _eq: {attempts} }}
                        }},
                        input: {{
                            status: "{escaped_status}",
                            lifecycle_state: "{escaped_lifecycle_state}",
                            terminal_redrive_attempts: {next_attempts}
                        }}
                    ) {{ _docID }}
                }}"#,
            );

            let resp = node.execute(&mutation).await;
            if resp.has_errors() {
                tracing::warn!(
                    doc_id = %doc_id,
                    request_id = %request_id,
                    status = %status,
                    errors = ?resp.errors,
                    "failed to re-drive terminal request convergence"
                );
                failed += 1;
                continue;
            }

            let updated = resp
                .data
                .as_ref()
                .and_then(|data| data.get("update_AgentRequest"))
                .is_some_and(response_has_documents);
            if !updated {
                continue;
            }
            reasserted += 1;
            tracing::debug!(
                doc_id = %doc_id,
                request_id = %request_id,
                status = %status,
                lifecycle_state = %lifecycle_state,
                terminal_redrive_attempts = next_attempts,
                "re-asserted terminal request state to converge replicas"
            );
        }

        Ok(TerminalRedriveReport {
            reasserted,
            scanned,
            failed,
        })
    }

    pub async fn repair_terminal_requests(
        node: &EmbeddedNode,
        agent_did: &str,
    ) -> Result<TerminalRepairReport> {
        // Key the stale predicate on `lifecycle_state ∈ {claimed, processing}` to
        // mirror the Lean `Recovery.requestRecoveryStale` model exactly, rather than
        // on the coarser `status = "processing"`. A stuck `claimed` own-request is
        // now recovered even if its `status` is not `"processing"`.
        let stale_states = crate::lifecycle::stuck_request_lifecycle_state_graphql_list();
        let escaped_agent_did = escape_graphql_string(agent_did);
        let query = format!(
            r#"{{
            AgentRequest(
                filter: {{
                    agent_did: {{ _eq: "{escaped_agent_did}" }},
                    lifecycle_state: {{ _in: {stale_states} }}
                }}
            ) {{
                _docID
                request_id
                behavior_id
                session_id
                retry_count
            }}
        }}"#
        );

        let resp = node.execute(&query).await;
        if resp.has_errors() {
            anyhow::bail!("querying stuck requests: {:?}", resp.errors);
        }

        let rows: Vec<serde_json::Value> = resp
            .data
            .as_ref()
            .and_then(|d| d.get("AgentRequest"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let mut report = TerminalRepairReport {
            scanned: rows.len(),
            ..Default::default()
        };
        for row in &rows {
            let doc_id = row.get("_docID").and_then(|v| v.as_str()).unwrap_or("");
            let request_id = row.get("request_id").and_then(|v| v.as_str()).unwrap_or("");
            let session_id = row.get("session_id").and_then(|v| v.as_str()).unwrap_or("");
            let retry_count = row.get("retry_count").and_then(|v| v.as_i64()).unwrap_or(0);
            let outcome =
                crate::response_outcome::load_accepted_response_outcome(node, agent_did, doc_id)
                    .await?;
            let Some(outcome) = outcome else {
                report.awaiting_outcome += 1;
                continue;
            };
            let (next_status, next_lifecycle_state) = match outcome.kind {
                crate::response_outcome::ResponseOutcomeKind::Complete => {
                    ("completed", PersistedLifecycleState::Completed.as_str())
                }
                crate::response_outcome::ResponseOutcomeKind::Interrupted => {
                    ("interrupted", PersistedLifecycleState::Interrupted.as_str())
                }
                crate::response_outcome::ResponseOutcomeKind::Error => {
                    ("error", PersistedLifecycleState::Failed.as_str())
                }
            };
            let terminalized_at = outcome.terminalized_at;
            let escaped_terminalized_at = escape_graphql_string(&terminalized_at);
            let escaped_doc_id = escape_graphql_string(doc_id);
            let failure_reason = match next_lifecycle_state {
                state if state == PersistedLifecycleState::Completed.as_str() => "",
                state if state == PersistedLifecycleState::Interrupted.as_str() => "interrupted",
                _ => outcome.reason_code.as_deref().unwrap_or("response_error"),
            };
            let escaped_failure_reason = escape_graphql_string(failure_reason);
            let escaped_agent_did = escape_graphql_string(agent_did);
            let stale_states = crate::lifecycle::stuck_request_lifecycle_state_graphql_list();

            let mutation = format!(
                r#"mutation {{
                update_AgentRequest(
                    filter: {{
                        _docID: {{ _eq: "{escaped_doc_id}" }},
                        agent_did: {{ _eq: "{escaped_agent_did}" }},
                        lifecycle_state: {{ _in: {stale_states} }}
                    }},
                    input: {{
                        status: "{next_status}",
                        lifecycle_state: "{next_lifecycle_state}",
                        failure_reason: "{escaped_failure_reason}",
                        terminalized_at: "{escaped_terminalized_at}",
                        terminal_redrive_attempts: 0
                    }}
                ) {{ _docID }}
            }}"#,
            );

            match crate::retry::execute_graphql_with_terminal_persistence_retry(
                node,
                &mutation,
                "repair_terminal_request",
            )
            .await
            {
                Err(error) => {
                    tracing::warn!(
                        doc_id = %doc_id,
                        request_id = %request_id,
                        session_id = %session_id,
                        next_status = %next_status,
                        outcome_kind = %outcome.kind.as_str(),
                        error = %error,
                        "failed to recover stuck request"
                    );
                    report.failed += 1;
                }
                Ok(resp) => {
                    let updated = resp
                        .data
                        .as_ref()
                        .and_then(|data| data.get("update_AgentRequest"))
                        .is_some_and(response_has_documents);
                    if !updated {
                        continue;
                    }
                    report.repaired += 1;
                    tracing::info!(
                        doc_id = %doc_id,
                        request_id = %request_id,
                        session_id = %session_id,
                        retry_count = retry_count,
                        outcome_kind = %outcome.kind.as_str(),
                        "recovered stuck request: processing → {next_status}"
                    );
                }
            }
        }

        Ok(report)
    }
}

fn recovery_identity(node: &EmbeddedNode, agent_did: &str) -> Result<identity::Did> {
    let node_did = node
        .node_identity_did()
        .ok_or_else(|| anyhow::anyhow!("response recovery requires a DefraDB node identity"))?;
    if agent_did.trim().is_empty() {
        anyhow::bail!("response recovery requires a semantic agent DID");
    }
    identity::Did::new(node_did).map_err(Into::into)
}

async fn stage_recovery_outcome_timestamp(
    node: &EmbeddedNode,
    identity: &identity::Did,
    response_doc_id: &str,
    existing: Option<&str>,
) -> Result<String> {
    if let Some(existing) = existing.filter(|value| !value.trim().is_empty()) {
        return Ok(existing.to_string());
    }
    let proposed = chrono::Utc::now().to_rfc3339();
    let escaped_doc_id = escape_graphql_string(response_doc_id);
    let escaped_proposed = escape_graphql_string(&proposed);
    let mutation = format!(
        r#"mutation {{
            update_AgentResponse(
                filter: {{
                    _docID: {{ _eq: "{escaped_doc_id}" }},
                    status: {{ _eq: "streaming" }},
                    outcome_terminalized_at: {{ _eq: null }}
                }},
                input: {{ outcome_terminalized_at: "{escaped_proposed}" }}
            ) {{ _docID }}
        }}"#
    );
    let response = node
        .execute_request_with_retry(
            defra_node::QueryRequest::new(mutation).with_identity(Some(identity.clone())),
            defra_node::ExecuteRetryPolicy::default(),
        )
        .await;
    if response.has_errors() {
        anyhow::bail!(
            "staging recovery outcome timestamp for AgentResponse {response_doc_id}: {:?}",
            response.errors
        );
    }
    let query = format!(
        r#"query {{
            AgentResponse(filter: {{ _docID: {{ _eq: "{escaped_doc_id}" }} }}) {{
                outcome_terminalized_at
            }}
        }}"#
    );
    let response = node
        .execute_request_with_retry(
            defra_node::QueryRequest::new(query).with_identity(Some(identity.clone())),
            defra_node::ExecuteRetryPolicy::default(),
        )
        .await;
    if response.has_errors() {
        anyhow::bail!(
            "reloading recovery outcome timestamp for AgentResponse {response_doc_id}: {:?}",
            response.errors
        );
    }
    response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentResponse"))
        .and_then(|rows| rows.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("outcome_terminalized_at"))
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("recovery outcome timestamp was not persisted"))
}

async fn recover_stuck_responses(node: &EmbeddedNode, agent_did: &str) -> Result<usize> {
    let now = escape_graphql_string(&chrono::Utc::now().to_rfc3339());
    let escaped_agent_did = escape_graphql_string(agent_did);
    let query = format!(
        r#"{{
            AgentResponse(
                filter: {{
                    agent_did: {{ _eq: "{escaped_agent_did}" }},
                    status: {{ _eq: "streaming" }}
                }}
            ) {{
                _docID
                request_id
                request_doc_id
                requester_did
                behavior_id
                session_id
                request_source_composite_commit_cid
                request_source_signer_did
                request_claim_composite_commit_cid
                request_claim_signer_did
                final_message_doc_id
                final_message_composite_commit_cid
                final_message_signer_did
                final_message_sequence
                outcome_terminalized_at
                content
            }}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("querying stuck responses: {:?}", resp.errors);
    }

    let rows: Vec<serde_json::Value> = resp
        .data
        .as_ref()
        .and_then(|d| d.get("AgentResponse"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let mut count = 0;
    for row in &rows {
        let doc_id = row.get("_docID").and_then(|v| v.as_str()).unwrap_or("");
        let request_id = row.get("request_id").and_then(|v| v.as_str()).unwrap_or("");
        let request_doc_id = row
            .get("request_doc_id")
            .and_then(|value| value.as_str())
            .filter(|value| !value.trim().is_empty());
        let existing_content = row.get("content").and_then(|v| v.as_str()).unwrap_or("");
        if let Some(request_doc_id) = request_doc_id {
            let identity = recovery_identity(node, agent_did)?;
            let accepted = crate::response_outcome::load_accepted_response_outcome(
                node,
                agent_did,
                request_doc_id,
            )
            .await?;
            let accepted = match accepted {
                Some(accepted) => accepted,
                None => {
                    let required = |field: &str| -> Result<&str> {
                        row.get(field)
                            .and_then(|value| value.as_str())
                            .filter(|value| !value.trim().is_empty())
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "AgentResponse {doc_id} is missing recovery provenance field {field}"
                                )
                            })
                    };
                    let provenance = crate::RequestExecutionProvenance::new(
                        crate::SignedDocumentVersionRef::new(
                            crate::DocumentVersionRef::new(
                                request_doc_id,
                                required("request_source_composite_commit_cid")?,
                            ),
                            required("request_source_signer_did")?,
                        ),
                        crate::SignedDocumentVersionRef::new(
                            crate::DocumentVersionRef::new(
                                request_doc_id,
                                required("request_claim_composite_commit_cid")?,
                            ),
                            required("request_claim_signer_did")?,
                        ),
                    );
                    let message_fields = (
                        row.get("final_message_doc_id")
                            .and_then(|value| value.as_str()),
                        row.get("final_message_composite_commit_cid")
                            .and_then(|value| value.as_str()),
                        row.get("final_message_signer_did")
                            .and_then(|value| value.as_str()),
                        row.get("final_message_sequence")
                            .and_then(|value| value.as_u64())
                            .map(|value| value as u32),
                    );
                    let final_message = match message_fields {
                        (None, None, None, None) => None,
                        (Some(doc_id), Some(cid), Some(signer_did), Some(sequence)) => {
                            Some(crate::MessageFactRef {
                                sequence,
                                doc_id: doc_id.to_string(),
                                composite_commit_cid: cid.to_string(),
                                signer_did: signer_did.to_string(),
                            })
                        }
                        _ => anyhow::bail!(
                            "AgentResponse {doc_id} has partial final-message recovery provenance"
                        ),
                    };
                    let terminalized_at = stage_recovery_outcome_timestamp(
                        node,
                        &identity,
                        doc_id,
                        row.get("outcome_terminalized_at")
                            .and_then(|value| value.as_str()),
                    )
                    .await?;
                    crate::response_outcome::publish_response_outcome(
                        node,
                        crate::response_outcome::ResponseOutcomeInput {
                            request_id,
                            session_id: required("session_id")?,
                            agent_did,
                            requester_did: row
                                .get("requester_did")
                                .and_then(|value| value.as_str())
                                .filter(|value| !value.trim().is_empty()),
                            behavior_id: required("behavior_id")?,
                            provenance: &provenance,
                            kind: crate::response_outcome::ResponseOutcomeKind::Error,
                            reason_code: Some("daemon_restart"),
                            final_message: final_message.as_ref(),
                            terminalized_at: &terminalized_at,
                        },
                    )
                    .await?;
                    crate::response_outcome::load_accepted_response_outcome(
                        node,
                        agent_did,
                        request_doc_id,
                    )
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("published recovery outcome disappeared"))?
                }
            };
            let (status, error_message, interrupted_at) = match accepted.kind {
                crate::response_outcome::ResponseOutcomeKind::Complete => ("complete", "", None),
                crate::response_outcome::ResponseOutcomeKind::Error => (
                    "error",
                    accepted.reason_code.as_deref().unwrap_or("response_error"),
                    None,
                ),
                crate::response_outcome::ResponseOutcomeKind::Interrupted => (
                    "error",
                    accepted.reason_code.as_deref().unwrap_or("interrupted"),
                    Some(accepted.terminalized_at.as_str()),
                ),
            };
            let interrupted_input = interrupted_at
                .map(|at| format!(r#"interrupted_at: "{}","#, escape_graphql_string(at)))
                .unwrap_or_default();
            let mutation = format!(
                r#"mutation {{
                    update_AgentResponse(
                        filter: {{
                            _docID: {{ _eq: "{}" }},
                            status: {{ _eq: "streaming" }}
                        }},
                        input: {{
                            content: ""
                            reasoning: ""
                            status: "{}"
                            error_message: "{}"
                            {interrupted_input}
                            completed_at: "{}"
                        }}
                    ) {{ _docID }}
                }}"#,
                escape_graphql_string(doc_id),
                status,
                escape_graphql_string(error_message),
                escape_graphql_string(&accepted.terminalized_at),
            );
            let response = node
                .execute_request_with_retry(
                    defra_node::QueryRequest::new(mutation).with_identity(Some(identity.clone())),
                    defra_node::ExecuteRetryPolicy::default(),
                )
                .await;
            if response.has_errors() {
                tracing::warn!(
                    doc_id = %doc_id,
                    request_id = %request_id,
                    errors = ?response.errors,
                    "failed to project accepted outcome onto stuck live response"
                );
            } else {
                count += 1;
            }
            continue;
        }

        // Breaking-generation legacy/test fallback for a live row that does
        // not carry exact request provenance. New production rows take the
        // outcome-first branch above.
        let error_suffix = if existing_content.trim().is_empty() {
            "Error: daemon restarted before response could be generated"
        } else {
            "\n\n[Response interrupted — daemon restarted]"
        };
        let final_content = format!("{existing_content}{error_suffix}");
        let escaped_content = escape_graphql_string(&final_content);
        let escaped_error_message =
            escape_graphql_string("daemon restarted before response could be finalized");
        let escaped_doc_id = escape_graphql_string(doc_id);

        let mutation = format!(
            r#"mutation {{
                update_AgentResponse(
                    filter: {{ _docID: {{ _eq: "{escaped_doc_id}" }} }},
                    input: {{
                        content: "{escaped_content}",
                        status: "error",
                        error_message: "{escaped_error_message}",
                        completed_at: "{now}"
                    }}
                ) {{ _docID }}
            }}"#
        );

        let resp = crate::retry::execute_graphql_with_terminal_persistence_retry(
            node,
            &mutation,
            "recover_stuck_response",
        )
        .await;
        if let Err(error) = resp {
            tracing::warn!(
                doc_id = %doc_id,
                request_id = %request_id,
                error = %error,
                "failed to finalize stuck response"
            );
        } else {
            count += 1;
            tracing::info!(
                doc_id = %doc_id,
                request_id = %request_id,
                "recovered stuck response: streaming → error"
            );
        }
    }

    Ok(count)
}

async fn recover_missing_response_documents(node: &EmbeddedNode, agent_did: &str) -> Result<usize> {
    #[derive(serde::Deserialize)]
    struct MissingResponseRequestRow {
        #[serde(rename = "_docID")]
        doc_id: String,
        request_id: String,
        requester_did: Option<String>,
        behavior_id: Option<String>,
        session_id: String,
    }

    let identity = recovery_identity(node, agent_did)?;
    let escaped_agent_did = escape_graphql_string(agent_did);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    agent_did: {{ _eq: "{escaped_agent_did}" }},
                    status: {{ _eq: "processing" }},
                    lifecycle_state: {{ _in: ["claimed", "processing"] }}
                }}
            ) {{
                _docID
                request_id
                requester_did
                behavior_id
                session_id
            }}
        }}"#
    );

    let resp = node
        .execute_request_with_retry(
            defra_node::QueryRequest::new(query).with_identity(Some(identity.clone())),
            defra_node::ExecuteRetryPolicy::default(),
        )
        .await;
    if resp.has_errors() {
        anyhow::bail!(
            "querying processing requests for missing responses: {:?}",
            resp.errors
        );
    }

    let rows: Vec<MissingResponseRequestRow> = resp
        .data
        .as_ref()
        .and_then(|d| d.get("AgentRequest"))
        .cloned()
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_default();

    let mut recovered = 0;
    for row in rows {
        let request_id = row.request_id.trim();
        let session_id = row.session_id.trim();
        if request_id.is_empty() || session_id.is_empty() {
            tracing::warn!(
                doc_id = %row.doc_id,
                request_id,
                "cannot recover missing response without complete immutable request lineage"
            );
            continue;
        }

        let escaped_request_doc_id = escape_graphql_string(&row.doc_id);
        let response_query = format!(
            r#"query {{
                AgentResponse(
                    filter: {{ request_doc_id: {{ _eq: "{escaped_request_doc_id}" }} }}
                ) {{ _docID }}
            }}"#
        );
        let response = node
            .execute_request_with_retry(
                defra_node::QueryRequest::new(response_query).with_identity(Some(identity.clone())),
                defra_node::ExecuteRetryPolicy::default(),
            )
            .await;
        if response.has_errors() {
            tracing::warn!(
                doc_id = %row.doc_id,
                request_id,
                errors = ?response.errors,
                "failed to check exact live response identity during recovery"
            );
            continue;
        }
        let response_count = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentResponse"))
            .and_then(serde_json::Value::as_array)
            .map(Vec::len)
            .unwrap_or(0);
        if response_count != 0 {
            continue;
        }

        let reconstructed = match super::reconstruct_execution_provenance_from_claim_ancestry(
            node,
            &row.doc_id,
            agent_did,
        )
        .await
        {
            Ok(reconstructed) => reconstructed,
            Err(error) => {
                tracing::warn!(
                    doc_id = %row.doc_id,
                    request_id,
                    error = %error,
                    "failed to reconstruct exact claim provenance for missing response"
                );
                continue;
            }
        };
        let claimed_request = &reconstructed.claimed_request;
        if row.request_id.as_str() != claimed_request.request_id.as_str()
            || row.session_id.as_str() != claimed_request.session_id.as_str()
            || row.requester_did.as_deref() != claimed_request.requester_did.as_deref()
            || row.behavior_id.as_deref() != claimed_request.behavior_id.as_deref()
        {
            tracing::warn!(
                doc_id = %row.doc_id,
                request_id,
                "current missing-response request changed logical lineage after its signed claim"
            );
            continue;
        }
        let request_id = claimed_request.request_id.as_str();
        let session_id = claimed_request.session_id.as_str();
        let Some(behavior_id) = claimed_request
            .behavior_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        else {
            tracing::warn!(
                doc_id = %row.doc_id,
                request_id,
                "signed claim has no behavior_id for missing-response recovery"
            );
            continue;
        };

        let accepted = match crate::response_outcome::load_accepted_response_outcome(
            node,
            agent_did,
            &row.doc_id,
        )
        .await
        {
            Ok(Some(accepted)) => accepted,
            Ok(None) => {
                let terminalized_at = chrono::Utc::now().to_rfc3339();
                if let Err(error) = crate::response_outcome::publish_response_outcome(
                    node,
                    crate::response_outcome::ResponseOutcomeInput {
                        request_id,
                        session_id,
                        agent_did,
                        requester_did: claimed_request
                            .requester_did
                            .as_deref()
                            .filter(|value| !value.trim().is_empty()),
                        behavior_id,
                        provenance: &reconstructed.provenance,
                        kind: crate::response_outcome::ResponseOutcomeKind::Error,
                        reason_code: Some("daemon_restart_missing_response"),
                        final_message: None,
                        terminalized_at: &terminalized_at,
                    },
                )
                .await
                {
                    tracing::warn!(
                        doc_id = %row.doc_id,
                        request_id,
                        error = %error,
                        "failed to publish recovery outcome for missing AgentResponse"
                    );
                    continue;
                }
                match crate::response_outcome::load_accepted_response_outcome(
                    node,
                    agent_did,
                    &row.doc_id,
                )
                .await
                {
                    Ok(Some(accepted)) => accepted,
                    Ok(None) => {
                        tracing::warn!(
                            doc_id = %row.doc_id,
                            request_id,
                            "published missing-response outcome disappeared"
                        );
                        continue;
                    }
                    Err(error) => {
                        tracing::warn!(
                            doc_id = %row.doc_id,
                            request_id,
                            error = %error,
                            "failed to reload missing-response outcome"
                        );
                        continue;
                    }
                }
            }
            Err(error) => {
                tracing::warn!(
                    doc_id = %row.doc_id,
                    request_id,
                    error = %error,
                    "failed to load missing-response outcome"
                );
                continue;
            }
        };
        if accepted.request_id != request_id || accepted.session_id != session_id {
            tracing::warn!(
                doc_id = %row.doc_id,
                request_id = %request_id,
                session_id = %session_id,
                "accepted outcome does not match missing-response request lineage"
            );
            continue;
        }
        if accepted.provenance != reconstructed.provenance {
            tracing::warn!(
                doc_id = %row.doc_id,
                request_id,
                outcome_signer_did = %accepted.outcome_signer_did,
                "accepted outcome does not retain the reconstructed request provenance"
            );
            continue;
        }

        let (status, lifecycle_state, failure_reason) = match accepted.kind {
            crate::response_outcome::ResponseOutcomeKind::Complete => {
                ("completed", "completed", "")
            }
            crate::response_outcome::ResponseOutcomeKind::Error => (
                "error",
                "failed",
                accepted.reason_code.as_deref().unwrap_or("response_error"),
            ),
            crate::response_outcome::ResponseOutcomeKind::Interrupted => (
                "interrupted",
                "interrupted",
                accepted.reason_code.as_deref().unwrap_or("interrupted"),
            ),
        };
        let mutation = format!(
            r#"mutation {{
                update_AgentRequest(
                    filter: {{
                        _docID: {{ _eq: "{}" }},
                        agent_did: {{ _eq: "{}" }},
                        status: {{ _eq: "processing" }},
                        lifecycle_state: {{ _in: ["claimed", "processing"] }}
                    }},
                    input: {{
                        status: "{}"
                        lifecycle_state: "{}"
                        failure_reason: "{}"
                        terminalized_at: "{}"
                        terminal_redrive_attempts: 0
                    }}
                ) {{ _docID }}
            }}"#,
            escape_graphql_string(&row.doc_id),
            escape_graphql_string(agent_did),
            status,
            lifecycle_state,
            escape_graphql_string(failure_reason),
            escape_graphql_string(&accepted.terminalized_at),
        );
        let response = node
            .execute_request_with_retry(
                defra_node::QueryRequest::new(mutation).with_identity(Some(identity.clone())),
                defra_node::ExecuteRetryPolicy::default(),
            )
            .await;
        if response.has_errors()
            || !response
                .data
                .as_ref()
                .and_then(|data| data.get("update_AgentRequest"))
                .is_some_and(response_has_documents)
        {
            tracing::warn!(
                doc_id = %row.doc_id,
                request_id,
                errors = ?response.errors,
                "failed to terminalize missing-response request after durable outcome"
            );
            continue;
        }

        recovered += 1;
        tracing::info!(
            doc_id = %row.doc_id,
            request_id = %request_id,
            session_id = %session_id,
            outcome_kind = %accepted.kind.as_str(),
            outcome_signer_did = %accepted.outcome_signer_did,
            "published missing-response outcome and terminalized request"
        );
    }

    Ok(recovered)
}

#[derive(Debug, Default)]
struct ConversationRecoveryOutcome {
    recovered: usize,
    failed: usize,
    duplicate_sessions: usize,
}

/// Terminalize conversations left mid-flight by a daemon restart.
///
/// Mirrors the Lean sweep `Recovery.conversationRecoverySweep`
/// (proofs/Proofs/Recovery/Sweeps/Conversation.lean), whose row is the
/// *duplicate group* — every doc sharing a `session_id` — not a single doc.
/// Two properties are load-bearing (#693):
///
/// 1. **Duplicate-tolerant.** Stores whose `AgentConversation` collection was
///    created before `session_id` was unique-indexed carry duplicate rows
///    permanently (DefraDB cannot add an index to an existing collection), and
///    replication can mint them. Every doc is therefore written by its own
///    `_docID`: a `session_id`-filtered upsert matches them all and is refused
///    (`cannot upsert multiple matching documents`), which failed the sweep.
///    The canonical doc is picked by an explicit total order — DefraDB returns
///    duplicates in docID order, not recency order — and Lean's
///    `canonical_perm_invariant` proves that pick is independent of scan order.
///    Duplicates are converged to the same terminal status rather than deleted:
///    the collection is replicated, so a delete can be resurrected by a peer or
///    fork the CRDT.
///
/// 2. **Counts successes, never attempts.** `recovered` is the number of
///    sessions whose write actually landed. Counting attempts made a fully
///    failed pass log as healthy; `Recovery.Step.all_failed_reports_zero` pins
///    the honest behavior.
async fn recover_stuck_conversations(
    node: &EmbeddedNode,
    agent_did: &str,
) -> Result<ConversationRecoveryOutcome> {
    let escaped_agent_did = escape_graphql_string(agent_did);
    let query = format!(
        r#"{{
            AgentConversation(
                filter: {{
                    agent_did: {{ _eq: "{escaped_agent_did}" }},
                    status: {{ _in: ["processing", "error"] }}
                }}
            ) {{
                _docID
                agent_name
                behavior_id
                session_id
                latest_request_id
                status
                title
                preview_text
                updated_at
            }}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("querying stuck conversations: {:?}", resp.errors);
    }

    let rows: Vec<serde_json::Value> = resp
        .data
        .as_ref()
        .and_then(|d| d.get("AgentConversation"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let mut sessions: BTreeMap<String, Vec<StuckConversationRow>> = BTreeMap::new();
    for row in &rows {
        let parsed = StuckConversationRow::from_row(row);
        sessions
            .entry(parsed.session_id.clone())
            .or_default()
            .push(parsed);
    }

    let mut outcome = ConversationRecoveryOutcome::default();
    for (session_id, mut docs) in sessions {
        // Canonical first: newest `updated_at`, then richest, then greatest
        // `_docID` (mirrors Lean `docRank`).
        docs.sort_by(|left, right| right.rank().cmp(&left.rank()));
        let Some(canonical) = docs.first().cloned() else {
            continue;
        };

        if docs.len() > 1 {
            outcome.duplicate_sessions += 1;
            let duplicate_doc_ids = docs
                .iter()
                .skip(1)
                .map(|doc| doc.doc_id.as_str())
                .collect::<Vec<_>>();
            tracing::warn!(
                session_id = %session_id,
                doc_count = docs.len(),
                canonical_doc_id = %canonical.doc_id,
                duplicate_doc_ids = ?duplicate_doc_ids,
                "duplicate AgentConversation documents share a session_id; recovering the \
                 canonical document and converging the duplicates onto it"
            );
        }

        let latest_request_status =
            lookup_request_status_by_request_id(node, agent_did, &canonical.latest_request_id)
                .await?;
        let next_status = match latest_request_status.as_deref() {
            Some("completed") => "completed",
            Some("error") => "active",
            _ => "active",
        };

        let mut session_failed = false;
        for doc in &docs {
            if let Err(error) =
                update_conversation_status_by_doc_id(node, &doc.doc_id, &canonical, next_status)
                    .await
            {
                session_failed = true;
                tracing::warn!(
                    doc_id = %doc.doc_id,
                    session_id = %session_id,
                    agent_name = %canonical.agent_name,
                    latest_request_id = %canonical.latest_request_id,
                    latest_request_status = latest_request_status.as_deref().unwrap_or("missing"),
                    error = %error,
                    "failed to recover stuck conversation"
                );
            }
        }

        if session_failed {
            outcome.failed += 1;
            continue;
        }

        outcome.recovered += 1;
        tracing::info!(
            doc_id = %canonical.doc_id,
            session_id = %session_id,
            agent_name = %canonical.agent_name,
            old_status = %canonical.status,
            doc_count = docs.len(),
            latest_request_id = %canonical.latest_request_id,
            latest_request_status = latest_request_status.as_deref().unwrap_or("missing"),
            "recovered stuck conversation: {} → {next_status}",
            canonical.status
        );
    }

    Ok(outcome)
}

#[derive(Debug, Clone, Default)]
struct StuckConversationRow {
    doc_id: String,
    session_id: String,
    agent_name: String,
    behavior_id: String,
    latest_request_id: String,
    status: String,
    title: String,
    preview_text: String,
    updated_at: String,
}

impl StuckConversationRow {
    fn from_row(row: &serde_json::Value) -> Self {
        let field = |key: &str| {
            row.get(key)
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string()
        };
        Self {
            doc_id: field("_docID"),
            session_id: field("session_id"),
            agent_name: field("agent_name"),
            behavior_id: field("behavior_id"),
            latest_request_id: field("latest_request_id"),
            status: field("status"),
            title: field("title"),
            preview_text: field("preview_text"),
            updated_at: field("updated_at"),
        }
    }

    /// Ranking key mirroring Lean `Recovery.docRank`: newest, then richest, then
    /// greatest docID (the primary key, so distinct docs never tie).
    fn rank(&self) -> (String, usize, String) {
        let richness = [
            self.title.trim(),
            self.preview_text.trim(),
            self.latest_request_id.trim(),
        ]
        .iter()
        .filter(|field| !field.is_empty())
        .count();
        (self.updated_at.clone(), richness, self.doc_id.clone())
    }
}

async fn update_conversation_status_by_doc_id(
    node: &EmbeddedNode,
    doc_id: &str,
    canonical: &StuckConversationRow,
    status: &str,
) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    let mutation = format!(
        r#"mutation {{
            update_AgentConversation(
                filter: {{ _docID: {{ _eq: "{doc_id}" }} }},
                input: {{
                    agent_name: "{agent_name}",
                    behavior_id: "{behavior_id}",
                    status: "{status}",
                    updated_at: "{now}",
                    latest_request_id: "{latest_request_id}"
                }}
            ) {{ _docID }}
        }}"#,
        doc_id = escape_graphql_string(doc_id),
        agent_name = escape_graphql_string(&canonical.agent_name),
        behavior_id = escape_graphql_string(&canonical.behavior_id),
        status = escape_graphql_string(status),
        latest_request_id = escape_graphql_string(&canonical.latest_request_id),
    );

    let resp =
        crate::retry::execute_graphql_with_conflict_retry(node, &mutation, "recover_conversation")
            .await;
    if resp.has_errors() {
        anyhow::bail!("recovering conversation doc_id={doc_id}: {:?}", resp.errors);
    }
    Ok(())
}

#[cfg(test)]
mod missing_response_tests {
    use std::sync::Arc;

    use super::*;
    use crate::identity::AgentIdentity as _;

    async fn execute_as(
        node: &EmbeddedNode,
        identity: &identity::Did,
        graphql: String,
    ) -> defra_node::QueryResponse {
        node.execute_request_with_retry(
            defra_node::QueryRequest::new(graphql).with_identity(Some(identity.clone())),
            defra_node::ExecuteRetryPolicy::default(),
        )
        .await
    }

    #[tokio::test]
    async fn missing_live_response_recovers_outcome_first_and_replay_is_idempotent() {
        let key_dir = tempfile::tempdir().unwrap();
        let key_identity =
            crate::identity::KeyIdentity::load_or_create(key_dir.path().join("node.key"), None)
                .unwrap();
        let agent_did = key_identity.did().to_string();
        let identity = identity::Did::new(&agent_did).unwrap();
        let node = Arc::new(
            EmbeddedNode::builder()
                .with_node_identity_did(&agent_did)
                .data_path(key_dir.path().join("data"))
                .build()
                .await
                .unwrap(),
        );
        crate::ensure_runtime_schemas(node.as_ref()).await.unwrap();

        let request_id = format!("missing-response-{}", uuid::Uuid::new_v4());
        let session_id = format!("missing-response-session-{}", uuid::Uuid::new_v4());
        let created_at = chrono::Utc::now().to_rfc3339();
        let mutation = format!(
            r#"mutation {{
                create_AgentRequest(input: {{
                    request_id: "{}"
                    agent_did: "{}"
                    source_author_did: "{}"
                    behavior_id: "general"
                    session_id: "{}"
                    retry_parent_request: ""
                    retry_root_request: "{}"
                    superseded_by_request: ""
                    content: "crash after begin_execution"
                    status: "pending"
                    lifecycle_state: "pending"
                    backend_id: ""
                    execution_origin: "interactive"
                    failure_reason: ""
                    created_at: "{}"
                    retry_count: 0
                    max_retries: 3
                    subagent_depth: 0
                }}) {{ _docID }}
            }}"#,
            escape_graphql_string(&request_id),
            escape_graphql_string(&agent_did),
            escape_graphql_string(&agent_did),
            escape_graphql_string(&session_id),
            escape_graphql_string(&request_id),
            escape_graphql_string(&created_at),
        );
        let response = execute_as(node.as_ref(), &identity, mutation).await;
        assert!(!response.has_errors(), "{:?}", response.errors);
        let inline_request_doc_id = response
            .data
            .as_ref()
            .and_then(|data| data.get("create_AgentRequest"))
            .and_then(|value| {
                value.get("_docID").or_else(|| {
                    value
                        .as_array()
                        .and_then(|rows| rows.first())?
                        .get("_docID")
                })
            })
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        let request_doc_id = match inline_request_doc_id {
            Some(doc_id) => doc_id,
            None => {
                let response = execute_as(
                    node.as_ref(),
                    &identity,
                    format!(
                        r#"query {{
                            AgentRequest(
                                filter: {{ request_id: {{ _eq: "{}" }} }}
                            ) {{ _docID }}
                        }}"#,
                        escape_graphql_string(&request_id)
                    ),
                )
                .await;
                assert!(!response.has_errors(), "{:?}", response.errors);
                let rows = response.data.as_ref().unwrap()["AgentRequest"]
                    .as_array()
                    .unwrap();
                assert_eq!(rows.len(), 1);
                rows[0]["_docID"].as_str().unwrap().to_string()
            }
        };
        let request = crate::watcher::AgentRequest {
            doc_id: request_doc_id.clone(),
            request_id: request_id.clone(),
            agent_did: agent_did.clone(),
            requester_did: None,
            behavior_id: Some("general".to_string()),
            session_id: session_id.clone(),
            content: "crash after begin_execution".to_string(),
            temperature: None,
            top_p: None,
            top_k: None,
            max_tokens: None,
            metadata: None,
            execution_origin: Some("interactive".to_string()),
            created_at,
            deadline: None,
            subagent_depth: 0,
            caused_by_parent_request_id: None,
            caused_by_parent_tool_call_id: None,
        };
        let mut lifecycle = RequestLifecycle::new_with_execution_binding(
            node.clone(),
            "general",
            &agent_did,
            request,
            60,
            ExecutionOrigin::Interactive,
            "test-backend",
        );
        assert_eq!(
            lifecycle.claim_with_identity().await.unwrap(),
            ClaimOutcome::Claimed
        );
        lifecycle.begin_execution().await.unwrap();

        let response = execute_as(
            node.as_ref(),
            &identity,
            format!(
                r#"query {{
                    AgentResponse(
                        filter: {{ request_doc_id: {{ _eq: "{}" }} }}
                    ) {{ _docID }}
                }}"#,
                escape_graphql_string(&request_doc_id)
            ),
        )
        .await;
        assert!(!response.has_errors(), "{:?}", response.errors);
        assert_eq!(
            response.data.as_ref().unwrap()["AgentResponse"]
                .as_array()
                .unwrap()
                .len(),
            0
        );

        let recovery_started = chrono::Utc::now() - chrono::Duration::seconds(1);
        assert_eq!(
            recover_missing_response_documents(node.as_ref(), &agent_did)
                .await
                .unwrap(),
            1
        );

        let outcome = execute_as(
            node.as_ref(),
            &identity,
            format!(
                r#"query {{
                    AgentResponseOutcome(
                        filter: {{ request_doc_id: {{ _eq: "{}" }} }}
                    ) {{
                        _docID
                        request_doc_id
                        request_source_composite_commit_cid
                        request_source_signer_did
                        request_claim_composite_commit_cid
                        request_claim_signer_did
                        outcome_kind
                        reason_code
                        terminalized_at
                    }}
                }}"#,
                escape_graphql_string(&request_doc_id)
            ),
        )
        .await;
        assert!(!outcome.has_errors(), "{:?}", outcome.errors);
        let outcomes = outcome.data.as_ref().unwrap()["AgentResponseOutcome"]
            .as_array()
            .unwrap();
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0]["request_doc_id"], request_doc_id);
        assert_eq!(outcomes[0]["outcome_kind"], "error");
        assert_eq!(
            outcomes[0]["reason_code"],
            "daemon_restart_missing_response"
        );
        assert_eq!(outcomes[0]["request_source_signer_did"], agent_did);
        assert_eq!(outcomes[0]["request_claim_signer_did"], agent_did);
        assert_ne!(
            outcomes[0]["request_source_composite_commit_cid"],
            outcomes[0]["request_claim_composite_commit_cid"]
        );
        let terminalized_at =
            chrono::DateTime::parse_from_rfc3339(outcomes[0]["terminalized_at"].as_str().unwrap())
                .unwrap()
                .with_timezone(&chrono::Utc);
        assert!(terminalized_at >= recovery_started);
        let outcome_doc_id = outcomes[0]["_docID"].as_str().unwrap();
        let signed_outcome =
            crate::document_version::verified_current_signed_document_version_with_identity(
                node.as_ref(),
                "AgentResponseOutcome",
                outcome_doc_id,
                Some(identity.clone()),
            )
            .await
            .unwrap();
        assert_eq!(signed_outcome.signer_did, agent_did);

        let request = execute_as(
            node.as_ref(),
            &identity,
            format!(
                r#"query {{
                    AgentRequest(filter: {{ _docID: {{ _eq: "{}" }} }}) {{
                        status lifecycle_state failure_reason terminalized_at
                    }}
                }}"#,
                escape_graphql_string(&request_doc_id)
            ),
        )
        .await;
        assert!(!request.has_errors(), "{:?}", request.errors);
        let request = &request.data.as_ref().unwrap()["AgentRequest"][0];
        assert_eq!(request["status"], "error");
        assert_eq!(request["lifecycle_state"], "failed");
        assert_eq!(request["failure_reason"], "daemon_restart_missing_response");
        assert_eq!(request["terminalized_at"], outcomes[0]["terminalized_at"]);

        let live = execute_as(
            node.as_ref(),
            &identity,
            format!(
                r#"query {{
                    AgentResponse(
                        filter: {{ request_doc_id: {{ _eq: "{}" }} }}
                    ) {{ _docID }}
                }}"#,
                escape_graphql_string(&request_doc_id)
            ),
        )
        .await;
        assert!(!live.has_errors(), "{:?}", live.errors);
        assert_eq!(
            live.data.as_ref().unwrap()["AgentResponse"]
                .as_array()
                .unwrap()
                .len(),
            0,
            "recovery must not fabricate a mutable live response"
        );

        assert_eq!(
            recover_missing_response_documents(node.as_ref(), &agent_did)
                .await
                .unwrap(),
            0
        );
        let replay = execute_as(
            node.as_ref(),
            &identity,
            format!(
                r#"query {{
                    AgentResponseOutcome(
                        filter: {{ request_doc_id: {{ _eq: "{}" }} }}
                    ) {{ _docID }}
                }}"#,
                escape_graphql_string(&request_doc_id)
            ),
        )
        .await;
        assert!(!replay.has_errors(), "{:?}", replay.errors);
        assert_eq!(
            replay.data.as_ref().unwrap()["AgentResponseOutcome"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }
}
