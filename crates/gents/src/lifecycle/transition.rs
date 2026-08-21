// Soft-cap justified: single impl block on RequestLifecycle; all methods are
// atomic DB mutations that must stay together to preserve the Lean-spec
// transition invariants (S1, S3, S6). Splitting by transition direction
// (complete/fail/supersede) would require re-exporting private helpers across
// submodules with no readability gain.
use anyhow::Context;

use super::rows::{RequestStatusTransition, RequestViewRow};
use super::*;

fn request_status_is_terminal(status: &str) -> bool {
    matches!(
        status,
        "completed" | "error" | "superseded" | "dead" | "interrupted"
    )
}

fn request_view_is_terminal(view: &RequestViewRow) -> bool {
    view.lifecycle_state
        .as_deref()
        .and_then(PersistedLifecycleState::from_persisted)
        .is_some_and(PersistedLifecycleState::is_terminal)
        || request_status_is_terminal(&view.status)
}

pub(super) async fn execute_request_projection_transaction(
    node: &EmbeddedNode,
    request_id: &str,
    session_id: &str,
    request_mutation: &str,
    conversation_mutation: &str,
    operation: &str,
) -> Result<defra_node::QueryResponse> {
    let escaped_session_id = escape_graphql_string(session_id);
    let projection_query = format!(
        r#"{{
            AgentConversation(
                filter: {{ session_id: {{ _eq: "{escaped_session_id}" }} }},
                limit: 2
            ) {{ latest_request_id }}
        }}"#
    );
    crate::retry::retry_terminal_persistence_operation(
        operation,
        crate::retry::TERMINAL_PERSISTENCE_MAX_RETRIES,
        std::time::Duration::from_millis(crate::retry::TERMINAL_PERSISTENCE_INITIAL_BACKOFF_MS),
        || async {
            let txn = crate::config_client::ConfigApplyTxn::begin_local(node, None).await?;
            let attempt = async {
                let request_response = txn.execute_local_response(request_mutation).await?;
                if request_response
                    .data
                    .as_ref()
                    .and_then(|data| data.get("update_AgentRequest"))
                    .is_some_and(response_has_documents)
                {
                    let conversation_response =
                        txn.execute_local_response(conversation_mutation).await?;
                    if !conversation_response
                        .data
                        .as_ref()
                        .and_then(|data| data.get("update_AgentConversation"))
                        .is_some_and(response_has_documents)
                    {
                        let projection = txn.execute_local_response(&projection_query).await?;
                        let rows = projection
                            .data
                            .as_ref()
                            .and_then(|data| data.get("AgentConversation"))
                            .and_then(serde_json::Value::as_array)
                            .map(Vec::as_slice)
                            .unwrap_or_default();
                        match rows {
                            [row]
                                if row
                                    .get("latest_request_id")
                                    .and_then(serde_json::Value::as_str)
                                    .is_some_and(|latest| latest != request_id) => {}
                            [] => anyhow::bail!(
                                "request {request_id} has no AgentConversation projection"
                            ),
                            [_] => anyhow::bail!(
                                "request {request_id} could not update its latest AgentConversation projection"
                            ),
                            _ => anyhow::bail!(
                                "session {session_id} has duplicate AgentConversation projections"
                            ),
                        }
                    }
                }
                Ok::<_, anyhow::Error>(request_response)
            }
            .await;
            match attempt {
                Ok(response) => txn.commit().await.map(|()| response),
                Err(error) => {
                    let _ = txn.discard().await;
                    Err(error)
                }
            }
        },
    )
    .await
}

impl RequestLifecycle {
    pub async fn record_failure_reason(&mut self, reason: &str) -> Result<()> {
        // Latch before I/O so the subsequent atomic terminal mutation still
        // carries the reason if this best-effort standalone write fails.
        self.failure_reason = Some(reason.to_string());
        let doc_id = escape_graphql_string(&self.request.doc_id);
        let agent_did = escape_graphql_string(&self.request.agent_did);
        let escaped_reason = escape_graphql_string(reason);
        let mutation = format!(
            r#"mutation {{
                update_AgentRequest(
                    filter: {{
                        _docID: {{ _eq: "{doc_id}" }},
                        agent_did: {{ _eq: "{agent_did}" }}
                    }},
                    input: {{ failure_reason: "{escaped_reason}" }}
                ) {{ _docID }}
            }}"#
        );

        crate::retry::execute_graphql_with_terminal_persistence_retry(
            &self.node,
            &mutation,
            "record_request_failure_reason",
        )
        .await
        .with_context(|| {
            format!(
                "recording failure reason for request {} doc_id={doc_id}",
                self.request.request_id
            )
        })?;
        Ok(())
    }

    pub async fn advance(&mut self) -> Result<()> {
        self.ensure_state(&[LocalLifecycleState::Streaming], "advance")?;
        let doc_id = self
            .response_doc_id
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("advance() called before response doc created"))?;
        let next_progress_seq = self.progress_seq + 1;
        tracing::debug!(
            request_id = %self.request.request_id,
            doc_id = %doc_id,
            next_progress_seq,
            "advancing response progress"
        );

        let mutation = format!(
            r#"mutation {{
                update_AgentResponse(
                    filter: {{
                        _docID: {{ _eq: "{doc_id}" }},
                        status: {{ _eq: "streaming" }}
                    }},
                    input: {{ progress_seq: {progress_seq} }}
                ) {{ _docID }}
            }}"#,
            progress_seq = next_progress_seq,
        );

        let operation = format!("advance_progress_seq_{next_progress_seq}");
        let resp = session::execute_mutation_with_retry(&self.node, &mutation, &operation)
            .await
            .with_context(|| {
                format!(
                    "failed to advance progress_seq for doc_id={doc_id} progress_seq={next_progress_seq}"
                )
            })?;

        if !resp
            .data
            .as_ref()
            .and_then(|data| data.get("update_AgentResponse"))
            .is_some_and(response_has_documents)
        {
            let status = self.response_status().await?;
            anyhow::bail!(
                "cannot advance progress for request_id={} doc_id={doc_id}: response is {}",
                self.request.request_id,
                status.as_deref().unwrap_or("missing")
            );
        }

        self.progress_seq = next_progress_seq;
        Ok(())
    }

    pub async fn complete(&mut self) -> Result<()> {
        if self.state == LocalLifecycleState::Completed {
            return Ok(());
        }
        if self.state == LocalLifecycleState::Failed {
            tracing::info!(
                request_id = %self.request.request_id,
                "skipping complete() because request lifecycle already failed"
            );
            return Ok(());
        }
        self.ensure_state(
            &[LocalLifecycleState::Claimed, LocalLifecycleState::Streaming],
            "complete",
        )?;

        match self
            .transition_request_status(
                "processing",
                &[
                    PersistedLifecycleState::Claimed,
                    PersistedLifecycleState::Processing,
                ],
                "completed",
                PersistedLifecycleState::Completed,
                "completed",
            )
            .await?
        {
            RequestStatusTransition::Updated | RequestStatusTransition::AlreadyTarget => {}
            RequestStatusTransition::ConflictingTerminal(current) => {
                tracing::info!(
                    request_id = %self.request.request_id,
                    current_status = %current.status,
                    current_lifecycle_state = %current.lifecycle_state.as_deref().unwrap_or("missing"),
                    "skipping completion because request is already terminal"
                );
            }
        }

        self.state = LocalLifecycleState::Completed;
        tracing::info!(
            request_id = %self.request.request_id,
            session_id = %self.request.session_id,
            "request completed"
        );
        Ok(())
    }

    pub async fn transition_to_interrupted(&mut self) -> Result<()> {
        let doc_id = escape_graphql_string(&self.request.doc_id);
        let agent_did = escape_graphql_string(&self.request.agent_did);
        let active_runtime_states = active_runtime_lifecycle_state_graphql_list();
        let terminalized_at_value = chrono::Utc::now().to_rfc3339();
        let terminalized_at = escape_graphql_string(&terminalized_at_value);
        let mutation = format!(
            r#"mutation {{
                update_AgentRequest(
                    filter: {{
                        _docID: {{ _eq: "{doc_id}" }},
                        agent_did: {{ _eq: "{agent_did}" }},
                        lifecycle_state: {{ _in: {active_runtime_states} }},
                        status: {{ _nin: ["completed", "interrupted", "dead", "superseded", "error"] }}
                    }},
                    input: {{
                        status: "interrupted",
                        lifecycle_state: "interrupted",
                        terminalized_at: "{terminalized_at}",
                        terminal_redrive_attempts: 0
                    }}
                ) {{ _docID }}
            }}"#
        );
        let conversation_mutation = session::request_conversation_status_projection_mutation(
            &self.request.session_id,
            &self.request.request_id,
            "active",
            &terminalized_at_value,
        );
        let resp = execute_request_projection_transaction(
            &self.node,
            &self.request.request_id,
            &self.request.session_id,
            &mutation,
            &conversation_mutation,
            "transition_interrupted",
        )
        .await?;
        if resp
            .data
            .as_ref()
            .and_then(|data| data.get("update_AgentRequest"))
            .is_some_and(response_has_documents)
        {
            self.state = LocalLifecycleState::Interrupted;
            return Ok(());
        }

        match self.request_view().await? {
            Some(current)
                if current.status == "interrupted"
                    && current.lifecycle_state.as_deref() == Some("interrupted") =>
            {
                self.state = LocalLifecycleState::Interrupted;
                Ok(())
            }
            Some(current) if request_view_is_terminal(&current) => Ok(()),
            Some(current) if current.lifecycle_state.as_deref() == Some("inputRequired") => {
                anyhow::bail!(
                    "cannot interrupt request_id={} from reserved lifecycle_state=inputRequired",
                    self.request.request_id
                )
            }
            Some(current) => anyhow::bail!(
                "request {} could not transition to interrupted; current status={} lifecycle_state={}",
                self.request.request_id,
                current.status,
                current.lifecycle_state.as_deref().unwrap_or("missing")
            ),
            None => anyhow::bail!(
                "request {} disappeared while transitioning to interrupted",
                self.request.request_id
            ),
        }
    }

    pub async fn fail(&mut self) -> Result<()> {
        if self.state == LocalLifecycleState::Failed {
            return Ok(());
        }
        if self.state == LocalLifecycleState::Completed {
            tracing::info!(
                request_id = %self.request.request_id,
                "skipping fail() because request lifecycle already completed"
            );
            return Ok(());
        }
        if crate::interrupt::fetch_interrupt_requested_at(&self.node, &self.request.request_id)
            .await?
            .is_some()
        {
            tracing::info!(
                request_id = %self.request.request_id,
                "request failure observed after interrupt_requested_at was latched; transitioning to interrupted"
            );
            self.transition_to_interrupted().await?;
            return Ok(());
        }
        self.ensure_state(
            &[LocalLifecycleState::Claimed, LocalLifecycleState::Streaming],
            "fail",
        )?;

        match self
            .transition_request_status(
                "processing",
                &[
                    PersistedLifecycleState::Claimed,
                    PersistedLifecycleState::Processing,
                ],
                "error",
                PersistedLifecycleState::Failed,
                "active",
            )
            .await?
        {
            RequestStatusTransition::Updated | RequestStatusTransition::AlreadyTarget => {}
            RequestStatusTransition::ConflictingTerminal(current) => {
                tracing::info!(
                    request_id = %self.request.request_id,
                    current_status = %current.status,
                    current_lifecycle_state = %current.lifecycle_state.as_deref().unwrap_or("missing"),
                    "skipping failure because request is already terminal"
                );
            }
        }

        self.state = LocalLifecycleState::Failed;
        tracing::info!(
            request_id = %self.request.request_id,
            session_id = %self.request.session_id,
            "request failed"
        );
        Ok(())
    }

    /// Atomically persist the failure reason with the request's terminal edge.
    /// The reason is latched in memory before any storage attempt, so callers do
    /// not depend on a separate `failure_reason` mutation succeeding first.
    pub async fn fail_with_reason(&mut self, reason: &str) -> Result<()> {
        self.failure_reason = Some(reason.to_string());
        self.fail().await
    }

    pub(super) async fn transition_request_status(
        &self,
        from_status: &str,
        from_lifecycle_states: &[PersistedLifecycleState],
        target_status: &str,
        target_lifecycle_state: PersistedLifecycleState,
        conversation_status: &str,
    ) -> Result<RequestStatusTransition> {
        let doc_id = escape_graphql_string(&self.request.doc_id);
        let agent_did = escape_graphql_string(&self.request.agent_did);
        let from_status = escape_graphql_string(from_status);
        let target_status = escape_graphql_string(target_status);
        let from_lifecycle_states = lifecycle_state_graphql_list_for(from_lifecycle_states);
        let terminalized_at_value = chrono::Utc::now().to_rfc3339();
        let terminalized_at = escape_graphql_string(&terminalized_at_value);
        let mutation = format!(
            r#"mutation {{
                update_AgentRequest(
                    filter: {{
                        _docID: {{ _eq: "{doc_id}" }},
                        agent_did: {{ _eq: "{agent_did}" }},
                        status: {{ _eq: "{from_status}" }},
                        lifecycle_state: {{ _in: {from_lifecycle_states} }}
                    }},
                    input: {{
                        status: "{target_status}",
                        lifecycle_state: "{target_lifecycle_state}",
                        behavior_id: "{behavior_id}",
                        backend_id: "{backend_id}",
                        execution_origin: "{execution_origin}",
                        failure_reason: "{failure_reason}",
                        terminalized_at: "{terminalized_at}",
                        terminal_redrive_attempts: 0
                    }}
                ) {{ _docID }}
            }}"#,
            target_lifecycle_state = target_lifecycle_state.as_str(),
            behavior_id = escape_graphql_string(&self.behavior_id),
            backend_id = escape_graphql_string(&self.backend_id),
            execution_origin = self.execution_origin.as_str(),
            failure_reason =
                escape_graphql_string(self.failure_reason.as_deref().unwrap_or_default()),
        );

        let conversation_mutation = session::request_conversation_status_projection_mutation(
            &self.request.session_id,
            &self.request.request_id,
            conversation_status,
            &terminalized_at_value,
        );
        let resp = execute_request_projection_transaction(
            &self.node,
            &self.request.request_id,
            &self.request.session_id,
            &mutation,
            &conversation_mutation,
            "transition_request_terminal_status",
        )
        .await
        .with_context(|| {
            format!(
                "updating request status {} -> {} for doc_id={doc_id}",
                from_status, target_status
            )
        })?;

        if resp
            .data
            .as_ref()
            .and_then(|data| data.get("update_AgentRequest"))
            .is_some_and(response_has_documents)
        {
            tracing::debug!(
                doc_id = %doc_id,
                from_status = %from_status,
                target_status = %target_status,
                "updated request status"
            );
            return Ok(RequestStatusTransition::Updated);
        }

        match self.request_view().await? {
            Some(current)
                if current.status == target_status
                    && current.lifecycle_state.as_deref()
                        == Some(target_lifecycle_state.as_str()) =>
            {
                Ok(RequestStatusTransition::AlreadyTarget)
            }
            Some(current) if request_view_is_terminal(&current) => {
                Ok(RequestStatusTransition::ConflictingTerminal(current))
            }
            Some(current) => anyhow::bail!(
                "request {} could not transition {} -> {}; current status={} lifecycle_state={}",
                self.request.request_id,
                from_status,
                target_status,
                current.status,
                current.lifecycle_state.as_deref().unwrap_or("missing")
            ),
            None => anyhow::bail!(
                "request {} disappeared while transitioning {} -> {}",
                self.request.request_id,
                from_status,
                target_status
            ),
        }
    }

    pub(super) async fn transition_execution_view(
        &self,
        from_status: &str,
        from_lifecycle_state: PersistedLifecycleState,
        target_status: &str,
        target_lifecycle_state: PersistedLifecycleState,
    ) -> Result<()> {
        let doc_id = self.request.doc_id.clone();
        let escaped_doc_id = escape_graphql_string(&doc_id);
        let mutation = format!(
            r#"mutation {{
                update_AgentRequest(
                    filter: {{
                        _docID: {{ _eq: "{escaped_doc_id}" }},
                        status: {{ _eq: "{from_status}" }},
                        lifecycle_state: {{ _eq: "{from_lifecycle_state}" }}
                    }},
                    input: {{
                        status: "{target_status}",
                        lifecycle_state: "{target_lifecycle_state}",
                        behavior_id: "{behavior_id}",
                        backend_id: "{backend_id}",
                        execution_origin: "{execution_origin}",
                        failure_reason: "{failure_reason}"
                    }}
                ) {{ _docID }}
            }}"#,
            from_lifecycle_state = from_lifecycle_state.as_str(),
            target_lifecycle_state = target_lifecycle_state.as_str(),
            behavior_id = escape_graphql_string(&self.behavior_id),
            backend_id = escape_graphql_string(&self.backend_id),
            execution_origin = self.execution_origin.as_str(),
            failure_reason =
                escape_graphql_string(self.failure_reason.as_deref().unwrap_or_default()),
        );

        let operation = format!(
            "transition_execution_view_{}_to_{}",
            from_lifecycle_state.as_str(),
            target_lifecycle_state.as_str()
        );
        let resp = session::execute_mutation_with_retry(&self.node, &mutation, &operation)
            .await
            .with_context(|| {
                format!(
                    "updating execution view {} -> {} for doc_id={doc_id}",
                    from_lifecycle_state.as_str(),
                    target_lifecycle_state.as_str()
                )
            })?;

        if resp
            .data
            .as_ref()
            .and_then(|data| data.get("update_AgentRequest"))
            .is_some_and(response_has_documents)
        {
            return Ok(());
        }

        let request_view = self.request_view().await?;
        match request_view {
            Some(current)
                if current.status == target_status
                    && current.lifecycle_state.as_deref() == Some(target_lifecycle_state.as_str()) =>
            {
                Ok(())
            }
            Some(current) => anyhow::bail!(
                "request {} could not transition execution view {} -> {}; current status={} lifecycle_state={}",
                self.request.request_id,
                from_lifecycle_state.as_str(),
                target_lifecycle_state.as_str(),
                current.status,
                current.lifecycle_state.as_deref().unwrap_or("missing")
            ),
            None => anyhow::bail!(
                "request {} disappeared while transitioning execution view {} -> {}",
                self.request.request_id,
                from_lifecycle_state.as_str(),
                target_lifecycle_state.as_str()
            ),
        }
    }

    pub(super) fn ensure_state(
        &self,
        expected: &[LocalLifecycleState],
        action: &str,
    ) -> Result<()> {
        if expected.contains(&self.state) {
            return Ok(());
        }

        anyhow::bail!(
            "cannot {} request_id={} while lifecycle is in {:?}",
            action,
            self.request.request_id,
            self.state
        )
    }
}
