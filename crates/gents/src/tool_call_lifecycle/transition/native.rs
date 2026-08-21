use super::*;

impl ToolCallLifecycle {
    fn requester_did_fragment(&self) -> String {
        crate::session::requester_did_create_field(self.requester_did.as_deref())
    }

    fn request_doc_id_fragment(&self) -> String {
        crate::session::request_doc_id_create_field(self.request_doc_id.as_deref())
    }

    fn selected_tool_fields_fragment(&self) -> String {
        match self.selected_tool_identity.as_ref() {
            Some(selected) => format!(
                "selected_service_id: \"{}\",\n                    selected_tool_name: \"{}\",",
                escape_graphql_string(&selected.service_id),
                escape_graphql_string(&selected.tool_name),
            ),
            _ => "selected_service_id: null,\n                    selected_tool_name: null,"
                .to_string(),
        }
    }

    /// Pending → Running. Creates the DefraDB row if missing; idempotent if
    /// already in Running. Sets `started_at` to `now`.
    pub async fn start_running(&mut self) -> Result<()> {
        if self.state == ToolCallState::Running {
            // Idempotent re-entry (retry path).
            return Ok(());
        }
        self.ensure_state(&[ToolCallState::Pending], "start_running")?;

        let now = Utc::now();
        let started_at_str = now.to_rfc3339();
        let deadline_at_str = self.deadline_at.to_rfc3339();
        let escaped_request_id = escape_graphql_string(&self.request_id);
        let escaped_session_id = escape_graphql_string(&self.session_id);
        let escaped_agent_did = escape_graphql_string(&self.agent_did);
        let escaped_tool_call_id = escape_graphql_string(&self.tool_call_id);
        let escaped_tool_name = escape_graphql_string(&self.tool_name);
        let escaped_args = escape_graphql_string(&self.args);
        let tool_call_key = format!("{escaped_session_id}:{escaped_tool_call_id}");
        let message_sequence = self.message_sequence;

        // Persist await_mode / cancel_policy for every tool call so composites
        // and native calls are not projected as `unknown` (#837). Child link,
        // spawn target, and unclaimed deadline remain bridge-only fields.
        let await_mode_str = self.await_mode.as_str();
        let cancel_policy_str = self.cancel_policy.as_str();
        let child_field = self
            .child_request_id
            .as_ref()
            .map(|crid| {
                let escaped_crid = escape_graphql_string(crid);
                format!(r#"child_request_id: "{escaped_crid}","#)
            })
            .unwrap_or_default();
        let spawn_target_field = self
            .spawn_target_did
            .as_ref()
            .map(|did| {
                let escaped_did = escape_graphql_string(did);
                format!(r#"spawn_target_did: "{escaped_did}","#)
            })
            .unwrap_or_default();
        let unclaimed_deadline_field = self
            .unclaimed_deadline_at
            .map(|deadline| {
                let escaped_deadline = escape_graphql_string(
                    &deadline.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                );
                format!(r#"unclaimed_deadline_at: "{escaped_deadline}","#)
            })
            .unwrap_or_default();
        let bridge_fields = format!(
            r#"{child_field}
                    {spawn_target_field}
                    {unclaimed_deadline_field}
                    await_mode: "{await_mode_str}",
                    cancel_policy: "{cancel_policy_str}","#
        );
        let requester_did_field = self.requester_did_fragment();
        let request_doc_id_field = self.request_doc_id_fragment();
        let selected_tool_fields = self.selected_tool_fields_fragment();

        let mutation = format!(
            r#"mutation {{
                create_AgentToolCall(input: {{
                    tool_call_key: "{tool_call_key}",
                    request_id: "{escaped_request_id}",
                    {request_doc_id_field}
                    session_id: "{escaped_session_id}",
                    agent_did: "{escaped_agent_did}",
                    {requester_did_field}
                    message_sequence: {message_sequence},
                    tool_name: "{escaped_tool_name}",
                    tool_call_id: "{escaped_tool_call_id}",
                    args: "{escaped_args}",
                    result: "",
                    status: "called",
                    lifecycle_state: "running",
                    started_at: "{started_at_str}",
                    deadline_at: "{deadline_at_str}",
                    {bridge_fields}
                    {selected_tool_fields}
                    tool_failure_class: null,
                    latency_ms: null
                }}) {{ _docID }}
            }}"#
        );

        let resp = execute_mutation_with_retry(&self.node, &mutation, "start_running")
            .await
            .context("start_running mutation")?;

        let doc_id = extract_doc_id_from_create_response(&resp)
            .ok_or_else(|| anyhow!("create_AgentToolCall returned no _docID"))?;

        self.doc_id = Some(doc_id);
        self.state = ToolCallState::Running;
        self.started_at = Some(now);
        Ok(())
    }

    /// Running → Completed. Writes the tool result; sets completed_at,
    /// latency_ms.
    pub async fn complete(&mut self, result: &str) -> Result<()> {
        self.ensure_state(&[ToolCallState::Running], "complete")?;
        if self.is_bridge() {
            return Err(IllegalToolCallTransition::NativeCompleteOnSubagentTool.into());
        }

        let doc_id = self
            .doc_id
            .as_ref()
            .ok_or_else(|| anyhow!("complete called before start_running persisted a row"))?;
        let now = Utc::now();
        let started_at = self
            .started_at
            .ok_or_else(|| anyhow!("complete called without started_at set"))?;
        let latency_ms = (now - started_at).num_milliseconds();

        let escaped_result = escape_graphql_string(result);
        let escaped_doc_id = escape_graphql_string(doc_id);
        let now_str = now.to_rfc3339();
        // DefraDB requires DateTime fields to be re-supplied on update to
        // avoid a type-mismatch error when re-validating the document.
        let started_at_str = started_at.to_rfc3339();
        let deadline_at_str = self.deadline_at.to_rfc3339();
        let unclaimed_deadline_clear = self.clear_unclaimed_deadline_fragment();

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
                        lifecycle_state: "completed",
                        started_at: "{started_at_str}",
                        deadline_at: "{deadline_at_str}",
                        completed_at: "{now_str}",
                        latency_ms: {latency_ms}
                        {unclaimed_deadline_clear}
                    }}
                ) {{ _docID }}
            }}"#
        );

        let response = execute_mutation_with_retry(&self.node, &mutation, "complete")
            .await
            .context("complete mutation")?;
        if !response
            .data
            .as_ref()
            .and_then(|data| data.get("update_AgentToolCall"))
            .is_some_and(response_has_documents)
        {
            // Interrupt/timeout won the race — adopt the durable terminal.
            self.sync_after_lost_running_compare("complete").await?;
            return Ok(());
        }

        self.state = ToolCallState::Completed;
        Ok(())
    }

    /// Running → Failed. For tool errors during execution. Sets failure_class.
    pub async fn fail(&mut self, result: &str, failure: super::FailureClass) -> Result<()> {
        self.fail_with_details(result, failure, None).await
    }

    pub(crate) async fn fail_with_command_denial(
        &mut self,
        result: &str,
        denial: &CommandPolicyDenial,
    ) -> Result<()> {
        self.fail_with_details(result, FailureClass::PolicyDenied, Some(denial))
            .await
    }

    async fn fail_with_details(
        &mut self,
        result: &str,
        failure: super::FailureClass,
        command_denial: Option<&CommandPolicyDenial>,
    ) -> Result<()> {
        self.ensure_state(&[ToolCallState::Running], "fail")?;
        if self.is_bridge() {
            return Err(IllegalToolCallTransition::NativeFailOnSubagentTool.into());
        }

        let doc_id = self
            .doc_id
            .as_ref()
            .ok_or_else(|| anyhow!("fail called before start_running persisted a row"))?;
        let now = Utc::now();
        let started_at = self
            .started_at
            .ok_or_else(|| anyhow!("fail called without started_at set"))?;
        let latency_ms = (now - started_at).num_milliseconds();

        let escaped_result = escape_graphql_string(result);
        let escaped_doc_id = escape_graphql_string(doc_id);
        let now_str = now.to_rfc3339();
        let failure_class_str = failure.as_str();
        // DefraDB requires DateTime fields to be re-supplied on update.
        let started_at_str = started_at.to_rfc3339();
        let deadline_at_str = self.deadline_at.to_rfc3339();
        let unclaimed_deadline_clear = self.clear_unclaimed_deadline_fragment();
        let command_denial_fields = command_denial_fields_fragment(command_denial);

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
                        started_at: "{started_at_str}",
                        deadline_at: "{deadline_at_str}",
                        completed_at: "{now_str}",
                        tool_failure_class: "{failure_class_str}",
                        {command_denial_fields}
                        latency_ms: {latency_ms}
                        {unclaimed_deadline_clear}
                    }}
                ) {{ _docID }}
            }}"#
        );

        let response = execute_mutation_with_retry(&self.node, &mutation, "fail")
            .await
            .context("fail mutation")?;
        if !response
            .data
            .as_ref()
            .and_then(|data| data.get("update_AgentToolCall"))
            .is_some_and(response_has_documents)
        {
            // Interrupt/timeout won the race — adopt the durable terminal.
            self.sync_after_lost_running_compare("fail").await?;
            return Ok(());
        }

        self.state = ToolCallState::Failed;
        self.failure_class = Some(failure);
        Ok(())
    }

    /// Pending → Failed. Used when the dispatcher cannot start the call
    /// (MCP service unreachable, argument parse failure pre-spawn).
    pub async fn spawn_failed(&mut self, failure: super::FailureClass, reason: &str) -> Result<()> {
        self.spawn_failed_with_details(failure, reason, None).await
    }

    #[allow(dead_code)]
    pub(crate) async fn spawn_failed_with_command_denial(
        &mut self,
        reason: &str,
        denial: &CommandPolicyDenial,
    ) -> Result<()> {
        self.spawn_failed_with_details(FailureClass::PolicyDenied, reason, Some(denial))
            .await
    }

    async fn spawn_failed_with_details(
        &mut self,
        failure: super::FailureClass,
        reason: &str,
        command_denial: Option<&CommandPolicyDenial>,
    ) -> Result<()> {
        self.ensure_state(&[ToolCallState::Pending], "spawn_failed")?;

        // Pending means the row hasn't been created yet. We create it
        // directly in Failed state.
        let now = Utc::now();
        let started_at_str = now.to_rfc3339();
        let deadline_at_str = self.deadline_at.to_rfc3339();
        let escaped_request_id = escape_graphql_string(&self.request_id);
        let escaped_session_id = escape_graphql_string(&self.session_id);
        let escaped_agent_did = escape_graphql_string(&self.agent_did);
        let escaped_tool_call_id = escape_graphql_string(&self.tool_call_id);
        let escaped_tool_name = escape_graphql_string(&self.tool_name);
        let escaped_args = escape_graphql_string(&self.args);
        let escaped_result = escape_graphql_string(reason);
        let tool_call_key = format!("{escaped_session_id}:{escaped_tool_call_id}");
        let message_sequence = self.message_sequence;
        let failure_class_str = failure.as_str();
        let command_denial_fields = command_denial_fields_fragment(command_denial);
        let requester_did_field = self.requester_did_fragment();
        let request_doc_id_field = self.request_doc_id_fragment();
        let selected_tool_fields = self.selected_tool_fields_fragment();

        let mutation = format!(
            r#"mutation {{
                create_AgentToolCall(input: {{
                    tool_call_key: "{tool_call_key}",
                    request_id: "{escaped_request_id}",
                    {request_doc_id_field}
                    session_id: "{escaped_session_id}",
                    agent_did: "{escaped_agent_did}",
                    {requester_did_field}
                    message_sequence: {message_sequence},
                    tool_name: "{escaped_tool_name}",
                    tool_call_id: "{escaped_tool_call_id}",
                    args: "{escaped_args}",
                    result: "{escaped_result}",
                    status: "completed",
                    lifecycle_state: "failed",
                    started_at: null,
                    deadline_at: "{deadline_at_str}",
                    completed_at: "{started_at_str}",
                    tool_failure_class: "{failure_class_str}",
                    {command_denial_fields}
                    {selected_tool_fields}
                    latency_ms: 0
                }}) {{ _docID }}
            }}"#
        );

        let resp = execute_mutation_with_retry(&self.node, &mutation, "spawn_failed")
            .await
            .context("spawn_failed mutation")?;

        let doc_id = extract_doc_id_from_create_response(&resp)
            .ok_or_else(|| anyhow!("create_AgentToolCall returned no _docID"))?;

        self.doc_id = Some(doc_id);
        self.state = ToolCallState::Failed;
        self.failure_class = Some(failure);
        Ok(())
    }

    /// Running → TimedOut. Called by the runtime deadline wrapper and startup
    /// recovery when a running tool call exceeds its effective deadline.
    ///
    /// Returns whether this caller won the durable running-state compare.
    /// A loser adopts the already-terminal durable row (another actor —
    /// interrupt, recovery sweep, or the tool itself — terminalized first),
    /// preserving that terminal's state and recorded cause.
    pub async fn timeout(&mut self) -> Result<bool> {
        self.ensure_state(&[ToolCallState::Running], "timeout")?;

        let doc_id = self
            .doc_id
            .as_ref()
            .ok_or_else(|| anyhow!("timeout called before start_running persisted a row"))?;
        let now = Utc::now();
        let started_at = self
            .started_at
            .ok_or_else(|| anyhow!("timeout called without started_at set"))?;
        let latency_ms = (now - started_at).num_milliseconds();

        let escaped_doc_id = escape_graphql_string(doc_id);
        let escaped_result = escape_graphql_string(&format!(
            "tool call deadline exceeded at {}",
            self.deadline_at.to_rfc3339()
        ));
        let now_str = now.to_rfc3339();
        // DefraDB requires DateTime fields to be re-supplied on update.
        let started_at_str = started_at.to_rfc3339();
        let deadline_at_str = self.deadline_at.to_rfc3339();
        let failure_class = FailureClass::External.as_str();
        let cancel_cause = CancelCause::Deadline.as_str();
        let unclaimed_deadline_clear = self.clear_unclaimed_deadline_fragment();

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
                        lifecycle_state: "timedOut",
                        tool_failure_class: "{failure_class}",
                        cancel_cause: "{cancel_cause}",
                        started_at: "{started_at_str}",
                        deadline_at: "{deadline_at_str}",
                        completed_at: "{now_str}",
                        latency_ms: {latency_ms}
                        {unclaimed_deadline_clear}
                    }}
                ) {{ _docID }}
            }}"#
        );

        let response = execute_mutation_with_retry(&self.node, &mutation, "timeout")
            .await
            .context("timeout mutation")?;
        if !response
            .data
            .as_ref()
            .and_then(|data| data.get("update_AgentToolCall"))
            .is_some_and(response_has_documents)
        {
            // Another actor terminalized first — adopt the durable terminal.
            self.sync_after_lost_running_compare("timeout").await?;
            return Ok(false);
        }

        self.state = ToolCallState::TimedOut;
        self.failure_class = Some(FailureClass::External);
        self.cancel_cause = Some(CancelCause::Deadline);
        Ok(true)
    }

    /// Pending → Cancelled. Used when a tool call is cancelled before
    /// dispatch creates a running row.
    ///
    pub async fn cancel_before_dispatch(&mut self, cause: CancelCause) -> Result<()> {
        self.ensure_state(&[ToolCallState::Pending], "cancel_before_dispatch")?;

        // Pending: row may not exist yet. Create directly in Cancelled.
        let now = Utc::now();
        let started_at_str = now.to_rfc3339();
        let deadline_at_str = self.deadline_at.to_rfc3339();
        let escaped_request_id = escape_graphql_string(&self.request_id);
        let escaped_session_id = escape_graphql_string(&self.session_id);
        let escaped_agent_did = escape_graphql_string(&self.agent_did);
        let escaped_tool_call_id = escape_graphql_string(&self.tool_call_id);
        let escaped_tool_name = escape_graphql_string(&self.tool_name);
        let escaped_args = escape_graphql_string(&self.args);
        let tool_call_key = format!("{escaped_session_id}:{escaped_tool_call_id}");
        let message_sequence = self.message_sequence;
        let cancel_cause = cause.as_str();
        let requester_did_field = self.requester_did_fragment();
        let request_doc_id_field = self.request_doc_id_fragment();
        let selected_tool_fields = self.selected_tool_fields_fragment();

        let escaped_result = escape_graphql_string("tool call cancelled before dispatch");

        let mutation = format!(
            r#"mutation {{
                create_AgentToolCall(input: {{
                    tool_call_key: "{tool_call_key}",
                    request_id: "{escaped_request_id}",
                    {request_doc_id_field}
                    session_id: "{escaped_session_id}",
                    agent_did: "{escaped_agent_did}",
                    {requester_did_field}
                    message_sequence: {message_sequence},
                    tool_name: "{escaped_tool_name}",
                    tool_call_id: "{escaped_tool_call_id}",
                    args: "{escaped_args}",
                    result: "{escaped_result}",
                    status: "completed",
                    lifecycle_state: "cancelled",
                    cancel_cause: "{cancel_cause}",
                    started_at: null,
                    deadline_at: "{deadline_at_str}",
                    completed_at: "{started_at_str}",
                    {selected_tool_fields}
                    latency_ms: 0
                }}) {{ _docID }}
            }}"#
        );

        let resp = execute_mutation_with_retry(&self.node, &mutation, "cancel_before_dispatch")
            .await
            .context("cancel_before_dispatch mutation")?;
        let doc_id = extract_doc_id_from_create_response(&resp)
            .ok_or_else(|| anyhow!("create_AgentToolCall returned no _docID"))?;

        self.doc_id = Some(doc_id);
        self.state = ToolCallState::Cancelled;
        self.cancel_cause = Some(cause);
        Ok(())
    }

    /// Pending → AwaitingApproval. Persists the row held for an operator
    /// verdict; the tool is NOT dispatched and `started_at` stays null until
    /// `approve_and_start`. Mirrors the Lean `holdForApproval` transition.
    pub async fn hold_for_approval(&mut self) -> Result<()> {
        if self.state == ToolCallState::AwaitingApproval {
            // Idempotent re-entry (retry path).
            return Ok(());
        }
        self.ensure_state(&[ToolCallState::Pending], "hold_for_approval")?;

        let deadline_at_str = self.deadline_at.to_rfc3339();
        let escaped_request_id = escape_graphql_string(&self.request_id);
        let escaped_session_id = escape_graphql_string(&self.session_id);
        let escaped_agent_did = escape_graphql_string(&self.agent_did);
        let escaped_tool_call_id = escape_graphql_string(&self.tool_call_id);
        let escaped_tool_name = escape_graphql_string(&self.tool_name);
        let escaped_args = escape_graphql_string(&self.args);
        let tool_call_key = format!("{escaped_session_id}:{escaped_tool_call_id}");
        let message_sequence = self.message_sequence;
        let requester_did_field = self.requester_did_fragment();
        let request_doc_id_field = self.request_doc_id_fragment();
        let selected_tool_fields = self.selected_tool_fields_fragment();

        let mutation = format!(
            r#"mutation {{
                create_AgentToolCall(input: {{
                    tool_call_key: "{tool_call_key}",
                    request_id: "{escaped_request_id}",
                    {request_doc_id_field}
                    session_id: "{escaped_session_id}",
                    agent_did: "{escaped_agent_did}",
                    {requester_did_field}
                    message_sequence: {message_sequence},
                    tool_name: "{escaped_tool_name}",
                    tool_call_id: "{escaped_tool_call_id}",
                    args: "{escaped_args}",
                    result: "",
                    status: "called",
                    lifecycle_state: "awaitingApproval",
                    started_at: null,
                    deadline_at: "{deadline_at_str}",
                    {selected_tool_fields}
                    tool_failure_class: null,
                    latency_ms: null
                }}) {{ _docID }}
            }}"#
        );

        let resp = execute_mutation_with_retry(&self.node, &mutation, "hold_for_approval")
            .await
            .context("hold_for_approval mutation")?;

        let doc_id = extract_doc_id_from_create_response(&resp)
            .ok_or_else(|| anyhow!("create_AgentToolCall returned no _docID"))?;

        self.doc_id = Some(doc_id);
        self.state = ToolCallState::AwaitingApproval;
        Ok(())
    }

    /// Reload after a lost held-row compare (the row left `awaitingApproval`
    /// under us — cancelled or timed out by another actor). Adopts current
    /// row state so the caller can observe the terminal.
    async fn sync_after_lost_held_compare(&mut self, method: &'static str) -> Result<()> {
        let current =
            ToolCallLifecycle::load(self.node.clone(), &self.session_id, &self.tool_call_id)
                .await?
                .ok_or_else(|| {
                    anyhow!(
                        "{method} compare failed and AgentToolCall row disappeared for session_id={} tool_call_id={}",
                        self.session_id,
                        self.tool_call_id
                    )
                })?;
        if current.state == ToolCallState::AwaitingApproval {
            anyhow::bail!(
                "{method} compare failed but AgentToolCall row is still awaitingApproval for session_id={} tool_call_id={}",
                self.session_id,
                self.tool_call_id
            );
        }
        self.doc_id = current.doc_id;
        self.deadline_at = current.deadline_at;
        self.state = current.state;
        self.started_at = current.started_at;
        self.failure_class = current.failure_class;
        self.cancel_cause = current.cancel_cause;
        Ok(())
    }

    /// AwaitingApproval → Running on approved evidence. Sets `started_at`
    /// (the Lean `approve` transition's startedAt discipline). Returns false
    /// when the compare-and-set loses (row already left awaitingApproval).
    pub async fn approve_and_start(&mut self) -> Result<bool> {
        self.ensure_state(&[ToolCallState::AwaitingApproval], "approve_and_start")?;

        let doc_id = self.doc_id.as_ref().ok_or_else(|| {
            anyhow!("approve_and_start called before hold_for_approval persisted a row")
        })?;
        let now = Utc::now();
        let escaped_doc_id = escape_graphql_string(doc_id);
        let started_at_str = now.to_rfc3339();
        // DefraDB requires DateTime fields to be re-supplied on update.
        let deadline_at_str = self.deadline_at.to_rfc3339();

        let mutation = format!(
            r#"mutation {{
                update_AgentToolCall(
                    filter: {{
                        _docID: {{ _eq: "{escaped_doc_id}" }},
                        lifecycle_state: {{ _eq: "awaitingApproval" }}
                    }},
                    input: {{
                        lifecycle_state: "running",
                        started_at: "{started_at_str}",
                        deadline_at: "{deadline_at_str}"
                    }}
                ) {{ _docID }}
            }}"#
        );

        let response = execute_mutation_with_retry(&self.node, &mutation, "approve_and_start")
            .await
            .context("approve_and_start mutation")?;
        if !response
            .data
            .as_ref()
            .and_then(|data| data.get("update_AgentToolCall"))
            .is_some_and(response_has_documents)
        {
            self.sync_after_lost_held_compare("approve_and_start")
                .await?;
            return Ok(false);
        }

        self.state = ToolCallState::Running;
        self.started_at = Some(now);
        Ok(true)
    }

    /// AwaitingApproval → Failed on denied evidence. Sets
    /// `failure_class = approvalDenied` (the Lean `deny` transition). Returns
    /// false when the compare-and-set loses.
    pub async fn deny_approval(&mut self, reason: &str) -> Result<bool> {
        self.ensure_state(&[ToolCallState::AwaitingApproval], "deny_approval")?;

        let doc_id = self.doc_id.as_ref().ok_or_else(|| {
            anyhow!("deny_approval called before hold_for_approval persisted a row")
        })?;
        let now = Utc::now();
        let escaped_doc_id = escape_graphql_string(doc_id);
        let escaped_result = escape_graphql_string(reason);
        let now_str = now.to_rfc3339();
        // DefraDB requires DateTime fields to be re-supplied on update.
        let deadline_at_str = self.deadline_at.to_rfc3339();
        let failure_class_str = FailureClass::ApprovalDenied.as_str();

        let mutation = format!(
            r#"mutation {{
                update_AgentToolCall(
                    filter: {{
                        _docID: {{ _eq: "{escaped_doc_id}" }},
                        lifecycle_state: {{ _eq: "awaitingApproval" }}
                    }},
                    input: {{
                        result: "{escaped_result}",
                        status: "completed",
                        lifecycle_state: "failed",
                        tool_failure_class: "{failure_class_str}",
                        deadline_at: "{deadline_at_str}",
                        completed_at: "{now_str}",
                        latency_ms: 0
                    }}
                ) {{ _docID }}
            }}"#
        );

        let response = execute_mutation_with_retry(&self.node, &mutation, "deny_approval")
            .await
            .context("deny_approval mutation")?;
        if !response
            .data
            .as_ref()
            .and_then(|data| data.get("update_AgentToolCall"))
            .is_some_and(response_has_documents)
        {
            self.sync_after_lost_held_compare("deny_approval").await?;
            return Ok(false);
        }

        self.state = ToolCallState::Failed;
        self.failure_class = Some(FailureClass::ApprovalDenied);
        Ok(true)
    }

    /// AwaitingApproval → Cancelled (the Lean `cancelWhileHeld` transition).
    /// Returns false when the compare-and-set loses.
    pub async fn cancel_while_held(&mut self, cause: CancelCause) -> Result<bool> {
        self.ensure_state(&[ToolCallState::AwaitingApproval], "cancel_while_held")?;

        let doc_id = self.doc_id.as_ref().ok_or_else(|| {
            anyhow!("cancel_while_held called before hold_for_approval persisted a row")
        })?;
        let now = Utc::now();
        let escaped_doc_id = escape_graphql_string(doc_id);
        let escaped_result = escape_graphql_string("tool call cancelled while awaiting approval");
        let now_str = now.to_rfc3339();
        // DefraDB requires DateTime fields to be re-supplied on update.
        let deadline_at_str = self.deadline_at.to_rfc3339();
        let cancel_cause = cause.as_str();

        let mutation = format!(
            r#"mutation {{
                update_AgentToolCall(
                    filter: {{
                        _docID: {{ _eq: "{escaped_doc_id}" }},
                        lifecycle_state: {{ _eq: "awaitingApproval" }}
                    }},
                    input: {{
                        result: "{escaped_result}",
                        status: "completed",
                        lifecycle_state: "cancelled",
                        cancel_cause: "{cancel_cause}",
                        deadline_at: "{deadline_at_str}",
                        completed_at: "{now_str}",
                        latency_ms: 0
                    }}
                ) {{ _docID }}
            }}"#
        );

        let response = execute_mutation_with_retry(&self.node, &mutation, "cancel_while_held")
            .await
            .context("cancel_while_held mutation")?;
        if !response
            .data
            .as_ref()
            .and_then(|data| data.get("update_AgentToolCall"))
            .is_some_and(response_has_documents)
        {
            self.sync_after_lost_held_compare("cancel_while_held")
                .await?;
            return Ok(false);
        }

        self.state = ToolCallState::Cancelled;
        self.cancel_cause = Some(cause);
        Ok(true)
    }

    /// AwaitingApproval → TimedOut when the deadline expires unanswered (the
    /// Lean `timeoutWhileHeld` transition). Returns false when the
    /// compare-and-set loses.
    pub async fn timeout_while_held(&mut self) -> Result<bool> {
        self.ensure_state(&[ToolCallState::AwaitingApproval], "timeout_while_held")?;

        let doc_id = self.doc_id.as_ref().ok_or_else(|| {
            anyhow!("timeout_while_held called before hold_for_approval persisted a row")
        })?;
        let now = Utc::now();
        let escaped_doc_id = escape_graphql_string(doc_id);
        let escaped_result = escape_graphql_string(&format!(
            "tool call approval deadline exceeded at {}",
            self.deadline_at.to_rfc3339()
        ));
        let now_str = now.to_rfc3339();
        // DefraDB requires DateTime fields to be re-supplied on update.
        let deadline_at_str = self.deadline_at.to_rfc3339();
        let failure_class = FailureClass::External.as_str();
        let cancel_cause = CancelCause::Deadline.as_str();

        let mutation = format!(
            r#"mutation {{
                update_AgentToolCall(
                    filter: {{
                        _docID: {{ _eq: "{escaped_doc_id}" }},
                        lifecycle_state: {{ _eq: "awaitingApproval" }}
                    }},
                    input: {{
                        result: "{escaped_result}",
                        status: "completed",
                        lifecycle_state: "timedOut",
                        tool_failure_class: "{failure_class}",
                        cancel_cause: "{cancel_cause}",
                        deadline_at: "{deadline_at_str}",
                        completed_at: "{now_str}",
                        latency_ms: 0
                    }}
                ) {{ _docID }}
            }}"#
        );

        let response = execute_mutation_with_retry(&self.node, &mutation, "timeout_while_held")
            .await
            .context("timeout_while_held mutation")?;
        if !response
            .data
            .as_ref()
            .and_then(|data| data.get("update_AgentToolCall"))
            .is_some_and(response_has_documents)
        {
            self.sync_after_lost_held_compare("timeout_while_held")
                .await?;
            return Ok(false);
        }

        self.state = ToolCallState::TimedOut;
        self.failure_class = Some(FailureClass::External);
        self.cancel_cause = Some(CancelCause::Deadline);
        Ok(true)
    }
}
