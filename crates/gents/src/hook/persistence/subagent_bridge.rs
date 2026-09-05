use super::*;

impl DefraSessionHook {
    pub(super) async fn cancel_live_subagent_descendants(
        &self,
        child_session_id: &str,
        cause: CancelCause,
    ) -> anyhow::Result<usize> {
        crate::background_tools::subagent_control::cancel_live_subagent_descendants(
            self.node.clone(),
            child_session_id,
            &self.agent_did,
            cause,
        )
        .await
    }

    pub(super) async fn await_foreground_subagent(
        &self,
        internal_call_id: &str,
        parent_context: &ParentSubagentContext,
        child_request_id: &str,
        child_session_id: &str,
        behavior_id: &str,
        parent_deadline_at: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<String> {
        let mut missing_owner_since = None;
        let mut child_session_id = child_session_id.to_string();

        loop {
            let now = chrono::Utc::now();
            let Some(edge) =
                try_load_authorized_child_edge(&self.node, parent_context, child_request_id)
                    .await?
            else {
                if now >= parent_deadline_at {
                    // Terminalize the bridge as timedOut (parent deadline
                    // exceeded before the child was ever materialized),
                    // mirroring the running-edge deadline path so the bridge
                    // does not leak in a `running` state. No child terminal
                    // evidence exists, so `bridge_failure` is not licensed
                    // here (#1002) — the deadline transition is.
                    if let Some(mut lifecycle) =
                        self.take_owned_in_flight_lifecycle(internal_call_id).await
                    {
                        let _ = lifecycle.timeout().await;
                    }
                    return Ok(foreground_terminal_failure_payload(
                        child_request_id,
                        &child_session_id,
                        "dead",
                        "parent request deadline exceeded while waiting for child subagent",
                        FailureClass::External,
                    ));
                }
                let remaining = (parent_deadline_at - now)
                    .to_std()
                    .unwrap_or(Duration::from_millis(0));
                tokio::time::sleep(remaining.min(Duration::from_millis(100))).await;
                continue;
            };
            if child_session_id.is_empty() {
                child_session_id = edge.child_session_id.clone();
            }
            let child_session_id = child_session_id.as_str();

            if edge.lifecycle_state == "cancelled" {
                self.discard_in_flight_lifecycle(internal_call_id).await;
                return Ok(foreground_terminal_failure_payload(
                    child_request_id,
                    child_session_id,
                    "interrupted",
                    "parent request was cancelled while waiting for child subagent",
                    FailureClass::External,
                ));
            }

            if edge.lifecycle_state == "failed" || edge.lifecycle_state == "timedOut" {
                self.discard_in_flight_lifecycle(internal_call_id).await;
                return Ok(foreground_terminal_failure_payload(
                    child_request_id,
                    child_session_id,
                    if edge.lifecycle_state == "timedOut" {
                        "dead"
                    } else {
                        "failed"
                    },
                    "parent subagent bridge reached a terminal failure while waiting for child subagent",
                    FailureClass::External,
                ));
            }

            if edge.lifecycle_state == "completed" {
                self.discard_in_flight_lifecycle(internal_call_id).await;
                return self
                    .foreground_completed_bridge_payload(
                        &parent_context.session_id,
                        internal_call_id,
                        child_request_id,
                        child_session_id,
                        behavior_id,
                    )
                    .await;
            }

            if edge.await_mode == AwaitMode::Background && edge.lifecycle_state == "running" {
                self.refresh_owned_in_flight_lifecycle_from_storage(
                    &parent_context.session_id,
                    internal_call_id,
                )
                .await?;
                return Ok(backgrounded_receipt_payload(
                    child_request_id,
                    child_session_id,
                    behavior_id,
                ));
            }

            if now >= parent_deadline_at {
                if edge.lifecycle_state == "running" {
                    let Some(mut lifecycle) =
                        self.take_owned_in_flight_lifecycle(internal_call_id).await
                    else {
                        wait_for_external_lifecycle_owner(
                            &mut missing_owner_since,
                            now,
                            internal_call_id,
                        )
                        .await?;
                        continue;
                    };
                    // The child may still be live: take the licensed deadline
                    // transition (`timedOut`), never a fabricated
                    // `ChildTerminal::Dead` (#1002). The child's own
                    // terminalization belongs to the subagent-liveness sweep.
                    if !lifecycle.timeout().await? {
                        return self
                            .foreground_external_bridge_terminal_payload(
                                parent_context,
                                internal_call_id,
                                child_request_id,
                                child_session_id,
                                behavior_id,
                            )
                            .await;
                    }
                }
                return Ok(foreground_terminal_failure_payload(
                    child_request_id,
                    child_session_id,
                    "dead",
                    "parent request deadline exceeded while waiting for child subagent",
                    FailureClass::External,
                ));
            }

            if let Some(row) = load_child_terminal_row(&self.node, child_request_id).await? {
                if child_request_completed(&row) {
                    let Some(final_response) = load_child_final_response(&self.node, &edge).await?
                    else {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    };
                    if edge.lifecycle_state == "running" {
                        let Some(mut lifecycle) =
                            self.take_owned_in_flight_lifecycle(internal_call_id).await
                        else {
                            wait_for_external_lifecycle_owner(
                                &mut missing_owner_since,
                                now,
                                internal_call_id,
                            )
                            .await?;
                            continue;
                        };
                        if !lifecycle.bridge_complete(final_response.clone()).await? {
                            return self
                                .foreground_external_bridge_terminal_payload(
                                    parent_context,
                                    internal_call_id,
                                    child_request_id,
                                    child_session_id,
                                    behavior_id,
                                )
                                .await;
                        }
                    } else {
                        self.discard_in_flight_lifecycle(internal_call_id).await;
                    }
                    return Ok(json_envelope_with_bounded_result(
                        json!({
                            "ok": true,
                            "child_request_id": child_request_id,
                            "child_session_id": child_session_id,
                            "behavior_id": behavior_id,
                            "await_mode": "foreground",
                            "status": "completed",
                            "final_response": serde_json::Value::Null,
                            "error": null
                        }),
                        "final_response",
                        &final_response,
                        SPAWN_SUBAGENT_TOOL_NAME,
                        &self.truncation_limits,
                    ));
                }

                if let Some(terminal) = project_child_terminal(&row) {
                    let status = child_terminal_status(&terminal);
                    let (reason, failure_class) = child_terminal_reason(&terminal);
                    if edge.lifecycle_state == "running" {
                        let Some(mut lifecycle) =
                            self.take_owned_in_flight_lifecycle(internal_call_id).await
                        else {
                            wait_for_external_lifecycle_owner(
                                &mut missing_owner_since,
                                now,
                                internal_call_id,
                            )
                            .await?;
                            continue;
                        };
                        if !lifecycle.bridge_failure(terminal).await? {
                            return self
                                .foreground_external_bridge_terminal_payload(
                                    parent_context,
                                    internal_call_id,
                                    child_request_id,
                                    child_session_id,
                                    behavior_id,
                                )
                                .await;
                        }
                    } else {
                        self.discard_in_flight_lifecycle(internal_call_id).await;
                    }
                    return Ok(foreground_terminal_failure_payload(
                        child_request_id,
                        child_session_id,
                        status,
                        reason,
                        failure_class,
                    ));
                }
            }

            let remaining = (parent_deadline_at - now)
                .to_std()
                .unwrap_or(Duration::from_millis(0));
            tokio::time::sleep(remaining.min(Duration::from_millis(250))).await;
        }
    }

    pub(super) async fn await_existing_subagent_bridge(
        &self,
        parent_context: &ParentSubagentContext,
        caller_request_id: &str,
        parent_tool_call_id: &str,
        child_request_id: &str,
        child_session_id: &str,
        behavior_id: &str,
        parent_deadline_at: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<String> {
        loop {
            let now = chrono::Utc::now();
            let edge =
                load_authorized_child_edge(&self.node, parent_context, child_request_id).await?;

            if edge.lifecycle_state == "cancelled" {
                self.discard_in_flight_lifecycle(parent_tool_call_id).await;
                return Ok(foreground_terminal_failure_payload(
                    child_request_id,
                    child_session_id,
                    "interrupted",
                    "parent request was cancelled while waiting for child subagent",
                    FailureClass::External,
                ));
            }

            if edge.lifecycle_state == "failed" || edge.lifecycle_state == "timedOut" {
                self.discard_in_flight_lifecycle(parent_tool_call_id).await;
                return Ok(foreground_terminal_failure_payload(
                    child_request_id,
                    child_session_id,
                    if edge.lifecycle_state == "timedOut" {
                        "dead"
                    } else {
                        "failed"
                    },
                    "parent subagent bridge reached a terminal failure while waiting for child subagent",
                    FailureClass::External,
                ));
            }

            if edge.lifecycle_state == "completed" {
                self.discard_in_flight_lifecycle(parent_tool_call_id).await;
                return self
                    .foreground_completed_bridge_payload(
                        &parent_context.session_id,
                        parent_tool_call_id,
                        child_request_id,
                        child_session_id,
                        behavior_id,
                    )
                    .await;
            }

            if edge.lifecycle_state == "running"
                && crate::interrupt::fetch_interrupt_requested_at(&self.node, caller_request_id)
                    .await?
                    .is_some()
            {
                if let Some(mut lifecycle) = self
                    .take_or_load_in_flight_lifecycle(
                        &parent_context.session_id,
                        parent_tool_call_id,
                    )
                    .await?
                {
                    let dispatch = match lifecycle
                        .cancel_during_run_with_cascade_dispatch(
                            CancelCause::Interrupted,
                            &self.agent_did,
                        )
                        .await
                    {
                        Ok(dispatch) => dispatch,
                        Err(error) => {
                            return self
                                .foreground_external_bridge_terminal_or_error(
                                    parent_context,
                                    parent_tool_call_id,
                                    child_request_id,
                                    child_session_id,
                                    behavior_id,
                                    error,
                                )
                                .await;
                        }
                    };
                    if !lifecycle.is_cancelled() {
                        return self
                            .foreground_external_bridge_terminal_payload(
                                parent_context,
                                parent_tool_call_id,
                                child_request_id,
                                child_session_id,
                                behavior_id,
                            )
                            .await;
                    }
                    if let Some(dispatch) = dispatch {
                        if let CascadeDispatch::Local(intent) = dispatch {
                            if let Err(error) = crate::interrupt::interrupt_request(
                                &self.node,
                                &intent.child_request_id,
                            )
                            .await
                            {
                                tracing::warn!(
                                    child_request_id = %intent.child_request_id,
                                    error = %error,
                                    "failed to cascade wait_subagent cancellation to child request"
                                );
                            }
                        }
                    }
                }
                self.discard_in_flight_lifecycle(parent_tool_call_id).await;
                return Ok(foreground_terminal_failure_payload(
                    child_request_id,
                    child_session_id,
                    "interrupted",
                    "parent request was cancelled while waiting for child subagent",
                    FailureClass::External,
                ));
            }

            if edge.await_mode == AwaitMode::Background && edge.lifecycle_state == "running" {
                self.refresh_owned_in_flight_lifecycle_from_storage(
                    &parent_context.session_id,
                    parent_tool_call_id,
                )
                .await?;
                return Ok(backgrounded_receipt_payload(
                    child_request_id,
                    child_session_id,
                    behavior_id,
                ));
            }

            if now >= parent_deadline_at {
                if edge.lifecycle_state == "running" {
                    if let Some(mut lifecycle) = self
                        .take_or_load_in_flight_lifecycle(
                            &parent_context.session_id,
                            parent_tool_call_id,
                        )
                        .await?
                    {
                        // Licensed deadline transition — no fabricated child
                        // terminal evidence (#1002); see
                        // `await_foreground_subagent`'s deadline arm.
                        let projected = match lifecycle.timeout().await {
                            Ok(projected) => projected,
                            Err(error) => {
                                return self
                                    .foreground_external_bridge_terminal_or_error(
                                        parent_context,
                                        parent_tool_call_id,
                                        child_request_id,
                                        child_session_id,
                                        behavior_id,
                                        error,
                                    )
                                    .await;
                            }
                        };
                        if !projected {
                            return self
                                .foreground_external_bridge_terminal_payload(
                                    parent_context,
                                    parent_tool_call_id,
                                    child_request_id,
                                    child_session_id,
                                    behavior_id,
                                )
                                .await;
                        }
                    }
                }
                self.discard_in_flight_lifecycle(parent_tool_call_id).await;
                return Ok(foreground_terminal_failure_payload(
                    child_request_id,
                    child_session_id,
                    "dead",
                    "parent request deadline exceeded while waiting for child subagent",
                    FailureClass::External,
                ));
            }

            if let Some(row) = load_child_terminal_row(&self.node, child_request_id).await? {
                if child_request_completed(&row) {
                    let Some(final_response) = load_child_final_response(&self.node, &edge).await?
                    else {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    };
                    if edge.lifecycle_state == "running" {
                        if let Some(mut lifecycle) = self
                            .take_or_load_in_flight_lifecycle(
                                &parent_context.session_id,
                                parent_tool_call_id,
                            )
                            .await?
                        {
                            let projected =
                                match lifecycle.bridge_complete(final_response.clone()).await {
                                    Ok(projected) => projected,
                                    Err(error) => {
                                        return self
                                            .foreground_external_bridge_terminal_or_error(
                                                parent_context,
                                                parent_tool_call_id,
                                                child_request_id,
                                                child_session_id,
                                                behavior_id,
                                                error,
                                            )
                                            .await;
                                    }
                                };
                            if !projected {
                                return self
                                    .foreground_external_bridge_terminal_payload(
                                        parent_context,
                                        parent_tool_call_id,
                                        child_request_id,
                                        child_session_id,
                                        behavior_id,
                                    )
                                    .await;
                            }
                        }
                    }
                    self.discard_in_flight_lifecycle(parent_tool_call_id).await;
                    return Ok(json_envelope_with_bounded_result(
                        json!({
                            "ok": true,
                            "child_request_id": child_request_id,
                            "child_session_id": child_session_id,
                            "behavior_id": behavior_id,
                            "await_mode": "foreground",
                            "status": "completed",
                            "final_response": serde_json::Value::Null,
                            "error": null
                        }),
                        "final_response",
                        &final_response,
                        SPAWN_SUBAGENT_TOOL_NAME,
                        &self.truncation_limits,
                    ));
                }

                if let Some(terminal) = project_child_terminal(&row) {
                    let status = child_terminal_status(&terminal);
                    let (reason, failure_class) = child_terminal_reason(&terminal);
                    if edge.lifecycle_state == "running" {
                        if let Some(mut lifecycle) = self
                            .take_or_load_in_flight_lifecycle(
                                &parent_context.session_id,
                                parent_tool_call_id,
                            )
                            .await?
                        {
                            let projected = match lifecycle.bridge_failure(terminal).await {
                                Ok(projected) => projected,
                                Err(error) => {
                                    return self
                                        .foreground_external_bridge_terminal_or_error(
                                            parent_context,
                                            parent_tool_call_id,
                                            child_request_id,
                                            child_session_id,
                                            behavior_id,
                                            error,
                                        )
                                        .await;
                                }
                            };
                            if !projected {
                                return self
                                    .foreground_external_bridge_terminal_payload(
                                        parent_context,
                                        parent_tool_call_id,
                                        child_request_id,
                                        child_session_id,
                                        behavior_id,
                                    )
                                    .await;
                            }
                        }
                    }
                    self.discard_in_flight_lifecycle(parent_tool_call_id).await;
                    return Ok(foreground_terminal_failure_payload(
                        child_request_id,
                        child_session_id,
                        status,
                        reason,
                        failure_class,
                    ));
                }
            }

            let remaining = (parent_deadline_at - now)
                .to_std()
                .unwrap_or(Duration::from_millis(0));
            tokio::time::sleep(remaining.min(Duration::from_millis(250))).await;
        }
    }

    pub(super) async fn foreground_external_bridge_terminal_payload(
        &self,
        parent_context: &ParentSubagentContext,
        internal_call_id: &str,
        child_request_id: &str,
        child_session_id: &str,
        behavior_id: &str,
    ) -> anyhow::Result<String> {
        self.discard_in_flight_lifecycle(internal_call_id).await;
        let edge = load_authorized_child_edge(&self.node, parent_context, child_request_id).await?;

        match edge.lifecycle_state.as_str() {
            "completed" => {
                self.foreground_completed_bridge_payload(
                    &parent_context.session_id,
                    internal_call_id,
                    child_request_id,
                    child_session_id,
                    behavior_id,
                )
                .await
            }
            "cancelled" => Ok(foreground_terminal_failure_payload(
                child_request_id,
                child_session_id,
                "interrupted",
                "parent request was cancelled while waiting for child subagent",
                FailureClass::External,
            )),
            "timedOut" => Ok(foreground_terminal_failure_payload(
                child_request_id,
                child_session_id,
                "dead",
                "parent subagent bridge timed out while waiting for child subagent",
                FailureClass::External,
            )),
            "failed" => Ok(foreground_terminal_failure_payload(
                child_request_id,
                child_session_id,
                "failed",
                "parent subagent bridge reached a terminal failure while waiting for child subagent",
                FailureClass::External,
            )),
            other => anyhow::bail!(
                "spawn_subagent foreground bridge lost running compare but persisted lifecycle_state is {other}"
            ),
        }
    }

    pub(super) async fn foreground_completed_bridge_payload(
        &self,
        session_id: &str,
        internal_call_id: &str,
        child_request_id: &str,
        child_session_id: &str,
        behavior_id: &str,
    ) -> anyhow::Result<String> {
        let final_response =
            load_tool_call_result(&self.node, session_id, internal_call_id).await?;
        Ok(json_envelope_with_bounded_result(
            json!({
                "ok": true,
                "child_request_id": child_request_id,
                "child_session_id": child_session_id,
                "behavior_id": behavior_id,
                "await_mode": "foreground",
                "status": "completed",
                "final_response": serde_json::Value::Null,
                "error": null
            }),
            "final_response",
            &final_response,
            SPAWN_SUBAGENT_TOOL_NAME,
            &self.truncation_limits,
        ))
    }

    pub(super) async fn take_owned_in_flight_lifecycle(
        &self,
        internal_call_id: &str,
    ) -> Option<ToolCallLifecycle> {
        self.in_flight_lifecycles
            .lock()
            .await
            .remove(internal_call_id)
    }

    pub(super) async fn take_or_load_in_flight_lifecycle(
        &self,
        session_id: &str,
        internal_call_id: &str,
    ) -> anyhow::Result<Option<ToolCallLifecycle>> {
        if let Some(lifecycle) = self.take_owned_in_flight_lifecycle(internal_call_id).await {
            return Ok(Some(lifecycle));
        }

        ToolCallLifecycle::load(self.node.clone(), session_id, internal_call_id).await
    }

    pub(super) async fn discard_in_flight_lifecycle(&self, internal_call_id: &str) {
        self.in_flight_lifecycles
            .lock()
            .await
            .remove(internal_call_id);
    }

    pub(super) async fn foreground_external_bridge_terminal_or_error(
        &self,
        parent_context: &ParentSubagentContext,
        parent_tool_call_id: &str,
        child_request_id: &str,
        child_session_id: &str,
        behavior_id: &str,
        error: anyhow::Error,
    ) -> anyhow::Result<String> {
        let edge = load_authorized_child_edge(&self.node, parent_context, child_request_id).await?;
        if edge.lifecycle_state == "running" {
            return Err(error);
        }

        self.foreground_external_bridge_terminal_payload(
            parent_context,
            parent_tool_call_id,
            child_request_id,
            child_session_id,
            behavior_id,
        )
        .await
    }

    pub(super) async fn foreground_and_track_existing_subagent_bridge(
        &self,
        parent_context: &ParentSubagentContext,
        child_request_id: &str,
        parent_tool_call_id: &str,
    ) -> anyhow::Result<()> {
        let Some(mut lifecycle) = ToolCallLifecycle::load(
            self.node.clone(),
            &parent_context.session_id,
            parent_tool_call_id,
        )
        .await?
        else {
            return Ok(());
        };

        if let Err(error) = lifecycle.foreground().await {
            let refreshed =
                load_authorized_child_edge(&self.node, parent_context, child_request_id).await?;
            if refreshed.lifecycle_state == "running"
                && refreshed.await_mode == AwaitMode::Background
            {
                return Err(error);
            }

            tracing::debug!(
                tool_call_id = %parent_tool_call_id,
                child_request_id = %child_request_id,
                error = %error,
                lifecycle_state = %refreshed.lifecycle_state,
                await_mode = ?refreshed.await_mode,
                "wait_subagent foreground race resolved by refreshed bridge state"
            );
        }

        self.track_in_flight_lifecycle_from_storage(&parent_context.session_id, parent_tool_call_id)
            .await
    }

    pub(super) async fn track_in_flight_lifecycle_from_storage(
        &self,
        session_id: &str,
        internal_call_id: &str,
    ) -> anyhow::Result<()> {
        if let Some(lifecycle) =
            ToolCallLifecycle::load(self.node.clone(), session_id, internal_call_id).await?
        {
            if lifecycle.is_running() {
                self.in_flight_lifecycles
                    .lock()
                    .await
                    .insert(internal_call_id.to_string(), lifecycle);
            }
        }
        Ok(())
    }

    pub(super) async fn refresh_owned_in_flight_lifecycle_from_storage(
        &self,
        session_id: &str,
        internal_call_id: &str,
    ) -> anyhow::Result<()> {
        if !self
            .in_flight_lifecycles
            .lock()
            .await
            .contains_key(internal_call_id)
        {
            return Ok(());
        }

        if let Some(lifecycle) =
            ToolCallLifecycle::load(self.node.clone(), session_id, internal_call_id).await?
        {
            let mut map = self.in_flight_lifecycles.lock().await;
            if map.contains_key(internal_call_id) {
                if lifecycle.is_running() {
                    map.insert(internal_call_id.to_string(), lifecycle);
                } else {
                    map.remove(internal_call_id);
                }
            }
        }
        Ok(())
    }

    pub(super) async fn load_authorized_background_tool(
        &self,
        caller: &ProcessControlScope,
        tool_call_id: &str,
    ) -> anyhow::Result<ToolCallLifecycle> {
        let Some(lifecycle) =
            ToolCallLifecycle::load(self.node.clone(), &caller.session_id, tool_call_id).await?
        else {
            anyhow::bail!("background tool call {tool_call_id} was not found");
        };
        if !caller.authorizes(
            lifecycle.session_id(),
            lifecycle.agent_did(),
            lifecycle.requester_did(),
        ) || lifecycle.await_mode() != AwaitMode::Background
            || lifecycle.is_subagent_bridge()
        {
            anyhow::bail!(
                "background tool call {tool_call_id} is not manageable by this session principal"
            );
        }
        Ok(lifecycle)
    }

    pub(super) async fn await_background_tool(
        &self,
        caller: &ProcessControlScope,
        tool_call_id: &str,
        caller_deadline_at: chrono::DateTime<chrono::Utc>,
        wait_deadline_at: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<String> {
        loop {
            let now = chrono::Utc::now();
            let lifecycle = self
                .load_authorized_background_tool(caller, tool_call_id)
                .await?;
            if lifecycle.is_terminal() {
                return self.background_tool_envelope(lifecycle, "terminal").await;
            }

            // Waiting is observational. Ending or interrupting this caller's
            // turn must not revoke the separately budgeted background job.
            if crate::interrupt::fetch_interrupt_requested_at(&self.node, &caller.request_id)
                .await?
                .is_some()
            {
                return self
                    .background_tool_envelope(lifecycle, "caller_interrupted")
                    .await;
            }

            if now >= caller_deadline_at {
                return self
                    .background_tool_envelope(lifecycle, "caller_deadline_exceeded")
                    .await;
            }

            // Bounded wait (#985): report the process as still running — do
            // NOT cancel it; the run continues and completion is delivered
            // via the background completion notification.
            if now >= wait_deadline_at {
                return self
                    .background_tool_envelope(lifecycle, "wait_timeout")
                    .await;
            }

            let remaining = (caller_deadline_at.min(wait_deadline_at) - now)
                .to_std()
                .unwrap_or(Duration::from_millis(0));
            tokio::time::sleep(remaining.min(Duration::from_millis(100))).await;
        }
    }

    pub(super) async fn cancel_background_tool_lifecycle(
        &self,
        mut lifecycle: ToolCallLifecycle,
        cause: CancelCause,
        completion_reason: &str,
    ) -> anyhow::Result<(ToolCallLifecycle, bool)> {
        let won_terminal_compare = if lifecycle.is_running() {
            lifecycle
                .cancel_during_run_owned(cause, completion_reason)
                .await?
        } else {
            false
        };
        // Persist the explicit cancellation before waking the worker. If the
        // token fires first, the worker can win the same running-state compare
        // and replace the user's specific cause with generic `interrupted`.
        if won_terminal_compare {
            self.background_executions
                .cancel(lifecycle.tool_call_id())
                .await;
        }
        Ok((lifecycle, won_terminal_compare))
    }

    pub(super) async fn background_tool_envelope(
        &self,
        lifecycle: ToolCallLifecycle,
        reason: &str,
    ) -> anyhow::Result<String> {
        let session_id = self
            .state
            .lock()
            .await
            .session_id
            .clone()
            .unwrap_or_default();
        let result = load_tool_call_result(&self.node, &session_id, lifecycle.tool_call_id())
            .await
            .unwrap_or_default();
        let status = lifecycle.state().as_str();
        let error = if lifecycle.state() == crate::tool_call_lifecycle::ToolCallState::Completed {
            serde_json::Value::Null
        } else {
            json!({
                "reason": reason,
                "failure_class": "external"
            })
        };
        Ok(json_envelope_with_bounded_result(
            json!({
                "ok": lifecycle.state() == crate::tool_call_lifecycle::ToolCallState::Completed,
                "tool_call_id": lifecycle.tool_call_id(),
                "tool_name": lifecycle.tool_name(),
                "await_mode": "background",
                "status": status,
                "result": serde_json::Value::Null,
                "error": error
            }),
            "result",
            &result,
            lifecycle.tool_name(),
            &self.truncation_limits,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn fail_spawn_subagent_tool_call(
        &self,
        session_id: String,
        request_id: String,
        deadline_at: chrono::DateTime<chrono::Utc>,
        message_sequence: u32,
        internal_call_id: &str,
        args: &str,
        failure_class: FailureClass,
        result: String,
    ) -> anyhow::Result<ToolCallHookAction> {
        let mut lifecycle = ToolCallLifecycle::new(
            self.node.clone(),
            request_id,
            session_id,
            self.agent_did.clone(),
            internal_call_id.to_string(),
            message_sequence,
            SPAWN_SUBAGENT_TOOL_NAME.to_string(),
            args.to_string(),
            deadline_at,
        )
        .with_requester_did(self.active_requester_did().await)
        .with_request_doc_id(self.active_request_doc_id().await);
        lifecycle.spawn_failed(failure_class, &result).await?;
        Ok(self.skip_tool_result(SPAWN_SUBAGENT_TOOL_NAME, result))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn fail_background_meta_tool_call(
        &self,
        session_id: String,
        request_id: String,
        deadline_at: chrono::DateTime<chrono::Utc>,
        message_sequence: u32,
        internal_call_id: &str,
        tool_name: &str,
        args: &str,
        failure_class: FailureClass,
        result: String,
    ) -> anyhow::Result<ToolCallHookAction> {
        let mut lifecycle = ToolCallLifecycle::new(
            self.node.clone(),
            request_id,
            session_id,
            self.agent_did.clone(),
            internal_call_id.to_string(),
            message_sequence,
            tool_name.to_string(),
            args.to_string(),
            deadline_at,
        )
        .with_requester_did(self.active_requester_did().await)
        .with_request_doc_id(self.active_request_doc_id().await);
        lifecycle.spawn_failed(failure_class, &result).await?;
        Ok(self.skip_tool_result(tool_name, result))
    }
}
