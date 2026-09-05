use super::*;

impl DefraSessionHook {
    pub(super) async fn persist_wait_subagent_tool_call(
        &self,
        tool_call_id: Option<String>,
        internal_call_id: &str,
        args: &str,
    ) -> anyhow::Result<ToolCallHookAction> {
        let (_session_id, request_id, parent_deadline_at, _seq) =
            self.ensure_assistant_turn_sequence().await?;
        self.state.lock().await.register_tool_result_identity(
            internal_call_id,
            None,
            tool_call_id.as_deref(),
        );

        let parsed = match serde_json::from_str::<WaitSubagentArgs>(args) {
            Ok(args) => args,
            Err(error) => {
                return Ok(self.skip_tool_result(
                    WAIT_SUBAGENT_TOOL_NAME,
                    invalid_tool_arguments_payload(
                        WAIT_SUBAGENT_TOOL_NAME,
                        "/",
                        format!("invalid wait_subagent arguments: {error}"),
                    ),
                ));
            }
        };
        let child_request_id = parsed.child_request_id.trim();
        if child_request_id.is_empty() {
            return Ok(self.skip_tool_result(
                WAIT_SUBAGENT_TOOL_NAME,
                invalid_tool_arguments_payload(
                    WAIT_SUBAGENT_TOOL_NAME,
                    "/child_request_id",
                    "child_request_id is required",
                ),
            ));
        }

        let Some(canonical) = crate::descendant_graph::resolve_session_descendant_edge(
            DescendantGraphAccess::Local(&self.node),
            &request_id,
            child_request_id,
        )
        .await?
        else {
            let result = service_unavailable_payload(
                WAIT_SUBAGENT_TOOL_NAME,
                "/child_request_id",
                "child subagent request is not available to this session principal",
                false,
            );
            return Ok(self.skip_tool_result(WAIT_SUBAGENT_TOOL_NAME, result));
        };
        if !canonical.readable() {
            let result = service_unavailable_payload(
                WAIT_SUBAGENT_TOOL_NAME,
                "/child_request_id",
                canonical.diagnostic.clone().unwrap_or_else(|| {
                    format!("child request {child_request_id} is not materialized")
                }),
                canonical.retryable(),
            );
            return Ok(self.skip_tool_result(WAIT_SUBAGENT_TOOL_NAME, result));
        }
        if !canonical.controllable() {
            let result = tool_not_allowed_payload(
                WAIT_SUBAGENT_TOOL_NAME,
                "/child_request_id",
                child_request_id,
                "descendant is visible but control belongs to its immediate parent session principal",
                Vec::new(),
            );
            return Ok(self.skip_tool_result(WAIT_SUBAGENT_TOOL_NAME, result));
        }
        let edge = ChildEdge::from_descendant(&canonical).ok_or_else(|| {
            anyhow::anyhow!("authorized descendant edge lacks materialized child identity")
        })?;
        let parent_context =
            load_parent_subagent_context(&self.node, &edge.parent_request_id).await?;

        if edge.lifecycle_state == "running" {
            if edge.await_mode == AwaitMode::Background {
                self.foreground_and_track_existing_subagent_bridge(
                    &parent_context,
                    child_request_id,
                    &edge.parent_tool_call_id,
                )
                .await?;
            } else {
                self.track_in_flight_lifecycle_from_storage(
                    &parent_context.session_id,
                    &edge.parent_tool_call_id,
                )
                .await?;
            }
        }

        let result = self
            .await_existing_subagent_bridge(
                &parent_context,
                &request_id,
                &edge.parent_tool_call_id,
                &edge.child_request_id,
                &edge.child_session_id,
                &edge.behavior_id,
                parent_deadline_at.min(parent_context.request_deadline_at),
            )
            .await?;

        Ok(self.skip_tool_result(WAIT_SUBAGENT_TOOL_NAME, result))
    }

    pub(super) async fn persist_list_subagents_tool_call(
        &self,
        tool_call_id: Option<String>,
        internal_call_id: &str,
        args: &str,
    ) -> anyhow::Result<ToolCallHookAction> {
        let (_session_id, request_id, _deadline_at, _seq) =
            self.ensure_assistant_turn_sequence().await?;
        self.state.lock().await.register_tool_result_identity(
            internal_call_id,
            None,
            tool_call_id.as_deref(),
        );

        let parsed = match serde_json::from_str::<ListSubagentsArgs>(args) {
            Ok(args) => args,
            Err(error) => {
                return Ok(self.skip_tool_result(
                    LIST_SUBAGENTS_TOOL_NAME,
                    invalid_tool_arguments_payload(
                        LIST_SUBAGENTS_TOOL_NAME,
                        "/",
                        format!("invalid list_subagents arguments: {error}"),
                    ),
                ));
            }
        };
        let response = handle_list_subagents(&self.node, &request_id, parsed).await?;
        let result = serde_json::to_value(response)
            .map_err(|error| anyhow::anyhow!("serialize list_subagents response: {error}"))?;
        Ok(self.skip_tool_result(LIST_SUBAGENTS_TOOL_NAME, json_string(result)))
    }

    pub(super) async fn persist_read_subagent_tool_call(
        &self,
        tool_call_id: Option<String>,
        internal_call_id: &str,
        args: &str,
    ) -> anyhow::Result<ToolCallHookAction> {
        let (_session_id, request_id, _deadline_at, _seq) =
            self.ensure_assistant_turn_sequence().await?;
        self.state.lock().await.register_tool_result_identity(
            internal_call_id,
            None,
            tool_call_id.as_deref(),
        );

        let parsed = match serde_json::from_str::<ReadSubagentArgs>(args) {
            Ok(args) => args,
            Err(error) => {
                return Ok(self.skip_tool_result(
                    READ_SUBAGENT_TOOL_NAME,
                    invalid_tool_arguments_payload(
                        READ_SUBAGENT_TOOL_NAME,
                        "/",
                        format!("invalid read_subagent arguments: {error}"),
                    ),
                ));
            }
        };
        let child_request_id = parsed.child_request_id.trim().to_string();
        if child_request_id.is_empty() {
            return Ok(self.skip_tool_result(
                READ_SUBAGENT_TOOL_NAME,
                invalid_tool_arguments_payload(
                    READ_SUBAGENT_TOOL_NAME,
                    "/child_request_id",
                    "child_request_id is required",
                ),
            ));
        }

        let Some(response) = handle_read_subagent(&self.node, &request_id, parsed).await? else {
            return Ok(self.skip_tool_result(
                READ_SUBAGENT_TOOL_NAME,
                tool_not_allowed_payload(
                    READ_SUBAGENT_TOOL_NAME,
                    "/child_request_id",
                    &child_request_id,
                    "child is not a background subagent owned by this parent request",
                    Vec::new(),
                ),
            ));
        };
        let result = serde_json::to_value(response)
            .map_err(|error| anyhow::anyhow!("serialize read_subagent response: {error}"))?;
        Ok(self.skip_tool_result(READ_SUBAGENT_TOOL_NAME, json_string(result)))
    }

    pub(super) async fn persist_steer_subagent_tool_call(
        &self,
        tool_call_id: Option<String>,
        internal_call_id: &str,
        args: &str,
    ) -> anyhow::Result<ToolCallHookAction> {
        let (_session_id, request_id, _deadline_at, _seq) =
            self.ensure_assistant_turn_sequence().await?;
        self.state.lock().await.register_tool_result_identity(
            internal_call_id,
            None,
            tool_call_id.as_deref(),
        );

        let parsed = match serde_json::from_str::<SteerSubagentArgs>(args) {
            Ok(args) => args,
            Err(error) => {
                return Ok(self.skip_tool_result(
                    STEER_SUBAGENT_TOOL_NAME,
                    invalid_tool_arguments_payload(
                        STEER_SUBAGENT_TOOL_NAME,
                        "/",
                        format!("invalid steer_subagent arguments: {error}"),
                    ),
                ));
            }
        };
        let child_request_id = parsed.child_request_id.trim().to_string();
        if child_request_id.is_empty() {
            return Ok(self.skip_tool_result(
                STEER_SUBAGENT_TOOL_NAME,
                invalid_tool_arguments_payload(
                    STEER_SUBAGENT_TOOL_NAME,
                    "/child_request_id",
                    "child_request_id is required",
                ),
            ));
        }
        let message = parsed.message.trim().to_string();
        if message.is_empty() {
            return Ok(self.skip_tool_result(
                STEER_SUBAGENT_TOOL_NAME,
                invalid_tool_arguments_payload(
                    STEER_SUBAGENT_TOOL_NAME,
                    "/message",
                    "message is required",
                ),
            ));
        }

        let edge = match load_steer_subagent_target(&self.node, &request_id, &child_request_id)
            .await?
        {
            SteerSubagentTarget::Found(edge) => edge,
            SteerSubagentTarget::NotAuthorized => {
                return Ok(self.skip_tool_result(
                    STEER_SUBAGENT_TOOL_NAME,
                    tool_not_allowed_payload(
                        STEER_SUBAGENT_TOOL_NAME,
                        "/child_request_id",
                        &child_request_id,
                        "child not owned by this parent request",
                        Vec::new(),
                    ),
                ));
            }
            SteerSubagentTarget::NotBackgrounded => {
                return Ok(self.skip_tool_result(
                    STEER_SUBAGENT_TOOL_NAME,
                    tool_not_allowed_payload(
                        STEER_SUBAGENT_TOOL_NAME,
                        "/child_request_id",
                        &child_request_id,
                        "foreground subagents cannot be steered; call cancel_subagent first",
                        Vec::new(),
                    ),
                ));
            }
            SteerSubagentTarget::AwaitingMaterialization { message, retryable } => {
                let result = service_unavailable_payload(
                    STEER_SUBAGENT_TOOL_NAME,
                    "/child_request_id",
                    message,
                    retryable,
                );
                return Ok(self.skip_tool_result(STEER_SUBAGENT_TOOL_NAME, result));
            }
            SteerSubagentTarget::Terminal(state) => {
                let result = invalid_tool_arguments_payload(
                    STEER_SUBAGENT_TOOL_NAME,
                    "/child_request_id",
                    format!("child is in terminal state '{state}'; spawn a new subagent instead"),
                );
                return Ok(self.skip_tool_result(STEER_SUBAGENT_TOOL_NAME, result));
            }
        };

        let mut interrupted_active_request_id = None;
        let mut drained_wake_up_request_ids = Vec::new();
        if parsed.interrupt {
            drained_wake_up_request_ids =
                pending_automated_wakeup_request_ids(&self.node, &edge.child_session_id).await?;
            if let Some(active_request_id) =
                active_session_request_id(&self.node, &edge.child_session_id).await?
            {
                crate::interrupt::interrupt_request(&self.node, &active_request_id).await?;
                let _descendants_cancelled = self
                    .cancel_live_subagent_descendants(
                        &edge.child_session_id,
                        CancelCause::UserCancelled,
                    )
                    .await?;
                interrupted_active_request_id = Some(active_request_id);
            }
            let post_interrupt_drained = drain_automated_wakeups_returning_ids(
                &self.node,
                &edge.child_session_id,
                &edge.child_agent_did,
                "automated wake-up drained because subagent was steered with interrupt=true",
            )
            .await?;
            for request_id in post_interrupt_drained {
                if !drained_wake_up_request_ids
                    .iter()
                    .any(|existing| existing == &request_id)
                {
                    drained_wake_up_request_ids.push(request_id);
                }
            }
        }

        let response = append_steering_request(
            &self.node,
            &request_id,
            &edge,
            &message,
            interrupted_active_request_id,
            drained_wake_up_request_ids,
        )
        .await?;
        let result = serde_json::to_value(response)
            .map_err(|error| anyhow::anyhow!("serialize steer_subagent response: {error}"))?;
        Ok(self.skip_tool_result(STEER_SUBAGENT_TOOL_NAME, json_string(result)))
    }

    pub(super) async fn persist_cancel_subagent_tool_call(
        &self,
        tool_call_id: Option<String>,
        internal_call_id: &str,
        args: &str,
    ) -> anyhow::Result<ToolCallHookAction> {
        let (_session_id, request_id, _deadline_at, _seq) =
            self.ensure_assistant_turn_sequence().await?;
        self.state.lock().await.register_tool_result_identity(
            internal_call_id,
            None,
            tool_call_id.as_deref(),
        );

        let parsed = match serde_json::from_str::<CancelSubagentArgs>(args) {
            Ok(args) => args,
            Err(error) => {
                return Ok(self.skip_tool_result(
                    CANCEL_SUBAGENT_TOOL_NAME,
                    invalid_tool_arguments_payload(
                        CANCEL_SUBAGENT_TOOL_NAME,
                        "/",
                        format!("invalid cancel_subagent arguments: {error}"),
                    ),
                ));
            }
        };
        let child_request_id = parsed.child_request_id.trim();
        if child_request_id.is_empty() {
            return Ok(self.skip_tool_result(
                CANCEL_SUBAGENT_TOOL_NAME,
                invalid_tool_arguments_payload(
                    CANCEL_SUBAGENT_TOOL_NAME,
                    "/child_request_id",
                    "child_request_id is required",
                ),
            ));
        }
        if parsed
            .reason
            .as_deref()
            .is_some_and(|reason| reason.trim().is_empty())
        {
            return Ok(self.skip_tool_result(
                CANCEL_SUBAGENT_TOOL_NAME,
                invalid_tool_arguments_payload(
                    CANCEL_SUBAGENT_TOOL_NAME,
                    "/reason",
                    "reason must be omitted or non-empty",
                ),
            ));
        }

        let outcome = crate::cancel_session_subagent(
            self.node.clone(),
            &request_id,
            child_request_id,
            parsed.reason.as_deref(),
        )
        .await?;
        let edge = match outcome {
            crate::CancelSubagentOutcome::Unavailable {
                diagnostic,
                retryable,
            } => {
                return Ok(self.skip_tool_result(
                    CANCEL_SUBAGENT_TOOL_NAME,
                    service_unavailable_payload(
                        CANCEL_SUBAGENT_TOOL_NAME,
                        "/child_request_id",
                        diagnostic,
                        retryable,
                    ),
                ));
            }
            crate::CancelSubagentOutcome::NotAuthorized => {
                return Ok(self.skip_tool_result(CANCEL_SUBAGENT_TOOL_NAME,
                    tool_not_allowed_payload(CANCEL_SUBAGENT_TOOL_NAME, "/child_request_id", child_request_id,
                        "descendant is visible but control belongs to its immediate parent session principal", Vec::new())));
            }
            crate::CancelSubagentOutcome::Cancelled(receipt)
            | crate::CancelSubagentOutcome::AlreadyTerminal(receipt) => receipt,
        };
        // The shared helper owns persisted cancellation. Refresh the hook's
        // execution handle so it cannot retain a now-terminal bridge.
        self.refresh_owned_in_flight_lifecycle_from_storage(
            &edge.parent_session_id,
            &edge.parent_tool_call_id,
        )
        .await?;

        Ok(self.skip_tool_result(
            CANCEL_SUBAGENT_TOOL_NAME,
            json_string(json!({
                "ok": true,
                "child_request_id": edge.child_request_id,
                "child_session_id": edge.child_session_id,
                "behavior_id": edge.behavior_id,
                "status": "cancelled",
                "active_interrupted": edge.active_interrupted,
                "descendants_cancelled": edge.descendants_cancelled,
                "queued_drained": edge.queued_drained
            })),
        ))
    }
}
