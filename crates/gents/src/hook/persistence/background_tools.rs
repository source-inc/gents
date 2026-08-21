use super::*;
use futures::FutureExt;
use std::panic::AssertUnwindSafe;

async fn project_background_completion_if_owned<F>(
    won_terminal_compare: bool,
    projection: F,
) -> Option<F::Output>
where
    F: std::future::Future,
{
    if won_terminal_compare {
        Some(projection.await)
    } else {
        None
    }
}

impl DefraSessionHook {
    pub(super) async fn persist_background_tool_call(
        &self,
        tool_call_id: Option<String>,
        internal_call_id: &str,
        args: &str,
    ) -> anyhow::Result<ToolCallHookAction> {
        let (session_id, request_id, deadline_at, seq) =
            self.ensure_assistant_turn_sequence().await?;
        self.state.lock().await.register_tool_result_identity(
            internal_call_id,
            None,
            tool_call_id.as_deref(),
        );

        let parsed = match serde_json::from_str::<BackgroundToolArgs>(args) {
            Ok(args) => args,
            Err(error) => {
                return self
                    .fail_background_meta_tool_call(
                        session_id,
                        request_id,
                        deadline_at,
                        seq,
                        internal_call_id,
                        SPAWN_PROCESS_TOOL_NAME,
                        args,
                        FailureClass::ArgumentInvalid,
                        background_invalid_tool_arguments_payload(
                            SPAWN_PROCESS_TOOL_NAME,
                            "/",
                            format!("invalid spawn_process arguments: {error}"),
                        ),
                    )
                    .await;
            }
        };

        let target_name = parsed.tool_name.trim();
        if target_name.is_empty() {
            return self
                .fail_background_meta_tool_call(
                    session_id,
                    request_id,
                    deadline_at,
                    seq,
                    internal_call_id,
                    SPAWN_PROCESS_TOOL_NAME,
                    args,
                    FailureClass::ArgumentInvalid,
                    background_invalid_tool_arguments_payload(
                        SPAWN_PROCESS_TOOL_NAME,
                        "/tool_name",
                        "tool_name is required",
                    ),
                )
                .await;
        }

        let Some(target_tool) = self.background_tool_registry.get(target_name) else {
            return self
                .fail_background_meta_tool_call(
                    session_id,
                    request_id,
                    deadline_at,
                    seq,
                    internal_call_id,
                    SPAWN_PROCESS_TOOL_NAME,
                    args,
                    FailureClass::ServiceUnavailable,
                    background_tool_not_allowed_payload(
                        SPAWN_PROCESS_TOOL_NAME,
                        "/tool_name",
                        target_name,
                        format!(
                            "tool '{target_name}' is not allowed for backgrounding by this behavior"
                        ),
                        self.background_tool_registry.allowlist(),
                    ),
                )
                .await;
        };

        let live_count = count_live_backgrounded_rows(&self.node, &request_id).await?;
        if live_count >= MAX_BACKGROUNDED_TOOLS_PER_PARENT {
            return self
                .fail_background_meta_tool_call(
                    session_id,
                    request_id,
                    deadline_at,
                    seq,
                    internal_call_id,
                    SPAWN_PROCESS_TOOL_NAME,
                    args,
                    FailureClass::ArgumentInvalid,
                    background_budget_exceeded_payload(live_count),
                )
                .await;
        }

        let background_tool_call_id = uuid::Uuid::new_v4().to_string();
        let target_tool_name = target_name.to_string();
        let target_args = serde_json::to_string(&parsed.args)?;
        // Backgrounded executions are decoupled from the parent request
        // deadline (like background subagent bridges): they get the
        // background lifetime budget, with cancel_process and the completion
        // notification as the lifecycle controls (#985).
        let background_deadline_at = chrono::Utc::now()
            + chrono::Duration::seconds(crate::toolset::BACKGROUND_COMMAND_TIMEOUT_SECS as i64);
        let mut lifecycle = ToolCallLifecycle::new_background_tool(
            self.node.clone(),
            request_id.clone(),
            session_id.clone(),
            self.agent_did.clone(),
            background_tool_call_id.clone(),
            seq,
            target_tool_name.clone(),
            target_args.clone(),
            background_deadline_at,
        )
        .with_requester_did(self.active_requester_did().await)
        .with_request_doc_id(self.active_request_doc_id().await);
        let cancellation_token = tokio_util::sync::CancellationToken::new();
        let execution_reservation = self
            .background_executions
            .reserve(background_tool_call_id.clone(), cancellation_token.clone());
        lifecycle.start_running().await?;

        let node = self.node.clone();
        let executions = self.background_executions.clone();
        let live_outputs = self.background_live_outputs.clone();
        let execution_call_id = background_tool_call_id.clone();
        let execution_session_id = session_id.clone();
        let execution_request_id = request_id.clone();
        let execution_tool_name = target_tool_name.clone();
        let live_output_writer = live_outputs
            .writer_for(background_tool_call_id.clone())
            .await;
        self.ensure_live_output_flusher();
        let runtime_context = crate::tool_call_lifecycle::runtime::current_tool_runtime_context();
        let workspace = crate::tool_call_lifecycle::runtime::ToolWorkspaceScope {
            workspace_cwd: runtime_context
                .as_ref()
                .and_then(|runtime| runtime.workspace_cwd.clone()),
            workspace_root: runtime_context
                .as_ref()
                .and_then(|runtime| runtime.workspace_root.clone()),
            workspace_authority: runtime_context
                .as_ref()
                .and_then(|runtime| runtime.workspace_authority),
        };
        let correlation = runtime_context
            .as_ref()
            .and_then(|runtime| runtime.correlation.clone());
        let source_fields = runtime_context
            .map(|runtime| runtime.source_fields)
            .unwrap_or_default();
        tokio::spawn(async move {
            let execution = AssertUnwindSafe(async {
                crate::tool_call_lifecycle::runtime::scope_request_tool_execution_with_workspace_overlay(
                    Some(background_deadline_at),
                    cancellation_token.clone(),
                    workspace,
                    Some(live_output_writer),
                    Some(execution_session_id.clone()),
                    correlation,
                    source_fields,
                    true,
                    async {
                        crate::tool_call_lifecycle::runtime::call_tool_managed(
                            target_tool.as_ref(),
                            target_args,
                        )
                        .await
                    },
                )
                .await
            })
            .catch_unwind()
            .await;

            match execution {
                Ok(outcome) => match outcome {
                    crate::tool_call_lifecycle::ToolOutcome::TimedOut { .. } => {
                        let won_terminal_compare = match lifecycle
                            .bridge_failure_with_completion_reason(
                                background_timeout_terminal(),
                                BACKGROUND_TIMEOUT_COMPLETION_REASON,
                            )
                            .await
                        {
                            Ok(updated) => updated,
                            Err(error) => {
                                tracing::warn!(
                                    tool_call_id = %execution_call_id,
                                    error = %error,
                                    "failed to terminalize timed-out background tool"
                                );
                                false
                            }
                        };
                        if let Some(Err(error)) = project_background_completion_if_owned(
                            won_terminal_compare,
                            crate::background_completion::append_background_tool_completion(
                                node.as_ref(),
                                &execution_session_id,
                                &execution_request_id,
                                &execution_call_id,
                                &execution_tool_name,
                                "failed",
                                "",
                                Some("deadline_exceeded"),
                            ),
                        )
                        .await
                        {
                            tracing::warn!(tool_call_id = %execution_call_id, error = %error, "failed to append timed-out background tool notification");
                        }
                    }
                    crate::tool_call_lifecycle::ToolOutcome::Cancelled => {
                        let won_terminal_compare =
                            match lifecycle.bridge_failure(ChildTerminal::Interrupted).await {
                                Ok(updated) => updated,
                                Err(error) => {
                                    tracing::warn!(
                                        tool_call_id = %execution_call_id,
                                        error = %error,
                                        "failed to terminalize cancelled background tool"
                                    );
                                    false
                                }
                            };
                        if let Some(Err(error)) = project_background_completion_if_owned(
                            won_terminal_compare,
                            crate::background_completion::append_background_tool_completion(
                                node.as_ref(),
                                &execution_session_id,
                                &execution_request_id,
                                &execution_call_id,
                                &execution_tool_name,
                                "cancelled",
                                "",
                                Some("explicit_cancel"),
                            ),
                        )
                        .await
                        {
                            tracing::warn!(tool_call_id = %execution_call_id, error = %error, "failed to append cancelled background tool notification");
                        }
                    }
                    crate::tool_call_lifecycle::ToolOutcome::Completed(output) => {
                        let notification_result = output.clone();
                        let won_terminal_compare = match lifecycle.bridge_complete(output).await {
                            Ok(updated) => updated,
                            Err(error) => {
                                tracing::warn!(
                                    tool_call_id = %execution_call_id,
                                    error = %error,
                                    "failed to complete background tool"
                                );
                                false
                            }
                        };
                        if let Some(Err(error)) = project_background_completion_if_owned(
                            won_terminal_compare,
                            crate::background_completion::append_background_tool_completion(
                                node.as_ref(),
                                &execution_session_id,
                                &execution_request_id,
                                &execution_call_id,
                                &execution_tool_name,
                                "completed",
                                &notification_result,
                                None,
                            ),
                        )
                        .await
                        {
                            tracing::warn!(tool_call_id = %execution_call_id, error = %error, "failed to append completed background tool notification");
                        }
                    }
                    crate::tool_call_lifecycle::ToolOutcome::Failed {
                        class: failure_class,
                        text: reason,
                        ..
                    } => {
                        let won_terminal_compare = match lifecycle
                            .bridge_failure(ChildTerminal::Failed {
                                reason: reason.clone(),
                                failure_class,
                            })
                            .await
                        {
                            Ok(updated) => updated,
                            Err(error) => {
                                tracing::warn!(
                                    tool_call_id = %execution_call_id,
                                    error = %error,
                                    "failed to fail background tool"
                                );
                                false
                            }
                        };
                        if let Some(Err(error)) = project_background_completion_if_owned(
                            won_terminal_compare,
                            crate::background_completion::append_background_tool_completion(
                                node.as_ref(),
                                &execution_session_id,
                                &execution_request_id,
                                &execution_call_id,
                                &execution_tool_name,
                                "failed",
                                &reason,
                                Some("tool_failed"),
                            ),
                        )
                        .await
                        {
                            tracing::warn!(tool_call_id = %execution_call_id, error = %error, "failed to append failed background tool notification");
                        }
                    }
                },
                Err(panic) => {
                    let panic = panic_payload_message(panic.as_ref());
                    let reason = format!("background tool panicked: {panic}");
                    let won_terminal_compare = match lifecycle
                        .bridge_failure_with_completion_reason(
                            ChildTerminal::Failed {
                                reason: reason.clone(),
                                failure_class: FailureClass::External,
                            },
                            "tool_panicked",
                        )
                        .await
                    {
                        Ok(updated) => updated,
                        Err(error) => {
                            tracing::warn!(
                                tool_call_id = %execution_call_id,
                                error = %error,
                                "failed to terminalize panicking background tool"
                            );
                            false
                        }
                    };
                    if let Some(Err(error)) = project_background_completion_if_owned(
                        won_terminal_compare,
                        crate::background_completion::append_background_tool_completion(
                            node.as_ref(),
                            &execution_session_id,
                            &execution_request_id,
                            &execution_call_id,
                            &execution_tool_name,
                            "failed",
                            &reason,
                            Some("tool_panicked"),
                        ),
                    )
                    .await
                    {
                        tracing::warn!(
                            tool_call_id = %execution_call_id,
                            error = %error,
                            "failed to append panicking background tool notification"
                        );
                    }
                }
            }

            executions.remove(&execution_call_id).await;
            live_outputs.remove(&execution_call_id).await;
        });
        execution_reservation.disarm();

        Ok(self.skip_tool_result(
            SPAWN_PROCESS_TOOL_NAME,
            json_string(json!({
                "ok": true,
                "tool_call_id": background_tool_call_id,
                "tool_name": target_tool_name,
                "await_mode": "background",
                "status": "running"
            })),
        ))
    }

    pub(super) async fn persist_wait_tool_call(
        &self,
        tool_call_id: Option<String>,
        internal_call_id: &str,
        args: &str,
    ) -> anyhow::Result<ToolCallHookAction> {
        let (session_id, request_id, parent_deadline_at, _seq) =
            self.ensure_assistant_turn_sequence().await?;
        self.state.lock().await.register_tool_result_identity(
            internal_call_id,
            None,
            tool_call_id.as_deref(),
        );

        let parsed = match serde_json::from_str::<WaitToolArgs>(args) {
            Ok(args) => args,
            Err(error) => {
                return Ok(self.skip_tool_result(
                    WAIT_PROCESS_TOOL_NAME,
                    background_invalid_tool_arguments_payload(
                        WAIT_PROCESS_TOOL_NAME,
                        "/",
                        format!("invalid wait_process arguments: {error}"),
                    ),
                ));
            }
        };
        let background_tool_call_id = parsed.tool_call_id.trim();
        if background_tool_call_id.is_empty() {
            return Ok(self.skip_tool_result(
                WAIT_PROCESS_TOOL_NAME,
                background_invalid_tool_arguments_payload(
                    WAIT_PROCESS_TOOL_NAME,
                    "/tool_call_id",
                    "tool_call_id is required",
                ),
            ));
        }

        let wait_deadline_at = chrono::Utc::now()
            + chrono::Duration::from_std(parsed.validated_wait_timeout())
                .unwrap_or_else(|_| chrono::Duration::seconds(30));
        let caller = ProcessControlScope {
            request_id,
            session_id,
            agent_did: self.agent_did.clone(),
            requester_did: self.active_requester_did().await,
        };
        let result = match self
            .await_background_tool(
                &caller,
                background_tool_call_id,
                parent_deadline_at,
                wait_deadline_at,
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                return Ok(self.skip_tool_result(
                    WAIT_PROCESS_TOOL_NAME,
                    background_invalid_tool_arguments_payload(
                        WAIT_PROCESS_TOOL_NAME,
                        "/tool_call_id",
                        format!("{error:#}"),
                    ),
                ));
            }
        };
        Ok(self.skip_tool_result(WAIT_PROCESS_TOOL_NAME, result))
    }

    pub(super) async fn persist_list_background_tools_tool_call(
        &self,
        tool_call_id: Option<String>,
        internal_call_id: &str,
        args: &str,
    ) -> anyhow::Result<ToolCallHookAction> {
        let (session_id, request_id, _deadline_at, _seq) =
            self.ensure_assistant_turn_sequence().await?;
        self.state.lock().await.register_tool_result_identity(
            internal_call_id,
            None,
            tool_call_id.as_deref(),
        );

        let parsed = match serde_json::from_str::<ListBackgroundToolsArgs>(args) {
            Ok(args) => args,
            Err(error) => {
                return Ok(self.skip_tool_result(
                    LIST_PROCESSES_TOOL_NAME,
                    background_invalid_tool_arguments_payload(
                        LIST_PROCESSES_TOOL_NAME,
                        "/",
                        format!("invalid list_processes arguments: {error}"),
                    ),
                ));
            }
        };
        let caller = ProcessControlScope {
            request_id,
            session_id,
            agent_did: self.agent_did.clone(),
            requester_did: self.active_requester_did().await,
        };
        let response = handle_list_background_tools(
            &self.node,
            &caller,
            &self.agent_did,
            &self.background_live_outputs.registry,
            parsed,
        )
        .await?;
        let result = serde_json::to_value(response).map_err(|error| {
            anyhow::anyhow!("serialize list_background_tools response: {error}")
        })?;
        Ok(self.skip_tool_result(LIST_PROCESSES_TOOL_NAME, json_string(result)))
    }

    pub(super) async fn persist_read_tool_output_tool_call(
        &self,
        tool_call_id: Option<String>,
        internal_call_id: &str,
        args: &str,
    ) -> anyhow::Result<ToolCallHookAction> {
        let (session_id, request_id, _deadline_at, _seq) =
            self.ensure_assistant_turn_sequence().await?;
        self.state.lock().await.register_tool_result_identity(
            internal_call_id,
            None,
            tool_call_id.as_deref(),
        );

        let parsed = match serde_json::from_str::<ReadToolOutputArgs>(args) {
            Ok(args) => args,
            Err(error) => {
                return Ok(self.skip_tool_result(
                    READ_PROCESS_TOOL_NAME,
                    background_invalid_tool_arguments_payload(
                        READ_PROCESS_TOOL_NAME,
                        "/",
                        format!("invalid read_process arguments: {error}"),
                    ),
                ));
            }
        };
        let background_tool_call_id = parsed.tool_call_id.trim().to_string();
        if background_tool_call_id.is_empty() {
            return Ok(self.skip_tool_result(
                READ_PROCESS_TOOL_NAME,
                background_invalid_tool_arguments_payload(
                    READ_PROCESS_TOOL_NAME,
                    "/tool_call_id",
                    "tool_call_id is required",
                ),
            ));
        }

        let caller = ProcessControlScope {
            request_id,
            session_id,
            agent_did: self.agent_did.clone(),
            requester_did: self.active_requester_did().await,
        };
        match handle_read_tool_output(
            &self.node,
            &caller,
            &self.background_live_outputs.registry,
            parsed,
        )
        .await?
        {
            ReadToolOutputOutcome::Found(response) => {
                let result = serde_json::to_value(response).map_err(|error| {
                    anyhow::anyhow!("serialize read_tool_output response: {error}")
                })?;
                Ok(self.skip_tool_result(READ_PROCESS_TOOL_NAME, json_string(result)))
            }
            ReadToolOutputOutcome::NotBackgrounded => Ok(self.skip_tool_result(
                READ_PROCESS_TOOL_NAME,
                background_invalid_tool_arguments_payload(
                    READ_PROCESS_TOOL_NAME,
                    "/tool_call_id",
                    "tool_call_id must identify an ordinary backgrounded tool call",
                ),
            )),
            ReadToolOutputOutcome::NotAuthorized => Ok(self.skip_tool_result(
                READ_PROCESS_TOOL_NAME,
                background_tool_not_allowed_payload(
                    READ_PROCESS_TOOL_NAME,
                    "/tool_call_id",
                    &background_tool_call_id,
                    "background tool call is not manageable by this session principal",
                    Vec::new(),
                ),
            )),
        }
    }

    pub(super) async fn persist_cancel_tool_call(
        &self,
        tool_call_id: Option<String>,
        internal_call_id: &str,
        args: &str,
    ) -> anyhow::Result<ToolCallHookAction> {
        let (session_id, request_id, _deadline_at, _seq) =
            self.ensure_assistant_turn_sequence().await?;
        self.state.lock().await.register_tool_result_identity(
            internal_call_id,
            None,
            tool_call_id.as_deref(),
        );

        let parsed = match serde_json::from_str::<CancelToolArgs>(args) {
            Ok(args) => args,
            Err(error) => {
                return Ok(self.skip_tool_result(
                    CANCEL_PROCESS_TOOL_NAME,
                    background_invalid_tool_arguments_payload(
                        CANCEL_PROCESS_TOOL_NAME,
                        "/",
                        format!("invalid cancel_process arguments: {error}"),
                    ),
                ));
            }
        };
        let background_tool_call_id = parsed.tool_call_id.trim();
        if background_tool_call_id.is_empty() {
            return Ok(self.skip_tool_result(
                CANCEL_PROCESS_TOOL_NAME,
                background_invalid_tool_arguments_payload(
                    CANCEL_PROCESS_TOOL_NAME,
                    "/tool_call_id",
                    "tool_call_id is required",
                ),
            ));
        }
        if parsed
            .reason
            .as_deref()
            .is_some_and(|reason| reason.trim().is_empty())
        {
            return Ok(self.skip_tool_result(
                CANCEL_PROCESS_TOOL_NAME,
                background_invalid_tool_arguments_payload(
                    CANCEL_PROCESS_TOOL_NAME,
                    "/reason",
                    "reason must be omitted or non-empty",
                ),
            ));
        }

        let caller = ProcessControlScope {
            request_id: request_id.clone(),
            session_id: session_id.clone(),
            agent_did: self.agent_did.clone(),
            requester_did: self.active_requester_did().await,
        };
        let lifecycle = match self
            .load_authorized_background_tool(&caller, background_tool_call_id)
            .await
        {
            Ok(lifecycle) => lifecycle,
            Err(error) => {
                return Ok(self.skip_tool_result(
                    CANCEL_PROCESS_TOOL_NAME,
                    background_invalid_tool_arguments_payload(
                        CANCEL_PROCESS_TOOL_NAME,
                        "/tool_call_id",
                        format!("{error:#}"),
                    ),
                ));
            }
        };
        if lifecycle.is_terminal() {
            let result = self
                .background_tool_envelope(lifecycle, "explicit_cancel")
                .await?;
            return Ok(self.skip_tool_result(CANCEL_PROCESS_TOOL_NAME, result));
        }

        let notification_tool_name = lifecycle.tool_name().to_string();
        let notification_request_id = lifecycle.request_id().to_string();
        let notification_reason = parsed
            .reason
            .as_deref()
            .map(str::trim)
            .unwrap_or("explicit_cancel");
        let (lifecycle, won_terminal_compare) = self
            .cancel_background_tool_lifecycle(
                lifecycle,
                CancelCause::UserCancelled,
                notification_reason,
            )
            .await?;
        if !won_terminal_compare {
            let result = self
                .background_tool_envelope(lifecycle, "terminal_compare_lost")
                .await?;
            return Ok(self.skip_tool_result(CANCEL_PROCESS_TOOL_NAME, result));
        }
        if let Err(error) = crate::background_completion::append_background_tool_completion(
            self.node.as_ref(),
            &session_id,
            &notification_request_id,
            background_tool_call_id,
            &notification_tool_name,
            "cancelled",
            "",
            Some(notification_reason),
        )
        .await
        {
            tracing::warn!(
                tool_call_id = %background_tool_call_id,
                error = %error,
                "failed to append explicitly cancelled background tool notification"
            );
        }
        Ok(self.skip_tool_result(
            CANCEL_PROCESS_TOOL_NAME,
            json_string(json!({
                "ok": true,
                "tool_call_id": background_tool_call_id,
                "status": "cancelled"
            })),
        ))
    }
}

fn background_timeout_terminal() -> ChildTerminal {
    ChildTerminal::Failed {
        reason: "background tool deadline exceeded".to_string(),
        failure_class: FailureClass::External,
    }
}

const BACKGROUND_TIMEOUT_COMPLETION_REASON: &str = "deadline_exceeded";

fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

#[cfg(test)]
mod ownership_projection_tests {
    use super::{
        background_timeout_terminal, project_background_completion_if_owned,
        BACKGROUND_TIMEOUT_COMPLETION_REASON,
    };
    use crate::tool_call_lifecycle::{ChildTerminal, FailureClass};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    #[tokio::test]
    async fn terminal_compare_loser_does_not_project_completion() {
        let projected = Arc::new(AtomicBool::new(false));
        let observed = projected.clone();

        let output = project_background_completion_if_owned(false, async move {
            observed.store(true, Ordering::SeqCst);
            Ok::<(), anyhow::Error>(())
        })
        .await;

        assert!(output.is_none());
        assert!(!projected.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn terminal_compare_winner_projects_completion_once() {
        let projected = Arc::new(AtomicBool::new(false));
        let observed = projected.clone();

        let output = project_background_completion_if_owned(true, async move {
            assert!(!observed.swap(true, Ordering::SeqCst));
            Ok::<(), anyhow::Error>(())
        })
        .await;

        assert!(matches!(output, Some(Ok(()))));
        assert!(projected.load(Ordering::SeqCst));
    }

    #[test]
    fn background_timeout_preserves_failure_metadata() {
        assert_eq!(BACKGROUND_TIMEOUT_COMPLETION_REASON, "deadline_exceeded");
        let terminal = background_timeout_terminal();
        assert_eq!(
            terminal,
            ChildTerminal::Failed {
                reason: "background tool deadline exceeded".to_string(),
                failure_class: FailureClass::External,
            }
        );
    }
}
