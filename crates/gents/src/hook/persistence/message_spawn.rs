use super::*;

impl DefraSessionHook {
    /// Persist the assistant tool-call envelope before inline execution can
    /// block. Background completion is appended by an independent task that
    /// allocates from durable transcript state; without this row it can reuse
    /// the hook's in-memory assistant sequence and the later upsert overwrites
    /// the notification (#945).
    ///
    /// Repeated snapshots in one assistant turn update the same row so
    /// providers that emit several tool calls still materialize one assistant
    /// message containing the accumulated calls.
    pub(crate) async fn persist_inflight_assistant_turn(
        &self,
        message: &Message,
    ) -> anyhow::Result<u32> {
        if !matches!(message, Message::Assistant { .. }) {
            anyhow::bail!("in-flight assistant persistence requires an assistant message");
        }

        let content = serde_json::to_string(message)?;
        let reasoning = gents_protocol::transcript::extract_message_reasoning(message);
        let (
            session_id,
            building_sequence,
            current_request_id,
            current_request_doc_id,
            current_requester_did,
        ) = {
            let state = self.state.lock().await;
            let session_id = state
                .session_id
                .clone()
                .ok_or_else(|| anyhow::anyhow!("session hook missing session id"))?;
            let building_sequence = match state.transcript_turn {
                TranscriptTurnState::AssistantBuilding { sequence } => Some(sequence),
                TranscriptTurnState::Idle | TranscriptTurnState::AssistantPersisted { .. } => None,
            };
            (
                session_id,
                building_sequence,
                state.current_request_id.clone(),
                state.current_request_doc_id.clone(),
                state.current_requester_did.clone(),
            )
        };

        let sequence = match building_sequence {
            Some(sequence) => {
                session::save_message_with_requester_did(
                    &self.node,
                    &session_id,
                    &self.agent_did,
                    current_requester_did.as_deref(),
                    sequence,
                    "assistant",
                    &content,
                    reasoning.as_deref(),
                    current_request_id.as_deref(),
                    current_request_doc_id.as_deref(),
                )
                .await?;
                sequence
            }
            None => {
                session::append_message_with_requester_did(
                    &self.node,
                    &session_id,
                    &self.agent_did,
                    current_requester_did.as_deref(),
                    "assistant",
                    &content,
                    reasoning.as_deref(),
                    current_request_id.as_deref(),
                    current_request_doc_id.as_deref(),
                )
                .await?
            }
        };

        let mut state = self.state.lock().await;
        if state.session_id.as_deref() == Some(session_id.as_str()) {
            state.sequence = state.sequence.max(sequence);
            state.transcript_turn = TranscriptTurnState::AssistantBuilding { sequence };
        }
        Ok(sequence)
    }

    pub async fn persist_message(&self, message: &Message) -> anyhow::Result<u32> {
        let content = serde_json::to_string(message)?;
        let tool_result_aliases = persisted_tool_result_message_aliases(message);
        // #492: durably persist the assistant turn's chain-of-thought reasoning
        // into `AgentMessage.reasoning` at materialize time. The reasoning is
        // already embedded in the serialized `content` blob, but we extract a
        // readable copy into the dedicated field so a post-finalize reader (our
        // offline harvest) can recover it even though the live
        // `AgentResponse.reasoning` tail is cleared on finalize (#64). Only
        // assistant messages carry reasoning; users/tool-results yield `None`.
        let reasoning = gents_protocol::transcript::extract_message_reasoning(message);
        let reasoning = reasoning.as_deref();
        let (
            session_id,
            turn_state,
            message_key,
            existing_sequence,
            current_request_id,
            current_request_doc_id,
            current_requester_did,
            preferred_sequence,
        ) = {
            let state = self.state.lock().await;
            let session_id = state
                .session_id
                .clone()
                .ok_or_else(|| anyhow::anyhow!("session hook missing session id"))?;
            let message_key = tool_result_message_key(&session_id, message)?;
            let existing_sequence = message_key
                .as_ref()
                .and_then(|key| state.persisted_tool_result_message_sequences.get(key))
                .or_else(|| {
                    tool_result_aliases
                        .iter()
                        .find_map(|alias| state.persisted_tool_result_message_sequences.get(alias))
                })
                .copied();
            (
                session_id,
                state.transcript_turn,
                message_key,
                existing_sequence,
                state.current_request_id.clone(),
                state.current_request_doc_id.clone(),
                state.current_requester_did.clone(),
                state.sequence.checked_add(1),
            )
        };

        if let Some(sequence) = existing_sequence {
            return Ok(sequence);
        }

        // #497: durable request-scoped dedup for the turn-1 user prompt and the
        // per-request `<context>` message. Tool-result user messages already
        // dedup by `message_key` above; these don't. The daemon retry loop builds
        // a fresh hook per attempt, so a transient failure before the first
        // assistant token would otherwise re-persist them. If this request
        // already persisted an identical message, reuse its sequence.
        if message_key.is_none() && matches!(message, Message::User { .. }) {
            if let Some(request_id) = current_request_id.as_deref() {
                if let Some(sequence) = session::message_sequence_for_request_content(
                    &self.node,
                    &session_id,
                    request_id,
                    &content,
                )
                .await?
                {
                    return Ok(sequence);
                }
            }
        }

        // Tool-result rows have a stable logical key, but their sequence must
        // still be allocated from the durable transcript. An in-memory
        // `state.sequence += 1` can race an independently appended background
        // notification and create duplicate sequence numbers.
        if let Some(message_key) = message_key.as_ref() {
            if !matches!(message, Message::User { .. }) {
                anyhow::bail!("only user tool-result messages may carry a message key");
            }
            let (sequence, _) = session::append_message_once_with_key_and_requester_did(
                &self.node,
                &session_id,
                &self.agent_did,
                current_requester_did.as_deref(),
                "user",
                &content,
                reasoning,
                current_request_id.as_deref(),
                current_request_doc_id.as_deref(),
                message_key,
                preferred_sequence,
            )
            .await?;
            let mut state = self.state.lock().await;
            if state.session_id.as_deref() == Some(session_id.as_str()) {
                state.sequence = state.sequence.max(sequence);
                state
                    .persisted_tool_result_message_sequences
                    .insert(message_key.clone(), sequence);
                for alias in &tool_result_aliases {
                    state
                        .persisted_tool_result_message_sequences
                        .insert(alias.clone(), sequence);
                }
            }
            return Ok(sequence);
        }

        let append_unreserved = matches!(turn_state, TranscriptTurnState::Idle)
            || (matches!(message, Message::Assistant { .. })
                && matches!(turn_state, TranscriptTurnState::AssistantPersisted { .. }));
        if append_unreserved {
            let role = match message {
                Message::User { .. } => "user",
                Message::Assistant { .. } => "assistant",
                Message::System { .. } => {
                    anyhow::bail!("system messages are not persisted in session history");
                }
            };
            let sequence = session::append_message_with_requester_did(
                &self.node,
                &session_id,
                &self.agent_did,
                current_requester_did.as_deref(),
                role,
                &content,
                reasoning,
                current_request_id.as_deref(),
                current_request_doc_id.as_deref(),
            )
            .await?;
            let mut state = self.state.lock().await;
            if state.session_id.as_deref() == Some(session_id.as_str()) {
                state.sequence = state.sequence.max(sequence);
                match message {
                    Message::User { .. } => state.reset_after_user_message(),
                    Message::Assistant { .. } => {
                        state.transcript_turn =
                            TranscriptTurnState::AssistantPersisted { sequence };
                    }
                    Message::System { .. } => {}
                }
            }
            return Ok(sequence);
        }

        let (session_id, sequence, role) = {
            let mut state = self.state.lock().await;
            let session_id = state
                .session_id
                .clone()
                .ok_or_else(|| anyhow::anyhow!("session hook missing session id"))?;

            match message {
                Message::User { .. } => {
                    state.sequence += 1;
                    let sequence = state.sequence;
                    state.reset_after_user_message();
                    (session_id, sequence, "user")
                }
                Message::Assistant { .. } => {
                    let sequence = state.persist_assistant_turn();
                    (session_id, sequence, "assistant")
                }
                Message::System { .. } => {
                    anyhow::bail!("system messages are not persisted in session history");
                }
            }
        };

        session::save_message_with_requester_did(
            &self.node,
            &session_id,
            &self.agent_did,
            current_requester_did.as_deref(),
            sequence,
            role,
            &content,
            reasoning,
            current_request_id.as_deref(),
            current_request_doc_id.as_deref(),
        )
        .await?;
        Ok(sequence)
    }

    pub async fn persist_stream_tool_result_message(
        &self,
        tool_result: &ToolResult,
        internal_call_id: &str,
    ) -> anyhow::Result<()> {
        let session_id = self
            .state
            .lock()
            .await
            .session_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("session hook missing session id"))?;

        let should_persist = {
            let mut state = self.state.lock().await;
            state.mark_stream_tool_result_seen(
                internal_call_id,
                &tool_result.id,
                tool_result.call_id.as_deref(),
            )?
        };
        if !should_persist {
            return Ok(());
        }

        let raw_stream_result = render_tool_result_text(tool_result);
        let prefer_stream_payload = is_subagent_tool_result_payload(&raw_stream_result);
        let (tool_name, stored_result) =
            match load_stored_tool_call_result(&self.node, &session_id, internal_call_id).await {
                Ok(stored) if !stored.result.is_empty() && !prefer_stream_payload => {
                    (stored.tool_name, stored.result)
                }
                Ok(stored) => {
                    let (text, _, _) = truncate_text(
                        &raw_stream_result,
                        TruncationMode::Head,
                        &self.truncation_limits,
                    );
                    (stored.tool_name, text)
                }
                Err(e) => {
                    if is_missing_tool_call_result(&e) {
                        tracing::debug!(
                            error = %e,
                            tool_call_id = %internal_call_id,
                            "stored tool result not found, falling back to stream payload"
                        );
                    } else {
                        tracing::warn!(
                            error = %e,
                            tool_call_id = %internal_call_id,
                            "failed to load stored tool result, falling back to stream payload"
                        );
                    }
                    let (text, _, _) = truncate_text(
                        &raw_stream_result,
                        TruncationMode::Head,
                        &self.truncation_limits,
                    );
                    ("unknown".to_string(), text)
                }
            };
        let model_observation = model_observation_for_tool_result(&tool_name, &stored_result);

        let persisted_result = ToolResult {
            id: tool_result.id.clone(),
            call_id: tool_result.call_id.clone(),
            content: vec![ToolResultContent::Text(Text {
                text: model_observation,
            })],
        };

        let message = Message::User {
            content: vec![UserContent::ToolResult(persisted_result)],
        };
        self.persist_message(&message).await?;
        Ok(())
    }

    /// Reconcile completed-but-unmessaged tool calls for the active request so
    /// the persisted transcript stays pair-closed on the abort path (#442).
    ///
    /// The owned loop runs each tool inline: `on_tool_result` marks the
    /// `AgentToolCall` row `.completed` (recording its result) before the
    /// result MESSAGE is yielded, which `StreamProcessor` persists only when it
    /// observes the streamed `ToolResult`. On a provider stall that streamed
    /// item never arrives, so a liveness/interrupt abort persists the assistant
    /// turn (via `persist_partial_turn`) but no result message — leaving a
    /// `completed` tool call with no paired result, violating
    /// `Transcript.CompletedToolCallsPaired`. This replays the existing streamed
    /// result-message persistence for each completed tool call (which loads the
    /// recorded result from the row and dedupes), restoring pairing. It is the
    /// `complete_tool_with_result` transition applied late.
    ///
    /// Must run after the assistant turn is persisted (so the message-sequence
    /// gate is satisfied); a no-op otherwise. Idempotent via tool-result dedup.
    pub(crate) async fn backfill_completed_tool_results(&self) -> anyhow::Result<usize> {
        let (session_id, request_id) = {
            let state = self.state.lock().await;
            if !state.assistant_turn_persisted() {
                return Ok(0);
            }
            match (state.session_id.clone(), state.current_request_id.clone()) {
                (Some(session_id), Some(request_id)) if !request_id.is_empty() => {
                    (session_id, request_id)
                }
                _ => return Ok(0),
            }
        };
        // `session_id` is required by the streamed-result path below; bind it to
        // keep the query and that call reading from the same active session.
        let _ = &session_id;

        let escaped_request_id = crate::graphql::escape_graphql_string(&request_id);
        let query = format!(
            r#"{{
                AgentToolCall(
                    filter: {{
                        request_id: {{ _eq: "{escaped_request_id}" }},
                        lifecycle_state: {{ _eq: "completed" }}
                    }},
                    order: {{ message_sequence: ASC }}
                ) {{ tool_call_id result }}
            }}"#
        );
        let response = self.node.execute(&query).await;
        if response.has_errors() {
            anyhow::bail!(
                "backfill_completed_tool_results query failed for request_id={}: {:?}",
                request_id,
                response.errors
            );
        }

        let rows = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentToolCall"))
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();

        let mut reconciled = 0usize;
        for row in rows {
            let internal_call_id = row
                .get("tool_call_id")
                .and_then(|value| value.as_str())
                .unwrap_or_default();
            let result = row
                .get("result")
                .and_then(|value| value.as_str())
                .unwrap_or_default();
            if internal_call_id.is_empty() || result.is_empty() {
                continue;
            }

            // Resolve the result-message identity the stream path would have used.
            let (result_id, call_id) = {
                let state = self.state.lock().await;
                state.tool_result_message_identity(internal_call_id, None)
            };

            // Replay the streamed result-message persistence: it loads the
            // recorded result from the row (so the empty content here is
            // replaced) and dedupes, so an already-paired call is a no-op.
            let tool_result = ToolResult {
                id: result_id,
                call_id,
                content: vec![ToolResultContent::Text(Text {
                    text: String::new(),
                })],
            };
            self.persist_stream_tool_result_message(&tool_result, internal_call_id)
                .await?;
            reconciled += 1;
        }

        Ok(reconciled)
    }

    pub(super) async fn ensure_assistant_turn_sequence(
        &self,
    ) -> anyhow::Result<(String, String, chrono::DateTime<chrono::Utc>, u32)> {
        let mut state = self.state.lock().await;
        let session_id = state
            .session_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("session hook missing session id"))?;
        let request_id = state.current_request_id.clone().unwrap_or_else(|| {
            tracing::warn!(
                "tool call has no active request id; persisting with empty request link"
            );
            String::new()
        });
        let deadline_at = state.request_deadline_at.unwrap_or_else(|| {
            tracing::warn!(
                "tool call has no active request deadline; using default lifecycle deadline"
            );
            chrono::Utc::now() + chrono::Duration::seconds(DEFAULT_DEADLINE_DURATION_SECS as i64)
        });

        let sequence = state.begin_or_continue_assistant_turn();

        Ok((session_id, request_id, deadline_at, sequence))
    }

    pub(super) async fn persist_spawn_subagent_tool_call(
        &self,
        tool_call_id: Option<String>,
        internal_call_id: &str,
        args: &str,
    ) -> anyhow::Result<ToolCallHookAction> {
        let (session_id, request_id, hook_deadline_at, seq) =
            self.ensure_assistant_turn_sequence().await?;
        self.state.lock().await.register_tool_result_identity(
            internal_call_id,
            None,
            tool_call_id.as_deref(),
        );

        let parsed = match serde_json::from_str::<SpawnSubagentArgs>(args) {
            Ok(args) => args,
            Err(error) => {
                return self
                    .fail_spawn_subagent_tool_call(
                        session_id,
                        request_id,
                        hook_deadline_at,
                        seq,
                        internal_call_id,
                        args,
                        FailureClass::ArgumentInvalid,
                        invalid_tool_arguments_payload(
                            SPAWN_SUBAGENT_TOOL_NAME,
                            "/",
                            format!("invalid spawn_subagent arguments: {error}"),
                        ),
                    )
                    .await;
            }
        };

        let parent_context = load_parent_subagent_context(&self.node, &request_id).await?;
        if parsed.name.trim().is_empty() {
            return self
                .fail_spawn_subagent_tool_call(
                    session_id,
                    request_id,
                    parent_context.request_deadline_at,
                    seq,
                    internal_call_id,
                    args,
                    FailureClass::ArgumentInvalid,
                    invalid_tool_arguments_payload(
                        SPAWN_SUBAGENT_TOOL_NAME,
                        "/name",
                        "name is required",
                    ),
                )
                .await;
        }
        if parsed.prompt.trim().is_empty() {
            return self
                .fail_spawn_subagent_tool_call(
                    session_id,
                    request_id,
                    parent_context.request_deadline_at,
                    seq,
                    internal_call_id,
                    args,
                    FailureClass::ArgumentInvalid,
                    invalid_tool_arguments_payload(
                        SPAWN_SUBAGENT_TOOL_NAME,
                        "/prompt",
                        "prompt is required",
                    ),
                )
                .await;
        }
        if !parent_context.subagent_spawn_enabled {
            return self
                .fail_spawn_subagent_tool_call(
                    session_id,
                    request_id,
                    parent_context.request_deadline_at,
                    seq,
                    internal_call_id,
                    args,
                    FailureClass::ServiceUnavailable,
                    tool_not_allowed_payload(
                        SPAWN_SUBAGENT_TOOL_NAME,
                        "/",
                        SPAWN_SUBAGENT_TOOL_NAME,
                        "subagent spawning is not enabled for this behavior",
                        context_allowed_target_names(&parent_context),
                    ),
                )
                .await;
        }
        let name = parsed.name.trim();
        let Some(target) = resolve_context_target(&parent_context, name).cloned() else {
            return self
                .fail_spawn_subagent_tool_call(
                    session_id,
                    request_id,
                    parent_context.request_deadline_at,
                    seq,
                    internal_call_id,
                    args,
                    FailureClass::ServiceUnavailable,
                    tool_not_allowed_payload(
                        SPAWN_SUBAGENT_TOOL_NAME,
                        "/name",
                        name,
                        format!("'{name}' is not an allowed subagent target for this behavior"),
                        context_allowed_target_names(&parent_context),
                    ),
                )
                .await;
        };
        let behavior_id = target.behavior_id.as_str();

        let await_mode = parsed
            .await_mode
            .map(|mode| mode.as_await_mode())
            .unwrap_or(parent_context.subagent_default_await_mode);
        let target_host = self.subagent_target_host(&target);
        // Cross-deployment (remote-DID) subagent delegation is deferred behind a
        // default-OFF flag (#377). When the parent behavior has not opted in,
        // reject ANY remote spawn (both await modes). Remote targets should not
        // even be surfaced to the model in this case (see tool_surface), so a
        // remote spawn here means a stale/forged target name.
        if target_host == SubagentTargetHost::Remote
            && !parent_context.subagent_allow_cross_deployment
        {
            return self
                .fail_spawn_subagent_tool_call(
                    session_id,
                    request_id,
                    parent_context.request_deadline_at,
                    seq,
                    internal_call_id,
                    args,
                    FailureClass::ServiceUnavailable,
                    tool_not_allowed_payload(
                        SPAWN_SUBAGENT_TOOL_NAME,
                        "/name",
                        name,
                        "cross-deployment subagent delegation is not enabled",
                        context_allowed_target_names(&parent_context),
                    ),
                )
                .await;
        }
        if target_host == SubagentTargetHost::Remote && await_mode == AwaitMode::Foreground {
            return self
                .fail_spawn_subagent_tool_call(
                    session_id,
                    request_id,
                    parent_context.request_deadline_at,
                    seq,
                    internal_call_id,
                    args,
                    FailureClass::ArgumentInvalid,
                    invalid_tool_arguments_payload(
                        SPAWN_SUBAGENT_TOOL_NAME,
                        "/await_mode",
                        "foreground cross-deployment subagents are not supported; use await_mode=background",
                    ),
                )
                .await;
        }
        if await_mode == AwaitMode::Background && !parent_context.subagent_background_enabled {
            return self
                .fail_spawn_subagent_tool_call(
                    session_id,
                    request_id,
                    parent_context.request_deadline_at,
                    seq,
                    internal_call_id,
                    args,
                    FailureClass::ServiceUnavailable,
                    tool_not_allowed_payload(
                        SPAWN_SUBAGENT_TOOL_NAME,
                        "/await_mode",
                        "background",
                        "background subagent spawning is not enabled for this behavior",
                        context_allowed_target_names(&parent_context),
                    ),
                )
                .await;
        }

        // Fail-safe for local targets whose behavior was deleted mid-session
        // (#377). If the resolved target is LOCAL (same agent DID) but its
        // behavior no longer exists in the DB, writing a child AgentRequest
        // would produce an orphan that can never be claimed. Reject cleanly
        // with a service_unavailable payload instead of writing the orphan.
        if target_host == SubagentTargetHost::Local {
            match load_agent_behavior(&self.node, behavior_id).await {
                Ok(None) => {
                    return self
                        .fail_spawn_subagent_tool_call(
                            session_id,
                            request_id,
                            parent_context.request_deadline_at,
                            seq,
                            internal_call_id,
                            args,
                            FailureClass::ServiceUnavailable,
                            tool_not_allowed_payload(
                                SPAWN_SUBAGENT_TOOL_NAME,
                                "/name",
                                name,
                                format!(
                                    "subagent target '{name}' refers to behavior '{behavior_id}' \
                                     which no longer exists; the target may have been removed \
                                     after this session started"
                                ),
                                context_allowed_target_names(&parent_context),
                            ),
                        )
                        .await;
                }
                Ok(Some(_)) => {}
                Err(error) => {
                    tracing::warn!(
                        behavior_id = %behavior_id,
                        %error,
                        "spawn guard: failed to verify local target behavior existence; \
                         proceeding with spawn"
                    );
                }
            }
        }

        if let Some(child_deadline) = parsed.deadline.as_ref() {
            if *child_deadline <= chrono::Utc::now() {
                return self
                    .fail_spawn_subagent_tool_call(
                        session_id,
                        request_id,
                        parent_context.request_deadline_at,
                        seq,
                        internal_call_id,
                        args,
                        FailureClass::ArgumentInvalid,
                        invalid_tool_arguments_payload(
                            SPAWN_SUBAGENT_TOOL_NAME,
                            "/deadline",
                            "deadline must be in the future",
                        ),
                    )
                    .await;
            }
            if *child_deadline > parent_context.request_deadline_at {
                return self
                    .fail_spawn_subagent_tool_call(
                        session_id,
                        request_id,
                        parent_context.request_deadline_at,
                        seq,
                        internal_call_id,
                        args,
                        FailureClass::ArgumentInvalid,
                        invalid_tool_arguments_payload(
                            SPAWN_SUBAGENT_TOOL_NAME,
                            "/deadline",
                            "deadline must be at or before the parent request deadline",
                        ),
                    )
                    .await;
            }
        }

        if parent_context.subagent_depth >= MAX_SUBAGENT_DEPTH {
            return self
                .fail_spawn_subagent_tool_call(
                    session_id,
                    request_id,
                    parent_context.request_deadline_at,
                    seq,
                    internal_call_id,
                    args,
                    FailureClass::ArgumentInvalid,
                    depth_exceeded_payload(parent_context.subagent_depth),
                )
                .await;
        }

        let parent_workspace = ParentWorkspaceStamp::from_fields(
            parent_context.workspace_id.as_deref(),
            parent_context.workspace_authority.as_deref(),
            parent_context.workspace_owner_deployment_id.as_deref(),
            parent_context.workspace_seal_hash.as_deref(),
        );
        let resolved_workspace = match resolve_spawn_workspace(
            &self.node,
            &parent_workspace,
            parsed.workspace.as_ref(),
            &self.agent_did,
            internal_call_id,
            &request_id,
        )
        .await
        {
            Ok(lineage) => lineage,
            Err(SpawnWorkspaceError { class, message }) => {
                let payload = match class {
                    FailureClass::ArgumentInvalid => invalid_tool_arguments_payload(
                        SPAWN_SUBAGENT_TOOL_NAME,
                        "/workspace",
                        message,
                    ),
                    _ => service_unavailable_payload(
                        SPAWN_SUBAGENT_TOOL_NAME,
                        "/workspace",
                        message,
                        false,
                    ),
                };
                return self
                    .fail_spawn_subagent_tool_call(
                        session_id,
                        request_id,
                        parent_context.request_deadline_at,
                        seq,
                        internal_call_id,
                        args,
                        class,
                        payload,
                    )
                    .await;
            }
        };

        // Persist a normalized bridge args payload that carries the RESOLVED
        // target `(agent_did, behavior_id)` alongside the model-facing `name`.
        // `SubagentSource` reads these resolved fields directly. For a remote
        // target, the targeted bridge reaches that host and the host writes the
        // child `AgentRequest` with its local DID + behavior id. The claiming
        // deployment never needs to re-resolve the friendly name (it has no
        // access to the parent's target table), which is what removes the
        // resolution seam.
        let target_agent_did = target.agent_did.clone();
        let mut bridge_args = serde_json::json!({
            "name": name,
            "agent_did": target_agent_did.clone(),
            "behavior_id": target.behavior_id,
            "prompt": parsed.prompt,
            "deadline": parsed.deadline,
            "parent_subagent_depth": parent_context.subagent_depth,
        });
        if let Some(workspace) = resolved_workspace.as_ref() {
            if target_host == SubagentTargetHost::Remote {
                return self
                    .fail_spawn_subagent_tool_call(
                        session_id,
                        request_id,
                        parent_context.request_deadline_at,
                        seq,
                        internal_call_id,
                        args,
                        FailureClass::ServiceUnavailable,
                        service_unavailable_payload(
                            SPAWN_SUBAGENT_TOOL_NAME,
                            "/workspace",
                            "workspace-bound spawn cannot target a remote host until the child can be materialized on the workspace owner deployment",
                            false,
                        ),
                    )
                    .await;
            }
            merge_workspace_lineage(&mut bridge_args, workspace);
        }
        let bridge_args = bridge_args.to_string();

        let child_request_id = uuid::Uuid::new_v4().to_string();
        let mut lifecycle = ToolCallLifecycle::new_subagent(
            self.node.clone(),
            request_id.clone(),
            session_id.clone(),
            self.agent_did.clone(),
            internal_call_id.to_string(),
            seq,
            SPAWN_SUBAGENT_TOOL_NAME.to_string(),
            bridge_args,
            parent_context.request_deadline_at,
            await_mode,
            CancelPolicy::Cascade,
            child_request_id.clone(),
            target_agent_did,
        )
        .with_requester_did(self.active_requester_did().await)
        .with_request_doc_id(self.active_request_doc_id().await);
        if await_mode == AwaitMode::Background {
            let timeout_secs =
                effective_context_cross_deployment_spawn_timeout_seconds(&parent_context);
            lifecycle.set_unclaimed_deadline_at(Some(
                chrono::Utc::now() + chrono::Duration::seconds(timeout_secs),
            ));
        }
        lifecycle.start_running().await?;

        // Spawn convergence (#377): both same-deployment (local) and
        // cross-deployment (remote) spawns now follow ONE path — write the
        // `AgentToolCall` bridge (done by `start_running()` above) and let
        // `SubagentSource` create the child `AgentRequest`. `SubagentSource`
        // dedups via `child_request_exists`, so there is exactly one creator
        // regardless of locality. The hook no longer synchronously creates the
        // child, so the background receipt does not yet carry the child session
        // id (the claiming deployment assigns it when it materializes the
        // child); foreground waits adopt the session id from the edge once
        // `SubagentSource` has materialized the child.
        self.in_flight_lifecycles
            .lock()
            .await
            .insert(internal_call_id.to_string(), lifecycle);

        if await_mode == AwaitMode::Background {
            let receipt = background_receipt_payload(&child_request_id, None, behavior_id);
            return Ok(self.skip_tool_result(SPAWN_SUBAGENT_TOOL_NAME, receipt));
        }

        // Foreground spawns are local-only (the remote-foreground case is
        // rejected above). Block until `SubagentSource` materializes the child
        // and the bridge reaches a terminal state.
        let result = self
            .await_foreground_subagent(
                internal_call_id,
                &parent_context,
                &child_request_id,
                "",
                behavior_id,
                parent_context.request_deadline_at,
            )
            .await?;

        Ok(self.skip_tool_result(SPAWN_SUBAGENT_TOOL_NAME, result))
    }

    /// Classify a resolved target as local or remote by comparing the target's
    /// `agent_did` to this deployment's own DID. No behavior DB lookup is
    /// needed: the target carries the owning agent's DID directly, which is
    /// also what removes the cross-node resolution seam.
    pub(super) fn subagent_target_host(&self, target: &SubagentTarget) -> SubagentTargetHost {
        if target.agent_did == self.agent_did {
            SubagentTargetHost::Local
        } else {
            SubagentTargetHost::Remote
        }
    }
}

fn persisted_tool_result_message_aliases(message: &Message) -> Vec<String> {
    let Message::User { content } = message else {
        return Vec::new();
    };
    if content.len() != 1 {
        return Vec::new();
    }
    let Some(UserContent::ToolResult(tool_result)) = content.first() else {
        return Vec::new();
    };

    let mut aliases = Vec::new();
    if !tool_result.id.is_empty() {
        aliases.push(format!("result:{}", tool_result.id));
    }
    if let Some(call_id) = tool_result.call_id.as_deref().filter(|id| !id.is_empty()) {
        let alias = format!("call:{call_id}");
        if !aliases.contains(&alias) {
            aliases.push(alias);
        }
    }
    aliases
}

fn is_missing_tool_call_result(error: &anyhow::Error) -> bool {
    format!("{error:#}").contains("no AgentToolCall")
}
