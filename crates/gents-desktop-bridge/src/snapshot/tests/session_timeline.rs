use super::*;

fn assistant_message_json(text: &str) -> String {
    serde_json::to_string(&Message::assistant(text)).expect("serialize assistant")
}

fn make_streaming_store_with_response_content(content: &str) -> ClientStore {
    ClientStore::from_rows(ClientStoreRows {
        conversations: vec![AgentConversationRow {
            session_id: "sess-1".to_string(),
            agent_name: Some("Amy".to_string()),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            title: Some("conversation".to_string()),
            title_source: Some("generated".to_string()),
            preview_text: Some("hello".to_string()),
            status: Some("active".to_string()),
            created_at: Some("2026-04-21T12:00:00Z".to_string()),
            updated_at: Some("2026-04-21T12:01:00Z".to_string()),
            latest_request_id: Some("req-1".to_string()),
        }],
        requests: vec![AgentRequestRow {
            request_id: "req-1".to_string(),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            session_id: Some("sess-1".to_string()),
            retry_parent_request: None,
            retry_root_request: None,
            superseded_by_request: None,
            content: Some("hello".to_string()),
            temperature: None,
            top_p: None,
            top_k: None,
            seed: None,
            max_tokens: None,
            metadata: None,
            status: Some("processing".to_string()),
            lifecycle_state: Some("processing".to_string()),
            backend_id: None,
            execution_origin: Some("interactive".to_string()),
            failure_reason: None,
            terminalized_at: None,
            terminal_redrive_attempts: None,
            created_at: Some("2026-04-21T12:00:00Z".to_string()),
            claimed_at: None,
            deadline: None,
            retry_count: Some(0),
            max_retries: Some(3),
            caused_by_trigger_id: None,
            caused_by_trigger_kind: None,
            caused_by_parent_request_id: None,
            interrupt_requested_at: None,
            valid_until: None,
        }],
        messages: vec![AgentMessageRow {
            message_key: "msg-1".to_string(),
            session_id: Some("sess-1".to_string()),
            request_id: None,
            requester_did: None,
            sequence: Some(1),
            role: Some("user".to_string()),
            content: Some(user_message_json("hello")),
            reasoning: None,
            timestamp: Some("2026-04-21T12:00:00Z".to_string()),
        }],
        responses: vec![AgentResponseRow {
            response_key: "resp-1".to_string(),
            request_id: Some("req-1".to_string()),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            session_id: Some("sess-1".to_string()),
            content: Some(content.to_string()),
            reasoning: None,
            status: Some("streaming".to_string()),
            error_message: None,
            token_count: Some(4),
            progress_seq: Some(1),
            reasoning_progress_seq: Some(0),
            materialized_message_sequence: None,
            materialized_at: None,
            created_at: Some("2026-04-21T12:00:01Z".to_string()),
            completed_at: None,
            interrupted_at: None,
        }],
        ..ClientStoreRows::default()
    })
}

#[test]
fn overlay_hidden_when_response_tail_is_empty() {
    let store = make_streaming_store_with_response_content("");
    let snapshot = build_session_snapshot_from_store(&store, "sess-1", None).expect("snapshot");
    let has_live = snapshot
        .timeline_items
        .iter()
        .any(|item| matches!(item, RenderedTimelineItem::LiveAssistant { .. }));
    assert!(!has_live, "overlay must be hidden when tail is empty");
}

#[test]
fn legacy_wake_is_filtered_from_latest_turn_projection() {
    let mut rows = make_streaming_store_with_response_content("").to_rows();
    rows.requests[0].status = Some("completed".to_string());
    rows.requests[0].lifecycle_state = Some("completed".to_string());
    rows.responses.clear();
    let mut wake = rows.requests[0].clone();
    wake.request_id = "legacy-wake".to_string();
    wake.content = Some("legacy wake".to_string());
    wake.status = Some("pending".to_string());
    wake.lifecycle_state = Some("pending".to_string());
    wake.execution_origin = Some("scheduled".to_string());
    wake.metadata = Some(
        r#"{"queue":{"source":"background_completion","policy":"coalesce","key":"child-1","queued_after_request_id":null}}"#
            .to_string(),
    );
    wake.created_at = Some("2026-04-21T12:02:00Z".to_string());
    rows.requests.push(wake);
    rows.conversations[0].latest_request_id = Some("legacy-wake".to_string());

    let store = ClientStore::from_rows(rows);
    let snapshot =
        build_session_snapshot_from_store(&store, "sess-1", Some("legacy-wake")).expect("snapshot");

    assert_eq!(snapshot.latest_request_id.as_deref(), Some("req-1"));
    assert_eq!(snapshot.turn_state.as_deref(), Some("completed"));
    assert!(snapshot.pending_turn.is_none());
}

#[test]
fn session_snapshot_deduplicates_persisted_rows_from_multiple_sources() {
    let mut rows = make_streaming_store_with_response_content("").to_rows();
    rows.responses.clear();

    let mut duplicate_user = rows.messages[0].clone();
    duplicate_user.sequence = Some(9);
    duplicate_user.content = Some(user_message_json("later duplicate"));
    rows.messages.push(duplicate_user);
    rows.message_source_agent_dids = vec![None, Some("did:test:amy".to_string())];

    let assistant = AgentMessageRow {
        message_key: "msg-2".to_string(),
        session_id: Some("sess-1".to_string()),
        request_id: None,
        requester_did: None,
        sequence: Some(2),
        role: Some("assistant".to_string()),
        content: Some(assistant_message_json("hello back")),
        reasoning: None,
        timestamp: Some("2026-04-21T12:00:01Z".to_string()),
    };
    rows.messages.push(assistant.clone());
    rows.message_source_agent_dids.push(None);
    let mut duplicate_assistant = assistant;
    duplicate_assistant.sequence = Some(10);
    duplicate_assistant.content = Some(assistant_message_json("later duplicate"));
    rows.messages.push(duplicate_assistant);
    rows.message_source_agent_dids
        .push(Some("did:test:amy".to_string()));

    let store = ClientStore::from_rows(rows);
    let snapshot = build_session_snapshot_from_store_for_agent(
        &store,
        Some("did:test:amy"),
        "sess-1",
        Some("req-1"),
    )
    .expect("session snapshot");

    let kinds = snapshot
        .timeline_items
        .iter()
        .map(|item| match item {
            RenderedTimelineItem::UserMessage { .. } => "user",
            RenderedTimelineItem::AssistantMessage { .. } => "assistant",
            RenderedTimelineItem::ToolGroup { .. } => "tools",
            RenderedTimelineItem::PendingUserTurn { .. } => "pending",
            RenderedTimelineItem::LiveAssistant { .. } => "live",
        })
        .collect::<Vec<_>>();
    assert_eq!(kinds, vec!["user", "assistant"]);
    assert!(matches!(
        &snapshot.timeline_items[0],
        RenderedTimelineItem::UserMessage {
            sequence: Some(1),
            content,
            ..
        } if content == "hello"
    ));
    assert!(matches!(
        &snapshot.timeline_items[1],
        RenderedTimelineItem::AssistantMessage {
            sequence: Some(2),
            content: Some(content),
            ..
        } if content == "hello back"
    ));
}

#[test]
fn session_snapshot_hides_live_overlay_matching_last_materialized_assistant() {
    let reply = "hello back";
    let mut rows = make_streaming_store_with_response_content(reply).to_rows();
    rows.messages.push(AgentMessageRow {
        message_key: "msg-2".to_string(),
        session_id: Some("sess-1".to_string()),
        request_id: None,
        requester_did: None,
        sequence: Some(2),
        role: Some("assistant".to_string()),
        content: Some(assistant_message_json(reply)),
        reasoning: None,
        timestamp: Some("2026-04-21T12:00:01Z".to_string()),
    });

    let store = ClientStore::from_rows(rows);
    let snapshot = build_session_snapshot_from_store(&store, "sess-1", Some("req-1"))
        .expect("session snapshot");

    let assistant_items = snapshot
        .timeline_items
        .iter()
        .filter(|item| matches!(item, RenderedTimelineItem::AssistantMessage { .. }))
        .count();
    let live_items = snapshot
        .timeline_items
        .iter()
        .filter(|item| matches!(item, RenderedTimelineItem::LiveAssistant { .. }))
        .count();

    assert_eq!(assistant_items, 1);
    assert_eq!(live_items, 0, "matching live overlay must be suppressed");
}

#[test]
fn session_snapshot_places_live_overlay_before_running_orphan_tool_group() {
    let store = ClientStore::from_rows(ClientStoreRows {
        conversations: vec![AgentConversationRow {
            session_id: "session-1".to_string(),
            agent_name: Some("Amy".to_string()),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            title: Some("conversation".to_string()),
            title_source: Some("generated".to_string()),
            preview_text: Some("turn two".to_string()),
            status: Some("active".to_string()),
            created_at: Some("2026-04-21T12:00:00Z".to_string()),
            updated_at: Some("2026-04-21T12:02:00Z".to_string()),
            latest_request_id: Some("req-2".to_string()),
        }],
        requests: vec![
            AgentRequestRow {
                request_id: "req-1".to_string(),
                agent_did: Some("did:test:amy".to_string()),
                requester_did: None,
                behavior_id: Some("amy-default".to_string()),
                session_id: Some("session-1".to_string()),
                retry_parent_request: None,
                retry_root_request: None,
                superseded_by_request: None,
                content: Some("turn one".to_string()),
                temperature: None,
                top_p: None,
                top_k: None,
                seed: None,
                max_tokens: None,
                metadata: None,
                status: Some("completed".to_string()),
                lifecycle_state: Some("completed".to_string()),
                backend_id: None,
                execution_origin: Some("interactive".to_string()),
                failure_reason: None,
                terminalized_at: None,
                terminal_redrive_attempts: None,
                created_at: Some("2026-04-21T12:00:00Z".to_string()),
                claimed_at: None,
                deadline: None,
                retry_count: Some(0),
                max_retries: Some(3),
                caused_by_trigger_id: None,
                caused_by_trigger_kind: None,
                caused_by_parent_request_id: None,
                interrupt_requested_at: None,
                valid_until: None,
            },
            AgentRequestRow {
                request_id: "req-2".to_string(),
                agent_did: Some("did:test:amy".to_string()),
                requester_did: None,
                behavior_id: Some("amy-default".to_string()),
                session_id: Some("session-1".to_string()),
                retry_parent_request: None,
                retry_root_request: None,
                superseded_by_request: None,
                content: Some("turn two".to_string()),
                temperature: None,
                top_p: None,
                top_k: None,
                seed: None,
                max_tokens: None,
                metadata: None,
                status: Some("processing".to_string()),
                lifecycle_state: Some("processing".to_string()),
                backend_id: None,
                execution_origin: Some("interactive".to_string()),
                failure_reason: None,
                terminalized_at: None,
                terminal_redrive_attempts: None,
                created_at: Some("2026-04-21T12:01:00Z".to_string()),
                claimed_at: None,
                deadline: None,
                retry_count: Some(0),
                max_retries: Some(3),
                caused_by_trigger_id: None,
                caused_by_trigger_kind: None,
                caused_by_parent_request_id: None,
                interrupt_requested_at: None,
                valid_until: None,
            },
        ],
        messages: vec![AgentMessageRow {
            message_key: "msg-1".to_string(),
            session_id: Some("session-1".to_string()),
            request_id: None,
            requester_did: None,
            sequence: Some(1),
            role: Some("user".to_string()),
            content: Some(user_message_json("turn one")),
            reasoning: None,
            timestamp: Some("2026-04-21T12:00:00Z".to_string()),
        }],
        responses: vec![AgentResponseRow {
            response_key: "resp-2".to_string(),
            request_id: Some("req-2".to_string()),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            session_id: Some("session-1".to_string()),
            content: Some("streaming reply".to_string()),
            reasoning: None,
            status: Some("streaming".to_string()),
            error_message: None,
            token_count: Some(12),
            progress_seq: Some(1),
            reasoning_progress_seq: Some(0),
            materialized_message_sequence: None,
            materialized_at: None,
            created_at: Some("2026-04-21T12:01:01Z".to_string()),
            completed_at: None,
            interrupted_at: None,
        }],
        tool_calls: vec![
            gents_protocol::row::AgentToolCallRow {
                partial_output_tail: None,
                partial_output_seq: None,
                tool_call_key: "historical-tool".to_string(),
                session_id: Some("session-1".to_string()),
                request_id: Some("req-1".to_string()),
                requester_did: None,
                message_sequence: Some(2),
                tool_name: Some("read".to_string()),
                tool_call_id: Some("call-0".to_string()),
                args: Some("{\"path\":\"README.md\"}".to_string()),
                result: Some("done".to_string()),
                status: Some("completed".to_string()),
                lifecycle_state: Some("completed".to_string()),
                child_request_id: None,
                await_mode: None,
                cancel_policy: None,
                workflow_group_id: None,
                workflow_role: None,
                started_at: Some("2026-04-21T12:00:02Z".to_string()),
                deadline_at: None,
                completed_at: Some("2026-04-21T12:00:03Z".to_string()),
                selected_service_id: None,
                selected_tool_name: None,
                tool_failure_class: None,
                denial_reason: None,
                denied_argv: None,
                denied_command: None,
                denied_argument: None,
                denied_subcommand: None,
                denied_prefix: None,
                policy_mode: None,
                policy_network: None,
                cancel_cause: None,
                latency_ms: None,
            },
            gents_protocol::row::AgentToolCallRow {
                partial_output_tail: None,
                partial_output_seq: None,
                tool_call_key: "tool-1".to_string(),
                session_id: Some("session-1".to_string()),
                request_id: Some("req-2".to_string()),
                requester_did: None,
                message_sequence: Some(3),
                tool_name: Some("glob".to_string()),
                tool_call_id: Some("call-1".to_string()),
                args: Some("{\"pattern\":\"**/*.rs\"}".to_string()),
                result: None,
                status: Some("running".to_string()),
                lifecycle_state: None,
                child_request_id: None,
                await_mode: None,
                cancel_policy: None,
                workflow_group_id: None,
                workflow_role: None,
                started_at: Some("2026-04-21T12:01:02Z".to_string()),
                deadline_at: None,
                completed_at: None,
                selected_service_id: None,
                selected_tool_name: None,
                tool_failure_class: None,
                denial_reason: None,
                denied_argv: None,
                denied_command: None,
                denied_argument: None,
                denied_subcommand: None,
                denied_prefix: None,
                policy_mode: None,
                policy_network: None,
                cancel_cause: None,
                latency_ms: None,
            },
        ],
        ..ClientStoreRows::default()
    });

    let snapshot = build_session_snapshot_from_store(&store, "session-1", Some("req-2"))
        .expect("session snapshot");
    let kinds = snapshot
        .timeline_items
        .iter()
        .map(|item| match item {
            RenderedTimelineItem::UserMessage { .. } => "user",
            RenderedTimelineItem::AssistantMessage { .. } => "assistant",
            RenderedTimelineItem::ToolGroup { .. } => "tools",
            RenderedTimelineItem::PendingUserTurn { .. } => "pending",
            RenderedTimelineItem::LiveAssistant { .. } => "live",
        })
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        vec!["user", "pending", "tools", "live", "tools"],
        "historical orphan tools stay before live reasoning, which targets the active group"
    );
}

#[test]
fn session_snapshot_hides_failed_unmaterialized_response_overlay() {
    let store = ClientStore::from_rows(ClientStoreRows {
        conversations: vec![AgentConversationRow {
            session_id: "session-1".to_string(),
            agent_name: Some("Amy".to_string()),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            title: Some("conversation".to_string()),
            title_source: Some("generated".to_string()),
            preview_text: Some("turn one".to_string()),
            status: Some("active".to_string()),
            created_at: Some("2026-04-21T12:00:00Z".to_string()),
            updated_at: Some("2026-04-21T12:15:00Z".to_string()),
            latest_request_id: Some("req-1".to_string()),
        }],
        requests: vec![AgentRequestRow {
            request_id: "req-1".to_string(),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            session_id: Some("session-1".to_string()),
            retry_parent_request: None,
            retry_root_request: None,
            superseded_by_request: None,
            content: Some("turn one".to_string()),
            temperature: None,
            top_p: None,
            top_k: None,
            seed: None,
            max_tokens: None,
            metadata: None,
            status: Some("error".to_string()),
            lifecycle_state: Some("failed".to_string()),
            backend_id: None,
            execution_origin: Some("interactive".to_string()),
            failure_reason: Some("request deadline exceeded".to_string()),
            terminalized_at: None,
            terminal_redrive_attempts: None,
            created_at: Some("2026-04-21T12:00:00Z".to_string()),
            claimed_at: Some("2026-04-21T12:00:01Z".to_string()),
            deadline: Some("2026-04-21T12:15:00Z".to_string()),
            retry_count: Some(0),
            max_retries: Some(3),
            caused_by_trigger_id: None,
            caused_by_trigger_kind: None,
            caused_by_parent_request_id: None,
            interrupt_requested_at: None,
            valid_until: None,
        }],
        messages: vec![AgentMessageRow {
            message_key: "msg-1".to_string(),
            session_id: Some("session-1".to_string()),
            request_id: None,
            requester_did: None,
            sequence: Some(1),
            role: Some("user".to_string()),
            content: Some(user_message_json("turn one")),
            reasoning: None,
            timestamp: Some("2026-04-21T12:00:00Z".to_string()),
        }],
        responses: vec![AgentResponseRow {
            response_key: "resp-1".to_string(),
            request_id: Some("req-1".to_string()),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            session_id: Some("session-1".to_string()),
            content: Some("partial answer before timeout".to_string()),
            reasoning: None,
            status: Some("error".to_string()),
            error_message: Some("request deadline exceeded".to_string()),
            token_count: Some(12),
            progress_seq: Some(3),
            reasoning_progress_seq: Some(0),
            materialized_message_sequence: None,
            materialized_at: None,
            created_at: Some("2026-04-21T12:00:02Z".to_string()),
            completed_at: Some("2026-04-21T12:15:00Z".to_string()),
            interrupted_at: None,
        }],
        ..ClientStoreRows::default()
    });

    let snapshot = build_session_snapshot_from_store(&store, "session-1", Some("req-1"))
        .expect("session snapshot");

    assert_eq!(snapshot.turn_state.as_deref(), Some("failed"));
    assert_eq!(
        snapshot
            .latest_response
            .as_ref()
            .and_then(|response| response.error_message.as_deref()),
        Some("request deadline exceeded")
    );
    let serialized = serde_json::to_value(&snapshot).expect("serialize snapshot");
    assert_eq!(
        serialized["latestResponse"]["errorMessage"],
        "request deadline exceeded"
    );
    assert!(snapshot.active_response_overlay.is_none());

    let has_live = snapshot
        .timeline_items
        .iter()
        .any(|item| matches!(item, RenderedTimelineItem::LiveAssistant { .. }));
    assert!(!has_live, "failed turns must not render live overlays");
}

#[test]
fn session_snapshot_keeps_full_live_overlay_when_only_prior_turn_shares_prefix() {
    let store = ClientStore::from_rows(ClientStoreRows {
        conversations: vec![AgentConversationRow {
            session_id: "session-1".to_string(),
            agent_name: Some("Amy".to_string()),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            title: Some("conversation".to_string()),
            title_source: Some("generated".to_string()),
            preview_text: Some("turn two".to_string()),
            status: Some("active".to_string()),
            created_at: Some("2026-04-21T12:00:00Z".to_string()),
            updated_at: Some("2026-04-21T12:02:00Z".to_string()),
            latest_request_id: Some("req-2".to_string()),
        }],
        requests: vec![
            AgentRequestRow {
                request_id: "req-1".to_string(),
                agent_did: Some("did:test:amy".to_string()),
                requester_did: None,
                behavior_id: Some("amy-default".to_string()),
                session_id: Some("session-1".to_string()),
                retry_parent_request: None,
                retry_root_request: None,
                superseded_by_request: None,
                content: Some("turn one".to_string()),
                temperature: None,
                top_p: None,
                top_k: None,
                seed: None,
                max_tokens: None,
                metadata: None,
                status: Some("completed".to_string()),
                lifecycle_state: Some("completed".to_string()),
                backend_id: None,
                execution_origin: Some("interactive".to_string()),
                failure_reason: None,
                terminalized_at: None,
                terminal_redrive_attempts: None,
                created_at: Some("2026-04-21T12:00:00Z".to_string()),
                claimed_at: None,
                deadline: None,
                retry_count: Some(0),
                max_retries: Some(3),
                caused_by_trigger_id: None,
                caused_by_trigger_kind: None,
                caused_by_parent_request_id: None,
                interrupt_requested_at: None,
                valid_until: None,
            },
            AgentRequestRow {
                request_id: "req-2".to_string(),
                agent_did: Some("did:test:amy".to_string()),
                requester_did: None,
                behavior_id: Some("amy-default".to_string()),
                session_id: Some("session-1".to_string()),
                retry_parent_request: None,
                retry_root_request: None,
                superseded_by_request: None,
                content: Some("turn two".to_string()),
                temperature: None,
                top_p: None,
                top_k: None,
                seed: None,
                max_tokens: None,
                metadata: None,
                status: Some("processing".to_string()),
                lifecycle_state: Some("processing".to_string()),
                backend_id: None,
                execution_origin: Some("interactive".to_string()),
                failure_reason: None,
                terminalized_at: None,
                terminal_redrive_attempts: None,
                created_at: Some("2026-04-21T12:01:00Z".to_string()),
                claimed_at: None,
                deadline: None,
                retry_count: Some(0),
                max_retries: Some(3),
                caused_by_trigger_id: None,
                caused_by_trigger_kind: None,
                caused_by_parent_request_id: None,
                interrupt_requested_at: None,
                valid_until: None,
            },
        ],
        messages: vec![
            AgentMessageRow {
                message_key: "msg-1".to_string(),
                session_id: Some("session-1".to_string()),
                request_id: None,
                requester_did: None,
                sequence: Some(1),
                role: Some("user".to_string()),
                content: Some(user_message_json("turn one")),
                reasoning: None,
                timestamp: Some("2026-04-21T12:00:00Z".to_string()),
            },
            AgentMessageRow {
                message_key: "msg-2".to_string(),
                session_id: Some("session-1".to_string()),
                request_id: None,
                requester_did: None,
                sequence: Some(2),
                role: Some("assistant".to_string()),
                content: Some(
                    serde_json::to_string(&Message::assistant("I'll investigate"))
                        .expect("serialize assistant"),
                ),
                reasoning: None,
                timestamp: Some("2026-04-21T12:00:01Z".to_string()),
            },
            AgentMessageRow {
                message_key: "msg-3".to_string(),
                session_id: Some("session-1".to_string()),
                request_id: None,
                requester_did: None,
                sequence: Some(3),
                role: Some("user".to_string()),
                content: Some(user_message_json("turn two")),
                reasoning: None,
                timestamp: Some("2026-04-21T12:01:00Z".to_string()),
            },
        ],
        responses: vec![AgentResponseRow {
            response_key: "resp-2".to_string(),
            request_id: Some("req-2".to_string()),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            session_id: Some("session-1".to_string()),
            content: Some("I'll investigate further into p2p".to_string()),
            reasoning: None,
            status: Some("streaming".to_string()),
            error_message: None,
            token_count: Some(12),
            progress_seq: Some(1),
            reasoning_progress_seq: Some(0),
            materialized_message_sequence: None,
            materialized_at: None,
            created_at: Some("2026-04-21T12:01:01Z".to_string()),
            completed_at: None,
            interrupted_at: None,
        }],
        ..ClientStoreRows::default()
    });

    let snapshot = build_session_snapshot_from_store(&store, "session-1", Some("req-2"))
        .expect("session snapshot");
    let live_content = snapshot.timeline_items.iter().find_map(|item| match item {
        RenderedTimelineItem::LiveAssistant { content, .. } => content.as_deref(),
        _ => None,
    });
    assert_eq!(live_content, Some("I'll investigate further into p2p"));
}

#[test]
fn session_snapshot_renders_structured_tool_payloads_in_timeline() {
    let store = ClientStore::from_rows(ClientStoreRows {
        conversations: vec![AgentConversationRow {
            session_id: "session-1".to_string(),
            agent_name: Some("Amy".to_string()),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            title: Some("conversation".to_string()),
            title_source: Some("generated".to_string()),
            preview_text: Some("turn one".to_string()),
            status: Some("active".to_string()),
            created_at: Some("2026-04-21T12:00:00Z".to_string()),
            updated_at: Some("2026-04-21T12:02:00Z".to_string()),
            latest_request_id: Some("req-1".to_string()),
        }],
        requests: vec![AgentRequestRow {
            request_id: "req-1".to_string(),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            session_id: Some("session-1".to_string()),
            retry_parent_request: None,
            retry_root_request: None,
            superseded_by_request: None,
            content: Some("turn one".to_string()),
            temperature: None,
            top_p: None,
            top_k: None,
            seed: None,
            max_tokens: None,
            metadata: None,
            status: Some("processing".to_string()),
            lifecycle_state: Some("processing".to_string()),
            backend_id: None,
            execution_origin: Some("interactive".to_string()),
            failure_reason: None,
            terminalized_at: None,
            terminal_redrive_attempts: None,
            created_at: Some("2026-04-21T12:00:00Z".to_string()),
            claimed_at: None,
            deadline: None,
            retry_count: Some(0),
            max_retries: Some(3),
            caused_by_trigger_id: None,
            caused_by_trigger_kind: None,
            caused_by_parent_request_id: None,
            interrupt_requested_at: None,
            valid_until: None,
        }],
        messages: vec![AgentMessageRow {
            message_key: "msg-1".to_string(),
            session_id: Some("session-1".to_string()),
            request_id: None,
            requester_did: None,
            sequence: Some(1),
            role: Some("user".to_string()),
            content: Some(user_message_json("turn one")),
            reasoning: None,
            timestamp: Some("2026-04-21T12:00:00Z".to_string()),
        }],
        tool_calls: vec![gents_protocol::row::AgentToolCallRow {
            partial_output_tail: None,
            partial_output_seq: None,
            tool_call_key: "tool-1".to_string(),
            session_id: Some("session-1".to_string()),
            request_id: None,
            requester_did: None,
            message_sequence: Some(2),
            tool_name: Some("glob".to_string()),
            tool_call_id: Some("call-1".to_string()),
            args: Some("{\"pattern\":\"**/*.rs\",\"recursive\":true}".to_string()),
            result: Some("{\"matches\":12}".to_string()),
            status: Some("completed".to_string()),
            lifecycle_state: None,
            child_request_id: None,
            await_mode: None,
            cancel_policy: None,
            workflow_group_id: None,
            workflow_role: None,
            started_at: Some("2026-04-21T12:00:01Z".to_string()),
            deadline_at: None,
            completed_at: Some("2026-04-21T12:00:02Z".to_string()),
            selected_service_id: None,
            selected_tool_name: None,
            tool_failure_class: None,
            denial_reason: None,
            denied_argv: None,
            denied_command: None,
            denied_argument: None,
            denied_subcommand: None,
            denied_prefix: None,
            policy_mode: None,
            policy_network: None,
            cancel_cause: None,
            latency_ms: None,
        }],
        ..ClientStoreRows::default()
    });

    let snapshot = build_session_snapshot_from_store(&store, "session-1", Some("req-1"))
        .expect("session snapshot");
    let tools = snapshot
        .timeline_items
        .iter()
        .find_map(|item| match item {
            RenderedTimelineItem::ToolGroup { tools, .. } => Some(tools),
            _ => None,
        })
        .expect("tool group");

    assert_eq!(tools.len(), 1);
    let tool = &tools[0];
    assert_eq!(tool.tool_name, "glob");
    assert_eq!(tool.status_kind, "success");
    assert_eq!(
        tool.args.as_ref().map(|value| value
            .fields
            .iter()
            .map(|field| field.key.as_str())
            .collect::<Vec<_>>()),
        Some(vec!["pattern", "recursive"])
    );
    assert_eq!(
        tool.result
            .as_ref()
            .and_then(|value| value.fields.iter().find(|field| field.key == "matches"))
            .map(|field| field.value.as_str()),
        Some("12")
    );
}

#[test]
fn structured_command_policy_denial_projects_to_rendered_tool() {
    let store = ClientStore::from_rows(ClientStoreRows {
        sessions: vec![AgentSessionRow {
            session_id: "session-denial".to_string(),
            agent_name: Some("Amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            started: None,
            ended: None,
            status: Some("active".to_string()),
        }],
        tool_calls: vec![gents_protocol::row::AgentToolCallRow {
            partial_output_tail: None,
            partial_output_seq: None,
            tool_call_key: "tool-denial".to_string(),
            session_id: Some("session-denial".to_string()),
            request_id: None,
            requester_did: None,
            message_sequence: Some(1),
            tool_name: Some("bash".to_string()),
            tool_call_id: Some("call-denial".to_string()),
            args: Some("{\"command\":\"git\",\"args\":[\"commit\"]}".to_string()),
            result: Some("structured policy denial payload".to_string()),
            status: Some("completed".to_string()),
            lifecycle_state: Some("failed".to_string()),
            child_request_id: None,
            await_mode: None,
            cancel_policy: None,
            workflow_group_id: None,
            workflow_role: None,
            started_at: None,
            deadline_at: None,
            completed_at: Some("2026-05-20T10:32:16Z".to_string()),
            selected_service_id: None,
            selected_tool_name: None,
            tool_failure_class: Some("policyDenied".to_string()),
            denial_reason: Some("readOnlySubcommandNotAllowlisted".to_string()),
            denied_argv: None,
            denied_command: Some("git".to_string()),
            denied_argument: None,
            denied_subcommand: Some("commit".to_string()),
            denied_prefix: None,
            policy_mode: Some("read_only".to_string()),
            policy_network: Some("inherit".to_string()),
            cancel_cause: None,
            latency_ms: Some(12),
        }],
        ..ClientStoreRows::default()
    });

    let snapshot =
        build_session_snapshot_from_store(&store, "session-denial", None).expect("snapshot");
    let tool = snapshot
        .timeline_items
        .iter()
        .find_map(|item| match item {
            RenderedTimelineItem::ToolGroup { tools, .. } => tools.first(),
            _ => None,
        })
        .expect("rendered tool");
    let denial = tool.denial.as_ref().expect("structured denial");

    assert_eq!(tool.status.as_deref(), Some("completed"));
    assert_eq!(tool.status_kind, "error");
    assert_eq!(denial.rule_id, "readOnlySubcommandNotAllowlisted");
    assert_eq!(denial.category, "read-only-guard");
    assert_eq!(denial.denied_command.as_deref(), Some("git"));
    assert_eq!(denial.denied_subcommand.as_deref(), Some("commit"));
    assert_eq!(denial.diagnostic, "structured policy denial payload");
}
