use super::*;

#[path = "../../../../../crates/gents/src/lean_vocab_test/support.rs"]
mod lean_vocab_test;

use gents::llm::message::{
    AssistantContent, Message, Text, ToolCall, ToolFunction, ToolResult, ToolResultContent,
    UserContent,
};
use lean_vocab_test::{
    lean_desktop_client_shell_cases, lean_request_lifecycle_operator_ui_cases,
    lean_response_transition_cases, lean_transcript_cases, LeanClientShellCase,
    LeanResponseTransitionCase, LeanTranscriptCase,
};
use serde_json::json;

#[test]
fn session_snapshot_projects_durable_goal_state() {
    let store = ClientStore::from_rows(ClientStoreRows {
        goals: vec![GoalRow {
            goal_id: "goal-1".to_string(),
            session_id: "session-goal".to_string(),
            agent_did: "did:test:amy".to_string(),
            objective: Some("Ship the durable controller".to_string()),
            status: Some("active".to_string()),
            token_budget: Some(50_000),
            tokens_used: Some(1_200),
            active_time_seconds: Some(42),
            active_started_at: Some("2026-07-15T00:00:00Z".to_string()),
            consecutive_blocked_audits: Some(2),
            last_blocked_request_id: Some("request-2".to_string()),
            last_blocked_reason: Some("needs approval".to_string()),
            last_continued_from_request_id: Some("request-2".to_string()),
            continuation_sequence: Some(3),
            wrapup_requested: Some(false),
            wrapup_completed: Some(false),
            infrastructure_retry_count: Some(0),
            last_failure: None,
            completion_evidence: None,
            created_at: Some("2026-07-15T00:00:00Z".to_string()),
            updated_at: Some("2026-07-15T00:01:00Z".to_string()),
        }],
        ..ClientStoreRows::default()
    });

    let snapshot = build_session_snapshot_from_store_for_agent(
        &store,
        Some("did:test:amy"),
        "session-goal",
        None,
    )
    .expect("goal-only session snapshot");
    let goal = snapshot.goal.expect("durable goal projection");
    assert_eq!(goal.goal_id, "goal-1");
    assert_eq!(
        goal.objective.as_deref(),
        Some("Ship the durable controller")
    );
    assert_eq!(goal.status.as_deref(), Some("active"));
    assert_eq!(goal.token_budget, Some(50_000));
    assert_eq!(goal.tokens_used, 1_200);
    assert_eq!(goal.active_time_seconds, 42);
    assert_eq!(goal.consecutive_blocked_audits, 2);
    assert_eq!(goal.continuation_sequence, 3);
}

#[test]
fn session_snapshot_can_be_built_without_conversation_row_when_session_is_observed() {
    let store = ClientStore::from_rows(ClientStoreRows {
        sessions: vec![AgentSessionRow {
            session_id: "session-1".to_string(),
            agent_name: Some("Amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            started: Some("2026-04-21T12:00:00Z".to_string()),
            ended: None,
            status: Some("active".to_string()),
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
            content: Some("follow up question".to_string()),
            temperature: None,
            top_p: None,
            top_k: None,
            seed: None,
            max_tokens: None,
            max_total_tokens: None,
            metadata: None,
            status: Some("completed".to_string()),
            lifecycle_state: Some("completed".to_string()),
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
        }],
        responses: vec![AgentResponseRow {
            response_key: "resp-1".to_string(),
            request_id: Some("req-1".to_string()),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            session_id: Some("session-1".to_string()),
            content: Some("done".to_string()),
            reasoning: None,
            status: Some("complete".to_string()),
            error_message: None,
            token_count: Some(12),
            progress_seq: Some(1),
            reasoning_progress_seq: Some(0),
            materialized_message_sequence: Some(2),
            materialized_at: Some("2026-04-21T12:01:05Z".to_string()),
            created_at: Some("2026-04-21T12:01:01Z".to_string()),
            completed_at: Some("2026-04-21T12:01:05Z".to_string()),
            interrupted_at: None,
        }],
        ..ClientStoreRows::default()
    });

    let snapshot =
        build_session_snapshot_from_store(&store, "session-1", None).expect("session snapshot");
    assert_eq!(snapshot.session_id, "session-1");
    assert_eq!(snapshot.agent_did.as_deref(), Some("did:test:amy"));
    assert_eq!(snapshot.behavior_id.as_deref(), Some("amy-default"));
    assert_eq!(snapshot.status.as_deref(), Some("active"));
    assert_eq!(snapshot.turn_state.as_deref(), Some("completed"));
    assert_eq!(snapshot.latest_request_id.as_deref(), Some("req-1"));
}

#[test]
fn session_snapshot_prefers_tracked_request_over_stale_conversation_latest_request() {
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
            latest_request_id: Some("req-1".to_string()),
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
                max_total_tokens: None,
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
                max_total_tokens: None,
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
        ..ClientStoreRows::default()
    });

    let snapshot = build_session_snapshot_from_store(&store, "session-1", Some("req-2"))
        .expect("session snapshot");

    assert_eq!(snapshot.latest_request_id.as_deref(), Some("req-2"));
    assert_eq!(snapshot.turn_state.as_deref(), Some("streaming"));
    assert_eq!(
        snapshot
            .pending_turn
            .as_ref()
            .map(|turn| turn.request_id.as_str()),
        Some("req-2")
    );
    assert_eq!(
        snapshot
            .active_response_overlay
            .as_ref()
            .and_then(|response| response.content.as_deref()),
        Some("streaming reply")
    );
}

#[test]
fn session_snapshot_does_not_report_unobserved_preferred_request() {
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
            updated_at: Some("2026-04-21T12:00:01Z".to_string()),
            latest_request_id: Some("req-old".to_string()),
        }],
        requests: vec![AgentRequestRow {
            request_id: "req-old".to_string(),
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
            max_total_tokens: None,
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
        ..ClientStoreRows::default()
    });

    let snapshot = build_session_snapshot_from_store(&store, "session-1", Some("req-new"))
        .expect("session snapshot");

    assert_eq!(
        snapshot.latest_request_id.as_deref(),
        Some("req-old"),
        "Proofs.ClientShell.C9: an awaiting request retires only after the matching request is observed"
    );
    assert_eq!(snapshot.turn_state.as_deref(), Some("completed"));
    assert!(snapshot.pending_turn.is_none());
}

#[test]
fn session_snapshot_projection_consumes_generated_client_shell_contract_cases() {
    let cases = lean_desktop_client_shell_cases();
    assert_eq!(
        cases.len(),
        12,
        "desktop ClientShell contract surface should include every selected-session case"
    );

    for case in cases {
        let name = case.name.as_str();
        let store = client_shell_contract_store(case);
        let selected_session_id = contract_session_id(
            case.desktop_selected_session_id
                .expect("contract case should select a session"),
        );
        let preferred_request_id = case.desktop_preferred_request_id.map(contract_request_id);

        let snapshot = build_session_snapshot_from_store(
            &store,
            &selected_session_id,
            preferred_request_id.as_deref(),
        );

        assert_eq!(
            snapshot.is_some(),
            case.desktop_snapshot_present,
            "case {name} snapshot presence drifted from Lean-selected observation"
        );

        let Some(snapshot) = snapshot else {
            continue;
        };

        assert_eq!(
            snapshot.latest_request_id.as_deref(),
            case.desktop_expected_latest_request_id
                .map(contract_request_id)
                .as_deref(),
            "case {name} should project the Lean-observed latest request"
        );
        assert_eq!(
            snapshot.turn_state.as_deref(),
            case.desktop_expected_turn_state.as_deref(),
            "case {name} should project the Lean-derived turn state"
        );
        if let Some(expect_pending) = case.desktop_expect_pending_turn {
            assert_eq!(
                snapshot.pending_turn.is_some(),
                expect_pending,
                "case {name} pending-turn projection drifted from Lean"
            );
        }
    }
}

#[test]
fn session_snapshot_binds_request_lifecycle_operator_ui_cases() {
    let cases = lean_request_lifecycle_operator_ui_cases();
    assert!(
        !cases.is_empty(),
        "request-lifecycle operator UI contract cases should be emitted"
    );

    let mut saw_nonterminal_turn = false;
    let mut saw_terminal_turn = false;

    for case in cases {
        let name = case.name.as_str();
        let observed_turn = case
            .desktop_observed_turn_state
            .as_deref()
            .expect("request-lifecycle UI cases must observe a request turn");
        let (_request_status, lifecycle_state) = request_state_for_turn(Some(observed_turn));
        saw_nonterminal_turn |= matches!(observed_turn, "waitingForClaim" | "streaming");
        saw_terminal_turn |= !matches!(observed_turn, "waitingForClaim" | "streaming");

        let store = client_shell_contract_store(case);
        let selected_session_id = contract_session_id(
            case.desktop_selected_session_id
                .expect("request-lifecycle UI cases should select a session"),
        );
        let preferred_request_id = case.desktop_preferred_request_id.map(contract_request_id);
        let snapshot = build_session_snapshot_from_store(
            &store,
            &selected_session_id,
            preferred_request_id.as_deref(),
        )
        .expect("request-lifecycle UI case should build a desktop session snapshot");

        assert_eq!(
            snapshot.latest_request_id.as_deref(),
            case.desktop_expected_latest_request_id
                .map(contract_request_id)
                .as_deref(),
            "case {name} should bind the UI snapshot to the observed lifecycle request"
        );
        assert_eq!(
            snapshot.turn_state.as_deref(),
            Some(observed_turn),
            "case {name} should expose request lifecycle state as the UI turn state"
        );
        if let Some(expect_pending) = case.desktop_expect_pending_turn {
            assert_eq!(
                snapshot.pending_turn.is_some(),
                expect_pending,
                "case {name} pending-turn visibility drifted from lifecycle state"
            );
        }
        if let Some(pending_turn) = snapshot.pending_turn.as_ref() {
            assert_eq!(
                pending_turn.lifecycle_state.as_deref(),
                Some(lifecycle_state),
                "case {name} should carry the raw lifecycle state for UI badges"
            );
        }
    }

    assert!(
        saw_nonterminal_turn && saw_terminal_turn,
        "request-lifecycle UI cases should cover active and terminal turn bindings"
    );
}

#[test]
fn session_snapshot_streaming_response_overlay_consumes_generated_transition_cases() {
    let cases = lean_response_transition_cases();
    assert_eq!(
        cases.len(),
        12,
        "desktop streaming renderer should consume every Lean response transition case"
    );

    for case in cases {
        let store = streaming_response_contract_store(case);
        let snapshot = build_session_snapshot_from_store(&store, "session-1", Some("req-1"))
            .unwrap_or_else(|| panic!("case {} should produce a session snapshot", case.name));

        assert_eq!(
            snapshot
                .latest_response
                .as_ref()
                .and_then(|response| response.status.as_deref()),
            Some(case.post_status.as_str()),
            "case {} should expose the Lean post response status",
            case.name
        );
        assert_eq!(
            snapshot
                .latest_response
                .as_ref()
                .and_then(|response| response.token_count)
                .map(|count| count as usize),
            Some(case.post_token_count),
            "case {} should expose the Lean post token count",
            case.name
        );
        assert_eq!(
            snapshot
                .latest_response
                .as_ref()
                .and_then(|response| response.materialized_message_sequence)
                .map(|sequence| sequence as usize),
            case.post_materialized_seq,
            "case {} should expose the Lean materialization sequence",
            case.name
        );

        let expected_live_overlay = streaming_case_should_render_live_overlay(case);
        assert_eq!(
            snapshot.active_response_overlay.is_some(),
            expected_live_overlay,
            "case {} active overlay visibility should follow the Lean streaming post state",
            case.name
        );

        let live_items = snapshot
            .timeline_items
            .iter()
            .filter_map(|item| match item {
                RenderedTimelineItem::LiveAssistant {
                    content, reasoning, ..
                } => Some((content.as_deref(), reasoning.as_deref())),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            live_items.len(),
            usize::from(expected_live_overlay),
            "case {} should render exactly the Lean-visible live assistant item count",
            case.name
        );
        if expected_live_overlay {
            let (expected_content, expected_reasoning) = streaming_case_tail(case);
            assert_eq!(
                live_items[0],
                (expected_content.as_deref(), expected_reasoning.as_deref()),
                "case {} live assistant item should carry the Lean live tail",
                case.name
            );
        }
    }
}

#[test]
fn session_snapshot_transcript_rendering_consumes_generated_transcript_cases() {
    let cases = lean_transcript_cases();
    assert_eq!(
        cases.len(),
        7,
        "desktop transcript rendering should consume every generated Lean transcript case"
    );

    for case in cases {
        let store = transcript_contract_store(case);
        let snapshot = build_session_snapshot_from_store(&store, "session-1", Some("req-1"))
            .unwrap_or_else(|| panic!("case {} should produce a session snapshot", case.name));

        assert_eq!(
            snapshot.messages.len(),
            case.post_message_count,
            "case {} should expose the Lean durable message count to the desktop renderer",
            case.name
        );
        assert_eq!(
            snapshot.tool_calls.len(),
            case.post_tool_call_count,
            "case {} should expose the Lean durable tool-call count to the desktop renderer",
            case.name
        );

        let hidden_tool_result_rows = snapshot
            .messages
            .iter()
            .filter(|message| message.has_tool_results)
            .count();
        assert_eq!(
            hidden_tool_result_rows,
            transcript_contract_tool_result_rows(case),
            "case {} should keep Lean tool-result transcript rows out of chat-message rendering",
            case.name
        );

        let rendered_tool_groups = snapshot
            .timeline_items
            .iter()
            .filter_map(|item| match item {
                RenderedTimelineItem::ToolGroup {
                    message_sequence,
                    tools,
                    ..
                } => Some((*message_sequence, tools)),
                _ => None,
            })
            .collect::<Vec<_>>();
        let rendered_tool_count = rendered_tool_groups
            .iter()
            .map(|(_, tools)| tools.len())
            .sum::<usize>();
        assert_eq!(
            rendered_tool_count, case.post_tool_call_count,
            "case {} should render every Lean tool-call row in transcript tool groups",
            case.name
        );

        if case.post_tool_call_count > 0 {
            assert_eq!(
                rendered_tool_groups.len(),
                1,
                "case {} should render one grouped tool-call block",
                case.name
            );
            assert_eq!(
                rendered_tool_groups[0].0,
                transcript_contract_tool_group_sequence(case),
                "case {} should attach rendered tools to the Lean assistant sequence",
                case.name
            );
        }

        let rendered_kinds = snapshot
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
            rendered_kinds,
            transcript_contract_rendered_kinds(case),
            "case {} transcript timeline shape should follow the Lean post-state",
            case.name
        );
    }
}

#[test]
fn session_snapshot_stays_renderable_across_single_turn_observation_updates() {
    let submitted = ClientStore::from_rows(ClientStoreRows {
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
            updated_at: Some("2026-04-21T12:00:01Z".to_string()),
            latest_request_id: None,
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
            max_total_tokens: None,
            metadata: None,
            status: Some("pending".to_string()),
            lifecycle_state: Some("pending".to_string()),
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
        ..ClientStoreRows::default()
    });
    let submitted_snapshot =
        build_session_snapshot_from_store(&submitted, "session-1", Some("req-1"))
            .expect("submitted snapshot");
    assert_eq!(
        submitted_snapshot.latest_request_id.as_deref(),
        Some("req-1")
    );
    assert_eq!(
        submitted_snapshot.turn_state.as_deref(),
        Some("waitingForClaim")
    );
    assert_eq!(
        submitted_snapshot
            .pending_turn
            .as_ref()
            .map(|turn| turn.request_id.as_str()),
        Some("req-1")
    );

    let streaming = ClientStore::from_rows(ClientStoreRows {
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
            updated_at: Some("2026-04-21T12:00:02Z".to_string()),
            latest_request_id: None,
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
            max_total_tokens: None,
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
        responses: vec![AgentResponseRow {
            response_key: "resp-1".to_string(),
            request_id: Some("req-1".to_string()),
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
            created_at: Some("2026-04-21T12:00:01Z".to_string()),
            completed_at: None,
            interrupted_at: None,
        }],
        ..ClientStoreRows::default()
    });
    let streaming_snapshot =
        build_session_snapshot_from_store(&streaming, "session-1", Some("req-1"))
            .expect("streaming snapshot");
    assert_eq!(streaming_snapshot.turn_state.as_deref(), Some("streaming"));
    assert_eq!(
        streaming_snapshot
            .active_response_overlay
            .as_ref()
            .and_then(|response| response.content.as_deref()),
        Some("streaming reply")
    );

    let completed = ClientStore::from_rows(ClientStoreRows {
        conversations: vec![AgentConversationRow {
            session_id: "session-1".to_string(),
            agent_name: Some("Amy".to_string()),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            title: Some("conversation".to_string()),
            title_source: Some("generated".to_string()),
            preview_text: Some("final answer".to_string()),
            status: Some("completed".to_string()),
            created_at: Some("2026-04-21T12:00:00Z".to_string()),
            updated_at: Some("2026-04-21T12:00:05Z".to_string()),
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
            max_total_tokens: None,
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
        }],
        responses: vec![AgentResponseRow {
            response_key: "resp-1".to_string(),
            request_id: Some("req-1".to_string()),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            session_id: Some("session-1".to_string()),
            content: Some("final answer".to_string()),
            reasoning: None,
            status: Some("complete".to_string()),
            error_message: None,
            token_count: Some(34),
            progress_seq: Some(2),
            reasoning_progress_seq: Some(0),
            materialized_message_sequence: Some(2),
            materialized_at: Some("2026-04-21T12:00:05Z".to_string()),
            created_at: Some("2026-04-21T12:00:01Z".to_string()),
            completed_at: Some("2026-04-21T12:00:05Z".to_string()),
            interrupted_at: None,
        }],
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
                    "{\"role\":\"assistant\",\"content\":[{\"text\":\"final answer\"}]}"
                        .to_string(),
                ),
                reasoning: None,
                timestamp: Some("2026-04-21T12:00:05Z".to_string()),
            },
        ],
        ..ClientStoreRows::default()
    });
    let completed_snapshot =
        build_session_snapshot_from_store(&completed, "session-1", Some("req-1"))
            .expect("completed snapshot");
    assert_eq!(completed_snapshot.turn_state.as_deref(), Some("completed"));
    assert!(completed_snapshot.active_response_overlay.is_none());
    assert!(completed_snapshot.pending_turn.is_none());
}

#[test]
fn session_snapshot_derives_cancel_cause_for_interrupted_response_and_cancelled_tool_call() {
    let store = ClientStore::from_rows(ClientStoreRows {
        conversations: vec![AgentConversationRow {
            session_id: "session-1".to_string(),
            agent_name: Some("Amy".to_string()),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            title: Some("cancel cause test".to_string()),
            title_source: Some("generated".to_string()),
            preview_text: Some("user question".to_string()),
            status: Some("interrupted".to_string()),
            created_at: Some("2026-05-20T10:30:00Z".to_string()),
            updated_at: Some("2026-05-20T10:32:20Z".to_string()),
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
            content: Some("user question".to_string()),
            temperature: None,
            top_p: None,
            top_k: None,
            seed: None,
            max_tokens: None,
            max_total_tokens: None,
            metadata: None,
            status: Some("interrupted".to_string()),
            lifecycle_state: Some("interrupted".to_string()),
            backend_id: None,
            execution_origin: Some("interactive".to_string()),
            failure_reason: None,
            terminalized_at: None,
            terminal_redrive_attempts: None,
            created_at: Some("2026-05-20T10:30:00Z".to_string()),
            claimed_at: Some("2026-05-20T10:30:01Z".to_string()),
            deadline: None,
            retry_count: Some(0),
            max_retries: Some(3),
            caused_by_trigger_id: None,
            caused_by_trigger_kind: None,
            caused_by_parent_request_id: None,
            interrupt_requested_at: Some("2026-05-20T10:32:14Z".to_string()),
            valid_until: None,
        }],
        responses: vec![AgentResponseRow {
            response_key: "resp-1".to_string(),
            request_id: Some("req-1".to_string()),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            session_id: Some("session-1".to_string()),
            content: Some("partial response before interrupt".to_string()),
            reasoning: None,
            status: Some("interrupted".to_string()),
            error_message: None,
            token_count: Some(8),
            progress_seq: Some(1),
            reasoning_progress_seq: Some(0),
            materialized_message_sequence: None,
            materialized_at: None,
            created_at: Some("2026-05-20T10:30:02Z".to_string()),
            completed_at: None,
            interrupted_at: Some("2026-05-20T10:32:15Z".to_string()),
        }],
        messages: vec![AgentMessageRow {
            message_key: "msg-1".to_string(),
            session_id: Some("session-1".to_string()),
            request_id: None,
            requester_did: None,
            sequence: Some(1),
            role: Some("user".to_string()),
            content: Some(user_message_json("user question")),
            reasoning: None,
            timestamp: Some("2026-05-20T10:30:00Z".to_string()),
        }],
        tool_calls: vec![gents_protocol::row::AgentToolCallRow {
            partial_output_tail: None,
            partial_output_seq: None,
            tool_call_key: "tool-1".to_string(),
            session_id: Some("session-1".to_string()),
            request_id: None,
            requester_did: None,
            message_sequence: Some(2),
            tool_name: Some("bash".to_string()),
            tool_call_id: Some("call-1".to_string()),
            args: Some("{\"command\":\"ls\"}".to_string()),
            result: None,
            status: Some("cancelled".to_string()),
            lifecycle_state: Some("cancelled".to_string()),
            child_request_id: None,
            await_mode: None,
            cancel_policy: None,
            workflow_group_id: None,
            workflow_role: None,
            started_at: Some("2026-05-20T10:31:00Z".to_string()),
            deadline_at: None,
            completed_at: Some("2026-05-20T10:32:16Z".to_string()),
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

    let snapshot =
        build_session_snapshot_from_store(&store, "session-1", None).expect("session snapshot");

    let response_cancel_cause = snapshot
        .latest_response
        .as_ref()
        .and_then(|r| r.cancel_cause.as_ref())
        .expect("interrupted response should have a derived cancel_cause");
    assert_eq!(
        response_cancel_cause.cause, "interrupted",
        "interrupted response cause should be 'interrupted'"
    );
    assert_eq!(
        response_cancel_cause.source, "responseInterruptedAt",
        "interrupted response source should be 'responseInterruptedAt'"
    );

    let tool_group = snapshot
        .timeline_items
        .iter()
        .find_map(|item| match item {
            RenderedTimelineItem::ToolGroup { tools, .. } => Some(tools),
            _ => None,
        })
        .expect("timeline should contain a ToolGroup");
    let tool = tool_group
        .iter()
        .find(|t| t.tool_name == "bash")
        .expect("bash tool call");
    let tool_cancel_cause = tool
        .cancel_cause
        .as_ref()
        .expect("cancelled tool call should have a derived cancel_cause");
    assert_eq!(
        tool_cancel_cause.cause, "userCancelled",
        "cancelled tool call cause should be 'userCancelled'"
    );
    assert_eq!(
        tool_cancel_cause.source, "requestInterrupt",
        "cancelled tool call source should be 'requestInterrupt'"
    );
}

#[test]
fn session_snapshot_derives_interrupted_cause_for_child_request_with_cascade_policy() {
    let store = ClientStore::from_rows(ClientStoreRows {
        conversations: vec![AgentConversationRow {
            session_id: "session-1".to_string(),
            agent_name: Some("Amy".to_string()),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            title: Some("cascade cancel test".to_string()),
            title_source: Some("generated".to_string()),
            preview_text: Some("subagent request".to_string()),
            status: Some("interrupted".to_string()),
            created_at: Some("2026-05-20T10:30:00Z".to_string()),
            updated_at: Some("2026-05-20T10:32:20Z".to_string()),
            latest_request_id: Some("req-child".to_string()),
        }],
        requests: vec![AgentRequestRow {
            request_id: "req-child".to_string(),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            session_id: Some("session-1".to_string()),
            retry_parent_request: None,
            retry_root_request: None,
            superseded_by_request: None,
            content: Some("subagent task".to_string()),
            temperature: None,
            top_p: None,
            top_k: None,
            seed: None,
            max_tokens: None,
            max_total_tokens: None,
            metadata: None,
            status: Some("interrupted".to_string()),
            lifecycle_state: Some("interrupted".to_string()),
            backend_id: None,
            execution_origin: Some("subagent".to_string()),
            failure_reason: None,
            terminalized_at: None,
            terminal_redrive_attempts: None,
            created_at: Some("2026-05-20T10:30:00Z".to_string()),
            claimed_at: Some("2026-05-20T10:30:01Z".to_string()),
            deadline: None,
            retry_count: Some(0),
            max_retries: Some(3),
            caused_by_trigger_id: None,
            caused_by_trigger_kind: None,
            caused_by_parent_request_id: Some("req-parent".to_string()),
            interrupt_requested_at: None,
            valid_until: None,
        }],
        responses: vec![AgentResponseRow {
            response_key: "resp-child".to_string(),
            request_id: Some("req-child".to_string()),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            session_id: Some("session-1".to_string()),
            content: Some("partial subagent response".to_string()),
            reasoning: None,
            status: Some("interrupted".to_string()),
            error_message: None,
            token_count: Some(5),
            progress_seq: Some(1),
            reasoning_progress_seq: Some(0),
            materialized_message_sequence: None,
            materialized_at: None,
            created_at: Some("2026-05-20T10:30:02Z".to_string()),
            completed_at: None,
            interrupted_at: Some("2026-05-20T10:32:15Z".to_string()),
        }],
        messages: vec![AgentMessageRow {
            message_key: "msg-1".to_string(),
            session_id: Some("session-1".to_string()),
            request_id: None,
            requester_did: None,
            sequence: Some(1),
            role: Some("user".to_string()),
            content: Some(user_message_json("subagent task")),
            reasoning: None,
            timestamp: Some("2026-05-20T10:30:00Z".to_string()),
        }],
        tool_calls: vec![gents_protocol::row::AgentToolCallRow {
            partial_output_tail: None,
            partial_output_seq: None,
            tool_call_key: "tool-cascade-1".to_string(),
            session_id: Some("session-1".to_string()),
            request_id: None,
            requester_did: None,
            message_sequence: Some(2),
            tool_name: Some("read_file".to_string()),
            tool_call_id: Some("call-cascade-1".to_string()),
            args: Some("{\"path\":\"/tmp/foo\"}".to_string()),
            result: None,
            status: Some("cancelled".to_string()),
            lifecycle_state: Some("cancelled".to_string()),
            child_request_id: None,
            await_mode: None,
            cancel_policy: Some("cascade".to_string()),
            workflow_group_id: None,
            workflow_role: None,
            started_at: Some("2026-05-20T10:31:00Z".to_string()),
            deadline_at: None,
            completed_at: Some("2026-05-20T10:32:16Z".to_string()),
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

    let snapshot =
        build_session_snapshot_from_store(&store, "session-1", None).expect("session snapshot");

    let tool_group = snapshot
        .timeline_items
        .iter()
        .find_map(|item| match item {
            RenderedTimelineItem::ToolGroup { tools, .. } => Some(tools),
            _ => None,
        })
        .expect("timeline should contain a ToolGroup");
    let tool = tool_group
        .iter()
        .find(|t| t.tool_name == "read_file")
        .expect("read_file tool call");
    let tool_cancel_cause = tool
        .cancel_cause
        .as_ref()
        .expect("cascade-cancelled tool call should have a derived cancel_cause");
    assert_eq!(
        tool_cancel_cause.cause, "interrupted",
        "cascade-cancelled tool call cause should be 'interrupted'"
    );
    assert_eq!(
        tool_cancel_cause.source, "parentCascade",
        "cascade-cancelled tool call source should be 'parentCascade'"
    );
}

fn transcript_contract_store(case: &LeanTranscriptCase) -> ClientStore {
    ClientStore::from_rows(ClientStoreRows {
        requests: vec![AgentRequestRow {
            request_id: "req-1".to_string(),
            agent_did: Some("did:test:contract-agent".to_string()),
            requester_did: None,
            behavior_id: Some("contract-behavior".to_string()),
            session_id: Some("session-1".to_string()),
            retry_parent_request: None,
            retry_root_request: None,
            superseded_by_request: None,
            content: transcript_contract_request_content(case),
            temperature: None,
            top_p: None,
            top_k: None,
            seed: None,
            max_tokens: None,
            max_total_tokens: None,
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
        }],
        messages: transcript_contract_messages(case),
        tool_calls: transcript_contract_tool_calls(case),
        ..ClientStoreRows::default()
    })
}

fn transcript_contract_messages(case: &LeanTranscriptCase) -> Vec<AgentMessageRow> {
    match case.name.as_str() {
        "ordering_user_assistant_tool_result"
        | "dedupe_duplicate_reuses_sequence"
        | "completed_tool_pair_closed" => vec![
            transcript_message_row(
                "msg-user",
                1,
                "user",
                user_message_json(&format!("{} prompt", case.name)),
            ),
            transcript_message_row(
                "msg-assistant-tool",
                case.assistant_sequence,
                "assistant",
                transcript_assistant_tool_call_message_json(&transcript_contract_result_id(case)),
            ),
            transcript_message_row(
                "msg-tool-result",
                case.result_sequence,
                "user",
                transcript_tool_result_message_json(
                    &transcript_contract_result_id(case),
                    &format!("payload-{}", case.payload_hash),
                ),
            ),
        ],
        "distinct_result_ids_append_distinct_rows" => vec![
            transcript_message_row(
                "msg-seed-result",
                1,
                "user",
                transcript_tool_result_message_json(
                    "result-10",
                    &format!("payload-{}", case.payload_hash),
                ),
            ),
            transcript_message_row(
                "msg-distinct-result",
                case.result_sequence,
                "user",
                transcript_tool_result_message_json(
                    &transcript_contract_result_id(case),
                    &format!("payload-{}", case.payload_hash),
                ),
            ),
        ],
        "explicit_drain_terminalizes_ownership" => vec![transcript_message_row(
            "msg-drain-assistant-tool",
            case.assistant_sequence,
            "assistant",
            transcript_assistant_tool_call_message_json("result-drain"),
        )],
        "drop_abandon_not_strong_drain" => Vec::new(),
        "parallel_results_share_assistant_turn" => {
            let result_ids = transcript_contract_result_ids(case);
            let mut rows = vec![
                transcript_message_row(
                    "msg-user",
                    1,
                    "user",
                    user_message_json(&format!("{} prompt", case.name)),
                ),
                transcript_message_row(
                    "msg-assistant-parallel-tools",
                    case.assistant_sequence,
                    "assistant",
                    transcript_assistant_parallel_tool_call_message_json(&result_ids),
                ),
            ];
            for (index, result_id) in result_ids.iter().enumerate() {
                rows.push(transcript_message_row(
                    &format!("msg-tool-result-{index}"),
                    case.result_sequence + index,
                    "user",
                    transcript_tool_result_message_json(
                        result_id,
                        &format!("payload-{}", case.payload_hash),
                    ),
                ));
            }
            rows
        }
        other => panic!("unsupported Lean transcript case {other:?}"),
    }
}

fn transcript_contract_request_content(case: &LeanTranscriptCase) -> Option<String> {
    matches!(
        case.name.as_str(),
        "ordering_user_assistant_tool_result"
            | "dedupe_duplicate_reuses_sequence"
            | "completed_tool_pair_closed"
            | "parallel_results_share_assistant_turn"
    )
    .then(|| format!("{} prompt", case.name))
}

fn transcript_contract_tool_calls(
    case: &LeanTranscriptCase,
) -> Vec<gents_protocol::row::AgentToolCallRow> {
    let lifecycle_state = transcript_contract_tool_lifecycle(case);
    let result_ids = transcript_contract_result_ids(case);
    (0..case.post_tool_call_count)
        .map(|index| gents_protocol::row::AgentToolCallRow {
            partial_output_tail: None,
            partial_output_seq: None,
            tool_call_key: format!("tool-{}-{index}", case.name),
            session_id: Some("session-1".to_string()),
            request_id: Some("req-1".to_string()),
            requester_did: None,
            message_sequence: transcript_contract_tool_group_sequence(case),
            tool_name: Some("read".to_string()),
            tool_call_id: Some(result_ids[index].clone()),
            args: Some(r#"{"file_path":"/tmp/transcript-contract.txt"}"#.to_string()),
            result: (lifecycle_state == "completed")
                .then(|| format!("payload-{}", case.payload_hash)),
            status: Some(lifecycle_state.to_string()),
            lifecycle_state: Some(lifecycle_state.to_string()),
            child_request_id: None,
            await_mode: None,
            cancel_policy: None,
            workflow_group_id: None,
            workflow_role: None,
            started_at: Some("2026-04-21T12:00:01Z".to_string()),
            deadline_at: None,
            completed_at: (lifecycle_state != "running")
                .then(|| "2026-04-21T12:00:05Z".to_string()),
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
        })
        .collect()
}

fn transcript_message_row(
    message_key: &str,
    sequence: usize,
    role: &str,
    content: String,
) -> AgentMessageRow {
    AgentMessageRow {
        message_key: message_key.to_string(),
        session_id: Some("session-1".to_string()),
        request_id: None,
        requester_did: None,
        sequence: Some(sequence as i64),
        role: Some(role.to_string()),
        content: Some(content),
        reasoning: None,
        timestamp: Some("2026-04-21T12:00:00Z".to_string()),
    }
}

fn transcript_assistant_tool_call_message_json(model_call_id: &str) -> String {
    serde_json::to_string(&Message::Assistant {
        id: None,
        content: vec![AssistantContent::ToolCall(ToolCall {
            id: model_call_id.to_string(),
            call_id: Some(model_call_id.to_string()),
            function: ToolFunction {
                name: "read".to_string(),
                arguments: json!({ "file_path": "/tmp/transcript-contract.txt" }),
            },
            signature: None,
            additional_params: None,
        })],
    })
    .expect("serialize assistant tool-call message")
}

fn transcript_assistant_parallel_tool_call_message_json(call_ids: &[String]) -> String {
    serde_json::to_string(&Message::Assistant {
        id: None,
        content: call_ids
            .iter()
            .map(|call_id| {
                AssistantContent::ToolCall(ToolCall {
                    id: call_id.clone(),
                    call_id: Some(call_id.clone()),
                    function: ToolFunction {
                        name: "read".to_string(),
                        arguments: json!({ "file_path": "/tmp/transcript-contract.txt" }),
                    },
                    signature: None,
                    additional_params: None,
                })
            })
            .collect(),
    })
    .expect("serialize parallel assistant tool-call message")
}

fn transcript_tool_result_message_json(result_id: &str, text: &str) -> String {
    serde_json::to_string(&Message::User {
        content: vec![UserContent::ToolResult(ToolResult {
            id: result_id.to_string(),
            call_id: Some(result_id.to_string()),
            content: vec![ToolResultContent::Text(Text {
                text: text.to_string(),
            })],
        })],
    })
    .expect("serialize tool-result message")
}

fn transcript_contract_result_id(case: &LeanTranscriptCase) -> String {
    if case.logical_result_id == 0 {
        format!("result-{}", case.name)
    } else {
        format!("result-{}", case.logical_result_id)
    }
}

fn transcript_contract_result_ids(case: &LeanTranscriptCase) -> Vec<String> {
    if case.post_tool_call_count <= 1 {
        return vec![transcript_contract_result_id(case)];
    }
    (0..case.post_tool_call_count)
        .map(|index| format!("result-{}", case.logical_result_id as usize + index))
        .collect()
}

fn transcript_contract_tool_lifecycle(case: &LeanTranscriptCase) -> &'static str {
    match case.action.as_str() {
        "cancel_fail_or_timeout_in_flight" => "cancelled",
        "abandon_hook_ownership" => "running",
        _ if case.expected_pair_closed => "completed",
        _ => "running",
    }
}

fn transcript_contract_tool_group_sequence(case: &LeanTranscriptCase) -> Option<i64> {
    (case.assistant_sequence > 0).then_some(case.assistant_sequence as i64)
}

fn transcript_contract_tool_result_rows(case: &LeanTranscriptCase) -> usize {
    match case.name.as_str() {
        "ordering_user_assistant_tool_result"
        | "dedupe_duplicate_reuses_sequence"
        | "completed_tool_pair_closed" => 1,
        "distinct_result_ids_append_distinct_rows" => 2,
        "parallel_results_share_assistant_turn" => 3,
        "explicit_drain_terminalizes_ownership" | "drop_abandon_not_strong_drain" => 0,
        other => panic!("unsupported Lean transcript case {other:?}"),
    }
}

fn transcript_contract_rendered_kinds(case: &LeanTranscriptCase) -> Vec<&'static str> {
    match case.name.as_str() {
        "ordering_user_assistant_tool_result"
        | "dedupe_duplicate_reuses_sequence"
        | "completed_tool_pair_closed"
        | "parallel_results_share_assistant_turn" => vec!["user", "tools"],
        "distinct_result_ids_append_distinct_rows" => Vec::new(),
        "explicit_drain_terminalizes_ownership" | "drop_abandon_not_strong_drain" => {
            vec!["tools"]
        }
        other => panic!("unsupported Lean transcript case {other:?}"),
    }
}

fn contract_session_id(id: usize) -> String {
    format!("session-{id}")
}

fn contract_request_id(id: usize) -> String {
    format!("req-{id}")
}

fn client_shell_contract_store(case: &LeanClientShellCase) -> ClientStore {
    let session_id = contract_session_id(
        case.desktop_selected_session_id
            .expect("ClientShell desktop case should select a session"),
    );
    let observed_request_id = case.desktop_observed_request_id.map(contract_request_id);
    let turn_state = case.desktop_observed_turn_state.as_deref();
    let (request_status, lifecycle_state) = request_state_for_turn(turn_state);

    assert!(
        !case.frontend_conversation_present || case.desktop_snapshot_present,
        "case {} must not emit a conversation row without a desktop session observation",
        case.name
    );

    let mut rows = ClientStoreRows::default();

    if case.desktop_snapshot_present {
        rows.sessions.push(AgentSessionRow {
            session_id: session_id.clone(),
            agent_name: Some("Contract Agent".to_string()),
            requester_did: None,
            behavior_id: Some("contract-behavior".to_string()),
            started: Some("2026-04-21T12:00:00Z".to_string()),
            ended: None,
            status: Some("active".to_string()),
        });
    }

    if case.frontend_conversation_present {
        rows.conversations.push(AgentConversationRow {
            session_id: session_id.clone(),
            agent_name: Some("Contract Agent".to_string()),
            agent_did: Some("did:test:contract-agent".to_string()),
            requester_did: None,
            behavior_id: Some("contract-behavior".to_string()),
            title: Some("contract conversation".to_string()),
            title_source: Some("generated".to_string()),
            preview_text: Some("contract prompt".to_string()),
            status: Some("active".to_string()),
            created_at: Some("2026-04-21T12:00:00Z".to_string()),
            updated_at: Some("2026-04-21T12:01:00Z".to_string()),
            latest_request_id: observed_request_id.clone(),
        });
    }

    if let Some(request_id) = observed_request_id {
        rows.requests.push(AgentRequestRow {
            request_id: request_id.clone(),
            agent_did: Some("did:test:contract-agent".to_string()),
            requester_did: None,
            behavior_id: Some("contract-behavior".to_string()),
            session_id: Some(session_id),
            retry_parent_request: None,
            retry_root_request: None,
            superseded_by_request: None,
            content: Some("contract prompt".to_string()),
            temperature: None,
            top_p: None,
            top_k: None,
            seed: None,
            max_tokens: None,
            max_total_tokens: None,
            metadata: None,
            status: Some(request_status.to_string()),
            lifecycle_state: Some(lifecycle_state.to_string()),
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
        });

        if let Some(response_status) = response_status_for_turn(turn_state) {
            rows.responses.push(AgentResponseRow {
                response_key: format!("resp-{request_id}"),
                request_id: Some(request_id),
                agent_did: Some("did:test:contract-agent".to_string()),
                requester_did: None,
                behavior_id: Some("contract-behavior".to_string()),
                session_id: rows.requests.last().and_then(|row| row.session_id.clone()),
                content: Some("contract response".to_string()),
                reasoning: None,
                status: Some(response_status.to_string()),
                error_message: None,
                token_count: Some(12),
                progress_seq: Some(1),
                reasoning_progress_seq: Some(0),
                materialized_message_sequence: None,
                materialized_at: None,
                created_at: Some("2026-04-21T12:01:01Z".to_string()),
                completed_at: None,
                interrupted_at: None,
            });
        }
    }

    ClientStore::from_rows(rows)
}

fn request_state_for_turn(turn_state: Option<&str>) -> (&'static str, &'static str) {
    match turn_state {
        Some("waitingForClaim") => ("pending", "pending"),
        Some("streaming") => ("processing", "processing"),
        Some("completed") => ("completed", "completed"),
        Some("failed") => ("failed", "failed"),
        Some("superseded") => ("superseded", "superseded"),
        Some("interrupted") => ("interrupted", "interrupted"),
        Some(other) => panic!("unsupported Lean ClientShell turn state {other:?}"),
        None => ("pending", "pending"),
    }
}

fn response_status_for_turn(turn_state: Option<&str>) -> Option<&'static str> {
    match turn_state {
        Some("streaming") => Some("streaming"),
        Some("completed") => Some("complete"),
        Some("failed") => Some("error"),
        Some("waitingForClaim") | Some("superseded") | Some("interrupted") | None => None,
        Some(other) => panic!("unsupported Lean ClientShell turn state {other:?}"),
    }
}

fn streaming_response_contract_store(case: &LeanResponseTransitionCase) -> ClientStore {
    let (content, reasoning) = streaming_case_tail(case);
    let lifecycle_state = request_lifecycle_for_streaming_post_status(case.post_status.as_str());

    ClientStore::from_rows(ClientStoreRows {
        conversations: vec![AgentConversationRow {
            session_id: "session-1".to_string(),
            agent_name: Some("Contract Agent".to_string()),
            agent_did: Some("did:test:contract-agent".to_string()),
            requester_did: None,
            behavior_id: Some("contract-behavior".to_string()),
            title: Some("streaming response contract".to_string()),
            title_source: Some("generated".to_string()),
            preview_text: Some("contract prompt".to_string()),
            status: Some("active".to_string()),
            created_at: Some("2026-04-21T12:00:00Z".to_string()),
            updated_at: Some("2026-04-21T12:01:00Z".to_string()),
            latest_request_id: Some("req-1".to_string()),
        }],
        requests: vec![AgentRequestRow {
            request_id: "req-1".to_string(),
            agent_did: Some("did:test:contract-agent".to_string()),
            requester_did: None,
            behavior_id: Some("contract-behavior".to_string()),
            session_id: Some("session-1".to_string()),
            retry_parent_request: None,
            retry_root_request: None,
            superseded_by_request: None,
            content: Some("contract prompt".to_string()),
            temperature: None,
            top_p: None,
            top_k: None,
            seed: None,
            max_tokens: None,
            max_total_tokens: None,
            metadata: None,
            status: Some(request_status_for_lifecycle(lifecycle_state).to_string()),
            lifecycle_state: Some(lifecycle_state.to_string()),
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
        responses: vec![AgentResponseRow {
            response_key: "resp-1".to_string(),
            request_id: Some("req-1".to_string()),
            agent_did: Some("did:test:contract-agent".to_string()),
            requester_did: None,
            behavior_id: Some("contract-behavior".to_string()),
            session_id: Some("session-1".to_string()),
            content,
            reasoning,
            status: Some(case.post_status.clone()),
            error_message: case.error_reason.clone(),
            token_count: Some(case.post_token_count as i64),
            progress_seq: Some(1),
            reasoning_progress_seq: Some(0),
            materialized_message_sequence: case
                .post_materialized_seq
                .map(|sequence| sequence as i64),
            materialized_at: case
                .post_materialized_seq
                .map(|_| "2026-04-21T12:01:05Z".to_string()),
            created_at: Some("2026-04-21T12:00:01Z".to_string()),
            completed_at: matches!(case.post_status.as_str(), "complete" | "error")
                .then(|| "2026-04-21T12:01:05Z".to_string()),
            interrupted_at: (case.action == "set_interrupted_at")
                .then(|| "2026-04-21T12:01:03Z".to_string()),
        }],
        ..ClientStoreRows::default()
    })
}

fn streaming_case_should_render_live_overlay(case: &LeanResponseTransitionCase) -> bool {
    case.post_status == "streaming"
        && case.post_live_tail == "nonEmpty"
        && case.post_materialized_seq.is_none()
        && case.action != "set_interrupted_at"
}

fn streaming_case_tail(case: &LeanResponseTransitionCase) -> (Option<String>, Option<String>) {
    if case.post_live_tail != "nonEmpty" {
        return (None, None);
    }
    if case.action == "write_reasoning" {
        return (
            None,
            Some(format!(
                "{} reasoning live tail",
                case.name.replace('_', " ")
            )),
        );
    }
    (
        Some(format!("{} content live tail", case.name.replace('_', " "))),
        None,
    )
}

fn request_lifecycle_for_streaming_post_status(status: &str) -> &'static str {
    match status {
        "streaming" => "processing",
        "complete" => "completed",
        "error" => "failed",
        other => panic!("unsupported Lean streaming response post status {other:?}"),
    }
}

fn request_status_for_lifecycle(lifecycle_state: &str) -> &str {
    match lifecycle_state {
        "completed" => "completed",
        "failed" => "error",
        other => other,
    }
}
