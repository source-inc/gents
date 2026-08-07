use super::*;

#[path = "../../../../../crates/gents/src/lean_vocab_test/support.rs"]
mod lean_vocab_test;

use crate::types::EventTriggerView;
use lean_vocab_test::{lean_trigger_dispatch_case_count, lean_trigger_dispatch_cases};

#[test]
fn conversation_summaries_keep_newest_row_per_session() {
    let mut conversations = vec![
        conversation_summary("session-1", "req-3", "2026-04-21T12:03:00Z"),
        conversation_summary("session-2", "req-a", "2026-04-21T12:02:00Z"),
        conversation_summary("session-1", "req-2", "2026-04-21T12:01:00Z"),
    ];

    retain_latest_conversation_summaries(&mut conversations);

    assert_eq!(conversations.len(), 2);
    assert_eq!(conversations[0].session_id, "session-1");
    assert_eq!(conversations[0].latest_request_id.as_deref(), Some("req-3"));
    assert_eq!(conversations[1].session_id, "session-2");
}

#[test]
fn request_backed_conversation_summaries_include_in_flight_sessions() {
    let store = ClientStore::from_rows(ClientStoreRows {
        requests: vec![AgentRequestRow {
            request_id: "req-live".to_string(),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            session_id: Some("session-live".to_string()),
            retry_parent_request: None,
            retry_root_request: None,
            superseded_by_request: None,
            content: Some("inspect your environment".to_string()),
            temperature: None,
            top_p: None,
            top_k: None,
            seed: None,
            max_tokens: None,
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

    let summaries =
        request_backed_conversation_summaries(&store, "did:test:amy", true, &[], &[], &[]);

    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].session_id, "session-live");
    assert_eq!(summaries[0].latest_request_id.as_deref(), Some("req-live"));
    assert_eq!(
        summaries[0].preview_text.as_deref(),
        Some("inspect your environment")
    );
    assert_eq!(summaries[0].turn_state.as_deref(), Some("waitingForClaim"));
}

#[test]
fn conversation_task_tag_uses_latest_schedule_lineage() {
    let store = ClientStore::from_rows(ClientStoreRows {
        requests: vec![
            AgentRequestRow {
                request_id: "req-old".to_string(),
                agent_did: Some("did:test:amy".to_string()),
                requester_did: None,
                behavior_id: Some("amy-default".to_string()),
                session_id: Some("session-1".to_string()),
                retry_parent_request: None,
                retry_root_request: None,
                superseded_by_request: None,
                content: Some("old".to_string()),
                temperature: None,
                top_p: None,
                top_k: None,
                seed: None,
                max_tokens: None,
                metadata: None,
                status: Some("completed".to_string()),
                lifecycle_state: Some("completed".to_string()),
                backend_id: None,
                execution_origin: Some("scheduled".to_string()),
                failure_reason: None,
                terminalized_at: None,
                terminal_redrive_attempts: None,
                created_at: Some("2026-04-21T12:00:00Z".to_string()),
                claimed_at: None,
                deadline: None,
                retry_count: Some(0),
                max_retries: Some(3),
                caused_by_trigger_id: Some("sched-old".to_string()),
                caused_by_trigger_kind: Some("schedule".to_string()),
                caused_by_parent_request_id: None,
                interrupt_requested_at: None,
                valid_until: None,
            },
            AgentRequestRow {
                request_id: "req-new".to_string(),
                agent_did: Some("did:test:amy".to_string()),
                requester_did: None,
                behavior_id: Some("amy-default".to_string()),
                session_id: Some("session-1".to_string()),
                retry_parent_request: None,
                retry_root_request: None,
                superseded_by_request: None,
                content: Some("new".to_string()),
                temperature: None,
                top_p: None,
                top_k: None,
                seed: None,
                max_tokens: None,
                metadata: None,
                status: Some("completed".to_string()),
                lifecycle_state: Some("completed".to_string()),
                backend_id: None,
                execution_origin: Some("scheduled".to_string()),
                failure_reason: None,
                terminalized_at: None,
                terminal_redrive_attempts: None,
                created_at: Some("2026-04-21T12:02:00Z".to_string()),
                claimed_at: None,
                deadline: None,
                retry_count: Some(0),
                max_retries: Some(3),
                caused_by_trigger_id: Some("sched-new".to_string()),
                caused_by_trigger_kind: Some("schedule".to_string()),
                caused_by_parent_request_id: None,
                interrupt_requested_at: None,
                valid_until: None,
            },
        ],
        ..ClientStoreRows::default()
    });
    let tasks = vec![
        task_view("task-old", "Old task"),
        task_view("task-new", "Freshness check"),
    ];
    let schedules = vec![
        schedule_view("sched-old", "task-old"),
        schedule_view("sched-new", "task-new"),
    ];

    let tag = conversation_task_tag(
        &store,
        "did:test:amy",
        true,
        "session-1",
        &tasks,
        &schedules,
        &[],
    )
    .expect("task tag");

    assert_eq!(tag.task_id, "task-new");
    assert_eq!(tag.task_name.as_deref(), Some("Freshness check"));
    assert_eq!(tag.trigger_id.as_deref(), Some("sched-new"));
    assert_eq!(tag.trigger_kind.as_deref(), Some("schedule"));
}

#[test]
fn task_run_history_is_agent_scoped_when_trigger_ids_match() {
    let store = ClientStore::from_rows(ClientStoreRows {
        requests: vec![
            AgentRequestRow {
                request_id: "req-mini-1".to_string(),
                agent_did: Some("did:test:mini-1".to_string()),
                requester_did: None,
                behavior_id: Some("default".to_string()),
                session_id: Some("session-mini-1".to_string()),
                retry_parent_request: None,
                retry_root_request: None,
                superseded_by_request: None,
                content: Some("run task".to_string()),
                temperature: None,
                top_p: None,
                top_k: None,
                seed: None,
                max_tokens: None,
                metadata: None,
                status: Some("completed".to_string()),
                lifecycle_state: Some("completed".to_string()),
                backend_id: None,
                execution_origin: Some("scheduled".to_string()),
                failure_reason: None,
                terminalized_at: None,
                terminal_redrive_attempts: None,
                created_at: Some("2026-04-21T12:00:00Z".to_string()),
                claimed_at: None,
                deadline: None,
                retry_count: Some(0),
                max_retries: Some(3),
                caused_by_trigger_id: Some("shared-schedule".to_string()),
                caused_by_trigger_kind: Some("schedule".to_string()),
                caused_by_parent_request_id: None,
                interrupt_requested_at: None,
                valid_until: None,
            },
            AgentRequestRow {
                request_id: "req-mini-2".to_string(),
                agent_did: Some("did:test:mini-2".to_string()),
                requester_did: None,
                behavior_id: Some("default".to_string()),
                session_id: Some("session-mini-2".to_string()),
                retry_parent_request: None,
                retry_root_request: None,
                superseded_by_request: None,
                content: Some("run task".to_string()),
                temperature: None,
                top_p: None,
                top_k: None,
                seed: None,
                max_tokens: None,
                metadata: None,
                status: Some("completed".to_string()),
                lifecycle_state: Some("completed".to_string()),
                backend_id: None,
                execution_origin: Some("scheduled".to_string()),
                failure_reason: None,
                terminalized_at: None,
                terminal_redrive_attempts: None,
                created_at: Some("2026-04-21T12:01:00Z".to_string()),
                claimed_at: None,
                deadline: None,
                retry_count: Some(0),
                max_retries: Some(3),
                caused_by_trigger_id: Some("shared-schedule".to_string()),
                caused_by_trigger_kind: Some("schedule".to_string()),
                caused_by_parent_request_id: None,
                interrupt_requested_at: None,
                valid_until: None,
            },
        ],
        ..ClientStoreRows::default()
    });
    let schedules = vec![schedule_view("shared-schedule", "task-1")];

    let runs = task_run_history(&store, "did:test:mini-1", true, "task-1", &schedules, &[]);

    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].request_id, "req-mini-1");
}

#[test]
fn task_recent_runs_view_consumes_generated_trigger_dispatch_lineage_contract_cases() {
    let cases = lean_trigger_dispatch_cases();
    assert!(
        !cases.is_empty(),
        "Lean trigger dispatch contract should emit recent-runs cases"
    );
    assert_eq!(
        cases.len(),
        lean_trigger_dispatch_case_count(),
        "Lean trigger dispatch case-count sentinel drifted"
    );

    let fired_task_cases = cases
        .iter()
        .filter(|case| {
            case.expected_result == "fired"
                && matches!(
                    case.expected_request_caused_by_kind.as_deref(),
                    Some("schedule" | "event")
                )
        })
        .collect::<Vec<_>>();

    assert!(
        fired_task_cases
            .iter()
            .any(|case| case.expected_request_caused_by_kind.as_deref() == Some("schedule")),
        "Lean trigger dispatch contract must include a fired schedule lineage case"
    );
    assert!(
        fired_task_cases
            .iter()
            .any(|case| case.expected_request_caused_by_kind.as_deref() == Some("event")),
        "Lean trigger dispatch contract must include a fired event lineage case"
    );

    for (index, case) in fired_task_cases.into_iter().enumerate() {
        let task_id = format!("contract-task-{}", case.name);
        let request_id = format!("contract-req-{index}");
        let created_at = format!("2026-04-21T12:{index:02}:00Z");
        let trigger_id = case
            .expected_request_caused_by_id
            .as_deref()
            .expect("fired schedule/event cases carry a trigger id");
        let trigger_kind = case
            .expected_request_caused_by_kind
            .as_deref()
            .expect("fired schedule/event cases carry a trigger kind");

        assert_eq!(
            case.expected_materialize_trigger_id.as_deref(),
            Some(trigger_id),
            "case {} materialized trigger id should feed recent-runs lineage",
            case.name
        );
        assert_eq!(
            case.expected_materialize_trigger_kind.as_deref(),
            Some(trigger_kind),
            "case {} materialized trigger kind should feed recent-runs lineage",
            case.name
        );

        let schedules = if trigger_kind == "schedule" {
            vec![ScheduleView {
                schedule_id: trigger_id.to_string(),
                task_id: Some(task_id.clone()),
                interval_secs: Some(60),
                cron: None,
                timezone: None,
                missed_run_policy: None,
                enabled: Some(true),
                concurrency: Some(case.concurrency.clone()),
                next_run_at: None,
                last_attempt_at: Some(created_at.clone()),
                last_status: Some("completed".to_string()),
                last_error: None,
                fire_count: Some(1),
            }]
        } else {
            Vec::new()
        };
        let event_triggers = if trigger_kind == "event" {
            vec![EventTriggerView {
                trigger_id: trigger_id.to_string(),
                task_id: Some(task_id.clone()),
                source_collection: Some("WebhookEvent".to_string()),
                event_kind: Some("created".to_string()),
                filter: None,
                enabled: Some(true),
                concurrency: Some(case.concurrency.clone()),
                last_attempt_at: Some(created_at.clone()),
                last_fired_source_doc_id: Some("source-doc-1".to_string()),
                last_status: Some("completed".to_string()),
                last_error: None,
                fire_count: Some(1),
            }]
        } else {
            Vec::new()
        };
        let store = ClientStore::from_rows(ClientStoreRows {
            requests: vec![AgentRequestRow {
                request_id: request_id.clone(),
                agent_did: Some("did:test:contract-agent".to_string()),
                requester_did: None,
                behavior_id: Some("contract-behavior".to_string()),
                session_id: Some(format!("contract-session-{index}")),
                retry_parent_request: None,
                retry_root_request: None,
                superseded_by_request: None,
                content: Some("contract prompt".to_string()),
                temperature: None,
                top_p: None,
                top_k: None,
                seed: None,
                max_tokens: None,
                metadata: None,
                status: Some("completed".to_string()),
                lifecycle_state: Some("completed".to_string()),
                backend_id: None,
                execution_origin: case.expected_execution_origin.clone(),
                failure_reason: None,
                terminalized_at: None,
                terminal_redrive_attempts: None,
                created_at: Some(created_at.clone()),
                claimed_at: None,
                deadline: None,
                retry_count: Some(0),
                max_retries: Some(3),
                caused_by_trigger_id: Some(trigger_id.to_string()),
                caused_by_trigger_kind: Some(trigger_kind.to_string()),
                caused_by_parent_request_id: None,
                interrupt_requested_at: None,
                valid_until: None,
            }],
            ..ClientStoreRows::default()
        });

        let recent_runs = recent_runs_for_task_views(&schedules, &event_triggers, &task_id);
        assert_eq!(
            recent_runs.total_fires, 1,
            "case {} should project one recent fire",
            case.name
        );
        assert_eq!(
            recent_runs.last_attempt_at.as_deref(),
            Some(created_at.as_str()),
            "case {} should surface the trigger bookkeeping timestamp",
            case.name
        );
        assert_eq!(
            recent_runs.schedule_count,
            usize::from(trigger_kind == "schedule"),
            "case {} schedule count drifted",
            case.name
        );
        assert_eq!(
            recent_runs.event_trigger_count,
            usize::from(trigger_kind == "event"),
            "case {} event trigger count drifted",
            case.name
        );

        let run_history = task_run_history(
            &store,
            "did:test:contract-agent",
            true,
            &task_id,
            &schedules,
            &event_triggers,
        );
        assert_eq!(
            run_history.len(),
            1,
            "case {} should project one recent-runs history row",
            case.name
        );
        let run = &run_history[0];
        assert_eq!(run.request_id, request_id);
        assert_eq!(run.caused_by_trigger_id.as_deref(), Some(trigger_id));
        assert_eq!(run.caused_by_trigger_kind.as_deref(), Some(trigger_kind));
        assert_eq!(
            run.execution_origin.as_deref(),
            case.expected_execution_origin.as_deref(),
            "case {} should preserve Lean trigger execution origin",
            case.name
        );
    }
}
