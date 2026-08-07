use std::time::Duration;

use anyhow::{Context, Result};
use gents_desktop_core::client::{
    ClientCore, ClientCoreOptions, ClientStore, ClientStoreRows, DesktopPaths,
};
use gents_protocol::client_protocol::ClientTurnState;
use gents_protocol::row::{
    AgentConversationRow, AgentMessageRow, AgentPrincipalRow, AgentRequestRow, AgentResponseRow,
    AgentRuntimeRow, AgentSessionRow,
};
use tokio::time::{sleep, timeout};

#[test]
fn store_indexes_conversations_and_runtimes() {
    let store = ClientStore::from_rows(ClientStoreRows {
        agent_principals: vec![AgentPrincipalRow {
            agent_did: "did:test:amy".to_string(),
            display_name: Some("Amy".to_string()),
            default_behavior_id: None,
            enabled: Some(true),
            created_at: None,
            created_by: None,
        }],
        conversations: vec![
            AgentConversationRow {
                session_id: "session-2".to_string(),
                agent_name: None,
                agent_did: Some("did:test:amy".to_string()),
                requester_did: None,
                behavior_id: None,
                title: Some("Second".to_string()),
                title_source: None,
                preview_text: None,
                status: None,
                created_at: Some("2026-04-14T00:00:00Z".to_string()),
                updated_at: Some("2026-04-14T00:02:00Z".to_string()),
                latest_request_id: None,
            },
            AgentConversationRow {
                session_id: "session-1".to_string(),
                agent_name: None,
                agent_did: Some("did:test:amy".to_string()),
                requester_did: None,
                behavior_id: None,
                title: Some("First".to_string()),
                title_source: None,
                preview_text: None,
                status: None,
                created_at: Some("2026-04-14T00:00:00Z".to_string()),
                updated_at: Some("2026-04-14T00:03:00Z".to_string()),
                latest_request_id: None,
            },
        ],
        runtimes: vec![AgentRuntimeRow {
            agent_did: "did:test:amy".to_string(),
            process_state: Some("online".to_string()),
            reconcile_phase: None,
            active_generation: None,
            router_generation: None,
            default_behavior_id: None,
            runnable_behavior_count: Some(1),
            unavailable_behavior_count: Some(0),
            behavior_executor_capacity: None,
            behavior_executor_queue_depth: None,
            behavior_executor_status_json: None,
            last_reconcile_result: None,
            last_reconcile_error: None,
            last_reconcile_completed_at: None,
            updated_at: Some("2026-04-14T00:05:00Z".to_string()),
        }],
        ..ClientStoreRows::default()
    });

    let conversations = store.conversation_rows("did:test:amy");
    assert_eq!(conversations.len(), 2);
    assert_eq!(conversations[0].session_id, "session-1");
    assert_eq!(
        store
            .latest_runtime("did:test:amy")
            .and_then(|runtime| runtime.process_state.as_deref()),
        Some("online")
    );
}

#[test]
fn store_derives_turn_from_retry_chain_tip() {
    let store = ClientStore::from_rows(ClientStoreRows {
        requests: vec![
            AgentRequestRow {
                request_id: "req-1".to_string(),
                agent_did: Some("did:test:amy".to_string()),
                requester_did: None,
                behavior_id: None,
                session_id: Some("session-1".to_string()),
                retry_parent_request: None,
                retry_root_request: None,
                superseded_by_request: Some("req-2".to_string()),
                content: None,
                temperature: None,
                top_p: None,
                top_k: None,
                seed: None,
                max_tokens: None,
                metadata: None,
                status: Some("pending".to_string()),
                lifecycle_state: Some("completed".to_string()),
                backend_id: None,
                execution_origin: None,
                failure_reason: None,
                terminalized_at: None,
                terminal_redrive_attempts: None,
                created_at: Some("2026-04-14T00:00:00Z".to_string()),
                claimed_at: None,
                deadline: None,
                retry_count: None,
                max_retries: None,
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
                behavior_id: None,
                session_id: Some("session-1".to_string()),
                retry_parent_request: Some("req-1".to_string()),
                retry_root_request: Some("req-1".to_string()),
                superseded_by_request: None,
                content: None,
                temperature: None,
                top_p: None,
                top_k: None,
                seed: None,
                max_tokens: None,
                metadata: None,
                status: Some("processing".to_string()),
                lifecycle_state: Some("processing".to_string()),
                backend_id: None,
                execution_origin: None,
                failure_reason: None,
                terminalized_at: None,
                terminal_redrive_attempts: None,
                created_at: Some("2026-04-14T00:01:00Z".to_string()),
                claimed_at: None,
                deadline: None,
                retry_count: None,
                max_retries: None,
                caused_by_trigger_id: None,
                caused_by_trigger_kind: None,
                caused_by_parent_request_id: None,
                interrupt_requested_at: None,
                valid_until: None,
            },
        ],
        responses: vec![
            AgentResponseRow {
                response_key: "resp-1".to_string(),
                request_id: Some("req-2".to_string()),
                agent_did: None,
                requester_did: None,
                behavior_id: None,
                session_id: Some("session-1".to_string()),
                content: None,
                reasoning: None,
                status: Some("streaming".to_string()),
                error_message: None,
                token_count: None,
                progress_seq: Some(1),
                reasoning_progress_seq: Some(0),
                materialized_message_sequence: None,
                materialized_at: None,
                created_at: Some("2026-04-14T00:01:01Z".to_string()),
                completed_at: None,
                interrupted_at: None,
            },
            AgentResponseRow {
                response_key: "resp-2".to_string(),
                request_id: Some("req-2".to_string()),
                agent_did: None,
                requester_did: None,
                behavior_id: None,
                session_id: Some("session-1".to_string()),
                content: None,
                reasoning: None,
                status: Some("completed".to_string()),
                error_message: None,
                token_count: None,
                progress_seq: Some(2),
                reasoning_progress_seq: Some(0),
                materialized_message_sequence: None,
                materialized_at: None,
                created_at: Some("2026-04-14T00:01:02Z".to_string()),
                completed_at: Some("2026-04-14T00:01:03Z".to_string()),
                interrupted_at: None,
            },
        ],
        ..ClientStoreRows::default()
    });

    assert_eq!(
        store.derive_turn("session-1"),
        Some(ClientTurnState::Completed)
    );
}

#[test]
fn store_derives_turn_from_conversation_latest_request_not_random_request_id_order() {
    let store = ClientStore::from_rows(ClientStoreRows {
        conversations: vec![AgentConversationRow {
            session_id: "session-1".to_string(),
            agent_name: None,
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: None,
            title: Some("Turn ordering".to_string()),
            title_source: None,
            preview_text: None,
            status: None,
            created_at: Some("2026-04-14T00:00:00Z".to_string()),
            updated_at: Some("2026-04-14T00:03:00Z".to_string()),
            latest_request_id: Some("req-a-complete".to_string()),
        }],
        requests: vec![
            AgentRequestRow {
                request_id: "req-z-still-processing".to_string(),
                agent_did: Some("did:test:amy".to_string()),
                requester_did: None,
                behavior_id: None,
                session_id: Some("session-1".to_string()),
                retry_parent_request: None,
                retry_root_request: None,
                superseded_by_request: None,
                content: None,
                temperature: None,
                top_p: None,
                top_k: None,
                seed: None,
                max_tokens: None,
                metadata: None,
                status: Some("processing".to_string()),
                lifecycle_state: Some("processing".to_string()),
                backend_id: None,
                execution_origin: None,
                failure_reason: None,
                terminalized_at: None,
                terminal_redrive_attempts: None,
                created_at: Some("2026-04-14T00:01:00Z".to_string()),
                claimed_at: None,
                deadline: None,
                retry_count: None,
                max_retries: None,
                caused_by_trigger_id: None,
                caused_by_trigger_kind: None,
                caused_by_parent_request_id: None,
                interrupt_requested_at: None,
                valid_until: None,
            },
            AgentRequestRow {
                request_id: "req-a-complete".to_string(),
                agent_did: Some("did:test:amy".to_string()),
                requester_did: None,
                behavior_id: None,
                session_id: Some("session-1".to_string()),
                retry_parent_request: None,
                retry_root_request: None,
                superseded_by_request: None,
                content: None,
                temperature: None,
                top_p: None,
                top_k: None,
                seed: None,
                max_tokens: None,
                metadata: None,
                status: Some("completed".to_string()),
                lifecycle_state: Some("completed".to_string()),
                backend_id: None,
                execution_origin: None,
                failure_reason: None,
                terminalized_at: None,
                terminal_redrive_attempts: None,
                created_at: Some("2026-04-14T00:02:00Z".to_string()),
                claimed_at: None,
                deadline: None,
                retry_count: None,
                max_retries: None,
                caused_by_trigger_id: None,
                caused_by_trigger_kind: None,
                caused_by_parent_request_id: None,
                interrupt_requested_at: None,
                valid_until: None,
            },
        ],
        responses: vec![AgentResponseRow {
            response_key: "resp-a-complete".to_string(),
            request_id: Some("req-a-complete".to_string()),
            agent_did: None,
            requester_did: None,
            behavior_id: None,
            session_id: Some("session-1".to_string()),
            content: Some("done".to_string()),
            reasoning: None,
            status: Some("completed".to_string()),
            error_message: None,
            token_count: None,
            progress_seq: Some(1),
            reasoning_progress_seq: Some(0),
            materialized_message_sequence: None,
            materialized_at: None,
            created_at: Some("2026-04-14T00:02:01Z".to_string()),
            completed_at: Some("2026-04-14T00:02:02Z".to_string()),
            interrupted_at: None,
        }],
        ..ClientStoreRows::default()
    });

    assert_eq!(
        store.derive_turn("session-1"),
        Some(ClientTurnState::Completed)
    );
}

#[test]
fn focused_request_id_defaults_to_none() {
    let (observed_store, _rx) =
        gents_desktop_core::client::ObservedStore::new(ClientStore::default());
    assert!(observed_store.focused_request_id().is_none());
}

#[test]
fn chat_patch_merge_updates_one_agent_without_dropping_other_agent_rows() {
    let base = ClientStore::from_rows(ClientStoreRows {
        conversations: vec![
            AgentConversationRow {
                session_id: "session-1".to_string(),
                agent_name: Some("Amy".to_string()),
                agent_did: Some("did:test:amy".to_string()),
                requester_did: None,
                behavior_id: Some("amy-default".to_string()),
                title: Some("Old title".to_string()),
                title_source: None,
                preview_text: None,
                status: None,
                created_at: Some("2026-04-14T00:00:00Z".to_string()),
                updated_at: Some("2026-04-14T00:01:00Z".to_string()),
                latest_request_id: Some("req-1".to_string()),
            },
            AgentConversationRow {
                session_id: "session-1".to_string(),
                agent_name: Some("Bea".to_string()),
                agent_did: Some("did:test:bea".to_string()),
                requester_did: None,
                behavior_id: Some("bea-default".to_string()),
                title: Some("Other agent".to_string()),
                title_source: None,
                preview_text: None,
                status: None,
                created_at: Some("2026-04-14T00:00:00Z".to_string()),
                updated_at: Some("2026-04-14T00:02:00Z".to_string()),
                latest_request_id: Some("bea-req-1".to_string()),
            },
        ],
        messages: vec![AgentMessageRow {
            message_key: "session-1:1".to_string(),
            session_id: Some("session-1".to_string()),
            request_id: None,
            requester_did: None,
            sequence: Some(1),
            role: Some("user".to_string()),
            content: Some("old".to_string()),
            reasoning: None,
            timestamp: Some("2026-04-14T00:01:00Z".to_string()),
        }],
        message_source_agent_dids: vec![Some("did:test:amy".to_string())],
        sessions: vec![AgentSessionRow {
            session_id: "session-1".to_string(),
            agent_name: Some("Bea".to_string()),
            requester_did: None,
            behavior_id: Some("bea-default".to_string()),
            started: Some("2026-04-14T00:00:00Z".to_string()),
            ended: None,
            status: Some("active".to_string()),
        }],
        session_source_agent_dids: vec![Some("did:test:bea".to_string())],
        ..ClientStoreRows::default()
    });

    let mut patch = ClientStore::from_rows(ClientStoreRows {
        conversations: vec![AgentConversationRow {
            session_id: "session-1".to_string(),
            agent_name: Some("Amy".to_string()),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            title: Some("Updated title".to_string()),
            title_source: None,
            preview_text: Some("new preview".to_string()),
            status: None,
            created_at: Some("2026-04-14T00:00:00Z".to_string()),
            updated_at: Some("2026-04-14T00:03:00Z".to_string()),
            latest_request_id: Some("req-2".to_string()),
        }],
        requests: vec![AgentRequestRow {
            request_id: "req-2".to_string(),
            agent_did: Some("did:test:amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            session_id: Some("session-1".to_string()),
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
            status: Some("completed".to_string()),
            lifecycle_state: Some("completed".to_string()),
            backend_id: None,
            execution_origin: None,
            caused_by_trigger_id: None,
            caused_by_trigger_kind: None,
            caused_by_parent_request_id: None,
            failure_reason: None,
            terminalized_at: None,
            terminal_redrive_attempts: None,
            created_at: Some("2026-04-14T00:02:00Z".to_string()),
            claimed_at: None,
            deadline: None,
            retry_count: None,
            max_retries: None,
            interrupt_requested_at: None,
            valid_until: None,
        }],
        messages: vec![AgentMessageRow {
            message_key: "session-1:1".to_string(),
            session_id: Some("session-1".to_string()),
            request_id: None,
            requester_did: None,
            sequence: Some(1),
            role: Some("user".to_string()),
            content: Some("new".to_string()),
            reasoning: None,
            timestamp: Some("2026-04-14T00:02:00Z".to_string()),
        }],
        sessions: vec![AgentSessionRow {
            session_id: "session-1".to_string(),
            agent_name: Some("Amy".to_string()),
            requester_did: None,
            behavior_id: Some("amy-default".to_string()),
            started: Some("2026-04-14T00:00:00Z".to_string()),
            ended: None,
            status: Some("active".to_string()),
        }],
        ..ClientStoreRows::default()
    });
    patch.stamp_source_agent_did("did:test:amy");

    let merged = base.merge_chat_patch(patch);

    assert_eq!(merged.conversations.len(), 2);
    assert_eq!(
        merged
            .conversation_rows("did:test:amy")
            .first()
            .and_then(|row| row.title.as_deref()),
        Some("Updated title")
    );
    assert_eq!(
        merged
            .conversation_rows("did:test:bea")
            .first()
            .and_then(|row| row.title.as_deref()),
        Some("Other agent")
    );
    assert_eq!(merged.messages.len(), 1);
    assert_eq!(merged.messages[0].content.as_deref(), Some("new"));
    assert_eq!(merged.sessions.len(), 2);
    assert!(merged
        .session_source_agent_dids
        .iter()
        .any(|source| source.as_deref() == Some("did:test:amy")));
    assert!(merged
        .session_source_agent_dids
        .iter()
        .any(|source| source.as_deref() == Some("did:test:bea")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observer_loads_initial_snapshot_and_ticks_on_update() -> Result<()> {
    let tempdir = tempfile::tempdir()?;
    let paths = DesktopPaths::from_root(tempdir.path());
    let core =
        ClientCore::start_with_paths_and_options(paths, ClientCoreOptions::local_only()).await?;
    let store = core.store();
    let mut updates = core.store_updates();

    assert_eq!(store.snapshot().agent_principals.len(), 0);

    let response = core
        .node()
        .execute(
            r#"mutation {
                add_AgentPrincipal(input: {
                    agent_did: "did:test:test-agent"
                    display_name: "Test Agent"
                    enabled: true
                }) { agent_did }
            }"#,
        )
        .await;
    assert!(
        !response.has_errors(),
        "agent principal mutation should succeed"
    );

    let baseline = *updates.borrow_and_update();
    timeout(Duration::from_secs(5), async {
        loop {
            updates.changed().await.context("watch channel closed")?;
            if *updates.borrow() > baseline {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await
    .context("timed out waiting for store update")??;

    timeout(Duration::from_secs(5), async {
        loop {
            if store.snapshot().agent_principals.len() == 1 {
                return Ok::<(), anyhow::Error>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("timed out waiting for refreshed snapshot")??;

    assert_eq!(
        store.snapshot().agent_principals[0].agent_did,
        "did:test:test-agent"
    );
    core.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_then_observer_no_lost_writes() -> Result<()> {
    let tmp = tempfile::TempDir::new().expect("tmpdir");
    let paths = DesktopPaths::from_root(tmp.path());

    let core =
        ClientCore::start_with_paths_and_options(paths, ClientCoreOptions::local_only()).await?;

    for i in 0..20usize {
        let mutation = format!(
            r#"mutation {{
                add_AgentPrincipal(input: {{
                    agent_did: "did:race-{i}",
                    display_name: "race-{i}",
                    enabled: true
                }}) {{ agent_did }}
            }}"#
        );
        let response = core.node().execute(&mutation).await;
        assert!(
            !response.has_errors(),
            "mutation {i} failed: {:?}",
            response.errors
        );
    }

    timeout(Duration::from_secs(5), async {
        loop {
            if core.store().snapshot().agent_principals.len() >= 20 {
                return Ok::<(), anyhow::Error>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("timed out waiting for all 20 principals to appear in store")??;

    let snap = core.store().snapshot();
    let dids: std::collections::HashSet<&str> = snap
        .agent_principals
        .iter()
        .map(|p| p.agent_did.as_str())
        .collect();
    for i in 0..20usize {
        let want = format!("did:race-{i}");
        assert!(dids.contains(want.as_str()), "missing {want}");
    }

    core.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incremental_observer_handles_long_session() -> Result<()> {
    let tmp = tempfile::TempDir::new().expect("tmpdir");
    let paths = DesktopPaths::from_root(tmp.path());
    let core =
        ClientCore::start_with_paths_and_options(paths, ClientCoreOptions::local_only()).await?;

    for i in 0..1_000usize {
        let mutation = format!(
            r#"mutation {{
                create_AgentMessage(input: {{
                    message_key: "long:{i}",
                    session_id: "long",
                    sequence: {i},
                    role: "user",
                    content: "msg",
                    timestamp: "2026-05-07T00:00:00Z"
                }}) {{ _docID }}
            }}"#
        );
        let resp = core.node().execute(&mutation).await;
        assert!(!resp.has_errors(), "seed message {i}: {:?}", resp.errors);
    }

    let resp = core
        .node()
        .execute(
            r#"mutation {
                create_AgentResponse(input: {
                    response_key: "long-req",
                    request_id: "long-req",
                    agent_did: "did:long",
                    behavior_id: "default",
                    session_id: "long",
                    content: "",
                    reasoning: "",
                    status: "streaming",
                    error_message: "",
                    token_count: 0,
                    progress_seq: 0,
                    created_at: "2026-05-07T00:00:00Z"
                }) { _docID }
            }"#,
        )
        .await;
    assert!(!resp.has_errors(), "seed response: {:?}", resp.errors);

    timeout(Duration::from_millis(2000), async {
        loop {
            let snap = core.store().snapshot();
            if snap
                .messages
                .iter()
                .filter(|m| m.session_id.as_deref() == Some("long"))
                .count()
                >= 1_000
                && snap.responses.iter().any(|r| r.response_key == "long-req")
            {
                return Ok::<(), anyhow::Error>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("timed out waiting for seed to drain")??;

    let baseline = core
        .observer_metrics()
        .await
        .expect("observer running and exposing metrics");

    for i in 1..=100usize {
        let mutation = format!(
            r#"mutation {{
                update_AgentResponse(
                    filter: {{ response_key: {{ _eq: "long-req" }} }},
                    input: {{ progress_seq: {i} }}
                ) {{ _docID }}
            }}"#
        );
        let resp = core.node().execute(&mutation).await;
        assert!(!resp.has_errors(), "update {i}: {:?}", resp.errors);
    }

    timeout(Duration::from_millis(2000), async {
        loop {
            let snap = core.store().snapshot();
            if snap
                .responses
                .iter()
                .any(|r| r.response_key == "long-req" && r.progress_seq == Some(100))
            {
                return Ok::<(), anyhow::Error>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("timed out waiting for progress_seq=100")??;

    let snap = core.store().snapshot();
    let resp = snap
        .responses
        .iter()
        .find(|r| r.response_key == "long-req")
        .expect("response present");
    assert_eq!(
        resp.progress_seq,
        Some(100),
        "final progress_seq should be 100"
    );

    let after = core
        .observer_metrics()
        .await
        .expect("observer running and exposing metrics");
    let streaming_docs_fetched = after.docs_fetched.saturating_sub(baseline.docs_fetched);
    assert!(
        streaming_docs_fetched < 50,
        "streaming phase fetched too many rows; got {streaming_docs_fetched}"
    );

    core.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agent_scope_isolation_under_drop_recovery() -> Result<()> {
    let tmp = tempfile::TempDir::new().expect("tmpdir");
    let paths = DesktopPaths::from_root(tmp.path());
    let core =
        ClientCore::start_with_paths_and_options(paths, ClientCoreOptions::local_only()).await?;

    for did in &["did:alpha", "did:beta"] {
        let mutation = format!(
            r#"mutation {{
                create_AgentPrincipal(input: {{
                    agent_did: "{did}",
                    display_name: "{did}",
                    default_behavior_id: "default",
                    enabled: true,
                    created_at: "2026-05-07T00:00:00Z",
                    created_by: "test"
                }}) {{ _docID }}
            }}"#
        );
        let resp = core.node().execute(&mutation).await;
        assert!(!resp.has_errors(), "seed {did}: {:?}", resp.errors);
    }

    core.set_selected_agent_did(Some("did:alpha".to_string()));

    timeout(Duration::from_secs(5), async {
        loop {
            let snap = core.store().snapshot();
            let dids: Vec<&str> = snap
                .agent_principals
                .iter()
                .map(|p| p.agent_did.as_str())
                .collect();
            if dids.contains(&"did:alpha") && dids.contains(&"did:beta") {
                return Ok::<(), anyhow::Error>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("timed out waiting for both agents in store")??;

    let snap = core.store().snapshot();
    let dids: Vec<&str> = snap
        .agent_principals
        .iter()
        .map(|p| p.agent_did.as_str())
        .collect();
    assert!(dids.contains(&"did:alpha"), "did:alpha missing");
    assert!(dids.contains(&"did:beta"), "did:beta missing");

    core.shutdown().await?;
    Ok(())
}
