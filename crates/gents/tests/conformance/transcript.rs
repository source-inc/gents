use super::*;

fn transcript_user_message(text: &str) -> Message {
    Message::User {
        content: vec![UserContent::Text(Text {
            text: text.to_string(),
        })],
    }
}

fn transcript_assistant_tool_call_message(model_call_id: &str) -> Message {
    transcript_assistant_tool_calls_message(&[model_call_id])
}

fn transcript_assistant_tool_calls_message(model_call_ids: &[&str]) -> Message {
    Message::Assistant {
        id: None,
        content: model_call_ids
            .iter()
            .map(|model_call_id| {
                AssistantContent::ToolCall(ToolCall {
                    id: model_call_id.to_string(),
                    call_id: Some(model_call_id.to_string()),
                    function: ToolFunction {
                        name: "read".to_string(),
                        arguments: json!({ "file_path": "/tmp/transcript-contract.txt" }),
                    },
                    signature: None,
                    additional_params: None,
                })
            })
            .collect(),
    }
}

fn transcript_tool_result_message(result_id: &str, text: &str) -> Message {
    Message::User {
        content: vec![UserContent::ToolResult(ToolResult {
            id: result_id.to_string(),
            call_id: Some(result_id.to_string()),
            content: vec![ToolResultContent::Text(Text {
                text: text.to_string(),
            })],
        })],
    }
}

async fn transcript_hook_fixture(test_name: &str) -> (support::TestDb, DefraSessionHook, String) {
    let db = test_db(test_name).await;
    let session_id = format!("{test_name}-session");
    support::create_agent_session(
        db.node.as_ref(),
        &session_id,
        AGENT_NAME,
        "2026-05-01T00:00:00Z",
    )
    .await;
    let hook = DefraSessionHook::resume_with_identity_policy(
        db.node.clone(),
        &session_id,
        AGENT_NAME,
        AGENT_DID,
        FailurePolicy::default(),
    )
    .await
    .expect("resume transcript hook");
    hook.set_active_request_id(Some(format!("{test_name}-request")))
        .await;
    hook.set_request_deadline_at(Some(chrono::Utc::now() + chrono::Duration::minutes(5)))
        .await;
    (db, hook, session_id)
}

async fn transcript_messages_and_calls(
    node: &EmbeddedNode,
    session_id: &str,
) -> (Vec<MessageSnapshot>, Vec<ToolCallSnapshot>, Vec<Message>) {
    let messages = fetch_message_snapshots_for_session(node, session_id).await;
    let tool_calls = fetch_tool_call_snapshots_for_session(node, session_id).await;
    let history = gents::load_history(node, session_id)
        .await
        .expect("load transcript history");
    (messages, tool_calls, history)
}

fn transcript_tool_result_count(history: &[Message]) -> usize {
    history
        .iter()
        .filter(|message| {
            matches!(
                message,
                Message::User { content }
                    if matches!(content.first().expect("non-empty content"), UserContent::ToolResult(_))
            )
        })
        .count()
}

fn transcript_ordered(messages: &[MessageSnapshot]) -> bool {
    messages
        .windows(2)
        .all(|window| window[0].sequence < window[1].sequence)
}

fn transcript_strong_drain(tool_calls: &[ToolCallSnapshot]) -> bool {
    tool_calls
        .iter()
        .all(|call| call.lifecycle_state.as_deref() != Some("running"))
}

fn transcript_pair_closed(
    messages: &[MessageSnapshot],
    tool_calls: &[ToolCallSnapshot],
    history: &[Message],
) -> bool {
    let tool_calls_reserved_by_assistant_message = tool_calls.iter().all(|call| {
        messages.iter().any(|message| {
            message.sequence == call.message_sequence && message.role.as_str() == "assistant"
        })
    });
    let no_running_tool_calls = transcript_strong_drain(tool_calls);
    let completed_tool_call_count = tool_calls
        .iter()
        .filter(|call| call.lifecycle_state.as_deref() == Some("completed"))
        .count();
    let completed_calls_have_results = completed_tool_call_count == 0
        || transcript_tool_result_count(history) == completed_tool_call_count;

    tool_calls_reserved_by_assistant_message
        && no_running_tool_calls
        && completed_calls_have_results
}

async fn assert_transcript_counts(
    label: &str,
    node: &EmbeddedNode,
    session_id: &str,
    expected_messages: usize,
    expected_tool_calls: usize,
) {
    let (messages, tool_calls, _) = transcript_messages_and_calls(node, session_id).await;
    assert_eq!(
        messages.len(),
        expected_messages,
        "{label}: AgentMessage count"
    );
    assert_eq!(
        tool_calls.len(),
        expected_tool_calls,
        "{label}: AgentToolCall count"
    );
}

async fn assert_transcript_post_state(
    case: &lean_vocab_test::LeanTranscriptCase,
    node: &EmbeddedNode,
    session_id: &str,
) -> (Vec<MessageSnapshot>, Vec<ToolCallSnapshot>, Vec<Message>) {
    let (messages, tool_calls, history) = transcript_messages_and_calls(node, session_id).await;
    assert_eq!(
        messages.len(),
        case.post_message_count,
        "{}: post_message_count",
        case.name
    );
    assert_eq!(
        tool_calls.len(),
        case.post_tool_call_count,
        "{}: post_tool_call_count",
        case.name
    );
    assert_eq!(
        transcript_ordered(&messages),
        case.expected_ordered,
        "{}: expected_ordered",
        case.name
    );
    assert_eq!(
        transcript_pair_closed(&messages, &tool_calls, &history),
        case.expected_pair_closed,
        "{}: expected_pair_closed",
        case.name
    );
    assert_eq!(
        transcript_strong_drain(&tool_calls),
        case.expected_strong_drain,
        "{}: expected_strong_drain",
        case.name
    );
    (messages, tool_calls, history)
}

async fn persist_completed_tool_sequence(
    test_name: &str,
    case: &lean_vocab_test::LeanTranscriptCase,
) -> (support::TestDb, DefraSessionHook, String, u32) {
    let (db, hook, session_id) = transcript_hook_fixture(test_name).await;
    assert_transcript_counts(
        &format!("{} pre-state", case.name),
        &db.node,
        &session_id,
        case.pre_message_count,
        case.pre_tool_call_count,
    )
    .await;

    assert!(matches!(
        hook.on_completion_call(
            &transcript_user_message("run transcript conformance tool"),
            &[],
        )
        .await,
        HookAction::Continue
    ));

    let model_call_id = format!("result-{}", case.logical_result_id);
    let internal_call_id = format!("internal-{}", case.logical_result_id);
    let payload = format!("payload-{}", case.payload_hash);
    let tool_args = r#"{"file_path":"/tmp/transcript-contract.txt"}"#;

    assert!(matches!(
        hook.on_tool_call(
            "read",
            Some(model_call_id.clone()),
            &internal_call_id,
            tool_args,
        )
        .await,
        ToolCallHookAction::Continue
    ));

    let assistant_sequence = hook
        .persist_message(&transcript_assistant_tool_call_message(&model_call_id))
        .await
        .expect("persist assistant tool-call message");
    assert_eq!(
        assistant_sequence as usize, case.assistant_sequence,
        "{}: assistant_sequence",
        case.name
    );

    assert!(matches!(
        hook.on_tool_result(
            "read",
            Some(model_call_id.clone()),
            &internal_call_id,
            tool_args,
            &gents::tool_call_lifecycle::ToolOutcome::Completed(payload.clone()),
        )
        .await,
        HookAction::Continue
    ));

    (db, hook, session_id, case.result_sequence as u32)
}

fn assert_transcript_case_shape() {
    let cases = lean_transcript_cases();
    assert_eq!(cases.len(), 7);

    let names = cases
        .iter()
        .map(|case| case.name.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        names,
        [
            "ordering_user_assistant_tool_result",
            "dedupe_duplicate_reuses_sequence",
            "distinct_result_ids_append_distinct_rows",
            "parallel_results_share_assistant_turn",
            "completed_tool_pair_closed",
            "explicit_drain_terminalizes_ownership",
            "drop_abandon_not_strong_drain",
        ]
        .into_iter()
        .collect::<BTreeSet<_>>()
    );

    let ordering = lean_transcript_case("ordering_user_assistant_tool_result");
    assert!(ordering.legal);
    assert_eq!(ordering.group.as_str(), "ordering");
    assert_eq!(ordering.pre_message_count, 0);
    assert_eq!(ordering.post_message_count, 3);
    assert_eq!(ordering.pre_tool_call_count, 0);
    assert_eq!(ordering.post_tool_call_count, 1);
    assert_eq!(ordering.assistant_sequence, 2);
    assert_eq!(ordering.result_sequence, 3);
    assert!(ordering.expected_ordered);
    assert!(ordering.expected_pair_closed);

    let dedupe = lean_transcript_case("dedupe_duplicate_reuses_sequence");
    assert_eq!(dedupe.group.as_str(), "dedupe");
    assert_eq!(dedupe.action.as_str(), "observe_duplicate_tool_result");
    assert_eq!(dedupe.pre_message_count, dedupe.post_message_count);
    assert_eq!(dedupe.pre_tool_call_count, dedupe.post_tool_call_count);
    assert_eq!(dedupe.logical_result_id, ordering.logical_result_id);
    assert_eq!(dedupe.payload_hash, ordering.payload_hash);
    assert!(dedupe.expected_duplicate_reused_sequence);
    assert_eq!(dedupe.result_sequence, ordering.result_sequence);

    let distinct = lean_transcript_case("distinct_result_ids_append_distinct_rows");
    assert_eq!(distinct.group.as_str(), "dedupe");
    assert_eq!(distinct.payload_hash, ordering.payload_hash);
    assert_ne!(distinct.logical_result_id, ordering.logical_result_id);
    assert_eq!(distinct.pre_message_count + 1, distinct.post_message_count);
    assert!(!distinct.expected_duplicate_reused_sequence);

    let parallel = lean_transcript_case("parallel_results_share_assistant_turn");
    assert!(parallel.legal);
    assert_eq!(parallel.group.as_str(), "ordering");
    assert_eq!(
        parallel.action.as_str(),
        "persist_assistant_once_then_complete_each_parallel_result"
    );
    assert_eq!(parallel.pre_message_count, 0);
    assert_eq!(parallel.post_message_count, 5);
    assert_eq!(parallel.pre_tool_call_count, 0);
    assert_eq!(parallel.post_tool_call_count, 3);
    assert_eq!(parallel.assistant_sequence, 2);
    assert_eq!(parallel.result_sequence, 3);
    assert_ne!(parallel.logical_result_id, ordering.logical_result_id);
    assert!(parallel.expected_pair_closed);
    assert!(!parallel.expected_duplicate_reused_sequence);

    let pair = lean_transcript_case("completed_tool_pair_closed");
    assert_eq!(pair.group.as_str(), "pairing");
    assert!(pair.expected_pair_closed);
    assert!(pair.expected_ordered);

    let drain = lean_transcript_case("explicit_drain_terminalizes_ownership");
    assert_eq!(drain.group.as_str(), "hook_boundary");
    assert_eq!(drain.pre_in_flight_count, 1);
    assert_eq!(drain.post_in_flight_count, 0);
    assert!(drain.expected_strong_drain);

    let abandon = lean_transcript_case("drop_abandon_not_strong_drain");
    assert_eq!(abandon.group.as_str(), "hook_boundary");
    assert_eq!(abandon.action.as_str(), "abandon_hook_ownership");
    assert_eq!(abandon.pre_in_flight_count, 1);
    assert_eq!(abandon.post_in_flight_count, 0);
    assert!(!abandon.expected_strong_drain);
    assert!(!abandon.expected_pair_closed);

    for case in cases {
        assert!(case.legal, "transcript case {} should be legal", case.name);
        assert!(
            case.expected_ordered,
            "transcript case {} should preserve ordering",
            case.name
        );
    }
}

pub(super) async fn generated_transcript_cases_drive_agent_message_ordering_contract() {
    assert_transcript_case_shape();

    let ordering = lean_transcript_case("ordering_user_assistant_tool_result");
    let (db, hook, session_id, result_sequence) =
        persist_completed_tool_sequence("transcript-ordering", ordering).await;
    assert_eq!(
        hook.cancel_in_flight_tool_calls().await.unwrap(),
        ordering.post_in_flight_count,
        "{}: post_in_flight_count",
        ordering.name
    );
    let (messages, tool_calls, history) =
        assert_transcript_post_state(ordering, &db.node, &session_id).await;
    assert_eq!(result_sequence as usize, ordering.result_sequence);
    assert_eq!(
        messages
            .iter()
            .find(|message| message.role.as_str() == "user" && message.sequence > 1)
            .map(|message| message.sequence as usize),
        Some(ordering.result_sequence),
        "{}: result_sequence",
        ordering.name
    );
    assert_eq!(
        tool_calls
            .first()
            .map(|call| call.message_sequence as usize),
        Some(ordering.assistant_sequence),
        "{}: tool call reserves assistant sequence",
        ordering.name
    );
    assert_eq!(
        transcript_tool_result_count(&history),
        1,
        "{}",
        ordering.name
    );

    let dedupe = lean_transcript_case("dedupe_duplicate_reuses_sequence");
    let (db, hook, session_id, first_result_sequence) =
        persist_completed_tool_sequence("transcript-dedupe", ordering).await;
    assert_transcript_counts(
        "dedupe duplicate pre-state",
        &db.node,
        &session_id,
        dedupe.pre_message_count,
        dedupe.pre_tool_call_count,
    )
    .await;
    let duplicate_sequence = hook
        .persist_message(&transcript_tool_result_message(
            &format!("result-{}", dedupe.logical_result_id),
            &format!("payload-{}", dedupe.payload_hash),
        ))
        .await
        .expect("persist duplicate tool-result message");
    assert_eq!(
        duplicate_sequence as usize, dedupe.result_sequence,
        "{}: duplicate reused sequence",
        dedupe.name
    );
    assert_eq!(
        first_result_sequence as usize, dedupe.result_sequence,
        "{}: original sequence",
        dedupe.name
    );
    assert_eq!(
        hook.cancel_in_flight_tool_calls().await.unwrap(),
        dedupe.post_in_flight_count,
        "{}: post_in_flight_count",
        dedupe.name
    );
    let (messages, _, history) = assert_transcript_post_state(dedupe, &db.node, &session_id).await;
    assert_eq!(messages.len(), dedupe.pre_message_count, "{}", dedupe.name);
    assert_eq!(transcript_tool_result_count(&history), 1, "{}", dedupe.name);

    let distinct = lean_transcript_case("distinct_result_ids_append_distinct_rows");
    let (db, hook, session_id) = transcript_hook_fixture("transcript-distinct").await;
    let seed_result_id = format!("result-{}", ordering.logical_result_id);
    let payload = format!("payload-{}", distinct.payload_hash);
    let first_sequence = hook
        .persist_message(&transcript_tool_result_message(&seed_result_id, &payload))
        .await
        .expect("persist seed tool-result message");
    assert_eq!(first_sequence, 1, "{}: seed sequence", distinct.name);
    assert_transcript_counts(
        "distinct result-id pre-state",
        &db.node,
        &session_id,
        distinct.pre_message_count,
        distinct.pre_tool_call_count,
    )
    .await;
    let distinct_sequence = hook
        .persist_message(&transcript_tool_result_message(
            &format!("result-{}", distinct.logical_result_id),
            &payload,
        ))
        .await
        .expect("persist distinct tool-result message");
    assert_eq!(
        distinct_sequence as usize, distinct.result_sequence,
        "{}: result_sequence",
        distinct.name
    );
    assert_eq!(
        hook.cancel_in_flight_tool_calls().await.unwrap(),
        distinct.post_in_flight_count,
        "{}: post_in_flight_count",
        distinct.name
    );
    let (_, _, history) = assert_transcript_post_state(distinct, &db.node, &session_id).await;
    assert_eq!(
        transcript_tool_result_count(&history),
        distinct.post_message_count,
        "{}: distinct result rows",
        distinct.name
    );

    let parallel = lean_transcript_case("parallel_results_share_assistant_turn");
    let (db, hook, session_id) = transcript_hook_fixture("transcript-parallel-results").await;
    assert_transcript_counts(
        &format!("{} pre-state", parallel.name),
        &db.node,
        &session_id,
        parallel.pre_message_count,
        parallel.pre_tool_call_count,
    )
    .await;
    assert!(matches!(
        hook.on_completion_call(
            &transcript_user_message("run parallel transcript conformance tools"),
            &[],
        )
        .await,
        HookAction::Continue
    ));

    let tool_args = r#"{"file_path":"/tmp/transcript-contract.txt"}"#;
    let model_call_ids = (0..parallel.post_tool_call_count)
        .map(|offset| format!("result-{}", parallel.logical_result_id + offset))
        .collect::<Vec<_>>();
    for (offset, model_call_id) in model_call_ids.iter().enumerate() {
        let internal_call_id = format!("internal-{}", parallel.logical_result_id + offset);
        assert!(matches!(
            hook.on_tool_call(
                "read",
                Some(model_call_id.clone()),
                &internal_call_id,
                tool_args,
            )
            .await,
            ToolCallHookAction::Continue
        ));
    }

    let assistant_sequence = hook
        .persist_message(&transcript_assistant_tool_calls_message(
            &model_call_ids
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
        ))
        .await
        .expect("persist accumulated parallel assistant turn");
    assert_eq!(
        assistant_sequence as usize, parallel.assistant_sequence,
        "{}: assistant_sequence",
        parallel.name
    );

    for (offset, model_call_id) in model_call_ids.iter().enumerate() {
        let internal_call_id = format!("internal-{}", parallel.logical_result_id + offset);
        let payload = format!("payload-{}", parallel.payload_hash);
        assert!(
            matches!(
                hook.on_tool_result(
                    "read",
                    Some(model_call_id.clone()),
                    &internal_call_id,
                    tool_args,
                    &gents::tool_call_lifecycle::ToolOutcome::Completed(payload.clone()),
                )
                .await,
                HookAction::Continue
            ),
            "{}: result #{} must persist under the once-persisted turn",
            parallel.name,
            offset + 1
        );
    }

    assert_eq!(
        hook.cancel_in_flight_tool_calls().await.unwrap(),
        parallel.post_in_flight_count,
        "{}: post_in_flight_count",
        parallel.name
    );
    let (messages, tool_calls, history) =
        assert_transcript_post_state(parallel, &db.node, &session_id).await;
    let result_sequences = messages
        .iter()
        .filter(|message| message.role.as_str() == "user" && message.sequence > 1)
        .map(|message| message.sequence as usize)
        .collect::<Vec<_>>();
    assert_eq!(
        result_sequences,
        (parallel.result_sequence..parallel.result_sequence + parallel.post_tool_call_count)
            .collect::<Vec<_>>(),
        "{}: each parallel result appends its own row",
        parallel.name
    );
    for call in &tool_calls {
        assert_eq!(
            call.message_sequence as usize, parallel.assistant_sequence,
            "{}: every parallel call reserves the one persisted assistant turn",
            parallel.name
        );
        assert_eq!(
            call.lifecycle_state.as_deref(),
            Some("completed"),
            "{}: parallel call completed",
            parallel.name
        );
    }
    assert_eq!(
        transcript_tool_result_count(&history),
        parallel.post_tool_call_count,
        "{}: history result rows",
        parallel.name
    );

    let pair = lean_transcript_case("completed_tool_pair_closed");
    let (db, hook, session_id, _) =
        persist_completed_tool_sequence("transcript-pair-closed", pair).await;
    assert_eq!(
        hook.cancel_in_flight_tool_calls().await.unwrap(),
        pair.post_in_flight_count,
        "{}: post_in_flight_count",
        pair.name
    );
    let (_, tool_calls, history) = assert_transcript_post_state(pair, &db.node, &session_id).await;
    assert_eq!(
        tool_calls
            .first()
            .and_then(|call| call.lifecycle_state.as_deref()),
        Some("completed"),
        "{}: completed tool call",
        pair.name
    );
    assert_eq!(transcript_tool_result_count(&history), 1, "{}", pair.name);

    let drain = lean_transcript_case("explicit_drain_terminalizes_ownership");
    let (db, hook, session_id) = transcript_hook_fixture("transcript-explicit-drain").await;
    assert!(matches!(
        hook.on_tool_call(
            "read",
            Some("result-drain".to_string()),
            "internal-drain",
            r#"{"file_path":"/tmp/transcript-contract.txt"}"#,
        )
        .await,
        ToolCallHookAction::Continue
    ));
    let assistant_sequence = hook
        .persist_message(&transcript_assistant_tool_call_message("result-drain"))
        .await
        .expect("persist drain assistant message");
    assert_eq!(
        assistant_sequence as usize, drain.assistant_sequence,
        "{}: assistant_sequence",
        drain.name
    );
    assert_transcript_counts(
        "explicit drain pre-state",
        &db.node,
        &session_id,
        drain.pre_message_count,
        drain.pre_tool_call_count,
    )
    .await;
    assert_eq!(
        hook.cancel_in_flight_tool_calls().await.unwrap(),
        drain.pre_in_flight_count,
        "{}: explicit drain count",
        drain.name
    );
    assert_eq!(
        hook.cancel_in_flight_tool_calls().await.unwrap(),
        drain.post_in_flight_count,
        "{}: post_in_flight_count",
        drain.name
    );
    let (_, tool_calls, _) = assert_transcript_post_state(drain, &db.node, &session_id).await;
    assert_eq!(
        tool_calls
            .first()
            .and_then(|call| call.lifecycle_state.as_deref()),
        Some("cancelled"),
        "{}: durable row terminalized",
        drain.name
    );

    let abandon = lean_transcript_case("drop_abandon_not_strong_drain");
    let (db, hook, session_id) = transcript_hook_fixture("transcript-drop-abandon").await;
    assert!(matches!(
        hook.on_tool_call(
            "read",
            Some("result-abandon".to_string()),
            "internal-abandon",
            r#"{"file_path":"/tmp/transcript-contract.txt"}"#,
        )
        .await,
        ToolCallHookAction::Continue
    ));
    assert_transcript_counts(
        "drop abandon pre-state",
        &db.node,
        &session_id,
        abandon.pre_message_count,
        abandon.pre_tool_call_count,
    )
    .await;
    drop(hook);
    let observer = DefraSessionHook::resume_with_identity_policy(
        db.node.clone(),
        &session_id,
        AGENT_NAME,
        AGENT_DID,
        FailurePolicy::default(),
    )
    .await
    .expect("resume transcript observer after ownership abandonment");
    assert_eq!(
        observer.cancel_in_flight_tool_calls().await.unwrap(),
        abandon.post_in_flight_count,
        "{}: drop abandons in-memory ownership",
        abandon.name
    );
    let (_, tool_calls, _) = assert_transcript_post_state(abandon, &db.node, &session_id).await;
    assert_eq!(
        tool_calls
            .first()
            .and_then(|call| call.lifecycle_state.as_deref()),
        Some("running"),
        "{}: durable row remains running after Drop",
        abandon.name
    );
}
