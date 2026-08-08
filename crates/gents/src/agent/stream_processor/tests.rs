use std::sync::Arc;
use std::time::Duration;

use crate::llm::message::{
    AssistantContent, Message, Reasoning, Text, ToolCall, ToolFunction, ToolResultContent,
    UserContent,
};
use crate::llm::HookAction;
use rig::agent::MultiTurnStreamItem;
use rig::streaming::{StreamedAssistantContent, StreamedUserContent};

use super::*;
use crate::ensure_schemas;
use crate::hook::FailurePolicy;
use crate::lifecycle::{ClaimOutcome, ExecutionOrigin, RequestLifecycle};
use crate::streaming::DefraStreamWriter;
use crate::test_support::first_content;
use crate::watcher::AgentRequest;

async fn signed_test_node(
    data_path: &std::path::Path,
) -> (
    Arc<defra_node::EmbeddedNode>,
    crate::test_support::SignedTestIdentity,
) {
    let identity = crate::test_support::signed_test_identity("agent-stream-processor-identity");
    let node = Arc::new(
        defra_node::EmbeddedNode::builder()
            .data_path(data_path)
            .with_node_identity_did(identity.did())
            .build()
            .await
            .unwrap(),
    );
    ensure_schemas(&node).await.unwrap();
    (node, identity)
}

fn user_text_message(text: &str) -> Message {
    Message::User {
        content: vec![UserContent::Text(Text {
            text: text.to_string(),
        })],
    }
}

#[tokio::test]
async fn persist_partial_turn_saves_reasoning_and_text_to_history() {
    let data_path =
        std::env::temp_dir().join(format!("agent-stream-processor-{}", uuid::Uuid::new_v4()));
    let (node, _identity) = signed_test_node(&data_path).await;

    let hook = DefraSessionHook::with_identity(
        node.clone(),
        "general",
        "did:test:test",
        FailurePolicy::default(),
    );
    assert!(matches!(
        hook.on_completion_call(&user_text_message("Inspect the repo"), &[])
            .await,
        HookAction::Continue
    ));

    let session_id = hook.session_id().await.expect("session id");
    let request_id = uuid::Uuid::new_v4().to_string();
    let request = AgentRequest {
        doc_id: "request-doc".to_string(),
        request_id: request_id.clone(),
        agent_did: "did:test:test".to_string(),
        requester_did: None,
        behavior_id: Some("general".to_string()),
        session_id: session_id.clone(),
        content: "Inspect the repo".to_string(),
        temperature: None,
        top_p: None,
        top_k: None,
        max_tokens: None,
        metadata: None,
        execution_origin: None,
        created_at: chrono::Utc::now().to_rfc3339(),
        deadline: None,
        subagent_depth: 0,
        caused_by_parent_request_id: None,
        caused_by_parent_tool_call_id: None,
    };
    let mut lifecycle = RequestLifecycle::new_with_execution_binding(
        node.clone(),
        "test-agent",
        "did:test:test",
        request,
        30,
        crate::lifecycle::ExecutionOrigin::Interactive,
        "test-backend",
    );
    let stream_writer = crate::streaming::DefraStreamWriter::new(
        node.clone(),
        "did:test:test",
        Duration::from_secs(60),
    );
    // Begin a streaming response so reset_tail (called by persist_partial_turn)
    // has a live buffer to clear.
    let response_doc_id = stream_writer
        .begin(&session_id, &request_id, "general")
        .await
        .unwrap();
    let mut processor =
        StreamProcessor::new(&hook, &stream_writer, &mut lifecycle, &response_doc_id);

    processor.assistant_turn.push_reasoning(
        Reasoning::new("Need to inspect directory structure first")
            .with_id("rs_partial".to_string()),
    );
    processor
        .assistant_turn
        .push_text("I started by checking the repo layout.");

    assert!(processor.has_observable_activity());
    assert!(processor
        .persist_partial_turn("persist errored assistant turn")
        .await
        .unwrap());

    let history = crate::session::load_history(&node, &session_id)
        .await
        .unwrap();
    assert_eq!(history.len(), 2);
    assert!(matches!(
        &history[1],
        Message::Assistant { content, .. }
            if content.len() == 2
                // Order is text, then reasoning (rig's threading/persist order).
                && matches!(first_content(content), AssistantContent::Text(Text { text })
                    if text == "I started by checking the repo layout.")
                && matches!(content.get(1), Some(AssistantContent::Reasoning(reasoning))
                    if reasoning.id.as_deref() == Some("rs_partial"))
    ));

    let _ = std::fs::remove_dir_all(&data_path);
}

// ---------------------------------------------------------------------------
// Helpers for the tail-reset integration test
// ---------------------------------------------------------------------------

async fn create_pending_request(
    node: &Arc<defra_node::EmbeddedNode>,
    request_id: &str,
    session_id: &str,
) -> String {
    create_pending_request_for_agent(node, request_id, session_id, "did:test:test").await
}

async fn create_pending_request_for_agent(
    node: &Arc<defra_node::EmbeddedNode>,
    request_id: &str,
    session_id: &str,
    agent_did: &str,
) -> String {
    let created_at = chrono::Utc::now().to_rfc3339();
    let agent_did = crate::graphql::escape_graphql_string(agent_did);
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{request_id}",
                agent_did: "{agent_did}",
                behavior_id: "general",
                session_id: "{session_id}",
                retry_parent_request: "",
                retry_root_request: "{request_id}",
                superseded_by_request: "",
                content: "test prompt",
                status: "pending",
                lifecycle_state: "pending",
                backend_id: "",
                execution_origin: "interactive",
                failure_reason: "",
                created_at: "{created_at}",
                retry_count: 0,
                max_retries: 3
            }}) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "create_AgentRequest failed: {:?}",
        resp.errors
    );
    // DefraDB returns the doc id in the mutation response or we query for it.
    if let Some(doc_id) = resp
        .data
        .as_ref()
        .and_then(|d| d.get("create_AgentRequest"))
        .and_then(|v| v.get("_docID"))
        .and_then(|v| v.as_str())
    {
        return doc_id.to_string();
    }
    // Fallback: query by request_id.
    let query = format!(
        r#"{{
            AgentRequest(filter: {{ request_id: {{ _eq: "{request_id}" }} }}, limit: 1) {{
                _docID
            }}
        }}"#
    );
    let resp = node.execute(&query).await;
    assert!(
        !resp.has_errors(),
        "AgentRequest lookup failed: {:?}",
        resp.errors
    );
    resp.data
        .as_ref()
        .and_then(|d| d.get("AgentRequest"))
        .and_then(|v| v.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("_docID"))
        .and_then(|v| v.as_str())
        .map(ToOwned::to_owned)
        .expect("request _docID")
}

async fn load_response_doc(
    node: &defra_node::EmbeddedNode,
    doc_id: &str,
) -> serde_json::Map<String, serde_json::Value> {
    let query = format!(
        r#"{{
            AgentResponse(filter: {{ _docID: {{ _eq: "{doc_id}" }} }}, limit: 1) {{
                _docID
                content
                reasoning
                status
                token_count
            }}
        }}"#
    );
    let resp = node.execute(&query).await;
    assert!(
        !resp.has_errors(),
        "load_response_doc failed: {:?}",
        resp.errors
    );
    resp.data
        .as_ref()
        .and_then(|d| d.get("AgentResponse"))
        .and_then(|v| v.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_object())
        .cloned()
        .expect("AgentResponse row")
}

#[derive(Debug, serde::Deserialize)]
struct PersistedMessageShape {
    sequence: u32,
    role: String,
    content: String,
}

async fn load_message_shapes(
    node: &defra_node::EmbeddedNode,
    session_id: &str,
) -> Vec<PersistedMessageShape> {
    let session_id = crate::graphql::escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            AgentMessage(
                filter: {{ session_id: {{ _eq: "{session_id}" }} }},
                order: {{ sequence: ASC }}
            ) {{ sequence role content }}
        }}"#
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "AgentMessage shape query failed: {:?}",
        response.errors
    );
    response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentMessage"))
        .and_then(|rows| serde_json::from_value(rows.clone()).ok())
        .unwrap_or_default()
}

fn text_item(text: &str) -> Result<LoopStreamItem<()>, rig::agent::StreamingError> {
    Ok(LoopStreamItem::Item(
        MultiTurnStreamItem::StreamAssistantItem(StreamedAssistantContent::Text(
            rig::completion::message::Text {
                text: text.to_string(),
            },
        )),
    ))
}

fn tool_call_item(
    name: &str,
    args_json: &str,
    internal_id: &str,
) -> Result<LoopStreamItem<()>, rig::agent::StreamingError> {
    tool_call_item_with_ids(name, args_json, internal_id, internal_id, None)
}

fn tool_call_item_with_ids(
    name: &str,
    args_json: &str,
    tool_id: &str,
    internal_id: &str,
    call_id: Option<&str>,
) -> Result<LoopStreamItem<()>, rig::agent::StreamingError> {
    Ok(LoopStreamItem::Item(
        MultiTurnStreamItem::StreamAssistantItem(StreamedAssistantContent::ToolCall {
            tool_call: rig::completion::message::ToolCall {
                id: tool_id.to_string(),
                call_id: call_id.map(ToOwned::to_owned),
                function: rig::completion::message::ToolFunction {
                    name: name.to_string(),
                    arguments: serde_json::from_str(args_json).unwrap(),
                },
                signature: None,
                additional_params: None,
            },
            internal_call_id: internal_id.to_string(),
        }),
    ))
}

fn tool_result_item(
    tool_id: &str,
    result_json: &str,
    internal_id: &str,
) -> Result<LoopStreamItem<()>, rig::agent::StreamingError> {
    tool_result_item_with_call_id(tool_id, None, result_json, internal_id)
}

fn tool_result_item_with_call_id(
    tool_id: &str,
    call_id: Option<&str>,
    result_json: &str,
    internal_id: &str,
) -> Result<LoopStreamItem<()>, rig::agent::StreamingError> {
    Ok(LoopStreamItem::Item(MultiTurnStreamItem::StreamUserItem(
        StreamedUserContent::ToolResult {
            tool_result: rig::completion::message::ToolResult {
                id: tool_id.to_string(),
                call_id: call_id.map(ToOwned::to_owned),
                content: rig::one_or_many::OneOrMany::one(
                    rig::completion::message::ToolResultContent::Text(
                        rig::completion::message::Text {
                            text: result_json.to_string(),
                        },
                    ),
                ),
            },
            internal_call_id: internal_id.to_string(),
        },
    )))
}

fn final_item(response_text: &str) -> Result<LoopStreamItem<()>, rig::agent::StreamingError> {
    Ok(LoopStreamItem::Item(
        MultiTurnStreamItem::<()>::final_response(response_text, rig::completion::Usage::new()),
    ))
}

fn turn_retracted_item(
    turn: usize,
    attempt: u32,
) -> Result<LoopStreamItem<()>, rig::agent::StreamingError> {
    Ok(LoopStreamItem::TurnRetracted {
        turn,
        attempt,
        backoff: std::time::Duration::ZERO,
    })
}

#[tokio::test]
async fn hook_persisted_tool_result_dedupes_matching_stream_result() {
    let data_path = std::env::temp_dir().join(format!(
        "agent-stream-processor-tool-dedupe-{}",
        uuid::Uuid::new_v4()
    ));
    let (node, identity) = signed_test_node(&data_path).await;
    let agent_did = identity.did();

    let hook = crate::hook::DefraSessionHook::with_identity(
        node.clone(),
        "general",
        agent_did,
        FailurePolicy::default(),
    );
    assert!(matches!(
        hook.on_completion_call(&user_text_message("discover available tools"), &[])
            .await,
        HookAction::Continue
    ));
    let session_id = hook.session_id().await.expect("session id");

    let request_id = uuid::Uuid::new_v4().to_string();
    let request_doc_id =
        create_pending_request_for_agent(&node, &request_id, &session_id, agent_did).await;
    let request = AgentRequest {
        doc_id: request_doc_id,
        request_id: request_id.clone(),
        agent_did: agent_did.to_string(),
        requester_did: None,
        behavior_id: Some("general".to_string()),
        session_id: session_id.clone(),
        content: "discover available tools".to_string(),
        temperature: None,
        top_p: None,
        top_k: None,
        max_tokens: None,
        metadata: None,
        execution_origin: None,
        created_at: chrono::Utc::now().to_rfc3339(),
        deadline: None,
        subagent_depth: 0,
        caused_by_parent_request_id: None,
        caused_by_parent_tool_call_id: None,
    };

    let mut lifecycle = RequestLifecycle::new_with_execution_binding(
        node.clone(),
        "general",
        agent_did,
        request,
        30,
        ExecutionOrigin::Interactive,
        "test-backend",
    );
    assert_eq!(
        lifecycle.claim_without_identity_for_test().await.unwrap(),
        ClaimOutcome::Claimed
    );

    let stream_writer = DefraStreamWriter::new(node.clone(), agent_did, Duration::from_millis(0));
    let response_doc_id = stream_writer
        .begin(&session_id, &request_id, "general")
        .await
        .unwrap();
    lifecycle.set_response_doc_id(&response_doc_id).unwrap();

    let mut processor =
        StreamProcessor::new(&hook, &stream_writer, &mut lifecycle, &response_doc_id);

    let stored_call_id = "OaoTQYzCdoptKiK_mdhBA";
    let model_result_id = "c6b8bdeb-ab92-4481-b763-bdafbd463904";
    let tool_args = r#"{"tool":"discover_tools"}"#;
    let tool_result = r#"{"tools":["discover_tools","describe_tool"]}"#;

    processor
        .process_item(tool_call_item_with_ids(
            "discover_tools",
            tool_args,
            model_result_id,
            model_result_id,
            Some(model_result_id),
        ))
        .await
        .unwrap();
    assert!(matches!(
        hook.on_tool_call(
            "discover_tools",
            Some(model_result_id.to_string()),
            stored_call_id,
            tool_args,
        )
        .await,
        crate::llm::ToolCallHookAction::Continue
    ));
    assert!(processor
        .persist_partial_turn("persist streamed assistant tool call")
        .await
        .unwrap());
    assert!(matches!(
        hook.on_tool_result(
            "discover_tools",
            Some(model_result_id.to_string()),
            stored_call_id,
            tool_args,
            &crate::tool_call_lifecycle::ToolOutcome::Completed(tool_result.to_string()),
        )
        .await,
        HookAction::Continue
    ));

    processor
        .process_item(tool_result_item_with_call_id(
            model_result_id,
            Some(model_result_id),
            tool_result,
            model_result_id,
        ))
        .await
        .unwrap();

    let history = crate::session::load_history(&node, &session_id)
        .await
        .unwrap();
    let tool_results = history
        .iter()
        .filter_map(|message| match message {
            Message::User { content } => match first_content(content) {
                UserContent::ToolResult(tool_result) => Some(tool_result),
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(
        tool_results.len(),
        1,
        "hook and stream paths must materialize one logical tool result"
    );
    assert_eq!(tool_results[0].id, model_result_id);
    assert_eq!(tool_results[0].call_id.as_deref(), Some(model_result_id));
    assert!(matches!(
        first_content(&tool_results[0].content),
        ToolResultContent::Text(Text { text }) if text == tool_result
    ));
    assert_eq!(
        crate::session::load_tool_call_result(&node, &session_id, stored_call_id)
            .await
            .unwrap(),
        tool_result
    );

    let _ = std::fs::remove_dir_all(&data_path);
}

#[tokio::test]
async fn streamed_wait_call_precedes_concurrent_notification_and_tool_result() {
    let data_path = std::env::temp_dir().join(format!(
        "agent-stream-processor-inline-tool-result-{}",
        uuid::Uuid::new_v4()
    ));
    let (node, _identity) = signed_test_node(&data_path).await;

    let hook = DefraSessionHook::with_identity(
        node.clone(),
        "general",
        "did:test:test",
        FailurePolicy::default(),
    );
    assert!(matches!(
        hook.on_completion_call(&user_text_message("read the source"), &[])
            .await,
        HookAction::Continue
    ));
    let session_id = hook.session_id().await.expect("session id");

    let request_id = uuid::Uuid::new_v4().to_string();
    let request_doc_id = create_pending_request(&node, &request_id, &session_id).await;
    hook.set_active_request_id(Some(request_id.clone())).await;
    hook.set_request_deadline_at(Some(chrono::Utc::now() + chrono::Duration::seconds(60)))
        .await;
    let request = AgentRequest {
        doc_id: request_doc_id,
        request_id: request_id.clone(),
        agent_did: "did:test:test".to_string(),
        requester_did: None,
        behavior_id: Some("general".to_string()),
        session_id: session_id.clone(),
        content: "read the source".to_string(),
        temperature: None,
        top_p: None,
        top_k: None,
        max_tokens: None,
        metadata: None,
        execution_origin: None,
        created_at: chrono::Utc::now().to_rfc3339(),
        deadline: None,
        subagent_depth: 0,
        caused_by_parent_request_id: None,
        caused_by_parent_tool_call_id: None,
    };
    let mut lifecycle = RequestLifecycle::new_with_execution_binding(
        node.clone(),
        "general",
        "did:test:test",
        request,
        30,
        ExecutionOrigin::Interactive,
        "test-backend",
    );
    assert_eq!(
        lifecycle.claim_without_identity_for_test().await.unwrap(),
        ClaimOutcome::Claimed
    );
    let stream_writer =
        DefraStreamWriter::new(node.clone(), "did:test:test", Duration::from_millis(0));
    let response_doc_id = stream_writer
        .begin(&session_id, &request_id, "general")
        .await
        .unwrap();
    lifecycle.set_response_doc_id(&response_doc_id).unwrap();
    let mut processor =
        StreamProcessor::new(&hook, &stream_writer, &mut lifecycle, &response_doc_id);

    processor
        .process_item(tool_call_item_with_ids(
            "read_file",
            r#"{"path":"/work/entry.c"}"#,
            "result-1",
            "internal-1",
            Some("call-1"),
        ))
        .await
        .unwrap();

    let finalized_before_result = crate::session::load_history(&node, &session_id)
        .await
        .unwrap();
    assert_eq!(
        finalized_before_result.len(),
        1,
        "an accumulating tool-call turn must remain outside finalized provider history"
    );
    let draft_response = node
        .execute(&format!(
            r#"{{
                AgentMessageDraft(
                    filter: {{ session_id: {{ _eq: "{}" }} }}
                ) {{ sequence }}
            }}"#,
            crate::graphql::escape_graphql_string(&session_id)
        ))
        .await;
    assert!(!draft_response.has_errors(), "{:?}", draft_response.errors);
    assert_eq!(
        draft_response.data.unwrap()["AgentMessageDraft"],
        serde_json::json!([{ "sequence": 2 }]),
        "the in-flight assistant turn must reserve sequence 2 as a mutable draft"
    );

    assert!(matches!(
        hook.on_tool_call(
            "read_file",
            Some("call-1".to_string()),
            "internal-1",
            r#"{"path":"/work/entry.c"}"#,
        )
        .await,
        crate::llm::ToolCallHookAction::Continue
    ));

    // Reproduce the live wait race: an independent background task appends a
    // user-role completion while inline tool execution is still active. The
    // streamed tool-call envelope must already own its durable assistant row,
    // so this append allocates the following sequence instead of colliding.
    let notification = serde_json::to_string(&Message::User {
        content: vec![UserContent::Text(Text {
            text: "<tool-notification status=\"completed\" />".to_string(),
        })],
    })
    .unwrap();
    crate::session::append_message_with_requester_did(
        &node,
        &session_id,
        "did:test:test",
        None,
        "user",
        &notification,
        None,
        Some(&request_id),
    )
    .await
    .unwrap();

    assert!(matches!(
        hook.on_tool_result(
            "read_file",
            Some("call-1".to_string()),
            "internal-1",
            r#"{"path":"/work/entry.c"}"#,
            &crate::tool_call_lifecycle::ToolOutcome::Completed("source bytes".to_string()),
        )
        .await,
        HookAction::Continue
    ));

    processor
        .process_item(tool_result_item_with_call_id(
            "result-1",
            Some("call-1"),
            "source bytes",
            "internal-1",
        ))
        .await
        .unwrap();

    let history = crate::session::load_history(&node, &session_id)
        .await
        .unwrap();
    assert_eq!(history.len(), 4);
    assert!(matches!(&history[1], Message::Assistant { content, .. }
        if matches!(content.first(), Some(AssistantContent::ToolCall(tool_call))
            if tool_call.call_id.as_deref() == Some("call-1"))));
    assert!(matches!(&history[2], Message::User { content }
        if matches!(content.first(), Some(UserContent::Text(Text { text }))
            if text.contains("<tool-notification"))));
    assert!(matches!(&history[3], Message::User { content }
        if matches!(content.first(), Some(UserContent::ToolResult(tool_result))
            if tool_result.call_id.as_deref() == Some("call-1"))));

    let shapes = load_message_shapes(&node, &session_id).await;
    assert_eq!(
        shapes
            .iter()
            .map(|row| row.role.as_str())
            .collect::<Vec<_>>(),
        vec!["user", "assistant", "user", "user"]
    );
    assert!(
        shapes
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence),
        "assistant wait, background notification, and result need distinct ordered rows: {shapes:#?}"
    );
    assert_eq!(
        shapes.iter().map(|row| row.sequence).collect::<Vec<_>>(),
        vec![1, 2, 3, 4],
        "the inline wait race must not overwrite a row or leave a phantom reservation"
    );
    assert!(
        shapes[1].content.contains("\"role\":\"assistant\""),
        "assistant database role must agree with its serialized envelope"
    );

    let _ = std::fs::remove_dir_all(&data_path);
}

/// Three parallel tool calls accumulate in ONE assistant turn; persisting that
/// turn on the first streamed result keeps the gate open for the remaining
/// results (Lean: `Transcript.parallel_results_complete_independently`). The
/// historical bug: the first result's user message reset the turn state to
/// Idle, so the second streamed result tripped the "cannot persist streamed
/// tool result before its assistant turn is persisted" guard.
#[tokio::test]
async fn multiple_streamed_tool_results_share_one_accumulated_assistant_turn() {
    let data_path = std::env::temp_dir().join(format!(
        "agent-stream-processor-multi-inline-tool-result-{}",
        uuid::Uuid::new_v4()
    ));
    let (node, _identity) = signed_test_node(&data_path).await;

    let hook = DefraSessionHook::with_identity(
        node.clone(),
        "general",
        "did:test:test",
        FailurePolicy::default(),
    );
    assert!(matches!(
        hook.on_completion_call(&user_text_message("read several files"), &[])
            .await,
        HookAction::Continue
    ));
    let session_id = hook.session_id().await.expect("session id");

    let request_id = uuid::Uuid::new_v4().to_string();
    let request_doc_id = create_pending_request(&node, &request_id, &session_id).await;
    hook.set_active_request_id(Some(request_id.clone())).await;
    hook.set_request_deadline_at(Some(chrono::Utc::now() + chrono::Duration::seconds(60)))
        .await;
    let request = AgentRequest {
        doc_id: request_doc_id,
        request_id: request_id.clone(),
        agent_did: "did:test:test".to_string(),
        requester_did: None,
        behavior_id: Some("general".to_string()),
        session_id: session_id.clone(),
        content: "read several files".to_string(),
        temperature: None,
        top_p: None,
        top_k: None,
        max_tokens: None,
        metadata: None,
        execution_origin: None,
        created_at: chrono::Utc::now().to_rfc3339(),
        deadline: None,
        subagent_depth: 0,
        caused_by_parent_request_id: None,
        caused_by_parent_tool_call_id: None,
    };
    let mut lifecycle = RequestLifecycle::new_with_execution_binding(
        node.clone(),
        "general",
        "did:test:test",
        request,
        30,
        ExecutionOrigin::Interactive,
        "test-backend",
    );
    assert_eq!(
        lifecycle.claim_without_identity_for_test().await.unwrap(),
        ClaimOutcome::Claimed
    );
    let stream_writer =
        DefraStreamWriter::new(node.clone(), "did:test:test", Duration::from_millis(0));
    let response_doc_id = stream_writer
        .begin(&session_id, &request_id, "general")
        .await
        .unwrap();
    lifecycle.set_response_doc_id(&response_doc_id).unwrap();
    let mut processor =
        StreamProcessor::new(&hook, &stream_writer, &mut lifecycle, &response_doc_id);

    for index in 1..=3 {
        let result_id = format!("result-{index}");
        let internal_id = format!("internal-{index}");
        let call_id = format!("call-{index}");
        let args = format!(r#"{{"path":"/work/file-{index}.c"}}"#);
        processor
            .process_item(tool_call_item_with_ids(
                "read_file",
                &args,
                &result_id,
                &internal_id,
                Some(&call_id),
            ))
            .await
            .unwrap();
        assert!(matches!(
            hook.on_tool_call("read_file", Some(call_id.clone()), &internal_id, &args,)
                .await,
            crate::llm::ToolCallHookAction::Continue
        ));
        assert!(matches!(
            hook.on_tool_result(
                "read_file",
                Some(call_id),
                &internal_id,
                &args,
                &crate::tool_call_lifecycle::ToolOutcome::Completed(format!(
                    "source bytes {index}"
                )),
            )
            .await,
            HookAction::Continue
        ));
    }

    for index in 1..=3 {
        processor
            .process_item(tool_result_item_with_call_id(
                &format!("result-{index}"),
                Some(&format!("call-{index}")),
                &format!("source bytes {index}"),
                &format!("internal-{index}"),
            ))
            .await
            .unwrap();
    }

    let history = crate::session::load_history(&node, &session_id)
        .await
        .unwrap();
    assert_eq!(history.len(), 5);
    assert!(matches!(&history[1], Message::Assistant { content, .. }
        if content.iter().filter(|item| matches!(item, AssistantContent::ToolCall(_))).count() == 3));
    let result_count = history
        .iter()
        .filter(|message| {
            matches!(message, Message::User { content }
            if matches!(content.first(), Some(UserContent::ToolResult(_))))
        })
        .count();
    assert_eq!(result_count, 3);

    let _ = std::fs::remove_dir_all(&data_path);
}

#[tokio::test]
async fn backfill_pairs_completed_tool_result_after_provider_stall() {
    // #442 regression. Owned-loop order on a provider stall: the tool runs
    // inline (on_tool_result marks the AgentToolCall row .completed and records
    // its result, but persists NO result message because the assistant turn is
    // not yet persisted), then the provider stalls so the streamed ToolResult
    // never arrives. The abort path persists the partial assistant turn (with
    // the tool call) — leaving a completed tool call with no result message,
    // violating Transcript.CompletedToolCallsPaired. backfill_completed_tool_results
    // must reconcile it (and be idempotent).
    let data_path =
        std::env::temp_dir().join(format!("agent-442-backfill-{}", uuid::Uuid::new_v4()));
    let (node, _identity) = signed_test_node(&data_path).await;

    let hook = crate::hook::DefraSessionHook::with_identity(
        node.clone(),
        "general",
        "did:test:test",
        FailurePolicy::default(),
    );
    assert!(matches!(
        hook.on_completion_call(&user_text_message("use the echo tool"), &[])
            .await,
        HookAction::Continue
    ));
    let session_id = hook.session_id().await.expect("session id");

    let request_id = uuid::Uuid::new_v4().to_string();
    let request_doc_id = create_pending_request(&node, &request_id, &session_id).await;
    // The AgentToolCall row records its request_id from the hook's active request,
    // which is what backfill scopes its query by.
    hook.set_active_request_id(Some(request_id.clone())).await;
    hook.set_request_deadline_at(Some(chrono::Utc::now() + chrono::Duration::seconds(60)))
        .await;
    let request = AgentRequest {
        doc_id: request_doc_id,
        request_id: request_id.clone(),
        agent_did: "did:test:test".to_string(),
        requester_did: None,
        behavior_id: Some("general".to_string()),
        session_id: session_id.clone(),
        content: "use the echo tool".to_string(),
        temperature: None,
        top_p: None,
        top_k: None,
        max_tokens: None,
        metadata: None,
        execution_origin: None,
        created_at: chrono::Utc::now().to_rfc3339(),
        deadline: None,
        subagent_depth: 0,
        caused_by_parent_request_id: None,
        caused_by_parent_tool_call_id: None,
    };
    let mut lifecycle = RequestLifecycle::new_with_execution_binding(
        node.clone(),
        "general",
        "did:test:test",
        request,
        30,
        ExecutionOrigin::Interactive,
        "test-backend",
    );
    assert_eq!(
        lifecycle.claim_without_identity_for_test().await.unwrap(),
        ClaimOutcome::Claimed
    );
    let stream_writer =
        DefraStreamWriter::new(node.clone(), "did:test:test", Duration::from_millis(0));
    let response_doc_id = stream_writer
        .begin(&session_id, &request_id, "general")
        .await
        .unwrap();
    lifecycle.set_response_doc_id(&response_doc_id).unwrap();
    let mut processor =
        StreamProcessor::new(&hook, &stream_writer, &mut lifecycle, &response_doc_id);

    let call_id = "call-1";
    let tool_args = r#"{"x":1}"#;
    let tool_output = "ECHOED-RESULT";

    // Accumulate the assistant tool call so persist_partial_turn writes the turn.
    processor.assistant_turn.push_tool_call(ToolCall {
        id: call_id.to_string(),
        call_id: Some(call_id.to_string()),
        function: ToolFunction {
            name: "echo".to_string(),
            arguments: serde_json::from_str(tool_args).unwrap(),
        },
        signature: None,
        additional_params: None,
    });
    hook.register_stream_tool_call_identity(call_id, call_id, Some(call_id))
        .await;

    // Tool runs inline: lifecycle started, then completed with its result. No
    // result message persists yet (assistant turn not persisted).
    assert!(matches!(
        hook.on_tool_call("echo", Some(call_id.to_string()), call_id, tool_args)
            .await,
        crate::llm::ToolCallHookAction::Continue
    ));
    assert!(matches!(
        hook.on_tool_result(
            "echo",
            Some(call_id.to_string()),
            call_id,
            tool_args,
            &crate::tool_call_lifecycle::ToolOutcome::Completed(tool_output.to_string())
        )
        .await,
        HookAction::Continue
    ));

    // Abort: persist the partial assistant turn (the tool-call message).
    assert!(processor
        .persist_partial_turn("persist errored assistant turn")
        .await
        .unwrap());

    // The orphan: the completed tool call has no paired result message yet.
    assert_eq!(
        count_tool_result_messages(&node, &session_id).await,
        0,
        "result message must be absent before backfill (the #442 orphan)"
    );

    // Backfill reconciles the completed tool call's result message.
    let reconciled = hook.backfill_completed_tool_results().await.unwrap();
    assert_eq!(
        reconciled, 1,
        "one completed tool call should be reconciled"
    );
    assert_eq!(
        count_tool_result_messages(&node, &session_id).await,
        1,
        "backfill must persist exactly one tool-result message (pair closure)"
    );

    // Idempotent: a second backfill must not duplicate the result message.
    hook.backfill_completed_tool_results().await.unwrap();
    assert_eq!(
        count_tool_result_messages(&node, &session_id).await,
        1,
        "backfill must be idempotent (dedup)"
    );

    let _ = std::fs::remove_dir_all(&data_path);
}

async fn count_tool_result_messages(node: &defra_node::EmbeddedNode, session_id: &str) -> usize {
    crate::session::load_history(node, session_id)
        .await
        .unwrap()
        .iter()
        .filter(|message| {
            matches!(message, Message::User { content }
                if matches!(first_content(content), UserContent::ToolResult(_)))
        })
        .count()
}

#[tokio::test]
async fn post_tool_resumed_resets_response_tail() {
    let data_path = std::env::temp_dir().join(format!(
        "agent-stream-processor-tool-reset-{}",
        uuid::Uuid::new_v4()
    ));
    let (node, identity) = signed_test_node(&data_path).await;
    let agent_did = identity.did();

    // Set up session hook + establish session by persisting user message.
    let hook = crate::hook::DefraSessionHook::with_identity(
        node.clone(),
        "general",
        agent_did,
        FailurePolicy::default(),
    );
    assert!(matches!(
        hook.on_completion_call(&user_text_message("test prompt"), &[])
            .await,
        HookAction::Continue
    ));
    let session_id = hook.session_id().await.expect("session id");

    // Create a pending request in the DB so the lifecycle can be claimed.
    let request_id = uuid::Uuid::new_v4().to_string();
    let request_doc_id =
        create_pending_request_for_agent(&node, &request_id, &session_id, agent_did).await;
    hook.set_active_request_id(Some(request_id.clone())).await;

    let request = AgentRequest {
        doc_id: request_doc_id.clone(),
        request_id: request_id.clone(),
        agent_did: agent_did.to_string(),
        requester_did: None,
        behavior_id: Some("general".to_string()),
        session_id: session_id.clone(),
        content: "test prompt".to_string(),
        temperature: None,
        top_p: None,
        top_k: None,
        max_tokens: None,
        metadata: None,
        execution_origin: None,
        created_at: chrono::Utc::now().to_rfc3339(),
        deadline: None,
        subagent_depth: 0,
        caused_by_parent_request_id: None,
        caused_by_parent_tool_call_id: None,
    };

    let mut lifecycle = RequestLifecycle::new_with_execution_binding(
        node.clone(),
        "general",
        agent_did,
        request,
        30,
        ExecutionOrigin::Interactive,
        "test-backend",
    );

    // Claim → Streaming so advance() calls will work.
    let outcome = lifecycle.claim_without_identity_for_test().await.unwrap();
    assert_eq!(outcome, ClaimOutcome::Claimed, "expected Claimed outcome");

    // Use 0 ms batch interval so write_tokens flushes immediately to DB.
    let stream_writer = DefraStreamWriter::new(node.clone(), agent_did, Duration::from_millis(0));
    let provenance = lifecycle
        .execution_provenance()
        .expect("claimed request provenance")
        .clone();
    let response_doc_id = stream_writer
        .begin_document_response(&session_id, &request_id, "general", None, &provenance)
        .await
        .unwrap();
    lifecycle.set_response_doc_id(&response_doc_id).unwrap();

    let mut processor =
        StreamProcessor::new(&hook, &stream_writer, &mut lifecycle, &response_doc_id);

    // Feed: Text → Text → ToolCall → ToolResult
    processor.process_item(text_item("hello ")).await.unwrap();
    processor.process_item(text_item("world")).await.unwrap();
    processor
        .process_item(tool_call_item("search", r#"{"q":"x"}"#, "call-1"))
        .await
        .unwrap();
    processor
        .process_item(tool_result_item("call-1", r#"{"hit":1}"#, "call-1"))
        .await
        .unwrap();

    // After ToolResult: tail must be reset to empty.
    let after_tool = load_response_doc(&node, &response_doc_id).await;
    assert_eq!(
        after_tool["content"].as_str(),
        Some(""),
        "content must be reset after tool-result persisted"
    );
    assert_eq!(
        after_tool["reasoning"].as_str(),
        Some(""),
        "reasoning must be reset after tool-result persisted"
    );

    // Feed: Text("done") after the tool boundary.
    processor.process_item(text_item("done")).await.unwrap();

    // The new text is live in the tail.
    let after_resume = load_response_doc(&node, &response_doc_id).await;
    assert_eq!(
        after_resume["content"].as_str(),
        Some("done"),
        "post-boundary text must appear in fresh tail"
    );

    // Feed: FinalResponse.
    processor.process_item(final_item("done")).await.unwrap();

    // After FinalResponse the tail is cleared again.
    let after_final = load_response_doc(&node, &response_doc_id).await;
    assert_eq!(
        after_final["content"].as_str(),
        Some(""),
        "content must be cleared after final-response persisted"
    );
    assert_eq!(
        after_final["reasoning"].as_str(),
        Some(""),
        "reasoning must be cleared after final-response persisted"
    );

    stream_writer
        .finalize(&response_doc_id, crate::streaming::StreamStatus::Complete)
        .await
        .expect("publish exact response outcome before terminalization");
    let outcome = node
        .execute(&format!(
            r#"{{
                AgentResponseOutcome(filter: {{ request_doc_id: {{ _eq: "{}" }} }}) {{
                    request_doc_id request_claim_composite_commit_cid outcome_kind
                    final_message_doc_id final_message_composite_commit_cid
                    final_message_signer_did final_message_sequence
                }}
            }}"#,
            crate::graphql::escape_graphql_string(&request_doc_id)
        ))
        .await;
    assert!(!outcome.has_errors(), "{:?}", outcome.errors);
    let rows = outcome.data.as_ref().unwrap()["AgentResponseOutcome"]
        .as_array()
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["outcome_kind"], "complete");
    assert_eq!(rows[0]["request_doc_id"], request_doc_id);
    assert_eq!(rows[0]["final_message_signer_did"], agent_did);
    assert!(rows[0]["final_message_composite_commit_cid"]
        .as_str()
        .is_some_and(|cid| !cid.is_empty()));

    let _ = std::fs::remove_dir_all(&data_path);
}

#[tokio::test]
async fn turn_retraction_resets_live_tail_and_discards_partial_assistant() {
    let data_path = std::env::temp_dir().join(format!(
        "agent-stream-processor-turn-retract-{}",
        uuid::Uuid::new_v4()
    ));
    let (node, _identity) = signed_test_node(&data_path).await;

    let hook = crate::hook::DefraSessionHook::with_identity(
        node.clone(),
        "general",
        "did:test:test",
        FailurePolicy::default(),
    );
    assert!(matches!(
        hook.on_completion_call(&user_text_message("test prompt"), &[])
            .await,
        HookAction::Continue
    ));
    let session_id = hook.session_id().await.expect("session id");

    let request_id = uuid::Uuid::new_v4().to_string();
    let request_doc_id = create_pending_request(&node, &request_id, &session_id).await;
    let request = AgentRequest {
        doc_id: request_doc_id,
        request_id: request_id.clone(),
        agent_did: "did:test:test".to_string(),
        requester_did: None,
        behavior_id: Some("general".to_string()),
        session_id: session_id.clone(),
        content: "test prompt".to_string(),
        temperature: None,
        top_p: None,
        top_k: None,
        max_tokens: None,
        metadata: None,
        execution_origin: None,
        created_at: chrono::Utc::now().to_rfc3339(),
        deadline: None,
        subagent_depth: 0,
        caused_by_parent_request_id: None,
        caused_by_parent_tool_call_id: None,
    };
    let mut lifecycle = RequestLifecycle::new_with_execution_binding(
        node.clone(),
        "general",
        "did:test:test",
        request,
        30,
        ExecutionOrigin::Interactive,
        "test-backend",
    );
    assert_eq!(
        lifecycle.claim_without_identity_for_test().await.unwrap(),
        ClaimOutcome::Claimed
    );

    let stream_writer =
        DefraStreamWriter::new(node.clone(), "did:test:test", Duration::from_millis(0));
    let response_doc_id = stream_writer
        .begin(&session_id, &request_id, "general")
        .await
        .unwrap();
    lifecycle.set_response_doc_id(&response_doc_id).unwrap();
    let mut processor =
        StreamProcessor::new(&hook, &stream_writer, &mut lifecycle, &response_doc_id);

    processor.process_item(text_item("Hel")).await.unwrap();
    let before_retract = load_response_doc(&node, &response_doc_id).await;
    assert_eq!(before_retract["content"].as_str(), Some("Hel"));

    processor
        .process_item(turn_retracted_item(0, 0))
        .await
        .unwrap();
    let after_retract = load_response_doc(&node, &response_doc_id).await;
    assert_eq!(
        after_retract["content"].as_str(),
        Some(""),
        "retraction must clear uncommitted live text"
    );
    assert_eq!(processor.streamed_text, "");

    processor
        .process_item(text_item("Hello world"))
        .await
        .unwrap();
    let after_retry_text = load_response_doc(&node, &response_doc_id).await;
    assert_eq!(
        after_retry_text["content"].as_str(),
        Some("Hello world"),
        "retry text must rebuild the live tail after retraction"
    );

    processor
        .process_item(final_item("Hello world"))
        .await
        .unwrap();
    assert_eq!(processor.streamed_text, "Hello world");

    let history = crate::session::load_history(&node, &session_id)
        .await
        .unwrap();
    let assistant_texts = history
        .iter()
        .filter_map(|message| match message {
            Message::Assistant { content, .. } => content.iter().find_map(|item| match item {
                AssistantContent::Text(text) => Some(text.text.as_str()),
                _ => None,
            }),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        assistant_texts,
        vec!["Hello world"],
        "partial retracted text must not persist as an assistant message"
    );

    let _ = std::fs::remove_dir_all(&data_path);
}

/// #589 durable-history fence: a streamed tool call whose `arguments` the wire
/// parser left as a raw corrupt string (the production poison shape) must
/// persist OBJECT-shaped into `AgentMessage` — the salvageable payload as its
/// intended object, never as a `Value::String` that would jam every subsequent
/// render of the session (#590).
#[tokio::test]
async fn corrupt_tool_call_arguments_persist_object_shaped() {
    let data_path = std::env::temp_dir().join(format!(
        "agent-stream-processor-corrupt-args-{}",
        uuid::Uuid::new_v4()
    ));
    let (node, _identity) = signed_test_node(&data_path).await;

    let hook = DefraSessionHook::with_identity(
        node.clone(),
        "general",
        "did:test:test",
        FailurePolicy::default(),
    );
    assert!(matches!(
        hook.on_completion_call(&user_text_message("describe list_hosts"), &[])
            .await,
        HookAction::Continue
    ));
    let session_id = hook.session_id().await.expect("session id");

    let request_id = uuid::Uuid::new_v4().to_string();
    let request_doc_id = create_pending_request(&node, &request_id, &session_id).await;
    hook.set_active_request_id(Some(request_id.clone())).await;
    hook.set_request_deadline_at(Some(chrono::Utc::now() + chrono::Duration::seconds(60)))
        .await;
    let request = AgentRequest {
        doc_id: request_doc_id,
        request_id: request_id.clone(),
        agent_did: "did:test:test".to_string(),
        requester_did: None,
        behavior_id: Some("general".to_string()),
        session_id: session_id.clone(),
        content: "describe list_hosts".to_string(),
        temperature: None,
        top_p: None,
        top_k: None,
        max_tokens: None,
        metadata: None,
        execution_origin: None,
        created_at: chrono::Utc::now().to_rfc3339(),
        deadline: None,
        subagent_depth: 0,
        caused_by_parent_request_id: None,
        caused_by_parent_tool_call_id: None,
    };
    let mut lifecycle = RequestLifecycle::new_with_execution_binding(
        node.clone(),
        "general",
        "did:test:test",
        request,
        30,
        ExecutionOrigin::Interactive,
        "test-backend",
    );
    assert_eq!(
        lifecycle.claim_without_identity_for_test().await.unwrap(),
        ClaimOutcome::Claimed
    );
    let stream_writer =
        DefraStreamWriter::new(node.clone(), "did:test:test", Duration::from_millis(0));
    let response_doc_id = stream_writer
        .begin(&session_id, &request_id, "general")
        .await
        .unwrap();
    lifecycle.set_response_doc_id(&response_doc_id).unwrap();
    let mut processor =
        StreamProcessor::new(&hook, &stream_writer, &mut lifecycle, &response_doc_id);

    // The wire parser could not shape the corrupt bytes, so the streamed rig
    // ToolCall carries them as a raw Value::String — the exact production shape.
    let corrupt_call: Result<LoopStreamItem<()>, rig::agent::StreamingError> =
        Ok(LoopStreamItem::Item(
            MultiTurnStreamItem::StreamAssistantItem(StreamedAssistantContent::ToolCall {
                tool_call: rig::completion::message::ToolCall {
                    id: "result-1".to_string(),
                    call_id: Some("call-1".to_string()),
                    function: rig::completion::message::ToolFunction {
                        name: "describe_tool".to_string(),
                        arguments: serde_json::Value::String(
                            crate::test_support::CORRUPT_TOOL_ARGS_589.to_string(),
                        ),
                    },
                    signature: None,
                    additional_params: None,
                },
                internal_call_id: "internal-1".to_string(),
            }),
        ));
    processor.process_item(corrupt_call).await.unwrap();

    assert!(matches!(
        hook.on_tool_call(
            "describe_tool",
            Some("call-1".to_string()),
            "internal-1",
            crate::test_support::CORRUPT_TOOL_ARGS_589,
        )
        .await,
        crate::llm::ToolCallHookAction::Continue
    ));
    assert!(matches!(
        hook.on_tool_result(
            "describe_tool",
            Some("call-1".to_string()),
            "internal-1",
            crate::test_support::CORRUPT_TOOL_ARGS_589,
            &crate::tool_call_lifecycle::ToolOutcome::Completed("described:list_hosts".to_string()),
        )
        .await,
        HookAction::Continue
    ));

    processor
        .process_item(tool_result_item_with_call_id(
            "result-1",
            Some("call-1"),
            "described:list_hosts",
            "internal-1",
        ))
        .await
        .unwrap();

    // The durable history's assistant turn carries the SALVAGED object — the
    // intended call — not the raw corrupt string.
    let history = crate::session::load_history(&node, &session_id)
        .await
        .unwrap();
    let arguments = history
        .iter()
        .find_map(|message| match message {
            Message::Assistant { content, .. } => content.iter().find_map(|item| match item {
                AssistantContent::ToolCall(tool_call) => Some(&tool_call.function.arguments),
                _ => None,
            }),
            _ => None,
        })
        .expect("a persisted assistant tool-call message");
    assert!(
        arguments.is_object(),
        "non-object tool-call arguments persisted to durable history: {arguments:?}"
    );
    assert_eq!(
        arguments["tool_name"], "list_hosts",
        "the salvageable #589 payload must persist its intended object"
    );

    let _ = std::fs::remove_dir_all(&data_path);
}
