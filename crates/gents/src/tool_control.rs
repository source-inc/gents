use std::sync::Arc;

use anyhow::Result;
use defra_node::EmbeddedNode;

use crate::hook::BackgroundExecutionRegistry;
use crate::interrupt::interrupt_request;
use crate::tool_call_lifecycle::{AwaitMode, CancelCause, CascadeDispatch, ToolCallLifecycle};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelBackgroundToolCallOutcome {
    Cancelled { live_execution_cancelled: bool },
    AlreadyTerminal { state: String },
    NotBackground,
    NotFound,
}

/// Session-principal boundary for client process controls. Keep authorization
/// identical to the model-facing process tools; the operator API below is
/// deliberately broader and must not be exposed directly to client IDs.
pub async fn cancel_session_background_process(
    node: Arc<EmbeddedNode>,
    executions: &BackgroundExecutionRegistry,
    agent_did: &str,
    requester_did: Option<&str>,
    session_id: &str,
    tool_call_id: &str,
) -> Result<CancelBackgroundToolCallOutcome> {
    let Some(lifecycle) = ToolCallLifecycle::load(node.clone(), session_id, tool_call_id).await?
    else {
        return Ok(CancelBackgroundToolCallOutcome::NotFound);
    };
    let scope = crate::background_tools::ProcessControlScope {
        request_id: String::new(),
        session_id: session_id.into(),
        agent_did: agent_did.into(),
        requester_did: requester_did.map(str::to_owned),
    };
    if !scope.authorizes(
        lifecycle.session_id(),
        lifecycle.agent_did(),
        lifecycle.requester_did(),
    ) || lifecycle.is_subagent_bridge()
    {
        return Ok(CancelBackgroundToolCallOutcome::NotFound);
    }
    cancel_background_tool_call(node, executions, agent_did, session_id, tool_call_id).await
}

pub async fn cancel_background_tool_call(
    node: Arc<EmbeddedNode>,
    background_executions: &BackgroundExecutionRegistry,
    agent_did: &str,
    session_id: &str,
    tool_call_id: &str,
) -> Result<CancelBackgroundToolCallOutcome> {
    let Some(mut lifecycle) =
        ToolCallLifecycle::load(node.clone(), session_id, tool_call_id).await?
    else {
        return Ok(CancelBackgroundToolCallOutcome::NotFound);
    };

    if lifecycle.await_mode() != AwaitMode::Background {
        return Ok(CancelBackgroundToolCallOutcome::NotBackground);
    }
    if lifecycle.is_terminal() {
        return Ok(CancelBackgroundToolCallOutcome::AlreadyTerminal {
            state: lifecycle.state().as_str().to_string(),
        });
    }

    let persisted = lifecycle
        .cancel_during_run_with_cascade_dispatch(CancelCause::UserCancelled, agent_did)
        .await;
    // Persist the operator-authored terminal cause before signalling the live
    // worker. Otherwise the worker can observe cancellation first and win the
    // terminal write with the less-specific `interrupted` cause. A persistence
    // failure must still stop the live work: cancellation is best-effort state
    // control, not contingent on observability storage being available.
    let live_execution_cancelled = background_executions.cancel(tool_call_id).await;
    let dispatch = match persisted {
        Ok(dispatch) => dispatch,
        Err(error) => {
            tracing::error!(
                tool_call_id,
                live_execution_cancelled,
                %error,
                "failed to persist background cancellation after stopping live execution",
            );
            return Err(error);
        }
    };

    if let Some(CascadeDispatch::Local(intent)) = dispatch {
        interrupt_request(node.as_ref(), &intent.child_request_id).await?;
    }

    if lifecycle.is_cancelled() {
        Ok(CancelBackgroundToolCallOutcome::Cancelled {
            live_execution_cancelled,
        })
    } else {
        Ok(CancelBackgroundToolCallOutcome::AlreadyTerminal {
            state: lifecycle.state().as_str().to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ensure_runtime_schemas;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn cancel_background_tool_call_terminalizes_row_and_token() {
        let data_path = std::env::temp_dir().join(format!(
            "agent-tool-control-cancel-{}",
            uuid::Uuid::new_v4()
        ));
        let node = Arc::new(
            defra_node::EmbeddedNode::builder()
                .data_path(&data_path)
                .build()
                .await
                .unwrap(),
        );
        ensure_runtime_schemas(&node).await.unwrap();

        let registry = BackgroundExecutionRegistry::default();
        let token = CancellationToken::new();
        registry
            .reserve("tool-1".to_string(), token.clone())
            .disarm();

        let mut lifecycle = ToolCallLifecycle::new_background_tool(
            node.clone(),
            "request-1".to_string(),
            "session-1".to_string(),
            "did:test:test".to_string(),
            "tool-1".to_string(),
            1,
            "bash_unrestricted".to_string(),
            "{}".to_string(),
            chrono::Utc::now() + chrono::Duration::minutes(5),
        );
        lifecycle.start_running().await.unwrap();

        let denied = cancel_session_background_process(
            node.clone(),
            &registry,
            "did:test:test",
            Some("foreign"),
            "session-1",
            "tool-1",
        )
        .await
        .unwrap();
        assert_eq!(denied, CancelBackgroundToolCallOutcome::NotFound);
        assert!(
            !token.is_cancelled(),
            "unauthorized UI cancellation must not signal the worker"
        );

        let outcome = cancel_session_background_process(
            node.clone(),
            &registry,
            "did:test:test",
            None,
            "session-1",
            "tool-1",
        )
        .await
        .unwrap();

        assert_eq!(
            outcome,
            CancelBackgroundToolCallOutcome::Cancelled {
                live_execution_cancelled: true
            }
        );
        assert!(token.is_cancelled());

        let row = ToolCallLifecycle::load(node.clone(), "session-1", "tool-1")
            .await
            .unwrap()
            .expect("tool row");
        assert!(row.is_cancelled());

        let _ = std::fs::remove_dir_all(&data_path);
    }

    #[tokio::test]
    async fn owned_cancel_persists_custom_completion_reason_for_redrive() {
        let data_path = std::env::temp_dir().join(format!(
            "agent-tool-control-custom-cancel-{}",
            uuid::Uuid::new_v4()
        ));
        let node = Arc::new(
            defra_node::EmbeddedNode::builder()
                .data_path(&data_path)
                .build()
                .await
                .unwrap(),
        );
        ensure_runtime_schemas(&node).await.unwrap();

        let mut lifecycle = ToolCallLifecycle::new_background_tool(
            node.clone(),
            "request-custom".to_string(),
            "session-custom".to_string(),
            "did:test:test".to_string(),
            "tool-custom".to_string(),
            1,
            "bash_unrestricted".to_string(),
            "{}".to_string(),
            chrono::Utc::now() + chrono::Duration::minutes(5),
        );
        lifecycle.start_running().await.unwrap();
        assert!(lifecycle
            .cancel_during_run_owned(CancelCause::UserCancelled, "operator requested drain")
            .await
            .unwrap());

        let response = node
            .execute(
                r#"{
                    AgentToolCall(filter: { tool_call_id: { _eq: "tool-custom" } }, limit: 1) {
                        status
                    }
                }"#,
            )
            .await;
        assert!(!response.has_errors(), "{:?}", response.errors);
        let status = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentToolCall"))
            .and_then(serde_json::Value::as_array)
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("status"))
            .and_then(serde_json::Value::as_str);
        assert_eq!(status, Some("completionPending:operator requested drain"));

        let _ = std::fs::remove_dir_all(&data_path);
    }
}
