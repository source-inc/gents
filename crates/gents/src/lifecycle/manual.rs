use anyhow::{anyhow, Result};
use defra_node::EmbeddedNode;
use serde_json::Value;

use crate::lifecycle::{
    write_pending_agent_request_with_lineage_and_conversation_title, ExecutionOrigin,
    TriggerLineage,
};
use crate::template::{render_template, task_node_ctx, TemplateScope};

pub async fn write_manual_agent_request(
    node: &EmbeddedNode,
    agent_did: &str,
    behavior_id: &str,
    task_id: &str,
    prompt_template: &str,
    args: Value,
) -> Result<String> {
    write_manual_agent_request_with_conversation_title(
        node,
        agent_did,
        behavior_id,
        task_id,
        prompt_template,
        args,
        None,
    )
    .await
}

pub async fn write_manual_agent_request_with_conversation_title(
    node: &EmbeddedNode,
    agent_did: &str,
    behavior_id: &str,
    task_id: &str,
    prompt_template: &str,
    args: Value,
    conversation_title: Option<&str>,
) -> Result<String> {
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let (node_scope, ctx_scope) = task_node_ctx(agent_did, behavior_id, &now);
    let scope = TemplateScope {
        event: serde_json::json!({
            "fired_at": now,
            "trigger_id": serde_json::Value::Null,
            "trigger_kind": "manual",
        }),
        doc: None,
        args: Some(args),
        group: None,
        node: node_scope,
        ctx: ctx_scope,
    };
    let content = render_template(prompt_template, &scope)
        .map_err(|e| anyhow!("render manual template for task {task_id}: {e}"))?;

    let enqueued = write_pending_agent_request_with_lineage_and_conversation_title(
        node,
        agent_did,
        behavior_id,
        &content,
        ExecutionOrigin::Interactive,
        TriggerLineage {
            trigger_id: None,
            trigger_kind: Some("manual".to_string()),
            source_doc_id: None,
            correlation: None,
            trigger_context: None,
        },
        conversation_title,
    )
    .await
    .map_err(|e| anyhow!("create manual AgentRequest for task {task_id}: {e}"))?;

    tracing::info!(
        task_id = %task_id,
        request_id = %enqueued.request_id,
        session_id = %enqueued.session_id,
        doc_id = %enqueued.doc_id,
        "manual task run enqueued as AgentRequest"
    );
    Ok(enqueued.doc_id)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use defra_node::EmbeddedNode;
    use serde_json::Value;

    use super::{write_manual_agent_request, write_manual_agent_request_with_conversation_title};
    use crate::schema::ensure_schemas;

    async fn test_node() -> Arc<EmbeddedNode> {
        let node = Arc::new(EmbeddedNode::builder().build().await.unwrap());
        ensure_schemas(node.as_ref()).await.unwrap();
        node
    }

    #[tokio::test]
    async fn writes_manual_request_with_rendered_template_and_manual_lineage() {
        let node = test_node().await;

        let doc_id = write_manual_agent_request(
            node.as_ref(),
            "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
            "behavior-1",
            "task-1",
            "hello {{ args.name }}",
            serde_json::json!({"name": "Amy"}),
        )
        .await
        .unwrap();
        assert!(!doc_id.is_empty());

        let query = format!(
            r#"{{
                AgentRequest(filter: {{ _docID: {{ _eq: "{doc_id}" }} }}) {{
                    _docID
                    content
                    caused_by_trigger_kind
                    caused_by_trigger_id
                    execution_origin
                    lifecycle_state
                    status
                    metadata
                }}
            }}"#
        );
        let response = node.execute(&query).await;
        assert!(
            !response.has_errors(),
            "query failed: {:?}",
            response.errors
        );

        let rows = response
            .data
            .as_ref()
            .and_then(|d| d.get("AgentRequest"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        assert_eq!(rows.len(), 1, "expected exactly one row, got {rows:?}");
        let row = &rows[0];

        assert_eq!(row["content"].as_str(), Some("hello Amy"));
        assert_eq!(row["caused_by_trigger_kind"].as_str(), Some("manual"));
        assert!(
            row["caused_by_trigger_id"].is_null(),
            "caused_by_trigger_id should be null for manual runs, got {:?}",
            row["caused_by_trigger_id"]
        );
        assert_eq!(row["execution_origin"].as_str(), Some("interactive"));
        assert_eq!(row["lifecycle_state"].as_str(), Some("pending"));
        assert_eq!(row["status"].as_str(), Some("pending"));
        assert!(row["metadata"].is_null());
    }

    #[tokio::test]
    async fn manual_task_template_gets_node_and_ctx_scope() {
        let node = test_node().await;

        let agent_did = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
        let doc_id = write_manual_agent_request(
            node.as_ref(),
            agent_did,
            "behavior-1",
            "task-1",
            "tick {{ ctx.now }} on {{ node.node_did }} / {{ node.behavior_id }}",
            serde_json::json!({}),
        )
        .await
        .unwrap();

        let query = format!(
            r#"{{
                AgentRequest(filter: {{ _docID: {{ _eq: "{doc_id}" }} }}, limit: 1) {{
                    content
                }}
            }}"#
        );
        let response = node.execute(&query).await;
        assert!(
            !response.has_errors(),
            "query failed: {:?}",
            response.errors
        );
        let content = response
            .data
            .as_ref()
            .and_then(|d| d.get("AgentRequest"))
            .and_then(Value::as_array)
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("content"))
            .and_then(Value::as_str)
            .expect("request content");

        assert!(content.starts_with("tick "));
        assert!(content.contains(agent_did), "content={content:?}");
        assert!(content.ends_with(" / behavior-1"), "content={content:?}");
    }

    #[tokio::test]
    async fn writes_manual_request_with_slash_selected_skill_metadata() {
        let node = test_node().await;

        let doc_id = write_manual_agent_request(
            node.as_ref(),
            "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
            "behavior-1",
            "task-1",
            "/vuln-scan\nReview /work",
            serde_json::json!({}),
        )
        .await
        .unwrap();

        let query = format!(
            r#"{{
                AgentRequest(filter: {{ _docID: {{ _eq: "{doc_id}" }} }}, limit: 1) {{
                    content
                    metadata
                }}
            }}"#
        );
        let response = node.execute(&query).await;
        assert!(
            !response.has_errors(),
            "query failed: {:?}",
            response.errors
        );
        let row = response
            .data
            .as_ref()
            .and_then(|d| d.get("AgentRequest"))
            .and_then(Value::as_array)
            .and_then(|rows| rows.first())
            .expect("expected request row");

        assert_eq!(row["content"].as_str(), Some("Review /work"));
        assert_eq!(
            row["metadata"].as_str(),
            Some(r#"{"selected_skill_ids":["vuln-scan"]}"#)
        );
    }

    #[tokio::test]
    async fn writes_manual_request_with_deferred_conversation_title() {
        let node = test_node().await;

        let doc_id = write_manual_agent_request_with_conversation_title(
            node.as_ref(),
            "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
            "behavior-1",
            "task-1",
            "hello {{ args.name }}",
            serde_json::json!({"name": "Amy"}),
            Some("mini-host-health-20260430t180405z"),
        )
        .await
        .unwrap();

        let query = format!(
            r#"{{
                AgentRequest(filter: {{ _docID: {{ _eq: "{doc_id}" }} }}, limit: 1) {{
                    session_id
                    metadata
                }}
            }}"#
        );
        let response = node.execute(&query).await;
        assert!(
            !response.has_errors(),
            "request query failed: {:?}",
            response.errors
        );
        let request = response
            .data
            .as_ref()
            .and_then(|d| d.get("AgentRequest"))
            .and_then(Value::as_array)
            .and_then(|rows| rows.first())
            .expect("request row");
        let session_id = request
            .get("session_id")
            .and_then(Value::as_str)
            .expect("request should have session_id");
        let metadata = request
            .get("metadata")
            .and_then(Value::as_str)
            .and_then(|value| serde_json::from_str::<Value>(value).ok())
            .expect("request should carry projection metadata");
        assert_eq!(
            metadata.get("conversation_title").and_then(Value::as_str),
            Some("mini-host-health-20260430t180405z")
        );

        let conversation_query = format!(
            r#"{{
                AgentConversation(
                    filter: {{ session_id: {{ _eq: "{session_id}" }} }},
                    limit: 1
                ) {{
                    title
                    title_source
                    status
                }}
            }}"#
        );
        let conversation_response = node.execute(&conversation_query).await;
        assert!(
            !conversation_response.has_errors(),
            "conversation query failed: {:?}",
            conversation_response.errors
        );
        let conversations = conversation_response
            .data
            .as_ref()
            .and_then(|d| d.get("AgentConversation"))
            .and_then(Value::as_array)
            .expect("conversation rows");
        assert!(
            conversations.is_empty(),
            "enqueue must persist only AgentRequest; the runtime projects AgentConversation"
        );
    }

    #[tokio::test]
    async fn surfaces_template_render_errors() {
        let node = test_node().await;
        let err = write_manual_agent_request(
            node.as_ref(),
            "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
            "behavior-1",
            "task-err",
            "hello {{ args.missing }}",
            serde_json::json!({}),
        )
        .await
        .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("render manual template for task task-err"),
            "expected render error context, got: {msg}"
        );
    }
}
