use super::*;
use tempfile::TempDir;

const TEST_AGENT_DID: &str = "did:test:queue-test";
const TEST_BEHAVIOR_ID: &str = "general";

struct TestDb {
    node: EmbeddedNode,
    _tempdir: TempDir,
}

#[derive(Debug, Deserialize)]
struct QueueRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    request_id: String,
    session_id: String,
    behavior_id: String,
    content: String,
    metadata: Option<String>,
    status: String,
    lifecycle_state: Option<String>,
    execution_origin: String,
    superseded_by_request: Option<String>,
    superseded_by_request_doc_id: Option<String>,
    subagent_depth: Option<u32>,
    caused_by_parent_request_id: Option<String>,
    caused_by_parent_request_doc_id: Option<String>,
    caused_by_parent_tool_call_id: Option<String>,
    caused_by_parent_tool_call_doc_id: Option<String>,
}

fn hints(source: QueueSource, policy: QueuePolicy) -> QueueHints {
    QueueHints {
        source,
        policy,
        key: Some("session:sess-1".to_string()),
        queued_after_request_id: Some("req-1".to_string()),
        interrupted_request_id: None,
    }
}

fn parent_request(session_id: &str) -> AgentRequest {
    AgentRequest {
        doc_id: "parent-doc".to_string(),
        request_id: "parent-request".to_string(),
        agent_did: TEST_AGENT_DID.to_string(),
        requester_did: None,
        behavior_id: Some(TEST_BEHAVIOR_ID.to_string()),
        session_id: session_id.to_string(),
        content: "parent".to_string(),
        temperature: None,
        top_p: None,
        top_k: None,
        seed: None,
        max_tokens: None,
        max_total_tokens: None,
        metadata: None,
        execution_origin: Some("interactive".to_string()),
        created_at: chrono::Utc::now().to_rfc3339(),
        deadline: None,
        subagent_depth: 2,
        caused_by_parent_request_id: Some("root-parent-request".to_string()),
        caused_by_parent_request_doc_id: Some("root-parent-request-doc".to_string()),
        caused_by_parent_tool_call_id: Some("root-parent-tool-call".to_string()),
        caused_by_parent_tool_call_doc_id: Some("root-parent-tool-call-doc".to_string()),
        caused_by_trigger_id: None,
        caused_by_trigger_kind: None,
        caused_by_source_doc_id: None,
        caused_by_correlation: None,
        caused_by_trigger_context: None,
        workspace_id: None,
        workspace_authority: None,
        workspace_owner_deployment_id: None,
        workspace_seal_hash: None,
    }
}

#[test]
fn transaction_create_doc_id_accepts_both_defradb_response_shapes() {
    for response in [
        serde_json::json!({
            "data": { "create_AgentRequest": { "_docID": "doc-object" } }
        }),
        serde_json::json!({
            "data": { "add_AgentRequest": [{ "_docID": "doc-array" }] }
        }),
    ] {
        let doc_id = transaction_created_doc_id(&response, "AgentRequest").unwrap();
        assert!(doc_id == "doc-object" || doc_id == "doc-array");
    }
}

#[test]
fn request_mutation_rejects_a_half_bound_parent_edge() {
    let mut parent = parent_request("session-half-bound");
    parent.caused_by_parent_tool_call_id = None;
    let result = session_request_create_mutation(
        &parent,
        TEST_BEHAVIOR_ID,
        "continue",
        ExecutionOrigin::Scheduled,
        "{}",
        "request-next",
        "2026-08-10T00:00:00Z",
        false,
    );
    assert!(result.is_err());
}

async fn test_db(name: &str) -> TestDb {
    let tempdir = tempfile::Builder::new()
        .prefix(&format!("gents-queue-{name}-"))
        .tempdir()
        .expect("tempdir");
    let node = EmbeddedNode::builder()
        .data_path(tempdir.path())
        .build()
        .await
        .expect("embedded node");
    crate::schema::ensure_runtime_schemas(&node)
        .await
        .expect("runtime schemas");
    TestDb {
        node,
        _tempdir: tempdir,
    }
}

async fn queue_rows(node: &EmbeddedNode, session_id: &str) -> Vec<QueueRow> {
    let escaped_session_id = escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ session_id: {{ _eq: "{escaped_session_id}" }} }},
                order: [{{ created_at: ASC }}, {{ request_id: ASC }}]
            ) {{
                _docID
                request_id
                session_id
                behavior_id
                content
                metadata
                status
                lifecycle_state
                execution_origin
                superseded_by_request
                superseded_by_request_doc_id
                subagent_depth
                caused_by_parent_request_id
                caused_by_parent_request_doc_id
                caused_by_parent_tool_call_id
                caused_by_parent_tool_call_doc_id
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "queue row query failed: {:?}",
        response.errors
    );
    response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default()
}

async fn insert_raw_queue_request(
    node: &EmbeddedNode,
    request_id: &str,
    session_id: &str,
    metadata: &str,
) -> String {
    let escaped_request_id = escape_graphql_string(request_id);
    let escaped_session_id = escape_graphql_string(session_id);
    let escaped_metadata = escape_graphql_string(metadata);
    let created_at = chrono::Utc::now().to_rfc3339();
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{escaped_request_id}",
                agent_did: "{TEST_AGENT_DID}",
                behavior_id: "{TEST_BEHAVIOR_ID}",
                session_id: "{escaped_session_id}",
                retry_parent_request: "",
                retry_root_request: "{escaped_request_id}",
                superseded_by_request: "",
                content: "raw duplicate",
                metadata: "{escaped_metadata}",
                status: "pending",
                lifecycle_state: "pending",
                backend_id: "",
                execution_origin: "scheduled",
                failure_reason: "",
                created_at: "{created_at}",
                retry_count: 0,
                max_retries: {max_retries},
                subagent_depth: 0
            }}) {{ _docID }}
        }}"#,
        max_retries = DEFAULT_REQUEST_MAX_RETRIES,
    );
    let response =
        session::execute_mutation_with_retry(node, &mutation, "insert_raw_queue_request")
            .await
            .unwrap();
    extract_single_doc_id(&response, "create_AgentRequest")
        .expect("raw queue create returns _docID")
}

#[tokio::test]
async fn request_doc_lookup_rejects_duplicate_logical_request_ids() {
    let db = test_db("ambiguous-request-doc-lookup").await;
    let metadata = queue_metadata_json(&hints(QueueSource::User, QueuePolicy::Append));
    insert_raw_queue_request(&db.node, "duplicate-logical-id", "session-a", &metadata).await;
    insert_raw_queue_request(&db.node, "duplicate-logical-id", "session-b", &metadata).await;

    let error = lookup_request_doc_id_optional(&db.node, "duplicate-logical-id")
        .await
        .expect_err("duplicate logical ids must not resolve to an arbitrary document");
    assert!(error.to_string().contains("ambiguous across 2 documents"));
}

#[tokio::test]
async fn control_parent_normalization_recovers_legacy_logical_request_edge() {
    let db = test_db("legacy-control-parent").await;
    let root_doc =
        insert_raw_queue_request(&db.node, "legacy-root-request", "legacy-root-session", "{}")
            .await;
    let mut parent = parent_request("legacy-child-session");
    parent.caused_by_parent_request_id = Some("legacy-root-request".to_string());
    parent.caused_by_parent_request_doc_id = None;

    let normalized = normalize_request_only_control_parent(&db.node, &parent)
        .await
        .unwrap();
    assert_eq!(
        normalized.caused_by_parent_request_doc_id.as_deref(),
        Some(root_doc.as_str())
    );
    assert!(normalized.caused_by_parent_tool_call_id.is_none());
    assert!(normalized.caused_by_parent_tool_call_doc_id.is_none());
}

mod background_completion;
mod coalescing;
mod metadata;
