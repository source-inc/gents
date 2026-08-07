#![allow(dead_code)]

use std::sync::Arc;

use gents::defra_node::{EmbeddedNode, P2PConfig, QueryResponse};
use gents::graphql::escape_graphql_string;
use gents::{ensure_runtime_schemas, watcher::AgentRequest};
use serde::Deserialize;
use tempfile::TempDir;

pub mod conformance_consumers;
pub mod fixtures;
pub mod http_mock;
pub(crate) mod identity_stubs;
pub mod interrupt;
pub mod mock_endpoint;
pub mod mock_subscription;
pub mod p2p_waits;
pub mod pairing_conformance;
pub mod r5_conformance;
pub mod snapshots;
pub mod streaming_backend;
pub mod waits;

pub const AGENT_DID: &str = "did:test:test";
pub const AGENT_NAME: &str = "test";
pub const BACKEND_ID: &str = "backend-test";
pub const DEADLINE_SECS: u64 = 300;

pub struct TestDb {
    pub node: Arc<EmbeddedNode>,
    pub process_generation: u64,
    tempdir: TempDir,
}

impl TestDb {
    pub async fn simulate_process_crash(&mut self) -> anyhow::Result<()> {
        let data_path = self.tempdir.path().to_path_buf();
        let before = self.process_generation;

        let strong = Arc::strong_count(&self.node);
        if strong != 1 {
            anyhow::bail!(
                "simulate_process_crash: cannot exclusively drop EmbeddedNode \
                 (strong_count={strong}); crash boundary would not clear process state"
            );
        }

        self.node.shutdown().await;

        let stand_in = Arc::new(
            EmbeddedNode::builder()
                .build()
                .await
                .map_err(|e| anyhow::anyhow!("simulate_process_crash: stand-in node: {e}"))?,
        );
        let old = std::mem::replace(&mut self.node, stand_in);

        match Arc::try_unwrap(old) {
            Ok(owned) => drop(owned),
            Err(shared) => {
                let count = Arc::strong_count(&shared);
                self.node = shared;
                anyhow::bail!(
                    "simulate_process_crash: cannot exclusively drop EmbeddedNode \
                     (strong_count={count} after replace); restored handle is shut \
                     down and unusable — fix outstanding Arc clones before Crash"
                );
            }
        }

        let reopened = EmbeddedNode::builder()
            .data_path(&data_path)
            .build()
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "simulate_process_crash: reopen durable store at {} failed: {e}",
                    data_path.display()
                )
            })?;
        self.node = Arc::new(reopened);

        ensure_runtime_schemas(&self.node)
            .await
            .map_err(|e| anyhow::anyhow!("simulate_process_crash: ensure schemas: {e}"))?;

        self.process_generation = before + 1;
        Ok(())
    }
}

pub async fn test_db(name: &str) -> TestDb {
    let tempdir = tempfile::Builder::new()
        .prefix(&format!("gents-{name}-"))
        .tempdir()
        .expect("tempdir");
    let node = Arc::new(
        EmbeddedNode::builder()
            .data_path(tempdir.path())
            .build()
            .await
            .expect("embedded node"),
    );
    ensure_runtime_schemas(&node)
        .await
        .expect("runtime schemas");
    TestDb {
        node,
        process_generation: 0,
        tempdir,
    }
}

/// `AgentConversation` as it exists on stores that predate the unique
/// `session_id` index — the shape that produced #693 in the field.
///
/// The shipped schema declares `session_id: String @index(unique: true)`, and
/// DefraDB enforces it on create: a duplicate cannot be minted on a fresh
/// store. But DefraDB also cannot add an index to an *existing* collection
/// (`add_schema` short-circuits), so a store whose `AgentConversation` was
/// first registered without the unique index keeps duplicates forever. This SDL
/// reproduces that store. Identical to the shipped schema except the index on
/// `session_id` is not unique.
pub const AGENT_CONVERSATION_NON_UNIQUE_SESSION_ID: &str = r#"
type AgentConversation @branchable {
    session_id: String @index
    agent_name: String @index
    agent_did: String @index @immutable
    behavior_id: String @index
    title: String
    title_source: String
    preview_text: String
    status: String @index
    created_at: DateTime @index(direction: DESC)
    updated_at: DateTime @index(direction: DESC)
    latest_request_id: String @index
    forked_from_session_id: String @index
    fork_at_user_turn: Int
    forked_at: DateTime
}
"#;

pub async fn test_db_with_duplicate_tolerant_conversations(name: &str) -> TestDb {
    let tempdir = tempfile::Builder::new()
        .prefix(&format!("gents-{name}-"))
        .tempdir()
        .expect("tempdir");
    let node = Arc::new(
        EmbeddedNode::builder()
            .data_path(tempdir.path())
            .build()
            .await
            .expect("embedded node"),
    );
    for schema in gents_protocol::schemas::RUNTIME_ALL
        .iter()
        .chain(gents_protocol::schemas::ALL.iter())
    {
        let fixture_schema = if *schema == gents_protocol::schemas::AGENT_CONVERSATION {
            AGENT_CONVERSATION_NON_UNIQUE_SESSION_ID
        } else {
            *schema
        };
        node.add_schema(fixture_schema)
            .await
            .expect("duplicate-tolerant fixture schema");
    }
    TestDb {
        node,
        process_generation: 0,
        tempdir,
    }
}

/// Raw `create_AgentConversation`, bypassing the upsert paths that would
/// collapse duplicates. Returns the new `_docID`.
///
/// Two rows sharing a `session_id` must differ in at least one other field:
/// DefraDB derives the docID from the content, so identical rows would collapse
/// into one document rather than duplicate.
#[allow(clippy::too_many_arguments)]
pub async fn create_conversation_row(
    node: &EmbeddedNode,
    session_id: &str,
    title: &str,
    preview_text: &str,
    status: &str,
    created_at: &str,
    updated_at: &str,
    latest_request_id: &str,
) -> String {
    let mutation = format!(
        r#"mutation {{
            create_AgentConversation(input: {{
                session_id: "{session_id}",
                agent_name: "{agent_name}",
                agent_did: "{agent_did}",
                behavior_id: "{behavior_id}",
                title: "{title}",
                title_source: "placeholder",
                preview_text: "{preview_text}",
                status: "{status}",
                created_at: "{created_at}",
                updated_at: "{updated_at}",
                latest_request_id: "{latest_request_id}"
            }}) {{ _docID }}
        }}"#,
        session_id = escape_graphql_string(session_id),
        agent_name = escape_graphql_string(AGENT_NAME),
        agent_did = escape_graphql_string(AGENT_DID),
        behavior_id = escape_graphql_string(AGENT_NAME),
        title = escape_graphql_string(title),
        preview_text = escape_graphql_string(preview_text),
        status = escape_graphql_string(status),
        created_at = escape_graphql_string(created_at),
        updated_at = escape_graphql_string(updated_at),
        latest_request_id = escape_graphql_string(latest_request_id),
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "create_AgentConversation failed: {:?}",
        resp.errors
    );
    let payload = resp
        .data
        .as_ref()
        .and_then(|data| {
            data.get("create_AgentConversation")
                .or_else(|| data.get("add_AgentConversation"))
        })
        .unwrap_or_else(|| panic!("create_AgentConversation payload missing: {:?}", resp.data));
    let row = match payload {
        serde_json::Value::Array(rows) => rows.first().cloned().unwrap_or_default(),
        other => other.clone(),
    };
    row.get("_docID")
        .and_then(|value| value.as_str())
        .unwrap_or_else(|| panic!("created conversation _docID missing in {row:?}"))
        .to_string()
}

pub async fn conversation_status_by_doc_id(node: &EmbeddedNode, doc_id: &str) -> String {
    let query = format!(
        r#"{{
            AgentConversation(filter: {{ _docID: {{ _eq: "{doc_id}" }} }}) {{ status }}
        }}"#,
        doc_id = escape_graphql_string(doc_id),
    );
    let resp = node.execute(&query).await;
    assert!(!resp.has_errors(), "status query failed: {:?}", resp.errors);
    resp.data
        .as_ref()
        .and_then(|data| data.get("AgentConversation"))
        .and_then(|value| value.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("status"))
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string()
}

#[derive(Debug, Clone)]
pub struct TestP2pAdmission {
    pub max_concurrent_push_tasks: usize,
    pub max_concurrent_dag_fetches: usize,
    pub max_pending_dags: usize,
    pub rate_limit_burst: u32,
    pub rate_limit_rate: f64,
}

impl Default for TestP2pAdmission {
    fn default() -> Self {
        Self {
            max_concurrent_push_tasks: p2p::sync::DEFAULT_MAX_CONCURRENT_PUSH_TASKS,
            max_concurrent_dag_fetches: p2p::sync::DEFAULT_MAX_CONCURRENT_DAG_FETCHES,
            max_pending_dags: p2p::sync::DEFAULT_MAX_PENDING_DAGS,
            rate_limit_burst: p2p::sync::DEFAULT_RATE_LIMIT_BURST,
            rate_limit_rate: p2p::sync::DEFAULT_RATE_LIMIT_RATE,
        }
    }
}

impl TestP2pAdmission {
    pub fn single_push_worker() -> Self {
        Self {
            max_concurrent_push_tasks: 1,
            ..Self::default()
        }
    }
}

pub async fn test_p2p_db(name: &str) -> TestDb {
    test_p2p_db_with_admission(name, TestP2pAdmission::default()).await
}

pub async fn test_p2p_db_with_admission(name: &str, admission: TestP2pAdmission) -> TestDb {
    let tempdir = tempfile::Builder::new()
        .prefix(&format!("gents-{name}-"))
        .tempdir()
        .expect("tempdir");
    let node = Arc::new(
        EmbeddedNode::builder()
            .data_path(tempdir.path())
            .with_p2p(P2PConfig {
                port: 0,
                bind_addr: Some(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
                relay_mode: p2p::iroh::IrohRelayModeConfig::Disabled,
                discovery: p2p::iroh::IrohDiscoveryConfig::Disabled,
                max_concurrent_multipath_paths: None,
                secret_key_path: None,
                load_persisted_collections: false,
                max_concurrent_dag_fetches: admission.max_concurrent_dag_fetches,
                max_concurrent_push_tasks: admission.max_concurrent_push_tasks,
                rate_limit_burst: admission.rate_limit_burst,
                rate_limit_rate: admission.rate_limit_rate,
                max_doc_sync_request_doc_ids: p2p::sync::DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS,
                max_pending_dags: admission.max_pending_dags,
            })
            .build()
            .await
            .expect("embedded p2p node"),
    );
    ensure_runtime_schemas(&node)
        .await
        .expect("runtime schemas");
    TestDb {
        node,
        process_generation: 0,
        tempdir,
    }
}

pub async fn create_request(
    node: &EmbeddedNode,
    request_id: &str,
    session_id: &str,
    status: &str,
    created_at: &str,
) -> String {
    let lifecycle_state = match status {
        "pending" => "pending",
        "processing" => "processing",
        "completed" => "completed",
        "error" => "failed",
        "superseded" => "superseded",
        "interrupted" => "interrupted",
        other => panic!("unsupported test request status: {other}"),
    };
    let request_id = escape_graphql_string(request_id);
    let session_id = escape_graphql_string(session_id);
    let created_at = escape_graphql_string(created_at);
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{request_id}",
                agent_did: "{AGENT_DID}",
                behavior_id: "{AGENT_NAME}",
                session_id: "{session_id}",
                retry_parent_request: "",
                retry_root_request: "{request_id}",
                superseded_by_request: "",
                content: "hello",
                status: "{status}",
                lifecycle_state: "{lifecycle_state}",
                backend_id: "",
                execution_origin: "interactive",
                created_at: "{created_at}",
                retry_count: 0,
                max_retries: {max_retries}
            }}) {{ _docID }}
        }}"#,
        max_retries = gents::lifecycle::DEFAULT_REQUEST_MAX_RETRIES,
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "create request failed: {:?}",
        resp.errors
    );

    let query = format!(
        r#"{{
            AgentRequest(filter: {{ request_id: {{ _eq: "{request_id}" }} }}) {{
                _docID
            }}
        }}"#
    );
    let resp = node.execute(&query).await;
    first_row::<DocIdRow>(&resp, "AgentRequest").doc_id
}

pub async fn create_retry_request(
    node: &EmbeddedNode,
    request_id: &str,
    session_id: &str,
    retry_parent_request: &str,
    retry_root_request: &str,
    content: &str,
    created_at: &str,
) -> String {
    let request_id_escaped = escape_graphql_string(request_id);
    let session_id_escaped = escape_graphql_string(session_id);
    let retry_parent_escaped = escape_graphql_string(retry_parent_request);
    let retry_root_escaped = escape_graphql_string(retry_root_request);
    let content_escaped = escape_graphql_string(content);
    let created_at_escaped = escape_graphql_string(created_at);
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{request_id_escaped}",
                agent_did: "{AGENT_DID}",
                behavior_id: "{AGENT_NAME}",
                session_id: "{session_id_escaped}",
                retry_parent_request: "{retry_parent_escaped}",
                retry_root_request: "{retry_root_escaped}",
                superseded_by_request: "",
                content: "{content_escaped}",
                status: "pending",
                lifecycle_state: "pending",
                backend_id: "",
                execution_origin: "interactive",
                created_at: "{created_at_escaped}",
                retry_count: 0,
                max_retries: {max_retries}
            }}) {{ _docID }}
        }}"#,
        max_retries = gents::lifecycle::DEFAULT_REQUEST_MAX_RETRIES,
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "create retry request failed: {:?}",
        resp.errors
    );

    let query = format!(
        r#"{{
            AgentRequest(filter: {{ request_id: {{ _eq: "{request_id_escaped}" }} }}) {{
                _docID
            }}
        }}"#
    );
    let resp = node.execute(&query).await;
    first_row::<DocIdRow>(&resp, "AgentRequest").doc_id
}

pub async fn create_response_with_status(
    node: &EmbeddedNode,
    response_key: &str,
    request_id: &str,
    session_id: &str,
    status: &str,
) -> String {
    create_response_with_content_and_status(node, response_key, request_id, session_id, "", status)
        .await
}

pub async fn create_response_with_content_and_status(
    node: &EmbeddedNode,
    response_key: &str,
    request_id: &str,
    session_id: &str,
    content: &str,
    status: &str,
) -> String {
    let response_key = escape_graphql_string(response_key);
    let request_id = escape_graphql_string(request_id);
    let session_id = escape_graphql_string(session_id);
    let content = escape_graphql_string(content);
    let completed_at = if matches!(status, "complete" | "error") {
        "2026-03-23T00:01:00Z"
    } else {
        ""
    };
    let mutation = format!(
        r#"mutation {{
            create_AgentResponse(input: {{
                response_key: "{response_key}",
                request_id: "{request_id}",
                agent_did: "{AGENT_DID}",
                behavior_id: "{AGENT_NAME}",
                session_id: "{session_id}",
                content: "{content}",
                status: "{status}",
                token_count: 0,
                progress_seq: 0,
                created_at: "2026-03-23T00:00:00Z",
                completed_at: "{completed_at}"
            }}) {{ _docID }}
        }}"#,
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "create response failed: {:?}",
        resp.errors
    );

    let query = format!(
        r#"{{
            AgentResponse(filter: {{ response_key: {{ _eq: "{response_key}" }} }}) {{
                _docID
            }}
        }}"#
    );
    let resp = node.execute(&query).await;
    first_row::<DocIdRow>(&resp, "AgentResponse").doc_id
}

pub async fn create_response(node: &EmbeddedNode, response_key: &str) -> String {
    create_response_with_status(node, response_key, "req-1", "session-1", "streaming").await
}

pub async fn upsert_conversation(
    node: &EmbeddedNode,
    session_id: &str,
    request_id: &str,
    content: &str,
    status: &str,
) {
    let session_id = escape_graphql_string(session_id);
    let request_id = escape_graphql_string(request_id);
    let content = escape_graphql_string(content);
    let status = escape_graphql_string(status);
    let now = chrono::Utc::now().to_rfc3339();
    let mutation = format!(
        r#"mutation {{
            upsert_AgentConversation(
                filter: {{ session_id: {{ _eq: "{session_id}" }} }},
                add: {{
                    session_id: "{session_id}",
                    agent_name: "{AGENT_NAME}",
                    agent_did: "{AGENT_DID}",
                    behavior_id: "{AGENT_NAME}",
                    title: "Test Conversation",
                    preview_text: "{content}",
                    status: "{status}",
                    created_at: "{now}",
                    updated_at: "{now}",
                    latest_request_id: "{request_id}"
                }},
                update: {{
                    agent_name: "{AGENT_NAME}",
                    agent_did: "{AGENT_DID}",
                    behavior_id: "{AGENT_NAME}",
                    title: "Test Conversation",
                    preview_text: "{content}",
                    status: "{status}",
                    created_at: "{now}",
                    updated_at: "{now}",
                    latest_request_id: "{request_id}"
                }}
            ) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "upsert conversation failed: {:?}",
        resp.errors
    );
}

pub async fn set_interrupt_requested_at(node: &EmbeddedNode, doc_id: &str, at: &str) {
    let doc_id = escape_graphql_string(doc_id);
    let at = escape_graphql_string(at);
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ _docID: {{ _eq: "{doc_id}" }} }},
                input: {{ interrupt_requested_at: "{at}" }}
            ) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "set_interrupt_requested_at failed: {:?}",
        resp.errors
    );
}

pub async fn set_request_lifecycle_state(node: &EmbeddedNode, doc_id: &str, lifecycle_state: &str) {
    let doc_id = escape_graphql_string(doc_id);
    let lifecycle_state = escape_graphql_string(lifecycle_state);
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ _docID: {{ _eq: "{doc_id}" }} }},
                input: {{ lifecycle_state: "{lifecycle_state}" }}
            ) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "set_request_lifecycle_state failed: {:?}",
        resp.errors
    );
}

pub async fn set_valid_until(node: &EmbeddedNode, doc_id: &str, at: &str) {
    let doc_id = escape_graphql_string(doc_id);
    let at = escape_graphql_string(at);
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ _docID: {{ _eq: "{doc_id}" }} }},
                input: {{ valid_until: "{at}" }}
            ) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "set_valid_until failed: {:?}",
        resp.errors
    );
}

pub fn build_request(
    doc_id: String,
    request_id: String,
    session_id: String,
    created_at: String,
) -> AgentRequest {
    AgentRequest {
        doc_id,
        request_id,
        agent_did: AGENT_DID.into(),
        requester_did: None,
        behavior_id: Some(AGENT_NAME.into()),
        session_id,
        content: "hello".into(),
        temperature: None,
        top_p: None,
        top_k: None,
        seed: None,
        max_tokens: None,
        metadata: None,
        execution_origin: None,
        created_at,
        deadline: None,
        subagent_depth: 0,
        caused_by_parent_request_id: None,
        caused_by_parent_tool_call_id: None,
    }
}

pub async fn create_agent_session(
    node: &EmbeddedNode,
    session_id: &str,
    behavior_id: &str,
    started: &str,
) {
    let session_id = escape_graphql_string(session_id);
    let behavior_id = escape_graphql_string(behavior_id);
    let started = escape_graphql_string(started);
    let mutation = format!(
        r#"mutation {{
            create_AgentSession(input: {{
                session_id: "{session_id}",
                agent_name: "{AGENT_NAME}",
                behavior_id: "{behavior_id}",
                started: "{started}",
                status: "active"
            }}) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "create_AgentSession failed: {:?}",
        resp.errors
    );
}

pub async fn create_agent_conversation(
    node: &EmbeddedNode,
    session_id: &str,
    behavior_id: &str,
    created_at: &str,
) {
    let session_id_escaped = escape_graphql_string(session_id);
    let behavior_id_escaped = escape_graphql_string(behavior_id);
    let created_at_escaped = escape_graphql_string(created_at);
    let mutation = format!(
        r#"mutation {{
            create_AgentConversation(input: {{
                session_id: "{session_id_escaped}",
                agent_name: "{AGENT_NAME}",
                agent_did: "{AGENT_DID}",
                behavior_id: "{behavior_id_escaped}",
                title: "test conversation",
                preview_text: "",
                status: "active",
                created_at: "{created_at_escaped}",
                updated_at: "{created_at_escaped}",
                latest_request_id: ""
            }}) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "create_AgentConversation failed: {:?}",
        resp.errors
    );
}

pub async fn create_agent_message(
    node: &EmbeddedNode,
    session_id: &str,
    sequence: u32,
    role: &str,
    content: &str,
    timestamp: &str,
) {
    let session_id_escaped = escape_graphql_string(session_id);
    let role_escaped = escape_graphql_string(role);
    let content_escaped = escape_graphql_string(content);
    let timestamp_escaped = escape_graphql_string(timestamp);
    let message_key = format!("{session_id_escaped}:{sequence}");
    let mutation = format!(
        r#"mutation {{
            create_AgentMessage(input: {{
                message_key: "{message_key}",
                session_id: "{session_id_escaped}",
                sequence: {sequence},
                role: "{role_escaped}",
                content: "{content_escaped}",
                timestamp: "{timestamp_escaped}"
            }}) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "create_AgentMessage failed: {:?}",
        resp.errors
    );
}

#[allow(clippy::too_many_arguments)]
pub async fn create_agent_tool_call(
    node: &EmbeddedNode,
    session_id: &str,
    message_sequence: u32,
    tool_call_id: &str,
    tool_name: &str,
    args: &str,
    result: &str,
    status: &str,
    started_at: &str,
    completed_at: &str,
) {
    let session_id_escaped = escape_graphql_string(session_id);
    let tool_call_id_escaped = escape_graphql_string(tool_call_id);
    let tool_name_escaped = escape_graphql_string(tool_name);
    let args_escaped = escape_graphql_string(args);
    let result_escaped = escape_graphql_string(result);
    let status_escaped = escape_graphql_string(status);
    let started_escaped = escape_graphql_string(started_at);
    let completed_escaped = escape_graphql_string(completed_at);
    let tool_call_key = format!("{session_id_escaped}:{tool_call_id_escaped}");
    let mutation = format!(
        r#"mutation {{
            create_AgentToolCall(input: {{
                tool_call_key: "{tool_call_key}",
                session_id: "{session_id_escaped}",
                message_sequence: {message_sequence},
                tool_name: "{tool_name_escaped}",
                tool_call_id: "{tool_call_id_escaped}",
                args: "{args_escaped}",
                result: "{result_escaped}",
                status: "{status_escaped}",
                started_at: "{started_escaped}",
                completed_at: "{completed_escaped}"
            }}) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "create_AgentToolCall failed: {:?}",
        resp.errors
    );
}

pub async fn create_agent_tool_result(
    node: &EmbeddedNode,
    session_id: &str,
    tool_name: &str,
    tool_input: &str,
    output_text: &str,
    created_at: &str,
) {
    let session_id_escaped = escape_graphql_string(session_id);
    let tool_name_escaped = escape_graphql_string(tool_name);
    let tool_input_escaped = escape_graphql_string(tool_input);
    let output_text_escaped = escape_graphql_string(output_text);
    let created_at_escaped = escape_graphql_string(created_at);
    let mutation = format!(
        r#"mutation {{
            create_AgentToolResult(input: {{
                agent_did: "{AGENT_DID}",
                session_id: "{session_id_escaped}",
                tool_name: "{tool_name_escaped}",
                tool_input: "{tool_input_escaped}",
                output_text: "{output_text_escaped}",
                truncated: false,
                truncation_metadata: "",
                conversation_doc_id: "",
                created_at: "{created_at_escaped}"
            }}) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "create_AgentToolResult failed: {:?}",
        resp.errors
    );
}

pub async fn create_compaction_entry(
    node: &EmbeddedNode,
    session_id: &str,
    sequence: u32,
    summary: &str,
    messages_compacted: u32,
    created_at: &str,
) {
    let session_id_escaped = escape_graphql_string(session_id);
    let summary_escaped = escape_graphql_string(summary);
    let created_at_escaped = escape_graphql_string(created_at);
    let compaction_key = format!("{session_id_escaped}:{sequence}");
    let mutation = format!(
        r#"mutation {{
            create_CompactionEntry(input: {{
                compaction_key: "{compaction_key}",
                session_id: "{session_id_escaped}",
                sequence: {sequence},
                summary: "{summary_escaped}",
                files_read: "[]",
                files_modified: "[]",
                messages_compacted: {messages_compacted},
                original_tokens: 100,
                compacted_tokens: 50,
                created_at: "{created_at_escaped}"
            }}) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "create_CompactionEntry failed: {:?}",
        resp.errors
    );
}

pub async fn create_agent_behavior(node: &EmbeddedNode, behavior_id: &str, agent_did: &str) {
    let behavior_id_escaped = escape_graphql_string(behavior_id);
    let agent_did_escaped = escape_graphql_string(agent_did);
    let mutation = format!(
        r#"mutation {{
            create_AgentBehavior(input: {{
                behavior_id: "{behavior_id_escaped}",
                agent_did: "{agent_did_escaped}",
                display_name: "test behavior",
                system_prompt: "",
                backend_id: "{BACKEND_ID}",
                model_name: "test-model",
                tool_selection_id: "",
                inference_profile_id: "",
                compaction_strategy: "StripThenSummarize",
                compaction_threshold: 0.75,
                enabled: true,
                created_at: "2026-04-21T00:00:00Z"
            }}) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "create_AgentBehavior failed: {:?}",
        resp.errors
    );
}

#[derive(Debug, Clone, Deserialize)]
pub struct DocIdRow {
    #[serde(rename = "_docID")]
    pub doc_id: String,
}

pub fn first_row<T>(resp: &QueryResponse, key: &str) -> T
where
    T: for<'de> Deserialize<'de>,
{
    assert!(!resp.has_errors(), "query failed: {:?}", resp.errors);
    let value = resp
        .data
        .as_ref()
        .and_then(|data| data.get(key))
        .and_then(|rows| rows.as_array())
        .and_then(|rows| rows.first())
        .cloned()
        .unwrap_or_else(|| panic!("missing row for {key}"));
    serde_json::from_value(value).unwrap_or_else(|err| panic!("decode {key} failed: {err}"))
}

pub fn first_optional_row<T>(resp: &QueryResponse, key: &str) -> Option<T>
where
    T: for<'de> Deserialize<'de>,
{
    assert!(!resp.has_errors(), "query failed: {:?}", resp.errors);
    resp.data
        .as_ref()
        .and_then(|data| data.get(key))
        .and_then(|rows| rows.as_array())
        .and_then(|rows| rows.first())
        .cloned()
        .map(|value| {
            serde_json::from_value(value).unwrap_or_else(|err| panic!("decode {key} failed: {err}"))
        })
}
