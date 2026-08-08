// Soft-cap justified: shared test harness for runtime integration tests. Splitting further would fragment fixture reuse (mock HTTP server, bind helpers, wait utilities must stay co-located with the tests that pair them).
use super::super::*;
use crate::identity::KeyIdentity;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::thread::JoinHandle;

pub(super) use crate::document_config::ToolSelectionDocument;
pub(super) use crate::ensure_runtime_schemas;
pub(super) use crate::graphql::escape_graphql_string;
pub(super) use crate::identity::AgentIdentity;
pub(super) use crate::runtime_status::RuntimeStatusHandle;
pub(super) use crate::tool_surface::ToolCeiling;
pub(super) use crate::watcher::AgentRequest;
pub(super) use serde_json::Value;

pub(super) async fn test_node() -> Arc<defra_node::EmbeddedNode> {
    let node_identity = test_identity("agent-runtime-node");
    Arc::new(
        defra_node::EmbeddedNode::builder()
            .with_node_identity_did(node_identity.did())
            .build()
            .await
            .unwrap(),
    )
}

pub(super) async fn test_node_with_identity(
    identity: &dyn AgentIdentity,
) -> Arc<defra_node::EmbeddedNode> {
    Arc::new(
        defra_node::EmbeddedNode::builder()
            .with_node_identity_did(identity.did())
            .build()
            .await
            .unwrap(),
    )
}

pub(super) fn test_identity(name: &str) -> KeyIdentity {
    let path = std::env::temp_dir().join(format!("{name}-{}.key", uuid::Uuid::new_v4()));
    KeyIdentity::load_or_create(path, None).unwrap()
}

pub(super) fn request(behavior_id: Option<&str>, session_id: &str) -> AgentRequest {
    AgentRequest {
        doc_id: "doc-1".to_string(),
        request_id: "req-1".to_string(),
        agent_did: "did:test:test".to_string(),
        requester_did: None,
        behavior_id: behavior_id.map(ToOwned::to_owned),
        session_id: session_id.to_string(),
        content: "hello".to_string(),
        temperature: None,
        top_p: None,
        top_k: None,
        max_tokens: None,
        metadata: None,
        execution_origin: None,
        created_at: "2026-04-09T00:00:00Z".to_string(),
        deadline: None,
        subagent_depth: 0,
        caused_by_parent_request_id: None,
        caused_by_parent_tool_call_id: None,
    }
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct RuntimeStatusRow {
    pub(super) process_state: String,
    pub(super) reconcile_phase: String,
    pub(super) active_generation: i64,
    pub(super) runnable_behavior_count: i64,
    pub(super) unavailable_behavior_count: i64,
    pub(super) last_reconcile_result: String,
    pub(super) last_reconcile_error: String,
}

pub(super) async fn fetch_runtime_status(
    node: &defra_node::EmbeddedNode,
    agent_did: &str,
) -> RuntimeStatusRow {
    let escaped_agent_did = escape_graphql_string(agent_did);
    let query = format!(
        r#"{{
            AgentRuntime(filter: {{ agent_did: {{ _eq: "{escaped_agent_did}" }} }}, limit: 1) {{
                process_state
                reconcile_phase
                active_generation
                runnable_behavior_count
                unavailable_behavior_count
                last_reconcile_result
                last_reconcile_error
            }}
        }}"#
    );
    // Signed exact-config loading adds several asynchronous verification reads
    // before the control watcher can publish its first phase. These paused-time
    // tests must wait for scheduler progress without sleeping: a timer would
    // advance the logical clock and could accidentally fire the debounce under
    // test. Bound the polling loop so a genuinely missing status row still
    // fails promptly.
    for _ in 0..512 {
        let response = node.execute(&query).await;
        assert!(
            !response.has_errors(),
            "AgentRuntime query failed: {:?}",
            response.errors
        );
        if let Some(value) = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRuntime"))
            .and_then(|rows| rows.as_array())
            .and_then(|rows| rows.first())
            .cloned()
        {
            return serde_json::from_value(value).expect("decode AgentRuntime row");
        }
        tokio::task::yield_now().await;
    }
    panic!("AgentRuntime row did not become visible after bounded scheduler progress")
}

pub(super) async fn wait_for_runtime_reconcile_result(
    node: &defra_node::EmbeddedNode,
    agent_did: &str,
    expected: &str,
) -> RuntimeStatusRow {
    for _ in 0..512 {
        let status = fetch_runtime_status(node, agent_did).await;
        if status.last_reconcile_result == expected {
            return status;
        }
        tokio::task::yield_now().await;
    }
    panic!("AgentRuntime did not publish reconcile result {expected:?}")
}

pub(super) async fn wait_for_runtime_process_state(
    node: &defra_node::EmbeddedNode,
    agent_did: &str,
    expected_process_state: &str,
) {
    let escaped_agent_did = escape_graphql_string(agent_did);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let query = format!(
            r#"{{
                AgentRuntime(filter: {{ agent_did: {{ _eq: "{escaped_agent_did}" }} }}, limit: 1) {{
                    process_state
                }}
            }}"#
        );
        let response = node.execute(&query).await;
        assert!(
            !response.has_errors(),
            "AgentRuntime query failed: {:?}",
            response.errors
        );
        let process_state = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRuntime"))
            .and_then(Value::as_array)
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("process_state"))
            .and_then(Value::as_str);
        if process_state == Some(expected_process_state) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for AgentRuntime {} to reach process_state={}; last={:?}",
            agent_did,
            expected_process_state,
            process_state
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub(super) struct MockModelEndpoint {
    pub(super) endpoint: String,
    pub(super) port: u16,
    pub(super) stop: Arc<AtomicBool>,
    pub(super) handle: Option<JoinHandle<()>>,
}

impl MockModelEndpoint {
    pub(super) fn start(model_name: &str) -> anyhow::Result<Self> {
        Self::start_with_blocking_chat(model_name, false)
    }

    pub(super) fn start_blocking_chat(model_name: &str) -> anyhow::Result<Self> {
        Self::start_with_blocking_chat(model_name, true)
    }

    fn start_with_blocking_chat(model_name: &str, blocking_chat: bool) -> anyhow::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        listener.set_nonblocking(true)?;
        let port = listener.local_addr()?.port();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_thread = stop.clone();
        let model_name = model_name.to_string();
        let handle = thread::spawn(move || {
            while !stop_for_thread.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let request = match read_http_request(&mut stream) {
                            Ok(request) => request,
                            Err(_) => {
                                let _ = stream.shutdown(Shutdown::Both);
                                continue;
                            }
                        };
                        if request.method == "POST" && request.path == "/v1/chat/completions" {
                            if blocking_chat {
                                while !stop_for_thread.load(Ordering::Relaxed) {
                                    thread::sleep(Duration::from_millis(25));
                                }
                                let _ = stream.shutdown(Shutdown::Both);
                                continue;
                            }
                        }

                        let (status, body) = if request.method == "GET"
                            && (request.path == "/v1/models" || request.path == "/models")
                        {
                            ("200 OK", format!(r#"{{"data":[{{"id":"{model_name}"}}]}}"#))
                        } else {
                            ("404 Not Found", r#"{"error":"not found"}"#.to_string())
                        };
                        let _ = write_http_response(&mut stream, status, "application/json", &body);
                        let _ = stream.shutdown(Shutdown::Both);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(_) => break,
                }
            }
        });

        Ok(Self {
            endpoint: format!("http://127.0.0.1:{port}/v1"),
            port,
            stop,
            handle: Some(handle),
        })
    }

    pub(super) fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl Drop for MockModelEndpoint {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(("127.0.0.1", self.port));
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

pub(super) struct HttpRequestData {
    pub(super) method: String,
    pub(super) path: String,
}

pub(super) fn read_http_request(stream: &mut TcpStream) -> anyhow::Result<HttpRequestData> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    let mut buffer = Vec::new();
    let mut temp = [0u8; 1024];
    let header_end = loop {
        let read = stream.read(&mut temp)?;
        if read == 0 {
            anyhow::bail!("connection closed before headers");
        }
        buffer.extend_from_slice(&temp[..read]);
        if let Some(index) = find_subslice(&buffer, b"\r\n\r\n") {
            break index + 4;
        }
    };
    let header_text = String::from_utf8_lossy(&buffer[..header_end]);
    let mut lines = header_text.split("\r\n").filter(|line| !line.is_empty());
    let request_line = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing request line"))?;
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing request method"))?
        .to_string();
    let path = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing request path"))?
        .to_string();
    Ok(HttpRequestData { method, path })
}

pub(super) fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

pub(super) fn write_http_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
) -> anyhow::Result<()> {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    Ok(())
}

#[derive(Default)]
pub(super) struct RecordingObserver {
    pub(super) states: std::sync::Mutex<Vec<crate::agent::ProcessLifecycleState>>,
}

impl crate::agent::ProcessLifecycleObserver for RecordingObserver {
    fn on_process_state_change(&self, state: crate::agent::ProcessLifecycleState) {
        self.states
            .lock()
            .expect("recording observer mutex poisoned")
            .push(state);
    }
}

pub(super) async fn bind_default_behavior_backend(
    node: &defra_node::EmbeddedNode,
    agent_did: &str,
    backend_id: &str,
    endpoint: &str,
) {
    bind_default_behavior_backend_with_capacity_and_probe_status(
        node, agent_did, backend_id, endpoint, 1, "healthy",
    )
    .await;
}

pub(super) async fn bind_default_behavior_backend_with_capacity(
    node: &defra_node::EmbeddedNode,
    agent_did: &str,
    backend_id: &str,
    endpoint: &str,
    max_concurrent: i64,
) {
    bind_default_behavior_backend_with_capacity_and_probe_status(
        node,
        agent_did,
        backend_id,
        endpoint,
        max_concurrent,
        "healthy",
    )
    .await;
}

pub(super) async fn bind_default_behavior_backend_with_capacity_and_probe_status(
    node: &defra_node::EmbeddedNode,
    agent_did: &str,
    backend_id: &str,
    endpoint: &str,
    max_concurrent: i64,
    probe_status: &str,
) {
    let bootstrap = crate::ensure_agent_principal(node, agent_did)
        .await
        .unwrap();
    let escaped_backend_id = escape_graphql_string(backend_id);
    let escaped_endpoint = escape_graphql_string(endpoint);
    let escaped_probe_status = escape_graphql_string(probe_status);
    let mutation = format!(
        r#"mutation {{
            upsert_InferenceBackend(
                filter: {{ backend_id: {{ _eq: "{escaped_backend_id}" }} }},
                add: {{
                    backend_id: "{escaped_backend_id}",
                    name: "{escaped_backend_id}",
                    endpoint: "{escaped_endpoint}",
                    max_concurrent: {max_concurrent},
                    enabled: true,
                    models: ["default"],
                    probe_status: "{escaped_probe_status}"
                }},
                update: {{
                    name: "{escaped_backend_id}",
                    endpoint: "{escaped_endpoint}",
                    max_concurrent: {max_concurrent},
                    enabled: true,
                    probe_status: "{escaped_probe_status}"
                }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "upsert InferenceBackend failed: {:?}",
        response.errors
    );

    let mut default_behavior =
        crate::load_agent_behavior(node, &bootstrap.default_behavior.behavior_id)
            .await
            .unwrap()
            .expect("default behavior document");
    default_behavior.backend_id = Some(backend_id.to_string());
    crate::upsert_agent_behavior(node, &default_behavior)
        .await
        .unwrap();
}

pub(super) async fn create_agent_request(
    node: &defra_node::EmbeddedNode,
    agent_did: &str,
    request_id: &str,
    session_id: &str,
    content: &str,
) -> String {
    create_agent_request_for_behavior(node, agent_did, None, request_id, session_id, content).await
}

pub(super) async fn create_agent_request_for_behavior(
    node: &defra_node::EmbeddedNode,
    agent_did: &str,
    behavior_id: Option<&str>,
    request_id: &str,
    session_id: &str,
    content: &str,
) -> String {
    let escaped_request_id = escape_graphql_string(request_id);
    let escaped_agent_did = escape_graphql_string(agent_did);
    let escaped_source_author_did = escape_graphql_string(agent_did);
    let escaped_behavior_id = escape_graphql_string(behavior_id.unwrap_or_default());
    let escaped_session_id = escape_graphql_string(session_id);
    let escaped_content = escape_graphql_string(content);
    let created_at = chrono::Utc::now().to_rfc3339();
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{escaped_request_id}",
                agent_did: "{escaped_agent_did}",
                source_author_did: "{escaped_source_author_did}",
                behavior_id: "{escaped_behavior_id}",
                session_id: "{escaped_session_id}",
                retry_parent_request: "",
                retry_root_request: "{escaped_request_id}",
                superseded_by_request: "",
                content: "{escaped_content}",
                status: "pending",
                lifecycle_state: "pending",
                backend_id: "",
                execution_origin: "interactive",
                created_at: "{created_at}",
                retry_count: 0,
                max_retries: {max_retries}
            }}) {{ _docID }}
        }}"#,
        max_retries = crate::lifecycle::DEFAULT_REQUEST_MAX_RETRIES,
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create_AgentRequest failed: {:?}",
        response.errors
    );
    let query = format!(
        r#"{{
            AgentRequest(filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }}, limit: 1) {{
                _docID
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "AgentRequest lookup failed: {:?}",
        response.errors
    );
    response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("_docID"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .expect("AgentRequest _docID")
}

pub(super) async fn wait_for_inference_call_state(
    node: &defra_node::EmbeddedNode,
    request_id: &str,
    expected_state: &str,
) {
    let escaped_request_id = escape_graphql_string(request_id);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let query = format!(
            r#"{{
                InferenceCall(filter: {{
                    request_id: {{ _eq: "{escaped_request_id}" }},
                    call_kind: {{ _eq: "inference" }}
                }}, limit: 1) {{
                    call_state
                    failure_reason
                }}
            }}"#
        );
        let response = node.execute(&query).await;
        assert!(
            !response.has_errors(),
            "InferenceCall query failed: {:?}",
            response.errors
        );
        let row = response
            .data
            .as_ref()
            .and_then(|data| data.get("InferenceCall"))
            .and_then(Value::as_array)
            .and_then(|rows| rows.first())
            .cloned();
        let state = row
            .as_ref()
            .and_then(|row| row.get("call_state"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        if state == expected_state {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for inference-kind InferenceCall request_id={} to reach call_state={}, last row={:?}",
            request_id,
            expected_state,
            row
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub(super) async fn update_backend_probe_status(
    node: &defra_node::EmbeddedNode,
    backend_id: &str,
    probe_status: &str,
) {
    let escaped_backend_id = escape_graphql_string(backend_id);
    let escaped_probe_status = escape_graphql_string(probe_status);
    let mutation = format!(
        r#"mutation {{
            update_InferenceBackend(
                filter: {{ backend_id: {{ _eq: "{escaped_backend_id}" }} }},
                input: {{ probe_status: "{escaped_probe_status}" }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "update InferenceBackend probe_status failed: {:?}",
        response.errors
    );
}

pub(super) struct ScriptedWatcher {
    pub(super) rx: mpsc::Receiver<anyhow::Result<AgentRequest>>,
}

impl Watcher for ScriptedWatcher {
    async fn next_request(&mut self) -> Option<anyhow::Result<AgentRequest>> {
        self.rx.recv().await
    }
}
