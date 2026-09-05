//! Grok TUI shim assembly.
//!
//! Gents is the leader server; stock Grok is its pager client. This module
//! assembles the shim the same way the Codex shim is assembled, from the
//! in-process [`EmbeddedNode`] plus the *bound* behavior/model/context
//! documents:
//!
//! 1. [`protocol`] owns the length-prefixed wire codec and the
//!    register/registered/ping/pong/disconnect/ACP envelope types;
//! 2. [`server`] owns the leader server: the exclusive leader lock, the
//!    register → registered handshake, readiness gating, ping/pong, and ACP
//!    payload forwarding to the delegate;
//! 3. [`acp`] owns the ACP service: initialize capabilities, session/new with
//!    a preferred id, persisted session list/load, model/catalog/mode updates,
//!    and the shaped method-not-found stubs (`x.ai/interject`,
//!    `x.ai/compact_conversation`);
//! 4. [`turn`] owns connection-scoped pending prompts: JSON-RPC ids,
//!    submission via [`crate::create_agent_request`], deferred responses until
//!    terminalization, and interruption via [`gents::interrupt_request`];
//! 5. [`projection`] owns the bounded, request-id-scoped read-only projection
//!    of durable rows into fresh Grok `session/update` notification payloads.
//!
//! Every projection query runs in-process (`node.execute(&query).await`) with
//! every interpolated value escaped by
//! [`gents::graphql::escape_graphql_string`]; no HTTP GraphQL helper and no
//! stock Grok import is used anywhere in the shim. All diagnostics go through
//! `tracing` — never `println!`/`eprintln!`.

use std::sync::Arc;

use anyhow::{Context, Result};
use defra_node::EmbeddedNode;

pub(crate) mod acp;
mod goals;
pub(crate) mod projection;
pub(crate) mod protocol;
pub(crate) mod server;
mod sessions;
mod task_control;
pub(crate) mod turn;
mod usage;

use crate::commands::grok_shim::projection::resolve_bound_model_context;
use crate::commands::grok_shim::server::{
    spawn_leader, AcpDelegate, LeaderHandle, LeaderServerConfig, Registration,
};

/// Everything the shim needs to bind, in one place.
///
/// Model and context-window configuration is *bound*: it is resolved once from
/// the bound behavior's `AgentBehavior`/`InferenceProfile` documents before
/// the leader accepts a client, so the pager's model catalog and every
/// `_meta.totalTokens` bound come from real configuration rather than a
/// synthetic catalog entry.
#[derive(Clone)]
pub(crate) struct GrokShimBindArgs {
    pub(crate) background_executions: gents::hook::BackgroundExecutionRegistry,
    /// In-process node every request, interrupt, and projection query uses.
    pub(crate) node: Arc<EmbeddedNode>,
    /// GraphQL endpoint string accepted by `create_agent_request`; the
    /// in-process embedded node is authoritative for reads.
    pub(crate) graphql: String,
    /// Bound behavior id; `None` resolves the agent principal's default.
    pub(crate) behavior_id: Option<String>,
    /// Agent DID requests are submitted for.
    pub(crate) agent_did: String,
    /// Display name stamped on `AgentSession.agent_name`.
    pub(crate) agent_name: String,
    /// Unix socket path the leader binds and the pager connects to.
    pub(crate) socket_path: std::path::PathBuf,
}

/// Bind and spawn the Grok shim leader.
///
/// Resolution order mirrors the Codex shim's bound-behavior resolution: an
/// explicit `--grok-shim-behavior-id` override wins, then the agent
/// principal's configured `default_behavior_id`, then the synthesized
/// `<did>:default` fallback. The behavior must exist and select a model and
/// backend before the socket is published, so a misconfigured home fails fast
/// instead of serving a fabricated model catalog.
///
/// The returned [`LeaderHandle`] owns shutdown and the listener task; the
/// caller (the `gents server` launch path) holds it for the lifetime of the
/// serving loop, so dropping it at exit stops the listener and releases the
/// exclusive leader lock.
pub(crate) async fn bind_grok_shim(args: GrokShimBindArgs) -> Result<LeaderHandle> {
    let node = args.node.clone();
    let behavior_id =
        resolve_grok_shim_behavior_id(node.as_ref(), args.behavior_id.as_deref(), &args.agent_did)
            .await;
    let bound = resolve_bound_model_context(node.as_ref(), &behavior_id)
        .await
        .with_context(|| {
            format!(
                "binding the Grok shim to behavior {behavior_id:?}; fix the behavior with \
                 `gents config behavior set --behavior-id {behavior_id} ...`"
            )
        })?;
    tracing::info!(
        behavior_id = %behavior_id,
        model_id = %bound.model_id,
        total_context_tokens = bound.total_context_tokens,
        socket = %args.socket_path.display(),
        "grok shim leader binding"
    );
    // Immutable per-connection construction inputs: the bound model/context
    // configuration is resolved once above, and everything else the factory
    // clones (identity strings, the GraphQL endpoint, the embedded node) is
    // shared configuration. All mutable ACP state — the AcpService, its
    // TurnManager, its ProjectionEngine, the session registry, and the
    // per-session event counters — is constructed fresh inside the factory,
    // once per registered connection.
    let factory_inputs = AcpDelegateFactoryInputs {
        background_executions: args.background_executions.clone(),
        node: args.node.clone(),
        graphql: args.graphql.clone(),
        agent_did: args.agent_did.clone(),
        agent_name: args.agent_name.clone(),
        behavior_id: behavior_id.clone(),
        bound: bound.clone(),
    };
    let leader = spawn_leader(
        LeaderServerConfig::new(args.socket_path.clone()),
        Arc::new(production_acp_delegate_factory(factory_inputs)),
    )
    .with_context(|| {
        format!(
            "spawning the Grok shim leader on socket {}",
            args.socket_path.display()
        )
    })?;
    tracing::info!(
        socket = %args.socket_path.display(),
        "grok shim leader is accepting pager connections"
    );
    Ok(leader)
}

/// Immutable inputs a per-connection delegate factory clones from.
///
/// Every field is resolved once at bind time (bound behavior/model/context,
/// identity, the GraphQL endpoint, and the embedded node). The factory never
/// clones mutable ACP state across connections: each [`Registration`] gets a
/// fresh `AcpService`, `TurnManager`, and `ProjectionEngine`.
#[derive(Clone)]
struct AcpDelegateFactoryInputs {
    background_executions: gents::hook::BackgroundExecutionRegistry,
    node: Arc<EmbeddedNode>,
    graphql: String,
    agent_did: String,
    agent_name: String,
    behavior_id: String,
    bound: crate::commands::grok_shim::projection::BoundModelContext,
}

/// Build the production per-connection delegate factory.
///
/// The factory receives the registered client's identity — its assigned
/// `client_id` plus the registration mode and advertised capabilities — and
/// stores them explicitly in the freshly constructed connection service
/// before handing the delegate to the leader. Constructing a delegate
/// therefore constructs the whole connection-scoped ACP world: the
/// `TurnManager` (pending prompt/cancel lifecycle), the `ProjectionEngine`
/// (durable row projection), the session registry, and the per-session event
/// counters. Nothing mutable is shared between two registered connections.
fn production_acp_delegate_factory(
    inputs: AcpDelegateFactoryInputs,
) -> impl Fn(u64, &Registration) -> Result<Arc<dyn AcpDelegate>> {
    move |client_id, registration: &Registration| {
        let turns = Arc::new(crate::commands::grok_shim::turn::TurnManager::new(
            inputs.node.clone(),
            crate::commands::grok_shim::turn::TurnManagerConfig {
                agent_did: inputs.agent_did.clone(),
                behavior_id: inputs.behavior_id.clone(),
                graphql: inputs.graphql.clone(),
            },
        ));
        let projections = Arc::new(
            crate::commands::grok_shim::projection::ProjectionEngine::new(
                inputs.node.clone(),
                inputs.bound.clone(),
            )
            .with_background_executions(inputs.background_executions.clone()),
        );
        let mut service = crate::commands::grok_shim::acp::AcpService::new(
            crate::commands::grok_shim::acp::AcpServiceConfig {
                node: inputs.node.clone(),
                agent_did: Arc::from(inputs.agent_did.as_str()),
                agent_name: Arc::from(inputs.agent_name.as_str()),
                behavior_id: Arc::from(inputs.behavior_id.as_str()),
                current_model: crate::commands::grok_shim::acp::BoundModel {
                    model_id: inputs.bound.model_id.clone(),
                    name: inputs.bound.model_name.clone(),
                    total_context_tokens: inputs.bound.total_context_tokens,
                },
            },
            turns,
            projections,
        );
        service.register_client_identity(client_id, registration);
        Ok(Arc::new(service) as Arc<dyn AcpDelegate>)
    }
}

/// Resolve the behavior the Grok shim binds to.
///
/// An explicit override always wins. Otherwise the agent principal's
/// configured `default_behavior_id` is used — that is the id behaviors are
/// actually stored under — and only a missing or unset principal falls back to
/// the synthesized `<did>:default` form, keeping legacy homes compatible.
pub(crate) async fn resolve_grok_shim_behavior_id(
    node: &EmbeddedNode,
    override_behavior_id: Option<&str>,
    agent_did: &str,
) -> String {
    if let Some(value) = explicit_behavior_override(override_behavior_id) {
        return value;
    }
    match gents::load_agent_principal(node, agent_did).await {
        Ok(Some(principal)) => principal
            .default_behavior_id
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(|| gents::default_behavior_id_for_agent(agent_did)),
        _ => gents::default_behavior_id_for_agent(agent_did),
    }
}

/// The trimmed, non-empty form of an explicit behavior override, if any.
///
/// Exposed `pub(crate)` so the CLI surface and tests can share the exact
/// trimming rule the async resolver applies.
pub(crate) fn explicit_behavior_override(override_behavior_id: Option<&str>) -> Option<String> {
    override_behavior_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::grok_shim::server::AcpDelegateFactory;

    /// A leader-side registration: `yolo_mode=true`, `auto_mode=false`,
    /// `terminal=false` — the exact capabilities the edge probe registers.
    fn production_registration() -> crate::commands::grok_shim::server::Registration {
        crate::commands::grok_shim::server::Registration {
            client_type: "grok-pager".to_string(),
            mode: crate::commands::grok_shim::protocol::RegisterMode::Stdio,
            capabilities: crate::commands::grok_shim::protocol::ClientCapabilities {
                yolo_mode: true,
                ..Default::default()
            },
        }
    }

    /// Spawn a mock GraphQL endpoint forwarding every request to the
    /// embedded node, so the factory's `create_agent_request` seam writes
    /// real durable rows without a running Gents server.
    async fn spawn_mock_graphql(node: Arc<EmbeddedNode>) -> String {
        async fn handler(
            axum::extract::State(node): axum::extract::State<Arc<EmbeddedNode>>,
            axum::Json(body): axum::Json<serde_json::Value>,
        ) -> axum::Json<serde_json::Value> {
            let query = body
                .get("query")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string();
            let response = node.execute(&query).await;
            axum::Json(serde_json::to_value(&response).unwrap_or_default())
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock graphql listener");
        let addr = listener
            .local_addr()
            .expect("mock graphql listener address");
        let router = axum::Router::new()
            .route("/", axum::routing::post(handler))
            .with_state(node);
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        format!("http://{addr}/")
    }

    /// Shared fixture inputs for the production-factory tests: an embedded
    /// node with runtime schemas ensured, plus the factory inputs a bound
    /// shim would clone. The GraphQL endpoint is a live mock forwarding to
    /// the embedded node, so `session/prompt` submissions write real
    /// durable `AgentRequest` rows.
    ///
    /// The staging `TempDir` is returned *first*: the ordinary binding
    /// `let (_staging, node, inputs) = factory_fixture().await;` then drops
    /// the delegates (and their shared node) before the `TempDir` deletes
    /// the node's storage directory, because tuple fields drop in
    /// declaration order.
    async fn factory_fixture() -> (
        tempfile::TempDir,
        Arc<EmbeddedNode>,
        AcpDelegateFactoryInputs,
    ) {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let identity = gents::KeyIdentity::load_or_create(tempdir.path().join("agent.key"), None)
            .expect("test signing identity");
        let agent_did = gents::AgentIdentity::did(&identity).to_string();
        let behavior_id = gents::default_behavior_id_for_agent(&agent_did);
        let node = Arc::new(
            EmbeddedNode::builder()
                .data_path(tempdir.path().join("node"))
                .with_storage_backend(gents::defra_node::StorageBackend::Regolith)
                .build()
                .await
                .expect("embedded node"),
        );
        gents::schema::ensure_runtime_schemas(node.as_ref())
            .await
            .expect("runtime schemas");
        let response = node
            .execute(&format!(
                r#"mutation {{
                    create_AgentPrincipal(input: {{
                        agent_did: "{agent_did}"
                        display_name: "Grok shim factory test"
                        default_behavior_id: "{behavior_id}"
                        enabled: true
                    }}) {{ _docID }}
                    create_AgentBehavior(input: {{
                        behavior_id: "{behavior_id}"
                        agent_did: "{agent_did}"
                        display_name: "Grok shim factory test"
                        enabled: true
                    }}) {{ _docID }}
                }}"#,
            ))
            .await;
        gents::graphql::ensure_no_errors(&response, "seed admitted factory behavior")
            .expect("seed admitted factory behavior");
        let graphql = spawn_mock_graphql(node.clone()).await;
        let inputs = AcpDelegateFactoryInputs {
            background_executions: Default::default(),
            node: node.clone(),
            graphql,
            agent_did,
            agent_name: "grok-shim".to_string(),
            behavior_id,
            bound: crate::commands::grok_shim::projection::BoundModelContext::new(
                "GLM-5.3-NVFP4".to_string(),
                "GLM 5.3 NVFP4".to_string(),
                262_144,
            ),
        };
        (tempdir, node, inputs)
    }

    /// Invoke the exact production factory with the given registration, then
    /// drive one JSON-RPC ACP request through the returned delegate over a
    /// real [`AcpOutbound`], returning every outbound line in wire order.
    ///
    /// The request never injects capabilities into the ACP service by hand:
    /// the factory applies the registration itself, exactly as the leader
    /// does for a registered connection.
    async fn delegate_request(
        factory: &dyn crate::commands::grok_shim::server::AcpDelegateFactory,
        client_id: u64,
        registration: &crate::commands::grok_shim::server::Registration,
        request: &serde_json::Value,
    ) -> Vec<serde_json::Value> {
        let delegate = factory
            .create_delegate(client_id, registration)
            .await
            .expect("the production factory must construct the delegate");
        let (tx, mut lines) = tokio::sync::mpsc::unbounded_channel();
        let outbound = crate::commands::grok_shim::server::AcpOutbound::for_frames(tx);
        let payload = serde_json::to_string(request).expect("request payload");
        delegate
            .handle_acp(&payload, outbound)
            .await
            .expect("the delegate must dispatch the request");
        drain_outbound(&mut lines).await
    }

    /// Drain every queued outbound ACP line into parsed JSON values.
    async fn drain_outbound(
        lines: &mut tokio::sync::mpsc::UnboundedReceiver<
            crate::commands::grok_shim::protocol::ServerEnvelope,
        >,
    ) -> Vec<serde_json::Value> {
        let mut drained = Vec::new();
        while let Ok(envelope) = lines.try_recv() {
            match envelope {
                crate::commands::grok_shim::protocol::ServerEnvelope::Acp { payload } => {
                    drained
                        .push(serde_json::from_str(&payload).expect("outbound ACP line is JSON"));
                }
                other => panic!("unexpected non-ACP outbound envelope: {other:?}"),
            }
        }
        drained
    }

    /// Separate the single response from notifications, preserving their order.
    fn split_response(
        lines: Vec<serde_json::Value>,
    ) -> (Vec<serde_json::Value>, serde_json::Value) {
        let (mut responses, notifications): (Vec<_>, Vec<_>) =
            lines.into_iter().partition(|line| line.get("id").is_some());
        assert_eq!(responses.len(), 1, "expected exactly one request response");
        (notifications, responses.pop().unwrap())
    }

    /// The `session/new` request shape the edge probe sends, with the three
    /// mode keys deliberately absent from `_meta`: the injection must come
    /// from the registration, never from the request.
    fn session_new_request(session_id: &str, model: &str) -> serde_json::Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "session/new",
            "params": {
                "cwd": "/tmp",
                "mcpServers": [],
                "_meta": {
                    "sessionId": session_id,
                    "modelId": model,
                },
            },
        })
    }

    /// A `session/prompt` request payload with an explicit wire id, so the
    /// isolation regression can correlate responses per delegate.
    fn session_prompt_request_with_id(
        request_id: u64,
        session_id: &str,
        prompt_id: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "session/prompt",
            "params": {
                "sessionId": session_id,
                "prompt": [{ "type": "text", "text": "stay pending" }],
                "_meta": { "promptId": prompt_id, "screenMode": "inline" },
            },
        })
    }

    /// Ask the embedded node for every `AgentRequest` row's id, lifecycle
    /// state, and interrupt marker.
    async fn agent_request_rows(node: &EmbeddedNode) -> Vec<(String, String, Option<String>)> {
        let query = r#"{ AgentRequest { request_id lifecycle_state interrupt_requested_at } }"#;
        let response = node.execute(query).await;
        assert!(
            !response.has_errors(),
            "AgentRequest query failed: {:?}",
            response.errors
        );
        response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|row| {
                (
                    row.get("request_id")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    row.get("lifecycle_state")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    row.get("interrupt_requested_at")
                        .and_then(serde_json::Value::as_str)
                        .map(ToOwned::to_owned),
                )
            })
            .collect()
    }

    /// The exact production factory must derive `session/new`'s mode
    /// capabilities from the *registered* capabilities — and the wire-facing
    /// model ids from the bound behavior's model name, never from the
    /// backend id — when the request itself carries none of the mode keys.
    ///
    /// The request is driven through the returned `Arc<dyn AcpDelegate>`
    /// over a real [`AcpOutbound`], with no manual capability injection
    /// into the `AcpService`: the factory applies the registration itself,
    /// exactly as the leader does for a registered connection.
    #[tokio::test]
    async fn the_production_factory_derives_session_new_from_registration() {
        let (_staging, _node, inputs) = factory_fixture().await;
        let factory = Arc::new(production_acp_delegate_factory(inputs.clone()));
        let registration = production_registration();

        let lines = delegate_request(
            factory.as_ref(),
            1,
            &registration,
            &session_new_request("grok-registered-session", "GLM-5.3-NVFP4"),
        )
        .await;
        let (notifications, response) = split_response(lines);

        assert_eq!(
            notifications,
            vec![serde_json::json!({
                "jsonrpc": "2.0",
                "method": "_x.ai/mcp_initialized",
                "params": {"sessionId": "grok-registered-session", "mcpToolCount": 0, "elapsedMs": 0}
            })]
        );
        assert!(
            response.get("error").is_none(),
            "session/new must succeed, got: {response}"
        );
        let result = &response["result"];
        assert_eq!(result["sessionId"], "grok-registered-session");

        // The request carried NO yoloMode/autoMode/clientTerminal keys in
        // its `_meta` — every one of the three must be derived from the
        // registered capabilities.
        assert_eq!(
            result["_meta"]["yoloMode"],
            serde_json::json!(true),
            "yoloMode must derive from the registered capability"
        );
        assert_eq!(
            result["_meta"]["autoMode"],
            serde_json::json!(false),
            "autoMode must derive from the registered capability"
        );
        assert_eq!(
            result["_meta"]["clientTerminal"],
            serde_json::json!(false),
            "clientTerminal must derive from the registered capability"
        );

        // The wire-facing model id is the bound behavior's `model_name`
        // exactly; the backend id never leaks into any of the three
        // model-id reads.
        let behavior_model_name = "GLM-5.3-NVFP4";
        assert_eq!(
            result["models"]["currentModelId"],
            serde_json::json!(behavior_model_name),
            "models.currentModelId must be the behavior model name"
        );
        let available = result["models"]["availableModels"]
            .as_array()
            .expect("availableModels must be an array");
        assert_eq!(available.len(), 1, "the bound catalog serves one model");
        assert_eq!(
            available[0]["modelId"],
            serde_json::json!(behavior_model_name),
            "the catalog modelId must be the behavior model name"
        );
        assert_eq!(
            result["_meta"]["modelId"],
            serde_json::json!(behavior_model_name),
            "_meta.modelId must be the behavior model name"
        );
        for value in [
            &result["models"]["currentModelId"],
            &available[0]["modelId"],
            &result["_meta"]["modelId"],
        ] {
            let rendered = serde_json::to_string(value).expect("model id serializes");
            assert!(
                !rendered.contains("grok-shim-backend"),
                "no model id may contain the backend id: {rendered}"
            );
        }
    }

    /// The production-factory pending-turn isolation regression.
    ///
    /// Two delegates from the exact production factory hold prompts on
    /// sessions A and B against one embedded node; disconnecting A must
    /// interrupt only A's durable request, while B's stays pending. A
    /// second disconnect on B then interrupts B's request too — every
    /// pending request is eventually interrupted, none is leaked, and no
    /// delegate ever observes another connection's pending turn.
    #[tokio::test]
    async fn the_production_factory_disconnects_isolate_pending_turns() {
        let (_staging, node, inputs) = factory_fixture().await;
        let factory = Arc::new(production_acp_delegate_factory(inputs.clone()));
        let registration = production_registration();

        // Two registered connections, each with its own outbound channel.
        let (tx_a, rx_a) = tokio::sync::mpsc::unbounded_channel();
        let (tx_b, rx_b) = tokio::sync::mpsc::unbounded_channel();
        let first = factory
            .create_delegate(1, &registration)
            .await
            .expect("delegate A must construct");
        let second = factory
            .create_delegate(2, &registration)
            .await
            .expect("delegate B must construct");

        // Each connection creates its own session, then prompts it. The
        // prompts never terminalize (no runtime daemon serves these rows),
        // so both durable requests stay pending.
        let outbound_a = crate::commands::grok_shim::server::AcpOutbound::for_frames(tx_a.clone());
        let payload_a = serde_json::to_string(&session_new_request(
            "grok-isolation-session-a",
            "GLM-5.3-NVFP4",
        ))
        .expect("session/new payload A");
        first
            .handle_acp(&payload_a, outbound_a)
            .await
            .expect("delegate A must dispatch session/new");
        let outbound_b = crate::commands::grok_shim::server::AcpOutbound::for_frames(tx_b.clone());
        let payload_b = serde_json::to_string(&session_new_request(
            "grok-isolation-session-b",
            "GLM-5.3-NVFP4",
        ))
        .expect("session/new payload B");
        second
            .handle_acp(&payload_b, outbound_b)
            .await
            .expect("delegate B must dispatch session/new");

        let mut rx_a = rx_a;
        let mut rx_b = rx_b;
        let _ = drain_outbound(&mut rx_a).await;
        let _ = drain_outbound(&mut rx_b).await;

        // Fire both prompts. Each blocks until its request terminalizes or
        // the entry is drained, so each runs as its own task; the fixture
        // continues once both durable rows exist and are still pending.
        let outbound_a = crate::commands::grok_shim::server::AcpOutbound::for_frames(tx_a.clone());
        let first_for_prompt = first.clone();
        let prompt_a = tokio::spawn(async move {
            let payload = serde_json::to_string(&session_prompt_request_with_id(
                1,
                "grok-isolation-session-a",
                "prompt-a",
            ))
            .expect("prompt payload A");
            first_for_prompt
                .handle_acp(&payload, outbound_a)
                .await
                .expect("delegate A must dispatch session/prompt");
        });
        let outbound_b = crate::commands::grok_shim::server::AcpOutbound::for_frames(tx_b.clone());
        let second_for_prompt = second.clone();
        let prompt_b = tokio::spawn(async move {
            let payload = serde_json::to_string(&session_prompt_request_with_id(
                2,
                "grok-isolation-session-b",
                "prompt-b",
            ))
            .expect("prompt payload B");
            second_for_prompt
                .handle_acp(&payload, outbound_b)
                .await
                .expect("delegate B must dispatch session/prompt");
        });

        // Wait until both durable request rows exist, then confirm both are
        // still pending and neither is interrupted.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let rows = agent_request_rows(node.as_ref()).await;
            if rows.len() == 2
                && rows.iter().all(|(_, state, _)| state == "pending")
                && rows.iter().all(|(_, _, interrupted)| interrupted.is_none())
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "both durable requests must exist and stay pending"
            );
            tokio::task::yield_now().await;
        }

        // Disconnect A only. Its pending entry is drained and its submitted
        // request interrupted; B's must remain pending and uninterruptable
        // by A's teardown. (Both rows stay lifecycle-state "pending" — no
        // daemon serves them — so the isolation signal is exactly one
        // interrupt marker with the other row still marker-free.)
        first.on_disconnect().await;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let rows = agent_request_rows(node.as_ref()).await;
            let interrupted = rows
                .iter()
                .filter(|(_, _, interrupted)| interrupted.is_some())
                .count();
            let still_pending = rows
                .iter()
                .filter(|(_, state, interrupted)| state == "pending" && interrupted.is_none())
                .count();
            if interrupted == 1 && still_pending == 1 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "disconnecting A must interrupt exactly A's request while B stays pending"
            );
            tokio::task::yield_now().await;
        }

        // B's prompt is still live: its turn future has not resolved, and
        // draining B's outbound sees the user echo but no prompt response.
        let b_lines = drain_outbound(&mut rx_b).await;
        assert!(
            b_lines
                .iter()
                .all(|line| line.get("id") != Some(&serde_json::json!(2))),
            "B's prompt response must not have resolved while B is connected: {b_lines:?}"
        );

        // Disconnect B. Its pending entry is drained and its submitted
        // request interrupted too; every pending request has now been
        // interrupted and none was leaked.
        second.on_disconnect().await;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let rows = agent_request_rows(node.as_ref()).await;
            let interrupted = rows
                .iter()
                .filter(|(_, _, interrupted)| interrupted.is_some())
                .count();
            if interrupted == 2 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "disconnecting B must interrupt B's request as well"
            );
            tokio::task::yield_now().await;
        }

        // Both turn futures resolved cancelled through their drains.
        let (a_result, b_result) = tokio::join!(prompt_a, prompt_b);
        a_result.expect("delegate A's prompt task must finish cleanly");
        b_result.expect("delegate B's prompt task must finish cleanly");
    }

    #[test]
    fn explicit_behavior_overrides_win_and_are_trimmed() {
        assert_eq!(
            explicit_behavior_override(Some("  custom-behavior  ")).as_deref(),
            Some("custom-behavior")
        );
        assert_eq!(
            explicit_behavior_override(Some("behavior-a")).as_deref(),
            Some("behavior-a")
        );
    }

    #[test]
    fn blank_behavior_overrides_are_treated_as_absent() {
        assert_eq!(explicit_behavior_override(Some("   ")), None);
        assert_eq!(explicit_behavior_override(Some("")), None);
        assert_eq!(explicit_behavior_override(None), None);
    }

    #[test]
    fn the_default_behavior_fallback_is_the_agent_scoped_form() {
        assert_eq!(
            gents::default_behavior_id_for_agent("did:test:agent"),
            "did:test:agent:default"
        );
    }

    /// The production factory must construct a fully fresh connection world
    /// per invocation: a distinct `AcpService` with its own `TurnManager` and
    /// `ProjectionEngine`, sharing only the immutable configuration and the
    /// embedded node. This exercises the exact closure `bind_grok_shim` hands
    /// to `spawn_leader`, without binding a socket or running the daemon.
    #[tokio::test]
    async fn the_production_factory_constructs_distinct_connection_services() {
        let (_staging, _node, inputs) = factory_fixture().await;
        let factory = Arc::new(production_acp_delegate_factory(inputs.clone()));
        let registration = production_registration();

        // Invoke the factory twice, exactly as two registered connections
        // would.
        let first = factory
            .create_delegate(1, &registration)
            .await
            .expect("the first delegate should construct");
        let second = factory
            .create_delegate(2, &registration)
            .await
            .expect("the second delegate should construct");

        assert!(
            !Arc::ptr_eq(&first, &second),
            "two invocations must construct distinct delegates"
        );
        assert_ne!(
            Arc::as_ptr(&first) as *const u8 as usize,
            Arc::as_ptr(&second) as *const u8 as usize,
            "the delegates must not alias the same connection service"
        );

        // The delegates must own distinct manager/service state, not share
        // one: each `handle_acp` dispatch on the second must never observe
        // state mutated through the first, and each drain on disconnect must
        // only touch its own pending turns.
        let first_service = first.as_ref() as *const dyn AcpDelegate as *const () as usize;
        let second_service = second.as_ref() as *const dyn AcpDelegate as *const () as usize;
        assert_ne!(
            first_service, second_service,
            "each delegate must own its own AcpService instance"
        );

        // Disconnecting one delegate must leave the other fully functional:
        // distinct TurnManager ownership means draining one leaves the other
        // untouched.
        first.on_disconnect().await;
        let outbound_payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {},
        })
        .to_string();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let outbound = crate::commands::grok_shim::server::AcpOutbound::for_frames(tx);
        second
            .handle_acp(&outbound_payload, outbound)
            .await
            .expect("the second delegate must still dispatch after the first disconnected");
        let _ = rx.recv().await;
    }

    #[tokio::test]
    async fn bind_args_carry_socket_behavior_and_identity() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let node = Arc::new(
            EmbeddedNode::builder()
                .data_path(tempdir.path().join("node"))
                .with_storage_backend(gents::defra_node::StorageBackend::Regolith)
                .build()
                .await
                .expect("embedded node"),
        );
        let args = GrokShimBindArgs {
            background_executions: Default::default(),
            node,
            graphql: "http://127.0.0.1:8000/api/v0/graphql".to_string(),
            behavior_id: Some("behavior-a".to_string()),
            agent_did: "did:test:agent".to_string(),
            agent_name: "grok-shim".to_string(),
            socket_path: std::path::PathBuf::from("/tmp/gents-grok.sock"),
        };
        assert_eq!(args.behavior_id.as_deref(), Some("behavior-a"));
        assert_eq!(args.agent_did, "did:test:agent");
        assert_eq!(
            args.socket_path,
            std::path::PathBuf::from("/tmp/gents-grok.sock")
        );
        let cloned = args.clone();
        assert_eq!(cloned.agent_did, args.agent_did);
        assert_eq!(cloned.socket_path, args.socket_path);
        assert!(
            Arc::ptr_eq(&cloned.node, &args.node),
            "clone must share the same embedded node"
        );
    }
}
