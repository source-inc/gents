use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use gents::defra_node::EmbeddedNode;
use gents_codex_protocol as codex;
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinHandle;

mod background;
mod bound_behavior;
mod child_stream;
mod command_projection;
mod compaction_projection;
mod compat;
mod continuation_stream;
mod handlers;
mod history_projection;
mod host_runtime;
mod progress;
mod projection_state;
mod protocol;
mod store;
mod subagent_projection;
mod thread_projection;
mod thread_routes;
mod trace;
mod turn;
mod turn_projection;

const JSONRPC_INVALID_REQUEST: i64 = -32600;
const JSONRPC_METHOD_NOT_FOUND: i64 = -32601;
const JSONRPC_INVALID_PARAMS: i64 = -32602;
const JSONRPC_INTERNAL_ERROR: i64 = -32603;

#[derive(Clone)]
struct ShimState {
    codex_home: PathBuf,
    trace_path: PathBuf,
    cwd: PathBuf,
    fs_root: Option<PathBuf>,
    node: Arc<EmbeddedNode>,
    background_execution_registry: gents::BackgroundExecutionRegistry,
    graphql: Arc<str>,
    agent_did: Arc<str>,
    behavior_id: Arc<str>,
    id_counter: Arc<AtomicU64>,
    timeout: Duration,
    poll_interval: Duration,
    sidecar: Arc<Mutex<CodexSidecar>>,
    auth_token: Option<Arc<str>>,
}

type Outbound = mpsc::UnboundedSender<String>;

/// Codex threads default to memory disabled: this shim does not wire the Codex
/// memory feature, so reporting it as enabled would be dishonest (#494).
pub(crate) const DEFAULT_MEMORY_MODE: &str = "disabled";

#[derive(Default)]
pub(crate) struct CodexSidecar {
    /// Empty threads created by this shim remain process-local until their
    /// first AgentRequest lets the runtime materialize the canonical session.
    pub(crate) created: BTreeSet<String>,
    pub(crate) cwd: BTreeMap<String, PathBuf>,
    pub(crate) loaded: BTreeSet<String>,
    pub(crate) archived: BTreeSet<String>,
    pub(crate) memory_mode: BTreeMap<String, String>,
    pub(crate) settings: BTreeMap<String, String>,
    pub(crate) names: BTreeMap<String, String>,
}

impl CodexSidecar {
    pub(crate) fn memory_mode_or_default(&self, thread_id: &str) -> String {
        self.memory_mode
            .get(thread_id)
            .cloned()
            .unwrap_or_else(|| DEFAULT_MEMORY_MODE.to_string())
    }
}

#[derive(Clone)]
struct ConnectionState {
    outbound: Outbound,
    turn_streams: Arc<Mutex<BTreeMap<String, TurnStreamControl>>>,
    fuzzy_file_search_sessions: Arc<Mutex<BTreeMap<String, Vec<String>>>>,
    pending_steering_inputs: Arc<Mutex<BTreeMap<String, Vec<codex::UserInput>>>>,
    child_thread_streams: Arc<Mutex<BTreeMap<String, ChildThreadStreamControl>>>,
    root_continuation_streams: Arc<Mutex<BTreeMap<String, RootContinuationStreamControl>>>,
}

#[derive(Clone, Debug)]
struct TurnStreamControl {
    stream_id: String,
    owner_id: Option<String>,
    cancel_tx: watch::Sender<bool>,
}

#[derive(Clone, Debug)]
struct ChildThreadStreamControl {
    watcher_id: String,
    abort_handle: tokio::task::AbortHandle,
}

#[derive(Clone, Debug)]
struct RootContinuationStreamControl {
    watcher_id: String,
    abort_handle: tokio::task::AbortHandle,
}

#[derive(Clone)]
pub(crate) struct CodexShimBindArgs {
    pub(crate) home: PathBuf,
    pub(crate) fs_root: Option<PathBuf>,
    pub(crate) node: Arc<EmbeddedNode>,
    pub(crate) background_execution_registry: gents::BackgroundExecutionRegistry,
    pub(crate) graphql: String,
    pub(crate) agent_did: String,
    pub(crate) behavior_id: Option<String>,
    pub(crate) auth_token: Option<String>,
    pub(crate) bind_addr: std::net::IpAddr,
    pub(crate) port: u16,
    pub(crate) timeout_secs: u64,
    pub(crate) poll_ms: u64,
}

pub(crate) struct BoundCodexShim {
    addr: SocketAddr,
    codex_home: PathBuf,
    trace_path: PathBuf,
    listener: TcpListener,
    app: Router,
    agent_did: String,
    behavior_id: String,
    auth_required: bool,
}

impl BoundCodexShim {
    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub(crate) fn codex_home(&self) -> &Path {
        &self.codex_home
    }

    pub(crate) fn trace_path(&self) -> &Path {
        &self.trace_path
    }

    pub(crate) fn agent_did(&self) -> &str {
        &self.agent_did
    }

    pub(crate) fn behavior_id(&self) -> &str {
        &self.behavior_id
    }

    pub(crate) fn auth_required(&self) -> bool {
        self.auth_required
    }

    pub(crate) fn spawn(self) -> JoinHandle<Result<()>> {
        tokio::spawn(self.serve())
    }

    async fn serve(self) -> Result<()> {
        axum::serve(self.listener, self.app)
            .await
            .context("serving Codex TUI shim")
    }
}

pub(crate) async fn resolve_codex_shim_behavior_id(
    node: &EmbeddedNode,
    override_behavior_id: Option<&str>,
    agent_did: &str,
) -> String {
    bound_behavior::resolve_bound_behavior_id(node, override_behavior_id, agent_did).await
}

pub(crate) enum CodexShimBindError {
    DependencyMissing(anyhow::Error),
    HostResource(anyhow::Error),
}

impl CodexShimBindError {
    pub(crate) fn error(&self) -> &anyhow::Error {
        match self {
            Self::DependencyMissing(error) | Self::HostResource(error) => error,
        }
    }

    pub(crate) fn is_dependency_missing(&self) -> bool {
        matches!(self, Self::DependencyMissing(_))
    }
}

pub(crate) async fn bind_codex_shim(
    args: CodexShimBindArgs,
) -> std::result::Result<BoundCodexShim, CodexShimBindError> {
    validate_bind_security(args.bind_addr, args.auth_token.as_deref())
        .map_err(CodexShimBindError::HostResource)?;

    let codex_home = args.home.join("codex-ui");
    let codex_log_dir = codex_home.join("log");
    fs::create_dir_all(&codex_log_dir)
        .with_context(|| format!("creating Codex UI log dir {}", codex_log_dir.display()))
        .map_err(CodexShimBindError::HostResource)?;
    let trace_path = codex_log_dir.join("codex-shim-events.jsonl");
    let agent_did = args.agent_did.clone();
    let auth_required = args.auth_token.is_some();

    let bound_behavior_id = bound_behavior::resolve_bound_behavior_id(
        args.node.as_ref(),
        args.behavior_id.as_deref(),
        &args.agent_did,
    )
    .await;
    bound_behavior::load_bound_inference_profile_id(args.node.as_ref(), &bound_behavior_id)
        .await
        .with_context(|| format!("validating Codex shim bound behavior {bound_behavior_id:?}"))
        .map_err(CodexShimBindError::DependencyMissing)?;

    let state = ShimState {
        codex_home: codex_home.clone(),
        trace_path: trace_path.clone(),
        cwd: std::env::current_dir()
            .context("resolving current working directory")
            .map_err(CodexShimBindError::HostResource)?,
        fs_root: args.fs_root,
        node: args.node,
        background_execution_registry: args.background_execution_registry,
        graphql: Arc::from(args.graphql.clone()),
        agent_did: Arc::from(args.agent_did.clone()),
        behavior_id: Arc::from(bound_behavior_id.clone()),
        id_counter: Arc::new(AtomicU64::new(1)),
        timeout: Duration::from_secs(args.timeout_secs),
        poll_interval: Duration::from_millis(args.poll_ms.max(1)),
        sidecar: Arc::new(Mutex::new(CodexSidecar::default())),
        auth_token: args.auth_token.map(Arc::from),
    };

    let app = Router::new()
        .route("/", get(ws_upgrade))
        .with_state(state.clone());
    let addr = SocketAddr::new(args.bind_addr, args.port);
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding Codex shim on {addr}"))
        .map_err(CodexShimBindError::HostResource)?;

    Ok(BoundCodexShim {
        addr,
        codex_home,
        trace_path,
        listener,
        app,
        agent_did,
        behavior_id: bound_behavior_id,
        auth_required,
    })
}

fn validate_bind_security(bind_addr: std::net::IpAddr, auth_token: Option<&str>) -> Result<()> {
    if bind_addr.is_unspecified() {
        anyhow::bail!(
            "refusing to bind app-server shim on unspecified address {bind_addr}; bind loopback or a specific interface instead"
        );
    }
    if !bind_addr.is_loopback() && auth_token.is_none() {
        anyhow::bail!(
            "refusing to bind unauthenticated app-server shim on {bind_addr}; set --codex-shim-auth-token-env or bind loopback"
        );
    }
    Ok(())
}

async fn ws_upgrade(
    ws: WebSocketUpgrade,
    State(state): State<ShimState>,
    headers: HeaderMap,
) -> Response {
    if !request_is_authorized(&headers, state.auth_token.as_deref()) {
        tracing::warn!("rejected unauthorized app-server WebSocket handshake");
        return StatusCode::UNAUTHORIZED.into_response();
    }
    ws.on_upgrade(move |socket| handle_socket(socket, state))
        .into_response()
}

fn request_is_authorized(headers: &HeaderMap, expected: Option<&str>) -> bool {
    let Some(expected) = expected else {
        return true;
    };
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|provided| bool::from(expected.as_bytes().ct_eq(provided.as_bytes())))
}

async fn handle_socket(socket: WebSocket, state: ShimState) {
    tracing::info!("Codex shim WebSocket connected");
    trace::shim_event(&state.trace_path, "websocket connected");
    let (mut sender, mut receiver) = socket.split();
    let (outbound, mut outbound_rx) = mpsc::unbounded_channel::<String>();
    let writer = tokio::spawn(async move {
        while let Some(text) = outbound_rx.recv().await {
            if sender.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });
    let connection = ConnectionState {
        outbound,
        turn_streams: Arc::new(Mutex::new(BTreeMap::new())),
        fuzzy_file_search_sessions: Arc::new(Mutex::new(BTreeMap::new())),
        pending_steering_inputs: Arc::new(Mutex::new(BTreeMap::new())),
        child_thread_streams: Arc::new(Mutex::new(BTreeMap::new())),
        root_continuation_streams: Arc::new(Mutex::new(BTreeMap::new())),
    };

    while let Some(message) = receiver.next().await {
        let Ok(message) = message else {
            break;
        };
        let Message::Text(text) = message else {
            continue;
        };

        let Ok(payload) = serde_json::from_str::<codex::JSONRPCMessage>(&text) else {
            tracing::warn!("dropping invalid Codex shim JSON-RPC message");
            continue;
        };

        let result = match payload {
            codex::JSONRPCMessage::Request(request) => {
                handlers::handle_request(&connection, &state, request).await
            }
            codex::JSONRPCMessage::Notification(notification) => {
                tracing::trace!(?notification, "Codex shim received client notification");
                Ok(())
            }
            codex::JSONRPCMessage::Response(response) => {
                tracing::trace!(?response, "Codex shim received client response");
                Ok(())
            }
            codex::JSONRPCMessage::Error(error) => {
                tracing::trace!(?error, "Codex shim received client error");
                Ok(())
            }
        };

        if let Err(err) = result {
            tracing::warn!(%err, "Codex shim request handling failed");
            break;
        }
    }

    connection.fuzzy_file_search_sessions.lock().await.clear();
    connection.pending_steering_inputs.lock().await.clear();
    connection.stop_all_child_streams().await;
    connection.stop_all_root_continuation_streams().await;
    writer.abort();
}

impl ShimState {
    fn next_thread_id(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }

    fn next_id(&self, prefix: &str) -> String {
        let id = self.id_counter.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{id}")
    }

    async fn thread_cwd(&self, thread_id: &str) -> PathBuf {
        self.sidecar
            .lock()
            .await
            .cwd
            .get(thread_id)
            .cloned()
            .unwrap_or_else(|| self.cwd.clone())
    }

    async fn thread_cwd_override(&self, thread_id: &str) -> Option<PathBuf> {
        self.sidecar.lock().await.cwd.get(thread_id).cloned()
    }

    async fn set_thread_cwd(&self, thread_id: &str, cwd: PathBuf) {
        self.sidecar
            .lock()
            .await
            .cwd
            .insert(thread_id.to_string(), cwd);
    }

    async fn is_thread_loaded(&self, thread_id: &str) -> bool {
        self.sidecar.lock().await.loaded.contains(thread_id)
    }

    async fn set_thread_loaded(&self, thread_id: &str, loaded: bool) {
        let mut guard = self.sidecar.lock().await;
        if loaded {
            guard.loaded.insert(thread_id.to_string());
        } else {
            guard.loaded.remove(thread_id);
        }
    }

    async fn loaded_thread_ids(&self) -> Vec<String> {
        let guard = self.sidecar.lock().await;
        guard
            .loaded
            .iter()
            .filter(|thread_id| !guard.archived.contains(*thread_id))
            .cloned()
            .collect()
    }

    async fn is_thread_archived(&self, thread_id: &str) -> bool {
        self.sidecar.lock().await.archived.contains(thread_id)
    }

    async fn set_thread_archived(&self, thread_id: &str, archived: bool) {
        let mut guard = self.sidecar.lock().await;
        if archived {
            guard.archived.insert(thread_id.to_string());
            guard.loaded.remove(thread_id);
        } else {
            guard.archived.remove(thread_id);
        }
    }

    async fn mark_thread_created(&self, thread_id: &str) {
        self.sidecar
            .lock()
            .await
            .created
            .insert(thread_id.to_string());
    }

    async fn is_thread_created(&self, thread_id: &str) -> bool {
        self.sidecar.lock().await.created.contains(thread_id)
    }

    async fn created_thread_ids(&self) -> Vec<String> {
        self.sidecar.lock().await.created.iter().cloned().collect()
    }

    async fn thread_name(&self, thread_id: &str) -> String {
        self.sidecar
            .lock()
            .await
            .names
            .get(thread_id)
            .cloned()
            .unwrap_or_default()
    }

    async fn set_thread_name(&self, thread_id: &str, name: &str) {
        self.sidecar
            .lock()
            .await
            .names
            .insert(thread_id.to_string(), name.to_string());
    }

    async fn thread_memory_mode(&self, thread_id: &str) -> String {
        self.sidecar.lock().await.memory_mode_or_default(thread_id)
    }

    async fn set_thread_memory_mode(&self, thread_id: &str, mode: &str) {
        self.sidecar
            .lock()
            .await
            .memory_mode
            .insert(thread_id.to_string(), mode.to_string());
    }

    async fn thread_settings(&self, thread_id: &str) -> String {
        self.sidecar
            .lock()
            .await
            .settings
            .get(thread_id)
            .cloned()
            .unwrap_or_else(|| "{}".to_string())
    }

    async fn set_thread_settings(&self, thread_id: &str, settings_json: &str) {
        self.sidecar
            .lock()
            .await
            .settings
            .insert(thread_id.to_string(), settings_json.to_string());
    }
}

impl ConnectionState {
    async fn has_turn_stream(&self, thread_id: &str, turn_id: &str) -> bool {
        self.turn_streams
            .lock()
            .await
            .contains_key(&format!("{thread_id}:{turn_id}"))
    }

    async fn replace_root_continuation_stream(
        &self,
        thread_id: String,
        watcher_id: String,
        abort_handle: tokio::task::AbortHandle,
    ) {
        let previous = self.root_continuation_streams.lock().await.insert(
            thread_id,
            RootContinuationStreamControl {
                watcher_id,
                abort_handle,
            },
        );
        if let Some(previous) = previous {
            self.clear_turn_streams_owned_by(&previous.watcher_id).await;
            previous.abort_handle.abort();
        }
    }

    async fn clear_turn_streams_owned_by(&self, owner_id: &str) {
        self.turn_streams
            .lock()
            .await
            .retain(|_, control| control.owner_id.as_deref() != Some(owner_id));
    }

    async fn clear_root_continuation_stream_if_current(&self, thread_id: &str, watcher_id: &str) {
        let mut streams = self.root_continuation_streams.lock().await;
        if streams
            .get(thread_id)
            .is_some_and(|control| control.watcher_id == watcher_id)
        {
            streams.remove(thread_id);
        }
    }

    async fn stop_root_continuation_stream(&self, thread_id: &str) {
        if let Some(control) = self
            .root_continuation_streams
            .lock()
            .await
            .remove(thread_id)
        {
            self.clear_turn_streams_owned_by(&control.watcher_id).await;
            control.abort_handle.abort();
        }
    }

    async fn stop_all_root_continuation_streams(&self) {
        let controls = std::mem::take(&mut *self.root_continuation_streams.lock().await);
        for control in controls.into_values() {
            self.clear_turn_streams_owned_by(&control.watcher_id).await;
            control.abort_handle.abort();
        }
    }

    async fn replace_child_stream(
        &self,
        thread_id: String,
        watcher_id: String,
        abort_handle: tokio::task::AbortHandle,
    ) {
        let previous = self.child_thread_streams.lock().await.insert(
            thread_id,
            ChildThreadStreamControl {
                watcher_id,
                abort_handle,
            },
        );
        if let Some(previous) = previous {
            previous.abort_handle.abort();
        }
    }

    async fn clear_child_stream_if_current(&self, thread_id: &str, watcher_id: &str) {
        let mut streams = self.child_thread_streams.lock().await;
        if streams
            .get(thread_id)
            .is_some_and(|control| control.watcher_id == watcher_id)
        {
            streams.remove(thread_id);
        }
    }

    async fn stop_child_stream(&self, thread_id: &str) {
        if let Some(control) = self.child_thread_streams.lock().await.remove(thread_id) {
            control.abort_handle.abort();
        }
    }

    async fn stop_all_child_streams(&self) {
        let controls = std::mem::take(&mut *self.child_thread_streams.lock().await);
        for control in controls.into_values() {
            control.abort_handle.abort();
        }
    }

    async fn remember_steering_input(&self, request_id: String, input: Vec<codex::UserInput>) {
        self.pending_steering_inputs
            .lock()
            .await
            .insert(request_id, input);
    }

    async fn take_steering_input(&self, request_id: &str) -> Option<Vec<codex::UserInput>> {
        self.pending_steering_inputs.lock().await.remove(request_id)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::{
        request_is_authorized, validate_bind_security, CodexSidecar, ConnectionState,
        DEFAULT_MEMORY_MODE,
    };
    use axum::http::header::AUTHORIZATION;
    use axum::http::{HeaderMap, HeaderValue};
    use tokio::sync::{mpsc, watch, Mutex};

    fn test_connection() -> ConnectionState {
        let (outbound, _outbound_rx) = mpsc::unbounded_channel::<String>();
        ConnectionState {
            outbound,
            turn_streams: Arc::new(Mutex::new(BTreeMap::new())),
            fuzzy_file_search_sessions: Arc::new(Mutex::new(BTreeMap::new())),
            pending_steering_inputs: Arc::new(Mutex::new(BTreeMap::new())),
            child_thread_streams: Arc::new(Mutex::new(BTreeMap::new())),
            root_continuation_streams: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    #[test]
    fn memory_mode_defaults_to_disabled_for_unknown_thread() {
        let sidecar = CodexSidecar::default();
        assert_eq!(sidecar.memory_mode_or_default("never-set"), "disabled");
        assert_eq!(DEFAULT_MEMORY_MODE, "disabled");
    }

    #[test]
    fn memory_mode_returns_explicit_override_when_set() {
        let mut sidecar = CodexSidecar::default();
        sidecar
            .memory_mode
            .insert("t1".to_string(), "enabled".to_string());
        assert_eq!(sidecar.memory_mode_or_default("t1"), "enabled");
        assert_eq!(sidecar.memory_mode_or_default("t2"), "disabled");
    }

    #[test]
    fn websocket_auth_requires_exact_bearer_token_when_configured() {
        let mut headers = HeaderMap::new();
        assert!(request_is_authorized(&headers, None));
        assert!(!request_is_authorized(&headers, Some("secret")));

        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer wrong"));
        assert!(!request_is_authorized(&headers, Some("secret")));

        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer secret"));
        assert!(request_is_authorized(&headers, Some("secret")));
    }

    #[test]
    fn non_loopback_bind_requires_authentication() {
        assert!(validate_bind_security("127.0.0.1".parse().unwrap(), None).is_ok());
        assert!(validate_bind_security("192.0.2.10".parse().unwrap(), None).is_err());
        assert!(validate_bind_security("192.0.2.10".parse().unwrap(), Some("secret")).is_ok());
        assert!(validate_bind_security("0.0.0.0".parse().unwrap(), Some("secret")).is_err());
    }

    #[tokio::test]
    async fn replacing_root_watcher_clears_only_its_owned_turn_generation() {
        let connection = test_connection();
        let old_task = tokio::spawn(std::future::pending::<()>());
        connection
            .replace_root_continuation_stream(
                "thread-1".to_string(),
                "watcher-old".to_string(),
                old_task.abort_handle(),
            )
            .await;

        let (interactive_tx, _) = watch::channel(false);
        let interactive = super::turn::install_stream_control(
            &connection,
            "thread-1".to_string(),
            "interactive".to_string(),
            None,
            interactive_tx,
        )
        .await;
        let (old_tx, _) = watch::channel(false);
        let old = super::turn::install_stream_control(
            &connection,
            "thread-1".to_string(),
            "wake-1".to_string(),
            Some("watcher-old"),
            old_tx,
        )
        .await;
        let (new_tx, _) = watch::channel(false);
        let new = super::turn::install_stream_control(
            &connection,
            "thread-1".to_string(),
            "wake-1".to_string(),
            Some("watcher-new"),
            new_tx,
        )
        .await;

        let new_task = tokio::spawn(std::future::pending::<()>());
        connection
            .replace_root_continuation_stream(
                "thread-1".to_string(),
                "watcher-new".to_string(),
                new_task.abort_handle(),
            )
            .await;
        drop(old);

        assert!(connection.has_turn_stream("thread-1", "interactive").await);
        assert!(connection.has_turn_stream("thread-1", "wake-1").await);

        connection.stop_root_continuation_stream("thread-1").await;
        assert!(connection.has_turn_stream("thread-1", "interactive").await);
        assert!(!connection.has_turn_stream("thread-1", "wake-1").await);

        interactive.clear().await;
        new.clear().await;
    }
}
