//! Grok shim ACP service: initialize, session lifecycle, and stubbed edges.
//!
//! This module owns the JSON-RPC 2.0 surface the stock Grok pager drives once
//! it has registered with the Gents leader socket ([`super::server`]) and its
//! ACP payloads are forwarded here. It implements:
//!
//! - `initialize` — capabilities include persisted session loading.
//! - `session/load` / `x.ai/session/list` — authorized read-only history,
//!   replay through the existing projector, and live observation handoff.
//! - `authenticate` — the single `gents.runtime` auth method always succeeds;
//!   client credentials are the transport's concern, not a Gents document.
//! - `session/new` — honors the preferred `_meta.sessionId`, creates exactly
//!   one `AgentSession` document for the returned id (create-only on the
//!   `@immutable` `agent_did`/`requester_did` fields, matching the runtime's
//!   `request_session_projection`), and returns the audited nested result
//!   shape `{"sessionId", "models": {"availableModels", "currentModelId"},
//!   "_meta"}`. Model, context window, and behavior identity all come from
//!   the bound configuration (`AgentBehavior` + `InferenceProfile`), never
//!   from a per-session override the runtime does not model.
//! - `session/set_model` — validates against the bound catalog and emits the
//!   `x.ai/models/update` ext notification. Gents has no per-session model
//!   override: only the currently bound model is accepted, and unsupported
//!   model/effort selections fail rather than pretending to change inference.
//! - `session/set_mode` — records the pager's mode and emits a
//!   `current_mode_update` session notification. Mode is a client capability
//!   concern, not an `AgentSession` field.
//! - `session/prompt` / `session/cancel` — dispatched to the sibling
//!   [`super::turn::TurnManager`], which owns the connection-scoped pending
//!   prompt, deferred response, and interrupt lifecycle.
//! - `x.ai/subagent/get` / `x.ai/subagent/list_running` /
//!   `x.ai/subagent/cancel` — inspect/control only runtime descendants of
//!   sessions registered on this connection, using the runtime's canonical
//!   authorization and the stock shell's extension DTOs.
//!   Any other `x.ai/subagent/*` method is
//!   unrouted and answers the typed method-not-found (`-32601`) like every
//!   other unknown method, never the sibling leaf's generic error.
//!
//! Shaped stubs return JSON-RPC method-not-found (`-32601`) with an explicit
//! owned-transition explanation, never a fabricated success:
//! - `x.ai/interject` — the owned completion loop has no formally specified
//!   injection transition; writing a detached `AgentMessage` would not affect
//!   provider input.
//! - `x.ai/compact_conversation` — `CompactionEntry` is runtime-owned and has
//!   no `tokens_before`/`tokens_after` fields; `AgentSession` has no usage
//!   counters.
//!
//! Anything not routed above — including the client-side `terminal/*`
//! methods, whose shaped not-supported stubs are owned by
//! [`super::projection::tools`] — falls back to the same explicit
//! method-not-found error so an unported control edge stays visible on the
//! wire instead of being silently swallowed.
//!
//! All GraphQL values pass through `gents::graphql::escape_graphql_string`
//! and every query runs in-process on the embedded node via
//! `EmbeddedNode::execute`; no HTTP GraphQL helper is used.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result};
use futures_util::future::BoxFuture;
use gents::defra_node::EmbeddedNode;
use gents::graphql::{ensure_no_errors, escape_graphql_string};
use serde_json::{json, Value};

use super::projection::subagents::{
    SUBAGENT_CANCEL_METHOD, SUBAGENT_GET_METHOD, SUBAGENT_LIST_RUNNING_METHOD,
};
use super::projection::{
    effective_context_window_tokens, stamp_update_meta, AsyncCommit, ProjectionEngine,
    UpdateTimestamps, SESSION_UPDATE_METHOD,
};
use super::protocol::{ClientCapabilities, RegisterMode};
use super::server::{AcpDelegate, AcpOutbound};
use super::turn::{
    parse_cancel_notification, parse_prompt_request, PromptSender, PromptSenderLine, TurnManager,
};

/// JSON-RPC 2.0 error code for a malformed request envelope.
pub(crate) const JSONRPC_INVALID_REQUEST: i64 = -32600;

/// JSON-RPC 2.0 error code for an unknown/unhandled method. Every shaped stub
/// in this module answers with exactly this code so the pager classifies the
/// edge as unsupported rather than failed.
pub(crate) const JSONRPC_METHOD_NOT_FOUND: i64 = -32601;

/// JSON-RPC 2.0 error code for structurally invalid request params.
pub(crate) const JSONRPC_INVALID_PARAMS: i64 = -32602;

/// JSON-RPC 2.0 error code for an internal shim failure.
pub(crate) const JSONRPC_INTERNAL_ERROR: i64 = -32603;

/// The single auth method the shim advertises: Gents runtime identity is the
/// only supported credential surface.
pub(crate) const GENTS_AUTH_METHOD_ID: &str = "gents.runtime";

/// ACP protocol version the shim speaks.
pub(crate) const ACP_PROTOCOL_VERSION: i64 = 1;

/// Wire name of the `initialize` request method.
pub(crate) const INITIALIZE_METHOD: &str = "initialize";

/// Wire name of the `authenticate` request method.
pub(crate) const AUTHENTICATE_METHOD: &str = "authenticate";

/// Wire name of the `session/new` request method.
pub(crate) const SESSION_NEW_METHOD: &str = "session/new";

/// Wire name of the persisted `session/load` request method.
pub(crate) const SESSION_LOAD_METHOD: &str = "session/load";

/// Wire name of the `session/set_model` request method.
pub(crate) const SESSION_SET_MODEL_METHOD: &str = "session/set_model";

/// Wire name of the `session/set_mode` request method.
pub(crate) const SESSION_SET_MODE_METHOD: &str = "session/set_mode";

/// Wire name of the `session/prompt` request method (owned by `turn.rs`).
pub(crate) const SESSION_PROMPT_METHOD: &str = "session/prompt";

/// Wire name of the `session/cancel` notification (owned by `turn.rs`).
pub(crate) const SESSION_CANCEL_METHOD: &str = "session/cancel";

/// Wire name of the `x.ai/interject` ext request (shaped stub).
pub(crate) const INTERJECT_METHOD: &str = "x.ai/interject";

/// Wire name of the `x.ai/compact_conversation` ext request (shaped stub).
pub(crate) const COMPACT_CONVERSATION_METHOD: &str = "x.ai/compact_conversation";

/// Ext notification method emitted after a model catalog switch.
pub(crate) const MODELS_UPDATE_METHOD: &str = "x.ai/models/update";

/// Ext notification method that clears the pager's new-session MCP spinner.
pub(crate) const MCP_INITIALIZED_METHOD: &str = "x.ai/mcp_initialized";

// ---------------------------------------------------------------------------
// Bound configuration
// ---------------------------------------------------------------------------

/// Bound model catalog entry derived from the serving configuration.
///
/// `model_id` is the wire-facing model identifier: exactly the bound
/// behavior's `model_name`, resolved once at bind time (see
/// `resolve_bound_model_context`). The backend id remains internal routing
/// configuration — it is validated during binding but never projected into
/// the wire-facing `modelId`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BoundModel {
    /// Wire-facing model identifier (`modelId` on the wire): the bound
    /// behavior's `model_name` exactly.
    pub(crate) model_id: String,
    /// Human-readable display name (`name` on the wire).
    pub(crate) name: String,
    /// Context window from the bound `InferenceProfile`, advertised as
    /// `meta.totalContextTokens`.
    pub(crate) total_context_tokens: u64,
}

impl BoundModel {
    pub(crate) fn effective_context_window(&self) -> u64 {
        effective_context_window_tokens(self.total_context_tokens)
    }

    /// Serialize this entry into the pager's `availableModels` item shape.
    ///
    /// The capability keys are the audited truth, not an aspiration: the
    /// shim's prompt parser is text-only (images are not natively plumbed
    /// into `SubmitRequestOptions`), and a selected reasoning effort never
    /// leaves connection-local state to reach provider inference. Until the
    /// runtime plumbs either natively, advertising image input or reasoning
    /// effort would be a false capability, so the catalog stays text-only
    /// with `supportsReasoningEffort: false` and no advertised effort list.
    fn catalog_entry(&self) -> Value {
        json!({
            "modelId": self.model_id,
            "name": self.name,
            "meta": {
                "totalContextTokens": self.effective_context_window(),
                "acceptsImages": false,
                "inputModalities": ["text"],
                "supportsReasoningEffort": false,
            },
        })
    }
}

impl BoundModel {
    /// Build the `models` object of a `session/new` result.
    ///
    /// The audited wire shape nests `availableModels` and `currentModelId`
    /// under a `models` key — see `recon-input/audited-ledger.json`
    /// (`session:new-load`) and the live probe's
    /// `session["models"]["currentModelId"]` read. Splicing the catalog keys
    /// into the top-level result object breaks the pager, so the nesting is
    /// asserted by the tests in this file.
    pub(crate) fn models_object(&self) -> Value {
        json!({
            "availableModels": [self.catalog_entry()],
            "currentModelId": self.model_id,
        })
    }
}

/// Immutable bound configuration for the ACP service.
///
/// Every field is resolved once at bind time from the bound behavior and
/// inference profile (see `grok_shim.rs` assembly); nothing here is
/// per-session runtime state.
#[derive(Clone)]
pub(crate) struct AcpServiceConfig {
    /// In-process embedded node used for every GraphQL query/mutation.
    pub(crate) node: Arc<EmbeddedNode>,
    /// Serving agent DID (the `@immutable` `AgentSession.agent_did` value).
    pub(crate) agent_did: Arc<str>,
    /// Serving agent display name (stamped on `AgentSession.agent_name`).
    pub(crate) agent_name: Arc<str>,
    /// Bound behavior id (stamped on `AgentSession.behavior_id`).
    pub(crate) behavior_id: Arc<str>,
    /// Bound model the runtime serves for this behavior.
    pub(crate) current_model: BoundModel,
}

impl AcpServiceConfig {
    /// Build the `models` object of a `session/new` result for the currently
    /// bound model. Delegates to [`BoundModel::models_object`], which owns the
    /// audited nested shape and its tests.
    pub(crate) fn models_object(&self) -> Value {
        self.current_model.models_object()
    }
}

// ---------------------------------------------------------------------------
// Connection-local session state
// ---------------------------------------------------------------------------

/// Per-session shim state.
///
/// Gents documents record session identity (`AgentSession`) and request
/// history (`AgentRequest`/`AgentResponse`); they have no cwd, model, or mode
/// fields. Everything the pager needs that the runtime does not model is
/// connection-local state here and is never persisted.
#[derive(Debug, Clone)]
struct AcpSessionState {
    /// Current pager mode id from `session/set_mode`.
    mode_id: String,
    /// Delivery handoff only: discovery begins from this pre-replay time
    /// after the load response has been successfully enqueued.
    resume_from: Option<String>,
}

impl AcpSessionState {
    fn new() -> Self {
        Self {
            mode_id: "default".to_string(),
            resume_from: None,
        }
    }
}

/// Connection-local reservation only. No mutex is held across replay I/O;
/// cancellation releases the reservation synchronously through Drop.
struct SessionLoadReservation<'a> {
    loading: &'a std::sync::Mutex<BTreeSet<String>>,
    session_id: String,
}

impl<'a> SessionLoadReservation<'a> {
    fn reserve(loading: &'a std::sync::Mutex<BTreeSet<String>>, session_id: &str) -> Result<Self> {
        if !loading
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(session_id.to_owned())
        {
            return Err(invalid_params(
                "session is already loading on this connection",
            ));
        }
        Ok(Self {
            loading,
            session_id: session_id.to_owned(),
        })
    }
}

impl Drop for SessionLoadReservation<'_> {
    fn drop(&mut self) {
        self.loading
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.session_id);
    }
}

/// The `session/set_mode` state commit: record the new mode in the session
/// registry exactly when the `current_mode_update` notification was
/// successfully enqueued, inside the common send path's per-session critical
/// section.
///
/// This is the atomicity hinge for `session/set_mode`: because the commit
/// runs while the session's send lock is still held, a failed send leaves
/// the mode state untouched (the hook never runs), and two concurrent
/// `set_mode` calls serialize coherently with their updates — the mode
/// state always reflects the last *delivered* update, in delivery order.
/// The hook only ever records connection-local state, so it is infallible.
struct ModeStateCommit<'a> {
    sessions: &'a tokio::sync::Mutex<BTreeMap<String, AcpSessionState>>,
    session_id: String,
    mode_id: String,
}

impl AsyncCommit for ModeStateCommit<'_> {
    async fn commit(&self) {
        let mut sessions = self.sessions.lock().await;
        if let Some(state) = sessions.get_mut(&self.session_id) {
            state.mode_id = self.mode_id.clone();
        }
    }
}

// ---------------------------------------------------------------------------
// Dispatch envelope
// ---------------------------------------------------------------------------

/// One parsed ACP JSON-RPC frame, dispatch-level only.
///
/// The wire envelope and frame codec are owned by [`super::protocol`]; this
/// is the dispatch-side view of a single decoded ACP payload line.
#[derive(Debug, Clone)]
pub(crate) struct AcpRequest {
    /// JSON-RPC request id. `None` marks a notification.
    pub(crate) id: Option<Value>,
    /// Requested method name.
    pub(crate) method: String,
    /// Request params (`{}` when absent).
    pub(crate) params: Value,
}

impl AcpRequest {
    /// Parse one ACP payload line. A non-object or id-less-when-claimed frame
    /// is rejected so the caller can answer `-32600`.
    pub(crate) fn from_payload(payload: &str) -> Result<Self> {
        let value: Value = serde_json::from_str(payload)
            .with_context(|| "decoding Grok shim ACP payload".to_string())?;
        let object = value
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("ACP payload is not a JSON object"))?;
        let method = object
            .get("method")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow::anyhow!("ACP payload has no method"))?
            .to_string();
        let method = internal_method(&method).to_owned();
        // JSON-RPC 2.0: a request carries an id; a notification does not. A
        // JSON `null` id is treated as absent, matching the pager's decoder.
        let id = match object.get("id") {
            Some(Value::Null) | None => None,
            Some(id) => Some(id.clone()),
        };
        let params = object.get("params").cloned().unwrap_or_else(|| json!({}));
        Ok(Self { id, method, params })
    }

    fn is_notification(&self) -> bool {
        self.id.is_none()
    }
}

/// The wire output of one dispatched frame.
///
/// Most notifications are emitted before the response so a pager that drains
/// until it sees its response id observes the same order the reference agent
/// produces (for example `x.ai/models/update` before the `session/set_model`
/// response). MCP initialization completion follows session creation's response,
/// because the native pager needs that response to attach its session routing.
#[derive(Debug, Clone, Default)]
pub(crate) struct AcpDispatch {
    /// Serialized JSON-RPC notification lines, ordered by the live delegate.
    pub(crate) notifications: Vec<String>,
    /// Serialized JSON-RPC response line, or `None` for notifications.
    pub(crate) response: Option<String>,
}

/// Outcome of a request handler: the result value plus notifications.
struct RequestOutcome {
    notifications: Vec<Value>,
    result: Value,
}

/// Shape a successful JSON-RPC response envelope.
fn response_line(id: &Value, result: &Value) -> String {
    serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    }))
    .expect("JSON-RPC response envelope is serializable")
}

/// Shape a JSON-RPC error envelope.
fn error_line(id: &Value, code: i64, message: &str) -> String {
    serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message },
    }))
    .expect("JSON-RPC error envelope is serializable")
}

/// Shape a JSON-RPC notification line.
/// ACP extensions carry a leading underscore on the JSON-RPC wire. The SDK
/// strips it before dispatching the extension to Grok's `x.ai/...` handlers.
pub(super) fn wire_method(method: &str) -> std::borrow::Cow<'_, str> {
    if method.starts_with("x.ai/") {
        std::borrow::Cow::Owned(format!("_{method}"))
    } else {
        std::borrow::Cow::Borrowed(method)
    }
}

fn internal_method(method: &str) -> &str {
    if method.starts_with("_x.ai/") {
        &method[1..]
    } else {
        method
    }
}

fn notification_line(method: &str, params: &Value) -> String {
    serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "method": wire_method(method),
        "params": params,
    }))
    .expect("JSON-RPC notification envelope is serializable")
}

// ---------------------------------------------------------------------------
// ACP service
// ---------------------------------------------------------------------------

/// The ACP service implementing the leader server's delegate.
pub(crate) struct AcpService {
    config: AcpServiceConfig,
    /// Connection-scoped prompt/cancel lifecycle (sibling slice `turn.rs`).
    turns: Arc<TurnManager>,
    /// Request-id-scoped durable projections (sibling slice `projection.rs`).
    projections: Arc<ProjectionEngine>,
    /// Connection-local session registry.
    sessions: tokio::sync::Mutex<BTreeMap<String, AcpSessionState>>,
    loading_sessions: std::sync::Mutex<BTreeSet<String>>,
    /// The connection's assigned leader client id, set when the factory binds
    /// the registering client's identity to this service.
    client_id: Option<u64>,
    /// The registering client's registration mode (stdio pager or headless
    /// client), stored when this per-connection service is constructed by the
    /// delegate factory.
    registered_mode: Option<RegisterMode>,
    /// The registering client's advertised capabilities, stored when this
    /// per-connection service is constructed by the delegate factory.
    registered_capabilities: Option<ClientCapabilities>,
}

impl AcpService {
    /// Assemble the service from bound configuration and the sibling
    /// engines. The assembly slice (`grok_shim.rs`) constructs the
    /// [`TurnManager`] and [`ProjectionEngine`] from the same embedded node
    /// and bound behavior/model/context configuration.
    pub(super) fn new(
        config: AcpServiceConfig,
        turns: Arc<TurnManager>,
        projections: Arc<ProjectionEngine>,
    ) -> Self {
        Self {
            config,
            turns,
            projections,
            sessions: tokio::sync::Mutex::new(BTreeMap::new()),
            loading_sessions: std::sync::Mutex::new(BTreeSet::new()),
            client_id: None,
            registered_mode: None,
            registered_capabilities: None,
        }
    }

    /// Store the registering client's identity and capabilities.
    ///
    /// The leader constructs this service per registered connection through
    /// the delegate factory, then hands it the connection's assigned client id
    /// plus the registration mode and advertised capabilities. The ACP service
    /// derives the `yoloMode` / `autoMode` / `clientTerminal` injection for
    /// `session/new` from the stored capabilities.
    pub(super) fn register_client_identity(
        &mut self,
        client_id: u64,
        registration: &crate::commands::grok_shim::server::Registration,
    ) {
        self.client_id = Some(client_id);
        self.registered_mode = Some(registration.mode);
        self.registered_capabilities = Some(registration.capabilities.clone());
    }

    /// Dispatch one decoded ACP payload line, sending every turn
    /// notification live through `sender` as it is produced.
    ///
    /// This is the dispatch core shared by tests (which use a `Buffer`
    /// sender) and the production delegate: the delegate builds a `Live`
    /// sender over the connection's [`AcpOutbound`], so the user echo and
    /// every novel durable projection update reach the pager immediately,
    /// and only the deferred `session/prompt` response is written by the
    /// delegate after the turn resolves. A buffered sender instead collects
    /// the same lines for the dispatch result — there is no second code
    /// path that skips the projection semantics.
    pub(super) async fn dispatch_with_sender(
        &self,
        payload: &str,
        prompt_sender: &PromptSender,
    ) -> AcpDispatch {
        let request = match AcpRequest::from_payload(payload) {
            Ok(request) => request,
            Err(error) => {
                tracing::warn!(%error, "grok shim rejected an undecodable ACP payload");
                return AcpDispatch {
                    notifications: Vec::new(),
                    response: Some(error_line(
                        &Value::Null,
                        JSONRPC_INVALID_REQUEST,
                        &format!("invalid ACP payload: {error}"),
                    )),
                };
            }
        };

        if request.is_notification() {
            tracing::debug!(method = %request.method, "grok shim ACP notification");
            return self.dispatch_notification(request).await;
        }
        tracing::debug!(method = %request.method, "grok shim ACP request");
        self.dispatch_request(request, prompt_sender).await
    }

    /// Dispatch a notification. Notifications have no response body; an
    /// unknown method is logged and dropped rather than answered.
    async fn dispatch_notification(&self, request: AcpRequest) -> AcpDispatch {
        match request.method.as_str() {
            SESSION_CANCEL_METHOD => {
                let cancel = match parse_cancel_notification(&request.params) {
                    Ok(cancel) => cancel,
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            method = %request.method,
                            "grok shim ignored a malformed session/cancel notification"
                        );
                        return AcpDispatch::default();
                    }
                };
                if let Err(error) = self.turns.handle_cancel(cancel).await {
                    tracing::warn!(
                        %error,
                        method = %request.method,
                        "grok shim failed to dispatch ACP notification"
                    );
                }
            }
            other => {
                tracing::warn!(%other, "grok shim ignored an unknown ACP notification");
            }
        }
        AcpDispatch::default()
    }

    /// Dispatch a request, mapping every handler failure to a shaped error.
    ///
    /// Session-update notifications were already enqueued live through the
    /// common send path while the handler ran (the per-session send lock
    /// makes allocation order equal enqueue order). A buffered sender
    /// collects those same lines; they are drained here — in enqueue order,
    /// before the response — so the headless dispatch result observes
    /// exactly what a live client would have received. A `RequestOutcome`
    /// notification value that never rode the session-update path (the
    /// `x.ai/models/update` catalog refresh) is still emitted before the
    /// response.
    async fn dispatch_request(
        &self,
        request: AcpRequest,
        prompt_sender: &PromptSender,
    ) -> AcpDispatch {
        let id = request.id.clone().unwrap_or_else(|| Value::Null);
        let method = request.method.clone();
        match self.handle_request(&request, prompt_sender).await {
            Ok(outcome) => {
                let mut notifications = outcome
                    .notifications
                    .iter()
                    .map(|params| {
                        // Each notification value carries its own method.
                        let notification_method = params
                            .get("__method")
                            .and_then(Value::as_str)
                            .unwrap_or(SESSION_UPDATE_METHOD);
                        let mut params = params.clone();
                        if let Some(object) = params.as_object_mut() {
                            object.remove("__method");
                        }
                        notification_line(notification_method, &params)
                    })
                    .collect::<Vec<_>>();
                notifications.extend(prompt_sender.take_lines().await);
                AcpDispatch {
                    notifications,
                    response: Some(response_line(&id, &outcome.result)),
                }
            }
            Err(error) => {
                let code = error_code_for(&error);
                tracing::warn!(%error, %method, code, "grok shim ACP request failed");
                // Lines already enqueued before the failure stay observable:
                // a live client would have received them.
                AcpDispatch {
                    notifications: prompt_sender.take_lines().await,
                    response: Some(error_line(&id, code, &error.to_string())),
                }
            }
        }
    }

    /// Route one request to its handler.
    async fn handle_request(
        &self,
        request: &AcpRequest,
        prompt_sender: &PromptSender,
    ) -> Result<RequestOutcome> {
        match request.method.as_str() {
            INITIALIZE_METHOD => self.handle_initialize(request).await,
            AUTHENTICATE_METHOD => self.handle_authenticate(request).await,
            "x.ai/session/info" => {
                let session = required_session_id(&request.params)?;
                let result = super::usage::session_info(
                    &self.config.node,
                    &self.config.agent_did,
                    &self.config.behavior_id,
                    &session,
                    &self.config.current_model.model_id,
                    &self.config.current_model.name,
                    self.config.current_model.effective_context_window(),
                )
                .await?;
                Ok(RequestOutcome {
                    notifications: Vec::new(),
                    result,
                })
            }
            // Gents has no x.ai subscription or cloud credit ledger. The
            // native nullable config communicates absence without invented
            // balances, subscription tiers, or an unsupported-method banner.
            "x.ai/billing" => Ok(RequestOutcome {
                notifications: Vec::new(),
                result: json!({"result":{"config":null,"on_demand_enabled":false}}),
            }),
            "x.ai/session/usage" => {
                let session = required_session_id(&request.params)?;
                let result = super::usage::session_usage(
                    &self.config.node,
                    &self.config.agent_did,
                    &self.config.behavior_id,
                    &session,
                )
                .await?;
                Ok(RequestOutcome {
                    notifications: Vec::new(),
                    result,
                })
            }
            "x.ai/sessions/list" => Ok(RequestOutcome {
                notifications: Vec::new(),
                result: super::sessions::roster(
                    &self.config.node,
                    &self.config.agent_did,
                    &self.config.behavior_id,
                )
                .await?,
            }),
            "x.ai/session/list" => {
                let params: super::sessions::ListParams =
                    serde_json::from_value(request.params.clone()).map_err(|error| {
                        invalid_params(format!("invalid session list: {error}"))
                    })?;
                params
                    .validate()
                    .map_err(|error| invalid_params(error.to_string()))?;
                let result = super::sessions::list(
                    &self.config.node,
                    &self.config.agent_did,
                    &self.config.behavior_id,
                    params,
                )
                .await?;
                Ok(RequestOutcome {
                    notifications: Vec::new(),
                    result,
                })
            }
            SESSION_NEW_METHOD => {
                let outcome = self.handle_session_new(request).await?;
                if let Some(session_id) = outcome.result.get("sessionId").and_then(Value::as_str) {
                    self.turns
                        .observe_session(
                            session_id,
                            prompt_sender.clone(),
                            self.projections.clone(),
                        )
                        .await;
                }
                Ok(outcome)
            }
            SESSION_SET_MODEL_METHOD => self.handle_session_set_model(request).await,
            SESSION_SET_MODE_METHOD => self.handle_session_set_mode(request, prompt_sender).await,
            SESSION_PROMPT_METHOD => self.handle_session_prompt(request, prompt_sender).await,
            SESSION_LOAD_METHOD => self.handle_session_load(request, prompt_sender).await,
            INTERJECT_METHOD | COMPACT_CONVERSATION_METHOD => {
                Err(shaped_stub_error(&request.method, &request.params))
            }
            // Only the three exact known subagent ext methods reach the
            // sibling leaf. Any other `x.ai/subagent/*` method falls
            // through to the typed `ShapedMethodNotFound` arm below so it
            // answers the exact `-32601`, never the sibling leaf's generic
            // (type-only-classified, hence `-32603`) anyhow error.
            SUBAGENT_GET_METHOD | SUBAGENT_LIST_RUNNING_METHOD | SUBAGENT_CANCEL_METHOD => {
                self.handle_subagent_ext_request(request.method.as_str(), request)
                    .await
            }
            "x.ai/task/kill" => {
                let params: super::task_control::KillTaskRequest =
                    serde_json::from_value(request.params.clone())
                        .map_err(|error| invalid_params(&format!("invalid task kill: {error}")))?;
                if params.session_id.trim().is_empty() || params.task_id.trim().is_empty() {
                    return Err(invalid_params(
                        "task kill requires non-empty sessionId and taskId",
                    ));
                }
                let sessions = self
                    .sessions
                    .lock()
                    .await
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>();
                let result = super::task_control::kill(
                    self.config.node.clone(),
                    &self.projections.background_executions,
                    &self.config.agent_did,
                    &sessions,
                    params,
                )
                .await?;
                Ok(RequestOutcome {
                    notifications: Vec::new(),
                    result: json!({"result":result}),
                })
            }
            other if other.starts_with("terminal/") => {
                // The client-side terminal/* methods are owned by
                // `projection::tools`: its shaped errors must be preserved
                // instead of collapsing into the generic method-not-found
                // below. `terminal/wait_for_exit` carries the pager's exact
                // `METHOD_NOT_FOUND` message
                // (`wait_for_exit_not_supported("pager")`); the other known
                // terminal methods carry the shim's own shaped wording.
                let error = super::projection::tools::handle_terminal_client_method(other)
                    .expect_err("terminal client methods are never supported");
                Err(anyhow::Error::new(ShapedMethodNotFound {
                    message: error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                }))
            }
            other => {
                // Unrouted methods stay explicit on the wire.
                Err(anyhow::Error::new(ShapedMethodNotFound {
                    message: format!("method {other:?} is not supported by the Gents Grok shim"),
                }))
            }
        }
    }

    /// Handle `initialize`.
    ///
    /// Session loading replays persisted observations through the same
    /// projection owner used for live delivery.
    async fn handle_initialize(&self, request: &AcpRequest) -> Result<RequestOutcome> {
        tracing::debug!(
            request_id = ?request.id,
            protocol_version = ACP_PROTOCOL_VERSION,
            "grok shim initialize"
        );
        Ok(RequestOutcome {
            notifications: Vec::new(),
            result: json!({
                "protocolVersion": ACP_PROTOCOL_VERSION,
                "agentCapabilities": {
                    "loadSession": true,
                    "prompt": true,
                    "cancel": true,
                    "setMode": true,
                    "setModel": true,
                },
                "authMethods": [{
                    "id": GENTS_AUTH_METHOD_ID,
                    "name": "Gents runtime",
                    "description": "Gents runtime identity",
                }],
            }),
        })
    }

    /// Handle `authenticate`.
    ///
    /// The only advertised method is `gents.runtime`; an unknown `methodId`
    /// is an explicit error, not a silent success. No credential document is
    /// written: the runtime's own identity is the credential.
    async fn handle_authenticate(&self, request: &AcpRequest) -> Result<RequestOutcome> {
        let method_id = request
            .params
            .get("methodId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if method_id != GENTS_AUTH_METHOD_ID {
            return Err(invalid_params(&format!(
                "unsupported auth method {method_id:?}; the Grok shim advertises only \
                 {GENTS_AUTH_METHOD_ID:?}"
            )));
        }
        Ok(RequestOutcome {
            notifications: Vec::new(),
            result: json!({
                "_meta": { "provider": "gents" },
            }),
        })
    }

    /// Handle `session/new`.
    ///
    /// The preferred `_meta.sessionId` is honored verbatim when non-empty;
    /// otherwise a fresh uuid is minted. Exactly one `AgentSession` document
    /// exists for the returned id afterwards — create-only when absent,
    /// matching the runtime's `request_session_projection` semantics, and
    /// never rewriting the `@immutable` `agent_did`/`requester_did` fields on
    /// an existing row.
    ///
    /// `cwd` and `mcpServers` are accepted and deliberately not persisted:
    /// `AgentSession` has no cwd field and the runtime serves from its own
    /// working directory, so fabricating either would be a schema violation.
    /// No `AgentConversation` and no `AgentRequest` rows are created here;
    /// the runtime materializes those through normal request execution, and
    /// fabricating them would desynchronize the durable timeline.
    ///
    /// Mode capabilities are **connection-scoped, not request-scoped**: the
    /// registering client advertised `yolo_mode`/`auto_mode`/`terminal` in
    /// its register envelope, and the reference leader injects the
    /// corresponding `_meta.yoloMode`/`autoMode`/`clientTerminal` keys from
    /// those capabilities. The per-request `_meta` still wins when the pager
    /// stamps an explicit override (`apply_permission_mode_override`), but an
    /// absent key falls back to the registered capability — never to `false`
    /// while the connection registered `true`.
    async fn handle_session_new(&self, request: &AcpRequest) -> Result<RequestOutcome> {
        let meta = request
            .params
            .get("_meta")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let preferred = meta
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let requested_model = meta.get("modelId").and_then(Value::as_str);

        // Registration-derived capabilities: the connection's stored
        // registration, with the request `_meta` acting only as an explicit
        // per-session override.
        let registered = self.registered_capabilities.as_ref();
        let yolo_mode = meta
            .get("yoloMode")
            .and_then(Value::as_bool)
            .unwrap_or_else(|| registered.is_some_and(|caps| caps.yolo_mode));
        let auto_mode = meta
            .get("autoMode")
            .and_then(Value::as_bool)
            .unwrap_or_else(|| registered.is_some_and(|caps| caps.auto_mode));
        let client_terminal = meta
            .get("clientTerminal")
            .and_then(Value::as_bool)
            .unwrap_or_else(|| registered.is_some_and(|caps| caps.terminal));

        if let Some(requested) = requested_model.map(str::trim).filter(|v| !v.is_empty()) {
            if requested != self.config.current_model.model_id {
                return Err(invalid_params(&format!(
                    "model {requested:?} is not in the bound catalog; the Grok shim serves \
                     {:?} from the bound AgentBehavior/InferenceProfile",
                    self.config.current_model.model_id
                )));
            }
        }

        let session_id = preferred.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        ensure_session_document(&self.config, &session_id).await?;

        {
            let mut sessions = self.sessions.lock().await;
            if self
                .loading_sessions
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .contains(&session_id)
            {
                return Err(invalid_params("session is loading on this connection"));
            }
            sessions.insert(session_id.clone(), AcpSessionState::new());
        }
        tracing::info!(%session_id, "grok shim session/new");

        // Audited result shape: sessionId + nested models + _meta. The
        // `models` object must stay nested — the pager reads
        // `result["models"]["currentModelId"]`. `clientTerminal` tells the
        // agent whether terminal/* commands route to the client; the
        // reference pager registers `terminal: false`, so the shaped
        // terminal/* not-supported stubs stay the answered behavior.
        Ok(RequestOutcome {
            // The stock pager seeds an MCP-initialization spinner before it
            // sends session/new and clears it only when this extension
            // notification arrives. Gents does not initialize the request's
            // client-side MCP servers, so report the truthful zero count once
            // the durable and connection-local session creation both succeed.
            notifications: vec![json!({
                "__method": MCP_INITIALIZED_METHOD,
                "sessionId": session_id,
                "mcpToolCount": 0_u32,
                "elapsedMs": 0_u64,
            })],
            result: json!({
                "sessionId": session_id,
                "models": self.config.models_object(),
                "_meta": {
                    "yoloMode": yolo_mode,
                    "autoMode": auto_mode,
                    "clientTerminal": client_terminal,
                    "modelId": self.config.current_model.model_id,
                },
            }),
        })
    }

    /// Replay existing observations, then attach after the RPC response.
    /// Loading does not reactivate or otherwise mutate AgentSession rows.
    async fn handle_session_load(
        &self,
        request: &AcpRequest,
        sender: &PromptSender,
    ) -> Result<RequestOutcome> {
        let session_id = required_session_id(&request.params)?;
        let _reservation = SessionLoadReservation::reserve(&self.loading_sessions, &session_id)?;
        if self.sessions.lock().await.contains_key(&session_id) {
            return Err(invalid_params(
                "session is already attached on this connection",
            ));
        }
        let attached_at = chrono::Utc::now().to_rfc3339();
        let rows = super::sessions::load(
            &self.config.node,
            &self.config.agent_did,
            &self.config.behavior_id,
            &session_id,
        )
        .await?;
        let running = self
            .turns
            .replay_session(&session_id, &rows, sender, &self.projections)
            .await?;
        let mut state = AcpSessionState::new();
        state.resume_from = Some(attached_at);
        self.sessions.lock().await.insert(session_id.clone(), state);
        Ok(RequestOutcome {
            notifications: vec![
                json!({"__method":MCP_INITIALIZED_METHOD, "sessionId":session_id, "mcpToolCount":0, "elapsedMs":0}),
            ],
            result: json!({"models":self.config.models_object(), "_meta":{
                "x.ai/runningPromptId":running, "modelId":self.config.current_model.model_id,
                "gents/codeRestored":false
            }}),
        })
    }

    /// Called for this load RPC only, after its response is enqueued. A
    /// concurrent unrelated RPC must not release another load's handoff.
    async fn activate_loaded_session(&self, payload: &str, sender: &PromptSender) {
        let Ok(request) = AcpRequest::from_payload(payload) else {
            return;
        };
        if request.method != SESSION_LOAD_METHOD {
            return;
        }
        let Ok(session_id) = required_session_id(&request.params) else {
            return;
        };
        let attached = self
            .sessions
            .lock()
            .await
            .get_mut(&session_id)
            .and_then(|state| state.resume_from.take());
        if let Some(attached) = attached {
            self.turns
                .observe_session_since(
                    &session_id,
                    attached,
                    sender.clone(),
                    self.projections.clone(),
                )
                .await;
        }
    }

    /// Handle `session/set_model`.
    ///
    /// The runtime has no per-session model field: the bound
    /// `AgentBehavior` selects the model every request is served with. The
    /// switch therefore validates against the bound catalog and emits
    /// `x.ai/models/update` so the pager refreshes its catalog in place
    /// (leaving current/effort alone, as the reference `update_catalog`
    /// does).
    ///
    /// `_meta.reasoningEffort` is rejected explicitly as invalid params:
    /// the shim advertises `supportsReasoningEffort: false`, and the
    /// catalog's absence of an effort list is the pager's contract for
    /// "unsupported" — so accepting (and silently storing) an effort the
    /// catalog never advertised would fabricate a capability that never
    /// reaches `SubmitRequestOptions` or provider inference anyway.
    async fn handle_session_set_model(&self, request: &AcpRequest) -> Result<RequestOutcome> {
        let session_id = required_session_id(&request.params)?;
        let requested = request
            .params
            .get("modelId")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| invalid_params("session/set_model requires a non-empty modelId"))?;
        if requested != self.config.current_model.model_id {
            return Err(invalid_params(&format!(
                "model {requested:?} is not in the bound catalog; the Grok shim serves \
                 {:?} from the bound AgentBehavior/InferenceProfile",
                self.config.current_model.model_id
            )));
        }
        if let Some(effort) = request
            .params
            .get("_meta")
            .and_then(|meta| meta.get("reasoningEffort"))
        {
            return Err(invalid_params(&format!(
                "session/set_model does not support _meta.reasoningEffort {effort:?}: the \
                 advertised catalog is text-only with supportsReasoningEffort=false, and a \
                 selected effort is not plumbed into SubmitRequestOptions or provider \
                 inference, so it is rejected rather than silently stored"
            )));
        }

        // Validate the session exists before answering: an unknown session
        // never receives a fabricated catalog switch.
        {
            let sessions = self.sessions.lock().await;
            if !sessions.contains_key(&session_id) {
                anyhow::bail!("unknown session {session_id:?}");
            }
        }

        tracing::info!(%session_id, %requested, "grok shim session/set_model");
        // Empty result per the audited wire; the catalog refresh rides the
        // x.ai/models/update ext notification emitted before the response.
        Ok(RequestOutcome {
            notifications: vec![{
                let mut notification = self.config.models_object();
                notification["__method"] = json!(MODELS_UPDATE_METHOD);
                notification
            }],
            result: json!({}),
        })
    }

    /// Handle `session/set_mode`.
    ///
    /// Mode is a client capability concern with no `AgentSession` field; the
    /// switch records it and emits a `current_mode_update` session
    /// notification so the pager renders the new mode.
    ///
    /// The mode update rides the connection's common session-update send
    /// path: the per-session send lock is held across reserve → stamp →
    /// enqueue → commit, so the update's event id is allocated in the same
    /// order it is enqueued. This is what keeps a `set_mode` racing a live
    /// prompt on the same session from delivering `session-2` before
    /// `session-1` (the pager deduplicates monotonically and would drop the
    /// real mode update as stale). The event id is consumed only by the
    /// successful enqueue; a failed send rolls the reservation back and the
    /// request surfaces the failure.
    ///
    /// **State/send atomicity:** the mode state is committed inside the
    /// channel's commit hook — the same critical section that enqueued the
    /// notification — so a failed send leaves the mode state untouched and
    /// concurrent mode changes serialize coherently with the update. A
    /// `set_mode` whose notification cannot be enqueued fails the request
    /// without leaving the session's recorded mode pointing at a mode the
    /// pager never saw acknowledged.
    async fn handle_session_set_mode(
        &self,
        request: &AcpRequest,
        prompt_sender: &PromptSender,
    ) -> Result<RequestOutcome> {
        let session_id = required_session_id(&request.params)?;
        let mode_id = request
            .params
            .get("modeId")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| invalid_params("session/set_mode requires a non-empty modeId"))?;

        // Validate the session exists before any send: an unknown session
        // must fail without touching the sequencer or the wire.
        {
            let sessions = self.sessions.lock().await;
            if !sessions.contains_key(&session_id) {
                anyhow::bail!("unknown session {session_id:?}");
            }
        }

        // The mode update shares the projection sequencer's per-session
        // event-id space with the user echo and the projected updates, so a
        // `set_mode` and a prompt on the same session can never stamp the
        // same eventId (the pager deduplicates non-replay counters by id).
        // The mode state is committed inside the hook — after the line was
        // enqueued through the actual sender (the live `AcpOutbound` in
        // production, the headless buffer in tests), never before — so a
        // failed send consumes no event id and leaves the mode state at its
        // previous value.
        let commit = ModeStateCommit {
            sessions: &self.sessions,
            session_id: session_id.clone(),
            mode_id: mode_id.to_string(),
        };
        self.projections
            .session_updates()
            .send_with_commit(
                &session_id,
                |event_id, total_tokens| {
                    let meta = stamp_update_meta(
                        event_id,
                        total_tokens,
                        None,
                        None,
                        UpdateTimestamps {
                            agent_timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
                            ..UpdateTimestamps::default()
                        },
                    );
                    Ok(json!({
                        "jsonrpc": "2.0",
                        "method": SESSION_UPDATE_METHOD,
                        "params": {
                            "sessionId": session_id,
                            "update": {
                                "sessionUpdate": "current_mode_update",
                                "currentModeId": mode_id,
                            },
                            "_meta": meta,
                        },
                    }))
                },
                PromptSenderLine(prompt_sender),
                commit,
            )
            .await?;
        tracing::info!(%session_id, %mode_id, "grok shim session/set_mode");
        Ok(RequestOutcome {
            notifications: Vec::new(),
            result: json!({}),
        })
    }

    /// Handle `session/prompt` by delegating the whole turn to the sibling
    /// [`TurnManager`].
    ///
    /// The turn manager owns the connection-scoped pending prompt, registers
    /// the runtime request id before the first fallible outbound send, keeps
    /// the JSON-RPC response deferred until terminalization, and streams the
    /// turn's `session/update` notifications through `sender` — live through
    /// the connection's [`AcpOutbound`] in production, buffered for the
    /// headless dispatch path. The result is the audited `stopReason` value
    /// the pager's drain loop waits for.
    async fn handle_session_prompt(
        &self,
        request: &AcpRequest,
        prompt_sender: &PromptSender,
    ) -> Result<RequestOutcome> {
        // The prompt shape is caller-controlled: every parser failure from
        // turn.rs (missing/empty prompt, block without text, invalid
        // screenMode, missing sessionId) is a client invalid-params error,
        // wrapped here at the handler boundary without modifying turn.rs.
        // The source message is preserved verbatim for diagnostics.
        let parsed = parse_prompt_request(&request.params, request.id.clone())
            .map_err(|error| invalid_params(error.to_string()))?;
        {
            let sessions = self.sessions.lock().await;
            let session = sessions
                .get(&parsed.session_id)
                .context("session must be created or loaded before prompting")?;
            if session.resume_from.is_some() {
                return Err(invalid_params(
                    "session replay has not handed off to live observation",
                ));
            }
        }
        // Turn-lifecycle failures after a well-formed prompt are operational
        // (submission/runtime/send) and stay internal `-32603`; the
        // caller-controlled shape validation happened in the parse above.
        if let [block] = parsed.prompt.as_slice() {
            if block.kind == "text"
                && block
                    .meta
                    .as_ref()
                    .is_none_or(|meta| meta.as_object().is_some_and(|meta| meta.is_empty()))
            {
                if let Some(command) = super::goals::GoalCommand::parse(&block.text)
                    .map_err(|error| invalid_params(error.to_string()))?
                    .filter(|command| !matches!(command, super::goals::GoalCommand::Create { .. }))
                {
                    // A stock slash control is an operator action, not a
                    // model prompt. Never ask inference to interpret pause.
                    super::sessions::load(
                        &self.config.node,
                        &self.config.agent_did,
                        &self.config.behavior_id,
                        &parsed.session_id,
                    )
                    .await?;
                    let reply = command
                        .execute(
                            &self.config.node,
                            &self.config.agent_did,
                            &parsed.session_id,
                        )
                        .await?;
                    for (kind, text) in [
                        ("user_message_chunk", block.text.as_str()),
                        ("agent_message_chunk", reply.as_str()),
                    ] {
                        self.projections.session_updates().send(&parsed.session_id, |event_id, total_tokens| {
                            Ok(super::projection::session_notification_for_method("session/update", &parsed.session_id,
                                json!({"sessionUpdate":kind,"content":{"type":"text","text":text}, "_meta":{"hostTurn":true}}),
                                super::projection::stamp_update_meta(event_id, total_tokens,
                                    parsed.prompt_id.as_deref(), None, super::projection::UpdateTimestamps::default())))
                        }, super::turn::PromptSenderLine(prompt_sender)).await?;
                    }
                    self.turns
                        .observe_session(
                            &parsed.session_id,
                            prompt_sender.clone(),
                            self.projections.clone(),
                        )
                        .await;
                    return Ok(RequestOutcome {
                        notifications: Vec::new(),
                        result: json!({"stopReason":"end_turn"}),
                    });
                }
            }
        }
        let stop_reason = self
            .turns
            .handle_prompt(parsed, prompt_sender, &self.projections)
            .await?;
        // Every turn notification was already enqueued through the common
        // session-update send path while the turn ran — live through the
        // connection's [`AcpOutbound`], buffered for the headless dispatch
        // path, where `dispatch_request` drains the buffer into the result.
        Ok(RequestOutcome {
            notifications: Vec::new(),
            result: stop_reason,
        })
    }

    /// Route one of the three exact known `x.ai/subagent/*` ext requests to
    /// the sibling subagents leaf, which owns the audited not-found result
    /// shapes. The leaf is pure: it never queries `Task` rows and never
    /// fabricates child documents.
    ///
    /// Enforce the connection's session boundary before any descendant read.
    /// Stock get/cancel requests carry only subagentId, whereas list_running
    /// requires sessionId. An optional explicit session is validated equally.
    async fn handle_subagent_ext_request(
        &self,
        method: &str,
        request: &AcpRequest,
    ) -> Result<RequestOutcome> {
        let session_id = if method == SUBAGENT_LIST_RUNNING_METHOD
            || request.params.get("sessionId").is_some()
        {
            Some(required_session_id(&request.params)?)
        } else {
            None
        };
        if method != SUBAGENT_LIST_RUNNING_METHOD {
            let id = request.params.get("subagentId");
            match id {
                None | Some(Value::Null) => {
                    return Err(invalid_params("subagent request requires a subagentId"));
                }
                Some(value) if !value.is_string() => {
                    return Err(invalid_params(&format!(
                        "subagent request requires a string subagentId, got {value:?}"
                    )));
                }
                Some(value) if value.as_str().is_some_and(|id| id.trim().is_empty()) => {
                    return Err(invalid_params(
                        "subagent request requires a non-empty subagentId",
                    ));
                }
                _ => {}
            }
        }
        for (key, valid) in [
            (
                "block",
                request
                    .params
                    .get("block")
                    .is_none_or(|v| v.is_null() || v.is_boolean()),
            ),
            (
                "timeoutMs",
                request
                    .params
                    .get("timeoutMs")
                    .is_none_or(|v| v.is_null() || v.as_u64().is_some()),
            ),
        ] {
            if !valid {
                return Err(invalid_params(&format!("invalid subagent {key}")));
            }
        }
        let sessions = {
            let known = self.sessions.lock().await;
            match session_id {
                Some(session) => {
                    if !known.contains_key(&session) {
                        return Err(invalid_params(
                            "subagent request names an unknown connection session",
                        ));
                    }
                    vec![session]
                }
                None => known.keys().cloned().collect(),
            }
        };
        let result = super::projection::subagents::control::handle(
            self.config.node.clone(),
            &self.config.agent_did,
            &sessions,
            method,
            &request.params,
            self.config.current_model.effective_context_window(),
        )
        .await?;
        Ok(RequestOutcome {
            notifications: Vec::new(),
            // The stock shell wraps extension DTOs in ExtMethodResult.
            result: json!({"result": result}),
        })
    }
}

// ---------------------------------------------------------------------------
// Delegate binding
// ---------------------------------------------------------------------------

/// A JSON-RPC method-not-found failure, distinguished from an internal error
/// so the dispatcher can shape the envelope with the exact audited code.
#[derive(Debug)]
pub(crate) struct ShapedMethodNotFound {
    pub(crate) message: String,
}

impl std::fmt::Display for ShapedMethodNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ShapedMethodNotFound {}

/// Build the shaped method-not-found error for the remaining audited
/// stubs, carrying the explicit owned-transition explanation the audit
/// requires. None of these stubs touches a document.
fn shaped_stub_error(method: &str, params: &Value) -> anyhow::Error {
    let session = optional_session_id(params).unwrap_or_default();
    let message = match method {
        INTERJECT_METHOD => format!(
            "x.ai/interject is not supported by the Gents Grok shim: the owned completion \
             loop has no formally specified injection transition, and writing a detached \
             AgentMessage would not affect provider input. No AgentMessage or \
             AgentRequest document was fabricated for session {session:?}, and no \
             x.ai/session/interjection notification was emitted."
        ),
        COMPACT_CONVERSATION_METHOD => format!(
            "x.ai/compact_conversation is not supported by the Gents Grok shim: \
             CompactionEntry is runtime-owned, its schema has no tokens_before or \
             tokens_after fields, and AgentSession has no usage counters. No \
             CompactionEntry or AgentSession field was fabricated for session \
             {session:?}."
        ),
        other => format!("method {other:?} is not supported by the Gents Grok shim"),
    };
    anyhow::Error::new(ShapedMethodNotFound { message })
}

/// Map a handler error to its JSON-RPC code.
///
/// Classification is type-driven, never by message string matching:
/// - [`ShapedMethodNotFound`] (shaped stubs, unrouted methods, the
///   `terminal/*` client methods) is the audited `-32601`.
/// - [`InvalidParams`] (caller-controlled shape/value validation: the
///   `session/prompt` parser errors, missing/empty `sessionId`/`modelId`/
///   `modeId`, a model outside the bound catalog, an unsupported auth
///   method, `_meta.reasoningEffort` on an unadvertised feature,
///   malformed subagent params — including a missing/blank/non-string
///   `sessionId` on the known subagent methods) is `-32602`.
/// - Anything else is an operational failure (GraphQL/storage/outbound/
///   runtime) and stays internal `-32603`, its message surfaced verbatim
///   for diagnosis.
fn error_code_for(error: &anyhow::Error) -> i64 {
    if error.downcast_ref::<ShapedMethodNotFound>().is_some() {
        JSONRPC_METHOD_NOT_FOUND
    } else if error.downcast_ref::<InvalidParams>().is_some() {
        JSONRPC_INVALID_PARAMS
    } else {
        JSONRPC_INTERNAL_ERROR
    }
}

/// A caller-controlled validation failure: the request's params were
/// structurally or semantically invalid for the method. Distinguished from
/// an internal failure so the dispatcher can answer the exact JSON-RPC
/// invalid-params code (`-32602`) while preserving the source message for
/// diagnostics.
#[derive(Debug)]
struct InvalidParams {
    message: String,
}

impl std::fmt::Display for InvalidParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for InvalidParams {}

/// Build a typed invalid-params error from a validation message.
fn invalid_params(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(InvalidParams {
        message: message.into(),
    })
}

/// Extract the required `sessionId` from request params.
fn required_session_id(params: &Value) -> Result<String> {
    params
        .get("sessionId")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| invalid_params("request requires a non-empty sessionId"))
}

/// Extract an optional `sessionId` from request params for stub messaging.
fn optional_session_id(params: &Value) -> Option<String> {
    params
        .get("sessionId")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

/// Create the `AgentSession` document for `session_id` if absent.
///
/// Mirrors the runtime's `request_session_projection`: update the mutable
/// identity fields when the row exists, create when it does not. The
/// `@immutable` fields (`agent_did`, `requester_did`) are only ever supplied
/// on create, and — matching the runtime's claim-admission behavior — a row
/// bound to a different behavior id *or* a different immutable `agent_did`
/// is an explicit error rather than a silent rewrite of session identity.
async fn ensure_session_document(config: &AcpServiceConfig, session_id: &str) -> Result<()> {
    let escaped_session_id = escape_graphql_string(session_id);
    let lookup = format!(
        r#"{{
            AgentSession(filter: {{ session_id: {{ _eq: "{escaped_session_id}" }} }}) {{
                session_id
                agent_did
                requester_did
                behavior_id
            }}
        }}"#
    );
    let response = config.node.execute(&lookup).await;
    ensure_no_errors(&response, "grok shim AgentSession lookup")?;
    let rows = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentSession"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if let Some(existing) = rows
        .iter()
        .filter_map(|row| row.get("behavior_id").and_then(Value::as_str))
        .map(str::trim)
        .find(|value| !value.is_empty())
    {
        if existing != config.behavior_id.as_ref() {
            anyhow::bail!(
                "session {session_id:?} already exists with behavior {existing:?}; the \
                 Grok shim is bound to {:?} and will not rewrite session identity",
                config.behavior_id
            );
        }
    }
    // The serving agent DID is `@immutable` on `AgentSession`: an existing
    // row stamped for a different principal is a hard identity mismatch and
    // must never be reactivated under this shim's identity.
    if let Some(existing) = rows
        .iter()
        .filter_map(|row| row.get("agent_did").and_then(Value::as_str))
        .map(str::trim)
        .find(|value| !value.is_empty())
    {
        if existing != config.agent_did.as_ref() {
            anyhow::bail!(
                "session {session_id:?} already exists with agent {existing:?}; the \
                 Grok shim serves as {:?} and will not reactivate a session bound to \
                 a different immutable agent_did",
                config.agent_did
            );
        }
    }

    let escaped_agent_name = escape_graphql_string(&config.agent_name);
    // Older shim-created sessions omitted requester_did. They remain
    // readable only through exactly scoped requests, but an explicit
    // foreign requester must never be reactivated by session/new.
    anyhow::ensure!(
        rows.iter().all(|row| row
            .get("requester_did")
            .and_then(Value::as_str)
            .is_none_or(|requester| requester == config.agent_did.as_ref())),
        "session belongs to a different immutable requester_did"
    );
    let escaped_agent_did = escape_graphql_string(&config.agent_did);
    let escaped_behavior_id = escape_graphql_string(&config.behavior_id);
    // DateTime fields round-trip through the "....Z" form the runtime's own
    // fixtures use; to_rfc3339() emits "+00:00" instead.
    let started = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let escaped_started = escape_graphql_string(&started);
    if rows.is_empty() {
        let create = format!(
            r#"mutation {{
                create_AgentSession(input: {{
                    session_id: "{escaped_session_id}",
                    agent_name: "{escaped_agent_name}",
                    agent_did: "{escaped_agent_did}",
                    requester_did: "{escaped_agent_did}",
                    behavior_id: "{escaped_behavior_id}",
                    started: "{escaped_started}",
                    status: "active"
                }}) {{ _docID }}
            }}"#
        );
        let response = config.node.execute(&create).await;
        ensure_no_errors(&response, "grok shim AgentSession create")?;
    } else {
        // Reactivate an existing row without touching the immutable fields.
        let update = format!(
            r#"mutation {{
                update_AgentSession(
                    filter: {{ session_id: {{ _eq: "{escaped_session_id}" }} }},
                    input: {{
                        agent_name: "{escaped_agent_name}",
                        behavior_id: "{escaped_behavior_id}",
                        status: "active",
                        ended: null
                    }}
                ) {{ _docID }}
            }}"#
        );
        let response = config.node.execute(&update).await;
        ensure_no_errors(&response, "grok shim AgentSession reactivate")?;
    }
    Ok(())
}

/// Bind the service to the leader server's delegate trait.
///
/// `spawn_leader` takes an `Arc<dyn AcpDelegate>`, so the trait must be
/// object-safe, and the workspace forbids adding an `async-trait` dependency
/// to this crate. The object-safe form is therefore a borrowed boxed future
/// over the payload line that [`super::protocol`] decodes from the
/// `{"type":"acp","payload":"..."}` envelope. The exact trait signature is
/// owned by the `server` slice; convergence reconciles this one impl block
/// if the sibling chose an equivalent spelling.
impl AcpDelegate for AcpService {
    fn handle_acp<'a>(
        &'a self,
        payload: &'a str,
        outbound: AcpOutbound,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            // The foreground transport turn ends only after its response is
            // enqueued. Keep autonomous session output behind that boundary,
            // including the gap after the durable request terminalizes.
            let envelope = serde_json::from_str::<Value>(payload).ok();
            let _foreground_delivery = if let Some(session_id) = envelope
                .as_ref()
                .filter(|value| {
                    value.get("method").and_then(Value::as_str) == Some(SESSION_PROMPT_METHOD)
                })
                .and_then(|value| value.pointer("/params/sessionId"))
                .and_then(Value::as_str)
            {
                Some(self.turns.begin_foreground_delivery(session_id).await)
            } else {
                None
            };
            // The live production path: every notification the turn produces
            // — the user echo plus each novel durable projection update — is
            // sent through the connection's outbound as it happens, and the
            // deferred `session/prompt` response is sent here once the turn
            // resolves. Notifications that resolve before the turn (model
            // catalog updates, mode updates) still precede their response.
            let sender = PromptSender::Live {
                outbound: outbound.clone(),
            };
            let dispatch = self.dispatch_with_sender(payload, &sender).await;
            // The native pager attaches routing from session/new's response.
            // An earlier MCP completion can be received yet dropped, leaving
            // its Starting session spinner alive indefinitely. Other extension
            // notifications keep their established pre-response ordering.
            let (after_response, before_response): (Vec<_>, Vec<_>) = dispatch
                .notifications
                .into_iter()
                .partition(|notification| {
                    serde_json::from_str::<Value>(notification)
                        .ok()
                        .and_then(|value| {
                            value
                                .get("method")
                                .and_then(Value::as_str)
                                .map(|method| internal_method(method).to_owned())
                        })
                        .as_deref()
                        == Some(MCP_INITIALIZED_METHOD)
                });
            for notification in before_response {
                outbound.send(notification).await?;
            }
            if let Some(response) = dispatch.response {
                outbound.send(response).await?;
            }
            for notification in after_response {
                outbound.send(notification).await?;
            }
            self.activate_loaded_session(payload, &sender).await;
            Ok(())
        })
    }

    fn on_disconnect(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if let Err(error) = self.turns.handle_disconnect().await {
                tracing::warn!(
                    %error,
                    "grok shim failed to drain pending turns on disconnect"
                );
            }
        })
    }
}

/// Test-only buffered dispatch: run the full dispatch through a `Buffer`
/// sender so the notifications surface as dispatch notifications after the
/// frame resolves, without a live connection.
#[cfg(test)]
impl AcpService {
    pub(super) async fn handle_acp_payload(&self, payload: &str) -> AcpDispatch {
        let buffer = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let sender = PromptSender::Buffer {
            buffer: buffer.clone(),
        };
        let dispatch = self.dispatch_with_sender(payload, &sender).await;
        self.activate_loaded_session(payload, &sender).await;
        dispatch
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::grok_shim::projection::tools::PAGER_WAIT_FOR_EXIT_MESSAGE;
    use crate::commands::grok_shim::turn::PromptBlock;
    use tokio::sync::Mutex;

    fn bound_model() -> BoundModel {
        BoundModel {
            model_id: "GLM-5.3-NVFP4".to_string(),
            name: "GLM 5.3 NVFP4".to_string(),
            total_context_tokens: 262_144,
        }
    }

    /// Build the service configuration over a throwaway node.
    ///
    /// The staging `TempDir` is returned *first* so an ordinary
    /// `let (_staging, config) = config().await;` binding drops the node
    /// (inside the config/service) before the `TempDir`: the node's storage
    /// path lives inside the guard's directory and must not be deleted out
    /// from under a still-alive node. The guard is never abandoned with
    /// `keep()` or leaked with `mem::forget`; it cleans the directory up
    /// when the test ends.
    async fn config() -> (tempfile::TempDir, AcpServiceConfig) {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let node = Arc::new(
            EmbeddedNode::builder()
                .data_path(tempdir.path().join("node"))
                .with_storage_backend(gents::defra_node::StorageBackend::Regolith)
                .build()
                .await
                .expect("embedded node"),
        );
        (
            tempdir,
            AcpServiceConfig {
                node,
                agent_did: Arc::from("did:test:grok-shim"),
                agent_name: Arc::from("grok-shim-test"),
                behavior_id: Arc::from("did:test:grok-shim:default"),
                current_model: bound_model(),
            },
        )
    }

    /// Build a service over a throwaway node with schemas ensured.
    ///
    /// The sibling constructors are the assembly slice's seam: `TurnManager`
    /// and `ProjectionEngine` are built from the same embedded node. Their
    /// exact constructor signatures are owned by those slices and reconciled
    /// at convergence.
    ///
    /// The staging `TempDir` guard is returned *first*: the ordinary binding
    /// `let (_staging, service) = test_service().await;` then guarantees the
    /// service (and its node) drop before the `TempDir` deletes the node's
    /// storage directory, because tuple/struct fields drop in declaration
    /// order. The service/node must drop before the `TempDir` in every test.
    async fn test_service() -> (tempfile::TempDir, AcpService) {
        let (staging, config) = config().await;
        gents::schema::ensure_runtime_schemas(config.node.as_ref())
            .await
            .expect("runtime schemas");
        let turns = Arc::new(TurnManager::new(
            config.node.clone(),
            super::super::turn::TurnManagerConfig {
                agent_did: config.agent_did.to_string(),
                behavior_id: config.behavior_id.to_string(),
                graphql: "http://127.0.0.1:8000/api/v0/graphql".to_string(),
            },
        ));
        let projections = Arc::new(ProjectionEngine::new(
            config.node.clone(),
            super::super::projection::BoundModelContext::new(
                config.current_model.model_id.clone(),
                config.current_model.name.clone(),
                config.current_model.total_context_tokens,
            ),
        ));
        (staging, AcpService::new(config, turns, projections))
    }

    #[tokio::test]
    async fn goal_slash_controls_use_runtime_owner_without_submitting_inference() {
        let (_dir, service) = test_service().await;
        let new = service
            .handle_acp_payload(&request_payload(
                "session/new",
                json!({"_meta":{"sessionId":"goal-controls"}}),
            ))
            .await;
        assert!(parse_response(new.response.as_deref().unwrap())
            .get("error")
            .is_none());
        let node = &service.config.node;
        gents::goal::set_goal(
            node,
            &service.config.agent_did,
            "goal-controls",
            Some("Finish the feature"),
            None,
            Some(Some(1000)),
        )
        .await
        .unwrap();
        for (command, expected) in [
            ("status", Some("active")),
            ("pause", Some("paused")),
            ("resume", Some("active")),
            ("clear", None),
        ] {
            let dispatch = service.handle_acp_payload(&request_payload("session/prompt", json!({
                "sessionId":"goal-controls", "prompt":[{"type":"text", "text":format!("/goal {command}"), "meta":{}}]
            }))).await;
            let response = parse_response(dispatch.response.as_deref().unwrap());
            assert_eq!(response["result"]["stopReason"], "end_turn", "{response}");
            assert!(dispatch.notifications.iter().any(|line| {
                let update = parse_response(line);
                update["params"]["update"]["sessionUpdate"] == "agent_message_chunk"
                    && update["params"]["update"]["_meta"]["hostTurn"] == true
            }));
            let goal =
                gents::goal::load_canonical_goal(node, &service.config.agent_did, "goal-controls")
                    .await
                    .unwrap();
            assert_eq!(goal.as_ref().map(|goal| goal.status.as_str()), expected);
        }
        let requests = node.execute("{AgentRequest {_docID}}").await;
        ensure_no_errors(&requests, "goal controls do not infer").unwrap();
        assert!(requests.data.as_ref().unwrap()["AgentRequest"]
            .as_array()
            .unwrap()
            .is_empty());
        let denied = service
            .handle_acp_payload(&request_payload(
                "session/prompt",
                json!({
                    "sessionId":"not-attached", "prompt":[{"type":"text", "text":"/goal clear"}]
                }),
            ))
            .await;
        assert!(parse_response(denied.response.as_deref().unwrap())
            .get("error")
            .is_some());
        service.turns.handle_disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn stalled_replay_reserves_only_its_session_and_abort_releases_it() {
        let (_dir, service) = test_service().await;
        let service = Arc::new(service);
        ensure_session_document(&service.config, "stalled-history")
            .await
            .unwrap();
        let seeded = service.config.node.execute(r#"mutation {create_AgentRequest(input:{
            request_id:"stalled-request", session_id:"stalled-history", agent_did:"did:test:grok-shim",
            requester_did:"did:test:grok-shim", content:"Original prompt", lifecycle_state:"completed",
            created_at:"2026-09-01T12:00:00Z"
        }) {_docID}}"#).await;
        ensure_no_errors(&seeded, "seed stalled replay").unwrap();
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let hold_output = buffer.lock().await;
        let sender = PromptSender::Buffer {
            buffer: buffer.clone(),
        };
        let request = AcpRequest::from_payload(&request_payload(
            "session/load",
            json!({"sessionId":"stalled-history"}),
        ))
        .unwrap();
        let task = tokio::spawn({
            let service = service.clone();
            let sender = sender.clone();
            let request = request.clone();
            async move { service.handle_session_load(&request, &sender).await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !service
                .loading_sessions
                .lock()
                .unwrap()
                .contains("stalled-history")
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("load reservation");
        let unrelated = AcpRequest::from_payload(&request_payload(
            "session/new",
            json!({"_meta":{"sessionId":"unrelated"}}),
        ))
        .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            service.handle_session_new(&unrelated),
        )
        .await
        .expect("stalled replay must not block another session")
        .unwrap();
        assert!(service
            .handle_session_load(&request, &sender)
            .await
            .is_err());
        let conflict = AcpRequest::from_payload(&request_payload(
            "session/new",
            json!({"_meta":{"sessionId":"stalled-history"}}),
        ))
        .unwrap();
        assert!(service.handle_session_new(&conflict).await.is_err());
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        assert!(service.loading_sessions.lock().unwrap().is_empty());
        drop(hold_output);
        service
            .handle_session_load(&request, &sender)
            .await
            .unwrap();
        assert!(service.loading_sessions.lock().unwrap().is_empty());
        assert!(service
            .sessions
            .lock()
            .await
            .contains_key("stalled-history"));
    }

    #[tokio::test]
    async fn session_load_replays_all_message_pages_without_creating_requests() {
        let (_dir, service) = test_service().await;
        ensure_session_document(&service.config, "resume-history")
            .await
            .unwrap();
        let node = &service.config.node;
        let result = node.execute(r#"mutation { create_AgentRequest(input: {
            request_id: "resume-request", session_id: "resume-history", agent_did: "did:test:grok-shim",
            requester_did: "did:test:grok-shim", content: "Original human prompt",
            lifecycle_state: "completed", created_at: "2026-09-01T12:00:00Z"
        }) { _docID } }"#).await;
        ensure_no_errors(&result, "seed resume request").unwrap();
        for sequence in 1..=70 {
            let content = serde_json::to_string(&json!({"role":"assistant", "content":[{"type":"text", "text":format!("REPLAY_{sequence:03}\n")}]})).unwrap();
            let result = node.execute(&format!(r#"mutation {{ create_AgentMessage(input: {{
                message_key: "resume-message-{sequence}", request_id: "resume-request", session_id: "resume-history",
                agent_did: "did:test:grok-shim", requester_did: "did:test:grok-shim",
                sequence: {sequence}, role: "assistant", content: "{}", timestamp: "2026-09-01T12:00:00Z"
            }}) {{_docID}} }}"#, escape_graphql_string(&content))).await;
            ensure_no_errors(&result, "seed replay page").unwrap();
        }
        let dispatch = service
            .handle_acp_payload(&request_payload(
                "session/load",
                json!({"sessionId":"resume-history", "cwd":"/tmp", "mcpServers":[]}),
            ))
            .await;
        let response = parse_response(dispatch.response.as_deref().unwrap());
        assert!(response.get("error").is_none(), "{response}");
        assert!(response["result"]["_meta"]["x.ai/runningPromptId"].is_null());
        let events: Vec<_> = dispatch
            .notifications
            .iter()
            .map(|line| parse_response(line))
            .filter(|line| line["method"] == "session/update")
            .collect();
        assert!(events
            .iter()
            .all(|event| event["params"]["_meta"]["isReplay"] == true));
        let texts: Vec<_> = events
            .iter()
            .filter_map(|event| {
                event
                    .pointer("/params/update/content/text")
                    .and_then(Value::as_str)
            })
            .collect();
        assert_eq!(
            texts
                .iter()
                .filter(|text| **text == "Original human prompt")
                .count(),
            1
        );
        let assistant = texts
            .iter()
            .filter(|text| text.starts_with("REPLAY_"))
            .copied()
            .collect::<String>();
        assert_eq!(
            assistant,
            (1..=70)
                .map(|sequence| format!("REPLAY_{sequence:03}\n"))
                .collect::<String>()
        );
        let result = node
            .execute(r#"{ AgentRequest { request_id } AgentSession { session_id requester_did } }"#)
            .await;
        ensure_no_errors(&result, "resume write check").unwrap();
        assert_eq!(
            result.data.as_ref().unwrap()["AgentRequest"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            result.data.as_ref().unwrap()["AgentSession"][0]["requester_did"],
            "did:test:grok-shim"
        );
        let repeated = service
            .handle_acp_payload(&request_payload(
                "session/load",
                json!({"sessionId":"resume-history"}),
            ))
            .await;
        assert_eq!(
            parse_response(repeated.response.as_deref().unwrap())["error"]["code"],
            -32602
        );
        service.turns.handle_disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn history_picker_scopes_searches_paginates_and_preserves_legacy_sessions() {
        let (_dir, service) = test_service().await;
        for (id, requester) in [
            ("older", "null"),
            ("newer", "\"did:test:grok-shim\""),
            ("foreign-session", "\"did:test:foreign\""),
            ("child", "\"did:test:grok-shim\""),
        ] {
            let result = service
                .config
                .node
                .execute(&format!(
                    r#"mutation {{ create_AgentSession(input: {{
                session_id: "{id}", agent_did: "did:test:grok-shim", requester_did: {requester},
                behavior_id: "did:test:grok-shim:default", status: "active"
            }}) {{ _docID }} }}"#
                ))
                .await;
            ensure_no_errors(&result, "seed history owner").unwrap();
        }
        for (id, session, requester, content, day, parent) in [
            (
                "a",
                "older",
                "did:test:grok-shim",
                "First older prompt",
                1,
                "null",
            ),
            (
                "b",
                "newer",
                "did:test:grok-shim",
                "First newer prompt",
                2,
                "null",
            ),
            (
                "c",
                "foreign-session",
                "did:test:grok-shim",
                "SECRET session",
                4,
                "null",
            ),
            (
                "d",
                "older",
                "did:test:foreign",
                "SECRET request",
                5,
                "null",
            ),
            (
                "e",
                "child",
                "did:test:grok-shim",
                "Child instructions",
                6,
                "\"a\"",
            ),
            (
                "f",
                "older",
                "did:test:grok-shim",
                "Later searchable NEEDLE",
                3,
                "null",
            ),
        ] {
            let result = service
                .config
                .node
                .execute(&format!(
                    r#"mutation {{ create_AgentRequest(input: {{
                request_id: "{id}", session_id: "{session}", agent_did: "did:test:grok-shim",
                requester_did: "{requester}", content: "{content}", lifecycle_state: "completed",
                caused_by_parent_request_id: {parent}, created_at: "2026-09-0{day}T12:00:00Z"
            }}) {{ _docID }} }}"#
                ))
                .await;
            ensure_no_errors(&result, "seed history request").unwrap();
        }
        let dispatch = service
            .handle_acp_payload(&request_payload(
                "_x.ai/session/list",
                json!({"limit":1, "cwd":"/not-historical"}),
            ))
            .await;
        let first = parse_response(dispatch.response.as_deref().unwrap());
        assert!(first.get("error").is_none(), "{first}");
        let page = &first["result"];
        assert_eq!(page["sessions"][0]["sessionId"], "older");
        assert_eq!(page["sessions"][0]["summary"], "First older prompt");
        assert!(page["sessions"][0].get("cwd").is_none());
        assert!(page["sessions"][0].get("modelId").is_none());
        assert_eq!(page["_meta"]["x.ai/listScope"], "all");
        let dispatch = service
            .handle_acp_payload(&request_payload(
                "x.ai/session/list",
                json!({"limit":1, "cursor":page["nextCursor"]}),
            ))
            .await;
        let second = parse_response(dispatch.response.as_deref().unwrap());
        assert_eq!(second["result"]["sessions"][0]["sessionId"], "newer");
        assert!(second["result"]["nextCursor"].is_null());
        for (query, count) in [("needle", 1), ("secret", 0)] {
            let dispatch = service
                .handle_acp_payload(&request_payload(
                    "x.ai/session/list",
                    json!({"query":query}),
                ))
                .await;
            let response = parse_response(dispatch.response.as_deref().unwrap());
            assert_eq!(
                response["result"]["sessions"].as_array().unwrap().len(),
                count
            );
        }
        let rows = super::super::sessions::load(
            &service.config.node,
            &service.config.agent_did,
            &service.config.behavior_id,
            "older",
        )
        .await
        .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.request_id.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "f"]
        );
        assert!(super::super::sessions::load(
            &service.config.node,
            &service.config.agent_did,
            &service.config.behavior_id,
            "foreign-session"
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn native_task_kill_is_scoped_idempotent_and_uses_stock_envelope() {
        let (_dir, service) = test_service().await;
        service
            .sessions
            .lock()
            .await
            .insert("task-session".into(), AcpSessionState::new());
        let mut lifecycle = gents::tool_call_lifecycle::ToolCallLifecycle::new_background_tool(
            service.config.node.clone(),
            "task-owner".into(),
            "task-session".into(),
            service.config.agent_did.to_string(),
            "task-button".into(),
            1,
            "bash".into(),
            "{}".into(),
            chrono::Utc::now() + chrono::Duration::minutes(5),
        )
        .with_requester_did(Some(service.config.agent_did.to_string()));
        lifecycle.start_running().await.unwrap();
        for params in [
            json!({"sessionId":"task-session"}),
            json!({"sessionId":"task-session","taskId":""}),
            json!({"sessionId":"task-session","taskId":"task-button","source":"invalid"}),
        ] {
            let response = service
                .handle_acp_payload(&request_payload("x.ai/task/kill", params))
                .await;
            assert_eq!(
                parse_response(response.response.as_deref().unwrap())["error"]["code"],
                -32602
            );
        }
        for (session, expected) in [
            ("foreign", "not_found"),
            ("task-session", "killed"),
            ("task-session", "already_exited"),
        ] {
            let response = service
                .handle_acp_payload(&request_payload(
                    "x.ai/task/kill",
                    json!({"sessionId":session, "taskId":"task-button", "source":"clientUi"}),
                ))
                .await;
            let value = parse_response(response.response.as_deref().unwrap());
            assert_eq!(
                value["result"]["result"],
                json!({"taskId":"task-button","outcome":expected}),
                "{value}"
            );
        }
        let response = service.config.node.execute(r#"{ AgentToolCall(filter: {tool_call_id: {_eq: "task-button"}}) {lifecycle_state} }"#).await;
        assert_eq!(
            response.data.unwrap()["AgentToolCall"][0]["lifecycle_state"],
            "cancelled"
        );
    }

    #[tokio::test]
    async fn native_child_controls_and_usage_follow_physical_lineage() {
        let (_dir, service) = test_service().await;
        service
            .sessions
            .lock()
            .await
            .insert("parent-session".into(), AcpSessionState::new());
        let node = &service.config.node;
        let did = gents::graphql::escape_graphql_string(&service.config.agent_did);
        let root = node.execute(&format!(r#"mutation {{ create_AgentRequest(input: {{
            request_id: "parent", session_id: "parent-session", agent_did: "{did}", requester_did: "{did}", lifecycle_state: "completed"
        }}) {{_docID}} }}"#)).await;
        ensure_no_errors(&root, "root fixture").unwrap();
        let root = node
            .execute(r#"{ AgentRequest(filter: {request_id: {_eq: "parent"}}) {_docID} }"#)
            .await;
        ensure_no_errors(&root, "root fixture lookup").unwrap();
        let root_doc = root.data.unwrap()["AgentRequest"][0]["_docID"]
            .as_str()
            .unwrap()
            .to_owned();
        let root_doc = gents::graphql::escape_graphql_string(&root_doc);
        let bridge = node.execute(&format!(r#"mutation {{ create_AgentToolCall(input: {{
            tool_call_key: "parent-session:spawn", tool_call_id: "spawn", request_id: "parent", request_doc_id: "{root_doc}",
            session_id: "parent-session", agent_did: "{did}", requester_did: "{did}", tool_name: "spawn_subagent",
            child_request_id: "child", await_mode: "background", lifecycle_state: "running"
        }}) {{_docID}} }}"#)).await;
        ensure_no_errors(&bridge, "bridge fixture").unwrap();
        let bridge = node
            .execute(r#"{ AgentToolCall(filter: {tool_call_id: {_eq: "spawn"}}) {_docID} }"#)
            .await;
        ensure_no_errors(&bridge, "bridge fixture lookup").unwrap();
        let bridge_doc = bridge.data.unwrap()["AgentToolCall"][0]["_docID"]
            .as_str()
            .unwrap()
            .to_owned();
        let bridge_doc = gents::graphql::escape_graphql_string(&bridge_doc);
        let child = node.execute(&format!(r#"mutation {{ create_AgentRequest(input: {{
            request_id: "child", session_id: "child-session", agent_did: "{did}", lifecycle_state: "processing",
            caused_by_parent_request_id: "parent", caused_by_parent_request_doc_id: "{root_doc}",
            caused_by_parent_tool_call_id: "spawn", caused_by_parent_tool_call_doc_id: "{bridge_doc}"
        }}) {{_docID}} }}"#)).await;
        ensure_no_errors(&child, "child fixture").unwrap();
        for session in ["parent-session", "child-session"] {
            let mut lifecycle = gents::tool_call_lifecycle::ToolCallLifecycle::new_background_tool(
                node.clone(),
                "child".into(),
                session.into(),
                service.config.agent_did.to_string(),
                "ambiguous-process".into(),
                2,
                "bash".into(),
                "{}".into(),
                chrono::Utc::now() + chrono::Duration::minutes(5),
            );
            lifecycle.start_running().await.unwrap();
        }
        let denied = service
            .handle_acp_payload(&request_payload(
                "x.ai/task/kill",
                json!({"sessionId":"parent-session", "taskId":"ambiguous-process"}),
            ))
            .await;
        assert_eq!(
            parse_response(denied.response.as_deref().unwrap())["result"]["result"]["outcome"],
            "not_found"
        );
        let unchanged = node.execute(r#"{ AgentToolCall(filter: {tool_call_id: {_eq: "ambiguous-process"}}) {lifecycle_state} }"#).await;
        assert!(unchanged.data.unwrap()["AgentToolCall"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["lifecycle_state"] == "running"));
        for session in ["child-session", "unlinked-session"] {
            let mut lifecycle = gents::tool_call_lifecycle::ToolCallLifecycle::new_background_tool(
                node.clone(),
                "child".into(),
                session.into(),
                service.config.agent_did.to_string(),
                "child-process".into(),
                1,
                "bash".into(),
                "{}".into(),
                chrono::Utc::now() + chrono::Duration::minutes(5),
            );
            lifecycle.start_running().await.unwrap();
        }
        for (session, outcome) in [
            ("unlinked-session", "not_found"),
            ("parent-session", "killed"),
            ("child-session", "already_exited"),
        ] {
            let response = service
                .handle_acp_payload(&request_payload(
                    "x.ai/task/kill",
                    json!({
                        "sessionId": session, "taskId": "child-process", "source": "teardown"
                    }),
                ))
                .await;
            let response = parse_response(response.response.as_deref().unwrap());
            assert_eq!(
                response["result"]["result"]["outcome"], outcome,
                "{response}"
            );
        }
        ensure_session_document(&service.config, "parent-session")
            .await
            .unwrap();
        let foreign = node.execute(&format!(r#"mutation {{ create_AgentRequest(input: {{
            request_id: "foreign-usage", session_id: "child-session", agent_did: "{did}", requester_did: "did:test:foreign", lifecycle_state: "completed"
        }}) {{_docID}} }}"#)).await;
        ensure_no_errors(&foreign, "foreign usage fixture").unwrap();
        for (request, input, output, cached) in [
            ("parent", 100, 10, 50),
            ("child", 200, 20, 100),
            ("foreign-usage", 9999, 999, 0),
        ] {
            let owner = node
                .execute(&format!(
                    r#"{{ AgentRequest(filter: {{request_id: {{_eq: "{request}"}}}}) {{_docID}} }}"#
                ))
                .await;
            ensure_no_errors(&owner, "usage physical identity").unwrap();
            let doc = owner.data.as_ref().unwrap()["AgentRequest"][0]["_docID"]
                .as_str()
                .unwrap();
            let call = node.execute(&format!(r#"mutation {{ create_InferenceCall(input: {{
                call_id: "usage-{request}", request_id: "parent", request_doc_id: "{}", agent_did: "{did}",
                call_seq: 1, call_kind: "inference", prompt_tokens: {input}, completion_tokens: {output}, cached_input_tokens: {cached}
            }}) {{_docID}} }}"#, escape_graphql_string(doc))).await;
            ensure_no_errors(&call, "usage call fixture").unwrap();
        }
        let usage = service
            .handle_acp_payload(&request_payload(
                "x.ai/session/usage",
                json!({"sessionId":"parent-session"}),
            ))
            .await;
        let response = parse_response(usage.response.as_deref().unwrap());
        assert!(response.get("error").is_none(), "{response}");
        assert_eq!(response["result"]["usage"]["inputTokens"], 300);
        assert_eq!(response["result"]["usage"]["outputTokens"], 30);
        assert_eq!(response["result"]["usage"]["cachedReadTokens"], 150);
        assert_eq!(response["result"]["usage"]["modelCalls"], 2);
        assert_eq!(response["result"]["usage"]["usageIsIncomplete"], true);
        assert!(response["result"]["usage"].get("costUsdTicks").is_none());
    }

    fn request_payload(method: &str, params: Value) -> String {
        serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": wire_method(method),
            "params": params,
        }))
        .expect("request payload")
    }

    fn notification_payload(method: &str, params: Value) -> String {
        serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "method": wire_method(method),
            "params": params,
        }))
        .expect("notification payload")
    }

    fn parse_response(line: &str) -> Value {
        serde_json::from_str(line).expect("response line is JSON")
    }

    /// Build a service whose `create_agent_request` seam points at a live
    /// mock GraphQL endpoint forwarding to the embedded node.
    async fn test_service_with_graphql(node: Arc<EmbeddedNode>, graphql: String) -> AcpService {
        let identity_dir = tempfile::tempdir().expect("identity tempdir");
        let identity =
            gents::KeyIdentity::load_or_create(identity_dir.path().join("agent.key"), None)
                .expect("test signing identity");
        let agent_did = gents::AgentIdentity::did(&identity).to_string();
        let behavior_id = gents::default_behavior_id_for_agent(&agent_did);
        let config = AcpServiceConfig {
            node,
            agent_did: Arc::from(agent_did.as_str()),
            agent_name: Arc::from("grok-shim-test"),
            behavior_id: Arc::from(behavior_id.as_str()),
            current_model: bound_model(),
        };
        gents::schema::ensure_runtime_schemas(config.node.as_ref())
            .await
            .expect("runtime schemas");
        let response = config
            .node
            .execute(&format!(
                r#"mutation {{
                    create_AgentPrincipal(input: {{
                        agent_did: "{agent_did}"
                        display_name: "Grok shim test"
                        default_behavior_id: "{behavior_id}"
                        enabled: true
                    }}) {{ _docID }}
                    create_AgentBehavior(input: {{
                        behavior_id: "{behavior_id}"
                        agent_did: "{agent_did}"
                        display_name: "Grok shim test"
                        enabled: true
                    }}) {{ _docID }}
                }}"#,
            ))
            .await;
        gents::graphql::ensure_no_errors(&response, "seed admitted test behavior")
            .expect("seed admitted test behavior");
        let turns = Arc::new(TurnManager::new(
            config.node.clone(),
            super::super::turn::TurnManagerConfig {
                agent_did: config.agent_did.to_string(),
                behavior_id: config.behavior_id.to_string(),
                graphql,
            },
        ));
        let projections = Arc::new(ProjectionEngine::new(
            config.node.clone(),
            super::super::projection::BoundModelContext::new(
                config.current_model.model_id.clone(),
                config.current_model.name.clone(),
                config.current_model.total_context_tokens,
            ),
        ));
        AcpService::new(config, turns, projections)
    }

    /// A mock GraphQL endpoint forwarding every query to the embedded node,
    /// so `create_agent_request` writes real durable rows.
    async fn spawn_mock_graphql(node: Arc<EmbeddedNode>) -> String {
        async fn handler(
            axum::extract::State(node): axum::extract::State<Arc<EmbeddedNode>>,
            axum::Json(body): axum::Json<Value>,
        ) -> axum::Json<Value> {
            let query = body
                .get("query")
                .and_then(Value::as_str)
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

    /// The live delegate path (`AcpDelegate::handle_acp` over a real
    /// `AcpOutbound`): the user echo and every novel durable projection
    /// update stream through the outbound *before* the deferred
    /// `session/prompt` response, in that wire order.
    #[tokio::test]
    async fn delegate_streams_updates_live_before_the_response() {
        let (_staging, config) = config().await;
        gents::schema::ensure_runtime_schemas(config.node.as_ref())
            .await
            .expect("runtime schemas");
        let graphql = spawn_mock_graphql(config.node.clone()).await;
        let node = config.node.clone();
        let service = test_service_with_graphql(node, graphql).await;

        // Create the session first so the prompt's session is known.
        service
            .handle_acp_payload(&request_payload(
                "session/new",
                json!({ "cwd": "/tmp", "mcpServers": [], "_meta": { "sessionId": "s-live" } }),
            ))
            .await;

        let payload = request_payload(
            "session/prompt",
            json!({
                "sessionId": "s-live",
                "prompt": [{"type": "text", "text": "hello"}],
                "_meta": {"promptId": "prompt-live"},
            }),
        );

        // The live outbound channel the delegate sends through.
        let (frames_tx, mut frames_rx) = tokio::sync::mpsc::unbounded_channel::<
            crate::commands::grok_shim::protocol::ServerEnvelope,
        >();
        let outbound = AcpOutbound::for_frames(frames_tx);

        // Materialize the turn's durable output while the delegate runs:
        // an assistant row, then terminalization.
        let node_for_seed = config.node.clone();
        let seed_handle = tokio::spawn(async move {
            loop {
                let query = r#"{ AgentRequest(filter: { lifecycle_state: { _eq: "pending" } }) { request_id } }"#;
                let response = node_for_seed.execute(query).await;
                let rows = response
                    .data
                    .as_ref()
                    .and_then(|data| data.get("AgentRequest"))
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                if let Some(row) = rows.first() {
                    let request_id = row
                        .get("request_id")
                        .and_then(Value::as_str)
                        .unwrap()
                        .to_string();
                    let message = serde_json::to_string(
                        &gents_protocol::message::Message::assistant("live answer"),
                    )
                    .expect("serialize assistant message");
                    let escaped = gents::graphql::escape_graphql_string(&message);
                    let escaped_request = gents::graphql::escape_graphql_string(&request_id);
                    let mutation = format!(
                        r#"mutation {{
                            create_AgentMessage(input: {{
                                message_key: "{escaped_request}:1"
                                session_id: "s-live"
                                agent_did: "did:test:grok-shim"
                                requester_did: "did:test:grok-shim"
                                request_id: "{escaped_request}"
                                sequence: 1
                                role: "assistant"
                                content: "{escaped}"
                            }}) {{ _docID }}
                        }}"#
                    );
                    let response = node_for_seed.execute(&mutation).await;
                    gents::graphql::ensure_no_errors(&response, "seed message")
                        .expect("seed message");
                    // Terminalize with a completed response row.
                    let now = chrono::Utc::now().to_rfc3339();
                    let mutation = format!(
                        r#"mutation {{
                            update_AgentRequest(
                                filter: {{ request_id: {{ _eq: "{escaped_request}" }} }},
                                input: {{ lifecycle_state: "completed" }}
                            ) {{ _docID }}
                            create_AgentResponse(input: {{
                                response_key: "{escaped_request}"
                                request_id: "{escaped_request}"
                                agent_did: "did:test:grok-shim"
                                behavior_id: "did:test:grok-shim:default"
                                session_id: "s-live"
                                content: ""
                                reasoning: ""
                                status: "complete"
                                error_message: ""
                                token_count: 0
                                progress_seq: 0
                                created_at: "{now}"
                                completed_at: "{now}"
                            }}) {{ _docID }}
                        }}"#
                    );
                    let response = node_for_seed.execute(&mutation).await;
                    gents::graphql::ensure_no_errors(&response, "terminalize")
                        .expect("terminalize");
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        });

        // Drive the delegate to completion: it sends the echo and every
        // novel projection update live, then the response.
        let service = Arc::new(service);
        let delegate_service = service.clone();
        let handle = tokio::spawn(async move {
            delegate_service
                .handle_acp(&payload, outbound)
                .await
                .expect("delegate should succeed")
        });

        // Collect the outbound frames until the response arrives.
        let mut frames: Vec<Value> = Vec::new();
        let response_seen = loop {
            let frame = tokio::time::timeout(std::time::Duration::from_secs(30), frames_rx.recv())
                .await
                .expect("frame should arrive within timeout")
                .expect("outbound stays open until the delegate resolves");
            let crate::commands::grok_shim::protocol::ServerEnvelope::Acp { payload } = frame
            else {
                continue;
            };
            let value: Value = serde_json::from_str(&payload).expect("acp payload is JSON");
            if value.get("id").is_some() {
                frames.push(value);
                break true;
            }
            frames.push(value);
        };
        assert!(response_seen, "the deferred response must arrive");
        handle.await.expect("delegate task");
        seed_handle.await.expect("seed task");

        // Wire order: the user echo streams first, then the assistant
        // message chunk, and the response is last.
        let kinds: Vec<String> = frames
            .iter()
            .filter_map(|frame| {
                frame["params"]["update"]["sessionUpdate"]
                    .as_str()
                    .map(ToOwned::to_owned)
            })
            .collect();
        let echo_index = kinds
            .iter()
            .position(|kind| kind == "user_message_chunk")
            .expect("the user echo must stream live");
        let answer_index = kinds
            .iter()
            .position(|kind| kind == "agent_message_chunk")
            .expect("the assistant answer must stream live");
        assert!(
            echo_index < answer_index,
            "the echo must precede the answer, got {kinds:?}"
        );
        // The response is the final frame.
        let last = frames.last().expect("response frame");
        assert_eq!(last["id"], 1, "the deferred response closes the stream");
        assert_eq!(last["result"]["stopReason"], "end_turn");
        // Every streamed notification carries the turn's promptId.
        for frame in &frames[..frames.len() - 1] {
            assert_eq!(frame["params"]["_meta"]["promptId"], "prompt-live");
        }
    }

    // -- Pure shape tests (no node, no siblings) -----------------------------

    #[test]
    fn models_object_is_nested_and_complete() {
        let models = bound_model().models_object();
        // THE audited shape: nested under "models", not spliced to top level.
        assert_eq!(
            models["currentModelId"], "GLM-5.3-NVFP4",
            "currentModelId must live inside the models object"
        );
        let available = models["availableModels"]
            .as_array()
            .expect("availableModels is an array");
        assert_eq!(available.len(), 1);
        assert_eq!(available[0]["modelId"], "GLM-5.3-NVFP4");
        assert_eq!(available[0]["meta"]["totalContextTokens"], 262_144);
        // Truthful capabilities: the shim's prompt parser is text-only and
        // a selected reasoning effort never reaches SubmitRequestOptions,
        // so the catalog must not advertise either.
        assert_eq!(
            available[0]["meta"]["acceptsImages"], false,
            "images are not natively plumbed; advertising them would be false"
        );
        assert_eq!(
            available[0]["meta"]["inputModalities"],
            json!(["text"]),
            "the catalog is text-only"
        );
        assert_eq!(
            available[0]["meta"]["supportsReasoningEffort"], false,
            "reasoning effort is not plumbed to provider inference"
        );
        // Absence of the effort list is the pager's contract for
        // "unsupported" — the list must not be advertised at all.
        assert!(
            available[0]["meta"].get("reasoningEfforts").is_none(),
            "no effort list is advertised while the feature is unsupported"
        );
    }

    #[test]
    fn model_catalog_normalizes_an_unspecified_context_window() {
        let mut model = bound_model();
        model.total_context_tokens = 0;
        assert_eq!(
            model.models_object()["availableModels"][0]["meta"]["totalContextTokens"],
            super::super::projection::DEFAULT_CONTEXT_WINDOW_TOKENS
        );
        assert_eq!(
            model.effective_context_window(),
            super::super::projection::DEFAULT_CONTEXT_WINDOW_TOKENS
        );
    }

    #[test]
    fn catalog_entry_has_no_leading_underscore_keys() {
        let entry = bound_model().catalog_entry();
        assert!(entry.get("meta").is_some());
        assert_eq!(entry["modelId"], "GLM-5.3-NVFP4");
        // The display name falls back to the raw id when absent; ours is set.
        assert_eq!(entry["name"], "GLM 5.3 NVFP4");
    }

    #[test]
    fn acp_request_parses_request_and_notification() {
        let request =
            AcpRequest::from_payload(&request_payload("initialize", json!({}))).expect("parse");
        assert_eq!(request.id, Some(json!(1)));
        assert_eq!(request.method, "initialize");
        assert!(!request.is_notification());

        let notification = AcpRequest::from_payload(&notification_payload(
            "session/cancel",
            json!({ "sessionId": "s1" }),
        ))
        .expect("parse");
        assert_eq!(notification.id, None);
        assert!(notification.is_notification());
        assert_eq!(notification.params["sessionId"], "s1");
    }

    #[test]
    fn acp_request_rejects_malformed_payloads() {
        assert!(AcpRequest::from_payload("not json").is_err());
        assert!(AcpRequest::from_payload("[]").is_err());
        assert!(AcpRequest::from_payload("{}").is_err());
        assert!(AcpRequest::from_payload("{\"id\":1}").is_err());
        // A null id is a notification, matching the pager's decoder.
        let null_id =
            AcpRequest::from_payload("{\"jsonrpc\":\"2.0\",\"id\":null,\"method\":\"x\"}")
                .expect("parse");
        assert!(null_id.is_notification());
    }

    #[test]
    fn extension_methods_use_sdk_wire_prefix_and_normalize_at_dispatch() {
        for method in [
            MCP_INITIALIZED_METHOD,
            MODELS_UPDATE_METHOD,
            "x.ai/session_notification",
            SUBAGENT_GET_METHOD,
        ] {
            let encoded = wire_method(method);
            assert_eq!(encoded, format!("_{method}"));
            assert_eq!(
                wire_method(&encoded),
                encoded,
                "encoding must not double-prefix"
            );
            let request = AcpRequest::from_payload(
                &json!({
                    "jsonrpc": "2.0", "id": 1, "method": encoded, "params": {},
                })
                .to_string(),
            )
            .unwrap();
            assert_eq!(request.method, method);
        }
        for method in [
            SESSION_PROMPT_METHOD,
            "_unknown/extension",
            "__x.ai/subagent/get",
        ] {
            assert_eq!(wire_method(method), method);
            let request = AcpRequest::from_payload(
                &json!({
                    "jsonrpc": "2.0", "id": 1, "method": method,
                })
                .to_string(),
            )
            .unwrap();
            assert_eq!(
                request.method, method,
                "only the exact _x.ai/ namespace is normalized"
            );
        }
    }

    #[test]
    fn response_and_error_envelopes_are_shaped() {
        let response = parse_response(&response_line(&json!(7), &json!({"ok": true})));
        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], 7);
        assert_eq!(response["result"]["ok"], true);

        let error = parse_response(&error_line(&json!(7), -32601, "nope"));
        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(error["id"], 7);
        assert_eq!(error["error"]["code"], -32601);
        assert_eq!(error["error"]["message"], "nope");

        let notification = parse_response(&notification_line("x.ai/models/update", &json!({})));
        assert_eq!(notification["jsonrpc"], "2.0");
        assert_eq!(notification["method"], "_x.ai/models/update");
        assert!(notification.get("id").is_none());
    }

    #[test]
    fn shaped_stub_messages_name_the_owned_transition() {
        let interject = shaped_stub_error(
            INTERJECT_METHOD,
            &json!({ "sessionId": "s1", "text": "hi", "interjectionId": "i1" }),
        );
        let message = interject.to_string();
        assert!(
            message.contains("no formally specified injection transition"),
            "interject stub must explain the formal-transition gap: {message}"
        );
        assert!(
            message.contains("No AgentMessage or AgentRequest document was fabricated"),
            "interject stub must state that nothing was fabricated: {message}"
        );

        let compact = shaped_stub_error(COMPACT_CONVERSATION_METHOD, &json!({ "sessionId": "s1" }));
        let message = compact.to_string();
        assert!(
            message.contains("tokens_before") && message.contains("tokens_after"),
            "compact stub must name the invented fields it refuses: {message}"
        );
        assert!(
            message.contains("no usage counters"),
            "compact stub must name the missing AgentSession counters: {message}"
        );
    }

    #[test]
    fn shaped_stub_maps_to_method_not_found_code() {
        let error = shaped_stub_error(SESSION_LOAD_METHOD, &json!({ "sessionId": "s1" }));
        assert_eq!(error_code_for(&error), -32601);
        let unknown = anyhow::Error::new(ShapedMethodNotFound {
            message: "unsupported".to_string(),
        });
        assert_eq!(error_code_for(&unknown), -32601);
    }

    #[test]
    fn internal_and_param_errors_map_to_their_codes() {
        let missing_session = required_session_id(&json!({})).expect_err("missing session id");
        assert_eq!(error_code_for(&missing_session), -32602);
        let internal = anyhow::anyhow!("boom");
        assert_eq!(error_code_for(&internal), -32603);
        // Classification is type-driven: an internal error whose message
        // happens to mention sessionId must still be internal, never
        // reclassified by string matching.
        let internal_looking =
            anyhow::anyhow!("storage failed while reading session requires a non-empty sessionId");
        assert_eq!(error_code_for(&internal_looking), -32603);
        let typed = invalid_params("any message");
        assert_eq!(error_code_for(&typed), -32602);
        // The wrapped turn.rs parser error classifies as invalid params while
        // preserving the source message.
        let parser_error =
            super::super::turn::parse_prompt_request(&json!({}), None).expect_err("missing params");
        assert!(
            parser_error.to_string().contains("session/prompt"),
            "the source parser message is preserved: {parser_error}"
        );
        let wrapped = invalid_params(parser_error.to_string());
        assert_eq!(error_code_for(&wrapped), -32602);
        assert_eq!(
            wrapped.to_string(),
            parser_error.to_string(),
            "the wrapped error message matches the source verbatim"
        );
    }

    #[test]
    fn required_session_id_rejects_missing_and_empty() {
        assert!(required_session_id(&json!({})).is_err());
        assert!(required_session_id(&json!({ "sessionId": "" })).is_err());
        assert!(required_session_id(&json!({ "sessionId": "  " })).is_err());
        assert_eq!(
            required_session_id(&json!({ "sessionId": " s1 " })).expect("session id"),
            "s1"
        );
    }

    #[test]
    fn optional_session_id_tolerates_absence() {
        assert_eq!(optional_session_id(&json!({})), None);
        assert_eq!(optional_session_id(&json!({ "sessionId": "" })), None);
        assert_eq!(
            optional_session_id(&json!({ "sessionId": "s1" })),
            Some("s1".to_string())
        );
    }

    // -- Service tests (node-backed) ----------------------------------------

    #[tokio::test]
    async fn initialize_advertises_persisted_session_loading_and_gents_auth() {
        let (_staging, service) = test_service().await;
        let dispatch = service
            .handle_acp_payload(&request_payload(
                "initialize",
                json!({ "protocolVersion": 1, "clientInfo": { "name": "probe" } }),
            ))
            .await;
        assert!(dispatch.notifications.is_empty());
        let response = parse_response(dispatch.response.as_deref().expect("response line"));
        assert_eq!(response["id"], 1);
        assert_eq!(response["result"]["agentCapabilities"]["loadSession"], true);
        assert_eq!(response["result"]["authMethods"][0]["id"], "gents.runtime");
        assert_eq!(
            response["result"]["authMethods"][0]["name"],
            "Gents runtime"
        );
    }

    #[tokio::test]
    async fn local_billing_has_no_cloud_balance_or_subscription() {
        let (_staging, service) = test_service().await;
        let dispatch = service
            .handle_acp_payload(&request_payload("x.ai/billing", json!({})))
            .await;
        assert!(dispatch.notifications.is_empty());
        let response = parse_response(dispatch.response.as_deref().expect("response line"));
        assert!(response.get("error").is_none(), "{response}");
        assert_eq!(
            response["result"]["result"],
            json!({"config":null,"on_demand_enabled":false})
        );
    }

    #[tokio::test]
    async fn authenticate_accepts_gents_runtime_and_rejects_others() {
        let (_staging, service) = test_service().await;
        let ok = service
            .handle_acp_payload(&request_payload(
                "authenticate",
                json!({ "methodId": "gents.runtime" }),
            ))
            .await;
        let response = parse_response(ok.response.as_deref().expect("response line"));
        assert_eq!(response["result"]["_meta"]["provider"], "gents");

        let rejected = service
            .handle_acp_payload(&request_payload(
                "authenticate",
                json!({ "methodId": "oauth" }),
            ))
            .await;
        let response = parse_response(rejected.response.as_deref().expect("response line"));
        assert_eq!(
            response["error"]["code"], -32602,
            "a caller-supplied auth method outside the advertised list is invalid params"
        );
        assert!(
            response["error"]["message"]
                .as_str()
                .expect("message")
                .contains("gents.runtime"),
            "rejection must name the only supported method"
        );
    }

    #[tokio::test]
    async fn session_new_honors_preferred_id_and_nests_models() {
        let (_staging, service) = test_service().await;
        let dispatch = service
            .handle_acp_payload(&request_payload(
                "session/new",
                json!({
                    "cwd": "/tmp",
                    "mcpServers": [],
                    "_meta": {
                        "sessionId": "grok-edge-preferred",
                        "modelId": "GLM-5.3-NVFP4",
                        "yoloMode": true,
                        "autoMode": false,
                    },
                }),
            ))
            .await;
        assert_eq!(dispatch.notifications.len(), 1);
        let mcp_initialized = parse_response(&dispatch.notifications[0]);
        assert_eq!(
            mcp_initialized,
            json!({
                "jsonrpc": "2.0",
                "method": "_x.ai/mcp_initialized",
                "params": {
                    "sessionId": "grok-edge-preferred",
                    "mcpToolCount": 0,
                    "elapsedMs": 0,
                },
            }),
            "session/new must emit the stock pager's exact MCP completion envelope"
        );
        let response = parse_response(dispatch.response.as_deref().expect("response line"));
        let result = &response["result"];
        assert_eq!(result["sessionId"], "grok-edge-preferred");
        // THE audited read path: result["models"]["currentModelId"].
        assert_eq!(
            result["models"]["currentModelId"], "GLM-5.3-NVFP4",
            "session/new must return models.currentModelId, not a top-level key"
        );
        assert_eq!(
            result["models"]["availableModels"][0]["modelId"],
            "GLM-5.3-NVFP4"
        );
        assert_eq!(
            result["models"]["availableModels"][0]["meta"]["totalContextTokens"],
            262_144
        );
        // And must NOT leak the catalog keys to the top level.
        assert!(result.get("availableModels").is_none());
        assert!(result.get("currentModelId").is_none());
        assert_eq!(result["_meta"]["yoloMode"], true);
        assert_eq!(result["_meta"]["autoMode"], false);
        assert_eq!(result["_meta"]["modelId"], "GLM-5.3-NVFP4");
    }

    /// Production registration-to-session wiring: a service whose delegate
    /// was constructed by the production factory (which stores the
    /// registration capabilities through `register_client_identity`) derives
    /// `yoloMode`/`autoMode`/`clientTerminal` for `session/new` from the
    /// *registered* capabilities — with no session `_meta` carrying them.
    /// This is the exact production path: `bind_grok_shim` →
    /// `production_acp_delegate_factory` → `register_client_identity` →
    /// `session/new`.
    #[tokio::test]
    async fn session_new_derives_mode_capabilities_from_registration() {
        let (_staging, mut service) = test_service().await;
        // Exactly what the production factory does after constructing the
        // service: store the registering client's identity and capabilities.
        service.register_client_identity(
            7,
            &crate::commands::grok_shim::server::Registration {
                client_type: "grok-pager".to_string(),
                mode: crate::commands::grok_shim::protocol::RegisterMode::Stdio,
                capabilities: crate::commands::grok_shim::protocol::ClientCapabilities {
                    yolo_mode: true,
                    auto_mode: false,
                    terminal: false,
                    ..Default::default()
                },
            },
        );

        // The session/new request carries NO yoloMode/autoMode/clientTerminal
        // keys in its `_meta` — the injection must come from registration.
        let dispatch = service
            .handle_acp_payload(&request_payload(
                "session/new",
                json!({
                    "cwd": "/tmp",
                    "mcpServers": [],
                    "_meta": { "sessionId": "grok-registered-session" },
                }),
            ))
            .await;
        let response = parse_response(dispatch.response.as_deref().expect("response line"));
        let result = &response["result"];
        assert_eq!(result["sessionId"], "grok-registered-session");
        assert_eq!(
            result["_meta"]["yoloMode"], true,
            "yoloMode must come from the registered capability, not the request"
        );
        assert_eq!(result["_meta"]["autoMode"], false);
        assert_eq!(
            result["_meta"]["clientTerminal"], false,
            "the reference pager registers terminal=false; the key must be present"
        );
    }

    /// An explicit per-request `_meta` override wins over the registered
    /// capability (the pager's `apply_permission_mode_override` stamps the
    /// same keys), and `clientTerminal` rides the registered `terminal`
    /// capability when the request does not carry it.
    #[tokio::test]
    async fn session_new_meta_overrides_registration_but_defaults_from_it() {
        let (_staging, mut service) = test_service().await;
        service.register_client_identity(
            9,
            &crate::commands::grok_shim::server::Registration {
                client_type: "grok-pager".to_string(),
                mode: crate::commands::grok_shim::protocol::RegisterMode::Stdio,
                capabilities: crate::commands::grok_shim::protocol::ClientCapabilities {
                    yolo_mode: false,
                    auto_mode: true,
                    terminal: true,
                    ..Default::default()
                },
            },
        );
        let dispatch = service
            .handle_acp_payload(&request_payload(
                "session/new",
                json!({
                    "cwd": "/tmp",
                    "_meta": {
                        "sessionId": "grok-override-session",
                        "yoloMode": true,
                    },
                }),
            ))
            .await;
        let response = parse_response(dispatch.response.as_deref().expect("response line"));
        let result = &response["result"];
        assert_eq!(
            result["_meta"]["yoloMode"], true,
            "the explicit request override must win"
        );
        assert_eq!(
            result["_meta"]["autoMode"], true,
            "an absent request key falls back to the registered capability"
        );
        assert_eq!(
            result["_meta"]["clientTerminal"], true,
            "clientTerminal must reflect the registered terminal capability"
        );
    }

    #[tokio::test]
    async fn session_new_creates_exactly_one_session_and_zero_requests() {
        let (_staging, service) = test_service().await;
        service
            .handle_acp_payload(&request_payload(
                "session/new",
                json!({
                    "cwd": "/tmp",
                    "mcpServers": [],
                    "_meta": { "sessionId": "grok-edge-docs" },
                }),
            ))
            .await;

        let node = service.config.node.clone();
        let query = r#"{
            AgentSession(filter: { session_id: { _eq: "grok-edge-docs" } }) {
                session_id behavior_id agent_did status
            }
            AgentRequest(filter: { session_id: { _eq: "grok-edge-docs" } }) { request_id }
        }"#
        .to_string();
        let response = node.execute(&query).await;
        ensure_no_errors(&response, "session document check").expect("query ok");
        let sessions = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentSession"))
            .and_then(Value::as_array)
            .expect("AgentSession array");
        assert_eq!(sessions.len(), 1, "exactly one AgentSession document");
        assert_eq!(sessions[0]["behavior_id"], "did:test:grok-shim:default");
        assert_eq!(sessions[0]["agent_did"], "did:test:grok-shim");
        assert_eq!(sessions[0]["status"], "active");
        let requests = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(Value::as_array)
            .expect("AgentRequest array");
        assert!(
            requests.is_empty(),
            "session/new must not fabricate AgentRequest rows"
        );
    }

    /// Gate 7 (fixture lifetime): the staging `TempDir` guard returned by
    /// the fixture owns the node's storage directory. While the guard is
    /// held the node's data path exists; when it drops at the end of the
    /// test the whole staging directory is removed — the guard is the
    /// cleanup, not a `mem::forget`-leaked or `keep()`-abandoned directory
    /// left for the temp cleaner.
    ///
    /// The binding matches the safe normal pattern — the `TempDir` is bound
    /// *first*, so the ordinary end-of-scope drop order is service/node
    /// first, staging second. The explicit drops below observe the same
    /// order: the service (and its node) must drop before the `TempDir`
    /// deletes the node's storage directory out from under it.
    #[tokio::test]
    async fn the_fixture_staging_tempdir_owns_and_cleans_up_the_node_storage() {
        let (staging, service) = test_service().await;
        // The node's storage path lives inside the guard's directory and is
        // observable while the guard is held.
        let staging_root = staging.path().to_path_buf();
        let node_data = staging_root.join("node");
        assert!(
            node_data.exists(),
            "the node's storage directory must exist while the staging guard is held"
        );

        // Run one real dispatch through the fixture so the node is genuinely
        // used, then observe the cleanup by dropping in the same order an
        // ordinary test's end-of-scope drop would use: service first,
        // staging second.
        service
            .handle_acp_payload(&request_payload(
                "session/new",
                json!({
                    "cwd": "/tmp",
                    "mcpServers": [],
                    "_meta": { "sessionId": "s-staging-cleanup" },
                }),
            ))
            .await;
        drop(service);
        drop(staging);
        assert!(
            !node_data.exists(),
            "dropping the staging guard must delete the node's storage directory"
        );
        assert!(
            !staging_root.exists(),
            "the whole staging directory must be cleaned up, not abandoned"
        );
    }

    #[tokio::test]
    async fn session_new_is_idempotent_for_the_same_session_id() {
        let (_staging, service) = test_service().await;
        let params = json!({
            "cwd": "/tmp",
            "mcpServers": [],
            "_meta": { "sessionId": "grok-edge-idempotent" },
        });
        let first = service
            .handle_acp_payload(&request_payload("session/new", params.clone()))
            .await;
        let second = service
            .handle_acp_payload(&request_payload("session/new", params))
            .await;
        let first = parse_response(first.response.as_deref().expect("response"));
        let second = parse_response(second.response.as_deref().expect("response"));
        assert_eq!(first["result"]["sessionId"], second["result"]["sessionId"]);

        let node = service.config.node.clone();
        let query = r#"{
            AgentSession(filter: { session_id: { _eq: "grok-edge-idempotent" } }) { session_id }
        }"#
        .to_string();
        let response = node.execute(&query).await;
        ensure_no_errors(&response, "idempotency check").expect("query ok");
        let sessions = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentSession"))
            .and_then(Value::as_array)
            .expect("AgentSession array");
        assert_eq!(
            sessions.len(),
            1,
            "repeat session/new must not duplicate rows"
        );
    }

    #[tokio::test]
    async fn session_new_rejects_an_existing_session_bound_to_a_different_agent() {
        // `agent_did` is `@immutable` on `AgentSession`. A row stamped for a
        // different principal must be rejected before reactivation, not
        // silently reactivated under this shim's identity.
        let (_staging, service) = test_service().await;
        let node = service.config.node.clone();
        let seed = r#"mutation {
            create_AgentSession(input: {
                session_id: "grok-edge-foreign-agent",
                agent_name: "foreign-agent",
                agent_did: "did:test:foreign-agent",
                behavior_id: "did:test:grok-shim:default",
                started: "2026-08-31T22:46:45Z",
                status: "ended",
                ended: "2026-08-31T22:46:46Z"
            }) { _docID }
        }"#
        .to_string();
        let response = node.execute(&seed).await;
        ensure_no_errors(&response, "foreign session seed").expect("seed ok");

        let dispatch = service
            .handle_acp_payload(&request_payload(
                "session/new",
                json!({
                    "cwd": "/tmp",
                    "mcpServers": [],
                    "_meta": { "sessionId": "grok-edge-foreign-agent" },
                }),
            ))
            .await;
        let response = parse_response(dispatch.response.as_deref().expect("response line"));
        assert_eq!(response["error"]["code"], -32603);
        let message = response["error"]["message"].as_str().expect("message");
        assert!(
            message.contains("did:test:foreign-agent"),
            "the mismatch must name the session's immutable agent: {message}"
        );
        assert!(
            message.contains("agent_did"),
            "the mismatch must name the immutable field: {message}"
        );

        // The foreign row is untouched: never reactivated, never rewritten.
        let query = r#"{
            AgentSession(filter: { session_id: { _eq: "grok-edge-foreign-agent" } }) {
                agent_did status
            }
        }"#
        .to_string();
        let response = node.execute(&query).await;
        ensure_no_errors(&response, "foreign session check").expect("query ok");
        let sessions = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentSession"))
            .and_then(Value::as_array)
            .expect("AgentSession array");
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0]["agent_did"], "did:test:foreign-agent");
        assert_eq!(sessions[0]["status"], "ended");
    }

    #[tokio::test]
    async fn session_new_escapes_session_ids_in_queries() {
        let (_staging, service) = test_service().await;
        // A quote/backslash-rich id proves every interpolated value is
        // escaped rather than spliced raw into the GraphQL document.
        let hostile = r#"grok"\<script>-id"#;
        let dispatch = service
            .handle_acp_payload(&request_payload(
                "session/new",
                json!({ "cwd": "/tmp", "mcpServers": [], "_meta": { "sessionId": hostile } }),
            ))
            .await;
        let response = parse_response(dispatch.response.as_deref().expect("response line"));
        assert_eq!(response["result"]["sessionId"], hostile);

        let node = service.config.node.clone();
        let query = format!(
            r#"{{ AgentSession(filter: {{ session_id: {{ _eq: "{}" }} }}) {{ session_id }} }}"#,
            escape_graphql_string(hostile)
        );
        let response = node.execute(&query).await;
        ensure_no_errors(&response, "hostile id check").expect("query ok");
        let sessions = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentSession"))
            .and_then(Value::as_array)
            .expect("AgentSession array");
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0]["session_id"], hostile);
    }

    #[tokio::test]
    async fn session_new_rejects_model_outside_bound_catalog() {
        let (_staging, service) = test_service().await;
        let dispatch = service
            .handle_acp_payload(&request_payload(
                "session/new",
                json!({
                    "cwd": "/tmp",
                    "mcpServers": [],
                    "_meta": { "sessionId": "s-model", "modelId": "gpt-9" },
                }),
            ))
            .await;
        let response = parse_response(dispatch.response.as_deref().expect("response line"));
        assert_eq!(
            response["error"]["code"], -32602,
            "a requested model outside the bound catalog is invalid params"
        );
        assert!(
            response["error"]["message"]
                .as_str()
                .expect("message")
                .contains("bound catalog"),
            "rejection must name the bound catalog"
        );
        assert!(
            dispatch.notifications.is_empty(),
            "a rejected session/new must not clear the pager's MCP spinner"
        );
    }

    #[tokio::test]
    async fn set_model_emits_models_update_and_rejects_unknown_model() {
        let (_staging, service) = test_service().await;
        service
            .handle_acp_payload(&request_payload(
                "session/new",
                json!({ "cwd": "/tmp", "mcpServers": [], "_meta": { "sessionId": "s-set-model" } }),
            ))
            .await;

        let rejected = service
            .handle_acp_payload(&request_payload(
                "session/set_model",
                json!({ "sessionId": "s-set-model", "modelId": "nope" }),
            ))
            .await;
        let response = parse_response(rejected.response.as_deref().expect("response line"));
        assert_eq!(
            response["error"]["code"], -32602,
            "a model outside the bound catalog is a client invalid-params error"
        );
        assert!(
            response["error"]["message"]
                .as_str()
                .expect("message")
                .contains("bound catalog"),
            "rejection must name the bound catalog"
        );

        let dispatch = service
            .handle_acp_payload(&request_payload(
                "session/set_model",
                json!({
                    "sessionId": "s-set-model",
                    "modelId": "GLM-5.3-NVFP4",
                }),
            ))
            .await;
        assert_eq!(dispatch.notifications.len(), 1);
        let notification = parse_response(&dispatch.notifications[0]);
        assert_eq!(notification["method"], "_x.ai/models/update");
        assert_eq!(notification["params"]["currentModelId"], "GLM-5.3-NVFP4");
        assert!(notification["params"].get("models").is_none());
        let response = parse_response(dispatch.response.as_deref().expect("response line"));
        assert_eq!(response["result"], json!({}), "set_model result is empty");
    }

    #[tokio::test]
    async fn set_model_rejects_a_reasoning_effort_as_invalid_params() {
        // The catalog advertises `supportsReasoningEffort: false` and no
        // effort list, and a selected effort never reaches
        // `SubmitRequestOptions` — so a caller-supplied
        // `_meta.reasoningEffort` is rejected explicitly as invalid params
        // instead of being silently stored as dead session state. Even a
        // value Gents' own `ReasoningEffort` would parse ("high") is
        // rejected, because the advertised catalog never offered it.
        let (_staging, service) = test_service().await;
        service
            .handle_acp_payload(&request_payload(
                "session/new",
                json!({ "cwd": "/tmp", "mcpServers": [], "_meta": { "sessionId": "s-effort" } }),
            ))
            .await;
        for effort in ["high", "extreme", "none", "low"] {
            let dispatch = service
                .handle_acp_payload(&request_payload(
                    "session/set_model",
                    json!({
                        "sessionId": "s-effort",
                        "modelId": "GLM-5.3-NVFP4",
                        "_meta": { "reasoningEffort": effort },
                    }),
                ))
                .await;
            let response = parse_response(dispatch.response.as_deref().expect("response line"));
            assert_eq!(
                response["error"]["code"], -32602,
                "reasoningEffort {effort:?} must be rejected as invalid params: the \
                     feature is unadvertised and unimplemented"
            );
            assert!(
                response["error"]["message"]
                    .as_str()
                    .expect("message")
                    .contains("reasoningEffort"),
                "the rejection must name the unsupported key: {effort}"
            );
            assert!(
                dispatch.notifications.is_empty(),
                "a rejected set_model must not emit a catalog update"
            );
        }
    }

    #[tokio::test]
    async fn set_mode_emits_current_mode_update() {
        let (_staging, service) = test_service().await;
        service
            .handle_acp_payload(&request_payload(
                "session/new",
                json!({ "cwd": "/tmp", "mcpServers": [], "_meta": { "sessionId": "s-set-mode" } }),
            ))
            .await;

        let dispatch = service
            .handle_acp_payload(&request_payload(
                "session/set_mode",
                json!({ "sessionId": "s-set-mode", "modeId": "yolo" }),
            ))
            .await;
        assert_eq!(dispatch.notifications.len(), 1);
        let notification = parse_response(&dispatch.notifications[0]);
        assert_eq!(notification["method"], "session/update");
        assert_eq!(
            notification["params"]["update"]["sessionUpdate"],
            "current_mode_update"
        );
        assert_eq!(notification["params"]["update"]["currentModeId"], "yolo");
        assert_eq!(notification["params"]["sessionId"], "s-set-mode");
        assert_eq!(
            notification["params"]["_meta"]["eventId"], "s-set-mode-1",
            "the MCP completion extension must not consume a session-update event id"
        );
        let response = parse_response(dispatch.response.as_deref().expect("response line"));
        assert_eq!(response["result"], json!({}), "set_mode result is empty");
    }

    #[tokio::test]
    async fn event_ids_are_monotonic_and_session_scoped_through_one_sequencer() {
        let (_staging, service) = test_service().await;
        // set_mode and the prompt echo share the projection sequencer's
        // per-session id space, so both routes go through the same counter.
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let sender = PromptSender::Buffer {
            buffer: buffer.clone(),
        };
        for _ in 0..3 {
            sender
                .send_user_message_chunk(
                    service.projections.session_updates(),
                    "s-events",
                    "prompt-1",
                    &PromptBlock {
                        kind: "text".to_string(),
                        text: "hello".to_string(),
                        meta: None,
                    },
                    0,
                )
                .await
                .expect("echo send");
        }
        let lines = buffer.lock().await;
        assert_eq!(lines.len(), 3, "three sends enqueue three lines");
        let event_ids: Vec<String> = lines
            .iter()
            .map(|line| {
                parse_response(line)["params"]["_meta"]["eventId"]
                    .as_str()
                    .expect("eventId")
                    .to_string()
            })
            .collect();
        assert_eq!(event_ids, vec!["s-events-1", "s-events-2", "s-events-3"]);

        // A different session has an independent counter starting at 1.
        let other_lines = {
            let other_buffer = Arc::new(Mutex::new(Vec::new()));
            let other_sender = PromptSender::Buffer {
                buffer: other_buffer.clone(),
            };
            other_sender
                .send_user_message_chunk(
                    service.projections.session_updates(),
                    "s-other",
                    "prompt-1",
                    &PromptBlock {
                        kind: "text".to_string(),
                        text: "hello".to_string(),
                        meta: None,
                    },
                    0,
                )
                .await
                .expect("echo send");
            let lines = other_buffer.lock().await;
            lines
                .iter()
                .map(|line| {
                    parse_response(line)["params"]["_meta"]["eventId"]
                        .as_str()
                        .expect("eventId")
                        .to_string()
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(other_lines, vec!["s-other-1"]);
        assert_eq!(
            service
                .projections
                .sequencer_arc()
                .event_counter("s-events"),
            3
        );
        assert_eq!(
            service.projections.sequencer_arc().event_counter("s-other"),
            1
        );
    }

    /// A `session/set_mode` and a `session/prompt` on the same session draw
    /// from the same monotonic id space and can never stamp a duplicate
    /// eventId (the pager deduplicates non-replay counters by id, so a
    /// repeated id would silently drop a live update).
    #[tokio::test]
    async fn set_mode_and_prompt_never_duplicate_an_event_id() {
        let (_staging, service) = test_service().await;
        service
            .handle_acp_payload(&request_payload(
                "session/new",
                json!({ "cwd": "/tmp", "mcpServers": [], "_meta": { "sessionId": "s-dup" } }),
            ))
            .await;
        let mode_dispatch = service
            .handle_acp_payload(&request_payload(
                "session/set_mode",
                json!({ "sessionId": "s-dup", "modeId": "yolo" }),
            ))
            .await;
        let mode_notification = parse_response(&mode_dispatch.notifications[0]);
        let mode_event_id = mode_notification["params"]["_meta"]["eventId"]
            .as_str()
            .expect("mode eventId")
            .to_string();

        // The turn's user echo through a buffered sender stamps the next id
        // of the same session space.
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let sender = PromptSender::Buffer {
            buffer: buffer.clone(),
        };
        sender
            .send_user_message_chunk(
                service.projections.session_updates(),
                "s-dup",
                "prompt-1",
                &crate::commands::grok_shim::turn::PromptBlock {
                    kind: "text".to_string(),
                    text: "hello".to_string(),
                    meta: None,
                },
                0,
            )
            .await
            .expect("echo send");
        let lines = buffer.lock().await;
        let echo: Value = serde_json::from_str(&lines[0]).expect("echo line");
        let echo_event_id = echo["params"]["_meta"]["eventId"]
            .as_str()
            .expect("echo eventId")
            .to_string();

        assert_ne!(mode_event_id, echo_event_id);
        assert_eq!(mode_event_id, "s-dup-1");
        assert_eq!(echo_event_id, "s-dup-2");
    }

    /// The production delegate sends MCP completion after session/new's
    /// response, so the native pager has attached the new session's routing.
    #[tokio::test]
    async fn session_new_streams_exact_mcp_completion_after_the_live_response() {
        let (_staging, service) = test_service().await;
        let payload = request_payload(
            "session/new",
            json!({
                "cwd": "/tmp",
                "mcpServers": [],
                "_meta": { "sessionId": "s-mcp-live" },
            }),
        );
        let (frames_tx, mut frames_rx) = tokio::sync::mpsc::unbounded_channel::<
            crate::commands::grok_shim::protocol::ServerEnvelope,
        >();

        service
            .handle_acp(&payload, AcpOutbound::for_frames(frames_tx))
            .await
            .expect("session/new dispatch");

        let first = frames_rx.recv().await.expect("session/new response frame");
        let crate::commands::grok_shim::protocol::ServerEnvelope::Acp { payload } = first else {
            panic!("session/new emitted a non-ACP frame first");
        };
        let response = parse_response(&payload);
        assert_eq!(response["id"], 1);
        assert_eq!(response["result"]["sessionId"], "s-mcp-live");

        let second = frames_rx.recv().await.expect("MCP completion frame");
        let crate::commands::grok_shim::protocol::ServerEnvelope::Acp { payload } = second else {
            panic!("session/new emitted a non-ACP completion frame");
        };
        assert_eq!(
            parse_response(&payload),
            json!({
                "jsonrpc": "2.0",
                "method": "_x.ai/mcp_initialized",
                "params": {
                    "sessionId": "s-mcp-live",
                    "mcpToolCount": 0,
                    "elapsedMs": 0,
                },
            })
        );

        assert!(
            frames_rx.try_recv().is_err(),
            "session/new must emit exactly one notification and one response"
        );
    }

    /// Gate 3: the live production path. A `session/set_mode` dispatched
    /// through the real `AcpDelegate` over a live `AcpOutbound` streams its
    /// `current_mode_update` notification *before* its JSON-RPC response —
    /// the real common send path enqueues the notification live while the
    /// deferred response is sent only after the dispatch resolves — and the
    /// notification carries the session's first event id from the shared
    /// per-session space.
    #[tokio::test]
    async fn set_mode_streams_its_notification_before_the_response_on_the_live_path() {
        let (_staging, service) = test_service().await;
        service
            .handle_acp_payload(&request_payload(
                "session/new",
                json!({ "cwd": "/tmp", "mcpServers": [], "_meta": { "sessionId": "s-mode-live" } }),
            ))
            .await;

        let payload = request_payload(
            "session/set_mode",
            json!({ "sessionId": "s-mode-live", "modeId": "yolo" }),
        );

        let (frames_tx, mut frames_rx) = tokio::sync::mpsc::unbounded_channel::<
            crate::commands::grok_shim::protocol::ServerEnvelope,
        >();
        let outbound = AcpOutbound::for_frames(frames_tx);
        service
            .handle_acp(&payload, outbound)
            .await
            .expect("set_mode dispatch");

        // The notification must arrive before the response on the wire.
        let mut saw_mode_update = false;
        let mut saw_response = false;
        while let Ok(frame) =
            tokio::time::timeout(std::time::Duration::from_secs(10), frames_rx.recv()).await
        {
            let Some(crate::commands::grok_shim::protocol::ServerEnvelope::Acp { payload }) = frame
            else {
                continue;
            };
            let value: Value = serde_json::from_str(&payload).expect("acp payload is JSON");
            if value.get("id").is_some() {
                saw_response = true;
                break;
            }
            assert!(!saw_response, "a notification arrived after the response");
            assert_eq!(
                value["params"]["update"]["sessionUpdate"], "current_mode_update",
                "the streamed notification is the mode update"
            );
            assert_eq!(
                value["params"]["_meta"]["eventId"], "s-mode-live-1",
                "the mode update consumes the session's first event id"
            );
            saw_mode_update = true;
        }
        assert!(saw_mode_update, "the mode update streamed live");
        assert!(saw_response, "the set_mode response arrived");
        assert!(
            service
                .projections
                .sequencer_arc()
                .event_counter("s-mode-live")
                == 1,
            "exactly one event id was consumed"
        );
    }

    /// Gate 4 (exact live mode failure): a `session/set_mode` dispatched
    /// through the production ACP delegate over a *closed* live
    /// `AcpOutbound` fails the dispatch and the send, leaves the session's
    /// recorded mode at `"default"`, and consumes no event id — so a later
    /// successful send on the same session receives `<session>-1`, never a
    /// re-used already-delivered id.
    #[tokio::test]
    async fn set_mode_over_a_closed_live_outbound_fails_without_committing_mode_or_event_id() {
        let (_staging, service) = test_service().await;
        // A known session in the default mode.
        service
            .handle_acp_payload(&request_payload(
                "session/new",
                json!({
                    "cwd": "/tmp",
                    "mcpServers": [],
                    "_meta": { "sessionId": "s-mode-closed" },
                }),
            ))
            .await;

        let payload = request_payload(
            "session/set_mode",
            json!({ "sessionId": "s-mode-closed", "modeId": "yolo" }),
        );

        // A real production `AcpOutbound` whose receiver is already gone:
        // every send through it fails, exactly like a pager connection that
        // closed before the mode update was enqueued.
        let (frames_tx, frames_rx) = tokio::sync::mpsc::unbounded_channel::<
            crate::commands::grok_shim::protocol::ServerEnvelope,
        >();
        let outbound = AcpOutbound::for_frames(frames_tx);
        drop(frames_rx);

        // The production dispatch entry: the delegate wraps the outbound in
        // the live prompt sender and runs the same handler the pager drives.
        let dispatch_result = service.handle_acp(&payload, outbound).await;
        let send_error =
            dispatch_result.expect_err("the closed outbound must fail the set_mode dispatch");
        assert!(
            send_error.to_string().contains("connection is closed"),
            "the dispatch must surface the closed-connection send error: {send_error}"
        );

        // The actual `AcpSessionState`: the failed send never ran the mode
        // commit hook, so the recorded mode is still the default.
        let sessions = service.sessions.lock().await;
        let state = sessions
            .get("s-mode-closed")
            .expect("the session state must exist after session/new");
        assert_eq!(
            state.mode_id, "default",
            "a failed live send must leave the recorded mode untouched"
        );
        drop(sessions);

        // The failed send consumed no event id: the reservation rolled back.
        assert_eq!(
            service
                .projections
                .sequencer_arc()
                .event_counter("s-mode-closed"),
            0,
            "a failed live send must consume no event id"
        );

        // Follow-up: a successful real/buffered send on the same session
        // receives `<session>-1` — the rolled-back id is reused, and an
        // already-delivered id is never handed out twice.
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let sender = PromptSender::Buffer {
            buffer: buffer.clone(),
        };
        sender
            .send_user_message_chunk(
                service.projections.session_updates(),
                "s-mode-closed",
                "prompt-1",
                &PromptBlock {
                    kind: "text".to_string(),
                    text: "hello".to_string(),
                    meta: None,
                },
                0,
            )
            .await
            .expect("the recovery send must succeed");
        let lines = buffer.lock().await;
        let echo: Value = serde_json::from_str(&lines[0]).expect("echo line");
        assert_eq!(
            echo["params"]["_meta"]["eventId"], "s-mode-closed-1",
            "the next successful send must receive the session's first id"
        );
    }

    /// Gate 3 (production entry, buffered sends): two sessions dispatched
    /// through the production dispatch path (`dispatch_with_sender`, the
    /// same entry the live delegate calls) each start their event-id space
    /// at 1 and each stream exactly one mode update. Both dispatches run
    /// against immediately-buffered sends, so this test proves the
    /// production entry's per-session id accounting — it does **not**
    /// prove the sessions' *send locks* are independent: a 30-second
    /// timeout over two immediate buffered operations can only observe
    /// completion, never non-blocking. The real independence proof is
    /// [`ProjectionEngine`'s delayed session-A/session-B channel test]
    /// (`a_delayed_first_send_is_not_overtaken_by_a_racing_second_send` and
    /// `two_sessions_start_at_one_and_stay_independently_concurrent` in
    /// `projection.rs`), which parks session A's first send inside its
    /// enqueue and asserts session B completes while A is still parked.
    /// A deterministic stalled live dispatch seam here would need a
    /// injectable transport parked at the `dispatch_with_sender` boundary.
    #[tokio::test]
    async fn two_sessions_dispatch_through_the_production_entry_with_independent_event_ids() {
        let (_staging, service) = test_service().await;
        let service = Arc::new(service);
        for session in ["s-concurrent-a", "s-concurrent-b"] {
            service
                .handle_acp_payload(&request_payload(
                    "session/new",
                    json!({ "cwd": "/tmp", "mcpServers": [], "_meta": { "sessionId": session } }),
                ))
                .await;
        }

        // Two set_mode dispatches on different sessions, run concurrently
        // through the production dispatch entry: neither waits on the other.
        let service_a = service.clone();
        let service_b = service.clone();
        let dispatch_a = tokio::spawn(async move {
            service_a
                .handle_acp_payload(&request_payload(
                    "session/set_mode",
                    json!({ "sessionId": "s-concurrent-a", "modeId": "yolo" }),
                ))
                .await
        });
        let dispatch_b = tokio::spawn(async move {
            service_b
                .handle_acp_payload(&request_payload(
                    "session/set_mode",
                    json!({ "sessionId": "s-concurrent-b", "modeId": "default" }),
                ))
                .await
        });
        let (a, b) = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            (dispatch_a.await, dispatch_b.await)
        })
        .await
        .expect("the two buffered session dispatches must both complete");
        let a = a.expect("join a").notifications;
        let b = b.expect("join b").notifications;
        assert_eq!(a.len(), 1, "session A streamed one mode update");
        assert_eq!(b.len(), 1, "session B streamed one mode update");

        // Each session consumed exactly its own first event id — per-session
        // counters, both starting at 1, never shared.
        let event_id = |line: &str| {
            parse_response(line)["params"]["_meta"]["eventId"]
                .as_str()
                .expect("eventId")
                .to_string()
        };
        assert_eq!(event_id(&a[0]), "s-concurrent-a-1");
        assert_eq!(event_id(&b[0]), "s-concurrent-b-1");
        let sequencer = service.projections.sequencer_arc();
        assert_eq!(sequencer.event_counter("s-concurrent-a"), 1);
        assert_eq!(sequencer.event_counter("s-concurrent-b"), 1);
    }

    #[tokio::test]
    async fn set_model_and_set_mode_require_a_known_session() {
        let (_staging, service) = test_service().await;
        let set_model = service
            .handle_acp_payload(&request_payload(
                "session/set_model",
                json!({ "sessionId": "missing", "modelId": "GLM-5.3-NVFP4" }),
            ))
            .await;
        assert!(
            parse_response(set_model.response.as_deref().expect("response"))
                .get("error")
                .is_some(),
            "set_model on an unknown session must fail"
        );
        let set_mode = service
            .handle_acp_payload(&request_payload(
                "session/set_mode",
                json!({ "sessionId": "missing", "modeId": "yolo" }),
            ))
            .await;
        assert!(
            parse_response(set_mode.response.as_deref().expect("response"))
                .get("error")
                .is_some(),
            "set_mode on an unknown session must fail"
        );
    }

    #[tokio::test]
    async fn interject_and_compact_answer_method_not_found() {
        let (_staging, service) = test_service().await;
        for (method, params) in [
            (
                "x.ai/interject",
                json!({ "sessionId": "s1", "text": "hi", "interjectionId": "i1" }),
            ),
            ("x.ai/compact_conversation", json!({ "sessionId": "s1" })),
        ] {
            let dispatch = service
                .handle_acp_payload(&request_payload(method, params))
                .await;
            assert!(dispatch.notifications.is_empty());
            let response = parse_response(dispatch.response.as_deref().expect("response line"));
            assert_eq!(
                response["error"]["code"], -32601,
                "{method} must answer with the audited method-not-found code"
            );
            assert!(
                !response["error"]["message"]
                    .as_str()
                    .expect("message")
                    .is_empty(),
                "{method} must carry the owned-transition explanation"
            );
        }
    }

    #[tokio::test]
    async fn stubs_never_write_documents() {
        let (_staging, service) = test_service().await;
        service
            .handle_acp_payload(&request_payload(
                "session/new",
                json!({ "cwd": "/tmp", "mcpServers": [], "_meta": { "sessionId": "s-stubs" } }),
            ))
            .await;
        for (method, params) in [
            (
                "x.ai/interject",
                json!({ "sessionId": "s-stubs", "text": "hi", "interjectionId": "i1" }),
            ),
            (
                "x.ai/compact_conversation",
                json!({ "sessionId": "s-stubs" }),
            ),
        ] {
            service
                .handle_acp_payload(&request_payload(method, params))
                .await;
        }
        let node = service.config.node.clone();
        let query = r#"{
            AgentRequest(filter: { session_id: { _eq: "s-stubs" } }) { request_id }
            AgentMessage(filter: { session_id: { _eq: "s-stubs" } }) { sequence }
        }"#
        .to_string();
        let response = node.execute(&query).await;
        ensure_no_errors(&response, "stub document check").expect("query ok");
        let requests = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(Value::as_array)
            .expect("AgentRequest array");
        let messages = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentMessage"))
            .and_then(Value::as_array)
            .expect("AgentMessage array");
        assert!(
            requests.is_empty() && messages.is_empty(),
            "rejected stubs must not fabricate AgentRequest or AgentMessage rows"
        );
    }

    #[tokio::test]
    async fn unknown_methods_answer_method_not_found() {
        let (_staging, service) = test_service().await;
        let dispatch = service
            .handle_acp_payload(&request_payload("terminal/invent", json!({})))
            .await;
        let response = parse_response(dispatch.response.as_deref().expect("response line"));
        assert_eq!(response["error"]["code"], -32601);
        assert!(
            response["error"]["message"]
                .as_str()
                .expect("message")
                .contains("terminal/invent"),
            "the unrouted method must be named in the error"
        );
    }

    #[tokio::test]
    async fn wait_for_exit_answers_the_pagers_exact_method_not_found_error() {
        // The reference pager answers `terminal/wait_for_exit` with
        // `wait_for_exit_not_supported("pager")` — a `METHOD_NOT_FOUND`
        // error whose message is exactly "pager does not handle
        // WaitForTerminalExit" — and its adapter falls back to polling on
        // that answer. The shim must reproduce the code and the message
        // verbatim through the full dispatch path.
        let (_staging, service) = test_service().await;
        let dispatch = service
            .handle_acp_payload(&request_payload("terminal/wait_for_exit", json!({})))
            .await;
        let response = parse_response(dispatch.response.as_deref().expect("response line"));
        assert_eq!(response["error"]["code"], -32601);
        assert_eq!(
            response["error"]["message"], PAGER_WAIT_FOR_EXIT_MESSAGE,
            "wait_for_exit must answer with the pager's exact message"
        );
    }

    #[tokio::test]
    async fn other_terminal_methods_answer_the_shaped_not_supported_error() {
        // Every other terminal/* ACP request routes through
        // `tools::handle_terminal_client_method` and answers with that
        // leaf's shaped method-not-found error — the shim's own wording,
        // never the generic method-not-found message and never a claim of
        // pager-exactness (only `terminal/wait_for_exit` is pager-exact).
        let (_staging, service) = test_service().await;
        for method in [
            "terminal/create",
            "terminal/output",
            "terminal/kill",
            "terminal/release",
        ] {
            let dispatch = service
                .handle_acp_payload(&request_payload(method, json!({})))
                .await;
            let response = parse_response(dispatch.response.as_deref().expect("response line"));
            assert_eq!(response["error"]["code"], -32601, "{method}");
            let message = response["error"]["message"].as_str().expect("message");
            assert!(
                message.contains(&format!("{method}: ")),
                "{method} must be named with the shaped prefix: {message}"
            );
            assert!(
                message.contains("clientTerminal=false"),
                "{method} must explain the agent-side terminal routing: {message}"
            );
            assert!(
                !message.contains("is not supported by the Gents Grok shim"),
                "{method} must not collapse to the generic method-not-found: {message}"
            );
        }
    }

    #[tokio::test]
    async fn undecodable_payloads_answer_invalid_request() {
        let (_staging, service) = test_service().await;
        let dispatch = service.handle_acp_payload("not json").await;
        let response = parse_response(dispatch.response.as_deref().expect("response line"));
        assert_eq!(response["id"], Value::Null);
        assert_eq!(response["error"]["code"], -32600);
    }

    /// Table-driven dispatch classification over real request payloads: every
    /// caller-controlled validation failure answers exactly `-32602`, the
    /// shaped unsupported methods answer exactly `-32601`, and no durable
    /// rows are fabricated for any rejected input.
    #[tokio::test]
    async fn request_validation_failures_answer_invalid_params() {
        let (_staging, service) = test_service().await;
        // A real session so session-scoped rejections aren't conflated with
        // the unknown-session path (which stays internal).
        service
            .handle_acp_payload(&request_payload(
                "session/new",
                json!({ "cwd": "/tmp", "mcpServers": [], "_meta": { "sessionId": "s-table" } }),
            ))
            .await;

        let text_block = json!({"type": "text", "text": "hello"});
        let cases: Vec<(&str, Value)> = vec![
            // missing/empty modelId on session/set_model
            (
                "session/set_model",
                json!({ "sessionId": "s-table", "modelId": "" }),
            ),
            ("session/set_model", json!({ "sessionId": "s-table" })),
            // wrong bound model on session/set_model
            (
                "session/set_model",
                json!({ "sessionId": "s-table", "modelId": "gpt-9" }),
            ),
            // wrong bound model on session/new `_meta.modelId`
            (
                "session/new",
                json!({ "cwd": "/tmp", "_meta": { "sessionId": "s-never", "modelId": "gpt-9" } }),
            ),
            // missing/empty sessionId on session/set_model
            ("session/set_model", json!({ "modelId": "GLM-5.3-NVFP4" })),
            (
                "session/set_model",
                json!({ "sessionId": "  ", "modelId": "GLM-5.3-NVFP4" }),
            ),
            // missing/empty modeId on session/set_mode
            (
                "session/set_mode",
                json!({ "sessionId": "s-table", "modeId": "" }),
            ),
            ("session/set_mode", json!({ "sessionId": "s-table" })),
            // bad auth method
            ("authenticate", json!({ "methodId": "oauth" })),
            ("authenticate", json!({ "methodId": "" })),
            // session/prompt: missing sessionId
            ("session/prompt", json!({ "prompt": [text_block.clone()] })),
            // session/prompt: missing prompt array
            ("session/prompt", json!({ "sessionId": "s-table" })),
            // session/prompt: empty prompt array
            (
                "session/prompt",
                json!({ "sessionId": "s-table", "prompt": [] }),
            ),
            // session/prompt: block without text
            (
                "session/prompt",
                json!({ "sessionId": "s-table", "prompt": [{"type": "text"}] }),
            ),
            // session/prompt: invalid screenMode
            (
                "session/prompt",
                json!({
                    "sessionId": "s-table",
                    "prompt": [text_block.clone()],
                    "_meta": { "screenMode": "sideways" },
                }),
            ),
            // unsupported reasoningEffort (feature is unadvertised)
            (
                "session/set_model",
                json!({
                    "sessionId": "s-table",
                    "modelId": "GLM-5.3-NVFP4",
                    "_meta": { "reasoningEffort": "high" },
                }),
            ),
            // malformed subagent params: each known session-scoped method
            // without a sessionId
            ("x.ai/subagent/get", json!({})),
            ("x.ai/subagent/get", json!({ "sessionId": 7 })),
            ("x.ai/subagent/get", json!({ "sessionId": "  " })),
            ("x.ai/subagent/list_running", json!({})),
            ("x.ai/subagent/list_running", json!({ "sessionId": 7 })),
            ("x.ai/subagent/list_running", json!({ "sessionId": "  " })),
            ("x.ai/subagent/cancel", json!({})),
            ("x.ai/subagent/cancel", json!({ "sessionId": 7 })),
            ("x.ai/subagent/cancel", json!({ "sessionId": "  " })),
            // malformed subagent params: cancel with a valid sessionId but
            // a missing/non-string/blank subagentId
            ("x.ai/subagent/cancel", json!({ "sessionId": "s-table" })),
            (
                "x.ai/subagent/cancel",
                json!({ "sessionId": "s-table", "subagentId": 7 }),
            ),
            (
                "x.ai/subagent/cancel",
                json!({ "sessionId": "s-table", "subagentId": "  " }),
            ),
        ];
        for (method, params) in cases {
            let dispatch = service
                .handle_acp_payload(&request_payload(method, params.clone()))
                .await;
            let response = parse_response(dispatch.response.as_deref().expect("response line"));
            assert_eq!(
                response["error"]["code"], -32602,
                "{method} with {params} must answer invalid params"
            );
            assert!(
                !response["error"]["message"]
                    .as_str()
                    .expect("message")
                    .is_empty(),
                "{method} must carry a diagnostic message"
            );
        }

        // Shaped unsupported methods stay method-not-found.
        for (method, params) in [
            (
                "x.ai/interject",
                json!({ "sessionId": "s-table", "text": "hi" }),
            ),
            (
                "x.ai/compact_conversation",
                json!({ "sessionId": "s-table" }),
            ),
            ("terminal/wait_for_exit", json!({})),
            ("terminal/create", json!({})),
            ("some/unknown/method", json!({})),
            // An unknown subagent ext method is unrouted, not a sibling-leaf
            // generic error: it answers the exact method-not-found code.
            ("x.ai/subagent/invent", json!({ "sessionId": "s-table" })),
        ] {
            let dispatch = service
                .handle_acp_payload(&request_payload(method, params))
                .await;
            let response = parse_response(dispatch.response.as_deref().expect("response line"));
            assert_eq!(
                response["error"]["code"], -32601,
                "{method} must stay method-not-found"
            );
        }

        // Well-formed subagent requests retain the exact current stub
        // results: the shaped not-found snapshots, the empty running list,
        // and the cancel not-found outcome echoing the requested id.
        let get = service
            .handle_acp_payload(&request_payload(
                "x.ai/subagent/get",
                json!({ "sessionId": "s-table", "subagentId": "child-1" }),
            ))
            .await;
        let get = parse_response(get.response.as_deref().expect("response line"));
        assert_eq!(get["result"], json!({ "result": {"snapshot": null} }));

        let list = service
            .handle_acp_payload(&request_payload(
                "x.ai/subagent/list_running",
                json!({ "sessionId": "s-table" }),
            ))
            .await;
        let list = parse_response(list.response.as_deref().expect("response line"));
        assert_eq!(list["result"], json!({ "result": {"subagents": []} }));

        let cancel = service
            .handle_acp_payload(&request_payload(
                "x.ai/subagent/cancel",
                json!({ "sessionId": "s-table", "subagentId": "child-1" }),
            ))
            .await;
        let cancel = parse_response(cancel.response.as_deref().expect("response line"));
        assert_eq!(
            cancel["result"]["result"],
            json!({
                "subagentId": "child-1",
                "cancelled": false,
                "outcome": { "kind": "not_found" },
            })
        );

        // Stock get/cancel DTOs omit sessionId; their reach is limited to
        // sessions registered on this connection, never an arbitrary ID.
        for method in [SUBAGENT_GET_METHOD, SUBAGENT_CANCEL_METHOD] {
            let dispatch = service
                .handle_acp_payload(&request_payload(
                    method,
                    json!({"subagentId": "unknown-child"}),
                ))
                .await;
            let response = parse_response(dispatch.response.as_deref().unwrap());
            assert!(response.get("error").is_none(), "{response}");
            assert!(response["result"].get("result").is_some());
        }
        for method in [
            SUBAGENT_GET_METHOD,
            SUBAGENT_LIST_RUNNING_METHOD,
            SUBAGENT_CANCEL_METHOD,
        ] {
            let dispatch = service
                .handle_acp_payload(&request_payload(
                    method,
                    json!({"sessionId": "not-registered", "subagentId": "child-1"}),
                ))
                .await;
            let response = parse_response(dispatch.response.as_deref().unwrap());
            assert_eq!(response["error"]["code"], -32602, "{response}");
        }

        // No durable rows were fabricated by any rejected input: no
        // AgentRequest, no AgentMessage, no extra AgentSession beyond the
        // one legitimate `s-table` session created above.
        let node = service.config.node.clone();
        let query = r#"{
            AgentRequest { request_id }
            AgentMessage { message_key }
            AgentSession { session_id }
        }"#
        .to_string();
        let response = node.execute(&query).await;
        ensure_no_errors(&response, "rejected-input document check").expect("query ok");
        let count = |key: &str| -> usize {
            response
                .data
                .as_ref()
                .and_then(|data| data.get(key))
                .and_then(Value::as_array)
                .map(|rows| rows.len())
                .unwrap_or_default()
        };
        assert_eq!(
            count("AgentRequest"),
            0,
            "no AgentRequest rows were fabricated"
        );
        assert_eq!(
            count("AgentMessage"),
            0,
            "no AgentMessage rows were fabricated"
        );
        assert_eq!(
            count("AgentSession"),
            1,
            "only the legitimate s-table session exists"
        );
    }

    /// A representative operational failure stays internal `-32603`: the
    /// immutable `agent_did` identity mismatch on `session/new` is a
    /// storage/identity failure of the durable layer, not a caller-shape
    /// error, and the rejected input fabricates nothing.
    #[tokio::test]
    async fn operational_failures_stay_internal_errors() {
        let (_staging, service) = test_service().await;
        let node = service.config.node.clone();
        let seed = r#"mutation {
            create_AgentSession(input: {
                session_id: "s-internal-op",
                agent_name: "foreign-agent",
                agent_did: "did:test:foreign-agent",
                behavior_id: "did:test:grok-shim:default",
                started: "2026-08-31T22:46:45Z",
                status: "ended"
            }) { _docID }
        }"#
        .to_string();
        let response = node.execute(&seed).await;
        ensure_no_errors(&response, "foreign session seed").expect("seed ok");

        let dispatch = service
            .handle_acp_payload(&request_payload(
                "session/new",
                json!({
                    "cwd": "/tmp",
                    "mcpServers": [],
                    "_meta": { "sessionId": "s-internal-op" },
                }),
            ))
            .await;
        let response = parse_response(dispatch.response.as_deref().expect("response line"));
        assert_eq!(
            response["error"]["code"], -32603,
            "a durable identity mismatch is an operational internal error"
        );
    }

    #[tokio::test]
    async fn notifications_never_produce_a_response_body() {
        let (_staging, service) = test_service().await;
        let dispatch = service
            .handle_acp_payload(&notification_payload(
                "session/cancel",
                json!({ "sessionId": "s1", "_meta": { "promptId": "p1" } }),
            ))
            .await;
        assert!(dispatch.response.is_none());
        assert!(dispatch.notifications.is_empty());

        let unknown = service
            .handle_acp_payload(&notification_payload("x.ai/unknown", json!({})))
            .await;
        assert!(
            unknown.response.is_none(),
            "unknown notifications are dropped"
        );
    }
}
