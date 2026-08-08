//! Grok / xAI SuperGrok subscription OAuth provider (subscription proxy path).

use std::future::Future;
use std::sync::Arc;
use std::{fmt, fmt::Formatter};

use anyhow::{Context, Result};
use bytes::Bytes;
use defra_node::EmbeddedNode;
use rig::http_client::{
    self, HeaderMap, HeaderValue, HttpClientExt, LazyBody, MultipartForm, Request, ReqwestClient,
    Response, StreamingResponse,
};
use rig::wasm_compat::WasmCompatSend;
use serde_json::Value;

use crate::oauth_credential::{
    classify_oauth_auth_error, lookup_oauth_credential, shared_bearer, BearerSource,
    DbCredentialBearer, OAuthAuthProblem, OAuthRefreshKind, XAI_OAUTH_PRODUCT,
};

pub const XAI_OAUTH_PROVIDER: &str = "xai-oauth";

/// Subscription inference proxy (not the metered developer API).
pub const XAI_GROK_OAUTH_BASE_URL: &str = "https://cli-chat-proxy.grok.com/v1";

const GROK_CLIENT_VERSION: &str = "0.2.93";
const GROK_CLIENT_VERSION_ENV: &str = "GENTS_XAI_GROK_CLIENT_VERSION";

pub fn default_backend_endpoint() -> &'static str {
    XAI_GROK_OAUTH_BASE_URL
}

pub fn default_model_name() -> &'static str {
    "grok-4.5"
}

pub fn normalize_endpoint(endpoint: &str) -> String {
    let trimmed = endpoint.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        default_backend_endpoint().to_string()
    } else {
        trimmed.to_string()
    }
}

pub fn normalize_provider(provider: &str) -> String {
    let provider = provider.trim();
    if provider.is_empty() {
        XAI_OAUTH_PROVIDER.to_string()
    } else {
        provider.to_string()
    }
}

pub fn classify_xai_auth_error(
    agent_did: &str,
    provider: &str,
    problem: &OAuthAuthProblem,
) -> String {
    classify_oauth_auth_error(&XAI_OAUTH_PRODUCT, agent_did, provider, problem)
}

pub fn grok_client_version() -> String {
    std::env::var(GROK_CLIENT_VERSION_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| GROK_CLIENT_VERSION.to_string())
}

/// Headers the Grok CLI chat proxy uses to recognize subscription clients.
pub fn build_xai_grok_oauth_headers() -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert(
        "Accept",
        HeaderValue::from_static("text/event-stream, application/json"),
    );
    headers.insert("x-xai-token-auth", HeaderValue::from_static("xai-grok-cli"));
    headers.insert(
        "x-authenticateresponse",
        HeaderValue::from_static("authenticate-response"),
    );
    headers.insert(
        "x-grok-client-identifier",
        HeaderValue::from_static("grok-shell"),
    );
    headers.insert(
        "x-grok-client-version",
        HeaderValue::from_str(&grok_client_version())
            .context("Grok client version could not be encoded as an HTTP header")?,
    );
    headers.insert("User-Agent", HeaderValue::from_static("xai-grok-cli"));
    Ok(headers)
}

fn bearer_rejection_status(error: &http_client::Error) -> Option<u16> {
    match error {
        http_client::Error::InvalidStatusCode(status)
        | http_client::Error::InvalidStatusCodeWithMessage(status, _) => Some(status.as_u16()),
        _ => None,
    }
}

// 403 is deliberately NOT a bearer rejection here (unlike Codex): the proxy
// uses it for the NotEntitled tier gate, which no refresh fixes — and with
// rotating refresh tokens a force-refresh loop would burn a rotation per
// request.
fn is_bearer_rejection(error: &http_client::Error) -> bool {
    matches!(bearer_rejection_status(error), Some(401))
}

/// HTTP client that injects a fresh OAuth bearer and lightly shapes Responses bodies.
pub struct XaiGrokOAuthHttpClient<S: BearerSource, H = ReqwestClient> {
    inner: H,
    bearer: Option<Arc<S>>,
}

impl<S: BearerSource, H: Clone> Clone for XaiGrokOAuthHttpClient<S, H> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            bearer: self.bearer.clone(),
        }
    }
}

impl<S: BearerSource, H: fmt::Debug> fmt::Debug for XaiGrokOAuthHttpClient<S, H> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("XaiGrokOAuthHttpClient")
            .field("inner", &self.inner)
            .field("bearer_configured", &self.bearer.is_some())
            .finish()
    }
}

impl<S: BearerSource, H: Default> Default for XaiGrokOAuthHttpClient<S, H> {
    fn default() -> Self {
        Self {
            inner: H::default(),
            bearer: None,
        }
    }
}

impl<S: BearerSource> XaiGrokOAuthHttpClient<S, ReqwestClient> {
    pub fn new(bearer: Arc<S>) -> Self {
        Self {
            inner: ReqwestClient::default(),
            bearer: Some(bearer),
        }
    }
}

impl<S: BearerSource, H> XaiGrokOAuthHttpClient<S, H> {
    pub fn with_inner(bearer: Arc<S>, inner: H) -> Self {
        Self {
            inner,
            bearer: Some(bearer),
        }
    }

    async fn fresh_auth_header(&self) -> http_client::Result<HeaderValue> {
        let bearer = self.bearer.as_ref().ok_or_else(|| {
            http_client::Error::Instance(
                anyhow::anyhow!("XaiGrokOAuthHttpClient used without a configured BearerSource")
                    .into(),
            )
        })?;
        let token = bearer
            .current_bearer()
            .await
            .map_err(|error| http_client::Error::Instance(error.into()))?;
        HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|error| http_client::Error::Instance(anyhow::Error::from(error).into()))
    }

    /// Inject the bearer plus the Grok-CLI identity headers the proxy's auth
    /// middleware and version gate expect on every request, regardless of wire
    /// API or body shape. Callers (e.g. the Responses client builder) may
    /// pre-set identity headers; those are never overwritten.
    async fn apply_auth_and_identity(&self, headers: &mut HeaderMap) -> http_client::Result<()> {
        let value = self.fresh_auth_header().await?;
        headers.insert("authorization", value);
        let identity = build_xai_grok_oauth_headers()
            .map_err(|error| http_client::Error::Instance(error.into()))?;
        for (name, header_value) in identity.iter() {
            if !headers.contains_key(name) {
                headers.insert(name, header_value.clone());
            }
        }
        Ok(())
    }

    async fn prepare(&self, req: Request<Bytes>) -> http_client::Result<Request<Bytes>> {
        let req = Self::patch_responses_body(req);
        let (mut parts, body) = req.into_parts();
        self.apply_auth_and_identity(&mut parts.headers).await?;
        Ok(Request::from_parts(parts, body))
    }

    fn bearer_to_invalidate<X>(&self, result: &http_client::Result<X>) -> Option<Arc<S>> {
        match result {
            Err(error) if is_bearer_rejection(error) => self.bearer.clone(),
            _ => None,
        }
    }

    fn patch_responses_body(req: Request<Bytes>) -> Request<Bytes> {
        let (parts, body) = req.into_parts();
        let mut body = body;
        if parts.uri.path().ends_with("/responses") {
            if let Some(patched) = patch_store_false(&body) {
                body = patched;
            }
        }
        Request::from_parts(parts, body)
    }

    #[cfg(test)]
    pub async fn prepare_for_test(
        &self,
        req: Request<Bytes>,
    ) -> http_client::Result<Request<Bytes>> {
        self.prepare(req).await
    }
}

impl<S, H> HttpClientExt for XaiGrokOAuthHttpClient<S, H>
where
    S: BearerSource + 'static,
    H: Clone + HttpClientExt + fmt::Debug + 'static,
{
    fn send<T, U>(
        &self,
        req: Request<T>,
    ) -> impl Future<Output = http_client::Result<Response<LazyBody<U>>>> + WasmCompatSend + 'static
    where
        T: Into<Bytes> + WasmCompatSend,
        U: From<Bytes>,
        U: WasmCompatSend + 'static,
    {
        let inner = self.inner.clone();
        let this = self.clone();
        let (parts, body) = req.into_parts();
        let req = Request::from_parts(parts, body.into());
        async move {
            let req = this.prepare(req).await?;
            let result = HttpClientExt::send::<Bytes, U>(&inner, req).await;
            if let Some(bearer) = this.bearer_to_invalidate(&result) {
                bearer.invalidate().await;
            }
            result
        }
    }

    fn send_multipart<U>(
        &self,
        req: Request<MultipartForm>,
    ) -> impl Future<Output = http_client::Result<Response<LazyBody<U>>>> + WasmCompatSend + 'static
    where
        U: From<Bytes>,
        U: WasmCompatSend + 'static,
    {
        let inner = self.inner.clone();
        let this = self.clone();
        async move {
            let (mut parts, body) = req.into_parts();
            this.apply_auth_and_identity(&mut parts.headers).await?;
            let req = Request::from_parts(parts, body);
            let result = HttpClientExt::send_multipart(&inner, req).await;
            if let Some(bearer) = this.bearer_to_invalidate(&result) {
                bearer.invalidate().await;
            }
            result
        }
    }

    fn send_streaming<T>(
        &self,
        req: Request<T>,
    ) -> impl Future<Output = http_client::Result<StreamingResponse>> + WasmCompatSend
    where
        T: Into<Bytes>,
    {
        let inner = self.inner.clone();
        let this = self.clone();
        let (parts, body) = req.into_parts();
        let req = Request::from_parts(parts, body.into());
        async move {
            let req = this.prepare(req).await?;
            let result = HttpClientExt::send_streaming(&inner, req).await;
            if let Some(bearer) = this.bearer_to_invalidate(&result) {
                bearer.invalidate().await;
            }
            let mut response = result?;
            ensure_event_stream_content_type(response.headers_mut());
            Ok(response)
        }
    }
}

fn ensure_event_stream_content_type(headers: &mut HeaderMap) {
    if !headers.contains_key("content-type") {
        headers.insert(
            "content-type",
            HeaderValue::from_static("text/event-stream"),
        );
    }
}

fn patch_store_false(body: &[u8]) -> Option<Bytes> {
    let mut value = serde_json::from_slice::<Value>(body).ok()?;
    let mut changed = false;
    if value.get("store").is_none() {
        value["store"] = Value::Bool(false);
        changed = true;
    }
    if !changed {
        return None;
    }
    serde_json::to_vec(&value).ok().map(Bytes::from)
}

/// The authenticated Grok transport, with the rendered-request capture wrapper
/// installed *below* it so the captured body already carries the `store:false`
/// this client injects in `prepare`.
async fn build_authenticated_http(
    node: Arc<EmbeddedNode>,
    agent_did: &str,
) -> Result<CapturingXaiGrokOAuthHttpClient> {
    let provider = XAI_OAUTH_PROVIDER;
    let credential = lookup_oauth_credential(node.as_ref(), agent_did, provider)
        .await
        .with_context(|| format!("loading OAuthCredential for agent {agent_did}"))?
        .ok_or_else(|| {
            anyhow::anyhow!(classify_xai_auth_error(
                agent_did,
                provider,
                &OAuthAuthProblem::Missing,
            ))
        })?;
    let credential_id = credential.credential_id.clone();
    let bearer = shared_bearer(&credential_id, || {
        DbCredentialBearer::with_cache(
            node,
            agent_did,
            provider,
            credential_id.clone(),
            true,
            Some(credential.clone()),
            OAuthRefreshKind::Xai,
            XAI_OAUTH_PRODUCT,
        )
    });
    Ok(XaiGrokOAuthHttpClient::with_inner(
        bearer,
        crate::rendered_request::RenderedRequestCapturingHttpClient::default(),
    ))
}

/// Grok's OAuth transport wrapping the capture seam, which wraps reqwest.
pub type CapturingXaiGrokOAuthHttpClient = XaiGrokOAuthHttpClient<
    DbCredentialBearer,
    crate::rendered_request::RenderedRequestCapturingHttpClient,
>;

pub async fn build_responses_client(
    node: Arc<EmbeddedNode>,
    agent_did: &str,
    endpoint: &str,
) -> Result<rig::providers::openai::Client<CapturingXaiGrokOAuthHttpClient>> {
    let headers = build_xai_grok_oauth_headers()?;
    let endpoint = normalize_endpoint(endpoint);
    let http = build_authenticated_http(node, agent_did).await?;
    crate::inference_http::build_openai_responses_client(
        "xai-oauth-managed",
        &endpoint,
        http,
        headers,
    )
    .context("building Grok OAuth Responses client")
}

pub async fn build_chat_completions_client(
    node: Arc<EmbeddedNode>,
    agent_did: &str,
    endpoint: &str,
) -> Result<rig::providers::openai::CompletionsClient<CapturingXaiGrokOAuthHttpClient>> {
    let endpoint = normalize_endpoint(endpoint);
    // Identity headers ride along via `prepare` on every request.
    let http = build_authenticated_http(node, agent_did).await?;
    crate::inference_http::build_openai_chat_completions_client(
        "xai-oauth-managed",
        &endpoint,
        http,
    )
    .context("building Grok OAuth Chat Completions client")
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingBearer {
        token: String,
        calls: AtomicUsize,
    }

    impl CountingBearer {
        fn new(token: &str) -> Arc<Self> {
            Arc::new(Self {
                token: token.to_string(),
                calls: AtomicUsize::new(0),
            })
        }
    }

    impl BearerSource for CountingBearer {
        async fn current_bearer(&self) -> Result<String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.token.clone())
        }
    }

    #[test]
    fn headers_advertise_cli_identity() {
        let headers = build_xai_grok_oauth_headers().unwrap();
        assert_eq!(
            headers
                .get("x-xai-token-auth")
                .and_then(|value| value.to_str().ok()),
            Some("xai-grok-cli")
        );
        assert_eq!(
            headers
                .get("x-grok-client-identifier")
                .and_then(|value| value.to_str().ok()),
            Some("grok-shell")
        );
        assert_eq!(
            headers
                .get("x-grok-client-version")
                .and_then(|value| value.to_str().ok()),
            Some(grok_client_version().as_str())
        );
        assert_eq!(
            headers
                .get("user-agent")
                .and_then(|value| value.to_str().ok()),
            Some("xai-grok-cli")
        );
    }

    #[tokio::test]
    async fn injects_fresh_bearer() {
        let bearer = CountingBearer::new("tok-xyz");
        let client = XaiGrokOAuthHttpClient::new(bearer.clone());
        let req = Request::builder()
            .method("POST")
            .uri("https://cli-chat-proxy.grok.com/v1/responses")
            .header("authorization", "Bearer STALE")
            .body(Bytes::from_static(br#"{"model":"grok-4.5"}"#))
            .unwrap();
        let prepared = client.prepare_for_test(req).await.unwrap();
        assert_eq!(
            prepared
                .headers()
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer tok-xyz")
        );
        let body: Value = serde_json::from_slice(prepared.body()).unwrap();
        assert_eq!(body.get("store").and_then(Value::as_bool), Some(false));
    }

    #[test]
    fn only_401_invalidates_bearer_403_is_a_tier_gate() {
        // 401 = expired/revoked grant: refresh may fix it. 403 = NotEntitled
        // tier gate: refresh never fixes it, and with rotating refresh tokens
        // a force-refresh loop would burn a rotation per request.
        let unauthorized =
            http_client::Error::InvalidStatusCode("401".parse().expect("valid status"));
        let forbidden = http_client::Error::InvalidStatusCode("403".parse().expect("valid status"));
        assert!(is_bearer_rejection(&unauthorized));
        assert!(!is_bearer_rejection(&forbidden));
    }

    #[derive(Clone, Debug, Default)]
    struct HeaderCapturingInner {
        headers: Arc<std::sync::Mutex<Option<HeaderMap>>>,
    }

    impl HttpClientExt for HeaderCapturingInner {
        fn send<T, U>(
            &self,
            _req: Request<T>,
        ) -> impl Future<Output = http_client::Result<Response<LazyBody<U>>>> + WasmCompatSend + 'static
        where
            T: Into<Bytes> + WasmCompatSend,
            U: From<Bytes> + WasmCompatSend + 'static,
        {
            std::future::ready(Err(http_client::Error::InvalidStatusCode(
                "501".parse().expect("valid status"),
            )))
        }

        fn send_multipart<U>(
            &self,
            req: Request<MultipartForm>,
        ) -> impl Future<Output = http_client::Result<Response<LazyBody<U>>>> + WasmCompatSend + 'static
        where
            U: From<Bytes> + WasmCompatSend + 'static,
        {
            *self.headers.lock().expect("capture lock") = Some(req.headers().clone());
            std::future::ready(Err(http_client::Error::InvalidStatusCode(
                "501".parse().expect("valid status"),
            )))
        }

        fn send_streaming<T>(
            &self,
            _req: Request<T>,
        ) -> impl Future<Output = http_client::Result<StreamingResponse>> + WasmCompatSend
        where
            T: Into<Bytes>,
        {
            std::future::ready(Err(http_client::Error::InvalidStatusCode(
                "501".parse().expect("valid status"),
            )))
        }
    }

    #[tokio::test]
    async fn send_multipart_carries_auth_and_identity_headers() {
        let inner = HeaderCapturingInner::default();
        let captured = inner.headers.clone();
        let client = XaiGrokOAuthHttpClient::with_inner(CountingBearer::new("tok-mp"), inner);
        let req = Request::builder()
            .method("POST")
            .uri("https://cli-chat-proxy.grok.com/v1/files")
            .body(MultipartForm::default())
            .unwrap();
        let _ = HttpClientExt::send_multipart::<Bytes>(&client, req).await;
        let headers = captured
            .lock()
            .expect("capture lock")
            .clone()
            .expect("multipart request reached inner client");
        assert_eq!(
            headers
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer tok-mp")
        );
        assert_eq!(
            headers
                .get("x-xai-token-auth")
                .and_then(|value| value.to_str().ok()),
            Some("xai-grok-cli"),
            "multipart requests must carry the Grok-CLI identity headers too"
        );
    }

    #[test]
    fn headers_include_proxy_auth_middleware_marker() {
        let headers = build_xai_grok_oauth_headers().unwrap();
        assert_eq!(
            headers
                .get("x-authenticateresponse")
                .and_then(|value| value.to_str().ok()),
            Some("authenticate-response")
        );
    }

    #[tokio::test]
    async fn prepare_injects_identity_headers_when_absent() {
        let bearer = CountingBearer::new("tok-abc");
        let client = XaiGrokOAuthHttpClient::new(bearer);
        let req = Request::builder()
            .method("POST")
            .uri("https://cli-chat-proxy.grok.com/v1/chat/completions")
            .body(Bytes::from_static(br#"{"model":"grok-4.5"}"#))
            .unwrap();
        let prepared = client.prepare_for_test(req).await.unwrap();
        for (name, expected) in [
            ("x-xai-token-auth", "xai-grok-cli"),
            ("x-authenticateresponse", "authenticate-response"),
            ("x-grok-client-identifier", "grok-shell"),
            ("user-agent", "xai-grok-cli"),
        ] {
            assert_eq!(
                prepared
                    .headers()
                    .get(name)
                    .and_then(|value| value.to_str().ok()),
                Some(expected),
                "{name} must be injected for proxy requests on any wire API"
            );
        }
        assert_eq!(
            prepared
                .headers()
                .get("x-grok-client-version")
                .and_then(|value| value.to_str().ok()),
            Some(grok_client_version().as_str())
        );
    }

    #[tokio::test]
    async fn prepare_preserves_caller_supplied_identity_headers() {
        let bearer = CountingBearer::new("tok-abc");
        let client = XaiGrokOAuthHttpClient::new(bearer);
        let req = Request::builder()
            .method("POST")
            .uri("https://cli-chat-proxy.grok.com/v1/responses")
            .header("x-grok-client-version", "9.9.9-custom")
            .body(Bytes::from_static(br#"{"model":"grok-4.5"}"#))
            .unwrap();
        let prepared = client.prepare_for_test(req).await.unwrap();
        assert_eq!(
            prepared
                .headers()
                .get("x-grok-client-version")
                .and_then(|value| value.to_str().ok()),
            Some("9.9.9-custom"),
            "caller-supplied identity headers must not be overwritten"
        );
    }

    #[test]
    fn classify_missing_points_at_grok_login() {
        let msg = classify_xai_auth_error(
            "did:key:zAgent",
            XAI_OAUTH_PROVIDER,
            &OAuthAuthProblem::Missing,
        );
        assert!(msg.contains("gents grok-login"), "{msg}");
    }

    #[test]
    fn patch_store_false_is_idempotent_when_present() {
        let body = serde_json::json!({"store": true, "model": "grok-4.5"});
        assert!(patch_store_false(&serde_json::to_vec(&body).unwrap()).is_none());
    }

    /// `build_authenticated_http` installs the capture seam *below* this
    /// wrapper so the row carries the `store:false` Grok injects. Both wrappers
    /// are generic over their inner client, so swapping their order compiles;
    /// only composing the real stack catches it.
    #[tokio::test]
    async fn the_captured_row_carries_the_store_false_grok_injects() {
        use crate::rendered_request::scope::{arm, scope_request, test_scope, CaptureScopeKind};
        use crate::rendered_request::transport::CountingInner;
        use crate::rendered_request::{
            AssemblyBuildPath, AssemblyTrace, RenderedCompletionRequest,
            RenderedRequestCaptureSink, RenderedRequestContext,
        };
        use std::sync::Mutex;

        let terminal = CountingInner::default();
        let client = XaiGrokOAuthHttpClient::with_inner(
            CountingBearer::new("tok"),
            crate::rendered_request::RenderedRequestCapturingHttpClient::new(terminal.clone()),
        );

        let seen: Arc<Mutex<Vec<RenderedCompletionRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let sink_seen = Arc::clone(&seen);
        let sink: RenderedRequestCaptureSink = Arc::new(move |rendered| {
            let seen = Arc::clone(&sink_seen);
            Box::pin(async move {
                seen.lock().expect("seen").push(rendered);
                Ok(crate::rendered_request::test_static_rendered_request_version())
            })
        });
        let scope = test_scope(
            RenderedRequestContext {
                request_doc_id: "doc-1".to_string(),
                request_provenance: Some(
                    crate::document_version::test_request_execution_provenance(
                        "doc-1",
                        "did:key:agent",
                    ),
                ),
                inference_call_provenance_scope:
                    crate::rendered_request::InferenceCallProvenanceScope::StaticOrTest,
                transcript_snapshot: Vec::new(),
                config_provenance_scope:
                    crate::rendered_request::ConfigProvenanceScope::StaticOrOneShot,
                config_provenance: None,
                request_id: "req-1".to_string(),
                agent_did: "did:key:agent".to_string(),
                requester_did: String::new(),
                behavior_id: "behavior".to_string(),
                session_id: "session".to_string(),
                model_name: "configured-model".to_string(),
            },
            sink,
        );

        scope_request(scope, async {
            arm(
                CaptureScopeKind::Inference,
                0,
                0,
                AssemblyTrace::from_effective_messages(AssemblyBuildPath::Budgeted, Vec::new()),
            )
            .expect("armed");
            let req = Request::builder()
                .method("POST")
                .uri("https://api.x.ai/v1/responses")
                .body(Bytes::from(
                    serde_json::to_vec(&serde_json::json!({
                        "model": "grok-4.5",
                        "input": [{"role": "user", "content": "hi"}],
                    }))
                    .expect("assembled body"),
                ))
                .expect("request");
            let _ = HttpClientExt::send_streaming(&client, req)
                .await
                .expect("send");
        })
        .await;

        let seen = seen.lock().expect("seen");
        assert_eq!(seen.len(), 1);
        assert_eq!(
            seen[0].request_json["store"],
            serde_json::Value::Bool(false),
            "the row must show the body after Grok's rewrite: {}",
            seen[0].request_json
        );
        assert_eq!(
            terminal.bodies().first().expect("a forwarded body"),
            &seen[0].request_json,
            "the persisted row and the bytes the network client received must be identical"
        );
    }
}
