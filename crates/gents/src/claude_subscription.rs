//! Claude subscription backend: one Messages HTTP wire (`claude_messages`),
//! authenticated with the agent's `OAuthCredential` (`claude-subscription`)
//! through the shared single-flight bearer. Login is `gents claude-login`;
//! refresh is `claude_oauth_refresh`; the `claude` binary is not involved.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use anyhow::Result;
use defra_node::EmbeddedNode;
use futures::StreamExt;
use rig::client::CompletionClient;
use rig::completion::{
    CompletionError, CompletionModel, CompletionRequest, CompletionResponse, GetTokenUsage, Usage,
};
use rig::http_client::ReqwestClient;
use rig::streaming::{RawStreamingChoice, StreamingCompletionResponse};
use rig::OneOrMany;
use serde::{Deserialize, Serialize};

use crate::claude_oauth::{CLAUDE_OAUTH_PRODUCT, CLAUDE_OAUTH_PROVIDER};
use crate::oauth_credential::{BearerSource, DbCredentialBearer, OAuthRefreshKind};

/// Placeholder endpoint for ClaudeCliSubscription InferenceBackend rows.
pub const DEFAULT_BACKEND_ENDPOINT: &str = "claude-cli://subscription";
/// Default client-facing model slug for ClaudeCliSubscription.
pub const DEFAULT_MODEL_ID: &str = "claude-sonnet-5";

pub fn default_backend_endpoint() -> &'static str {
    DEFAULT_BACKEND_ENDPOINT
}

pub fn default_model_name() -> &'static str {
    DEFAULT_MODEL_ID
}

pub struct ClaudeSubscriptionClient<S: BearerSource = DbCredentialBearer> {
    pub(crate) bearer: Arc<S>,
    pub(crate) http: ReqwestClient,
}

impl<S: BearerSource> Clone for ClaudeSubscriptionClient<S> {
    fn clone(&self) -> Self {
        Self {
            bearer: self.bearer.clone(),
            http: self.http.clone(),
        }
    }
}

impl<S: BearerSource> fmt::Debug for ClaudeSubscriptionClient<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClaudeSubscriptionClient")
            .finish_non_exhaustive()
    }
}

impl ClaudeSubscriptionClient<DbCredentialBearer> {
    /// Look the agent's Claude credential up once and bind the shared bearer.
    /// Fails closed with the `claude-login` hint when no enabled credential exists.
    pub async fn build(node: Arc<EmbeddedNode>, agent_did: &str) -> Result<Self> {
        let (bearer, _credential) = crate::oauth_http::bootstrap_oauth_client(
            node,
            agent_did,
            CLAUDE_OAUTH_PROVIDER,
            OAuthRefreshKind::Claude,
            CLAUDE_OAUTH_PRODUCT,
        )
        .await?;
        Ok(Self {
            bearer,
            http: ReqwestClient::new(),
        })
    }
}

impl<S: BearerSource> ClaudeSubscriptionClient<S> {
    #[cfg(test)]
    pub(crate) fn with_bearer(bearer: Arc<S>) -> Self {
        Self {
            bearer,
            http: ReqwestClient::new(),
        }
    }
}

impl<S: BearerSource + 'static> CompletionClient for ClaudeSubscriptionClient<S> {
    type CompletionModel = ClaudeSubscriptionModel<S>;
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct ClaudeStreamResponse {
    pub usage: Option<Usage>,
}

impl GetTokenUsage for ClaudeStreamResponse {
    fn token_usage(&self) -> Option<Usage> {
        self.usage
    }
}

pub struct ClaudeSubscriptionModel<S: BearerSource = DbCredentialBearer> {
    model: String,
    bearer: Arc<S>,
    http: ReqwestClient,
}

impl<S: BearerSource> Clone for ClaudeSubscriptionModel<S> {
    fn clone(&self) -> Self {
        Self {
            model: self.model.clone(),
            bearer: self.bearer.clone(),
            http: self.http.clone(),
        }
    }
}

impl<S: BearerSource> fmt::Debug for ClaudeSubscriptionModel<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClaudeSubscriptionModel")
            .field("model", &self.model)
            .finish_non_exhaustive()
    }
}

fn surface_of(request: &CompletionRequest) -> HashSet<String> {
    request.tools.iter().map(|tool| tool.name.clone()).collect()
}

impl<S: BearerSource + 'static> CompletionModel for ClaudeSubscriptionModel<S> {
    type Response = ();
    type StreamingResponse = ClaudeStreamResponse;
    type Client = ClaudeSubscriptionClient<S>;

    fn make(client: &Self::Client, model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            bearer: client.bearer.clone(),
            http: client.http.clone(),
        }
    }

    /// Non-streaming turn: drains the same Messages stream `stream` returns
    /// and folds the text. Calls `stream_messages` directly so the only
    /// provider invocations in this crate stay inside the owned loop.
    ///
    /// Text-only by construction: the owned loop uses `stream()`, so this
    /// path never maps `tool_use`; a tool-only turn here surfaces as the
    /// empty-assistant-text error below.
    async fn completion(
        &self,
        request: CompletionRequest,
    ) -> Result<CompletionResponse<Self::Response>, CompletionError> {
        let surface = surface_of(&request);
        let stream = crate::claude_messages::stream_messages(
            &self.model,
            &request,
            surface,
            self.bearer.as_ref(),
            &self.http,
        )
        .await?;
        futures::pin_mut!(stream);
        let mut text = String::new();
        let mut usage = Usage::new();
        while let Some(item) = stream.next().await {
            match item? {
                RawStreamingChoice::Message(chunk) => text.push_str(&chunk),
                RawStreamingChoice::FinalResponse(raw) => {
                    if let Some(reported) = raw.token_usage() {
                        usage = reported;
                    }
                    break;
                }
                _ => {}
            }
        }
        if text.trim().is_empty() {
            return Err(CompletionError::ProviderError(
                "Claude Messages returned empty assistant text".to_string(),
            ));
        }
        Ok(CompletionResponse {
            choice: OneOrMany::one(rig::completion::AssistantContent::text(text)),
            usage,
            raw_response: (),
            message_id: None,
        })
    }

    async fn stream(
        &self,
        request: CompletionRequest,
    ) -> Result<StreamingCompletionResponse<Self::StreamingResponse>, CompletionError> {
        let surface = surface_of(&request);
        let stream = crate::claude_messages::stream_messages(
            &self.model,
            &request,
            surface,
            self.bearer.as_ref(),
            &self.http,
        )
        .await?;
        Ok(StreamingCompletionResponse::stream(Box::pin(stream)))
    }
}

/// Test bearer: a fixed token, or a fixed error; counts calls and invalidations.
#[cfg(test)]
pub(crate) struct StaticBearer {
    token: std::result::Result<String, String>,
    calls: std::sync::atomic::AtomicUsize,
    invalidations: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl StaticBearer {
    pub(crate) fn new(token: &str) -> Self {
        Self {
            token: Ok(token.to_string()),
            calls: Default::default(),
            invalidations: Default::default(),
        }
    }

    pub(crate) fn failing(message: &str) -> Self {
        Self {
            token: Err(message.to_string()),
            calls: Default::default(),
            invalidations: Default::default(),
        }
    }

    pub(crate) fn bearer_calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn invalidations(&self) -> usize {
        self.invalidations.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
impl BearerSource for StaticBearer {
    async fn current_bearer(&self) -> Result<String> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.token
            .clone()
            .map_err(|message| anyhow::anyhow!(message))
    }

    async fn invalidate(&self) {
        self.invalidations
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
#[path = "claude_subscription/tests.rs"]
mod tests;
