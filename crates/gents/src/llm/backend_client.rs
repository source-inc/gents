//! Single owner of "build the provider completion client for a behavior's
//! `BackendProviderKind`" — the daemon (`agent/runtime/context.rs`) and
//! one-shot (`oneshot.rs`) used to carry independent copies of this
//! four-way match, and only the daemon wrapped OAuth-branch client
//! construction in a build timeout. Both callers now go through
//! [`build_backend_client`], which applies that timeout to every OAuth build
//! regardless of caller.
//!
//! Each `BackendProviderKind` × wire-API combination produces a distinct
//! concrete `rig` client type, so the result is a small closed enum
//! ([`BackendClient`]) rather than a `dyn` client: callers match on it once to
//! hand the concrete value to their own generic completion-loop entry point
//! (`run_behavior_with_client` / `run_oneshot_with_completion_client`), which
//! is the only place left that needs to be generic over the client type.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use defra_node::EmbeddedNode;

use crate::backend_provider::BackendProviderKind;
use crate::config::AgentBehavior;

/// A built provider completion client, tagged by which
/// `BackendProviderKind` × wire-API combination produced it.
pub(crate) enum BackendClient {
    OpenAiChatCompletions(
        rig::providers::openai::CompletionsClient<
            crate::inference_http::SessionTaggingHttpClient<
                crate::rendered_request::RenderedRequestCapturingHttpClient,
            >,
        >,
    ),
    OpenAiResponses(
        rig::providers::openai::Client<
            crate::inference_http::SessionTaggingHttpClient<
                crate::inference_http::ResponsesNormalizingHttpClient<
                    crate::rendered_request::RenderedRequestCapturingHttpClient,
                >,
            >,
        >,
    ),
    OpenRouter(
        rig::providers::openrouter::Client<
            crate::rendered_request::RenderedRequestCapturingHttpClient,
        >,
    ),
    ChatGptCodex(
        rig::providers::openai::Client<
            crate::chatgpt_codex::ChatGptCodexHttpClient<
                crate::oauth_credential::DbCredentialBearer,
                crate::rendered_request::RenderedRequestCapturingHttpClient,
            >,
        >,
    ),
    XaiGrokChatCompletions(
        rig::providers::openai::CompletionsClient<
            crate::xai_grok_oauth::CapturingXaiGrokOAuthHttpClient,
        >,
    ),
    XaiGrokResponses(
        rig::providers::openai::Client<crate::xai_grok_oauth::CapturingXaiGrokOAuthHttpClient>,
    ),
    ClaudeSubscription(
        crate::claude_subscription::ClaudeSubscriptionClient<
            crate::oauth_credential::DbCredentialBearer,
        >,
    ),
}

/// Build the provider completion client for `behavior`'s
/// `backend_provider_kind` (and, where the provider has one, its configured
/// `openai_wire_api`).
///
/// OAuth-branch construction (ChatGPT Codex, Grok/xAI OAuth) hits DefraDB for
/// the agent's `OAuthCredential` and is bounded by `build_timeout` so a wedged
/// lookup cannot hang startup forever — the same protection the daemon has
/// always applied, now shared by one-shot too (#1338).
pub(crate) async fn build_backend_client(
    node: Arc<EmbeddedNode>,
    behavior: &AgentBehavior,
    api_key: &str,
    build_timeout: Duration,
) -> Result<BackendClient> {
    match behavior.backend_provider_kind {
        BackendProviderKind::OpenAiCompatible => {
            let build_context = format!(
                "building OpenAI-compatible completion client for behavior {} against {}",
                behavior.behavior_id, behavior.backend_endpoint
            );
            if behavior.openai_wire_api == crate::OpenAiWireApi::ChatCompletions {
                let client = crate::inference_http::build_openai_chat_completions_client(
                    api_key,
                    &behavior.backend_endpoint,
                    crate::inference_http::SessionTaggingHttpClient::new(
                        crate::rendered_request::RenderedRequestCapturingHttpClient::default(),
                    ),
                )
                .with_context(|| build_context.clone())?;
                Ok(BackendClient::OpenAiChatCompletions(client))
            } else {
                let client = crate::inference_http::build_openai_responses_client(
                    api_key,
                    &behavior.backend_endpoint,
                    crate::inference_http::SessionTaggingHttpClient::new(
                        crate::inference_http::ResponsesNormalizingHttpClient::new(
                            crate::rendered_request::RenderedRequestCapturingHttpClient::default(),
                        ),
                    ),
                    Default::default(),
                )
                .with_context(|| build_context.clone())?;
                Ok(BackendClient::OpenAiResponses(client))
            }
        }
        BackendProviderKind::OpenRouter => {
            let build_context = format!(
                "building OpenRouter completion client for behavior {} against {}",
                behavior.behavior_id, behavior.backend_endpoint
            );
            let client: rig::providers::openrouter::Client<
                crate::rendered_request::RenderedRequestCapturingHttpClient,
            > = rig::providers::openrouter::Client::builder()
                .api_key(api_key)
                .base_url(&behavior.backend_endpoint)
                .http_client(crate::rendered_request::RenderedRequestCapturingHttpClient::default())
                .build()
                .with_context(|| build_context.clone())?;
            Ok(BackendClient::OpenRouter(client))
        }
        BackendProviderKind::ChatGptCodex => {
            let client = tokio::time::timeout(
                build_timeout,
                crate::chatgpt_codex::build_responses_client(
                    node,
                    behavior.agent_did(),
                    &behavior.backend_endpoint,
                ),
            )
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "timed out after {build_timeout:?} building the ChatGPT Codex completion client"
                )
            })
            .and_then(|result| result)
            .with_context(|| {
                format!(
                    "building ChatGPT Codex completion client for behavior {} against {}",
                    behavior.behavior_id, behavior.backend_endpoint
                )
            })?;
            Ok(BackendClient::ChatGptCodex(client))
        }
        BackendProviderKind::XaiGrokOAuth => {
            let build_context = format!(
                "building Grok OAuth completion client for behavior {} against {}",
                behavior.behavior_id, behavior.backend_endpoint
            );
            let timeout_error = || {
                anyhow::anyhow!(
                    "timed out after {build_timeout:?} building the Grok OAuth completion client"
                )
            };
            if behavior.openai_wire_api == crate::OpenAiWireApi::ChatCompletions {
                let client = tokio::time::timeout(
                    build_timeout,
                    crate::xai_grok_oauth::build_chat_completions_client(
                        node,
                        behavior.agent_did(),
                        &behavior.backend_endpoint,
                    ),
                )
                .await
                .map_err(|_| timeout_error())
                .and_then(|result| result)
                .with_context(|| build_context.clone())?;
                Ok(BackendClient::XaiGrokChatCompletions(client))
            } else {
                let client = tokio::time::timeout(
                    build_timeout,
                    crate::xai_grok_oauth::build_responses_client(
                        node,
                        behavior.agent_did(),
                        &behavior.backend_endpoint,
                    ),
                )
                .await
                .map_err(|_| timeout_error())
                .and_then(|result| result)
                .with_context(|| build_context.clone())?;
                Ok(BackendClient::XaiGrokResponses(client))
            }
        }
        BackendProviderKind::ClaudeCliSubscription => {
            let client = tokio::time::timeout(
                build_timeout,
                crate::claude_subscription::ClaudeSubscriptionClient::build(
                    node,
                    behavior.agent_did(),
                ),
            )
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "timed out after {build_timeout:?} building the Claude subscription completion client"
                )
            })
            .and_then(|result| result)
            .with_context(|| {
                format!(
                    "building Claude subscription completion client for behavior {}",
                    behavior.behavior_id
                )
            })?;
            Ok(BackendClient::ClaudeSubscription(client))
        }
    }
}

/// Dispatch a built `BackendClient` to `$body`, binding the unwrapped
/// concrete client to `$c`. Single owner of the six-arm match every caller
/// otherwise has to repeat: each `BackendProviderKind` × wire-API
/// combination produces a distinct concrete `rig` client type, and `$body`
/// is typically a call into a `CompletionClient`-generic continuation
/// (`run_behavior_with_client` / `run_oneshot_with_completion_client`) that
/// Rust monomorphizes once per concrete type it's invoked with — so the
/// match itself can't become a plain function, only written once.
macro_rules! with_backend_client {
    ($client:expr, |$c:ident| $body:expr) => {
        match $client {
            $crate::llm::backend_client::BackendClient::OpenAiChatCompletions($c) => $body,
            $crate::llm::backend_client::BackendClient::OpenAiResponses($c) => $body,
            $crate::llm::backend_client::BackendClient::OpenRouter($c) => $body,
            $crate::llm::backend_client::BackendClient::ChatGptCodex($c) => $body,
            $crate::llm::backend_client::BackendClient::XaiGrokChatCompletions($c) => $body,
            $crate::llm::backend_client::BackendClient::XaiGrokResponses($c) => $body,
            $crate::llm::backend_client::BackendClient::ClaudeSubscription($c) => $body,
        }
    };
}
pub(crate) use with_backend_client;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::PendingAgentBehavior;
    use crate::identity::KeyIdentity;

    async fn test_node() -> Arc<EmbeddedNode> {
        Arc::new(EmbeddedNode::builder().build().await.unwrap())
    }

    fn test_behavior(kind: BackendProviderKind, wire_api: crate::OpenAiWireApi) -> AgentBehavior {
        let identity = KeyIdentity::load_or_create(
            std::env::temp_dir().join(format!("backend-client-table-{}.key", uuid::Uuid::new_v4())),
            None,
        )
        .unwrap();
        let mut behavior = PendingAgentBehavior::new("backend-client-table")
            .build_with_identity_for_test(identity);
        behavior.backend_provider_kind = kind;
        behavior.openai_wire_api = wire_api;
        behavior.backend_endpoint = "http://127.0.0.1:1/v1".to_string();
        behavior
    }

    /// OpenAI-compatible and OpenRouter clients are built synchronously from
    /// strings (api key/endpoint) with no I/O, so the table test can assert
    /// on the exact concrete variant the shared constructor returns for every
    /// `BackendProviderKind` that doesn't need a live OAuthCredential.
    #[tokio::test]
    async fn each_openai_shaped_provider_kind_yields_the_expected_client_variant() {
        let node = test_node().await;

        let chat = test_behavior(
            BackendProviderKind::OpenAiCompatible,
            crate::OpenAiWireApi::ChatCompletions,
        );
        let client = build_backend_client(node.clone(), &chat, "key", Duration::from_secs(1))
            .await
            .expect("chat completions client builds without I/O");
        assert!(matches!(client, BackendClient::OpenAiChatCompletions(_)));

        let responses = test_behavior(
            BackendProviderKind::OpenAiCompatible,
            crate::OpenAiWireApi::Responses,
        );
        let client = build_backend_client(node.clone(), &responses, "key", Duration::from_secs(1))
            .await
            .expect("responses client builds without I/O");
        assert!(matches!(client, BackendClient::OpenAiResponses(_)));

        let mut openrouter = test_behavior(
            BackendProviderKind::OpenRouter,
            crate::OpenAiWireApi::ChatCompletions,
        );
        openrouter.backend_endpoint = "https://openrouter.ai/api/v1".to_string();
        let client = build_backend_client(node.clone(), &openrouter, "key", Duration::from_secs(1))
            .await
            .expect("openrouter client builds without I/O");
        assert!(matches!(client, BackendClient::OpenRouter(_)));
    }

    async fn seed_oauth_credential(node: &EmbeddedNode, behavior: &AgentBehavior, provider: &str) {
        let agent_did = behavior.agent_did();
        let credential = crate::oauth_credential::OAuthCredential {
            doc_id: None,
            credential_id: crate::oauth_credential::oauth_credential_id(agent_did, provider),
            agent_did: agent_did.to_string(),
            provider: provider.to_string(),
            access_token: "access-token".to_string(),
            refresh_token: "refresh-token".to_string(),
            id_token: None,
            account_id: None,
            chatgpt_plan_type: None,
            is_fedramp: false,
            access_token_expires_at: chrono::Utc::now() + chrono::Duration::hours(1),
            last_refresh: None,
            enabled: true,
        };
        crate::oauth_credential::upsert_oauth_credential(node, &credential)
            .await
            .expect("test OAuthCredential must persist");
    }

    /// Seed credentials so every OAuth route reaches a concrete client. A
    /// missing-credential assertion cannot distinguish xAI Chat Completions
    /// from Responses because both fail in their shared bootstrap preamble.
    #[tokio::test]
    async fn each_oauth_provider_kind_yields_the_expected_client_variant() {
        let node = test_node().await;
        crate::migration::ensure_all_runtime_migrations(node.clone())
            .await
            .unwrap();

        let codex = test_behavior(
            BackendProviderKind::ChatGptCodex,
            crate::OpenAiWireApi::Responses,
        );
        seed_oauth_credential(
            node.as_ref(),
            &codex,
            crate::chatgpt_codex::CHATGPT_CODEX_PROVIDER,
        )
        .await;
        let client = build_backend_client(node.clone(), &codex, "key", Duration::from_secs(5))
            .await
            .expect("Codex client builds from the seeded credential without network I/O");
        assert!(matches!(client, BackendClient::ChatGptCodex(_)));

        let xai_chat = test_behavior(
            BackendProviderKind::XaiGrokOAuth,
            crate::OpenAiWireApi::ChatCompletions,
        );
        seed_oauth_credential(
            node.as_ref(),
            &xai_chat,
            crate::xai_grok_oauth::XAI_OAUTH_PROVIDER,
        )
        .await;
        let client = build_backend_client(node.clone(), &xai_chat, "key", Duration::from_secs(5))
            .await
            .expect("Grok Chat Completions client builds without network I/O");
        assert!(matches!(client, BackendClient::XaiGrokChatCompletions(_)));

        let xai_responses = test_behavior(
            BackendProviderKind::XaiGrokOAuth,
            crate::OpenAiWireApi::Responses,
        );
        seed_oauth_credential(
            node.as_ref(),
            &xai_responses,
            crate::xai_grok_oauth::XAI_OAUTH_PROVIDER,
        )
        .await;
        let client =
            build_backend_client(node.clone(), &xai_responses, "key", Duration::from_secs(5))
                .await
                .expect("Grok Responses client builds without network I/O");
        assert!(matches!(client, BackendClient::XaiGrokResponses(_)));

        let claude = test_behavior(
            BackendProviderKind::ClaudeCliSubscription,
            crate::OpenAiWireApi::ChatCompletions,
        );
        seed_oauth_credential(
            node.as_ref(),
            &claude,
            crate::claude_oauth::CLAUDE_OAUTH_PROVIDER,
        )
        .await;
        let client = build_backend_client(node.clone(), &claude, "key", Duration::from_secs(5))
            .await
            .expect("Claude subscription client builds without network I/O");
        assert!(matches!(client, BackendClient::ClaudeSubscription(_)));
    }
}
