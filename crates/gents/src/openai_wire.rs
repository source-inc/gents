use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::backend_provider::BackendProviderKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpenAiWireApi {
    #[serde(rename = "responses")]
    Responses,
    #[serde(rename = "chat_completions")]
    ChatCompletions,
}

impl OpenAiWireApi {
    pub fn parse_optional(value: Option<&str>) -> Result<Option<Self>> {
        match value.map(str::trim).filter(|value| !value.is_empty()) {
            None => Ok(None),
            Some("responses") => Ok(Some(Self::Responses)),
            Some("chat_completions") | Some("chat-completions") => Ok(Some(Self::ChatCompletions)),
            Some(other) => anyhow::bail!("unknown OpenAI wire API {other}"),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Responses => "responses",
            Self::ChatCompletions => "chat_completions",
        }
    }

    pub fn effective_for_provider(
        provider_kind: BackendProviderKind,
        configured: Option<Self>,
        backend_id: &str,
    ) -> Self {
        match provider_kind {
            BackendProviderKind::OpenAiCompatible => configured.unwrap_or(Self::ChatCompletions),
            BackendProviderKind::OpenRouter => {
                if let Some(value) = configured {
                    tracing::warn!(
                        backend_id = %backend_id,
                        openai_wire_api = value.as_str(),
                        provider_kind = %provider_kind,
                        "openai_wire_api is ignored for this backend provider"
                    );
                }
                Self::ChatCompletions
            }
            BackendProviderKind::ChatGptCodex => {
                if let Some(value) = configured {
                    tracing::warn!(
                        backend_id = %backend_id,
                        openai_wire_api = value.as_str(),
                        provider_kind = %provider_kind,
                        "openai_wire_api is ignored for this backend provider"
                    );
                }
                Self::Responses
            }
            // The Grok proxy serves both wires and the official client picks
            // per model; default Responses, honor an explicit override.
            BackendProviderKind::XaiGrokOAuth => configured.unwrap_or(Self::Responses),
            // Claude is not an OpenAI wire provider; ChatCompletions is only a
            // placeholder so SamplingConfig / loop_config keep compiling. The
            // Messages HTTP wire (`claude_messages`) ignores openai_wire_api.
            BackendProviderKind::ClaudeCliSubscription => {
                if let Some(value) = configured {
                    tracing::warn!(
                        backend_id = %backend_id,
                        openai_wire_api = value.as_str(),
                        provider_kind = %provider_kind,
                        "openai_wire_api is ignored for this backend provider"
                    );
                }
                Self::ChatCompletions
            }
        }
    }
}

impl std::fmt::Display for OpenAiWireApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_effective_wire_api_by_provider() {
        assert_eq!(
            OpenAiWireApi::effective_for_provider(
                BackendProviderKind::OpenAiCompatible,
                None,
                "local",
            ),
            OpenAiWireApi::ChatCompletions
        );
        assert_eq!(
            OpenAiWireApi::effective_for_provider(
                BackendProviderKind::OpenAiCompatible,
                Some(OpenAiWireApi::Responses),
                "local",
            ),
            OpenAiWireApi::Responses
        );
        assert_eq!(
            OpenAiWireApi::effective_for_provider(
                BackendProviderKind::OpenRouter,
                Some(OpenAiWireApi::Responses),
                "openrouter",
            ),
            OpenAiWireApi::ChatCompletions
        );
        assert_eq!(
            OpenAiWireApi::effective_for_provider(
                BackendProviderKind::ChatGptCodex,
                Some(OpenAiWireApi::ChatCompletions),
                "codex",
            ),
            OpenAiWireApi::Responses
        );
        assert_eq!(
            OpenAiWireApi::effective_for_provider(
                BackendProviderKind::ClaudeCliSubscription,
                Some(OpenAiWireApi::Responses),
                "claude",
            ),
            OpenAiWireApi::ChatCompletions
        );
    }

    #[test]
    fn xai_grok_oauth_defaults_to_responses_but_honors_configured_wire() {
        // The Grok proxy serves both wires; the official client picks per
        // model. Default to Responses, but let operators pin chat_completions.
        assert_eq!(
            OpenAiWireApi::effective_for_provider(BackendProviderKind::XaiGrokOAuth, None, "grok",),
            OpenAiWireApi::Responses
        );
        assert_eq!(
            OpenAiWireApi::effective_for_provider(
                BackendProviderKind::XaiGrokOAuth,
                Some(OpenAiWireApi::ChatCompletions),
                "grok",
            ),
            OpenAiWireApi::ChatCompletions
        );
    }
}
