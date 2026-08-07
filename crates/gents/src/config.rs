use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::agent::completion_retry::CompletionRetryProfileFields;
use crate::backend_provider::BackendProviderKind;
use crate::compaction::CompactionStrategy;
use crate::identity::{AgentIdentity, AgentPrincipal};
use crate::openai_wire::OpenAiWireApi;
use crate::tool_surface::BehaviorToolConfig;

pub const DEFAULT_CONTEXT_WINDOW: usize = 131_072;
pub const DEFAULT_MAX_OUTPUT_TOKENS: usize = 32_768;
pub const DEFAULT_MAX_TURNS: usize = 250;
pub const DEFAULT_STREAM_BATCH_MS: u64 = 1_000;
pub const DEFAULT_COMPACTION_THRESHOLD: f64 = 0.75;
/// Output budget for the internal compaction summary completion — independent
/// of the user turn's `max_output_tokens` (#1017).
pub const DEFAULT_COMPACTION_SUMMARY_MAX_OUTPUT_TOKENS: usize = 32_768;
pub const MAX_COMPACTION_SUMMARY_MAX_OUTPUT_TOKENS: usize = 32_768;
/// Most file paths rendered per list in the formatted compaction summary.
pub const DEFAULT_COMPACTION_SUMMARY_FILE_LIST_MAX: usize = 100;
pub const MAX_COMPACTION_SUMMARY_FILE_LIST_MAX: usize = 1_000;
pub const DEFAULT_STREAM_LIVENESS_TIMEOUT_SECS: u64 = 1_800;
pub const DEFAULT_DEADLINE_DURATION_SECS: u64 = 1_800;
pub const DEFAULT_MODEL_NAME: &str = "default";

/// Runtime configuration for one loaded behavior executor.
///
/// Mirrors the Lean `Identity.Behavior` record. Holds an
/// `Arc<AgentPrincipal>` back-reference; the principal owns the
/// signing identity used for all DefraDB ops issued for this
/// behavior. Two behaviors sharing the same principal Arc share the
/// same actor DID (Lean's `behavior_id_determines_principal` is
/// structural at the type level here).
#[derive(Clone)]
pub struct AgentBehavior {
    pub behavior_id: String,
    pub principal: Arc<AgentPrincipal>,
    pub backend_id: Option<String>,
    pub backend_provider_kind: BackendProviderKind,
    pub openai_wire_api: OpenAiWireApi,
    pub backend_endpoint: String,
    pub backend_api_key: Option<String>,
    pub backend_api_key_env_var: Option<String>,
    pub model_name: String,
    pub context_window: usize,
    pub max_output_tokens: usize,
    pub max_turns: usize,
    pub system_prompt: String,
    pub request_context_template: Option<String>,
    pub tools: BehaviorToolConfig,
    pub compaction_threshold: f64,
    pub compaction_strategy: CompactionStrategy,
    pub stream_batch_ms: u64,
    pub stream_liveness_timeout: Duration,
    pub deadline_duration: Duration,
    pub completion_retry: CompletionRetryProfileFields,
    pub sampling: SamplingConfig,
    /// Effective skill set for this behavior (decision D5), resolved at
    /// snapshot-build time. Their instructions compose into the prompt
    /// preamble; their tool deps are intersected with the tool ceiling and
    /// never widen it (decision D3). See `crate::skills`.
    pub skills: Vec<crate::skills::Skill>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReasoningEffort {
    None,
    Minimal,
    Low,
    Medium,
    High,
    XHigh,
    Max,
    Ultra,
}

impl ReasoningEffort {
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "none" => Ok(Self::None),
            "minimal" => Ok(Self::Minimal),
            "low" => Ok(Self::Low),
            "medium" => Ok(Self::Medium),
            "high" => Ok(Self::High),
            "xhigh" => Ok(Self::XHigh),
            "max" => Ok(Self::Max),
            "ultra" => Ok(Self::Ultra),
            _ => anyhow::bail!(
                "reasoning_effort must be one of: none, minimal, low, medium, high, xhigh, max, ultra"
            ),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::XHigh => "xhigh",
            Self::Max => "max",
            Self::Ultra => "ultra",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct SamplingConfig {
    pub temperature: Option<f64>,
    pub top_p: Option<f64>,
    pub top_k: Option<i64>,
    pub seed: Option<i64>,
    /// Sampling knobs the provider takes as extra body params (#649). rig's
    /// `CompletionRequest` models only `temperature`, so everything else rides
    /// `additional_params` — see [`SamplingConfig::additional_params`].
    pub min_p: Option<f64>,
    pub frequency_penalty: Option<f64>,
    pub presence_penalty: Option<f64>,
    pub repetition_penalty: Option<f64>,
    pub max_tokens: Option<u64>,
    pub reasoning_effort: Option<ReasoningEffort>,
}

impl SamplingConfig {
    pub fn validate_for_provider(
        self,
        provider_kind: BackendProviderKind,
        openai_wire_api: OpenAiWireApi,
    ) -> Result<()> {
        let Some(seed) = self.seed else {
            return Ok(());
        };
        if seed < 0 {
            anyhow::bail!("sampling seed must be non-negative");
        }
        if !matches!(
            (provider_kind, openai_wire_api),
            (
                BackendProviderKind::OpenAiCompatible,
                OpenAiWireApi::ChatCompletions
            ) | (BackendProviderKind::OpenRouter, _)
        ) {
            anyhow::bail!(
                "sampling seed is unsupported by provider {} on the {} wire",
                provider_kind,
                openai_wire_api
            );
        }
        Ok(())
    }

    pub fn is_empty(self) -> bool {
        self.temperature.is_none()
            && self.top_p.is_none()
            && self.top_k.is_none()
            && self.seed.is_none()
            && self.min_p.is_none()
            && self.frequency_penalty.is_none()
            && self.presence_penalty.is_none()
            && self.repetition_penalty.is_none()
            && self.max_tokens.is_none()
            && self.reasoning_effort.is_none()
    }

    /// The sampling knobs that must travel as provider body params.
    ///
    /// `temperature` and `max_tokens` are modeled fields on rig's request, and
    /// `reasoning_effort` needs a provider-specific wire shape in
    /// `completion_factory`; everything else is emitted here and deep-merged
    /// into `additional_params` at the request boundary. A `None` knob emits
    /// nothing at all — the served model's own default stands, which is the
    /// pre-#649 behavior for every profile that does not pin a value.
    pub fn additional_params(self) -> Option<serde_json::Value> {
        let mut params = serde_json::Map::new();
        if let Some(top_p) = self.top_p {
            params.insert("top_p".to_string(), serde_json::json!(top_p));
        }
        if let Some(top_k) = self.top_k {
            params.insert("top_k".to_string(), serde_json::json!(top_k));
        }
        if let Some(seed) = self.seed {
            params.insert("seed".to_string(), serde_json::json!(seed));
        }
        if let Some(min_p) = self.min_p {
            params.insert("min_p".to_string(), serde_json::json!(min_p));
        }
        if let Some(frequency_penalty) = self.frequency_penalty {
            params.insert(
                "frequency_penalty".to_string(),
                serde_json::json!(frequency_penalty),
            );
        }
        if let Some(presence_penalty) = self.presence_penalty {
            params.insert(
                "presence_penalty".to_string(),
                serde_json::json!(presence_penalty),
            );
        }
        if let Some(repetition_penalty) = self.repetition_penalty {
            params.insert(
                "repetition_penalty".to_string(),
                serde_json::json!(repetition_penalty),
            );
        }

        (!params.is_empty()).then_some(serde_json::Value::Object(params))
    }
}

impl std::fmt::Debug for AgentBehavior {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentBehavior")
            .field("behavior_id", &self.behavior_id)
            .field("principal_did", &self.principal.agent_did)
            .field("backend_id", &self.backend_id)
            .field("backend_provider_kind", &self.backend_provider_kind)
            .field("openai_wire_api", &self.openai_wire_api)
            .field("backend_endpoint", &self.backend_endpoint)
            .field(
                "backend_api_key",
                &self.backend_api_key.as_ref().map(|_| "<redacted>"),
            )
            .field("backend_api_key_env_var", &self.backend_api_key_env_var)
            .field("model_name", &self.model_name)
            .field("context_window", &self.context_window)
            .field("max_output_tokens", &self.max_output_tokens)
            .field("max_turns", &self.max_turns)
            .field("system_prompt", &self.system_prompt)
            .field("request_context_template", &self.request_context_template)
            .field("tools", &self.tools)
            .field("compaction_threshold", &self.compaction_threshold)
            .field("compaction_strategy", &self.compaction_strategy)
            .field("stream_batch_ms", &self.stream_batch_ms)
            .field("stream_liveness_timeout", &self.stream_liveness_timeout)
            .field("deadline_duration", &self.deadline_duration)
            .field("completion_retry", &self.completion_retry)
            .field("sampling", &self.sampling)
            // Included so the runtime configuration fingerprint (which hashes
            // `{behavior:?}`) changes when a behavior's effective skills change,
            // letting the control watcher reconcile live skill updates (#340).
            .field("skills", &self.skills)
            .finish()
    }
}

impl AgentBehavior {
    /// Returns the principal's agent_did.
    pub fn agent_did(&self) -> &str {
        &self.principal.agent_did
    }

    /// Returns the principal's signing identity.
    ///
    /// This is the only way to obtain an `Arc<dyn AgentIdentity>` for
    /// a behavior; the behavior itself does not hold one. Two
    /// behaviors sharing an `Arc<AgentPrincipal>` return identical
    /// clones, so DefraDB ACP receives the same actor for both —
    /// satisfying Lean's `RespectsPrincipal` predicate.
    pub fn principal_identity(&self) -> &Arc<dyn AgentIdentity> {
        &self.principal.identity
    }

    pub fn resolve_backend_api_key(&self) -> Result<Option<String>> {
        if let Some(api_key) = normalize_optional_secret(self.backend_api_key.as_deref()) {
            return Ok(Some(api_key.to_string()));
        }

        if let Some(env_var) = normalize_optional_env_var(self.backend_api_key_env_var.as_deref()) {
            let value = std::env::var(env_var).with_context(|| {
                format!(
                    "backend {} for behavior {} requires environment variable {}",
                    self.backend_id.as_deref().unwrap_or("<unbound>"),
                    self.behavior_id,
                    env_var
                )
            })?;
            let value = value.trim();
            if value.is_empty() {
                anyhow::bail!(
                    "backend {} for behavior {} resolved empty API key from environment variable {}",
                    self.backend_id.as_deref().unwrap_or("<unbound>"),
                    self.behavior_id,
                    env_var
                );
            }
            return Ok(Some(value.to_string()));
        }

        Ok(None)
    }

    pub fn completion_client_api_key(&self) -> Result<String> {
        Ok(self
            .resolve_backend_api_key()?
            .unwrap_or_else(|| "no-key".to_string()))
    }
}

fn normalize_optional_env_var(value: Option<&str>) -> Option<&str> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then_some(trimmed)
    })
}

fn normalize_optional_secret(value: Option<&str>) -> Option<&str> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then_some(trimmed)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::KeyIdentity;

    fn stub_principal() -> Arc<AgentPrincipal> {
        let identity = Arc::new(
            KeyIdentity::load_or_create(
                std::env::temp_dir().join(format!("config-behavior-{}.key", uuid::Uuid::new_v4())),
                None,
            )
            .unwrap(),
        );
        Arc::new(AgentPrincipal {
            agent_did: identity.did().to_string(),
            identity,
            default_behavior_id: String::new(),
            display_name: None,
            enabled: true,
        })
    }

    fn behavior_with_wire(openai_wire_api: OpenAiWireApi) -> AgentBehavior {
        AgentBehavior {
            behavior_id: "general".to_string(),
            principal: stub_principal(),
            backend_id: Some("backend-general".to_string()),
            backend_provider_kind: BackendProviderKind::OpenAiCompatible,
            openai_wire_api,
            backend_endpoint: "http://127.0.0.1:8999/v1".to_string(),
            backend_api_key: None,
            backend_api_key_env_var: None,
            model_name: DEFAULT_MODEL_NAME.to_string(),
            context_window: DEFAULT_CONTEXT_WINDOW,
            max_output_tokens: DEFAULT_MAX_OUTPUT_TOKENS,
            max_turns: DEFAULT_MAX_TURNS,
            system_prompt: "system".to_string(),
            request_context_template: None,
            tools: BehaviorToolConfig::meta_only(),
            compaction_threshold: DEFAULT_COMPACTION_THRESHOLD,
            compaction_strategy: CompactionStrategy::StripThenSummarize,
            stream_batch_ms: DEFAULT_STREAM_BATCH_MS,
            stream_liveness_timeout: Duration::from_secs(DEFAULT_STREAM_LIVENESS_TIMEOUT_SECS),
            deadline_duration: Duration::from_secs(DEFAULT_DEADLINE_DURATION_SECS),
            completion_retry: CompletionRetryProfileFields::default(),
            sampling: SamplingConfig::default(),
            skills: Vec::new(),
        }
    }

    #[test]
    fn default_max_turns_supports_long_running_agents() {
        assert_eq!(DEFAULT_MAX_TURNS, 250);
    }

    #[test]
    fn reasoning_effort_accepts_provider_vocabulary() {
        for value in [
            "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra",
        ] {
            let effort = ReasoningEffort::parse(value).expect("known effort must parse");
            assert_eq!(effort.as_str(), value);
        }
        assert!(ReasoningEffort::parse("extreme").is_err());
    }

    /// TA-1 (#566 review): `openai_wire_api` must appear in the manual `Debug`
    /// impl, because the runtime configuration fingerprint hashes
    /// `format!("{behavior:?}")` (see `runtime_snapshot::configuration_fingerprint`).
    /// Without the Debug field, switching a backend's wire API would not change the
    /// fingerprint, so the control watcher would never reconcile the change into a new
    /// generation. Deleting the `.field("openai_wire_api", …)` line makes these equal.
    #[test]
    fn debug_distinguishes_openai_wire_api_for_reconcile_fingerprint() {
        let chat = format!("{:?}", behavior_with_wire(OpenAiWireApi::ChatCompletions));
        let responses = format!("{:?}", behavior_with_wire(OpenAiWireApi::Responses));
        assert_ne!(
            chat, responses,
            "openai_wire_api must be in AgentBehavior Debug so the runtime fingerprint \
             changes when the wire API changes"
        );
        assert!(chat.contains("ChatCompletions"));
        assert!(responses.contains("Responses"));
    }
}
