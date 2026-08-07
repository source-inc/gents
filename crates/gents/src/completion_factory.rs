use crate::llm::ToolChoice;
use chrono::{DateTime, Utc};
use rig::client::CompletionClient;
use rig::completion::CompletionModel;

use crate::admission::{AdmissionRegistry, AdmittedCompletionClient};
use crate::agent::completion_retry::CompletionRetryPolicy;
use crate::agent::loop_stream::{AggregateTokenBudget, LoopConfig};
use crate::backend_provider::BackendProviderKind;
use crate::config::{AgentBehavior, ReasoningEffort, SamplingConfig};
use crate::lifecycle::ExecutionOrigin;
use crate::openai_wire::OpenAiWireApi;
use crate::rendered_request::CaptureScopeKind;
use crate::watcher::AgentRequest;

fn effective_max_tokens(max_output_tokens: usize, sampling_max_tokens: Option<u64>) -> Option<u64> {
    sampling_max_tokens.or_else(|| u64::try_from(max_output_tokens).ok())
}

pub(crate) fn build_admitted_model<C>(
    client: C,
    admission: AdmissionRegistry,
    behavior: &AgentBehavior,
) -> <AdmittedCompletionClient<C> as CompletionClient>::CompletionModel
where
    C: CompletionClient,
    C::CompletionModel: 'static,
    <C::CompletionModel as CompletionModel>::Response: 'static,
    <C::CompletionModel as CompletionModel>::StreamingResponse: 'static,
{
    AdmittedCompletionClient::new(client, admission).completion_model(&behavior.model_name)
}

/// Build a loop config for one completion loop.
///
/// `capture_scope` is not decoration. Every loop this factory serves issues
/// provider calls under the same `(agent_did, session_id, request_id)`, and
/// every one of them starts its turn and attempt counters at zero — the owned
/// inference loop, the compaction summarizer and its JSON fallback, title
/// generation, and the one-shot runner. The scope is what keeps their first
/// calls from colliding into one durable fact, and passing it here is what
/// makes capture the default for all of them instead of a privilege of the
/// inference path (#840).
pub(crate) fn loop_config(
    behavior: &AgentBehavior,
    preamble: String,
    tool_count: usize,
    capture_scope: CaptureScopeKind,
) -> LoopConfig {
    LoopConfig {
        preamble: Some(preamble),
        context_message: None,
        temperature: behavior.sampling.temperature,
        max_tokens: effective_max_tokens(behavior.max_output_tokens, behavior.sampling.max_tokens),
        aggregate_token_budget: None,
        additional_params: merge_optional_params(
            merge_optional_params(
                reasoning_profile_params(
                    behavior.backend_provider_kind,
                    behavior.openai_wire_api,
                    behavior.sampling.reasoning_effort,
                ),
                provider_additional_params(behavior.backend_provider_kind),
            ),
            behavior.sampling.additional_params(),
        ),
        structured_output: None,
        tool_choice: (tool_count > 0).then_some(ToolChoice::Auto),
        on_rendered_request: Some(crate::rendered_request::scope::ambient_arming_sink(
            capture_scope,
        )),
        turn_compactor: None,
        context_window: behavior.context_window,
        compaction_threshold: behavior.compaction_threshold,
        retry_policy: CompletionRetryPolicy::scheduled_default(),
        deadline: None,
        max_turns: behavior.max_turns,
    }
}

pub(crate) fn loop_config_for_request(
    behavior: &AgentBehavior,
    preamble: String,
    request: &AgentRequest,
    aggregate_token_budget: Option<AggregateTokenBudget>,
    tool_count: usize,
) -> anyhow::Result<LoopConfig> {
    let mut config = loop_config(behavior, preamble, tool_count, CaptureScopeKind::Inference);
    let sampling = sampling_for_request(behavior.sampling, request);
    sampling.validate_for_provider(behavior.backend_provider_kind, behavior.openai_wire_api)?;
    config.temperature = sampling.temperature;
    config.max_tokens = effective_max_tokens(behavior.max_output_tokens, sampling.max_tokens);
    config.aggregate_token_budget = aggregate_token_budget;
    let request_additional_params = merge_optional_params(
        sampling.additional_params(),
        request_additional_params(behavior, request),
    );
    if let Some(additional_params) = request_additional_params {
        config.additional_params =
            merge_optional_params(config.additional_params.take(), Some(additional_params));
    }
    let origin = completion_retry_origin(request.execution_origin.as_deref());
    config.retry_policy = CompletionRetryPolicy::resolve(&behavior.completion_retry, origin);
    config.deadline = parse_request_deadline(request.deadline.as_deref());
    Ok(config)
}

/// Mint the single monotone provider-usage ledger for one durable request.
/// Callers must construct it before any request-scoped provider work so
/// session compaction and the owned inference loop cannot receive independent
/// allowances.
pub(crate) fn aggregate_token_budget_for_request(
    request: &AgentRequest,
) -> anyhow::Result<Option<AggregateTokenBudget>> {
    request
        .max_total_tokens
        .map(|limit| {
            let limit = u64::try_from(limit)
                .map_err(|_| anyhow::anyhow!("max_total_tokens must be a positive integer"))?;
            if limit == 0 {
                anyhow::bail!("max_total_tokens must be a positive integer");
            }
            Ok(AggregateTokenBudget::new(limit))
        })
        .transpose()
}

fn completion_retry_origin(value: Option<&str>) -> ExecutionOrigin {
    match value {
        Some("interactive") => ExecutionOrigin::Interactive,
        _ => ExecutionOrigin::Scheduled,
    }
}

fn parse_request_deadline(value: Option<&str>) -> Option<DateTime<Utc>> {
    value
        .and_then(|value| DateTime::parse_from_rfc3339(value.trim()).ok())
        .map(|value| value.with_timezone(&Utc))
}

fn sampling_for_request(defaults: SamplingConfig, request: &AgentRequest) -> SamplingConfig {
    SamplingConfig {
        temperature: request.temperature.or(defaults.temperature),
        top_p: request.top_p.or(defaults.top_p),
        top_k: request.top_k.or(defaults.top_k),
        seed: request.seed.or(defaults.seed),
        min_p: defaults.min_p,
        frequency_penalty: defaults.frequency_penalty,
        presence_penalty: defaults.presence_penalty,
        repetition_penalty: defaults.repetition_penalty,
        reasoning_effort: defaults.reasoning_effort,
        max_tokens: request
            .max_tokens
            .and_then(|value| u64::try_from(value).ok())
            .or(defaults.max_tokens),
    }
}

fn merge_optional_params(
    left: Option<serde_json::Value>,
    right: Option<serde_json::Value>,
) -> Option<serde_json::Value> {
    match (left, right) {
        (Some(left), Some(right)) => Some(merge_json_values(left, right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn merge_json_values(left: serde_json::Value, right: serde_json::Value) -> serde_json::Value {
    match (left, right) {
        (serde_json::Value::Object(mut left), serde_json::Value::Object(right)) => {
            for (key, right_value) in right {
                let value = left
                    .remove(&key)
                    .map(|left_value| merge_json_values(left_value, right_value.clone()))
                    .unwrap_or(right_value);
                left.insert(key, value);
            }
            serde_json::Value::Object(left)
        }
        (_, right) => right,
    }
}

/// Maps the inference profile's reasoning effort into each provider's wire
/// contract. An absent profile setting injects no reasoning default, except for
/// ChatGPT Codex: it has a known Responses contract and keeps the `medium`
/// default it shipped before reasoning effort became profile configuration
/// (#540).
///
/// vLLM's OpenAI-compatible server (with a `--reasoning-parser`, e.g.
/// `deepseek_v4` on the d4f harvest server) only emits the chain-of-thought in
/// the response `message.reasoning` field when the request carries
/// `chat_template_kwargs={"enable_thinking": true}`. Without it the server
/// defaults thinking OFF and the `reasoning` field is empty, so our harvest
/// trajectories lose the model's reasoning. That local-only toggle must not be
/// sent to Responses or OpenRouter endpoints, which use the standard
/// `reasoning.effort` object instead.
///
/// The key is serialized flat into the OpenAI completion body (rig flattens
/// `additional_params`), so it reaches vLLM as a top-level `chat_template_kwargs`
/// object — exactly where the server reads it.
fn reasoning_profile_params(
    kind: BackendProviderKind,
    wire_api: OpenAiWireApi,
    reasoning_effort: Option<ReasoningEffort>,
) -> Option<serde_json::Value> {
    let Some(reasoning_effort) = reasoning_effort else {
        // Codex predates profile-configured reasoning: it has a known Responses
        // contract and shipped an unconditional `medium`. Profile plumbing may
        // override that default, never silently drop it (#540). Every other
        // backend waits for explicit configuration.
        return matches!(kind, BackendProviderKind::ChatGptCodex).then(|| {
            serde_json::json!({
                "reasoning": { "effort": ReasoningEffort::Medium.as_str() }
            })
        });
    };
    match (kind, wire_api) {
        (BackendProviderKind::OpenAiCompatible, OpenAiWireApi::ChatCompletions) => {
            let mut kwargs = serde_json::Map::from_iter([(
                "enable_thinking".to_string(),
                serde_json::Value::Bool(reasoning_effort != ReasoningEffort::None),
            )]);
            if reasoning_effort != ReasoningEffort::None {
                kwargs.insert(
                    "reasoning_effort".to_string(),
                    serde_json::Value::String(reasoning_effort.as_str().to_string()),
                );
            }
            Some(serde_json::json!({ "chat_template_kwargs": kwargs }))
        }
        (BackendProviderKind::OpenAiCompatible, OpenAiWireApi::Responses)
        | (BackendProviderKind::OpenRouter, _)
        | (BackendProviderKind::ChatGptCodex, _) => Some(serde_json::json!({
            "reasoning": { "effort": reasoning_effort.as_str() }
        })),
        // Grok: do not force reasoning.effort — several grok models 400 on it.
        (BackendProviderKind::XaiGrokOAuth, _) => None,
    }
}

fn provider_additional_params(kind: BackendProviderKind) -> Option<serde_json::Value> {
    match kind {
        BackendProviderKind::OpenAiCompatible => None,
        BackendProviderKind::OpenRouter => Some(
            rig::providers::openrouter::ProviderPreferences::new()
                .require_parameters(true)
                .to_json(),
        ),
        BackendProviderKind::ChatGptCodex | BackendProviderKind::XaiGrokOAuth => None,
    }
}

fn request_additional_params(
    behavior: &AgentBehavior,
    request: &AgentRequest,
) -> Option<serde_json::Value> {
    match behavior.backend_provider_kind {
        BackendProviderKind::OpenAiCompatible => openai_cache_scope_params(request),
        BackendProviderKind::OpenRouter
        | BackendProviderKind::ChatGptCodex
        | BackendProviderKind::XaiGrokOAuth => None,
    }
}

fn openai_cache_scope_params(request: &AgentRequest) -> Option<serde_json::Value> {
    let scope = normalize_cache_scope(request.session_id.as_str())
        .or_else(|| normalize_cache_scope(request.request_id.as_str()))?;
    Some(serde_json::json!({ "user": scope }))
}

fn normalize_cache_scope(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

#[cfg(test)]
mod tests;
