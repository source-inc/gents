//! Provider-shaped preflight accounting.
//!
//! Rig's core [`rig::completion::CompletionRequest`] is an intermediate. Each
//! backend converts it again before sending, and those conversions are
//! semantically significant: OpenAI Chat Completions, for example, omits
//! assistant-history reasoning entirely. Budget decisions must therefore use
//! the same selected projection as the provider client, not serialization of
//! the core request.

use anyhow::{Context, Result};
use rig::completion::CompletionRequest;
use serde_json::{Map, Value};

use crate::backend_provider::BackendProviderKind;
use crate::openai_wire::OpenAiWireApi;

pub(crate) mod budget;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProviderInputProfile {
    OpenAiChatCompletions,
    OpenAiResponsesNormalized,
    OpenRouterChatCompletions,
    ChatGptCodexResponses,
    XaiResponses,
    /// Anthropic Messages body (`claude_messages`); the OpenAI wire setting
    /// does not apply.
    ClaudeMessages,
}

impl ProviderInputProfile {
    pub(crate) fn resolve(provider: BackendProviderKind, wire: OpenAiWireApi) -> Self {
        match (provider, wire) {
            (BackendProviderKind::OpenAiCompatible, OpenAiWireApi::ChatCompletions)
            | (BackendProviderKind::XaiGrokOAuth, OpenAiWireApi::ChatCompletions) => {
                Self::OpenAiChatCompletions
            }
            (BackendProviderKind::OpenAiCompatible, OpenAiWireApi::Responses) => {
                Self::OpenAiResponsesNormalized
            }
            (BackendProviderKind::OpenRouter, _) => Self::OpenRouterChatCompletions,
            (BackendProviderKind::ChatGptCodex, _) => Self::ChatGptCodexResponses,
            (BackendProviderKind::XaiGrokOAuth, OpenAiWireApi::Responses) => Self::XaiResponses,
            (BackendProviderKind::ClaudeCliSubscription, _) => Self::ClaudeMessages,
        }
    }

    const fn estimator_name(self) -> &'static str {
        match self {
            Self::OpenAiChatCompletions => "openai_chat_wire_json_bytes_div_4_v1",
            Self::OpenAiResponsesNormalized => "openai_responses_wire_json_bytes_div_4_v1",
            Self::OpenRouterChatCompletions => "openrouter_chat_wire_json_bytes_div_4_v1",
            Self::ChatGptCodexResponses => "chatgpt_codex_wire_json_bytes_div_4_v1",
            Self::XaiResponses => "xai_responses_wire_json_bytes_div_4_v1",
            Self::ClaudeMessages => "claude_messages_wire_json_bytes_div_4_v1",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ProviderInputCounter {
    profile: ProviderInputProfile,
    model: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProviderInputProjection {
    pub(crate) components: gents_protocol::rendered_request::ContextInputComponents,
    /// One authoritative estimate over the composed provider projection. It is
    /// deliberately not a sum of independently floored component estimates.
    pub(crate) estimated_input_tokens: usize,
    pub(crate) estimator: &'static str,
}

impl ProviderInputCounter {
    pub(crate) fn new(
        provider: BackendProviderKind,
        wire: OpenAiWireApi,
        model: impl Into<String>,
    ) -> Self {
        Self {
            profile: ProviderInputProfile::resolve(provider, wire),
            model: model.into(),
        }
    }

    #[cfg(test)]
    pub(crate) fn profile(&self) -> ProviderInputProfile {
        self.profile
    }

    pub(crate) fn project_request(
        &self,
        request: &CompletionRequest,
    ) -> Result<ProviderInputProjection> {
        let body = self.project_body(request)?;
        let documentless_body = if request.documents.is_empty() {
            None
        } else {
            let mut documentless = request.clone();
            documentless.documents.clear();
            Some(self.project_body(&documentless)?)
        };

        projected_accounting(body, documentless_body, self.profile.estimator_name())
    }

    /// Estimate one complete provider request without computing the diagnostic
    /// component partition. Candidate search and admission use this scalar hot
    /// path; rendered-request accounting calls `project_request` once for the
    /// request that may actually be dispatched.
    pub(crate) fn estimate_request(&self, request: &CompletionRequest) -> Result<usize> {
        let mut body = self.project_body(request)?;
        remove_output_limits(&mut body);
        estimate_json(&body)
    }

    fn project_body(&self, request: &CompletionRequest) -> Result<Value> {
        let body = match self.profile {
            ProviderInputProfile::OpenAiChatCompletions => {
                let dto = rig::providers::openai::completion::CompletionRequest::try_from((
                    self.model.clone(),
                    request.clone(),
                ))
                .context("projecting OpenAI Chat Completions request")?;
                let mut body = serde_json::to_value(dto)
                    .context("serializing OpenAI Chat Completions request")?;
                set_streaming_fields(&mut body, true);
                body
            }
            ProviderInputProfile::OpenAiResponsesNormalized => {
                let mut body = self.responses_body(request)?;
                set_streaming_fields(&mut body, false);
                crate::llm::responses_normalize::normalize_responses_assistant_items(&mut body);
                body
            }
            ProviderInputProfile::OpenRouterChatCompletions => {
                let mut body = self.openrouter_body(request)?;
                set_streaming_fields(&mut body, false);
                body
            }
            ProviderInputProfile::ChatGptCodexResponses => {
                let mut body = self.responses_body(request)?;
                set_streaming_fields(&mut body, false);
                rewrite_bytes(body, crate::chatgpt_codex::patch_instructions_body)?
            }
            ProviderInputProfile::XaiResponses => {
                let mut body = self.responses_body(request)?;
                set_streaming_fields(&mut body, false);
                rewrite_bytes(body, crate::xai_grok_oauth::patch_store_false)?
            }
            ProviderInputProfile::ClaudeMessages => {
                crate::claude_messages::build_messages_body(&self.model, request)
            }
        };
        Ok(body)
    }

    /// Estimate a messages-only provider request. The result includes the
    /// selected wire API's request framing; it is not an additive per-row cost.
    pub(crate) fn estimate_message_request(
        &self,
        messages: &[crate::llm::message::Message],
    ) -> Result<usize> {
        if messages.is_empty() {
            return Ok(0);
        }
        if self.profile == ProviderInputProfile::OpenAiChatCompletions {
            let has_visible_message = crate::llm::rig_compat::to_rig_messages(messages)
                .into_iter()
                .map(Vec::<rig::providers::openai::completion::Message>::try_from)
                .collect::<std::result::Result<Vec<_>, _>>()?
                .into_iter()
                .any(|converted| !converted.is_empty());
            if !has_visible_message {
                return Ok(0);
            }
        }
        let chat_history =
            rig::one_or_many::OneOrMany::many(crate::llm::rig_compat::to_rig_messages(messages))
                .map_err(|_| anyhow::anyhow!("provider input message projection was empty"))?;
        let request = CompletionRequest {
            model: None,
            preamble: None,
            chat_history,
            documents: Vec::new(),
            tools: Vec::new(),
            temperature: None,
            max_tokens: None,
            tool_choice: None,
            additional_params: None,
            output_schema: None,
        };
        self.estimate_request(&request)
    }

    fn responses_body(&self, request: &CompletionRequest) -> Result<Value> {
        let dto = rig::providers::openai::responses_api::CompletionRequest::try_from((
            self.model.clone(),
            request.clone(),
        ))
        .context("projecting OpenAI Responses request")?;
        serde_json::to_value(dto).context("serializing OpenAI Responses request")
    }

    /// Rig keeps its complete OpenRouter DTO crate-private. Build the same
    /// shape from Rig's public provider message/tool converters so production
    /// accounting still follows the actual wire representation, including
    /// `reasoning_details`.
    fn openrouter_body(&self, request: &CompletionRequest) -> Result<Value> {
        use rig::providers::openrouter::completion::Message as OpenRouterMessage;

        let mut messages = Vec::new();
        if let Some(preamble) = request.preamble.as_ref() {
            messages.extend(
                Vec::<OpenRouterMessage>::try_from(rig::completion::Message::system(preamble))
                    .context("projecting OpenRouter preamble")?,
            );
        }
        if let Some(documents) = request.normalized_documents() {
            messages.extend(
                Vec::<OpenRouterMessage>::try_from(documents)
                    .context("projecting OpenRouter documents")?,
            );
        }
        for message in request.chat_history.clone() {
            messages.extend(
                Vec::<OpenRouterMessage>::try_from(message)
                    .context("projecting OpenRouter message")?,
            );
        }

        let tools = request
            .tools
            .clone()
            .into_iter()
            .map(rig::providers::openai::completion::ToolDefinition::from)
            .collect::<Vec<_>>();
        let tool_choice = request
            .tool_choice
            .clone()
            .map(rig::providers::openai::completion::ToolChoice::try_from)
            .transpose()
            .context("projecting OpenRouter tool choice")?;

        let mut body = Map::new();
        body.insert(
            "model".to_string(),
            Value::String(request.model.clone().unwrap_or_else(|| self.model.clone())),
        );
        body.insert("messages".to_string(), serde_json::to_value(messages)?);
        if let Some(temperature) = request.temperature {
            body.insert("temperature".to_string(), Value::from(temperature));
        }
        if !tools.is_empty() {
            body.insert("tools".to_string(), serde_json::to_value(tools)?);
        }
        if let Some(tool_choice) = tool_choice {
            body.insert(
                "tool_choice".to_string(),
                serde_json::to_value(tool_choice)?,
            );
        }
        if let Some(additional) = request
            .additional_params
            .as_ref()
            .and_then(Value::as_object)
        {
            for (key, value) in additional {
                body.insert(key.clone(), value.clone());
            }
        }
        Ok(Value::Object(body))
    }
}

fn set_streaming_fields(body: &mut Value, include_usage: bool) {
    let Some(object) = body.as_object_mut() else {
        return;
    };
    object.insert("stream".to_string(), Value::Bool(true));
    if include_usage {
        object.insert(
            "stream_options".to_string(),
            serde_json::json!({"include_usage": true}),
        );
    }
}

fn rewrite_bytes(body: Value, rewrite: fn(&[u8]) -> Option<bytes::Bytes>) -> Result<Value> {
    let encoded = serde_json::to_vec(&body).context("serializing provider body for rewrite")?;
    match rewrite(&encoded) {
        Some(rewritten) => serde_json::from_slice(&rewritten)
            .context("decoding deterministically rewritten provider body"),
        None => Ok(body),
    }
}

fn projected_accounting(
    mut body: Value,
    mut documentless_body: Option<Value>,
    estimator: &'static str,
) -> Result<ProviderInputProjection> {
    remove_output_limits(&mut body);
    let estimated_input_tokens = estimate_json(&body)?;

    let provider_messages =
        field_estimate(&body, &["messages", "system", "input", "instructions"])?;
    let documentless_messages = documentless_body
        .as_mut()
        .map(|body| {
            remove_output_limits(body);
            field_estimate(body, &["messages", "system", "input", "instructions"])
        })
        .transpose()?
        .unwrap_or(provider_messages);
    let documents = provider_messages
        .checked_sub(documentless_messages)
        .context("provider document projection reduced the provider message estimate")?;
    let messages = documentless_messages;
    let tool_schemas = field_estimate(&body, &["tools", "tool_choice"])?;
    let output_schema = field_estimate(&body, &["response_format", "text"])?;
    let classified = messages
        .checked_add(documents)
        .and_then(|total| total.checked_add(tool_schemas))
        .and_then(|total| total.checked_add(output_schema))
        .context("provider input component estimate overflow")?;
    // Framing, model, and provider parameters occupy the remainder. Assigning
    // the remainder makes the diagnostic partition agree
    // exactly with the one-shot authoritative total despite `/4` remainders.
    let additional_parameters = estimated_input_tokens.checked_sub(classified).ok_or_else(|| {
        anyhow::anyhow!(
            "provider input component partition exceeded total: classified={classified}, total={estimated_input_tokens}"
        )
    })?;
    let components = gents_protocol::rendered_request::ContextInputComponents {
        messages,
        documents,
        tool_schemas,
        additional_parameters,
        output_schema,
    };

    Ok(ProviderInputProjection {
        components,
        estimated_input_tokens,
        estimator,
    })
}

fn remove_output_limits(body: &mut Value) {
    // Output limits are dispatch parameters, not provider input. Excluding
    // them also avoids circular accounting when the dynamic clamp changes the
    // number of digits in the field itself.
    if let Some(object) = body.as_object_mut() {
        object.remove("max_tokens");
        object.remove("max_output_tokens");
    }
}

fn field_estimate(value: &Value, fields: &[&str]) -> Result<usize> {
    let Some(object) = value.as_object() else {
        return Ok(0);
    };
    fields.iter().try_fold(0usize, |total, field| {
        let field_tokens = object
            .get(*field)
            .map(estimate_json)
            .transpose()?
            .unwrap_or(0);
        total
            .checked_add(field_tokens)
            .context("provider input field estimate overflow")
    })
}

fn estimate_json(value: &Value) -> Result<usize> {
    Ok(serde_json::to_vec(value)
        .context("serializing provider input for token estimate")?
        .len()
        / 4)
}

#[cfg(test)]
mod tests;
