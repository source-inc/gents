use std::sync::Arc;

use crate::llm::message::Message;
use crate::llm::tool::ToolDyn;
use anyhow::{anyhow, Context, Result};
use defra_node::EmbeddedNode;
use rig::client::CompletionClient;
use rig::completion::CompletionModel;

use crate::backend_provider::BackendProviderKind;
use crate::completion_factory::loop_config;
use crate::config::AgentBehavior;
use crate::hook::{BackgroundToolRegistry, DefraSessionHook, FailurePolicy};
use crate::prompt::{LayeredPromptBuilder, PromptBuilder};
use crate::tool_surface::{self, ToolRuntimeContext};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OneshotRunResult {
    pub session_id: String,
    pub response_text: String,
}

pub async fn run_openai_oneshot(
    node: Arc<EmbeddedNode>,
    behavior: &AgentBehavior,
    prompt: &str,
) -> Result<OneshotRunResult> {
    run_openai_oneshot_with_tools(node, behavior, Vec::new(), prompt).await
}

pub async fn run_openai_oneshot_with_tools(
    node: Arc<EmbeddedNode>,
    behavior: &AgentBehavior,
    extra_tools: Vec<Box<dyn ToolDyn>>,
    prompt: &str,
) -> Result<OneshotRunResult> {
    crate::migration::ensure_all_runtime_migrations(node.clone()).await?;

    let api_key = behavior.completion_client_api_key()?;
    let tool_runtime =
        ToolRuntimeContext::oneshot_with_agent_did(node.clone(), behavior.agent_did());
    let tool_surface = behavior.tools.resolve(node.as_ref()).await?;
    let allowed_targets = tool_surface::resolve_subagent_target_descriptions(&tool_surface);
    let prompt_builder = LayeredPromptBuilder::new(behavior, &tool_surface, &allowed_targets);
    let preamble = prompt_builder.preamble().to_string();

    let mut tools = tool_surface.build_tools(&tool_runtime)?;
    tools.extend(extra_tools);
    let tools = Arc::new(tools);
    // Background executions run through `call_tool_managed`, which owns the
    // deadline/cancellation envelope — no per-tool wrapper needed.
    let background_tool_registry = BackgroundToolRegistry::from_tools(
        tool_surface.build_tools(&tool_runtime)?,
        &tool_surface.background_tools().allowlist,
    );

    match behavior.backend_provider_kind {
        BackendProviderKind::OpenAiCompatible => {
            let build_context = format!(
                "building OpenAI-compatible completion client for behavior {} against {}",
                behavior.behavior_id, behavior.backend_endpoint
            );
            if behavior.openai_wire_api == crate::OpenAiWireApi::ChatCompletions {
                let client: rig::providers::openai::CompletionsClient<
                    crate::inference_http::SessionTaggingHttpClient<
                        crate::rendered_request::RenderedRequestCapturingHttpClient,
                    >,
                > = crate::inference_http::build_openai_chat_completions_client(
                    &api_key,
                    &behavior.backend_endpoint,
                    crate::inference_http::SessionTaggingHttpClient::new(
                        crate::rendered_request::RenderedRequestCapturingHttpClient::default(),
                    ),
                )
                .with_context(|| build_context.clone())?;
                run_oneshot_with_completion_client(
                    node,
                    behavior,
                    prompt,
                    prompt_builder,
                    preamble,
                    tools,
                    background_tool_registry,
                    client,
                )
                .await
            } else {
                let client: rig::providers::openai::Client<
                    crate::inference_http::SessionTaggingHttpClient<
                        crate::inference_http::ResponsesNormalizingHttpClient<
                            crate::rendered_request::RenderedRequestCapturingHttpClient,
                        >,
                    >,
                > = crate::inference_http::build_openai_responses_client(
                    &api_key,
                    &behavior.backend_endpoint,
                    crate::inference_http::SessionTaggingHttpClient::new(
                        crate::inference_http::ResponsesNormalizingHttpClient::new(
                            crate::rendered_request::RenderedRequestCapturingHttpClient::default(),
                        ),
                    ),
                    Default::default(),
                )
                .with_context(|| build_context.clone())?;
                run_oneshot_with_completion_client(
                    node,
                    behavior,
                    prompt,
                    prompt_builder,
                    preamble,
                    tools,
                    background_tool_registry,
                    client,
                )
                .await
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
                .api_key(&api_key)
                .base_url(&behavior.backend_endpoint)
                .http_client(crate::rendered_request::RenderedRequestCapturingHttpClient::default())
                .build()
                .with_context(|| build_context.clone())?;
            run_oneshot_with_completion_client(
                node,
                behavior,
                prompt,
                prompt_builder,
                preamble,
                tools,
                background_tool_registry,
                client,
            )
            .await
        }
        BackendProviderKind::ChatGptCodex => {
            let client = crate::chatgpt_codex::build_responses_client(
                node.clone(),
                behavior.agent_did(),
                &behavior.backend_endpoint,
            )
            .await
            .with_context(|| {
                format!(
                    "building ChatGPT Codex completion client for behavior {} against {}",
                    behavior.behavior_id, behavior.backend_endpoint
                )
            })?;
            run_oneshot_with_completion_client(
                node,
                behavior,
                prompt,
                prompt_builder,
                preamble,
                tools,
                background_tool_registry,
                client,
            )
            .await
        }
        BackendProviderKind::XaiGrokOAuth => {
            let build_context = format!(
                "building Grok OAuth completion client for behavior {} against {}",
                behavior.behavior_id, behavior.backend_endpoint
            );
            if behavior.openai_wire_api == crate::OpenAiWireApi::ChatCompletions {
                let client = crate::xai_grok_oauth::build_chat_completions_client(
                    node.clone(),
                    behavior.agent_did(),
                    &behavior.backend_endpoint,
                )
                .await
                .with_context(|| build_context.clone())?;
                run_oneshot_with_completion_client(
                    node,
                    behavior,
                    prompt,
                    prompt_builder,
                    preamble,
                    tools,
                    background_tool_registry,
                    client,
                )
                .await
            } else {
                let client = crate::xai_grok_oauth::build_responses_client(
                    node.clone(),
                    behavior.agent_did(),
                    &behavior.backend_endpoint,
                )
                .await
                .with_context(|| build_context.clone())?;
                run_oneshot_with_completion_client(
                    node,
                    behavior,
                    prompt,
                    prompt_builder,
                    preamble,
                    tools,
                    background_tool_registry,
                    client,
                )
                .await
            }
        }
    }
}

async fn run_oneshot_with_completion_client<C>(
    node: Arc<EmbeddedNode>,
    behavior: &AgentBehavior,
    prompt: &str,
    prompt_builder: LayeredPromptBuilder,
    preamble: String,
    tools: Arc<Vec<Box<dyn ToolDyn>>>,
    background_tool_registry: BackgroundToolRegistry,
    client: C,
) -> Result<OneshotRunResult>
where
    C: CompletionClient,
    C::CompletionModel: 'static,
    <C::CompletionModel as CompletionModel>::StreamingResponse: 'static,
{
    // One-shot calls are still provider calls. Give them the same durable
    // admission/send fence as document-backed runtime calls instead of using
    // the absence of an AgentRequest as an excuse to omit InferenceCall
    // provenance.
    let admission = crate::admission::AdmissionRegistry::new_direct_oneshot(node.clone());
    let model = crate::completion_factory::build_admitted_model(client, admission, behavior);
    let config = loop_config(
        behavior,
        preamble,
        tools.len(),
        crate::rendered_request::CaptureScopeKind::OneShot,
    );
    run_oneshot_owned(
        node,
        behavior,
        &prompt_builder,
        model,
        prompt,
        tools,
        config,
        background_tool_registry,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_oneshot_owned<M: CompletionModel + 'static>(
    node: Arc<EmbeddedNode>,
    behavior: &AgentBehavior,
    prompt_builder: &LayeredPromptBuilder,
    model: M,
    prompt: &str,
    tools: Arc<Vec<Box<dyn ToolDyn>>>,
    config: crate::agent::loop_stream::LoopConfig,
    background_tool_registry: BackgroundToolRegistry,
) -> Result<OneshotRunResult>
where
    M::StreamingResponse: 'static,
{
    // A one-shot run has no `AgentRequest` document, so `request_doc_id` stays
    // empty rather than impersonating a durable document. Its random session id
    // still gives the capture key a unique durable scope. The session and
    // logical request id are minted before the first provider call, and the
    // session is created eagerly rather than on the hook's first write.
    let session_id = uuid::Uuid::new_v4().to_string();
    let request_id = format!("oneshot-{}", uuid::Uuid::new_v4());
    let capture_scope = crate::rendered_request::scope::scope_from_factory(
        crate::rendered_request::RenderedRequestContext {
            request_doc_id: String::new(),
            request_provenance: None,
            inference_call_provenance_scope:
                crate::rendered_request::InferenceCallProvenanceScope::AdmittedProviderCall,
            transcript_snapshot: Vec::new(),
            config_provenance_scope:
                crate::rendered_request::ConfigProvenanceScope::StaticOrOneShot,
            config_provenance: None,
            request_id: request_id.clone(),
            agent_did: behavior.agent_did().to_string(),
            requester_did: String::new(),
            behavior_id: behavior.behavior_id.clone(),
            session_id: session_id.clone(),
            model_name: behavior.model_name.clone(),
        },
        Some(&crate::rendered_request::defra_rendered_request_capture_factory(node.clone())),
    );

    let hook = DefraSessionHook::resume_or_create_with_identity_policy(
        node,
        &session_id,
        &behavior.behavior_id,
        behavior.agent_did(),
        FailurePolicy::default(),
    )
    .await?
    .with_background_tool_registry(background_tool_registry);
    let history = prompt_builder.build(&[], &[]).await?.messages;

    let inference = crate::agent::loop_stream::run_loop_to_text(
        model,
        Some(hook.clone()),
        Message::user(prompt),
        history,
        tools,
        config,
    );
    let backend_id = behavior
        .backend_id
        .as_deref()
        .filter(|backend_id| !backend_id.trim().is_empty())
        .ok_or_else(|| {
            anyhow!(
                "one-shot behavior {} has no backend binding",
                behavior.behavior_id
            )
        })?;
    let admission_scope = crate::admission::AdmissionCallContext::for_oneoff(
        &request_id,
        &session_id,
        &behavior.behavior_id,
        backend_id,
        behavior.agent_did(),
    );
    let admitted_inference = async move {
        match capture_scope {
            Some(scope) => crate::rendered_request::scope::scope_request(scope, inference).await,
            None => inference.await,
        }
    };
    let response = crate::admission::scope_request(admission_scope, admitted_inference)
        .await
        .map_err(|error| anyhow!("one-shot inference failed: {error}"));

    let session_id = hook.session_id().await;
    let close_result = hook.close().await;

    match response {
        Ok(response_text) => {
            let session_id = session_id.context("one-shot run did not create a session")?;
            close_result.with_context(|| format!("closing one-shot session {session_id}"))?;

            Ok(OneshotRunResult {
                session_id,
                response_text,
            })
        }
        Err(error) => {
            if let Some(session_id) = session_id {
                if let Err(close_error) = close_result {
                    return Err(anyhow!(
                        "agent prompt failed: {error}; additionally failed to close session {session_id}: {close_error}"
                    ));
                }
            } else if let Err(close_error) = close_result {
                return Err(anyhow!(
                    "agent prompt failed: {error}; additionally failed to close one-shot hook: {close_error}"
                ));
            }

            Err(error)
        }
    }
}
