use std::collections::HashSet;
use std::sync::Arc;

use crate::llm::tool::ToolDyn;
use anyhow::{Context, Result};
use rig::client::CompletionClient;
use tokio::sync::{mpsc, watch, Mutex};

use crate::admission::AdmissionRegistry;
use crate::agent::daemon::BehaviorDaemon;
use crate::backend_provider::BackendProviderKind;
use crate::completion_factory::build_admitted_model;
use crate::hook::{BackgroundExecutionRegistry, BackgroundToolRegistry};
use crate::prompt::LayeredPromptBuilder;
use crate::tool_surface::{self, ToolRuntimeContext, ToolSurface};
use crate::watcher::AgentRequest;

#[derive(Clone)]
pub(super) struct RuntimeContext {
    pub(super) node: Arc<defra_node::EmbeddedNode>,
    pub(super) tool_runtime: ToolRuntimeContext,
    pub(super) admission_registry: AdmissionRegistry,
    pub(super) hook_failure_policy: crate::hook::FailurePolicy,
    pub(super) rendered_request_capture_factory:
        Option<crate::rendered_request::RenderedRequestCaptureFactory>,
    pub(super) background_execution_registry: BackgroundExecutionRegistry,
    pub(super) startup_barrier: Arc<StartupBarrier>,
    pub(super) startup_readiness: crate::startup_readiness::StartupReadinessOptions,
    pub(super) startup_demotions: Arc<crate::startup_readiness::StartupDemotions>,
}

pub(super) struct BehaviorResolution {
    pub(super) behavior_id: String,
    pub(super) rejection_reason: Option<String>,
}

pub struct StartupBarrier {
    pending_behaviors: Mutex<HashSet<String>>,
    pending_count_tx: watch::Sender<usize>,
}

impl StartupBarrier {
    pub(super) fn new(behaviors: &[Arc<crate::config::AgentBehavior>]) -> Self {
        let pending: HashSet<String> = behaviors
            .iter()
            .map(|behavior| behavior.behavior_id.clone())
            .collect();
        let (pending_count_tx, _) = watch::channel(pending.len());
        Self {
            pending_behaviors: Mutex::new(pending),
            pending_count_tx,
        }
    }

    async fn release(&self, behavior_id: &str) {
        let mut pending = self.pending_behaviors.lock().await;
        if pending.remove(behavior_id) {
            let _ = self.pending_count_tx.send_replace(pending.len());
        }
    }

    #[cfg(test)]
    pub(in crate::agent) fn ready_for_test() -> Self {
        Self::new(&[])
    }

    pub async fn mark_behavior_ready(&self, behavior_id: &str) {
        self.release(behavior_id).await;
    }

    pub async fn mark_behavior_demoted(&self, behavior_id: &str) {
        self.release(behavior_id).await;
    }

    pub async fn mark_behavior_superseded(&self, behavior_id: &str) {
        self.release(behavior_id).await;
    }

    pub async fn is_pending(&self, behavior_id: &str) -> bool {
        self.pending_behaviors.lock().await.contains(behavior_id)
    }

    pub async fn pending_behaviors(&self) -> Vec<String> {
        let mut pending: Vec<String> = self
            .pending_behaviors
            .lock()
            .await
            .iter()
            .cloned()
            .collect();
        pending.sort();
        pending
    }

    pub(super) async fn wait_ready(&self) {
        let mut rx = self.pending_count_tx.subscribe();
        let _ = rx.wait_for(|count| *count == 0).await;
    }
}

impl RuntimeContext {
    pub(super) async fn run_behavior(
        &self,
        behavior: Arc<crate::config::AgentBehavior>,
        tool_surface: Arc<ToolSurface>,
        config_provenance: crate::runtime_snapshot::ScopedBehaviorConfigProvenance,
        request_rx: Arc<Mutex<mpsc::Receiver<AgentRequest>>>,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let tool_names = tool_surface.tool_names();
        let api_key = behavior.completion_client_api_key()?;
        let allowed_targets =
            tool_surface::resolve_subagent_target_descriptions(tool_surface.as_ref());
        let prompt_builder =
            LayeredPromptBuilder::new(behavior.as_ref(), tool_surface.as_ref(), &allowed_targets);
        let preamble = prompt_builder.preamble().to_string();
        let mut loop_tools = tool_surface.build_tools(&self.tool_runtime)?;
        if tool_surface.includes_skills() && !behavior.skills.is_empty() {
            let ceiling = crate::skills::skill_tool_ceiling(
                tool_surface.tool_names(),
                tool_surface.allowed_mcp_service_ids(),
                tool_surface.includes_meta_tools(),
            );
            loop_tools.push(Box::new(crate::skills::LoadSkillTool::new(
                behavior.skills.clone(),
                ceiling,
            )));
        }
        let loop_tools = std::sync::Arc::new(loop_tools);
        // Background executions run through `call_tool_managed`, which owns
        // the deadline/cancellation envelope — no per-tool wrapper needed.
        let background_tool_registry = BackgroundToolRegistry::from_tools(
            tool_surface.build_tools(&self.tool_runtime)?,
            &tool_surface.background_tools().allowlist,
        );
        tracing::info!(
            behavior_id = %behavior.behavior_id,
            did = %behavior.agent_did(),
            model = %behavior.model_name,
            tools = ?tool_names,
            "building behavior runtime"
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
                    self.run_behavior_with_client(
                        behavior,
                        config_provenance,
                        request_rx,
                        shutdown,
                        prompt_builder,
                        preamble,
                        loop_tools.clone(),
                        background_tool_registry,
                        tool_surface.approval_required_tools().to_vec(),
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
                    self.run_behavior_with_client(
                        behavior,
                        config_provenance,
                        request_rx,
                        shutdown,
                        prompt_builder,
                        preamble,
                        loop_tools.clone(),
                        background_tool_registry,
                        tool_surface.approval_required_tools().to_vec(),
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
                    .http_client(
                        crate::rendered_request::RenderedRequestCapturingHttpClient::default(),
                    )
                    .build()
                    .with_context(|| build_context.clone())?;
                self.run_behavior_with_client(
                    behavior,
                    config_provenance,
                    request_rx,
                    shutdown,
                    prompt_builder,
                    preamble,
                    loop_tools.clone(),
                    background_tool_registry,
                    tool_surface.approval_required_tools().to_vec(),
                    client,
                )
                .await
            }
            BackendProviderKind::ChatGptCodex => {
                let client = tokio::time::timeout(
                    self.startup_readiness.build_timeout,
                    crate::chatgpt_codex::build_responses_client(
                        self.node.clone(),
                        behavior.agent_did(),
                        &behavior.backend_endpoint,
                    ),
                )
                .await
                .map_err(|_| {
                    anyhow::anyhow!(
                        "timed out after {:?} building the ChatGPT Codex completion client",
                        self.startup_readiness.build_timeout
                    )
                })
                .and_then(|result| result)
                .with_context(|| {
                    format!(
                        "building ChatGPT Codex completion client for behavior {} against {}",
                        behavior.behavior_id, behavior.backend_endpoint
                    )
                })?;
                self.run_behavior_with_client(
                    behavior,
                    config_provenance,
                    request_rx,
                    shutdown,
                    prompt_builder,
                    preamble,
                    loop_tools.clone(),
                    background_tool_registry,
                    tool_surface.approval_required_tools().to_vec(),
                    client,
                )
                .await
            }
            BackendProviderKind::XaiGrokOAuth => {
                let build_context = format!(
                    "building Grok OAuth completion client for behavior {} against {}",
                    behavior.behavior_id, behavior.backend_endpoint
                );
                let timeout_error = || {
                    anyhow::anyhow!(
                        "timed out after {:?} building the Grok OAuth completion client",
                        self.startup_readiness.build_timeout
                    )
                };
                if behavior.openai_wire_api == crate::OpenAiWireApi::ChatCompletions {
                    let client = tokio::time::timeout(
                        self.startup_readiness.build_timeout,
                        crate::xai_grok_oauth::build_chat_completions_client(
                            self.node.clone(),
                            behavior.agent_did(),
                            &behavior.backend_endpoint,
                        ),
                    )
                    .await
                    .map_err(|_| timeout_error())
                    .and_then(|result| result)
                    .with_context(|| build_context.clone())?;
                    self.run_behavior_with_client(
                        behavior,
                        config_provenance,
                        request_rx,
                        shutdown,
                        prompt_builder,
                        preamble,
                        loop_tools.clone(),
                        background_tool_registry,
                        tool_surface.approval_required_tools().to_vec(),
                        client,
                    )
                    .await
                } else {
                    let client = tokio::time::timeout(
                        self.startup_readiness.build_timeout,
                        crate::xai_grok_oauth::build_responses_client(
                            self.node.clone(),
                            behavior.agent_did(),
                            &behavior.backend_endpoint,
                        ),
                    )
                    .await
                    .map_err(|_| timeout_error())
                    .and_then(|result| result)
                    .with_context(|| build_context.clone())?;
                    self.run_behavior_with_client(
                        behavior,
                        config_provenance,
                        request_rx,
                        shutdown,
                        prompt_builder,
                        preamble,
                        loop_tools.clone(),
                        background_tool_registry,
                        tool_surface.approval_required_tools().to_vec(),
                        client,
                    )
                    .await
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_behavior_with_client<C>(
        &self,
        behavior: Arc<crate::config::AgentBehavior>,
        config_provenance: crate::runtime_snapshot::ScopedBehaviorConfigProvenance,
        request_rx: Arc<Mutex<mpsc::Receiver<AgentRequest>>>,
        shutdown: watch::Receiver<bool>,
        prompt_builder: LayeredPromptBuilder,
        preamble: String,
        loop_tools: Arc<Vec<Box<dyn ToolDyn>>>,
        background_tool_registry: BackgroundToolRegistry,
        approval_required_tools: Vec<String>,
        client: C,
    ) -> Result<()>
    where
        C: CompletionClient,
        C::CompletionModel: 'static,
    {
        let model = Arc::new(build_admitted_model(
            client,
            self.admission_registry.clone(),
            behavior.as_ref(),
        ));
        let mut daemon = BehaviorDaemon::new(
            self.node.clone(),
            behavior,
            config_provenance,
            model,
            preamble,
            loop_tools,
            prompt_builder,
            self.hook_failure_policy,
            self.rendered_request_capture_factory.clone(),
            background_tool_registry,
            self.background_execution_registry.clone(),
            self.startup_barrier.clone(),
            self.startup_demotions.clone(),
        )
        .with_approval_required_tools(approval_required_tools);
        daemon.run(request_rx, shutdown).await
    }
}

#[cfg(test)]
mod startup_barrier_tests {
    use super::*;

    fn behavior(behavior_id: &str) -> Arc<crate::config::AgentBehavior> {
        Arc::new(
            crate::agent::PendingAgentBehavior::new(behavior_id).build_with_identity_for_test(
                crate::KeyIdentity::load_or_create(
                    std::env::temp_dir().join(format!(
                        "barrier-{behavior_id}-{}.key",
                        uuid::Uuid::new_v4()
                    )),
                    None,
                )
                .unwrap(),
            ),
        )
    }

    #[tokio::test]
    async fn empty_seed_is_immediately_ready() {
        let barrier = StartupBarrier::new(&[]);
        tokio::time::timeout(std::time::Duration::from_secs(1), barrier.wait_ready())
            .await
            .expect("an empty barrier must not wait");
    }

    /// The lost-wakeup regression (#559): the final release may land at any
    /// point relative to the waiter. With the watch channel the waiter always
    /// observes the current count, so release-before-wait, release-after-wait,
    /// and anything between all complete.
    #[tokio::test]
    async fn release_before_and_after_wait_both_complete() {
        let barrier = Arc::new(StartupBarrier::new(&[behavior("a"), behavior("b")]));

        // Release one before any waiter exists.
        barrier.mark_behavior_ready("a").await;

        let waiter = {
            let barrier = barrier.clone();
            tokio::spawn(async move { barrier.wait_ready().await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        barrier.mark_behavior_ready("b").await;

        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("waiter must observe the final release")
            .unwrap();
    }

    /// Every release class frees the barrier: readiness, demotion (budget
    /// spent), and supersession (retired mid-startup). Only readiness claims
    /// health; the others carry their verdict in the demotion ledger.
    #[tokio::test]
    async fn demotion_and_supersession_release_without_readiness() {
        let barrier = Arc::new(StartupBarrier::new(&[
            behavior("healthy"),
            behavior("unbuildable"),
            behavior("retired"),
        ]));

        barrier.mark_behavior_ready("healthy").await;
        barrier.mark_behavior_demoted("unbuildable").await;
        assert!(barrier.is_pending("retired").await);
        barrier.mark_behavior_superseded("retired").await;
        assert!(!barrier.is_pending("retired").await);

        tokio::time::timeout(std::time::Duration::from_secs(1), barrier.wait_ready())
            .await
            .expect("all release classes together must free the barrier");
        assert!(barrier.pending_behaviors().await.is_empty());
    }
}
