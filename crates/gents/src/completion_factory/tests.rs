use super::*;
use std::sync::Arc;
use std::time::Duration;

use crate::agent::completion_retry::CompletionRetryProfileFields;
use crate::compaction::CompactionStrategy;
use crate::config::SamplingConfig;
use crate::identity::{AgentIdentity, AgentPrincipal, KeyIdentity};
use crate::tool_surface::BehaviorToolConfig;
use crate::watcher::AgentRequest;

fn request() -> AgentRequest {
    AgentRequest {
        doc_id: String::new(),
        request_id: "request-123".to_string(),
        agent_did: String::new(),
        requester_did: None,
        behavior_id: None,
        session_id: "session-456".to_string(),
        content: String::new(),
        temperature: None,
        top_p: None,
        top_k: None,
        seed: None,
        max_tokens: None,
        max_total_tokens: None,
        metadata: None,
        execution_origin: None,
        created_at: String::new(),
        deadline: None,
        subagent_depth: 0,
        caused_by_parent_request_id: None,
        caused_by_parent_request_doc_id: None,
        caused_by_parent_tool_call_id: None,
        caused_by_parent_tool_call_doc_id: None,
        caused_by_trigger_id: None,
        caused_by_trigger_kind: None,
        caused_by_source_doc_id: None,
        caused_by_correlation: None,
        caused_by_trigger_context: None,
        workspace_id: None,
        workspace_authority: None,
        workspace_owner_deployment_id: None,
        workspace_seal_hash: None,
    }
}

#[test]
fn absent_profile_effort_does_not_inject_reasoning() {
    assert!(reasoning_profile_params(
        BackendProviderKind::OpenAiCompatible,
        crate::OpenAiWireApi::ChatCompletions,
        None,
    )
    .is_none());
    assert!(reasoning_profile_params(
        BackendProviderKind::OpenAiCompatible,
        crate::OpenAiWireApi::Responses,
        None,
    )
    .is_none());
    assert!(reasoning_profile_params(
        BackendProviderKind::OpenRouter,
        crate::OpenAiWireApi::ChatCompletions,
        None,
    )
    .is_none());
}

/// Codex is the one backend whose reasoning default predates profile
/// configuration: it has a known Responses contract and shipped a hardcoded
/// `medium`. Profile plumbing may override that, never silently drop it (#540).
#[test]
fn absent_profile_effort_keeps_the_codex_medium_default() {
    assert_eq!(
        reasoning_profile_params(
            BackendProviderKind::ChatGptCodex,
            crate::OpenAiWireApi::Responses,
            None,
        ),
        Some(serde_json::json!({ "reasoning": { "effort": "medium" } })),
    );
    assert_eq!(
        reasoning_profile_params(
            BackendProviderKind::ChatGptCodex,
            crate::OpenAiWireApi::Responses,
            Some(ReasoningEffort::High),
        ),
        Some(serde_json::json!({ "reasoning": { "effort": "high" } })),
    );
}

#[test]
fn generic_responses_request_does_not_force_reasoning() {
    use rig::completion::message::{Message, Text, UserContent};
    use rig::one_or_many::OneOrMany;

    let core_req = rig::completion::CompletionRequest {
        model: None,
        preamble: None,
        chat_history: OneOrMany::one(Message::User {
            content: OneOrMany::one(UserContent::Text(Text {
                text: "hi".to_string(),
            })),
        }),
        documents: Vec::new(),
        tools: Vec::new(),
        temperature: Some(0.0),
        max_tokens: None,
        tool_choice: None,
        additional_params: reasoning_profile_params(
            BackendProviderKind::OpenAiCompatible,
            crate::OpenAiWireApi::Responses,
            None,
        ),
        output_schema: None,
    };

    let openai_req = rig::providers::openai::responses_api::CompletionRequest::try_from((
        "gpt-5.4-mini".to_string(),
        core_req,
    ))
    .expect("OpenAI Responses request conversion should succeed");
    let body = serde_json::to_value(&openai_req).expect("serializing OpenAI Responses request");

    assert_eq!(body["temperature"], 0.0);
    assert!(body.get("reasoning").is_none());
    assert!(body.get("chat_template_kwargs").is_none());
}

#[test]
fn responses_and_openrouter_use_profile_reasoning_effort() {
    let codex = reasoning_profile_params(
        BackendProviderKind::ChatGptCodex,
        crate::OpenAiWireApi::Responses,
        Some(crate::config::ReasoningEffort::Ultra),
    )
    .expect("profile reasoning effort must be present");
    let responses = reasoning_profile_params(
        BackendProviderKind::OpenAiCompatible,
        crate::OpenAiWireApi::Responses,
        Some(crate::config::ReasoningEffort::XHigh),
    )
    .expect("profile reasoning effort must be present");
    let openrouter = reasoning_profile_params(
        BackendProviderKind::OpenRouter,
        crate::OpenAiWireApi::ChatCompletions,
        Some(crate::config::ReasoningEffort::Medium),
    )
    .expect("profile reasoning effort must be present");

    assert_eq!(codex["reasoning"]["effort"], "ultra");
    assert_eq!(responses["reasoning"]["effort"], "xhigh");
    assert_eq!(openrouter["reasoning"]["effort"], "medium");
    assert!(codex.get("chat_template_kwargs").is_none());
}

#[test]
fn profile_none_disables_local_thinking() {
    let value = reasoning_profile_params(
        BackendProviderKind::OpenAiCompatible,
        crate::OpenAiWireApi::ChatCompletions,
        Some(crate::config::ReasoningEffort::None),
    )
    .expect("explicit none must be present");

    assert_eq!(value["chat_template_kwargs"]["enable_thinking"], false);
    assert!(value["chat_template_kwargs"]
        .get("reasoning_effort")
        .is_none());
}

/// End-to-end wire-shape proof: the `additional_params` we attach by default in
/// [`loop_config`] must serialize into the OpenAI-compatible completion body as
/// a TOP-LEVEL `chat_template_kwargs` object — that is exactly where vLLM's
/// `--reasoning-parser` reads `enable_thinking` to turn the reasoning trace on.
/// This runs the real rig OpenAI request conversion (the same path the live
/// `CompletionsClient` uses for the d4f backend) and asserts the flattened body,
/// so it proves the kwarg reaches the server without needing a live endpoint.
#[test]
fn profile_reasoning_serializes_top_level_into_openai_body() {
    use rig::completion::message::{Message, Text, UserContent};
    use rig::one_or_many::OneOrMany;

    // The profile-derived additional params for a DeepSeek-compatible backend.
    let additional_params = merge_optional_params(
        merge_optional_params(
            reasoning_profile_params(
                BackendProviderKind::OpenAiCompatible,
                crate::OpenAiWireApi::ChatCompletions,
                Some(crate::config::ReasoningEffort::Max),
            ),
            provider_additional_params(BackendProviderKind::OpenAiCompatible),
        ),
        SamplingConfig::default().additional_params(),
    );

    let core_req = rig::completion::CompletionRequest {
        model: None,
        preamble: None,
        chat_history: OneOrMany::one(Message::User {
            content: OneOrMany::one(UserContent::Text(Text {
                text: "hi".to_string(),
            })),
        }),
        documents: Vec::new(),
        tools: Vec::new(),
        temperature: None,
        max_tokens: None,
        tool_choice: None,
        additional_params,
        output_schema: None,
    };

    // Same conversion the live OpenAI CompletionsClient performs before POSTing
    // to `/chat/completions`.
    let openai_req =
        rig::providers::openai::CompletionRequest::try_from(("d4f".to_string(), core_req))
            .expect("openai request conversion should succeed");
    let body = serde_json::to_value(&openai_req).expect("serializing openai request");

    // Flattened to the top level of the request body — NOT nested under any
    // wrapper — which is where vLLM expects it.
    assert_eq!(
        body["chat_template_kwargs"]["enable_thinking"], true,
        "request body must carry top-level chat_template_kwargs.enable_thinking=true; body was {body}"
    );
    assert_eq!(body["chat_template_kwargs"]["reasoning_effort"], "max");
}

#[test]
fn openrouter_additional_params_require_parameters() {
    let value = provider_additional_params(BackendProviderKind::OpenRouter)
        .expect("OpenRouter should contribute additional params");

    assert_eq!(value["provider"]["require_parameters"], true);
}

#[test]
fn openai_compatible_has_no_provider_specific_additional_params() {
    assert!(provider_additional_params(BackendProviderKind::OpenAiCompatible).is_none());
}

#[test]
fn sampling_additional_params_merge_with_provider_params() {
    let sampling = SamplingConfig {
        temperature: Some(0.1),
        top_p: Some(0.95),
        top_k: Some(40),
        max_tokens: Some(1024),
        ..Default::default()
    };

    let value = merge_optional_params(
        provider_additional_params(BackendProviderKind::OpenRouter),
        sampling.additional_params(),
    )
    .expect("sampling params should be present");

    assert_eq!(value["provider"]["require_parameters"], true);
    assert_eq!(value["top_p"], 0.95);
    assert_eq!(value["top_k"], 40);
    assert!(value.get("max_tokens").is_none());
    assert!(value.get("temperature").is_none());
}

#[test]
fn sampling_additional_params_omit_dedicated_completion_fields() {
    let sampling = SamplingConfig {
        temperature: Some(0.1),
        top_p: None,
        top_k: None,
        max_tokens: Some(1024),
        ..Default::default()
    };

    assert!(sampling.additional_params().is_none());
}

#[test]
fn request_sampling_overrides_behavior_defaults() {
    let defaults = SamplingConfig {
        temperature: Some(0.7),
        top_p: Some(0.9),
        top_k: Some(20),
        max_tokens: Some(2048),
        ..Default::default()
    };
    let request = AgentRequest {
        doc_id: String::new(),
        request_id: String::new(),
        agent_did: String::new(),
        requester_did: None,
        behavior_id: None,
        session_id: String::new(),
        content: String::new(),
        temperature: Some(0.0),
        top_p: None,
        top_k: Some(40),
        seed: Some(1234),
        max_tokens: Some(512),
        max_total_tokens: Some(4096),
        metadata: Some(r#"{"run_id":"foo"}"#.to_string()),
        execution_origin: None,
        created_at: String::new(),
        deadline: None,
        subagent_depth: 0,
        caused_by_parent_request_id: None,
        caused_by_parent_request_doc_id: None,
        caused_by_parent_tool_call_id: None,
        caused_by_parent_tool_call_doc_id: None,
        caused_by_trigger_id: None,
        caused_by_trigger_kind: None,
        caused_by_source_doc_id: None,
        caused_by_correlation: None,
        caused_by_trigger_context: None,
        workspace_id: None,
        workspace_authority: None,
        workspace_owner_deployment_id: None,
        workspace_seal_hash: None,
    };

    let sampling = sampling_for_request(defaults, &request);

    assert_eq!(sampling.temperature, Some(0.0));
    assert_eq!(sampling.top_p, Some(0.9));
    assert_eq!(sampling.top_k, Some(40));
    assert_eq!(sampling.seed, Some(1234));
    assert_eq!(sampling.max_tokens, Some(512));
}

#[test]
fn effective_max_tokens_falls_back_to_behavior_budget() {
    assert_eq!(effective_max_tokens(4096, None), Some(4096));
}

#[test]
fn effective_max_tokens_prefers_sampling_override() {
    assert_eq!(effective_max_tokens(4096, Some(512)), Some(512));
}

#[test]
fn openai_cache_scope_prefers_session_id() {
    let value = openai_cache_scope_params(&request()).expect("scope should be present");

    assert_eq!(value["user"], "session-456");
}

#[test]
fn openai_cache_scope_falls_back_to_request_id() {
    let mut request = request();
    request.session_id.clear();

    let value = openai_cache_scope_params(&request).expect("fallback scope should be present");

    assert_eq!(value["user"], "request-123");
}

#[test]
fn loop_config_for_request_resolves_completion_retry_policy_and_deadline() {
    let behavior = behavior_with_retry(CompletionRetryProfileFields {
        retry_interactive_max: Some(2),
        ..Default::default()
    });
    let mut request = request();
    request.execution_origin = Some("interactive".to_string());
    request.deadline = Some("2030-01-01T00:00:00Z".to_string());

    let config =
        loop_config_for_request(&behavior, "preamble".to_string(), &request, None, 0).unwrap();

    assert_eq!(
        config.retry_policy.transport_backoff,
        vec![Duration::from_secs(2), Duration::from_secs(2)]
    );
    assert_eq!(
        config.deadline.map(|deadline| deadline.timestamp()),
        Some(1_893_456_000)
    );
}

#[test]
fn loop_config_for_request_defaults_unknown_retry_origin_to_scheduled() {
    let behavior = behavior_with_retry(CompletionRetryProfileFields::default());
    let mut request = request();
    request.execution_origin = Some("legacy-or-missing".to_string());

    let config =
        loop_config_for_request(&behavior, "preamble".to_string(), &request, None, 0).unwrap();

    assert_eq!(
        config.retry_policy,
        CompletionRetryPolicy::scheduled_default()
    );
}

#[test]
fn request_seed_rejects_provider_paths_without_seed_support() {
    let mut behavior = behavior_with_retry(CompletionRetryProfileFields::default());
    behavior.openai_wire_api = crate::OpenAiWireApi::Responses;
    let mut request = request();
    request.seed = Some(1234);

    let Err(error) = loop_config_for_request(&behavior, "preamble".to_string(), &request, None, 0)
    else {
        panic!("Responses must reject a sampling seed");
    };
    assert_eq!(
        error.to_string(),
        "sampling seed is unsupported by provider OpenAiCompatible on the responses wire"
    );
}

#[test]
fn profile_seed_rejects_provider_paths_without_seed_support() {
    let sampling = SamplingConfig {
        seed: Some(1234),
        ..Default::default()
    };

    sampling
        .validate_for_provider(
            BackendProviderKind::OpenAiCompatible,
            crate::OpenAiWireApi::ChatCompletions,
        )
        .unwrap();
    sampling
        .validate_for_provider(
            BackendProviderKind::OpenRouter,
            crate::OpenAiWireApi::ChatCompletions,
        )
        .unwrap();
    assert!(sampling
        .validate_for_provider(
            BackendProviderKind::ChatGptCodex,
            crate::OpenAiWireApi::Responses,
        )
        .is_err());
    assert!(sampling
        .validate_for_provider(
            BackendProviderKind::XaiGrokOAuth,
            crate::OpenAiWireApi::ChatCompletions,
        )
        .is_err());
}

fn behavior_with_retry(completion_retry: CompletionRetryProfileFields) -> AgentBehavior {
    let identity = Arc::new(
        KeyIdentity::load_or_create(
            std::env::temp_dir().join(format!("completion-factory-{}.key", uuid::Uuid::new_v4())),
            None,
        )
        .unwrap(),
    );
    let principal = Arc::new(AgentPrincipal {
        agent_did: identity.did().to_string(),
        identity,
        default_behavior_id: "general".to_string(),
        display_name: None,
        enabled: true,
    });

    AgentBehavior {
        behavior_id: "general".to_string(),
        principal,
        backend_id: Some("backend-general".to_string()),
        backend_provider_kind: BackendProviderKind::OpenAiCompatible,
        openai_wire_api: crate::OpenAiWireApi::ChatCompletions,
        backend_endpoint: "http://127.0.0.1:8999/v1".to_string(),
        backend_api_key: None,
        backend_api_key_env_var: None,
        model_name: crate::config::DEFAULT_MODEL_NAME.to_string(),
        context_window: crate::config::DEFAULT_CONTEXT_WINDOW,
        max_output_tokens: crate::config::DEFAULT_MAX_OUTPUT_TOKENS,
        max_turns: crate::config::DEFAULT_MAX_TURNS,
        system_prompt: "system".to_string(),
        request_context_template: None,
        tools: BehaviorToolConfig::meta_only(),
        compaction_threshold: crate::config::DEFAULT_COMPACTION_THRESHOLD,
        compaction_strategy: CompactionStrategy::StripThenSummarize,
        stream_batch_ms: crate::config::DEFAULT_STREAM_BATCH_MS,
        stream_liveness_timeout: Duration::from_secs(
            crate::config::DEFAULT_STREAM_LIVENESS_TIMEOUT_SECS,
        ),
        deadline_duration: Duration::from_secs(crate::config::DEFAULT_DEADLINE_DURATION_SECS),
        completion_retry,
        sampling: SamplingConfig::default(),
        skills: Vec::new(),
    }
}

#[test]
fn request_budget_construction_fails_closed_on_non_positive_values() {
    let behavior = behavior_with_retry(CompletionRetryProfileFields::default());
    for invalid in [-1, 0] {
        let error = parse_aggregate_token_limit(Some(invalid))
            .err()
            .expect("non-positive aggregate budget must be rejected");
        assert!(error.to_string().contains("must be a positive integer"));
    }

    assert_eq!(parse_aggregate_token_limit(None).unwrap(), None);
    assert_eq!(
        parse_aggregate_token_limit(Some(4_096)).unwrap(),
        Some(4_096)
    );

    let mut request = request();
    request.max_total_tokens = Some(4_096);
    let budget = Some(AggregateTokenBudget::new(4_096));
    assert!(
        loop_config_for_request(&behavior, "system".to_string(), &request, budget, 0)
            .unwrap()
            .aggregate_token_budget
            .is_some()
    );
}

#[test]
fn prior_usage_blocks_dispatch_when_already_at_limit() {
    let budget = AggregateTokenBudget::with_prior_usage(1_000, 1_000);
    let ledger = budget.snapshot().expect("ledger lock");
    assert_eq!(ledger.remaining(), 0);
    assert!(!ledger.can_dispatch(1, 100));
}

#[test]
fn prior_usage_decode_fails_closed_on_malformed_payload() {
    let err = super::prior_usage_rows_from_response(Some(&serde_json::json!({"not": "an-array"})))
        .expect_err("object payload must not decode as empty usage");
    assert!(
        err.to_string().contains("not a row array"),
        "unexpected error: {err}"
    );
    assert!(super::prior_usage_rows_from_response(None)
        .unwrap()
        .is_empty());
    assert!(
        super::prior_usage_rows_from_response(Some(&serde_json::Value::Null))
            .unwrap()
            .is_empty()
    );
    let rows = super::prior_usage_rows_from_response(Some(&serde_json::json!([
        {"prompt_tokens": 100, "completion_tokens": 50},
        {"prompt_tokens": 200, "completion_tokens": 10},
    ])))
    .unwrap();
    assert_eq!(
        crate::provider_usage::sum_charged_from_persisted_parts(
            rows.into_iter()
                .map(|row| (row.prompt_tokens, row.completion_tokens))
        )
        .unwrap(),
        360
    );
}

#[test]
fn prior_usage_decode_rejects_partial_or_negative_components() {
    for payload in [
        serde_json::json!([{"prompt_tokens": 100, "completion_tokens": null}]),
        serde_json::json!([{"prompt_tokens": null, "completion_tokens": 50}]),
        serde_json::json!([{"prompt_tokens": -1, "completion_tokens": 50}]),
        serde_json::json!([{"prompt_tokens": 100, "completion_tokens": -1}]),
    ] {
        let rows = super::prior_usage_rows_from_response(Some(&payload)).unwrap();
        assert!(
            crate::provider_usage::sum_charged_from_persisted_parts(
                rows.into_iter()
                    .map(|row| (row.prompt_tokens, row.completion_tokens))
            )
            .is_err(),
            "invalid durable usage must fail closed: {payload}"
        );
    }
}

#[tokio::test]
async fn rehydrates_aggregate_budget_from_durable_inference_calls() {
    use crate::schema::ensure_schemas;
    use defra_node::EmbeddedNode;

    let node = EmbeddedNode::builder().build().await.unwrap();
    ensure_schemas(&node).await.unwrap();

    let request_doc_id = "doc-budget-rehydrate";
    let seed = r#"mutation {
        create_InferenceCall(input: {
            call_id: "call-1"
            runtime_instance_id: "runtime-1"
            request_id: "req-budget"
            request_doc_id: "doc-budget-rehydrate"
            call_seq: 1
            backend_id: "backend-1"
            behavior_id: "behavior-1"
            agent_did: "did:test:agent"
            call_kind: "inference"
            attempt: 1
            call_state: "completed"
            prompt_tokens: 100
            completion_tokens: 50
            queue_depth_at_enqueue: 0
            controller_generation: 1
            backend_config_fingerprint: "fp"
        }) { _docID }
        create_InferenceCall(input: {
            call_id: "call-2"
            runtime_instance_id: "runtime-1"
            request_id: "req-budget"
            request_doc_id: "doc-budget-rehydrate"
            call_seq: 2
            backend_id: "backend-1"
            behavior_id: "behavior-1"
            agent_did: "did:test:agent"
            call_kind: "compaction"
            attempt: 1
            call_state: "completed"
            prompt_tokens: 200
            completion_tokens: 10
            queue_depth_at_enqueue: 0
            controller_generation: 1
            backend_config_fingerprint: "fp"
        }) { _docID }
    }"#;
    let response = node.execute(seed).await;
    assert!(
        !response.has_errors(),
        "seed InferenceCall rows: {:?}",
        response.errors
    );

    let mut request = request();
    request.doc_id = request_doc_id.to_string();
    request.request_id = "req-budget".to_string();
    request.max_total_tokens = Some(1_000);

    let budget = aggregate_token_budget_for_request(&node, &request)
        .await
        .expect("rehydrate")
        .expect("budget present");
    let ledger = budget.snapshot().expect("ledger");
    assert_eq!(ledger.used, 360, "100+50 + 200+10 charged totals");
    assert_eq!(ledger.limit, 1_000);
    assert!(
        !ledger.can_dispatch(700, 100),
        "remaining 640 cannot cover 700 input"
    );
    assert!(ledger.can_dispatch(100, 100));

    node.shutdown().await;
}

/// #649: every sampling knob a profile can pin must reach the provider body.
///
/// rig's `CompletionRequest` models only `temperature`/`max_tokens`, so the
/// rest ride `additional_params` — if a knob is missing here it is silently at
/// the mercy of the served checkpoint's `generation_config.json`, which is
/// exactly the gap #649 reports.
#[test]
fn every_profile_sampling_knob_reaches_the_provider_body() {
    let sampling = SamplingConfig {
        temperature: Some(0.7),
        top_p: Some(0.95),
        top_k: Some(40),
        seed: Some(1234),
        min_p: Some(0.05),
        frequency_penalty: Some(0.5),
        presence_penalty: Some(-0.25),
        repetition_penalty: Some(1.1),
        max_tokens: Some(1024),
        reasoning_effort: None,
    };

    let value = sampling
        .additional_params()
        .expect("pinned sampling knobs must produce provider params");

    assert_eq!(value["top_p"], 0.95);
    assert_eq!(value["top_k"], 40);
    assert_eq!(value["seed"], 1234);
    assert_eq!(value["min_p"], 0.05);
    assert_eq!(value["frequency_penalty"], 0.5);
    assert_eq!(value["presence_penalty"], -0.25);
    assert_eq!(value["repetition_penalty"], 1.1);
    // temperature and max_tokens are modeled rig fields, not body extras —
    // emitting them here too would double-send them.
    assert!(value.get("temperature").is_none());
    assert!(value.get("max_tokens").is_none());
}

/// An unpinned knob must emit NOTHING: the served model's own default has to
/// stand. Emitting a null/zero would silently override the checkpoint.
#[test]
fn unpinned_sampling_knobs_emit_no_provider_params() {
    let sampling = SamplingConfig {
        temperature: Some(0.0),
        max_tokens: Some(256),
        ..Default::default()
    };
    assert!(
        sampling.additional_params().is_none(),
        "a profile that pins no body-param knob must not send any"
    );
}

/// Capture is a property of every completion loop, not a privilege of the
/// inference path (#840). `loop_config` is the only place a loop's capture
/// scope is chosen, so this is where a call site that forgets one — or reuses
/// another's — becomes visible.
#[tokio::test]
async fn every_loop_config_arms_the_capture_scope_it_was_built_for() {
    use crate::rendered_request::scope::{
        claim_pending, scope_request, test_scope, CaptureClaim, CaptureScopeKind,
    };
    use crate::rendered_request::{
        AssemblyBuildPath, AssemblyTrace, RenderedRequestCaptureSink, RenderedRequestContext,
    };

    let behavior = behavior_with_retry(CompletionRetryProfileFields::default());
    let context = RenderedRequestContext {
        request_doc_id: "doc-1".to_string(),
        request_commit_cid: "bafy-request-commit".to_string(),
        request_id: "req-1".to_string(),
        agent_did: "did:key:agent".to_string(),
        requester_did: String::new(),
        behavior_id: "general".to_string(),
        session_id: "session-1".to_string(),
        model_name: "model".to_string(),
    };
    let sink: RenderedRequestCaptureSink = Arc::new(|_| Box::pin(async { Ok(()) }));
    let scope = test_scope(context, sink);

    scope_request(scope, async {
        for (kind, expected) in [
            (CaptureScopeKind::Inference, "inference.1"),
            (CaptureScopeKind::Compaction, "compaction.1"),
            (
                CaptureScopeKind::CompactionFallback,
                "compaction_fallback.1",
            ),
            (CaptureScopeKind::Title, "title.1"),
            (CaptureScopeKind::OneShot, "oneshot.1"),
        ] {
            let config = loop_config(&behavior, "preamble".to_string(), 0, kind);
            let on_rendered_request = config
                .on_rendered_request
                .clone()
                .expect("every production loop config installs an arming sink");
            on_rendered_request(
                0,
                0,
                rig::completion::CompletionRequest {
                    model: None,
                    preamble: None,
                    chat_history: rig::one_or_many::OneOrMany::one(rig::completion::Message::user(
                        "hi",
                    )),
                    documents: Vec::new(),
                    tools: Vec::new(),
                    temperature: None,
                    max_tokens: None,
                    tool_choice: None,
                    additional_params: None,
                    output_schema: None,
                },
                AssemblyTrace::from_effective_messages(AssemblyBuildPath::Budgeted, Vec::new()),
            )
            .await
            .expect("arming never fails");

            let (_, claim) = claim_pending().expect("a scope is installed");
            let CaptureClaim::Armed(pending) = claim else {
                panic!("{kind} did not arm a pending capture");
            };
            assert_eq!(
                pending.capture_scope, expected,
                "{kind} armed the wrong scope"
            );
        }
    })
    .await;
}

/// `loop_config_for_request` is the inference path's entry point; it must not
/// silently drop the capture the daemon depends on.
#[test]
fn loop_config_for_request_keeps_the_inference_capture_scope() {
    let behavior = behavior_with_retry(CompletionRetryProfileFields::default());
    let config =
        loop_config_for_request(&behavior, "preamble".to_string(), &request(), None, 0).unwrap();
    assert!(
        config.on_rendered_request.is_some(),
        "the request path must arm a rendered-request capture"
    );
}
