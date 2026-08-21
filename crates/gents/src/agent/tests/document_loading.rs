use std::sync::Arc;
use std::time::Duration;

use super::super::*;
use super::support::*;
use crate::default_behavior_id_for_agent;
use crate::document_config::{AgentBehavior, ToolSelectionDocument};
use crate::ensure_runtime_schemas;
use crate::tool_surface::ToolCeiling;
use crate::toolset::ToolSet;

#[tokio::test]
async fn document_constructor_rejects_node_without_signing_did_before_migrations() {
    let node = Arc::new(EmbeddedNode::builder().build().await.unwrap());
    let identity = Arc::new(test_identity("document-constructor-unsigned-node"));

    let error = match Gents::from_default_behavior_documents(
        node,
        identity,
        DocumentRuntimeOptions::default(),
    )
    .await
    {
        Ok(_) => panic!("document constructor should reject an unsigned node"),
        Err(error) => error,
    };

    assert!(error
        .to_string()
        .contains("EmbeddedNode configured with a node signing DID"));
}

#[tokio::test]
async fn from_default_behavior_documents_marks_unbound_default_behavior_unavailable() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("bootstrap-profile"));
    let did = identity.did().to_string();
    let default_behavior_id = default_behavior_id_for_agent(&did);

    let agent = Gents::from_default_behavior_documents(
        node,
        identity.clone(),
        DocumentRuntimeOptions {
            tool_ceiling: ToolCeiling::readonly(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert!(agent.behaviors().is_empty());
    assert_eq!(agent.default_behavior_id(), default_behavior_id);
    assert_eq!(agent.agent_did(), did);
    assert_eq!(
        agent
            .unavailable_behaviors()
            .get(default_behavior_id.as_str())
            .map(String::as_str),
        Some(format!("behavior {default_behavior_id} has no backend binding").as_str())
    );
}

#[tokio::test]
async fn from_default_behavior_documents_composes_behavior_and_inference_profile() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("composed-profile"));
    let did = identity.did().to_string();
    let default_behavior_id = default_behavior_id_for_agent(&did);

    crate::ensure_agent_principal(node.as_ref(), &did)
        .await
        .unwrap();
    insert_backend(
        node.as_ref(),
        "backend-balanced",
        "http://127.0.0.1:8123/v1",
    )
    .await;
    insert_inference_profile(node.as_ref(), "balanced").await;
    update_default_behavior(
        node.as_ref(),
        &default_behavior_id,
        "balanced",
        "You are precise.",
        "backend-balanced",
        "gpt-local",
        "Summarize",
        0.6,
    )
    .await;

    let agent = Gents::from_default_behavior_documents(
        node,
        identity.clone(),
        DocumentRuntimeOptions {
            tool_ceiling: ToolCeiling::readonly(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let behavior = &agent.behaviors()[0];
    assert_eq!(behavior.behavior_id, default_behavior_id);
    assert_eq!(behavior.agent_did(), did);
    assert_eq!(behavior.backend_endpoint, "http://127.0.0.1:8123/v1");
    assert_eq!(behavior.model_name, "gpt-local");
    assert_eq!(behavior.context_window, 32768);
    assert_eq!(behavior.max_output_tokens, 4096);
    assert_eq!(behavior.max_turns, 8);
    assert_eq!(behavior.system_prompt, "You are precise.");
    assert_eq!(behavior.backend_id.as_deref(), Some("backend-balanced"));
    assert!(matches!(
        behavior.compaction_strategy,
        crate::compaction::CompactionStrategy::Summarize
    ));
    assert_eq!(behavior.compaction_threshold, 0.6);
    assert_eq!(behavior.stream_batch_ms, 500);
    assert_eq!(behavior.stream_liveness_timeout, Duration::from_secs(45));
    assert_eq!(behavior.deadline_duration, Duration::from_secs(120));

    // #649: the profile's sampling knobs must land on the behavior. This hop
    // hardcoded `top_p: None, top_k: None`, so a profile could not express any
    // sampling beyond temperature and every agent silently inherited whatever
    // the served checkpoint's generation_config.json baked in.
    assert_eq!(behavior.sampling.temperature, Some(0.2));
    assert_eq!(behavior.sampling.top_p, Some(0.95));
    assert_eq!(behavior.sampling.top_k, Some(40));
    assert_eq!(behavior.sampling.min_p, Some(0.05));
    assert_eq!(behavior.sampling.frequency_penalty, Some(0.5));
    assert_eq!(behavior.sampling.presence_penalty, Some(-0.25));
    assert_eq!(behavior.sampling.repetition_penalty, Some(1.1));
    assert_eq!(
        behavior.sampling.reasoning_effort,
        Some(crate::config::ReasoningEffort::Max)
    );

    // ...and from the behavior they must reach the provider request body.
    let params = behavior
        .sampling
        .additional_params()
        .expect("pinned sampling knobs must produce provider body params");
    assert_eq!(params["top_p"], 0.95);
    assert_eq!(params["top_k"], 40);
    assert_eq!(params["repetition_penalty"], 1.1);
}

#[tokio::test]
async fn from_default_behavior_documents_resolves_tool_selection_with_ceiling() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("tool-selection"));
    let did = identity.did().to_string();
    let default_behavior_id = default_behavior_id_for_agent(&did);
    let selection_id = crate::default_tool_selection_id_for_behavior(&default_behavior_id);

    let bootstrap = crate::ensure_agent_principal(node.as_ref(), &did)
        .await
        .unwrap();
    insert_backend(node.as_ref(), "backend-tools", "http://127.0.0.1:8222/v1").await;
    crate::upsert_tool_selection(
        node.as_ref(),
        &ToolSelectionDocument {
            selection_id: selection_id.clone(),
            agent_did: did.clone(),
            display_name: Some("Ops".to_string()),
            tool_policy_version: None,
            enable_file_tools: Some(true),
            file_tools_mode: Some("ReadWrite".to_string()),
            file_tool_root: None,
            enable_bash: Some(true),
            bash_mode: Some("Unrestricted".to_string()),
            command_execution_policy: None,
            command_allowed_argv_prefixes: Some(Vec::new()),
            command_forbidden_argv_prefixes: Some(Vec::new()),
            command_network_mode: None,
            cli_tool_names: Some(Vec::new()),
            enable_meta_tools: Some(false),
            allowed_mcp_service_ids: Some(Vec::new()),
            subagent_targets: Some(vec![
                crate::document_config::subagent_target_entry(
                    "researcher",
                    &did,
                    "researcher",
                    None,
                ),
                crate::document_config::subagent_target_entry(
                    "researcher",
                    &did,
                    "researcher",
                    None,
                ),
            ]),
            subagent_spawn_enabled: Some(true),
            subagent_steering_enabled: Some(true),
            subagent_background_enabled: Some(true),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    crate::upsert_agent_behavior(
        node.as_ref(),
        &AgentBehavior {
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            behavior_id: bootstrap.default_behavior.behavior_id,
            agent_did: did.clone(),
            display_name: Some("Default".to_string()),
            description: None,
            summary: None,
            system_prompt: Some("Use tools carefully.".to_string()),
            request_context_template: None,
            backend_id: Some("backend-tools".to_string()),
            model_name: None,
            tool_selection_id: Some(selection_id.clone()),
            inference_profile_id: bootstrap.default_behavior.inference_profile_id.clone(),
            compaction_strategy: Some("StripThenSummarize".to_string()),
            compaction_threshold: Some(0.75),
            enabled: true,
            created_at: bootstrap.default_behavior.created_at.clone(),
        },
    )
    .await
    .unwrap();
    crate::upsert_agent_behavior(
        node.as_ref(),
        &AgentBehavior {
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            behavior_id: "researcher".to_string(),
            agent_did: did.clone(),
            display_name: Some("Researcher".to_string()),
            description: None,
            summary: None,
            system_prompt: Some("Research carefully.".to_string()),
            request_context_template: None,
            backend_id: Some("backend-tools".to_string()),
            model_name: None,
            tool_selection_id: None,
            inference_profile_id: bootstrap.default_behavior.inference_profile_id,
            compaction_strategy: None,
            compaction_threshold: None,
            enabled: true,
            created_at: Some(chrono::Utc::now().to_rfc3339()),
        },
    )
    .await
    .unwrap();

    let agent = Gents::from_default_behavior_documents(
        node,
        identity,
        DocumentRuntimeOptions {
            tool_ceiling: ToolCeiling::readonly(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let behavior = &agent.behaviors()[0];
    assert_eq!(behavior.behavior_id, default_behavior_id);
    assert_eq!(behavior.tools.host_tools(), &ToolSet::readonly());
    assert!(!behavior.tools.meta_tools_requested());
    assert_eq!(
        behavior
            .tools
            .subagent_tools()
            .targets
            .iter()
            .map(|target| target.name.clone())
            .collect::<Vec<_>>(),
        ["researcher".to_string()]
    );
    assert!(behavior.tools.subagent_tools().spawn_enabled);
    assert!(behavior.tools.subagent_tools().steering_enabled);
    assert!(behavior.tools.subagent_tools().background_enabled);
    let snapshot = resolve_document_runtime_snapshot(
        agent.node.as_ref(),
        agent.document_runtime_context().unwrap(),
    )
    .await
    .unwrap();
    let tool_surface = snapshot
        .tool_surfaces
        .get(&default_behavior_id)
        .expect("tool surface for default behavior");
    let tool_names = tool_surface.tool_names();
    assert!(tool_names.contains(&"spawn_subagent".to_string()));
    assert!(tool_names.contains(&"wait_subagent".to_string()));
    assert!(tool_names.contains(&"list_subagents".to_string()));
    assert!(tool_names.contains(&"read_subagent".to_string()));
    assert!(tool_names.contains(&"steer_subagent".to_string()));
    assert!(tool_names.contains(&"cancel_subagent".to_string()));
}

#[tokio::test]
async fn from_default_behavior_documents_filters_inactive_subagent_targets_from_surface() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("subagent-target-disabled"));
    let did = identity.did().to_string();
    let default_behavior_id = default_behavior_id_for_agent(&did);
    let selection_id = crate::default_tool_selection_id_for_behavior(&default_behavior_id);

    let bootstrap = crate::ensure_agent_principal(node.as_ref(), &did)
        .await
        .unwrap();
    insert_backend(
        node.as_ref(),
        "backend-disabled-target",
        "http://127.0.0.1:8234/v1",
    )
    .await;
    crate::upsert_tool_selection(
        node.as_ref(),
        &ToolSelectionDocument {
            selection_id: selection_id.clone(),
            agent_did: did.clone(),
            enable_meta_tools: Some(false),
            subagent_targets: Some(vec![crate::document_config::subagent_target_entry(
                "disabled-researcher",
                &did,
                "disabled-researcher",
                None,
            )]),
            subagent_spawn_enabled: Some(true),
            subagent_background_enabled: Some(true),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    crate::upsert_agent_behavior(
        node.as_ref(),
        &AgentBehavior {
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            behavior_id: bootstrap.default_behavior.behavior_id,
            agent_did: did.clone(),
            display_name: Some("Default".to_string()),
            description: None,
            summary: None,
            system_prompt: Some("Use tools carefully.".to_string()),
            request_context_template: None,
            backend_id: Some("backend-disabled-target".to_string()),
            model_name: None,
            tool_selection_id: Some(selection_id),
            inference_profile_id: bootstrap.default_behavior.inference_profile_id,
            compaction_strategy: Some("StripThenSummarize".to_string()),
            compaction_threshold: Some(0.75),
            enabled: true,
            created_at: bootstrap.default_behavior.created_at,
        },
    )
    .await
    .unwrap();
    crate::upsert_agent_behavior(
        node.as_ref(),
        &AgentBehavior {
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            behavior_id: "disabled-researcher".to_string(),
            agent_did: did.clone(),
            display_name: Some("Disabled Researcher".to_string()),
            description: None,
            summary: None,
            system_prompt: Some("Research carefully.".to_string()),
            request_context_template: None,
            backend_id: Some("backend-disabled-target".to_string()),
            model_name: None,
            tool_selection_id: None,
            inference_profile_id: None,
            compaction_strategy: None,
            compaction_threshold: None,
            enabled: false,
            created_at: Some(chrono::Utc::now().to_rfc3339()),
        },
    )
    .await
    .unwrap();

    let snapshot = resolve_document_runtime_snapshot(
        node.as_ref(),
        &DocumentResolveContext {
            identity,
            tool_ceiling: ToolCeiling::readonly(),
            backend_health: crate::backend_health::BackendHealthMap::new(),
        },
    )
    .await
    .unwrap();
    let tool_names = snapshot
        .tool_surfaces
        .get(&default_behavior_id)
        .expect("tool surface for default behavior")
        .tool_names();

    assert!(!tool_names.contains(&"spawn_subagent".to_string()));
    assert!(!tool_names.contains(&"wait_subagent".to_string()));
    assert!(!tool_names.contains(&"cancel_subagent".to_string()));
}

#[tokio::test]
async fn from_default_behavior_documents_rejects_unresolved_subagent_target() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("subagent-target-missing"));
    let did = identity.did().to_string();
    let default_behavior_id = default_behavior_id_for_agent(&did);
    let selection_id = crate::default_tool_selection_id_for_behavior(&default_behavior_id);

    let bootstrap = crate::ensure_agent_principal(node.as_ref(), &did)
        .await
        .unwrap();
    insert_backend(
        node.as_ref(),
        "backend-missing-target",
        "http://127.0.0.1:8233/v1",
    )
    .await;
    crate::upsert_tool_selection(
        node.as_ref(),
        &ToolSelectionDocument {
            selection_id: selection_id.clone(),
            agent_did: did.clone(),
            enable_meta_tools: Some(false),
            subagent_targets: Some(vec![crate::document_config::subagent_target_entry(
                "missing-behavior",
                &did,
                "missing-behavior",
                None,
            )]),
            subagent_spawn_enabled: Some(true),
            subagent_background_enabled: Some(true),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    crate::upsert_agent_behavior(
        node.as_ref(),
        &AgentBehavior {
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            behavior_id: bootstrap.default_behavior.behavior_id,
            agent_did: did.clone(),
            display_name: Some("Default".to_string()),
            description: None,
            summary: None,
            system_prompt: Some("Use tools carefully.".to_string()),
            request_context_template: None,
            backend_id: Some("backend-missing-target".to_string()),
            model_name: None,
            tool_selection_id: Some(selection_id),
            inference_profile_id: bootstrap.default_behavior.inference_profile_id,
            compaction_strategy: Some("StripThenSummarize".to_string()),
            compaction_threshold: Some(0.75),
            enabled: true,
            created_at: bootstrap.default_behavior.created_at,
        },
    )
    .await
    .unwrap();

    let agent = Gents::from_default_behavior_documents(
        node,
        identity,
        DocumentRuntimeOptions {
            tool_ceiling: ToolCeiling::readonly(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert!(agent.behaviors().is_empty());
    assert!(agent
        .unavailable_behaviors()
        .get(default_behavior_id.as_str())
        .is_some_and(|message| message.contains("subagent_targets entry")));
}

#[tokio::test]
async fn from_default_behavior_documents_loads_runnable_behaviors_and_tracks_unavailable() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("behavior-catalog"));
    let did = identity.did().to_string();
    let default_behavior_id = default_behavior_id_for_agent(&did);

    crate::ensure_agent_principal(node.as_ref(), &did)
        .await
        .unwrap();
    let default_profile_id = crate::default_inference_profile_id_for_behavior(&default_behavior_id);
    insert_backend_with_health(
        node.as_ref(),
        "backend-healthy",
        "http://127.0.0.1:8444/v1",
        true,
        "healthy",
    )
    .await;
    insert_backend_with_health(
        node.as_ref(),
        "backend-unhealthy",
        "http://127.0.0.1:8555/v1",
        true,
        "unhealthy",
    )
    .await;
    crate::upsert_agent_behavior(
        node.as_ref(),
        &AgentBehavior {
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            behavior_id: "code".to_string(),
            agent_did: did.clone(),
            display_name: Some("Code".to_string()),
            description: None,
            summary: None,
            system_prompt: Some("You write code.".to_string()),
            request_context_template: None,
            backend_id: Some("backend-healthy".to_string()),
            model_name: Some("gpt-code".to_string()),
            tool_selection_id: None,
            inference_profile_id: Some(default_profile_id),
            compaction_strategy: Some("StripThenSummarize".to_string()),
            compaction_threshold: Some(0.7),
            enabled: true,
            created_at: None,
        },
    )
    .await
    .unwrap();
    crate::upsert_agent_behavior(
        node.as_ref(),
        &AgentBehavior {
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            behavior_id: "broken".to_string(),
            agent_did: did.clone(),
            display_name: Some("Broken".to_string()),
            description: None,
            summary: None,
            system_prompt: Some("This backend is missing.".to_string()),
            request_context_template: None,
            backend_id: Some("backend-missing".to_string()),
            model_name: Some("gpt-missing".to_string()),
            tool_selection_id: None,
            inference_profile_id: None,
            compaction_strategy: Some("StripThenSummarize".to_string()),
            compaction_threshold: Some(0.7),
            enabled: true,
            created_at: None,
        },
    )
    .await
    .unwrap();
    crate::upsert_agent_behavior(
        node.as_ref(),
        &AgentBehavior {
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            behavior_id: "disabled".to_string(),
            agent_did: did.clone(),
            display_name: Some("Disabled".to_string()),
            description: None,
            summary: None,
            system_prompt: Some("You should never run.".to_string()),
            request_context_template: None,
            backend_id: None,
            model_name: None,
            tool_selection_id: None,
            inference_profile_id: None,
            compaction_strategy: Some("StripThenSummarize".to_string()),
            compaction_threshold: Some(0.7),
            enabled: false,
            created_at: None,
        },
    )
    .await
    .unwrap();
    crate::upsert_agent_behavior(
        node.as_ref(),
        &AgentBehavior {
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            behavior_id: "unhealthy".to_string(),
            agent_did: did.clone(),
            display_name: Some("Unhealthy".to_string()),
            description: None,
            summary: None,
            system_prompt: Some("Backend is unhealthy.".to_string()),
            request_context_template: None,
            backend_id: Some("backend-unhealthy".to_string()),
            model_name: Some("gpt-unhealthy".to_string()),
            tool_selection_id: None,
            inference_profile_id: None,
            compaction_strategy: Some("StripThenSummarize".to_string()),
            compaction_threshold: Some(0.7),
            enabled: true,
            created_at: None,
        },
    )
    .await
    .unwrap();

    let agent = Gents::from_default_behavior_documents(
        node,
        identity,
        DocumentRuntimeOptions {
            tool_ceiling: ToolCeiling::readonly(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let runnable_names = agent
        .behaviors()
        .iter()
        .map(|behavior| behavior.behavior_id.as_str())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(agent.agent_did(), did);
    assert_eq!(agent.default_behavior_id(), default_behavior_id);
    assert_eq!(agent.behaviors().len(), 1);
    assert!(runnable_names.contains("code"));
    let default_reason = agent
        .unavailable_behaviors()
        .get(default_behavior_id.as_str())
        .cloned()
        .expect("missing default behavior rejection");
    assert_eq!(
        default_reason,
        format!("behavior {default_behavior_id} has no backend binding")
    );
    let broken_reason = agent
        .unavailable_behaviors()
        .get("broken")
        .cloned()
        .expect("missing broken behavior rejection");
    assert!(broken_reason.contains("references missing backend backend-missing"));
    let disabled_reason = agent
        .unavailable_behaviors()
        .get("disabled")
        .cloned()
        .expect("missing disabled behavior rejection");
    assert_eq!(disabled_reason, "behavior disabled is disabled");
    let unhealthy_reason = agent
        .unavailable_behaviors()
        .get("unhealthy")
        .cloned()
        .expect("missing unhealthy behavior rejection");
    assert!(unhealthy_reason.contains("backend backend-unhealthy is unavailable"));
}
