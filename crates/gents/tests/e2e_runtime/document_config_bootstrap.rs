use gents::graphql::escape_graphql_string;
use gents::{
    default_behavior_id_for_agent, default_inference_profile_id_for_behavior,
    ensure_agent_principal, list_agent_behaviors, load_agent_behavior, load_inference_profile,
    upsert_agent_behavior, upsert_inference_profile, AgentBehaviorDocument, InferenceProfile,
};

use crate::support::test_db;

#[tokio::test]
async fn ensure_agent_principal_creates_and_reuses_default_behavior() {
    let db = test_db("principal-bootstrap-create").await;
    let agent_did = "did:test:amy";

    let created = ensure_agent_principal(db.node.as_ref(), agent_did)
        .await
        .expect("bootstrap succeeds");
    assert!(created.created_principal);
    assert!(created.created_default_behavior);
    assert!(created.created_default_inference_profile);
    assert_eq!(created.principal.agent_did, agent_did);
    assert_eq!(created.principal.display_name.as_deref(), Some("amy"));
    assert_eq!(
        created.principal.default_behavior_id.as_deref(),
        Some(default_behavior_id_for_agent(agent_did).as_str())
    );
    assert_eq!(
        created.default_behavior.behavior_id,
        default_behavior_id_for_agent(agent_did)
    );
    assert_eq!(
        created.default_behavior.display_name.as_deref(),
        Some("Default")
    );
    assert_eq!(
        created.default_behavior.inference_profile_id.as_deref(),
        Some(
            default_inference_profile_id_for_behavior(&default_behavior_id_for_agent(agent_did))
                .as_str()
        )
    );
    assert_eq!(
        created.default_inference_profile.profile_id,
        default_inference_profile_id_for_behavior(&default_behavior_id_for_agent(agent_did))
    );
    assert!(created.default_behavior.enabled);

    let reused = ensure_agent_principal(db.node.as_ref(), agent_did)
        .await
        .expect("second bootstrap succeeds");
    assert!(!reused.created_principal);
    assert!(!reused.created_default_behavior);
    assert!(!reused.created_default_inference_profile);

    let behaviors = list_agent_behaviors(db.node.as_ref(), agent_did)
        .await
        .expect("list behaviors");
    assert_eq!(behaviors.len(), 1);
    assert_eq!(
        behaviors[0].behavior_id,
        default_behavior_id_for_agent(agent_did)
    );
}

#[tokio::test]
async fn ensure_agent_principal_backfills_missing_default_behavior() {
    let db = test_db("principal-bootstrap-backfill").await;
    let agent_did = "did:test:backfill";
    insert_principal(db.node.as_ref(), agent_did, "").await;

    let bootstrap = ensure_agent_principal(db.node.as_ref(), agent_did)
        .await
        .expect("bootstrap succeeds");
    assert!(!bootstrap.created_principal);
    assert!(bootstrap.created_default_behavior);
    assert!(bootstrap.created_default_inference_profile);
    assert_eq!(
        bootstrap.principal.default_behavior_id.as_deref(),
        Some(default_behavior_id_for_agent(agent_did).as_str())
    );
    assert_eq!(
        bootstrap.default_behavior.inference_profile_id.as_deref(),
        Some(
            default_inference_profile_id_for_behavior(&default_behavior_id_for_agent(agent_did))
                .as_str()
        )
    );
}

#[tokio::test]
async fn ensure_agent_principal_rejects_missing_referenced_default_behavior() {
    let db = test_db("principal-bootstrap-missing-default").await;
    let agent_did = "did:test:broken";
    insert_principal(db.node.as_ref(), agent_did, "custom-behavior").await;

    let error = ensure_agent_principal(db.node.as_ref(), agent_did)
        .await
        .expect_err("bootstrap should fail");
    assert!(error
        .to_string()
        .contains("references missing default behavior custom-behavior"));
}

#[tokio::test]
async fn load_inference_profile_reads_document_fields() {
    let db = test_db("inference-profile-load").await;
    let profile_id = "balanced";
    insert_inference_profile(db.node.as_ref(), profile_id).await;

    let profile = load_inference_profile(db.node.as_ref(), profile_id)
        .await
        .expect("load succeeds")
        .expect("profile exists");
    assert_eq!(profile.profile_id, profile_id);
    assert_eq!(profile.display_name.as_deref(), Some("Balanced"));
    assert_eq!(profile.context_window, Some(32768));
    assert_eq!(profile.max_output_tokens, Some(4096));
    assert_eq!(profile.temperature, Some(0.2));
    assert_eq!(profile.top_p, Some(0.95));
    assert_eq!(profile.top_k, Some(40));
    assert_eq!(profile.min_p, Some(0.05));
    assert_eq!(profile.frequency_penalty, Some(0.5));
    assert_eq!(profile.presence_penalty, Some(-0.25));
    assert_eq!(profile.repetition_penalty, Some(1.1));
    assert_eq!(profile.reasoning_effort.as_deref(), Some("max"));
    assert_eq!(profile.stream_liveness_timeout_secs, Some(45));
    assert_eq!(profile.deadline_duration_secs, Some(120));
}

#[tokio::test]
async fn profile_sampling_knobs_reach_the_behavior_and_provider_body() {
    let db = test_db("profile-sampling-knobs").await;
    let profile_id = "sampling";
    insert_inference_profile(db.node.as_ref(), profile_id).await;

    let profile = load_inference_profile(db.node.as_ref(), profile_id)
        .await
        .expect("load succeeds")
        .expect("profile exists");

    let sampling = gents::SamplingConfig {
        temperature: profile.temperature,
        top_p: profile.top_p,
        top_k: profile.top_k,
        seed: profile.seed,
        min_p: profile.min_p,
        frequency_penalty: profile.frequency_penalty,
        presence_penalty: profile.presence_penalty,
        repetition_penalty: profile.repetition_penalty,
        max_tokens: None,
        reasoning_effort: profile
            .reasoning_effort
            .as_deref()
            .map(gents::ReasoningEffort::parse)
            .transpose()
            .expect("fixture reasoning effort must be valid"),
    };
    let params = sampling
        .additional_params()
        .expect("pinned knobs must produce provider body params");

    assert_eq!(params["top_p"], 0.95);
    assert_eq!(params["top_k"], 40);
    assert_eq!(params["seed"], 1234);
    assert_eq!(params["min_p"], 0.05);
    assert_eq!(params["frequency_penalty"], 0.5);
    assert_eq!(params["presence_penalty"], -0.25);
    assert_eq!(params["repetition_penalty"], 1.1);
}

#[tokio::test]
async fn upsert_helpers_roundtrip_behavior_and_profile() {
    let db = test_db("document-config-upsert-roundtrip").await;
    let agent_did = "did:test:roundtrip";
    let behavior_id = default_behavior_id_for_agent(agent_did);

    upsert_inference_profile(
        db.node.as_ref(),
        &InferenceProfile {
            profile_id: "balanced".to_string(),
            display_name: Some("Balanced".to_string()),
            context_window: Some(32768),
            max_output_tokens: Some(4096),
            max_turns: Some(8),
            temperature: Some(0.2),
            stream_batch_ms: Some(500),
            stream_liveness_timeout_secs: Some(45),
            deadline_duration_secs: Some(120),
            retry_max_transport: None,
            retry_backoff_ms: None,
            retry_max_resample: None,
            retry_allow_repair: None,
            retry_interactive_max: None,
            ..Default::default()
        },
    )
    .await
    .expect("upsert inference profile");

    upsert_agent_behavior(
        db.node.as_ref(),
        &AgentBehaviorDocument {
            behavior_id: behavior_id.clone(),
            agent_did: agent_did.to_string(),
            display_name: Some("Default".to_string()),
            description: None,
            summary: None,
            system_prompt: Some("Be precise".to_string()),
            request_context_template: None,
            backend_id: Some("backend-local".to_string()),
            model_name: Some("gpt-local".to_string()),
            tool_selection_id: None,
            inference_profile_id: Some("balanced".to_string()),
            compaction_strategy: Some("Summarize".to_string()),
            compaction_threshold: Some(0.6),
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            enabled: true,
            created_at: None,
        },
    )
    .await
    .expect("upsert behavior");

    let behavior = load_agent_behavior(db.node.as_ref(), &behavior_id)
        .await
        .expect("load behavior")
        .expect("behavior exists");
    assert_eq!(behavior.agent_did, agent_did);
    assert_eq!(behavior.system_prompt.as_deref(), Some("Be precise"));
    assert_eq!(behavior.backend_id.as_deref(), Some("backend-local"));
    assert_eq!(behavior.inference_profile_id.as_deref(), Some("balanced"));

    let profile = load_inference_profile(db.node.as_ref(), "balanced")
        .await
        .expect("load profile")
        .expect("profile exists");
    assert_eq!(profile.context_window, Some(32768));
    assert_eq!(profile.stream_liveness_timeout_secs, Some(45));
    assert_eq!(profile.deadline_duration_secs, Some(120));
}

#[tokio::test]
async fn tool_service_registry_schema_does_not_expose_broken_tools_relation() {
    let db = test_db("tool-service-registry-tools-relation").await;
    let response = db
        .node
        .execute(
            r#"{
                ToolServiceRegistry {
                    service_id
                    tools { name }
                }
            }"#,
        )
        .await;

    assert!(
        response.has_errors(),
        "querying the removed tools relation should fail validation"
    );
    let errors = format!("{:?}", response.errors);
    assert!(
        errors.contains("tools"),
        "expected validation error to mention tools field, got {errors}"
    );
    assert!(
        !errors.contains("TypeJoinMany"),
        "schema should not expose a tools relation that fails during join planning: {errors}"
    );
}

async fn insert_principal(
    node: &gents::defra_node::EmbeddedNode,
    agent_did: &str,
    default_behavior_id: &str,
) {
    let escaped_agent_did = escape_graphql_string(agent_did);
    let escaped_default_behavior_id = escape_graphql_string(default_behavior_id);
    let mutation = format!(
        r#"mutation {{
            create_AgentPrincipal(input: {{
                agent_did: "{escaped_agent_did}",
                display_name: "Preset",
                default_behavior_id: "{escaped_default_behavior_id}",
                enabled: true,
                created_by: "{escaped_agent_did}"
            }}) {{ _docID }}
        }}"#
    );

    let resp = node.execute(&mutation).await;
    assert!(!resp.has_errors(), "{:?}", resp.errors);
}

async fn insert_inference_profile(node: &gents::defra_node::EmbeddedNode, profile_id: &str) {
    let escaped_profile_id = escape_graphql_string(profile_id);
    let mutation = format!(
        r#"mutation {{
            create_InferenceProfile(input: {{
                profile_id: "{escaped_profile_id}",
                display_name: "Balanced",
                context_window: 32768,
                max_output_tokens: 4096,
                max_turns: 8,
                temperature: 0.2,
                top_p: 0.95,
                top_k: 40,
                seed: 1234,
                min_p: 0.05,
                frequency_penalty: 0.5,
                presence_penalty: -0.25,
                repetition_penalty: 1.1,
                reasoning_effort: "max",
                stream_batch_ms: 500,
                stream_liveness_timeout_secs: 45,
                deadline_duration_secs: 120
            }}) {{ _docID }}
        }}"#
    );

    let resp = node.execute(&mutation).await;
    assert!(!resp.has_errors(), "{:?}", resp.errors);
}
