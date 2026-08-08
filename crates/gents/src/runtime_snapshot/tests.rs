use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use super::*;
use crate::identity::{AgentIdentity as _, AgentPrincipal, KeyIdentity};

fn skill(id: &str) -> crate::skills::Skill {
    crate::skills::Skill {
        skill_id: id.to_string(),
        agent_did: "did:test:skill-owner".to_string(),
        scope: crate::skills::SkillScope::Principal,
        name: format!("{id}-name"),
        description: format!("{id}-description"),
        instructions: format!("{id}-instructions"),
        tool_refs: Vec::new(),
        display_name: None,
        enabled: true,
    }
}

fn behavior_with_skills(skills: Vec<crate::skills::Skill>) -> Arc<crate::config::AgentBehavior> {
    Arc::new(crate::config::AgentBehavior {
        behavior_id: "general".to_string(),
        principal: stub_principal(),
        backend_id: Some("backend-general".to_string()),
        backend_provider_kind: crate::backend_provider::BackendProviderKind::OpenAiCompatible,
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
        tools: crate::tool_surface::BehaviorToolConfig::meta_only(),
        compaction_threshold: crate::config::DEFAULT_COMPACTION_THRESHOLD,
        compaction_strategy: crate::compaction::CompactionStrategy::StripThenSummarize,
        stream_batch_ms: crate::config::DEFAULT_STREAM_BATCH_MS,
        stream_liveness_timeout: std::time::Duration::from_secs(
            crate::config::DEFAULT_STREAM_LIVENESS_TIMEOUT_SECS,
        ),
        deadline_duration: std::time::Duration::from_secs(
            crate::config::DEFAULT_DEADLINE_DURATION_SECS,
        ),
        completion_retry: crate::agent::completion_retry::CompletionRetryProfileFields::default(),
        sampling: crate::config::SamplingConfig::default(),
        skills,
    })
}

fn config_fact(collection: &str, logical_id: &str, cid: &str) -> crate::ConfigFactRef {
    crate::ConfigFactRef::new(
        collection,
        logical_id,
        crate::SignedDocumentVersionRef::new(
            crate::DocumentVersionRef::new(format!("doc-{collection}-{logical_id}"), cid),
            "did:key:verified-config-writer",
        ),
    )
}

fn config_provenance(
    behavior: &crate::config::AgentBehavior,
    behavior_cid: &str,
) -> crate::ResolvedBehaviorConfigProvenance {
    crate::ResolvedBehaviorConfigProvenance {
        principal: config_fact("AgentPrincipal", behavior.agent_did(), "cid-principal"),
        behavior: config_fact("AgentBehavior", &behavior.behavior_id, behavior_cid),
        inference_backend: config_fact("InferenceBackend", "backend-general", "cid-backend"),
        inference_profile: config_fact("InferenceProfile", "profile-general", "cid-profile"),
        tool_selection: None,
        skills: Vec::new(),
        resolution_algorithm_version: 1,
    }
}

/// Build a minimal `Arc<AgentPrincipal>` for tests that call `.activate()`.
/// Does not exercise signing — only satisfies the principal invariant so that
/// the `debug_assert!` in `activate()` does not fire.
fn stub_principal() -> Arc<AgentPrincipal> {
    let identity = Arc::new(
        KeyIdentity::load_or_create(
            std::env::temp_dir().join(format!("stub-principal-{}.key", uuid::Uuid::new_v4())),
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

fn snapshot(generation: u64, default_behavior_id: &str) -> Arc<ActiveRuntimeSnapshot> {
    Arc::new(ActiveRuntimeSnapshot {
        generation,
        principal: None,
        local_did: String::new(),
        paired_peer_dids: HashSet::new(),
        default_behavior_id: default_behavior_id.to_string(),
        behaviors: HashMap::new(),
        config_provenance_scope: crate::rendered_request::ConfigProvenanceScope::StaticOrOneShot,
        behavior_config_provenance: HashMap::new(),
        tool_surfaces: HashMap::new(),
        backend_admission_configs: HashMap::new(),
        unavailable_behaviors: HashMap::new(),
        active_schedules: HashMap::new(),
        unavailable_schedules: HashSet::new(),
        active_event_triggers: HashMap::new(),
        unavailable_event_triggers: HashSet::new(),
        active_tasks: HashMap::new(),
        dispatchers: HashMap::new(),
        behavior_executor_capacities: HashMap::new(),
        behavior_executor_queue_capacities: HashMap::new(),
    })
}

#[test]
fn resolved_snapshot_activate_preserves_generation_and_dispatchers() {
    let resolved = ResolvedRuntimeSnapshot {
        principal: None,
        local_did: "did:local".to_string(),
        paired_peer_dids: HashSet::from(["did:peer".to_string()]),
        default_behavior_id: "general".to_string(),
        behaviors: HashMap::new(),
        config_provenance_scope: crate::rendered_request::ConfigProvenanceScope::StaticOrOneShot,
        behavior_config_provenance: HashMap::new(),
        tool_surfaces: HashMap::new(),
        backend_admission_configs: HashMap::new(),
        unavailable_behaviors: HashMap::from([("code".to_string(), "missing backend".to_string())]),
        active_schedules: HashMap::new(),
        unavailable_schedules: HashSet::new(),
        active_event_triggers: HashMap::new(),
        unavailable_event_triggers: HashSet::new(),
        active_tasks: HashMap::new(),
    }
    .with_principal(stub_principal());
    let (general_tx, _general_rx) = mpsc::channel(1);
    let active = resolved.activate(1, HashMap::from([("general".to_string(), general_tx)]));

    assert_eq!(active.generation, 1);
    assert_eq!(active.default_behavior_id, "general");
    assert_eq!(active.local_did, "did:local");
    assert!(active.paired_peer_dids.contains("did:peer"));
    assert!(active.dispatchers.contains_key("general"));
    assert_eq!(active.unavailable_reason("code"), Some("missing backend"));
}

#[test]
fn concurrency_mode_parse_accepts_exact_known_values() {
    assert_eq!(
        ConcurrencyMode::parse("parallel"),
        Some(ConcurrencyMode::Parallel)
    );
    assert_eq!(
        ConcurrencyMode::parse("serial"),
        Some(ConcurrencyMode::Serial)
    );
    assert_eq!(
        ConcurrencyMode::parse("latest_only"),
        Some(ConcurrencyMode::LatestOnly)
    );
}

#[test]
fn concurrency_mode_parse_is_strict() {
    assert_eq!(ConcurrencyMode::parse("Parallel"), None);
    assert_eq!(ConcurrencyMode::parse("SERIAL"), None);
    assert_eq!(ConcurrencyMode::parse("latest-only"), None);
    assert_eq!(ConcurrencyMode::parse("latestOnly"), None);
    assert_eq!(ConcurrencyMode::parse(" parallel "), None);
    assert_eq!(ConcurrencyMode::parse(""), None);
}

#[test]
fn configuration_fingerprint_reflects_schedule_set() {
    let base = ResolvedRuntimeSnapshot {
        principal: None,
        local_did: String::new(),
        paired_peer_dids: HashSet::new(),
        default_behavior_id: "general".to_string(),
        behaviors: HashMap::new(),
        config_provenance_scope: crate::rendered_request::ConfigProvenanceScope::StaticOrOneShot,
        behavior_config_provenance: HashMap::new(),
        tool_surfaces: HashMap::new(),
        backend_admission_configs: HashMap::new(),
        unavailable_behaviors: HashMap::new(),
        active_schedules: HashMap::new(),
        unavailable_schedules: HashSet::new(),
        active_event_triggers: HashMap::new(),
        unavailable_event_triggers: HashSet::new(),
        active_tasks: HashMap::new(),
    };
    let baseline = base.configuration_fingerprint();

    let task = ResolvedTask {
        task_id: "t1".to_string(),
        name: None,
        behavior_id: "general".to_string(),
        prompt_template: "do the thing".to_string(),
        output_schema_ref: None,
    };
    let with_schedule = base.clone().with_schedules(
        HashMap::from([(
            "s1".to_string(),
            ResolvedSchedule {
                schedule_id: "s1".to_string(),
                task_id: "t1".to_string(),
                task: task.clone(),
                cadence: ScheduleCadence::Interval { interval_secs: 60 },
                enabled: true,
                concurrency: ConcurrencyMode::Serial,
            },
        )]),
        HashSet::new(),
    );
    assert_ne!(baseline, with_schedule.configuration_fingerprint());

    let with_unavailable = base
        .clone()
        .with_schedules(HashMap::new(), HashSet::from(["s2".to_string()]));
    assert_ne!(baseline, with_unavailable.configuration_fingerprint());
}

#[test]
fn configuration_fingerprint_is_independent_of_skill_source_order() {
    let alpha = skill("alpha");
    let beta = skill("beta");
    let forward_behavior = behavior_with_skills(vec![alpha.clone(), beta.clone()]);
    let mut permuted_behavior = forward_behavior.as_ref().clone();
    permuted_behavior.skills = vec![beta, alpha];
    let forward = ResolvedRuntimeSnapshot::from_parts(
        "general".to_string(),
        vec![forward_behavior],
        HashMap::new(),
        HashMap::new(),
    );
    let permuted = ResolvedRuntimeSnapshot::from_parts(
        "general".to_string(),
        vec![Arc::new(permuted_behavior)],
        HashMap::new(),
        HashMap::new(),
    );

    assert_eq!(
        forward.configuration_fingerprint(),
        permuted.configuration_fingerprint(),
        "the same skill set must not publish a new generation solely because its source map iterated differently"
    );
}

#[test]
fn configuration_fingerprint_rotates_on_exact_config_version_change() {
    let behavior = behavior_with_skills(Vec::new());
    let behavior_id = behavior.behavior_id.clone();
    let first = ResolvedRuntimeSnapshot::from_parts(
        behavior_id.clone(),
        vec![behavior.clone()],
        HashMap::new(),
        HashMap::new(),
    )
    .with_behavior_config_provenance(HashMap::from([(
        behavior_id.clone(),
        Arc::new(config_provenance(&behavior, "cid-behavior-a")),
    )]));
    let second = ResolvedRuntimeSnapshot::from_parts(
        behavior_id.clone(),
        vec![behavior.clone()],
        HashMap::new(),
        HashMap::new(),
    )
    .with_behavior_config_provenance(HashMap::from([(
        behavior_id,
        Arc::new(config_provenance(&behavior, "cid-behavior-b")),
    )]));

    assert_ne!(
        first.configuration_fingerprint(),
        second.configuration_fingerprint(),
        "a CID-only config change is a new audit fact and must rotate the active generation"
    );
}

#[test]
fn reconciled_snapshot_requires_and_retains_scope_for_every_runnable_behavior() {
    let behavior = behavior_with_skills(Vec::new());
    let behavior_id = behavior.behavior_id.clone();

    let missing = ResolvedRuntimeSnapshot::from_parts(
        behavior_id.clone(),
        vec![behavior.clone()],
        HashMap::new(),
        HashMap::new(),
    )
    .with_reconciled_document_runtime_config_provenance(HashMap::new())
    .expect_err("reconciled snapshots must pin every runnable behavior");
    assert!(missing
        .to_string()
        .contains("has no exact config provenance"));

    let mut reconciled = ResolvedRuntimeSnapshot::from_parts(
        behavior_id.clone(),
        vec![behavior.clone()],
        HashMap::new(),
        HashMap::new(),
    )
    .with_reconciled_document_runtime_config_provenance(HashMap::from([(
        behavior_id.clone(),
        Arc::new(config_provenance(&behavior, "cid-behavior")),
    )]))
    .expect("complete reconciled provenance");
    assert_eq!(
        reconciled.config_provenance_scope,
        crate::rendered_request::ConfigProvenanceScope::ReconciledDocumentRuntime
    );

    reconciled.behavior_config_provenance.remove(&behavior_id);
    let scoped = reconciled.scoped_config_provenance_for(&behavior_id);
    assert_eq!(
        scoped.scope,
        crate::rendered_request::ConfigProvenanceScope::ReconciledDocumentRuntime
    );
    assert!(scoped.exact.is_none());
    let dropped = reconciled
        .validate_config_provenance_scope()
        .expect_err("a dropped reconciled map entry must fail closed");
    assert!(dropped
        .to_string()
        .contains("has no exact config provenance"));
}

#[test]
fn static_snapshot_validates_any_supplied_exact_bundle() {
    let behavior = behavior_with_skills(Vec::new());
    let behavior_id = behavior.behavior_id.clone();
    let mut invalid = config_provenance(&behavior, "cid-behavior");
    invalid.behavior.logical_id = "other-behavior".to_string();
    let snapshot = ResolvedRuntimeSnapshot::from_parts(
        behavior_id.clone(),
        vec![behavior],
        HashMap::new(),
        HashMap::new(),
    )
    .with_behavior_config_provenance(HashMap::from([(behavior_id, Arc::new(invalid))]));

    assert!(snapshot.validate_config_provenance_scope().is_err());
}

#[test]
fn refresh_active_snapshot_updates_to_new_generation() {
    let initial = snapshot(1, "general");
    let updated = snapshot(2, "code");
    let (tx, mut rx) = watch::channel(initial.clone());
    let mut current = initial;

    tx.send(updated.clone()).unwrap();

    assert!(refresh_active_snapshot(&mut current, &mut rx));
    assert!(Arc::ptr_eq(&current, &updated));
    assert_eq!(current.generation, 2);
    assert_eq!(current.default_behavior_id, "code");
}

#[test]
fn refresh_active_snapshot_is_noop_when_unchanged() {
    let initial = snapshot(1, "general");
    let (_tx, mut rx) = watch::channel(initial.clone());
    let mut current = initial.clone();

    assert!(!refresh_active_snapshot(&mut current, &mut rx));
    assert!(Arc::ptr_eq(&current, &initial));
}
