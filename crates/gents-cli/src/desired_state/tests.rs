use std::fs;
use std::path::PathBuf;

use serde_json::json;

use super::convert::{
    export_bundle_from_manifest, manifest_from_export_bundle, tool_service_registry_from_live_value,
};
use super::diff::{diff_collection, diff_manifests};
use super::validate::validate_manifest;
use super::*;

fn empty_manifest(agent_did: &str) -> DesiredStateManifest {
    DesiredStateManifest {
        agent_principal: DesiredAgentPrincipal {
            agent_did: agent_did.to_string(),
            display_name: None,
            default_behavior_id: None,
            enabled: true,
        },
        agent_behaviors: Vec::new(),
        skills: Vec::new(),
        datastore_tool_surfaces: Vec::new(),
        tool_selections: Vec::new(),
        inference_backends: Vec::new(),
        inference_profiles: Vec::new(),
        tool_service_registries: Vec::new(),
        projection_acp_bindings: Vec::new(),
        peer_pairings: Vec::new(),
        tasks: Vec::new(),
        schedules: Vec::new(),
        event_triggers: Vec::new(),
        callback_bindings: Vec::new(),
        repository_placements: Vec::new(),
    }
}

fn manifest_with_default_behavior() -> DesiredStateManifest {
    let mut manifest = empty_manifest("did:test:test");
    manifest.agent_principal.default_behavior_id = Some("default".to_string());
    manifest.agent_behaviors.push(DesiredAgentBehavior {
        behavior_id: "default".to_string(),
        agent_did: "did:test:test".to_string(),
        display_name: None,
        description: None,
        summary: None,
        system_prompt: None,
        request_context_template: None,
        backend_id: None,
        model_name: None,
        tool_selection_id: None,
        inference_profile_id: None,
        compaction_strategy: None,
        compaction_threshold: None,
        enabled: true,
        skill_refs: Vec::new(),
        skill_excludes: Vec::new(),
    });
    manifest
}

fn behavior_with(id: &str, backend_id: Option<&str>) -> DesiredAgentBehavior {
    DesiredAgentBehavior {
        behavior_id: id.to_string(),
        agent_did: "did:test:test".to_string(),
        display_name: None,
        description: None,
        summary: None,
        system_prompt: None,
        request_context_template: None,
        backend_id: backend_id.map(|s| s.to_string()),
        model_name: None,
        tool_selection_id: None,
        inference_profile_id: None,
        compaction_strategy: None,
        compaction_threshold: None,
        enabled: true,
        skill_refs: Vec::new(),
        skill_excludes: Vec::new(),
    }
}

fn backend(id: &str) -> DesiredInferenceBackend {
    DesiredInferenceBackend {
        backend_id: id.to_string(),
        name: id.to_string(),
        provider_kind: Default::default(),
        openai_wire_api: None,
        endpoint: "http://localhost:1234".to_string(),
        api_key: None,
        api_key_env_var: None,
        max_concurrent: 1,
        max_queue_depth: 1,
        enabled: true,
        models: Vec::new(),
    }
}

fn profile(id: &str) -> DesiredInferenceProfile {
    DesiredInferenceProfile {
        profile_id: id.to_string(),
        display_name: Some(id.to_string()),
        context_window: None,
        max_output_tokens: None,
        max_turns: None,
        temperature: None,
        stream_batch_ms: None,
        stream_liveness_timeout_secs: None,
        deadline_duration_secs: None,
        retry_max_transport: None,
        retry_backoff_ms: None,
        retry_max_resample: None,
        retry_allow_repair: None,
        retry_interactive_max: None,
        ..Default::default()
    }
}

fn peer_pairing(
    peer_did: &str,
    peer_id: &str,
    template: &str,
    enabled: bool,
) -> DesiredPeerPairing {
    DesiredPeerPairing {
        peer_did: peer_did.to_string(),
        addresses: vec![format!("{peer_id}@127.0.0.1:4100")],
        template: template.to_string(),
        enabled,
        peer_id: peer_id.to_string(),
    }
}

fn pairing_diff(
    desired: &DesiredStateManifest,
    live: &DesiredStateManifest,
) -> DesiredStateCollectionDiff {
    diff_manifests(
        &PathBuf::from("/tmp/fake-root"),
        "local",
        desired,
        Some(&live.agent_principal),
        live,
        false,
    )
    .collections
    .peer_pairings
}

#[test]
fn peer_pairing_diff_covers_create_update_unchanged_and_remove() {
    let peer_a = "11".repeat(32);
    let peer_b = "22".repeat(32);
    let mut desired = empty_manifest("did:key:owner");
    desired.peer_pairings.push(peer_pairing(
        "did:key:peer-a",
        &peer_a,
        "conversation",
        true,
    ));
    desired.peer_pairings.push(peer_pairing(
        "did:key:peer-b",
        &peer_b,
        "agent-config",
        true,
    ));

    let mut live = empty_manifest("did:key:owner");
    live.peer_pairings.push(peer_pairing(
        "did:key:peer-b",
        &peer_b,
        "conversation",
        true,
    ));
    let stale_id = "33".repeat(32);
    live.peer_pairings.push(peer_pairing(
        "did:key:stale",
        &stale_id,
        "conversation",
        true,
    ));

    let diff = pairing_diff(&desired, &live);
    assert_eq!(diff.create, vec![peer_a]);
    assert_eq!(diff.update, vec![peer_b]);
    assert_eq!(diff.delete, vec![stale_id]);
    assert!(diff.unchanged.is_empty());

    live.peer_pairings = desired.peer_pairings.clone();
    let diff = pairing_diff(&desired, &live);
    assert_eq!(diff.unchanged, vec!["11".repeat(32), "22".repeat(32)]);
    assert!(diff.create.is_empty());
    assert!(diff.update.is_empty());
    assert!(diff.delete.is_empty());
}

#[test]
fn disabled_or_absent_manifest_pairing_removes_stale_enabled_owned_row() {
    let peer_id = "44".repeat(32);
    let mut live = empty_manifest("did:key:owner");
    live.peer_pairings
        .push(peer_pairing("did:key:peer", &peer_id, "conversation", true));

    let absent = empty_manifest("did:key:owner");
    assert_eq!(pairing_diff(&absent, &live).delete, vec![peer_id.clone()]);

    let mut disabled = empty_manifest("did:key:owner");
    disabled.peer_pairings.push(peer_pairing(
        "did:key:peer",
        &peer_id,
        "conversation",
        false,
    ));
    let diff = pairing_diff(&disabled, &live);
    assert_eq!(diff.delete, vec![peer_id]);
    assert!(diff.update.is_empty(), "disabled is absence, not an update");
}

#[test]
fn pairing_address_surface_forms_compare_semantically() {
    let peer_id = "55".repeat(32);
    let mut desired = empty_manifest("did:key:owner");
    desired
        .peer_pairings
        .push(peer_pairing("did:key:peer", &peer_id, "conversation", true));
    let mut live = desired.clone();
    live.peer_pairings[0].addresses = vec![format!("127.0.0.1:4100/p2p/{peer_id}")];

    assert_eq!(pairing_diff(&desired, &live).unchanged, vec![peer_id]);
}

#[test]
fn pairing_did_correction_updates_the_existing_peer_id() {
    let peer_id = "5a".repeat(32);
    let mut desired = empty_manifest("did:key:owner");
    desired.peer_pairings.push(peer_pairing(
        "did:key:corrected",
        &peer_id,
        "conversation",
        true,
    ));
    let mut live = empty_manifest("did:key:owner");
    live.peer_pairings.push(peer_pairing(
        "did:key:stale",
        &peer_id,
        "conversation",
        true,
    ));

    let diff = pairing_diff(&desired, &live);
    assert_eq!(diff.update, vec![peer_id]);
    assert!(diff.create.is_empty());
    assert!(diff.delete.is_empty());
}

#[test]
fn peer_pairing_validation_rejects_unsafe_shapes() {
    let peer_a = "66".repeat(32);
    let peer_b = "77".repeat(32);
    let mut manifest = empty_manifest("did:key:owner");
    manifest.peer_pairings.push(DesiredPeerPairing {
        peer_did: "did:key:owner".to_string(),
        addresses: vec![
            format!("{peer_a}@127.0.0.1:4100"),
            format!("{peer_b}@127.0.0.1:4200"),
        ],
        template: "app-collections".to_string(),
        enabled: true,
        peer_id: String::new(),
    });
    let mut errors = Vec::new();
    validate_manifest(&manifest, &mut errors);
    assert!(errors.iter().any(|error| error.contains("own agent_did")));
    assert!(errors.iter().any(|error| error.contains("data-plane-only")));
    assert!(errors.iter().any(|error| error.contains("mixes addresses")));

    manifest.peer_pairings[0].peer_did = "did:key:peer".to_string();
    manifest.peer_pairings[0].addresses.clear();
    manifest.peer_pairings[0].template = "conversation".to_string();
    errors.clear();
    validate_manifest(&manifest, &mut errors);
    assert!(errors
        .iter()
        .any(|error| error.contains("must contain at least one address")));

    manifest.peer_pairings[0].addresses = vec!["not-a-peer@127.0.0.1:4100".to_string()];
    errors.clear();
    validate_manifest(&manifest, &mut errors);
    assert!(errors
        .iter()
        .any(|error| error.contains("invalid iroh peer id")));
}

#[test]
fn pairing_apply_bundle_stamps_owner_provenance_and_omits_disabled_rows() {
    let peer_id = "88".repeat(32);
    let mut manifest = empty_manifest("did:key:owner");
    manifest.peer_pairings.push(peer_pairing(
        "did:key:enabled",
        &peer_id,
        "conversation",
        true,
    ));
    manifest.peer_pairings.push(DesiredPeerPairing {
        peer_did: "did:key:disabled".to_string(),
        addresses: Vec::new(),
        template: "conversation".to_string(),
        enabled: false,
        peer_id: String::new(),
    });

    let bundle = export_bundle_from_manifest(&manifest, "local").unwrap();
    assert_eq!(bundle.as_bundle().peer_pairings.len(), 1);
    let row = &bundle.as_bundle().peer_pairings[0];
    assert_eq!(row["peer_id"], peer_id);
    assert_eq!(row["agent_did"], "did:key:enabled");
    assert_eq!(row["source"], "manifest:did:key:owner");
    assert!(row["profiles"].is_null());
}

fn deletes_contain(
    deletes: &[gents::apply_model::DocRef],
    collection: gents::Collection,
    id: &str,
) -> bool {
    deletes
        .iter()
        .any(|d| d.collection == collection && d.id == id)
}

#[test]
fn prune_deletes_unreferenced_orphan_backend() {
    let desired = empty_manifest("did:test:test");
    let mut live = empty_manifest("did:test:test");
    live.inference_backends.push(backend("k-orphan"));

    let deletes = super::prune::prune_safe_deletes(&desired, &live);
    assert!(deletes_contain(
        &deletes,
        gents::Collection::InferenceBackend,
        "k-orphan"
    ));
}

#[test]
fn prune_blocks_backend_referenced_by_behavior() {
    let desired = empty_manifest("did:test:test");
    let mut live = empty_manifest("did:test:test");
    live.inference_backends.push(backend("k1"));
    live.agent_behaviors.push(behavior_with("b1", Some("k1")));

    let deletes = super::prune::prune_safe_deletes(&desired, &live);
    assert!(deletes_contain(
        &deletes,
        gents::Collection::AgentBehavior,
        "b1"
    ));
    assert!(
        !deletes_contain(&deletes, gents::Collection::InferenceBackend, "k1"),
        "backend referenced by a live behavior must not be pruned"
    );
}

#[test]
fn prune_blocks_behavior_referenced_by_task() {
    let desired = empty_manifest("did:test:test");
    let mut live = empty_manifest("did:test:test");
    live.agent_behaviors.push(behavior_with("b1", None));
    let mut task = sample_task("t1");
    task.behavior_id = "b1".to_string();
    live.tasks.push(task);

    let deletes = super::prune::prune_safe_deletes(&desired, &live);
    assert!(deletes_contain(&deletes, gents::Collection::Task, "t1"));
    assert!(
        !deletes_contain(&deletes, gents::Collection::AgentBehavior, "b1"),
        "behavior referenced by a live task must not be pruned"
    );
}

#[test]
fn prune_blocks_task_referenced_by_schedule_and_trigger() {
    let desired = empty_manifest("did:test:test");
    let mut live = empty_manifest("did:test:test");
    let mut task = sample_task("t1");
    task.behavior_id = "b1".to_string();
    live.agent_behaviors.push(behavior_with("b1", None));
    live.tasks.push(task);
    live.schedules.push(sample_schedule("s1", "t1"));
    live.event_triggers
        .push(sample_event_trigger_for("e1", "t1"));

    let deletes = super::prune::prune_safe_deletes(&desired, &live);
    assert!(deletes_contain(&deletes, gents::Collection::Schedule, "s1"));
    assert!(deletes_contain(
        &deletes,
        gents::Collection::EventTrigger,
        "e1"
    ));
    assert!(
        !deletes_contain(&deletes, gents::Collection::Task, "t1"),
        "task referenced by a live schedule/trigger must not be pruned"
    );
}

#[test]
fn diff_manifests_prune_records_deletes_in_collection_diff() {
    let desired = empty_manifest("did:test:test");
    let mut live = empty_manifest("did:test:test");
    live.inference_backends.push(backend("k-orphan"));

    let report = diff_manifests(
        &PathBuf::from("/tmp/fake-root"),
        "local",
        &desired,
        Some(&live.agent_principal),
        &live,
        true,
    );
    assert_eq!(
        report.collections.inference_backends.delete,
        vec!["k-orphan".to_string()]
    );
    assert!(report.collections.inference_backends.live_only.is_empty());
}

#[test]
fn diff_manifests_without_prune_records_no_deletes() {
    let desired = empty_manifest("did:test:test");
    let mut live = empty_manifest("did:test:test");
    live.inference_backends.push(backend("k-orphan"));

    let report = diff_manifests(
        &PathBuf::from("/tmp/fake-root"),
        "local",
        &desired,
        Some(&live.agent_principal),
        &live,
        false,
    );
    assert!(report.collections.inference_backends.delete.is_empty());
    assert_eq!(
        report.collections.inference_backends.live_only,
        vec!["k-orphan".to_string()]
    );
}

fn sample_task(task_id: &str) -> DesiredTask {
    DesiredTask {
        task_id: task_id.to_string(),
        name: "Sample task".to_string(),
        description: None,
        behavior_id: "default".to_string(),
        prompt_template: "Do the thing.".to_string(),
        enabled: true,
        output_schema_ref: None,
    }
}

fn sample_tool_selection(selection_id: &str) -> DesiredToolSelection {
    DesiredToolSelection {
        selection_id: selection_id.to_string(),
        agent_did: "did:test:test".to_string(),
        display_name: None,
        tool_policy_version: None,
        enable_file_tools: false,
        file_tools_mode: "ReadOnly".to_string(),
        file_tool_root: None,
        enable_bash: false,
        bash_mode: "ReadOnly".to_string(),
        command_execution_policy: None,
        command_allowed_argv_prefixes: Vec::new(),
        command_forbidden_argv_prefixes: Vec::new(),
        read_only_command_allowlist: Vec::new(),
        command_network_mode: None,
        cli_tool_names: Vec::new(),
        enable_meta_tools: true,
        allowed_mcp_service_ids: Vec::new(),
        delegate_to: Vec::new(),
        backgroundable_tool_names: Vec::new(),
        enable_memory: false,
        enable_session_history_tool: false,
        enable_context_budget: true,
        enable_defra_query: true,
        defra_query_collections: Vec::new(),
        subagent_targets: Vec::new(),
        subagent_spawn_enabled: false,
        subagent_steering_enabled: false,
        subagent_background_enabled: false,
        subagent_default_await_mode: None,
        subagent_allow_cross_deployment: false,
        cross_deployment_spawn_timeout_seconds: None,
        write_tools: Vec::new(),
        datastore_tool_surface_ids: Vec::new(),
        enable_self_config: false,
        self_config_categories: Vec::new(),
        self_config_no_lockout: false,
        self_config_dry_run: false,
        enable_lsp: false,
        lsp_config: None,
    }
}

fn sample_schedule(schedule_id: &str, task_id: &str) -> DesiredSchedule {
    DesiredSchedule {
        schedule_id: schedule_id.to_string(),
        task_id: task_id.to_string(),
        interval_secs: Some(3600),
        cron: None,
        timezone: None,
        missed_run_policy: None,
        enabled: true,
        concurrency: "serial".to_string(),
    }
}

fn sample_event_trigger() -> DesiredEventTrigger {
    DesiredEventTrigger {
        trigger_id: "new-customer-greet".into(),
        task_id: "summarize-inbox".into(),
        source_collection: "CustomerSignup".into(),
        event_kind: "created".into(),
        filter: None,
        correlation_field: None,
        fire_mode: None,
        expected_count: None,
        expected_count_field: None,
        group_timeout_secs: None,
        group_min_count: None,
        workspace_authority: None,
        enabled: true,
        concurrency: "serial".into(),
    }
}

fn empty_manifest_with_event_trigger(t: DesiredEventTrigger) -> DesiredStateManifest {
    let mut m = empty_manifest("did:test:test");
    m.event_triggers.push(t);
    m
}

#[test]
fn normalize_sorts_and_dedups_read_only_command_allowlist() {
    let mut manifest = empty_manifest("did:test:test");
    let mut selection = sample_tool_selection("tasks-tools");
    selection.read_only_command_allowlist =
        vec!["ss".to_string(), "cat".to_string(), "ss".to_string()];
    manifest.tool_selections.push(selection);

    super::normalize::normalize_manifest(&mut manifest);

    assert_eq!(
        manifest.tool_selections[0].read_only_command_allowlist,
        vec!["cat".to_string(), "ss".to_string()],
        "read_only_command_allowlist must be sorted and deduped by normalize"
    );
}

#[test]
fn normalize_makes_read_only_command_allowlist_order_insensitive() {
    let mut a = empty_manifest("did:test:test");
    let mut sel_a = sample_tool_selection("tasks-tools");
    sel_a.read_only_command_allowlist = vec!["cat".to_string(), "ss".to_string()];
    a.tool_selections.push(sel_a);

    let mut b = empty_manifest("did:test:test");
    let mut sel_b = sample_tool_selection("tasks-tools");
    sel_b.read_only_command_allowlist = vec!["ss".to_string(), "cat".to_string()];
    b.tool_selections.push(sel_b);

    super::normalize::normalize_manifest(&mut a);
    super::normalize::normalize_manifest(&mut b);

    assert_eq!(
        a.tool_selections[0].read_only_command_allowlist,
        b.tool_selections[0].read_only_command_allowlist,
        "differing allowlist order must normalize to the same value (no spurious diff)"
    );
}

#[test]
fn normalize_treats_empty_reasoning_effort_as_unset() {
    let mut manifest = empty_manifest("did:test:test");
    let mut inference_profile = profile("default-profile");
    inference_profile.reasoning_effort = Some("  ".to_string());
    manifest.inference_profiles.push(inference_profile);

    super::normalize::normalize_manifest(&mut manifest);

    assert_eq!(manifest.inference_profiles[0].reasoning_effort, None);
}

#[test]
fn normalize_trims_nonempty_reasoning_effort() {
    let mut manifest = empty_manifest("did:test:test");
    let mut inference_profile = profile("default-profile");
    inference_profile.reasoning_effort = Some(" high ".to_string());
    manifest.inference_profiles.push(inference_profile);

    super::normalize::normalize_manifest(&mut manifest);

    assert_eq!(
        manifest.inference_profiles[0].reasoning_effort.as_deref(),
        Some("high")
    );
}

#[test]
fn diff_treats_migrated_empty_reasoning_effort_as_unset() {
    let mut desired = empty_manifest("did:test:test");
    desired.inference_profiles.push(profile("default-profile"));
    let mut live = desired.clone();
    live.inference_profiles[0].reasoning_effort = Some(String::new());

    super::normalize::normalize_manifest(&mut desired);
    super::normalize::normalize_manifest(&mut live);
    let report = diff_manifests(
        &PathBuf::from("/tmp/fake-root"),
        "local",
        &desired,
        Some(&live.agent_principal),
        &live,
        false,
    );

    assert_eq!(
        report.collections.inference_profiles.unchanged,
        vec!["default-profile"]
    );
    assert!(report.collections.inference_profiles.update.is_empty());
}

#[test]
fn desired_tool_service_registry_normalizes_address_storage_fields() {
    let service: DesiredToolServiceRegistry = serde_json::from_value(json!({
        "service_id": "observability-mcp",
        "display_name": "Observability",
        "description": null,
        "hostname": null,
        "tailscale_ip": " 100.64.0.10 ",
        "lan_ip": null,
        "mcp_port": 9201,
        "mcp_path": "mcp"
    }))
    .expect("desired tool service should deserialize");

    assert_eq!(service.hostname.as_deref(), Some(""));
    assert_eq!(service.tailscale_ip.as_deref(), Some("100.64.0.10"));
    assert_eq!(service.lan_ip.as_deref(), Some(""));
    assert_eq!(service.mcp_path.as_deref(), Some("/mcp"));
    assert!(!service.send_agent_did);
}

#[test]
fn live_tool_service_registry_preserves_null_storage_for_diff() {
    let service = tool_service_registry_from_live_value(&json!({
        "service_id": "observability-mcp",
        "hostname": null,
        "tailscale_ip": null,
        "lan_ip": null,
        "mcp_port": 9201,
        "mcp_path": null
    }))
    .expect("live tool service should parse");

    assert_eq!(service.hostname, None);
    assert_eq!(service.tailscale_ip, None);
    assert_eq!(service.lan_ip, None);
    assert_eq!(service.mcp_path, None);
    assert!(!service.send_agent_did);
}

#[test]
fn tool_service_registry_round_trip_preserves_send_agent_did() {
    let mut manifest = empty_manifest("did:test:test");
    manifest
        .tool_service_registries
        .push(DesiredToolServiceRegistry {
            service_id: "identity-aware-mcp".to_string(),
            display_name: Some("Identity-aware MCP".to_string()),
            description: None,
            hostname: Some("studio-1".to_string()),
            tailscale_ip: Some(String::new()),
            lan_ip: Some(String::new()),
            mcp_port: Some(9201),
            mcp_path: Some("/mcp".to_string()),
            send_agent_did: true,
        });

    let bundle =
        export_bundle_from_manifest(&manifest, "local").expect("export bundle should be produced");
    assert_eq!(
        bundle.as_bundle().tool_service_registries[0]["send_agent_did"],
        json!(true)
    );

    let round_tripped = manifest_from_export_bundle(bundle.as_bundle())
        .expect("manifest should parse back from bundle");
    assert!(round_tripped.tool_service_registries[0].send_agent_did);
}

#[test]
fn tool_selection_round_trip_preserves_subagent_controls() {
    let mut manifest = empty_manifest("did:test:test");
    let mut selection = sample_tool_selection("default-tools");
    selection.subagent_targets = vec!["researcher".to_string()];
    selection.subagent_spawn_enabled = true;
    selection.subagent_steering_enabled = true;
    selection.subagent_background_enabled = true;
    selection.subagent_default_await_mode = Some("background".to_string());
    selection.subagent_allow_cross_deployment = true;
    selection.cross_deployment_spawn_timeout_seconds = Some(90);
    manifest.tool_selections.push(selection);

    let bundle =
        export_bundle_from_manifest(&manifest, "local").expect("export bundle should be produced");
    let exported_selection = &bundle.as_bundle().tool_selections[0];
    assert_eq!(
        exported_selection["subagent_targets"],
        json!(["researcher"])
    );
    assert_eq!(exported_selection["subagent_spawn_enabled"], json!(true));
    assert_eq!(exported_selection["subagent_steering_enabled"], json!(true));
    assert_eq!(
        exported_selection["subagent_background_enabled"],
        json!(true)
    );
    assert_eq!(
        exported_selection["subagent_default_await_mode"],
        json!("background")
    );
    assert_eq!(
        exported_selection["subagent_allow_cross_deployment"],
        json!(true)
    );
    assert_eq!(
        exported_selection["cross_deployment_spawn_timeout_seconds"],
        json!(90)
    );

    let round_tripped = manifest_from_export_bundle(bundle.as_bundle())
        .expect("manifest should parse back from bundle");
    let round_tripped_selection = &round_tripped.tool_selections[0];
    assert_eq!(
        round_tripped_selection.subagent_targets,
        vec!["researcher".to_string()]
    );
    assert!(round_tripped_selection.subagent_spawn_enabled);
    assert!(round_tripped_selection.subagent_steering_enabled);
    assert!(round_tripped_selection.subagent_background_enabled);
    assert_eq!(
        round_tripped_selection
            .subagent_default_await_mode
            .as_deref(),
        Some("background")
    );
    assert!(round_tripped_selection.subagent_allow_cross_deployment);
    assert_eq!(
        round_tripped_selection.cross_deployment_spawn_timeout_seconds,
        Some(90)
    );
}

#[test]
fn diff_marks_live_null_tool_service_storage_for_update() {
    let desired: DesiredToolServiceRegistry = serde_json::from_value(json!({
        "service_id": "observability-mcp",
        "hostname": "studio-1",
        "mcp_port": 9201
    }))
    .expect("desired tool service should deserialize");
    let live = tool_service_registry_from_live_value(&json!({
        "service_id": "observability-mcp",
        "hostname": "studio-1",
        "tailscale_ip": null,
        "lan_ip": null,
        "mcp_port": 9201,
        "mcp_path": null
    }))
    .expect("live tool service should parse");

    let diff = diff_collection(
        vec![(desired.service_id.clone(), &desired)],
        vec![(live.service_id.clone(), &live)],
    );

    assert_eq!(diff.update, vec!["observability-mcp"]);
    assert!(diff.unchanged.is_empty());
}

#[test]
fn deprecated_backend_capability_fields_are_ignored_for_diff_equality() {
    let with_deprecated: DesiredInferenceBackend = serde_json::from_value(json!({
        "backend_id": "local",
        "name": "Local",
        "provider_kind": "OpenAiCompatible",
        "endpoint": "http://127.0.0.1:11434/v1",
        "api_key": null,
        "api_key_env_var": null,
        "max_concurrent": 1,
        "max_queue_depth": 100,
        "enabled": true,
        "supports_tool_calls": false,
        "supports_streaming": false,
        "supports_structured_outputs": true,
        "supports_json_schema": true,
        "context_window": 32768,
        "max_output_tokens": 4096,
        "models": ["test-model"]
    }))
    .expect("deprecated fields should deserialize");

    let current: DesiredInferenceBackend = serde_json::from_value(json!({
        "backend_id": "local",
        "name": "Local",
        "provider_kind": "OpenAiCompatible",
        "endpoint": "http://127.0.0.1:11434/v1",
        "api_key": null,
        "api_key_env_var": null,
        "max_concurrent": 1,
        "max_queue_depth": 100,
        "enabled": true,
        "models": ["test-model"]
    }))
    .expect("current fields should deserialize");

    assert_eq!(with_deprecated, current);
    assert_eq!(
        serde_json::to_value(with_deprecated).unwrap(),
        serde_json::to_value(current).unwrap()
    );
}

#[test]
fn round_trip_load_write_load_is_identity() {
    use crate::desired_state::{load::load_manifest_root, write_manifest_root};
    use tempfile::tempdir;

    let tmp = tempdir().unwrap();
    let original = self::write_manifest_root::minimal_manifest();

    write_manifest_root(tmp.path(), &original, false).unwrap();
    let (loaded, report) = load_manifest_root(tmp.path());
    assert!(report.ok, "errors: {:?}", report.errors);
    let loaded = loaded.unwrap();

    assert_eq!(loaded.agent_principal, original.agent_principal);
    assert_eq!(loaded.agent_behaviors, original.agent_behaviors);
    assert_eq!(loaded.tool_selections, original.tool_selections);
    assert_eq!(loaded.inference_backends, original.inference_backends);
    assert_eq!(loaded.inference_profiles, original.inference_profiles);
    assert_eq!(
        loaded.tool_service_registries,
        original.tool_service_registries
    );
    assert_eq!(loaded.peer_pairings, original.peer_pairings);
    assert_eq!(loaded.tasks, original.tasks);
    assert_eq!(loaded.schedules, original.schedules);
}

mod load_manifest_root {
    use crate::desired_state::load::load_manifest_root;
    use crate::desired_state::write_manifest_root;
    use std::fs;
    use tempfile::tempdir;

    fn write_minimal_root(root: &std::path::Path) {
        fs::write(
            root.join("agent-principal.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "agent_did": "did:key:example",
                "default_behavior_id": "default",
                "enabled": true,
            }))
            .unwrap(),
        )
        .unwrap();

        let behavior_dir = root.join("agent-behaviors").join("default");
        fs::create_dir_all(&behavior_dir).unwrap();
        fs::write(
            behavior_dir.join("object.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "behavior_id": "default",
                "agent_did": "did:key:example",
                "enabled": true,
            }))
            .unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn loads_minimal_valid_root() {
        let tmp = tempdir().unwrap();
        write_minimal_root(tmp.path());

        let (manifest, report) = load_manifest_root(tmp.path());
        assert!(report.ok, "expected ok, got errors: {:?}", report.errors);
        let manifest = manifest.unwrap();
        assert_eq!(manifest.agent_principal.agent_did, "did:key:example");
        assert_eq!(manifest.agent_behaviors.len(), 1);
        assert!(manifest.tasks.is_empty());
    }

    #[test]
    fn loads_tool_selection_with_retired_orchestration_field() {
        let tmp = tempdir().unwrap();
        write_minimal_root(tmp.path());

        let selection_dir = tmp.path().join("tool-selections").join("legacy-tools");
        fs::create_dir_all(&selection_dir).unwrap();
        let mut selection = super::sample_tool_selection("legacy-tools");
        selection.agent_did = "did:key:example".to_string();
        let mut value = serde_json::to_value(selection).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("orchestration_enabled".to_string(), serde_json::json!(true));
        fs::write(
            selection_dir.join("object.json"),
            serde_json::to_vec_pretty(&value).unwrap(),
        )
        .unwrap();

        let (manifest, report) = load_manifest_root(tmp.path());
        assert!(report.ok, "errors: {:?}", report.errors);
        assert_eq!(manifest.unwrap().tool_selections.len(), 1);
    }

    #[test]
    fn loads_peer_pairing_from_human_readable_handle() {
        let tmp = tempdir().unwrap();
        write_minimal_root(tmp.path());
        let peer_id = "bb".repeat(32);
        let pairing_dir = tmp.path().join("peer-pairings").join("coding-steward");
        fs::create_dir_all(&pairing_dir).unwrap();
        fs::write(
            pairing_dir.join("object.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "peer_did": "did:key:remote",
                "addresses": [format!("{peer_id}@127.0.0.1:4100")],
                "template": "subagent-coordinator",
                "enabled": true
            }))
            .unwrap(),
        )
        .unwrap();

        let (manifest, report) = load_manifest_root(tmp.path());
        assert!(report.ok, "errors: {:?}", report.errors);
        let pairing = &manifest.unwrap().peer_pairings[0];
        assert_eq!(pairing.peer_did, "did:key:remote");
        assert_eq!(pairing.peer_id, peer_id);
    }

    #[test]
    fn missing_principal_file_is_error() {
        let tmp = tempdir().unwrap();
        let (_, report) = load_manifest_root(tmp.path());
        assert!(!report.ok);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.contains("agent-principal.json")),
            "got: {:?}",
            report.errors
        );
    }

    #[test]
    fn loads_behavior_with_sidecar_hydration() {
        let tmp = tempdir().unwrap();
        write_minimal_root(tmp.path());

        let behavior_dir = tmp.path().join("agent-behaviors").join("default");
        fs::write(
            behavior_dir.join("object.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "behavior_id": "default",
                "agent_did": "did:key:example",
                "system_prompt": "./system_prompt.md",
                "request_context_template": "./request_context_template.md",
                "enabled": true,
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(behavior_dir.join("system_prompt.md"), "You are helpful.").unwrap();
        fs::write(
            behavior_dir.join("request_context_template.md"),
            "Context {{ ctx.now }}",
        )
        .unwrap();

        let (manifest, report) = load_manifest_root(tmp.path());
        assert!(report.ok, "errors: {:?}", report.errors);
        let behavior = &manifest.unwrap().agent_behaviors[0];
        assert_eq!(behavior.system_prompt.as_deref(), Some("You are helpful."));
        assert_eq!(
            behavior.request_context_template.as_deref(),
            Some("Context {{ ctx.now }}")
        );
    }

    #[test]
    fn missing_sidecar_surfaces_error() {
        let tmp = tempdir().unwrap();
        write_minimal_root(tmp.path());

        let behavior_dir = tmp.path().join("agent-behaviors").join("default");
        fs::write(
            behavior_dir.join("object.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "behavior_id": "default",
                "agent_did": "did:key:example",
                "system_prompt": "./system_prompt.md",
                "enabled": true,
            }))
            .unwrap(),
        )
        .unwrap();

        let (_, report) = load_manifest_root(tmp.path());
        assert!(!report.ok);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.contains("sidecar path does not resolve")),
            "got: {:?}",
            report.errors
        );
    }

    #[test]
    fn loads_event_trigger_from_per_doc_dir() {
        let tmp = tempdir().unwrap();
        write_minimal_root(tmp.path());

        let task_dir = tmp.path().join("tasks").join("summarize-inbox");
        fs::create_dir_all(&task_dir).unwrap();
        fs::write(
            task_dir.join("object.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "task_id": "summarize-inbox",
                "name": "Summarize inbox",
                "behavior_id": "default",
                "prompt_template": "Summarize the unread emails.",
                "enabled": true,
            }))
            .unwrap(),
        )
        .unwrap();

        let trigger_dir = tmp.path().join("event_triggers").join("new-customer-greet");
        fs::create_dir_all(&trigger_dir).unwrap();
        fs::write(
            trigger_dir.join("object.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "trigger_id": "new-customer-greet",
                "task_id": "summarize-inbox",
                "source_collection": "CustomerSignup",
                "event_kind": "created",
                "enabled": true,
                "concurrency": "serial",
            }))
            .unwrap(),
        )
        .unwrap();

        let (manifest, report) = load_manifest_root(tmp.path());
        assert!(
            report.ok,
            "expected valid manifest, got {:?}",
            report.errors
        );
        let manifest = manifest.expect("manifest should load");

        assert_eq!(report.counts.event_triggers, 1);
        assert_eq!(manifest.event_triggers.len(), 1);
        assert_eq!(manifest.event_triggers[0].trigger_id, "new-customer-greet");
        assert_eq!(
            manifest.event_triggers[0].source_collection,
            "CustomerSignup"
        );
        assert_eq!(manifest.event_triggers[0].event_kind, "created");
        assert!(manifest.event_triggers[0].enabled);
    }

    #[test]
    fn loads_callback_binding_and_repository_placement() {
        let tmp = tempdir().unwrap();
        write_minimal_root(tmp.path());

        let binding_dir = tmp
            .path()
            .join("callback-bindings")
            .join("defense-patch-workspace");
        fs::create_dir_all(&binding_dir).unwrap();
        fs::write(
            binding_dir.join("object.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "binding_id": "defense-patch-workspace",
                "source_collection": "DefensePatchAssignment",
                "event_kind": "created",
                "filter": "{ status: { _eq: \"ready\" } }",
                "source_fields": "[\"assignment_id\",\"repository_id\",\"base_revision\",\"branch\"]",
                "builtin_emitter": "create_workspace",
                "principal_did": "did:key:zPlaceholder",
                "capability_set": "[\"create_workspace\",\"observe_dirty_base\",\"clone_artifacts\"]",
                "enabled": true
            }))
            .unwrap(),
        )
        .unwrap();

        let placement_dir = tmp
            .path()
            .join("repository-placements")
            .join("defending-code");
        fs::create_dir_all(&placement_dir).unwrap();
        fs::write(
            placement_dir.join("object.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "repository_id": "defending-code",
                "host_path": "/tmp/repo",
                "enabled": true
            }))
            .unwrap(),
        )
        .unwrap();

        let (manifest, report) = load_manifest_root(tmp.path());
        assert!(
            report.ok,
            "expected valid manifest, got {:?}",
            report.errors
        );
        let manifest = manifest.expect("manifest should load");
        assert_eq!(report.counts.callback_bindings, 1);
        assert_eq!(report.counts.repository_placements, 1);
        assert_eq!(
            manifest.callback_bindings[0].binding_id,
            "defense-patch-workspace"
        );
        assert_eq!(
            manifest.callback_bindings[0].builtin_emitter.as_deref(),
            Some("create_workspace")
        );
        assert_eq!(
            manifest.repository_placements[0].repository_id,
            "defending-code"
        );
    }

    #[test]
    fn projection_acp_binding_round_trips_per_doc_dir() {
        let tmp = tempdir().unwrap();
        write_minimal_root(tmp.path());

        let binding_dir = tmp
            .path()
            .join("projection-acp-bindings")
            .join("codex-read");
        fs::create_dir_all(&binding_dir).unwrap();
        fs::write(
            binding_dir.join("object.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "binding_id": "codex-read",
                "agent_did": "did:key:example",
                "behavior_id": "default",
                "projection_id": "openai_codex_run_trace",
                "policy_id": "policy-codex-read",
                "staged_policy_id": "policy-codex-read-next",
                "previous_policy_id": "policy-codex-read-prev",
                "resource_map_json": "{\"AgentRequest\":\"AgentRequest\"}",
                "publication_status": "rotating",
                "published_at": "2026-06-05T00:00:00Z",
                "enabled": true,
            }))
            .unwrap(),
        )
        .unwrap();

        let (manifest, report) = load_manifest_root(tmp.path());
        assert!(
            report.ok,
            "expected valid manifest, got {:?}",
            report.errors
        );
        assert_eq!(report.counts.projection_acp_bindings, 1);
        let manifest = manifest.expect("manifest should load");
        assert_eq!(manifest.projection_acp_bindings.len(), 1);
        assert_eq!(
            manifest.projection_acp_bindings[0].projection_id.as_deref(),
            Some("openai_codex_run_trace")
        );
        assert_eq!(
            manifest.projection_acp_bindings[0]
                .staged_policy_id
                .as_deref(),
            Some("policy-codex-read-next")
        );
        assert_eq!(
            manifest.projection_acp_bindings[0]
                .publication_status
                .as_deref(),
            Some("rotating")
        );

        let mut invalid = manifest.clone();
        invalid.projection_acp_bindings[0].projection_id = Some("codex_thread".to_string());
        let errors = super::validation_errors(&invalid);
        assert!(
            errors
                .iter()
                .any(|error| error.contains("invalid projection_id")),
            "expected projection-id validation error, got {errors:?}"
        );

        let mut invalid = manifest.clone();
        invalid.projection_acp_bindings[0].resource_map_json =
            Some(r#"{"":"AgentRequest"}"#.to_string());
        let errors = super::validation_errors(&invalid);
        assert!(
            errors
                .iter()
                .any(|error| error.contains("resource_map_json must map non-empty")),
            "expected resource-map validation error, got {errors:?}"
        );

        let mut invalid = manifest.clone();
        invalid.projection_acp_bindings[0].resource_map_json =
            Some(r#"{"AgentMesage":"AgentMessage"}"#.to_string());
        let errors = super::validation_errors(&invalid);
        assert!(
            errors
                .iter()
                .any(|error| error.contains("unknown runtime collection AgentMesage")),
            "expected resource-map collection validation error, got {errors:?}"
        );

        let mut invalid = manifest.clone();
        invalid.projection_acp_bindings[0].staged_policy_id = Some("policy-codex-read".to_string());
        let errors = super::validation_errors(&invalid);
        assert!(
            errors
                .iter()
                .any(|error| error.contains("staged_policy_id must differ")),
            "expected staged-policy validation error, got {errors:?}"
        );

        let mut invalid = manifest.clone();
        invalid.projection_acp_bindings[0].staged_policy_id = None;
        invalid.projection_acp_bindings[0].publication_status = Some("rotating".to_string());
        let errors = super::validation_errors(&invalid);
        assert!(
            errors.iter().any(|error| {
                error.contains("publication_status rotating requires staged_policy_id")
            }),
            "expected rotating-status validation error, got {errors:?}"
        );

        let mut invalid = manifest.clone();
        invalid.projection_acp_bindings[0].publication_status = Some("archived".to_string());
        let errors = super::validation_errors(&invalid);
        assert!(
            errors
                .iter()
                .any(|error| error.contains("invalid publication_status")),
            "expected publication-status validation error, got {errors:?}"
        );

        let mut invalid = manifest.clone();
        invalid.projection_acp_bindings[0].publication_status = Some("retired".to_string());
        invalid.projection_acp_bindings[0].staged_policy_id = None;
        let errors = super::validation_errors(&invalid);
        assert!(
            errors
                .iter()
                .any(|error| error.contains("publication_status retired must not be enabled")),
            "expected enabled-retired validation error, got {errors:?}"
        );

        let out = tempdir().unwrap();
        write_manifest_root(out.path(), &manifest, false).unwrap();
        let written_path = out
            .path()
            .join("projection-acp-bindings")
            .join("codex-read")
            .join("object.json");
        let written: serde_json::Value =
            serde_json::from_slice(&fs::read(written_path).unwrap()).unwrap();
        assert_eq!(
            written
                .get("binding_id")
                .and_then(serde_json::Value::as_str),
            Some("codex-read")
        );
        assert_eq!(
            written
                .get("staged_policy_id")
                .and_then(serde_json::Value::as_str),
            Some("policy-codex-read-next")
        );
        assert_eq!(
            written
                .get("publication_status")
                .and_then(serde_json::Value::as_str),
            Some("rotating")
        );
        assert!(written.get("created_at").is_none());
        assert!(written.get("updated_at").is_none());
    }

    #[test]
    fn deprecated_backend_capability_fields_are_ignored() {
        let tmp = tempdir().unwrap();
        write_minimal_root(tmp.path());

        let backend_dir = tmp.path().join("inference-backends").join("local");
        fs::create_dir_all(&backend_dir).unwrap();
        fs::write(
            backend_dir.join("object.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "backend_id": "local",
                "name": "Local",
                "provider_kind": "OpenAiCompatible",
                "endpoint": "http://127.0.0.1:11434/v1",
                "api_key": null,
                "api_key_env_var": null,
                "max_concurrent": 1,
                "max_queue_depth": 100,
                "enabled": true,
                "supports_tool_calls": true,
                "supports_streaming": true,
                "supports_structured_outputs": false,
                "supports_json_schema": false,
                "models": ["test-model"],
            }))
            .unwrap(),
        )
        .unwrap();

        let (_, report) = load_manifest_root(tmp.path());
        assert!(
            report.ok,
            "expected valid manifest, got {:?}",
            report.errors
        );
    }
}

#[test]
fn diff_manifests_creates_task_when_live_is_empty() {
    let mut desired = empty_manifest("did:test:test");
    desired.tasks.push(sample_task("summarize-inbox"));
    let live = empty_manifest("did:test:test");

    let report = diff_manifests(
        &PathBuf::from("/tmp/fake-root"),
        "local",
        &desired,
        Some(&live.agent_principal),
        &live,
        false,
    );

    assert_eq!(report.collections.tasks.create, vec!["summarize-inbox"]);
    assert!(report.collections.tasks.update.is_empty());
    assert!(report.collections.tasks.unchanged.is_empty());
    assert!(report.collections.tasks.live_only.is_empty());
    assert_eq!(report.counts.tasks.create, 1);
    assert_eq!(report.counts.tasks.update, 0);
    assert_eq!(report.counts.tasks.unchanged, 0);
    assert_eq!(report.counts.tasks.live_only, 0);
}

#[test]
fn diff_manifests_creates_schedule_when_live_is_empty() {
    let mut desired = empty_manifest("did:test:test");
    desired
        .schedules
        .push(sample_schedule("summarize-inbox-hourly", "summarize-inbox"));
    let live = empty_manifest("did:test:test");

    let report = diff_manifests(
        &PathBuf::from("/tmp/fake-root"),
        "local",
        &desired,
        Some(&live.agent_principal),
        &live,
        false,
    );

    assert_eq!(
        report.collections.schedules.create,
        vec!["summarize-inbox-hourly"]
    );
    assert!(report.collections.schedules.update.is_empty());
    assert!(report.collections.schedules.unchanged.is_empty());
    assert!(report.collections.schedules.live_only.is_empty());
    assert_eq!(report.counts.schedules.create, 1);
    assert_eq!(report.counts.schedules.update, 0);
    assert_eq!(report.counts.schedules.unchanged, 0);
    assert_eq!(report.counts.schedules.live_only, 0);
}

#[test]
fn diff_manifests_reports_live_only_without_delete_by_default() {
    let desired = manifest_with_default_behavior();
    let mut live = desired.clone();
    live.tasks.push(sample_task("stale-task"));

    let report = diff_manifests(
        &PathBuf::from("/tmp/fake-root"),
        "local",
        &desired,
        Some(&live.agent_principal),
        &live,
        false,
    );

    assert_eq!(report.collections.tasks.live_only, vec!["stale-task"]);
    assert!(report.collections.tasks.delete.is_empty());
    assert_eq!(report.counts.tasks.live_only, 1);
    assert_eq!(report.counts.tasks.delete, 0);
}

#[test]
fn diff_manifests_with_prune_deletes_only_unreferenced_live_only_docs() {
    let desired = manifest_with_default_behavior();
    let mut live = desired.clone();
    live.tasks.push(sample_task("stale-task"));
    live.schedules
        .push(sample_schedule("stale-schedule", "stale-task"));

    let report = diff_manifests(
        &PathBuf::from("/tmp/fake-root"),
        "local",
        &desired,
        Some(&live.agent_principal),
        &live,
        true,
    );

    assert_eq!(report.collections.tasks.live_only, vec!["stale-task"]);
    assert!(report.collections.schedules.live_only.is_empty());
    assert!(
        report.collections.tasks.delete.is_empty(),
        "task remains protected while the live schedule references it"
    );
    assert_eq!(report.collections.schedules.delete, vec!["stale-schedule"]);
}

#[test]
fn diff_manifests_marks_task_update_when_prompt_changes() {
    let mut desired = empty_manifest("did:test:test");
    let mut desired_task = sample_task("summarize-inbox");
    desired_task.prompt_template = "New prompt body.".to_string();
    desired.tasks.push(desired_task);

    let mut live = empty_manifest("did:test:test");
    live.tasks.push(sample_task("summarize-inbox"));

    let report = diff_manifests(
        &PathBuf::from("/tmp/fake-root"),
        "local",
        &desired,
        Some(&live.agent_principal),
        &live,
        false,
    );

    assert_eq!(report.collections.tasks.update, vec!["summarize-inbox"]);
    assert!(report.collections.tasks.create.is_empty());
    assert!(report.collections.tasks.unchanged.is_empty());
    assert_eq!(report.counts.tasks.update, 1);
}

#[test]
fn diff_manifests_marks_tool_selection_update_when_mcp_allowlist_changes() {
    let mut desired = empty_manifest("did:test:test");
    let mut desired_selection = sample_tool_selection("service-tools");
    desired_selection.allowed_mcp_service_ids = vec!["x-data".to_string()];
    desired.tool_selections.push(desired_selection);

    let mut live = empty_manifest("did:test:test");
    live.tool_selections
        .push(sample_tool_selection("service-tools"));

    let report = diff_manifests(
        &PathBuf::from("/tmp/fake-root"),
        "local",
        &desired,
        Some(&live.agent_principal),
        &live,
        false,
    );

    assert_eq!(
        report.collections.tool_selections.update,
        vec!["service-tools"]
    );
    assert!(report.collections.tool_selections.create.is_empty());
    assert!(report.collections.tool_selections.unchanged.is_empty());
    assert_eq!(report.counts.tool_selections.update, 1);
}

#[test]
fn diff_manifests_marks_schedule_update_when_interval_changes() {
    let mut desired = empty_manifest("did:test:test");
    let mut desired_schedule = sample_schedule("summarize-inbox-hourly", "summarize-inbox");
    desired_schedule.interval_secs = Some(7200);
    desired.schedules.push(desired_schedule);

    let mut live = empty_manifest("did:test:test");
    live.schedules
        .push(sample_schedule("summarize-inbox-hourly", "summarize-inbox"));

    let report = diff_manifests(
        &PathBuf::from("/tmp/fake-root"),
        "local",
        &desired,
        Some(&live.agent_principal),
        &live,
        false,
    );

    assert_eq!(
        report.collections.schedules.update,
        vec!["summarize-inbox-hourly"]
    );
    assert!(report.collections.schedules.create.is_empty());
    assert!(report.collections.schedules.unchanged.is_empty());
    assert_eq!(report.counts.schedules.update, 1);
}

#[test]
fn diff_manifests_creates_event_trigger_when_live_is_empty() {
    let manifest = empty_manifest_with_event_trigger(sample_event_trigger());
    let live = empty_manifest("did:test:test");

    let report = diff_manifests(
        &PathBuf::from("/tmp/fake-root"),
        "local",
        &manifest,
        Some(&live.agent_principal),
        &live,
        false,
    );

    assert_eq!(
        report.collections.event_triggers.create,
        vec!["new-customer-greet"]
    );
    assert!(report.collections.event_triggers.update.is_empty());
    assert!(report.collections.event_triggers.unchanged.is_empty());
    assert!(report.collections.event_triggers.live_only.is_empty());
    assert_eq!(report.counts.event_triggers.create, 1);
}

#[test]
fn diff_manifests_marks_event_trigger_update_when_filter_changes() {
    let mut desired = sample_event_trigger();
    desired.filter = Some(r#"{ plan: { _eq: "paid" } }"#.to_string());
    let live = sample_event_trigger();
    let manifest = empty_manifest_with_event_trigger(desired);
    let live_manifest = empty_manifest_with_event_trigger(live);

    let report = diff_manifests(
        &PathBuf::from("/tmp/fake-root"),
        "local",
        &manifest,
        Some(&live_manifest.agent_principal),
        &live_manifest,
        false,
    );

    assert_eq!(
        report.collections.event_triggers.update,
        vec!["new-customer-greet"]
    );
    assert!(report.collections.event_triggers.create.is_empty());
    assert!(report.collections.event_triggers.unchanged.is_empty());
}

fn validation_errors(manifest: &DesiredStateManifest) -> Vec<String> {
    let mut errors = Vec::new();
    validate_manifest(manifest, &mut errors);
    errors
}

#[test]
fn validate_rejects_non_positive_stream_liveness_timeout() {
    let mut manifest = empty_manifest("did:test:test");
    let mut profile = profile("fast");
    profile.stream_liveness_timeout_secs = Some(0);
    manifest.inference_profiles.push(profile);

    let errors = validation_errors(&manifest);

    assert!(
        errors
            .iter()
            .any(|message| message.contains("stream_liveness_timeout_secs must be positive")),
        "expected stream_liveness_timeout_secs validation error, got {errors:?}"
    );
}

#[test]
fn validate_rejects_negative_sampling_seed() {
    let mut manifest = empty_manifest("did:test:test");
    let mut profile = profile("seeded");
    profile.seed = Some(-1);
    manifest.inference_profiles.push(profile);

    let errors = validation_errors(&manifest);

    assert!(
        errors
            .iter()
            .any(|message| message.contains("seed must be non-negative")),
        "expected seed validation error, got {errors:?}"
    );
}

#[test]
fn validate_rejects_empty_string_in_subagent_targets() {
    let mut manifest = manifest_with_default_behavior();
    let mut sel = sample_tool_selection("agent-tools");
    sel.agent_did = "did:test:test".to_string();
    sel.subagent_targets = vec!["".to_string()];
    manifest.tool_selections.push(sel);

    let errors = validation_errors(&manifest);
    assert!(
        errors
            .iter()
            .any(|msg| msg.contains("subagent_targets") && msg.contains("agent-tools")),
        "expected empty subagent_targets entry rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_subagent_spawn_enabled_without_targets() {
    let mut manifest = manifest_with_default_behavior();
    let mut sel = sample_tool_selection("agent-tools");
    sel.agent_did = "did:test:test".to_string();
    sel.subagent_spawn_enabled = true;
    sel.subagent_targets = Vec::new();
    manifest.tool_selections.push(sel);

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|msg| {
            msg.contains("agent-tools")
                && msg.contains("subagent_spawn_enabled")
                && msg.contains("subagent_targets")
        }),
        "expected subagent_spawn_enabled-without-targets rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_subagent_spawn_enabled_with_empty_targets_vec() {
    let mut manifest = manifest_with_default_behavior();
    let mut sel = sample_tool_selection("agent-tools");
    sel.agent_did = "did:test:test".to_string();
    sel.subagent_spawn_enabled = true;
    sel.subagent_targets = Vec::new();
    manifest.tool_selections.push(sel);

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|msg| {
            msg.contains("agent-tools")
                && msg.contains("subagent_spawn_enabled")
                && msg.contains("subagent_targets")
        }),
        "expected subagent_spawn_enabled-with-empty-targets-vec rejection, got {errors:?}"
    );
}

#[test]
fn validate_accepts_subagent_spawn_enabled_with_targets() {
    let mut manifest = manifest_with_default_behavior();
    let mut sel = sample_tool_selection("agent-tools");
    sel.agent_did = "did:test:test".to_string();
    sel.subagent_spawn_enabled = true;
    sel.subagent_targets = vec![gents::subagent_target_entry(
        "researcher",
        "did:test:test",
        "amy-research",
        None,
    )];
    manifest.tool_selections.push(sel);

    let errors = validation_errors(&manifest);
    assert!(
        !errors
            .iter()
            .any(|msg| msg.contains("subagent_targets") || msg.contains("subagent_spawn_enabled")),
        "expected no subagent rejections for valid config, got {errors:?}"
    );
}

#[test]
fn validate_rejects_duplicate_subagent_target_name() {
    let mut manifest = manifest_with_default_behavior();
    let mut sel = sample_tool_selection("agent-tools");
    sel.agent_did = "did:test:test".to_string();
    sel.subagent_spawn_enabled = true;
    sel.subagent_targets = vec![
        gents::subagent_target_entry("dup", "did:test:test", "amy-research", None),
        gents::subagent_target_entry("dup", "did:test:test", "amy-code", None),
    ];
    manifest.tool_selections.push(sel);

    let errors = validation_errors(&manifest);
    assert!(
        errors
            .iter()
            .any(|msg| msg.contains("duplicate subagent target name") && msg.contains("dup")),
        "expected duplicate-name rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_remote_did_target_when_cross_deployment_off() {
    let mut manifest = manifest_with_default_behavior();
    let mut sel = sample_tool_selection("agent-tools");
    sel.agent_did = "did:test:test".to_string();
    sel.subagent_spawn_enabled = true;
    sel.subagent_allow_cross_deployment = false;
    sel.subagent_targets = vec![gents::subagent_target_entry(
        "remote-researcher",
        "did:test:OTHER-deployment",
        "amy-research",
        None,
    )];
    manifest.tool_selections.push(sel);

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|msg| {
            msg.contains("cross-deployment subagent delegation is deferred")
                && msg.contains("remote-researcher")
                && msg.contains("subagent_allow_cross_deployment=true")
        }),
        "expected remote-DID rejection when flag is off, got {errors:?}"
    );
}

#[test]
fn validate_accepts_remote_did_target_when_cross_deployment_on() {
    let mut manifest = manifest_with_default_behavior();
    let mut sel = sample_tool_selection("agent-tools");
    sel.agent_did = "did:test:test".to_string();
    sel.subagent_spawn_enabled = true;
    sel.subagent_allow_cross_deployment = true;
    sel.subagent_targets = vec![gents::subagent_target_entry(
        "remote-researcher",
        "did:test:OTHER-deployment",
        "amy-research",
        None,
    )];
    manifest.tool_selections.push(sel);

    let errors = validation_errors(&manifest);
    assert!(
        !errors
            .iter()
            .any(|msg| msg.contains("cross-deployment subagent delegation is deferred")),
        "expected no cross-deployment rejection when flag is on, got {errors:?}"
    );
}

#[test]
fn validate_accepts_local_did_target_when_cross_deployment_off() {
    let mut manifest = manifest_with_default_behavior();
    let mut sel = sample_tool_selection("agent-tools");
    sel.agent_did = "did:test:test".to_string();
    sel.subagent_spawn_enabled = true;
    sel.subagent_allow_cross_deployment = false;
    sel.subagent_targets = vec![gents::subagent_target_entry(
        "local-researcher",
        "did:test:test",
        "amy-research",
        None,
    )];
    manifest.tool_selections.push(sel);

    let errors = validation_errors(&manifest);
    assert!(
        !errors
            .iter()
            .any(|msg| msg.contains("cross-deployment subagent delegation is deferred")),
        "expected no cross-deployment rejection for local target, got {errors:?}"
    );
}

#[test]
fn write_tools_deserializer_converges_object_and_string_shapes() {
    fn deser_write_tools(write_tools: serde_json::Value) -> Vec<String> {
        let selection = json!({
            "selection_id": "conv-sel",
            "agent_did": "did:test:test",
            "enable_file_tools": false,
            "file_tools_mode": "ReadOnly",
            "enable_bash": false,
            "bash_mode": "ReadOnly",
            "enable_meta_tools": false,
            "write_tools": write_tools,
        });
        let parsed: DesiredToolSelection =
            serde_json::from_value(selection).expect("DesiredToolSelection deserializes");
        parsed.write_tools
    }

    let object_list = deser_write_tools(json!([
        {
            "tool_name": "request_action",
            "collection": "ActionRequest",
            "fields": [{ "name": "drift_sig", "required": true }]
        }
    ]));

    let string_list = deser_write_tools(json!([
        "{\"collection\":\"ActionRequest\",\"fields\":[{\"required\":true,\"name\":\"drift_sig\"}],\"tool_name\":\"request_action\"}"
    ]));

    assert_eq!(
        object_list, string_list,
        "object-list and string-list shapes of the same decl must canonicalize to the SAME storage Vec<String> (else false apply/diff drift)"
    );
}

fn write_tool_storage_entry(decl: &gents::WriteToolDecl) -> String {
    serde_json::to_string(decl).expect("WriteToolDecl serializes to JSON")
}

#[test]
fn validate_rejects_write_tool_with_invalid_collection_identifier() {
    use gents::WriteToolDecl;
    for collection in ["", "ActionRequest) { _docID } mutation {"] {
        let mut manifest = manifest_with_default_behavior();
        let mut sel = sample_tool_selection("agent-tools");
        sel.agent_did = "did:test:test".to_string();
        sel.write_tools = vec![write_tool_storage_entry(&WriteToolDecl {
            tool_name: "request_action".to_string(),
            collection: collection.to_string(),
            description: String::new(),
            fields: Vec::new(),
            output_obligation: None,
        })];
        manifest.tool_selections.push(sel);

        let errors = validation_errors(&manifest);
        assert!(
            errors.iter().any(|msg| msg.contains("write_tools")
                && msg.contains("agent-tools")
                && msg.contains("collection")),
            "expected invalid-collection write_tools rejection, got {errors:?}"
        );
    }
}

#[test]
fn validate_rejects_write_tool_with_empty_field_name() {
    use gents::{WriteToolDecl, WriteToolField};
    let mut manifest = manifest_with_default_behavior();
    let mut sel = sample_tool_selection("agent-tools");
    sel.agent_did = "did:test:test".to_string();
    sel.write_tools = vec![write_tool_storage_entry(&WriteToolDecl {
        tool_name: "request_action".to_string(),
        collection: "ActionRequest".to_string(),
        description: String::new(),
        output_obligation: None,
        fields: vec![WriteToolField {
            name: "   ".to_string(),
            required: true,
            fill: None,
        }],
    })];
    manifest.tool_selections.push(sel);

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|msg| msg.contains("write_tools")
            && msg.contains("agent-tools")
            && msg.contains("empty name")),
        "expected empty-field-name write_tools rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_duplicate_write_tool_name() {
    use gents::WriteToolDecl;
    let mut manifest = manifest_with_default_behavior();
    let mut sel = sample_tool_selection("agent-tools");
    sel.agent_did = "did:test:test".to_string();
    let decl = WriteToolDecl {
        tool_name: "request_action".to_string(),
        collection: "ActionRequest".to_string(),
        description: String::new(),
        output_obligation: None,
        fields: Vec::new(),
    };
    sel.write_tools = vec![
        write_tool_storage_entry(&decl),
        write_tool_storage_entry(&WriteToolDecl {
            collection: "OtherCollection".to_string(),
            ..decl
        }),
    ];
    manifest.tool_selections.push(sel);

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|msg| {
            msg.contains("duplicate write_tools tool_name") && msg.contains("request_action")
        }),
        "expected duplicate write_tools tool_name rejection, got {errors:?}"
    );
}

#[test]
fn validate_accepts_well_formed_write_tools() {
    use gents::{WriteToolDecl, WriteToolField};
    let mut manifest = manifest_with_default_behavior();
    let mut sel = sample_tool_selection("agent-tools");
    sel.agent_did = "did:test:test".to_string();
    sel.write_tools = vec![write_tool_storage_entry(&WriteToolDecl {
        tool_name: "request_action".to_string(),
        collection: "ActionRequest".to_string(),
        description: "Request a bounded action".to_string(),
        output_obligation: None,
        fields: vec![
            WriteToolField {
                name: "title".to_string(),
                required: true,
                fill: None,
            },
            WriteToolField {
                name: "detail".to_string(),
                required: false,
                fill: None,
            },
        ],
    })];
    manifest.tool_selections.push(sel);

    let errors = validation_errors(&manifest);
    assert!(
        !errors.iter().any(|msg| msg.contains("write_tools")),
        "expected no write_tools rejections for a well-formed decl, got {errors:?}"
    );
}

#[test]
fn validate_rejects_zero_write_tool_output_obligation() {
    use gents::document_config::{WriteToolOutputObligation, WriteToolOutputObligationScope};
    use gents::WriteToolDecl;
    let mut manifest = manifest_with_default_behavior();
    let mut sel = sample_tool_selection("agent-tools");
    sel.agent_did = "did:test:test".to_string();
    sel.write_tools = vec![write_tool_storage_entry(&WriteToolDecl {
        tool_name: "request_action".to_string(),
        collection: "ActionRequest".to_string(),
        description: String::new(),
        fields: Vec::new(),
        output_obligation: Some(WriteToolOutputObligation {
            scope: WriteToolOutputObligationScope::Trigger,
            minimum_writes: 0,
            expected_count_field: None,
        }),
    })];
    manifest.tool_selections.push(sel);

    let errors = validation_errors(&manifest);
    assert!(
        errors
            .iter()
            .any(|error| error.contains("output_obligation.minimum_writes")),
        "{errors:?}"
    );
}

#[test]
fn validate_rejects_write_tool_name_colliding_with_builtin() {
    use gents::{WriteToolDecl, WriteToolField};
    let mut manifest = manifest_with_default_behavior();
    let mut sel = sample_tool_selection("agent-tools");
    sel.agent_did = "did:test:test".to_string();
    sel.write_tools = vec![write_tool_storage_entry(&WriteToolDecl {
        tool_name: "read_file".to_string(),
        collection: "AuditLog".to_string(),
        description: String::new(),
        output_obligation: None,
        fields: vec![WriteToolField {
            name: "path".to_string(),
            required: true,
            fill: None,
        }],
    })];
    manifest.tool_selections.push(sel);

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|msg| msg.contains("write_tools")
            && msg.contains("read_file")
            && msg.contains("built-in")),
        "expected built-in collision rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_write_tool_name_colliding_with_cli_tool() {
    use gents::WriteToolDecl;
    let mut manifest = manifest_with_default_behavior();
    let mut sel = sample_tool_selection("agent-tools");
    sel.agent_did = "did:test:test".to_string();
    sel.cli_tool_names = vec!["rg".to_string()];
    sel.write_tools = vec![write_tool_storage_entry(&WriteToolDecl {
        tool_name: "rg".to_string(),
        collection: "AuditLog".to_string(),
        description: String::new(),
        output_obligation: None,
        fields: Vec::new(),
    })];
    manifest.tool_selections.push(sel);

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|msg| msg.contains("write_tools")
            && msg.contains("rg")
            && msg.contains("cli_tool_names")),
        "expected cli_tool_names collision rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_duplicate_write_tool_field_name() {
    use gents::{WriteToolDecl, WriteToolField};
    let mut manifest = manifest_with_default_behavior();
    let mut sel = sample_tool_selection("agent-tools");
    sel.agent_did = "did:test:test".to_string();
    sel.write_tools = vec![write_tool_storage_entry(&WriteToolDecl {
        tool_name: "request_action".to_string(),
        collection: "ActionRequest".to_string(),
        description: String::new(),
        output_obligation: None,
        fields: vec![
            WriteToolField {
                name: "summary".to_string(),
                required: true,
                fill: None,
            },
            WriteToolField {
                name: "summary".to_string(),
                required: false,
                fill: None,
            },
        ],
    })];
    manifest.tool_selections.push(sel);

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|msg| msg.contains("write_tools")
            && msg.contains("request_action")
            && msg.contains("duplicate field name")),
        "expected duplicate field-name rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_empty_task_id() {
    let mut manifest = manifest_with_default_behavior();
    let mut task = sample_task("");
    task.task_id = String::new();
    manifest.tasks.push(task);

    let errors = validation_errors(&manifest);
    assert!(
        errors
            .iter()
            .any(|message| message.contains("empty task_id")),
        "expected empty task_id rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_empty_task_behavior_id() {
    let mut manifest = manifest_with_default_behavior();
    let mut task = sample_task("summarize-inbox");
    task.behavior_id = String::new();
    manifest.tasks.push(task);

    let errors = validation_errors(&manifest);
    assert!(
        errors
            .iter()
            .any(|message| message.contains("summarize-inbox") && message.contains("behavior_id")),
        "expected empty behavior_id rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_duplicate_task_id() {
    let mut manifest = manifest_with_default_behavior();
    manifest.tasks.push(sample_task("summarize-inbox"));
    manifest.tasks.push(sample_task("summarize-inbox"));

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|message| {
            message.contains("duplicate task_id") && message.contains("summarize-inbox")
        }),
        "expected duplicate task_id rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_empty_schedule_id() {
    let mut manifest = manifest_with_default_behavior();
    manifest.tasks.push(sample_task("summarize-inbox"));
    let mut schedule = sample_schedule("", "summarize-inbox");
    schedule.schedule_id = String::new();
    manifest.schedules.push(schedule);

    let errors = validation_errors(&manifest);
    assert!(
        errors
            .iter()
            .any(|message| message.contains("empty schedule_id")),
        "expected empty schedule_id rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_duplicate_schedule_id() {
    let mut manifest = manifest_with_default_behavior();
    manifest.tasks.push(sample_task("summarize-inbox"));
    manifest
        .schedules
        .push(sample_schedule("hourly", "summarize-inbox"));
    manifest
        .schedules
        .push(sample_schedule("hourly", "summarize-inbox"));

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|message| {
            message.contains("duplicate schedule_id") && message.contains("hourly")
        }),
        "expected duplicate schedule_id rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_schedule_interval_zero_or_negative() {
    let mut manifest = manifest_with_default_behavior();
    manifest.tasks.push(sample_task("summarize-inbox"));
    let mut schedule = sample_schedule("hourly", "summarize-inbox");
    schedule.interval_secs = Some(0);
    manifest.schedules.push(schedule);

    let errors = validation_errors(&manifest);
    assert!(
        errors
            .iter()
            .any(|message| message.contains("hourly") && message.contains("interval_secs")),
        "expected interval_secs >= 1 rejection, got {errors:?}"
    );
}

#[test]
fn validate_accepts_cron_schedule_with_timezone() {
    let mut manifest = manifest_with_default_behavior();
    manifest.tasks.push(sample_task("summarize-inbox"));
    let mut schedule = sample_schedule("weekday-digest", "summarize-inbox");
    schedule.interval_secs = None;
    schedule.cron = Some("30 3 * * MON".to_string());
    schedule.timezone = Some("America/Los_Angeles".to_string());
    schedule.missed_run_policy = Some("latest_only".to_string());
    manifest.schedules.push(schedule);

    let errors = validation_errors(&manifest);
    assert!(
        errors.is_empty(),
        "expected valid cron schedule, got {errors:?}"
    );
}

#[test]
fn validate_rejects_malformed_cron_schedule() {
    let mut manifest = manifest_with_default_behavior();
    manifest.tasks.push(sample_task("summarize-inbox"));
    let mut schedule = sample_schedule("bad-cron", "summarize-inbox");
    schedule.interval_secs = None;
    schedule.cron = Some("30 3 * *".to_string());
    schedule.timezone = Some("UTC".to_string());
    manifest.schedules.push(schedule);

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|message| {
            message.contains("bad-cron")
                && message.contains("invalid cron schedule")
                && message.contains("exactly 5 fields")
        }),
        "expected malformed cron rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_invalid_cron_timezone() {
    let mut manifest = manifest_with_default_behavior();
    manifest.tasks.push(sample_task("summarize-inbox"));
    let mut schedule = sample_schedule("bad-zone", "summarize-inbox");
    schedule.interval_secs = None;
    schedule.cron = Some("30 3 * * MON".to_string());
    schedule.timezone = Some("Mars/Olympus".to_string());
    manifest.schedules.push(schedule);

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|message| {
            message.contains("bad-zone")
                && message.contains("invalid cron schedule")
                && message.contains("invalid IANA timezone")
        }),
        "expected invalid timezone rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_schedule_with_interval_and_cron() {
    let mut manifest = manifest_with_default_behavior();
    manifest.tasks.push(sample_task("summarize-inbox"));
    let mut schedule = sample_schedule("double-cadence", "summarize-inbox");
    schedule.cron = Some("30 3 * * MON".to_string());
    schedule.timezone = Some("UTC".to_string());
    manifest.schedules.push(schedule);

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|message| {
            message.contains("double-cadence")
                && message.contains("exactly one of interval_secs or cron")
        }),
        "expected double cadence rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_schedule_unknown_concurrency() {
    let mut manifest = manifest_with_default_behavior();
    manifest.tasks.push(sample_task("summarize-inbox"));
    let mut schedule = sample_schedule("hourly", "summarize-inbox");
    schedule.concurrency = "everything-everywhere".to_string();
    manifest.schedules.push(schedule);

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|message| {
            message.contains("hourly")
                && message.contains("concurrency")
                && message.contains("everything-everywhere")
        }),
        "expected unknown concurrency rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_task_unknown_behavior() {
    let mut manifest = manifest_with_default_behavior();
    let mut task = sample_task("summarize-inbox");
    task.behavior_id = "did:test:test:missing".to_string();
    manifest.tasks.push(task);

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|message| {
            message.contains("summarize-inbox")
                && message.contains("missing")
                && message.contains("behavior_id")
        }),
        "expected missing behavior_id reference rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_schedule_task_template_referencing_doc_scope() {
    let mut manifest = manifest_with_default_behavior();
    let mut task = sample_task("summarize-inbox");
    task.prompt_template = "Schedule fired at {{ event.fired_at }} for {{ doc.foo }}.".to_string();
    manifest.tasks.push(task);
    manifest
        .schedules
        .push(sample_schedule("hourly", "summarize-inbox"));

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|message| {
            message.contains("hourly")
                && message.contains("forbidden scope")
                && message.contains("doc")
                && message.contains("event.*")
        }),
        "expected schedule-scope rejection for doc.*, got {errors:?}"
    );
}

#[test]
fn validate_rejects_schedule_task_template_referencing_args_scope() {
    let mut manifest = manifest_with_default_behavior();
    let mut task = sample_task("summarize-inbox");
    task.prompt_template = "{{ args.target }}".to_string();
    manifest.tasks.push(task);
    manifest
        .schedules
        .push(sample_schedule("hourly", "summarize-inbox"));

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|message| {
            message.contains("hourly")
                && message.contains("forbidden scope")
                && message.contains("args")
        }),
        "expected schedule-scope rejection for args.*, got {errors:?}"
    );
}

#[test]
fn validate_accepts_schedule_task_template_using_only_event_scope() {
    let mut manifest = manifest_with_default_behavior();
    let mut task = sample_task("summarize-inbox");
    task.prompt_template = "Run at {{ event.fired_at }} for {{ event.trigger_kind }}.".to_string();
    manifest.tasks.push(task);
    manifest
        .schedules
        .push(sample_schedule("hourly", "summarize-inbox"));

    let errors = validation_errors(&manifest);
    assert!(
        !errors
            .iter()
            .any(|message| message.contains("forbidden scope")),
        "expected no schedule-scope rejections, got {errors:?}"
    );
}

#[test]
fn validate_rejects_schedule_unknown_task() {
    let mut manifest = manifest_with_default_behavior();
    manifest
        .schedules
        .push(sample_schedule("hourly", "missing-task"));

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|message| {
            message.contains("hourly")
                && message.contains("missing-task")
                && message.contains("task_id")
        }),
        "expected missing task_id reference rejection, got {errors:?}"
    );
}

fn sample_event_trigger_for(trigger_id: &str, task_id: &str) -> DesiredEventTrigger {
    DesiredEventTrigger {
        trigger_id: trigger_id.to_string(),
        task_id: task_id.to_string(),
        source_collection: "CustomerSignup".to_string(),
        event_kind: "created".to_string(),
        filter: None,
        correlation_field: None,
        fire_mode: None,
        expected_count: None,
        expected_count_field: None,
        group_timeout_secs: None,
        group_min_count: None,
        workspace_authority: None,
        enabled: true,
        concurrency: "serial".to_string(),
    }
}

#[test]
fn validate_rejects_event_trigger_referencing_unknown_task() {
    let mut manifest = manifest_with_default_behavior();
    manifest.event_triggers.push(sample_event_trigger_for(
        "new-customer-greet",
        "missing-task",
    ));

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|message| {
            message.contains("new-customer-greet")
                && message.contains("unknown task_id")
                && message.contains("missing-task")
        }),
        "expected unknown task_id rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_event_trigger_unknown_event_kind() {
    let mut manifest = manifest_with_default_behavior();
    manifest.tasks.push(sample_task("summarize-inbox"));
    let mut trig = sample_event_trigger_for("new-customer-greet", "summarize-inbox");
    trig.event_kind = "updated".to_string();
    manifest.event_triggers.push(trig);

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|message| {
            message.contains("new-customer-greet") && message.contains("unsupported event_kind")
        }),
        "expected unsupported event_kind rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_event_trigger_template_referencing_args_scope() {
    let mut manifest = manifest_with_default_behavior();
    let mut task = sample_task("summarize-inbox");
    task.prompt_template = "{{ args.foo }}".to_string();
    manifest.tasks.push(task);
    manifest.event_triggers.push(sample_event_trigger_for(
        "new-customer-greet",
        "summarize-inbox",
    ));

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|message| {
            message.contains("new-customer-greet") && message.contains("forbidden scope: args")
        }),
        "expected event-trigger forbidden-args rejection, got {errors:?}"
    );
}

#[test]
fn validate_accepts_event_trigger_template_using_event_and_doc_scopes() {
    let mut manifest = manifest_with_default_behavior();
    let mut task = sample_task("summarize-inbox");
    task.prompt_template = "{{ event.fired_at }} {{ doc.name }}".to_string();
    manifest.tasks.push(task);
    manifest.event_triggers.push(sample_event_trigger_for(
        "new-customer-greet",
        "summarize-inbox",
    ));

    let errors = validation_errors(&manifest);
    assert!(
        !errors
            .iter()
            .any(|message| message.contains("forbidden scope")),
        "expected no forbidden-scope rejections for event+doc scopes, got {errors:?}"
    );
}

#[test]
fn validate_rejects_duplicate_event_trigger_id() {
    let mut manifest = manifest_with_default_behavior();
    manifest.tasks.push(sample_task("summarize-inbox"));
    manifest.event_triggers.push(sample_event_trigger_for(
        "new-customer-greet",
        "summarize-inbox",
    ));
    manifest.event_triggers.push(sample_event_trigger_for(
        "new-customer-greet",
        "summarize-inbox",
    ));

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|message| {
            message.contains("duplicate") && message.contains("new-customer-greet")
        }),
        "expected duplicate trigger_id rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_event_trigger_unknown_concurrency() {
    let mut manifest = manifest_with_default_behavior();
    manifest.tasks.push(sample_task("summarize-inbox"));
    let mut trig = sample_event_trigger_for("new-customer-greet", "summarize-inbox");
    trig.concurrency = "weird".to_string();
    manifest.event_triggers.push(trig);

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|message| {
            message.contains("new-customer-greet")
                && message.contains("unknown concurrency")
                && message.contains("weird")
        }),
        "expected unknown concurrency rejection, got {errors:?}"
    );
}

#[test]
fn export_bundle_round_trip_preserves_tasks_and_schedules() {
    let mut manifest = manifest_with_default_behavior();
    manifest.tasks.push(sample_task("beta-task"));
    manifest.tasks.push(sample_task("alpha-task"));
    manifest
        .schedules
        .push(sample_schedule("beta-hourly", "beta-task"));
    let mut cron_schedule = sample_schedule("alpha-weekday", "alpha-task");
    cron_schedule.interval_secs = None;
    cron_schedule.cron = Some("30 3 * * MON".to_string());
    cron_schedule.timezone = Some("America/Los_Angeles".to_string());
    cron_schedule.missed_run_policy = Some("latest_only".to_string());
    manifest.schedules.push(cron_schedule);

    let bundle =
        export_bundle_from_manifest(&manifest, "local").expect("export bundle should be produced");
    assert_eq!(bundle.as_bundle().tasks.len(), 2);
    assert_eq!(bundle.as_bundle().schedules.len(), 2);

    let round_tripped = manifest_from_export_bundle(bundle.as_bundle())
        .expect("manifest should parse back from bundle");

    let task_ids: Vec<_> = round_tripped
        .tasks
        .iter()
        .map(|task| task.task_id.as_str())
        .collect();
    assert_eq!(task_ids, vec!["alpha-task", "beta-task"]);

    let schedule_ids: Vec<_> = round_tripped
        .schedules
        .iter()
        .map(|schedule| schedule.schedule_id.as_str())
        .collect();
    assert_eq!(schedule_ids, vec!["alpha-weekday", "beta-hourly"]);

    let mut expected_tasks = manifest.tasks.clone();
    expected_tasks.sort_by(|left, right| left.task_id.cmp(&right.task_id));
    assert_eq!(round_tripped.tasks, expected_tasks);

    let mut expected_schedules = manifest.schedules.clone();
    expected_schedules.sort_by(|left, right| left.schedule_id.cmp(&right.schedule_id));
    assert_eq!(round_tripped.schedules, expected_schedules);
}

#[test]
fn hydrate_sidecar_replaces_dot_slash_path_with_file_contents() {
    use super::load::hydrate_sidecar;
    use tempfile::tempdir;

    let dir = tempdir().unwrap();
    fs::write(dir.path().join("prompt.md"), "You are a helpful agent.").unwrap();

    let mut value = Some("./prompt.md".to_string());
    hydrate_sidecar(&mut value, dir.path()).unwrap();
    assert_eq!(value.as_deref(), Some("You are a helpful agent."));
}

#[test]
fn hydrate_sidecar_leaves_literal_string_untouched() {
    use super::load::hydrate_sidecar;
    use tempfile::tempdir;

    let dir = tempdir().unwrap();
    let mut value = Some("You are a helpful agent.".to_string());
    hydrate_sidecar(&mut value, dir.path()).unwrap();
    assert_eq!(value.as_deref(), Some("You are a helpful agent."));
}

#[test]
fn hydrate_sidecar_ignores_absolute_path() {
    use super::load::hydrate_sidecar;
    use tempfile::tempdir;

    let dir = tempdir().unwrap();
    let mut value = Some("/etc/hosts".to_string());
    hydrate_sidecar(&mut value, dir.path()).unwrap();
    assert_eq!(value.as_deref(), Some("/etc/hosts"));
}

#[test]
fn hydrate_sidecar_ignores_parent_relative_path() {
    use super::load::hydrate_sidecar;
    use tempfile::tempdir;

    let dir = tempdir().unwrap();
    let mut value = Some("../elsewhere.md".to_string());
    hydrate_sidecar(&mut value, dir.path()).unwrap();
    assert_eq!(value.as_deref(), Some("../elsewhere.md"));
}

#[test]
fn hydrate_sidecar_rejects_parent_component_in_rel_path() {
    use super::load::hydrate_sidecar;
    use tempfile::tempdir;

    let tmp = tempdir().unwrap();
    let json_dir = tmp.path().join("doc-dir");
    fs::create_dir_all(&json_dir).unwrap();
    fs::write(tmp.path().join("sibling.md"), "secret contents").unwrap();

    let mut value = Some("./../sibling.md".to_string());
    let err = hydrate_sidecar(&mut value, &json_dir).unwrap_err();
    assert!(err.contains("escapes document directory"), "got: {err}");
}

#[test]
fn hydrate_sidecar_rejects_nested_parent_component() {
    use super::load::hydrate_sidecar;
    use tempfile::tempdir;

    let tmp = tempdir().unwrap();
    let json_dir = tmp.path().join("a").join("b");
    fs::create_dir_all(&json_dir).unwrap();
    fs::write(tmp.path().join("outside.md"), "not yours").unwrap();

    let mut value = Some("./inner/../../outside.md".to_string());
    let err = hydrate_sidecar(&mut value, &json_dir).unwrap_err();
    assert!(err.contains("escapes document directory"), "got: {err}");
}

#[test]
fn hydrate_sidecar_errors_when_file_missing() {
    use super::load::hydrate_sidecar;
    use tempfile::tempdir;

    let dir = tempdir().unwrap();
    let mut value = Some("./missing.md".to_string());
    let err = hydrate_sidecar(&mut value, dir.path()).unwrap_err();
    assert!(err.contains("sidecar path does not resolve"), "got: {err}");
    assert!(err.contains("missing.md"), "got: {err}");
}

#[test]
fn hydrate_sidecar_errors_on_non_utf8() {
    use super::load::hydrate_sidecar;
    use tempfile::tempdir;

    let dir = tempdir().unwrap();
    fs::write(dir.path().join("bad.md"), [0xff, 0xfe, 0xfd]).unwrap();
    let mut value = Some("./bad.md".to_string());
    let err = hydrate_sidecar(&mut value, dir.path()).unwrap_err();
    assert!(err.contains("not valid UTF-8"), "got: {err}");
}

#[test]
fn hydrate_sidecar_is_noop_on_none() {
    use super::load::hydrate_sidecar;
    use tempfile::tempdir;

    let dir = tempdir().unwrap();
    let mut value: Option<String> = None;
    hydrate_sidecar(&mut value, dir.path()).unwrap();
    assert!(value.is_none());
}

mod load_per_doc_collection {
    use crate::desired_state::load::load_per_doc_collection;
    use crate::desired_state::{DesiredAgentBehavior, HasUniqueId};
    use gents::Collection;
    use std::fs;
    use tempfile::tempdir;

    fn write_behavior_dir(root: &std::path::Path, handle: &str, behavior_id: &str) {
        let dir = root.join("agent-behaviors").join(handle);
        fs::create_dir_all(&dir).unwrap();
        let body = serde_json::json!({
            "behavior_id": behavior_id,
            "agent_did": "did:key:example",
            "enabled": true,
        });
        fs::write(
            dir.join("object.json"),
            serde_json::to_vec_pretty(&body).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn loads_one_document_per_subdir() {
        let tmp = tempdir().unwrap();
        write_behavior_dir(tmp.path(), "default", "default");
        write_behavior_dir(tmp.path(), "other", "other");

        let mut errors = Vec::new();
        let result: Vec<DesiredAgentBehavior> =
            load_per_doc_collection(tmp.path(), Collection::AgentBehavior, &mut errors);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(result.len(), 2);
        let ids: Vec<&str> = result.iter().map(DesiredAgentBehavior::unique_id).collect();
        assert!(ids.contains(&"default") && ids.contains(&"other"));
    }

    #[test]
    fn missing_dir_is_empty_not_error() {
        let tmp = tempdir().unwrap();
        let mut errors = Vec::new();
        let result: Vec<DesiredAgentBehavior> =
            load_per_doc_collection(tmp.path(), Collection::AgentBehavior, &mut errors);
        assert!(errors.is_empty());
        assert!(result.is_empty());
    }

    #[test]
    fn missing_object_json_is_error() {
        let tmp = tempdir().unwrap();
        fs::create_dir_all(tmp.path().join("agent-behaviors").join("default")).unwrap();
        let mut errors = Vec::new();
        let _: Vec<DesiredAgentBehavior> =
            load_per_doc_collection(tmp.path(), Collection::AgentBehavior, &mut errors);
        assert_eq!(errors.len(), 1);
        assert!(
            errors[0].contains("is missing object.json"),
            "got: {:?}",
            errors
        );
    }

    #[test]
    fn handle_mismatch_is_error() {
        let tmp = tempdir().unwrap();
        write_behavior_dir(tmp.path(), "on-disk-name", "id-inside-json");
        let mut errors = Vec::new();
        let _: Vec<DesiredAgentBehavior> =
            load_per_doc_collection(tmp.path(), Collection::AgentBehavior, &mut errors);
        assert_eq!(errors.len(), 1);
        assert!(
            errors[0].contains("does not match behavior_id"),
            "got: {:?}",
            errors
        );
        assert!(errors[0].contains("on-disk-name"));
        assert!(errors[0].contains("id-inside-json"));
    }

    #[test]
    fn duplicate_unique_id_is_error() {
        let tmp = tempdir().unwrap();
        write_behavior_dir(tmp.path(), "alpha", "shared");
        write_behavior_dir(tmp.path(), "beta", "shared");
        let mut errors = Vec::new();
        let _: Vec<DesiredAgentBehavior> =
            load_per_doc_collection(tmp.path(), Collection::AgentBehavior, &mut errors);
        assert!(
            errors.iter().any(|e| {
                e.contains("duplicate behavior_id 'shared'")
                    && e.contains("alpha")
                    && e.contains("beta")
            }),
            "got: {:?}",
            errors
        );
    }

    #[test]
    fn unknown_sibling_files_are_ignored() {
        let tmp = tempdir().unwrap();
        write_behavior_dir(tmp.path(), "default", "default");
        fs::write(
            tmp.path()
                .join("agent-behaviors")
                .join("default")
                .join("README.md"),
            "notes",
        )
        .unwrap();
        fs::write(
            tmp.path()
                .join("agent-behaviors")
                .join("default")
                .join(".DS_Store"),
            "",
        )
        .unwrap();
        let mut errors = Vec::new();
        let result: Vec<DesiredAgentBehavior> =
            load_per_doc_collection(tmp.path(), Collection::AgentBehavior, &mut errors);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn non_directory_collection_path_is_error() {
        let tmp = tempdir().unwrap();
        fs::write(tmp.path().join("agent-behaviors"), "not a dir").unwrap();
        let mut errors = Vec::new();
        let _: Vec<DesiredAgentBehavior> =
            load_per_doc_collection(tmp.path(), Collection::AgentBehavior, &mut errors);
        assert!(
            errors.iter().any(|e| e.contains("is not a directory")),
            "got: {:?}",
            errors
        );
    }
}

pub(super) mod write_manifest_root {
    use std::fs;
    use tempfile::tempdir;

    use crate::desired_state::{
        write_manifest_root, DesiredAgentBehavior, DesiredAgentPrincipal, DesiredStateManifest,
        DesiredTask,
    };

    pub(in crate::desired_state::tests) fn minimal_manifest() -> DesiredStateManifest {
        DesiredStateManifest {
            agent_principal: DesiredAgentPrincipal {
                agent_did: "did:key:example".to_string(),
                display_name: None,
                default_behavior_id: Some("default".to_string()),
                enabled: true,
            },
            agent_behaviors: vec![DesiredAgentBehavior {
                behavior_id: "default".to_string(),
                agent_did: "did:key:example".to_string(),
                display_name: None,
                description: None,
                summary: None,
                system_prompt: Some("You are helpful.".to_string()),
                request_context_template: None,
                backend_id: None,
                model_name: None,
                tool_selection_id: None,
                inference_profile_id: None,
                compaction_strategy: None,
                compaction_threshold: None,
                enabled: true,
                skill_refs: Vec::new(),
                skill_excludes: Vec::new(),
            }],
            skills: Vec::new(),
            datastore_tool_surfaces: Vec::new(),
            tool_selections: Vec::new(),
            inference_backends: Vec::new(),
            inference_profiles: Vec::new(),
            tool_service_registries: Vec::new(),
            projection_acp_bindings: Vec::new(),
            peer_pairings: Vec::new(),
            tasks: vec![DesiredTask {
                task_id: "seed-health".to_string(),
                name: "Seed fleet health".to_string(),
                description: None,
                behavior_id: "default".to_string(),
                prompt_template: "Check the fleet.".to_string(),
                enabled: true,
                output_schema_ref: None,
            }],
            schedules: Vec::new(),
            event_triggers: Vec::new(),
            callback_bindings: Vec::new(),
            repository_placements: Vec::new(),
        }
    }

    #[test]
    fn writes_principal_and_per_doc_dirs_with_sidecars() {
        let tmp = tempdir().unwrap();
        let mut manifest = minimal_manifest();
        manifest.agent_behaviors[0].request_context_template =
            Some("Context {{ ctx.now }}".to_string());
        write_manifest_root(tmp.path(), &manifest, false).unwrap();

        assert!(tmp.path().join("agent-principal.json").is_file());

        let behavior_object = tmp.path().join("agent-behaviors/default/object.json");
        assert!(behavior_object.is_file());
        let behavior_sidecar = tmp.path().join("agent-behaviors/default/system_prompt.md");
        assert!(behavior_sidecar.is_file());
        assert_eq!(
            fs::read_to_string(&behavior_sidecar).unwrap(),
            "You are helpful."
        );
        let behavior_body: serde_json::Value =
            serde_json::from_slice(&fs::read(&behavior_object).unwrap()).unwrap();
        assert_eq!(
            behavior_body.get("system_prompt").and_then(|v| v.as_str()),
            Some("./system_prompt.md")
        );
        let context_sidecar = tmp
            .path()
            .join("agent-behaviors/default/request_context_template.md");
        assert!(context_sidecar.is_file());
        assert_eq!(
            fs::read_to_string(&context_sidecar).unwrap(),
            "Context {{ ctx.now }}"
        );
        assert_eq!(
            behavior_body
                .get("request_context_template")
                .and_then(|v| v.as_str()),
            Some("./request_context_template.md")
        );

        let task_object = tmp.path().join("tasks/seed-health/object.json");
        assert!(task_object.is_file());
        let task_sidecar = tmp.path().join("tasks/seed-health/prompt.md");
        assert!(task_sidecar.is_file());
        assert_eq!(
            fs::read_to_string(&task_sidecar).unwrap(),
            "Check the fleet."
        );
        let task_body: serde_json::Value =
            serde_json::from_slice(&fs::read(&task_object).unwrap()).unwrap();
        assert_eq!(
            task_body.get("prompt_template").and_then(|v| v.as_str()),
            Some("./prompt.md")
        );
    }

    #[test]
    fn none_system_prompt_omits_sidecar_and_field() {
        let tmp = tempdir().unwrap();
        let mut m = minimal_manifest();
        m.agent_behaviors[0].system_prompt = None;
        write_manifest_root(tmp.path(), &m, false).unwrap();

        let sidecar = tmp.path().join("agent-behaviors/default/system_prompt.md");
        assert!(!sidecar.exists());
        let body: serde_json::Value = serde_json::from_slice(
            &fs::read(tmp.path().join("agent-behaviors/default/object.json")).unwrap(),
        )
        .unwrap();
        assert!(body.get("system_prompt").is_none());
    }

    #[test]
    fn rejects_behavior_with_unsafe_id() {
        let tmp = tempdir().unwrap();
        let mut m = minimal_manifest();
        m.agent_behaviors[0].behavior_id = "bad/id".to_string();
        let err = write_manifest_root(tmp.path(), &m, false).unwrap_err();
        assert!(err.contains("filesystem-unsafe"), "got: {err}");
    }

    #[test]
    fn force_refuses_dir_without_agent_principal() {
        let tmp = tempdir().unwrap();
        fs::write(tmp.path().join("random.txt"), "this is not a manifest root").unwrap();
        let err = write_manifest_root(tmp.path(), &minimal_manifest(), true).unwrap_err();
        assert!(
            err.contains("does not contain agent-principal.json"),
            "got: {err}"
        );
        assert!(tmp.path().join("random.txt").exists());
    }

    #[test]
    fn refuses_to_overwrite_without_force() {
        let tmp = tempdir().unwrap();
        fs::write(tmp.path().join("leftover.txt"), "junk").unwrap();
        let err = write_manifest_root(tmp.path(), &minimal_manifest(), false).unwrap_err();
        assert!(err.contains("--force"), "got: {err}");
    }

    #[test]
    fn preflight_unsafe_id_does_not_delete_existing_root() {
        let tmp = tempdir().unwrap();

        fs::write(
            tmp.path().join("agent-principal.json"),
            b"{\"agent_did\":\"did:key:old\",\"enabled\":true}",
        )
        .unwrap();
        let old_behavior_dir = tmp.path().join("agent-behaviors").join("old-safe-id");
        fs::create_dir_all(&old_behavior_dir).unwrap();
        fs::write(old_behavior_dir.join("object.json"), b"{}").unwrap();

        let mut bad_manifest = minimal_manifest();
        bad_manifest.agent_behaviors.push(DesiredAgentBehavior {
            behavior_id: "bad/id".to_string(),
            agent_did: "did:key:example".to_string(),
            display_name: None,
            description: None,
            summary: None,
            system_prompt: None,
            request_context_template: None,
            backend_id: None,
            model_name: None,
            tool_selection_id: None,
            inference_profile_id: None,
            compaction_strategy: None,
            compaction_threshold: None,
            enabled: true,
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
        });

        let err = write_manifest_root(tmp.path(), &bad_manifest, true).unwrap_err();
        assert!(err.contains("filesystem-unsafe"), "got: {err}");

        assert!(
            tmp.path().join("agent-principal.json").exists(),
            "old agent-principal.json was deleted before pre-flight finished"
        );
        assert!(
            old_behavior_dir.join("object.json").exists(),
            "old behavior dir was deleted before pre-flight finished"
        );
    }

    #[test]
    fn force_removes_stray_files_from_previous_export() {
        let tmp = tempdir().unwrap();
        fs::write(
            tmp.path().join("agent-principal.json"),
            b"{\"agent_did\":\"did:key:stale\",\"enabled\":false}",
        )
        .unwrap();
        fs::create_dir_all(tmp.path().join("agent-behaviors").join("old-name")).unwrap();
        fs::write(
            tmp.path().join("agent-behaviors/old-name/object.json"),
            b"{}",
        )
        .unwrap();
        fs::write(tmp.path().join("leftover.txt"), "junk").unwrap();

        write_manifest_root(tmp.path(), &minimal_manifest(), true).unwrap();

        assert!(!tmp.path().join("leftover.txt").exists());
        assert!(!tmp.path().join("agent-behaviors/old-name").exists());
        assert!(tmp
            .path()
            .join("agent-behaviors/default/object.json")
            .is_file());
    }
}

mod write_manifest_root_safe_id {
    use crate::desired_state::write::check_filesystem_safe_id;

    #[test]
    fn accepts_ordinary_ids() {
        assert!(check_filesystem_safe_id("default").is_ok());
        assert!(check_filesystem_safe_id("workstation-1").is_ok());
        assert!(check_filesystem_safe_id("seed_fleet_health").is_ok());
    }

    #[test]
    fn rejects_forward_slash() {
        let err = check_filesystem_safe_id("foo/bar").unwrap_err();
        assert!(err.contains("filesystem-unsafe"), "got: {err}");
    }

    #[test]
    fn rejects_null_byte() {
        assert!(
            check_filesystem_safe_id("a\0b").is_err(),
            "should reject null byte"
        );
    }

    #[test]
    fn accepts_colon_in_human_keys() {
        assert!(
            check_filesystem_safe_id("profile:default").is_ok(),
            "colons are legal on POSIX"
        );
        assert!(check_filesystem_safe_id("tools:default").is_ok());
    }

    #[test]
    fn rejects_dot_and_dotdot() {
        assert!(check_filesystem_safe_id(".").is_err());
        assert!(check_filesystem_safe_id("..").is_err());
    }

    #[test]
    fn rejects_empty() {
        assert!(check_filesystem_safe_id("").is_err());
    }

    #[test]
    fn rejects_dot_prefix() {
        let err = check_filesystem_safe_id(".foo").unwrap_err();
        assert!(err.contains("dot-prefixed"), "got: {err}");
        let err = check_filesystem_safe_id(".hidden").unwrap_err();
        assert!(err.contains("dot-prefixed"), "got: {err}");
    }
}

fn sample_surface(surface_id: &str) -> DesiredDatastoreToolSurface {
    use gents::WriteToolDecl;
    DesiredDatastoreToolSurface {
        surface_id: surface_id.to_string(),
        agent_did: "did:test:test".to_string(),
        display_name: Some("experiment writes".to_string()),
        enabled: true,
        entries: vec![write_tool_storage_entry(&WriteToolDecl {
            tool_name: "write_experiment_finding".to_string(),
            collection: "ExperimentFinding".to_string(),
            description: "Record a finding".to_string(),
            output_obligation: None,
            fields: Vec::new(),
        })],
    }
}

#[test]
fn validate_accepts_surface_query_entry() {
    use gents::{QueryToolDecl, SurfaceToolDecl, WriteToolField, WriteToolFieldFill};
    let mut manifest = manifest_with_default_behavior();
    let mut surface = sample_surface("scan-reads");
    surface.entries.push(
        serde_json::to_string(&SurfaceToolDecl::Query(QueryToolDecl {
            tool_name: "query_candidate_finding".to_string(),
            collection: "CandidateFinding".to_string(),
            description: "Load candidates".to_string(),
            fields: vec!["finding_id".to_string(), "title".to_string()],
            filter_fields: vec![WriteToolField {
                name: "run_id".to_string(),
                required: false,
                fill: Some(WriteToolFieldFill::Correlation),
            }],
        }))
        .unwrap(),
    );
    manifest.datastore_tool_surfaces.push(surface);
    let mut sel = sample_tool_selection("stage-tools");
    sel.agent_did = "did:test:test".to_string();
    sel.datastore_tool_surface_ids = vec!["scan-reads".to_string()];
    manifest.tool_selections.push(sel);

    let errors = validation_errors(&manifest);
    assert!(
        errors.is_empty(),
        "query surface entries should validate, got {errors:?}"
    );
}

#[test]
fn validate_rejects_query_tool_name_colliding_with_cli_tool() {
    use gents::{QueryToolDecl, SurfaceToolDecl};
    let mut manifest = manifest_with_default_behavior();
    let mut surface = sample_surface("scan-reads");
    surface.entries.push(
        serde_json::to_string(&SurfaceToolDecl::Query(QueryToolDecl {
            tool_name: "gh".to_string(),
            collection: "CandidateFinding".to_string(),
            description: "Load candidates".to_string(),
            fields: vec!["finding_id".to_string()],
            filter_fields: Vec::new(),
        }))
        .unwrap(),
    );
    manifest.datastore_tool_surfaces.push(surface);
    let mut sel = sample_tool_selection("stage-tools");
    sel.agent_did = "did:test:test".to_string();
    sel.datastore_tool_surface_ids = vec!["scan-reads".to_string()];
    sel.cli_tool_names = vec!["gh".to_string()];
    manifest.tool_selections.push(sel);

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|error| error.contains("cli_tool_names")),
        "query/cli name collision should fail apply validation, got {errors:?}"
    );
}

#[test]
fn validate_accepts_surface_linked_tool_selection() {
    let mut manifest = manifest_with_default_behavior();
    manifest
        .datastore_tool_surfaces
        .push(sample_surface("experiment-writes"));
    let mut sel = sample_tool_selection("stage-tools");
    sel.agent_did = "did:test:test".to_string();
    sel.datastore_tool_surface_ids = vec!["experiment-writes".to_string()];
    manifest.tool_selections.push(sel);

    let errors = validation_errors(&manifest);
    assert!(
        errors.is_empty(),
        "surface-linked selection should validate cleanly, got {errors:?}"
    );
}

#[test]
fn validate_rejects_missing_surface_link() {
    let mut manifest = manifest_with_default_behavior();
    let mut sel = sample_tool_selection("stage-tools");
    sel.agent_did = "did:test:test".to_string();
    sel.datastore_tool_surface_ids = vec!["does-not-exist".to_string()];
    manifest.tool_selections.push(sel);

    let errors = validation_errors(&manifest);
    assert!(
        errors
            .iter()
            .any(|msg| msg.contains("missing DatastoreToolSurface")
                && msg.contains("does-not-exist")),
        "expected missing surface rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_disabled_surface_link() {
    let mut manifest = manifest_with_default_behavior();
    let mut surface = sample_surface("experiment-writes");
    surface.enabled = false;
    manifest.datastore_tool_surfaces.push(surface);
    let mut sel = sample_tool_selection("stage-tools");
    sel.agent_did = "did:test:test".to_string();
    sel.datastore_tool_surface_ids = vec!["experiment-writes".to_string()];
    manifest.tool_selections.push(sel);

    let errors = validation_errors(&manifest);
    assert!(
        errors.iter().any(|msg| msg.contains("disabled")),
        "expected disabled surface rejection, got {errors:?}"
    );
}

#[test]
fn validate_rejects_foreign_agent_surface_link() {
    let mut manifest = manifest_with_default_behavior();
    let mut surface = sample_surface("experiment-writes");
    surface.agent_did = "did:key:zOther".to_string();
    manifest.datastore_tool_surfaces.push(surface);
    let mut sel = sample_tool_selection("stage-tools");
    sel.agent_did = "did:test:test".to_string();
    sel.datastore_tool_surface_ids = vec!["experiment-writes".to_string()];
    manifest.tool_selections.push(sel);

    let errors = validation_errors(&manifest);
    assert!(
        errors
            .iter()
            .any(|msg| msg.contains("different agent") || msg.contains("does not match principal")),
        "expected foreign surface rejection, got {errors:?}"
    );
}

/// The checked-in pack must load with no environment set — `${VAR:-default}`
/// keeps it runnable as authored — and must honour an override, which is what
/// lets one pack be compared across models and endpoints.
#[test]
fn pipeline_pack_interpolates_endpoint_and_model() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../demo/pipeline");

    let (manifest, report) = load_manifest_root(&root);
    assert!(
        report.errors.is_empty(),
        "pack must load with no env set: {:?}",
        report.errors
    );
    let manifest = manifest.expect("manifest");
    let backend = manifest
        .inference_backends
        .iter()
        .find(|b| b.backend_id == "exp-deepseek")
        .expect("exp-deepseek backend");
    assert_eq!(
        backend.endpoint, "http://100.73.235.38:8000/v1",
        "unset GENTS_EXP_ENDPOINT must fall back to the checked-in default"
    );
    assert!(
        manifest
            .agent_behaviors
            .iter()
            .all(|b| b.model_name.as_deref() == Some("d4f")),
        "unset GENTS_EXP_MODEL must fall back to d4f: {:?}",
        manifest.agent_behaviors
    );
    assert!(
        !backend.endpoint.contains("${"),
        "no unexpanded reference may survive into the manifest"
    );
}

#[test]
fn load_pipeline_two_stage_fixture_with_surface() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../demo/pipeline");
    assert!(
        root.join("datastore-tool-surfaces/experiment-writes/object.json")
            .is_file(),
        "pipeline fixture must include experiment-writes surface"
    );
    assert!(
        root.join("schemas/experiment_finding.graphql").is_file(),
        "pipeline pack must be self-contained with pack-local schemas/"
    );
    let (manifest, report) = load_manifest_root(&root);
    assert!(
        report.errors.is_empty(),
        "pipeline fixture must load without errors: {:?}",
        report.errors
    );
    let manifest = manifest.expect("pipeline fixture must produce a manifest");
    assert_eq!(report.counts.datastore_tool_surfaces, 1);
    assert!(
        manifest
            .tool_selections
            .iter()
            .any(|s| s.selection_id == "exp-tools-stage1"
                && s.datastore_tool_surface_ids == ["experiment-writes".to_string()]
                && s.write_tools.is_empty()),
        "stage-1 selection must reference surface and drop inline write_tools: {:?}",
        manifest.tool_selections
    );
    let mut errors = Vec::new();
    validate_manifest(&manifest, &mut errors);
    // Backend/model offline endpoints may still pass static validate; surface links must not error.
    assert!(
        !errors
            .iter()
            .any(|e| e.contains("DatastoreToolSurface") || e.contains("datastore_tool_surface")),
        "pipeline fixture surface wiring must be valid: {errors:?}"
    );
}

#[test]
fn load_background_continuation_demo_with_local_background_target() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../demo/background-continuation");
    let (manifest, report) = load_manifest_root(&root);
    assert!(
        report.errors.is_empty(),
        "background-continuation pack must load without errors: {:?}",
        report.errors
    );
    let manifest = manifest.expect("background-continuation pack must produce a manifest");
    assert_eq!(report.counts.agent_behaviors, 2);
    assert_eq!(report.counts.tool_selections, 2);
    let parent = manifest
        .tool_selections
        .iter()
        .find(|selection| selection.selection_id == "background-parent-tools")
        .expect("background parent tool selection");
    assert!(parent.subagent_spawn_enabled);
    assert!(parent.subagent_background_enabled);
    assert_eq!(
        parent.subagent_default_await_mode.as_deref(),
        Some("background")
    );
    assert_eq!(parent.subagent_targets.len(), 1);
    let target = gents::SubagentTarget::parse(&parent.subagent_targets[0])
        .expect("pack target must be structured JSON");
    assert_eq!(target.name, "worker");
    assert_eq!(target.behavior_id, "background-worker");
    assert_eq!(target.agent_did, parent.agent_did);

    let mut errors = Vec::new();
    validate_manifest(&manifest, &mut errors);
    assert!(
        errors.is_empty(),
        "background-continuation pack wiring must validate: {errors:?}"
    );
}
