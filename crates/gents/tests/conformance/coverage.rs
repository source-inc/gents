use super::*;
use crate::lean_vocab_test::{lean_goal_decision_cases, lean_goal_transition_cases};

pub(super) fn lean_executable_contracts_cover_initial_domains() {
    for domain in [
        "Request",
        "Process",
        "Persistence.failClosed",
        "Persistence.failOpen",
        "StorageObservation.failClosed",
        "StorageObservation.failOpen",
        "RuntimeReconcile",
        "PairingReconcile",
        "SessionRecovery",
        "InferenceCall",
    ] {
        assert_state_machine_contract_is_complete(domain);
    }

    assert_lean_transition_is_legal("RuntimeReconcile", "applying", "idle");
    assert_lean_transition_is_legal("RuntimeReconcile", "idle", "debouncing");
    assert_lean_transition_is_legal("PairingReconcile", "idle", "diverged");
    assert_lean_transition_is_legal("PairingReconcile", "diverged", "converged");
    assert_lean_transition_is_legal("PairingReconcile", "converged", "crashed");
    assert_lean_transition_is_illegal("PairingReconcile", "idle", "converged");
    assert_lean_transition_is_legal("Persistence.failClosed", "committing", "uncommitted");
    assert_lean_transition_is_legal("Persistence.failOpen", "committing", "lost");
    assert_lean_transition_is_legal("StorageObservation.failClosed", "noMutation", "inFlight");
    assert_lean_transition_is_legal(
        "StorageObservation.failClosed",
        "inFlight",
        "successAcknowledged",
    );
    assert_lean_transition_is_legal(
        "StorageObservation.failClosed",
        "inFlight",
        "mutationFailed",
    );
    assert_lean_transition_is_legal(
        "StorageObservation.failClosed",
        "mutationFailed",
        "noMutation",
    );
    assert_lean_transition_is_legal(
        "StorageObservation.failOpen",
        "mutationFailed",
        "lostAcknowledged",
    );
    assert_lean_transition_is_legal(
        "StorageObservation.failClosed",
        "successAcknowledged",
        "staleObserved",
    );
    assert_lean_transition_is_legal(
        "StorageObservation.failClosed",
        "successAcknowledged",
        "readVisible",
    );
    assert_lean_transition_is_legal(
        "StorageObservation.failClosed",
        "staleObserved",
        "readVisible",
    );
    assert_lean_transition_is_illegal(
        "StorageObservation.failClosed",
        "mutationFailed",
        "lostAcknowledged",
    );
    assert_lean_transition_is_illegal(
        "StorageObservation.failOpen",
        "mutationFailed",
        "noMutation",
    );
    assert_eq!(
        lean_vocabulary_values("SessionRecoveryLatestRequestState"),
        vec![
            "pending",
            "claimed",
            "processing",
            "inputRequired",
            "completed",
            "failed",
            "superseded",
            "dead",
            "interrupted"
        ]
    );
    assert_lifecycle_transition_cases_partition(
        "Request",
        &lean_vocabulary_values("RequestState"),
        lean_request_transition_cases(),
    );
    assert_lean_transition_is_legal("SessionRecovery", "failed", "pending");
    assert_lean_transition_is_illegal("SessionRecovery", "dead", "pending");
    assert_lean_transition_is_illegal("SessionRecovery", "superseded", "pending");
    assert_lean_transition_is_illegal("SessionRecovery", "interrupted", "pending");
    assert_lean_transition_is_illegal("SessionRecovery", "inputRequired", "pending");
    assert_lean_transition_is_legal("InferenceCall", "queued", "running");
    assert_lean_transition_is_legal("InferenceCall", "running", "completed");
    assert_lean_transition_is_legal("InferenceCall", "running", "failed");
    let follow_up_hooks = &lean_contract_snapshot().follow_up_hooks;
    assert!(
        !follow_up_hooks
            .iter()
            .any(|hook| hook.contains("RuntimeReconcile")),
        "RuntimeReconcile should be emitted as generated contract output, not a follow-up hook"
    );
    assert!(
        !follow_up_hooks
            .iter()
            .any(|hook| hook.contains("ToolExecution")),
        "ToolExecution should be emitted as generated contract output, not a follow-up hook"
    );
    assert!(
        !follow_up_hooks
            .iter()
            .any(|hook| hook.contains("CommandPolicy")),
        "CommandPolicy should be emitted as generated contract output, not a follow-up hook"
    );
    assert_eq!(lean_contract_snapshot().runtime_reconcile_cases.len(), 6);
    assert_eq!(lean_contract_snapshot().request_transition_cases.len(), 81);
    assert_eq!(lean_contract_snapshot().process_transition_cases.len(), 25);
    assert_eq!(lean_contract_snapshot().apply_reconcile_cases.len(), 10);
    assert_eq!(lean_contract_snapshot().session_recovery_cases.len(), 18);
    assert_eq!(
        lean_contract_snapshot()
            .inference_slot_accounting_cases
            .len(),
        11
    );
    assert_eq!(
        lean_contract_snapshot().fleet_slot_accounting_cases.len(),
        5
    );
    assert_eq!(
        lean_contract_snapshot()
            .persistence_failure_policy_cases
            .len(),
        2
    );
    assert_eq!(
        lean_contract_snapshot()
            .storage_observation_runtime_cases
            .len(),
        8
    );
    assert_eq!(
        lean_contract_snapshot()
            .backend_health_admission_cases
            .len(),
        7
    );
    assert_eq!(
        lean_contract_snapshot().frontend_client_shell_case_count,
        lean_contract_snapshot().frontend_client_shell_cases.len()
    );
    assert_eq!(
        lean_contract_snapshot().frontend_client_shell_cases.len(),
        15
    );
    assert_eq!(
        lean_contract_snapshot().desktop_client_shell_case_count,
        lean_contract_snapshot().desktop_client_shell_cases.len()
    );
    assert_eq!(
        lean_contract_snapshot().desktop_client_shell_cases.len(),
        12
    );
    assert_eq!(lean_contract_snapshot().tool_preflight_cases.len(), 9);
    assert_eq!(lean_contract_snapshot().tool_retry_cases.len(), 63);
    assert_eq!(lean_contract_snapshot().command_policy_cases.len(), 48);
    assert_eq!(lean_contract_snapshot().command_sandbox_cases.len(), 4);
    assert_eq!(lean_contract_snapshot().command_env_cases.len(), 14);
    assert_eq!(lean_queue_deadline_cases().len(), 5);
    assert_eq!(lean_recovery_sweep_cases().len(), 37);
    assert_eq!(lean_recovery_equivalence_cases().len(), 37);
    assert_eq!(lean_recovery_outcome_cases().len(), 4);
    assert_eq!(lean_transcript_cases().len(), 7);
    assert_eq!(lean_response_interrupt_flow_cases().len(), 1);
    assert_eq!(lean_subagent_delegation_graph_cases().len(), 3);
    assert_eq!(lean_composed_invariant_witnesses().len(), 4);
    assert_eq!(lean_cancel_propagation_cases().len(), 1);
}

#[tokio::test]
async fn agent_tool_call_has_r5_cross_deployment_fields() {
    let db = crate::support::test_db("agent-tool-call-r5-fields").await;
    let response = db
        .node
        .execute(
            r#"{
                __type(name: "AgentToolCall") {
                    fields { name }
                }
            }"#,
        )
        .await;
    assert!(
        !response.has_errors(),
        "introspection errors: {:?}",
        response.errors
    );
    let names: HashSet<String> = response
        .data
        .as_ref()
        .and_then(|d| d.get("__type"))
        .and_then(|t| t.get("fields"))
        .and_then(|fs| fs.as_array())
        .map(|fs| {
            fs.iter()
                .filter_map(|f| f.get("name").and_then(|n| n.as_str()).map(String::from))
                .collect()
        })
        .unwrap_or_default();
    for field in [
        "unclaimed_deadline_at",
        "cancel_cascade_intent_at",
        "cancel_pending_remote_ack",
        "cancel_cause",
        "stuck_since",
        "spawn_target_did",
    ] {
        assert!(names.contains(field), "AgentToolCall missing field {field}");
    }
}

#[tokio::test]
async fn tool_selection_has_cross_deployment_spawn_timeout() {
    let db = crate::support::test_db("tool-selection-r5-timeout").await;
    let response = db
        .node
        .execute(
            r#"{
                __type(name: "ToolSelection") {
                    fields { name }
                }
            }"#,
        )
        .await;
    assert!(
        !response.has_errors(),
        "introspection errors: {:?}",
        response.errors
    );
    let names: HashSet<String> = response
        .data
        .as_ref()
        .and_then(|d| d.get("__type"))
        .and_then(|t| t.get("fields"))
        .and_then(|fs| fs.as_array())
        .map(|fs| {
            fs.iter()
                .filter_map(|f| f.get("name").and_then(|n| n.as_str()).map(String::from))
                .collect()
        })
        .unwrap_or_default();
    assert!(
        names.contains("cross_deployment_spawn_timeout_seconds"),
        "ToolSelection missing cross_deployment_spawn_timeout_seconds",
    );
}

#[test]
fn lean_boundary_metadata_is_typed_and_reviewable() {
    let snapshot = lean_contract_snapshot();
    let expected_boundary_ids = [
        "boundary.request.input-required-reserved",
        "boundary.request.dead-preclaim-only",
        "boundary.request.recovery-sweep-reachable",
        "boundary.tool-call.permanent-without-retry-evidence",
        "boundary.mcp.call-tool-dispatch-retry-evidence",
        "boundary.inference-slots.running-row-derived",
        "boundary.fleet-slot-accounting.derived-view",
        "boundary.command-policy.host-execution-assumptions",
        "boundary.compaction.safe-to-reduce-session-scope",
        "boundary.compaction.unique-call-ids-checked",
        "boundary.trigger.dispatch-source-delivery",
        "boundary.persistence.abstract-lifecycle",
        "boundary.storage.hook-failure-policy",
        "boundary.storage.observation-daemon-visible",
        "boundary.storage.minimum-visibility-path",
        "boundary.backend-health.admission-freshness",
        "boundary.session-recovery.client-retry-surface",
        "boundary.coverage-ledger.review-discipline",
        "boundary.event-delivery.fair-substrate",
        "boundary.event-delivery.rescan-doc-cap",
        "boundary.streaming-response.idle-timeout-deadline",
        "boundary.prompt-assembly.provider-input-sanitization",
        "boundary.model.nat-typed-ids-time",
        "boundary.p2p-backpressure.obligation-model",
        "boundary.rendered-capture.assembled-request-artifact",
        "boundary.rendered-capture.key-encoding-injectivity",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<BTreeSet<_>>();

    let mut actual_boundary_ids = BTreeSet::new();
    let mut boundary_subjects = BTreeSet::new();
    for boundary in &snapshot.boundaries {
        assert!(
            !boundary.id.trim().is_empty(),
            "boundary id must be non-empty: {:?}",
            boundary
        );
        assert!(
            !boundary.domain.trim().is_empty(),
            "boundary domain must be non-empty: {:?}",
            boundary
        );
        assert!(
            !boundary.subject.trim().is_empty(),
            "boundary subject must be non-empty: {:?}",
            boundary
        );
        assert!(
            !boundary.statement.trim().is_empty(),
            "boundary statement must be non-empty: {:?}",
            boundary
        );
        assert!(
            boundary
                .accepted_failure_mode
                .as_deref()
                .map_or(true, |text| !text.trim().is_empty()),
            "boundary accepted_failure_mode must be omitted or non-empty: {:?}",
            boundary
        );
        assert!(
            boundary
                .accepted_follow_up
                .as_deref()
                .map_or(true, |text| !text.trim().is_empty()),
            "boundary accepted_follow_up must be omitted or non-empty: {:?}",
            boundary
        );
        assert!(
            actual_boundary_ids.insert(boundary.id.clone()),
            "duplicate boundary id: {:?}",
            boundary
        );
        assert!(
            boundary_subjects.insert((boundary.domain.clone(), boundary.subject.clone())),
            "duplicate boundary subject in domain {:?}: {:?}",
            boundary.domain,
            boundary
        );
    }

    assert_eq!(
        actual_boundary_ids, expected_boundary_ids,
        "Lean boundary metadata ids changed; update this review-discipline list with the boundary data"
    );
}

#[test]
fn lean_deviation_metadata_is_empty_or_explicitly_classified() {
    let snapshot = lean_contract_snapshot();
    let mut deviation_ids = BTreeSet::new();
    let mut deviation_subjects = BTreeSet::new();

    for deviation in &snapshot.deviations {
        assert!(
            !deviation.id.trim().is_empty(),
            "deviation id must be non-empty: {:?}",
            deviation
        );
        assert!(
            !deviation.domain.trim().is_empty(),
            "deviation domain must be non-empty: {:?}",
            deviation
        );
        assert!(
            !deviation.subject.trim().is_empty(),
            "deviation subject must be non-empty: {:?}",
            deviation
        );
        assert!(
            !deviation.statement.trim().is_empty(),
            "deviation statement must be non-empty: {:?}",
            deviation
        );
        assert!(
            deviation
                .accepted_failure_mode
                .as_deref()
                .is_some_and(|text| !text.trim().is_empty())
                || deviation
                    .accepted_follow_up
                    .as_deref()
                    .is_some_and(|text| !text.trim().is_empty()),
            "active deviations must carry accepted_failure_mode or accepted_follow_up text: {:?}",
            deviation
        );
        assert!(
            deviation_ids.insert(deviation.id.clone()),
            "duplicate deviation id: {:?}",
            deviation
        );
        assert!(
            deviation_subjects.insert((deviation.domain.clone(), deviation.subject.clone())),
            "duplicate deviation subject in domain {:?}: {:?}",
            deviation.domain,
            deviation
        );
    }
}

const REQUIRE_FEATURE_TAG_FOR_ALL_ROWS: bool = true;

#[test]
fn lean_feature_matrix_covers_every_declared_required_surface() {
    let snapshot = lean_contract_snapshot();
    let valid_features: BTreeSet<&str> = snapshot
        .feature_surface_requirements
        .iter()
        .map(|req| req.feature.as_str())
        .collect();

    for entry in &snapshot.coverage_ledger {
        if REQUIRE_FEATURE_TAG_FOR_ALL_ROWS {
            assert!(
                !entry.feature.is_empty(),
                "coverage ledger row is untagged: {:?}",
                entry
            );
        }
        if !entry.feature.is_empty() {
            assert!(
                valid_features.contains(entry.feature.as_str()),
                "coverage ledger row tags unknown feature: {:?}",
                entry
            );
            if entry.category != "follow_up_hook" {
                assert!(
                    !entry.surfaces.is_empty(),
                    "coverage ledger row carries feature {:?} but no surfaces; \
                     each tagged non-follow-up hook row must declare at least one surface: {:?}",
                    entry.feature,
                    entry
                );
            }
        }
    }

    for req in &snapshot.feature_surface_requirements {
        for surface in &req.required {
            let covered = snapshot.coverage_ledger.iter().any(|entry| {
                entry.feature == req.feature
                    && entry.surfaces.iter().any(|candidate| candidate == surface)
            });
            assert!(
                covered,
                "feature {:?} declares required surface {:?} but no \
                 ledger row tags this (feature, surface). Either add a \
                 ledger row, or move this surface to `deferred` with a \
                 follow-up note.",
                req.feature, surface
            );
        }
    }

    for req in &snapshot.feature_surface_requirements {
        for surface in &req.required {
            let cell = snapshot
                .feature_matrix
                .get(&req.feature)
                .and_then(|surfaces| surfaces.get(surface.as_str()));
            let strength = cell
                .map(|cell| cell.coverage_strength.as_str())
                .unwrap_or("missing");
            assert!(
                strength != "missing",
                "feature_matrix[{}][{:?}] is `missing` but required",
                req.feature,
                surface
            );
        }
    }
}

#[test]
fn lean_contract_coverage_ledger_accounts_for_every_emitted_domain() {
    let snapshot = lean_contract_snapshot();
    let mut emitted = BTreeSet::new();
    let boundary_ids = snapshot
        .boundaries
        .iter()
        .map(|boundary| boundary.id.clone())
        .collect::<BTreeSet<_>>();

    for vocabulary in &snapshot.vocabularies {
        emitted.insert(("vocabulary".to_string(), vocabulary.domain.clone()));
    }
    for machine in &snapshot.state_machines {
        emitted.insert(("state_machine".to_string(), machine.domain.clone()));
    }
    if !snapshot.request_transition_cases.is_empty() {
        emitted.insert((
            "lifecycle_transition_cases".to_string(),
            "RequestTransitions".to_string(),
        ));
    }
    if !snapshot.process_transition_cases.is_empty() {
        emitted.insert((
            "lifecycle_transition_cases".to_string(),
            "ProcessTransitions".to_string(),
        ));
    }
    assert_eq!(
        snapshot.trigger_dispatch_case_count,
        snapshot.trigger_dispatch_cases.len(),
        "Lean trigger dispatch case count drifted from emitted cases"
    );
    if !snapshot.trigger_dispatch_cases.is_empty() {
        emitted.insert(("trigger_cases".to_string(), "TriggerDispatch".to_string()));
    }
    if !snapshot.runtime_reconcile_cases.is_empty() {
        emitted.insert((
            "runtime_cases".to_string(),
            "RuntimeReconcileCases".to_string(),
        ));
    }
    if !snapshot.apply_reconcile_cases.is_empty() {
        emitted.insert((
            "apply_reconcile_cases".to_string(),
            "ApplyReconcileCases".to_string(),
        ));
    }
    if !snapshot.tool_policy_cases.is_empty() {
        emitted.insert((
            "tool_policy_cases".to_string(),
            "ToolPolicyCases".to_string(),
        ));
    }
    if !snapshot.self_config_field_tables.is_empty() {
        emitted.insert((
            "self_config_field_tables".to_string(),
            "SelfConfigFieldTables".to_string(),
        ));
    }
    if !snapshot.self_config_cases.is_empty() {
        emitted.insert((
            "self_config_cases".to_string(),
            "SelfConfigCases".to_string(),
        ));
    }
    if !snapshot.session_recovery_cases.is_empty() {
        emitted.insert((
            "session_recovery_cases".to_string(),
            "SessionRecoveryCases".to_string(),
        ));
    }
    if !snapshot.inference_slot_accounting_cases.is_empty() {
        emitted.insert((
            "slot_cases".to_string(),
            "InferenceCallSlotAccounting".to_string(),
        ));
    }
    if !snapshot.inference_call_exact_target_cases.is_empty() {
        emitted.insert((
            "inference_exact_target_cases".to_string(),
            "InferenceCallExactTarget".to_string(),
        ));
    }
    if !snapshot.inference_call_exact_target_trace_cases.is_empty() {
        emitted.insert((
            "inference_exact_target_trace_cases".to_string(),
            "InferenceCallExactTargetTraces".to_string(),
        ));
    }
    if !snapshot.fleet_slot_accounting_cases.is_empty() {
        emitted.insert(("fleet_cases".to_string(), "FleetSlotAccounting".to_string()));
    }
    if !snapshot.persistence_failure_policy_cases.is_empty() {
        emitted.insert((
            "persistence_policy_cases".to_string(),
            "PersistenceFailurePolicyCases".to_string(),
        ));
    }
    if !snapshot.storage_observation_runtime_cases.is_empty() {
        emitted.insert((
            "storage_observation_cases".to_string(),
            "StorageObservationRuntimeCases".to_string(),
        ));
    }
    if !snapshot.backend_health_admission_cases.is_empty() {
        emitted.insert((
            "backend_health_cases".to_string(),
            "BackendHealthAdmissionCases".to_string(),
        ));
    }
    if !snapshot.backend_health_cases.is_empty() {
        emitted.insert((
            "backend_health_cases".to_string(),
            "BackendHealthTransitionCases".to_string(),
        ));
    }
    if !snapshot.native_filesystem_boundary_cases.is_empty() {
        emitted.insert((
            "native_filesystem_boundary_cases".to_string(),
            "NativeFilesystemBoundaryCases".to_string(),
        ));
    }
    if !snapshot.managed_exec_tool_boundary_cases.is_empty() {
        emitted.insert((
            "managed_exec_cases".to_string(),
            "ManagedExecToolBoundaryCases".to_string(),
        ));
    }
    if !snapshot
        .pairing_reconcile_shutdown_boundary_cases
        .is_empty()
    {
        emitted.insert((
            "pairing_reconcile_cases".to_string(),
            "PairingReconcileShutdownBoundaryCases".to_string(),
        ));
    }
    if !snapshot
        .pairing_reconcile_sweep_retry_boundary_cases
        .is_empty()
    {
        emitted.insert((
            "pairing_reconcile_cases".to_string(),
            "PairingReconcileSweepRetryBoundaryCases".to_string(),
        ));
    }
    if !snapshot.pairing_reconcile_sweep_scheduling_cases.is_empty() {
        emitted.insert((
            "pairing_reconcile_cases".to_string(),
            "PairingReconcileSweepSchedulingCases".to_string(),
        ));
    }
    if !snapshot.managed_exec_liveness_cases.is_empty() {
        emitted.insert((
            "managed_exec_cases".to_string(),
            "ManagedExecLivenessCases".to_string(),
        ));
    }
    assert_eq!(
        snapshot.frontend_client_shell_case_count,
        snapshot.frontend_client_shell_cases.len(),
        "Lean frontend ClientShell case count drifted from emitted cases"
    );
    if !snapshot.frontend_client_shell_cases.is_empty() {
        emitted.insert((
            "frontend_client_shell_cases".to_string(),
            "FrontendClientShellCases".to_string(),
        ));
    }
    assert_eq!(
        snapshot.desktop_client_shell_case_count,
        snapshot.desktop_client_shell_cases.len(),
        "Lean desktop ClientShell case count drifted from emitted cases"
    );
    if !snapshot.desktop_client_shell_cases.is_empty() {
        emitted.insert((
            "desktop_client_shell_cases".to_string(),
            "DesktopClientShellCases".to_string(),
        ));
    }
    if !snapshot.request_lifecycle_operator_ui_cases.is_empty() {
        emitted.insert((
            "request_lifecycle_operator_ui_cases".to_string(),
            "RequestLifecycleOperatorUiCases".to_string(),
        ));
    }
    if !snapshot.tool_preflight_cases.is_empty() {
        emitted.insert((
            "tool_cases".to_string(),
            "ToolExecutionPreflight".to_string(),
        ));
    }
    if !snapshot.tool_retry_cases.is_empty() {
        emitted.insert(("tool_cases".to_string(), "ToolExecutionRetry".to_string()));
    }
    if !snapshot.completion_retry_cases.is_empty() {
        emitted.insert((
            "completion_retry_cases".to_string(),
            "completionRetry".to_string(),
        ));
    }
    if !snapshot.command_policy_cases.is_empty() {
        emitted.insert((
            "command_policy_cases".to_string(),
            "CommandPolicyValidation".to_string(),
        ));
        emitted.insert((
            "command_policy_cases".to_string(),
            "CommandPolicyOperatorUi".to_string(),
        ));
    }
    if !snapshot.command_sandbox_cases.is_empty() {
        emitted.insert((
            "command_policy_cases".to_string(),
            "CommandPolicySandbox".to_string(),
        ));
    }
    if !snapshot.command_env_cases.is_empty() {
        emitted.insert((
            "command_policy_cases".to_string(),
            "CommandPolicyEnv".to_string(),
        ));
    }
    if !snapshot.live_overlay_cases.is_empty() {
        emitted.insert((
            "live_overlay_cases".to_string(),
            "LiveOverlayCases".to_string(),
        ));
    }
    if !lean_queue_deadline_cases().is_empty() {
        emitted.insert((
            "queue_deadline_cases".to_string(),
            "QueueDeadlineConformanceCases".to_string(),
        ));
    }
    if !lean_recovery_sweep_cases().is_empty() {
        emitted.insert((
            "recovery_sweep_cases".to_string(),
            "RecoverySweepCases".to_string(),
        ));
    }
    if !lean_recovery_outcome_cases().is_empty() {
        emitted.insert((
            "recovery_outcome_cases".to_string(),
            "RecoveryOutcomeCases".to_string(),
        ));
    }
    if !lean_recovery_equivalence_cases().is_empty() {
        emitted.insert((
            "recovery_equivalence_cases".to_string(),
            "RecoveryEquivalenceCases".to_string(),
        ));
    }
    if !lean_restart_disposition_cases().is_empty() {
        emitted.insert((
            "restart_disposition_cases".to_string(),
            "RestartDispositionCases".to_string(),
        ));
    }
    if !lean_tool_output_paging_cases().is_empty() {
        emitted.insert((
            "tool_output_paging_cases".to_string(),
            "ToolOutputPagingCases".to_string(),
        ));
    }
    if !lean_bridge_step_cases().is_empty() {
        emitted.insert((
            "bridge_step_cases".to_string(),
            "BridgeStepCases".to_string(),
        ));
    }
    if !lean_transcript_cases().is_empty() {
        emitted.insert((
            "transcript_cases".to_string(),
            "TranscriptConformanceCases".to_string(),
        ));
    }
    if !lean_transcript_finalization_cases().is_empty() {
        emitted.insert((
            "transcript_finalization_cases".to_string(),
            "TranscriptFinalizationCases".to_string(),
        ));
    }
    if !lean_transcript_provider_history_cases().is_empty() {
        emitted.insert((
            "transcript_provider_history_cases".to_string(),
            "TranscriptProviderHistoryCases".to_string(),
        ));
    }
    if !lean_response_transition_cases().is_empty() {
        emitted.insert((
            "streaming_response_cases".to_string(),
            "ResponseTransitionCases".to_string(),
        ));
    }
    if !lean_response_interrupt_flow_cases().is_empty() {
        emitted.insert((
            "streaming_response_interrupt_flow_cases".to_string(),
            "ResponseInterruptFlowCases".to_string(),
        ));
    }
    if !lean_compaction_reducer_cases().is_empty() {
        emitted.insert((
            "compaction_reducer_cases".to_string(),
            "CompactionReducerCases".to_string(),
        ));
    }
    if !snapshot.prompt_assembly_sanitize_cases.is_empty() {
        emitted.insert((
            "prompt_assembly_cases".to_string(),
            "PromptAssemblySanitizeCases".to_string(),
        ));
    }
    if !snapshot.prompt_assembly_layer_cases.is_empty() {
        emitted.insert((
            "prompt_assembly_cases".to_string(),
            "PromptAssemblyLayerCases".to_string(),
        ));
    }
    if !snapshot.prompt_assembly_repair_cases.is_empty() {
        emitted.insert((
            "prompt_assembly_cases".to_string(),
            "PromptAssemblyRepairCases".to_string(),
        ));
    }
    if !snapshot.prompt_assembly_budget_cases.is_empty() {
        emitted.insert((
            "prompt_assembly_cases".to_string(),
            "PromptAssemblyBudgetCases".to_string(),
        ));
    }
    if !snapshot.prompt_assembly_turn_budget_cases.is_empty() {
        emitted.insert((
            "prompt_assembly_cases".to_string(),
            "PromptAssemblyTurnBudgetCases".to_string(),
        ));
    }
    if !snapshot.rendered_capture_cases.is_empty() {
        emitted.insert((
            "rendered_capture_cases".to_string(),
            "RenderedCaptureCases".to_string(),
        ));
    }
    if !snapshot.rendered_capture_key_cases.is_empty() {
        emitted.insert((
            "rendered_capture_cases".to_string(),
            "RenderedCaptureKeyCases".to_string(),
        ));
    }
    if !snapshot.request_ingest_cases.is_empty() {
        emitted.insert((
            "request_ingest_cases".to_string(),
            "RequestIngestCases".to_string(),
        ));
    }
    if !snapshot.subagent_bridge_admission_cases.is_empty() {
        emitted.insert((
            "subagent_bridge_admission_cases".to_string(),
            "SubagentBridgeAdmissionCases".to_string(),
        ));
    }
    assert_eq!(
        snapshot.event_delivery_transition_case_count,
        snapshot.event_delivery_transition_cases.len(),
        "Lean event-delivery transition case count drifted from emitted cases"
    );
    if !snapshot.event_delivery_transition_cases.is_empty() {
        emitted.insert((
            "event_delivery_cases".to_string(),
            "EventDeliveryTransitionCases".to_string(),
        ));
    }
    if !snapshot.event_delivery_source_instances.is_empty() {
        emitted.insert((
            "event_delivery_cases".to_string(),
            "EventDeliverySourceInstances".to_string(),
        ));
    }
    if !snapshot.event_delivery_convergence_traces.is_empty() {
        emitted.insert((
            "event_delivery_cases".to_string(),
            "EventDeliveryConvergenceTraces".to_string(),
        ));
    }
    if !lean_mcp_health_cases().is_empty() {
        emitted.insert(("mcp_health_cases".to_string(), "MCPHealthCases".to_string()));
    }
    if !snapshot.identity_structural_cases.is_empty() {
        emitted.insert((
            "identity_structural_cases".to_string(),
            "IdentityStructuralCases".to_string(),
        ));
    }
    if !snapshot.identity_permission_cases.is_empty() {
        emitted.insert((
            "identity_permission_cases".to_string(),
            "IdentityPermissionCases".to_string(),
        ));
    }
    if !snapshot.identity_contracts.is_empty() {
        emitted.insert((
            "identity_contracts".to_string(),
            "IdentityContracts".to_string(),
        ));
    }
    if !lean_r4c_background_work_cases().is_empty() {
        emitted.insert((
            "r4c_background_work_cases".to_string(),
            "R4cBackgroundWorkCases".to_string(),
        ));
    }
    if !lean_codex_shim_projection_cases().is_empty() {
        emitted.insert((
            "codex_shim_projection_cases".to_string(),
            "CodexShimProjectionCases".to_string(),
        ));
    }
    if !lean_codex_shim_subagent_tool_cases().is_empty() {
        emitted.insert((
            "codex_shim_subagent_tool_cases".to_string(),
            "CodexShimSubagentToolCases".to_string(),
        ));
    }
    if !lean_codex_shim_subagent_status_cases().is_empty() {
        emitted.insert((
            "codex_shim_subagent_status_cases".to_string(),
            "CodexShimSubagentStatusCases".to_string(),
        ));
    }
    if !lean_codex_shim_subagent_visibility_cases().is_empty() {
        emitted.insert((
            "codex_shim_subagent_visibility_cases".to_string(),
            "CodexShimSubagentVisibilityCases".to_string(),
        ));
    }
    if !lean_codex_shim_subagent_metadata_cases().is_empty() {
        emitted.insert((
            "codex_shim_subagent_metadata_cases".to_string(),
            "CodexShimSubagentMetadataCases".to_string(),
        ));
    }
    if !lean_codex_shim_subagent_listing_cases().is_empty() {
        emitted.insert((
            "codex_shim_subagent_listing_cases".to_string(),
            "CodexShimSubagentListingCases".to_string(),
        ));
    }
    if !lean_codex_shim_subagent_thread_shape_cases().is_empty() {
        emitted.insert((
            "codex_shim_subagent_thread_shape_cases".to_string(),
            "CodexShimSubagentThreadShapeCases".to_string(),
        ));
    }
    if !lean_codex_shim_reasoning_projection_cases().is_empty() {
        emitted.insert((
            "codex_shim_reasoning_projection_cases".to_string(),
            "CodexShimReasoningProjectionCases".to_string(),
        ));
    }
    if !lean_codex_shim_thread_status_cases().is_empty() {
        emitted.insert((
            "codex_shim_thread_status_cases".to_string(),
            "CodexShimThreadStatusCases".to_string(),
        ));
    }
    if !lean_codex_shim_behavior_selection_cases().is_empty() {
        emitted.insert((
            "codex_shim_behavior_selection_cases".to_string(),
            "CodexShimBehaviorSelectionCases".to_string(),
        ));
    }
    if !lean_codex_shim_tool_metadata_cases().is_empty() {
        emitted.insert((
            "codex_shim_tool_metadata_cases".to_string(),
            "CodexShimToolMetadataCases".to_string(),
        ));
    }
    if !lean_codex_shim_context_usage_cases().is_empty() {
        emitted.insert((
            "codex_shim_context_usage_cases".to_string(),
            "CodexShimContextUsageCases".to_string(),
        ));
    }
    if !lean_codex_shim_compaction_projection_cases().is_empty() {
        emitted.insert((
            "codex_shim_compaction_projection_cases".to_string(),
            "CodexShimCompactionProjectionCases".to_string(),
        ));
    }
    if !lean_codex_shim_binding_cases().is_empty() {
        emitted.insert((
            "codex_shim_binding_cases".to_string(),
            "CodexShimBindingCases".to_string(),
        ));
    }
    if !lean_startup_readiness_cases().is_empty() {
        emitted.insert((
            "startup_readiness_cases".to_string(),
            "StartupReadinessCases".to_string(),
        ));
    }
    if !lean_codex_shim_turn_lifecycle_cases().is_empty() {
        emitted.insert((
            "codex_shim_turn_lifecycle_cases".to_string(),
            "CodexShimTurnLifecycleCases".to_string(),
        ));
    }
    if !lean_r6_backgrounding_cases().is_empty() {
        emitted.insert((
            "r6_background_cases".to_string(),
            "R6BackgroundingCases".to_string(),
        ));
    }
    if !lean_r5_cross_deployment_cases().is_empty() {
        emitted.insert((
            "r5_cross_deployment_cases".to_string(),
            "R5CrossDeploymentCases".to_string(),
        ));
    }
    if !lean_composed_invariant_witnesses().is_empty() {
        emitted.insert((
            "composed_invariant_witnesses".to_string(),
            "ComposedInvariantWitnesses".to_string(),
        ));
    }
    if !lean_cancel_propagation_cases().is_empty() {
        emitted.insert((
            "cancel_propagation_cases".to_string(),
            "CancelPropagationCases".to_string(),
        ));
    }
    if !lean_r6_background_theorem_witnesses().is_empty() {
        emitted.insert((
            "r6_background_theorem_witnesses".to_string(),
            "BackgroundBudgetBoundedTheoremWitness".to_string(),
        ));
        emitted.insert((
            "r6_background_theorem_witnesses".to_string(),
            "CascadeCancelsChildTheoremWitness".to_string(),
        ));
    }
    if !lean_subagent_delegation_graph_cases().is_empty() {
        emitted.insert((
            "subagent_delegation_graph_cases".to_string(),
            "SubagentDelegationGraphCases".to_string(),
        ));
    }
    if !lean_goal_decision_cases().is_empty() {
        emitted.insert((
            "goal_decision_cases".to_string(),
            "GoalDecisionCases".to_string(),
        ));
    }
    if !lean_goal_transition_cases().is_empty() {
        emitted.insert((
            "goal_transition_cases".to_string(),
            "GoalTransitionCases".to_string(),
        ));
    }
    for hook in &snapshot.follow_up_hooks {
        emitted.insert(("follow_up_hook".to_string(), hook.clone()));
    }

    let valid_categories = [
        "vocabulary",
        "state_machine",
        "lifecycle_transition_cases",
        "trigger_cases",
        "runtime_cases",
        "apply_reconcile_cases",
        "tool_policy_cases",
        "self_config_field_tables",
        "self_config_cases",
        "session_recovery_cases",
        "slot_cases",
        "inference_exact_target_cases",
        "inference_exact_target_trace_cases",
        "fleet_cases",
        "persistence_policy_cases",
        "storage_observation_cases",
        "backend_health_cases",
        "native_filesystem_boundary_cases",
        "managed_exec_cases",
        "pairing_reconcile_cases",
        "frontend_client_shell_cases",
        "desktop_client_shell_cases",
        "request_lifecycle_operator_ui_cases",
        "tool_cases",
        "completion_retry_cases",
        "command_policy_cases",
        "live_overlay_cases",
        "queue_deadline_cases",
        "recovery_sweep_cases",
        "recovery_outcome_cases",
        "recovery_equivalence_cases",
        "restart_disposition_cases",
        "tool_output_paging_cases",
        "bridge_step_cases",
        "subagent_bridge_admission_cases",
        "transcript_cases",
        "transcript_finalization_cases",
        "transcript_provider_history_cases",
        "compaction_reducer_cases",
        "prompt_assembly_cases",
        "rendered_capture_cases",
        "request_ingest_cases",
        "streaming_response_cases",
        "streaming_response_interrupt_flow_cases",
        "event_delivery_cases",
        "mcp_health_cases",
        "identity_structural_cases",
        "identity_permission_cases",
        "identity_contracts",
        "r4c_background_work_cases",
        "codex_shim_projection_cases",
        "codex_shim_subagent_tool_cases",
        "codex_shim_subagent_status_cases",
        "codex_shim_subagent_visibility_cases",
        "codex_shim_subagent_metadata_cases",
        "codex_shim_subagent_listing_cases",
        "codex_shim_subagent_thread_shape_cases",
        "codex_shim_reasoning_projection_cases",
        "codex_shim_thread_status_cases",
        "codex_shim_behavior_selection_cases",
        "codex_shim_tool_metadata_cases",
        "codex_shim_context_usage_cases",
        "codex_shim_compaction_projection_cases",
        "codex_shim_binding_cases",
        "startup_readiness_cases",
        "codex_shim_turn_lifecycle_cases",
        "r6_background_cases",
        "r5_cross_deployment_cases",
        "composed_invariant_witnesses",
        "cancel_propagation_cases",
        "r6_background_theorem_witnesses",
        "subagent_delegation_graph_cases",
        "goal_decision_cases",
        "goal_transition_cases",
        "follow_up_hook",
    ];
    let registered_consumers = assert_registered_conformance_consumers_resolve();
    let mut ledger_domains = BTreeSet::new();
    let mut ledger_domain_surfaces = BTreeSet::new();
    let mut ledger_consumers = BTreeSet::new();

    for entry in &snapshot.coverage_ledger {
        assert!(
            valid_categories.contains(&entry.category.as_str()),
            "coverage ledger entry has unknown category: {:?}",
            entry
        );
        assert!(
            !entry.domain.trim().is_empty(),
            "coverage ledger entry has an empty domain: {:?}",
            entry
        );

        let has_consumer = !entry.consumer.trim().is_empty();
        let has_boundary = !entry.accepted_boundary.trim().is_empty();
        let has_follow_up = !entry.accepted_follow_up.trim().is_empty();
        assert!(
            has_consumer || has_boundary || has_follow_up,
            "coverage ledger entry must name a consumer, boundary, or follow-up: {:?}",
            entry
        );
        if entry.category == "follow_up_hook" {
            assert!(
                has_follow_up || has_boundary,
                "follow-up hook ledger entries must carry accepted_follow_up text or accepted_boundary id: {:?}",
                entry
            );
        }
        if has_boundary {
            assert!(
                boundary_ids.contains(&entry.accepted_boundary),
                "coverage ledger accepted_boundary must reference an emitted boundary id: {:?}",
                entry
            );
        }
        if has_consumer {
            assert!(
                registered_consumers.contains(entry.consumer.as_str()),
                "coverage ledger consumer must resolve to a registered Rust/TS conformance consumer: {:?}",
                entry
            );
            ledger_consumers.insert(entry.consumer.as_str());
        }

        ledger_domains.insert((entry.category.clone(), entry.domain.clone()));
        for surface in &entry.surfaces {
            assert!(
                ledger_domain_surfaces.insert((
                    entry.category.clone(),
                    entry.domain.clone(),
                    *surface
                )),
                "duplicate coverage ledger entry for {:?} / {:?} / {:?}",
                entry.category,
                entry.domain,
                surface
            );
        }
    }

    let missing = emitted
        .difference(&ledger_domains)
        .cloned()
        .collect::<Vec<_>>();
    let extra = ledger_domains
        .difference(&emitted)
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "coverage ledger must exactly match emitted Lean contract domains\n  missing ledger entries: {:?}\n  extra ledger entries: {:?}\n  emitted: {:?}\n  ledger: {:?}",
        missing,
        extra,
        emitted,
        ledger_domains
    );
    let unreferenced_consumers = registered_consumers
        .difference(&ledger_consumers)
        .copied()
        .collect::<Vec<_>>();
    assert!(
        unreferenced_consumers.is_empty(),
        "coverage ledger consumer registry has unreferenced entries: {:?}",
        unreferenced_consumers
    );
}
