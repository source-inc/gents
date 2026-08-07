use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy)]
pub enum ConformanceConsumer {
    RustTest {
        id: &'static str,
        package: &'static str,
        source_path: &'static str,
        module_path: &'static str,
        function: &'static str,
    },
    TypeScriptTest {
        id: &'static str,
        app: &'static str,
        source_path: &'static str,
        suite: &'static str,
        test: &'static str,
    },
}

impl ConformanceConsumer {
    pub fn id(&self) -> &'static str {
        match self {
            Self::RustTest { id, .. } | Self::TypeScriptTest { id, .. } => id,
        }
    }

    fn assert_resolves(&self, repo_root: &Path, sources: &mut BTreeMap<&'static str, String>) {
        match self {
            Self::RustTest {
                id,
                package,
                source_path,
                module_path,
                function,
            } => {
                let source = cached_source(repo_root, sources, source_path);
                assert_rust_test_function(source, id, package, source_path, module_path, function);
            }
            Self::TypeScriptTest {
                id,
                app,
                source_path,
                suite,
                test,
            } => {
                let source = cached_source(repo_root, sources, source_path);
                assert_typescript_test(source, id, app, source_path, suite, test);
            }
        }
    }
}

pub fn registered_conformance_consumers() -> &'static [ConformanceConsumer] {
    &[
        ConformanceConsumer::RustTest {
            id: "conformance::goals::rust_goal_status_vocabulary_and_machine_match_lean_contract",
            package: "gents",
            source_path: "crates/gents/tests/conformance/goals.rs",
            module_path: "conformance::goals",
            function: "rust_goal_status_vocabulary_and_machine_match_lean_contract",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::goals::generated_goal_decision_cases_fence_runtime_controller",
            package: "gents",
            source_path: "crates/gents/tests/conformance/goals.rs",
            module_path: "conformance::goals",
            function: "generated_goal_decision_cases_fence_runtime_controller",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::goals::generated_goal_transition_cases_fence_runtime_state_machine",
            package: "gents",
            source_path: "crates/gents/tests/conformance/goals.rs",
            module_path: "conformance::goals",
            function: "generated_goal_transition_cases_fence_runtime_state_machine",
        },
        ConformanceConsumer::RustTest {
            id: "goal_continuation_live::durable_goal_continues_with_real_inference_until_model_completes",
            package: "gents",
            source_path: "crates/gents/tests/e2e_live/goal_continuation_live.rs",
            module_path: "goal_continuation_live",
            function: "durable_goal_continues_with_real_inference_until_model_completes",
        },
        ConformanceConsumer::RustTest {
            id: "cli_goal::goal_set_get_pause_resume_and_clear_are_durable",
            package: "gents-cli",
            source_path: "crates/gents-cli/tests/cli_goal.rs",
            module_path: "cli_goal",
            function: "goal_set_get_pause_resume_and_clear_are_durable",
        },
        ConformanceConsumer::RustTest {
            id: "cli_codex_shim::thread_goal_round_trip_survives_shim_restart",
            package: "gents-cli",
            source_path: "crates/gents-cli/tests/cli_codex_shim.rs",
            module_path: "cli_codex_shim",
            function: "thread_goal_round_trip_survives_shim_restart",
        },
        ConformanceConsumer::TypeScriptTest {
            id: "apps/gents-desktop/tests/durable-goal-card.test.tsx::durable goal transcript card renders persisted goal status, objective, token usage, and active time",
            app: "gents-desktop",
            source_path: "apps/gents-desktop/tests/durable-goal-card.test.tsx",
            suite: "durable goal transcript card",
            test: "renders persisted goal status, objective, token usage, and active time",
        },
        ConformanceConsumer::RustTest {
            id: "admission::tests::generated_slot_accounting_fleet_cases_match_admission_runtime_boundary",
            package: "gents",
            source_path: "crates/gents/src/admission/tests.rs",
            module_path: "admission::tests",
            function: "generated_slot_accounting_fleet_cases_match_admission_runtime_boundary",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_inference_slot_accounting_cases_drive_db_backed_reconstruction",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_inference_slot_accounting_cases_drive_db_backed_reconstruction",
        },
        ConformanceConsumer::RustTest {
            id: "admission::tests::rust_inference_call_state_vocabulary_matches_lean_model",
            package: "gents",
            source_path: "crates/gents/src/admission/tests.rs",
            module_path: "admission::tests",
            function: "rust_inference_call_state_vocabulary_matches_lean_model",
        },
        ConformanceConsumer::RustTest {
            id: "admission::tests::rust_inference_call_terminal_reason_vocabulary_matches_lean_model",
            package: "gents",
            source_path: "crates/gents/src/admission/tests.rs",
            module_path: "admission::tests",
            function: "rust_inference_call_terminal_reason_vocabulary_matches_lean_model",
        },
        ConformanceConsumer::RustTest {
            id: "admission::tests::rust_inference_call_transition_table_matches_lean_contract",
            package: "gents",
            source_path: "crates/gents/src/admission/tests.rs",
            module_path: "admission::tests",
            function: "rust_inference_call_transition_table_matches_lean_contract",
        },
        ConformanceConsumer::TypeScriptTest {
            id: "packages/gents-desktop-chat/src/chat-shell.test.ts::projectChatShell matches generated Lean ClientShell projection contracts",
            app: "gents-desktop-chat",
            source_path: "packages/gents-desktop-chat/src/chat-shell.test.ts",
            suite: "projectChatShell",
            test: "matches generated Lean ClientShell projection contracts",
        },
        ConformanceConsumer::RustTest {
            id: "agent::reconcile::tests::pairing_reconcile_state_machine_contract_is_complete",
            package: "gents",
            source_path: "crates/gents/src/agent/reconcile/tests.rs",
            module_path: "agent::reconcile::tests",
            function: "pairing_reconcile_state_machine_contract_is_complete",
        },
        ConformanceConsumer::RustTest {
            id: "config_import::lean_apply_write_boundary_tests::generated_apply_reconcile_cases_fence_production_apply_write_boundary",
            package: "gents-cli",
            source_path:
                "crates/gents-cli/src/config_import/lean_apply_write_boundary_tests.rs",
            module_path: "config_import::lean_apply_write_boundary_tests",
            function: "generated_apply_reconcile_cases_fence_production_apply_write_boundary",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_tool_policy_cases_match_lean_composition",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_tool_policy_cases_match_lean_composition",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::self_config_field_tables_match_lean_contract",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "self_config_field_tables_match_lean_contract",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_self_config_cases_fence_patch_merge",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_self_config_cases_fence_patch_merge",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::completion_retry_lean_witness_cases_hold",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "completion_retry_lean_witness_cases_hold",
        },
        ConformanceConsumer::RustTest {
            id: "cli_mcp_probe::mcp_probe_json_reports_health_snapshot_for_registry_service",
            package: "gents-cli",
            source_path: "crates/gents-cli/tests/cli_mcp_probe.rs",
            module_path: "cli_mcp_probe",
            function: "mcp_probe_json_reports_health_snapshot_for_registry_service",
        },
        ConformanceConsumer::RustTest {
            id: "cli_config_task_run::config_task_run_matches_lean_manual_dispatch_contract",
            package: "gents-cli",
            source_path: "crates/gents-cli/tests/cli_config_task_run.rs",
            module_path: "cli_config_task_run",
            function: "config_task_run_matches_lean_manual_dispatch_contract",
        },
        ConformanceConsumer::RustTest {
            id: "backend_registry::tests::generated_backend_health_admission_cases_match_registry_and_admission_policy",
            package: "gents",
            source_path: "crates/gents/src/backend_registry/tests.rs",
            module_path: "backend_registry::tests",
            function: "generated_backend_health_admission_cases_match_registry_and_admission_policy",
        },
        ConformanceConsumer::RustTest {
            id: "backend_registry::tests::display_state_matches_every_lean_backend_health_admission_case",
            package: "gents",
            source_path: "crates/gents/src/backend_registry/tests.rs",
            module_path: "backend_registry::tests",
            function: "display_state_matches_every_lean_backend_health_admission_case",
        },
        ConformanceConsumer::RustTest {
            id: "backend_health::tests::generated_backend_health_cases_match_prober_transitions",
            package: "gents",
            source_path: "crates/gents/src/backend_health.rs",
            module_path: "backend_health::tests",
            function: "generated_backend_health_cases_match_prober_transitions",
        },
        ConformanceConsumer::RustTest {
            id: "http::prometheus::tests::backend_probe_status_metric_reflects_measured_health",
            package: "gents-cli",
            source_path: "crates/gents-cli/src/http/prometheus.rs",
            module_path: "http::prometheus::tests",
            function: "backend_probe_status_metric_reflects_measured_health",
        },
        ConformanceConsumer::RustTest {
            id: "cli_server::server_exposes_fleet_slot_snapshot_endpoint",
            package: "gents-cli",
            source_path: "crates/gents-cli/tests/cli_server.rs",
            module_path: "cli_server",
            function: "server_exposes_fleet_slot_snapshot_endpoint",
        },
        ConformanceConsumer::RustTest {
            id: "gents_desktop_bridge::snapshot::tests::session_state::session_snapshot_projection_consumes_generated_client_shell_contract_cases",
            package: "gents-desktop-bridge",
            source_path: "crates/gents-desktop-bridge/src/snapshot/tests/session_state.rs",
            module_path: "gents_desktop_bridge::snapshot::tests::session_state",
            function: "session_snapshot_projection_consumes_generated_client_shell_contract_cases",
        },
        ConformanceConsumer::RustTest {
            id: "gents_desktop_bridge::snapshot::tests::session_state::session_snapshot_binds_request_lifecycle_operator_ui_cases",
            package: "gents-desktop-bridge",
            source_path: "crates/gents-desktop-bridge/src/snapshot/tests/session_state.rs",
            module_path: "gents_desktop_bridge::snapshot::tests::session_state",
            function: "session_snapshot_binds_request_lifecycle_operator_ui_cases",
        },
        ConformanceConsumer::RustTest {
            id: "gents_desktop_bridge::snapshot::tests::session_state::session_snapshot_streaming_response_overlay_consumes_generated_transition_cases",
            package: "gents-desktop-bridge",
            source_path: "crates/gents-desktop-bridge/src/snapshot/tests/session_state.rs",
            module_path: "gents_desktop_bridge::snapshot::tests::session_state",
            function: "session_snapshot_streaming_response_overlay_consumes_generated_transition_cases",
        },
        ConformanceConsumer::RustTest {
            id: "gents_desktop_bridge::snapshot::tests::session_state::session_snapshot_transcript_rendering_consumes_generated_transcript_cases",
            package: "gents-desktop-bridge",
            source_path: "crates/gents-desktop-bridge/src/snapshot/tests/session_state.rs",
            module_path: "gents_desktop_bridge::snapshot::tests::session_state",
            function: "session_snapshot_transcript_rendering_consumes_generated_transcript_cases",
        },
        ConformanceConsumer::RustTest {
            id: "gents_desktop_bridge::snapshot::tests::session_timeline::structured_command_policy_denial_projects_to_rendered_tool",
            package: "gents-desktop-bridge",
            source_path: "crates/gents-desktop-bridge/src/snapshot/tests/session_timeline.rs",
            module_path: "gents_desktop_bridge::snapshot::tests::session_timeline",
            function: "structured_command_policy_denial_projects_to_rendered_tool",
        },
        ConformanceConsumer::RustTest {
            id: "gents_desktop_bridge::snapshot::tests::runtime::task_recent_runs_view_consumes_generated_trigger_dispatch_lineage_contract_cases",
            package: "gents-desktop-bridge",
            source_path: "crates/gents-desktop-bridge/src/snapshot/tests/runtime.rs",
            module_path: "gents_desktop_bridge::snapshot::tests::runtime",
            function: "task_recent_runs_view_consumes_generated_trigger_dispatch_lineage_contract_cases",
        },
        ConformanceConsumer::RustTest {
            id: "gents_desktop_bridge::snapshot::tests::subagent_lineage::subagent_tree_view_consumes_generated_r5_cross_deployment_contract_cases",
            package: "gents-desktop-bridge",
            source_path: "crates/gents-desktop-bridge/src/snapshot/tests/subagent_lineage.rs",
            module_path: "gents_desktop_bridge::snapshot::tests::subagent_lineage",
            function: "subagent_tree_view_consumes_generated_r5_cross_deployment_contract_cases",
        },
        ConformanceConsumer::RustTest {
            id: "gents_desktop_bridge::snapshot::tests::mcp_health::mcp_health_view_preserves_every_generated_lean_mcp_health_case_transition",
            package: "gents-desktop-bridge",
            source_path: "crates/gents-desktop-bridge/src/snapshot/tests/mcp_health.rs",
            module_path: "gents_desktop_bridge::snapshot::tests::mcp_health",
            function: "mcp_health_view_preserves_every_generated_lean_mcp_health_case_transition",
        },
        ConformanceConsumer::RustTest {
            id: "gents_desktop_bridge::snapshot::operations_snapshot::tests::project_filters_to_background_await_mode_only",
            package: "gents-desktop-bridge",
            source_path: "crates/gents-desktop-bridge/src/snapshot/operations_snapshot/tests.rs",
            module_path: "gents_desktop_bridge::snapshot::operations_snapshot::tests",
            function: "project_filters_to_background_await_mode_only",
        },
        ConformanceConsumer::RustTest {
            id: "hook::tests::generated_persistence_failure_policy_cases_match_hook_decisions",
            package: "gents",
            source_path: "crates/gents/src/hook/tests.rs",
            module_path: "hook::tests",
            function: "generated_persistence_failure_policy_cases_match_hook_decisions",
        },
        ConformanceConsumer::RustTest {
            id: "hook::tests::generated_storage_observation_cases_match_hook_runtime_classification",
            package: "gents",
            source_path: "crates/gents/src/hook/tests.rs",
            module_path: "hook::tests",
            function: "generated_storage_observation_cases_match_hook_runtime_classification",
        },
        ConformanceConsumer::RustTest {
            id: "identity::identity_structural_cases_match_lean_verdicts",
            package: "gents",
            source_path: "crates/gents/tests/conformance/identity.rs",
            module_path: "identity",
            function: "identity_structural_cases_match_lean_verdicts",
        },
        ConformanceConsumer::RustTest {
            id: "identity::identity_permission_cases_pin_runtime_permission_contract_shape",
            package: "gents",
            source_path: "crates/gents/tests/conformance/identity.rs",
            module_path: "identity",
            function: "identity_permission_cases_pin_runtime_permission_contract_shape",
        },
        ConformanceConsumer::RustTest {
            id: "http::identity_decide::tests::identity_decide_endpoint_matches_lean_permission_cases",
            package: "gents-cli",
            source_path: "crates/gents-cli/src/http/identity_decide.rs",
            module_path: "http::identity_decide::tests",
            function: "identity_decide_endpoint_matches_lean_permission_cases",
        },
        ConformanceConsumer::RustTest {
            id: "http::r5_dispatch::tests::subagent_dispatch_endpoint_matches_agent_request_parent_walk",
            package: "gents-cli",
            source_path: "crates/gents-cli/src/http/r5_dispatch.rs",
            module_path: "http::r5_dispatch::tests",
            function: "subagent_dispatch_endpoint_matches_agent_request_parent_walk",
        },
        ConformanceConsumer::RustTest {
            id: "identity::identity_respects_principal_contract_enforced_by_runtime_routing",
            package: "gents",
            source_path: "crates/gents/tests/conformance/identity.rs",
            module_path: "identity",
            function: "identity_respects_principal_contract_enforced_by_runtime_routing",
        },
        ConformanceConsumer::RustTest {
            id: "lifecycle::tests::request_state_machine_contract_is_complete",
            package: "gents",
            source_path: "crates/gents/src/lifecycle.rs",
            module_path: "lifecycle::tests",
            function: "request_state_machine_contract_is_complete",
        },
        ConformanceConsumer::RustTest {
            id: "lifecycle::tests::rust_execution_origin_vocabulary_matches_lean_model",
            package: "gents",
            source_path: "crates/gents/src/lifecycle.rs",
            module_path: "lifecycle::tests",
            function: "rust_execution_origin_vocabulary_matches_lean_model",
        },
        ConformanceConsumer::RustTest {
            id: "lifecycle::tests::rust_request_lifecycle_state_vocabulary_matches_lean_model",
            package: "gents",
            source_path: "crates/gents/src/lifecycle.rs",
            module_path: "lifecycle::tests",
            function: "rust_request_lifecycle_state_vocabulary_matches_lean_model",
        },
        ConformanceConsumer::RustTest {
            id: "live_overlay::live_overlay_cases_match_lean_table",
            package: "gents",
            source_path: "crates/gents/tests/conformance/live_overlay.rs",
            module_path: "live_overlay",
            function: "live_overlay_cases_match_lean_table",
        },
        ConformanceConsumer::RustTest {
            id: "mcp_pool::tests::tool_retry_disposition_contract_cases_match_mcp_pool_policy",
            package: "gents",
            source_path: "crates/gents/src/mcp_pool/tests.rs",
            module_path: "mcp_pool::tests",
            function: "tool_retry_disposition_contract_cases_match_mcp_pool_policy",
        },
        ConformanceConsumer::RustTest {
            id: "managed_exec::tests::rust_managed_exec_state_vocabulary_matches_lean_model",
            package: "gents",
            source_path: "crates/gents/src/managed_exec/tests.rs",
            module_path: "managed_exec::tests",
            function: "rust_managed_exec_state_vocabulary_matches_lean_model",
        },
        ConformanceConsumer::RustTest {
            id: "managed_exec::tests::managed_exec_state_machine_contract_is_complete",
            package: "gents",
            source_path: "crates/gents/src/managed_exec/tests.rs",
            module_path: "managed_exec::tests",
            function: "managed_exec_state_machine_contract_is_complete",
        },
        ConformanceConsumer::RustTest {
            id: "runtime_status::tests::runtime_status_generation_updates_match_lean_runtime_reconcile_cases",
            package: "gents",
            source_path: "crates/gents/src/runtime_status/tests.rs",
            module_path: "runtime_status::tests",
            function: "runtime_status_generation_updates_match_lean_runtime_reconcile_cases",
        },
        ConformanceConsumer::RustTest {
            id: "runtime_status::tests::generated_process_transition_cases_match_runtime_status_policy",
            package: "gents",
            source_path: "crates/gents/src/runtime_status/tests.rs",
            module_path: "runtime_status::tests",
            function: "generated_process_transition_cases_match_runtime_status_policy",
        },
        ConformanceConsumer::RustTest {
            id: "runtime_status::tests::rust_process_state_transitions_match_lean_contract",
            package: "gents",
            source_path: "crates/gents/src/runtime_status/tests.rs",
            module_path: "runtime_status::tests",
            function: "rust_process_state_transitions_match_lean_contract",
        },
        ConformanceConsumer::RustTest {
            id: "runtime_status::tests::rust_process_state_vocabulary_matches_lean_model",
            package: "gents",
            source_path: "crates/gents/src/runtime_status/tests.rs",
            module_path: "runtime_status::tests",
            function: "rust_process_state_vocabulary_matches_lean_model",
        },
        ConformanceConsumer::RustTest {
            id: "runtime_status::tests::rust_reconcile_phase_vocabulary_matches_lean_model",
            package: "gents",
            source_path: "crates/gents/src/runtime_status/tests.rs",
            module_path: "runtime_status::tests",
            function: "rust_reconcile_phase_vocabulary_matches_lean_model",
        },
        ConformanceConsumer::RustTest {
            id: "runtime_status::tests::runtime_reconcile_state_machine_contract_is_complete",
            package: "gents",
            source_path: "crates/gents/src/runtime_status/tests.rs",
            module_path: "runtime_status::tests",
            function: "runtime_reconcile_state_machine_contract_is_complete",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_session_recovery_cases_drive_db_backed_reissue_contract",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_session_recovery_cases_drive_db_backed_reissue_contract",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_request_transition_cases_cover_lifecycle_policy",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_request_transition_cases_cover_lifecycle_policy",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_queue_deadline_cases_pin_r4a_contract_rows",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_queue_deadline_cases_pin_r4a_contract_rows",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_recovery_sweep_cases_drive_startup_recovery_contract",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_recovery_sweep_cases_drive_startup_recovery_contract",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_recovery_outcome_cases_fence_duplicate_tolerant_counting",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_recovery_outcome_cases_fence_duplicate_tolerant_counting",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_restart_disposition_cases_drive_recover_all",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_restart_disposition_cases_drive_recover_all",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_read_tool_output_witness_drives_hook_dispatch",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_read_tool_output_witness_drives_hook_dispatch",
        },
        ConformanceConsumer::RustTest {
            id: "background_tools::tests::generated_tool_output_paging_cases_match_slice_function",
            package: "gents",
            source_path: "crates/gents/src/background_tools.rs",
            module_path: "background_tools::tests",
            function: "generated_tool_output_paging_cases_match_slice_function",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_bridge_step_cases_drive_bridge_lifecycle",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_bridge_step_cases_drive_bridge_lifecycle",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_recovery_equivalence_cases_pin_uninterrupted_convergence_contract",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_recovery_equivalence_cases_pin_uninterrupted_convergence_contract",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_r6_backgrounding_cases_drive_tool_backgrounding_contract",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_r6_backgrounding_cases_drive_tool_backgrounding_contract",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_r5_cross_deployment_cases_drive_production_dispatch",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_r5_cross_deployment_cases_drive_production_dispatch",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_composed_invariant_witnesses_drive_tool_lifecycle_conformance",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_composed_invariant_witnesses_drive_tool_lifecycle_conformance",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::cancel_propagation_cases_drive_production_interrupt",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "cancel_propagation_cases_drive_production_interrupt",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_r6_background_theorem_witnesses_drive_admission_budget_invariant",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_r6_background_theorem_witnesses_drive_admission_budget_invariant",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_r6_background_theorem_witnesses_drive_cascade_cancellation_trace",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_r6_background_theorem_witnesses_drive_cascade_cancellation_trace",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_subagent_delegation_graph_cases_pin_gap2_contract",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_subagent_delegation_graph_cases_pin_gap2_contract",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_r4c_background_work_cases_pin_observable_shapes",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_r4c_background_work_cases_pin_observable_shapes",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_codex_shim_projection_cases_pin_adapter_mapping",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_codex_shim_projection_cases_pin_adapter_mapping",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_codex_shim_binding_cases_pin_runnable_gated_binding",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_codex_shim_binding_cases_pin_runnable_gated_binding",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_startup_readiness_cases_pin_bounded_barrier_release",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_startup_readiness_cases_pin_bounded_barrier_release",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_transcript_cases_drive_agent_message_ordering_contract",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_transcript_cases_drive_agent_message_ordering_contract",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_streaming_response_cases_pin_lifecycle_contract",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_streaming_response_cases_pin_lifecycle_contract",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_streaming_response_interrupt_flow_cases_drive_daemon_contract",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_streaming_response_interrupt_flow_cases_drive_daemon_contract",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_compaction_reducer_cases_pin_contract",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_compaction_reducer_cases_pin_contract",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::prompt_assembly::generated_sanitize_cases_drive_the_production_sanitizer",
            package: "gents",
            source_path: "crates/gents/tests/conformance/prompt_assembly.rs",
            module_path: "conformance::prompt_assembly",
            function: "generated_sanitize_cases_drive_the_production_sanitizer",
        },
        ConformanceConsumer::RustTest {
            id: "agent::daemon::request::budget_contract_tests::generated_budget_cases_drive_dynamic_output_compaction_trigger",
            package: "gents",
            source_path: "crates/gents/src/agent/daemon/request.rs",
            module_path: "agent::daemon::request::budget_contract_tests",
            function: "generated_budget_cases_drive_dynamic_output_compaction_trigger",
        },
        ConformanceConsumer::RustTest {
            id: "agent::loop_stream::tests::generated_turn_budget_cases_drive_every_completion_dispatch",
            package: "gents",
            source_path: "crates/gents/src/agent/loop_stream/tests.rs",
            module_path: "agent::loop_stream::tests",
            function: "generated_turn_budget_cases_drive_every_completion_dispatch",
        },
        ConformanceConsumer::RustTest {
            id: "agent::loop_stream::tests::generated_rendered_capture_cases_fence_persist_before_send",
            package: "gents",
            source_path: "crates/gents/src/agent/loop_stream/tests.rs",
            module_path: "agent::loop_stream::tests",
            function: "generated_rendered_capture_cases_fence_persist_before_send",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::rendered_capture::generated_rendered_capture_key_cases_pin_the_capture_key_tuple",
            package: "gents",
            source_path: "crates/gents/tests/conformance/rendered_capture.rs",
            module_path: "conformance::rendered_capture",
            function: "generated_rendered_capture_key_cases_pin_the_capture_key_tuple",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::rendered_capture::generated_capture_scope_cases_pin_the_shared_parser_and_order",
            package: "gents",
            source_path: "crates/gents/tests/conformance/rendered_capture.rs",
            module_path: "conformance::rendered_capture",
            function: "generated_capture_scope_cases_pin_the_shared_parser_and_order",
        },
        ConformanceConsumer::RustTest {
            id: "cli_trace_export::trace_capture_fetches_metadata_with_field_commit_cid",
            package: "gents-cli",
            source_path: "crates/gents-cli/tests/cli_trace_export.rs",
            module_path: "cli_trace_export",
            function: "trace_capture_fetches_metadata_with_field_commit_cid",
        },
        ConformanceConsumer::TypeScriptTest {
            id: "apps/gents-desktop/tests/request-trace.test.tsx::request trace panel renders the reconstructed event stream",
            app: "gents-desktop",
            source_path: "apps/gents-desktop/tests/request-trace.test.tsx",
            suite: "request trace panel",
            test: "renders the reconstructed event stream",
        },
        ConformanceConsumer::RustTest {
            id: "agent::loop_stream::tests::generated_layer_cases_pin_the_assembled_request_order",
            package: "gents",
            source_path: "crates/gents/src/agent/loop_stream/tests.rs",
            module_path: "agent::loop_stream::tests",
            function: "generated_layer_cases_pin_the_assembled_request_order",
        },
        ConformanceConsumer::RustTest {
            id: "agent::loop_stream::tests::generated_repair_cases_drive_tool_argument_repair",
            package: "gents",
            source_path: "crates/gents/src/agent/loop_stream/tests.rs",
            module_path: "agent::loop_stream::tests",
            function: "generated_repair_cases_drive_tool_argument_repair",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::generated_tool_execution_cases_cover_preflight_and_retry_contracts",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "generated_tool_execution_cases_cover_preflight_and_retry_contracts",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::lean_executable_contracts_cover_initial_domains",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "lean_executable_contracts_cover_initial_domains",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::managed_exec_liveness_cases_pin_native_process_boundary",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "managed_exec_liveness_cases_pin_native_process_boundary",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::managed_exec_tool_boundary_cases_cover_every_native_subprocess_tool",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "managed_exec_tool_boundary_cases_cover_every_native_subprocess_tool",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::pairing_reconcile_shutdown_boundary_preempts_in_flight_sweep",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "pairing_reconcile_shutdown_boundary_preempts_in_flight_sweep",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::pairing_reconcile_top_level_sweep_failure_is_nonterminal_and_retried",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "pairing_reconcile_top_level_sweep_failure_is_nonterminal_and_retried",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::pairing_reconcile_sweep_does_not_head_of_line_block_ready_peer",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "pairing_reconcile_sweep_does_not_head_of_line_block_ready_peer",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::lean_emits_await_mode_vocabulary",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "lean_emits_await_mode_vocabulary",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::lean_emits_cancel_policy_vocabulary",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "lean_emits_cancel_policy_vocabulary",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::lean_emits_child_terminal_vocabulary_and_projections",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "lean_emits_child_terminal_vocabulary_and_projections",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::event_delivery_transition_cases_match_contract",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "event_delivery_transition_cases_match_contract",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::event_delivery_source_instances_match_runtime",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "event_delivery_source_instances_match_runtime",
        },
        ConformanceConsumer::RustTest {
            id: "conformance::event_delivery_convergence_traces_match_runtime_or_deviation",
            package: "gents",
            source_path: "crates/gents/tests/conformance.rs",
            module_path: "conformance",
            function: "event_delivery_convergence_traces_match_runtime_or_deviation",
        },
        ConformanceConsumer::RustTest {
            id: "toolset::tests::generated_command_env_cases_match_rust_filtering",
            package: "gents",
            source_path: "crates/gents/src/toolset/tests.rs",
            module_path: "toolset::tests",
            function: "generated_command_env_cases_match_rust_filtering",
        },
        ConformanceConsumer::RustTest {
            id: "toolset::tests::generated_command_policy_cases_match_rust_validation",
            package: "gents",
            source_path: "crates/gents/src/toolset/tests.rs",
            module_path: "toolset::tests",
            function: "generated_command_policy_cases_match_rust_validation",
        },
        ConformanceConsumer::RustTest {
            id: "toolset::tests::generated_command_sandbox_cases_match_rust_selection",
            package: "gents",
            source_path: "crates/gents/src/toolset/tests.rs",
            module_path: "toolset::tests",
            function: "generated_command_sandbox_cases_match_rust_selection",
        },
        ConformanceConsumer::RustTest {
            id: "toolset::tests::generated_native_filesystem_boundary_cases_match_preemptible_boundary_contract",
            package: "gents",
            source_path: "crates/gents/src/toolset/tests.rs",
            module_path: "toolset::tests",
            function: "generated_native_filesystem_boundary_cases_match_preemptible_boundary_contract",
        },
        ConformanceConsumer::RustTest {
            id: "trigger_engine::tests::trigger_engine_dispatch_matches_lean_generated_contract_cases",
            package: "gents",
            source_path: "crates/gents/src/trigger_engine/tests/mod.rs",
            module_path: "trigger_engine::tests",
            function: "trigger_engine_dispatch_matches_lean_generated_contract_cases",
        },
        ConformanceConsumer::RustTest {
            id: "tool_call_lifecycle::tests::rust_tool_call_state_vocabulary_matches_lean_model",
            package: "gents",
            source_path: "crates/gents/src/tool_call_lifecycle.rs",
            module_path: "tool_call_lifecycle::tests",
            function: "rust_tool_call_state_vocabulary_matches_lean_model",
        },
        ConformanceConsumer::RustTest {
            id: "tool_call_lifecycle::tests::rust_cancel_cause_vocabulary_matches_lean_model",
            package: "gents",
            source_path: "crates/gents/src/tool_call_lifecycle.rs",
            module_path: "tool_call_lifecycle::tests",
            function: "rust_cancel_cause_vocabulary_matches_lean_model",
        },
        ConformanceConsumer::RustTest {
            id: "tool_call_lifecycle::tests::rust_failure_class_vocabulary_matches_lean_model",
            package: "gents",
            source_path: "crates/gents/src/tool_call_lifecycle.rs",
            module_path: "tool_call_lifecycle::tests",
            function: "rust_failure_class_vocabulary_matches_lean_model",
        },
        ConformanceConsumer::RustTest {
            id: "tool_call_lifecycle::tests::tool_call_state_machine_contract_is_complete",
            package: "gents",
            source_path: "crates/gents/src/tool_call_lifecycle.rs",
            module_path: "tool_call_lifecycle::tests",
            function: "tool_call_state_machine_contract_is_complete",
        },
        ConformanceConsumer::RustTest {
            id: "health_checker::tests::generated_mcp_health_cases_match_health_checker_transitions",
            package: "gents",
            source_path: "crates/gents/src/health_checker.rs",
            module_path: "health_checker::tests",
            function: "generated_mcp_health_cases_match_health_checker_transitions",
        },
        ConformanceConsumer::RustTest {
            id: "gents_desktop_bridge::tests::operations_cascade::preview_returns_four_classified_groups_and_a_signature",
            package: "gents-desktop-bridge",
            source_path: "crates/gents-desktop-bridge/src/tests/operations_cascade.rs",
            module_path: "gents_desktop_bridge::tests::operations_cascade",
            function: "preview_returns_four_classified_groups_and_a_signature",
        },
        ConformanceConsumer::RustTest {
            id: "gents_desktop_bridge::tests::operations_interrupt::interrupt_request_cascade_returns_accepted_when_signature_matches",
            package: "gents-desktop-bridge",
            source_path: "crates/gents-desktop-bridge/src/tests/operations_interrupt.rs",
            module_path: "gents_desktop_bridge::tests::operations_interrupt",
            function: "interrupt_request_cascade_returns_accepted_when_signature_matches",
        },
        ConformanceConsumer::RustTest {
            id: "gents_desktop_bridge::snapshot::tests::session_state::session_snapshot_derives_cancel_cause_for_interrupted_response_and_cancelled_tool_call",
            package: "gents-desktop-bridge",
            source_path: "crates/gents-desktop-bridge/src/snapshot/tests/session_state.rs",
            module_path: "gents_desktop_bridge::snapshot::tests::session_state",
            function: "session_snapshot_derives_cancel_cause_for_interrupted_response_and_cancelled_tool_call",
        },
    ]
}

pub fn assert_registered_conformance_consumers_resolve() -> BTreeSet<&'static str> {
    let repo_root = repo_root();
    let mut sources = BTreeMap::new();
    let mut ids = BTreeSet::new();
    for consumer in registered_conformance_consumers() {
        let id = consumer.id();
        assert!(
            ids.insert(id),
            "duplicate registered conformance consumer id: {id}"
        );
        consumer.assert_resolves(&repo_root, &mut sources);
    }
    ids
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .find(|path| path.join("crates/gents/proofs/lakefile.lean").exists())
        .expect("repository root should contain crates/gents/proofs/lakefile.lean")
        .to_path_buf()
}

fn cached_source<'a>(
    repo_root: &Path,
    sources: &'a mut BTreeMap<&'static str, String>,
    source_path: &'static str,
) -> &'a str {
    if !sources.contains_key(source_path) {
        sources.insert(source_path, read_source(repo_root, source_path));
    }
    sources
        .get(source_path)
        .expect("source should be cached after insertion")
}

fn read_source(repo_root: &Path, source_path: &str) -> String {
    let path = repo_root.join(source_path);
    fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()))
}

fn assert_rust_test_function(
    source: &str,
    id: &str,
    package: &str,
    source_path: &str,
    module_path: &str,
    function: &str,
) {
    let expected_id = format!("{module_path}::{function}");
    assert!(
        id == expected_id,
        "registered Rust consumer {id:?} in package {package} must equal {expected_id:?}"
    );

    let needle = format!("fn {function}(");
    let matches = source.match_indices(&needle).collect::<Vec<_>>();
    assert_eq!(
        matches.len(),
        1,
        "registered Rust consumer {id:?} in package {package} must resolve to exactly one `{needle}` in {source_path}; found {}",
        matches.len()
    );

    let declaration_line_start = source[..matches[0].0]
        .rfind('\n')
        .map_or(0, |index| index + 1);
    let attrs = preceding_attribute_block(&source[..declaration_line_start]);
    assert!(
        attrs
            .iter()
            .any(|attr| attr.starts_with("#[test") || attr.starts_with("#[tokio::test")),
        "registered Rust consumer {id:?} in package {package} resolves to {source_path}::{function}, but that function is not marked #[test] or #[tokio::test]"
    );
}

fn assert_typescript_test(
    source: &str,
    id: &str,
    app: &str,
    source_path: &str,
    suite: &str,
    test: &str,
) {
    assert!(
        id.starts_with(source_path),
        "registered TypeScript consumer {id:?} for app {app} must start with source path {source_path}"
    );

    let suite_call = find_ts_call(source, &["describe"], suite, 0, source.len()).unwrap_or_else(|| {
        panic!(
            "registered TypeScript consumer {id:?} for app {app} must resolve suite {suite:?} in {source_path}"
        )
    });
    let suite_open = source[suite_call.literal_end..]
        .find('{')
        .map(|offset| suite_call.literal_end + offset)
        .unwrap_or_else(|| {
            panic!(
                "registered TypeScript consumer {id:?} for app {app} resolved suite {suite:?} in {source_path}, but the suite callback body was not found"
            )
        });
    let suite_close = matching_brace(source, suite_open).unwrap_or_else(|| {
        panic!(
            "registered TypeScript consumer {id:?} for app {app} resolved suite {suite:?} in {source_path}, but the suite callback body was not balanced"
        )
    });
    assert!(
        find_ts_call(source, &["test", "it"], test, suite_open, suite_close).is_some(),
        "registered TypeScript consumer {id:?} for app {app} must resolve test {test:?} inside suite {suite:?} in {source_path}"
    );
}

fn preceding_attribute_block(source_before_fn: &str) -> Vec<&str> {
    let mut attrs = Vec::new();
    for line in source_before_fn.lines().rev() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.starts_with("#[") {
            attrs.push(trimmed);
            continue;
        }
        break;
    }
    attrs
}

#[derive(Debug, Clone, Copy)]
struct TsCall {
    literal_end: usize,
}

fn find_ts_call(
    source: &str,
    callees: &[&str],
    first_arg: &str,
    start: usize,
    end: usize,
) -> Option<TsCall> {
    let mut offset = start;
    while offset < end {
        let remaining = &source[offset..end];
        let next = callees
            .iter()
            .filter_map(|callee| {
                remaining
                    .find(callee)
                    .map(|index| (offset + index, *callee))
            })
            .min_by_key(|(index, _)| *index)?;
        let call_start = next.0;
        let callee = next.1;
        offset = call_start + callee.len();
        if !is_identifier_boundary(source, call_start, callee.len()) {
            continue;
        }

        let Some(open_paren) = skip_ws(source, offset, end) else {
            continue;
        };
        if source.as_bytes().get(open_paren) != Some(&b'(') {
            continue;
        }
        let Some(arg_start) = skip_ws(source, open_paren + 1, end) else {
            continue;
        };
        let Some(literal_end) = quoted_literal_matches(source, arg_start, end, first_arg) else {
            continue;
        };
        return Some(TsCall { literal_end });
    }
    None
}

fn is_identifier_boundary(source: &str, start: usize, len: usize) -> bool {
    let before = start
        .checked_sub(1)
        .and_then(|index| source.as_bytes().get(index))
        .copied();
    let after = source.as_bytes().get(start + len).copied();
    !before.is_some_and(is_identifier_byte) && !after.is_some_and(is_identifier_byte)
}

fn is_identifier_byte(byte: u8) -> bool {
    byte == b'_' || byte == b'$' || byte.is_ascii_alphanumeric()
}

fn skip_ws(source: &str, start: usize, end: usize) -> Option<usize> {
    (start..end).find(|index| {
        source
            .as_bytes()
            .get(*index)
            .is_some_and(|byte| !byte.is_ascii_whitespace())
    })
}

fn quoted_literal_matches(source: &str, start: usize, end: usize, expected: &str) -> Option<usize> {
    let quote = *source.as_bytes().get(start)?;
    if !matches!(quote, b'\'' | b'"' | b'`') {
        return None;
    }

    let mut index = start + 1;
    let literal_start = index;
    while index < end {
        match source.as_bytes()[index] {
            b'\\' => index += 2,
            byte if byte == quote => {
                if &source[literal_start..index] == expected {
                    return Some(index + 1);
                }
                return None;
            }
            _ => index += 1,
        }
    }
    None
}

fn matching_brace(source: &str, open: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    if bytes.get(open) != Some(&b'{') {
        return None;
    }

    let mut depth = 1usize;
    let mut index = open + 1;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' | b'"' | b'`' => index = skip_quoted(source, index)?,
            b'/' if bytes.get(index + 1) == Some(&b'/') => index = skip_line_comment(bytes, index),
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index = skip_block_comment(bytes, index)?
            }
            b'{' => {
                depth += 1;
                index += 1;
            }
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(index);
                }
                index += 1;
            }
            _ => index += 1,
        }
    }
    None
}

fn skip_quoted(source: &str, start: usize) -> Option<usize> {
    let quote = source.as_bytes()[start];
    let mut index = start + 1;
    while index < source.len() {
        match source.as_bytes()[index] {
            b'\\' => index += 2,
            byte if byte == quote => return Some(index + 1),
            _ => index += 1,
        }
    }
    None
}

fn skip_line_comment(bytes: &[u8], start: usize) -> usize {
    bytes[start..]
        .iter()
        .position(|byte| *byte == b'\n')
        .map_or(bytes.len(), |offset| start + offset + 1)
}

fn skip_block_comment(bytes: &[u8], start: usize) -> Option<usize> {
    bytes[start + 2..]
        .windows(2)
        .position(|window| window == b"*/")
        .map(|offset| start + offset + 4)
}
