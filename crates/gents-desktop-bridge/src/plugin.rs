use tauri::plugin::{Builder, TauriPlugin};
use tauri::{Manager, Runtime};

use crate::config::BridgeConfig;
use crate::state::{resolve_policy, DesktopAppState};
use crate::tauri_commands;

pub fn init<R: Runtime>(config: BridgeConfig) -> TauriPlugin<R> {
    Builder::<R>::new("gents-desktop-bridge")
        .setup(move |app, _api| {
            let app_data_dir = app.path().app_data_dir().ok();
            let policy = resolve_policy(&config, app_data_dir)
                .map_err(|error| Box::<dyn std::error::Error>::from(error))?;
            tracing::info!(
                app = %policy.app_meta.app_name,
                version = %policy.app_meta.app_version,
                home = %policy.desktop_paths.root().display(),
                "gents-desktop-bridge initialized"
            );
            app.manage(DesktopAppState::new(policy));
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            tauri_commands::lifecycle::desktop_bridge_contract,
            tauri_commands::lifecycle::desktop_bootstrap_summary,
            tauri_commands::e2e::desktop_native_e2e_config,
            tauri_commands::e2e::desktop_native_e2e_status,
            tauri_commands::lifecycle::desktop_init_local_standard,
            tauri_commands::lifecycle::desktop_client_start,
            tauri_commands::lifecycle::desktop_client_shutdown,
            tauri_commands::managed_server::desktop_managed_server_status,
            tauri_commands::managed_server::desktop_managed_server_start,
            tauri_commands::managed_server::desktop_managed_server_stop,
            tauri_commands::peers::desktop_peer_add,
            tauri_commands::peers::desktop_peer_pair_bearer,
            tauri_commands::peers::desktop_peer_remove,
            tauri_commands::peers::desktop_peer_rename,
            tauri_commands::peers::desktop_peer_status_fetch,
            tauri_commands::peers::desktop_peer_probe_address,
            tauri_commands::peers::desktop_p2p_repair,
            tauri_commands::workspace::desktop_workspace_list,
            tauri_commands::peers::desktop_network_status,
            tauri_commands::lifecycle::desktop_client_snapshot,
            tauri_commands::lifecycle::desktop_set_selected_agent,
            tauri_commands::lifecycle::desktop_observer_metrics,
            tauri_commands::chat::desktop_session_snapshot,
            tauri_commands::chat::desktop_request_timeline,
            tauri_commands::tools_explain::desktop_tool_surface_explain,
            tauri_commands::chat::desktop_chat_send,
            tauri_commands::chat::desktop_conversation_rename,
            tauri_commands::chat::desktop_request_resend,
            tauri_commands::chat::desktop_request_retry,
            tauri_commands::config::desktop_agent_config_save,
            tauri_commands::config::desktop_behavior_save,
            tauri_commands::config::desktop_skill_save,
            tauri_commands::config::desktop_skill_delete,
            tauri_commands::config::desktop_task_delete,
            tauri_commands::config::desktop_schedule_delete,
            tauri_commands::config::desktop_event_trigger_delete,
            tauri_commands::config::desktop_backend_delete,
            tauri_commands::config::desktop_inference_profile_delete,
            tauri_commands::config::desktop_tool_selection_delete,
            tauri_commands::config::desktop_tool_service_delete,
            tauri_commands::config::desktop_behavior_delete,
            tauri_commands::config::desktop_backend_save,
            tauri_commands::config::desktop_inference_profile_save,
            tauri_commands::config::desktop_tool_selection_save,
            tauri_commands::config::desktop_tool_service_save,
            tauri_commands::config::desktop_tool_service_test,
            tauri_commands::inference_setup::desktop_probe_inference_endpoint,
            tauri_commands::inference_setup::desktop_codex_login,
            tauri_commands::inference_setup::desktop_codex_login_cancel,
            tauri_commands::inference_setup::desktop_grok_login,
            tauri_commands::inference_setup::desktop_grok_login_cancel,
            tauri_commands::inference_setup::desktop_provider_accounts_list,
            tauri_commands::inference_setup::desktop_provider_account_disconnect,
            tauri_commands::tasks::desktop_task_save,
            tauri_commands::tasks::desktop_schedule_save,
            tauri_commands::tasks::desktop_schedule_run,
            tauri_commands::tasks::desktop_event_trigger_save,
            tauri_commands::tasks::desktop_task_run,
            tauri_commands::operations::desktop_operations_snapshot,
            tauri_commands::operations::desktop_list_subagent_tree,
            tauri_commands::operations::desktop_preview_interrupt_cascade,
            tauri_commands::operations::desktop_interrupt_request,
            tauri_commands::operations::desktop_list_backends_with_health,
            tauri_commands::operations::desktop_list_mcp_services_with_health,
            tauri_commands::operations::desktop_probe_mcp_service,
            tauri_commands::operations::desktop_list_tool_call_holds,
            tauri_commands::operations::desktop_resolve_tool_call_hold
        ])
        .build()
}
