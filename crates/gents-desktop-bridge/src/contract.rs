//! Bridge contract fingerprint: command inventory, permission sets, events,
//! error codes, and version. Phase 2 wires the snapshot; phase 3 enforces
//! permission projection and typed errors against it.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::error::BridgeErrorCode;

/// `MAJOR.MINOR` contract version. MINOR = additive; MAJOR = breaking.
// 1.0: breaking — clients submit requests; desktop session-fork projection removed.
// 0.8: additive — managed-server tray event inventory.
// 0.7: additive — retry eligibility projection and agent-scoped conversation rename.
// 0.6: additive — predecessor-aware desktop_request_retry command.
// 0.5: additive — inference onboarding (probe endpoint, Codex login/cancel in
// config-write) merged from main (#871); desktop://codex-login-url event.
// 0.4: additive — Pairing error code; fingerprint set inventory aligned with
// grantable [[set]] entries + default (core/client-lifecycle).
// 0.3: BridgeError on command Err paths; SnapshotGrants projection; native-e2e.
// 0.2: desktop_bridge_contract, desktop_peer_probe_address; peer_status by id.
pub const CONTRACT_VERSION: &str = "1.0";

/// Package version string shared with workspace release train.
pub const PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(rename_all = "camelCase")]
pub struct BridgeContract {
    pub contract_version: String,
    pub package_version: String,
    pub events: Vec<String>,
    pub event_reasons: Vec<String>,
    pub error_codes: Vec<String>,
    pub commands: Vec<CommandContract>,
    pub permission_sets: Vec<PermissionSetContract>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(rename_all = "camelCase")]
pub struct CommandContract {
    pub name: String,
    pub permission_set: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(rename_all = "camelCase")]
pub struct PermissionSetContract {
    pub name: String,
    /// `"read"` or `"mutate"` — never mixed within one set.
    pub kind: String,
}

/// Production event name emitted by the update pump.
pub const CLIENT_UPDATED_EVENT: &str = "desktop://client-updated";

/// One-shot auth URL emission during the guided Codex login flow.
pub const CODEX_LOGIN_URL_EVENT: &str = "desktop://codex-login-url";
pub const MANAGED_SERVER_UPDATED_EVENT: &str = "desktop://managed-server-updated";
pub const MANAGED_SERVER_TRAY_STOP_EVENT: &str = "desktop://managed-server-tray-stop";

/// One-shot auth URL emission during the guided Grok login flow.
pub const GROK_LOGIN_URL_EVENT: &str = "desktop://grok-login-url";

/// Coarse ping reasons on `desktop://client-updated`.
pub const EVENT_REASONS: &[&str] = &["store", "health", "lifecycle", "config"];

/// Provisional command → permission-set map from the design table.
/// Phase 3 finalizes assignment under the no-read/mutate-mixing rule.
pub fn command_inventory() -> Vec<CommandContract> {
    let entries: &[(&str, &str)] = &[
        // core
        ("desktop_bridge_contract", "core"),
        ("desktop_bootstrap_summary", "core"),
        ("desktop_client_snapshot", "core"),
        ("desktop_observer_metrics", "core"),
        // client-lifecycle
        ("desktop_client_start", "client-lifecycle"),
        ("desktop_client_shutdown", "client-lifecycle"),
        ("desktop_set_selected_agent", "client-lifecycle"),
        // runtime-admin
        ("desktop_init_local_standard", "runtime-admin"),
        ("desktop_managed_server_status", "runtime-admin"),
        ("desktop_managed_server_start", "runtime-admin"),
        ("desktop_managed_server_stop", "runtime-admin"),
        // session-read
        ("desktop_session_snapshot", "session-read"),
        // trace-read
        ("desktop_request_timeline", "trace-read"),
        // tool-surface-read
        ("desktop_tool_surface_explain", "tool-surface-read"),
        // chat-write
        ("desktop_chat_send", "chat-write"),
        ("desktop_conversation_rename", "chat-write"),
        // resend-control
        ("desktop_request_resend", "resend-control"),
        ("desktop_request_retry", "resend-control"),
        // fleet-read
        ("desktop_peer_status_fetch", "fleet-read"),
        ("desktop_network_status", "fleet-read"),
        // workspace-read
        ("desktop_workspace_list", "workspace-read"),
        // fleet-admin
        ("desktop_peer_add", "fleet-admin"),
        ("desktop_peer_pair_bearer", "fleet-admin"),
        ("desktop_peer_remove", "fleet-admin"),
        ("desktop_peer_rename", "fleet-admin"),
        ("desktop_peer_probe_address", "fleet-admin"),
        ("desktop_p2p_repair", "fleet-admin"),
        // operations-read
        ("desktop_operations_snapshot", "operations-read"),
        ("desktop_list_subagent_tree", "operations-read"),
        ("desktop_list_backends_with_health", "operations-read"),
        ("desktop_list_mcp_services_with_health", "operations-read"),
        ("desktop_probe_mcp_service", "operations-read"),
        // interrupt-read / interrupt-control
        ("desktop_preview_interrupt_cascade", "interrupt-read"),
        ("desktop_interrupt_request", "interrupt-control"),
        // holds-read / holds-control
        ("desktop_list_tool_call_holds", "holds-read"),
        ("desktop_resolve_tool_call_hold", "holds-control"),
        // config-write (save/delete/test/auth — 20 commands)
        ("desktop_agent_config_save", "config-write"),
        ("desktop_behavior_save", "config-write"),
        ("desktop_skill_save", "config-write"),
        ("desktop_skill_delete", "config-write"),
        ("desktop_task_delete", "config-write"),
        ("desktop_schedule_delete", "config-write"),
        ("desktop_event_trigger_delete", "config-write"),
        ("desktop_backend_delete", "config-write"),
        ("desktop_inference_profile_delete", "config-write"),
        ("desktop_tool_selection_delete", "config-write"),
        ("desktop_tool_service_delete", "config-write"),
        ("desktop_behavior_delete", "config-write"),
        ("desktop_backend_save", "config-write"),
        ("desktop_inference_profile_save", "config-write"),
        ("desktop_tool_selection_save", "config-write"),
        ("desktop_tool_service_save", "config-write"),
        ("desktop_tool_service_test", "config-write"),
        ("desktop_probe_inference_endpoint", "config-write"),
        ("desktop_codex_login", "config-write"),
        ("desktop_codex_login_cancel", "config-write"),
        ("desktop_grok_login", "config-write"),
        ("desktop_grok_login_cancel", "config-write"),
        ("desktop_provider_accounts_list", "provider-accounts-read"),
        ("desktop_provider_account_disconnect", "config-write"),
        // tasks
        ("desktop_task_save", "tasks"),
        ("desktop_schedule_save", "tasks"),
        ("desktop_schedule_run", "tasks"),
        ("desktop_event_trigger_save", "tasks"),
        ("desktop_task_run", "tasks"),
        // native-e2e
        ("desktop_native_e2e_config", "native-e2e"),
        ("desktop_native_e2e_status", "native-e2e"),
    ];
    entries
        .iter()
        .map(|(name, set)| CommandContract {
            name: (*name).to_string(),
            permission_set: (*set).to_string(),
        })
        .collect()
}

/// Grantable permission sets aligned with `permissions/default.toml` +
/// `permissions/sets.toml`. Labels:
/// - `read` / `mutate`: fine-grained sets that must not mix categories
/// - `bundle`: composed defaults (default) or E2E-only bundles that may
///   intentionally span command classes (native-e2e)
pub fn permission_set_inventory() -> Vec<PermissionSetContract> {
    [
        ("default", "bundle"),
        ("core", "read"),
        ("client-lifecycle", "mutate"),
        ("runtime-admin", "mutate"),
        ("session-read", "read"),
        ("trace-read", "read"),
        ("tool-surface-read", "read"),
        ("chat-write", "mutate"),
        ("resend-control", "mutate"),
        ("fleet-read", "read"),
        ("workspace-read", "read"),
        ("fleet-admin", "mutate"),
        ("operations-read", "read"),
        ("interrupt-read", "read"),
        ("interrupt-control", "mutate"),
        ("holds-read", "read"),
        ("holds-control", "mutate"),
        // Projection section only in v1 (no dedicated IPC allow-* commands).
        ("config-read", "read"),
        ("config-write", "mutate"),
        ("provider-accounts-read", "read"),
        ("tasks", "mutate"),
        ("native-e2e", "bundle"),
        ("full", "bundle"),
    ]
    .into_iter()
    .map(|(name, kind)| PermissionSetContract {
        name: name.to_string(),
        kind: kind.to_string(),
    })
    .collect()
}

pub fn error_code_inventory() -> Vec<String> {
    [
        BridgeErrorCode::ClientNotRunning,
        BridgeErrorCode::ClientStartFailed,
        BridgeErrorCode::NotFound,
        BridgeErrorCode::InvalidArgument,
        BridgeErrorCode::Unsupported,
        BridgeErrorCode::EndpointUnreachable,
        BridgeErrorCode::StalePreview,
        BridgeErrorCode::CascadeDepthExceeded,
        BridgeErrorCode::PathEscapesRoot,
        BridgeErrorCode::Backend,
        BridgeErrorCode::Pairing,
        BridgeErrorCode::Unknown,
    ]
    .into_iter()
    .map(|code| code.as_str().to_string())
    .collect()
}

pub fn current_contract() -> BridgeContract {
    BridgeContract {
        contract_version: CONTRACT_VERSION.to_string(),
        package_version: PACKAGE_VERSION.to_string(),
        events: vec![
            CLIENT_UPDATED_EVENT.to_string(),
            CODEX_LOGIN_URL_EVENT.to_string(),
            GROK_LOGIN_URL_EVENT.to_string(),
            MANAGED_SERVER_UPDATED_EVENT.to_string(),
            MANAGED_SERVER_TRAY_STOP_EVENT.to_string(),
        ],
        event_reasons: EVENT_REASONS.iter().map(|s| (*s).to_string()).collect(),
        error_codes: error_code_inventory(),
        commands: command_inventory(),
        permission_sets: permission_set_inventory(),
    }
}

/// Pretty-printed fingerprint JSON (stable key order via serde_json::Value sort).
pub fn fingerprint_json() -> String {
    let value = serde_json::to_value(current_contract()).expect("contract serializes");
    let sorted = sort_json(value);
    let mut out = serde_json::to_string_pretty(&sorted).expect("pretty json");
    out.push('\n');
    out
}

fn sort_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<_> = map.keys().cloned().collect();
            keys.sort();
            let mut out = serde_json::Map::new();
            for key in keys {
                out.insert(key.clone(), sort_json(map.get(&key).unwrap().clone()));
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(sort_json).collect())
        }
        other => other,
    }
}

/// Path to the committed fingerprint relative to the workspace root.
pub const FINGERPRINT_REL_PATH: &str = "contracts/desktop-bridge.json";

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::PathBuf;

    #[derive(Debug, Deserialize)]
    struct PermissionSetFile {
        set: Vec<PermissionSetDefinition>,
    }

    #[derive(Debug, Deserialize)]
    struct DefaultPermissionFile {
        default: PermissionSetDefinition,
    }

    #[derive(Debug, Deserialize)]
    struct PermissionSetDefinition {
        identifier: Option<String>,
        permissions: Vec<String>,
    }

    fn inventory_by_name() -> BTreeMap<String, String> {
        command_inventory()
            .into_iter()
            .map(|command| (command.name, command.permission_set))
            .collect()
    }

    fn build_script_commands() -> BTreeSet<String> {
        let source = include_str!("../build.rs");
        let block = source
            .split_once("const COMMANDS: &[&str] = &[")
            .expect("build.rs COMMANDS declaration")
            .1
            .split_once("];")
            .expect("build.rs COMMANDS terminator")
            .0;
        block
            .lines()
            .filter_map(|line| {
                line.trim()
                    .strip_prefix('"')
                    .and_then(|line| line.strip_suffix("\","))
                    .map(str::to_string)
            })
            .collect()
    }

    fn plugin_handler_commands() -> BTreeSet<String> {
        let source = include_str!("plugin.rs");
        let block = source
            .split_once("tauri::generate_handler![")
            .expect("plugin generate_handler declaration")
            .1
            .split_once("])")
            .expect("plugin generate_handler terminator")
            .0;
        block
            .lines()
            .filter_map(|line| {
                let path = line.trim().trim_end_matches(',');
                path.contains("::")
                    .then(|| path.rsplit("::").next().expect("command path").to_string())
            })
            .collect()
    }

    fn permission_to_command(permission: &str) -> Option<String> {
        permission
            .strip_prefix("allow-")
            .map(|name| name.replace('-', "_"))
    }

    #[test]
    fn command_contract_matches_plugin_build_and_permission_sets() {
        let inventory = inventory_by_name();
        let inventory_names = inventory.keys().cloned().collect::<BTreeSet<_>>();
        assert_eq!(
            plugin_handler_commands(),
            inventory_names,
            "plugin generate_handler! and contract command inventory drifted"
        );
        assert_eq!(
            build_script_commands(),
            inventory_names,
            "build.rs COMMANDS and contract command inventory drifted"
        );

        let permission_file: PermissionSetFile =
            toml::from_str(include_str!("../permissions/sets.toml"))
                .expect("permissions/sets.toml parses");
        let actual_set_ids = permission_file
            .set
            .iter()
            .filter_map(|set| set.identifier.clone())
            .collect::<BTreeSet<_>>();
        let advertised_set_ids = permission_set_inventory()
            .into_iter()
            .map(|set| set.name)
            .filter(|name| name != "default" && name != "config-read")
            .collect::<BTreeSet<_>>();
        assert_eq!(
            actual_set_ids, advertised_set_ids,
            "permissions/sets.toml and advertised permission-set inventory drifted"
        );

        for set in permission_file
            .set
            .iter()
            .filter(|set| set.identifier.as_deref() != Some("full"))
        {
            let set_id = set.identifier.as_deref().expect("set identifier");
            let actual_commands = set
                .permissions
                .iter()
                .map(|permission| {
                    permission_to_command(permission).unwrap_or_else(|| {
                        panic!("{set_id} contains non-command permission {permission}")
                    })
                })
                .collect::<BTreeSet<_>>();
            let expected_commands = inventory
                .iter()
                .filter(|(_, permission_set)| permission_set.as_str() == set_id)
                .map(|(command, _)| command.clone())
                .collect::<BTreeSet<_>>();
            assert_eq!(
                actual_commands, expected_commands,
                "permission set {set_id} and contract command assignments drifted"
            );
        }

        let default_file: DefaultPermissionFile =
            toml::from_str(include_str!("../permissions/default.toml"))
                .expect("permissions/default.toml parses");
        let default_commands = default_file
            .default
            .permissions
            .iter()
            .map(|permission| {
                permission_to_command(permission)
                    .unwrap_or_else(|| panic!("default contains non-command {permission}"))
            })
            .collect::<BTreeSet<_>>();
        let expected_default_commands = inventory
            .iter()
            .filter(|(_, set)| set.as_str() == "core" || set.as_str() == "client-lifecycle")
            .map(|(command, _)| command.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            default_commands, expected_default_commands,
            "permissions/default.toml must compose exactly core + client-lifecycle"
        );

        let full = permission_file
            .set
            .iter()
            .find(|set| set.identifier.as_deref() == Some("full"))
            .expect("permissions/sets.toml full bundle");
        let full_references = full.permissions.iter().cloned().collect::<BTreeSet<_>>();
        let mut expected_full_references = actual_set_ids
            .iter()
            .filter(|set_id| {
                !matches!(
                    set_id.as_str(),
                    "full" | "native-e2e" | "core" | "client-lifecycle"
                )
            })
            .cloned()
            .collect::<BTreeSet<_>>();
        expected_full_references.insert("default".to_string());
        assert_eq!(
            full_references, expected_full_references,
            "production full bundle must compose default plus every production set exactly once"
        );

        let sets_by_id = permission_file
            .set
            .iter()
            .filter_map(|set| {
                set.identifier
                    .as_ref()
                    .map(|identifier| (identifier.as_str(), set))
            })
            .collect::<BTreeMap<_, _>>();
        let mut expanded_full_commands = BTreeSet::new();
        for reference in &full.permissions {
            let permissions = if reference == "default" {
                &default_file.default.permissions
            } else {
                &sets_by_id
                    .get(reference.as_str())
                    .unwrap_or_else(|| panic!("full references unknown set {reference}"))
                    .permissions
            };
            for permission in permissions {
                expanded_full_commands.insert(permission_to_command(permission).unwrap_or_else(
                    || panic!("full member {reference} contains non-command {permission}"),
                ));
            }
        }
        let expected_production_commands = inventory
            .iter()
            .filter(|(_, set)| set.as_str() != "native-e2e")
            .map(|(command, _)| command.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            expanded_full_commands, expected_production_commands,
            "production full bundle must expand to every production command and exclude native-e2e"
        );
    }

    #[test]
    fn no_fine_grained_permission_set_mixes_read_and_mutate() {
        // Fine-grained sets (read/mutate) must be pure. Bundle sets (default,
        // full, native-e2e) intentionally compose mixed surfaces.
        let mut kinds = std::collections::BTreeMap::<String, String>::new();
        for set in permission_set_inventory() {
            if let Some(existing) = kinds.insert(set.name.clone(), set.kind.clone()) {
                assert_eq!(existing, set.kind, "set {} has mixed kinds", set.name);
            }
        }
        // Independent per-command IO/privilege classification. Privileged
        // active actions (address probe, service test, repair) classify as
        // mutate deliberately: set purity tracks privilege, not just IO.
        const COMMAND_KINDS: &[(&str, &str)] = &[
            ("desktop_bridge_contract", "read"),
            ("desktop_bootstrap_summary", "read"),
            ("desktop_client_snapshot", "read"),
            ("desktop_observer_metrics", "read"),
            ("desktop_client_start", "mutate"),
            ("desktop_client_shutdown", "mutate"),
            ("desktop_set_selected_agent", "mutate"),
            ("desktop_init_local_standard", "mutate"),
            ("desktop_managed_server_status", "mutate"),
            ("desktop_managed_server_start", "mutate"),
            ("desktop_managed_server_stop", "mutate"),
            ("desktop_session_snapshot", "read"),
            ("desktop_request_timeline", "read"),
            ("desktop_tool_surface_explain", "read"),
            ("desktop_chat_send", "mutate"),
            ("desktop_conversation_rename", "mutate"),
            ("desktop_request_resend", "mutate"),
            ("desktop_request_retry", "mutate"),
            ("desktop_peer_status_fetch", "read"),
            ("desktop_network_status", "read"),
            ("desktop_workspace_list", "read"),
            ("desktop_peer_add", "mutate"),
            ("desktop_peer_pair_bearer", "mutate"),
            ("desktop_peer_remove", "mutate"),
            ("desktop_peer_rename", "mutate"),
            ("desktop_peer_probe_address", "mutate"),
            ("desktop_p2p_repair", "mutate"),
            ("desktop_operations_snapshot", "read"),
            ("desktop_list_subagent_tree", "read"),
            ("desktop_list_backends_with_health", "read"),
            ("desktop_list_mcp_services_with_health", "read"),
            ("desktop_probe_mcp_service", "read"),
            ("desktop_preview_interrupt_cascade", "read"),
            ("desktop_interrupt_request", "mutate"),
            ("desktop_list_tool_call_holds", "read"),
            ("desktop_resolve_tool_call_hold", "mutate"),
            ("desktop_agent_config_save", "mutate"),
            ("desktop_behavior_save", "mutate"),
            ("desktop_skill_save", "mutate"),
            ("desktop_skill_delete", "mutate"),
            ("desktop_task_delete", "mutate"),
            ("desktop_schedule_delete", "mutate"),
            ("desktop_event_trigger_delete", "mutate"),
            ("desktop_backend_delete", "mutate"),
            ("desktop_inference_profile_delete", "mutate"),
            ("desktop_tool_selection_delete", "mutate"),
            ("desktop_tool_service_delete", "mutate"),
            ("desktop_behavior_delete", "mutate"),
            ("desktop_backend_save", "mutate"),
            ("desktop_inference_profile_save", "mutate"),
            ("desktop_tool_selection_save", "mutate"),
            ("desktop_tool_service_save", "mutate"),
            ("desktop_tool_service_test", "mutate"),
            ("desktop_probe_inference_endpoint", "mutate"),
            ("desktop_codex_login", "mutate"),
            ("desktop_codex_login_cancel", "mutate"),
            ("desktop_grok_login", "mutate"),
            ("desktop_grok_login_cancel", "mutate"),
            ("desktop_provider_accounts_list", "read"),
            ("desktop_provider_account_disconnect", "mutate"),
            ("desktop_task_save", "mutate"),
            ("desktop_schedule_save", "mutate"),
            ("desktop_schedule_run", "mutate"),
            ("desktop_event_trigger_save", "mutate"),
            ("desktop_task_run", "mutate"),
            ("desktop_native_e2e_config", "read"),
            ("desktop_native_e2e_status", "mutate"),
        ];
        let kind_by_command: std::collections::BTreeMap<&str, &str> =
            COMMAND_KINDS.iter().copied().collect();
        assert_eq!(
            kind_by_command.len(),
            command_inventory().len(),
            "COMMAND_KINDS must classify every command exactly once"
        );
        for command in command_inventory() {
            let command_kind = *kind_by_command
                .get(command.name.as_str())
                .unwrap_or_else(|| panic!("command {} is unclassified", command.name));
            let set_kind = kinds[&command.permission_set].as_str();
            if set_kind == "read" || set_kind == "mutate" {
                assert_eq!(
                    command_kind, set_kind,
                    "command {} is {} but sits in {} set {}",
                    command.name, command_kind, set_kind, command.permission_set
                );
            }
        }
        // Every command maps to a fine-grained set or the native-e2e test set.
        let allowed_command_sets: std::collections::BTreeSet<_> = permission_set_inventory()
            .into_iter()
            .filter(|s| s.kind == "read" || s.kind == "mutate" || s.name == "native-e2e")
            .map(|s| s.name)
            .collect();
        for command in command_inventory() {
            assert!(
                allowed_command_sets.contains(&command.permission_set),
                "command {} references unknown/non-grantable set {}",
                command.name,
                command.permission_set
            );
            if command.permission_set == "native-e2e" {
                assert!(
                    command.name.contains("native_e2e"),
                    "native-e2e set must only hold e2e commands, got {}",
                    command.name
                );
            }
        }
    }

    #[test]
    fn command_inventory_is_unique() {
        let mut seen = BTreeSet::new();
        for command in command_inventory() {
            assert!(
                seen.insert(command.name.clone()),
                "duplicate command {}",
                command.name
            );
        }
    }

    #[test]
    fn fingerprint_matches_committed_snapshot() {
        let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("workspace root");
        let path = workspace_root.join(FINGERPRINT_REL_PATH);
        let expected = fingerprint_json();
        let actual = std::fs::read_to_string(&path).unwrap_or_else(|error| {
            panic!(
                "missing committed fingerprint at {}: {error}\n\nWrite it with:\n  cargo test -p gents-desktop-bridge write_fingerprint -- --ignored\n",
                path.display()
            )
        });
        assert_eq!(
            actual, expected,
            "desktop bridge contract fingerprint drifted.\n\
             If the change is intentional, regenerate with:\n\
             cargo test -p gents-desktop-bridge write_fingerprint -- --ignored\n\
             and bump contract_version (MINOR additive / MAJOR breaking)."
        );
    }

    #[test]
    #[ignore = "run explicitly to regenerate contracts/desktop-bridge.json"]
    fn write_fingerprint() {
        let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("workspace root");
        let path = workspace_root.join(FINGERPRINT_REL_PATH);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create contracts dir");
        }
        std::fs::write(&path, fingerprint_json()).expect("write fingerprint");
        eprintln!("wrote {}", path.display());
    }
}
