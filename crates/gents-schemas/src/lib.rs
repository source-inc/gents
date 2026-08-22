//! Static GraphQL schema strings for gents agent collections.
//!
//! This crate is intentionally dependency-free so external document-peer
//! consumers can depend on the agent collection contract without also pulling
//! in the runtime, protocol, Codex, or DefraDB dependency graph.

pub const AGENT_PRINCIPAL_NAME: &str = "AgentPrincipal";
pub const AGENT_PRINCIPAL: &str = include_str!("../schemas/agent/agent_principal.graphql");
pub const AGENT_BEHAVIOR_NAME: &str = "AgentBehavior";
pub const AGENT_BEHAVIOR: &str = include_str!("../schemas/agent/agent_behavior.graphql");
pub const AGENT_RUNTIME_NAME: &str = "AgentRuntime";
pub const AGENT_RUNTIME: &str = include_str!("../schemas/agent/agent_runtime.graphql");
pub const AGENT_DIRECTORY_ENTRY_NAME: &str = "AgentDirectoryEntry";
pub const AGENT_DIRECTORY_ENTRY: &str =
    include_str!("../schemas/agent/agent_directory_entry.graphql");
pub const AGENT_MEMORY_NAME: &str = "AgentMemory";
pub const AGENT_MEMORY: &str = include_str!("../schemas/agent/agent_memory.graphql");
pub const AGENT_CONVERSATION_NAME: &str = "AgentConversation";
pub const AGENT_CONVERSATION: &str = include_str!("../schemas/agent/agent_conversation.graphql");
pub const AGENT_REQUEST_NAME: &str = "AgentRequest";
pub const AGENT_REQUEST: &str = include_str!("../schemas/agent/agent_request.graphql");
pub const AGENT_RESPONSE_NAME: &str = "AgentResponse";
pub const AGENT_RESPONSE: &str = include_str!("../schemas/agent/agent_response.graphql");
pub const AGENT_MESSAGE_NAME: &str = "AgentMessage";
pub const AGENT_MESSAGE: &str = include_str!("../schemas/agent/agent_message.graphql");
pub const AGENT_SESSION_NAME: &str = "AgentSession";
pub const AGENT_SESSION: &str = include_str!("../schemas/agent/agent_session.graphql");
pub const GOAL_NAME: &str = "Goal";
pub const GOAL: &str = include_str!("../schemas/agent/goal.graphql");
pub const AGENT_TOOL_CALL_NAME: &str = "AgentToolCall";
pub const AGENT_TOOL_CALL: &str = include_str!("../schemas/agent/agent_tool_call.graphql");
pub const AGENT_TOOL_APPROVAL_NAME: &str = "AgentToolApproval";
pub const AGENT_TOOL_APPROVAL: &str = include_str!("../schemas/agent/agent_tool_approval.graphql");
pub const AGENT_TOOL_RESULT_NAME: &str = "AgentToolResult";
pub const AGENT_TOOL_RESULT: &str = include_str!("../schemas/agent/agent_tool_result.graphql");
pub const COMPACTION_ENTRY_NAME: &str = "CompactionEntry";
pub const COMPACTION_ENTRY: &str = include_str!("../schemas/agent/compaction_entry.graphql");
pub const RENDERED_REQUEST_NAME: &str = "RenderedRequest";
pub const RENDERED_REQUEST: &str = include_str!("../schemas/agent/rendered_request.graphql");
pub const PROVIDER_CONTEXT_REDUCTION_NAME: &str = "ProviderContextReduction";
pub const PROVIDER_CONTEXT_REDUCTION: &str =
    include_str!("../schemas/agent/provider_context_reduction.graphql");
pub const PROJECTION_ACP_BINDING_NAME: &str = "ProjectionAcpBinding";
pub const PROJECTION_ACP_BINDING: &str =
    include_str!("../schemas/agent/projection_acp_binding.graphql");
pub const TOOL_SELECTION_NAME: &str = "ToolSelection";
pub const TOOL_SELECTION: &str = include_str!("../schemas/agent/tool_selection.graphql");
pub const SKILL_NAME: &str = "Skill";
pub const SKILL: &str = include_str!("../schemas/agent/skill.graphql");
pub const DATASTORE_TOOL_SURFACE_NAME: &str = "DatastoreToolSurface";
pub const DATASTORE_TOOL_SURFACE: &str =
    include_str!("../schemas/agent/datastore_tool_surface.graphql");
pub const WORKSPACE_ROOT_NAME: &str = "WorkspaceRoot";
pub const WORKSPACE_ROOT: &str = include_str!("../schemas/agent/workspace_root.graphql");
pub const ISOLATED_WORKSPACE_NAME: &str = "IsolatedWorkspace";
pub const ISOLATED_WORKSPACE: &str = include_str!("../schemas/agent/isolated_workspace.graphql");
pub const WORKSPACE_PLACEMENT_NAME: &str = "WorkspacePlacement";
pub const WORKSPACE_PLACEMENT: &str = include_str!("../schemas/agent/workspace_placement.graphql");
pub const REPOSITORY_PLACEMENT_NAME: &str = "RepositoryPlacement";
pub const REPOSITORY_PLACEMENT: &str =
    include_str!("../schemas/agent/repository_placement.graphql");
pub const HOST_DEPLOYMENT_NAME: &str = "HostDeployment";
pub const HOST_DEPLOYMENT: &str = include_str!("../schemas/agent/host_deployment.graphql");
pub const WORKSPACE_BINDING_NAME: &str = "WorkspaceBinding";
pub const WORKSPACE_BINDING: &str = include_str!("../schemas/agent/workspace_binding.graphql");
pub const WORKSPACE_RECEIPT_NAME: &str = "WorkspaceReceipt";
pub const WORKSPACE_RECEIPT: &str = include_str!("../schemas/agent/workspace_receipt.graphql");
pub const CALLBACK_MODULE_NAME: &str = "CallbackModule";
pub const CALLBACK_MODULE: &str = include_str!("../schemas/agent/callback_module.graphql");
pub const CALLBACK_BINDING_NAME: &str = "CallbackBinding";
pub const CALLBACK_BINDING: &str = include_str!("../schemas/agent/callback_binding.graphql");
pub const CALLBACK_INVOCATION_NAME: &str = "CallbackInvocation";
pub const CALLBACK_INVOCATION: &str = include_str!("../schemas/agent/callback_invocation.graphql");
pub const CALLBACK_RESULT_NAME: &str = "CallbackResult";
pub const CALLBACK_RESULT: &str = include_str!("../schemas/agent/callback_result.graphql");
pub const TASK_NAME: &str = "Task";
pub const TASK: &str = include_str!("../schemas/agent/task.graphql");
pub const SCHEDULE_NAME: &str = "Schedule";
pub const SCHEDULE: &str = include_str!("../schemas/agent/schedule.graphql");
pub const EVENT_TRIGGER_NAME: &str = "EventTrigger";
pub const EVENT_TRIGGER: &str = include_str!("../schemas/agent/event_trigger.graphql");
pub const EVENT_TRIGGER_GROUP_STATE_NAME: &str = "EventTriggerGroupState";
pub const EVENT_TRIGGER_GROUP_STATE: &str =
    include_str!("../schemas/agent/event_trigger_group_state.graphql");
pub const PEER_PAIRING_DESIRED_NAME: &str = "PeerPairingDesired";
pub const PEER_PAIRING_DESIRED: &str =
    include_str!("../schemas/agent/peer_pairing_desired.graphql");
pub const DATA_PLANE_PAIRING_DESIRED_NAME: &str = "DataPlanePairingDesired";
pub const DATA_PLANE_PAIRING_DESIRED: &str =
    include_str!("../schemas/agent/data_plane_pairing_desired.graphql");
pub const PEER_PAIRING_APPLIED_NAME: &str = "PeerPairingApplied";
pub const PEER_PAIRING_APPLIED: &str =
    include_str!("../schemas/agent/peer_pairing_applied.graphql");
pub const PEER_REGISTRY_NAME: &str = "PeerRegistry";
pub const PEER_REGISTRY: &str = include_str!("../schemas/agent/peer_registry.graphql");
pub const CONSUMED_INVITE_NONCE_NAME: &str = "ConsumedInviteNonce";
pub const CONSUMED_INVITE_NONCE: &str =
    include_str!("../schemas/agent/consumed_invite_nonce.graphql");
pub const RECIPROCAL_CONVERSATION_INTENT_NAME: &str = "ReciprocalConversationIntent";
pub const RECIPROCAL_CONVERSATION_INTENT: &str =
    include_str!("../schemas/agent/reciprocal_conversation_intent.graphql");
pub const PAIRING_BEARER_CLAIM_NAME: &str = "PairingBearerClaim";
pub const PAIRING_BEARER_CLAIM: &str =
    include_str!("../schemas/agent/pairing_bearer_claim.graphql");
pub const BEARER_PAIRING_READY_NAME: &str = "BearerPairingReady";
pub const BEARER_PAIRING_READY: &str =
    include_str!("../schemas/agent/bearer_pairing_ready.graphql");
pub const AGENT_NETWORK_NAME: &str = "AgentNetwork";
pub const AGENT_NETWORK: &str = include_str!("../schemas/agent/agent_network.graphql");
pub const NETWORK_MEMBERSHIP_NAME: &str = "NetworkMembership";
pub const NETWORK_MEMBERSHIP: &str = include_str!("../schemas/agent/network_membership.graphql");
pub const PEER_ENDPOINT_NAME: &str = "PeerEndpoint";
pub const PEER_ENDPOINT: &str = include_str!("../schemas/agent/peer_endpoint.graphql");
pub const NETWORK_JOIN_REQUEST_NAME: &str = "NetworkJoinRequest";
pub const NETWORK_JOIN_REQUEST: &str =
    include_str!("../schemas/agent/network_join_request.graphql");
pub const PERSONA_CONFIG_REQUEST_NAME: &str = "PersonaConfigRequest";
pub const PERSONA_CONFIG_REQUEST: &str =
    include_str!("../schemas/agent/persona_config_request.graphql");

/// Every agent-domain schema in registration order.
pub const ALL: &[&str] = &[
    AGENT_PRINCIPAL,
    AGENT_BEHAVIOR,
    AGENT_RUNTIME,
    AGENT_DIRECTORY_ENTRY,
    AGENT_MEMORY,
    TOOL_SELECTION,
    SKILL,
    DATASTORE_TOOL_SURFACE,
    WORKSPACE_ROOT,
    ISOLATED_WORKSPACE,
    WORKSPACE_PLACEMENT,
    REPOSITORY_PLACEMENT,
    HOST_DEPLOYMENT,
    WORKSPACE_BINDING,
    WORKSPACE_RECEIPT,
    CALLBACK_MODULE,
    CALLBACK_BINDING,
    CALLBACK_INVOCATION,
    CALLBACK_RESULT,
    AGENT_CONVERSATION,
    AGENT_REQUEST,
    AGENT_RESPONSE,
    AGENT_TOOL_RESULT,
    AGENT_SESSION,
    GOAL,
    AGENT_MESSAGE,
    AGENT_TOOL_CALL,
    AGENT_TOOL_APPROVAL,
    COMPACTION_ENTRY,
    RENDERED_REQUEST,
    PROVIDER_CONTEXT_REDUCTION,
    PROJECTION_ACP_BINDING,
    TASK,
    SCHEDULE,
    EVENT_TRIGGER,
    EVENT_TRIGGER_GROUP_STATE,
    PEER_PAIRING_DESIRED,
    DATA_PLANE_PAIRING_DESIRED,
    PEER_PAIRING_APPLIED,
    PEER_REGISTRY,
    CONSUMED_INVITE_NONCE,
    RECIPROCAL_CONVERSATION_INTENT,
    PAIRING_BEARER_CLAIM,
    BEARER_PAIRING_READY,
    AGENT_NETWORK,
    NETWORK_MEMBERSHIP,
    PEER_ENDPOINT,
    NETWORK_JOIN_REQUEST,
    PERSONA_CONFIG_REQUEST,
];

/// Collection names matching [`ALL`] order.
pub const ALL_COLLECTION_NAMES: &[&str] = &[
    AGENT_PRINCIPAL_NAME,
    AGENT_BEHAVIOR_NAME,
    AGENT_RUNTIME_NAME,
    AGENT_DIRECTORY_ENTRY_NAME,
    AGENT_MEMORY_NAME,
    TOOL_SELECTION_NAME,
    SKILL_NAME,
    DATASTORE_TOOL_SURFACE_NAME,
    WORKSPACE_ROOT_NAME,
    ISOLATED_WORKSPACE_NAME,
    WORKSPACE_PLACEMENT_NAME,
    REPOSITORY_PLACEMENT_NAME,
    HOST_DEPLOYMENT_NAME,
    WORKSPACE_BINDING_NAME,
    WORKSPACE_RECEIPT_NAME,
    CALLBACK_MODULE_NAME,
    CALLBACK_BINDING_NAME,
    CALLBACK_INVOCATION_NAME,
    CALLBACK_RESULT_NAME,
    AGENT_CONVERSATION_NAME,
    AGENT_REQUEST_NAME,
    AGENT_RESPONSE_NAME,
    AGENT_TOOL_RESULT_NAME,
    AGENT_SESSION_NAME,
    GOAL_NAME,
    AGENT_MESSAGE_NAME,
    AGENT_TOOL_CALL_NAME,
    AGENT_TOOL_APPROVAL_NAME,
    COMPACTION_ENTRY_NAME,
    RENDERED_REQUEST_NAME,
    PROVIDER_CONTEXT_REDUCTION_NAME,
    PROJECTION_ACP_BINDING_NAME,
    TASK_NAME,
    SCHEDULE_NAME,
    EVENT_TRIGGER_NAME,
    EVENT_TRIGGER_GROUP_STATE_NAME,
    PEER_PAIRING_DESIRED_NAME,
    DATA_PLANE_PAIRING_DESIRED_NAME,
    PEER_PAIRING_APPLIED_NAME,
    PEER_REGISTRY_NAME,
    CONSUMED_INVITE_NONCE_NAME,
    RECIPROCAL_CONVERSATION_INTENT_NAME,
    PAIRING_BEARER_CLAIM_NAME,
    BEARER_PAIRING_READY_NAME,
    AGENT_NETWORK_NAME,
    NETWORK_MEMBERSHIP_NAME,
    PEER_ENDPOINT_NAME,
    NETWORK_JOIN_REQUEST_NAME,
    PERSONA_CONFIG_REQUEST_NAME,
];

/// Agent-domain collections the desktop bulk-syncs after pairing.
///
/// This is a curated subset of the `@branchable` collections, not a mirror of
/// the directive: `WorkspaceRoot`, `AgentNetwork`, `PeerEndpoint`,
/// `RenderedRequest`, `ProviderContextReduction`, and Callback planner/journal
/// rows (`CallbackModule`, `CallbackBinding`, `CallbackInvocation`) are
/// branchable but deliberately not bulk-synced.
pub const BRANCHABLE_COLLECTION_NAMES: &[&str] = &[
    AGENT_DIRECTORY_ENTRY_NAME,
    AGENT_MEMORY_NAME,
    AGENT_CONVERSATION_NAME,
    AGENT_REQUEST_NAME,
    AGENT_RESPONSE_NAME,
    AGENT_TOOL_RESULT_NAME,
    AGENT_SESSION_NAME,
    GOAL_NAME,
    AGENT_MESSAGE_NAME,
    AGENT_TOOL_CALL_NAME,
    AGENT_TOOL_APPROVAL_NAME,
    COMPACTION_ENTRY_NAME,
    TASK_NAME,
    SCHEDULE_NAME,
    EVENT_TRIGGER_NAME,
    EVENT_TRIGGER_GROUP_STATE_NAME,
    ISOLATED_WORKSPACE_NAME,
    WORKSPACE_BINDING_NAME,
    WORKSPACE_RECEIPT_NAME,
    CALLBACK_RESULT_NAME,
];

/// Plaintext prompt-bearing audit facts that stay on the runtime node until
/// installable DefraDB ACP provides an enforceable read boundary (#1074).
///
/// This is a placement classification, not an ACP claim. Every broad
/// replication catalog filters these names explicitly.
pub const LOCAL_AUDIT_COLLECTION_NAMES: &[&str] =
    &[RENDERED_REQUEST_NAME, PROVIDER_CONTEXT_REDUCTION_NAME];

pub fn is_local_audit_collection(name: &str) -> bool {
    LOCAL_AUDIT_COLLECTION_NAMES.contains(&name)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn all_contains_every_agent_schema_file() {
        let schema_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("schemas/agent");
        let schema_file_count = std::fs::read_dir(schema_dir)
            .expect("read agent schema directory")
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.path().extension().and_then(|ext| ext.to_str()) == Some("graphql")
            })
            .count();

        assert_eq!(ALL.len(), schema_file_count);
    }

    #[test]
    fn every_agent_schema_starts_with_type_declaration() {
        for sdl in ALL {
            let first_sdl_line = sdl
                .lines()
                .map(str::trim)
                .find(|line| !line.is_empty() && !line.starts_with('#'))
                .unwrap_or("");
            assert!(
                first_sdl_line.starts_with("type "),
                "schema must begin with `type`: {}",
                sdl.lines().next().unwrap_or("")
            );
        }
    }

    #[test]
    fn collection_names_align_with_sdl_arrays() {
        assert_eq!(ALL.len(), ALL_COLLECTION_NAMES.len());
    }

    #[test]
    fn collection_names_are_unique() {
        let mut seen = HashSet::new();

        for name in ALL_COLLECTION_NAMES {
            assert!(seen.insert(*name), "duplicate collection name: {name}");
        }
    }

    #[test]
    fn every_bulk_sync_collection_is_branchable() {
        for name in BRANCHABLE_COLLECTION_NAMES {
            let sdl = ALL_COLLECTION_NAMES
                .iter()
                .position(|candidate| candidate == name)
                .map(|index| ALL[index])
                .unwrap_or_else(|| panic!("bulk-sync collection {name} has no registered SDL"));
            let declaration = sdl
                .lines()
                .map(str::trim)
                .find(|line| line.starts_with("type "))
                .unwrap_or_else(|| panic!("bulk-sync collection {name} has no type declaration"));
            assert!(
                declaration.contains("@branchable"),
                "bulk-sync collection {name} must be @branchable: {declaration}"
            );
        }
    }

    #[test]
    fn prompt_bearing_audit_facts_are_classified_and_not_bulk_synced() {
        for name in LOCAL_AUDIT_COLLECTION_NAMES {
            assert!(ALL_COLLECTION_NAMES.contains(name));
            assert!(!BRANCHABLE_COLLECTION_NAMES.contains(name));
        }
    }

    #[test]
    fn isolated_workspace_identity_is_branchable_and_placements_are_local() {
        for name in [
            ISOLATED_WORKSPACE_NAME,
            WORKSPACE_BINDING_NAME,
            WORKSPACE_RECEIPT_NAME,
            CALLBACK_MODULE_NAME,
            CALLBACK_BINDING_NAME,
            CALLBACK_INVOCATION_NAME,
            CALLBACK_RESULT_NAME,
        ] {
            let declaration = type_declaration(name);
            assert!(
                declaration.contains("@branchable"),
                "{name} must be @branchable: {declaration}"
            );
        }
        for name in [
            WORKSPACE_PLACEMENT_NAME,
            REPOSITORY_PLACEMENT_NAME,
            HOST_DEPLOYMENT_NAME,
            WORKSPACE_ROOT_NAME,
        ] {
            let declaration = type_declaration(name);
            assert!(
                !declaration.contains("@branchable"),
                "{name} must stay local-only: {declaration}"
            );
        }
        for forbidden in [
            "host_path:",
            "remotes:",
            "git_common_dir:",
            "git-common-dir:",
        ] {
            assert!(
                !ISOLATED_WORKSPACE.contains(forbidden),
                "IsolatedWorkspace must not declare {forbidden}"
            );
        }
        assert!(WORKSPACE_PLACEMENT.contains("host_path:"));
        for name in [
            ISOLATED_WORKSPACE_NAME,
            WORKSPACE_BINDING_NAME,
            WORKSPACE_RECEIPT_NAME,
            CALLBACK_RESULT_NAME,
        ] {
            assert!(
                BRANCHABLE_COLLECTION_NAMES.contains(&name),
                "{name} should bulk-sync"
            );
        }
        for name in [
            CALLBACK_MODULE_NAME,
            CALLBACK_BINDING_NAME,
            CALLBACK_INVOCATION_NAME,
        ] {
            assert!(
                !BRANCHABLE_COLLECTION_NAMES.contains(&name),
                "{name} stays off desktop bulk-sync until a client consumes it"
            );
        }
        assert!(AGENT_REQUEST.contains("workspace_id: String @index @immutable"));
        assert!(AGENT_REQUEST.contains("workspace_authority: String @immutable"));
        assert!(EVENT_TRIGGER.contains("workspace_authority: String"));
        assert!(CALLBACK_RESULT.contains("work_unit_id: String @index"));
        assert!(WORKSPACE_RECEIPT.contains("caused_by_correlation: String @index @immutable"));
        assert!(AGENT_REQUEST.contains("workspace_owner_deployment_id: String @index @immutable"));
        assert!(AGENT_REQUEST.contains("workspace_seal_hash: String @immutable"));
    }

    fn type_declaration(name: &str) -> &'static str {
        let sdl = ALL_COLLECTION_NAMES
            .iter()
            .position(|candidate| *candidate == name)
            .map(|index| ALL[index])
            .unwrap_or_else(|| panic!("{name} has no registered SDL"));
        sdl.lines()
            .map(str::trim)
            .find(|line| line.starts_with("type "))
            .unwrap_or_else(|| panic!("{name} has no type declaration"))
    }
}
