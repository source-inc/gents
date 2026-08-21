use crate::types::{
    BehaviorView, DeploymentView, DesktopBootstrapSummary, DesktopClientSnapshot,
    DesktopRuntimeSnapshot, InferenceBackendView, SkillView, ToolSelectionView,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotGrants {
    pub session_read: bool,
    pub fleet_read: bool,
    pub config_read: bool,
    pub operations_read: bool,
    pub runtime_admin: bool,
}

impl Default for SnapshotGrants {
    fn default() -> Self {
        Self {
            session_read: false,
            fleet_read: false,
            config_read: false,
            operations_read: false,
            runtime_admin: false,
        }
    }
}

impl SnapshotGrants {
    pub fn core_only() -> Self {
        Self::default()
    }

    pub fn all() -> Self {
        Self {
            session_read: true,
            fleet_read: true,
            config_read: true,
            operations_read: true,
            runtime_admin: true,
        }
    }

    pub fn chat_package() -> Self {
        Self {
            session_read: true,
            fleet_read: false,
            config_read: false,
            operations_read: false,
            runtime_admin: false,
        }
    }

    pub fn fleet_package() -> Self {
        Self {
            session_read: false,
            fleet_read: true,
            config_read: false,
            operations_read: false,
            runtime_admin: false,
        }
    }
}

pub fn project_client_snapshot(
    mut snapshot: DesktopClientSnapshot,
    grants: SnapshotGrants,
) -> DesktopClientSnapshot {
    snapshot.bootstrap = project_bootstrap_summary(snapshot.bootstrap, grants);
    if let Some(client) = snapshot.client.take() {
        snapshot.client = Some(project_runtime_snapshot(client, grants));
    }
    snapshot
}

pub fn project_bootstrap_summary(
    mut summary: DesktopBootstrapSummary,
    grants: SnapshotGrants,
) -> DesktopBootstrapSummary {
    if !grants.runtime_admin {
        summary.default_agent_home = String::new();
        summary.init_tool_root = None;
        summary.desktop_home = String::new();
        summary.peer_directory_path = String::new();
        summary.node_data_dir = String::new();
        summary.log_file_path = String::new();
    }
    if !grants.fleet_read {
        summary.saved_peers.clear();
    } else if !grants.runtime_admin {
        for peer in &mut summary.saved_peers {
            peer.addr = String::new();
            peer.graphql = None;
        }
    }
    summary
}

pub fn project_runtime_snapshot(
    mut runtime: DesktopRuntimeSnapshot,
    grants: SnapshotGrants,
) -> DesktopRuntimeSnapshot {
    if !grants.fleet_read {
        runtime.listen_addresses.clear();
    }
    runtime.deployments = runtime
        .deployments
        .into_iter()
        .map(|d| project_deployment(d, grants))
        .collect();
    runtime
}

fn project_deployment(mut deployment: DeploymentView, grants: SnapshotGrants) -> DeploymentView {
    if !grants.fleet_read {
        deployment.addr = String::new();
        deployment.graphql = None;
        deployment.dial_succeeded = false;
        deployment.last_error = None;
    }

    if !grants.session_read {
        deployment.conversations.clear();
        for environment in &mut deployment.behavior_environments {
            environment.session_count = 0;
            environment.active_session_count = 0;
        }
    }

    if grants.config_read {
        return deployment;
    }

    deployment.behaviors = deployment
        .behaviors
        .into_iter()
        .map(project_behavior_for_chat)
        .collect();
    deployment.skills = deployment
        .skills
        .into_iter()
        .map(project_skill_for_chat)
        .collect();
    deployment.inference_backends = deployment
        .inference_backends
        .into_iter()
        .map(project_backend_for_fleet)
        .collect();
    deployment.tool_selections = deployment
        .tool_selections
        .into_iter()
        .map(project_tool_selection_for_fleet)
        .collect();
    for environment in &mut deployment.behavior_environments {
        // Keep coarse capability labels: chat clients need an honest account
        // of the behavior's authority. Referenced profile labels and host paths
        // remain config-only.
        environment.inference_profile_name = None;
        environment.workspace_root = None;
    }

    if !grants.fleet_read {
        deployment.tasks.clear();
        deployment.schedules.clear();
        deployment.event_triggers.clear();
    } else {
        for task in &mut deployment.tasks {
            task.prompt_template = None;
            task.description = None;
            task.run_history.clear();
        }
    }
    deployment.inference_profiles.clear();
    deployment.tool_service_registries.clear();

    deployment
}

fn project_behavior_for_chat(mut behavior: BehaviorView) -> BehaviorView {
    behavior.system_prompt = None;
    behavior.backend_id = None;
    behavior.inference_profile_id = None;
    behavior.compaction_strategy = None;
    behavior.compaction_threshold = None;
    behavior
}

fn project_skill_for_chat(mut skill: SkillView) -> SkillView {
    skill.instructions = None;
    skill.description = None;
    skill.tool_refs.clear();
    skill
}

fn project_backend_for_fleet(mut backend: InferenceBackendView) -> InferenceBackendView {
    backend.endpoint = None;
    backend.api_key_configured = false;
    backend.api_key_env_var = None;
    backend
}

fn project_tool_selection_for_fleet(mut selection: ToolSelectionView) -> ToolSelectionView {
    selection.file_tool_root = None;
    selection.command_execution_policy = None;
    selection.command_allowed_argv_prefixes.clear();
    selection.command_forbidden_argv_prefixes.clear();
    selection.write_tools.clear();
    selection.defra_query_collections.clear();
    selection
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        AgentPrincipalView, BehaviorEnvironmentView, BehaviorView, ConversationSummary,
        DeploymentView, DesktopBootstrapSummary, DesktopClientSnapshot, DesktopRuntimeSnapshot,
        P2PHealthView, SavedPeerView, SkillView,
    };

    fn sample_snapshot() -> DesktopClientSnapshot {
        DesktopClientSnapshot {
            bootstrap: DesktopBootstrapSummary {
                default_agent_home: "/secret/agent".into(),
                init_agent_name: Some("Agent".into()),
                init_agent_did: Some("did:test:local".into()),
                init_tool_ceiling: Some("readonly".into()),
                init_tool_root: Some("/secret/tools".into()),
                desktop_home: "/secret/desktop".into(),
                peer_directory_path: "/secret/peers.json".into(),
                node_data_dir: "/secret/node".into(),
                log_file_path: "/secret/log".into(),
                agent_home_exists: true,
                desktop_home_exists: true,
                peer_directory_exists: true,
                saved_peers: vec![SavedPeerView {
                    peer_id: "peer_1".into(),
                    label: "Remote".into(),
                    agent_did: "did:test:remote".into(),
                    addr: "/ip4/10.0.0.1/tcp/1".into(),
                    source: Some("manual".into()),
                    graphql: Some("http://10.0.0.1/graphql".into()),
                }],
            },
            client: Some(DesktopRuntimeSnapshot {
                local_peer_id: "local".into(),
                listen_addresses: vec!["/ip4/0.0.0.0/tcp/1".into()],
                p2p_health: P2PHealthView {
                    status: "ok".into(),
                    connected_peer_count: 1,
                    replicator_count: 1,
                    consecutive_failures: 0,
                    last_error: None,
                    last_ok_at: None,
                    last_failure_at: None,
                },
                bootstrap_errors: vec![],
                last_mutation_error: None,
                focused_request_id: None,
                configured_peer_count: 1,
                dialed_peer_count: 1,
                peer_issue_count: 0,
                row_count: 10,
                approx_serialized_bytes: 100,
                deployments: vec![DeploymentView {
                    peer_id: "local".into(),
                    label: "Local".into(),
                    agent_did: "did:test:local".into(),
                    addr: "/ip4/127.0.0.1/tcp/1".into(),
                    source: Some("local".into()),
                    graphql: Some("http://127.0.0.1/graphql".into()),
                    dial_succeeded: true,
                    pairing_ready: true,
                    last_error: None,
                    default_behavior_id: Some("default".into()),
                    agent_principal: AgentPrincipalView {
                        agent_did: "did:test:local".into(),
                        display_name: Some("Local".into()),
                        default_behavior_id: Some("default".into()),
                        enabled: Some(true),
                        created_at: None,
                        created_by: None,
                    },
                    runtime: None,
                    behaviors: vec![BehaviorView {
                        behavior_id: "default".into(),
                        display_name: "Default".into(),
                        system_prompt: Some("SECRET PROMPT".into()),
                        backend_id: Some("openai".into()),
                        model_name: Some("gpt".into()),
                        tool_selection_id: None,
                        inference_profile_id: None,
                        compaction_strategy: None,
                        compaction_threshold: None,
                        enabled: true,
                        is_default: true,
                        skill_refs: vec!["skill_a".into()],
                        skill_excludes: vec![],
                    }],
                    behavior_environments: vec![BehaviorEnvironmentView {
                        behavior_id: "default".into(),
                        display_name: "Default".into(),
                        enabled: true,
                        is_default: true,
                        model_name: Some("gpt".into()),
                        inference_profile_name: Some("Long context".into()),
                        workspace_root: Some("/secret/workspace".into()),
                        file_access: "read / write".into(),
                        bash_access: "unrestricted".into(),
                        network_access: Some("allow".into()),
                        skill_names: vec!["Skill A".into()],
                        session_count: 1,
                        active_session_count: 0,
                    }],
                    inference_backends: vec![],
                    inference_profiles: vec![],
                    tool_selections: vec![],
                    tool_service_registries: vec![],
                    skills: vec![SkillView {
                        skill_id: "skill_a".into(),
                        agent_did: None,
                        scope: Some("behavior".into()),
                        name: Some("Skill A".into()),
                        description: Some("desc".into()),
                        instructions: Some("DO SECRET THINGS".into()),
                        tool_refs: vec![],
                        display_name: Some("Skill A".into()),
                        enabled: Some(true),
                        created_at: None,
                    }],
                    tasks: vec![],
                    schedules: vec![],
                    event_triggers: vec![],
                    conversations: vec![ConversationSummary {
                        session_id: "sess_1".into(),
                        title: Some("Chat".into()),
                        preview_text: Some("hi".into()),
                        status: None,
                        behavior_id: None,
                        latest_request_id: None,
                        task_id: None,
                        task_name: None,
                        trigger_id: None,
                        trigger_kind: None,
                        created_at: None,
                        updated_at: None,
                        turn_state: None,
                        message_count: 1,
                        tool_call_count: 0,
                    }],
                }],
            }),
        }
    }

    #[test]
    fn core_only_strips_paths_peers_conversations_and_authored_content() {
        let projected = project_client_snapshot(sample_snapshot(), SnapshotGrants::core_only());
        assert!(projected.bootstrap.desktop_home.is_empty());
        assert!(projected.bootstrap.default_agent_home.is_empty());
        assert!(projected.bootstrap.saved_peers.is_empty());
        let client = projected.client.expect("client");
        assert!(client.listen_addresses.is_empty());
        let dep = &client.deployments[0];
        assert!(dep.conversations.is_empty());
        assert_eq!(dep.behavior_environments[0].session_count, 0);
        assert!(dep.behavior_environments[0].workspace_root.is_none());
        assert!(dep.behavior_environments[0]
            .inference_profile_name
            .is_none());
        assert!(dep.addr.is_empty());
        assert!(dep.behaviors[0].system_prompt.is_none());
        assert_eq!(dep.behaviors[0].model_name.as_deref(), Some("gpt"));
        assert_eq!(dep.behaviors[0].skill_refs, vec!["skill_a".to_string()]);
        assert!(dep.skills[0].instructions.is_none());
        assert_eq!(dep.skills[0].skill_id, "skill_a");
        assert_eq!(
            projected.bootstrap.init_agent_did.as_deref(),
            Some("did:test:local")
        );
    }

    #[test]
    fn session_read_keeps_conversations_and_chat_projections() {
        let projected = project_client_snapshot(sample_snapshot(), SnapshotGrants::chat_package());
        let dep = &projected.client.as_ref().unwrap().deployments[0];
        assert_eq!(dep.conversations.len(), 1);
        assert_eq!(dep.behavior_environments[0].session_count, 1);
        assert!(dep.behavior_environments[0].workspace_root.is_none());
        assert!(dep.behavior_environments[0]
            .inference_profile_name
            .is_none());
        assert!(dep.behaviors[0].system_prompt.is_none());
        assert_eq!(dep.behaviors[0].model_name.as_deref(), Some("gpt"));
        assert!(dep.skills[0].instructions.is_none());
        assert_eq!(dep.behaviors[0].skill_refs, vec!["skill_a".to_string()]);
    }

    #[test]
    fn fleet_read_keeps_peer_summaries_without_paths() {
        let projected = project_client_snapshot(sample_snapshot(), SnapshotGrants::fleet_package());
        assert!(projected.bootstrap.desktop_home.is_empty());
        assert_eq!(projected.bootstrap.saved_peers.len(), 1);
        assert!(projected.bootstrap.saved_peers[0].addr.is_empty());
        assert!(projected.bootstrap.saved_peers[0].graphql.is_none());
        let dep = &projected.client.as_ref().unwrap().deployments[0];
        assert_eq!(dep.addr, "/ip4/127.0.0.1/tcp/1");
        assert!(dep.conversations.is_empty());
        assert_eq!(dep.behavior_environments[0].session_count, 0);
        assert!(dep.behavior_environments[0].workspace_root.is_none());
    }

    #[test]
    fn full_grants_preserve_sensitive_fields() {
        let projected = project_client_snapshot(sample_snapshot(), SnapshotGrants::all());
        assert_eq!(projected.bootstrap.desktop_home, "/secret/desktop");
        assert_eq!(
            projected.client.as_ref().unwrap().deployments[0].behaviors[0]
                .system_prompt
                .as_deref(),
            Some("SECRET PROMPT")
        );
        assert_eq!(
            projected.client.as_ref().unwrap().deployments[0].behavior_environments[0]
                .workspace_root
                .as_deref(),
            Some("/secret/workspace")
        );
        assert_eq!(
            projected.client.as_ref().unwrap().deployments[0].behavior_environments[0]
                .inference_profile_name
                .as_deref(),
            Some("Long context")
        );
    }
}
