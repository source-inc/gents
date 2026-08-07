use anyhow::Result;
use gents_desktop_core::client::{ClientCore, ClientCoreOptions, DesktopPaths};
use gents_protocol::row::{
    AgentBehaviorRow, InferenceBackendRow, InferenceProfileRow, ScheduleRow, SkillRow, TaskRow,
    ToolSelectionRow,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manage_document_saves_refresh_store() -> Result<()> {
    let tempdir = tempfile::tempdir()?;
    let core = ClientCore::start_with_paths_and_options(
        DesktopPaths::from_root(tempdir.path()),
        ClientCoreOptions::local_only(),
    )
    .await?;

    let principal_resp = core
        .node()
        .execute(
            r#"mutation {
                add_AgentPrincipal(input: {
                    agent_did: "did:test:amy"
                    display_name: "Amy"
                    default_behavior_id: "amy-default"
                    enabled: true
                }) { agent_did }
            }"#,
        )
        .await;
    assert!(!principal_resp.has_errors());

    core.refresh_store().await?;

    core.save_backend(&InferenceBackendRow {
        backend_id: "backend-amy".to_string(),
        name: Some("OpenRouter".to_string()),
        provider_kind: Some("openrouter".to_string()),
        openai_wire_api: None,
        endpoint: Some("https://openrouter.ai/api/v1".to_string()),
        api_key: None,
        api_key_env_var: Some("OPENROUTER_API_KEY".to_string()),
        max_concurrent: Some(2),
        max_queue_depth: Some(100),
        enabled: Some(true),
        models: vec!["openai/gpt-5.4".to_string()],
        last_probe: None,
        probe_status: Some("healthy".to_string()),
    })
    .await?;

    core.save_inference_profile(&InferenceProfileRow {
        profile_id: "profile-amy".to_string(),
        display_name: Some("Amy Profile".to_string()),
        context_window: Some(128000),
        max_output_tokens: Some(4096),
        max_turns: Some(24),
        temperature: Some(0.2),
        stream_batch_ms: Some(50),
        stream_liveness_timeout_secs: Some(300),
        deadline_duration_secs: Some(300),
        retry_max_transport: None,
        retry_backoff_ms: None,
        retry_max_resample: None,
        retry_allow_repair: None,
        retry_interactive_max: None,
        top_p: Some(0.95),
        top_k: Some(40),
        seed: Some(1234),
        min_p: None,
        frequency_penalty: None,
        presence_penalty: None,
        repetition_penalty: None,
        reasoning_effort: None,
    })
    .await?;

    core.save_tool_selection(&ToolSelectionRow {
        selection_id: "tools-amy".to_string(),
        agent_did: Some("did:test:amy".to_string()),
        display_name: Some("Amy Tools".to_string()),
        tool_policy_version: Some("tool-policy/v1".to_string()),
        subagent_default_await_mode: Some("foreground".to_string()),
        write_tools: vec![
            r#"{"tool_name":"upsert_note","collection":"Note","fields":[]}"#.to_string(),
        ],
        enable_self_config: None,
        self_config_categories: Vec::new(),
        self_config_no_lockout: None,
        self_config_dry_run: None,
        enable_file_tools: Some(true),
        file_tools_mode: Some("workspace-write".to_string()),
        file_tool_root: Some("/workspace".to_string()),
        enable_bash: Some(true),
        bash_mode: Some("workspace".to_string()),
        command_execution_policy: None,
        read_only_command_allowlist: Vec::new(),
        command_allowed_argv_prefixes: Vec::new(),
        command_forbidden_argv_prefixes: Vec::new(),
        command_network_mode: None,
        cli_tool_names: vec!["rg".to_string(), "cargo".to_string()],
        enable_meta_tools: Some(true),
        allowed_mcp_service_ids: Vec::new(),
        delegate_to: Vec::new(),
        backgroundable_tool_names: vec!["read_file".to_string()],
        enable_memory: Some(false),
        enable_session_history_tool: Some(false),
        enable_context_budget: Some(true),
        enable_defra_query: Some(true),
        defra_query_collections: vec!["AgentSession".to_string()],
        subagent_targets: vec!["amy-research".to_string()],
        subagent_spawn_enabled: Some(true),
        orchestration_enabled: Some(true),
        subagent_steering_enabled: Some(true),
        subagent_background_enabled: Some(true),
        subagent_allow_cross_deployment: Some(true),
        cross_deployment_spawn_timeout_seconds: Some(45),
    })
    .await?;

    core.save_skill(&SkillRow {
        skill_id: "amy-skill".to_string(),
        agent_did: Some("did:test:amy".to_string()),
        scope: Some("behavior".to_string()),
        name: Some("Amy Skill".to_string()),
        description: Some("Focus Amy on the queue.".to_string()),
        instructions: Some("Inspect the queue and summarize the next action.".to_string()),
        tool_refs: vec!["read_file".to_string()],
        display_name: Some("Queue Skill".to_string()),
        interface_json: None,
        enabled: Some(true),
        created_at: None,
    })
    .await?;

    core.save_behavior(&AgentBehaviorRow {
        behavior_id: "amy-default".to_string(),
        agent_did: Some("did:test:amy".to_string()),
        display_name: Some("Amy Default".to_string()),
        system_prompt: Some("You are Amy.".to_string()),
        backend_id: Some("backend-amy".to_string()),
        model_name: Some("openai/gpt-5.4".to_string()),
        tool_selection_id: Some("tools-amy".to_string()),
        inference_profile_id: Some("profile-amy".to_string()),
        compaction_strategy: Some("rolling-summary".to_string()),
        compaction_threshold: Some(0.7),
        enabled: Some(true),
        skill_refs: vec!["amy-skill".to_string()],
        skill_excludes: vec!["amy-skill".to_string()],
        created_at: Some("2026-04-14T00:00:00Z".to_string()),
    })
    .await?;

    core.save_task(&TaskRow {
        task_id: "task-amy-daily".to_string(),
        name: Some("Daily Amy".to_string()),
        description: Some("Check the daily queue.".to_string()),
        behavior_id: Some("amy-default".to_string()),
        prompt_template: Some("Check the daily queue.".to_string()),
        enabled: Some(true),
        output_schema_ref: None,
        created_at: None,
        updated_at: None,
    })
    .await?;

    core.save_schedule(&ScheduleRow {
        schedule_id: "schedule-amy-daily".to_string(),
        task_id: Some("task-amy-daily".to_string()),
        interval_secs: Some(300),
        cron: None,
        timezone: None,
        missed_run_policy: None,
        enabled: Some(true),
        concurrency: Some("latest_only".to_string()),
        next_run_at: Some("2026-04-15T00:00:00Z".to_string()),
        last_attempt_at: None,
        last_status: None,
        last_error: None,
        fire_count: Some(0),
        created_at: None,
        updated_at: None,
    })
    .await?;

    let snapshot = core.store().snapshot();

    assert!(snapshot
        .inference_backends
        .iter()
        .any(|row| row.backend_id == "backend-amy" && row.name.as_deref() == Some("OpenRouter")));
    let profile = snapshot
        .inference_profiles
        .iter()
        .find(|row| row.profile_id == "profile-amy")
        .expect("inference profile should be present");
    assert_eq!(profile.display_name.as_deref(), Some("Amy Profile"));
    assert_eq!(profile.top_p, Some(0.95));
    assert_eq!(profile.top_k, Some(40));
    let tools = snapshot
        .tool_selections
        .iter()
        .find(|row| row.selection_id == "tools-amy")
        .expect("tool selection should be present");
    assert_eq!(tools.cli_tool_names.len(), 2);
    assert_eq!(
        tools.backgroundable_tool_names,
        vec!["read_file".to_string()]
    );
    assert_eq!(tools.subagent_targets, vec!["amy-research".to_string()]);
    assert_eq!(tools.subagent_spawn_enabled, Some(true));
    assert_eq!(tools.orchestration_enabled, Some(true));
    assert_eq!(tools.subagent_steering_enabled, Some(true));
    assert_eq!(tools.subagent_background_enabled, Some(true));
    assert_eq!(tools.subagent_allow_cross_deployment, Some(true));
    assert_eq!(tools.cross_deployment_spawn_timeout_seconds, Some(45));
    assert_eq!(tools.enable_defra_query, Some(true));
    assert_eq!(
        tools.defra_query_collections,
        vec!["AgentSession".to_string()]
    );
    assert_eq!(tools.tool_policy_version.as_deref(), Some("tool-policy/v1"));
    assert_eq!(
        tools.subagent_default_await_mode.as_deref(),
        Some("foreground")
    );
    assert_eq!(tools.write_tools.len(), 1);
    assert!(tools.write_tools[0].contains("upsert_note"));
    assert!(snapshot.skills.iter().any(|row| row.skill_id == "amy-skill"
        && row.name.as_deref() == Some("Amy Skill")
        && row.tool_refs == vec!["read_file".to_string()]));
    let behavior = snapshot
        .behaviors
        .iter()
        .find(|row| {
            row.behavior_id == "amy-default"
                && row.backend_id.as_deref() == Some("backend-amy")
                && row.inference_profile_id.as_deref() == Some("profile-amy")
                && row.tool_selection_id.as_deref() == Some("tools-amy")
        })
        .expect("behavior should be present");
    assert_eq!(behavior.skill_refs, vec!["amy-skill".to_string()]);
    assert_eq!(behavior.skill_excludes, vec!["amy-skill".to_string()]);
    let task = snapshot
        .tasks
        .iter()
        .find(|row| row.task_id == "task-amy-daily")
        .expect("task should be present");
    assert_eq!(task.behavior_id.as_deref(), Some("amy-default"));
    assert_eq!(task.enabled, Some(true));

    let schedule = snapshot
        .schedules
        .iter()
        .find(|row| row.schedule_id == "schedule-amy-daily")
        .expect("schedule should be present");
    assert_eq!(schedule.task_id.as_deref(), Some("task-amy-daily"));
    assert_eq!(schedule.interval_secs, Some(300));
    assert_eq!(schedule.enabled, Some(true));

    core.delete_skill("amy-skill", "did:test:amy").await?;
    let snapshot = core.store().snapshot();
    assert!(!snapshot
        .skills
        .iter()
        .any(|row| row.skill_id == "amy-skill"));
    let behavior = snapshot
        .behaviors
        .iter()
        .find(|row| row.behavior_id == "amy-default")
        .expect("behavior should survive skill deletion");
    assert!(behavior.skill_refs.is_empty());
    assert!(behavior.skill_excludes.is_empty());

    core.shutdown().await?;
    Ok(())
}
