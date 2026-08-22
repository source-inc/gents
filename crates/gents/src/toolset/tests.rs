use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use super::args::{
    BashArgs, EditFileArgs, GlobArgs, GrepArgs, ListFilesArgs, ReadFileArgs, WriteFileArgs,
};
use super::bash_tools::{ReadOnlyBashTool, UnrestrictedBashTool};
use super::file_tools::{
    EditFileTool, GlobTool, GrepTool, ListFilesTool, ReadFileTool, WriteFileTool,
};
use super::shared::{
    build_shell_env_from_vars, select_sandbox_for_policy, validate_command_policy,
    validate_read_only_command, CommandExecutionMode, CommandExecutionPolicy, CommandNetworkMode,
    ToolContext, ToolError, WorkspaceAuthority,
};
use super::*;
use crate::lean_vocab_test::{
    lean_command_env_cases, lean_command_policy_case, lean_command_policy_cases,
    lean_command_sandbox_cases, lean_native_filesystem_boundary_cases, LeanCommandPolicyCase,
};
use crate::tool_call_lifecycle::AwaitMode;

#[test]
fn toolset_presets_have_expected_counts() {
    assert_eq!(ToolSet::readonly().native_tools().len(), 5);
    assert_eq!(
        ToolSet::readwrite(std::env::temp_dir())
            .native_tools()
            .len(),
        8
    );
    assert_eq!(ToolSet::meta_only().native_tools().len(), 0);
}

#[tokio::test]
async fn native_tool_definitions_include_model_facing_defaults_and_constraints() {
    let root = temp_root("gents-tool-definitions");
    let context = ToolContext::new(root.clone(), false).unwrap();

    let list_tool = ListFilesTool::new(context.clone(), DEFAULT_MAX_LIST_ENTRIES);
    let list_def = crate::llm::tool::Tool::definition(&list_tool, String::new()).await;
    assert!(list_def.parameters.get("required").is_none());
    assert_eq!(list_def.parameters["properties"]["path"]["default"], ".");
    assert_eq!(
        list_def.parameters["properties"]["recursive"]["default"],
        false
    );
    assert_eq!(
        list_def.parameters["properties"]["max_entries"]["maximum"],
        DEFAULT_MAX_LIST_ENTRIES
    );
    assert!(
        list_def.parameters["properties"]["max_entries"]["description"]
            .as_str()
            .unwrap()
            .contains("capped by the tool")
    );

    let read_tool = ReadFileTool::new(context.clone(), DEFAULT_MAX_FILE_CHARS);
    let read_def = crate::llm::tool::Tool::definition(&read_tool, String::new()).await;
    assert_eq!(read_def.parameters["required"], serde_json::json!(["path"]));
    assert_eq!(
        read_def.parameters["properties"]["start_line"]["default"],
        1
    );
    assert_eq!(
        read_def.parameters["properties"]["max_chars"]["maximum"],
        DEFAULT_MAX_FILE_CHARS
    );

    let write_tool = WriteFileTool::new(context.clone());
    let write_def = crate::llm::tool::Tool::definition(&write_tool, String::new()).await;
    assert_eq!(
        write_def.parameters["required"],
        serde_json::json!(["path", "content"])
    );
    assert!(write_def.parameters["properties"]["content"]["description"]
        .as_str()
        .unwrap()
        .contains("Existing file contents are replaced"));

    let bash_tool = UnrestrictedBashTool::new(
        ToolContext::new(root, false).unwrap(),
        Duration::from_secs(DEFAULT_COMMAND_TIMEOUT_SECS),
    );
    let bash_def = crate::llm::tool::Tool::definition(&bash_tool, String::new()).await;
    assert_eq!(
        bash_def.parameters["required"],
        serde_json::json!(["command"])
    );
    assert_eq!(
        bash_def.parameters["properties"]["args"]["default"],
        serde_json::json!([])
    );
    assert_eq!(
        bash_def.parameters["properties"]["timeout_secs"]["maximum"],
        DEFAULT_COMMAND_TIMEOUT_SECS
    );
    assert_eq!(
        bash_def.parameters["properties"]["timeout_secs"]["default"],
        DEFAULT_COMMAND_TIMEOUT_SECS
    );
    let timeout_description = bash_def.parameters["properties"]["timeout_secs"]["description"]
        .as_str()
        .unwrap();
    assert!(
        timeout_description.contains(&BACKGROUND_COMMAND_TIMEOUT_SECS.to_string()),
        "schema must state the background lifetime budget: {timeout_description}"
    );
}

// #1018: when the operator raises the foreground cap above the default, the
// model-visible schema advertises the default and the cap as distinct values.
#[tokio::test]
async fn bash_schema_advertises_decoupled_default_and_max() {
    let root = temp_root("gents-decoupled-timeout");
    let tool = UnrestrictedBashTool::with_policy(
        ToolContext::new(root, false).unwrap(),
        Duration::from_secs(600),
        Duration::from_secs(3_600),
        CommandExecutionPolicy::write_capable(),
    );
    let def = crate::llm::tool::Tool::definition(&tool, String::new()).await;
    assert_eq!(def.parameters["properties"]["timeout_secs"]["default"], 600);
    assert_eq!(
        def.parameters["properties"]["timeout_secs"]["maximum"],
        3_600
    );
    let description = def.parameters["properties"]["timeout_secs"]["description"]
        .as_str()
        .unwrap();
    assert!(description.contains("600"), "{description}");
    assert!(description.contains("3600"), "{description}");
}

#[test]
fn subagent_tool_names_are_gated_by_spawn_and_targets() {
    let disabled = SubagentToolConfig {
        targets: subagent_targets("worker"),
        spawn_enabled: false,
        steering_enabled: false,
        background_enabled: true,
        default_await_mode: AwaitMode::Foreground,
        allow_cross_deployment: false,
    };
    assert!(subagent_tool_names(&disabled).is_empty());

    let no_targets = SubagentToolConfig {
        targets: Vec::new(),
        spawn_enabled: true,
        steering_enabled: true,
        background_enabled: true,
        default_await_mode: AwaitMode::Foreground,
        allow_cross_deployment: false,
    };
    assert!(subagent_tool_names(&no_targets).is_empty());

    let enabled = SubagentToolConfig {
        targets: subagent_targets("worker"),
        spawn_enabled: true,
        steering_enabled: true,
        background_enabled: false,
        default_await_mode: AwaitMode::Foreground,
        allow_cross_deployment: false,
    };
    let names = subagent_tool_names(&enabled);
    assert_eq!(
        names,
        vec![
            SPAWN_SUBAGENT_TOOL_NAME.to_string(),
            WAIT_SUBAGENT_TOOL_NAME.to_string(),
            LIST_SUBAGENTS_TOOL_NAME.to_string(),
            CANCEL_SUBAGENT_TOOL_NAME.to_string()
        ]
    );
    assert!(!names.contains(&"read_subagent".to_string()));
    assert!(!names.contains(&"steer_subagent".to_string()));

    let background_without_steering = SubagentToolConfig {
        targets: subagent_targets("worker"),
        spawn_enabled: true,
        steering_enabled: false,
        background_enabled: true,
        default_await_mode: AwaitMode::Foreground,
        allow_cross_deployment: false,
    };
    assert_eq!(
        subagent_tool_names(&background_without_steering),
        vec![
            SPAWN_SUBAGENT_TOOL_NAME.to_string(),
            WAIT_SUBAGENT_TOOL_NAME.to_string(),
            LIST_SUBAGENTS_TOOL_NAME.to_string(),
            READ_SUBAGENT_TOOL_NAME.to_string(),
            CANCEL_SUBAGENT_TOOL_NAME.to_string()
        ]
    );

    let steering_and_background = SubagentToolConfig {
        targets: subagent_targets("worker"),
        spawn_enabled: true,
        steering_enabled: true,
        background_enabled: true,
        default_await_mode: AwaitMode::Foreground,
        allow_cross_deployment: false,
    };
    assert_eq!(
        subagent_tool_names(&steering_and_background),
        vec![
            SPAWN_SUBAGENT_TOOL_NAME.to_string(),
            WAIT_SUBAGENT_TOOL_NAME.to_string(),
            LIST_SUBAGENTS_TOOL_NAME.to_string(),
            READ_SUBAGENT_TOOL_NAME.to_string(),
            STEER_SUBAGENT_TOOL_NAME.to_string(),
            CANCEL_SUBAGENT_TOOL_NAME.to_string()
        ]
    );
}

#[test]
fn native_tool_backgroundable_capability_is_explicit() {
    let root = temp_root("gents-backgroundable-capability");
    let tools = ToolSet::readwrite(root);
    let backgroundable = tools.backgroundable_tool_names();

    assert!(backgroundable.contains(&"bash".to_string()));
    assert!(backgroundable.contains(&"bash_unrestricted".to_string()));
    assert!(tools.is_backgroundable_tool_name("bash"));
    assert!(tools.is_backgroundable_tool_name("bash_unrestricted"));
    assert!(!tools.is_backgroundable_tool_name("read_file"));
    assert!(!tools.is_backgroundable_tool_name("glob"));
    assert!(!tools.is_backgroundable_tool_name("grep"));
}

#[test]
fn background_tool_names_are_gated_by_allowlist() {
    let disabled = BackgroundToolConfig {
        allowlist: Vec::new(),
    };
    assert!(background_tool_names(&disabled).is_empty());

    let enabled = BackgroundToolConfig {
        allowlist: vec!["bash".to_string()],
    };
    assert_eq!(
        background_tool_names(&enabled),
        vec![
            SPAWN_PROCESS_TOOL_NAME.to_string(),
            WAIT_PROCESS_TOOL_NAME.to_string(),
            LIST_PROCESSES_TOOL_NAME.to_string(),
            READ_PROCESS_TOOL_NAME.to_string(),
            CANCEL_PROCESS_TOOL_NAME.to_string()
        ]
    );
}

#[tokio::test]
async fn subagent_tool_definitions_register_expected_surface() {
    let config = SubagentToolConfig {
        targets: subagent_targets("research"),
        spawn_enabled: true,
        steering_enabled: true,
        background_enabled: false,
        default_await_mode: AwaitMode::Foreground,
        allow_cross_deployment: false,
    };
    let tools = build_subagent_tools(config);
    let names = tools.iter().map(|tool| tool.name()).collect::<Vec<_>>();
    assert_eq!(
        names,
        vec![
            SPAWN_SUBAGENT_TOOL_NAME.to_string(),
            WAIT_SUBAGENT_TOOL_NAME.to_string(),
            LIST_SUBAGENTS_TOOL_NAME.to_string(),
            CANCEL_SUBAGENT_TOOL_NAME.to_string()
        ]
    );

    let spawn_def = tools[0].definition(String::new()).await;
    assert_eq!(
        spawn_def.parameters["properties"]["name"]["enum"],
        serde_json::json!(["research"])
    );
    assert_eq!(
        spawn_def.parameters["properties"]["await_mode"]["enum"],
        serde_json::json!(["foreground"])
    );
    assert!(
        spawn_def.parameters["properties"].get("deadline").is_none(),
        "spawn_subagent should not advertise model-supplied absolute deadlines"
    );
}

#[tokio::test]
async fn spawn_subagent_definition_exposes_background_mode_when_enabled() {
    let config = SubagentToolConfig {
        targets: subagent_targets("research"),
        spawn_enabled: true,
        steering_enabled: true,
        background_enabled: true,
        default_await_mode: AwaitMode::Foreground,
        allow_cross_deployment: false,
    };
    let tools = build_subagent_tools(config);
    let spawn_def = tools[0].definition(String::new()).await;

    assert_eq!(
        spawn_def.parameters["properties"]["await_mode"]["enum"],
        serde_json::json!(["foreground", "background"])
    );
}

#[tokio::test]
async fn spawn_subagent_definition_uses_configured_default_await_mode() {
    let config = SubagentToolConfig {
        targets: subagent_targets("research"),
        spawn_enabled: true,
        steering_enabled: true,
        background_enabled: true,
        default_await_mode: AwaitMode::Background,
        allow_cross_deployment: false,
    };
    let tools = build_subagent_tools(config);
    let spawn_def = tools[0].definition(String::new()).await;

    assert_eq!(
        spawn_def.parameters["properties"]["await_mode"]["default"],
        serde_json::json!("background")
    );
}

/// Build a single-target list for subagent tool tests. `name` doubles as the
/// behavior id; the agent_did is a fixed local placeholder.
fn subagent_targets(name: &str) -> Vec<crate::document_config::SubagentTarget> {
    vec![crate::document_config::SubagentTarget {
        name: name.to_string(),
        agent_did: "did:key:zTest".to_string(),
        behavior_id: name.to_string(),
        description: None,
    }]
}

fn temp_root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("{name}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn ensure_native_fs_runner_for_test() {
    static BUILT: OnceLock<()> = OnceLock::new();
    BUILT.get_or_init(|| {
        if native_fs_runner_binary_for_current_test().is_some() {
            return;
        }

        let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("gents manifest should be under workspace crates/")
            .to_path_buf();
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
        let status = std::process::Command::new(cargo)
            .args(["build", "-p", "gents-fs-runner"])
            .current_dir(repo_root)
            .status()
            .expect("building gents-fs-runner test binary");
        assert!(
            status.success(),
            "gents-fs-runner test binary must build before native filesystem tool tests"
        );
        assert!(
            native_fs_runner_binary_for_current_test().is_some(),
            "gents-fs-runner test binary must be adjacent to test binary after build"
        );
    });
}

fn native_fs_runner_binary_for_current_test() -> Option<PathBuf> {
    let exe_name = if cfg!(windows) {
        "gents-fs-runner.exe"
    } else {
        "gents-fs-runner"
    };

    let current = std::env::current_exe().ok()?;
    let parent = current.parent()?;
    [parent.to_path_buf(), parent.parent()?.to_path_buf()]
        .into_iter()
        .map(|dir| dir.join(exe_name))
        .find(|candidate| candidate.is_file())
}

fn compact_meta(output: &str) -> serde_json::Value {
    let first_line = output.lines().next().expect("metadata line");
    let raw = first_line
        .strip_prefix("gents_fs: ")
        .unwrap_or_else(|| panic!("missing gents_fs metadata line in output:\n{output}"));
    serde_json::from_str(raw).expect("metadata json")
}

fn compact_exec_meta(output: &str) -> serde_json::Value {
    let first_line = output.lines().next().expect("metadata line");
    let raw = first_line
        .strip_prefix("gents_exec: ")
        .unwrap_or_else(|| panic!("missing gents_exec metadata line in output:\n{output}"));
    serde_json::from_str(raw).expect("metadata json")
}

struct EnvVarGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
        let previous = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self { key, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(previous) => std::env::set_var(self.key, previous),
            None => std::env::remove_var(self.key),
        }
    }
}

#[tokio::test]
async fn native_filesystem_deadline_preempts_single_poll_blocker_and_advances_queue() {
    ensure_native_fs_runner_for_test();
    let root = temp_root("gents-native-boundary");
    std::fs::write(root.join("first.txt"), "first request\n").unwrap();
    std::fs::write(root.join("second.txt"), "second request\n").unwrap();
    let context = ToolContext::new(root.clone(), false).unwrap();

    let _block_dir = EnvVarGuard::set("GENTS_FS_RUNNER_BLOCK_DIR", context.root().as_os_str());
    let _block_ms = EnvVarGuard::set("GENTS_FS_RUNNER_BLOCK_MS", "200");
    let blocking_tool: Box<dyn crate::llm::tool::ToolDyn> =
        Box::new(GlobTool::new(context.clone(), DEFAULT_MAX_MATCHES));
    let second_tool: Box<dyn crate::llm::tool::ToolDyn> =
        Box::new(ReadFileTool::new(context, DEFAULT_MAX_FILE_CHARS));

    let started = Instant::now();
    let first_deadline = chrono::Utc::now() + chrono::Duration::milliseconds(15);
    let first_outcome = crate::tool_call_lifecycle::runtime::scope_request_tool_execution(
        Some(first_deadline),
        tokio_util::sync::CancellationToken::new(),
        crate::tool_call_lifecycle::runtime::call_tool_managed(
            blocking_tool.as_ref(),
            r#"{"pattern":"*.txt"}"#.to_string(),
        ),
    )
    .await;
    let first_elapsed = started.elapsed();

    let second_deadline = chrono::Utc::now() + chrono::Duration::seconds(1);
    let second_outcome = crate::tool_call_lifecycle::runtime::scope_request_tool_execution(
        Some(second_deadline),
        tokio_util::sync::CancellationToken::new(),
        crate::tool_call_lifecycle::runtime::call_tool_managed(
            second_tool.as_ref(),
            r#"{"path":"second.txt"}"#.to_string(),
        ),
    )
    .await;
    let queue_elapsed = started.elapsed();

    tokio::time::sleep(Duration::from_millis(225)).await;
    let _ = std::fs::remove_dir_all(&root);

    assert!(
        matches!(
            first_outcome,
            crate::tool_call_lifecycle::ToolOutcome::TimedOut { .. }
        ),
        "blocking native tool must resolve to a typed timeout, got {first_outcome:?}"
    );
    assert!(
        first_elapsed < Duration::from_millis(150),
        "blocking native tool should terminalize at the request deadline, elapsed={first_elapsed:?}"
    );
    assert!(
        queue_elapsed < Duration::from_millis(150),
        "single-worker queue should advance before the blocking native work returns, elapsed={queue_elapsed:?}"
    );
    match &second_outcome {
        crate::tool_call_lifecycle::ToolOutcome::Completed(text) => {
            assert!(text.contains("second request"));
        }
        other => panic!("second read should complete, got {other:?}"),
    }
}

#[test]
fn generated_native_filesystem_boundary_cases_match_preemptible_boundary_contract() {
    let cases = lean_native_filesystem_boundary_cases();
    let tool_names = cases
        .iter()
        .map(|case| case.tool_name.as_str())
        .collect::<BTreeSet<_>>();

    assert_eq!(tool_names, BTreeSet::from(["glob", "grep", "list_files"]));
    for case in cases {
        assert!(
            case.name
                .ends_with("_single_poll_blocker_times_out_and_queue_advances"),
            "unexpected native filesystem boundary case name: {}",
            case.name
        );
        assert_eq!(case.work_class, "filesystemTraversal");
        assert_eq!(case.boundary, "managedExecProcessGroupBoundary");
        assert!(case.inner_poll_blocks);
        assert!(case.request_deadline_ms <= 20);
        assert!(case.blocker_ms >= 200);
        assert!(
            case.request_deadline_ms < case.blocker_ms,
            "deadline must be shorter than deterministic blocker"
        );
        assert_eq!(case.expected_terminal, "timedOut");
        assert_eq!(case.expected_failure_class.as_deref(), Some("external"));
        assert!(case.queue_advances_before_blocker_returns);
    }
}

#[tokio::test]
async fn read_file_returns_compact_numbered_contents() {
    let root = temp_root("gents-read-file");
    let file = root.join("notes.txt");
    std::fs::write(&file, "alpha\nbeta\ngamma\n").unwrap();
    let tool = ReadFileTool::new(
        ToolContext::new(root, false).unwrap(),
        DEFAULT_MAX_FILE_CHARS,
    );

    let output = crate::llm::tool::Tool::call(
        &tool,
        ReadFileArgs {
            path: "notes.txt".to_string(),
            start_line: Some(2),
            end_line: Some(3),
            max_chars: DEFAULT_MAX_FILE_CHARS,
            raw_json: false,
        },
    )
    .await
    .unwrap();

    let meta = compact_meta(&output);
    assert_eq!(meta["ok"], true);
    assert_eq!(meta["status"], "success");
    assert_eq!(meta["tool"], "read_file");
    assert_eq!(meta["path"], "notes.txt");
    assert_eq!(meta["start_line"], 2);
    assert_eq!(meta["end_line"], 3);
    assert_eq!(meta["returned_count"], 2);
    assert_eq!(meta["total_count"], 3);
    assert!(output.contains("content:\nL2: beta\nL3: gamma"), "{output}");
}

#[tokio::test]
async fn read_file_reports_truncation_in_compact_metadata() {
    let root = temp_root("gents-read-file-truncate");
    std::fs::write(root.join("notes.txt"), "alpha\nbeta\ngamma\n").unwrap();
    let tool = ReadFileTool::new(ToolContext::new(root, false).unwrap(), 12);

    let output = crate::llm::tool::Tool::call(
        &tool,
        ReadFileArgs {
            path: "notes.txt".to_string(),
            start_line: None,
            end_line: None,
            max_chars: 12,
            raw_json: false,
        },
    )
    .await
    .unwrap();

    let meta = compact_meta(&output);
    assert_eq!(meta["truncated"], true);
    assert_eq!(meta["returned_count"], 3);
    assert_eq!(meta["total_count"], 3);
    assert!(
        output.contains("[Showing lines 1-1 of 3 (28 bytes total)]"),
        "{output}"
    );
}

#[tokio::test]
async fn read_file_rejects_paths_outside_root() {
    let root = temp_root("gents-read-file-rooted");
    std::fs::create_dir_all(root.join("workspace")).unwrap();
    std::fs::write(root.join("outside.txt"), "nope").unwrap();
    std::fs::write(root.join("workspace").join("notes.txt"), "alpha\n").unwrap();
    let tool = ReadFileTool::new(
        ToolContext::new(root.join("workspace"), false).unwrap(),
        DEFAULT_MAX_FILE_CHARS,
    );

    let error = crate::llm::tool::Tool::call(
        &tool,
        ReadFileArgs {
            path: "../outside.txt".to_string(),
            start_line: None,
            end_line: None,
            max_chars: DEFAULT_MAX_FILE_CHARS,
            raw_json: false,
        },
    )
    .await
    .expect_err("path escape should be rejected");

    assert!(
        error.to_string().contains("outside the allowed tool root"),
        "{error}"
    );
}

#[tokio::test]
async fn write_and_edit_file_work_under_root() {
    let root = temp_root("gents-write-edit");
    let context = ToolContext::new(root.clone(), true).unwrap();
    let writer = WriteFileTool::new(context.clone());
    let editor = EditFileTool::new(context);

    let write_output = crate::llm::tool::Tool::call(
        &writer,
        WriteFileArgs {
            path: "nested/file.txt".to_string(),
            content: "hello world".to_string(),
            raw_json: false,
        },
    )
    .await
    .unwrap();
    let write_meta = compact_meta(&write_output);
    assert_eq!(write_meta["tool"], "write_file");
    assert_eq!(write_meta["path"], "nested/file.txt");
    assert_eq!(write_meta["bytes_written"], 11);
    assert_eq!(write_meta["created"], true);
    assert!(write_output.contains("write_file: wrote 11 bytes"));

    let edit_output = crate::llm::tool::Tool::call(
        &editor,
        EditFileArgs {
            path: "nested/file.txt".to_string(),
            old_text: "world".to_string(),
            new_text: "amy".to_string(),
            replace_all: false,
            raw_json: false,
            dry_run: false,
            match_mode: None,
            operation: None,
            expected_content_hash: None,
        },
    )
    .await
    .unwrap();
    let edit_meta = compact_meta(&edit_output);
    assert_eq!(edit_meta["tool"], "edit_file");
    assert_eq!(edit_meta["path"], "nested/file.txt");
    assert_eq!(edit_meta["replacements_applied"], 1);
    assert_eq!(edit_meta["total_count"], 1);
    assert!(edit_output.contains("edit_file: edited nested/file.txt"));

    let content = std::fs::read_to_string(root.join("nested/file.txt")).unwrap();
    assert_eq!(content, "hello amy");
}

#[tokio::test]
async fn read_only_workspace_authority_denies_file_writes() {
    let root = temp_root("gents-workspace-ro-write");
    std::fs::write(root.join("file.txt"), "hello").unwrap();
    let writer = WriteFileTool::new(ToolContext::new(root.clone(), true).unwrap());
    let error =
        crate::tool_call_lifecycle::runtime::scope_request_tool_execution_with_workspace_overlay(
            None,
            tokio_util::sync::CancellationToken::new(),
            crate::tool_call_lifecycle::runtime::ToolWorkspaceScope {
                workspace_cwd: Some(root.clone()),
                workspace_root: Some(std::fs::canonicalize(&root).unwrap()),
                workspace_authority: Some(WorkspaceAuthority::ReadOnly),
            },
            None,
            None,
            None,
            Default::default(),
            false,
            async {
                crate::llm::tool::Tool::call(
                    &writer,
                    WriteFileArgs {
                        path: "file.txt".to_string(),
                        content: "nope".to_string(),
                        raw_json: false,
                    },
                )
                .await
            },
        )
        .await
        .expect_err("ReadOnly workspace authority must deny write_file");
    assert!(
        error.to_string().contains("does not allow file writes"),
        "{error}"
    );
    assert_eq!(
        std::fs::read_to_string(root.join("file.txt")).unwrap(),
        "hello"
    );
}

#[tokio::test]
async fn read_write_overlay_meets_unrestricted_bash_to_workspace_write() {
    let root = temp_root("gents-workspace-rw-bash");
    let policy =
        CommandExecutionPolicy::write_capable().with_mode(CommandExecutionMode::Unrestricted);
    let tool = UnrestrictedBashTool::with_policy(
        ToolContext::new(root.clone(), true).unwrap(),
        Duration::from_secs(5),
        Duration::from_secs(5),
        policy.clone(),
    );
    let result =
        crate::tool_call_lifecycle::runtime::scope_request_tool_execution_with_workspace_overlay(
            None,
            tokio_util::sync::CancellationToken::new(),
            crate::tool_call_lifecycle::runtime::ToolWorkspaceScope {
                workspace_cwd: Some(root.clone()),
                workspace_root: Some(std::fs::canonicalize(&root).unwrap()),
                workspace_authority: Some(WorkspaceAuthority::ReadWrite),
            },
            None,
            None,
            None,
            Default::default(),
            false,
            async {
                let met = crate::toolset::effective_command_policy(&policy);
                assert_eq!(met.mode, CommandExecutionMode::WorkspaceWrite);
                crate::llm::tool::Tool::call(
                    &tool,
                    BashArgs {
                        command: "true".to_string(),
                        args: Vec::new(),
                        cwd: None,
                        timeout_secs: None,
                        raw_json: true,
                    },
                )
                .await
            },
        )
        .await;
    if crate::toolset::workspace_write_sandbox_enforced() {
        let output = result.expect("WorkspaceWrite bash should run when Seatbelt is available");
        let value: serde_json::Value = serde_json::from_str(&output).unwrap_or_else(|_| {
            serde_json::from_str(output.lines().next().unwrap_or(&output))
                .unwrap_or(serde_json::json!({}))
        });
        let mode = value
            .pointer("/execution_mode")
            .or_else(|| value.get("execution_mode"))
            .and_then(|mode| mode.as_str())
            .unwrap_or_default();
        assert!(
            mode.contains("workspace_write") || output.contains("workspace_write"),
            "expected workspace_write metadata, got {output}"
        );
    } else {
        let error = result.expect_err(
            "ReadWrite overlay must refuse Unrestricted bash without WorkspaceWrite sandbox",
        );
        assert!(
            error
                .to_string()
                .contains("workspaceWriteSandboxUnavailable")
                || error.to_string().contains("workspace_write")
                || error.to_string().contains("seatbelt")
                || error.to_string().contains("sandbox"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn raw_json_escape_hatch_returns_structured_output() {
    ensure_native_fs_runner_for_test();
    let root = temp_root("gents-raw-json");
    std::fs::create_dir_all(root.join("src")).unwrap();
    std::fs::write(root.join("src/lib.rs"), "pub fn hi() {}\n").unwrap();
    let tool = ListFilesTool::new(ToolContext::new(root.clone(), false).unwrap(), 100);

    let output = crate::llm::tool::Tool::call(
        &tool,
        ListFilesArgs {
            path: Some(".".to_string()),
            recursive: true,
            max_entries: 100,
            raw_json: true,
        },
    )
    .await
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(value["ok"], true);
    assert_eq!(value["tool"], "list_files");
    assert_eq!(value["returned_count"], 2);
    assert_eq!(value["total_count"], 2);
    let entries = value["entries"].as_array().unwrap();
    assert!(
        entries.iter().any(|entry| entry["path"] == "src/lib.rs"),
        "{output}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn list_files_skips_permission_denied_subtrees() {
    use std::os::unix::fs::PermissionsExt;

    ensure_native_fs_runner_for_test();
    let root = temp_root("gents-list-files-perms");
    std::fs::write(root.join("visible.txt"), "ok").unwrap();
    let restricted = root.join("restricted");
    std::fs::create_dir_all(restricted.join("nested")).unwrap();
    std::fs::write(restricted.join("nested/secret.txt"), "hidden").unwrap();
    std::fs::set_permissions(&restricted, std::fs::Permissions::from_mode(0o000)).unwrap();

    let tool = ListFilesTool::new(ToolContext::new(root.clone(), false).unwrap(), 100);
    let output = crate::llm::tool::Tool::call(
        &tool,
        ListFilesArgs {
            path: Some(".".to_string()),
            recursive: true,
            max_entries: 100,
            raw_json: true,
        },
    )
    .await
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&output).unwrap();
    let entries = value["entries"].as_array().unwrap();
    assert!(
        entries.iter().any(|entry| entry["path"] == "visible.txt"),
        "{output}"
    );

    std::fs::set_permissions(&restricted, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[tokio::test]
async fn list_files_ignores_common_generated_directories_by_default() {
    ensure_native_fs_runner_for_test();
    let root = temp_root("gents-list-files-ignored");
    std::fs::create_dir_all(root.join("src")).unwrap();
    std::fs::create_dir_all(root.join("target/debug")).unwrap();
    std::fs::write(root.join("src/lib.rs"), "pub fn hi() {}\n").unwrap();
    std::fs::write(root.join("target/debug/app"), "compiled").unwrap();
    let tool = ListFilesTool::new(ToolContext::new(root.clone(), false).unwrap(), 100);

    let output = crate::llm::tool::Tool::call(
        &tool,
        ListFilesArgs {
            path: Some(".".to_string()),
            recursive: true,
            max_entries: 100,
            raw_json: false,
        },
    )
    .await
    .unwrap();

    let meta = compact_meta(&output);
    assert_eq!(meta["tool"], "list_files");
    assert_eq!(meta["returned_count"], 2);
    assert_eq!(meta["truncated"], false);
    assert!(output.contains("\ndirectory src"), "{output}");
    assert!(output.contains("\nfile src/lib.rs"), "{output}");
    assert!(!output.contains("\ndirectory target"), "{output}");
    assert!(!output.contains("target/debug/app"), "{output}");
}

#[tokio::test]
async fn glob_returns_compact_matches() {
    ensure_native_fs_runner_for_test();
    let root = temp_root("gents-glob");
    std::fs::create_dir_all(root.join("src")).unwrap();
    std::fs::create_dir_all(root.join("target/debug")).unwrap();
    std::fs::write(root.join("src/main.rs"), "fn main() {}\n").unwrap();
    std::fs::write(root.join("target/debug/main.rs"), "generated\n").unwrap();
    let tool = GlobTool::new(ToolContext::new(root.clone(), false).unwrap(), 100);

    let output = crate::llm::tool::Tool::call(
        &tool,
        GlobArgs {
            pattern: "**/*.rs".to_string(),
            path: Some(".".to_string()),
            max_matches: 100,
            raw_json: false,
        },
    )
    .await
    .unwrap();

    let meta = compact_meta(&output);
    assert_eq!(meta["tool"], "glob");
    assert_eq!(meta["pattern"], "**/*.rs");
    assert_eq!(meta["returned_count"], 1);
    assert_eq!(meta["total_count"], 1);
    assert!(output.contains("\nfile src/main.rs"), "{output}");
    assert!(!output.contains("target/debug/main.rs"), "{output}");
}

#[tokio::test]
async fn grep_returns_compact_line_numbered_matches() {
    ensure_native_fs_runner_for_test();
    let root = temp_root("gents-grep");
    std::fs::create_dir_all(root.join("src")).unwrap();
    std::fs::write(
        root.join("src/main.rs"),
        "fn main() {\n    println!(\"hello\");\n}\n",
    )
    .unwrap();
    let tool = GrepTool::new(ToolContext::new(root.clone(), false).unwrap(), 100);

    let output = crate::llm::tool::Tool::call(
        &tool,
        GrepArgs {
            pattern: "println".to_string(),
            path: Some(".".to_string()),
            case_sensitive: true,
            max_matches: 100,
            raw_json: false,
        },
    )
    .await
    .unwrap();

    let meta = compact_meta(&output);
    assert_eq!(meta["tool"], "grep");
    assert_eq!(meta["files_with_matches"], 1);
    assert_eq!(meta["returned_count"], 1);
    assert!(
        output.contains("src/main.rs:L2:     println!(\"hello\");"),
        "{output}"
    );
}

#[tokio::test]
async fn compact_list_output_is_smaller_than_representative_pretty_json() {
    ensure_native_fs_runner_for_test();
    let root = temp_root("gents-list-files-smaller");
    std::fs::create_dir_all(root.join("src")).unwrap();
    std::fs::write(root.join("README.md"), "hello\n").unwrap();
    std::fs::write(root.join("src/lib.rs"), "pub fn hi() {}\n").unwrap();
    let tool = ListFilesTool::new(ToolContext::new(root, false).unwrap(), 100);

    let output = crate::llm::tool::Tool::call(
        &tool,
        ListFilesArgs {
            path: Some(".".to_string()),
            recursive: true,
            max_entries: 100,
            raw_json: false,
        },
    )
    .await
    .unwrap();
    let old_pretty_json = serde_json::to_string_pretty(&serde_json::json!({
        "path": ".",
        "recursive": true,
        "returned_entries": 3,
        "truncated": false,
        "default_ignored": [".cache", ".direnv", ".git", ".next", ".turbo", ".venv", "dist", "node_modules", "target", "venv"],
        "summary": {
            "files": 2,
            "directories": 1
        },
        "entries": [
            {"path": "README.md", "entry_type": "file"},
            {"path": "src", "entry_type": "directory"},
            {"path": "src/lib.rs", "entry_type": "file"}
        ]
    }))
    .unwrap();

    assert!(
        output.len() < old_pretty_json.len(),
        "compact output should be smaller than old pretty JSON\ncompact={}\nold={}",
        output.len(),
        old_pretty_json.len()
    );
}

#[test]
fn read_only_bash_rejects_write_commands() {
    assert!(validate_read_only_command(
        "git",
        &[String::from("commit")],
        &default_read_only_commands()
    )
    .is_err());
}

#[test]
fn shell_environment_filters_secrets_and_forces_noninteractive_values() {
    let env = build_shell_env_from_vars([
        ("PATH".to_string(), "/custom/bin".to_string()),
        ("HOME".to_string(), "/tmp/home".to_string()),
        ("OPENAI_API_KEY".to_string(), "secret".to_string()),
        ("SESSION_TOKEN".to_string(), "secret".to_string()),
        ("DATABASE_SECRET".to_string(), "secret".to_string()),
        ("UNRELATED".to_string(), "drop".to_string()),
        ("PAGER".to_string(), "less".to_string()),
        ("TERM".to_string(), "xterm-256color".to_string()),
    ]);

    assert_eq!(env.get("PATH").map(String::as_str), Some("/custom/bin"));
    assert_eq!(env.get("HOME").map(String::as_str), Some("/tmp/home"));
    assert!(!env.contains_key("OPENAI_API_KEY"));
    assert!(!env.contains_key("SESSION_TOKEN"));
    assert!(!env.contains_key("DATABASE_SECRET"));
    assert!(!env.contains_key("UNRELATED"));
    assert_eq!(env.get("PAGER").map(String::as_str), Some("cat"));
    assert_eq!(env.get("GIT_PAGER").map(String::as_str), Some("cat"));
    assert_eq!(env.get("NO_COLOR").map(String::as_str), Some("1"));
    assert_eq!(env.get("CLICOLOR").map(String::as_str), Some("0"));
    assert_eq!(env.get("TERM").map(String::as_str), Some("dumb"));
}

#[test]
fn read_only_bash_rejects_codex_style_unsafe_flags() {
    let allowlist = default_read_only_commands();
    assert!(validate_read_only_command(
        "git",
        &[
            String::from("status"),
            String::from("--output=/tmp/git-status.txt")
        ],
        &allowlist,
    )
    .is_err());
    assert!(validate_read_only_command(
        "git",
        &[
            String::from("-C"),
            String::from("."),
            String::from("status")
        ],
        &allowlist,
    )
    .is_err());
    assert!(validate_read_only_command(
        "rg",
        &[String::from("--pre"), String::from("touch /tmp/nope")],
        &allowlist,
    )
    .is_err());
    assert!(validate_read_only_command(
        "find",
        &[
            String::from("."),
            String::from("-fprint0"),
            String::from("out")
        ],
        &allowlist,
    )
    .is_err());
    assert!(validate_read_only_command(
        "sed",
        &[
            String::from("--in-place=.bak"),
            String::from("s/a/b/g"),
            String::from("README.md"),
        ],
        &allowlist,
    )
    .is_err());
}

#[test]
fn command_policy_applies_forbidden_and_allowed_prefixes() {
    let policy = CommandExecutionPolicy::read_only(vec!["git".to_string()])
        .with_allowed_argv_prefixes(vec![vec!["git".to_string(), "status".to_string()]])
        .with_forbidden_argv_prefixes(vec![vec![
            "git".to_string(),
            "status".to_string(),
            "--short".to_string(),
        ]]);

    validate_command_policy("git", &[String::from("status")], &policy).unwrap();
    assert!(validate_command_policy("git", &[String::from("diff")], &policy).is_err());
    assert!(validate_command_policy(
        "git",
        &[String::from("status"), String::from("--short")],
        &policy
    )
    .is_err());
}

#[test]
fn deny_all_argv_policy_rejects_every_command() {
    // An effective `Only(∅)` allowed scope projects to deny_all_argv, which must
    // reject every command — an EMPTY allowed_argv_prefixes would instead mean
    // allow-all (the `Only(∅) ≠ All` trap). The allowed list stays empty here, so
    // this proves the sentinel (not the list) is what enforces deny-all.
    let policy = CommandExecutionPolicy::write_capable().with_deny_all_argv(true);
    assert!(policy.allowed_argv_prefixes.is_empty());
    assert!(validate_command_policy("ls", &[], &policy).is_err());
    assert!(validate_command_policy("git", &[String::from("status")], &policy).is_err());
    assert!(validate_command_policy("echo", &[String::from("hi")], &policy).is_err());
}

#[test]
fn read_only_policy_allows_operator_configured_diagnostic_prefix() {
    let policy = CommandExecutionPolicy::read_only(default_read_only_commands())
        .with_allowed_argv_prefixes(vec![vec![
            "spctl".to_string(),
            "--assess".to_string(),
            "--type".to_string(),
            "execute".to_string(),
        ]]);

    validate_command_policy(
        "spctl",
        &[
            String::from("--assess"),
            String::from("--type"),
            String::from("execute"),
            String::from("/Applications/Gents.app"),
        ],
        &policy,
    )
    .unwrap();
    assert!(validate_command_policy(
        "spctl",
        &[String::from("--assess"), String::from("--raw")],
        &policy,
    )
    .is_err());
}

/// Fence the operator docs in `docs/macos-bash-sandbox.md` (#629): the two
/// ToolSelection knobs are not aliases — prefixes gate/extend by argv; the
/// allowlist field replaces the whole-executable base.
#[test]
fn read_only_allowlist_knobs_match_operator_docs() {
    let defaults = default_read_only_commands();

    // Base defaults admit built-in heads; unknown heads are denied.
    let default_policy = CommandExecutionPolicy::read_only(defaults.clone());
    validate_command_policy("date", &[], &default_policy).unwrap();
    assert!(
        validate_command_policy("journalctl", &[], &default_policy).is_err(),
        "unknown head must not pass the default base allowlist"
    );

    // read_only_command_allowlist REPLACES the base (narrow / customize).
    let narrowed = CommandExecutionPolicy::read_only(vec![
        "ls".to_string(),
        "cat".to_string(),
        "git".to_string(),
        "journalctl".to_string(),
    ]);
    validate_command_policy("cat", &[], &narrowed).unwrap();
    validate_command_policy("journalctl", &[], &narrowed).unwrap();
    assert!(
        validate_command_policy("date", &[], &narrowed).is_err(),
        "replace/narrow must drop unlisted default heads (e.g. date)"
    );
    assert!(
        validate_command_policy("sudo", &[], &narrowed).is_err(),
        "replace/narrow must drop unlisted default heads (e.g. sudo)"
    );

    // Non-empty command_allowed_argv_prefixes is a global gate: a single
    // diagnostic prefix admits matching argv outside the base, but also
    // blocks default heads that do not match any prefix.
    let prefix_only =
        CommandExecutionPolicy::read_only(defaults).with_allowed_argv_prefixes(vec![vec![
            "spctl".to_string(),
            "--assess".to_string(),
            "--type".to_string(),
            "execute".to_string(),
        ]]);
    validate_command_policy(
        "spctl",
        &[
            String::from("--assess"),
            String::from("--type"),
            String::from("execute"),
            String::from("/Applications/Gents.app"),
        ],
        &prefix_only,
    )
    .unwrap();
    assert!(
        validate_command_policy("date", &[], &prefix_only).is_err(),
        "non-empty allowed prefixes must gate out default heads that do not match (docs global-gate caveat)"
    );
}

#[test]
fn read_only_policy_forbidden_prefix_overrides_configured_diagnostic_prefix() {
    let policy = CommandExecutionPolicy::read_only(default_read_only_commands())
        .with_allowed_argv_prefixes(vec![vec!["spctl".to_string(), "--assess".to_string()]])
        .with_forbidden_argv_prefixes(vec![vec![
            "spctl".to_string(),
            "--assess".to_string(),
            "--raw".to_string(),
        ]]);

    assert!(validate_command_policy(
        "spctl",
        &[
            String::from("--assess"),
            String::from("--raw"),
            String::from("/Applications/Gents.app"),
        ],
        &policy,
    )
    .is_err());
}

#[test]
fn generated_command_policy_cases_match_rust_validation() {
    for case in lean_command_policy_cases() {
        let mode = rust_command_execution_mode(&case.mode);
        let network_mode = rust_command_network_mode(&case.network_mode);
        let lookup_command = std::path::Path::new(&case.command)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(&case.command);
        assert_eq!(
            lookup_command, case.lookup_command,
            "Lean command policy case {} must use Rust basename lookup",
            case.name
        );

        let policy = CommandExecutionPolicy::read_only(case.read_only_allowlist.clone())
            .with_mode(mode)
            .with_allowed_argv_prefixes(case.allowed_argv_prefixes.clone())
            .with_forbidden_argv_prefixes(case.forbidden_argv_prefixes.clone())
            .with_network_mode(network_mode);
        let result = validate_command_policy(&case.command, &case.args, &policy);

        match case.decision.as_str() {
            "allow" => assert!(
                result.is_ok(),
                "Lean CommandPolicy case {} should allow but Rust denied: {:?}",
                case.name,
                result.err()
            ),
            "deny" => {
                let error = result.unwrap_err();
                assert_command_denial_matches(case, &error);
            }
            other => panic!(
                "Lean CommandPolicy case {} emitted unknown decision {other:?}",
                case.name
            ),
        }
    }
}

#[test]
fn generated_command_policy_cases_cover_read_only_safety_matrix() {
    let emitted = lean_command_policy_cases()
        .iter()
        .map(|case| case.name.as_str())
        .collect::<BTreeSet<_>>();
    let expected = [
        "read_only_git_status_allows",
        "read_only_git_branch_list_allows",
        "read_only_git_global_config_denies",
        "read_only_git_config_env_denies",
        "read_only_git_output_flag_denies",
        "read_only_git_exec_flag_denies",
        "read_only_git_commit_subcommand_denies",
        "read_only_git_branch_delete_denies",
        "read_only_sed_print_allows",
        "read_only_sed_in_place_short_denies",
        "read_only_sed_in_place_long_denies",
        "read_only_sed_in_place_suffix_denies",
        "read_only_find_type_file_allows",
        "read_only_find_delete_denies",
        "read_only_find_exec_denies",
        "read_only_find_fprint_denies",
        "read_only_rg_search_allows",
        "read_only_rg_pre_denies",
        "read_only_rg_search_zip_denies",
        "read_only_curl_http_get_allows",
        "read_only_curl_post_denies",
        "read_only_curl_data_denies",
        "read_only_curl_output_denies",
        "read_only_curl_upload_denies",
        "read_only_curl_config_denies",
        "read_only_curl_missing_http_url_denies",
        "read_only_launchctl_print_allows",
        "read_only_launchctl_bootout_denies",
        "read_only_launchctl_missing_subcommand_denies",
        "read_only_tailscale_status_allows",
        "read_only_tailscale_netcheck_allows",
        "read_only_tailscale_up_denies",
        "read_only_sudo_launchctl_print_allows",
        "read_only_sudo_launchctl_wrong_path_denies",
        "read_only_sudo_rm_denies",
        "read_only_sudo_missing_command_denies",
    ];

    for name in expected {
        assert!(
            emitted.contains(name),
            "Lean CommandPolicy contract must emit read-only safety case {name}"
        );
    }

    for name in [
        "read_only_git_status_allows",
        "read_only_git_branch_list_allows",
        "read_only_sed_print_allows",
        "read_only_find_type_file_allows",
        "read_only_rg_search_allows",
        "read_only_curl_http_get_allows",
        "read_only_launchctl_print_allows",
        "read_only_tailscale_status_allows",
        "read_only_tailscale_netcheck_allows",
        "read_only_sudo_launchctl_print_allows",
    ] {
        let case = lean_command_policy_case(name);
        assert_eq!(case.decision, "allow");
        assert_eq!(case.denial_reason, None);
    }

    for (name, argument) in [
        ("read_only_git_global_config_denies", "-c"),
        (
            "read_only_git_config_env_denies",
            "--config-env=GIT_CONFIG_GLOBAL=ENV_FILE",
        ),
        (
            "read_only_git_output_flag_denies",
            "--output=/tmp/gents-diff.txt",
        ),
        (
            "read_only_git_exec_flag_denies",
            "--exec=touch /tmp/gents-nope",
        ),
        ("read_only_git_branch_delete_denies", "-D"),
        ("read_only_sed_in_place_short_denies", "-i"),
        ("read_only_sed_in_place_long_denies", "--in-place"),
        ("read_only_sed_in_place_suffix_denies", "--in-place=.bak"),
        ("read_only_find_delete_denies", "-delete"),
        ("read_only_find_exec_denies", "-exec"),
        ("read_only_find_fprint_denies", "-fprint0"),
        ("read_only_rg_pre_denies", "--pre"),
        ("read_only_rg_search_zip_denies", "--search-zip"),
        ("read_only_curl_post_denies", "-X"),
        ("read_only_curl_data_denies", "--data={}"),
        ("read_only_curl_output_denies", "-o"),
        ("read_only_curl_upload_denies", "-T"),
        ("read_only_curl_config_denies", "-K"),
        (
            "read_only_sudo_launchctl_wrong_path_denies",
            "/usr/bin/launchctl",
        ),
    ] {
        let case = lean_command_policy_case(name);
        assert_eq!(case.decision, "deny");
        assert_eq!(
            case.denial_reason.as_deref(),
            Some("readOnlyArgumentNotAllowed")
        );
        assert_eq!(case.denied_argument.as_deref(), Some(argument));
    }

    for (name, subcommand) in [
        ("read_only_git_commit_subcommand_denies", "commit"),
        ("read_only_launchctl_bootout_denies", "bootout"),
        ("read_only_tailscale_up_denies", "up"),
        ("read_only_sudo_rm_denies", "rm"),
    ] {
        let case = lean_command_policy_case(name);
        assert_eq!(case.decision, "deny");
        assert_eq!(
            case.denial_reason.as_deref(),
            Some("readOnlySubcommandNotAllowlisted")
        );
        assert_eq!(case.denied_subcommand.as_deref(), Some(subcommand));
    }

    for name in [
        "read_only_launchctl_missing_subcommand_denies",
        "read_only_sudo_missing_command_denies",
    ] {
        let case = lean_command_policy_case(name);
        assert_eq!(case.decision, "deny");
        assert_eq!(
            case.denial_reason.as_deref(),
            Some("readOnlySubcommandRequired")
        );
    }

    let missing_url = lean_command_policy_case("read_only_curl_missing_http_url_denies");
    assert_eq!(missing_url.decision, "deny");
    assert_eq!(
        missing_url.denial_reason.as_deref(),
        Some("readOnlyUrlRequired")
    );
    assert_eq!(missing_url.denied_command.as_deref(), Some("curl"));
}

#[test]
fn generated_command_sandbox_cases_match_rust_selection() {
    for case in lean_command_sandbox_cases() {
        let mode = rust_command_execution_mode(&case.mode);
        let result = select_sandbox_for_policy(mode, case.workspace_write_sandbox_enforced);

        match case.decision.as_str() {
            "selected" => assert_eq!(
                result.unwrap(),
                case.sandbox.as_deref().unwrap(),
                "Lean CommandPolicy sandbox case {} must match Rust sandbox label",
                case.name
            ),
            "denied" => {
                let error = result.unwrap_err().to_string();
                assert_eq!(
                    case.denial_reason.as_deref(),
                    Some("workspaceWriteSandboxUnavailable"),
                    "Lean CommandPolicy sandbox case {} denied for unexpected reason",
                    case.name
                );
                assert!(
                    error.contains("workspace_write") || error.contains("sandbox-exec"),
                    "Lean CommandPolicy sandbox case {} expected workspace_write denial, got: {error}",
                    case.name
                );
            }
            other => panic!(
                "Lean CommandPolicy sandbox case {} emitted unknown decision {other:?}",
                case.name
            ),
        }
    }
}

#[test]
fn generated_command_env_cases_match_rust_filtering() {
    for case in lean_command_env_cases() {
        let vars = if case.input_present {
            vec![(case.input_name.clone(), case.input_value.clone())]
        } else {
            Vec::new()
        };
        let env = build_shell_env_from_vars(vars);
        let actual = env.get(&case.output_name).map(String::as_str);

        assert_eq!(
            actual,
            case.expected_output_value.as_deref(),
            "Lean CommandPolicy env case {} ({}) must match Rust shell env filtering; expected kind {:?}",
            case.name,
            case.env_key,
            case.expected_value_kind
        );
    }
}

fn rust_command_execution_mode(value: &str) -> CommandExecutionMode {
    CommandExecutionMode::parse(value)
        .unwrap_or_else(|error| panic!("unknown Lean command execution mode {value:?}: {error}"))
}

fn rust_command_network_mode(value: &str) -> CommandNetworkMode {
    CommandNetworkMode::parse(value)
        .unwrap_or_else(|error| panic!("unknown Lean command network mode {value:?}: {error}"))
}

fn assert_command_denial_matches(case: &LeanCommandPolicyCase, error: &ToolError) {
    let denial = error.command_policy_denial().unwrap_or_else(|| {
        panic!(
            "Lean CommandPolicy case {} denied with unstructured Rust error: {error}",
            case.name
        )
    });
    assert_eq!(
        Some(denial.to_contract()),
        case.denial_reason.as_deref(),
        "Lean CommandPolicy case {} emitted a different denial reason",
        case.name
    );
    assert_eq!(
        denial.reason.matched_prefix(),
        case.matched_prefix.as_deref(),
        "Lean CommandPolicy case {} matched_prefix drifted",
        case.name
    );
    assert_eq!(
        denial.reason.denied_argv(),
        case.denied_argv.as_deref(),
        "Lean CommandPolicy case {} denied_argv drifted",
        case.name
    );
    assert_eq!(
        denial.reason.denied_command(),
        case.denied_command.as_deref(),
        "Lean CommandPolicy case {} denied_command drifted",
        case.name
    );
    assert_eq!(
        denial.reason.denied_argument(),
        case.denied_argument.as_deref(),
        "Lean CommandPolicy case {} denied_argument drifted",
        case.name
    );
    assert_eq!(
        denial.reason.denied_subcommand(),
        case.denied_subcommand.as_deref(),
        "Lean CommandPolicy case {} denied_subcommand drifted",
        case.name
    );
    assert_eq!(denial.policy_mode, case.mode);
    assert_eq!(denial.policy_network, case.network_mode);
}

#[test]
fn command_policy_disabled_network_fails_closed_when_not_enforced() {
    let read_only = CommandExecutionPolicy::read_only(vec!["curl".to_string()])
        .with_network_mode(CommandNetworkMode::Disabled);
    assert!(
        validate_command_policy("curl", &[String::from("https://example.com")], &read_only)
            .is_err()
    );

    let unrestricted = CommandExecutionPolicy::write_capable()
        .with_mode(CommandExecutionMode::Unrestricted)
        .with_network_mode(CommandNetworkMode::Disabled);
    assert!(validate_command_policy("printf", &[String::from("ok")], &unrestricted).is_err());
}

#[test]
fn managed_write_policy_spelling_is_workspace_write_alias() {
    assert_eq!(
        CommandExecutionMode::parse("managed_write").unwrap(),
        CommandExecutionMode::WorkspaceWrite
    );
}

#[cfg(unix)]
#[tokio::test]
async fn unrestricted_bash_runs_shell_command_strings() {
    let root = temp_root("gents-unrestricted-shell");
    let tool = UnrestrictedBashTool::new(
        ToolContext::new(root, false).unwrap(),
        Duration::from_secs(DEFAULT_COMMAND_TIMEOUT_SECS),
    );

    let output = crate::llm::tool::Tool::call(
        &tool,
        BashArgs {
            command: "printf OK && printf ERR >&2".to_string(),
            args: Vec::new(),
            cwd: None,
            timeout_secs: Some(DEFAULT_COMMAND_TIMEOUT_SECS),
            raw_json: false,
        },
    )
    .await
    .unwrap();

    let meta = compact_exec_meta(&output);
    assert_eq!(meta["ok"], true);
    assert_eq!(meta["exit_code"], 0);
    assert_eq!(meta["timed_out"], false);
    assert_eq!(meta["stdout_capture_incomplete"], false);
    assert_eq!(meta["stderr_capture_incomplete"], false);
    assert_eq!(meta["argv"][0], "/bin/sh");
    assert_eq!(meta["stdout_truncation"]["total_bytes"], 2);
    assert_eq!(meta["stderr_truncation"]["total_bytes"], 3);
    assert!(output.contains("stdout:\nOK"));
    assert!(output.contains("stderr:\nERR"));
}

#[cfg(unix)]
#[tokio::test]
async fn unrestricted_bash_reports_descendant_bounded_capture() {
    let root = temp_root("gents-unrestricted-capture-drain");
    let tool = UnrestrictedBashTool::new(
        ToolContext::new(root, false).unwrap(),
        Duration::from_secs(DEFAULT_COMMAND_TIMEOUT_SECS),
    );

    let output = crate::llm::tool::Tool::call(
        &tool,
        BashArgs {
            command: "printf ERR >&2; sleep 2 >/dev/null & exit 0".to_string(),
            args: Vec::new(),
            cwd: None,
            timeout_secs: Some(DEFAULT_COMMAND_TIMEOUT_SECS),
            raw_json: false,
        },
    )
    .await
    .unwrap();

    let meta = compact_exec_meta(&output);
    assert_eq!(meta["ok"], true);
    assert_eq!(meta["exit_code"], 0);
    assert_eq!(meta["stdout_capture_incomplete"], false);
    assert_eq!(meta["stderr_capture_incomplete"], true);
    assert!(output.contains("stderr:\nERR"));
}

#[tokio::test]
async fn command_policy_explicit_unrestricted_reports_unsandboxed_metadata() {
    let root = temp_root("gents-unrestricted-policy");
    let policy =
        CommandExecutionPolicy::write_capable().with_mode(CommandExecutionMode::Unrestricted);
    let tool = UnrestrictedBashTool::with_policy(
        ToolContext::new(root, false).unwrap(),
        Duration::from_secs(DEFAULT_COMMAND_TIMEOUT_SECS),
        Duration::from_secs(DEFAULT_COMMAND_TIMEOUT_SECS),
        policy,
    );

    let output = crate::llm::tool::Tool::call(
        &tool,
        BashArgs {
            command: "printf".to_string(),
            args: vec!["ok".to_string()],
            cwd: None,
            timeout_secs: Some(DEFAULT_COMMAND_TIMEOUT_SECS),
            raw_json: false,
        },
    )
    .await
    .unwrap();

    let meta = compact_exec_meta(&output);
    assert_eq!(meta["ok"], true);
    assert_eq!(meta["execution_mode"], "unrestricted");
    assert_eq!(meta["sandbox"], "unsandboxed_unrestricted");
    assert_eq!(meta["stdout_truncation"]["total_bytes"], 2);
}

#[tokio::test]
async fn bash_output_supports_raw_json_escape_hatch() {
    let root = temp_root("gents-bash-raw-json");
    let tool = ReadOnlyBashTool::new(
        ToolContext::new(root, false).unwrap(),
        Duration::from_secs(DEFAULT_COMMAND_TIMEOUT_SECS),
        vec!["printf".to_string()],
    );

    let output = crate::llm::tool::Tool::call(
        &tool,
        BashArgs {
            command: "printf".to_string(),
            args: vec!["json".to_string()],
            cwd: None,
            timeout_secs: Some(DEFAULT_COMMAND_TIMEOUT_SECS),
            raw_json: true,
        },
    )
    .await
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(value["ok"], true);
    assert_eq!(value["command"], "printf json");
    assert_eq!(value["stdout"], "json");
    assert_eq!(value["stderr"], "");
}

#[tokio::test]
async fn bash_nonzero_exit_is_a_typed_tool_failure_with_metadata() {
    let root = temp_root("gents-bash-nonzero");
    let tool = ReadOnlyBashTool::new(
        ToolContext::new(root, false).unwrap(),
        Duration::from_secs(DEFAULT_COMMAND_TIMEOUT_SECS),
        vec!["false".to_string()],
    );
    let boxed: Box<dyn crate::llm::tool::ToolDyn> = Box::new(tool);

    let outcome = crate::tool_call_lifecycle::runtime::call_tool_managed(
        boxed.as_ref(),
        serde_json::json!({ "command": "false" }).to_string(),
    )
    .await;

    match outcome {
        crate::tool_call_lifecycle::ToolOutcome::Failed { class, text, .. } => {
            assert_eq!(
                class,
                crate::tool_call_lifecycle::FailureClass::ToolReturnedError
            );
            let meta = compact_exec_meta(&text);
            assert_eq!(meta["ok"], false);
            assert_eq!(meta["status"], "exit_nonzero");
            assert_ne!(meta["exit_code"], 0);
        }
        other => panic!("nonzero command must be a typed failure, got {other:?}"),
    }
}

#[tokio::test]
async fn bash_per_call_timeout_is_a_recoverable_typed_failure_with_metadata() {
    let root = temp_root("gents-bash-timeout");
    let tool = ReadOnlyBashTool::new(
        ToolContext::new(root, false).unwrap(),
        Duration::from_secs(1),
        vec!["sleep".to_string()],
    );

    let boxed: Box<dyn crate::llm::tool::ToolDyn> = Box::new(tool);
    let outcome = crate::tool_call_lifecycle::runtime::call_tool_managed(
        boxed.as_ref(),
        serde_json::json!({
            "command": "sleep",
            "args": ["2"],
            "timeout_secs": 1,
        })
        .to_string(),
    )
    .await;

    match outcome {
        crate::tool_call_lifecycle::ToolOutcome::Failed { class, text, .. } => {
            assert_eq!(class, crate::tool_call_lifecycle::FailureClass::External);
            let meta = compact_exec_meta(&text);
            assert_eq!(meta["ok"], false);
            assert_eq!(meta["status"], "timeout");
            assert_eq!(meta["timed_out"], true);
            assert!(meta["exit_code"].is_null());
        }
        other => panic!("per-call timeout must be a recoverable typed failure, got {other:?}"),
    }
}

#[cfg(unix)]
#[tokio::test]
async fn bash_request_deadline_resolves_to_typed_timeout() {
    let root = temp_root("gents-bash-request-deadline");
    let tool = ReadOnlyBashTool::new(
        ToolContext::new(root, false).unwrap(),
        Duration::from_secs(5),
        vec!["sleep".to_string()],
    );
    let deadline = chrono::Utc::now() + chrono::Duration::milliseconds(100);

    let boxed: Box<dyn crate::llm::tool::ToolDyn> = Box::new(tool);
    let args = serde_json::to_string(&serde_json::json!({
        "command": "sleep",
        "args": ["2"],
        "timeout_secs": 5,
    }))
    .unwrap();
    let outcome = crate::tool_call_lifecycle::runtime::scope_request_tool_execution(
        Some(deadline),
        tokio_util::sync::CancellationToken::new(),
        crate::tool_call_lifecycle::runtime::call_tool_managed(boxed.as_ref(), args),
    )
    .await;

    assert!(
        matches!(
            outcome,
            crate::tool_call_lifecycle::ToolOutcome::TimedOut { .. }
        ),
        "bash exceeding the request deadline must resolve to a typed timeout, got {outcome:?}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn unrestricted_bash_timeout_kills_descendants_and_returns_promptly() {
    let root = temp_root("gents-bash-process-tree-timeout");
    let pid_file = root.join("descendant.pid");
    let tool = UnrestrictedBashTool::with_policy(
        ToolContext::new(root, false).unwrap(),
        Duration::from_secs(1),
        Duration::from_secs(1),
        CommandExecutionPolicy::write_capable().with_mode(CommandExecutionMode::Unrestricted),
    );
    let command =
        "trap '' TERM; while :; do sleep 1; done & child=$!; printf '%s' \"$child\" > descendant.pid; wait";

    let boxed: Box<dyn crate::llm::tool::ToolDyn> = Box::new(tool);
    let call = crate::tool_call_lifecycle::runtime::call_tool_managed(
        boxed.as_ref(),
        serde_json::json!({
            "command": command,
            "timeout_secs": 1,
        })
        .to_string(),
    );
    let outcome = match tokio::time::timeout(Duration::from_secs(4), call).await {
        Ok(outcome) => outcome,
        Err(_) => {
            if let Ok(pid) =
                std::fs::read_to_string(&pid_file).map(|value| value.trim().parse::<i32>().unwrap())
            {
                unsafe {
                    libc::kill(pid, libc::SIGKILL);
                }
            }
            panic!("bash timeout hung while a descendant held its output pipes open");
        }
    };
    let crate::tool_call_lifecycle::ToolOutcome::Failed {
        class: crate::tool_call_lifecycle::FailureClass::External,
        text: output,
        ..
    } = outcome
    else {
        panic!("background process-tree timeout must be typed failed, got {outcome:?}");
    };

    let meta = compact_exec_meta(&output);
    assert_eq!(meta["status"], "timeout");
    assert_eq!(meta["timed_out"], true);
    let descendant_pid = std::fs::read_to_string(&pid_file)
        .unwrap()
        .trim()
        .parse::<i32>()
        .unwrap();
    assert_unix_process_exited(descendant_pid).await;
}

#[cfg(unix)]
async fn assert_unix_process_exited(pid: i32) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let result = unsafe { libc::kill(pid, 0) };
        if result == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
            return;
        }
        if Instant::now() >= deadline {
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
            panic!("managed bash descendant process {pid} survived timeout");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn workspace_write_bash_contains_writes_to_tool_root() {
    if !std::path::Path::new("/usr/bin/sandbox-exec").exists() {
        return;
    }

    let root = temp_root("gents-bash-seatbelt");
    let outside = std::env::temp_dir().join(format!(
        "gents-bash-seatbelt-outside-{}",
        uuid::Uuid::new_v4()
    ));
    let tool = UnrestrictedBashTool::new(
        ToolContext::new(root.clone(), false).unwrap(),
        Duration::from_secs(DEFAULT_COMMAND_TIMEOUT_SECS),
    );
    let shell = format!(
        "printf inside > inside.txt; printf outside > {}",
        outside.display()
    );

    let boxed: Box<dyn crate::llm::tool::ToolDyn> = Box::new(tool);
    let outcome = crate::tool_call_lifecycle::runtime::call_tool_managed(
        boxed.as_ref(),
        serde_json::json!({
            "command": shell,
            "timeout_secs": DEFAULT_COMMAND_TIMEOUT_SECS,
        })
        .to_string(),
    )
    .await;
    let crate::tool_call_lifecycle::ToolOutcome::Failed {
        class: crate::tool_call_lifecycle::FailureClass::ToolReturnedError,
        text: output,
        ..
    } = outcome
    else {
        panic!("seatbelt denial must be typed failed, got {outcome:?}");
    };

    let meta = compact_exec_meta(&output);
    assert_eq!(meta["sandbox"], "macos_seatbelt");
    assert_ne!(meta["exit_code"].as_i64(), Some(0));
    assert_eq!(
        std::fs::read_to_string(root.join("inside.txt")).unwrap(),
        "inside"
    );
    assert!(!outside.exists(), "sandbox should deny writes outside root");
}

#[test]
fn read_only_bash_allows_host_diagnostics_commands() {
    let allowlist = default_read_only_commands();
    assert!(validate_read_only_command("date", &[String::from("-u")], &allowlist).is_ok());
    assert!(validate_read_only_command(
        "launchctl",
        &[
            String::from("print"),
            String::from("system/com.amygdala.alloy.mini-1")
        ],
        &allowlist
    )
    .is_ok());
    assert!(validate_read_only_command(
        "sudo",
        &[
            String::from("/bin/launchctl"),
            String::from("print"),
            String::from("system/com.amygdala.alloy.mini-1"),
        ],
        &allowlist
    )
    .is_ok());
    assert!(validate_read_only_command(
        "curl",
        &[
            String::from("-fsS"),
            String::from("http://127.0.0.1:9100/metrics"),
        ],
        &allowlist
    )
    .is_ok());
    assert!(validate_read_only_command("tailscale", &[String::from("status")], &allowlist).is_ok());
}

#[test]
fn read_only_bash_rejects_mutating_host_diagnostics_commands() {
    let allowlist = default_read_only_commands();
    assert!(validate_read_only_command(
        "launchctl",
        &[String::from("bootout"), String::from("system/com.example")],
        &allowlist
    )
    .is_err());
    assert!(validate_read_only_command(
        "sudo",
        &[
            String::from("/bin/launchctl"),
            String::from("kickstart"),
            String::from("system/com.example"),
        ],
        &allowlist
    )
    .is_err());
    assert!(validate_read_only_command(
        "curl",
        &[
            String::from("-X"),
            String::from("POST"),
            String::from("http://127.0.0.1:9191/api/v0/graphql"),
        ],
        &allowlist
    )
    .is_err());
    assert!(validate_read_only_command("tailscale", &[String::from("up")], &allowlist).is_err());
}

// --- #738/#724: edit_file match ladder, dry-run, and content-hash wiring ---

fn edit_args(path: &str, old: &str, new: &str) -> EditFileArgs {
    EditFileArgs {
        path: path.to_string(),
        old_text: old.to_string(),
        new_text: new.to_string(),
        replace_all: false,
        raw_json: true,
        dry_run: false,
        match_mode: None,
        operation: None,
        expected_content_hash: None,
    }
}

#[tokio::test]
async fn read_file_reports_raw_content_hash() {
    let root = temp_root("gents-read-hash");
    std::fs::write(root.join("config.json"), "{\"max_turns\": 20}\n").unwrap();
    let tool = ReadFileTool::new(
        ToolContext::new(root, false).unwrap(),
        DEFAULT_MAX_FILE_CHARS,
    );
    let output = crate::llm::tool::Tool::call(
        &tool,
        ReadFileArgs {
            path: "config.json".to_string(),
            start_line: None,
            end_line: None,
            max_chars: DEFAULT_MAX_FILE_CHARS,
            raw_json: true,
        },
    )
    .await
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&output).unwrap();
    let hash = value["content_hash"].as_str().unwrap();
    assert!(hash.starts_with("sha256:"), "{hash}");
    assert_eq!(hash.len(), "sha256:".len() + 64);
}

#[tokio::test]
async fn edit_file_dry_run_previews_diff_without_writing() {
    let root = temp_root("gents-edit-dry-run");
    let file = root.join("config.json");
    std::fs::write(&file, "{\n  \"max_turns\": 20\n}\n").unwrap();
    let tool = EditFileTool::new(ToolContext::new(root, false).unwrap());
    let mut args = edit_args("config.json", "\"max_turns\": 20", "\"max_turns\": 250");
    args.dry_run = true;
    let output = crate::llm::tool::Tool::call(&tool, args).await.unwrap();
    let value: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(value["dry_run"], true, "{value}");
    assert!(
        value["diff"].as_str().unwrap().contains("max_turns"),
        "{value}"
    );
    // Nothing was written.
    assert_eq!(
        std::fs::read_to_string(&file).unwrap(),
        "{\n  \"max_turns\": 20\n}\n"
    );
}

#[tokio::test]
async fn edit_file_stale_hash_rejects_before_matching_and_reports_current() {
    let root = temp_root("gents-edit-stale");
    let file = root.join("a.txt");
    // Pattern is ambiguous — but the stale gate must fire FIRST (Lean E6).
    std::fs::write(&file, "dup\ndup\n").unwrap();
    let tool = EditFileTool::new(ToolContext::new(root, false).unwrap());
    let mut args = edit_args("a.txt", "dup", "x");
    args.expected_content_hash =
        Some("sha256:0000000000000000000000000000000000000000000000000000000000000000".to_string());
    let err = crate::llm::tool::Tool::call(&tool, args)
        .await
        .expect_err("stale hash must reject");
    let text = err.to_string();
    assert!(text.contains("changed since"), "{text}");
    assert!(text.contains("sha256:"), "{text}");
    assert!(
        text.contains("re-read") || text.contains("Re-read"),
        "{text}"
    );
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "dup\ndup\n");
}

#[tokio::test]
async fn edit_file_success_reports_strategy_hashes_and_diff() {
    let root = temp_root("gents-edit-success");
    let file = root.join("profile.json");
    std::fs::write(&file, "{\n  \"max_turns\": 20\n}\n").unwrap();
    let pre_hash = {
        use sha2::{Digest, Sha256};
        format!("sha256:{:x}", Sha256::digest(std::fs::read(&file).unwrap()))
    };
    let tool = EditFileTool::new(ToolContext::new(root, false).unwrap());
    // Pattern carries trailing whitespace the file lacks: ladder must apply.
    let mut args = edit_args(
        "profile.json",
        "  \"max_turns\": 20   ",
        "  \"max_turns\": 250",
    );
    args.expected_content_hash = Some(pre_hash.clone());
    let output = crate::llm::tool::Tool::call(&tool, args).await.unwrap();
    let value: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(value["match_strategy"], "trailing_whitespace", "{value}");
    assert_eq!(value["pre_edit_hash"].as_str().unwrap(), pre_hash);
    assert!(value["post_edit_hash"]
        .as_str()
        .unwrap()
        .starts_with("sha256:"));
    assert_ne!(value["post_edit_hash"], value["pre_edit_hash"]);
    assert!(value["diff"].as_str().unwrap().contains("+"), "{value}");
    assert!(std::fs::read_to_string(&file)
        .unwrap()
        .contains("\"max_turns\": 250"));
}

#[tokio::test]
async fn edit_file_not_found_error_carries_closest_match() {
    let root = temp_root("gents-edit-closest");
    std::fs::write(root.join("a.yaml"), "max_turns: 20\nmodel: d4f\n").unwrap();
    let tool = EditFileTool::new(ToolContext::new(root, false).unwrap());
    let err = crate::llm::tool::Tool::call(
        &tool,
        edit_args("a.yaml", "max_turns: 21", "max_turns: 250"),
    )
    .await
    .expect_err("no match");
    let text = err.to_string();
    assert!(text.contains("Closest match"), "{text}");
    assert!(text.contains("line 1"), "{text}");
    assert!(text.contains("% similar"), "{text}");
}

#[tokio::test]
async fn edit_file_ambiguous_error_lists_occurrences() {
    let root = temp_root("gents-edit-ambiguous");
    std::fs::write(root.join("a.txt"), "x = 1\ny\nx = 1\n").unwrap();
    let tool = EditFileTool::new(ToolContext::new(root, false).unwrap());
    let err = crate::llm::tool::Tool::call(&tool, edit_args("a.txt", "x = 1", "x = 2"))
        .await
        .expect_err("ambiguous");
    let text = err.to_string();
    assert!(text.contains("2 "), "{text}");
    assert!(text.contains("line 1"), "{text}");
    assert!(text.contains("line 3"), "{text}");
    assert!(text.contains("replace_all"), "{text}");
}

#[tokio::test]
async fn edit_file_crlf_file_round_trips() {
    let root = temp_root("gents-edit-crlf");
    let file = root.join("w.ini");
    std::fs::write(&file, "a=1\r\nmax_turns=20\r\n").unwrap();
    let tool = EditFileTool::new(ToolContext::new(root, false).unwrap());
    let output =
        crate::llm::tool::Tool::call(&tool, edit_args("w.ini", "max_turns=20", "max_turns=250"))
            .await
            .unwrap();
    let _: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(std::fs::read(&file).unwrap(), b"a=1\r\nmax_turns=250\r\n");
}

#[tokio::test]
async fn edit_file_regex_mode_and_insert_after_operation() {
    let root = temp_root("gents-edit-regex-ops");
    let file = root.join("c.toml");
    std::fs::write(&file, "timeout = 1800\n").unwrap();
    let tool = EditFileTool::new(ToolContext::new(root.clone(), false).unwrap());
    let mut args = edit_args("c.toml", r"timeout = (\d+)", "timeout = 3600 # was $1");
    args.match_mode = Some("regex".to_string());
    crate::llm::tool::Tool::call(&tool, args).await.unwrap();
    assert_eq!(
        std::fs::read_to_string(&file).unwrap(),
        "timeout = 3600 # was 1800\n"
    );

    let mut args = edit_args("c.toml", "timeout = 3600 # was 1800", "\nretries = 3");
    args.operation = Some("insert_after".to_string());
    crate::llm::tool::Tool::call(&tool, args).await.unwrap();
    assert_eq!(
        std::fs::read_to_string(&file).unwrap(),
        "timeout = 3600 # was 1800\nretries = 3\n"
    );
}

// Review finding 2: non-UTF-8 files are rejected, never lossy-rewritten.
#[tokio::test]
async fn edit_file_rejects_non_utf8_instead_of_corrupting() {
    let root = temp_root("gents-edit-non-utf8");
    let file = root.join("latin1.txt");
    std::fs::write(&file, b"caf\xe9 target\n").unwrap();
    let tool = EditFileTool::new(ToolContext::new(root, false).unwrap());
    let err = crate::llm::tool::Tool::call(&tool, edit_args("latin1.txt", "target", "x"))
        .await
        .expect_err("non-UTF-8 must be rejected");
    assert!(err.to_string().contains("UTF-8"), "{err}");
    // The file is untouched — no U+FFFD rewrite.
    assert_eq!(std::fs::read(&file).unwrap(), b"caf\xe9 target\n");
}

// Review finding 1: concurrent edits to the same file serialize — neither
// lost-updates the other. (Without the per-path lock this interleaves:
// both read, both write full content, one edit vanishes.)
#[tokio::test(flavor = "multi_thread")]
async fn edit_file_concurrent_edits_do_not_lose_updates() {
    let root = temp_root("gents-edit-concurrent");
    let file = root.join("both.txt");
    let tool = EditFileTool::new(ToolContext::new(root, false).unwrap());
    for round in 0..20 {
        std::fs::write(&file, "alpha: 0\nbeta: 0\n").unwrap();
        let a = crate::llm::tool::Tool::call(&tool, edit_args("both.txt", "alpha: 0", "alpha: 1"));
        let b = crate::llm::tool::Tool::call(&tool, edit_args("both.txt", "beta: 0", "beta: 1"));
        let (ra, rb) = tokio::join!(a, b);
        ra.unwrap();
        rb.unwrap();
        let text = std::fs::read_to_string(&file).unwrap();
        assert_eq!(
            text, "alpha: 1\nbeta: 1\n",
            "round {round}: lost update, file is {text:?}"
        );
    }
}

// Round-2 finding 1: write_file shares the mutation lock — a concurrent
// edit_file + write_file pair serializes, so the final state is always one
// of the two legal serial orders, never a torn third state (edit reads
// before the write, lands after it, resurrecting overwritten content).
#[tokio::test(flavor = "multi_thread")]
async fn write_file_and_edit_file_serialize_on_the_same_lock() {
    let root = temp_root("gents-write-edit-serialize");
    let file = root.join("both.txt");
    let context = ToolContext::new(root, false).unwrap();
    let editor = EditFileTool::new(context.clone());
    let writer = WriteFileTool::new(context);
    for round in 0..20 {
        std::fs::write(&file, "alpha: 0\nbeta: 0\n").unwrap();
        let edit =
            crate::llm::tool::Tool::call(&editor, edit_args("both.txt", "alpha: 0", "alpha: 1"));
        let write = crate::llm::tool::Tool::call(
            &writer,
            WriteFileArgs {
                path: "both.txt".to_string(),
                content: "alpha: 0\nbeta: 1\n".to_string(),
                raw_json: false,
            },
        );
        let (re, rw) = tokio::join!(edit, write);
        rw.unwrap();
        re.unwrap();
        let text = std::fs::read_to_string(&file).unwrap();
        let legal = text == "alpha: 1\nbeta: 1\n" // write then edit
            || text == "alpha: 0\nbeta: 1\n"; // edit then write (write wins)
        assert!(legal, "round {round}: torn interleaving produced {text:?}");
    }
}

// Round-3 finding 2: mutation-lock keys for not-yet-existing files resolve
// symlinked directory aliases to one key — both spellings share one lock.
#[cfg(unix)]
#[test]
fn mutation_lock_keys_resolve_symlinked_parents_for_new_files() {
    let root = temp_root("gents-lock-alias");
    std::fs::create_dir_all(root.join("real")).unwrap();
    std::os::unix::fs::symlink(root.join("real"), root.join("alias")).unwrap();
    let via_alias = super::file_tools::file_mutation_lock_for(&root.join("alias/new.txt"));
    let via_real = super::file_tools::file_mutation_lock_for(&root.join("real/new.txt"));
    assert!(
        std::sync::Arc::ptr_eq(&via_alias, &via_real),
        "alias and real spellings of a new file must share one lock"
    );
}

// #985/#1018: the timeout applied when the model omits timeout_secs must equal
// the schema-advertised default; explicit foreground requests may raise it up
// to the operator's foreground ceiling; backgrounded runs get their own
// lifetime budget instead of either foreground value.
#[test]
fn command_timeout_resolution_matches_advertised_schema() {
    use super::shared::resolve_command_timeout;

    let foreground_default = Duration::from_secs(120);
    let foreground_max = Duration::from_secs(3_600);
    assert_eq!(
        resolve_command_timeout(None, foreground_default, foreground_max, false),
        foreground_default,
        "omission must apply the advertised default, not the ceiling"
    );
    assert_eq!(
        resolve_command_timeout(Some(600), foreground_default, foreground_max, false),
        Duration::from_secs(600),
        "explicit foreground requests may exceed the default up to the ceiling"
    );
    assert_eq!(
        resolve_command_timeout(Some(7_200), foreground_default, foreground_max, false),
        foreground_max,
        "explicit foreground requests are capped at the ceiling"
    );
    assert_eq!(
        resolve_command_timeout(Some(5), foreground_default, foreground_max, false),
        Duration::from_secs(5)
    );
    assert_eq!(
        resolve_command_timeout(Some(0), foreground_default, foreground_max, false),
        Duration::from_secs(1)
    );
    assert_eq!(
        resolve_command_timeout(Some(600), foreground_default, foreground_default, false),
        foreground_default,
        "max equal to default reproduces the coupled #985 ceiling"
    );
    assert_eq!(
        resolve_command_timeout(Some(600), foreground_default, Duration::from_secs(1), false),
        foreground_default,
        "a misconfigured max below the default is raised to the default"
    );

    let budget = Duration::from_secs(BACKGROUND_COMMAND_TIMEOUT_SECS);
    assert_eq!(
        resolve_command_timeout(None, foreground_default, foreground_max, true),
        budget,
        "background omission uses the background lifetime budget"
    );
    assert_eq!(
        resolve_command_timeout(Some(7_200), foreground_default, foreground_max, true),
        Duration::from_secs(7_200),
        "background requests are exempt from the foreground ceiling"
    );
    assert_eq!(
        resolve_command_timeout(Some(999_999), foreground_default, foreground_max, true),
        budget,
        "background requests are still capped at the background budget"
    );
    assert_eq!(
        resolve_command_timeout(Some(0), foreground_default, foreground_max, true),
        Duration::from_secs(1)
    );
}

#[test]
fn bash_args_omitted_timeout_deserializes_to_none() {
    let args: BashArgs = serde_json::from_str(r#"{"command":"true"}"#).unwrap();
    assert_eq!(args.timeout_secs, None);
}

#[cfg(unix)]
#[tokio::test]
async fn backgrounded_bash_is_exempt_from_foreground_ceiling() {
    let root = temp_root("gents-bash-bg-ceiling");
    let tool = ReadOnlyBashTool::new(
        ToolContext::new(root, false).unwrap(),
        Duration::from_secs(1),
        vec!["sleep".to_string()],
    );

    let deadline = chrono::Utc::now() + chrono::Duration::seconds(30);
    let output = crate::tool_call_lifecycle::runtime::scope_background_tool_execution(
        Some(deadline),
        tokio_util::sync::CancellationToken::new(),
        None,
        None,
        crate::llm::tool::Tool::call(
            &tool,
            BashArgs {
                command: "sleep".to_string(),
                args: vec!["2".to_string()],
                cwd: None,
                timeout_secs: None,
                raw_json: false,
            },
        ),
    )
    .await
    .unwrap();

    let meta = compact_exec_meta(&output);
    assert_eq!(
        meta["ok"], true,
        "a backgrounded command must not be killed by the 1s foreground ceiling: {output}"
    );
    assert_eq!(meta["timed_out"], false);
}
