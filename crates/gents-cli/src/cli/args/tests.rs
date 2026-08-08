use super::*;
use clap::Parser;
use std::sync::{Mutex, OnceLock};

struct EnvVarGuard {
    saved: Vec<(&'static str, Option<std::ffi::OsString>)>,
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl EnvVarGuard {
    fn set(vars: &[(&'static str, &'static str)]) -> Self {
        let lock = env_lock().lock().expect("env lock poisoned");
        let saved = vars
            .iter()
            .map(|(name, _)| (*name, std::env::var_os(name)))
            .collect();
        for (name, value) in vars {
            std::env::set_var(name, value);
        }
        Self { saved, _lock: lock }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        for (name, value) in &self.saved {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }
}

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn parse_tools_set(extra: &[&str]) -> ToolSelectionUpsertArgs {
    let mut argv = vec![
        "gents",
        "config",
        "tools",
        "set",
        "--graphql",
        "http://127.0.0.1/api/v0/graphql",
        "--agent-did",
        "did:key:z-test",
        "--selection-id",
        "s1",
    ];
    argv.extend_from_slice(extra);
    let cli = Cli::try_parse_from(argv).expect("config tools set should parse");
    match cli.command {
        Command::Config {
            command:
                ConfigCommand::Tools {
                    command: ToolSelectionCommand::Set(args),
                },
        } => args,
        _ => panic!("expected `config tools set`"),
    }
}

fn parse_init(extra: &[&str]) -> InitArgs {
    let mut argv = vec!["gents", "init"];
    argv.extend_from_slice(extra);
    let cli = Cli::try_parse_from(argv).expect("init should parse");
    match cli.command {
        Command::Init(args) => args,
        _ => panic!("expected `init`"),
    }
}

fn parse_server(extra: &[&str]) -> ServeArgs {
    let mut argv = vec!["gents", "server"];
    argv.extend_from_slice(extra);
    let cli = Cli::try_parse_from(argv).expect("server should parse");
    match cli.command {
        Command::Server(args) => args,
        _ => panic!("expected `server`"),
    }
}

fn assert_task_run_args(args: ConfigTaskRunArgs) {
    assert_eq!(args.task_id.as_deref(), None);
    assert_eq!(args.task_id_flag.as_deref(), Some("host-check"));
    assert_eq!(args.args, r#"{"scope":"host"}"#);
    assert_eq!(
        args.graphql.as_deref(),
        Some("http://127.0.0.1:9191/api/v0/graphql")
    );
    assert!(args.home.is_none());
    assert!(!args.wait);
    assert_eq!(
        args.timeout_secs,
        crate::DEFAULT_INTERACTIVE_WAIT_TIMEOUT_SECS
    );
    assert_eq!(args.poll_secs, 1);
}

#[test]
fn enable_defra_query_flag_accepts_false_true_and_omission() {
    assert_eq!(parse_tools_set(&[]).enable_defra_query, None);
    assert_eq!(
        parse_tools_set(&["--enable-defra-query", "false"]).enable_defra_query,
        Some(false)
    );
    assert_eq!(
        parse_tools_set(&["--enable-defra-query", "true"]).enable_defra_query,
        Some(true)
    );
}

#[test]
fn enable_context_budget_flag_accepts_false_true_and_omission() {
    assert_eq!(parse_tools_set(&[]).enable_context_budget, None);
    assert_eq!(
        parse_tools_set(&["--enable-context-budget", "false"]).enable_context_budget,
        Some(false)
    );
    assert_eq!(
        parse_tools_set(&["--enable-context-budget", "true"]).enable_context_budget,
        Some(true)
    );
}

#[test]
fn enable_memory_flag_accepts_false_true_and_omission() {
    assert_eq!(parse_tools_set(&[]).enable_memory, None);
    assert_eq!(
        parse_tools_set(&["--enable-memory", "false"]).enable_memory,
        Some(false)
    );
    assert_eq!(
        parse_tools_set(&["--enable-memory", "true"]).enable_memory,
        Some(true)
    );
}

#[test]
fn enable_session_history_tool_flag_accepts_false_true_and_omission() {
    assert_eq!(parse_tools_set(&[]).enable_session_history_tool, None);
    assert_eq!(
        parse_tools_set(&["--enable-session-history-tool", "false"]).enable_session_history_tool,
        Some(false)
    );
    assert_eq!(
        parse_tools_set(&["--enable-session-history-tool", "true"]).enable_session_history_tool,
        Some(true)
    );
}

#[test]
fn init_tool_audit_flags_parse() {
    let args = parse_init(&[
        "--enable-memory",
        "--disable-defra-query",
        "--tool-package",
        "write",
    ]);
    assert!(args.enable_memory);
    assert!(args.disable_defra_query);
    assert_eq!(args.tool_package, Some(ToolPackageArg::Write));

    let scoped = parse_init(&[
        "--defra-query-collection",
        "AgentRequest",
        "--defra-query-collection",
        "AgentResponse",
    ]);
    assert_eq!(
        scoped.defra_query_collections,
        vec!["AgentRequest".to_string(), "AgentResponse".to_string()]
    );
}

#[test]
fn init_write_and_yolo_flags_parse() {
    assert!(parse_init(&["--write"]).write_tools);
    assert!(parse_init(&["--write-tools"]).write_tools);
    let yolo = parse_init(&["--yolo"]);
    assert!(yolo.yolo);
    assert!(!yolo.write_tools);
    assert!(!parse_init(&[]).write_tools);
    assert!(!parse_init(&[]).yolo);
    assert!(
        Cli::try_parse_from(["gents", "init", "--write", "--yolo"]).is_err(),
        "--write and --yolo conflict"
    );
}

#[test]
fn server_apply_root_flags_parse() {
    let args = parse_server(&["--apply-root", "experiments/pipeline", "--apply-prune"]);
    assert_eq!(
        args.apply_root.as_deref(),
        Some(std::path::Path::new("experiments/pipeline"))
    );
    assert!(args.apply_prune);
    let bare = parse_server(&[]);
    assert!(bare.apply_root.is_none());
    assert!(!bare.apply_prune);
}

#[test]
fn server_p2p_admission_flags_parse() {
    let args = parse_server(&[
        "--p2p-max-pending-dags",
        "321",
        "--p2p-max-concurrent-push-tasks",
        "16",
        "--p2p-max-concurrent-dag-fetches",
        "12",
        "--p2p-rate-limit-burst",
        "654",
        "--p2p-rate-limit-rate",
        "98.5",
    ]);

    assert_eq!(args.p2p_max_pending_dags, Some(321));
    assert_eq!(args.p2p_max_concurrent_push_tasks, Some(16));
    assert_eq!(args.p2p_max_concurrent_dag_fetches, Some(12));
    assert_eq!(args.p2p_rate_limit_burst, Some(654));
    assert_eq!(args.p2p_rate_limit_rate, Some(98.5));
}

#[test]
fn server_command_timeout_defaults_and_parses() {
    assert_eq!(parse_server(&[]).command_timeout_secs, 120);
    assert_eq!(parse_server(&[]).command_timeout_max_secs, None);
    assert_eq!(
        parse_server(&["--command-timeout-secs", "300"]).command_timeout_secs,
        300
    );
    assert_eq!(
        parse_server(&["--command-timeout-max-secs", "3600"]).command_timeout_max_secs,
        Some(3600)
    );
}

#[test]
fn server_p2p_admission_env_parse() {
    let _env = EnvVarGuard::set(&[
        ("DEFRA_P2P_MAX_PENDING_DAGS", "1234"),
        ("DEFRA_P2P_MAX_CONCURRENT_PUSH_TASKS", "24"),
        ("DEFRA_P2P_MAX_CONCURRENT_DAG_FETCHES", "9"),
        ("DEFRA_P2P_RATE_LIMIT_BURST", "5678"),
        ("DEFRA_P2P_RATE_LIMIT_RATE", "42.25"),
    ]);

    let args = parse_server(&[]);

    assert_eq!(args.p2p_max_pending_dags, Some(1234));
    assert_eq!(args.p2p_max_concurrent_push_tasks, Some(24));
    assert_eq!(args.p2p_max_concurrent_dag_fetches, Some(9));
    assert_eq!(args.p2p_rate_limit_burst, Some(5678));
    assert_eq!(args.p2p_rate_limit_rate, Some(42.25));
}

#[test]
fn subagent_tool_flags_preserve_when_omitted_and_parse_when_present() {
    let omitted = parse_tools_set(&[]);
    assert_eq!(omitted.enable_file_tools, None);
    assert_eq!(omitted.enable_bash, None);
    assert_eq!(omitted.enable_meta_tools, None);
    assert!(omitted.subagent_targets.is_empty());
    assert!(!omitted.clear_subagent_targets);
    assert_eq!(omitted.subagent_spawn_enabled, None);
    assert_eq!(omitted.orchestration_enabled, None);
    assert_eq!(omitted.subagent_steering_enabled, None);
    assert_eq!(omitted.subagent_background_enabled, None);
    assert_eq!(omitted.subagent_allow_cross_deployment, None);
    assert_eq!(omitted.cross_deployment_spawn_timeout_seconds, None);

    let configured = parse_tools_set(&[
        "--subagent-target",
        r#"{"name":"worker","agent_did":"did:key:z-test","behavior_id":"worker","description":"worker"}"#,
        "--subagent-spawn-enabled",
        "true",
        "--orchestration-enabled",
        "true",
        "--subagent-steering-enabled",
        "true",
        "--subagent-background-enabled",
        "false",
        "--subagent-allow-cross-deployment",
        "true",
        "--cross-deployment-spawn-timeout-seconds",
        "90",
    ]);
    assert_eq!(configured.subagent_targets.len(), 1);
    assert_eq!(configured.subagent_spawn_enabled, Some(true));
    assert_eq!(configured.orchestration_enabled, Some(true));
    assert_eq!(configured.subagent_steering_enabled, Some(true));
    assert_eq!(configured.subagent_background_enabled, Some(false));
    assert_eq!(configured.subagent_allow_cross_deployment, Some(true));
    assert_eq!(configured.cross_deployment_spawn_timeout_seconds, Some(90));
}

#[test]
fn legacy_tool_bool_flags_are_patch_optional() {
    let bare = parse_tools_set(&["--enable-file-tools", "--enable-bash"]);
    assert_eq!(bare.enable_file_tools, Some(true));
    assert_eq!(bare.enable_bash, Some(true));

    let explicit = parse_tools_set(&[
        "--enable-file-tools",
        "false",
        "--enable-bash",
        "false",
        "--enable-meta-tools",
        "false",
    ]);
    assert_eq!(explicit.enable_file_tools, Some(false));
    assert_eq!(explicit.enable_bash, Some(false));
    assert_eq!(explicit.enable_meta_tools, Some(false));
}

#[test]
fn top_level_task_run_parses_operator_affordance() {
    let cli = Cli::try_parse_from([
        "gents",
        "task",
        "run",
        "--task-id",
        "host-check",
        "--args",
        r#"{"scope":"host"}"#,
        "--graphql",
        "http://127.0.0.1:9191/api/v0/graphql",
    ])
    .expect("task run should parse");

    match cli.command {
        Command::Task {
            command: TaskCommand::Run(args),
        } => assert_task_run_args(args),
        _ => panic!("expected `task run`"),
    }
}

#[test]
fn top_level_task_run_accepts_positional_task_id_and_wait() {
    let cli = Cli::try_parse_from([
        "gents",
        "task",
        "run",
        "host-check",
        "--args",
        r#"{"scope":"host"}"#,
        "--wait",
        "--timeout-secs",
        "60",
        "--poll-secs",
        "2",
    ])
    .expect("task run positional form should parse");

    match cli.command {
        Command::Task {
            command: TaskCommand::Run(args),
        } => {
            assert_eq!(args.task_id.as_deref(), Some("host-check"));
            assert_eq!(args.task_id_flag, None);
            assert_eq!(args.args, r#"{"scope":"host"}"#);
            assert!(args.wait);
            assert_eq!(args.timeout_secs, 60);
            assert_eq!(args.poll_secs, 2);
        }
        _ => panic!("expected `task run`"),
    }
}

#[test]
fn top_level_task_list_and_show_parse() {
    let list = Cli::try_parse_from([
        "gents",
        "task",
        "list",
        "--graphql",
        "http://127.0.0.1:9191/api/v0/graphql",
    ])
    .expect("task list should parse");
    match list.command {
        Command::Task {
            command: TaskCommand::List(args),
        } => assert_eq!(
            args.graphql.as_deref(),
            Some("http://127.0.0.1:9191/api/v0/graphql")
        ),
        _ => panic!("expected `task list`"),
    }

    let show = Cli::try_parse_from(["gents", "task", "show", "host-check"])
        .expect("task show should parse");
    match show.command {
        Command::Task {
            command: TaskCommand::Show(args),
        } => {
            assert_eq!(args.task_id.as_deref(), Some("host-check"));
            assert_eq!(args.task_id_flag, None);
        }
        _ => panic!("expected `task show`"),
    }
}

#[test]
fn config_task_run_remains_available_as_compatibility_path() {
    let cli = Cli::try_parse_from([
        "gents",
        "config",
        "task",
        "run",
        "--task-id",
        "host-check",
        "--args",
        r#"{"scope":"host"}"#,
        "--graphql",
        "http://127.0.0.1:9191/api/v0/graphql",
    ])
    .expect("config task run should parse");

    match cli.command {
        Command::Config {
            command:
                ConfigCommand::Task {
                    command: TaskCommand::Run(args),
                },
        } => assert_task_run_args(args),
        _ => panic!("expected `config task run`"),
    }
}

#[test]
fn config_task_list_and_show_remain_available() {
    let list = Cli::try_parse_from(["gents", "config", "task", "list"])
        .expect("config task list should parse");
    match list.command {
        Command::Config {
            command:
                ConfigCommand::Task {
                    command: TaskCommand::List(_),
                },
        } => {}
        _ => panic!("expected `config task list`"),
    }

    let show = Cli::try_parse_from(["gents", "config", "task", "show", "--task-id", "host-check"])
        .expect("config task show should parse");
    match show.command {
        Command::Config {
            command:
                ConfigCommand::Task {
                    command: TaskCommand::Show(args),
                },
        } => {
            assert_eq!(args.task_id, None);
            assert_eq!(args.task_id_flag.as_deref(), Some("host-check"));
        }
        _ => panic!("expected `config task show`"),
    }
}

#[test]
fn deprecated_spellings_still_parse() {
    for argv in [
        vec!["gents", "config", "task", "list"],
        vec!["gents", "p2p", "pairings", "rm", "--peer", "p1"],
        vec!["gents", "p2p", "pairings", "unpair", "--peer", "p1"],
        vec!["gents", "show", "request", "req-1"],
    ] {
        Cli::try_parse_from(&argv).unwrap_or_else(|err| panic!("{argv:?}: {err}"));
    }
}

fn parse_p2p_invite(extra: &[&str]) -> P2pInviteArgs {
    let mut argv = vec!["gents", "p2p", "pairings", "invite"];
    argv.extend_from_slice(extra);
    let cli = Cli::try_parse_from(argv).expect("p2p pairings invite should parse");
    match cli.command {
        Command::P2p {
            command:
                P2pCommand::Pairings {
                    command: P2pPairingsCommand::Invite(args),
                },
        } => args,
        _ => panic!("expected `p2p pairings invite`"),
    }
}

fn parse_p2p_join(extra: &[&str]) -> P2pJoinArgs {
    let mut argv = vec!["gents", "p2p", "pairings", "join", "dapair1-token"];
    argv.extend_from_slice(extra);
    let cli = Cli::try_parse_from(argv).expect("p2p pairings join should parse");
    match cli.command {
        Command::P2p {
            command:
                P2pCommand::Pairings {
                    command: P2pPairingsCommand::Join(args),
                },
        } => args,
        _ => panic!("expected `p2p pairings join`"),
    }
}

fn parse_p2p_replicator_add(extra: &[&str]) -> P2pReplicatorAddArgs {
    let mut argv = vec![
        "gents",
        "p2p",
        "admin",
        "replicators",
        "add",
        "--peer",
        "peer-a",
        "--collection",
        "AgentRequest",
    ];
    argv.extend_from_slice(extra);
    let cli = Cli::try_parse_from(argv).expect("p2p admin replicators add should parse");
    match cli.command {
        Command::P2p {
            command:
                P2pCommand::Admin {
                    command:
                        P2pAdminCommand::Replicators {
                            command: P2pReplicatorsCommand::Add(args),
                        },
                },
        } => args,
        _ => panic!("expected `p2p admin replicators add`"),
    }
}

#[test]
fn p2p_invite_template_defaults_to_conversation() {
    let args = parse_p2p_invite(&[]);
    assert_eq!(args.template, "conversation");
    assert_eq!(args.member_did, None);
}

#[test]
fn p2p_invite_member_did_parses() {
    let args = parse_p2p_invite(&["--member-did", "did:key:zMember"]);
    assert_eq!(args.member_did.as_deref(), Some("did:key:zMember"));
}

#[test]
fn p2p_pairing_front_door_rejects_removed_scope_flags() {
    for argv in [
        vec![
            "gents",
            "p2p",
            "pairings",
            "set",
            "--did",
            "did:key:peer",
            "--peer",
            "peer-one",
            "--collection",
            "AgentRequest",
        ],
        vec![
            "gents",
            "p2p",
            "pairings",
            "set",
            "--did",
            "did:key:peer",
            "--peer",
            "peer-one",
            "--profile",
            "chat-requests",
        ],
        vec![
            "gents",
            "p2p",
            "pairings",
            "invite",
            "--profile",
            "chat-requests",
        ],
        vec![
            "gents",
            "p2p",
            "pairings",
            "join",
            "dapair1-token",
            "--profile",
            "chat-requests",
        ],
    ] {
        assert!(
            Cli::try_parse_from(&argv).is_err(),
            "expected clap to reject removed scope flag in: {argv:?}"
        );
    }
}

#[test]
fn p2p_invite_template_accepts_known_templates() {
    assert_eq!(
        parse_p2p_invite(&["--template", "backup"]).template,
        "backup"
    );
    assert_eq!(
        parse_p2p_invite(&["--template", "agent-config"]).template,
        "agent-config"
    );
}

#[test]
fn p2p_join_template_is_optional_override() {
    let no_override = parse_p2p_join(&[]);
    assert_eq!(no_override.template, None);
    let with_override = parse_p2p_join(&["--template", "backup"]);
    assert_eq!(with_override.template.as_deref(), Some("backup"));
}

#[test]
fn p2p_replicator_add_filter_parses() {
    let args = parse_p2p_replicator_add(&[
        "--filter",
        "AgentRequest:agent_did=did:key:alice",
        "--filter",
        "AgentResponse:agent_did=did:key:bob",
    ]);
    assert_eq!(args.filters.len(), 2);
    assert_eq!(args.filters[0], "AgentRequest:agent_did=did:key:alice");
    assert_eq!(args.filters[1], "AgentResponse:agent_did=did:key:bob");
}

#[test]
fn p2p_replicator_add_no_filter_is_empty() {
    let args = parse_p2p_replicator_add(&[]);
    assert!(args.filters.is_empty());
}

#[test]
fn every_deprecated_path_warns() {
    use crate::cli::deprecations::{deprecation_warning, DEPRECATED};

    for (path, replacement) in DEPRECATED {
        let mut argv = vec!["gents".to_string()];
        argv.extend(path.iter().copied().map(str::to_string));
        argv.extend(deprecated_path_required_args(path));

        let warning =
            deprecation_warning(&argv).unwrap_or_else(|| panic!("missing warning: {argv:?}"));
        assert!(
            warning.contains(&format!("use `gents {}`", replacement)),
            "warning did not mention replacement for {argv:?}: {warning}"
        );

        if deprecated_path_still_parses(path) {
            Cli::try_parse_from(&argv).unwrap_or_else(|err| panic!("{argv:?}: {err}"));
        } else {
            assert!(
                Cli::try_parse_from(&argv).is_err(),
                "{argv:?}: expected removed deprecated path to fail clap parsing"
            );
        }
    }
}

fn deprecated_path_still_parses(path: &[&str]) -> bool {
    !matches!(path, ["p2p", "pair"] | ["p2p", "unpair"])
}

fn deprecated_path_required_args(path: &[&str]) -> Vec<String> {
    match path {
        ["config", "task"] => vec!["list".to_string()],
        ["show", "request"] | ["show", "response"] => vec!["req-1".to_string()],
        ["p2p", "pair"] | ["p2p", "unpair"] => {
            vec!["--peer".to_string(), "peer-1".to_string()]
        }
        _ => panic!("no parse fixture for deprecated path: {path:?}"),
    }
}
