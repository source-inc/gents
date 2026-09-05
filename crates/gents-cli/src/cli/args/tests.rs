use super::*;
use clap::{CommandFactory, Parser};
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

#[test]
fn chat_goal_submission_flags_are_opt_in_and_budget_requires_objective() {
    let cli = Cli::try_parse_from([
        "gents",
        "chat",
        "--session-id",
        "durable-session",
        "--goal-objective",
        "Ship the durable feature",
        "--goal-token-budget",
        "1000",
        "start",
    ])
    .expect("goal-backed chat should parse");
    let Command::Chat(args) = cli.command else {
        panic!("expected chat command")
    };
    assert_eq!(
        args.goal_objective.as_deref(),
        Some("Ship the durable feature")
    );
    assert_eq!(args.goal_token_budget, Some(1000));

    assert!(Cli::try_parse_from(["gents", "chat", "--goal-objective", "Ship", "start",]).is_err());

    assert!(
        Cli::try_parse_from(["gents", "chat", "--goal-token-budget", "1000", "start",]).is_err()
    );
    assert!(Cli::try_parse_from([
        "gents",
        "chat",
        "--session-id",
        "durable-session",
        "--goal-objective",
        "Ship",
        "--goal-token-budget",
        "0",
        "start",
    ])
    .is_err());
}

fn assert_task_run_args(args: ConfigTaskRunArgs) {
    assert_eq!(args.task_id.as_deref(), None);
    assert_eq!(args.task_id_flag.as_deref(), Some("host-check"));
    assert_eq!(args.args, r#"{"scope":"host"}"#);
    assert_eq!(args.session_id, None);
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
fn init_tool_package_shorthands_parse() {
    assert!(parse_init(&["--write"]).write_tools);
    let yolo = parse_init(&["--yolo"]);
    assert!(yolo.yolo);
    assert!(!yolo.write_tools);
    assert!(!parse_init(&[]).write_tools);
    assert!(!parse_init(&[]).yolo);
    assert!(
        Cli::try_parse_from(["gents", "init", "--write", "--yolo"]).is_err(),
        "--write and --yolo conflict"
    );
    assert!(
        Cli::try_parse_from(["gents", "init", "--write-tools"]).is_err(),
        "the removed field-name alias must not parse"
    );
}

#[test]
fn server_apply_root_flags_parse() {
    let args = parse_server(&["--apply-root", "demo/pipeline", "--apply-prune"]);
    assert_eq!(
        args.apply_root.as_deref(),
        Some(std::path::Path::new("demo/pipeline"))
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
    assert_eq!(omitted.enable_goal_tools, None);
    assert_eq!(omitted.enable_goal_creation, None);
    assert!(omitted.subagent_targets.is_empty());
    assert!(!omitted.clear_subagent_targets);
    assert_eq!(omitted.subagent_spawn_enabled, None);
    assert_eq!(omitted.subagent_steering_enabled, None);
    assert_eq!(omitted.subagent_background_enabled, None);
    assert_eq!(omitted.subagent_allow_cross_deployment, None);
    assert_eq!(omitted.cross_deployment_spawn_timeout_seconds, None);

    let configured = parse_tools_set(&[
        "--subagent-target",
        r#"{"name":"worker","agent_did":"did:key:z-test","behavior_id":"worker","description":"worker"}"#,
        "--subagent-spawn-enabled",
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
    assert_eq!(configured.subagent_steering_enabled, Some(true));
    assert_eq!(configured.subagent_background_enabled, Some(false));
    assert_eq!(configured.subagent_allow_cross_deployment, Some(true));
    assert_eq!(configured.cross_deployment_spawn_timeout_seconds, Some(90));
}

#[test]
fn tool_bool_flags_are_patch_optional() {
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
        "--enable-goal-tools",
        "true",
        "--enable-goal-creation",
        "false",
    ]);
    assert_eq!(explicit.enable_file_tools, Some(false));
    assert_eq!(explicit.enable_bash, Some(false));
    assert_eq!(explicit.enable_meta_tools, Some(false));
    assert_eq!(explicit.enable_goal_tools, Some(true));
    assert_eq!(explicit.enable_goal_creation, Some(false));
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
            assert_eq!(args.session_id, None);
            assert!(args.wait);
            assert_eq!(args.timeout_secs, 60);
            assert_eq!(args.poll_secs, 2);
        }
        _ => panic!("expected `task run`"),
    }
}

#[test]
fn top_level_task_run_accepts_stable_goal_session_id() {
    let cli = Cli::try_parse_from([
        "gents",
        "task",
        "run",
        "durable-task",
        "--session-id",
        "pipeline-run-42",
    ])
    .expect("durable task run should parse");

    match cli.command {
        Command::Task {
            command: TaskCommand::Run(args),
        } => assert_eq!(args.session_id.as_deref(), Some("pipeline-run-42")),
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
fn removed_command_paths_are_rejected() {
    for argv in [
        vec!["gents", "config", "task", "list"],
        vec!["gents", "show", "request", "req-1"],
        vec!["gents", "config", "import", "agent-config.json"],
    ] {
        assert!(
            Cli::try_parse_from(&argv).is_err(),
            "removed path should fail parsing: {argv:?}"
        );
    }
}

#[test]
fn enrollment_owned_pairings_have_no_cli_authoring_surface() {
    for argv in [
        ["gents", "p2p", "pairings", "set"],
        ["gents", "p2p", "pairings", "rm"],
        ["gents", "p2p", "pairings", "remove"],
    ] {
        assert!(
            Cli::try_parse_from(argv).is_err(),
            "{argv:?} must not parse"
        );
    }

    let help = Cli::command().render_long_help().to_string();
    assert!(!help.contains("pairings set"));
    assert!(!help.contains("pairings rm"));

    let pairing_commands = include_str!("../../commands/p2p/pairings.rs");
    assert!(!pairing_commands.contains("upsert_PeerPairingDesired"));
    assert!(!pairing_commands.contains("delete_PeerPairingDesired"));
    assert!(!include_str!("../../desired_state/mod.rs").contains("DesiredPeerPairing"));
    assert!(!include_str!("../../config_import.rs").contains("PeerPairingDesired"));
}

#[test]
fn enrollment_operator_surface_is_explicit_and_old_network_grants_do_not_parse() {
    for argv in [
        vec!["gents", "p2p", "enrollment", "pending"],
        vec!["gents", "p2p", "enrollment", "approve", "request-1"],
        vec!["gents", "p2p", "enrollment", "deny", "request-1"],
        vec!["gents", "p2p", "enrollment", "revoke", "request-1"],
    ] {
        Cli::try_parse_from(&argv).unwrap_or_else(|error| panic!("{argv:?}: {error}"));
    }
    let native_simulator_argv = vec![
        "gents",
        "p2p",
        "enrollment",
        "approve",
        "request-1",
        "--home",
        "/tmp/gents-native-e2e",
    ];
    Cli::try_parse_from(&native_simulator_argv)
        .unwrap_or_else(|error| panic!("{native_simulator_argv:?}: {error}"));
    for argv in [
        vec!["gents", "p2p", "network", "grant", "request-1"],
        vec!["gents", "p2p", "network", "revoke", "request-1"],
        vec!["gents", "p2p", "network", "invite"],
        vec!["gents", "p2p", "network", "join"],
        vec!["gents", "p2p", "network", "approve-enrollment", "request-1"],
        vec!["gents", "p2p", "network", "deny-enrollment", "request-1"],
        vec!["gents", "p2p", "network", "revoke-enrollment", "request-1"],
    ] {
        assert!(
            Cli::try_parse_from(&argv).is_err(),
            "legacy network authority must not parse: {argv:?}"
        );
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
    ] {
        assert!(
            Cli::try_parse_from(&argv).is_err(),
            "expected clap to reject removed scope flag in: {argv:?}"
        );
    }
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

fn parse_graph(argv: &[&str]) -> GraphCommand {
    let mut args = vec!["gents", "graph"];
    args.extend_from_slice(argv);
    let cli = Cli::try_parse_from(args).expect("graph command should parse");
    match cli.command {
        Command::Graph { command } => command,
        _ => panic!("expected `graph`"),
    }
}

#[test]
fn graph_catalog_and_install_parse() {
    assert!(matches!(
        parse_graph(&["catalog", "code-review"]),
        GraphCommand::Catalog(GraphCatalogArgs { package: Some(package) }) if package == "code-review"
    ));

    match parse_graph(&[
        "install",
        "code-review",
        "--bindings",
        "/tmp/bindings.json",
        "--home",
        "/tmp/gents-home",
    ]) {
        GraphCommand::Install(args) => {
            assert_eq!(args.package, "code-review");
            assert_eq!(
                args.bindings.as_deref(),
                Some(std::path::Path::new("/tmp/bindings.json"))
            );
            assert_eq!(
                args.scope.home.as_deref(),
                Some(std::path::Path::new("/tmp/gents-home"))
            );
        }
        _ => panic!("expected graph install"),
    }
}

#[test]
fn graph_run_defaults_to_current_checkout() {
    match parse_graph(&["run", "code-review"]) {
        GraphCommand::Run(args) => {
            assert_eq!(args.repo, std::path::PathBuf::from("."));
            assert_eq!(args.base, "origin/main");
            assert_eq!(args.head, "HEAD");
        }
        _ => panic!("expected graph run"),
    }
}

#[test]
fn graph_run_watch_result_cancel_and_toggle_parse() {
    match parse_graph(&[
        "run",
        "code-review",
        "--repo",
        "/tmp/repo",
        "--base",
        "origin/main",
        "--head",
        "HEAD",
        "--watch",
    ]) {
        GraphCommand::Run(args) => {
            assert_eq!(args.package, "code-review");
            assert_eq!(args.repo, std::path::PathBuf::from("/tmp/repo"));
            assert_eq!(args.base, "origin/main");
            assert_eq!(args.head, "HEAD");
            assert!(args.watch);
        }
        _ => panic!("expected graph run"),
    }

    assert!(
        matches!(parse_graph(&["watch", "run-1"]), GraphCommand::Watch(args) if args.run_id == "run-1")
    );
    assert!(
        matches!(parse_graph(&["result", "run-1"]), GraphCommand::Result(args) if args.run_id == "run-1")
    );
    assert!(
        matches!(parse_graph(&["cancel", "run-1", "--reason", "operator"]), GraphCommand::Cancel(args) if args.run_id == "run-1" && args.reason.as_deref() == Some("operator"))
    );
    assert!(
        matches!(parse_graph(&["disable", "code-review"]), GraphCommand::Disable(args) if args.package == "code-review")
    );
    assert!(
        matches!(parse_graph(&["enable", "code-review"]), GraphCommand::Enable(args) if args.package == "code-review")
    );
}

fn parse_demo(argv: &[&str]) -> DemoArgs {
    let mut args = vec!["gents", "demo"];
    args.extend_from_slice(argv);
    let cli = Cli::try_parse_from(args).expect("demo should parse");
    match cli.command {
        Command::Demo(args) => args,
        _ => panic!("expected `demo`"),
    }
}

#[test]
fn demo_seed_parses_pack_port_home_and_page() {
    let args = parse_demo(&[
        "seed",
        "demo/pipeline",
        "--http-port",
        "19191",
        "--home",
        "/tmp/review-home",
        "--page-port",
        "19190",
        "--prompt",
        "review the diff",
        "--job-id",
        "review-1",
    ]);
    match args.command {
        Some(DemoCommand::Seed(seed)) => {
            assert_eq!(seed.pack, "demo/pipeline");
            assert_eq!(seed.http_port, 19191);
            assert_eq!(
                seed.home.as_deref(),
                Some(std::path::Path::new("/tmp/review-home"))
            );
            assert_eq!(seed.page_port, Some(19190));
            assert_eq!(seed.prompt.as_deref(), Some("review the diff"));
            assert_eq!(seed.job_id.as_deref(), Some("review-1"));
        }
        _ => panic!("expected demo seed"),
    }
}

#[test]
fn demo_init_parses_pack_and_home() {
    let args = parse_demo(&["init", "demo/pipeline", "--home", "/tmp/review-home"]);
    match args.command {
        Some(DemoCommand::Init(init)) => {
            assert_eq!(init.pack, "demo/pipeline");
            assert_eq!(init.home, std::path::PathBuf::from("/tmp/review-home"));
            assert!(!init.overwrite);
        }
        _ => panic!("expected demo init"),
    }
}

#[test]
fn chain_key_commands_parse() {
    let list = Cli::try_parse_from(["gents", "chain", "key", "list"]).expect("list");
    assert!(matches!(
        list.command,
        Command::Chain {
            command: ChainCommand::Key {
                command: ChainKeyCommand::List(_),
            },
        }
    ));
    let generate = Cli::try_parse_from(["gents", "chain", "key", "generate", "--name", "hot"])
        .expect("generate");
    match generate.command {
        Command::Chain {
            command:
                ChainCommand::Key {
                    command: ChainKeyCommand::Generate(args),
                },
        } => assert_eq!(args.name.as_deref(), Some("hot")),
        _ => panic!("expected chain key generate"),
    }
    let show = Cli::try_parse_from(["gents", "chain", "key", "show", "bind-1"]).expect("show");
    match show.command {
        Command::Chain {
            command:
                ChainCommand::Key {
                    command: ChainKeyCommand::Show(args),
                },
        } => assert_eq!(args.binding_id, "bind-1"),
        _ => panic!("expected chain key show"),
    }
    let revoke =
        Cli::try_parse_from(["gents", "chain", "key", "revoke", "bind-1"]).expect("revoke");
    match revoke.command {
        Command::Chain {
            command:
                ChainCommand::Key {
                    command: ChainKeyCommand::Revoke(args),
                },
        } => assert_eq!(args.binding_id, "bind-1"),
        _ => panic!("expected chain key revoke"),
    }
}

#[test]
fn chain_query_command_parses() {
    let parsed = Cli::try_parse_from([
        "gents",
        "chain",
        "query",
        "--tool-id",
        "base-read",
        "eth_blockNumber",
        "[]",
    ])
    .expect("query");
    match parsed.command {
        Command::Chain {
            command: ChainCommand::Query(args),
        } => {
            assert_eq!(args.tool_id, "base-read");
            assert_eq!(args.method, "eth_blockNumber");
            assert_eq!(args.params.as_deref(), Some("[]"));
        }
        _ => panic!("expected chain query"),
    }
}

#[test]
fn claude_login_parses_oauth_flags_and_rejects_seat_flags() {
    let cli = Cli::try_parse_from([
        "gents",
        "claude-login",
        "--agent-did",
        "did:key:z6MkTest",
        "--manual",
        "--no-browser",
    ])
    .expect("parse");
    let Command::ClaudeLogin(args) = cli.command else {
        panic!("expected claude-login")
    };
    assert_eq!(args.agent_did.as_deref(), Some("did:key:z6MkTest"));
    assert!(args.manual && args.no_browser);
    assert_eq!(args.provider, "claude-subscription");
    for removed in [
        "--config-dir",
        "--claude-bin",
        "--dry-run",
        "--claude-write-approved",
        "--email",
    ] {
        assert!(
            Cli::try_parse_from(["gents", "claude-login", removed, "x"]).is_err(),
            "{removed} must be gone"
        );
    }
}
