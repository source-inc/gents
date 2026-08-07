use std::net::IpAddr;
use std::path::PathBuf;

use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use gents::{BackendProviderKind, OpenAiWireApi};
use serde::{Deserialize, Serialize};

use crate::cli::output_format::OutputFormat;
use crate::{
    BACKGROUND_AFTER_HELP, CHAT_AFTER_HELP, CLI_AFTER_HELP, CODEX_AFTER_HELP, CONFIG_AFTER_HELP,
    CONFIG_EXPORT_AFTER_HELP, CONFIG_IMPORT_AFTER_HELP, DIAGNOSE_AFTER_HELP, FLEET_AFTER_HELP,
    INIT_AFTER_HELP, MCP_AFTER_HELP, P2P_AFTER_HELP, PROVISION_AFTER_HELP, REQUEST_AFTER_HELP,
    RESET_AFTER_HELP, RESPONSE_AFTER_HELP, SCHEMA_AFTER_HELP, SERVER_AFTER_HELP,
    SESSION_AFTER_HELP, SHOW_AFTER_HELP, STATUS_AFTER_HELP, SUBAGENT_AFTER_HELP,
    SUBAGENT_LIST_AFTER_HELP, TASK_AFTER_HELP, TOOLS_AFTER_HELP, TRACE_AFTER_HELP,
};

use crate::default_backend_max_queue_depth;

fn parse_env_var_name(value: &str) -> Result<String, String> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err("environment variable name cannot be empty".to_string());
    };
    if !(first == '_' || first.is_ascii_alphabetic())
        || !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        return Err(format!("invalid environment variable name {value:?}"));
    }
    Ok(value.to_string())
}

#[derive(Parser)]
#[command(
    name = "gents",
    about = "Local-first CLI for bootstrapping, running, and inspecting a gents runtime",
    after_help = CLI_AFTER_HELP
)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Subcommand)]
pub(crate) enum Command {
    #[command(about = "Print build and release metadata")]
    Version,
    #[command(about = "Initialize a local agent home directory", after_help = INIT_AFTER_HELP)]
    Init(InitArgs),
    #[command(
        about = "Provision a local agent home from a portable manifest root",
        after_help = PROVISION_AFTER_HELP
    )]
    Provision(ProvisionArgs),
    #[command(about = "Clear persisted local runtime state", after_help = RESET_AFTER_HELP)]
    Reset(ResetArgs),
    #[command(
        name = "server",
        alias = "serve",
        about = "Run the local gents runtime from an initialized home",
        after_help = SERVER_AFTER_HELP
    )]
    Server(ServeArgs),
    #[command(about = "Chat with the local agent in the terminal", after_help = CHAT_AFTER_HELP)]
    Chat(ChatArgs),
    #[command(
        about = "Open the Codex terminal UI against the local agent",
        after_help = CODEX_AFTER_HELP
    )]
    Codex(CodexArgs),
    #[command(about = "Sign in with ChatGPT and store OAuth credentials in DefraDB")]
    CodexLogin(CodexLoginArgs),
    #[command(about = "Probe a DefraDB-backed ChatGPT OAuth credential")]
    CodexAuthProbe(CodexAuthProbeArgs),
    #[command(
        name = "grok-login",
        alias = "xai-login",
        about = "Sign in with Grok / xAI subscription OAuth and store credentials in DefraDB"
    )]
    GrokLogin(GrokLoginArgs),
    #[command(
        name = "grok-auth-probe",
        alias = "xai-auth-probe",
        about = "Probe a DefraDB-backed Grok / xAI OAuth credential (read-only)"
    )]
    GrokAuthProbe(GrokAuthProbeArgs),
    #[command(name = "__native-fs-runner", hide = true)]
    NativeFsRunner(NativeFsRunnerArgs),
    #[command(about = "Inspect and control live P2P runtime connectivity", after_help = P2P_AFTER_HELP)]
    P2p {
        #[command(subcommand)]
        command: P2pCommand,
    },
    #[command(about = "Apply app-specific collection schemas", after_help = SCHEMA_AFTER_HELP)]
    Schema {
        #[command(subcommand)]
        command: SchemaCommand,
    },
    #[command(
        about = "Show stored runtime, request, or response state",
        after_help = SHOW_AFTER_HELP,
        hide = true
    )]
    Show {
        #[command(subcommand)]
        command: ShowCommand,
    },
    #[command(
        about = "Export persisted tool-call traces for measurement",
        after_help = TRACE_AFTER_HELP
    )]
    Trace {
        #[command(subcommand)]
        command: TraceCommand,
    },
    #[command(about = "Show the current local runtime status", after_help = STATUS_AFTER_HELP)]
    Status(StatusArgs),
    #[command(about = "Run a read-only structured query against a DefraDB collection")]
    Query(QueryArgs),
    #[command(
        about = "Inspect backgrounded tool calls",
        after_help = BACKGROUND_AFTER_HELP
    )]
    Background {
        #[command(subcommand)]
        command: BackgroundCommand,
    },
    #[command(about = "Probe registered MCP service health", after_help = MCP_AFTER_HELP)]
    Mcp {
        #[command(subcommand)]
        command: McpCommand,
    },
    #[command(about = "Inspect fleet admission slot accounting", after_help = FLEET_AFTER_HELP)]
    Fleet {
        #[command(subcommand)]
        command: FleetCommand,
    },
    #[command(
        about = "Trigger configured Task documents on demand",
        after_help = TASK_AFTER_HELP
    )]
    Task {
        #[command(subcommand)]
        command: TaskCommand,
    },
    #[command(about = "Run local configuration and runtime diagnostics", after_help = DIAGNOSE_AFTER_HELP)]
    Diagnose(DiagnoseArgs),
    #[command(about = "Explain resolved behavior tool surfaces", after_help = TOOLS_AFTER_HELP)]
    Tools {
        #[command(subcommand)]
        command: ToolsCommand,
    },
    #[command(about = "Inspect and write runtime configuration documents", after_help = CONFIG_AFTER_HELP)]
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    #[command(about = "Low-level request submission and inspection", after_help = REQUEST_AFTER_HELP)]
    Request {
        #[command(subcommand)]
        command: RequestCommand,
    },
    #[command(about = "Low-level response inspection", after_help = RESPONSE_AFTER_HELP)]
    Response {
        #[command(subcommand)]
        command: ResponseCommand,
    },
    #[command(about = "Manage and fork agent sessions", after_help = SESSION_AFTER_HELP)]
    Session {
        #[command(subcommand)]
        command: SessionCommand,
    },
    #[command(about = "Inspect and control durable session goals")]
    Goal {
        #[command(subcommand)]
        command: GoalCommand,
    },
    #[command(
        about = "Inspect and control background subagents",
        after_help = SUBAGENT_AFTER_HELP
    )]
    Subagent {
        #[command(subcommand)]
        command: SubagentCommand,
    },
    #[command(about = "Interactive, self-contained fleet demo (single node -> paired fleet)")]
    Demo(DemoArgs),
}

#[derive(clap::Args)]
pub(crate) struct DemoArgs {
    #[arg(
        long,
        help = "Demo state directory. Defaults to ~/.gents-demo (persists)"
    )]
    pub(crate) home: Option<PathBuf>,
    #[arg(
        long,
        default_value_t = false,
        help = "Wipe the demo home and start fresh"
    )]
    pub(crate) reset: bool,
    #[arg(
        long,
        help = "Inference backend base URL, e.g. http://127.0.0.1:8080/v1"
    )]
    pub(crate) inference_url: Option<String>,
    #[arg(
        long,
        help = "Backend preset (openai, openrouter, ollama, llama-cpp, vllm)"
    )]
    pub(crate) backend_preset: Option<String>,
    #[arg(
        long,
        help = "Model name to bind. Defaults to the detected/preset model"
    )]
    pub(crate) model: Option<String>,
    #[arg(
        long,
        help = "API key (stored in the backend document). Prefer OPENAI_API_KEY"
    )]
    pub(crate) api_key: Option<String>,
    #[arg(
        long,
        default_value_t = false,
        help = "Launch the native desktop app as soon as the demo runtime is ready"
    )]
    pub(crate) desktop: bool,
    #[arg(long, default_value_t = 19501, help = "HTTP port for the first node")]
    pub(crate) http_port: u16,
}

#[derive(clap::Args)]
pub(crate) struct NativeFsRunnerArgs {
    #[arg(long, value_name = "ROOT")]
    pub(crate) root: Option<PathBuf>,
    #[arg(long, value_name = "BASE")]
    pub(crate) base: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    pub(crate) self_test: bool,
}

#[derive(clap::Args)]
pub(crate) struct CodexAuthProbeArgs {
    #[arg(long, help = "Agent home directory. Defaults to ~/.gents")]
    pub(crate) home: Option<PathBuf>,
    #[arg(long, help = "GraphQL endpoint for the target gents node")]
    pub(crate) graphql: Option<String>,
    #[arg(long, help = "Agent DID that owns the OAuthCredential document")]
    pub(crate) agent_did: Option<String>,
    #[arg(long, default_value = "chatgpt-codex")]
    pub(crate) provider: String,
    #[arg(
        long,
        default_value_t = 20,
        help = "Maximum number of model slugs to print"
    )]
    pub(crate) max_models: usize,
}

#[derive(clap::Args)]
pub(crate) struct CodexLoginArgs {
    #[arg(long, help = "Agent home directory. Defaults to ~/.gents")]
    pub(crate) home: Option<PathBuf>,
    #[arg(long, help = "GraphQL endpoint for the target gents node")]
    pub(crate) graphql: Option<String>,
    #[arg(long, help = "Agent DID that owns the OAuthCredential document")]
    pub(crate) agent_did: Option<String>,
    #[arg(long, default_value = "chatgpt-codex")]
    pub(crate) provider: String,
    #[arg(long, default_value_t = false, help = "Use ChatGPT device-code login")]
    pub(crate) device_auth: bool,
    #[arg(long, help = "OAuth issuer override for testing")]
    pub(crate) issuer: Option<String>,
    #[arg(long, help = "OAuth client ID override for testing")]
    pub(crate) client_id: Option<String>,
}

#[derive(clap::Args)]
pub(crate) struct GrokAuthProbeArgs {
    #[arg(long, help = "Agent home directory. Defaults to ~/.gents")]
    pub(crate) home: Option<PathBuf>,
    #[arg(long, help = "GraphQL endpoint for the target gents node")]
    pub(crate) graphql: Option<String>,
    #[arg(long, help = "Agent DID that owns the OAuthCredential document")]
    pub(crate) agent_did: Option<String>,
    #[arg(long, default_value = "xai-oauth")]
    pub(crate) provider: String,
    #[arg(
        long,
        default_value_t = 20,
        help = "Maximum number of model slugs to print"
    )]
    pub(crate) max_models: usize,
}

#[derive(clap::Args)]
pub(crate) struct GrokLoginArgs {
    #[arg(long, help = "Agent home directory. Defaults to ~/.gents")]
    pub(crate) home: Option<PathBuf>,
    #[arg(long, help = "GraphQL endpoint for the target gents node")]
    pub(crate) graphql: Option<String>,
    #[arg(long, help = "Agent DID that owns the OAuthCredential document")]
    pub(crate) agent_did: Option<String>,
    #[arg(long, default_value = "xai-oauth")]
    pub(crate) provider: String,
}

#[derive(clap::Args)]
pub(crate) struct ProvisionArgs {
    #[arg(long, help = "Agent home directory. Defaults to ~/.gents")]
    pub(crate) home: Option<PathBuf>,
    #[arg(
        long,
        value_name = "ROOT",
        help = "Portable manifest root to bind to this home and apply"
    )]
    pub(crate) root: PathBuf,
    #[arg(
        long,
        help = "Local display name and default key filename when the home has not been initialized. Defaults to the manifest root directory name."
    )]
    pub(crate) agent_name: Option<String>,
    #[arg(
        long,
        default_value_t = false,
        help = "Create a local file-key identity when the home is uninitialized. Production hosts should bootstrap identity first."
    )]
    pub(crate) bootstrap_file_identity: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Create/load a macOS Secure Enclave identity when the home is uninitialized."
    )]
    pub(crate) bootstrap_macos_secure_enclave: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Create/load a macOS login-keychain software identity when the home is uninitialized."
    )]
    pub(crate) bootstrap_macos_keychain: bool,
    #[arg(
        long,
        value_name = "LABEL",
        help = "Keychain label for the macOS keychain identity."
    )]
    pub(crate) keychain_label: Option<String>,
    #[arg(
        long,
        value_name = "LABEL",
        help = "Keychain label for the macOS Secure Enclave identity."
    )]
    pub(crate) secure_enclave_label: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum IdentityBackendArg {
    File,
    MacosKeychain,
    MacosSecureEnclave,
}

#[derive(clap::Args)]
pub(crate) struct InitArgs {
    #[arg(long, help = "Agent home directory. Defaults to ~/.gents")]
    pub(crate) home: Option<PathBuf>,
    #[arg(long, hide = true)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(
        long,
        default_value_t = false,
        help = "Delete the existing home directory before re-initializing it"
    )]
    pub(crate) dangerously_overwrite: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Clear persisted local runtime state after initialization"
    )]
    pub(crate) reset: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Create/load identity and write init.json without seeding runtime config documents"
    )]
    pub(crate) identity_only: bool,
    #[arg(long, default_value = crate::DEFAULT_AGENT_NAME, help = "Local display name and default key filename. The agent DID is derived from the identity key.")]
    pub(crate) agent_name: String,
    #[arg(long)]
    pub(crate) key_path: Option<PathBuf>,
    #[arg(
        long,
        value_enum,
        default_value_t = IdentityBackendArg::File,
        help = "Local identity backend for init metadata."
    )]
    pub(crate) identity_backend: IdentityBackendArg,
    #[arg(
        long,
        value_name = "LABEL",
        help = "Keychain label for --identity-backend macos-keychain."
    )]
    pub(crate) keychain_label: Option<String>,
    #[arg(
        long,
        value_name = "LABEL",
        help = "Keychain label for --identity-backend macos-secure-enclave."
    )]
    pub(crate) secure_enclave_label: Option<String>,
    #[arg(
        long = "inference-url",
        alias = "inference-endpoint",
        value_name = "INFERENCE_URL",
        help = "Inference backend base URL, usually including /v1. Falls back to INFERENCE_ENDPOINT, then the local llama-server default."
    )]
    pub(crate) inference_endpoint: Option<String>,
    #[arg(value_name = "INFERENCE_URL", hide = true)]
    pub(crate) inference_endpoint_legacy: Option<String>,
    #[arg(
        long,
        help = "Optional backend document id. Defaults to <agent-name>-backend"
    )]
    pub(crate) backend_id: Option<String>,
    #[arg(
        long,
        help = "Optional backend display name. Defaults to the backend id"
    )]
    pub(crate) backend_name: Option<String>,
    #[arg(
        long,
        value_enum,
        help = "Backend preset with provider/auth defaults for common local and hosted backends"
    )]
    pub(crate) backend_preset: Option<BackendPresetArg>,
    #[arg(
        long,
        help = "Backend provider kind. OpenAiCompatible covers OpenAI-style local and hosted endpoints"
    )]
    pub(crate) provider_kind: Option<String>,
    #[arg(
        long,
        value_enum,
        help = "OpenAI-style wire API for OpenAiCompatible backends: responses or chat-completions"
    )]
    pub(crate) openai_wire_api: Option<OpenAiWireApiArg>,
    #[arg(long, help = "Raw API key stored directly in the backend document")]
    pub(crate) api_key: Option<String>,
    #[arg(long, help = "Environment variable name holding the backend API key")]
    pub(crate) api_key_env_var: Option<String>,
    #[arg(
        long,
        help = "Model id to bind to the default behavior. Required for presets without a local default model"
    )]
    pub(crate) model_name: Option<String>,
    #[arg(long, default_value_t = 2)]
    pub(crate) max_concurrent: i64,
    #[arg(long, default_value_t = default_backend_max_queue_depth())]
    pub(crate) max_queue_depth: i64,
    #[arg(
        long = "write",
        alias = "write-tools",
        default_value_t = false,
        help = "Bootstrap write-capable tools, sandboxed and scoped to the tool root, instead of the safe read-only default"
    )]
    pub(crate) write_tools: bool,
    #[arg(
        long,
        default_value_t = false,
        conflicts_with = "write_tools",
        help = "Bootstrap write-capable tools with UNRESTRICTED bash: no sandbox, full host access as your user"
    )]
    pub(crate) yolo: bool,
    #[arg(
        long,
        value_enum,
        help = "Bootstrap a named tool package. Defaults to readonly; --write and --yolo are shorthands for the write and yolo packages"
    )]
    pub(crate) tool_package: Option<ToolPackageArg>,
    #[arg(
        long,
        help = "Root directory for local file/bash tools. Defaults to the current working directory"
    )]
    pub(crate) tool_root: Option<PathBuf>,
    #[arg(
        long,
        default_value_t = false,
        help = "Enable the feature-gated per-agent memory tool in the default ToolSelection"
    )]
    pub(crate) enable_memory: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Disable the read-only defra_query tool even when the tool package enables it (only the introspection package does; defra_query is otherwise opt-in)"
    )]
    pub(crate) disable_defra_query: bool,
    #[arg(
        long,
        default_value_t = false,
        conflicts_with = "disable_defra_query",
        help = "Enable the read-only defra_query tool regardless of the tool package (pair with --defra-query-collection to scope it, e.g. the \"agent-config\" preset)"
    )]
    pub(crate) enable_defra_query: bool,
    #[arg(
        long = "defra-query-collection",
        help = "Restrict the defra_query tool to these collections when a package enables it (repeatable); omit for all collections"
    )]
    pub(crate) defra_query_collections: Vec<String>,
}

impl InitArgs {
    pub(crate) fn resolved_inference_endpoint(&self) -> Option<&str> {
        self.inference_endpoint
            .as_deref()
            .or(self.inference_endpoint_legacy.as_deref())
    }

    pub(crate) fn preset_default_model_name(&self) -> Option<&'static str> {
        self.backend_preset
            .and_then(BackendPresetArg::default_model_name)
    }
}

#[derive(clap::Args)]
pub(crate) struct ResetArgs {
    #[arg(long, help = "Agent home directory. Defaults to ~/.gents")]
    pub(crate) home: Option<PathBuf>,
}

#[derive(clap::Args)]
pub(crate) struct ServeArgs {
    #[arg(long, help = "Agent home directory. Defaults to ~/.gents")]
    pub(crate) home: Option<PathBuf>,
    #[arg(long, hide = true)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long, default_value = "127.0.0.1")]
    pub(crate) http_addr: IpAddr,
    #[arg(long, default_value_t = crate::DEFAULT_HTTP_PORT)]
    pub(crate) http_port: u16,
    #[arg(long)]
    pub(crate) agent_name: Option<String>,
    #[arg(long)]
    pub(crate) key_path: Option<PathBuf>,
    #[arg(
        long,
        value_enum,
        help = "Operator safety cap that clamps document tool selection at runtime"
    )]
    pub(crate) tool_ceiling: Option<ToolCeilingArg>,
    #[arg(
        long,
        default_value_t = 120,
        help = "Foreground Bash command timeout in seconds: applied when a call omits timeout_secs, and the foreground cap unless --command-timeout-max-secs raises it (backgrounded runs use the built-in background lifetime budget)"
    )]
    pub(crate) command_timeout_secs: u64,
    #[arg(
        long,
        help = "Foreground cap in seconds for explicit Bash timeout_secs requests. Defaults to --command-timeout-secs; values below it are raised to it (#1018)"
    )]
    pub(crate) command_timeout_max_secs: Option<u64>,
    #[arg(long = "cli-tool")]
    pub(crate) cli_tools: Vec<String>,
    #[arg(
        long,
        default_value_t = false,
        help = "Expose the read-only defra_query MCP tool at /mcp. Off by default: this is an unauthenticated read surface (same listener exposure as the GraphQL endpoint)"
    )]
    pub(crate) enable_mcp: bool,
    #[arg(
        long = "mcp-query-collection",
        help = "When --enable-mcp is set, restrict the /mcp defra_query tool to these collections (repeatable); omit for all"
    )]
    pub(crate) mcp_query_collections: Vec<String>,
    #[arg(
        long,
        help = "Root directory for readonly/readwrite tool ceilings. Readonly defaults to the current working directory when unset"
    )]
    pub(crate) tool_root: Option<PathBuf>,
    #[arg(
        long,
        hide = true,
        default_value_t = false,
        conflicts_with = "no_codex_shim",
        help = "Deprecated no-op: the Codex endpoint now runs by default"
    )]
    pub(crate) codex_shim: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Disable the Codex TUI endpoint (`gents codex` needs it)"
    )]
    pub(crate) no_codex_shim: bool,
    #[arg(
        long,
        default_value = "127.0.0.1",
        help = "Address for the app-server shim to listen on; non-loopback addresses require --codex-shim-auth-token-env"
    )]
    pub(crate) codex_shim_bind_addr: IpAddr,
    #[arg(long, default_value_t = crate::DEFAULT_CODEX_SHIM_PORT)]
    pub(crate) codex_shim_port: u16,
    #[arg(
        long,
        value_name = "ENV_VAR",
        value_parser = parse_env_var_name,
        conflicts_with = "no_codex_shim",
        help = "Environment variable containing the bearer token required by the app-server WebSocket"
    )]
    pub(crate) codex_shim_auth_token_env: Option<String>,
    #[arg(
        long,
        value_name = "WSS_URL",
        requires = "codex_shim_auth_token_env",
        conflicts_with = "no_codex_shim",
        help = "Public wss:// app-server URL advertised when TLS terminates in a reverse proxy"
    )]
    pub(crate) codex_shim_public_url: Option<String>,
    #[arg(long, help = "Optional GENTS behavior override for Codex turns")]
    pub(crate) codex_shim_behavior_id: Option<String>,
    #[arg(long, default_value_t = crate::DEFAULT_CODEX_SHIM_TIMEOUT_SECS)]
    pub(crate) codex_shim_timeout_secs: u64,
    #[arg(long, default_value_t = 250)]
    pub(crate) codex_shim_poll_ms: u64,
    #[arg(
        long,
        value_enum,
        default_value_t = P2pTransportArg::Iroh,
        help = "P2P transport for this server. Use `none` for local-only demos that only need GraphQL/Codex shim"
    )]
    pub(crate) p2p_transport: P2pTransportArg,
    #[arg(long)]
    pub(crate) p2p_bind_addr: Option<IpAddr>,
    #[arg(long)]
    pub(crate) p2p_port: Option<u16>,
    #[arg(long)]
    pub(crate) p2p_secret_key_path: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = P2pRelayModeArg::Disabled)]
    pub(crate) p2p_relay_mode: P2pRelayModeArg,
    #[arg(long, value_enum, default_value_t = P2pDiscoveryArg::Disabled)]
    pub(crate) p2p_discovery: P2pDiscoveryArg,
    #[arg(
        long,
        env = "DEFRA_P2P_MAX_PENDING_DAGS",
        help = "Maximum pending DAG registrations retained while Bitswap resolves missing links (must be > 0)"
    )]
    pub(crate) p2p_max_pending_dags: Option<usize>,
    #[arg(
        long,
        env = "DEFRA_P2P_MAX_CONCURRENT_PUSH_TASKS",
        help = "Maximum concurrent outbound PushLog worker slots (must be > 0). This is the hub fan-out semaphore that must free on timeout"
    )]
    pub(crate) p2p_max_concurrent_push_tasks: Option<usize>,
    #[arg(
        long,
        env = "DEFRA_P2P_MAX_CONCURRENT_DAG_FETCHES",
        help = "Maximum concurrent Bitswap DAG fetches while resolving missing links (must be > 0)"
    )]
    pub(crate) p2p_max_concurrent_dag_fetches: Option<usize>,
    #[arg(
        long,
        env = "DEFRA_P2P_RATE_LIMIT_BURST",
        help = "Per-peer P2P rate-limit burst capacity (must be > 0)"
    )]
    pub(crate) p2p_rate_limit_burst: Option<u32>,
    #[arg(
        long,
        env = "DEFRA_P2P_RATE_LIMIT_RATE",
        help = "Per-peer P2P rate-limit refill rate in tokens per second (must be finite and > 0)"
    )]
    pub(crate) p2p_rate_limit_rate: Option<f64>,
}

#[derive(clap::Args)]
pub(crate) struct CodexArgs {
    #[arg(
        long,
        default_value = crate::DEFAULT_CODEX_REMOTE,
        help = "Codex shim endpoint (ws://HOST:PORT, wss://HOST:PORT, or unix://PATH)"
    )]
    pub(crate) remote: String,
    #[arg(
        long,
        value_name = "ENV_VAR",
        value_parser = parse_env_var_name,
        help = "Environment variable containing the bearer token for the remote app server; requires wss:// or loopback ws://"
    )]
    pub(crate) remote_auth_token_env: Option<String>,
    #[arg(
        long,
        default_value_t = false,
        help = "Run inline, preserving terminal scrollback, instead of the alternate screen"
    )]
    pub(crate) no_alt_screen: bool,
    #[arg(
        value_name = "PROMPT",
        help = "Optional prompt to start the session with"
    )]
    pub(crate) prompt: Option<String>,
}

#[derive(clap::Args)]
pub(crate) struct ChatArgs {
    #[arg(long, help = "Agent home directory. Defaults to ~/.gents")]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long)]
    pub(crate) agent_did: Option<String>,
    #[arg(long)]
    pub(crate) agent_name: Option<String>,
    #[arg(
        long,
        help = "Continue an existing session instead of starting a fresh one"
    )]
    pub(crate) session_id: Option<String>,
    #[arg(long, help = "Override the behavior for this one-off turn or session")]
    pub(crate) behavior_id: Option<String>,
    #[arg(long = "message-file", help = "Read the user message from a file")]
    pub(crate) message_file: Option<PathBuf>,
    #[arg(long = "output", alias = "output-format", value_enum, default_value_t = OutputFormat::Text)]
    pub(crate) output_format: OutputFormat,
    #[arg(long = "output-file", help = "Write the final response to a file")]
    pub(crate) output_file: Option<PathBuf>,
    #[arg(long, default_value_t = crate::DEFAULT_INTERACTIVE_WAIT_TIMEOUT_SECS)]
    pub(crate) timeout_secs: u64,
    #[arg(long, default_value_t = 1)]
    pub(crate) poll_secs: u64,
    #[arg(value_name = "MESSAGE")]
    pub(crate) message: Vec<String>,
}

#[derive(Subcommand)]
pub(crate) enum ShowCommand {
    #[command(about = "Show a stored AgentRequest document")]
    Request(RequestShowArgs),
    #[command(about = "Show the latest AgentResponse for a request")]
    Response(ResponseShowArgs),
    #[command(about = "Show the persisted AgentRuntime document")]
    Runtime(RuntimeShowArgs),
}

#[derive(Subcommand)]
pub(crate) enum BackgroundCommand {
    #[command(
        name = "list",
        about = "List backgrounded AgentToolCall rows",
        after_help = BACKGROUND_AFTER_HELP
    )]
    List(BackgroundListArgs),
}

#[derive(Subcommand)]
pub(crate) enum McpCommand {
    #[command(
        name = "probe",
        about = "Run a one-shot health probe for registered MCP services",
        after_help = MCP_AFTER_HELP
    )]
    Probe(McpProbeArgs),
}

#[derive(clap::Args)]
pub(crate) struct McpProbeArgs {
    #[arg(long, help = "Agent home directory. Defaults to ~/.gents")]
    pub(crate) home: Option<PathBuf>,
    #[arg(
        long,
        help = "GraphQL endpoint to read registry rows instead of local home state"
    )]
    pub(crate) graphql: Option<String>,
    #[arg(long, action = ArgAction::SetTrue, help = "Probe every online MCP service")]
    pub(crate) all: bool,
    #[arg(long, default_value = "5s", value_name = "DURATION")]
    pub(crate) timeout: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub(crate) output: OutputFormat,
    #[arg(value_name = "SERVICE")]
    pub(crate) service: Option<String>,
}

#[derive(Subcommand)]
pub(crate) enum FleetCommand {
    #[command(
        name = "slots",
        about = "Show derived fleet slot usage from the live runtime HTTP API",
        after_help = FLEET_AFTER_HELP
    )]
    Slots(FleetSlotsArgs),
}

#[derive(clap::Args)]
pub(crate) struct BackgroundListArgs {
    #[arg(long, help = "Agent home directory. Defaults to ~/.gents")]
    pub(crate) home: Option<PathBuf>,
    #[arg(long, help = "GraphQL endpoint to read instead of local home state")]
    pub(crate) graphql: Option<String>,
    #[arg(
        long = "request",
        value_name = "ID",
        help = "Only show backgrounded tools for this parent request"
    )]
    pub(crate) request_id: Option<String>,
    #[arg(
        long,
        value_name = "STATE",
        help = "Only show tool calls whose displayed state matches this value"
    )]
    pub(crate) state: Option<String>,
    #[arg(
        long,
        value_name = "DURATION",
        help = "Only show calls older than this duration, e.g. 30s, 5m, 2h"
    )]
    pub(crate) age_gt: Option<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub(crate) output: OutputFormat,
}

#[derive(clap::Args)]
pub(crate) struct FleetSlotsArgs {
    #[arg(long, help = "Agent home directory. Defaults to ~/.gents")]
    pub(crate) home: Option<PathBuf>,
    #[arg(long, help = "GraphQL endpoint for the live runtime")]
    pub(crate) graphql: Option<String>,
}

#[derive(clap::Args)]
pub(crate) struct StatusArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long)]
    pub(crate) agent_did: Option<String>,
}

#[derive(clap::Args)]
pub(crate) struct QueryArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long, help = "Collection (GraphQL type) to read, e.g. AgentRequest")]
    pub(crate) collection: String,
    #[arg(
        long = "field",
        help = "Field to return (repeatable); at least one is required"
    )]
    pub(crate) fields: Vec<String>,
    #[arg(
        long,
        help = r#"DefraDB filter as JSON, e.g. '{"status":{"_eq":"completed"}}'"#
    )]
    pub(crate) filter: Option<String>,
    #[arg(long, help = "Maximum rows to return (default 50, capped at 1000)")]
    pub(crate) limit: Option<u32>,
    #[arg(
        long = "allow-collection",
        help = "Restrict the query to these collections (repeatable); omit for all"
    )]
    pub(crate) allow_collections: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum P2pTransportArg {
    None,
    Iroh,
}

impl P2pTransportArg {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Iroh => "iroh",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum P2pRelayModeArg {
    Default,
    Disabled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum P2pDiscoveryArg {
    #[value(name = "n0")]
    N0,
    Disabled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum OpenAiWireApiArg {
    Responses,
    #[value(name = "chat-completions", alias = "chat_completions")]
    ChatCompletions,
}

impl OpenAiWireApiArg {
    pub(crate) fn to_config(self) -> OpenAiWireApi {
        match self {
            Self::Responses => OpenAiWireApi::Responses,
            Self::ChatCompletions => OpenAiWireApi::ChatCompletions,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum BackendPresetArg {
    #[value(name = "generic-openai-compatible")]
    GenericOpenAiCompatible,
    #[value(name = "openai")]
    OpenAi,
    #[value(name = "openrouter")]
    OpenRouter,
    #[value(name = "chatgpt-codex")]
    ChatGptCodex,
    #[value(name = "xai-oauth", alias = "grok-oauth")]
    XaiGrokOAuth,
    #[value(name = "ollama")]
    Ollama,
    #[value(name = "vllm")]
    Vllm,
    #[value(name = "llama-cpp")]
    LlamaCpp,
}

impl BackendPresetArg {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::GenericOpenAiCompatible => "generic-openai-compatible",
            Self::OpenAi => "openai",
            Self::OpenRouter => "openrouter",
            Self::ChatGptCodex => "chatgpt-codex",
            Self::XaiGrokOAuth => "xai-oauth",
            Self::Ollama => "ollama",
            Self::Vllm => "vllm",
            Self::LlamaCpp => "llama-cpp",
        }
    }

    pub(crate) fn provider_kind(self) -> BackendProviderKind {
        match self {
            Self::OpenRouter => BackendProviderKind::OpenRouter,
            Self::ChatGptCodex => BackendProviderKind::ChatGptCodex,
            Self::XaiGrokOAuth => BackendProviderKind::XaiGrokOAuth,
            Self::GenericOpenAiCompatible
            | Self::OpenAi
            | Self::Ollama
            | Self::Vllm
            | Self::LlamaCpp => BackendProviderKind::OpenAiCompatible,
        }
    }

    pub(crate) fn default_endpoint(self) -> Option<&'static str> {
        match self {
            Self::GenericOpenAiCompatible => None,
            Self::OpenAi => Some("https://api.openai.com/v1"),
            Self::OpenRouter => Some("https://openrouter.ai/api/v1"),
            Self::ChatGptCodex => Some(gents::chatgpt_codex::default_backend_endpoint()),
            Self::XaiGrokOAuth => Some(gents::xai_grok_oauth::default_backend_endpoint()),
            Self::Ollama => Some(crate::DEFAULT_OLLAMA_ENDPOINT),
            Self::Vllm => Some("http://127.0.0.1:8000/v1"),
            Self::LlamaCpp => Some("http://127.0.0.1:8080/v1"),
        }
    }

    /// Default model for presets whose endpoint implies a model source. The
    /// shared DEFAULT_INIT_MODEL_NAME is an HF GGUF repo path: valid for
    /// llama-server (`-hf`) but not an Ollama tag, so the ollama preset pulls
    /// the same model through Ollama's `hf.co/` form instead.
    pub(crate) fn default_model_name(self) -> Option<&'static str> {
        match self {
            Self::Ollama => Some(crate::DEFAULT_OLLAMA_MODEL_NAME),
            Self::LlamaCpp => Some(crate::DEFAULT_INIT_MODEL_NAME),
            Self::ChatGptCodex => Some(crate::DEFAULT_CHATGPT_CODEX_MODEL_NAME),
            Self::XaiGrokOAuth => Some(crate::DEFAULT_XAI_GROK_OAUTH_MODEL_NAME),
            Self::GenericOpenAiCompatible | Self::OpenAi | Self::OpenRouter | Self::Vllm => None,
        }
    }

    pub(crate) fn default_api_key_env_var(self) -> Option<&'static str> {
        match self {
            Self::OpenAi => Some("OPENAI_API_KEY"),
            Self::OpenRouter => Some("OPENROUTER_API_KEY"),
            Self::GenericOpenAiCompatible
            | Self::ChatGptCodex
            | Self::XaiGrokOAuth
            | Self::Ollama
            | Self::Vllm
            | Self::LlamaCpp => None,
        }
    }

    pub(crate) fn default_openai_wire_api(self) -> Option<OpenAiWireApi> {
        match self {
            Self::OpenAi => Some(OpenAiWireApi::Responses),
            Self::GenericOpenAiCompatible
            | Self::OpenRouter
            | Self::ChatGptCodex
            | Self::XaiGrokOAuth
            | Self::Ollama
            | Self::Vllm
            | Self::LlamaCpp => None,
        }
    }
}

#[derive(clap::Args)]
pub(crate) struct DiagnoseArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long)]
    pub(crate) agent_did: Option<String>,
    #[arg(long = "bind-agent-did", value_enum)]
    pub(crate) bind_agent_did: Option<ManifestAgentDidBindingArg>,
}

#[derive(clap::Args)]
pub(crate) struct RuntimeShowArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long)]
    pub(crate) agent_did: Option<String>,
}

#[derive(Subcommand)]
pub(crate) enum TraceCommand {
    #[command(name = "export", about = "Export Amy-style tool-call JSONL")]
    Export(TraceExportArgs),
    #[command(
        name = "timeline",
        about = "Export a reconstructed run timeline for one AgentRequest"
    )]
    Timeline(TraceTimelineArgs),
    #[command(
        name = "project",
        about = "Project a reconstructed run into an adapter-facing interop shape"
    )]
    Project(TraceProjectArgs),
    #[command(
        name = "project-schema",
        about = "Print the JSON Schema for an adapter projection output"
    )]
    ProjectSchema(TraceProjectSchemaArgs),
}

#[derive(clap::Args)]
pub(crate) struct TraceExportArgs {
    #[arg(long, help = "Agent home directory. Defaults to ~/.gents")]
    pub(crate) home: Option<PathBuf>,
    #[arg(long, help = "GraphQL endpoint to read instead of local home state")]
    pub(crate) graphql: Option<String>,
    #[arg(long, help = "Restrict export to one session_id")]
    pub(crate) session_id: Option<String>,
    #[arg(long, help = "Restrict export to one inferred request_id")]
    pub(crate) request_id: Option<String>,
    #[arg(long, help = "Run id to stamp on exported JSONL records")]
    pub(crate) run_id: Option<String>,
    #[arg(long, help = "Case id to stamp on exported JSONL records")]
    pub(crate) case_id: Option<String>,
    #[arg(
        long,
        default_value_t = 500,
        help = "Maximum recent AgentToolCall rows to export"
    )]
    pub(crate) limit: usize,
    #[arg(long = "output-file", help = "Write JSONL to a file instead of stdout")]
    pub(crate) output_file: Option<PathBuf>,
}

#[derive(clap::Args)]
pub(crate) struct TraceTimelineArgs {
    #[arg(long, help = "Agent home directory. Defaults to ~/.gents")]
    pub(crate) home: Option<PathBuf>,
    #[arg(long, help = "GraphQL endpoint to read instead of local home state")]
    pub(crate) graphql: Option<String>,
    #[arg(long = "request-id", help = "Request id to reconstruct")]
    pub(crate) request_id: String,
    #[arg(long = "output-file", help = "Write JSON to a file instead of stdout")]
    pub(crate) output_file: Option<PathBuf>,
}

#[derive(clap::Args)]
pub(crate) struct TraceProjectArgs {
    #[arg(long, help = "Agent home directory. Defaults to ~/.gents")]
    pub(crate) home: Option<PathBuf>,
    #[arg(long, help = "GraphQL endpoint to read instead of local home state")]
    pub(crate) graphql: Option<String>,
    #[arg(long = "request-id", help = "Request id to reconstruct and project")]
    pub(crate) request_id: String,
    #[arg(long, value_enum, help = "Adapter projection to export")]
    pub(crate) projection: TraceProjectionArg,
    #[arg(
        long,
        value_enum,
        default_value_t = TraceProjectionRedactionArg::Full,
        help = "Redaction policy to apply before serializing adapter output"
    )]
    pub(crate) redaction: TraceProjectionRedactionArg,
    #[arg(
        long = "actor-did",
        help = "Actor identity used for projection provenance"
    )]
    pub(crate) actor_did: Option<String>,
    #[arg(
        long = "acp-policy-id",
        help = "DefraDB Document ACP policy id used to enforce row-level read decisions before projection. Requires --graphql and --actor-did."
    )]
    pub(crate) acp_policy_id: Option<String>,
    #[arg(
        long = "scope-agent-did",
        help = "Require the root request to match this agent DID and omit content-bearing events for other agents"
    )]
    pub(crate) scope_agent_did: Option<String>,
    #[arg(
        long = "scope-behavior-id",
        help = "Require the root request to match this behavior id and omit content-bearing events for other behaviors"
    )]
    pub(crate) scope_behavior_id: Option<String>,
    #[arg(
        long = "scope-session-id",
        help = "Require the root request to belong to this session id"
    )]
    pub(crate) scope_session_id: Option<String>,
    #[arg(
        long,
        value_enum,
        default_value_t = TraceProjectionFormatArg::Json,
        help = "Adapter projection output format; native-json omits the Gents envelope"
    )]
    pub(crate) format: TraceProjectionFormatArg,
    #[arg(
        long = "output-file",
        help = "Write projection output to a file instead of stdout"
    )]
    pub(crate) output_file: Option<PathBuf>,
}

#[derive(clap::Args)]
pub(crate) struct TraceProjectSchemaArgs {
    #[arg(long, value_enum, help = "Adapter projection schema to print")]
    pub(crate) projection: TraceProjectionArg,
    #[arg(
        long,
        value_enum,
        default_value_t = TraceProjectionFormatArg::Json,
        help = "Output schema for a projection envelope, native projection, or JSONL record"
    )]
    pub(crate) format: TraceProjectionFormatArg,
    #[arg(
        long = "output-file",
        help = "Write JSON Schema to a file instead of stdout"
    )]
    pub(crate) output_file: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum TraceProjectionArg {
    Atif,
    OpenaiCodex,
    Langgraph,
    MultiAgent,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum TraceProjectionRedactionArg {
    Full,
    TrainingSafe,
    Public,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum TraceProjectionFormatArg {
    Json,
    NativeJson,
    Jsonl,
    EvalJsonl,
}

#[derive(Subcommand)]
pub(crate) enum ConfigCommand {
    #[command(about = "Validate desired-state manifests under a repository root")]
    Validate(ConfigValidateArgs),
    #[command(about = "Diff desired-state manifests against live configuration")]
    Diff(ConfigDiffArgs),
    #[command(about = "Apply desired-state manifests to live configuration")]
    Apply(ConfigApplyArgs),
    #[command(about = "Write an InferenceBackend document")]
    Backend {
        #[command(subcommand)]
        command: BackendCommand,
    },
    #[command(about = "Write an AgentBehavior document")]
    Behavior {
        #[command(subcommand)]
        command: BehaviorCommand,
    },
    #[command(about = "Write a ToolSelection document")]
    Tools {
        #[command(subcommand)]
        command: ToolSelectionCommand,
    },
    #[command(about = "Write an InferenceProfile document")]
    Profile {
        #[command(subcommand)]
        command: InferenceProfileCommand,
    },
    #[command(about = "Inspect and fire configured Task documents", hide = true)]
    Task {
        #[command(subcommand)]
        command: TaskCommand,
    },
    #[command(about = "Inspect EventTrigger documents")]
    Trigger {
        #[command(subcommand)]
        command: ConfigTriggerCommand,
    },
    #[command(about = "Inspect Schedule documents")]
    Schedule {
        #[command(subcommand)]
        command: ConfigScheduleCommand,
    },
    #[command(about = "Inspect MCP service registry documents")]
    Mcp {
        #[command(subcommand)]
        command: ConfigMcpCommand,
    },
    #[command(about = "Create, list, show, delete, enable, or disable Skill documents")]
    Skill {
        #[command(subcommand)]
        command: SkillCommand,
    },
    #[command(about = "Write a WorkspaceRoot document")]
    WorkspaceRoot {
        #[command(subcommand)]
        command: WorkspaceRootCommand,
    },
    #[command(about = "Export desired configuration documents", after_help = CONFIG_EXPORT_AFTER_HELP)]
    Export(ConfigExportArgs),
    #[command(about = "Import desired configuration documents", after_help = CONFIG_IMPORT_AFTER_HELP)]
    Import(ConfigImportArgs),
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, ValueEnum, PartialEq, Eq)]
pub(crate) enum ToolCeilingArg {
    MetaOnly,
    Readonly,
    Readwrite,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, ValueEnum, PartialEq, Eq)]
pub(crate) enum ToolPackageArg {
    Minimal,
    Introspection,
    Readonly,
    Write,
    Yolo,
}

#[derive(Subcommand)]
pub(crate) enum BackendCommand {
    #[command(name = "set")]
    Set(BackendUpsertArgs),
    #[command(name = "discover-models")]
    DiscoverModels(BackendDiscoverModelsArgs),
    #[command(name = "list", about = "List InferenceBackend documents")]
    List(ConfigListArgs),
    #[command(name = "show", about = "Show an InferenceBackend document")]
    Show(ConfigShowArgs),
    #[command(
        name = "rm",
        about = "Delete an InferenceBackend document",
        alias = "remove"
    )]
    Rm(ConfigShowArgs),
}

#[derive(Subcommand)]
pub(crate) enum BehaviorCommand {
    #[command(name = "set")]
    Set(BehaviorUpsertArgs),
    #[command(
        name = "create",
        about = "Create a persona's AgentBehavior through the shared persona materializer"
    )]
    Create(BehaviorCreateArgs),
    #[command(
        name = "clone",
        about = "Clone an existing persona's tool selection into a new AgentBehavior"
    )]
    Clone(BehaviorCloneArgs),
    #[command(
        name = "disable",
        about = "Disable a persona's AgentBehavior through the shared persona materializer"
    )]
    Disable(BehaviorDisableArgs),
    #[command(name = "list", about = "List AgentBehavior documents")]
    List(ConfigListArgs),
    #[command(name = "show", about = "Show an AgentBehavior document")]
    Show(ConfigShowArgs),
    #[command(
        name = "rm",
        about = "Delete an AgentBehavior document",
        alias = "remove"
    )]
    Rm(ConfigShowArgs),
}

#[derive(Subcommand)]
pub(crate) enum WorkspaceRootCommand {
    #[command(name = "set")]
    Set(WorkspaceRootUpsertArgs),
    #[command(name = "list", about = "List WorkspaceRoot documents")]
    List(ConfigListArgs),
    #[command(name = "show", about = "Show a WorkspaceRoot document")]
    Show(ConfigShowArgs),
    #[command(
        name = "rm",
        about = "Delete a WorkspaceRoot document",
        alias = "remove"
    )]
    Rm(ConfigShowArgs),
}

#[derive(Subcommand)]
pub(crate) enum ToolSelectionCommand {
    #[command(name = "set")]
    Set(ToolSelectionUpsertArgs),
    #[command(name = "list", about = "List ToolSelection documents")]
    List(ConfigListArgs),
    #[command(name = "show", about = "Show a ToolSelection document")]
    Show(ConfigShowArgs),
    #[command(
        name = "rm",
        about = "Delete a ToolSelection document",
        alias = "remove"
    )]
    Rm(ConfigShowArgs),
    #[command(
        name = "subagent-target-entry",
        about = "Build a single --subagent-target JSON entry from its parts",
        after_help = "Example:\n  gents config tools set --graphql <url> --agent-did <did> \\\n    --selection-id main --subagent-target \"$(gents config tools \\\n    subagent-target-entry --name researcher --agent-did did:key:z... \\\n    --behavior-id did:key:z...:default)\""
    )]
    SubagentTargetEntry(SubagentTargetEntryArgs),
}

#[derive(Debug, clap::Args)]
pub(crate) struct SubagentTargetEntryArgs {
    #[arg(long, help = "Model-facing name used by spawn_subagent")]
    pub(crate) name: String,
    #[arg(
        long,
        help = "DID of the agent that owns the target behavior (local or remote)"
    )]
    pub(crate) agent_did: String,
    #[arg(long, help = "Behavior id on the owning agent")]
    pub(crate) behavior_id: String,
    #[arg(
        long,
        help = "Optional human-readable description surfaced to the model"
    )]
    pub(crate) description: Option<String>,
}

#[derive(Subcommand)]
pub(crate) enum SkillCommand {
    #[command(name = "add", about = "Create or update a Skill document")]
    Add(SkillAddArgs),
    #[command(
        name = "import",
        about = "Import a directory tree of Codex-format SKILL.md files as Skill documents"
    )]
    Import(SkillImportArgs),
    #[command(
        name = "export",
        about = "Export an agent's Skill documents as a SKILL.md directory tree"
    )]
    Export(SkillExportArgs),
    #[command(name = "list", about = "List Skill documents for an agent")]
    List(SkillListArgs),
    #[command(name = "show", about = "Show a single Skill document")]
    Show(SkillShowArgs),
    #[command(name = "rm", about = "Delete a Skill document")]
    Rm(SkillRefArgs),
    #[command(name = "enable", about = "Enable a Skill document")]
    Enable(SkillRefArgs),
    #[command(name = "disable", about = "Disable a Skill document")]
    Disable(SkillRefArgs),
}

#[derive(Subcommand)]
pub(crate) enum ToolsCommand {
    #[command(
        name = "explain",
        about = "Explain final model-callable tools per behavior"
    )]
    Explain(ToolExplainArgs),
    #[command(name = "holds", about = "List tool calls held awaiting approval")]
    Holds(ToolsHoldsArgs),
    #[command(
        name = "approve",
        about = "Approve (or deny) a held tool call by writing the decision document"
    )]
    Approve(ToolsApproveArgs),
}

#[derive(clap::Args)]
pub(crate) struct ToolsHoldsArgs {
    #[arg(long, help = "Agent home directory. Defaults to ~/.gents")]
    pub(crate) home: Option<PathBuf>,
    #[arg(long, help = "GraphQL endpoint to read instead of local home state")]
    pub(crate) graphql: Option<String>,
    #[arg(long, help = "Scope to one agent DID. Defaults to the home agent")]
    pub(crate) agent_did: Option<String>,
    #[arg(long, help = "List holds across every agent")]
    pub(crate) all: bool,
}

#[derive(clap::Args)]
pub(crate) struct ToolsApproveArgs {
    #[arg(value_name = "TOOL_CALL_ID")]
    pub(crate) tool_call_id: String,
    #[arg(long, help = "Agent home directory. Defaults to ~/.gents")]
    pub(crate) home: Option<PathBuf>,
    #[arg(
        long,
        help = "GraphQL endpoint to write through instead of local home state"
    )]
    pub(crate) graphql: Option<String>,
    #[arg(
        long,
        help = "Agent DID the held call belongs to. Defaults to the home agent"
    )]
    pub(crate) agent_did: Option<String>,
    #[arg(long, help = "Record a denial instead of an approval")]
    pub(crate) deny: bool,
    #[arg(
        long,
        help = "Reason recorded on the decision (shown to the model on deny)"
    )]
    pub(crate) reason: Option<String>,
    #[arg(
        long,
        help = "Approver DID recorded on the decision. Defaults to the home agent DID"
    )]
    pub(crate) approver_did: Option<String>,
}

#[derive(clap::Args)]
pub(crate) struct ToolExplainArgs {
    #[arg(long, help = "Agent home directory. Defaults to ~/.gents")]
    pub(crate) home: Option<PathBuf>,
    #[arg(long, help = "GraphQL endpoint to read instead of local home state")]
    pub(crate) graphql: Option<String>,
    #[arg(long)]
    pub(crate) agent_did: Option<String>,
    #[arg(long, help = "Only explain one behavior_id")]
    pub(crate) behavior_id: Option<String>,
}

#[derive(clap::Args)]
pub(crate) struct SkillAddArgs {
    #[arg(long)]
    pub(crate) graphql: String,
    #[arg(long)]
    pub(crate) agent_did: String,
    #[arg(long)]
    pub(crate) skill_id: String,
    #[arg(long)]
    pub(crate) name: Option<String>,
    /// Activation scope: "principal" (inherited by all the agent's behaviors)
    /// or "behavior" (only where a behavior opts in via skill_refs).
    #[arg(long, default_value = "behavior")]
    pub(crate) scope: String,
    #[arg(long)]
    pub(crate) description: Option<String>,
    /// Inline skill instructions (the body composed into the prompt).
    #[arg(long)]
    pub(crate) instructions: Option<String>,
    /// Read instructions from a file (takes precedence over --instructions).
    #[arg(long)]
    pub(crate) instructions_file: Option<PathBuf>,
    /// Declared tool dependency (repeatable). Intersected with the behavior
    /// tool ceiling at activation; never grants a tool.
    #[arg(long = "tool-ref")]
    pub(crate) tool_refs: Vec<String>,
    #[arg(long)]
    pub(crate) display_name: Option<String>,
    #[arg(long, default_value_t = true)]
    pub(crate) enabled: bool,
}

#[derive(clap::Args)]
pub(crate) struct SkillImportArgs {
    #[arg(long)]
    pub(crate) graphql: String,
    #[arg(long)]
    pub(crate) agent_did: String,
    /// Directory tree to scan for `SKILL.md` files (Codex skill layout:
    /// `<dir>/<skill-name>/SKILL.md` + optional `agents/openai.yaml`).
    #[arg(value_name = "DIR")]
    pub(crate) dir: PathBuf,
    /// Scope applied to every imported skill: "principal" or "behavior".
    #[arg(long, default_value = "behavior")]
    pub(crate) scope: String,
    /// Import skills as disabled.
    #[arg(long)]
    pub(crate) disabled: bool,
    /// Parse and report what would be imported without writing.
    #[arg(long)]
    pub(crate) dry_run: bool,
}

#[derive(clap::Args)]
pub(crate) struct SkillExportArgs {
    #[arg(long)]
    pub(crate) graphql: String,
    #[arg(long)]
    pub(crate) agent_did: String,
    /// Output directory. Each skill is written to `<dir>/<skill_id>/SKILL.md`
    /// (plus `agents/openai.yaml` when it has tool_refs or a display name).
    #[arg(value_name = "DIR")]
    pub(crate) dir: PathBuf,
}

#[derive(clap::Args)]
pub(crate) struct SkillListArgs {
    #[arg(long)]
    pub(crate) graphql: String,
    #[arg(long)]
    pub(crate) agent_did: String,
}

#[derive(clap::Args)]
pub(crate) struct SkillShowArgs {
    #[arg(long)]
    pub(crate) graphql: String,
    #[arg(long)]
    pub(crate) skill_id: String,
}

/// Shared args for skill commands that target a single skill by id.
#[derive(clap::Args)]
pub(crate) struct SkillRefArgs {
    #[arg(long)]
    pub(crate) graphql: String,
    #[arg(long)]
    pub(crate) skill_id: String,
}

#[derive(clap::Args)]
pub(crate) struct ConfigListArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub(crate) output: OutputFormat,
}

#[derive(clap::Args)]
pub(crate) struct ConfigShowArgs {
    #[arg(long = "id", value_name = "ID")]
    pub(crate) id_flag: Option<String>,
    #[arg(value_name = "ID")]
    pub(crate) id: Option<String>,
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    pub(crate) output: OutputFormat,
}

#[derive(clap::Args)]
pub(crate) struct WorkspaceRootUpsertArgs {
    #[arg(long)]
    pub(crate) graphql: String,
    #[arg(long, help = "Absolute path to register as a workspace root")]
    pub(crate) path: PathBuf,
    #[arg(long)]
    pub(crate) display_name: Option<String>,
    #[arg(
        long,
        help = "Register the root disabled; excluded from allowed_roots until re-enabled"
    )]
    pub(crate) disabled: bool,
}

#[derive(clap::Args)]
pub(crate) struct BehaviorUpsertArgs {
    #[arg(long)]
    pub(crate) graphql: String,
    #[arg(long)]
    pub(crate) agent_did: String,
    #[arg(long)]
    pub(crate) behavior_id: Option<String>,
    #[arg(long)]
    pub(crate) display_name: Option<String>,
    #[arg(long)]
    pub(crate) system_prompt_file: Option<PathBuf>,
    #[arg(long)]
    pub(crate) backend_id: Option<String>,
    #[arg(long)]
    pub(crate) model_name: Option<String>,
    #[arg(long)]
    pub(crate) tool_selection_id: Option<String>,
    #[arg(long)]
    pub(crate) inference_profile_id: Option<String>,
    #[arg(long)]
    pub(crate) compaction_strategy: Option<String>,
    #[arg(long)]
    pub(crate) compaction_threshold: Option<f64>,
    #[arg(long, default_value_t = true)]
    pub(crate) enabled: bool,
}

/// Routes through the shared persona materializer (`gents::agent::persona_ops`):
/// a `PersonaConfigRequest` row is submitted and polled to a terminal status
/// rather than writing `AgentBehavior`/`ToolSelection` directly, so admission
/// and materialization never drift from the reconciler / self-config tool's
/// own writes. `--model` and `--backend-id`/`--model-name` are two spellings
/// of the same `backend_id|model_name` value; supply one or the other.
#[derive(clap::Args)]
pub(crate) struct BehaviorCreateArgs {
    #[arg(long)]
    pub(crate) graphql: String,
    #[arg(long)]
    pub(crate) agent_did: String,
    #[arg(long, help = "Persona display name")]
    pub(crate) display_name: String,
    #[arg(
        long,
        help = "Built-in permission preset (readonly|write); mutually exclusive with --clone-from"
    )]
    pub(crate) preset: Option<String>,
    #[arg(
        long,
        help = "Optional workspace root scope; must be a published WorkspaceRoot path"
    )]
    pub(crate) root: Option<String>,
    #[arg(
        long,
        help = "behavior_id of an existing enabled persona to clone the tool selection from; mutually exclusive with --preset"
    )]
    pub(crate) clone_from: Option<String>,
    #[arg(long, help = r#""backend_id|model_name", e.g. "openai|gpt-5""#)]
    pub(crate) model: Option<String>,
    #[arg(long, help = "Alternative to --model")]
    pub(crate) backend_id: Option<String>,
    #[arg(long, help = "Alternative to --model")]
    pub(crate) model_name: Option<String>,
    #[arg(long)]
    pub(crate) profile_id: Option<String>,
}

/// See [`BehaviorCreateArgs`] doc comment: same materializer routing. The
/// clone's `agent_did` is derived from `source_behavior_id` (a clone always
/// creates a sibling persona of the same agent as its source); `--model`/
/// `--profile-id` default to the source behavior's own current values when
/// omitted.
#[derive(clap::Args)]
pub(crate) struct BehaviorCloneArgs {
    #[arg(long)]
    pub(crate) graphql: String,
    #[arg(value_name = "SOURCE_BEHAVIOR_ID")]
    pub(crate) source_behavior_id: String,
    #[arg(long, help = "Display name for the cloned persona")]
    pub(crate) display_name: String,
    #[arg(long, help = "Override the source's workspace root scope")]
    pub(crate) root: Option<String>,
    #[arg(long, help = r#"Override the source's model, "backend_id|model_name""#)]
    pub(crate) model: Option<String>,
    #[arg(long, help = "Override the source's inference profile")]
    pub(crate) profile_id: Option<String>,
}

/// See [`BehaviorCreateArgs`] doc comment: same materializer routing. The
/// target's `agent_did` is derived from `behavior_id` itself.
#[derive(clap::Args)]
pub(crate) struct BehaviorDisableArgs {
    #[arg(long)]
    pub(crate) graphql: String,
    #[arg(value_name = "BEHAVIOR_ID")]
    pub(crate) behavior_id: String,
}

#[derive(clap::Args)]
pub(crate) struct ToolSelectionUpsertArgs {
    #[arg(long)]
    pub(crate) graphql: String,
    #[arg(long)]
    pub(crate) agent_did: String,
    #[arg(long)]
    pub(crate) selection_id: String,
    #[arg(long)]
    pub(crate) display_name: Option<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_display_name: bool,
    #[arg(
        long,
        num_args = 0..=1,
        default_missing_value = "true",
        value_name = "BOOL",
        help = "Enable or disable file tools. Omit to preserve existing setting"
    )]
    pub(crate) enable_file_tools: Option<bool>,
    #[arg(long)]
    pub(crate) file_tools_mode: Option<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_file_tools_mode: bool,
    #[arg(
        long,
        help = "Optional per-behavior file-tool root; relative paths resolve from the daemon cwd and must stay within any node-level tool root"
    )]
    pub(crate) file_tool_root: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_file_tool_root: bool,
    #[arg(
        long,
        num_args = 0..=1,
        default_missing_value = "true",
        value_name = "BOOL",
        help = "Enable or disable bash tools. Omit to preserve existing setting"
    )]
    pub(crate) enable_bash: Option<bool>,
    #[arg(long)]
    pub(crate) bash_mode: Option<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_bash_mode: bool,
    #[arg(
        long,
        help = "Command policy for bash: read_only, workspace_write, managed_write, or unrestricted"
    )]
    pub(crate) command_execution_policy: Option<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_command_execution_policy: bool,
    #[arg(
        long,
        help = "Network policy hint for bash commands: inherit, disabled, or enabled"
    )]
    pub(crate) command_network_mode: Option<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_command_network_mode: bool,
    #[arg(
        long = "command-allowed-argv-prefix",
        help = "Argv prefix allowed for bash (subcommand-precise). When set, every command must match a prefix; also admits heads outside the read-only base allowlist. Prefer over replacing the base when extending. See docs/macos-bash-sandbox.md"
    )]
    pub(crate) command_allowed_argv_prefixes: Vec<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_command_allowed_argv_prefixes: bool,
    #[arg(
        long = "command-forbidden-argv-prefix",
        help = "Argv prefix always denied for bash (wins over allowed prefixes and the read-only allowlist)"
    )]
    pub(crate) command_forbidden_argv_prefixes: Vec<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_command_forbidden_argv_prefixes: bool,
    #[arg(long = "cli-tool-name")]
    pub(crate) cli_tool_names: Vec<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_cli_tool_names: bool,
    #[arg(
        long,
        num_args = 0..=1,
        default_missing_value = "true",
        value_name = "BOOL",
        help = "Enable or disable meta MCP tools. Omit to preserve existing setting"
    )]
    pub(crate) enable_meta_tools: Option<bool>,
    #[arg(long = "allowed-mcp-service-id")]
    pub(crate) allowed_mcp_service_ids: Vec<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_allowed_mcp_service_ids: bool,
    #[arg(
        long = "backgroundable-tool-name",
        help = "Host tool that may be spawned as a background process via spawn_process, e.g. bash_unrestricted"
    )]
    pub(crate) backgroundable_tool_names: Vec<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_backgroundable_tool_names: bool,
    #[arg(
        long,
        help = "Enable or disable the feature-gated memory tool: --enable-memory true|false. Omit to leave the existing document setting unchanged (default is disabled)"
    )]
    pub(crate) enable_memory: Option<bool>,
    #[arg(
        long,
        help = "Enable or disable the sessions history convenience tool: --enable-session-history-tool true|false. Omit to leave the existing document setting unchanged (default is disabled)"
    )]
    pub(crate) enable_session_history_tool: Option<bool>,
    #[arg(
        long,
        help = "Enable or disable the context_budget tool: --enable-context-budget true|false. Omit to leave the existing document setting unchanged (default is enabled)"
    )]
    pub(crate) enable_context_budget: Option<bool>,
    #[arg(
        long,
        help = "Enable or disable the read-only defra_query tool: --enable-defra-query true|false. Omit to leave the existing document setting unchanged (default is enabled)"
    )]
    pub(crate) enable_defra_query: Option<bool>,
    #[arg(
        long = "defra-query-collection",
        help = "Restrict defra_query to these collections (repeatable); omit for all collections"
    )]
    pub(crate) defra_query_collections: Vec<String>,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_defra_query_collections: bool,
    #[arg(
        long = "subagent-target",
        help = "SubagentTarget JSON entry allowed for spawn_subagent, e.g. \
                {\"name\":\"researcher\",\"agent_did\":\"did:key:...\",\"behavior_id\":\"did:key:...:default\",\"description\":\"...\"} \
                (repeatable); or @path/@- to read one entry or a JSON array of entries from a \
                file/stdin; omit to preserve existing targets. See `config tools \
                subagent-target-entry --help` to build a single entry from its parts."
    )]
    pub(crate) subagent_targets: Vec<String>,
    #[arg(
        long,
        default_value_t = false,
        help = "Clear existing subagent_targets when no --subagent-target values are provided"
    )]
    pub(crate) clear_subagent_targets: bool,
    #[arg(
        long,
        help = "Enable or disable spawn_subagent: --subagent-spawn-enabled true|false. Omit to preserve existing setting"
    )]
    pub(crate) subagent_spawn_enabled: Option<bool>,
    #[arg(
        long,
        help = "Enable or disable workflow orchestration tools: --orchestration-enabled true|false. Omit to preserve existing setting"
    )]
    pub(crate) orchestration_enabled: Option<bool>,
    #[arg(
        long,
        help = "Enable or disable subagent steering tools: --subagent-steering-enabled true|false. Omit to preserve existing setting"
    )]
    pub(crate) subagent_steering_enabled: Option<bool>,
    #[arg(
        long,
        help = "Enable or disable background subagent steering: --subagent-background-enabled true|false. Omit to preserve existing setting"
    )]
    pub(crate) subagent_background_enabled: Option<bool>,
    #[arg(
        long,
        help = "Enable or disable remote-DID subagent targets: --subagent-allow-cross-deployment true|false. Omit to preserve existing setting"
    )]
    pub(crate) subagent_allow_cross_deployment: Option<bool>,
    #[arg(
        long,
        help = "Cross-deployment spawn timeout in seconds. Omit to preserve existing setting"
    )]
    pub(crate) cross_deployment_spawn_timeout_seconds: Option<i64>,
    #[arg(long, default_value_t = false)]
    pub(crate) clear_cross_deployment_spawn_timeout_seconds: bool,
}

#[derive(Subcommand)]
pub(crate) enum InferenceProfileCommand {
    #[command(name = "set")]
    Set(InferenceProfileUpsertArgs),
    #[command(name = "list", about = "List InferenceProfile documents")]
    List(ConfigListArgs),
    #[command(name = "show", about = "Show an InferenceProfile document")]
    Show(ConfigShowArgs),
    #[command(
        name = "rm",
        about = "Delete an InferenceProfile document",
        alias = "remove"
    )]
    Rm(ConfigShowArgs),
}

#[derive(Subcommand)]
pub(crate) enum ConfigTriggerCommand {
    #[command(name = "list", about = "List EventTrigger documents")]
    List(ConfigListArgs),
    #[command(name = "show", about = "Show an EventTrigger document")]
    Show(ConfigShowArgs),
}

#[derive(Subcommand)]
pub(crate) enum ConfigScheduleCommand {
    #[command(name = "list", about = "List Schedule documents")]
    List(ConfigListArgs),
    #[command(name = "show", about = "Show a Schedule document")]
    Show(ConfigShowArgs),
}

#[derive(Subcommand)]
pub(crate) enum ConfigMcpCommand {
    #[command(name = "list", about = "List ToolServiceRegistry documents")]
    List(ConfigListArgs),
    #[command(name = "show", about = "Show a ToolServiceRegistry document")]
    Show(ConfigShowArgs),
}

#[derive(Subcommand)]
pub(crate) enum TaskCommand {
    #[command(name = "list", about = "List configured Task documents")]
    List(TaskListArgs),
    #[command(name = "show", about = "Show a configured Task document")]
    Show(TaskShowArgs),
    #[command(
        name = "run",
        about = "Run a configured Task once, now",
        visible_alias = "trigger"
    )]
    Run(ConfigTaskRunArgs),
}

#[derive(Debug, clap::Args)]
pub(crate) struct TaskListArgs {
    /// GraphQL endpoint of the running agent's DefraDB. Defaults to local.
    #[arg(long)]
    pub(crate) graphql: Option<String>,

    /// Path to the agent home. Used to resolve GraphQL endpoint when
    /// `--graphql` is not set.
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct TaskShowArgs {
    /// The task_id of the task to show.
    #[arg(long = "task-id")]
    pub(crate) task_id_flag: Option<String>,

    /// The task_id of the task to show.
    #[arg(value_name = "TASK_ID")]
    pub(crate) task_id: Option<String>,

    /// GraphQL endpoint of the running agent's DefraDB. Defaults to local.
    #[arg(long)]
    pub(crate) graphql: Option<String>,

    /// Path to the agent home. Used to resolve GraphQL endpoint when
    /// `--graphql` is not set.
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
pub(crate) struct ConfigTaskRunArgs {
    /// The task_id of the task to run.
    #[arg(long = "task-id")]
    pub(crate) task_id_flag: Option<String>,

    /// The task_id of the task to run.
    #[arg(value_name = "TASK_ID")]
    pub(crate) task_id: Option<String>,

    /// JSON object of arguments bound as the `args.*` template scope.
    /// Example: `--args '{"name": "Amy"}'`.
    #[arg(long, default_value = "{}")]
    pub(crate) args: String,

    /// GraphQL endpoint of the running agent's DefraDB. Defaults to local.
    #[arg(long)]
    pub(crate) graphql: Option<String>,

    /// Path to the agent home. Used to resolve GraphQL endpoint when
    /// `--graphql` is not set.
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,

    /// Wait for the created AgentRequest to reach a terminal response.
    #[arg(long, default_value_t = false)]
    pub(crate) wait: bool,

    /// Idle timeout while waiting for a terminal response.
    #[arg(long, default_value_t = crate::DEFAULT_INTERACTIVE_WAIT_TIMEOUT_SECS)]
    pub(crate) timeout_secs: u64,

    /// Poll interval while waiting for a terminal response.
    #[arg(long, default_value_t = 1)]
    pub(crate) poll_secs: u64,
}

#[derive(clap::Args)]
pub(crate) struct InferenceProfileUpsertArgs {
    #[arg(long)]
    pub(crate) graphql: String,
    #[arg(long)]
    pub(crate) profile_id: String,
    #[arg(long)]
    pub(crate) display_name: Option<String>,
    #[arg(long)]
    pub(crate) context_window: Option<i64>,
    #[arg(long)]
    pub(crate) max_output_tokens: Option<i64>,
    #[arg(long)]
    pub(crate) max_turns: Option<i64>,
    #[arg(long)]
    pub(crate) temperature: Option<f64>,
    /// Sampling knobs beyond temperature (#649). Unset leaves the served
    /// model's `generation_config.json` default in force.
    #[arg(long)]
    pub(crate) top_p: Option<f64>,
    #[arg(long)]
    pub(crate) top_k: Option<i64>,
    /// Requested provider sampling seed. Reproducibility still depends on the
    /// pinned provider, model, and hardware configuration.
    #[arg(long, value_parser = clap::value_parser!(i64).range(0..))]
    pub(crate) seed: Option<i64>,
    #[arg(long)]
    pub(crate) min_p: Option<f64>,
    #[arg(long)]
    pub(crate) frequency_penalty: Option<f64>,
    #[arg(long)]
    pub(crate) presence_penalty: Option<f64>,
    #[arg(long)]
    pub(crate) repetition_penalty: Option<f64>,
    #[arg(
        long,
        value_parser = ["none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"]
    )]
    pub(crate) reasoning_effort: Option<String>,
    #[arg(long)]
    pub(crate) stream_batch_ms: Option<i64>,
    #[arg(long)]
    pub(crate) stream_liveness_timeout_secs: Option<i64>,
    #[arg(long)]
    pub(crate) deadline_duration_secs: Option<i64>,
    #[arg(long)]
    pub(crate) retry_max_transport: Option<i64>,
    #[arg(long, value_delimiter = ',')]
    pub(crate) retry_backoff_ms: Option<Vec<i64>>,
    #[arg(long)]
    pub(crate) retry_max_resample: Option<i64>,
    #[arg(long)]
    pub(crate) retry_allow_repair: Option<bool>,
    #[arg(long)]
    pub(crate) retry_interactive_max: Option<i64>,
}

#[derive(clap::Args)]
pub(crate) struct BackendUpsertArgs {
    #[arg(long)]
    pub(crate) graphql: String,
    #[arg(long)]
    pub(crate) backend_id: String,
    #[arg(long)]
    pub(crate) name: String,
    #[arg(
        long,
        value_enum,
        help = "Backend preset with provider/auth defaults for common local and hosted backends"
    )]
    pub(crate) backend_preset: Option<BackendPresetArg>,
    #[arg(
        long,
        help = "Backend provider kind. OpenAiCompatible covers OpenAI-style local and hosted endpoints"
    )]
    pub(crate) provider_kind: Option<String>,
    #[arg(
        long,
        value_enum,
        help = "OpenAI-style wire API for OpenAiCompatible backends: responses or chat-completions"
    )]
    pub(crate) openai_wire_api: Option<OpenAiWireApiArg>,
    #[arg(
        long,
        help = "Inference backend base URL, usually including /v1. Falls back to the preset default when available"
    )]
    pub(crate) endpoint: Option<String>,
    #[arg(long, help = "Raw API key stored directly in the backend document")]
    pub(crate) api_key: Option<String>,
    #[arg(
        long,
        help = "Environment variable name holding this backend's API key"
    )]
    pub(crate) api_key_env_var: Option<String>,
    #[arg(long)]
    pub(crate) max_concurrent: i64,
    #[arg(long, default_value_t = default_backend_max_queue_depth())]
    pub(crate) max_queue_depth: i64,
    #[arg(long, default_value_t = true)]
    pub(crate) enabled: bool,
    #[arg(long, default_value = "healthy")]
    pub(crate) probe_status: String,
}

#[derive(clap::Args)]
pub(crate) struct BackendDiscoverModelsArgs {
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long)]
    pub(crate) backend_id: Option<String>,
    #[arg(
        long,
        value_enum,
        help = "Backend preset with provider/auth defaults for common local and hosted backends"
    )]
    pub(crate) backend_preset: Option<BackendPresetArg>,
    #[arg(
        long,
        help = "Backend provider kind. OpenAiCompatible covers OpenAI-style local and hosted endpoints"
    )]
    pub(crate) provider_kind: Option<String>,
    #[arg(
        long,
        help = "Inference backend base URL, usually including /v1. Falls back to the preset default when available"
    )]
    pub(crate) endpoint: Option<String>,
    #[arg(long, help = "Raw API key to use for this probe only")]
    pub(crate) api_key: Option<String>,
    #[arg(long, help = "Environment variable name holding the probe API key")]
    pub(crate) api_key_env_var: Option<String>,
    #[arg(
        long,
        help = "Agent DID owning the ChatGptCodex OAuth credential (defaults to the local agent). Only used for ChatGptCodex backends, whose bearer is a DefraDB document rather than an api_key"
    )]
    pub(crate) agent_did: Option<String>,
    #[arg(
        long,
        help = "Agent home directory used to resolve the local agent DID for ChatGptCodex discovery (defaults to ~/.gents). Pass --agent-did instead to target a specific agent"
    )]
    pub(crate) home: Option<PathBuf>,
}

#[derive(clap::Args)]
pub(crate) struct ConfigExportArgs {
    #[arg(
        long,
        value_name = "ROOT",
        help = "Directory to write the manifest root into (author format for `config validate`, `diff`, and `apply`)"
    )]
    pub(crate) root: PathBuf,
    #[arg(
        long,
        default_value_t = false,
        help = "Overwrite the root dir if it is non-empty"
    )]
    pub(crate) force: bool,
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long)]
    pub(crate) agent_did: Option<String>,
    #[arg(long = "bind-agent-did", value_enum)]
    pub(crate) bind_agent_did: Option<ManifestAgentDidBindingArg>,
}

#[derive(clap::Args)]
pub(crate) struct ConfigImportArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(
        long = "override",
        default_value_t = false,
        help = "Upsert documents instead of failing when they already exist"
    )]
    pub(crate) override_existing: bool,
    #[arg(
        value_name = "PATH",
        help = "Legacy JSON bundle file to import (separate from `config export --root` manifest-dir output; use `config apply --root <dir>` to apply manifest roots). Reads stdin when omitted"
    )]
    pub(crate) path: Option<PathBuf>,
}

#[derive(clap::Args)]
pub(crate) struct ConfigValidateArgs {
    #[arg(long, value_name = "ROOT")]
    pub(crate) root: PathBuf,
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long = "bind-agent-did", value_enum)]
    pub(crate) bind_agent_did: Option<ManifestAgentDidBindingArg>,
    #[arg(long, default_value_t = false)]
    pub(crate) force_rebind_concrete_did: bool,
}

#[derive(clap::Args)]
pub(crate) struct ConfigDiffArgs {
    #[arg(long, value_name = "ROOT")]
    pub(crate) root: PathBuf,
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long = "bind-agent-did", value_enum)]
    pub(crate) bind_agent_did: Option<ManifestAgentDidBindingArg>,
    #[arg(long, default_value_t = false)]
    pub(crate) force_rebind_concrete_did: bool,
}

#[derive(clap::Args)]
pub(crate) struct ConfigApplyArgs {
    #[arg(long, value_name = "ROOT")]
    pub(crate) root: PathBuf,
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long = "bind-agent-did", value_enum)]
    pub(crate) bind_agent_did: Option<ManifestAgentDidBindingArg>,
    #[arg(long, default_value_t = false)]
    pub(crate) force_rebind_concrete_did: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Delete live-only desired-state documents absent from the manifest, routed through the proven ApplyReconcile delete-safety model"
    )]
    pub(crate) prune: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum ManifestAgentDidBindingArg {
    Home,
    Live,
}

#[derive(Subcommand)]
pub(crate) enum SchemaCommand {
    #[command(about = "Apply SDL and JSON Patch schema files")]
    Apply(SchemaApplyArgs),
}

#[derive(clap::Args)]
pub(crate) struct SchemaApplyArgs {
    #[arg(long, help = "Agent home directory. Defaults to ~/.gents")]
    pub(crate) home: Option<PathBuf>,
    #[arg(
        long,
        help = "GraphQL endpoint to apply to instead of local home state"
    )]
    pub(crate) graphql: Option<String>,
    #[arg(
        value_name = "PATH",
        help = "Schema file or directory. Directories apply *.graphql/*.gql files, then *.patch.json/*.json-patch files"
    )]
    pub(crate) path: PathBuf,
    #[arg(
        long = "patch",
        value_name = "PATCH",
        help = "Extra JSON Patch file to apply after SDL files. May be repeated"
    )]
    pub(crate) patches: Vec<PathBuf>,
}

#[derive(Subcommand)]
pub(crate) enum P2pCommand {
    #[command(about = "Show live P2P connectivity for the running runtime")]
    Status(P2pAccessArgs),
    #[command(about = "List connected peers for the running runtime")]
    Peers(P2pAccessArgs),
    #[command(about = "Run P2P HTTP endpoint diagnostics")]
    Diagnose(P2pAccessArgs),
    #[command(
        about = "Manage declarative P2P pairings (the runtime reconciles them)",
        after_help = "\
Pairings are declarative: every subcommand here writes or reads \
PeerPairingDesired documents, and the running runtime reconciles live P2P \
state toward them. This is the normal way to configure P2P. The imperative \
`p2p admin` commands mutate live wiring directly and are an escape hatch for \
diagnostics, break-glass repair, and non-paired topologies."
    )]
    Pairings {
        #[command(subcommand)]
        command: P2pPairingsCommand,
    },
    #[command(
        about = "Register into and inspect the peer discovery registry",
        after_help = "\
The peer registry (PeerRegistry collection) is the service-discovery layer: \
nodes self-register here, peers replicate the collection, and the discovery \
reconciler materializes registry-owned PeerPairingDesired rows for live peers.\n\
\n\
`p2p network register` — write this node's row (idempotent / upsert).\n\
`p2p network list`     — read all PeerRegistry rows with liveness and pairing annotations.\n\
`p2p network rm`       — deregister this node (delete its PeerRegistry row)."
    )]
    Network {
        #[command(subcommand)]
        command: P2pNetworkCommand,
    },
    #[command(
        about = "Low-level live P2P admin (escape hatch — prefer `p2p pairings`)",
        after_help = "\
These commands mutate live P2P state on the running runtime directly, without \
writing PeerPairingDesired documents — so the pairing reconciler does not own \
or manage what they install. Use them for diagnostics, break-glass repair when \
the reconciler is the suspect component, and intentionally ad-hoc topologies \
(one-off document fetches, test connectivity). For normal pairing, use \
`p2p pairings`."
    )]
    Admin {
        #[command(subcommand)]
        command: P2pAdminCommand,
    },
    #[command(
        about = "Inspect the built-in scope-template catalog",
        after_help = "\
Scope templates are named pairing intents that bundle a fixed collection set, \
a per-collection scoping policy, and a delivery \
mode (push or replicate).  Use them as the --template argument when creating \
PeerPairingDesired rows."
    )]
    Templates {
        #[command(subcommand)]
        command: P2pTemplatesCommand,
    },
}

/// Subcommands for `p2p network` — the peer-registry front door.
#[derive(Subcommand)]
pub(crate) enum P2pNetworkCommand {
    #[command(about = "Register this node into the peer discovery registry (idempotent upsert)")]
    Register(P2pNetworkRegisterArgs),
    #[command(about = "List PeerRegistry rows with liveness and pairing annotations")]
    List(P2pNetworkListArgs),
    #[command(
        name = "rm",
        about = "Deregister this node from the peer discovery registry",
        aliases = ["deregister", "remove"]
    )]
    Rm(P2pAccessArgs),
    #[command(about = "Create the local singleton AgentNetwork control-plane root")]
    Create(P2pNetworkCreateArgs),
    #[command(about = "Grant active network membership to a member DID")]
    Grant(P2pNetworkGrantArgs),
    #[command(about = "Revoke network membership with a retained tombstone")]
    Revoke(P2pNetworkRevokeArgs),
}

#[derive(clap::Args)]
pub(crate) struct P2pNetworkRegisterArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    /// Human-readable display name for this node in the registry.
    #[arg(long, value_name = "NAME")]
    pub(crate) display_name: Option<String>,
    /// Scope template this node offers (repeatable). A node advertises the
    /// templates it is willing to replicate; a discovering peer materializes a
    /// scoped pairing from one of them. Defaults to `conversation` (filtered
    /// push of the peer's conversation slice) when none are given.
    #[arg(long = "template", value_name = "TEMPLATE")]
    pub(crate) templates: Vec<String>,
    /// Network / fleet id. Defaults to "default".
    #[arg(long = "network", value_name = "NETWORK_ID")]
    pub(crate) network_id: Option<String>,
}

#[derive(clap::Args)]
pub(crate) struct P2pNetworkListArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub(crate) output: OutputFormat,
}

#[derive(Debug, clap::Args)]
pub(crate) struct P2pNetworkCreateArgs {
    /// Human-readable network name; network_id is derived from (admin_did, name).
    #[arg(long)]
    pub(crate) name: String,
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long = "output", value_enum, default_value_t = OutputFormat::Text)]
    pub(crate) output: OutputFormat,
}

#[derive(Debug, clap::Args)]
pub(crate) struct P2pNetworkGrantArgs {
    /// The member DID to admit.
    pub(crate) member_did: String,
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long = "output", value_enum, default_value_t = OutputFormat::Text)]
    pub(crate) output: OutputFormat,
}

#[derive(Debug, clap::Args)]
pub(crate) struct P2pNetworkRevokeArgs {
    /// The member DID to revoke.
    pub(crate) member_did: String,
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long = "output", value_enum, default_value_t = OutputFormat::Text)]
    pub(crate) output: OutputFormat,
}

/// Subcommands for `p2p templates`.
#[derive(Subcommand)]
pub(crate) enum P2pTemplatesCommand {
    #[command(about = "List all built-in scope templates")]
    List(P2pTemplatesListArgs),
}

#[derive(clap::Args)]
pub(crate) struct P2pTemplatesListArgs {
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub(crate) output: OutputFormat,
}

/// Low-level live P2P admin commands. Escape hatch beneath `p2p pairings`.
#[derive(Subcommand)]
pub(crate) enum P2pAdminCommand {
    #[command(about = "Connect the running runtime to another peer")]
    Connect(P2pConnectArgs),
    #[command(about = "Manage live collection subscriptions on the running runtime")]
    Collections {
        #[command(subcommand)]
        command: P2pCollectionsCommand,
    },
    #[command(about = "Manage live push replicators on the running runtime")]
    Replicators {
        #[command(subcommand)]
        command: P2pReplicatorsCommand,
    },
    #[command(about = "Manage live document subscriptions and document sync")]
    Documents {
        #[command(subcommand)]
        command: P2pDocumentsCommand,
    },
}

#[derive(clap::Args)]
pub(crate) struct P2pAccessArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
}

#[derive(clap::Args)]
pub(crate) struct P2pConnectArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long)]
    pub(crate) peer: String,
}

#[derive(Subcommand)]
pub(crate) enum P2pCollectionsCommand {
    #[command(about = "List subscribed P2P collections")]
    List(P2pAccessArgs),
    #[command(about = "Subscribe collections or collection profiles for P2P replication")]
    Add(P2pCollectionsMutateArgs),
    #[command(about = "Remove subscribed P2P collections")]
    Remove(P2pCollectionsMutateArgs),
    #[command(about = "Fetch a branchable collection DAG from connected peers")]
    SyncBranchable(P2pSyncBranchableArgs),
    #[command(about = "Fetch collection-version DAG blocks from connected peers")]
    SyncVersions(P2pSyncVersionsArgs),
}

#[derive(Subcommand)]
pub(crate) enum P2pReplicatorsCommand {
    #[command(about = "List configured P2P replicators")]
    List(P2pAccessArgs),
    #[command(about = "Configure a peer replicator for collections or profiles")]
    Add(P2pReplicatorAddArgs),
    #[command(about = "Remove a peer replicator for collections or profiles")]
    Remove(P2pReplicatorRemoveArgs),
}

#[derive(Subcommand)]
pub(crate) enum P2pDocumentsCommand {
    #[command(about = "List document subscriptions for P2P replication")]
    List(P2pAccessArgs),
    #[command(about = "Subscribe documents for P2P replication")]
    Add(P2pDocumentsMutateArgs),
    #[command(about = "Remove document subscriptions from P2P replication")]
    Remove(P2pDocumentsMutateArgs),
    #[command(about = "Fetch documents from connected peers")]
    Sync(P2pDocumentsSyncArgs),
}

#[derive(Subcommand)]
pub(crate) enum P2pPairingsCommand {
    #[command(about = "List desired pairings annotated with live health")]
    List(P2pPairingsListArgs),
    #[command(
        about = "Create or update a desired pairing row (the runtime reconciles it)",
        after_help = "\
Writes a PeerPairingDesired row. A running gents runtime reads desired \
rows on its pairing sweep and reconciles live P2P state toward them — this \
command never mutates live wiring itself.\n\
\n\
--did (the expected remote agent DID) is required: the DID is the permission \
and audit boundary, so a pairing always names who it trusts. If you do not \
know the remote DID, use `p2p pairings invite` / `p2p pairings join`, which \
carry it for you.\n\
\n\
--peer may be omitted when a --address is a shareable ticket or multiaddr; the \
peer id is derived from it.\n\
\n\
Pairing is directional. For bidirectional delegation, create a row on BOTH \
servers, each naming the other.\n\
\n\
NOTE: replication alone does NOT enable cross-deployment delegation. That is \
off by default and DEFERRED — opt in with `subagent_allow_cross_deployment: \
true` on the relevant behaviors' tool selections on BOTH servers \
(trusted-fleet only)."
    )]
    Set(P2pPairingSetArgs),
    #[command(
        name = "rm",
        about = "Remove a desired pairing row (runtime tears down only what it applied)",
        aliases = ["unpair", "remove"]
    )]
    Remove(P2pPairingRefArgs),
    #[command(about = "Create a shareable, DID-carrying pairing invite token")]
    Invite(P2pInviteArgs),
    #[command(about = "Accept a pairing invite token and write a desired row")]
    Join(P2pJoinArgs),
    #[command(
        about = "Claim an audience-unbound bearer invite token (scan-one-QR pairing)",
        long_about = "Claim a dabear1- bearer invite: verify the issuer signature, wire the \
local push pairing toward the issuer, and publish a self-signed claim that the \
issuer's daemon turns into a membership grant (and, for conversation invites, \
the reciprocal conversation edge)."
    )]
    Claim(P2pClaimArgs),
}

#[derive(clap::Args)]
pub(crate) struct P2pPairingSetArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    /// Remote peer ID. Optional when a --address is a shareable ticket or
    /// multiaddr the peer id can be derived from.
    #[arg(long = "peer", alias = "peer-id", value_name = "PEER_ID")]
    pub(crate) peer_id: Option<String>,
    /// Agent DID expected for the remote peer. Required: the DID is the trust boundary.
    #[arg(long = "did", alias = "agent-did", value_name = "AGENT_DID")]
    pub(crate) agent_did: String,
    /// Replicator address (shareable ticket or multiaddr) to install during
    /// reconcile. Repeat for multiple addresses.
    #[arg(long = "address", value_name = "ADDRESS")]
    pub(crate) addresses: Vec<String>,
    /// Scope template id driving the pairing (collections + scope + delivery).
    /// Defaults to `conversation` (filtered push of the peer's conversation slice).
    /// Use `p2p templates list` to see all available templates.
    #[arg(
        long = "template",
        value_name = "TEMPLATE",
        default_value = "conversation"
    )]
    pub(crate) template: String,
    /// Wait for the runtime to observe the peer as connected.
    #[arg(long, default_value_t = false)]
    pub(crate) wait: bool,
    /// Wait timeout such as 30s, 5m, or 1h. Only used with --wait.
    #[arg(long, default_value = "30s")]
    pub(crate) timeout: String,
}

#[derive(clap::Args)]
pub(crate) struct P2pPairingRefArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    /// Remote peer ID stored in PeerPairingDesired.peer_id.
    #[arg(long = "peer", alias = "peer-id", value_name = "PEER_ID")]
    pub(crate) peer_id: String,
}

#[derive(clap::Args)]
pub(crate) struct P2pPairingsListArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub(crate) output: OutputFormat,
}

#[derive(clap::Args)]
pub(crate) struct P2pInviteArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    /// Scope template id for this invite (e.g. `conversation`, `agent-config`, `backup`).
    /// The template is encoded in the invite token and read by `p2p pairings join`.
    /// Defaults to `conversation` (filtered push of the peer's conversation slice).
    /// Use `p2p templates list` to see all available templates.
    #[arg(
        long = "template",
        value_name = "TEMPLATE",
        default_value = "conversation"
    )]
    pub(crate) template: String,
    /// Member DID this invite admits. The DID must already have an active
    /// admin-signed NetworkMembership grant on the issuer.
    #[arg(long = "member-did", value_name = "DID")]
    pub(crate) member_did: Option<String>,
    /// Mint an audience-unbound bearer invite (`dabear1-`) instead of a
    /// DID-bound one. The claiming device binds itself at claim time with
    /// `p2p pairings claim`; the running issuer daemon authors the membership
    /// grant (and, for conversation invites, the reciprocal intent) when the
    /// claim replicates in. Bearer invites are single-use and expire after
    /// 5 minutes.
    #[arg(
        long = "bearer",
        default_value_t = false,
        conflicts_with = "member_did"
    )]
    pub(crate) bearer: bool,
    /// Render the minted token as a scannable QR code on stdout (bearer only).
    #[arg(long = "qr", default_value_t = false, requires = "bearer")]
    pub(crate) qr: bool,
}

#[derive(clap::Args)]
pub(crate) struct P2pClaimArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    /// The `dabear1-` bearer invite token to claim.
    #[arg(value_name = "TOKEN")]
    pub(crate) token: String,
}

#[derive(clap::Args)]
pub(crate) struct P2pJoinArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    /// Invite token produced by `gents p2p pairings invite`.
    #[arg(value_name = "TOKEN")]
    pub(crate) token: String,
    /// Override the scope template from the token. When both `--template` and a
    /// token template are present, `--template` wins. Omit to use the token's
    /// template (the normal path).
    #[arg(long = "template", value_name = "TEMPLATE")]
    pub(crate) template: Option<String>,
    /// Deprecated compatibility flag accepted by older scripts. v5 joins always
    /// verify the token's admin-signed network root and membership grant.
    #[arg(long, default_value_t = false)]
    pub(crate) reciprocal: bool,
    /// Wait for the runtime to observe the peer as connected.
    #[arg(long, default_value_t = false)]
    pub(crate) wait: bool,
    /// Wait timeout such as 30s, 5m, or 1h. Only used with --wait.
    #[arg(long, default_value = "30s")]
    pub(crate) timeout: String,
}

#[derive(clap::Args)]
pub(crate) struct P2pCollectionsMutateArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long = "collection", value_name = "COLLECTION")]
    pub(crate) collections: Vec<String>,
    #[arg(long = "profile", value_enum, value_name = "PROFILE")]
    pub(crate) profiles: Vec<P2pCollectionProfileArg>,
}

#[derive(clap::Args)]
pub(crate) struct P2pSyncBranchableArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long = "collection-id", value_name = "COLLECTION_ID")]
    pub(crate) collection_id: String,
}

#[derive(clap::Args)]
pub(crate) struct P2pSyncVersionsArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long = "version-id", value_name = "VERSION_ID")]
    pub(crate) version_ids: Vec<String>,
}

#[derive(clap::Args)]
pub(crate) struct P2pReplicatorAddArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long)]
    pub(crate) peer: String,
    #[arg(long = "collection", value_name = "COLLECTION")]
    pub(crate) collections: Vec<String>,
    #[arg(long = "profile", value_enum, value_name = "PROFILE")]
    pub(crate) profiles: Vec<P2pCollectionProfileArg>,
    /// Per-collection field-equality filter for filtered replication (repeatable).
    /// Format: `<collection>:<field>=<value>`, e.g.
    /// `AgentRequest:agent_did=did:key:alice`. Forwarded to the node as the
    /// replicator's `Filters`, which installs a filtered (push-only) replicator
    /// that sends only matching documents. The filter field must be `@immutable`.
    #[arg(long = "filter", value_name = "COLLECTION:FIELD=VALUE")]
    pub(crate) filters: Vec<String>,
}

#[derive(clap::Args)]
pub(crate) struct P2pReplicatorRemoveArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long)]
    pub(crate) peer: String,
    #[arg(long = "collection", value_name = "COLLECTION")]
    pub(crate) collections: Vec<String>,
    #[arg(long = "profile", value_enum, value_name = "PROFILE")]
    pub(crate) profiles: Vec<P2pCollectionProfileArg>,
}

#[derive(clap::Args)]
pub(crate) struct P2pDocumentsMutateArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long = "doc-id", value_name = "DOC_ID")]
    pub(crate) doc_ids: Vec<String>,
}

#[derive(clap::Args)]
pub(crate) struct P2pDocumentsSyncArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long, value_name = "COLLECTION")]
    pub(crate) collection: String,
    #[arg(long = "doc-id", value_name = "DOC_ID")]
    pub(crate) doc_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum P2pCollectionProfileArg {
    Runtime,
    Agent,
    DesktopConfig,
    ChatRequests,
    ToolServices,
    Discovery,
}

#[derive(Subcommand)]
pub(crate) enum RequestCommand {
    #[command(
        about = "Create an AgentRequest document and optionally wait for the final AgentResponse"
    )]
    Submit(RequestSubmitArgs),
    #[command(about = "Show a stored AgentRequest document")]
    Show(RequestShowArgs),
    #[command(about = "Signal interrupt on an in-flight request (idempotent latch)")]
    Interrupt(RequestInterruptArgs),
    #[command(about = "Resend a stale-terminal request with a fresh TTL")]
    Resend(RequestResendArgs),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum RequestInterruptCauseArg {
    #[value(name = "interrupted")]
    Interrupted,
    #[value(name = "deadline")]
    Deadline,
    #[value(name = "userCancelled")]
    UserCancelled,
}

impl From<RequestInterruptCauseArg> for gents::tool_call_lifecycle::CancelCause {
    fn from(value: RequestInterruptCauseArg) -> Self {
        match value {
            RequestInterruptCauseArg::Interrupted => Self::Interrupted,
            RequestInterruptCauseArg::Deadline => Self::Deadline,
            RequestInterruptCauseArg::UserCancelled => Self::UserCancelled,
        }
    }
}

#[derive(clap::Args)]
pub(crate) struct RequestSubmitArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long)]
    pub(crate) agent_did: Option<String>,
    #[arg(long)]
    pub(crate) content: Option<String>,
    #[arg(long = "content-file")]
    pub(crate) content_file: Option<PathBuf>,
    #[arg(long)]
    pub(crate) session_id: Option<String>,
    #[arg(long)]
    pub(crate) behavior_id: Option<String>,
    #[arg(long)]
    pub(crate) temperature: Option<f64>,
    #[arg(long)]
    pub(crate) top_p: Option<f64>,
    #[arg(long)]
    pub(crate) top_k: Option<i64>,
    #[arg(long, value_parser = clap::value_parser!(i64).range(0..))]
    pub(crate) seed: Option<i64>,
    #[arg(long)]
    pub(crate) max_tokens: Option<i64>,
    #[arg(long)]
    pub(crate) metadata: Option<String>,
    #[arg(
        long = "valid-until",
        help = "TTL for this request (e.g. 30s, 5m, 2h, 1d). Default: 5m. Use \"none\" or 0 to disable."
    )]
    pub(crate) valid_until: Option<String>,
    #[arg(long = "output-file")]
    pub(crate) output_file: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    pub(crate) no_wait: bool,
    #[arg(long, default_value_t = crate::DEFAULT_INTERACTIVE_WAIT_TIMEOUT_SECS)]
    pub(crate) timeout_secs: u64,
    #[arg(long, default_value_t = 1)]
    pub(crate) poll_secs: u64,
}

#[derive(clap::Args)]
pub(crate) struct RequestInterruptArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(
        long,
        value_enum,
        default_value_t = RequestInterruptCauseArg::UserCancelled,
        help = "Reason for the interrupt: userCancelled for operator action, deadline for timeout-driven cancellation, interrupted for propagated runtime interruption"
    )]
    pub(crate) cause: RequestInterruptCauseArg,
    #[arg(long, default_value_t = false)]
    pub(crate) wait: bool,
    #[arg(
        long,
        value_name = "DURATION",
        default_value = "30s",
        help = "Maximum time to wait for a terminal request state when --wait is set"
    )]
    pub(crate) timeout: String,
    #[arg(
        long,
        value_enum,
        default_value_t = OutputFormat::Text,
        help = "Output format; use json for scripts"
    )]
    pub(crate) output: OutputFormat,
    #[arg(long = "request-id")]
    pub(crate) request_id_flag: Option<String>,
    #[arg(value_name = "REQUEST_ID")]
    pub(crate) request_id: Option<String>,
}

#[derive(clap::Args)]
pub(crate) struct RequestResendArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long = "request-id")]
    pub(crate) request_id_flag: Option<String>,
    #[arg(value_name = "REQUEST_ID")]
    pub(crate) request_id: Option<String>,
    #[arg(long = "output-file")]
    pub(crate) output_file: Option<PathBuf>,
    #[arg(long, default_value_t = true)]
    pub(crate) no_wait: bool,
    #[arg(long, default_value_t = crate::DEFAULT_INTERACTIVE_WAIT_TIMEOUT_SECS)]
    pub(crate) timeout_secs: u64,
    #[arg(long, default_value_t = 1)]
    pub(crate) poll_secs: u64,
}

#[derive(clap::Args)]
pub(crate) struct RequestShowArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long = "output", value_enum, default_value_t = OutputFormat::Text)]
    pub(crate) output: OutputFormat,
    #[arg(long = "request-id")]
    pub(crate) request_id_flag: Option<String>,
    #[arg(value_name = "REQUEST_ID")]
    pub(crate) request_id: Option<String>,
}

#[derive(Subcommand)]
pub(crate) enum SubagentCommand {
    #[command(
        name = "list",
        about = "List subagent dispatch lineage",
        after_help = SUBAGENT_LIST_AFTER_HELP
    )]
    List(SubagentListArgs),
    #[command(about = "Cancel a subagent request and optionally cascade to linked children")]
    Cancel(SubagentCancelArgs),
}

#[derive(clap::Args)]
pub(crate) struct SubagentListArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long, value_name = "REQUEST_ID")]
    pub(crate) root: Option<String>,
    #[arg(long, value_name = "N")]
    pub(crate) depth: Option<usize>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Tree)]
    pub(crate) output: OutputFormat,
}

#[derive(clap::Args)]
pub(crate) struct SubagentCancelArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long)]
    pub(crate) agent_did: Option<String>,
    #[arg(long = "request-id")]
    pub(crate) request_id_flag: Option<String>,
    #[arg(value_name = "REQUEST_ID")]
    pub(crate) request_id: Option<String>,
    #[arg(
        long,
        default_value_t = true,
        default_missing_value = "true",
        num_args = 0..=1,
        action = ArgAction::Set,
        help = "Cancel linked subagent bridge tool-calls and interrupt linked child requests when their cancel policy allows it"
    )]
    pub(crate) cascade: bool,
    #[arg(
        long,
        default_value = "userCancelled",
        help = "CancelCause vocabulary value included in output and persisted for local bridge lifecycle cancellations: interrupted, deadline, or userCancelled"
    )]
    pub(crate) cause: String,
    #[arg(
        long,
        default_value_t = false,
        help = "Wait until affected requests are terminal"
    )]
    pub(crate) wait: bool,
    #[arg(
        long,
        value_name = "DURATION",
        help = "Wait timeout such as 30s, 5m, or 1h. Only valid with --wait"
    )]
    pub(crate) timeout: Option<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub(crate) output: OutputFormat,
}

#[derive(Subcommand)]
pub(crate) enum SessionCommand {
    #[command(about = "List AgentSession documents")]
    List(ConfigListArgs),
    #[command(about = "Show an AgentSession document")]
    Show(ConfigShowArgs),
    #[command(about = "Fork an existing session at a user-turn boundary")]
    Fork(SessionForkArgs),
}

#[derive(Subcommand)]
pub(crate) enum GoalCommand {
    #[command(about = "Show the durable goal for a session")]
    Show(GoalShowArgs),
    #[command(about = "Create or update the durable goal for a session")]
    Set(GoalSetArgs),
    #[command(about = "Delete the durable goal for a session")]
    Clear(GoalShowArgs),
}

#[derive(clap::Args)]
pub(crate) struct GoalScopeArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long, help = "GraphQL endpoint for a running runtime")]
    pub(crate) graphql: Option<String>,
    #[arg(
        long,
        help = "Override the goal owner DID (defaults to local identity)"
    )]
    pub(crate) agent_did: Option<String>,
    #[arg(long, value_name = "SESSION_ID")]
    pub(crate) session: String,
}

#[derive(clap::Args)]
pub(crate) struct GoalShowArgs {
    #[command(flatten)]
    pub(crate) scope: GoalScopeArgs,
    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    pub(crate) output: OutputFormat,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum GoalStatusArg {
    Active,
    Paused,
    Blocked,
    UsageLimited,
    BudgetLimited,
    Complete,
}

#[derive(clap::Args)]
pub(crate) struct GoalSetArgs {
    #[command(flatten)]
    pub(crate) scope: GoalScopeArgs,
    #[arg(long, help = "Goal objective; required when creating a goal")]
    pub(crate) objective: Option<String>,
    #[arg(long, value_enum)]
    pub(crate) status: Option<GoalStatusArg>,
    #[arg(long, value_name = "TOKENS", help = "Positive charged-token budget")]
    pub(crate) token_budget: Option<i64>,
    #[arg(
        long,
        conflicts_with = "token_budget",
        help = "Remove the charged-token budget"
    )]
    pub(crate) clear_token_budget: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    pub(crate) output: OutputFormat,
}

#[derive(clap::Args)]
pub(crate) struct SessionForkArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(
        long,
        help = "GraphQL endpoint for forking through a running runtime instead of opening local state"
    )]
    pub(crate) graphql: Option<String>,
    #[arg(
        long,
        help = "Override the caller agent DID (defaults to local identity)"
    )]
    pub(crate) agent_did: Option<String>,
    #[arg(long, value_name = "SOURCE_SESSION_ID")]
    pub(crate) from: String,
    #[arg(
        long,
        value_name = "N",
        help = "0-based user-turn index; fork cuts before this user message"
    )]
    pub(crate) at_user_turn: u32,
    #[arg(
        long,
        help = "Target behavior_id for the child; omit to inherit the parent's behavior"
    )]
    pub(crate) behavior: Option<String>,
}

#[derive(Subcommand)]
pub(crate) enum ResponseCommand {
    #[command(about = "Show the latest AgentResponse for a request")]
    Show(ResponseShowArgs),
    #[command(about = "Wait until a request reaches a terminal AgentResponse")]
    Wait(ResponseWaitArgs),
}

#[derive(clap::Args)]
pub(crate) struct ResponseShowArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long = "request-id")]
    pub(crate) request_id_flag: Option<String>,
    #[arg(value_name = "REQUEST_ID")]
    pub(crate) request_id: Option<String>,
}

#[derive(clap::Args)]
pub(crate) struct ResponseWaitArgs {
    #[arg(long)]
    pub(crate) home: Option<PathBuf>,
    #[arg(long)]
    pub(crate) graphql: Option<String>,
    #[arg(long = "request-id")]
    pub(crate) request_id_flag: Option<String>,
    #[arg(value_name = "REQUEST_ID")]
    pub(crate) request_id: Option<String>,
    #[arg(long, default_value_t = crate::DEFAULT_INTERACTIVE_WAIT_TIMEOUT_SECS)]
    pub(crate) timeout_secs: u64,
    #[arg(long, default_value_t = 1)]
    pub(crate) poll_secs: u64,
}

#[cfg(test)]
mod tests;
