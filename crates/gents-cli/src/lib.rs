use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use gents::defra_node::{EmbeddedNode, NodeBuilder, StorageBackend};
use serde::de::DeserializeOwned;
use serde::Serialize;

mod cli;
mod commands;
mod config_bundle;
mod config_import;
mod config_writes;
mod desired_state;
mod graphql_access;
mod home_state;
mod http;
mod interactive_backend;
mod p2p_relay;
mod request_helpers;
mod resolve_helpers;
pub mod server_host;
mod shared;
mod telemetry;

use cli::*;
use shared::*;

use config_bundle::*;
use config_import::*;
use config_writes::ConfigAccess;
use graphql_access::*;
use home_state::*;
use request_helpers::*;
use resolve_helpers::*;

const DEFAULT_AGENT_NAME: &str = "default";
const DEFAULT_INIT_ENDPOINT: &str = "http://127.0.0.1:8080/v1";
const DEFAULT_INIT_MODEL_NAME: &str = "google/gemma-4-12B-it-qat-q4_0-gguf";
const DEFAULT_OLLAMA_ENDPOINT: &str = "http://localhost:11434/v1";
const DEFAULT_OLLAMA_MODEL_NAME: &str = "hf.co/google/gemma-4-12B-it-qat-q4_0-gguf";
const DEFAULT_CHATGPT_CODEX_MODEL_NAME: &str = "gpt-5.5";
const DEFAULT_XAI_GROK_OAUTH_MODEL_NAME: &str = "grok-4.5";
const DEFAULT_HTTP_PORT: u16 = 9191;
const DEFAULT_CODEX_SHIM_PORT: u16 = 9292;
const DEFAULT_CODEX_REMOTE: &str = "ws://127.0.0.1:9292/";
const DEFAULT_INTERACTIVE_WAIT_TIMEOUT_SECS: u64 = 1_800;
const DEFAULT_CODEX_SHIM_TIMEOUT_SECS: u64 = DEFAULT_INTERACTIVE_WAIT_TIMEOUT_SECS;
const TOKIO_WORKER_STACK_SIZE: usize = 16 * 1024 * 1024;
const DEFAULT_P2P_MAX_CONCURRENT_DAG_FETCHES: usize = p2p::sync::DEFAULT_MAX_CONCURRENT_DAG_FETCHES;
const DEFAULT_P2P_MAX_CONCURRENT_PUSH_TASKS: usize = p2p::sync::DEFAULT_MAX_CONCURRENT_PUSH_TASKS;
const DEFAULT_P2P_RATE_LIMIT_BURST: u32 = p2p::sync::DEFAULT_RATE_LIMIT_BURST;
const DEFAULT_P2P_RATE_LIMIT_RATE: f64 = p2p::sync::DEFAULT_RATE_LIMIT_RATE;
const DEFAULT_P2P_MAX_PENDING_DAGS: usize = p2p::sync::DEFAULT_MAX_PENDING_DAGS;
const DEFAULT_LOG_FILTER: &str = concat!(
    "warn,",
    "gents::agent::runtime=info,",
    "gents::agent::daemon=info,",
    "gents::agent::reconcile=info,",
    "gents::hook=info,",
    "gents::session::sessions=info,",
    "gents::streaming=info,",
    "gents::trigger_engine=info"
);
const INIT_CONFIG_FILE_NAME: &str = "init.json";
const RUNTIME_STATE_FILE_NAME: &str = "runtime.json";
const CLI_AFTER_HELP: &str = "\
Quick start:
  gents init
  gents server
  gents codex

Or without the Codex TUI:
  gents chat

Inspect the local runtime:
  gents status
  gents response show REQUEST_ID
  gents task list
  gents task show TASK_ID
  gents task run TASK_ID --wait
  gents background list
  gents mcp probe --all
  gents reset

Update runtime documents:
  gents config backend set ...
  gents config behavior set ...
  gents config tools set ...";
const INIT_AFTER_HELP: &str = "\
Bootstrap a local home directory with one default backend, one default behavior, and a safe read-only tool selection.

Examples:
  gents init
  gents init --inference-url http://HOST:PORT/v1 --model-name MODEL
  gents init --backend-preset openrouter --model-name MODEL
  gents init --backend-preset openai --model-name MODEL
  gents init --write
  gents init --yolo
  gents init --inference-url $INFERENCE_ENDPOINT --model-name MODEL --write
  gents init --enable-memory --defra-query-collection AgentRequest
  gents init --identity-only
  gents init --identity-only --identity-backend macos-keychain --keychain-label LABEL
  gents init --identity-only --identity-backend macos-secure-enclave --secure-enclave-label LABEL

Next:
  llama-server -hf google/gemma-4-12B-it-qat-q4_0-gguf
  gents server
  gents codex";
const PROVISION_AFTER_HELP: &str = "\
Provision binds a portable manifest root to this host's initialized identity,
applies it locally, and verifies an exact post-apply diff.

Examples:
  gents provision --home /path/to/home --root infra/agents/HOST/AGENT
  gents provision --root infra/agents/mini-1/mini-1-steward
  gents provision --root infra/agents/dev/dev-agent --bootstrap-file-identity
  gents provision --root infra/agents/mini-1/mini-1-steward --bootstrap-macos-keychain --keychain-label LABEL
  gents provision --root infra/agents/mini-1/mini-1-steward --bootstrap-macos-secure-enclave --secure-enclave-label LABEL

Production low-level flow:
  gents init --identity-only --identity-backend macos-keychain --keychain-label LABEL
  gents init --identity-only --identity-backend macos-secure-enclave --secure-enclave-label LABEL
  gents config apply --root <root> --home <home> --bind-agent-did home
  gents config diff --root <root> --home <home> --bind-agent-did home

File-key development flow:
  gents provision --root <root> --bootstrap-file-identity";
const RESET_AFTER_HELP: &str = "\
Examples:
  gents reset
  gents reset --home /path/to/home";
const SERVER_AFTER_HELP: &str = "\
`server` reads the initialized home directory, starts the embedded DefraDB runtime, serves GraphQL locally, and starts IROH P2P for desktop pairing.

Common flow:
  gents init
  gents server
  gents codex

The server runs the Codex TUI endpoint by default (loopback only); disable it
with --no-codex-shim. `gents codex` in another terminal connects to it.

For authenticated remote use, terminate TLS in a reverse proxy to the loopback
listener and advertise that endpoint:
  gents server --codex-shim-auth-token-env GENTS_REMOTE_TOKEN --codex-shim-public-url wss://<host>:443/
  gents codex --remote wss://<host>:443/ --remote-auth-token-env GENTS_REMOTE_TOKEN

Identity note:
  Standalone server startup supports file keys, macOS keychain software-key homes initialized with identity_backend=macos-keychain, and macOS Secure Enclave homes initialized with identity_backend=macos-secure-enclave.
  Homes with a real agent DID and no key_path must include a supported identity_backend and label in init.json.";
const CHAT_AFTER_HELP: &str = "\
Examples:
  gents chat
  gents chat \"summarize this repo\"
  gents chat --session-id SESSION_ID \"continue the previous conversation\"

Diagnostics:
  gents status
  gents response show REQUEST_ID";
const CODEX_AFTER_HELP: &str = "\
Launches the `codex` terminal UI as a separate process connected to the local
agent's Codex shim. Codex-side approvals and sandboxing are bypassed: the tool
preset chosen at `gents init` (read-only by default) is the permission boundary.

Examples:
  gents codex
  gents codex \"what is in this directory?\"
  gents codex --remote ws://127.0.0.1:9292/

Requires a running `gents server` in another terminal and `codex` on PATH.
Set GENTS_CODEX_BIN to use a different Codex executable.";
const P2P_AFTER_HELP: &str = "\
Examples:
  gents p2p status
  gents p2p peers --home /path/to/home
  gents p2p diagnose

  # Declarative pairing (the normal path — the runtime reconciles these):
  gents p2p pairings set --did <agent-did> --address <ticket-or-multiaddr> --template conversation
  gents p2p pairings list
  gents p2p pairings rm --peer <peer-id>
  gents p2p network create --name \"Fleet One\"
  gents p2p network grant <member-did>
  gents p2p pairings invite --member-did <member-did> --template network-control
  gents p2p pairings join <invite-token>

  # Service discovery:
  gents p2p network register
  gents p2p network list
  gents p2p templates list

  # Low-level live wiring (escape hatch — prefer `p2p pairings`):
  gents p2p admin connect --peer <peer-id-or-address>
  gents p2p admin replicators add --peer <peer-id-or-address> --collection AgentRequest --filter AgentRequest:agent_did=<agent-did>
  gents p2p admin documents sync --collection AgentRequest --doc-id <doc-id>";
const SCHEMA_AFTER_HELP: &str = "\
Apply app-specific DefraDB collection schemas to a running or local store.

Examples:
  gents schema apply ./app-schemas
  gents schema apply ./app-schemas --graphql http://127.0.0.1:9191/api/v0/graphql
  gents schema apply ./schemas/action_request.graphql --patch ./schemas/action_request.patch.json

Directory inputs apply *.graphql and *.gql files, then additive patch files
named *.patch.json or *.json-patch. Patch files contain a JSON Patch array, or
an object with Patch/patch.";
const STATUS_AFTER_HELP: &str = "\
Status reads the local runtime by default.

Examples:
  gents status
  gents status --home /path/to/home
  gents status --graphql http://127.0.0.1:9191/api/v0/graphql";
const BACKGROUND_AFTER_HELP: &str = "\
Lists AgentToolCall rows persisted with await_mode=background and enriches them with live runtime liveness when available.
Live native-process enrichment requires --graphql pointing at the running runtime; local --home reads print NATIVE_TOOL=unknown.
Native-process matches are scoped by tool name because runtime liveness does not expose per-call native process IDs.

Examples:
  gents background list
  gents background list --home /path/to/home
  gents background list --graphql http://127.0.0.1:9191/api/v0/graphql
  gents background list --request REQUEST_ID
  gents background list --state running --age-gt 5m
  gents background list --output json";
const MCP_AFTER_HELP: &str = "\
Examples:
  gents mcp probe SERVICE_ID
  gents mcp probe --all
  gents mcp probe SERVICE_ID --timeout 10s
  gents mcp probe --all --graphql http://127.0.0.1:9191/api/v0/graphql --output json";
const FLEET_AFTER_HELP: &str = "\
Shows the live fleet slot-accounting snapshot exposed by the local runtime HTTP API.

Examples:
  gents fleet slots
  gents fleet slots --home /path/to/home
  gents fleet slots --graphql http://127.0.0.1:9191/api/v0/graphql";
const TASK_AFTER_HELP: &str = "\
Inspect configured Task documents and create pending AgentRequests with manual trigger lineage.

Examples:
  gents task list
  gents task show host-check
  gents task run host-check
  gents task run host-check --args '{\"scope\":\"host\"}' --wait
  gents task run --task-id host-check --graphql http://127.0.0.1:9191/api/v0/graphql";
const SHOW_AFTER_HELP: &str = "\
Examples:
  gents status
  gents request show REQUEST_ID
  gents response show REQUEST_ID";
const TRACE_AFTER_HELP: &str = "\
Exports one JSON object per persisted AgentToolCall row. The command reads
AgentSession, AgentRequest, AgentResponse, AgentMessage, AgentBehavior, and
AgentToolCall rows, then infers Amy baseline fields without mutating runtime
state.

Examples:
  gents trace export --home /path/to/home
  gents trace export --graphql http://127.0.0.1:9191/api/v0/graphql
  gents trace export --graphql http://100.69.4.79:9191/api/v0/graphql --run-id amy-readonly-001 --limit 200 > amy-tool-calls.jsonl
  gents trace timeline --request-id REQUEST_ID --home /path/to/home
  gents trace project --projection atif --request-id REQUEST_ID --format native-json --output-file /logs/agent/trajectory.json --home /path/to/home
  gents trace project --projection openai-codex --request-id REQUEST_ID --redaction public --home /path/to/home
  gents trace project --projection langgraph --request-id REQUEST_ID --format jsonl --home /path/to/home
  gents trace project --projection multi-agent --request-id REQUEST_ID --scope-agent-did DID --home /path/to/home
  gents trace project --projection multi-agent --request-id REQUEST_ID --format eval-jsonl --home /path/to/home
  gents trace project-schema --projection multi-agent --format eval-jsonl";
const CONFIG_AFTER_HELP: &str = "\
Examples:
  gents config validate --root infra/agents/default
  gents config validate --root infra/agents/default --home /path/to/home --bind-agent-did home
  gents config diff --root infra/agents/default --home /path/to/home
  gents config apply --root infra/agents/default --home /path/to/home --bind-agent-did home
  gents config backend set --graphql URL --backend-id <backend-id> --name <name> --backend-preset openrouter --max-concurrent 2
  gents config backend discover-models --backend-preset openrouter
  gents config behavior set --graphql URL --agent-did <AGENT_DID> --backend-id <backend-id> --model-name MODEL
  gents config tools set --graphql URL --agent-did <AGENT_DID> --selection-id <selection-id> --enable-file-tools
  gents config tools set --graphql URL --agent-did <AGENT_DID> --selection-id <selection-id> --enable-memory true
  gents config tools set --graphql URL --agent-did <AGENT_DID> --selection-id <selection-id> --subagent-spawn-enabled true --subagent-target '<json>'";
const REQUEST_AFTER_HELP: &str = "\
`request` is the low-level document path. Most users should prefer `gents chat`.

Examples:
  gents request submit --content \"summarize this repo\"
  gents request show REQUEST_ID";
const RESPONSE_AFTER_HELP: &str = "\
Examples:
  gents response wait REQUEST_ID
  gents response show REQUEST_ID";
const SESSION_AFTER_HELP: &str = "\
Fork a conversation into a new session seeded from a user-turn prefix \
of the source. Child inherits principal; behavior can be swapped with \
--behavior.";
const SUBAGENT_AFTER_HELP: &str = "\
Examples:
  gents subagent list
  gents subagent list --root REQUEST_ID
  gents subagent list --root REQUEST_ID --depth 2
  gents subagent list --root REQUEST_ID --output json
  gents subagent cancel REQUEST_ID
  gents subagent cancel REQUEST_ID --cascade=false
  gents subagent cancel REQUEST_ID --wait --timeout 30s --output json";
const SUBAGENT_LIST_AFTER_HELP: &str =
    "Without --root, only requests that participate in subagent lineage are shown.";
const DIAGNOSE_AFTER_HELP: &str = "\
Examples:
  gents diagnose
  gents diagnose --home /path/to/home
  gents diagnose --graphql http://127.0.0.1:9191/api/v0/graphql";
const TOOLS_AFTER_HELP: &str = "\
Examples:
  gents tools explain
  gents tools explain --behavior-id BEHAVIOR_ID
  gents tools explain --graphql http://127.0.0.1:9191/api/v0/graphql

The explain output separates model-callable tools from operator HTTP/MCP surfaces
and includes warnings for confusing defaults such as empty allowlists that mean
all, or built-in read tools that are always included today.";
const CONFIG_EXPORT_AFTER_HELP: &str = "\
Exports the desired configuration documents for one agent principal as a
manifest root directory (per-document subdirectories, optional prompt sidecars).
The output is designed to be committed to version control and applied with
`config apply --root <dir>`. This is distinct from the legacy JSON bundle
format that `config import` consumes.

Examples:
  gents config export --root ./my-agent
  gents config export --root ./my-agent --force
  gents config export --root ./my-agent --agent-did <AGENT_DID>
  gents config export --root ./my-agent --home /path/to/home --bind-agent-did home";
const CONFIG_IMPORT_AFTER_HELP: &str = "\
Imports desired configuration documents from a legacy JSON bundle file.

NOTE: `config import` reads the legacy JSON bundle format and is decoupled from
`config export --root`. To apply a manifest root produced by `config export`,
use `config apply --root <dir>` instead.

Default behavior is insert-only and will fail if a document already exists.
Use --override to switch to upsert mode.

Examples:
  gents config import agent-config.json
  cat agent-config.json | gents config import
  gents config import agent-config.json --override";
pub(crate) const CONFIG_EXPORT_FORMAT_V1: &str = "gents-config/v1";
pub(crate) const CONFIG_EXPORT_FORMAT: &str = "gents-config/v2";

pub(crate) const SCHEMA_COLLECTION_CHECKS: &[(&str, &str)] = &[
    ("AgentPrincipal", "agent_did"),
    ("AgentBehavior", "behavior_id"),
    ("AgentRuntime", "agent_did"),
    ("ToolSelection", "selection_id"),
    ("InferenceProfile", "profile_id"),
    ("InferenceBackend", "backend_id"),
    ("AgentConversation", "session_id"),
    ("AgentRequest", "request_id"),
    ("AgentResponse", "request_id"),
    ("AgentToolResult", "agent_did"),
    ("AgentSession", "session_id"),
    ("AgentMessage", "message_key"),
    ("AgentToolCall", "tool_call_key"),
    ("CompactionEntry", "compaction_key"),
    ("ProjectionAcpBinding", "binding_id"),
    ("Task", "task_id"),
    ("Schedule", "schedule_id"),
    ("ToolServiceRegistry", "service_id"),
];
const CONFIG_SCHEMA_COLLECTIONS: &[&str] = &[
    "AgentPrincipal",
    "AgentBehavior",
    "ToolSelection",
    "InferenceBackend",
    "InferenceProfile",
];
pub(crate) const EXPORT_AGENT_PRINCIPAL_FIELDS: &str =
    "agent_did display_name default_behavior_id enabled created_at created_by";
pub(crate) const EXPORT_AGENT_BEHAVIOR_FIELDS: &str = "behavior_id agent_did display_name description summary system_prompt request_context_template backend_id model_name tool_selection_id inference_profile_id compaction_strategy compaction_threshold enabled skill_refs skill_excludes created_at";
pub(crate) const EXPORT_TOOL_SELECTION_FIELDS: &str = "selection_id agent_did display_name tool_policy_version enable_file_tools file_tools_mode file_tool_root enable_bash bash_mode command_execution_policy command_allowed_argv_prefixes command_forbidden_argv_prefixes read_only_command_allowlist command_network_mode cli_tool_names enable_meta_tools allowed_mcp_service_ids delegate_to backgroundable_tool_names approval_required_tools enable_memory enable_session_history_tool enable_context_budget enable_defra_query defra_query_collections subagent_targets subagent_spawn_enabled orchestration_enabled subagent_steering_enabled subagent_background_enabled subagent_default_await_mode subagent_allow_cross_deployment cross_deployment_spawn_timeout_seconds write_tools enable_self_config self_config_categories self_config_no_lockout self_config_dry_run";
pub(crate) const EXPORT_SKILL_FIELDS: &str =
    "skill_id agent_did scope name description instructions tool_refs display_name interface_json enabled created_at";
pub(crate) const EXPORT_WORKSPACE_ROOT_FIELDS: &str = "root_path display_name enabled updated_at";
pub(crate) const EXPORT_INFERENCE_BACKEND_FIELDS: &str =
    "backend_id name provider_kind openai_wire_api endpoint api_key api_key_env_var max_concurrent max_queue_depth enabled models last_probe probe_status";
pub(crate) const EXPORT_INFERENCE_PROFILE_FIELDS: &str =
    "profile_id display_name context_window max_output_tokens max_turns temperature top_p top_k seed min_p frequency_penalty presence_penalty repetition_penalty reasoning_effort stream_batch_ms stream_liveness_timeout_secs deadline_duration_secs retry_max_transport retry_backoff_ms retry_max_resample retry_allow_repair retry_interactive_max";
pub(crate) const EXPORT_TOOL_SERVICE_REGISTRY_FIELDS: &str =
    "service_id display_name description hostname tailscale_ip lan_ip mcp_port mcp_path send_agent_did";
pub(crate) const EXPORT_PROJECTION_ACP_BINDING_FIELDS: &str =
    "binding_id agent_did behavior_id projection_id policy_id staged_policy_id previous_policy_id resource_map_json publication_status published_at enabled created_at updated_at";
pub(crate) const EXPORT_TASK_FIELDS: &str =
    "task_id name description behavior_id prompt_template enabled output_schema_ref created_at updated_at";
pub(crate) const EXPORT_SCHEDULE_FIELDS: &str =
    "schedule_id task_id interval_secs cron timezone missed_run_policy enabled concurrency created_at updated_at";
pub(crate) const EXPORT_EVENT_TRIGGER_FIELDS: &str =
    "trigger_id task_id source_collection event_kind filter enabled concurrency created_at updated_at";

pub fn run_cli() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(TOKIO_WORKER_STACK_SIZE)
        .build()
        .context("building tokio runtime")?
        .block_on(async_main())
}

async fn async_main() -> Result<()> {
    let argv = std::env::args().collect::<Vec<_>>();
    if let Some(warning) = cli::deprecations::deprecation_warning(&argv) {
        eprintln!("{warning}");
    }
    let cli = Cli::parse_from(argv);
    let command = match cli.command {
        Command::NativeFsRunner(args) => {
            return commands::native_fs_runner::native_fs_runner(args);
        }
        Command::Codex(args) => {
            return commands::codex::codex(args).await;
        }
        command => command,
    };

    let telemetry = telemetry::init(DEFAULT_LOG_FILTER)?;
    let result = match command {
        Command::Version => {
            print!("{}", http::version::version_text());
            Ok(())
        }
        Command::Init(args) => commands::init::init(args).await,
        Command::Provision(args) => commands::provision::provision(args).await,
        Command::Reset(args) => commands::reset::reset(args).await,
        Command::Server(args) => commands::serve::serve(args).await,
        Command::Chat(args) => commands::chat::chat(args).await,
        Command::Codex(_) => unreachable!("codex dispatches before telemetry init"),
        Command::CodexLogin(args) => commands::codex_login::codex_login(args).await,
        Command::CodexAuthProbe(args) => commands::codex_auth_probe::codex_auth_probe(args).await,
        Command::GrokLogin(args) => commands::grok_login::grok_login(args).await,
        Command::GrokAuthProbe(args) => commands::grok_auth_probe::grok_auth_probe(args).await,
        Command::P2p { command } => commands::p2p::dispatch(command).await,
        Command::Schema { command } => commands::schema::dispatch(command).await,
        Command::Show { command } => commands::show::dispatch(command).await,
        Command::Trace { command } => commands::trace::dispatch(command).await,
        Command::Status(args) => commands::status::status(args).await,
        Command::Query(args) => commands::query::query(args).await,
        Command::Background { command } => commands::background::dispatch(command).await,
        Command::Mcp { command } => commands::mcp::dispatch(command).await,
        Command::Fleet { command } => commands::fleet::dispatch(command).await,
        Command::Task { command } => commands::task::dispatch(command).await,
        Command::Diagnose(args) => commands::diagnose::diagnose(args).await,
        Command::Tools { command } => commands::tools::dispatch(command).await,
        Command::Config { command } => commands::config::dispatch(command).await,
        Command::Request { command } => commands::request::dispatch(command).await,
        Command::Response { command } => commands::response::dispatch(command).await,
        Command::Session { command } => commands::session::dispatch(command).await,
        Command::Goal { command } => commands::goal::dispatch(command).await,
        Command::Subagent { command } => commands::subagent::dispatch(command).await,
        Command::Demo(args) => commands::demo::demo(args).await,
        Command::NativeFsRunner(_) => unreachable!("handled before telemetry initialization"),
    };
    telemetry.shutdown();
    result
}

pub(crate) fn expand_nonempty_values(values: &[String], flag_name: &str) -> Result<Vec<String>> {
    let values = values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();

    if values.is_empty() {
        anyhow::bail!("provide at least one {flag_name}");
    }

    Ok(values.into_iter().collect())
}

pub(crate) async fn http_get_json<T: DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
) -> Result<T> {
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("sending GET request to {url}"))?;
    let status = response.status();
    let body = response
        .bytes()
        .await
        .with_context(|| format!("reading GET response body from {url}"))?;
    if !status.is_success() {
        anyhow::bail!(
            "GET {url} failed with {status}: {}",
            String::from_utf8_lossy(&body)
        );
    }
    serde_json::from_slice(&body).with_context(|| format!("decoding JSON response from {url}"))
}

pub(crate) async fn http_post_json<B: Serialize>(
    client: &reqwest::Client,
    url: &str,
    body: &B,
) -> Result<()> {
    let response = client
        .post(url)
        .json(body)
        .send()
        .await
        .with_context(|| format!("sending POST request to {url}"))?;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("reading POST response body from {url}"))?;
    if !status.is_success() {
        anyhow::bail!(
            "POST {url} failed with {status}: {}",
            String::from_utf8_lossy(&bytes)
        );
    }
    Ok(())
}

pub(crate) async fn http_delete_json<B: Serialize>(
    client: &reqwest::Client,
    url: &str,
    body: &B,
) -> Result<()> {
    let response = client
        .delete(url)
        .json(body)
        .send()
        .await
        .with_context(|| format!("sending DELETE request to {url}"))?;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("reading DELETE response body from {url}"))?;
    if !status.is_success() {
        anyhow::bail!(
            "DELETE {url} failed with {status}: {}",
            String::from_utf8_lossy(&bytes)
        );
    }
    Ok(())
}

pub(crate) async fn resolve_config_access(
    home: Option<&Path>,
    explicit_graphql: Option<&str>,
) -> Result<(ConfigAccess, PathBuf)> {
    let home_dir = resolve_home_dir(home);
    if let Some(graphql) = explicit_graphql
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok((ConfigAccess::Graphql(graphql.to_string()), home_dir));
    }
    if let Some(runtime_state) = read_runtime_state(&home_dir)? {
        if graphql_endpoint_available(&runtime_state.graphql).await {
            return Ok((ConfigAccess::Graphql(runtime_state.graphql), home_dir));
        }
    }

    let data_dir = default_data_dir(&home_dir);
    fs::create_dir_all(&data_dir)
        .with_context(|| format!("creating data directory {}", data_dir.display()))?;
    let node = {
        use std::sync::Arc;
        let node_arc = Arc::new(
            persistent_node_builder(&data_dir)
                .build()
                .await
                .with_context(|| {
                    format!("building embedded DefraDB node from {}", data_dir.display())
                })?,
        );
        gents::migration::ensure_all_runtime_migrations(node_arc.clone()).await?;
        Arc::try_unwrap(node_arc).unwrap_or_else(|_| {
            unreachable!("node_arc had exactly one strong reference at this point")
        })
    };
    Ok((ConfigAccess::Local(std::sync::Arc::new(node)), home_dir))
}

pub(crate) fn persistent_node_builder(data_dir: &Path) -> NodeBuilder {
    EmbeddedNode::builder()
        .data_path(data_dir)
        .with_storage_backend(StorageBackend::RocksDb)
}

pub(crate) fn require_non_empty<'a>(field: &str, value: &'a str) -> Result<&'a str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        anyhow::bail!("--{field} must not be empty");
    }
    Ok(trimmed)
}

pub(crate) fn normalize_optional_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|candidate| !candidate.is_empty())
        .map(ToOwned::to_owned)
}

pub(crate) fn dangerously_overwrite_home(home_dir: &Path) -> Result<()> {
    if !home_dir.exists() {
        return Ok(());
    }

    if home_dir.as_os_str().is_empty() || home_dir == Path::new("/") {
        anyhow::bail!("refusing to dangerously overwrite {}", home_dir.display());
    }
    if let Some(user_home) = std::env::var_os("HOME").map(PathBuf::from) {
        if home_dir == user_home {
            anyhow::bail!(
                "refusing to dangerously overwrite the user home directory {}; pass a dedicated gents home instead",
                home_dir.display()
            );
        }
    }

    fs::remove_dir_all(home_dir)
        .with_context(|| format!("dangerously overwriting {}", home_dir.display()))?;
    Ok(())
}

pub(crate) fn server_start_failure_hint(home_dir: &Path) -> String {
    format!(
        "Next:\n  1. For the default local backend, run `llama-server -hf {DEFAULT_INIT_MODEL_NAME}` and make sure it is listening on {DEFAULT_INIT_ENDPOINT}\n  2. Point the backend elsewhere with `gents config backend set --graphql http://127.0.0.1:{DEFAULT_HTTP_PORT}/api/v0/graphql --backend-id <ID> --name <NAME> --endpoint <URL> --max-concurrent 2`\n  3. Inspect the initialized home at {}\n  4. If persisted runtime state is stale, run `gents reset --home {}`",
        init_config_path(home_dir).display(),
        home_dir.display()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn default_codex_remote_matches_shim_port() {
        // serve.rs decides whether to print a --remote hint by comparing the
        // formatted shim URL against DEFAULT_CODEX_REMOTE; the two constants
        // must agree byte-for-byte.
        assert_eq!(
            DEFAULT_CODEX_REMOTE,
            format!("ws://127.0.0.1:{DEFAULT_CODEX_SHIM_PORT}/")
        );
    }

    #[test]
    fn sanitize_inference_backend_drops_deprecated_capability_fields() {
        let input = serde_json::json!({
            "backend_id": "local",
            "name": "Local",
            "provider_kind": "OpenAiCompatible",
            "endpoint": "http://127.0.0.1:11434/v1",
            "api_key": null,
            "api_key_env_var": null,
            "max_concurrent": 1,
            "max_queue_depth": 100,
            "enabled": true,
            "supports_tool_calls": true,
            "supports_streaming": true,
            "supports_structured_outputs": false,
            "supports_json_schema": false,
            "context_window": 32768,
            "max_output_tokens": 4096,
            "last_probe": "2026-04-15T00:00:00Z",
            "models": ["test-model"],
            "probe_status": "healthy"
        });

        let out = sanitize_import_document("InferenceBackend", &input, false).unwrap();
        let obj = out.as_object().unwrap();
        for field in [
            "supports_tool_calls",
            "supports_streaming",
            "supports_structured_outputs",
            "supports_json_schema",
            "context_window",
            "max_output_tokens",
            "last_probe",
        ] {
            assert!(!obj.contains_key(field), "{field} should be stripped");
        }
        assert_eq!(obj.get("backend_id").and_then(Value::as_str), Some("local"));
    }

    #[test]
    fn read_config_import_bundle_migrates_v1_backend_capability_fields() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("config.json");
        fs::write(
            &path,
            serde_json::to_string(&serde_json::json!({
                "format": CONFIG_EXPORT_FORMAT_V1,
                "agent_did": "did:test:test",
                "exported_at": "2026-04-15T00:00:00Z",
                "access_mode": "local",
                "agent_principal": null,
                "agent_behaviors": [],
                "tool_selections": [],
                "inference_backends": [{
                    "backend_id": "local",
                    "name": "Local",
                    "provider_kind": "OpenAiCompatible",
                    "endpoint": "http://127.0.0.1:11434/v1",
                    "api_key": null,
                    "api_key_env_var": null,
                    "max_concurrent": 1,
                    "max_queue_depth": 100,
                    "enabled": true,
                    "supports_tool_calls": true,
                    "supports_streaming": true,
                    "supports_structured_outputs": false,
                    "supports_json_schema": false,
                    "models": ["test-model"],
                    "probe_status": "healthy"
                }],
                "inference_profiles": [],
                "tool_service_registries": []
            }))
            .unwrap(),
        )
        .unwrap();

        let bundle = read_config_import_bundle(Some(&path)).unwrap();
        validate_config_import_bundle(&bundle).unwrap();
        assert_eq!(bundle.format, CONFIG_EXPORT_FORMAT);
        let backend = bundle.inference_backends[0].as_object().unwrap();
        assert!(!backend.contains_key("supports_tool_calls"));
        assert!(!backend.contains_key("supports_streaming"));
        assert!(!backend.contains_key("supports_structured_outputs"));
        assert!(!backend.contains_key("supports_json_schema"));
    }

    #[test]
    fn sanitize_tool_service_registry_defaults_status_online_when_absent() {
        let input = serde_json::json!({
            "service_id": "observability-mcp",
            "hostname": "studio-1",
            "tailscale_ip": "100.69.4.79",
            "mcp_port": 9201
        });
        let out = sanitize_import_document("ToolServiceRegistry", &input, false).unwrap();
        let obj = out.as_object().unwrap();
        assert_eq!(obj.get("status").and_then(|v| v.as_str()), Some("online"));
    }

    #[test]
    fn sanitize_tool_service_registry_fills_status_when_null() {
        let input = serde_json::json!({
            "service_id": "observability-mcp",
            "status": null,
            "hostname": "studio-1",
            "mcp_port": 9201
        });
        let out = sanitize_import_document("ToolServiceRegistry", &input, false).unwrap();
        let obj = out.as_object().unwrap();
        assert_eq!(obj.get("status").and_then(|v| v.as_str()), Some("online"));
    }

    #[test]
    fn sanitize_tool_service_registry_preserves_explicit_status() {
        let input = serde_json::json!({
            "service_id": "observability-mcp",
            "status": "offline",
            "mcp_port": 9201
        });
        let out = sanitize_import_document("ToolServiceRegistry", &input, false).unwrap();
        let obj = out.as_object().unwrap();
        assert_eq!(obj.get("status").and_then(|v| v.as_str()), Some("offline"));
    }

    #[test]
    fn sanitize_tool_service_registry_normalizes_address_fields_for_storage() {
        let input = serde_json::json!({
            "service_id": "observability-mcp",
            "hostname": null,
            "tailscale_ip": " 100.69.4.79 ",
            "lan_ip": null,
            "mcp_port": 9201,
            "mcp_path": "mcp"
        });
        let out = sanitize_import_document("ToolServiceRegistry", &input, false).unwrap();
        let obj = out.as_object().unwrap();
        assert_eq!(obj.get("hostname").and_then(|v| v.as_str()), Some(""));
        assert_eq!(obj.get("lan_ip").and_then(|v| v.as_str()), Some(""));
        assert_eq!(
            obj.get("tailscale_ip").and_then(|v| v.as_str()),
            Some("100.69.4.79")
        );
        assert_eq!(obj.get("mcp_path").and_then(|v| v.as_str()), Some("/mcp"));
    }

    #[test]
    fn sanitize_tool_service_registry_defaults_mcp_path() {
        let input = serde_json::json!({
            "service_id": "observability-mcp",
            "hostname": "studio-1",
            "mcp_port": 9201
        });
        let out = sanitize_import_document("ToolServiceRegistry", &input, false).unwrap();
        let obj = out.as_object().unwrap();
        assert_eq!(obj.get("mcp_path").and_then(|v| v.as_str()), Some("/mcp"));
    }

    #[test]
    fn sanitize_tool_service_registry_still_strips_runtime_owned_fields() {
        let input = serde_json::json!({
            "service_id": "observability-mcp",
            "mcp_port": 9201,
            "tools": [{"name": "x", "description": "y"}],
            "version": "1.2.3",
            "updated_at": "2026-04-14T00:00:00Z"
        });
        let out = sanitize_import_document("ToolServiceRegistry", &input, false).unwrap();
        let obj = out.as_object().unwrap();
        assert!(obj.get("tools").is_none(), "tools should be stripped");
        assert!(obj.get("version").is_none(), "version should be stripped");
        assert!(
            obj.get("updated_at").is_none(),
            "updated_at should be stripped on create"
        );
    }
}
