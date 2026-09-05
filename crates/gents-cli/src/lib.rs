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
const DEFAULT_INTERACTIVE_WAIT_TIMEOUT_SECS: u64 = 86_400;
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

  # Authenticated enrollment operator decisions and observation:
  gents p2p enrollment approve <request-id>
  gents p2p enrollment deny <request-id>
  gents p2p pairings list

  # Service discovery:
  gents p2p network register
  gents p2p network list
  gents p2p templates list

  # Low-level non-authoritative live wiring for diagnostics/repair:
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
  gents mcp register web --endpoint http://127.0.0.1:9213/mcp
  gents mcp register web --endpoint http://127.0.0.1:9213/mcp --send-agent-did
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
For a Task with a durable-goal declaration, --session-id is a stable invocation key;
the emitted session_id is the deterministic Task/fire session derived from it.

Examples:
  gents task list
  gents task show host-check
  gents task run host-check
  gents task run host-check --args '{\"scope\":\"host\"}' --wait
  gents task run durable-task --session-id stable-invocation-key --wait
  gents task run --task-id host-check --graphql http://127.0.0.1:9191/api/v0/graphql";
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
`config apply --root <dir>`.

Examples:
  gents config export --root ./my-agent
  gents config export --root ./my-agent --force
  gents config export --root ./my-agent --agent-did <AGENT_DID>
  gents config export --root ./my-agent --home /path/to/home --bind-agent-did home";
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
pub(crate) const EXPORT_TOOL_SELECTION_FIELDS: &str = "selection_id agent_did display_name tool_policy_version enable_file_tools file_tools_mode file_tool_root enable_bash bash_mode command_execution_policy command_allowed_argv_prefixes command_forbidden_argv_prefixes read_only_command_allowlist command_network_mode cli_tool_names enable_meta_tools enable_goal_tools enable_goal_creation allowed_mcp_service_ids required_mcp_service_ids backgroundable_tool_names approval_required_tools enable_memory enable_session_history_tool enable_context_budget enable_defra_query defra_query_collections subagent_targets subagent_spawn_enabled subagent_steering_enabled subagent_background_enabled subagent_default_await_mode subagent_allow_cross_deployment cross_deployment_spawn_timeout_seconds write_tools datastore_tool_surface_ids eth_tool_ids enable_self_config self_config_categories self_config_no_lockout self_config_dry_run enable_lsp lsp_config";
pub(crate) const EXPORT_DATASTORE_TOOL_SURFACE_FIELDS: &str =
    "surface_id agent_did display_name enabled entries";
pub(crate) const EXPORT_CHAIN_KEY_BINDING_FIELDS: &str =
    "binding_id principal_did address key_backend attestation created_at revoked_at";
pub(crate) const EXPORT_ETH_TOOL_FIELDS: &str =
    "tool_id agent_did display_name enabled chain_id rpc_url query_methods calls key_binding_id";
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
    "task_id name description behavior_id prompt_template goal_objective_template goal_token_budget enabled output_schema_ref created_at updated_at";
pub(crate) const EXPORT_SCHEDULE_FIELDS: &str =
    "schedule_id task_id interval_secs cron timezone missed_run_policy enabled concurrency created_at updated_at";
pub(crate) const EXPORT_EVENT_TRIGGER_FIELDS: &str =
    "trigger_id task_id source_collection event_kind filter correlation_field fire_mode expected_count expected_count_field group_timeout_secs group_min_count workspace_authority enabled concurrency created_at updated_at";

pub fn run_cli() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(TOKIO_WORKER_STACK_SIZE)
        .build()
        .context("building tokio runtime")?
        .block_on(async_main())
}

async fn async_main() -> Result<()> {
    let cli = Cli::parse();
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
        Command::ClaudeLogin(args) => commands::claude_login::claude_login(args).await,
        Command::P2p { command } => commands::p2p::dispatch(command).await,
        Command::Schema { command } => commands::schema::dispatch(command).await,
        Command::Trace { command } => commands::trace::dispatch(command).await,
        Command::Status(args) => commands::status::status(args).await,
        Command::Query(args) => commands::query::query(args).await,
        Command::Background { command } => commands::background::dispatch(command).await,
        Command::Mcp { command } => commands::mcp::dispatch(command).await,
        Command::Fleet { command } => commands::fleet::dispatch(command).await,
        Command::Task { command } => commands::task::dispatch(command).await,
        Command::Graph { command } => commands::graph::dispatch(command).await,
        Command::Diagnose(args) => commands::diagnose::diagnose(args).await,
        Command::Tools { command } => commands::tools::dispatch(command).await,
        Command::Config { command } => commands::config::dispatch(command).await,
        Command::Request { command } => commands::request::dispatch(command).await,
        Command::Response { command } => commands::response::dispatch(command).await,
        Command::Session { command } => commands::session::dispatch(command).await,
        Command::Goal { command } => commands::goal::dispatch(command).await,
        Command::Chain { command } => commands::chain::dispatch(command).await,
        Command::Mailbox { command } => commands::mailbox::dispatch(command).await,
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
            persistent_node_builder_with_stored_identity(&home_dir, &data_dir)?
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

pub(crate) fn persistent_node_builder(data_dir: &Path) -> Result<NodeBuilder> {
    gents::storage_backend::reject_legacy_store(data_dir)?;
    Ok(EmbeddedNode::builder()
        .data_path(data_dir)
        .with_storage_backend(StorageBackend::Regolith))
}

pub(crate) fn persistent_node_builder_with_stored_identity(
    home_dir: &Path,
    data_dir: &Path,
) -> Result<NodeBuilder> {
    let mut builder = persistent_node_builder(data_dir)?;
    let config = read_init_config(home_dir)?.ok_or_else(|| {
        anyhow::anyhow!(
            "gents home {} is not initialized; run `gents init --home {}` first",
            home_dir.display(),
            home_dir.display()
        )
    })?;
    let identity = load_initialized_home_identity(home_dir, &config)?;
    builder = builder.with_node_identity_did(identity.did().to_string());
    Ok(builder)
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
    use gents::AgentIdentity as _;
    use serde_json::Value;

    #[tokio::test]
    async fn initialized_offline_node_reuses_the_home_signer() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let data = default_data_dir(&home);
        let key_path = default_key_path(&home, "default");
        std::fs::create_dir_all(key_path.parent().unwrap()).unwrap();
        let identity = gents::KeyIdentity::load_or_create(&key_path, None).unwrap();
        let did = identity.did().to_string();
        write_init_config(
            &home,
            &StoredInitConfig {
                home: home.to_string_lossy().to_string(),
                agent_name: "default".to_string(),
                agent_did: did.clone(),
                key_path: Some(key_path.to_string_lossy().to_string()),
                identity_backend: None,
                keychain_label: None,
                secure_enclave_label: None,
                tool_package: None,
                tool_ceiling: ToolCeilingArg::Readonly,
                tool_root: None,
            },
        )
        .unwrap();

        let node = persistent_node_builder_with_stored_identity(&home, &data)
            .unwrap()
            .build()
            .await
            .unwrap();
        assert_eq!(node.node_identity_did(), Some(did.as_str()));
    }

    #[test]
    fn uninitialized_offline_home_requires_init() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let data = default_data_dir(&home);

        let error = match persistent_node_builder_with_stored_identity(&home, &data) {
            Ok(_) => panic!("uninitialized home should not produce an unsigned node builder"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("run `gents init --home"));
    }

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
    fn sanitize_inference_backend_does_not_translate_unknown_capability_fields() {
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
        ] {
            assert!(
                obj.contains_key(field),
                "{field} must not be silently translated"
            );
        }
        assert!(
            !obj.contains_key("last_probe"),
            "runtime health is never imported"
        );
        assert_eq!(obj.get("backend_id").and_then(Value::as_str), Some("local"));
    }

    #[test]
    fn sanitize_tool_service_registry_does_not_invent_status_when_absent() {
        let input = serde_json::json!({
            "service_id": "observability-mcp",
            "hostname": "studio-1",
            "tailscale_ip": "100.69.4.79",
            "mcp_port": 9201,
            "mcp_path": "/mcp"
        });
        let out = sanitize_import_document("ToolServiceRegistry", &input, false).unwrap();
        let obj = out.as_object().unwrap();
        assert!(obj.get("status").is_none());
    }

    #[test]
    fn sanitize_tool_service_registry_drops_runtime_status_when_null() {
        let input = serde_json::json!({
            "service_id": "observability-mcp",
            "status": null,
            "hostname": "studio-1",
            "mcp_port": 9201,
            "mcp_path": "/mcp"
        });
        let out = sanitize_import_document("ToolServiceRegistry", &input, false).unwrap();
        let obj = out.as_object().unwrap();
        assert!(obj.get("status").is_none());
    }

    #[test]
    fn sanitize_tool_service_registry_drops_runtime_status_when_explicit() {
        let input = serde_json::json!({
            "service_id": "observability-mcp",
            "status": "offline",
            "mcp_port": 9201,
            "mcp_path": "/mcp"
        });
        let out = sanitize_import_document("ToolServiceRegistry", &input, false).unwrap();
        let obj = out.as_object().unwrap();
        assert!(obj.get("status").is_none());
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
    fn sanitize_tool_service_registry_rejects_missing_mcp_path() {
        let input = serde_json::json!({
            "service_id": "observability-mcp",
            "hostname": "studio-1",
            "mcp_port": 9201
        });
        let error = sanitize_import_document("ToolServiceRegistry", &input, false)
            .expect_err("missing MCP path must fail closed");
        assert!(error.to_string().contains("mcp_path is required"));
    }

    #[test]
    fn sanitize_tool_service_registry_still_strips_runtime_owned_fields() {
        let input = serde_json::json!({
            "service_id": "observability-mcp",
            "mcp_port": 9201,
            "mcp_path": "/mcp",
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
