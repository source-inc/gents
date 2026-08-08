use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::http::Uri;
use gents::defra_node::EmbeddedNode;
use gents::{
    load_macos_keychain_identity, load_macos_secure_enclave_identity, AgentIdentity,
    DocumentRuntimeOptions, Gents, KeyIdentity, McpPool, ProcessLifecycleObserver,
    ProcessLifecycleState, ToolCeiling,
};
use serde_json::{json, Value};
use tokio::sync::watch;

use crate::cli::*;
use crate::commands::codex_shim::{bind_codex_shim, CodexShimBindArgs};
use crate::http::runtime_contract_router;
use crate::shared::{P2pAdmissionState, *};
use crate::{
    default_data_dir, default_key_path, display_host, format_tool_ceiling, parse_cli_tool_arg,
    print_json, read_init_config, resolve_home_dir, server_start_failure_hint, write_runtime_state,
    DEFAULT_AGENT_NAME,
};
use gents::codex_shim_binding::{ShimBinding, ShimUnboundReason};

pub(crate) struct CliReadyObserver {
    pub(crate) tx: watch::Sender<ProcessLifecycleState>,
}

impl ProcessLifecycleObserver for CliReadyObserver {
    fn on_process_state_change(&self, state: ProcessLifecycleState) {
        let _ = self.tx.send(state);
    }
}

struct CliRunnableBehaviorObserver {
    tx: watch::Sender<Vec<String>>,
}

impl gents::RuntimeSnapshotObserver for CliRunnableBehaviorObserver {
    fn on_generation_published(&self, _generation: u64, runnable_behavior_ids: &[String]) {
        let _ = self.tx.send(runnable_behavior_ids.to_vec());
    }
}

fn announce_codex_shim(
    bound: &crate::commands::codex_shim::BoundCodexShim,
    args: &ServeArgs,
) -> Value {
    let bound_url = format!(
        "ws://{}:{}/",
        display_shim_host(bound.addr().ip()),
        bound.addr().port()
    );
    let codex_shim_url = args.codex_shim_public_url.as_deref().unwrap_or(&bound_url);
    let launch_command =
        codex_shim_launch_command(codex_shim_url, args.codex_shim_auth_token_env.as_deref());
    eprintln!(
        "Codex shim is listening on {bound_url} with state dir {}",
        bound.codex_home().display(),
    );
    if codex_shim_url != bound_url {
        eprintln!("Codex shim public endpoint: {codex_shim_url}");
    }
    eprintln!("Codex shim event log: {}", bound.trace_path().display());
    eprintln!("Chat from another terminal with: {launch_command}");
    json!({
        "websocket": codex_shim_url,
        "launch_command": launch_command,
        "auth_required": bound.auth_required(),
        "bound_agent_did": bound.agent_did(),
        "bound_behavior_id": bound.behavior_id(),
        "shim_home": bound.codex_home().to_path_buf(),
        "codex_home": bound.codex_home().to_path_buf(),
        "event_log": bound.trace_path().to_path_buf(),
    })
}

fn codex_shim_launch_command(websocket: &str, auth_token_env: Option<&str>) -> String {
    let mut command = if websocket == crate::DEFAULT_CODEX_REMOTE {
        "gents codex".to_string()
    } else {
        format!("gents codex --remote {websocket}")
    };
    if let Some(env_name) = auth_token_env {
        command.push_str(" --remote-auth-token-env ");
        command.push_str(env_name);
    }
    command
}

fn set_codex_shim_health(handle: &CodexShimHealthHandle, health: CodexShimHealth) {
    if let Ok(mut guard) = handle.write() {
        *guard = health;
    }
}

/// Apply a self-contained pack (optional `schemas/` then desired-state) to the
/// server's live node after readiness. Rebinds placeholder DIDs to the home
/// principal so checked-in experiment packs work without hand-editing.
async fn apply_pack_after_ready(
    node: Arc<EmbeddedNode>,
    home_dir: &Path,
    root: &Path,
    prune: bool,
) -> Result<Value> {
    use crate::cli::ManifestAgentDidBindingArg;
    use crate::commands::config::apply::apply_bound_desired_manifest;
    use crate::commands::config::binding::{load_bound_manifest, ManifestBindingOptions};
    use crate::commands::schema::apply_pack_schemas_if_present;
    use crate::config_writes::ConfigAccess;

    if !root.is_dir() {
        anyhow::bail!("--apply-root is not a directory: {}", root.display());
    }

    eprintln!(
        "Applying pack {} to in-process node (schemas/ if present, then config)…",
        root.display()
    );

    let access = ConfigAccess::Local(node);
    let schemas = apply_pack_schemas_if_present(&access, root)
        .await
        .with_context(|| format!("pack schemas under {}", root.display()))?;
    if let Some(phase) = schemas.as_ref() {
        eprintln!(
            "  schemas: {} ({} SDL file(s))",
            phase.status,
            phase.schema_files.len()
        );
    }

    let bound = load_bound_manifest(ManifestBindingOptions {
        root,
        home: Some(home_dir),
        graphql: None,
        bind_agent_did: Some(ManifestAgentDidBindingArg::Home),
        force_rebind_concrete_did: true,
        access: Some(&access),
    })
    .await?
    .require_valid()?;

    let mut report = apply_bound_desired_manifest(root, &access, &bound, prune).await?;
    report.schemas = schemas;
    if report
        .schemas
        .as_ref()
        .is_some_and(crate::commands::schema::PackSchemaPhase::changed)
        && report.status == "noop"
    {
        report.status = "applied";
        report.changed = true;
    }

    eprintln!(
        "  config apply: status={} ok={} agent_did={}",
        report.status, report.ok, report.agent_did
    );
    if !report.ok {
        anyhow::bail!(
            "pack apply did not converge for {} (status={})",
            root.display(),
            report.status
        );
    }

    serde_json::to_value(&report).context("serializing pack apply report")
}

fn spawn_codex_shim_supervisor(
    bind_args: CodexShimBindArgs,
    bound_behavior_id: String,
    mut runnable_rx: watch::Receiver<Vec<String>>,
    public_url: Option<String>,
    auth_token_env: Option<String>,
    health: CodexShimHealthHandle,
) {
    tokio::spawn(async move {
        let mut binding = ShimBinding::unbound(
            bound_behavior_id.clone(),
            ShimUnboundReason::DependencyMissing,
        );

        loop {
            let runnable = runnable_rx.borrow_and_update().clone();

            if binding.grants_listen(runnable.iter().map(String::as_str)) {
                match bind_codex_shim(bind_args.clone()).await {
                    Ok(bound) => {
                        binding.settle_listen(true);
                        let bound_url = format!(
                            "ws://{}:{}/",
                            display_shim_host(bound.addr().ip()),
                            bound.addr().port()
                        );
                        let url = public_url.as_deref().unwrap_or(&bound_url).to_string();
                        set_codex_shim_health(
                            &health,
                            CodexShimHealth::Listening {
                                websocket: url.clone(),
                                auth_required: bound.auth_required(),
                                bound_agent_did: bound.agent_did().to_string(),
                                bound_behavior_id: bound.behavior_id().to_string(),
                            },
                        );
                        eprintln!(
                            "Codex endpoint bound: behavior {bound_behavior_id:?} became runnable; \
                             the shim is now running on {url} (no restart was needed)."
                        );
                        eprintln!(
                            "Chat from another terminal with: {}",
                            codex_shim_launch_command(&url, auth_token_env.as_deref())
                        );
                        bound.spawn();
                        return;
                    }
                    Err(error) if error.is_dependency_missing() => {
                        tracing::debug!(
                            behavior_id = %bound_behavior_id,
                            error = %error.error(),
                            "Codex shim still waiting on its bound behavior's documents"
                        );
                    }
                    Err(error) => {
                        // Not a document. No generation retracts it, so stop.
                        binding.settle_listen(false);
                        set_codex_shim_health(
                            &health,
                            CodexShimHealth::Disabled {
                                reason: format!("{:#}", error.error()),
                            },
                        );
                        eprintln!(
                            "Codex endpoint disabled: behavior {bound_behavior_id:?} became runnable, \
                             but the shim could not bind: {:#}",
                            error.error()
                        );
                        eprintln!(
                            "This is not something configuration can fix. Restart with --codex-shim-port <free-port>, or silence this with --no-codex-shim."
                        );
                        return;
                    }
                }
            }

            if runnable_rx.changed().await.is_err() {
                return;
            }
        }
    });
}

fn read_codex_shim_auth_token(env_name: Option<&str>) -> Result<Option<String>> {
    env_name
        .map(|name| read_codex_shim_auth_token_with(name, |name| std::env::var(name)))
        .transpose()
}

fn read_codex_shim_auth_token_with<F>(env_name: &str, read: F) -> Result<String>
where
    F: FnOnce(&str) -> std::result::Result<String, std::env::VarError>,
{
    let token =
        read(env_name).with_context(|| format!("reading app-server token from {env_name}"))?;
    let token = token.trim();
    if token.is_empty() {
        anyhow::bail!("app-server token in {env_name} is empty");
    }
    Ok(token.to_string())
}

fn normalize_codex_shim_public_url(raw: &str) -> Result<String> {
    let uri = raw
        .parse::<Uri>()
        .with_context(|| format!("invalid --codex-shim-public-url {raw:?}"))?;
    if uri.scheme_str() != Some("wss") {
        anyhow::bail!("--codex-shim-public-url must use wss://");
    }
    let authority = uri
        .authority()
        .ok_or_else(|| anyhow::anyhow!("--codex-shim-public-url must include a host"))?;
    if authority.as_str().contains('@') {
        anyhow::bail!("--codex-shim-public-url must not contain credentials");
    }
    if authority.port_u16().is_none() {
        anyhow::bail!("--codex-shim-public-url must include an explicit port");
    }
    if uri.path() != "/" || uri.query().is_some() {
        anyhow::bail!("--codex-shim-public-url must point to the root path without a query");
    }
    Ok(format!("wss://{authority}/"))
}

pub(crate) async fn serve(args: ServeArgs) -> Result<()> {
    serve_with_control(args, None, None).await
}

pub(crate) async fn serve_with_control(
    mut args: ServeArgs,
    external_shutdown: Option<watch::Receiver<bool>>,
    ready: Option<tokio::sync::oneshot::Sender<Value>>,
) -> Result<()> {
    args.codex_shim_public_url = args
        .codex_shim_public_url
        .as_deref()
        .map(normalize_codex_shim_public_url)
        .transpose()?;
    let codex_shim_auth_token = if args.no_codex_shim {
        None
    } else {
        read_codex_shim_auth_token(args.codex_shim_auth_token_env.as_deref())?
    };
    if args.codex_shim_public_url.is_some() && codex_shim_auth_token.is_none() {
        anyhow::bail!("--codex-shim-public-url requires --codex-shim-auth-token-env");
    }
    let home_dir = resolve_home_dir(args.home.as_deref());
    let data_dir = args
        .data_dir
        .clone()
        .unwrap_or_else(|| default_data_dir(&home_dir));
    fs::create_dir_all(&data_dir)
        .with_context(|| format!("creating data directory {}", data_dir.display()))?;
    let http_addr = SocketAddr::new(args.http_addr, args.http_port);
    let graphql_url = format!(
        "http://{}:{}/api/v0/graphql",
        display_host(args.http_addr),
        args.http_port
    );
    let init_config = read_init_config(&home_dir)?;
    if let (Some(explicit), Some(config)) = (args.agent_name.as_deref(), init_config.as_ref()) {
        if explicit != config.agent_name {
            anyhow::bail!(
                "--agent-name {} does not match initialized home agent {}",
                explicit,
                config.agent_name
            );
        }
    }

    let local_hostname = hostname::get()
        .map(|host| host.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let agent_name = args
        .agent_name
        .clone()
        .or_else(|| init_config.as_ref().map(|config| config.agent_name.clone()))
        .unwrap_or_else(|| DEFAULT_AGENT_NAME.to_string());
    let server_identity =
        resolve_server_identity(&args, init_config.as_ref(), &home_dir, &agent_name)?;
    let identity = server_identity.identity;
    let effective_tool_ceiling = args
        .tool_ceiling
        .or_else(|| init_config.as_ref().map(|config| config.tool_ceiling))
        .unwrap_or(ToolCeilingArg::MetaOnly);
    let configured_tool_root = args.tool_root.clone().or_else(|| {
        init_config
            .as_ref()
            .and_then(|config| config.tool_root.as_ref().map(PathBuf::from))
    });
    let effective_tool_root = match effective_tool_ceiling {
        ToolCeilingArg::MetaOnly => configured_tool_root,
        ToolCeilingArg::Readonly => Some(match configured_tool_root {
            Some(root) => root,
            None => resolve_default_tool_root(None)?,
        }),
        ToolCeilingArg::Readwrite => Some(configured_tool_root.ok_or_else(|| {
            anyhow::anyhow!("--tool-root is required when --tool-ceiling readwrite")
        })?),
    };
    let mut tool_ceiling = match effective_tool_ceiling {
        ToolCeilingArg::MetaOnly => ToolCeiling::meta_only(),
        ToolCeilingArg::Readonly => ToolCeiling::readonly_at(
            effective_tool_root
                .as_ref()
                .expect("readonly root resolved"),
        ),
        ToolCeilingArg::Readwrite => ToolCeiling::readwrite(
            effective_tool_root
                .as_ref()
                .expect("readwrite root resolved"),
        ),
    };
    tool_ceiling = tool_ceiling.with_command_timeout_secs(args.command_timeout_secs);
    if let Some(max_secs) = args.command_timeout_max_secs {
        tool_ceiling = tool_ceiling.with_command_timeout_max_secs(max_secs);
    }
    let effective_command_timeout_max_secs = args
        .command_timeout_max_secs
        .unwrap_or(args.command_timeout_secs)
        .max(args.command_timeout_secs)
        .max(1);
    tracing::info!(
        command_timeout_secs = args.command_timeout_secs.max(1),
        command_timeout_max_secs = effective_command_timeout_max_secs,
        "configured foreground command timeout default and ceiling"
    );
    for cli_tool_arg in &args.cli_tools {
        tool_ceiling = tool_ceiling.with_cli_tool(parse_cli_tool_arg(cli_tool_arg)?);
    }

    let p2p_config = resolve_server_p2p_config(&home_dir, &args)?;
    if let Some(config) = p2p_config.as_ref() {
        crate::p2p_relay::log_relay_mode_diagnostics(args.p2p_relay_mode);
        log_p2p_admission_config(config);
    }
    let mcp_query_scope = if !args.enable_mcp {
        None
    } else if args.mcp_query_collections.is_empty() {
        Some(gents::defra_query::CollectionScope::all())
    } else {
        Some(gents::defra_query::CollectionScope::restricted(
            args.mcp_query_collections.clone(),
        ))
    };
    let p2p_admission_state = p2p_config.as_ref().map(p2p_admission_state);
    let p2p_admission = p2p_admission_state.as_ref().map(P2pAdmissionState::to_json);
    let backend_health = gents::BackendHealthMap::new();
    let codex_shim_health: CodexShimHealthHandle =
        Arc::new(std::sync::RwLock::new(if args.no_codex_shim {
            CodexShimHealth::Off
        } else {
            CodexShimHealth::Pending {
                bound_behavior_id: args
                    .codex_shim_behavior_id
                    .clone()
                    .unwrap_or_else(|| "<default>".to_string()),
                reason: "the Codex shim has not bound yet".to_string(),
            }
        }));
    let mut node_builder = crate::persistent_node_builder(&data_dir).with_http(
        defra_node::HttpConfig::with_addr(http_addr).with_extra_routes(runtime_contract_router(
            graphql_url.clone(),
            agent_name.clone(),
            identity.did().to_string(),
            mcp_query_scope,
            Some(backend_health.clone()),
            p2p_admission_state.clone(),
            Some(codex_shim_health.clone()),
        )),
    );
    if let Some(node_identity_did) = server_identity.node_identity_did.as_ref() {
        node_builder = node_builder.with_node_identity_did(node_identity_did.clone());
    }
    if let Some(config) = p2p_config {
        node_builder = node_builder.with_p2p(config);
    }
    let node = Arc::new(
        node_builder
            .build()
            .await
            .context("building embedded DefraDB node")?,
    );
    gents::migration::ensure_all_runtime_migrations(node.clone()).await?;
    let (ready_tx, mut ready_rx) = watch::channel(ProcessLifecycleState::Uninitialized);
    let (runnable_tx, runnable_rx) = watch::channel::<Vec<String>>(Vec::new());

    let agent = Gents::from_default_behavior_documents(
        node.clone(),
        identity.clone(),
        DocumentRuntimeOptions {
            mcp_pool: McpPool::new(),
            local_hostname: Some(local_hostname),
            tool_ceiling,
            backend_health: Some(backend_health),
            process_state_observer: Some(Arc::new(CliReadyObserver { tx: ready_tx })),
            runtime_snapshot_observer: Some(Arc::new(CliRunnableBehaviorObserver {
                tx: runnable_tx,
            })),
            ..Default::default()
        },
    )
    .await
    .with_context(|| {
        format!(
            "starting gents server from {}\n{}",
            home_dir.display(),
            server_start_failure_hint(&home_dir)
        )
    })?;
    let runnable_behaviors = agent
        .behaviors()
        .iter()
        .map(|behavior| {
            json!({
                "behavior_id": behavior.behavior_id,
                "backend_id": behavior.backend_id,
                "model_name": behavior.model_name,
            })
        })
        .collect::<Vec<_>>();
    let default_behavior_id = agent.default_behavior_id().to_string();
    let unavailable_behaviors = agent.unavailable_behaviors().clone();
    let behavior_readiness = if unavailable_behaviors.is_empty() {
        "ready"
    } else {
        "degraded"
    };
    let background_execution_registry = agent.background_execution_registry();

    let shutdown_rx = match external_shutdown {
        Some(shutdown_rx) => shutdown_rx,
        None => {
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    let _ = shutdown_tx.send(true);
                }
            });
            shutdown_rx
        }
    };

    let mut run_handle = tokio::spawn(agent.run(shutdown_rx));
    loop {
        if *ready_rx.borrow() == ProcessLifecycleState::Ready {
            break;
        }

        tokio::select! {
            changed = ready_rx.changed() => {
                if changed.is_err() {
                    break;
                }
            }
            joined = &mut run_handle => {
                let result = joined.context("joining gents runtime task")?;
                return result;
            }
        }
    }

    let p2p_status =
        load_local_server_p2p_status(node.as_ref(), args.p2p_transport, p2p_admission).await?;
    write_runtime_state(
        &home_dir,
        &StoredRuntimeState {
            home: home_dir.to_string_lossy().to_string(),
            graphql: graphql_url.clone(),
            agent_name: agent_name.clone(),
            agent_did: identity.did().to_string(),
            default_behavior_id: default_behavior_id.clone(),
            p2p_transport: p2p_status
                .get("p2p_transport")
                .and_then(Value::as_str)
                .unwrap_or(P2pTransportArg::None.as_str())
                .to_string(),
            p2p_peer_id: p2p_status
                .get("p2p_peer_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            p2p_listen_addresses: p2p_status
                .get("p2p_listen_addresses")
                .and_then(Value::as_array)
                .map(|rows| {
                    rows.iter()
                        .filter_map(Value::as_str)
                        .map(ToOwned::to_owned)
                        .collect()
                })
                .unwrap_or_default(),
            p2p_admission: p2p_admission_state,
        },
    )?;

    let mut codex_shim_output = None;
    let codex_shim_bind_args = CodexShimBindArgs {
        home: home_dir.clone(),
        fs_root: effective_tool_root.clone(),
        node: node.clone(),
        background_execution_registry: background_execution_registry.clone(),
        graphql: graphql_url.clone(),
        agent_did: identity.did().to_string(),
        behavior_id: args.codex_shim_behavior_id.clone(),
        auth_token: codex_shim_auth_token,
        bind_addr: args.codex_shim_bind_addr,
        port: args.codex_shim_port,
        timeout_secs: args.codex_shim_timeout_secs,
        poll_ms: args.codex_shim_poll_ms,
    };
    let mut codex_shim_handle = if args.no_codex_shim {
        None
    } else {
        match bind_codex_shim(codex_shim_bind_args.clone()).await {
            Ok(bound) => {
                let announced = announce_codex_shim(&bound, &args);
                set_codex_shim_health(
                    &codex_shim_health,
                    CodexShimHealth::Listening {
                        websocket: announced
                            .get("websocket")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        auth_required: bound.auth_required(),
                        bound_agent_did: bound.agent_did().to_string(),
                        bound_behavior_id: bound.behavior_id().to_string(),
                    },
                );
                codex_shim_output = Some(announced);
                Some(bound.spawn())
            }
            Err(error) if error.is_dependency_missing() => {
                let bound_behavior_id =
                    crate::commands::codex_shim::resolve_codex_shim_behavior_id(
                        node.as_ref(),
                        args.codex_shim_behavior_id.as_deref(),
                        identity.did(),
                    )
                    .await;
                eprintln!("Codex endpoint pending: {:#}", error.error());
                eprintln!(
                    "The server keeps running. The shim binds by itself once behavior {bound_behavior_id:?} \
                     becomes runnable (for example after `gents config apply`) — no restart needed."
                );
                codex_shim_output = Some(json!({
                    "pending": true,
                    "bound_behavior_id": bound_behavior_id,
                    "reason": format!("{:#}", error.error()),
                }));
                set_codex_shim_health(
                    &codex_shim_health,
                    CodexShimHealth::Pending {
                        bound_behavior_id: bound_behavior_id.clone(),
                        reason: format!("{:#}", error.error()),
                    },
                );
                spawn_codex_shim_supervisor(
                    codex_shim_bind_args.clone(),
                    bound_behavior_id,
                    runnable_rx,
                    args.codex_shim_public_url.clone(),
                    args.codex_shim_auth_token_env.clone(),
                    codex_shim_health.clone(),
                );
                None
            }
            Err(error) => {
                eprintln!("Codex endpoint disabled: {:#}", error.error());
                eprintln!(
                    "The server keeps running without it. Fix the cause and restart, pick another port with --codex-shim-port, or silence this with --no-codex-shim."
                );
                codex_shim_output = Some(json!({
                    "disabled": true,
                    "reason": format!("{:#}", error.error()),
                }));
                set_codex_shim_health(
                    &codex_shim_health,
                    CodexShimHealth::Disabled {
                        reason: format!("{:#}", error.error()),
                    },
                );
                None
            }
        }
    };

    // Optional pack apply against the same in-process node (schemas/ first,
    // then desired-state). Uses Local access so collection registration works
    // without a separate home open / remote schema API.
    let pack_apply = if let Some(root) = args.apply_root.as_ref() {
        Some(
            apply_pack_after_ready(node.clone(), &home_dir, root, args.apply_prune)
                .await
                .with_context(|| {
                    format!(
                        "server --apply-root {} failed; the server is shutting down rather than \
                     serving without the requested pack. Schema registration is not \
                     transactional, so schemas/ may be partially applied — fix the pack and \
                     restart.",
                        root.display()
                    )
                })?,
        )
    } else {
        None
    };

    let output = json!({
        "status": "serving",
        "behavior_readiness": behavior_readiness,
        "home": home_dir,
        "agent_name": agent_name,
        "agent_did": identity.did(),
        "default_behavior_id": default_behavior_id,
        "tool_ceiling": format_tool_ceiling(effective_tool_ceiling),
        "tool_root": effective_tool_root,
        "runnable_behaviors": runnable_behaviors,
        "unavailable_behaviors": unavailable_behaviors,
        "graphql": graphql_url,
        "p2p_transport": p2p_status.get("p2p_transport").cloned().unwrap_or(Value::String(default_p2p_transport())),
        "p2p_peer_id": p2p_status.get("p2p_peer_id").cloned().unwrap_or(Value::Null),
        "p2p_listen_addresses": p2p_status.get("p2p_listen_addresses").cloned().unwrap_or_else(|| json!([])),
        "p2p_admission": p2p_status.get("p2p_admission").cloned().unwrap_or(Value::Null),
        "codex_shim": codex_shim_output,
        "apply_root": pack_apply,
    });
    if let Some(ready) = ready {
        let _ = ready.send(output.clone());
    }
    print_json(&output)?;
    if args.p2p_transport == P2pTransportArg::Iroh {
        if let Some(admission) = output.get("p2p_admission") {
            eprintln!(
                "P2P admission: pending_dags={} push_tasks={} dag_fetches={} rate_burst={} rate/s={}",
                admission
                    .get("max_pending_dags")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                admission
                    .get("max_concurrent_push_tasks")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                admission
                    .get("max_concurrent_dag_fetches")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                admission
                    .get("rate_limit_burst")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                admission
                    .get("rate_limit_rate")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0),
            );
        }
        eprintln!(
            "gents server is running with IROH P2P. Press Ctrl-C to stop. For the desktop demo, run `gents-desktop init`, launch `gents-desktop`, wait for `replication: subscriptions armed`, then chat."
        );
    } else {
        eprintln!("gents server is running local-only. Press Ctrl-C to stop.");
    }

    if let Some(handle) = codex_shim_handle.as_mut() {
        tokio::select! {
            result = &mut run_handle => {
                result.context("joining gents runtime task")?
            }
            result = handle => {
                result.context("joining Codex shim task")?
                    .context("Codex shim task failed")?;
                Ok(())
            }
        }
    } else {
        run_handle.await.context("joining gents runtime task")?
    }
}

struct ServerIdentity {
    identity: Arc<dyn AgentIdentity>,
    node_identity_did: Option<String>,
}

fn resolve_server_identity(
    args: &ServeArgs,
    init_config: Option<&StoredInitConfig>,
    home_dir: &Path,
    agent_name: &str,
) -> Result<ServerIdentity> {
    if let Some(config) = init_config {
        let agent_did = config.agent_did.trim();
        if has_agent_did(agent_did)
            && args.key_path.is_none()
            && config
                .key_path
                .as_deref()
                .map(str::trim)
                .is_none_or(str::is_empty)
        {
            return resolve_no_key_server_identity(config, home_dir);
        }
    }

    let key_path = resolve_server_key_path(args, init_config, home_dir, agent_name)?;
    ensure_key_path_exists_for_initialized_did(init_config, &key_path)?;
    if let Some(parent) = key_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating key directory {}", parent.display()))?;
    }
    let identity = Arc::new(
        KeyIdentity::load_or_create(&key_path, None)
            .context("creating or loading agent identity key")?,
    );
    ensure_identity_matches_init_config(init_config, identity.did())?;
    Ok(ServerIdentity {
        identity,
        node_identity_did: None,
    })
}

fn resolve_no_key_server_identity(
    config: &StoredInitConfig,
    home_dir: &Path,
) -> Result<ServerIdentity> {
    let backend = config
        .identity_backend
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "initialized home {} has agent DID {} but no key_path or identity_backend",
                home_dir.display(),
                config.agent_did
            )
        })?;
    match backend {
        "macos-keychain" => {
            let label = config
                .keychain_label
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "initialized home {} uses macos-keychain but has no keychain_label",
                        home_dir.display()
                    )
                })?;
            let identity = Arc::new(
                load_macos_keychain_identity(label, None)
                    .with_context(|| format!("loading macOS keychain identity {label}"))?,
            );
            ensure_identity_matches_init_config(Some(config), identity.did())?;
            Ok(ServerIdentity {
                node_identity_did: Some(identity.did().to_string()),
                identity,
            })
        }
        "macos-secure-enclave" => {
            let label = config
                .secure_enclave_label
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "initialized home {} uses macos-secure-enclave but has no secure_enclave_label",
                        home_dir.display()
                    )
                })?;
            let identity = Arc::new(
                load_macos_secure_enclave_identity(label, None)
                    .with_context(|| format!("loading macOS Secure Enclave identity {label}"))?,
            );
            ensure_identity_matches_init_config(Some(config), identity.did())?;
            Ok(ServerIdentity {
                node_identity_did: Some(identity.did().to_string()),
                identity,
            })
        }
        other => anyhow::bail!(
            "initialized home {} uses unsupported identity_backend {other:?} without key_path",
            home_dir.display()
        ),
    }
}

fn default_p2p_transport() -> String {
    P2pTransportArg::Iroh.as_str().to_string()
}

fn resolve_server_key_path(
    args: &ServeArgs,
    init_config: Option<&StoredInitConfig>,
    home_dir: &Path,
    agent_name: &str,
) -> Result<PathBuf> {
    if let Some(path) = args.key_path.clone() {
        return Ok(path);
    }

    if let Some(config) = init_config {
        if let Some(path) = config
            .key_path
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return Ok(PathBuf::from(path));
        }
    }

    Ok(default_key_path(home_dir, agent_name))
}

fn ensure_identity_matches_init_config(
    init_config: Option<&StoredInitConfig>,
    resolved_did: &str,
) -> Result<()> {
    let Some(config) = init_config else {
        return Ok(());
    };
    if has_agent_did(&config.agent_did) && config.agent_did.trim() != resolved_did {
        anyhow::bail!(
            "initialized home agent DID {} does not match loaded identity DID {}; repair init.json or use the correct --key-path",
            config.agent_did,
            resolved_did
        );
    }
    Ok(())
}

fn ensure_key_path_exists_for_initialized_did(
    init_config: Option<&StoredInitConfig>,
    key_path: &Path,
) -> Result<()> {
    let Some(config) = init_config else {
        return Ok(());
    };
    if has_agent_did(&config.agent_did) && !key_path.exists() {
        anyhow::bail!(
            "initialized home agent DID {} requires identity key {} to already exist; restore the configured key, pass --key-path for the matching key, or bootstrap the host identity backend first",
            config.agent_did,
            key_path.display()
        );
    }
    Ok(())
}

fn has_agent_did(did: &str) -> bool {
    !did.trim().is_empty()
}

fn default_p2p_secret_key_path(home_dir: &Path) -> PathBuf {
    home_dir.join("p2p-secret-key")
}

fn resolve_server_p2p_config(
    home_dir: &Path,
    args: &ServeArgs,
) -> Result<Option<defra_node::P2PConfig>> {
    if args.p2p_transport == P2pTransportArg::None {
        return Ok(None);
    }

    let max_pending_dags = args
        .p2p_max_pending_dags
        .unwrap_or(crate::DEFAULT_P2P_MAX_PENDING_DAGS);
    let max_concurrent_push_tasks = args
        .p2p_max_concurrent_push_tasks
        .unwrap_or(crate::DEFAULT_P2P_MAX_CONCURRENT_PUSH_TASKS);
    let max_concurrent_dag_fetches = args
        .p2p_max_concurrent_dag_fetches
        .unwrap_or(crate::DEFAULT_P2P_MAX_CONCURRENT_DAG_FETCHES);
    let rate_limit_burst = args
        .p2p_rate_limit_burst
        .unwrap_or(crate::DEFAULT_P2P_RATE_LIMIT_BURST);
    let rate_limit_rate = args
        .p2p_rate_limit_rate
        .unwrap_or(crate::DEFAULT_P2P_RATE_LIMIT_RATE);
    validate_p2p_admission_config(
        max_pending_dags,
        max_concurrent_push_tasks,
        max_concurrent_dag_fetches,
        rate_limit_burst,
        rate_limit_rate,
    )?;

    let secret_key_path = args
        .p2p_secret_key_path
        .clone()
        .unwrap_or_else(|| default_p2p_secret_key_path(home_dir));
    if let Some(parent) = secret_key_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating P2P key directory {}", parent.display()))?;
    }
    Ok(Some(defra_node::P2PConfig {
        port: args.p2p_port.unwrap_or(0),
        bind_addr: Some(
            args.p2p_bind_addr
                .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        ),
        relay_mode: match args.p2p_relay_mode {
            P2pRelayModeArg::Default => p2p::iroh::IrohRelayModeConfig::Default,
            P2pRelayModeArg::Disabled => p2p::iroh::IrohRelayModeConfig::Disabled,
        },
        discovery: match args.p2p_discovery {
            P2pDiscoveryArg::N0 => p2p::iroh::IrohDiscoveryConfig::N0,
            P2pDiscoveryArg::Disabled => p2p::iroh::IrohDiscoveryConfig::Disabled,
        },
        max_concurrent_multipath_paths: None,
        secret_key_path: Some(secret_key_path),
        load_persisted_collections: true,
        max_concurrent_dag_fetches,
        max_concurrent_push_tasks,
        rate_limit_burst,
        rate_limit_rate,
        max_doc_sync_request_doc_ids: p2p::sync::DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS,
        max_pending_dags,
    }))
}

/// Upper bound matching `tokio::sync::Semaphore::MAX_PERMITS` (`usize::MAX >> 3`).
/// Worker counts flow into `Semaphore::new` and panic above this.
const MAX_P2P_SEMAPHORE_PERMITS: usize = usize::MAX >> 3;
const MAX_P2P_METRICS_GAUGE: usize = i64::MAX as usize;

/// Reject degenerate or unrepresentable admission values before the node starts.
/// Upstream clamps some zeros to `1`, but an explicit zero is almost always an
/// operator footgun. Huge values can panic `Semaphore::new` or wrap i64 metrics.
fn validate_p2p_admission_config(
    max_pending_dags: usize,
    max_concurrent_push_tasks: usize,
    max_concurrent_dag_fetches: usize,
    rate_limit_burst: u32,
    rate_limit_rate: f64,
) -> Result<()> {
    if max_pending_dags == 0 {
        anyhow::bail!("--p2p-max-pending-dags must be > 0");
    }
    if max_pending_dags > MAX_P2P_METRICS_GAUGE {
        anyhow::bail!(
            "--p2p-max-pending-dags must be <= {MAX_P2P_METRICS_GAUGE} (metrics gauge limit)"
        );
    }
    if max_concurrent_push_tasks == 0 {
        anyhow::bail!("--p2p-max-concurrent-push-tasks must be > 0");
    }
    if max_concurrent_push_tasks > MAX_P2P_SEMAPHORE_PERMITS {
        anyhow::bail!(
            "--p2p-max-concurrent-push-tasks must be <= {MAX_P2P_SEMAPHORE_PERMITS} (tokio Semaphore::MAX_PERMITS)"
        );
    }
    if max_concurrent_dag_fetches == 0 {
        anyhow::bail!("--p2p-max-concurrent-dag-fetches must be > 0");
    }
    if max_concurrent_dag_fetches > MAX_P2P_SEMAPHORE_PERMITS {
        anyhow::bail!(
            "--p2p-max-concurrent-dag-fetches must be <= {MAX_P2P_SEMAPHORE_PERMITS} (tokio Semaphore::MAX_PERMITS)"
        );
    }
    if rate_limit_burst == 0 {
        anyhow::bail!("--p2p-rate-limit-burst must be > 0");
    }
    if !rate_limit_rate.is_finite() || rate_limit_rate <= 0.0 {
        anyhow::bail!("--p2p-rate-limit-rate must be a finite value > 0");
    }
    Ok(())
}

fn log_p2p_admission_config(config: &defra_node::P2PConfig) {
    tracing::info!(
        max_pending_dags = config.max_pending_dags,
        max_concurrent_push_tasks = config.max_concurrent_push_tasks,
        max_concurrent_dag_fetches = config.max_concurrent_dag_fetches,
        rate_limit_burst = config.rate_limit_burst,
        rate_limit_rate = config.rate_limit_rate,
        "P2P admission configuration"
    );
}

fn p2p_admission_state(config: &defra_node::P2PConfig) -> P2pAdmissionState {
    P2pAdmissionState {
        max_pending_dags: config.max_pending_dags,
        max_concurrent_push_tasks: config.max_concurrent_push_tasks,
        max_concurrent_dag_fetches: config.max_concurrent_dag_fetches,
        rate_limit_burst: config.rate_limit_burst,
        rate_limit_rate: config.rate_limit_rate,
    }
}

async fn load_local_server_p2p_status(
    node: &EmbeddedNode,
    transport: P2pTransportArg,
    admission: Option<Value>,
) -> Result<Value> {
    let admission = admission.unwrap_or(Value::Null);
    match transport {
        P2pTransportArg::None => Ok(json!({
            "enabled": false,
            "p2p_transport": transport.as_str(),
            "p2p_peer_id": Value::Null,
            "p2p_listen_addresses": [],
            "p2p_connected_peers": [],
            "p2p_admission": admission,
        })),
        P2pTransportArg::Iroh => {
            let p2p = node.p2p().ok_or_else(|| {
                anyhow::anyhow!(
                    "P2P transport was requested but is not available on the embedded node"
                )
            })?;
            let peer_id = p2p
                .local_peer_id()
                .await
                .context("loading local P2P peer id from the embedded node")?;
            let listen_addresses = wait_for_p2p_listen_addresses(p2p).await?;
            let connected_peers = p2p
                .connected_peers()
                .await
                .context("loading connected P2P peers from the embedded node")?;
            Ok(json!({
                "enabled": true,
                "p2p_transport": transport.as_str(),
                "p2p_peer_id": peer_id,
                "p2p_listen_addresses": listen_addresses,
                "p2p_connected_peers": connected_peers,
                "p2p_admission": admission,
            }))
        }
    }
}

async fn wait_for_p2p_listen_addresses(
    p2p: &dyn defra_p2p_adapter::P2POperations,
) -> Result<Vec<String>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let listen_addresses = p2p
            .listen_addresses()
            .await
            .context("loading local P2P listen addresses from the embedded node")?;
        if !listen_addresses.is_empty() {
            return Ok(listen_addresses);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(listen_addresses);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn resolve_default_tool_root(explicit: Option<&Path>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path.to_path_buf());
    }

    std::env::current_dir()
        .ok()
        .or_else(|| std::env::var_os("HOME").map(PathBuf::from))
        .ok_or_else(|| anyhow::anyhow!("unable to determine a default tool root for local tools"))
}

fn display_shim_host(host: IpAddr) -> String {
    let host_text = display_host(host);
    if host.is_ipv6() {
        format!("[{host_text}]")
    } else {
        host_text
    }
}

#[cfg(test)]
mod shim_host_tests {
    use super::*;
    use crate::cli::{Cli, Command};
    use clap::Parser;

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
    fn ipv6_shim_hosts_are_bracketed() {
        assert_eq!(display_shim_host("::1".parse().unwrap()), "[::1]");
        assert_eq!(display_shim_host("127.0.0.1".parse().unwrap()), "127.0.0.1");
    }

    #[test]
    fn public_shim_url_requires_a_root_wss_endpoint_with_explicit_port() {
        assert_eq!(
            normalize_codex_shim_public_url("wss://agent.example:443/").unwrap(),
            "wss://agent.example:443/"
        );
        assert!(normalize_codex_shim_public_url("ws://agent.example:9292/").is_err());
        assert!(normalize_codex_shim_public_url("wss://agent.example/").is_err());
        assert!(normalize_codex_shim_public_url("wss://agent.example:443/rpc").is_err());
    }

    #[test]
    fn shim_auth_token_env_is_trimmed_and_required() {
        assert_eq!(
            read_codex_shim_auth_token_with("GENTS_TOKEN", |_| Ok(" secret ".to_string())).unwrap(),
            "secret"
        );
        assert!(read_codex_shim_auth_token_with("GENTS_TOKEN", |_| Ok(" ".to_string())).is_err());
        assert!(read_codex_shim_auth_token_with("GENTS_TOKEN", |_| {
            Err(std::env::VarError::NotPresent)
        })
        .is_err());
    }

    #[test]
    fn shim_launch_command_references_the_token_environment_variable() {
        assert_eq!(
            codex_shim_launch_command(
                "wss://agent.example:443/",
                Some("GENTS_REMOTE_TOKEN")
            ),
            "gents codex --remote wss://agent.example:443/ --remote-auth-token-env GENTS_REMOTE_TOKEN"
        );
    }

    #[test]
    fn server_parses_remote_shim_security_flags() {
        let args = parse_server(&[
            "--codex-shim-auth-token-env",
            "GENTS_REMOTE_TOKEN",
            "--codex-shim-public-url",
            "wss://agent.example:443/",
        ]);
        assert_eq!(
            args.codex_shim_auth_token_env.as_deref(),
            Some("GENTS_REMOTE_TOKEN")
        );
        assert_eq!(
            args.codex_shim_public_url.as_deref(),
            Some("wss://agent.example:443/")
        );
    }

    #[test]
    fn server_p2p_config_uses_upstream_admission_defaults() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let args = parse_server(&[]);

        let config = resolve_server_p2p_config(tempdir.path(), &args)
            .expect("resolve p2p config")
            .expect("p2p enabled");

        assert_eq!(config.max_pending_dags, p2p::sync::DEFAULT_MAX_PENDING_DAGS);
        assert_eq!(
            config.max_concurrent_push_tasks,
            p2p::sync::DEFAULT_MAX_CONCURRENT_PUSH_TASKS
        );
        assert_eq!(
            config.max_concurrent_dag_fetches,
            p2p::sync::DEFAULT_MAX_CONCURRENT_DAG_FETCHES
        );
        assert_eq!(config.rate_limit_burst, p2p::sync::DEFAULT_RATE_LIMIT_BURST);
        assert_eq!(config.rate_limit_rate, p2p::sync::DEFAULT_RATE_LIMIT_RATE);
    }

    #[test]
    fn server_p2p_config_uses_admission_overrides() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let args = parse_server(&[
            "--p2p-max-pending-dags",
            "222",
            "--p2p-max-concurrent-push-tasks",
            "16",
            "--p2p-max-concurrent-dag-fetches",
            "11",
            "--p2p-rate-limit-burst",
            "333",
            "--p2p-rate-limit-rate",
            "44.5",
        ]);

        let config = resolve_server_p2p_config(tempdir.path(), &args)
            .expect("resolve p2p config")
            .expect("p2p enabled");

        assert_eq!(config.max_pending_dags, 222);
        assert_eq!(config.max_concurrent_push_tasks, 16);
        assert_eq!(config.max_concurrent_dag_fetches, 11);
        assert_eq!(config.rate_limit_burst, 333);
        assert_eq!(config.rate_limit_rate, 44.5);
    }

    #[test]
    fn server_p2p_config_rejects_degenerate_admission_values() {
        let tempdir = tempfile::tempdir().expect("tempdir");

        let zero_pending = parse_server(&["--p2p-max-pending-dags", "0"]);
        assert!(resolve_server_p2p_config(tempdir.path(), &zero_pending).is_err());

        let zero_push = parse_server(&["--p2p-max-concurrent-push-tasks", "0"]);
        assert!(resolve_server_p2p_config(tempdir.path(), &zero_push).is_err());

        let zero_rate = parse_server(&["--p2p-rate-limit-rate", "0"]);
        assert!(resolve_server_p2p_config(tempdir.path(), &zero_rate).is_err());

        let negative_rate = parse_server(&["--p2p-rate-limit-rate=-1.0"]);
        assert!(resolve_server_p2p_config(tempdir.path(), &negative_rate).is_err());

        // tokio::Semaphore::new panics above MAX_PERMITS (usize::MAX >> 3).
        let huge_push = parse_server(&["--p2p-max-concurrent-push-tasks", &usize::MAX.to_string()]);
        assert!(resolve_server_p2p_config(tempdir.path(), &huge_push).is_err());

        let over_gauge = ((i64::MAX as u128) + 1).to_string();
        let huge_pending = parse_server(&["--p2p-max-pending-dags", &over_gauge]);
        assert!(resolve_server_p2p_config(tempdir.path(), &huge_pending).is_err());
    }
}
