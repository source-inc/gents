use std::fs;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use gents::config::{
    DEFAULT_CONTEXT_WINDOW, DEFAULT_DEADLINE_DURATION_SECS, DEFAULT_MAX_OUTPUT_TOKENS,
    DEFAULT_MAX_TURNS, DEFAULT_STREAM_BATCH_MS, DEFAULT_STREAM_LIVENESS_TIMEOUT_SECS,
};
use gents::{
    default_behavior_id_for_agent, default_inference_profile_id_for_behavior,
    default_tool_selection_id_for_behavior, load_agent_behavior, load_agent_principal,
    load_or_create_macos_keychain_identity, load_or_create_macos_secure_enclave_identity,
    upsert_agent_principal, upsert_inference_profile, wide_open_tool_selection_document,
    wide_open_tool_selection_id_for_agent, AgentBehaviorDocument, AgentIdentity, InferenceProfile,
    KeyIdentity, ToolSelectionDocument,
};
use serde::Serialize;
use serde_json::json;

use crate::cli::*;
use crate::config_writes::{
    write_agent_behavior_document, write_inference_backend_document, write_tool_selection_document,
    ConfigAccess, InferenceBackendUpsertDocument,
};
use crate::shared::*;
use crate::{
    clear_runtime_state, dangerously_overwrite_home, default_data_dir, default_key_path,
    format_tool_ceiling, format_tool_package, normalize_optional_string, print_json,
    resolve_home_dir, write_init_config, BackendResolutionMode, DEFAULT_HTTP_PORT,
};

const STANDARD_READONLY_SYSTEM_PROMPT: &str = r#"You are a terminal-native engineering and operations agent running for the user inside a local DefraDB runtime.

Your job is to help with software work, debugging, codebase inspection, incident triage, release checks, infrastructure investigation, and general computer operations tasks. Build your conclusions from real evidence: inspect files, logs, command output, and tool results before making claims.

Work like a strong command-line operator:
- be concise and factual
- prefer direct answers over long essays
- explain what you found, not what you assume
- propose the next command, file, or check when it helps

You are currently in a read-only operating mode for local tools. You can inspect local state, but you cannot modify files or perform write-capable shell actions. If the user asks for a change, say clearly that the current tool mode is read-only and describe the exact edit or command you would apply if write access were enabled."#;
const STANDARD_READWRITE_SYSTEM_PROMPT: &str = r#"You are a terminal-native engineering and operations agent running for the user inside a local DefraDB runtime.

Your job is to help with software work, debugging, code changes, codebase maintenance, incident triage, release checks, infrastructure investigation, and general computer operations tasks. Build your conclusions from real evidence: inspect files, logs, command output, and tool results before making claims.

Work like a strong command-line operator:
- inspect first, then act
- keep changes focused and easy to explain
- prefer direct answers over long essays
- summarize exactly what changed and why
- avoid broad or risky operations unless the user clearly wants them

You have write-capable local tools. When the user asks you to make a change, you may edit files and use write-capable shell actions deliberately. Read the relevant state first, make the smallest effective change, and report the concrete outcome.

For long-running commands such as builds, test suites, installs, servers, and log tails, prefer spawn_process with tool_name "bash_unrestricted" instead of shell backgrounding with "&". Use list_processes, read_process, wait_process, or cancel_process to inspect, finish, or stop backgrounded work."#;

const YOLO_WARNING: &str = "\
WARNING: --yolo bootstraps UNRESTRICTED tools. The agent can run any command\n\
and write any file your user account can reach — no sandbox, no containment.\n\
Use --write for sandboxed writes scoped to the tool root.";

pub(crate) async fn init(mut args: InitArgs) -> Result<()> {
    let home_dir = resolve_home_dir(args.home.as_deref());
    let tool_package = resolve_init_tool_package(args.write_tools, args.yolo, args.tool_package)?;
    validate_init_tool_flags(&args, tool_package)?;
    if matches!(tool_package, ToolPackageArg::Yolo) {
        eprintln!("{YOLO_WARNING}");
    }
    crate::interactive_backend::resolve_backend_interactively(&mut args).await?;
    if args.dangerously_overwrite {
        dangerously_overwrite_home(&home_dir)?;
    }
    let data_dir = args
        .data_dir
        .clone()
        .unwrap_or_else(|| default_data_dir(&home_dir));
    fs::create_dir_all(&data_dir)
        .with_context(|| format!("creating data directory {}", data_dir.display()))?;

    if args.identity_only {
        if args.identity_backend != IdentityBackendArg::File && args.key_path.is_some() {
            anyhow::bail!("--key-path cannot be used with non-file identity backends");
        }
        let key_path = (args.identity_backend == IdentityBackendArg::File).then(|| {
            args.key_path
                .clone()
                .unwrap_or_else(|| default_key_path(&home_dir, &args.agent_name))
        });
        let summary = write_identity_only_home_metadata(IdentityOnlyHomeOptions {
            home: &home_dir,
            agent_name: &args.agent_name,
            key_path: key_path.as_deref(),
            identity_backend: args.identity_backend,
            keychain_label: args.keychain_label.as_deref(),
            secure_enclave_label: args.secure_enclave_label.as_deref(),
            tool_package,
            tool_root: args.tool_root.as_deref(),
            reset: args.reset,
        })
        .await?;
        let output = json!({
            "status": "initialized",
            "identity_only": true,
            "home": summary.home,
            "agent_name": summary.agent_name,
            "agent_did": summary.agent_did,
            "key_path": summary.key_path,
            "tool_package": format_tool_package(summary.tool_package),
            "tool_ceiling": format_tool_ceiling(summary.tool_ceiling),
            "tool_root": summary.tool_root,
            "runtime_state_reset": summary.runtime_state_reset,
            "identity": {
                "agent_did": summary.agent_did,
                "key_path": summary.key_path,
                "identity_backend": summary.identity_backend,
                "keychain_label": summary.keychain_label,
                "secure_enclave_label": summary.secure_enclave_label,
                "permission_boundary": "This DID and key identify the permission boundary for every action the agent runtime performs."
            },
            "next_steps": [
                "gents config apply --root <manifest-root> --home <home> --bind-agent-did home",
                "gents server"
            ],
            "init": null
        });
        print_json(&output)?;
        return Ok(());
    }

    let initialized_identity = load_or_create_home_identity(HomeIdentityOptions {
        home: &home_dir,
        agent_name: &args.agent_name,
        key_path: args.key_path.as_deref(),
        identity_backend: args.identity_backend,
        keychain_label: args.keychain_label.as_deref(),
        secure_enclave_label: args.secure_enclave_label.as_deref(),
    })?;
    initialized_identity
        .identity
        .sign(b"gents init identity")
        .await
        .context("creating or loading agent identity key")?;

    let mut node_builder = crate::persistent_node_builder(&data_dir);
    if let Some(node_identity_did) = initialized_identity.node_identity_did.as_ref() {
        node_builder = node_builder.with_node_identity_did(node_identity_did.clone());
    }
    let node_arc = std::sync::Arc::new(
        node_builder
            .build()
            .await
            .context("building embedded DefraDB node for init")?,
    );
    gents::migration::ensure_all_runtime_migrations(node_arc.clone()).await?;
    let node = std::sync::Arc::try_unwrap(node_arc).unwrap_or_else(|_| {
        unreachable!("node_arc had exactly one strong reference at this point")
    });

    let access = ConfigAccess::Local(std::sync::Arc::new(node));
    let summary = initialize_runtime_home(
        &access,
        &args,
        initialized_identity.identity.did(),
        tool_package,
    )
    .await?;
    let stored = StoredInitConfig {
        home: home_dir.to_string_lossy().to_string(),
        agent_name: args.agent_name.clone(),
        agent_did: initialized_identity.identity.did().to_string(),
        key_path: initialized_identity.key_path.clone(),
        identity_backend: initialized_identity.identity_backend.clone(),
        keychain_label: initialized_identity.keychain_label.clone(),
        secure_enclave_label: initialized_identity.secure_enclave_label.clone(),
        tool_package: Some(tool_package),
        tool_ceiling: summary.tool_ceiling,
        tool_root: summary.tool_root.clone(),
    };
    write_init_config(&home_dir, &stored)?;
    let runtime_state_reset = if args.reset {
        clear_runtime_state(&home_dir)?
    } else {
        false
    };

    let codex_login =
        maybe_inline_codex_login(&access, initialized_identity.identity.did(), &summary).await;
    let grok_login =
        maybe_inline_grok_login(&access, initialized_identity.identity.did(), &summary).await;

    let output = json!({
        "status": "initialized",
        "home": home_dir,
        "agent_name": args.agent_name,
        "agent_did": initialized_identity.identity.did(),
        "key_path": initialized_identity.key_path,
        "identity_backend": initialized_identity.identity_backend,
        "keychain_label": initialized_identity.keychain_label,
        "secure_enclave_label": initialized_identity.secure_enclave_label,
        "default_behavior_id": summary.default_behavior_id,
        "tool_selection_id": summary.tool_selection_id,
        "wide_open_preset_id": summary.wide_open_preset_id,
        "inference_profile_id": summary.inference_profile_id,
        "tool_package": format_tool_package(summary.tool_package),
        "tool_ceiling": format_tool_ceiling(summary.tool_ceiling),
        "tool_root": summary.tool_root,
        "enable_memory": summary.enable_memory,
        "enable_defra_query": summary.enable_defra_query,
        "defra_query_collections": summary.defra_query_collections,
        "runtime_state_reset": runtime_state_reset,
        "identity": {
            "agent_did": initialized_identity.identity.did(),
            "key_path": stored.key_path,
            "identity_backend": stored.identity_backend,
            "keychain_label": stored.keychain_label,
            "secure_enclave_label": stored.secure_enclave_label,
            "permission_boundary": "This DID and key identify the permission boundary for every action the agent runtime performs."
        },
        "codex_login": codex_login
            .outcome()
            .map(crate::commands::codex_login::codex_login_result_json),
        "grok_login": grok_login
            .outcome()
            .map(crate::commands::grok_login::grok_login_result_json),
        "next_steps": init_next_steps(
            &summary,
            codex_login.is_authenticated(),
            grok_login.is_authenticated(),
        ),
        "init": summary,
    });
    print_json(&output)?;

    Ok(())
}

enum InlineCodexLoginState {
    Unauthenticated,
    ExistingCredential,
    Completed(crate::commands::codex_login::CodexLoginOutcome),
}

impl InlineCodexLoginState {
    fn is_authenticated(&self) -> bool {
        matches!(self, Self::ExistingCredential | Self::Completed(_))
    }

    fn outcome(&self) -> Option<&crate::commands::codex_login::CodexLoginOutcome> {
        match self {
            Self::Completed(outcome) => Some(outcome),
            Self::Unauthenticated | Self::ExistingCredential => None,
        }
    }
}

enum InlineGrokLoginState {
    Unauthenticated,
    ExistingCredential,
    Completed(crate::commands::grok_login::GrokLoginOutcome),
}

impl InlineGrokLoginState {
    fn is_authenticated(&self) -> bool {
        matches!(self, Self::ExistingCredential | Self::Completed(_))
    }

    fn outcome(&self) -> Option<&crate::commands::grok_login::GrokLoginOutcome> {
        match self {
            Self::Completed(outcome) => Some(outcome),
            Self::Unauthenticated | Self::ExistingCredential => None,
        }
    }
}

async fn maybe_inline_grok_login(
    access: &ConfigAccess,
    agent_did: &str,
    summary: &InitSummary,
) -> InlineGrokLoginState {
    if summary.provider_kind != gents::BackendProviderKind::XaiGrokOAuth {
        return InlineGrokLoginState::Unauthenticated;
    }
    if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
        return InlineGrokLoginState::Unauthenticated;
    }
    let provider = gents::xai_grok_oauth::normalize_provider("xai-oauth");
    match crate::commands::grok_auth_probe::load_oauth_credential(access, agent_did, &provider)
        .await
    {
        Ok(Some(_)) => return InlineGrokLoginState::ExistingCredential,
        Ok(None) => {}
        Err(error) => {
            eprintln!("Could not check for an existing Grok credential: {error:#}");
        }
    }
    if !crate::interactive_backend::confirm("Log in to Grok / xAI now to finish setup?", true).await
    {
        return InlineGrokLoginState::Unauthenticated;
    }
    match crate::commands::grok_login::run_grok_login(
        access,
        agent_did,
        &crate::commands::grok_login::GrokLoginOptions { provider },
    )
    .await
    {
        Ok(outcome) => InlineGrokLoginState::Completed(outcome),
        Err(error) => {
            eprintln!("Grok login failed: {error:#}");
            InlineGrokLoginState::Unauthenticated
        }
    }
}

async fn maybe_inline_codex_login(
    access: &ConfigAccess,
    agent_did: &str,
    summary: &InitSummary,
) -> InlineCodexLoginState {
    if summary.provider_kind != gents::BackendProviderKind::ChatGptCodex {
        return InlineCodexLoginState::Unauthenticated;
    }
    if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
        return InlineCodexLoginState::Unauthenticated;
    }
    let provider = gents::chatgpt_codex::normalize_provider("chatgpt-codex");
    match crate::commands::codex_auth_probe::load_oauth_credential(access, agent_did, &provider)
        .await
    {
        Ok(Some(_)) => return InlineCodexLoginState::ExistingCredential,
        Ok(None) => {}
        Err(error) => {
            eprintln!("Could not check for an existing ChatGPT credential: {error:#}");
        }
    }
    if !crate::interactive_backend::confirm("Log in to ChatGPT now to finish setup?", true).await {
        return InlineCodexLoginState::Unauthenticated;
    }
    match crate::commands::codex_login::run_codex_login(
        access,
        agent_did,
        &crate::commands::codex_login::CodexLoginOptions {
            provider,
            client_id: None,
            issuer: None,
            device_auth: false,
        },
    )
    .await
    {
        Ok(outcome) => InlineCodexLoginState::Completed(outcome),
        Err(error) => {
            eprintln!(
                "ChatGPT login did not complete: {error:#}\n\
                 The backend is configured; finish later with `gents codex-login` \
                 (add --device-auth on a headless machine)."
            );
            InlineCodexLoginState::Unauthenticated
        }
    }
}

pub(crate) struct IdentityOnlyHomeOptions<'a> {
    pub(crate) home: &'a Path,
    pub(crate) agent_name: &'a str,
    pub(crate) key_path: Option<&'a Path>,
    pub(crate) identity_backend: IdentityBackendArg,
    pub(crate) keychain_label: Option<&'a str>,
    pub(crate) secure_enclave_label: Option<&'a str>,
    pub(crate) tool_package: ToolPackageArg,
    pub(crate) tool_root: Option<&'a Path>,
    pub(crate) reset: bool,
}

struct HomeIdentityOptions<'a> {
    home: &'a Path,
    agent_name: &'a str,
    key_path: Option<&'a Path>,
    identity_backend: IdentityBackendArg,
    keychain_label: Option<&'a str>,
    secure_enclave_label: Option<&'a str>,
}

struct HomeIdentity {
    identity: Arc<dyn AgentIdentity>,
    key_path: Option<String>,
    identity_backend: Option<String>,
    keychain_label: Option<String>,
    secure_enclave_label: Option<String>,
    node_identity_did: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct IdentityOnlyHomeSummary {
    pub(crate) home: String,
    pub(crate) agent_name: String,
    pub(crate) agent_did: String,
    pub(crate) key_path: Option<String>,
    pub(crate) identity_backend: Option<String>,
    pub(crate) keychain_label: Option<String>,
    pub(crate) secure_enclave_label: Option<String>,
    pub(crate) tool_ceiling: ToolCeilingArg,
    pub(crate) tool_package: ToolPackageArg,
    pub(crate) tool_root: Option<String>,
    pub(crate) runtime_state_reset: bool,
}

pub(crate) async fn write_identity_only_home_metadata(
    options: IdentityOnlyHomeOptions<'_>,
) -> Result<IdentityOnlyHomeSummary> {
    let data_dir = default_data_dir(options.home);
    fs::create_dir_all(&data_dir)
        .with_context(|| format!("creating data directory {}", data_dir.display()))?;

    let initialized_identity = load_or_create_home_identity(HomeIdentityOptions {
        home: options.home,
        agent_name: options.agent_name,
        key_path: options.key_path,
        identity_backend: options.identity_backend,
        keychain_label: options.keychain_label,
        secure_enclave_label: options.secure_enclave_label,
    })?;
    initialized_identity
        .identity
        .sign(b"gents init identity")
        .await
        .context("creating or loading agent identity key")?;

    let tool_ceiling = tool_ceiling_for_package(options.tool_package);
    let tool_root = resolve_tool_root_for_package(options.tool_package, options.tool_root)?
        .map(|path| path.to_string_lossy().to_string());
    let stored = StoredInitConfig {
        home: options.home.to_string_lossy().to_string(),
        agent_name: options.agent_name.to_string(),
        agent_did: initialized_identity.identity.did().to_string(),
        key_path: initialized_identity.key_path.clone(),
        identity_backend: initialized_identity.identity_backend.clone(),
        keychain_label: initialized_identity.keychain_label.clone(),
        secure_enclave_label: initialized_identity.secure_enclave_label.clone(),
        tool_package: Some(options.tool_package),
        tool_ceiling,
        tool_root: tool_root.clone(),
    };
    write_init_config(options.home, &stored)?;
    let runtime_state_reset = if options.reset {
        clear_runtime_state(options.home)?
    } else {
        false
    };

    Ok(IdentityOnlyHomeSummary {
        home: options.home.to_string_lossy().to_string(),
        agent_name: options.agent_name.to_string(),
        agent_did: initialized_identity.identity.did().to_string(),
        key_path: initialized_identity.key_path,
        identity_backend: initialized_identity.identity_backend,
        keychain_label: initialized_identity.keychain_label,
        secure_enclave_label: initialized_identity.secure_enclave_label,
        tool_ceiling,
        tool_package: options.tool_package,
        tool_root,
        runtime_state_reset,
    })
}

fn load_or_create_home_identity(options: HomeIdentityOptions<'_>) -> Result<HomeIdentity> {
    match options.identity_backend {
        IdentityBackendArg::File => {
            let key_path = options
                .key_path
                .map(Path::to_path_buf)
                .unwrap_or_else(|| default_key_path(options.home, options.agent_name));
            if let Some(parent) = key_path.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("creating key directory {}", parent.display()))?;
            }
            let identity = Arc::new(
                KeyIdentity::load_or_create(&key_path, None)
                    .context("creating or loading agent identity key")?,
            );
            let node_identity_did = identity.did().to_string();
            Ok(HomeIdentity {
                identity,
                key_path: Some(key_path.to_string_lossy().to_string()),
                identity_backend: None,
                keychain_label: None,
                secure_enclave_label: None,
                node_identity_did: Some(node_identity_did),
            })
        }
        IdentityBackendArg::MacosKeychain => {
            if options.key_path.is_some() {
                anyhow::bail!("--key-path cannot be used with --identity-backend macos-keychain");
            }
            let label = options
                .keychain_label
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "--keychain-label is required with --identity-backend macos-keychain"
                    )
                })?;
            let identity = Arc::new(
                load_or_create_macos_keychain_identity(label, None)
                    .with_context(|| format!("loading macOS keychain identity {label}"))?,
            );
            let did = identity.did().to_string();
            Ok(HomeIdentity {
                identity,
                key_path: None,
                identity_backend: Some("macos-keychain".to_string()),
                keychain_label: Some(label.to_string()),
                secure_enclave_label: None,
                node_identity_did: Some(did),
            })
        }
        IdentityBackendArg::MacosSecureEnclave => {
            if options.key_path.is_some() {
                anyhow::bail!(
                    "--key-path cannot be used with --identity-backend macos-secure-enclave"
                );
            }
            let label = options
                .secure_enclave_label
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "--secure-enclave-label is required with --identity-backend macos-secure-enclave"
                    )
                })?;
            let identity = Arc::new(
                load_or_create_macos_secure_enclave_identity(label, None)
                    .with_context(|| format!("loading macOS Secure Enclave identity {label}"))?,
            );
            let did = identity.did().to_string();
            Ok(HomeIdentity {
                identity,
                key_path: None,
                identity_backend: Some("macos-secure-enclave".to_string()),
                keychain_label: None,
                secure_enclave_label: Some(label.to_string()),
                node_identity_did: Some(did),
            })
        }
    }
}

async fn initialize_runtime_home(
    access: &ConfigAccess,
    args: &InitArgs,
    agent_did: &str,
    tool_package: ToolPackageArg,
) -> Result<InitSummary> {
    let ConfigAccess::Local(node) = access else {
        anyhow::bail!("init requires local DefraDB access");
    };
    let explicit_backend_id = args
        .backend_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let explicit_backend_name = args
        .backend_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let model_name = resolve_init_model_name(args)?;
    let backend = resolve_init_backend_config(args)?;
    let backend_id_was_generated = explicit_backend_id.is_none();
    let backend_id = explicit_backend_id
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| default_backend_id_for_agent(agent_did));
    let backend_name = explicit_backend_name
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| {
            if backend_id_was_generated {
                format!("{} backend", args.agent_name)
            } else {
                backend_id.clone()
            }
        });
    let existing_principal = load_agent_principal(node, agent_did).await?;
    let default_behavior_id = existing_principal
        .as_ref()
        .and_then(|principal| normalize_optional_string(principal.default_behavior_id.as_deref()))
        .unwrap_or_else(|| default_behavior_id_for_agent(agent_did));
    let existing_default_behavior = load_agent_behavior(node, &default_behavior_id).await?;
    if let Some(behavior) = existing_default_behavior.as_ref() {
        if behavior.agent_did != agent_did {
            anyhow::bail!(
                "AgentBehavior {} belongs to {} not {}",
                default_behavior_id,
                behavior.agent_did,
                agent_did
            );
        }
    }
    let principal_display_name = existing_principal
        .as_ref()
        .and_then(|principal| normalize_optional_string(principal.display_name.as_deref()))
        .unwrap_or_else(|| args.agent_name.clone());
    let principal_enabled = existing_principal
        .as_ref()
        .map(|principal| principal.enabled)
        .unwrap_or(true);
    upsert_agent_principal(
        node,
        agent_did,
        Some(&principal_display_name),
        Some(&default_behavior_id),
        principal_enabled,
    )
    .await?;
    let tool_selection_id = default_tool_selection_id_for_behavior(&default_behavior_id);
    let tool_ceiling = tool_ceiling_for_package(tool_package);
    let tool_root = resolve_tool_root_for_package(tool_package, args.tool_root.as_deref())?;
    let backend_doc = InferenceBackendUpsertDocument {
        backend_id: backend_id.clone(),
        name: backend_name.clone(),
        provider_kind: backend.provider_kind,
        openai_wire_api: backend.openai_wire_api,
        endpoint: backend.endpoint.clone(),
        api_key: backend.api_key.clone(),
        api_key_env_var: backend.api_key_env_var.clone(),
        max_concurrent: args.max_concurrent,
        max_queue_depth: args.max_queue_depth,
        enabled: true,
        models_on_add: vec![model_name.to_string()],
        models_on_update: Some(vec![model_name.to_string()]),
        probe_status: "healthy".to_string(),
    };
    write_inference_backend_document(access, &backend_doc).await?;

    let enable_defra_query = init_enable_defra_query(
        tool_package,
        args.enable_defra_query,
        args.disable_defra_query,
    );
    let tool_selection = tool_selection_for_package(
        agent_did,
        &tool_selection_id,
        tool_package,
        args.enable_memory,
        enable_defra_query,
        args.defra_query_collections.clone(),
    );
    write_tool_selection_document(access, &tool_selection).await?;

    let wide_open_preset_id = wide_open_tool_selection_id_for_agent(agent_did);
    let wide_open_preset = wide_open_tool_selection_document(agent_did);
    write_tool_selection_document(access, &wide_open_preset).await?;

    let inference_profile_id = default_inference_profile_id_for_behavior(&default_behavior_id);
    let inference_profile = standard_inference_profile(&inference_profile_id);
    upsert_inference_profile(node, &inference_profile).await?;

    let behavior = AgentBehaviorDocument {
        behavior_id: default_behavior_id.clone(),
        agent_did: agent_did.to_string(),
        display_name: Some("Default".to_string()),
        description: None,
        summary: None,
        system_prompt: Some(standard_system_prompt(tool_package).to_string()),
        request_context_template: None,
        backend_id: Some(backend_id.clone()),
        model_name: Some(model_name.to_string()),
        tool_selection_id: Some(tool_selection_id.clone()),
        inference_profile_id: Some(inference_profile_id.clone()),
        compaction_strategy: None,
        compaction_threshold: None,
        enabled: true,
        skill_refs: Vec::new(),
        skill_excludes: Vec::new(),
        created_at: Some(chrono::Utc::now().to_rfc3339()),
    };
    write_agent_behavior_document(access, &behavior).await?;

    Ok(InitSummary {
        backend_id,
        backend_name,
        provider_kind: backend.provider_kind,
        endpoint: backend.endpoint,
        api_key: backend.api_key.map(|_| "<redacted>".to_string()),
        api_key_env_var: backend.api_key_env_var,
        model_name: model_name.to_string(),
        max_concurrent: args.max_concurrent,
        max_queue_depth: args.max_queue_depth,
        default_behavior_id,
        tool_selection_id,
        wide_open_preset_id,
        inference_profile_id,
        tool_package,
        tool_ceiling,
        tool_root: tool_root.map(|path| path.to_string_lossy().to_string()),
        enable_memory: args.enable_memory,
        enable_defra_query,
        defra_query_collections: args.defra_query_collections.clone(),
        created_principal: existing_principal.is_none(),
        created_default_behavior: existing_default_behavior.is_none(),
    })
}

// `pub(crate)` so the cross-crate init-parity drift guard
// (`crate::commands::config::behavior::tests`) can compare a persona-minted
// "write" `ToolSelectionDocument` against this authoritative source
// field-for-field; `gents::agent::persona_presets`' module doc documents the
// same contract from the other side.
pub(crate) fn tool_selection_for_package(
    agent_did: &str,
    tool_selection_id: &str,
    tool_package: ToolPackageArg,
    enable_memory: bool,
    enable_defra_query: bool,
    defra_query_collections: Vec<String>,
) -> ToolSelectionDocument {
    let profile = tool_package_profile(tool_package);
    ToolSelectionDocument {
        selection_id: tool_selection_id.to_string(),
        agent_did: agent_did.to_string(),
        display_name: Some(profile.display_name.to_string()),
        tool_policy_version: None,
        enable_file_tools: Some(profile.enable_file_tools),
        file_tools_mode: Some(profile.file_tools_mode.to_string()),
        file_tool_root: None,
        enable_bash: Some(profile.enable_bash),
        bash_mode: Some(profile.bash_mode.to_string()),
        command_execution_policy: default_command_execution_policy_for_init(tool_package),
        command_allowed_argv_prefixes: Some(Vec::new()),
        command_forbidden_argv_prefixes: Some(Vec::new()),
        read_only_command_allowlist: Some(Vec::new()),
        command_network_mode: None,
        cli_tool_names: Some(Vec::new()),
        enable_meta_tools: Some(profile.enable_meta_tools),
        allowed_mcp_service_ids: Some(Vec::new()),
        backgroundable_tool_names: Some(default_backgroundable_tool_names(tool_package)),
        approval_required_tools: None,
        subagent_targets: Some(Vec::new()),
        subagent_spawn_enabled: Some(false),
        orchestration_enabled: Some(false),
        subagent_steering_enabled: Some(false),
        subagent_background_enabled: Some(false),
        subagent_default_await_mode: Some("foreground".to_string()),
        subagent_allow_cross_deployment: Some(false),
        cross_deployment_spawn_timeout_seconds: None,
        enable_memory: Some(enable_memory),
        enable_session_history_tool: Some(false),
        enable_context_budget: Some(true),
        enable_defra_query: Some(enable_defra_query),
        defra_query_collections: Some(defra_query_collections),
        write_tools: None,
        enable_self_config: None,
        self_config_categories: None,
        self_config_no_lockout: None,
        self_config_dry_run: None,
    }
}

fn default_command_execution_policy_for_init(tool_package: ToolPackageArg) -> Option<String> {
    match tool_package {
        ToolPackageArg::Write if cfg!(target_os = "macos") => Some("workspace_write".to_string()),
        ToolPackageArg::Write | ToolPackageArg::Yolo => Some("unrestricted".to_string()),
        ToolPackageArg::Minimal | ToolPackageArg::Introspection | ToolPackageArg::Readonly => None,
    }
}

fn default_backgroundable_tool_names(tool_package: ToolPackageArg) -> Vec<String> {
    match tool_package {
        ToolPackageArg::Write | ToolPackageArg::Yolo => vec!["bash_unrestricted".to_string()],
        ToolPackageArg::Minimal | ToolPackageArg::Introspection | ToolPackageArg::Readonly => {
            Vec::new()
        }
    }
}

#[derive(Clone, Copy)]
struct ToolPackageProfile {
    display_name: &'static str,
    enable_file_tools: bool,
    file_tools_mode: &'static str,
    enable_bash: bool,
    bash_mode: &'static str,
    enable_meta_tools: bool,
    enable_defra_query: bool,
}

fn tool_package_profile(tool_package: ToolPackageArg) -> ToolPackageProfile {
    match tool_package {
        ToolPackageArg::Minimal => ToolPackageProfile {
            display_name: "Minimal Tools",
            enable_file_tools: false,
            file_tools_mode: "Off",
            enable_bash: false,
            bash_mode: "Off",
            enable_meta_tools: false,
            enable_defra_query: false,
        },
        ToolPackageArg::Introspection => ToolPackageProfile {
            display_name: "Introspection Tools",
            enable_file_tools: false,
            file_tools_mode: "Off",
            enable_bash: false,
            bash_mode: "Off",
            enable_meta_tools: true,
            enable_defra_query: true,
        },
        ToolPackageArg::Readonly => ToolPackageProfile {
            display_name: "Standard Read-Only Tools",
            enable_file_tools: true,
            file_tools_mode: "ReadOnly",
            enable_bash: true,
            bash_mode: "ReadOnly",
            enable_meta_tools: true,
            enable_defra_query: false,
        },
        ToolPackageArg::Write => ToolPackageProfile {
            display_name: "Standard Write Tools",
            enable_file_tools: true,
            file_tools_mode: "ReadWrite",
            enable_bash: true,
            bash_mode: "Unrestricted",
            enable_meta_tools: true,
            enable_defra_query: false,
        },
        ToolPackageArg::Yolo => ToolPackageProfile {
            display_name: "Unrestricted Write Tools (YOLO)",
            ..tool_package_profile(ToolPackageArg::Write)
        },
    }
}

fn resolve_init_tool_package(
    write_tools: bool,
    yolo: bool,
    explicit: Option<ToolPackageArg>,
) -> Result<ToolPackageArg> {
    if yolo {
        return match explicit {
            Some(ToolPackageArg::Yolo) | None => Ok(ToolPackageArg::Yolo),
            Some(other) => anyhow::bail!(
                "--yolo is shorthand for --tool-package yolo; remove --yolo or choose --tool-package {}",
                format_tool_package(other)
            ),
        };
    }
    match (write_tools, explicit) {
        (true, Some(ToolPackageArg::Write)) | (true, None) => Ok(ToolPackageArg::Write),
        (true, Some(other)) => anyhow::bail!(
            "--write is shorthand for --tool-package write; remove --write or choose --tool-package {}",
            format_tool_package(other)
        ),
        (false, Some(package)) => Ok(package),
        (false, None) => Ok(ToolPackageArg::Readonly),
    }
}

fn validate_init_tool_flags(args: &InitArgs, tool_package: ToolPackageArg) -> Result<()> {
    if args.identity_only
        && (args.enable_memory
            || args.enable_defra_query
            || args.disable_defra_query
            || !args.defra_query_collections.is_empty())
    {
        anyhow::bail!(
            "--enable-memory, --enable-defra-query, --disable-defra-query, and --defra-query-collection cannot be used with --identity-only because no ToolSelection document is written"
        );
    }
    if !args.defra_query_collections.is_empty() {
        let profile = tool_package_profile(tool_package);
        if args.disable_defra_query || !(profile.enable_defra_query || args.enable_defra_query) {
            anyhow::bail!(
                "--defra-query-collection requires defra_query to be enabled — pass --enable-defra-query or a tool package that enables it, and do not combine with --disable-defra-query"
            );
        }
    }
    Ok(())
}

fn init_enable_defra_query(
    tool_package: ToolPackageArg,
    enable_defra_query: bool,
    disable_defra_query: bool,
) -> bool {
    (tool_package_profile(tool_package).enable_defra_query || enable_defra_query)
        && !disable_defra_query
}

fn tool_ceiling_for_package(tool_package: ToolPackageArg) -> ToolCeilingArg {
    match tool_package {
        ToolPackageArg::Minimal | ToolPackageArg::Introspection => ToolCeilingArg::MetaOnly,
        ToolPackageArg::Readonly => ToolCeilingArg::Readonly,
        ToolPackageArg::Write | ToolPackageArg::Yolo => ToolCeilingArg::Readwrite,
    }
}

fn resolve_tool_root_for_package(
    tool_package: ToolPackageArg,
    explicit: Option<&Path>,
) -> Result<Option<PathBuf>> {
    let needs_root = matches!(
        tool_package,
        ToolPackageArg::Readonly | ToolPackageArg::Write | ToolPackageArg::Yolo
    );
    if needs_root || explicit.is_some() {
        Ok(Some(resolve_default_tool_root(explicit)?))
    } else {
        Ok(None)
    }
}

fn standard_inference_profile(profile_id: &str) -> InferenceProfile {
    InferenceProfile {
        profile_id: profile_id.to_string(),
        display_name: Some("Default".to_string()),
        context_window: Some(DEFAULT_CONTEXT_WINDOW as i64),
        max_output_tokens: Some(DEFAULT_MAX_OUTPUT_TOKENS as i64),
        max_turns: Some(DEFAULT_MAX_TURNS as i64),
        temperature: Some(0.0),
        stream_batch_ms: Some(DEFAULT_STREAM_BATCH_MS as i64),
        stream_liveness_timeout_secs: Some(DEFAULT_STREAM_LIVENESS_TIMEOUT_SECS as i64),
        deadline_duration_secs: Some(DEFAULT_DEADLINE_DURATION_SECS as i64),
        retry_max_transport: None,
        retry_backoff_ms: None,
        retry_max_resample: None,
        retry_allow_repair: None,
        retry_interactive_max: None,
        ..Default::default()
    }
}

fn default_backend_id_for_agent(agent_did: &str) -> String {
    format!("{agent_did}:backend")
}

fn standard_system_prompt(tool_package: ToolPackageArg) -> &'static str {
    match tool_package {
        ToolPackageArg::Write | ToolPackageArg::Yolo => STANDARD_READWRITE_SYSTEM_PROMPT,
        ToolPackageArg::Minimal | ToolPackageArg::Introspection | ToolPackageArg::Readonly => {
            STANDARD_READONLY_SYSTEM_PROMPT
        }
    }
}

fn init_next_steps(
    summary: &InitSummary,
    codex_logged_in: bool,
    grok_logged_in: bool,
) -> Vec<String> {
    let mut steps = Vec::new();
    if summary.provider_kind == gents::BackendProviderKind::ChatGptCodex && !codex_logged_in {
        steps.push("gents codex-login".to_string());
    }
    if summary.provider_kind == gents::BackendProviderKind::XaiGrokOAuth && !grok_logged_in {
        steps.push("gents grok-login".to_string());
    }
    if is_probably_ollama_endpoint(&summary.endpoint) {
        steps.push(format!("ollama pull {}", summary.model_name));
    } else if is_probably_llama_server_endpoint(&summary.endpoint) {
        steps.push(format!("llama-server -hf {}", summary.model_name));
    }
    steps.push("gents server".to_string());
    steps.push("gents codex".to_string());
    steps.push(format!(
        "gents config backend set --graphql http://127.0.0.1:{DEFAULT_HTTP_PORT}/api/v0/graphql --backend-id {} --name {} --endpoint <URL> --max-concurrent {}",
        summary.backend_id, summary.backend_name, summary.max_concurrent
    ));
    steps
}

fn is_probably_ollama_endpoint(endpoint: &str) -> bool {
    endpoint.contains("localhost:11434") || endpoint.contains("127.0.0.1:11434")
}

fn is_probably_llama_server_endpoint(endpoint: &str) -> bool {
    endpoint.contains("localhost:8080") || endpoint.contains("127.0.0.1:8080")
}

fn resolve_init_backend_config(args: &InitArgs) -> Result<ResolvedBackendConfig> {
    crate::resolve_backend_config_with_preset(
        args.backend_preset,
        args.resolved_inference_endpoint(),
        args.provider_kind.as_deref(),
        args.openai_wire_api,
        args.api_key.as_deref(),
        args.api_key_env_var.as_deref(),
        BackendResolutionMode::Init,
    )
}

fn resolve_init_model_name(args: &InitArgs) -> Result<&str> {
    if let Some(explicit) = args.model_name.as_deref() {
        let model_name = explicit.trim();
        if model_name.is_empty() {
            anyhow::bail!("--model-name must not be empty");
        }
        return Ok(model_name);
    }

    if let Some(default) = args.preset_default_model_name() {
        return Ok(default);
    }

    if let Some(preset) = args.backend_preset {
        anyhow::bail!(
            "--model-name is required for --backend-preset {} because that preset has no safe default model",
            preset.as_str()
        );
    }

    Ok(crate::DEFAULT_INIT_MODEL_NAME)
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

#[cfg(test)]
mod tests {
    use super::*;
    use gents::BackendProviderKind;

    fn init_summary(provider_kind: BackendProviderKind, endpoint: &str) -> InitSummary {
        InitSummary {
            backend_id: "did:key:z-init:backend".to_string(),
            backend_name: "test-agent backend".to_string(),
            provider_kind,
            endpoint: endpoint.to_string(),
            api_key: None,
            api_key_env_var: None,
            model_name: "test-model".to_string(),
            max_concurrent: 2,
            max_queue_depth: 16,
            default_behavior_id: "default".to_string(),
            tool_selection_id: "default-tools".to_string(),
            wide_open_preset_id: "wide-open".to_string(),
            inference_profile_id: "default-profile".to_string(),
            tool_package: ToolPackageArg::Readonly,
            tool_ceiling: ToolCeilingArg::Readonly,
            tool_root: None,
            enable_memory: false,
            enable_defra_query: false,
            defra_query_collections: Vec::new(),
            created_principal: true,
            created_default_behavior: true,
        }
    }

    #[test]
    fn chatgpt_codex_next_steps_lead_with_login() {
        let steps = init_next_steps(
            &init_summary(
                BackendProviderKind::ChatGptCodex,
                "https://chatgpt.com/backend-api/codex",
            ),
            false,
            false,
        );
        assert_eq!(steps.first().map(String::as_str), Some("gents codex-login"));
    }

    #[test]
    fn chatgpt_codex_next_steps_drop_login_after_inline_login() {
        let steps = init_next_steps(
            &init_summary(
                BackendProviderKind::ChatGptCodex,
                "https://chatgpt.com/backend-api/codex",
            ),
            true,
            false,
        );
        assert!(!steps.iter().any(|step| step == "gents codex-login"));
    }

    #[test]
    fn existing_codex_credential_drops_login_step_without_new_outcome() {
        let state = InlineCodexLoginState::ExistingCredential;
        assert!(state.is_authenticated());
        assert!(state.outcome().is_none());

        let steps = init_next_steps(
            &init_summary(
                BackendProviderKind::ChatGptCodex,
                "https://chatgpt.com/backend-api/codex",
            ),
            state.is_authenticated(),
            false,
        );
        assert!(!steps.iter().any(|step| step == "gents codex-login"));
    }

    #[test]
    fn non_codex_next_steps_omit_login() {
        let steps = init_next_steps(
            &init_summary(
                BackendProviderKind::OpenAiCompatible,
                "http://127.0.0.1:8080/v1",
            ),
            false,
            false,
        );
        assert!(!steps.iter().any(|step| step == "gents codex-login"));
        assert!(!steps.iter().any(|step| step == "gents grok-login"));
    }

    #[test]
    fn grok_oauth_next_steps_lead_with_login() {
        let steps = init_next_steps(
            &init_summary(
                BackendProviderKind::XaiGrokOAuth,
                "https://cli-chat-proxy.grok.com/v1",
            ),
            false,
            false,
        );
        assert_eq!(steps.first().map(String::as_str), Some("gents grok-login"));
    }

    fn init_args() -> InitArgs {
        InitArgs {
            home: None,
            data_dir: None,
            dangerously_overwrite: false,
            reset: false,
            identity_only: false,
            agent_name: "test-agent".to_string(),
            key_path: None,
            identity_backend: IdentityBackendArg::File,
            keychain_label: None,
            secure_enclave_label: None,
            inference_endpoint: None,
            inference_endpoint_legacy: None,
            backend_id: None,
            backend_name: None,
            backend_preset: None,
            provider_kind: None,
            openai_wire_api: None,
            api_key: None,
            api_key_env_var: None,
            model_name: Some("test-model".to_string()),
            max_concurrent: 2,
            max_queue_depth: 16,
            write_tools: false,
            yolo: false,
            tool_package: None,
            tool_root: None,
            enable_memory: false,
            disable_defra_query: false,
            enable_defra_query: false,
            defra_query_collections: Vec::new(),
        }
    }

    #[test]
    fn init_affordances_seed_tool_selection_document() {
        let selection = tool_selection_for_package(
            "did:key:z-init",
            "default-tools",
            ToolPackageArg::Readonly,
            true,
            true,
            vec!["AgentRequest".to_string(), "AgentResponse".to_string()],
        );

        assert_eq!(selection.enable_memory, Some(true));
        assert_eq!(selection.enable_defra_query, Some(true));
        assert_eq!(
            selection.defra_query_collections,
            Some(vec![
                "AgentRequest".to_string(),
                "AgentResponse".to_string()
            ])
        );
        assert_eq!(selection.enable_file_tools, Some(true));
        assert_eq!(selection.file_tools_mode.as_deref(), Some("ReadOnly"));
    }

    /// Drift fence between init's tool packages and the directory persona
    /// catalog's preset templates (`gents::agent::persona_presets`): the
    /// templates are copied verbatim from `tool_package_profile`, and
    /// nothing else ties the two together. Classify the exact
    /// `ToolSelectionDocument` init mints — projected into `PresetFields`
    /// with the same None-means-default reads the directory projection
    /// applies to stored rows — so a change to either side fails here
    /// instead of silently mislabeling directory rows.
    #[test]
    fn init_minted_selections_classify_as_their_persona_preset() {
        use gents::agent::persona_presets::{preset_name, PresetFields};

        fn classify(package: ToolPackageArg) -> Option<&'static str> {
            let doc = tool_selection_for_package(
                "did:key:z-init",
                "default-tools",
                package,
                false,
                false,
                Vec::new(),
            );
            preset_name(&PresetFields {
                enable_file_tools: doc.enable_file_tools.unwrap_or_default(),
                file_tools_mode: doc.file_tools_mode.unwrap_or_default(),
                enable_bash: doc.enable_bash.unwrap_or_default(),
                bash_mode: doc.bash_mode.unwrap_or_default(),
                command_allowed_argv_prefixes: doc
                    .command_allowed_argv_prefixes
                    .unwrap_or_default(),
                command_forbidden_argv_prefixes: doc
                    .command_forbidden_argv_prefixes
                    .unwrap_or_default(),
                read_only_command_allowlist: doc.read_only_command_allowlist.unwrap_or_default(),
                enable_self_config: doc.enable_self_config.unwrap_or_default(),
                // The `[String]` column stores each decl JSON-encoded.
                write_tools: doc
                    .write_tools
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .map(|decl| serde_json::to_string(decl).expect("encode write tool decl"))
                    .collect(),
            })
        }

        assert_eq!(classify(ToolPackageArg::Readonly), Some("readonly"));
        assert_eq!(classify(ToolPackageArg::Write), Some("write"));
        // Yolo differs from Write only in fields the classifier deliberately
        // excludes (display name, exec policy), so it badges as "write".
        assert_eq!(classify(ToolPackageArg::Yolo), Some("write"));
        assert_eq!(classify(ToolPackageArg::Minimal), None);
        assert_eq!(classify(ToolPackageArg::Introspection), None);
    }

    #[test]
    fn init_tool_packages_seed_expected_tool_selection_documents() {
        struct Case {
            package: ToolPackageArg,
            ceiling: ToolCeilingArg,
            display_name: &'static str,
            enable_file_tools: bool,
            file_tools_mode: &'static str,
            enable_bash: bool,
            bash_mode: &'static str,
            enable_meta_tools: bool,
            enable_defra_query: bool,
            backgroundable_tools: Vec<String>,
        }

        let cases = [
            Case {
                package: ToolPackageArg::Minimal,
                ceiling: ToolCeilingArg::MetaOnly,
                display_name: "Minimal Tools",
                enable_file_tools: false,
                file_tools_mode: "Off",
                enable_bash: false,
                bash_mode: "Off",
                enable_meta_tools: false,
                enable_defra_query: false,
                backgroundable_tools: Vec::new(),
            },
            Case {
                package: ToolPackageArg::Introspection,
                ceiling: ToolCeilingArg::MetaOnly,
                display_name: "Introspection Tools",
                enable_file_tools: false,
                file_tools_mode: "Off",
                enable_bash: false,
                bash_mode: "Off",
                enable_meta_tools: true,
                enable_defra_query: true,
                backgroundable_tools: Vec::new(),
            },
            Case {
                package: ToolPackageArg::Readonly,
                ceiling: ToolCeilingArg::Readonly,
                display_name: "Standard Read-Only Tools",
                enable_file_tools: true,
                file_tools_mode: "ReadOnly",
                enable_bash: true,
                bash_mode: "ReadOnly",
                enable_meta_tools: true,
                enable_defra_query: false,
                backgroundable_tools: Vec::new(),
            },
            Case {
                package: ToolPackageArg::Write,
                ceiling: ToolCeilingArg::Readwrite,
                display_name: "Standard Write Tools",
                enable_file_tools: true,
                file_tools_mode: "ReadWrite",
                enable_bash: true,
                bash_mode: "Unrestricted",
                enable_meta_tools: true,
                enable_defra_query: false,
                backgroundable_tools: vec!["bash_unrestricted".to_string()],
            },
        ];

        for case in cases {
            let selection = tool_selection_for_package(
                "did:key:z-init",
                "default-tools",
                case.package,
                false,
                init_enable_defra_query(case.package, false, false),
                Vec::new(),
            );

            assert_eq!(tool_ceiling_for_package(case.package), case.ceiling);
            assert_eq!(selection.display_name.as_deref(), Some(case.display_name));
            assert_eq!(selection.enable_file_tools, Some(case.enable_file_tools));
            assert_eq!(
                selection.file_tools_mode.as_deref(),
                Some(case.file_tools_mode)
            );
            assert_eq!(selection.enable_bash, Some(case.enable_bash));
            assert_eq!(selection.bash_mode.as_deref(), Some(case.bash_mode));
            assert_eq!(selection.enable_meta_tools, Some(case.enable_meta_tools));
            assert_eq!(selection.enable_defra_query, Some(case.enable_defra_query));
            assert_eq!(
                selection.backgroundable_tool_names,
                Some(case.backgroundable_tools)
            );
            assert_eq!(selection.enable_memory, Some(false));
            assert_eq!(selection.allowed_mcp_service_ids, Some(Vec::new()));
            assert_eq!(selection.subagent_targets, Some(Vec::new()));
            assert_eq!(selection.subagent_spawn_enabled, Some(false));
            assert_eq!(selection.orchestration_enabled, Some(false));
            assert_eq!(selection.subagent_background_enabled, Some(false));
            assert_eq!(selection.subagent_allow_cross_deployment, Some(false));
        }
    }

    #[test]
    fn init_can_seed_defra_query_disabled_document() {
        let selection = tool_selection_for_package(
            "did:key:z-init",
            "default-tools",
            ToolPackageArg::Write,
            false,
            init_enable_defra_query(ToolPackageArg::Write, false, true),
            Vec::new(),
        );

        assert_eq!(selection.enable_memory, Some(false));
        assert_eq!(selection.enable_defra_query, Some(false));
        assert_eq!(selection.defra_query_collections, Some(Vec::new()));
        assert_eq!(
            selection.backgroundable_tool_names,
            Some(vec!["bash_unrestricted".to_string()])
        );
    }

    #[test]
    fn init_tool_package_aliases_and_defra_query_scope_validation_match_seeded_docs() {
        assert_eq!(
            resolve_init_tool_package(false, false, None).unwrap(),
            ToolPackageArg::Readonly
        );
        assert_eq!(
            resolve_init_tool_package(true, false, None).unwrap(),
            ToolPackageArg::Write
        );
        assert_eq!(
            resolve_init_tool_package(true, false, Some(ToolPackageArg::Write)).unwrap(),
            ToolPackageArg::Write
        );
        assert!(
            resolve_init_tool_package(true, false, Some(ToolPackageArg::Readonly))
                .unwrap_err()
                .to_string()
                .contains("--write")
        );
        assert_eq!(
            resolve_init_tool_package(false, true, None).unwrap(),
            ToolPackageArg::Yolo
        );
        assert_eq!(
            resolve_init_tool_package(false, true, Some(ToolPackageArg::Yolo)).unwrap(),
            ToolPackageArg::Yolo
        );
        assert!(
            resolve_init_tool_package(false, true, Some(ToolPackageArg::Readonly))
                .unwrap_err()
                .to_string()
                .contains("--yolo")
        );

        let mut scoped_introspection = init_args();
        scoped_introspection.tool_package = Some(ToolPackageArg::Introspection);
        scoped_introspection.defra_query_collections = vec!["AgentRequest".to_string()];
        validate_init_tool_flags(&scoped_introspection, ToolPackageArg::Introspection).unwrap();

        let selection = tool_selection_for_package(
            "did:key:z-init",
            "default-tools",
            ToolPackageArg::Introspection,
            true,
            init_enable_defra_query(ToolPackageArg::Introspection, false, false),
            scoped_introspection.defra_query_collections.clone(),
        );
        assert_eq!(selection.enable_memory, Some(true));
        assert_eq!(selection.enable_defra_query, Some(true));
        assert_eq!(
            selection.defra_query_collections,
            Some(vec!["AgentRequest".to_string()])
        );
    }

    #[test]
    fn yolo_package_seeds_unrestricted_write_documents() {
        let selection = tool_selection_for_package(
            "did:key:z-init",
            "default-tools",
            ToolPackageArg::Yolo,
            false,
            init_enable_defra_query(ToolPackageArg::Yolo, false, false),
            Vec::new(),
        );
        assert_eq!(selection.enable_file_tools, Some(true));
        assert_eq!(selection.file_tools_mode.as_deref(), Some("ReadWrite"));
        assert_eq!(selection.enable_bash, Some(true));
        assert_eq!(selection.bash_mode.as_deref(), Some("Unrestricted"));
        assert_eq!(
            selection.command_execution_policy.as_deref(),
            Some("unrestricted")
        );
        assert_eq!(
            selection.backgroundable_tool_names,
            Some(vec!["bash_unrestricted".to_string()])
        );

        assert_eq!(
            default_command_execution_policy_for_init(ToolPackageArg::Write).as_deref(),
            if cfg!(target_os = "macos") {
                Some("workspace_write")
            } else {
                Some("unrestricted")
            }
        );
        assert_eq!(
            standard_system_prompt(ToolPackageArg::Yolo),
            STANDARD_READWRITE_SYSTEM_PROMPT
        );
        assert_eq!(
            tool_ceiling_for_package(ToolPackageArg::Yolo),
            ToolCeilingArg::Readwrite
        );
    }

    #[test]
    fn init_rejects_affordances_that_would_not_write_documents() {
        let mut identity_only = init_args();
        identity_only.identity_only = true;
        identity_only.enable_memory = true;
        assert!(
            validate_init_tool_flags(&identity_only, ToolPackageArg::Readonly)
                .unwrap_err()
                .to_string()
                .contains("--identity-only")
        );

        let mut identity_only_query = init_args();
        identity_only_query.identity_only = true;
        identity_only_query.enable_defra_query = true;
        assert!(
            validate_init_tool_flags(&identity_only_query, ToolPackageArg::Readonly)
                .unwrap_err()
                .to_string()
                .contains("--identity-only")
        );

        let mut scoped_without_tool = init_args();
        scoped_without_tool.defra_query_collections = vec!["AgentRequest".to_string()];
        assert!(
            validate_init_tool_flags(&scoped_without_tool, ToolPackageArg::Minimal)
                .unwrap_err()
                .to_string()
                .contains("--defra-query-collection")
        );

        let mut scoped_disabled = init_args();
        scoped_disabled.disable_defra_query = true;
        scoped_disabled.defra_query_collections = vec!["AgentRequest".to_string()];
        assert!(
            validate_init_tool_flags(&scoped_disabled, ToolPackageArg::Readonly)
                .unwrap_err()
                .to_string()
                .contains("--defra-query-collection")
        );
    }
}
