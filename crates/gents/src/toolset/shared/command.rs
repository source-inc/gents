use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;

use super::context::{ToolContext, ToolError};
use crate::managed_exec::{run_managed_exec, ManagedExecOutcome, ManagedExecRequest};
use crate::tool_call_lifecycle::runtime::current_tool_runtime_context;
use crate::tool_call_lifecycle::FailureClass;
use crate::toolset::{CommandPolicyDenial, DenialReason};
use crate::truncation::{truncate, TruncationLimits, TruncationMode};

const OUTPUT_META_PREFIX: &str = "gents_exec: ";
const FALLBACK_PATH: &str = "/usr/bin:/bin:/usr/sbin:/sbin";
#[cfg(target_os = "macos")]
const SANDBOX_EXEC: &str = "/usr/bin/sandbox-exec";
const CORE_ENV_VARS: &[&str] = &[
    "PATH",
    "SHELL",
    "TMPDIR",
    "TEMP",
    "TMP",
    "HOME",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "LOGNAME",
    "USER",
    "RUSTUP_HOME",
    "CARGO_HOME",
    "RUSTUP_TOOLCHAIN",
    "SDKROOT",
    "DEVELOPER_DIR",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandExecutionMode {
    ReadOnly,
    WorkspaceWrite,
    Unrestricted,
}

impl CommandExecutionMode {
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim() {
            "" | "read_only" | "ReadOnly" => Ok(Self::ReadOnly),
            "workspace_write" | "WorkspaceWrite" | "managed_write" | "ManagedWrite" => {
                Ok(Self::WorkspaceWrite)
            }
            "unrestricted" | "Unrestricted" => Ok(Self::Unrestricted),
            other => bail!("unknown command execution policy mode {other}"),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::WorkspaceWrite => "workspace_write",
            Self::Unrestricted => "unrestricted",
        }
    }

    /// More restrictive mode wins: ReadOnly < WorkspaceWrite < Unrestricted.
    pub fn meet(self, other: Self) -> Self {
        if self.rank() <= other.rank() {
            self
        } else {
            other
        }
    }

    fn rank(self) -> u8 {
        match self {
            Self::ReadOnly => 0,
            Self::WorkspaceWrite => 1,
            Self::Unrestricted => 2,
        }
    }
}

/// Request `workspace_authority`. ReadWrite meets command mode to WorkspaceWrite,
/// never Unrestricted. Integrate is inspect-only (no bash writes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceAuthority {
    ReadOnly,
    ReadWrite,
    Integrate,
}

impl WorkspaceAuthority {
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim() {
            "readOnly" | "ReadOnly" | "read_only" => Ok(Self::ReadOnly),
            "readWrite" | "ReadWrite" | "read_write" => Ok(Self::ReadWrite),
            "integrate" | "Integrate" => Ok(Self::Integrate),
            other => bail!("unknown workspace authority {other}"),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "readOnly",
            Self::ReadWrite => "readWrite",
            Self::Integrate => "integrate",
        }
    }

    pub fn command_mode(self) -> CommandExecutionMode {
        match self {
            Self::ReadOnly | Self::Integrate => CommandExecutionMode::ReadOnly,
            Self::ReadWrite => CommandExecutionMode::WorkspaceWrite,
        }
    }

    pub fn allows_file_writes(self) -> bool {
        matches!(self, Self::ReadWrite)
    }

    pub fn bindable_lifecycle_state(self, state: &str) -> bool {
        match (self, normalize_workspace_lifecycle_state(state)) {
            (Self::ReadWrite, Some("ready")) => true,
            (Self::ReadOnly, Some("ready" | "sealed")) => true,
            (Self::Integrate, Some("sealed")) => true,
            _ => false,
        }
    }
}

pub(crate) fn normalize_workspace_lifecycle_state(value: &str) -> Option<&'static str> {
    match value.trim() {
        "provisioning" | "Provisioning" => Some("provisioning"),
        "ready" | "Ready" => Some("ready"),
        "provisionFailed" | "provision_failed" | "ProvisionFailed" => Some("provisionFailed"),
        "sealed" | "Sealed" => Some("sealed"),
        "cleaning" | "Cleaning" => Some("cleaning"),
        "cleaned" | "Cleaned" => Some("cleaned"),
        _ => None,
    }
}

/// Meet the baked command policy with the request's workspace authority.
pub(crate) fn apply_workspace_authority(
    policy: &CommandExecutionPolicy,
    authority: WorkspaceAuthority,
) -> CommandExecutionPolicy {
    let mut met = policy.clone();
    met.mode = policy.mode.meet(authority.command_mode());
    if matches!(met.mode, CommandExecutionMode::ReadOnly) && met.read_only_allowlist().is_empty() {
        met = met.with_read_only_allowlist(
            crate::toolset::default_read_only_command_policy()
                .read_only_allowlist()
                .to_vec(),
        );
    }
    met
}

pub(crate) fn effective_command_policy(policy: &CommandExecutionPolicy) -> CommandExecutionPolicy {
    match crate::tool_call_lifecycle::runtime::current_tool_runtime_context()
        .and_then(|scope| scope.workspace_authority)
    {
        Some(authority) => apply_workspace_authority(policy, authority),
        None => policy.clone(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandNetworkMode {
    Inherit,
    Disabled,
    Enabled,
}

impl CommandNetworkMode {
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim() {
            "" | "inherit" | "Inherit" => Ok(Self::Inherit),
            "disabled" | "Disabled" | "off" | "Off" => Ok(Self::Disabled),
            "enabled" | "Enabled" | "on" | "On" => Ok(Self::Enabled),
            other => bail!("unknown command network mode {other}"),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inherit => "inherit",
            Self::Disabled => "disabled",
            Self::Enabled => "enabled",
        }
    }

    fn allows_network(self) -> bool {
        !matches!(self, Self::Disabled)
    }

    /// More restrictive mode wins: Disabled < Inherit < Enabled.
    pub fn meet(self, other: Self) -> Self {
        if self.rank() <= other.rank() {
            self
        } else {
            other
        }
    }

    fn rank(self) -> u8 {
        match self {
            Self::Disabled => 0,
            Self::Inherit => 1,
            Self::Enabled => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandExecutionPolicy {
    pub mode: CommandExecutionMode,
    pub allowed_argv_prefixes: Vec<Vec<String>>,
    pub forbidden_argv_prefixes: Vec<Vec<String>>,
    pub network_mode: CommandNetworkMode,
    read_only_allowlist: Vec<String>,
    deny_all_argv: bool,
}

impl CommandExecutionPolicy {
    pub fn read_only(allowlist: Vec<String>) -> Self {
        Self {
            mode: CommandExecutionMode::ReadOnly,
            allowed_argv_prefixes: Vec::new(),
            forbidden_argv_prefixes: Vec::new(),
            network_mode: CommandNetworkMode::Inherit,
            read_only_allowlist: allowlist,
            deny_all_argv: false,
        }
    }

    pub fn write_capable() -> Self {
        Self {
            mode: if cfg!(target_os = "macos") {
                CommandExecutionMode::WorkspaceWrite
            } else {
                CommandExecutionMode::Unrestricted
            },
            allowed_argv_prefixes: Vec::new(),
            forbidden_argv_prefixes: Vec::new(),
            network_mode: CommandNetworkMode::Inherit,
            read_only_allowlist: Vec::new(),
            deny_all_argv: false,
        }
    }

    pub fn with_mode(mut self, mode: CommandExecutionMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn with_allowed_argv_prefixes(mut self, prefixes: Vec<Vec<String>>) -> Self {
        self.allowed_argv_prefixes = prefixes;
        self
    }

    pub fn with_forbidden_argv_prefixes(mut self, prefixes: Vec<Vec<String>>) -> Self {
        self.forbidden_argv_prefixes = prefixes;
        self
    }

    pub fn read_only_allowlist(&self) -> &[String] {
        &self.read_only_allowlist
    }

    pub fn with_read_only_allowlist(mut self, allowlist: Vec<String>) -> Self {
        self.read_only_allowlist = allowlist;
        self
    }

    pub fn with_deny_all_argv(mut self, deny_all_argv: bool) -> Self {
        self.deny_all_argv = deny_all_argv;
        self
    }

    pub fn deny_all_argv(&self) -> bool {
        self.deny_all_argv
    }

    pub fn with_network_mode(mut self, network_mode: CommandNetworkMode) -> Self {
        self.network_mode = network_mode;
        self
    }
}

/// Bash-independent spawn constraints projected from the effective policy meet.
/// Ignores `bash.tool` and the read-only command allowlist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandConstraints {
    pub allowed_argv_prefixes: Vec<Vec<String>>,
    pub forbidden_argv_prefixes: Vec<Vec<String>>,
    pub network_mode: CommandNetworkMode,
    pub execution_mode: CommandExecutionMode,
    /// Seatbelt vs none. Never `ReadOnly`. Hashed into the LSP config digest.
    pub sandbox: CommandExecutionMode,
    pub deny_all_argv: bool,
}

impl CommandConstraints {
    pub fn to_spawn_policy(&self) -> CommandExecutionPolicy {
        CommandExecutionPolicy {
            mode: self.sandbox,
            allowed_argv_prefixes: self.allowed_argv_prefixes.clone(),
            forbidden_argv_prefixes: self.forbidden_argv_prefixes.clone(),
            network_mode: self.network_mode,
            read_only_allowlist: Vec::new(),
            deny_all_argv: self.deny_all_argv,
        }
    }
}

/// Platform default for an omitted `lsp_config.network_mode`.
pub fn default_lsp_network_mode() -> CommandNetworkMode {
    if workspace_write_sandbox_enforced() {
        CommandNetworkMode::Disabled
    } else {
        CommandNetworkMode::Inherit
    }
}

/// Seatbelt vs none for an LSP spawn. `ReadOnly` (bash Off) uses the platform
/// default: macOS `workspace_write`, elsewhere `Unrestricted`.
pub fn lsp_sandbox_for_effective(execution_mode: CommandExecutionMode) -> CommandExecutionMode {
    match execution_mode {
        CommandExecutionMode::WorkspaceWrite => CommandExecutionMode::WorkspaceWrite,
        CommandExecutionMode::Unrestricted => CommandExecutionMode::Unrestricted,
        CommandExecutionMode::ReadOnly => {
            if workspace_write_sandbox_enforced() {
                CommandExecutionMode::WorkspaceWrite
            } else {
                CommandExecutionMode::Unrestricted
            }
        }
    }
}

/// PATH lookup + canonicalize. Never admits a path under `tool_root`.
pub(crate) fn admit_host_executable(
    command: &str,
    tool_root: &Path,
) -> std::result::Result<PathBuf, crate::toolset::denial::DenialReason> {
    use crate::toolset::denial::DenialReason;
    let trimmed = command.trim();
    if trimmed.is_empty() {
        return Err(DenialReason::WorkspaceExecutable);
    }
    let candidate =
        if trimmed.contains('/') || trimmed.contains('\\') || Path::new(trimmed).is_absolute() {
            PathBuf::from(trimmed)
        } else {
            which_on_host_path(trimmed).ok_or(DenialReason::WorkspaceExecutable)?
        };
    let canonical = std::fs::canonicalize(&candidate).unwrap_or(candidate);
    let root = std::fs::canonicalize(tool_root).unwrap_or_else(|_| tool_root.to_path_buf());
    if canonical.starts_with(&root) {
        return Err(DenialReason::WorkspaceExecutable);
    }
    if !canonical.is_file() {
        return Err(DenialReason::WorkspaceExecutable);
    }
    resolve_rustup_proxy_path(trimmed, canonical, &root).ok_or(DenialReason::WorkspaceExecutable)
}

/// rustup shims in `~/.cargo/bin` canonicalize to the `rustup` binary. An LSP
/// workspace tempdir has no rust-toolchain.toml, so that shim exits before
/// initialize. Resolve rust-analyzer from rustup's on-disk selection state so
/// admission remains side-effect free and the spawned/digested path is the
/// actual host toolchain binary, never a PATH lookup or helper subprocess.
fn resolve_rustup_proxy_path(
    requested: &str,
    admitted: PathBuf,
    tool_root: &Path,
) -> Option<PathBuf> {
    let Some(name) = Path::new(requested)
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.trim_end_matches(".exe"))
    else {
        return Some(admitted);
    };
    if name != "rust-analyzer"
        || admitted
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.trim_end_matches(".exe"))
            != Some("rustup")
    {
        return Some(admitted);
    }

    let rustup_home = std::env::var_os("RUSTUP_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".rustup")))?;
    let settings = read_toml(&rustup_home.join("settings.toml"));
    let channel = std::env::var("RUSTUP_TOOLCHAIN")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| rustup_override_channel(settings.as_ref(), tool_root))
        .or_else(|| rust_toolchain_file_channel(tool_root))
        .or_else(|| {
            settings
                .as_ref()?
                .get("default_toolchain")?
                .as_str()
                .map(ToOwned::to_owned)
        })?;
    let toolchains = rustup_home.join("toolchains");
    let exact = toolchains.join(&channel);
    let toolchain = if exact.is_dir() {
        exact
    } else {
        let mut matches = std::fs::read_dir(&toolchains)
            .ok()?
            .flatten()
            .filter(|entry| entry.path().is_dir())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|value| value.starts_with(&format!("{channel}-")))
            })
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        matches.sort();
        matches.pop()?
    };
    let executable = toolchain.join("bin").join(if cfg!(windows) {
        "rust-analyzer.exe"
    } else {
        name
    });
    let canonical = std::fs::canonicalize(executable).ok()?;
    (!canonical.starts_with(tool_root) && canonical.is_file()).then_some(canonical)
}

fn read_toml(path: &Path) -> Option<toml::Value> {
    std::fs::read_to_string(path).ok()?.parse().ok()
}

fn rustup_override_channel(settings: Option<&toml::Value>, tool_root: &Path) -> Option<String> {
    let overrides = settings?.get("overrides")?.as_table()?;
    tool_root.ancestors().find_map(|directory| {
        let key = directory.to_string_lossy();
        overrides
            .get(key.as_ref())
            .and_then(toml::Value::as_str)
            .map(ToOwned::to_owned)
    })
}

fn rust_toolchain_file_channel(tool_root: &Path) -> Option<String> {
    for directory in tool_root.ancestors() {
        let toml_path = directory.join("rust-toolchain.toml");
        if toml_path.is_file() {
            return read_toml(&toml_path)?
                .get("toolchain")?
                .get("channel")?
                .as_str()
                .map(ToOwned::to_owned);
        }
        let legacy = directory.join("rust-toolchain");
        if legacy.is_file() {
            let raw = std::fs::read_to_string(&legacy).ok()?;
            if let Ok(parsed) = raw.parse::<toml::Value>() {
                return parsed
                    .get("toolchain")?
                    .get("channel")?
                    .as_str()
                    .map(ToOwned::to_owned);
            }
            let channel = raw.lines().next()?.trim();
            return (!channel.is_empty()).then(|| channel.to_string());
        }
    }
    None
}

fn which_on_host_path(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// Shared PATH / prefix / network / sandbox preparation for bash and LSP.
pub(crate) fn prepare_managed_command(
    root: &Path,
    command: &str,
    args: &[String],
    constraints: &CommandConstraints,
) -> std::result::Result<(PathBuf, Vec<String>, HashMap<String, String>, &'static str), ToolError> {
    let admitted = admit_host_executable(command, root)
        .map_err(|reason| policy_denial(&constraints.to_spawn_policy(), reason))?;
    let spawn_policy = constraints.to_spawn_policy();
    validate_command_policy_with_resolved_executable(
        command,
        args,
        &admitted.to_string_lossy(),
        &spawn_policy,
    )?;
    let (program, argv, sandbox) =
        sandboxed_command_for_policy(root, &admitted.to_string_lossy(), args, &spawn_policy)?;
    Ok((PathBuf::from(program), argv, build_shell_env(), sandbox))
}

pub(crate) async fn run_command(
    context: &ToolContext,
    tool_name: &'static str,
    command_name: &str,
    args: &[String],
    cwd: Option<&str>,
    timeout: Duration,
    policy: &CommandExecutionPolicy,
    raw_json: bool,
) -> std::result::Result<String, ToolError> {
    let policy = effective_command_policy(policy);
    let cwd = context.resolve_existing_dir(cwd)?;
    let argv = std::iter::once(command_name.to_string())
        .chain(args.iter().cloned())
        .collect::<Vec<_>>();
    let command_line = shell_join(&argv);
    let root = context.root();
    let (program, command_args, sandbox) =
        sandboxed_command_for_policy(&root, command_name, args, &policy)?;
    let runtime = current_tool_runtime_context();
    let request_deadline = runtime.as_ref().and_then(|runtime| runtime.deadline_at);
    let command_deadline = chrono::Utc::now()
        + chrono::Duration::from_std(timeout).unwrap_or_else(|_| chrono::Duration::days(36_500));
    let deadline_at =
        Some(request_deadline.map_or(command_deadline, |deadline| deadline.min(command_deadline)));
    let cancellation_token = runtime
        .as_ref()
        .map(|runtime| runtime.cancellation_token.clone())
        .unwrap_or_default();
    let live_output = runtime.and_then(|runtime| runtime.live_output);
    let started = Instant::now();
    let outcome = run_managed_exec(ManagedExecRequest {
        argv: std::iter::once(program)
            .chain(command_args)
            .collect::<Vec<_>>(),
        cwd: cwd.clone(),
        deadline_at,
        cancellation_token,
        max_output_bytes: usize::MAX,
        stdin: Vec::new(),
        environment: Some(build_shell_env()),
        tool_name: Some(tool_name.to_string()),
        live_output,
    })
    .await;
    let duration_ms = elapsed_ms(started);
    let (
        exit_code,
        timed_out,
        stdout_bytes,
        stderr_bytes,
        stdout_capture_incomplete,
        stderr_capture_incomplete,
    ) = match outcome {
        ManagedExecOutcome::Exited {
            code,
            stdout,
            stderr,
            stdout_truncated,
            stderr_truncated,
        } => (
            code,
            false,
            stdout,
            stderr,
            stdout_truncated,
            stderr_truncated,
        ),
        ManagedExecOutcome::TimedOut {
            stdout,
            stderr,
            stdout_truncated,
            stderr_truncated,
            ..
        } => {
            if request_deadline.is_some_and(|deadline| chrono::Utc::now() >= deadline) {
                // Never model-facing: the dispatcher's envelope shares this
                // deadline and its `biased` select resolves the call to
                // `ToolOutcome::TimedOut` before this text can thread.
                return Ok("command was stopped at the request deadline".to_string());
            }
            (
                None,
                true,
                stdout,
                stderr,
                stdout_truncated,
                stderr_truncated,
            )
        }
        // Never model-facing: the envelope's (fired) cancellation branch is
        // polled first and resolves the call to `ToolOutcome::Cancelled`.
        ManagedExecOutcome::Cancelled { .. } => return Ok("command was cancelled".to_string()),
        ManagedExecOutcome::SpawnFailed { error } => {
            return Err(anyhow!("spawning managed command failed: {error}").into())
        }
    };
    let stdout_raw = String::from_utf8_lossy(&stdout_bytes).into_owned();
    let stderr_raw = String::from_utf8_lossy(&stderr_bytes).into_owned();

    let stdout = truncate_stream(&stdout_raw, super::super::DEFAULT_MAX_COMMAND_CHARS);
    let stderr = truncate_stream(&stderr_raw, super::super::DEFAULT_MAX_COMMAND_CHARS);
    let status = if timed_out {
        "timeout"
    } else if exit_code == Some(0) {
        "success"
    } else {
        "exit_nonzero"
    };
    let metadata = CommandMetadata {
        ok: !timed_out && exit_code == Some(0),
        status,
        command: command_line,
        argv,
        cwd: context.display_path(&cwd),
        exit_code,
        timed_out,
        duration_ms,
        timeout_ms: millis(timeout),
        execution_mode: policy.mode,
        network_mode: policy.network_mode,
        sandbox,
        stdout_capture_incomplete,
        stderr_capture_incomplete,
        stdout_truncation: stdout.metadata,
        stderr_truncation: stderr.metadata,
    };
    let output = CommandOutput {
        metadata,
        stdout: stdout.content,
        stderr: stderr.content,
    };
    let rendered = render_command_output(&output, raw_json).map_err(ToolError::from)?;
    if output.metadata.ok {
        Ok(rendered)
    } else {
        let class = if output.metadata.timed_out {
            FailureClass::External
        } else {
            FailureClass::ToolReturnedError
        };
        Err(ToolError::reported_failure(class, rendered))
    }
}

#[cfg(test)]
pub(crate) fn validate_read_only_command(
    command: &str,
    args: &[String],
    allowlist: &[String],
) -> std::result::Result<(), ToolError> {
    let policy = CommandExecutionPolicy::read_only(allowlist.to_vec());
    validate_command_policy(command, args, &policy)
}

pub(crate) fn validate_command_policy(
    command: &str,
    args: &[String],
    policy: &CommandExecutionPolicy,
) -> std::result::Result<(), ToolError> {
    validate_command_policy_inner(command, args, None, policy)
}

fn validate_command_policy_with_resolved_executable(
    command: &str,
    args: &[String],
    resolved_command: &str,
    policy: &CommandExecutionPolicy,
) -> std::result::Result<(), ToolError> {
    validate_command_policy_inner(command, args, Some(resolved_command), policy)
}

fn validate_command_policy_inner(
    command: &str,
    args: &[String],
    resolved_command: Option<&str>,
    policy: &CommandExecutionPolicy,
) -> std::result::Result<(), ToolError> {
    let argv = std::iter::once(command.to_string())
        .chain(args.iter().cloned())
        .collect::<Vec<_>>();
    let resolved_argv = resolved_command.map(|resolved| {
        std::iter::once(resolved.to_string())
            .chain(args.iter().cloned())
            .collect::<Vec<_>>()
    });

    if let Some(prefix) =
        first_matching_prefix(&argv, &policy.forbidden_argv_prefixes).or_else(|| {
            resolved_argv
                .as_ref()
                .and_then(|argv| first_matching_prefix(argv, &policy.forbidden_argv_prefixes))
        })
    {
        return Err(policy_denial(
            policy,
            DenialReason::ForbiddenPrefix {
                matched: prefix.clone(),
            },
        ));
    }

    if policy.deny_all_argv {
        return Err(policy_denial(
            policy,
            DenialReason::AllowedPrefixRequired { argv: argv.clone() },
        ));
    }

    let allowed_prefix_matched = first_matching_prefix(&argv, &policy.allowed_argv_prefixes)
        .is_some()
        || resolved_argv.as_ref().is_some_and(|argv| {
            first_matching_prefix(argv, &policy.allowed_argv_prefixes).is_some()
        });
    if !policy.allowed_argv_prefixes.is_empty() && !allowed_prefix_matched {
        return Err(policy_denial(
            policy,
            DenialReason::AllowedPrefixRequired { argv },
        ));
    }

    validate_network_mode(command, args, policy)?;

    if matches!(policy.mode, CommandExecutionMode::ReadOnly) {
        validate_read_only_command_inner(
            command,
            args,
            &policy.read_only_allowlist,
            allowed_prefix_matched,
            policy,
        )?;
    }

    Ok(())
}

pub(crate) fn parse_argv_prefixes(values: &[String]) -> Result<Vec<Vec<String>>> {
    values
        .iter()
        .map(|value| parse_argv_prefix(value))
        .collect::<Result<Vec<_>>>()
}

pub(crate) fn build_shell_env() -> HashMap<String, String> {
    build_shell_env_from_vars(std::env::vars())
}

pub(crate) fn build_shell_env_from_vars<I>(vars: I) -> HashMap<String, String>
where
    I: IntoIterator<Item = (String, String)>,
{
    let mut env = vars
        .into_iter()
        .filter(|(key, _)| {
            CORE_ENV_VARS
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(key))
        })
        .filter(|(key, _)| !is_secret_env_name(key))
        .collect::<HashMap<_, _>>();

    env.entry("PATH".to_string())
        .or_insert_with(|| FALLBACK_PATH.to_string());
    env.insert("PAGER".to_string(), "cat".to_string());
    env.insert("GIT_PAGER".to_string(), "cat".to_string());
    env.insert("NO_COLOR".to_string(), "1".to_string());
    env.insert("CLICOLOR".to_string(), "0".to_string());
    env.insert("TERM".to_string(), "dumb".to_string());
    env
}

fn validate_read_only_command_inner(
    command: &str,
    args: &[String],
    allowlist: &[String],
    allowed_prefix_matched: bool,
    policy: &CommandExecutionPolicy,
) -> std::result::Result<(), ToolError> {
    let command_key = executable_name_lookup_key(command).unwrap_or_else(|| command.to_string());
    let built_in_allowlisted = allowlist.iter().any(|allowed| {
        allowed == command
            || executable_name_lookup_key(allowed)
                .as_deref()
                .is_some_and(|allowed_key| allowed_key == command_key)
    });
    if !built_in_allowlisted && !allowed_prefix_matched {
        return Err(policy_denial(
            policy,
            DenialReason::ReadOnlyCommandNotAllowlisted {
                command: command_key,
            },
        ));
    }

    match command_key.as_str() {
        "sed" => {
            if let Some(argument) = args.iter().find(|arg| {
                arg.as_str() == "-i"
                    || arg.as_str() == "--in-place"
                    || arg.starts_with("-i")
                    || arg.starts_with("--in-place=")
            }) {
                return Err(read_only_argument_denial(policy, "sed", argument));
            }
        }
        "find" => {
            if let Some(argument) = args.iter().find(|arg| {
                matches!(
                    arg.as_str(),
                    "-delete"
                        | "-exec"
                        | "-execdir"
                        | "-ok"
                        | "-okdir"
                        | "-fprint"
                        | "-fprint0"
                        | "-fprintf"
                        | "-fls"
                )
            }) {
                return Err(read_only_argument_denial(policy, "find", argument));
            }
        }
        "git" => validate_git_args(args, policy)?,
        "rg" => validate_ripgrep_args(args, policy)?,
        "launchctl" => validate_launchctl_args(args, policy)?,
        "tailscale" => validate_tailscale_args(args, policy)?,
        "curl" => validate_curl_args(args, policy)?,
        "sudo" => validate_sudo_args(args, policy)?,
        _ => {}
    }

    Ok(())
}

fn parse_argv_prefix(value: &str) -> Result<Vec<String>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("argv prefix cannot be empty");
    }

    if trimmed.starts_with('[') {
        let prefix = serde_json::from_str::<Vec<String>>(trimmed)
            .with_context(|| format!("parsing argv prefix JSON {trimmed}"))?;
        if prefix.is_empty() || prefix.iter().any(|token| token.trim().is_empty()) {
            bail!("argv prefix must contain non-empty tokens");
        }
        return Ok(prefix);
    }

    let prefix = trimmed
        .split_ascii_whitespace()
        .map(str::to_string)
        .collect::<Vec<_>>();
    if prefix.is_empty() {
        bail!("argv prefix cannot be empty");
    }
    Ok(prefix)
}

fn first_matching_prefix<'a>(
    argv: &[String],
    prefixes: &'a [Vec<String>],
) -> Option<&'a Vec<String>> {
    prefixes.iter().find(|prefix| {
        argv.len() >= prefix.len() && argv.iter().zip(prefix.iter()).all(|(a, b)| a == b)
    })
}

fn policy_denial(policy: &CommandExecutionPolicy, reason: DenialReason) -> ToolError {
    ToolError::policy_denial(CommandPolicyDenial::new(
        reason,
        policy.mode,
        policy.network_mode,
    ))
}

fn read_only_argument_denial(
    policy: &CommandExecutionPolicy,
    command: &str,
    argument: &str,
) -> ToolError {
    policy_denial(
        policy,
        DenialReason::ReadOnlyArgumentNotAllowed {
            command: command.to_string(),
            argument: argument.to_string(),
        },
    )
}

fn executable_name_lookup_key(raw: &str) -> Option<String> {
    Path::new(raw)
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::to_string)
}

fn is_secret_env_name(key: &str) -> bool {
    let key = key.to_ascii_uppercase();
    key.contains("KEY")
        || key.contains("SECRET")
        || key.contains("TOKEN")
        || key.contains("PASSWORD")
}

fn validate_network_mode(
    command: &str,
    args: &[String],
    policy: &CommandExecutionPolicy,
) -> std::result::Result<(), ToolError> {
    if !matches!(policy.network_mode, CommandNetworkMode::Disabled) {
        return Ok(());
    }

    match policy.mode {
        CommandExecutionMode::WorkspaceWrite => Ok(()),
        CommandExecutionMode::Unrestricted => Err(policy_denial(
            policy,
            DenialReason::DisabledNetworkUnenforceable,
        )),
        CommandExecutionMode::ReadOnly => {
            validate_read_only_network_disabled(command, args, policy)
        }
    }
}

fn validate_read_only_network_disabled(
    command: &str,
    args: &[String],
    policy: &CommandExecutionPolicy,
) -> std::result::Result<(), ToolError> {
    let command_key = executable_name_lookup_key(command).unwrap_or_else(|| command.to_string());
    match command_key.as_str() {
        "curl" => Err(policy_denial(
            policy,
            DenialReason::DisabledNetworkCommand {
                command: "curl".to_string(),
            },
        )),
        "tailscale" => match args.first().map(String::as_str) {
            Some("ping" | "netcheck") => Err(policy_denial(
                policy,
                DenialReason::DisabledNetworkCommand {
                    command: "tailscale".to_string(),
                },
            )),
            _ => Ok(()),
        },
        _ => Ok(()),
    }
}

#[cfg(test)]
pub(in crate::toolset) fn select_sandbox_for_policy(
    mode: CommandExecutionMode,
    workspace_write_sandbox_enforced: bool,
) -> Result<&'static str> {
    match mode {
        CommandExecutionMode::ReadOnly => Ok("policy_read_only"),
        CommandExecutionMode::Unrestricted => Ok("unsandboxed_unrestricted"),
        CommandExecutionMode::WorkspaceWrite if workspace_write_sandbox_enforced => {
            Ok("macos_seatbelt")
        }
        CommandExecutionMode::WorkspaceWrite => {
            if cfg!(target_os = "macos") {
                bail!("macOS sandbox-exec is required for workspace_write bash but was not found")
            } else {
                bail!("workspace_write bash requires macOS seatbelt sandbox enforcement on this build")
            }
        }
    }
}

#[cfg(target_os = "macos")]
pub(crate) fn workspace_write_sandbox_enforced() -> bool {
    Path::new(SANDBOX_EXEC).exists()
}

#[cfg(not(target_os = "macos"))]
pub(crate) fn workspace_write_sandbox_enforced() -> bool {
    false
}

fn sandboxed_command_for_policy(
    root: &Path,
    command_name: &str,
    args: &[String],
    policy: &CommandExecutionPolicy,
) -> std::result::Result<(String, Vec<String>, &'static str), ToolError> {
    let sandbox = select_sandbox_for_execution(policy)?;
    match policy.mode {
        CommandExecutionMode::ReadOnly => Ok((command_name.to_string(), args.to_vec(), sandbox)),
        CommandExecutionMode::Unrestricted => {
            Ok((command_name.to_string(), args.to_vec(), sandbox))
        }
        CommandExecutionMode::WorkspaceWrite => sandboxed_workspace_write_command(
            root,
            command_name,
            args,
            policy.network_mode,
            sandbox,
        )
        .map_err(Into::into),
    }
}

fn select_sandbox_for_execution(
    policy: &CommandExecutionPolicy,
) -> std::result::Result<&'static str, ToolError> {
    match policy.mode {
        CommandExecutionMode::ReadOnly => Ok("policy_read_only"),
        CommandExecutionMode::Unrestricted => Ok("unsandboxed_unrestricted"),
        CommandExecutionMode::WorkspaceWrite if workspace_write_sandbox_enforced() => {
            Ok("macos_seatbelt")
        }
        CommandExecutionMode::WorkspaceWrite => Err(policy_denial(
            policy,
            DenialReason::WorkspaceWriteSandboxUnavailable,
        )),
    }
}

#[cfg(target_os = "macos")]
fn sandboxed_workspace_write_command(
    root: &Path,
    command_name: &str,
    args: &[String],
    network_mode: CommandNetworkMode,
    sandbox: &'static str,
) -> Result<(String, Vec<String>, &'static str)> {
    let policy = macos_workspace_write_policy(network_mode);
    let mut sandbox_args = vec![
        "-p".to_string(),
        policy,
        format!("-DWRITABLE_ROOT={}", root.display()),
        "--".to_string(),
        command_name.to_string(),
    ];
    sandbox_args.extend(args.iter().cloned());
    Ok((SANDBOX_EXEC.to_string(), sandbox_args, sandbox))
}

#[cfg(not(target_os = "macos"))]
fn sandboxed_workspace_write_command(
    _root: &Path,
    _command_name: &str,
    _args: &[String],
    _network_mode: CommandNetworkMode,
    _sandbox: &'static str,
) -> Result<(String, Vec<String>, &'static str)> {
    bail!("workspace_write bash requires macOS seatbelt sandbox enforcement on this build")
}

#[cfg(target_os = "macos")]
fn macos_workspace_write_policy(network_mode: CommandNetworkMode) -> String {
    let network_policy = if network_mode.allows_network() {
        "(allow network-outbound)\n(allow network-inbound)\n"
    } else {
        ""
    };
    format!(
        r#"(version 1)
(deny default)
(allow process-exec)
(allow process-fork)
(allow signal (target same-sandbox))
(allow process-info* (target same-sandbox))
(allow sysctl-read)
(allow mach-lookup)
(allow ipc-posix-sem)
(allow ipc-posix-shm-read*)
(allow ipc-posix-shm-write*)
(allow file-read*)
(allow file-write-data (literal "/dev/null"))
(allow file-write* (subpath (param "WRITABLE_ROOT")))
{network_policy}"#
    )
}

fn validate_launchctl_args(
    args: &[String],
    policy: &CommandExecutionPolicy,
) -> std::result::Result<(), ToolError> {
    let subcommand = args.first().map(String::as_str).ok_or_else(|| {
        policy_denial(
            policy,
            DenialReason::ReadOnlySubcommandRequired {
                command: "launchctl".to_string(),
            },
        )
    })?;

    match subcommand {
        "list" | "print" | "print-disabled" | "blame" => Ok(()),
        other => Err(policy_denial(
            policy,
            DenialReason::ReadOnlySubcommandNotAllowlisted {
                command: "launchctl".to_string(),
                subcommand: other.to_string(),
            },
        )),
    }
}

fn validate_tailscale_args(
    args: &[String],
    policy: &CommandExecutionPolicy,
) -> std::result::Result<(), ToolError> {
    let subcommand = args.first().map(String::as_str).ok_or_else(|| {
        policy_denial(
            policy,
            DenialReason::ReadOnlySubcommandRequired {
                command: "tailscale".to_string(),
            },
        )
    })?;

    match subcommand {
        "status" | "ip" | "netcheck" | "version" | "ping" => Ok(()),
        other => Err(policy_denial(
            policy,
            DenialReason::ReadOnlySubcommandNotAllowlisted {
                command: "tailscale".to_string(),
                subcommand: other.to_string(),
            },
        )),
    }
}

fn validate_curl_args(
    args: &[String],
    policy: &CommandExecutionPolicy,
) -> std::result::Result<(), ToolError> {
    let mut has_http_url = false;
    for arg in args {
        if arg.starts_with("http://") || arg.starts_with("https://") {
            has_http_url = true;
        }

        let mutating = matches!(
            arg.as_str(),
            "-d" | "--data"
                | "--data-raw"
                | "--data-binary"
                | "--data-urlencode"
                | "-F"
                | "--form"
                | "-T"
                | "--upload-file"
                | "-X"
                | "--request"
                | "-o"
                | "--output"
                | "-O"
                | "--remote-name"
                | "--remote-header-name"
                | "-K"
                | "--config"
                | "--next"
        ) || arg.starts_with("-d")
            || arg.starts_with("--data=")
            || arg.starts_with("-F")
            || arg.starts_with("--form=")
            || arg.starts_with("-T")
            || arg.starts_with("--upload-file=")
            || arg.starts_with("-X")
            || arg.starts_with("--request=")
            || arg.starts_with("-o")
            || arg.starts_with("--output=")
            || arg.starts_with("-O")
            || arg.starts_with("-K")
            || arg.starts_with("--config=");
        if mutating {
            return Err(read_only_argument_denial(policy, "curl", arg));
        }
    }

    if !has_http_url {
        return Err(policy_denial(
            policy,
            DenialReason::ReadOnlyUrlRequired {
                command: "curl".to_string(),
            },
        ));
    }

    Ok(())
}

fn validate_sudo_args(
    args: &[String],
    policy: &CommandExecutionPolicy,
) -> std::result::Result<(), ToolError> {
    let command = args.first().map(String::as_str).ok_or_else(|| {
        policy_denial(
            policy,
            DenialReason::ReadOnlySubcommandRequired {
                command: "sudo".to_string(),
            },
        )
    })?;
    let command_name = Path::new(command)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(command);

    match command_name {
        "launchctl" if command == "/bin/launchctl" => validate_launchctl_args(&args[1..], policy),
        "launchctl" => Err(read_only_argument_denial(policy, "sudo", command)),
        other => Err(policy_denial(
            policy,
            DenialReason::ReadOnlySubcommandNotAllowlisted {
                command: "sudo".to_string(),
                subcommand: other.to_string(),
            },
        )),
    }
}

fn validate_git_args(
    args: &[String],
    policy: &CommandExecutionPolicy,
) -> std::result::Result<(), ToolError> {
    if let Some(argument) = args
        .iter()
        .map(String::as_str)
        .find(|arg| git_global_option_requires_denial(arg))
    {
        return Err(read_only_argument_denial(policy, "git", argument));
    }

    let (subcommand_idx, subcommand) = find_git_subcommand(args).ok_or_else(|| {
        policy_denial(
            policy,
            DenialReason::ReadOnlySubcommandRequired {
                command: "git".to_string(),
            },
        )
    })?;
    let subcommand_args = &args[subcommand_idx + 1..];
    validate_git_read_only_flags(subcommand_args, policy)?;

    match subcommand {
        "status" | "diff" | "show" | "log" | "ls-files" | "grep" | "rev-parse" => Ok(()),
        "branch" => validate_git_branch_args(subcommand_args, policy),
        other => Err(policy_denial(
            policy,
            DenialReason::ReadOnlySubcommandNotAllowlisted {
                command: "git".to_string(),
                subcommand: other.to_string(),
            },
        )),
    }
}

fn validate_ripgrep_args(
    args: &[String],
    policy: &CommandExecutionPolicy,
) -> std::result::Result<(), ToolError> {
    const UNSAFE_WITH_ARGS: &[&str] = &["--pre", "--hostname-bin"];
    const UNSAFE_WITHOUT_ARGS: &[&str] = &["--search-zip", "-z"];
    for arg in args {
        if UNSAFE_WITHOUT_ARGS.contains(&arg.as_str())
            || UNSAFE_WITH_ARGS
                .iter()
                .any(|option| arg == option || arg.starts_with(&format!("{option}=")))
        {
            return Err(read_only_argument_denial(policy, "rg", arg));
        }
    }
    Ok(())
}

fn git_global_option_requires_denial(arg: &str) -> bool {
    matches!(
        arg,
        "-C" | "-c"
            | "--config-env"
            | "--exec-path"
            | "--git-dir"
            | "--namespace"
            | "--super-prefix"
            | "--work-tree"
    ) || ((arg.starts_with("-C") || arg.starts_with("-c")) && arg.len() > 2)
        || arg.starts_with("--config-env=")
        || arg.starts_with("--exec-path=")
        || arg.starts_with("--git-dir=")
        || arg.starts_with("--namespace=")
        || arg.starts_with("--super-prefix=")
        || arg.starts_with("--work-tree=")
}

fn find_git_subcommand(args: &[String]) -> Option<(usize, &str)> {
    let mut skip_next = false;
    for (idx, arg) in args.iter().enumerate() {
        if skip_next {
            skip_next = false;
            continue;
        }

        let arg = arg.as_str();
        if matches!(
            arg,
            "-C" | "-c"
                | "--config-env"
                | "--exec-path"
                | "--git-dir"
                | "--namespace"
                | "--super-prefix"
                | "--work-tree"
        ) {
            skip_next = true;
            continue;
        }
        if arg == "--" || arg.starts_with('-') {
            continue;
        }
        return Some((idx, arg));
    }
    None
}

fn validate_git_read_only_flags(
    args: &[String],
    policy: &CommandExecutionPolicy,
) -> std::result::Result<(), ToolError> {
    const UNSAFE_GIT_FLAGS: &[&str] = &[
        "--output",
        "--ext-diff",
        "--textconv",
        "--exec",
        "--paginate",
    ];
    for arg in args {
        if UNSAFE_GIT_FLAGS.contains(&arg.as_str())
            || arg.starts_with("--output=")
            || arg.starts_with("--exec=")
        {
            return Err(read_only_argument_denial(policy, "git", arg));
        }
    }
    Ok(())
}

fn validate_git_branch_args(
    args: &[String],
    policy: &CommandExecutionPolicy,
) -> std::result::Result<(), ToolError> {
    if args.is_empty() {
        return Ok(());
    }

    for arg in args {
        match arg.as_str() {
            "--list" | "-l" | "--show-current" | "-a" | "--all" | "-r" | "--remotes" | "-v"
            | "-vv" | "--verbose" => {}
            _ if arg.starts_with("--format=") => {}
            _ => return Err(read_only_argument_denial(policy, "git", arg)),
        }
    }
    Ok(())
}

#[derive(Serialize)]
struct CommandOutput {
    #[serde(flatten)]
    metadata: CommandMetadata,
    stdout: String,
    stderr: String,
}

#[derive(Serialize)]
struct CommandMetadata {
    ok: bool,
    status: &'static str,
    command: String,
    argv: Vec<String>,
    cwd: String,
    exit_code: Option<i32>,
    timed_out: bool,
    duration_ms: u64,
    timeout_ms: u64,
    execution_mode: CommandExecutionMode,
    network_mode: CommandNetworkMode,
    sandbox: &'static str,
    stdout_capture_incomplete: bool,
    stderr_capture_incomplete: bool,
    stdout_truncation: StreamTruncationMetadata,
    stderr_truncation: StreamTruncationMetadata,
}

#[derive(Clone, Copy, Serialize)]
struct StreamTruncationMetadata {
    returned_bytes: usize,
    total_bytes: usize,
    max_bytes: usize,
    truncated: bool,
}

struct TruncatedStream {
    content: String,
    metadata: StreamTruncationMetadata,
}

fn truncate_stream(text: &str, max_bytes: usize) -> TruncatedStream {
    let limits = TruncationLimits {
        max_bytes,
        max_lines: usize::MAX,
    };
    let result = truncate(text, TruncationMode::Head, &limits);
    TruncatedStream {
        content: result.text,
        metadata: StreamTruncationMetadata {
            returned_bytes: result.returned_bytes,
            total_bytes: result.original_bytes,
            max_bytes,
            truncated: result.truncated,
        },
    }
}

fn render_command_output(output: &CommandOutput, raw_json: bool) -> Result<String> {
    if raw_json {
        return serde_json::to_string(output).context("serializing command output");
    }

    let mut out = String::from(OUTPUT_META_PREFIX);
    out.push_str(&serde_json::to_string(&output.metadata).context("serializing command metadata")?);
    out.push_str("\nstdout:\n");
    out.push_str(if output.stdout.is_empty() {
        "(empty)"
    } else {
        &output.stdout
    });
    out.push_str("\nstderr:\n");
    out.push_str(if output.stderr.is_empty() {
        "(empty)"
    } else {
        &output.stderr
    });
    Ok(out)
}

fn elapsed_ms(started: Instant) -> u64 {
    let millis = started.elapsed().as_millis();
    millis.min(u128::from(u64::MAX)) as u64
}

fn millis(duration: Duration) -> u64 {
    let millis = duration.as_millis();
    millis.min(u128::from(u64::MAX)) as u64
}

fn shell_join(argv: &[String]) -> String {
    argv.iter()
        .map(|arg| shell_quote(arg))
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_quote(arg: &str) -> String {
    if !arg.is_empty()
        && arg
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-' | ':' | '='))
    {
        return arg.to_string();
    }
    format!("'{}'", arg.replace('\'', "'\"'\"'"))
}
