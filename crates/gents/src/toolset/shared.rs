mod command;
mod context;
mod filesystem;

#[cfg(test)]
pub(crate) use command::apply_workspace_authority;
pub(crate) use command::parse_argv_prefixes;
pub(super) use command::run_command;
pub(crate) use command::validate_command_policy;
pub(crate) use command::{
    admit_host_executable, default_lsp_network_mode, effective_command_policy,
    lsp_sandbox_for_effective, normalize_workspace_lifecycle_state, prepare_managed_command,
    workspace_write_sandbox_enforced,
};
#[cfg(test)]
pub(super) use command::{
    build_shell_env_from_vars, select_sandbox_for_policy, validate_read_only_command,
};
pub use command::{
    CommandConstraints, CommandExecutionMode, CommandExecutionPolicy, CommandNetworkMode,
    WorkspaceAuthority,
};
pub(super) use context::{ToolContext, ToolError};
pub(super) use filesystem::{cap_output, render_file_contents};

pub(super) fn default_max_list_entries() -> usize {
    super::DEFAULT_MAX_LIST_ENTRIES
}

pub(super) fn default_max_file_chars() -> usize {
    super::DEFAULT_MAX_FILE_CHARS
}

pub(super) fn default_max_matches() -> usize {
    super::DEFAULT_MAX_MATCHES
}

/// Resolves the effective command timeout for a bash tool call (#985, #1018).
///
/// Foreground: an omitted `timeout_secs` applies the tool's configured
/// default — the same value the schema advertises — and explicit requests are
/// clamped to the operator's foreground ceiling, which is never below the
/// default. Background (spawn_process): neither foreground value applies; the
/// run gets the `BACKGROUND_COMMAND_TIMEOUT_SECS` lifetime budget instead.
pub(super) fn resolve_command_timeout(
    requested_secs: Option<u64>,
    foreground_default: std::time::Duration,
    foreground_max: std::time::Duration,
    background: bool,
) -> std::time::Duration {
    if background {
        let budget = std::time::Duration::from_secs(super::BACKGROUND_COMMAND_TIMEOUT_SECS);
        return match requested_secs {
            Some(secs) => std::time::Duration::from_secs(secs.max(1)).min(budget),
            None => budget,
        };
    }
    let ceiling = foreground_max.max(foreground_default);
    match requested_secs {
        Some(secs) => std::time::Duration::from_secs(secs.max(1)).min(ceiling),
        None => foreground_default,
    }
}

/// Scope-aware wrapper: reads whether the current execution was backgrounded
/// from the task-local tool runtime scope.
pub(super) fn resolve_command_timeout_in_scope(
    requested_secs: Option<u64>,
    foreground_default: std::time::Duration,
    foreground_max: std::time::Duration,
) -> std::time::Duration {
    let background = crate::tool_call_lifecycle::runtime::current_tool_runtime_context()
        .is_some_and(|context| context.background);
    resolve_command_timeout(
        requested_secs,
        foreground_default,
        foreground_max,
        background,
    )
}
