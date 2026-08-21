use std::path::PathBuf;
use std::time::Duration;

use crate::llm::tool::ToolDyn;
use anyhow::Result;
use std::collections::HashMap;

mod args;
mod bash_tools;
mod cli_tool;
mod context_budget;
mod denial;
pub mod edit_match;
mod file_tools;
mod goal;
pub(crate) mod lsp;
#[cfg(feature = "agent-memory")]
mod memory;
mod native_runner;
pub use native_runner::enable_self_runner;
mod session_history;
mod shared;
mod subagent;
#[cfg(test)]
mod tests;

use bash_tools::{ReadOnlyBashTool, UnrestrictedBashTool};
use cli_tool::CliTool;
use file_tools::{EditFileTool, GlobTool, GrepTool, ListFilesTool, ReadFileTool, WriteFileTool};
use shared::ToolContext;
use subagent::{
    CancelProcessTool, CancelSubagentTool, ListProcessesTool, ListSubagentsTool, ReadProcessTool,
    ReadSubagentTool, SpawnProcessTool, SpawnSubagentTool, SteerSubagentTool, WaitProcessTool,
    WaitSubagentTool,
};

use crate::tool_surface::{BackgroundToolConfig, SubagentToolConfig};

pub use context_budget::{
    build_context_budget_tool, load_context_budget_snapshot, ContextBudgetSnapshot,
    CONTEXT_BUDGET_TOOL_NAME,
};
pub use denial::CommandPolicyDenial;
pub(crate) use denial::DenialReason;
pub(crate) use goal::build_goal_tools;
pub(crate) use goal::{GetGoalArgs, UpdateGoalArgs};
pub use lsp::{
    lsp_action_authorized, lsp_advertised, lsp_apply_authorized, result_looks_failed,
    result_path_matches, LspAction, LspMutationSource,
};
#[cfg(feature = "agent-memory")]
pub use memory::{build_memory_tool, MEMORY_TOOL_NAME};
pub use session_history::{
    build_session_history_tool, load_session_history_snapshot, SessionHistoryRow,
    SessionHistorySnapshot, SESSION_HISTORY_TOOL_NAME,
};
#[cfg(test)]
pub(crate) use shared::apply_workspace_authority;
pub(crate) use shared::parse_argv_prefixes;
pub(crate) use shared::{
    admit_host_executable, default_lsp_network_mode, effective_command_policy,
    lsp_sandbox_for_effective, normalize_workspace_lifecycle_state, prepare_managed_command,
    workspace_write_sandbox_enforced,
};
pub use shared::{
    CommandConstraints, CommandExecutionMode, CommandExecutionPolicy, CommandNetworkMode,
    WorkspaceAuthority,
};

pub(crate) fn default_read_only_command_policy() -> CommandExecutionPolicy {
    CommandExecutionPolicy::read_only(default_read_only_commands())
}

const DEFAULT_MAX_FILE_CHARS: usize = 32_000;
const DEFAULT_MAX_COMMAND_CHARS: usize = 16_000;
const DEFAULT_MAX_LIST_ENTRIES: usize = 200;
const DEFAULT_MAX_MATCHES: usize = 200;
// Foreground default aligned with other agent frameworks (Claude Code and
// grok-build both default to 120s); deployments raise or lower it with
// `--command-timeout-secs`. Explicit model requests may exceed it up to the
// separately configured `--command-timeout-max-secs` ceiling (#985, #1018).
pub(crate) const DEFAULT_COMMAND_TIMEOUT_SECS: u64 = 120;
// Lifetime budget for commands backgrounded via spawn_process. Background
// runs are exempt from the foreground ceiling — cancel_process and the
// completion notification are the lifecycle controls — but keep a 10-hour
// backstop (grok-build uses the same bound) so an orphaned job cannot run
// forever (#985).
pub(crate) const BACKGROUND_COMMAND_TIMEOUT_SECS: u64 = 36_000;
pub(crate) const SPAWN_SUBAGENT_TOOL_NAME: &str = "spawn_subagent";
pub(crate) const WAIT_SUBAGENT_TOOL_NAME: &str = "wait_subagent";
pub(crate) const LIST_SUBAGENTS_TOOL_NAME: &str = "list_subagents";
pub(crate) const READ_SUBAGENT_TOOL_NAME: &str = "read_subagent";
pub(crate) const STEER_SUBAGENT_TOOL_NAME: &str = "steer_subagent";
pub(crate) const CANCEL_SUBAGENT_TOOL_NAME: &str = "cancel_subagent";
pub(crate) const SPAWN_PROCESS_TOOL_NAME: &str = "spawn_process";
pub(crate) const WAIT_PROCESS_TOOL_NAME: &str = "wait_process";
pub(crate) const LIST_PROCESSES_TOOL_NAME: &str = "list_processes";
pub(crate) const READ_PROCESS_TOOL_NAME: &str = "read_process";
pub(crate) const CANCEL_PROCESS_TOOL_NAME: &str = "cancel_process";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliToolConfig {
    pub name: String,
    pub binary_path: PathBuf,
    pub description: String,
    pub allowed_argv_prefixes: Vec<Vec<String>>,
    pub env_vars: HashMap<String, String>,
    pub working_dir: Option<PathBuf>,
    pub timeout_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolSet {
    tools: Vec<NativeTool>,
    read_root: Option<PathBuf>,
}

impl ToolSet {
    pub fn readonly() -> Self {
        Self {
            tools: vec![
                NativeTool::ListFiles {
                    max_entries: DEFAULT_MAX_LIST_ENTRIES,
                },
                NativeTool::ReadFile {
                    max_chars: DEFAULT_MAX_FILE_CHARS,
                },
                NativeTool::Glob {
                    max_matches: DEFAULT_MAX_MATCHES,
                },
                NativeTool::Grep {
                    max_matches: DEFAULT_MAX_MATCHES,
                },
                NativeTool::BashReadOnly {
                    timeout: Duration::from_secs(DEFAULT_COMMAND_TIMEOUT_SECS),
                    timeout_max: Duration::from_secs(DEFAULT_COMMAND_TIMEOUT_SECS),
                    allowlist: default_read_only_commands(),
                    policy: CommandExecutionPolicy::read_only(default_read_only_commands()),
                },
            ],
            read_root: None,
        }
    }

    pub fn readwrite(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            tools: vec![
                NativeTool::ListFiles {
                    max_entries: DEFAULT_MAX_LIST_ENTRIES,
                },
                NativeTool::ReadFile {
                    max_chars: DEFAULT_MAX_FILE_CHARS,
                },
                NativeTool::Glob {
                    max_matches: DEFAULT_MAX_MATCHES,
                },
                NativeTool::Grep {
                    max_matches: DEFAULT_MAX_MATCHES,
                },
                NativeTool::BashReadOnly {
                    timeout: Duration::from_secs(DEFAULT_COMMAND_TIMEOUT_SECS),
                    timeout_max: Duration::from_secs(DEFAULT_COMMAND_TIMEOUT_SECS),
                    allowlist: default_read_only_commands(),
                    policy: CommandExecutionPolicy::read_only(default_read_only_commands()),
                },
                NativeTool::WriteFile { root: root.clone() },
                NativeTool::EditFile { root: root.clone() },
                NativeTool::BashUnrestricted {
                    timeout: Duration::from_secs(DEFAULT_COMMAND_TIMEOUT_SECS),
                    timeout_max: Duration::from_secs(DEFAULT_COMMAND_TIMEOUT_SECS),
                    root: root.clone(),
                    policy: CommandExecutionPolicy::write_capable(),
                },
            ],
            read_root: Some(root),
        }
    }

    pub fn meta_only() -> Self {
        Self {
            tools: Vec::new(),
            read_root: None,
        }
    }

    pub fn builder() -> ToolSetBuilder {
        ToolSetBuilder::default()
    }

    pub fn native_tools(&self) -> &[NativeTool] {
        &self.tools
    }

    pub fn read_root(&self) -> Option<&std::path::Path> {
        self.read_root.as_deref()
    }

    pub fn tool_names(&self) -> Vec<String> {
        self.tools.iter().map(NativeTool::tool_name).collect()
    }

    pub fn backgroundable_tool_names(&self) -> Vec<String> {
        self.tools
            .iter()
            .filter(|tool| tool.backgroundable())
            .map(NativeTool::tool_name)
            .collect()
    }

    pub fn is_backgroundable_tool_name(&self, name: &str) -> bool {
        self.tools
            .iter()
            .any(|tool| tool.tool_name() == name && tool.backgroundable())
    }

    pub fn build_native_tools(&self) -> Result<Vec<Box<dyn ToolDyn>>> {
        self.build_native_tools_with_writethrough(None)
    }

    pub fn build_native_tools_with_writethrough(
        &self,
        writethrough: Option<lsp::LspWritethrough>,
    ) -> Result<Vec<Box<dyn ToolDyn>>> {
        let read_context = match &self.read_root {
            Some(root) => ToolContext::new(root.clone(), read_root_requires_create(&self.tools))?,
            None => ToolContext::from_default_read_root()?,
        };

        let mut built: Vec<Box<dyn ToolDyn>> = Vec::new();
        for tool in &self.tools {
            match tool {
                NativeTool::ListFiles { max_entries } => built.push(Box::new(ListFilesTool::new(
                    read_context.clone(),
                    *max_entries,
                ))),
                NativeTool::ReadFile { max_chars } => built.push(Box::new(ReadFileTool::new(
                    read_context.clone(),
                    *max_chars,
                ))),
                NativeTool::Glob { max_matches } => {
                    built.push(Box::new(GlobTool::new(read_context.clone(), *max_matches)))
                }
                NativeTool::Grep { max_matches } => {
                    built.push(Box::new(GrepTool::new(read_context.clone(), *max_matches)))
                }
                NativeTool::WriteFile { root } => {
                    let mut writer = WriteFileTool::new(ToolContext::new(root.clone(), true)?);
                    if let Some(writethrough) = writethrough.clone() {
                        writer = writer.with_writethrough(writethrough);
                    }
                    built.push(Box::new(writer));
                }
                NativeTool::EditFile { root } => {
                    let mut editor = EditFileTool::new(ToolContext::new(root.clone(), true)?);
                    if let Some(writethrough) = writethrough.clone() {
                        editor = editor.with_writethrough(writethrough);
                    }
                    built.push(Box::new(editor));
                }
                NativeTool::BashReadOnly {
                    timeout,
                    timeout_max,
                    policy,
                    ..
                } => built.push(Box::new(ReadOnlyBashTool::with_policy(
                    read_context.clone(),
                    *timeout,
                    *timeout_max,
                    policy.clone(),
                ))),
                NativeTool::BashUnrestricted {
                    timeout,
                    timeout_max,
                    root,
                    policy,
                } => built.push(Box::new(UnrestrictedBashTool::with_policy(
                    ToolContext::new(root.clone(), true)?,
                    *timeout,
                    *timeout_max,
                    policy.clone(),
                ))),
                NativeTool::Cli(tool) => built.push(Box::new(CliTool::new(tool.clone()))),
            }
        }
        Ok(built)
    }
}

fn read_root_requires_create(tools: &[NativeTool]) -> bool {
    tools.iter().any(|tool| {
        matches!(
            tool,
            NativeTool::WriteFile { .. }
                | NativeTool::EditFile { .. }
                | NativeTool::BashUnrestricted { .. }
        )
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeTool {
    ListFiles {
        max_entries: usize,
    },
    ReadFile {
        max_chars: usize,
    },
    Glob {
        max_matches: usize,
    },
    Grep {
        max_matches: usize,
    },
    WriteFile {
        root: PathBuf,
    },
    EditFile {
        root: PathBuf,
    },
    BashReadOnly {
        timeout: Duration,
        timeout_max: Duration,
        allowlist: Vec<String>,
        policy: CommandExecutionPolicy,
    },
    BashUnrestricted {
        timeout: Duration,
        timeout_max: Duration,
        root: PathBuf,
        policy: CommandExecutionPolicy,
    },
    Cli(CliToolConfig),
}

impl NativeTool {
    pub fn tool_name(&self) -> String {
        match self {
            Self::ListFiles { .. } => "list_files".to_string(),
            Self::ReadFile { .. } => "read_file".to_string(),
            Self::Glob { .. } => "glob".to_string(),
            Self::Grep { .. } => "grep".to_string(),
            Self::WriteFile { .. } => "write_file".to_string(),
            Self::EditFile { .. } => "edit_file".to_string(),
            Self::BashReadOnly { .. } => "bash".to_string(),
            Self::BashUnrestricted { .. } => "bash_unrestricted".to_string(),
            Self::Cli(tool) => tool.name.clone(),
        }
    }

    pub fn backgroundable(&self) -> bool {
        matches!(
            self,
            Self::BashReadOnly { .. } | Self::BashUnrestricted { .. }
        )
    }
}

#[derive(Debug, Default)]
pub struct ToolSetBuilder {
    tools: Vec<NativeTool>,
    read_root: Option<PathBuf>,
}

impl ToolSetBuilder {
    pub fn read_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.read_root = Some(root.into());
        self
    }

    pub fn list_files(mut self) -> Self {
        self.tools.push(NativeTool::ListFiles {
            max_entries: DEFAULT_MAX_LIST_ENTRIES,
        });
        self
    }

    pub fn read_file(mut self) -> Self {
        self.tools.push(NativeTool::ReadFile {
            max_chars: DEFAULT_MAX_FILE_CHARS,
        });
        self
    }

    pub fn glob(mut self) -> Self {
        self.tools.push(NativeTool::Glob {
            max_matches: DEFAULT_MAX_MATCHES,
        });
        self
    }

    pub fn grep(mut self) -> Self {
        self.tools.push(NativeTool::Grep {
            max_matches: DEFAULT_MAX_MATCHES,
        });
        self
    }

    pub fn write_file(mut self, root: impl Into<PathBuf>) -> Self {
        self.tools.push(NativeTool::WriteFile { root: root.into() });
        self
    }

    pub fn edit_file(mut self, root: impl Into<PathBuf>) -> Self {
        self.tools.push(NativeTool::EditFile { root: root.into() });
        self
    }

    pub fn bash_read_only(self) -> Self {
        let timeout = Duration::from_secs(DEFAULT_COMMAND_TIMEOUT_SECS);
        self.bash_read_only_with_timeouts(timeout, timeout)
    }

    pub fn bash_read_only_with_timeouts(
        mut self,
        timeout: Duration,
        timeout_max: Duration,
    ) -> Self {
        self.tools.push(NativeTool::BashReadOnly {
            timeout,
            timeout_max: timeout_max.max(timeout),
            allowlist: default_read_only_commands(),
            policy: CommandExecutionPolicy::read_only(default_read_only_commands()),
        });
        self
    }

    pub fn bash_read_only_with_policy(self, policy: CommandExecutionPolicy) -> Self {
        let timeout = Duration::from_secs(DEFAULT_COMMAND_TIMEOUT_SECS);
        self.bash_read_only_with_policy_and_timeouts(policy, timeout, timeout)
    }

    pub fn bash_read_only_with_policy_and_timeouts(
        mut self,
        policy: CommandExecutionPolicy,
        timeout: Duration,
        timeout_max: Duration,
    ) -> Self {
        self.tools.push(NativeTool::BashReadOnly {
            timeout,
            timeout_max: timeout_max.max(timeout),
            allowlist: default_read_only_commands(),
            policy,
        });
        self
    }

    pub fn bash_unrestricted(self, root: impl Into<PathBuf>) -> Self {
        let timeout = Duration::from_secs(DEFAULT_COMMAND_TIMEOUT_SECS);
        self.bash_unrestricted_with_timeouts(root, timeout, timeout)
    }

    pub fn bash_unrestricted_with_timeouts(
        mut self,
        root: impl Into<PathBuf>,
        timeout: Duration,
        timeout_max: Duration,
    ) -> Self {
        self.tools.push(NativeTool::BashUnrestricted {
            timeout,
            timeout_max: timeout_max.max(timeout),
            root: root.into(),
            policy: CommandExecutionPolicy::write_capable(),
        });
        self
    }

    pub fn bash_unrestricted_with_policy(
        self,
        root: impl Into<PathBuf>,
        policy: CommandExecutionPolicy,
    ) -> Self {
        let timeout = Duration::from_secs(DEFAULT_COMMAND_TIMEOUT_SECS);
        self.bash_unrestricted_with_policy_and_timeouts(root, policy, timeout, timeout)
    }

    pub fn bash_unrestricted_with_policy_and_timeouts(
        mut self,
        root: impl Into<PathBuf>,
        policy: CommandExecutionPolicy,
        timeout: Duration,
        timeout_max: Duration,
    ) -> Self {
        self.tools.push(NativeTool::BashUnrestricted {
            timeout,
            timeout_max: timeout_max.max(timeout),
            root: root.into(),
            policy,
        });
        self
    }

    pub fn cli_tool(mut self, tool: CliToolConfig) -> Self {
        self.tools.push(NativeTool::Cli(tool));
        self
    }

    pub fn build(self) -> ToolSet {
        ToolSet {
            tools: self.tools,
            read_root: self.read_root,
        }
    }
}

pub fn build_native_tools() -> Result<Vec<Box<dyn ToolDyn>>> {
    ToolSet::builder()
        .list_files()
        .read_file()
        .bash_read_only()
        .build()
        .build_native_tools()
}

pub(crate) fn subagent_tool_names(config: &SubagentToolConfig) -> Vec<String> {
    if !config.tools_enabled() {
        return Vec::new();
    }

    let mut names = [
        SPAWN_SUBAGENT_TOOL_NAME,
        WAIT_SUBAGENT_TOOL_NAME,
        LIST_SUBAGENTS_TOOL_NAME,
        CANCEL_SUBAGENT_TOOL_NAME,
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<Vec<_>>();
    if config.background_inspection_tools_enabled() {
        names.insert(3, READ_SUBAGENT_TOOL_NAME.to_string());
    }
    if config.steer_subagent_enabled() {
        let insert_at = if config.background_inspection_tools_enabled() {
            4
        } else {
            3
        };
        names.insert(insert_at, STEER_SUBAGENT_TOOL_NAME.to_string());
    }
    names
}

pub(crate) fn build_subagent_tools(config: SubagentToolConfig) -> Vec<Box<dyn ToolDyn>> {
    if !config.tools_enabled() {
        return Vec::new();
    }

    let mut tools: Vec<Box<dyn ToolDyn>> = vec![
        Box::new(SpawnSubagentTool::new(config.clone())),
        Box::new(WaitSubagentTool),
        Box::new(ListSubagentsTool),
    ];
    if config.background_inspection_tools_enabled() {
        tools.push(Box::new(ReadSubagentTool));
    }
    if config.steer_subagent_enabled() {
        tools.push(Box::new(SteerSubagentTool));
    }
    tools.push(Box::new(CancelSubagentTool));
    tools
}

pub(crate) fn background_tool_names(config: &BackgroundToolConfig) -> Vec<String> {
    if !config.tools_enabled() {
        return Vec::new();
    }

    [
        SPAWN_PROCESS_TOOL_NAME,
        WAIT_PROCESS_TOOL_NAME,
        LIST_PROCESSES_TOOL_NAME,
        READ_PROCESS_TOOL_NAME,
        CANCEL_PROCESS_TOOL_NAME,
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub(crate) fn build_background_tools(config: BackgroundToolConfig) -> Vec<Box<dyn ToolDyn>> {
    if !config.tools_enabled() {
        return Vec::new();
    }

    vec![
        Box::new(SpawnProcessTool::new(config.clone())),
        Box::new(WaitProcessTool),
        Box::new(ListProcessesTool),
        Box::new(ReadProcessTool),
        Box::new(CancelProcessTool),
    ]
}

fn default_read_only_commands() -> Vec<String> {
    [
        "pwd",
        "ls",
        "find",
        "cat",
        "head",
        "tail",
        "sed",
        "grep",
        "rg",
        "wc",
        "stat",
        "file",
        "git",
        "date",
        "hostname",
        "uptime",
        "df",
        "vm_stat",
        "ps",
        "lsof",
        "curl",
        "launchctl",
        "tailscale",
        "sudo",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}
