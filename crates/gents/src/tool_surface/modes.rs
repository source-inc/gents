use std::path::{Path, PathBuf};

use anyhow::{bail, Result};

use crate::toolset::CliToolConfig;

use super::policy::{EndpointScope, ToolPolicySurface};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FileToolMode {
    #[default]
    Off,
    ReadOnly,
    ReadWrite,
}

impl FileToolMode {
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim() {
            "" | "Off" => Ok(Self::Off),
            "ReadOnly" => Ok(Self::ReadOnly),
            "ReadWrite" => Ok(Self::ReadWrite),
            other => bail!("unknown file tool mode {other}"),
        }
    }

    pub(super) fn rank(self) -> u8 {
        match self {
            Self::Off => 0,
            Self::ReadOnly => 1,
            Self::ReadWrite => 2,
        }
    }

    pub(crate) fn meet(self, other: Self) -> Self {
        if self.rank() <= other.rank() {
            self
        } else {
            other
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BashMode {
    #[default]
    Off,
    ReadOnly,
    Unrestricted,
}

impl BashMode {
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim() {
            "" | "Off" => Ok(Self::Off),
            "ReadOnly" => Ok(Self::ReadOnly),
            "Unrestricted" => Ok(Self::Unrestricted),
            other => bail!("unknown bash mode {other}"),
        }
    }

    pub(super) fn rank(self) -> u8 {
        match self {
            Self::Off => 0,
            Self::ReadOnly => 1,
            Self::Unrestricted => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCeiling {
    file_tools: FileToolMode,
    bash: BashMode,
    cli_tools: Vec<CliToolConfig>,
    command_timeout: std::time::Duration,
    command_timeout_max: Option<std::time::Duration>,
    root: Option<PathBuf>,
    policy: ToolPolicySurface,
}

impl ToolCeiling {
    pub fn meta_only() -> Self {
        Self {
            file_tools: FileToolMode::Off,
            bash: BashMode::Off,
            cli_tools: Vec::new(),
            command_timeout: std::time::Duration::from_secs(
                crate::toolset::DEFAULT_COMMAND_TIMEOUT_SECS,
            ),
            command_timeout_max: None,
            root: None,
            policy: ToolPolicySurface::legacy_non_host_wide(FileToolMode::Off, BashMode::Off),
        }
    }

    pub fn readonly() -> Self {
        Self {
            file_tools: FileToolMode::ReadOnly,
            bash: BashMode::ReadOnly,
            cli_tools: Vec::new(),
            command_timeout: std::time::Duration::from_secs(
                crate::toolset::DEFAULT_COMMAND_TIMEOUT_SECS,
            ),
            command_timeout_max: None,
            root: None,
            policy: ToolPolicySurface::legacy_non_host_wide(
                FileToolMode::ReadOnly,
                BashMode::ReadOnly,
            ),
        }
    }

    pub fn readonly_at(root: impl Into<PathBuf>) -> Self {
        Self {
            file_tools: FileToolMode::ReadOnly,
            bash: BashMode::ReadOnly,
            cli_tools: Vec::new(),
            command_timeout: std::time::Duration::from_secs(
                crate::toolset::DEFAULT_COMMAND_TIMEOUT_SECS,
            ),
            command_timeout_max: None,
            root: Some(root.into()),
            policy: ToolPolicySurface::legacy_non_host_wide(
                FileToolMode::ReadOnly,
                BashMode::ReadOnly,
            ),
        }
    }

    pub fn readwrite(root: impl Into<PathBuf>) -> Self {
        Self {
            file_tools: FileToolMode::ReadWrite,
            bash: BashMode::Unrestricted,
            cli_tools: Vec::new(),
            command_timeout: std::time::Duration::from_secs(
                crate::toolset::DEFAULT_COMMAND_TIMEOUT_SECS,
            ),
            command_timeout_max: None,
            root: Some(root.into()),
            policy: ToolPolicySurface::legacy_non_host_wide(
                FileToolMode::ReadWrite,
                BashMode::Unrestricted,
            ),
        }
    }

    pub fn with_cli_tool(mut self, tool: CliToolConfig) -> Self {
        self.cli_tools.push(tool);
        self.refresh_cli_policy();
        self
    }

    pub fn with_cli_tools<I>(mut self, tools: I) -> Self
    where
        I: IntoIterator<Item = CliToolConfig>,
    {
        self.cli_tools.extend(tools);
        self.refresh_cli_policy();
        self
    }

    pub fn with_command_timeout_secs(mut self, timeout_secs: u64) -> Self {
        self.command_timeout = std::time::Duration::from_secs(timeout_secs.max(1));
        self
    }

    /// Foreground cap for explicit `timeout_secs` requests (#1018). Unset ⇒
    /// the cap equals the default, i.e. the coupled #985 behavior.
    pub fn with_command_timeout_max_secs(mut self, timeout_secs: u64) -> Self {
        self.command_timeout_max = Some(std::time::Duration::from_secs(timeout_secs.max(1)));
        self
    }

    pub fn with_policy(mut self, policy: ToolPolicySurface) -> Self {
        self.file_tools = policy.file;
        self.bash = policy.bash.tool;
        self.policy = policy;
        self.refresh_cli_policy();
        self
    }

    pub fn file_tools(&self) -> FileToolMode {
        self.file_tools
    }

    pub fn bash(&self) -> BashMode {
        self.bash
    }

    pub fn cli_tools(&self) -> &[CliToolConfig] {
        &self.cli_tools
    }

    pub(crate) fn command_timeout(&self) -> std::time::Duration {
        self.command_timeout
    }

    pub(crate) fn command_timeout_max(&self) -> std::time::Duration {
        self.command_timeout_max
            .unwrap_or(self.command_timeout)
            .max(self.command_timeout)
    }

    pub fn root(&self) -> Option<&Path> {
        self.root.as_deref()
    }

    pub(crate) fn policy(&self) -> &ToolPolicySurface {
        &self.policy
    }

    fn refresh_cli_policy(&mut self) {
        self.policy.cli_tools =
            EndpointScope::<String, std::collections::BTreeSet<String>>::only_map(
                self.cli_tools
                    .iter()
                    .map(|tool| {
                        (
                            tool.name.trim().to_string(),
                            std::collections::BTreeSet::new(),
                        )
                    })
                    .collect(),
            );
    }
}

impl Default for ToolCeiling {
    fn default() -> Self {
        Self::meta_only()
    }
}
