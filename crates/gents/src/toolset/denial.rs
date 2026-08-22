use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::shared::{CommandExecutionMode, CommandNetworkMode};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum DenialReason {
    ForbiddenPrefix { matched: Vec<String> },
    AllowedPrefixRequired { argv: Vec<String> },
    ReadOnlyCommandNotAllowlisted { command: String },
    ReadOnlyArgumentNotAllowed { command: String, argument: String },
    ReadOnlySubcommandRequired { command: String },
    ReadOnlySubcommandNotAllowlisted { command: String, subcommand: String },
    ReadOnlyUrlRequired { command: String },
    DisabledNetworkUnenforceable,
    DisabledNetworkCommand { command: String },
    WorkspaceWriteSandboxUnavailable,
    WorkspaceExecutable,
    GitMetadataWriteDenied { command: String, subcommand: String },
}

impl DenialReason {
    pub(crate) fn to_contract(&self) -> &'static str {
        match self {
            Self::ForbiddenPrefix { .. } => "forbiddenPrefix",
            Self::AllowedPrefixRequired { .. } => "allowedPrefixRequired",
            Self::ReadOnlyCommandNotAllowlisted { .. } => "readOnlyCommandNotAllowlisted",
            Self::ReadOnlyArgumentNotAllowed { .. } => "readOnlyArgumentNotAllowed",
            Self::ReadOnlySubcommandRequired { .. } => "readOnlySubcommandRequired",
            Self::ReadOnlySubcommandNotAllowlisted { .. } => "readOnlySubcommandNotAllowlisted",
            Self::ReadOnlyUrlRequired { .. } => "readOnlyUrlRequired",
            Self::DisabledNetworkUnenforceable => "disabledNetworkUnenforceable",
            Self::DisabledNetworkCommand { .. } => "disabledNetworkCommand",
            Self::WorkspaceWriteSandboxUnavailable => "workspaceWriteSandboxUnavailable",
            Self::WorkspaceExecutable => "workspaceExecutable",
            Self::GitMetadataWriteDenied { .. } => "gitMetadataWriteDenied",
        }
    }

    pub(crate) fn matched_prefix(&self) -> Option<&[String]> {
        match self {
            Self::ForbiddenPrefix { matched } => Some(matched),
            _ => None,
        }
    }

    pub(crate) fn denied_argv(&self) -> Option<&[String]> {
        match self {
            Self::AllowedPrefixRequired { argv } => Some(argv),
            _ => None,
        }
    }

    pub(crate) fn denied_command(&self) -> Option<&str> {
        match self {
            Self::ReadOnlyCommandNotAllowlisted { command }
            | Self::ReadOnlyArgumentNotAllowed { command, .. }
            | Self::ReadOnlySubcommandRequired { command }
            | Self::ReadOnlySubcommandNotAllowlisted { command, .. }
            | Self::ReadOnlyUrlRequired { command }
            | Self::DisabledNetworkCommand { command }
            | Self::GitMetadataWriteDenied { command, .. } => Some(command),
            _ => None,
        }
    }

    pub(crate) fn denied_argument(&self) -> Option<&str> {
        match self {
            Self::ReadOnlyArgumentNotAllowed { argument, .. } => Some(argument),
            _ => None,
        }
    }

    pub(crate) fn denied_subcommand(&self) -> Option<&str> {
        match self {
            Self::ReadOnlySubcommandNotAllowlisted { subcommand, .. }
            | Self::GitMetadataWriteDenied { subcommand, .. } => Some(subcommand),
            _ => None,
        }
    }

    pub(crate) fn diagnostic(&self) -> String {
        match self {
            Self::ForbiddenPrefix { matched } => format!(
                "command is forbidden by command execution policy prefix: {}",
                shell_join_display(matched)
            ),
            Self::AllowedPrefixRequired { argv } => format!(
                "command is not allowed by command execution policy prefixes: {}",
                shell_join_display(argv)
            ),
            Self::ReadOnlyCommandNotAllowlisted { command } => {
                format!("command is not allowed by the read-only bash tool: {command}")
            }
            Self::ReadOnlyArgumentNotAllowed { command, argument } => {
                match (command.as_str(), argument.as_str()) {
                    ("sed", "-i" | "--in-place") => "sed in-place edits are not allowed".into(),
                    ("sed", arg) if arg.starts_with("-i") || arg.starts_with("--in-place=") => {
                        "sed in-place edits are not allowed".into()
                    }
                    ("find", _) => {
                        "find arguments that can write or execute are not allowed".into()
                    }
                    ("sudo", arg) if arg.ends_with("launchctl") => {
                        "sudo launchctl must use the absolute /bin/launchctl path".into()
                    }
                    ("git", arg)
                        if arg == "-C"
                            || arg == "-c"
                            || arg.starts_with("-C")
                            || arg.starts_with("-c")
                            || arg.starts_with("--config-env")
                            || arg.starts_with("--exec-path")
                            || arg.starts_with("--git-dir")
                            || arg.starts_with("--namespace")
                            || arg.starts_with("--super-prefix")
                            || arg.starts_with("--work-tree") =>
                    {
                        "git global options that redirect config or helper lookup are not allowed"
                            .into()
                    }
                    ("git", arg) if arg == "-D" || arg.starts_with("--format=") => {
                        format!("git branch argument is not read-only: {argument}")
                    }
                    ("git", _) => {
                        format!(
                            "git argument is not allowed by the read-only bash tool: {argument}"
                        )
                    }
                    ("rg", _) => {
                        format!("rg argument is not allowed by the read-only bash tool: {argument}")
                    }
                    ("curl", _) => {
                        format!(
                            "curl argument is not allowed by the read-only bash tool: {argument}"
                        )
                    }
                    _ => format!(
                        "{command} argument is not allowed by the read-only bash tool: {argument}"
                    ),
                }
            }
            Self::ReadOnlySubcommandRequired { command } => match command.as_str() {
                "sudo" => "sudo requires an approved command".into(),
                _ => format!("{command} requires a read-only subcommand"),
            },
            Self::ReadOnlySubcommandNotAllowlisted {
                command,
                subcommand,
            } => match command.as_str() {
                "sudo" => {
                    format!("sudo command is not allowed by the read-only bash tool: {subcommand}")
                }
                _ => format!(
                    "{command} subcommand is not allowed by the read-only bash tool: {subcommand}"
                ),
            },
            Self::ReadOnlyUrlRequired { command } => {
                format!("{command} requires an http:// or https:// URL in the read-only bash tool")
            }
            Self::DisabledNetworkUnenforceable => {
                "command_network_mode=disabled cannot be enforced for unrestricted bash".into()
            }
            Self::DisabledNetworkCommand { command } => match command.as_str() {
                "tailscale" => {
                    "tailscale network probes are not allowed when command_network_mode=disabled"
                        .into()
                }
                _ => format!("{command} is not allowed when command_network_mode=disabled"),
            },
            Self::WorkspaceWriteSandboxUnavailable => {
                if cfg!(target_os = "macos") {
                    "macOS sandbox-exec is required for workspace_write bash but was not found"
                        .into()
                } else {
                    "workspace_write bash requires macOS seatbelt sandbox enforcement on this build"
                        .into()
                }
            }
            Self::WorkspaceExecutable => {
                "language-server executable is not admitted (workspace-local or missing)".into()
            }
            Self::GitMetadataWriteDenied {
                command,
                subcommand,
            } => format!(
                "{command} {subcommand} is denied under WorkspaceWrite+git_worktree_diff (shared Git metadata is integrator-only)"
            ),
        }
    }

    pub(crate) fn from_contract_fields(
        reason: &str,
        matched_prefix: Option<Vec<String>>,
        denied_argv: Option<Vec<String>>,
        denied_command: Option<String>,
        denied_argument: Option<String>,
        denied_subcommand: Option<String>,
    ) -> Option<Self> {
        match reason {
            "forbiddenPrefix" => Some(Self::ForbiddenPrefix {
                matched: matched_prefix?,
            }),
            "allowedPrefixRequired" => Some(Self::AllowedPrefixRequired { argv: denied_argv? }),
            "readOnlyCommandNotAllowlisted" => Some(Self::ReadOnlyCommandNotAllowlisted {
                command: denied_command?,
            }),
            "readOnlyArgumentNotAllowed" => Some(Self::ReadOnlyArgumentNotAllowed {
                command: denied_command?,
                argument: denied_argument?,
            }),
            "readOnlySubcommandRequired" => Some(Self::ReadOnlySubcommandRequired {
                command: denied_command?,
            }),
            "readOnlySubcommandNotAllowlisted" => Some(Self::ReadOnlySubcommandNotAllowlisted {
                command: denied_command?,
                subcommand: denied_subcommand?,
            }),
            "readOnlyUrlRequired" => Some(Self::ReadOnlyUrlRequired {
                command: denied_command?,
            }),
            "disabledNetworkUnenforceable" => Some(Self::DisabledNetworkUnenforceable),
            "disabledNetworkCommand" => Some(Self::DisabledNetworkCommand {
                command: denied_command?,
            }),
            "workspaceWriteSandboxUnavailable" => Some(Self::WorkspaceWriteSandboxUnavailable),
            "workspaceExecutable" => Some(Self::WorkspaceExecutable),
            "gitMetadataWriteDenied" => Some(Self::GitMetadataWriteDenied {
                command: denied_command?,
                subcommand: denied_subcommand?,
            }),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandPolicyDenial {
    pub(crate) reason: DenialReason,
    pub(crate) policy_mode: String,
    pub(crate) policy_network: String,
}

impl CommandPolicyDenial {
    pub(crate) fn new(
        reason: DenialReason,
        mode: CommandExecutionMode,
        network_mode: CommandNetworkMode,
    ) -> Self {
        Self {
            reason,
            policy_mode: mode.as_str().to_string(),
            policy_network: network_mode.as_str().to_string(),
        }
    }

    pub(crate) fn to_contract(&self) -> &'static str {
        self.reason.to_contract()
    }

    pub(crate) fn diagnostic(&self) -> String {
        self.reason.diagnostic()
    }

    pub(crate) fn payload_value(&self) -> Value {
        json!({
            "ok": false,
            "failure_class": "policyDenied",
            "denial_reason": self.reason.to_contract(),
            "denied_argv": self.reason.denied_argv(),
            "denied_command": self.reason.denied_command(),
            "denied_argument": self.reason.denied_argument(),
            "denied_subcommand": self.reason.denied_subcommand(),
            "denied_prefix": self.reason.matched_prefix(),
            "policy_mode": self.policy_mode,
            "policy_network": self.policy_network,
            "message": self.diagnostic(),
        })
    }

    pub(crate) fn tool_error_payload(&self) -> String {
        serde_json::to_string(&self.payload_value()).unwrap_or_else(|_| self.diagnostic())
    }

    pub(crate) fn from_payload_value(value: &Value) -> Option<Self> {
        let reason = value.get("denial_reason")?.as_str()?;
        let matched_prefix = string_vec_field(value, "denied_prefix");
        let denied_argv = string_vec_field(value, "denied_argv");
        let denied_command = string_field(value, "denied_command");
        let denied_argument = string_field(value, "denied_argument");
        let denied_subcommand = string_field(value, "denied_subcommand");
        let reason = DenialReason::from_contract_fields(
            reason,
            matched_prefix,
            denied_argv,
            denied_command,
            denied_argument,
            denied_subcommand,
        )?;
        Some(Self {
            reason,
            policy_mode: string_field(value, "policy_mode").unwrap_or_default(),
            policy_network: string_field(value, "policy_network").unwrap_or_default(),
        })
    }
}

fn string_field(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn string_vec_field(value: &Value, key: &str) -> Option<Vec<String>> {
    value.get(key).and_then(Value::as_array).map(|items| {
        items
            .iter()
            .filter_map(Value::as_str)
            .map(ToOwned::to_owned)
            .collect()
    })
}

fn shell_join_display(argv: &[String]) -> String {
    argv.join(" ")
}
