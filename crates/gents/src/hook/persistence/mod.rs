use std::time::Duration;

use crate::llm::message::{Message, Text, ToolResult, ToolResultContent, UserContent};
use crate::llm::{HookAction, ToolCallHookAction};
use serde::Deserialize;
use serde_json::json;
use tracing::Instrument;

use crate::background_tools::r4c_args::{
    ListBackgroundToolsArgs, ListSubagentsArgs, ReadSubagentArgs, ReadToolOutputArgs,
    SteerSubagentArgs,
};
use crate::background_tools::{
    active_session_request_id, append_steering_request, child_request_completed,
    context_allowed_target_names, drain_automated_wakeups_returning_ids,
    effective_context_cross_deployment_spawn_timeout_seconds, handle_list_background_tools,
    handle_list_subagents, handle_read_subagent, handle_read_tool_output,
    load_authorized_child_edge, load_child_final_response, load_child_terminal_row,
    load_parent_subagent_context, load_steer_subagent_target, pending_automated_wakeup_request_ids,
    project_child_terminal, resolve_context_target, try_load_authorized_child_edge,
    BackgroundToolArgs, CancelSubagentArgs, CancelToolArgs, ChildEdge, ParentSubagentContext,
    ProcessControlScope, ReadToolOutputOutcome, SpawnSubagentArgs, SteerSubagentTarget,
    WaitSubagentArgs, WaitToolArgs,
};
use crate::config::DEFAULT_DEADLINE_DURATION_SECS;
use crate::descendant_graph::{
    resolve_descendant_edge, resolve_descendant_root_request_id, DescendantGraphAccess,
};
use crate::document_config::{load_agent_behavior, SubagentTarget};
use crate::session;
use crate::tool_call_lifecycle::query::load_tool_call_result;
use crate::tool_call_lifecycle::subagent_workspace::{
    merge_workspace_lineage, resolve_spawn_workspace, ParentWorkspaceStamp, SpawnWorkspaceError,
};
use crate::tool_call_lifecycle::{
    AwaitMode, CancelCause, CancelPolicy, CascadeDispatch, ChildTerminal, FailureClass,
    ToolCallLifecycle, MAX_SUBAGENT_DEPTH,
};
use crate::toolset::{
    CANCEL_PROCESS_TOOL_NAME, CANCEL_SUBAGENT_TOOL_NAME, LIST_PROCESSES_TOOL_NAME,
    LIST_SUBAGENTS_TOOL_NAME, READ_PROCESS_TOOL_NAME, READ_SUBAGENT_TOOL_NAME,
    SPAWN_PROCESS_TOOL_NAME, SPAWN_SUBAGENT_TOOL_NAME, STEER_SUBAGENT_TOOL_NAME,
    WAIT_PROCESS_TOOL_NAME, WAIT_SUBAGENT_TOOL_NAME,
};
use crate::truncation::{truncate_text, DefraSpillTruncator, TruncationMode, Truncator};

use super::{non_empty, DefraSessionHook, TranscriptTurnState};

pub(crate) const MAX_BACKGROUNDED_TOOLS_PER_PARENT: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubagentTargetHost {
    Local,
    Remote,
}

mod approval;
mod background_tools;
mod goal_tools;
mod helpers;
mod message_spawn;
mod prompt_hook;
mod subagent_bridge;
mod subagent_tools;

use helpers::*;

impl DefraSessionHook {
    pub(super) fn skip_tool_result(
        &self,
        tool_name: &str,
        result: impl Into<String>,
    ) -> ToolCallHookAction {
        let result = result.into();
        ToolCallHookAction::skip(bounded_tool_result_for_model(
            tool_name,
            &result,
            &self.truncation_limits,
        ))
    }
}
