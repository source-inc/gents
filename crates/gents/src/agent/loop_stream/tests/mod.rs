use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::llm::message::{AssistantContent, ToolResultContent, UserContent};
use crate::llm::tool::{BoxFuture, ToolDefinition, ToolDyn, ToolError};
use futures::{stream, Stream, StreamExt};
use rig::completion::{CompletionError, CompletionModel, CompletionRequest, CompletionResponse};

use crate::llm::message::Message;
use rig::streaming::{
    RawStreamingChoice, RawStreamingToolCall, StreamedAssistantContent, StreamedUserContent,
    StreamingCompletionResponse,
};
use tokio::sync::Mutex;

use super::*;
use crate::ensure_runtime_schemas;
use crate::hook::{DefraSessionHook, FailurePolicy};
use crate::test_support::first_content;

mod support;
use support::*;

include!("budgeting.rs");
include!("capture.rs");
include!("claude.rs");
include!("one_shot.rs");
include!("request_assembly.rs");
include!("retry.rs");
include!("streaming.rs");
include!("tool_execution.rs");
