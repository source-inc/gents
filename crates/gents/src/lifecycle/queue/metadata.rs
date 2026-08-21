#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RequestQueueMetadata {
    pub queue: QueueHints,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background_completion_wake_version: Option<u32>,
}

const BACKGROUND_COMPLETION_WAKE_VERSION: u32 = 1;
const STEERING_INPUT_MESSAGE_PREFIX: &str = "steering-input:";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct QueueHints {
    pub source: QueueSource,
    pub policy: QueuePolicy,
    pub key: Option<String>,
    pub queued_after_request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interrupted_request_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueSource {
    User,
    #[serde(alias = "subagent_completion")]
    BackgroundCompletion,
    Steering,
    Goal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum QueuePolicy {
    Append,
    Coalesce,
}

pub(crate) fn parse_queue_hints(metadata: Option<&str>) -> Option<QueueHints> {
    parse_queue_metadata(metadata).map(|metadata| metadata.queue)
}

fn parse_queue_metadata(metadata: Option<&str>) -> Option<RequestQueueMetadata> {
    let metadata = metadata?.trim();
    if metadata.is_empty() {
        return None;
    }
    if metadata.contains("\"subagent_completion\"") {
        tracing::warn!(
            "parsed deprecated queue source alias subagent_completion as background_completion"
        );
    }

    serde_json::from_str::<RequestQueueMetadata>(metadata).ok()
}

pub(crate) fn queue_metadata_json(hints: &QueueHints) -> String {
    let background_completion_wake_version =
        queue_hints_are_automated_wakeup(hints).then_some(BACKGROUND_COMPLETION_WAKE_VERSION);
    serde_json::to_string(&RequestQueueMetadata {
        queue: hints.clone(),
        background_completion_wake_version,
    })
    .expect("queue metadata serialization should not fail")
}

pub(crate) fn is_automated_wakeup(metadata: Option<&str>) -> bool {
    parse_queue_hints(metadata).is_some_and(|hints| queue_hints_are_automated_wakeup(&hints))
}

fn queue_hints_are_automated_wakeup(hints: &QueueHints) -> bool {
    matches!(hints.source, QueueSource::BackgroundCompletion)
        && hints.policy == QueuePolicy::Coalesce
        && hints
            .key
            .as_deref()
            .is_some_and(|key| !key.trim().is_empty())
}

pub(crate) fn is_deprecated_background_completion_wakeup(
    execution_origin: Option<&str>,
    metadata: Option<&str>,
) -> bool {
    if execution_origin != Some("scheduled") {
        return false;
    }
    parse_queue_metadata(metadata).is_some_and(|metadata| {
        queue_hints_are_automated_wakeup(&metadata.queue)
            && metadata.background_completion_wake_version
                != Some(BACKGROUND_COMPLETION_WAKE_VERSION)
    })
}

pub(crate) fn is_subagent_owned_queue(metadata: Option<&str>) -> bool {
    parse_queue_hints(metadata).is_some_and(|hints| {
        matches!(hints.source, QueueSource::Steering)
            || (matches!(hints.source, QueueSource::BackgroundCompletion)
                && hints.policy == QueuePolicy::Coalesce
                && hints
                    .key
                    .as_deref()
                    .is_some_and(|key| !key.trim().is_empty()))
    })
}

pub(crate) fn is_goal_queue(metadata: Option<&str>) -> bool {
    parse_queue_hints(metadata).is_some_and(|hints| matches!(hints.source, QueueSource::Goal))
}

pub(crate) fn steering_input_message_key(request_id: &str) -> String {
    format!("{STEERING_INPUT_MESSAGE_PREFIX}{request_id}")
}

pub(crate) fn is_steering_input_message_key(message_key: &str) -> bool {
    message_key.starts_with(STEERING_INPUT_MESSAGE_PREFIX)
}
