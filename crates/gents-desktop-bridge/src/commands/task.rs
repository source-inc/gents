use anyhow::{anyhow, bail, Result};
use gents::graphql::escape_graphql_string;
use gents_desktop_core::client::ClientCore;
use gents_protocol::row::{AgentRequestRow, EventTriggerRow, ScheduleRow, TaskRow};

use super::super::types::{
    EventTriggerSaveRequest, ScheduleRunRequest, ScheduleSaveRequest, TaskRunRequest,
    TaskRunResult, TaskSaveRequest,
};
use super::util::{require_trimmed, trim_optional};

fn validate_event_kind(event_kind: &str) -> Result<()> {
    if event_kind == "created" {
        Ok(())
    } else {
        bail!("event_kind currently supports only created")
    }
}

async fn load_agent_request_by_doc_id(
    core: &ClientCore,
    request_doc_id: &str,
) -> Result<AgentRequestRow> {
    let escaped_doc_id = escape_graphql_string(request_doc_id);
    let query = format!(
        r#"{{
            AgentRequest(filter: {{ _docID: {{ _eq: "{escaped_doc_id}" }} }}, limit: 1) {{
                request_id
                agent_did
                behavior_id
                session_id
                status
                lifecycle_state
            }}
        }}"#
    );
    let response = core.node().execute(&query).await;
    if response.has_errors() {
        bail!(
            "query manual task run request failed: {:?}",
            response.errors
        );
    }

    let row = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|rows| rows.as_array())
        .and_then(|rows| rows.first())
        .cloned()
        .ok_or_else(|| anyhow!("manual task run request {request_doc_id} was not found"))?;
    serde_json::from_value(row).map_err(Into::into)
}

pub async fn save_task_config(core: &ClientCore, request: TaskSaveRequest) -> Result<()> {
    let task_id = require_trimmed("task_id", request.task_id)?;
    let name = require_trimmed("name", request.name)?;
    let behavior_id = require_trimmed("behavior_id", request.behavior_id)?;
    let prompt_template = require_trimmed("prompt_template", request.prompt_template)?;

    let store = core.store().snapshot();
    let mut row = store
        .tasks
        .iter()
        .find(|row| row.task_id == task_id)
        .cloned()
        .unwrap_or_else(|| TaskRow {
            task_id: task_id.clone(),
            name: None,
            description: None,
            behavior_id: None,
            prompt_template: None,
            enabled: Some(true),
            output_schema_ref: None,
            created_at: None,
            updated_at: None,
        });
    row.name = Some(name);
    row.description = trim_optional(request.description);
    row.behavior_id = Some(behavior_id);
    row.prompt_template = Some(prompt_template);
    row.enabled = request.enabled.or(row.enabled).or(Some(true));
    row.output_schema_ref = trim_optional(request.output_schema_ref);
    core.save_task(&row).await?;
    Ok(())
}

pub async fn save_schedule_config(core: &ClientCore, request: ScheduleSaveRequest) -> Result<()> {
    let schedule_id = require_trimmed("schedule_id", request.schedule_id)?;
    let task_id = require_trimmed("task_id", request.task_id)?;

    let store = core.store().snapshot();
    let mut row = store
        .schedules
        .iter()
        .find(|row| row.schedule_id == schedule_id)
        .cloned()
        .unwrap_or_else(|| ScheduleRow {
            schedule_id: schedule_id.clone(),
            task_id: Some(task_id.clone()),
            interval_secs: None,
            cron: None,
            timezone: None,
            missed_run_policy: None,
            enabled: Some(true),
            concurrency: Some("serial".to_string()),
            next_run_at: None,
            last_attempt_at: None,
            last_status: None,
            last_error: None,
            fire_count: None,
            created_at: None,
            updated_at: None,
        });
    row.task_id = Some(task_id);
    row.interval_secs = request.interval_secs;
    row.cron = trim_optional(request.cron);
    row.timezone = trim_optional(request.timezone);
    row.missed_run_policy = trim_optional(request.missed_run_policy);
    row.enabled = request.enabled.or(row.enabled).or(Some(true));
    row.concurrency = trim_optional(request.concurrency).or_else(|| Some("serial".to_string()));
    core.save_schedule(&row).await?;
    Ok(())
}

pub async fn run_schedule_config(
    core: &ClientCore,
    request: ScheduleRunRequest,
) -> Result<TaskRunResult> {
    let schedule_id = require_trimmed("schedule_id", request.schedule_id)?;
    let store = core.store().snapshot();
    let schedule = store
        .schedules
        .iter()
        .find(|row| row.schedule_id == schedule_id)
        .cloned()
        .ok_or_else(|| anyhow!("schedule {schedule_id} was not found"))?;
    let request_doc_id = core.fire_schedule_now(&schedule).await?;
    let row = load_agent_request_by_doc_id(core, &request_doc_id).await?;

    Ok(TaskRunResult {
        request_doc_id,
        request_id: row.request_id,
        session_id: row.session_id.unwrap_or_default(),
        agent_did: row.agent_did.unwrap_or_default(),
        behavior_id: row.behavior_id.unwrap_or_default(),
        status: row.status,
        lifecycle_state: row.lifecycle_state,
    })
}

pub async fn save_event_trigger_config(
    core: &ClientCore,
    request: EventTriggerSaveRequest,
) -> Result<()> {
    let trigger_id = require_trimmed("trigger_id", request.trigger_id)?;
    let task_id = require_trimmed("task_id", request.task_id)?;
    let source_collection = require_trimmed("source_collection", request.source_collection)?;
    let event_kind = require_trimmed("event_kind", request.event_kind)?;
    validate_event_kind(&event_kind)?;

    let store = core.store().snapshot();
    let mut row = store
        .event_triggers
        .iter()
        .find(|row| row.trigger_id == trigger_id)
        .cloned()
        .unwrap_or_else(|| EventTriggerRow {
            trigger_id: trigger_id.clone(),
            task_id: Some(task_id.clone()),
            source_collection: None,
            event_kind: None,
            filter: None,
            enabled: Some(true),
            concurrency: Some("serial".to_string()),
            correlation_field: None,
            fire_mode: None,
            expected_count: None,
            expected_count_field: None,
            group_timeout_secs: None,
            group_min_count: None,
            workspace_authority: None,
            created_at: None,
            updated_at: None,
            last_attempt_at: None,
            last_fired_source_doc_id: None,
            last_status: None,
            last_error: None,
            fire_count: None,
        });
    row.task_id = Some(task_id);
    row.source_collection = Some(source_collection);
    row.event_kind = Some(event_kind);
    row.filter = trim_optional(request.filter);
    row.enabled = request.enabled.or(row.enabled).or(Some(true));
    row.concurrency = trim_optional(request.concurrency).or_else(|| Some("serial".to_string()));
    core.save_event_trigger(&row).await?;
    Ok(())
}

pub async fn run_task_config(core: &ClientCore, request: TaskRunRequest) -> Result<TaskRunResult> {
    let task_id = require_trimmed("task_id", request.task_id)?;
    let args = request.args.unwrap_or_else(|| serde_json::json!({}));
    let store = core.store().snapshot();
    let task = store
        .tasks
        .iter()
        .find(|row| row.task_id == task_id)
        .cloned()
        .ok_or_else(|| anyhow!("task {task_id} was not found"))?;
    let request_doc_id = core.fire_task_now(&task, args).await?;
    let row = load_agent_request_by_doc_id(core, &request_doc_id).await?;

    Ok(TaskRunResult {
        request_doc_id,
        request_id: row.request_id,
        session_id: row.session_id.unwrap_or_default(),
        agent_did: row.agent_did.unwrap_or_default(),
        behavior_id: row.behavior_id.unwrap_or_default(),
        status: row.status,
        lifecycle_state: row.lifecycle_state,
    })
}
