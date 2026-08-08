use anyhow::{anyhow, Result};
use gents::graphql::escape_graphql_string;
use gents::template::{render_template, task_node_ctx, TemplateScope};
use serde::Serialize;
use serde_json::Value;

use crate::cli::ConfigTaskRunArgs;
use crate::config_writes::ConfigAccess;
use crate::request_helpers::{
    content_and_metadata_with_prompt_selected_skill_ids, wait_for_terminal_response,
};
use crate::{print_json, resolve_config_access, resolve_graphql_endpoint};

const DEFAULT_REQUEST_MAX_RETRIES: u32 = 3;

pub(crate) async fn config_task_run(args: ConfigTaskRunArgs) -> Result<()> {
    let output = enqueue_task_run(&args).await?;
    let mut value = serde_json::to_value(&output)?;
    if args.wait {
        let graphql = resolve_graphql_endpoint(args.graphql.as_deref(), args.home.as_deref())?;
        let response = wait_for_terminal_response(
            &graphql,
            &output.request_id,
            args.timeout_secs,
            args.poll_secs,
        )
        .await?;
        if let Some(object) = value.as_object_mut() {
            object.insert(
                "wait".to_string(),
                serde_json::json!({
                    "timeout_secs": args.timeout_secs,
                    "poll_secs": args.poll_secs,
                    "response": response,
                }),
            );
        }
    }
    print_json(&value)?;
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TaskRunOutput {
    pub(crate) task_id: String,
    pub(crate) behavior_id: String,
    pub(crate) agent_did: String,
    pub(crate) request_id: String,
    pub(crate) session_id: String,
    pub(crate) request_doc_id: String,
    pub(crate) metadata: Option<String>,
    pub(crate) status: &'static str,
}

pub(crate) async fn enqueue_task_run(args: &ConfigTaskRunArgs) -> Result<TaskRunOutput> {
    let task_id =
        resolve_task_id_for("run", args.task_id.as_deref(), args.task_id_flag.as_deref())?;

    let args_value: Value =
        serde_json::from_str(&args.args).map_err(|e| anyhow!("--args is not valid JSON: {e}"))?;
    if !args_value.is_object() {
        anyhow::bail!("--args must be a JSON object (got: {args_value})");
    }

    let (access, _) = resolve_config_access(args.home.as_deref(), args.graphql.as_deref()).await?;
    let source_author_did = access
        .node_identity_did()
        .await
        .map_err(|error| anyhow!("task run requires a signed database endpoint: {error}"))?;

    let task_query = format!(
        r#"query {{
            Task(filter: {{ task_id: {{ _eq: "{id}" }} }}, limit: 1) {{
                task_id
                behavior_id
                prompt_template
                enabled
            }}
        }}"#,
        id = escape_graphql_string(&task_id),
    );
    let task_response = access.execute(&task_query).await?;
    let task_row = task_response
        .get("data")
        .and_then(|d| d.get("Task"))
        .and_then(|arr| arr.as_array())
        .and_then(|arr| arr.first())
        .ok_or_else(|| anyhow!("no Task with task_id = {}", task_id))?;
    let behavior_id = task_row
        .get("behavior_id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("Task {} has no behavior_id", task_id))?
        .to_string();
    let prompt_template = task_row
        .get("prompt_template")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let enabled = task_row
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !enabled {
        anyhow::bail!("Task {} is disabled; cannot run", task_id);
    }

    let behavior_query = format!(
        r#"query {{
            AgentBehavior(filter: {{ behavior_id: {{ _eq: "{id}" }} }}, limit: 1) {{
                agent_did
                enabled
            }}
        }}"#,
        id = escape_graphql_string(&behavior_id),
    );
    let behavior_response = access.execute(&behavior_query).await?;
    let behavior_row = behavior_response
        .get("data")
        .and_then(|d| d.get("AgentBehavior"))
        .and_then(|arr| arr.as_array())
        .and_then(|arr| arr.first())
        .ok_or_else(|| {
            anyhow!(
                "no AgentBehavior with behavior_id = {} (referenced by task {})",
                behavior_id,
                task_id
            )
        })?;
    let agent_did = behavior_row
        .get("agent_did")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("AgentBehavior {} has no agent_did", behavior_id))?
        .to_string();
    if !behavior_row
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        anyhow::bail!("AgentBehavior {} is disabled", behavior_id);
    }

    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let (node_scope, ctx_scope) = task_node_ctx(&agent_did, &behavior_id, &now);
    let scope = TemplateScope {
        event: serde_json::json!({
            "fired_at": now,
            "trigger_id": serde_json::Value::Null,
            "trigger_kind": "manual",
        }),
        doc: None,
        args: Some(args_value),
        node: node_scope,
        ctx: ctx_scope,
    };
    let content = render_template(&prompt_template, &scope)
        .map_err(|e| anyhow!("render manual template for task {}: {e}", task_id))?;

    let request_id = uuid::Uuid::new_v4().to_string();
    let session_id = uuid::Uuid::new_v4().to_string();
    let (content, metadata) = content_and_metadata_with_prompt_selected_skill_ids(None, &content);
    let mutation = build_create_manual_request_mutation(CreateManualRequestInput {
        request_id: &request_id,
        session_id: &session_id,
        agent_did: &agent_did,
        source_author_did: &source_author_did,
        behavior_id: &behavior_id,
        content: &content,
        metadata: metadata.as_deref(),
        created_at: &now,
    });
    let response = access.execute(&mutation).await?;
    if let Some(errs) = response.get("errors").and_then(|v| v.as_array()) {
        if !errs.is_empty() {
            anyhow::bail!("create manual AgentRequest failed: {errs:?}");
        }
    }
    let doc_id = match extract_doc_id(&response) {
        Some(doc_id) => doc_id,
        None => lookup_doc_id_by_request_id(&access, &request_id)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "manual AgentRequest for task {} persisted but _docID lookup by request_id returned nothing",
                    task_id
                )
            })?,
    };

    Ok(TaskRunOutput {
        task_id,
        behavior_id,
        agent_did,
        request_id,
        session_id,
        request_doc_id: doc_id,
        metadata,
        status: "pending",
    })
}

pub(crate) fn resolve_task_id_for(
    command: &str,
    positional: Option<&str>,
    flag: Option<&str>,
) -> Result<String> {
    let positional = positional.map(str::trim).filter(|value| !value.is_empty());
    let flag = flag.map(str::trim).filter(|value| !value.is_empty());
    match (positional, flag) {
        (Some(positional), Some(flag)) if positional != flag => {
            anyhow::bail!(
                "conflicting task ids provided: positional={} and --task-id={}\nNext:\n  1. Pass the task id once: `gents task {command} TASK_ID`\n  2. Or use `--task-id TASK_ID`, but not both",
                positional,
                flag
            );
        }
        (Some(task_id), _) | (_, Some(task_id)) => Ok(task_id.to_string()),
        (None, None) => anyhow::bail!(
            "missing task id\nNext:\n  1. Pass it positionally: `gents task {command} TASK_ID`\n  2. Or use `--task-id TASK_ID`"
        ),
    }
}

struct CreateManualRequestInput<'a> {
    request_id: &'a str,
    session_id: &'a str,
    agent_did: &'a str,
    source_author_did: &'a str,
    behavior_id: &'a str,
    content: &'a str,
    metadata: Option<&'a str>,
    created_at: &'a str,
}

fn build_create_manual_request_mutation(input: CreateManualRequestInput<'_>) -> String {
    let metadata_field = input
        .metadata
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|metadata| {
            format!(
                r#"
                metadata: "{}","#,
                escape_graphql_string(metadata)
            )
        })
        .unwrap_or_default();
    format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{request_id}",
                agent_did: "{agent_did}",
                source_author_did: "{source_author_did}",
                behavior_id: "{behavior_id}",
                session_id: "{session_id}",
                retry_parent_request: "",
                retry_root_request: "{request_id}",
                superseded_by_request: "",
                content: "{content}",{metadata_field}
                status: "pending",
                lifecycle_state: "pending",
                backend_id: "",
                execution_origin: "interactive",
                caused_by_trigger_kind: "manual",
                failure_reason: "",
                created_at: "{created_at}",
                retry_count: 0,
                max_retries: {max_retries}
            }}) {{ _docID }}
        }}"#,
        request_id = escape_graphql_string(input.request_id),
        agent_did = escape_graphql_string(input.agent_did),
        source_author_did = escape_graphql_string(input.source_author_did),
        behavior_id = escape_graphql_string(input.behavior_id),
        session_id = escape_graphql_string(input.session_id),
        content = escape_graphql_string(input.content),
        metadata_field = metadata_field,
        created_at = escape_graphql_string(input.created_at),
        max_retries = DEFAULT_REQUEST_MAX_RETRIES,
    )
}

async fn lookup_doc_id_by_request_id(
    access: &ConfigAccess,
    request_id: &str,
) -> Result<Option<String>> {
    let query = format!(
        r#"query {{
            AgentRequest(filter: {{ request_id: {{ _eq: "{id}" }} }}, limit: 1) {{
                _docID
            }}
        }}"#,
        id = escape_graphql_string(request_id),
    );
    let response = access.execute(&query).await?;
    if let Some(errs) = response.get("errors").and_then(|v| v.as_array()) {
        if !errs.is_empty() {
            anyhow::bail!("lookup AgentRequest by request_id {request_id} failed: {errs:?}");
        }
    }
    Ok(response
        .get("data")
        .and_then(|d| d.get("AgentRequest"))
        .and_then(|arr| arr.as_array())
        .and_then(|arr| arr.first())
        .and_then(|row| row.get("_docID"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string()))
}

fn extract_doc_id(response: &Value) -> Option<String> {
    let data = response.get("data")?;
    let candidates = [
        data.get("create_AgentRequest"),
        data.get("add_AgentRequest"),
    ];
    for value in candidates.into_iter().flatten() {
        if let Some(doc_id) = value.get("_docID").and_then(|v| v.as_str()) {
            return Some(doc_id.to_string());
        }
        if let Some(doc_id) = value
            .as_array()
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("_docID"))
            .and_then(|v| v.as_str())
        {
            return Some(doc_id.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_mutation_includes_manual_lineage_and_omits_trigger_id() {
        let mutation = build_create_manual_request_mutation(CreateManualRequestInput {
            request_id: "req-1",
            session_id: "sess-1",
            agent_did: "did:test:test",
            source_author_did: "did:test:node",
            behavior_id: "behavior-1",
            content: "hello Amy",
            metadata: None,
            created_at: "2026-04-21T00:00:00Z",
        });
        assert!(mutation.contains("caused_by_trigger_kind: \"manual\""));
        assert!(
            !mutation.contains("caused_by_trigger_id:"),
            "caused_by_trigger_id must be omitted so it stays null for manual runs"
        );
        assert!(mutation.contains("execution_origin: \"interactive\""));
        assert!(mutation.contains("lifecycle_state: \"pending\""));
        assert!(mutation.contains("status: \"pending\""));
        assert!(mutation.contains("content: \"hello Amy\""));
        assert!(mutation.contains("source_author_did: \"did:test:node\""));
    }

    #[test]
    fn build_mutation_includes_selected_skill_metadata_when_present() {
        let mutation = build_create_manual_request_mutation(CreateManualRequestInput {
            request_id: "req-1",
            session_id: "sess-1",
            agent_did: "did:test:test",
            source_author_did: "did:test:node",
            behavior_id: "behavior-1",
            content: "/vuln-scan /work",
            metadata: Some(r#"{"selected_skill_ids":["vuln-scan"]}"#),
            created_at: "2026-04-21T00:00:00Z",
        });

        assert!(mutation.contains("metadata:"));
        assert!(mutation.contains(r#"\"selected_skill_ids\":[\"vuln-scan\"]"#));
    }

    #[test]
    fn extract_doc_id_handles_object_and_array_shapes() {
        let object_shape = serde_json::json!({
            "data": { "create_AgentRequest": { "_docID": "doc-1" } }
        });
        assert_eq!(extract_doc_id(&object_shape), Some("doc-1".to_string()));

        let array_shape = serde_json::json!({
            "data": { "create_AgentRequest": [ { "_docID": "doc-2" } ] }
        });
        assert_eq!(extract_doc_id(&array_shape), Some("doc-2".to_string()));

        let empty = serde_json::json!({
            "data": { "create_AgentRequest": [] }
        });
        assert_eq!(extract_doc_id(&empty), None);
    }

    #[test]
    fn extract_doc_id_returns_none_when_response_omits_doc_id_entirely() {
        let object_without_doc_id = serde_json::json!({
            "data": { "create_AgentRequest": {} }
        });
        assert_eq!(extract_doc_id(&object_without_doc_id), None);

        let array_without_doc_id = serde_json::json!({
            "data": { "create_AgentRequest": [ {} ] }
        });
        assert_eq!(extract_doc_id(&array_without_doc_id), None);

        let missing_field = serde_json::json!({ "data": {} });
        assert_eq!(extract_doc_id(&missing_field), None);
    }

    #[test]
    fn extract_doc_id_handles_add_alias_response() {
        let add_object = serde_json::json!({
            "data": { "add_AgentRequest": { "_docID": "doc-3" } }
        });
        assert_eq!(extract_doc_id(&add_object), Some("doc-3".to_string()));

        let add_array = serde_json::json!({
            "data": { "add_AgentRequest": [ { "_docID": "doc-4" } ] }
        });
        assert_eq!(extract_doc_id(&add_array), Some("doc-4".to_string()));
    }

    #[test]
    fn resolve_task_id_accepts_positional_or_flag_and_rejects_conflict() {
        assert_eq!(
            resolve_task_id_for("run", Some("host-check"), None).unwrap(),
            "host-check"
        );
        assert_eq!(
            resolve_task_id_for("run", None, Some("host-check")).unwrap(),
            "host-check"
        );
        assert_eq!(
            resolve_task_id_for("run", Some("host-check"), Some("host-check")).unwrap(),
            "host-check"
        );
        assert!(resolve_task_id_for("run", Some("host-check"), Some("other")).is_err());
        assert!(resolve_task_id_for("run", None, None).is_err());
    }
}
