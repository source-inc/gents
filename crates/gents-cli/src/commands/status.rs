use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};
use gents::graphql::escape_graphql_string;
use serde_json::{json, Value};

use crate::cli::args::StatusArgs;
use crate::config_writes::ConfigAccess;
use crate::shared::ConfigExportBundle;
use crate::{
    build_config_export_bundle, normalize_optional_string, post_graphql, print_json,
    read_runtime_state, resolve_agent_did, resolve_graphql_endpoint, resolve_home_dir,
};

pub(crate) async fn status(args: StatusArgs) -> Result<()> {
    let graphql = resolve_graphql_endpoint(args.graphql.as_deref(), args.home.as_deref())?;
    let agent_did = resolve_agent_did(args.home.as_deref(), args.agent_did.as_deref())?;
    let output = load_runtime_status_output(args.home.as_deref(), &graphql, &agent_did).await?;
    print_json(&output)?;
    Ok(())
}

pub(crate) async fn load_runtime_status_output(
    home: Option<&Path>,
    graphql: &str,
    agent_did: &str,
) -> Result<Value> {
    let unavailable_behaviors = load_live_unavailable_behaviors(graphql, agent_did).await;
    let query = format!(
        r#"{{
            AgentRuntime(
                filter: {{ agent_did: {{ _eq: "{agent_did}" }} }},
                limit: 1
            ) {{
                agent_did
                process_state
                reconcile_phase
                active_generation
                router_generation
                default_behavior_id
                runnable_behavior_count
                unavailable_behavior_count
                behavior_executor_capacity
                behavior_executor_queue_depth
                behavior_executor_status_json
                last_reconcile_result
                last_reconcile_error
                last_reconcile_completed_at
                updated_at
            }}
        }}"#,
        agent_did = escape_graphql_string(agent_did),
    );
    let response = post_graphql(graphql, &query).await?;
    let runtime_row = response
        .pointer("/data/AgentRuntime")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .cloned()
        .unwrap_or(Value::Null);
    let liveness_value = crate::commands::status::load_liveness_value(graphql, agent_did).await;
    let home_dir = resolve_home_dir(home);
    let runtime_state = read_runtime_state(&home_dir)?;
    let p2p_status = crate::commands::p2p::load_live_http_p2p_status(home, graphql).await;
    let mut output = json!({
        "home": home_dir,
        "graphql": graphql,
        "agent_did": agent_did,
        "runtime_state": runtime_state,
        "runtime": runtime_row,
        "liveness": liveness_value,
        "p2p": p2p_status,
        "behavior_readiness": if unavailable_behaviors.is_empty() { "ready" } else { "degraded" },
        "unavailable_behaviors": unavailable_behaviors,
    });
    if let Some(map) = output.as_object_mut() {
        for field in [
            "process_state",
            "reconcile_phase",
            "active_generation",
            "router_generation",
            "default_behavior_id",
            "runnable_behavior_count",
            "unavailable_behavior_count",
            "behavior_executor_capacity",
            "behavior_executor_queue_depth",
            "last_reconcile_result",
            "last_reconcile_error",
            "last_reconcile_completed_at",
        ] {
            map.insert(
                field.to_string(),
                runtime_row.get(field).cloned().unwrap_or(Value::Null),
            );
        }
        let behavior_executors = runtime_row
            .get("behavior_executor_status_json")
            .and_then(Value::as_str)
            .and_then(|json| serde_json::from_str::<Value>(json).ok())
            .unwrap_or(Value::Null);
        map.insert("behavior_executors".to_string(), behavior_executors);
        let p2p_value = map.get("p2p").cloned().unwrap_or(Value::Null);
        crate::commands::p2p::flatten_p2p_fields(map, &p2p_value);
    }
    Ok(output)
}

pub(crate) async fn load_liveness_value(graphql: &str, agent_did: &str) -> Value {
    if let Some(liveness) = load_live_http_liveness_value(graphql).await {
        return liveness;
    }
    match crate::http::prometheus::load_metrics_query_data(graphql, agent_did).await {
        Ok(data) => serde_json::to_value(&data.liveness).unwrap_or(Value::Null),
        Err(_) => Value::Null,
    }
}

async fn load_live_http_liveness_value(graphql: &str) -> Option<Value> {
    let status_url = runtime_status_url(graphql).ok()?;
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .ok()?;
    let response = client.get(status_url).send().await.ok()?;
    if !response.status().is_success() {
        return None;
    }
    let body: Value = response.json().await.ok()?;
    body.get("liveness").cloned()
}

fn runtime_status_url(graphql: &str) -> Result<String> {
    let mut url = reqwest::Url::parse(graphql).context("parsing GraphQL endpoint URL")?;
    url.set_path("/status");
    url.set_query(None);
    url.set_fragment(None);
    Ok(url.to_string())
}

async fn load_live_unavailable_behaviors(
    graphql: &str,
    agent_did: &str,
) -> BTreeMap<String, String> {
    let access = ConfigAccess::Graphql(graphql.to_string());
    match build_config_export_bundle(&access, agent_did).await {
        Ok(bundle) => collect_unavailable_behaviors_from_bundle(&bundle),
        Err(_) => BTreeMap::new(),
    }
}

pub(crate) fn collect_unavailable_behaviors_from_bundle(
    bundle: &ConfigExportBundle,
) -> BTreeMap<String, String> {
    let backend_rows = bundle
        .inference_backends
        .iter()
        .filter_map(|row| string_field(row, "backend_id").map(|backend_id| (backend_id, row)))
        .collect::<BTreeMap<_, _>>();
    let tool_selection_rows = bundle
        .tool_selections
        .iter()
        .filter_map(|row| string_field(row, "selection_id").map(|selection_id| (selection_id, row)))
        .collect::<BTreeMap<_, _>>();
    let inference_profile_rows = bundle
        .inference_profiles
        .iter()
        .filter_map(|row| string_field(row, "profile_id").map(|profile_id| (profile_id, row)))
        .collect::<BTreeMap<_, _>>();

    let mut unavailable = BTreeMap::new();
    for behavior in &bundle.agent_behaviors {
        let Some(behavior_id) = string_field(behavior, "behavior_id") else {
            continue;
        };
        if !bool_field(behavior, "enabled", true) {
            unavailable.insert(
                behavior_id.clone(),
                format!("behavior {behavior_id} is disabled"),
            );
            continue;
        }

        let Some(backend_id) = string_field(behavior, "backend_id") else {
            unavailable.insert(
                behavior_id.clone(),
                format!("behavior {behavior_id} has no backend binding"),
            );
            continue;
        };
        let Some(backend) = backend_rows.get(&backend_id) else {
            unavailable.insert(
                behavior_id.clone(),
                format!("behavior {behavior_id} references missing backend {backend_id}"),
            );
            continue;
        };

        let probe_status =
            string_field(backend, "probe_status").unwrap_or_else(|| "unknown".to_string());
        let backend_enabled = bool_field(backend, "enabled", true);
        if !backend_enabled || probe_status != "healthy" {
            unavailable.insert(
                behavior_id.clone(),
                format!(
                    "behavior {behavior_id} backend {backend_id} is unavailable (enabled={backend_enabled} probe_status={probe_status})"
                ),
            );
            continue;
        }

        if let Some(profile_id) = string_field(behavior, "inference_profile_id") {
            if !inference_profile_rows.contains_key(&profile_id) {
                unavailable.insert(
                    behavior_id.clone(),
                    format!(
                        "behavior {behavior_id} references missing inference profile {profile_id}"
                    ),
                );
                continue;
            }
        }

        let _tool_selection = match string_field(behavior, "tool_selection_id") {
            Some(selection_id) => match tool_selection_rows.get(&selection_id) {
                Some(row) => Some(*row),
                None => {
                    unavailable.insert(
                        behavior_id.clone(),
                        format!(
                            "behavior {behavior_id} references missing tool selection {selection_id}"
                        ),
                    );
                    continue;
                }
            },
            None => None,
        };
    }

    unavailable
}

fn string_field(row: &Value, field: &str) -> Option<String> {
    normalize_optional_string(row.get(field).and_then(Value::as_str))
}

fn bool_field(row: &Value, field: &str, default: bool) -> bool {
    row.get(field).and_then(Value::as_bool).unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::ConfigExportBundle;
    use crate::CONFIG_EXPORT_FORMAT;
    use serde_json::json;

    fn bundle_with_rows(
        agent_behaviors: Vec<serde_json::Value>,
        tool_selections: Vec<serde_json::Value>,
        inference_backends: Vec<serde_json::Value>,
        inference_profiles: Vec<serde_json::Value>,
    ) -> ConfigExportBundle {
        ConfigExportBundle {
            format: CONFIG_EXPORT_FORMAT.to_string(),
            agent_did: "did:test:test".to_string(),
            exported_at: "2026-04-14T00:00:00Z".to_string(),
            access_mode: "graphql".to_string(),
            agent_principal: None,
            agent_behaviors,
            skills: Vec::new(),
            datastore_tool_surfaces: Vec::new(),
            workspace_roots: Vec::new(),
            tool_selections,
            inference_backends,
            inference_profiles,
            tool_service_registries: Vec::new(),
            projection_acp_bindings: Vec::new(),
            peer_pairings: Vec::new(),
            tasks: Vec::new(),
            schedules: Vec::new(),
            event_triggers: Vec::new(),
        }
    }

    #[test]
    fn collect_unavailable_behaviors_from_bundle_reports_config_and_backend_issues() {
        let bundle = bundle_with_rows(
            vec![
                json!({
                    "behavior_id": "default",
                    "enabled": true,
                    "backend_id": "",
                    "tool_selection_id": "",
                    "inference_profile_id": ""
                }),
                json!({
                    "behavior_id": "ops",
                    "enabled": true,
                    "backend_id": "backend-unhealthy",
                    "tool_selection_id": "",
                    "inference_profile_id": ""
                }),
                json!({
                    "behavior_id": "broken-tools",
                    "enabled": true,
                    "backend_id": "backend-healthy",
                    "tool_selection_id": "missing-tools",
                    "inference_profile_id": ""
                }),
            ],
            Vec::new(),
            vec![
                json!({
                    "backend_id": "backend-unhealthy",
                    "provider_kind": "OpenAiCompatible",
                    "enabled": true,
                    "probe_status": "unknown"
                }),
                json!({
                    "backend_id": "backend-healthy",
                    "provider_kind": "OpenAiCompatible",
                    "enabled": true,
                    "probe_status": "healthy"
                }),
            ],
            Vec::new(),
        );

        let unavailable = collect_unavailable_behaviors_from_bundle(&bundle);
        assert_eq!(
            unavailable.get("default"),
            Some(&"behavior default has no backend binding".to_string())
        );
        assert_eq!(
            unavailable.get("ops"),
            Some(
                &"behavior ops backend backend-unhealthy is unavailable (enabled=true probe_status=unknown)".to_string()
            )
        );
        assert_eq!(
            unavailable.get("broken-tools"),
            Some(
                &"behavior broken-tools references missing tool selection missing-tools"
                    .to_string()
            )
        );
    }

    #[test]
    fn runtime_status_url_points_at_server_status_root() {
        assert_eq!(
            runtime_status_url("http://127.0.0.1:9191/api/v0/graphql?ignored=true").unwrap(),
            "http://127.0.0.1:9191/status"
        );
    }
}
