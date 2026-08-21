use std::collections::BTreeSet;

use anyhow::Result;
use gents::graphql::escape_graphql_string;
use serde_json::Value;

use crate::config_writes::ConfigAccess;
use crate::desired_state;
use crate::shared::ConfigExportBundle;
use crate::{
    graphql_rows, graphql_rows_or_empty_if_collection_missing, graphql_string_list_literal,
    CONFIG_EXPORT_FORMAT, EXPORT_AGENT_BEHAVIOR_FIELDS, EXPORT_AGENT_PRINCIPAL_FIELDS,
    EXPORT_DATASTORE_TOOL_SURFACE_FIELDS, EXPORT_EVENT_TRIGGER_FIELDS,
    EXPORT_INFERENCE_BACKEND_FIELDS, EXPORT_INFERENCE_PROFILE_FIELDS,
    EXPORT_PROJECTION_ACP_BINDING_FIELDS, EXPORT_SCHEDULE_FIELDS, EXPORT_SKILL_FIELDS,
    EXPORT_TASK_FIELDS, EXPORT_TOOL_SELECTION_FIELDS, EXPORT_TOOL_SERVICE_REGISTRY_FIELDS,
};

pub(crate) async fn build_config_export_bundle(
    access: &ConfigAccess,
    agent_did: &str,
) -> Result<ConfigExportBundle> {
    let principal_rows = graphql_rows(
        access,
        "AgentPrincipal",
        &format!(
            r#"{{
                AgentPrincipal(
                    filter: {{ agent_did: {{ _eq: "{agent_did}" }} }},
                    limit: 1
                ) {{
                    {fields}
                }}
            }}"#,
            agent_did = escape_graphql_string(agent_did),
            fields = EXPORT_AGENT_PRINCIPAL_FIELDS,
        ),
    )
    .await?;
    let mut behavior_rows = graphql_rows(
        access,
        "AgentBehavior",
        &format!(
            r#"{{
                AgentBehavior(
                    filter: {{ agent_did: {{ _eq: "{agent_did}" }} }},
                    order: {{ created_at: ASC }}
                ) {{
                    {fields}
                }}
            }}"#,
            agent_did = escape_graphql_string(agent_did),
            fields = EXPORT_AGENT_BEHAVIOR_FIELDS,
        ),
    )
    .await?;
    sort_document_rows(&mut behavior_rows, "behavior_id");

    let tool_selection_ids = collect_string_field_values(&behavior_rows, "tool_selection_id");
    let backend_ids = collect_string_field_values(&behavior_rows, "backend_id");
    let profile_ids = collect_string_field_values(&behavior_rows, "inference_profile_id");

    let mut tool_selection_rows = if tool_selection_ids.is_empty() {
        Vec::new()
    } else {
        graphql_rows(
            access,
            "ToolSelection",
            &format!(
                r#"{{
                    ToolSelection(
                        filter: {{ selection_id: {{ _in: {} }} }}
                    ) {{
                        {fields}
                    }}
                }}"#,
                graphql_string_list_literal(&tool_selection_ids),
                fields = EXPORT_TOOL_SELECTION_FIELDS,
            ),
        )
        .await?
    };
    sort_document_rows(&mut tool_selection_rows, "selection_id");

    let mut backend_rows = if backend_ids.is_empty() {
        Vec::new()
    } else {
        graphql_rows(
            access,
            "InferenceBackend",
            &format!(
                r#"{{
                    InferenceBackend(
                        filter: {{ backend_id: {{ _in: {} }} }}
                    ) {{
                        {fields}
                    }}
                }}"#,
                graphql_string_list_literal(&backend_ids),
                fields = EXPORT_INFERENCE_BACKEND_FIELDS,
            ),
        )
        .await?
    };
    sort_document_rows(&mut backend_rows, "backend_id");

    let mut profile_rows = if profile_ids.is_empty() {
        Vec::new()
    } else {
        graphql_rows(
            access,
            "InferenceProfile",
            &format!(
                r#"{{
                    InferenceProfile(
                        filter: {{ profile_id: {{ _in: {} }} }}
                    ) {{
                        {fields}
                    }}
                }}"#,
                graphql_string_list_literal(&profile_ids),
                fields = EXPORT_INFERENCE_PROFILE_FIELDS,
            ),
        )
        .await?
    };
    sort_document_rows(&mut profile_rows, "profile_id");
    let mut tool_service_registry_rows = graphql_rows_or_empty_if_collection_missing(
        access,
        "ToolServiceRegistry",
        &format!(
            r#"{{
                ToolServiceRegistry {{
                    {fields}
                }}
            }}"#,
            fields = EXPORT_TOOL_SERVICE_REGISTRY_FIELDS,
        ),
    )
    .await?;
    sort_document_rows(&mut tool_service_registry_rows, "service_id");
    normalize_tool_service_registry_export_rows(&mut tool_service_registry_rows)?;
    let mut task_rows = graphql_rows_or_empty_if_collection_missing(
        access,
        "Task",
        &format!(
            r#"{{
                Task {{
                    {fields}
                }}
            }}"#,
            fields = EXPORT_TASK_FIELDS,
        ),
    )
    .await?;
    let mut schedule_rows = graphql_rows_or_empty_if_collection_missing(
        access,
        "Schedule",
        &format!(
            r#"{{
                Schedule {{
                    {fields}
                }}
            }}"#,
            fields = EXPORT_SCHEDULE_FIELDS,
        ),
    )
    .await?;
    let mut event_trigger_rows = graphql_rows_or_empty_if_collection_missing(
        access,
        "EventTrigger",
        &format!(
            r#"{{
                EventTrigger {{
                    {fields}
                }}
            }}"#,
            fields = EXPORT_EVENT_TRIGGER_FIELDS,
        ),
    )
    .await?;
    filter_tasks_and_schedules_by_agent_reachability(
        &behavior_rows,
        &mut task_rows,
        &mut schedule_rows,
        &mut event_trigger_rows,
    );
    sort_document_rows(&mut task_rows, "task_id");
    sort_document_rows(&mut schedule_rows, "schedule_id");
    sort_document_rows(&mut event_trigger_rows, "trigger_id");

    let mut skill_rows = graphql_rows_or_empty_if_collection_missing(
        access,
        "Skill",
        &format!(
            r#"{{
                Skill(
                    filter: {{ agent_did: {{ _eq: "{agent_did}" }} }}
                ) {{
                    {fields}
                }}
            }}"#,
            agent_did = escape_graphql_string(agent_did),
            fields = EXPORT_SKILL_FIELDS,
        ),
    )
    .await?;
    sort_document_rows(&mut skill_rows, "skill_id");

    let mut datastore_tool_surface_rows = graphql_rows_or_empty_if_collection_missing(
        access,
        "DatastoreToolSurface",
        &format!(
            r#"{{
                DatastoreToolSurface(
                    filter: {{ agent_did: {{ _eq: "{agent_did}" }} }}
                ) {{
                    {fields}
                }}
            }}"#,
            agent_did = escape_graphql_string(agent_did),
            fields = EXPORT_DATASTORE_TOOL_SURFACE_FIELDS,
        ),
    )
    .await?;
    sort_document_rows(&mut datastore_tool_surface_rows, "surface_id");

    let mut projection_acp_binding_rows = graphql_rows_or_empty_if_collection_missing(
        access,
        "ProjectionAcpBinding",
        &format!(
            r#"{{
                ProjectionAcpBinding(
                    filter: {{ agent_did: {{ _eq: "{agent_did}" }} }}
                ) {{
                    {fields}
                }}
            }}"#,
            agent_did = escape_graphql_string(agent_did),
            fields = EXPORT_PROJECTION_ACP_BINDING_FIELDS,
        ),
    )
    .await?;
    sort_document_rows(&mut projection_acp_binding_rows, "binding_id");
    let peer_pairing_rows = load_manifest_owned_peer_pairings(access, agent_did).await?;

    Ok(ConfigExportBundle {
        format: CONFIG_EXPORT_FORMAT.to_string(),
        agent_did: agent_did.to_string(),
        exported_at: chrono::Utc::now().to_rfc3339(),
        access_mode: access.mode().to_string(),
        agent_principal: principal_rows.into_iter().next(),
        agent_behaviors: behavior_rows,
        skills: skill_rows,
        datastore_tool_surfaces: datastore_tool_surface_rows,
        // WorkspaceRoot has no live-query wiring yet; see shared::ConfigExportBundle::workspace_roots.
        workspace_roots: Vec::new(),
        tool_selections: tool_selection_rows,
        inference_backends: backend_rows,
        inference_profiles: profile_rows,
        tool_service_registries: tool_service_registry_rows,
        projection_acp_bindings: projection_acp_binding_rows,
        peer_pairings: peer_pairing_rows,
        tasks: task_rows,
        schedules: schedule_rows,
        event_triggers: event_trigger_rows,
    })
}

pub(crate) async fn build_desired_state_live_bundle(
    access: &ConfigAccess,
    desired_manifest: &desired_state::DesiredStateManifest,
) -> Result<ConfigExportBundle> {
    let agent_did = desired_manifest.agent_principal.agent_did.as_str();
    let principal_rows = graphql_rows(
        access,
        "AgentPrincipal",
        &format!(
            r#"{{
                AgentPrincipal(
                    filter: {{ agent_did: {{ _eq: "{agent_did}" }} }},
                    limit: 1
                ) {{
                    {fields}
                }}
            }}"#,
            agent_did = escape_graphql_string(agent_did),
            fields = EXPORT_AGENT_PRINCIPAL_FIELDS,
        ),
    )
    .await?;
    let mut behavior_rows = graphql_rows(
        access,
        "AgentBehavior",
        &format!(
            r#"{{
                AgentBehavior(
                    filter: {{ agent_did: {{ _eq: "{agent_did}" }} }},
                    order: {{ created_at: ASC }}
                ) {{
                    {fields}
                }}
            }}"#,
            agent_did = escape_graphql_string(agent_did),
            fields = EXPORT_AGENT_BEHAVIOR_FIELDS,
        ),
    )
    .await?;
    sort_document_rows(&mut behavior_rows, "behavior_id");

    let tool_selection_ids = collect_string_field_values(&behavior_rows, "tool_selection_id")
        .into_iter()
        .chain(
            desired_manifest
                .tool_selections
                .iter()
                .map(|value| value.selection_id.clone()),
        )
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let backend_scope = collect_string_field_values(&behavior_rows, "backend_id")
        .into_iter()
        .chain(
            desired_manifest
                .inference_backends
                .iter()
                .map(|value| value.backend_id.clone()),
        )
        .collect::<BTreeSet<_>>();
    let profile_ids = collect_string_field_values(&behavior_rows, "inference_profile_id")
        .into_iter()
        .chain(
            desired_manifest
                .inference_profiles
                .iter()
                .map(|value| value.profile_id.clone()),
        )
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    let mut tool_selection_rows = if tool_selection_ids.is_empty() {
        Vec::new()
    } else {
        graphql_rows(
            access,
            "ToolSelection",
            &format!(
                r#"{{
                    ToolSelection(
                        filter: {{ selection_id: {{ _in: {} }} }}
                    ) {{
                        {fields}
                    }}
                }}"#,
                graphql_string_list_literal(&tool_selection_ids),
                fields = EXPORT_TOOL_SELECTION_FIELDS,
            ),
        )
        .await?
    };
    sort_document_rows(&mut tool_selection_rows, "selection_id");

    // InferenceBackend documents are node-global (no agent_did), so fetch
    // them all: a backend absent from the manifest and referenced by nothing
    // must surface as live_only so --prune can remove it (#981). Backends
    // another agent's behavior still references belong to that agent's scope
    // and are excluded so this manifest can never prune them.
    let behavior_backend_ref_rows = graphql_rows(
        access,
        "AgentBehavior",
        "{ AgentBehavior { agent_did backend_id } }",
    )
    .await?;
    let foreign_backend_refs = behavior_backend_ref_rows
        .iter()
        .filter(|row| row.get("agent_did").and_then(Value::as_str) != Some(agent_did))
        .filter_map(|row| row.get("backend_id").and_then(Value::as_str))
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    let mut backend_rows = graphql_rows(
        access,
        "InferenceBackend",
        &format!(
            r#"{{
                InferenceBackend {{
                    {fields}
                }}
            }}"#,
            fields = EXPORT_INFERENCE_BACKEND_FIELDS,
        ),
    )
    .await?;
    backend_rows.retain(|row| {
        row.get("backend_id")
            .and_then(Value::as_str)
            .is_some_and(|id| backend_scope.contains(id) || !foreign_backend_refs.contains(id))
    });
    sort_document_rows(&mut backend_rows, "backend_id");

    let mut profile_rows = if profile_ids.is_empty() {
        Vec::new()
    } else {
        graphql_rows(
            access,
            "InferenceProfile",
            &format!(
                r#"{{
                    InferenceProfile(
                        filter: {{ profile_id: {{ _in: {} }} }}
                    ) {{
                        {fields}
                    }}
                }}"#,
                graphql_string_list_literal(&profile_ids),
                fields = EXPORT_INFERENCE_PROFILE_FIELDS,
            ),
        )
        .await?
    };
    sort_document_rows(&mut profile_rows, "profile_id");
    let tool_service_ids = desired_manifest
        .tool_service_registries
        .iter()
        .map(|value| value.service_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut tool_service_registry_rows = if tool_service_ids.is_empty() {
        Vec::new()
    } else {
        graphql_rows_or_empty_if_collection_missing(
            access,
            "ToolServiceRegistry",
            &format!(
                r#"{{
                    ToolServiceRegistry(
                        filter: {{ service_id: {{ _in: {} }} }}
                    ) {{
                        {fields}
                    }}
                }}"#,
                graphql_string_list_literal(&tool_service_ids),
                fields = EXPORT_TOOL_SERVICE_REGISTRY_FIELDS,
            ),
        )
        .await?
    };
    sort_document_rows(&mut tool_service_registry_rows, "service_id");
    let mut task_rows = graphql_rows_or_empty_if_collection_missing(
        access,
        "Task",
        &format!(
            r#"{{
                Task {{
                    {fields}
                }}
            }}"#,
            fields = EXPORT_TASK_FIELDS,
        ),
    )
    .await?;
    let mut schedule_rows = graphql_rows_or_empty_if_collection_missing(
        access,
        "Schedule",
        &format!(
            r#"{{
                Schedule {{
                    {fields}
                }}
            }}"#,
            fields = EXPORT_SCHEDULE_FIELDS,
        ),
    )
    .await?;
    let mut event_trigger_rows = graphql_rows_or_empty_if_collection_missing(
        access,
        "EventTrigger",
        &format!(
            r#"{{
                EventTrigger {{
                    {fields}
                }}
            }}"#,
            fields = EXPORT_EVENT_TRIGGER_FIELDS,
        ),
    )
    .await?;
    filter_tasks_and_schedules_by_agent_reachability(
        &behavior_rows,
        &mut task_rows,
        &mut schedule_rows,
        &mut event_trigger_rows,
    );
    sort_document_rows(&mut task_rows, "task_id");
    sort_document_rows(&mut schedule_rows, "schedule_id");
    sort_document_rows(&mut event_trigger_rows, "trigger_id");

    let mut skill_rows = graphql_rows_or_empty_if_collection_missing(
        access,
        "Skill",
        &format!(
            r#"{{
                Skill(
                    filter: {{ agent_did: {{ _eq: "{agent_did}" }} }}
                ) {{
                    {fields}
                }}
            }}"#,
            agent_did = escape_graphql_string(agent_did),
            fields = EXPORT_SKILL_FIELDS,
        ),
    )
    .await?;
    sort_document_rows(&mut skill_rows, "skill_id");

    let mut datastore_tool_surface_rows = graphql_rows_or_empty_if_collection_missing(
        access,
        "DatastoreToolSurface",
        &format!(
            r#"{{
                DatastoreToolSurface(
                    filter: {{ agent_did: {{ _eq: "{agent_did}" }} }}
                ) {{
                    {fields}
                }}
            }}"#,
            agent_did = escape_graphql_string(agent_did),
            fields = EXPORT_DATASTORE_TOOL_SURFACE_FIELDS,
        ),
    )
    .await?;
    sort_document_rows(&mut datastore_tool_surface_rows, "surface_id");

    let desired_binding_ids = desired_manifest
        .projection_acp_bindings
        .iter()
        .map(|value| value.binding_id.clone())
        .collect::<BTreeSet<_>>();
    let mut projection_acp_binding_rows = graphql_rows_or_empty_if_collection_missing(
        access,
        "ProjectionAcpBinding",
        &format!(
            r#"{{
                ProjectionAcpBinding {{
                    {fields}
                }}
            }}"#,
            fields = EXPORT_PROJECTION_ACP_BINDING_FIELDS,
        ),
    )
    .await?;
    projection_acp_binding_rows.retain(|row| {
        row.get("agent_did").and_then(Value::as_str) == Some(agent_did)
            || row
                .get("binding_id")
                .and_then(Value::as_str)
                .is_some_and(|binding_id| desired_binding_ids.contains(binding_id))
    });
    sort_document_rows(&mut projection_acp_binding_rows, "binding_id");
    let peer_pairing_rows = load_manifest_owned_peer_pairings(access, agent_did).await?;

    Ok(ConfigExportBundle {
        format: CONFIG_EXPORT_FORMAT.to_string(),
        agent_did: agent_did.to_string(),
        exported_at: chrono::Utc::now().to_rfc3339(),
        access_mode: access.mode().to_string(),
        agent_principal: principal_rows.into_iter().next(),
        agent_behaviors: behavior_rows,
        skills: skill_rows,
        datastore_tool_surfaces: datastore_tool_surface_rows,
        // WorkspaceRoot has no live-query wiring yet; see shared::ConfigExportBundle::workspace_roots.
        workspace_roots: Vec::new(),
        tool_selections: tool_selection_rows,
        inference_backends: backend_rows,
        inference_profiles: profile_rows,
        tool_service_registries: tool_service_registry_rows,
        projection_acp_bindings: projection_acp_binding_rows,
        peer_pairings: peer_pairing_rows,
        tasks: task_rows,
        schedules: schedule_rows,
        event_triggers: event_trigger_rows,
    })
}

pub(crate) fn live_manifest_from_bundle(
    desired_manifest: &desired_state::DesiredStateManifest,
    live_bundle: &ConfigExportBundle,
) -> Result<(
    Option<desired_state::DesiredAgentPrincipal>,
    desired_state::DesiredStateManifest,
)> {
    if live_bundle.agent_principal.is_some() {
        let live_manifest = desired_state::manifest_from_export_bundle(live_bundle)?;
        Ok((Some(live_manifest.agent_principal.clone()), live_manifest))
    } else {
        Ok((
            None,
            desired_state::DesiredStateManifest {
                agent_principal: desired_manifest.agent_principal.clone(),
                agent_behaviors: Vec::new(),
                skills: Vec::new(),
                datastore_tool_surfaces: Vec::new(),
                tool_selections: Vec::new(),
                inference_backends: Vec::new(),
                inference_profiles: Vec::new(),
                tool_service_registries: Vec::new(),
                projection_acp_bindings: Vec::new(),
                peer_pairings: Vec::new(),
                tasks: Vec::new(),
                schedules: Vec::new(),
                event_triggers: Vec::new(),
            },
        ))
    }
}

async fn load_manifest_owned_peer_pairings(
    access: &ConfigAccess,
    owner_agent_did: &str,
) -> Result<Vec<Value>> {
    let source = desired_state::peer_pairing_manifest_source(owner_agent_did);
    let source = escape_graphql_string(&source);
    let mut rows = graphql_rows_or_empty_if_collection_missing(
        access,
        "PeerPairingDesired",
        &format!(
            r#"{{
                PeerPairingDesired(filter: {{ source: {{ _eq: "{source}" }} }}) {{
                    peer_id
                    agent_did
                    replicator_addresses
                    template
                    source
                }}
            }}"#
        ),
    )
    .await?;
    sort_document_rows(&mut rows, "peer_id");
    Ok(rows)
}

pub(crate) fn filter_tasks_and_schedules_by_agent_reachability(
    behavior_rows: &[Value],
    task_rows: &mut Vec<Value>,
    schedule_rows: &mut Vec<Value>,
    event_trigger_rows: &mut Vec<Value>,
) {
    let behavior_ids: BTreeSet<String> = behavior_rows
        .iter()
        .filter_map(|row| row.get("behavior_id").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect();

    task_rows.retain(|row| {
        row.get("behavior_id")
            .and_then(Value::as_str)
            .is_some_and(|behavior_id| behavior_ids.contains(behavior_id))
    });

    let reachable_task_ids: BTreeSet<String> = task_rows
        .iter()
        .filter_map(|row| row.get("task_id").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect();

    schedule_rows.retain(|row| {
        row.get("task_id")
            .and_then(Value::as_str)
            .is_some_and(|task_id| reachable_task_ids.contains(task_id))
    });

    event_trigger_rows.retain(|row| {
        row.get("task_id")
            .and_then(Value::as_str)
            .is_some_and(|task_id| reachable_task_ids.contains(task_id))
    });
}

pub(crate) fn sort_document_rows(rows: &mut [Value], key: &str) {
    rows.sort_by(|left, right| {
        let left_key = left.get(key).and_then(Value::as_str).unwrap_or_default();
        let right_key = right.get(key).and_then(Value::as_str).unwrap_or_default();
        left_key.cmp(right_key)
    });
}

pub(crate) fn normalize_tool_service_registry_export_rows(rows: &mut [Value]) -> Result<()> {
    for row in rows {
        let object = row
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("ToolServiceRegistry export row must be an object"))?;
        desired_state::normalize_tool_service_registry_storage_fields(object)?;
    }
    Ok(())
}

pub(crate) fn collect_string_field_values(rows: &[Value], field: &str) -> Vec<String> {
    let mut values = rows
        .iter()
        .filter_map(|row| row.get(field).and_then(Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values
}

pub(crate) fn sanitize_import_document(
    collection_name: &str,
    doc: &Value,
    for_update: bool,
) -> Result<Value> {
    let mut object = doc
        .as_object()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("{collection_name} import document must be an object"))?;

    match collection_name {
        "AgentBehavior" => {
            // `created_at` is immutable once set; on create (add doc) inject
            // the current timestamp if the document doesn't already carry one,
            // so the live-bundle ordering query (`order: { created_at: ASC }`)
            // is deterministic. On update, leave it untouched (Null sentinel
            // tells DefraDB to skip the field on upsert update).
            if for_update {
                object.remove("created_at");
            } else {
                let has_created_at = object
                    .get("created_at")
                    .map(|v| !matches!(v, Value::Null))
                    .and_then(|present| present.then_some(()))
                    .is_some()
                    && object
                        .get("created_at")
                        .and_then(Value::as_str)
                        .map(|s| !s.trim().is_empty())
                        .unwrap_or(false);
                if !has_created_at {
                    object.insert(
                        "created_at".to_string(),
                        Value::String(chrono::Utc::now().to_rfc3339()),
                    );
                }
            }
        }
        "InferenceBackend" => {
            desired_state::strip_deprecated_inference_backend_fields(&mut object);
            object.remove("last_probe");
            if for_update {
                object.insert("last_probe".to_string(), Value::Null);
            }
        }
        "ToolSelection" => {
            desired_state::strip_retired_tool_selection_fields(&mut object);
        }
        "Task" => {
            for field in ["created_at", "updated_at"] {
                object.remove(field);
            }
            if for_update {
                object.insert("created_at".to_string(), Value::Null);
                object.insert("updated_at".to_string(), Value::Null);
            }
        }
        "Schedule" => {
            for field in [
                "next_run_at",
                "last_attempt_at",
                "last_status",
                "last_error",
                "fire_count",
                "created_at",
                "updated_at",
            ] {
                object.remove(field);
            }
            if for_update {
                object.insert("created_at".to_string(), Value::Null);
                object.insert("updated_at".to_string(), Value::Null);
            }
        }
        "EventTrigger" => {
            for field in [
                "last_attempt_at",
                "last_fired_source_doc_id",
                "last_status",
                "last_error",
                "fire_count",
                "created_at",
                "updated_at",
            ] {
                object.remove(field);
            }
            if for_update {
                object.insert("created_at".to_string(), Value::Null);
                object.insert("updated_at".to_string(), Value::Null);
            }
        }
        "ToolServiceRegistry" => {
            for field in ["tools", "version", "updated_at"] {
                object.remove(field);
            }
            desired_state::normalize_tool_service_registry_storage_fields(&mut object)?;
            if for_update {
                object.insert("updated_at".to_string(), Value::Null);
            }
            match object.get("status") {
                Some(Value::String(s)) if !s.is_empty() => {}
                _ => {
                    object.insert("status".to_string(), Value::String("online".to_string()));
                }
            }
        }
        "ProjectionAcpBinding" => {
            for field in ["created_at", "updated_at"] {
                object.remove(field);
            }
            if for_update {
                object.insert("created_at".to_string(), Value::Null);
                object.insert("updated_at".to_string(), Value::Null);
            }
        }
        _ => {}
    }

    // DefraDB cannot type an empty array literal (`[]`) and rejects it. How an
    // empty list is handled depends on whether this is a create or an update:
    //
    //  - CREATE: there is no prior value to clear, so omit the field (a new row
    //    reads back as empty via the null-safe deserializers). Required-list
    //    fields would also reject `null` here.
    //  - UPDATE: omitting would leave a previously non-empty list in place,
    //    so a "remove the last entry" edit could never converge on re-apply.
    //    Write `null` to actually CLEAR it (the deserializers read null as
    //    empty). Required non-null list fields (peer_pairing_desired) cannot be
    //    nulled — leave them omitted so apply does not clobber/err on them.
    const REQUIRED_LIST_FIELDS: [&str; 2] = ["collections", "replicator_addresses"];
    let empty_list_fields: Vec<String> = object
        .iter()
        .filter(|(_, value)| matches!(value, Value::Array(items) if items.is_empty()))
        .map(|(key, _)| key.clone())
        .collect();
    for field in empty_list_fields {
        if for_update && !REQUIRED_LIST_FIELDS.contains(&field.as_str()) {
            object.insert(field, Value::Null);
        } else {
            object.remove(&field);
        }
    }

    Ok(Value::Object(object))
}

pub(crate) fn select_apply_collection_docs(
    docs: &[Value],
    unique_field: &str,
    collection_name: &str,
    diff: &desired_state::DesiredStateCollectionDiff,
) -> Result<Vec<Value>> {
    let requested_ids = diff
        .create
        .iter()
        .chain(diff.update.iter())
        .cloned()
        .collect::<BTreeSet<_>>();
    if requested_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut selected = docs
        .iter()
        .filter(|doc| {
            doc.get(unique_field)
                .and_then(Value::as_str)
                .is_some_and(|value| requested_ids.contains(value))
        })
        .cloned()
        .collect::<Vec<_>>();
    sort_document_rows(&mut selected, unique_field);

    let found_ids = selected
        .iter()
        .filter_map(|doc| doc.get(unique_field).and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();
    let missing_ids = requested_ids
        .difference(&found_ids)
        .cloned()
        .collect::<Vec<_>>();
    if !missing_ids.is_empty() {
        anyhow::bail!(
            "desired-state apply missing {collection_name} documents for ids: {}",
            missing_ids.join(", ")
        );
    }

    Ok(selected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn empty_list_is_cleared_on_update_but_omitted_on_create() {
        let created = sanitize_import_document(
            "Skill",
            &json!({ "skill_id": "s", "tool_refs": [], "agent_did": "did:p" }),
            false,
        )
        .unwrap();
        assert!(
            created.get("tool_refs").is_none(),
            "create omits empty list"
        );

        let updated = sanitize_import_document(
            "AgentBehavior",
            &json!({ "behavior_id": "b", "skill_refs": [], "skill_excludes": [] }),
            true,
        )
        .unwrap();
        assert_eq!(updated.get("skill_refs"), Some(&Value::Null));
        assert_eq!(updated.get("skill_excludes"), Some(&Value::Null));
    }

    #[test]
    fn required_non_null_list_fields_are_never_nulled() {
        let updated = sanitize_import_document(
            "PeerPairingDesired",
            &json!({ "id": "p", "collections": [], "replicator_addresses": [] }),
            true,
        )
        .unwrap();
        assert!(updated.get("collections").is_none());
        assert!(updated.get("replicator_addresses").is_none());
    }

    #[test]
    fn filter_retains_only_rows_reachable_from_selected_agent_behaviors() {
        let behaviors_a = vec![
            json!({ "behavior_id": "general-a" }),
            json!({ "behavior_id": "code-a" }),
        ];
        let mut tasks = vec![
            json!({ "task_id": "task-a1", "behavior_id": "general-a" }),
            json!({ "task_id": "task-a2", "behavior_id": "code-a" }),
            json!({ "task_id": "task-b1", "behavior_id": "general-b" }),
            json!({ "task_id": "task-orphan", "behavior_id": "nonexistent" }),
            json!({ "task_id": "task-missing-behavior" }),
        ];
        let mut schedules = vec![
            json!({ "schedule_id": "sched-a1", "task_id": "task-a1" }),
            json!({ "schedule_id": "sched-a2", "task_id": "task-a2" }),
            json!({ "schedule_id": "sched-b1", "task_id": "task-b1" }),
            json!({ "schedule_id": "sched-dangling", "task_id": "task-missing" }),
            json!({ "schedule_id": "sched-missing-task" }),
        ];
        let mut event_triggers = vec![
            json!({ "trigger_id": "trig-a1", "task_id": "task-a1" }),
            json!({ "trigger_id": "trig-a2", "task_id": "task-a2" }),
            json!({ "trigger_id": "trig-b1", "task_id": "task-b1" }),
            json!({ "trigger_id": "trig-dangling", "task_id": "task-missing" }),
            json!({ "trigger_id": "trig-missing-task" }),
        ];

        filter_tasks_and_schedules_by_agent_reachability(
            &behaviors_a,
            &mut tasks,
            &mut schedules,
            &mut event_triggers,
        );

        let task_ids: Vec<&str> = tasks
            .iter()
            .map(|t| t.get("task_id").and_then(Value::as_str).unwrap())
            .collect();
        assert_eq!(
            task_ids,
            vec!["task-a1", "task-a2"],
            "only agent A's tasks should survive reachability filtering"
        );

        let schedule_ids: Vec<&str> = schedules
            .iter()
            .map(|s| s.get("schedule_id").and_then(Value::as_str).unwrap())
            .collect();
        assert_eq!(
            schedule_ids,
            vec!["sched-a1", "sched-a2"],
            "only schedules whose task_id is in agent A's reachable task set should survive"
        );

        let trigger_ids: Vec<&str> = event_triggers
            .iter()
            .map(|t| t.get("trigger_id").and_then(Value::as_str).unwrap())
            .collect();
        assert_eq!(
            trigger_ids,
            vec!["trig-a1", "trig-a2"],
            "only event triggers whose task_id is in agent A's reachable task set should survive"
        );
    }

    #[test]
    fn filter_is_disjoint_across_agents_on_same_node() {
        let mut tasks_for_a = vec![
            json!({ "task_id": "task-a1", "behavior_id": "general-a" }),
            json!({ "task_id": "task-b1", "behavior_id": "general-b" }),
        ];
        let mut schedules_for_a = vec![
            json!({ "schedule_id": "sched-a1", "task_id": "task-a1" }),
            json!({ "schedule_id": "sched-b1", "task_id": "task-b1" }),
        ];
        let mut triggers_for_a = vec![
            json!({ "trigger_id": "trig-a1", "task_id": "task-a1" }),
            json!({ "trigger_id": "trig-b1", "task_id": "task-b1" }),
        ];
        let mut tasks_for_b = tasks_for_a.clone();
        let mut schedules_for_b = schedules_for_a.clone();
        let mut triggers_for_b = triggers_for_a.clone();

        let behaviors_a = vec![json!({ "behavior_id": "general-a" })];
        let behaviors_b = vec![json!({ "behavior_id": "general-b" })];

        filter_tasks_and_schedules_by_agent_reachability(
            &behaviors_a,
            &mut tasks_for_a,
            &mut schedules_for_a,
            &mut triggers_for_a,
        );
        filter_tasks_and_schedules_by_agent_reachability(
            &behaviors_b,
            &mut tasks_for_b,
            &mut schedules_for_b,
            &mut triggers_for_b,
        );

        let a_task_ids: Vec<&str> = tasks_for_a
            .iter()
            .map(|t| t.get("task_id").and_then(Value::as_str).unwrap())
            .collect();
        let b_task_ids: Vec<&str> = tasks_for_b
            .iter()
            .map(|t| t.get("task_id").and_then(Value::as_str).unwrap())
            .collect();
        assert_eq!(a_task_ids, vec!["task-a1"]);
        assert_eq!(b_task_ids, vec!["task-b1"]);

        let a_sched_ids: Vec<&str> = schedules_for_a
            .iter()
            .map(|s| s.get("schedule_id").and_then(Value::as_str).unwrap())
            .collect();
        let b_sched_ids: Vec<&str> = schedules_for_b
            .iter()
            .map(|s| s.get("schedule_id").and_then(Value::as_str).unwrap())
            .collect();
        assert_eq!(a_sched_ids, vec!["sched-a1"]);
        assert_eq!(b_sched_ids, vec!["sched-b1"]);

        let a_trigger_ids: Vec<&str> = triggers_for_a
            .iter()
            .map(|t| t.get("trigger_id").and_then(Value::as_str).unwrap())
            .collect();
        let b_trigger_ids: Vec<&str> = triggers_for_b
            .iter()
            .map(|t| t.get("trigger_id").and_then(Value::as_str).unwrap())
            .collect();
        assert_eq!(a_trigger_ids, vec!["trig-a1"]);
        assert_eq!(b_trigger_ids, vec!["trig-b1"]);
    }

    #[test]
    fn filter_with_no_behaviors_drops_all_tasks_and_schedules() {
        let behaviors: Vec<Value> = Vec::new();
        let mut tasks = vec![json!({ "task_id": "task-x", "behavior_id": "some" })];
        let mut schedules = vec![json!({ "schedule_id": "sched-x", "task_id": "task-x" })];
        let mut triggers = vec![json!({ "trigger_id": "trig-x", "task_id": "task-x" })];

        filter_tasks_and_schedules_by_agent_reachability(
            &behaviors,
            &mut tasks,
            &mut schedules,
            &mut triggers,
        );

        assert!(
            tasks.is_empty(),
            "no behaviors means no reachable tasks; got {tasks:?}"
        );
        assert!(
            schedules.is_empty(),
            "no reachable tasks means no reachable schedules; got {schedules:?}"
        );
        assert!(
            triggers.is_empty(),
            "no reachable tasks means no reachable event triggers; got {triggers:?}"
        );
    }
}
