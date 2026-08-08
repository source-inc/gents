use anyhow::{Context, Result};
use gents::graphql::escape_graphql_string;
use gents::Collection;
use serde_json::{json, Value};

use crate::cli::output_format::OutputFormat;
use crate::cli::{ConfigListArgs, ConfigShowArgs};
use crate::config_import::apply_delete_collection;
use crate::config_writes::ConfigAccess;
use crate::desired_state;
use crate::request_helpers::resolve_dual_id;
use crate::shared::ConfigExportBundle;
use crate::{
    graphql_rows, print_json, resolve_config_access, CONFIG_EXPORT_FORMAT,
    EXPORT_AGENT_BEHAVIOR_FIELDS, EXPORT_EVENT_TRIGGER_FIELDS, EXPORT_INFERENCE_BACKEND_FIELDS,
    EXPORT_INFERENCE_PROFILE_FIELDS, EXPORT_SCHEDULE_FIELDS, EXPORT_TOOL_SELECTION_FIELDS,
    EXPORT_TOOL_SERVICE_REGISTRY_FIELDS, EXPORT_WORKSPACE_ROOT_FIELDS,
};

#[derive(Clone, Copy)]
pub(super) struct ConfigDocumentSpec {
    pub(super) noun: &'static str,
    pub(super) collection: Collection,
    pub(super) fields: &'static str,
}

pub(super) const BACKEND_SPEC: ConfigDocumentSpec = ConfigDocumentSpec {
    noun: "backend",
    collection: Collection::InferenceBackend,
    fields: EXPORT_INFERENCE_BACKEND_FIELDS,
};

pub(super) const BEHAVIOR_SPEC: ConfigDocumentSpec = ConfigDocumentSpec {
    noun: "behavior",
    collection: Collection::AgentBehavior,
    fields: EXPORT_AGENT_BEHAVIOR_FIELDS,
};

pub(super) const TOOL_SELECTION_SPEC: ConfigDocumentSpec = ConfigDocumentSpec {
    noun: "tool selection",
    collection: Collection::ToolSelection,
    fields: EXPORT_TOOL_SELECTION_FIELDS,
};

pub(super) const PROFILE_SPEC: ConfigDocumentSpec = ConfigDocumentSpec {
    noun: "profile",
    collection: Collection::InferenceProfile,
    fields: EXPORT_INFERENCE_PROFILE_FIELDS,
};

pub(super) const TRIGGER_SPEC: ConfigDocumentSpec = ConfigDocumentSpec {
    noun: "trigger",
    collection: Collection::EventTrigger,
    fields: EXPORT_EVENT_TRIGGER_FIELDS,
};

pub(super) const SCHEDULE_SPEC: ConfigDocumentSpec = ConfigDocumentSpec {
    noun: "schedule",
    collection: Collection::Schedule,
    fields: EXPORT_SCHEDULE_FIELDS,
};

pub(super) const MCP_SPEC: ConfigDocumentSpec = ConfigDocumentSpec {
    noun: "mcp",
    collection: Collection::ToolServiceRegistry,
    fields: EXPORT_TOOL_SERVICE_REGISTRY_FIELDS,
};

// list/show only: WorkspaceRoot is local-only config with no agent_did and
// no incoming/outgoing references (see workspace_root.rs), so it does not
// route through config_rm's desired-state reference-safety check — rm is
// implemented directly in commands/config/workspace_root.rs instead.
pub(super) const WORKSPACE_ROOT_SPEC: ConfigDocumentSpec = ConfigDocumentSpec {
    noun: "workspace root",
    collection: Collection::WorkspaceRoot,
    fields: EXPORT_WORKSPACE_ROOT_FIELDS,
};

pub(super) async fn config_list(spec: ConfigDocumentSpec, args: ConfigListArgs) -> Result<()> {
    let (access, _) = resolve_config_access(args.home.as_deref(), args.graphql.as_deref())
        .await
        .with_context(|| format!("resolving access for config {} list", spec.noun))?;
    let mut rows = query_collection(&access, spec, None, None).await?;
    sort_rows(&mut rows, spec.collection.unique_field());

    match args.output.ensure_supported(
        &format!("config {} list", spec.noun),
        &[OutputFormat::Table, OutputFormat::Json],
    )? {
        OutputFormat::Json => print_json(&json!({
            "collection": spec.collection.graphql_type(),
            "count": rows.len(),
            "items": rows,
        })),
        OutputFormat::Table => {
            print_list_table(spec, &rows);
            Ok(())
        }
        _ => unreachable!("ensure_supported restricts config list output formats"),
    }
}

pub(super) async fn config_show(spec: ConfigDocumentSpec, args: ConfigShowArgs) -> Result<()> {
    let id = resolve_config_id(spec, &args)?;
    let (access, _) = resolve_config_access(args.home.as_deref(), args.graphql.as_deref())
        .await
        .with_context(|| format!("resolving access for config {} show", spec.noun))?;
    let row = load_one(&access, spec, &id).await?;

    match args
        .output
        .ensure_supported(&format!("config {} show", spec.noun), &[OutputFormat::Json])?
    {
        OutputFormat::Json => print_json(&row),
        _ => unreachable!("ensure_supported restricts config show output formats"),
    }
}

pub(super) async fn config_rm(spec: ConfigDocumentSpec, args: ConfigShowArgs) -> Result<()> {
    let id = resolve_config_id(spec, &args)?;
    let output = args
        .output
        .ensure_supported(&format!("config {} rm", spec.noun), &[OutputFormat::Json])?;
    let (access, _) = resolve_config_access(args.home.as_deref(), args.graphql.as_deref())
        .await
        .with_context(|| format!("resolving access for config {} rm", spec.noun))?;
    let live = live_manifest_for_delete(
        &access,
        spec,
        &id,
        args.home.as_deref(),
        args.graphql.as_deref(),
    )
    .await?;
    let mut desired = live.clone();
    remove_target(&mut desired, spec.collection, &id);

    let deletes = desired_state::prune::prune_safe_deletes(&desired, &live);
    let target_selected = deletes
        .iter()
        .any(|doc| doc.collection == spec.collection && doc.id == id);
    if !target_selected {
        anyhow::bail!("refused: {} {} is still referenced", spec.noun, id);
    }
    if deletes
        .iter()
        .any(|doc| doc.collection != spec.collection || doc.id != id)
    {
        anyhow::bail!(
            "refused: deleting {} {} would select additional documents",
            spec.noun,
            id
        );
    }

    let txn = access.begin_apply_txn().await?;
    let result = apply_delete_collection(
        &txn,
        spec.collection.graphql_type(),
        spec.collection.unique_field(),
        std::slice::from_ref(&id),
    )
    .await;
    match result {
        Ok(deleted) => {
            txn.commit().await?;
            match output {
                OutputFormat::Json => print_json(&json!({
                    "collection": spec.collection.graphql_type(),
                    "id": id,
                    "deleted": deleted,
                })),
                _ => unreachable!("ensure_supported restricts config rm output formats"),
            }
        }
        Err(error) => {
            let _ = txn.discard().await;
            Err(error)
        }
    }
}

fn resolve_config_id(spec: ConfigDocumentSpec, args: &ConfigShowArgs) -> Result<String> {
    resolve_dual_id(
        spec.noun,
        "--id",
        args.id.as_deref(),
        args.id_flag.as_deref(),
    )
}

async fn live_manifest_for_delete(
    access: &ConfigAccess,
    spec: ConfigDocumentSpec,
    id: &str,
    home: Option<&std::path::Path>,
    graphql: Option<&str>,
) -> Result<desired_state::DesiredStateManifest> {
    let target = load_one(access, spec, id).await?;
    let agent_did =
        super::binding::resolve_target_agent_did(None, None, home, graphql, Some(access)).await?;
    let mut bundle = empty_bundle(access.mode(), &agent_did);
    bundle.agent_principal = Some(synthetic_principal(&agent_did));
    push_doc(&mut bundle, spec.collection, target);
    normalize_bundle_for_manifest(&mut bundle);
    let desired_with_target = desired_state::manifest_from_export_bundle(&bundle)?;
    let mut live_bundle =
        crate::build_desired_state_live_bundle(access, &desired_with_target).await?;
    normalize_bundle_for_manifest(&mut live_bundle);
    desired_state::manifest_from_export_bundle(&live_bundle)
}

async fn query_collection(
    access: &ConfigAccess,
    spec: ConfigDocumentSpec,
    filter_field: Option<&str>,
    filter_value: Option<&str>,
) -> Result<Vec<Value>> {
    let args = match (filter_field, filter_value) {
        (Some(field), Some(value)) => format!(
            r#"(filter: {{ {field}: {{ _eq: "{}" }} }})"#,
            escape_graphql_string(value)
        ),
        (None, None) => String::new(),
        _ => unreachable!("filter field and value are supplied together"),
    };
    let query = format!(
        r#"{{
            {collection}{args} {{
                {fields}
            }}
        }}"#,
        collection = spec.collection.graphql_type(),
        args = args,
        fields = spec.fields,
    );
    graphql_rows(access, spec.collection.graphql_type(), &query).await
}

async fn load_one(access: &ConfigAccess, spec: ConfigDocumentSpec, id: &str) -> Result<Value> {
    let rows =
        query_collection(access, spec, Some(spec.collection.unique_field()), Some(id)).await?;
    rows.into_iter().next().ok_or_else(|| {
        anyhow::anyhow!(
            "not found: no {} document with {} {}",
            spec.collection.graphql_type(),
            spec.collection.unique_field(),
            id
        )
    })
}

fn empty_bundle(access_mode: &str, agent_did: &str) -> ConfigExportBundle {
    ConfigExportBundle {
        format: CONFIG_EXPORT_FORMAT.to_string(),
        agent_did: agent_did.to_string(),
        exported_at: chrono::Utc::now().to_rfc3339(),
        access_mode: access_mode.to_string(),
        agent_principal: None,
        agent_behaviors: Vec::new(),
        skills: Vec::new(),
        datastore_tool_surfaces: Vec::new(),
        workspace_roots: Vec::new(),
        tool_selections: Vec::new(),
        inference_backends: Vec::new(),
        inference_profiles: Vec::new(),
        tool_service_registries: Vec::new(),
        projection_acp_bindings: Vec::new(),
        peer_pairings: Vec::new(),
        tasks: Vec::new(),
        schedules: Vec::new(),
        event_triggers: Vec::new(),
    }
}

fn synthetic_principal(agent_did: &str) -> Value {
    json!({
        "agent_did": agent_did,
        "display_name": null,
        "default_behavior_id": null,
        "enabled": true,
    })
}

fn push_doc(bundle: &mut ConfigExportBundle, collection: Collection, doc: Value) {
    match collection {
        Collection::AgentBehavior => bundle.agent_behaviors.push(doc),
        Collection::Skill => bundle.skills.push(doc),
        Collection::DatastoreToolSurface => bundle.datastore_tool_surfaces.push(doc),
        Collection::WorkspaceRoot => bundle.workspace_roots.push(doc),
        Collection::ToolSelection => bundle.tool_selections.push(doc),
        Collection::InferenceBackend => bundle.inference_backends.push(doc),
        Collection::InferenceProfile => bundle.inference_profiles.push(doc),
        Collection::ToolServiceRegistry => bundle.tool_service_registries.push(doc),
        Collection::ProjectionAcpBinding => bundle.projection_acp_bindings.push(doc),
        Collection::PeerPairingDesired => bundle.peer_pairings.push(doc),
        Collection::Task => bundle.tasks.push(doc),
        Collection::Schedule => bundle.schedules.push(doc),
        Collection::EventTrigger => bundle.event_triggers.push(doc),
        Collection::AgentPrincipal => bundle.agent_principal = Some(doc),
    }
}

fn normalize_bundle_for_manifest(bundle: &mut ConfigExportBundle) {
    for row in &mut bundle.tool_selections {
        if let Some(object) = row.as_object_mut() {
            ensure_bool(object, "enable_file_tools", false);
            ensure_string(object, "file_tools_mode", "Off");
            ensure_bool(object, "enable_bash", false);
            ensure_string(object, "bash_mode", "Off");
            ensure_bool(object, "enable_meta_tools", false);
        }
    }
}

fn ensure_bool(object: &mut serde_json::Map<String, Value>, field: &str, default: bool) {
    if object.get(field).map(Value::is_null).unwrap_or(true) {
        object.insert(field.to_string(), Value::Bool(default));
    }
}

fn ensure_string(object: &mut serde_json::Map<String, Value>, field: &str, default: &str) {
    if object.get(field).map(Value::is_null).unwrap_or(true) {
        object.insert(field.to_string(), Value::String(default.to_string()));
    }
}

fn remove_target(
    manifest: &mut desired_state::DesiredStateManifest,
    collection: Collection,
    id: &str,
) {
    match collection {
        Collection::AgentBehavior => manifest.agent_behaviors.retain(|row| row.behavior_id != id),
        Collection::ToolSelection => manifest
            .tool_selections
            .retain(|row| row.selection_id != id),
        Collection::InferenceBackend => manifest
            .inference_backends
            .retain(|row| row.backend_id != id),
        Collection::InferenceProfile => manifest
            .inference_profiles
            .retain(|row| row.profile_id != id),
        Collection::Skill => manifest.skills.retain(|row| row.skill_id != id),
        Collection::DatastoreToolSurface => manifest
            .datastore_tool_surfaces
            .retain(|row| row.surface_id != id),
        Collection::ToolServiceRegistry => manifest
            .tool_service_registries
            .retain(|row| row.service_id != id),
        Collection::ProjectionAcpBinding => manifest
            .projection_acp_bindings
            .retain(|row| row.binding_id != id),
        Collection::PeerPairingDesired => manifest
            .peer_pairings
            .retain(|row| row.resolved_peer_id().as_deref() != Some(id)),
        Collection::Task => manifest.tasks.retain(|row| row.task_id != id),
        Collection::Schedule => manifest.schedules.retain(|row| row.schedule_id != id),
        Collection::EventTrigger => manifest.event_triggers.retain(|row| row.trigger_id != id),
        Collection::AgentPrincipal => {}
        // WorkspaceRoot has no desired-state manifest list yet (not part of
        // Collection::ALL / CONFIG_APPLY_ORDER); nothing to retain against
        // until a follow-up task wires the file-based CRUD surface.
        Collection::WorkspaceRoot => {}
    }
}

fn sort_rows(rows: &mut [Value], id_field: &str) {
    rows.sort_by(|a, b| {
        a.get(id_field)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .cmp(b.get(id_field).and_then(Value::as_str).unwrap_or_default())
    });
}

fn print_list_table(spec: ConfigDocumentSpec, rows: &[Value]) {
    let id_field = spec.collection.unique_field();
    let headers = ["ID", "ENABLED", "NAME"];
    let rendered = rows
        .iter()
        .map(|row| {
            [
                string_cell(row, id_field),
                bool_cell(row, "enabled"),
                string_cell(row, "display_name").or_else(|| string_cell(row, "name")),
            ]
        })
        .collect::<Vec<_>>();
    let widths = column_widths(&headers, &rendered);
    print_table_row(&headers, &widths);
    let separators = widths.map(|width| "-".repeat(width));
    let separator_cells = [
        separators[0].as_str(),
        separators[1].as_str(),
        separators[2].as_str(),
    ];
    print_table_row(&separator_cells, &widths);
    for row in rendered {
        let cells = [
            row[0].as_deref().unwrap_or(""),
            row[1].as_deref().unwrap_or(""),
            row[2].as_deref().unwrap_or(""),
        ];
        print_table_row(&cells, &widths);
    }
}

fn string_cell(row: &Value, field: &str) -> Option<String> {
    row.get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn bool_cell(row: &Value, field: &str) -> Option<String> {
    row.get(field).and_then(Value::as_bool).map(|value| {
        if value {
            "true".to_string()
        } else {
            "false".to_string()
        }
    })
}

fn column_widths(headers: &[&str; 3], rows: &[[Option<String>; 3]]) -> [usize; 3] {
    let mut widths = headers.map(str::len);
    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            widths[index] = widths[index].max(cell.as_deref().unwrap_or("").len());
        }
    }
    widths
}

fn print_table_row(cells: &[&str; 3], widths: &[usize; 3]) {
    println!(
        "{:<w0$}  {:<w1$}  {:<w2$}",
        cells[0],
        cells[1],
        cells[2],
        w0 = widths[0],
        w1 = widths[1],
        w2 = widths[2],
    );
}
