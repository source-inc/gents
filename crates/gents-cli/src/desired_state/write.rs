use std::fs;
use std::path::Path;

use serde_json::Value;

use gents::Collection;

use super::{DesiredStateManifest, HasUniqueId, CALLBACK_BINDINGS_DIR, REPOSITORY_PLACEMENTS_DIR};

pub(crate) fn check_filesystem_safe_id(id: &str) -> Result<(), String> {
    if id.is_empty() {
        return Err("unique id is empty; choose a filesystem-safe id".to_string());
    }
    if id == "." || id == ".." {
        return Err(format!(
            "unique id '{id}' contains filesystem-unsafe value; choose a filesystem-safe id"
        ));
    }
    if id.starts_with('.') {
        return Err(format!(
            "unique id '{id}' starts with '.'; dot-prefixed handles are reserved for hidden \
             files and are silently skipped by the loader"
        ));
    }
    for ch in id.chars() {
        if matches!(ch, '/' | '\0') {
            return Err(format!(
                "unique id '{id}' contains filesystem-unsafe character(s); choose a filesystem-safe id"
            ));
        }
    }
    Ok(())
}

fn validate_handles(manifest: &DesiredStateManifest) -> Result<(), String> {
    fn validate_vec<T: HasUniqueId>(docs: &[T], collection_name: &str) -> Result<(), String> {
        let mut seen: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
        for doc in docs {
            let id = doc.unique_id();
            check_filesystem_safe_id(id)?;
            if !seen.insert(id) {
                return Err(format!("duplicate {collection_name} id '{id}' in manifest"));
            }
        }
        Ok(())
    }

    validate_vec(&manifest.agent_behaviors, "behavior_id")?;
    validate_vec(&manifest.skills, "skill_id")?;
    validate_vec(&manifest.datastore_tool_surfaces, "surface_id")?;
    validate_vec(&manifest.tool_selections, "selection_id")?;
    validate_vec(&manifest.inference_backends, "backend_id")?;
    validate_vec(&manifest.inference_profiles, "profile_id")?;
    validate_vec(&manifest.tool_service_registries, "service_id")?;
    validate_vec(&manifest.projection_acp_bindings, "binding_id")?;
    validate_vec(&manifest.peer_pairings, "peer_did")?;
    validate_vec(&manifest.tasks, "task_id")?;
    validate_vec(&manifest.schedules, "schedule_id")?;
    validate_vec(&manifest.event_triggers, "trigger_id")?;
    validate_vec(&manifest.callback_bindings, "binding_id")?;
    validate_vec(&manifest.repository_placements, "repository_id")?;
    Ok(())
}

pub(crate) fn write_manifest_root(
    root: &Path,
    manifest: &DesiredStateManifest,
    force: bool,
) -> Result<(), String> {
    validate_handles(manifest)?;
    prepare_root(root, force)?;

    let principal_value = serde_json::to_value(&manifest.agent_principal)
        .map_err(|e| format!("serializing agent_principal failed: {e}"))?;
    write_json_file(
        &root.join(
            Collection::AgentPrincipal
                .file_name()
                .expect("AgentPrincipal has a top-level file"),
        ),
        &principal_value,
    )?;

    write_per_doc_collection(
        root,
        Collection::AgentBehavior,
        &manifest.agent_behaviors,
        spill_behavior_sidecar,
    )?;
    write_per_doc_collection(root, Collection::Skill, &manifest.skills, no_sidecar)?;
    write_per_doc_collection(
        root,
        Collection::DatastoreToolSurface,
        &manifest.datastore_tool_surfaces,
        no_sidecar,
    )?;
    write_per_doc_collection(
        root,
        Collection::ToolSelection,
        &manifest.tool_selections,
        no_sidecar,
    )?;
    write_per_doc_collection(
        root,
        Collection::InferenceBackend,
        &manifest.inference_backends,
        no_sidecar,
    )?;
    write_per_doc_collection(
        root,
        Collection::InferenceProfile,
        &manifest.inference_profiles,
        no_sidecar,
    )?;
    write_per_doc_collection(
        root,
        Collection::ToolServiceRegistry,
        &manifest.tool_service_registries,
        no_sidecar,
    )?;
    write_per_doc_collection(
        root,
        Collection::ProjectionAcpBinding,
        &manifest.projection_acp_bindings,
        no_sidecar,
    )?;
    write_per_doc_collection(
        root,
        Collection::PeerPairingDesired,
        &manifest.peer_pairings,
        no_sidecar,
    )?;
    write_per_doc_collection(root, Collection::Task, &manifest.tasks, spill_task_sidecar)?;
    write_per_doc_collection(root, Collection::Schedule, &manifest.schedules, no_sidecar)?;
    write_per_doc_collection(
        root,
        Collection::EventTrigger,
        &manifest.event_triggers,
        no_sidecar,
    )?;
    write_per_doc_dir(
        root,
        CALLBACK_BINDINGS_DIR,
        &manifest.callback_bindings,
        no_sidecar,
    )?;
    write_per_doc_dir(
        root,
        REPOSITORY_PLACEMENTS_DIR,
        &manifest.repository_placements,
        no_sidecar,
    )?;

    Ok(())
}

fn prepare_root(root: &Path, force: bool) -> Result<(), String> {
    if !root.exists() {
        fs::create_dir_all(root).map_err(|e| format!("creating {} failed: {e}", root.display()))?;
        return Ok(());
    }
    let is_empty = fs::read_dir(root)
        .map_err(|e| format!("reading {} failed: {e}", root.display()))?
        .next()
        .is_none();
    if is_empty {
        return Ok(());
    }
    if !force {
        return Err(format!(
            "manifest root is non-empty; pass --force to overwrite: {}",
            root.display()
        ));
    }
    if !root.join("agent-principal.json").exists() {
        return Err(format!(
            "refusing to overwrite {}: directory is non-empty and does not \
             contain agent-principal.json (not a manifest root); remove the \
             directory manually or target an empty one",
            root.display()
        ));
    }
    fs::remove_dir_all(root).map_err(|e| format!("clearing {} failed: {e}", root.display()))?;
    fs::create_dir_all(root).map_err(|e| format!("creating {} failed: {e}", root.display()))?;
    Ok(())
}

fn write_per_doc_collection<T>(
    root: &Path,
    collection: Collection,
    docs: &[T],
    mut spill: impl FnMut(&Path, &mut Value) -> Result<(), String>,
) -> Result<(), String>
where
    T: serde::Serialize + HasUniqueId,
{
    if docs.is_empty() {
        return Ok(());
    }
    let dir_name = collection
        .dir_name()
        .expect("write_per_doc_collection called with non-dir collection");
    let collection_dir = root.join(dir_name);
    fs::create_dir_all(&collection_dir)
        .map_err(|e| format!("creating {} failed: {e}", collection_dir.display()))?;

    for doc in docs {
        let handle = doc.unique_id();
        check_filesystem_safe_id(handle)?;
        let doc_dir = collection_dir.join(handle);
        fs::create_dir_all(&doc_dir)
            .map_err(|e| format!("creating {} failed: {e}", doc_dir.display()))?;

        let mut body = serde_json::to_value(doc)
            .map_err(|e| format!("serializing {} '{handle}' failed: {e}", collection))?;
        spill(&doc_dir, &mut body)?;
        write_json_file(&doc_dir.join("object.json"), &body)?;
    }
    Ok(())
}

fn write_per_doc_dir<T>(
    root: &Path,
    dir_name: &str,
    docs: &[T],
    mut spill: impl FnMut(&Path, &mut Value) -> Result<(), String>,
) -> Result<(), String>
where
    T: serde::Serialize + HasUniqueId,
{
    if docs.is_empty() {
        return Ok(());
    }
    let collection_dir = root.join(dir_name);
    fs::create_dir_all(&collection_dir)
        .map_err(|e| format!("creating {} failed: {e}", collection_dir.display()))?;

    for doc in docs {
        let handle = doc.unique_id();
        check_filesystem_safe_id(handle)?;
        let doc_dir = collection_dir.join(handle);
        fs::create_dir_all(&doc_dir)
            .map_err(|e| format!("creating {} failed: {e}", doc_dir.display()))?;

        let mut body = serde_json::to_value(doc)
            .map_err(|e| format!("serializing {dir_name} '{handle}' failed: {e}"))?;
        spill(&doc_dir, &mut body)?;
        write_json_file(&doc_dir.join("object.json"), &body)?;
    }
    Ok(())
}

fn no_sidecar(_dir: &Path, _value: &mut Value) -> Result<(), String> {
    Ok(())
}

fn spill_behavior_sidecar(doc_dir: &Path, body: &mut Value) -> Result<(), String> {
    spill_string_field(doc_dir, body, "system_prompt", "system_prompt.md")?;
    spill_string_field(
        doc_dir,
        body,
        "request_context_template",
        "request_context_template.md",
    )
}

fn spill_task_sidecar(doc_dir: &Path, body: &mut Value) -> Result<(), String> {
    spill_string_field(doc_dir, body, "prompt_template", "prompt.md")
}

fn spill_string_field(
    doc_dir: &Path,
    body: &mut Value,
    field: &str,
    sidecar_name: &str,
) -> Result<(), String> {
    let object = body
        .as_object_mut()
        .ok_or_else(|| "expected object body for sidecar spill, got non-object".to_string())?;
    let raw = object.get(field).cloned();
    match raw {
        None => return Ok(()),
        Some(Value::Null) => {
            object.remove(field);
            return Ok(());
        }
        _ => {}
    }
    let Some(current) = object.get(field).and_then(Value::as_str).map(str::to_owned) else {
        return Ok(());
    };
    if current.is_empty() {
        return Ok(());
    }
    fs::write(doc_dir.join(sidecar_name), &current).map_err(|e| {
        format!(
            "writing {} failed: {e}",
            doc_dir.join(sidecar_name).display()
        )
    })?;
    object.insert(
        field.to_string(),
        Value::String(format!("./{sidecar_name}")),
    );
    Ok(())
}

fn write_json_file(path: &Path, value: &Value) -> Result<(), String> {
    let mut bytes = serde_json::to_vec_pretty(value)
        .map_err(|e| format!("serializing {} failed: {e}", path.display()))?;
    bytes.push(b'\n');
    fs::write(path, &bytes).map_err(|e| format!("writing {} failed: {e}", path.display()))
}
