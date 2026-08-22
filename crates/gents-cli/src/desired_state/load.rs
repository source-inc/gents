use std::fs;
use std::path::Path;

use serde::Deserialize;

use super::normalize::normalize_manifest;
use super::validate::validate_manifest;
use super::{
    DesiredAgentBehavior, DesiredAgentPrincipal, DesiredCallbackBinding,
    DesiredDatastoreToolSurface, DesiredEventTrigger, DesiredInferenceBackend,
    DesiredInferenceProfile, DesiredPeerPairing, DesiredProjectionAcpBinding,
    DesiredRepositoryPlacement, DesiredSchedule, DesiredSkill, DesiredStateCounts,
    DesiredStateManifest, DesiredStateValidationReport, DesiredTask, DesiredToolSelection,
    DesiredToolServiceRegistry, HasUniqueId, CALLBACK_BINDINGS_DIR, REPOSITORY_PLACEMENTS_DIR,
};
use gents::Collection;

pub(crate) fn load_manifest_root(
    root: &Path,
) -> (Option<DesiredStateManifest>, DesiredStateValidationReport) {
    let root_display = root.display().to_string();
    let mut errors = Vec::new();

    if !root.exists() {
        errors.push(format!("manifest root does not exist: {root_display}"));
        return (None, empty_report(root_display, errors));
    }
    if !root.is_dir() {
        errors.push(format!("manifest root is not a directory: {root_display}"));
        return (None, empty_report(root_display, errors));
    }

    let principal = load_agent_principal(root, &mut errors);

    let mut agent_behaviors: Vec<DesiredAgentBehavior> =
        load_per_doc_collection(root, Collection::AgentBehavior, &mut errors);
    let skills: Vec<DesiredSkill> = load_per_doc_collection(root, Collection::Skill, &mut errors);
    let datastore_tool_surfaces: Vec<DesiredDatastoreToolSurface> =
        load_per_doc_collection(root, Collection::DatastoreToolSurface, &mut errors);
    let tool_selections: Vec<DesiredToolSelection> =
        load_per_doc_collection(root, Collection::ToolSelection, &mut errors);
    let inference_backends: Vec<DesiredInferenceBackend> =
        load_per_doc_collection(root, Collection::InferenceBackend, &mut errors);
    let inference_profiles: Vec<DesiredInferenceProfile> =
        load_per_doc_collection(root, Collection::InferenceProfile, &mut errors);
    let tool_service_registries: Vec<DesiredToolServiceRegistry> =
        load_per_doc_collection(root, Collection::ToolServiceRegistry, &mut errors);
    let projection_acp_bindings: Vec<DesiredProjectionAcpBinding> =
        load_per_doc_collection(root, Collection::ProjectionAcpBinding, &mut errors);
    let peer_pairings = load_peer_pairings(root, &mut errors);
    let mut tasks: Vec<DesiredTask> = load_per_doc_collection(root, Collection::Task, &mut errors);
    let schedules: Vec<DesiredSchedule> =
        load_per_doc_collection(root, Collection::Schedule, &mut errors);
    let event_triggers: Vec<DesiredEventTrigger> =
        load_per_doc_collection(root, Collection::EventTrigger, &mut errors);
    let callback_bindings: Vec<DesiredCallbackBinding> =
        load_per_doc_dir(root, CALLBACK_BINDINGS_DIR, "binding_id", &mut errors);
    let repository_placements: Vec<DesiredRepositoryPlacement> = load_per_doc_dir(
        root,
        REPOSITORY_PLACEMENTS_DIR,
        "repository_id",
        &mut errors,
    );

    for behavior in &mut agent_behaviors {
        let dir = per_doc_dir(root, Collection::AgentBehavior, behavior.unique_id());
        if let Err(error) = hydrate_sidecar(&mut behavior.system_prompt, &dir) {
            errors.push(error);
        }
        if let Err(error) = hydrate_sidecar(&mut behavior.request_context_template, &dir) {
            errors.push(error);
        }
    }
    for task in &mut tasks {
        let dir = per_doc_dir(root, Collection::Task, task.unique_id());
        let mut wrapped = Some(std::mem::take(&mut task.prompt_template));
        if let Err(error) = hydrate_sidecar(&mut wrapped, &dir) {
            errors.push(error);
        }
        task.prompt_template = wrapped.unwrap_or_default();
    }

    let counts = DesiredStateCounts {
        agent_principal: usize::from(principal.is_some()),
        agent_behaviors: agent_behaviors.len(),
        skills: skills.len(),
        datastore_tool_surfaces: datastore_tool_surfaces.len(),
        tool_selections: tool_selections.len(),
        inference_backends: inference_backends.len(),
        inference_profiles: inference_profiles.len(),
        tool_service_registries: tool_service_registries.len(),
        projection_acp_bindings: projection_acp_bindings.len(),
        peer_pairings: peer_pairings.len(),
        tasks: tasks.len(),
        schedules: schedules.len(),
        event_triggers: event_triggers.len(),
        callback_bindings: callback_bindings.len(),
        repository_placements: repository_placements.len(),
    };

    let agent_did = principal.as_ref().map(|p| p.agent_did.clone());

    let manifest = principal.map(|principal| {
        let mut manifest = DesiredStateManifest {
            agent_principal: principal,
            agent_behaviors,
            skills,
            datastore_tool_surfaces,
            tool_selections,
            inference_backends,
            inference_profiles,
            tool_service_registries,
            projection_acp_bindings,
            peer_pairings,
            tasks,
            schedules,
            event_triggers,
            callback_bindings,
            repository_placements,
        };
        normalize_manifest(&mut manifest);
        validate_manifest(&manifest, &mut errors);
        manifest
    });

    (
        manifest,
        DesiredStateValidationReport {
            status: if errors.is_empty() {
                "validated"
            } else {
                "invalid"
            },
            ok: errors.is_empty(),
            root: root_display,
            agent_did,
            counts,
            errors,
        },
    )
}

fn load_peer_pairings(root: &Path, errors: &mut Vec<String>) -> Vec<DesiredPeerPairing> {
    let collection_path = root.join(
        Collection::PeerPairingDesired
            .dir_name()
            .expect("PeerPairingDesired uses a directory manifest form"),
    );
    if !collection_path.exists() {
        return Vec::new();
    }
    if !collection_path.is_dir() {
        errors.push(format!(
            "manifest collection path is not a directory: {}",
            collection_path.display()
        ));
        return Vec::new();
    }

    let entries = match fs::read_dir(&collection_path) {
        Ok(entries) => entries,
        Err(error) => {
            errors.push(format!(
                "reading {} failed: {error}",
                collection_path.display()
            ));
            return Vec::new();
        }
    };
    let mut subdirs = entries
        .filter_map(|entry| match entry {
            Ok(entry) if entry.path().is_dir() => entry
                .file_name()
                .to_str()
                .filter(|name| !name.starts_with('.'))
                .map(|name| (name.to_string(), entry.path())),
            Ok(_) => None,
            Err(error) => {
                errors.push(format!(
                    "reading {} failed: {error}",
                    collection_path.display()
                ));
                None
            }
        })
        .collect::<Vec<_>>();
    subdirs.sort_by(|left, right| left.0.cmp(&right.0));

    let mut pairings = Vec::with_capacity(subdirs.len());
    let mut did_to_handle = std::collections::BTreeMap::<String, String>::new();
    for (handle, path) in subdirs {
        let object_path = path.join("object.json");
        let Some(bytes) = read_document_json(&object_path, errors) else {
            continue;
        };
        let pairing: DesiredPeerPairing = match serde_json::from_slice(&bytes) {
            Ok(pairing) => pairing,
            Err(error) => {
                errors.push(format!("invalid {}: {error}", object_path.display()));
                continue;
            }
        };
        let peer_did = pairing.peer_did.trim().to_string();
        if let Some(previous) = did_to_handle.insert(peer_did.clone(), handle.clone()) {
            errors.push(format!(
                "duplicate peer_did in peer-pairings manifest: {peer_did} ({previous}/ and {handle}/)"
            ));
            continue;
        }
        pairings.push(pairing);
    }
    pairings
}

fn empty_report(root_display: String, errors: Vec<String>) -> DesiredStateValidationReport {
    DesiredStateValidationReport {
        status: "invalid",
        ok: false,
        root: root_display,
        agent_did: None,
        counts: DesiredStateCounts::empty(),
        errors,
    }
}

fn load_agent_principal(root: &Path, errors: &mut Vec<String>) -> Option<DesiredAgentPrincipal> {
    let file_name = Collection::AgentPrincipal
        .file_name()
        .expect("AgentPrincipal has a top-level file");
    let path = root.join(file_name);
    if !path.exists() {
        errors.push(format!(
            "required manifest file is missing: {}",
            path.display()
        ));
        return None;
    }
    let bytes = read_document_json(&path, errors)?;
    match serde_json::from_slice(&bytes) {
        Ok(value) => Some(value),
        Err(error) => {
            errors.push(format!("invalid {}: {error}", path.display()));
            None
        }
    }
}

/// Read a document JSON file and expand `${VAR}` references. Interpolation is
/// scoped to document JSON: `.md` sidecars carry runtime `{{ }}` templates and
/// are hydrated separately, untouched.
fn read_document_json(path: &Path, errors: &mut Vec<String>) -> Option<Vec<u8>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) => {
            errors.push(if error.kind() == std::io::ErrorKind::NotFound {
                format!("per-doc dir is missing object.json: {}", path.display())
            } else {
                format!("reading {} failed: {error}", path.display())
            });
            return None;
        }
    };
    let text = match String::from_utf8(bytes) {
        Ok(text) => text,
        Err(_) => {
            errors.push(format!("{} is not valid UTF-8", path.display()));
            return None;
        }
    };
    match super::interpolate::interpolate(&text) {
        Ok(expanded) => Some(expanded.into_bytes()),
        Err(missing) => {
            errors.push(format!(
                "{} references unset environment variable(s): {}. Set them, or give each a \
                 default with ${{NAME:-value}}.",
                path.display(),
                missing.join(", ")
            ));
            None
        }
    }
}

fn per_doc_dir(root: &Path, collection: Collection, handle: &str) -> std::path::PathBuf {
    let dir_name = collection
        .dir_name()
        .expect("per_doc_dir called with non-directory collection");
    root.join(dir_name).join(handle)
}

pub(crate) fn load_per_doc_collection<T>(
    root: &Path,
    collection: Collection,
    errors: &mut Vec<String>,
) -> Vec<T>
where
    T: for<'de> Deserialize<'de> + HasUniqueId,
{
    let dir_name = collection
        .dir_name()
        .expect("load_per_doc_collection called with a non-directory collection");
    load_per_doc_dir(root, dir_name, collection.unique_field(), errors)
}

pub(crate) fn load_per_doc_dir<T>(
    root: &Path,
    dir_name: &str,
    unique_field: &str,
    errors: &mut Vec<String>,
) -> Vec<T>
where
    T: for<'de> Deserialize<'de> + HasUniqueId,
{
    let collection_path = root.join(dir_name);
    if !collection_path.exists() {
        return Vec::new();
    }
    if !collection_path.is_dir() {
        errors.push(format!(
            "manifest collection path is not a directory: {}",
            collection_path.display()
        ));
        return Vec::new();
    }

    let entries = match fs::read_dir(&collection_path) {
        Ok(iter) => iter,
        Err(error) => {
            errors.push(format!(
                "reading {} failed: {error}",
                collection_path.display()
            ));
            return Vec::new();
        }
    };

    let mut subdirs: Vec<(String, std::path::PathBuf)> = Vec::new();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                errors.push(format!(
                    "reading {} failed: {error}",
                    collection_path.display()
                ));
                continue;
            }
        };
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if name.starts_with('.') {
            continue;
        }
        subdirs.push((name.to_string(), path));
    }
    subdirs.sort_by(|a, b| a.0.cmp(&b.0));

    let mut docs: Vec<T> = Vec::with_capacity(subdirs.len());
    let mut id_to_handle: std::collections::BTreeMap<String, String> =
        std::collections::BTreeMap::new();

    for (handle, subdir_path) in subdirs {
        let object_path = subdir_path.join("object.json");
        if !object_path.exists() {
            errors.push(format!(
                "per-doc dir is missing object.json: {}",
                subdir_path.display()
            ));
            continue;
        }
        let Some(bytes) = read_document_json(&object_path, errors) else {
            continue;
        };
        let parsed: T =
            match serde_json::from_slice::<serde_json::Value>(&bytes).and_then(|mut value| {
                if collection == Collection::ToolSelection {
                    if let Some(object) = value.as_object_mut() {
                        super::strip_retired_tool_selection_fields(object);
                    }
                }
                serde_json::from_value(value)
            }) {
                Ok(parsed) => parsed,
                Err(error) => {
                    errors.push(format!("invalid {}: {error}", object_path.display()));
                    continue;
                }
            };

        if let Some(prior) = id_to_handle.get(parsed.unique_id()) {
            errors.push(format!(
                "duplicate {} '{}' across {}/ and {}/",
                unique_field,
                parsed.unique_id(),
                prior,
                handle
            ));
            continue;
        }
        id_to_handle.insert(parsed.unique_id().to_string(), handle.clone());

        if parsed.unique_id() != handle {
            errors.push(format!(
                "directory name '{handle}' does not match {} '{}' in {}",
                unique_field,
                parsed.unique_id(),
                object_path.display()
            ));
            continue;
        }

        docs.push(parsed);
    }
    docs
}

pub(crate) fn hydrate_sidecar(value: &mut Option<String>, json_dir: &Path) -> Result<(), String> {
    use std::path::Component;

    let Some(current) = value.as_deref() else {
        return Ok(());
    };
    if !current.starts_with("./") {
        return Ok(());
    }
    let rel = &current[2..];

    let rel_path = std::path::Path::new(rel);
    for comp in rel_path.components() {
        match comp {
            Component::ParentDir => {
                return Err(format!(
                    "sidecar path escapes document directory: {current} (referenced from {})",
                    json_dir.display()
                ));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(format!(
                    "sidecar path must be relative: {current} (referenced from {})",
                    json_dir.display()
                ));
            }
            _ => {}
        }
    }

    let sidecar_path = json_dir.join(rel);
    let bytes = fs::read(&sidecar_path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            format!(
                "sidecar path does not resolve: {} (referenced from {})",
                sidecar_path.display(),
                json_dir.display()
            )
        } else {
            format!("reading {} failed: {error}", sidecar_path.display())
        }
    })?;
    let body = String::from_utf8(bytes)
        .map_err(|_| format!("sidecar is not valid UTF-8: {}", sidecar_path.display()))?;
    *value = Some(body);
    Ok(())
}
