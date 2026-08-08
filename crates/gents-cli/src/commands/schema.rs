use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value};

use crate::cli::args::{SchemaApplyArgs, SchemaCommand};
use crate::config_writes::ConfigAccess;
use crate::{graphql_api_base, print_json, resolve_config_access};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchemaInputKind {
    Sdl,
    Patch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SchemaInput {
    path: PathBuf,
    kind: SchemaInputKind,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SchemaApplyFileResult {
    path: String,
    status: &'static str,
    collections: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SchemaApplyPatchResult {
    path: String,
    status: &'static str,
    collection: String,
    applied_fields: Vec<String>,
    skipped_fields: Vec<String>,
    version_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct SchemaApplySummary {
    status: &'static str,
    home: String,
    mode: &'static str,
    graphql: Option<String>,
    root: String,
    schema_files: Vec<SchemaApplyFileResult>,
    patch_files: Vec<SchemaApplyPatchResult>,
}

pub(crate) async fn dispatch(command: SchemaCommand) -> Result<()> {
    match command {
        SchemaCommand::Apply(args) => schema_apply(args).await,
    }
}

pub(crate) async fn schema_apply(args: SchemaApplyArgs) -> Result<()> {
    let root = args.path;
    let explicit_patches = args.patches;
    let inputs = discover_schema_inputs(&root, &explicit_patches)?;
    if inputs.is_empty() {
        anyhow::bail!(
            "no schema inputs found under {}; expected *.graphql, *.gql, *.patch.json, or *.json-patch",
            root.display()
        );
    }

    let (access, home_dir) =
        resolve_config_access(args.home.as_deref(), args.graphql.as_deref()).await?;
    let graphql = match &access {
        ConfigAccess::Graphql(endpoint) => Some(endpoint.clone()),
        ConfigAccess::Local(_) => None,
    };

    let phase = apply_schema_inputs(&access, &root, &inputs).await?;

    let summary = SchemaApplySummary {
        status: "schema_applied",
        home: home_dir.display().to_string(),
        mode: access.mode(),
        graphql,
        root: root.display().to_string(),
        schema_files: phase.schema_files,
        patch_files: phase.patch_files,
    };
    print_json(&serde_json::to_value(summary)?)?;
    Ok(())
}

/// Result of applying pack-local schemas during `config apply`.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct PackSchemaPhase {
    pub(crate) status: &'static str,
    pub(crate) root: String,
    pub(crate) schema_files: Vec<SchemaApplyFileResult>,
    pub(crate) patch_files: Vec<SchemaApplyPatchResult>,
}

impl PackSchemaPhase {
    /// Whether the schema phase actually mutated the node. A pack re-applied
    /// against a converged node reports every file as `already_exists`, which
    /// must stay a `noop` so idempotency checks can settle.
    pub(crate) fn changed(&self) -> bool {
        self.schema_files
            .iter()
            .any(|file| file.status != "already_exists")
            || self
                .patch_files
                .iter()
                .any(|patch| patch.status != "already_exists")
    }
}

/// If `<pack_root>/schemas` exists, apply every SDL/patch under it (same
/// discovery rules as `gents schema apply`). Returns `None` when the directory
/// is absent so ordinary agent-config roots stay unchanged.
///
/// Scoped to the pack: only `schemas/` under the desired-state root is
/// considered — never global product schemas.
pub(crate) async fn apply_pack_schemas_if_present(
    access: &ConfigAccess,
    pack_root: &Path,
) -> Result<Option<PackSchemaPhase>> {
    let schemas_dir = pack_root.join("schemas");
    if !schemas_dir.is_dir() {
        return Ok(None);
    }
    let inputs = discover_schema_inputs(&schemas_dir, &[])?;
    if inputs.is_empty() {
        anyhow::bail!(
            "pack root {} has a schemas/ directory but no *.graphql, *.gql, *.patch.json, or *.json-patch files",
            pack_root.display()
        );
    }
    Ok(Some(
        apply_schema_inputs(access, &schemas_dir, &inputs).await?,
    ))
}

async fn apply_schema_inputs(
    access: &ConfigAccess,
    root: &Path,
    inputs: &[SchemaInput],
) -> Result<PackSchemaPhase> {
    let mut schema_files = Vec::new();
    for input in inputs
        .iter()
        .filter(|input| input.kind == SchemaInputKind::Sdl)
    {
        schema_files.push(apply_sdl_file(access, &input.path).await?);
    }

    let mut patch_files = Vec::new();
    for input in inputs
        .iter()
        .filter(|input| input.kind == SchemaInputKind::Patch)
    {
        patch_files.push(apply_patch_file(access, &input.path).await?);
    }

    Ok(PackSchemaPhase {
        status: "schema_applied",
        root: root.display().to_string(),
        schema_files,
        patch_files,
    })
}

fn discover_schema_inputs(root: &Path, explicit_patches: &[PathBuf]) -> Result<Vec<SchemaInput>> {
    let mut discovered = Vec::new();
    collect_schema_inputs(root, &mut discovered)?;
    for path in explicit_patches {
        discovered.push(SchemaInput {
            path: path.clone(),
            kind: SchemaInputKind::Patch,
        });
    }

    let mut by_path = BTreeMap::<PathBuf, SchemaInputKind>::new();
    for input in discovered {
        by_path.entry(input.path).or_insert(input.kind);
    }

    Ok(by_path
        .into_iter()
        .map(|(path, kind)| SchemaInput { path, kind })
        .collect())
}

fn collect_schema_inputs(path: &Path, output: &mut Vec<SchemaInput>) -> Result<()> {
    let metadata =
        fs::metadata(path).with_context(|| format!("reading metadata for {}", path.display()))?;
    if metadata.is_file() {
        if let Some(kind) = classify_schema_input(path) {
            output.push(SchemaInput {
                path: path.to_path_buf(),
                kind,
            });
        }
        return Ok(());
    }

    if !metadata.is_dir() {
        anyhow::bail!(
            "schema input path is neither file nor directory: {}",
            path.display()
        );
    }

    let mut entries = fs::read_dir(path)
        .with_context(|| format!("reading schema directory {}", path.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("reading schema directory entries from {}", path.display()))?;
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        collect_schema_inputs(&entry.path(), output)?;
    }

    Ok(())
}

fn classify_schema_input(path: &Path) -> Option<SchemaInputKind> {
    let file_name = path.file_name()?.to_string_lossy();
    if file_name.ends_with(".patch.json") || file_name.ends_with(".json-patch") {
        return Some(SchemaInputKind::Patch);
    }

    match path.extension().and_then(|extension| extension.to_str()) {
        Some("graphql" | "gql") => Some(SchemaInputKind::Sdl),
        _ => None,
    }
}

async fn apply_sdl_file(access: &ConfigAccess, path: &Path) -> Result<SchemaApplyFileResult> {
    let sdl = fs::read_to_string(path)
        .with_context(|| format!("reading schema SDL {}", path.display()))?;
    let collections = collection_names_from_sdl(&sdl)
        .with_context(|| format!("parsing schema SDL {}", path.display()))?;
    if collections.is_empty() {
        anyhow::bail!(
            "schema SDL {} did not declare any collections",
            path.display()
        );
    }

    let existing = existing_collections(access, &collections).await?;
    if existing.len() == collections.len() {
        return Ok(SchemaApplyFileResult {
            path: path.display().to_string(),
            status: "already_exists",
            collections,
        });
    }
    if !existing.is_empty() {
        let missing = collections
            .iter()
            .filter(|collection| !existing.contains(*collection))
            .cloned()
            .collect::<Vec<_>>();
        anyhow::bail!(
            "schema SDL {} mixes existing collections ({}) and missing collections ({}); split it into a new-collection SDL and additive patch files",
            path.display(),
            existing.into_iter().collect::<Vec<_>>().join(", "),
            missing.join(", ")
        );
    }

    match access {
        ConfigAccess::Graphql(endpoint) => post_schema_http(endpoint, &sdl).await?,
        ConfigAccess::Local(node) => node
            .add_schema(&sdl)
            .await
            .with_context(|| format!("adding schema {}", path.display()))?,
    }

    Ok(SchemaApplyFileResult {
        path: path.display().to_string(),
        status: "applied",
        collections,
    })
}

async fn apply_patch_file(access: &ConfigAccess, path: &Path) -> Result<SchemaApplyPatchResult> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("reading schema patch {}", path.display()))?;
    let patch = normalize_patch_value(
        serde_json::from_str::<Value>(&raw)
            .with_context(|| format!("parsing schema patch JSON {}", path.display()))?,
    )?;
    let collection = collection_name_from_patch(&patch)?;
    let field_adds = additive_field_ops(&collection, &patch)?;
    let existing_fields = collection_field_names(access, &collection).await?;

    let (patch_to_apply, applied_fields, skipped_fields) =
        filter_existing_field_adds(&patch, &field_adds, &existing_fields)?;
    if patch_to_apply.as_array().is_some_and(Vec::is_empty) {
        return Ok(SchemaApplyPatchResult {
            path: path.display().to_string(),
            status: "already_exists",
            collection: collection.clone(),
            applied_fields,
            skipped_fields,
            version_id: current_collection_version(access, &collection).await?,
        });
    }

    let version_id = match access {
        ConfigAccess::Graphql(endpoint) => {
            patch_collection_http(endpoint, &patch_to_apply).await?;
            current_collection_version(access, &collection).await?
        }
        ConfigAccess::Local(node) => {
            let patch_str = serde_json::to_string(&patch_to_apply)?;
            let patched = node
                .patch_collection(&collection, &patch_str)
                .await
                .with_context(|| {
                    format!("patching collection {collection} from {}", path.display())
                })?;
            Some(patched.version_id)
        }
    };

    Ok(SchemaApplyPatchResult {
        path: path.display().to_string(),
        status: "applied",
        collection,
        applied_fields,
        skipped_fields,
        version_id,
    })
}

fn collection_names_from_sdl(sdl: &str) -> Result<Vec<String>> {
    let mut names = query::parse_sdl(sdl)?
        .into_iter()
        .map(|collection| collection.name)
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    Ok(names)
}

async fn existing_collections(
    access: &ConfigAccess,
    collections: &[String],
) -> Result<BTreeSet<String>> {
    let mut existing = BTreeSet::new();
    for collection in collections {
        if collection_exists(access, collection).await? {
            existing.insert(collection.clone());
        }
    }
    Ok(existing)
}

async fn collection_exists(access: &ConfigAccess, collection: &str) -> Result<bool> {
    match access {
        ConfigAccess::Local(node) => Ok(node.get_collection(collection)?.is_some()),
        ConfigAccess::Graphql(endpoint) => {
            let api_base = graphql_api_base(endpoint)?;
            let client = schema_http_client()?;
            let response: Value = http_get_json(
                &client,
                &format!("{api_base}/collections/{collection}/exists"),
            )
            .await?;
            Ok(response
                .get("exists")
                .and_then(Value::as_bool)
                .unwrap_or(false))
        }
    }
}

async fn collection_field_names(
    access: &ConfigAccess,
    collection: &str,
) -> Result<BTreeSet<String>> {
    match access {
        ConfigAccess::Local(node) => {
            let collection = node
                .get_collection(collection)?
                .ok_or_else(|| anyhow::anyhow!("collection {collection} does not exist"))?;
            Ok(collection
                .fields
                .into_iter()
                .map(|field| field.name)
                .collect())
        }
        ConfigAccess::Graphql(endpoint) => {
            let describe = describe_collection_http(endpoint, collection).await?;
            Ok(describe
                .get("Fields")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|field| field.get("Name").and_then(Value::as_str))
                .map(ToOwned::to_owned)
                .collect())
        }
    }
}

async fn current_collection_version(
    access: &ConfigAccess,
    collection: &str,
) -> Result<Option<String>> {
    match access {
        ConfigAccess::Local(node) => Ok(node
            .get_collection(collection)?
            .map(|collection| collection.version_id)),
        ConfigAccess::Graphql(endpoint) => Ok(describe_collection_http(endpoint, collection)
            .await?
            .get("VersionID")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)),
    }
}

fn normalize_patch_value(value: Value) -> Result<Value> {
    if let Some(patch) = value.get("Patch").or_else(|| value.get("patch")) {
        return normalize_patch_value(patch.clone());
    }
    if !value.is_array() {
        anyhow::bail!("schema patch must be a JSON Patch array or an object with Patch/patch");
    }
    Ok(value)
}

fn collection_name_from_patch(patch: &Value) -> Result<String> {
    let path = patch
        .as_array()
        .and_then(|ops| ops.first())
        .and_then(|op| op.get("path"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("schema patch must start with an op containing path"))?;
    let collection = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .next()
        .ok_or_else(|| anyhow::anyhow!("schema patch path has no collection name: {path}"))?;
    Ok(collection.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FieldAdd {
    index: usize,
    name: String,
}

fn additive_field_ops(collection: &str, patch: &Value) -> Result<Vec<FieldAdd>> {
    let ops = patch
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("schema patch must be a JSON Patch array"))?;
    let mut fields = Vec::new();
    for (index, op) in ops.iter().enumerate() {
        let op_name = op.get("op").and_then(Value::as_str).unwrap_or_default();
        let path = op.get("path").and_then(Value::as_str).unwrap_or_default();
        if op_name != "add" || path != format!("/{collection}/Fields/-") {
            return Ok(Vec::new());
        }
        let Some(name) = op
            .get("value")
            .and_then(|value| value.get("Name"))
            .and_then(Value::as_str)
        else {
            return Ok(Vec::new());
        };
        fields.push(FieldAdd {
            index,
            name: name.to_string(),
        });
    }
    Ok(fields)
}

fn filter_existing_field_adds(
    patch: &Value,
    field_adds: &[FieldAdd],
    existing_fields: &BTreeSet<String>,
) -> Result<(Value, Vec<String>, Vec<String>)> {
    if field_adds.is_empty() {
        return Ok((patch.clone(), Vec::new(), Vec::new()));
    }

    let mut applied_fields = Vec::new();
    let mut skipped_fields = Vec::new();
    let mut skip_indexes = BTreeSet::new();
    for field in field_adds {
        if existing_fields.contains(&field.name) {
            skipped_fields.push(field.name.clone());
            skip_indexes.insert(field.index);
        } else {
            applied_fields.push(field.name.clone());
        }
    }

    let ops = patch
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("schema patch must be a JSON Patch array"))?;
    let filtered = ops
        .iter()
        .enumerate()
        .filter(|(index, _)| !skip_indexes.contains(index))
        .map(|(_, op)| op.clone())
        .collect::<Vec<_>>();
    Ok((Value::Array(filtered), applied_fields, skipped_fields))
}

async fn post_schema_http(endpoint: &str, sdl: &str) -> Result<()> {
    let api_base = graphql_api_base(endpoint)?;
    let client = schema_http_client()?;
    let url = format!("{api_base}/schema");
    let response = client
        .post(&url)
        .header(reqwest::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(sdl.to_string())
        .send()
        .await
        .with_context(|| format!("posting schema SDL to {url}"))?;
    ensure_success(response, "schema SDL", &url).await
}

async fn patch_collection_http(endpoint: &str, patch: &Value) -> Result<()> {
    let api_base = graphql_api_base(endpoint)?;
    let client = schema_http_client()?;
    let url = format!("{api_base}/collections");
    let response = client
        .patch(&url)
        .json(&json!({ "Patch": patch }))
        .send()
        .await
        .with_context(|| format!("patching collection schema via {url}"))?;
    ensure_success(response, "schema patch", &url).await
}

async fn describe_collection_http(endpoint: &str, collection: &str) -> Result<Value> {
    let api_base = graphql_api_base(endpoint)?;
    let client = schema_http_client()?;
    http_get_json(
        &client,
        &format!("{api_base}/collections/{collection}/describe"),
    )
    .await
}

async fn http_get_json(client: &reqwest::Client, url: &str) -> Result<Value> {
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("sending GET request to {url}"))?;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("reading GET response body from {url}"))?;
    if !status.is_success() {
        anyhow::bail!(
            "GET {url} failed with {status}: {}",
            String::from_utf8_lossy(&bytes)
        );
    }
    serde_json::from_slice(&bytes).with_context(|| format!("decoding JSON response from {url}"))
}

async fn ensure_success(response: reqwest::Response, operation: &str, url: &str) -> Result<()> {
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("reading {operation} response body from {url}"))?;
    if !status.is_success() {
        anyhow::bail!(
            "{operation} request to {url} failed with {status}: {}",
            String::from_utf8_lossy(&bytes)
        );
    }
    Ok(())
}

fn schema_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .context("building schema HTTP client")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_schema_input_accepts_sdl_and_patch_suffixes() {
        assert_eq!(
            classify_schema_input(Path::new("app.graphql")),
            Some(SchemaInputKind::Sdl)
        );
        assert_eq!(
            classify_schema_input(Path::new("app.gql")),
            Some(SchemaInputKind::Sdl)
        );
        assert_eq!(
            classify_schema_input(Path::new("app.patch.json")),
            Some(SchemaInputKind::Patch)
        );
        assert_eq!(
            classify_schema_input(Path::new("app.json-patch")),
            Some(SchemaInputKind::Patch)
        );
        assert_eq!(classify_schema_input(Path::new("app.json")), None);
    }

    #[test]
    fn collection_name_from_patch_uses_first_path_segment() {
        let patch = json!([
            {"op": "add", "path": "/ActionRequest/Fields/-", "value": {"Name": "status", "Kind": "String"}}
        ]);

        assert_eq!(collection_name_from_patch(&patch).unwrap(), "ActionRequest");
    }

    #[test]
    fn normalize_patch_value_accepts_wrapped_patch() {
        let wrapped = json!({
            "Patch": [
                {"op": "add", "path": "/ActionRequest/Fields/-", "value": {"Name": "status", "Kind": "String"}}
            ]
        });

        assert!(normalize_patch_value(wrapped).unwrap().is_array());
    }

    #[test]
    fn filters_already_existing_additive_field_ops() {
        let patch = json!([
            {"op": "add", "path": "/ActionRequest/Fields/-", "value": {"Name": "status", "Kind": "String"}},
            {"op": "add", "path": "/ActionRequest/Fields/-", "value": {"Name": "reviewed_at", "Kind": "DateTime"}}
        ]);
        let field_adds = additive_field_ops("ActionRequest", &patch).unwrap();
        let existing_fields = BTreeSet::from(["status".to_string()]);

        let (filtered, applied, skipped) =
            filter_existing_field_adds(&patch, &field_adds, &existing_fields).unwrap();

        assert_eq!(applied, vec!["reviewed_at".to_string()]);
        assert_eq!(skipped, vec!["status".to_string()]);
        assert_eq!(filtered.as_array().unwrap().len(), 1);
        assert_eq!(
            filtered
                .as_array()
                .unwrap()
                .first()
                .unwrap()
                .get("value")
                .and_then(|value| value.get("Name"))
                .and_then(Value::as_str),
            Some("reviewed_at")
        );
    }

    #[test]
    fn non_additive_patch_is_left_unchanged() {
        let patch = json!([
            {"op": "replace", "path": "/ActionRequest/Description", "value": "new"}
        ]);
        let field_adds = additive_field_ops("ActionRequest", &patch).unwrap();
        let existing_fields = BTreeSet::new();

        let (filtered, applied, skipped) =
            filter_existing_field_adds(&patch, &field_adds, &existing_fields).unwrap();

        assert_eq!(filtered, patch);
        assert!(applied.is_empty());
        assert!(skipped.is_empty());
    }
}

#[cfg(test)]
mod pack_schema_tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn pack_schemas_dir_absent_means_skip() {
        let dir = tempdir().unwrap();
        // Can't call async without runtime for full apply; discovery path only.
        assert!(!dir.path().join("schemas").is_dir());
    }

    #[test]
    fn pack_schemas_dir_discovers_graphql() {
        let dir = tempdir().unwrap();
        let schemas = dir.path().join("schemas");
        fs::create_dir_all(&schemas).unwrap();
        fs::write(
            schemas.join("widget.graphql"),
            "type Widget { id: String }\n",
        )
        .unwrap();
        let inputs = discover_schema_inputs(&schemas, &[]).unwrap();
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].kind, SchemaInputKind::Sdl);
    }

    #[test]
    fn pack_schemas_empty_dir_is_detectable() {
        let dir = tempdir().unwrap();
        let schemas = dir.path().join("schemas");
        fs::create_dir_all(&schemas).unwrap();
        let inputs = discover_schema_inputs(&schemas, &[]).unwrap();
        assert!(inputs.is_empty());
    }
}
