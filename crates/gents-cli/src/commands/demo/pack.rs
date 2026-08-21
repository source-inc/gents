//! Non-interactive pack runs: `gents demo run <pack>`, `gents demo init`,
//! and `gents demo seed` against an already-serving node.
//!
//! A pack is a self-contained desired-state root (its own `schemas/` plus the
//! config documents) with an `experiment.json` describing how to drive it.
//!
//! Two orderings are load-bearing and neither is visible from the outside: the
//! pack applies *after* the runtime is ready, so its backend is unprobed for up
//! to one probe interval while the server already reports `serving`; and a seed
//! written before the event source observes its collection is dropped in
//! silence, because triggers are created/first-seen only. `demo seed` waits
//! for `/healthz` and an enabled EventTrigger, then confirms a correlated
//! AgentRequest actually fired.
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};

use super::fleet::{spawn_server_with_args_and_env, wait_http, wait_runtime_ready};
use super::secscan;
use super::util::{path_arg, run_cli_json};
use crate::cli::args::{DemoInitArgs, DemoRunArgs, DemoSeedArgs};
use crate::desired_state::interpolate::interpolate_with;
use crate::graphql_access::post_graphql;
use gents::graphql::{escape_graphql_string, validate_collection_identifier};

#[derive(Debug, Deserialize)]
struct PackManifest {
    name: String,
    #[serde(default)]
    description: String,
    init: PackInit,
    seed: PackSeed,
    #[serde(default)]
    default_prompt: String,
    expect: PackExpect,
    #[serde(default = "default_timeout")]
    await_timeout_secs: u64,
    #[serde(default)]
    scan: Option<PackScan>,
}

fn default_timeout() -> u64 {
    240
}

#[derive(Debug, Deserialize)]
struct PackScan {
    root: String,
    #[serde(default = "default_scan_payload_chars")]
    max_payload_chars: String, // string for ${VAR:-default} interpolation parity
}

fn default_scan_payload_chars() -> String {
    "49152".to_string()
}

/// Renders a scan's counters as `seed.fields` entries so the manifest can
/// interpolate them into the seeded document without any special-casing in
/// `seed_mutation`. `slug_counts` keeps the pre-sorted (count desc, then
/// slug) order `format_payload` produced.
fn scan_seed_fields(output: &secscan::ScanOutput) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    fields.insert("candidates".to_string(), output.payload.clone());
    fields.insert(
        "candidate_total".to_string(),
        output.candidate_total.to_string(),
    );
    fields.insert(
        "candidate_files".to_string(),
        output.candidate_files.to_string(),
    );
    fields.insert(
        "slug_counts".to_string(),
        output
            .slug_counts
            .iter()
            .map(|(slug, count)| format!("{slug}={count}"))
            .collect::<Vec<_>>()
            .join(" "),
    );
    fields.insert(
        "overflow_count".to_string(),
        output.overflow_count.to_string(),
    );
    fields
}

#[derive(Debug, Deserialize)]
struct PackInit {
    inference_url: String,
    model_name: String,
    #[serde(default = "default_tool_package")]
    tool_package: String,
    #[serde(default)]
    api_key_env_var: Option<String>,
    #[serde(default)]
    backend_preset: Option<String>,
    #[serde(default)]
    openai_wire_api: Option<String>,
    /// `gents init --tool-root`. Required for readonly/write/yolo when not
    /// inferable. Relative paths resolve against the process cwd.
    #[serde(default)]
    tool_root: Option<String>,
    /// Environment variable receiving the canonical tool root while the
    /// child applies the pack. This keeps pack interpolation aligned with
    /// the root passed to `gents init`.
    #[serde(default)]
    tool_root_env_var: Option<String>,
    /// Files or directories that must exist under the resolved tool root.
    /// Packs use this to fail fast when invoked from the wrong checkout.
    #[serde(default)]
    tool_root_markers: Vec<String>,
}

fn default_tool_package() -> String {
    "minimal".to_string()
}

#[derive(Debug, Deserialize)]
struct PackSeed {
    collection: String,
    job_id_field: String,
    prompt_field: String,
    #[serde(default)]
    fields: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct PackExpect {
    trigger_ids: Vec<String>,
    #[serde(default)]
    trigger_request_counts: BTreeMap<String, usize>,
    #[serde(default)]
    trigger_request_count_sources: BTreeMap<String, TriggerRequestCountSource>,
    #[serde(default)]
    collection_counts: BTreeMap<String, u64>,
    #[serde(default)]
    projections: Vec<String>,
    #[serde(default)]
    signed_provenance: bool,
    #[serde(default)]
    required_tool_call_trigger_ids: Vec<String>,
    #[serde(default)]
    source_edges: Vec<SourceEdgeExpectation>,
    #[serde(default)]
    fan_in: Option<FanInExpectation>,
    #[serde(default)]
    prompt_tool_contracts: Vec<PromptToolContract>,
    #[serde(default)]
    background_completion: Option<BackgroundCompletionExpectation>,
    #[serde(default)]
    tool_calls: Vec<ToolCallExpectation>,
    #[serde(default)]
    result_documents: Vec<ResultDocumentExpectation>,
}

#[derive(Debug, Deserialize)]
struct TriggerRequestCountSource {
    collection: String,
    correlation_field: String,
    expected_count_field: String,
}

#[derive(Debug, Deserialize)]
struct ResultDocumentExpectation {
    collection: String,
    correlation_field: String,
    fields: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ToolCallExpectation {
    trigger_id: String,
    tool_name: String,
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    file: Option<String>,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    result_contains: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct PromptToolContract {
    task_id: String,
    required_tool_names: Vec<String>,
    #[serde(default)]
    required_query_collections: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct FanInExpectation {
    member_collection: String,
    result_collection: String,
    report_collection: String,
    correlation_field: String,
    expected_count_field: String,
    #[serde(default)]
    min_expected_count: Option<usize>,
    #[serde(default)]
    max_expected_count: Option<usize>,
    consumer_trigger_id: String,
    #[serde(default)]
    member_required_fields: Vec<String>,
    #[serde(default)]
    verification: Option<VerificationExpectation>,
}

#[derive(Debug, Deserialize)]
struct VerificationExpectation {
    candidate_collection: String,
    decision_collection: String,
    summary_collection: String,
    confirmed_collection: String,
    final_consumer_trigger_id: String,
    finding_id_field: String,
    verdict_field: String,
    evidence_field: String,
    confirmed_count_field: String,
    refuted_count_field: String,
}

#[derive(Debug, Clone)]
struct FanInEvidence {
    correlation: String,
    expected_count: usize,
    member_count: usize,
    result_count: usize,
    consumer_request_id: String,
    report_count: usize,
    candidate_count: Option<usize>,
    confirmed_count: Option<usize>,
    refuted_count: Option<usize>,
    decision_count: Option<usize>,
    verification_summary_count: Option<usize>,
    final_consumer_request_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct BackgroundCompletionExpectation {
    min_completed_subagent_requests: usize,
    min_completed_wakes: usize,
    min_acknowledged_notifications: usize,
    #[serde(default)]
    max_pending_notifications: usize,
    #[serde(default)]
    max_stranded_notifications: usize,
}

#[derive(Debug, Clone)]
struct BackgroundCompletionEvidence {
    completed_subagent_request_ids: Vec<String>,
    failed_subagent_request_ids: Vec<String>,
    completed_wake_request_ids: Vec<String>,
    pending_notifications: usize,
    acknowledged_notifications: usize,
    stranded_notifications: usize,
    diagnostics: Value,
}

impl BackgroundCompletionEvidence {
    fn satisfies(&self, expected: &BackgroundCompletionExpectation) -> bool {
        self.completed_subagent_request_ids.len() >= expected.min_completed_subagent_requests
            && self.completed_wake_request_ids.len() >= expected.min_completed_wakes
            && self.acknowledged_notifications >= expected.min_acknowledged_notifications
            && self.pending_notifications <= expected.max_pending_notifications
            && self.stranded_notifications <= expected.max_stranded_notifications
    }
}

#[derive(Debug, Deserialize)]
struct SourceEdgeExpectation {
    producer_trigger_id: String,
    producer_tool_name: String,
    consumer_trigger_id: String,
    source_collection: String,
}

/// Resolve a pack by path, or by name under `demo/`.
fn resolve_pack(target: &str) -> Result<PathBuf> {
    let direct = PathBuf::from(target);
    if direct.join("experiment.json").is_file() {
        return Ok(direct);
    }
    let under_demo = PathBuf::from("demo").join(target);
    if under_demo.join("experiment.json").is_file() {
        return Ok(under_demo);
    }
    bail!(
        "no pack at {} or {} (a pack is a directory containing experiment.json)",
        direct.display(),
        under_demo.display()
    )
}

fn load_manifest(pack: &Path) -> Result<PackManifest> {
    load_manifest_with(pack, &|name| std::env::var(name).ok())
}

fn load_manifest_with(
    pack: &Path,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<PackManifest> {
    let path = pack.join("experiment.json");
    let raw =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    let expanded = interpolate_with(&raw, lookup).map_err(|missing| {
        anyhow::anyhow!(
            "{} references unset environment variable(s): {}",
            path.display(),
            missing.join(", ")
        )
    })?;
    let manifest =
        serde_json::from_str(&expanded).with_context(|| format!("parsing {}", path.display()))?;
    validate_manifest(&manifest).with_context(|| format!("validating {}", path.display()))?;
    validate_prompt_tool_contracts_with(pack, &manifest, lookup)
        .with_context(|| format!("validating prompt/tool contracts in {}", path.display()))?;
    Ok(manifest)
}

fn read_pack_json(path: &Path) -> Result<Value> {
    read_pack_json_with(path, &|name| std::env::var(name).ok())
}

fn read_pack_json_with(path: &Path, lookup: &dyn Fn(&str) -> Option<String>) -> Result<Value> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading pack document {}", path.display()))?;
    let expanded = interpolate_with(&raw, lookup).map_err(|missing| {
        anyhow::anyhow!(
            "{} references unset environment variable(s): {}",
            path.display(),
            missing.join(", ")
        )
    })?;
    serde_json::from_str(&expanded)
        .with_context(|| format!("parsing pack document {}", path.display()))
}

fn required_json_string<'a>(value: &'a Value, field: &str, path: &Path) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("{} has no non-empty {field}", path.display()))
}

/// Keep model instructions coupled to the exact tools exposed by the pack.
///
/// A config can be structurally valid while asking the model to call a stale
/// tool name. These contracts follow Task -> Behavior -> ToolSelection ->
/// DatastoreToolSurface and require the exact advertised name to occur in the
/// task or system prompt. Surface collections must also exist in `schemas/`.
fn validate_prompt_tool_contracts_with(
    pack: &Path,
    manifest: &PackManifest,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<()> {
    if manifest.expect.prompt_tool_contracts.is_empty() {
        return Ok(());
    }
    let schemas = std::fs::read_dir(pack.join("schemas"))
        .with_context(|| format!("reading {}", pack.join("schemas").display()))?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "graphql"))
        .map(|entry| std::fs::read_to_string(entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?
        .join("\n");

    for contract in &manifest.expect.prompt_tool_contracts {
        let task_dir = pack.join("tasks").join(&contract.task_id);
        let task_path = task_dir.join("object.json");
        let task = read_pack_json_with(&task_path, lookup)?;
        let behavior_id = required_json_string(&task, "behavior_id", &task_path)?;
        let task_prompt_path = task_dir.join(
            required_json_string(&task, "prompt_template", &task_path)?.trim_start_matches("./"),
        );
        let task_prompt = std::fs::read_to_string(&task_prompt_path)
            .with_context(|| format!("reading {}", task_prompt_path.display()))?;

        let behavior_dir = pack.join("agent-behaviors").join(behavior_id);
        let behavior_path = behavior_dir.join("object.json");
        let behavior = read_pack_json_with(&behavior_path, lookup)?;
        let system_prompt_path = behavior_dir.join(
            required_json_string(&behavior, "system_prompt", &behavior_path)?
                .trim_start_matches("./"),
        );
        let system_prompt = std::fs::read_to_string(&system_prompt_path)
            .with_context(|| format!("reading {}", system_prompt_path.display()))?;
        let selection_id = required_json_string(&behavior, "tool_selection_id", &behavior_path)?;
        let selection_path = pack
            .join("tool-selections")
            .join(selection_id)
            .join("object.json");
        let selection = read_pack_json_with(&selection_path, lookup)?;

        let mut advertised = std::collections::BTreeSet::new();
        let query_collections = selection
            .get("defra_query_collections")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect::<std::collections::BTreeSet<_>>();
        if selection
            .get("enable_defra_query")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            advertised.insert("defra_query".to_string());
        }
        for surface_id in selection
            .get("datastore_tool_surface_ids")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            let surface_path = pack
                .join("datastore-tool-surfaces")
                .join(surface_id)
                .join("object.json");
            let surface = read_pack_json_with(&surface_path, lookup)?;
            for entry in surface
                .get("entries")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let tool_name = entry
                    .get("tool_name")
                    .and_then(Value::as_str)
                    .context("datastore tool entry has no tool_name")?;
                let collection = entry
                    .get("collection")
                    .and_then(Value::as_str)
                    .context("datastore tool entry has no collection")?;
                if !schemas.contains(&format!("type {collection} {{")) {
                    bail!(
                        "tool {tool_name} targets collection {collection}, but the pack has no matching schema"
                    );
                }
                let type_start = schemas
                    .find(&format!("type {collection} {{"))
                    .context("schema type disappeared during tool validation")?;
                let type_body = schemas[type_start..]
                    .split_once('}')
                    .map(|(body, _)| body)
                    .context("schema type has no closing brace")?;
                let mut field_names = Vec::new();
                for field in entry
                    .get("fields")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    if let Some(name) = field.as_str() {
                        field_names.push(name.to_string());
                    } else if let Some(name) = field.get("name").and_then(Value::as_str) {
                        field_names.push(name.to_string());
                    } else {
                        bail!("datastore tool field has no name");
                    }
                }
                for field in entry
                    .get("filter_fields")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    let field_name = field
                        .get("name")
                        .and_then(Value::as_str)
                        .context("datastore tool filter field has no name")?;
                    field_names.push(field_name.to_string());
                }
                for field_name in field_names {
                    if !type_body.lines().any(|line| {
                        line.trim_start()
                            .strip_prefix(&field_name)
                            .is_some_and(|rest| rest.trim_start().starts_with(':'))
                    }) {
                        bail!(
                            "tool {tool_name} exposes field {field_name}, but {collection} has no matching schema field"
                        );
                    }
                }
                advertised.insert(tool_name.to_string());
            }
        }

        let combined_prompt = format!("{system_prompt}\n{task_prompt}");
        for tool_name in &contract.required_tool_names {
            if !advertised.contains(tool_name) {
                bail!(
                    "task {} requires tool {tool_name}, but behavior {behavior_id} does not advertise it",
                    contract.task_id
                );
            }
            if !combined_prompt.contains(&format!("`{tool_name}`")) {
                bail!(
                    "task {} exposes tool {tool_name}, but its prompts do not name it exactly as `{tool_name}`",
                    contract.task_id
                );
            }
        }
        for collection in &contract.required_query_collections {
            if !query_collections.contains(collection.as_str()) {
                bail!(
                    "task {} asks defra_query for {collection}, but behavior {behavior_id} cannot query it",
                    contract.task_id
                );
            }
            if !combined_prompt.contains(&format!("`{collection}`")) {
                bail!(
                    "task {} can query {collection}, but its prompts do not name that collection exactly",
                    contract.task_id
                );
            }
        }
    }
    Ok(())
}

fn trigger_source_collections(pack: &Path, trigger_ids: &[String]) -> Result<Vec<String>> {
    let mut collections = std::collections::BTreeSet::new();
    for trigger_id in trigger_ids {
        let path = pack
            .join("event_triggers")
            .join(trigger_id)
            .join("object.json");
        let trigger = read_pack_json(&path)?;
        let source_collection = required_json_string(&trigger, "source_collection", &path)?;
        validate_collection_identifier(source_collection)?;
        collections.insert(source_collection.to_string());
    }
    Ok(collections.into_iter().collect())
}

fn validate_manifest(manifest: &PackManifest) -> Result<()> {
    if !manifest.expect.source_edges.is_empty() && !manifest.expect.signed_provenance {
        bail!("expect.source_edges requires expect.signed_provenance=true");
    }
    validate_tool_package(&manifest.init.tool_package)?;
    let mut result_collections = BTreeSet::new();
    for result in &manifest.expect.result_documents {
        validate_collection_identifier(&result.collection)?;
        gents::graphql::validate_graphql_name(&result.correlation_field)?;
        if result.fields.is_empty() {
            bail!(
                "expect.result_documents entry for {} must select at least one field",
                result.collection
            );
        }
        for field in &result.fields {
            gents::graphql::validate_graphql_name(field)?;
        }
        if !result_collections.insert(&result.collection) {
            bail!(
                "expect.result_documents contains duplicate collection {}",
                result.collection
            );
        }
    }
    for (trigger_id, count) in &manifest.expect.trigger_request_counts {
        if !manifest.expect.trigger_ids.contains(trigger_id) {
            bail!("expect.trigger_request_counts names unknown trigger {trigger_id}");
        }
        if *count == 0 {
            bail!("expect.trigger_request_counts[{trigger_id}] must be greater than zero");
        }
    }
    for (trigger_id, source) in &manifest.expect.trigger_request_count_sources {
        if !manifest.expect.trigger_ids.contains(trigger_id) {
            bail!("expect.trigger_request_count_sources names unknown trigger {trigger_id}");
        }
        if manifest
            .expect
            .trigger_request_counts
            .contains_key(trigger_id)
        {
            bail!("trigger {trigger_id} has both a fixed request count and a request count source");
        }
        validate_collection_identifier(&source.collection)?;
        gents::graphql::validate_graphql_name(&source.correlation_field)?;
        gents::graphql::validate_graphql_name(&source.expected_count_field)?;
    }
    if let Some(fan_in) = &manifest.expect.fan_in {
        if fan_in.min_expected_count == Some(0) || fan_in.max_expected_count == Some(0) {
            bail!("expect.fan_in count bounds must be greater than zero");
        }
        if matches!(
            (fan_in.min_expected_count, fan_in.max_expected_count),
            (Some(minimum), Some(maximum)) if minimum > maximum
        ) {
            bail!("expect.fan_in minimum count cannot exceed its maximum count");
        }
    }
    if let Some(expected) = &manifest.expect.background_completion {
        if expected.min_completed_subagent_requests == 0
            || expected.min_completed_wakes == 0
            || expected.min_acknowledged_notifications == 0
        {
            bail!("expect.background_completion minimums must all be greater than zero");
        }
    }
    if tool_package_needs_root(&manifest.init.tool_package)
        && manifest
            .init
            .tool_root
            .as_deref()
            .map(str::trim)
            .unwrap_or("")
            .is_empty()
    {
        bail!(
            "init.tool_package={} requires init.tool_root (a read-only/write ceiling needs a workspace root)",
            manifest.init.tool_package
        );
    }
    Ok(())
}

fn validate_tool_package(package: &str) -> Result<()> {
    match package {
        "minimal" | "introspection" | "readonly" | "write" | "yolo" => Ok(()),
        other => bail!("unknown init.tool_package {other}"),
    }
}

fn tool_package_needs_root(package: &str) -> bool {
    matches!(package, "readonly" | "write" | "yolo")
}

fn resolve_pack_tool_root(
    pack: &Path,
    declared: Option<&str>,
    markers: &[String],
) -> Result<PathBuf> {
    let raw = declared.map(str::trim).filter(|value| !value.is_empty());
    let path = match raw {
        Some(raw) => {
            let path = PathBuf::from(raw);
            if path.is_absolute() {
                path
            } else {
                std::env::current_dir()
                    .context("resolving init.tool_root against the process cwd")?
                    .join(path)
            }
        }
        None => pack.join("../.."),
    };
    let root = path
        .canonicalize()
        .with_context(|| format!("resolving pack tool root {}", path.display()))?;
    if !root.is_dir() {
        bail!("pack tool root is not a directory: {}", root.display());
    }
    let missing = markers
        .iter()
        .filter(|marker| !root.join(marker).exists())
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!(
            "pack tool root {} is missing required marker(s): {}",
            root.display(),
            missing.join(", ")
        );
    }
    Ok(root)
}

pub(crate) async fn list(root: &Path) -> Result<()> {
    let mut rows: Vec<(String, String)> = Vec::new();
    let entries =
        std::fs::read_dir(root).with_context(|| format!("reading pack root {}", root.display()))?;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.join("experiment.json").is_file() {
            continue;
        }
        match load_manifest(&path) {
            Ok(manifest) => rows.push((manifest.name, manifest.description)),
            Err(error) => rows.push((
                path.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into(),
                format!("(unreadable: {error})"),
            )),
        }
    }
    rows.sort();
    if rows.is_empty() {
        println!("no packs under {}", root.display());
        return Ok(());
    }
    for (name, description) in rows {
        println!("{name:<20} {description}");
    }
    Ok(())
}

/// Wait for the trigger engine to announce it is watching `collection` and
/// has opened the global update subscription that can actually receive its
/// documents.
///
/// Both signals are strictly later than "serving": the event source only
/// starts observing once the behaviors behind the triggers are runnable,
/// which needs the pack's backend probed. The per-collection messages are
/// emitted just before the subscription is opened, so treating either one as
/// the go-signal can race the first seed. Seeding earlier is silently dropped
/// rather than rejected.
async fn wait_for_event_source(log: &Path, collection: &str, deadline: Duration) -> Result<()> {
    let started = Instant::now();
    while started.elapsed() < deadline {
        if let Ok(text) = std::fs::read_to_string(log) {
            if event_source_ready(&text, collection) {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    bail!(
        "timed out after {}s waiting for the event source to observe {collection} \
         and open its global Update subscription. \
         The pack's backend may still be unprobed — check {} for \
         'behavior unavailable after runtime reconcile'.",
        deadline.as_secs(),
        log.display()
    )
}

fn event_source_ready(log: &str, collection: &str) -> bool {
    let mut observes_target = false;
    let mut subscription_open = false;
    for line in log.lines() {
        let line = strip_ansi(line);
        observes_target |= observes_collection(&line, collection);
        subscription_open |= line.contains("event source opened global Update subscription");
    }
    observes_target && subscription_open
}

/// The runtime writes coloured tracing output even to a file, which splits
/// `source_collection=Name` with escape sequences. Strip them before matching.
fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\u{1b}' {
            out.push(ch);
            continue;
        }
        // CSI: ESC '[' params… final-byte. The '[' introducer is itself inside
        // the final-byte range, so it must be consumed before scanning.
        if chars.peek() == Some(&'[') {
            chars.next();
        }
        for next in chars.by_ref() {
            if ('@'..='~').contains(&next) {
                break;
            }
        }
    }
    out
}

/// Match the observe line for exactly `collection`, so `ExperimentJob` does
/// not satisfy a wait on a collection whose name it prefixes.
fn observes_collection(line: &str, collection: &str) -> bool {
    if !line.contains("event source now observing") {
        return false;
    }
    let needle = format!("source_collection={collection}");
    let Some(idx) = line.find(&needle) else {
        return false;
    };
    line[idx + needle.len()..]
        .chars()
        .next()
        .is_none_or(|next| !next.is_alphanumeric() && next != '_')
}

fn seed_mutation(seed: &PackSeed, job_id: &str, prompt: &str) -> Result<String> {
    // The collection and every field key below are interpolated in identifier
    // position; validate them so a malformed pack manifest cannot inject
    // GraphQL through the seed create.
    validate_collection_identifier(&seed.collection)?;
    gents::graphql::validate_graphql_name(&seed.job_id_field)?;
    gents::graphql::validate_graphql_name(&seed.prompt_field)?;
    for key in seed.fields.keys() {
        gents::graphql::validate_graphql_name(key)?;
    }
    let mut fields = vec![
        format!(
            "{}: \"{}\"",
            seed.job_id_field,
            escape_graphql_string(job_id)
        ),
        format!(
            "{}: \"{}\"",
            seed.prompt_field,
            escape_graphql_string(prompt)
        ),
    ];
    for (key, value) in &seed.fields {
        fields.push(format!("{key}: \"{}\"", escape_graphql_string(value)));
    }
    Ok(format!(
        "mutation {{ create_{}(input: {{ {} }}) {{ _docID }} }}",
        seed.collection,
        fields.join(", ")
    ))
}

fn pack_init_cli_args(
    home: &Path,
    manifest: &PackManifest,
    tool_root: Option<&Path>,
) -> Vec<String> {
    let mut init_args: Vec<String> = vec![
        "init".into(),
        "--home".into(),
        path_arg(home),
        "--dangerously-overwrite".into(),
        "--inference-url".into(),
        manifest.init.inference_url.clone(),
        "--model-name".into(),
        manifest.init.model_name.clone(),
        "--tool-package".into(),
        manifest.init.tool_package.clone(),
    ];
    if let Some(root) = tool_root {
        init_args.push("--tool-root".into());
        init_args.push(path_arg(root));
    }
    if let Some(preset) = manifest.init.backend_preset.as_deref() {
        init_args.push("--backend-preset".into());
        init_args.push(preset.into());
    }
    if let Some(wire) = manifest.init.openai_wire_api.as_deref() {
        init_args.push("--openai-wire-api".into());
        init_args.push(wire.into());
    }
    if let Some(api_key_env_var) = manifest.init.api_key_env_var.as_deref() {
        init_args.push("--api-key-env-var".into());
        init_args.push(api_key_env_var.into());
    }
    init_args
}

fn refuse_stale_review_root(home: &Path, env_root: Option<&Path>) -> Result<()> {
    let stamp = home.join("review-root");
    if !stamp.is_file() {
        return Ok(());
    }
    let Some(env_root) = env_root else {
        return Ok(());
    };
    let applied =
        std::fs::read_to_string(&stamp).with_context(|| format!("reading {}", stamp.display()))?;
    let applied = applied.trim();
    if applied.is_empty() {
        return Ok(());
    }
    if !same_path(Path::new(applied), env_root) {
        bail!(
            "REVIEW_ROOT {} does not match the pack node at {}; re-run make review-serve (REVIEW_RESET=1 if you meant to retarget)",
            env_root.display(),
            applied
        );
    }
    Ok(())
}

fn same_path(left: &Path, right: &Path) -> bool {
    match (left.canonicalize(), right.canonicalize()) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

fn event_trigger_ready(response: &Value, collection: &str) -> bool {
    response
        .pointer("/data/EventTrigger")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .any(|row| {
            row.get("source_collection").and_then(Value::as_str) == Some(collection)
                && row.get("enabled").and_then(Value::as_bool) == Some(true)
        })
}

fn has_correlated_request(response: &Value) -> bool {
    response
        .pointer("/data/AgentRequest")
        .and_then(Value::as_array)
        .is_some_and(|rows| !rows.is_empty())
}

async fn wait_until<F, Fut>(label: &str, deadline: Duration, mut probe: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool>>,
{
    let started = Instant::now();
    let mut last = None::<String>;
    while started.elapsed() < deadline {
        match probe().await {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(error) => last = Some(error.to_string()),
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    match last {
        Some(error) => bail!("{label} timed out after {}s: {error}", deadline.as_secs()),
        None => bail!("{label} timed out after {}s", deadline.as_secs()),
    }
}

async fn wait_http_ok(url: &str, deadline: Duration) -> Result<()> {
    let client = reqwest::Client::new();
    wait_until(url, deadline, || {
        let client = client.clone();
        async move {
            Ok(client
                .get(url)
                .timeout(Duration::from_secs(2))
                .send()
                .await
                .map(|response| response.status().is_success())
                .unwrap_or(false))
        }
    })
    .await
}

#[derive(Debug, Clone)]
struct StageResult {
    trigger_id: String,
    request_id: String,
    lifecycle_state: String,
    caused_by_source_doc_id: Option<String>,
}

#[derive(Debug, Clone)]
struct StageProvenance {
    request_id: String,
    request_doc_id: String,
    rendered_request_count: usize,
    request_commit_cids: Vec<String>,
    request_fact_counts: BTreeMap<String, usize>,
    signer_identity: String,
}

#[derive(Debug, Clone)]
struct SourceEdgeEvidence {
    producer_trigger_id: String,
    producer_request_id: String,
    producer_request_doc_id: String,
    producer_tool_name: String,
    producer_tool_call_doc_id: String,
    source_collection: String,
    source_doc_id: String,
    source_commit_cids: Vec<String>,
    consumer_trigger_id: String,
    consumer_request_id: String,
    consumer_request_doc_id: String,
}

async fn verify_tool_call_expectations(
    graphql: &str,
    stages: &[StageResult],
    expectations: &[ToolCallExpectation],
) -> Result<()> {
    for expected in expectations {
        let matching_stages = stages
            .iter()
            .filter(|stage| stage.trigger_id == expected.trigger_id)
            .collect::<Vec<_>>();
        if matching_stages.is_empty() {
            return Err(anyhow::anyhow!(
                "tool call expectation references unknown trigger {}",
                expected.trigger_id
            ));
        }
        let mut all_rows = Vec::new();
        let mut matched = false;
        for stage in matching_stages {
            let escaped = escape_graphql_string(&stage.request_id);
            let query = format!(
                r#"{{
                AgentToolCall(filter: {{ request_id: {{ _eq: "{escaped}" }} }}) {{
                    tool_name
                    status
                    lifecycle_state
                    args
                    result
                }}
            }}"#
            );
            let rows = graphql_rows(graphql, "AgentToolCall", &query).await?;
            matched |= rows.iter().any(|row| tool_call_matches(row, expected));
            all_rows.extend(rows);
        }
        if !matched {
            bail!(
                "no completed {} call for {} matched action={:?} file={:?} symbol={:?} result_contains={:?}; rows={all_rows:?}",
                expected.tool_name,
                expected.trigger_id,
                expected.action,
                expected.file,
                expected.symbol,
                expected.result_contains
            );
        }
    }
    Ok(())
}

fn tool_call_matches(row: &Value, expected: &ToolCallExpectation) -> bool {
    if row.get("tool_name").and_then(Value::as_str) != Some(expected.tool_name.as_str()) {
        return false;
    }
    let completed = row.get("lifecycle_state").and_then(Value::as_str) == Some("completed");
    if !completed {
        return false;
    }
    let parsed = row
        .get("args")
        .and_then(Value::as_str)
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok());
    for (field, expected_value) in [
        ("action", expected.action.as_deref()),
        ("file", expected.file.as_deref()),
        ("symbol", expected.symbol.as_deref()),
    ] {
        let Some(expected_value) = expected_value else {
            continue;
        };
        let Some(actual) = parsed
            .as_ref()
            .and_then(|value| value.get(field))
            .and_then(Value::as_str)
        else {
            return false;
        };
        let matches = if field == "file" {
            gents::toolset::result_path_matches(expected_value, actual)
        } else {
            actual == expected_value
        };
        if !matches {
            return false;
        }
    }
    let result = row.get("result").and_then(Value::as_str).unwrap_or("");
    if result_looks_failed(result) {
        return false;
    }
    expected
        .result_contains
        .iter()
        .all(|needle| result.contains(needle))
}

fn result_looks_failed(result: &str) -> bool {
    gents::toolset::result_looks_failed(result)
}

async fn graphql_rows(graphql: &str, field: &str, query: &str) -> Result<Vec<Value>> {
    let response = post_graphql(graphql, query).await?;
    if let Some(errors) = response.get("errors").and_then(Value::as_array) {
        if !errors.is_empty() {
            bail!("GraphQL {field} query failed: {errors:?}");
        }
    }
    response
        .pointer(&format!("/data/{field}"))
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("GraphQL {field} query returned no row array"))
}

async fn load_result_documents(
    graphql: &str,
    expected: &[ResultDocumentExpectation],
    correlation: &str,
) -> Result<BTreeMap<String, Vec<Value>>> {
    let mut documents = BTreeMap::new();
    let correlation = escape_graphql_string(correlation);
    for result in expected {
        let projection = result.fields.join(" ");
        let query = format!(
            r#"{{ {collection}(filter: {{ {correlation_field}: {{ _eq: "{correlation}" }} }}) {{ _docID {projection} }} }}"#,
            collection = result.collection,
            correlation_field = result.correlation_field,
        );
        let mut rows = graphql_rows(graphql, &result.collection, &query).await?;
        rows.sort_by_key(Value::to_string);
        documents.insert(result.collection.clone(), rows);
    }
    Ok(documents)
}

async fn composite_commits(graphql: &str, doc_id: &str) -> Result<Vec<Value>> {
    let query = format!(
        r#"query {{
            _commits(docID: "{}") {{
                cid
                fieldName
                signature {{ identity type }}
            }}
        }}"#,
        escape_graphql_string(doc_id),
    );
    Ok(graphql_rows(graphql, "_commits", &query)
        .await?
        .into_iter()
        .filter(|commit| commit.get("fieldName").and_then(Value::as_str) == Some("_C"))
        .collect())
}

fn commit_has_signer(commit: &Value, signer_identity: &str) -> bool {
    commit
        .pointer("/signature/identity")
        .and_then(Value::as_str)
        == Some(signer_identity)
}

fn require_signed_commits(
    collection: &str,
    doc_id: &str,
    commits: &[Value],
    signer_identity: &str,
) -> Result<()> {
    if commits.is_empty() {
        bail!("{collection} {doc_id} has no composite commits");
    }
    if let Some(unsigned) = commits
        .iter()
        .find(|commit| !commit_has_signer(commit, signer_identity))
    {
        bail!(
            "{collection} {doc_id} commit {} was not signed by the node identity",
            unsigned
                .get("cid")
                .and_then(Value::as_str)
                .unwrap_or("(unknown)")
        );
    }
    Ok(())
}

async fn verify_request_fact_collection(
    graphql: &str,
    stage: &StageResult,
    request_doc_id: &str,
    signer_identity: &str,
    collection: &str,
    required: bool,
    extra_fields: &str,
) -> Result<Vec<Value>> {
    let query = format!(
        r#"{{ {collection}(filter: {{ request_doc_id: {{ _eq: "{}" }} }}) {{
            _docID
            request_doc_id
            {extra_fields}
        }} }}"#,
        escape_graphql_string(request_doc_id),
    );
    let rows = graphql_rows(graphql, collection, &query).await?;
    if required && rows.is_empty() {
        bail!(
            "completed request {} has no durable {collection} facts",
            stage.request_id
        );
    }

    for row in &rows {
        let doc_id = row
            .get("_docID")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .with_context(|| format!("{collection} provenance query returned no _docID"))?;
        if row.get("request_doc_id").and_then(Value::as_str) != Some(request_doc_id) {
            bail!("{collection} {doc_id} does not point to AgentRequest {request_doc_id}");
        }
        let commits = composite_commits(graphql, doc_id).await?;
        require_signed_commits(collection, doc_id, &commits, signer_identity)?;
    }
    Ok(rows)
}

async fn verify_stage_provenance(
    graphql: &str,
    stage: &StageResult,
    signer_identity: &str,
    require_tool_call: bool,
) -> Result<StageProvenance> {
    let request_query = format!(
        r#"{{ AgentRequest(filter: {{ request_id: {{ _eq: "{}" }} }}, limit: 2) {{ _docID }} }}"#,
        escape_graphql_string(&stage.request_id),
    );
    let request_rows = graphql_rows(graphql, "AgentRequest", &request_query).await?;
    if request_rows.len() != 1 {
        bail!(
            "request {} resolved to {} AgentRequest documents; provenance requires exactly one",
            stage.request_id,
            request_rows.len()
        );
    }
    let request_doc_id = request_rows[0]
        .get("_docID")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .context("AgentRequest provenance query returned no _docID")?
        .to_string();
    let request_commits = composite_commits(graphql, &request_doc_id).await?;
    require_signed_commits(
        "AgentRequest",
        &request_doc_id,
        &request_commits,
        signer_identity,
    )?;

    let rendered_query = format!(
        r#"{{ RenderedRequest(filter: {{ request_doc_id: {{ _eq: "{}" }} }}) {{
            _docID
            request_doc_id
            request_commit_cid
        }} }}"#,
        escape_graphql_string(&request_doc_id),
    );
    let rendered_rows = graphql_rows(graphql, "RenderedRequest", &rendered_query).await?;
    if rendered_rows.is_empty() {
        bail!(
            "request {} completed without a durable RenderedRequest",
            stage.request_id
        );
    }

    let mut request_commit_cids = Vec::with_capacity(rendered_rows.len());
    for row in &rendered_rows {
        let rendered_doc_id = row
            .get("_docID")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .context("RenderedRequest provenance query returned no _docID")?;
        if row.get("request_doc_id").and_then(Value::as_str) != Some(&request_doc_id) {
            bail!(
                "RenderedRequest {rendered_doc_id} does not point to AgentRequest {request_doc_id}"
            );
        }
        let request_commit_cid = row
            .get("request_commit_cid")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .with_context(|| {
                format!("RenderedRequest {rendered_doc_id} has no exact request commit CID")
            })?;
        let Some(request_commit) = request_commits
            .iter()
            .find(|commit| commit.get("cid").and_then(Value::as_str) == Some(request_commit_cid))
        else {
            bail!(
                "RenderedRequest {rendered_doc_id} pins unknown AgentRequest commit {request_commit_cid}"
            );
        };
        if !commit_has_signer(request_commit, signer_identity) {
            bail!("AgentRequest commit {request_commit_cid} was not signed by the node identity");
        }
        let rendered_commits = composite_commits(graphql, rendered_doc_id).await?;
        require_signed_commits(
            "RenderedRequest",
            rendered_doc_id,
            &rendered_commits,
            signer_identity,
        )?;
        request_commit_cids.push(request_commit_cid.to_string());
    }
    request_commit_cids.sort();
    request_commit_cids.dedup();

    let mut request_fact_counts = BTreeMap::new();
    for (collection, required, extra_fields) in [
        ("AgentResponse", true, "status content reasoning"),
        ("AgentMessage", true, "role content reasoning"),
        ("InferenceCall", true, "call_state"),
        (
            "AgentToolCall",
            require_tool_call,
            "status tool_name result",
        ),
        ("CompactionEntry", false, "summary"),
    ] {
        let rows = verify_request_fact_collection(
            graphql,
            stage,
            &request_doc_id,
            signer_identity,
            collection,
            required,
            extra_fields,
        )
        .await?;
        match collection {
            "AgentResponse"
                if !rows.iter().any(|row| {
                    matches!(
                        row.get("status").and_then(Value::as_str),
                        Some("complete" | "completed")
                    )
                }) =>
            {
                bail!(
                    "completed request {} has no terminal AgentResponse",
                    stage.request_id
                );
            }
            "AgentMessage"
                if !rows.iter().any(|row| {
                    row.get("role").and_then(Value::as_str) == Some("assistant")
                        && ["content", "reasoning"].iter().any(|field| {
                            row.get(*field)
                                .and_then(Value::as_str)
                                .is_some_and(|value| !value.trim().is_empty())
                        })
                }) =>
            {
                bail!(
                    "completed request {} has no materialized assistant AgentMessage",
                    stage.request_id
                );
            }
            _ => {}
        }
        request_fact_counts.insert(collection.to_string(), rows.len());
    }

    Ok(StageProvenance {
        request_id: stage.request_id.clone(),
        request_doc_id,
        rendered_request_count: rendered_rows.len(),
        request_commit_cids,
        request_fact_counts,
        signer_identity: signer_identity.to_string(),
    })
}

fn created_doc_reference<'a>(result: &'a str, collection: &str) -> Option<&'a str> {
    let mut parts = result.split_whitespace();
    if parts.next() != Some("created") || parts.next() != Some(collection) {
        return None;
    }
    let doc_id = parts.next().filter(|value| !value.is_empty())?;
    parts.next().is_none().then_some(doc_id)
}

fn stage_for_trigger<'a>(stages: &'a [StageResult], trigger_id: &str) -> Result<&'a StageResult> {
    let mut matching = stages.iter().filter(|stage| stage.trigger_id == trigger_id);
    let stage = matching
        .next()
        .with_context(|| format!("source edge trigger {trigger_id} produced no stage"))?;
    if matching.next().is_some() {
        bail!("source edge trigger {trigger_id} produced multiple stages");
    }
    Ok(stage)
}

fn provenance_for_stage<'a>(
    provenance: &'a [StageProvenance],
    stage: &StageResult,
) -> Result<&'a StageProvenance> {
    provenance
        .iter()
        .find(|evidence| evidence.request_id == stage.request_id)
        .with_context(|| {
            format!(
                "source edge stage {} has no signed request provenance",
                stage.request_id
            )
        })
}

async fn verify_source_edges(
    graphql: &str,
    expected_edges: &[SourceEdgeExpectation],
    stages: &[StageResult],
    provenance: &[StageProvenance],
    signer_identity: &str,
) -> Result<Vec<SourceEdgeEvidence>> {
    let mut evidence = Vec::with_capacity(expected_edges.len());
    for expected in expected_edges {
        validate_collection_identifier(&expected.source_collection).with_context(|| {
            format!(
                "source edge {} -> {} has invalid source collection",
                expected.producer_trigger_id, expected.consumer_trigger_id
            )
        })?;
        let producer = stage_for_trigger(stages, &expected.producer_trigger_id)?;
        let consumer = stage_for_trigger(stages, &expected.consumer_trigger_id)?;
        let producer_provenance = provenance_for_stage(provenance, producer)?;
        let consumer_provenance = provenance_for_stage(provenance, consumer)?;
        let source_doc_id = consumer
            .caused_by_source_doc_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .with_context(|| {
                format!(
                    "consumer request {} records no caused_by_source_doc_id",
                    consumer.request_id
                )
            })?;

        let tool_query = format!(
            r#"{{ AgentToolCall(filter: {{
                request_doc_id: {{ _eq: "{}" }},
                tool_name: {{ _eq: "{}" }}
            }}) {{
                _docID
                request_doc_id
                tool_name
                result
            }} }}"#,
            escape_graphql_string(&producer_provenance.request_doc_id),
            escape_graphql_string(&expected.producer_tool_name),
        );
        let tool_rows = graphql_rows(graphql, "AgentToolCall", &tool_query).await?;
        let matching_tool_rows = tool_rows
            .iter()
            .filter(|row| {
                row.get("request_doc_id").and_then(Value::as_str)
                    == Some(producer_provenance.request_doc_id.as_str())
                    && row.get("tool_name").and_then(Value::as_str)
                        == Some(expected.producer_tool_name.as_str())
                    && row
                        .get("result")
                        .and_then(Value::as_str)
                        .and_then(|result| {
                            created_doc_reference(result, &expected.source_collection)
                        })
                        == Some(source_doc_id)
            })
            .collect::<Vec<_>>();
        if matching_tool_rows.len() != 1 {
            bail!(
                "producer request {} has {} {} results referencing {} {}",
                producer.request_id,
                matching_tool_rows.len(),
                expected.producer_tool_name,
                expected.source_collection,
                source_doc_id
            );
        }
        let tool_row = matching_tool_rows[0];
        let tool_call_doc_id = tool_row
            .get("_docID")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .context("source-edge AgentToolCall returned no _docID")?;
        if tool_row.get("request_doc_id").and_then(Value::as_str)
            != Some(producer_provenance.request_doc_id.as_str())
        {
            bail!(
                "AgentToolCall {tool_call_doc_id} does not point to producer AgentRequest {}",
                producer_provenance.request_doc_id
            );
        }
        let tool_commits = composite_commits(graphql, tool_call_doc_id).await?;
        require_signed_commits(
            "AgentToolCall",
            tool_call_doc_id,
            &tool_commits,
            signer_identity,
        )?;

        let source_query = format!(
            r#"{{ {}(filter: {{ _docID: {{ _eq: "{}" }} }}, limit: 2) {{ _docID }} }}"#,
            expected.source_collection,
            escape_graphql_string(source_doc_id),
        );
        let source_rows = graphql_rows(graphql, &expected.source_collection, &source_query).await?;
        if source_rows.len() != 1
            || source_rows[0].get("_docID").and_then(Value::as_str) != Some(source_doc_id)
        {
            bail!(
                "consumer request {} points to {} {}, which resolved to {} documents",
                consumer.request_id,
                expected.source_collection,
                source_doc_id,
                source_rows.len()
            );
        }
        let source_commits = composite_commits(graphql, source_doc_id).await?;
        require_signed_commits(
            &expected.source_collection,
            source_doc_id,
            &source_commits,
            signer_identity,
        )?;
        let mut source_commit_cids = source_commits
            .iter()
            .filter_map(|commit| commit.get("cid").and_then(Value::as_str))
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        source_commit_cids.sort();
        source_commit_cids.dedup();

        evidence.push(SourceEdgeEvidence {
            producer_trigger_id: producer.trigger_id.clone(),
            producer_request_id: producer.request_id.clone(),
            producer_request_doc_id: producer_provenance.request_doc_id.clone(),
            producer_tool_name: expected.producer_tool_name.clone(),
            producer_tool_call_doc_id: tool_call_doc_id.to_string(),
            source_collection: expected.source_collection.clone(),
            source_doc_id: source_doc_id.to_string(),
            source_commit_cids,
            consumer_trigger_id: consumer.trigger_id.clone(),
            consumer_request_id: consumer.request_id.clone(),
            consumer_request_doc_id: consumer_provenance.request_doc_id.clone(),
        });
    }
    Ok(evidence)
}

async fn render_projection_artifacts(
    bin: &Path,
    graphql: &str,
    run_dir: &Path,
    stages: &[StageResult],
    projections: &[String],
) -> Result<BTreeMap<String, BTreeMap<String, String>>> {
    let artifact_dir = run_dir.join("projections");
    std::fs::create_dir_all(&artifact_dir)
        .with_context(|| format!("creating projection directory {}", artifact_dir.display()))?;
    let mut artifacts = BTreeMap::new();
    for stage in stages {
        let timeline_args = vec![
            "trace".to_string(),
            "timeline".to_string(),
            "--graphql".to_string(),
            graphql.to_string(),
            "--request-id".to_string(),
            stage.request_id.clone(),
        ];
        let timeline = run_cli_json(bin, &timeline_args)
            .await
            .with_context(|| format!("projecting timeline for request {}", stage.request_id))?;
        let timeline_path = artifact_dir.join(format!("{}-timeline.json", stage.request_id));
        std::fs::write(&timeline_path, serde_json::to_vec_pretty(&timeline)?)
            .with_context(|| format!("writing {}", timeline_path.display()))?;

        let mut request_artifacts =
            BTreeMap::from([("timeline".to_string(), path_arg(&timeline_path))]);
        for projection in projections {
            let project_args = vec![
                "trace".to_string(),
                "project".to_string(),
                "--graphql".to_string(),
                graphql.to_string(),
                "--request-id".to_string(),
                stage.request_id.clone(),
                "--projection".to_string(),
                projection.clone(),
            ];
            let projected = run_cli_json(bin, &project_args).await.with_context(|| {
                format!(
                    "rendering {projection} projection for request {}",
                    stage.request_id
                )
            })?;
            let projection_path = artifact_dir.join(format!(
                "{}-{}.json",
                stage.request_id,
                projection.replace('_', "-")
            ));
            std::fs::write(&projection_path, serde_json::to_vec_pretty(&projected)?)
                .with_context(|| format!("writing {}", projection_path.display()))?;
            request_artifacts.insert(projection.clone(), path_arg(&projection_path));
        }
        artifacts.insert(stage.request_id.clone(), request_artifacts);
    }
    Ok(artifacts)
}

async fn sourced_trigger_request_count(
    graphql: &str,
    source: &TriggerRequestCountSource,
    correlation: &str,
) -> Result<Option<usize>> {
    let correlation = escape_graphql_string(correlation);
    let query = format!(
        r#"{{ {collection}(filter: {{ {correlation_field}: {{ _eq: "{correlation}" }} }}) {{ {expected_count_field} }} }}"#,
        collection = source.collection,
        correlation_field = source.correlation_field,
        expected_count_field = source.expected_count_field,
    );
    let rows = graphql_rows(graphql, &source.collection, &query).await?;
    let mut expected = None;
    for row in rows {
        let value = row.get(&source.expected_count_field).with_context(|| {
            format!(
                "{} row omitted expected count field {}",
                source.collection, source.expected_count_field
            )
        })?;
        let count =
            gents::graphql::canonical_positive_count(value, gents::MAX_EVENT_TRIGGER_GROUP_DOCS)
                .with_context(|| {
                    format!(
                        "{}.{} must be a canonical positive integer <= {}",
                        source.collection,
                        source.expected_count_field,
                        gents::MAX_EVENT_TRIGGER_GROUP_DOCS
                    )
                })?;
        if expected.is_some_and(|prior| prior != count) {
            bail!(
                "{}.{} is inconsistent for correlation {correlation}",
                source.collection,
                source.expected_count_field
            );
        }
        expected = Some(count);
    }
    Ok(expected)
}

async fn await_stages(
    graphql: &str,
    trigger_ids: &[String],
    trigger_request_counts: &BTreeMap<String, usize>,
    trigger_request_count_sources: &BTreeMap<String, TriggerRequestCountSource>,
    correlation: &str,
    deadline: Duration,
) -> Result<Vec<StageResult>> {
    let correlated_trigger_ids = correlated_trigger_ids(graphql, trigger_ids).await?;
    let started = Instant::now();
    loop {
        let mut done: Vec<StageResult> = Vec::new();
        let mut resolved_counts = BTreeMap::new();
        for trigger_id in trigger_ids {
            let expected_count = if let Some(source) = trigger_request_count_sources.get(trigger_id)
            {
                sourced_trigger_request_count(graphql, source, correlation).await?
            } else {
                Some(trigger_request_counts.get(trigger_id).copied().unwrap_or(1))
            };
            resolved_counts.insert(trigger_id, expected_count);
            let query = stage_requests_query(
                trigger_id,
                correlated_trigger_ids
                    .contains(trigger_id)
                    .then_some(correlation),
            );
            let Ok(resp) = post_graphql(graphql, &query).await else {
                continue;
            };
            let Some(rows) = resp.pointer("/data/AgentRequest").and_then(Value::as_array) else {
                continue;
            };
            if expected_count.is_some_and(|expected_count| rows.len() > expected_count) {
                bail!(
                    "trigger {trigger_id} materialized {} requests for correlation {correlation}; expected {}",
                    rows.len(),
                    expected_count.expect("guard establishes expected count")
                );
            }
            for row in rows {
                let state = row
                    .get("lifecycle_state")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if matches!(state, "completed" | "failed" | "cancelled") {
                    let request_id = row
                        .get("request_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if state != "completed" {
                        let reason = row
                            .get("failure_reason")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        bail!("trigger {trigger_id} request {request_id} ended {state}: {reason}");
                    }
                    done.push(StageResult {
                        trigger_id: trigger_id.clone(),
                        request_id: request_id.to_string(),
                        lifecycle_state: state.to_string(),
                        caused_by_source_doc_id: row
                            .get("caused_by_source_doc_id")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned),
                    });
                }
            }
        }
        let all_counts_resolved = resolved_counts.values().all(Option::is_some);
        let expected_total = resolved_counts
            .values()
            .filter_map(|count| *count)
            .sum::<usize>();
        if all_counts_resolved && done.len() == expected_total {
            return Ok(done);
        }
        // A trigger that fired and failed to materialize will never retry:
        // created/first-seen means the source document is already marked seen.
        // Surface its own last_error instead of waiting out the deadline.
        for trigger_id in trigger_ids {
            let Some(expected_count) = resolved_counts.get(trigger_id).copied().flatten() else {
                continue;
            };
            if done
                .iter()
                .filter(|stage| &stage.trigger_id == trigger_id)
                .count()
                == expected_count
            {
                continue;
            }
            if let Some(error) = trigger_error(graphql, trigger_id).await {
                bail!("trigger {trigger_id} fired but did not materialize: {error}");
            }
        }
        if started.elapsed() >= deadline {
            let seen: Vec<&str> = done.iter().map(|s| s.trigger_id.as_str()).collect();
            bail!(
                "timed out after {}s: reached a terminal state for [{}], expected [{}]",
                deadline.as_secs(),
                seen.join(", "),
                trigger_ids.join(", ")
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Load the applied trigger policy rather than assuming every pack stage uses
/// correlation. The original pipeline demo predates correlated fan-in, so its
/// request lineage deliberately leaves `caused_by_correlation` null.
async fn correlated_trigger_ids(graphql: &str, trigger_ids: &[String]) -> Result<BTreeSet<String>> {
    let mut correlated = BTreeSet::new();
    for trigger_id in trigger_ids {
        let query = format!(
            r#"{{ EventTrigger(filter: {{ trigger_id: {{ _eq: "{}" }} }}, limit: 2) {{
                trigger_id
                correlation_field
            }} }}"#,
            escape_graphql_string(trigger_id),
        );
        let rows = graphql_rows(graphql, "EventTrigger", &query).await?;
        if rows.len() != 1 {
            bail!(
                "trigger {trigger_id} resolved to {} EventTrigger documents; expected exactly one",
                rows.len()
            );
        }
        if rows[0]
            .get("correlation_field")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty())
        {
            correlated.insert(trigger_id.clone());
        }
    }
    Ok(correlated)
}

fn stage_requests_query(trigger_id: &str, correlation: Option<&str>) -> String {
    let correlation_filter = correlation.map_or_else(String::new, |correlation| {
        format!(
            r#",
                    caused_by_correlation: {{ _eq: "{}" }}"#,
            escape_graphql_string(correlation)
        )
    });
    format!(
        r#"{{ AgentRequest(filter: {{
                    caused_by_trigger_id: {{ _eq: "{}" }}{}
                }}) {{ request_id lifecycle_state failure_reason caused_by_source_doc_id }} }}"#,
        escape_graphql_string(trigger_id),
        correlation_filter,
    )
}

/// The trigger's own `last_error`, when it recorded a failed fire.
async fn trigger_error(graphql: &str, trigger_id: &str) -> Option<String> {
    let query = format!(
        r#"{{ EventTrigger(filter: {{ trigger_id: {{ _eq: "{}" }} }}) {{ last_status last_error }} }}"#,
        escape_graphql_string(trigger_id)
    );
    let resp = post_graphql(graphql, &query).await.ok()?;
    let row = resp.pointer("/data/EventTrigger/0")?;
    if row.get("last_status").and_then(Value::as_str) != Some("error") {
        return None;
    }
    Some(
        row.get("last_error")
            .and_then(Value::as_str)
            .unwrap_or("(no last_error recorded)")
            .to_string(),
    )
}

async fn verify_fan_in(
    graphql: &str,
    expected: &FanInExpectation,
    correlation: &str,
    agent_did: &str,
) -> Result<Option<FanInEvidence>> {
    for collection in [
        &expected.member_collection,
        &expected.result_collection,
        &expected.report_collection,
    ] {
        validate_collection_identifier(collection)?;
    }
    for field in [&expected.correlation_field, &expected.expected_count_field] {
        gents::graphql::validate_graphql_name(field)?;
    }
    for field in &expected.member_required_fields {
        gents::graphql::validate_graphql_name(field)?;
    }
    if let Some(verification) = &expected.verification {
        for collection in [
            &verification.candidate_collection,
            &verification.decision_collection,
            &verification.summary_collection,
            &verification.confirmed_collection,
        ] {
            validate_collection_identifier(collection)?;
        }
        for field in [
            &verification.finding_id_field,
            &verification.verdict_field,
            &verification.evidence_field,
            &verification.confirmed_count_field,
            &verification.refuted_count_field,
        ] {
            gents::graphql::validate_graphql_name(field)?;
        }
    }

    let escaped_correlation = escape_graphql_string(correlation);
    let required_member_projection = expected.member_required_fields.join(" ");
    let load_members = |collection: &str, required_projection: &str| {
        format!(
            r#"{{ {collection}(filter: {{ {correlation_field}: {{ _eq: "{escaped_correlation}" }} }}) {{ _docID {correlation_field} {expected_count_field} {required_projection} }} }}"#,
            correlation_field = expected.correlation_field,
            expected_count_field = expected.expected_count_field,
        )
    };
    let member_rows = graphql_rows(
        graphql,
        &expected.member_collection,
        &load_members(&expected.member_collection, &required_member_projection),
    )
    .await?;
    if member_rows.is_empty() {
        bail!("fan-in produced no {} rows", expected.member_collection);
    }
    let expected_count = member_rows.len();
    if expected
        .min_expected_count
        .is_some_and(|minimum| expected_count < minimum)
    {
        bail!(
            "fan-in chose {expected_count} members, below minimum {}",
            expected
                .min_expected_count
                .expect("guard establishes minimum")
        );
    }
    if expected
        .max_expected_count
        .is_some_and(|maximum| expected_count > maximum)
    {
        bail!(
            "fan-in chose {expected_count} members, above maximum {}",
            expected
                .max_expected_count
                .expect("guard establishes maximum")
        );
    }
    for row in &member_rows {
        let count = row
            .get(&expected.expected_count_field)
            .and_then(|value| {
                value
                    .as_u64()
                    .and_then(|value| usize::try_from(value).ok())
                    .or_else(|| value.as_str()?.parse::<usize>().ok())
            })
            .context("fan-in member has no valid expected count")?;
        if count != expected_count {
            bail!(
                "fan-in {} rows disagree with closed-set count: row says {}, actual {}",
                expected.member_collection,
                count,
                expected_count
            );
        }
        for field in &expected.member_required_fields {
            if !row
                .get(field)
                .and_then(Value::as_str)
                .is_some_and(|value| !value.trim().is_empty())
            {
                bail!("fan-in member has no non-empty required field {field}");
            }
        }
    }

    let result_rows = graphql_rows(
        graphql,
        &expected.result_collection,
        &load_members(&expected.result_collection, ""),
    )
    .await?;
    if result_rows.len() != expected_count {
        bail!(
            "fan-in expected {} correlated {} rows, found {}",
            expected_count,
            expected.result_collection,
            result_rows.len()
        );
    }
    for row in &result_rows {
        let count = row
            .get(&expected.expected_count_field)
            .and_then(|value| {
                value
                    .as_u64()
                    .and_then(|value| usize::try_from(value).ok())
                    .or_else(|| value.as_str()?.parse::<usize>().ok())
            })
            .context("fan-in result has no valid expected count")?;
        if count != expected_count {
            bail!("fan-in result cardinality snapshot drifted");
        }
    }

    let request_query = format!(
        r#"{{ AgentRequest(filter: {{
            agent_did: {{ _eq: "{}" }},
            caused_by_trigger_id: {{ _eq: "{}" }},
            caused_by_trigger_kind: {{ _eq: "event" }},
            caused_by_correlation: {{ _eq: "{}" }}
        }}) {{ request_id }} }}"#,
        escape_graphql_string(agent_did),
        escape_graphql_string(&expected.consumer_trigger_id),
        escaped_correlation,
    );
    let request_rows = graphql_rows(graphql, "AgentRequest", &request_query).await?;
    if request_rows.len() != 1 {
        bail!(
            "fan-in consumer {} expected exactly one correlated AgentRequest, found {}",
            expected.consumer_trigger_id,
            request_rows.len()
        );
    }
    let consumer_request_id = request_rows[0]
        .get("request_id")
        .and_then(Value::as_str)
        .context("fan-in consumer request has no request_id")?
        .to_string();

    let report_projection = expected.verification.as_ref().map_or_else(
        || "_docID".to_string(),
        |verification| {
            format!(
                "_docID {} {}",
                verification.confirmed_count_field, verification.refuted_count_field
            )
        },
    );
    let report_query = format!(
        r#"{{ {collection}(filter: {{ {field}: {{ _eq: "{escaped_correlation}" }} }}) {{ {report_projection} }} }}"#,
        collection = expected.report_collection,
        field = expected.correlation_field,
    );
    let report_rows = graphql_rows(graphql, &expected.report_collection, &report_query).await?;
    if report_rows.len() != 1 {
        bail!(
            "fan-in expected exactly one correlated {}, found {}",
            expected.report_collection,
            report_rows.len()
        );
    }

    let mut candidate_count = None;
    let mut confirmed_count = None;
    let mut refuted_count = None;
    let mut decision_count = None;
    let mut verification_summary_count = None;
    let mut final_consumer_request_id = None;
    if let Some(verification) = &expected.verification {
        let candidate_query = format!(
            r#"{{ {collection}(filter: {{ {field}: {{ _eq: "{escaped_correlation}" }} }}) {{ _docID {finding_id} }} }}"#,
            collection = verification.candidate_collection,
            field = expected.correlation_field,
            finding_id = verification.finding_id_field,
        );
        let candidates = graphql_rows(
            graphql,
            &verification.candidate_collection,
            &candidate_query,
        )
        .await?;
        let decision_query = format!(
            r#"{{ {collection}(filter: {{ {field}: {{ _eq: "{escaped_correlation}" }} }}) {{ _docID {finding_id} {verdict} {evidence} }} }}"#,
            collection = verification.decision_collection,
            field = expected.correlation_field,
            finding_id = verification.finding_id_field,
            verdict = verification.verdict_field,
            evidence = verification.evidence_field,
        );
        let decisions =
            graphql_rows(graphql, &verification.decision_collection, &decision_query).await?;
        let candidate_ids = candidates
            .iter()
            .map(|row| {
                row.get(&verification.finding_id_field)
                    .and_then(Value::as_str)
                    .context("candidate finding has no finding id")
            })
            .collect::<Result<std::collections::BTreeSet<_>>>()?;
        let decision_ids = decisions
            .iter()
            .map(|row| {
                row.get(&verification.finding_id_field)
                    .and_then(Value::as_str)
                    .context("verification decision has no finding id")
            })
            .collect::<Result<std::collections::BTreeSet<_>>>()?;
        if candidate_ids.len() != candidates.len() {
            bail!("fan-in candidate finding ids are not unique");
        }
        if decision_ids.len() != decisions.len() {
            bail!("fan-in verification decision ids are not unique");
        }
        if candidate_ids != decision_ids {
            bail!("fan-in verification decisions do not cover the exact candidate set");
        }
        let mut verified_confirmed = 0usize;
        let mut verified_refuted = 0usize;
        for row in &decisions {
            match row.get(&verification.verdict_field).and_then(Value::as_str) {
                Some("confirmed") => verified_confirmed += 1,
                Some("refuted") => verified_refuted += 1,
                verdict => bail!("verification decision has invalid verdict {verdict:?}"),
            }
            if !row
                .get(&verification.evidence_field)
                .and_then(Value::as_str)
                .is_some_and(|evidence| !evidence.trim().is_empty())
            {
                bail!("verification decision has no fresh evidence");
            }
        }
        let summary_query = format!(
            r#"{{ {collection}(filter: {{ {field}: {{ _eq: "{escaped_correlation}" }} }}) {{ _docID {confirmed_count} {refuted_count} }} }}"#,
            collection = verification.summary_collection,
            field = expected.correlation_field,
            confirmed_count = verification.confirmed_count_field,
            refuted_count = verification.refuted_count_field,
        );
        let summaries =
            graphql_rows(graphql, &verification.summary_collection, &summary_query).await?;
        if summaries.len() != 1 {
            bail!(
                "fan-in expected exactly one correlated {}, found {}",
                verification.summary_collection,
                summaries.len()
            );
        }
        let parse_count = |row: &Value, field: &str| -> Result<usize> {
            row.get(field)
                .and_then(|value| {
                    value
                        .as_u64()
                        .and_then(|value| usize::try_from(value).ok())
                        .or_else(|| value.as_str()?.parse::<usize>().ok())
                })
                .with_context(|| format!("row has no valid {field}"))
        };
        if parse_count(&summaries[0], &verification.confirmed_count_field)? != verified_confirmed
            || parse_count(&summaries[0], &verification.refuted_count_field)? != verified_refuted
        {
            bail!("verification summary does not match the durable decision ledger");
        }

        let final_request_query = format!(
            r#"{{ AgentRequest(filter: {{
                agent_did: {{ _eq: "{}" }},
                caused_by_trigger_id: {{ _eq: "{}" }},
                caused_by_trigger_kind: {{ _eq: "event" }},
                caused_by_correlation: {{ _eq: "{}" }}
            }}) {{ request_id }} }}"#,
            escape_graphql_string(agent_did),
            escape_graphql_string(&verification.final_consumer_trigger_id),
            escaped_correlation,
        );
        let final_requests = graphql_rows(graphql, "AgentRequest", &final_request_query).await?;
        if final_requests.len() != 1 {
            bail!(
                "final consumer {} expected exactly one correlated AgentRequest, found {}",
                verification.final_consumer_trigger_id,
                final_requests.len()
            );
        }
        final_consumer_request_id = Some(
            final_requests[0]
                .get("request_id")
                .and_then(Value::as_str)
                .context("final consumer request has no request_id")?
                .to_string(),
        );
        let confirmed_query = format!(
            r#"{{ {collection}(filter: {{ {field}: {{ _eq: "{escaped_correlation}" }} }}) {{ _docID {verdict} {evidence} }} }}"#,
            collection = verification.confirmed_collection,
            field = expected.correlation_field,
            verdict = verification.verdict_field,
            evidence = verification.evidence_field,
        );
        let confirmed = graphql_rows(
            graphql,
            &verification.confirmed_collection,
            &confirmed_query,
        )
        .await?;
        for row in &confirmed {
            if row.get(&verification.verdict_field).and_then(Value::as_str) != Some("confirmed") {
                bail!(
                    "fan-in promoted {} row has a verdict other than confirmed",
                    verification.confirmed_collection
                );
            }
            if !row
                .get(&verification.evidence_field)
                .and_then(Value::as_str)
                .is_some_and(|evidence| !evidence.trim().is_empty())
            {
                bail!(
                    "fan-in promoted {} row has no verification evidence",
                    verification.confirmed_collection
                );
            }
        }
        let parse_report_count = |field: &str| -> Result<usize> {
            report_rows[0]
                .get(field)
                .and_then(|value| {
                    value
                        .as_u64()
                        .and_then(|value| usize::try_from(value).ok())
                        .or_else(|| value.as_str()?.parse::<usize>().ok())
                })
                .with_context(|| format!("fan-in report has no valid {field}"))
        };
        let reported_confirmed = parse_report_count(&verification.confirmed_count_field)?;
        let reported_refuted = parse_report_count(&verification.refuted_count_field)?;
        if reported_confirmed != confirmed.len() {
            bail!(
                "fan-in report says {} confirmed findings, found {}",
                reported_confirmed,
                confirmed.len()
            );
        }
        if reported_confirmed != verified_confirmed || reported_refuted != verified_refuted {
            bail!("final report counts do not match the verification decision ledger");
        }
        if reported_confirmed + reported_refuted != candidates.len() {
            bail!(
                "fan-in verification ledger is unbalanced: {} confirmed + {} refuted != {} candidates",
                reported_confirmed,
                reported_refuted,
                candidates.len()
            );
        }
        candidate_count = Some(candidates.len());
        confirmed_count = Some(reported_confirmed);
        refuted_count = Some(reported_refuted);
        decision_count = Some(decisions.len());
        verification_summary_count = Some(summaries.len());
    }

    Ok(Some(FanInEvidence {
        correlation: correlation.to_string(),
        expected_count,
        member_count: member_rows.len(),
        result_count: result_rows.len(),
        consumer_request_id,
        report_count: report_rows.len(),
        candidate_count,
        confirmed_count,
        refuted_count,
        decision_count,
        verification_summary_count,
        final_consumer_request_id,
    }))
}

async fn count_rows(graphql: &str, collection: &str) -> u64 {
    let query = format!("{{ {collection} {{ _docID }} }}");
    post_graphql(graphql, &query)
        .await
        .ok()
        .and_then(|resp| {
            resp.pointer(&format!("/data/{collection}"))
                .and_then(Value::as_array)
                .map(|rows| rows.len() as u64)
        })
        .unwrap_or(0)
}

async fn token_totals(graphql: &str) -> (u64, u64) {
    let query = "{ InferenceCall { prompt_tokens completion_tokens } }";
    let Ok(resp) = post_graphql(graphql, query).await else {
        return (0, 0);
    };
    let Some(rows) = resp
        .pointer("/data/InferenceCall")
        .and_then(Value::as_array)
    else {
        return (0, 0);
    };
    rows.iter().fold((0, 0), |(p, c), row| {
        (
            p + row
                .get("prompt_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            c + row
                .get("completion_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0),
        )
    })
}

fn usize_field(value: &Value, pointer: &str) -> Result<usize> {
    value
        .pointer(pointer)
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .with_context(|| format!("status output has no integer {pointer}"))
}

async fn load_background_completion_evidence(
    bin: &Path,
    home: &Path,
    graphql: &str,
    agent_did: &str,
) -> Result<BackgroundCompletionEvidence> {
    let status = run_cli_json(
        bin,
        &[
            "status".to_string(),
            "--home".to_string(),
            path_arg(home),
            "--graphql".to_string(),
            graphql.to_string(),
            "--agent-did".to_string(),
            agent_did.to_string(),
        ],
    )
    .await
    .context("loading background-completion status")?;
    let diagnostics = status
        .get("background_completion")
        .cloned()
        .context("status output has no background_completion diagnostics")?;
    if diagnostics.get("state").and_then(Value::as_str) == Some("unavailable") {
        bail!(
            "background-completion diagnostics unavailable: {}",
            diagnostics
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("unknown error")
        );
    }

    let query = format!(
        r#"{{
            AgentRequest(filter: {{ agent_did: {{ _eq: "{}" }} }}) {{
                request_id lifecycle_state execution_origin subagent_depth metadata
            }}
        }}"#,
        escape_graphql_string(agent_did),
    );
    let rows = graphql_rows(graphql, "AgentRequest", &query).await?;
    let mut completed_subagent_request_ids = Vec::new();
    let mut failed_subagent_request_ids = Vec::new();
    let mut completed_wake_request_ids = Vec::new();
    for row in rows {
        let request_id = row
            .get("request_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let lifecycle_state = row
            .get("lifecycle_state")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let subagent_depth = row
            .get("subagent_depth")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        if subagent_depth > 0 {
            match lifecycle_state {
                "completed" => completed_subagent_request_ids.push(request_id.to_string()),
                "failed" | "dead" | "interrupted" => {
                    failed_subagent_request_ids.push(request_id.to_string())
                }
                _ => {}
            }
        }
        if row.get("execution_origin").and_then(Value::as_str) == Some("scheduled")
            && gents::lifecycle::is_background_completion_request(
                row.get("metadata").and_then(Value::as_str),
            )
            && lifecycle_state == "completed"
        {
            completed_wake_request_ids.push(request_id.to_string());
        }
    }
    completed_subagent_request_ids.sort();
    failed_subagent_request_ids.sort();
    completed_wake_request_ids.sort();

    Ok(BackgroundCompletionEvidence {
        completed_subagent_request_ids,
        failed_subagent_request_ids,
        completed_wake_request_ids,
        pending_notifications: usize_field(&diagnostics, "/pending_notifications")?,
        acknowledged_notifications: usize_field(&diagnostics, "/acknowledged_notifications")?,
        stranded_notifications: usize_field(&diagnostics, "/stranded_notifications")?,
        diagnostics,
    })
}

async fn await_background_completion(
    bin: &Path,
    home: &Path,
    graphql: &str,
    agent_did: &str,
    expected: &BackgroundCompletionExpectation,
    deadline: Duration,
) -> Result<BackgroundCompletionEvidence> {
    let started = Instant::now();
    let mut last = None;
    loop {
        match load_background_completion_evidence(bin, home, graphql, agent_did).await {
            Ok(evidence) => {
                if evidence.stranded_notifications > expected.max_stranded_notifications {
                    bail!(
                        "background completion stranded {} notification(s), expected at most {}: {:?}",
                        evidence.stranded_notifications,
                        expected.max_stranded_notifications,
                        evidence.diagnostics
                    );
                }
                if evidence.failed_subagent_request_ids.len()
                    + evidence.completed_subagent_request_ids.len()
                    >= expected.min_completed_subagent_requests
                    && evidence.completed_subagent_request_ids.len()
                        < expected.min_completed_subagent_requests
                {
                    bail!(
                        "background subagents terminalized unsuccessfully: {:?}",
                        evidence.failed_subagent_request_ids
                    );
                }
                if evidence.satisfies(expected) {
                    return Ok(evidence);
                }
                last = Some(evidence);
            }
            Err(error) => tracing::debug!(%error, "background-completion demo evidence not ready"),
        }
        if started.elapsed() >= deadline {
            bail!(
                "timed out after {}s waiting for background completion; last evidence: {last:?}",
                deadline.as_secs()
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Timestamped so `runs/` sorts chronologically and two runs never collide.
fn default_job_id() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    format!("exp-{secs}")
}

fn resolve_manifest_tool_root(pack: &Path, manifest: &PackManifest) -> Result<Option<PathBuf>> {
    if tool_package_needs_root(&manifest.init.tool_package) {
        Ok(Some(resolve_pack_tool_root(
            pack,
            manifest.init.tool_root.as_deref(),
            &manifest.init.tool_root_markers,
        )?))
    } else {
        Ok(None)
    }
}

pub(crate) async fn init_pack(args: DemoInitArgs) -> Result<()> {
    let bin = std::env::current_exe().context("resolving the gents binary path")?;
    let pack = resolve_pack(&args.pack)?;
    let manifest = load_manifest(&pack)?;
    let home = args.home;
    if home.join("init.json").is_file() && !args.overwrite {
        bail!(
            "home {} already initialized; pass --overwrite to replace it",
            home.display()
        );
    }
    std::fs::create_dir_all(&home)
        .with_context(|| format!("creating pack home {}", home.display()))?;
    let tool_root = resolve_manifest_tool_root(&pack, &manifest)?;
    let init = run_cli_json(
        &bin,
        &pack_init_cli_args(&home, &manifest, tool_root.as_deref()),
    )
    .await?;
    let agent_did = init
        .get("agent_did")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    println!(
        "initialized pack {} at {} ({agent_did})",
        manifest.name,
        home.display()
    );
    Ok(())
}

pub(crate) async fn seed(args: DemoSeedArgs) -> Result<()> {
    let pack = resolve_pack(&args.pack)?;
    let manifest = load_manifest(&pack)?;
    if let Some(home) = args.home.as_deref() {
        let env_root = std::env::var_os("GENTS_REVIEW_ROOT").map(PathBuf::from);
        refuse_stale_review_root(home, env_root.as_deref())?;
    }

    let port = args.http_port;
    let graphql = format!("http://127.0.0.1:{port}/api/v0/graphql");
    let healthz = format!("http://127.0.0.1:{port}/healthz");
    wait_http_ok(&healthz, Duration::from_secs(120))
        .await
        .with_context(|| format!("start the pack node first (waiting on {healthz})"))?;
    wait_until(
        &format!("EventTrigger on {}", manifest.seed.collection),
        Duration::from_secs(60),
        || {
            let graphql = graphql.clone();
            let collection = manifest.seed.collection.clone();
            async move {
                let response = post_graphql(
                    &graphql,
                    "{ EventTrigger { trigger_id source_collection enabled } }",
                )
                .await?;
                Ok(event_trigger_ready(&response, &collection))
            }
        },
    )
    .await
    .context("start the pack node first")?;

    let job_id = args.job_id.clone().unwrap_or_else(default_job_id);
    let prompt = args
        .prompt
        .clone()
        .unwrap_or_else(|| manifest.default_prompt.clone());
    if prompt.trim().is_empty() {
        bail!("no prompt: pass --prompt or give the pack a default_prompt");
    }

    let mutation = seed_mutation(&manifest.seed, &job_id, &prompt)?;
    post_graphql(&graphql, &mutation)
        .await
        .context("seeding the pack")?;

    wait_until(
        &format!("request for {job_id}"),
        Duration::from_secs(60),
        || {
            let graphql = graphql.clone();
            let job_id = job_id.clone();
            async move {
                let query = format!(
                    "{{ AgentRequest(filter: {{ caused_by_correlation: {{ _eq: \"{}\" }} }}) {{ request_id }} }}",
                    escape_graphql_string(&job_id)
                );
                let response = post_graphql(&graphql, &query).await?;
                Ok(has_correlated_request(&response))
            }
        },
    )
    .await
    .with_context(|| {
        format!(
            "the {} was written but no request fired; the event source was not observing yet",
            manifest.seed.collection
        )
    })?;

    println!("seeded {} run_id={job_id}", manifest.seed.collection);
    if let Some(page_port) = args.page_port {
        println!("page     http://127.0.0.1:{page_port}/?run={job_id}");
    }
    Ok(())
}

pub(crate) async fn run(args: DemoRunArgs) -> Result<()> {
    let bin = std::env::current_exe().context("resolving the gents binary path")?;
    let pack = resolve_pack(&args.pack)?;
    let mut manifest = load_manifest(&pack)?;
    let observed_collections = trigger_source_collections(&pack, &manifest.expect.trigger_ids)?;
    let job_id = args.job_id.clone().unwrap_or_else(default_job_id);
    let prompt = args
        .prompt
        .clone()
        .unwrap_or_else(|| manifest.default_prompt.clone());
    if prompt.trim().is_empty() {
        bail!("no prompt: pass --prompt or give the pack a default_prompt");
    }

    // Everything a run produces lands under <pack>/runs/<job_id>/ — home, log,
    // and artifacts together, so a failed run is debuggable from one place.
    let run_dir = pack.join("runs").join(&job_id);
    std::fs::create_dir_all(&run_dir)
        .with_context(|| format!("creating run directory {}", run_dir.display()))?;

    // A fresh home per run by default. Triggers are first-seen, so a reused
    // home can silently skip a stage whose source rows already existed.
    let owned_home = args.home.is_none();
    let home = args.home.clone().unwrap_or_else(|| run_dir.join("home"));
    if owned_home && home.exists() {
        std::fs::remove_dir_all(&home).ok();
    }
    std::fs::create_dir_all(&home)
        .with_context(|| format!("creating pack home {}", home.display()))?;

    println!("pack     {} ({})", manifest.name, pack.display());
    println!("job_id   {job_id}");
    println!("run dir  {}", run_dir.display());
    println!("endpoint {}", manifest.init.inference_url);
    println!("model    {}", manifest.init.model_name);

    let tool_root = resolve_manifest_tool_root(&pack, &manifest)?;
    if let Some(root) = tool_root.as_ref() {
        println!(
            "tool     {} @ {}",
            manifest.init.tool_package,
            root.display()
        );
    } else {
        println!("tool     {}", manifest.init.tool_package);
    }

    let init = run_cli_json(
        &bin,
        &pack_init_cli_args(&home, &manifest, tool_root.as_deref()),
    )
    .await?;
    let agent_did = init
        .get("agent_did")
        .and_then(Value::as_str)
        .context("init did not return agent_did")?
        .to_string();

    let port = args.http_port;
    let graphql = format!("http://127.0.0.1:{port}/api/v0/graphql");
    let log = run_dir.join("server.log");
    let started = Instant::now();

    let mut server = spawn_server_with_pack(
        &bin,
        &home,
        port,
        &log,
        &pack,
        tool_root.as_deref(),
        manifest.init.tool_root_env_var.as_deref(),
    )?;
    let outcome = async {
        wait_http(&format!("http://127.0.0.1:{port}/healthz"), &mut server).await?;
        wait_runtime_ready(&graphql, &agent_did, &mut server).await?;
        println!(
            "runtime  ready; waiting for {} event source collection(s)…",
            observed_collections.len()
        );
        for collection in &observed_collections {
            wait_for_event_source(
                &log,
                collection,
                Duration::from_secs(manifest.await_timeout_secs),
            )
            .await?;
            println!("observing {collection}");
        }

        if let Some(scan) = &manifest.scan {
            let scan_root_path = std::path::Path::new(&scan.root);
            let max_chars: usize = scan
                .max_payload_chars
                .parse()
                .context("scan.max_payload_chars")?;
            println!("scanning {} …", scan.root);
            let files = secscan::scan_root(scan_root_path)?;
            let output = secscan::format_payload(&files, max_chars);
            println!(
                "scanned  {} candidate files, {} candidates ({} overflow)",
                output.candidate_files, output.candidate_total, output.overflow_count
            );
            manifest.seed.fields.extend(scan_seed_fields(&output));
        }

        let mutation = seed_mutation(&manifest.seed, &job_id, &prompt)?;
        post_graphql(&graphql, &mutation)
            .await
            .context("seeding the pack")?;
        println!("seeded   1 {} document", manifest.seed.collection);

        let stages = await_stages(
            &graphql,
            &manifest.expect.trigger_ids,
            &manifest.expect.trigger_request_counts,
            &manifest.expect.trigger_request_count_sources,
            &job_id,
            Duration::from_secs(manifest.await_timeout_secs),
        )
        .await?;

        let background_completion = match &manifest.expect.background_completion {
            Some(expected) => {
                println!("background waiting for durable subagent completion acknowledgement…");
                Some(
                    await_background_completion(
                        &bin,
                        &home,
                        &graphql,
                        &agent_did,
                        expected,
                        Duration::from_secs(manifest.await_timeout_secs),
                    )
                    .await?,
                )
            }
            None => None,
        };

        let mut counts: BTreeMap<String, u64> = BTreeMap::new();
        for collection in manifest.expect.collection_counts.keys() {
            counts.insert(collection.clone(), count_rows(&graphql, collection).await);
        }
        let signer_identity = gents::identity::commit_signer_identity_for_did(&agent_did)?;
        let provenance = if manifest.expect.signed_provenance {
            let mut evidence = Vec::with_capacity(stages.len());
            for stage in &stages {
                evidence.push(
                    verify_stage_provenance(
                        &graphql,
                        stage,
                        &signer_identity,
                        manifest
                            .expect
                            .required_tool_call_trigger_ids
                            .contains(&stage.trigger_id),
                    )
                    .await
                    .with_context(|| {
                        format!("verifying signed provenance for {}", stage.request_id)
                    })?,
                );
            }
            evidence
        } else {
            Vec::new()
        };
        let source_edges = verify_source_edges(
            &graphql,
            &manifest.expect.source_edges,
            &stages,
            &provenance,
            &signer_identity,
        )
        .await
        .context("verifying durable source edges")?;
        let fan_in = match manifest.expect.fan_in.as_ref() {
            Some(expected) => verify_fan_in(&graphql, expected, &job_id, &agent_did).await?,
            None => None,
        };
        let mut projection_requests = stages.clone();
        if let Some(evidence) = &background_completion {
            projection_requests.extend(evidence.completed_wake_request_ids.iter().map(
                |request_id| StageResult {
                    trigger_id: "background_completion".to_string(),
                    request_id: request_id.clone(),
                    lifecycle_state: "completed".to_string(),
                    caused_by_source_doc_id: None,
                },
            ));
        }
        let projection_artifacts = render_projection_artifacts(
            &bin,
            &graphql,
            &run_dir,
            &projection_requests,
            &manifest.expect.projections,
        )
        .await?;
        verify_tool_call_expectations(&graphql, &stages, &manifest.expect.tool_calls)
            .await
            .context("verifying persisted tool calls")?;
        let result_documents =
            load_result_documents(&graphql, &manifest.expect.result_documents, &job_id)
                .await
                .context("loading configured result documents")?;
        let (prompt_tokens, completion_tokens) = token_totals(&graphql).await;
        Ok::<_, anyhow::Error>((
            stages,
            background_completion,
            counts,
            provenance,
            source_edges,
            fan_in,
            projection_artifacts,
            result_documents,
            prompt_tokens,
            completion_tokens,
        ))
    }
    .await;

    let _ = server.start_kill();

    let (
        stages,
        background_completion,
        counts,
        provenance,
        source_edges,
        fan_in,
        projection_artifacts,
        result_documents,
        prompt_tokens,
        completion_tokens,
    ) = match outcome {
        Ok(values) => values,
        Err(error) => {
            eprintln!("\nrun failed: {error:#}");
            eprintln!("server log: {}", log.display());
            return Err(error);
        }
    };

    let elapsed = started.elapsed();
    let result_path = if result_documents.is_empty() {
        None
    } else {
        let path = run_dir.join("results.json");
        let text = serde_json::to_string_pretty(&result_documents)?;
        std::fs::write(&path, text).with_context(|| format!("writing {}", path.display()))?;
        Some(path)
    };
    let mut failures: Vec<String> = Vec::new();
    for stage in &stages {
        if stage.lifecycle_state != "completed" {
            failures.push(format!(
                "{} ended {}",
                stage.trigger_id, stage.lifecycle_state
            ));
        }
    }
    for (collection, expected) in &manifest.expect.collection_counts {
        let actual = counts.get(collection).copied().unwrap_or(0);
        if actual < *expected {
            failures.push(format!(
                "{collection}: expected at least {expected}, found {actual}"
            ));
        }
    }

    let meta = json!({
        "pack": manifest.name,
        "job_id": job_id,
        "agent_did": agent_did,
        "endpoint": manifest.init.inference_url,
        "model": manifest.init.model_name,
        "elapsed_secs": elapsed.as_secs(),
        "prompt": prompt,
        "stages": stages.iter().map(|s| json!({
            "trigger_id": s.trigger_id,
            "request_id": s.request_id,
            "lifecycle_state": s.lifecycle_state,
            "caused_by_source_doc_id": s.caused_by_source_doc_id,
        })).collect::<Vec<_>>(),
        "background_completion": background_completion.as_ref().map(|evidence| json!({
            "completed_subagent_request_ids": evidence.completed_subagent_request_ids,
            "failed_subagent_request_ids": evidence.failed_subagent_request_ids,
            "completed_wake_request_ids": evidence.completed_wake_request_ids,
            "pending_notifications": evidence.pending_notifications,
            "acknowledged_notifications": evidence.acknowledged_notifications,
            "stranded_notifications": evidence.stranded_notifications,
            "diagnostics": evidence.diagnostics,
        })),
        "collection_counts": counts,
        "provenance": provenance.iter().map(|evidence| json!({
            "request_id": evidence.request_id,
            "request_doc_id": evidence.request_doc_id,
            "rendered_request_count": evidence.rendered_request_count,
            "request_commit_cids": evidence.request_commit_cids,
            "request_fact_counts": evidence.request_fact_counts,
            "signer_identity": evidence.signer_identity,
        })).collect::<Vec<_>>(),
        "source_edges": source_edges.iter().map(|edge| json!({
            "producer_trigger_id": edge.producer_trigger_id,
            "producer_request_id": edge.producer_request_id,
            "producer_request_doc_id": edge.producer_request_doc_id,
            "producer_tool_name": edge.producer_tool_name,
            "producer_tool_call_doc_id": edge.producer_tool_call_doc_id,
            "source_collection": edge.source_collection,
            "source_doc_id": edge.source_doc_id,
            "source_commit_cids": edge.source_commit_cids,
            "consumer_trigger_id": edge.consumer_trigger_id,
            "consumer_request_id": edge.consumer_request_id,
            "consumer_request_doc_id": edge.consumer_request_doc_id,
        })).collect::<Vec<_>>(),
        "fan_in": fan_in.as_ref().map(|evidence| json!({
            "correlation": evidence.correlation,
            "expected_count": evidence.expected_count,
            "member_count": evidence.member_count,
            "result_count": evidence.result_count,
            "consumer_request_id": evidence.consumer_request_id,
            "report_count": evidence.report_count,
            "candidate_count": evidence.candidate_count,
            "confirmed_count": evidence.confirmed_count,
            "refuted_count": evidence.refuted_count,
            "decision_count": evidence.decision_count,
            "verification_summary_count": evidence.verification_summary_count,
            "final_consumer_request_id": evidence.final_consumer_request_id,
        })),
        "projection_artifacts": projection_artifacts,
        "result_artifact": result_path.as_ref().map(|path| path_arg(path)),
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "ok": failures.is_empty(),
        "failures": failures,
    });
    let meta_path = run_dir.join("meta.json");
    if let Ok(text) = serde_json::to_string_pretty(&meta) {
        let _ = std::fs::write(&meta_path, text);
    }

    println!();
    for stage in &stages {
        println!(
            "  {:<12} {:<10} {}",
            stage.trigger_id, stage.lifecycle_state, stage.request_id
        );
    }
    for (collection, actual) in &counts {
        println!("  {collection:<12} {actual} document(s)");
    }
    if let Some(evidence) = &background_completion {
        println!(
            "  background   {} child request(s), {} wake(s), {} acknowledged, {} pending, {} stranded",
            evidence.completed_subagent_request_ids.len(),
            evidence.completed_wake_request_ids.len(),
            evidence.acknowledged_notifications,
            evidence.pending_notifications,
            evidence.stranded_notifications,
        );
    }
    println!(
        "  tokens       {prompt_tokens} prompt + {completion_tokens} completion in {}s",
        elapsed.as_secs()
    );
    println!("  artifacts    {}", meta_path.display());
    if let Some(path) = &result_path {
        println!("  results      {}", path.display());
    }
    if owned_home && !args.keep_home {
        std::fs::remove_dir_all(&home).ok();
    } else {
        println!("  home         {}", home.display());
    }

    if failures.is_empty() {
        println!("\nok");
        Ok(())
    } else {
        bail!(
            "pack run did not meet expectations: {}",
            failures.join("; ")
        )
    }
}

fn spawn_server_with_pack(
    bin: &Path,
    home: &Path,
    port: u16,
    log: &Path,
    pack: &Path,
    tool_root: Option<&Path>,
    tool_root_env_var: Option<&str>,
) -> Result<tokio::process::Child> {
    let root = path_arg(pack);
    let environment = tool_root
        .zip(tool_root_env_var)
        .map(|(path, name)| vec![(name, path.to_string_lossy().into_owned())])
        .unwrap_or_default();
    spawn_server_with_args_and_env(bin, home, port, log, &["--apply-root", &root], &environment)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load_manifest_defaults(pack: &Path) -> Result<PackManifest> {
        load_manifest_with(pack, &|_| None)
    }

    fn read_pack_json_defaults(path: &Path) -> Result<Value> {
        read_pack_json_with(path, &|_| None)
    }

    /// Regression: tracing colours its file output, so the raw bytes are
    /// `source_collection` ESC `=` ESC `ExperimentJob`. Matching the plain
    /// substring silently never fired and the runner timed out with the
    /// observe line sitting in the log.
    const COLOURED: &str = "\u{1b}[32m INFO\u{1b}[0m \u{1b}[2mgents::trigger_engine::event_source\u{1b}[0m\u{1b}[2m:\u{1b}[0m event source now observing source collection \u{1b}[3msource_collection\u{1b}[0m\u{1b}[2m=\u{1b}[0mExperimentJob \u{1b}[3mgeneration\u{1b}[0m\u{1b}[2m=\u{1b}[0m3";

    #[test]
    fn matches_the_observe_line_through_ansi_colouring() {
        assert!(observes_collection(&strip_ansi(COLOURED), "ExperimentJob"));
    }

    #[test]
    fn observe_line_alone_is_not_ready_to_seed() {
        assert!(!event_source_ready(COLOURED, "ExperimentJob"));
    }

    #[test]
    fn seed_mutation_escapes_prompt_and_extra_fields() {
        let seed = PackSeed {
            collection: "ReviewJob".into(),
            job_id_field: "run_id".into(),
            prompt_field: "focus".into(),
            fields: BTreeMap::from([("repository_path".into(), "/tmp/\"repo\"".into())]),
        };
        let mutation = seed_mutation(&seed, "run-1", "say \"hi\"\nnext").unwrap();
        assert!(mutation.contains("create_ReviewJob"));
        assert!(mutation.contains(r#"run_id: "run-1""#));
        assert!(mutation.contains(r#"focus: "say \"hi\"\nnext""#));
        assert!(mutation.contains(r#"repository_path: "/tmp/\"repo\"""#));
    }

    #[test]
    fn event_trigger_ready_requires_enabled_matching_collection() {
        let response = json!({
            "data": {
                "EventTrigger": [
                    {"trigger_id": "other", "source_collection": "Other", "enabled": true},
                    {"trigger_id": "recon", "source_collection": "ReviewJob", "enabled": false}
                ]
            }
        });
        assert!(!event_trigger_ready(&response, "ReviewJob"));
        let response = json!({
            "data": {
                "EventTrigger": [
                    {"trigger_id": "recon", "source_collection": "ReviewJob", "enabled": true}
                ]
            }
        });
        assert!(event_trigger_ready(&response, "ReviewJob"));
    }

    #[test]
    fn has_correlated_request_requires_rows() {
        assert!(!has_correlated_request(
            &json!({"data": {"AgentRequest": []}})
        ));
        assert!(has_correlated_request(&json!({
            "data": {"AgentRequest": [{"request_id": "r1"}]}
        })));
    }

    #[test]
    fn refuse_stale_review_root_when_both_sides_are_set() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        let applied = dir.path().join("applied");
        let other = dir.path().join("other");
        std::fs::create_dir_all(&home).unwrap();
        std::fs::create_dir_all(&applied).unwrap();
        std::fs::create_dir_all(&other).unwrap();
        std::fs::write(home.join("review-root"), format!("{}\n", applied.display())).unwrap();
        refuse_stale_review_root(&home, Some(&applied)).unwrap();
        let error = refuse_stale_review_root(&home, Some(&other)).unwrap_err();
        assert!(error.to_string().contains("does not match the pack node"));
        refuse_stale_review_root(&home, None).unwrap();
    }

    #[test]
    fn global_update_subscription_completes_seed_readiness() {
        let log = format!(
            "{COLOURED}\n INFO event source opened global Update subscription collections=1 generation=3"
        );
        assert!(event_source_ready(&log, "ExperimentJob"));
    }

    #[test]
    fn does_not_match_a_different_collection() {
        assert!(!observes_collection(
            &strip_ansi(COLOURED),
            "ExperimentFinding"
        ));
    }

    #[test]
    fn does_not_match_a_name_that_merely_prefixes_another() {
        let line = "event source now observing source collection source_collection=ExperimentJobArchive generation=3";
        assert!(!observes_collection(line, "ExperimentJob"));
        assert!(observes_collection(line, "ExperimentJobArchive"));
    }

    #[test]
    fn ignores_unrelated_lines() {
        assert!(!observes_collection(
            "gents behavior started behavior_id=exp-stage1",
            "ExperimentJob"
        ));
    }

    #[test]
    fn signed_fact_gate_requires_every_composite_commit_to_have_the_node_signer() {
        let signer = "did:key:zNode";
        let signed = json!({
            "cid": "bafy-signed",
            "signature": { "identity": signer, "type": "ES256K" }
        });
        let unsigned = json!({ "cid": "bafy-unsigned", "signature": null });

        assert!(require_signed_commits("AgentMessage", "doc-1", &[signed.clone()], signer).is_ok());
        assert!(require_signed_commits("AgentMessage", "doc-1", &[], signer).is_err());
        let error = require_signed_commits("AgentMessage", "doc-1", &[signed, unsigned], signer)
            .unwrap_err()
            .to_string();
        assert!(error.contains("bafy-unsigned"), "{error}");
    }

    #[test]
    fn source_edge_expectations_require_signed_provenance() {
        let manifest = PackManifest {
            name: "invalid-source-edge".to_string(),
            description: String::new(),
            init: PackInit {
                inference_url: "http://127.0.0.1:8080".to_string(),
                model_name: "test".to_string(),
                tool_package: "minimal".to_string(),
                api_key_env_var: None,
                backend_preset: None,
                openai_wire_api: None,
                tool_root: None,
                tool_root_env_var: None,
                tool_root_markers: Vec::new(),
            },
            seed: PackSeed {
                collection: "Source".to_string(),
                job_id_field: "job_id".to_string(),
                prompt_field: "prompt".to_string(),
                fields: BTreeMap::new(),
            },
            default_prompt: String::new(),
            expect: PackExpect {
                trigger_ids: Vec::new(),
                trigger_request_counts: BTreeMap::new(),
                trigger_request_count_sources: BTreeMap::new(),
                collection_counts: BTreeMap::new(),
                projections: Vec::new(),
                signed_provenance: false,
                required_tool_call_trigger_ids: Vec::new(),
                source_edges: vec![SourceEdgeExpectation {
                    producer_trigger_id: "producer".to_string(),
                    producer_tool_name: "create_Source".to_string(),
                    consumer_trigger_id: "consumer".to_string(),
                    source_collection: "Source".to_string(),
                }],
                fan_in: None,
                prompt_tool_contracts: Vec::new(),
                background_completion: None,
                tool_calls: Vec::new(),
                result_documents: Vec::new(),
            },
            await_timeout_secs: 1,
            scan: None,
        };

        let error = validate_manifest(&manifest).expect_err("unsigned source edges must fail");
        assert!(error
            .to_string()
            .contains("source_edges requires expect.signed_provenance=true"));
    }

    #[test]
    fn code_review_prompts_name_the_tools_their_behaviors_advertise() {
        let pack = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../demo/code-review");
        let manifest = load_manifest_defaults(&pack).expect("code-review pack should load");
        assert_eq!(manifest.expect.prompt_tool_contracts.len(), 4);
        assert_eq!(manifest.expect.result_documents.len(), 2);
        let scan_count_source = manifest
            .expect
            .trigger_request_count_sources
            .get("review-scan")
            .expect("review-scan count source");
        assert_eq!(scan_count_source.collection, "ReviewArea");
        assert_eq!(scan_count_source.correlation_field, "run_id");
        assert_eq!(scan_count_source.expected_count_field, "expected_total");
        assert_eq!(
            manifest.seed.fields.get("lens_count").map(String::as_str),
            Some("auto")
        );
        let verify_trigger = read_pack_json_defaults(
            &pack
                .join("event_triggers")
                .join("review-verify")
                .join("object.json"),
        )
        .expect("review-verify trigger");
        assert!(verify_trigger
            .get("group_min_count")
            .is_some_and(Value::is_null));
        for selection in [
            "review-recon-tools",
            "review-scan-tools",
            "review-verify-tools",
        ] {
            let document = read_pack_json_defaults(
                &pack
                    .join("tool-selections")
                    .join(selection)
                    .join("object.json"),
            )
            .unwrap_or_else(|error| panic!("{selection} should load: {error:#}"));
            assert_eq!(
                document.get("enable_lsp").and_then(Value::as_bool),
                Some(true)
            );
            assert_eq!(
                document.get("bash_mode").and_then(Value::as_str),
                Some("Unrestricted")
            );
            assert_eq!(
                document.get("command_network_mode").and_then(Value::as_str),
                Some("enabled")
            );
            assert_eq!(
                document
                    .get("enable_context_budget")
                    .and_then(Value::as_bool),
                Some(true)
            );
            assert!(document
                .get("backgroundable_tool_names")
                .and_then(Value::as_array)
                .is_some_and(|names| {
                    names
                        .iter()
                        .any(|name| name.as_str() == Some("bash_unrestricted"))
                }));
        }
        for behavior in [
            "review-recon",
            "review-scan",
            "review-verify",
            "review-triage",
        ] {
            let document = read_pack_json_defaults(
                &pack
                    .join("agent-behaviors")
                    .join(behavior)
                    .join("object.json"),
            )
            .unwrap_or_else(|error| panic!("{behavior} should load: {error:#}"));
            assert_eq!(
                document.get("compaction_strategy").and_then(Value::as_str),
                Some("StripThenSummarize")
            );
            assert_eq!(
                document.get("compaction_threshold").and_then(Value::as_f64),
                Some(0.85)
            );
        }
        let triage_tools = read_pack_json_defaults(
            &pack
                .join("tool-selections")
                .join("review-triage-tools")
                .join("object.json"),
        )
        .expect("review-triage-tools should load");
        assert_eq!(
            triage_tools
                .get("enable_context_budget")
                .and_then(Value::as_bool),
            Some(true)
        );
        for (surface, tool_name) in [
            ("review-recon-writes", "write_review_area"),
            ("review-scan-writes", "write_scan_result"),
            ("review-verify-writes", "write_verification_summary"),
            ("review-triage-writes", "write_triage_report"),
        ] {
            let document = read_pack_json_defaults(
                &pack
                    .join("datastore-tool-surfaces")
                    .join(surface)
                    .join("object.json"),
            )
            .unwrap_or_else(|error| panic!("{surface} should load: {error:#}"));
            let entry = document["entries"]
                .as_array()
                .and_then(|entries| entries.iter().find(|entry| entry["tool_name"] == tool_name))
                .unwrap_or_else(|| panic!("{surface} should declare {tool_name}"));
            assert_eq!(entry["output_obligation"]["scope"], "trigger");
            assert_eq!(entry["output_obligation"]["minimum_writes"], 1);
            if tool_name == "write_review_area" {
                assert_eq!(
                    entry["output_obligation"]["expected_count_field"],
                    "expected_total"
                );
            }
        }
        let recon_prompt = std::fs::read_to_string(
            pack.join("tasks")
                .join("review-recon-task")
                .join("prompt.md"),
        )
        .expect("review recon prompt should load");
        assert!(recon_prompt.contains("You are not a scanner"));
        assert!(recon_prompt.contains("Never repeat a successful repository command"));
        let scan_prompt = std::fs::read_to_string(
            pack.join("tasks")
                .join("review-scan-task")
                .join("prompt.md"),
        )
        .expect("review scan prompt should load");
        assert!(scan_prompt.contains("Never repeat an identical tool call"));
        for profile in [
            "review-recon-profile",
            "review-scan-profile",
            "review-verify-profile",
            "review-profile",
        ] {
            let document = read_pack_json_defaults(
                &pack
                    .join("inference-profiles")
                    .join(profile)
                    .join("object.json"),
            )
            .unwrap_or_else(|error| panic!("{profile} should load: {error:#}"));
            assert_eq!(
                document.get("context_window").and_then(Value::as_u64),
                Some(262_144)
            );
            assert_eq!(
                document.get("max_output_tokens").and_then(Value::as_u64),
                Some(65_536)
            );
            assert_eq!(
                document.get("max_turns").and_then(Value::as_u64),
                Some(1_000_000)
            );
            assert_eq!(
                document.get("temperature").and_then(Value::as_f64),
                Some(1.0)
            );
            assert_eq!(document.get("top_p").and_then(Value::as_f64), Some(0.95));
            assert_eq!(
                document.get("stream_batch_ms").and_then(Value::as_u64),
                Some(5_000)
            );
            assert_eq!(
                document
                    .get("deadline_duration_secs")
                    .and_then(Value::as_u64),
                Some(86_400)
            );
            assert_eq!(
                document
                    .get("stream_liveness_timeout_secs")
                    .and_then(Value::as_u64),
                Some(86_400)
            );
            assert_eq!(
                document.get("retry_max_transport").and_then(Value::as_u64),
                Some(720)
            );
        }
    }

    #[test]
    fn defending_code_pack_is_typed_static_and_closes_both_fan_outs() {
        let pack = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../demo/defending-code");
        let manifest = load_manifest_defaults(&pack).expect("defending-code pack should load");
        assert_eq!(manifest.expect.prompt_tool_contracts.len(), 14);
        assert_eq!(manifest.expect.result_documents.len(), 17);
        assert_eq!(manifest.init.tool_package, "write");

        for (trigger, collection) in [
            ("defend-scan", "DefenseReviewArea"),
            ("defend-verifier", "DefenseVerificationAssignment"),
            ("defend-contract-review", "DefenseRootCauseCluster"),
            ("defend-patch", "DefensePatchAssignment"),
            ("defend-patch-validation", "DefensePatchCandidate"),
            ("defend-patch-review", "DefensePatchValidation"),
            ("defend-patch-security-review", "DefensePatchReview"),
        ] {
            let source = manifest
                .expect
                .trigger_request_count_sources
                .get(trigger)
                .unwrap_or_else(|| panic!("{trigger} should have a count source"));
            assert_eq!(source.collection, collection);
            assert_eq!(source.correlation_field, "run_id");
            assert_eq!(source.expected_count_field, "expected_total");
        }

        let fan_in = manifest.expect.fan_in.as_ref().expect("fan-in contract");
        assert_eq!(fan_in.member_collection, "DefenseReviewArea");
        assert_eq!(fan_in.result_collection, "DefenseScanResult");
        assert_eq!(fan_in.report_collection, "DefenseReport");
        assert_eq!(fan_in.min_expected_count, Some(4));
        assert_eq!(fan_in.max_expected_count, Some(10));

        for (trigger, collection, fire_mode) in [
            ("defend-verification-plan", "DefenseScanResult", "per_group"),
            (
                "defend-verifier",
                "DefenseVerificationAssignment",
                "per_document",
            ),
            (
                "defend-triage",
                "DefenseVerificationCompletion",
                "per_group",
            ),
            ("defend-cluster", "DefenseTriageSummary", "per_document"),
            (
                "defend-contract-review",
                "DefenseRootCauseCluster",
                "per_document",
            ),
            (
                "defend-remediation-plan",
                "DefenseContractReview",
                "per_group",
            ),
            (
                "defend-patch-validation",
                "DefensePatchCandidate",
                "per_document",
            ),
            (
                "defend-patch-review",
                "DefensePatchValidation",
                "per_document",
            ),
            (
                "defend-patch-security-review",
                "DefensePatchReview",
                "per_document",
            ),
        ] {
            let document = read_pack_json_defaults(
                &pack
                    .join("event_triggers")
                    .join(trigger)
                    .join("object.json"),
            )
            .unwrap_or_else(|error| panic!("{trigger} should load: {error:#}"));
            assert_eq!(
                document.get("source_collection").and_then(Value::as_str),
                Some(collection)
            );
            assert_eq!(
                document.get("fire_mode").and_then(Value::as_str),
                Some(fire_mode)
            );
        }

        for selection in [
            "defend-threat-model-tools",
            "defend-plan-tools",
            "defend-scan-tools",
            "defend-verification-plan-tools",
            "defend-triage-tools",
            "defend-verifier-tools",
            "defend-cluster-tools",
            "defend-contract-review-tools",
            "defend-remediation-plan-tools",
            "defend-patch-tools",
            "defend-patch-validation-tools",
            "defend-patch-review-tools",
            "defend-patch-security-review-tools",
            "defend-report-tools",
        ] {
            let document = read_pack_json_defaults(
                &pack
                    .join("tool-selections")
                    .join(selection)
                    .join("object.json"),
            )
            .unwrap_or_else(|error| panic!("{selection} should load: {error:#}"));
            assert_eq!(
                document.get("enable_defra_query").and_then(Value::as_bool),
                Some(false),
                "{selection} must use collection-bound reads"
            );
            let expected_network_mode = if matches!(
                selection,
                "defend-threat-model-tools"
                    | "defend-plan-tools"
                    | "defend-scan-tools"
                    | "defend-verifier-tools"
                    | "defend-contract-review-tools"
                    | "defend-patch-tools"
                    | "defend-patch-validation-tools"
                    | "defend-patch-review-tools"
                    | "defend-patch-security-review-tools"
            ) {
                "enabled"
            } else {
                "disabled"
            };
            assert_eq!(
                document.get("command_network_mode").and_then(Value::as_str),
                Some(expected_network_mode)
            );
        }

        for selection in [
            "defend-threat-model-tools",
            "defend-plan-tools",
            "defend-scan-tools",
            "defend-verifier-tools",
            "defend-patch-tools",
        ] {
            let document = read_pack_json_defaults(
                &pack
                    .join("tool-selections")
                    .join(selection)
                    .join("object.json"),
            )
            .unwrap_or_else(|error| panic!("{selection} should load: {error:#}"));
            assert_eq!(
                document.get("enable_bash").and_then(Value::as_bool),
                Some(true)
            );
            assert_eq!(
                document.get("bash_mode").and_then(Value::as_str),
                Some("Unrestricted")
            );
            assert_eq!(
                document
                    .get("command_execution_policy")
                    .and_then(Value::as_str),
                Some("unrestricted")
            );
            assert_eq!(
                document.get("enable_lsp").and_then(Value::as_bool),
                Some(true)
            );
            assert!(document
                .get("lsp_config")
                .and_then(Value::as_str)
                .is_some_and(|config| config.contains("rust-analyzer")));
            assert_eq!(
                document
                    .get("backgroundable_tool_names")
                    .and_then(Value::as_array)
                    .and_then(|names| names.first())
                    .and_then(Value::as_str),
                Some("bash_unrestricted")
            );
        }

        let triage_tools = read_pack_json_defaults(
            &pack
                .join("tool-selections")
                .join("defend-triage-tools")
                .join("object.json"),
        )
        .expect("triage reducer tools should load");
        assert_eq!(
            triage_tools
                .get("subagent_spawn_enabled")
                .and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            triage_tools
                .get("subagent_background_enabled")
                .and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            triage_tools
                .get("subagent_default_await_mode")
                .is_some_and(Value::is_null),
            true
        );
        assert_eq!(
            triage_tools.get("enable_bash").and_then(Value::as_bool),
            Some(false)
        );
        assert!(triage_tools
            .get("subagent_targets")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty));
        let triage_prompt = std::fs::read_to_string(
            pack.join("tasks")
                .join("defend-triage-task")
                .join("prompt.md"),
        )
        .expect("triage reducer prompt should load");
        assert!(!triage_prompt.contains("spawn_subagent"));

        let verifier_surface = read_pack_json_defaults(
            &pack
                .join("datastore-tool-surfaces")
                .join("defend-verifier-io")
                .join("object.json"),
        )
        .expect("verifier datastore surface should load");
        let completion_write = verifier_surface
            .get("entries")
            .and_then(Value::as_array)
            .and_then(|entries| {
                entries.iter().find(|entry| {
                    entry.get("tool_name").and_then(Value::as_str)
                        == Some("write_defense_verification_completion")
                })
            })
            .expect("verifier surface should expose completion writes");
        assert_eq!(
            completion_write
                .get("output_obligation")
                .and_then(|obligation| obligation.get("scope"))
                .and_then(Value::as_str),
            Some("trigger"),
            "every event-triggered verifier request must close its assignment"
        );

        let verification_plan_surface = read_pack_json_defaults(
            &pack
                .join("datastore-tool-surfaces")
                .join("defend-verification-plan-io")
                .join("object.json"),
        )
        .expect("verification-plan datastore surface should load");
        let assignment_write = verification_plan_surface
            .get("entries")
            .and_then(Value::as_array)
            .and_then(|entries| {
                entries.iter().find(|entry| {
                    entry.get("tool_name").and_then(Value::as_str)
                        == Some("write_defense_verification_assignment")
                })
            })
            .expect("verification-plan surface should expose assignment writes");
        assert_eq!(
            assignment_write
                .get("output_obligation")
                .and_then(|obligation| obligation.get("expected_count_field"))
                .and_then(Value::as_str),
            Some("expected_total"),
            "the planner must close the exact verifier work set"
        );

        for (task, surface, source_collection, source_template, redundant_read) in [
            (
                "defend-threat-model-task",
                "defend-threat-model-writes",
                "DefendingCodeJob",
                "{{ doc.repository_path }}",
                "read_defending_code_job",
            ),
            (
                "defend-plan-task",
                "defend-plan-writes",
                "DefenseThreatModel",
                "{{ doc.source_revision }}",
                "read_defense_threat_model",
            ),
            (
                "defend-scan-task",
                "defend-scan-writes",
                "DefenseReviewArea",
                "{{ doc.area_id }}",
                "read_defense_review_area",
            ),
            (
                "defend-verification-plan-task",
                "defend-verification-plan-io",
                "DefenseScanResult",
                "{{ group.docs }}",
                "read_defense_scan_result",
            ),
            (
                "defend-verifier-task",
                "defend-verifier-io",
                "DefenseVerificationAssignment",
                "{{ doc.assignment_id }}",
                "read_defense_verification_assignment",
            ),
            (
                "defend-triage-task",
                "defend-triage-io",
                "DefenseVerificationCompletion",
                "{{ group.docs }}",
                "read_defense_verification_completion",
            ),
            (
                "defend-cluster-task",
                "defend-cluster-io",
                "DefenseTriageSummary",
                "{{ doc.promoted_count }}",
                "read_defense_triage_summary",
            ),
            (
                "defend-contract-review-task",
                "defend-contract-review-io",
                "DefenseRootCauseCluster",
                "{{ doc.cluster_id }}",
                "read_defense_root_cause_cluster",
            ),
            (
                "defend-remediation-plan-task",
                "defend-remediation-plan-io",
                "DefenseContractReview",
                "{{ group.docs }}",
                "read_defense_contract_review",
            ),
            (
                "defend-patch-task",
                "defend-patch-io",
                "DefensePatchAssignment",
                "{{ doc.assignment_id }}",
                "read_defense_patch_assignment",
            ),
            (
                "defend-patch-validation-task",
                "defend-patch-validation-writes",
                "DefensePatchCandidate",
                "{{ doc.diff }}",
                "read_defense_patch_candidate",
            ),
            (
                "defend-patch-review-task",
                "defend-patch-review-writes",
                "DefensePatchValidation",
                "{{ doc.validated_diff_sha256 }}",
                "read_defense_patch_validation",
            ),
            (
                "defend-patch-security-review-task",
                "defend-patch-security-review-io",
                "DefensePatchReview",
                "{{ doc.validation_id }}",
                "read_defense_patch_review",
            ),
            (
                "defend-report-task",
                "defend-report-io",
                "DefensePatchSecurityReview",
                "{{ group.docs }}",
                "read_defense_patch_security_review",
            ),
        ] {
            let prompt = std::fs::read_to_string(pack.join("tasks").join(task).join("prompt.md"))
                .unwrap_or_else(|error| panic!("{task} prompt should load: {error}"));
            assert!(
                prompt.contains(source_template),
                "{task} must interpolate its trigger document directly"
            );
            assert!(
                !prompt.contains(redundant_read),
                "{task} must not re-query its trigger document with {redundant_read}"
            );
            let datastore = read_pack_json_defaults(
                &pack
                    .join("datastore-tool-surfaces")
                    .join(surface)
                    .join("object.json"),
            )
            .unwrap_or_else(|error| panic!("{surface} should load: {error:#}"));
            assert!(
                datastore
                    .get("entries")
                    .and_then(Value::as_array)
                    .is_some_and(|entries| entries.iter().all(|entry| {
                        entry.get("kind").and_then(Value::as_str) != Some("read")
                            || entry.get("collection").and_then(Value::as_str)
                                != Some(source_collection)
                    })),
                "{surface} must not expose a redundant read of trigger source {source_collection}"
            );
        }

        for selection in [
            "defend-contract-review-tools",
            "defend-patch-validation-tools",
            "defend-patch-review-tools",
            "defend-patch-security-review-tools",
        ] {
            let document = read_pack_json_defaults(
                &pack
                    .join("tool-selections")
                    .join(selection)
                    .join("object.json"),
            )
            .unwrap_or_else(|error| panic!("{selection} should load: {error:#}"));
            assert_eq!(
                document.get("enable_bash").and_then(Value::as_bool),
                Some(true)
            );
            assert_eq!(
                document.get("enable_lsp").and_then(Value::as_bool),
                Some(true)
            );
        }

        let report_tools = read_pack_json_defaults(
            &pack
                .join("tool-selections")
                .join("defend-report-tools")
                .join("object.json"),
        )
        .expect("report tools should load");
        assert_eq!(
            report_tools.get("enable_bash").and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            report_tools.get("enable_lsp").and_then(Value::as_bool),
            Some(false)
        );

        let backend = read_pack_json_defaults(
            &pack
                .join("inference-backends")
                .join("defending-backend")
                .join("object.json"),
        )
        .expect("defending backend should load");
        assert_eq!(
            backend.get("max_concurrent").and_then(Value::as_u64),
            Some(8)
        );

        for behavior in [
            "defend-threat-model",
            "defend-plan",
            "defend-scan",
            "defend-verification-plan",
            "defend-triage",
            "defend-verifier",
            "defend-cluster",
            "defend-contract-review",
            "defend-remediation-plan",
            "defend-patch",
            "defend-patch-validation",
            "defend-patch-review",
            "defend-patch-security-review",
            "defend-report",
        ] {
            let document = read_pack_json_defaults(
                &pack
                    .join("agent-behaviors")
                    .join(behavior)
                    .join("object.json"),
            )
            .unwrap_or_else(|error| panic!("{behavior} should load: {error:#}"));
            assert_eq!(
                document.get("compaction_threshold").and_then(Value::as_f64),
                Some(0.762_939_453_125),
                "{behavior} should compact at 200,000 of 262,144 tokens"
            );
        }

        let review_prompt = std::fs::read_to_string(
            pack.join("tasks")
                .join("defend-patch-review-task")
                .join("prompt.md"),
        )
        .expect("patch review prompt should load");
        assert!(review_prompt.contains("do not receive scanner conversation"));
        assert!(!review_prompt.contains("{{ doc.rationale }}"));
        assert!(!review_prompt.contains("{{ doc.description }}"));

        let read_surface = std::fs::read_to_string(
            pack.join("datastore-tool-surfaces")
                .join("defend-report-io")
                .join("object.json"),
        )
        .expect("report surface should load");
        assert!(!read_surface.contains("defra_query"));
        assert!(read_surface.contains("read_defense_patch_review"));
    }

    #[test]
    fn repo_maintenance_pack_preserves_categories_and_worktree_sized_packages() {
        let pack = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../demo/repo-maintenance");
        let manifest = load_manifest_defaults(&pack).expect("repo-maintenance pack should load");
        assert_eq!(manifest.expect.prompt_tool_contracts.len(), 6);
        assert_eq!(manifest.expect.result_documents.len(), 6);
        assert!(manifest
            .default_prompt
            .contains("one shared branch and worktree"));
        assert!(!manifest
            .default_prompt
            .contains("independent 1-3 finding worktrees"));
        assert_eq!(
            manifest.seed.fields.get("area_count").map(String::as_str),
            Some("auto")
        );
        assert_eq!(
            manifest
                .seed
                .fields
                .get("history_depth")
                .map(String::as_str),
            Some("250")
        );
        assert_eq!(
            manifest.seed.fields.get("pr_base").map(String::as_str),
            Some("main")
        );
        assert_eq!(
            manifest
                .seed
                .fields
                .get("worktree_parent")
                .map(String::as_str),
            Some("..")
        );
        assert_eq!(
            manifest
                .seed
                .fields
                .get("worktree_path")
                .map(String::as_str),
            Some("../gents-maintenance")
        );
        assert_eq!(
            manifest
                .seed
                .fields
                .get("suggested_branch")
                .map(String::as_str),
            Some("agent/maintenance")
        );
        let fan_in = manifest.expect.fan_in.as_ref().expect("fan-in contract");
        assert_eq!(fan_in.min_expected_count, Some(5));
        assert_eq!(fan_in.max_expected_count, Some(10));

        let recon_prompt = std::fs::read_to_string(
            pack.join("tasks")
                .join("maintenance-recon-task")
                .join("prompt.md"),
        )
        .expect("maintenance recon prompt should load");
        for category in [
            "dead-surface",
            "duplicate-ownership",
            "test-value",
            "module-boundaries",
            "comment-contract-drift",
        ] {
            assert!(recon_prompt.contains(category), "missing {category}");
        }

        let triage_surface = read_pack_json_defaults(
            &pack
                .join("datastore-tool-surfaces")
                .join("maintenance-triage-writes")
                .join("object.json"),
        )
        .expect("maintenance triage surface should load");
        let package_entry = triage_surface["entries"]
            .as_array()
            .and_then(|entries| {
                entries.iter().find(|entry| {
                    entry["tool_name"].as_str() == Some("write_maintenance_work_package")
                })
            })
            .expect("maintenance work-package writer");
        assert_eq!(package_entry["collection"], "MaintenanceWorkPackage");

        let triage_prompt = std::fs::read_to_string(
            pack.join("tasks")
                .join("maintenance-triage-task")
                .join("prompt.md"),
        )
        .expect("maintenance triage prompt should load");
        assert!(triage_prompt.contains("becomes exactly one commit"));
        assert!(triage_prompt.contains("runtime-owned execution boundary"));
        assert!(triage_prompt.contains("do not supply or reinterpret them"));

        let execute_trigger = read_pack_json_defaults(
            &pack
                .join("event_triggers")
                .join("maintenance-execute")
                .join("object.json"),
        )
        .expect("maintenance execute trigger should load");
        assert_eq!(execute_trigger["concurrency"], "serial");
        assert_eq!(execute_trigger["source_collection"], "MaintenanceReport");

        let execute_prompt = std::fs::read_to_string(
            pack.join("tasks")
                .join("maintenance-execute-task")
                .join("prompt.md"),
        )
        .expect("maintenance execute prompt should load");
        assert!(execute_prompt.contains("single execution owner"));
        assert!(execute_prompt.contains("On a fresh execution with no prior results"));
        assert!(execute_prompt.contains("On a valid completed-prefix resume"));
        assert!(execute_prompt.contains("Process packages strictly in numeric order"));
        assert!(execute_prompt.contains("write_maintenance_execution_summary"));

        let makefile = std::fs::read_to_string(pack.join("../../Makefile"))
            .expect("repository Makefile should load");
        assert!(makefile.contains("test \"$(MAINTENANCE_AREAS)\" -ge \"$(MAINTENANCE_MIN_AREAS)\""));

        let publish_trigger = read_pack_json_defaults(
            &pack
                .join("event_triggers")
                .join("maintenance-publish")
                .join("object.json"),
        )
        .expect("maintenance publish trigger should load");
        assert_eq!(
            publish_trigger["source_collection"],
            "MaintenanceExecutionSummary"
        );

        let publish_prompt = std::fs::read_to_string(
            pack.join("tasks")
                .join("maintenance-publish-task")
                .join("prompt.md"),
        )
        .expect("maintenance publish prompt should load");
        assert!(publish_prompt.contains("one normal, non-draft PR"));
        assert!(publish_prompt.contains("Bound this at two full review rounds"));
        assert!(publish_prompt.contains("cargo fmt --all --check"));
        assert!(publish_prompt.contains("poll at intervals no longer than 60 seconds"));
        assert!(publish_prompt.contains("Never kill by port or broad process-name match"));
    }

    #[test]
    fn every_checked_in_demo_pack_loads() {
        let demo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../demo");
        let mut packs = std::fs::read_dir(&demo)
            .expect("read demo directory")
            .map(|entry| entry.expect("read demo entry").path())
            .filter(|path| path.join("experiment.json").is_file())
            .collect::<Vec<_>>();
        packs.sort();
        assert!(!packs.is_empty(), "expected checked-in demo packs");
        for pack in packs {
            load_manifest_defaults(&pack)
                .unwrap_or_else(|error| panic!("{} should load: {error:#}", pack.display()));
        }
    }

    #[test]
    fn omitted_tool_package_keeps_the_minimal_ceiling() {
        let pack = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../demo/pipeline");
        let manifest = load_manifest_defaults(&pack).expect("pipeline pack should load");
        assert_eq!(manifest.init.tool_package, "minimal");
        assert!(manifest.init.tool_root.is_none());
    }

    #[test]
    fn created_doc_reference_requires_the_exact_collection_and_doc_token() {
        assert_eq!(
            created_doc_reference("created ExperimentFinding bae-source", "ExperimentFinding"),
            Some("bae-source")
        );
        assert_eq!(
            created_doc_reference("created OtherFinding bae-source", "ExperimentFinding"),
            None
        );
        assert_eq!(
            created_doc_reference(
                "created ExperimentFinding bae-source trailing",
                "ExperimentFinding"
            ),
            None
        );
        assert_eq!(
            created_doc_reference(
                "prefix created ExperimentFinding bae-source",
                "ExperimentFinding"
            ),
            None
        );
    }

    #[test]
    fn stage_request_query_scopes_correlated_triggers_to_the_current_run() {
        let query = stage_requests_query("review-\"scan", Some("run-\"42"));
        assert!(query.contains(r#"caused_by_trigger_id: { _eq: "review-\"scan" }"#));
        assert!(query.contains(r#"caused_by_correlation: { _eq: "run-\"42" }"#));
    }

    #[test]
    fn stage_request_query_accepts_null_correlation_for_legacy_pipeline_triggers() {
        let query = stage_requests_query("exp-stage1", None);
        assert!(query.contains(r#"caused_by_trigger_id: { _eq: "exp-stage1" }"#));
        assert!(!query.contains("caused_by_correlation"));
    }

    #[test]
    fn background_completion_expectation_requires_the_whole_delivery_path() {
        let expected = BackgroundCompletionExpectation {
            min_completed_subagent_requests: 2,
            min_completed_wakes: 1,
            min_acknowledged_notifications: 2,
            max_pending_notifications: 0,
            max_stranded_notifications: 0,
        };
        let complete = BackgroundCompletionEvidence {
            completed_subagent_request_ids: vec!["child-1".into(), "child-2".into()],
            failed_subagent_request_ids: Vec::new(),
            completed_wake_request_ids: vec!["wake-1".into()],
            pending_notifications: 0,
            acknowledged_notifications: 2,
            stranded_notifications: 0,
            diagnostics: Value::Null,
        };
        assert!(complete.satisfies(&expected));

        let mut pending = complete.clone();
        pending.pending_notifications = 1;
        assert!(!pending.satisfies(&expected));

        let mut missing_wake = complete;
        missing_wake.completed_wake_request_ids.clear();
        assert!(!missing_wake.satisfies(&expected));
    }

    #[test]
    fn lsp_rust_pack_declares_readonly_ceiling_and_tool_calls() {
        let pack = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../demo/lsp-rust");
        let manifest = load_manifest_defaults(&pack).expect("demo/lsp-rust experiment.json");
        assert_eq!(manifest.init.tool_package, "readonly");
        assert!(manifest
            .init
            .tool_root
            .as_deref()
            .is_some_and(|root| !root.is_empty()));
        assert_eq!(
            manifest.init.tool_root_env_var.as_deref(),
            Some("GENTS_LSP_WORKSPACE")
        );
        assert!(manifest
            .expect
            .tool_calls
            .iter()
            .any(|call| call.tool_name == "lsp" && call.action.as_deref() == Some("hover")));
        assert!(manifest
            .expect
            .tool_calls
            .iter()
            .any(|call| call.result_contains.iter().any(|n| n == "FileToolMode")));
        for (file, symbol, result_needle) in [
            (
                "crates/gents/src/toolset/shared/command.rs",
                "meet",
                "Disabled",
            ),
            (
                "crates/gents/src/toolset/lsp/auth.rs",
                "lsp_advertised",
                "FileToolMode",
            ),
        ] {
            assert!(manifest.expect.tool_calls.iter().any(|call| {
                call.tool_name == "lsp"
                    && call.action.as_deref() == Some("hover")
                    && call.file.as_deref() == Some(file)
                    && call.symbol.as_deref() == Some(symbol)
                    && call
                        .result_contains
                        .iter()
                        .any(|needle| needle == result_needle)
            }));
        }
    }

    #[test]
    fn pack_tool_root_requires_declared_markers() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("Cargo.toml"), "[workspace]\n").unwrap();
        let markers = vec!["Cargo.toml".to_string(), "crates/gents".to_string()];
        let error =
            resolve_pack_tool_root(root.path(), Some(root.path().to_str().unwrap()), &markers)
                .unwrap_err();
        assert!(error.to_string().contains("crates/gents"), "{error}");

        std::fs::create_dir_all(root.path().join("crates/gents")).unwrap();
        let resolved =
            resolve_pack_tool_root(root.path(), Some(root.path().to_str().unwrap()), &markers)
                .unwrap();
        assert_eq!(resolved, std::fs::canonicalize(root.path()).unwrap());
    }

    #[test]
    fn readonly_pack_requires_tool_root() {
        let mut manifest = PackManifest {
            name: "lsp".into(),
            description: String::new(),
            init: PackInit {
                inference_url: "http://127.0.0.1:8080".into(),
                model_name: "test".into(),
                api_key_env_var: None,
                backend_preset: None,
                openai_wire_api: None,
                tool_package: "readonly".into(),
                tool_root: None,
                tool_root_env_var: None,
                tool_root_markers: Vec::new(),
            },
            seed: PackSeed {
                collection: "Job".into(),
                job_id_field: "job_id".into(),
                prompt_field: "prompt".into(),
                fields: BTreeMap::new(),
            },
            default_prompt: String::new(),
            expect: PackExpect {
                trigger_ids: Vec::new(),
                trigger_request_counts: BTreeMap::new(),
                trigger_request_count_sources: BTreeMap::new(),
                collection_counts: BTreeMap::new(),
                projections: Vec::new(),
                signed_provenance: false,
                required_tool_call_trigger_ids: Vec::new(),
                source_edges: Vec::new(),
                fan_in: None,
                prompt_tool_contracts: Vec::new(),
                background_completion: None,
                tool_calls: Vec::new(),
                result_documents: Vec::new(),
            },
            await_timeout_secs: 1,
            scan: None,
        };
        let error = validate_manifest(&manifest).expect_err("readonly needs tool_root");
        assert!(error.to_string().contains("tool_root"), "{error}");
        manifest.init.tool_root = Some(".".into());
        validate_manifest(&manifest).expect("declared tool_root is enough");
    }

    #[test]
    fn tool_call_match_requires_completed_action_and_needles() {
        let expected = ToolCallExpectation {
            trigger_id: "lsp-hover".into(),
            tool_name: "lsp".into(),
            action: Some("hover".into()),
            file: Some("src/auth.rs".into()),
            symbol: None,
            result_contains: vec!["FileToolMode".into()],
        };
        let ok = json!({
            "tool_name": "lsp",
            "status": "completed",
            "lifecycle_state": "completed",
            "args": "{\"action\":\"hover\",\"file\":\"src/auth.rs\"}",
            "result": "pub fn lsp_advertised(lsp: bool, file: FileToolMode) -> bool"
        });
        assert!(tool_call_matches(&ok, &expected));
        let status_only = json!({
            "tool_name": "lsp",
            "status": "completed",
            "lifecycle_state": "completed",
            "args": "{\"action\":\"status\"}",
            "result": "Language servers: rust-analyzer (ready)"
        });
        assert!(!tool_call_matches(&status_only, &expected));
        let empty_hover = json!({
            "tool_name": "lsp",
            "status": "completed",
            "lifecycle_state": "completed",
            "args": "{\"action\":\"hover\"}",
            "result": "No hover information"
        });
        assert!(!tool_call_matches(&empty_hover, &expected));
        let wrong_file = json!({
            "tool_name": "lsp",
            "status": "completed",
            "lifecycle_state": "completed",
            "args": "{\"action\":\"hover\",\"file\":\"src/other.rs\"}",
            "result": "pub fn lsp_advertised(lsp: bool, file: FileToolMode) -> bool"
        });
        assert!(!tool_call_matches(&wrong_file, &expected));
        let empty_symbols = json!({
            "tool_name": "lsp",
            "status": "completed",
            "lifecycle_state": "completed",
            "args": "{\"action\":\"symbols\",\"file\":\"src/auth.rs\"}",
            "result": "No result"
        });
        assert!(!tool_call_matches(
            &empty_symbols,
            &ToolCallExpectation {
                trigger_id: "lsp-hover".into(),
                tool_name: "lsp".into(),
                action: Some("symbols".into()),
                file: Some("src/auth.rs".into()),
                symbol: None,
                result_contains: Vec::new(),
            }
        ));
        let failed_lifecycle = json!({
            "tool_name": "lsp",
            "status": "completed",
            "lifecycle_state": "failed",
            "args": "{\"action\":\"hover\",\"file\":\"src/auth.rs\"}",
            "result": "pub fn lsp_advertised(lsp: bool, file: FileToolMode) -> bool"
        });
        assert!(!tool_call_matches(&failed_lifecycle, &expected));
    }

    #[test]
    fn manifest_parses_optional_scan_section() {
        let manifest: PackManifest = serde_json::from_value(serde_json::json!({
            "name": "t", "init": {"inference_url": "http://x", "model_name": "m"},
            "seed": {"collection": "ScanJob", "job_id_field": "run_id", "prompt_field": "focus"},
            "expect": {"trigger_ids": []},
            "scan": {"root": ".", "max_payload_chars": "1024"}
        }))
        .expect("manifest with scan");
        let scan = manifest.scan.expect("scan section");
        assert_eq!(scan.root, ".");
        assert_eq!(scan.max_payload_chars, "1024");

        let bare: PackManifest = serde_json::from_value(serde_json::json!({
            "name": "t", "init": {"inference_url": "http://x", "model_name": "m"},
            "seed": {"collection": "J", "job_id_field": "run_id", "prompt_field": "focus"},
            "expect": {"trigger_ids": []}
        }))
        .expect("manifest without scan");
        assert!(bare.scan.is_none());
    }

    #[test]
    fn scan_seed_fields_render_all_counters() {
        let output = secscan::ScanOutput {
            payload: "files: 1  candidates: 2\nsrc/a.rs\n  [precise] graphql-injection L3: x"
                .to_string(),
            candidate_total: 2,
            candidate_files: 1,
            slug_counts: vec![("graphql-injection".to_string(), 2)],
            overflow_count: 0,
        };
        let fields = scan_seed_fields(&output);
        assert_eq!(fields.get("candidate_total").map(String::as_str), Some("2"));
        assert_eq!(fields.get("candidate_files").map(String::as_str), Some("1"));
        assert_eq!(fields.get("overflow_count").map(String::as_str), Some("0"));
        assert_eq!(
            fields.get("slug_counts").map(String::as_str),
            Some("graphql-injection=2")
        );
        assert!(fields.get("candidates").unwrap().contains("src/a.rs"));
    }

    /// Regression for audit finding `seed-mutation-unvalidated-identifiers`:
    /// the seed collection and field keys are interpolated as bare GraphQL
    /// identifiers, so a malformed pack manifest must be rejected instead of
    /// producing an injectable mutation.
    #[test]
    fn seed_mutation_validates_identifiers() {
        let seed = |collection: &str, fields: BTreeMap<String, String>| PackSeed {
            collection: collection.to_string(),
            job_id_field: "run_id".to_string(),
            prompt_field: "focus".to_string(),
            fields,
        };

        assert!(seed_mutation(&seed("ScanJob", BTreeMap::new()), "job-1", "hi").is_ok());

        let bad_collection = seed(
            "ScanJob) { _docID } } mutation evil { x(input: { a",
            BTreeMap::new(),
        );
        assert!(seed_mutation(&bad_collection, "job-1", "hi").is_err());

        let mut bad_fields = BTreeMap::new();
        bad_fields.insert("a\": \"x\", evil".to_string(), "v".to_string());
        assert!(seed_mutation(&seed("ScanJob", bad_fields), "job-1", "hi").is_err());
    }
}
