//! Shared persona-request materializer (directory persona catalog, PR 2 /
//! Task 2).
//!
//! `PersonaConfigRequest` rows are phone-authored asks to create, clone,
//! edit, or disable a persona's backing `AgentBehavior`. Three write
//! channels mint the SAME outcome from one of these rows: the server-side
//! persona reconciler (P2P-replicated requests), the agent's own
//! self-configuration tool, and the `gents` CLI. This module is the single
//! core all three call so admission and materialization can never drift
//! between them.
//!
//! The split mirrors the runtime's usual boundary between decision and
//! effect:
//! - [`decide_persona_request`] is a pure function — no I/O, no clock, no
//!   randomness. It answers exactly one question: does this request, against
//!   this catalog snapshot, get to touch config at all?
//! - [`apply_persona_request`] assumes admission already ran (every
//!   `.context("... admission guaranteed")` in this module documents a
//!   precondition [`decide_persona_request`] establishes, not a check this
//!   function repeats) and performs the actual writes through the proven
//!   `config_client` writers. It is idempotent: replaying the same
//!   `request_key` after a successful create returns the prior outcome
//!   (`repaired: true`) instead of minting a duplicate behavior.
//!
//! See `crate::agent::persona_presets` for why a "preset" name alone
//! under-provisions a `ToolSelection` — this module is the place that closes
//! that gap by copying the init-parity extras verbatim from
//! `gents-cli`'s `init.rs`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use defra_node::EmbeddedNode;

use super::persona_presets;
use crate::config_client::{
    write_agent_behavior_document, write_tool_selection_document,
    write_tool_selection_document_with_clear_fields, ConfigAccess,
};
use crate::{
    load_agent_behavior, load_tool_selection, AgentBehaviorDocument, ToolSelectionDocument,
};

/// The published options and this agent's current behaviors — everything
/// [`decide_persona_request`] validates a request against. Callers (the
/// reconciler, the self-config tool, the CLI) assemble this once per
/// decision from the directory-catalog projection and this agent's
/// `AgentBehavior` rows; this module does not load it itself.
#[derive(Debug, Clone, Default)]
pub struct PersonaCatalogView {
    /// `"backend_id|model_name"` entries published for this deployment.
    pub available_models: BTreeSet<String>,
    /// `WorkspaceRoot` values a persona is allowed to be scoped to. An empty
    /// requested root is always fine regardless of this set (it means "no
    /// root restriction"), so this set only gates *non-empty* requests.
    pub allowed_roots: BTreeSet<String>,
    /// Inference profile ids published for this deployment.
    pub available_profile_ids: BTreeSet<String>,
    /// Enabled `AgentPrincipal` DIDs on this deployment. Every op requires
    /// the request's `agent_did` to be in this set (Lean `agentOk`): a
    /// paired device cannot mint orphan behaviors/selections for a phantom
    /// or foreign agent.
    pub known_agent_dids: BTreeSet<String>,
    /// This agent's own `AgentBehavior` rows, keyed by `behavior_id`.
    pub behaviors: BTreeMap<String, BehaviorRef>,
}

/// The slice of an `AgentBehavior` row admission and apply need: whether it
/// is a legal `clone_from`/edit/disable target, and (for create) which
/// `ToolSelection` it currently points at (used for the repair scan).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BehaviorRef {
    pub enabled: bool,
    pub tool_selection_id: String,
}

/// The requested operation, with `clone_from` folded in for `create` (the
/// only op it applies to).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersonaOp {
    Create { clone_from: Option<String> },
    Edit,
    Disable,
}

impl PersonaOp {
    /// Parse a `PersonaConfigRequest.op` column value. `None` means the
    /// request is malformed and [`decide_persona_request`] must reject it —
    /// callers building a [`PersonaRequestDoc`] from a raw row should set
    /// both `op_raw` (for the rejection message) and this parsed result.
    pub fn parse(op_raw: &str, clone_from: Option<String>) -> Option<Self> {
        match op_raw {
            "create" => Some(PersonaOp::Create { clone_from }),
            "edit" => Some(PersonaOp::Edit),
            "disable" => Some(PersonaOp::Disable),
            _ => None,
        }
    }
}

/// A typed `PersonaConfigRequest` row. `op_raw` is kept alongside the parsed
/// `op` so a bad op string survives into the rejection message instead of
/// being discarded during parsing.
#[derive(Debug, Clone, Default)]
pub struct PersonaRequestDoc {
    pub request_key: String,
    pub requester_did: String,
    pub agent_did: String,
    pub op_raw: String,
    pub op: Option<PersonaOp>,
    pub behavior_id: Option<String>,
    pub persona_name: Option<String>,
    pub backend_model: Option<String>,
    pub root: Option<String>,
    pub preset: Option<String>,
    pub profile_id: Option<String>,
    pub created_at: Option<String>,
    pub status: Option<String>,
    pub status_detail: Option<String>,
    pub applied_behavior_id: Option<String>,
    pub processed_at: Option<String>,
}

/// The result of admission. `Reject`'s message is user-facing: it becomes
/// `PersonaConfigRequest.status_detail` and surfaces verbatim in CLI error
/// output, so every rejection names the offending value and where the valid
/// options came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersonaVerdict {
    Admit,
    Reject(String),
}

const PERSONA_NAME_MAX_LEN: usize = 64;

fn validate_persona_name(name: Option<&str>) -> Option<String> {
    let name = name.unwrap_or("");
    let len = name.chars().count();
    if len == 0 || len > PERSONA_NAME_MAX_LEN {
        return Some(format!(
            r#"persona_name "{name}" must be 1-{PERSONA_NAME_MAX_LEN} characters (got {len})"#
        ));
    }
    None
}

/// The most entries an enumerated rejection lists before summarizing the
/// remainder — enough to usually settle the question in one round trip
/// without dumping an unbounded catalog into `status_detail` (which renders
/// on phone cards and CLI errors, and must stay one line).
const ENUMERATION_LIMIT: usize = 10;

/// Render a catalog set as `[a, b, c]`, bounded to [`ENUMERATION_LIMIT`]
/// entries with `… and N more` appended when there are more. Iterates a
/// `BTreeSet`, so the shown entries (and thus the message) are deterministic.
fn enumerate_behavior_ids(behaviors: &BTreeMap<String, BehaviorRef>) -> String {
    let ids: BTreeSet<String> = behaviors.keys().cloned().collect();
    enumerate_bounded(&ids)
}

fn enumerate_bounded(values: &BTreeSet<String>) -> String {
    let total = values.len();
    let shown: Vec<&str> = values
        .iter()
        .take(ENUMERATION_LIMIT)
        .map(String::as_str)
        .collect();
    if total > ENUMERATION_LIMIT {
        format!(
            "[{}] … and {} more",
            shown.join(", "),
            total - ENUMERATION_LIMIT
        )
    } else {
        format!("[{}]", shown.join(", "))
    }
}

fn validate_backend_model(value: Option<&str>, catalog: &PersonaCatalogView) -> Option<String> {
    let value = value.unwrap_or("");
    if !catalog.available_models.contains(value) {
        return Some(format!(
            r#"unknown model "{value}" — pick from the published available_models: {}"#,
            enumerate_bounded(&catalog.available_models)
        ));
    }
    None
}

fn validate_root(root: Option<&str>, catalog: &PersonaCatalogView) -> Option<String> {
    let root = root.unwrap_or("").trim();
    if root.is_empty() {
        return None;
    }
    if !catalog.allowed_roots.contains(root) {
        return Some(format!(
            r#"root "{root}" is not allowed — pick from the published allowed_roots: {}"#,
            enumerate_bounded(&catalog.allowed_roots)
        ));
    }
    None
}

fn validate_profile(profile_id: Option<&str>, catalog: &PersonaCatalogView) -> Option<String> {
    let profile_id = profile_id.unwrap_or("");
    if !catalog.available_profile_ids.contains(profile_id) {
        return Some(format!(
            r#"unknown profile "{profile_id}" — pick from the published available_profile_ids: {}"#,
            enumerate_bounded(&catalog.available_profile_ids)
        ));
    }
    None
}

fn validate_preset_name(preset: &str) -> Option<String> {
    if persona_presets::preset_fields(preset).is_none() {
        return Some(format!(
            r#"unknown preset "{preset}" — pick from {}"#,
            persona_presets::builtin_preset_names().join("|")
        ));
    }
    None
}

/// Pure admission gate. Every conjunct's rejection message names the
/// offending value and the source of truth for valid values, per the
/// contract in [`PersonaVerdict`].
pub fn decide_persona_request(
    doc: &PersonaRequestDoc,
    catalog: &PersonaCatalogView,
) -> PersonaVerdict {
    let Some(op) = doc.op.as_ref() else {
        return PersonaVerdict::Reject(format!(
            r#"unknown op "{}" — pick from create|edit|disable"#,
            doc.op_raw
        ));
    };

    // Lean `agentOk`: every op requires a known enabled principal, so a
    // request can never mint or touch config for a phantom/foreign agent.
    if !catalog.known_agent_dids.contains(&doc.agent_did) {
        return PersonaVerdict::Reject(format!(
            r#"unknown agent_did "{}" — no enabled AgentPrincipal with this DID on this deployment"#,
            doc.agent_did
        ));
    }

    match op {
        PersonaOp::Create { clone_from } => {
            if let Some(msg) = validate_persona_name(doc.persona_name.as_deref()) {
                return PersonaVerdict::Reject(msg);
            }
            if let Some(msg) = validate_backend_model(doc.backend_model.as_deref(), catalog) {
                return PersonaVerdict::Reject(msg);
            }
            if let Some(msg) = validate_root(doc.root.as_deref(), catalog) {
                return PersonaVerdict::Reject(msg);
            }
            if let Some(msg) = validate_profile(doc.profile_id.as_deref(), catalog) {
                return PersonaVerdict::Reject(msg);
            }

            let preset = doc.preset.as_deref().unwrap_or("").trim();
            match clone_from {
                Some(source_id) => {
                    if !preset.is_empty() {
                        return PersonaVerdict::Reject(format!(
                            r#"create with clone_from must not also set preset "{preset}" — omit preset when cloning"#
                        ));
                    }
                    match catalog.behaviors.get(source_id) {
                        None => {
                            return PersonaVerdict::Reject(format!(
                                r#"unknown clone_from "{source_id}" — pick from this agent's behaviors: {}"#,
                                enumerate_behavior_ids(&catalog.behaviors)
                            ));
                        }
                        Some(source) if !source.enabled => {
                            return PersonaVerdict::Reject(format!(
                                r#"clone_from "{source_id}" is disabled — pick an enabled behavior_id"#
                            ));
                        }
                        // Without this conjunct an admitted clone would fail
                        // in `apply_create` (nothing to copy) and the row
                        // would retry as pending on every sweep, forever.
                        Some(source) if source.tool_selection_id.trim().is_empty() => {
                            return PersonaVerdict::Reject(format!(
                                r#"clone_from "{source_id}" has no tool selection to copy — pick a behavior with a tool_selection_id, or create with a preset instead"#
                            ));
                        }
                        Some(_) => {}
                    }
                }
                None => {
                    if preset.is_empty() {
                        return PersonaVerdict::Reject(format!(
                            r#"unknown preset "" — pick from {}"#,
                            persona_presets::builtin_preset_names().join("|")
                        ));
                    }
                    if let Some(msg) = validate_preset_name(preset) {
                        return PersonaVerdict::Reject(msg);
                    }
                }
            }
            PersonaVerdict::Admit
        }
        PersonaOp::Edit => {
            let behavior_id = doc.behavior_id.as_deref().unwrap_or("");
            let Some(target) = catalog.behaviors.get(behavior_id) else {
                return PersonaVerdict::Reject(format!(
                    r#"unknown behavior_id "{behavior_id}" — pick from this agent's behaviors: {}"#,
                    enumerate_behavior_ids(&catalog.behaviors)
                ));
            };
            if let Some(msg) = validate_persona_name(doc.persona_name.as_deref()) {
                return PersonaVerdict::Reject(msg);
            }
            if let Some(msg) = validate_backend_model(doc.backend_model.as_deref(), catalog) {
                return PersonaVerdict::Reject(msg);
            }
            if let Some(msg) = validate_root(doc.root.as_deref(), catalog) {
                return PersonaVerdict::Reject(msg);
            }
            if let Some(msg) = validate_profile(doc.profile_id.as_deref(), catalog) {
                return PersonaVerdict::Reject(msg);
            }
            let preset = doc.preset.as_deref().unwrap_or("").trim();
            if !preset.is_empty() {
                if let Some(msg) = validate_preset_name(preset) {
                    return PersonaVerdict::Reject(msg);
                }
            } else if target.tool_selection_id.trim().is_empty() {
                // An empty preset means "keep the current selection", but this
                // behavior has none to keep — an admitted edit would fail in
                // `apply_edit` and the row would retry as pending on every
                // sweep, forever. Rejecting names the remedy instead.
                return PersonaVerdict::Reject(format!(
                    r#"behavior "{behavior_id}" has no tool selection to keep — name a preset to mint one"#
                ));
            }
            PersonaVerdict::Admit
        }
        PersonaOp::Disable => {
            let behavior_id = doc.behavior_id.as_deref().unwrap_or("");
            if !catalog.behaviors.contains_key(behavior_id) {
                return PersonaVerdict::Reject(format!(
                    r#"unknown behavior_id "{behavior_id}" — pick from this agent's behaviors: {}"#,
                    enumerate_behavior_ids(&catalog.behaviors)
                ));
            }
            PersonaVerdict::Admit
        }
    }
}

fn slugify(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut pending_sep = false;
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            if pending_sep && !out.is_empty() {
                out.push('-');
            }
            out.push(ch.to_ascii_lowercase());
            pending_sep = false;
        } else {
            pending_sep = true;
        }
    }
    out
}

/// Derive a globally-unique `AgentBehavior.behavior_id` from a persona name:
/// `{agent_did}:{slug}`, with `-2`, `-3`, … appended on collision against
/// `existing` (this agent's current behaviors). The `agent_did` prefix keeps
/// ids globally unique across agents without needing to scan the whole
/// collection; collision detection only needs to consider this agent's own
/// behaviors.
///
/// Callers must run the repair scan (does a behavior already exist with
/// `tool_selection_id == sel-{request_key}`?) BEFORE calling this — deriving
/// an id on every retry of an already-applied create would see its own prior
/// output in `existing` and mint a `-2` duplicate instead of recognizing the
/// repair.
pub fn derive_behavior_id(
    agent_did: &str,
    persona_name: &str,
    existing: &BTreeMap<String, BehaviorRef>,
) -> String {
    let slug = slugify(persona_name);
    let base = format!("{agent_did}:{slug}");
    if !existing.contains_key(&base) {
        return base;
    }
    let mut n = 2;
    loop {
        let candidate = format!("{agent_did}:{slug}-{n}");
        if !existing.contains_key(&candidate) {
            return candidate;
        }
        n += 1;
    }
}

/// The init-parity extras a preset name implies, beyond what
/// [`persona_presets::PresetFields`] classifies on — see `persona_presets`'
/// module doc for why these are excluded there. Copied verbatim from
/// `gents-cli/src/commands/init.rs`'s `tool_package_profile` Readonly/Write
/// arms, `default_command_execution_policy_for_init`, and
/// `default_backgroundable_tool_names`.
struct InitParityExtras {
    command_execution_policy: Option<String>,
    backgroundable_tool_names: Vec<String>,
    enable_meta_tools: bool,
    enable_defra_query: bool,
}

fn init_parity_extras(preset: &str) -> InitParityExtras {
    match preset {
        persona_presets::PRESET_WRITE => InitParityExtras {
            // init.rs's `default_command_execution_policy_for_init`, Write arm.
            command_execution_policy: if cfg!(target_os = "macos") {
                Some("workspace_write".to_string())
            } else {
                Some("unrestricted".to_string())
            },
            // init.rs's `default_backgroundable_tool_names`, Write arm.
            backgroundable_tool_names: vec!["bash_unrestricted".to_string()],
            enable_meta_tools: true,
            enable_defra_query: false,
        },
        // Readonly (and any other builtin, defensively): init.rs's Readonly arm.
        _ => InitParityExtras {
            command_execution_policy: None,
            backgroundable_tool_names: Vec::new(),
            enable_meta_tools: true,
            enable_defra_query: false,
        },
    }
}

/// Mint a full `ToolSelectionDocument` from a validated builtin preset name
/// (admission must have already confirmed `preset` resolves via
/// [`persona_presets::preset_fields`]). Every field outside
/// `selection_id`/`display_name`/`file_tool_root` mirrors
/// `gents-cli`'s `tool_selection_for_package` bit-for-bit — see the
/// INIT-PARITY unit test below.
fn tool_selection_from_preset(
    selection_id: String,
    agent_did: &str,
    display_name: String,
    preset: &str,
    root: &str,
) -> ToolSelectionDocument {
    let fields = persona_presets::preset_fields(preset)
        .expect("preset must be validated by admission before materializing");
    let extras = init_parity_extras(preset);
    let file_tool_root = if root.trim().is_empty() {
        None
    } else {
        Some(root.to_string())
    };
    ToolSelectionDocument {
        selection_id,
        agent_did: agent_did.to_string(),
        display_name: Some(display_name),
        tool_policy_version: None,
        enable_file_tools: Some(fields.enable_file_tools),
        file_tools_mode: Some(fields.file_tools_mode),
        file_tool_root,
        enable_bash: Some(fields.enable_bash),
        bash_mode: Some(fields.bash_mode),
        command_execution_policy: extras.command_execution_policy,
        command_allowed_argv_prefixes: Some(fields.command_allowed_argv_prefixes),
        command_forbidden_argv_prefixes: Some(fields.command_forbidden_argv_prefixes),
        read_only_command_allowlist: Some(fields.read_only_command_allowlist),
        command_network_mode: None,
        cli_tool_names: Some(Vec::new()),
        enable_meta_tools: Some(extras.enable_meta_tools),
        allowed_mcp_service_ids: Some(Vec::new()),
        backgroundable_tool_names: Some(extras.backgroundable_tool_names),
        approval_required_tools: None,
        subagent_targets: Some(Vec::new()),
        subagent_spawn_enabled: Some(false),
        subagent_steering_enabled: Some(false),
        subagent_background_enabled: Some(false),
        subagent_default_await_mode: Some("foreground".to_string()),
        subagent_allow_cross_deployment: Some(false),
        cross_deployment_spawn_timeout_seconds: None,
        enable_memory: Some(false),
        enable_session_history_tool: Some(false),
        enable_context_budget: Some(true),
        enable_defra_query: Some(extras.enable_defra_query),
        defra_query_collections: Some(Vec::new()),
        write_tools: None,
        datastore_tool_surface_ids: None,
        enable_self_config: None,
        self_config_categories: None,
        self_config_no_lockout: None,
        self_config_dry_run: None,
        enable_lsp: None,
        lsp_config: None,
    }
}

fn split_backend_model(value: &str) -> Result<(String, String)> {
    value
        .split_once('|')
        .map(|(backend, model)| (backend.to_string(), model.to_string()))
        .with_context(|| {
            format!(
                "backend_model {value:?} missing '|' separator (admission guaranteed valid format)"
            )
        })
}

fn find_repaired_behavior_id(catalog: &PersonaCatalogView, selection_id: &str) -> Option<String> {
    catalog
        .behaviors
        .iter()
        .find(|(_, reference)| reference.tool_selection_id == selection_id)
        .map(|(behavior_id, _)| behavior_id.clone())
}

/// The outcome of a successful apply. `repaired: true` means the request was
/// already applied (a prior attempt's `AgentBehavior` was found pointing at
/// this request's `sel-{request_key}` selection) and nothing was written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonaApplyOutcome {
    pub behavior_id: String,
    pub repaired: bool,
}

/// Materialize an ADMITTED [`PersonaRequestDoc`]. Callers must run
/// [`decide_persona_request`] first and only call this on `Admit` — every
/// `.context("... admission guaranteed")` below documents a precondition
/// admission establishes rather than a check this function repeats.
pub async fn apply_persona_request(
    node: &Arc<EmbeddedNode>,
    doc: &PersonaRequestDoc,
    catalog: &PersonaCatalogView,
) -> Result<PersonaApplyOutcome> {
    let access = ConfigAccess::Local(node.clone());
    let op = doc
        .op
        .as_ref()
        .context("apply_persona_request requires an admitted request (op must parse)")?;

    match op {
        PersonaOp::Create { clone_from } => {
            apply_create(node, &access, doc, catalog, clone_from.as_deref()).await
        }
        PersonaOp::Edit => apply_edit(node, &access, doc).await,
        PersonaOp::Disable => apply_disable(node, &access, doc).await,
    }
}

async fn apply_create(
    node: &Arc<EmbeddedNode>,
    access: &ConfigAccess,
    doc: &PersonaRequestDoc,
    catalog: &PersonaCatalogView,
    clone_from: Option<&str>,
) -> Result<PersonaApplyOutcome> {
    let selection_id = format!("sel-{}", doc.request_key);

    if let Some(existing_id) = find_repaired_behavior_id(catalog, &selection_id) {
        return Ok(PersonaApplyOutcome {
            behavior_id: existing_id,
            repaired: true,
        });
    }

    let persona_name = doc
        .persona_name
        .as_deref()
        .context("persona_name is required for create (admission guaranteed)")?;
    let root = doc.root.as_deref().unwrap_or("");
    let display_name = format!("{persona_name} tools");

    match clone_from {
        Some(source_id) => {
            let source_selection_id = catalog
                .behaviors
                .get(source_id)
                .map(|reference| reference.tool_selection_id.clone())
                .context("clone_from behavior missing from catalog (admission guaranteed)")?;
            let source_selection = load_tool_selection(node, &source_selection_id)
                .await?
                .context("clone_from's ToolSelection missing (admission guaranteed)")?;
            let mut cloned = source_selection;
            cloned.selection_id = selection_id.clone();
            if !root.trim().is_empty() {
                cloned.file_tool_root = Some(root.to_string());
            }
            write_tool_selection_document(access, &cloned).await?;
        }
        None => {
            let preset = doc.preset.as_deref().context(
                "preset is required for create without clone_from (admission guaranteed)",
            )?;
            let selection = tool_selection_from_preset(
                selection_id.clone(),
                &doc.agent_did,
                display_name,
                preset,
                root,
            );
            write_tool_selection_document(access, &selection).await?;
        }
    }

    let behavior_id = derive_behavior_id(&doc.agent_did, persona_name, &catalog.behaviors);
    let (backend_id, model_name) = split_backend_model(doc.backend_model.as_deref().unwrap_or(""))?;
    let profile_id = doc
        .profile_id
        .as_deref()
        .context("profile_id is required for create (admission guaranteed)")?;

    let behavior = AgentBehaviorDocument {
        behavior_id: behavior_id.clone(),
        agent_did: doc.agent_did.clone(),
        display_name: Some(persona_name.to_string()),
        description: None,
        summary: None,
        system_prompt: None,
        request_context_template: None,
        backend_id: Some(backend_id),
        model_name: Some(model_name),
        tool_selection_id: Some(selection_id),
        inference_profile_id: Some(profile_id.to_string()),
        compaction_strategy: None,
        compaction_threshold: None,
        enabled: true,
        skill_refs: Vec::new(),
        skill_excludes: Vec::new(),
        created_at: None,
    };
    write_agent_behavior_document(access, &behavior).await?;

    Ok(PersonaApplyOutcome {
        behavior_id,
        repaired: false,
    })
}

async fn apply_edit(
    node: &Arc<EmbeddedNode>,
    access: &ConfigAccess,
    doc: &PersonaRequestDoc,
) -> Result<PersonaApplyOutcome> {
    let behavior_id = doc
        .behavior_id
        .as_deref()
        .context("behavior_id is required for edit (admission guaranteed)")?;
    let mut behavior = load_agent_behavior(node, behavior_id)
        .await?
        .with_context(|| {
            format!("behavior {behavior_id:?} missing (admission guaranteed it exists)")
        })?;

    let persona_name = doc
        .persona_name
        .as_deref()
        .context("persona_name is required for edit (admission guaranteed)")?;
    let (backend_id, model_name) = split_backend_model(doc.backend_model.as_deref().unwrap_or(""))?;
    let profile_id = doc
        .profile_id
        .as_deref()
        .context("profile_id is required for edit (admission guaranteed)")?;
    let root = doc.root.as_deref().unwrap_or("");
    let preset = doc.preset.as_deref().unwrap_or("").trim();

    if preset.is_empty() {
        // Clients always resend the full current pick set: `root` is the
        // desired value verbatim, patched onto the existing (possibly
        // shared) selection only when it actually differs.
        let current_selection_id = behavior
            .tool_selection_id
            .clone()
            .context("behavior missing tool_selection_id during edit (admission guaranteed)")?;
        let mut selection = load_tool_selection(node, &current_selection_id)
            .await?
            .with_context(|| {
                format!("ToolSelection {current_selection_id:?} missing (admission guaranteed)")
            })?;
        let desired_root = if root.trim().is_empty() {
            None
        } else {
            Some(root.to_string())
        };
        if selection.file_tool_root != desired_root {
            selection.file_tool_root = desired_root.clone();
            if desired_root.is_none() {
                // The plain writer omits `None` fields from the update
                // clause (so `None` on other, untouched fields means
                // "preserve"), so clearing an existing root to empty needs
                // the explicit-null writer instead of a no-op omission.
                write_tool_selection_document_with_clear_fields(
                    access,
                    &selection,
                    &["file_tool_root"],
                )
                .await?;
            } else {
                write_tool_selection_document(access, &selection).await?;
            }
        }
    } else {
        // A named preset always mints a NEW selection — never mutate a
        // possibly-shared selection in place.
        let selection_id = format!("sel-{}", doc.request_key);
        let display_name = format!("{persona_name} tools");
        let selection = tool_selection_from_preset(
            selection_id.clone(),
            &doc.agent_did,
            display_name,
            preset,
            root,
        );
        write_tool_selection_document(access, &selection).await?;
        behavior.tool_selection_id = Some(selection_id);
    }

    behavior.display_name = Some(persona_name.to_string());
    behavior.backend_id = Some(backend_id);
    behavior.model_name = Some(model_name);
    behavior.inference_profile_id = Some(profile_id.to_string());

    write_agent_behavior_document(access, &behavior).await?;

    Ok(PersonaApplyOutcome {
        behavior_id: behavior_id.to_string(),
        repaired: false,
    })
}

async fn apply_disable(
    node: &Arc<EmbeddedNode>,
    access: &ConfigAccess,
    doc: &PersonaRequestDoc,
) -> Result<PersonaApplyOutcome> {
    let behavior_id = doc
        .behavior_id
        .as_deref()
        .context("behavior_id is required for disable (admission guaranteed)")?;
    let mut behavior = load_agent_behavior(node, behavior_id)
        .await?
        .with_context(|| {
            format!("behavior {behavior_id:?} missing (admission guaranteed it exists)")
        })?;
    behavior.enabled = false;
    write_agent_behavior_document(access, &behavior).await?;
    Ok(PersonaApplyOutcome {
        behavior_id: behavior_id.to_string(),
        repaired: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog_with(
        models: &[&str],
        roots: &[&str],
        profiles: &[&str],
        behaviors: &[(&str, bool, &str)],
    ) -> PersonaCatalogView {
        PersonaCatalogView {
            available_models: models.iter().map(|s| s.to_string()).collect(),
            allowed_roots: roots.iter().map(|s| s.to_string()).collect(),
            available_profile_ids: profiles.iter().map(|s| s.to_string()).collect(),
            known_agent_dids: BTreeSet::from(["did:key:agent".to_string()]),
            behaviors: behaviors
                .iter()
                .map(|(id, enabled, selection_id)| {
                    (
                        id.to_string(),
                        BehaviorRef {
                            enabled: *enabled,
                            tool_selection_id: selection_id.to_string(),
                        },
                    )
                })
                .collect(),
        }
    }

    fn base_catalog() -> PersonaCatalogView {
        catalog_with(
            &["openai|gpt-5"],
            &["/workspace/root"],
            &["profile-1"],
            &[
                ("existing-enabled", true, "sel-existing-enabled"),
                ("existing-disabled", false, "sel-existing-disabled"),
                ("existing-selectionless", true, ""),
            ],
        )
    }

    fn create_doc(op: PersonaOp) -> PersonaRequestDoc {
        PersonaRequestDoc {
            request_key: "req-1".to_string(),
            requester_did: "did:key:requester".to_string(),
            agent_did: "did:key:agent".to_string(),
            op_raw: "create".to_string(),
            op: Some(op),
            persona_name: Some("Research Assistant".to_string()),
            backend_model: Some("openai|gpt-5".to_string()),
            root: None,
            preset: Some(persona_presets::PRESET_WRITE.to_string()),
            profile_id: Some("profile-1".to_string()),
            ..Default::default()
        }
    }

    // -- decide_persona_request: one conjunct per test --

    #[test]
    fn rejects_bad_op() {
        let doc = PersonaRequestDoc {
            op_raw: "yeet".to_string(),
            op: None,
            ..Default::default()
        };
        let verdict = decide_persona_request(&doc, &base_catalog());
        assert_eq!(
            verdict,
            PersonaVerdict::Reject(
                r#"unknown op "yeet" — pick from create|edit|disable"#.to_string()
            )
        );
    }

    #[test]
    fn rejects_unknown_agent_did() {
        let mut doc = create_doc(PersonaOp::Create { clone_from: None });
        doc.agent_did = "did:key:phantom".to_string();
        let verdict = decide_persona_request(&doc, &base_catalog());
        assert_eq!(
            verdict,
            PersonaVerdict::Reject(
                r#"unknown agent_did "did:key:phantom" — no enabled AgentPrincipal with this DID on this deployment"#
                    .to_string()
            )
        );
    }

    #[test]
    fn rejects_unknown_model() {
        let doc = create_doc(PersonaOp::Create { clone_from: None });
        let mut doc = doc;
        doc.backend_model = Some("nope|nope".to_string());
        let verdict = decide_persona_request(&doc, &base_catalog());
        assert_eq!(
            verdict,
            PersonaVerdict::Reject(
                r#"unknown model "nope|nope" — pick from the published available_models: [openai|gpt-5]"#
                    .to_string()
            )
        );
    }

    /// Enumerated rejections bound the listed values to
    /// [`ENUMERATION_LIMIT`] and summarize the remainder — otherwise an
    /// unbounded catalog defeats the point (`status_detail` must stay one
    /// line) while still forcing a second round-trip past the bound.
    #[test]
    fn rejects_unknown_model_enumerates_bounded_list() {
        let mut cat = base_catalog();
        cat.available_models = (1..=13).map(|n| format!("backend|model-{n:02}")).collect();
        let mut doc = create_doc(PersonaOp::Create { clone_from: None });
        doc.backend_model = Some("nope|nope".to_string());
        let verdict = decide_persona_request(&doc, &cat);
        let expected_shown = (1..=10)
            .map(|n| format!("backend|model-{n:02}"))
            .collect::<Vec<_>>()
            .join(", ");
        assert_eq!(
            verdict,
            PersonaVerdict::Reject(format!(
                r#"unknown model "nope|nope" — pick from the published available_models: [{expected_shown}] … and 3 more"#
            ))
        );
    }

    #[test]
    fn rejects_root_not_allowed() {
        let mut doc = create_doc(PersonaOp::Create { clone_from: None });
        doc.root = Some("/not/allowed".to_string());
        let verdict = decide_persona_request(&doc, &base_catalog());
        assert_eq!(
            verdict,
            PersonaVerdict::Reject(
                r#"root "/not/allowed" is not allowed — pick from the published allowed_roots: [/workspace/root]"#
                    .to_string()
            )
        );
    }

    #[test]
    fn empty_root_is_admitted() {
        let mut doc = create_doc(PersonaOp::Create { clone_from: None });
        doc.root = Some("".to_string());
        let verdict = decide_persona_request(&doc, &base_catalog());
        assert_eq!(verdict, PersonaVerdict::Admit);
    }

    #[test]
    fn rejects_unknown_profile() {
        let mut doc = create_doc(PersonaOp::Create { clone_from: None });
        doc.profile_id = Some("no-such-profile".to_string());
        let verdict = decide_persona_request(&doc, &base_catalog());
        assert_eq!(
            verdict,
            PersonaVerdict::Reject(
                r#"unknown profile "no-such-profile" — pick from the published available_profile_ids: [profile-1]"#
                    .to_string()
            )
        );
    }

    #[test]
    fn rejects_empty_persona_name() {
        let mut doc = create_doc(PersonaOp::Create { clone_from: None });
        doc.persona_name = Some("".to_string());
        let verdict = decide_persona_request(&doc, &base_catalog());
        assert_eq!(
            verdict,
            PersonaVerdict::Reject(
                r#"persona_name "" must be 1-64 characters (got 0)"#.to_string()
            )
        );
    }

    #[test]
    fn rejects_65_char_persona_name() {
        let mut doc = create_doc(PersonaOp::Create { clone_from: None });
        let name = "a".repeat(65);
        doc.persona_name = Some(name.clone());
        let verdict = decide_persona_request(&doc, &base_catalog());
        assert_eq!(
            verdict,
            PersonaVerdict::Reject(format!(
                r#"persona_name "{name}" must be 1-64 characters (got 65)"#
            ))
        );
    }

    #[test]
    fn rejects_create_clone_with_named_preset() {
        let mut doc = create_doc(PersonaOp::Create {
            clone_from: Some("existing-enabled".to_string()),
        });
        doc.preset = Some(persona_presets::PRESET_WRITE.to_string());
        let verdict = decide_persona_request(&doc, &base_catalog());
        assert_eq!(
            verdict,
            PersonaVerdict::Reject(
                r#"create with clone_from must not also set preset "write" — omit preset when cloning"#
                    .to_string()
            )
        );
    }

    #[test]
    fn rejects_unknown_clone_from() {
        let mut doc = create_doc(PersonaOp::Create {
            clone_from: Some("no-such-behavior".to_string()),
        });
        doc.preset = None;
        let verdict = decide_persona_request(&doc, &base_catalog());
        assert_eq!(
            verdict,
            PersonaVerdict::Reject(
                r#"unknown clone_from "no-such-behavior" — pick from this agent's behaviors: [existing-disabled, existing-enabled, existing-selectionless]"#
                    .to_string()
            )
        );
    }

    #[test]
    fn rejects_disabled_clone_from() {
        let mut doc = create_doc(PersonaOp::Create {
            clone_from: Some("existing-disabled".to_string()),
        });
        doc.preset = None;
        let verdict = decide_persona_request(&doc, &base_catalog());
        assert_eq!(
            verdict,
            PersonaVerdict::Reject(
                r#"clone_from "existing-disabled" is disabled — pick an enabled behavior_id"#
                    .to_string()
            )
        );
    }

    #[test]
    fn rejects_clone_from_without_selection() {
        let mut doc = create_doc(PersonaOp::Create {
            clone_from: Some("existing-selectionless".to_string()),
        });
        doc.preset = None;
        let verdict = decide_persona_request(&doc, &base_catalog());
        assert_eq!(
            verdict,
            PersonaVerdict::Reject(
                r#"clone_from "existing-selectionless" has no tool selection to copy — pick a behavior with a tool_selection_id, or create with a preset instead"#
                    .to_string()
            )
        );
    }

    #[test]
    fn rejects_empty_preset_edit_of_selectionless_behavior() {
        let mut doc = create_doc(PersonaOp::Edit);
        doc.op_raw = "edit".to_string();
        doc.behavior_id = Some("existing-selectionless".to_string());
        doc.preset = None;
        let verdict = decide_persona_request(&doc, &base_catalog());
        assert_eq!(
            verdict,
            PersonaVerdict::Reject(
                r#"behavior "existing-selectionless" has no tool selection to keep — name a preset to mint one"#
                    .to_string()
            )
        );
    }

    /// The remedy the rejection above names must itself be admissible: a
    /// named-preset edit mints a fresh selection, so it needs no existing one.
    #[test]
    fn admits_preset_edit_of_selectionless_behavior() {
        let mut doc = create_doc(PersonaOp::Edit);
        doc.op_raw = "edit".to_string();
        doc.behavior_id = Some("existing-selectionless".to_string());
        doc.preset = Some(persona_presets::PRESET_READONLY.to_string());
        assert_eq!(
            decide_persona_request(&doc, &base_catalog()),
            PersonaVerdict::Admit
        );
    }

    #[test]
    fn rejects_edit_unknown_behavior_id() {
        let mut doc = create_doc(PersonaOp::Edit);
        doc.behavior_id = Some("no-such-behavior".to_string());
        let verdict = decide_persona_request(&doc, &base_catalog());
        assert_eq!(
            verdict,
            PersonaVerdict::Reject(
                r#"unknown behavior_id "no-such-behavior" — pick from this agent's behaviors: [existing-disabled, existing-enabled, existing-selectionless]"#
                    .to_string()
            )
        );
    }

    #[test]
    fn rejects_disable_unknown_behavior_id() {
        let doc = PersonaRequestDoc {
            agent_did: "did:key:agent".to_string(),
            op_raw: "disable".to_string(),
            op: Some(PersonaOp::Disable),
            behavior_id: Some("no-such-behavior".to_string()),
            ..Default::default()
        };
        let verdict = decide_persona_request(&doc, &base_catalog());
        assert_eq!(
            verdict,
            PersonaVerdict::Reject(
                r#"unknown behavior_id "no-such-behavior" — pick from this agent's behaviors: [existing-disabled, existing-enabled, existing-selectionless]"#
                    .to_string()
            )
        );
    }

    #[test]
    fn rejects_unknown_preset_on_plain_create() {
        let mut doc = create_doc(PersonaOp::Create { clone_from: None });
        doc.preset = Some("bogus".to_string());
        let verdict = decide_persona_request(&doc, &base_catalog());
        assert_eq!(
            verdict,
            PersonaVerdict::Reject(
                r#"unknown preset "bogus" — pick from readonly|write"#.to_string()
            )
        );
    }

    #[test]
    fn admits_happy_create() {
        let doc = create_doc(PersonaOp::Create { clone_from: None });
        assert_eq!(
            decide_persona_request(&doc, &base_catalog()),
            PersonaVerdict::Admit
        );
    }

    #[test]
    fn admits_happy_clone() {
        let mut doc = create_doc(PersonaOp::Create {
            clone_from: Some("existing-enabled".to_string()),
        });
        doc.preset = None;
        assert_eq!(
            decide_persona_request(&doc, &base_catalog()),
            PersonaVerdict::Admit
        );
    }

    #[test]
    fn admits_happy_edit() {
        let mut doc = create_doc(PersonaOp::Edit);
        doc.behavior_id = Some("existing-enabled".to_string());
        assert_eq!(
            decide_persona_request(&doc, &base_catalog()),
            PersonaVerdict::Admit
        );
    }

    #[test]
    fn admits_happy_disable() {
        let doc = PersonaRequestDoc {
            agent_did: "did:key:agent".to_string(),
            op_raw: "disable".to_string(),
            op: Some(PersonaOp::Disable),
            behavior_id: Some("existing-enabled".to_string()),
            ..Default::default()
        };
        assert_eq!(
            decide_persona_request(&doc, &base_catalog()),
            PersonaVerdict::Admit
        );
    }

    // -- derive_behavior_id --

    #[test]
    fn derives_slugged_id() {
        let id = derive_behavior_id("did:key:agent", "Research Assistant!!", &BTreeMap::new());
        assert_eq!(id, "did:key:agent:research-assistant");
    }

    #[test]
    fn derives_collision_suffix() {
        let mut existing = BTreeMap::new();
        existing.insert(
            "did:key:agent:research-assistant".to_string(),
            BehaviorRef {
                enabled: true,
                tool_selection_id: "sel-a".to_string(),
            },
        );
        let id = derive_behavior_id("did:key:agent", "Research Assistant", &existing);
        assert_eq!(id, "did:key:agent:research-assistant-2");

        existing.insert(
            "did:key:agent:research-assistant-2".to_string(),
            BehaviorRef {
                enabled: true,
                tool_selection_id: "sel-b".to_string(),
            },
        );
        let id = derive_behavior_id("did:key:agent", "Research Assistant", &existing);
        assert_eq!(id, "did:key:agent:research-assistant-3");
    }

    // -- INIT-PARITY RULE --

    /// A persona-minted `write` selection must equal an init-minted `write`
    /// selection field-for-field, excluding `selection_id`/`display_name`
    /// (never varied by init) and `file_tool_root` (init never sets it; the
    /// persona layer treats root as its own dimension). This is the literal
    /// contract `persona_presets`' module doc calls out as the gap `PresetFields`
    /// alone leaves open. The expected values below are transcribed verbatim
    /// from `gents-cli/src/commands/init.rs`'s `tool_package_profile` Write arm,
    /// `default_command_execution_policy_for_init`, and
    /// `default_backgroundable_tool_names` (see also
    /// `persona_presets::tests::write_mirrors_init_write_package`, which pins
    /// the `PresetFields` half of this same parity).
    #[test]
    fn write_preset_selection_matches_init_minted_write_selection_field_for_field() {
        let mut persona_minted = tool_selection_from_preset(
            "sel-req-1".to_string(),
            "did:key:agent",
            "Research Assistant tools".to_string(),
            persona_presets::PRESET_WRITE,
            "",
        );

        let mut init_minted = ToolSelectionDocument {
            selection_id: "tool-selection-init".to_string(),
            agent_did: "did:key:agent".to_string(),
            display_name: Some("Standard Write Tools".to_string()),
            tool_policy_version: None,
            enable_file_tools: Some(true),
            file_tools_mode: Some("ReadWrite".to_string()),
            file_tool_root: None,
            enable_bash: Some(true),
            bash_mode: Some("Unrestricted".to_string()),
            command_execution_policy: if cfg!(target_os = "macos") {
                Some("workspace_write".to_string())
            } else {
                Some("unrestricted".to_string())
            },
            command_allowed_argv_prefixes: Some(Vec::new()),
            command_forbidden_argv_prefixes: Some(Vec::new()),
            read_only_command_allowlist: Some(Vec::new()),
            command_network_mode: None,
            cli_tool_names: Some(Vec::new()),
            enable_meta_tools: Some(true),
            allowed_mcp_service_ids: Some(Vec::new()),
            backgroundable_tool_names: Some(vec!["bash_unrestricted".to_string()]),
            approval_required_tools: None,
            subagent_targets: Some(Vec::new()),
            subagent_spawn_enabled: Some(false),
            subagent_steering_enabled: Some(false),
            subagent_background_enabled: Some(false),
            subagent_default_await_mode: Some("foreground".to_string()),
            subagent_allow_cross_deployment: Some(false),
            cross_deployment_spawn_timeout_seconds: None,
            enable_memory: Some(false),
            enable_session_history_tool: Some(false),
            enable_context_budget: Some(true),
            enable_defra_query: Some(false),
            defra_query_collections: Some(Vec::new()),
            write_tools: None,
            datastore_tool_surface_ids: None,
            enable_self_config: None,
            self_config_categories: None,
            self_config_no_lockout: None,
            self_config_dry_run: None,
            enable_lsp: None,
            lsp_config: None,
        };

        // Fields the two channels are explicitly allowed to differ on.
        persona_minted.selection_id = "same".to_string();
        init_minted.selection_id = "same".to_string();
        persona_minted.display_name = None;
        init_minted.display_name = None;
        persona_minted.file_tool_root = None;
        init_minted.file_tool_root = None;

        assert_eq!(persona_minted, init_minted);
    }

    // -- apply_persona_request: embedded-node tests --

    async fn build_node(tempdir: &tempfile::TempDir) -> Arc<EmbeddedNode> {
        let node = EmbeddedNode::builder()
            .data_path(tempdir.path().join("data"))
            .build()
            .await
            .expect("embedded node boots");
        crate::ensure_runtime_schemas(&node)
            .await
            .expect("runtime schemas register");
        Arc::new(node)
    }

    #[tokio::test]
    async fn create_materializes_behavior_and_selection_never_null() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let node = build_node(&tempdir).await;

        let doc = PersonaRequestDoc {
            request_key: "req-create-1".to_string(),
            agent_did: "did:key:create-agent".to_string(),
            op_raw: "create".to_string(),
            op: Some(PersonaOp::Create { clone_from: None }),
            persona_name: Some("Research Assistant".to_string()),
            backend_model: Some("openai|gpt-5".to_string()),
            root: Some("".to_string()),
            preset: Some(persona_presets::PRESET_WRITE.to_string()),
            profile_id: Some("profile-1".to_string()),
            ..Default::default()
        };
        let catalog = PersonaCatalogView::default();

        let outcome = apply_persona_request(&node, &doc, &catalog).await?;
        assert!(!outcome.repaired);
        assert_eq!(
            outcome.behavior_id,
            "did:key:create-agent:research-assistant"
        );

        let behavior = load_agent_behavior(&node, &outcome.behavior_id)
            .await?
            .expect("behavior created");
        assert_eq!(
            behavior.display_name,
            Some("Research Assistant".to_string())
        );
        assert_eq!(behavior.backend_id, Some("openai".to_string()));
        assert_eq!(behavior.model_name, Some("gpt-5".to_string()));
        assert_eq!(
            behavior.tool_selection_id,
            Some("sel-req-create-1".to_string())
        );
        assert_eq!(
            behavior.inference_profile_id,
            Some("profile-1".to_string()),
            "inference profile must be stamped, never null"
        );
        assert!(behavior.enabled);

        let selection = load_tool_selection(&node, "sel-req-create-1")
            .await?
            .expect("selection created");
        assert_eq!(selection.enable_bash, Some(true));
        assert_eq!(selection.bash_mode, Some("Unrestricted".to_string()));
        assert_eq!(selection.enable_file_tools, Some(true));
        assert_eq!(selection.file_tools_mode, Some("ReadWrite".to_string()));
        assert_eq!(selection.file_tool_root, None);
        assert_eq!(selection.enable_meta_tools, Some(true));
        assert_eq!(selection.enable_defra_query, Some(false));
        assert_eq!(
            selection.backgroundable_tool_names,
            Some(vec!["bash_unrestricted".to_string()])
        );
        let expected_policy = if cfg!(target_os = "macos") {
            Some("workspace_write".to_string())
        } else {
            Some("unrestricted".to_string())
        };
        assert_eq!(selection.command_execution_policy, expected_policy);
        Ok(())
    }

    #[tokio::test]
    async fn create_repair_short_circuits_without_duplicate() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let node = build_node(&tempdir).await;

        let doc = PersonaRequestDoc {
            request_key: "req-repair-1".to_string(),
            agent_did: "did:key:repair-agent".to_string(),
            op_raw: "create".to_string(),
            op: Some(PersonaOp::Create { clone_from: None }),
            persona_name: Some("Repair Persona".to_string()),
            backend_model: Some("openai|gpt-5".to_string()),
            root: None,
            preset: Some(persona_presets::PRESET_READONLY.to_string()),
            profile_id: Some("profile-1".to_string()),
            ..Default::default()
        };
        let empty_catalog = PersonaCatalogView::default();

        let first = apply_persona_request(&node, &doc, &empty_catalog).await?;
        assert!(!first.repaired);

        // Reload a fresh catalog reflecting the just-created behavior, as a
        // reconciler retry would.
        let behaviors = crate::list_agent_behaviors(&node, &doc.agent_did).await?;
        let mut catalog_after = PersonaCatalogView::default();
        for behavior in &behaviors {
            catalog_after.behaviors.insert(
                behavior.behavior_id.clone(),
                BehaviorRef {
                    enabled: behavior.enabled,
                    tool_selection_id: behavior.tool_selection_id.clone().unwrap_or_default(),
                },
            );
        }

        let second = apply_persona_request(&node, &doc, &catalog_after).await?;
        assert!(second.repaired);
        assert_eq!(second.behavior_id, first.behavior_id);

        let behaviors_after = crate::list_agent_behaviors(&node, &doc.agent_did).await?;
        assert_eq!(
            behaviors_after.len(),
            1,
            "repair must not mint a duplicate behavior"
        );
        Ok(())
    }

    #[tokio::test]
    async fn clone_copies_fields_except_root_rule() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let node = build_node(&tempdir).await;
        let access = ConfigAccess::Local(node.clone());

        let source_selection = ToolSelectionDocument {
            selection_id: "sel-source".to_string(),
            agent_did: "did:key:clone-agent".to_string(),
            display_name: Some("Source tools".to_string()),
            enable_file_tools: Some(true),
            file_tools_mode: Some("ReadOnly".to_string()),
            file_tool_root: Some("/src/root".to_string()),
            enable_bash: Some(true),
            bash_mode: Some("ReadOnly".to_string()),
            enable_meta_tools: Some(true),
            enable_memory: Some(true),
            ..Default::default()
        };
        write_tool_selection_document(&access, &source_selection).await?;
        let source_behavior = AgentBehaviorDocument {
            behavior_id: "source-behavior".to_string(),
            agent_did: "did:key:clone-agent".to_string(),
            display_name: Some("Source Persona".to_string()),
            description: None,
            summary: None,
            system_prompt: None,
            request_context_template: None,
            backend_id: None,
            model_name: None,
            tool_selection_id: Some("sel-source".to_string()),
            inference_profile_id: None,
            compaction_strategy: None,
            compaction_threshold: None,
            enabled: true,
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            created_at: None,
        };
        write_agent_behavior_document(&access, &source_behavior).await?;

        let mut catalog = PersonaCatalogView::default();
        catalog.behaviors.insert(
            "source-behavior".to_string(),
            BehaviorRef {
                enabled: true,
                tool_selection_id: "sel-source".to_string(),
            },
        );

        let doc = PersonaRequestDoc {
            request_key: "req-clone-1".to_string(),
            agent_did: "did:key:clone-agent".to_string(),
            op_raw: "create".to_string(),
            op: Some(PersonaOp::Create {
                clone_from: Some("source-behavior".to_string()),
            }),
            persona_name: Some("Cloned Persona".to_string()),
            backend_model: Some("openai|gpt-5".to_string()),
            root: Some("/new/root".to_string()),
            preset: None,
            profile_id: Some("profile-1".to_string()),
            ..Default::default()
        };

        let outcome = apply_persona_request(&node, &doc, &catalog).await?;
        assert!(!outcome.repaired);

        let cloned = load_tool_selection(&node, "sel-req-clone-1")
            .await?
            .expect("cloned selection exists");
        let reloaded_source = load_tool_selection(&node, "sel-source")
            .await?
            .expect("source selection untouched");

        assert_ne!(cloned.selection_id, reloaded_source.selection_id);
        // Root rule: request supplied a non-empty root, so it overrides.
        assert_eq!(cloned.file_tool_root, Some("/new/root".to_string()));
        assert_eq!(
            reloaded_source.file_tool_root,
            Some("/src/root".to_string()),
            "source untouched"
        );
        // Every other field copied field-for-field.
        let mut cloned_norm = cloned.clone();
        let mut source_norm = reloaded_source.clone();
        cloned_norm.selection_id = "same".to_string();
        source_norm.selection_id = "same".to_string();
        cloned_norm.file_tool_root = None;
        source_norm.file_tool_root = None;
        assert_eq!(cloned_norm, source_norm);

        Ok(())
    }

    #[tokio::test]
    async fn edit_named_preset_mints_new_selection_and_repoints() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let node = build_node(&tempdir).await;
        let access = ConfigAccess::Local(node.clone());

        let old_selection = ToolSelectionDocument {
            selection_id: "sel-old".to_string(),
            agent_did: "did:key:edit-agent".to_string(),
            enable_file_tools: Some(true),
            file_tools_mode: Some("ReadWrite".to_string()),
            enable_bash: Some(true),
            bash_mode: Some("Unrestricted".to_string()),
            ..Default::default()
        };
        write_tool_selection_document(&access, &old_selection).await?;
        let old_behavior = AgentBehaviorDocument {
            behavior_id: "existing-behavior".to_string(),
            agent_did: "did:key:edit-agent".to_string(),
            display_name: Some("Old Name".to_string()),
            description: Some("kept as-is".to_string()),
            summary: None,
            system_prompt: None,
            request_context_template: None,
            backend_id: Some("openai".to_string()),
            model_name: Some("gpt-5".to_string()),
            tool_selection_id: Some("sel-old".to_string()),
            inference_profile_id: Some("profile-1".to_string()),
            compaction_strategy: None,
            compaction_threshold: None,
            enabled: true,
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            created_at: None,
        };
        write_agent_behavior_document(&access, &old_behavior).await?;

        let doc = PersonaRequestDoc {
            request_key: "req-edit-1".to_string(),
            agent_did: "did:key:edit-agent".to_string(),
            op_raw: "edit".to_string(),
            op: Some(PersonaOp::Edit),
            behavior_id: Some("existing-behavior".to_string()),
            persona_name: Some("Renamed".to_string()),
            backend_model: Some("anthropic|claude".to_string()),
            root: None,
            preset: Some(persona_presets::PRESET_READONLY.to_string()),
            profile_id: Some("profile-2".to_string()),
            ..Default::default()
        };

        let outcome = apply_persona_request(&node, &doc, &PersonaCatalogView::default()).await?;
        assert!(!outcome.repaired);
        assert_eq!(outcome.behavior_id, "existing-behavior");

        let behavior = load_agent_behavior(&node, "existing-behavior")
            .await?
            .expect("behavior still exists");
        assert_eq!(behavior.display_name, Some("Renamed".to_string()));
        assert_eq!(behavior.backend_id, Some("anthropic".to_string()));
        assert_eq!(behavior.model_name, Some("claude".to_string()));
        assert_eq!(behavior.inference_profile_id, Some("profile-2".to_string()));
        assert_eq!(
            behavior.tool_selection_id,
            Some("sel-req-edit-1".to_string()),
            "behavior must repoint to the newly minted selection"
        );
        assert!(behavior.enabled, "enabled must be preserved across edit");
        assert_eq!(
            behavior.description,
            Some("kept as-is".to_string()),
            "untouched fields preserved"
        );

        let new_selection = load_tool_selection(&node, "sel-req-edit-1")
            .await?
            .expect("new selection minted");
        assert_eq!(new_selection.enable_bash, Some(true));
        assert_eq!(new_selection.bash_mode, Some("ReadOnly".to_string()));

        let old_selection_after = load_tool_selection(&node, "sel-old")
            .await?
            .expect("old selection intact");
        assert_eq!(
            old_selection_after.bash_mode,
            Some("Unrestricted".to_string()),
            "old selection must not be mutated in place"
        );

        Ok(())
    }

    #[tokio::test]
    async fn edit_keep_current_patches_root_only() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let node = build_node(&tempdir).await;
        let access = ConfigAccess::Local(node.clone());

        let selection = ToolSelectionDocument {
            selection_id: "sel-keep".to_string(),
            agent_did: "did:key:keep-agent".to_string(),
            enable_file_tools: Some(true),
            file_tools_mode: Some("ReadOnly".to_string()),
            enable_bash: Some(true),
            bash_mode: Some("ReadOnly".to_string()),
            enable_memory: Some(true),
            file_tool_root: None,
            ..Default::default()
        };
        write_tool_selection_document(&access, &selection).await?;
        let behavior = AgentBehaviorDocument {
            behavior_id: "keep-behavior".to_string(),
            agent_did: "did:key:keep-agent".to_string(),
            display_name: Some("Keep Persona".to_string()),
            description: None,
            summary: None,
            system_prompt: None,
            request_context_template: None,
            backend_id: Some("openai".to_string()),
            model_name: Some("gpt-5".to_string()),
            tool_selection_id: Some("sel-keep".to_string()),
            inference_profile_id: Some("profile-1".to_string()),
            compaction_strategy: None,
            compaction_threshold: None,
            enabled: true,
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            created_at: None,
        };
        write_agent_behavior_document(&access, &behavior).await?;

        let doc = PersonaRequestDoc {
            request_key: "req-keep-1".to_string(),
            agent_did: "did:key:keep-agent".to_string(),
            op_raw: "edit".to_string(),
            op: Some(PersonaOp::Edit),
            behavior_id: Some("keep-behavior".to_string()),
            persona_name: Some("Keep Persona".to_string()),
            backend_model: Some("openai|gpt-5".to_string()),
            root: Some("/new/desired/root".to_string()),
            preset: Some("".to_string()),
            profile_id: Some("profile-1".to_string()),
            ..Default::default()
        };

        apply_persona_request(&node, &doc, &PersonaCatalogView::default()).await?;

        let updated_behavior = load_agent_behavior(&node, "keep-behavior")
            .await?
            .expect("behavior exists");
        assert_eq!(
            updated_behavior.tool_selection_id,
            Some("sel-keep".to_string()),
            "selection id must not repoint"
        );

        let updated_selection = load_tool_selection(&node, "sel-keep")
            .await?
            .expect("selection still exists");
        assert_eq!(
            updated_selection.file_tool_root,
            Some("/new/desired/root".to_string())
        );
        assert_eq!(
            updated_selection.bash_mode,
            Some("ReadOnly".to_string()),
            "unrelated fields unchanged"
        );
        assert_eq!(
            updated_selection.enable_memory,
            Some(true),
            "unrelated fields unchanged"
        );

        Ok(())
    }

    #[tokio::test]
    async fn disable_sets_enabled_false_preserving_other_fields() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let node = build_node(&tempdir).await;
        let access = ConfigAccess::Local(node.clone());

        let behavior = AgentBehaviorDocument {
            behavior_id: "disable-behavior".to_string(),
            agent_did: "did:key:disable-agent".to_string(),
            display_name: Some("Disable Persona".to_string()),
            description: Some("keep me".to_string()),
            summary: None,
            system_prompt: None,
            request_context_template: None,
            backend_id: Some("openai".to_string()),
            model_name: Some("gpt-5".to_string()),
            tool_selection_id: Some("sel-disable".to_string()),
            inference_profile_id: Some("profile-1".to_string()),
            compaction_strategy: None,
            compaction_threshold: None,
            enabled: true,
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            created_at: None,
        };
        write_agent_behavior_document(&access, &behavior).await?;

        let doc = PersonaRequestDoc {
            request_key: "req-disable-1".to_string(),
            agent_did: "did:key:disable-agent".to_string(),
            op_raw: "disable".to_string(),
            op: Some(PersonaOp::Disable),
            behavior_id: Some("disable-behavior".to_string()),
            ..Default::default()
        };

        let outcome = apply_persona_request(&node, &doc, &PersonaCatalogView::default()).await?;
        assert!(!outcome.repaired);
        assert_eq!(outcome.behavior_id, "disable-behavior");

        let after = load_agent_behavior(&node, "disable-behavior")
            .await?
            .expect("behavior still exists");
        assert!(!after.enabled);
        assert_eq!(after.display_name, Some("Disable Persona".to_string()));
        assert_eq!(after.description, Some("keep me".to_string()));
        assert_eq!(after.tool_selection_id, Some("sel-disable".to_string()));
        assert_eq!(after.backend_id, Some("openai".to_string()));
        assert_eq!(after.model_name, Some("gpt-5".to_string()));
        assert_eq!(after.inference_profile_id, Some("profile-1".to_string()));

        Ok(())
    }
}
