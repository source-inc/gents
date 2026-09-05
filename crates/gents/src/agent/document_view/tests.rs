use std::sync::Arc;

use super::*;
use crate::agent::DocumentResolveContext;
use crate::document_config::ToolSelectionDocument;
use crate::ensure_runtime_schemas;
use crate::graphql::escape_graphql_string;
use crate::identity::{AgentIdentity, KeyIdentity};
use crate::tool_surface::ToolCeiling;

async fn test_node() -> Arc<defra_node::EmbeddedNode> {
    Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap())
}

/// Extract a created document's `_docID` from a create/add mutation response,
/// regardless of the wrapper field name (`add_Skill`/`create_Skill`/…) or
/// whether the row is an object or a single-element array.
fn created_skill_doc_id(data: Option<&serde_json::Value>) -> Option<String> {
    for value in data?.as_object()?.values() {
        let row = value
            .as_array()
            .and_then(|rows| rows.first())
            .unwrap_or(value);
        if let Some(id) = row.get("_docID").and_then(|v| v.as_str()) {
            return Some(id.to_string());
        }
    }
    None
}

fn test_identity(name: &str) -> KeyIdentity {
    let path = std::env::temp_dir().join(format!("{name}-{}.key", uuid::Uuid::new_v4()));
    KeyIdentity::load_or_create(path, None).unwrap()
}

async fn bind_default_behavior_backend(
    node: &defra_node::EmbeddedNode,
    agent_did: &str,
    backend_id: &str,
    endpoint: &str,
) {
    let bootstrap = crate::ensure_agent_principal(node, agent_did)
        .await
        .unwrap();
    let escaped_backend_id = escape_graphql_string(backend_id);
    let escaped_endpoint = escape_graphql_string(endpoint);
    let mutation = format!(
        r#"mutation {{
            upsert_InferenceBackend(
                filter: {{ backend_id: {{ _eq: "{escaped_backend_id}" }} }},
                add: {{
                    backend_id: "{escaped_backend_id}",
                    name: "{escaped_backend_id}",
                    provider_kind: "OpenAiCompatible",
                    endpoint: "{escaped_endpoint}",
                    max_concurrent: 1,
                    enabled: true,
                    models: ["default"],
                    probe_status: "healthy"
                }},
                update: {{
                    name: "{escaped_backend_id}",
                    endpoint: "{escaped_endpoint}",
                    max_concurrent: 1,
                    enabled: true,
                    probe_status: "healthy"
                }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "upsert InferenceBackend failed: {:?}",
        response.errors
    );

    let mut default_behavior =
        crate::load_agent_behavior(node, &bootstrap.default_behavior.behavior_id)
            .await
            .unwrap()
            .expect("default behavior document");
    default_behavior.backend_id = Some(backend_id.to_string());
    crate::upsert_agent_behavior(node, &default_behavior)
        .await
        .unwrap();
}

#[tokio::test]
async fn load_document_runtime_view_includes_referenced_documents() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("document-view-load"));
    bind_default_behavior_backend(
        node.as_ref(),
        identity.did(),
        "backend-document-view",
        "http://127.0.0.1:8121/v1",
    )
    .await;

    let default_behavior_id = crate::default_behavior_id_for_agent(identity.did());
    let selection_id = crate::default_tool_selection_id_for_behavior(&default_behavior_id);
    crate::upsert_tool_selection(
        node.as_ref(),
        &ToolSelectionDocument {
            selection_id: selection_id.clone(),
            agent_did: identity.did().to_string(),
            display_name: Some("Read tools".to_string()),
            tool_policy_version: Some(crate::tool_surface::TOOL_POLICY_V1.to_string()),
            enable_file_tools: Some(true),
            file_tools_mode: Some("ReadOnly".to_string()),
            file_tool_root: None,
            enable_bash: Some(false),
            bash_mode: Some("Off".to_string()),
            command_execution_policy: None,
            command_allowed_argv_prefixes: Some(Vec::new()),
            command_forbidden_argv_prefixes: Some(Vec::new()),
            command_network_mode: None,
            cli_tool_names: Some(Vec::new()),
            enable_meta_tools: Some(false),
            allowed_mcp_service_ids: Some(Vec::new()),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let mut default_behavior = crate::load_agent_behavior(node.as_ref(), &default_behavior_id)
        .await
        .unwrap()
        .expect("default behavior");
    default_behavior.tool_selection_id = Some(selection_id.clone());
    crate::upsert_agent_behavior(node.as_ref(), &default_behavior)
        .await
        .unwrap();

    let view = load_document_runtime_view(node.as_ref(), identity.did())
        .await
        .expect("document view should load");

    assert_eq!(view.principal.value.agent_did, identity.did());
    assert!(view.behaviors.contains_key(&default_behavior_id));
    assert!(view.tool_selections.contains_key(&selection_id));
    assert!(view.backends.contains_key("backend-document-view"));
}

#[tokio::test]
async fn apply_control_update_reconciles_tool_selection_via_doc_id() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("document-view-update"));
    bind_default_behavior_backend(
        node.as_ref(),
        identity.did(),
        "backend-document-view-update",
        "http://127.0.0.1:8122/v1",
    )
    .await;

    let resolve_context = DocumentResolveContext {
        identity: identity.clone(),
        tool_ceiling: ToolCeiling::readonly(),
        backend_health: crate::backend_health::BackendHealthMap::new(),
    };
    let mut view = load_document_runtime_view(node.as_ref(), identity.did())
        .await
        .expect("initial document view");

    let default_behavior_id = crate::default_behavior_id_for_agent(identity.did());
    let selection_id = crate::default_tool_selection_id_for_behavior(&default_behavior_id);
    crate::upsert_tool_selection(
        node.as_ref(),
        &ToolSelectionDocument {
            selection_id: selection_id.clone(),
            agent_did: identity.did().to_string(),
            display_name: Some("Read tools".to_string()),
            tool_policy_version: Some(crate::tool_surface::TOOL_POLICY_V1.to_string()),
            enable_file_tools: Some(true),
            file_tools_mode: Some("ReadOnly".to_string()),
            file_tool_root: None,
            enable_bash: Some(false),
            bash_mode: Some("Off".to_string()),
            command_execution_policy: None,
            command_allowed_argv_prefixes: Some(Vec::new()),
            command_forbidden_argv_prefixes: Some(Vec::new()),
            command_network_mode: None,
            cli_tool_names: Some(Vec::new()),
            enable_meta_tools: Some(false),
            allowed_mcp_service_ids: Some(Vec::new()),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let selection_doc_id =
        crate::document_config::load_tool_selection_record(node.as_ref(), &selection_id)
            .await
            .unwrap()
            .expect("tool selection record")
            .0;

    let mut default_behavior = crate::load_agent_behavior(node.as_ref(), &default_behavior_id)
        .await
        .unwrap()
        .expect("default behavior");
    default_behavior.tool_selection_id = Some(selection_id.clone());
    crate::upsert_agent_behavior(node.as_ref(), &default_behavior)
        .await
        .unwrap();

    let behavior_doc_id =
        crate::document_config::load_agent_behavior_record(node.as_ref(), &default_behavior_id)
            .await
            .unwrap()
            .expect("behavior record")
            .0;

    assert!(apply_control_update(
        node.as_ref(),
        identity.did(),
        "opaque-tool-selection-collection",
        &selection_doc_id,
        &mut view,
    )
    .await
    .is_ok_and(|outcome| outcome == ControlUpdateOutcome::Applied));
    assert!(apply_control_update(
        node.as_ref(),
        identity.did(),
        "opaque-agent-behavior-collection",
        &behavior_doc_id,
        &mut view,
    )
    .await
    .is_ok_and(|outcome| outcome == ControlUpdateOutcome::Applied));

    let snapshot =
        resolve_document_runtime_snapshot_from_view(node.as_ref(), &resolve_context, &view)
            .await
            .expect("snapshot from updated document view");
    let tool_surface = snapshot
        .tool_surfaces
        .get(&default_behavior_id)
        .expect("tool surface for default behavior");
    let tool_names = tool_surface.tool_names();
    assert!(tool_names.contains(&"read_file".to_string()));
    assert!(tool_names.contains(&"list_files".to_string()));
}

/// End-to-end (#340 progressive disclosure / D2): a principal-scoped Skill
/// document is inherited by the behavior (D5); its name+description appear in
/// the prompt CATALOG (not its body), and `load_skill` returns the full body on
/// demand with a degrade note for tool_refs outside the behavior ceiling (D3).
#[tokio::test]
async fn resolve_composes_principal_scoped_skill_into_prompt() {
    use crate::llm::tool::Tool;
    use crate::prompt::LayeredPromptBuilder;

    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("document-view-skill"));
    bind_default_behavior_backend(
        node.as_ref(),
        identity.did(),
        "backend-document-view-skill",
        "http://127.0.0.1:8231/v1",
    )
    .await;

    // Read-only tool selection: the ceiling contains read_file but not bash.
    let default_behavior_id = crate::default_behavior_id_for_agent(identity.did());
    let selection_id = crate::default_tool_selection_id_for_behavior(&default_behavior_id);
    crate::upsert_tool_selection(
        node.as_ref(),
        &ToolSelectionDocument {
            selection_id: selection_id.clone(),
            agent_did: identity.did().to_string(),
            display_name: Some("Read tools".to_string()),
            tool_policy_version: Some(crate::tool_surface::TOOL_POLICY_V1.to_string()),
            enable_file_tools: Some(true),
            file_tools_mode: Some("ReadOnly".to_string()),
            enable_bash: Some(false),
            bash_mode: Some("Off".to_string()),
            enable_meta_tools: Some(false),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut default_behavior = crate::load_agent_behavior(node.as_ref(), &default_behavior_id)
        .await
        .unwrap()
        .expect("default behavior");
    default_behavior.tool_selection_id = Some(selection_id.clone());
    crate::upsert_agent_behavior(node.as_ref(), &default_behavior)
        .await
        .unwrap();

    // Principal-scoped skill referencing one in-ceiling tool (read_file) and one
    // ungranted tool (exercises the D3 degrade note).
    let create_skill = format!(
        r#"mutation {{ create_Skill(input: {{
            skill_id: "skill-research",
            agent_did: "{did}",
            scope: "principal",
            name: "Research",
            description: "Find and cite sources",
            instructions: "Always cite your sources.",
            tool_refs: ["read_file", "definitely_not_a_tool"],
            enabled: true
        }}) {{ _docID }} }}"#,
        did = escape_graphql_string(identity.did()),
    );
    let resp = node.execute(&create_skill).await;
    assert!(!resp.has_errors(), "create_Skill failed: {:?}", resp.errors);

    let resolve_context = DocumentResolveContext {
        identity: identity.clone(),
        tool_ceiling: ToolCeiling::readonly(),
        backend_health: crate::backend_health::BackendHealthMap::new(),
    };
    let view = load_document_runtime_view(node.as_ref(), identity.did())
        .await
        .expect("document view");
    let snapshot =
        resolve_document_runtime_snapshot_from_view(node.as_ref(), &resolve_context, &view)
            .await
            .expect("snapshot");

    let behavior = snapshot
        .behaviors
        .get(&default_behavior_id)
        .expect("resolved default behavior");
    assert_eq!(
        behavior.skills.len(),
        1,
        "principal-scoped skill must be inherited by the behavior"
    );
    assert_eq!(behavior.skills[0].skill_id, "skill-research");

    let tool_surface = snapshot
        .tool_surfaces
        .get(&default_behavior_id)
        .expect("tool surface");
    // The preamble holds the CATALOG (name + description + load_skill mandate),
    // NOT the skill body (progressive disclosure).
    let preamble = LayeredPromptBuilder::new(behavior.as_ref(), tool_surface.as_ref(), &[])
        .preamble()
        .to_string();
    assert!(
        preamble.contains("Research"),
        "catalog lists the skill name: {preamble}"
    );
    assert!(
        preamble.contains("Find and cite sources"),
        "catalog lists the skill description: {preamble}"
    );
    assert!(
        preamble.contains("load_skill"),
        "catalog directs the model to load_skill"
    );
    assert!(
        !preamble.contains("Always cite your sources."),
        "skill BODY must NOT be in the catalog (loaded on demand): {preamble}"
    );

    // `load_skill` returns the full body on demand, with the D3 degrade note.
    // mcp_enabled=false: this read-only surface has no MCP, so an out-of-ceiling
    // ref is genuinely unavailable and must be flagged.
    let ceiling = crate::skills::skill_tool_ceiling(tool_surface.tool_names(), &[], false);
    let load_skill = crate::skills::LoadSkillTool::new(behavior.skills.clone(), ceiling);
    let loaded = load_skill
        .call(crate::skills::LoadSkillArgs {
            name: "Research".to_string(),
        })
        .await
        .expect("load_skill");
    assert!(
        loaded.contains("Always cite your sources."),
        "load_skill returns the full body: {loaded}"
    );
    assert!(
        loaded.contains("definitely_not_a_tool"),
        "load_skill body carries the degrade note for the ungranted tool_ref: {loaded}"
    );
}

/// Validates the raw GraphQL mutations the CLI `config skill` commands and the
/// Codex shim use against the live Skill schema: upsert (create/update),
/// update-by-filter (enable/disable), and delete-by-filter.
#[tokio::test]
async fn skill_crud_mutations_round_trip() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let did = "did:key:zSkillCrud";

    let create = format!(
        r#"mutation {{ upsert_Skill(
            filter: {{ skill_id: {{ _eq: "s1" }} }},
            add: {{ skill_id: "s1", agent_did: "{did}", scope: "behavior", name: "S", tool_refs: ["read_file"], enabled: true }},
            update: {{ enabled: true }}
        ) {{ _docID }} }}"#
    );
    let resp = node.execute(&create).await;
    assert!(!resp.has_errors(), "upsert_Skill: {:?}", resp.errors);

    let update = r#"mutation { update_Skill(
        filter: { skill_id: { _eq: "s1" } },
        input: { enabled: false }
    ) { _docID } }"#;
    let resp = node.execute(update).await;
    assert!(
        !resp.has_errors(),
        "update_Skill by filter: {:?}",
        resp.errors
    );

    let query = r#"{ Skill(filter: { skill_id: { _eq: "s1" } }) { skill_id enabled } }"#;
    let resp = node.execute(query).await;
    assert!(!resp.has_errors(), "query Skill: {:?}", resp.errors);
    let enabled = resp
        .data
        .as_ref()
        .and_then(|d| d.get("Skill"))
        .and_then(|a| a.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("enabled"))
        .and_then(|v| v.as_bool());
    assert_eq!(enabled, Some(false), "disable must persist");

    let delete = r#"mutation { delete_Skill(filter: { skill_id: { _eq: "s1" } }) { _docID } }"#;
    let resp = node.execute(delete).await;
    assert!(
        !resp.has_errors(),
        "delete_Skill by filter: {:?}",
        resp.errors
    );
}

/// The control watcher must hot-reload Skill changes (#340): a Skill
/// create/delete drives `apply_control_update` to `Applied` and updates the
/// view, so a running agent picks up skills without a restart.
#[tokio::test]
async fn apply_control_update_hot_reloads_skill() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("document-view-skill-reload"));
    bind_default_behavior_backend(
        node.as_ref(),
        identity.did(),
        "backend-skill-reload",
        "http://127.0.0.1:8233/v1",
    )
    .await;

    let mut view = load_document_runtime_view(node.as_ref(), identity.did())
        .await
        .expect("document view");
    assert!(view.skills.is_empty(), "no skills before create");

    let create = format!(
        r#"mutation {{ create_Skill(input: {{
            skill_id: "s-reload", agent_did: "{did}", scope: "principal",
            name: "Reload", instructions: "Reload me.", enabled: true
        }}) {{ _docID }} }}"#,
        did = escape_graphql_string(identity.did()),
    );
    let resp = node.execute(&create).await;
    assert!(!resp.has_errors(), "create_Skill: {:?}", resp.errors);
    let doc_id = created_skill_doc_id(resp.data.as_ref()).expect("created Skill _docID");

    let outcome = apply_control_update(node.as_ref(), identity.did(), "skill", &doc_id, &mut view)
        .await
        .expect("apply skill create");
    assert_eq!(outcome, ControlUpdateOutcome::Applied);
    assert!(view.skills.contains_key("s-reload"), "skill added to view");

    // A skill owned by a different principal is irrelevant.
    let foreign = "mutation { create_Skill(input: { skill_id: \"s-foreign\", agent_did: \"did:key:zOther\", scope: \"principal\", name: \"F\", enabled: true }) { _docID } }";
    let resp = node.execute(foreign).await;
    let foreign_doc_id = created_skill_doc_id(resp.data.as_ref()).expect("foreign Skill _docID");
    let outcome = apply_control_update(
        node.as_ref(),
        identity.did(),
        "skill",
        &foreign_doc_id,
        &mut view,
    )
    .await
    .expect("apply foreign skill");
    assert_eq!(outcome, ControlUpdateOutcome::Irrelevant);

    // Deletion drops it from the view.
    let delete =
        r#"mutation { delete_Skill(filter: { skill_id: { _eq: "s-reload" } }) { _docID } }"#;
    assert!(!node.execute(delete).await.has_errors());
    let outcome = apply_control_update(node.as_ref(), identity.did(), "skill", &doc_id, &mut view)
        .await
        .expect("apply skill delete");
    assert_eq!(outcome, ControlUpdateOutcome::Applied);
    assert!(
        !view.skills.contains_key("s-reload"),
        "skill removed from view"
    );
}

/// Insert a ToolSelection row with an empty string in `subagent_targets` and
/// return its `_docID`.  DefraDB schema has no non-empty constraint on
/// `[String]` fields, so the document writes successfully.  The validator
/// must catch this on read.
async fn insert_invalid_tool_selection(
    node: &defra_node::EmbeddedNode,
    selection_id: &str,
    agent_did: &str,
) -> String {
    let escaped_selection_id = escape_graphql_string(selection_id);
    let escaped_agent_did = escape_graphql_string(agent_did);

    // subagent_targets contains an empty string — valid at the DB level but
    // invalid per ToolSelectionDocument::validate().
    let mutation = format!(
        r#"mutation {{
            create_ToolSelection(input: {{
                selection_id: "{escaped_selection_id}",
                agent_did: "{escaped_agent_did}",
                subagent_targets: ["", "valid-behavior"],
                subagent_spawn_enabled: true
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create_ToolSelection (invalid) failed: {:?}",
        response.errors
    );

    let lookup = format!(
        r#"{{
            ToolSelection(filter: {{ selection_id: {{ _eq: "{escaped_selection_id}" }} }}, limit: 1) {{
                _docID
            }}
        }}"#
    );
    let response = node.execute(&lookup).await;
    response
        .data
        .as_ref()
        .and_then(|data| data.get("ToolSelection"))
        .and_then(|rows| rows.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("_docID"))
        .and_then(|v| v.as_str())
        .map(ToOwned::to_owned)
        .expect("ToolSelection _docID")
}

#[tokio::test]
async fn apply_control_update_rejects_tool_selection_with_empty_subagent_target() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("document-view-invalid-tool-selection"));
    let agent_did = identity.did();

    let selection_id = "invalid-targets-selection";
    let doc_id = insert_invalid_tool_selection(node.as_ref(), selection_id, agent_did).await;

    let mut view = load_document_runtime_view(node.as_ref(), agent_did)
        .await
        .expect("initial document view should load");

    let result = apply_control_update(
        node.as_ref(),
        agent_did,
        "opaque-tool-selection-collection",
        &doc_id,
        &mut view,
    )
    .await;

    assert!(
        result.is_err(),
        "apply_control_update must reject ToolSelection with empty subagent_target, got: {:?}",
        result
    );
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("subagent_targets"),
        "error message must mention subagent_targets, got: {err_msg}"
    );
}

#[tokio::test]
async fn apply_control_update_rejects_tool_selection_with_missing_subagent_target() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("document-view-missing-subagent-target"));
    let agent_did = identity.did();
    let selection_id = "missing-targets-selection";
    let escaped_selection_id = escape_graphql_string(selection_id);
    let escaped_agent_did = escape_graphql_string(agent_did);
    // A LOCAL-DID target (own agent_did) naming a behavior that does not exist
    // locally must be rejected; remote targets are exempt (handled elsewhere).
    let target_entry = crate::document_config::subagent_target_entry(
        "missing-behavior",
        agent_did,
        "missing-behavior",
        None,
    );
    let escaped_target_entry = escape_graphql_string(&target_entry);
    let mutation = format!(
        r#"mutation {{
            create_ToolSelection(input: {{
                selection_id: "{escaped_selection_id}",
                agent_did: "{escaped_agent_did}",
                subagent_targets: ["{escaped_target_entry}"],
                subagent_spawn_enabled: true
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create_ToolSelection failed: {:?}",
        response.errors
    );
    let record = crate::document_config::load_tool_selection_record(node.as_ref(), selection_id)
        .await
        .unwrap()
        .expect("tool selection record");
    let mut view = load_document_runtime_view(node.as_ref(), agent_did)
        .await
        .expect("initial document view should load");

    let result = apply_control_update(
        node.as_ref(),
        agent_did,
        "opaque-tool-selection-collection",
        &record.0,
        &mut view,
    )
    .await;

    assert!(
        result.is_err(),
        "apply_control_update must reject missing subagent target, got: {:?}",
        result
    );
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("missing-behavior") && err_msg.contains("AgentBehavior"),
        "error message must mention the missing target and AgentBehavior, got: {err_msg}"
    );
}

async fn create_task(node: &defra_node::EmbeddedNode, task_id: &str, name: &str) {
    let escaped_task_id = escape_graphql_string(task_id);
    let escaped_name = escape_graphql_string(name);
    let mutation = format!(
        r#"mutation {{
            create_Task(input: {{
                task_id: "{escaped_task_id}",
                name: "{escaped_name}",
                enabled: true
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create Task failed: {:?}",
        response.errors
    );
}

async fn create_task_bound(
    node: &defra_node::EmbeddedNode,
    task_id: &str,
    behavior_id: &str,
    prompt_template: &str,
    enabled: bool,
) {
    let escaped_task_id = escape_graphql_string(task_id);
    let escaped_behavior_id = escape_graphql_string(behavior_id);
    let escaped_prompt_template = escape_graphql_string(prompt_template);
    let mutation = format!(
        r#"mutation {{
            create_Task(input: {{
                task_id: "{escaped_task_id}",
                name: "{escaped_task_id}",
                behavior_id: "{escaped_behavior_id}",
                prompt_template: "{escaped_prompt_template}",
                enabled: {enabled}
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create Task failed: {:?}",
        response.errors
    );
}

async fn create_event_trigger(
    node: &defra_node::EmbeddedNode,
    trigger_id: &str,
    task_id: &str,
    source_collection: &str,
    event_kind: &str,
    concurrency: &str,
) -> String {
    let escaped_trigger_id = escape_graphql_string(trigger_id);
    let escaped_task_id = escape_graphql_string(task_id);
    let escaped_source_collection = escape_graphql_string(source_collection);
    let escaped_event_kind = escape_graphql_string(event_kind);
    let escaped_concurrency = escape_graphql_string(concurrency);
    let mutation = format!(
        r#"mutation {{
            create_EventTrigger(input: {{
                trigger_id: "{escaped_trigger_id}",
                task_id: "{escaped_task_id}",
                source_collection: "{escaped_source_collection}",
                event_kind: "{escaped_event_kind}",
                enabled: true,
                concurrency: "{escaped_concurrency}"
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create EventTrigger failed: {:?}",
        response.errors
    );
    created_skill_doc_id(response.data.as_ref()).expect("created EventTrigger _docID")
}

#[tokio::test]
async fn apply_control_update_full_reloads_reserved_graph_triggers() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("document-view-graph-trigger"));
    bind_default_behavior_backend(
        node.as_ref(),
        identity.did(),
        "backend-document-view-graph-trigger",
        "http://127.0.0.1:8123/v1",
    )
    .await;
    let mut view = load_document_runtime_view(node.as_ref(), identity.did())
        .await
        .expect("initial document view");
    let doc_id = create_event_trigger(
        node.as_ref(),
        "graph-trigger-not-a-valid-revision",
        "operator-task",
        "AgentRequest",
        "created",
        "serial",
    )
    .await;

    let outcome = apply_control_update(
        node.as_ref(),
        identity.did(),
        "EventTrigger",
        &doc_id,
        &mut view,
    )
    .await
    .unwrap();
    assert_eq!(outcome, ControlUpdateOutcome::FullReload);

    node.shutdown().await;
}

async fn create_schedule(node: &defra_node::EmbeddedNode, schedule_id: &str, task_id: &str) {
    let escaped_schedule_id = escape_graphql_string(schedule_id);
    let escaped_task_id = escape_graphql_string(task_id);
    let mutation = format!(
        r#"mutation {{
            create_Schedule(input: {{
                schedule_id: "{escaped_schedule_id}",
                task_id: "{escaped_task_id}",
                interval_secs: 60,
                enabled: true
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create Schedule failed: {:?}",
        response.errors
    );
}

async fn create_schedule_with_concurrency(
    node: &defra_node::EmbeddedNode,
    schedule_id: &str,
    task_id: &str,
    concurrency: &str,
) {
    let escaped_schedule_id = escape_graphql_string(schedule_id);
    let escaped_task_id = escape_graphql_string(task_id);
    let escaped_concurrency = escape_graphql_string(concurrency);
    let mutation = format!(
        r#"mutation {{
            create_Schedule(input: {{
                schedule_id: "{escaped_schedule_id}",
                task_id: "{escaped_task_id}",
                interval_secs: 60,
                enabled: true,
                concurrency: "{escaped_concurrency}"
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create Schedule failed: {:?}",
        response.errors
    );
}

#[tokio::test]
async fn load_document_runtime_view_populates_tasks_and_schedules() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("document-view-tasks-schedules"));
    bind_default_behavior_backend(
        node.as_ref(),
        identity.did(),
        "backend-document-view-tasks",
        "http://127.0.0.1:8123/v1",
    )
    .await;

    create_task(node.as_ref(), "task-alpha", "Alpha").await;
    create_task(node.as_ref(), "task-beta", "Beta").await;
    create_schedule(node.as_ref(), "schedule-alpha", "task-alpha").await;

    let view = load_document_runtime_view(node.as_ref(), identity.did())
        .await
        .expect("document view should load");

    assert_eq!(view.tasks.len(), 2, "expected two Task documents");
    assert!(view.tasks.contains_key("task-alpha"));
    assert!(view.tasks.contains_key("task-beta"));

    assert_eq!(view.schedules.len(), 1, "expected one Schedule document");
    assert!(view.schedules.contains_key("schedule-alpha"));
    let schedule_record = view
        .schedules
        .get("schedule-alpha")
        .expect("schedule-alpha present");
    assert_eq!(
        schedule_record.value.task_id.as_deref(),
        Some("task-alpha"),
        "schedule references task-alpha"
    );
}

#[tokio::test]
async fn load_document_runtime_view_populates_event_triggers() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("document-view-event-triggers"));
    bind_default_behavior_backend(
        node.as_ref(),
        identity.did(),
        "backend-document-view-event-triggers",
        "http://127.0.0.1:8126/v1",
    )
    .await;

    create_task(node.as_ref(), "task-1", "Task One").await;
    create_event_trigger(
        node.as_ref(),
        "trig-1",
        "task-1",
        "CustomerSignup",
        "created",
        "serial",
    )
    .await;

    let view = load_document_runtime_view(node.as_ref(), identity.did())
        .await
        .expect("document view should load");

    assert_eq!(view.event_triggers.len(), 1);
    let record = view
        .event_triggers
        .get("trig-1")
        .expect("trig-1 present in event_triggers");
    assert_eq!(
        record.value.source_collection.as_deref(),
        Some("CustomerSignup")
    );
    assert_eq!(record.value.event_kind.as_deref(), Some("created"));
    assert_eq!(record.value.task_id.as_deref(), Some("task-1"));
}

#[tokio::test]
async fn resolve_produces_active_schedule_when_task_and_behavior_exist() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("document-view-resolve-schedule-active"));
    bind_default_behavior_backend(
        node.as_ref(),
        identity.did(),
        "backend-resolve-schedule-active",
        "http://127.0.0.1:8124/v1",
    )
    .await;

    let default_behavior_id = crate::default_behavior_id_for_agent(identity.did());
    create_task_bound(
        node.as_ref(),
        "task-resolve-active",
        &default_behavior_id,
        "do the thing",
        true,
    )
    .await;
    create_schedule_with_concurrency(
        node.as_ref(),
        "schedule-resolve-active",
        "task-resolve-active",
        "serial",
    )
    .await;

    let resolve_context = DocumentResolveContext {
        identity: identity.clone(),
        tool_ceiling: ToolCeiling::readonly(),
        backend_health: crate::backend_health::BackendHealthMap::new(),
    };
    let view = load_document_runtime_view(node.as_ref(), identity.did())
        .await
        .expect("document view should load");

    let snapshot =
        resolve_document_runtime_snapshot_from_view(node.as_ref(), &resolve_context, &view)
            .await
            .expect("resolve should succeed");

    assert_eq!(
        snapshot.active_schedules.len(),
        1,
        "expected exactly one active schedule"
    );
    assert!(
        snapshot.unavailable_schedules.is_empty(),
        "expected no unavailable schedules, got {:?}",
        snapshot.unavailable_schedules
    );
    let resolved = snapshot
        .active_schedules
        .get("schedule-resolve-active")
        .expect("schedule-resolve-active present in active_schedules");
    assert_eq!(resolved.task_id, "task-resolve-active");
    assert_eq!(resolved.task.behavior_id, default_behavior_id);
    assert_eq!(resolved.task.prompt_template, "do the thing");
    assert_eq!(
        resolved.cadence,
        crate::runtime_snapshot::ScheduleCadence::Interval { interval_secs: 60 }
    );
    assert!(resolved.enabled);
}

#[tokio::test]
async fn resolve_produces_active_event_trigger_when_task_and_behavior_exist() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("document-view-resolve-trigger-active"));
    bind_default_behavior_backend(
        node.as_ref(),
        identity.did(),
        "backend-resolve-trigger-active",
        "http://127.0.0.1:8127/v1",
    )
    .await;

    let default_behavior_id = crate::default_behavior_id_for_agent(identity.did());
    create_task_bound(
        node.as_ref(),
        "task-trigger-active",
        &default_behavior_id,
        "do the thing on event",
        true,
    )
    .await;
    create_event_trigger(
        node.as_ref(),
        "trigger-active",
        "task-trigger-active",
        "CustomerSignup",
        "created",
        "serial",
    )
    .await;

    let resolve_context = DocumentResolveContext {
        identity: identity.clone(),
        tool_ceiling: ToolCeiling::readonly(),
        backend_health: crate::backend_health::BackendHealthMap::new(),
    };
    let view = load_document_runtime_view(node.as_ref(), identity.did())
        .await
        .expect("document view should load");

    let snapshot =
        resolve_document_runtime_snapshot_from_view(node.as_ref(), &resolve_context, &view)
            .await
            .expect("resolve should succeed");

    assert_eq!(
        snapshot.active_event_triggers.len(),
        1,
        "expected exactly one active event trigger"
    );
    assert!(
        snapshot.unavailable_event_triggers.is_empty(),
        "expected no unavailable event triggers, got {:?}",
        snapshot.unavailable_event_triggers
    );
    let resolved = snapshot
        .active_event_triggers
        .get("trigger-active")
        .expect("trigger-active present in active_event_triggers");
    assert_eq!(resolved.trigger_id, "trigger-active");
    assert_eq!(resolved.task_id, "task-trigger-active");
    assert_eq!(resolved.task.behavior_id, default_behavior_id);
    assert_eq!(resolved.task.prompt_template, "do the thing on event");
    assert_eq!(resolved.source_collection, "CustomerSignup");
    assert_eq!(resolved.event_kind, "created");
    assert!(resolved.enabled);
}

#[tokio::test]
async fn resolve_marks_event_trigger_unavailable_when_task_missing_or_disabled() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("document-view-resolve-trigger-unavailable"));
    bind_default_behavior_backend(
        node.as_ref(),
        identity.did(),
        "backend-resolve-trigger-unavailable",
        "http://127.0.0.1:8128/v1",
    )
    .await;

    let default_behavior_id = crate::default_behavior_id_for_agent(identity.did());
    // Disabled task — trigger should be unavailable even though the task
    // document exists.
    create_task_bound(
        node.as_ref(),
        "task-trigger-disabled",
        &default_behavior_id,
        "disabled task",
        false,
    )
    .await;
    create_event_trigger(
        node.as_ref(),
        "trigger-task-disabled",
        "task-trigger-disabled",
        "CustomerSignup",
        "created",
        "serial",
    )
    .await;
    // Trigger whose task_id does not match any Task document.
    create_event_trigger(
        node.as_ref(),
        "trigger-task-missing",
        "task-that-never-existed",
        "CustomerSignup",
        "created",
        "serial",
    )
    .await;

    let resolve_context = DocumentResolveContext {
        identity: identity.clone(),
        tool_ceiling: ToolCeiling::readonly(),
        backend_health: crate::backend_health::BackendHealthMap::new(),
    };
    let view = load_document_runtime_view(node.as_ref(), identity.did())
        .await
        .expect("document view should load");

    let snapshot =
        resolve_document_runtime_snapshot_from_view(node.as_ref(), &resolve_context, &view)
            .await
            .expect("resolve should succeed");

    assert!(
        snapshot.active_event_triggers.is_empty(),
        "expected no active event triggers, got {:?}",
        snapshot.active_event_triggers.keys().collect::<Vec<_>>()
    );
    assert!(
        snapshot
            .unavailable_event_triggers
            .contains("trigger-task-missing"),
        "missing-task trigger should be in unavailable_event_triggers: {:?}",
        snapshot.unavailable_event_triggers
    );
    assert!(
        snapshot
            .unavailable_event_triggers
            .contains("trigger-task-disabled"),
        "disabled-task trigger should be in unavailable_event_triggers: {:?}",
        snapshot.unavailable_event_triggers
    );
}

/// `source_collection` is interpolated into GraphQL identifier positions by
/// the event source, where escaping cannot apply. A trigger document whose
/// `source_collection` is not a valid GraphQL Name (query injection) or is
/// `__`-prefixed (introspection-reserved) must be quarantined at resolve
/// time, never activated. This covers documents that bypass self-config
/// validation entirely (e.g. replicated from a peer).
#[tokio::test]
async fn resolve_quarantines_event_trigger_with_invalid_source_collection() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("document-view-resolve-trigger-injection"));
    bind_default_behavior_backend(
        node.as_ref(),
        identity.did(),
        "backend-resolve-trigger-injection",
        "http://127.0.0.1:8131/v1",
    )
    .await;

    let default_behavior_id = crate::default_behavior_id_for_agent(identity.did());
    create_task_bound(
        node.as_ref(),
        "task-trigger-injection",
        &default_behavior_id,
        "enabled task",
        true,
    )
    .await;
    create_event_trigger(
        node.as_ref(),
        "trigger-injection",
        "task-trigger-injection",
        "Msg(limit: 1) { _docID } Foo",
        "created",
        "serial",
    )
    .await;
    create_event_trigger(
        node.as_ref(),
        "trigger-introspection",
        "task-trigger-injection",
        "__Type",
        "created",
        "serial",
    )
    .await;

    let resolve_context = DocumentResolveContext {
        identity: identity.clone(),
        tool_ceiling: ToolCeiling::readonly(),
        backend_health: crate::backend_health::BackendHealthMap::new(),
    };
    let view = load_document_runtime_view(node.as_ref(), identity.did())
        .await
        .expect("document view should load");

    let snapshot =
        resolve_document_runtime_snapshot_from_view(node.as_ref(), &resolve_context, &view)
            .await
            .expect("resolve should succeed");

    assert!(
        snapshot.active_event_triggers.is_empty(),
        "no injection-shaped trigger may activate, got {:?}",
        snapshot.active_event_triggers.keys().collect::<Vec<_>>()
    );
    for trigger_id in ["trigger-injection", "trigger-introspection"] {
        assert!(
            snapshot.unavailable_event_triggers.contains(trigger_id),
            "{trigger_id} should be quarantined in unavailable_event_triggers: {:?}",
            snapshot.unavailable_event_triggers
        );
    }
}

#[tokio::test]
async fn resolve_marks_schedule_unavailable_when_task_missing_or_disabled() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("document-view-resolve-schedule-unavailable"));
    bind_default_behavior_backend(
        node.as_ref(),
        identity.did(),
        "backend-resolve-schedule-unavailable",
        "http://127.0.0.1:8125/v1",
    )
    .await;

    let default_behavior_id = crate::default_behavior_id_for_agent(identity.did());
    // Disabled task — schedule should be unavailable even though the task
    // document exists.
    create_task_bound(
        node.as_ref(),
        "task-resolve-disabled",
        &default_behavior_id,
        "disabled task",
        false,
    )
    .await;
    create_schedule_with_concurrency(
        node.as_ref(),
        "schedule-resolve-task-disabled",
        "task-resolve-disabled",
        "serial",
    )
    .await;
    // Schedule whose task_id does not match any Task document.
    create_schedule_with_concurrency(
        node.as_ref(),
        "schedule-resolve-task-missing",
        "task-that-never-existed",
        "serial",
    )
    .await;

    let resolve_context = DocumentResolveContext {
        identity: identity.clone(),
        tool_ceiling: ToolCeiling::readonly(),
        backend_health: crate::backend_health::BackendHealthMap::new(),
    };
    let view = load_document_runtime_view(node.as_ref(), identity.did())
        .await
        .expect("document view should load");

    let snapshot =
        resolve_document_runtime_snapshot_from_view(node.as_ref(), &resolve_context, &view)
            .await
            .expect("resolve should succeed");

    assert!(
        snapshot.active_schedules.is_empty(),
        "expected no active schedules, got {:?}",
        snapshot.active_schedules.keys().collect::<Vec<_>>()
    );
    assert!(
        snapshot
            .unavailable_schedules
            .contains("schedule-resolve-task-missing"),
        "missing-task schedule should be in unavailable_schedules: {:?}",
        snapshot.unavailable_schedules
    );
    assert!(
        snapshot
            .unavailable_schedules
            .contains("schedule-resolve-task-disabled"),
        "disabled-task schedule should be in unavailable_schedules: {:?}",
        snapshot.unavailable_schedules
    );
}

#[tokio::test]
async fn resolve_populates_active_tasks_for_enabled_tasks_with_ready_behaviors() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("document-view-resolve-active-tasks"));
    bind_default_behavior_backend(
        node.as_ref(),
        identity.did(),
        "backend-resolve-active-tasks",
        "http://127.0.0.1:8129/v1",
    )
    .await;

    let default_behavior_id = crate::default_behavior_id_for_agent(identity.did());
    // Enabled task bound to the ready default behavior — should land in
    // active_tasks.
    create_task_bound(
        node.as_ref(),
        "task-active",
        &default_behavior_id,
        "hello",
        true,
    )
    .await;
    // Disabled task — should NOT land in active_tasks even though its
    // behavior is ready.
    create_task_bound(
        node.as_ref(),
        "task-disabled",
        &default_behavior_id,
        "disabled",
        false,
    )
    .await;
    // Task bound to a behavior_id that does not resolve to any behavior
    // document — should NOT land in active_tasks.
    create_task_bound(
        node.as_ref(),
        "task-missing-behavior",
        "behavior-that-never-existed",
        "orphan",
        true,
    )
    .await;

    let resolve_context = DocumentResolveContext {
        identity: identity.clone(),
        tool_ceiling: ToolCeiling::readonly(),
        backend_health: crate::backend_health::BackendHealthMap::new(),
    };
    let view = load_document_runtime_view(node.as_ref(), identity.did())
        .await
        .expect("document view should load");

    let snapshot =
        resolve_document_runtime_snapshot_from_view(node.as_ref(), &resolve_context, &view)
            .await
            .expect("resolve should succeed");

    assert_eq!(
        snapshot.active_tasks.len(),
        1,
        "expected exactly one active task, got {:?}",
        snapshot.active_tasks.keys().collect::<Vec<_>>()
    );
    assert!(
        snapshot.active_tasks.contains_key("task-active"),
        "task-active should be in active_tasks: {:?}",
        snapshot.active_tasks.keys().collect::<Vec<_>>()
    );
    assert!(
        !snapshot.active_tasks.contains_key("task-disabled"),
        "disabled task must NOT be in active_tasks"
    );
    assert!(
        !snapshot.active_tasks.contains_key("task-missing-behavior"),
        "task with unavailable behavior must NOT be in active_tasks"
    );

    let resolved = snapshot
        .active_tasks
        .get("task-active")
        .expect("task-active present");
    assert_eq!(resolved.task_id, "task-active");
    assert_eq!(resolved.behavior_id, default_behavior_id);
    assert_eq!(resolved.prompt_template, "hello");
    assert!(resolved.output_schema_ref.is_none());
}

async fn bind_default_behavior_chatgpt_backend(
    node: &defra_node::EmbeddedNode,
    agent_did: &str,
    backend_id: &str,
) {
    let bootstrap = crate::ensure_agent_principal(node, agent_did)
        .await
        .unwrap();
    let escaped_backend_id = escape_graphql_string(backend_id);
    let mutation = format!(
        r#"mutation {{
            upsert_InferenceBackend(
                filter: {{ backend_id: {{ _eq: "{escaped_backend_id}" }} }},
                add: {{
                    backend_id: "{escaped_backend_id}",
                    name: "{escaped_backend_id}",
                    provider_kind: "ChatGptCodex",
                    endpoint: "https://chatgpt.com/backend-api/codex",
                    max_concurrent: 1,
                    enabled: true,
                    models: ["gpt-5.2"],
                    probe_status: "healthy"
                }},
                update: {{
                    name: "{escaped_backend_id}",
                    provider_kind: "ChatGptCodex",
                    enabled: true,
                    probe_status: "healthy"
                }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "upsert ChatGptCodex InferenceBackend failed: {:?}",
        response.errors
    );

    let mut default_behavior =
        crate::load_agent_behavior(node, &bootstrap.default_behavior.behavior_id)
            .await
            .unwrap()
            .expect("default behavior document");
    default_behavior.backend_id = Some(backend_id.to_string());
    crate::upsert_agent_behavior(node, &default_behavior)
        .await
        .unwrap();
}

async fn insert_enabled_oauth_credential(node: &defra_node::EmbeddedNode, agent_did: &str) {
    let credential = crate::oauth_credential::OAuthCredential {
        doc_id: None,
        credential_id: crate::oauth_credential::oauth_credential_id(
            agent_did,
            crate::chatgpt_codex::CHATGPT_CODEX_PROVIDER,
        ),
        agent_did: agent_did.to_string(),
        provider: crate::chatgpt_codex::CHATGPT_CODEX_PROVIDER.to_string(),
        access_token: "access-token".to_string(),
        refresh_token: "refresh-token".to_string(),
        id_token: None,
        account_id: None,
        chatgpt_plan_type: None,
        is_fedramp: false,
        access_token_expires_at: chrono::Utc::now() + chrono::Duration::hours(1),
        last_refresh: None,
        enabled: true,
    };
    let mutation = crate::oauth_credential::oauth_credential_upsert_mutation(&credential);
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "upsert OAuthCredential failed: {:?}",
        response.errors
    );
}

#[tokio::test]
async fn chatgpt_codex_behavior_without_credential_is_unavailable() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("document-view-chatgpt-nocred"));
    bind_default_behavior_chatgpt_backend(node.as_ref(), identity.did(), "backend-chatgpt-nocred")
        .await;
    let default_behavior_id = crate::default_behavior_id_for_agent(identity.did());

    let resolve_context = DocumentResolveContext {
        identity: identity.clone(),
        tool_ceiling: ToolCeiling::readonly(),
        backend_health: crate::backend_health::BackendHealthMap::new(),
    };
    let view = load_document_runtime_view(node.as_ref(), identity.did())
        .await
        .expect("document view");
    let snapshot =
        resolve_document_runtime_snapshot_from_view(node.as_ref(), &resolve_context, &view)
            .await
            .expect("snapshot");

    assert!(
        !snapshot.behaviors.contains_key(&default_behavior_id),
        "a ChatGptCodex behavior without an OAuthCredential must not be runnable (it would hang \
         startup readiness building the client)"
    );
    let reason = snapshot
        .unavailable_behaviors
        .get(&default_behavior_id)
        .expect("behavior should be reported unavailable");
    assert!(
        reason.diagnostic.contains("codex-login"),
        "unavailable reason should point at codex-login: {}",
        reason.diagnostic
    );
}

#[tokio::test]
async fn chatgpt_codex_behavior_with_enabled_credential_is_runnable() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("document-view-chatgpt-cred"));
    bind_default_behavior_chatgpt_backend(node.as_ref(), identity.did(), "backend-chatgpt-cred")
        .await;
    insert_enabled_oauth_credential(node.as_ref(), identity.did()).await;
    let default_behavior_id = crate::default_behavior_id_for_agent(identity.did());

    let resolve_context = DocumentResolveContext {
        identity: identity.clone(),
        tool_ceiling: ToolCeiling::readonly(),
        backend_health: crate::backend_health::BackendHealthMap::new(),
    };
    let view = load_document_runtime_view(node.as_ref(), identity.did())
        .await
        .expect("document view");
    let snapshot =
        resolve_document_runtime_snapshot_from_view(node.as_ref(), &resolve_context, &view)
            .await
            .expect("snapshot");

    assert!(
        snapshot.behaviors.contains_key(&default_behavior_id),
        "a ChatGptCodex behavior with an enabled OAuthCredential must be runnable; unavailable: {:?}",
        snapshot.unavailable_behaviors
    );
}

async fn bind_default_behavior_claude_backend(
    node: &defra_node::EmbeddedNode,
    agent_did: &str,
    backend_id: &str,
) {
    let bootstrap = crate::ensure_agent_principal(node, agent_did)
        .await
        .unwrap();
    let escaped_backend_id = escape_graphql_string(backend_id);
    let escaped_endpoint =
        escape_graphql_string(crate::claude_subscription::default_backend_endpoint());
    let mutation = format!(
        r#"mutation {{
            upsert_InferenceBackend(
                filter: {{ backend_id: {{ _eq: "{escaped_backend_id}" }} }},
                add: {{
                    backend_id: "{escaped_backend_id}",
                    name: "{escaped_backend_id}",
                    provider_kind: "ClaudeCliSubscription",
                    endpoint: "{escaped_endpoint}",
                    max_concurrent: 1,
                    enabled: true,
                    models: ["default"],
                    probe_status: "healthy"
                }},
                update: {{
                    name: "{escaped_backend_id}",
                    provider_kind: "ClaudeCliSubscription",
                    enabled: true,
                    probe_status: "healthy"
                }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "upsert ClaudeCliSubscription InferenceBackend failed: {:?}",
        response.errors
    );

    let mut default_behavior =
        crate::load_agent_behavior(node, &bootstrap.default_behavior.behavior_id)
            .await
            .unwrap()
            .expect("default behavior document");
    default_behavior.backend_id = Some(backend_id.to_string());
    crate::upsert_agent_behavior(node, &default_behavior)
        .await
        .unwrap();
}

#[tokio::test]
async fn claude_subscription_behavior_requires_enabled_credential() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("document-view-claude-cred"));
    bind_default_behavior_claude_backend(node.as_ref(), identity.did(), "backend-claude-cred")
        .await;
    let default_behavior_id = crate::default_behavior_id_for_agent(identity.did());
    let resolve_context = DocumentResolveContext {
        identity: identity.clone(),
        tool_ceiling: ToolCeiling::readonly(),
        backend_health: crate::backend_health::BackendHealthMap::new(),
    };

    let view = load_document_runtime_view(node.as_ref(), identity.did())
        .await
        .expect("document view");
    let snapshot =
        resolve_document_runtime_snapshot_from_view(node.as_ref(), &resolve_context, &view)
            .await
            .expect("snapshot");
    assert!(
        !snapshot.behaviors.contains_key(&default_behavior_id),
        "a ClaudeCliSubscription behavior without an OAuthCredential must not be runnable"
    );
    let reason = snapshot
        .unavailable_behaviors
        .get(&default_behavior_id)
        .expect("behavior should be reported unavailable");
    assert_eq!(
        reason.public_reason,
        gents_protocol::row::BehaviorReadinessUnavailableReason::CredentialsRequired
    );
    assert!(
        reason.diagnostic.contains(&format!(
            "run `gents claude-login --agent-did {}`",
            identity.did()
        )),
        "unavailable reason should point at claude-login: {}",
        reason.diagnostic
    );

    let credential = crate::claude_oauth::credential_from_login_tokens(
        identity.did(),
        crate::claude_oauth::CLAUDE_OAUTH_PROVIDER,
        &crate::claude_oauth::ClaudeLoginTokens {
            access_token: "access-token".to_string(),
            refresh_token: "refresh-token".to_string(),
            expires_in: Some(3600),
            scope: None,
        },
        chrono::Utc::now(),
    );
    crate::oauth_credential::upsert_oauth_credential(node.as_ref(), &credential)
        .await
        .expect("upsert credential");

    let view = load_document_runtime_view(node.as_ref(), identity.did())
        .await
        .expect("document view");
    let snapshot =
        resolve_document_runtime_snapshot_from_view(node.as_ref(), &resolve_context, &view)
            .await
            .expect("snapshot");
    assert!(
        snapshot.behaviors.contains_key(&default_behavior_id),
        "a ClaudeCliSubscription behavior with an enabled OAuthCredential must be runnable; unavailable: {:?}",
        snapshot.unavailable_behaviors
    );
}

#[tokio::test]
async fn apply_control_update_admits_chatgpt_behavior_when_credential_added() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("document-view-chatgpt-apply"));
    bind_default_behavior_chatgpt_backend(node.as_ref(), identity.did(), "backend-chatgpt-apply")
        .await;
    let default_behavior_id = crate::default_behavior_id_for_agent(identity.did());
    let resolve_context = DocumentResolveContext {
        identity: identity.clone(),
        tool_ceiling: ToolCeiling::readonly(),
        backend_health: crate::backend_health::BackendHealthMap::new(),
    };

    let mut view = load_document_runtime_view(node.as_ref(), identity.did())
        .await
        .expect("document view");
    let before =
        resolve_document_runtime_snapshot_from_view(node.as_ref(), &resolve_context, &view)
            .await
            .expect("snapshot");
    assert!(
        !before.behaviors.contains_key(&default_behavior_id),
        "behavior must start unavailable without a credential"
    );

    // Runtime codex-login: create the credential, then drive the incremental control update.
    let credential = crate::oauth_credential::OAuthCredential {
        doc_id: None,
        credential_id: crate::oauth_credential::oauth_credential_id(
            identity.did(),
            crate::chatgpt_codex::CHATGPT_CODEX_PROVIDER,
        ),
        agent_did: identity.did().to_string(),
        provider: crate::chatgpt_codex::CHATGPT_CODEX_PROVIDER.to_string(),
        access_token: "access-token".to_string(),
        refresh_token: "refresh-token".to_string(),
        id_token: None,
        account_id: None,
        chatgpt_plan_type: None,
        is_fedramp: false,
        access_token_expires_at: chrono::Utc::now() + chrono::Duration::hours(1),
        last_refresh: None,
        enabled: true,
    };
    let doc_id = crate::oauth_credential::upsert_oauth_credential(node.as_ref(), &credential)
        .await
        .expect("upsert credential");

    let outcome = apply_control_update(
        node.as_ref(),
        identity.did(),
        "OAuthCredential",
        &doc_id,
        &mut view,
    )
    .await
    .expect("apply control update");
    assert_eq!(
        outcome,
        ControlUpdateOutcome::Applied,
        "creating an OAuthCredential must drive a reconcile"
    );

    let after = resolve_document_runtime_snapshot_from_view(node.as_ref(), &resolve_context, &view)
        .await
        .expect("snapshot");
    assert!(
        after.behaviors.contains_key(&default_behavior_id),
        "behavior must become runnable once the credential exists; unavailable: {:?}",
        after.unavailable_behaviors
    );
}

// ---------------------------------------------------------------------------
// DatastoreToolSurface expand: surface ≡ inline, fail-closed
// ---------------------------------------------------------------------------

fn finding_decl() -> crate::document_config::WriteToolDecl {
    crate::document_config::WriteToolDecl {
        tool_name: "write_experiment_finding".to_string(),
        collection: "ExperimentFinding".to_string(),
        description: "Record a finding document for the next pipeline stage.".to_string(),
        fields: vec![
            crate::document_config::WriteToolField {
                name: "job_id".to_string(),
                required: true,
                fill: None,
            },
            crate::document_config::WriteToolField {
                name: "finding_id".to_string(),
                required: true,
                fill: None,
            },
            crate::document_config::WriteToolField {
                name: "content".to_string(),
                required: true,
                fill: None,
            },
            crate::document_config::WriteToolField {
                name: "stage".to_string(),
                required: true,
                fill: None,
            },
        ],
        output_obligation: None,
    }
}

fn empty_runtime_view(agent_did: &str) -> DocumentRuntimeView {
    DocumentRuntimeView {
        principal: DocumentRecord {
            doc_id: "principal".to_string(),
            value: crate::document_config::AgentPrincipal {
                agent_did: agent_did.to_string(),
                display_name: None,
                default_behavior_id: None,
                enabled: true,
                created_at: None,
                created_by: None,
            },
        },
        behaviors: Default::default(),
        skills: Default::default(),
        datastore_tool_surfaces: Default::default(),
        eth_tools: Default::default(),
        tool_selections: Default::default(),
        inference_profiles: Default::default(),
        backends: Default::default(),
        oauth_credentials: Default::default(),
        tasks: Default::default(),
        schedules: Default::default(),
        event_triggers: Default::default(),
        graph_definitions: Default::default(),
        graph_run_pins: Default::default(),
        visible_graph_package_artifact_ids: Default::default(),
    }
}

#[test]
fn runtime_skill_projection_is_canonical_across_map_insertion_order() {
    fn skill(skill_id: &str) -> DocumentRecord<crate::document_config::SkillDocument> {
        DocumentRecord {
            doc_id: format!("doc-{skill_id}"),
            value: crate::document_config::SkillDocument {
                skill_id: skill_id.to_string(),
                agent_did: "did:key:owner".to_string(),
                scope: Some("principal".to_string()),
                name: Some(skill_id.to_string()),
                description: None,
                instructions: Some(format!("Instructions for {skill_id}")),
                tool_refs: Vec::new(),
                display_name: None,
                interface_json: None,
                enabled: true,
                created_at: None,
            },
        }
    }

    let mut forward = empty_runtime_view("did:key:owner");
    forward.skills.insert("alpha".to_string(), skill("alpha"));
    forward.skills.insert("zeta".to_string(), skill("zeta"));
    let mut reverse = empty_runtime_view("did:key:owner");
    reverse.skills.insert("zeta".to_string(), skill("zeta"));
    reverse.skills.insert("alpha".to_string(), skill("alpha"));

    let forward_ids = super::snapshot::sorted_skills(&forward)
        .into_iter()
        .map(|skill| skill.skill_id)
        .collect::<Vec<_>>();
    let reverse_ids = super::snapshot::sorted_skills(&reverse)
        .into_iter()
        .map(|skill| skill.skill_id)
        .collect::<Vec<_>>();

    assert_eq!(forward_ids, vec!["alpha", "zeta"]);
    assert_eq!(reverse_ids, forward_ids);
}

#[test]
fn graph_artifact_visibility_unions_active_revisions_with_nonterminal_run_pins() {
    let mut view = empty_runtime_view("did:key:owner");
    view.graph_definitions.insert(
        "graph".to_owned(),
        DocumentRecord {
            doc_id: "definition-doc".to_owned(),
            value: crate::document_config::GraphDefinition {
                graph_id: "graph".to_owned(),
                owner_did: "did:key:owner".to_owned(),
                enabled: true,
                active_revision_digest: Some("sha256:active".to_owned()),
                generation: Some(2),
                created_at: None,
                updated_at: None,
            },
        },
    );
    view.graph_definitions.insert(
        "foreign-graph".to_owned(),
        DocumentRecord {
            doc_id: "foreign-definition-doc".to_owned(),
            value: crate::document_config::GraphDefinition {
                graph_id: "foreign-graph".to_owned(),
                owner_did: "did:key:foreign".to_owned(),
                enabled: true,
                active_revision_digest: Some("sha256:foreign-active".to_owned()),
                generation: Some(1),
                created_at: None,
                updated_at: None,
            },
        },
    );
    view.graph_definitions.insert(
        "disabled-graph".to_owned(),
        DocumentRecord {
            doc_id: "disabled-definition-doc".to_owned(),
            value: crate::document_config::GraphDefinition {
                graph_id: "disabled-graph".to_owned(),
                owner_did: "did:key:owner".to_owned(),
                enabled: false,
                active_revision_digest: Some("sha256:disabled".to_owned()),
                generation: Some(1),
                created_at: None,
                updated_at: None,
            },
        },
    );
    for (run_id, digest, status) in [
        ("running", "sha256:pinned", "running"),
        ("done", "sha256:retired", "succeeded"),
    ] {
        view.graph_run_pins.insert(
            run_id.to_owned(),
            DocumentRecord {
                doc_id: format!("{run_id}-doc"),
                value: crate::document_config::GraphRunPin {
                    run_id: run_id.to_owned(),
                    revision_digest: digest.to_owned(),
                    owner_did: "did:key:owner".to_owned(),
                    status: status.to_owned(),
                },
            },
        );
    }
    view.graph_run_pins.insert(
        "foreign-running".to_owned(),
        DocumentRecord {
            doc_id: "foreign-running-doc".to_owned(),
            value: crate::document_config::GraphRunPin {
                run_id: "foreign-running".to_owned(),
                revision_digest: "sha256:foreign-pinned".to_owned(),
                owner_did: "did:key:foreign".to_owned(),
                status: "running".to_owned(),
            },
        },
    );

    assert_eq!(
        super::active_graph_revision_pins(&view),
        (
            std::collections::BTreeSet::from(["sha256:active".to_owned()]),
            std::collections::BTreeSet::from(["sha256:pinned".to_owned()]),
        )
    );
}

#[test]
fn merge_surface_entries_match_inline_write_tools() {
    let agent_did = "did:key:zSurfaceTest";
    let decl = finding_decl();

    let inline_selection = ToolSelectionDocument {
        selection_id: "sel-inline".to_string(),
        agent_did: agent_did.to_string(),
        write_tools: Some(vec![decl.clone()]),
        datastore_tool_surface_ids: None,
        eth_tool_ids: None,
        ..Default::default()
    };
    let surface_selection = ToolSelectionDocument {
        selection_id: "sel-surface".to_string(),
        agent_did: agent_did.to_string(),
        write_tools: None,
        datastore_tool_surface_ids: Some(vec!["experiment-writes".to_string()]),
        ..Default::default()
    };

    let mut view = empty_runtime_view(agent_did);
    view.datastore_tool_surfaces.insert(
        "experiment-writes".to_string(),
        DocumentRecord {
            doc_id: "surf-doc".to_string(),
            value: crate::document_config::DatastoreToolSurfaceDocument {
                surface_id: "experiment-writes".to_string(),
                agent_did: agent_did.to_string(),
                display_name: Some("experiment writes".to_string()),
                enabled: true,
                entries: Some(vec![crate::document_config::SurfaceToolDecl::Create(
                    decl.clone(),
                )]),
                created_at: None,
            },
        },
    );

    let from_inline = merge_surface_tools(&inline_selection, &view).unwrap();
    let from_surface = merge_surface_tools(&surface_selection, &view).unwrap();
    assert_eq!(
        from_inline, from_surface,
        "surface expand must produce the same WriteToolDecl list as equivalent inline write_tools"
    );
    assert_eq!(from_surface.write_tools.len(), 1);
    assert_eq!(
        from_surface.write_tools[0].tool_name,
        "write_experiment_finding"
    );
    assert_eq!(from_surface.write_tools[0].collection, "ExperimentFinding");
    assert_eq!(from_surface.write_tools[0].fields.len(), 4);
}

#[test]
fn merge_fails_closed_on_missing_surface() {
    let agent_did = "did:key:zSurfaceMissing";
    let selection = ToolSelectionDocument {
        selection_id: "sel".to_string(),
        agent_did: agent_did.to_string(),
        datastore_tool_surface_ids: Some(vec!["does-not-exist".to_string()]),
        ..Default::default()
    };
    let view = empty_runtime_view(agent_did);
    let err = merge_surface_tools(&selection, &view).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("missing") && msg.contains("does-not-exist"),
        "expected missing surface error, got: {msg}"
    );
}

#[test]
fn merge_fails_closed_on_disabled_surface() {
    let agent_did = "did:key:zSurfaceDisabled";
    let mut view = empty_runtime_view(agent_did);
    view.datastore_tool_surfaces.insert(
        "disabled-writes".to_string(),
        DocumentRecord {
            doc_id: "surf-doc".to_string(),
            value: crate::document_config::DatastoreToolSurfaceDocument {
                surface_id: "disabled-writes".to_string(),
                agent_did: agent_did.to_string(),
                display_name: None,
                enabled: false,
                entries: Some(vec![crate::document_config::SurfaceToolDecl::Create(
                    finding_decl(),
                )]),
                created_at: None,
            },
        },
    );
    let selection = ToolSelectionDocument {
        selection_id: "sel".to_string(),
        agent_did: agent_did.to_string(),
        datastore_tool_surface_ids: Some(vec!["disabled-writes".to_string()]),
        ..Default::default()
    };
    let err = merge_surface_tools(&selection, &view).unwrap_err();
    assert!(
        err.to_string().contains("disabled"),
        "expected disabled error, got: {err}"
    );
}

#[test]
fn merge_reports_invalid_output_obligation_fields() {
    let agent_did = "did:key:zSurfaceObligation";
    let mut decl = finding_decl();
    decl.output_obligation = Some(crate::document_config::WriteToolOutputObligation {
        scope: crate::document_config::WriteToolOutputObligationScope::Trigger,
        minimum_writes: 1,
        expected_count_field: Some("missing_count".to_string()),
    });

    let mut view = empty_runtime_view(agent_did);
    view.datastore_tool_surfaces.insert(
        "invalid-obligation-writes".to_string(),
        DocumentRecord {
            doc_id: "surf-doc".to_string(),
            value: crate::document_config::DatastoreToolSurfaceDocument {
                surface_id: "invalid-obligation-writes".to_string(),
                agent_did: agent_did.to_string(),
                display_name: None,
                enabled: true,
                entries: Some(vec![crate::document_config::SurfaceToolDecl::Create(decl)]),
                created_at: None,
            },
        },
    );
    let selection = ToolSelectionDocument {
        selection_id: "sel".to_string(),
        agent_did: agent_did.to_string(),
        datastore_tool_surface_ids: Some(vec!["invalid-obligation-writes".to_string()]),
        ..Default::default()
    };

    let error = merge_surface_tools(&selection, &view).unwrap_err();
    let message = error.to_string();
    assert!(message.contains("expected_count_field"), "got: {message}");
    assert!(!message.contains("zero minimum_writes"), "got: {message}");
}

#[test]
fn merge_fails_closed_on_foreign_agent_surface() {
    let agent_did = "did:key:zSurfaceOwner";
    let mut view = empty_runtime_view(agent_did);
    view.datastore_tool_surfaces.insert(
        "foreign-writes".to_string(),
        DocumentRecord {
            doc_id: "surf-doc".to_string(),
            value: crate::document_config::DatastoreToolSurfaceDocument {
                surface_id: "foreign-writes".to_string(),
                agent_did: "did:key:zOtherAgent".to_string(),
                display_name: None,
                enabled: true,
                entries: Some(vec![crate::document_config::SurfaceToolDecl::Create(
                    finding_decl(),
                )]),
                created_at: None,
            },
        },
    );
    let selection = ToolSelectionDocument {
        selection_id: "sel".to_string(),
        agent_did: agent_did.to_string(),
        datastore_tool_surface_ids: Some(vec!["foreign-writes".to_string()]),
        ..Default::default()
    };
    let err = merge_surface_tools(&selection, &view).unwrap_err();
    assert!(
        err.to_string().contains("different agent"),
        "expected foreign agent error, got: {err}"
    );
}

#[test]
fn merge_fails_closed_on_name_collision() {
    let agent_did = "did:key:zSurfaceCollide";
    let decl = finding_decl();
    let mut view = empty_runtime_view(agent_did);
    view.datastore_tool_surfaces.insert(
        "experiment-writes".to_string(),
        DocumentRecord {
            doc_id: "surf-doc".to_string(),
            value: crate::document_config::DatastoreToolSurfaceDocument {
                surface_id: "experiment-writes".to_string(),
                agent_did: agent_did.to_string(),
                display_name: None,
                enabled: true,
                entries: Some(vec![crate::document_config::SurfaceToolDecl::Create(
                    decl.clone(),
                )]),
                created_at: None,
            },
        },
    );
    let selection = ToolSelectionDocument {
        selection_id: "sel".to_string(),
        agent_did: agent_did.to_string(),
        write_tools: Some(vec![decl]),
        datastore_tool_surface_ids: Some(vec!["experiment-writes".to_string()]),
        ..Default::default()
    };
    let err = merge_surface_tools(&selection, &view).unwrap_err();
    assert!(
        err.to_string().contains("duplicate"),
        "expected duplicate name error, got: {err}"
    );
}

#[test]
fn merge_expands_query_entries_separately_from_creates() {
    let agent_did = "did:key:zSurfaceQuery";
    let write = finding_decl();
    let query = crate::document_config::QueryToolDecl {
        tool_name: "query_experiment_finding".to_string(),
        collection: "ExperimentFinding".to_string(),
        description: "Load findings for this run.".to_string(),
        fields: vec!["finding_id".into(), "content".into()],
        filter_fields: vec![crate::document_config::WriteToolField {
            name: "run_id".into(),
            required: false,
            fill: Some(crate::document_config::WriteToolFieldFill::Correlation),
        }],
    };
    let mut view = empty_runtime_view(agent_did);
    view.datastore_tool_surfaces.insert(
        "experiment-io".to_string(),
        DocumentRecord {
            doc_id: "surf-doc".to_string(),
            value: crate::document_config::DatastoreToolSurfaceDocument {
                surface_id: "experiment-io".to_string(),
                agent_did: agent_did.to_string(),
                display_name: None,
                enabled: true,
                entries: Some(vec![
                    crate::document_config::SurfaceToolDecl::Create(write.clone()),
                    crate::document_config::SurfaceToolDecl::Query(query.clone()),
                ]),
                created_at: None,
            },
        },
    );
    let selection = ToolSelectionDocument {
        selection_id: "sel".to_string(),
        agent_did: agent_did.to_string(),
        datastore_tool_surface_ids: Some(vec!["experiment-io".to_string()]),
        ..Default::default()
    };
    let merged = super::merge_surface_tools(&selection, &view).unwrap();
    assert_eq!(merged.write_tools, vec![write]);
    assert_eq!(merged.query_tools, vec![query]);
}

#[tokio::test]
async fn apply_control_update_evicts_surface_when_ownership_moves_away() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let identity = Arc::new(test_identity("document-view-surface-revoke"));
    bind_default_behavior_backend(
        node.as_ref(),
        identity.did(),
        "backend-surface-revoke",
        "http://127.0.0.1:8234/v1",
    )
    .await;

    let mut view = load_document_runtime_view(node.as_ref(), identity.did())
        .await
        .expect("document view");

    let create = format!(
        r#"mutation {{ create_DatastoreToolSurface(input: {{
            surface_id: "experiment-writes", agent_did: "{did}", enabled: true,
            entries: ["{entry}"]
        }}) {{ _docID }} }}"#,
        did = escape_graphql_string(identity.did()),
        entry = escape_graphql_string(&serde_json::to_string(&finding_decl()).unwrap()),
    );
    let resp = node.execute(&create).await;
    assert!(
        !resp.has_errors(),
        "create_DatastoreToolSurface: {:?}",
        resp.errors
    );
    let doc_id = created_skill_doc_id(resp.data.as_ref()).expect("created surface _docID");

    let outcome = apply_control_update(
        node.as_ref(),
        identity.did(),
        "datastore_tool_surface",
        &doc_id,
        &mut view,
    )
    .await
    .expect("apply surface create");
    assert_eq!(outcome, ControlUpdateOutcome::Applied);
    assert!(view
        .datastore_tool_surfaces
        .contains_key("experiment-writes"));

    // Reassigning the surface to another principal must revoke the grant now,
    // not at the next process restart.
    let reassign = format!(
        r#"mutation {{ update_DatastoreToolSurface(
            docID: "{doc_id}", input: {{ agent_did: "did:key:zOtherOwner" }}
        ) {{ _docID }} }}"#,
        doc_id = escape_graphql_string(&doc_id),
    );
    let resp = node.execute(&reassign).await;
    assert!(
        !resp.has_errors(),
        "update_DatastoreToolSurface: {:?}",
        resp.errors
    );

    let outcome = apply_control_update(
        node.as_ref(),
        identity.did(),
        "datastore_tool_surface",
        &doc_id,
        &mut view,
    )
    .await
    .expect("apply surface reassign");
    assert_eq!(outcome, ControlUpdateOutcome::Applied);
    assert!(
        view.datastore_tool_surfaces.is_empty(),
        "surface must be evicted once it is owned by another principal"
    );
}

fn sample_eth_tool(
    agent_did: &str,
    tool_id: &str,
    enabled: bool,
    methods: &[&str],
) -> crate::document_config::EthToolDocument {
    crate::document_config::EthToolDocument {
        tool_id: tool_id.to_string(),
        agent_did: agent_did.to_string(),
        display_name: Some(tool_id.to_string()),
        enabled,
        chain_id: Some(8453),
        rpc_url: Some("https://mainnet.base.org".to_string()),
        query_methods: Some(methods.iter().map(|m| m.to_string()).collect()),
        calls: None,
        key_binding_id: None,
        created_at: None,
    }
}

#[test]
fn expand_eth_tools_skips_disabled_and_empty_methods() {
    let agent_did = "did:key:zEth";
    let selection = ToolSelectionDocument {
        selection_id: "sel".to_string(),
        agent_did: agent_did.to_string(),
        eth_tool_ids: Some(vec![
            "base-read".to_string(),
            "disabled".to_string(),
            "no-methods".to_string(),
        ]),
        ..Default::default()
    };
    let mut view = empty_runtime_view(agent_did);
    view.eth_tools.insert(
        "base-read".to_string(),
        DocumentRecord {
            doc_id: "e1".to_string(),
            value: sample_eth_tool(agent_did, "base-read", true, &["eth_chainId"]),
        },
    );
    view.eth_tools.insert(
        "disabled".to_string(),
        DocumentRecord {
            doc_id: "e2".to_string(),
            value: sample_eth_tool(agent_did, "disabled", false, &["eth_chainId"]),
        },
    );
    view.eth_tools.insert(
        "no-methods".to_string(),
        DocumentRecord {
            doc_id: "e3".to_string(),
            value: sample_eth_tool(agent_did, "no-methods", true, &[]),
        },
    );
    let expanded = expand_eth_tools(&selection, &view).expect("expand");
    assert_eq!(expanded.queries.len(), 1);
    assert_eq!(expanded.queries[0].tool_name(), "base-read_query");
}

#[test]
fn expand_eth_tools_fails_closed_on_missing_and_foreign() {
    let agent_did = "did:key:zEth";
    let missing = ToolSelectionDocument {
        selection_id: "sel".to_string(),
        agent_did: agent_did.to_string(),
        eth_tool_ids: Some(vec!["nope".to_string()]),
        ..Default::default()
    };
    let err = expand_eth_tools(&missing, &empty_runtime_view(agent_did)).unwrap_err();
    assert!(err.to_string().contains("missing"));

    let foreign = ToolSelectionDocument {
        selection_id: "sel".to_string(),
        agent_did: agent_did.to_string(),
        eth_tool_ids: Some(vec!["other".to_string()]),
        ..Default::default()
    };
    let mut view = empty_runtime_view(agent_did);
    view.eth_tools.insert(
        "other".to_string(),
        DocumentRecord {
            doc_id: "e1".to_string(),
            value: sample_eth_tool("did:key:zOther", "other", true, &["eth_chainId"]),
        },
    );
    let err = expand_eth_tools(&foreign, &view).unwrap_err();
    assert!(err.to_string().contains("different agent"));
}
