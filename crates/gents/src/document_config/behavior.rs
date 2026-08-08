use anyhow::Result;
use defra_node::EmbeddedNode;
use serde::{Deserialize, Serialize};

use crate::graphql::escape_graphql_string;
use crate::retry::execute_graphql_with_conflict_retry;

use super::graphql_fields;
use super::serde_helpers::{first_row_with_doc_id, rows_with_doc_id};

const DEFAULT_BEHAVIOR_LABEL: &str = "Default";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentBehavior {
    pub behavior_id: String,
    pub agent_did: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub summary: Option<String>,
    pub system_prompt: Option<String>,
    pub request_context_template: Option<String>,
    pub backend_id: Option<String>,
    pub model_name: Option<String>,
    pub tool_selection_id: Option<String>,
    pub inference_profile_id: Option<String>,
    pub compaction_strategy: Option<String>,
    pub compaction_threshold: Option<f64>,
    pub enabled: bool,
    #[serde(
        default,
        deserialize_with = "super::serde_helpers::deserialize_string_vec_or_null"
    )]
    pub skill_refs: Vec<String>,
    #[serde(
        default,
        deserialize_with = "super::serde_helpers::deserialize_string_vec_or_null"
    )]
    pub skill_excludes: Vec<String>,
    pub created_at: Option<String>,
}

const AGENT_BEHAVIOR_FIELDS: &str = r#"
                _docID
                behavior_id
                agent_did
                display_name
                description
                summary
                system_prompt
                request_context_template
                backend_id
                model_name
                tool_selection_id
                inference_profile_id
                compaction_strategy
                compaction_threshold
                enabled
                skill_refs
                skill_excludes
                created_at
"#;

pub async fn load_agent_behavior(
    node: &EmbeddedNode,
    behavior_id: &str,
) -> Result<Option<AgentBehavior>> {
    Ok(load_agent_behavior_record(node, behavior_id)
        .await?
        .map(|(_, behavior)| behavior))
}

pub(crate) async fn load_agent_behavior_record(
    node: &EmbeddedNode,
    behavior_id: &str,
) -> Result<Option<(String, AgentBehavior)>> {
    let escaped_behavior_id = escape_graphql_string(behavior_id);
    let query = format!(
        r#"{{
            AgentBehavior(
                filter: {{ behavior_id: {{ _eq: "{escaped_behavior_id}" }} }},
                limit: 1
            ) {{{AGENT_BEHAVIOR_FIELDS}}}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("query AgentBehavior failed: {:?}", resp.errors);
    }

    Ok(first_row_with_doc_id(resp.data.as_ref(), "AgentBehavior"))
}

pub(crate) async fn load_agent_behavior_by_doc_id(
    node: &EmbeddedNode,
    doc_id: &str,
) -> Result<Option<(String, AgentBehavior)>> {
    let escaped_doc_id = escape_graphql_string(doc_id);
    let query = format!(
        r#"{{
            AgentBehavior(
                filter: {{ _docID: {{ _eq: "{escaped_doc_id}" }} }},
                limit: 1
            ) {{{AGENT_BEHAVIOR_FIELDS}}}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("query AgentBehavior by _docID failed: {:?}", resp.errors);
    }

    Ok(first_row_with_doc_id(resp.data.as_ref(), "AgentBehavior"))
}

pub(crate) async fn load_agent_behavior_at_cid(
    node: &EmbeddedNode,
    composite_commit_cid: &str,
) -> Result<Option<(String, AgentBehavior)>> {
    let escaped_cid = escape_graphql_string(composite_commit_cid);
    let query = format!(
        r#"{{
            AgentBehavior(cid: ["{escaped_cid}"]) {{{AGENT_BEHAVIOR_FIELDS}}}
        }}"#
    );
    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("query AgentBehavior at CID failed: {:?}", resp.errors);
    }
    Ok(first_row_with_doc_id(resp.data.as_ref(), "AgentBehavior"))
}

pub async fn list_agent_behaviors(
    node: &EmbeddedNode,
    agent_did: &str,
) -> Result<Vec<AgentBehavior>> {
    Ok(list_agent_behavior_records(node, agent_did)
        .await?
        .into_iter()
        .map(|(_, behavior)| behavior)
        .collect())
}

pub(crate) async fn list_agent_behavior_records(
    node: &EmbeddedNode,
    agent_did: &str,
) -> Result<Vec<(String, AgentBehavior)>> {
    let escaped_agent_did = escape_graphql_string(agent_did);
    let query = format!(
        r#"{{
            AgentBehavior(
                filter: {{ agent_did: {{ _eq: "{escaped_agent_did}" }} }},
                order: {{ created_at: ASC }}
            ) {{{AGENT_BEHAVIOR_FIELDS}}}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("list AgentBehavior failed: {:?}", resp.errors);
    }

    Ok(rows_with_doc_id(resp.data.as_ref(), "AgentBehavior"))
}

pub async fn upsert_agent_behavior(node: &EmbeddedNode, behavior: &AgentBehavior) -> Result<()> {
    let escaped_behavior_id = escape_graphql_string(&behavior.behavior_id);
    let escaped_agent_did = escape_graphql_string(&behavior.agent_did);
    let created_at = behavior
        .created_at
        .as_deref()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());

    let add_fields = vec![
        Some(format!(r#"behavior_id: "{escaped_behavior_id}""#)),
        Some(format!(r#"agent_did: "{escaped_agent_did}""#)),
        graphql_fields::graphql_string_field("display_name", behavior.display_name.as_deref()),
        graphql_fields::graphql_string_field("description", behavior.description.as_deref()),
        graphql_fields::graphql_string_field("summary", behavior.summary.as_deref()),
        graphql_fields::graphql_string_field("system_prompt", behavior.system_prompt.as_deref()),
        graphql_fields::graphql_string_field(
            "request_context_template",
            behavior.request_context_template.as_deref(),
        ),
        graphql_fields::graphql_string_field("backend_id", behavior.backend_id.as_deref()),
        graphql_fields::graphql_string_field("model_name", behavior.model_name.as_deref()),
        graphql_fields::graphql_string_field(
            "tool_selection_id",
            behavior.tool_selection_id.as_deref(),
        ),
        graphql_fields::graphql_string_field(
            "inference_profile_id",
            behavior.inference_profile_id.as_deref(),
        ),
        graphql_fields::graphql_string_field(
            "compaction_strategy",
            behavior.compaction_strategy.as_deref(),
        ),
        graphql_fields::graphql_optional_float_field(
            "compaction_threshold",
            behavior.compaction_threshold,
        ),
        Some(format!(
            "enabled: {}",
            graphql_fields::graphql_bool(behavior.enabled)
        )),
        Some(format!(
            r#"created_at: "{}""#,
            escape_graphql_string(created_at.as_str())
        )),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(",\n                    ");

    let update_fields = vec![
        Some(format!(r#"agent_did: "{escaped_agent_did}""#)),
        graphql_fields::graphql_string_field("display_name", behavior.display_name.as_deref()),
        graphql_fields::graphql_string_field("description", behavior.description.as_deref()),
        graphql_fields::graphql_string_field("summary", behavior.summary.as_deref()),
        graphql_fields::graphql_string_field("system_prompt", behavior.system_prompt.as_deref()),
        graphql_fields::graphql_string_field(
            "request_context_template",
            behavior.request_context_template.as_deref(),
        ),
        graphql_fields::graphql_string_field("backend_id", behavior.backend_id.as_deref()),
        graphql_fields::graphql_string_field("model_name", behavior.model_name.as_deref()),
        graphql_fields::graphql_string_field(
            "tool_selection_id",
            behavior.tool_selection_id.as_deref(),
        ),
        graphql_fields::graphql_string_field(
            "inference_profile_id",
            behavior.inference_profile_id.as_deref(),
        ),
        graphql_fields::graphql_string_field(
            "compaction_strategy",
            behavior.compaction_strategy.as_deref(),
        ),
        graphql_fields::graphql_optional_float_field(
            "compaction_threshold",
            behavior.compaction_threshold,
        ),
        Some(format!(
            "enabled: {}",
            graphql_fields::graphql_bool(behavior.enabled)
        )),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(",\n                    ");

    let mutation = format!(
        r#"mutation {{
            upsert_AgentBehavior(
                filter: {{ behavior_id: {{ _eq: "{escaped_behavior_id}" }} }},
                add: {{
                    {add_fields}
                }},
                update: {{
                    {update_fields}
                }}
            ) {{ _docID }}
        }}"#
    );

    let resp = execute_graphql_with_conflict_retry(node, &mutation, "upsert AgentBehavior").await;
    if resp.has_errors() {
        anyhow::bail!("upsert AgentBehavior failed: {:?}", resp.errors);
    }
    Ok(())
}

pub(super) async fn create_default_behavior(
    node: &EmbeddedNode,
    agent_did: &str,
    behavior_id: &str,
    inference_profile_id: &str,
) -> Result<()> {
    upsert_agent_behavior(
        node,
        &AgentBehavior {
            behavior_id: behavior_id.to_string(),
            agent_did: agent_did.to_string(),
            display_name: Some(DEFAULT_BEHAVIOR_LABEL.to_string()),
            description: None,
            summary: None,
            system_prompt: None,
            request_context_template: None,
            backend_id: None,
            model_name: None,
            tool_selection_id: None,
            inference_profile_id: Some(inference_profile_id.to_string()),
            compaction_strategy: None,
            compaction_threshold: None,
            enabled: true,
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            created_at: Some(chrono::Utc::now().to_rfc3339()),
        },
    )
    .await
}
