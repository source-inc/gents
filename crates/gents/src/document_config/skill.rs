use anyhow::Result;
use defra_node::EmbeddedNode;
use serde::{Deserialize, Serialize};

use crate::graphql::escape_graphql_string;

use super::serde_helpers::{first_row_with_doc_id, rows_with_doc_id};

/// Document-layer view of a `Skill` row (decision D1). Mirrors
/// `crates/gents-protocol/schemas/agent/skill.graphql`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SkillDocument {
    pub skill_id: String,
    pub agent_did: String,
    pub scope: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub instructions: Option<String>,
    #[serde(
        default,
        deserialize_with = "super::serde_helpers::deserialize_string_vec_or_null"
    )]
    pub tool_refs: Vec<String>,
    pub display_name: Option<String>,
    pub interface_json: Option<String>,
    #[serde(default)]
    pub enabled: bool,
    pub created_at: Option<String>,
}

const SKILL_FIELDS: &str = r#"
                _docID
                skill_id
                agent_did
                scope
                name
                description
                instructions
                tool_refs
                display_name
                interface_json
                enabled
                created_at
"#;

/// List all `Skill` documents owned by `agent_did`. Returns `(doc_id, doc)`
/// pairs. Tolerates a missing `Skill` collection (older nodes) by surfacing the
/// query error to the caller, who treats absence as an empty set.
#[allow(dead_code)]
pub(crate) async fn list_skill_records(
    node: &EmbeddedNode,
    agent_did: &str,
) -> Result<Vec<(String, SkillDocument)>> {
    let escaped_agent_did = escape_graphql_string(agent_did);
    let query = format!(
        r#"{{
            Skill(
                filter: {{ agent_did: {{ _eq: "{escaped_agent_did}" }} }}
            ) {{{SKILL_FIELDS}}}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("list Skill failed: {:?}", resp.errors);
    }

    Ok(rows_with_doc_id(resp.data.as_ref(), "Skill"))
}

/// Load a single `Skill` document by its DefraDB `_docID` (used by the control
/// watcher to hot-reload skill changes into the runtime snapshot).
pub(crate) async fn load_skill_by_doc_id(
    node: &EmbeddedNode,
    doc_id: &str,
) -> Result<Option<(String, SkillDocument)>> {
    let escaped_doc_id = escape_graphql_string(doc_id);
    let query = format!(
        r#"{{
            Skill(filter: {{ _docID: {{ _eq: "{escaped_doc_id}" }} }}, limit: 1) {{{SKILL_FIELDS}}}
        }}"#
    );
    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("query Skill by _docID failed: {:?}", resp.errors);
    }
    Ok(first_row_with_doc_id(resp.data.as_ref(), "Skill"))
}

pub(crate) async fn load_skill_at_cid(
    node: &EmbeddedNode,
    composite_commit_cid: &str,
) -> Result<Option<(String, SkillDocument)>> {
    let escaped_cid = escape_graphql_string(composite_commit_cid);
    let query = format!(
        r#"{{
            Skill(cid: ["{escaped_cid}"]) {{{SKILL_FIELDS}}}
        }}"#
    );
    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("query Skill at CID failed: {:?}", resp.errors);
    }
    Ok(first_row_with_doc_id(resp.data.as_ref(), "Skill"))
}
