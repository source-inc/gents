//! Apply-owned `DatastoreToolSurface` documents.

use anyhow::Result;
use defra_node::EmbeddedNode;
use serde::{Deserialize, Serialize};

use crate::graphql::escape_graphql_string;

use super::serde_helpers::{first_row_with_doc_id, rows_with_doc_id};
use super::tool_selection::{deserialize_optional_write_tools, WriteToolDecl};

/// Document-layer view of a `DatastoreToolSurface` row.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DatastoreToolSurfaceDocument {
    pub surface_id: String,
    pub agent_did: String,
    pub display_name: Option<String>,
    #[serde(default)]
    pub enabled: bool,
    /// Create-tool declarations (same dual-shape as `ToolSelection.write_tools`).
    #[serde(default, deserialize_with = "deserialize_optional_write_tools")]
    pub entries: Option<Vec<WriteToolDecl>>,
    pub created_at: Option<String>,
}

const SURFACE_FIELDS: &str = r#"
                _docID
                surface_id
                agent_did
                display_name
                enabled
                entries
                created_at
"#;

/// List all surfaces owned by `agent_did`.
pub(crate) async fn list_datastore_tool_surface_records(
    node: &EmbeddedNode,
    agent_did: &str,
) -> Result<Vec<(String, DatastoreToolSurfaceDocument)>> {
    let escaped_agent_did = escape_graphql_string(agent_did);
    let query = format!(
        r#"{{
            DatastoreToolSurface(
                filter: {{ agent_did: {{ _eq: "{escaped_agent_did}" }} }}
            ) {{{SURFACE_FIELDS}}}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("list DatastoreToolSurface failed: {:?}", resp.errors);
    }

    Ok(rows_with_doc_id(resp.data.as_ref(), "DatastoreToolSurface"))
}

/// Load one surface by DefraDB `_docID` (control-watcher hot-reload).
pub(crate) async fn load_datastore_tool_surface_by_doc_id(
    node: &EmbeddedNode,
    doc_id: &str,
) -> Result<Option<(String, DatastoreToolSurfaceDocument)>> {
    let escaped_doc_id = escape_graphql_string(doc_id);
    let query = format!(
        r#"{{
            DatastoreToolSurface(filter: {{ _docID: {{ _eq: "{escaped_doc_id}" }} }}, limit: 1) {{{SURFACE_FIELDS}}}
        }}"#
    );
    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!(
            "query DatastoreToolSurface by _docID failed: {:?}",
            resp.errors
        );
    }
    Ok(first_row_with_doc_id(
        resp.data.as_ref(),
        "DatastoreToolSurface",
    ))
}
