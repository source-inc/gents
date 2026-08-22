use anyhow::{anyhow, Result};
use defra_node::EmbeddedNode;
use serde::Deserialize;

use crate::graphql::{
    escape_graphql_string, first_row, graphql_mutation_with_transaction_retry, rows,
};

const HOST_DEPLOYMENT_FIELDS: &str = "deployment_id display_name created_at updated_at";

#[derive(Debug, Clone, Deserialize)]
struct HostDeploymentRow {
    deployment_id: String,
    created_at: Option<String>,
}

/// Load or mint the local-only HostDeployment id. Distinct from any agent DID.
pub async fn ensure_local_host_deployment(node: &EmbeddedNode) -> Result<String> {
    if let Some(existing) = load_local_host_deployment(node).await? {
        return Ok(existing);
    }

    let deployment_id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let mutation = format!(
        r#"mutation {{
            create_HostDeployment(input: {{
                deployment_id: "{deployment_id}",
                display_name: "local",
                created_at: "{now}",
                updated_at: "{now}"
            }}) {{ _docID }}
        }}"#,
        deployment_id = escape_graphql_string(&deployment_id),
        now = escape_graphql_string(&now),
    );
    let response =
        graphql_mutation_with_transaction_retry(node, &mutation, "create_HostDeployment").await;
    match response {
        Ok(_) => Ok(deployment_id),
        Err(error) => {
            if let Some(existing) = load_local_host_deployment(node).await? {
                return Ok(existing);
            }
            Err(anyhow!("creating HostDeployment failed: {error}"))
        }
    }
}

pub async fn load_local_host_deployment(node: &EmbeddedNode) -> Result<Option<String>> {
    let query = format!(
        r#"{{
            HostDeployment(
                order: {{ created_at: ASC }},
                limit: 8
            ) {{ {HOST_DEPLOYMENT_FIELDS} }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!("query HostDeployment failed: {:?}", response.errors);
    }
    let mut rows: Vec<HostDeploymentRow> = rows(&response, "HostDeployment")?;
    if rows.is_empty() {
        return Ok(first_row::<HostDeploymentRow>(&response, "HostDeployment")?
            .map(|row| row.deployment_id));
    }
    if rows.len() > 1 {
        tracing::warn!(
            count = rows.len(),
            "multiple HostDeployment rows on this node; using the oldest"
        );
        rows.sort_by(|left, right| left.created_at.cmp(&right.created_at));
    }
    Ok(rows.into_iter().next().map(|row| row.deployment_id))
}
