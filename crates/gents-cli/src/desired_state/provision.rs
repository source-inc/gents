use anyhow::{anyhow, Context, Result};
use serde_json::Value;

use crate::config_writes::ConfigAccess;
use gents::graphql::escape_graphql_string;

use super::{DesiredCallbackBinding, DesiredRepositoryPlacement, DesiredStateManifest};

pub(crate) async fn apply_workspace_provisioning(
    access: &ConfigAccess,
    manifest: &DesiredStateManifest,
) -> Result<WorkspaceProvisioningReport> {
    if manifest.callback_bindings.is_empty() && manifest.repository_placements.is_empty() {
        return Ok(WorkspaceProvisioningReport::default());
    }
    let deployment_id = load_host_deployment_id(access).await?.ok_or_else(|| {
        anyhow!("CallbackBinding/RepositoryPlacement require a HostDeployment row")
    })?;
    let mut report = WorkspaceProvisioningReport::default();
    for binding in &manifest.callback_bindings {
        upsert_callback_binding(
            access,
            binding,
            &deployment_id,
            &manifest.agent_principal.agent_did,
        )
        .await
        .with_context(|| format!("applying CallbackBinding {}", binding.binding_id))?;
        report.callback_bindings += 1;
    }
    for placement in &manifest.repository_placements {
        upsert_repository_placement(access, placement, &deployment_id)
            .await
            .with_context(|| format!("applying RepositoryPlacement {}", placement.repository_id))?;
        report.repository_placements += 1;
    }
    Ok(report)
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub(crate) struct WorkspaceProvisioningReport {
    pub(crate) callback_bindings: usize,
    pub(crate) repository_placements: usize,
}

async fn load_host_deployment_id(access: &ConfigAccess) -> Result<Option<String>> {
    let response = access
        .execute(
            r#"{
                HostDeployment(order: { created_at: ASC }, limit: 8) {
                    deployment_id
                }
            }"#,
        )
        .await
        .context("query HostDeployment")?;
    let rows = response
        .pointer("/data/HostDeployment")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            row.get("deployment_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        })
        .next())
}

async fn upsert_callback_binding(
    access: &ConfigAccess,
    binding: &DesiredCallbackBinding,
    deployment_id: &str,
    principal_did: &str,
) -> Result<()> {
    let owner = binding
        .owner_deployment_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(deployment_id);
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let filter = binding.filter.as_deref().unwrap_or("");
    let source_fields = binding.source_fields.as_deref().unwrap_or("");
    let module_id = binding.module_id.as_deref().unwrap_or("");
    let builtin = binding.builtin_emitter.as_deref().unwrap_or("");
    let capability_set = binding.capability_set.as_deref().unwrap_or("");
    let retry_policy = binding.retry_policy.as_deref().unwrap_or("");
    let principal = if binding.principal_did.trim().is_empty() {
        principal_did
    } else {
        binding.principal_did.trim()
    };
    let existing = access
        .execute(&format!(
            r#"{{
                CallbackBinding(filter: {{ binding_id: {{ _eq: "{id}" }} }}, limit: 1) {{
                    _docID
                }}
            }}"#,
            id = escape_graphql_string(&binding.binding_id),
        ))
        .await?;
    let existing_id = existing
        .pointer("/data/CallbackBinding/0/_docID")
        .and_then(Value::as_str)
        .map(str::to_string);
    let input = format!(
        r#"binding_id: "{}", source_collection: "{}", event_kind: "{}", filter: "{}", source_fields: "{}", module_id: "{}", builtin_emitter: "{}", principal_did: "{}", capability_set: "{}", retry_policy: "{}", owner_deployment_id: "{}", enabled: {}, created_at: "{}", updated_at: "{}""#,
        escape_graphql_string(&binding.binding_id),
        escape_graphql_string(&binding.source_collection),
        escape_graphql_string(&binding.event_kind),
        escape_graphql_string(filter),
        escape_graphql_string(source_fields),
        escape_graphql_string(module_id),
        escape_graphql_string(builtin),
        escape_graphql_string(principal),
        escape_graphql_string(capability_set),
        escape_graphql_string(retry_policy),
        escape_graphql_string(owner),
        if binding.enabled { "true" } else { "false" },
        escape_graphql_string(&now),
        escape_graphql_string(&now),
    );
    let mutation = if let Some(doc_id) = existing_id {
        format!(
            r#"mutation {{
            update_CallbackBinding(docID: "{doc_id}", input: {{ {input} }}) {{ _docID }}
        }}"#,
            doc_id = escape_graphql_string(&doc_id),
            input = input,
        )
    } else {
        format!(
            r#"mutation {{
            create_CallbackBinding(input: {{ {input} }}) {{ _docID }}
        }}"#
        )
    };
    access
        .execute_mutation(&mutation, "write CallbackBinding")
        .await?;
    Ok(())
}

async fn upsert_repository_placement(
    access: &ConfigAccess,
    placement: &DesiredRepositoryPlacement,
    deployment_id: &str,
) -> Result<()> {
    let host_path = std::fs::canonicalize(&placement.host_path)
        .unwrap_or_else(|_| std::path::PathBuf::from(&placement.host_path));
    let host_path = host_path.to_string_lossy();
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let existing = access
        .execute(&format!(
            r#"{{
                RepositoryPlacement(filter: {{ repository_id: {{ _eq: "{id}" }} }}, limit: 1) {{
                    _docID
                }}
            }}"#,
            id = escape_graphql_string(&placement.repository_id),
        ))
        .await?;
    let existing_id = existing
        .pointer("/data/RepositoryPlacement/0/_docID")
        .and_then(Value::as_str)
        .map(str::to_string);
    let input = format!(
        r#"repository_id: "{}", deployment_id: "{}", host_path: "{}", enabled: {}, updated_at: "{}""#,
        escape_graphql_string(&placement.repository_id),
        escape_graphql_string(deployment_id),
        escape_graphql_string(&host_path),
        if placement.enabled { "true" } else { "false" },
        escape_graphql_string(&now),
    );
    let mutation = if let Some(doc_id) = existing_id {
        format!(
            r#"mutation {{
            update_RepositoryPlacement(docID: "{doc_id}", input: {{ {input} }}) {{ _docID }}
        }}"#,
            doc_id = escape_graphql_string(&doc_id),
            input = input,
        )
    } else {
        format!(
            r#"mutation {{
            create_RepositoryPlacement(input: {{ {input} }}) {{ _docID }}
        }}"#
        )
    };
    access
        .execute_mutation(&mutation, "write RepositoryPlacement")
        .await?;
    Ok(())
}
