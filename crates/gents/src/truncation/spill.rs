use anyhow::Result;
use serde::Deserialize;

use crate::graphql::escape_graphql_string;
use crate::session::execute_mutation_with_retry;
use crate::truncation::{
    truncate_text, DefraSpillTruncator, TruncationLimits, TruncationMode, TruncationResult,
    TruncationTrigger, Truncator,
};

#[derive(Deserialize)]
struct ExistingToolResultFact {
    #[serde(rename = "_docID")]
    doc_id: String,
    tool_call_key: String,
    tool_call_doc_id: String,
    tool_call_composite_commit_cid: String,
    tool_call_signer_did: String,
    agent_did: String,
    requester_did: Option<String>,
    session_id: String,
    tool_name: String,
    tool_input: String,
    output_text: String,
    model_output_truncated: bool,
    truncation_metadata: String,
    conversation_doc_id: String,
}

async fn verify_existing_result_fact(
    node: &defra_node::EmbeddedNode,
    row: ExistingToolResultFact,
) -> Result<crate::SignedDocumentVersionRef> {
    let signer = node
        .verified_block_signer_did(&row.tool_call_composite_commit_cid)
        .await?;
    if signer != row.tool_call_signer_did {
        anyhow::bail!("stored AgentToolResult parent signer does not verify");
    }
    let query = format!(
        r#"{{ AgentToolCall(cid: ["{}"]) {{ _docID tool_call_key }} }}"#,
        escape_graphql_string(&row.tool_call_composite_commit_cid)
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "reconstructing stored AgentToolResult parent failed: {:?}",
            response.errors
        );
    }
    let parents = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentToolCall"))
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("stored AgentToolResult parent returned no rows"))?;
    match parents.as_slice() {
        [parent]
            if parent.get("_docID").and_then(serde_json::Value::as_str)
                == Some(row.tool_call_doc_id.as_str())
                && parent
                    .get("tool_call_key")
                    .and_then(serde_json::Value::as_str)
                    == Some(row.tool_call_key.as_str()) => {}
        rows => anyhow::bail!(
            "stored AgentToolResult parent reconstructed {} rows or a different call",
            rows.len()
        ),
    }
    crate::document_version::verified_current_signed_document_version(
        node,
        "AgentToolResult",
        &row.doc_id,
    )
    .await
}

impl DefraSpillTruncator {
    async fn exact_tool_call(&self) -> Result<(String, crate::SignedDocumentVersionRef)> {
        #[derive(Deserialize)]
        struct Row {
            #[serde(rename = "_docID")]
            doc_id: String,
            tool_call_key: String,
        }

        let tool_call_id = self
            .tool_call_id
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("full tool-output retention requires a tool_call_id"))?;
        let query = format!(
            r#"{{ AgentToolCall(filter: {{ session_id: {{ _eq: "{}" }}, tool_call_id: {{ _eq: "{}" }} }}) {{ _docID tool_call_key }} }}"#,
            escape_graphql_string(&self.session_id),
            escape_graphql_string(tool_call_id),
        );
        let response = self.node.execute(&query).await;
        if response.has_errors() {
            anyhow::bail!(
                "enumerating exact AgentToolCall parent failed: {:?}",
                response.errors
            );
        }
        let rows: Vec<Row> = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentToolCall"))
            .cloned()
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or_default();
        let [row] = rows.as_slice() else {
            anyhow::bail!(
                "tool-call logical key resolved to {} physical rows; refusing ambiguous output fact",
                rows.len()
            );
        };
        let signed = crate::document_version::verified_current_signed_document_version(
            &self.node,
            "AgentToolCall",
            &row.doc_id,
        )
        .await?;
        Ok((row.tool_call_key.clone(), signed))
    }

    async fn spill(
        &self,
        tool_name: &str,
        tool_input: &str,
        output: &str,
        metadata: &str,
        conversation_doc_id: Option<&str>,
        model_output_truncated: bool,
    ) -> Result<crate::SignedDocumentVersionRef> {
        let (tool_call_key, call) = self.exact_tool_call().await?;
        let result_key = call.version.doc_id.clone();
        let now = chrono::Utc::now().to_rfc3339();
        let escaped_result_key = escape_graphql_string(&result_key);
        let escaped_tool_call_key = escape_graphql_string(&tool_call_key);
        let escaped_call_doc_id = escape_graphql_string(&call.version.doc_id);
        let escaped_call_cid = escape_graphql_string(&call.version.composite_commit_cid);
        let escaped_call_signer = escape_graphql_string(&call.signer_did);
        let escaped_agent_did = escape_graphql_string(&self.agent_did);
        let escaped_session_id = escape_graphql_string(&self.session_id);
        let escaped_tool_name = escape_graphql_string(tool_name);
        let escaped_output = escape_graphql_string(output);
        let escaped_input = escape_graphql_string(tool_input);
        let escaped_metadata = escape_graphql_string(metadata);
        let escaped_conversation_doc_id =
            escape_graphql_string(conversation_doc_id.unwrap_or_default());
        let requester_did_field =
            crate::session::requester_did_create_field(self.requester_did.as_deref());
        let lookup = format!(
            r#"{{ AgentToolResult(filter: {{ result_key: {{ _eq: "{}" }} }}) {{ _docID tool_call_key tool_call_doc_id tool_call_composite_commit_cid tool_call_signer_did agent_did requester_did session_id tool_name tool_input output_text model_output_truncated truncation_metadata conversation_doc_id }} }}"#,
            escape_graphql_string(&result_key)
        );
        let observe =
            |rows: Vec<ExistingToolResultFact>| -> Result<Option<ExistingToolResultFact>> {
                if rows.len() > 1 {
                    anyhow::bail!(
                        "AgentToolResult logical key has {} physical twins; refusing ambiguity",
                        rows.len()
                    );
                }
                let Some(row) = rows.into_iter().next() else {
                    return Ok(None);
                };
                if row.tool_call_key == tool_call_key
                    && row.tool_call_doc_id == call.version.doc_id
                    && row.agent_did == self.agent_did
                    && row.requester_did == self.requester_did
                    && row.session_id == self.session_id
                    && row.tool_name == tool_name
                    && row.tool_input == tool_input
                    && row.output_text == output
                    && row.model_output_truncated == model_output_truncated
                    && row.truncation_metadata == metadata
                    && row.conversation_doc_id == conversation_doc_id.unwrap_or_default()
                {
                    Ok(Some(row))
                } else {
                    anyhow::bail!("AgentToolResult replay conflicts with immutable payload")
                }
            };
        let load_existing = || async {
            let response = self.node.execute(&lookup).await;
            if response.has_errors() {
                anyhow::bail!(
                    "enumerating AgentToolResult twins failed: {:?}",
                    response.errors
                );
            }
            Ok::<Vec<ExistingToolResultFact>, anyhow::Error>(
                response
                    .data
                    .as_ref()
                    .and_then(|data| data.get("AgentToolResult"))
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()?
                    .unwrap_or_default(),
            )
        };
        if let Some(existing) = observe(load_existing().await?)? {
            return verify_existing_result_fact(&self.node, existing).await;
        }
        let mutation = format!(
            r#"mutation {{
                create_AgentToolResult(input: {{
                    result_key: "{escaped_result_key}",
                    tool_call_key: "{escaped_tool_call_key}",
                    tool_call_doc_id: "{escaped_call_doc_id}",
                    tool_call_composite_commit_cid: "{escaped_call_cid}",
                    tool_call_signer_did: "{escaped_call_signer}",
                    agent_did: "{escaped_agent_did}",
                    {requester_did_field}
                    session_id: "{escaped_session_id}",
                    tool_name: "{escaped_tool_name}",
                    tool_input: "{escaped_input}",
                    output_text: "{escaped_output}",
                    model_output_truncated: {model_output_truncated},
                    truncation_metadata: "{escaped_metadata}",
                    conversation_doc_id: "{escaped_conversation_doc_id}",
                    created_at: "{now}"
                }}) {{ _docID }}
            }}"#,
        );

        let resp =
            match execute_mutation_with_retry(&self.node, &mutation, "spill_tool_output").await {
                Ok(response) => response,
                Err(create_error) => {
                    if let Some(existing) = observe(load_existing().await?)? {
                        return verify_existing_result_fact(&self.node, existing).await;
                    }
                    return Err(create_error);
                }
            };

        let doc_id = resp
            .data
            .as_ref()
            .and_then(|data| extract_mutation_doc_id(data, "AgentToolResult"))
            .ok_or_else(|| anyhow::anyhow!("spill mutation returned no _docID"))?
            .to_string();

        tracing::debug!(
            tool = %tool_name,
            doc_id = %doc_id,
            bytes = output.len(),
            "spilled full tool output to DefraDB"
        );

        crate::document_version::verified_current_signed_document_version(
            &self.node,
            "AgentToolResult",
            &doc_id,
        )
        .await
    }
}

impl Truncator for DefraSpillTruncator {
    async fn truncate(
        &self,
        tool_name: &str,
        tool_input: &str,
        output: &str,
        mode: TruncationMode,
        limits: &TruncationLimits,
        conversation_doc_id: Option<&str>,
    ) -> Result<TruncationResult> {
        let original_lines = output.lines().count();
        let original_bytes = output.len();

        let (text, trigger, truncated) = truncate_text(output, mode, limits);

        let metadata = serde_json::json!({
            "truncated": truncated,
            "truncated_by": trigger.map(|value| match value {
                TruncationTrigger::Lines => "lines",
                TruncationTrigger::Bytes => "bytes",
            }),
            "mode": match mode {
                TruncationMode::Head => "head",
                TruncationMode::Tail => "tail",
            },
            "original_lines": original_lines,
            "original_bytes": original_bytes,
            "max_lines": limits.max_lines,
            "max_bytes": limits.max_bytes,
        })
        .to_string();
        let spill_ref = self
            .spill(
                tool_name,
                tool_input,
                output,
                &metadata,
                conversation_doc_id,
                truncated,
            )
            .await?;
        let spill_doc_id = Some(spill_ref.version.doc_id.clone());

        let text = if let Some(ref doc_id) = spill_doc_id {
            format!("{}\n[Full output: DefraDB doc {}]", text, doc_id)
        } else {
            text
        };

        Ok(TruncationResult {
            text,
            truncated,
            truncated_by: trigger,
            original_lines,
            original_bytes,
            spill_doc_id,
            spill_ref: Some(spill_ref),
        })
    }
}

pub(super) fn extract_mutation_doc_id<'a>(
    data: &'a serde_json::Value,
    collection_name: &str,
) -> Option<&'a str> {
    for field_name in [
        format!("create_{collection_name}"),
        format!("add_{collection_name}"),
    ] {
        if let Some(value) = data.get(&field_name) {
            if let Some(doc_id) = value.get("_docID").and_then(|value| value.as_str()) {
                return Some(doc_id);
            }

            if let Some(doc_id) = value
                .as_array()
                .and_then(|rows| rows.first())
                .and_then(|row| row.get("_docID"))
                .and_then(|value| value.as_str())
            {
                return Some(doc_id);
            }
        }
    }

    None
}
