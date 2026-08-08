use std::sync::Arc;

use crate::llm::tool::Tool;
use crate::llm::tool::ToolDefinition;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::super::*;
use crate::graphql::escape_graphql_string;
use crate::identity::KeyIdentity;

pub(super) async fn test_node() -> Arc<EmbeddedNode> {
    let node_identity = test_identity("agent-document-runtime-node");
    Arc::new(
        EmbeddedNode::builder()
            .with_node_identity_did(node_identity.did())
            .build()
            .await
            .unwrap(),
    )
}

pub(super) fn test_identity(name: &str) -> KeyIdentity {
    let path = std::env::temp_dir().join(format!("{name}-{}.key", uuid::Uuid::new_v4()));
    KeyIdentity::load_or_create(path, None).unwrap()
}

#[derive(Debug, Deserialize)]
pub(super) struct EchoArgs {
    pub(super) value: String,
}

#[derive(Debug, thiserror::Error)]
#[error("echo tool error")]
pub(super) struct EchoToolError;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct EchoTool;

impl Tool for EchoTool {
    const NAME: &'static str = "echo_value";

    type Error = EchoToolError;
    type Args = EchoArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Echo a value back".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "value": {
                        "type": "string",
                        "description": "Value to echo"
                    }
                },
                "required": ["value"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        Ok(args.value)
    }
}

pub(super) async fn insert_inference_profile(node: &EmbeddedNode, profile_id: &str) {
    let escaped_profile_id = escape_graphql_string(profile_id);
    let mutation = format!(
        r#"mutation {{
            create_InferenceProfile(input: {{
                profile_id: "{escaped_profile_id}",
                display_name: "Balanced",
                context_window: 32768,
                max_output_tokens: 4096,
                max_turns: 8,
                temperature: 0.2,
                top_p: 0.95,
                top_k: 40,
                min_p: 0.05,
                frequency_penalty: 0.5,
                presence_penalty: -0.25,
                repetition_penalty: 1.1,
                reasoning_effort: "max",
                stream_batch_ms: 500,
                stream_liveness_timeout_secs: 45,
                deadline_duration_secs: 120
            }}) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(!resp.has_errors(), "{:?}", resp.errors);
}

pub(super) async fn insert_backend(node: &EmbeddedNode, backend_id: &str, endpoint: &str) {
    insert_backend_with_health(node, backend_id, endpoint, true, "healthy").await;
}

pub(super) async fn insert_backend_with_health(
    node: &EmbeddedNode,
    backend_id: &str,
    endpoint: &str,
    enabled: bool,
    probe_status: &str,
) {
    let escaped_backend_id = escape_graphql_string(backend_id);
    let escaped_endpoint = escape_graphql_string(endpoint);
    let escaped_probe_status = escape_graphql_string(probe_status);
    let mutation = format!(
        r#"mutation {{
            create_InferenceBackend(input: {{
                backend_id: "{escaped_backend_id}",
                name: "Balanced Backend",
                endpoint: "{escaped_endpoint}",
                max_concurrent: 2,
                enabled: {enabled},
                models: ["default"],
                probe_status: "{escaped_probe_status}"
            }}) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(!resp.has_errors(), "{:?}", resp.errors);
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn update_default_behavior(
    node: &EmbeddedNode,
    behavior_id: &str,
    inference_profile_id: &str,
    system_prompt: &str,
    backend_id: &str,
    model_name: &str,
    compaction_strategy: &str,
    compaction_threshold: f64,
) {
    let escaped_behavior_id = escape_graphql_string(behavior_id);
    let escaped_inference_profile_id = escape_graphql_string(inference_profile_id);
    let escaped_system_prompt = escape_graphql_string(system_prompt);
    let escaped_backend_id = escape_graphql_string(backend_id);
    let escaped_model_name = escape_graphql_string(model_name);
    let escaped_compaction_strategy = escape_graphql_string(compaction_strategy);
    let mutation = format!(
        r#"mutation {{
            update_AgentBehavior(
                filter: {{ behavior_id: {{ _eq: "{escaped_behavior_id}" }} }},
                input: {{
                    inference_profile_id: "{escaped_inference_profile_id}",
                    system_prompt: "{escaped_system_prompt}",
                    backend_id: "{escaped_backend_id}",
                    model_name: "{escaped_model_name}",
                    compaction_strategy: "{escaped_compaction_strategy}",
                    compaction_threshold: {compaction_threshold},
                    enabled: true
                }}
            ) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(!resp.has_errors(), "{:?}", resp.errors);
}
