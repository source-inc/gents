//! Stock usage projection from the runtime's persisted accounting owner.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use defra_node::EmbeddedNode;
use gents::graphql::{ensure_no_errors, escape_graphql_string};
use gents::toolset::{load_session_inference_observation, SessionTokenUsage};
use gents_protocol::row::AgentRequestRow;
use serde_json::{json, Value};

/// The stock ContextInfo DTO explicitly accepts partial snapshots. Report
/// the runtime's component groups as-is; do not invent a system/message
/// split or provider message/tool counts that this observation lacks.
fn context_info(
    observation: &gents::toolset::SessionInferenceObservation,
    configured_window: u64,
) -> Result<Value> {
    let Some(context) = observation.latest_context.as_ref() else {
        anyhow::ensure!(
            observation.token_usage.model_calls == 0,
            "context accounting is unavailable for this session's persisted inference calls"
        );
        return Ok(
            json!({"used":0, "total":configured_window, "freeTokens":configured_window,
            "usagePct":0, "usageCategories":[]}),
        );
    };
    let accounting = &context.accounting;
    let used = (accounting.estimated_input_tokens as u64)
        .saturating_add(observation.latest_completion_tokens.unwrap_or(0));
    let total = (accounting.context_window as u64).max(1);
    let components = &accounting.components;
    let mut categories = Vec::new();
    for (label, tokens) in [
        ("Messages (including system)", components.messages as u64),
        ("Tool schemas", components.tool_schemas as u64),
        ("Documents", components.documents as u64),
        (
            "Additional parameters",
            components.additional_parameters as u64,
        ),
        ("Output schema", components.output_schema as u64),
        (
            "Latest generated output",
            observation.latest_completion_tokens.unwrap_or(0),
        ),
    ] {
        if tokens > 0 {
            categories.push(json!({"label":label, "tokens":tokens, "detail":accounting.estimator}));
        }
    }
    Ok(
        json!({"used":used, "total":total, "freeTokens":total.saturating_sub(used),
        "usagePct":((used as u128 * 100) / total as u128).min(100) as u64,
        "toolDefinitionsTokens":components.tool_schemas, "messageTokens":components.messages,
        "autoCompactThresholdPercent":(accounting.compaction_threshold_basis_points / 100).min(100),
        "usageCategories":categories}),
    )
}

pub(super) async fn session_info(
    node: &EmbeddedNode,
    principal: &str,
    behavior: &str,
    session: &str,
    model: &str,
    model_name: &str,
    context_window: u64,
) -> Result<Value> {
    let requests = super::sessions::load(node, principal, behavior, session).await?;
    let observation =
        load_session_inference_observation(node, principal, Some(principal), session).await?;
    let mut context = context_info(&observation, context_window)?;
    let details = match observation.latest_context.as_ref() {
        Some(snapshot) => {
            gents::toolset::load_session_context_details(
                node,
                principal,
                Some(principal),
                session,
                snapshot,
            )
            .await?
        }
        None => None,
    };
    if let Some(details) = details.as_ref() {
        context["systemPromptTokens"] = json!(details.system_prompt_tokens);
        context["messageTokens"] = json!(details.message_tokens);
        context["messageCount"] = json!(details.message_count);
        context["toolDefinitionsCount"] = json!(details.tool_definitions_count);
        context["toolCallCount"] = json!(details.tool_call_count);
    }
    let turns = observation
        .inference_turns
        .context("provider turn coordinates are unavailable for this session")?;
    context["turnCount"] = json!(turns);
    let mut compactions = BTreeSet::new();
    for page in requests.chunks(128) {
        let ids = page
            .iter()
            .map(|request| {
                request
                    .doc_id
                    .as_deref()
                    .context("missing context request identity")
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|id| format!("\"{}\"", escape_graphql_string(id)))
            .collect::<Vec<_>>()
            .join(",");
        let response = node
            .execute(&format!(
                r#"{{
            CompactionEntry(filter: {{request_doc_id: {{_in: [{ids}]}}}}) {{_docID}}
            ProviderContextReduction(filter: {{request_doc_id: {{_in: [{ids}]}}}}) {{_docID}}
        }}"#
            ))
            .await;
        ensure_no_errors(&response, "Grok context compactions")?;
        for collection in ["CompactionEntry", "ProviderContextReduction"] {
            let rows = response
                .data
                .as_ref()
                .and_then(|data| data.get(collection))
                .and_then(Value::as_array)
                .context("missing context compaction rows")?;
            for row in rows {
                compactions.insert((
                    collection,
                    row["_docID"]
                        .as_str()
                        .context("missing compaction identity")?
                        .to_owned(),
                ));
            }
        }
    }
    context["compactionCount"] = json!(compactions.len());
    // SessionInfo uses the extension's nested result envelope, unlike usage.
    // cwd is unknown, not the current client's claimed historical cwd.
    Ok(json!({"result":{
        "sessionId":session, "cwd":"", "agentName":behavior, "model":model,
        "modelDisplayName":model_name, "apiBackend":"gents", "turns":turns,
        "turnIndex":turns.saturating_sub(1), "context":context,
        "_meta":{"gents/partialContext":details.is_none(),
            "gents/unavailableContextFields":if details.is_some() {json!([])} else {json!(["systemPromptTokens", "toolDefinitionsCount", "messageCount", "toolCallCount"])},
            "gents/contextAccounting":observation.latest_context}
    }}))
}

#[derive(Default)]
struct Totals {
    input: u64,
    output: u64,
    cached: u64,
    calls: u64,
    duration_ms: u64,
    duration_incomplete: bool,
    incomplete: bool,
}

impl Totals {
    fn add(&mut self, usage: &SessionTokenUsage) {
        // These are observed lower bounds, not fabricated complete totals.
        // Stock PromptUsage has an explicit incomplete flag for this case.
        self.input = self.input.saturating_add(usage.input_tokens.unwrap_or(0));
        self.output = self.output.saturating_add(usage.output_tokens.unwrap_or(0));
        self.cached = self
            .cached
            .saturating_add(usage.cached_input_tokens.unwrap_or(0));
        self.calls = self.calls.saturating_add(usage.model_calls.max(0) as u64);
        self.duration_ms = self
            .duration_ms
            .saturating_add(usage.api_duration_ms.unwrap_or(0));
        self.duration_incomplete |= usage.model_calls > 0 && usage.api_duration_ms.is_none();
        self.incomplete |= usage.incomplete;
    }

    fn wire(&self, turns: Option<u64>) -> Value {
        json!({"usage":{
            "inputTokens":self.input, "outputTokens":self.output,
            "totalTokens":self.input.saturating_add(self.output), "cachedReadTokens":self.cached,
            "modelCalls":self.calls, "numTurns":turns.unwrap_or(0),
            "apiDurationMs":self.duration_ms,
            "usageIsIncomplete":self.incomplete || self.duration_incomplete || turns.is_none(),
            "costIsPartial":true
        }, "_meta":{"gents/usageScope":"persisted-session-and-readable-descendants",
            "gents/durationIsIncomplete":self.duration_incomplete,
            "gents/unavailableUsageFields":["costUsdTicks", "reasoningTokens", "cacheCreationTokens", "modelUsage"]}})
    }
}

/// Resolve logical child identities once per bounded page. Ambiguous aliases
/// remain unreadable, exactly as in the former per-edge lookup.
async fn descendant_owners(
    node: &EmbeddedNode,
    ids: BTreeSet<String>,
) -> Result<BTreeMap<String, Option<AgentRequestRow>>> {
    let ids: Vec<_> = ids.into_iter().collect();
    let mut owners = BTreeMap::new();
    for page in ids.chunks(128) {
        let ids = page
            .iter()
            .map(|id| format!("\"{}\"", escape_graphql_string(id)))
            .collect::<Vec<_>>()
            .join(",");
        let response = node
            .execute(&format!(
                r#"{{ AgentRequest(filter: {{request_id: {{_in: [{ids}]}}}}) {{
            request_id agent_did requester_did session_id
        }} }}"#
            ))
            .await;
        ensure_no_errors(&response, "Grok descendant usage owners")?;
        let rows: Vec<AgentRequestRow> = serde_json::from_value(
            response
                .data
                .as_ref()
                .and_then(|data| data.get("AgentRequest"))
                .cloned()
                .context("missing descendant usage owners")?,
        )?;
        for row in rows {
            owners
                .entry(row.request_id.clone())
                .and_modify(|owner| *owner = None)
                .or_insert(Some(row));
        }
    }
    Ok(owners)
}

pub(super) async fn session_usage(
    node: &EmbeddedNode,
    principal: &str,
    behavior: &str,
    session: &str,
) -> Result<Value> {
    let roots = super::sessions::load(node, principal, behavior, session).await?;
    let root =
        load_session_inference_observation(node, principal, Some(principal), session).await?;
    let mut totals = Totals::default();
    totals.add(&root.token_usage);
    totals.incomplete |= roots.iter().any(|row| !row.is_terminal());
    let mut scopes = BTreeSet::from([(
        principal.to_owned(),
        Some(principal.to_owned()),
        session.to_owned(),
    )]);
    for parent in roots {
        let mut query = gents::DescendantQuery::all(&parent.request_id);
        query.limit = gents::MAX_DESCENDANT_PAGE_LIMIT;
        loop {
            let page =
                gents::resolve_descendant_graph(gents::DescendantGraphAccess::Local(node), &query)
                    .await?;
            let owners = descendant_owners(
                node,
                page.edges
                    .iter()
                    .filter(|edge| edge.readable())
                    .map(|edge| edge.child_request_id.clone())
                    .collect(),
            )
            .await?;
            for edge in page.edges {
                totals.incomplete |= !edge.is_terminal();
                if !edge.readable() {
                    totals.incomplete = true;
                    continue;
                }
                let Some(Some(owner)) = owners.get(&edge.child_request_id) else {
                    totals.incomplete = true;
                    continue;
                };
                let (Some(agent), Some(child_session)) =
                    (owner.agent_did.as_deref(), owner.session_id.as_deref())
                else {
                    totals.incomplete = true;
                    continue;
                };
                if edge.principal_did.as_deref() != Some(agent)
                    || edge.child_session_id.as_deref() != Some(child_session)
                {
                    totals.incomplete = true;
                    continue;
                }
                if scopes.insert((
                    agent.to_owned(),
                    owner.requester_did.clone(),
                    child_session.to_owned(),
                )) {
                    let child = load_session_inference_observation(
                        node,
                        agent,
                        owner.requester_did.as_deref(),
                        child_session,
                    )
                    .await?;
                    totals.add(&child.token_usage);
                }
            }
            if !page.has_more {
                break;
            }
            query.after = page.next_cursor;
        }
    }
    Ok(totals.wire(root.inference_turns))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn descendant_owner_batches_cover_all_pages_and_preserve_requesters() {
        let directory = tempfile::tempdir().unwrap();
        let node = EmbeddedNode::builder()
            .data_path(directory.path().join("node"))
            .with_storage_backend(gents::defra_node::StorageBackend::Regolith)
            .build()
            .await
            .unwrap();
        gents::schema::ensure_runtime_schemas(&node).await.unwrap();
        assert!(descendant_owners(&node, BTreeSet::new())
            .await
            .unwrap()
            .is_empty());
        let mut ids = BTreeSet::new();
        for index in 0..129 {
            let id = format!("child-{index:03}");
            let requester = if index % 2 == 0 {
                "null"
            } else {
                "\"did:test:requester\""
            };
            let response = node
                .execute(&format!(
                    r#"mutation {{create_AgentRequest(input: {{
                request_id:"{id}", agent_did:"did:test:child", requester_did:{requester},
                session_id:"child-session"
            }}) {{_docID}}}}"#
                ))
                .await;
            ensure_no_errors(&response, "seed batched owner").unwrap();
            ids.insert(id);
        }
        ids.insert("missing-child".into());
        for principal in ["did:test:first", "did:test:second", "did:test:third"] {
            let response = node
                .execute(&format!(
                    r#"mutation {{create_AgentRequest(input: {{
                request_id:"ambiguous-child", agent_did:"{principal}", session_id:"other-session"
            }}) {{_docID}}}}"#
                ))
                .await;
            ensure_no_errors(&response, "seed ambiguous owner").unwrap();
        }
        ids.insert("ambiguous-child".into());
        let owners = descendant_owners(&node, ids).await.unwrap();
        assert_eq!(owners.len(), 130);
        assert!(!owners.contains_key("missing-child"));
        assert!(owners["ambiguous-child"].is_none());
        assert_eq!(
            owners["child-001"]
                .as_ref()
                .unwrap()
                .requester_did
                .as_deref(),
            Some("did:test:requester")
        );
        assert_eq!(owners["child-128"].as_ref().unwrap().requester_did, None);
        assert_eq!(
            owners["child-128"].as_ref().unwrap().agent_did.as_deref(),
            Some("did:test:child")
        );
    }

    #[test]
    fn context_info_uses_runtime_groups_and_distinguishes_missing_accounting() {
        let mut observation = gents::toolset::SessionInferenceObservation {
            token_usage: SessionTokenUsage::default(),
            inference_turns: Some(0),
            latest_context: None,
            latest_completion_tokens: None,
        };
        assert_eq!(
            context_info(&observation, 2000).unwrap()["freeTokens"],
            2000
        );
        observation.token_usage.model_calls = 1;
        assert!(context_info(&observation, 2000).is_err());
        observation.latest_context = Some(serde_json::from_value(json!({
            "request_id":"r", "call_id":"c", "call_sequence":1, "queued_at":"2026-09-01T12:00:00Z",
            "accounting":{"accounting_version":1, "estimator":"provider-wire", "components":{
                "messages":800, "tool_schemas":100, "documents":50, "additional_parameters":20, "output_schema":30},
                "estimated_input_tokens":1000, "context_window":2000,
                "compaction_threshold_basis_points":7500, "compaction_threshold_tokens":1500,
                "compaction_reason":"below_threshold"}
        })).unwrap());
        observation.latest_completion_tokens = Some(100);
        let info = context_info(&observation, 9000).unwrap();
        assert_eq!(info["used"], 1100);
        assert_eq!(info["total"], 2000);
        assert_eq!(info["freeTokens"], 900);
        assert_eq!(info["usagePct"], 55);
        assert_eq!(info["autoCompactThresholdPercent"], 75);
        assert_eq!(info["toolDefinitionsTokens"], 100);
        assert!(info.get("systemPromptTokens").is_none());
        assert_eq!(
            info["usageCategories"]
                .as_array()
                .unwrap()
                .iter()
                .map(|category| category["tokens"].as_u64().unwrap())
                .sum::<u64>(),
            1100
        );
    }

    #[test]
    fn usage_keeps_cache_inside_input_and_never_invents_cost() {
        let mut totals = Totals::default();
        totals.add(&SessionTokenUsage {
            input_tokens: Some(100),
            output_tokens: Some(20),
            cached_input_tokens: Some(70),
            model_calls: 2,
            api_duration_ms: Some(1250),
            ..Default::default()
        });
        totals.add(&SessionTokenUsage {
            model_calls: 1,
            incomplete: true,
            ..Default::default()
        });
        let value = totals.wire(Some(1));
        assert_eq!(value["usage"]["totalTokens"], 120);
        assert_eq!(value["usage"]["cachedReadTokens"], 70);
        assert_eq!(value["usage"]["usageIsIncomplete"], true);
        assert_eq!(value["usage"]["modelCalls"], 3);
        assert_eq!(value["usage"]["apiDurationMs"], 1250);
        assert_eq!(value["_meta"]["gents/durationIsIncomplete"], true);
        assert!(value["usage"].get("costUsdTicks").is_none());
        assert_eq!(value["usage"]["costIsPartial"], true);
    }
}
