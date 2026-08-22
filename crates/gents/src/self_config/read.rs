//! Effective-config read assembly for `get_my_config` (#654).
//!
//! Reads the durable documents for behavior, tool selection, profile, backend,
//! owned skills, and automation inside one identity-scoped transaction, so
//! the projection is a consistent snapshot and DefraDB ACP governs visibility.
//! Documents are the truth being reported; the running slot may still be on
//! the previous generation (see [`super::core::EFFECT_TIMING_NOTE`]).
//!
//! `InferenceBackend.api_key` is never selected (`read_doc_in_txn` excludes
//! it), so the secret cannot round-trip through this surface.

use anyhow::Result;
use serde_json::{json, Map, Value};

use crate::config_client::patch::SelfConfigTarget;
use crate::config_client::ConfigApplyTxn;
use crate::graphql::escape_graphql_string;

use super::ops::{BehaviorAnchor, SelfConfigCore, EFFECT_TIMING_NOTE};

impl SelfConfigCore {
    /// Assemble the effective configuration projection.
    pub(crate) async fn read_effective_config(
        &self,
        categories: &std::collections::BTreeSet<String>,
        no_lockout: bool,
        dry_run: bool,
    ) -> Result<Value> {
        let txn = self.begin_txn().await?;
        let result = self
            .read_in_txn(&txn, categories, no_lockout, dry_run)
            .await;
        let _ = txn.discard().await;
        result
    }

    async fn read_in_txn(
        &self,
        txn: &ConfigApplyTxn<'_>,
        categories: &std::collections::BTreeSet<String>,
        no_lockout: bool,
        dry_run: bool,
    ) -> Result<Value> {
        let anchor = self.load_behavior_anchor(txn).await?;

        let selection = match anchor.ref_id("tool_selection_id") {
            Some(id) => doc_or_missing(txn, SelfConfigTarget::ToolSelection, &id).await?,
            None => json!({ "unset": true }),
        };
        let profile = match anchor.ref_id("inference_profile_id") {
            Some(id) => doc_or_missing(txn, SelfConfigTarget::InferenceProfile, &id).await?,
            None => json!({ "unset": true }),
        };
        let backend = match anchor.ref_id("backend_id") {
            Some(id) => doc_or_missing(txn, SelfConfigTarget::InferenceBackend, &id).await?,
            None => json!({ "unset": true }),
        };

        let skills = self.owned_skills(txn).await?;
        let (tasks, schedules, event_triggers) = self.owned_automation(txn).await?;

        Ok(json!({
            "agent_did": self.agent_did(),
            "behavior_id": self.behavior_id(),
            "behavior": Value::Object(anchor.doc.clone()),
            "tool_selection": selection,
            "inference_profile": profile,
            "inference_backend": backend,
            "skills": skills,
            "automation": {
                "tasks": tasks,
                "schedules": schedules,
                "event_triggers": event_triggers,
            },
            "self_config": {
                "categories": categories,
                "no_lockout": no_lockout,
                "dry_run": dry_run,
            },
            "effect_timing": EFFECT_TIMING_NOTE,
        }))
    }

    async fn owned_skills(&self, txn: &ConfigApplyTxn<'_>) -> Result<Value> {
        let query = format!(
            r#"{{
                Skill(filter: {{ agent_did: {{ _eq: "{agent_did}" }} }}, order: {{ skill_id: ASC }}) {{
                    skill_id
                    name
                    scope
                    enabled
                    description
                }}
            }}"#,
            agent_did = escape_graphql_string(self.agent_did()),
        );
        let response = txn.execute(&query).await?;
        Ok(response
            .get("data")
            .and_then(|data| data.get("Skill"))
            .cloned()
            .unwrap_or(Value::Array(Vec::new())))
    }

    /// Automation owned by this behavior: `Task.behavior_id == behavior_id`,
    /// then schedules/triggers whose `task_id` is one of the owned tasks (the
    /// canonical reachability rule — these collections carry no `agent_did`).
    pub(crate) async fn owned_automation(
        &self,
        txn: &ConfigApplyTxn<'_>,
    ) -> Result<(Vec<Value>, Vec<Value>, Vec<Value>)> {
        let tasks_query = format!(
            r#"{{
                Task(filter: {{ behavior_id: {{ _eq: "{behavior_id}" }} }}, order: {{ task_id: ASC }}) {{
                    task_id
                    name
                    description
                    behavior_id
                    prompt_template
                    enabled
                    output_schema_ref
                }}
            }}"#,
            behavior_id = escape_graphql_string(self.behavior_id()),
        );
        let response = txn.execute(&tasks_query).await?;
        let tasks = rows(&response, "Task");
        let task_ids: Vec<String> = tasks
            .iter()
            .filter_map(|task| task.get("task_id").and_then(Value::as_str))
            .map(ToOwned::to_owned)
            .collect();

        let mut schedules = Vec::new();
        let mut event_triggers = Vec::new();
        for task_id in &task_ids {
            let task_id = escape_graphql_string(task_id);
            let schedule_query = format!(
                r#"{{
                    Schedule(filter: {{ task_id: {{ _eq: "{task_id}" }} }}, order: {{ schedule_id: ASC }}) {{
                        schedule_id
                        task_id
                        interval_secs
                        cron
                        timezone
                        missed_run_policy
                        enabled
                        concurrency
                    }}
                }}"#,
            );
            schedules.extend(rows(&txn.execute(&schedule_query).await?, "Schedule"));
            let trigger_query = format!(
                r#"{{
                    EventTrigger(filter: {{ task_id: {{ _eq: "{task_id}" }} }}, order: {{ trigger_id: ASC }}) {{
                        trigger_id
                        task_id
                        source_collection
                        event_kind
                        filter
                        correlation_field
                        fire_mode
                        expected_count
                        expected_count_field
                        group_timeout_secs
                        group_min_count
                        workspace_authority
                        enabled
                        concurrency
                    }}
                }}"#,
            );
            event_triggers.extend(rows(&txn.execute(&trigger_query).await?, "EventTrigger"));
        }

        Ok((tasks, schedules, event_triggers))
    }

    /// Whether `task_id` belongs to this behavior (for schedule/trigger
    /// linkage validation).
    pub(crate) async fn task_owned(
        &self,
        txn: &ConfigApplyTxn<'_>,
        _anchor: &BehaviorAnchor,
        task_id: &str,
    ) -> Result<bool> {
        let Some((_, task)) =
            crate::config_client::patch::read_doc_in_txn(txn, SelfConfigTarget::Task, task_id)
                .await?
        else {
            return Ok(false);
        };
        Ok(task.get("behavior_id").and_then(Value::as_str) == Some(self.behavior_id()))
    }
}

async fn doc_or_missing(
    txn: &ConfigApplyTxn<'_>,
    target: SelfConfigTarget,
    unique_value: &str,
) -> Result<Value> {
    Ok(
        match crate::config_client::patch::read_doc_in_txn(txn, target, unique_value).await? {
            Some((_, doc)) => Value::Object(doc),
            None => json!({ "missing_reference": unique_value }),
        },
    )
}

fn rows(response: &Value, collection: &str) -> Vec<Value> {
    response
        .get("data")
        .and_then(|data| data.get(collection))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

/// Convenience: a `Map` from optional rows.
#[allow(dead_code)]
pub(crate) fn object(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(map) => map,
        _ => Map::new(),
    }
}
