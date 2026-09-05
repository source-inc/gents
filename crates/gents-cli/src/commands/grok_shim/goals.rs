//! Native goal-panel hydration from the runtime's canonical goal owner.
//! Only the last successfully delivered observation is connection-local.
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use defra_node::EmbeddedNode;
use gents::goal::{GoalDocument, GoalStatus};
use serde_json::{json, Value};

use super::projection::{ProjectionEngine, UpdateTimestamps};
use super::turn::{PromptSender, PromptSenderLine};

#[derive(Debug, PartialEq)]
pub(super) enum GoalCommand {
    Status,
    Pause,
    Resume,
    Clear,
    Create {
        objective: String,
        token_budget: Option<i64>,
    },
}

impl GoalCommand {
    pub(super) fn parse(text: &str) -> Result<Option<Self>> {
        let text = text.trim();
        let mut words = text.split_whitespace();
        if words.next() != Some("/goal") {
            return Ok(None);
        }
        let args = text.strip_prefix("/goal").unwrap().trim();
        let command = match args.to_ascii_lowercase().as_str() {
            "" | "status" => Self::Status,
            "pause" => Self::Pause,
            "resume" => Self::Resume,
            "clear" => Self::Clear,
            _ => {
                // Match stock Grok: only a trailing standalone positive
                // --budget value is syntax; other occurrences are prose.
                let mut objective = args.to_string();
                let mut token_budget = None;
                if let Some((head, tail)) = args.rsplit_once("--budget") {
                    let value = tail.trim();
                    if head.ends_with(char::is_whitespace)
                        && tail.starts_with(char::is_whitespace)
                        && !head.trim().is_empty()
                        && !value.is_empty()
                        && value.bytes().all(|byte| byte.is_ascii_digit())
                    {
                        if let Ok(budget) = value.parse::<i64>() {
                            if budget > 0 {
                                objective = head.trim_end().to_string();
                                token_budget = Some(budget);
                            }
                        }
                    }
                }
                Self::Create {
                    objective,
                    token_budget,
                }
            }
        };
        Ok(Some(command))
    }

    pub(super) fn from_prompt(request: &super::turn::PromptRequest) -> Result<Option<Self>> {
        if let [block] = request.prompt.as_slice() {
            if block.kind == "text"
                && block
                    .meta
                    .as_ref()
                    .is_none_or(|meta| meta.as_object().is_some_and(|meta| meta.is_empty()))
            {
                return Self::parse(&block.text);
            }
        }
        Ok(None)
    }

    /// Operator controls reuse the runtime transition/clear owners. The ACP
    /// caller must authorize the exact attached session before invoking this.
    pub(super) async fn execute(
        &self,
        node: &EmbeddedNode,
        principal: &str,
        session: &str,
    ) -> Result<String> {
        anyhow::ensure!(
            !matches!(self, Self::Create { .. }),
            "goal creation must use atomic request admission"
        );
        if *self == Self::Clear {
            let count = gents::goal::delete_goals_for_session(node, principal, session).await?;
            return Ok(if count == 0 {
                "No goal is set."
            } else {
                "Goal cleared."
            }
            .into());
        }
        let Some(mut goal) = gents::goal::load_canonical_goal(node, principal, session).await?
        else {
            return Ok("No goal is set.".into());
        };
        let status = match self {
            Self::Pause => Some(GoalStatus::Paused),
            Self::Resume => Some(GoalStatus::Active),
            _ => None,
        };
        if let Some(status) = status {
            let state = goal.state().context("unrecognized persisted goal state")?;
            // A legitimate but unavailable control is a host-command reply,
            // not a failed model turn inviting the user to retry endlessly.
            // The runtime's transition function remains the legality owner.
            if gents::goal::apply_operator_status_transition(state, status).is_err() {
                return Ok(format!(
                    "Goal remains {}. This control is not available in that state.",
                    goal.status
                ));
            }
            goal =
                gents::goal::set_goal(node, principal, session, None, Some(status), None).await?;
        }
        goal.tokens_used = Some(gents::goal::session_token_usage(node, principal, session).await?);
        Ok(format!(
            "Goal: {}\nStatus: {}\nTokens used: {}{}",
            goal.objective,
            goal.status,
            goal.tokens_used.unwrap_or_default().max(0),
            goal.token_budget
                .map(|budget| format!(" / {budget}"))
                .unwrap_or_default()
        ))
    }
}

#[derive(Default)]
pub(super) struct GoalCursor {
    delivered: Option<Value>,
}

impl GoalCursor {
    /// The session observer has already authorized the attached root session.
    /// No refresh_goal_usage/set_goal calls: observation must not mutate goals.
    pub(super) async fn refresh(
        &mut self,
        node: &EmbeddedNode,
        principal: &str,
        session: &str,
        sender: &PromptSender,
        projections: &ProjectionEngine,
    ) -> Result<()> {
        let mut goal = gents::goal::load_canonical_goal(node, principal, session).await?;
        if let Some(goal) = goal.as_mut() {
            // The stored Goal counter is a scheduler checkpoint. Inference can
            // finish after update_goal marks it complete, when the scheduler
            // no longer refreshes it. Read the runtime's budget calculation;
            // never repair persisted state from this observer.
            goal.tokens_used =
                Some(gents::goal::session_token_usage(node, principal, session).await?);
        }
        // Compare the rendered observation, including runtime-derived active
        // time, rather than only fields stored in the scheduler checkpoint.
        let observed = goal
            .as_ref()
            .map(|goal| project(goal, Utc::now()))
            .transpose()?;
        if observed == self.delivered {
            return Ok(());
        }
        let update = match observed.as_ref() {
            Some(update) => update.clone(),
            None => {
                let id = self
                    .delivered
                    .as_ref()
                    .and_then(|row| row["goal_id"].as_str())
                    .context("missing previously delivered goal identity")?;
                base_update(id, "", "cleared", 0)
            }
        };
        projections
            .session_updates()
            .send(
                session,
                |event_id, total_tokens| {
                    Ok(super::projection::session_notification_for_method(
                        "x.ai/session_notification",
                        session,
                        update,
                        super::projection::stamp_update_meta(
                            event_id,
                            total_tokens,
                            None,
                            None,
                            UpdateTimestamps::default(),
                        ),
                    ))
                },
                PromptSenderLine(sender),
            )
            .await?;
        self.delivered = observed;
        Ok(())
    }
}

/// Stock permanently suppresses a cleared wire ID. Gents logical goal IDs
/// are reused across clear/create, so project the stable physical incarnation
/// instead. No synthesized generation counter or second identity owner.
fn wire_goal_id(doc_id: &str) -> String {
    format!("gents:{doc_id}")
}

fn base_update(id: &str, objective: &str, status: &str, elapsed_ms: u64) -> Value {
    // Grok's optional worker/verifier orchestration is not used by Gents'
    // durable goal owner. Do not reinterpret continuation attempts as rounds.
    json!({"sessionUpdate":"goal_updated", "goal_id":id, "objective":objective,
        "status":status, "phase":"idle", "elapsed_ms":elapsed_ms,
        "total_deliverables":0, "completed_deliverables":0,
        "total_worker_rounds":0, "total_verify_rounds":0})
}

fn project(goal: &GoalDocument, now: DateTime<Utc>) -> Result<Value> {
    let status = match goal
        .parsed_status()
        .context("unrecognized persisted goal status")?
    {
        GoalStatus::Active => "active",
        GoalStatus::Paused => "user_paused",
        GoalStatus::Blocked => "blocked",
        // Stock explicitly renders unfamiliar status strings as paused.
        // Preserve the real cause rather than claiming an infrastructure
        // failure or a token-budget limit; pause_message explains it.
        GoalStatus::UsageLimited => "usage_limited",
        GoalStatus::BudgetLimited => "budget_limited",
        GoalStatus::Complete => "complete",
    };
    let elapsed_ms = (goal.current_active_time_seconds(now) as u64).saturating_mul(1000);
    let mut update = base_update(
        &wire_goal_id(&goal.doc_id),
        &goal.objective,
        status,
        elapsed_ms,
    );
    update["tokens_used"] = json!(goal.tokens_used.unwrap_or_default().max(0));
    if let Some(budget) = goal.token_budget {
        update["token_budget"] = json!(budget);
    }
    let reason = if goal.parsed_status() == Some(GoalStatus::UsageLimited) {
        Some("Gents usage limit reached; waiting for runtime admission".to_owned())
    } else {
        goal.last_blocked_reason
            .clone()
            .or_else(|| goal.last_failure.clone())
    };
    if let Some(reason) = reason {
        update["pause_message"] = json!(reason);
    }
    update["_meta"] = json!({"gents/goalId":goal.goal_id, "gents/goalStatus":goal.status,
        "gents/continuationSequence":goal.continuation_sequence(),
        "gents/goalOrchestration":"runtime-owned; no stock worker/verifier phases"});
    Ok(update)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn goal_controls_are_exact_commands_not_prompt_substrings() {
        for (text, expected) in [
            ("/goal", GoalCommand::Status),
            (" /goal STATUS ", GoalCommand::Status),
            ("/goal pause", GoalCommand::Pause),
            ("/goal resume", GoalCommand::Resume),
            ("/goal clear", GoalCommand::Clear),
        ] {
            assert_eq!(GoalCommand::parse(text).unwrap(), Some(expected));
        }
        for text in ["Explain /goal pause", "/goals pause", "hello"] {
            assert_eq!(GoalCommand::parse(text).unwrap(), None);
        }
        for (text, objective, token_budget) in [
            ("/goal pause extra", "pause extra", None),
            ("/goal build something", "build something", None),
            (
                "/goal read\nthen explain --budget 123",
                "read\nthen explain",
                Some(123),
            ),
            ("/goal explain --budget 0", "explain --budget 0", None),
            (
                "/goal explain --budget 12 later",
                "explain --budget 12 later",
                None,
            ),
            ("/goal explain--budget 12", "explain--budget 12", None),
            (
                "/goal explain --budget 999999999999999999999",
                "explain --budget 999999999999999999999",
                None,
            ),
        ] {
            assert_eq!(
                GoalCommand::parse(text).unwrap(),
                Some(GoalCommand::Create {
                    objective: objective.into(),
                    token_budget,
                })
            );
        }
    }

    #[tokio::test]
    async fn goal_observation_is_scoped_read_only_and_retries_failed_delivery() {
        use super::super::projection::BoundModelContext;
        use gents::graphql::ensure_no_errors;
        use std::sync::Arc;
        let directory = tempfile::tempdir().unwrap();
        let node = Arc::new(
            EmbeddedNode::builder()
                .data_path(directory.path().join("node"))
                .with_storage_backend(gents::defra_node::StorageBackend::Regolith)
                .build()
                .await
                .unwrap(),
        );
        gents::schema::ensure_runtime_schemas(&node).await.unwrap();
        let projections = ProjectionEngine::new(
            node.clone(),
            BoundModelContext::new("model".into(), "Model".into(), 1000),
        );
        let buffer = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let sender = PromptSender::Buffer {
            buffer: buffer.clone(),
        };
        let mut cursor = GoalCursor::default();
        for (id, agent, session) in [
            ("owned", "principal", "session"),
            ("foreign-agent", "other", "session"),
            ("foreign-session", "principal", "other"),
        ] {
            let result = node.execute(&format!(r#"mutation {{create_Goal(input: {{
                goal_id:"{id}", agent_did:"{agent}", session_id:"{session}", objective:"Objective {id}",
                status:"active", tokens_used:12, active_time_seconds:3, created_at:"2026-09-01T00:00:00Z"
            }}) {{_docID}}}}"#)).await;
            ensure_no_errors(&result, "seed goal").unwrap();
        }
        let before = node
            .execute("{Goal {goal_id status tokens_used}}")
            .await
            .data;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        drop(rx);
        let failed = PromptSender::Live {
            outbound: super::super::server::AcpOutbound::for_frames(tx),
        };
        assert!(cursor
            .refresh(&node, "principal", "session", &failed, &projections)
            .await
            .is_err());
        assert!(cursor.delivered.is_none());
        cursor
            .refresh(&node, "principal", "session", &sender, &projections)
            .await
            .unwrap();
        cursor
            .refresh(&node, "principal", "session", &sender, &projections)
            .await
            .unwrap();
        assert_eq!(buffer.lock().await.len(), 1);
        let delivered: Value = serde_json::from_str(&buffer.lock().await[0]).unwrap();
        assert_eq!(
            delivered["params"]["update"]["_meta"]["gents/goalId"],
            "owned"
        );
        let first_wire_id = delivered["params"]["update"]["goal_id"].clone();
        assert_eq!(delivered["params"]["update"]["tokens_used"], 0);
        assert_eq!(
            before,
            node.execute("{Goal {goal_id status tokens_used}}")
                .await
                .data
        );
        let limited = node.execute(r#"mutation {update_Goal(filter:{goal_id:{_eq:"owned"}}, input:{status:"budget_limited"}) {_docID}}"#).await;
        ensure_no_errors(&limited, "budget-limited fixture").unwrap();
        let reply = GoalCommand::Pause
            .execute(&node, "principal", "session")
            .await
            .unwrap();
        assert!(reply.contains("Goal remains budget_limited"));
        assert_eq!(
            gents::goal::load_canonical_goal(&node, "principal", "session")
                .await
                .unwrap()
                .unwrap()
                .status,
            "budget_limited"
        );
        let removed = node
            .execute(r#"mutation {delete_Goal(filter:{goal_id:{_eq:"owned"}}) {_docID}}"#)
            .await;
        ensure_no_errors(&removed, "remove goal fixture").unwrap();
        cursor
            .refresh(&node, "principal", "session", &sender, &projections)
            .await
            .unwrap();
        cursor
            .refresh(&node, "principal", "session", &sender, &projections)
            .await
            .unwrap();
        let lines = buffer.lock().await;
        assert_eq!(lines.len(), 2);
        let cleared: Value = serde_json::from_str(&lines[1]).unwrap();
        assert_eq!(cleared["params"]["update"]["status"], "cleared");
        assert_eq!(cleared["params"]["update"]["goal_id"], first_wire_id);
        drop(lines);
        // Reusing a logical goal ID must not hit the stock pager's permanent
        // last_cleared_goal_id suppression for the preceding incarnation.
        let recreated = node.execute(r#"mutation {create_Goal(input:{
            goal_id:"owned", agent_did:"principal", session_id:"session", objective:"New incarnation",
            status:"active", created_at:"2026-09-02T00:00:00Z"
        }) {_docID}}"#).await;
        ensure_no_errors(&recreated, "recreate same logical goal").unwrap();
        cursor
            .refresh(&node, "principal", "session", &sender, &projections)
            .await
            .unwrap();
        let lines = buffer.lock().await;
        assert_eq!(lines.len(), 3);
        let next: Value = serde_json::from_str(&lines[2]).unwrap();
        assert_ne!(next["params"]["update"]["goal_id"], first_wire_id);
        assert_eq!(next["params"]["update"]["_meta"]["gents/goalId"], "owned");
        drop(lines);

        // A completed goal does not receive scheduler usage refreshes. Late
        // persisted inference must still update its panel, without changing
        // the canonical Goal row or depending on a Goal document event.
        for mutation in [
            r#"mutation {update_Goal(filter:{goal_id:{_eq:"owned"}}, input:{status:"complete",tokens_used:12}) {_docID}}"#,
            r#"mutation {create_AgentRequest(input:{request_id:"usage-request",agent_did:"principal",session_id:"session"}) {_docID}}"#,
            r#"mutation {create_InferenceCall(input:{call_id:"usage-call",request_id:"usage-request",agent_did:"principal",prompt_tokens:20,completion_tokens:3}) {_docID}}"#,
        ] {
            ensure_no_errors(&node.execute(mutation).await, "seed completed usage").unwrap();
        }
        cursor
            .refresh(&node, "principal", "session", &sender, &projections)
            .await
            .unwrap();
        let snapshot = node
            .execute("{Goal {goal_id status tokens_used}}")
            .await
            .data;
        let updated = node.execute(r#"mutation {update_InferenceCall(filter:{call_id:{_eq:"usage-call"}},input:{completion_tokens:8}) {_docID}}"#).await;
        ensure_no_errors(&updated, "late completion usage").unwrap();
        cursor
            .refresh(&node, "principal", "session", &sender, &projections)
            .await
            .unwrap();
        let lines = buffer.lock().await;
        assert_eq!(lines.len(), 5);
        let first: Value = serde_json::from_str(&lines[3]).unwrap();
        let late: Value = serde_json::from_str(&lines[4]).unwrap();
        assert_eq!(first["params"]["update"]["tokens_used"], 23);
        assert_eq!(late["params"]["update"]["tokens_used"], 28);
        assert_eq!(late["params"]["update"]["status"], "complete");
        drop(lines);
        let reply = GoalCommand::Status
            .execute(&node, "principal", "session")
            .await
            .unwrap();
        assert!(reply.contains("Tokens used: 28"), "{reply}");
        assert_eq!(
            snapshot,
            node.execute("{Goal {goal_id status tokens_used}}")
                .await
                .data
        );
    }

    #[test]
    fn native_goal_snapshot_preserves_budget_usage_and_runtime_active_time() {
        let mut goal: GoalDocument = serde_json::from_value(json!({
            "_docID":"physical-goal", "goal_id":"goal-1", "session_id":"s", "agent_did":"a",
            "objective":"Finish the feature", "status":"active", "token_budget":1000,
            "tokens_used":123, "active_time_seconds":10,
            "active_started_at":"2026-09-01T00:00:00Z", "continuation_sequence":4
        }))
        .unwrap();
        let now = "2026-09-01T00:00:05Z".parse().unwrap();
        let update = project(&goal, now).unwrap();
        assert_eq!(update["sessionUpdate"], "goal_updated");
        assert_eq!(update["elapsed_ms"], 15000);
        assert_eq!(update["tokens_used"], 123);
        assert_eq!(update["token_budget"], 1000);
        assert_eq!(update["total_worker_rounds"], 0);
        let later = project(&goal, now + chrono::Duration::seconds(1)).unwrap();
        assert_ne!(
            update, later,
            "active timer must advance without a Goal write"
        );
        assert_eq!(later["elapsed_ms"], 16000);
        assert_eq!(later["tokens_used"], update["tokens_used"]);
        for (runtime, native) in [
            ("paused", "user_paused"),
            ("blocked", "blocked"),
            ("usage_limited", "usage_limited"),
            ("budget_limited", "budget_limited"),
            ("complete", "complete"),
        ] {
            goal.status = runtime.into();
            let update = project(&goal, now).unwrap();
            assert_eq!(
                update,
                project(&goal, now + chrono::Duration::seconds(1)).unwrap(),
                "non-active goals must not accrue elapsed time"
            );
            assert_eq!(update["goal_id"], "gents:physical-goal");
            assert_eq!(update["status"], native);
            assert_eq!(update["_meta"]["gents/goalStatus"], runtime);
            assert_eq!(update["elapsed_ms"], 10000);
        }
        goal.status = "unknown".into();
        assert!(project(&goal, now).is_err());
    }
}
