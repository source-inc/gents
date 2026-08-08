mod behavior;
mod event_trigger;
mod graphql_fields;
mod inference_profile;
mod principal;
mod schedule;
mod serde_helpers;
mod skill;
mod subagent_target;
mod task;
mod tool_selection;

pub use principal::{load_agent_principal, upsert_agent_principal, AgentPrincipal};
pub(crate) use principal::{load_agent_principal_at_cid, load_agent_principal_by_doc_id};

use behavior::create_default_behavior;
#[allow(unused_imports)]
pub(crate) use behavior::{
    list_agent_behavior_records, load_agent_behavior_at_cid, load_agent_behavior_by_doc_id,
    load_agent_behavior_record,
};
pub use behavior::{
    list_agent_behaviors, load_agent_behavior, upsert_agent_behavior, AgentBehavior,
};

pub use inference_profile::{
    default_inference_profile_id_for_behavior, list_inference_profile_records,
    load_inference_profile, upsert_inference_profile, InferenceProfile,
};
#[allow(unused_imports)]
pub(crate) use inference_profile::{
    load_inference_profile_at_cid, load_inference_profile_by_doc_id, load_inference_profile_record,
};

pub use tool_selection::default_tool_selection_id_for_behavior;
pub use tool_selection::{
    is_reserved_builtin_tool_name, load_tool_selection, upsert_tool_selection,
    wide_open_tool_selection_document, wide_open_tool_selection_id_for_agent,
    ToolSelectionDocument, WriteToolDecl, WriteToolField,
};
#[allow(unused_imports)]
pub(crate) use tool_selection::{
    list_all_tool_selection_records, list_tool_selection_records, load_tool_selection_at_cid,
    load_tool_selection_by_doc_id, load_tool_selection_record,
};

pub use subagent_target::{subagent_target_entry, SubagentTarget};

#[allow(unused_imports)]
pub(crate) use skill::{
    list_skill_records, load_skill_at_cid, load_skill_by_doc_id, SkillDocument,
};

#[allow(unused_imports)]
pub(crate) use event_trigger::{
    list_event_trigger_records, load_event_trigger_by_doc_id, update_event_trigger_runtime_fields,
    EventTrigger, EventTriggerRuntimeUpdate,
};
#[allow(unused_imports)]
pub(crate) use schedule::{
    list_schedule_records, load_schedule_by_doc_id, load_schedule_next_run_at,
    update_schedule_runtime_fields, Schedule, ScheduleRuntimeUpdate,
};
#[allow(unused_imports)]
pub(crate) use task::{list_task_records, load_task_by_doc_id, Task};

use anyhow::{anyhow, Result};
use defra_node::EmbeddedNode;

#[derive(Debug, Clone, PartialEq)]
pub struct PrincipalBootstrap {
    pub principal: AgentPrincipal,
    pub default_behavior: AgentBehavior,
    pub default_inference_profile: InferenceProfile,
    pub created_principal: bool,
    pub created_default_behavior: bool,
    pub created_default_inference_profile: bool,
}

pub fn default_behavior_id_for_agent(agent_did: &str) -> String {
    format!("{agent_did}:default")
}

pub async fn ensure_agent_principal(
    node: &EmbeddedNode,
    agent_did: &str,
) -> Result<PrincipalBootstrap> {
    let existing_principal = load_agent_principal(node, agent_did).await?;
    let (default_behavior_id, created_principal) = match existing_principal.as_ref() {
        Some(principal) => {
            let behavior_id =
                serde_helpers::normalize_optional_string(principal.default_behavior_id.as_deref())
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| default_behavior_id_for_agent(agent_did));
            (behavior_id, false)
        }
        None => (default_behavior_id_for_agent(agent_did), true),
    };

    let default_inference_profile_id =
        inference_profile::default_inference_profile_id_for_behavior(&default_behavior_id);

    let mut created_profile_with_default_behavior = false;
    let (mut default_behavior, created_default_behavior) = match load_agent_behavior(
        node,
        &default_behavior_id,
    )
    .await?
    {
        Some(behavior) => {
            if behavior.agent_did != agent_did {
                return Err(anyhow!(
                    "AgentBehavior {default_behavior_id} belongs to {} not {agent_did}",
                    behavior.agent_did
                ));
            }
            (behavior, false)
        }
        None => {
            if existing_principal
                .as_ref()
                .and_then(|principal| {
                    serde_helpers::normalize_optional_string(
                        principal.default_behavior_id.as_deref(),
                    )
                })
                .is_some()
            {
                return Err(anyhow!(
                    "AgentPrincipal {agent_did} references missing default behavior {default_behavior_id}"
                ));
            }

            let profile =
                inference_profile::create_default_inference_profile(node, &default_behavior_id)
                    .await?;
            created_profile_with_default_behavior = true;
            create_default_behavior(node, agent_did, &default_behavior_id, &profile.profile_id)
                .await?;
            let behavior = load_agent_behavior(node, &default_behavior_id)
                .await?
                .ok_or_else(|| {
                    anyhow!("default behavior {default_behavior_id} was not persisted")
                })?;
            (behavior, true)
        }
    };

    let (default_inference_profile, created_default_inference_profile) =
        match load_inference_profile(node, &default_inference_profile_id).await? {
            Some(profile) => (profile, created_profile_with_default_behavior),
            None => (
                inference_profile::create_default_inference_profile(node, &default_behavior_id)
                    .await?,
                true,
            ),
        };

    if serde_helpers::normalize_optional_string(default_behavior.inference_profile_id.as_deref())
        .is_none()
    {
        default_behavior.inference_profile_id = Some(default_inference_profile.profile_id.clone());
        upsert_agent_behavior(node, &default_behavior).await?;
        default_behavior = load_agent_behavior(node, &default_behavior_id)
            .await?
            .ok_or_else(|| anyhow!("default behavior {default_behavior_id} was not persisted"))?;
    }

    match existing_principal {
        Some(principal) => {
            if serde_helpers::normalize_optional_string(principal.default_behavior_id.as_deref())
                .is_none()
            {
                let fallback_display_name = serde_helpers::default_display_name_for_did(agent_did);
                upsert_agent_principal(
                    node,
                    agent_did,
                    principal
                        .display_name
                        .as_deref()
                        .or(Some(fallback_display_name.as_str())),
                    Some(&default_behavior_id),
                    principal.enabled,
                )
                .await?;
            }
        }
        None => {
            let fallback_display_name = serde_helpers::default_display_name_for_did(agent_did);
            upsert_agent_principal(
                node,
                agent_did,
                Some(fallback_display_name.as_str()),
                Some(&default_behavior_id),
                true,
            )
            .await?;
        }
    }

    let principal = load_agent_principal(node, agent_did)
        .await?
        .ok_or_else(|| anyhow!("AgentPrincipal {agent_did} was not persisted"))?;

    Ok(PrincipalBootstrap {
        principal,
        default_behavior,
        default_inference_profile,
        created_principal,
        created_default_behavior,
        created_default_inference_profile,
    })
}

#[cfg(test)]
mod tests;
