//! Shared P2P collection profile definitions.

use std::collections::BTreeSet;

use anyhow::{bail, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum P2pCollectionProfile {
    Runtime,
    Agent,
    DesktopConfig,
    ChatRequests,
    ToolServices,
    Discovery,
}

impl P2pCollectionProfile {
    pub fn id(self) -> &'static str {
        match self {
            P2pCollectionProfile::Runtime => "runtime",
            P2pCollectionProfile::Agent => "agent",
            P2pCollectionProfile::DesktopConfig => "desktop-config",
            P2pCollectionProfile::ChatRequests => "chat-requests",
            P2pCollectionProfile::ToolServices => "tool-services",
            P2pCollectionProfile::Discovery => "discovery",
        }
    }

    pub fn from_id(id: &str) -> Option<Self> {
        match id.trim() {
            "runtime" => Some(P2pCollectionProfile::Runtime),
            "agent" => Some(P2pCollectionProfile::Agent),
            "desktop-config" => Some(P2pCollectionProfile::DesktopConfig),
            "chat-requests" => Some(P2pCollectionProfile::ChatRequests),
            "tool-services" => Some(P2pCollectionProfile::ToolServices),
            "discovery" => Some(P2pCollectionProfile::Discovery),
            _ => None,
        }
    }

    pub fn collection_names(self) -> &'static [&'static str] {
        match self {
            P2pCollectionProfile::Runtime => RUNTIME_COLLECTIONS,
            P2pCollectionProfile::Agent => AGENT_COLLECTIONS,
            P2pCollectionProfile::DesktopConfig => DESKTOP_CONFIG_COLLECTIONS,
            P2pCollectionProfile::ChatRequests => CHAT_REQUEST_COLLECTIONS,
            P2pCollectionProfile::ToolServices => TOOL_SERVICE_COLLECTIONS,
            P2pCollectionProfile::Discovery => DISCOVERY_COLLECTIONS,
        }
    }
}

pub fn expand_p2p_collection_profile_ids<'a>(
    explicit_collections: impl IntoIterator<Item = &'a str>,
    profile_ids: impl IntoIterator<Item = &'a str>,
) -> Result<BTreeSet<String>> {
    let mut collections = explicit_collections
        .into_iter()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();

    for profile_id in profile_ids {
        let profile_id = profile_id.trim();
        if profile_id.is_empty() {
            continue;
        }
        let profile = P2pCollectionProfile::from_id(profile_id)
            .ok_or_else(|| anyhow::anyhow!("unknown P2P collection profile {profile_id:?}"))?;
        for collection in profile.collection_names() {
            collections.insert((*collection).to_string());
        }
    }

    if collections.is_empty() {
        bail!("provide at least one --collection or --profile");
    }

    Ok(collections)
}

const RUNTIME_COLLECTIONS: &[&str] = &[
    "AgentPrincipal",
    "AgentBehavior",
    "AgentRuntime",
    "ToolSelection",
    "InferenceProfile",
    "InferenceBackend",
    "AgentConversation",
    "AgentRequest",
    "AgentResponse",
    "AgentResponseOutcome",
    "AgentToolResult",
    "AgentToolApproval",
    "AgentSession",
    "AgentMessage",
    "AgentToolCall",
    "CompactionEntry",
    "ProjectionAcpBinding",
    "Task",
    "Schedule",
    "ToolServiceRegistry",
];

const AGENT_COLLECTIONS: &[&str] = &[
    "AgentPrincipal",
    "AgentBehavior",
    "AgentRuntime",
    "ToolSelection",
    "InferenceBackend",
    "InferenceProfile",
];

const DESKTOP_CONFIG_COLLECTIONS: &[&str] = &[
    "AgentPrincipal",
    "AgentBehavior",
    "ToolSelection",
    "InferenceBackend",
    "InferenceProfile",
    "ToolServiceRegistry",
    "Task",
    "Schedule",
];

const CHAT_REQUEST_COLLECTIONS: &[&str] = &[
    "AgentConversation",
    "AgentRequest",
    "AgentResponse",
    "AgentResponseOutcome",
    "AgentToolResult",
    "AgentToolApproval",
    "AgentSession",
    "AgentMessage",
    "AgentToolCall",
    "CompactionEntry",
];

const TOOL_SERVICE_COLLECTIONS: &[&str] = &["ToolServiceRegistry"];

const DISCOVERY_COLLECTIONS: &[&str] = &[
    "PeerRegistry",
    "AgentPrincipal",
    "AgentBehavior",
    "AgentRuntime",
    "ToolSelection",
    "InferenceBackend",
    "InferenceProfile",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expands_profiles_and_dedupes_explicit_collections() {
        let collections = expand_p2p_collection_profile_ids(
            [" AgentRequest ", "AgentRequest", ""],
            ["chat-requests", "tool-services"],
        )
        .unwrap();

        assert!(collections.contains("AgentRequest"));
        assert!(collections.contains("AgentResponse"));
        assert!(collections.contains("ToolServiceRegistry"));
        assert_eq!(
            collections
                .iter()
                .filter(|name| name.as_str() == "AgentRequest")
                .count(),
            1
        );
    }

    #[test]
    fn rejects_unknown_profile() {
        let error = expand_p2p_collection_profile_ids([], ["unknown"]).unwrap_err();
        assert!(error.to_string().contains("unknown P2P collection profile"));
    }

    #[test]
    fn ignores_empty_profile_ids() {
        let collections = expand_p2p_collection_profile_ids(["AgentRequest"], ["", "  "]).unwrap();
        assert_eq!(
            collections,
            ["AgentRequest".to_string()].into_iter().collect()
        );
    }
}
