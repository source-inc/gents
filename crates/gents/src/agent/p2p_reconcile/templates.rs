//! Scope-template catalog for P2P filtered replication.
//!
//! A `ScopeTemplate` is a named pairing intent: a fixed collection set, a
//! `Scope` (how per-peer document filtering is derived), and a `Delivery`
//! (push vs. replicate).  The catalog is static and hardcoded here; later
//! tasks will wire it into the pairing reconciler and defradb.rs #1033.
//!
//! `PairingFilters` is our own seam type that decouples this crate from the
//! unmerged defradb.rs #1033 filter API.  It holds per-collection single-field
//! equality predicates and can be translated by later tasks into whatever
//! upstream shape emerges.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Delivery mode for a template pairing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Delivery {
    /// Caller pushes documents to the peer.
    Push,
    /// Bidirectional replication.
    Replicate,
}

/// Scoping policy for per-peer document filtering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    /// Filter each collection on a single field that must equal the paired
    /// peer's DID.
    PeerDid {
        /// The field name on each collection document.
        field: &'static str,
    },
    /// No per-peer filtering — replicate all documents in the collection set.
    Unscoped,
    /// Explicit per-collection filter rules for directional pairings where
    /// different collections scope to different DID sources.
    PerCollection(&'static [CollectionRule]),
}

/// DID source for one per-collection filter rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DidSource {
    /// Use this node's local agent DID.
    LocalDid,
    /// Use the paired peer's agent DID.
    PeerDid,
    /// Use the DID that owns the pairing's authoritative projection.
    ///
    /// This is the local DID on the issuer/runtime side and the remote issuer
    /// DID on a bearer client. It lets both directions select the same
    /// issuer-owned rows without replicating another runtime's projection.
    HomeDid,
}

/// One exact per-collection filter rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CollectionRule {
    /// Collection this rule filters.
    pub collection: &'static str,
    /// Field name filtered on the collection.
    pub field: &'static str,
    /// DID source used as the filter value.
    pub source: DidSource,
}

/// A named pairing intent in the static catalog.
#[derive(Debug, Clone)]
pub struct ScopeTemplate {
    /// Stable identifier used to look up the template from CLI args or config.
    pub id: &'static str,
    /// The exact collection names included in this template.
    pub collections: &'static [&'static str],
    /// How to derive per-peer document filters.
    pub scope: Scope,
    /// Delivery mode for this pairing.
    pub delivery: Delivery,
}

// ---------------------------------------------------------------------------
// PairingFilters seam type (#1033-independent)
// ---------------------------------------------------------------------------

/// A single-field equality predicate for one collection.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FilterPredicate {
    /// The field name to filter on.
    pub field: String,
    /// The value the field must equal.
    pub value: String,
}

/// Per-collection filter predicates for a concrete pairing.
///
/// `key` = collection name, `value` = equality predicate to apply when
/// subscribing / pushing documents for that collection.  An empty map means
/// no filtering (Unscoped).
///
/// This type is our own seam that later tasks will translate into whatever
/// shape defradb.rs #1033 exposes.
pub type PairingFilters = BTreeMap<String, FilterPredicate>;

// ---------------------------------------------------------------------------
// Built-in template catalog
// ---------------------------------------------------------------------------

/// Conversation grants carry requester-scoped transcript artifacts plus the
/// small unfiltered configuration control plane needed to render and operate
/// the paired agent without a second transport.
const CONVERSATION_COLLECTIONS: &[&str] = &[
    "AgentRequest",
    "AgentResponse",
    "AgentResponseOutcome",
    "AgentMessage",
    "AgentToolCall",
    "AgentToolResult",
    "AgentToolApproval",
    "AgentSession",
    "AgentConversation",
    "CompactionEntry",
    "BearerPairingReady",
    "AgentBehavior",
    "ToolSelection",
    "InferenceBackend",
    "InferenceProfile",
    "ToolServiceRegistry",
    "Skill",
];

const CONVERSATION_TRANSCRIPT_COLLECTIONS: &[&str] = &[
    "AgentRequest",
    "AgentResponse",
    "AgentResponseOutcome",
    "AgentMessage",
    "AgentToolCall",
    "AgentToolResult",
    "AgentToolApproval",
    "AgentSession",
    "AgentConversation",
    "CompactionEntry",
    "BearerPairingReady",
];

const CONVERSATION_RULES: &[CollectionRule] = &[
    CollectionRule {
        collection: "AgentRequest",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentResponse",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentResponseOutcome",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentMessage",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentToolCall",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentToolResult",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentToolApproval",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentSession",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentConversation",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "CompactionEntry",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "BearerPairingReady",
        field: "claimant_did",
        source: DidSource::PeerDid,
    },
];

/// The fleet-discovery directory collection replicated by the `machine`
/// template (issue #714). Registered in `gents-schemas`; named here as a
/// literal because the catalog is deliberately dependency-free strings.
pub const AGENT_DIRECTORY_COLLECTION: &str = "AgentDirectoryEntry";

/// Machine template collections: the conversation plane plus the agent
/// directory. Order mirrors CONVERSATION_COLLECTIONS + the directory.
const MACHINE_COLLECTIONS: &[&str] = &[
    "AgentRequest",
    "AgentResponse",
    "AgentResponseOutcome",
    "AgentMessage",
    "AgentToolCall",
    "AgentToolResult",
    "AgentToolApproval",
    "AgentSession",
    "AgentConversation",
    "CompactionEntry",
    "BearerPairingReady",
    "AgentBehavior",
    "ToolSelection",
    "InferenceBackend",
    "InferenceProfile",
    "ToolServiceRegistry",
    "Skill",
    "PersonaConfigRequest",
    AGENT_DIRECTORY_COLLECTION,
];

const MACHINE_RULES: &[CollectionRule] = &[
    CollectionRule {
        collection: "AgentRequest",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentResponse",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentResponseOutcome",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentMessage",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentToolCall",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentToolResult",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentToolApproval",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentSession",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentConversation",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "CompactionEntry",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "BearerPairingReady",
        field: "claimant_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "PersonaConfigRequest",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: AGENT_DIRECTORY_COLLECTION,
        field: "source_did",
        source: DidSource::HomeDid,
    },
];

/// Agent-config collections: behavior + tool configuration.  Unscoped because
/// the operator wants the full config set replicated, not per-peer slices.
const AGENT_CONFIG_COLLECTIONS: &[&str] = &[
    "AgentBehavior",
    "ToolSelection",
    "InferenceBackend",
    "InferenceProfile",
    "ToolServiceRegistry",
    "Skill",
];

/// Discovery (network control-plane) collections: the membership documents a
/// joiner needs to learn and run the network, plus agent-config so it can run
/// what it discovers. Unscoped Replicate — small control-plane docs, not
/// per-peer slices. This is the bootstrap on-ramp template.
const DISCOVERY_COLLECTIONS: &[&str] = &[
    "AgentNetwork",
    "NetworkMembership",
    "PeerEndpoint",
    "NetworkJoinRequest",
    "AgentBehavior",
    "ToolSelection",
    "InferenceBackend",
    "InferenceProfile",
    "ToolServiceRegistry",
    "Skill",
];

/// Narrow network-control collections: the signed network-membership substrate
/// only. Layer-1 network-derived mesh edges use this instead of the broader
/// `discovery` bootstrap template so agent config is not re-replicated
/// fleet-wide.
pub const NETWORK_CONTROL_COLLECTIONS: &[&str] = &[
    "AgentNetwork",
    "NetworkMembership",
    "PeerEndpoint",
    "NetworkJoinRequest",
];

/// Coordinator → host leg for subagent delegation: carry only bridges
/// addressed to this host. Coordinator-owned parent requests are not
/// pair-specific and must not fan out across every host pairing (#683).
const SUBAGENT_COORDINATOR_COLLECTIONS: &[&str] = &["AgentToolCall"];

const SUBAGENT_COORDINATOR_RULES: &[CollectionRule] = &[CollectionRule {
    collection: "AgentToolCall",
    field: "spawn_target_did",
    source: DidSource::PeerDid,
}];

/// Host → coordinator leg for subagent completion: carry only artifacts whose
/// immutable requester route names this coordinator. This preserves child
/// returns without replaying unrelated host-owned conversation history.
const SUBAGENT_HOST_COLLECTIONS: &[&str] = &[
    "AgentRequest",
    "AgentResponse",
    "AgentResponseOutcome",
    "AgentMessage",
    "AgentToolCall",
    "AgentToolApproval",
];

const SUBAGENT_HOST_RULES: &[CollectionRule] = &[
    CollectionRule {
        collection: "AgentRequest",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentResponse",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentResponseOutcome",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentMessage",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentToolCall",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentToolApproval",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
];

/// Owner-authored scheduler recovery facts. Replication (rather than push)
/// backfills immutable admissions to late peers, while the owner-DID filter
/// prevents these control-plane facts from becoming participant gossip.
const SCHEDULER_OWNER_COLLECTIONS: &[&str] = &["EventTriggerActivation", "EventDeliveryAdmission"];

const SCHEDULER_OWNER_RULES: &[CollectionRule] = &[
    CollectionRule {
        collection: "EventTriggerActivation",
        field: "agent_did",
        source: DidSource::LocalDid,
    },
    CollectionRule {
        collection: "EventDeliveryAdmission",
        field: "agent_did",
        source: DidSource::LocalDid,
    },
];

pub const NETWORK_CONTROL_TEMPLATE: &str = "network-control";
pub const SUBAGENT_COORDINATOR_TEMPLATE: &str = "subagent-coordinator";
pub const SUBAGENT_HOST_TEMPLATE: &str = "subagent-host";
pub const APP_COLLECTIONS_TEMPLATE: &str = "app-collections";
pub const MACHINE_TEMPLATE: &str = "machine";
pub const SCHEDULER_OWNER_TEMPLATE: &str = "scheduler-owner";

static BUILTIN_TEMPLATES: &[ScopeTemplate] = &[
    ScopeTemplate {
        id: "conversation",
        collections: CONVERSATION_COLLECTIONS,
        scope: Scope::PerCollection(CONVERSATION_RULES),
        delivery: Delivery::Push,
    },
    ScopeTemplate {
        id: MACHINE_TEMPLATE,
        collections: MACHINE_COLLECTIONS,
        scope: Scope::PerCollection(MACHINE_RULES),
        delivery: Delivery::Push,
    },
    ScopeTemplate {
        id: "agent-config",
        collections: AGENT_CONFIG_COLLECTIONS,
        scope: Scope::Unscoped,
        delivery: Delivery::Replicate,
    },
    ScopeTemplate {
        id: "backup",
        collections: CONVERSATION_TRANSCRIPT_COLLECTIONS,
        scope: Scope::Unscoped,
        delivery: Delivery::Replicate,
    },
    ScopeTemplate {
        id: "discovery",
        collections: DISCOVERY_COLLECTIONS,
        scope: Scope::Unscoped,
        delivery: Delivery::Replicate,
    },
    ScopeTemplate {
        id: NETWORK_CONTROL_TEMPLATE,
        collections: NETWORK_CONTROL_COLLECTIONS,
        scope: Scope::Unscoped,
        delivery: Delivery::Replicate,
    },
    ScopeTemplate {
        id: SUBAGENT_COORDINATOR_TEMPLATE,
        collections: SUBAGENT_COORDINATOR_COLLECTIONS,
        scope: Scope::PerCollection(SUBAGENT_COORDINATOR_RULES),
        delivery: Delivery::Push,
    },
    ScopeTemplate {
        id: SUBAGENT_HOST_TEMPLATE,
        collections: SUBAGENT_HOST_COLLECTIONS,
        scope: Scope::PerCollection(SUBAGENT_HOST_RULES),
        delivery: Delivery::Push,
    },
    ScopeTemplate {
        id: SCHEDULER_OWNER_TEMPLATE,
        collections: SCHEDULER_OWNER_COLLECTIONS,
        scope: Scope::PerCollection(SCHEDULER_OWNER_RULES),
        delivery: Delivery::Replicate,
    },
    ScopeTemplate {
        id: APP_COLLECTIONS_TEMPLATE,
        // Bring-your-own: the DataPlanePairingDesired row supplies the set.
        collections: &[],
        scope: Scope::Unscoped,
        delivery: Delivery::Replicate,
    },
];

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Return all built-in templates in catalog order.
pub fn builtin_templates() -> &'static [ScopeTemplate] {
    BUILTIN_TEMPLATES
}

/// Look up a template by id.  Returns `None` for unknown ids.
pub fn resolve_template(id: &str) -> Option<&'static ScopeTemplate> {
    BUILTIN_TEMPLATES.iter().find(|t| t.id == id)
}

/// Build per-collection `PairingFilters` for a template scope against a
/// concrete peer/local DID pair.
///
/// - `Scope::PeerDid { field }` → for each collection, insert a predicate
///   `{ field, value: peer_did }`.
/// - `Scope::Unscoped` → empty map (no filtering).
/// - `Scope::PerCollection(rules)` → insert each exact collection rule using
///   either the peer DID or local DID as the value source. Declared
///   collections without a rule are deliberately unfiltered.
pub fn scope_filter(
    scope: &Scope,
    collections: &[&str],
    peer_did: &str,
    local_did: &str,
) -> PairingFilters {
    match scope {
        Scope::PeerDid { field } => collections
            .iter()
            .map(|&col| {
                (
                    col.to_string(),
                    FilterPredicate {
                        field: (*field).to_string(),
                        value: peer_did.to_string(),
                    },
                )
            })
            .collect(),
        Scope::Unscoped => BTreeMap::new(),
        Scope::PerCollection(rules) => rules
            .iter()
            .map(|rule| {
                let value = match rule.source {
                    DidSource::LocalDid => local_did,
                    DidSource::PeerDid => peer_did,
                    DidSource::HomeDid => local_did,
                };
                (
                    rule.collection.to_string(),
                    FilterPredicate {
                        field: rule.field.to_string(),
                        value: value.to_string(),
                    },
                )
            })
            .collect(),
    }
}

/// Templates whose bearer/dapair claims mint a reciprocal conversation
/// intent (mirrors Lean `conversationLike` in BearerClaim.lean).
pub fn conversation_like(id: &str) -> bool {
    // Rust trims incoming ids before applying the model's exact-equality
    // predicate: normalization happens at this boundary, not in the model —
    // Lean `conversationLike` is exact string equality with no trimming.
    let id = id.trim();
    id == "conversation" || id == MACHINE_TEMPLATE
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversation_is_scoped_push_with_transcript_and_readiness_collections() {
        let t = resolve_template("conversation").unwrap();
        assert_eq!(t.delivery, Delivery::Push);
        assert!(matches!(t.scope, Scope::PerCollection(_)));
        assert_eq!(t.collections.len(), 15);
        assert!(t.collections.contains(&"AgentRequest"));
        assert!(t.collections.contains(&"BearerPairingReady"));
        assert!(t.collections.contains(&"AgentBehavior"));
    }

    #[test]
    fn agent_config_includes_behavior_excludes_principal() {
        let t = resolve_template("agent-config").unwrap();
        assert_eq!(t.delivery, Delivery::Replicate);
        assert!(matches!(t.scope, Scope::Unscoped));
        assert!(t.collections.contains(&"AgentBehavior"));
        assert!(!t.collections.contains(&"AgentPrincipal"));
    }

    #[test]
    fn backup_is_unscoped_replicate() {
        let t = resolve_template("backup").unwrap();
        assert!(matches!(t.scope, Scope::Unscoped));
        assert_eq!(t.delivery, Delivery::Replicate);
    }

    #[test]
    fn conversation_scope_filters_transcript_by_requester_and_readiness_by_claimant() {
        let t = resolve_template("conversation").unwrap();
        let f = scope_filter(&t.scope, t.collections, "did:key:bob", "did:key:alice");
        assert_eq!(f.len(), 11);
        let p = f.get("AgentRequest").unwrap();
        assert_eq!(p.field, "requester_did");
        assert_eq!(p.value, "did:key:bob");
        let ready = f.get("BearerPairingReady").unwrap();
        assert_eq!(ready.field, "claimant_did");
        assert_eq!(ready.value, "did:key:bob");
    }

    #[test]
    fn unscoped_scope_filter_is_empty() {
        let t = resolve_template("backup").unwrap();
        assert!(scope_filter(&t.scope, t.collections, "did:key:bob", "did:key:alice").is_empty());
    }

    #[test]
    fn unknown_template_is_none() {
        assert!(resolve_template("nope").is_none());
    }

    // Additional coverage
    #[test]
    fn all_builtin_templates_have_nonempty_collections() {
        for t in builtin_templates() {
            // app-collections is the one bring-your-own template: its collection
            // set is supplied by the DataPlanePairingDesired row, not the catalog.
            if t.id == APP_COLLECTIONS_TEMPLATE {
                assert!(
                    t.collections.is_empty(),
                    "app-collections must carry no fixed collections"
                );
                continue;
            }
            assert!(
                !t.collections.is_empty(),
                "template {} has no collections",
                t.id
            );
        }
    }

    #[test]
    fn builtin_template_count_is_ten() {
        assert_eq!(builtin_templates().len(), 10);
    }

    #[test]
    fn scheduler_owner_replicates_only_owner_scoped_recovery_facts() {
        let template = resolve_template(SCHEDULER_OWNER_TEMPLATE).unwrap();
        assert_eq!(template.delivery, Delivery::Replicate);
        assert_eq!(template.collections, SCHEDULER_OWNER_COLLECTIONS);
        let filters = scope_filter(
            &template.scope,
            template.collections,
            "did:key:peer",
            "did:key:owner",
        );
        assert_eq!(filters.len(), 2);
        for collection in SCHEDULER_OWNER_COLLECTIONS {
            let filter = filters.get(*collection).expect("owner-scoped filter");
            assert_eq!(filter.field, "agent_did");
            assert_eq!(filter.value, "did:key:owner");
        }
    }

    #[test]
    fn app_collections_is_byo_unscoped_replicate() {
        let t = resolve_template(APP_COLLECTIONS_TEMPLATE).unwrap();
        assert_eq!(t.delivery, Delivery::Replicate);
        assert!(matches!(t.scope, Scope::Unscoped));
        assert!(t.collections.is_empty());
    }

    #[test]
    fn network_control_is_narrow_unscoped_control_plane() {
        let t = resolve_template(NETWORK_CONTROL_TEMPLATE).unwrap();
        assert_eq!(t.delivery, Delivery::Replicate);
        assert!(matches!(t.scope, Scope::Unscoped));
        assert_eq!(t.collections, NETWORK_CONTROL_COLLECTIONS);
        assert!(t.collections.contains(&"AgentNetwork"));
        assert!(!t.collections.contains(&"AgentBehavior"));
    }

    #[test]
    fn discovery_is_unscoped_replicate_with_control_plane_and_config() {
        let t = resolve_template("discovery").unwrap();
        assert_eq!(t.delivery, Delivery::Replicate);
        assert!(matches!(t.scope, Scope::Unscoped));
        // control-plane collections
        assert!(t.collections.contains(&"AgentNetwork"));
        assert!(t.collections.contains(&"NetworkMembership"));
        assert!(t.collections.contains(&"PeerEndpoint"));
        assert!(t.collections.contains(&"NetworkJoinRequest"));
        // agent-config so a joiner can run what it discovers
        for col in AGENT_CONFIG_COLLECTIONS {
            assert!(
                t.collections.contains(col),
                "discovery missing config collection {col}"
            );
        }
    }

    #[test]
    fn conversation_scope_filters_transcript_and_leaves_config_unfiltered() {
        let t = resolve_template("conversation").unwrap();
        let f = scope_filter(&t.scope, t.collections, "did:key:alice", "did:key:self");
        for col in CONVERSATION_RULES.iter().map(|rule| rule.collection) {
            assert!(f.contains_key(col), "missing filter for {col}");
        }
        for col in AGENT_CONFIG_COLLECTIONS {
            assert!(
                !f.contains_key(*col),
                "config collection {col} must be unfiltered"
            );
        }
    }

    #[test]
    fn subagent_coordinator_has_directional_rules() {
        let t = resolve_template(SUBAGENT_COORDINATOR_TEMPLATE).unwrap();
        assert_eq!(t.delivery, Delivery::Push);
        assert_eq!(t.collections, SUBAGENT_COORDINATOR_COLLECTIONS);
        let f = scope_filter(&t.scope, t.collections, "did:key:host", "did:key:coord");
        assert!(!f.contains_key("AgentRequest"));
        assert_eq!(
            f.get("AgentToolCall"),
            Some(&FilterPredicate {
                field: "spawn_target_did".to_string(),
                value: "did:key:host".to_string(),
            })
        );
    }

    #[test]
    fn subagent_host_filters_only_return_projection_on_requester() {
        let t = resolve_template(SUBAGENT_HOST_TEMPLATE).unwrap();
        assert_eq!(t.delivery, Delivery::Push);
        assert_eq!(t.collections, SUBAGENT_HOST_COLLECTIONS);
        let f = scope_filter(&t.scope, t.collections, "did:key:coord", "did:key:host");
        assert_eq!(f.len(), SUBAGENT_HOST_COLLECTIONS.len());
        assert_eq!(
            f.get("AgentRequest"),
            Some(&FilterPredicate {
                field: "requester_did".to_string(),
                value: "did:key:coord".to_string(),
            })
        );
        for col in SUBAGENT_HOST_COLLECTIONS {
            assert_eq!(
                f.get(*col),
                Some(&FilterPredicate {
                    field: "requester_did".to_string(),
                    value: "did:key:coord".to_string(),
                }),
                "unexpected subagent-host filter for {col}"
            );
        }
        for local_collection in [
            "AgentToolResult",
            "AgentSession",
            "AgentConversation",
            "CompactionEntry",
        ] {
            assert!(!t.collections.contains(&local_collection));
            assert!(!f.contains_key(local_collection));
        }
    }

    #[test]
    fn machine_template_scopes_conversation_and_issuer_owned_directory() {
        let t = resolve_template("machine").expect("machine template registered");
        assert_eq!(t.delivery, Delivery::Push);
        assert_eq!(t.collections.len(), 17);
        assert!(t.collections.contains(&AGENT_DIRECTORY_COLLECTION));
        let filters = scope_filter(&t.scope, t.collections, "did:key:phone", "did:key:server");
        // Conversation collections stay member-scoped exactly like `conversation`.
        for col in CONVERSATION_RULES.iter().map(|rule| rule.collection) {
            let predicate = filters.get(col).expect("conversation collection filtered");
            let expected_field = if col == "BearerPairingReady" {
                "claimant_did"
            } else {
                "requester_did"
            };
            assert_eq!(predicate.field, expected_field);
            assert_eq!(predicate.value, "did:key:phone");
        }
        assert_eq!(
            filters.get(AGENT_DIRECTORY_COLLECTION),
            Some(&FilterPredicate {
                field: "source_did".to_string(),
                value: "did:key:server".to_string(),
            })
        );
        // Persona request rows carry requester-authored config picks and the
        // server's status_detail; losing this rule would push every
        // requester's rows to every machine peer (the #687 leak class).
        assert_eq!(
            filters.get("PersonaConfigRequest"),
            Some(&FilterPredicate {
                field: "requester_did".to_string(),
                value: "did:key:phone".to_string(),
            })
        );
    }

    #[test]
    fn conversation_like_accepts_exactly_the_intent_minting_templates() {
        assert!(conversation_like("conversation"));
        assert!(conversation_like("machine"));
        assert!(conversation_like(" machine "));
        assert!(!conversation_like("network-control"));
        assert!(!conversation_like("discovery"));
        assert!(!conversation_like(""));
    }
}
