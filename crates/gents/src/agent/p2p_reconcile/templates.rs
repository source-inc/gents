//! Scope-template catalog for P2P filtered replication.
//!
//! A `ScopeTemplate` is a named pairing intent: a fixed collection set, a
//! `Scope` (how per-peer document filtering is derived), and a `Delivery`
//! (push vs. replicate). The catalog is static and hardcoded here.
//!
//! Pairing filters use DefraDB's predicate type directly. Local helpers only
//! derive, combine, and inspect those predicates.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

pub use p2p::ReplicationFilter as FilterPredicate;

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
// Pairing filters
// ---------------------------------------------------------------------------

pub fn equality_filter(field: impl Into<String>, value: impl Into<String>) -> FilterPredicate {
    FilterPredicate::eq(field, Value::String(value.into()))
}

pub fn combine_filters(left: FilterPredicate, right: FilterPredicate) -> FilterPredicate {
    let mut filters = Vec::new();
    match left {
        FilterPredicate::All(nested) => filters.extend(nested),
        predicate => filters.push(predicate),
    }
    match right {
        FilterPredicate::All(nested) => filters.extend(nested),
        predicate => filters.push(predicate),
    }
    let mut unique = Vec::with_capacity(filters.len());
    for filter in filters {
        if !unique.contains(&filter) {
            unique.push(filter);
        }
    }
    if unique.len() == 1 {
        unique.pop().expect("one filter")
    } else {
        FilterPredicate::All(unique)
    }
}

pub fn filter_conditions(filter: &FilterPredicate) -> Option<Map<String, Value>> {
    match filter {
        FilterPredicate::Predicate(conditions) => Some(conditions.clone()),
        FilterPredicate::All(filters) => {
            let mut conditions = Map::new();
            conditions.insert(
                "_and".to_string(),
                Value::Array(
                    filters
                        .iter()
                        .map(|filter| filter_conditions(filter).map(Value::Object))
                        .collect::<Option<Vec<_>>>()?,
                ),
            );
            Some(conditions)
        }
        FilterPredicate::Acp { .. } => None,
    }
}

pub fn to_replication_filters(
    filters: &PairingFilters,
) -> Result<defra_p2p_adapter::ReplicationFilters, String> {
    filters
        .iter()
        .map(|(collection, filter)| {
            let conditions = filter_conditions(filter).ok_or_else(|| {
                format!("ACP filter for {collection} is not supported by the replication API")
            })?;
            Ok((
                collection.clone(),
                defra_p2p_adapter::ReplicationFilter::predicate(conditions),
            ))
        })
        .collect()
}

pub fn single_string_eq(filter: &FilterPredicate) -> Option<(&str, &str)> {
    let FilterPredicate::Predicate(conditions) = filter else {
        return None;
    };
    let (field, value) = conditions.iter().next()?;
    if conditions.len() != 1 {
        return None;
    }
    let operators = value.as_object()?;
    let value = operators.get("_eq")?.as_str()?;
    (operators.len() == 1).then_some((field.as_str(), value))
}

/// Per-collection filter predicates for a concrete pairing.
///
/// `key` = collection name, `value` = predicate to apply when
/// subscribing / pushing documents for that collection.  An empty map means
/// no filtering (Unscoped).
///
pub type PairingFilters = BTreeMap<String, FilterPredicate>;

pub fn decode_pairing_filters(raw: &str) -> serde_json::Result<PairingFilters> {
    let mut value: Value = serde_json::from_str(raw)?;
    if let Some(filters) = value.as_object_mut() {
        for filter in filters.values_mut() {
            let legacy = filter.as_object().and_then(|object| {
                Some((
                    object.get("field")?.as_str()?.to_string(),
                    object.get("value")?.as_str()?.to_string(),
                ))
            });
            if let Some((field, value)) = legacy {
                *filter = serde_json::to_value(equality_filter(field, value))?;
            }
        }
    }
    serde_json::from_value(value)
}

// ---------------------------------------------------------------------------
// Built-in template catalog
// ---------------------------------------------------------------------------

/// Conversation grants carry requester-scoped transcript artifacts plus the
/// small unfiltered configuration control plane needed to render and operate
/// the paired agent without a second transport.
const CONVERSATION_COLLECTIONS: &[&str] = &[
    "AgentRequest",
    "AgentResponse",
    "AgentMessage",
    "AgentToolCall",
    "AgentToolResult",
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
    "DatastoreToolSurface",
];

const CONVERSATION_TRANSCRIPT_COLLECTIONS: &[&str] = &[
    "AgentRequest",
    "AgentResponse",
    "AgentMessage",
    "AgentToolCall",
    "AgentToolResult",
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

/// Requester-scoped session-index grant. Complete historical index hydration
/// is handled separately by the desktop's node-global branchable pull.
pub const CLIENT_INDEX_COLLECTIONS: [&str; 2] = ["AgentConversation", "AgentSession"];

const CLIENT_INDEX_RULES: &[CollectionRule] = &[
    CollectionRule {
        collection: "AgentConversation",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentSession",
        field: "requester_did",
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
    "AgentMessage",
    "AgentToolCall",
    "AgentToolResult",
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
    "DatastoreToolSurface",
    "PersonaConfigRequest",
    "SessionHydrationRequest",
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
        collection: "SessionHydrationRequest",
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
    "DatastoreToolSurface",
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
    "DatastoreToolSurface",
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
    "AgentMessage",
    "AgentToolCall",
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
        collection: "AgentMessage",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
    CollectionRule {
        collection: "AgentToolCall",
        field: "requester_did",
        source: DidSource::PeerDid,
    },
];

pub const NETWORK_CONTROL_TEMPLATE: &str = "network-control";
pub const SUBAGENT_COORDINATOR_TEMPLATE: &str = "subagent-coordinator";
pub const SUBAGENT_HOST_TEMPLATE: &str = "subagent-host";
pub const APP_COLLECTIONS_TEMPLATE: &str = "app-collections";
pub const MACHINE_TEMPLATE: &str = "machine";
pub const CLIENT_INDEX_TEMPLATE: &str = "client-index";

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
        id: APP_COLLECTIONS_TEMPLATE,
        // Bring-your-own: the DataPlanePairingDesired row supplies the set.
        collections: &[],
        scope: Scope::Unscoped,
        delivery: Delivery::Replicate,
    },
    ScopeTemplate {
        id: CLIENT_INDEX_TEMPLATE,
        collections: &CLIENT_INDEX_COLLECTIONS,
        scope: Scope::PerCollection(CLIENT_INDEX_RULES),
        delivery: Delivery::Push,
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
            .map(|&col| (col.to_string(), equality_filter(*field, peer_did)))
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
                    equality_filter(rule.field, value),
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
        assert_eq!(t.collections.len(), 16);
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
        assert_eq!(f.len(), 9);
        let p = f.get("AgentRequest").unwrap();
        assert_eq!(single_string_eq(p), Some(("requester_did", "did:key:bob")));
        let ready = f.get("BearerPairingReady").unwrap();
        assert_eq!(
            single_string_eq(ready),
            Some(("claimant_did", "did:key:bob"))
        );
    }

    #[test]
    fn rich_predicates_and_layered_equalities_keep_both_conditions() {
        let rich = FilterPredicate::Predicate(
            serde_json::json!({ "status": { "_in": ["pending", "processing"] } })
                .as_object()
                .expect("object")
                .clone(),
        );
        let combined = combine_filters(equality_filter("requester_did", "did:key:phone"), rich);

        assert_eq!(
            Value::Object(filter_conditions(&combined).expect("predicate conditions")),
            serde_json::json!({
                "_and": [
                    { "requester_did": { "_eq": "did:key:phone" } },
                    { "status": { "_in": ["pending", "processing"] } }
                ]
            })
        );
    }

    #[test]
    fn unsupported_acp_filter_is_rejected() {
        let filters = [(
            "AgentRequest".to_string(),
            FilterPredicate::Acp {
                relation: "reader".to_string(),
            },
        )]
        .into_iter()
        .collect();

        assert!(to_replication_filters(&filters).is_err());
    }

    #[test]
    fn legacy_pairing_filter_decodes() {
        let filters = decode_pairing_filters(
            r#"{"AgentRequest":{"field":"requester_did","value":"did:key:phone"}}"#,
        )
        .expect("legacy filters");

        assert_eq!(
            filters.get("AgentRequest").and_then(single_string_eq),
            Some(("requester_did", "did:key:phone"))
        );
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
    fn client_index_is_requester_scoped_push_of_the_session_index() {
        let t = resolve_template(CLIENT_INDEX_TEMPLATE).unwrap();
        assert_eq!(t.delivery, Delivery::Push);
        assert!(matches!(t.scope, Scope::PerCollection(_)));
        assert_eq!(t.collections, &["AgentConversation", "AgentSession"]);

        let filter = scope_filter(&t.scope, t.collections, "did:key:phone", "did:key:home");
        assert_eq!(filter.len(), 2);
        for collection in &CLIENT_INDEX_COLLECTIONS {
            let predicate = filter
                .get(*collection)
                .expect("indexed collection is filtered");
            assert_eq!(
                single_string_eq(predicate),
                Some(("requester_did", "did:key:phone"))
            );
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
            Some(&equality_filter("spawn_target_did", "did:key:host"))
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
            Some(&equality_filter("requester_did", "did:key:coord"))
        );
        for col in SUBAGENT_HOST_COLLECTIONS {
            assert_eq!(
                f.get(*col),
                Some(&equality_filter("requester_did", "did:key:coord")),
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
        assert_eq!(t.collections.len(), 19);
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
            assert_eq!(
                single_string_eq(predicate),
                Some((expected_field, "did:key:phone"))
            );
        }
        assert_eq!(
            filters.get(AGENT_DIRECTORY_COLLECTION),
            Some(&equality_filter("source_did", "did:key:server"))
        );
        // Persona request rows carry requester-authored config picks and the
        // server's status_detail; losing this rule would push every
        // requester's rows to every machine peer (the #687 leak class).
        assert_eq!(
            filters.get("PersonaConfigRequest"),
            Some(&equality_filter("requester_did", "did:key:phone"))
        );
        assert_eq!(
            filters.get("SessionHydrationRequest"),
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
