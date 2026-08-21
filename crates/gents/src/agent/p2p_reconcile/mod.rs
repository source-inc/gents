//! Runtime-owned P2P pairing reconcile seam.

pub mod bearer_claim;
pub mod diff;
pub mod discovery;
pub mod embedded_impl;
pub mod endpoint;
pub mod engine;
pub mod error_class;
mod graphql_helpers;
pub mod intervals;
pub mod network;
pub mod persona_requests;
pub mod profiles;
pub mod reciprocal;
pub mod registry;
pub mod session_hydration;
pub mod templates;
pub mod trait_def;

pub use bearer_claim::{
    decide_bearer_claim, reconcile_bearer_claim_tick, run_bearer_claim_reconciler,
    BearerClaimStore, BearerClaimTickOutcome, BearerClaimVerdicts, BearerRejection,
    GraphqlBearerClaimStore, NonceBinding, PreparedBearerClaim,
};
pub use diff::{
    compute_owned_pairing_diff, compute_pairing_diff, DiffOp, PairingActual, PairingApplied,
    PairingDesired,
};
pub use discovery::{
    decide_join_admission, derive_registry_desired, heartbeat_is_fresh, reconcile_discovery_tick,
    run_discovery_reconciler, DiscoveredEntry, DiscoveryStore, DiscoveryTickOutcome,
    GraphqlDiscoveryStore, JoinAdmission, RegistryMemberRow, REGISTRY_STALE_AFTER,
    SOURCE_MANIFEST_PREFIX, SOURCE_OPERATOR, SOURCE_REGISTRY,
};
pub use embedded_impl::EmbeddedRemoteP2pAdmin;
pub use endpoint::{peer_endpoint_upsert_mutation, run_endpoint_heartbeat};
pub use engine::{
    merge_layered_desired, reconcile_peer_tick, run_pairing_reconciler,
    update_applied_after_success, GraphqlPairingStateStore, LoadedPairingApplied,
    PairingStateStore, PairingTickOutcome, MAX_CONCURRENT_PEER_PREPARATIONS,
    PAIRING_SWEEP_INTERVAL,
};
pub use error_class::{classify_remote_admin_error, PairingErrorClass};
pub use network::{
    derive_network_desired, endpoint_is_fresh, reconcile_network_tick, run_network_reconciler,
    GraphqlNetworkStore, NetworkEndpointEntry, NetworkStore, NetworkTickOutcome, SOURCE_NETWORK,
};
pub use persona_requests::{
    reconcile_persona_tick, run_persona_request_reconciler, GraphqlPersonaRequestStore,
    PersonaRequestStore, PersonaTickOutcome,
};
pub use profiles::{expand_p2p_collection_profile_ids, P2pCollectionProfile};
pub use reciprocal::{
    derive_reciprocal_desired, reconcile_reciprocal_tick, run_reciprocal_reconciler,
    GraphqlReciprocalStore, ReciprocalRowState, ReciprocalStore, ReciprocalTickOutcome,
};
pub use registry::{
    registry_upsert_mutation, resolve_network_id, run_registry_heartbeat, RegistryEntry,
    UpsertKind, DEFAULT_NETWORK_ID, NETWORK_ID_ENV, REGISTRY_HEARTBEAT_INTERVAL,
};
pub use templates::{
    builtin_templates, combine_filters, conversation_like, decode_pairing_filters, equality_filter,
    filter_conditions, resolve_template, scope_filter, single_string_eq, to_replication_filters,
    Delivery, DidSource, FilterPredicate, PairingFilters, Scope, ScopeTemplate,
    AGENT_DIRECTORY_COLLECTION, NETWORK_CONTROL_TEMPLATE,
};
pub use trait_def::{
    RemoteP2pAdmin, RemoteP2pAdminError, RemoteP2pAdminResult, RemoteP2pDocument, RemoteReplicator,
};
