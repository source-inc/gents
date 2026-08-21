mod collection_resolver;
mod core;
mod mutations;
mod observe;
mod paths;
mod peer_directory;
mod principal_identity;
mod query;
mod schema;
mod store;

pub use collection_resolver::CollectionResolver;
pub use core::{
    BearerInvitePreview, BearerPairingResult, ClientCore, ClientCoreOptions, ClientPeerStatus,
    P2PHealth, P2PHealthStatus,
};
pub use mutations::{PeerMutationResult, SubmitRequestOptions, SubmittedRequest};
pub use observe::{ObservedStore, ObserverHandle, ObserverMetricsSnapshot};
pub use paths::DesktopPaths;
pub use peer_directory::{PeerDirectory, PeerRecord};
pub use principal_identity::PrincipalIdentity;
pub use query::{fetch_doc_patch, load_agent_scoped_snapshot};
pub use store::{ClientStore, ClientStoreRows, TaskRecentRuns, TranscriptView};
