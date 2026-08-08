use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use events::EventName;
use gents::{ActiveRuntimeSnapshot, EventSource, UpdateSubscriptionSource};
use tokio::sync::watch;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::support::mock_subscription::MockUpdateSubscriptionSource;
use crate::support::test_db;

#[tokio::test]
async fn integration_can_construct_event_source_and_mock_delivers_updates() {
    let db = test_db("event-source-subscription-factory-smoke").await;
    let snapshot = Arc::new(ActiveRuntimeSnapshot {
        generation: 1,
        principal: None,
        local_did: String::new(),
        paired_peer_dids: HashSet::new(),
        default_behavior_id: "test".to_string(),
        behaviors: HashMap::new(),
        config_provenance_scope: gents::rendered_request::ConfigProvenanceScope::StaticOrOneShot,
        behavior_config_provenance: HashMap::new(),
        tool_surfaces: HashMap::new(),
        backend_admission_configs: HashMap::new(),
        unavailable_behaviors: HashMap::new(),
        active_schedules: HashMap::new(),
        unavailable_schedules: HashSet::new(),
        active_event_triggers: HashMap::new(),
        unavailable_event_triggers: HashSet::new(),
        active_tasks: HashMap::new(),
        dispatchers: HashMap::new(),
        behavior_executor_capacities: HashMap::new(),
        behavior_executor_queue_capacities: HashMap::new(),
    });
    let (_snapshot_tx, snapshot_rx) = watch::channel(snapshot);

    let mock = MockUpdateSubscriptionSource::new();
    let subs: Arc<dyn UpdateSubscriptionSource> = Arc::new(mock.clone());
    let _source = EventSource::with_subscription_source(
        subs,
        snapshot_rx,
        db.node.clone(),
        CancellationToken::new(),
    );

    let mut subscription = mock.subscribe_updates();
    mock.publish_update("collection-id", "doc-id");

    let message = timeout(Duration::from_secs(1), subscription.recv())
        .await
        .expect("mock subscription should deliver the published update")
        .expect("mock subscription should remain open");

    assert_eq!(message.name, EventName::Update);
    let update = message
        .as_update()
        .expect("message should contain update data");
    assert_eq!(update.collection_id, "collection-id");
    assert_eq!(update.doc_id, "doc-id");
}
