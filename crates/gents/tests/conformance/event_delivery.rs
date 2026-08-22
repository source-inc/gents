use super::*;
use std::sync::Arc;
use std::time::Duration;

use gents::defra_node::{EmbeddedNode, QueryResponse};
use gents::{
    ActiveRuntimeSnapshot, AgentRequest, ConcurrencyMode, DefraWatcher, EventSource,
    ResolvedEventTrigger, ResolvedTask, SubagentSource, TriggerSource, Watcher,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::support::mock_subscription::MockUpdateSubscriptionSource;

const EVENT_SOURCE_COLLECTION: &str = "EventDeliveryDoc";
const EVENT_SOURCE_TRIGGER_ID: &str = "event-delivery-trigger";
const EVENT_SOURCE_TASK_ID: &str = "event-delivery-task";
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(2);
const RESCAN_TEST_INTERVAL: Duration = Duration::from_millis(50);

pub(super) async fn event_delivery_transition_cases_match_contract() {
    let cases = lean_event_delivery_transition_cases();
    let watcher = runtime_event_delivery_source_contract("Watcher");
    assert!(
        cases.len() >= 12,
        "Expected at least 12 transition-case rows; got {}",
        cases.len()
    );
    for case in cases {
        let mut runtime = ProductionEventDeliveryDriver::new(watcher, &case.pre).await;
        runtime
            .apply(&case.action)
            .await
            .unwrap_or_else(|err| panic!("case `{}` rejected runtime action: {err}", case.name));
        assert_eq!(
            runtime.world, case.post,
            "case `{}` drifted from production runtime replay",
            case.name
        );
    }
}

pub(super) fn event_delivery_source_instances_match_runtime() {
    let runtime_by_name = runtime_event_delivery_source_contracts()
        .iter()
        .map(|instance| (instance.name, *instance))
        .collect::<HashMap<_, _>>();

    for lean in lean_event_delivery_source_instances() {
        let runtime = runtime_by_name
            .get(lean.name.as_str())
            .unwrap_or_else(|| panic!("runtime source {:?} must be present", lean.name));
        assert_eq!(runtime.dedupe_policy, lean.dedupe_policy);
        assert_eq!(runtime.rescan_bounded_by, lean.rescan_bounded_by);
        assert_eq!(runtime.deviation, lean.deviation.as_deref());
    }
    assert_eq!(
        runtime_by_name.len(),
        lean_event_delivery_source_instances().len(),
        "runtime source introspection should not expose unmodeled sources"
    );
}

pub(super) async fn event_delivery_convergence_traces_match_runtime_or_deviation() {
    let traces = lean_event_delivery_convergence_traces();
    assert!(
        traces.len() >= 3,
        "Expected at least one convergence trace per source"
    );

    for trace in traces {
        let source = runtime_event_delivery_source_contract(&trace.instance_name);
        let mut runtime = ProductionEventDeliveryDriver::new(source, &trace.initial_world).await;
        for action in &trace.actions {
            runtime.apply(action).await.unwrap_or_else(|err| {
                panic!("trace `{}` rejected runtime action: {err}", trace.name)
            });
        }
        assert_eq!(
            runtime.world, trace.final_world,
            "trace `{}` drifted from production runtime replay",
            trace.name
        );

        match trace.status.as_str() {
            "substantive" => {
                assert!(
                    runtime.unhandled_persistent_docs().await.is_empty(),
                    "substantive trace `{}` left persistent docs unhandled: {:?}",
                    trace.name,
                    runtime.unhandled_persistent_docs().await
                );
                assert!(
                    source.deviation.is_none(),
                    "substantive trace `{}` should run against a non-deviation source",
                    trace.name
                );
            }
            "deviation" => panic!(
                "event-delivery deviation trace `{}` is retired; live sources must emit substantive convergence traces",
                trace.name,
            ),
            other => panic!(
                "trace `{}` has unknown status `{}` (expected 'substantive' or 'deviation')",
                trace.name, other,
            ),
        }
    }

    let trace_instances: std::collections::HashSet<&str> =
        traces.iter().map(|t| t.instance_name.as_str()).collect();
    for name in &["Watcher", "EventSource", "SubagentSource"] {
        assert!(
            trace_instances.contains(name),
            "Expected a convergence trace for instance `{}`",
            name
        );
    }
}

struct ProductionEventDeliveryDriver {
    source: EventDeliverySourceContract,
    db: super::support::TestDb,
    mock_subs: MockUpdateSubscriptionSource,
    runtime: ProductionRuntime,
    cancel: CancellationToken,
    _snapshot_tx: Option<watch::Sender<Arc<ActiveRuntimeSnapshot>>>,
    runner: Option<tokio::task::JoinHandle<()>>,
    emitted_rx: Option<mpsc::Receiver<String>>,
    emitted_buffer: Vec<String>,
    doc_ids: HashMap<String, String>,
    world: lean_vocab_test::LeanEventDeliveryWorld,
}

enum ProductionRuntime {
    Watcher { watcher: DefraWatcher },
    EventSource,
    SubagentSource,
}

impl ProductionEventDeliveryDriver {
    async fn new(
        source: EventDeliverySourceContract,
        world: &lean_vocab_test::LeanEventDeliveryWorld,
    ) -> Self {
        let db = test_db(&format!("event-delivery-{}", source.name)).await;
        let mock_subs = MockUpdateSubscriptionSource::new();
        let cancel = CancellationToken::new();
        let mut driver = match source.name {
            "Watcher" => {
                let watcher = DefraWatcher::with_subscription_source(
                    Arc::new(mock_subs.clone()),
                    db.node.clone(),
                    AGENT_DID,
                );
                Self {
                    source,
                    db,
                    mock_subs,
                    runtime: ProductionRuntime::Watcher { watcher },
                    cancel,
                    _snapshot_tx: None,
                    runner: None,
                    emitted_rx: None,
                    emitted_buffer: Vec::new(),
                    doc_ids: HashMap::new(),
                    world: empty_event_delivery_world(),
                }
            }
            "EventSource" => {
                install_event_delivery_source_schema(db.node.as_ref()).await;
                let (runner, emitted_rx, snapshot_tx) =
                    spawn_event_source_runner(db.node.clone(), mock_subs.clone(), cancel.clone());
                assert!(
                    mock_subs
                        .wait_for_subscribers(1, Duration::from_secs(2))
                        .await,
                    "EventSource runner did not open its mock subscription"
                );
                Self {
                    source,
                    db,
                    mock_subs,
                    runtime: ProductionRuntime::EventSource,
                    cancel,
                    _snapshot_tx: Some(snapshot_tx),
                    runner: Some(runner),
                    emitted_rx: Some(emitted_rx),
                    emitted_buffer: Vec::new(),
                    doc_ids: HashMap::new(),
                    world: empty_event_delivery_world(),
                }
            }
            "SubagentSource" => {
                install_subagent_source_fixture(db.node.as_ref())
                    .await
                    .expect("install SubagentSource event-delivery fixture");
                let (runner, emitted_rx, snapshot_tx) = spawn_subagent_source_runner(
                    db.node.clone(),
                    mock_subs.clone(),
                    cancel.clone(),
                );
                assert!(
                    mock_subs
                        .wait_for_subscribers(1, Duration::from_secs(2))
                        .await,
                    "SubagentSource runner did not open its mock subscription"
                );
                Self {
                    source,
                    db,
                    mock_subs,
                    runtime: ProductionRuntime::SubagentSource,
                    cancel,
                    _snapshot_tx: Some(snapshot_tx),
                    runner: Some(runner),
                    emitted_rx: Some(emitted_rx),
                    emitted_buffer: Vec::new(),
                    doc_ids: HashMap::new(),
                    world: empty_event_delivery_world(),
                }
            }
            other => panic!("unhandled event-delivery source {other:?}"),
        };
        driver.seed_world(world).await.unwrap_or_else(|err| {
            panic!(
                "failed to seed production event-delivery world for {}: {err}",
                driver.source.name
            )
        });
        driver
    }

    async fn seed_world(
        &mut self,
        world: &lean_vocab_test::LeanEventDeliveryWorld,
    ) -> Result<(), String> {
        for (index, doc) in world.persistent_set.iter().enumerate() {
            self.persist_runtime_doc(doc, index).await?;
            if world.processed_set.contains(doc) {
                self.mark_runtime_doc_processed(doc).await?;
            }
        }
        for doc in &world.subscription_queue {
            self.publish_update(doc)?;
        }
        self.world = world.clone();
        Ok(())
    }

    async fn apply(&mut self, action: &LeanEventDeliveryAction) -> Result<(), String> {
        match action {
            LeanEventDeliveryAction::Persist { doc } => {
                if self.world.persistent_set.contains(doc) {
                    return Err(format!("doc {doc:?} already persisted"));
                }
                let sequence = self.world.persistent_set.len();
                self.persist_runtime_doc(doc, sequence).await?;
                self.world.persistent_set.insert(0, doc.clone());
            }
            LeanEventDeliveryAction::Depersist { doc } => {
                erase_first(&mut self.world.persistent_set, doc)
                    .ok_or_else(|| format!("doc {doc:?} is not persistent"))?;
                self.depersist_runtime_doc(doc).await?;
            }
            LeanEventDeliveryAction::Enqueue { doc } => {
                if !self.world.persistent_set.contains(doc) {
                    return Err(format!("doc {doc:?} is not persistent"));
                }
                self.publish_update(doc)?;
                self.world.subscription_queue.insert(0, doc.clone());
            }
            LeanEventDeliveryAction::Drop { doc }
            | LeanEventDeliveryAction::DeliverFromQueue { doc } => {
                erase_first(&mut self.world.subscription_queue, doc)
                    .ok_or_else(|| format!("doc {doc:?} is not queued"))?;
                self.drop_production_delivery(doc).await?;
            }
            LeanEventDeliveryAction::RescanTick => {
                if self.source.rescan_bounded_by == 0 {
                    return Err(format!(
                        "source {} does not advertise a positive bounded live rescan",
                        self.source.name
                    ));
                }
                let mut rescanned = self
                    .world
                    .persistent_set
                    .iter()
                    .filter(|doc| !self.world.processed_set.contains(*doc))
                    .cloned()
                    .collect::<Vec<_>>();
                self.drive_rescan(&rescanned).await?;
                rescanned.extend(self.world.subscription_queue.clone());
                self.world.subscription_queue = rescanned;
            }
            LeanEventDeliveryAction::Handle { doc } => {
                if self.world.processed_set.contains(doc) {
                    return Err(format!("doc {doc:?} is already processed"));
                }
                erase_first(&mut self.world.subscription_queue, doc)
                    .ok_or_else(|| format!("doc {doc:?} is not queued"))?;
                self.drive_handle(doc).await?;
                self.mark_runtime_doc_processed(doc).await?;
                self.world.processed_set.insert(0, doc.clone());
                self.world.handled.insert(0, doc.clone());
            }
        }
        Ok(())
    }

    async fn unhandled_persistent_docs(&self) -> Vec<String> {
        self.world
            .persistent_set
            .iter()
            .filter(|doc| !self.world.handled.contains(*doc))
            .cloned()
            .collect()
    }

    async fn persist_runtime_doc(&mut self, doc: &str, sequence: usize) -> Result<(), String> {
        if self.doc_ids.contains_key(doc) {
            return Ok(());
        }
        let doc_id = match self.source.name {
            "Watcher" => {
                create_request(
                    self.db.node.as_ref(),
                    doc,
                    &format!("event-delivery-session-{}", sanitize_graphql_id(doc)),
                    "pending",
                    &format!("2026-05-20T00:00:{:02}Z", sequence % 60),
                )
                .await
            }
            "EventSource" => self.create_event_delivery_doc(doc).await?,
            "SubagentSource" => self.create_subagent_tool_call_doc(doc).await?,
            other => return Err(format!("unsupported source {other:?}")),
        };
        self.doc_ids.insert(doc.to_string(), doc_id);
        Ok(())
    }

    async fn depersist_runtime_doc(&mut self, doc: &str) -> Result<(), String> {
        match self.source.name {
            "Watcher" => {
                self.update_agent_request_state(doc, "completed", "completed")
                    .await
            }
            "EventSource" | "SubagentSource" => Ok(()),
            other => Err(format!("unsupported source {other:?}")),
        }
    }

    async fn mark_runtime_doc_processed(&self, doc: &str) -> Result<(), String> {
        match self.source.name {
            "Watcher" => {
                self.update_agent_request_state(doc, "completed", "completed")
                    .await
            }
            "EventSource" | "SubagentSource" => Ok(()),
            other => Err(format!("unsupported source {other:?}")),
        }
    }

    async fn update_agent_request_state(
        &self,
        doc: &str,
        status: &str,
        lifecycle_state: &str,
    ) -> Result<(), String> {
        let doc_id = self
            .doc_ids
            .get(doc)
            .ok_or_else(|| format!("doc {doc:?} has no AgentRequest row"))?;
        let doc_id = escape_graphql_string(doc_id);
        let status = escape_graphql_string(status);
        let lifecycle_state = escape_graphql_string(lifecycle_state);
        let mutation = format!(
            r#"mutation {{
                update_AgentRequest(
                    filter: {{ _docID: {{ _eq: "{doc_id}" }} }},
                    input: {{
                        status: "{status}",
                        lifecycle_state: "{lifecycle_state}"
                    }}
                ) {{ _docID }}
            }}"#
        );
        let resp = self.db.node.execute(&mutation).await;
        if resp.has_errors() {
            return Err(format!("update_AgentRequest failed: {:?}", resp.errors));
        }
        Ok(())
    }

    async fn create_event_delivery_doc(&self, doc: &str) -> Result<String, String> {
        let external_id = escape_graphql_string(doc);
        let mutation = format!(
            r#"mutation {{
                add_{EVENT_SOURCE_COLLECTION}(input: {{
                    external_id: "{external_id}",
                    payload: "{{}}"
                }}) {{ _docID }}
            }}"#
        );
        let resp = self.db.node.execute(&mutation).await;
        if resp.has_errors() {
            return Err(format!(
                "add_{EVENT_SOURCE_COLLECTION} failed: {:?}",
                resp.errors
            ));
        }
        mutation_doc_id(&resp, &format!("add_{EVENT_SOURCE_COLLECTION}"))
            .ok_or_else(|| format!("add_{EVENT_SOURCE_COLLECTION} returned no _docID"))
    }

    async fn create_subagent_tool_call_doc(&self, doc: &str) -> Result<String, String> {
        let tool_call_id = escape_graphql_string(doc);
        let tool_call_key = escape_graphql_string(&format!("event-delivery-session:{doc}"));
        let parent_request_id = format!("event-delivery-parent-{doc}");
        let parent_session_id = format!("event-delivery-session-{doc}");
        let child_request_id = format!("event-delivery-child-{doc}");
        let parent_request_doc_id = create_request(
            self.db.node.as_ref(),
            &parent_request_id,
            &parent_session_id,
            "processing",
            "2026-05-20T00:00:00Z",
        )
        .await;
        let parent_request_id = escape_graphql_string(&parent_request_id);
        let parent_request_doc_id = escape_graphql_string(&parent_request_doc_id);
        let parent_session_id = escape_graphql_string(&parent_session_id);
        let child_request_id = escape_graphql_string(&child_request_id);
        let args = escape_graphql_string(
            &serde_json::json!({
                "name": AGENT_NAME,
                "agent_did": AGENT_DID,
                "behavior_id": AGENT_NAME,
                "prompt": "materialize event-delivery child",
            })
            .to_string(),
        );
        let mutation = format!(
            r#"mutation {{
                create_AgentToolCall(input: {{
                    tool_call_key: "{tool_call_key}",
                    request_id: "{parent_request_id}",
                    request_doc_id: "{parent_request_doc_id}",
                    session_id: "{parent_session_id}",
                    message_sequence: 1,
                    tool_name: "spawn_subagent",
                    tool_call_id: "{tool_call_id}",
                    args: "{args}",
                    result: "",
                    status: "called",
                    lifecycle_state: "running",
                    started_at: "2026-05-20T00:00:00Z",
                    await_mode: "foreground",
                    cancel_policy: "cascade",
                    child_request_id: "{child_request_id}",
                    selected_service_id: null,
                    selected_tool_name: null,
                    tool_failure_class: null,
                    latency_ms: null
                }}) {{ _docID }}
            }}"#
        );
        let resp = self.db.node.execute(&mutation).await;
        if resp.has_errors() {
            return Err(format!("create_AgentToolCall failed: {:?}", resp.errors));
        }
        if let Some(doc_id) = mutation_doc_id(&resp, "create_AgentToolCall") {
            return Ok(doc_id);
        }
        let query = format!(
            r#"{{
                AgentToolCall(
                    filter: {{ tool_call_id: {{ _eq: "{tool_call_id}" }} }},
                    limit: 1
                ) {{ _docID }}
            }}"#
        );
        let resp = self.db.node.execute(&query).await;
        if resp.has_errors() {
            return Err(format!(
                "query AgentToolCall after create failed: {:?}",
                resp.errors
            ));
        }
        first_optional_row::<super::support::DocIdRow>(&resp, "AgentToolCall")
            .map(|row| row.doc_id)
            .ok_or_else(|| "created AgentToolCall row was not found".to_string())
    }

    fn publish_update(&self, doc: &str) -> Result<(), String> {
        let collection = match self.source.name {
            "Watcher" => "AgentRequest",
            "EventSource" => EVENT_SOURCE_COLLECTION,
            "SubagentSource" => "AgentToolCall",
            other => return Err(format!("unsupported source {other:?}")),
        };
        let collection_id = self.collection_id(collection)?;
        let doc_id = self
            .doc_ids
            .get(doc)
            .cloned()
            .unwrap_or_else(|| doc.to_string());
        self.mock_subs.publish_update(collection_id, doc_id);
        Ok(())
    }

    fn collection_id(&self, collection: &str) -> Result<String, String> {
        self.db
            .node
            .get_collection(collection)
            .map_err(|err| format!("get_collection({collection}) failed: {err}"))?
            .map(|definition| definition.collection_id)
            .ok_or_else(|| format!("collection {collection:?} not found"))
    }

    async fn drive_rescan(&mut self, expected_docs: &[String]) -> Result<(), String> {
        match &mut self.runtime {
            ProductionRuntime::Watcher { watcher } => {
                for expected in expected_docs {
                    let request = poll_watcher(watcher).await?;
                    if request.request_id != *expected {
                        return Err(format!(
                            "watcher rescan emitted {:?}, expected {:?}",
                            request.request_id, expected
                        ));
                    }
                    self.emitted_buffer.push(request.request_id);
                }
                Ok(())
            }
            ProductionRuntime::EventSource | ProductionRuntime::SubagentSource => {
                for expected in expected_docs {
                    let expected = self.production_doc_id(expected)?;
                    self.wait_for_emitted_doc_buffered(&expected, DELIVERY_TIMEOUT)
                        .await?;
                }
                Ok(())
            }
        }
    }

    async fn drive_handle(&mut self, doc: &str) -> Result<(), String> {
        match &mut self.runtime {
            ProductionRuntime::Watcher { watcher } => {
                if erase_first(&mut self.emitted_buffer, doc).is_some() {
                    return Ok(());
                }
                let request = poll_watcher(watcher).await?;
                if request.request_id != doc {
                    return Err(format!(
                        "watcher handle emitted {:?}, expected {:?}",
                        request.request_id, doc
                    ));
                }
                Ok(())
            }
            ProductionRuntime::EventSource | ProductionRuntime::SubagentSource => {
                let expected = self.production_doc_id(doc)?;
                self.wait_for_emitted_doc(&expected, DELIVERY_TIMEOUT).await
            }
        }
    }

    async fn drop_production_delivery(&mut self, doc: &str) -> Result<(), String> {
        if matches!(self.runtime, ProductionRuntime::Watcher { .. }) {
            return Ok(());
        }
        let expected = self.production_doc_id(doc)?;
        match self
            .wait_for_any_emitted_doc(Duration::from_millis(250))
            .await?
        {
            Some(emitted) if emitted == expected => Ok(()),
            Some(emitted) => {
                self.emitted_buffer.push(emitted);
                Ok(())
            }
            None => Ok(()),
        }
    }

    fn production_doc_id(&self, doc: &str) -> Result<String, String> {
        match self.source.name {
            "Watcher" => Ok(doc.to_string()),
            "EventSource" => self
                .doc_ids
                .get(doc)
                .cloned()
                .ok_or_else(|| format!("doc {doc:?} has no runtime row")),
            "SubagentSource" => Ok(doc.to_string()),
            other => Err(format!("unsupported source {other:?}")),
        }
    }

    async fn wait_for_emitted_doc(
        &mut self,
        expected: &str,
        timeout: Duration,
    ) -> Result<(), String> {
        if erase_first(&mut self.emitted_buffer, expected).is_some() {
            return Ok(());
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            match self.wait_for_any_emitted_doc_until(deadline).await? {
                Some(emitted) if emitted == expected => return Ok(()),
                Some(emitted) => self.emitted_buffer.push(emitted),
                None => {
                    return Err(format!(
                        "{} did not emit expected runtime doc {:?}",
                        self.source.name, expected
                    ));
                }
            }
        }
    }

    async fn wait_for_emitted_doc_buffered(
        &mut self,
        expected: &str,
        timeout: Duration,
    ) -> Result<(), String> {
        if self.emitted_buffer.iter().any(|doc| doc == expected) {
            return Ok(());
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            match self.wait_for_any_emitted_doc_until(deadline).await? {
                Some(emitted) => {
                    let matched = emitted == expected;
                    self.emitted_buffer.push(emitted);
                    if matched {
                        return Ok(());
                    }
                }
                None => {
                    return Err(format!(
                        "{} did not emit expected runtime doc {:?}",
                        self.source.name, expected
                    ));
                }
            }
        }
    }

    async fn wait_for_any_emitted_doc(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<String>, String> {
        self.wait_for_any_emitted_doc_until(tokio::time::Instant::now() + timeout)
            .await
    }

    async fn wait_for_any_emitted_doc_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<Option<String>, String> {
        let Some(rx) = &mut self.emitted_rx else {
            return Ok(None);
        };
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(doc)) => Ok(Some(doc)),
            Ok(None) => Err(format!("{} runner exited", self.source.name)),
            Err(_) => Ok(None),
        }
    }
}

impl Drop for ProductionEventDeliveryDriver {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(runner) = &self.runner {
            runner.abort();
        }
    }
}

fn spawn_event_source_runner(
    node: Arc<EmbeddedNode>,
    mock_subs: MockUpdateSubscriptionSource,
    cancel: CancellationToken,
) -> (
    tokio::task::JoinHandle<()>,
    mpsc::Receiver<String>,
    watch::Sender<Arc<ActiveRuntimeSnapshot>>,
) {
    let snapshot = active_snapshot_with_event_trigger();
    let (snapshot_tx, snapshot_rx) = watch::channel(snapshot);
    let mut source =
        EventSource::with_subscription_source(Arc::new(mock_subs), snapshot_rx, node, cancel)
            .with_rescan_interval(RESCAN_TEST_INTERVAL);
    let (tx, rx) = mpsc::channel(16);
    let runner = tokio::spawn(async move {
        while let Some(intent) = source.next_fire().await {
            if let Some(doc_id) = intent
                .event_vars
                .get("source_doc_id")
                .and_then(serde_json::Value::as_str)
            {
                if tx.send(doc_id.to_string()).await.is_err() {
                    break;
                }
            }
        }
    });
    (runner, rx, snapshot_tx)
}

fn spawn_subagent_source_runner(
    node: Arc<EmbeddedNode>,
    mock_subs: MockUpdateSubscriptionSource,
    cancel: CancellationToken,
) -> (
    tokio::task::JoinHandle<()>,
    mpsc::Receiver<String>,
    watch::Sender<Arc<ActiveRuntimeSnapshot>>,
) {
    let snapshot = active_snapshot_without_event_triggers();
    let (snapshot_tx, snapshot_rx) = watch::channel(snapshot);
    let mut source =
        SubagentSource::with_subscription_source(Arc::new(mock_subs), snapshot_rx, node, cancel)
            .with_rescan_interval(RESCAN_TEST_INTERVAL);
    let (tx, rx) = mpsc::channel(16);
    let runner = tokio::spawn(async move {
        while let Some(intent) = source.next_fire().await {
            if let Some(doc_id) = intent
                .event_vars
                .get("trigger_id")
                .and_then(serde_json::Value::as_str)
            {
                if tx.send(doc_id.to_string()).await.is_err() {
                    break;
                }
            }
        }
    });
    (runner, rx, snapshot_tx)
}

async fn poll_watcher(watcher: &mut DefraWatcher) -> Result<AgentRequest, String> {
    tokio::time::timeout(DELIVERY_TIMEOUT, watcher.next_request())
        .await
        .map_err(|_| "watcher timed out waiting for AgentRequest".to_string())?
        .ok_or_else(|| "watcher exhausted before emitting AgentRequest".to_string())?
        .map_err(|err| format!("watcher returned error: {err}"))
}

async fn install_event_delivery_source_schema(node: &EmbeddedNode) {
    let schema = r#"
        type EventDeliveryDoc {
            external_id: String @index
            payload: String
        }
    "#;
    node.add_schema(schema)
        .await
        .expect("add_schema for EventDeliveryDoc");
}

async fn install_subagent_source_fixture(node: &EmbeddedNode) -> Result<(), String> {
    const TOOL_SELECTION_ID: &str = "event-delivery-subagent-tools";

    upsert_tool_selection(
        node,
        &ToolSelectionDocument {
            selection_id: TOOL_SELECTION_ID.to_string(),
            agent_did: AGENT_DID.to_string(),
            subagent_targets: Some(vec![gents::subagent_target_entry(
                AGENT_NAME, AGENT_DID, AGENT_NAME, None,
            )]),
            subagent_spawn_enabled: Some(true),
            subagent_background_enabled: Some(true),
            ..Default::default()
        },
    )
    .await
    .map_err(|err| format!("upsert ToolSelection failed: {err}"))?;

    upsert_agent_behavior(
        node,
        &AgentBehaviorDocument {
            behavior_id: AGENT_NAME.to_string(),
            agent_did: AGENT_DID.to_string(),
            display_name: Some("Event delivery subagent fixture".to_string()),
            description: None,
            summary: None,
            system_prompt: None,
            request_context_template: None,
            backend_id: None,
            model_name: None,
            tool_selection_id: Some(TOOL_SELECTION_ID.to_string()),
            inference_profile_id: None,
            compaction_strategy: None,
            compaction_threshold: None,
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            enabled: true,
            created_at: Some("2026-05-20T00:00:00Z".to_string()),
        },
    )
    .await
    .map_err(|err| format!("upsert AgentBehavior failed: {err}"))?;

    Ok(())
}

fn active_snapshot_with_event_trigger() -> Arc<ActiveRuntimeSnapshot> {
    let task = ResolvedTask {
        task_id: EVENT_SOURCE_TASK_ID.to_string(),
        name: Some(EVENT_SOURCE_TASK_ID.to_string()),
        behavior_id: AGENT_NAME.to_string(),
        prompt_template: "handle event delivery doc".to_string(),
        output_schema_ref: None,
    };
    let trigger = ResolvedEventTrigger {
        trigger_id: EVENT_SOURCE_TRIGGER_ID.to_string(),
        task_id: task.task_id.clone(),
        task: task.clone(),
        source_collection: EVENT_SOURCE_COLLECTION.to_string(),
        event_kind: "created".to_string(),
        filter: None,
        enabled: true,
        concurrency: ConcurrencyMode::Serial,
        fire_mode: gents::EventTriggerFireMode::PerDocument,
        correlation_field: None,
        expected_count: None,
        expected_count_field: None,
        group_timeout_secs: None,
        group_min_count: 1,
        workspace_authority: None,
    };
    active_snapshot(
        HashMap::from([(trigger.trigger_id.clone(), trigger)]),
        HashMap::from([(task.task_id.clone(), task)]),
    )
}

fn active_snapshot_without_event_triggers() -> Arc<ActiveRuntimeSnapshot> {
    active_snapshot(HashMap::new(), HashMap::new())
}

fn active_snapshot(
    active_event_triggers: HashMap<String, ResolvedEventTrigger>,
    active_tasks: HashMap<String, ResolvedTask>,
) -> Arc<ActiveRuntimeSnapshot> {
    Arc::new(ActiveRuntimeSnapshot {
        generation: 1,
        principal: None,
        local_did: AGENT_DID.to_string(),
        paired_peer_dids: HashSet::new(),
        default_behavior_id: AGENT_NAME.to_string(),
        behaviors: HashMap::from([(AGENT_NAME.to_string(), runtime_behavior(AGENT_NAME))]),
        tool_surfaces: HashMap::new(),
        backend_admission_configs: HashMap::new(),
        unavailable_behaviors: HashMap::new(),
        active_schedules: HashMap::new(),
        unavailable_schedules: HashSet::new(),
        active_event_triggers,
        unavailable_event_triggers: HashSet::new(),
        active_tasks,
        dispatchers: HashMap::new(),
        behavior_executor_capacities: HashMap::new(),
        behavior_executor_queue_capacities: HashMap::new(),
    })
}

fn runtime_behavior(behavior_id: &str) -> Arc<gents::AgentBehavior> {
    let identity: Arc<dyn gents::AgentIdentity> = Arc::new(
        crate::support::fixtures::test_identity(&format!("event-delivery-{behavior_id}")),
    );
    let principal = Arc::new(gents::AgentPrincipal {
        agent_did: AGENT_DID.to_string(),
        identity,
        default_behavior_id: AGENT_NAME.to_string(),
        display_name: None,
        enabled: true,
    });
    Arc::new(crate::support::fixtures::test_behavior_for_principal(
        behavior_id,
        principal,
    ))
}

fn empty_event_delivery_world() -> lean_vocab_test::LeanEventDeliveryWorld {
    lean_vocab_test::LeanEventDeliveryWorld {
        persistent_set: Vec::new(),
        subscription_queue: Vec::new(),
        processed_set: Vec::new(),
        handled: Vec::new(),
    }
}

fn sanitize_graphql_id(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect()
}

fn mutation_doc_id(resp: &QueryResponse, field: &str) -> Option<String> {
    let value = resp.data.as_ref()?.get(field)?;
    value
        .get("_docID")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            value
                .as_array()
                .and_then(|rows| rows.first())
                .and_then(|row| row.get("_docID"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
}

fn erase_first(values: &mut Vec<String>, target: &str) -> Option<String> {
    values
        .iter()
        .position(|value| value == target)
        .map(|index| values.remove(index))
}

fn runtime_event_delivery_source_contract(name: &str) -> EventDeliverySourceContract {
    runtime_event_delivery_source_contracts()
        .into_iter()
        .find(|source| source.name == name)
        .unwrap_or_else(|| panic!("runtime event-delivery source {name:?} must be present"))
}
