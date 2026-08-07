//! End-to-end fence for durable rendered-request capture (#840).
//!
//! `Proofs/RenderedCapture.lean` proves the order — a provider send is legal
//! only after the matching `(capture key, canonical request)` is durable — and
//! `crates/gents/src/agent/loop_stream/tests.rs` fences that order against the
//! owned loop with an in-process sink. What neither can reach is the claim this
//! slice actually makes: that the bytes in `RenderedRequest.request_json` are
//! the bytes the provider received, and that a sink failure stops the HTTP call
//! rather than merely being logged next to it.
//!
//! Both need a real HTTP round trip, so these tests run a full daemon against
//! the deterministic mock backend and compare the persisted payload with the
//! body that backend was posted.

use std::sync::Arc;
use std::time::Duration;

use gents::defra_node::EmbeddedNode;
use gents::graphql::escape_graphql_string;
use gents::llm::tool::{BoxFuture, ToolDefinition, ToolDyn, ToolError};
use gents::rendered_request::RenderedCompletionRequest;
use gents::{AgentIdentity, BehaviorBuilder, CompactionStrategy, Gents, ToolCeiling};
use serde_json::Value;

use crate::support::fixtures::test_identity;
use crate::support::interrupt::{
    create_runtime_request, wait_for_request_lifecycle_state, wait_for_runtime_ready, BootedAgent,
};
use crate::support::snapshots::fetch_request_snapshot;
use crate::support::streaming_backend::{
    MockStreamingBackend, StreamChunk, StreamPlan, StreamResponse, StreamScript,
};
use crate::support::test_db;

const CAPTURE_MODEL: &str = "capture-model";
const CAPTURE_BACKEND_ID: &str = "capture-backend";
const CAPTURE_BEHAVIOR_ID: &str = "capture-behavior";
const CAPTURE_TOOL: &str = "capture_probe";

/// The production parse-400 body that classifies as `ParseBadRequest` and so
/// reaches `PreStreamDirective::Repair`. Copied verbatim from
/// `completion_retry_tape.rs`, which is where it was captured from a real vLLM.
const PROD_PARSE_400_BODY: &str = r#"{"object":"error","message":"BadRequestError: Error in processing prompt inputs: Expecting value: line 1 column 28 (char 27)","type":"BadRequestError","code":400}"#;

/// The exact bytes the provider received must be the exact bytes on the row.
///
/// This is the claim the transport seam exists to make true. Capturing the
/// rig-assembled request would pass a weaker test — "the row looks like the
/// request we meant to send" — while the ChatGPT-Codex and Grok transports
/// rewrite the body underneath it. Comparing against the backend's own
/// observation is the only version of this assertion that cannot be satisfied
/// by a second serializer agreeing with itself.
#[tokio::test]
async fn the_persisted_request_json_is_the_body_the_provider_received() {
    let backend = MockStreamingBackend::start(
        CAPTURE_MODEL,
        vec![StreamScript::completes("capture-me", ["ok"])],
    )
    .expect("mock backend");
    let db = test_db("rendered-request-capture").await;
    let agent = boot_capture_agent(&db, "rendered-request-capture", backend.endpoint(), None).await;

    let doc_id = create_runtime_request(
        db.node.as_ref(),
        &agent.agent_did,
        CAPTURE_BEHAVIOR_ID,
        "req-capture-1",
        "session-capture-1",
        "please capture-me",
    )
    .await;
    wait_for_request_lifecycle_state(db.node.as_ref(), &doc_id, "completed").await;

    let observed = backend.observed_completion_bodies();
    assert_eq!(
        observed.len(),
        1,
        "the mock backend should have served exactly one completion"
    );

    let rows = wait_for_rendered_requests(db.node.as_ref(), "req-capture-1", observed.len()).await;
    let row = &rows[0];

    assert_eq!(
        parse_json(&row["request_json"]),
        canonical(&observed[0]),
        "the persisted payload must be the body the provider was posted"
    );
    assert_eq!(row["capture_scope"], "inference.1");
    assert_eq!(row["turn_index"], 0);
    assert_eq!(row["attempt"], 0);
    assert_eq!(row["source"], "openai_chat_completions");
    assert_eq!(row["session_id"], "session-capture-1");
    assert_eq!(row["agent_did"].as_str(), Some(agent.agent_did.as_str()));
    assert_eq!(
        row["model_name"], CAPTURE_MODEL,
        "the model column must name the model the provider was asked for"
    );
    assert!(
        row["capture_key"]
            .as_str()
            .is_some_and(|key| key.starts_with("rendered:v1:")),
        "unexpected capture key {:?}",
        row["capture_key"]
    );

    // Provenance says, positively, where the bytes were read.
    let provenance = parse_json(&row["provenance_json"]);
    assert_eq!(provenance["capture_seam"], "transport_body");
    assert_eq!(provenance["status"], "captured_only");
    assert_eq!(provenance["capture_scope"], "inference.1");
    assert!(
        provenance["assembly_trace"]
            .get("effective_messages")
            .is_none(),
        "a reconstructible turn must not duplicate its full transcript: {provenance}"
    );
    assert_eq!(
        provenance["assembly_trace"]["effective_message_count"], 1,
        "the compact trace still validates positional overlays"
    );

    agent.shutdown().await;
}

/// The fail-closed property, measured where it matters: at the provider.
///
/// `capture_failure_blocks_send` says a rejected capture leaves `sent`
/// unreachable. A sink that logged its failure and let the request through
/// would still pass every in-process test that only inspects the loop's error;
/// only the backend's own request count can distinguish "refused" from
/// "reported".
#[tokio::test]
async fn a_failing_capture_sink_issues_no_provider_request() {
    let backend = MockStreamingBackend::start(
        CAPTURE_MODEL,
        vec![StreamScript::completes("must-not-send", ["ok"])],
    )
    .expect("mock backend");
    let db = test_db("rendered-request-capture-faults").await;
    let agent = boot_capture_agent(
        &db,
        "rendered-request-capture-faults",
        backend.endpoint(),
        Some("injected rendered-request capture failure"),
    )
    .await;

    let doc_id = create_runtime_request(
        db.node.as_ref(),
        &agent.agent_did,
        CAPTURE_BEHAVIOR_ID,
        "req-capture-fault-1",
        "session-capture-fault-1",
        "please must-not-send",
    )
    .await;
    // Wait for *any* terminal state, then assert both halves separately. If the
    // zero-request assertion only ran after `lifecycle_state == "failed"`, a
    // fail-open sink would fail this test on a timeout instead of on the claim
    // under test, and the diagnostic would point at the wrong thing.
    let terminal = wait_for_request_terminal_state(db.node.as_ref(), &doc_id).await;

    assert_eq!(
        backend.observed_completion_requests(),
        0,
        "a failed capture must not issue the provider call; terminal state {terminal}, \
         bodies observed: {:?}",
        backend.observed_completion_bodies()
    );
    assert_eq!(
        terminal, "failed",
        "a request whose capture never succeeded must terminate as failed"
    );
    assert!(
        rendered_requests(db.node.as_ref(), "req-capture-fault-1")
            .await
            .is_empty(),
        "a failed capture must not leave a partial fact record"
    );

    agent.shutdown().await;
}

/// Redelivering the identical canonical request is a success without a write;
/// reusing the key for a different one is an integrity error, never an update.
/// This drives the sink directly because the loop can never produce the second
/// case — that is the point of proving it here rather than assuming it.
///
/// Row counts alone cannot tell "the sink skipped the write" from "the sink
/// rewrote the row with the same bytes", and the second is not idempotent: it
/// appends a commit and moves the content anchor this design takes integrity
/// from. So both outcomes are also measured against the row's `_commits`. The
/// commit for `request_json` is asserted by name in Rust rather than filtered
/// for in the query, because `_commits` evaluates its `fieldName` filter in
/// memory and a malformed one degrades to no filter at all.
#[tokio::test]
async fn capture_is_idempotent_and_never_rebinds_a_key() {
    let db = test_db("rendered-request-capture-idempotency").await;
    let sink = gents::rendered_request::DefraRenderedRequestSink::new(
        db.node.clone(),
        "did:key:z6MkCaptureIdempotency",
    );

    let first = rendered_fixture(serde_json::json!({"model": "m", "messages": [{"role": "user"}]}));
    sink.capture(first.clone()).await.expect("first capture");
    assert_eq!(
        rendered_requests(db.node.as_ref(), &first.request_id)
            .await
            .len(),
        1
    );

    let anchor = commit_set(db.node.as_ref(), &first.capture_key).await;
    assert!(
        anchor
            .iter()
            .any(|(field_name, _)| field_name == "request_json"),
        "the payload must have its own field commit; that CID is the version \
         anchor this design uses instead of a stored request_hash: {anchor:?}"
    );

    // Same key, same canonical value, keys reordered: still one fact.
    let mut redelivered = first.clone();
    redelivered.request_json = serde_json::json!({"messages": [{"role": "user"}], "model": "m"});
    sink.capture(redelivered)
        .await
        .expect("identical redelivery is idempotent");
    assert_eq!(
        rendered_requests(db.node.as_ref(), &first.request_id)
            .await
            .len(),
        1,
        "an idempotent redelivery must not write a second row"
    );
    assert_eq!(
        commit_set(db.node.as_ref(), &first.capture_key).await,
        anchor,
        "an idempotent redelivery must not write at all: the commit set, and so \
        the content anchor, must be untouched"
    );

    // Same key and provider body, different provenance: still a different
    // immutable fact. A future reconstructor trusts AssemblyTrace just as much
    // as request_json, so accepting this as idempotent would make a false
    // projection look verified.
    let mut provenance_conflict = first.clone();
    provenance_conflict.provenance_json["status_reason"] =
        Value::String("conflicting provenance".to_string());
    let error = sink
        .capture(provenance_conflict)
        .await
        .expect_err("provenance rebinding must be an integrity error");
    assert!(
        error.to_string().contains("integrity violation"),
        "unexpected error: {error:#}"
    );
    assert_eq!(
        commit_set(db.node.as_ref(), &first.capture_key).await,
        anchor,
        "a provenance conflict must not mutate the winning fact"
    );

    // Same key, different canonical value: integrity error, no write.
    let mut conflicting = first.clone();
    conflicting.request_json = serde_json::json!({"model": "m", "messages": []});
    let error = sink
        .capture(conflicting)
        .await
        .expect_err("a rebound key must be an integrity error");
    assert!(
        error.to_string().contains("integrity violation"),
        "unexpected error: {error:#}"
    );

    let rows = rendered_requests(db.node.as_ref(), &first.request_id).await;
    assert_eq!(rows.len(), 1, "the store must be left exactly as it was");
    assert_eq!(
        parse_json(&rows[0]["request_json"]),
        canonical(&first.request_json),
        "the original fact must survive the rejected rebinding"
    );
    assert_eq!(
        commit_set(db.node.as_ref(), &first.capture_key).await,
        anchor,
        "a rejected rebinding must leave no trace in the commit history either"
    );
}

/// Two attempts at one turn are two facts, and `attempt` is the only thing that
/// makes them so.
///
/// `RenderedCapture.attempt_distinguishes_facts` is proven in Lean, and for a
/// while nothing in Rust depended on it: a mutation probe that replaced the
/// loop's `attempt` with a literal `0` compiled and failed no test. The in-crate
/// seam test
/// (`agent::loop_stream::tests::capture_seam_reports_distinct_attempts_and_the_repair_build_path`)
/// now fences the loop's *arguments*; this fences the *durable rows*, which is
/// the thing #840 actually promises, and it does so through the full daemon,
/// transport, and DefraDB path where the counter has to survive a task-local
/// hand-off from the loop to the capturing HTTP client.
///
/// The scenario is deliberately the weakest one: a 503 that is retried with a
/// byte-identical request. Nothing about the payload distinguishes the two
/// provider calls, so if `attempt` did not reach the key, the sink would find
/// the key already bound to the identical canonical value, report an idempotent
/// success, and leave exactly one row behind — a durable fact record that
/// silently under-counts what the provider was actually sent.
///
/// The scope assertion is load-bearing for the same reason. `label_for` treats a
/// `(turn 0, attempt 0)` arm as the start of a new completion loop, so an
/// `attempt` pinned to `0` does not merely collapse the rows — it allocates a
/// second scope, `inference.2`, and produces two rows that look plausible until
/// you read their coordinates.
#[tokio::test]
async fn a_retried_attempt_is_its_own_durable_fact() {
    let marker = "capture-attempt-fence";
    let backend = MockStreamingBackend::start_with_plans(
        CAPTURE_MODEL,
        vec![StreamPlan::new(
            marker,
            vec![
                StreamResponse::service_unavailable(
                    "HTTP status 503 forcing exactly one transport retry",
                ),
                StreamResponse::completes(marker, ["recovered"]),
            ],
        )],
    )
    .expect("mock backend");
    let db = test_db("rendered-request-capture-attempts").await;
    let agent = boot_capture_agent(
        &db,
        "rendered-request-capture-attempts",
        backend.endpoint(),
        None,
    )
    .await;

    let doc_id = create_runtime_request(
        db.node.as_ref(),
        &agent.agent_did,
        CAPTURE_BEHAVIOR_ID,
        "req-capture-attempts",
        "session-capture-attempts",
        &format!("please recover {marker}"),
    )
    .await;
    wait_for_request_lifecycle_state(db.node.as_ref(), &doc_id, "completed").await;

    let observed = backend.observed_completion_bodies();
    assert_eq!(
        observed.len(),
        2,
        "one refused attempt and one recovery: {observed:?}"
    );
    // The premise the fence rests on: a transport retry resamples the same
    // request, so the two provider calls are byte-identical and `attempt` is
    // the only component of the capture key that can separate them.
    assert_eq!(
        canonical(&observed[0]),
        canonical(&observed[1]),
        "a transport retry must resample the same request; if this ever stops \
         being true, the fence below no longer isolates `attempt`"
    );

    let rows = wait_for_rendered_requests(db.node.as_ref(), "req-capture-attempts", 2).await;
    assert_eq!(
        coordinates(&rows),
        vec![("inference.1", 0, 0), ("inference.1", 0, 1)],
        "both provider calls belong to the same completion loop and the same \
         turn, and are distinguished only by the loop's own attempt counter"
    );
    assert_ne!(
        rows[0]["capture_key"], rows[1]["capture_key"],
        "distinct attempts must derive distinct capture keys"
    );
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(
            parse_json(&row["request_json"]),
            canonical(&observed[index]),
            "row {index} must carry the body the provider was posted"
        );
        assert_eq!(build_path(row), "budgeted");
    }

    agent.shutdown().await;
}

/// Every turn of a multi-turn, tool-using request is its own ordered fact, and
/// each one is the body the provider received.
///
/// `turn_index` is the other half of what the single-turn test cannot reach: one
/// request, one completion loop, several provider calls, each strictly extending
/// the last. The assertion that turn 1 carries the tool call id and the tool's
/// output is what makes the ordering meaningful — a capture that recorded turn
/// 1's coordinates but turn 0's bytes would satisfy the coordinate check alone.
#[tokio::test]
async fn a_multi_turn_tool_using_request_captures_every_turn_in_order() {
    const TOOL_OUTPUT: &str = "MULTI_TURN_TOOL_OUTPUT_47bd";
    let marker = "capture-multi-turn";
    let backend = MockStreamingBackend::start_with_plans(
        CAPTURE_MODEL,
        vec![StreamPlan::new(
            marker,
            vec![
                StreamResponse::streams(
                    marker,
                    vec![StreamChunk::tool_call(
                        "call-multi-1",
                        CAPTURE_TOOL,
                        r#"{"note":"first"}"#,
                    )],
                ),
                StreamResponse::completes(marker, ["done"]),
            ],
        )],
    )
    .expect("mock backend");
    let db = test_db("rendered-request-capture-multi-turn").await;
    let agent = boot_capture_agent_with(
        &db,
        "rendered-request-capture-multi-turn",
        backend.endpoint(),
        None,
        |behavior| behavior.custom_tool(FixedOutputTool::new(CAPTURE_TOOL, TOOL_OUTPUT)),
    )
    .await;

    let doc_id = create_runtime_request(
        db.node.as_ref(),
        &agent.agent_did,
        CAPTURE_BEHAVIOR_ID,
        "req-capture-multi-turn",
        "session-capture-multi-turn",
        &format!("please use the tool {marker}"),
    )
    .await;
    wait_for_request_lifecycle_state(db.node.as_ref(), &doc_id, "completed").await;

    let observed = backend.observed_completion_bodies();
    assert_eq!(
        observed.len(),
        2,
        "a tool-calling turn and the turn that reads its result: {observed:?}"
    );

    let rows = wait_for_rendered_requests(db.node.as_ref(), "req-capture-multi-turn", 2).await;
    assert_eq!(
        coordinates(&rows),
        vec![("inference.1", 0, 0), ("inference.1", 1, 0)],
        "one row per turn, in order, all inside one completion loop"
    );
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(
            parse_json(&row["request_json"]),
            canonical(&observed[index]),
            "turn {index} must carry the body the provider was posted"
        );
    }

    // Turn 1 is turn 0 plus the tool exchange, not a re-render of turn 0.
    let turn_zero = message_count(&rows[0]);
    let turn_one = message_count(&rows[1]);
    assert!(
        turn_one > turn_zero,
        "turn 1 must extend turn 0's message list ({turn_zero} -> {turn_one})"
    );
    let turn_one_body = row_text(&rows[1]);
    assert!(
        turn_one_body.contains("call-multi-1"),
        "turn 1 must thread the tool call id back to the provider"
    );
    assert!(
        turn_one_body.contains(TOOL_OUTPUT),
        "turn 1 must thread the tool's output back to the provider"
    );
    assert!(
        !row_text(&rows[0]).contains(TOOL_OUTPUT),
        "turn 0 was sent before the tool ran; its body cannot contain the result"
    );

    // The trace records the same exchange in native form, keyed by call id: this
    // is the leak set a reconstructor overlays onto rebuilt `AgentMessage` rows.
    let threaded = serde_json::to_string(
        &parse_json(&rows[1]["provenance_json"])["assembly_trace"]["threaded_tool_results"],
    )
    .expect("threaded tool results");
    assert!(
        threaded.contains("call-multi-1") && threaded.contains(TOOL_OUTPUT),
        "the assembly trace must carry the threaded tool result verbatim: {threaded}"
    );

    // #1066: the provenance manifest carries the exact admission join — the
    // `call_id`/`call_seq` the admission registry minted for the call this
    // capture preceded. This is the Rust fence for the task-local plumbing:
    // `next_call` writes the slot, the transport-side capture reads it, and
    // the values must be the ones persisted on the request's `InferenceCall`
    // rows, not an ordinal guess.
    let calls = inference_calls(db.node.as_ref(), "req-capture-multi-turn").await;
    let inference_call_seqs: Vec<i64> = calls
        .iter()
        .filter(|call| call["call_kind"].as_str() == Some("inference"))
        .filter_map(|call| call["call_seq"].as_i64())
        .collect();
    let mut seen_call_ids = std::collections::BTreeSet::new();
    for row in &rows {
        let manifest = parse_json(&row["provenance_json"]);
        assert_eq!(manifest["manifest_version"], 3, "manifest version");
        let admission = manifest
            .get("admission")
            .unwrap_or_else(|| panic!("daemon capture must carry an admission join: {manifest}"));
        let call_seq = admission["call_seq"].as_i64().expect("join call_seq");
        assert!(
            inference_call_seqs.contains(&call_seq),
            "joined call_seq {call_seq} must name a persisted inference InferenceCall \
             (persisted: {inference_call_seqs:?})"
        );
        let call_id = admission["call_id"].as_str().expect("join call_id");
        assert!(
            calls
                .iter()
                .any(|call| call["call_id"].as_str() == Some(call_id)),
            "joined call_id {call_id} must name a persisted InferenceCall row"
        );
        assert!(
            seen_call_ids.insert(call_id.to_string()),
            "each capture joins a distinct provider call"
        );
    }

    agent.shutdown().await;
}

/// A repaired attempt is a second fact whose bytes genuinely differ, and the row
/// says which builder produced it.
///
/// `PreStreamDirective::Repair` rewrites the assembled provider input in place —
/// it normalizes tool-call arguments — and rebuilds with `build_request` rather
/// than `build_budgeted_request`, so it also skips the output clamp. Both make
/// the repaired attempt a different provider request from attempt 0 with *no
/// transcript write in between*: nothing in `AgentMessage` records that the
/// arguments the provider saw the second time had their control characters
/// stripped. Without `build_path` on the row a reconstructor would replay the
/// budgeted path and report a false mismatch.
#[tokio::test]
async fn a_repaired_attempt_is_a_second_fact_with_a_different_canonical_request() {
    let marker = "capture-repair";
    // A raw control character in the tool-call arguments. `repair_provider_input`
    // strips it, which is what makes attempt 1's body differ from attempt 0's.
    let poisoned_arguments = format!("{{\"note\":\"bad{}value\"}}", '\u{0007}');
    let backend = MockStreamingBackend::start_with_plans(
        CAPTURE_MODEL,
        vec![StreamPlan::new(
            marker,
            vec![
                StreamResponse::streams(
                    marker,
                    vec![StreamChunk::tool_call(
                        "call-repair-1",
                        CAPTURE_TOOL,
                        poisoned_arguments,
                    )],
                ),
                StreamResponse::bad_request(PROD_PARSE_400_BODY),
                StreamResponse::completes(marker, ["repaired"]),
            ],
        )],
    )
    .expect("mock backend");
    let db = test_db("rendered-request-capture-repair").await;
    let agent = boot_capture_agent_with(
        &db,
        "rendered-request-capture-repair",
        backend.endpoint(),
        None,
        |behavior| behavior.custom_tool(FixedOutputTool::new(CAPTURE_TOOL, "REPAIR_TOOL_OUTPUT")),
    )
    .await;

    let doc_id = create_runtime_request(
        db.node.as_ref(),
        &agent.agent_did,
        CAPTURE_BEHAVIOR_ID,
        "req-capture-repair",
        "session-capture-repair",
        &format!("please use the tool {marker}"),
    )
    .await;
    // Wait for *any* terminal state and assert which one separately. A defect
    // that made the two attempts collide on one capture key ends this request
    // as `failed` — the sink rejecting the rebinding, correctly — and waiting
    // for "completed" would report that as a lifecycle timeout instead of as
    // the coordinate claim this test is about.
    let terminal = wait_for_request_terminal_state(db.node.as_ref(), &doc_id).await;
    assert_eq!(
        terminal, "completed",
        "the repaired attempt must be a second durable fact, not a rejected \
         rebinding of the first"
    );

    let observed = backend.observed_completion_bodies();
    assert_eq!(
        observed.len(),
        3,
        "the tool turn, its parse-400, and the repaired retry: {observed:?}"
    );

    let rows = wait_for_rendered_requests(db.node.as_ref(), "req-capture-repair", 3).await;
    assert_eq!(
        coordinates(&rows),
        vec![
            ("inference.1", 0, 0),
            ("inference.1", 1, 0),
            ("inference.1", 1, 1),
        ],
        "the repaired attempt is a second attempt at the same turn"
    );
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(
            parse_json(&row["request_json"]),
            canonical(&observed[index]),
            "row {index} must carry the body the provider was posted"
        );
    }

    assert_eq!(
        rows.iter().map(build_path).collect::<Vec<_>>(),
        vec!["budgeted", "budgeted", "repair"],
        "only the rebuilt attempt reports the repair path"
    );

    let original = parse_json(&rows[1]["request_json"]);
    let repaired = parse_json(&rows[2]["request_json"]);
    assert_ne!(
        original, repaired,
        "a repaired attempt is a different provider request, and no transcript \
         row records the difference"
    );
    let original_arguments = tool_call_arguments(&rows[1], "call-repair-1");
    let repaired_arguments = tool_call_arguments(&rows[2], "call-repair-1");
    assert_ne!(
        original_arguments, repaired_arguments,
        "the difference must be the repaired tool-call arguments"
    );
    // The wire form is a JSON string, so the control character travels as the
    // six-character escape `\u0007` rather than as a raw byte.
    assert!(
        original_arguments.contains(r"bad\u0007value"),
        "attempt 0 must carry the arguments the model actually emitted, control \
         character and all; got {original_arguments:?}"
    );
    assert!(
        !repaired_arguments.contains(r"\u0007") && repaired_arguments.contains("badvalue"),
        "the repaired attempt must carry the sanitized arguments; got \
         {repaired_arguments:?}"
    );

    agent.shutdown().await;
}

/// The post-compaction message list is captured, and later turns are assembled
/// from it.
///
/// Per-turn compaction is a *sticky* mutation of the loop's own state
/// (`*history = compacted; *new_messages = vec![compacted_prompt]`): one turn's
/// narrowing governs every later turn of the request, and nothing durable
/// records that it happened — no `AgentCompactionEntry` is written, and the
/// `AgentMessage` rows still hold the full tool result. So the captured bodies
/// are the only evidence of what the provider was actually shown from that turn
/// onward.
///
/// The strategy is `StripToolResults`, which is deterministic and needs no
/// provider call, so every completion body this test observes belongs to the
/// inference loop.
///
/// Two assertions make this a *stickiness* fence rather than a compaction
/// fence. The trace must agree with the captured body — a compaction that
/// narrowed the request without rewriting the loop's own state would leave the
/// two describing different conversations. And turn 2 must carry its own tool
/// result verbatim: if the compacted list were not retained, turn 2 would
/// reassemble from the full result, land over budget again, and be compacted a
/// second time, stripping the second result too.
#[tokio::test]
async fn per_turn_compaction_is_captured_and_governs_later_turns() {
    const BIG_MARKER: &str = "COMPACTED_AWAY_PAYLOAD_5f1c";
    const SMALL_MARKER: &str = "SECOND_TOOL_RESULT_9a02";
    // 40 000 chars: comfortably over the budget below, comfortably under the
    // 50 KiB threading cap so the result is not truncated before compaction.
    const BIG_OUTPUT_CHARS: usize = 40_000;
    const CONTEXT_WINDOW: usize = 40_000;
    const COMPACTION_THRESHOLD: f64 = 0.25;
    // `estimate_tokens` is `len / 4`, so the same budget in characters is 4x the
    // token budget: 10 000 tokens, 40 000 characters.
    let budget_chars = ((CONTEXT_WINDOW as f64 * COMPACTION_THRESHOLD) as usize) * 4;

    let big_output = format!("{BIG_MARKER}{}", "x".repeat(BIG_OUTPUT_CHARS));
    let marker = "capture-compaction";
    let backend = MockStreamingBackend::start_with_plans(
        CAPTURE_MODEL,
        vec![StreamPlan::new(
            marker,
            vec![
                StreamResponse::streams(
                    marker,
                    vec![StreamChunk::tool_call(
                        "call-compaction-1",
                        "capture_big",
                        r#"{"note":"big"}"#,
                    )],
                ),
                StreamResponse::streams(
                    marker,
                    vec![StreamChunk::tool_call(
                        "call-compaction-2",
                        "capture_small",
                        r#"{"note":"small"}"#,
                    )],
                ),
                StreamResponse::completes(marker, ["done"]),
            ],
        )],
    )
    .expect("mock backend");
    let db = test_db("rendered-request-capture-compaction").await;
    let agent = boot_capture_agent_with(
        &db,
        "rendered-request-capture-compaction",
        backend.endpoint(),
        None,
        |behavior| {
            behavior
                .enable_meta_tools(false)
                .enable_context_budget(false)
                .context_window(CONTEXT_WINDOW)
                .compaction_threshold(COMPACTION_THRESHOLD)
                .compaction_strategy(CompactionStrategy::StripToolResults)
                .custom_tool(FixedOutputTool::new("capture_big", big_output))
                .custom_tool(FixedOutputTool::new("capture_small", SMALL_MARKER))
        },
    )
    .await;

    let doc_id = create_runtime_request(
        db.node.as_ref(),
        &agent.agent_did,
        CAPTURE_BEHAVIOR_ID,
        "req-capture-compaction",
        "session-capture-compaction",
        &format!("please use both tools {marker}"),
    )
    .await;
    wait_for_request_lifecycle_state(db.node.as_ref(), &doc_id, "completed").await;

    let observed = backend.observed_completion_bodies();
    assert_eq!(
        observed.len(),
        3,
        "three inference turns and no summarizer call: {}",
        observed.len()
    );

    let rows = wait_for_rendered_requests(db.node.as_ref(), "req-capture-compaction", 3).await;
    assert_eq!(
        coordinates(&rows),
        vec![
            ("inference.1", 0, 0),
            ("inference.1", 1, 0),
            ("inference.1", 2, 0),
        ],
        "three turns of one completion loop, none of them retried"
    );
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(
            parse_json(&row["request_json"]),
            canonical(&observed[index]),
            "turn {index} must carry the body the provider was posted"
        );
    }

    // The premise: turn 0 starts under budget, so the compaction observed at
    // turn 1 is caused by the tool result and not by a preamble that has since
    // outgrown the window.
    let turn_zero_chars = row_text(&rows[0]).len();
    assert!(
        turn_zero_chars < budget_chars,
        "turn 0 must start under the compaction budget; it was {turn_zero_chars} \
         chars against a budget of {budget_chars}"
    );

    // Turn 1: the compacted list is what the provider was shown.
    let turn_one = row_text(&rows[1]);
    assert!(
        !turn_one.contains(BIG_MARKER),
        "turn 1 must not carry the compacted-away tool output"
    );
    assert!(
        turn_one.contains("see DefraDB AgentToolCall for full output]"),
        "turn 1 must carry the stub compaction left in its place; the body was \
         {} chars",
        turn_one.len()
    );

    // ...and the trace records that same narrowed list, in native form.
    let trace = parse_json(&rows[1]["provenance_json"])["assembly_trace"].clone();
    let effective = serde_json::to_string(&trace["effective_messages"]).expect("trace messages");
    assert!(
        !effective.contains(BIG_MARKER)
            && effective.contains("see DefraDB AgentToolCall for full output]"),
        "the assembly trace must be the post-compaction message list"
    );

    // Turn 2: assembled from the sticky compacted list. The first result is
    // still a stub, and — the sharp half — the second result is verbatim, which
    // is only possible if turn 2 was already under budget.
    let turn_two = row_text(&rows[2]);
    assert!(
        !turn_two.contains(BIG_MARKER),
        "a turn after the compaction turn must still be assembled from the \
         compacted list"
    );
    assert!(
        turn_two.contains(SMALL_MARKER),
        "turn 2 must carry its own tool result verbatim; a stripped one would \
         mean turn 2 compacted again, which is what stickiness prevents"
    );

    agent.shutdown().await;
}

/// The summarizer is a provider call too, and it runs *before* the request's
/// own completion loop exists.
///
/// `BehaviorDaemon::handle_request` compacts while it is assembling the prompt,
/// roughly ninety lines before `run_inference`. `StripThenSummarize` is the
/// default strategy, so for any session over its threshold that pre-request
/// compaction issues a real, model-backed provider call whose input is the
/// entire pre-truncated transcript. While the capture scope was installed
/// around `run_inference` only, that call had no ambient scope: its arming sink
/// no-opped, the transport found nothing pending and forwarded, and the loop's
/// backstop could not fire because nothing was armed. Zero rows, zero
/// diagnostics, for the single largest provider input the runtime produces.
///
/// The other compaction test deliberately picks `StripToolResults` precisely so
/// no summarizer runs, which is why that hole survived. This one forces the
/// summarizer and states the invariant at its strongest: every completion body
/// the backend was posted — inference, pre-request summarizer, and #988's
/// per-turn budget guard alike — has exactly one durable row carrying exactly
/// those bytes.
#[tokio::test]
async fn model_backed_compaction_is_captured_like_every_other_provider_call() {
    // 200 000-token window at a 0.25 threshold is a 50 000-token budget. The
    // seeded assistant turn is ~65 000 tokens, so request 2 is over budget at
    // prompt-assembly time and the daemon summarizes before it builds a loop.
    const CONTEXT_WINDOW: usize = 200_000;
    const COMPACTION_THRESHOLD: f64 = 0.25;
    const SEED_CHARS: usize = 260_000;
    const SEED_MARKER: &str = "capture-summarizer";
    /// The summarizer's own user turn. Unique to the compaction loop, so the
    /// mock can answer it with a checkpoint instead of prose — and matched
    /// first, because the summarizer's body also carries the transcript and so
    /// contains the inference marker too.
    const SUMMARIZER_MARKER: &str = "Produce the required structured continuation checkpoint now.";

    let checkpoint = serde_json::json!({
        "goal": "continue the seeded conversation",
        "constraints_and_preferences": [],
        "completed_work": ["seeded a long assistant turn"],
        "in_progress": [],
        "blockers": [],
        "current_work": ["answering the follow-up"],
        "key_decisions": [],
        "errors_and_fixes": [],
        "verification": [],
        "uncertainties": [],
        "next_actions": ["answer the follow-up turn"],
        "critical_context": [],
    })
    .to_string();

    let backend = MockStreamingBackend::start_with_plans(
        CAPTURE_MODEL,
        vec![
            StreamPlan::new(
                SUMMARIZER_MARKER,
                vec![StreamResponse::streams(
                    SUMMARIZER_MARKER,
                    vec![StreamChunk::text(checkpoint)],
                )],
            ),
            StreamPlan::new(
                SEED_MARKER,
                vec![
                    StreamResponse::streams(
                        SEED_MARKER,
                        vec![StreamChunk::text(format!(
                            "SEEDED{}",
                            "x".repeat(SEED_CHARS)
                        ))],
                    ),
                    StreamResponse::completes(SEED_MARKER, ["done"]),
                ],
            ),
        ],
    )
    .expect("mock backend");

    let db = test_db("rendered-request-capture-summarizer").await;
    let agent = boot_capture_agent_with(
        &db,
        "rendered-request-capture-summarizer",
        backend.endpoint(),
        None,
        |behavior| {
            behavior
                .enable_meta_tools(false)
                .enable_context_budget(false)
                .context_window(CONTEXT_WINDOW)
                .compaction_threshold(COMPACTION_THRESHOLD)
                .compaction_strategy(CompactionStrategy::StripThenSummarize)
        },
    )
    .await;

    let session_id = "session-capture-summarizer";
    let seed_doc = create_runtime_request(
        db.node.as_ref(),
        &agent.agent_did,
        CAPTURE_BEHAVIOR_ID,
        "req-capture-seed",
        session_id,
        &format!("seed the transcript {SEED_MARKER}"),
    )
    .await;
    wait_for_request_lifecycle_state(db.node.as_ref(), &seed_doc, "completed").await;
    let seed_bodies = backend.observed_completion_requests();
    assert_eq!(
        seed_bodies, 1,
        "the seeding request must be a single uncompacted completion"
    );

    let follow_up_doc = create_runtime_request(
        db.node.as_ref(),
        &agent.agent_did,
        CAPTURE_BEHAVIOR_ID,
        "req-capture-summarized",
        session_id,
        "and now a short follow-up",
    )
    .await;
    wait_for_request_lifecycle_state(db.node.as_ref(), &follow_up_doc, "completed").await;

    let follow_up_bodies = backend.observed_completion_bodies()[seed_bodies..].to_vec();
    assert!(
        follow_up_bodies.len() >= 2,
        "the follow-up must have summarized before answering; it issued {} \
         completion(s)",
        follow_up_bodies.len()
    );

    let rows = wait_for_rendered_requests(
        db.node.as_ref(),
        "req-capture-summarized",
        follow_up_bodies.len(),
    )
    .await;

    // The invariant, measured at the backend rather than at the loop: every
    // body the provider was posted has one row, and the row carries those
    // bytes. A summarizer call outside every capture scope fails here as a
    // count mismatch, not as a missing assertion.
    assert_eq!(
        rows.len(),
        follow_up_bodies.len(),
        "one durable row per provider call; rows {:?} against {} bodies",
        coordinates(&rows),
        follow_up_bodies.len()
    );
    let mut persisted = rows
        .iter()
        .map(|row| parse_json(&row["request_json"]))
        .collect::<Vec<_>>();
    let mut observed = follow_up_bodies.iter().map(canonical).collect::<Vec<_>>();
    let sort_key = |value: &Value| serde_json::to_string(value).expect("body re-serializes");
    persisted.sort_by_key(sort_key);
    observed.sort_by_key(sort_key);
    assert_eq!(
        persisted, observed,
        "the persisted rows and the bodies the provider received must be the \
         same set of requests"
    );

    // ...and the summarizer's row is its own fact, under its own capture scope,
    // rather than a rebinding of the inference loop's `(turn 0, attempt 0)`.
    let summarizer_rows = rows
        .iter()
        .filter(|row| {
            row["capture_scope"]
                .as_str()
                .is_some_and(|scope| scope.starts_with("compaction"))
        })
        .collect::<Vec<_>>();
    assert!(
        !summarizer_rows.is_empty(),
        "the summarizer's provider call must be captured under a compaction \
         scope; the rows were {:?}",
        coordinates(&rows)
    );
    for row in &summarizer_rows {
        assert!(
            row_text(row).contains("continuation checkpoint"),
            "a compaction-scoped row must carry the summarizer's own prompt"
        );
    }
    assert!(
        rows.iter().any(|row| row["capture_scope"] == "inference.1"),
        "the request's own completion loop must still be captured alongside it"
    );

    // The summary reached the transcript, which is what makes the summarizer's
    // input unrecoverable from anything but this row: the durable entry holds
    // the model's words, never the request that produced them.
    let entries = compaction_entry_count(db.node.as_ref(), session_id).await;
    assert_eq!(
        entries, 1,
        "the pre-request summarizer must have written its compaction entry"
    );

    agent.shutdown().await;
}

// ===== helpers =====

/// A tool with a fixed name and a fixed output.
///
/// `ToolDyn` is implemented directly rather than through `Tool` so the tool
/// never parses its arguments. The repair fence deliberately streams a tool call
/// whose arguments carry a raw control character, and a parsing tool would
/// reject it before the loop ever assembled the turn that gets repaired.
#[derive(Clone)]
struct FixedOutputTool {
    name: String,
    output: String,
}

impl FixedOutputTool {
    fn new(name: impl Into<String>, output: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            output: output.into(),
        }
    }
}

impl ToolDyn for FixedOutputTool {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn definition<'a>(&'a self, _prompt: String) -> BoxFuture<'a, ToolDefinition> {
        Box::pin(async move {
            ToolDefinition {
                name: self.name.clone(),
                description: "Test probe that returns a fixed string".to_string(),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": { "note": { "type": "string" } },
                    "required": ["note"]
                }),
            }
        })
    }

    fn call<'a>(&'a self, _args: String) -> BoxFuture<'a, Result<String, ToolError>> {
        Box::pin(async move { Ok(self.output.clone()) })
    }
}

/// `(capture_scope, turn_index, attempt)` for each row, in query order.
fn coordinates(rows: &[Value]) -> Vec<(&str, i64, i64)> {
    rows.iter()
        .map(|row| {
            (
                row["capture_scope"].as_str().unwrap_or("<missing>"),
                row["turn_index"].as_i64().unwrap_or(-1),
                row["attempt"].as_i64().unwrap_or(-1),
            )
        })
        .collect()
}

/// The builder that produced the captured request, as recorded in the manifest.
fn build_path(row: &Value) -> String {
    parse_json(&row["provenance_json"])["assembly_trace"]["build_path"]
        .as_str()
        .unwrap_or("<missing>")
        .to_string()
}

/// The captured body re-serialized, for substring assertions. Control characters
/// come back as `\uXXXX` escapes, exactly as they went out on the wire.
fn row_text(row: &Value) -> String {
    serde_json::to_string(&parse_json(&row["request_json"])).expect("captured body re-serializes")
}

/// The `arguments` string the captured body carried for one tool call id.
///
/// The wire form is a JSON *string* holding the argument object, so this is the
/// exact text the provider had to parse — which is what the parse-400 and its
/// repair are about.
fn tool_call_arguments(row: &Value, tool_call_id: &str) -> String {
    let body = parse_json(&row["request_json"]);
    let messages = body["messages"]
        .as_array()
        .cloned()
        .unwrap_or_else(|| panic!("captured body has no messages: {body}"));
    for message in messages {
        let Some(tool_calls) = message["tool_calls"].as_array() else {
            continue;
        };
        for tool_call in tool_calls {
            if tool_call["id"] == tool_call_id {
                return tool_call["function"]["arguments"]
                    .as_str()
                    .unwrap_or_else(|| panic!("tool call {tool_call_id} has no arguments string"))
                    .to_string();
            }
        }
    }
    panic!("captured body carries no tool call {tool_call_id}: {body}");
}

/// How many messages the captured body carried.
fn message_count(row: &Value) -> usize {
    parse_json(&row["request_json"])["messages"]
        .as_array()
        .map(Vec::len)
        .unwrap_or_default()
}

fn canonical(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonical).collect()),
        Value::Object(map) => {
            let sorted = map
                .iter()
                .map(|(key, value)| (key.clone(), canonical(value)))
                .collect::<std::collections::BTreeMap<_, _>>();
            Value::Object(sorted.into_iter().collect())
        }
        other => other.clone(),
    }
}

fn parse_json(value: &Value) -> Value {
    let text = value
        .as_str()
        .unwrap_or_else(|| panic!("expected a JSON string column, got {value}"));
    canonical(&serde_json::from_str::<Value>(text).expect("stored column must be valid JSON"))
}

fn rendered_fixture(request_json: Value) -> RenderedCompletionRequest {
    let agent_did = "did:key:z6MkCaptureIdempotency".to_string();
    let session_id = "session-idem".to_string();
    let request_doc_id = "bae-request-idem".to_string();
    let request_id = "req-idem".to_string();
    let capture_scope = "inference.1".to_string();
    let assembly_trace = gents::rendered_request::AssemblyTrace::from_effective_messages(
        gents::rendered_request::AssemblyBuildPath::Budgeted,
        Vec::new(),
    );
    RenderedCompletionRequest {
        capture_key: gents::rendered_request::capture_key(
            &agent_did,
            &session_id,
            &request_doc_id,
            &capture_scope,
            0,
            0,
        )
        .expect("capture key"),
        capture_version: gents::rendered_request::CAPTURE_VERSION,
        request_doc_id,
        request_id,
        capture_scope: capture_scope.clone(),
        turn_index: 0,
        attempt: 0,
        agent_did,
        requester_did: String::new(),
        behavior_id: "behavior".to_string(),
        session_id,
        model_name: "m".to_string(),
        source: gents::rendered_request::RenderedRequestSource::OpenAiChatCompletions,
        request_json,
        messages_json: serde_json::json!([]),
        tools_json: serde_json::json!([]),
        tool_choice_json: Value::Null,
        sampling_json: Value::Null,
        prompt_hash: "0".repeat(64),
        tools_hash: "0".repeat(64),
        provenance_json: serde_json::to_value(
            gents::rendered_request::ProvenanceManifest::captured_only(
                capture_scope,
                None,
                None,
                assembly_trace.clone(),
            ),
        )
        .expect("provenance"),
        assembly_trace,
    }
}

/// How many durable compaction entries a session has — the evidence that the
/// pre-request summarizer actually summarized rather than being skipped by the
/// gate.
async fn compaction_entry_count(node: &EmbeddedNode, session_id: &str) -> usize {
    let query = format!(
        r#"query {{
            CompactionEntry(filter: {{ session_id: {{ _eq: "{session_id}" }} }}) {{
                compaction_key
            }}
        }}"#,
        session_id = escape_graphql_string(session_id),
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "CompactionEntry query failed: {:?}",
        response.errors
    );
    response
        .data
        .and_then(|data| data.get("CompactionEntry").cloned())
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default()
        .len()
}

async fn rendered_requests(node: &EmbeddedNode, request_id: &str) -> Vec<Value> {
    let query = format!(
        r#"query {{
            RenderedRequest(filter: {{ request_id: {{ _eq: "{request_id}" }} }}) {{
                capture_key
                request_doc_id
                request_id
                session_id
                agent_did
                requester_did
                behavior_id
                capture_scope
                turn_index
                attempt
                capture_version
                model_name
                source
                request_json
                prompt_hash
                tools_hash
                provenance_json
            }}
        }}"#,
        request_id = escape_graphql_string(request_id),
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "RenderedRequest query failed: {:?}",
        response.errors
    );
    let mut rows = response
        .data
        .and_then(|data| data.get("RenderedRequest").cloned())
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default();
    rows.sort_by_key(|row| {
        (
            row["capture_scope"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            row["turn_index"].as_i64().unwrap_or_default(),
            row["attempt"].as_i64().unwrap_or_default(),
        )
    });
    rows
}

/// The persisted `InferenceCall` rows for one request — the other half of the
/// provenance admission join.
async fn inference_calls(node: &EmbeddedNode, request_id: &str) -> Vec<Value> {
    let query = format!(
        r#"query {{
            InferenceCall(filter: {{ request_id: {{ _eq: "{request_id}" }} }}) {{
                call_id
                call_seq
                call_kind
                attempt
            }}
        }}"#,
        request_id = escape_graphql_string(request_id),
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "InferenceCall query failed: {:?}",
        response.errors
    );
    response
        .data
        .and_then(|data| data.get("InferenceCall").cloned())
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default()
}

/// Every `(field_name, cid)` DefraDB holds for the row under `capture_key`,
/// sorted so two reads are comparable.
///
/// `_commits` takes exactly one document id — two or more is a parse error — so
/// the document id is resolved first. No `fieldName` filter is used: that filter
/// is applied in memory and a malformed one degrades to no filter at all, which
/// would make a filtered assertion pass for the wrong reason. The field names
/// come back and are checked in Rust instead.
async fn commit_set(node: &EmbeddedNode, capture_key: &str) -> Vec<(String, String)> {
    let doc_id_query = format!(
        r#"query {{
            RenderedRequest(filter: {{ capture_key: {{ _eq: "{capture_key}" }} }}, limit: 1) {{
                _docID
            }}
        }}"#,
        capture_key = escape_graphql_string(capture_key),
    );
    let response = node.execute(&doc_id_query).await;
    assert!(
        !response.has_errors(),
        "RenderedRequest _docID query failed: {:?}",
        response.errors
    );
    let doc_id = response
        .data
        .and_then(|data| data.get("RenderedRequest").cloned())
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default()
        .first()
        .and_then(|row| row["_docID"].as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| panic!("no RenderedRequest row for capture key {capture_key}"));

    let commits_query = format!(
        r#"query {{
            _commits(docID: "{doc_id}") {{
                cid
                fieldName
            }}
        }}"#,
        doc_id = escape_graphql_string(&doc_id),
    );
    let response = node.execute(&commits_query).await;
    assert!(
        !response.has_errors(),
        "_commits query failed: {:?}",
        response.errors
    );
    let mut commits = response
        .data
        .and_then(|data| data.get("_commits").cloned())
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default()
        .into_iter()
        .map(|commit| {
            (
                commit["fieldName"].as_str().unwrap_or("").to_string(),
                commit["cid"].as_str().unwrap_or("").to_string(),
            )
        })
        .collect::<Vec<_>>();
    assert!(
        !commits.is_empty(),
        "a stored RenderedRequest must have commits; an empty list is \
         'unavailable', never 'unchanged'"
    );
    commits.sort();
    commits
}

/// Wait for any terminal lifecycle state and report which one it reached.
async fn wait_for_request_terminal_state(node: &EmbeddedNode, request_doc_id: &str) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let snapshot = fetch_request_snapshot(node, request_doc_id).await;
        if matches!(
            snapshot.lifecycle_state.as_str(),
            "completed" | "failed" | "cancelled" | "expired"
        ) {
            return snapshot.lifecycle_state;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "request {request_doc_id} never reached a terminal state; last={}",
            snapshot.lifecycle_state
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_rendered_requests(
    node: &EmbeddedNode,
    request_id: &str,
    expected: usize,
) -> Vec<Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let rows = rendered_requests(node, request_id).await;
        if rows.len() >= expected {
            return rows;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected {expected} RenderedRequest rows for {request_id}, saw {}",
            rows.len()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn boot_capture_agent(
    db: &crate::support::TestDb,
    test_name: &str,
    endpoint: &str,
    capture_failure: Option<&str>,
) -> BootedAgent {
    boot_capture_agent_with(db, test_name, endpoint, capture_failure, |behavior| {
        behavior
    })
    .await
}

/// `customize` receives the behavior mid-build so a test can add tools or
/// change the compaction budget without each test rebuilding the whole agent.
async fn boot_capture_agent_with(
    db: &crate::support::TestDb,
    test_name: &str,
    endpoint: &str,
    capture_failure: Option<&str>,
    customize: impl FnOnce(BehaviorBuilder) -> BehaviorBuilder,
) -> BootedAgent {
    upsert_capture_backend(db.node.as_ref(), endpoint).await;
    let identity: Arc<dyn AgentIdentity> = Arc::new(test_identity(test_name));
    let mut builder = Gents::builder()
        .node(db.node.clone())
        .identity(identity)
        .default_behavior_id(CAPTURE_BEHAVIOR_ID)
        .tool_ceiling(ToolCeiling::meta_only());
    if let Some(message) = capture_failure {
        builder = builder.fail_rendered_request_capture_for_test(message);
    }
    let behavior = builder
        .behavior(CAPTURE_BEHAVIOR_ID)
        .backend_id(CAPTURE_BACKEND_ID)
        .model_name(CAPTURE_MODEL)
        .stream_batch_ms(0)
        .deadline_duration_secs(30);
    let agent = customize(behavior)
        .done()
        .build()
        .await
        .expect("build rendered-request capture agent");
    let agent_did = agent.agent_did().to_string();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(agent.run(shutdown_rx));
    wait_for_runtime_ready(db.node.as_ref(), &agent_did).await;
    BootedAgent::new(shutdown_tx, handle, agent_did)
}

async fn upsert_capture_backend(node: &EmbeddedNode, endpoint: &str) {
    let escaped_backend_id = escape_graphql_string(CAPTURE_BACKEND_ID);
    let escaped_endpoint = escape_graphql_string(endpoint);
    let escaped_model = escape_graphql_string(CAPTURE_MODEL);
    let mutation = format!(
        r#"mutation {{
            upsert_InferenceBackend(
                filter: {{ backend_id: {{ _eq: "{escaped_backend_id}" }} }},
                add: {{
                    backend_id: "{escaped_backend_id}",
                    name: "{escaped_backend_id}",
                    provider_kind: "OpenAiCompatible",
                    endpoint: "{escaped_endpoint}",
                    api_key: "",
                    api_key_env_var: "",
                    max_concurrent: 4,
                    max_queue_depth: 100,
                    enabled: true,
                    models: ["{escaped_model}"],
                    probe_status: "healthy"
                }},
                update: {{
                    name: "{escaped_backend_id}",
                    provider_kind: "OpenAiCompatible",
                    endpoint: "{escaped_endpoint}",
                    max_concurrent: 4,
                    max_queue_depth: 100,
                    enabled: true,
                    models: ["{escaped_model}"],
                    probe_status: "healthy"
                }}
            ) {{ _docID }}
        }}"#,
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "upserting the capture backend failed: {:?}",
        response.errors
    );
}
