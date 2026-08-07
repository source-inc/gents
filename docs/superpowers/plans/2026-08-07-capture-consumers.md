# Capture Consumers Implementation Plan (#1066)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `RenderedRequest` fact records readable — protocol row type + shared parse/ordering/manifest vocabulary, central `_commits` helper, run-timeline events with an exact admission join, metadata-only projection exposure, CLI `trace capture`, desktop rendering, and Lean coverage-ledger discharge.

**Architecture:** Shared capture vocabulary moves from `gents::rendered_request` to `gents-protocol::rendered_request` (runtime re-exports, producer call sites unchanged), mirroring the message-family precedent. All consumers read through that one vocabulary. Captured bodies never enter the timeline/projection model structurally; the only body read is the explicit CLI `--include-body`.

**Tech Stack:** Rust (workspace crates `gents-protocol`, `gents`, `gents-cli`), Lean 4 (`crates/gents/proofs`), TypeScript (desktop trace panel), DefraDB GraphQL.

**Spec:** `docs/superpowers/specs/2026-08-07-capture-consumers-design.md` (committed in this worktree). Read it first.

## Global Constraints

- Worktree: `/Users/johnzampolin/go/src/github.com/source-inc/gents-1066-capture-consumers`, branch `agent/1066-capture-consumers`, based on `415a2ef6`.
- Warm test builds before first test run: `cargo build --tests -p gents-protocol -p gents -p gents-cli` (check artifacts don't warm test artifacts).
- Always `crate::graphql::escape_graphql_string()` (or `gents_protocol::graphql::escape_graphql_string`) for GraphQL string literals. `_commits` `fieldName` is a string literal, never an identifier.
- Never emit `[]` in a DefraDB mutation — emit `null`. (Consumer side is read-mostly; applies to test fixtures.)
- GraphQL response errors are errors; missing `data` ≠ "no rows".
- Test gates: `cargo test -p gents` (never `--lib`), `cargo test -p gents-protocol`, `cargo test -p gents-cli`, `cd crates/gents/proofs && lake build` (zero `sorry`s), `cargo check --workspace --all-targets` before push.
- CLI tests that construct `EmbeddedNode`s must pin RocksDb (cli-shard flake class).
- `tracing`, never `println` (CLI user output via the existing `output` helpers in `gents-cli`).
- Serde field names must match GraphQL column names exactly; protocol rows: identity key required, everything else `#[serde(default)] Option<T>`.
- Commit after every task with a focused message ending in the Claude co-author trailer.

**Producer-side line references** (all valid at `415a2ef6`): capture DTO/types `crates/gents/src/rendered_request/mod.rs` (`RenderedRequestSource:123-146`, `AssemblyBuildPath:166-177`, `AssistantMessageId:193-197`, `ThreadedToolResult:216-224`, `AssemblyTrace:272-330`, `ProvenanceStatus:359-364`, `CaptureSeam:373-377`, `ProvenanceManifest:386-430`, constants `:78-92`, `capture_key():673-690`, canonical json `:701-725`); `CaptureScopeKind` `crates/gents/src/rendered_request/scope.rs:58-91`; manifest assembly `mod.rs:605-612` via `ProvenanceManifest::captured_only` `mod.rs:415-429`; admission context `crates/gents/src/admission/client.rs:155-207` (`next_call:189-202`), registry call site `crates/gents/src/admission/registry.rs:177`; timeline `crates/gents/src/run_timeline.rs` (`RunTimelineRows:7`, event enum `:295-303` at this commit, `event_sort_key:~772-827`), fetch `crates/gents/src/run_timeline_fetch.rs` (`load_run_timeline_rows:27-80`, inference query `:344-371`, request queries `:82-182`, `rows_or_empty_if_collection_missing:~483`); projections `crates/gents/src/adapter_projection.rs` (envelope `:73-98`, envelope schema `:990-1025`, dispatch `:366-402`), ATIF `adapter_projection/atif.rs`; CLI `crates/gents-cli/src/cli/args.rs:1029-1181`, `crates/gents-cli/src/commands/trace.rs`; ledger `crates/gents/proofs/Proofs/Conformance/CoverageLedger.lean:194-202` (deferral), `:915-924` (existing tagged rows); e2e helpers `crates/gents/tests/e2e_runtime/rendered_request_capture.rs:1201-1317`.

---

### Task 1: Move the capture vocabulary to `gents-protocol::rendered_request`

Pure move + re-export. No behavior change; the fence is that every existing test stays green and serialization is byte-identical.

**Files:**
- Create: `crates/gents-protocol/src/rendered_request.rs`
- Modify: `crates/gents-protocol/src/lib.rs` (add `pub mod rendered_request;` to the alphabetized module list)
- Modify: `crates/gents/src/rendered_request/mod.rs`
- Modify: `crates/gents/src/rendered_request/scope.rs`

**Interfaces:**
- Produces: `gents_protocol::rendered_request::{RenderedRequestSource, CaptureScopeKind, AssemblyBuildPath, AssistantMessageId, ThreadedToolResult, AssemblyTrace, ProvenanceStatus, CaptureSeam, ProvenanceManifest, CAPTURE_VERSION, PROVENANCE_MANIFEST_VERSION, ASSEMBLY_TRACE_VERSION}` — exact same serde shapes as today.
- `gents::rendered_request` re-exports all of the above at their current paths (`crate::rendered_request::ProvenanceManifest` etc. keep resolving).

- [ ] **Step 1: Cut the types from `gents`**

Move verbatim into `crates/gents-protocol/src/rendered_request.rs` (module doc: "Shared vocabulary for RenderedRequest capture rows. Producer lives in `gents::rendered_request`; every consumer reads through these types."):
- From `mod.rs`: constants `CAPTURE_VERSION`, `PROVENANCE_MANIFEST_VERSION`, `ASSEMBLY_TRACE_VERSION` (`:78-88`); `RenderedRequestSource` incl. `for_request_path`/`messages_field` and `COMPLETION_REQUEST_PATHS` (`:99-146`); `AssemblyBuildPath` (`:166-177`); `AssistantMessageId` (`:193-197`); `ThreadedToolResult` (`:216-224`); `AssemblyTrace` + constructors (`:272-330`); `ProvenanceStatus` (`:359-364`); `CaptureSeam` (`:373-377`); `ProvenanceManifest` + `captured_only` + `CAPTURED_ONLY_REASON` (`:386-430`).
- From `scope.rs`: `CaptureScopeKind` + `as_str` + `Display` (`:58-91`).
- Message imports become `crate::message::{Message, ToolResultContent}` (they already live in `gents-protocol`).
- `CAPTURE_KEY_PREFIX` and `capture_key()` STAY in `gents` (producer-only; key derivation is not consumer vocabulary).
- Unit tests colocated with the moved types move too if they only touch moved items; tests that touch the sink/scope machinery stay in `gents`.

- [ ] **Step 2: Re-export from `gents`**

In `crates/gents/src/rendered_request/mod.rs`, replace the moved definitions with:

```rust
pub use gents_protocol::rendered_request::{
    AssemblyBuildPath, AssemblyTrace, AssistantMessageId, CaptureSeam, CaptureScopeKind,
    ProvenanceManifest, ProvenanceStatus, RenderedRequestSource, ThreadedToolResult,
    ASSEMBLY_TRACE_VERSION, CAPTURE_VERSION, PROVENANCE_MANIFEST_VERSION,
};
```

In `scope.rs`, delete the local `CaptureScopeKind` and `use super::CaptureScopeKind;` (the `mod.rs:67-74` re-export block already exposes it; keep that block consistent).

- [ ] **Step 3: Verify no behavior change**

Run: `cargo test -p gents-protocol && cargo test -p gents rendered_request && cargo test -p gents --test conformance rendered_capture && cargo test -p gents --test e2e_runtime rendered_request_capture`
Expected: all PASS with zero source diffs outside the move.

- [ ] **Step 4: Commit** — `refactor: move capture vocabulary to gents-protocol (#1066)`

---

### Task 2: Scope parse, order key, manifest reader (TDD)

**Files:**
- Modify: `crates/gents-protocol/src/rendered_request.rs`

**Interfaces:**
- Produces:
  - `CaptureScope { pub kind: CaptureScopeKind, pub seq: u64 }` with `impl FromStr` (error type `CaptureScopeParseError`), `impl Display` (`"{kind}.{seq}"` round-trip), and `CaptureScopeKind::from_label(&str) -> Option<CaptureScopeKind>`.
  - `CaptureOrderKey { pub scope: CaptureScope, pub turn_index: i64, pub attempt: i64 }` with derived `PartialOrd/Ord` over `(scope.kind, scope.seq, turn_index, attempt)` (derive `Ord` on `CaptureScopeKind` in declaration order), plus `fn padded(&self) -> String` returning `format!("{}.{:010}.{:06}.{:06}", self.scope.kind, self.scope.seq, self.turn_index, self.attempt)` for lexical-sort contexts.
  - `ProvenanceManifest::parse(&str) -> Result<ParsedProvenance, ProvenanceParseError>` where:

```rust
pub const SUPPORTED_PROVENANCE_MANIFEST_VERSIONS: std::ops::RangeInclusive<u32> = 2..=3;

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedProvenance {
    Manifest(ProvenanceManifest),
    Unsupported { manifest_version: u32 },
}

#[derive(Debug, thiserror::Error)]  // use the crate's existing error idiom; anyhow if thiserror is absent
pub enum ProvenanceParseError { /* Empty, InvalidJson(..), MissingVersion */ }
```

  Parse peeks `manifest_version` from a `serde_json::Value` first; out-of-range → `Unsupported` (not an error); in-range → full typed deserialize (a v3 manifest with the Task 4 `admission` field must parse when the field is absent, i.e. the field is `#[serde(default)]`).

- [ ] **Step 1: Write failing tests** (in `#[cfg(test)] mod tests` of the same file)

```rust
#[test]
fn capture_scope_round_trips_and_rejects_garbage() {
    for label in ["inference.1", "compaction.2", "compaction_fallback.1", "title.1", "oneshot.1"] {
        let scope: CaptureScope = label.parse().expect(label);
        assert_eq!(scope.to_string(), label);
    }
    for bad in ["", "inference", "inference.", ".1", "inference.0x2", "mystery.1", "inference.1.2"] {
        assert!(bad.parse::<CaptureScope>().is_err(), "{bad}");
    }
}

#[test]
fn order_key_sorts_seq_numerically_not_lexically() {
    let key = |l: &str, t, a| CaptureOrderKey { scope: l.parse().unwrap(), turn_index: t, attempt: a };
    let mut keys = vec![key("inference.10", 0, 0), key("inference.2", 3, 1), key("inference.2", 3, 0)];
    keys.sort();
    assert_eq!(keys[0], key("inference.2", 3, 0));
    assert_eq!(keys[1], key("inference.2", 3, 1));
    assert_eq!(keys[2], key("inference.10", 0, 0));
    // padded() must agree with Ord under lexical sort
    let mut padded: Vec<String> = keys.iter().map(|k| k.padded()).collect();
    let sorted = padded.clone();
    padded.sort();
    assert_eq!(padded, sorted);
}

#[test]
fn manifest_reader_gates_on_version() {
    let v2 = serde_json::json!({
        "manifest_version": 2, "status": "captured_only", "status_reason": "r",
        "capture_seam": "transport_body", "capture_scope": "inference.1",
        "assembly_trace": { "trace_version": 2, "build_path": "budgeted",
            "effective_message_count": 0, "assistant_message_ids": [], "threaded_tool_results": [] }
    });
    assert!(matches!(ProvenanceManifest::parse(&v2.to_string()), Ok(ParsedProvenance::Manifest(_))));
    let v99 = serde_json::json!({ "manifest_version": 99 });
    assert!(matches!(ProvenanceManifest::parse(&v99.to_string()),
        Ok(ParsedProvenance::Unsupported { manifest_version: 99 })));
    assert!(ProvenanceManifest::parse("").is_err());
    assert!(ProvenanceManifest::parse("{\"status\":\"captured_only\"}").is_err()); // no version
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test -p gents-protocol rendered_request` → FAIL (types missing)
- [ ] **Step 3: Implement** the three items exactly as the Interfaces block specifies. `FromStr` splits on the LAST `.`, requires nonempty kind matched via `from_label`, `seq` via `str::parse::<u64>` rejecting leading `+`/whitespace; no silent defaults.
- [ ] **Step 4: Run to verify pass** — same command → PASS
- [ ] **Step 5: Commit** — `feat: capture scope parse, order key, versioned manifest reader (#1066)`

---

### Task 3: `RenderedRequestRow` in `gents-protocol::row`

**Files:**
- Modify: `crates/gents-protocol/src/row.rs` (append after `CompactionEntryRow:485-507`, its closest analogue)

**Interfaces:**
- Produces:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RenderedRequestRow {
    pub capture_key: String,
    #[serde(default)] pub request_doc_id: Option<String>,
    #[serde(default)] pub request_id: Option<String>,
    #[serde(default)] pub session_id: Option<String>,
    #[serde(default)] pub agent_did: Option<String>,
    #[serde(default)] pub requester_did: Option<String>,
    #[serde(default)] pub behavior_id: Option<String>,
    #[serde(default)] pub capture_scope: Option<String>,
    #[serde(default)] pub turn_index: Option<i64>,
    #[serde(default)] pub attempt: Option<i64>,
    #[serde(default)] pub capture_version: Option<i64>,
    #[serde(default)] pub model_name: Option<String>,
    #[serde(default)] pub source: Option<String>,
    #[serde(default)] pub request_json: Option<String>,
    #[serde(default)] pub prompt_hash: Option<String>,
    #[serde(default)] pub tools_hash: Option<String>,
    #[serde(default)] pub provenance_json: Option<String>,
    #[serde(default)] pub created_at: Option<String>,
}

impl RenderedRequestRow {
    pub fn order_key(&self) -> Result<crate::rendered_request::CaptureOrderKey, crate::rendered_request::CaptureScopeParseError>;
    pub fn provenance(&self) -> Result<crate::rendered_request::ParsedProvenance, crate::rendered_request::ProvenanceParseError>;
}
```

`order_key()` errors on missing/malformed `capture_scope` and treats missing `turn_index`/`attempt` as malformed (they are core facts; a row without them cannot be ordered — do not default to 0, which would silently collide with real first-turn rows).

- [ ] **Step 1: Write failing test** (row.rs test module) — deserialize a raw GraphQL-shaped `serde_json::json!` fixture with all 18 fields, assert `order_key()` == `("compaction", 2, 1, 0)` parse, and a second fixture missing `turn_index` errors from `order_key()` but still deserializes.
- [ ] **Step 2: Run** — `cargo test -p gents-protocol row::` → FAIL
- [ ] **Step 3: Implement**; **Step 4: Run** → PASS
- [ ] **Step 5: Commit** — `feat: RenderedRequestRow protocol row with order-key and provenance bridges (#1066)`

---### Task 4: Admission stamp — manifest v3

**Files:**
- Modify: `crates/gents/src/admission/client.rs`, `crates/gents/src/admission/mod.rs`
- Modify: `crates/gents-protocol/src/rendered_request.rs` (manifest field + version bump)
- Modify: `crates/gents/src/rendered_request/mod.rs` (`build_rendered_completion_request`), `crates/gents/src/rendered_request/scope.rs`, `crates/gents/src/rendered_request/transport.rs` (only if plumbing forces it — prefer reading the task-local inside `build_rendered_completion_request`)
- Test: `crates/gents/src/rendered_request/mod.rs` unit tests + `crates/gents/tests/e2e_runtime/rendered_request_capture.rs`

**Interfaces:**
- Produces:
  - `gents_protocol::rendered_request::AdmissionJoin { pub call_id: String, pub call_seq: i64 }`; `ProvenanceManifest.admission: Option<AdmissionJoin>` with `#[serde(default, skip_serializing_if = "Option::is_none")]`; `PROVENANCE_MANIFEST_VERSION` = 3; `SUPPORTED_PROVENANCE_MANIFEST_VERSIONS` = 2..=3.
  - `gents::admission::current_call_join() -> Option<(String, i64)>` (`pub(crate)`), reading a new `pub(super) current_call: Arc<std::sync::Mutex<Option<(String, i64)>>>` slot on `AdmissionCallContext`, written by `next_call` (`client.rs:189-202`) right after it mints `PendingCallMetadata` (store `(call_id.clone(), call_seq as i64)`). Follow the `current_session_id` precedent (`client.rs:279-283`) for the accessor shape. Verify the slot's `Arc` survives however `scope_call` (`client.rs:216`) constructs per-call contexts from the request-scope context — the transport capture must observe the value `next_call` wrote for THIS call. The e2e assertion in Step 4 is the fence for that plumbing, not code review.
- Consumes: Task 2's reader (a v3 manifest with `admission` absent must parse — oneshot).

- [ ] **Step 1: Failing unit test** in `rendered_request/mod.rs` tests: build a manifest via `ProvenanceManifest::captured_only(...)` with an injected join, assert serialized JSON contains `"manifest_version":3` and the `admission` object; build without a join (oneshot path) and assert the key is absent; `ProvenanceManifest::parse` accepts both.
- [ ] **Step 2: Run** — `cargo test -p gents rendered_request` → FAIL
- [ ] **Step 3: Implement**: `captured_only` gains `admission: Option<AdmissionJoin>` parameter; `build_rendered_completion_request` (`mod.rs:576-643`) passes `crate::admission::current_call_join().map(|(call_id, call_seq)| AdmissionJoin { call_id, call_seq })`. Update the existing oneshot caveat comment (`mod.rs:399-404`).
- [ ] **Step 4: Extend e2e**: in `rendered_request_capture.rs`, extend the multi-turn test to parse each captured row's provenance and assert (a) every `inference.*` capture carries `admission` whose `call_seq` matches a persisted `InferenceCall` row for the same request with `call_kind == "inference"`, and distinct captures carry distinct `call_id`s; (b) provenance for a title/oneshot capture (where the fixture produces one) has `admission: None`.
- [ ] **Step 5: Run** — `cargo test -p gents rendered_request && cargo test -p gents --test e2e_runtime rendered_request_capture` → PASS
- [ ] **Step 6: Commit** — `feat: stamp admission call_id/call_seq into capture provenance, manifest v3 (#1066)`

---

### Task 5: Central `_commits` helper

**Files:**
- Create: `crates/gents/src/rendered_request/commits.rs` (`pub mod commits;` in `mod.rs`)
- Test: same file, `#[cfg(test)]` + e2e usage in Task 8

**Interfaces:**
- Produces:

```rust
pub struct RequestJsonCommit { pub cid: String, pub height: i64 }

/// Ok(None) is explicit *Unavailable*: no commits, or none for request_json.
pub async fn request_json_commit(
    access: &crate::config::ConfigAccess,
    doc_id: &str,
) -> anyhow::Result<Option<RequestJsonCommit>>;
```

Contract (from spec §3): one docID; **no `fieldName` filter in the query** (in-memory `filter.matches(..).unwrap_or(true)` degrades malformed filters to no filter); select `cid height fieldName`; pick `fieldName == "request_json"` rows in Rust, take max `height`; `[]` → `Ok(None)`; GraphQL errors → `Err`. Query text:

```rust
let query = format!(
    r#"query {{ _commits(docID: "{doc_id}") {{ cid height fieldName }} }}"#,
    doc_id = escape_graphql_string(doc_id),
);
```

Route through the same `ConfigAccess` execution path `run_timeline_fetch.rs` uses (see `load_rows`/raw query execution there) so both Graphql and Local transports work.

- [ ] **Step 1: Failing unit test** with a canned response `Value` (happy path picks max-height `request_json` commit; response with commits but none for `request_json` → `None`; empty array → `None`; error body → `Err`). Factor the response→result selection into a pure `fn select_request_json_commit(response: &serde_json::Value) -> anyhow::Result<Option<RequestJsonCommit>>` so it unit-tests without a node.
- [ ] **Step 2: Run** — `cargo test -p gents rendered_request::commits` → FAIL; **Step 3: Implement**; **Step 4: Run** → PASS
- [ ] **Step 5: Commit** — `feat: central _commits request_json CID helper (#1066)`

---

### Task 6: Run timeline — capture events, ordering, #991/#841 fold-ins

**Files:**
- Modify: `crates/gents/src/run_timeline.rs`, `crates/gents/src/run_timeline_fetch.rs`
- Test: `run_timeline.rs` `#[cfg(test)]` (fixture-driven `build_run_timeline` tests live there)

**Interfaces:**
- Consumes: `RenderedRequestRow` (Task 3) is NOT used here — timeline has its own row structs with `_docID`; define `TimelineRenderedRequestRow` matching that local convention, **without a `request_json` field** (structural body exclusion — the query never selects it).
- Produces:

```rust
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineRenderedRequestRow {
    #[serde(default, rename = "_docID", skip_serializing)] pub doc_id: Option<String>,
    pub capture_key: String,
    #[serde(default)] pub request_doc_id: Option<String>,
    #[serde(default)] pub request_id: Option<String>,
    #[serde(default)] pub session_id: Option<String>,
    #[serde(default)] pub capture_scope: Option<String>,
    #[serde(default)] pub turn_index: Option<i64>,
    #[serde(default)] pub attempt: Option<i64>,
    #[serde(default)] pub capture_version: Option<i64>,
    #[serde(default)] pub model_name: Option<String>,
    #[serde(default)] pub source: Option<String>,
    #[serde(default)] pub prompt_hash: Option<String>,
    #[serde(default)] pub tools_hash: Option<String>,
    #[serde(default)] pub provenance_json: Option<String>,
    #[serde(default)] pub created_at: Option<String>,
}
```

  - `RunTimelineRows.rendered_requests: Vec<TimelineRenderedRequestRow>` (`#[serde(default)]` so existing fixture ingest keeps deserializing).
  - `RunTimelineEvent::RenderedRequest(TimelineRenderedRequestEvent)` (serde kind `"rendered_request"`), event fields: `capture_key, request_doc_id, request_id, capture_scope, scope_kind: Option<String>, scope_seq: Option<i64>, turn_index, attempt, capture_version, model_name, source, prompt_hash, tools_hash, provenance_status: Option<String>, manifest_version: Option<i64>, call_id: Option<String>, call_seq: Option<i64>, created_at` — scalars only; `provenance_status` is `"captured_only"` / `"unsupported_manifest"` / `"unavailable"` / `"malformed"` derived via Task 2's reader (parse failure → `"malformed"`, never a panic; `scope_kind`/`scope_seq` from `CaptureScope` parse, `None` on malformed).
  - `TimelineInferenceCallRow`/`TimelineInferenceCallEvent` + inference query gain `prompt_tokens`, `completion_tokens`, `cached_input_tokens` (`Option<i64>`, skip-if-none on the event) — #991.
  - `TimelineRequestRow`/`TimelineRequestEvent` + all three AgentRequest queries gain `execution_origin`, `caused_by_trigger_id`, `caused_by_trigger_kind` (`Option<String>`, skip-if-none on the event) — #841.
  - Fetch: `load_timeline_rendered_requests_for_session(access, session_id)` and `..._for_request(access, request_id)` (no-session fallback), selecting exactly the row's fields (never `request_json`), `order: { created_at: ASC }`, wrapped in `rows_or_empty_if_collection_missing` so pre-#1059 DBs yield empty. Wire into `load_run_timeline_rows` next to the per-session loop (`:47-51`).
  - Ordering in `event_sort_key`: family ranks become Request 0, **RenderedRequest 1**, InferenceCall 2, Message 3, ToolCall 4, Response 5 (ranks are unserialized internals — renumbering is safe; update the rank table comment). RenderedRequest: timestamp = `created_at` millis; intra rank = `call_seq.unwrap_or(i64::MAX)`; tiebreak = `CaptureOrderKey::padded()` when the scope parses, else `capture_key`.

- [ ] **Step 1: Failing fixture test** in `run_timeline.rs` tests: `RunTimelineRows` fixture with 2 inference calls (call_seq 1,2 with token fields), 3 rendered rows (`inference.1` turns 0/0, 0/1 with `call_seq` 1 and 2 stamped in provenance fixtures, plus `compaction.1` 0/0 with a **v99 manifest**), request row with trigger lineage. Assert: event order interleaves capture-before-its-call; token and lineage fields present on events; v99 row surfaces `provenance_status == "unsupported_manifest"`; seq-10-vs-2 ordering via two extra unstamped rows (`inference.10`/`inference.2` fixtures — unstamped path sorts by padded key).
- [ ] **Step 2: Run** — `cargo test -p gents run_timeline` → FAIL; **Step 3: Implement**; **Step 4: Run** → PASS; also `cargo test -p gents adapter_projection` (existing projections consume the enum — exhaustive matches gain an ignore arm here; Task 7 fills them in).
- [ ] **Step 5: Commit** — `feat: rendered-capture timeline events; fold InferenceCall tokens (#991) and trigger lineage (#841) into fetch (#1066)`

---

### Task 7: Projections — envelope + ATIF/LangGraph metadata, sentinel leak test

**Files:**
- Modify: `crates/gents/src/adapter_projection.rs`, `crates/gents/src/adapter_projection/atif.rs`
- Test: `crates/gents/src/adapter_projection/tests.rs`

**Interfaces:**
- Consumes: `RunTimelineEvent::RenderedRequest` events + `RunTimeline` (Task 6).
- Produces:
  - `RenderedCaptureSummary` (serialize-only struct in `adapter_projection.rs`): exactly the `TimelineRenderedRequestEvent` scalar fields minus nothing — derive it by `From<&TimelineRenderedRequestEvent>`.
  - `AdapterProjectionEnvelope.rendered_captures: Vec<RenderedCaptureSummary>` with `#[serde(default, skip_serializing_if = "Vec::is_empty")]`; envelope schema (`adapter_projection_json_schema:990-1025`) gains the matching `"rendered_captures"` array property (objects fully typed, `additionalProperties: false`), root stays `additionalProperties: false`.
  - ATIF: trajectory-level `extra.rendered_captures` = the same summaries as JSON; per-step `extra.rendered_capture` on the inference-derived step whose `InferenceCall.call_seq` equals a summary's `call_seq` (exact join only — no ordinal fallback).
  - LangGraph: default `values.rendered_captures` = same JSON summaries (the `apply_langgraph_metadata_hint` wholesale-replace path may drop it; that is the hint contract's existing semantics — do not fight it).
  - OpenAI-Codex / multi-agent native shapes: untouched (closed schemas); their envelope carries the section. `RunTimelineEvent::RenderedRequest` is explicitly ignored in their builders like `InferenceCall` already is (`:1710`, `:1986`).
- **Bodies never appear**: input events have no body fields (Task 6 structural guarantee); this task's test pins it end-to-end.

- [ ] **Step 1: Failing tests** in `adapter_projection/tests.rs`:

```rust
const BODY_SENTINEL: &str = "SENTINEL_RENDERED_BODY_9f3a";

#[test]
fn rendered_captures_surface_as_metadata_and_bodies_never_leak() {
    // fixture timeline: one request, two inference calls, two stamped rendered events,
    // provenance fixture strings containing BODY_SENTINEL inside an effective_messages
    // text (the realistic leak vector) — Task 6's event derivation must have dropped it.
    for kind in [AdapterProjectionKind::Atif, AdapterProjectionKind::OpenAiCodex,
                 AdapterProjectionKind::LangGraph, AdapterProjectionKind::MultiAgent] {
        for mode in [ProjectionRedactionMode::Full, ProjectionRedactionMode::TrainingSafe,
                     ProjectionRedactionMode::Public] {
            let envelope = build_adapter_projection(kind, &timeline, &ProjectionContext {
                actor_did: None, redaction_mode: mode });
            let serialized = serde_json::to_string(&envelope).unwrap();
            assert!(!serialized.contains(BODY_SENTINEL), "{kind:?}/{mode:?}");
            assert_eq!(envelope.rendered_captures.len(), 2, "{kind:?}/{mode:?}");
        }
    }
}
```

  (Note: the sentinel lives in the ROW fixture's `provenance_json`; Task 6's event carries only scalars, so the timeline built from rows must already be clean — build the fixture through `build_run_timeline(rows)`, not by hand-crafting events, so the test fences the whole pipeline.) Plus: an ATIF test asserting trajectory `extra.rendered_captures` length and the per-step join lands on the step with matching `call_seq`; a LangGraph test for `values.rendered_captures`; a schema-validation test running the existing `validate_adapter_projection_contract` over an envelope with captures.
- [ ] **Step 2: Run** — `cargo test -p gents adapter_projection` → FAIL; **Step 3: Implement**; **Step 4: Run** → PASS
- [ ] **Step 5: Commit** — `feat: metadata-only rendered-capture exposure in projection envelope, ATIF extras, LangGraph values (#1066)`

---

### Task 8: CLI `gents trace capture`

**Files:**
- Modify: `crates/gents-cli/src/cli/args.rs` (extend `TraceCommand:1029-1048`), `crates/gents-cli/src/commands/trace.rs`
- Test: `crates/gents-cli/tests/cli_trace_export.rs` (same harness; RocksDb-pinned EmbeddedNodes)

**Interfaces:**
- Consumes: `gents_protocol::row::RenderedRequestRow` (Task 3), `gents::rendered_request::commits::request_json_commit` (Task 5), Task 6 timeline (for `trace timeline` output assertions).
- Produces: `TraceCommand::Capture(TraceCaptureArgs)`:

```rust
#[derive(Debug, Parser)]
pub(crate) struct TraceCaptureArgs {
    #[arg(long)] pub(crate) home: Option<PathBuf>,          // same access pattern as TraceTimelineArgs
    #[arg(long)] pub(crate) graphql: Option<String>,
    #[arg(long, help = "Fetch one capture by its capture_key")] pub(crate) capture_key: Option<String>,
    #[arg(long, help = "List/narrow captures for a request")] pub(crate) request_id: Option<String>,
    #[arg(long, requires = "request_id")] pub(crate) scope: Option<String>,     // e.g. inference.1
    #[arg(long, requires = "request_id")] pub(crate) turn: Option<i64>,
    #[arg(long, requires = "request_id")] pub(crate) attempt: Option<i64>,
    #[arg(long, help = "List all matches instead of requiring exactly one")] pub(crate) list: bool,
    #[arg(long, help = "Include request_json and full provenance in the output")] pub(crate) include_body: bool,
    #[arg(long)] pub(crate) output_file: Option<PathBuf>,
}
```

Behavior: query `RenderedRequest` by key or filters (escape every literal; select all columns EXCEPT `request_json` unless `--include-body`); rows sort by `order_key()` (malformed scope → row still listed with an explicit `"order": "unparseable"` marker, sorted last by `capture_key`). Exactly-one match → JSON object: metadata + `request_json_commit` result rendered as `{"cid": ..., "height": ...}` or `"unavailable"` + (with `--include-body`) `request_json` and raw `provenance_json`. Multiple matches without `--list` → print metadata list, exit nonzero with a "narrow with --scope/--turn/--attempt or pass --list" error. Zero matches → nonzero exit, no existence-oracle wording beyond "no capture rows matched".

- [ ] **Step 1: Failing tests** in `cli_trace_export.rs` (follow its existing seeded-node pattern): seed two captures for one request (same scope, turns 0/1) with realistic v3 provenance; assert (a) `trace capture --request-id X --list` prints both with numeric-order keys; (b) `--request-id X --scope inference.1 --turn 0 --attempt 0` prints one object whose `commit.cid` is nonempty and which has NO `request_json` key; (c) `--include-body` includes it; (d) ambiguous without `--list` exits nonzero; (e) `trace timeline` output for the request contains `"kind":"rendered_request"` events.
- [ ] **Step 2: Run** — `cargo test -p gents-cli cli_trace_export` → FAIL; **Step 3: Implement** (`dispatch` arm in `trace.rs:43-50`, new `trace_capture` fn using the `ConfigAccess` construction `trace_timeline:52-63` uses); **Step 4: Run** → PASS
- [ ] **Step 5: Commit** — `feat: gents trace capture — fetch capture metadata with request_json field-commit CID (#1066)`

---

### Task 9: Desktop trace panel renders capture events

**Files:**
- Modify: `packages/gents-desktop-operations/src/components/trace/RequestTracePanel.tsx` (`eventSummary:151`, `eventTimestamp:140`)
- Test: `apps/gents-desktop/tests/request-trace.test.tsx`

**Interfaces:** consumes the serialized `"rendered_request"` event kind (open-shaped `RunTimelineEventView` — no type changes needed).

- [ ] **Step 1: Failing test**: render `RequestTracePanel` with a fixture timeline containing a `rendered_request` event (`capture_scope: "inference.1"`, `turn_index: 0`, `attempt: 1`, `model_name: "gpt-5"`, `provenance_status: "captured_only"`); assert the row shows a summary like `captured inference.1 turn 0 attempt 1 · gpt-5 · captured_only` and uses `created_at` for its timestamp. Follow the test file's existing fixture/render pattern.
- [ ] **Step 2: Run** the desktop test suite the way `apps/gents-desktop/package.json` defines (`npm test -- request-trace` from `apps/gents-desktop`) → FAIL
- [ ] **Step 3: Implement** the `case "rendered_request":` arm in `eventSummary` (and `eventTimestamp` if kinds are switched there) rendering exactly that summary from the event's fields with sensible fallbacks (missing model → omit segment).
- [ ] **Step 4: Run** → PASS
- [ ] **Step 5: Commit** — `feat: desktop trace panel renders rendered-capture events (#1066)`

---

### Task 10: Lean — conformance cases for the parse/order vocabulary, ledger discharge

**Files:**
- Modify: `crates/gents/proofs/Proofs/Conformance/ContractCases/RenderedCapture.lean`, `crates/gents/proofs/Proofs/Conformance/Contracts/Json/RenderedCapture.lean` (case emission), `crates/gents/proofs/Proofs/Conformance/CoverageLedger.lean`
- Modify: `crates/gents/tests/conformance/rendered_capture.rs` (consume new cases), `crates/gents/tests/support/conformance_consumers.rs` (register new consumers)

**Interfaces:** consumes Task 2 (`CaptureScope` parse), Task 8 test name, Task 9 test name.

- [ ] **Step 1: Scope cases in Lean.** In `ContractCases/RenderedCapture.lean`, add a `CaptureScopeCase` list: for each kind in the existing Lean capture vocabulary (inference, compaction, compaction_fallback, title, oneshot) emit `{ label := "<kind>.<seq>", kind := "<kind>", seq := <n>, valid := true }` cases (seqs 1, 2, 10 — 10 pins numeric ordering) plus invalid cases (`""`, `"inference"`, `"mystery.1"`, `"inference.0x2"`) with `valid := false`, and an `ordering` list pinning that `inference.2 < inference.10` at equal turn/attempt. Emit through `Contracts/Json/RenderedCapture.lean` following the existing key-case emission shape there.
- [ ] **Step 2: Consume from Rust.** In `tests/conformance/rendered_capture.rs`, add `generated_capture_scope_cases_pin_the_parser`: for each valid case `label.parse::<CaptureScope>()` succeeds with matching kind/seq; each invalid case errs; the ordering list matches `CaptureOrderKey` comparison. Run `cd crates/gents/proofs && lake build && cargo test -p gents --test conformance rendered_capture` → PASS (iterate until).
- [ ] **Step 3: Discharge the deferrals.** In `CoverageLedger.lean:194-202`: `required := [Surface.runtimeInternal, Surface.operatorCli, Surface.operatorUi]`, `deferred := []`. Next to the existing tagged rows (`:915-924`) add:

```lean
  , tagged (consumerCoverage
      "rendered_capture_cases"
      "RenderedCaptureCliSurface"
      "gents-cli::cli_trace_export::<actual Task 8 test fn name>")
      "rendered-capture" [Surface.operatorCli]
  , tagged (consumerCoverage
      "rendered_capture_cases"
      "RenderedCaptureDesktopSurface"
      "gents-desktop::request-trace::<actual Task 9 test name>")
      "rendered-capture" [Surface.operatorUi]
```

  Match the consumer-string format to what `conformance_consumers.rs` validates (read its existing entries first and register the two new consumers there in the same shape).
- [ ] **Step 4: Run** — `cd crates/gents/proofs && lake build` (zero `sorry`s) then `cargo test -p gents --test conformance` → PASS (coverage test enforces the discharge).
- [ ] **Step 5: Commit** — `feat: fence capture-scope parsing in conformance; discharge rendered-capture operatorCli/operatorUi deferrals (#1066)`

---

### Task 11: Final verification and PR

- [ ] **Step 1:** `cd crates/gents/proofs && lake build`
- [ ] **Step 2:** `cargo test -p gents-protocol && cargo test -p gents && cargo test -p gents-cli`
- [ ] **Step 3:** desktop test suite (as run in Task 9)
- [ ] **Step 4:** `cargo check --workspace --all-targets`
- [ ] **Step 5:** `git log --oneline origin/main..HEAD` sanity; push branch; open PR titled "Capture consumers: rendered-request fact records readable once, centrally (#1066)" with body covering: closes #1066; folds #991 + #841 timeline fields; explicitly NOT fixed: #842 (training_safe masking — nothing here routes bodies through redaction), #523 (reconstruction — unblocked, not built), ACP/encryption (defradb.rs#1318); manifest v2→v3 note for PR #1065's provenance slice; note the sibling 1059-worktree uncommitted ATIF/Harbor token-metrics slice overlaps only on the #991 timeline fields. End body with the standard generated-with footer.

## Self-Review Notes

- Spec §1→Tasks 1-3, §2→Task 4, §3→Task 5, §4→Task 6, §5→Task 7, §6→Task 8, §7→Task 9, §8→Task 10, error table→Tasks 2/5/6/8, testing section→each task's Step 1, gates→Task 11. No gaps found.
- Type names cross-checked: `CaptureScope`/`CaptureOrderKey`/`ParsedProvenance`/`AdmissionJoin`/`RenderedRequestRow`/`TimelineRenderedRequestRow`/`RenderedCaptureSummary` used consistently.
- Two deliberate verify-by-test seams (admission Arc plumbing in Task 4, consumer-string format in Task 10) are fenced by concrete named tests, not left open.
