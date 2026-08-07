# Capture consumers: make the RenderedRequest fact record readable, once, centrally

Date: 2026-08-07
Issue: #1066 (consumer side of #840's durable capture; producer landed in #1059 at `415a2ef6`)
Folds in: #991 (InferenceCall token fields), #841 (AgentRequest trigger lineage in timeline)
Out of scope: ACP/`@policy` (defradb.rs#1318), at-rest encryption, retention/off-switch,
verified reconstruction (#523), the `training_safe` redaction defect (#842 — but see §5:
nothing this design ships routes captured bodies through redaction at all).

## Goal

`RenderedRequest` rows accumulate from #1059 onward and nothing reads them. Make the fact
record reachable through the run timeline, the adapter projections, and the CLI — with the
three sharp reads (ordering, `_commits`, provenance manifest) implemented exactly once —
and keep captured bodies out of every default export as a positive default.

## Design

### 1. Shared vocabulary moves to `gents-protocol`

The repo's precedent is the message family: native types live in `gents-protocol`, the
runtime re-exports them. Captures follow it. A new `gents-protocol::rendered_request`
module takes ownership (moved, not mirrored — `gents::rendered_request` re-exports and
deletes its local definitions, so producer call sites do not change) of:

- `RenderedRequestSource`, `CaptureScopeKind`, `ProvenanceStatus`, `CaptureSeam`,
  `AssemblyBuildPath`, `AssistantMessageId`, `ThreadedToolResult`, `AssemblyTrace`,
  `ProvenanceManifest`, and the `CAPTURE_VERSION` / `PROVENANCE_MANIFEST_VERSION` /
  `ASSEMBLY_TRACE_VERSION` constants. `AssemblyTrace.effective_messages` already uses
  `gents-protocol::message::Message`, so the move introduces no new dependency edge.
- **`CaptureScope` parse**: `"{kind}.{seq}"` → `{ kind: CaptureScopeKind, seq: u64 }`,
  with `Display` round-trip. Malformed scope is an error, never a default.
- **`CaptureOrderKey`**: `(kind, seq, turn_index, attempt)` with derived `Ord`. This is
  the stable identity ordering within a request. It parses `seq` numerically — the e2e
  helper's lexical `capture_scope` sort would order `inference.10` before `inference.2`;
  the shared key is where that class of bug dies. Cross-loop interleaving *in time* is
  the timeline's job (created_at / admission join), not this key's.
- **Manifest reader**: `ProvenanceManifest::parse(&str)` peeks `manifest_version` first
  and returns `UnsupportedManifest { version }` for anything outside the supported range
  (2..=3 after §2) rather than guessing. Absent/empty provenance is `Unavailable`.

`RenderedRequestRow` joins its siblings in `gents-protocol::row` per the file's stated
convention: `capture_key: String` required, every other column `#[serde(default)]
Option<T>`, field names exactly matching the 18 GraphQL columns. A `RenderedRequestRow::
order_key()` accessor bridges to `CaptureOrderKey` (erroring on malformed scope), and
`provenance()` bridges to the manifest reader.

### 2. Producer stamp: exact admission join, manifest v3

Capture↔`InferenceCall` correspondence is currently ordinal; an admission rejection
desynchronises it. Fix at the source:

- `admission` gains a `pub(crate)` accessor exposing the *current call's* minted
  metadata (`call_id`, `call_seq`) — set when `acquire_current_call` mints
  `PendingCallMetadata`, cleared when the call scope ends. Note `AdmissionCallContext.
  call_seq` is the shared `Arc<AtomicU64>` counter, not the current value, and `call_id`
  today lives only in `PendingCallMetadata`; the accessor is the smallest honest seam.
- `ProvenanceManifest` gains optional `admission: Option<{ call_id, call_seq }>`
  (skip-if-none). One-shot runs never enter an admission scope and legitimately carry
  none — that stays a documented absence, not an error.
- `PROVENANCE_MANIFEST_VERSION` bumps 2 → 3. The reader accepts 2 and 3; v2 rows written
  since #1059 merged stay readable forever. (PR #1065's provisional slice wants an exact
  AgentRequest doc-version in provenance later; it will be v4 on the same reader pattern.)

### 3. The `_commits` read, once

`gents::rendered_request::commits` (runtime crate — it needs `ConfigAccess`):
`request_json_commit(access, doc_id) -> Result<Option<RequestJsonCommit>>` where the
`Option::None` is explicit *Unavailable*. Contract, lifted from the only existing
implementation (the e2e helper) and the #1059 plan's citation fixes:

- exactly one `docID` per query — two or more is a DefraDB parse error;
- **no `fieldName` filter in the query** — that filter is evaluated in memory and a
  malformed filter silently degrades to no filter; the helper fetches all commits and
  selects `fieldName == "request_json"` in Rust, taking max height;
- `[]` from `_commits` is `Unavailable`, never "unchanged";
- GraphQL response errors are errors — a missing `data` field is not "no rows".

The e2e helper keeps its own assertion-oriented `commit_set` (it deliberately inspects
*all* field commits); the CLI and any future reconstructor use this helper.

### 4. Run timeline

- `RunTimelineRows` gains `rendered_requests: Vec<TimelineRenderedRequestRow>`; the fetch
  layer loads them per session (fallback per request) via the existing
  `rows_or_empty_if_collection_missing` escape hatch, so a pre-#1059 database yields an
  empty section — never a failed timeline.
- New event variant `RunTimelineEvent::RenderedRequest(TimelineRenderedRequestEvent)`,
  kind `"rendered_request"` — **metadata only**: capture_key, request_doc_id, request_id,
  capture_scope (+ parsed kind/seq), turn_index, attempt, capture_version, model_name,
  source, prompt_hash, tools_hash, provenance status + manifest_version, the admission
  join (call_id/call_seq) when stamped, created_at. `request_json` is never on the event.
- Ordering: timestamp = `created_at`; persist-before-send means a capture's timestamp
  precedes its provider call's `started_at`, and the family rank places
  `rendered_request` immediately before `inference_call` on ties. Intra rank uses the
  stamped `call_seq` when present (exact join); the tiebreak string is the zero-padded
  order key (`{kind}.{seq:010}.{turn:06}.{attempt:06}`) so unstamped rows (v2, oneshot)
  still order deterministically and numerically.
- Fold-ins while in the fetch layer: `prompt_tokens` / `completion_tokens` /
  `cached_input_tokens` on `TimelineInferenceCallRow`+event+query (#991);
  `execution_origin` / `caused_by_trigger_id` / `caused_by_trigger_kind` on
  `TimelineRequestRow`+event+queries (#841). (Note: a sibling worktree holds uncommitted
  edits doing the #991 part plus an ATIF/Harbor token-metrics slice; the latter is not
  absorbed here.)

### 5. Adapter projections: metadata yes, bodies never

The #1059 plan imagined "decrypted request_json only in an explicitly authorized full
projection" — but ACP is deferred, so *authorized* cannot currently be expressed. Until
it can, projections carry capture **metadata only**, unconditionally. This is the
positive default the issue demands: Harbor invokes `trace project` with neither
`--redaction` nor `--actor-did`, redaction defaults to `Full`, ATIF `extra` bypasses
redaction — none of that matters if bodies never enter the projection model.

- The projection input is the timeline event (metadata-only by construction in §4), so
  the property holds structurally, and a leak test pins it: build every projection in
  every redaction mode over a fixture whose `request_json` contains a sentinel; assert
  the sentinel appears in no serialized output.
- `AdapterProjectionEnvelope` gains optional `rendered_captures` (array of the metadata
  shape; skip-if-empty; envelope schema updated, still `additionalProperties: false`
  with the new property defined). This gives all four projections one documented,
  uniform surface even where native shapes are closed.
- Native shapes: ATIF attaches trajectory-level `extra.rendered_captures` and, where the
  admission join identifies a step's call, per-step `extra.rendered_capture`; LangGraph
  adds `values.rendered_captures`. OpenAI-Codex and multi-agent native shapes are closed
  (`additionalProperties: false` throughout) and stay untouched — their envelope carries
  the section.

### 6. CLI

- `gents trace timeline` surfaces capture events with no further work (it serializes the
  timeline).
- New `gents trace capture`: fetch by `--capture-key`, or by `--request-id` with optional
  `--scope/--turn/--attempt` narrowing. Default output is metadata plus the
  `request_json` field-commit CID from §3 (CID lookup requires the doc; `Unavailable` is
  printed honestly). `--include-body` opts into printing `request_json` and the full
  provenance manifest — the one deliberate, explicit body read in the system.
  Multiple matches print the metadata list (with order keys) and exit nonzero unless
  listing was the intent (`--list`). `--output-file` / JSON output follow `trace
  timeline` conventions.

### 7. Desktop (operatorUi)

The timeline event flows through `desktop_request_timeline`'s untyped passthrough and the
client's open-shaped `RunTimelineEventView` with zero plumbing. `RequestTracePanel.
eventSummary` gains a `rendered_request` case (scope, turn/attempt, model, provenance
status) plus a test — that is the operatorUi consumer.

### 8. Lean coverage ledger

`CoverageLedger.lean`'s `rendered-capture` entry: delete both deferrals, move
`operatorCli` and `operatorUi` into `required`, and add tagged consumer rows pointing at
the CLI trace-capture test and the desktop trace-panel test (consumer registry entries
included). `lake build` stays at zero `sorry`s; the conformance coverage test enforces
the discharge. No lifecycle/invariant change is being made, so no new Lean model is
required; the existing `RenderedCaptureKeyCases` continue to pin the key tuple. The
scope-parse and order-key comparator added in §1 get conformance cases emitted through
the existing RenderedCapture contract JSON so the parser is spec-fenced, not just
unit-tested.

## Error handling summary

| Condition | Behavior |
|---|---|
| Pre-#1059 DB, no collection | empty timeline section; no error |
| Malformed `capture_scope` | row surfaces with an explicit parse error marker in CLI; excluded from order-sensitive joins; never a panic |
| Unknown `manifest_version` | `UnsupportedManifest` surfaced as provenance status; row still listed |
| `_commits` returns `[]` or no `request_json` field | explicit `Unavailable` |
| Multiple rows for one `(request, scope, turn, attempt)` | integrity error (unique `capture_key` makes it corruption) |

## Testing

- Unit: scope parse/round-trip, order-key comparator (incl. seq ≥ 10), manifest version
  gate (2 ok, 3 ok, 4 unsupported, garbage error), row deserialization from a raw
  GraphQL response fixture.
- Conformance: scope/order cases emitted from Lean; ledger discharge test.
- Timeline: fixture with retries + compaction loop asserting capture events interleave
  correctly with inference calls (stamped and unstamped).
- Projection: the sentinel leak test across all four projections × three redaction
  modes; envelope schema validation.
- CLI: `trace capture` happy path, `--include-body`, ambiguous match, missing collection
  (`cli_trace_export`-style harness); timeline output includes capture events.
- E2E: extend `rendered_request_capture.rs` with a read-back through
  `load_run_timeline` + CLI path against a multi-turn retry session.
- Desktop: `request-trace.test.tsx` case for the new event kind.

## Verification gates

`cd crates/gents/proofs && lake build` · `cargo test -p gents` (full package) ·
`cargo test -p gents-protocol -p gents-cli` · desktop test suite ·
`cargo check --workspace --all-targets`.
