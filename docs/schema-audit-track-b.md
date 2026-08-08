# Track B schema audit: responses and inference attempts

This audit covers the durable path from an `AgentRequest` through provider
attempts, streaming response state, transcript materialization, recovery, and
run-timeline projection. It applies the rules in
[`defradb-schema-guide.md`](defradb-schema-guide.md) to `AgentResponse`,
`InferenceCall`, and the response-facing parts of `AgentMessage` and
`RenderedRequest`.

Tracking issues: #1063 and #1070.

The document deliberately separates **current evidence** from **target
decisions**. A target decision is the proposed breaking contract for Track B;
it is not a claim that the current SDL or runtime already enforces it.

Retention classes and evidence downgrades use the
[shared retention and erasure lattice](schema-retention-lattice.md); this track
does not establish independent default durations.

## Executive finding

The current runtime has strong local lifecycle and crash-repair work, but its
database graph does not preserve those guarantees across duplicates,
replication, or archival:

- `AgentResponse` mixes a replaceable live stream overlay with terminal outcome
  state. Its successful terminal `content` and `reasoning` are cleared, while
  the durable answer is copied into `AgentMessage`.
- The only persisted response-to-message edge is a session-local integer
  sequence. It is not a DefraDB document reference and does not pin the message
  version.
- `InferenceCall` is updated by a random logical `call_id` even though creation
  returns `_docID`; the permit retains that `_docID` but does not use it.
- Provider attempts join to `AgentRequest`, backend configuration, and
  `RenderedRequest` only through mutable or application-generated values. There
  is no exact `InferenceCall -> RenderedRequest` edge.
- Recovery and client/timeline reads frequently use logical-ID filters with
  `limit: 1`. A unique index is not a cross-peer conflict policy, so these reads
  can select an arbitrary conflicting document after replication.
- Neither collection has ACP. Response and transcript collections are broadly
  present in participant replication profiles, while `InferenceCall` is local
  and absent from every profile. Retention and archival behavior are unstated.

The target is therefore a split model: immutable transcript messages and
terminal response outcomes are canonical facts; live stream state is an
explicitly replaceable observed-state projection; and every provider attempt
is a lifecycle-fenced ledger document linked by `_docID` and composite CID to
the exact request, rendered provider body, and configuration it used.

## Scope and evidence boundary

The primary evidence was read from the current worktree. Important sources are:

- schemas: `crates/gents-schemas/schemas/agent/agent_response.graphql:1`,
  `crates/gents-schemas/schemas/agent/agent_message.graphql:1`,
  `crates/gents-protocol/schemas/inference/inference_call.graphql:1`, and
  `crates/gents-schemas/schemas/agent/rendered_request.graphql:31`;
- response writer and materializer: `crates/gents/src/streaming.rs:155`,
  `crates/gents/src/streaming.rs:406`,
  `crates/gents/src/agent/stream_processor.rs:53`, and
  `crates/gents/src/hook/persistence/message_spawn.rs:13`;
- inference admission and persistence:
  `crates/gents/src/admission/client.rs:155`,
  `crates/gents/src/admission/controller.rs:175`,
  `crates/gents/src/admission/permit.rs:12`, and
  `crates/gents/src/admission/persistence.rs:43`;
- recovery: `crates/gents/src/lifecycle/recovery.rs:196`,
  `crates/gents/src/lifecycle/recovery.rs:349`,
  `crates/gents/src/admission/recovery.rs:36`, and
  `crates/gents/src/startup_recovery.rs:33`;
- reads and projection: `crates/gents-protocol/src/graphql.rs:808`,
  `crates/gents/src/background_tools.rs:2354`,
  `crates/gents/src/run_timeline_fetch.rs:27`, and
  `crates/gents/src/run_timeline.rs:429`;
- formal contracts: `crates/gents/proofs/Proofs/StreamingResponse/State.lean:79`,
  `crates/gents/proofs/Proofs/StreamingResponse/Transition.lean:5`, and
  `crates/gents/proofs/Proofs/InferenceCall/Transition.lean:5`;
- placement: `crates/gents-schemas/src/lib.rs:171`,
  `crates/gents/src/agent/p2p_reconcile/profiles.rs:83`, and
  `crates/gents/src/agent/p2p_reconcile/templates.rs:112`.

This audit does not assert that a claimed `agent_did` is the commit signer.
That unresolved verification boundary is tracked by issue #1064.

## Current fact graph

The graph below is what the code can currently correlate. A dashed edge is a
logical or inferred join, not a durable DefraDB reference.

```text
AgentRequest._docID + claimed composite CID
    | exact only in RenderedRequest
    +---------------------------> RenderedRequest
    |
    : request_id
    +----> AgentResponse (response_key duplicates request_id)
    |
    +----> InferenceCall (call_id is its update key)
    |
    +----> AgentMessage (request_id is optional/empty on some paths)

AgentResponse.materialized_message_sequence
    : (session_id, sequence)
    +----> AgentMessage

InferenceCall
    : no persisted edge
    + - -> RenderedRequest
```

DefraDB still provides a composite CID for every mutation of these documents,
including non-branchable `InferenceCall`. The application simply does not
persist or export the relevant version references.

## Canonical fact model

### Current evidence

| Collection/field set | Current role | Canonical today? | Evidence and consequence |
| --- | --- | --- | --- |
| `AgentResponse.content`, `reasoning` | Mutable in-memory-buffer snapshot and live client overlay | No | Flush replaces the whole tail (`streaming.rs:166-218`); tool-result and retry paths reset it (`streaming.rs:242-295`); normal finalize writes both fields to `""` (`streaming.rs:830-873`). |
| `AgentResponse.status`, error/timestamps | Response lifecycle and request-terminal bridge | Partly | Response and request normally terminalize in one GraphQL mutation (`streaming.rs:785-909`), but materialization is a prior mutation and recovery can create or mutate a response independently (`lifecycle/recovery.rs:349-543`). |
| `AgentMessage.content` | Serialized native message used to reconstruct provider history | Yes, after the turn is final | `load_history` decodes this column (`session/history.rs:7-48`). During an assistant tool-call turn the same row is upserted repeatedly (`hook/persistence/message_spawn.rs:13-76`), so it is not currently immutable. |
| `AgentMessage.role`, `reasoning` | Query/read convenience copies of data encoded in `content` | Derived duplicate | Dedicated reasoning is extracted from the serialized message (`hook/persistence/message_spawn.rs:79-89`). No stored source-version assertion proves that the copies agree. |
| `InferenceCall` row | Admission/provider-attempt lifecycle ledger | Yes for scheduling telemetry | It is created before admission/send and transitions through queued/running/terminal states (`admission/controller.rs:175-235`, `admission/persistence.rs:43-154`). It does not identify the exact provider body or config versions. |
| `RenderedRequest.request_json` | Exact canonical JSON body captured before HTTP send | Yes | The transport refuses the send when capture fails; the fact is immutable and request-version-pinned. It has no inference-call reference (`rendered_request/mod.rs:379-456`, `rendered_request/sink.rs:1-32`). |

### Target decisions

1. **`AgentMessage` is the immutable transcript fact.** A committed message is
   never updated. In-progress assistant assembly moves to a separate
   `AgentResponseLive` (or equivalently named) observed-state collection. If a
   durable pre-tool-effect checkpoint is required, append a typed message
   revision/event; do not overwrite a fact row.
2. **`AgentResponseOutcome` is the immutable terminal response fact.** It records
   one terminal outcome for one exact request document and optionally references
   the exact final/partial assistant message version. It contains no live token
   tail. `complete`, `error`, and `interrupted` are distinct outcomes.
3. **`AgentResponseLive` is a replaceable projection.** It owns streaming text,
   reasoning preview, progress counters, liveness timestamps, and local writer
   lease. It may be compacted or expired after a terminal outcome is durable.
4. **`InferenceCall` remains a canonical provider-attempt lifecycle ledger.** It
   may mutate only along the proven state machine. Its identity and lineage
   fields are immutable; terminal fields are write-once. Every actual HTTP send
   is linked to exactly one immutable `RenderedRequest` fact.
5. **`RenderedRequest` remains the provider-body fact.** Add an exact edge to the
   `InferenceCall` document/version that authorized the send. Do not infer that
   edge from `(request_id, attempt)` or timestamps.

## Cardinality and lifecycle contracts

### Target cardinality

| Edge | Cardinality | Contract |
| --- | --- | --- |
| `AgentRequest -> AgentResponseLive` | `0..1` active projection | At most one canonical live document per request document. A conflict is surfaced and writes stop until ownership is resolved. |
| `AgentRequest -> AgentResponseOutcome` | `0..1` terminal fact | Zero before response terminalization; exactly one after the response-producing execution terminalizes. Rebinding to a different outcome is illegal. |
| `AgentRequest -> AgentMessage` | `0..many` | Every request-produced transcript message carries the exact request `_docID`; legacy/session-only messages are not silently assigned by position. |
| `AgentResponseOutcome -> AgentMessage` | `0..1` final-message version | Required for `complete`; optional for `error`/`interrupted` when no assistant content was durably produced. The reference is `_docID` plus composite CID. |
| `AgentRequest -> InferenceCall` | `0..many` | Compaction, title, inference, scheduled, and one-off attempts are distinct ledger documents. |
| `InferenceCall -> RenderedRequest` | `0..1` | Zero for queue rejection/cancellation before send; exactly one for a provider request that crossed the HTTP send boundary. |
| `RenderedRequest -> InferenceCall` | exactly `1` for admitted calls | One-shot transports without admission require an explicit `call_kind = oneoff` attempt document or a separately declared non-admitted provenance class; empty-string joins are forbidden. |
| `InferenceCall -> backend/config` | exactly `1` version per used source | Persist `_docID` plus composite CID for backend, behavior, and inference profile versions that determined the call. |

### Current `AgentResponse` lifecycle

| State/operation | Current behavior | Durability gap |
| --- | --- | --- |
| begin | Pre-reads by `response_key == request_id`, then creates one `streaming` row with empty tails (`streaming.rs:610-688`). | Check-then-create is only node-serialized; replicated concurrent creates can survive. The row has no request `_docID`. |
| stream | Full buffered snapshots replace `content`/`reasoning`; progress updates are separate mutations (`streaming.rs:166-218`, `lifecycle/transition.rs:61-113`). | Multiple commits are useful live history but not a declared canonical transcript. `token_count` is whitespace-counted preview telemetry. |
| materialize | `AgentMessage` is written, then `AgentResponse` is marked by a `request_id` filter (`agent/stream_processor.rs:159-173`, `session/history.rs:493-514`). | The two writes are not atomic. The marker is a sequence, not a document/version reference, and the mutation can update duplicates. |
| finalize | Response tail is cleared and status/timestamps are written together with the request terminal transition (`streaming.rs:406-584`, `streaming.rs:785-909`). | Success depends on the earlier message marker. `AgentResponse.content` no longer represents the final answer despite clients reading it. |
| restart recovery | Every owner-scoped `streaming` row becomes `error`; partial content is retained/appended. Missing response rows are synthesized for processing requests (`lifecycle/recovery.rs:349-543`). | Recovery uses logical request joins to find absence and can create a competing response. Recovered tail semantics differ from normal finalize and from the model's canonical message. |

### Current `InferenceCall` lifecycle

| Transition | Current writer | Current guard | Gap |
| --- | --- | --- | --- |
| absent -> `queued` | admission controller | unique random `call_id` create | `_docID` is returned but subsequent normal writes ignore it. |
| absent -> `running` | immediate permit | unique random `call_id` create | Valid shortcut, but timestamps and immutable source fields are schema-optional. |
| `queued` -> `running` | queued permit acquisition | `upsert` by `call_id` | Update branch has no expected-state condition and can reopen a terminal row if identity is reused/corrupted. |
| live -> terminal | stream guard / permit drop | `upsert` by `call_id` | Update branch has no source-state fence; a later terminal write can replace a different terminal outcome. Persistence failure is logged after permit drop. |
| stale live -> terminal | startup sweep | exact `_docID` | This is the sound addressing pattern, but it is parent-gated by a `request_id` + `limit: 1` lookup (`admission/recovery.rs:111-148`). |

The target state machines are:

```text
AgentResponseLive: active -> superseded | expired
AgentResponseOutcome: absent -> complete | error | interrupted  (immutable)

InferenceCall: absent -> queued -> running -> completed | failed | cancelled
               absent -> running -> completed | failed | cancelled
               absent -> cancelled/failed  (pre-admission rejection only)
```

No terminal document may transition again. An idempotent observation of the
same terminal fact is a no-op, and a different terminal outcome is an integrity
conflict.

## Identity and version edges

### Current evidence

| Value | Current scope/use | Problem |
| --- | --- | --- |
| `response_key` | Equal to `request_id`; unique-indexed | Redundant application identity. It does not identify the request document and distributed conflicts remain possible. |
| `request_id` | Logical correlation across all four collections | Its schema uniqueness is not consistently declared; many correctness reads use it with `limit: 1`. |
| `materialized_message_sequence` | Joins response to `(session_id, AgentMessage.sequence)` | Sequence is an ordering label, not stable document identity. A duplicate sequence or wrong-session row can redirect the answer. |
| `message_key` | Unique idempotency key; commonly `session_id:sequence` | Delimiter encoding is not a durable relationship. Some paths update the row selected by this key. |
| `call_id` | UUID and unique index used for all normal call updates | It duplicates the identity DefraDB already returned. `AdmissionPermit._doc_id` is stored but unused (`admission/permit.rs:12-18`). |
| `call_seq` | Monotonic in one shared in-memory admission context | It restarts with a new context/process and is not a global or database invariant. |
| `attempt` | Task-local call policy value | It is not `RenderedRequest.attempt`; completion-loop retries can create later calls while the surrounding scope still reports attempt `1`. |
| `backend_config_fingerprint` | Hash stored by the inference writer | Self-attested query convenience, not a version/proof edge to `InferenceBackend`. |

### Target decisions

- Every response/live/outcome/call/message stores `request_doc_id` as an
  immutable direct edge. Where behavior depends on the consumed request
  snapshot, it also stores `request_commit_cid`.
- `AgentResponseOutcome.final_message` is a `DocumentVersionRef` represented as
  `final_message_doc_id` and `final_message_commit_cid`. Keep message sequence
  only for ordered display.
- `RenderedRequest.inference_call_doc_id` and
  `inference_call_commit_cid` name the running attempt snapshot that authorized
  the send. Queue-only attempts have no render.
- Normal call transitions target `InferenceCall._docID`, carry the expected
  source state, and verify the affected document. Remove `call_id` unless an
  external provider supplies a meaningful call identifier; if retained, rename
  it `external_call_id` and do not use it for database identity.
- Store configuration document/version references. A fingerprint may remain as
  an index, but never substitutes for those references.
- Store component fields for idempotency and ordering. Do not construct
  relationship keys by concatenating or rehashing identifiers. If DefraDB
  cannot enforce the needed composite index, keep an explicitly versioned
  idempotency encoding while retaining every component and a conflict policy.
- Export `_docID`, terminal composite CID, relevant field CIDs, signer evidence,
  and logical correlation fields. The run timeline must retain these references
  rather than `skip_serializing` them.

## Source-of-truth and duplication inventory

| Concept | Current copies | Target authority |
| --- | --- | --- |
| Final assistant body | `AgentMessage.content`; transiently `AgentResponse.content`; projected timeline/client fields | Exact immutable `AgentMessage` version. Response outcome references it; projections never copy it as a competing fact. |
| Reasoning | Encoded in `AgentMessage.content`, copied to `AgentMessage.reasoning`, previewed in `AgentResponse.reasoning` | Message envelope is canonical; any searchable/redacted reasoning projection carries its source message CID and policy decision. Live preview is explicitly lossy. |
| Request identity | `request_id` repeated in response, call, message, and render | `request_doc_id` for relationships; `request_id` only for user/protocol correlation. |
| Response identity | `response_key == request_id`, plus DefraDB `_docID` | `_docID`; uniqueness/conflict policy is over immutable `request_doc_id`. |
| Message identity | `message_key`, `(session_id, sequence)`, `_docID` | `_docID`; component fields express session order and idempotency scope. |
| Provider attempt identity | `call_id`, `(request_id, call_seq)`, and unrelated rendered `(capture_scope, turn_index, attempt)` | `InferenceCall._docID`, explicitly referenced from `RenderedRequest`. |
| Backend used | request `backend_id`, call `backend_id`/fingerprint, render endpoint/model, mutable config | Call stores exact backend config version; render stores observed endpoint/body. Both are needed and have different meanings. |
| Token usage | response whitespace `token_count`; inference provider prompt/completion/cache counts | Provider usage belongs to terminal `InferenceCall`; live token estimates are named approximate projection fields. |
| Terminal status | request lifecycle, response status, call status, message presence | Each fact owns its state; cross-document outcome transaction/recovery enforces the bridge. No reader guesses terminality from content. |

## Writer and query matrix

| Operation | Current addressing | File evidence | Target addressing/behavior |
| --- | --- | --- | --- |
| Begin response | `response_key/request_id` pre-read, then create | `streaming.rs:610-688`; `streaming/queries.rs:98-140` | Create live projection with immutable request version ref and writer lease; on duplicate, load all conflicts and fail closed. |
| Flush/reset live tail | response `_docID` + `status=streaming` | `streaming.rs:166-295` | Keep `_docID` plus lease/monotonic revision compare-and-set. Never mutate transcript facts. |
| Advance response progress | response `_docID` | `lifecycle/transition.rs:61-113` | Same document, expected revision/state, monotonic counters. |
| Materialize assistant message | append or upsert by logical key/sequence | `hook/persistence/message_spawn.rs:13-259`; `session/history.rs:68-255` | Append immutable message; acquire `_docID` and composite CID; reject non-identical idempotency conflicts. |
| Mark materialized | all responses matching `request_id` | `session/history.rs:493-514` | Create immutable outcome with exact message reference; no logical-ID fan-out update. |
| Finalize response + request | response `_docID`, request `request_id + agent_did + state` | `streaming.rs:785-909` | Outcome creation and exact request `_docID` terminal transition share a transaction; live projection is then disposable. |
| Recover response | scan owner `streaming`, mutate `_docID`; infer missing by request logical ID | `lifecycle/recovery.rs:349-543` | Recover from exact request/live/message refs. Multiple candidates are an integrity report, not `limit: 1`; append one outcome fact. |
| Create/update inference call | create returns `_docID`; update/upsert by `call_id` | `admission/persistence.rs:43-154`; `admission/persistence.rs:219-328` | Retain `_docID` in the permit and every async drop task; conditional update by `_docID` and expected state. |
| Recover inference call | stale call `_docID`; parent by `request_id limit:1` | `admission/recovery.rs:76-195` | Call stores request `_docID`; parent lookup is exact and signer/authority scoped. |
| Load child final answer | response by request ID, then message by session/sequence | `background_tools.rs:2354-2421` | Outcome by exact child request `_docID`, then message CID read; verify role, request edge, and signer. |
| Client turn state | request and response by `request_id limit:1` | `gents-protocol/src/graphql.rs:808-918` | Resolve request conflict explicitly, then traverse its exact outcome/live references. |
| Timeline fetch | request latest by logical ID; session-wide scans; inference calls by request ID | `run_timeline_fetch.rs:82-370` | Start from exact request `_docID` (or return all logical-ID conflicts), traverse direct edges, and include CIDs/signer status. |
| Timeline association | infer message request from response sequence and session position | `run_timeline.rs:478-558`; `run_timeline.rs:678-703` | Use immutable request/message/outcome refs only. Unknown legacy lineage remains unknown. |
| Goal activity/usage failure | existence/latest by `request_id`, `attempt DESC` | `trigger_engine/goal_source.rs:601-667` | Query exact request edge; order calls by declared per-request ordinal plus `_docID`, not ambiguous `attempt`. |

## Branchability, gossip, and late-peer backfill

### Current evidence

- `AgentResponse` and `AgentMessage` are `@branchable`, included in the desktop
  branchable sync list, the broad runtime/chat profiles, and requester-filtered
  conversation/machine/subagent-host templates
  (`gents-schemas/src/lib.rs:171-193`, `p2p_reconcile/templates.rs:112-191`).
- Their live replication filter is `requester_did`; that field is immutable in
  both schemas. Other scope fields, including `session_id` and `request_id`, are
  not immutable.
- `InferenceCall` is not branchable and appears in no P2P collection profile or
  pairing template. It is local operational telemetry and startup-recovery
  state.
- `RenderedRequest` is branchable but deliberately excluded from desktop bulk
  sync. It also appears in no participant pairing template.

### Target decisions

| Collection role | Live gossip | Late-peer backfill | Branchable |
| --- | --- | --- | --- |
| immutable `AgentMessage` | Owner to requester/session participants, filtered by immutable participant/tenant scope | Required for an authorized participant joining or reconnecting | **Yes** |
| immutable `AgentResponseOutcome` | Same participant scope as its request/message; terminal facts receive bounded convergence re-drive | Required | **Yes** |
| `AgentResponseLive` | Best-effort owner-to-active-requester only; never fleet-wide | Not required; a late peer reads outcome/messages | **No**, unless collection-scoped ACP is the only deployable enforcement mechanism |
| `InferenceCall` | None by default; optional filtered operator-observability channel, never participant gossip | Not required for execution recovery; governed archive supplies enterprise history | **No** in this schema. A future multi-host audit/handoff requirement gets a new branchable successor rather than assuming replication. |
| `RenderedRequest` | No participant gossip; optionally an encrypted/governed audit channel | Required only for authorized audit nodes, not chat clients | **Keep Yes** because the shipped choice is irreversible and governed audit backfill is a stated use; add an explicit audit-sync profile before relying on it |

Terminal response re-drive currently exists only for `AgentRequest`. The target
adds convergence repair for outcome/message facts or proves that branchable
backfill is sufficient. Live overlays must never be used as the only recovery
source.

## ACP, identity, and confidentiality

### Current evidence

None of the four schemas in this audit has `@policy`. Most runtime mutations use
identity-less `EmbeddedNode::execute`. The rendered-request sink attaches the
claimed agent DID to a `QueryRequest`, but its own documentation correctly notes
that the identity is inert without a policy (`rendered_request/sink.rs:19-32`).
Replication filters decide placement; they do not enforce reads after blocks
arrive. Reasoning and provider payloads therefore have no data-layer
least-privilege boundary today.

### Target decisions

- **Principal and deployment:** the agent principal DID is the owner/audit
  subject. Only a registered deployment authorized for that principal may
  create live state, calls, messages, renders, or outcomes. Commit signer
  verification must prove that delegation; a stored `agent_did` is insufficient.
- **Participant reads:** requester/session-participant identities may read their
  request outcome and redacted transcript. They do not automatically receive
  `InferenceCall`, provider endpoint, full rendered prompts, or reasoning.
- **Operator/auditor reads:** tenant operators and explicitly delegated auditors
  may read call telemetry and governed render captures. Access is relationship
  based and tenant-scoped, not a broad collection fallback.
- **Write boundaries:** requester may create the request command only. The
  authorized agent deployment creates/updates live response and calls and
  appends messages/outcomes. No participant may rewrite a terminal fact.
- **History target:** normal reads, CID reads, `_version`, and `_commits` should
  enforce the same or stricter relationships. The pinned paths are not uniform:
  mutation-result `_version` enrichment can bypass an equivalent read check, so
  this remains blocked on separate tests/upstream repair rather than assumed.
- **Registration:** policy resources and document relationships are installed
  before writes are enabled; an unregistered document fails closed. Tests must
  cover installation failure and the pinned DefraDB fallback behavior.
- **Confidentiality:** reasoning and rendered payloads use a stricter redaction
  class than ordinary assistant text. ACP is not encryption; local datastore,
  replicated-delta, archive, key-custody, and key-rotation controls remain
  separate deliverables.

## Retention, archive, and sunset

These are target policy classes; exact durations are deployment policy, not
hard-coded schema defaults.

| Data | Hot retention | Archive/export | Sunset/purge |
| --- | --- | --- | --- |
| `AgentResponseLive` | Active request plus short diagnostic grace | Normally none; sampled operational metrics only | Expire after outcome durability is verified. Tombstone is not evidence of physical erasure. |
| `AgentResponseOutcome` | Session lifetime plus tenant audit window | Required audit record with request/message version refs and signer/ACP evidence | Logical deletion is replicated; legal hold blocks purge; physical purge coordinates peers/backups. |
| `AgentMessage` | Conversation retention class | Versioned transcript export with redaction decision and all provenance refs | Participant-visible sunset plus governed purge; cryptographic erasure by key destruction where configured. |
| `InferenceCall` | Short operational/incident window | Aggregated metrics by default; full attempt records only for authorized audit/debug retention | Host-local purge after export and hold checks. Failure text is classified because it can contain provider or prompt data. |
| `RenderedRequest` | Shortest practical hot window | Encrypted governed archive only when audit/replay policy requires it | Separate high-sensitivity purge and key-destruction schedule; never inherit transcript retention implicitly. |

Every enterprise export includes schema version, `_docID`, composite and
relevant field CIDs, signer/signature status, request/message/render references,
logical IDs, ACP/redaction decision, and export contract version. A projection
that lacks signer or authorized CID-read evidence labels the record
`CapturedOnly`/unverified rather than upgrading it by inference.

## Illegal states and conflict behavior

### Response/message states that must be rejected

- more than one canonical live response or terminal outcome for one request
  document;
- `complete` without a final assistant `DocumentVersionRef`;
- `complete` with an error reason, or `error`/`interrupted` without a typed reason;
- terminal outcome without `terminalized_at`, or a live projection claiming a
  terminal fact;
- a final-message reference whose document is not an assistant message, belongs
  to another session/request/principal, or does not resolve at the named CID;
- a message whose typed query fields disagree with its canonical envelope;
- a committed message that is later updated;
- a progress or writer-lease revision that decreases;
- a participant-scope field that changes after creation;
- a logical-ID duplicate silently selected by `limit: 1`;
- recovered partial live text represented as if it were a normal completed
  assistant message.

### Inference states that must be rejected

- queued with `started_at`/`ended_at`; running without `started_at`; terminal
  without `ended_at`;
- completed with a failure reason; cancelled/failed without a typed reason;
- any terminal-to-live or terminal-to-different-terminal transition;
- immutable request/backend/behavior/config/call-kind/ordinal fields changing;
- a sent/completed provider attempt without a `RenderedRequest` reference, or a
  render referencing a different attempt/request;
- provider usage claimed on a row whose terminal provider response cannot be
  identified; partial usage must be explicitly typed rather than overloaded;
- two attempts with the same declared request/capture-scope/turn/attempt
  identity silently coexisting;
- recovery binding an attempt to whichever parent request happens to win a
  logical-ID query.

On concurrent create, collect all conflicting documents, verify signatures and
lineage, and expose the conflict. A deterministic `_docID` ordering may choose a
temporary projection winner, but it must not erase or authenticate the loser.

## Run timeline target

The current timeline is a useful UI projection, not an audit reconstruction. It
starts from latest `request_id`, scans session-wide rows, infers message lineage
from materialized sequence, sorts call rows using logical ordinals/timestamps,
and omits `_docID` from serialized output (`run_timeline.rs:45-177`,
`run_timeline.rs:592-703`). It does not load `RenderedRequest` at all.

The target timeline traverses a versioned fact graph:

```text
request DocumentVersionRef
  -> zero/one response outcome DocumentVersionRef
       -> zero/one final message DocumentVersionRef
  -> every request-scoped message DocumentVersionRef
  -> every inference-call DocumentVersionRef
       -> zero/one rendered-request DocumentVersionRef
       -> backend/behavior/profile DocumentVersionRefs
```

The CLI accepts `_docID` (preferred) or a logical `request_id`. A logical lookup
that finds more than one document returns a conflict set and requires selection;
it never chooses latest silently. Timeline events include version references,
signer verification state, redaction status, and recovery provenance. Legacy
rows with only inferred joins remain visibly `unverified_legacy`.

## Lean and conformance implications

This is lifecycle and provider-input work, so implementation must begin in the
formal layer.

1. Extend `StreamingResponse` into separate live-projection and immutable
   outcome/message states. Prove:
   - one request document cannot acquire two accepted outcomes;
   - completion requires an exact, matching assistant message version;
   - terminal outcome is immutable;
   - recovery never upgrades partial live text to completed output; and
   - outcome + exact request terminalization converge after every persistence
     cut point.
2. Replace `materializedMessageSequence : Option Sequence` as the proof-level
   completion edge with a message document/version reference. Retain sequence
   as the canonical transcript ordering property, allocated by the
   lease-fenced single writer defined in Track A.
3. Extend `InferenceCall` with immutable request/config identity, exact render
   linkage, timestamps, and typed terminal reasons. Prove terminal
   irreversibility against the executable persistence transitions, including
   queued-to-running and drop/recovery races.
4. Compose `InferenceCall` with `RenderedCapture`: send implies both a durable
   running attempt and a matching durable render; queue rejection implies no
   render/send; capture failure blocks send and terminalizes the attempt.
5. Add concurrency models for duplicate response/outcome/attempt creates and
   deterministic conflict projection without treating the winner as authentic.
6. Drive conformance cases for every crash cut point: message persisted before
   outcome, outcome before request terminal, dropped inference permit before
   terminal write, capture persisted before network failure, and startup sweep
   ordering.
7. Add DefraDB integration tests for replicated conflicting creates, late-peer
   backfill, filtered gossip, unauthorized normal/CID/history reads, exact CID
   timeline reconstruction, and archive/redaction export.

When policies land, model response finalization as the same sequence of
recoverable persistence cuts. The pinned policy-backed mutation path can split
an otherwise implicit multi-mutation transaction, so ACP installation must not
silently strengthen the formal model's atomicity assumption.

The existing Lean response model already proves bounded status flow, terminal
irreversibility, and a response/request bridge. It currently models completion
as if materialization were part of the finalize transition, while Rust writes
the message marker separately. The new model must expose those persistence cut
points rather than assume atomicity. The existing inference model preserves only
logical request/backend values and cannot validate current unguarded upserts.

## Breaking-schema implications

Track B should be treated as an intentional breaking schema generation, not an
in-place compatibility migration:

- introduce explicit `AgentResponseLive` and `AgentResponseOutcome` roles;
- make message/call identity and lineage fields immutable;
- replace response/message/call logical joins with `_docID`/CID fields;
- replace string timestamps with `DateTime` and empty-string absence with null;
- remove or rename ambiguous fields (`response_key`, call `attempt`, response
  `token_count`) instead of preserving misleading semantics;
- add exact render and config version references; and
- install ACP resources/relationships as part of bootstrap before enabling the
  new collections.

Existing stores require an explicit export/re-import or successor-collection
tool. The importer may preserve old rows as `unverified_legacy`, but must not
invent CIDs, signer evidence, request edges, or final-message identity. Because
`@branchable` cannot be enabled later, successor collection branchability must
match the decisions above at creation time.

## Prioritized child-issue candidates

### Active checkpoint: #1075 exact provider-attempt edge

The first Track B implementation checkpoint uses a bidirectional exact-version
contract instead of the logical/ordinal joins described in the original
inventory:

```text
signed running InferenceCall V1
    -> immutable RenderedRequest pins V1
    -> conditional InferenceCall V2 pins that RenderedRequest
    -> one HTTP send attempt
    -> terminal InferenceCall V3 preserves the binding
```

Each edge carries `_docID`, composite CID, and the verified commit signer DID.
All reads and writes on this path attach the node actor identity; signature
verification, rather than the query actor alone, supplies authorship evidence.
The binding write null-CASes all render fields and a zero-row result is accepted
only when exact reload observes the identical V2, so a concurrent different
render cannot be overwritten or mistaken for idempotency. One-off calls create
the same running call fact explicitly rather than bypassing provenance.

This checkpoint proves durable render and send authorization. It does not prove
that bytes reached the network, that the provider received or processed them,
or that a response belongs to the attempt; those require later transport and
response facts. `AgentResponseLive`/`AgentResponseOutcome` remains the next
broader Track B redesign, including subscription and crash-cut semantics.

1. **P0 — Formalize the response fact split and exact materialization edge.**
   Define live/outcome/message states in Lean; prove crash-cut convergence and
   terminal immutability; generate conformance cases.
2. **P0 — Link provider attempts to exact renders and request/config versions.**
   Compose `InferenceCall` with `RenderedCapture`; carry the call `_docID` into
   the transport scope and persist both directions needed for exact traversal.
3. **P0 — Replace logical-ID mutation and recovery addressing.** Change
   response materialization, inference running/terminal updates, parent
   recovery, and child final-response loading to `_docID`/CID; make duplicates
   explicit failures.
4. **P1 — Implement breaking Track B schemas.** Add immutable
   `AgentResponseOutcome`, replaceable `AgentResponseLive`, immutable message
   commits, typed timestamps/reasons, and exact version-reference fields. No
   compatibility migration is required; provide a legacy importer only if an
   existing deployment needs it.
5. **P1 — Enforce principal/deployment ACP at the data layer.** Install policies
   and relationships before writes, attach caller identity to every mutation,
   and test unrelated/anonymous normal, CID, version, and commit-history reads.
6. **P1 — Make timeline/export provenance-complete.** Traverse exact document
   edges, include render/call/config versions and signer state, surface logical
   duplicates, and version redaction/export contracts.
7. **P1 — Prove participant gossip and outcome convergence.** Test requester
   filters, immutable placement fields, live-overlay exclusion from backfill,
   terminal fact replay, and a late authorized peer.
8. **P2 — Implement retention and governed archival.** Separate live,
   transcript, inference telemetry, and rendered-payload retention; add legal
   hold, coordinated purge, and cryptographic-erasure evidence.
9. **P2 — Remove derived-key and source duplication.** Replace delimiter message
   keys and `response_key`, demote fingerprints/hashes to query indexes, and
   validate any retained typed projections against their source CID.

## Track B completion gate

Track B is complete only when the P0/P1 work above is implemented and the
following evidence exists:

- Lean proofs and generated conformance tests cover every target lifecycle and
  persistence cut point with zero `sorry`s;
- every correctness mutation and traversal uses exact document identity and,
  where version-sensitive, a composite CID;
- a sent provider request has one matching inference attempt and render fact;
- a completed response has one matching immutable assistant message version;
- duplicate and replicated conflicts fail closed and are visible in timeline
  output;
- participant/operator ACP tests cover normal, CID, `_version`, and `_commits`
  reads;
- gossip, late-peer backfill, archive, sunset, and legal-hold tests match the
  collection decisions; and
- `cargo test -p gents` plus `cargo check --workspace --all-targets` pass on the
  breaking schema generation.
