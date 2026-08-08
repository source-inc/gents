# Track A schema audit: conversation and session durability

Status: **provisional architecture decision** for issues #1063 and #1068. This document is
an evidence-backed audit, not an assertion that the current schemas already
enforce the recommended contract.

## Scope and method

This audit covers `AgentSession`, `AgentConversation`, `AgentRequest`,
`AgentMessage`, `AgentToolCall`, `AgentToolResult`, `AgentToolApproval`,
`CompactionEntry`, `Goal`, and `AgentMemory`. It applies the vocabulary and
decision template in [DefraDB Schema Design for Gents](defradb-schema-guide.md)
and [Schema Decision Ledger](schema-decision-ledger.md).

Evidence came from the current SDL, canonical Rust writers, lifecycle and
recovery queries, Lean models, replication templates, projection/export code,
and the pinned DefraDB implementation. Recommendations are labelled as such.
No recommendation below is implemented merely by recording it here.

Retention terms and evidence downgrades in this track are governed by the
[shared retention and erasure lattice](schema-retention-lattice.md). Any
example duration below is illustrative deployment policy, not a schema default.

The intended end state is a database-enforced conversation plane that can move
between authorized hosts without changing identity, authority, ordering, or
provenance. A host is an executor, not the owner of the truth.

## Executive decision

All ten collections need a breaking schema pass. Retain `@branchable` for each
one, but for concrete reasons: authorized peers must be able to initiate
collection catch-up, and these collections will require collection-scoped ACP.
A push replicator can also backfill non-branchable documents, so branchability
is not being used as a synonym for all backfill, document history, or live
replication.

The current collection names obscure four different roles that must be
separated:

1. Immutable commands and facts: request intent, messages, tool invocation,
   tool output, approval decisions, and compaction records.
2. Single-writer lifecycle records: request execution, tool execution, session
   lifecycle, and goal-controller state.
3. Replaceable projections: conversation list/title/preview and mutable heads
   over append-only memory revisions.
4. Placement and authorization fields: immutable principal, participant, and
   session document references used by replication and ACP.

The highest-risk defects are:

- `AgentRequest` combines requester-authored intent and agent-authored mutable
  execution state, which prevents least-privilege document ACP.
- session, request, tool, and transcript joins commonly use logical strings;
  several correctness reads still use logical-ID `limit: 1`.
- `AgentMessage` and `CompactionEntry` are described as durable facts but have
  upsert paths that rewrite existing rows.
- transcript sequence allocation is a read-max-then-create protocol and the
  schema does not enforce unique `(session, sequence)` ordering.
- `AgentToolResult` is only a best-effort overflow spill, has no request or tool
  call reference, and is omitted from the run timeline despite its name.
- multiple approval documents are allowed and the first `created_at` value is
  accepted without a total tie-break or signer verification.
- fork copies create new facts with no source `_docID`/CID manifest and omit
  routing fields on copied rows, so copied history is neither verifiably
  derived nor guaranteed to reach the same participant.
- the standard replication templates omit `AgentToolApproval`, `Goal`, and
  `AgentMemory`; none of the ten base schemas has a DefraDB `@policy`.
- the participant-facing conversation/machine templates also carry several
  configuration collections without filters, including secret-bearing
  `InferenceBackend`; this is a current placement fact even though policy and
  encryption remediation are deferred from the provenance milestone.

## Current entity and provenance graph

Solid arrows are current logical-string joins. The dotted arrows are fields
that exist but do not provide a complete durable edge.

```text
AgentSession(session_id)
  ├── AgentConversation(session_id, latest_request_id) ....> AgentRequest
  ├── AgentRequest(session_id, request_id)
  │     ├── retry_parent_request / retry_root_request .....> AgentRequest
  │     ├── caused_by_parent_request_id ...................> AgentRequest
  │     └── caused_by_parent_tool_call_id .................> AgentToolCall
  ├── AgentMessage(session_id, request_id)
  ├── AgentToolCall(session_id, request_id, tool_call_id)
  │     └── child_request_id ..............................> AgentRequest
  ├── AgentToolResult(session_id, conversation_doc_id) ....> ?
  ├── CompactionEntry(session_id)
  └── Goal(session_id, last_*_request_id) .................> AgentRequest

AgentToolApproval(tool_call_id, request_id) ...............> AgentToolCall
AgentMemory(agent_did, key)  [cross-session; no session edge]
```

Only `conversation_doc_id` is named as a document ID, and it is not sufficient:
it is nullable text with no source collection/version contract. The remaining
edges use logical IDs. The implemented `AgentRequest -> RenderedRequest` edge
is the useful counterexample: it carries `_docID` plus a composite commit CID.

### Recommended graph

```text
Session (_docID, immutable principal/participant scope)
  ├── RequestIntent (immutable requester command)
  │      └── RequestExecution (agent-owned lifecycle; intent _docID + CID)
  ├── MessageFact (immutable; request-intent _docID when request-scoped)
  ├── ToolInvocation (immutable; request/message _docID + CID)
  │      ├── ToolExecution (agent-owned lifecycle)
  │      ├── ToolOutputFact (immutable, complete or chunked)
  │      └── ToolApprovalFact (immutable operator decision + expected call CID)
  ├── CompactionFact (immutable source-version manifest and boundary)
  ├── GoalIntent + GoalExecution (operator intent / agent controller state)
  └── ConversationProjection (replaceable head, source-versioned)

AgentMemoryHead(agent _docID, key)
  └── AgentMemoryRevision (immutable previous-head reference)
```

Every child carries the parent `_docID`. Any edge that claims what bytes or
state were consumed also carries the parent composite CID. Logical IDs remain
at API boundaries and in exports for correlation; they do not choose a row.

## Evidence: placement, ACP, and export today

- The `conversation` and `machine` scope templates push `AgentRequest`,
  `AgentMessage`, `AgentToolCall`, `AgentToolResult`, `AgentSession`,
  `AgentConversation`, and `CompactionEntry` using immutable `requester_did`
  filters (`crates/gents/src/agent/p2p_reconcile/templates.rs:114-262`). Those
  templates also include `AgentResponse`, pairing readiness, and six
  configuration collections without a filter rule; this audit must not describe
  the complete participant route as transcript-only.
- The `subagent-host` template returns only requests, responses, messages, and
  tool calls; it deliberately omits session/conversation, spills, and
  compactions (`templates.rs:329-359`).
- The legacy `runtime` and `chat-requests` profiles include the same eight
  Track A collections (`profiles.rs:82-133`). `AgentToolApproval`, `Goal`, and
  `AgentMemory` occur in none of these runtime profiles, and the unscoped
  `backup` template excludes them (`templates.rs:366-390`). They are present in
  the desktop branchable bulk-sync list
  (`crates/gents-schemas/src/lib.rs:176-193`), however, so paired desktops can
  currently backfill them without the target private-memory/approval placement
  policy.
- All ten SDL files use `@branchable`; none uses `@policy`. Consequently the
  claimed `agent_did`, `requester_did`, and `approver_did` values are routing
  claims, not database-enforced authorship.
- Projection ACP in `gents trace` performs a post-query decision over already
  fetched rows (`crates/gents-cli/src/commands/trace.rs:440-559`). It is not
  base-collection read/write ACP and does not cover tool-output spills,
  approvals, compactions, goals, or memory.
- `load_run_timeline_rows` fetches requests, messages, tool calls, responses,
  inference calls, session, and conversation
  (`crates/gents/src/run_timeline_fetch.rs:23-73`). It omits five Track A
  collections: tool-result spills, approvals, compactions, goals, and memory.
- No operational retention, legal-hold, coordinated purge, or full-fidelity
  enterprise archive implementation was found for these collections.
- In pinned DefraDB, `sync_branchable_collection` rejects a non-branchable
  collection (`../../sourcenetwork/defradb.rs/crates/p2p-adapter/src/libp2p_doc_pusher.rs:283-289`),
  and collection-scoped ACP is selected only for branchable collections
  (`../../sourcenetwork/defradb.rs/crates/acp/src/read_access.rs:91-109`).

### Recommended placement and access model

Use two explicit, independently authorized replication routes instead of one
broad conversation profile:

- **Execution/failover route:** filter every owned execution row on immutable
  `agent_did`; deliver to deployments authorized to execute that principal.
- **Participant route:** filter shareable session facts on immutable
  `requester_did`; deliver only the redaction class permitted to that
  participant. Never use this route for private memory.

`requester_did` must be non-null for participant-routed data. Locally scoped
data needs an explicit placement enum, not null/empty-string overloading.
`AgentMemory` gossips only among the principal's authorized execution hosts.
Approvals flow from authorized operators to the owning execution hosts. Goals
flow to the principal's failover set and only to participants granted goal
visibility.

At the ACP layer, install policies and relationships before enabling the schema
and fail closed if registration is absent. The minimum roles are principal
owner, delegated executor, session participant, approval operator, auditor,
archive exporter, and retention administrator. CID reads, `_version`, and
`_commits` must be included in negative and positive ACP tests. Issue #1064's
commit-signer verification is a prerequisite for calling any author field
verified.

## Uniqueness and ordering invariants

### Evidence

| Invariant | Current mechanism | Durability gap |
| --- | --- | --- |
| One session | globally unique `session_id`; upsert by it | concurrent peers can create twins; reads use `limit: 1` |
| One conversation per session | unique `session_id` | known legacy/replicated duplicates require ranking and sweeping |
| One request | `request_id` is non-unique; `retry_key` is unique only for retry successors | many request reads choose newest logical match |
| One transcript position | unique `message_key`, normally `session_id:sequence` | stable event keys may differ; no `(session, sequence)` unique index |
| Next transcript position | max message/tool reservation + 1, retry five times | a local retry protocol, not a replicated allocator |
| One tool call | unique `tool_call_key = session_id:tool_call_id` | components are mutable and logical; several reads use raw `tool_call_id` |
| One approval verdict | random unique `approval_id`; first `created_at` wins | multiple verdicts are legal and equal timestamps have no tie-break |
| One compaction step | unique `compaction_key = session_id:sequence` | sequence is read-last + 1 and existing fact is mutable by upsert |
| One goal | length-prefixed `(agent_did, session_id)` logical `goal_id`, not unique | twins are expected; only one canonical twin is advanced |
| One memory key | injective length-prefixed `(agent_did, key)` `memory_id` | components mutable; concurrent LWW winner is not a declared policy |

The transcript Lean model requires strictly increasing, unique message
sequences, unique tool-call IDs, unique result keys, and pair closure
(`proofs/Proofs/Transcript/State.lean:113-181`). The database schema does not
enforce the first three at their actual component scope.

### Recommendations

- Add real composite unique indexes where supported: session document plus
  sequence, session document plus tool-call ID, tool-call document plus approval
  round, and principal document plus memory key. Pinned DefraDB's uniqueness
  implementation is field-count generic, but null components bypass uniqueness
  and upstream lacks direct composite-unique coverage. Make every component
  non-null and add a DefraDB integration test for each critical index.
- Treat such indexes as local conflict detection, not distributed consensus.
  On convergence, enumerate every conflicting document and apply a declared
  canonical rule; never scan with `limit: 1`.
- Allocate transcript positions through one lease-fenced session owner and a
  compare-and-set append reservation. Retain the existing Lean-visible strictly
  increasing `sequence` as the canonical order; offline multi-writer append
  during a partition fails closed rather than introducing a competing tuple
  order. The lease epoch and reservation crash cut must be modeled explicitly.
- Remove opaque concatenated keys when a composite index can carry the
  components. Retain stable idempotency keys only for actual retry semantics.
  `retry_key` can become components `(requester, parent_request_doc_id,
  retry_ordinal)`; the SHA-256 value adds no integrity.
- Every conflict resolver must return the complete conflict set plus the chosen
  `_docID`, and archive both the conflict and repair decision.

## Canonical writer and query matrix

Tests and fixtures also write these collections directly. This table names the
production ownership seams and hot or correctness-sensitive reads.

| Collection | Canonical writers | Hot/correctness-sensitive reads | Ambiguity observed |
| --- | --- | --- | --- |
| `AgentSession` | session ensure/close (`session/sessions.rs:5-218`); fork (`session/fork.rs:801-850`); desktop transactional submit (`gents-desktop-core/.../chat/conversation.rs`) | completion binding (`session/query.rs:6-55`); desktop session snapshot; run timeline | `load_session_document_optional` filters `session_id`, `limit: 1` |
| `AgentConversation` | request projection/title/status (`session/conversation.rs:76-359`); recovery sweep (`lifecycle/recovery.rs:555-780`); desktop submit; fork | ranked canonical loader (`session/query.rs:133-207`); recent-title query; run timeline | duplicates are real; deterministic rank exists, but a later update can change the winner |
| `AgentRequest` | lifecycle materialize/claim/transition/recovery/queue; desktop submit/retry; trigger and subagent materializers | watcher pending scan and CID reload; lifecycle `_docID` reads; timeline/session/client queries | `request_id` is not unique; timeline and multiple APIs order/limit by logical ID |
| `AgentMessage` | owned-loop hook through `session/history.rs:52-488`; fork copy; desktop projection fixtures | provider history (`history.rs:7-48`); compaction; timeline/session projections | max+1 allocation races; save path rewrites a fact; request/content dedup uses first match |
| `AgentToolCall` | `ToolCallLifecycle` native/bridge/mode transitions (`tool_call_lifecycle/transition/*.rs`); background recovery; fork | lifecycle load/result; held-call polling; transcript/timeline/background projections | some reads correctly use `tool_call_key`, others use `(session_id, tool_call_id)` or raw `tool_call_id` with `limit: 1` |
| `AgentToolResult` | truncation spill (`truncation/spill.rs:11-72`); fork copy | desktop/session snapshot only | no call/request identity or uniqueness; spill failure is explicitly fail-open |
| `AgentToolApproval` | CLI/desktop approval client (`config_client/approval.rs:78-129`) | held-call watcher (`hook/persistence/approval.rs:40-103`) | first by `created_at`; no total tie-break, expected call version, or signer verification |
| `CompactionEntry` | compaction reducer (`session/compaction_entries.rs:61-171`); fork copy | prompt assembly, context-budget tools, session UI | read-last+1 then mutable upsert; no exact compacted prefix manifest |
| `Goal` | goal API/CLI and trigger controller (`goal.rs:546-888`, `trigger_engine/goal_source.rs`) | canonical goal load, active-goal trigger scan, usage aggregation | canonical earliest twin is chosen, but twins can diverge and only the selected doc is CAS-updated |
| `AgentMemory` | agent memory tool (`toolset/memory.rs:233-285`) | same tool by `memory_id`, `limit: 1` (`memory.rs:189-230`) | unique local key masks replicated twins; no conflict enumeration |

## Detailed collection decisions

Each entry first records evidence, then the recommended target contract. A
target field marked immutable is intended to be non-null unless explicitly
described as optional.

### `AgentSession`

**Evidence.** The schema is a mutable branchable envelope keyed by globally
unique `session_id`; only `agent_did` and `requester_did` are immutable. Ensure
uses an upsert by `session_id` and rewrites name, behavior, start, and status
(`session/sessions.rs:43-105`). Close reloads by logical ID and then updates one
`_docID` (`sessions.rs:190-218`). Fork creates a fresh row and does not copy
`requester_did` (`session/fork.rs:801-850`).

**Recommended contract.** Primary archetype: single-writer lifecycle envelope;
canonical, with a separate conversation projection. Meaning: one durable
collaboration scope. Authorized creator: requester creates session intent or
the owning agent creates it while processing an authorized request. Transition
writer: the elected agent execution owner; requester may request closure but
does not write lifecycle fields. Claimed principal: `agent_did`; required
signers: creator for genesis and elected owner for lifecycle commits.

Logical `session_id` is globally scoped protocol correlation, while `_docID` is
the parent for every Track A row. Concurrent creates are a surfaced conflict;
choose lowest `_docID` only as a temporary deterministic display canonical and
stop execution until ownership/routing fields agree. A genesis CID is not a
portable substitute for document identity. Immutable fields: `session_id`,
agent principal doc ID/DID, participant DID, behavior doc ID, placement class,
and created time. Mutable fields: lifecycle state and ended time only. Illegal:
completed with no `ended_at`, active with `ended_at`, principal/behavior change,
or children referencing a noncanonical/conflicted session.

Live gossip: agent failover route plus participant route. Backfill: branchable
sync by the same scopes. Branchable: **retain**. ACP: participant read/create
intent; principal executor update; auditor/exporter governed read. Encryption:
per-session data-encryption key, with envelope keys for participant and executor
groups.

Retention: hot while active plus a policy default of 90 days after closure;
metadata/audit archive seven years by default. Legal hold freezes purge and key
destruction. Sunset is a signed closed/tombstone fact followed by coordinated
peer/archive purge; cryptographic erasure destroys the session DEK where no
hold applies. Indexes: unique logical session ID for local conflict detection,
principal/status, participant, and ended time. Remove all logical-ID
`limit: 1` correctness reads.

Lean/conformance: add session lifecycle, ownership, conflict, and child-parent
closure; existing conversation recovery is not a session model. Breaking plan:
fresh schema epoch or successor collection, explicit import ledger for old
rows, and rejection/quarantine of duplicate bindings. Status: **provisional**.

### `AgentConversation`

**Evidence.** This is a mutable UI materialization, but title updates and fork
metadata give it non-regenerable input. Known duplicate `session_id` rows are
ranked by `(updated_at, richness, _docID)` and recovery converges their status
instead of deleting them (`session/query.rs:133-226`,
`lifecycle/recovery.rs:555-780`). Writes correctly target the chosen `_docID`,
but the chosen row can change when any twin gets a newer update.

**Recommended contract.** Primary archetype: projection/materialization;
derived. Meaning: list/detail head for one session. Authorized creator/updater:
projection worker acting for the agent; human title changes enter a separate
immutable `ConversationAnnotation` command/fact. Claimed principal and required
signer: projection owner agent, while annotation signer is the participant or
operator.

Identity: one projection per `session_doc_id` and projection version. It stores
the session `_docID` plus the exact source manifest/CIDs that produced
`latest_request`, preview, status, and generated title. Immutable: session
reference, agent/participant placement, projection contract version. Mutable:
projection body and source manifest as one atomic replacement. Illegal: latest
request outside the session, generated title without source capture, fork
metadata without an exact source session/version, or mixed-source fields.

Live gossip/backfill: participant and failover scopes; branchable **retain** so
late clients can obtain annotations and the current head. ACP: participants
read; projection worker update; annotation authors cannot rewrite projection.
Sensitive preview/title use the session key. Retention follows the session;
projection may be rebuilt or purged independently, but annotation facts follow
audit/legal hold.

Hot indexes: unique session doc reference, principal/updated time,
participant/updated time, and status. Remove the recovery rank as normal
identity; keep it only in legacy import. Lean/conformance: model projection
determinism, source-version monotonicity, annotation precedence, and rebuild
equivalence. Breaking plan: archive legacy twins, import annotations, rebuild
one projection from canonical facts. Status: **provisional**.

### `AgentRequest`

**Evidence.** One document mixes immutable routing fields, mutable user input,
claim ownership, backend selection, retries, interrupt command, lineage, and
terminal state. `request_id` is indexed but not unique. The Lean request model
defines nine states and guarded transitions
(`proofs/Proofs/Request/State.lean:5-169`,
`Request/Transition.lean:5-75`), and runtime transitions usually compare
`_docID` plus expected state. The first provenance slice pins the exact claim
commit before provider use. Nevertheless, timeline and status helpers still
select a logical request ID with order plus `limit: 1`
(`run_timeline_fetch.rs:75-111`, `lifecycle/lookup.rs:90-132`).

**Recommended contract.** Split into immutable `RequestIntent` and mutable
`RequestExecution`. Intent is a command created and signed by the requester (or
an agent/trigger acting under a recorded delegation); execution is a
single-writer lifecycle owned by the target principal's elected deployment.
The execution genesis pins intent `_docID` plus composite CID. `request_id`
remains globally scoped correlation; retry/root/parent/trigger/subagent edges
become document references, with CIDs wherever state was consumed.

Intent immutable fields: session doc, requester, target principal, behavior
reference, content, sampling options, metadata envelope version, origin,
valid-until, lineage, and created time. Execution immutable fields: intent
version, elected deployment/claim epoch, selected behavior/backend versions,
and claim time. Mutable execution fields: lifecycle, deadline, retry count,
failure/terminal evidence, and interrupt acknowledgement. An interrupt itself
is a separate immutable command so requester and executor do not share update
authority.

Illegal states include `status != lifecycle_state` projections, terminal with
no terminal evidence/time, processing without an active claim/backend,
completed without committed response, retry edges outside one root, asymmetric
subagent parent links, expired intent claimed after TTL, and lifecycle changes
by a non-owner. Live gossip/backfill: requester participant route and target
principal failover route; branchable **retain**. ACP: requester creates/reads
intent, target reads intent and creates/updates execution, participant reads
per grant, nobody mutates intent. Encrypt content/metadata with the session key.

Retention: request intent/execution hot with session, audit archive seven years;
payload erasure uses the session key while retaining non-sensitive signed
metadata when policy permits. Indexes: intent `request_id`, target/status/time,
session/created; execution unique intent doc and owner/state/deadline. Replace
hashed `retry_key` with component fields and a composite local unique index.

Lean/conformance: refactor the existing state machine over the execution row;
prove intent immutability, owner fencing, exact intent binding, retry graph
closure, interrupt command consumption, conflict fail-closed, and terminal
replica convergence. Breaking plan: split every legacy row at a named composite
CID; legacy rows lacking a trustworthy boundary are imported as unverified and
cannot be replayed. Execution-owner fencing depends on the deployment assignment
and lease epoch defined by Track D; a string `agent_did` alone is not a fence.
Status: **provisional; P0**.

### `AgentMessage`

**Evidence.** The provider loads messages by `session_id`, ordered only by
`sequence` (`session/history.rs:7-48`). New appends use create, but
`save_message_inner` upserts and rewrites content, reasoning, and timestamp for
an existing key (`history.rs:95-145`). Sequence is `max(message,
background-reservation)+1`, followed by bounded retries
(`history.rs:147-255`, `385-437`). Fork creates indistinguishable new message
facts with copied payloads and timestamps (`session/fork.rs:450-510`).

**Recommended contract.** Primary archetype: immutable durable transcript fact;
canonical. Creator: the actual message author—requester for user content,
principal executor for assistant/tool observations—under a role-specific ACP
relationship. Claimed principal: immutable `author_did`, distinct from session
owner and participant. Required signer: author/delegated executor consistent
with role.

Identity: `_docID`; logical `message_id` is globally unique correlation.
Immutable fields: session doc, author, role/kind, request intent/execution ref,
sequence tuple, content/reasoning envelope, timestamp, tool pairing refs,
source doc/CID for forked material. There are no mutable fields. Duplicate
idempotency keys must compare the complete canonical fact; mismatch is an
integrity error.

Illegal: duplicate session order key, unknown role/kind, tool result without a
completed linked execution, assistant call without invocation rows, message
after a closed session unless explicitly late/recovered, or copied content
without source lineage. Live gossip/backfill: session participant plus owner
failover; branchable **retain**. ACP: participant reads; author creates; no
update/delete outside retention workflow. Encrypt payload/reasoning per session.

Retention: configurable 30-day hot payload default, seven-year encrypted audit
archive for regulated deployments; metadata may outlive erased payload only if
the export contract marks it redacted. Legal hold retains DEK and blocks purge.
Indexes: composite unique session/order; request doc; author/time; tool
execution doc. Lean/conformance: strengthen Transcript refinement so database
rows enforce unique total order, author/role coherence, immutable idempotency,
and replicated conflict handling. Breaking plan: freeze old rows, import each
as a fact with source `_docID`/CID; never upsert. Status: **provisional; P0**.

### `AgentToolCall`

**Evidence.** `ToolCallLifecycle` models seven states and writes conditional
`_docID` transitions (`tool_call_lifecycle.rs:20-73`,
`tool_call_lifecycle/transition/native.rs:31-811`). The schema still combines
invocation arguments, lifecycle, output, policy decision detail, background
coordination, and child linkage. Only `agent_did`, `requester_did`, and
`spawn_target_did` are immutable. `tool_call_key` concatenates session and
provider call ID. Some reads use that key; lifecycle load filters session plus
call ID with `limit: 1` (`tool_call_lifecycle/query.rs:89-139`).

**Recommended contract.** Split immutable `ToolInvocation` from mutable
`ToolExecution`; represent full outputs as `ToolOutputFact`. Invocation creator
is the request execution owner and pins request, assistant message, selected
tool/service/config, arguments, policy-input versions, and call ID. Execution
writer is the elected local/remote tool owner; bridge ownership is explicit.
Required signers correspond to those owners.

Immutable invocation fields include session/request/message refs, agent and
requester route, tool identity, args, await/cancel policy, child target, and
workflow membership. Execution genesis pins invocation CID and immutable claim
epoch/deployment. Mutable fields are lifecycle, deadlines, partial-output head,
terminal classification, latency, remote-ack state, and output reference.
Denial evidence is an immutable policy-decision fact rather than fields that can
be rewritten with execution state.

Illegal: terminal without completion/failure evidence, completed without output
fact, approval-required running without accepted approval bound to the held
version, native execution with child link, bridge with missing symmetric parent
links, cancel policy changed after dispatch, or terminal-to-live transition.
Live gossip: owner/failover, participant-redacted, subagent coordinator by
target, and host return by requester. Backfill: all relevant routes;
branchable **retain**. ACP separates invocation creator, execution owner,
approver, participant reader, and auditor. Args/results use session or
delegation-scoped encryption.

Retention follows request/session, with long-lived policy and authorization
evidence. Indexes: composite unique session doc/call ID; request doc/state;
spawn target/state; workflow group/role; deadline/state. Lean/conformance:
retain ToolExecution/Background models, then prove split-row coherence,
owner fencing, exact approval/output binding, and recovery across replication.
Breaking plan: split at invocation genesis and terminal composite CIDs; mark
ambiguous legacy mutations unverified. Status: **provisional; P0**.

### `AgentToolResult`

**Evidence.** Despite its name, the only canonical writer is the truncation
spill path, which creates a row only when model-visible output is truncated
(`truncation/spill.rs:11-72`). Failure is logged and execution continues
without a spill (`spill.rs:87-108`). The row has no `request_id`,
`tool_call_id`, invocation/execution doc ID, idempotency key, or output sequence.
Fork copies it by session and timestamp and preserves an unexplained
`conversation_doc_id` string (`session/fork.rs:637-712`). It is not included in
the run timeline or trace ACP filter.

**Recommended contract.** Replace with immutable `ToolOutputFact`; canonical
for every tool output, not just truncation overflow. Creator and signer: the
writer that actually observed the output—normally the tool execution owner,
but the parent/bridge owner for a returned child result. Identity: `_docID`,
linked to invocation/execution `_docID` and the exact execution CID that
accepted it. For chunking, unique composite
`(execution_doc_id, output_stream, chunk_sequence)` plus terminal manifest.

Immutable fields: session/request/invocation/execution refs, agent/requester
scope, MIME/encoding, complete output or encrypted blob reference, truncation
projection metadata, chunk position, created time, discard/interruption status,
and source refs for imports/forks. No mutable fields. Illegal: orphan output,
multiple complete terminal manifests, truncated model observation without a
durable full-output fact when retention policy requires it, or an interrupted
output reported as model-consumed.

Live gossip/backfill: owner/failover always; participant only under result
redaction grant; branchable **retain**. ACP: execution owner create, session
grantee read at allowed redaction, no update. Use session/tool-output DEKs and
external object-store envelope encryption for large values.

Retention defaults shorter for raw output (30 days hot) with policy-controlled
archive; authorization/metadata manifests remain with request audit. Legal hold
pins blobs and keys. Indexes: execution doc/chunk, request doc, session/time,
participant route. Lean/conformance: extend transcript pairing to require exact
output fact and test fail-closed durability when full output is promised.
Breaking plan: import spills as `legacy_partial_archive`; do not claim they are
a complete output ledger. Status: **provisional; P0**.

### `AgentToolApproval`

**Evidence.** An operator creates a random `approval_id` with claimed agent,
approver, call ID, decision, reason, and client timestamp
(`config_client/approval.rs:78-129`). The runtime queries all rows for
`(agent_did, tool_call_id)`, orders only by `created_at`, and accepts the first
recognized decision (`hook/persistence/approval.rs:40-103`). The schema makes
only `agent_did` and `approver_did` immutable, and the collection is absent from
all standard P2P templates. The Lean tool model fences approve/deny transitions
but does not prove signer identity or deterministic choice among documents.

**Recommended contract.** Primary archetype: immutable authorization fact;
canonical. Creator/signer: an ACP-authorized human/device/service approver.
Meaning: one decision for one approval round and one exact held tool-execution
version. Identity: `_docID`; composite unique `(tool_execution_doc_id,
approval_round, approver_did)` with a separately declared quorum/first-wins
policy. Store held execution `_docID` plus CID and policy/version evaluated.

Every field is immutable: agent, requester/session scope, approver, decision,
reason, tool execution/version, approval round, policy reference, signed time,
and expiry. Illegal: decision for a non-held version, unauthorized signer,
decision after expiry/cancel, conflicting decisions without a deterministic
policy outcome, or approval reused after args/tool/policy change.

Live gossip: approver/operator to owning agent execution hosts; optional
participant receipt. Backfill: required for audit and failover; branchable
**retain**. ACP: approver create, owner read/consume, participant/auditor
governed read, nobody update/delete. Encrypt arguments/reason but retain a
verifiable non-sensitive decision envelope.

Retention: seven-year authorization audit default, legal hold capable; purge
only with the linked execution and policy audit set. Indexes: tool execution +
round + approver, agent/unconsumed/time, expiry. Lean/conformance: model
approval document selection, version binding, signer authorization, replay
rejection, and concurrent decisions. Breaking plan: old decisions import as
unverified because signer and held-version evidence are absent. Status:
**provisional; P0**.

### `CompactionEntry`

**Evidence.** A save loads every previous entry, derives `last.sequence + 1`,
accumulates paths, then upserts `session_id:sequence`; the update arm can rewrite
summary and counts (`session/compaction_entries.rs:88-171`). The schema records
a count but no exact compacted-through position or source message versions.
Fork copies compactions by timestamp rather than transcript boundary
(`session/fork.rs:714-799`). Lean proves reduction properties over an abstract
ordered transcript (`proofs/Proofs/Compaction/`), not that the persisted row
identifies the exact database prefix summarized.

**Recommended contract.** Primary archetype: immutable transcript-reduction
fact; canonical. Creator/signer: owning agent's compaction worker. Identity:
`_docID` with composite unique session doc/compaction ordinal. The fact pins
the prior compaction (if any), exact message/tool-output document versions,
compacted-through order key, summarizer rendered-request capture, and resulting
summary/output field CID.

All fields immutable: session and actor scope, ordinal, source manifest,
boundary, summary, file activity as typed/versioned data, token accounting,
created time, and algorithm/version. Illegal: overlapping or regressing
boundaries, missing source versions, summary captured before safe-to-reduce,
token counts inconsistent with manifest, mutable same-key rewrite, or fork copy
without original provenance.

Live gossip/backfill follows session participant and failover routes;
branchable **retain**. ACP: agent compactor create, participant read under
transcript grant, no update. Encrypt summary and paths with session key.
Retention follows source transcript and cannot outlive erasure of source
payload as “verified”; archive keeps manifest/CIDs and redaction state. Indexes:
unique session/ordinal, session/boundary, created time.

Lean/conformance: connect the Compaction model to persisted `_docID`/CID
manifests, database order allocation, idempotent create equality, and fork
prefix composition. Breaking plan: legacy entries import as captured summaries
without reconstructible provenance. Status: **provisional; P0**.

### `Goal`

**Evidence.** Goal combines operator objective/budget and runtime controller
state. `goal_id` is deliberately non-unique; code sorts twins by earliest
`(created_at, goal_id, _docID)` and advances one with `_docID` compare-and-set
(`goal.rs:463-530`, `546-784`). A comment says twins arise through replication,
but standard replication templates omit the collection. Most timestamps are
nullable strings. Lean proves lifecycle, blocked-audit threshold, and
continuation decisions (`proofs/Proofs/Goals.lean`).

**Recommended contract.** Split immutable/versioned `GoalIntent` (objective,
budget, operator actions) from agent-owned `GoalExecution` lifecycle. Creator:
session participant/operator with goal authority, or the agent under explicit
self-goal authority. Execution transition writer/signer: elected agent owner.
Identity: one active goal execution per session document; intent versions are
append-only and execution records which version they adopted. Request
continuations link by request `_docID`, not ID strings.

Immutable execution fields: session/principal/intent version and controller
epoch. Mutable fields: formally modelled status, usage checkpoint, active time,
blocked-audit state, continuation CAS counter, wrapup and failure evidence.
Illegal states are those rejected by `Goals.step?`, plus negative counters,
non-positive budgets, `complete` without completion evidence,
`budget_limited` without wrapup requested, completed wrapup without request
evidence, or two active controller epochs.

Live gossip/backfill: principal failover set; participant read/write-intent only
when granted; branchable **retain**. ACP separates intent author from execution
updater. Encrypt objective/failure/evidence per session. Retention follows
session plus controller audit; legal hold retains intent/action/execution chain.
Clearing a goal appends a cancel/sunset intent rather than deleting twins.
Indexes: unique session/current execution, principal/status, intent version,
continuation parent request.

Lean/conformance: preserve Goals model and add split authority, epoch fencing,
exact request evidence, twin conflict convergence, and replicated failover.
Breaking plan: select legacy canonical only for import, record every twin and
selection; do not silently delete. Status: **provisional; P1**.

### `AgentMemory`

**Evidence.** The agent tool builds an injective length-prefixed logical
`memory_id` from `(agent_did, key)`, reads it with `limit: 1`, and upserts value
and update time (`toolset/memory.rs:185-285`). `agent_did` and `key` are mutable
in the schema. The collection is branchable but has no standard replication
route or ACP. There is no consumption/version reference when memory affects a
decision, beyond any resulting transcript/provider capture.

**Recommended contract.** Model a mutable `MemoryHead` projection over
immutable `MemoryRevision` facts. Meaning: principal-owned cross-session
knowledge, never participant transcript by default. Creator/updater/signer:
the principal's authorized execution owner or explicit operator. Identity:
composite `(agent_principal_doc_id, normalized_key)`; no concatenated
`memory_id` required. Each revision pins previous head/version and author.

Immutable head scope: principal document, key, placement/encryption class.
Mutable head: current revision reference and updated time. Revision fields are
all immutable: value envelope, previous ref, author, source request/message
refs, created time, TTL/classification. Illegal: principal/key move, revision
cycle, head regression, conflicting concurrent revisions hidden by `limit: 1`,
or sharing to requester without an explicit release fact.

Live gossip/backfill: only the principal's authorized failover/execution hosts;
branchable **retain**. ACP: principal/executor read and append; operator by
delegation; ordinary session participants denied. Use a per-principal or
per-memory-class DEK with rotation and revision-aware erasure.

Retention: classification-specific TTL; default 30 days hot for unclassified
memory, with no enterprise archive unless explicitly promoted. Legal hold is
explicit and exceptional. Sunset appends a deletion revision, removes head
visibility, then coordinates peer/archive purge and key destruction. Indexes:
unique principal/key, head update time, revision previous/source/TTL.

Lean/conformance: add revision DAG/head selection, owner fencing, concurrent
write policy, placement noninterference, and deletion/legal-hold behavior.
Breaking plan: import current value as a genesis revision with legacy source
CID; enumerate duplicate logical keys before choosing a head. Status:
**provisional; P1**.

## Fork durability decision

Fork is not a bulk-copy implementation detail; it is a provenance operation.
The current implementation computes a cut from message sequence/time and
creates child messages, calls, spills, and compactions with new keys
(`session/fork.rs:380-799`). The new documents cannot prove which original
versions they copied, and timestamp cuts for spills/compactions are not the same
boundary as the transcript order. Copied rows also omit `requester_did`.

The target fork is an immutable `SessionFork` fact containing source session
`_docID`, source composite CID, exact ordered prefix manifest, child session
`_docID`, and fork policy/version. Prefer a child history view that composes the
immutable source prefix with child-local facts. If physical copying is needed
for placement or retention, every copy must carry original `_docID` and CID and
must retain the child's immutable routing fields. Lean must prove prefix
closure, pair closure, source-version stability, and that copying/reordering
cannot manufacture a different history.

## Retention and enterprise archive contract

The target archive unit is a session bundle plus separately governed principal
memory. A bundle is incomplete unless it carries:

- schema epoch and collection schema version;
- every `_docID`, composite CID, relevant field CIDs, signature, and verified
  signer status;
- logical IDs and exact document-version lineage edges;
- request/tool/goal lifecycle transition evidence;
- fork and compaction source manifests;
- the ACP policy/resource/relationship version and redaction decision applied;
- payload encryption class, key identifier (not key material), hold state, and
  purge eligibility;
- conflict sets and canonicalization/repair decisions; and
- export contract version, exporter identity, time, and destination receipt.

Archive is not the current run timeline. The timeline omits several facts and
performs logical-ID joins. Implement archive projection from exact document
references, fail closed on ambiguous/missing sources, and test restore into an
empty authorized host. A tombstone or projection deletion alone is not physical
erasure; completion requires acknowledgements from operational peers, archive,
backup, and key custody.

## Breaking schema and proof sequence

These recommendations intentionally do not require an in-place compatibility
migration. The implementation sequence is:

1. Declare a new schema epoch and exact successor collection contracts.
2. Update Lean models first for changed lifecycle, authority, transcript order,
   provenance, fork, compaction, approval, and goal semantics.
3. Generate conformance cases for legal/illegal transitions, conflict sets,
   exact-version reconstruction, ACP roles, and replication recovery.
4. Implement new writers that create immutable facts and update only
   `_docID`-addressed lifecycle/projection heads.
5. Switch all hot reads to document references and complete conflict reads.
6. Add filtered live replication, branchable late-peer sync, signer checks,
   ACP installation/relationships, encryption, archive, and purge workflows.
7. For disposable pre-release stores, reset at the epoch boundary. For retained
   stores, export the legacy DAG first and import successor facts with explicit
   `legacy_source_doc_id`/CID and verification status. New CIDs must never be
   represented as preservation of old CIDs.

Required integration tests span at least two writers and two nodes: concurrent
session/request/message/tool/goal/memory creation, filtered gossip, reconnect
and late-peer backfill, unauthorized normal/history/CID reads, owner failover,
fork reconstruction, compaction reconstruction, archive/restore, hold, and
coordinated purge.

## Prioritized child-issue candidates

1. **P0 — Split `AgentRequest` intent from execution and replace logical joins
   with exact document/version references.** Includes lifecycle owner fencing,
   interrupt facts, retry components, and removal of request-ID `limit: 1`.
2. **P0 — Make transcript persistence append-only with database-enforced order
   and pairing.** Covers message upsert removal, composite uniqueness,
   multi-host allocation, tool pairing, and exact request/message links.
3. **P0 — Split tool invocation/execution/output and make full output durable.**
   Replace best-effort orphan spills; bind terminal execution to an output
   version and include it in timeline/archive.
4. **P0 — Make approvals signed, exact-version-bound, replicated authorization
   facts.** Define concurrent/quorum behavior and add base DefraDB ACP, signer,
   replay, and expiry tests.
5. **P0 — Persist compaction source manifests and immutable boundaries.** Bridge
   the existing Lean reducer to database `_docID`/CID evidence and fork-safe
   reconstruction.
6. **P1 — Establish `AgentSession` `_docID` as the conversation-plane spine.**
   Add lifecycle/ownership model, route fields, conflict quarantine, and remove
   session-ID `limit: 1` reads.
7. **P1 — Replace fork copying with an exact-version fork manifest.** Preserve
   routing, prefix/pair closure, and auditable derivation.
8. **P1 — Split goal intent/controller state and implement replicated owner
   failover.** Eliminate divergent canonical twins and deletion-as-clear.
9. **P1 — Introduce versioned principal memory with private placement.** Make
   principal/key immutable, define concurrency, ACP, TTL, and erasure.
10. **P1 — Rebuild `AgentConversation` as a source-versioned projection.** Move
    human annotations to facts and retire duplicate ranking from normal reads.
11. **P1 — Implement the two-route replication and base-collection ACP matrix.**
    Cover all Track A collections, relationship bootstrap, history/CID reads,
    and late-peer sync. Coordinate signer verification with #1064.
12. **P2 — Ship governed session archive/restore, retention, legal hold, and
    purge receipts.** Require the full provenance envelope above.

The first five issues form one durability foundation and should be designed in
parallel but merged in dependency order: authority split, transcript order,
tool facts, approvals, then compaction. Session identity and fork provenance
must land before declaring Track A complete.
