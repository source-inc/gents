# Schema Decision Ledger

This ledger is the working inventory for
[issue #1063](https://github.com/source-inc/gents/issues/1063). It applies the
[DefraDB schema guide](defradb-schema-guide.md) to every collection shipped by
Gents.

The inventory records current state and review hypotheses. It does not endorse
the current schema. A row becomes **decided** only after its detailed entry
answers every question in the template below and links the resulting tests and
migration work.

The evidence and proposed breaking contracts are split into four audit tracks:

- [Track A: conversation and session durability](schema-audit-track-a.md)
  ([#1068](https://github.com/source-inc/gents/issues/1068))
- [Track B: responses and inference attempts](schema-audit-track-b.md)
  ([#1070](https://github.com/source-inc/gents/issues/1070))
- [Track C: provenance and projections](schema-audit-track-c.md)
  ([#1069](https://github.com/source-inc/gents/issues/1069))
- [Track D: configuration, automation, and placement](schema-audit-track-d.md)
  ([#1067](https://github.com/source-inc/gents/issues/1067))
- [Shared retention and erasure lattice](schema-retention-lattice.md)
- [PR-sized durability roadmap](schema-durability-roadmap.md)

`Provisional` in the inventory means the track has recorded a target direction;
it does not mean that the schema, proofs, ACP, replication, or retention work is
implemented.

## Status vocabulary

- **Unreviewed:** only current schema facts and a preliminary archetype are
  recorded.
- **In review:** writers, readers, invariants, and DefraDB capabilities are
  being traced.
- **Provisional:** a direction is recorded, but a dependency, proof, or
  migration decision remains.
- **Decided:** the target contract, tests, and migration path are accepted.
- **Implemented:** the decided contract is enforced in the shipped schema and
  runtime.

## Detailed entry template

Every collection review must record:

```text
Collection:
Primary archetype:
Document meaning:
Canonical or derived:

Authorized creator:
Authorized transition writers:
Claimed principal field:
Required commit signer:

Logical ID and uniqueness scope:
Durable _docID relationships:
Pinned composite/field CIDs:
Concurrent-create/conflict behavior:

Immutable fields:
Mutable lifecycle/configuration fields:
Illegal state combinations:

Live gossip scope/filter:
Late-peer backfill:
Branchable decision:
ACP policy/resource/relationships:
Encryption and key-custody class:

Hot retention:
Archive/export contract:
Sunset/purge behavior:
Legal-hold behavior:

Canonical writers and queries:
Indexes justified by those queries:
Ambiguous limit: 1 or logical-ID reads:

Lean/conformance impact:
Migration/successor/backfill plan:
Open dependencies:
Decision status:
```

## Current collection inventory

`Branchable` reflects the schema root merged by #1059. Archetypes are starting
hypotheses to test, not conclusions.

### Conversation, execution, and projections — Tracks A-C

| Collection | Archetype hypothesis | Branchable | Track | Status |
| --- | --- | ---: | --- | --- |
| `AgentSession` | Lifecycle envelope | Yes | A | Provisional |
| `AgentConversation` | Materialized UX projection | Yes | A | Provisional |
| `AgentRequest` | Command plus lifecycle envelope | Yes | A/B | Provisional |
| `AgentResponse` | Streaming materialization plus terminal result | Yes | B | Provisional |
| `InferenceCall` | Durable provider-attempt fact/ledger | No | B | Provisional |
| `AgentMessage` | Durable transcript fact | Yes | A | Provisional |
| `AgentToolCall` | Tool lifecycle envelope | Yes | A | Provisional |
| `AgentToolResult` | Durable tool-result fact | Yes | A | Provisional |
| `AgentToolApproval` | Durable authorization decision | Yes | A | Provisional |
| `CompactionEntry` | Durable transcript-reduction fact | Yes | A | Provisional |
| `Goal` | Long-lived lifecycle envelope | Yes | A | Provisional |
| `AgentMemory` | Mutable principal knowledge | Yes | A | Provisional |
| `RenderedRequest` | Immutable provider-call fact | Yes | C | Provisional |
| `ProjectionAcpBinding` | Desired projection authorization state | No | C/D | Provisional |

### Agent and inference configuration — Track D

| Collection | Archetype hypothesis | Branchable | Track | Status |
| --- | --- | ---: | --- | --- |
| `AgentPrincipal` | Desired identity configuration | No | D | Provisional |
| `AgentBehavior` | Desired behavior configuration | No | D | Provisional |
| `ToolSelection` | Desired capability configuration | No | D | Provisional |
| `Skill` | Desired capability/instruction configuration | No | D | Provisional |
| `InferenceBackend` | Desired backend plus observed health state | No | D | Provisional |
| `InferenceProfile` | Desired inference configuration | No | D | Provisional |
| `OAuthCredential` | Local secret/configuration | No | D | Provisional |
| `WorkspaceRoot` | Local host configuration | No | D | Provisional |
| `AgentRuntime` | Observed deployment state | Yes | D | Provisional |
| `ToolServiceRegistry` | Desired service identity plus observed state | No | D | Provisional |
| `ToolServiceHealthState` | Observed service state | No | D | Provisional |

### Automation

| Collection | Archetype hypothesis | Branchable | Track | Status |
| --- | --- | ---: | --- | --- |
| `Task` | Desired work configuration | Yes | D | Provisional |
| `Schedule` | Desired schedule plus observed firing state | Yes | D | Provisional |
| `EventTrigger` | Desired trigger plus observed firing state | Yes | D | Provisional |
| `PersonaConfigRequest` | Command/intent plus outcome | Yes | D | Provisional |

### Network, pairing, and placement

| Collection | Archetype hypothesis | Branchable | Track | Status |
| --- | --- | ---: | --- | --- |
| `AgentDirectoryEntry` | Replicated directory projection | Yes | D | Provisional |
| `AgentNetwork` | Durable network configuration | Yes | D | Provisional |
| `NetworkMembership` | Durable authorization/membership fact | Yes | D | Provisional |
| `NetworkJoinRequest` | Command/intent plus outcome | Yes | D | Provisional |
| `PeerEndpoint` | Replicated endpoint configuration | Yes | D | Provisional |
| `PeerRegistry` | Local/desired peer registry | No | D | Provisional |
| `PeerPairingDesired` | Desired local pairing state | No | D | Provisional |
| `DataPlanePairingDesired` | Desired local pairing state | No | D | Provisional |
| `PeerPairingApplied` | Observed local reconciliation state | No | D | Provisional |
| `ConsumedInviteNonce` | Durable replay-prevention fact | No | D | Provisional |
| `ReciprocalConversationIntent` | Desired cross-peer intent | No | D | Provisional |
| `PairingBearerClaim` | Replicated command/claim | Yes | D | Provisional |
| `BearerPairingReady` | Replicated acknowledgement fact | Yes | D | Provisional |

## First vertical slice: request provenance

### Scope

Build on [PR #1059](https://github.com/source-inc/gents/pull/1059) by
establishing a reusable exact-version reference and applying it to the
`AgentRequest -> RenderedRequest` edge.

```text
DocumentVersionRef {
    doc_id
    composite_commit_cid
}
```

### Current facts

- `RenderedRequest.request_doc_id` identifies the exact `AgentRequest`
  document, while `request_id` remains a logical correlation value.
- `AgentRequest` is lifecycle-mutated after creation. Its `_docID` therefore
  does not identify which version supplied the captured request.
- `RenderedRequest.request_json` is immutable and its field commit CID anchors
  the stored provider payload.
- `@branchable` is not required for the `AgentRequest` document-version CID or
  the `RenderedRequest.request_json` field CID. It remains independently
  relevant to backfill and collection-scoped ACP.
- The signer of either document is not yet guaranteed to match its claimed
  principal; that is tracked by issue #1064.

### Implemented direction

1. `DocumentVersionRef` pairs the stable `_docID` with a composite commit CID.
2. The formal capture fact compares that source version as well as the rendered
   body; equal provider bytes cannot make a different source version
   idempotent.
3. The conditional claim write is the named provider-input boundary. After the
   write, the runtime excludes every composite observed before the mutation,
   then uses CID time-travel reads to locate the lowest-height new snapshot
   whose complete claim marker tuple matches `status = processing`,
   `lifecycle_state = claimed`, timestamp, deadline, behavior, backend, and
   execution origin. More than one matching CID at that height fails closed.
   Selecting that unique boundary prevents a later content edit that inherits
   the claim markers from moving the boundary forward.
4. The runtime replaces the watcher-loaded value with that reconstructed
   snapshot before prompt assembly. This closes the watcher-read/claim-write
   race even while request input fields remain mutable in the current schema.
5. A document-backed capture fails closed without the reference. The
   `RenderedRequest` row stores `request_doc_id` and `request_commit_cid`, and
   provenance manifest v3 carries the structured pair.
6. The status remains `CapturedOnly`: config, transcript, ACP-read, and signer
   evidence are not all pinned yet.

### Findings from the pinned DefraDB implementation

- `_version` on a normal query or mutation returns all reachable composite
  versions, sorted by height. Treating element zero as “the consumed version”
  would be unsound under concurrent heads.
- DefraDB update events carry the exact written CID, but the event bus is
  live-only and is therefore not a sufficient recovery source.
- `_commits` exposes composite `_C` CIDs and `Collection(cid: [CID])` performs
  an exact historical read. Excluding the pre-mutation history and matching the
  unique lowest-height new claim-state snapshot selects the runtime's claim
  commit without relying on head order or assuming unchanged marker fields are
  unique forever.
- The exact `status = processing`, `lifecycle_state = claimed` snapshot written
  by the `pending -> claimed` mutation is the request source boundary.
- ACP behavior for CID/history reads, signer evidence, and complete
  reconstructibility remain follow-up gates before `Verified` is legal.

### Initial acceptance criteria

- Every document-backed rendered request carries an `AgentRequest` `_docID`
  and composite commit CID.
- No capture-time re-query by `request_id` or current configuration state is
  used to manufacture provenance.
- A later request lifecycle transition does not change reconstruction output.
- A CID belonging to another document is rejected.
- Missing or unauthorized history produces an explicit non-verified result.
- Signer verification remains explicit and cannot be inferred from
  `agent_did` or `requester_did` fields.

Status: **Implemented first slice**. Exact request-version provenance is
captured; the manifest intentionally remains `CapturedOnly` until the remaining
config, transcript, ACP, and signer evidence is modeled and implemented.
