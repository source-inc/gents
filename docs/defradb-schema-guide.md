# DefraDB Schema Design for Gents

Gents uses DefraDB as its control plane, durable fact store, replication
substrate, and access-control boundary. A collection definition is therefore
part of the runtime's network and security protocol. It is not just a table
shape.

This guide defines the questions that must be answered before a Gents
collection is added or changed. The accompanying
[schema decision ledger](schema-decision-ledger.md) records those answers for
the collections that ship with Gents. The
[retention and erasure lattice](schema-retention-lattice.md) is the shared
vocabulary for archive, legal hold, downgrade, and purge decisions; individual
collection reviews must not invent incompatible duration guarantees.

## Design objective

The schema must support a fleet of agents and users operating across multiple
hosts and trust boundaries while preserving:

- verifiable document authorship and lineage;
- least-privilege reads and writes enforced by DefraDB ACP;
- live gossip and deterministic recovery after concurrent writes;
- late-joining hosts that can backfill the data they are allowed to hold;
- exact reconstruction of the configuration and transcript versions that
  produced a provider request;
- governed export to enterprise data stores without losing provenance; and
- explicit retention, archival, legal-hold, and sunset behavior.

A schema is incomplete until these operational properties are decided. Field
names and GraphQL types alone are not the contract.

## DefraDB identity vocabulary

Four identities must remain distinct.

### Logical ID

A logical ID is a Gents or external-protocol correlation value such as
`session_id`, `request_id`, or `tool_call_id`. It can remain useful in CLI and
projection APIs even when it is not the durable database identity.

Every logical ID must declare its uniqueness scope: global, principal,
deployment, session, request, turn, or another named boundary. UUID generation
is collision resistance, not a declared database invariant.

### Document ID (`_docID`)

`_docID` is DefraDB's stable identity for one document. For an ordinary create,
DefraDB derives it from the genesis composite commit CID. The serialized
`_docID` is not the genesis CID, and the CID cannot be recovered from the
serialized value.

Use `_docID` to:

- target exactly one document for mutation;
- persist a durable relationship to another document; and
- disambiguate documents that share or conflict on a logical ID.

Do not use a logical-ID filter plus `limit: 1` where correctness requires one
specific document.

### Commit CID

A CID identifies content committed to DefraDB's Merkle DAG. DefraDB writes
field commits and a composite document commit for every document mutation,
whether or not the collection is branchable.

- A field CID anchors the value committed for one field.
- A composite commit (`fieldName == "_C"` in `_commits`) anchors a document
  version and links the relevant field heads.
- A collection commit exists only for branchable collections and anchors a
  collection-level state.

Use `_docID` plus a composite commit CID when provenance must identify the exact
document version consumed. DefraDB exposes commit metadata through `_version`
and `_commits`, and accepts `docID`/`cid` for time-travel reads.

CID time-travel is deliberately a flat document read in the pinned version:
nested relationships are not resolved historically, one-to-many fields are not
returned, and a CID for another document can yield no row rather than a typed
wrong-document error. Callers must provide and verify `_docID`, request direct
fields, and reject an empty or mismatched result.

The runtime must acquire the value and its version reference at one named
boundary and carry that reference forward. For a plain read, acquire both as
one consistent observation. When a conditional mutation defines the boundary,
record the pre-mutation commit set, then locate the lowest-height new composite
whose complete marker tuple matches that mutation and reload the value by CID
before using it. If more than one candidate exists at that height, fail closed.
The pre-mutation exclusion matters: later mutations inherit unchanged claim
fields and can otherwise look like the boundary. Re-querying the current head
later is a time-correlation race, not provenance.

On the pinned DefraDB version, `_version` without a target CID returns every
reachable composite version and sorts by height. Concurrent heads make
“element zero is current” an invalid correctness rule. Use an exact mutation
marker or update-event CID, verify it with a CID time-travel read, and retain
the resulting `_docID`/CID pair.

### Signer identity

A DID stored in `agent_did` or `requester_did` is a claim made by the document.
It is not proof of authorship. The ACP caller identity and the identity used by
DefraDB to sign commit blocks are also distinct inputs.

Commit signing requires a registered node signing identity. Production init,
serve, and offline configuration access must all load one explicitly; offline
configuration fails closed when the initialized identity cannot be recovered.
Unsigned blocks can still exist in legacy stores or be accepted during merge,
so correctness loaders verify the selected commit and reject unsigned evidence.
A node-level signer also does not prove that a remote requester authored a
mutation submitted through that node; preserve and verify a requester-signed
envelope when storage signer and request author differ.

A verified provenance claim requires all of the following:

1. the relevant commit has a valid signature;
2. its signer is the identity authorized to author that fact or transition;
3. the document's claimed principal agrees with that signer under the declared
   ownership model; and
4. projections report unsigned, mismatched, or unavailable evidence honestly.

[Issue #1064](https://github.com/source-inc/gents/issues/1064) tracks the
current signer-versus-claimed-principal gap.

## Document archetypes

Every collection must have one primary archetype. A document that mixes these
roles should usually be split into a canonical fact and one or more projections
or mutable envelopes.

| Archetype | Meaning | Expected write pattern |
| --- | --- | --- |
| Durable fact | Something that happened or was observed | Immutable or append-only |
| Command or intent | A request for an authorized actor to perform work | Immutable intent plus a fenced outcome |
| Desired configuration | The state operators or authorized agents want | Mutable, versioned desired state |
| Observed state | What one deployment currently sees or is doing | Mutable and usually host-scoped |
| Projection/materialization | A replaceable view derived from facts | Regenerable and source-versioned |
| Local secret/configuration | Host-bound state that must not gossip | Local-only and tightly restricted |

State such as `status` is not automatically evidence that the entire document
is a lifecycle envelope. For every mutable field, name the transition writer
and the invariant that permits the transition.

## Branchability

`@branchable` is an irreversible collection capability, not a synonym for
history, replication, or immutability.

Choose it when at least one of these capabilities is required:

- a peer must initiate historical catch-up through the branchable collection
  sync path; or
- the collection needs collection-scoped ACP decisions over collection commits.

Do not choose it merely because document history matters: document field and
composite commits already exist. A newly installed push replicator can push
existing documents from a non-branchable collection, so ordinary backfill is
not by itself proof that branchability is needed. Do not assume branchability
causes a collection to replicate: runtime subscription and replication profiles
still decide what is gossiped.

Because the pinned DefraDB rejects changes to `is_branchable`, an existing
non-branchable collection that needs these capabilities requires an explicit
successor/backfill design unless DefraDB gains a supported migration path.

### Gents default posture

Use branchable roots by default for canonical facts and shared desired
configuration that must move across authorized machines, support peer-initiated
catch-up, or later carry collection-scoped policy. Keep deployment-local health,
leases/cursors, replaceable caches, and secrets nonbranchable unless a concrete
cross-machine contract says otherwise.

This default does not make branchability part of provenance. Field and composite
CIDs, commit signatures, and exact document-version manifests work for both
branchable and nonbranchable collections. Branchability supplies an additional
collection commit and multi-machine lifecycle capabilities; it does not prove
which version a runtime consumed.

Before accepting a branchable root, measure and test:

- collection-commit write/storage amplification on hot append and lifecycle
  paths;
- concurrent collection-head behavior and repair after partition/rejoin;
- push replication and peer-initiated catch-up under the intended filters;
- late-peer authorization, retention, tombstone, and reset/successor behavior;
  and
- archive/export behavior when the operational collection has multiple heads.

Lens can add compatible fields, project old versions, select an active version,
and materialize data, but it cannot change branchability or collection policy.
Changing either requires a new root (or a deliberate destructive pre-release
reset), an explicit cutover boundary, and version references that continue to
resolve for every retained run.

## Replication and placement

For each collection, decide three separate data-placement paths:

1. **Live gossip:** which peers receive new mutations, under what filter?
2. **Late-peer backfill:** how does a newly authorized host receive prior data?
3. **Archive/export:** how does data leave the operational mesh for governed
   enterprise storage?

The answer must identify the unit of placement. Common scopes include a
principal, session participants, deployment, workspace, or network membership.
"All paired peers" is a policy decision and must not be inherited accidentally
from a broad schema catalog.

Replication filters and their source fields are security-sensitive schema.
Filter fields must be immutable so a document cannot move between placement
scopes after creation.

In the pinned implementation, filters are installed on push replicators rather
than collection subscriptions, and ACP-derived replication filters are not yet
implemented. DefraDB requires filter fields to be immutable scalar LWW fields;
Gents must still test every profile and push direction.

## ACP and encryption

Replication and authorization answer different questions. Replication decides
which node holds blocks. ACP decides whether an identity may perform an
operation through DefraDB.

For every collection with a policy, define:

- policy and resource identifiers;
- document registration and relationship creation order;
- creator, reader, updater, and deletion relationships;
- collection-level fallback behavior, if used;
- negative behavior for anonymous and unrelated identities; and
- authorization of normal reads, CID reads, `_version`, and `_commits`.

Never ship a `@policy` directive without proving that the policy and document
relationships are installed. Under the currently pinned DefraDB behavior, an
unregistered object can fall through as public rather than fail closed.

A collection policy is immutable in the pinned schema validator, including a
change from no policy to a policy. Collections designed under the assumption of
future policies need policy-bearing successor roots later; a runtime policy API
alone cannot retrofit existing roots. Identity-less genesis writes to a
protected collection can also create permanently public documents, so successor
bootstrap must establish identity and relationships before accepting writes.

Normal selects, exact-CID reads, `_commits`, and `_version` do not all share one
enforcement path. `_commits` applies per-commit checks and CID reads apply
document checks, while mutation-result `_version` enrichment in the pinned
implementation can disclose history without an equivalent read check. Treat
uniform history confidentiality as unproven, test each surface separately, and
repair that upstream leak before claiming history ACP.

Policy-backed mutations in the pinned implementation also do not preserve the
implicit all-fields transaction boundary of an unprotected multi-mutation.
Design lifecycle writes as individually recoverable cuts, or add and verify an
explicit transaction path; never assume that adding a future policy leaves a
multi-document finalize operation atomic.

ACP is not at-rest encryption. Replicated-delta encryption, local datastore
encryption, key custody, key rotation, and archival encryption must be designed
as separate layers.

## Identity and relationships

Persist references according to their purpose:

| Purpose | Required representation |
| --- | --- |
| User/protocol correlation | Logical ID |
| Target one durable document | `_docID` |
| Reconstruct one consumed version | `_docID` plus composite commit CID |
| Prove one field value | Field commit CID |
| Prove authorship | Commit CID plus verified signature/signer |
| Regenerate a projection | Versioned manifest of all source document references |

Keep a derived hash only when the hash itself has an operational role, such as
a fixed-width idempotency key or query fingerprint. A digest stored by the same
writer as the bytes is not independent integrity evidence. A hashed composite
key also does not turn a DefraDB unique index into global distributed
consensus.

Prefer component fields and a real composite index when the database and
migration path support the intended query. If an opaque key is required, its
canonical encoding and collision assumptions are part of the contract.

Pinned DefraDB's uniqueness implementation handles multiple index fields, but a
null component bypasses uniqueness. Every component of a correctness-critical
composite unique key must be non-null, and multi-field uniqueness needs an
integration test rather than relying only on SDL parser coverage.

## Uniqueness and concurrent authorship

Every uniqueness claim must state:

- the component fields and scope;
- which identity is allowed to create the document;
- whether multiple hosts may race to create it;
- how a replicated unique-index conflict is surfaced and repaired; and
- which deterministic document is canonical while a conflict exists.

DefraDB unique indexes protect local indexed writes. Concurrent peers can still
author conflicting documents before gossip converges. The merge path may retain
a deterministic indexed winner while the conflicting document remains visible
to a non-index scan. Code must not use `limit: 1` as a substitute for a conflict
policy.

The merge conflict outcome is logged rather than persisted as a queryable
record or event. Gents conflict handling must enumerate the complete set with a
non-index/application scan and persist its own repair decision.

## Types, nullability, and indexes

- Use `DateTime` for timestamps unless opaque external text is the real value.
- Use null for absence. Do not overload empty strings, zero, false, or `{}` with
  "not captured" or "inherit" semantics.
- Prefer typed fields for values that participate in queries or invariants.
- Use a versioned opaque envelope when forward compatibility matters more than
  database queryability.
- Add indexes from observed hot queries and declared uniqueness constraints.
  Descriptive fields are not automatically indexes.
- DefraDB mutations must emit `null`, not `[]`, for an empty nillable array.

Pinned DefraDB supports compatible in-place evolution through nillable field
patches, Lens read-time migration, active-version selection, and collection
materialization; `gents-migration` already uses these mechanisms. Use a
successor collection or deliberate destructive schema-epoch reset when identity,
archetype, branchability, or policy changes. Do not claim that every schema
change requires export/re-import, and do not preserve an unsafe mixed archetype
merely because a compatible patch is technically available.

## Retention, archival, and sunset

Each collection receives a retention class and named owner. At minimum, the
decision must distinguish:

- operational hot retention;
- late-peer backfill availability;
- legal or audit hold;
- cold archive format and destination class;
- logical deletion or sunset visible to replicas;
- cryptographic erasure through key destruction, where applicable; and
- coordinated physical purge from peers, archives, and backups.

The selected class and evidence downgrade follow the
[retention and erasure lattice](schema-retention-lattice.md). In particular,
destroying a payload or key can preserve commitment evidence while making
plaintext reconstruction impossible; projections and exports must report that
downgrade rather than continuing to claim reconstructibility.

A replicated tombstone is not proof that all physical copies were erased.
Likewise, removing a row from a projection does not remove its source facts.

Enterprise export must retain enough information to verify and reconstruct the
record outside the live node:

- collection and schema version;
- `_docID` and composite commit CID;
- relevant field CIDs;
- commit signature and signer;
- logical correlation IDs;
- source/lineage references;
- the applied ACP and redaction decision; and
- export contract version.

## Review workflow

Substantive schema work follows this sequence:

1. Classify the collection and complete its ledger entry.
2. Write the identity, authority, replication, ACP, and retention invariants.
3. Map every writer and ambiguous read.
4. Decide the target schema and successor/backfill path.
5. For changed lifecycle or provider-input invariants, update Lean first.
6. Drive conformance tests from the model and ledger decisions.
7. Implement the Rust and migration steps.
8. Test local writes, concurrent/replicated conflicts, late-peer backfill,
   unauthorized reads, time-travel reconstruction, and archive projection.

The full package and workspace gates remain required after implementation:
`cargo test -p gents` and `cargo check --workspace --all-targets`.

## Per-collection completion criteria

A collection review is complete only when:

- one document has one stated meaning and primary archetype;
- logical ID, `_docID`, version CID, and signer roles are unambiguous;
- creation and every legal transition have an authorized writer;
- live gossip and late-peer backfill are tested or explicitly absent;
- unauthorized normal, CID, and commit-history reads fail;
- duplicate or concurrent authorship has deterministic behavior;
- projections carry exact source-version references;
- retention, archival, sunset, and physical-erasure limits are documented; and
- the migration and formal-conformance impact is recorded.
