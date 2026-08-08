# Track D schema audit: configuration, automation, and placement

Status: evidence-backed design audit for issues #1063 and #1067;
recommendations are not yet accepted schema decisions.

This audit covers the Track D collections in the schema decision ledger. It
uses the shipped SDL and runtime as evidence, then proposes a durability
contract for an enterprise, multi-host Gents deployment. “Current” statements
below describe repository evidence. “Recommendation” statements are design
proposals that require review, Lean changes where noted, conformance tests, and
breaking schema work.

Retention classes and post-erasure evidence states are subordinate to the
[shared retention and erasure lattice](schema-retention-lattice.md); the local
class labels below refine that common contract rather than replace it.

## Executive findings

1. **The resolved runtime generation has no durable source-version manifest.**
   `DocumentRecord<T>` retains `_docID` but no composite commit CID, and the
   resolved snapshot drops even that `_docID`
   (`crates/gents/src/agent/document_view/mod.rs:19-37`,
   `crates/gents/src/agent/document_view/snapshot.rs:75-223`). The
   `RenderedRequest` manifest therefore honestly remains `CapturedOnly`: it
   pins the `AgentRequest`, endpoint, and sent body, but explicitly does not pin
   configuration or transcript versions
   (`crates/gents/src/rendered_request/mod.rs:379-419`).
2. **Desired, resolved, and observed state are not consistently separated.**
   `InferenceBackend` combines routing intent/secrets with probe observations;
   `Schedule` and `EventTrigger` combine operator intent with runtime cursors
   and counters; `PeerRegistry` combines operator metadata with a heartbeat;
   and `PersonaConfigRequest` combines an immutable request with its mutable
   result. `AgentRuntime`, `ToolServiceHealthState`, and `PeerPairingApplied`
   are already recognizable observed-state projections.
3. **Principal placement is under-specified.** `AgentRuntime` is uniquely keyed
   only by `agent_did`, and `PeerEndpoint` is uniquely keyed only by DID. Those
   identities cannot represent or fence multiple deployment hosts. The stated
   invariant “each `(did, behavior)` runs on exactly one deployment” needs an
   explicit deployment/lease document, not a mutable agent-wide status row.
4. **Logical unique indexes do not define a replicated winner.** The config
   client now detects multiple live rows instead of blindly accepting the first
   (`crates/gents/src/config_client/common.rs:7-44,107-116`), which is the right
   fail-closed read behavior. It still does not provide distributed consensus
   for concurrent writers. Every replicated desired or authorization fact
   needs a single-authority or deterministic conflict contract.
5. **The network control plane has stronger application signatures than the
   general schema, but still lacks DefraDB-layer ACP.** Network materialization
   verifies the network admin, membership, endpoint signature, membership
   status, and endpoint freshness
   (`crates/gents/src/agent/p2p_reconcile/network.rs:63-139`). No Track D schema
   carries an installed `@policy`; `ProjectionAcpBinding.policy_id` is
   configuration, not evidence that policy/relationships were installed.
6. **The nonce model is safe only under a single write authority.** Lean and the
   issuer reconciler model an atomic nonce bind. The implementation uses a
   local unique index, re-reads after create, and proceeds only for its claimant
   (`crates/gents/src/agent/p2p_reconcile/bearer_claim.rs:157-194,415-494`). Two
   disconnected issuer hosts can each accept a different claimant before
   replication. Multi-host issuance therefore requires a fenced authority or a
   stronger arbitration protocol.
7. **Breaking schemas are appropriate for this pass.** These changes alter
   identities, archetypes, ownership, and collection placement rather than
   merely adding nullable fields. The implementation plan should use new schema
   roots/successor collections plus explicit export/backfill tooling where old
   data matters; it should not preserve a mixed model solely for in-place
   compatibility.

## Current state: evidence

### How desired configuration reaches a provider call

The current document control path is:

```text
DefraDB current-value queries
  -> DocumentRuntimeView { _docID, parsed value }
  -> reference resolution and local health/tool-ceiling checks
  -> ResolvedRuntimeSnapshot generation
  -> request selects behavior from active generation
  -> prompt + tool surface + provider parameters
  -> transport-body RenderedRequest capture
```

The loader records `_docID` for principal, behavior, skill, tool selection,
profile, backend, OAuth credential, task, schedule, and event trigger. It does
not read or retain composite commit CIDs
(`crates/gents/src/agent/document_view/load.rs:18-185`). Resolution then:

- rejects unavailable backends and locally vetoed backend health;
- requires an enabled OAuth credential for OAuth provider kinds;
- resolves behavior references to one backend, inference profile, and optional
  tool selection;
- computes effective skills from every loaded skill;
- builds the behavior and resolves the live tool surface; and
- resolves tasks, schedules, event triggers, and paired-peer DIDs.

The relevant code is
`crates/gents/src/agent/document_view/snapshot.rs:75-223,251-317,326-359`.
The resolved structs carry values, not source `DocumentVersionRef`s. Later
configuration changes do not mutate an already-built `Arc<AgentBehavior>`, but
there is no durable record of which generation or source versions served a
request.

The final HTTP body is nevertheless strong evidence of what was sent. The
transport capture records the exact body, observed endpoint, request document
version, and an assembly trace. It deliberately reports `CapturedOnly`
(`crates/gents/src/rendered_request/mod.rs:379-455,618-700`). This distinction
must remain: exact sent bytes are not proof that every input, signer, or policy
decision can be reconstructed.

### Existing desired-state machinery

Twelve collections participate in the manifest apply surface. Apply order
places backend/profile/tool/service/skill dependencies before behavior, then
tasks and bindings, then principal and event triggers
(`crates/gents/src/collection.rs:38-137`). `WorkspaceRoot` has an enum variant
but is intentionally excluded from the full desired-state `ALL` set
(`crates/gents/src/collection.rs:30-56`). Only `PeerPairingDesired` is currently
manifest-authoritative, so ordinary apply does not delete other live-only
configuration (`crates/gents/src/collection.rs:134-136`).

The Lean ApplyReconcile model and Rust reference model prove/order retryable
desired-state application and keep a separate opaque `live` map untouched
(`crates/gents/proofs/Proofs/ApplyReconcile.lean`,
`crates/gents/src/apply_model.rs:1-187`). This is useful separation at the
model layer, but several physical documents still mix desired and live fields.

### Existing observed-state writers

- `AgentRuntime` is runtime-owned and upserted by `agent_did`; startup,
  reconcile, and router observers update process and generation state
  (`crates/gents/src/runtime_status.rs:126-252,285-352`).
- `InferenceBackend.probe_status` and `last_probe` are written by startup and
  health probing. The newer in-memory K-state is intentionally local, but a
  successful probe still promotes the shared backend document
  (`crates/gents/src/backend_health.rs:1-12,284-331`;
  `crates/gents/src/backend_registry.rs:301-346`).
- `ToolServiceHealthState` is a separate per-`(service_id, agent_did)` observed
  row, written and deleted by the health checker
  (`crates/gents/src/health_checker.rs:383-430,770-909`). This is directionally
  correct, but its compound identity is only application-enforced.
- Schedule and trigger callbacks update counters and cursors in the desired
  documents. Both increments are read-then-write and explicitly permit lost
  increments (`crates/gents/src/document_config/schedule.rs:86-186`;
  `crates/gents/src/document_config/event_trigger.rs:39-143`).
- Pairing reconciliation explicitly reads desired and applied rows, computes a
  diff against actual P2P state, and persists `PeerPairingApplied` after each
  successful operation
  (`crates/gents/src/agent/p2p_reconcile/engine.rs:44-54,194-269,880-1017`).

## Recommended model: desired, resolved, and observed

These are three different facts and must not share ownership or lifecycle.

| Layer | Meaning | Writer | Durability rule |
| --- | --- | --- | --- |
| Desired | Authorized intent: what should run, with which behavior, tools, backend, automation, and placement | Operator/principal controller named by ACP | Mutable document history; immutable identity/placement scope; branchable only when peer-initiated catch-up or collection-scoped ACP is required |
| Resolved | One immutable, internally consistent generation derived from exact desired versions plus local constraints/discovery | Deployment reconciler | Append-only manifest with every source `_docID`/composite CID, resolution algorithm version, local deployment id, and output fingerprint |
| Observed | What a deployment probed, applied, or is currently doing | Exactly one deployment/lease holder | Host/deployment-scoped, replaceable or append-only observations; short retention; never overwrites desired state |

The missing middle layer is the central Track D deliverable. Add an immutable
`ResolvedAgentGeneration` (name provisional) keyed by
`(deployment_id, deployment-lease epoch, generation)` and carrying:

- principal, behavior, backend, inference profile, tool selection, and every
  selected skill `DocumentVersionRef`;
- every `ToolServiceRegistry` version consulted and an immutable discovery
  observation for the exact MCP tool definitions exposed;
- the effective local tool-ceiling/config version and canonical workspace-root
  decision (without leaking host secrets);
- backend provider/wire kind, model, normalized endpoint, admission capacity,
  and routing-decision algorithm version;
- versions of placement/lease and ACP binding inputs;
- an output fingerprint over the resolved behavior and tool surface; and
- publication time, deployment DID/host identity, and signer evidence.

Generation numbering is meaningful only inside that deployment/lease epoch.
Failover to another host publishes a new generation under its own fenced epoch;
it does not continue an unfenced agent-global counter.

Publish this fact before activating a generation. Carry its `_docID` and
composite CID into the active snapshot and every `RenderedRequest`. A request
must use one already-published generation; a later config update creates a new
generation and cannot rewrite prior provenance.

### Acquiring source versions

Replacing `DocumentRecord<T>` with a versioned record is necessary but not
sufficient. A current-value query followed by a later `_version` query is a
time-correlation race. Startup/bulk reconciliation must enumerate maximal
composite heads, time-travel read the chosen CID, and fail closed on unresolved
multi-head ambiguity. Live control events should use the exact event/mutation
CID and confirm it by CID time-travel read. Resolution must validate that every
cross-reference points to the intended owner/scope before publishing the
generation.

## Render-contributing provenance inventory

| Source | Current effect on rendering/execution | Required immutable evidence |
| --- | --- | --- |
| `AgentPrincipal` | Enables the principal and selects the default behavior | Principal document version and verified authorized signer; default selection result |
| `AgentBehavior` | System prompt, behavior name/context, model, compaction settings, and references to tools/profile/backend/skills | Behavior document version; resolved-reference manifest |
| `InferenceProfile` | Context/output limits, sampling, reasoning, stream/deadline, turn and retry behavior | Profile document version and normalized effective parameter object |
| `InferenceBackend` desired portion | Provider kind, wire API, endpoint, capacity, enabled state, model availability gate | Backend desired version, normalized endpoint/wire route, secret reference (never secret value) |
| Backend observed health | Can veto routing and generation availability | Deployment-scoped health observation version/sequence used for the decision; not a mutation of desired backend |
| `ToolSelection` | Native tools, execution policy, file root, MCP allowlist, approvals, subagents, memory/history/query tools | Selection document version plus effective ceiling/intersection result |
| `Skill` | Skill catalog in preamble and activated reminders; skill tool constraints | Version of every selected/effective skill, including explicit excludes and ordering algorithm |
| `ToolServiceRegistry` | Online registry rows determine available MCP services (`crates/gents/src/tool_surface/build.rs:433-455`) | Registry versions actually consulted |
| MCP discovery response | Dynamic `list_tools` schemas become provider tool definitions but are not durable config documents | New immutable discovery fact containing canonical service/tool schemas and endpoint observation |
| `ToolServiceHealthState` | Health can suppress or explain service availability | Deployment-scoped observation ref if it affected resolution; otherwise explicitly “not consulted” |
| `WorkspaceRoot` and host tool ceiling | Root publication and canonical path/ceiling constrain effective tools | Local config version and canonical resolved root fingerprint; raw path visibility governed separately |
| `OAuthCredential` / backend key | Authorizes transport; token rotation and account headers can change request handling | Confidential credential reference/version and auth-mode fingerprint, never access/refresh/id tokens or raw keys in general provenance |
| `Task` | Supplies the prompt template for automated requests | Task document version stamped into request lineage at materialization |
| `Schedule` / `EventTrigger` | Selects task, cadence/source/filter, concurrency, and execution origin | Exact desired trigger version; source event `_docID`/CID for events; observed cursor/result stored separately |
| `PeerPairingDesired` / membership | Affects which peers/targets are considered reachable/authorized | Placement or membership fact versions used for cross-deployment routing |

`RenderedRequest.request_json` remains the conformance oracle for bytes sent.
The new manifest proves why those bytes and tools were selected. Neither should
be substituted for the other.

## Per-collection contract audit

“Immutable target” names fields that should never change on one document. For
desired documents, other fields may change by authoring new commits. For facts
and commands, the recommendation is usually an immutable document plus a
separate result/revocation fact.

### Configuration and runtime

| Collection | Current identity and writers (evidence) | Immutable target and ownership recommendation |
| --- | --- | --- |
| `AgentPrincipal` | Unique `agent_did`; config import/CLI and bootstrap upsert; runtime reads by DID and `_docID` | `agent_did`, `created_at`, `created_by`; controller named by ACP is sole desired writer. Verify signer instead of trusting `created_by`. Principal DID is global, but configuration scope must name its control domain. |
| `AgentBehavior` | Globally unique `behavior_id`; operator/import, persona reconciler, and self-config can update it; references backend/profile/selection and skills | `behavior_id`, `agent_did`, `created_at`; owner is the principal controller. Prefer uniqueness `(agent_did, behavior_id)` unless behavior IDs are explicitly globally allocated. All render-relevant values are desired-only. |
| `AgentRuntime` | Unique `agent_did`; runtime status writer upserts process/generation/capacity/error fields (`runtime_status.rs:285-352`) | Replace with deployment-scoped observed identity `(deployment_id, agent_did)`; immutable deployment/principal keys and observation sequence. One lease holder writes its row. Do not use it as placement authority. |
| `InferenceBackend` | Unique `backend_id`; config writers set endpoint/key/capacity/models/enabled; probes update `probe_status`/`last_probe` | Split `InferenceBackendDesired` from `InferenceBackendHealth`. Desired: immutable `backend_id`, owner/scope, secret reference. Health: immutable `(deployment_id, backend_id, observation_id/sequence)` or one replaceable deployment-scoped projection. Remove raw `api_key` from gossipable config. |
| `InferenceProfile` | Unique `profile_id`; operator/import/self-config; behavior reads it by logical ID | `profile_id`, owning scope, created metadata. All sampling/retry/limit fields are desired. Declare global catalog versus principal-owned IDs; current schema does neither. |
| `ToolSelection` | Unique `selection_id`; operator/import/self-config/persona; `agent_did` filters ownership but is mutable | `selection_id`, `agent_did`, creation metadata. Owner is the named principal controller. Every field is desired. Validate every service/skill/subagent reference in the same ownership domain. |
| `Skill` | Unique `skill_id`; operator/import; selected through behavior refs and agent/scope matching | `skill_id`, `agent_did`, `scope`, creation metadata. Owner is principal controller. Prefer `(agent_did, skill_id)` scope or explicitly global signed catalog entries. Instructions and interfaces are render-contributing desired state. |
| `OAuthCredential` | Unique `credential_id`; login/refresh writers; `agent_did` already immutable; runtime loads enabled credential by provider | Local confidential identity `(agent_did, provider, account/credential_id)`. Provider and credential identity immutable; refresh rotates a secret version. Never gossip. ACP restricts metadata and secret access to the principal's credential broker; encrypt local/backup storage. |
| `WorkspaceRoot` | Unique `root_path`; operator CLI; directory projection publishes enabled raw paths | Host-local desired identity should be `(deployment_id, root_id)`, not a mutable/path-as-ID secret. `root_id` and deployment immutable; path mutable only through a new version. Publishing raw paths to clients is a separate redacted projection decision. |
| `ToolServiceRegistry` | Unique global `service_id`; config/import/CLI; tool resolution queries rows whose `status == "online"` | Split service desired identity/address from availability. Immutable `(scope, service_id)`; principal/deployment controller writes desired endpoint. Remove observed `status`/`version` unless they are explicitly operator intent. |
| `ToolServiceHealthState` | Application compound `(service_id, agent_did)`; health checker upserts/deletes; no composite uniqueness in schema | Replace `agent_did` with explicit `deployment_id` plus service ref; immutable scope fields. Health checker holding that deployment lease is sole writer. If mutable projection, conflicts fail closed and TTL expiry removes it; append facts if audit history matters. |
| `ProjectionAcpBinding` | Unique `binding_id`; desired-state/config writer; carries policy IDs, resource map, publication lifecycle | Immutable binding identity, owner agent, optional behavior/projection scope. Split draft/staging workflow from immutable published binding versions. Policy installation and relationship CIDs/receipts must be observed evidence, not inferred from `publication_status`. |

### Automation

| Collection | Current identity and writers (evidence) | Immutable target and ownership recommendation |
| --- | --- | --- |
| `Task` | Unique `task_id`; desired-state/config/self-config/persona writers; runtime resolves task to behavior/prompt | Immutable `task_id` and owner/principal scope. Desired-only document. Stamp task version onto every materialized request. Prefer `(owner_did, task_id)` unless IDs are a global catalog. |
| `Schedule` | Unique `schedule_id`; operator owns cadence/task/enabled/concurrency while runtime writes next/last/count fields | Split `ScheduleDesired` from `ScheduleCursor` and immutable `ScheduleFire` result facts. Desired identity/owner immutable. Cursor keyed by `(deployment_id, schedule_doc_id)` and fenced by a scheduler lease. |
| `EventTrigger` | Unique `trigger_id`; operator owns source/filter/task/enabled/concurrency while runtime writes last source/result/count | Split `EventTriggerDesired`, deployment cursor, and immutable dispatch result. Desired identity/owner/source scope immutable. A consumed event is identified by source `_docID` plus composite CID, not only `last_fired_source_doc_id`. |
| `PersonaConfigRequest` | Unique immutable `request_key`; phone creates pending row; server mutates it to applied/rejected (`persona_requests.rs:225-269,413-445`) | Keep immutable request fields and requester scope; move outcome to an immutable `PersonaConfigResult` keyed by request `_docID`/CID. Server authorized for the target principal signs result. The request's logical key is idempotency, not proof of caller identity. |

### Network, pairing, and placement

| Collection | Current identity and writers (evidence) | Immutable target and ownership recommendation |
| --- | --- | --- |
| `AgentDirectoryEntry` | Hash `directory_key(source_did, agent_did)`; source projector upserts/deletes its partition; pure projection (`directory_projection.rs:1-10,184-338`) | Keep projection identity and immutable `source_did`; make all inputs source-versioned in a projection manifest. Only source DID projector writes its partition. Regenerable. |
| `AgentNetwork` | Unique `network_id`; admin-signed root record; materializer verifies admin signature | Immutable signed fact: all defining fields including `admin_sig`. Updates create a successor/version with explicit predecessor; do not silently mutate the signed payload. Admin signer is sole authority. |
| `NetworkMembership` | Hashed `membership_key(network_id, member_did)`; admin authors signed `active`/`revoked` state; materializer verifies admin | Prefer immutable signed grant and revocation facts with explicit target grant/ref. If one document remains, identity fields immutable and concurrent status heads fail closed. The admin signer, not `admin_did` text alone, is authoritative. |
| `NetworkJoinRequest` | Hashed request key for `(network_id, candidate_did)`; candidate-signed request, informational status | Immutable command including key, candidate/network, endpoint claim, timestamp, signature. Outcome is membership/rejection fact; remove mutable informational status or make it a projection. Candidate signer is creator. |
| `PeerEndpoint` | Unique immutable DID; that DID's heartbeat upserts node/address/time/signature (`endpoint.rs:18-124`) | A principal can have multiple deployments, so key by `(did, deployment_id/node_id)` and make each signed endpoint assertion immutable or sequence-fenced. Owner signer writes only its scope. Current one-DID row causes host overwrite/conflict. |
| `PeerRegistry` | Unique `peer_id`; local P2P heartbeat updates addresses/templates/status/time while operator update may set display metadata (`registry.rs:72-128,128-239`) | Split operator peer metadata/desired admission from local observed advertisement. Observed identity `(deployment_id, peer_id)` and TTL; operator display name must not share a heartbeat document. |
| `PeerPairingDesired` | Unique `peer_id`; manifest, discovery, network and invite paths write it; `source` partitions ownership; only manifest-authoritative config collection | Identity must include owner/source partition, e.g. `(deployment_id, source, peer_id)`, or use separate collections per authority. Immutable placement/filter fields. Current unique `peer_id` lets independent controllers overwrite one another. |
| `DataPlanePairingDesired` | Unique `peer_id`; reciprocal/network flows write local desired data-plane wiring | Same partitioned identity rule as pairing desired. Treat signed network/endpoint facts as inputs; the row itself is a local materialization and cannot override signed address evidence. |
| `PeerPairingApplied` | Unique `peer_id`; pairing reconciler writes applied collections/addresses/filter after each successful operation | Deployment-scoped observed identity `(deployment_id, source, peer_id)`; only reconciler lease holder writes. Regenerable from actual state plus desired, short retention. |
| `ConsumedInviteNonce` | Unique nonce; issuer creates a local burn and re-reads it; no branchable backfill | Immutable replay-prevention fact: nonce, issuer, claimant, consumed time, claim CID. For more than one issuer host, require one fenced nonce authority or a consensus/serialization service; Defra replicated uniqueness alone is insufficient. Never purge before every invite expiry plus audit margin. |
| `ReciprocalConversationIntent` | Unique `member_did`; issuer-local CLI/reconciler upsert; unsigned row authorizes data push | Make an immutable issuer-signed intent keyed by `(issuer_did, member_did, scope/template)` with revocation/sunset fact. Keep inbound replication disabled unless signer and ACP checks are enforced. This is authorization-bearing, not ordinary mutable config. |
| `PairingBearerClaim` | No explicit logical unique key; claimant writes self-signed claim carrying signed token; issuer validates both, freshness, and nonce | Immutable command. Add deterministic claim id/idempotency key over token nonce and claimant DID for queries, while retaining `_docID`/CID as durable identity. Claimant field and entire signed payload immutable. |
| `BearerPairingReady` | Unique immutable readiness hash; issuer authors signed acknowledgement after replicator application | Immutable acknowledgement fact. Include refs/CIDs for claim, membership, desired pairing, and applied observation that justified readiness. Issuer signer is required. Do not treat readiness as timeless after revocation. |

## Placement, branchability, gossip, ACP, and retention decisions

The following are recommended target decisions, not descriptions of current
deployment. `B` means branchable because authorized late peers/deployments need
collection-history backfill; `N` means non-branchable/local because backfill is
handled by controlled backup/restore or a successor projection. Branchability
does not itself enable gossip.

Retention classes:

- **C-config:** retain all published desired versions needed by any retained
  run; archive indefinitely or for the enterprise audit period.
- **R-resolved:** retain at least as long as every run that references the
  generation; archive with source commits and signer evidence.
- **O-observed:** hot TTL plus a bounded operational archive; not legal truth.
- **A-authority:** retain signed grants/revocations/nonces for the security and
  legal-audit horizon; legal hold overrides purge.
- **S-secret:** active plus rotation/revocation window; encrypted local backup;
  cryptographic erasure and coordinated secret-store purge.
- **P-projection:** short hot TTL; regenerate; archive only if an export
  contract needs it.
- **Q-command:** retain request and immutable outcome for audit/idempotency
  horizon.

| Collection/group | Branch | Live gossip and late backfill | ACP relationship | Retention / sunset |
| --- | ---: | --- | --- | --- |
| Principal/behavior/profile/tool selection/skill/task desired | B | Filter by owning control domain/principal to authorized deployment hosts; same filter for late-host backfill | controller writes; principal/deployment reads; no unrelated peer | C-config; sunset fact disables new use, while old versions remain through referenced-run retention |
| Backend and service desired | B when shared across deployments; otherwise N | Deployment-pool filtered; never gossip raw keys | infrastructure controller writes; assigned deployments read | C-config; endpoint/secret rotation preserves old referenced version metadata |
| OAuth credentials | N | Never live gossip; controlled encrypted secret backup only | credential broker and owning principal/runtime only | S-secret; revoke, destroy keys, purge replicas/backups under policy |
| Workspace roots/tool ceilings | N | Local only; publish a redacted pick-list projection if needed | host operator writes; assigned principal reads allowed subset | C-config locally; redact path on export; purge with host decommission |
| Resolved generation (new) | B | Principal/deployment participants; backfill with retained runs | deployment reconciler writes; run participants/auditors read | R-resolved |
| Agent/runtime/backend/service health and pairing applied | N by default | No cross-host gossip; expose redacted fleet projection. Backfill unnecessary | deployment writer; fleet operators read projection | O-observed; TTL/tombstone after deployment sunset |
| Schedule/trigger desired | B | Owning principal's scheduler candidates; late scheduler backfill | automation controller writes; elected scheduler reads | C-config |
| Schedule cursor/event cursor/fire result | Cursor N, result B | Cursor only to lease holder; immutable results to principal audit peers | scheduler lease holder writes; participants/auditors read results | cursor O-observed; result Q-command/audit |
| Persona request/result | B | requester-to-home filtered in both directions; late authorized device backfill | requester creates request; principal controller creates result | Q-command; redact request detail on governed export |
| Directory entry | B | Existing source/recipient-filtered machine/client discovery channel; branchability is justified by late-client backfill | source projector writes partition; paired clients read | P-projection; delete on source/principal sunset and regenerate |
| Network root/membership/revocation/endpoint/join/claim/readiness | B | Network/claimant/issuer-specific filters, never “all paired peers”; late member receives only authorized control history | signer-specific create rights; network members read scoped facts; unrelated/anonymous denied | A-authority or Q-command; revocation retained; purge only after network sunset and hold expiry |
| Peer registry/desired/applied local materializations | N | Local reconciliation inputs only; signed facts are the portable source | deployment controller/reconciler only | C-config for desired, O-observed for registry/applied |
| Nonce and reciprocal intent | B only inside a trusted authority cluster; otherwise N with one authoritative host | Never accept arbitrary inbound rows; authority-cluster backfill must preserve serialization | issuer authority writes; security auditor reads | A-authority; nonce retained beyond token expiry; signed sunset/revocation precedes purge |
| Projection ACP binding | B | Owning principal/deployment scope; backfill before projection reads are enabled | security controller publishes; runtime reads; policy-install service writes receipt | C-config plus immutable installation receipts |

No target collection should be considered ACP-complete until tests prove
anonymous/unrelated denial for normal reads, CID reads, `_version`, `_commits`,
mutation, and deletion. Policy installation and relationship creation must be
atomic or fail closed before any protected document is made discoverable.

### Encryption boundaries

DefraDB ACP is the data-layer access boundary, not at-rest encryption. Raw API
keys and OAuth tokens should move out of ordinary desired documents into a
local encrypted secret provider. Replicated config stores opaque secret refs.
Enterprise export contains those refs and rotation metadata, never the secret.
Network tokens/claims and host paths receive field-level redaction contracts.

## Multi-host conflicts and recovery

### Desired configuration

One authorized controller may update a desired key. If offline multi-controller
writes are required, the key must also carry a monotonic authority epoch and
signed predecessor. Reconciliation accepts a head only when:

1. the signer is authorized for that owner/scope and epoch;
2. all immutable identity/placement fields agree;
3. exactly one maximal authorized head exists; and
4. every referenced source version is readable and signer-valid.

Two valid concurrent maximal heads are an explicit `ConfigConflict`, not
last-write-wins and not `limit: 1`. Runtime keeps the last verified generation
active and publishes an observed conflict; it does not synthesize a new merge.

### Runtime placement

Add a desired deployment assignment and a renewable observed lease/fence. The
assignment identifies `(agent_did, behavior_id, deployment_id)`. Every claim,
scheduler cursor, runtime observation, and outbound reconciliation action
carries the lease epoch. Only the current epoch can make new externally visible
transitions. Stale hosts may retain blocks but cannot claim work or publish a
new resolved generation.

### Automation

Schedules need one fenced cursor writer. Fire facts use a deterministic key over
`(schedule_version, nominal_fire_time)` so recovery can retry idempotently.
Event dispatch uses `(trigger_version, source_doc_id, source_commit_cid)`.
Counters are projections over fire/result facts rather than correctness state.
This removes the existing tolerated lost-increment race.

### Network authority

Signature verification continues to be mandatory, but concurrent signed state
must have defined semantics:

- network roots form an explicit signed successor chain;
- membership grants and revocations are immutable facts, with revocation
  dominating a referenced grant;
- endpoint assertions are per deployment and sequence/expiry bounded;
- invite nonce redemption is serialized by one authority epoch; and
- readiness references the exact applied observation and becomes invalid when
  its membership/intent is revoked.

## Lean and conformance impact

The change is not schema-only. Required foundation work includes:

1. **Resolved configuration provenance model.** Prove that a published
   generation is a function of the exact source-version set, that changing any
   render-contributing version changes the manifest identity, and that a
   provider send references one previously published generation.
2. **Desired/resolved/observed separation.** Extend ApplyReconcile so apply can
   mutate only desired collections; prove observed and immutable result facts
   are untouched. Preserve prefix/retry convergence with successor collections.
3. **Configuration head selection.** Model authorized heads and prove the
   resolver never activates an ambiguous or unauthorized head. Conformance
   cases must cover two replicated conflicting documents despite unique logical
   IDs.
4. **Deployment fencing.** Model assignment plus lease epoch and prove at most
   one deployment can claim `(did, behavior)` or advance its schedule cursor at
   an epoch.
5. **Automation identity.** Replace mutable counters as correctness state with
   deterministic fire/event keys; prove crash retry idempotence and no double
   materialization.
6. **Network membership succession and nonce authority.** Preserve current
   signature/freshness/revocation theorems while adding multi-host issuer
   fencing. The current nonce-set proof must not be claimed for disconnected
   writers using local unique indexes.
7. **Projection provenance.** Directory/readiness projections must reference
   exact input versions; prove different source partitions do not overwrite
   each other and stale source versions cannot authorize new placement.

Conformance suites should exercise startup bulk reads, live update CIDs,
multi-head ambiguity, unauthorized signer, ACP denial (including history/CID
reads), lease failover, partition/rejoin, late-peer backfill, secret
non-replication, archive reconstruction, and legal-hold/sunset behavior.

## Breaking schema and backfill implications

Assume this Track D redesign is intentionally breaking.

- Use successor collections when identity/archetype changes: backend
  desired/health, schedule desired/cursor/fire, trigger desired/cursor/result,
  persona request/result, deployment assignment/lease/status, signed
  membership transitions, and deployment endpoints.
- Do not add compatibility fields that preserve ambiguous logical-ID reads or
  mixed writer ownership.
- Bootstrap a new desired collection by exporting the selected old document
  with `_docID`, composite CID, schema root, and an explicit operator selection
  receipt. If multiple live or maximal heads exist, stop for conflict
  resolution.
- Backfill only canonical desired state. Do not import mutable health/counters
  as authority; optionally archive them as legacy observations.
- Convert secrets through the secret broker and write only new secret refs.
  Never place legacy raw values in migration logs or general archives.
- Re-issue signed network facts into the new canonical payload rather than
  copying signatures over changed encodings. Preserve the old signed record as
  archived evidence and link it from the successor.
- Populate new resolved-generation facts only for future activations. Historic
  runs without complete source CIDs remain `CapturedOnly`; never manufacture
  “verified” provenance from current config.
- Branchability changes require successor collection roots. Backfill tools must
  verify target ACP/relationships before copying any block or document.

## Prioritized child-issue candidates

### P0: blocks a truthful durability claim

1. **ResolvedAgentGeneration and render provenance.** Add versioned document
   loading, deterministic/fail-closed head selection, immutable generation
   publication, and a generation `DocumentVersionRef` in `RenderedRequest`.
   Include tool-discovery observations and task/trigger source versions.
2. **Deployment identity, assignment, and lease fencing.** Replace
   agent-wide `AgentRuntime`/endpoint identity with explicit deployment-scoped
   desired placement, lease epoch, and observed status. Fence request claiming,
   scheduler cursors, and reconciliation actions.
3. **Track D ACP installation and negative tests.** Define controller,
   principal, deployment, participant, and auditor relationships; install them
   through the embedded node; prove normal/history/CID reads and writes fail
   closed before policy registration.
4. **Secret extraction.** Remove raw backend keys and OAuth tokens from general
   config/replication; integrate encrypted local secret refs, rotation,
   revocation, backup, and cryptographic-erasure tests.
5. **Automation desired/cursor/result split.** Introduce schedule/event result
   facts with deterministic versioned source keys and lease-fenced cursors;
   stamp exact task/trigger/event versions onto `AgentRequest`.
6. **Multi-host nonce redemption.** Add issuer authority fencing or an
   explicitly serialized redemption service; update Lean so disconnected local
   unique indexes are not modeled as one atomic set.

### P1: required for multi-host recovery and governed backfill

7. **Principal-owned config identities and conflict protocol.** Decide global
   versus scoped IDs, make owner/filter fields immutable, add authorized-head
   conflict reporting, and remove ambiguous logical-ID reads.
8. **Backend/service desired versus health split.** Move probe/status fields to
   deployment observations; make registry service identity scoped; persist the
   exact discovery/tool-schema fact used by a resolved generation.
9. **Signed network successor facts.** Separate membership grant/revocation and
   join outcome, add predecessor/target refs, deployment endpoints, and exact
   readiness lineage.
10. **Pairing ownership partitions.** Change pairing desired/applied identities
    to include deployment and source authority; prove manifest, discovery,
    network, and reciprocal controllers cannot overwrite one another.
11. **Branchability/backfill profile.** Create successor branchable schemas for
    shared desired/resolved facts, explicit replication filters, late-host
    authorization tests, and archive export manifests.

### P2: operational governance and projection cleanup

12. **Persona immutable result split.** Preserve phone request and server result
    as separately signed/versioned facts with requester pushback ACP.
13. **Peer registry and directory projection cleanup.** Split operator metadata
    from heartbeat, add projection source-version manifests, redact workspace
    paths, and establish TTL/rebuild behavior.
14. **Retention, legal hold, and enterprise export implementation.** Encode the
    retention classes above; export schema roots, document/field CIDs, signers,
    source refs, ACP/redaction receipts, and coordinated sunset/purge status.
15. **Projection ACP publication receipts.** Replace mutable publication status
    with immutable published-binding versions and verifiable policy/relationship
    installation receipts.

## Decision summary

Track D should not be declared “decided” yet. The code already has valuable
foundations—ordered desired apply, fail-closed duplicate reads, immutable
transport capture, signed network materialization, and explicit
desired/applied pairing reconciliation. The decisive next step is to persist
the resolved generation that connects them. Without that fact and explicit
deployment fencing, current configuration is recoverable as a collection of
documents but not provably the configuration one host used for one provider
call, and multi-host control remains vulnerable to ambiguous concurrent
authority.
