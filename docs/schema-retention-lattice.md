# Schema Retention and Erasure Lattice

This document is the single retention vocabulary for the schema audits under
issue #1063. Collection audits select classes and dependencies from this file;
they do not invent independent default durations.

Retention policy is deployment/tenant policy. The schema contract defines what
must be retained together, what guarantee remains after each kind of erasure,
who may place a hold, and what evidence proves a purge. It deliberately does
not prescribe a universal number of days or years.

## Evidence states after retention actions

| State | Required retained material | Claim that remains truthful |
| --- | --- | --- |
| `full_reconstruction` | encrypted payload, decryption key, exact source manifest, schema/projection versions, commit/signature evidence, ACP/redaction receipt | Authorized replay can recover the governed plaintext and verify its lineage. |
| `redacted_reconstruction` | approved redacted payload, exact source manifest, redaction contract/receipt, commit/signature evidence | Authorized replay can recover only the recorded redacted view and verify which source versions and policy produced it. |
| `commitment_only` | document/field CIDs, signatures/signers, non-sensitive lineage and erasure receipt; payload or key is gone | An auditor can verify retained commitments and the erasure event, but cannot reconstruct or reclassify the plaintext. |
| `tombstone_only` | signed sunset/tombstone, conflict/hold disposition, purge receipts | An auditor can verify that the governed record existed and was retired; content and full lineage are unavailable. |
| `purged` | only policy-permitted aggregate/accounting evidence | No record-level reconstruction or verification claim is made. |

`CapturedOnly`, `Reconstructible`, and `Verified` describe provenance before a
retention action. The state above describes what evidence remains afterward.
Erasing a required source or key must downgrade the advertised capability; a
manifest may never continue to claim full reconstruction merely because its
CIDs survived.

## Retention classes

| Class | Examples | Hot/archive rule | Erasure rule |
| --- | --- | --- | --- |
| `payload-sensitive` | messages, prompts, reasoning, tool arguments/results, rendered provider bodies, objectives | Hot and archive periods are independently configured by data classification and purpose. Archives are encrypted and access-controlled. | Payload/key destruction may intentionally leave `commitment_only`; all projections and caches must be purged or downgraded too. |
| `provenance-metadata` | source manifests, document/field CIDs, schema and projection versions, signer state, conflict/repair and export receipts | Retain at least as long as the audit claim or any retained derived artifact that cites it. | May outlive payload only if it contains no recoverable sensitive material and explicitly records the resulting evidence state. |
| `authority` | approvals, policy publications, membership grants/revocations, nonce burns, placement leases/epochs | Retain through the authorization/replay horizon and every dependent audit period. Revocation evidence must not disappear while a grant can still authorize a retained action. | Purge requires authority-owner approval and no dependent record or replay window. Signature evidence normally survives payload erasure. |
| `desired-config` | principal, behavior, inference profile, tool selection, task, schedule/trigger intent | Retain every exact version referenced by a retained generation or run, plus the configured audit horizon. | Sunset blocks new resolution; referenced versions remain until dependants downgrade or expire. Secret values are never part of this class. |
| `resolved-generation` | immutable effective config/tool/placement manifest | Retain at least as long as every run or render that references it. | Can become `commitment_only` if governed source payloads or secrets are erased; never reconstruct missing inputs from current config. |
| `operational-observation` | live response overlay, health, runtime status, cursors, applied pairing state | Short hot TTL; archive only when an incident/audit policy requires it. It is not authority or canonical history. | Expire after the canonical outcome/result is durable and dependencies no longer require the observation. |
| `secret` | API/OAuth tokens, encryption keys, secret-store versions | Local encrypted custody, explicit rotation/revocation and separately governed backup. Never general gossip or ordinary provenance export. | Cryptographic erasure destroys every live/backup key copy and records a non-secret receipt. A secret reference may remain. |
| `regenerable-projection` | conversation head, directory entry, timeline cache | Short hot TTL; archive only with a named export contract. Source manifest is mandatory if retained. | Delete/rebuild independently. Deleting the projection never counts as deleting its sources. |

## Dependency rules

1. A derived record cannot promise a stronger evidence state than its weakest
   required source. If one required source becomes `commitment_only`, the
   projection cannot remain `full_reconstruction`.
2. A source may have a shorter payload lifetime than its metadata. The source
   manifest and erasure receipt then survive without plaintext and the status
   is downgraded explicitly.
3. A source cannot be purged while an active legal hold applies to it or to a
   dependent bundle that requires it. Holds form a dependency closure over
   exact `_docID`/CID references, not logical IDs.
4. Configuration and resolved-generation versions referenced by retained runs
   survive for the run audit period. Secret values do not: retain a versioned
   secret reference and auth-mode fingerprint, never the secret itself.
5. Authority facts survive every action they authorize or revoke through the
   configured audit/replay horizon. A projection of an authorization decision
   is not a substitute for the signed fact.
6. A fork, compaction, timeline, adapter projection, or archive bundle records
   whether each omitted source was denied, redacted, erased, expired, missing,
   or never captured. Silent omission is not a valid retention state.

## Policy record requirements

Each deployable retention policy version must name:

- tenant/control domain and policy id/version;
- duration or event condition for hot data, cold archive, key destruction, and
  final record-level purge for each class;
- data classification and permitted archive destination class;
- hold placers, releasers, scope, reason, and immutable hold/release receipts;
- peer, archive, backup, cache, and key-custody systems that must acknowledge a
  purge;
- downgrade behavior for every projection and export contract;
- retry/escalation behavior when a purge target is unreachable; and
- policy signer and effective/sunset times.

Policy timestamps use `DateTime`. Absence is null, not an empty string or zero.
A policy update creates a new version and does not reinterpret an earlier
export or erasure receipt.

## Archive, restore, and purge receipts

An archive bundle records its evidence state and contains only data permitted
by that state. A full or redacted reconstruction bundle includes exact source
references, schema roots, projection/redaction versions, signer evidence, ACP
decision receipt, encryption class/key id (never key material), hold state,
exporter identity, destination class, and destination receipt.

Restore validation targets an empty authorized node or verifier and checks:

1. manifest closure and schema availability;
2. CID/field commitment and signature evidence;
3. authorization to restore each classification;
4. canonical projection replay at the declared evidence state; and
5. explicit rejection of missing, ambiguous, erased, or unauthorized sources.

A purge receipt identifies exact document versions and dependent artifacts,
the governing policy/hold release, each required system acknowledgement, key
destruction evidence where applicable, completion time, and signer. A DefraDB
tombstone alone is not a complete purge receipt.

## Collection audit checklist

Every collection decision must select:

- primary retention class and owner;
- payload and metadata classes when they differ;
- minimum dependencies that preserve each advertised evidence state;
- legal-hold closure;
- sunset fact and visibility behavior;
- archive/export state and restore test; and
- peer/archive/backup/key purge acknowledgements.

Until these selections and their workflows are implemented, a collection is
not enterprise-retention complete regardless of its branchability or history.
