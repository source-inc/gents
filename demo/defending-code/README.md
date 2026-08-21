# Defending-code pack

This pack adapts the static find-and-fix workflow from Anthropic's
`defending-code-reference-harness` into a Gents-native document graph. It is
deliberately different from `security-scan`: there is no regex kickoff and no
generic datastore console. The graph starts by building a threat model, uses
that model to partition a static review, independently verifies the candidate
ledger, clusters confirmed consequences into root causes, checks repository
contracts, validates proposed patches, re-attacks them, and publishes one
campaign report.

```text
DefendingCodeJob
  -> DefenseThreatModel
  -> N DefenseReviewArea
  -> N scanners -> DefenseCandidateFinding* + N DefenseScanResult
  -> scan barrier -> K DefenseVerificationAssignment
  -> K per-document triggers -> K independent verifier requests
  -> K DefenseFindingVerdict + K DefenseVerificationCompletion
  -> completion barrier
  -> one triage reducer -> confirmed DefendingFinding*
  -> one root-cause reducer -> M DefenseRootCauseCluster
  -> M triggered contract/spec reviewers -> M DefenseContractReview
  -> contract barrier -> M DefensePatchAssignment
  -> M patch authors -> M DefensePatchCandidate
  -> M triggered mechanical validators -> M DefensePatchValidation
  -> M triggered maintainer reviewers -> M DefensePatchReview
  -> M triggered adversarial re-attackers -> M DefensePatchSecurityReview
  -> one report barrier -> DefenseReport
```

Every intermediate artifact is a typed DefraDB document correlated by
`run_id`. Agents never receive `defra_query`: each read tool is bound to one
collection, a fixed projection, and a runtime-filled `run_id`; each write tool
is bound to one collection and an explicit field allowlist.

Each event edge treats its created source document as the immutable stage-input
envelope. Per-document tasks interpolate the source fields directly through
`{{ doc.* }}`; group barriers interpolate the complete closed source ledger
through `{{ group.docs }}`. The model is never asked to query the document that
triggered its own request. A stage receives typed read tools only for other
ledgers it must join, and one schema-bound write surface for its output facts or
receipt. Runtime-filled correlation and source fields carry identities forward
without asking the model to transcribe them.

Prompts follow a bounded-interface, open-investigation rule. They provide the
best available evidence, the stage objective, the typed output contract, and
hard authority/provenance constraints. They do not prescribe a search recipe,
tool-call order, or chain of reasoning. Investigative agents choose how to use
their repository, shell, and LSP capabilities; document-only reducers remain
deliberately deterministic because their task is ledger reconciliation rather
than source analysis.

The threat-model bootstrap freezes the audited Git revision and dirty-tree
observation once. That provenance is copied through review areas, candidates,
verdicts, confirmed findings, root-cause clusters, and patch validation. A
dirty tree is never silently reconstructed from a clean commit checkout.
Patch proposals bind their exact raw diff bytes with SHA-256; validation,
maintainer review, and independent security review each persist structured
base/tree/digest receipts. The final report compares those receipts and emits
`complete`, `blocked_provenance`, `inconsistent`, or `partial` as a typed audit
status instead of making consumers infer campaign health from prose.

The current datastore surface supports bounded creates and reads, not bounded
updates, while event edges are create-only. State changes are therefore
append-only facts (`CandidateFinding -> FindingVerdict -> DefendingFinding`,
and `PatchCandidate -> PatchValidation -> PatchReview -> PatchSecurityReview`)
rather than in-place mutations. This
keeps the full audit history in the graph and avoids introducing free-form
GraphQL merely to simulate status updates.

## Safety boundary

This is the reference harness's **static mode**. Threat-model, planning,
scanning and verifier agents receive native LSP plus an unrestricted shell so
they can use `rust-analyzer`, `rg`, and Git history rather than depending on a
single file reader. Their shell network mode is enabled, their file tools are
read-only, and their prompts prohibit source edits, dependency installation,
builds, tests, and target execution. Run this pack only against an authorized,
trusted checkout and network environment.

Patch authors receive read-only file tools, LSP, and unrestricted shell so they
can inspect Git objects or create an exact temporary checkout when the live
tree has moved. They emit unified diffs into `DefensePatchCandidate.diff` and
do not modify the operator checkout. Contract
reviewers, mechanical validators, maintainer reviewers, and security
re-attackers receive shell plus LSP. Until managed workspaces can bind a
request's real file root, shell CWD, LSP root, and repository-instruction root,
the mechanical validator applies the diff only in a unique disposable local
clone at the recorded base revision. It records exactly which format, compile,
test, and proof gates ran. The original checkout remains unchanged.

The report stage can only use collection-bound graph tools. Findings, source
excerpts, command output, and diffs are treated as untrusted evidence by
downstream prompts, not as instructions. This remains an authorized source
review pack, not the reference harness's two-container untrusted-target
execution boundary.

## Run

```bash
GENTS_DEFENDING_ROOT=/path/to/repository \
  gents demo run defending-code
```

From this repository, the Make target exposes the same controls:

```bash
make defend \
  DEFENDING_ROOT=/path/to/repository \
  DEFENDING_ENDPOINT=http://100.73.235.38:8000/v1 \
  DEFENDING_MODEL=GLM-5.2 \
  DEFENDING_MAX_CONCURRENT=8
```

While the runtime is active, launch the live document-graph visualizer in a
second terminal:

```bash
make defend-page
```

It opens `http://127.0.0.1:19194/?pack=defending`, proxies the runtime on
`DEFENDING_PORT` (19193 by default), and shows both fan-outs, ledger counts,
per-request token totals, interpolated prompts, typed documents, and tool-call
details. The page is read-only and does not seed or mutate the campaign.

Useful controls:

```bash
export GENTS_DEFENDING_ENDPOINT=http://127.0.0.1:8000/v1
export GENTS_DEFENDING_MODEL=GLM-5.2
export GENTS_DEFENDING_MIN_AREAS=4
export GENTS_DEFENDING_MAX_AREAS=10
export GENTS_DEFENDING_MAX_CONCURRENT=8
export GENTS_DEFENDING_CONTEXT_WINDOW=262144
export GENTS_DEFENDING_COMPACTION_THRESHOLD=0.762939453125 # 200,000 tokens
export GENTS_DEFENDING_PROMPT='Prioritize authorization and data-integrity boundaries.'
```

The campaign is deliberately a DAG containing several document-owned DAGs:
the scan barrier writes one assignment document per candidate, a per-document
event trigger creates each isolated verifier request, and each verifier writes
one typed verdict followed by one completion document. A per-group completion
barrier invokes the small triage reducer. Triage does not verify or patch. A
root-cause reducer collapses consequence findings into remediation units;
per-document triggers create contract reviewers, patch authors, validators,
maintainer reviewers, and re-attackers, with group barriers only where a closed
ledger must be joined. No model calls `spawn_subagent`; DefraDB documents and
event triggers own the fan-out, counting, retries, and audit trail.

The runner verifies the closed review-area/result ledger, declared scan counts,
exact
candidate-to-verdict coverage, balanced confirmed/refuted counts, root-cause
membership, contract-to-patch lineage, patch/base/diff-bound validation
receipts, the single final report, stage tool contracts, and signed request
provenance. Results and all four trace projections land under `runs/<job_id>/`.

## Upstream lineage

Prompt structure and workflow principles are adapted from
`anthropics/defending-code-reference-harness` (Apache-2.0): map before scan,
partition by threat-model focus area, keep discovery permissive, make
verification adversarial, derive severity from preconditions, hunt patch
variants, isolate patch review from finder rationale, and treat target-derived
text as untrusted data. The upstream detection-and-response track is a
different workload over telemetry and is intentionally not folded into this
source-review pack.
