# Repository maintenance pack

This self-contained pack performs a whole-repository, behavior-preserving cleanup round. It follows the code-review pack's durable graph, but scans the current tree and recent history, plans focused work units, and executes them as an ordered commit series rather than stopping at recommendations:

```text
MaintenanceJob -> recon -> N MaintenanceArea scanners
               -> MaintenanceCandidate + MaintenanceScanResult
               -> adversarial verifier -> MaintenanceVerdict + MaintenanceVerificationSummary
               -> commit planning -> MaintenanceFinding + MaintenanceWorkPackage + MaintenanceReport
               -> CallbackBinding provisions one IsolatedWorkspace
               -> one execution owner edits the bound workspace (no git commit)
               -> host seal + typed integrate_workspace from the writer WorkspaceReceipt
               -> integrator WorkspaceReceipt
               -> review + CI repair loop -> one MaintenancePullRequest
```

Recon, scanning, verification, and commit planning are read-only. The runtime provisions one isolated workspace and one branch before execute. Each work package contains one to three verified findings and is one focused edit unit. A single execution owner reads the closed package ledger and implements it in numeric order in the bound placement. Workers do not `make worktree` or `git commit`. Maintenance has no reviewer workspace stage: integrate applies the sealed writer tree, then publish fires from the integrator `WorkspaceReceipt`. DefensePatchAssignment is the spec §11 graph and integrates only after an accepted security review. A terminal agent reviews the applied result, opens one normal GitHub PR, watches required checks, and performs bounded CI repairs. Long local gates and CI waits are polled rather than assigned short wall-clock deadlines. It never merges the PR.

## Stable maintenance categories

The five mandatory categories come from six recurring cleanup waves in this repository between April and July 2026:

1. dead code, dependencies, assets, compatibility paths, and unwired scaffolding;
2. duplicate helpers, pathways, fixtures, tests, and canonical-owner drift;
3. hollow, false-green, flaky, stale, or exactly redundant tests;
4. oversized or mixed-responsibility files that have cohesive extraction seams;
5. narration, stale implementation history, duplicated documentation, and comment/contract drift.

Recon may add narrow repository-specific categories, but cannot replace the mandatory five.

## Run it

```bash
make maintain
make maintain MAINTENANCE_PROMPT='Focus on CLI and runtime ownership seams'
make maintain MAINTENANCE_AREAS=7 MAINTENANCE_HISTORY_DEPTH=400
make maintain MAINTENANCE_KEEP_HOME=1 MAINTENANCE_JOB_ID=cleanup-2026-08
```

`MAINTENANCE_ROOT` defaults to the current repository and is the operator tool ceiling. The runtime places the isolated worktree under that ceiling; execute does not `cd` into a sibling the model created. `MAINTENANCE_HEAD` defaults to `HEAD` and `MAINTENANCE_PR_BASE` to `main`. History identifies prior cleanup patterns and avoids reopening merged work; it does not restrict findings to a diff. Automatic runs use 5-10 areas. The usual provider/profile controls mirror `make review` with a `MAINTENANCE_` prefix.

Every run lands under `demo/repo-maintenance/runs/<job-id>/`. `results.json` contains the report, confirmed findings, commit plan, execution ledger, and terminal PR status. A zero-finding run emits one no-safe-work sentinel, provisions no IsolatedWorkspace, and records skipped execution/PR documents without opening a GitHub PR. `green` means the final review has no confirmed findings and every required GitHub check succeeded; all other terminal states retain exact evidence.

## False-positive policy

Counts are routing signals, not findings. A scanner must prove reachability and ownership before deleting code, and must preserve feature-gated/generated/public/serialization/GraphQL/FFI/reflection/compatibility surfaces, formal and conformance contracts, observability, operator guidance, rationale, safety arguments, and intentionally distinct boundary tests.
