Maintenance run {{ event.correlation }} has an integrator receipt for
workspace `{{ doc.workspace_id }}` (seal `{{ doc.seal_hash }}`). The
sealed diff is on trunk. Do not treat this request as workspace-bound.

Call `defra_query` for `MaintenanceExecutionSummary`, `MaintenanceExecutionResult`, and `MaintenanceWorkPackage` in this run. Sort package/results by numeric sequence and verify exact package coverage and balanced summary counts. If the sentinel is the only package and the barrier is `skipped`, call `write_maintenance_pull_request` with status `skipped`, empty URL, zero commits/reviews/findings, CI status `not-run`, and stop. If the barrier is not `completed`, record `blocked` with no PR and stop.

The isolated workspace was provisioned by the runtime, sealed after execute, and applied by a typed integrator. Do not run `make worktree` or create a sibling checkout. Package git commits are a typed integrator action, not a worker tool.

For completed executable work:

1. Inspect the applied sealed diff against `pr_base`. Recheck preservation obligations, package boundaries, generated artifacts, dependency changes, tests, formal/conformance ownership, and repository instructions.
2. Run the repository-required local gates. For Gents these include `cargo fmt --all --check`, `cargo test -p gents`, and `cargo check --workspace --all-targets`; run additional package validation from the work ledger. Do not publish while required local gates fail.
3. Push the branch once with upstream tracking and open one normal, non-draft PR using `gh pr create --base <pr_base> --head <branch>`. The body must summarize each commit/work unit, linked issues, preservation safeguards, and exact validation.
4. Run the checked-in repository review harness against the open PR. For Gents, invoke `make review` with the maintenance worktree as `REVIEW_ROOT`, the declared PR base/head, and the new PR number. Require multiple concern lenses and parse its durable `TriageReport`/`Finding` results rather than relying on process exit alone.
5. For each confirmed review finding, make a focused fix commit on the same branch, rerun affected validation, and push. Rerun the review harness after fixes. Bound this at two full review rounds; any confirmed finding remaining after round two makes the result `needs_attention`.
6. Use `gh pr checks` and primary GitHub Actions logs to wait for and diagnose required checks. Make at most three bounded CI repair iterations, each as an additional focused commit followed by relevant local gates and a push. Treat a deterministic or cold-run test timeout as a test defect and repair its budget or implementation; do not hide it with an unexplained rerun. Unrelated infrastructure failures are reported, not worked around or hidden.
7. Re-read the final PR metadata and checks. Never merge it.

Repository gates, the review harness, and CI commonly run for many minutes. Start them through a background-capable shell when needed and poll at intervals no longer than 60 seconds; do not impose an arbitrary short command timeout, and do not classify a still-running process or check as failed. Track every child process by its exact PID and every temporary artifact by its exact run-owned path. Never kill by port or broad process-name match, and never recursively remove a path that was not created by this run. Use a fresh review job ID for every harness attempt.

Call `write_maintenance_pull_request` exactly once with the PR URL, final commit count, review-round count, remaining confirmed-review-finding count, CI status, validation evidence, and a concise summary. Status is `green` only when the final review has zero confirmed findings and every required check is successful. Otherwise use `needs_attention` or `blocked`. Do not supply runtime-filled `run_id`.
