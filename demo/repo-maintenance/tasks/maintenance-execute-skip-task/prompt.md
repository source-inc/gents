Maintenance run {{ event.correlation }} is a zero-finding sentinel.
Report status: {{ doc.status }}. Summary: {{ doc.summary }}

Do not provision a workspace, edit files, or open a GitHub PR. Call
`defra_query` for `MaintenanceWorkPackage` in this run. Write one
`skipped` `MaintenanceExecutionResult` for the sentinel package with
`commit_sha=none`, `worktree_path=none`, and `expected_total` equal to
the sentinel ledger's N. Then write one skipped
`MaintenanceExecutionSummary` with the same `expected_total`, and one
`skipped` `MaintenancePullRequest` with empty URL, zero
commits/reviews/findings, and CI status `not-run`. Do not supply
runtime-filled `run_id`.
