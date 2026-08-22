Maintenance run {{ event.correlation }} is a zero-finding sentinel.
Report status: {{ doc.status }}. Summary: {{ doc.summary }}

Do not provision a workspace, edit files, or open a PR. Call
`defra_query` for `MaintenanceWorkPackage` in this run. Write one
`skipped` `MaintenanceExecutionResult` for the sentinel package with
`commit_sha=none` and `worktree_path=none`, then one skipped
`MaintenanceExecutionSummary`. Do not supply runtime-filled `run_id`.
