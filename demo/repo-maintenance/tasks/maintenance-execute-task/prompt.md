Execute the closed maintenance plan for run {{ event.correlation }} in bound workspace `{{ doc.workspace_id }}` (work unit `{{ doc.work_unit_id }}`).

The runtime already provisioned this isolated workspace and bound it as the file-tool root, shell CWD, and LSP root. Do not run `make worktree`, `git worktree`, or create a sibling path. Do not run `git commit` or `git add`. Edit files in the bound tree. The host seals this workspace after the request, and the integrator applies the sealed diff.

The report declared the ordered work packages. You are the single execution owner.

1. Call `defra_query` for `MaintenanceWorkPackage` in this run. Require exactly the declared unique packages. Parse `sequence` numerically, sort ascending, and require the exact contiguous range 1..N. If the ledger is incomplete, write blocked results and a blocked `MaintenanceExecutionSummary`. Do not edit.
2. Call `defra_query` for `MaintenanceExecutionResult` in this run before editing. If a complete ledger already exists, close it with the summary instead of duplicating work.
3. If the only package has `finding_ids` exactly `none`, write its result as `skipped`, create no workspace edits, then write a skipped summary and stop.
4. Process packages strictly in numeric order. Implement only that package in the bound tree. A package may span code areas, but it must remain a focused, reviewable unit.
5. Run the package validation and every repository gate proportional to the touched boundary. For Gents, use the full `cargo test -p gents` suite and `cargo check --workspace --all-targets` where required; never substitute `--lib`.
6. Call `write_maintenance_execution_result` exactly once for that package before advancing. Use status `completed` only for validated bound-tree edits. Set `commit_sha=none` and `worktree_path=bound-workspace`. Preserve package ID, sequence, and branch. Do not supply runtime-filled `run_id` or `expected_total`.
7. If any package cannot safely complete, write its result as `blocked`, stop executing later packages, and write blocked results for the unattempted suffix.

After every planned package has exactly one terminal result, call `write_maintenance_execution_summary` exactly once. Set status `completed` only when all N executable packages completed. Use `skipped` only for the sentinel. Otherwise use `blocked`. Record `final_commit_sha=none` and `worktree_path=bound-workspace`.
