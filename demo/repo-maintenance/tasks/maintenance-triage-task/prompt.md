Run {{ event.correlation }} has a closed verification ledger: {{ doc.candidate_count }} candidates, {{ doc.confirmed_count }} confirmed, and {{ doc.refuted_count }} refuted.

The runtime-owned execution boundary is repository `{{ doc.repository_path }}`, base `{{ doc.base_ref }}`, PR base `{{ doc.pr_base }}`, and branch `{{ doc.suggested_branch }}`. The runtime provisions one isolated workspace before execute; do not tell execute to create a worktree.

Verifier summary: {{ doc.summary }}

Call `defra_query` for `MaintenanceVerdict` in this run and confirm the counts. Promote every exactly `confirmed` row with `write_maintenance_finding`, preserving all fields. Never promote a refuted row.

Then form a small ordered commit slate for one maintenance branch:

- Each `MaintenanceWorkPackage` becomes exactly one commit and groups one to three compatible findings.
- Prefer a short, reviewable series. Split when findings have different preservation or validation boundaries; combine when they form one coherent owner-level cleanup.
- The shared branch is the isolation boundary. A commit package may span categories, directories, or crates when the change remains coherent.
- Order packages so foundational extraction/deletion precedes dependent cleanup. Use contiguous numeric `sequence` strings beginning at `1`, and set `previous_package_id` to `none` for the first and the exact prior package id thereafter.
- Every executable package uses the same runtime-owned repository/base/PR-base/branch boundary shown above. Those fields are filled from the verification-summary trigger; do not supply or reinterpret them.
- Keep semantic behavior changes, feature work, and unrelated fixes out.
- Use comma-separated strings for `finding_ids`, `path`, and `existing_issues`; never pass an empty list.
- Set each `package_id` to `{{ event.correlation }}:<commit-slug>`.
- Merge the findings' preservation and validation obligations without weakening them.

Decide the complete package list before the first write. Every package must carry the same positive `expected_total`, equal to the number of packages. If there are no confirmed findings, write exactly one explicit sentinel package with sequence `1`, previous package `none`, `finding_ids` set to `none`, category `no-safe-work`, priority `low`, expected total `1`, and a rationale explaining that verification found no safe cleanup. Set the report `status=skipped` so no workspace is provisioned. Otherwise set `status=planned` and `repository_id=repo-maintenance`.

Call `write_maintenance_work_package` once per package. Finally call `write_maintenance_report` exactly once. Counts must balance, `work_package_count` must equal successful package writes including a sentinel, and `high_priority_count` counts confirmed findings whose priority is `high`. The summary should describe the intended commit series and explicitly say when only the sentinel exists. Do not supply runtime-filled `run_id`, `work_unit_id`, `base_sha`, or `branch`.
