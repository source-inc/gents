Maintenance run {{ event.correlation }} sealed workspace
`{{ doc.workspace_id }}` (seal `{{ doc.seal_hash }}`, work unit
`{{ doc.work_unit_id }}`).

This request is Integrate-bound. Do not run git commit, git add, git merge,
or any shell that mutates trunk. Inspect the sealed tree if needed, then
finish. The host applies the sealed diff to the operator checkout after this
request succeeds.
