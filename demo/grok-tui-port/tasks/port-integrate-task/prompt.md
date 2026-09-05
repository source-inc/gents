Run {{ event.correlation }} accepted sealed workspace
`{{ doc.workspace_id }}` for work unit `{{ doc.work_unit_id }}`
(implementation `{{ doc.implementation_id }}`).

This request is Integrate-bound. The sealed tree has already passed its
ReadOnly review. Do not inspect files or re-review the implementation. Do not
git commit, git add, or mutate trunk. Immediately finish with a short textual
acknowledgement. Do not write an integration result: the host applies the
sealed diff only after this request succeeds, and a separate receipt-triggered
stage records `applied` after that host action has durably succeeded.

Call `update_goal` with `status="complete"` immediately before the short
acknowledgement. This goal covers only the Integrate-bound authorization
request; the separate receipt stage remains responsible for durable application evidence.
