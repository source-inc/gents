Run {{ event.correlation }} has a closed verification ledger: {{ doc.candidate_count }} candidates, {{ doc.confirmed_count }} confirmed, and {{ doc.refuted_count }} refuted.

Verifier summary: {{ doc.summary }}

The verifier already persisted every CodeReviewFindingVerdict and promoted every confirmed row to CodeReviewFinding. Do not write findings. Your job is the operator-facing merge report.

Call `read_finding` once to load the confirmed CodeReviewFinding rows for this run (`run_id` is filled from the correlation). Call `write_triage_report` exactly once. `confirmed_count` and `refuted_count` must match the ledger above. `high_priority_count` is the number of those CodeReviewFinding rows whose `severity` is exactly `Critical` or `Major` (zero if none). The summary should lead with the merge verdict and rank the confirmed defects by severity and practical impact. If there are no confirmed findings, say so and recommend merge unless the verifier summary names a blocking process failure. Do not supply `run_id`: the tools hide it and the runtime fills it from `{{ event.correlation }}`. After the report is durably written, call `update_goal` with `status="complete"`. Never complete the goal before the report exists.
