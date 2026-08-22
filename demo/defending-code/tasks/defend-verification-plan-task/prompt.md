Defense run {{ group.correlation_value }} has {{ group.count }} completed scan
documents (complete={{ group.complete }}):

{{ group.docs }}

The complete immutable scan ledger is interpolated above; do not query it
again. Use `read_defense_candidate` for the bounded candidate-ledger join.
Require one unique scan row per group document and compare each scan's
`finding_count` with actual candidates grouped by `area_id`. Set one shared
`scan_ledger_status`: `consistent` when every count and area matches and every
candidate has one valid classification pair (`vulnerability` with
`HIGH|MEDIUM|LOW`, or `hardening`, `correctness`, `operational`, or
`specification` with `NONE`);
`blocked_provenance` when any scan is blocked; otherwise
`count_mismatch: <concise area/count details>` or
`classification_mismatch: <finding ids and pairs>`. The durable candidate
rows, not model-authored counters, are the authoritative work set.

Let N be the exact number of returned candidates. In stable `finding_id` order call
`write_defense_verification_assignment` exactly N times, once per candidate,
with `assignment_id=<finding_id>:verify`, the candidate's exact `finding_id`
and `area_id`, exact `source_revision` and `source_tree_state`, `status=ready`,
the shared `scan_ledger_status`, and `expected_total=N`.

If N is zero, call the write tool exactly once with
`assignment_id={{ group.correlation_value }}:no-candidates`,
`finding_id=none`, `area_id=none`, `source_revision=none`,
`source_tree_state=none`, `status=skipped`, and `expected_total=1`.
Include the same computed `scan_ledger_status`.
Do not launch or wait for agents. Do not supply runtime-filled `run_id` or
`repository_path`.
