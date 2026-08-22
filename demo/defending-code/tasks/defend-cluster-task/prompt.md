Defense run {{ event.correlation }} closed triage with
confirmed={{ doc.confirmed_count }}, refuted={{ doc.refuted_count }}, and
promoted={{ doc.promoted_count }}.
Scan ledger status: {{ doc.scan_ledger_status }}
Triage summary: {{ doc.summary }}

Use `read_defending_finding` to load the bounded confirmed-finding ledger.
Require its row count to equal `{{ doc.promoted_count }}`. Partition every row
into exactly one root-cause cluster.
Sort clusters by their lexicographically smallest member finding id. For N
clusters, call `write_defense_root_cause_cluster` N times with:

- `cluster_id={{ event.correlation }}:cluster-<two-digit-index>`
- `base_revision`: the one exact `source_revision` shared by every member;
  never mix revisions or tree states in one cluster—split them if necessary
- `base_tree_state`: the one frozen source-tree state shared by every member
- `status=ready`, one `primary_finding_id`, all `member_finding_ids`, and any
  consequence-only ids in `consequence_finding_ids`; both lists are sorted,
  comma-delimited strings, or `none` when the consequence subset is empty
- a canonical title/root cause, claim kind, maximum supported severity,
  security boundary, affected paths, and precise remediation scope derived
  from the members' structured control source, entry point, sink, guard,
  fail-closed behavior, violated invariant, and impact
- the identical `expected_total=N`

If the finding row count differs from `promoted_count`, do not cluster a
partial ledger. Write one sentinel with
`cluster_id={{ event.correlation }}:blocked-ledger`,
`status=blocked_handoff`, all finding/narrative/base fields `none`, and
`expected_total=1`; this keeps the graph inspectably closed for the report.
This mismatch branch has precedence over the empty-set branch. Only when both
`promoted_count` and the actual finding row count are exactly zero, write one
sentinel with `cluster_id={{ event.correlation }}:no-findings`,
`status=skipped`, `none` for `base_revision`, `base_tree_state`, and all
finding and narrative fields, and `expected_total=1`. Do not supply
runtime-filled run or repository fields. Never retry a successful write.
Verifier rows are independent adjudications and are never deduplicated. This
stage alone may collapse multiple confirmed consequences into one remediation
unit, while retaining every member id exactly once.
