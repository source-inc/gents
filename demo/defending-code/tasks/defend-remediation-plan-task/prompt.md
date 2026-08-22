Defense run {{ group.correlation_value }} has {{ group.count }} completed
contract reviews (complete={{ group.complete }}):

{{ group.docs }}

The complete immutable contract-review ledger is interpolated above; do not
query it again. Use `read_defense_root_cause_cluster` for the bounded cluster
join. Require an exact cluster-to-review bijection. Sort by `cluster_id`, let N
be the cluster count, and call `write_defense_patch_assignment` exactly N
times. Each assignment uses `assignment_id=<cluster_id>:patch`, the exact
cluster id, primary finding id, member finding ids, frozen `base_revision` and
`base_tree_state`, and identical
`expected_total=N`. Copy the exact contract `review_id` and `disposition` into
`contract_review_id` and `contract_disposition`. Use `status=ready` only for
`disposition=actionable` on a ready cluster, with `skip_reason=none`.
Otherwise use `status=skipped` and a concrete `skip_reason` copied from the
contract evidence or required human decision. Set `repository_id=defending-code`
on every assignment. Do not supply runtime-filled run
or repository_path fields. Never retry successful writes or call subagent tools.

If the cluster-to-review relation is not a bijection, still write exactly one
assignment per interpolated contract row. For every missing or duplicate join,
use the source row's unique `assignment_id=<review_id>:blocked-patch`, preserve
its cluster id and contract fields, use `none` for unavailable
finding/member/base fields, `status=skipped`, and a `skip_reason` naming every
missing or duplicate id. Do not also emit the normal `<cluster_id>:patch` id
for any row participating in a duplicate. N remains the number of interpolated
contract rows, so a corrupt join closes visibly without unique-id collisions
or a stranded barrier.
