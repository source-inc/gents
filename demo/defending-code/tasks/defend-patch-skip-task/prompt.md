Defense run {{ event.correlation }}, skipped patch assignment
`{{ doc.assignment_id }}` for cluster `{{ doc.cluster_id }}`.
Status: `{{ doc.status }}`. Skip reason: {{ doc.skip_reason }}
Contract review/disposition: `{{ doc.contract_review_id }}` /
`{{ doc.contract_disposition }}`.

This assignment is not workspace-bound. Do not inspect or edit source.
Call `write_defense_patch_candidate` exactly once with `status=no_patch`,
`workspace_requirement=none`, `diff=NONE`, `diff_sha256=none`, `none` for
path/line/category/variant/test/plan fields, and a rationale that copies
the assignment skip reason. Omit `workspace_id`. Then write skipped
sentinels so collection counts and the report barrier still close: call
`write_defense_patch_validation` exactly once with `status=skipped`,
`validation_id` derived from the assignment id, `seal_hash=none`, and
`none` for mechanical-receipt fields; call `write_defense_patch_review`
exactly once with `verdict=skipped`, `validation_id=none`,
`seal_hash=none`, and `none` for quality fields; then call
`write_defense_patch_security_review` exactly once with
`security_review_id` derived from the assignment id, `verdict=skipped`,
`validation_id=none`, `seal_hash=none`, `reviewed_diff_sha256=none`,
`receipt_match=none`, and `none` for the remaining exploit-closure fields.
Omit `workspace_id` on every write; do not pass the string `none` as a
workspace id.
Do not supply runtime-filled `run_id`, `patch_id`, `cluster_id`,
`finding_id`, `member_finding_ids`, `contract_review_id`,
`contract_disposition`, `repository_path`, `base_revision`,
`base_tree_state`, `workspace_id`, or `expected_total`.
