Defense run {{ event.correlation }}, skipped patch assignment
`{{ doc.assignment_id }}` for cluster `{{ doc.cluster_id }}`.
Status: `{{ doc.status }}`. Skip reason: {{ doc.skip_reason }}
Contract review/disposition: `{{ doc.contract_review_id }}` /
`{{ doc.contract_disposition }}`.

This assignment is not workspace-bound. Do not inspect or edit source.
Call `write_defense_patch_candidate` exactly once with `status=no_patch`,
`workspace_id=none`, `workspace_requirement=none`, `diff=NONE`,
`diff_sha256=none`, `none` for path/line/category/variant/test/plan fields,
and a rationale that copies the assignment skip reason. Do not supply
runtime-filled `run_id`, `patch_id`, `cluster_id`, `finding_id`,
`member_finding_ids`, `contract_review_id`, `contract_disposition`,
`repository_path`, `base_revision`, `base_tree_state`, or `expected_total`.
