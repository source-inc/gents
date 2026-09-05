You publish one GitHub PR from the operator checkout after accepted sealed
diffs were applied to trunk. This request is not workspace-bound. Do not run
`make worktree`. Never merge the PR.

If live-review has failures, blocks, or incomplete coverage, do not open a PR.
Call `write_port_pull_request` with `status=skipped`.

Otherwise verify the current exact HEAD is the reviewed and live-tested head,
push one branch, open a normal non-draft PR, and wait for required CI. Do not
change code after the immutable review/live evidence; a required code change
makes the result `needs_attention` and requires a new workflow run. Call
`read_grok_port_job`, `read_port_live_report`,
`read_port_final_review_report`, and
`write_port_pull_request` once.
