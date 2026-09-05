Grok TUI port run {{ event.correlation }} finished live-review.

passed={{ doc.passed_count }} failed={{ doc.failed_count }} blocked={{ doc.blocked_count }}
expected={{ doc.expected_count }} observed={{ doc.observed_count }} coverage={{ doc.coverage_complete }}
<summary>
{{ doc.summary }}
</summary>

Call `read_grok_port_job`, `read_port_live_report`, and
`read_port_final_review_report`. The sealed diffs for
accepted units are already on the operator checkout. Do not run `make worktree`.
Do not run `git commit` for package work; integrator commits already landed.
Never merge.

Unless `failed_count=0`, `blocked_count=0`, `coverage_complete=true`, and
`expected_count=observed_count`, call `write_port_pull_request` once with
`status=skipped`, empty URL, and stop.

Otherwise:

1. Call `read_grok_port_job` for `pr_base` and `branch`.
2. On the operator checkout, `git checkout -B <branch>` from current HEAD and
   push with upstream tracking.
3. Run repository gates (`cargo fmt --all --check`, `cargo test -p gents`,
   `cargo check --workspace --all-targets`).
   Use `spawn_process` with `tool_name: "bash_unrestricted"` for cargo commands
   that may exceed the foreground bash timeout, then use
   `wait_process`/`read_process` until that exact managed process is terminal.
   Do not use `nohup`, shell `&`, or an untracked log file. A gate passes only
   when the managed process reaches a terminal result with exit code zero.
4. Open one normal, non-draft PR: `gh pr create --base <pr_base> --head <branch>`.
5. Confirm the PR head equals both live `final_review_head` and the final
   review report's `head_sha`; copy its exact review-round and
   confirmed-finding counts into the PR result. The full code-review graph
   already passed before live probes and must not be invalidated here.
6. Wait on required GitHub checks (`gh pr checks`). Do not make a code-changing
   repair after immutable review/live evidence; report `needs_attention` and
   require a fresh workflow run. Never kill by port or broad process-name
   match.

Call `write_port_pull_request` once. Status is `green` only when the
reviewed/live-tested head is the PR head and every required check succeeded.
Do not supply `run_id`.

After the pull-request receipt is durably written, call `update_goal` with
`status="complete"`. Green and needs-attention receipts both complete this
publication stage. Never merge, and never complete the goal before the receipt exists.
