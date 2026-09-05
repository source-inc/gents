Review the converged Grok TUI port for run {{ event.correlation }}.

<untrusted_convergence>
status={{ doc.status }} head={{ doc.head_sha }}
{{ doc.summary }}
</untrusted_convergence>

Call `read_grok_port_job`, `read_port_convergence_report`, and
`read_port_surface`. Count every executable surface (`implement` or
`shaped-stub`) and put that exact number in `implement_surface_count` and, for
a green report, `live_result_count` (use 1 only if the executable count is
zero). A non-green report uses `live_result_count=1` to close the blocked live
sentinel path.

If convergence is skipped or blocked before any executable unit integrates,
`implement_surface_count` is `0`, not the sentinel document count, and
`live_result_count` is `1` solely to close the downstream sentinel path.

If convergence is not green, its head does not equal current exact HEAD, or
the tracked worktree is dirty, do not review or modify source. Write one final
report with `status=blocked`, zero rounds, the actual head and counts, and the
precise mismatch.

Treat the convergence report as non-green evidence if `tests_run` records any
nonzero exit, failed test, permission error, timeout, or required gate that was
not executed, regardless of its declared `status`. Before a green final report,
independently rerun all three convergence gates, including
`RUSTC_WRAPPER= TMPDIR="$PWD/target" cargo test -p gents-cli --lib grok_shim`.
As in convergence, command exit status is authoritative and failures cannot be
waived as environmental.

The bundled graph is installed in the orchestration home. For each full review
round:

1. Resolve exact HEAD and require it to equal the first round's converged head
   (or the focused fix head on round two).
2. Run
   `gents graph run code-review --repo . --base <job.base_sha> --head <head-sha> --home <job.orchestrator_home> --graphql <job.orchestrator_graphql> --output json`
   and capture its run id.
3. Watch that exact run to terminal state and call `gents graph result` with
   the same home/GraphQL endpoint. Inspect durable findings and the
   CodeReviewTriageReport; process exit alone is not review evidence.
4. If round one has confirmed findings, inspect each against current source,
   make focused fixes, run affected tests and `cargo fmt --all --check`, stage
   only explicit fix paths, create focused commits, and start one fresh full
   review against the new exact HEAD.

At most two full review rounds are allowed. Before green, run the repository
foundation gates `cargo test -p gents` and
`cargo check --workspace --all-targets`, require zero confirmed findings and a
clean tracked worktree, and record exact HEAD. Never push, open a PR, or merge.
Use `spawn_process` with `tool_name: "bash_unrestricted"` for any cargo command
that may exceed the foreground bash timeout, then use
`wait_process`/`read_process` until that exact managed process is terminal. Do
not use `nohup`, shell `&`, or untracked gate logs. Require the managed process
terminal result to report exit code zero.

Call `write_port_final_review_report` exactly once. Use `status=green` only
when the durable review and both gates pass; otherwise block with honest round,
finding, and test evidence. Do not supply run_id.

After the terminal final-review report is durably written, call `update_goal`
with `status="complete"`. Green and blocked reports both complete this stage;
never complete the goal before the review and foundation gates reach that recorded outcome.
