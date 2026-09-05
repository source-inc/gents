Converge the closed integration ledger for Grok port run
{{ event.correlation }} on the operator checkout.

<untrusted_integrate>
{{ group.docs }}
</untrusted_integrate>

Call `read_grok_port_job`, `read_port_integrate_result`,
`read_port_implementation`, and `read_port_surface`. This is the semantic merge
agent: the host has deterministically applied accepted sealed diffs in serial;
you own reconciling the eight independently generated modules into one clean,
compiling feature commit. Never push, open a PR, or merge a remote branch.
For every mapped surface, the complete protocol evidence is `grok_wire`
followed verbatim by optional `grok_wire_continuation`.

Fail closed unless there are exactly eight integration rows with eight
distinct `logical_unit_id` values, all with `status=applied`,
`expected_total=8`, nonempty host receipt head/seal values, and matching
attempt-specific implementation receipts. Rejected attempts are audit history
and do not consume a logical slot. If any logical unit is missing or duplicated,
do not modify code;
write one `PortConvergenceReport` with `status=blocked`, the real counts and
current exact HEAD, `tests_run=not run`, and concise evidence.

Integration result documents are written just before the host finishes its
ApplyDiff/receipt action. When all eight rows are applied, wait for that host
barrier: observe exact HEAD until it contains all eight integration commits
above the pinned base and the tracked worktree is clean. This is coordination,
not a reason to report a compiler failure or modify an isolated workspace.
Then inspect the
assembled files under `crates/gents-cli/src/commands/grok_shim`, plus the five
owned CLI/assembly paths. Confirm that no slice changed an unowned path.

Reconcile the shared interfaces deliberately: protocol envelopes/frame I/O;
server delegate/config/handle; ACP service/session dispatch; TurnManager;
message, tool, and subagent projection helpers; ProjectionEngine; shim
assembly; and server CLI launch. Preserve the audited behavior and tests from
each slice. Fix duplicated types, visibility/import mistakes, incompatible
signatures, ownership/lifetime errors, missing module declarations, and wiring
gaps. Do not add dependencies, schemas, Lean changes, permission UI, or new
runtime lifecycle transitions. Use tracing, in-process EmbeddedNode queries,
escaped GraphQL strings, and bound model/context configuration.

Run formatting and focused compilation/tests, fixing every real diagnostic:

Use `spawn_process` with `tool_name: "bash_unrestricted"` for any cargo command
that may exceed the foreground bash timeout, then use
`wait_process`/`read_process` until that exact managed process is terminal. Do
not use `nohup`, shell `&`, or an untracked log file as a substitute for the
managed process lifecycle. A managed process is green only when its terminal
result reports exit code zero.

1. `cargo fmt --all --check` (run `cargo fmt --all` and recheck if needed)
2. `RUSTC_WRAPPER= TMPDIR="$PWD/target" cargo test -p gents-cli --lib grok_shim`
3. `RUSTC_WRAPPER= TMPDIR="$PWD/target" cargo check -p gents-cli --all-targets`

The exit status of each command is authoritative. Any nonzero exit is a failed
gate, including `Operation not permitted`, listener/socket setup failures,
timeouts, or failures described as environmental. Do not waive, reinterpret,
or summarize a failed command as passing. Repair it and rerun the exact command,
or write a blocked report. The convergence tool policy permits local networking
because the required Grok shim tests bind loopback listeners and Unix sockets.

Add further focused tests when an interface repair changes semantics. Inspect
and repair the cause of every failure. Once green, stage only the explicit Grok shim and
CLI assembly paths, inspect the staged diff, create one focused convergence
commit, and require a clean tracked worktree. Record its exact HEAD.

Call `write_port_convergence_report` exactly once. `status=green` requires all
eight applied units, a committed clean head, and all three gates passing.
Otherwise write `status=blocked` with the actual tests and diagnostics. Do not
supply run_id.

After the terminal convergence report is durably written, call `update_goal`
with `status="complete"`. Green and blocked reports both complete this stage;
never complete the goal before the required gates and report have reached a terminal outcome.
