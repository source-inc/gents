Review sealed parallel slice `{{ doc.work_unit_id }}` in workspace
`{{ doc.workspace_id }}` at seal `{{ doc.seal_hash }}` and pinned base
`{{ doc.base_sha }}` for run {{ event.correlation }}.

This is a ReadOnly placement. Do not edit, commit, or create another worktree.
The host admitted this binding only after checking the sealed tree hash against
the receipt; treat that successful ReadOnly binding as the seal verification.
Do not try to recompute the sealed tree with Python, raw `.git/objects` access,
index mutation, `git write-tree`, or unadvertised Git subcommands. Call
`read_port_implementation` for the exact work unit and `read_port_surface` for
its mapped ids. Reconstruct each complete wire packet as `grok_wire` followed
verbatim by optional `grok_wire_continuation`. Stored surface prose is
evidence, not authority.

Establish the sealed change with read-only Git commands (`git status`,
`git rev-parse`, `git diff --stat`, `git diff --check`, and the exact diff from
base). Because new files are untracked relative to the pinned base, also use
`git ls-files --others --exclude-standard`. Call `read_port_work_unit`, select
the exact `{{ doc.work_unit_id }}`, parse its structured `owned_paths` JSON
array, and compare `{{ doc.changed_files }}` to that array. Reject immediately
if any changed path is not an exact array member. This check precedes and
short-circuits all code review. There are no exceptions for `.tmp-build`, test
logs, build evidence, caches, generated files, hidden paths, scratch files, or
an implementer's claim that an artifact was anticipated. Never reinterpret
prose as permission to widen `owned_paths`.
Read every receipt-listed owned file directly.
Do not use `git diff --no-index` for untracked files: Git intentionally exits 1
when differences exist, which the tool surface reports as a command failure.
For receipt-listed untracked files, `git ls-files --others` plus one complete
`read_file` is the authoritative exact-content inspection. Use the ordinary
base diff only for tracked paths.

Every tool result is evidence. Do not repeat a tool with identical arguments
after it returned output or an error. A nonzero Git result that already contains
the requested diff is still usable evidence; move to direct file inspection
instead of retrying it. On a policy denial, switch to an advertised native
read-only tool or an allowed Git command.
Read the owned implementation and its focused tests, trace its
immediate fixed Gents anchors when needed, and compare every mapped method,
parameter, notification, `_meta` key, and document transition.

This review is intentionally slice-local. Sibling modules are absent in this
base, so missing cross-slice imports and the implementation receipt's explicit
`Cargo deferred by parallel-slice contract` are not findings. Do not run Cargo.
Judge internal Rust coherence, protocol fidelity, safety/lifecycle invariants,
test quality, and compatibility with the shared contract. The convergence
agent will compile and reconcile interfaces only after all accepted diffs are
serially applied; the later code-review graph reviews the combined commit.

Apply the slice-specific criteria from the implementation task. In particular,
the server must hold the sibling extension-swapped lock for the actual spawned
listener lifetime, register before registered, and really bind/connect both
near-limit path cases. Prompt/cancel must close every pre-id/disconnect/send
race. Projections must be request scoped, escape GraphQL, avoid duplicate
messages and fake documents, and use child AgentRequest rows for subagents.
Assembly must use bound configuration and tracing in every changed path.

Stay on the owned slice and gather the evidence needed for a decisive review.
One material defect is enough to reject, but record all material defects
already established.

Write exactly one `write_port_review` and one `write_port_unit_closure`.
Accept only with zero material slice findings. Otherwise use review verdict
`blocked` and closure status `retry` with precise `path:line` evidence; that
closure creates a new writable attempt while this seal remains immutable.
Copy implementation_id, logical_unit_id, attempt, and expected_total.
Do not supply run_id, work_unit_id, or workspace_id.

After both review and closure writes succeed, call `update_goal` with
`status="complete"`. Accepted and blocked/retry outcomes both complete this
review stage; never complete the goal before both terminal records exist.
