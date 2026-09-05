<host_review_summary>
{{ doc.evidence_summary }}
</host_review_summary>

Dispatch exactly four scanner assignments. Do not reason about the summary and do not list individual files: every scanner receives the complete immutable evidence packet. Your first output must be tool calls, with no text before or after them.

Call `write_review_area` exactly once for each row below. For every call set `expected_total` to `4`, set `baseline` to `{{ doc.base_ref }}..{{ doc.head_ref }}`, omit `repository_path`, and copy the row values literally except for the shown run id.

| `area_id` | `lens` | `path` | `instructions` |
| --- | --- | --- | --- |
| `{{ event.correlation }}:correctness` | `correctness` | `all changed paths` | `Find concrete functional, state-machine, cancellation, error-handling, compatibility, or test defects introduced by the complete diff. Honor the operator focus: {{ doc.focus }}` |
| `{{ event.correlation }}:architecture-reuse` | `architecture-reuse` | `all changed paths` | `Find concrete duplication, abstraction-boundary violations, or incompatible reimplementations introduced by the complete diff. Prefer existing repository abstractions when the evidence proves one exists.` |
| `{{ event.correlation }}:security-concurrency` | `security-concurrency` | `all changed paths` | `Find concrete authorization, filesystem, identity, race, lifecycle, recovery, resource-bound, or unsafe-concurrency defects introduced by the complete diff.` |
| `{{ event.correlation }}:workflow-invariants` | `workflow-invariants` | `all changed paths` | `Find concrete repository-specific workflow, evidence, worktree seal/integration, live-probe, review-gating, or never-merge invariant violations introduced by the complete diff.` |

Emit all four complete calls now, preferably in one parallel batch. Never emit analysis or prose. Never retry a successful write. After all four area rows are durably written, call `update_goal` with `status="complete"`. Never complete the goal while any required row is missing.
