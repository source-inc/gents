Review run {{ group.correlation_value }} has {{ group.count }} completed lens scans (complete={{ group.complete }}):

{{ group.docs }}

Call `read_candidate_finding` once to load every CodeReviewCandidateFinding for this run. For each candidate, freshly read the exact artifact, its enclosing behavior, and relevant callers/usages. Try to refute it using the actual repository language, dependency APIs, error propagation, tests, and surrounding invariants. Keep the issue attributable to changed lines; reject style-only, speculative, baseline-duplicate, or unrelated pre-existing claims. Distinguish an active-revision pointer from a nonterminal run's immutable pin: a retired successor revision may still need its artifacts for already-pinned work. For cancellation claims, inspect both materialization guards and the durable recovery reconciler; a single client pass missing a race is not a liveness failure when recovery necessarily interrupts it. Treat a deliberately package-specific first vertical CLI adapter as a future product limitation, not a Major correctness defect, unless the changed code claims generic contract support or makes the accepted package behave incorrectly.

Read only the bounded source ranges and callers needed for each candidate. Never
run a whole-file diff for a newly added or large file. If any file, Git, or shell
result reports truncation, do not repeat that command: switch immediately to
bounded file reads around the candidate line and named symbols.

Immediately call `write_finding_verdict` exactly once per candidate with the preserved identity/content fields, fresh evidence, verification reasoning, confidence from 0 through 100, and verdict exactly `confirmed` or `refuted`. Confidence below 80 must be refuted. After a confirmed verdict, call `write_finding` with the same fields and verdict `confirmed`; never write a finding for a refuted candidate. Do not repeat successful reads or writes. If a command is denied or fails, do not retry it with different flags; use file/search tools or decide from existing evidence. Do not treat a denial as evidence for either verdict.

Finally call `write_verification_summary` exactly once with candidate, confirmed, and refuted counts that balance exactly, including the zero-candidate case. Do not supply runtime-filled `run_id`. After every required verdict/finding and the balanced summary are durably written, call `update_goal` with `status="complete"`. Never complete the goal while the ledger is incomplete.
