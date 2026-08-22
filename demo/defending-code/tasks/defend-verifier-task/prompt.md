Verify assignment `{{ doc.assignment_id }}` for finding
`{{ doc.finding_id }}` in area `{{ doc.area_id }}`.
Repository: `{{ doc.repository_path }}`
Frozen source revision: `{{ doc.source_revision }}`
Frozen source tree state: `{{ doc.source_tree_state }}`
Status: `{{ doc.status }}`
Scan ledger: `{{ doc.scan_ledger_status }}`
Expected verifier total: `{{ doc.expected_total }}`

The immutable assignment is interpolated above; do not query it again. If
status is `skipped`, do not read source or a candidate and do not write a verdict. Call
`write_defense_verification_completion` exactly once with `status=skipped`.
Do not supply runtime-filled ids, repository path, or `expected_total`.

Otherwise use `read_defense_threat_model` and `read_defense_candidate` as the
bounded context joins. The candidate read is restricted to this run and exact finding.
If either row is missing or its assignment/finding/repository/revision/tree
identity does not match, do not invent a verdict; write the completion once with
`status=blocked_handoff` so triage can close the inconsistency visibly.
Adjudicate only `{{ doc.finding_id }}`, then call `write_defense_verdict`
exactly once. Do not supply runtime-filled `run_id`, `finding_id`, `area_id`,
`repository_path`, `source_revision`, `source_tree_state`, or `expected_total`. After the verdict write
succeeds, call `write_defense_verification_completion` exactly once as the
last write with `status=verified`. Never write completion before the verdict.

`verdict` remains exactly `confirmed` or `refuted`. `confirmed` is allowed only
when every exploitability gate in the system prompt is supported by concrete
source evidence. Set `adjudicated_claim_kind` to exactly `vulnerability`,
`hardening`, `correctness`, `operational`, or `specification`; a confirmed
verdict must use `vulnerability`, while a refutation records the best-supported
classification. Populate `security_boundary`, `attacker_identity`, `attacker_controlled_input`,
`control_source`, `entry_point`, `sink`, adjudicated `attacker_control`,
`default_reachable`, `required_configuration`, `required_privileges`,
`guard_checked`, `fails_closed`, `violated_invariant`, `impact`, and
`contract_surface` even when refuting; use `none` for a gate that fresh
evidence disproves rather than inventing support. Record the resulting
`preconditions`, `access_level`, severity, confidence, fresh `evidence`, and
`verification`. A confirmed vulnerability uses severity `HIGH`, `MEDIUM`, or
`LOW`; every other adjudicated kind and every refutation uses `NONE`. Evidence
must come from the exact clean source named by the
assignment; if the live checkout differs, do not mix revisions and record the
observation in `verification`. Candidate description, location, classification,
recommendation, threat linkage, ownership routing, and root-cause clustering
belong to their existing records and later stages; do not repeat them in the
verdict.

If the frozen revision/tree cannot be reconstructed and verified, write no
verdict. Close the assignment once with
`DefenseVerificationCompletion(status=blocked_provenance)` so source
unavailability is never misreported as a refutation.

Write `verification` as a compact evidence ledger covering `attacker`,
`control`, `entry_to_sink`, `boundary`, `default_reachability`, `guards`,
`impact`, `invariant`, and `counterevidence`, each with concrete source
references. You may use the available read-only file, LSP, shell, and history
capabilities however they best establish or disprove those claims. Set severity
only after accounting for required access, configuration, and existing guards.
