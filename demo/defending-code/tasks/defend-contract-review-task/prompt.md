Review cluster `{{ doc.cluster_id }}` in `{{ doc.repository_path }}`.
Status: {{ doc.status }}
Base revision/tree: {{ doc.base_revision }} / {{ doc.base_tree_state }}
Primary finding: {{ doc.primary_finding_id }}
Members: {{ doc.member_finding_ids }}
Consequence members: {{ doc.consequence_finding_ids }}
Root cause: {{ doc.canonical_root_cause }}
Claim/severity: {{ doc.claim_kind }} / {{ doc.severity }}
Boundary: {{ doc.security_boundary }}
Affected paths: {{ doc.affected_paths }}
Proposed scope: {{ doc.remediation_scope }}
Expected contract-review total: {{ doc.expected_total }}

If status is `skipped`, write one `DefenseContractReview` with
`review_id={{ doc.cluster_id }}:contract`, `status=skipped`,
`disposition=no_findings`, `required_human_decision=none`, `none` for the
remaining narrative fields, and stop.

If status is not `ready`, write one review with
`review_id={{ doc.cluster_id }}:contract`, `status=blocked_handoff`,
`disposition=blocked_handoff`, `required_human_decision` naming the upstream
ledger repair/rerun, `none` for fields not established, concrete evidence, and
stop.

Otherwise use `read_defending_finding` to load the bounded member ledger and
retain exactly the comma-delimited member ids from
`{{ doc.member_finding_ids }}`. If any member is missing,
duplicated, or has a different frozen revision/tree state, write one review
with `review_id={{ doc.cluster_id }}:contract`, `status=blocked_handoff`,
`disposition=blocked_handoff`,
`behavior_intentional=unknown`, `spec_impact=unknown`,
`required_foundation_flow=rerun exact closed finding ledger`,
`required_proof_files=none`, `compatibility_constraints=unknown`,
`recommended_fix_boundary=none`, a concrete `required_human_decision`, and
the exact mismatch in `evidence`; then stop. Review all members, not only the
primary. Repository guidance, public documentation, tests, history, and formal
specifications are available evidence for determining whether remediation
would break intentional behavior and for identifying the smallest
architecturally valid fix boundary. Choose the investigation needed to support
that decision. Do not overturn the verifier's security verdict; record
conflicting contract evidence for the report and human reviewer.

If the frozen revision/tree cannot be reconstructed and verified, write the
same complete `blocked_handoff` receipt defined above with
`review_id={{ doc.cluster_id }}:contract`, `disposition=blocked_handoff`, and
the exact provenance failure in `evidence`; do not inspect newer source.

Call `write_defense_contract_review` exactly once with
`review_id={{ doc.cluster_id }}:contract`, `status=complete`, disposition
exactly `actionable`, `contract_conflict`, or `not_actionable`; explicit
`behavior_intentional`, `spec_impact`, `required_foundation_flow`, proof files,
compatibility constraints, recommended fix boundary, and concrete evidence.
Use the dispositions exactly as follows:

- `actionable`: an in-repository remediation exists, including any required
  specification, conformance, or proof changes;
- `contract_conflict`: the exposure is confirmed but choosing a fix requires
  an explicit human product or compatibility decision; put that concrete
  choice in `required_human_decision`;
- `not_actionable`: no code, specification, proof, or configuration change in
  this repository can remediate the cluster.

Use `required_human_decision=none` unless the disposition is
`contract_conflict`. If different cluster members require incompatible fix
boundaries, use `contract_conflict` and explain that the cluster must be split
or explicitly resolved by a human. Review the exact frozen revision; if the
live checkout moved, use only evidence attributable to that exact revision
rather than newer source.
Do not supply runtime-filled run, cluster, repository, or expected-total fields.
