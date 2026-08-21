import type { AgentRequestRow, InferenceCallRow } from "../graph/types.ts";

export type DefenseJobRow = {
  _docID?: string;
  run_id: string;
  repository_path?: string;
  focus?: string;
  area_min?: string;
  area_max?: string;
  engagement_context?: string;
};

export type ThreatModelRow = {
  _docID?: string;
  run_id: string;
  repository_path?: string;
  focus?: string;
  area_min?: string;
  area_max?: string;
  source_revision?: string;
  source_tree_state?: string;
  provenance_status?: string;
  system_context?: string;
  assets?: string;
  entry_points?: string;
  threats?: string;
  deprioritized?: string;
  open_questions?: string;
  mitigations?: string;
  provenance?: string;
};

export type DefenseAreaRow = {
  _docID?: string;
  run_id: string;
  area_id: string;
  repository_path?: string;
  source_revision?: string;
  source_tree_state?: string;
  status?: string;
  focus?: string;
  threat_ids?: string;
  threat_context?: string;
  trust_boundary?: string;
  reachable_assets?: string;
  instructions?: string;
  expected_total?: string;
};

export type DefenseScanRow = {
  _docID?: string;
  run_id: string;
  area_id: string;
  repository_path?: string;
  status?: string;
  expected_total?: string;
  finding_count?: string;
  coverage?: string;
  summary?: string;
};

export type DefenseCandidateRow = {
  _docID?: string;
  run_id: string;
  finding_id: string;
  area_id?: string;
  source_revision?: string;
  source_tree_state?: string;
  claim_kind?: string;
  root_cause_key?: string;
  security_boundary?: string;
  attacker_identity?: string;
  attacker_controlled_input?: string;
  control_source?: string;
  entry_point?: string;
  sink?: string;
  default_reachable?: string;
  required_configuration?: string;
  required_privileges?: string;
  guard_checked?: string;
  fails_closed?: string;
  violated_invariant?: string;
  category?: string;
  claimed_severity?: string;
  confidence?: string;
  path?: string;
  line?: string;
  title?: string;
  description?: string;
  exploit_scenario?: string;
  recommendation?: string;
  evidence?: string;
  threat_ids?: string;
};

export type DefenseVerdictRow = {
  _docID?: string;
  run_id: string;
  finding_id: string;
  area_id?: string;
  verdict?: string;
  source_revision?: string;
  source_tree_state?: string;
  claim_kind?: string;
  root_cause_key?: string;
  security_boundary?: string;
  attacker_identity?: string;
  attacker_controlled_input?: string;
  control_source?: string;
  entry_point?: string;
  sink?: string;
  attacker_control?: string;
  default_reachable?: string;
  required_configuration?: string;
  required_privileges?: string;
  guard_checked?: string;
  fails_closed?: string;
  violated_invariant?: string;
  impact?: string;
  contract_surface?: string;
  category?: string;
  severity?: string;
  confidence?: string;
  path?: string;
  line?: string;
  title?: string;
  description?: string;
  exploit_scenario?: string;
  recommendation?: string;
  evidence?: string;
  verification?: string;
  duplicate_of?: string;
  preconditions?: string;
  access_level?: string;
  owner_hint?: string;
  threat_ids?: string;
};

export type VerificationAssignmentRow = {
  _docID?: string;
  run_id: string;
  assignment_id: string;
  finding_id: string;
  area_id?: string;
  repository_path?: string;
  status?: string;
  scan_ledger_status?: string;
  expected_total?: string;
};

export type VerificationCompletionRow = {
  _docID?: string;
  run_id: string;
  assignment_id: string;
  finding_id: string;
  repository_path?: string;
  status?: string;
  scan_ledger_status?: string;
  expected_total?: string;
};

export type DefendingFindingRow = DefenseVerdictRow & {
  category?: string;
  path?: string;
  line?: string;
  description?: string;
  exploit_scenario?: string;
  recommendation?: string;
  evidence?: string;
  threat_ids?: string;
};

export type TriageSummaryRow = {
  _docID?: string;
  run_id: string;
  scan_ledger_status?: string;
  candidate_count?: string;
  confirmed_count?: string;
  refuted_count?: string;
  duplicate_count?: string;
  promoted_count?: string;
  summary?: string;
};

export type PatchAssignmentRow = {
  _docID?: string;
  run_id: string;
  assignment_id: string;
  cluster_id?: string;
  finding_id?: string;
  member_finding_ids?: string;
  contract_review_id?: string;
  contract_disposition?: string;
  skip_reason?: string;
  repository_path?: string;
  base_revision?: string;
  base_tree_state?: string;
  status?: string;
  expected_total?: string;
};

export type PatchCandidateRow = {
  _docID?: string;
  run_id: string;
  patch_id: string;
  cluster_id?: string;
  finding_id?: string;
  member_finding_ids?: string;
  contract_review_id?: string;
  contract_disposition?: string;
  status?: string;
  repository_path?: string;
  base_revision?: string;
  base_tree_state?: string;
  workspace_requirement?: string;
  path?: string;
  line?: string;
  category?: string;
  diff?: string;
  diff_sha256?: string;
  rationale?: string;
  variants_checked?: string;
  bypass_considered?: string;
  test_note?: string;
  validation_plan?: string;
  expected_total?: string;
};

export type PatchReviewRow = {
  _docID?: string;
  run_id: string;
  patch_id: string;
  cluster_id?: string;
  finding_id?: string;
  validation_id?: string;
  reviewed_base_revision?: string;
  reviewed_base_tree_state?: string;
  reviewed_diff_sha256?: string;
  receipt_match?: string;
  verdict?: string;
  quality_status?: string;
  out_of_scope_hunks?: string;
  new_surface?: string;
  reason?: string;
  expected_total?: string;
};

export type RootCauseClusterRow = {
  _docID?: string;
  run_id: string;
  cluster_id: string;
  repository_path?: string;
  base_revision?: string;
  base_tree_state?: string;
  status?: string;
  primary_finding_id?: string;
  member_finding_ids?: string;
  consequence_finding_ids?: string;
  canonical_title?: string;
  canonical_root_cause?: string;
  claim_kind?: string;
  severity?: string;
  security_boundary?: string;
  affected_paths?: string;
  remediation_scope?: string;
  expected_total?: string;
};

export type ContractReviewRow = {
  _docID?: string;
  run_id: string;
  review_id: string;
  cluster_id: string;
  status?: string;
  disposition?: string;
  spec_impact?: string;
  required_foundation_flow?: string;
  required_proof_files?: string;
  compatibility_constraints?: string;
  recommended_fix_boundary?: string;
  required_human_decision?: string;
  evidence?: string;
  expected_total?: string;
};

export type PatchValidationRow = {
  _docID?: string;
  run_id: string;
  validation_id: string;
  patch_id: string;
  cluster_id?: string;
  finding_id?: string;
  status?: string;
  validated_base_revision?: string;
  base_tree_state?: string;
  validated_diff_sha256?: string;
  observed_head_revision?: string;
  result_tree_hash?: string;
  workspace_mode?: string;
  workspace_identity?: string;
  changed_files?: string;
  provenance_match?: string;
  applies_cleanly?: string;
  format_status?: string;
  compile_status?: string;
  test_status?: string;
  proof_status?: string;
  commands?: string;
  evidence?: string;
  expected_total?: string;
};

export type PatchSecurityReviewRow = {
  _docID?: string;
  run_id: string;
  security_review_id: string;
  patch_id: string;
  cluster_id?: string;
  finding_id?: string;
  validation_id?: string;
  reviewed_base_revision?: string;
  reviewed_base_tree_state?: string;
  reviewed_diff_sha256?: string;
  receipt_match?: string;
  verdict?: string;
  original_path_closed?: string;
  sibling_variants_checked?: string;
  bypass_found?: string;
  contract_alignment?: string;
  evidence?: string;
  expected_total?: string;
};

export type DefenseReportRow = {
  _docID?: string;
  run_id: string;
  audit_status?: string;
  candidate_count?: string;
  confirmed_count?: string;
  refuted_count?: string;
  root_cause_count?: string;
  actionable_cluster_count?: string;
  patch_count?: string;
  mechanically_valid_patch_count?: string;
  maintainer_accepted_patch_count?: string;
  security_accepted_patch_count?: string;
  accepted_patch_count?: string;
  rejected_patch_count?: string;
  severity_counts?: string;
  top_risks?: string;
  summary?: string;
  human_actions?: string;
};

export type DefenseSnapshot = {
  contractPipelineAvailable: boolean;
  jobs: DefenseJobRow[];
  threats: ThreatModelRow[];
  areas: DefenseAreaRow[];
  scans: DefenseScanRow[];
  candidates: DefenseCandidateRow[];
  verificationAssignments: VerificationAssignmentRow[];
  verificationCompletions: VerificationCompletionRow[];
  verdicts: DefenseVerdictRow[];
  findings: DefendingFindingRow[];
  triage: TriageSummaryRow[];
  clusters: RootCauseClusterRow[];
  contractReviews: ContractReviewRow[];
  assignments: PatchAssignmentRow[];
  patches: PatchCandidateRow[];
  validations: PatchValidationRow[];
  reviews: PatchReviewRow[];
  securityReviews: PatchSecurityReviewRow[];
  reports: DefenseReportRow[];
  requests: AgentRequestRow[];
  calls: InferenceCallRow[];
};

export type DefenseNodeState =
  "expected" | "live" | "done" | "failed" | "waiting-group" | "input-required";

export type DefenseNodeKind =
  | "job"
  | "threat"
  | "plan"
  | "area"
  | "scan"
  | "triage"
  | "candidate"
  | "verification-plan"
  | "verification-assignment"
  | "verifier"
  | "verdict"
  | "cluster-plan"
  | "cluster"
  | "contract-review"
  | "remediation-plan"
  | "assignment"
  | "patch"
  | "validation"
  | "review"
  | "security-review"
  | "report";

export type DefenseNode = {
  id: string;
  kind: DefenseNodeKind;
  label: string;
  detail?: string;
  state: DefenseNodeState;
  runId: string;
  requestId?: string;
  sessionId?: string;
  sourceDocId?: string;
  badges: string[];
};

export type DefenseGraph = {
  runId: string | null;
  nodes: DefenseNode[];
};
