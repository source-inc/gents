export type ReviewJobRow = {
  _docID?: string;
  run_id: string;
  focus?: string;
  created_at?: string;
  repository_path?: string;
  base_ref?: string;
  head_ref?: string;
  lens_count?: string;
  lens_min?: string;
  lens_max?: string;
  pr_number?: string;
};

export type ReviewAreaRow = {
  _docID?: string;
  run_id: string;
  area_id: string;
  lens?: string;
  expected_total?: string;
  repository_path?: string;
  path?: string;
  instructions?: string;
  baseline?: string;
};

export type ScanResultRow = {
  _docID?: string;
  run_id: string;
  area_id: string;
  expected_total?: string;
  summary?: string;
};

export type CandidateFindingRow = {
  finding_id: string;
  area_id?: string;
  run_id: string;
};

export type FindingVerdictRow = {
  finding_id: string;
  run_id: string;
  area_id?: string;
  verdict?: string;
  title?: string;
  severity?: string;
  evidence?: string;
  verification?: string;
};

export type VerificationSummaryRow = {
  _docID?: string;
  run_id: string;
  candidate_count?: string;
  confirmed_count?: string;
  refuted_count?: string;
  summary?: string;
};

export type FindingRow = {
  finding_id: string;
  run_id: string;
  title?: string;
  verdict?: string;
  severity?: string;
};

export type TriageReportRow = {
  _docID?: string;
  run_id: string;
  high_priority_count?: string;
  confirmed_count?: string;
  refuted_count?: string;
  summary?: string;
};

export type InferenceCallRow = {
  request_id: string;
  prompt_tokens?: number | null;
  completion_tokens?: number | null;
};

export type AgentRequestRow = {
  request_id: string;
  session_id?: string | null;
  behavior_id?: string | null;
  status?: string | null;
  lifecycle_state?: string | null;
  caused_by_trigger_id?: string | null;
  caused_by_correlation?: string | null;
  caused_by_source_doc_id?: string | null;
  caused_by_parent_request_id?: string | null;
  caused_by_parent_tool_call_id?: string | null;
  subagent_depth?: number | null;
  content?: string | null;
  created_at?: string | null;
};

export type ReviewSnapshot = {
  jobs: ReviewJobRow[];
  areas: ReviewAreaRow[];
  candidates: CandidateFindingRow[];
  scans: ScanResultRow[];
  verdicts: FindingVerdictRow[];
  summaries: VerificationSummaryRow[];
  findings: FindingRow[];
  reports: TriageReportRow[];
  requests: AgentRequestRow[];
  calls: InferenceCallRow[];
};

export type NodeState =
  | "expected"
  | "live"
  | "done"
  | "failed"
  | "waiting-group"
  | "input-required";

export type NodeKind = "job" | "area" | "scan" | "verify" | "verdict" | "triage";

export type GraphNode = {
  id: string;
  kind: NodeKind;
  label: string;
  detail?: string;
  state: NodeState;
  runId: string;
  requestId?: string;
  sessionId?: string;
  sourceDocId?: string;
  badges: string[];
};

export type ReviewGraph = {
  runId: string | null;
  nodes: GraphNode[];
};
