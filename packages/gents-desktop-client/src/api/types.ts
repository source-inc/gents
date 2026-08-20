import type { BackendHealth } from "../types/backendHealth.js";
import type { ManagedServerStatus } from "../generated/ManagedServerStatus.js";
import type { ProviderAccountView } from "../generated/ProviderAccountView.js";
import type {
  AgentConfigSaveRequest,
  BackendDeleteRequest,
  BackendSaveRequest,
  BearerPairingRequest,
  BearerPairingResponse,
  BehaviorDeleteRequest,
  BehaviorSaveRequest,
  CascadeCancelPreview,
  ChatSendResult,
  CodexLoginResult,
  GrokLoginResult,
  DesktopClientSnapshot,
  DesktopInterruptRequestRequest,
  DesktopListSubagentTreeRequest,
  DesktopPreviewInterruptCascadeRequest,
  DesktopSessionSnapshot,
  EventTriggerDeleteRequest,
  EventTriggerSaveRequest,
  InferenceProbeResult,
  InferenceProfileDeleteRequest,
  InferenceProfileSaveRequest,
  InitSummary,
  InterruptRequestResult,
  MCPServiceHealthView,
  McpServiceProbeResult,
  NetworkStatusView,
  PeerAddRequest,
  RequestResendResult,
  RequestTimelineView,
  ScheduleDeleteRequest,
  ScheduleRunRequest,
  ScheduleSaveRequest,
  SkillDeleteRequest,
  SkillSaveRequest,
  SubagentTreeView,
  TaskDeleteRequest,
  TaskRunRequest,
  TaskRunResult,
  TaskSaveRequest,
  ToolSelectionDeleteRequest,
  ToolSelectionSaveRequest,
  ToolServiceDeleteRequest,
  ToolServiceSaveRequest,
  ToolServiceTestRequest,
  ToolServiceTestResult,
  ToolSurfaceExplanationView,
  WorkspaceListingView,
} from "../types.js";
import type {
  DesktopOperationsSnapshot,
  DesktopOperationsSnapshotRequest,
  DesktopResolveHoldRequest,
  HeldToolCallView,
  ResolveHoldResult,
} from "../types/operations.js";

export type DesktopApiAdapter = {
  fetchDesktopSnapshot: () => Promise<DesktopClientSnapshot>;
  initLocalStandardRuntime: (request: {
    label: string;
    dangerouslyOverwrite: boolean;
    reset: boolean;
  }) => Promise<InitSummary>;
  startDesktopClient: () => Promise<DesktopClientSnapshot>;
  shutdownDesktopClient: () => Promise<DesktopClientSnapshot>;
  managedServerStatus?: () => Promise<ManagedServerStatus>;
  startManagedServer?: (agentName: string) => Promise<ManagedServerStatus>;
  commitManagedServerAutoStart?: (
    agentName: string,
  ) => Promise<ManagedServerStatus>;
  stopManagedServer?: (
    disableAutoStart: boolean,
  ) => Promise<ManagedServerStatus>;
  setSelectedAgent: (agentDid: string | null) => Promise<void>;
  addPeer: (request: PeerAddRequest) => Promise<DesktopClientSnapshot>;
  pairBearer: (request: BearerPairingRequest) => Promise<BearerPairingResponse>;
  removePeer: (peerId: string) => Promise<DesktopClientSnapshot>;
  renamePeer: (peerId: string, label: string) => Promise<DesktopClientSnapshot>;
  fetchPeerStatus: (peerId: string) => Promise<unknown>;
  probePeerAddress: (serverAddress: string) => Promise<unknown>;
  repairP2P: () => Promise<DesktopClientSnapshot>;
  listWorkspace: (subpath?: string | null) => Promise<WorkspaceListingView>;
  fetchRequestTimeline: (
    agentDid: string,
    requestId: string,
  ) => Promise<RequestTimelineView>;
  explainToolSurface: (
    agentDid: string,
    behaviorId: string,
  ) => Promise<ToolSurfaceExplanationView>;
  fetchNetworkStatus: () => Promise<NetworkStatusView>;
  fetchSessionSnapshot: (
    sessionId: string,
    agentDid?: string | null,
    requestId?: string | null,
  ) => Promise<DesktopSessionSnapshot | null>;
  sendChatMessage: (request: {
    agentDid: string;
    behaviorId?: string | null;
    sessionId?: string | null;
    content: string;
  }) => Promise<ChatSendResult>;
  renameConversation: (request: {
    agentDid: string;
    sessionId: string;
    title: string;
  }) => Promise<void>;
  resendRequest: (requestId: string) => Promise<RequestResendResult>;
  retryRequest: (requestId: string) => Promise<ChatSendResult>;
  saveAgentConfig: (
    request: AgentConfigSaveRequest,
  ) => Promise<DesktopClientSnapshot>;
  saveBehaviorConfig: (
    request: BehaviorSaveRequest,
  ) => Promise<DesktopClientSnapshot>;
  saveSkillConfig: (
    request: SkillSaveRequest,
  ) => Promise<DesktopClientSnapshot>;
  deleteSkillConfig: (
    request: SkillDeleteRequest,
  ) => Promise<DesktopClientSnapshot>;
  deleteTaskConfig: (
    request: TaskDeleteRequest,
  ) => Promise<DesktopClientSnapshot>;
  deleteScheduleConfig: (
    request: ScheduleDeleteRequest,
  ) => Promise<DesktopClientSnapshot>;
  deleteEventTriggerConfig: (
    request: EventTriggerDeleteRequest,
  ) => Promise<DesktopClientSnapshot>;
  deleteBackendConfig: (
    request: BackendDeleteRequest,
  ) => Promise<DesktopClientSnapshot>;
  deleteInferenceProfileConfig: (
    request: InferenceProfileDeleteRequest,
  ) => Promise<DesktopClientSnapshot>;
  deleteToolSelectionConfig: (
    request: ToolSelectionDeleteRequest,
  ) => Promise<DesktopClientSnapshot>;
  deleteToolServiceConfig: (
    request: ToolServiceDeleteRequest,
  ) => Promise<DesktopClientSnapshot>;
  deleteBehaviorConfig: (
    request: BehaviorDeleteRequest,
  ) => Promise<DesktopClientSnapshot>;
  saveBackendConfig: (
    request: BackendSaveRequest,
  ) => Promise<DesktopClientSnapshot>;
  probeInferenceEndpoint: (endpoint: string) => Promise<InferenceProbeResult>;
  codexLogin: (
    agentDid: string,
    provider?: string | null,
  ) => Promise<CodexLoginResult>;
  cancelCodexLogin: () => Promise<void>;
  grokLogin: (
    agentDid: string,
    provider?: string | null,
  ) => Promise<GrokLoginResult>;
  cancelGrokLogin: () => Promise<void>;
  listProviderAccounts?: (agentDid: string) => Promise<ProviderAccountView[]>;
  disconnectProviderAccount?: (
    agentDid: string,
    credentialId: string,
  ) => Promise<void>;
  saveInferenceProfileConfig: (
    request: InferenceProfileSaveRequest,
  ) => Promise<DesktopClientSnapshot>;
  saveToolSelectionConfig: (
    request: ToolSelectionSaveRequest,
  ) => Promise<DesktopClientSnapshot>;
  saveToolServiceConfig: (
    request: ToolServiceSaveRequest,
  ) => Promise<DesktopClientSnapshot>;
  testToolService: (
    request: ToolServiceTestRequest,
  ) => Promise<ToolServiceTestResult>;
  saveTaskConfig: (request: TaskSaveRequest) => Promise<DesktopClientSnapshot>;
  saveScheduleConfig: (
    request: ScheduleSaveRequest,
  ) => Promise<DesktopClientSnapshot>;
  runSchedule: (request: ScheduleRunRequest) => Promise<TaskRunResult>;
  saveEventTriggerConfig: (
    request: EventTriggerSaveRequest,
  ) => Promise<DesktopClientSnapshot>;
  runTask: (request: TaskRunRequest) => Promise<TaskRunResult>;
  listSubagentTree: (
    request: DesktopListSubagentTreeRequest,
  ) => Promise<SubagentTreeView>;
  listBackendsWithHealth: () => Promise<BackendHealth[]>;
  listMcpServicesWithHealth: () => Promise<MCPServiceHealthView[]>;
  probeMcpService: (serviceId: string) => Promise<McpServiceProbeResult>;
  fetchOperationsSnapshot: (
    request: DesktopOperationsSnapshotRequest,
  ) => Promise<DesktopOperationsSnapshot>;
  previewInterruptCascade: (
    request: DesktopPreviewInterruptCascadeRequest,
  ) => Promise<CascadeCancelPreview>;
  interruptRequest: (
    request: DesktopInterruptRequestRequest,
  ) => Promise<InterruptRequestResult>;
  listToolCallHolds: (agentDid: string) => Promise<HeldToolCallView[]>;
  resolveToolCallHold: (
    request: DesktopResolveHoldRequest,
  ) => Promise<ResolveHoldResult>;
};

export type { ManagedServerStatus };
