import { vi } from "vitest";

import type {
  AgentConfigSaveRequest,
  BackendSaveRequest,
  BehaviorSaveRequest,
  BootstrapSummary,
  DeploymentView,
  EventTriggerSaveRequest,
  InferenceProfileSaveRequest,
  ScheduleSaveRequest,
  TaskRunResult,
  TaskSaveRequest,
  ToolSelectionSaveRequest,
  ToolServiceSaveRequest,
  ToolServiceTestRequest,
  ToolServiceTestResult,
} from "@source-inc/gents-desktop-client";

export const runResult: TaskRunResult = {
  requestDocId: "bae-run",
  requestId: "run-1",
  sessionId: "session-1",
  agentDid: "did:key:z6MkAgent",
  behaviorId: "default",
  status: "submitted",
  lifecycleState: "queued",
};

export const deployment: DeploymentView = {
  peerId: "peer-1",
  label: "Local Agent",
  agentDid: "did:key:z6MkAgent",
  addr: "iroh://local",
  source: "local",
  graphql: null,
  dialSucceeded: true,
  pairingReady: true,
  defaultBehaviorId: "default",
  agentPrincipal: {
    agentDid: "did:key:z6MkAgent",
    displayName: "Local Agent",
    defaultBehaviorId: "default",
    enabled: true,
  },
  runtime: null,
  behaviors: [
    {
      behaviorId: "default",
      displayName: "Default",
      systemPrompt: "You are the default behavior.",
      backendId: "backend-a",
      inferenceProfileId: "profile-a",
      toolSelectionId: "tools-a",
      enabled: true,
      isDefault: true,
    },
    {
      behaviorId: "ops",
      displayName: "Ops",
      systemPrompt: "You are the ops behavior.",
      backendId: "backend-b",
      inferenceProfileId: "profile-b",
      toolSelectionId: "tools-b",
      enabled: true,
      isDefault: false,
    },
  ],
  behaviorEnvironments: [
    {
      behaviorId: "default",
      displayName: "Default",
      enabled: true,
      isDefault: true,
      modelName: "model-a",
      inferenceProfileName: "Profile A",
      workspaceRoot: "/tmp/work",
      fileAccess: "read-only",
      bashAccess: "read-only",
      networkAccess: null,
      skillNames: [],
      sessionCount: 0,
      activeSessionCount: 0,
    },
    {
      behaviorId: "ops",
      displayName: "Ops",
      enabled: true,
      isDefault: false,
      modelName: "model-b",
      inferenceProfileName: "Profile B",
      workspaceRoot: null,
      fileAccess: "off",
      bashAccess: "off",
      networkAccess: null,
      skillNames: [],
      sessionCount: 0,
      activeSessionCount: 0,
    },
  ],
  inferenceBackends: [
    {
      backendId: "backend-a",
      name: "Backend A",
      providerKind: "openai",
      endpoint: "http://127.0.0.1:8000/v1",
      apiKeyConfigured: false,
      enabled: true,
      models: ["model-a"],
    },
    {
      backendId: "backend-b",
      name: "Backend B",
      providerKind: "openai",
      endpoint: "http://127.0.0.1:9000/v1",
      apiKeyConfigured: false,
      enabled: true,
      models: ["model-b"],
    },
  ],
  inferenceProfiles: [
    {
      profileId: "profile-a",
      displayName: "Profile A",
      contextWindow: 131072,
    },
    {
      profileId: "profile-b",
      displayName: "Profile B",
      contextWindow: 65536,
    },
  ],
  toolSelections: [
    {
      selectionId: "tools-a",
      agentDid: "did:key:z6MkAgent",
      displayName: "Tools A",
      enableFileTools: true,
      fileToolsMode: "ReadOnly",
      enableBash: true,
      bashMode: "ReadOnly",
      cliToolNames: ["grep"],
      enableMetaTools: true,
      allowedMcpServiceIds: ["service-a"],
      delegateTo: [],
    },
    {
      selectionId: "tools-b",
      agentDid: "did:key:z6MkAgent",
      displayName: "Tools B",
      enableFileTools: false,
      fileToolsMode: "ReadOnly",
      enableBash: false,
      bashMode: "ReadOnly",
      cliToolNames: [],
      enableMetaTools: false,
      allowedMcpServiceIds: [],
      delegateTo: [],
    },
  ],
  toolServiceRegistries: [
    {
      serviceId: "service-a",
      displayName: "Service A",
      hostname: "localhost",
      mcpPort: 7331,
      mcpPath: "/mcp",
      status: "online",
    },
  ],
  tasks: [
    {
      taskId: "task-a",
      name: "Task A",
      behaviorId: "default",
      promptTemplate: "Run task A",
      enabled: true,
      recentRuns: {
        totalFires: 0,
        scheduleCount: 1,
        eventTriggerCount: 1,
      },
      runHistory: [],
    },
    {
      taskId: "task-b",
      name: "Task B",
      behaviorId: "ops",
      promptTemplate: "Run task B",
      enabled: true,
      recentRuns: {
        totalFires: 0,
        scheduleCount: 0,
        eventTriggerCount: 0,
      },
      runHistory: [],
    },
  ],
  schedules: [
    {
      scheduleId: "timer-a",
      taskId: "task-a",
      intervalSecs: 60,
      enabled: true,
      concurrency: "serial",
      fireCount: 0,
    },
  ],
  eventTriggers: [
    {
      triggerId: "event-a",
      taskId: "task-a",
      sourceCollection: "AgentRequest",
      eventKind: "created",
      enabled: true,
      concurrency: "serial",
      fireCount: 0,
    },
  ],
  conversations: [],
};

export const bootstrap: BootstrapSummary = {
  defaultAgentHome: "/tmp/agent",
  initAgentName: "Local Agent",
  initAgentDid: "did:key:z6MkAgent",
  initToolCeiling: "Readwrite",
  initToolRoot: "/tmp/work",
  desktopHome: "/tmp/gents",
  peerDirectoryPath: "/tmp/gents/peers.json",
  nodeDataDir: "/tmp/gents/node",
  logFilePath: "/tmp/gents/logs/desktop.log",
  agentHomeExists: true,
  desktopHomeExists: true,
  peerDirectoryExists: true,
  savedPeers: [],
};

export function workspaceHandlers() {
  return {
    onBack: vi.fn(),
    onSaveAgentConfig: vi.fn<[(request: AgentConfigSaveRequest) => Promise<unknown>]>(
      () => Promise.resolve(),
    ),
    onSaveBackendConfig: vi.fn<[(request: BackendSaveRequest) => Promise<unknown>]>(
      () => Promise.resolve(),
    ),
    onSaveInferenceProfileConfig: vi.fn<
      [(request: InferenceProfileSaveRequest) => Promise<unknown>]
    >(() => Promise.resolve()),
    onSaveToolSelectionConfig: vi.fn<
      [(request: ToolSelectionSaveRequest) => Promise<unknown>]
    >(() => Promise.resolve()),
    onSaveToolServiceConfig: vi.fn<
      [(request: ToolServiceSaveRequest) => Promise<unknown>]
    >(() => Promise.resolve()),
    onTestToolService: vi.fn<
      [(request: ToolServiceTestRequest) => Promise<ToolServiceTestResult>]
    >(() =>
      Promise.resolve({
        serviceId: "service-a",
        endpoint: "http://localhost:7331/mcp",
        status: "ok",
        toolCount: 0,
        tools: [],
      }),
    ),
    onSaveBehaviorConfig: vi.fn<[(request: BehaviorSaveRequest) => Promise<unknown>]>(
      () => Promise.resolve(),
    ),
    onSaveTaskConfig: vi.fn<[(request: TaskSaveRequest) => Promise<unknown>]>(() =>
      Promise.resolve(),
    ),
    onSaveScheduleConfig: vi.fn<[(request: ScheduleSaveRequest) => Promise<unknown>]>(
      () => Promise.resolve(),
    ),
    onRunSchedule: vi.fn<[(request: { scheduleId: string }) => Promise<TaskRunResult>]>(
      () => Promise.resolve(runResult),
    ),
    onSaveEventTriggerConfig: vi.fn<
      [(request: EventTriggerSaveRequest) => Promise<unknown>]
    >(() => Promise.resolve()),
    onRunTask: vi.fn<
      [(request: { taskId: string; args?: unknown }) => Promise<TaskRunResult>]
    >(() => Promise.resolve(runResult)),
  };
}
