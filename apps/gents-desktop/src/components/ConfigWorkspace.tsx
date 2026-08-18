import type {
  AgentConfigSaveRequest,
  BackendSaveRequest,
  BehaviorSaveRequest,
  BootstrapSummary,
  DeploymentView,
  DesktopApiAdapter,
  EventTriggerSaveRequest,
  InferenceProfileSaveRequest,
  ScheduleSaveRequest,
  SkillDeleteRequest,
  SkillSaveRequest,
  TaskRunResult,
  TaskSaveRequest,
  ToolSelectionSaveRequest,
  ToolServiceSaveRequest,
  ToolServiceTestRequest,
  ToolServiceTestResult,
  TaskDeleteRequest,
  ScheduleDeleteRequest,
  EventTriggerDeleteRequest,
  BackendDeleteRequest,
  InferenceProfileDeleteRequest,
  ToolSelectionDeleteRequest,
  ToolServiceDeleteRequest,
  BehaviorDeleteRequest,
} from "@source-inc/gents-desktop-client";
import { NEW_DOCUMENT_ID, TABS } from "./config-workspace/model";
import { useConfigWorkspaceSelection } from "./config-workspace/useConfigWorkspaceSelection";
import {
  AgentConfigPanel,
  BackendConfigPanel,
  BehaviorConfigPanel,
  EventTriggerConfigPanel,
  InferenceProfileConfigPanel,
  ProviderAccountsPanel,
  ScheduleConfigPanel,
  SkillConfigPanel,
  TaskConfigPanel,
  ToolSelectionConfigPanel,
  ToolServiceConfigPanel,
} from "./config";
import { useConfigNavigationGuard } from "./config/ConfigNavigationGuard";

type ConfigWorkspaceProps = {
  api?: DesktopApiAdapter;
  bootstrap: BootstrapSummary | null;
  selectedDeployment: DeploymentView | null;
  selectedBehaviorId: string | null;
  saving: boolean;
  runningTask: boolean;
  onBack: () => void;
  onSaveAgentConfig: (request: AgentConfigSaveRequest) => Promise<unknown>;
  onSaveBackendConfig: (request: BackendSaveRequest) => Promise<unknown>;
  onSaveInferenceProfileConfig: (
    request: InferenceProfileSaveRequest,
  ) => Promise<unknown>;
  onSaveToolSelectionConfig: (request: ToolSelectionSaveRequest) => Promise<unknown>;
  onSaveToolServiceConfig: (request: ToolServiceSaveRequest) => Promise<unknown>;
  onTestToolService: (
    request: ToolServiceTestRequest,
  ) => Promise<ToolServiceTestResult>;
  onSaveBehaviorConfig: (request: BehaviorSaveRequest) => Promise<unknown>;
  onDeleteSkillConfig: (request: SkillDeleteRequest) => Promise<unknown>;
  onDeleteTaskConfig: (request: TaskDeleteRequest) => Promise<unknown>;
  onDeleteScheduleConfig: (request: ScheduleDeleteRequest) => Promise<unknown>;
  onDeleteEventTriggerConfig: (request: EventTriggerDeleteRequest) => Promise<unknown>;
  onDeleteBackendConfig: (request: BackendDeleteRequest) => Promise<unknown>;
  onDeleteInferenceProfileConfig: (
    request: InferenceProfileDeleteRequest,
  ) => Promise<unknown>;
  onDeleteToolSelectionConfig: (
    request: ToolSelectionDeleteRequest,
  ) => Promise<unknown>;
  onDeleteToolServiceConfig: (request: ToolServiceDeleteRequest) => Promise<unknown>;
  onDeleteBehaviorConfig: (request: BehaviorDeleteRequest) => Promise<unknown>;
  onSaveSkillConfig: (request: SkillSaveRequest) => Promise<unknown>;
  onSaveTaskConfig: (request: TaskSaveRequest) => Promise<unknown>;
  onSaveScheduleConfig: (request: ScheduleSaveRequest) => Promise<unknown>;
  onRunSchedule: (request: { scheduleId: string }) => Promise<TaskRunResult>;
  onSaveEventTriggerConfig: (request: EventTriggerSaveRequest) => Promise<unknown>;
  onRunTask: (request: { taskId: string; args?: unknown }) => Promise<TaskRunResult>;
};

export function ConfigWorkspace({
  api,
  bootstrap,
  selectedDeployment,
  selectedBehaviorId,
  saving,
  runningTask,
  onBack,
  onSaveAgentConfig,
  onSaveBackendConfig,
  onSaveInferenceProfileConfig,
  onSaveToolSelectionConfig,
  onSaveToolServiceConfig,
  onTestToolService,
  onSaveBehaviorConfig,
  onDeleteSkillConfig,
  onDeleteTaskConfig,
  onDeleteScheduleConfig,
  onDeleteEventTriggerConfig,
  onDeleteBackendConfig,
  onDeleteInferenceProfileConfig,
  onDeleteToolSelectionConfig,
  onDeleteToolServiceConfig,
  onDeleteBehaviorConfig,
  onSaveSkillConfig,
  onSaveTaskConfig,
  onSaveScheduleConfig,
  onRunSchedule,
  onSaveEventTriggerConfig,
  onRunTask,
}: ConfigWorkspaceProps) {
  const { requestNavigation } = useConfigNavigationGuard();
  const {
    activeTab,
    savedStatus,
    selectConfigBehavior,
    selectedBackendId,
    selectedBehavior,
    selectedConfigBehaviorId,
    selectedEventTriggerId,
    selectedProfileId,
    selectedScheduleId,
    selectedSkillId,
    selectedTaskId,
    selectedToolSelectionId,
    selectedToolServiceId,
    setActiveTab,
    setSavedStatus,
    setSelectedBackendId,
    setSelectedConfigBehaviorId,
    setSelectedEventTriggerId,
    setSelectedProfileId,
    setSelectedScheduleId,
    setSelectedSkillId,
    setSelectedTaskId,
    setSelectedToolSelectionId,
    setSelectedToolServiceId,
  } = useConfigWorkspaceSelection(selectedDeployment, selectedBehaviorId);

  if (!selectedDeployment) {
    return (
      <article className="panel centered-panel">
        <p className="eyebrow">Config</p>
        <h2>Select a deployment</h2>
        <button className="ghost-button" onClick={onBack} type="button">
          Back
        </button>
      </article>
    );
  }

  const activeTabConfig = TABS.find((tab) => tab.id === activeTab) ?? TABS[0];
  const activeTabPanelId = `config-tab-panel-${activeTabConfig.id}`;
  const activeTabControlId = `config-tab-control-${activeTabConfig.id}`;

  return (
    <>
      <section className="config-workspace config-workspace-full">
        <header className="config-header">
          <div className="config-brand">
            <div className="config-title-block">
              <p className="eyebrow">Configuration</p>
              <h1>{selectedDeployment.label}</h1>
              <p className="muted mono" title={selectedDeployment.agentDid}>
                {selectedDeployment.agentDid}
              </p>
            </div>
          </div>
          <div className="config-header-actions">
            <span
              aria-hidden="true"
              className={
                selectedDeployment.dialSucceeded
                  ? "config-status-dot green"
                  : "config-status-dot yellow"
              }
              title={
                selectedDeployment.dialSucceeded
                  ? "P2P connected"
                  : "P2P connection saved"
              }
            />
            <span className="chip">
              {selectedDeployment.dialSucceeded ? "connected" : "saved"}
            </span>
            <button
              className="ghost-button"
              data-testid="config-back-tab"
              onClick={() => requestNavigation(onBack)}
              type="button"
            >
              Back
            </button>
          </div>
        </header>

        <nav
          className="config-screen-nav"
          role="tablist"
          aria-label="Configuration"
          onKeyDown={(event) => {
            const keys = ["ArrowLeft", "ArrowRight", "Home", "End"];
            if (!keys.includes(event.key)) {
              return;
            }
            event.preventDefault();
            const currentIndex = TABS.findIndex((tab) => tab.id === activeTab);
            const nextIndex =
              event.key === "Home"
                ? 0
                : event.key === "End"
                  ? TABS.length - 1
                  : (currentIndex +
                      (event.key === "ArrowRight" ? 1 : -1) +
                      TABS.length) %
                    TABS.length;
            const next = TABS[nextIndex];
            if (next.id === activeTab) {
              document.getElementById(`config-tab-control-${next.id}`)?.focus();
              return;
            }
            requestNavigation(() => {
              setActiveTab(next.id);
              setSavedStatus(null);
              document.getElementById(`config-tab-control-${next.id}`)?.focus();
            });
          }}
        >
          {TABS.map((tab) => (
            <button
              aria-controls={`config-tab-panel-${tab.id}`}
              aria-selected={activeTab === tab.id}
              className={activeTab === tab.id ? "tab-button selected" : "tab-button"}
              data-testid={`config-tab-${tab.id}`}
              id={`config-tab-control-${tab.id}`}
              key={tab.id}
              onClick={() => {
                if (tab.id === activeTab) {
                  return;
                }
                requestNavigation(() => {
                  setActiveTab(tab.id);
                  setSavedStatus(null);
                });
              }}
              role="tab"
              tabIndex={activeTab === tab.id ? 0 : -1}
              type="button"
            >
              {tab.label}
            </button>
          ))}
        </nav>

        <section
          aria-labelledby={activeTabControlId}
          className="config-tab-panel"
          id={activeTabPanelId}
          role="tabpanel"
        >
          {activeTab === "agent" ? (
            <AgentConfigPanel
              bootstrap={bootstrap}
              deployment={selectedDeployment}
              savedStatus={savedStatus}
              saving={saving}
              onSaveAgentConfig={onSaveAgentConfig}
              onSavedStatusChange={setSavedStatus}
            />
          ) : null}

          {activeTab === "behavior" ? (
            <BehaviorConfigPanel
              api={api}
              onDeleteBehaviorConfig={onDeleteBehaviorConfig}
              onDeletedBehavior={() => selectConfigBehavior(null)}
              deployment={selectedDeployment}
              savedStatus={savedStatus}
              saving={saving}
              selectedBehavior={
                selectedConfigBehaviorId === NEW_DOCUMENT_ID ? null : selectedBehavior
              }
              onCreateBehavior={() => setSelectedConfigBehaviorId(NEW_DOCUMENT_ID)}
              onCreateBackend={() => {
                requestNavigation(() => {
                  setActiveTab("backends");
                  setSelectedBackendId(NEW_DOCUMENT_ID);
                  setSavedStatus(null);
                });
              }}
              onCreateProfile={() => {
                requestNavigation(() => {
                  setActiveTab("profiles");
                  setSelectedProfileId(NEW_DOCUMENT_ID);
                  setSavedStatus(null);
                });
              }}
              onCreateToolSelection={() => {
                requestNavigation(() => {
                  setActiveTab("toolSelections");
                  setSelectedToolSelectionId(NEW_DOCUMENT_ID);
                  setSavedStatus(null);
                });
              }}
              onSaveAgentConfig={onSaveAgentConfig}
              onSaveBehaviorConfig={onSaveBehaviorConfig}
              onSavedStatusChange={setSavedStatus}
              onSelectBehavior={selectConfigBehavior}
            />
          ) : null}

          {activeTab === "skills" ? (
            <SkillConfigPanel
              deployment={selectedDeployment}
              savedStatus={savedStatus}
              saving={saving}
              selectedSkillId={selectedSkillId}
              onCreateSkill={() => setSelectedSkillId(NEW_DOCUMENT_ID)}
              onDeleteSkillConfig={onDeleteSkillConfig}
              onDeletedSkill={() => setSelectedSkillId(null)}
              onSaveSkillConfig={onSaveSkillConfig}
              onSavedStatusChange={setSavedStatus}
              onSelectSkill={setSelectedSkillId}
            />
          ) : null}

          {activeTab === "backends" ? (
            <BackendConfigPanel
              onDeleteBackendConfig={onDeleteBackendConfig}
              onDeletedBackend={() => setSelectedBackendId(null)}
              deployment={selectedDeployment}
              savedStatus={savedStatus}
              saving={saving}
              selectedBackendId={selectedBackendId}
              onCreateBackend={() => setSelectedBackendId(NEW_DOCUMENT_ID)}
              onSaveBackendConfig={onSaveBackendConfig}
              onSavedStatusChange={setSavedStatus}
              onSelectBackend={setSelectedBackendId}
            />
          ) : null}

          {activeTab === "providerAccounts" ? (
            <ProviderAccountsPanel api={api} deployment={selectedDeployment} />
          ) : null}

          {activeTab === "profiles" ? (
            <InferenceProfileConfigPanel
              onDeleteInferenceProfileConfig={onDeleteInferenceProfileConfig}
              onDeletedProfile={() => setSelectedProfileId(null)}
              deployment={selectedDeployment}
              savedStatus={savedStatus}
              saving={saving}
              selectedProfileId={selectedProfileId}
              onCreateProfile={() => setSelectedProfileId(NEW_DOCUMENT_ID)}
              onSaveInferenceProfileConfig={onSaveInferenceProfileConfig}
              onSavedStatusChange={setSavedStatus}
              onSelectProfile={setSelectedProfileId}
            />
          ) : null}

          {activeTab === "toolSelections" ? (
            <ToolSelectionConfigPanel
              onDeleteToolSelectionConfig={onDeleteToolSelectionConfig}
              onDeletedToolSelection={() => setSelectedToolSelectionId(null)}
              deployment={selectedDeployment}
              savedStatus={savedStatus}
              saving={saving}
              selectedToolSelectionId={selectedToolSelectionId}
              toolCeiling={bootstrap?.initToolCeiling ?? null}
              toolRoot={bootstrap?.initToolRoot ?? null}
              onCreateToolSelection={() => setSelectedToolSelectionId(NEW_DOCUMENT_ID)}
              onSaveToolSelectionConfig={onSaveToolSelectionConfig}
              onSavedStatusChange={setSavedStatus}
              onSelectToolSelection={setSelectedToolSelectionId}
            />
          ) : null}

          {activeTab === "metaTools" ? (
            <ToolServiceConfigPanel
              onDeleteToolServiceConfig={onDeleteToolServiceConfig}
              onDeletedToolService={() => setSelectedToolServiceId(null)}
              deployment={selectedDeployment}
              savedStatus={savedStatus}
              saving={saving}
              selectedToolServiceId={selectedToolServiceId}
              onCreateToolService={() => setSelectedToolServiceId(NEW_DOCUMENT_ID)}
              onSaveToolServiceConfig={onSaveToolServiceConfig}
              onSavedStatusChange={setSavedStatus}
              onSelectToolService={setSelectedToolServiceId}
              onTestToolService={onTestToolService}
            />
          ) : null}

          {activeTab === "tasks" ? (
            <TaskConfigPanel
              deployment={selectedDeployment}
              runningTask={runningTask}
              savedStatus={savedStatus}
              saving={saving}
              selectedBehavior={selectedBehavior}
              selectedTaskId={selectedTaskId}
              onCreateTask={() => setSelectedTaskId(NEW_DOCUMENT_ID)}
              onDeleteTaskConfig={onDeleteTaskConfig}
              onDeletedTask={() => setSelectedTaskId(null)}
              onRunTask={onRunTask}
              onSaveTaskConfig={onSaveTaskConfig}
              onSavedStatusChange={setSavedStatus}
              onSelectTask={setSelectedTaskId}
            />
          ) : null}

          {activeTab === "timerTriggers" ? (
            <ScheduleConfigPanel
              onDeleteScheduleConfig={onDeleteScheduleConfig}
              onDeletedSchedule={() => setSelectedScheduleId(null)}
              deployment={selectedDeployment}
              runningTask={runningTask}
              savedStatus={savedStatus}
              saving={saving}
              selectedScheduleId={selectedScheduleId}
              selectedTaskId={selectedTaskId}
              onCreateSchedule={() => setSelectedScheduleId(NEW_DOCUMENT_ID)}
              onRunSchedule={onRunSchedule}
              onSaveScheduleConfig={onSaveScheduleConfig}
              onSavedStatusChange={setSavedStatus}
              onSelectSchedule={setSelectedScheduleId}
            />
          ) : null}

          {activeTab === "eventTriggers" ? (
            <EventTriggerConfigPanel
              onDeleteEventTriggerConfig={onDeleteEventTriggerConfig}
              onDeletedEventTrigger={() => setSelectedEventTriggerId(null)}
              deployment={selectedDeployment}
              savedStatus={savedStatus}
              saving={saving}
              selectedEventTriggerId={selectedEventTriggerId}
              selectedTaskId={selectedTaskId}
              onCreateEventTrigger={() => setSelectedEventTriggerId(NEW_DOCUMENT_ID)}
              onSaveEventTriggerConfig={onSaveEventTriggerConfig}
              onSavedStatusChange={setSavedStatus}
              onSelectEventTrigger={setSelectedEventTriggerId}
            />
          ) : null}
        </section>
      </section>
    </>
  );
}
