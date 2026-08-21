export type { ChatSendResult } from "../generated/ChatSendResult.js";
export type { CommandDenialView } from "../generated/CommandDenialView.js";
export type { DesktopSessionSnapshot } from "../generated/DesktopSessionSnapshot.js";
export type { GoalView } from "../generated/GoalView.js";
export type { MessageView } from "../generated/MessageView.js";
export type { PendingTurnView } from "../generated/PendingTurnView.js";
export type { RenderedTimelineItem } from "../generated/RenderedTimelineItem.js";
export type { RenderedToolCallView } from "../generated/RenderedToolCallView.js";
export type { ResponseView } from "../generated/ResponseView.js";
export type { ToolCallView } from "../generated/ToolCallView.js";
export type { ToolDiffLineView } from "../generated/ToolDiffLineView.js";
export type { ToolPresentationView } from "../generated/ToolPresentationView.js";
export type { ToolResultView } from "../generated/ToolResultView.js";

export type { RequestResendResultView as RequestResendResult } from "../generated/RequestResendResultView.js";

export type RunTimelineEventView = { kind: string } & Record<string, unknown>;

export type RequestTimelineView = {
  request_id: string;
  session_id?: string | null;
  agent_did?: string | null;
  behavior_id?: string | null;
  child_request_ids?: string[];
  events: RunTimelineEventView[];
};
