import { useCallback, useRef, useState } from "react";

export type WorkspaceView = "fleet" | "chat" | "code" | "config";
export type MobileChatPane = "navigation" | "conversation";

export type AppLocation = {
  view: WorkspaceView;
  mobileChatPane: MobileChatPane;
};

type NavigateOptions = {
  mobileChatPane?: MobileChatPane;
  replace?: boolean;
};

const INITIAL_LOCATION: AppLocation = {
  view: "fleet",
  mobileChatPane: "navigation",
};

export function useAppNavigation() {
  const [location, setLocation] = useState<AppLocation>(INITIAL_LOCATION);
  const history = useRef<AppLocation[]>([]);

  const navigate = useCallback((view: WorkspaceView, options: NavigateOptions = {}) => {
    setLocation((current) => {
      const next: AppLocation = {
        view,
        mobileChatPane:
          options.mobileChatPane ??
          (view === "code"
            ? "conversation"
            : view === "chat" && current.view === "chat"
              ? current.mobileChatPane
              : "navigation"),
      };

      if (
        current.view === next.view &&
        current.mobileChatPane === next.mobileChatPane
      ) {
        return current;
      }
      if (
        options.replace &&
        history.current[history.current.length - 1]?.view === next.view
      ) {
        history.current.pop();
      }
      if (current.view !== next.view && !options.replace) {
        history.current.push(current);
      }
      return next;
    });
  }, []);

  const showChatNavigation = useCallback(() => {
    setLocation(() => ({
      view: "chat",
      mobileChatPane: "navigation",
    }));
  }, []);

  const showConversation = useCallback(() => {
    setLocation((current) => ({
      view: current.view === "code" ? "code" : "chat",
      mobileChatPane: "conversation",
    }));
  }, []);

  const back = useCallback(() => {
    setLocation((current) => {
      if (current.view === "chat" && current.mobileChatPane === "conversation") {
        return { ...current, mobileChatPane: "navigation" };
      }
      const previous = history.current.pop();
      if (previous) return previous;
      return INITIAL_LOCATION;
    });
  }, []);

  return {
    ...location,
    back,
    navigate,
    showChatNavigation,
    showConversation,
  };
}
