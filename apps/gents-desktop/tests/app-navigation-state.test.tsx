import { act, renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { useAppNavigation } from "../src/hooks/useAppNavigation";

describe("app navigation state", () => {
  it("starts on Fleet and returns through top-level history", () => {
    const { result } = renderHook(() => useAppNavigation());

    expect(result.current.view).toBe("fleet");
    act(() => result.current.navigate("chat"));
    act(() => result.current.navigate("config"));
    expect(result.current.view).toBe("config");

    act(() => result.current.back());
    expect(result.current.view).toBe("chat");
    act(() => result.current.back());
    expect(result.current.view).toBe("fleet");
  });

  it("backs out of a mobile conversation before leaving Chat", () => {
    const { result } = renderHook(() => useAppNavigation());

    act(() => result.current.navigate("chat", { mobileChatPane: "conversation" }));
    expect(result.current.mobileChatPane).toBe("conversation");

    act(() => result.current.back());
    expect(result.current.view).toBe("chat");
    expect(result.current.mobileChatPane).toBe("navigation");

    act(() => result.current.back());
    expect(result.current.view).toBe("fleet");
  });

  it("does not add history when only the Chat pane changes", () => {
    const { result } = renderHook(() => useAppNavigation());

    act(() => result.current.navigate("chat"));
    act(() => result.current.showConversation());
    act(() => result.current.back());
    act(() => result.current.back());
    act(() => result.current.back());

    expect(result.current.view).toBe("fleet");
  });

  it("replaces Code with Chat without creating a back-navigation loop", () => {
    const { result } = renderHook(() => useAppNavigation());

    act(() => result.current.navigate("chat"));
    act(() => result.current.navigate("code"));
    act(() =>
      result.current.navigate("chat", {
        mobileChatPane: "conversation",
        replace: true,
      }),
    );
    act(() => result.current.back());
    act(() => result.current.back());

    expect(result.current.view).toBe("fleet");
  });
});
