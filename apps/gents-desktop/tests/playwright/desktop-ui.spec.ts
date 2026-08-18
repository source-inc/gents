import {
  adjacentDuplicateTranscriptRows,
  captureStableScreenshot,
  expect,
  gotoHarness,
  openAppNavigation,
  openChat,
  openChatNavigation,
  openConfig,
  openConfigTab,
  PEER_ID,
  saveConfig,
  test,
} from "./desktopTest";

test.describe("desktop UI harness", () => {
  test("fleet dashboard connects a local runtime and opens chat/config", async ({
    page,
  }, testInfo) => {
    await gotoHarness(page);
    await expect(page.getByTestId("fleet-dashboard")).toBeVisible();
    await expect(page.getByTestId(`fleet-row-${PEER_ID}`)).toBeVisible();
    await captureStableScreenshot(page, testInfo, "fleet-dashboard");

    await page.getByRole("button", { name: "Add Agent", exact: true }).click();
    await page.getByTestId("fleet-connect-local").click();
    await expect(page.getByTestId(`fleet-row-${PEER_ID}`)).toBeVisible();
    await page.getByRole("button", { name: "Add Agent", exact: true }).click();

    await openChat(page);
    await expect(page.getByTestId("transcript-panel")).toContainText(
      "desktop UI test agent",
    );
    await captureStableScreenshot(page, testInfo, "chat-with-transcript");

    await openChatNavigation(page);
    await openAppNavigation(page);
    await page.getByTestId("app-nav-config").click();
    await expect(page.locator(".config-workspace")).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Bombadil UI Agent" }),
    ).toBeVisible();
  });

  test("chat sends a message, creates a new conversation, renames, and avoids duplicate adjacent messages", async ({
    page,
  }) => {
    await gotoHarness(page);
    await openChat(page);

    await expect(page.getByTestId("transcript-panel")).toContainText(
      "desktop UI test agent",
    );
    await expect(adjacentDuplicateTranscriptRows(page)).resolves.toEqual([]);

    await openChatNavigation(page);
    await page.getByTestId("sidebar-new-chat-ops").click();
    await expect(
      page.getByRole("heading", { name: "Start a conversation" }),
    ).toBeVisible();
    await page.getByTestId("composer-input").fill("Check the harness fleet status");
    await page.getByTestId("composer-send").click();

    await expect(page.getByTestId("transcript-panel")).toContainText(
      "Check the harness fleet status",
    );
    await expect(page.getByTestId("transcript-panel")).toContainText(
      "Bombadil harness response",
    );
    await expect(adjacentDuplicateTranscriptRows(page)).resolves.toEqual([]);

    await page.getByTestId("conversation-title-edit").click();
    await page.getByTestId("conversation-title-input").fill("manual ops check");
    await page.keyboard.press("Enter");
    await expect(page.getByRole("heading", { name: "manual ops check" })).toBeVisible();
  });

  test("chat drafts stay scoped to their conversation or new-chat context", async ({
    page,
  }) => {
    await gotoHarness(page);
    await openChat(page);

    await page.getByTestId("composer-input").fill("existing conversation draft");
    await openChatNavigation(page);
    await page.getByTestId("sidebar-new-chat-ops").click();
    await expect(page.getByTestId("composer-input")).toHaveValue("");

    await page.getByTestId("composer-input").fill("new ops conversation draft");
    await openChatNavigation(page);
    await page.getByTestId("sidebar-behavior-default").click();
    await page.getByTestId("conversation-session-intro").click();
    await expect(page.getByTestId("composer-input")).toHaveValue(
      "existing conversation draft",
    );

    await openChatNavigation(page);
    await page.getByTestId("sidebar-new-chat-ops").click();
    await expect(page.getByTestId("composer-input")).toHaveValue(
      "new ops conversation draft",
    );
  });

  test("config workspace supports core CRUD and run flows", async ({
    page,
  }, testInfo) => {
    await gotoHarness(page);
    await openConfig(page);
    await captureStableScreenshot(page, testInfo, "config-workspace");

    await openConfigTab(page, "behavior");
    await page
      .getByTestId("behavior-system-prompt")
      .fill("You are a deterministic Playwright-managed behavior.");
    await saveConfig(page, "behavior-save");

    await openConfigTab(page, "backends");
    await page.getByTestId("backend-name").fill("OpenAI Harness Edited");
    await page.getByTestId("backend-endpoint").fill("http://127.0.0.1:9000/v1");
    await page.getByTestId("backend-models").fill("gpt-4.1-mini, gpt-4.1");
    await saveConfig(page, "backend-save");

    await openConfigTab(page, "profiles");
    await page.getByTestId("profile-display-name").fill("Playwright profile");
    await page.getByTestId("profile-context-window").fill("64000");
    await saveConfig(page, "profile-save");

    await openConfigTab(page, "toolSelections");
    await page.getByTestId("tool-selection-display-name").fill("Playwright tools");
    await page.getByTestId("tool-command-allowed-argv-prefixes").fill("rg\ngit status");
    await saveConfig(page, "tool-selection-save");

    await openConfigTab(page, "metaTools");
    await page.getByTestId("tool-service-display-name").fill("Observability MCP");
    await page.getByTestId("tool-service-test").click();
    await expect(page.getByTestId("tool-service-test-result")).toContainText("whoami");
    await saveConfig(page, "tool-service-save");

    await openConfigTab(page, "tasks");
    await page.getByTestId("task-name").fill("Playwright host check");
    await page
      .getByTestId("task-prompt-template")
      .fill("Inspect this test host and summarize health.");
    await saveConfig(page, "task-save");
    await page.getByTestId("task-run").click();
    await expect(page.getByTestId("task-run-status")).toContainText("request-");
    await expect(page.getByTestId("task-run-history")).toContainText("completed");

    await openConfigTab(page, "timerTriggers");
    await page.getByTestId("schedule-interval-secs").fill("120");
    await saveConfig(page, "schedule-save");
    await page.getByTestId("schedule-run").click();
    await expect(page.getByTestId("schedule-run-status")).toContainText("request-");

    await openConfigTab(page, "eventTriggers");
    await page.getByTestId("event-trigger-new").click();
    await page.getByTestId("event-trigger-id").fill("playwright-trigger");
    await saveConfig(page, "event-trigger-save");
  });

  test("operations drawer exposes background, lineage, backend, and MCP health tabs", async ({
    page,
  }, testInfo) => {
    await gotoHarness(page);
    await openChat(page);
    await page.getByRole("button", { name: /open operations drawer/i }).click();
    await expect(page.getByRole("complementary", { name: "Operations" })).toBeVisible();
    await captureStableScreenshot(page, testInfo, "operations-drawer-open");

    await expect(page.getByRole("tab", { name: /Background/ })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    await expect(page.getByText("No backgrounded tools.")).toBeVisible();

    await page.getByRole("tab", { name: /Lineage/ }).click();
    await expect(page.getByRole("tree", { name: "Subagent lineage" })).toBeVisible();

    await page.getByRole("tab", { name: /Backends/ }).click();
    await expect(page.getByRole("heading", { name: "Backend health" })).toBeVisible();
    await expect(page.getByText("OpenAI Harness")).toBeVisible();

    await page.getByRole("tab", { name: /MCP health/ }).click();
    await expect(
      page.getByRole("heading", { name: "MCP services / health" }),
    ).toBeVisible();
    await expect(page.getByText("mcp-observability")).toBeVisible();
  });

  test("interrupt cancellation handles direct and cascade flows", async ({ page }) => {
    await gotoHarness(page, "active-turn");
    await openChat(page);
    await page.getByTestId("cancel-button").click();
    await expect(page.getByTestId("chat-toast")).toContainText("Interrupt requested");

    await gotoHarness(page, "cascade-turn");
    await openChat(page);
    await page.getByTestId("cancel-button").click();
    await expect(
      page.getByRole("dialog", { name: /interrupt parent request/i }),
    ).toBeVisible();
    await expect(page.getByText("Will be interrupted")).toBeVisible();
    await page.getByTestId("cascade-interrupt-confirm").click();
    await expect(page.getByTestId("chat-toast")).toContainText("Interrupt requested");
  });

  test("sad path scenarios surface empty, bridge, save, and backend-health errors", async ({
    page,
  }, testInfo) => {
    await gotoHarness(page, "empty-fleet");
    await expect(page.getByTestId("fleet-empty")).toBeVisible();
    await captureStableScreenshot(page, testInfo, "empty-fleet");

    await gotoHarness(page, "bridge-unavailable");
    await expect(page.getByTestId("startup-screen")).toContainText(
      "Desktop native bridge is unavailable",
    );

    await gotoHarness(page, "save-error");
    await openConfig(page);
    await openConfigTab(page, "behavior");
    await page
      .getByTestId("behavior-system-prompt")
      .fill("This save intentionally fails in the harness.");
    await page.getByTestId("behavior-save").click();
    await expect(page.getByTestId("error-banner")).toContainText(
      "Harness rejected behavior save",
    );

    await page.getByTestId("error-banner-dismiss").click();
    await expect(page.getByTestId("error-banner")).toHaveCount(0);

    await gotoHarness(page, "backend-health-error");
    await openChat(page);
    await page.getByRole("button", { name: /open operations drawer/i }).click();
    await page.getByRole("tab", { name: /Backends/ }).click();
    await expect(page.getByRole("alert")).toContainText(
      "Harness backend health bridge unavailable",
    );
  });

  test("long transcript content remains readable and keeps composer usable", async ({
    page,
  }) => {
    await gotoHarness(page, "long-content");
    await openChat(page);
    await expect(page.getByTestId("transcript-panel")).toContainText(
      "deliberately long transcript row",
    );
    await expect(page.getByTestId("composer-input")).toBeVisible();
    await expect(page.getByTestId("composer-send")).toBeVisible();
  });
});
