import {
  expect,
  expectNoPageHorizontalOverflow,
  gotoHarness,
  openChat,
  openChatNavigation,
  openConfig,
  openConfigTab,
  PEER_ID,
  test,
} from "./desktopTest";

const scenarios = ["default", "empty-fleet", "loading", "long-content"] as const;

test.describe("desktop responsive layout guardrails", () => {
  for (const scenario of scenarios) {
    test(`${scenario} has no page-level horizontal overflow`, async ({ page }) => {
      await gotoHarness(page, scenario);
      if (scenario !== "empty-fleet" && scenario !== "loading") {
        await openChat(page);
      }
      await expectNoPageHorizontalOverflow(page);
    });
  }

  test("config tabs remain reachable without widening the page", async ({ page }) => {
    await gotoHarness(page);
    await openConfig(page);

    for (const tabId of [
      "agent",
      "behavior",
      "backends",
      "profiles",
      "toolSelections",
      "metaTools",
      "tasks",
      "timerTriggers",
      "eventTriggers",
    ]) {
      await openConfigTab(page, tabId);
      await expect(page.locator(".config-editor").first()).toBeVisible();
      await expectNoPageHorizontalOverflow(page);
    }
  });

  test("phone chat uses one full-screen pane at a time", async ({ page }) => {
    test.skip(
      (page.viewportSize()?.width ?? Number.POSITIVE_INFINITY) > 760,
      "mobile viewport guardrail",
    );

    await gotoHarness(page);
    await openChat(page);
    await expect(page.locator(".chat-column")).toBeVisible();
    await expect(page.locator(".sidebar")).toBeHidden();

    await openChatNavigation(page);
    await expect(page.locator(".sidebar")).toBeVisible();
    await expect(page.locator(".chat-column")).toBeHidden();

    await page.getByTestId("conversation-session-intro").click();
    await expect(page.locator(".chat-column")).toBeVisible();
    await expect(page.locator(".sidebar")).toBeHidden();
  });

  test("fleet deployment navigation stays reachable at any width", async ({ page }) => {
    await gotoHarness(page);
    await expect(page.getByTestId("fleet-dashboard")).toBeVisible();
    const deploymentRow = page.getByTestId(`fleet-row-${PEER_ID}`);
    await expect(deploymentRow).toBeVisible();
    await deploymentRow.click();
    await page.getByTestId("agent-actions").click();
    const configureButton = page.getByRole("button", { name: "Configure" });
    await expect(configureButton).toBeVisible();

    await expectNoPageHorizontalOverflow(page);

    await configureButton.click();
    await expect(page.locator(".config-workspace")).toBeVisible();
    await expectNoPageHorizontalOverflow(page);
  });

  test("empty-fleet remote connection submit stays reachable on mobile", async ({
    page,
  }) => {
    test.skip(
      (page.viewportSize()?.width ?? Number.POSITIVE_INFINITY) > 760,
      "mobile viewport guardrail",
    );

    await gotoHarness(page, "empty-fleet");
    await page
      .getByTestId("fleet-remote-disclosure")
      .locator(":scope > summary")
      .click();
    await page.getByText("Advanced manual discovery", { exact: true }).click();
    await page.getByTestId("fleet-add-server-address").fill("http://studio-1:9191");

    const submit = page.getByTestId("fleet-add-submit");
    await expect(submit).toBeAttached();
    await submit.scrollIntoViewIfNeeded();
    await expect(submit).toBeVisible();
    await submit.click({ trial: true });

    const shellScrollTop = await page
      .locator(".app-shell")
      .evaluate((element) => element.scrollTop);
    expect(shellScrollTop).toBeGreaterThan(0);
    await expectNoPageHorizontalOverflow(page);
  });

  test("populated-fleet status discovery stays reachable on mobile", async ({
    page,
  }) => {
    test.skip(
      (page.viewportSize()?.width ?? Number.POSITIVE_INFINITY) > 760,
      "mobile viewport guardrail",
    );

    await gotoHarness(page);
    await page.getByRole("button", { name: "Add Agent", exact: true }).click();
    await page.getByText("Advanced manual discovery", { exact: true }).click();

    const address = page.getByTestId("fleet-add-server-address");
    const fetchStatus = page.getByTestId("fleet-fetch-status");
    await address.scrollIntoViewIfNeeded();
    await expect(address).toBeVisible();
    await address.fill("http://studio-1:9191");

    await fetchStatus.scrollIntoViewIfNeeded();
    await expect(fetchStatus).toBeVisible();
    await fetchStatus.click({ trial: true });

    const dashboardScroll = await page
      .getByTestId("fleet-dashboard")
      .evaluate((element) => ({
        clientHeight: element.clientHeight,
        overflowY: getComputedStyle(element).overflowY,
        scrollHeight: element.scrollHeight,
        scrollTop: element.scrollTop,
      }));
    expect(dashboardScroll.overflowY).toBe("auto");
    expect(dashboardScroll.scrollHeight).toBeGreaterThan(dashboardScroll.clientHeight);
    expect(dashboardScroll.scrollTop).toBeGreaterThan(0);
    await expectNoPageHorizontalOverflow(page);
  });
});
