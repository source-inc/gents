import {
  expect,
  expectNoPageHorizontalOverflow,
  gotoHarness,
  openAppNavigation,
  openChat,
  openChatNavigation,
  openConfig,
  openConfigTab,
  PEER_ID,
  test,
} from "./desktopTest";

const scenarios = [
  "default",
  "empty-fleet",
  "loading",
  "long-content",
  "operations-rich",
] as const;

test.describe("desktop responsive layout guardrails", () => {
  test("global navigation adapts between a collapsible rail and mobile drawer", async ({
    page,
  }) => {
    await gotoHarness(page);
    const navigation = page.getByTestId("app-navigation");
    const narrow = (page.viewportSize()?.width ?? Number.POSITIVE_INFINITY) <= 760;

    if (narrow) {
      await expect(navigation).toBeHidden();
      await openAppNavigation(page);
      await expect(navigation).toBeVisible();
      await expect(navigation.getByText("Chat", { exact: true })).toBeVisible();
      await page
        .getByRole("button", { name: "Close navigation" })
        .click({ position: { x: 380, y: 400 } });
      await expect(navigation).toBeHidden();
      return;
    }

    await expect(navigation).toBeVisible();
    await expect(navigation.getByText("Chat", { exact: true })).toBeVisible();
    await expect(page.getByTestId("theme-toggle")).toBeVisible();
    const expandedWidth = (await navigation.boundingBox())?.width ?? 0;

    await page.getByTestId("app-navigation-collapse").click();
    await expect(navigation).toHaveClass(/collapsed/);
    await expect
      .poll(async () => (await navigation.boundingBox())?.width ?? expandedWidth)
      .toBeLessThan(expandedWidth);

    await page.getByTestId("app-navigation-collapse").click();
    await expect(navigation).toHaveClass(/expanded/);
  });

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

  test("opened operations drawer stays inside the viewport", async ({ page }) => {
    await gotoHarness(page, "operations-rich");
    await openChat(page);
    await page.getByRole("button", { name: /open operations drawer/i }).click();

    for (const tab of [/Background/, /Lineage/, /Backends/, /MCP health/]) {
      await page.getByRole("tab", { name: tab }).click();
      await expect(
        page.getByRole("complementary", { name: "Operations" }),
      ).toBeVisible();
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
    await openAppNavigation(page);
    const configureButton = page.getByTestId("app-nav-config");
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
