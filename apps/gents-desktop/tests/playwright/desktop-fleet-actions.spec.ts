import { expect, gotoHarness, openAppNavigation, PEER_ID, test } from "./desktopTest";

test.describe("fleet deployment navigation", () => {
  test("signed bearer invite is the primary remote pairing flow", async ({ page }) => {
    await gotoHarness(page, "default");
    await page.getByRole("button", { name: "Add Agent", exact: true }).click();

    await page.getByTestId("fleet-pair-label").fill("Amy");
    await page.getByTestId("fleet-pair-token").fill("dabear1-harness-signed-token");
    await page.getByTestId("fleet-pair-submit").click();

    await expect(page.getByTestId(`fleet-row-${PEER_ID}`)).toContainText("Amy");
    await expect(page.getByTestId("fleet-pair-token")).toHaveCount(0);
  });

  test("deployment row opens the chat workspace", async ({ page }) => {
    await gotoHarness(page, "default");
    await expect(page.getByTestId("fleet-dashboard")).toBeVisible();

    await page.getByTestId(`fleet-row-${PEER_ID}`).click();
    if ((page.viewportSize()?.width ?? Number.POSITIVE_INFINITY) <= 760) {
      await page.getByTestId("conversation-session-intro").click();
    }

    await expect(page.getByTestId("composer-input")).toBeVisible();
  });

  test("deployment workspace opens config", async ({ page }) => {
    await gotoHarness(page, "default");
    await expect(page.getByTestId("fleet-dashboard")).toBeVisible();

    await page.getByTestId(`fleet-row-${PEER_ID}`).click();
    await openAppNavigation(page);
    await page.getByTestId("app-nav-config").click();

    await expect(page.locator(".config-workspace")).toBeVisible();
  });

  test("P2P repair is fleet-level and hidden while healthy, never a row action", async ({
    page,
  }) => {
    await gotoHarness(page, "default");
    await expect(page.getByTestId("fleet-dashboard")).toBeVisible();

    await expect(page.getByTestId(`fleet-repair-${PEER_ID}`)).toHaveCount(0);
    await expect(page.getByTestId("fleet-repair-p2p")).toHaveCount(0);
  });
});
