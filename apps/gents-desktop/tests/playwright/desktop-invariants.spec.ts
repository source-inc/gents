import {
  adjacentDuplicateTranscriptRows,
  enabledButtonsWithoutAccessibleNames,
  expect,
  gotoHarness,
  openChat,
  primarySurfaceCount,
  test,
  type HarnessScenario,
} from "./desktopTest";

const healthyScenarios: HarnessScenario[] = [
  "default",
  "empty-fleet",
  "loading",
  "long-content",
  "active-turn",
  "cascade-turn",
];

test.describe("desktop UI invariants", () => {
  for (const scenario of healthyScenarios) {
    test(`${scenario} keeps shell invariants`, async ({ page }) => {
      await gotoHarness(page, scenario);

      await expect(page.getByTestId("error-banner")).toHaveCount(0);
      await expect(primarySurfaceCount(page)).resolves.toBe(1);
      await expect(enabledButtonsWithoutAccessibleNames(page)).resolves.toEqual([]);

      if (scenario !== "empty-fleet") {
        await openChat(page);
        await expect(adjacentDuplicateTranscriptRows(page)).resolves.toEqual([]);
      }
    });
  }

  test("sad-path scenarios show handled errors without crashing the shell", async ({
    page,
  }) => {
    await gotoHarness(page, "bridge-unavailable");
    await expect(page.locator(".app-shell")).toBeVisible();
    await expect(page.getByTestId("startup-screen")).toContainText(
      "Desktop native bridge is unavailable",
    );
  });
});
