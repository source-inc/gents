import {
  expect,
  gotoHarness,
  openConfig,
  openConfigTab,
  PEER_ID,
  saveConfig,
  test,
} from "./desktopTest";

// Exercises async shell scenarios with convergence and recovery assertions so
// regressions in client-side state handling cannot ship unseen.
test.describe("desktop async states", () => {
  test("loading scenario converges from a busy fleet-empty to the dashboard", async ({
    page,
  }) => {
    await gotoHarness(page, "loading");
    await expect(page.getByTestId("fleet-dashboard")).toBeVisible();
    await expect(page.getByTestId(`fleet-row-${PEER_ID}`)).toBeVisible();
    await expect(page.getByTestId("error-banner")).toHaveCount(0);
  });

  test("save-error surfaces the banner, suppresses the Saved chip, and recovers", async ({
    page,
  }) => {
    await gotoHarness(page, "save-error");
    await openConfig(page);
    await openConfigTab(page, "behavior");

    await page
      .getByTestId("behavior-system-prompt")
      .fill("This behavior save is rejected by the harness.");
    await page.getByTestId("behavior-save").click();

    await expect(page.getByTestId("error-banner")).toContainText(
      "Harness rejected behavior save",
    );
    await expect(
      page.locator(".config-editor").getByText("Saved", { exact: true }),
    ).toHaveCount(0);
    const saveButton = page.getByTestId("behavior-save");
    await expect(saveButton).toBeEnabled();
    await expect(saveButton).toHaveText("Save Behavior");

    await page.getByTestId("config-tab-backends").click();
    await expect(page.getByTestId("confirm-dialog")).toBeVisible();
    await expect(page.getByTestId("behavior-system-prompt")).toHaveValue(
      "This behavior save is rejected by the harness.",
    );
    await page.getByTestId("confirm-dialog-confirm").click();
    await expect(page.getByTestId("config-tab-backends")).toHaveClass(/selected/);
    await page.getByTestId("backend-name").fill("OpenAI Harness Recovered");
    await saveConfig(page, "backend-save");
    await expect(page.getByTestId("error-banner")).toHaveCount(0);
  });
});
