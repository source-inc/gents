import { writeFile } from "node:fs/promises";

import {
  expect,
  gotoHarness,
  openChat,
  openConfig,
  test,
  type TestInfo,
} from "../playwright/desktopTest";

type VisualReviewEntry = {
  state: string;
  scenario: string;
  snapshotName: string;
};

test.describe("desktop visual baselines", () => {
  test("matches stable shell states", async ({ page }, testInfo) => {
    const snapshots: VisualReviewEntry[] = [];

    await gotoHarness(page);
    await expect(page.getByTestId("fleet-dashboard")).toBeVisible();
    await expect(page).toHaveScreenshot("fleet-dashboard.png", {
      animations: "disabled",
      fullPage: true,
    });
    snapshots.push({
      state: "fleet dashboard",
      scenario: "default",
      snapshotName: "fleet-dashboard.png",
    });

    await openChat(page);
    await expect(page.getByTestId("transcript-panel")).toBeVisible();
    await expect(page).toHaveScreenshot("chat-transcript.png", {
      animations: "disabled",
      fullPage: true,
    });
    snapshots.push({
      state: "chat transcript",
      scenario: "default",
      snapshotName: "chat-transcript.png",
    });

    await gotoHarness(page, "coding");
    await openChat(page);
    await expect(page.getByTestId("tool-intro-bash")).toBeVisible();
    await expect(page).toHaveScreenshot("tool-timeline.png", {
      animations: "disabled",
      fullPage: true,
    });
    snapshots.push({
      state: "tool timeline",
      scenario: "coding",
      snapshotName: "tool-timeline.png",
    });

    await gotoHarness(page);
    await openConfig(page);
    await expect(page.locator(".config-workspace")).toBeVisible();
    await expect(page).toHaveScreenshot("config-workspace.png", {
      animations: "disabled",
      fullPage: true,
    });
    snapshots.push({
      state: "config workspace",
      scenario: "default",
      snapshotName: "config-workspace.png",
    });

    await gotoHarness(page, "empty-fleet");
    await expect(page.getByTestId("fleet-empty")).toBeVisible();
    await expect(page).toHaveScreenshot("empty-fleet.png", {
      animations: "disabled",
      fullPage: true,
    });
    snapshots.push({
      state: "empty fleet",
      scenario: "empty-fleet",
      snapshotName: "empty-fleet.png",
    });

    await gotoHarness(page, "bridge-unavailable");
    await expect(page.getByTestId("startup-screen")).toContainText(
      "Desktop native bridge is unavailable",
    );
    await expect(page).toHaveScreenshot("bridge-error.png", {
      animations: "disabled",
      fullPage: true,
    });
    snapshots.push({
      state: "bridge error",
      scenario: "bridge-unavailable",
      snapshotName: "bridge-error.png",
    });

    await gotoHarness(page);
    await page.getByTestId("theme-toggle").click();
    await expect(page.locator('html[data-theme="light"]')).toHaveCount(1);
    await expect(page).toHaveScreenshot("fleet-dashboard-light.png", {
      animations: "disabled",
      fullPage: true,
    });
    snapshots.push({
      state: "fleet dashboard (light theme)",
      scenario: "default",
      snapshotName: "fleet-dashboard-light.png",
    });
    await page.evaluate(() => window.localStorage.removeItem("gents-desktop-theme"));

    await attachVisualReviewManifest(testInfo, snapshots);
  });
});

async function attachVisualReviewManifest(
  testInfo: TestInfo,
  snapshots: VisualReviewEntry[],
) {
  const rows = snapshots
    .map(
      (snapshot) =>
        `| ${snapshot.state} | \`${snapshot.scenario}\` | \`${snapshot.snapshotName}\` |`,
    )
    .join("\n");
  const body = [
    "# Desktop Visual Baseline Review",
    "",
    `Project: \`${testInfo.project.name}\``,
    "Command: `npm run test:ui:visual`",
    "",
    "These are golden snapshot assertions for stable desktop shell states.",
    "",
    "| State | Harness scenario | Snapshot |",
    "| --- | --- | --- |",
    rows,
    "",
    "When a visual diff fails, inspect the Playwright visual report and decide",
    "whether the changed pixels are an intended UI update or a confirmed defect.",
    "File confirmed defects with labels `bug` and `ui`.",
    "",
  ].join("\n");
  const path = testInfo.outputPath("desktop-visual-review.md");
  await writeFile(path, body);

  await testInfo.attach("desktop-visual-review.md", {
    path,
    contentType: "text/markdown",
  });
}
