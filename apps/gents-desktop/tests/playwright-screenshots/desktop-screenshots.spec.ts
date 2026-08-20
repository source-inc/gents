import { writeFile } from "node:fs/promises";

import {
  captureStableScreenshot,
  expect,
  gotoHarness,
  openChat,
  openConfig,
  test,
} from "../playwright/desktopTest";

type ScreenshotReviewEntry = {
  state: string;
  scenario: string;
  attachmentName: string;
};

test.describe("desktop stable screenshot states", () => {
  test("captures core shell states", async ({ page }, testInfo) => {
    const screenshots: ScreenshotReviewEntry[] = [];
    const captureReviewScreenshot = async (
      state: string,
      scenario: string,
      name: string,
    ) => {
      const capture = await captureStableScreenshot(page, testInfo, name);
      screenshots.push({
        state,
        scenario,
        attachmentName: capture.attachmentName,
      });
    };

    await gotoHarness(page);
    await expect(page.getByTestId("fleet-dashboard")).toBeVisible();
    await captureReviewScreenshot(
      "fleet dashboard",
      "default",
      "stable-fleet-dashboard",
    );

    await openChat(page);
    await expect(page.getByTestId("transcript-panel")).toBeVisible();
    await captureReviewScreenshot(
      "chat transcript",
      "default",
      "stable-chat-transcript",
    );

    await gotoHarness(page, "coding");
    await openChat(page);
    await expect(page.getByTestId("tool-intro-bash")).toBeVisible();
    await captureReviewScreenshot("tool timeline", "coding", "stable-tool-timeline");

    await gotoHarness(page);
    await openConfig(page);
    await expect(page.locator(".config-workspace")).toBeVisible();
    await captureReviewScreenshot(
      "config workspace",
      "default",
      "stable-config-workspace",
    );

    await gotoHarness(page, "empty-fleet");
    await expect(page.getByTestId("fleet-empty")).toBeVisible();
    await captureReviewScreenshot("empty fleet", "empty-fleet", "stable-empty-fleet");

    await gotoHarness(page, "bridge-unavailable");
    await expect(page.getByTestId("startup-screen")).toContainText(
      "Desktop native bridge is unavailable",
    );
    await captureReviewScreenshot(
      "bridge error",
      "bridge-unavailable",
      "stable-bridge-error",
    );

    await attachScreenshotReviewManifest(page, testInfo, screenshots);
  });
});

async function attachScreenshotReviewManifest(
  page: Parameters<typeof captureStableScreenshot>[0],
  testInfo: Parameters<typeof captureStableScreenshot>[1],
  screenshots: ScreenshotReviewEntry[],
) {
  const viewport = page.viewportSize();
  const rows = screenshots
    .map(
      (screenshot) =>
        `| ${screenshot.state} | \`${screenshot.scenario}\` | \`${screenshot.attachmentName}\` | \`./${screenshot.attachmentName}\` |`,
    )
    .join("\n");
  const body = [
    "# Desktop Screenshot Review",
    "",
    `Project: \`${testInfo.project.name}\``,
    `Viewport: \`${viewport?.width ?? "unknown"}x${viewport?.height ?? "unknown"}\``,
    "Command: `npm run test:ui:screenshots`",
    "",
    "Use this manifest when reviewing downloaded workflow artifacts or filing UI bugs.",
    "",
    "| State | Harness scenario | Attachment | Artifact path |",
    "| --- | --- | --- | --- |",
    rows,
    "",
    "When filing a confirmed defect, include:",
    "",
    "- expected vs actual",
    "- command and viewport/project from this manifest",
    "- screenshot attachment or artifact path",
    "- labels `bug` and `ui`",
    "",
  ].join("\n");
  const path = testInfo.outputPath("desktop-screenshot-review.md");
  await writeFile(path, body);

  await testInfo.attach("desktop-screenshot-review.md", {
    path,
    contentType: "text/markdown",
  });
}
