import { expect, test as base, type Page, type TestInfo } from "@playwright/test";

export type HarnessScenario =
  | "default"
  | "empty-fleet"
  | "loading"
  | "bridge-unavailable"
  | "save-error"
  | "backend-health-error"
  | "long-content"
  | "active-turn"
  | "cascade-turn"
  | "coding";

export const PEER_ID = "peer-bombadil-local";

type DesktopFixtures = {
  browserLogs: string[];
};

export const test = base.extend<DesktopFixtures>({
  browserLogs: [
    async ({ page }, use, testInfo) => {
      const logs: string[] = [];
      page.on("console", (message) => {
        logs.push(`[console:${message.type()}] ${message.text()}`);
      });
      page.on("pageerror", (error) => {
        logs.push(`[pageerror] ${error.stack ?? error.message}`);
      });

      await use(logs);

      const unexpected = logs.filter(
        (line) => line.startsWith("[pageerror]") || line.startsWith("[console:error]"),
      );
      if (testInfo.status !== testInfo.expectedStatus || unexpected.length > 0) {
        await testInfo.attach("browser-console.log", {
          body: logs.join("\n") || "(no browser console output)",
          contentType: "text/plain",
        });
      }
      expect(unexpected).toEqual([]);
    },
    { auto: true },
  ],
});

export { expect };
export type { Page, TestInfo };

export async function gotoHarness(page: Page, scenario: HarnessScenario = "default") {
  await page.goto(`/tests/ui-harness/harness.html?scenario=${scenario}`);
  await expect(page.locator(".app-shell")).toBeVisible();
  await expect(
    page
      .locator(
        [
          '[data-testid="fleet-dashboard"]',
          '[data-testid="fleet-empty"]',
          '[data-testid="transcript-panel"]',
          ".config-workspace",
          '[data-testid="error-banner"]',
          '[data-testid="startup-screen"]',
        ].join(", "),
      )
      .first(),
  ).toBeVisible();
}

export async function gotoLiveHarness(page: Page, bridgeUrl?: string) {
  const params = new URLSearchParams({ backend: "live" });
  if (bridgeUrl) {
    params.set("bridgeUrl", bridgeUrl);
  }
  await page.goto(`/tests/ui-harness/harness.html?${params.toString()}`);
  await expect(page.locator(".app-shell")).toBeVisible();
  await expect(page.locator("html")).toHaveAttribute(
    "data-desktop-ui-harness-backend",
    "live",
  );
}

export async function openChat(page: Page) {
  await expect(page.getByTestId("fleet-dashboard")).toBeVisible();
  await page.getByTestId(`fleet-row-${PEER_ID}`).click();
  if ((page.viewportSize()?.width ?? Number.POSITIVE_INFINITY) <= 760) {
    await page.getByTestId("conversation-session-intro").click();
  }
  await expect(page.getByTestId("composer-input")).toBeVisible();
}

export async function openChatNavigation(page: Page) {
  const sidebar = page.locator(".sidebar");
  if (
    !(await sidebar.isVisible()) &&
    (page.viewportSize()?.width ?? Number.POSITIVE_INFINITY) <= 760
  ) {
    await page.getByTestId("mobile-chat-navigation").click();
  }
  await expect(sidebar).toBeVisible();
}

export async function openConfig(page: Page) {
  await expect(page.getByTestId("fleet-dashboard")).toBeVisible();
  await page.getByTestId(`fleet-row-${PEER_ID}`).click();
  await openChatNavigation(page);
  await page.getByTestId("agent-actions").click();
  await page.getByRole("button", { name: "Configure" }).click();
  await expect(page.locator(".config-workspace")).toBeVisible();
}

export async function openConfigTab(page: Page, tabId: string) {
  const tab = page.getByTestId(`config-tab-${tabId}`);
  await tab.click();
  await expect(tab).toHaveClass(/selected/);
}

export async function saveConfig(page: Page, testId: string) {
  await page.getByTestId(testId).click();
  await expect(page.locator(".config-editor").getByText("Saved")).toBeVisible();
}

export async function primarySurfaceCount(page: Page) {
  return page.evaluate(() => {
    const selectors = [
      '[data-testid="fleet-dashboard"]',
      '[data-testid="fleet-empty"]',
      '[data-testid="transcript-panel"]',
      ".config-workspace",
      '[data-testid="startup-screen"]',
    ];
    return selectors.filter((selector) => document.querySelector(selector)).length;
  });
}

export async function enabledButtonsWithoutAccessibleNames(page: Page) {
  return page.evaluate(() => {
    return Array.from(document.querySelectorAll("button"))
      .filter((button) => !button.disabled)
      .map((button) => {
        const label =
          button.getAttribute("aria-label") ??
          button.getAttribute("title") ??
          button.textContent ??
          "";
        return {
          html: button.outerHTML,
          label: label.replace(/\s+/g, " ").trim(),
        };
      })
      .filter((button) => button.label.length === 0)
      .map((button) => button.html);
  });
}

export async function adjacentDuplicateTranscriptRows(page: Page) {
  return page
    .locator('[data-testid="transcript-panel"] .message-card')
    .evaluateAll((cards) => {
      const rows = cards.map((card) => {
        const roleText = card.querySelector(".message-role")?.textContent ?? "";
        const contentText = card.querySelector(".message-content")?.textContent ?? "";
        const role = roleText.replace(/\s+/g, " ").trim();
        const content = contentText.replace(/\s+/g, " ").trim();
        return { role, content };
      });
      const duplicates: string[] = [];
      for (let index = 1; index < rows.length; index += 1) {
        const previous = rows[index - 1];
        const current = rows[index];
        if (
          previous.role &&
          previous.content &&
          previous.role === current.role &&
          previous.content === current.content
        ) {
          duplicates.push(`${current.role}: ${current.content}`);
        }
      }
      return duplicates;
    });
}

export async function expectNoPageHorizontalOverflow(page: Page) {
  const overflow = await page.evaluate(() => {
    const documentWidth = Math.max(
      document.documentElement.scrollWidth,
      document.body?.scrollWidth ?? 0,
    );
    return {
      documentWidth,
      viewportWidth: window.innerWidth,
      offenders: Array.from(
        document.querySelectorAll(
          [
            ".app-shell",
            ".workspace",
            ".chat-workspace",
            ".chat-main",
            ".config-workspace",
            ".fleet-dashboard",
            ".fleet-empty",
            ".operations-rail",
          ].join(", "),
        ),
      )
        .map((element) => {
          const htmlElement = element as HTMLElement;
          const style = window.getComputedStyle(htmlElement);
          return {
            selector:
              htmlElement.getAttribute("data-testid") ??
              htmlElement.className.toString() ??
              htmlElement.tagName,
            clientWidth: htmlElement.clientWidth,
            scrollWidth: htmlElement.scrollWidth,
            overflowX: style.overflowX,
          };
        })
        .filter((entry) => {
          const scrollDelta = entry.scrollWidth - entry.clientWidth;
          return scrollDelta > 2 && entry.overflowX === "visible";
        }),
    };
  });

  expect(overflow.documentWidth).toBeLessThanOrEqual(overflow.viewportWidth + 2);
  expect(overflow.offenders).toEqual([]);
}

export async function captureStableScreenshot(
  page: Page,
  testInfo: TestInfo,
  name: string,
): Promise<{ attachmentName: string; path: string }> {
  const path = testInfo.outputPath(`${name}.png`);
  await page.screenshot({ fullPage: true, path });
  const attachmentName = `${name}.png`;
  await testInfo.attach(attachmentName, {
    path,
    contentType: "image/png",
  });
  return { attachmentName, path };
}
