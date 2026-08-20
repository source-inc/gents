import { expect, gotoHarness, openChat, openChatNavigation, test } from "./desktopTest";

test.describe("desktop code experience", () => {
  test("renders file edits as diffs and commands as terminal output", async ({
    page,
  }) => {
    await gotoHarness(page, "coding");
    await openChat(page);

    const fileEdit = page.getByTestId("tool-intro-edit-file");
    await expect(fileEdit).toContainText("src/parser.rs");
    await expect(fileEdit).not.toHaveAttribute("open", "");
    await fileEdit.locator("summary").click();
    await expect(fileEdit.locator(".tool-diff")).toContainText("Ast::default()");

    const command = page.getByTestId("tool-intro-bash");
    await expect(command).toContainText("cargo test parser");
    await expect(command.locator(".tool-exit")).toHaveText("exit 0");
    await expect(command).not.toHaveAttribute("open", "");
    await command.locator("summary").click();
    await expect(command.locator(".tool-payload")).toContainText("2 passed");
  });

  test("Code mode surfaces the agent's working directory and permission boundary", async ({
    page,
  }) => {
    await gotoHarness(page, "coding");
    await openChat(page);
    await openChatNavigation(page);
    await page.getByTestId("sidebar-open-code").click();

    const header = page.getByTestId("code-context-header");
    await expect(header).toBeVisible();
    await expect(page.getByTestId("code-context-workdir")).toContainText(
      "/tmp/gents-bombadil/workspace",
    );
    await expect(page.getByTestId("code-context-files")).toHaveText("read-only");
    await page.getByTestId("tool-intro-edit-file").locator("summary").click();
    await expect(page.locator(".tool-diff")).toBeVisible();

    await page.getByTestId("code-back-to-chat").click();
    await expect(page.getByTestId("code-context-header")).toHaveCount(0);
    await expect(page.getByTestId("composer-input")).toBeVisible();
  });
});
