import {
  expect,
  expectNoPageHorizontalOverflow,
  gotoHarness,
  openChat,
  test,
} from "./desktopTest";

test.describe("desktop operations drawer rich states", () => {
  test("operations data does not leak into conversation chrome", async ({ page }) => {
    await gotoHarness(page, "operations-rich");
    await openChat(page);
    await expect(
      page.getByRole("button", { name: /open operations drawer/i }),
    ).toHaveCount(0);
    await expect(page.getByRole("complementary", { name: "Operations" })).toHaveCount(
      0,
    );
    await expect(page.getByText("cargo test", { exact: true })).toHaveCount(0);
    await expect(page.getByText("mcp-observability")).toHaveCount(0);
    await expectNoPageHorizontalOverflow(page);
  });
});
