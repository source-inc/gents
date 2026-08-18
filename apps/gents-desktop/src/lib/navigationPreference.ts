const NAVIGATION_EXPANDED_KEY = "gents-desktop-navigation-expanded";

export function loadNavigationExpanded(): boolean {
  try {
    return window.localStorage.getItem(NAVIGATION_EXPANDED_KEY) !== "false";
  } catch {
    return true;
  }
}

export function saveNavigationExpanded(expanded: boolean) {
  try {
    window.localStorage.setItem(NAVIGATION_EXPANDED_KEY, String(expanded));
  } catch {}
}
