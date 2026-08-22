use std::io::Read;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Discovery order from the immutable `base_sha` blob, not the live worktree.
/// Live unbound walks use the same names, one non-empty file per directory.
pub const DEFAULT_INSTRUCTION_PATHS: &[&str] = &["AGENTS.override.md", "AGENTS.md"];
pub const INSTRUCTION_TEXT_BUDGET: usize = 32 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstructionManifest {
    pub schema: u32,
    pub base_sha: String,
    pub files: Vec<InstructionFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstructionFile {
    pub path: String,
    pub sha256: String,
    pub bytes: usize,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub truncated: String,
    pub text: String,
}

impl InstructionManifest {
    pub fn new(base_sha: impl Into<String>, files: Vec<InstructionFile>) -> Self {
        Self {
            schema: 1,
            base_sha: base_sha.into(),
            files,
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        let raw = raw.trim();
        if raw.is_empty() || raw == "{}" {
            return None;
        }
        serde_json::from_str(raw).ok()
    }

    pub fn to_json_string(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
}

impl InstructionFile {
    pub fn from_bytes(path: &str, bytes: &[u8]) -> Self {
        Self::from_bytes_with_budget(path, bytes, INSTRUCTION_TEXT_BUDGET)
    }

    fn from_bytes_with_budget(path: &str, bytes: &[u8], budget: usize) -> Self {
        let digest = Sha256::digest(bytes);
        let sha256 = format!("{digest:x}");
        let (text, truncated) = bound_instruction_text_to(bytes, budget);
        Self {
            path: path.to_string(),
            sha256,
            bytes: bytes.len(),
            truncated,
            text,
        }
    }
}

pub fn is_empty_manifest(raw: &str) -> bool {
    match InstructionManifest::parse(raw) {
        Some(manifest) => manifest.is_empty(),
        None => true,
    }
}

/// History-stripped `<context>` section for workspace-bound requests.
pub fn instruction_context_section(raw: &str) -> Option<String> {
    let manifest = InstructionManifest::parse(raw)?;
    if manifest.is_empty() {
        return None;
    }
    Some(format_instruction_section(
        &format!(
            "# Frozen workspace instructions (base_sha={})",
            manifest.base_sha
        ),
        &manifest.files,
    ))
}

/// Live cwd→tool-root walk for unbound requests only (#728 remainder).
pub fn live_instruction_context_section(
    cwd: Option<&Path>,
    tool_root: Option<&Path>,
) -> Option<String> {
    let tool_root = tool_root?;
    let files = discover_live_instruction_files(cwd, tool_root);
    if files.is_empty() {
        return None;
    }
    Some(format_instruction_section(
        "# Live workspace instructions",
        &files,
    ))
}

/// Bound requests use the frozen manifest even when empty; unbound walks live files.
pub fn instruction_body_for_request(
    frozen_instruction_manifest: Option<&str>,
    live_cwd: Option<&Path>,
    live_tool_root: Option<&Path>,
) -> Option<String> {
    match frozen_instruction_manifest {
        Some(raw) => instruction_context_section(raw),
        None => live_instruction_context_section(live_cwd, live_tool_root),
    }
}

fn format_instruction_section(header: &str, files: &[InstructionFile]) -> String {
    let mut body = String::from(header);
    body.push('\n');
    for file in files {
        body.push('\n');
        body.push_str("## ");
        body.push_str(&file.path);
        body.push('\n');
        if !file.truncated.is_empty() {
            body.push_str(&file.truncated);
            body.push('\n');
        }
        body.push_str(&file.text);
        if !file.text.ends_with('\n') {
            body.push('\n');
        }
    }
    body
}

fn discover_live_instruction_files(cwd: Option<&Path>, tool_root: &Path) -> Vec<InstructionFile> {
    let Ok(tool_root) = std::fs::canonicalize(tool_root) else {
        return Vec::new();
    };
    if !tool_root.is_dir() {
        return Vec::new();
    }
    let start = cwd
        .and_then(|path| std::fs::canonicalize(path).ok())
        .filter(|path| path.is_dir() && path.starts_with(&tool_root))
        .unwrap_or_else(|| tool_root.clone());

    let mut dirs = Vec::new();
    let mut current = start;
    loop {
        dirs.push(current.clone());
        if current == tool_root {
            break;
        }
        match current.parent() {
            Some(parent) => current = parent.to_path_buf(),
            None => break,
        }
    }
    dirs.reverse();

    let mut remaining = INSTRUCTION_TEXT_BUDGET;
    let mut files = Vec::new();
    for dir in dirs {
        if remaining == 0 {
            break;
        }
        let Some(found) = first_live_instruction_file(&dir, &tool_root) else {
            continue;
        };
        let Some(bytes) = read_live_instruction_prefix(&found.read_path, remaining) else {
            continue;
        };
        let file = InstructionFile::from_bytes_with_budget(&found.display, &bytes, remaining);
        if file.text.is_empty() {
            continue;
        }
        if !file.truncated.is_empty() {
            tracing::warn!(
                path = %found.display,
                budget = remaining,
                "live AGENTS.md truncated to instruction budget"
            );
        }
        remaining = remaining.saturating_sub(file.text.len());
        files.push(file);
    }
    files
}

struct LiveInstructionPath {
    display: String,
    read_path: PathBuf,
}

fn first_live_instruction_file(dir: &Path, tool_root: &Path) -> Option<LiveInstructionPath> {
    for name in DEFAULT_INSTRUCTION_PATHS {
        let candidate = dir.join(name);
        let Ok(canonical) = std::fs::canonicalize(&candidate) else {
            continue;
        };
        if !canonical.is_file() || !canonical.starts_with(tool_root) {
            continue;
        }
        match std::fs::metadata(&canonical) {
            Ok(meta) if meta.len() > 0 => {
                return Some(LiveInstructionPath {
                    display: instruction_display_path(&candidate, tool_root),
                    read_path: canonical,
                });
            }
            _ => continue,
        }
    }
    None
}

fn instruction_display_path(path: &Path, tool_root: &Path) -> String {
    path.strip_prefix(tool_root)
        .ok()
        .map(|relative| relative.to_string_lossy().replace('\\', "/"))
        .filter(|relative| !relative.is_empty())
        .or_else(|| {
            path.file_name()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .unwrap_or_else(|| "AGENTS.md".to_string())
}

/// Read at most `remaining` bytes plus a short UTF-8 tail so a multi-gigabyte
/// file cannot be pulled into the inference entry path.
fn read_live_instruction_prefix(path: &Path, remaining: usize) -> Option<Vec<u8>> {
    let file = std::fs::File::open(path).ok()?;
    let mut buf = Vec::new();
    let cap = remaining.saturating_add(4);
    file.take(cap as u64).read_to_end(&mut buf).ok()?;
    if buf.is_empty() {
        None
    } else {
        Some(buf)
    }
}

fn bound_instruction_text_to(bytes: &[u8], budget: usize) -> (String, String) {
    let lossy = String::from_utf8_lossy(bytes);
    if budget == 0 {
        return (String::new(), "truncated to 0 bytes".to_string());
    }
    if lossy.len() <= budget {
        return (lossy.into_owned(), String::new());
    }
    let mut end = budget;
    while end > 0 && !lossy.is_char_boundary(end) {
        end -= 1;
    }
    (
        lossy[..end].to_string(),
        format!("truncated to {end} bytes"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_json_is_not_injected() {
        assert!(instruction_context_section("{}").is_none());
        assert!(instruction_context_section("").is_none());
        assert!(is_empty_manifest("{}"));
    }

    #[test]
    fn frozen_section_uses_manifest_text_not_a_live_path() {
        let manifest = InstructionManifest::new(
            "abc",
            vec![InstructionFile::from_bytes(
                "AGENTS.md",
                b"frozen-base-instructions\n",
            )],
        );
        let section = instruction_context_section(&manifest.to_json_string()).unwrap();
        assert!(section.contains("frozen-base-instructions"));
        assert!(section.contains("base_sha=abc"));
        assert!(!section.contains("live-writer"));
    }

    fn live_tree() -> (tempfile::TempDir, PathBuf, PathBuf, PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let outside = tmp.path().join("outside");
        let root = tmp.path().join("root");
        let nested = root.join("src");
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::create_dir_all(&nested).unwrap();
        // Parent of the tool root — must not load if the walk stops at root.
        std::fs::write(
            tmp.path().join("AGENTS.md"),
            "parent-of-root-instructions\n",
        )
        .unwrap();
        std::fs::write(outside.join("AGENTS.md"), "outside-instructions\n").unwrap();
        std::fs::write(root.join("AGENTS.md"), "root-instructions\n").unwrap();
        std::fs::write(nested.join("AGENTS.md"), "nested-instructions\n").unwrap();
        let root = std::fs::canonicalize(&root).unwrap();
        let nested = std::fs::canonicalize(&nested).unwrap();
        let outside = std::fs::canonicalize(&outside).unwrap();
        (tmp, root, nested, outside)
    }

    #[test]
    fn live_walk_concatenates_tool_root_to_cwd_most_local_last() {
        let (_tmp, root, nested, _outside) = live_tree();
        let section = live_instruction_context_section(Some(&nested), Some(&root)).unwrap();
        let root_at = section.find("root-instructions").expect("root file");
        let nested_at = section.find("nested-instructions").expect("nested file");
        assert!(root_at < nested_at, "tool-root content must precede cwd");
        assert!(!section.contains("outside-instructions"));
        assert!(!section.contains("parent-of-root-instructions"));
        assert!(section.contains("# Live workspace instructions"));
        assert!(section.contains("## AGENTS.md\n"));
        assert!(section.contains("## src/AGENTS.md\n"));
        assert!(
            !section.contains(&root.to_string_lossy().replace('\\', "/")),
            "headings must not leak the host tool-root path: {section}"
        );
    }

    #[test]
    fn live_walk_omits_parent_of_tool_root() {
        let (_tmp, root, nested, _outside) = live_tree();
        let section = live_instruction_context_section(Some(&nested), Some(&root)).unwrap();
        assert!(section.contains("root-instructions"));
        assert!(section.contains("nested-instructions"));
        assert!(!section.contains("parent-of-root-instructions"));
    }

    #[test]
    fn live_walk_cwd_outside_root_uses_tool_root_only() {
        let (_tmp, root, _nested, outside) = live_tree();
        let section = live_instruction_context_section(Some(&outside), Some(&root)).unwrap();
        assert!(section.contains("root-instructions"));
        assert!(!section.contains("nested-instructions"));
        assert!(!section.contains("outside-instructions"));
        assert!(!section.contains("parent-of-root-instructions"));
    }

    #[test]
    fn live_walk_prefers_override_over_agents_md_in_the_same_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("AGENTS.md"), "default-instructions\n").unwrap();
        std::fs::write(root.join("AGENTS.override.md"), "override-instructions\n").unwrap();
        let root = std::fs::canonicalize(&root).unwrap();
        let section = live_instruction_context_section(Some(&root), Some(&root)).unwrap();
        assert!(section.contains("override-instructions"));
        assert!(!section.contains("default-instructions"));
    }

    #[test]
    fn live_walk_without_cwd_uses_tool_root_only() {
        let (_tmp, root, _nested, _outside) = live_tree();
        let section = live_instruction_context_section(None, Some(&root)).unwrap();
        assert!(section.contains("root-instructions"));
        assert!(!section.contains("nested-instructions"));
        assert!(!section.contains("parent-of-root-instructions"));
    }

    #[cfg(unix)]
    #[test]
    fn live_walk_follows_agents_md_symlink_inside_tool_root() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("CLAUDE.md"), "symlink-target-instructions\n").unwrap();
        std::os::unix::fs::symlink(root.join("CLAUDE.md"), root.join("AGENTS.md")).unwrap();
        let root = std::fs::canonicalize(&root).unwrap();
        let section = live_instruction_context_section(Some(&root), Some(&root)).unwrap();
        assert!(section.contains("symlink-target-instructions"));
    }

    #[cfg(unix)]
    #[test]
    fn live_walk_skips_symlink_escape_outside_tool_root() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let outside = tmp.path().join("outside");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("AGENTS.md"), "escaped-instructions\n").unwrap();
        std::os::unix::fs::symlink(outside.join("AGENTS.md"), root.join("AGENTS.md")).unwrap();
        let root = std::fs::canonicalize(&root).unwrap();
        assert!(live_instruction_context_section(Some(&root), Some(&root)).is_none());
    }

    #[test]
    fn live_walk_truncates_to_budget_root_first() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let nested = root.join("src");
        std::fs::create_dir_all(&nested).unwrap();
        let root_text = "R".repeat(INSTRUCTION_TEXT_BUDGET);
        std::fs::write(root.join("AGENTS.md"), &root_text).unwrap();
        std::fs::write(nested.join("AGENTS.md"), "nested-should-not-fit\n").unwrap();
        let root = std::fs::canonicalize(&root).unwrap();
        let nested = std::fs::canonicalize(&nested).unwrap();
        let section = live_instruction_context_section(Some(&nested), Some(&root)).unwrap();
        assert!(section.contains(&root_text));
        assert!(!section.contains("nested-should-not-fit"));
    }

    #[test]
    fn live_walk_truncates_file_larger_than_budget() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        let huge = "H".repeat(INSTRUCTION_TEXT_BUDGET + 128);
        std::fs::write(root.join("AGENTS.md"), &huge).unwrap();
        let root = std::fs::canonicalize(&root).unwrap();
        let section = live_instruction_context_section(Some(&root), Some(&root)).unwrap();
        assert!(section.contains("truncated"));
        assert!(!section.contains(&huge));
        let heading = "## AGENTS.md\n";
        let start = section.find(heading).expect("heading") + heading.len();
        let rest = &section[start..];
        let text = rest
            .strip_prefix("truncated to ")
            .and_then(|tail| tail.find('\n').map(|idx| &tail[idx + 1..]))
            .unwrap_or(rest)
            .trim_end_matches('\n');
        assert!(text.len() <= INSTRUCTION_TEXT_BUDGET);
        assert_eq!(text, "H".repeat(INSTRUCTION_TEXT_BUDGET));
        assert!(!section.contains(&root.to_string_lossy().replace('\\', "/")));
    }

    #[test]
    fn live_walk_skips_empty_override_and_uses_agents_md() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("AGENTS.override.md"), "").unwrap();
        std::fs::write(root.join("AGENTS.md"), "default-instructions\n").unwrap();
        let root = std::fs::canonicalize(&root).unwrap();
        let section = live_instruction_context_section(Some(&root), Some(&root)).unwrap();
        assert!(section.contains("default-instructions"));
    }

    #[test]
    fn bound_empty_manifest_does_not_live_walk() {
        let (_tmp, root, nested, _outside) = live_tree();
        assert!(instruction_body_for_request(Some("{}"), Some(&nested), Some(&root)).is_none());
        assert!(instruction_body_for_request(Some(""), Some(&nested), Some(&root)).is_none());
    }

    #[test]
    fn unbound_body_uses_live_files() {
        let (_tmp, root, nested, _outside) = live_tree();
        let body = instruction_body_for_request(None, Some(&nested), Some(&root)).unwrap();
        assert!(body.contains("nested-instructions"));
        assert!(body.contains("root-instructions"));
        assert!(!body.contains("parent-of-root-instructions"));
    }

    #[test]
    fn bound_body_keeps_frozen_when_live_tree_changed() {
        let (_tmp, root, nested, _outside) = live_tree();
        let manifest = InstructionManifest::new(
            "abc",
            vec![InstructionFile::from_bytes(
                "AGENTS.md",
                b"frozen-base-instructions\n",
            )],
        );
        let body = instruction_body_for_request(
            Some(&manifest.to_json_string()),
            Some(&nested),
            Some(&root),
        )
        .unwrap();
        assert!(body.contains("frozen-base-instructions"));
        assert!(!body.contains("nested-instructions"));
        assert!(!body.contains("root-instructions"));
        assert!(!body.contains("live-writer"));
    }
}
