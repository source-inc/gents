use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Discovery order from the immutable `base_sha` blob, not the live worktree.
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
        let digest = Sha256::digest(bytes);
        let sha256 = format!("{digest:x}");
        let (text, truncated) = bound_instruction_text(bytes);
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
    let mut body = format!(
        "# Frozen workspace instructions (base_sha={})\n",
        manifest.base_sha
    );
    for file in &manifest.files {
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
    Some(body)
}

fn bound_instruction_text(bytes: &[u8]) -> (String, String) {
    let lossy = String::from_utf8_lossy(bytes);
    if lossy.len() <= INSTRUCTION_TEXT_BUDGET {
        return (lossy.into_owned(), String::new());
    }
    let mut end = INSTRUCTION_TEXT_BUDGET;
    while end > 0 && !lossy.is_char_boundary(end) {
        end -= 1;
    }
    (
        lossy[..end].to_string(),
        format!("truncated to {INSTRUCTION_TEXT_BUDGET} bytes"),
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
}
