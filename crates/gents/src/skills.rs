//! Runtime skill resolution — the executable realization of the privilege
//! algebra proved in `proofs/Proofs/Skills.lean`.
//!
//! A [`Skill`] declares the tools it *depends on* (`tool_refs`); it never
//! *grants* them (decision D3, Codex-faithful). [`effective_skills`] computes
//! the per-behavior candidate set (decision D5: scope-on-skill inheritance +
//! `skill_refs`/`skill_excludes`). [`skill_tools`] intersects a skill's
//! declared refs with the behavior's resolved tool ceiling and degrades when a
//! dep is missing, so activation can never widen the tool surface beyond the
//! ceiling — the executable counterpart of `Skills.activation_subset_ceiling`.
//!
//! This module is pure (no DB / request plumbing). The runtime wiring that
//! loads `Skill` documents and feeds these results into `prompt.rs` and
//! `tool_surface` is layered on top of it.

use std::cmp::Ordering;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkillScope {
    Principal,
    Behavior,
}

impl SkillScope {
    pub fn parse(value: &str) -> Option<SkillScope> {
        match value.trim() {
            "principal" => Some(SkillScope::Principal),
            "behavior" => Some(SkillScope::Behavior),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            SkillScope::Principal => "principal",
            SkillScope::Behavior => "behavior",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Skill {
    pub skill_id: String,
    pub agent_did: String,
    pub scope: SkillScope,
    pub name: String,
    pub description: String,
    pub instructions: String,
    pub tool_refs: Vec<String>,
    pub display_name: Option<String>,
    pub enabled: bool,
}

pub fn effective_skills<'a>(
    skills: &'a [Skill],
    behavior_principal: &str,
    skill_refs: &[String],
    skill_excludes: &[String],
) -> Vec<&'a Skill> {
    let refs: BTreeSet<&str> = skill_refs.iter().map(String::as_str).collect();
    let excludes: BTreeSet<&str> = skill_excludes.iter().map(String::as_str).collect();
    let mut effective = skills
        .iter()
        .filter(|skill| {
            skill.agent_did == behavior_principal
                && skill.enabled
                && (skill.scope == SkillScope::Principal || refs.contains(skill.skill_id.as_str()))
                && !excludes.contains(skill.skill_id.as_str())
        })
        .collect::<Vec<_>>();
    effective.sort_by(|left, right| canonical_skill_order(left, right));
    effective
}

/// Canonicalize a resolved skill set before it crosses a rendering or
/// fingerprint boundary.
///
/// Document-backed resolution starts from a `HashMap`, whose iteration order is
/// intentionally unspecified. Ordering by the complete render-relevant value
/// makes equal source sets produce equal provider preambles even if rows arrive
/// in a different order on another host. `skill_id` remains the primary key for
/// readability; the remaining fields make replicated logical-ID conflicts
/// deterministic until the config loader can reject them explicitly.
pub(crate) fn sort_skills_canonically(skills: &mut [Skill]) {
    skills.sort_by(canonical_skill_order);
}

fn canonical_skill_order(left: &Skill, right: &Skill) -> Ordering {
    left.skill_id
        .cmp(&right.skill_id)
        .then_with(|| left.agent_did.cmp(&right.agent_did))
        .then_with(|| left.scope.as_str().cmp(right.scope.as_str()))
        .then_with(|| left.name.cmp(&right.name))
        .then_with(|| left.description.cmp(&right.description))
        .then_with(|| left.instructions.cmp(&right.instructions))
        .then_with(|| left.tool_refs.cmp(&right.tool_refs))
        .then_with(|| left.display_name.cmp(&right.display_name))
        .then_with(|| left.enabled.cmp(&right.enabled))
}

#[derive(Debug, Clone, Default)]
pub struct SkillToolCeiling {
    names: BTreeSet<String>,
    mcp_unrestricted: bool,
}

impl SkillToolCeiling {
    pub fn new(names: BTreeSet<String>, mcp_unrestricted: bool) -> Self {
        Self {
            names,
            mcp_unrestricted,
        }
    }

    pub fn allows(&self, tool: &str) -> bool {
        self.mcp_unrestricted || self.names.contains(tool)
    }
}

pub fn skill_tool_ceiling(
    tool_names: impl IntoIterator<Item = String>,
    allowed_mcp_service_ids: &[String],
    mcp_enabled: bool,
) -> SkillToolCeiling {
    let mut names: BTreeSet<String> = tool_names.into_iter().collect();
    names.extend(allowed_mcp_service_ids.iter().cloned());
    let mcp_unrestricted = mcp_enabled && allowed_mcp_service_ids.is_empty();
    SkillToolCeiling::new(names, mcp_unrestricted)
}

pub fn skill_tools<'a>(skill: &'a Skill, ceiling: &SkillToolCeiling) -> Vec<&'a str> {
    skill
        .tool_refs
        .iter()
        .map(String::as_str)
        .filter(|tool| ceiling.allows(tool))
        .collect()
}

pub fn missing_tool_refs<'a>(skill: &'a Skill, ceiling: &SkillToolCeiling) -> Vec<&'a str> {
    skill
        .tool_refs
        .iter()
        .map(String::as_str)
        .filter(|tool| !ceiling.allows(tool))
        .collect()
}

fn skill_label(skill: &Skill) -> &str {
    if let Some(display_name) = skill
        .display_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return display_name;
    }
    if skill.name.trim().is_empty() {
        skill.skill_id.as_str()
    } else {
        skill.name.as_str()
    }
}

pub fn render_skill_catalog(skills: &[Skill]) -> Option<String> {
    if skills.is_empty() {
        return None;
    }
    let mut out = String::from(
        "## Skills\n\nThese skills are available. Before acting, scan them; if one is relevant \
         to the task, call the `load_skill` tool with its name and follow the returned \
         instructions. Skip skills only when none are relevant.\n",
    );
    let mut ordered = skills.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| canonical_skill_order(left, right));
    for skill in ordered {
        out.push_str(&format!(
            "\n- {}: {}",
            skill_label(skill),
            skill.description
        ));
    }
    Some(out)
}

pub fn render_activated_skill(skill: &Skill, ceiling: &SkillToolCeiling) -> String {
    let mut out = format!("Skill: {}\n\n{}", skill_label(skill), skill.instructions);
    let missing = missing_tool_refs(skill, ceiling);
    if !missing.is_empty() {
        out.push_str(&format!(
            "\n\nNote: this skill references tools that are not available to this behavior \
             and cannot be used: {}.",
            missing.join(", ")
        ));
    }
    out
}

pub fn find_skill<'a>(skills: &'a [Skill], needle: &str) -> Option<&'a Skill> {
    let needle = needle.trim();
    let display_name = |skill: &Skill| {
        skill
            .display_name
            .as_deref()
            .unwrap_or_default()
            .trim()
            .to_string()
    };
    skills
        .iter()
        .find(|skill| {
            skill.name == needle || skill.skill_id == needle || display_name(skill) == needle
        })
        .or_else(|| {
            skills.iter().find(|skill| {
                skill.name.eq_ignore_ascii_case(needle)
                    || skill.skill_id.eq_ignore_ascii_case(needle)
                    || display_name(skill).eq_ignore_ascii_case(needle)
            })
        })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptSlashSkillSelection {
    pub selected_skill_ids: Vec<String>,
    pub prompt: String,
}

pub fn prompt_slash_skill_selection(prompt: &str) -> PromptSlashSkillSelection {
    let mut selected = Vec::new();
    let mut body_lines = Vec::new();
    let mut saw_selector = false;

    let mut lines = prompt.lines();
    while let Some(line) = lines.next() {
        if line.trim().is_empty() && !saw_selector {
            continue;
        }

        let Some(selector) = leading_slash_skill_selector(line) else {
            if !saw_selector {
                return PromptSlashSkillSelection {
                    selected_skill_ids: Vec::new(),
                    prompt: prompt.to_string(),
                };
            }
            body_lines.push(line.to_string());
            body_lines.extend(lines.map(ToOwned::to_owned));
            break;
        };

        if !selected
            .iter()
            .any(|existing| existing == &selector.skill_id)
        {
            selected.push(selector.skill_id);
        }
        if !selector.remainder.is_empty() {
            body_lines.push(selector.remainder);
        }
        saw_selector = true;
    }

    if !saw_selector {
        return PromptSlashSkillSelection {
            selected_skill_ids: Vec::new(),
            prompt: prompt.to_string(),
        };
    }

    PromptSlashSkillSelection {
        selected_skill_ids: selected,
        prompt: body_lines.join("\n"),
    }
}

pub fn selected_skill_ids_from_prompt_slash_commands(prompt: &str) -> Vec<String> {
    prompt_slash_skill_selection(prompt).selected_skill_ids
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SlashSkillSelector {
    skill_id: String,
    remainder: String,
}

fn leading_slash_skill_selector(line: &str) -> Option<SlashSkillSelector> {
    let trimmed = line.trim_start();
    let rest = trimmed.strip_prefix('/')?;
    let end = rest
        .find(|ch: char| !(ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.')))
        .unwrap_or(rest.len());
    let command = &rest[..end];
    if command.is_empty() {
        return None;
    }

    let after_command = &rest[end..];
    if command.eq_ignore_ascii_case("skill") {
        let remainder = after_command.trim_start();
        return parse_skill_command_argument(remainder);
    }

    if is_reserved_client_slash_command(command) {
        return None;
    }

    if rest[end..].starts_with('/') {
        return None;
    }

    Some(SlashSkillSelector {
        skill_id: command.to_string(),
        remainder: after_command.trim_start().to_string(),
    })
}

fn parse_skill_command_argument(input: &str) -> Option<SlashSkillSelector> {
    let end = input
        .find(|ch: char| !(ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.')))
        .unwrap_or(input.len());
    let skill_id = &input[..end];
    if skill_id.is_empty() || input[end..].starts_with('/') {
        return None;
    }
    Some(SlashSkillSelector {
        skill_id: skill_id.to_string(),
        remainder: input[end..].trim_start().to_string(),
    })
}

fn is_reserved_client_slash_command(command: &str) -> bool {
    matches!(
        command.to_ascii_lowercase().as_str(),
        "agent"
            | "apps"
            | "approve"
            | "btw"
            | "clean"
            | "clear"
            | "compact"
            | "copy"
            | "debug-config"
            | "debug-m-drop"
            | "debug-m-update"
            | "diff"
            | "exit"
            | "experimental"
            | "feedback"
            | "fork"
            | "goal"
            | "hooks"
            | "ide"
            | "init"
            | "keymap"
            | "logout"
            | "mcp"
            | "memories"
            | "mention"
            | "model"
            | "new"
            | "permissions"
            | "personality"
            | "pet"
            | "pets"
            | "plan"
            | "plugins"
            | "ps"
            | "quit"
            | "raw"
            | "realtime"
            | "rename"
            | "resume"
            | "review"
            | "rollout"
            | "sandbox-add-read-dir"
            | "settings"
            | "setup-default-sandbox"
            | "side"
            | "skills"
            | "status"
            | "statusline"
            | "stop"
            | "subagents"
            | "test-approval"
            | "theme"
            | "title"
            | "vim"
    )
}

#[derive(Debug)]
pub struct LoadSkillError(pub String);

impl std::fmt::Display for LoadSkillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for LoadSkillError {}

#[derive(Debug, serde::Deserialize)]
pub struct LoadSkillArgs {
    pub name: String,
}

#[derive(Clone)]
pub struct LoadSkillTool {
    skills: Vec<Skill>,
    ceiling: SkillToolCeiling,
}

impl LoadSkillTool {
    pub fn new(skills: Vec<Skill>, ceiling: SkillToolCeiling) -> Self {
        Self { skills, ceiling }
    }
}

impl crate::llm::tool::Tool for LoadSkillTool {
    const NAME: &'static str = "load_skill";
    type Error = LoadSkillError;
    type Args = LoadSkillArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> crate::llm::tool::ToolDefinition {
        crate::llm::tool::ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Load a skill's full instructions by name (or skill_id), then follow \
                them for the task. Choose a skill from the Skills catalog in your system prompt."
                .to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "The skill name or skill_id from the Skills catalog."
                    }
                },
                "required": ["name"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> std::result::Result<Self::Output, Self::Error> {
        match find_skill(&self.skills, &args.name) {
            Some(skill) => Ok(render_activated_skill(skill, &self.ceiling)),
            None => {
                let available = self
                    .skills
                    .iter()
                    .map(skill_label)
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!(
                    "No skill named {:?}. Available skills: {available}.",
                    args.name.trim()
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn skill(id: &str, principal: &str, scope: SkillScope, tool_refs: &[&str]) -> Skill {
        Skill {
            skill_id: id.to_string(),
            agent_did: principal.to_string(),
            scope,
            name: format!("{id}-name"),
            description: format!("{id}-desc"),
            instructions: format!("{id}-instructions"),
            tool_refs: tool_refs.iter().map(|s| s.to_string()).collect(),
            display_name: None,
            enabled: true,
        }
    }

    /// A restricted ceiling (MCP not unrestricted) listing exactly `tools`.
    fn ceiling(tools: &[&str]) -> SkillToolCeiling {
        SkillToolCeiling::new(tools.iter().map(|s| s.to_string()).collect(), false)
    }

    fn ids(skills: &[&Skill]) -> Vec<String> {
        skills.iter().map(|s| s.skill_id.clone()).collect()
    }

    #[test]
    fn principal_scope_is_inherited_without_refs() {
        let skills = vec![skill("a", "did:p", SkillScope::Principal, &[])];
        let got = effective_skills(&skills, "did:p", &[], &[]);
        assert_eq!(ids(&got), vec!["a"]);
    }

    #[test]
    fn behavior_scope_requires_an_explicit_ref() {
        let skills = vec![skill("a", "did:p", SkillScope::Behavior, &[])];
        assert!(effective_skills(&skills, "did:p", &[], &[]).is_empty());
        let got = effective_skills(&skills, "did:p", &["a".to_string()], &[]);
        assert_eq!(ids(&got), vec!["a"]);
    }

    #[test]
    fn excludes_remove_inherited_principal_skills() {
        let skills = vec![skill("a", "did:p", SkillScope::Principal, &[])];
        let got = effective_skills(&skills, "did:p", &[], &["a".to_string()]);
        assert!(got.is_empty());
    }

    #[test]
    fn disabled_and_foreign_principal_skills_are_excluded() {
        let mut disabled = skill("a", "did:p", SkillScope::Principal, &[]);
        disabled.enabled = false;
        let foreign = skill("b", "did:other", SkillScope::Principal, &[]);
        let skills = vec![disabled, foreign];
        assert!(effective_skills(&skills, "did:p", &[], &[]).is_empty());
    }

    /// S-Skill-3 (candidate_set respects principal): every effective skill
    /// belongs to the behavior's principal and is enabled.
    #[test]
    fn effective_skills_respect_principal() {
        let skills = vec![
            skill("a", "did:p", SkillScope::Principal, &[]),
            skill("b", "did:p", SkillScope::Behavior, &[]),
            skill("c", "did:other", SkillScope::Principal, &[]),
        ];
        for got in effective_skills(&skills, "did:p", &["b".to_string()], &[]) {
            assert_eq!(got.agent_did, "did:p");
            assert!(got.enabled);
        }
    }

    #[test]
    fn effective_skill_order_and_catalog_are_source_order_independent() {
        let alpha = skill("alpha", "did:p", SkillScope::Principal, &["read"]);
        let beta = skill("beta", "did:p", SkillScope::Behavior, &["bash"]);
        let gamma = skill("gamma", "did:p", SkillScope::Principal, &[]);
        let refs = vec!["beta".to_string()];

        let forward = vec![alpha.clone(), beta.clone(), gamma.clone()];
        let permuted = vec![gamma, alpha, beta];
        let forward_effective = effective_skills(&forward, "did:p", &refs, &[]);
        let permuted_effective = effective_skills(&permuted, "did:p", &refs, &[]);

        assert_eq!(ids(&forward_effective), vec!["alpha", "beta", "gamma"]);
        assert_eq!(ids(&forward_effective), ids(&permuted_effective));

        let forward_owned = forward_effective.into_iter().cloned().collect::<Vec<_>>();
        let permuted_owned = permuted_effective.into_iter().cloned().collect::<Vec<_>>();
        assert_eq!(
            render_skill_catalog(&forward_owned),
            render_skill_catalog(&permuted_owned)
        );

        // Rendering is also canonical when a programmatic caller bypasses
        // `effective_skills` and supplies the same set in a different order.
        assert_eq!(
            render_skill_catalog(&forward),
            render_skill_catalog(&permuted)
        );
    }

    /// S-Skill-1 (activation_subset_ceiling): the union of every active skill's
    /// resolved tools is a subset of the behavior ceiling — activation never
    /// widens the tool surface.
    #[test]
    fn skill_tools_never_widen_the_ceiling() {
        let ceiling = ceiling(&["read", "bash"]);
        let s = skill(
            "a",
            "did:p",
            SkillScope::Principal,
            &["read", "bash", "net"],
        );
        let resolved = skill_tools(&s, &ceiling);
        assert_eq!(resolved, vec!["read", "bash"]); // "net" degraded away
        for tool in &resolved {
            assert!(ceiling.allows(tool));
        }
        assert_eq!(missing_tool_refs(&s, &ceiling), vec!["net"]);
    }

    #[test]
    fn catalog_lists_descriptions_not_bodies() {
        assert!(render_skill_catalog(&[]).is_none());
        let skills = vec![
            skill("a", "did:p", SkillScope::Principal, &[]),
            skill("b", "did:p", SkillScope::Behavior, &[]),
        ];
        let catalog = render_skill_catalog(&skills).expect("catalog");
        assert!(catalog.contains("## Skills"));
        assert!(catalog.contains("load_skill")); // mandate to load on demand
        assert!(catalog.contains("a-name"));
        assert!(catalog.contains("a-desc"));
        assert!(catalog.contains("b-name"));
        // Progressive disclosure: bodies are NOT in the catalog.
        assert!(!catalog.contains("a-instructions"));
        assert!(!catalog.contains("b-instructions"));
    }

    #[test]
    fn catalog_prefers_display_name_over_name() {
        let mut s = skill("a", "did:p", SkillScope::Principal, &[]);
        s.display_name = Some("Pretty Label".to_string());
        let catalog = render_skill_catalog(std::slice::from_ref(&s)).expect("catalog");
        assert!(catalog.contains("Pretty Label"));
        assert!(!catalog.contains("a-name")); // the raw name is superseded by the UI label
    }

    #[test]
    fn render_activated_skill_appends_degrade_note() {
        let ceiling = ceiling(&["read"]);
        let s = skill("a", "did:p", SkillScope::Principal, &["read", "net"]);
        let body = render_activated_skill(&s, &ceiling);
        assert!(body.contains("a-instructions"));
        assert!(body.contains("net")); // degrade note names the missing tool
        let s_ok = skill("b", "did:p", SkillScope::Principal, &["read"]);
        assert!(!render_activated_skill(&s_ok, &ceiling).contains("not available"));
    }

    #[tokio::test]
    async fn load_skill_tool_returns_body_on_demand_and_handles_unknown() {
        use crate::llm::tool::Tool;
        let ceiling = ceiling(&["read"]);
        let skills = vec![skill(
            "research",
            "did:p",
            SkillScope::Principal,
            &["read", "net"],
        )];
        let tool = LoadSkillTool::new(skills, ceiling);

        // load by name -> full body + degrade note for the ungranted "net" ref.
        let body = tool
            .call(LoadSkillArgs {
                name: "research-name".to_string(),
            })
            .await
            .expect("load_skill");
        assert!(body.contains("research-instructions"));
        assert!(body.contains("net"));

        // load by skill_id also works.
        assert!(tool
            .call(LoadSkillArgs {
                name: "research".to_string(),
            })
            .await
            .expect("load by id")
            .contains("research-instructions"));

        // unknown skill -> readable Ok message listing what is available.
        let miss = tool
            .call(LoadSkillArgs {
                name: "nope".to_string(),
            })
            .await
            .expect("unknown is Ok text");
        assert!(miss.contains("No skill named"));
        assert!(miss.contains("research-name"));
    }

    #[test]
    fn skill_tool_ceiling_folds_in_explicit_mcp_service_ids() {
        // Restricted allowlist: built tools AND the explicit MCP ids are allowed.
        let ceiling = skill_tool_ceiling(
            vec!["read".to_string(), "bash".to_string()],
            &["x-data".to_string(), "observability-mcp".to_string()],
            /*mcp_enabled*/ true,
        );
        assert!(ceiling.allows("read"));
        assert!(ceiling.allows("x-data"));
        assert!(ceiling.allows("observability-mcp"));
        assert!(!ceiling.allows("unlisted-service")); // restricted: unknown is denied

        let mut mcp_skill = skill("a", "did:p", SkillScope::Principal, &["x-data"]);
        mcp_skill.tool_refs = vec!["x-data".to_string()];
        assert!(missing_tool_refs(&mcp_skill, &ceiling).is_empty());
    }

    #[test]
    fn skill_tool_ceiling_unrestricted_mcp_allows_any_service() {
        // Default behavior: meta tools on + EMPTY allowlist == any MCP service
        // allowed. A skill's MCP tool_ref must NOT be flagged unavailable, since
        // it may well be a reachable service we cannot enumerate.
        let ceiling = skill_tool_ceiling(
            vec!["read".to_string()],
            &[], // empty allowlist
            /*mcp_enabled*/ true,
        );
        assert!(ceiling.allows("read"));
        assert!(ceiling.allows("some-mcp-service")); // benefit of the doubt
        let mut mcp_skill = skill("a", "did:p", SkillScope::Principal, &["some-mcp-service"]);
        mcp_skill.tool_refs = vec!["some-mcp-service".to_string()];
        assert!(
            missing_tool_refs(&mcp_skill, &ceiling).is_empty(),
            "unrestricted MCP must not flag a service ref as unavailable"
        );

        // But with MCP disabled (no call_tool), an empty allowlist grants nothing.
        let no_mcp = skill_tool_ceiling(vec!["read".to_string()], &[], /*mcp_enabled*/ false);
        assert!(!no_mcp.allows("some-mcp-service"));
        assert_eq!(
            missing_tool_refs(&mcp_skill, &no_mcp),
            vec!["some-mcp-service"]
        );
    }

    #[tokio::test]
    async fn load_skill_resolves_by_display_name() {
        use crate::llm::tool::Tool;
        let mut s = skill("research", "did:p", SkillScope::Principal, &["read"]);
        s.display_name = Some("Deep Research".to_string());
        let tool = LoadSkillTool::new(vec![s], ceiling(&["read"]));
        // The catalog labels it "Deep Research"; load_skill with that label must
        // resolve (else a cataloged skill is unloadable).
        let body = tool
            .call(LoadSkillArgs {
                name: "Deep Research".to_string(),
            })
            .await
            .expect("load by display_name");
        assert!(body.contains("research-instructions"));
    }

    #[test]
    fn leading_slash_commands_select_skills_and_strip_control_syntax() {
        let selection = prompt_slash_skill_selection(
            "\n/vuln-scan /work --focus parser\n/triage\nRun the task.",
        );
        assert_eq!(selection.selected_skill_ids, vec!["vuln-scan", "triage"]);
        assert_eq!(selection.prompt, "/work --focus parser\nRun the task.");

        assert!(
            selected_skill_ids_from_prompt_slash_commands("Run /vuln-scan as plain text",)
                .is_empty()
        );
        assert!(
            selected_skill_ids_from_prompt_slash_commands("/work/entry.c is an absolute path",)
                .is_empty()
        );

        let explicit = prompt_slash_skill_selection("/skill review inspect the diff");
        assert_eq!(explicit.selected_skill_ids, vec!["review"]);
        assert_eq!(explicit.prompt, "inspect the diff");

        let codex_command = prompt_slash_skill_selection("/review inspect the diff");
        assert!(codex_command.selected_skill_ids.is_empty());
        assert_eq!(codex_command.prompt, "/review inspect the diff");
    }

    #[test]
    fn scope_parse_round_trips_and_rejects_unknown() {
        assert_eq!(SkillScope::parse("principal"), Some(SkillScope::Principal));
        assert_eq!(SkillScope::parse(" behavior "), Some(SkillScope::Behavior));
        assert_eq!(SkillScope::parse("global"), None);
        assert_eq!(SkillScope::Principal.as_str(), "principal");
    }
}
