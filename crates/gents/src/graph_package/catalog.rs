use std::collections::BTreeSet;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::document_config::SurfaceToolDecl;
use crate::graph_pipeline::{
    EntryBinding, GraphIntent, PortSpec, ResultContract, WorkspaceAuthorityCeiling,
    COMPILER_VERSION,
};

include!(concat!(env!("OUT_DIR"), "/bundled_graph_packages.rs"));

#[derive(Deserialize)]
struct BundledToolSurface {
    entries: Vec<SurfaceToolDecl>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageRoleDeclaration {
    pub name: String,
    pub description: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageExternalDependency {
    pub service_id: String,
    pub description: String,
    pub repository_url: String,
    pub install_command: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphPackageManifest {
    pub manifest_version: u32,
    pub name: String,
    pub version: String,
    pub description: String,
    pub compiler_version: String,
    #[serde(default)]
    pub external_dependencies: Vec<PackageExternalDependency>,
    pub roles: Vec<PackageRoleDeclaration>,
    pub schemas: Vec<String>,
    pub intent: String,
    pub capabilities: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageCapabilityTemplate {
    pub capability_id: String,
    pub revision: String,
    pub role: String,
    pub behavior_asset: String,
    pub system_prompt_asset: String,
    pub task_asset: String,
    pub task_prompt_asset: String,
    pub tool_selection_asset: String,
    #[serde(default)]
    pub tool_surface_assets: Vec<String>,
    pub input_ports: Vec<PortSpec>,
    pub output_ports: Vec<PortSpec>,
    pub workspace_authority: WorkspaceAuthorityCeiling,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct GraphPackageCatalogEntry {
    pub name: String,
    pub version: String,
    pub description: String,
    pub package_digest: String,
    pub compiler_version: String,
    pub external_dependencies: Vec<PackageExternalDependency>,
    pub roles: Vec<PackageRoleDeclaration>,
    pub entries: Vec<EntryBinding>,
    pub results: Vec<ResultContract>,
    pub capabilities: Vec<PackageCapabilityTemplate>,
}

#[derive(Clone, Debug)]
pub struct BundledGraphPackage {
    pub manifest: GraphPackageManifest,
    pub intent: GraphIntent,
    pub capabilities: Vec<PackageCapabilityTemplate>,
    pub package_digest: String,
    asset_paths: Vec<String>,
}

impl BundledGraphPackage {
    pub fn asset(&self, path: &str) -> Result<&'static [u8]> {
        if !self.asset_paths.iter().any(|candidate| candidate == path) {
            anyhow::bail!(
                "asset {path:?} is not declared by package {}",
                self.manifest.name
            );
        }
        bundled_graph_package_asset(&self.manifest.name, path)
            .with_context(|| format!("bundled asset {path:?} is missing"))
    }

    pub fn asset_text(&self, path: &str) -> Result<&'static str> {
        std::str::from_utf8(self.asset(path)?)
            .with_context(|| format!("bundled asset {path:?} is not UTF-8"))
    }

    pub fn catalog_entry(&self) -> GraphPackageCatalogEntry {
        GraphPackageCatalogEntry {
            name: self.manifest.name.clone(),
            version: self.manifest.version.clone(),
            description: self.manifest.description.clone(),
            package_digest: self.package_digest.clone(),
            compiler_version: self.manifest.compiler_version.clone(),
            external_dependencies: self.manifest.external_dependencies.clone(),
            roles: self.manifest.roles.clone(),
            entries: self.intent.entries.clone(),
            results: self.intent.results.clone(),
            capabilities: self.capabilities.clone(),
        }
    }
}

fn digest_assets(package_name: &str, paths: &[String]) -> Result<String> {
    let mut hasher = Sha256::new();
    for path in paths {
        let bytes = bundled_graph_package_asset(package_name, path)
            .with_context(|| format!("bundled package references missing asset {path:?}"))?;
        hasher.update((path.len() as u64).to_be_bytes());
        hasher.update(path.as_bytes());
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn validate_tool_surface_asset(package_name: &str, path: &str) -> Result<()> {
    let bytes = bundled_graph_package_asset(package_name, path)
        .with_context(|| format!("bundled package references missing asset {path:?}"))?;
    let surface: BundledToolSurface = serde_json::from_slice(bytes)
        .with_context(|| format!("bundled tool surface asset {path:?} is malformed"))?;
    for entry in &surface.entries {
        entry
            .validate()
            .with_context(|| format!("bundled tool surface asset {path:?} is invalid"))?;
    }
    Ok(())
}

fn validate_tool_selection_asset(package_name: &str, path: &str) -> Result<()> {
    let bytes = bundled_graph_package_asset(package_name, path)
        .with_context(|| format!("bundled package references missing asset {path:?}"))?;
    let selection: serde_json::Value = serde_json::from_slice(bytes)
        .with_context(|| format!("bundled tool selection asset {path:?} is malformed"))?;
    let object = selection
        .as_object()
        .with_context(|| format!("bundled tool selection asset {path:?} must be an object"))?;

    if object.get("tool_policy_version")
        != Some(&serde_json::json!(crate::tool_surface::TOOL_POLICY_V1))
    {
        anyhow::bail!(
            "bundled tool selection asset {path:?} must declare tool_policy_version {:?}",
            crate::tool_surface::TOOL_POLICY_V1
        );
    }
    if !matches!(
        object.get("enable_goal_tools"),
        Some(serde_json::Value::Bool(_))
    ) {
        anyhow::bail!(
            "bundled tool selection asset {path:?} must explicitly declare boolean enable_goal_tools"
        );
    }
    if object.get("enable_goal_creation") != Some(&serde_json::Value::Bool(false)) {
        anyhow::bail!(
            "bundled tool selection asset {path:?} must explicitly disable enable_goal_creation"
        );
    }
    Ok(())
}

fn load_package(package_name: &str) -> Result<BundledGraphPackage> {
    let manifest_bytes = bundled_graph_package_asset(package_name, "manifest.json")
        .with_context(|| format!("bundled package {package_name:?} has no manifest"))?;
    let manifest: GraphPackageManifest = serde_json::from_slice(manifest_bytes)?;
    if manifest.name != package_name {
        anyhow::bail!(
            "bundled package directory {package_name:?} disagrees with manifest name {:?}",
            manifest.name
        );
    }
    if manifest.manifest_version != 1 {
        anyhow::bail!("unsupported bundled package manifest version");
    }
    if manifest.compiler_version != COMPILER_VERSION {
        anyhow::bail!(
            "package compiler {} does not match runtime {}",
            manifest.compiler_version,
            COMPILER_VERSION
        );
    }
    let intent: GraphIntent = serde_json::from_slice(
        bundled_graph_package_asset(package_name, &manifest.intent)
            .with_context(|| format!("bundled package intent {:?} is missing", manifest.intent))?,
    )?;
    let capabilities: Vec<PackageCapabilityTemplate> = serde_json::from_slice(
        bundled_graph_package_asset(package_name, &manifest.capabilities).with_context(|| {
            format!(
                "bundled package capabilities {:?} are missing",
                manifest.capabilities
            )
        })?,
    )?;
    let roles = manifest
        .roles
        .iter()
        .map(|role| role.name.as_str())
        .collect::<BTreeSet<_>>();
    if roles.len() != manifest.roles.len()
        || capabilities
            .iter()
            .any(|capability| !roles.contains(capability.role.as_str()))
    {
        anyhow::bail!("package capabilities reference missing or duplicate logical roles");
    }
    let mut assets = vec![
        "manifest.json".to_owned(),
        manifest.intent.clone(),
        manifest.capabilities.clone(),
    ];
    assets.extend(manifest.schemas.iter().cloned());
    for capability in &capabilities {
        validate_tool_selection_asset(package_name, &capability.tool_selection_asset)?;
        assets.extend([
            capability.behavior_asset.clone(),
            capability.system_prompt_asset.clone(),
            capability.task_asset.clone(),
            capability.task_prompt_asset.clone(),
            capability.tool_selection_asset.clone(),
        ]);
        for path in &capability.tool_surface_assets {
            validate_tool_surface_asset(package_name, path)?;
        }
        assets.extend(capability.tool_surface_assets.iter().cloned());
    }
    assets.sort();
    assets.dedup();
    let package_digest = digest_assets(package_name, &assets)?;
    Ok(BundledGraphPackage {
        manifest,
        intent,
        capabilities,
        package_digest,
        asset_paths: assets,
    })
}

pub fn load_bundled_graph_package(name: &str) -> Result<BundledGraphPackage> {
    if !BUNDLED_GRAPH_PACKAGE_NAMES.contains(&name) {
        anyhow::bail!("unknown bundled graph package {name:?}");
    }
    load_package(name)
}

pub fn graph_package_catalog() -> Result<Vec<GraphPackageCatalogEntry>> {
    BUNDLED_GRAPH_PACKAGE_NAMES
        .iter()
        .map(|name| Ok(load_package(name)?.catalog_entry()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document_config::{SurfaceToolDecl, WriteToolFieldFill};
    use crate::graph_pipeline::{compile_graph, CompilerPolicy, StageCapability};
    use serde_json::{json, Value};

    #[test]
    fn bundled_catalog_is_read_only_complete_and_compiler_valid() {
        let package = load_bundled_graph_package("code-review").unwrap();
        assert_eq!(graph_package_catalog().unwrap().len(), 2);
        assert!(package.package_digest.starts_with("sha256:"));
        for path in &package.asset_paths {
            assert!(!package.asset(path).unwrap().is_empty(), "{path}");
        }
        let capabilities = package
            .capabilities
            .iter()
            .map(|template| StageCapability {
                capability_id: template.capability_id.clone(),
                revision: template.revision.clone(),
                task_id: format!("fixture-task-{}", template.capability_id),
                input_ports: template.input_ports.clone(),
                output_ports: template.output_ports.clone(),
                allowed_callers: vec!["did:key:fixture".to_owned()],
            })
            .collect::<Vec<_>>();
        let plan = compile_graph(
            &package.intent,
            &capabilities,
            "did:key:fixture",
            &CompilerPolicy::default(),
        )
        .unwrap();
        assert_eq!(plan.nodes.len(), 4);
        assert_eq!(plan.results.len(), 2);
        assert_eq!(plan.entries[0].name, "review");
    }

    #[test]
    fn bundled_tool_selections_declare_current_policy_and_goal_authority() {
        for package_name in BUNDLED_GRAPH_PACKAGE_NAMES {
            let package = load_bundled_graph_package(package_name).unwrap();
            for capability in &package.capabilities {
                validate_tool_selection_asset(package_name, &capability.tool_selection_asset)
                    .unwrap();
            }
        }
    }

    #[test]
    fn web_deep_research_package_is_complete_and_compiler_valid() {
        let package = load_bundled_graph_package("web-deep-research").unwrap();
        assert!(package.package_digest.starts_with("sha256:"));
        assert_eq!(package.manifest.external_dependencies.len(), 1);
        let dependency = &package.manifest.external_dependencies[0];
        assert_eq!(dependency.service_id, "web-research-mcp");
        assert_eq!(dependency.install_command, "./scripts/stack install-mcp");
        for path in &package.asset_paths {
            assert!(!package.asset(path).unwrap().is_empty(), "{path}");
        }
        let capabilities = package
            .capabilities
            .iter()
            .map(|template| StageCapability {
                capability_id: template.capability_id.clone(),
                revision: template.revision.clone(),
                task_id: format!("fixture-task-{}", template.capability_id),
                input_ports: template.input_ports.clone(),
                output_ports: template.output_ports.clone(),
                allowed_callers: vec!["did:key:fixture".to_owned()],
            })
            .collect::<Vec<_>>();
        let plan = compile_graph(
            &package.intent,
            &capabilities,
            "did:key:fixture",
            &CompilerPolicy::default(),
        )
        .unwrap();
        assert_eq!(plan.nodes.len(), 4);
        assert_eq!(plan.results.len(), 6);
        assert_eq!(plan.entries[0].name, "research");
        for capability in &package.capabilities {
            let selection: serde_json::Value = serde_json::from_str(
                package
                    .asset_text(&capability.tool_selection_asset)
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(
                selection.get("enable_bash"),
                Some(&serde_json::json!(false))
            );
            assert_eq!(
                selection.get("tool_policy_version"),
                Some(&serde_json::json!(crate::tool_surface::TOOL_POLICY_V1)),
                "{} must use secure-default tool policy decoding",
                capability.tool_selection_asset
            );
            for disabled in [
                "enable_file_tools",
                "enable_memory",
                "enable_session_history_tool",
                "enable_context_budget",
                "enable_defra_query",
                "subagent_spawn_enabled",
                "subagent_steering_enabled",
                "subagent_background_enabled",
                "enable_self_config",
                "enable_lsp",
            ] {
                assert_eq!(
                    selection.get(disabled),
                    Some(&serde_json::json!(false)),
                    "{} unexpectedly enables {disabled}",
                    capability.tool_selection_asset
                );
            }
            assert_eq!(
                selection.get("command_execution_policy"),
                Some(&serde_json::Value::Null),
                "{} must not invent a command-policy enum when bash is disabled",
                capability.tool_selection_asset
            );
            let allowed_mcp_services = selection
                .get("allowed_mcp_service_ids")
                .and_then(serde_json::Value::as_array)
                .unwrap();
            let required_mcp_services = selection
                .get("required_mcp_service_ids")
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();
            let (meta_tools, services, surface) = match capability.capability_id.as_str() {
                "research-plan" => (true, vec!["web-research-mcp"], "research-plan-writes"),
                "research-investigate" => (
                    true,
                    vec!["web-research-mcp"],
                    "research-investigate-writes",
                ),
                "research-adjudicate" => (false, vec![], "research-adjudicate-io"),
                "research-report" => (false, vec![], "research-report-io"),
                other => panic!("unexpected capability {other}"),
            };
            assert_eq!(
                selection.get("enable_meta_tools"),
                Some(&serde_json::json!(meta_tools))
            );
            assert_eq!(
                allowed_mcp_services,
                &services
                    .into_iter()
                    .map(serde_json::Value::from)
                    .collect::<Vec<_>>()
            );
            let expected_required = if matches!(
                capability.capability_id.as_str(),
                "research-plan" | "research-investigate"
            ) {
                vec![serde_json::Value::from("web-research-mcp")]
            } else {
                Vec::new()
            };
            assert_eq!(required_mcp_services, expected_required);
            assert_eq!(
                selection
                    .get("datastore_tool_surface_ids")
                    .and_then(serde_json::Value::as_array),
                Some(&vec![serde_json::Value::from(surface)])
            );
            assert_eq!(
                capability.workspace_authority,
                WorkspaceAuthorityCeiling::None
            );
        }
    }

    #[test]
    fn web_deep_research_handoffs_are_typed_and_correlation_scoped() {
        let package = load_bundled_graph_package("web-deep-research").unwrap();
        for (asset, fields) in [
            (
                "tasks/research-plan-task/prompt.md",
                &[
                    "question",
                    "scope",
                    "freshness",
                    "audience",
                    "output_requirements",
                    "investigator_count",
                ][..],
            ),
            (
                "tasks/research-investigate-task/prompt.md",
                &[
                    "assignment_id",
                    "question",
                    "lens",
                    "instructions",
                    "query_plan",
                    "source_requirements",
                    "freshness",
                ][..],
            ),
            (
                "tasks/research-report-task/prompt.md",
                &[
                    "title",
                    "thesis",
                    "outline",
                    "synthesis",
                    "unresolved_questions",
                ][..],
            ),
        ] {
            let prompt = package.asset_text(asset).unwrap();
            for field in fields {
                assert!(
                    prompt.contains(&format!("{{{{ doc.{field} }}}}")),
                    "{asset} does not interpolate typed carrier field {field}"
                );
            }
        }
        let adjudicate_prompt = package
            .asset_text("tasks/research-adjudicate-task/prompt.md")
            .unwrap();
        assert!(adjudicate_prompt.contains("{{ group.correlation_value }}"));
        assert!(adjudicate_prompt.contains("{{ group.count }}"));
        assert!(!adjudicate_prompt.contains("event.group_size"));

        let expected_surface_tools = [
            (
                "datastore-tool-surfaces/research-plan-writes/object.json",
                vec!["write_research_assignment", "write_research_plan"],
            ),
            (
                "datastore-tool-surfaces/research-investigate-writes/object.json",
                vec![
                    "write_research_source",
                    "write_research_claim",
                    "write_research_evidence",
                    "write_research_investigation",
                ],
            ),
            (
                "datastore-tool-surfaces/research-adjudicate-io/object.json",
                vec![
                    "read_research_investigation",
                    "read_research_source",
                    "read_research_claim",
                    "read_research_evidence",
                    "write_research_claim_verdict",
                    "write_research_draft",
                ],
            ),
            (
                "datastore-tool-surfaces/research-report-io/object.json",
                vec![
                    "read_report_research_source",
                    "read_report_research_evidence",
                    "read_report_claim_verdict",
                    "write_research_result",
                ],
            ),
        ];
        for (asset, expected_tools) in expected_surface_tools {
            let surface: BundledToolSurface =
                serde_json::from_str(package.asset_text(asset).unwrap()).unwrap();
            assert_eq!(
                surface
                    .entries
                    .iter()
                    .map(SurfaceToolDecl::tool_name)
                    .collect::<Vec<_>>(),
                expected_tools,
                "unexpected authority in {asset}"
            );
            for entry in &surface.entries {
                let fields = match entry {
                    SurfaceToolDecl::Create(entry) => &entry.fields,
                    SurfaceToolDecl::Query(entry) => &entry.filter_fields,
                };
                let run_id = fields
                    .iter()
                    .find(|field| field.name == "run_id")
                    .unwrap_or_else(|| {
                        panic!("{} has no correlation filter/fill", entry.tool_name())
                    });
                assert!(!run_id.required, "{}", entry.tool_name());
                assert_eq!(
                    run_id.fill,
                    Some(WriteToolFieldFill::Correlation),
                    "{}",
                    entry.tool_name()
                );
            }
        }

        let investigator: BundledToolSurface = serde_json::from_str(
            package
                .asset_text("datastore-tool-surfaces/research-investigate-writes/object.json")
                .unwrap(),
        )
        .unwrap();
        for (tool_name, minimum_writes) in [
            ("write_research_source", 2),
            ("write_research_claim", 6),
            ("write_research_evidence", 6),
        ] {
            let SurfaceToolDecl::Create(decl) = investigator
                .entries
                .iter()
                .find(|entry| entry.tool_name() == tool_name)
                .unwrap_or_else(|| panic!("missing {tool_name}"))
            else {
                panic!("{tool_name} must be a create tool");
            };
            assert_eq!(
                decl.output_obligation
                    .as_ref()
                    .map(|obligation| obligation.minimum_writes),
                Some(minimum_writes),
                "{tool_name} must enforce its investigator minimum"
            );
        }
        let SurfaceToolDecl::Create(evidence_write) = investigator
            .entries
            .iter()
            .find(|entry| entry.tool_name() == "write_research_evidence")
            .unwrap()
        else {
            unreachable!()
        };
        assert!(evidence_write
            .fields
            .iter()
            .all(|field| !matches!(field.name.as_str(), "fetch_id" | "content_hash")));
        let evidence_schema = package
            .asset_text("schemas/research_evidence.graphql")
            .unwrap();
        assert!(!evidence_schema.contains("fetch_id"));
        assert!(!evidence_schema.contains("content_hash"));

        let all_assets = package
            .asset_paths
            .iter()
            .map(|path| package.asset_text(path).unwrap())
            .collect::<String>();
        assert!(!all_assets.contains("evidence_json"));
        for typed_field in [
            "verified_quote",
            "quote_verified",
            "evidence_id",
            "source_id",
            "fetch_id",
            "content_hash",
            "matched_query",
            "retrieval_queries",
            "search_engines",
            "candidate_relevance_score",
            "content_relevance_score",
            "extraction_method",
            "content_integrity_verified",
            "evidence_shortfall",
            "search_degradation",
            "retrieval_failures",
            "relationship",
            "evidence_summary",
        ] {
            assert!(all_assets.contains(typed_field), "missing {typed_field}");
        }
    }

    #[test]
    fn code_review_scan_writes_use_the_trigger_area_id() {
        let package = load_bundled_graph_package("code-review").unwrap();
        let surface: BundledToolSurface = serde_json::from_str(
            package
                .asset_text("datastore-tool-surfaces/review-scan-writes/object.json")
                .unwrap(),
        )
        .unwrap();
        for tool_name in ["write_candidate_finding", "write_scan_result"] {
            let entry = surface
                .entries
                .iter()
                .find(|entry| entry.tool_name() == tool_name)
                .unwrap();
            let SurfaceToolDecl::Create(entry) = entry else {
                panic!("{tool_name} must be a create tool");
            };
            let area_id = entry
                .fields
                .iter()
                .find(|field| field.name == "area_id")
                .unwrap();
            assert!(!area_id.required, "{tool_name}");
            assert_eq!(
                area_id.fill,
                Some(WriteToolFieldFill::SourceField("area_id".to_owned())),
                "{tool_name}"
            );
        }
    }

    #[test]
    fn code_review_evidence_handoff_is_compact_and_correlation_scoped() {
        let package = load_bundled_graph_package("code-review").unwrap();
        let surface: BundledToolSurface = serde_json::from_str(
            package
                .asset_text("datastore-tool-surfaces/review-recon-writes/object.json")
                .unwrap(),
        )
        .unwrap();
        let SurfaceToolDecl::Create(entry) = &surface.entries[0] else {
            panic!("review recon writer must be a create tool");
        };
        let repository_path = entry
            .fields
            .iter()
            .find(|field| field.name == "repository_path")
            .unwrap();
        assert!(!repository_path.required);
        assert_eq!(
            repository_path.fill,
            Some(WriteToolFieldFill::SourceField(
                "repository_path".to_owned()
            ))
        );
        let evidence_id = entry
            .fields
            .iter()
            .find(|field| field.name == "evidence_id")
            .unwrap();
        assert!(!evidence_id.required);
        assert_eq!(
            evidence_id.fill,
            Some(WriteToolFieldFill::SourceField("evidence_id".to_owned()))
        );
        assert!(entry.fields.iter().all(|field| field.name != "evidence"));
        let expected_total = entry
            .fields
            .iter()
            .find(|field| field.name == "expected_total")
            .unwrap();
        assert!(expected_total.required);
        assert_eq!(expected_total.fill, None);
        let recon_prompt = package
            .asset_text("tasks/review-recon-task/prompt.md")
            .unwrap();
        assert!(recon_prompt.contains("{{ doc.evidence_summary }}"));
        assert!(!recon_prompt.contains("{{ doc.evidence }}"));
        let scan_prompt = package
            .asset_text("tasks/review-scan-task/prompt.md")
            .unwrap();
        assert!(!scan_prompt.contains("{{ doc.evidence }}"));

        let scan_surface: BundledToolSurface = serde_json::from_str(
            package
                .asset_text("datastore-tool-surfaces/review-scan-writes/object.json")
                .unwrap(),
        )
        .unwrap();
        let manifest_tool = scan_surface
            .entries
            .iter()
            .find(|entry| entry.tool_name() == "read_review_evidence_manifest")
            .unwrap();
        let SurfaceToolDecl::Query(manifest_tool) = manifest_tool else {
            panic!("review evidence manifest must be a query tool");
        };
        assert_eq!(manifest_tool.collection, "CodeReviewEvidenceManifest");
        assert_eq!(manifest_tool.filter_fields.len(), 1);
        assert_eq!(manifest_tool.filter_fields[0].name, "evidence_id");
        assert_eq!(
            manifest_tool.filter_fields[0].fill,
            Some(WriteToolFieldFill::SourceField("evidence_id".to_owned()))
        );

        let page_tool = scan_surface
            .entries
            .iter()
            .find(|entry| entry.tool_name() == "read_review_evidence_page")
            .unwrap();
        let SurfaceToolDecl::Query(page_tool) = page_tool else {
            panic!("review evidence page must be a query tool");
        };
        assert_eq!(page_tool.collection, "CodeReviewEvidencePage");
        assert_eq!(page_tool.filter_fields.len(), 2);
        assert_eq!(page_tool.filter_fields[0].name, "evidence_id");
        assert_eq!(
            page_tool.filter_fields[0].fill,
            Some(WriteToolFieldFill::SourceField("evidence_id".to_owned()))
        );
        assert_eq!(page_tool.filter_fields[1].name, "page_index");
        assert!(page_tool.filter_fields[1].required);
        assert_eq!(page_tool.filter_fields[1].fill, None);
        let expected_chunk_fields = (0..16)
            .map(|chunk| format!("evidence_chunk_{chunk}"))
            .collect::<Vec<_>>();
        assert!(expected_chunk_fields
            .iter()
            .all(|field| page_tool.fields.contains(field)));

        let manifest_schema = package
            .asset_text("schemas/evidence_manifest.graphql")
            .unwrap();
        assert!(manifest_schema.starts_with("type CodeReviewEvidenceManifest {"));
        let page_schema = package.asset_text("schemas/evidence_page.graphql").unwrap();
        assert!(page_schema.starts_with("type CodeReviewEvidencePage {"));
        assert!(page_schema.contains("page_key: String @index(unique: true) @immutable"));
        assert!(page_schema.contains("evidence_chunk_15: String @immutable"));

        assert!(scan_prompt.contains("read_review_evidence_manifest"));
        assert!(scan_prompt.contains("read_review_evidence_page"));
        assert!(scan_prompt.contains("page_count - 1"));
        assert!(scan_prompt.contains("evidence_chunk_15"));
        assert!(!scan_prompt.contains("read_review_evidence_0"));
    }

    #[test]
    fn code_review_stages_use_role_specific_least_privilege_tools() {
        let package = load_bundled_graph_package("code-review").unwrap();
        for asset in [
            "tool-selections/review-recon-tools/object.json",
            "tool-selections/review-scan-tools/object.json",
        ] {
            let selection: Value =
                serde_json::from_str(package.asset_text(asset).unwrap()).unwrap();
            assert_eq!(selection["enable_file_tools"], false, "{asset}");
            assert_eq!(selection["file_tools_mode"], "Off", "{asset}");
            assert_eq!(selection["enable_bash"], false, "{asset}");
            assert_eq!(selection["bash_mode"], "Off", "{asset}");
            assert!(selection["command_execution_policy"].is_null(), "{asset}");
            assert!(selection["command_network_mode"].is_null(), "{asset}");
            assert_eq!(selection["enable_lsp"], false, "{asset}");
            assert_eq!(selection["enable_context_budget"], false, "{asset}");
            assert_eq!(selection["backgroundable_tool_names"], json!([]), "{asset}");
        }

        let asset = "tool-selections/review-verify-tools/object.json";
        let selection: Value = serde_json::from_str(package.asset_text(asset).unwrap()).unwrap();
        assert_eq!(selection["enable_file_tools"], true, "{asset}");
        assert_eq!(selection["file_tools_mode"], "ReadOnly", "{asset}");
        assert_eq!(selection["enable_bash"], true, "{asset}");
        assert_eq!(selection["bash_mode"], "ReadOnly", "{asset}");
        assert_eq!(
            selection["command_execution_policy"], "read_only",
            "{asset}"
        );
        assert_eq!(selection["command_network_mode"], "disabled", "{asset}");
        assert_eq!(selection["enable_lsp"], false, "{asset}");
        assert_eq!(selection["enable_context_budget"], true, "{asset}");
        assert_eq!(
            selection["backgroundable_tool_names"],
            json!(["bash"]),
            "{asset}"
        );
    }

    #[test]
    fn code_review_stages_are_durable_goal_controlled() {
        let package = load_bundled_graph_package("code-review").unwrap();
        for capability in &package.capabilities {
            let task: Value =
                serde_json::from_str(package.asset_text(&capability.task_asset).unwrap()).unwrap();
            assert!(
                task["goal_objective_template"]
                    .as_str()
                    .is_some_and(|objective| !objective.trim().is_empty()),
                "{} must provision a controller-owned durable goal",
                capability.task_asset
            );
            assert!(
                task["goal_token_budget"].is_null(),
                "{}",
                capability.task_asset
            );

            let selection: Value = serde_json::from_str(
                package
                    .asset_text(&capability.tool_selection_asset)
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(
                selection["enable_goal_tools"], true,
                "{}",
                capability.tool_selection_asset
            );
            assert_eq!(
                selection["enable_goal_creation"], false,
                "{}",
                capability.tool_selection_asset
            );

            let prompt = package.asset_text(&capability.task_prompt_asset).unwrap();
            assert!(
                prompt.contains("`update_goal`"),
                "{}",
                capability.task_prompt_asset
            );
            assert!(
                prompt.contains("`status=\"complete\"`"),
                "{}",
                capability.task_prompt_asset
            );
        }
    }
}
