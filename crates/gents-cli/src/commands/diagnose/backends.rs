use std::time::Duration;

use gents::{discover_backend_models, BackendProviderKind};
use serde_json::{json, Value};

use crate::shared::ConfigExportBundle;

pub(super) async fn diagnose_backends(bundle: &ConfigExportBundle) -> Vec<Value> {
    let mut models_by_backend = std::collections::BTreeMap::<String, Vec<String>>::new();
    for behavior in &bundle.agent_behaviors {
        let Some(backend_id) = behavior.get("backend_id").and_then(Value::as_str) else {
            continue;
        };
        let Some(model_name) = behavior.get("model_name").and_then(Value::as_str) else {
            continue;
        };
        if backend_id.trim().is_empty() || model_name.trim().is_empty() {
            continue;
        }
        models_by_backend
            .entry(backend_id.to_string())
            .or_default()
            .push(model_name.to_string());
    }
    for models in models_by_backend.values_mut() {
        models.sort();
        models.dedup();
    }

    let mut reports = Vec::new();
    let present_backend_ids = bundle
        .inference_backends
        .iter()
        .filter_map(|backend| backend.get("backend_id").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect::<std::collections::BTreeSet<_>>();
    for backend in &bundle.inference_backends {
        reports.push(
            diagnose_backend(
                backend,
                models_by_backend
                    .get(
                        backend
                            .get("backend_id")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                    )
                    .cloned()
                    .unwrap_or_default(),
            )
            .await,
        );
    }
    for backend_id in models_by_backend.keys() {
        if !present_backend_ids.contains(backend_id) {
            reports.push(json!({
                "backend_id": backend_id,
                "ok": false,
                "error": format!("referenced backend {} is missing", backend_id),
                "required_models": models_by_backend.get(backend_id).cloned().unwrap_or_default(),
            }));
        }
    }
    reports.sort_by(|left, right| {
        let left_key = left
            .get("backend_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let right_key = right
            .get("backend_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        left_key.cmp(right_key)
    });
    reports
}

async fn diagnose_backend(backend: &Value, required_models: Vec<String>) -> Value {
    let backend_id = backend
        .get("backend_id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let provider_kind = match BackendProviderKind::parse_optional(
        backend.get("provider_kind").and_then(Value::as_str),
    ) {
        Ok(kind) => kind,
        Err(error) => {
            return json!({
                "backend_id": backend_id,
                "ok": false,
                "provider_kind": backend.get("provider_kind"),
                "error": error.to_string(),
            });
        }
    };
    let endpoint = backend
        .get("endpoint")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let enabled = backend
        .get("enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let probe_status = backend
        .get("probe_status")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let api_key_env_var = backend
        .get("api_key_env_var")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let raw_api_key = backend
        .get("api_key")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let mut ok = gents::document_configured_from_fields(enabled, &probe_status);
    let mut error = None::<String>;
    let mut note = None::<&'static str>;
    let mut discovered_models = Vec::<String>::new();

    let api_key = match (raw_api_key.as_ref(), api_key_env_var.as_deref()) {
        (Some(raw), Some(name)) => {
            ok = false;
            error = Some(format!(
                "backend {} sets both raw api_key and api_key_env_var {}",
                backend_id, name
            ));
            Some(raw.clone())
        }
        (Some(raw), None) => Some(raw.clone()),
        (None, Some(name)) => match std::env::var(name) {
            Ok(value) if !value.trim().is_empty() => Some(value),
            _ => {
                ok = false;
                error = Some(format!(
                    "required backend API key env var {} is not set",
                    name
                ));
                None
            }
        },
        (None, None) => None,
    };

    if ok {
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
        {
            Ok(client) => client,
            Err(build_error) => {
                ok = false;
                error = Some(format!("building probe client: {build_error}"));
                return json!({
                    "backend_id": backend_id,
                    "ok": ok,
                    "provider_kind": provider_kind.as_str(),
                    "endpoint": endpoint,
                    "enabled": enabled,
                    "probe_status": probe_status,
                    "api_key": raw_api_key.as_ref().map(|_| "<redacted>"),
                    "api_key_env_var": api_key_env_var,
                    "required_models": required_models,
                    "discovered_models": discovered_models,
                    "error": error,
                    "note": null,
                });
            }
        };
        // Mirrors the runtime prober (`backend_health`): agent-scoped OAuth
        // kinds are not discovered here (no credential is loaded); discovery is
        // the explicit `gents config backend discover-models`.
        if provider_kind.is_agent_scoped_oauth() {
            note = Some(OAUTH_CREDENTIAL_DISCOVERY_NOTE);
        } else {
            match discover_backend_models(
                &client,
                provider_kind,
                &endpoint,
                api_key.as_deref(),
                None,
            )
            .await
            {
                Ok(models) => {
                    discovered_models = models;
                    let missing_models = required_models
                        .iter()
                        .filter(|model| {
                            !discovered_models
                                .iter()
                                .any(|candidate| candidate == *model)
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    if !missing_models.is_empty() {
                        ok = false;
                        error = Some(format!(
                            "backend {} is missing required models: {}",
                            backend_id,
                            missing_models.join(", ")
                        ));
                    }
                }
                Err(request_error) => {
                    ok = false;
                    error = Some(format!("backend discovery failed: {}", request_error));
                }
            }
        }
    }

    json!({
        "backend_id": backend_id,
        "ok": ok,
        "provider_kind": provider_kind.as_str(),
        "endpoint": endpoint,
        "enabled": enabled,
        "probe_status": probe_status,
        "api_key": raw_api_key.as_ref().map(|_| "<redacted>"),
        "api_key_env_var": api_key_env_var,
        "required_models": required_models,
        "discovered_models": discovered_models,
        "note": note,
        "error": error,
    })
}

const OAUTH_CREDENTIAL_DISCOVERY_NOTE: &str =
    "OAuth credential backend: diagnose skips discovery (use `gents config backend discover-models`); health is the credential-expiry probe";

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::*;

    #[tokio::test]
    async fn oauth_backend_skips_discovery_and_stays_ok() {
        let backend = json!({
            "backend_id": "claude-max",
            "provider_kind": "ClaudeCliSubscription",
            "endpoint": "claude-cli://subscription",
            "enabled": true,
            "probe_status": "healthy",
        });
        let report = diagnose_backend(&backend, vec!["claude-sonnet-5".to_string()]).await;
        assert_eq!(report["ok"], Value::Bool(true), "{report}");
        assert_eq!(report["error"], Value::Null, "{report}");
        assert_eq!(
            report["note"],
            Value::String(OAUTH_CREDENTIAL_DISCOVERY_NOTE.to_string())
        );
        assert_eq!(report["discovered_models"], json!([]));
    }
}
