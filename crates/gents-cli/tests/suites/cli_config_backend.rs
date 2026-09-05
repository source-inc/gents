use crate::support::*;

use std::fs;
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::{json, Value};
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_backend_discover_models_supports_explicit_preset_probe() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("discover-openrouter-{}", Uuid::new_v4().simple());
    let raw_api_key = "discover-openrouter-key";
    let mock_endpoint =
        MockModelEndpoint::start_with_required_bearer(&model_name, Some(raw_api_key))?;

    let output = run_cli_json(
        &home_dir,
        &[
            "config",
            "backend",
            "discover-models",
            "--backend-preset",
            "openrouter",
            "--endpoint",
            mock_endpoint.endpoint(),
            "--api-key",
            raw_api_key,
        ],
    )?;

    assert_eq!(
        output.get("provider_kind").and_then(Value::as_str),
        Some("OpenRouter")
    );
    assert_eq!(
        output.get("endpoint").and_then(Value::as_str),
        Some(mock_endpoint.endpoint())
    );
    assert_eq!(
        output
            .pointer("/discovered_models/0")
            .and_then(Value::as_str),
        Some(model_name.as_str())
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_backend_set_preset_and_discover_models_from_backend_id() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let bootstrap_model = format!("bootstrap-model-{}", Uuid::new_v4().simple());
    let bootstrap_endpoint = MockModelEndpoint::start(&bootstrap_model)?;
    let discover_model = format!("discover-backend-id-{}", Uuid::new_v4().simple());
    let discover_api_key = "stored-openrouter-key";
    let discover_endpoint =
        MockModelEndpoint::start_with_required_bearer(&discover_model, Some(discover_api_key))?;

    let port = allocate_port()?;
    let agent_name = format!("cli-backend-preset-{}", Uuid::new_v4().simple());
    let graphql = graphql_url(port);

    let init = run_init_json(
        &home_dir,
        &[
            "--agent-name",
            &agent_name,
            "--model-name",
            &bootstrap_model,
            "--inference-url",
            bootstrap_endpoint.endpoint(),
        ],
    )?;
    let agent_did = agent_did_from_init(&init)?;
    let mut serve = spawn_server(&home_dir, port)?;
    wait_for_port(port, &mut serve)?;
    wait_for_runtime_ready(&graphql, &agent_did, Duration::from_secs(30)).await?;

    let backend_id = format!("{agent_name}-openrouter");
    let upsert = run_cli_json(
        &home_dir,
        &[
            "config",
            "backend",
            "set",
            "--graphql",
            &graphql,
            "--backend-id",
            &backend_id,
            "--name",
            "OpenRouter",
            "--backend-preset",
            "openrouter",
            "--endpoint",
            discover_endpoint.endpoint(),
            "--api-key",
            discover_api_key,
            "--max-concurrent",
            "2",
        ],
    )?;

    assert_eq!(
        upsert.get("backend_preset").and_then(Value::as_str),
        Some("openrouter")
    );
    assert_eq!(
        upsert.get("provider_kind").and_then(Value::as_str),
        Some("OpenRouter")
    );
    assert_eq!(
        upsert.get("endpoint").and_then(Value::as_str),
        Some(discover_endpoint.endpoint())
    );

    let discover = run_cli_json(
        &home_dir,
        &[
            "config",
            "backend",
            "discover-models",
            "--graphql",
            &graphql,
            "--backend-id",
            &backend_id,
        ],
    )?;

    assert_eq!(
        discover
            .pointer("/discovered_models/0")
            .and_then(Value::as_str),
        Some(discover_model.as_str())
    );
    assert_eq!(
        discover.get("provider_kind").and_then(Value::as_str),
        Some("OpenRouter")
    );
    assert_eq!(
        discover.get("models_written").and_then(Value::as_u64),
        Some(0),
        "without --write nothing is written: {discover}"
    );

    let backend_query = |id: &str| {
        format!(
            r#"{{
            InferenceBackend(filter: {{ backend_id: {{ _eq: "{}" }} }}, limit: 1) {{
                backend_id
                name
                provider_kind
                endpoint
                api_key
                api_key_env_var
                max_concurrent
                enabled
                probe_status
                models
            }}
        }}"#,
            escape_graphql_string(id),
        )
    };
    let backend_rows = graphql_query(&graphql, &backend_query(&backend_id)).await?;
    let backend = first_graphql_row(&backend_rows, "InferenceBackend")?;
    assert_eq!(backend.get("models"), Some(&json!(["default"])));
    assert_eq!(
        backend.get("provider_kind").and_then(Value::as_str),
        Some("OpenRouter")
    );
    assert_eq!(
        backend.get("endpoint").and_then(Value::as_str),
        Some(discover_endpoint.endpoint())
    );
    assert_eq!(
        backend.get("api_key").and_then(Value::as_str),
        Some(discover_api_key)
    );
    assert_eq!(backend.get("api_key_env_var").and_then(Value::as_str), None);

    let written = run_cli_json(
        &home_dir,
        &[
            "config",
            "backend",
            "discover-models",
            "--graphql",
            &graphql,
            "--backend-id",
            &backend_id,
            "--write",
        ],
    )?;
    assert_eq!(
        written.get("models_written").and_then(Value::as_u64),
        Some(1),
        "--write reports the persisted count: {written}"
    );

    let backend_rows = graphql_query(&graphql, &backend_query(&backend_id)).await?;
    let backend = first_graphql_row(&backend_rows, "InferenceBackend")?;
    assert_eq!(
        backend.get("models"),
        Some(&json!([discover_model])),
        "--write replaces models[] with the discovered catalog: {backend}"
    );
    assert_eq!(
        backend.get("name").and_then(Value::as_str),
        Some("OpenRouter")
    );
    assert_eq!(
        backend.get("endpoint").and_then(Value::as_str),
        Some(discover_endpoint.endpoint())
    );
    assert_eq!(
        backend.get("api_key").and_then(Value::as_str),
        Some(discover_api_key)
    );
    assert_eq!(
        backend.get("max_concurrent").and_then(Value::as_i64),
        Some(2)
    );
    assert_eq!(backend.get("enabled").and_then(Value::as_bool), Some(true));
    assert_eq!(
        backend.get("probe_status").and_then(Value::as_str),
        Some("healthy")
    );

    // Zero usable ids: the mock renders {"data":[{"id":""}]} and blank ids are
    // dropped, so --write must not touch models[] (an empty list would render
    // `models: null` and wipe the column).
    let empty_endpoint = MockModelEndpoint::start("")?;
    let empty_backend_id = format!("{agent_name}-empty");
    run_cli_json(
        &home_dir,
        &[
            "config",
            "backend",
            "set",
            "--graphql",
            &graphql,
            "--backend-id",
            &empty_backend_id,
            "--name",
            "Empty catalog",
            "--backend-preset",
            "openrouter",
            "--endpoint",
            empty_endpoint.endpoint(),
            "--api-key",
            "unused",
            "--max-concurrent",
            "1",
        ],
    )?;
    let skipped = run_cli_json(
        &home_dir,
        &[
            "config",
            "backend",
            "discover-models",
            "--graphql",
            &graphql,
            "--backend-id",
            &empty_backend_id,
            "--write",
        ],
    )?;
    assert_eq!(
        skipped.get("models_written").and_then(Value::as_u64),
        Some(0),
        "{skipped}"
    );
    assert!(
        skipped
            .get("write_skipped")
            .and_then(Value::as_str)
            .is_some(),
        "zero ids must report write_skipped: {skipped}"
    );
    let backend_rows = graphql_query(&graphql, &backend_query(&empty_backend_id)).await?;
    let backend = first_graphql_row(&backend_rows, "InferenceBackend")?;
    assert_eq!(
        backend.get("models"),
        Some(&json!(["default"])),
        "zero ids must leave models[] untouched: {backend}"
    );

    Ok(())
}

#[test]
fn config_backend_discover_models_write_requires_backend_id() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let stderr = run_cli_failure_stderr(
        tempdir.path(),
        &["config", "backend", "discover-models", "--write"],
    )?;
    assert!(
        stderr.contains("--write requires --backend-id"),
        "guard must fire before any server is needed: {stderr}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn imperative_config_writers_recreate_after_remove() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("imperative-recreate-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockModelEndpoint::start(&model_name)?;
    let port = allocate_port()?;
    let graphql = graphql_url(port);
    let agent_name = format!("cli-imperative-recreate-{}", Uuid::new_v4().simple());
    let init = run_init_json(
        &home_dir,
        &[
            "--agent-name",
            &agent_name,
            "--model-name",
            &model_name,
            "--inference-url",
            mock_endpoint.endpoint(),
        ],
    )?;
    let agent_did = agent_did_from_init(&init)?;
    let mut serve = spawn_server(&home_dir, port)?;
    wait_for_port(port, &mut serve)?;
    wait_for_runtime_ready(&graphql, &agent_did, Duration::from_secs(30)).await?;

    let selection_id = format!("recreate-tools-{}", Uuid::new_v4().simple());
    let tool_args = [
        "config",
        "tools",
        "set",
        "--graphql",
        &graphql,
        "--agent-did",
        &agent_did,
        "--selection-id",
        &selection_id,
        "--display-name",
        "Recreate tools",
    ];
    assert_remove_then_identical_set_recreates(
        &home_dir,
        &tool_args,
        &[
            "config",
            "tools",
            "rm",
            "--graphql",
            &graphql,
            "--id",
            &selection_id,
        ],
    )?;

    let backend_id = format!("recreate-backend-{}", Uuid::new_v4().simple());
    let backend_args = [
        "config",
        "backend",
        "set",
        "--graphql",
        &graphql,
        "--backend-id",
        &backend_id,
        "--name",
        "Recreate backend",
        "--backend-preset",
        "openrouter",
        "--endpoint",
        mock_endpoint.endpoint(),
        "--max-concurrent",
        "1",
    ];
    assert_remove_then_identical_set_recreates(
        &home_dir,
        &backend_args,
        &[
            "config",
            "backend",
            "rm",
            "--graphql",
            &graphql,
            "--id",
            &backend_id,
        ],
    )?;

    let profile_id = format!("recreate-profile-{}", Uuid::new_v4().simple());
    let profile_args = [
        "config",
        "profile",
        "set",
        "--graphql",
        &graphql,
        "--profile-id",
        &profile_id,
        "--display-name",
        "Recreate profile",
    ];
    assert_remove_then_identical_set_recreates(
        &home_dir,
        &profile_args,
        &[
            "config",
            "profile",
            "rm",
            "--graphql",
            &graphql,
            "--id",
            &profile_id,
        ],
    )?;

    Ok(())
}

fn assert_remove_then_identical_set_recreates(
    home_dir: &std::path::Path,
    set_args: &[&str],
    remove_args: &[&str],
) -> Result<()> {
    let first = run_cli_json(home_dir, set_args)?;
    let first_doc_id = first
        .get("doc_id")
        .and_then(Value::as_str)
        .context("first set output missing doc_id")?;

    run_cli_json(home_dir, remove_args)?;

    let second = run_cli_json(home_dir, set_args)?;
    let second_doc_id = second
        .get("doc_id")
        .and_then(Value::as_str)
        .context("second set output missing doc_id")?;
    assert_ne!(
        first_doc_id, second_doc_id,
        "recreate must mint a distinct content-addressed document"
    );
    Ok(())
}
