mod support;

use std::sync::{Arc, Mutex};

use anyhow::Result;
use gents::graphql::escape_graphql_string;
use gents::{
    AgentIdentity, DocumentRuntimeOptions, Gents, ProcessLifecycleObserver, ProcessLifecycleState,
    ToolCeiling,
};
use serde_json::Value;
use tokio::sync::watch;

use support::fixtures::{
    bind_default_behavior_backend, test_behavior_for_principal, test_identity, test_principal_for,
};
use support::mock_endpoint::MockModelEndpoint;
use support::test_db_with_identity;
use support::waits::wait_for_runtime_process_state;

#[derive(Default)]
struct RecordingObserver {
    states: Mutex<Vec<ProcessLifecycleState>>,
}

impl ProcessLifecycleObserver for RecordingObserver {
    fn on_process_state_change(&self, state: ProcessLifecycleState) {
        self.states
            .lock()
            .expect("recording observer mutex poisoned")
            .push(state);
    }
}

#[tokio::test]
async fn run_agent_uses_backend_specific_api_key_env_var_for_startup_probe() -> Result<()> {
    use std::ffi::OsString;
    use std::sync::LazyLock;

    static ENV_VAR_LOCK: LazyLock<tokio::sync::Mutex<()>> =
        LazyLock::new(|| tokio::sync::Mutex::new(()));

    struct TestEnvGuard {
        saved: Vec<(&'static str, Option<OsString>)>,
    }
    impl TestEnvGuard {
        fn new(names: &[&'static str]) -> Self {
            let saved = names
                .iter()
                .map(|name| (*name, std::env::var_os(name)))
                .collect();
            Self { saved }
        }
        fn set(&mut self, name: &'static str, value: &str) {
            unsafe {
                std::env::set_var(name, value);
            }
        }
    }
    impl Drop for TestEnvGuard {
        fn drop(&mut self) {
            for (name, value) in self.saved.iter().rev() {
                unsafe {
                    match value {
                        Some(value) => std::env::set_var(name, value),
                        None => std::env::remove_var(name),
                    }
                }
            }
        }
    }

    let _env_guard = ENV_VAR_LOCK.lock().await;
    let identity = Arc::new(test_identity("startup-probe-backend-auth"));
    let db = test_db_with_identity("startup-probe-backend-auth", identity.clone()).await;
    let node = db.node.clone();
    let mock_endpoint =
        MockModelEndpoint::start_with_required_bearer("default", Some("backend-key"))?;
    bind_default_behavior_backend(
        node.as_ref(),
        identity.did(),
        "backend-startup-auth",
        mock_endpoint.endpoint(),
    )
    .await;

    let escaped_backend_id = escape_graphql_string("backend-startup-auth");
    let mutation = format!(
        r#"mutation {{
            update_InferenceBackend(
                filter: {{ backend_id: {{ _eq: "{escaped_backend_id}" }} }},
                input: {{ api_key_env_var: "GENTS_TEST_RUNTIME_BACKEND_KEY" }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(!response.has_errors(), "{:?}", response.errors);

    let mut env = TestEnvGuard::new(&["GENTS_TEST_RUNTIME_BACKEND_KEY"]);
    env.set("GENTS_TEST_RUNTIME_BACKEND_KEY", "backend-key");
    let observer = Arc::new(RecordingObserver::default());
    let agent = Gents::from_default_behavior_documents(
        node.clone(),
        identity.clone(),
        DocumentRuntimeOptions {
            tool_ceiling: ToolCeiling::meta_only(),
            process_state_observer: Some(observer.clone()),
            ..Default::default()
        },
    )
    .await?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let run_task = tokio::spawn(agent.run(shutdown_rx));

    wait_for_runtime_process_state(node.as_ref(), identity.did(), "ready").await;
    let _ = shutdown_tx.send(true);
    run_task.await??;

    let observed = observer
        .states
        .lock()
        .expect("recording observer mutex poisoned")
        .clone();
    assert_eq!(
        observed,
        vec![
            ProcessLifecycleState::Recovering,
            ProcessLifecycleState::Ready,
            ProcessLifecycleState::ShuttingDown,
            ProcessLifecycleState::Shutdown,
        ]
    );

    Ok(())
}

#[tokio::test]
async fn openrouter_oneshot_uses_provider_request_preferences() -> Result<()> {
    use gents::BackendProviderKind;

    let identity = Arc::new(test_identity("openrouter-oneshot-provider-preferences"));
    let db =
        test_db_with_identity("openrouter-oneshot-provider-preferences", identity.clone()).await;
    let node = db.node.clone();
    let mock_endpoint = MockModelEndpoint::start_with_required_bearer(
        "openai/gpt-4o-mini",
        Some("openrouter-key"),
    )?;
    let principal = test_principal_for(identity, "openrouter-oneshot");
    let mut behavior = test_behavior_for_principal("openrouter-oneshot", principal);
    behavior.backend_id = Some("backend-openrouter".to_string());
    behavior.backend_provider_kind = BackendProviderKind::OpenRouter;
    behavior.backend_endpoint = mock_endpoint.endpoint().to_string();
    behavior.backend_api_key = Some("openrouter-key".to_string());
    behavior.model_name = "openai/gpt-4o-mini".to_string();

    let result = gents::run_openai_oneshot(node, &behavior, "Say hello in one sentence.").await?;
    assert_eq!(result.response_text, "mock response");

    let completion_request = mock_endpoint
        .recorded_requests()
        .into_iter()
        .find(|request| request.method == "POST" && request.path.ends_with("/chat/completions"))
        .expect("completion request should be recorded");
    let body: Value = serde_json::from_str(&completion_request.body)?;

    assert_eq!(body["provider"]["require_parameters"], true);
    assert_eq!(body["model"], "openai/gpt-4o-mini");

    Ok(())
}
