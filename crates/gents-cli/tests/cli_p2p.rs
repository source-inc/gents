mod support;
use support::*;

use std::fs;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde_json::Value;
use uuid::Uuid;

fn create_network(home: &std::path::Path, name: &str) -> Result<Value> {
    let out = run_cli_json(
        home,
        &[
            "p2p", "network", "create", "--name", name, "--output", "json",
        ],
    )?;
    assert_eq!(
        out.get("status").and_then(Value::as_str),
        Some("network_created"),
        "network create output: {out}"
    );
    Ok(out)
}

fn grant_member(home: &std::path::Path, member_did: &str) -> Result<Value> {
    let out = run_cli_json(
        home,
        &["p2p", "network", "grant", member_did, "--output", "json"],
    )?;
    assert_eq!(
        out.get("status").and_then(Value::as_str),
        Some("membership_granted"),
        "network grant output: {out}"
    );
    Ok(out)
}

fn mint_network_control_invite_for(home: &std::path::Path, member_did: &str) -> Result<Value> {
    run_cli_json(
        home,
        &[
            "p2p",
            "pairings",
            "invite",
            "--member-did",
            member_did,
            "--template",
            "network-control",
        ],
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p2p_invite_conversation_records_reciprocal_intent() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_a = tempdir.path().join("intent-a");
    let home_b = tempdir.path().join("intent-b");
    fs::create_dir_all(&home_a)?;
    fs::create_dir_all(&home_b)?;

    let model_name = format!("mock-p2p-intent-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockModelEndpoint::start(&model_name)?;

    let port_a = allocate_port()?;
    let agent_name_a = format!("cli-intent-a-{}", Uuid::new_v4().simple());
    let agent_name_b = format!("cli-intent-b-{}", Uuid::new_v4().simple());
    let graphql_a = graphql_url(port_a);

    let init_a = run_init_json(
        &home_a,
        &[
            "--agent-name",
            &agent_name_a,
            "--model-name",
            &model_name,
            mock_endpoint.endpoint(),
        ],
    )?;
    let init_b = run_init_json(
        &home_b,
        &[
            "--agent-name",
            &agent_name_b,
            "--model-name",
            &model_name,
            mock_endpoint.endpoint(),
        ],
    )?;
    let agent_did_a = agent_did_from_init(&init_a)?;
    let agent_did_b = agent_did_from_init(&init_b)?;

    let (mut serve_a, _readiness_a) = spawn_server_with_ready_json(
        &home_a,
        port_a,
        &[
            "--p2p-bind-addr",
            "127.0.0.1",
            "--p2p-port",
            "0",
            "--p2p-relay-mode",
            "disabled",
            "--p2p-discovery",
            "disabled",
        ],
        &[],
    )?;
    wait_for_port(port_a, &mut serve_a)?;
    wait_for_runtime_ready(&graphql_a, &agent_did_a, Duration::from_secs(30)).await?;

    create_network(&home_a, "Conversation Intent Fleet")?;
    grant_member(&home_a, &agent_did_b)?;

    let invite = run_cli_json(
        &home_a,
        &[
            "p2p",
            "pairings",
            "invite",
            "--member-did",
            &agent_did_b,
            "--template",
            "conversation",
        ],
    )?;
    assert_eq!(
        invite.get("status").and_then(Value::as_str),
        Some("invite_created"),
        "invite output: {invite}"
    );

    let escaped_member = escape_graphql_string(&agent_did_b);
    let response = graphql_query(
        &graphql_a,
        &format!(
            r#"query {{
                ReciprocalConversationIntent(filter: {{ member_did: {{ _eq: "{escaped_member}" }} }}) {{
                    member_did
                    template
                }}
            }}"#
        ),
    )
    .await?;
    let row = first_graphql_row(&response, "ReciprocalConversationIntent")?;
    assert_eq!(
        row.get("member_did").and_then(Value::as_str),
        Some(agent_did_b.as_str())
    );
    assert_eq!(
        row.get("template").and_then(Value::as_str),
        Some("conversation")
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p2p_bearer_claim_grants_membership_and_intent_end_to_end() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_a = tempdir.path().join("bearer-a");
    let home_b = tempdir.path().join("bearer-b");
    fs::create_dir_all(&home_a)?;
    fs::create_dir_all(&home_b)?;

    let model_name = format!("mock-p2p-bearer-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockModelEndpoint::start(&model_name)?;

    let port_a = allocate_port()?;
    let port_b = allocate_port()?;
    let agent_name_a = format!("cli-bearer-a-{}", Uuid::new_v4().simple());
    let agent_name_b = format!("cli-bearer-b-{}", Uuid::new_v4().simple());
    let graphql_a = graphql_url(port_a);
    let graphql_b = graphql_url(port_b);

    let init_a = run_init_json(
        &home_a,
        &[
            "--agent-name",
            &agent_name_a,
            "--model-name",
            &model_name,
            mock_endpoint.endpoint(),
        ],
    )?;
    let init_b = run_init_json(
        &home_b,
        &[
            "--agent-name",
            &agent_name_b,
            "--model-name",
            &model_name,
            mock_endpoint.endpoint(),
        ],
    )?;
    let agent_did_a = agent_did_from_init(&init_a)?;
    let agent_did_b = agent_did_from_init(&init_b)?;

    let p2p_flags = [
        "--p2p-bind-addr",
        "127.0.0.1",
        "--p2p-port",
        "0",
        "--p2p-relay-mode",
        "disabled",
        "--p2p-discovery",
        "disabled",
    ];
    let (mut serve_a, _ready_a) = spawn_server_with_ready_json(&home_a, port_a, &p2p_flags, &[])?;
    wait_for_port(port_a, &mut serve_a)?;
    wait_for_runtime_ready(&graphql_a, &agent_did_a, Duration::from_secs(30)).await?;
    let (mut serve_b, _ready_b) = spawn_server_with_ready_json(&home_b, port_b, &p2p_flags, &[])?;
    wait_for_port(port_b, &mut serve_b)?;
    wait_for_runtime_ready(&graphql_b, &agent_did_b, Duration::from_secs(30)).await?;

    create_network(&home_a, "Bearer Claim Fleet")?;

    let invite = run_cli_json(
        &home_a,
        &[
            "p2p",
            "pairings",
            "invite",
            "--bearer",
            "--template",
            "conversation",
        ],
    )?;
    assert_eq!(
        invite.get("status").and_then(Value::as_str),
        Some("bearer_invite_created"),
        "invite output: {invite}"
    );
    let token = invite
        .get("token")
        .and_then(Value::as_str)
        .context("bearer invite output missing token")?
        .to_string();
    assert!(token.starts_with("dabear1-"), "unexpected token: {token}");

    // Issue #1122: the invite carries a schema-digest fingerprint of the
    // template's SDLs, computed the same way the claimant will recompute it
    // locally. Both processes here share one binary/schema bundle, so the
    // claim below exercises the ordinary (matching-digest) path end to end.
    let decoded_token = gents_protocol::bearer_token::decode_bearer(&token)
        .context("decoding minted bearer invite token")?;
    let conversation_template =
        gents::agent::p2p_reconcile::templates::resolve_template("conversation")
            .context("resolving conversation scope template")?;
    let expected_digest =
        gents::agent::p2p_reconcile::templates::template_schema_digest(conversation_template)
            .context("computing expected schema digest")?;
    assert_eq!(
        decoded_token.schema_digest,
        Some(expected_digest),
        "bearer invite should carry the conversation template's schema digest"
    );

    let claim = run_cli_json(&home_b, &["p2p", "pairings", "claim", &token])?;
    assert_eq!(
        claim.get("status").and_then(Value::as_str),
        Some("claim_submitted"),
        "claim output: {claim}"
    );
    assert_eq!(
        claim.get("claimant_did").and_then(Value::as_str),
        Some(agent_did_b.as_str())
    );

    let escaped_b = escape_graphql_string(&agent_did_b);
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        let memberships = graphql_query(
            &graphql_a,
            &format!(
                r#"query {{
                    NetworkMembership(filter: {{ member_did: {{ _eq: "{escaped_b}" }} }}) {{
                        member_did
                        status
                    }}
                    ReciprocalConversationIntent(filter: {{ member_did: {{ _eq: "{escaped_b}" }} }}) {{
                        member_did
                    }}
                    ConsumedInviteNonce(filter: {{ claimant_did: {{ _eq: "{escaped_b}" }} }}) {{
                        claimant_did
                    }}
                }}"#
            ),
        )
        .await?;
        let granted = first_graphql_row(&memberships, "NetworkMembership")
            .ok()
            .map(|row| row.get("status").and_then(Value::as_str) == Some("active"))
            .unwrap_or(false);
        let intent = first_graphql_row(&memberships, "ReciprocalConversationIntent").is_ok();
        let burned = first_graphql_row(&memberships, "ConsumedInviteNonce").is_ok();
        if granted && intent && burned {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "bearer claim did not converge on issuer: granted={granted} intent={intent} burned={burned}; last response: {memberships}"
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let nonce_rows = graphql_query(
        &graphql_a,
        &format!(
            r#"query {{
                ConsumedInviteNonce(filter: {{ claimant_did: {{ _eq: "{escaped_b}" }} }}) {{
                    claimant_did
                    issuer_did
                }}
            }}"#
        ),
    )
    .await?;
    let row = first_graphql_row(&nonce_rows, "ConsumedInviteNonce")?;
    assert_eq!(
        row.get("issuer_did").and_then(Value::as_str),
        Some(agent_did_a.as_str())
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p2p_invite_is_single_use_replay_rejected() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_a = tempdir.path().join("replay-a");
    let home_b = tempdir.path().join("replay-b");
    fs::create_dir_all(&home_a)?;
    fs::create_dir_all(&home_b)?;

    let model_name = format!("mock-p2p-replay-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockModelEndpoint::start(&model_name)?;

    let port_a = allocate_port()?;
    let port_b = allocate_port()?;
    let agent_name_a = format!("cli-replay-a-{}", Uuid::new_v4().simple());
    let agent_name_b = format!("cli-replay-b-{}", Uuid::new_v4().simple());
    let graphql_a = graphql_url(port_a);
    let graphql_b = graphql_url(port_b);

    let init_a = run_init_json(
        &home_a,
        &[
            "--agent-name",
            &agent_name_a,
            "--model-name",
            &model_name,
            mock_endpoint.endpoint(),
        ],
    )?;
    let init_b = run_init_json(
        &home_b,
        &[
            "--agent-name",
            &agent_name_b,
            "--model-name",
            &model_name,
            mock_endpoint.endpoint(),
        ],
    )?;
    let agent_did_a = agent_did_from_init(&init_a)?;
    let agent_did_b = agent_did_from_init(&init_b)?;

    let (mut serve_a, readiness_a) = spawn_server_with_ready_json(
        &home_a,
        port_a,
        &[
            "--p2p-bind-addr",
            "127.0.0.1",
            "--p2p-port",
            "0",
            "--p2p-relay-mode",
            "disabled",
            "--p2p-discovery",
            "disabled",
        ],
        &[],
    )?;
    let (mut serve_b, _readiness_b) = spawn_server_with_ready_json(
        &home_b,
        port_b,
        &[
            "--p2p-bind-addr",
            "127.0.0.1",
            "--p2p-port",
            "0",
            "--p2p-relay-mode",
            "disabled",
            "--p2p-discovery",
            "disabled",
        ],
        &[],
    )?;
    wait_for_port(port_a, &mut serve_a)?;
    wait_for_port(port_b, &mut serve_b)?;
    wait_for_runtime_ready(&graphql_a, &agent_did_a, Duration::from_secs(30)).await?;
    wait_for_runtime_ready(&graphql_b, &agent_did_b, Duration::from_secs(30)).await?;

    let peer_id_a = readiness_a
        .get("p2p_peer_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("A readiness JSON missing p2p_peer_id: {readiness_a}"))?;

    create_network(&home_a, "Replay Fleet")?;
    grant_member(&home_a, &agent_did_b)?;

    let invite_a = mint_network_control_invite_for(&home_a, &agent_did_b)?;
    let token_a = invite_a
        .get("token")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("invite A missing token: {invite_a}"))?
        .to_string();

    let join_one = run_cli_json(&home_b, &["p2p", "pairings", "join", &token_a])?;
    assert_eq!(
        join_one.get("status").and_then(Value::as_str),
        Some("pairing_joined"),
        "first join should succeed: {join_one}"
    );
    assert_eq!(
        join_one.get("peer_id").and_then(Value::as_str),
        Some(peer_id_a)
    );

    let stderr = run_cli_failure_stderr(&home_b, &["p2p", "pairings", "join", &token_a])?;
    let lowered = stderr.to_lowercase();
    assert!(
        lowered.contains("replay") || lowered.contains("already used"),
        "second join should be rejected as a replay; stderr was:\n{stderr}"
    );

    Ok(())
}

#[test]
fn p2p_pairings_manage_desired_rows_locally() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home = tempdir.path().join("pairings-agent");
    fs::create_dir_all(&home)?;
    run_init_json(
        &home,
        &["--identity-only", "--agent-name", "pairings-agent"],
    )?;

    let set = run_cli_json(
        &home,
        &[
            "p2p",
            "pairings",
            "set",
            "--peer",
            "peer-one",
            "--did",
            "did:key:peer-one",
            "--address",
            "/ip4/127.0.0.1/tcp/4001/p2p/peer-one",
        ],
    )?;
    assert_eq!(
        set.get("status").and_then(Value::as_str),
        Some("pairing_set")
    );
    assert_eq!(
        set.get("access_mode").and_then(Value::as_str),
        Some("local")
    );
    assert_eq!(set.get("peer_id").and_then(Value::as_str), Some("peer-one"));

    let list = run_cli_json(&home, &["p2p", "pairings", "list", "--output", "json"])?;
    assert_eq!(list.get("count").and_then(Value::as_u64), Some(1));
    let row = list
        .get("pairings")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .ok_or_else(|| anyhow!("pairings list missing row: {list}"))?;
    assert_eq!(row.get("peer_id").and_then(Value::as_str), Some("peer-one"));
    assert_eq!(
        row.get("agent_did").and_then(Value::as_str),
        Some("did:key:peer-one")
    );
    assert!(row
        .get("collections")
        .and_then(Value::as_array)
        .is_some_and(|rows| rows.iter().any(|row| row.as_str() == Some("AgentRequest"))));
    assert!(row
        .get("collections")
        .and_then(Value::as_array)
        .is_some_and(|rows| rows.iter().any(|row| row.as_str() == Some("AgentResponse"))));
    assert!(row
        .get("profiles")
        .and_then(Value::as_array)
        .is_none_or(|rows| rows.is_empty()));
    assert_eq!(row.get("connected").and_then(Value::as_bool), Some(false));
    assert_eq!(row.get("subscribed").and_then(Value::as_bool), Some(false));
    assert_eq!(row.get("replicating").and_then(Value::as_bool), Some(false));

    let table = run_cli_text(&home, &["p2p", "pairings", "list", "--output", "table"])?;
    assert!(table.contains("PEER"));
    assert!(table.contains("DID"));
    assert!(table.contains("PROFILES"));
    assert!(table.contains("CONNECTED"));
    assert!(table.contains("SUBSCRIBED"));
    assert!(table.contains("REPLICATING"));

    let remove = run_cli_json(&home, &["p2p", "pairings", "unpair", "--peer", "peer-one"])?;
    assert_eq!(
        remove.get("status").and_then(Value::as_str),
        Some("pairing_removed")
    );
    assert_eq!(remove.get("removed_count").and_then(Value::as_u64), Some(1));

    let list = run_cli_json(&home, &["p2p", "pairings", "list", "--output", "json"])?;
    assert_eq!(list.get("count").and_then(Value::as_u64), Some(0));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p2p_connects_two_local_servers_via_operator_commands() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_a = tempdir.path().join("amy");
    let home_b = tempdir.path().join("coding");
    fs::create_dir_all(&home_a)?;
    fs::create_dir_all(&home_b)?;

    let model_name = format!("mock-p2p-connect-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockModelEndpoint::start(&model_name)?;

    let port_a = allocate_port()?;
    let port_b = allocate_port()?;
    let agent_name_a = format!("cli-amy-{}", Uuid::new_v4().simple());
    let agent_name_b = format!("cli-coding-{}", Uuid::new_v4().simple());
    let graphql_a = graphql_url(port_a);
    let graphql_b = graphql_url(port_b);

    let init_a = run_init_json(
        &home_a,
        &[
            "--agent-name",
            &agent_name_a,
            "--model-name",
            &model_name,
            mock_endpoint.endpoint(),
        ],
    )?;
    let init_b = run_init_json(
        &home_b,
        &[
            "--agent-name",
            &agent_name_b,
            "--model-name",
            &model_name,
            mock_endpoint.endpoint(),
        ],
    )?;
    let agent_did_a = agent_did_from_init(&init_a)?;
    let agent_did_b = agent_did_from_init(&init_b)?;

    let (mut serve_a, readiness_a) = spawn_server_with_ready_json(
        &home_a,
        port_a,
        &[
            "--p2p-bind-addr",
            "127.0.0.1",
            "--p2p-port",
            "0",
            "--p2p-relay-mode",
            "disabled",
            "--p2p-discovery",
            "disabled",
        ],
        &[],
    )?;
    let (mut serve_b, readiness_b) = spawn_server_with_ready_json(
        &home_b,
        port_b,
        &[
            "--p2p-bind-addr",
            "127.0.0.1",
            "--p2p-port",
            "0",
            "--p2p-relay-mode",
            "disabled",
            "--p2p-discovery",
            "disabled",
        ],
        &[],
    )?;
    wait_for_port(port_a, &mut serve_a)?;
    wait_for_port(port_b, &mut serve_b)?;
    wait_for_runtime_ready(&graphql_a, &agent_did_a, Duration::from_secs(30)).await?;
    wait_for_runtime_ready(&graphql_b, &agent_did_b, Duration::from_secs(30)).await?;

    let peer_id_a = readiness_a
        .get("p2p_peer_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("Amy readiness JSON missing p2p_peer_id: {readiness_a}"))?;
    let peer_id_b = readiness_b
        .get("p2p_peer_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("Coding readiness JSON missing p2p_peer_id: {readiness_b}"))?;
    let peer_addr_a = readiness_a
        .get("p2p_listen_addresses")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("Amy readiness JSON missing P2P listen address: {readiness_a}"))?;

    let connect = run_cli_json(&home_b, &["p2p", "admin", "connect", "--peer", peer_addr_a])?;
    assert_eq!(
        connect.get("status").and_then(Value::as_str),
        Some("connect_requested")
    );

    let status_b = wait_for_connected_peer(&home_b, peer_id_a, Duration::from_secs(20)).await?;
    let status_a = wait_for_connected_peer(&home_a, peer_id_b, Duration::from_secs(20)).await?;
    assert!(status_b
        .get("p2p_connected_peers")
        .and_then(Value::as_array)
        .is_some_and(|rows| rows
            .iter()
            .filter_map(Value::as_str)
            .any(|row| row.contains(peer_id_a))));
    assert!(status_a
        .get("p2p_connected_peers")
        .and_then(Value::as_array)
        .is_some_and(|rows| rows
            .iter()
            .filter_map(Value::as_str)
            .any(|row| row.contains(peer_id_b))));

    let peers_b = run_cli_json(&home_b, &["p2p", "peers"])?;
    assert_eq!(peers_b.get("count").and_then(Value::as_u64), Some(1));

    let collections_add = run_cli_json(
        &home_b,
        &[
            "p2p",
            "admin",
            "collections",
            "add",
            "--profile",
            "chat-requests",
        ],
    )?;
    assert_eq!(
        collections_add.get("status").and_then(Value::as_str),
        Some("collections_added")
    );
    assert!(collections_add
        .get("collections")
        .and_then(Value::as_array)
        .is_some_and(|rows| rows.iter().any(|row| row.as_str() == Some("AgentRequest"))));

    let replicator_add = run_cli_json(
        &home_b,
        &[
            "p2p",
            "admin",
            "replicators",
            "add",
            "--peer",
            peer_addr_a,
            "--profile",
            "chat-requests",
        ],
    )?;
    assert_eq!(
        replicator_add.get("status").and_then(Value::as_str),
        Some("replicator_added")
    );

    let diagnose_b = run_cli_json(&home_b, &["p2p", "diagnose"])?;
    assert_eq!(
        diagnose_b
            .pointer("/checks/p2p/info/ok")
            .and_then(Value::as_bool),
        Some(true)
    );
    for path in [
        "/checks/p2p/info/ok",
        "/checks/p2p/shareable_address/ok",
        "/checks/p2p/peers/ok",
        "/checks/p2p/collections/ok",
        "/checks/p2p/replicators/ok",
        "/checks/p2p/documents/ok",
    ] {
        assert!(
            diagnose_b
                .pointer(path)
                .and_then(Value::as_bool)
                .is_some_and(|ok| ok),
            "expected successful diagnostic at {path}: {diagnose_b}"
        );
    }
    assert!(diagnose_b
        .pointer("/checks/p2p/collections/value")
        .and_then(Value::as_array)
        .is_some_and(|rows| !rows.is_empty()));
    assert!(diagnose_b
        .pointer("/checks/p2p/replicators/value")
        .and_then(Value::as_array)
        .is_some_and(|rows| !rows.is_empty()));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p2p_pairings_set_writes_desired_row_for_runtime_reconcile() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_a = tempdir.path().join("parent-agent");
    let home_b = tempdir.path().join("child-agent");
    fs::create_dir_all(&home_a)?;
    fs::create_dir_all(&home_b)?;

    let model_name = format!("mock-p2p-pair-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockModelEndpoint::start(&model_name)?;

    let port_a = allocate_port()?;
    let port_b = allocate_port()?;
    let agent_name_a = format!("cli-parent-{}", Uuid::new_v4().simple());
    let agent_name_b = format!("cli-child-{}", Uuid::new_v4().simple());
    let graphql_a = graphql_url(port_a);
    let graphql_b = graphql_url(port_b);

    let init_a = run_init_json(
        &home_a,
        &[
            "--agent-name",
            &agent_name_a,
            "--model-name",
            &model_name,
            mock_endpoint.endpoint(),
        ],
    )?;
    let init_b = run_init_json(
        &home_b,
        &[
            "--agent-name",
            &agent_name_b,
            "--model-name",
            &model_name,
            mock_endpoint.endpoint(),
        ],
    )?;
    let agent_did_a = agent_did_from_init(&init_a)?;
    let agent_did_b = agent_did_from_init(&init_b)?;

    let (mut serve_a, readiness_a) = spawn_server_with_ready_json(
        &home_a,
        port_a,
        &[
            "--p2p-bind-addr",
            "127.0.0.1",
            "--p2p-port",
            "0",
            "--p2p-relay-mode",
            "disabled",
            "--p2p-discovery",
            "disabled",
        ],
        &[],
    )?;
    let (mut serve_b, readiness_b) = spawn_server_with_ready_json(
        &home_b,
        port_b,
        &[
            "--p2p-bind-addr",
            "127.0.0.1",
            "--p2p-port",
            "0",
            "--p2p-relay-mode",
            "disabled",
            "--p2p-discovery",
            "disabled",
        ],
        &[],
    )?;
    wait_for_port(port_a, &mut serve_a)?;
    wait_for_port(port_b, &mut serve_b)?;
    wait_for_runtime_ready(&graphql_a, &agent_did_a, Duration::from_secs(30)).await?;
    wait_for_runtime_ready(&graphql_b, &agent_did_b, Duration::from_secs(30)).await?;

    let peer_id_a = readiness_a
        .get("p2p_peer_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("Parent readiness JSON missing p2p_peer_id: {readiness_a}"))?;
    let _peer_id_b = readiness_b
        .get("p2p_peer_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("Child readiness JSON missing p2p_peer_id: {readiness_b}"))?;
    let peer_addr_a = readiness_a
        .get("p2p_listen_addresses")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(Value::as_str)
        .ok_or_else(|| {
            anyhow!("Parent readiness JSON missing P2P listen address: {readiness_a}")
        })?;
    let pair_b = run_cli_json(
        &home_b,
        &[
            "p2p",
            "pairings",
            "set",
            "--did",
            agent_did_a.as_str(),
            "--address",
            peer_addr_a,
        ],
    )?;
    assert_eq!(
        pair_b.get("status").and_then(Value::as_str),
        Some("pairing_set"),
        "child pairings set status: {pair_b}"
    );
    assert_eq!(
        pair_b.get("peer_id").and_then(Value::as_str),
        Some(peer_id_a),
        "peer id should be derived from the shareable address: {pair_b}"
    );
    assert_eq!(
        pair_b.get("agent_did").and_then(Value::as_str),
        Some(agent_did_a.as_str())
    );
    assert!(
        pair_b
            .get("collections")
            .and_then(Value::as_array)
            .is_some_and(|rows| rows.iter().any(|r| r.as_str() == Some("AgentRequest"))),
        "pairings set output missing AgentRequest in collections: {pair_b}"
    );
    assert!(
        pair_b.get("note").and_then(Value::as_str).is_some(),
        "pairings set output missing runtime reconcile note: {pair_b}"
    );

    let row = peer_pairing_row(&graphql_b, peer_id_a).await?;
    assert_eq!(
        row.get("agent_did").and_then(Value::as_str),
        Some(agent_did_a.as_str())
    );
    assert!(row
        .get("replicator_addresses")
        .and_then(Value::as_array)
        .is_some_and(|addresses| addresses
            .iter()
            .any(|address| address.as_str() == Some(peer_addr_a))));
    assert!(row
        .get("profiles")
        .and_then(Value::as_array)
        .is_none_or(|profiles| profiles.is_empty()));

    let list = run_cli_json(&home_b, &["p2p", "pairings", "list", "--output", "json"])?;
    assert_eq!(list.get("count").and_then(Value::as_u64), Some(1));
    let table = run_cli_text(&home_b, &["p2p", "pairings", "list", "--output", "table"])?;
    assert!(table.contains("PEER"));
    assert!(table.contains("CONNECTED"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p2p_invite_join_round_trips_pairing_rows() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_a = tempdir.path().join("invite-a");
    let home_b = tempdir.path().join("invite-b");
    fs::create_dir_all(&home_a)?;
    fs::create_dir_all(&home_b)?;

    let model_name = format!("mock-p2p-invite-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockModelEndpoint::start(&model_name)?;

    let port_a = allocate_port()?;
    let port_b = allocate_port()?;
    let agent_name_a = format!("cli-invite-a-{}", Uuid::new_v4().simple());
    let agent_name_b = format!("cli-invite-b-{}", Uuid::new_v4().simple());
    let graphql_a = graphql_url(port_a);
    let graphql_b = graphql_url(port_b);

    let init_a = run_init_json(
        &home_a,
        &[
            "--agent-name",
            &agent_name_a,
            "--model-name",
            &model_name,
            mock_endpoint.endpoint(),
        ],
    )?;
    let init_b = run_init_json(
        &home_b,
        &[
            "--agent-name",
            &agent_name_b,
            "--model-name",
            &model_name,
            mock_endpoint.endpoint(),
        ],
    )?;
    let agent_did_a = agent_did_from_init(&init_a)?;
    let agent_did_b = agent_did_from_init(&init_b)?;

    let (mut serve_a, readiness_a) = spawn_server_with_ready_json(
        &home_a,
        port_a,
        &[
            "--p2p-bind-addr",
            "127.0.0.1",
            "--p2p-port",
            "0",
            "--p2p-relay-mode",
            "disabled",
            "--p2p-discovery",
            "disabled",
        ],
        &[],
    )?;
    let (mut serve_b, readiness_b) = spawn_server_with_ready_json(
        &home_b,
        port_b,
        &[
            "--p2p-bind-addr",
            "127.0.0.1",
            "--p2p-port",
            "0",
            "--p2p-relay-mode",
            "disabled",
            "--p2p-discovery",
            "disabled",
        ],
        &[],
    )?;
    wait_for_port(port_a, &mut serve_a)?;
    wait_for_port(port_b, &mut serve_b)?;
    wait_for_runtime_ready(&graphql_a, &agent_did_a, Duration::from_secs(30)).await?;
    wait_for_runtime_ready(&graphql_b, &agent_did_b, Duration::from_secs(30)).await?;

    let peer_id_a = readiness_a
        .get("p2p_peer_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("A readiness JSON missing p2p_peer_id: {readiness_a}"))?;
    assert!(
        readiness_b
            .get("p2p_peer_id")
            .and_then(Value::as_str)
            .is_some(),
        "B readiness JSON missing p2p_peer_id: {readiness_b}"
    );

    create_network(&home_a, "Invite Fleet")?;
    grant_member(&home_a, &agent_did_b)?;

    let invite_a = mint_network_control_invite_for(&home_a, &agent_did_b)?;
    assert_eq!(
        invite_a.get("status").and_then(Value::as_str),
        Some("invite_created")
    );
    assert_eq!(
        invite_a.get("peer_id").and_then(Value::as_str),
        Some(peer_id_a)
    );
    assert_eq!(
        invite_a.get("did").and_then(Value::as_str),
        Some(agent_did_a.as_str())
    );
    let token_a = invite_a
        .get("token")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("invite A missing token: {invite_a}"))?;

    let join_b = run_cli_json(&home_b, &["p2p", "pairings", "join", token_a])?;
    assert_eq!(
        join_b.get("status").and_then(Value::as_str),
        Some("pairing_joined"),
        "join B output: {join_b}"
    );
    assert_eq!(
        join_b.get("peer_id").and_then(Value::as_str),
        Some(peer_id_a)
    );
    assert_eq!(
        join_b.get("agent_did").and_then(Value::as_str),
        Some(agent_did_a.as_str())
    );
    assert_eq!(
        join_b.get("member_did").and_then(Value::as_str),
        Some(agent_did_b.as_str())
    );
    assert!(
        join_b.get("reciprocal_token").is_none(),
        "v5 joins no longer mint reciprocal tokens: {join_b}"
    );

    let row_b = peer_pairing_row(&graphql_b, peer_id_a).await?;
    assert_eq!(
        row_b.get("agent_did").and_then(Value::as_str),
        Some(agent_did_a.as_str())
    );

    let applied_b =
        wait_for_pairing_applied(&graphql_b, peer_id_a, Duration::from_secs(90)).await?;
    assert!(
        applied_b
            .get("collections")
            .and_then(Value::as_array)
            .is_some_and(|rows| rows.iter().any(|row| row.as_str() == Some("AgentNetwork"))),
        "B applied row missing network-control collections after joining: {applied_b}"
    );

    Ok(())
}
