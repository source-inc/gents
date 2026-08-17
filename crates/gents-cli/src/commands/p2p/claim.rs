use anyhow::{Context, Result};
use chrono::{SecondsFormat, Utc};
use gents::agent::p2p_reconcile::templates::{resolve_template, template_schema_digest};
use gents::graphql::escape_graphql_string;
use gents_protocol::bearer_token::{
    bearer_signing_payload, check_bearer_freshness, decode_bearer, BearerClaimRecord,
    BearerInviteToken,
};
use gents_protocol::network_token::NetworkRecord;
use serde_json::json;

use crate::cli::args::P2pClaimArgs;
use crate::shared::P2pReplicatorRequest;
use crate::{
    http_get_json, http_post_json, normalize_optional_string, print_json, resolve_config_access,
    resolve_graphql_endpoint,
};

use super::invite::resolve_home_identity;
use super::network_admin::{load_optional_network_record, write_agent_network};
use super::output::resolve_p2p_peer_id;
use super::p2p_http_client;
use super::pairings::{peer_pairing_exists, resolve_pairing_template, write_pairing_desired};

pub(super) async fn p2p_claim(args: P2pClaimArgs) -> Result<()> {
    let token = decode_bearer(&args.token)?;

    let identity = resolve_home_identity(args.home.as_deref())
        .context("resolving local agent identity for bearer claim signing")?;
    let payload = bearer_signing_payload(&token);
    let valid = identity
        .verify(&token.issuer_did, &payload, &token.sig)
        .await
        .with_context(|| {
            format!(
                "verifying bearer invite signature for issuer {}",
                token.issuer_did
            )
        })?;
    if !valid {
        anyhow::bail!(
            "bearer invite signature invalid for issuer {}",
            token.issuer_did
        );
    }

    check_bearer_freshness(&token, Utc::now())
        .context("bearer invite failed the freshness check (re-mint the QR)")?;

    check_token_network_authority(&token)?;
    let root_valid = identity
        .verify(
            &token.network.admin_did,
            &token.network.signing_payload(),
            &token.network.sig,
        )
        .await
        .context("verifying bearer invite network root signature")?;
    if !root_valid {
        anyhow::bail!(
            "bearer invite network root signature invalid for admin {}",
            token.network.admin_did
        );
    }

    let template = resolve_pairing_template(&token.template)?;
    let collections = super::join::template_collections(&template);
    let addresses = vec![token.ticket.clone()];
    let graphql = resolve_graphql_endpoint(args.graphql.as_deref(), args.home.as_deref())?;
    let (access, home_dir) =
        resolve_config_access(args.home.as_deref(), args.graphql.as_deref()).await?;

    let local_network = load_optional_network_record(&access)
        .await
        .context("loading local AgentNetwork before claim")?;
    check_local_network_match(local_network.as_ref(), &token)?;

    // Schema-digest preflight (issue #1122), before any pairing row is
    // written: a paired client whose bundled SDLs differ from the server's
    // reads fine but every document it authors is merge-rejected forever
    // with no signal anywhere the user looks. The issuer stamps a digest of
    // its template's SDLs into the (signed) invite token — recompute the
    // same digest from this build's local schema bundle and compare before
    // committing to the pairing.
    let scope_template = resolve_template(&template)
        .with_context(|| format!("resolving scope template {template} for schema digest"))?;
    let local_schema_digest = template_schema_digest(scope_template)
        .context("computing local schema digest for bearer claim")?;
    check_schema_digest(
        &template,
        &local_schema_digest,
        token.schema_digest.as_deref(),
        args.allow_schema_mismatch,
    )?;

    write_agent_network(&access, &token.network).await?;

    let existed = peer_pairing_exists(&access, &token.peer_id).await?;
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    let doc_id = write_pairing_desired(
        &access,
        &token.peer_id,
        Some(&token.issuer_did),
        &collections,
        &addresses,
        &template,
        &now,
    )
    .await?;

    let (claimant_node_id, claimant_address) = local_transport_info(&graphql).await;

    let mut record = BearerClaimRecord {
        token: args.token.trim().to_string(),
        claimant_did: identity.did().to_string(),
        claimant_node_id,
        claimant_address,
        claimed_at: now.clone(),
        sig: Vec::new(),
    };
    record.sig = identity
        .sign(&record.signing_payload())
        .await
        .context("signing bearer claim record")?;
    let claim_mutation = bearer_claim_create_mutation(&record);
    access
        .execute(&claim_mutation)
        .await
        .context("writing local PairingBearerClaim row")?;

    let client = p2p_http_client()?;
    let api_base = crate::graphql_access::graphql_api_base(&graphql)?;
    let request = P2pReplicatorRequest {
        collections: vec!["PairingBearerClaim".to_string()],
        addresses: vec![token.ticket.clone()],
        filters: Default::default(),
    };
    http_post_json(&client, &format!("{api_base}/p2p/replicators"), &request)
        .await
        .context(
            "installing the claim push replicator (is the local `gents serve` daemon running?)",
        )?;

    print_json(&json!({
        "status": if existed { "claim_submitted_pairing_exists" } else { "claim_submitted" },
        "home": home_dir,
        "graphql": graphql,
        "access_mode": access.mode(),
        "peer_id": token.peer_id,
        "issuer_did": token.issuer_did,
        "network_id": token.network_id,
        "claimant_did": identity.did(),
        "template": template,
        "collections": collections,
        "replicator_addresses": addresses,
        "doc_id": doc_id,
        "note": "the issuer daemon authors the membership grant when this claim replicates in",
    }))?;
    Ok(())
}

async fn local_transport_info(graphql: &str) -> (String, String) {
    use crate::http::version::{NodeIdentityResponse, P2pShareableAddressResponse};

    let Ok(client) = p2p_http_client() else {
        return (String::new(), String::new());
    };
    let Ok(api_base) = crate::graphql_access::graphql_api_base(graphql) else {
        return (String::new(), String::new());
    };
    let node_identity =
        http_get_json::<NodeIdentityResponse>(&client, &format!("{api_base}/node/identity"))
            .await
            .ok();
    let address = http_get_json::<P2pShareableAddressResponse>(
        &client,
        &format!("{api_base}/p2p/shareable-address"),
    )
    .await
    .ok()
    .and_then(|response| normalize_optional_string(response.address.as_deref()))
    .unwrap_or_default();
    let node_id = resolve_p2p_peer_id(
        node_identity.as_ref().and_then(|id| id.peer_id.as_deref()),
        (!address.is_empty()).then_some(address.as_str()),
        &[],
        None,
    )
    .unwrap_or_default();
    (node_id, address)
}

fn check_token_network_authority(token: &BearerInviteToken) -> Result<()> {
    if token.issuer_did != token.network.admin_did {
        anyhow::bail!(
            "bearer invite issuer {} is not the network admin {}; claim rejected",
            token.issuer_did,
            token.network.admin_did
        );
    }
    Ok(())
}

fn check_local_network_match(
    local: Option<&NetworkRecord>,
    token: &BearerInviteToken,
) -> Result<()> {
    let Some(local) = local else {
        return Ok(());
    };
    if local.network_id != token.network.network_id {
        anyhow::bail!(
            "bearer invite is for network {} but this node is already bound to network {}; \
             claim rejected",
            token.network.network_id,
            local.network_id
        );
    }
    if local.admin_did != token.network.admin_did {
        anyhow::bail!(
            "bearer invite network admin {} does not match local network admin {}; claim rejected",
            token.network.admin_did,
            local.admin_did
        );
    }
    Ok(())
}

/// Compares the local schema-bundle digest against the digest the issuer
/// stamped into the invite token. `remote_digest` is `None` for invites
/// minted by a server too old to compute one — back-compat, skip silently.
/// On a mismatch: bail naming both digests unless `allow_mismatch`, in which
/// case downgrade to a loud warning and proceed.
fn check_schema_digest(
    template_id: &str,
    local_digest: &str,
    remote_digest: Option<&str>,
    allow_mismatch: bool,
) -> Result<()> {
    let Some(remote_digest) = remote_digest else {
        return Ok(());
    };
    if remote_digest == local_digest {
        return Ok(());
    }
    if allow_mismatch {
        let message = format!(
            "schema mismatch: this build's bundled schemas for template '{template_id}' \
             (digest {local_digest}) differ from the server's (digest {remote_digest}); \
             documents you author may be silently rejected. Pairing anyway because \
             --allow-schema-mismatch was set."
        );
        tracing::warn!("{message}");
        eprintln!("{message}");
        return Ok(());
    }
    anyhow::bail!(
        "schema mismatch: this build's bundled schemas for template '{template_id}' (digest \
         {local_digest}) differ from the server's (digest {remote_digest}); documents you \
         author would be silently rejected. Update this build to match the server, or re-run \
         with --allow-schema-mismatch to pair anyway."
    );
}

fn bearer_claim_create_mutation(record: &BearerClaimRecord) -> String {
    let token = escape_graphql_string(&record.token);
    let claimant_did = escape_graphql_string(&record.claimant_did);
    let claimant_node_id = escape_graphql_string(&record.claimant_node_id);
    let claimant_address = escape_graphql_string(&record.claimant_address);
    let claimed_at = escape_graphql_string(&record.claimed_at);
    let binding_sig = escape_graphql_string(&bs58::encode(&record.sig).into_string());
    format!(
        r#"mutation {{
            create_PairingBearerClaim(input: {{
                token: "{token}",
                claimant_did: "{claimant_did}",
                claimant_node_id: "{claimant_node_id}",
                claimant_address: "{claimant_address}",
                claimed_at: "{claimed_at}",
                binding_sig: "{binding_sig}"
            }}) {{ _docID }}
        }}"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use gents_protocol::bearer_token::BEARER_TOKEN_VERSION;

    fn network(network_id: &str, admin_did: &str) -> NetworkRecord {
        NetworkRecord {
            network_id: network_id.into(),
            admin_did: admin_did.into(),
            display_name: "Net".into(),
            default_template: "network-control".into(),
            created_at: "2026-07-08T00:00:00Z".into(),
            sig: vec![1],
        }
    }

    fn bearer(network_rec: NetworkRecord, issuer_did: &str) -> BearerInviteToken {
        BearerInviteToken {
            v: BEARER_TOKEN_VERSION,
            issuer_did: issuer_did.into(),
            peer_id: "peer-issuer".into(),
            ticket: "/ticket/issuer".into(),
            nonce: "nonce".into(),
            network_id: network_rec.network_id.clone(),
            issued_at: "2026-07-08T00:00:00Z".into(),
            template: "conversation".into(),
            default_behavior_id: Some("default".into()),
            schema_digest: None,
            network: network_rec,
            sig: vec![2],
        }
    }

    #[test]
    fn claim_rejects_issuer_that_is_not_network_admin() {
        let token = bearer(network("default", "did:key:admin"), "did:key:imposter");
        let err = check_token_network_authority(&token)
            .unwrap_err()
            .to_string();
        assert!(err.contains("not the network admin"), "unexpected: {err}");

        let ok = bearer(network("default", "did:key:admin"), "did:key:admin");
        assert!(check_token_network_authority(&ok).is_ok());
    }

    #[test]
    fn claim_refuses_to_overwrite_a_different_local_network() {
        let token = bearer(network("net-b", "did:key:admin-b"), "did:key:admin-b");

        assert!(check_local_network_match(None, &token).is_ok());

        let local = network("net-b", "did:key:admin-b");
        assert!(check_local_network_match(Some(&local), &token).is_ok());

        let local = network("net-a", "did:key:admin-a");
        let err = check_local_network_match(Some(&local), &token)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("already bound to network"),
            "unexpected: {err}"
        );

        let local = network("net-b", "did:key:other-admin");
        let err = check_local_network_match(Some(&local), &token)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("does not match local network admin"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn bearer_claim_mutation_escapes_all_fields_and_emits_no_empty_lists() {
        let record = BearerClaimRecord {
            token: "dabear1-tok\"quoted".into(),
            claimant_did: "did:key:phone".into(),
            claimant_node_id: "peer-phone".into(),
            claimant_address: "/ticket/phone".into(),
            claimed_at: "2026-07-08T00:00:00Z".into(),
            sig: vec![1, 2, 3],
        };
        let mutation = bearer_claim_create_mutation(&record);
        assert!(mutation.contains("create_PairingBearerClaim"));
        assert!(mutation.contains("token: \"dabear1-tok\\\"quoted\""));
        assert!(mutation.contains("claimant_did: \"did:key:phone\""));
        assert!(mutation.contains("binding_sig: "));
        assert!(!mutation.contains("[]"));
    }

    #[test]
    fn schema_digest_mismatch_bails_naming_both_digests() {
        let err = check_schema_digest("machine", "localDigest1", Some("remoteDigest2"), false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("schema mismatch"), "unexpected: {err}");
        assert!(err.contains("machine"), "unexpected: {err}");
        assert!(err.contains("localDigest1"), "unexpected: {err}");
        assert!(err.contains("remoteDigest2"), "unexpected: {err}");
        assert!(
            err.contains("--allow-schema-mismatch"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn schema_digest_mismatch_with_allow_flag_proceeds() {
        let result = check_schema_digest("machine", "localDigest1", Some("remoteDigest2"), true);
        assert!(result.is_ok(), "expected override to proceed: {result:?}");
    }

    #[test]
    fn schema_digest_match_proceeds() {
        let result = check_schema_digest("machine", "sameDigest", Some("sameDigest"), false);
        assert!(result.is_ok());
    }

    #[test]
    fn missing_remote_schema_digest_proceeds_silently_for_back_compat() {
        let result = check_schema_digest("machine", "localDigest1", None, false);
        assert!(result.is_ok());
    }
}
