use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{SecondsFormat, Utc};
use flate2::{write::GzEncoder, Compression};
use gents::agent::p2p_reconcile::templates::{resolve_template, template_schema_digest};
use gents::{graphql::escape_graphql_string, AgentIdentity, KeyIdentity};
use gents_protocol::bearer_token::{
    bearer_signing_payload, encode_bearer, BearerInviteToken, BEARER_TOKEN_VERSION,
};
use gents_protocol::network_token::{MembershipRecord, NetworkRecord};
use gents_protocol::pairing_token::{encode as encode_invite, signing_payload, InviteToken};
use serde_json::json;

use crate::cli::args::P2pInviteArgs;
use crate::config_writes::ConfigAccess;
use crate::{
    graphql_rows, http_get_json, normalize_optional_string, print_json, read_init_config,
    read_runtime_state, resolve_agent_did, resolve_config_access, resolve_graphql_endpoint,
    resolve_home_dir,
};

use super::network_admin::{load_membership_record, load_single_network_record};
use super::output::resolve_p2p_peer_id;
use super::pairings::resolve_pairing_template;

const BEARER_QR_MAGIC: &[u8] = b"dabear1z\0";

pub(super) async fn p2p_invite(args: P2pInviteArgs) -> Result<()> {
    if args.bearer {
        return p2p_invite_bearer(args).await;
    }
    let graphql = resolve_graphql_endpoint(args.graphql.as_deref(), args.home.as_deref())?;
    let home_dir = resolve_home_dir(args.home.as_deref());
    let template = resolve_pairing_template(&args.template)?;
    let member_did = args
        .member_did
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .context("p2p pairings invite requires --member-did for v5 membership-gated invites")?;
    let (access, _) = resolve_config_access(args.home.as_deref(), args.graphql.as_deref()).await?;
    let network = load_single_network_record(&access)
        .await
        .context("loading local AgentNetwork for v5 invite")?;
    let grant = load_membership_record(&access, &network.network_id, member_did)
        .await?
        .with_context(|| format!("no NetworkMembership grant found for {member_did}"))?;
    validate_invite_grant(&network, &grant, member_did)?;

    let identity = resolve_home_identity(args.home.as_deref())
        .context("resolving local agent identity for invite signing")?;
    if identity.did() != network.admin_did {
        anyhow::bail!(
            "local DID {} is not network admin {}; only admin-issued v5 invites are supported",
            identity.did(),
            network.admin_did
        );
    }
    let token = current_invite_token_signed(
        args.home.as_deref(),
        &graphql,
        &template,
        identity.as_ref(),
        grant,
        network,
    )
    .await?;
    record_reciprocal_conversation_intent(&access, member_did, &template).await?;
    let encoded = encode_invite(&token)?;

    print_json(&json!({
        "status": "invite_created",
        "home": home_dir,
        "graphql": graphql,
        "token": encoded,
        "peer_id": token.peer_id,
        "issuer_did": token.issuer_did,
        "did": token.issuer_did,
        "network_id": token.network_id,
        "template": token.template,
        "ticket": token.ticket,
        "join_command": format!("gents p2p pairings join {encoded}"),
    }))?;
    Ok(())
}

async fn p2p_invite_bearer(args: P2pInviteArgs) -> Result<()> {
    let graphql = resolve_graphql_endpoint(args.graphql.as_deref(), args.home.as_deref())?;
    let home_dir = resolve_home_dir(args.home.as_deref());
    let template = resolve_pairing_template(&args.template)?;
    let (access, _) = resolve_config_access(args.home.as_deref(), args.graphql.as_deref()).await?;
    let network = load_single_network_record(&access)
        .await
        .context("loading local AgentNetwork for bearer invite")?;
    let identity = resolve_home_identity(args.home.as_deref())
        .context("resolving local agent identity for bearer invite signing")?;
    if identity.did() != network.admin_did {
        anyhow::bail!(
            "local DID {} is not network admin {}; only admin-issued bearer invites are supported",
            identity.did(),
            network.admin_did
        );
    }

    let default_behavior_id = if template == "conversation" {
        Some(
            load_default_behavior_id(&access, identity.did())
                .await
                .context("loading signed default behavior routing hint")?,
        )
    } else {
        None
    };
    // Schema-digest preflight (issue #1122): fingerprint the SDLs this
    // build has compiled in for the exact collections this template pushes,
    // so a claimant whose bundled schemas have drifted can refuse to pair
    // loudly instead of silently black-holing every document it authors.
    // Can't live as a new field on `PairingBearerClaim`/`BearerPairingReady`
    // — both are in `gents_migration::CLIENT_AUTHORED_COLLECTIONS`
    // (crates/gents-migration/src/registry.rs:568-610), so an SDL change
    // there would force a baseline re-pin (fresh_apply_parity.rs) that
    // breaks exactly the stale clients this feature warns about. The signed,
    // pre-pairing invite token is the only channel that reaches the
    // claimant before it authors anything.
    let scope_template = resolve_template(&template)
        .with_context(|| format!("resolving scope template {template} for schema digest"))?;
    let schema_digest = Some(
        template_schema_digest(scope_template)
            .context("computing schema digest for bearer invite")?,
    );

    let (peer_id, ticket) = resolve_invite_transport(args.home.as_deref(), &graphql).await?;
    let mut token = BearerInviteToken {
        v: BEARER_TOKEN_VERSION,
        issuer_did: identity.did().to_string(),
        peer_id,
        ticket,
        nonce: mint_nonce(),
        network_id: network.network_id.clone(),
        issued_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        template: template.clone(),
        default_behavior_id,
        schema_digest,
        network,
        sig: Vec::new(),
    };
    token.sig = identity
        .sign(&bearer_signing_payload(&token))
        .await
        .context("signing bearer invite token")?;
    let encoded = encode_bearer(&token)?;

    if args.qr {
        let payload = compact_bearer_qr_payload(&token)?;
        let code = qrcode::QrCode::with_error_correction_level(&payload, qrcode::EcLevel::L)
            .context("encoding bearer invite token as a QR code")?;
        let rendered = code
            .render::<qrcode::render::unicode::Dense1x2>()
            .quiet_zone(true)
            .build();
        eprintln!("{rendered}");
        eprintln!("scan within 5 minutes; the invite is single-use");
    }

    print_json(&json!({
        "status": "bearer_invite_created",
        "home": home_dir,
        "graphql": graphql,
        "token": encoded,
        "peer_id": token.peer_id,
        "issuer_did": token.issuer_did,
        "network_id": token.network_id,
        "template": token.template,
        "ticket": token.ticket,
        "expires_in": "5m",
        "note": "single-use; the issuer daemon must be running to process the claim — if it is down past the 5m window, mint a fresh invite",
        "claim_command": format!("gents p2p pairings claim {encoded}"),
    }))?;
    Ok(())
}

async fn load_default_behavior_id(access: &ConfigAccess, agent_did: &str) -> Result<String> {
    let agent_did = escape_graphql_string(agent_did);
    let query = format!(
        r#"{{
            AgentPrincipal(
                filter: {{ agent_did: {{ _eq: "{agent_did}" }} }},
                limit: 1
            ) {{
                default_behavior_id
            }}
        }}"#
    );
    let rows = graphql_rows(access, "AgentPrincipal", &query).await?;
    rows.first()
        .and_then(|row| row.get("default_behavior_id"))
        .and_then(serde_json::Value::as_str)
        .and_then(|value| normalize_optional_string(Some(value)))
        .context("conversation bearer invite requires AgentPrincipal.default_behavior_id")
}

fn compact_bearer_qr_payload(token: &BearerInviteToken) -> Result<Vec<u8>> {
    let mut cbor = Vec::new();
    ciborium::ser::into_writer(token, &mut cbor)
        .context("encoding bearer invite for compact QR")?;

    let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
    encoder
        .write_all(&cbor)
        .context("compressing bearer invite for compact QR")?;
    let compressed = encoder
        .finish()
        .context("finishing compact bearer invite QR")?;

    let mut payload = Vec::with_capacity(BEARER_QR_MAGIC.len() + compressed.len());
    payload.extend_from_slice(BEARER_QR_MAGIC);
    payload.extend_from_slice(&compressed);
    Ok(payload)
}

async fn resolve_invite_transport(home: Option<&Path>, graphql: &str) -> Result<(String, String)> {
    use crate::http::version::{NodeIdentityResponse, P2pShareableAddressResponse};

    let live = async {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .context("building P2P invite HTTP client")?;
        let api_base = crate::graphql_access::graphql_api_base(graphql)?;
        let node_identity =
            http_get_json::<NodeIdentityResponse>(&client, &format!("{api_base}/node/identity"))
                .await
                .ok();
        let shareable_address: P2pShareableAddressResponse =
            http_get_json(&client, &format!("{api_base}/p2p/shareable-address"))
                .await
                .context("loading shareable P2P address")?;
        let ticket = normalize_optional_string(shareable_address.address.as_deref())
            .context("runtime did not report a shareable P2P address")?;
        let peer_id = resolve_p2p_peer_id(
            node_identity.as_ref().and_then(|id| id.peer_id.as_deref()),
            Some(&ticket),
            &[],
            None,
        )
        .context("runtime reported a shareable P2P address but no usable peer id")?;
        Ok::<(String, String), anyhow::Error>((peer_id, ticket))
    }
    .await;

    match live {
        Ok(transport) => Ok(transport),
        Err(live_err) => {
            let home_dir = resolve_home_dir(home);
            let Some(runtime_state) = read_runtime_state(&home_dir)? else {
                return Err(live_err);
            };
            if runtime_state.graphql != graphql {
                return Err(live_err);
            }
            let Some(peer_id) = normalize_optional_string(runtime_state.p2p_peer_id.as_deref())
            else {
                return Err(live_err);
            };
            let Some(ticket) = runtime_state
                .p2p_listen_addresses
                .iter()
                .find_map(|address| normalize_optional_string(Some(address.as_str())))
            else {
                return Err(live_err);
            };
            Ok((peer_id, ticket))
        }
    }
}

async fn current_invite_token_signed(
    home: Option<&Path>,
    graphql: &str,
    template: &str,
    identity: &dyn AgentIdentity,
    grant: MembershipRecord,
    network: NetworkRecord,
) -> Result<InviteToken> {
    let home_dir = resolve_home_dir(home);
    let mut token = match build_live_token(home, graphql, template, identity, &grant, &network)
        .await
    {
        Ok(t) => t,
        Err(live_err) => {
            match build_persisted_token(&home_dir, graphql, template, identity, &grant, &network)? {
                Some(t) => t,
                None => return Err(live_err),
            }
        }
    };

    let payload = signing_payload(&token);
    let sig = identity
        .sign(&payload)
        .await
        .context("signing pairing invite token")?;
    token.sig = sig;
    Ok(token)
}

fn mint_nonce() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn build_persisted_token(
    home_dir: &Path,
    graphql: &str,
    template: &str,
    identity: &dyn AgentIdentity,
    grant: &MembershipRecord,
    network: &NetworkRecord,
) -> Result<Option<InviteToken>> {
    let Some(runtime_state) = read_runtime_state(home_dir)? else {
        return Ok(None);
    };
    if runtime_state.graphql != graphql {
        return Ok(None);
    }
    let Some(peer_id) = normalize_optional_string(runtime_state.p2p_peer_id.as_deref()) else {
        return Ok(None);
    };
    let Some(ticket) = runtime_state
        .p2p_listen_addresses
        .iter()
        .find_map(|address| normalize_optional_string(Some(address.as_str())))
    else {
        return Ok(None);
    };

    Ok(Some(InviteToken {
        v: 5,
        issuer_did: identity.did().to_string(),
        peer_id,
        ticket,
        nonce: mint_nonce(),
        network_id: network.network_id.clone(),
        issued_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        template: template.to_string(),
        grant: grant.clone(),
        network: network.clone(),
        sig: Vec::new(),
    }))
}

async fn build_live_token(
    home: Option<&Path>,
    graphql: &str,
    template: &str,
    identity: &dyn AgentIdentity,
    grant: &MembershipRecord,
    network: &NetworkRecord,
) -> Result<InviteToken> {
    use crate::http::version::{NodeIdentityResponse, P2pShareableAddressResponse};

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .context("building P2P invite HTTP client")?;
    let api_base = crate::graphql_access::graphql_api_base(graphql)?;
    let node_identity =
        http_get_json::<NodeIdentityResponse>(&client, &format!("{api_base}/node/identity"))
            .await
            .ok();
    let shareable_address: P2pShareableAddressResponse =
        http_get_json(&client, &format!("{api_base}/p2p/shareable-address"))
            .await
            .context("loading shareable P2P address")?;
    let ticket = normalize_optional_string(shareable_address.address.as_deref())
        .context("runtime did not report a shareable P2P address")?;
    let peer_id = resolve_p2p_peer_id(
        node_identity.as_ref().and_then(|id| id.peer_id.as_deref()),
        Some(&ticket),
        &[],
        None,
    )
    .context("runtime reported a shareable P2P address but no usable peer id")?;

    let issuer_did = {
        let id_did = identity.did();
        if id_did.is_empty() {
            resolve_agent_did(home, None).context("resolving local agent DID for invite")?
        } else {
            id_did.to_string()
        }
    };

    Ok(InviteToken {
        v: 5,
        issuer_did,
        peer_id,
        ticket,
        nonce: mint_nonce(),
        network_id: network.network_id.clone(),
        issued_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        template: template.to_string(),
        grant: grant.clone(),
        network: network.clone(),
        sig: Vec::new(),
    })
}

async fn record_reciprocal_conversation_intent(
    access: &ConfigAccess,
    member_did: &str,
    template: &str,
) -> Result<()> {
    if !gents::agent::p2p_reconcile::templates::conversation_like(template) {
        return Ok(());
    }

    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    let mutation = reciprocal_conversation_intent_upsert_mutation(member_did, template, &now);
    access
        .execute(&mutation)
        .await
        .context("recording ReciprocalConversationIntent for conversation invite")?;
    tracing::debug!(
        member_did = %member_did,
        template = %template,
        "recorded reciprocal conversation intent for invite"
    );
    Ok(())
}

fn reciprocal_conversation_intent_upsert_mutation(
    member_did: &str,
    template: &str,
    now: &str,
) -> String {
    let member_did = escape_graphql_string(member_did);
    let template = escape_graphql_string(template);
    let now = escape_graphql_string(now);
    format!(
        r#"mutation {{
            upsert_ReciprocalConversationIntent(
                filter: {{ member_did: {{ _eq: "{member_did}" }} }},
                add: {{
                    member_did: "{member_did}",
                    template: "{template}",
                    created_at: "{now}",
                    updated_at: "{now}"
                }},
                update: {{
                    template: "{template}",
                    updated_at: "{now}"
                }}
            ) {{ _docID }}
        }}"#
    )
}

fn validate_invite_grant(
    network: &NetworkRecord,
    grant: &MembershipRecord,
    member_did: &str,
) -> Result<()> {
    if grant.network_id != network.network_id {
        anyhow::bail!(
            "NetworkMembership grant is for network {} but AgentNetwork is {}",
            grant.network_id,
            network.network_id
        );
    }
    if grant.member_did != member_did {
        anyhow::bail!(
            "NetworkMembership grant is for {} but invite requested {member_did}",
            grant.member_did
        );
    }
    if grant.status.trim() != "active" {
        anyhow::bail!(
            "NetworkMembership grant for {member_did} is not active (status={})",
            grant.status
        );
    }
    Ok(())
}

/// Load the local agent identity from the home dir's init config.
///
/// Supports file-key (the common case).  macOS keychain / Secure Enclave
/// identities cannot be signed from an offline CLI sub-command today; those
/// paths surface a clear error.
pub(super) fn resolve_home_identity(home: Option<&Path>) -> Result<Arc<dyn AgentIdentity>> {
    let home_dir = resolve_home_dir(home);
    let Some(config) = read_init_config(&home_dir)? else {
        anyhow::bail!(
            "no init config found in {}; run `gents init` first",
            home_dir.display()
        )
    };

    let backend = config
        .identity_backend
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or("file");

    match backend {
        "file" | "" => {
            let key_path = config
                .key_path
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| crate::default_key_path(&home_dir, &config.agent_name));
            let identity = KeyIdentity::load_or_create(&key_path, None)
                .context("loading agent identity key for invite signing")?;
            Ok(Arc::new(identity))
        }
        other => anyhow::bail!(
            "identity backend {other:?} is not supported for offline invite signing; \
             start `gents server` and use `--graphql` to connect"
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use flate2::read::GzDecoder;
    use gents_protocol::bearer_token::{encode_bearer, BearerInviteToken, BEARER_TOKEN_VERSION};
    use gents_protocol::network_token::{MembershipRecord, NetworkRecord};

    use super::*;

    fn network_record() -> NetworkRecord {
        NetworkRecord {
            network_id: "default".to_string(),
            admin_did: "did:key:agent-a".to_string(),
            display_name: "Default".to_string(),
            default_template: "network-control".to_string(),
            created_at: "2026-06-13T00:00:00Z".to_string(),
            sig: vec![1, 2, 3],
        }
    }

    fn grant_record() -> MembershipRecord {
        MembershipRecord {
            network_id: "default".to_string(),
            member_did: "did:key:agent-b".to_string(),
            status: "active".to_string(),
            granted_at: "2026-06-13T00:00:00Z".to_string(),
            revoked_at: String::new(),
            sig: vec![4, 5, 6],
        }
    }

    fn bearer_token() -> BearerInviteToken {
        BearerInviteToken {
            v: BEARER_TOKEN_VERSION,
            issuer_did: "did:key:z6MkiRC5mMbJM45SmvLhmv2MadX2KzkXhRqJwVYE5k3ThDyJ".to_string(),
            peer_id: "775ad8b54cfff922733f96d4f5f7e1a3bb59e9031a32087040a644f9cdf67d3d"
                .to_string(),
            ticket: "endpointab3vvwfvjt77sitth6lnj5px4gr3wwpjamndecdqictej6on6z6t2aqbabsekbcp5bdqcagavaawr2ch".to_string(),
            nonce: "9d2fe907-e5d6-48ed-af44-c603f0a89a1e".to_string(),
            network_id: "net-Euv46XiYtc8knZqM7cJBAE".to_string(),
            issued_at: "2026-07-23T21:30:00Z".to_string(),
            template: "conversation".to_string(),
            default_behavior_id: Some("default".to_string()),
            network: NetworkRecord {
                network_id: "net-Euv46XiYtc8knZqM7cJBAE".to_string(),
                admin_did:
                    "did:key:z6MkiRC5mMbJM45SmvLhmv2MadX2KzkXhRqJwVYE5k3ThDyJ".to_string(),
                display_name: "amygdala".to_string(),
                default_template: "conversation".to_string(),
                created_at: "2026-07-23T20:48:52Z".to_string(),
                sig: vec![7; 64],
            },
            sig: vec![9; 64],
        }
    }

    #[test]
    fn compact_bearer_qr_round_trips_and_is_smaller_than_text_token() {
        let token = bearer_token();
        let encoded = encode_bearer(&token).expect("encode bearer");
        let payload = compact_bearer_qr_payload(&token).expect("compact QR payload");

        assert!(payload.starts_with(BEARER_QR_MAGIC));
        assert!(
            payload.len() * 4 < encoded.len() * 3,
            "compact QR should save at least 25% ({} vs {})",
            payload.len(),
            encoded.len()
        );

        let mut decoder = GzDecoder::new(&payload[BEARER_QR_MAGIC.len()..]);
        let mut cbor = Vec::new();
        decoder.read_to_end(&mut cbor).expect("decompress QR");
        let decoded: BearerInviteToken =
            ciborium::de::from_reader(cbor.as_slice()).expect("decode compact QR CBOR");
        assert_eq!(decoded, token);
    }

    #[test]
    fn reciprocal_intent_upsert_mutation_escapes_member_template_and_timestamps() {
        let mutation = reciprocal_conversation_intent_upsert_mutation(
            "did:key:phone\"quoted",
            "conversation",
            "2026-07-08T00:00:00Z",
        );

        assert!(mutation.contains("upsert_ReciprocalConversationIntent"));
        assert!(mutation.contains("member_did: { _eq: \"did:key:phone\\\"quoted\" }"));
        assert!(mutation.contains("template: \"conversation\""));
        assert!(mutation.contains("created_at: \"2026-07-08T00:00:00Z\""));
        assert!(mutation.contains("updated_at: \"2026-07-08T00:00:00Z\""));
        assert!(
            !mutation.contains("[]"),
            "mutation must not emit empty GraphQL list literals"
        );
    }

    #[test]
    fn validate_invite_grant_requires_active_matching_member_and_network() {
        let network = network_record();
        let grant = grant_record();
        assert!(validate_invite_grant(&network, &grant, "did:key:agent-b").is_ok());

        let mut wrong_member = grant.clone();
        wrong_member.member_did = "did:key:other".to_string();
        assert!(validate_invite_grant(&network, &wrong_member, "did:key:agent-b").is_err());

        let mut revoked = grant.clone();
        revoked.status = "revoked".to_string();
        assert!(validate_invite_grant(&network, &revoked, "did:key:agent-b").is_err());

        let mut wrong_network = grant;
        wrong_network.network_id = "net-other".to_string();
        assert!(validate_invite_grant(&network, &wrong_network, "did:key:agent-b").is_err());
    }
}
