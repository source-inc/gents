#[cfg(test)]
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use ciborium::value::{Integer, Value};
#[cfg(test)]
use flate2::read::GzDecoder;
use flate2::{write::GzEncoder, Compression};
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

/// v1 compact QR magic. Superseded by `BEARER_QR_MAGIC_V2` for newly-minted
/// invites; only referenced from the v1 encode/decode test-fixture pair now
/// (see `compact_bearer_qr_payload`).
#[cfg(test)]
const BEARER_QR_MAGIC: &[u8] = b"dabear1z\0";
/// v2 compact QR magic. Same transport shell as v1 (magic + gzip), but the
/// gzip body is a positional CBOR array instead of a struct-as-map, and it
/// omits fields the scanner can losslessly reconstruct (see
/// `compact_bearer_qr_payload_v2`). The signature is verified over
/// `bearer_signing_payload`, computed from the *reconstructed* struct, so
/// this is purely a transport encoding — no token-format or signing change.
const BEARER_QR_MAGIC_V2: &[u8] = b"dabear2z\0";

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
        network,
        sig: Vec::new(),
    };
    token.sig = identity
        .sign(&bearer_signing_payload(&token))
        .await
        .context("signing bearer invite token")?;
    let encoded = encode_bearer(&token)?;

    if args.qr {
        let payload = compact_bearer_qr_payload_v2(&token)?;
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

/// v1 compact QR payload: magic + gzip(struct-as-map CBOR). Superseded by
/// `compact_bearer_qr_payload_v2` for newly-minted invites; kept (and its
/// decode path in `decode_compact_bearer_qr_payload_v1`) so QR codes already
/// in the wild — and older `gents` binaries — keep decoding.
#[cfg(test)]
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

/// True if `peer_id` is recoverable from `ticket` using only the simple,
/// string-level address shapes: `id@host:port`, a legacy `.../p2p/<id>`
/// suffix, or a bare id (optionally `iroh://`-prefixed). When true, the v2
/// payload omits `peer_id` and the decoder rederives it via
/// `resolve_p2p_peer_id`; when false, the payload keeps `peer_id` explicit.
///
/// Deliberately narrower than `resolve_p2p_peer_id` / the iroh
/// `parse_public_peer_addr` it wraps: that helper *also* decodes the compact
/// binary `EndpointTicket` wire format (iroh's own postcard-based ticket
/// encoding) — the shape most real, freshly-minted tickets actually use.
/// This function mirrors only the three plain-string branches, because the
/// real consumer of an omitted `peer_id` isn't this Rust decoder (which
/// could always fall back to the full parser) — it's the phone/TS scanner
/// (`QrScannerDialog.tsx`), which reconstructs the omitted field with its
/// own small, independent implementation and has no practical way to embed
/// iroh's ticket decoder. Omitting `peer_id` is only sound when *every*
/// legitimate decoder can recover the same value, so the gate here is
/// exactly what a plain-string implementation on the other side can match.
fn peer_id_derivable_from_ticket(peer_id: &str, ticket: &str) -> bool {
    fn normalize(id: &str) -> &str {
        let trimmed = id.trim();
        trimmed.strip_prefix("iroh://").unwrap_or(trimmed)
    }

    let trimmed = ticket.trim();
    if let Some((endpoint_id, _host_port)) = trimmed.split_once('@') {
        return normalize(endpoint_id) == peer_id;
    }
    if let Some(pos) = trimmed.rfind("/p2p/") {
        return normalize(&trimmed[pos + 5..]) == peer_id;
    }
    normalize(trimmed) == peer_id
}

/// If `nonce` is a canonical (lowercase, hyphenated) UUID string, returns its
/// 16 raw bytes. Round-trips through `Uuid::to_string()` first so a
/// differently-cased or otherwise non-canonical nonce falls back to the text
/// encoding rather than silently changing shape on decode.
fn uuid_nonce_bytes(nonce: &str) -> Option<[u8; 16]> {
    let parsed = uuid::Uuid::parse_str(nonce).ok()?;
    (parsed.to_string() == nonce).then(|| *parsed.as_bytes())
}

/// If `issued_at` is an RFC3339 timestamp at seconds precision in the exact
/// form `issued_at` (`SecondsFormat::Secs`, `Z` suffix — the form every
/// bearer invite is minted with), returns its Unix epoch seconds. Anything
/// else (sub-second precision, a non-UTC offset that renders differently,
/// non-RFC3339 text) falls back to the text encoding.
fn epoch_seconds_issued_at(issued_at: &str) -> Option<i64> {
    let parsed = DateTime::parse_from_rfc3339(issued_at).ok()?;
    let utc = parsed.with_timezone(&Utc);
    (utc.to_rfc3339_opts(SecondsFormat::Secs, true) == issued_at).then(|| utc.timestamp())
}

/// v2 compact QR payload: `BEARER_QR_MAGIC_V2` + gzip(CBOR array).
///
/// Fields omitted from the array (and reconstructed by the decoder):
/// - `peer_id`, when it is losslessly derivable from `ticket`.
/// - `network.network_id`, always reconstructed as the top-level
///   `network_id`.
/// - `network.admin_did`, always reconstructed as the top-level
///   `issuer_did`.
///
/// The `network.network_id == network_id` and `network.admin_did ==
/// issuer_did` reconstructions are sound for any bearer invite that will
/// pass claim validation: `p2p_invite_bearer` always mints tokens with those
/// fields equal, and `check_token_network_authority`
/// (`commands/p2p/claim.rs:174-183`) independently *requires*
/// `issuer_did == network.admin_did` before a claim is accepted — so a
/// token where reconstruction would diverge from the original is already
/// not a token the claim path would honor.
///
/// `nonce` and `issued_at` are packed into a smaller wire form when they
/// round-trip losslessly (raw 16 bytes for a canonical UUID nonce, integer
/// epoch seconds for a `SecondsFormat::Secs` RFC3339 timestamp), and kept as
/// text otherwise.
fn compact_bearer_qr_payload_v2(token: &BearerInviteToken) -> Result<Vec<u8>> {
    let peer_id_value = if peer_id_derivable_from_ticket(&token.peer_id, &token.ticket) {
        Value::Null
    } else {
        Value::Text(token.peer_id.clone())
    };
    let nonce_value = match uuid_nonce_bytes(&token.nonce) {
        Some(bytes) => Value::Bytes(bytes.to_vec()),
        None => Value::Text(token.nonce.clone()),
    };
    let issued_at_value = match epoch_seconds_issued_at(&token.issued_at) {
        Some(secs) => Value::Integer(Integer::from(secs)),
        None => Value::Text(token.issued_at.clone()),
    };
    let default_behavior_value = match &token.default_behavior_id {
        Some(id) => Value::Text(id.clone()),
        None => Value::Null,
    };
    let network_value = Value::Array(vec![
        Value::Text(token.network.display_name.clone()),
        Value::Text(token.network.default_template.clone()),
        Value::Text(token.network.created_at.clone()),
        Value::Bytes(token.network.sig.clone()),
    ]);

    let array = Value::Array(vec![
        Value::Integer(Integer::from(token.v)),
        Value::Text(token.issuer_did.clone()),
        peer_id_value,
        Value::Text(token.ticket.clone()),
        nonce_value,
        Value::Text(token.network_id.clone()),
        issued_at_value,
        Value::Text(token.template.clone()),
        default_behavior_value,
        network_value,
        Value::Bytes(token.sig.clone()),
    ]);

    let mut cbor = Vec::new();
    ciborium::ser::into_writer(&array, &mut cbor)
        .context("encoding v2 compact bearer invite CBOR array")?;

    let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
    encoder
        .write_all(&cbor)
        .context("compressing v2 compact bearer invite for QR")?;
    let compressed = encoder
        .finish()
        .context("finishing v2 compact bearer invite QR")?;

    let mut payload = Vec::with_capacity(BEARER_QR_MAGIC_V2.len() + compressed.len());
    payload.extend_from_slice(BEARER_QR_MAGIC_V2);
    payload.extend_from_slice(&compressed);
    Ok(payload)
}

#[cfg(test)]
fn gunzip(body: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(body);
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .context("decompressing compact bearer QR payload")?;
    Ok(out)
}

#[cfg(test)]
fn decode_compact_bearer_qr_payload_v1(body: &[u8]) -> Result<BearerInviteToken> {
    let cbor = gunzip(body)?;
    ciborium::de::from_reader(cbor.as_slice()).context("decoding v1 compact bearer invite CBOR")
}

#[cfg(test)]
fn decode_compact_bearer_qr_payload_v2(body: &[u8]) -> Result<BearerInviteToken> {
    let cbor = gunzip(body)?;
    let value: Value = ciborium::de::from_reader(cbor.as_slice())
        .context("decoding v2 compact bearer invite CBOR array")?;
    let items = value
        .into_array()
        .map_err(|_| anyhow::anyhow!("v2 compact bearer invite payload is not a CBOR array"))?;
    let items: [Value; 11] = items.try_into().map_err(|items: Vec<Value>| {
        anyhow::anyhow!(
            "v2 compact bearer invite payload has {} elements, expected 11",
            items.len()
        )
    })?;
    let [v_raw, issuer_did_raw, peer_id_raw, ticket_raw, nonce_raw, network_id_raw, issued_at_raw, template_raw, default_behavior_raw, network_raw, sig_raw] =
        items;

    let v = v_raw
        .into_integer()
        .ok()
        .and_then(|i| u8::try_from(i).ok())
        .context("v2 payload: invalid version field")?;
    let issuer_did = issuer_did_raw
        .into_text()
        .map_err(|_| anyhow::anyhow!("v2 payload: issuer_did is not text"))?;
    let ticket = ticket_raw
        .into_text()
        .map_err(|_| anyhow::anyhow!("v2 payload: ticket is not text"))?;
    let network_id = network_id_raw
        .into_text()
        .map_err(|_| anyhow::anyhow!("v2 payload: network_id is not text"))?;
    let template = template_raw
        .into_text()
        .map_err(|_| anyhow::anyhow!("v2 payload: template is not text"))?;
    let sig = sig_raw
        .into_bytes()
        .map_err(|_| anyhow::anyhow!("v2 payload: sig is not bytes"))?;

    let peer_id = match peer_id_raw {
        Value::Null => resolve_p2p_peer_id(None, Some(&ticket), &[], None)
            .context("v2 payload: peer_id omitted but not derivable from ticket")?,
        Value::Text(text) => text,
        _ => anyhow::bail!("v2 payload: peer_id field has unexpected CBOR type"),
    };

    let nonce = match nonce_raw {
        Value::Bytes(bytes) => {
            let arr: [u8; 16] = bytes
                .as_slice()
                .try_into()
                .map_err(|_| anyhow::anyhow!("v2 payload: nonce bytes are not 16 bytes"))?;
            uuid::Uuid::from_bytes(arr).to_string()
        }
        Value::Text(text) => text,
        _ => anyhow::bail!("v2 payload: nonce field has unexpected CBOR type"),
    };

    let issued_at = match issued_at_raw {
        Value::Integer(i) => {
            let secs = i64::try_from(i)
                .map_err(|_| anyhow::anyhow!("v2 payload: issued_at epoch is out of range"))?;
            let dt = DateTime::<Utc>::from_timestamp(secs, 0)
                .context("v2 payload: issued_at epoch is out of range")?;
            dt.to_rfc3339_opts(SecondsFormat::Secs, true)
        }
        Value::Text(text) => text,
        _ => anyhow::bail!("v2 payload: issued_at field has unexpected CBOR type"),
    };

    let default_behavior_id = match default_behavior_raw {
        Value::Null => None,
        Value::Text(text) => Some(text),
        _ => anyhow::bail!("v2 payload: default_behavior_id field has unexpected CBOR type"),
    };

    let network_items = network_raw
        .into_array()
        .map_err(|_| anyhow::anyhow!("v2 payload: network field is not an array"))?;
    let [display_name, default_template, created_at, network_sig]: [Value; 4] = network_items
        .try_into()
        .map_err(|_| anyhow::anyhow!("v2 payload: network field has unexpected length"))?;
    let display_name = display_name
        .into_text()
        .map_err(|_| anyhow::anyhow!("v2 payload: network.display_name is not text"))?;
    let default_template = default_template
        .into_text()
        .map_err(|_| anyhow::anyhow!("v2 payload: network.default_template is not text"))?;
    let created_at = created_at
        .into_text()
        .map_err(|_| anyhow::anyhow!("v2 payload: network.created_at is not text"))?;
    let network_sig = network_sig
        .into_bytes()
        .map_err(|_| anyhow::anyhow!("v2 payload: network.sig is not bytes"))?;

    Ok(BearerInviteToken {
        v,
        // Reconstructed: see the soundness note on `compact_bearer_qr_payload_v2`.
        network: NetworkRecord {
            network_id: network_id.clone(),
            admin_did: issuer_did.clone(),
            display_name,
            default_template,
            created_at,
            sig: network_sig,
        },
        issuer_did,
        peer_id,
        ticket,
        nonce,
        network_id,
        issued_at,
        template,
        default_behavior_id,
        sig,
    })
}

/// Decodes a compact bearer invite QR payload produced by either
/// `compact_bearer_qr_payload` (v1) or `compact_bearer_qr_payload_v2` (v2),
/// dispatching on the leading magic. Kept for symmetry with the encoders and
/// to exercise both formats from Rust; the phone scanner is the real
/// consumer (`QrScannerDialog.tsx`'s `decodePairingQrPayload`), reimplemented
/// there since that's where compact QR payloads are actually scanned.
#[cfg(test)]
fn decode_compact_bearer_qr_payload(payload: &[u8]) -> Result<BearerInviteToken> {
    if let Some(body) = payload.strip_prefix(BEARER_QR_MAGIC_V2) {
        return decode_compact_bearer_qr_payload_v2(body);
    }
    if let Some(body) = payload.strip_prefix(BEARER_QR_MAGIC) {
        return decode_compact_bearer_qr_payload_v1(body);
    }
    anyhow::bail!("unrecognized compact bearer QR payload magic")
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
    fn compact_bearer_qr_v2_round_trips_for_realistic_token() {
        let token = bearer_token();
        let payload = compact_bearer_qr_payload_v2(&token).expect("compact v2 QR payload");
        assert!(payload.starts_with(BEARER_QR_MAGIC_V2));

        let decoded = decode_compact_bearer_qr_payload_v2(&payload[BEARER_QR_MAGIC_V2.len()..])
            .expect("decode compact v2 QR payload");
        assert_eq!(decoded, token);

        // The fixture's ticket is a real iroh `EndpointTicket` string (the
        // shape freshly-minted tickets actually use), which only the full
        // iroh parser — not the plain-string rule `peer_id_derivable_from_
        // ticket` deliberately limits itself to — can decode. So peer_id
        // stays explicit in the v2 payload here; see the derivable-fallback
        // test below for a ticket shape where omission does apply.
        assert!(
            !peer_id_derivable_from_ticket(&token.peer_id, &token.ticket),
            "fixture ticket is an EndpointTicket string; peer_id must stay explicit"
        );
    }

    #[test]
    fn compact_bearer_qr_v2_omits_peer_id_for_legacy_at_host_tickets() {
        // A `peer_id@host:port` ticket is one of the plain-string shapes
        // `peer_id_derivable_from_ticket` recognizes (mirroring iroh's own
        // legacy address format), and the phone/TS scanner can recover the
        // same value with an equally simple string split — so v2 should
        // omit `peer_id` here and the decoder should reconstruct it exactly.
        let mut token = bearer_token();
        token.ticket = format!("{}@127.0.0.1:4242", token.peer_id);
        assert!(
            peer_id_derivable_from_ticket(&token.peer_id, &token.ticket),
            "legacy id@host ticket should derive the token's peer_id"
        );

        let payload = compact_bearer_qr_payload_v2(&token).expect("compact v2 QR payload");
        let decoded = decode_compact_bearer_qr_payload_v2(&payload[BEARER_QR_MAGIC_V2.len()..])
            .expect("decode compact v2 QR payload");
        assert_eq!(decoded, token);
    }

    #[test]
    fn compact_bearer_qr_v2_round_trips_without_default_behavior_id() {
        let mut token = bearer_token();
        token.default_behavior_id = None;

        let payload = compact_bearer_qr_payload_v2(&token).expect("compact v2 QR payload");
        let decoded = decode_compact_bearer_qr_payload_v2(&payload[BEARER_QR_MAGIC_V2.len()..])
            .expect("decode compact v2 QR payload");
        assert_eq!(decoded, token);
        assert_eq!(decoded.default_behavior_id, None);
    }

    #[test]
    fn compact_bearer_qr_v2_round_trips_fallback_forms() {
        let mut token = bearer_token();
        // Non-UUID nonce, non-RFC3339 issued_at, and a ticket that does not
        // encode the token's peer_id: every optional/compact slot must fall
        // back to its explicit text (or non-derivable-peer_id) form and
        // still round-trip exactly, byte for byte.
        token.nonce = "not-a-uuid-nonce".to_string();
        token.issued_at = "not-a-timestamp".to_string();
        token.ticket = "opaque-ticket-with-no-embedded-peer-id".to_string();
        assert!(
            !peer_id_derivable_from_ticket(&token.peer_id, &token.ticket),
            "test ticket must not accidentally encode the fixture peer_id"
        );

        let payload = compact_bearer_qr_payload_v2(&token).expect("compact v2 QR payload");
        let decoded = decode_compact_bearer_qr_payload_v2(&payload[BEARER_QR_MAGIC_V2.len()..])
            .expect("decode compact v2 QR payload");
        assert_eq!(decoded, token);
        assert_eq!(decoded.nonce, "not-a-uuid-nonce");
        assert_eq!(decoded.issued_at, "not-a-timestamp");
        assert_eq!(decoded.peer_id, token.peer_id);
    }

    #[test]
    fn compact_bearer_qr_v2_preserves_signing_payload_bytes() {
        let token = bearer_token();
        let original_signing_payload = bearer_signing_payload(&token);

        let payload = compact_bearer_qr_payload_v2(&token).expect("compact v2 QR payload");
        let decoded = decode_compact_bearer_qr_payload_v2(&payload[BEARER_QR_MAGIC_V2.len()..])
            .expect("decode compact v2 QR payload");

        // This byte-identity IS the crypto safety property: the reconstructed
        // struct must serialize to the exact bytes the issuer signed, or the
        // existing (unmodified) signature over `original_signing_payload`
        // would no longer verify against the decoded token.
        assert_eq!(bearer_signing_payload(&decoded), original_signing_payload);
        assert_eq!(decoded.sig, token.sig);
    }

    #[test]
    fn compact_bearer_qr_v2_is_materially_smaller_than_v1_and_bs58_text() {
        let token = bearer_token();
        let bs58_text = encode_bearer(&token).expect("encode bearer bs58 text");
        let v1_payload = compact_bearer_qr_payload(&token).expect("compact v1 QR payload");
        let v2_payload = compact_bearer_qr_payload_v2(&token).expect("compact v2 QR payload");

        // 0.85, not a tighter ratio: this fixture's ticket is a real iroh
        // `EndpointTicket` string, so `peer_id` stays explicit (see
        // `peer_id_derivable_from_ticket`'s doc comment — omitting it here
        // would require the TS/mobile scanner to embed an iroh ticket
        // decoder, which isn't a sound trade for the extra bytes). The
        // remaining savings — positional array, no text keys, byte-string
        // sigs, deduped network_id/admin_did, compact nonce/issued_at — are
        // still a real, material reduction on their own.
        assert!(
            (v2_payload.len() as f64) <= 0.85 * (v1_payload.len() as f64),
            "v2 ({} bytes) should be at most 85% of v1 ({} bytes)",
            v2_payload.len(),
            v1_payload.len()
        );
        assert!(
            v1_payload.len() < bs58_text.len(),
            "v1 compact ({} bytes) should be smaller than bs58 text ({} bytes)",
            v1_payload.len(),
            bs58_text.len()
        );
        assert!(
            v2_payload.len() < bs58_text.len(),
            "v2 compact ({} bytes) should be smaller than bs58 text ({} bytes)",
            v2_payload.len(),
            bs58_text.len()
        );

        let bs58_qr = qrcode::QrCode::with_error_correction_level(&bs58_text, qrcode::EcLevel::L)
            .expect("encode bs58 text as QR");
        let v1_qr = qrcode::QrCode::with_error_correction_level(&v1_payload, qrcode::EcLevel::L)
            .expect("encode v1 compact payload as QR");
        let v2_qr = qrcode::QrCode::with_error_correction_level(&v2_payload, qrcode::EcLevel::L)
            .expect("encode v2 compact payload as QR");

        println!(
            "bearer QR payload sizes (EC level L): bs58 text = {} bytes, {}x{} modules; \
             v1 compact = {} bytes, {}x{} modules; v2 compact = {} bytes, {}x{} modules",
            bs58_text.len(),
            bs58_qr.width(),
            bs58_qr.width(),
            v1_payload.len(),
            v1_qr.width(),
            v1_qr.width(),
            v2_payload.len(),
            v2_qr.width(),
            v2_qr.width(),
        );
    }

    #[test]
    fn compact_bearer_qr_v1_payloads_still_decode_via_dispatcher() {
        let token = bearer_token();

        let v1_payload = compact_bearer_qr_payload(&token).expect("compact v1 QR payload");
        let decoded_v1 =
            decode_compact_bearer_qr_payload(&v1_payload).expect("decode v1 via dispatcher");
        assert_eq!(decoded_v1, token);

        let v2_payload = compact_bearer_qr_payload_v2(&token).expect("compact v2 QR payload");
        let decoded_v2 =
            decode_compact_bearer_qr_payload(&v2_payload).expect("decode v2 via dispatcher");
        assert_eq!(decoded_v2, token);
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

#[cfg(test)]
mod scratch_v1_fixture_probe {
    use super::*;
    use gents_protocol::bearer_token::{BearerInviteToken, BEARER_TOKEN_VERSION};
    use gents_protocol::network_token::NetworkRecord;

    #[test]
    fn probe() {
        let token = BearerInviteToken {
            v: BEARER_TOKEN_VERSION,
            issuer_did: "did:key:z6MktestIssuerFixture".to_string(),
            peer_id: "peerlegacyabc123".to_string(),
            ticket: "peerlegacyabc123@127.0.0.1:4242".to_string(),
            nonce: "3fa85f64-5717-4562-b3fc-2c963f66afa6".to_string(),
            network_id: "net-test-fixture".to_string(),
            issued_at: "2026-07-08T00:00:00Z".to_string(),
            template: "conversation".to_string(),
            default_behavior_id: Some("default".to_string()),
            network: NetworkRecord {
                network_id: "net-test-fixture".to_string(),
                admin_did: "did:key:z6MktestIssuerFixture".to_string(),
                display_name: "Test Net".to_string(),
                default_template: "conversation".to_string(),
                created_at: "2026-07-08T00:00:00Z".to_string(),
                sig: vec![1, 2, 3, 4],
            },
            sig: vec![9, 9, 9, 9],
        };

        let v1_payload = compact_bearer_qr_payload(&token).expect("v1 payload");
        let raw_v1_cbor = gunzip(&v1_payload[BEARER_QR_MAGIC.len()..]).expect("gunzip v1");
        println!("RAW_V1_CBOR={:?}", raw_v1_cbor);
    }
}
