use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use defra_node::{EmbeddedNode, QueryResponse};
use defra_p2p_adapter::{P2POperations as P2POps, ReplicationFilter, ReplicationFilters};
use gents::agent::p2p_reconcile::{
    conversation_like, peer_endpoint_upsert_mutation, resolve_template, scope_filter,
    template_schema_digest, DidSource, Scope, AGENT_DIRECTORY_COLLECTION,
};
use gents::graphql::escape_graphql_string;
use gents::identity::AgentIdentity;
use gents_protocol::bearer_token::{
    bearer_signing_payload, check_bearer_freshness, decode_bearer, derive_bearer_readiness_key,
    BearerClaimRecord, BearerInviteToken, BearerPairingReadyRecord,
};
use gents_protocol::network_token::{EndpointRecord, MembershipRecord, NetworkRecord};
use p2p::iroh::parse_public_peer_addr;
use serde::Deserialize;
use tokio::time::{sleep, timeout, Instant};

use super::super::peer_directory::PeerRecord;
use super::super::schema::subscribed_collection_names;
use super::bootstrap::connect_peer_with_retry_until;
use super::p2p_ops::p2p_remove_replicator;
use super::{ClientCore, ClientPeerStatus, BOOTSTRAP_OPERATION_TIMEOUT, P2P_OPERATION_TIMEOUT};

pub const BEARER_PAIRING_SOURCE: &str = "bearer-pairing";
pub const BEARER_CONTROL_PLANE_COLLECTIONS: &[&str] = &["PairingBearerClaim", "PeerEndpoint"];
const BEARER_GRANT_TIMEOUT: Duration = Duration::from_secs(60);
const BEARER_GRANT_POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BearerInvitePreview {
    pub issuer_did: String,
    pub peer_id: String,
    pub network_id: String,
    pub network_name: String,
    pub template: String,
    pub issued_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BearerPairingResult {
    pub peer_id: String,
    pub label: String,
    pub addr: String,
    pub issuer_did: String,
    pub claimant_did: String,
    pub network_id: String,
    pub template: String,
    pub connected: bool,
    pub claim_submitted: bool,
    pub endpoint_published: bool,
    pub replication_configured: bool,
    pub membership_observed: bool,
    pub bidirectional_replication_observed: bool,
}

#[derive(Debug, Clone)]
struct VerifiedBearerInvite {
    raw: String,
    token: BearerInviteToken,
}

impl VerifiedBearerInvite {
    fn preview(&self) -> BearerInvitePreview {
        BearerInvitePreview {
            issuer_did: self.token.issuer_did.clone(),
            peer_id: self.token.peer_id.clone(),
            network_id: self.token.network_id.clone(),
            network_name: self.token.network.display_name.clone(),
            template: self.token.template.clone(),
            issued_at: self.token.issued_at.clone(),
        }
    }
}

impl ClientCore {
    pub async fn preview_bearer_invite(&self, raw_token: &str) -> Result<BearerInvitePreview> {
        Ok(verify_bearer_invite(&self.principal, raw_token, Utc::now())
            .await?
            .preview())
    }

    pub async fn pair_with_bearer_invite(
        &self,
        raw_token: &str,
        requested_label: Option<&str>,
    ) -> Result<BearerPairingResult> {
        let verified = verify_bearer_invite(&self.principal, raw_token, Utc::now()).await?;
        let token = &verified.token;
        let previous_addresses = self
            .peer_directory
            .read()
            .await
            .records()
            .iter()
            .filter(|record| record.agent_did == token.issuer_did)
            .map(|record| record.addr.clone())
            .collect::<Vec<_>>();
        let label = requested_label
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .or_else(|| {
                let value = token.network.display_name.trim();
                (!value.is_empty()).then_some(value)
            })
            .unwrap_or("Paired Agent")
            .to_string();

        connect_peer_with_retry_until(
            &self.p2p,
            &token.ticket,
            &label,
            BOOTSTRAP_OPERATION_TIMEOUT,
        )
        .await
        .context("connecting to the verified bearer-invite peer")?;

        ensure_local_network_match(self.node.as_ref(), token).await?;
        write_agent_network(self.node.as_ref(), &token.network).await?;
        let local_endpoint =
            publish_local_endpoint(self.node.as_ref(), &self.p2p, &self.principal).await?;
        write_bearer_claim_if_absent(
            self.node.as_ref(),
            &self.p2p,
            &self.principal,
            &verified.raw,
        )
        .await?;

        let record = {
            let mut directory = self.peer_directory.write().await;
            directory
                .upsert_bearer_peer(
                    &label,
                    &token.ticket,
                    &token.issuer_did,
                    &token.network_id,
                    &token.template,
                    token.default_behavior_id.as_deref(),
                )
                .await?
        };
        let mut replicator_addresses = previous_addresses;
        if !replicator_addresses.contains(&record.addr) {
            replicator_addresses.push(record.addr.clone());
        }
        for address in &replicator_addresses {
            remove_incompatible_replicators(&self.p2p, address).await?;
        }
        install_bearer_replicator(
            &self.p2p,
            &token.ticket,
            self.principal.did(),
            &token.issuer_did,
            &token.template,
        )
        .await?;
        wait_for_bearer_readiness(
            self.node.as_ref(),
            &self.principal,
            &token.issuer_did,
            &token.network_id,
            &token.template,
            &local_endpoint,
            BEARER_GRANT_TIMEOUT,
        )
        .await?;
        let record = {
            let mut directory = self.peer_directory.write().await;
            directory
                .set_bearer_pairing_ready(&record.peer_id, true)
                .await?
                .context("saved bearer peer disappeared before readiness could be persisted")?
        };
        self.update_peer_status(ClientPeerStatus {
            peer_id: record.peer_id.clone(),
            label: record.label.clone(),
            agent_did: record.agent_did.clone(),
            addr: record.addr.clone(),
            dial_succeeded: true,
            last_error: None,
            pairing: Vec::new(),
        });
        self.clear_mutation_error();

        tracing::info!(
            target: "gents_desktop_core::bearer_pairing",
            peer_id = %record.peer_id,
            remote_peer_id = %token.peer_id,
            issuer_did = %token.issuer_did,
            claimant_did = %self.principal.did(),
            network_id = %token.network_id,
            "desktop bearer pairing reached signed-grant readiness"
        );

        Ok(BearerPairingResult {
            peer_id: record.peer_id,
            label: record.label,
            addr: record.addr,
            issuer_did: token.issuer_did.clone(),
            claimant_did: self.principal.did().to_string(),
            network_id: token.network_id.clone(),
            template: token.template.clone(),
            connected: true,
            claim_submitted: true,
            endpoint_published: true,
            replication_configured: true,
            membership_observed: true,
            bidirectional_replication_observed: true,
        })
    }
}

async fn wait_for_bearer_readiness(
    node: &EmbeddedNode,
    identity: &dyn AgentIdentity,
    issuer_did: &str,
    network_id: &str,
    template: &str,
    local_endpoint: &EndpointRecord,
    wait_timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + wait_timeout;
    loop {
        if observe_bearer_pairing_readiness(
            node,
            identity,
            issuer_did,
            network_id,
            template,
            local_endpoint,
        )
        .await?
        {
            return Ok(());
        }

        if Instant::now() >= deadline {
            bail!(
                "timed out after {}s waiting for the issuer-signed membership grant and reciprocal-replication acknowledgement; verify that the server is running with P2P, bearer-claim, reciprocal, and pairing reconcilers enabled, then relaunch to resume the saved pairing (mint a fresh invite only if the server rejected this nonce)",
                wait_timeout.as_secs()
            );
        }
        sleep(BEARER_GRANT_POLL_INTERVAL).await;
    }
}

pub(super) async fn observe_bearer_pairing_readiness(
    node: &EmbeddedNode,
    identity: &dyn AgentIdentity,
    issuer_did: &str,
    network_id: &str,
    template: &str,
    local_endpoint: &EndpointRecord,
) -> Result<bool> {
    let escaped_network_id = escape_graphql_string(network_id);
    let escaped_member_did = escape_graphql_string(identity.did());
    let readiness_key =
        escape_graphql_string(&derive_bearer_readiness_key(issuer_did, identity.did()));
    let query = format!(
        r#"{{
            NetworkMembership(
                filter: {{
                    network_id: {{ _eq: "{escaped_network_id}" }},
                    member_did: {{ _eq: "{escaped_member_did}" }}
                }},
                limit: 1
            ) {{
                network_id
                member_did
                status
                granted_at
                revoked_at
                admin_sig
            }}
            BearerPairingReady(
                filter: {{ readiness_key: {{ _eq: "{readiness_key}" }} }},
                limit: 1
            ) {{
                issuer_did
                claimant_did
                peer_id
                address
                template
                acknowledged_at
                issuer_sig
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    ensure_no_errors(
        &response,
        "checking issuer-signed bearer readiness evidence",
    )?;
    let membership_rows = response
        .data
        .as_ref()
        .and_then(|data| data.get("NetworkMembership"))
        .cloned()
        .unwrap_or_else(|| serde_json::Value::Array(Vec::new()));
    let membership_rows = serde_json::from_value::<Vec<MembershipObservationRow>>(membership_rows)
        .context("decoding the replicated bearer membership grant")?;
    let Some(membership) = membership_rows.first() else {
        return Ok(false);
    };
    verify_active_membership_row(identity, issuer_did, network_id, identity.did(), membership)
        .await?;

    let readiness_rows = response
        .data
        .as_ref()
        .and_then(|data| data.get("BearerPairingReady"))
        .cloned()
        .unwrap_or_else(|| serde_json::Value::Array(Vec::new()));
    let readiness_rows =
        serde_json::from_value::<Vec<BearerPairingReadyObservationRow>>(readiness_rows)
            .context("decoding the replicated bearer readiness acknowledgement")?;
    let Some(readiness) = readiness_rows.first() else {
        return Ok(false);
    };
    verify_bearer_pairing_ready_row(
        identity,
        issuer_did,
        identity.did(),
        template,
        local_endpoint,
        readiness,
    )
    .await?;
    Ok(true)
}

async fn verify_bearer_pairing_ready_row(
    identity: &dyn AgentIdentity,
    issuer_did: &str,
    claimant_did: &str,
    expected_template: &str,
    local_endpoint: &EndpointRecord,
    row: &BearerPairingReadyObservationRow,
) -> Result<()> {
    let record = BearerPairingReadyRecord {
        issuer_did: required_membership_field(
            row.issuer_did.as_deref(),
            "BearerPairingReady.issuer_did",
        )?,
        claimant_did: required_membership_field(
            row.claimant_did.as_deref(),
            "BearerPairingReady.claimant_did",
        )?,
        peer_id: required_membership_field(row.peer_id.as_deref(), "BearerPairingReady.peer_id")?,
        address: required_membership_field(row.address.as_deref(), "BearerPairingReady.address")?,
        template: required_membership_field(
            row.template.as_deref(),
            "BearerPairingReady.template",
        )?,
        acknowledged_at: required_membership_field(
            row.acknowledged_at.as_deref(),
            "BearerPairingReady.acknowledged_at",
        )?,
        sig: bs58::decode(required_membership_field(
            row.issuer_sig.as_deref(),
            "BearerPairingReady.issuer_sig",
        )?)
        .into_vec()
        .context("decoding the bearer readiness signature")?,
    };
    if record.issuer_did != issuer_did
        || record.claimant_did != claimant_did
        || record.peer_id != local_endpoint.node_id
        || record.address != local_endpoint.address
        || record.template != expected_template
    {
        bail!(
            "bearer readiness acknowledgement does not match issuer, claimant, or the current signed endpoint; pairing rejected"
        );
    }
    match identity
        .verify(issuer_did, &record.signing_payload(), &record.sig)
        .await
    {
        Ok(true) => Ok(()),
        Ok(false) => bail!(
            "bearer readiness acknowledgement signature is invalid for issuer {}; pairing rejected",
            issuer_did
        ),
        Err(error) => bail!(
            "bearer readiness acknowledgement signature is invalid for issuer {}: {}",
            issuer_did,
            error
        ),
    }
}

async fn verify_active_membership_row(
    identity: &dyn AgentIdentity,
    issuer_did: &str,
    expected_network_id: &str,
    expected_member_did: &str,
    row: &MembershipObservationRow,
) -> Result<()> {
    let record = MembershipRecord {
        network_id: required_membership_field(
            row.network_id.as_deref(),
            "NetworkMembership.network_id",
        )?,
        member_did: required_membership_field(
            row.member_did.as_deref(),
            "NetworkMembership.member_did",
        )?,
        status: required_membership_field(row.status.as_deref(), "NetworkMembership.status")?,
        granted_at: required_membership_field(
            row.granted_at.as_deref(),
            "NetworkMembership.granted_at",
        )?,
        revoked_at: row.revoked_at.clone().unwrap_or_default(),
        sig: bs58::decode(required_membership_field(
            row.admin_sig.as_deref(),
            "NetworkMembership.admin_sig",
        )?)
        .into_vec()
        .context("decoding the membership grant signature")?,
    };
    if record.network_id != expected_network_id || record.member_did != expected_member_did {
        bail!(
            "replicated membership grant targets network {} and member {}, expected network {} and member {}; pairing rejected",
            record.network_id,
            record.member_did,
            expected_network_id,
            expected_member_did
        );
    }
    if record.status.trim() != "active" {
        bail!(
            "membership grant for {} is {}; an active grant is required before chat can start",
            expected_member_did,
            record.status.trim()
        );
    }
    match identity
        .verify(issuer_did, &record.signing_payload(), &record.sig)
        .await
    {
        Ok(true) => Ok(()),
        Ok(false) => bail!(
            "membership grant signature is invalid for issuer {}; pairing rejected",
            issuer_did
        ),
        Err(error) => bail!(
            "membership grant signature is invalid for issuer {}: {}",
            issuer_did,
            error
        ),
    }
}

fn required_membership_field(value: Option<&str>, field: &str) -> Result<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .with_context(|| format!("{field} is missing from the replicated grant"))
}

#[derive(Debug, Deserialize)]
struct MembershipObservationRow {
    network_id: Option<String>,
    member_did: Option<String>,
    status: Option<String>,
    granted_at: Option<String>,
    revoked_at: Option<String>,
    admin_sig: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct BearerPairingReadyObservationRow {
    issuer_did: Option<String>,
    claimant_did: Option<String>,
    peer_id: Option<String>,
    address: Option<String>,
    template: Option<String>,
    acknowledged_at: Option<String>,
    issuer_sig: Option<String>,
}

async fn remove_incompatible_replicators(p2p: &Arc<dyn P2POps>, address: &str) -> Result<()> {
    p2p_remove_replicator(
        p2p,
        subscribed_collection_names()
            .into_iter()
            .map(str::to_owned)
            .collect(),
        address,
    )
    .await
    .with_context(|| format!("removing legacy unfiltered replicator for {address}"))?;
    for template in ["conversation", "machine"] {
        p2p_remove_replicator(p2p, bearer_replicator_collections(template), address)
            .await
            .with_context(|| {
                format!("replacing prior {template} signed bearer replicator for {address}")
            })?;
    }
    Ok(())
}

async fn verify_bearer_invite(
    identity: &dyn AgentIdentity,
    raw_token: &str,
    now: DateTime<Utc>,
) -> Result<VerifiedBearerInvite> {
    let raw = raw_token.trim();
    let token = decode_bearer(raw)?;

    match identity
        .verify(
            &token.issuer_did,
            &bearer_signing_payload(&token),
            &token.sig,
        )
        .await
    {
        Ok(true) => {}
        Ok(false) => bail!(
            "bearer invite signature invalid for issuer {}",
            token.issuer_did
        ),
        Err(error) => bail!(
            "bearer invite signature invalid for issuer {}: {}",
            token.issuer_did,
            error
        ),
    }

    check_bearer_freshness(&token, now)
        .context("bearer invite expired or is future-dated; re-mint the QR")?;
    if token.issuer_did != token.network.admin_did {
        bail!(
            "bearer invite issuer {} is not the network admin {}; claim rejected",
            token.issuer_did,
            token.network.admin_did
        );
    }
    if token.network_id != token.network.network_id {
        bail!(
            "bearer invite network id {} does not match its signed network root {}; claim rejected",
            token.network_id,
            token.network.network_id
        );
    }

    match identity
        .verify(
            &token.network.admin_did,
            &token.network.signing_payload(),
            &token.network.sig,
        )
        .await
    {
        Ok(true) => {}
        Ok(false) => bail!(
            "bearer invite network root signature invalid for admin {}",
            token.network.admin_did
        ),
        Err(error) => bail!(
            "bearer invite network root signature invalid for admin {}: {}",
            token.network.admin_did,
            error
        ),
    }

    let template = resolve_template(&token.template)
        .with_context(|| format!("unknown bearer pairing template {}", token.template))?;
    if !conversation_like(&token.template) {
        bail!(
            "desktop chat pairing requires a conversation-like template; invite uses {}",
            token.template
        );
    }
    if !bearer_template_is_safely_scoped(template) {
        bail!("bearer pairing template has an unexpected scope; re-issue with a compatible gents");
    }
    if token
        .default_behavior_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_none()
    {
        bail!(
            "conversation bearer invite is missing its signed default behavior; re-mint it with a compatible gents"
        );
    }

    let (ticket_peer_id, _) = parse_public_peer_addr(&token.ticket)
        .context("bearer invite contains an invalid or non-dialable Iroh ticket")?;
    if ticket_peer_id.to_string() != token.peer_id {
        bail!(
            "bearer invite peer id {} does not match the signed ticket peer id {}; claim rejected",
            token.peer_id,
            ticket_peer_id
        );
    }

    // Schema-digest preflight (issue #1122), before any pairing row is
    // written (this runs ahead of every write in `pair_with_bearer_invite`):
    // a paired client whose bundled SDLs differ from the server's reads
    // fine but every document it authors is merge-rejected forever with no
    // signal anywhere the user looks. The issuer stamps a digest of its
    // template's SDLs into the (signed) invite token; recompute the same
    // digest from this build's local schema bundle and compare. `None`
    // means an older server minted no digest — skip silently, back-compat.
    if let Some(remote_digest) = token.schema_digest.as_deref() {
        let local_digest = template_schema_digest(template)
            .context("computing local schema digest for bearer invite verification")?;
        if local_digest != remote_digest {
            bail!(
                "schema mismatch: this build's bundled schemas for template '{}' (digest {local_digest}) \
                 differ from the server's (digest {remote_digest}); documents you author would be \
                 silently rejected. Update this app to match the server before pairing.",
                token.template
            );
        }
    }

    Ok(VerifiedBearerInvite {
        raw: raw.to_string(),
        token,
    })
}

fn bearer_template_is_safely_scoped(template: &gents::agent::p2p_reconcile::ScopeTemplate) -> bool {
    let Scope::PerCollection(rules) = &template.scope else {
        return false;
    };
    let Some(config_template) = resolve_template("agent-config") else {
        return false;
    };
    let filtered_collections = template
        .collections
        .iter()
        .filter(|collection| !config_template.collections.contains(collection))
        .collect::<Vec<_>>();
    rules.len() == filtered_collections.len()
        && filtered_collections.into_iter().all(|collection| {
            let (expected_field, expected_source) = if *collection == "BearerPairingReady" {
                ("claimant_did", DidSource::PeerDid)
            } else if *collection == AGENT_DIRECTORY_COLLECTION {
                ("source_did", DidSource::HomeDid)
            } else {
                ("requester_did", DidSource::PeerDid)
            };
            rules.iter().any(|rule| {
                rule.collection == *collection
                    && rule.field == expected_field
                    && rule.source == expected_source
            })
        })
}

pub(super) fn is_bearer_peer(record: &PeerRecord) -> bool {
    record.is_bearer_pairing()
}

pub(super) fn bearer_replicator_collections(template_id: &str) -> Vec<String> {
    let template = resolve_template(template_id)
        .filter(|template| conversation_like(template.id))
        .unwrap_or_else(|| resolve_template("conversation").expect("conversation template"));
    let mut collections = template
        .collections
        .iter()
        .map(|value| (*value).to_string())
        .collect::<Vec<_>>();
    collections.extend(
        BEARER_CONTROL_PLANE_COLLECTIONS
            .iter()
            .map(|value| (*value).to_string()),
    );
    collections
}

pub(super) async fn install_bearer_replicator_for_record(
    p2p: &Arc<dyn P2POps>,
    record: &PeerRecord,
    requester_did: &str,
) -> Result<()> {
    remove_incompatible_replicators(p2p, &record.addr).await?;
    let template = record.pairing_template.as_deref().unwrap_or("conversation");
    install_bearer_replicator(
        p2p,
        &record.addr,
        requester_did,
        &record.agent_did,
        template,
    )
    .await
}

async fn install_bearer_replicator(
    p2p: &Arc<dyn P2POps>,
    address: &str,
    requester_did: &str,
    issuer_did: &str,
    template_id: &str,
) -> Result<()> {
    let filters = bearer_replicator_filters(template_id, requester_did, issuer_did);
    let collections = bearer_replicator_collections(template_id);

    match timeout(
        P2P_OPERATION_TIMEOUT,
        p2p.add_replicator(collections, Some(address), filters, Vec::new(), None),
    )
    .await
    {
        Ok(result) => result
            .map_err(anyhow::Error::msg)
            .with_context(|| format!("installing signed bearer replicator for {address}")),
        Err(_) => bail!("timed out installing signed bearer replicator for {address}"),
    }
}

fn bearer_replicator_filters(
    template_id: &str,
    requester_did: &str,
    issuer_did: &str,
) -> ReplicationFilters {
    let template = resolve_template(template_id)
        .filter(|template| conversation_like(template.id))
        .unwrap_or_else(|| resolve_template("conversation").expect("conversation template"));
    let mut filters = scope_filter(
        &template.scope,
        template.collections,
        requester_did,
        issuer_did,
    )
    .into_iter()
    .map(|(collection, predicate)| {
        (
            collection,
            ReplicationFilter::eq(&predicate.field, serde_json::Value::String(predicate.value)),
        )
    })
    .collect::<ReplicationFilters>();
    filters.insert(
        "PairingBearerClaim".to_string(),
        ReplicationFilter::eq(
            "claimant_did",
            serde_json::Value::String(requester_did.to_string()),
        ),
    );
    filters.insert(
        "PeerEndpoint".to_string(),
        ReplicationFilter::eq("did", serde_json::Value::String(requester_did.to_string())),
    );
    filters
}

pub(super) async fn publish_local_endpoint(
    node: &EmbeddedNode,
    p2p: &Arc<dyn P2POps>,
    identity: &dyn AgentIdentity,
) -> Result<EndpointRecord> {
    let mut record = current_local_endpoint(p2p, identity).await?;
    record.sig = identity
        .sign(&record.signing_payload())
        .await
        .context("signing desktop PeerEndpoint")?;
    let response = node.execute(&peer_endpoint_upsert_mutation(&record)).await;
    ensure_no_errors(&response, "publishing desktop PeerEndpoint")?;
    Ok(record)
}

pub(super) async fn current_local_endpoint(
    p2p: &Arc<dyn P2POps>,
    identity: &dyn AgentIdentity,
) -> Result<EndpointRecord> {
    let peer_id = match timeout(P2P_OPERATION_TIMEOUT, p2p.local_peer_id()).await {
        Ok(result) => result
            .map_err(anyhow::Error::msg)
            .context("reading desktop P2P peer id for signed endpoint")?,
        Err(_) => bail!("timed out reading desktop P2P peer id for signed endpoint"),
    };
    let address = match timeout(P2P_OPERATION_TIMEOUT, p2p.shareable_address()).await {
        Ok(result) => result
            .map_err(anyhow::Error::msg)
            .context("reading desktop shareable P2P address")?
            .context("desktop P2P transport has no dialable shareable address yet")?,
        Err(_) => bail!("timed out reading desktop shareable P2P address"),
    };
    Ok(EndpointRecord {
        did: identity.did().to_string(),
        node_id: peer_id,
        address,
        updated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        sig: Vec::new(),
    })
}

async fn write_bearer_claim_if_absent(
    node: &EmbeddedNode,
    p2p: &Arc<dyn P2POps>,
    identity: &dyn AgentIdentity,
    raw_token: &str,
) -> Result<()> {
    let token = escape_graphql_string(raw_token);
    let claimant_did = escape_graphql_string(identity.did());
    let query = format!(
        r#"{{
            PairingBearerClaim(
                filter: {{
                    token: {{ _eq: "{token}" }},
                    claimant_did: {{ _eq: "{claimant_did}" }}
                }},
                limit: 1
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&query).await;
    ensure_no_errors(&response, "checking existing desktop bearer claim")?;
    let exists = response
        .data
        .as_ref()
        .and_then(|data| data.get("PairingBearerClaim"))
        .and_then(|rows| rows.as_array())
        .is_some_and(|rows| !rows.is_empty());
    if exists {
        return Ok(());
    }

    let claimant_node_id = match timeout(P2P_OPERATION_TIMEOUT, p2p.local_peer_id()).await {
        Ok(result) => result.map_err(anyhow::Error::msg)?,
        Err(_) => bail!("timed out reading claimant P2P peer id"),
    };
    let claimant_address = match timeout(P2P_OPERATION_TIMEOUT, p2p.shareable_address()).await {
        Ok(result) => result
            .map_err(anyhow::Error::msg)?
            .context("desktop P2P transport has no claimant shareable address")?,
        Err(_) => bail!("timed out reading claimant P2P address"),
    };
    let mut claim = BearerClaimRecord {
        token: raw_token.to_string(),
        claimant_did: identity.did().to_string(),
        claimant_node_id,
        claimant_address,
        claimed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        sig: Vec::new(),
    };
    claim.sig = identity
        .sign(&claim.signing_payload())
        .await
        .context("signing desktop bearer claim")?;
    let response = node.execute(&bearer_claim_create_mutation(&claim)).await;
    ensure_no_errors(&response, "writing desktop bearer claim")
}

async fn ensure_local_network_match(node: &EmbeddedNode, token: &BearerInviteToken) -> Result<()> {
    let response = node
        .execute("{ AgentNetwork { network_id admin_did } }")
        .await;
    ensure_no_errors(
        &response,
        "loading local AgentNetwork before bearer pairing",
    )?;
    let rows = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentNetwork"))
        .and_then(|rows| rows.as_array())
        .cloned()
        .unwrap_or_default();
    for row in rows {
        let network_id = row
            .get("network_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let admin_did = row
            .get("admin_did")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if network_id != token.network.network_id {
            bail!(
                "bearer invite is for network {} but this client is already bound to network {}; pairing rejected",
                token.network.network_id,
                network_id
            );
        }
        if admin_did != token.network.admin_did {
            bail!(
                "bearer invite network admin {} does not match local network admin {}; pairing rejected",
                token.network.admin_did,
                admin_did
            );
        }
    }
    Ok(())
}

async fn write_agent_network(node: &EmbeddedNode, record: &NetworkRecord) -> Result<()> {
    let network_id = escape_graphql_string(&record.network_id);
    let admin_did = escape_graphql_string(&record.admin_did);
    let display_name = escape_graphql_string(&record.display_name);
    let default_template = escape_graphql_string(&record.default_template);
    let created_at = escape_graphql_string(&record.created_at);
    let admin_sig = escape_graphql_string(&bs58::encode(&record.sig).into_string());
    let mutation = format!(
        r#"mutation {{
            upsert_AgentNetwork(
                filter: {{ network_id: {{ _eq: "{network_id}" }} }},
                add: {{
                    network_id: "{network_id}",
                    admin_did: "{admin_did}",
                    display_name: "{display_name}",
                    default_template: "{default_template}",
                    created_at: "{created_at}",
                    admin_sig: "{admin_sig}"
                }},
                update: {{
                    admin_did: "{admin_did}",
                    display_name: "{display_name}",
                    default_template: "{default_template}",
                    created_at: "{created_at}",
                    admin_sig: "{admin_sig}"
                }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    ensure_no_errors(&response, "pinning signed bearer network root")
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

fn ensure_no_errors(response: &QueryResponse, label: &str) -> Result<()> {
    if response.has_errors() {
        bail!(
            "{label} failed: {}",
            response
                .errors
                .iter()
                .map(|error| error.message.as_str())
                .collect::<Vec<_>>()
                .join("; ")
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{DesktopPaths, PrincipalIdentity};
    use gents_protocol::bearer_token::{
        decode_bearer, encode_bearer, BearerInviteToken, BEARER_TOKEN_VERSION,
    };

    async fn signed_token(now: DateTime<Utc>) -> (tempfile::TempDir, PrincipalIdentity, String) {
        let temp = tempfile::tempdir().expect("tempdir");
        let identity = PrincipalIdentity::load_or_create(&DesktopPaths::from_root(temp.path()))
            .await
            .expect("identity");
        let mut network = NetworkRecord {
            network_id: "net-amy".into(),
            admin_did: identity.did().to_string(),
            display_name: "Amy".into(),
            default_template: "conversation".into(),
            created_at: now.to_rfc3339_opts(SecondsFormat::Secs, true),
            sig: Vec::new(),
        };
        network.sig = identity
            .sign(&network.signing_payload())
            .expect("sign root");
        let mut token = BearerInviteToken {
            v: BEARER_TOKEN_VERSION,
            issuer_did: identity.did().to_string(),
            peer_id: "6fe391e1c69d66de633034ca40cda6d39ca1a3c94792f2f510add7d1421ea7bb".into(),
            ticket: "127.0.0.1:56000/p2p/6fe391e1c69d66de633034ca40cda6d39ca1a3c94792f2f510add7d1421ea7bb".into(),
            nonce: "nonce-amy".into(),
            network_id: network.network_id.clone(),
            issued_at: now.to_rfc3339_opts(SecondsFormat::Secs, true),
            template: "conversation".into(),
            default_behavior_id: Some("default".into()),
            schema_digest: None,
            network,
            sig: Vec::new(),
        };
        token.sig = identity
            .sign(&bearer_signing_payload(&token))
            .expect("sign invite");
        let encoded = encode_bearer(&token).expect("encode");
        (temp, identity, encoded)
    }

    fn resign(identity: &PrincipalIdentity, mut token: BearerInviteToken) -> String {
        token.sig = Vec::new();
        token.sig = identity
            .sign(&bearer_signing_payload(&token))
            .expect("resign");
        encode_bearer(&token).expect("encode")
    }

    #[tokio::test]
    async fn verify_rejects_schema_digest_mismatch_before_any_write() {
        let now = Utc::now();
        let (_temp, identity, encoded) = signed_token(now).await;
        let mut token = decode_bearer(&encoded).expect("decode");
        token.schema_digest = Some("mismatched-digest".into());
        let encoded = resign(&identity, token);

        let error = verify_bearer_invite(&identity, &encoded, now)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("schema mismatch"), "unexpected: {error}");
        assert!(error.contains("mismatched-digest"), "unexpected: {error}");
    }

    #[tokio::test]
    async fn verify_accepts_matching_schema_digest() {
        let now = Utc::now();
        let (_temp, identity, encoded) = signed_token(now).await;
        let mut token = decode_bearer(&encoded).expect("decode");
        let template = resolve_template(&token.template).expect("template registered");
        let digest = template_schema_digest(template).expect("digest computes");
        token.schema_digest = Some(digest.clone());
        let encoded = resign(&identity, token);

        let verified = verify_bearer_invite(&identity, &encoded, now)
            .await
            .expect("matching digest verifies");
        assert_eq!(verified.token.schema_digest, Some(digest));
    }

    #[tokio::test]
    async fn preview_accepts_cli_compatible_signed_bearer_invite() {
        let now = Utc::now();
        let (_temp, identity, encoded) = signed_token(now).await;

        let verified = verify_bearer_invite(&identity, &encoded, now)
            .await
            .expect("valid token");

        assert_eq!(verified.preview().network_name, "Amy");
        assert_eq!(verified.preview().template, "conversation");
    }

    #[tokio::test]
    async fn preview_accepts_machine_bearer_invite_with_owned_directory_scope() {
        let now = Utc::now();
        let (_temp, identity, encoded) = signed_token(now).await;
        let mut token = decode_bearer(&encoded).expect("decode");
        token.template = "machine".to_string();
        token.sig = identity
            .sign(&bearer_signing_payload(&token))
            .expect("sign machine invite");
        let encoded = encode_bearer(&token).expect("encode");

        let verified = verify_bearer_invite(&identity, &encoded, now)
            .await
            .expect("valid machine token");

        assert_eq!(verified.preview().template, "machine");
    }

    #[tokio::test]
    async fn preview_rejects_expired_bearer_invite() {
        let issued_at = Utc::now() - chrono::Duration::minutes(10);
        let (_temp, identity, encoded) = signed_token(issued_at).await;

        let error = verify_bearer_invite(&identity, &encoded, Utc::now())
            .await
            .expect_err("expired token");

        assert!(error.to_string().contains("re-mint"), "{error}");
    }

    #[tokio::test]
    async fn preview_rejects_tampered_bearer_invite() {
        let now = Utc::now();
        let (_temp, identity, encoded) = signed_token(now).await;
        let mut token = decode_bearer(&encoded).expect("decode");
        token.network.display_name = "Mallory".into();
        let tampered = encode_bearer(&token).expect("encode");

        let error = verify_bearer_invite(&identity, &tampered, now)
            .await
            .expect_err("tampered token");

        assert!(error.to_string().contains("signature invalid"), "{error}");
    }

    #[tokio::test]
    async fn readiness_accepts_only_the_issuer_signed_active_membership() {
        let now = Utc::now();
        let (_temp, identity, _encoded) = signed_token(now).await;
        let mut record = MembershipRecord {
            network_id: "net-amy".into(),
            member_did: "did:key:phone".into(),
            status: "active".into(),
            granted_at: now.to_rfc3339_opts(SecondsFormat::Secs, true),
            revoked_at: String::new(),
            sig: Vec::new(),
        };
        record.sig = identity
            .sign(&record.signing_payload())
            .expect("sign membership");
        let row = MembershipObservationRow {
            network_id: Some(record.network_id.clone()),
            member_did: Some(record.member_did.clone()),
            status: Some(record.status.clone()),
            granted_at: Some(record.granted_at.clone()),
            revoked_at: Some(record.revoked_at.clone()),
            admin_sig: Some(bs58::encode(&record.sig).into_string()),
        };

        verify_active_membership_row(&identity, identity.did(), "net-amy", "did:key:phone", &row)
            .await
            .expect("valid active membership");
    }

    #[tokio::test]
    async fn readiness_rejects_revoked_or_tampered_membership() {
        let now = Utc::now();
        let (_temp, identity, _encoded) = signed_token(now).await;
        let mut record = MembershipRecord {
            network_id: "net-amy".into(),
            member_did: "did:key:phone".into(),
            status: "active".into(),
            granted_at: now.to_rfc3339_opts(SecondsFormat::Secs, true),
            revoked_at: String::new(),
            sig: Vec::new(),
        };
        record.sig = identity
            .sign(&record.signing_payload())
            .expect("sign membership");
        let mut row = MembershipObservationRow {
            network_id: Some(record.network_id.clone()),
            member_did: Some(record.member_did.clone()),
            status: Some("revoked".into()),
            granted_at: Some(record.granted_at.clone()),
            revoked_at: Some(now.to_rfc3339_opts(SecondsFormat::Secs, true)),
            admin_sig: Some(bs58::encode(&record.sig).into_string()),
        };

        let revoked = verify_active_membership_row(
            &identity,
            identity.did(),
            "net-amy",
            "did:key:phone",
            &row,
        )
        .await
        .expect_err("revoked membership");
        assert!(revoked.to_string().contains("active grant"), "{revoked}");

        row.status = Some("active".into());
        row.revoked_at = Some(String::new());
        row.admin_sig = Some(bs58::encode([0_u8; 64]).into_string());
        let tampered = verify_active_membership_row(
            &identity,
            identity.did(),
            "net-amy",
            "did:key:phone",
            &row,
        )
        .await
        .expect_err("tampered membership");
        assert!(
            tampered.to_string().contains("signature is invalid"),
            "{tampered}"
        );
    }

    #[tokio::test]
    async fn readiness_acknowledgement_is_bound_to_current_endpoint() {
        let now = Utc::now();
        let (_temp, identity, _encoded) = signed_token(now).await;
        let endpoint = EndpointRecord {
            did: "did:key:phone".into(),
            node_id: "phone-node".into(),
            address: "phone-ticket".into(),
            updated_at: now.to_rfc3339_opts(SecondsFormat::Secs, true),
            sig: Vec::new(),
        };
        let mut record = BearerPairingReadyRecord {
            issuer_did: identity.did().to_string(),
            claimant_did: endpoint.did.clone(),
            peer_id: endpoint.node_id.clone(),
            address: endpoint.address.clone(),
            template: "conversation".into(),
            acknowledged_at: now.to_rfc3339_opts(SecondsFormat::Secs, true),
            sig: Vec::new(),
        };
        record.sig = identity
            .sign(&record.signing_payload())
            .expect("sign readiness");
        let row = BearerPairingReadyObservationRow {
            issuer_did: Some(record.issuer_did.clone()),
            claimant_did: Some(record.claimant_did.clone()),
            peer_id: Some(record.peer_id.clone()),
            address: Some(record.address.clone()),
            template: Some(record.template.clone()),
            acknowledged_at: Some(record.acknowledged_at.clone()),
            issuer_sig: Some(bs58::encode(&record.sig).into_string()),
        };

        verify_bearer_pairing_ready_row(
            &identity,
            identity.did(),
            &endpoint.did,
            "conversation",
            &endpoint,
            &row,
        )
        .await
        .expect("valid readiness acknowledgement");

        let mut machine_record = record.clone();
        machine_record.template = "machine".to_string();
        machine_record.sig = identity
            .sign(&machine_record.signing_payload())
            .expect("sign machine readiness");
        let machine_row = BearerPairingReadyObservationRow {
            template: Some(machine_record.template.clone()),
            issuer_sig: Some(bs58::encode(&machine_record.sig).into_string()),
            ..row.clone()
        };
        verify_bearer_pairing_ready_row(
            &identity,
            identity.did(),
            &endpoint.did,
            "machine",
            &endpoint,
            &machine_row,
        )
        .await
        .expect("valid machine readiness acknowledgement");

        let mut rotated_endpoint = endpoint;
        rotated_endpoint.address = "rotated-ticket".into();
        let error = verify_bearer_pairing_ready_row(
            &identity,
            identity.did(),
            &rotated_endpoint.did,
            "conversation",
            &rotated_endpoint,
            &row,
        )
        .await
        .expect_err("stale endpoint acknowledgement");
        assert!(error.to_string().contains("current signed endpoint"));
    }

    #[test]
    fn combined_replicator_contains_scoped_conversation_and_claim_control_plane() {
        let collections = bearer_replicator_collections("conversation");
        let filters = bearer_replicator_filters("conversation", "did:key:phone", "did:key:issuer");
        assert!(collections.contains(&"AgentRequest".to_string()));
        assert!(collections.contains(&"AgentResponse".to_string()));
        assert!(collections.contains(&"AgentBehavior".to_string()));
        assert!(collections.contains(&"PairingBearerClaim".to_string()));
        assert!(collections.contains(&"PeerEndpoint".to_string()));
        assert!(collections.contains(&"BearerPairingReady".to_string()));
        assert!(!collections.contains(&"ReciprocalConversationIntent".to_string()));
        assert_eq!(filters.len(), 11);
        for collection in [
            "AgentRequest",
            "AgentResponse",
            "AgentMessage",
            "AgentToolCall",
            "AgentToolResult",
            "AgentSession",
            "AgentConversation",
            "CompactionEntry",
            "BearerPairingReady",
        ] {
            let field = if collection == "BearerPairingReady" {
                "claimant_did"
            } else {
                "requester_did"
            };
            assert_eq!(
                filters.get(collection),
                Some(&ReplicationFilter::eq(
                    field,
                    serde_json::json!("did:key:phone")
                ))
            );
        }
        for collection in [
            "AgentBehavior",
            "ToolSelection",
            "InferenceBackend",
            "InferenceProfile",
            "ToolServiceRegistry",
            "Skill",
        ] {
            assert!(!filters.contains_key(collection));
        }
        assert_eq!(
            filters.get("PairingBearerClaim"),
            Some(&ReplicationFilter::eq(
                "claimant_did",
                serde_json::json!("did:key:phone")
            ))
        );
        assert_eq!(
            filters.get("PeerEndpoint"),
            Some(&ReplicationFilter::eq(
                "did",
                serde_json::json!("did:key:phone")
            ))
        );
        assert_eq!(
            filters.get("AgentRequest"),
            Some(&ReplicationFilter::eq(
                "requester_did",
                serde_json::json!("did:key:phone")
            ))
        );
    }

    #[test]
    fn machine_replicator_adds_only_issuer_owned_directory_rows() {
        let collections = bearer_replicator_collections("machine");
        let filters = bearer_replicator_filters("machine", "did:key:phone", "did:key:issuer");

        assert!(collections.contains(&AGENT_DIRECTORY_COLLECTION.to_string()));
        assert_eq!(
            filters.get(AGENT_DIRECTORY_COLLECTION),
            Some(&ReplicationFilter::eq(
                "source_did",
                serde_json::json!("did:key:issuer")
            ))
        );
        assert_eq!(
            filters.get("AgentRequest"),
            Some(&ReplicationFilter::eq(
                "requester_did",
                serde_json::json!("did:key:phone")
            ))
        );
        assert_eq!(
            filters.get("BearerPairingReady"),
            Some(&ReplicationFilter::eq(
                "claimant_did",
                serde_json::json!("did:key:phone")
            ))
        );
    }

    #[test]
    fn bearer_claim_mutation_escapes_fields_and_never_emits_empty_lists() {
        let record = BearerClaimRecord {
            token: "dabear1-quoted\"".into(),
            claimant_did: "did:key:phone".into(),
            claimant_node_id: "phone-node".into(),
            claimant_address: "endpoint-phone".into(),
            claimed_at: "2026-07-23T00:00:00Z".into(),
            sig: vec![1, 2, 3],
        };

        let mutation = bearer_claim_create_mutation(&record);

        assert!(mutation.contains("dabear1-quoted\\\""));
        assert!(mutation.contains("binding_sig"));
        assert!(!mutation.contains("[]"));
    }

    #[test]
    fn bearer_control_plane_collections_are_guarded_client_authored_collections() {
        // A claimant device bootstrap-pushes these collections before any
        // scope template applies, so each must be fresh-apply guarded
        // (#1123/#1125). This ties the constant to the guard list; the
        // `client_authored_collections_fence` test in the gents crate holds
        // the reverse direction (the guard list matching the full surface).
        for name in BEARER_CONTROL_PLANE_COLLECTIONS {
            assert!(
                gents_migration::CLIENT_AUTHORED_COLLECTIONS.contains(name),
                "{name} is bootstrap-pushed by claimant devices but missing from \
                 gents_migration::CLIENT_AUTHORED_COLLECTIONS — add it so \
                 fresh_apply_parity.rs guards it against chained schema evolution"
            );
        }
    }
}
