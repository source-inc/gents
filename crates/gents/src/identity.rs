use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use crypto::Key;
use defra_core::signing::{RemoteSigner, SigningConfig, SigningKeyType};
use identity::{FullIdentity as _, Identity as _, RawIdentity};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceAccount {
    pub host_id: String,
    pub deployment_id: String,
}

/// Deployment-level principal record.
///
/// Represents the DID-backed principal for a gents deployment.
/// The runtime constructs a single instance per deployment and shares
/// it as `Arc<AgentPrincipal>` across all behaviors; the type itself
/// does not enforce this constraint — the single-principal invariant
/// lives in the loader and will be fenced by a loader-dedup proptest
/// (Task 12, `tests/identity_conformance_proptest.rs`).
///
/// Owns the signing identity used for every DefraDB op the runtime
/// issues. Every `AgentBehavior` on the deployment holds an
/// `Arc<AgentPrincipal>` back-reference; the back-reference makes
/// Lean's `behavior_id_determines_principal` theorem (`Identity.Properties`)
/// structural at the type level (no path constructs a behavior with a
/// dangling agent_did).
///
/// Extends the Lean `Identity.Principal` record in
/// `crates/gents/proofs/Proofs/Identity/State.lean`
/// (`Identity.Principal`) with the live signing handle (`identity`)
/// and the routing shortcut (`default_behavior_id`).
#[derive(Clone)]
pub struct AgentPrincipal {
    pub agent_did: String,
    pub identity: Arc<dyn AgentIdentity>,
    pub default_behavior_id: String,
    pub display_name: Option<String>,
    pub enabled: bool,
}

impl std::fmt::Debug for AgentPrincipal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentPrincipal")
            .field("agent_did", &self.agent_did)
            .field("default_behavior_id", &self.default_behavior_id)
            .field("display_name", &self.display_name)
            .field("enabled", &self.enabled)
            .finish_non_exhaustive()
    }
}

#[async_trait]
pub trait AgentIdentity: Send + Sync {
    fn did(&self) -> &str;

    async fn sign(&self, payload: &[u8]) -> Result<Vec<u8>>;

    async fn verify(&self, did: &str, payload: &[u8], signature: &[u8]) -> Result<bool>;

    fn service_account(&self) -> Option<&ServiceAccount>;
}

#[derive(Debug, Clone)]
struct KnownPublicKey {
    key_type: crypto::KeyType,
    bytes: Vec<u8>,
}

fn known_public_keys() -> &'static RwLock<HashMap<String, KnownPublicKey>> {
    static TYPED_KEYS: OnceLock<RwLock<HashMap<String, KnownPublicKey>>> = OnceLock::new();
    TYPED_KEYS.get_or_init(|| RwLock::new(HashMap::new()))
}

#[derive(Debug)]
pub struct KeyIdentity {
    did: String,
    service_account: Option<ServiceAccount>,
    identity: Arc<RawIdentity>,
}

impl KeyIdentity {
    pub fn load_or_create(
        key_path: impl Into<PathBuf>,
        service_account: Option<ServiceAccount>,
    ) -> Result<Self> {
        let identity = Arc::new(load_or_create_identity(&key_path.into())?);
        let did = identity.did().map_err(anyhow::Error::from)?.to_string();
        let public_key_bytes = identity.public_key_bytes();
        register_public_key(&did, identity.key_type(), public_key_bytes.clone());
        register_ed25519_signing_identity(&did, &identity.private_key_bytes(), &public_key_bytes)?;
        Ok(Self {
            did,
            service_account,
            identity,
        })
    }
}

pub fn register_ed25519_signing_identity(
    did: &str,
    private_key_bytes: &[u8],
    public_key_bytes: &[u8],
) -> Result<()> {
    let identity = RawIdentity::from_bytes(crypto::KeyType::Ed25519, private_key_bytes)
        .map_err(anyhow::Error::from)
        .context("loading Ed25519 node signing identity")?;
    let derived_did = identity.did().map_err(anyhow::Error::from)?.to_string();
    if derived_did != did {
        anyhow::bail!("node signing DID mismatch: expected {did}, derived {derived_did}");
    }
    if identity.public_key_bytes() != public_key_bytes {
        anyhow::bail!("node signing public key does not match DID {did}");
    }

    defra_core::signing::store_identity(
        did,
        SigningConfig {
            key_type: SigningKeyType::Ed25519,
            private_key_bytes: SigningConfig::private_key_bytes_from_vec(
                private_key_bytes.to_vec(),
            ),
            public_key_bytes: public_key_bytes.to_vec(),
            public_key_hex: lowercase_hex(public_key_bytes),
            remote_signer: None,
            signing_authorization: None,
        },
    );
    Ok(())
}

#[async_trait]
impl AgentIdentity for KeyIdentity {
    fn did(&self) -> &str {
        &self.did
    }

    async fn sign(&self, payload: &[u8]) -> Result<Vec<u8>> {
        self.identity
            .sign(payload)
            .map_err(anyhow::Error::from)
            .with_context(|| format!("signing payload for {}", self.did))
    }

    async fn verify(&self, did: &str, payload: &[u8], signature: &[u8]) -> Result<bool> {
        let public_key = if did == self.did {
            KnownPublicKey {
                key_type: self.identity.key_type(),
                bytes: self.identity.public_key_bytes(),
            }
        } else {
            known_public_key_for_did(did)?
        };

        verify_with_public_key(did, public_key, payload, signature)
    }

    fn service_account(&self) -> Option<&ServiceAccount> {
        self.service_account.as_ref()
    }
}

pub struct RegisteredIdentity {
    did: String,
    service_account: Option<ServiceAccount>,
    config: SigningConfig,
}

impl RegisteredIdentity {
    pub fn from_registered_did(
        did: impl Into<String>,
        service_account: Option<ServiceAccount>,
    ) -> Result<Self> {
        let did = did.into();
        let config = defra_core::signing::get_identity(&did)
            .ok_or_else(|| anyhow!("no DefraDB signing identity registered for DID {did}"))?;
        validate_registered_identity_config(&did, &config)?;
        register_public_key(
            &did,
            signing_key_type_to_crypto_key_type(config.key_type)?,
            config.public_key_bytes.clone(),
        );
        Ok(Self {
            did,
            service_account,
            config,
        })
    }
}

pub fn load_or_create_macos_secure_enclave_identity(
    label: &str,
    service_account: Option<ServiceAccount>,
) -> Result<RegisteredIdentity> {
    register_macos_secure_enclave_identity(label, true, service_account)
}

pub fn load_macos_secure_enclave_identity(
    label: &str,
    service_account: Option<ServiceAccount>,
) -> Result<RegisteredIdentity> {
    register_macos_secure_enclave_identity(label, false, service_account)
}

pub fn load_or_create_macos_keychain_identity(
    label: &str,
    service_account: Option<ServiceAccount>,
) -> Result<RegisteredIdentity> {
    register_macos_keychain_identity(label, true, service_account)
}

pub fn load_macos_keychain_identity(
    label: &str,
    service_account: Option<ServiceAccount>,
) -> Result<RegisteredIdentity> {
    register_macos_keychain_identity(label, false, service_account)
}

impl std::fmt::Debug for RegisteredIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredIdentity")
            .field("did", &self.did)
            .field("key_type", &self.config.key_type)
            .field(
                "has_local_private_key",
                &self.config.has_local_private_key(),
            )
            .field("has_remote_signer", &self.config.has_remote_signer())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl AgentIdentity for RegisteredIdentity {
    fn did(&self) -> &str {
        &self.did
    }

    async fn sign(&self, payload: &[u8]) -> Result<Vec<u8>> {
        if let Some(signer) = self.config.remote_signer.clone() {
            let payload = payload.to_vec();
            let authorization = self.config.signing_authorization.clone();
            return tokio::task::spawn_blocking(move || {
                signer.sign_sync(&payload, authorization.as_ref())
            })
            .await
            .context("joining DefraDB remote signing task")?
            .map_err(anyhow::Error::msg)
            .with_context(|| format!("signing payload for {}", self.did));
        }

        let identity = raw_identity_from_signing_config(&self.config)?;
        identity
            .sign(payload)
            .map_err(anyhow::Error::from)
            .with_context(|| format!("signing payload for {}", self.did))
    }

    async fn verify(&self, did: &str, payload: &[u8], signature: &[u8]) -> Result<bool> {
        let public_key = if did == self.did {
            KnownPublicKey {
                key_type: signing_key_type_to_crypto_key_type(self.config.key_type)?,
                bytes: self.config.public_key_bytes.clone(),
            }
        } else {
            known_public_key_for_did(did)?
        };

        verify_with_public_key(did, public_key, payload, signature)
    }

    fn service_account(&self) -> Option<&ServiceAccount> {
        self.service_account.as_ref()
    }
}

fn register_public_key(did: &str, key_type: crypto::KeyType, public_key: Vec<u8>) {
    known_public_keys()
        .write()
        .expect("known public keys lock poisoned")
        .insert(
            did.to_string(),
            KnownPublicKey {
                key_type,
                bytes: public_key,
            },
        );
}

#[cfg(target_os = "macos")]
const ERR_SEC_ITEM_NOT_FOUND: i32 = -25300;
#[cfg(target_os = "macos")]
const ERR_SEC_MISSING_ENTITLEMENT: i32 = -34018;

#[cfg(target_os = "macos")]
fn register_macos_secure_enclave_identity(
    label: &str,
    create_if_missing: bool,
    service_account: Option<ServiceAccount>,
) -> Result<RegisteredIdentity> {
    let signer = MacosSecureEnclaveSigner::load(label, create_if_missing)?;
    let did = signer.did.clone();
    let public_key_bytes = signer.public_key_bytes.clone();
    defra_core::signing::store_identity(
        &did,
        SigningConfig {
            key_type: SigningKeyType::Secp256r1,
            private_key_bytes: Vec::new(),
            public_key_bytes: public_key_bytes.clone(),
            public_key_hex: lowercase_hex(&public_key_bytes),
            remote_signer: Some(Arc::new(signer)),
            signing_authorization: None,
        },
    );
    RegisteredIdentity::from_registered_did(did, service_account)
}

#[cfg(not(target_os = "macos"))]
fn register_macos_secure_enclave_identity(
    _label: &str,
    _create_if_missing: bool,
    _service_account: Option<ServiceAccount>,
) -> Result<RegisteredIdentity> {
    anyhow::bail!("macos-secure-enclave identity backend is only available on macOS")
}

#[cfg(target_os = "macos")]
fn register_macos_keychain_identity(
    label: &str,
    create_if_missing: bool,
    service_account: Option<ServiceAccount>,
) -> Result<RegisteredIdentity> {
    let identity = load_or_create_macos_keychain_raw_identity(label, create_if_missing)?;
    let did = identity.did().map_err(anyhow::Error::from)?.to_string();
    let public_key_bytes = identity.public_key_bytes();
    defra_core::signing::store_identity(
        &did,
        SigningConfig {
            key_type: SigningKeyType::Ed25519,
            private_key_bytes: SigningConfig::private_key_bytes_from_vec(
                identity.private_key_bytes(),
            ),
            public_key_bytes: public_key_bytes.clone(),
            public_key_hex: lowercase_hex(&public_key_bytes),
            remote_signer: None,
            signing_authorization: None,
        },
    );
    RegisteredIdentity::from_registered_did(did, service_account)
}

#[cfg(not(target_os = "macos"))]
fn register_macos_keychain_identity(
    _label: &str,
    _create_if_missing: bool,
    _service_account: Option<ServiceAccount>,
) -> Result<RegisteredIdentity> {
    anyhow::bail!("macos-keychain identity backend is only available on macOS")
}

#[cfg(target_os = "macos")]
fn load_or_create_macos_keychain_raw_identity(
    label: &str,
    create_if_missing: bool,
) -> Result<RawIdentity> {
    let keychain = security_framework::os::macos::keychain::SecKeychain::default()
        .context("loading default macOS keychain")?;
    match keychain.find_generic_password(MACOS_KEYCHAIN_SERVICE, label) {
        Ok((password, _)) => RawIdentity::from_bytes(crypto::KeyType::Ed25519, password.as_ref())
            .map_err(anyhow::Error::from)
            .with_context(|| format!("loading macOS keychain identity {label}")),
        Err(error) if error.code() == ERR_SEC_ITEM_NOT_FOUND => {
            if !create_if_missing {
                anyhow::bail!("macOS keychain identity not found for label {label}");
            }
            let private_key = crypto::generate_ed25519().map_err(anyhow::Error::from)?;
            let bytes = private_key.raw();
            keychain
                .set_generic_password(MACOS_KEYCHAIN_SERVICE, label, bytes)
                .with_context(|| format!("storing macOS keychain identity {label}"))?;
            RawIdentity::from_private_key(private_key)
                .map_err(anyhow::Error::from)
                .with_context(|| format!("constructing macOS keychain identity {label}"))
        }
        Err(error) => Err(anyhow::Error::from(error))
            .with_context(|| format!("reading macOS keychain identity {label}")),
    }
}

#[cfg(target_os = "macos")]
const MACOS_KEYCHAIN_SERVICE: &str = "com.source-inc.gents.identity";

#[cfg(target_os = "macos")]
#[derive(Debug)]
struct MacosSecureEnclaveSigner {
    label: String,
    key: security_framework::key::SecKey,
    did: String,
    public_key_bytes: Vec<u8>,
}

#[cfg(target_os = "macos")]
impl MacosSecureEnclaveSigner {
    fn load(label: &str, create_if_missing: bool) -> Result<Self> {
        let key = match find_macos_secure_enclave_key(label)? {
            Some(key) => key,
            None if create_if_missing => create_macos_secure_enclave_key(label)?,
            None => anyhow::bail!("macOS Secure Enclave key not found for label {label}"),
        };
        let public_key_bytes = public_key_bytes_for_sec_key(&key)
            .with_context(|| format!("loading public key for Secure Enclave key {label}"))?;
        let did = did_for_secp256r1_public_key(&public_key_bytes)
            .with_context(|| format!("deriving DID for Secure Enclave key {label}"))?;
        Ok(Self {
            label: label.to_string(),
            key,
            did,
            public_key_bytes,
        })
    }
}

#[cfg(target_os = "macos")]
impl RemoteSigner for MacosSecureEnclaveSigner {
    fn sign_sync(
        &self,
        data: &[u8],
        _authorization: Option<&defra_core::signing::SigningAuthorization>,
    ) -> std::result::Result<Vec<u8>, String> {
        self.key
            .create_signature(
                security_framework::key::Algorithm::ECDSASignatureMessageX962SHA256,
                data,
            )
            .map_err(|error| {
                format!(
                    "Secure Enclave signing failed for label {}: {error}",
                    self.label
                )
            })
    }
}

#[cfg(target_os = "macos")]
fn find_macos_secure_enclave_key(label: &str) -> Result<Option<security_framework::key::SecKey>> {
    use security_framework::item::{ItemSearchOptions, KeyClass, Reference, SearchResult};

    let mut search = ItemSearchOptions::new();
    search
        .key_class(KeyClass::private())
        .label(label)
        .ignore_legacy_keychains()
        .load_refs(true)
        .limit(2);
    let keys = match search.search() {
        Ok(keys) => keys,
        Err(error) if error.code() == ERR_SEC_ITEM_NOT_FOUND => {
            return Ok(None);
        }
        Err(error) if error.code() == ERR_SEC_MISSING_ENTITLEMENT => {
            return Err(missing_secure_enclave_entitlement_error(label));
        }
        Err(error) => {
            return Err(anyhow::Error::from(error)).with_context(|| {
                format!("searching keychain for macOS Secure Enclave key label {label}")
            });
        }
    };
    let mut matches = keys.into_iter().filter_map(|item| match item {
        SearchResult::Ref(Reference::Key(key)) => Some(key),
        _ => None,
    });
    let first = matches.next();
    if matches.next().is_some() {
        anyhow::bail!("multiple keychain private keys found for label {label}");
    }
    Ok(first)
}

#[cfg(target_os = "macos")]
fn create_macos_secure_enclave_key(label: &str) -> Result<security_framework::key::SecKey> {
    use security_framework::access_control::SecAccessControl;
    use security_framework::item::Location;
    use security_framework::key::{GenerateKeyOptions, KeyType, SecKey, Token};
    use security_framework::passwords_options::AccessControlOptions;

    let access_control =
        SecAccessControl::create_with_flags(AccessControlOptions::PRIVATE_KEY_USAGE.bits())
            .context("creating Secure Enclave key access control")?;
    let mut options = GenerateKeyOptions::default();
    options
        .set_key_type(KeyType::ec())
        .set_size_in_bits(256)
        .set_label(label)
        .set_access_control(access_control)
        .set_token(Token::SecureEnclave)
        .set_location(Location::DataProtectionKeychain);
    match SecKey::new(&options) {
        Ok(key) => Ok(key),
        Err(error) if error.code() == ERR_SEC_MISSING_ENTITLEMENT as isize => {
            Err(missing_secure_enclave_entitlement_error(label))
        }
        Err(error) => Err(anyhow!("{error}"))
            .with_context(|| format!("creating macOS Secure Enclave key label {label}")),
    }
}

#[cfg(target_os = "macos")]
fn missing_secure_enclave_entitlement_error(label: &str) -> anyhow::Error {
    anyhow!(
        "macOS Secure Enclave key label {label} requires a codesigned binary with Data Protection keychain access-group entitlements"
    )
}

#[cfg(target_os = "macos")]
fn public_key_bytes_for_sec_key(key: &security_framework::key::SecKey) -> Result<Vec<u8>> {
    let public_key = key
        .public_key()
        .ok_or_else(|| anyhow!("Secure Enclave key has no public key"))?;
    let data = public_key
        .external_representation()
        .ok_or_else(|| anyhow!("Secure Enclave public key is not exportable"))?;
    Ok(data.to_vec())
}

fn did_for_secp256r1_public_key(public_key_bytes: &[u8]) -> Result<String> {
    let public_key = crypto::public_key_from_bytes(crypto::KeyType::Secp256r1, public_key_bytes)
        .map_err(anyhow::Error::from)?;
    public_key.did().map_err(anyhow::Error::from)
}

fn lowercase_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn known_public_key_for_did(did: &str) -> Result<KnownPublicKey> {
    if let Some(key) = known_public_keys()
        .read()
        .expect("known public keys lock poisoned")
        .get(did)
        .cloned()
    {
        return Ok(key);
    }

    if did.starts_with("did:key:") {
        let (key_type, bytes) = crypto::parse_did_key(did)
            .map_err(anyhow::Error::from)
            .with_context(|| format!("parsing did:key public key from DID {did}"))?;
        tracing::trace!(
            did,
            ?key_type,
            "resolved public key from did:key (not in local registry)"
        );
        return Ok(KnownPublicKey { key_type, bytes });
    }

    anyhow::bail!("no public key registered for DID {did}")
}

fn verify_with_public_key(
    did: &str,
    public_key: KnownPublicKey,
    payload: &[u8],
    signature: &[u8],
) -> Result<bool> {
    let public_key = crypto::public_key_from_bytes(public_key.key_type, &public_key.bytes)
        .map_err(anyhow::Error::from)?;
    public_key
        .verify(payload, signature)
        .map_err(anyhow::Error::from)
        .with_context(|| format!("verifying payload for {did}"))
}

fn validate_registered_identity_config(did: &str, config: &SigningConfig) -> Result<()> {
    if !config.has_local_private_key() && !config.has_remote_signer() {
        anyhow::bail!("registered identity {did} has neither a local key nor a remote signer");
    }
    if config.public_key_bytes.is_empty() {
        anyhow::bail!("registered identity {did} has no public key bytes");
    }

    let public_key = crypto::public_key_from_bytes(
        signing_key_type_to_crypto_key_type(config.key_type)?,
        &config.public_key_bytes,
    )
    .map_err(anyhow::Error::from)
    .with_context(|| format!("loading public key for registered identity {did}"))?;
    let derived_did = public_key
        .did()
        .map_err(anyhow::Error::from)
        .with_context(|| format!("deriving DID for registered identity {did}"))?;
    if derived_did != did {
        anyhow::bail!("registered identity DID mismatch: expected {did}, derived {derived_did}");
    }

    Ok(())
}

fn raw_identity_from_signing_config(config: &SigningConfig) -> Result<RawIdentity> {
    if config.private_key_bytes.is_empty() {
        anyhow::bail!(
            "registered identity has no local private key and no remote signer was available"
        );
    }
    RawIdentity::from_bytes(
        signing_key_type_to_crypto_key_type(config.key_type)?,
        &config.private_key_bytes,
    )
    .map_err(anyhow::Error::from)
    .context("constructing identity from DefraDB signing config")
}

fn signing_key_type_to_crypto_key_type(key_type: SigningKeyType) -> Result<crypto::KeyType> {
    match key_type {
        SigningKeyType::Ed25519 => Ok(crypto::KeyType::Ed25519),
        SigningKeyType::Secp256k1 => Ok(crypto::KeyType::Secp256k1),
        SigningKeyType::Secp256r1 => Ok(crypto::KeyType::Secp256r1),
        SigningKeyType::Bls => {
            anyhow::bail!("BLS registered identities cannot be used as gents runtime identities")
        }
        other => anyhow::bail!(
            "registered identity key type {other} cannot be used as a gents runtime identity"
        ),
    }
}

fn load_or_create_identity(path: &Path) -> Result<RawIdentity> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating key directory {}", parent.display()))?;
    }

    match std::fs::read(path) {
        Ok(bytes) => RawIdentity::from_bytes(crypto::KeyType::Ed25519, &bytes)
            .map_err(anyhow::Error::from)
            .with_context(|| format!("loading identity from {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let private_key = crypto::generate_ed25519().map_err(anyhow::Error::from)?;
            let bytes = private_key.raw();
            std::fs::write(path, &bytes)
                .with_context(|| format!("persisting identity key to {}", path.display()))?;
            RawIdentity::from_private_key(private_key)
                .map_err(anyhow::Error::from)
                .with_context(|| format!("constructing identity from {}", path.display()))
        }
        Err(error) => {
            Err(anyhow::Error::from(error)).with_context(|| format!("reading {}", path.display()))
        }
    }
}

#[cfg(test)]
mod tests;
