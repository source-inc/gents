use std::path::Path;

use anyhow::{Context, Result};
use crypto::Key;
use gents::identity::{register_ed25519_signing_identity, AgentIdentity, ServiceAccount};
use identity::{FullIdentity as _, Identity as _, RawIdentity};
use serde::{Deserialize, Serialize};

use super::paths::DesktopPaths;

#[derive(Debug, Clone)]
pub struct PrincipalIdentity {
    did: String,
    public_key_bytes: Vec<u8>,
    private_key_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PrincipalMetadata {
    did: String,
    public_key_bytes: Vec<u8>,
}

impl PrincipalIdentity {
    pub async fn load_or_create(paths: &DesktopPaths) -> Result<Self> {
        let key_path = paths.identity_key_path();
        let metadata_path = paths.principal_metadata_path();

        if let Some(parent) = key_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating principal directory {}", parent.display()))?;
        }

        let identity = match tokio::fs::read(key_path).await {
            Ok(bytes) => RawIdentity::from_bytes(crypto::KeyType::Ed25519, &bytes)
                .map_err(anyhow::Error::from)
                .with_context(|| {
                    format!("loading principal identity from {}", key_path.display())
                })?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let private_key = crypto::generate_ed25519().map_err(anyhow::Error::from)?;
                let bytes = private_key.raw();
                tokio::fs::write(key_path, &bytes).await.with_context(|| {
                    format!("persisting principal key to {}", key_path.display())
                })?;
                RawIdentity::from_private_key(private_key)
                    .map_err(anyhow::Error::from)
                    .with_context(|| {
                        format!("constructing principal identity at {}", key_path.display())
                    })?
            }
            Err(error) => {
                return Err(anyhow::Error::from(error))
                    .with_context(|| format!("reading principal key {}", key_path.display()));
            }
        };

        let did = identity.did().map_err(anyhow::Error::from)?.to_string();
        let public_key_bytes = identity.public_key_bytes();
        let private_key_bytes = identity.private_key_bytes().to_vec();
        let metadata = PrincipalMetadata {
            did: did.clone(),
            public_key_bytes: public_key_bytes.clone(),
        };

        validate_or_persist_metadata(metadata_path, &metadata).await?;
        register_ed25519_signing_identity(&did, &private_key_bytes, &public_key_bytes)?;

        Ok(Self {
            did,
            public_key_bytes,
            private_key_bytes,
        })
    }

    pub fn did(&self) -> &str {
        &self.did
    }

    pub fn short_did(&self) -> String {
        abbreviate_did(&self.did)
    }

    pub fn public_key_bytes(&self) -> &[u8] {
        &self.public_key_bytes
    }

    pub fn private_key_bytes(&self) -> &[u8] {
        &self.private_key_bytes
    }

    pub(crate) fn sign(&self, payload: &[u8]) -> Result<Vec<u8>> {
        RawIdentity::from_bytes(crypto::KeyType::Ed25519, &self.private_key_bytes)
            .map_err(anyhow::Error::from)
            .with_context(|| format!("loading principal identity for {}", self.did))?
            .sign(payload)
            .map_err(anyhow::Error::from)
            .with_context(|| format!("signing payload as {}", self.did))
    }
}

#[async_trait::async_trait]
impl AgentIdentity for PrincipalIdentity {
    fn did(&self) -> &str {
        &self.did
    }

    async fn sign(&self, payload: &[u8]) -> Result<Vec<u8>> {
        PrincipalIdentity::sign(self, payload)
    }

    async fn verify(&self, did: &str, payload: &[u8], signature: &[u8]) -> Result<bool> {
        let (key_type, public_key_bytes) = if did == self.did {
            (crypto::KeyType::Ed25519, self.public_key_bytes.clone())
        } else if did.starts_with("did:key:") {
            crypto::parse_did_key(did)
                .map_err(anyhow::Error::from)
                .with_context(|| format!("parsing did:key public key from DID {did}"))?
        } else {
            anyhow::bail!("no public key registered for DID {did}");
        };

        let public_key = crypto::public_key_from_bytes(key_type, &public_key_bytes)
            .map_err(anyhow::Error::from)?;
        public_key
            .verify(payload, signature)
            .map_err(anyhow::Error::from)
            .with_context(|| format!("verifying payload for {did}"))
    }

    fn service_account(&self) -> Option<&ServiceAccount> {
        None
    }
}

async fn validate_or_persist_metadata(path: &Path, expected: &PrincipalMetadata) -> Result<()> {
    match tokio::fs::read(path).await {
        Ok(bytes) => {
            let stored: PrincipalMetadata = serde_json::from_slice(&bytes)
                .with_context(|| format!("parsing principal metadata {}", path.display()))?;
            if stored != *expected {
                return Err(anyhow::anyhow!(
                    "principal metadata mismatch at {}",
                    path.display()
                ));
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            write_json_atomically(path, expected).await
        }
        Err(error) => {
            Err(anyhow::Error::from(error)).with_context(|| format!("reading {}", path.display()))
        }
    }
}

async fn write_json_atomically<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    let tmp_path = path.with_extension("tmp");

    tokio::fs::write(&tmp_path, bytes)
        .await
        .with_context(|| format!("writing {}", tmp_path.display()))?;
    if tokio::fs::try_exists(path).await.unwrap_or(false) {
        let _ = tokio::fs::remove_file(path).await;
    }
    tokio::fs::rename(&tmp_path, path)
        .await
        .with_context(|| format!("persisting {}", path.display()))?;
    Ok(())
}

fn abbreviate_did(did: &str) -> String {
    if did.len() <= 20 {
        return did.to_string();
    }

    format!("{}..{}", &did[..16], &did[did.len() - 4..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn first_launch_creates_principal_files() {
        let tempdir = tempfile::tempdir().unwrap();
        let paths = DesktopPaths::from_root(tempdir.path());

        let identity = PrincipalIdentity::load_or_create(&paths).await.unwrap();

        assert!(!identity.did().is_empty());
        assert!(tokio::fs::try_exists(paths.identity_key_path())
            .await
            .unwrap());
        assert!(tokio::fs::try_exists(paths.principal_metadata_path())
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn second_launch_reuses_same_identity_material() {
        let tempdir = tempfile::tempdir().unwrap();
        let paths = DesktopPaths::from_root(tempdir.path());

        let first = PrincipalIdentity::load_or_create(&paths).await.unwrap();
        let second = PrincipalIdentity::load_or_create(&paths).await.unwrap();

        assert_eq!(first.did(), second.did());
        assert_eq!(first.public_key_bytes(), second.public_key_bytes());
        assert_eq!(first.private_key_bytes(), second.private_key_bytes());
    }
}
