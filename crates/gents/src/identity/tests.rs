use super::*;
use crypto::keys::PrivateKey;
use defra_core::signing::{RemoteSigner, SigningAuthorization, SigningConfig, SigningKeyType};
use std::sync::Arc;

#[tokio::test]
async fn key_identity_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("amy-general.key");
    let identity = KeyIdentity::load_or_create(&path, None).unwrap();
    let payload = b"hello world";

    assert!(!identity.did().starts_with("did:test:"));
    let signature = identity.sign(payload).await.unwrap();
    assert!(identity
        .verify(identity.did(), payload, &signature)
        .await
        .unwrap());

    let second = KeyIdentity::load_or_create(path, None).unwrap();
    assert_eq!(identity.did(), second.did());
    assert!(second
        .verify(identity.did(), payload, &signature)
        .await
        .unwrap());

    let signing_config = defra_core::signing::get_identity(identity.did())
        .expect("file identity should register as a DefraDB signer");
    assert!(signing_config.has_local_private_key());
    assert_eq!(signing_config.key_type, SigningKeyType::Ed25519);
}

#[tokio::test]
async fn registered_identity_uses_defradb_local_signing_config() {
    let raw_identity = RawIdentity::from_secp256r1(crypto::generate_secp256r1().unwrap()).unwrap();
    let did = raw_identity.did().unwrap().to_string();
    defra_core::signing::store_identity(
        &did,
        SigningConfig {
            key_type: SigningKeyType::Secp256r1,
            private_key_bytes: SigningConfig::private_key_bytes_from_vec(
                raw_identity.private_key_bytes(),
            ),
            public_key_bytes: raw_identity.public_key_bytes(),
            public_key_hex: String::new(),
            remote_signer: None,
            signing_authorization: None,
        },
    );

    let identity = RegisteredIdentity::from_registered_did(&did, None).unwrap();
    let payload = b"gents registered local signing";
    let signature = identity.sign(payload).await.unwrap();

    assert_eq!(identity.did(), did);
    assert!(identity.verify(&did, payload, &signature).await.unwrap());
}

#[tokio::test]
async fn registered_identity_delegates_to_defradb_remote_signer() {
    let raw_identity = RawIdentity::from_secp256r1(crypto::generate_secp256r1().unwrap()).unwrap();
    let did = raw_identity.did().unwrap().to_string();
    let public_key_bytes = raw_identity.public_key_bytes();
    let private_key_bytes = raw_identity.private_key_bytes();
    defra_core::signing::store_identity(
        &did,
        SigningConfig {
            key_type: SigningKeyType::Secp256r1,
            private_key_bytes: Vec::new(),
            public_key_bytes,
            public_key_hex: String::new(),
            remote_signer: Some(Arc::new(TestRemoteSigner { private_key_bytes })),
            signing_authorization: None,
        },
    );

    let identity = RegisteredIdentity::from_registered_did(&did, None).unwrap();
    let payload = b"gents registered remote signing";
    let signature = identity.sign(payload).await.unwrap();

    assert_eq!(identity.did(), did);
    assert!(identity.verify(&did, payload, &signature).await.unwrap());
}

struct TestRemoteSigner {
    private_key_bytes: Vec<u8>,
}

impl RemoteSigner for TestRemoteSigner {
    fn sign_sync(
        &self,
        data: &[u8],
        _authorization: Option<&SigningAuthorization>,
    ) -> std::result::Result<Vec<u8>, String> {
        let private_key = crypto::Secp256r1PrivateKey::from_bytes(&self.private_key_bytes)
            .map_err(|error| error.to_string())?;
        private_key.sign(data).map_err(|error| error.to_string())
    }
}

/// Verifies that `AgentIdentity::verify` resolves a `did:key` DID to its public key
/// without requiring the signer's key to be pre-registered in the process-local map.
///
/// This reproduces the cross-node signed-invite pairing bug: node B tried to verify
/// a signature from node A's DID but had never registered A's key, so `verify`
/// returned an error instead of `Ok(true)`.
///
/// The test proves the fallback path by using a raw `RawIdentity` (never wrapped in
/// `KeyIdentity`, never registered) to sign a payload, then having a *different*
/// `KeyIdentity` (node B) verify against the raw signer's `did:key` DID.
#[tokio::test]
async fn verify_resolves_did_key_without_prior_registration() {
    use identity::FullIdentity as _;

    // Generate a fresh Ed25519 key that is NEVER registered in the process-local map.
    let raw_private_key = crypto::generate_ed25519().unwrap();
    let raw_public_key_bytes = raw_private_key.to_public_key().raw_owned();
    let raw_did = crypto::create_did_key(crypto::KeyType::Ed25519, &raw_public_key_bytes).unwrap();
    let raw_identity = RawIdentity::from_private_key(raw_private_key).unwrap();

    // Sign a payload with the raw (unregistered) identity.
    let payload = b"cross-node invite payload";
    let signature = raw_identity.sign(payload).unwrap();

    // Build node B's identity using a separate temp key file.
    let dir = tempfile::tempdir().unwrap();
    let path_b = dir.path().join("node-b.key");
    let node_b = KeyIdentity::load_or_create(&path_b, None).unwrap();

    // Node B should NOT have raw_did in its registered-keys map.
    // Verify the signature using raw_did — this exercises the did:key fallback.
    let result = node_b.verify(&raw_did, payload, &signature).await;
    assert!(
        result.is_ok(),
        "verify should succeed for an unregistered did:key, got: {result:?}"
    );
    assert!(
        result.unwrap(),
        "signature over the correct payload should verify as true"
    );

    // A tampered payload must not verify.
    let tampered = b"tampered payload";
    let tampered_result = node_b.verify(&raw_did, tampered, &signature).await;
    // verify returns Ok(false) or Err — either is acceptable; it must not be Ok(true).
    assert!(
        !matches!(tampered_result, Ok(true)),
        "tampered payload must not verify as true, got: {tampered_result:?}"
    );

    // A garbage DID must fail (either Ok(false) or Err) — never Ok(true).
    let garbage_did = "did:key:zGARBAGE_INVALID_DID";
    let garbage_result = node_b.verify(garbage_did, payload, &signature).await;
    assert!(
        !matches!(garbage_result, Ok(true)),
        "garbage DID must not verify as true, got: {garbage_result:?}"
    );
}
