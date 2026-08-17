//! Schema-bundle fingerprint carried in the bearer invite token (issue
//! #1122).
//!
//! A paired client whose bundled SDLs have drifted from the server's admits
//! cleanly — replicated reads keep flowing — but every document the client
//! *authors* is merge-rejected forever with `Collection not found for
//! schema_version_id`, and nothing surfaces that to the operator. The fix is
//! a pre-pairing handshake: the issuer stamps a short digest of the SDLs its
//! invite's template will push into the (signed) [`crate::bearer_token::BearerInviteToken`],
//! and the claimant recomputes the same digest locally before writing any
//! pairing row.
//!
//! This can't be a new field on `PairingBearerClaim` or `BearerPairingReady`:
//! both are listed in `gents_migration::CLIENT_AUTHORED_COLLECTIONS`
//! (crates/gents-migration/src/registry.rs:568-610), so adding a field would
//! force a baseline re-pin (crates/gents-migration/tests/fresh_apply_parity.rs)
//! that breaks precisely the stale clients this feature exists to warn about.
//! The invite token is server-minted, signed, and parsed by the claimant
//! before any document is authored — it is the only channel that reaches the
//! client pre-pairing without an SDL change.
//!
//! Deliberately short (8 bytes / ~11 bs58 chars): it rides a QR-size-
//! constrained token alongside the rest of the bearer invite payload.

use sha2::{Digest, Sha256};

/// Fingerprint a bundle of `(collection_name, sdl_text)` pairs.
///
/// Each SDL is canonicalized before hashing so purely cosmetic edits
/// (comment wording, re-indentation) don't false-positive a mismatch:
/// whole-line and trailing `#` comments are stripped, each line is trimmed,
/// blank lines are dropped, and internal whitespace runs collapse to a
/// single space. Pairs are sorted by collection name so input order never
/// affects the result. A real schema change (field added/removed/reordered)
/// changes the canonical text and therefore the digest.
pub fn schema_bundle_digest(collections: &[(&str, &str)]) -> String {
    let mut sorted: Vec<(&str, &str)> = collections.to_vec();
    sorted.sort_by_key(|(name, _)| *name);

    let mut hasher = Sha256::new();
    for (name, sdl) in sorted {
        hasher.update(name.as_bytes());
        hasher.update(b"\n");
        hasher.update(canonicalize_sdl(sdl).as_bytes());
        hasher.update(b"\n");
    }
    let digest = hasher.finalize();
    bs58::encode(&digest[..8]).into_string()
}

/// Strip comments and normalize whitespace so cosmetic-only SDL edits hash
/// identically to the original.
fn canonicalize_sdl(sdl: &str) -> String {
    sdl.lines()
        .map(strip_comment)
        .map(collapse_whitespace)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Drop everything from the first unescaped `#` onward (SDL in this codebase
/// never quotes a literal `#`, so a naive first-index truncation is safe —
/// see `every_agent_schema_starts_with_type_declaration` and friends, which
/// would fail loudly if that assumption ever broke).
fn strip_comment(line: &str) -> &str {
    match line.find('#') {
        Some(idx) => &line[..idx],
        None => line,
    }
}

fn collapse_whitespace(line: &str) -> String {
    line.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_inputs_produce_the_same_digest() {
        let a: &[(&str, &str)] = &[("Foo", "type Foo { id: String }")];
        let b: &[(&str, &str)] = &[("Foo", "type Foo { id: String }")];
        assert_eq!(schema_bundle_digest(a), schema_bundle_digest(b));
    }

    #[test]
    fn comment_only_edits_do_not_change_the_digest() {
        let original: &[(&str, &str)] = &[(
            "Foo",
            "# a comment\ntype Foo {\n    id: String # inline note\n}\n",
        )];
        let recommented: &[(&str, &str)] = &[(
            "Foo",
            "# a totally different comment\ntype Foo {\n    id: String # a different inline note\n}\n",
        )];
        assert_eq!(
            schema_bundle_digest(original),
            schema_bundle_digest(recommented)
        );
    }

    #[test]
    fn whitespace_only_edits_do_not_change_the_digest() {
        let original: &[(&str, &str)] = &[("Foo", "type Foo {\n    id: String\n}\n")];
        let reindented: &[(&str, &str)] =
            &[("Foo", "type   Foo   {\n\tid:    String\n}\n\n\n")];
        assert_eq!(
            schema_bundle_digest(original),
            schema_bundle_digest(reindented)
        );
    }

    #[test]
    fn a_real_field_change_produces_a_different_digest() {
        let before: &[(&str, &str)] = &[("Foo", "type Foo {\n    id: String\n}\n")];
        let added: &[(&str, &str)] =
            &[("Foo", "type Foo {\n    id: String\n    extra: Int\n}\n")];
        assert_ne!(schema_bundle_digest(before), schema_bundle_digest(added));

        let removed: &[(&str, &str)] = &[("Foo", "type Foo {\n}\n")];
        assert_ne!(schema_bundle_digest(before), schema_bundle_digest(removed));

        let reordered: &[(&str, &str)] = &[(
            "Foo",
            "type Foo {\n    extra: Int\n    id: String\n}\n",
        )];
        assert_ne!(schema_bundle_digest(added), schema_bundle_digest(reordered));
    }

    #[test]
    fn input_slice_order_does_not_matter() {
        let a: &[(&str, &str)] = &[
            ("Foo", "type Foo { id: String }"),
            ("Bar", "type Bar { id: String }"),
        ];
        let b: &[(&str, &str)] = &[
            ("Bar", "type Bar { id: String }"),
            ("Foo", "type Foo { id: String }"),
        ];
        assert_eq!(schema_bundle_digest(a), schema_bundle_digest(b));
    }
}
