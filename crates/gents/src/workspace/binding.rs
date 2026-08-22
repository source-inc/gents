use anyhow::{bail, Result};

use super::documents::{
    binding_id_for, WorkspaceBindingDoc, BINDING_ACTIVE, BINDING_RELEASED, LIFECYCLE_SEALED,
};
use crate::toolset::{normalize_workspace_lifecycle_state, WorkspaceAuthority};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmitBinding {
    Reuse(WorkspaceBindingDoc),
    Create {
        binding: WorkspaceBindingDoc,
        release: Vec<WorkspaceBindingDoc>,
    },
}

/// Admit an append-only workspace binding.
///
/// ReadWrite is exclusive while Ready. `release_previous_read_write` is the
/// retry path: the previous Active ReadWrite is Released first. After Sealed,
/// ReadWrite is illegal; ReadOnly/Integrate copy and verify `seal_hash`.
pub fn admit_workspace_binding(
    workspace_id: &str,
    workspace_state: &str,
    workspace_seal_hash: Option<&str>,
    existing: &[WorkspaceBindingDoc],
    candidate: WorkspaceBindingDoc,
    release_previous_read_write: bool,
) -> Result<AdmitBinding> {
    let authority = WorkspaceAuthority::parse(&candidate.authority)?;
    if !authority.bindable_lifecycle_state(workspace_state) {
        bail!(
            "isolated workspace {workspace_id} in state {workspace_state} is not bindable for authority {}",
            authority.as_str()
        );
    }
    if candidate.workspace_id != workspace_id {
        bail!(
            "binding workspace_id {} does not match {workspace_id}",
            candidate.workspace_id
        );
    }

    let mut candidate = candidate;
    if normalize_workspace_lifecycle_state(workspace_state) == Some(LIFECYCLE_SEALED) {
        let workspace_hash = workspace_seal_hash
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!("sealed workspace {workspace_id} is missing seal_hash")
            })?;
        match candidate
            .seal_hash
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            None => candidate.seal_hash = Some(workspace_hash.to_string()),
            Some(hash) if hash == workspace_hash => {}
            Some(hash) => bail!(
                "binding seal_hash {hash} does not match workspace seal_hash {workspace_hash}"
            ),
        }
    }

    if matches!(authority, WorkspaceAuthority::ReadWrite) {
        let active: Vec<_> = existing
            .iter()
            .filter(|binding| {
                binding.workspace_id == workspace_id && binding.is_active_read_write()
            })
            .cloned()
            .collect();
        let others: Vec<_> = active
            .iter()
            .filter(|binding| binding.request_id != candidate.request_id)
            .cloned()
            .collect();
        if others.len() > 1 {
            bail!(
                "multiple Active ReadWrite bindings exist for workspace {workspace_id}; failing closed"
            );
        }
        if let Some(existing_active) = active
            .iter()
            .find(|binding| binding.request_id == candidate.request_id)
        {
            return Ok(AdmitBinding::Reuse(existing_active.clone()));
        }
        if !others.is_empty() && !release_previous_read_write {
            bail!("unique Active ReadWrite binding already exists for workspace {workspace_id}");
        }
        let release = if release_previous_read_write {
            others
                .into_iter()
                .map(|mut binding| {
                    binding.lifecycle_state = BINDING_RELEASED.to_string();
                    binding
                })
                .collect()
        } else {
            Vec::new()
        };
        candidate.lifecycle_state = BINDING_ACTIVE.to_string();
        return Ok(AdmitBinding::Create {
            binding: candidate,
            release,
        });
    }

    if let Some(existing_active) = existing.iter().find(|binding| {
        binding.is_active()
            && binding.request_id == candidate.request_id
            && binding.authority == candidate.authority
    }) {
        return Ok(AdmitBinding::Reuse(existing_active.clone()));
    }
    candidate.lifecycle_state = BINDING_ACTIVE.to_string();
    Ok(AdmitBinding::Create {
        binding: candidate,
        release: Vec::new(),
    })
}

pub fn new_binding(
    workspace_id: &str,
    request_id: &str,
    request_doc_id: &str,
    authority: WorkspaceAuthority,
    deployment_id: &str,
    seal_hash: Option<&str>,
) -> WorkspaceBindingDoc {
    WorkspaceBindingDoc {
        binding_id: binding_id_for(workspace_id, request_id),
        workspace_id: workspace_id.to_string(),
        request_id: request_id.to_string(),
        request_doc_id: request_doc_id.to_string(),
        authority: authority.as_str().to_string(),
        deployment_id: deployment_id.to_string(),
        seal_hash: seal_hash
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        lifecycle_state: BINDING_ACTIVE.to_string(),
    }
}

pub fn release_binding(mut binding: WorkspaceBindingDoc) -> WorkspaceBindingDoc {
    binding.lifecycle_state = BINDING_RELEASED.to_string();
    binding
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::toolset::WorkspaceAuthority;

    fn ready_rw(request_id: &str) -> WorkspaceBindingDoc {
        new_binding(
            "ws-1",
            request_id,
            "doc-1",
            WorkspaceAuthority::ReadWrite,
            "dep-1",
            None,
        )
    }

    fn sealed_ro(request_id: &str, hash: &str) -> WorkspaceBindingDoc {
        new_binding(
            "ws-1",
            request_id,
            "doc-1",
            WorkspaceAuthority::ReadOnly,
            "dep-1",
            Some(hash),
        )
    }

    #[test]
    fn unique_active_read_write_while_ready() {
        let first =
            admit_workspace_binding("ws-1", "ready", None, &[], ready_rw("req-1"), false).unwrap();
        let AdmitBinding::Create { binding, .. } = first else {
            panic!("expected create");
        };
        let existing = vec![binding];
        let err =
            admit_workspace_binding("ws-1", "ready", None, &existing, ready_rw("req-2"), false)
                .unwrap_err();
        assert!(
            err.to_string().contains("unique Active ReadWrite"),
            "{err:#}"
        );
    }

    #[test]
    fn two_active_read_write_fail_closed_even_on_retry() {
        let first = ready_rw("req-1");
        let second = ready_rw("req-2");
        let err = admit_workspace_binding(
            "ws-1",
            "ready",
            None,
            &[first, second],
            ready_rw("req-3"),
            true,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("multiple Active ReadWrite"),
            "{err:#}"
        );

        let err = admit_workspace_binding(
            "ws-1",
            "ready",
            None,
            &[ready_rw("req-1"), ready_rw("req-2"), ready_rw("req-3")],
            ready_rw("req-1"),
            true,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("multiple Active ReadWrite"),
            "{err:#}"
        );
    }

    #[test]
    fn retries_release_previous_read_write() {
        let mut previous = ready_rw("req-1");
        previous.lifecycle_state = BINDING_ACTIVE.to_string();
        let admitted =
            admit_workspace_binding("ws-1", "ready", None, &[previous], ready_rw("req-2"), true)
                .unwrap();
        let AdmitBinding::Create { binding, release } = admitted else {
            panic!("expected create after release");
        };
        assert_eq!(binding.request_id, "req-2");
        assert_eq!(release.len(), 1);
        assert_eq!(release[0].lifecycle_state, BINDING_RELEASED);
        assert_eq!(release[0].request_id, "req-1");
    }

    #[test]
    fn no_read_write_after_sealed() {
        let err = admit_workspace_binding(
            "ws-1",
            "sealed",
            Some("hash-1"),
            &[],
            ready_rw("req-1"),
            false,
        )
        .unwrap_err();
        assert!(err.to_string().contains("not bindable"), "{err:#}");
    }

    #[test]
    fn concurrent_read_only_after_seal_copies_hash() {
        let first = admit_workspace_binding(
            "ws-1",
            "sealed",
            Some("hash-1"),
            &[],
            sealed_ro("req-a", ""),
            false,
        )
        .unwrap();
        let AdmitBinding::Create { binding: a, .. } = first else {
            panic!("expected create");
        };
        assert_eq!(a.seal_hash.as_deref(), Some("hash-1"));
        let second = admit_workspace_binding(
            "ws-1",
            "sealed",
            Some("hash-1"),
            std::slice::from_ref(&a),
            sealed_ro("req-b", "hash-1"),
            false,
        )
        .unwrap();
        let AdmitBinding::Create { binding: b, .. } = second else {
            panic!("expected concurrent create");
        };
        assert_eq!(b.seal_hash.as_deref(), Some("hash-1"));
        assert_ne!(a.request_id, b.request_id);
        assert!(a.is_active());
        assert!(b.is_active());
    }

    #[test]
    fn read_only_seal_hash_mismatch_is_denied() {
        let err = admit_workspace_binding(
            "ws-1",
            "sealed",
            Some("hash-1"),
            &[],
            sealed_ro("req-a", "hash-other"),
            false,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("does not match workspace seal_hash"),
            "{err:#}"
        );
    }
}
