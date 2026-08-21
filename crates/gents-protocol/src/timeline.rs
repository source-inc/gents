//! Presentation-neutral timeline ordering (#608 parity).
//!
//! Every client shell — desktop today, mobile next — must render a session's
//! transcript in the **same order**, interleaving assistant/user messages with
//! their tool groups, placing the pending turn, and ordering orphan tool groups
//! around the live-assistant overlay. The *order and the message↔tool-group
//! partition* are semantics; only the pixels are presentation.
//!
//! That ordering used to live only in the desktop Tauri bridge
//! (`build_rendered_timeline`), unshared and unfenced — the single biggest
//! parity risk, because a second shell that re-interleaves will drift on order
//! and on which tool group is an orphan. This module is the shared, Lean-fenced
//! skeleton (`proofs/Proofs/ClientShell/Timeline.lean`): shells compute the slot
//! order here, then map each neutral slot to their own rich item.

/// A message's role, reduced to what the ordering cares about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimelineRole {
    User,
    Assistant,
}

/// The ordering-relevant projection of one transcript message.
///
/// A shell fills this from its rich view; the skeleton reads only these fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelineMessageInput {
    /// Stable identity for first-wins dedup (the desktop `message_key`).
    pub key: String,
    /// Ordering key and tool-group attach key. `None` sorts first, matching the
    /// `BTreeMap<Option<i64>, _>` grouping the desktop bridge uses.
    pub sequence: Option<i64>,
    pub role: TimelineRole,
    /// Whether this message contributes a visible message slot. A message can
    /// survive dedup, emit no slot (e.g. a user turn with no rendered content),
    /// and still own a tool group — so this is separate from dedup.
    pub emits_item: bool,
    /// Opaque secondary dedup token (the desktop presentation key). `None` opts
    /// out of presentation dedup. The skeleton only compares tokens for
    /// equality — it never inspects content, so it stays presentation-neutral.
    pub dedup_token: Option<String>,
}

/// The request-ownership fields needed to decide whether a durable user
/// message has taken over from the request document's pending projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableUserOwnerInput<'a> {
    pub request_id: Option<&'a str>,
    pub is_user: bool,
    pub has_visible_content: bool,
    pub runtime_control: bool,
}

/// A durable user message owns the pending projection only when it names the
/// exact request. Missing and unrelated request ids deliberately do not fall
/// back to content or turn-count heuristics.
pub fn has_durable_user_owner(messages: &[DurableUserOwnerInput<'_>], request_id: &str) -> bool {
    messages.iter().any(|message| {
        message.request_id == Some(request_id)
            && message.is_user
            && message.has_visible_content
            && !message.runtime_control
    })
}

/// Where the live-assistant overlay belongs relative to orphan tool groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverlayPlacement {
    /// Append the overlay after every orphan tool group.
    Tail,
    /// Insert the overlay immediately before the orphan group belonging to the
    /// active, still-unmaterialized assistant turn.
    BeforeOrphan { message_sequence: Option<i64> },
}

/// The live-assistant overlay's ordering-relevant state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OverlayInput {
    /// True when a durable assistant turn from the same request already owns
    /// the overlay content — in which case it must NOT be re-emitted. This is
    /// deliberately independent of timeline position because P2P replication
    /// may expose newer messages alongside an older response snapshot.
    pub has_durable_owner: bool,
    pub placement: OverlayPlacement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingPlacement {
    Tail,
    BeforeMessage { message_sequence: i64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingInput {
    pub placement: PendingPlacement,
}

/// One ordered slot. A shell maps each to its rich, platform-specific item;
/// the *order and identity* of the slots is the shared contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimelineSlot {
    Message {
        key: String,
        sequence: Option<i64>,
        role: TimelineRole,
    },
    ToolGroup {
        message_sequence: Option<i64>,
    },
    Pending,
    Overlay,
}

/// The BTreeMap ordering the desktop bridge relies on: `None` first, then
/// ascending. Kept explicit so the ordering is a stated contract, not an
/// accident of a collection type.
fn sequence_lt(left: Option<i64>, right: Option<i64>) -> bool {
    match (left, right) {
        (None, None) => false,
        (None, Some(_)) => true,
        (Some(_), None) => false,
        (Some(l), Some(r)) => l < r,
    }
}

/// Build the canonical timeline slot order (#608).
///
/// `messages` are the already-content-filtered transcript messages in arbitrary
/// order (the shell decides which messages are worth showing — that is
/// presentation). `group_sequences` are the `message_sequence` keys that have
/// at least one tool call. `has_pending` / `overlay` gate the two tail slots.
///
/// Discipline, mirrored by `ClientShell.Timeline.buildOrder`:
/// 1. sort messages by `sequence` (None first),
/// 2. first-wins dedup by `key`, then by `dedup_token`,
/// 3. each surviving message emits its slot (if `emits_item`) immediately
///    followed by its tool group (if it owns one), marking that group attached,
/// 4. the pending turn immediately before the first durable message from the
///    same request, or at the body tail when no such message is visible,
/// 5. orphan tool groups — those attached to no surviving message — in sequence
///    order, inserting the overlay immediately before the specifically targeted
///    active group when it owns a still-running, unmaterialized tool turn,
/// 7. otherwise the overlay at the tail, iff present and not already owned by
///    a durable assistant turn from the same request.
pub fn build_timeline_order(
    messages: &[TimelineMessageInput],
    group_sequences: &[Option<i64>],
    pending: Option<PendingInput>,
    overlay: Option<OverlayInput>,
) -> Vec<TimelineSlot> {
    let mut ordered: Vec<&TimelineMessageInput> = messages.iter().collect();
    ordered.sort_by(|left, right| {
        if sequence_lt(left.sequence, right.sequence) {
            std::cmp::Ordering::Less
        } else if sequence_lt(right.sequence, left.sequence) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });

    let mut slots = Vec::new();
    let mut seen_keys = std::collections::BTreeSet::new();
    let mut seen_tokens = std::collections::BTreeSet::new();
    let mut attached = std::collections::BTreeSet::new();
    let pending_target = pending.and_then(|pending| match pending.placement {
        PendingPlacement::Tail => None,
        PendingPlacement::BeforeMessage { message_sequence } => Some(message_sequence),
    });
    let mut pending_placed = false;

    for message in ordered {
        if !seen_keys.insert(message.key.clone()) {
            continue;
        }
        if let Some(token) = &message.dedup_token {
            if !seen_tokens.insert(token.clone()) {
                continue;
            }
        }
        if pending.is_some()
            && !pending_placed
            && message.sequence == pending_target
            && (message.emits_item || group_sequences.contains(&message.sequence))
        {
            slots.push(TimelineSlot::Pending);
            pending_placed = true;
        }
        if message.emits_item {
            slots.push(TimelineSlot::Message {
                key: message.key.clone(),
                sequence: message.sequence,
                role: message.role,
            });
        }
        if group_sequences.contains(&message.sequence) && attached.insert(message.sequence) {
            slots.push(TimelineSlot::ToolGroup {
                message_sequence: message.sequence,
            });
        }
    }

    if pending.is_some() && !pending_placed {
        slots.push(TimelineSlot::Pending);
    }

    let mut orphans: Vec<Option<i64>> = group_sequences
        .iter()
        .copied()
        .filter(|sequence| !attached.contains(sequence))
        .collect();
    orphans.sort_by(|left, right| {
        if sequence_lt(*left, *right) {
            std::cmp::Ordering::Less
        } else if sequence_lt(*right, *left) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });
    orphans.dedup();
    let show_overlay = overlay.is_some_and(|overlay| !overlay.has_durable_owner);
    let target_orphan = overlay.and_then(|overlay| match overlay.placement {
        OverlayPlacement::Tail => None,
        OverlayPlacement::BeforeOrphan { message_sequence } => Some(message_sequence),
    });
    let mut overlay_placed = false;
    for sequence in orphans {
        if show_overlay && !overlay_placed && target_orphan == Some(sequence) {
            slots.push(TimelineSlot::Overlay);
            overlay_placed = true;
        }
        slots.push(TimelineSlot::ToolGroup {
            message_sequence: sequence,
        });
    }

    if show_overlay && !overlay_placed {
        slots.push(TimelineSlot::Overlay);
    }

    slots
}

#[cfg(test)]
mod tests {
    //! Conformance for the timeline ordering skeleton. Each test drives the real
    //! `build_timeline_order` through a witness and asserts a property the Lean
    //! model proves (`proofs/Proofs/ClientShell/Timeline.lean`). If the Rust
    //! ordering drifts from the fenced discipline, the matching test fails —
    //! which is what stops a second shell from silently diverging.

    use super::*;

    fn msg(key: &str, seq: i64, role: TimelineRole) -> TimelineMessageInput {
        TimelineMessageInput {
            key: key.to_string(),
            sequence: Some(seq),
            role,
            emits_item: true,
            dedup_token: None,
        }
    }

    fn count_group(slots: &[TimelineSlot], seq: Option<i64>) -> usize {
        slots
            .iter()
            .filter(|s| matches!(s, TimelineSlot::ToolGroup { message_sequence } if *message_sequence == seq))
            .count()
    }

    fn pending_tail() -> Option<PendingInput> {
        Some(PendingInput {
            placement: PendingPlacement::Tail,
        })
    }

    #[test]
    fn durable_user_owner_requires_the_exact_request_id() {
        let messages = [
            DurableUserOwnerInput {
                request_id: None,
                is_user: true,
                has_visible_content: true,
                runtime_control: false,
            },
            DurableUserOwnerInput {
                request_id: Some("unrelated"),
                is_user: true,
                has_visible_content: true,
                runtime_control: false,
            },
        ];

        assert!(!has_durable_user_owner(&messages, "request-under-test"));
    }

    /// Lean `group_attached_or_orphan` + `group_not_both`: every tool group is
    /// placed exactly once — attached to its owner or as an orphan, never both,
    /// never dropped.
    #[test]
    fn every_tool_group_is_placed_exactly_once() {
        let messages = vec![
            msg("a", 0, TimelineRole::User),
            msg("b", 2, TimelineRole::Assistant),
        ];
        // seq 0 is owned by message "a"; seq 5 is an orphan (no message owns it).
        let groups = vec![Some(0), Some(5)];
        let slots = build_timeline_order(&messages, &groups, None, None);

        assert_eq!(
            count_group(&slots, Some(0)),
            1,
            "attached group placed once"
        );
        assert_eq!(count_group(&slots, Some(5)), 1, "orphan group placed once");
        // The attached group immediately follows its owner message.
        let a_pos = slots
            .iter()
            .position(|s| matches!(s, TimelineSlot::Message { key, .. } if key == "a"))
            .unwrap();
        assert!(
            matches!(
                &slots[a_pos + 1],
                TimelineSlot::ToolGroup {
                    message_sequence: Some(0)
                }
            ),
            "attached group must immediately follow its owner: {slots:?}"
        );
    }

    /// Lean `overlay_shown_iff` + `overlay_is_last`: the overlay is emitted iff
    /// present and not a duplicate of the trailing assistant, and remains last
    /// when no running orphan tool requires early placement.
    #[test]
    fn overlay_is_shown_conditionally_and_last() {
        let messages = vec![msg("a", 0, TimelineRole::Assistant)];
        let groups = vec![Some(0)];

        let shown = build_timeline_order(
            &messages,
            &groups,
            pending_tail(),
            Some(OverlayInput {
                has_durable_owner: false,
                placement: OverlayPlacement::Tail,
            }),
        );
        assert_eq!(
            shown.last(),
            Some(&TimelineSlot::Overlay),
            "overlay must be last: {shown:?}"
        );

        let hidden = build_timeline_order(
            &messages,
            &groups,
            pending_tail(),
            Some(OverlayInput {
                has_durable_owner: true,
                placement: OverlayPlacement::Tail,
            }),
        );
        assert!(
            !hidden.contains(&TimelineSlot::Overlay),
            "a duplicate overlay must not be shown: {hidden:?}"
        );

        let absent = build_timeline_order(&messages, &groups, pending_tail(), None);
        assert!(!absent.contains(&TimelineSlot::Overlay));
    }

    #[test]
    fn overlay_precedes_orphan_groups_for_running_unmaterialized_tool_turn() {
        let messages = vec![msg("user", 0, TimelineRole::User)];
        let groups = vec![Some(1)];
        let slots = build_timeline_order(
            &messages,
            &groups,
            pending_tail(),
            Some(OverlayInput {
                has_durable_owner: false,
                placement: OverlayPlacement::BeforeOrphan {
                    message_sequence: Some(1),
                },
            }),
        );

        let overlay_pos = slots
            .iter()
            .position(|slot| matches!(slot, TimelineSlot::Overlay))
            .expect("overlay");
        let tool_pos = slots
            .iter()
            .position(|slot| {
                matches!(
                    slot,
                    TimelineSlot::ToolGroup {
                        message_sequence: Some(1)
                    }
                )
            })
            .expect("orphan tool group");
        assert!(
            overlay_pos < tool_pos,
            "live reasoning must precede its running orphan tool group: {slots:?}"
        );
    }

    #[test]
    fn overlay_preserves_earlier_terminal_orphans_before_active_target() {
        let messages = vec![msg("user", 0, TimelineRole::User)];
        let groups = vec![Some(1), Some(2)];
        let slots = build_timeline_order(
            &messages,
            &groups,
            pending_tail(),
            Some(OverlayInput {
                has_durable_owner: false,
                placement: OverlayPlacement::BeforeOrphan {
                    message_sequence: Some(2),
                },
            }),
        );

        assert_eq!(
            slots,
            vec![
                TimelineSlot::Message {
                    key: "user".to_string(),
                    sequence: Some(0),
                    role: TimelineRole::User,
                },
                TimelineSlot::Pending,
                TimelineSlot::ToolGroup {
                    message_sequence: Some(1),
                },
                TimelineSlot::Overlay,
                TimelineSlot::ToolGroup {
                    message_sequence: Some(2),
                },
            ],
            "historical orphan tools must stay before live reasoning"
        );
    }

    #[test]
    fn overlay_follows_terminal_orphan_groups() {
        let messages = vec![msg("user", 0, TimelineRole::User)];
        let groups = vec![Some(1)];
        let slots = build_timeline_order(
            &messages,
            &groups,
            pending_tail(),
            Some(OverlayInput {
                has_durable_owner: false,
                placement: OverlayPlacement::Tail,
            }),
        );

        assert_eq!(
            slots.last(),
            Some(&TimelineSlot::Overlay),
            "the next live assistant turn must follow terminal orphan tools: {slots:?}"
        );
    }

    /// Lean `pending_shown_iff`: the pending turn appears iff a turn is pending,
    /// and (structurally) after the body, before orphan groups.
    #[test]
    fn pending_turn_shown_iff_and_before_orphans() {
        let messages = vec![msg("a", 0, TimelineRole::User)];
        let groups = vec![Some(9)]; // orphan
        let slots = build_timeline_order(&messages, &groups, pending_tail(), None);

        let pending_pos = slots
            .iter()
            .position(|s| matches!(s, TimelineSlot::Pending));
        let orphan_pos = slots.iter().position(|s| {
            matches!(
                s,
                TimelineSlot::ToolGroup {
                    message_sequence: Some(9)
                }
            )
        });
        assert!(pending_pos.is_some(), "pending must be shown");
        assert!(
            pending_pos < orphan_pos,
            "pending must precede orphan groups: {slots:?}"
        );

        let no_pending = build_timeline_order(&messages, &groups, None, None);
        assert!(!no_pending
            .iter()
            .any(|s| matches!(s, TimelineSlot::Pending)));
    }

    #[test]
    fn pending_turn_precedes_later_message_from_same_partially_replicated_turn() {
        let messages = vec![
            msg("historical", 1, TimelineRole::Assistant),
            msg("continued", 3, TimelineRole::Assistant),
        ];
        let slots = build_timeline_order(
            &messages,
            &[],
            Some(PendingInput {
                placement: PendingPlacement::BeforeMessage {
                    message_sequence: 3,
                },
            }),
            None,
        );

        let pending_pos = slots
            .iter()
            .position(|slot| matches!(slot, TimelineSlot::Pending))
            .expect("pending slot");
        let continued_pos = slots
            .iter()
            .position(
                |slot| matches!(slot, TimelineSlot::Message { key, .. } if key == "continued"),
            )
            .expect("continued assistant message");
        assert_eq!(pending_pos + 1, continued_pos, "{slots:?}");
    }

    #[test]
    fn pending_turn_precedes_tool_group_from_non_emitting_same_request_row() {
        let messages = vec![TimelineMessageInput {
            key: "tool-first".to_string(),
            sequence: Some(3),
            role: TimelineRole::Assistant,
            emits_item: false,
            dedup_token: None,
        }];
        let slots = build_timeline_order(
            &messages,
            &[Some(3)],
            Some(PendingInput {
                placement: PendingPlacement::BeforeMessage {
                    message_sequence: 3,
                },
            }),
            None,
        );

        assert_eq!(
            slots,
            vec![
                TimelineSlot::Pending,
                TimelineSlot::ToolGroup {
                    message_sequence: Some(3),
                },
            ],
            "a tool-first partial replica must keep its request-owned prompt first"
        );
    }

    #[test]
    fn non_emitting_target_without_group_falls_back_to_tail() {
        let messages = vec![
            TimelineMessageInput {
                key: "invisible".to_string(),
                sequence: Some(2),
                role: TimelineRole::Assistant,
                emits_item: false,
                dedup_token: None,
            },
            msg("visible", 3, TimelineRole::Assistant),
        ];
        let slots = build_timeline_order(
            &messages,
            &[],
            Some(PendingInput {
                placement: PendingPlacement::BeforeMessage {
                    message_sequence: 2,
                },
            }),
            None,
        );

        assert_eq!(slots.last(), Some(&TimelineSlot::Pending));
    }

    /// Lean `kept_keys_nodup`: first-wins dedup by key — a repeated message key
    /// yields a single message slot.
    #[test]
    fn duplicate_message_keys_are_deduped_first_wins() {
        let messages = vec![
            msg("dup", 0, TimelineRole::Assistant),
            msg("dup", 1, TimelineRole::Assistant),
            msg("other", 2, TimelineRole::Assistant),
        ];
        let slots = build_timeline_order(&messages, &[], None, None);
        let dup_count = slots
            .iter()
            .filter(|s| matches!(s, TimelineSlot::Message { key, .. } if key == "dup"))
            .count();
        assert_eq!(
            dup_count, 1,
            "a repeated message key must render once: {slots:?}"
        );
    }

    /// Presentation-token dedup collapses re-presentations AND suppresses the
    /// second message's group attach (the desktop `continue`-before-attach
    /// nuance a naive shell would miss).
    #[test]
    fn presentation_token_dedup_also_drops_the_second_group_attach() {
        let mut first = msg("m1", 0, TimelineRole::Assistant);
        first.dedup_token = Some("same".to_string());
        let mut second = msg("m2", 1, TimelineRole::Assistant);
        second.dedup_token = Some("same".to_string());

        // Both message sequences own a group; the second message is dropped by
        // presentation dedup, so its group becomes an orphan (placed in the tail),
        // not attached.
        let slots = build_timeline_order(&[first, second], &[Some(0), Some(1)], None, None);

        let m2_shown = slots
            .iter()
            .any(|s| matches!(s, TimelineSlot::Message { key, .. } if key == "m2"));
        assert!(
            !m2_shown,
            "presentation-deduped message must not render: {slots:?}"
        );
        // Group 1 still appears exactly once — as an orphan.
        assert_eq!(count_group(&slots, Some(1)), 1);
    }

    /// `None` sequences sort first (matching the desktop `BTreeMap<Option<i64>>`).
    #[test]
    fn none_sequence_sorts_before_some() {
        let mut none_msg = msg("none", 0, TimelineRole::Assistant);
        none_msg.sequence = None;
        let some_msg = msg("some", 5, TimelineRole::Assistant);
        let slots = build_timeline_order(&[some_msg, none_msg], &[], None, None);
        let none_pos = slots
            .iter()
            .position(|s| matches!(s, TimelineSlot::Message { key, .. } if key == "none"));
        let some_pos = slots
            .iter()
            .position(|s| matches!(s, TimelineSlot::Message { key, .. } if key == "some"));
        assert!(
            none_pos < some_pos,
            "None sequence must sort first: {slots:?}"
        );
    }
}
