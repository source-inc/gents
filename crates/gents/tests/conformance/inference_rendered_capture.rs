//! Pure-Rust consumer for the generated inference-call/render composition
//! contract. Production integration belongs to later implementation slices;
//! this fence makes every generated provenance edge explicit in Rust now.

use crate::lean_vocab_test::{
    lean_inference_rendered_capture_cases, LeanInferenceRenderedCaptureCase,
};

#[derive(Clone, Copy)]
enum Path {
    QueueOnly,
    Sent,
    CaptureFailed,
    RecoveredBeforeSend,
    NetworkFailed,
}

#[derive(Clone, Copy)]
enum Capture {
    NotAttempted,
    Fresh,
    Idempotent,
    Rejected,
}

/// Compact independent mirror of the composed transition projection. The ids
/// are the concrete exact-version witnesses emitted by Lean: call V1 is CID
/// 31, render R is CID 41, pre-send V2 is CID 32, and terminal V3 is CID 33.
fn mirror(name: &str, path: Path, capture: Capture) -> LeanInferenceRenderedCaptureCase {
    let captured = matches!(
        path,
        Path::Sent | Path::RecoveredBeforeSend | Path::NetworkFailed
    );
    let (initial_stage, initial_call_state) = match path {
        Path::QueueOnly => ("queue_only", "queued"),
        _ => ("running", "running"),
    };
    let (final_stage, final_call_state, current_call_cid) = match path {
        Path::QueueOnly => ("queue_only", "queued", 30),
        Path::Sent => ("sent", "running", 32),
        Path::CaptureFailed => ("capture_failed", "running", 31),
        Path::RecoveredBeforeSend => ("recovered_before_send", "failed", 33),
        Path::NetworkFailed => ("network_failed", "failed", 33),
    };
    let capture_outcome = match capture {
        Capture::NotAttempted => "not_attempted",
        Capture::Fresh => "fresh",
        Capture::Idempotent => "idempotent",
        Capture::Rejected => "rejected",
    };
    let http_requests_observed = match path {
        Path::Sent | Path::NetworkFailed => 1,
        _ => 0,
    };
    let terminal_failed = matches!(path, Path::RecoveredBeforeSend | Path::NetworkFailed);

    LeanInferenceRenderedCaptureCase {
        name: name.to_owned(),
        initial_stage: initial_stage.to_owned(),
        final_stage: final_stage.to_owned(),
        initial_call_state: initial_call_state.to_owned(),
        final_call_state: final_call_state.to_owned(),
        capture_outcome: capture_outcome.to_owned(),
        running_call_doc_id: captured.then_some(300),
        running_call_cid: captured.then_some(31),
        render_doc_id: captured.then_some(400),
        render_cid: captured.then_some(41),
        current_call_cid,
        render_durable: captured,
        render_pins_running: captured,
        call_pins_render: captured,
        http_requests_observed,
        terminal_failed,
        second_send_permitted: false,
    }
}

pub(super) fn generated_cases_pin_exact_version_composition() {
    let expected = [
        mirror(
            "queue_only_has_no_render",
            Path::QueueOnly,
            Capture::NotAttempted,
        ),
        mirror(
            "fresh_capture_exact_v1_r_v2_then_send",
            Path::Sent,
            Capture::Fresh,
        ),
        mirror(
            "idempotent_capture_exact_v1_r_v2_then_send",
            Path::Sent,
            Capture::Idempotent,
        ),
        mirror(
            "conflicting_capture_blocks_send",
            Path::CaptureFailed,
            Capture::Rejected,
        ),
        mirror(
            "crash_after_v2_before_send_recovers_failed_unsent",
            Path::RecoveredBeforeSend,
            Capture::Fresh,
        ),
        mirror(
            "network_failure_after_send_preserves_render_and_fails_call",
            Path::NetworkFailed,
            Capture::Fresh,
        ),
        mirror(
            "one_shot_still_requires_explicit_call_chain",
            Path::Sent,
            Capture::Fresh,
        ),
    ];

    assert_eq!(
        lean_inference_rendered_capture_cases(),
        expected,
        "Rust's exact-version composition mirror drifted from the generated Lean witnesses"
    );

    assert!(
        expected.iter().all(|case| !case.second_send_permitted),
        "one capture arm must never authorize a second unexplained send"
    );
}
