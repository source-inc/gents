//! Adapt runtime snapshots to the stock pager's child-aware append channel.
//! Receipts contain offsets and hashes only, never another output buffer.
use super::{payload_fingerprint, CursorAdvance, NovelProjectionEvent, RequestCursor};
use serde_json::json;

#[derive(Clone, Debug)]
pub(crate) struct OutputReceipt {
    start: Option<u64>,
    end: u64,
    hash: u64,
}

fn hash(text: &str) -> u64 {
    payload_fingerprint(&json!(text))
}

fn append_projection(
    previous: Option<&OutputReceipt>,
    start: Option<u64>,
    text: &str,
    truncated: bool,
) -> (String, OutputReceipt) {
    let origin = start.unwrap_or(0);
    let end = origin.saturating_add(text.len() as u64);
    let receipt = OutputReceipt {
        start,
        end,
        hash: hash(text),
    };
    if text.is_empty() {
        return (String::new(), previous.cloned().unwrap_or(receipt));
    }
    let Some(previous) = previous else {
        let notice = if origin > 0 || truncated {
            "[Earlier output was truncated]\n"
        } else {
            ""
        };
        return (format!("{notice}{text}"), receipt);
    };
    // An unchanged unknown-offset tail is not a new append. Do not infer
    // overlap from repeated text when the source has no byte identity.
    if start == previous.start && end == previous.end && receipt.hash == previous.hash {
        return (String::new(), receipt);
    }
    if let (Some(start), Some(old_start)) = (start, previous.start) {
        if start > previous.end {
            return (
                format!(
                    "\n[{} output bytes unavailable]\n{text}",
                    start - previous.end
                ),
                receipt,
            );
        }
        if end >= previous.end {
            // Live ring windows move forward. Final captures start at zero
            // and may reorder stdout/stderr: verify the old window before
            // treating that different representation as a continuation.
            let same_stream = start > old_start
                || text
                    .get(
                        (old_start.saturating_sub(start)) as usize..(previous.end - start) as usize,
                    )
                    .is_some_and(|old| hash(old) == previous.hash);
            if same_stream {
                let mut offset = (previous.end - start) as usize;
                while offset < text.len() && !text.is_char_boundary(offset) {
                    offset += 1;
                }
                return (text[offset..].to_owned(), receipt);
            }
        }
    }
    // A persisted capture is not necessarily the live interleaved stream.
    // Preserve it honestly as a labelled snapshot, never guessed deltas.
    (
        format!("\n[Captured output snapshot; stream continuity unavailable]\n{text}"),
        receipt,
    )
}

impl RequestCursor {
    pub(crate) fn child_output_event(
        &self,
        mut event: NovelProjectionEvent,
    ) -> NovelProjectionEvent {
        let CursorAdvance::BackgroundOutput {
            key, output_start, ..
        } = &event.advance
        else {
            return event;
        };
        let raw = &event.payload["rawOutput"];
        let text = raw["output_for_prompt"].as_str().unwrap_or("");
        let (delta, receipt) = append_projection(
            self.child_outputs.get(key),
            *output_start,
            text,
            raw["truncated"].as_bool().unwrap_or(false),
        );
        let key = key.clone();
        event.method = "x.ai/monitor_event";
        event.payload = json!({"sessionUpdate":"monitor_event", "task_id":key,
            "description":"", "event_text":delta});
        event.advance = CursorAdvance::Many(vec![
            event.advance,
            CursorAdvance::ChildOutput { key, receipt },
        ]);
        event
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn growing_and_rolling_windows_append_only_unseen_bytes() {
        let (first, a) = append_projection(None, Some(0), "abcabc", false);
        assert_eq!(first, "abcabc");
        let (next, b) = append_projection(Some(&a), Some(0), "abcabcabc", false);
        assert_eq!(next, "abc");
        let (roll, c) = append_projection(Some(&b), Some(6), "abcabc", true);
        assert_eq!(roll, "abc");
        assert_eq!(append_projection(Some(&c), Some(6), "abcabc", true).0, "");
    }

    #[test]
    fn gaps_and_unrelated_captures_are_labelled_not_guessed() {
        let (_, a) = append_projection(None, Some(0), "abc", false);
        assert_eq!(
            append_projection(Some(&a), Some(8), "xyz", true).0,
            "\n[5 output bytes unavailable]\nxyz"
        );
        let (_, b) = append_projection(None, Some(3), "def", true);
        assert_eq!(
            append_projection(Some(&b), Some(0), "abcdefghi", false).0,
            "ghi"
        );
        assert!(
            append_projection(Some(&b), Some(0), "stderr\nstdout", false)
                .0
                .contains("stream continuity unavailable")
        );
        assert!(append_projection(None, Some(9), "tail", true)
            .0
            .contains("truncated"));
    }

    #[test]
    fn empty_cancellation_output_keeps_receipt_and_unicode_is_not_split() {
        let (_, a) = append_projection(None, Some(0), "é", false);
        assert_eq!(append_projection(Some(&a), Some(0), "é🙂", false).0, "🙂");
        let (empty, b) = append_projection(Some(&a), Some(0), "", false);
        assert!(empty.is_empty());
        assert_eq!(b.end, a.end);
    }

    #[test]
    fn receipt_is_committed_only_after_send_and_uses_native_child_channel() {
        let mut cursor = RequestCursor::new();
        let make = || NovelProjectionEvent {
            method: "session/update",
            timing: None,
            payload: json!({"rawOutput":{"output_for_prompt":"hello"}}),
            advance: CursorAdvance::BackgroundOutput {
                key: "task".into(),
                fingerprint: 7,
                output_start: Some(0),
            },
        };
        let first = cursor.child_output_event(make());
        assert_eq!(first.method, "x.ai/monitor_event");
        assert_eq!(first.payload["event_text"], "hello");
        assert_eq!(cursor.child_output_event(make()).payload, first.payload);
        cursor.record(first.advance);
        assert_eq!(cursor.child_output_event(make()).payload["event_text"], "");
    }
}
