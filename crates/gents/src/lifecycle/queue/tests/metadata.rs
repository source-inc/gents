use super::*;

#[test]
fn parses_queue_hints_from_metadata_queue_field() {
    let metadata = r#"{
        "queue": {
            "source": "subagent_completion",
            "policy": "coalesce",
            "key": "session:sess-1",
            "queued_after_request_id": "req-1"
        }
    }"#;

    assert_eq!(
        parse_queue_hints(Some(metadata)),
        Some(hints(
            QueueSource::BackgroundCompletion,
            QueuePolicy::Coalesce
        ))
    );
}

#[test]
fn parses_all_supported_string_values() {
    let cases = [
        ("user", QueueSource::User),
        ("background_completion", QueueSource::BackgroundCompletion),
        ("subagent_completion", QueueSource::BackgroundCompletion),
        ("steering", QueueSource::Steering),
        ("goal", QueueSource::Goal),
    ];

    for (source, expected_source) in cases {
        let metadata = format!(
            r#"{{
                "queue": {{
                    "source": "{source}",
                    "policy": "append",
                    "key": null,
                    "queued_after_request_id": null
                }}
            }}"#
        );

        assert_eq!(
            parse_queue_hints(Some(&metadata)),
            Some(QueueHints {
                source: expected_source,
                policy: QueuePolicy::Append,
                key: None,
                queued_after_request_id: None,
                interrupted_request_id: None,
            })
        );
    }

    let metadata = r#"{
        "queue": {
            "source": "user",
            "policy": "coalesce",
            "key": null,
            "queued_after_request_id": null
        }
    }"#;

    assert_eq!(
        parse_queue_hints(Some(metadata)).map(|hints| hints.policy),
        Some(QueuePolicy::Coalesce)
    );
}

#[test]
fn returns_none_for_absent_blank_invalid_or_non_queue_metadata() {
    assert_eq!(parse_queue_hints(None), None);
    assert_eq!(parse_queue_hints(Some("   ")), None);
    assert_eq!(parse_queue_hints(Some("not json")), None);
    assert_eq!(parse_queue_hints(Some(r#"{"run_id":"abc"}"#)), None);
    assert_eq!(
        parse_queue_hints(Some(r#"{"queue":{"source":"timer","policy":"append"}}"#)),
        None
    );
}

#[test]
fn serializes_queue_metadata_json() {
    let json = queue_metadata_json(&QueueHints {
        source: QueueSource::Steering,
        policy: QueuePolicy::Coalesce,
        key: Some("agent:did:key:z123".to_string()),
        queued_after_request_id: None,
        interrupted_request_id: None,
    });

    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(
        value,
        serde_json::json!({
            "queue": {
                "source": "steering",
                "policy": "coalesce",
                "key": "agent:did:key:z123",
                "queued_after_request_id": null
            }
        })
    );
}

#[test]
fn automated_wakeup_is_true_only_for_keyed_subagent_completion_coalesce() {
    assert!(!is_automated_wakeup(None));
    assert!(!is_automated_wakeup(Some(&queue_metadata_json(
        &QueueHints {
            source: QueueSource::User,
            policy: QueuePolicy::Append,
            key: None,
            queued_after_request_id: None,
            interrupted_request_id: None,
        }
    ))));
    assert!(is_automated_wakeup(Some(&queue_metadata_json(
        &QueueHints {
            source: QueueSource::BackgroundCompletion,
            policy: QueuePolicy::Coalesce,
            key: Some("background_completion:session-1".to_string()),
            queued_after_request_id: None,
            interrupted_request_id: None,
        }
    ))));
    assert!(!is_automated_wakeup(Some(&queue_metadata_json(
        &QueueHints {
            source: QueueSource::BackgroundCompletion,
            policy: QueuePolicy::Append,
            key: Some("background_completion:session-1".to_string()),
            queued_after_request_id: None,
            interrupted_request_id: None,
        }
    ))));
    assert!(!is_automated_wakeup(Some(&queue_metadata_json(
        &QueueHints {
            source: QueueSource::BackgroundCompletion,
            policy: QueuePolicy::Coalesce,
            key: None,
            queued_after_request_id: None,
            interrupted_request_id: None,
        }
    ))));
    assert!(!is_automated_wakeup(Some(&queue_metadata_json(
        &QueueHints {
            source: QueueSource::Steering,
            policy: QueuePolicy::Coalesce,
            key: None,
            queued_after_request_id: None,
            interrupted_request_id: None,
        }
    ))));
}

#[test]
fn runtime_control_projection_keeps_only_the_steering_input_visible() {
    let metadata = |source| {
        queue_metadata_json(&QueueHints {
            source,
            policy: QueuePolicy::Append,
            key: None,
            queued_after_request_id: None,
            interrupted_request_id: None,
        })
    };
    let steering = metadata(QueueSource::Steering);
    let steering_input = steering_input_message_key("request-1");

    assert!(!crate::lifecycle::is_runtime_control_message(
        Some(&steering),
        &steering_input
    ));
    assert!(crate::lifecycle::is_runtime_control_message(
        Some(&steering),
        ""
    ));
    assert!(crate::lifecycle::is_runtime_control_message(
        Some(&metadata(QueueSource::Goal)),
        ""
    ));
    assert!(crate::lifecycle::is_runtime_control_message(
        None,
        "background-completion-notification:child-1:subagent"
    ));
    assert!(!crate::lifecycle::is_runtime_control_message(
        Some(&metadata(QueueSource::User)),
        ""
    ));
}
