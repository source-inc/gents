use crate::lean_vocab_test::lean_callback_cases;

fn later_than_validated(state: &str) -> bool {
    matches!(state, "executing" | "effectObserved" | "resultDocsWritten")
}

fn journal_prefix_ok(journal: &[String]) -> bool {
    journal.windows(2).all(|pair| {
        !later_than_validated(&pair[1]) || pair[0] == "resultDocsWritten"
    })
}

fn result_emitted_ok(state: &str, journal: &[String], result_emitted: bool) -> bool {
    !result_emitted
        || (state == "succeeded" && journal.iter().all(|entry| entry == "resultDocsWritten"))
}

fn denied_failed_no_execute(state: &str, journal: &[String]) -> bool {
    !(state == "denied" || state == "failed") || journal.is_empty()
}

fn invocation_legal(state: &str, journal: &[String], result_emitted: bool) -> bool {
    journal_prefix_ok(journal)
        && result_emitted_ok(state, journal, result_emitted)
        && denied_failed_no_execute(state, journal)
}

#[test]
fn generated_callback_cases_match_lean_predicate() {
    let cases = lean_callback_cases();
    assert!(
        !cases.is_empty(),
        "Lean callback_cases must include journal witnesses"
    );
    let names: Vec<&str> = cases.iter().map(|case| case.name.as_str()).collect();
    for required in [
        "happy_journal_prefix",
        "action_1_executing_while_0_not_result_docs_written_illegal",
        "result_emitted_while_running_illegal",
        "result_emitted_on_succeeded_with_complete_journal_legal",
        "denied_empty_journal_legal",
        "denied_executing_journal_illegal",
    ] {
        assert!(
            names.iter().any(|name| *name == required),
            "missing callback case {required}"
        );
    }
    for case in cases {
        let actual = invocation_legal(&case.state, &case.journal, case.result_emitted);
        assert_eq!(
            actual, case.legal,
            "callback case {} disagreed with the Lean predicate",
            case.name
        );
    }
}
