use std::collections::BTreeMap;

use crate::lean_vocab_test::lean_compaction_source_manifest_cases;

pub(crate) fn generated_cases_pin_exact_immutable_compaction_sources() {
    let cases = lean_compaction_source_manifest_cases()
        .iter()
        .map(|case| (case.name.as_str(), case))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(cases.len(), 8);

    let fresh = cases["fresh_exact_manifest_applied"];
    assert_eq!(fresh.disposition, "applied");
    assert!(fresh.manifest_valid && fresh.sources_current);
    assert_eq!(fresh.durable_rows, 1);

    let replay = cases["identical_replay_is_idempotent"];
    assert_eq!(replay.disposition, "idempotent");
    assert_eq!(replay.visible_logical_twins, 1);
    assert_eq!(replay.durable_rows, 1);

    for name in [
        "malformed_empty_transcript_rejected",
        "unsigned_source_rejected",
        "conflicting_final_fact_rejected",
        "logical_twins_rejected",
        "mutated_transcript_rejected",
        "mutated_config_rejected",
    ] {
        assert_eq!(cases[name].disposition, "rejected", "case {name}");
    }
    assert!(!cases["malformed_empty_transcript_rejected"].manifest_valid);
    assert!(!cases["unsigned_source_rejected"].manifest_valid);
    assert_eq!(cases["logical_twins_rejected"].visible_logical_twins, 2);
    assert!(!cases["mutated_transcript_rejected"].sources_current);
    assert!(!cases["mutated_config_rejected"].sources_current);
}
