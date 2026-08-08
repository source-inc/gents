use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "witness", deny_unknown_fields)]
pub(crate) enum LeanR4cBackgroundWorkCase {
    #[serde(rename = "r4c.list_subagents.lineage_rejects")]
    ListSubagentsLineageRejects {
        caller_request_id: String,
        sibling_request_id: String,
        sibling_child_id: String,
        caller_sees_sibling_child: bool,
    },
    #[serde(rename = "r4c.read_subagent_transcript.cursor_advances")]
    ReadTranscriptCursorAdvances {
        child_session_id: String,
        first_since_sequence: usize,
        first_through_sequence: usize,
        first_next_sequence: usize,
        second_since_sequence: usize,
        second_through_sequence: usize,
        no_gap: bool,
        no_overlap: bool,
    },
    #[serde(rename = "r4c.read_subagent_transcript.hides_bridge_rows")]
    ReadTranscriptHidesBridgeRows {
        child_session_id: String,
        bridge_call_id: String,
        rendered_transcript: String,
    },
    #[serde(rename = "r4c.read_tool_output.dispatch_by_state")]
    ReadToolOutputDispatchesByState {
        tool_call_id: String,
        running_source: String,
        running_no_buffer_source: String,
        terminal_source: String,
        running_payload: String,
        running_no_buffer_payload: String,
        terminal_payload: String,
        running_next_offset: u64,
        running_total_bytes: u64,
        running_has_more: bool,
        terminal_total_bytes: u64,
    },
    #[serde(rename = "r4c.steer_subagent.append_preserves_lineage")]
    SteerAppendPreservesLineage {
        caller_request_id: String,
        child_session_id: String,
        queued_request_id: String,
        caused_by_parent_request_id: String,
        queue_source: String,
        queue_policy: String,
    },
    #[serde(rename = "r4c.steer_subagent.interrupt_composes")]
    SteerInterruptComposes {
        caller_request_id: String,
        child_session_id: String,
        interrupted_active_request_id: String,
        drained_wake_up_request_ids: Vec<String>,
        drained_wake_up_queue_key: String,
        queued_request_id: String,
        queue_interrupted_request_id: String,
    },
    #[serde(rename = "r4c.list_subagents.unmaterialized_child_visible")]
    UnmaterializedChildVisible {
        caller_request_id: String,
        bridge_tool_call_id: String,
        child_request_id: String,
        child_materialized: bool,
        bridge_lifecycle_state: String,
        listed_status: String,
        listed_under_all_filter: bool,
        listed_under_running_filter: bool,
        read_lifecycle_state: String,
        read_terminal: bool,
        wait_retryable: bool,
    },
}

impl LeanR4cBackgroundWorkCase {
    pub(crate) fn witness(&self) -> &'static str {
        match self {
            Self::ListSubagentsLineageRejects { .. } => "r4c.list_subagents.lineage_rejects",
            Self::ReadTranscriptCursorAdvances { .. } => {
                "r4c.read_subagent_transcript.cursor_advances"
            }
            Self::ReadTranscriptHidesBridgeRows { .. } => {
                "r4c.read_subagent_transcript.hides_bridge_rows"
            }
            Self::ReadToolOutputDispatchesByState { .. } => {
                "r4c.read_tool_output.dispatch_by_state"
            }
            Self::SteerAppendPreservesLineage { .. } => {
                "r4c.steer_subagent.append_preserves_lineage"
            }
            Self::SteerInterruptComposes { .. } => "r4c.steer_subagent.interrupt_composes",
            Self::UnmaterializedChildVisible { .. } => {
                "r4c.list_subagents.unmaterialized_child_visible"
            }
        }
    }
}

/// Executable bridge-step witness (#937): one concrete subagent-bridge
/// fixture, one bridge event, and the outcome computed by running the Lean
/// `Subagent.BridgedState.step` on it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanBridgeStepCase {
    pub(crate) name: String,
    pub(crate) event: String,
    pub(crate) child_state: String,
    pub(crate) parent_state: String,
    pub(crate) cancel_policy: String,
    pub(crate) bridge_committed: bool,
    pub(crate) legal: bool,
    pub(crate) post_tool_state: Option<String>,
    pub(crate) post_child_interrupt_set: bool,
    #[allow(dead_code)]
    pub(crate) theorem: String,
}

/// Paging witness over the retained output window (#937): inputs plus the
/// slice outputs computed from the Lean `Subagent.ToolOutput.readSlice`
/// model, consumed against `read_retained_output_slice`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanToolOutputPagingCase {
    pub(crate) name: String,
    pub(crate) first_offset: u64,
    pub(crate) retained_len: u64,
    pub(crate) total_bytes: u64,
    pub(crate) offset: u64,
    pub(crate) max_bytes: u64,
    pub(crate) start: u64,
    pub(crate) slice_len: u64,
    pub(crate) next_offset: u64,
    pub(crate) first_available_offset: u64,
    pub(crate) total_bytes_out: u64,
    pub(crate) has_more: bool,
    #[allow(dead_code)]
    pub(crate) theorem: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanR6BackgroundingCase {
    pub(crate) name: String,
    pub(crate) group: String,
    pub(crate) action: String,
    pub(crate) legal: bool,
    pub(crate) pre_live_count: usize,
    pub(crate) max_backgrounded: usize,
    pub(crate) await_mode: String,
    pub(crate) cancel_policy: String,
    pub(crate) child_request_id: Option<String>,
    pub(crate) terminal_state: String,
    pub(crate) result: Option<String>,
    pub(crate) reason: Option<String>,
    pub(crate) error_code: Option<String>,
    pub(crate) queue_source: Option<String>,
    pub(crate) queue_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanR5CrossDeploymentCase {
    pub(crate) name: String,
    pub(crate) route: String,
    pub(crate) action: String,
    pub(crate) parent_deployment: String,
    pub(crate) child_deployment: String,
    pub(crate) parent_request_id: String,
    pub(crate) parent_tool_call_id: String,
    pub(crate) child_request_id: String,
    pub(crate) target_behavior_id: String,
    pub(crate) await_mode: String,
    pub(crate) cancel_policy: String,
    pub(crate) parent_trigger_persisted: bool,
    pub(crate) child_materialized: bool,
    pub(crate) child_owned_by_target_deployment: bool,
    pub(crate) caused_by_parent_request_id_matches: bool,
    pub(crate) caused_by_parent_tool_call_id_matches: bool,
    pub(crate) caused_by_trigger_kind: String,
    pub(crate) cross_deployment_routing_fired: bool,
    pub(crate) single_deployment_fallback: bool,
    pub(crate) unclaimed_deadline_set: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanCancelPropagationCase {
    pub(crate) name: String,
    pub(crate) route: String,
    pub(crate) action: String,
    pub(crate) parent_deployment: String,
    pub(crate) child_deployment: String,
    pub(crate) parent_request_id: String,
    pub(crate) parent_tool_call_id: String,
    pub(crate) child_request_id: String,
    pub(crate) bridge_collection: String,
    pub(crate) child_request_collection: String,
    pub(crate) cancel_intent_written_on_bridge: bool,
    pub(crate) bridge_cancel_replicates_to_host: bool,
    pub(crate) host_interrupts_child: bool,
    pub(crate) child_terminal_replicates_to_coordinator: bool,
    pub(crate) cancel_ack_returns_to_coordinator: bool,
    pub(crate) no_third_party_rows: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanBackgroundTheoremWitness {
    pub(crate) theorem_name: String,
    pub(crate) witness_kind: String,
    pub(crate) scenario: String,
    pub(crate) numeric_bound: usize,
    pub(crate) kind_fields: Vec<LeanBackgroundTheoremKindField>,
}

impl LeanBackgroundTheoremWitness {
    pub(crate) fn kind_field(&self, key: &str) -> &str {
        self.kind_fields
            .iter()
            .find(|field| field.key == key)
            .map(|field| field.value.as_str())
            .unwrap_or_else(|| {
                panic!(
                    "Lean Background theorem witness {:?} omitted kind field {:?}",
                    self.theorem_name, key
                )
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanBackgroundTheoremKindField {
    pub(crate) key: String,
    pub(crate) value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanSubagentDelegationGraphCase {
    pub(crate) name: String,
    pub(crate) theorem_name: String,
    pub(crate) property: String,
    pub(crate) witness_kind: String,
    pub(crate) max_depth: usize,
    pub(crate) path_length: usize,
    pub(crate) parent_depth: usize,
    pub(crate) terminal_depth: usize,
    pub(crate) cascade_path: bool,
    pub(crate) acyclic: bool,
    pub(crate) bounded: bool,
    pub(crate) cascade_covered: bool,
    pub(crate) edge_theorem: String,
    pub(crate) cascade_edge_theorem: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanTranscriptCase {
    pub(crate) name: String,
    pub(crate) group: String,
    pub(crate) action: String,
    pub(crate) legal: bool,
    pub(crate) pre_message_count: usize,
    pub(crate) post_message_count: usize,
    pub(crate) pre_tool_call_count: usize,
    pub(crate) post_tool_call_count: usize,
    pub(crate) pre_in_flight_count: usize,
    pub(crate) post_in_flight_count: usize,
    pub(crate) assistant_sequence: usize,
    pub(crate) result_sequence: usize,
    pub(crate) logical_result_id: usize,
    pub(crate) payload_hash: usize,
    pub(crate) expected_pair_closed: bool,
    pub(crate) expected_ordered: bool,
    pub(crate) expected_duplicate_reused_sequence: bool,
    pub(crate) expected_strong_drain: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanTranscriptFinalizationCase {
    pub(crate) name: String,
    pub(crate) action: String,
    pub(crate) visible_logical_fact_count: usize,
    pub(crate) checkpoint_present: bool,
    pub(crate) fact_present_before: bool,
    pub(crate) fact_present_after: bool,
    pub(crate) fact_commit_cid: Option<usize>,
    pub(crate) checkpoint_payload_hash: Option<usize>,
    pub(crate) write_payload_hash: usize,
    pub(crate) fact_payload_hash: Option<usize>,
    pub(crate) write_signer_did: Option<usize>,
    pub(crate) write_signature_valid: Option<bool>,
    pub(crate) write_policy_authorized: Option<bool>,
    pub(crate) fact_signer_did: Option<usize>,
    pub(crate) disposition: String,
    pub(crate) checkpoint_preserved: bool,
    pub(crate) sibling_isolated: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanTranscriptProviderHistoryCase {
    pub(crate) name: String,
    pub(crate) reference_count: usize,
    pub(crate) visible_conflict_count: usize,
    pub(crate) accepted: bool,
    pub(crate) output_count: usize,
    pub(crate) output_payload_hashes: Vec<usize>,
    pub(crate) exact_finalized_domain_only: bool,
    pub(crate) strictly_increasing: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanResponseTransitionCase {
    pub(crate) name: String,
    pub(crate) group: String,
    pub(crate) action: String,
    pub(crate) legal: bool,
    pub(crate) pre_status: String,
    pub(crate) post_status: String,
    pub(crate) pre_live_tail: String,
    pub(crate) post_live_tail: String,
    /// #492 reasoning-presence in the live tail before/after the step.
    #[serde(default)]
    pub(crate) pre_tail_reasoning: String,
    #[serde(default)]
    pub(crate) post_tail_reasoning: String,
    /// #492 durable reasoning-presence persisted into the materialized
    /// `AgentMessage.reasoning` field before/after the step.
    #[serde(default)]
    pub(crate) pre_durable_reasoning: String,
    #[serde(default)]
    pub(crate) post_durable_reasoning: String,
    pub(crate) pre_token_count: usize,
    pub(crate) post_token_count: usize,
    pub(crate) error_reason: Option<String>,
    pub(crate) pre_materialized_seq: Option<usize>,
    pub(crate) post_materialized_seq: Option<usize>,
    pub(crate) expected_request_state: Option<String>,
    pub(crate) expected_request_persistence: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanResponseInterruptFlowCase {
    pub(crate) name: String,
    pub(crate) group: String,
    pub(crate) action: String,
    pub(crate) pre_request_state: String,
    pub(crate) post_request_state: String,
    pub(crate) pre_response_status: String,
    pub(crate) post_response_status: String,
    pub(crate) pre_inference_call_state: String,
    pub(crate) post_inference_call_state: String,
    pub(crate) response_error_reason: String,
    pub(crate) interrupted_at_required: bool,
    pub(crate) completed_at_required: bool,
    pub(crate) live_tail_cleared: bool,
    pub(crate) partial_turn_materialized: bool,
    pub(crate) request_terminal: bool,
    pub(crate) response_terminal: bool,
    pub(crate) inference_call_terminal: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanCompactionReducerCase {
    pub(crate) name: String,
    pub(crate) group: String,
    pub(crate) reducer: String,
    pub(crate) legal: bool,
    pub(crate) pre_message_count: usize,
    pub(crate) post_message_count: usize,
    pub(crate) preserves_pairs: bool,
    pub(crate) preserves_order: bool,
    pub(crate) gate_open: bool,
    pub(crate) safe_to_reduce: bool,
    pub(crate) reducer_is_identity: bool,
    pub(crate) split_index: usize,
    pub(crate) safe_boundary: usize,
    pub(crate) retained_count: usize,
}
