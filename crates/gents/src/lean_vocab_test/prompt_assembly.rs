use serde::Deserialize;

/// One item inside a message, as emitted by `PromptAssembly.Content.Item`.
/// `value` is the text/reasoning index for `text`/`other`, and the tool-call id
/// for `call`.
#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub(crate) struct LeanPromptAssemblyItem {
    pub(crate) item: String,
    pub(crate) value: u64,
}

/// One provider-bound row: the abstract transcript row plus its content.
#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub(crate) struct LeanPromptAssemblyRow {
    pub(crate) role: String,
    pub(crate) kind: String,
    pub(crate) call_ids: Vec<u64>,
    pub(crate) content: Vec<LeanPromptAssemblyItem>,
}

/// `sanitize` applied to a suffix of the input, for split-stability.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LeanPromptAssemblySplit {
    pub(crate) index: usize,
    pub(crate) expected: Vec<LeanPromptAssemblyRow>,
}

/// A sanitize witness. `expected`, `expected_twice`, and every `splits` entry
/// are computed by running the Lean model, never written by hand.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LeanPromptAssemblySanitizeCase {
    pub(crate) name: String,
    pub(crate) input: Vec<LeanPromptAssemblyRow>,
    pub(crate) expected: Vec<LeanPromptAssemblyRow>,
    pub(crate) expected_twice: Vec<LeanPromptAssemblyRow>,
    pub(crate) splits: Vec<LeanPromptAssemblySplit>,
}

/// The assembled layer order from `PromptAssembly.Template.assembleWithContext`.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LeanPromptAssemblyLayerCase {
    pub(crate) name: String,
    pub(crate) skill_count: usize,
    pub(crate) summary_count: usize,
    pub(crate) conversation_len: usize,
    pub(crate) slots: Vec<String>,
}

/// A tool-argument repair witness from `PromptAssembly.ToolArgs.repairArgs`.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LeanPromptAssemblyRepairCase {
    pub(crate) name: String,
    pub(crate) input: String,
    pub(crate) expected: String,
    pub(crate) expected_twice: String,
    pub(crate) payload_only: bool,
}

/// A provider-input budget witness computed by `PromptAssembly.Budget`.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LeanPromptAssemblyBudgetCase {
    pub(crate) name: String,
    pub(crate) context_window: usize,
    pub(crate) max_output_tokens: usize,
    pub(crate) threshold_basis_points: usize,
    pub(crate) configured_threshold_budget: usize,
    pub(crate) prompt_tokens: usize,
    pub(crate) request_tokens: usize,
    pub(crate) effective_input_budget: usize,
    pub(crate) effective_output_tokens: usize,
    pub(crate) should_compact: bool,
    pub(crate) provider_safe: bool,
}

/// A multi-turn provider-input budget trace computed by
/// `PromptAssembly.Budget`.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LeanPromptAssemblyTurnBudgetCase {
    pub(crate) name: String,
    pub(crate) context_window: usize,
    pub(crate) max_output_tokens: usize,
    pub(crate) threshold_basis_points: usize,
    pub(crate) configured_threshold_budget: usize,
    pub(crate) effective_input_budget: usize,
    pub(crate) turn_input_tokens: Vec<usize>,
    pub(crate) turn_output_tokens: Vec<usize>,
    pub(crate) turn_should_compact: Vec<bool>,
}

/// A request-wide token-ledger witness computed by
/// `PromptAssembly.AggregateBudget`.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LeanAggregateTokenBudgetCase {
    pub(crate) name: String,
    pub(crate) limit: u64,
    pub(crate) used: u64,
    pub(crate) input_tokens: u64,
    pub(crate) configured_max_output_tokens: u64,
    pub(crate) reported_input_tokens: u64,
    pub(crate) reported_output_tokens: u64,
    pub(crate) reported_total_tokens: u64,
    pub(crate) usage_present: bool,
    pub(crate) terminal_valid: bool,
    pub(crate) effective_output_tokens: u64,
    pub(crate) can_dispatch: bool,
    pub(crate) charged_tokens: u64,
    pub(crate) charge_result: String,
    pub(crate) next_used: Option<u64>,
    pub(crate) post_charge_action: String,
}
