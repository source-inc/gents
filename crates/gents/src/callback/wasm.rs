//! WASM ActionPlan planner. No WASI. Resource limits deny execution.

use std::collections::BTreeSet;
use std::sync::OnceLock;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use wasmtime::{Config, Engine, Linker, Module, Store, StoreLimits, StoreLimitsBuilder};

use crate::workspace::{
    action_plan_canonical_json, canonical_json_string, parse_action_plan_json, ActionPlan,
    ACTION_PLAN_ABI,
};

use super::documents::CallbackModuleDoc;

const WASM_PAGE_BYTES: u64 = 65536;
const CALLBACK_MODULE_ID_DOMAIN: &[u8] = b"gents.callback.module.v1";

/// Host ceilings independent of CallbackModule fields. Over-ceiling → Denied.
pub const MAX_FUEL: u64 = 100_000_000;
pub const MAX_MEMORY_PAGES: u32 = 256;
pub const MAX_INPUT_BYTES: usize = 1_048_576;
pub const MAX_OUTPUT_BYTES: usize = 1_048_576;
pub const MAX_WASM_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_TABLE_ELEMENTS: usize = 1024;

pub struct CallbackModuleLimits {
    pub fuel_limit: u64,
    pub memory_pages: u32,
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
}

struct PlannerHost {
    limits: StoreLimits,
}

/// Content-addressed id over decoded bytes + canonical JSON args + ABI.
/// Hashes the bytes themselves, never a host path.
pub fn compute_module_id(wasm: &[u8], args: &Value, abi_version: i64) -> Result<String, String> {
    let args_json = canonical_json_string(args)?;
    let mut hasher = Sha256::new();
    hasher.update(CALLBACK_MODULE_ID_DOMAIN);
    hasher.update(&(wasm.len() as u64).to_be_bytes());
    hasher.update(wasm);
    hasher.update(&(args_json.len() as u64).to_be_bytes());
    hasher.update(args_json.as_bytes());
    hasher.update(&(abi_version as u64).to_be_bytes());
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

pub fn decode_wasm_bytes(base64_text: &str) -> Result<Vec<u8>, String> {
    let trimmed = base64_text.trim();
    if trimmed.is_empty() {
        return Err("CallbackModule.wasm_bytes is empty".into());
    }
    STANDARD
        .decode(trimmed)
        .map_err(|error| format!("CallbackModule.wasm_bytes is not standard base64: {error}"))
}

pub fn parse_canonical_args(raw: Option<&str>) -> Result<Value, String> {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(json!({}));
    };
    let value: Value = serde_json::from_str(raw)
        .map_err(|error| format!("CallbackModule.canonical_args is not JSON: {error}"))?;
    let _ = canonical_json_string(&value)?;
    Ok(value)
}

pub fn limits_from_module(module: &CallbackModuleDoc) -> Result<CallbackModuleLimits, String> {
    Ok(CallbackModuleLimits {
        fuel_limit: bounded_u64(module.fuel_limit, "fuel_limit", MAX_FUEL)?,
        memory_pages: bounded_u32(module.memory_pages, "memory_pages", MAX_MEMORY_PAGES)?,
        max_input_bytes: bounded_usize(module.max_input_bytes, "max_input_bytes", MAX_INPUT_BYTES)?,
        max_output_bytes: bounded_usize(
            module.max_output_bytes,
            "max_output_bytes",
            MAX_OUTPUT_BYTES,
        )?,
    })
}

/// Install/apply predicate for CallbackModule (fail closed).
///
/// v1 is an **installer allowlist**, not a cryptographic signature: `signer_did`
/// must be an enabled `AgentPrincipal` DID. `provenance` is a required non-empty
/// operator note (pack name, review URL, etc.), not a signature over bytes.
/// `CallbackBinding.principal_did` is the workspace writer and is not required
/// to equal `signer_did`. Call this before first use. Recovery that already
/// has a stored ActionPlan must not re-run it.
pub fn validate_callback_module(
    module: &CallbackModuleDoc,
    trusted_signers: &BTreeSet<String>,
) -> Result<(), String> {
    if module.module_id.trim().is_empty() {
        return Err("CallbackModule.module_id is missing".into());
    }
    let signer = module
        .signer_did
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "CallbackModule signer_did is missing".to_string())?;
    let provenance = module
        .provenance
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "CallbackModule provenance is missing".to_string())?;
    if trusted_signers.is_empty() {
        return Err("CallbackModule signer policy is missing: no trusted principals".into());
    }
    if !trusted_signers.contains(signer) {
        return Err(format!(
            "CallbackModule signer_did `{signer}` is not a trusted installer (allowlist, not a signature)"
        ));
    }
    tracing::debug!(
        signer_did = signer,
        provenance,
        module_id = %module.module_id,
        "CallbackModule installer allowlist accepted"
    );
    let abi = module.abi_version.unwrap_or(0);
    if abi != i64::from(ACTION_PLAN_ABI) {
        return Err(format!(
            "CallbackModule abi_version {abi} is unsupported (expected {})",
            ACTION_PLAN_ABI
        ));
    }
    let wasm = decode_wasm_bytes(module.wasm_bytes.as_deref().unwrap_or(""))?;
    require_wasm_binary(&wasm)?;
    let args = parse_canonical_args(module.canonical_args.as_deref())?;
    let expected = compute_module_id(&wasm, &args, abi)?;
    if expected != module.module_id.trim() {
        return Err(format!(
            "CallbackModule.module_id does not match content-addressed id (expected {expected})"
        ));
    }
    let _ = limits_from_module(module)?;
    Ok(())
}

pub fn plan_from_wasm_module(
    module: &CallbackModuleDoc,
    source: &Value,
    capabilities: &BTreeSet<String>,
) -> Result<ActionPlan, String> {
    if module.enabled == Some(false) {
        return Err("CallbackModule is disabled".into());
    }
    let wasm = decode_wasm_bytes(module.wasm_bytes.as_deref().unwrap_or(""))?;
    let args = parse_canonical_args(module.canonical_args.as_deref())?;
    let limits = limits_from_module(module)?;
    let capabilities: Vec<String> = capabilities.iter().cloned().collect();
    let input = json!({
        "args": args,
        "capabilities": capabilities,
        "source": source,
    });
    let input_json = canonical_json_string(&input)?;
    let output = invoke_wasm_planner(&wasm, &limits, input_json.as_bytes())?;
    let raw = std::str::from_utf8(&output)
        .map_err(|error| format!("WASM planner output is not UTF-8: {error}"))?;
    let plan = parse_action_plan_json(raw)?;
    let _ = action_plan_canonical_json(&plan)?;
    Ok(plan)
}

/// Instantiate with an empty linker (no WASI). Limits exceeded → Denied.
pub fn invoke_wasm_planner(
    wasm: &[u8],
    limits: &CallbackModuleLimits,
    input: &[u8],
) -> Result<Vec<u8>, String> {
    require_wasm_binary(wasm)?;
    if input.len() > limits.max_input_bytes {
        return Err(format!(
            "WASM planner input {} bytes exceeds max_input_bytes {}",
            input.len(),
            limits.max_input_bytes
        ));
    }
    if limits.fuel_limit > MAX_FUEL
        || limits.memory_pages > MAX_MEMORY_PAGES
        || limits.max_input_bytes > MAX_INPUT_BYTES
        || limits.max_output_bytes > MAX_OUTPUT_BYTES
    {
        return Err("WASM planner limits exceed host ceilings".into());
    }
    let engine = planner_engine()?;
    let module =
        Module::new(engine, wasm).map_err(|error| format!("WASM module rejected: {error}"))?;
    if let Some(import) = module.imports().next() {
        return Err(format!(
            "WASM planner may not import host functions (no WASI); found {}::{}",
            import.module(),
            import.name()
        ));
    }

    let memory_bytes = (u64::from(limits.memory_pages))
        .saturating_mul(WASM_PAGE_BYTES)
        .min(usize::MAX as u64) as usize;
    let host = PlannerHost {
        limits: StoreLimitsBuilder::new()
            .memory_size(memory_bytes)
            .table_elements(MAX_TABLE_ELEMENTS)
            .memories(1)
            .tables(1)
            .instances(1)
            .trap_on_grow_failure(true)
            .build(),
    };
    let mut store = Store::new(engine, host);
    store.limiter(|host| &mut host.limits);
    store
        .set_fuel(limits.fuel_limit)
        .map_err(|error| format!("WASM fuel_limit rejected: {error}"))?;

    let linker = Linker::new(engine);
    let instance = linker
        .instantiate(&mut store, &module)
        .map_err(|error| wasm_err("WASM planner denied at instantiate", &error))?;

    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or_else(|| "WASM planner missing exported memory".to_string())?;
    let alloc = instance
        .get_typed_func::<u32, u32>(&mut store, "alloc")
        .map_err(|error| format!("WASM planner missing alloc export: {error}"))?;
    let plan = instance
        .get_typed_func::<(u32, u32), i32>(&mut store, "plan")
        .map_err(|error| format!("WASM planner missing plan export: {error}"))?;
    let output_ptr = instance
        .get_typed_func::<(), u32>(&mut store, "output_ptr")
        .map_err(|error| format!("WASM planner missing output_ptr export: {error}"))?;

    let in_ptr = if input.is_empty() {
        0
    } else {
        let ptr = alloc
            .call(&mut store, input.len() as u32)
            .map_err(|error| wasm_err("WASM planner alloc denied", &error))?;
        if ptr == 0 {
            return Err("WASM planner alloc returned null".into());
        }
        memory
            .write(&mut store, ptr as usize, input)
            .map_err(|error| format!("WASM planner input write denied: {error}"))?;
        ptr
    };

    let n = plan
        .call(&mut store, (in_ptr, input.len() as u32))
        .map_err(|error| wasm_err("WASM planner denied", &error))?;
    let len = n.unsigned_abs() as usize;
    if len > limits.max_output_bytes {
        return Err(format!(
            "WASM planner output {len} bytes exceeds max_output_bytes {}",
            limits.max_output_bytes
        ));
    }
    let ptr = output_ptr
        .call(&mut store, ())
        .map_err(|error| wasm_err("WASM planner output_ptr denied", &error))?;
    let mut buf = vec![0u8; len];
    if len > 0 {
        memory
            .read(&mut store, ptr as usize, &mut buf)
            .map_err(|error| format!("WASM planner output read denied: {error}"))?;
    }
    if n < 0 {
        let reason = String::from_utf8_lossy(&buf);
        return Err(format!("WASM planner denied: {reason}"));
    }
    Ok(buf)
}

fn wasm_err(context: &str, error: &wasmtime::Error) -> String {
    format!("{context}: {error:?}")
}

fn planner_engine() -> Result<&'static Engine, String> {
    static ENGINE: OnceLock<Engine> = OnceLock::new();
    if let Some(engine) = ENGINE.get() {
        return Ok(engine);
    }
    let mut config = Config::new();
    config.consume_fuel(true);
    #[cfg(target_os = "macos")]
    {
        // Match DefraDB/lens: Mach-port trap handlers cannot be mixed in-process.
        config.macos_use_mach_ports(false);
    }
    let engine = Engine::new(&config).map_err(|error| format!("wasmtime engine: {error}"))?;
    Ok(ENGINE.get_or_init(|| engine))
}

pub(crate) fn require_wasm_binary(wasm: &[u8]) -> Result<(), String> {
    if wasm.len() > MAX_WASM_BYTES {
        return Err(format!(
            "WASM module {} bytes exceeds max_wasm_bytes {MAX_WASM_BYTES}",
            wasm.len()
        ));
    }
    if wasm.get(..4) != Some(&b"\0asm"[..]) {
        return Err("WASM planner requires a WebAssembly binary module".into());
    }
    Ok(())
}

fn bounded_u64(value: Option<i64>, field: &str, max: u64) -> Result<u64, String> {
    let n = match value {
        Some(n) if n > 0 => n as u64,
        _ => return Err(format!("CallbackModule.{field} must be a positive integer")),
    };
    if n > max {
        return Err(format!(
            "CallbackModule.{field} {n} exceeds host maximum {max}"
        ));
    }
    Ok(n)
}

fn bounded_u32(value: Option<i64>, field: &str, max: u32) -> Result<u32, String> {
    let n = bounded_u64(value, field, u64::from(max))?;
    u32::try_from(n).map_err(|_| format!("CallbackModule.{field} exceeds u32"))
}

fn bounded_usize(value: Option<i64>, field: &str, max: usize) -> Result<usize, String> {
    let n = bounded_u64(value, field, max as u64)?;
    usize::try_from(n).map_err(|_| format!("CallbackModule.{field} exceeds usize"))
}

#[cfg(test)]
pub(crate) fn fixture_create_workspace_wasm() -> &'static [u8] {
    include_bytes!(env!("GENTS_CALLBACK_FIXTURE_CREATE_WORKSPACE_WASM_PATH"))
}

#[cfg(test)]
pub(crate) fn fixture_wasm_is_stub(bytes: &[u8]) -> bool {
    bytes.len() <= 16
}
