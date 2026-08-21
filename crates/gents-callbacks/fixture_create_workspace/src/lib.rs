//! Fixture callback planner: emit a `create_workspace` ActionPlan.
//!
//! Host ABI (no WASI). Wasmtime instantiates this module with an empty linker;
//! any import is a denial.
//!
//! - `memory` — exported linear memory
//! - `alloc(size: u32) -> u32` — bump pointer for the host to write input JSON
//! - `plan(in_ptr: u32, in_len: u32) -> i32` — output length; negative = error
//! - `output_ptr() -> u32` — start of the last `plan` output in linear memory
//!
//! Input JSON (secrets already stripped by the host):
//! `{ "source": {...}, "args": {...}, "capabilities": ["create_workspace", ...] }`
//!
//! Output JSON is an ActionPlan. No host paths. The host re-validates the plan
//! against granted capabilities; this crate cannot mint capabilities.

use serde_json::{json, Map, Value};

pub const ACTION_PLAN_ABI: u32 = 1;

/// Pure planner used by the wasm entry and host rlib tests.
pub fn plan_create_workspace(
    source: &Value,
    args: &Value,
    _capabilities: &[String],
) -> Result<Value, String> {
    let work_unit_id = required_string(source, "work_unit_id")?;
    let repository_id = required_string(source, "repository_id")?;
    let base_sha = required_string(source, "base_sha")?;
    let branch = required_string(source, "branch")?;
    let workspace_id =
        optional_string(source, "workspace_id").unwrap_or_else(|| work_unit_id.clone());

    let creation_policy = match pick_string(args, source, "creation_policy").as_deref() {
        None | Some("git_worktree_diff") => "git_worktree_diff",
        Some(other) => {
            return Err(format!(
                "creation_policy `{other}` is not implemented in v1"
            ))
        }
    };
    let adapter = match pick_string(args, source, "adapter").as_deref() {
        None | Some("make_worktree") => "make_worktree",
        Some("git_worktree") => "git_worktree",
        Some(other) => return Err(format!("unknown workspace adapter `{other}`")),
    };

    let mut action = Map::new();
    action.insert("adapter".into(), Value::String(adapter.to_string()));
    action.insert("base_sha".into(), Value::String(base_sha));
    action.insert("branch".into(), Value::String(branch));
    if let Some(artifacts) = pick_string_array(args, source, "clone_artifacts") {
        action.insert("clone_artifacts".into(), Value::Array(artifacts));
    }
    action.insert(
        "creation_policy".into(),
        Value::String(creation_policy.to_string()),
    );
    action.insert("repository_id".into(), Value::String(repository_id));
    action.insert("type".into(), Value::String("create_workspace".into()));
    action.insert("work_unit_id".into(), Value::String(work_unit_id));
    action.insert("workspace_id".into(), Value::String(workspace_id));

    let mut plan = Map::new();
    plan.insert("abi".into(), json!(ACTION_PLAN_ABI));
    plan.insert("actions".into(), Value::Array(vec![Value::Object(action)]));
    Ok(Value::Object(plan))
}

/// Decode the host input envelope and run [`plan_create_workspace`].
pub fn plan_from_bytes(input: &[u8]) -> Result<Vec<u8>, String> {
    let value: Value = serde_json::from_slice(input)
        .map_err(|error| format!("planner input is not JSON: {error}"))?;
    if value.get("deployment_id").is_some() {
        return Err("planner input must not include deployment_id".into());
    }
    let source = value
        .get("source")
        .ok_or_else(|| "planner input missing `source`".to_string())?;
    let args = value.get("args").cloned().unwrap_or_else(|| json!({}));
    let capabilities = match value.get("capabilities") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| item.as_str().map(str::to_string))
            .collect(),
        Some(_) => return Err("planner input `capabilities` must be an array".into()),
    };
    let plan = plan_create_workspace(source, &args, &capabilities)?;
    serde_json::to_vec(&plan).map_err(|error| format!("serialize ActionPlan: {error}"))
}

fn pick<'a>(args: &'a Value, source: &'a Value, field: &str) -> Option<&'a Value> {
    args.get(field)
        .filter(|value| !value.is_null())
        .or_else(|| source.get(field).filter(|value| !value.is_null()))
}

fn pick_string(args: &Value, source: &Value, field: &str) -> Option<String> {
    pick(args, source, field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn pick_string_array(args: &Value, source: &Value, field: &str) -> Option<Vec<Value>> {
    pick(args, source, field)
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    item.as_str()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(|value| Value::String(value.to_string()))
                })
                .collect()
        })
}

fn required_string(source: &Value, field: &str) -> Result<String, String> {
    optional_string(source, field).ok_or_else(|| format!("source document missing `{field}`"))
}

fn optional_string(source: &Value, field: &str) -> Option<String> {
    source
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

#[cfg(all(feature = "wasm-entry", target_arch = "wasm32"))]
#[allow(static_mut_refs)]
mod wasm_abi {
    use super::plan_from_bytes;

    static mut OUTPUT: Vec<u8> = Vec::new();

    #[no_mangle]
    pub extern "C" fn alloc(size: u32) -> u32 {
        if size == 0 {
            return 0;
        }
        let mut buf = vec![0u8; size as usize];
        let ptr = buf.as_mut_ptr() as u32;
        std::mem::forget(buf);
        ptr
    }

    #[no_mangle]
    pub extern "C" fn plan(in_ptr: u32, in_len: u32) -> i32 {
        let input = if in_len == 0 {
            &[][..]
        } else {
            unsafe { std::slice::from_raw_parts(in_ptr as *const u8, in_len as usize) }
        };
        match plan_from_bytes(input) {
            Ok(bytes) => store_output(bytes),
            Err(error) => {
                let msg = if error.is_empty() {
                    "planner error".to_string()
                } else {
                    error
                };
                -store_output(msg.into_bytes())
            }
        }
    }

    #[no_mangle]
    pub extern "C" fn output_ptr() -> u32 {
        unsafe { OUTPUT.as_ptr() as u32 }
    }

    fn store_output(bytes: Vec<u8>) -> i32 {
        let len = i32::try_from(bytes.len()).unwrap_or(i32::MAX);
        unsafe {
            OUTPUT = bytes;
        }
        len
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn emits_create_workspace_without_host_paths() {
        let source = json!({
            "work_unit_id": "unit-1",
            "repository_id": "repo-1",
            "base_sha": "abc",
            "branch": "topic",
            "workspace_id": "ws-1"
        });
        let plan = plan_create_workspace(&source, &json!({}), &[]).expect("plan");
        let encoded = serde_json::to_string(&plan).unwrap();
        assert!(!encoded.contains("host_path"), "{encoded}");
        assert!(!encoded.contains("/tmp"), "{encoded}");
        assert_eq!(plan["abi"], 1);
        assert_eq!(plan["actions"][0]["type"], "create_workspace");
        assert_eq!(plan["actions"][0]["workspace_id"], "ws-1");
        assert_eq!(plan["actions"][0]["adapter"], "make_worktree");
        assert_eq!(plan["actions"][0]["creation_policy"], "git_worktree_diff");
    }

    #[test]
    fn args_override_adapter() {
        let source = json!({
            "work_unit_id": "unit-1",
            "repository_id": "repo-1",
            "base_sha": "abc",
            "branch": "topic"
        });
        let plan =
            plan_create_workspace(&source, &json!({"adapter": "git_worktree"}), &[]).expect("plan");
        assert_eq!(plan["actions"][0]["adapter"], "git_worktree");
        assert_eq!(plan["actions"][0]["workspace_id"], "unit-1");
    }

    #[test]
    fn missing_source_field_is_an_error() {
        let err =
            plan_create_workspace(&json!({"work_unit_id": "unit-1"}), &json!({}), &[]).unwrap_err();
        assert!(err.contains("repository_id"), "{err}");
    }

    #[test]
    fn envelope_rejects_deployment_id() {
        let err =
            plan_from_bytes(br#"{"source":{},"args":{},"deployment_id":"deploy-1"}"#).unwrap_err();
        assert!(err.contains("deployment_id"), "{err}");
    }
}
