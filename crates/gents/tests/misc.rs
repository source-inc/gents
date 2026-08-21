//! Aggregated small integration suites.
//!
//! Each submodule was previously its own test binary; four of them paid a
//! 116–152MB DefraDB/wasmtime link to run two tests apiece, and each carried
//! its own copy of the `support/` tree. One binary pays the link once.
//! Submodules stay self-contained files under `misc/` — run one with
//! `cargo test -p gents --test misc <module>::`.

mod support;

#[path = "misc/apply_property.rs"]
mod apply_property;
#[path = "misc/backend_auth_config.rs"]
mod backend_auth_config;
#[path = "misc/backend_auth_startup.rs"]
mod backend_auth_startup;
#[path = "misc/client_authored_collections_fence.rs"]
mod client_authored_collections_fence;
#[path = "misc/descendant_graph.rs"]
mod descendant_graph;
#[path = "misc/goal_controller.rs"]
mod goal_controller;
#[path = "misc/log_rate_filter.rs"]
mod log_rate_filter;
#[path = "misc/startup_readiness_barrier.rs"]
mod startup_readiness_barrier;
