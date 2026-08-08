mod context;
mod control_watcher;
mod router;
mod startup;

pub(super) use context::StartupBarrier;
pub(in crate::agent) use router::default_hostname;
pub(super) use startup::run_agent;

#[cfg(test)]
use crate::watcher::Watcher;
#[cfg(test)]
use control_watcher::{
    run_control_watcher_inner, CONTROL_FULL_RESCAN_INTERVAL, CONTROL_RECONCILE_DEBOUNCE,
};
#[cfg(test)]
use router::{
    resolve_behavior_for_request, run_router_generation_observer,
    wait_for_next_request_with_latest_snapshot,
};
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;
#[cfg(test)]
use tokio::sync::{mpsc, watch};

#[cfg(test)]
mod tests;
