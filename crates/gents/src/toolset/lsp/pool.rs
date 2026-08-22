use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Notify};

use crate::managed_exec::{spawn_managed_process, ManagedExecKind, SpawnManagedProcessRequest};
use crate::toolset::prepare_managed_command;

use super::catalog::CatalogServer;
use super::client::LspClient;
use super::LspToolConfig;

const MAX_PER_SESSION: usize = 4;
const MAX_GLOBAL: usize = 16;
const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
pub(crate) const INIT_BACKOFF: Duration = Duration::from_secs(180);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PoolKey {
    pub session_id: String,
    pub behavior_id: String,
    pub workspace_root: std::path::PathBuf,
    pub server_name: String,
    pub config_digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryState {
    Starting,
    Ready,
    Retiring,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PoolServerState {
    Starting,
    Ready,
    Retiring,
    Failed(String),
}

struct PoolEntry {
    state: EntryState,
    leases: Arc<AtomicUsize>,
    last_used: Instant,
    idle_timeout: Duration,
    client: Option<Arc<LspClient>>,
    ready: Arc<Notify>,
    start_error: Option<String>,
}

pub(crate) struct LspLease {
    client: Arc<LspClient>,
    leases: Arc<AtomicUsize>,
    slot: Arc<Mutex<PoolEntry>>,
}

impl LspLease {
    pub fn client(&self) -> &LspClient {
        &self.client
    }
}

impl Drop for LspLease {
    fn drop(&mut self) {
        let prev = self.leases.fetch_sub(1, Ordering::SeqCst);
        if prev == 1 {
            let slot = self.slot.clone();
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    complete_retirement_if_idle(slot).await;
                });
            }
        }
    }
}

struct StartingCleanup {
    pool: LspPool,
    key: PoolKey,
    slot: Arc<Mutex<PoolEntry>>,
    disarm: Arc<AtomicBool>,
}

impl Drop for StartingCleanup {
    fn drop(&mut self) {
        if self.disarm.load(Ordering::SeqCst) {
            return;
        }
        let pool = self.pool.clone();
        let key = self.key.clone();
        let slot = self.slot.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                {
                    let mut entry = slot.lock().await;
                    if entry.state == EntryState::Starting {
                        entry.start_error = Some("language-server start cancelled".into());
                        entry.state = EntryState::Retiring;
                        entry.ready.notify_waiters();
                    }
                }
                pool.inner.lock().await.remove(&key);
            });
        }
    }
}

async fn retire_slot(slot: Arc<Mutex<PoolEntry>>) {
    let client = {
        let mut entry = slot.lock().await;
        entry.state = EntryState::Retiring;
        entry.ready.notify_waiters();
        if entry.leases.load(Ordering::SeqCst) == 0 {
            entry.client.take()
        } else {
            None
        }
    };
    if let Some(client) = client {
        client.shutdown_exit().await;
    }
}

async fn lru_idle_ready(
    map: &HashMap<PoolKey, Arc<Mutex<PoolEntry>>>,
    pred: impl Fn(&PoolKey) -> bool,
) -> Option<PoolKey> {
    let mut victim: Option<(PoolKey, Instant)> = None;
    for (key, slot) in map.iter() {
        if !pred(key) {
            continue;
        }
        let entry = slot.lock().await;
        if entry.state == EntryState::Ready && entry.leases.load(Ordering::SeqCst) == 0 {
            if victim
                .as_ref()
                .is_none_or(|(_, used)| entry.last_used < *used)
            {
                victim = Some((key.clone(), entry.last_used));
            }
        }
    }
    victim.map(|(key, _)| key)
}

async fn complete_retirement_if_idle(slot: Arc<Mutex<PoolEntry>>) {
    let client = {
        let mut entry = slot.lock().await;
        if entry.state == EntryState::Retiring && entry.leases.load(Ordering::SeqCst) == 0 {
            entry.client.take()
        } else {
            None
        }
    };
    if let Some(client) = client {
        client.shutdown_exit().await;
    }
}

#[derive(Clone)]
pub struct LspPool {
    inner: Arc<Mutex<HashMap<PoolKey, Arc<Mutex<PoolEntry>>>>>,
    failed: Arc<Mutex<HashMap<PoolKey, (String, Instant)>>>,
    sweep_started: Arc<AtomicBool>,
}

impl Default for LspPool {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            failed: Arc::new(Mutex::new(HashMap::new())),
            sweep_started: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl LspPool {
    pub fn new() -> Self {
        let pool = Self::default();
        pool.try_spawn_sweeper();
        pool
    }

    fn try_spawn_sweeper(&self) {
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        if self.sweep_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let pool = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(SWEEP_INTERVAL);
            interval.tick().await;
            loop {
                interval.tick().await;
                pool.sweep_idle().await;
            }
        });
    }

    pub(crate) async fn sweep_idle(&self) {
        let now = Instant::now();
        let victims: Vec<PoolKey> = {
            let map = self.inner.lock().await;
            let mut keys = Vec::new();
            for (key, slot) in map.iter() {
                let entry = slot.lock().await;
                if entry.state == EntryState::Ready
                    && entry.leases.load(Ordering::SeqCst) == 0
                    && now.duration_since(entry.last_used) >= entry.idle_timeout
                {
                    keys.push(key.clone());
                }
            }
            keys
        };
        for key in victims {
            self.retire(&key).await;
        }
    }

    pub(crate) async fn get_ready(&self, key: &PoolKey) -> Option<LspLease> {
        let map = self.inner.lock().await;
        let slot = map.get(key)?.clone();
        drop(map);
        let mut entry = slot.lock().await;
        if entry.state != EntryState::Ready {
            return None;
        }
        let client = entry.client.clone()?;
        if !client.is_alive() {
            drop(entry);
            self.retire(key).await;
            return None;
        }
        entry.leases.fetch_add(1, Ordering::SeqCst);
        entry.last_used = Instant::now();
        Some(LspLease {
            client,
            leases: entry.leases.clone(),
            slot: slot.clone(),
        })
    }

    pub(crate) async fn get_or_start(
        &self,
        key: PoolKey,
        server: &CatalogServer,
        config: &LspToolConfig,
    ) -> Result<LspLease, String> {
        self.try_spawn_sweeper();
        {
            let mut failed = self.failed.lock().await;
            if let Some((error, at)) = failed.get(&key) {
                if at.elapsed() < INIT_BACKOFF {
                    return Err(error.clone());
                }
                failed.remove(&key);
            }
        }

        let (slot, starter, evicted) = {
            let mut map = self.inner.lock().await;
            let mut evicted = Vec::new();
            let mut reusable = None;
            if let Some(existing) = map.get(&key).cloned() {
                let dead = {
                    let entry = existing.lock().await;
                    entry.state == EntryState::Ready
                        && entry
                            .client
                            .as_ref()
                            .is_some_and(|client| !client.is_alive())
                };
                if dead {
                    map.remove(&key);
                    evicted.push(existing);
                } else {
                    reusable = Some(existing);
                }
            }
            if let Some(existing) = reusable {
                (existing, false, evicted)
            } else {
                let Some(cap_victims) = self.take_eviction_victims(&mut map, &key).await else {
                    return Err("language-server client cap reached".into());
                };
                evicted.extend(cap_victims);
                let slot = Arc::new(Mutex::new(PoolEntry {
                    state: EntryState::Starting,
                    leases: Arc::new(AtomicUsize::new(0)),
                    last_used: Instant::now(),
                    idle_timeout: config.idle_timeout,
                    client: None,
                    ready: Arc::new(Notify::new()),
                    start_error: None,
                }));
                map.insert(key.clone(), slot.clone());
                (slot, true, evicted)
            }
        };
        for victim in evicted {
            retire_slot(victim).await;
        }

        if !starter {
            loop {
                let mut entry = slot.lock().await;
                if entry.state == EntryState::Ready {
                    if let Some(client) = entry.client.clone() {
                        entry.leases.fetch_add(1, Ordering::SeqCst);
                        entry.last_used = Instant::now();
                        return Ok(LspLease {
                            client,
                            leases: entry.leases.clone(),
                            slot: slot.clone(),
                        });
                    }
                }
                if let Some(error) = &entry.start_error {
                    return Err(error.clone());
                }
                if entry.state == EntryState::Retiring {
                    return Err("language-server client is retiring".into());
                }
                let ready = entry.ready.clone();
                let notified = ready.notified();
                tokio::pin!(notified);
                // `notify_waiters` does not retain a permit. Register this
                // waiter before releasing the state lock so the starter
                // cannot publish Ready in the registration gap.
                notified.as_mut().enable();
                drop(entry);
                notified.await;
            }
        }

        let cleanup = StartingCleanup {
            pool: self.clone(),
            key: key.clone(),
            slot: slot.clone(),
            disarm: Arc::new(AtomicBool::new(false)),
        };
        let started = self.start_client(&key, server, config).await;
        cleanup.disarm.store(true, Ordering::SeqCst);
        match started {
            Ok(client) => {
                let client = Arc::new(client);
                let leases = {
                    let mut entry = slot.lock().await;
                    entry.client = Some(client.clone());
                    entry.state = EntryState::Ready;
                    entry.last_used = Instant::now();
                    entry.idle_timeout = config.idle_timeout;
                    entry.leases.fetch_add(1, Ordering::SeqCst);
                    entry.ready.notify_waiters();
                    entry.leases.clone()
                };
                Ok(LspLease {
                    client,
                    leases,
                    slot,
                })
            }
            Err(error) => {
                {
                    let mut entry = slot.lock().await;
                    entry.start_error = Some(error.clone());
                    entry.state = EntryState::Retiring;
                    entry.ready.notify_waiters();
                }
                self.inner.lock().await.remove(&key);
                self.failed
                    .lock()
                    .await
                    .insert(key, (error.clone(), Instant::now()));
                Err(error)
            }
        }
    }

    async fn start_client(
        &self,
        key: &PoolKey,
        server: &CatalogServer,
        config: &LspToolConfig,
    ) -> Result<LspClient, String> {
        let workspace = key.workspace_root.clone();
        let constraints = super::overlay_lsp_constraints(&config.constraints);
        let (program, argv, env, _sandbox) =
            prepare_managed_command(&workspace, &server.command, &server.args, &constraints)
                .map_err(|err| err.to_string())?;
        let mut full_argv = vec![program.to_string_lossy().into_owned()];
        full_argv.extend(argv);
        let process = spawn_managed_process(SpawnManagedProcessRequest {
            argv: full_argv,
            cwd: workspace.clone(),
            environment: Some(env),
            tool_name: Some("lsp".into()),
            kind: ManagedExecKind::PersistentService,
        })
        .await?;
        let client = LspClient::start(process, server.name.clone(), server, workspace)?;
        if let Err(error) = client.initialize().await {
            client.shutdown_exit().await;
            return Err(error);
        }
        client
            .wait_until_ready(server.workspace_ready_timings.as_ref())
            .await;
        Ok(client)
    }

    async fn take_eviction_victims(
        &self,
        map: &mut HashMap<PoolKey, Arc<Mutex<PoolEntry>>>,
        incoming: &PoolKey,
    ) -> Option<Vec<Arc<Mutex<PoolEntry>>>> {
        let session_count = map
            .keys()
            .filter(|key| {
                key.session_id == incoming.session_id && key.behavior_id == incoming.behavior_id
            })
            .count();
        let mut victims = Vec::new();
        if session_count >= MAX_PER_SESSION {
            let victim = lru_idle_ready(map, |key| {
                key.session_id == incoming.session_id && key.behavior_id == incoming.behavior_id
            })
            .await?;
            if let Some(slot) = map.remove(&victim) {
                victims.push(slot);
            } else {
                return None;
            }
        }
        if map.len() >= MAX_GLOBAL {
            let victim = lru_idle_ready(map, |_| true).await?;
            if let Some(slot) = map.remove(&victim) {
                victims.push(slot);
            } else {
                return None;
            }
        }
        Some(victims)
    }

    pub async fn reload_snapshot(
        &self,
        session_id: &str,
        behavior_id: &str,
        workspace: &std::path::Path,
        digest: &str,
    ) -> usize {
        let keys: Vec<PoolKey> = {
            let map = self.inner.lock().await;
            map.keys()
                .filter(|key| {
                    key.session_id == session_id
                        && key.behavior_id == behavior_id
                        && key.workspace_root == workspace
                        && key.config_digest == digest
                })
                .cloned()
                .collect()
        };
        let count = keys.len();
        for key in keys {
            self.retire(&key).await;
        }
        count
    }

    pub async fn retire(&self, key: &PoolKey) {
        let slot = {
            let mut map = self.inner.lock().await;
            map.remove(key)
        };
        if let Some(slot) = slot {
            retire_slot(slot).await;
        }
    }

    pub async fn close_session(&self, session_id: &str) {
        let keys: Vec<PoolKey> = {
            let map = self.inner.lock().await;
            map.keys()
                .filter(|key| key.session_id == session_id)
                .cloned()
                .collect()
        };
        for key in keys {
            self.retire(&key).await;
        }
        self.failed
            .lock()
            .await
            .retain(|key, _| key.session_id != session_id);
    }

    pub async fn shutdown(&self) {
        let keys: Vec<PoolKey> = {
            let map = self.inner.lock().await;
            map.keys().cloned().collect()
        };
        for key in keys {
            self.retire(&key).await;
        }
        self.failed.lock().await.clear();
    }

    pub async fn has_ready(&self, key: &PoolKey) -> bool {
        let map = self.inner.lock().await;
        if let Some(slot) = map.get(key) {
            let entry = slot.lock().await;
            return entry.state == EntryState::Ready && entry.client.is_some();
        }
        false
    }

    pub async fn live_count(&self) -> usize {
        self.inner.lock().await.len()
    }

    pub(crate) async fn inspect_session(
        &self,
        session_id: &str,
        behavior_id: &str,
        workspace: &std::path::Path,
        digest: &str,
    ) -> HashMap<String, PoolServerState> {
        let map = self.inner.lock().await;
        let mut states = HashMap::new();
        for (key, slot) in map.iter() {
            if key.session_id == session_id
                && key.behavior_id == behavior_id
                && key.workspace_root == workspace
                && key.config_digest == digest
            {
                let entry = slot.lock().await;
                let state = match entry.state {
                    EntryState::Starting => PoolServerState::Starting,
                    EntryState::Ready
                        if entry
                            .client
                            .as_ref()
                            .is_some_and(|client| !client.is_alive()) =>
                    {
                        PoolServerState::Failed(
                            "language server exited; next action restarts it".into(),
                        )
                    }
                    EntryState::Ready => PoolServerState::Ready,
                    EntryState::Retiring => PoolServerState::Retiring,
                };
                states.insert(key.server_name.clone(), state);
            }
        }
        drop(map);
        let failed = self.failed.lock().await;
        for (key, (error, at)) in failed.iter() {
            if key.session_id == session_id
                && key.behavior_id == behavior_id
                && key.workspace_root == workspace
                && key.config_digest == digest
                && at.elapsed() < INIT_BACKOFF
            {
                states.insert(
                    key.server_name.clone(),
                    PoolServerState::Failed(error.clone()),
                );
            }
        }
        states
    }

    #[cfg(test)]
    pub async fn expire_init_backoffs(&self) {
        let mut failed = self.failed.lock().await;
        for (_, at) in failed.values_mut() {
            *at = Instant::now()
                .checked_sub(INIT_BACKOFF + Duration::from_secs(1))
                .unwrap_or_else(Instant::now);
        }
    }

    #[cfg(test)]
    pub async fn force_last_used(&self, key: &PoolKey, used: Instant) {
        let map = self.inner.lock().await;
        if let Some(slot) = map.get(key) {
            slot.lock().await.last_used = used;
        }
    }

    #[cfg(test)]
    pub async fn force_idle_timeout(&self, key: &PoolKey, timeout: Duration) {
        let map = self.inner.lock().await;
        if let Some(slot) = map.get(key) {
            slot.lock().await.idle_timeout = timeout;
        }
    }
}
