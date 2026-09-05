//! Grok shim leader server: the Unix-domain-socket leader that stock Grok
//! attaches to as its pager client.
//!
//! Gents binds the socket; the pager client connects to it. Ownership and
//! lifecycle contract (every rule below is enforced in this file and exercised
//! by the tests at the bottom):
//!
//! * Election is exclusive. [`spawn_leader`] acquires the sibling
//!   extension-swapped lock at `socket_path.with_extension("lock")` — the same
//!   extension swap a stock Grok leader performs, so the shim and the closed
//!   source leader can never both own one socket. The lock file is opened with
//!   `O_NOFOLLOW` so a symlink planted at the lock path cannot redirect the
//!   open, forced to `0600`, and locked with a *nonblocking exclusive* lock
//!   before anything is published; the holder PID is written for diagnostics.
//!   A second leader on the same socket path fails fast and never removes or
//!   replaces the winner's lock or socket.
//! * The open lock guard is moved into the accept-loop future, so the lock is
//!   held for exactly the lifetime of the spawned listener task, not merely
//!   for the lifetime of [`LeaderHandle`].
//! * The socket is published atomically: the listener binds inside a private
//!   `0700` short same-device staging ancestor, the socket is forced to
//!   `0600` while it is still unreachable inside the staging directory, and
//!   only then is it `rename(2)`d onto the published path. Binding therefore
//!   never depends on the length of the published path, so both a long parent
//!   and a long filename can really bind and connect, and a pager client only
//!   ever observes a finished `0600` socket or no socket at all.
//! * Registration order is enforced: the first frame on a connection must be a
//!   valid `register` envelope; `registered` is written only after it
//!   validates, with `leader_binary_version = gents-<CARGO_PKG_VERSION>`.
//! * Delegates are constructed per registered connection: the leader holds an
//!   [`AcpDelegateFactory`] and only calls it after a `register` frame
//!   validates, passing the assigned client id plus the registration's
//!   mode/capabilities. A connection that fails registration constructs zero
//!   delegates, and `on_disconnect` runs only on the delegate that connection
//!   constructed.
//! * `ping` is answered with `pong`, `acp` frames are dispatched to the
//!   [`AcpDelegate`] with a connection-scoped outbound handle, unsupported
//!   `control` commands answer a method-not-found error envelope, and
//!   `disconnect` (or EOF) triggers connection cleanup and the delegate's
//!   disconnect notification.
//! * On a clean stop the accept loop announces `shutting_down` plus `shutdown`
//!   to every live connection, releases the leader lock, and removes the
//!   socket. The private lock inode is retained to avoid split-lock races.
//!   [`LeaderHandle::shutdown`] awaits that clean stop.
//!   Dropping the handle without shutting down is the documented emergency
//!   path: it aborts the listener task and unlinks the published socket, and
//!   deliberately leaves the lock file for the next leader to reclaim, because
//!   unlinking a lock file the dropper may no longer hold would let a new
//!   leader lose exclusivity.
//!
//! Two deliberate adjustments to the leader contract, both forced by the
//! "no Cargo dependency change" rule for this shim:
//!
//! * The nonblocking exclusive lock is taken with `std::fs::File::try_lock`.
//!   On Unix the standard library implements that API with `flock(2)`
//!   (`LOCK_EX | LOCK_NB`) on the open file description, which is exactly the
//!   primitive the leader contract requires. `gents-cli` has no `libc` or
//!   `nix` dependency and this slice may not add one, so the shim calls the
//!   standard-library API instead of `libc::flock` directly.
//! * `O_NOFOLLOW` is passed to `OpenOptions::custom_flags` from a per-target
//!   constant declared in this file with the same ABI value `libc`/`nix` use.
//!
//! Expected `super::protocol` surface (owned by the wire-codec slice;
//! convergence reconciles any naming drift):
//!
//! * `ClientEnvelope` enum, serde-tagged `"type"`, snake_case:
//!   `Register { client_type: String, mode: String, capabilities:
//!   ClientCapabilities }`, `Acp { payload: String }`,
//!   `Control { request_id: String, command: serde_json::Value }`, `Ping`,
//!   `Disconnect`.
//! * `ServerEnvelope` enum: `Registered { client_id: u64, ready: bool,
//!   leader_protocol_version: u32, leader_binary_version: String,
//!   leader_capabilities: LeaderCapabilities }`, `Acp { payload: String }`,
//!   `Pong`, `Error { code: i32, message: String }`,
//!   `ShuttingDown { reason: String, delay_ms: u64 }`, `Shutdown`.
//! * `ClientCapabilities { yolo_mode, auto_mode, default_model, client_version,
//!   code_nav_enabled, terminal, fs_read, fs_write, status_line }` and
//!   `LeaderCapabilities { control_v1, runtime_cpu_profile, profile_formats,
//!   workspace_exposure, relaunch_v1 }`, both `Clone + Debug`.
//! * `pub const LEADER_PROTOCOL_VERSION: u32 = 1;`
//! * `async fn read_frame<R: AsyncRead + Unpin, E: DeserializeOwned>(reader:
//!   &mut R) -> anyhow::Result<Option<E>>` — `Ok(None)` is a clean EOF before
//!   any payload byte; truncation, oversize, and invalid JSON are errors.
//! * `async fn write_frame<W: AsyncWrite + Unpin, E: Serialize>(writer: &mut W,
//!   envelope: &E) -> anyhow::Result<()>` — four-byte big-endian length
//!   prefix.

use std::fs::{File, OpenOptions, Permissions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use futures_util::future::BoxFuture;
use serde_json::Value;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use uuid::Uuid;

use super::protocol::{
    read_client_envelope, write_server_envelope, ClientCapabilities, ClientEnvelope,
    LeaderCapabilities, ProtocolError, RegisterMode, ServerEnvelope, ShutdownReason,
    LEADER_PROTOCOL_VERSION,
};

/// Tracing target for every log line this module emits.
const LOG_TARGET: &str = "gents_cli::commands::grok_shim::server";

/// Envelope error code for a frame that violates the leader protocol.
const ENVELOPE_ERROR_INVALID_REQUEST: i32 = -32600;

/// Envelope error code for a leader method this shim does not implement.
const ENVELOPE_ERROR_METHOD_NOT_FOUND: i32 = -32601;

/// JSON-RPC internal-error code used when ACP dispatch itself fails.
const JSONRPC_INTERNAL_ERROR: i64 = -32603;

/// Mode forced on the lock file and on the published socket.
const PRIVATE_FILE_MODE: u32 = 0o600;

/// Mode of the private staging ancestor directory.
const STAGING_DIR_MODE: u32 = 0o700;

/// Mode of every socket-parent directory the shim creates itself. Existing
/// directories are never chmodmed to this mode; it applies only to components
/// that did not exist and are created by [`ensure_socket_parent`].
const SOCKET_PARENT_DIR_MODE: u32 = 0o700;

/// Name of the socket *inside* the staging directory. A single character keeps
/// the bind path short even when the published path is near the `sun_path`
/// limit, which is the whole point of staging.
const STAGED_SOCKET_NAME: &str = "s";

/// Prefix of the private staging directory; the eight random hex characters
/// that follow keep the entire staging bind path short.
const STAGING_DIR_PREFIX: &str = ".gents-grok-";

/// Naming attempts per staging ancestor before falling back to a deeper one.
const STAGING_ATTEMPTS: usize = 8;

/// Upper bound on one per-connection dispatch/writer cleanup operation.
const MAX_SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

/// Internal bound for joining all live connection tasks during leader
/// shutdown. This is deliberately independent of the pager-facing shutdown
/// envelope, whose delay remains zero: the wire asks clients to stop now,
/// while the leader still waits for their `on_disconnect` cleanup to finish.
const CONNECTION_CLEANUP_GRACE: Duration = Duration::from_secs(5);

/// Grok's audited shutdown envelope is immediate. This is a wire value, not
/// the server's internal cleanup deadline.
const WIRE_SHUTDOWN_DELAY_MS: u64 = 0;

/// Maximum number of complete outbound envelopes waiting behind a slow pager.
const OUTBOUND_FRAME_CAPACITY: usize = 256;

/// One decoded inbound envelope may wait while the connection loop services a
/// lifecycle event. The dedicated reader retains partial frame bytes across
/// `select!` cancellation in the connection loop.
const INBOUND_FRAME_CAPACITY: usize = 1;

/// Maximum concurrent ACP dispatches per connection. This still permits a
/// prompt and its cancel to overlap without allowing an unbounded task fanout.
const MAX_ACP_DISPATCH_TASKS: usize = 64;

/// Attempts to acquire and revalidate one stable lock-file inode.
const LOCK_ACQUIRE_ATTEMPTS: usize = 8;

/// Published socket path length exercised by the near-limit tests. `sun_path`
/// is 104 bytes on macOS and 108 on Linux, so 100 bytes of path is genuinely
/// near the limit while remaining connectable on every supported target.
#[cfg(test)]
const NEAR_LIMIT_PATH_BYTES: usize = 100;

/// `O_NOFOLLOW` for `OpenOptions::custom_flags`.
///
/// The values are the OS ABI constants that `libc` and `nix` define per target.
/// Unsupported Unix targets fail the build rather than silently opening a lock
/// path through a symlink.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("the grok shim leader lock needs this target's O_NOFOLLOW value; add it here");
#[cfg(target_os = "linux")]
const O_NOFOLLOW: i32 = 0x0000_8000;
#[cfg(target_os = "macos")]
const O_NOFOLLOW: i32 = 0x0000_0040;

// ---------------------------------------------------------------------------
// ACP delegate seam
// ---------------------------------------------------------------------------

/// Connection-scoped outbound handle for ACP payloads.
///
/// The leader owns frame writing; the delegate only hands it finished JSON-RPC
/// lines. The handle is cheap to clone, so a delegate can keep one for a
/// deferred response (for example a `session/prompt` result that must wait for
/// turn terminalization) and push notifications while a turn streams. Sending
/// after the connection closed is an error the caller may ignore.
#[derive(Clone)]
pub(crate) struct AcpOutbound {
    frames: FrameSender,
}

#[derive(Clone)]
enum FrameSender {
    Bounded(mpsc::Sender<ServerEnvelope>),
    #[cfg(test)]
    Unbounded(mpsc::UnboundedSender<ServerEnvelope>),
}

impl FrameSender {
    async fn send(&self, envelope: ServerEnvelope) -> Result<()> {
        match self {
            FrameSender::Bounded(frames) => frames
                .send(envelope)
                .await
                .map_err(|_| anyhow!("the grok shim leader connection is closed")),
            #[cfg(test)]
            FrameSender::Unbounded(frames) => frames
                .send(envelope)
                .map_err(|_| anyhow!("the grok shim leader connection is closed")),
        }
    }

    /// Nonblocking connection-loop send. Protocol/control/lifecycle frames
    /// cannot park the reader behind a saturated data queue; saturation is a
    /// fatal connection condition for these callers.
    fn try_send(&self, envelope: ServerEnvelope) -> Result<()> {
        match self {
            FrameSender::Bounded(frames) => {
                frames.try_send(envelope).map_err(|error| match error {
                    mpsc::error::TrySendError::Full(_) => {
                        anyhow!("the grok shim leader outbound queue is full")
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        anyhow!("the grok shim leader connection is closed")
                    }
                })
            }
            #[cfg(test)]
            FrameSender::Unbounded(frames) => frames
                .send(envelope)
                .map_err(|_| anyhow!("the grok shim leader connection is closed")),
        }
    }
}

impl AcpOutbound {
    /// Wrap a raw frame sender.
    ///
    /// Production connections get their handle from the leader's
    /// per-connection channel; tests (and any other embedder that already owns
    /// a channel) can build one directly.
    #[cfg(test)]
    pub(crate) fn for_frames(frames: mpsc::UnboundedSender<ServerEnvelope>) -> Self {
        Self {
            frames: FrameSender::Unbounded(frames),
        }
    }

    /// Queue one ACP JSON-RPC line for the pager client.
    pub(crate) async fn send(&self, payload: impl Into<String>) -> Result<()> {
        self.frames
            .send(ServerEnvelope::Acp {
                payload: payload.into(),
            })
            .await
    }

    fn try_send(&self, payload: impl Into<String>) -> Result<()> {
        self.frames.try_send(ServerEnvelope::Acp {
            payload: payload.into(),
        })
    }
}

/// The ACP behavior behind the leader, implemented by `acp.rs`.
///
/// The methods are object-safe boxed futures because the leader stores the
/// delegate as `Arc<dyn AcpDelegate>` and dispatches each ACP frame on its own
/// task: a `session/prompt` handler must be able to run to terminalization
/// while the connection keeps reading (a `session/cancel` frame must not wait
/// behind the prompt it cancels).
pub(crate) trait AcpDelegate: Send + Sync + 'static {
    /// Handle one inbound ACP JSON-RPC line. Responses and notifications are
    /// pushed through `outbound`; the leader never interprets ACP payloads.
    fn handle_acp<'a>(
        &'a self,
        payload: &'a str,
        outbound: AcpOutbound,
    ) -> BoxFuture<'a, Result<()>>;

    /// The connection went away (disconnect, EOF, protocol violation, or
    /// leader shutdown). The ACP service drains and interrupts its
    /// connection-scoped pending turns.
    fn on_disconnect(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

/// A validated `register` frame: the registered client's identity for
/// delegate construction.
#[derive(Debug, Clone)]
pub(crate) struct Registration {
    /// `client_type` from the register frame (validated non-empty).
    pub(crate) client_type: String,
    /// Registration mode: interactive stdio pager or headless client.
    pub(crate) mode: RegisterMode,
    /// Capabilities the client advertised on register.
    pub(crate) capabilities: ClientCapabilities,
}

/// Constructs one [`AcpDelegate`] per *registered* connection.
///
/// The leader never shares a delegate across connections: the factory is
/// invoked only after a `register` frame validates, with the connection's
/// generated `client_id` plus the registration's mode/capabilities, and the
/// returned `Arc<dyn AcpDelegate>` is used for that connection alone — its
/// `on_client_capabilities` has already been applied at construction time by
/// the factory, and its `on_disconnect` runs only for that connection. A
/// connection that fails registration constructs zero delegates.
pub(crate) trait AcpDelegateFactory: Send + Sync + 'static {
    fn create_delegate<'a>(
        &'a self,
        client_id: u64,
        registration: &'a Registration,
    ) -> BoxFuture<'a, Result<Arc<dyn AcpDelegate>>>;
}

impl<F> AcpDelegateFactory for F
where
    F: Fn(u64, &Registration) -> Result<Arc<dyn AcpDelegate>> + Send + Sync + 'static,
{
    fn create_delegate<'a>(
        &'a self,
        client_id: u64,
        registration: &'a Registration,
    ) -> BoxFuture<'a, Result<Arc<dyn AcpDelegate>>> {
        Box::pin(async move { self(client_id, registration) })
    }
}

// ---------------------------------------------------------------------------
// Configuration and handle
// ---------------------------------------------------------------------------

/// Configuration for one spawned leader.
#[derive(Debug, Clone)]
pub(crate) struct LeaderServerConfig {
    /// Filesystem path of the published leader socket.
    pub(crate) socket_path: PathBuf,
}

impl LeaderServerConfig {
    pub(crate) fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }
}

/// Handle to one spawned leader. Owns shutdown and the listener task.
pub(crate) struct LeaderHandle {
    socket_path: PathBuf,
    shutdown_tx: watch::Sender<bool>,
    task: Option<JoinHandle<Result<()>>>,
}

impl LeaderHandle {
    /// Published socket path.
    #[cfg(test)]
    pub(crate) fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Leader lock path (the socket path with its extension swapped to
    /// `lock`).
    #[cfg(test)]
    pub(crate) fn lock_path(&self) -> PathBuf {
        self.socket_path.with_extension("lock")
    }

    /// Request a clean stop and wait for the listener task to finish it.
    ///
    /// On return the socket has been removed and the exclusive leader lock has
    /// been released, so a new leader may take the socket. The private lock
    /// inode remains in place deliberately: unlinking a flock file creates a
    /// split-lock race with a concurrent opener.
    pub(crate) async fn shutdown(&mut self) -> Result<()> {
        let _ = self.shutdown_tx.send(true);
        self.join().await
    }

    /// Wait for the listener task to finish without requesting a stop.
    pub(crate) async fn join(&mut self) -> Result<()> {
        match self.task.take() {
            None => Ok(()),
            Some(task) => task
                .await
                .map_err(|error| anyhow!("the grok shim leader task failed to join: {error}"))
                .and_then(|result| result.context("the grok shim leader task failed")),
        }
    }
}

impl Drop for LeaderHandle {
    fn drop(&mut self) {
        let Some(task) = self.task.take() else {
            return;
        };
        task.abort();
        // Emergency cleanup only. The published socket is unlinked
        // synchronously so no pager client can keep connecting to a dead
        // leader. The lock file is deliberately left in place: once the
        // aborted task drops the guard the next `spawn_leader` reclaims it
        // safely, whereas unlinking a lock file this dropper may no longer
        // hold would let a concurrently starting leader lose exclusivity.
        // `shutdown` is the clean path that releases the lock descriptor.
        if let Err(error) = std::fs::remove_file(&self.socket_path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    target: LOG_TARGET,
                    %error,
                    socket = %self.socket_path.display(),
                    "failed to unlink the grok shim socket while dropping a leader handle"
                );
            }
        }
    }
}

/// Spawn the production leader for `config`.
///
/// Election, stale-socket removal, and socket publication all happen before
/// the accept-loop task is spawned, and the open lock guard is moved into that
/// task so the exclusive lock is held for the actual listener lifetime.
///
/// Must be called from within a Tokio runtime: the bound listener registers
/// with the runtime reactor, and the accept loop is a spawned task.
pub(crate) fn spawn_leader(
    config: LeaderServerConfig,
    factory: Arc<dyn AcpDelegateFactory>,
) -> Result<LeaderHandle> {
    let socket_path = normalize_platform_temp_root(&config.socket_path)?;
    if socket_path.file_name().is_none_or(|name| name.is_empty()) {
        bail!(
            "the grok shim leader socket path {} must name a socket file",
            socket_path.display()
        );
    }

    // 1. Exclusive election. Every later step happens under this lock, and a
    //    loser never touches the winner's lock or socket.
    let lock = LeaderLock::acquire(&socket_path)?;

    // 2. Remove a stale socket left by a crashed leader. Only reachable while
    //    we hold the exclusive lock, so this can never delete a live leader's
    //    socket.
    if let Err(error) = remove_stale_socket(&socket_path) {
        lock.release();
        return Err(error);
    }

    // 3. Publish the socket atomically from a private staging ancestor.
    let listener = match publish_listener(&socket_path) {
        Ok(listener) => listener,
        Err(error) => {
            lock.release();
            return Err(error);
        }
    };

    tracing::info!(
        target: LOG_TARGET,
        socket = %socket_path.display(),
        lock = %socket_path.with_extension("lock").display(),
        "grok shim leader listening for the pager client"
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (lifecycle_tx, _lifecycle_rx) = broadcast::channel::<ServerEnvelope>(16);
    let task = tokio::spawn(accept_loop(
        listener,
        // The open guard moves into the accept-loop future: the exclusive
        // lock is held for exactly the spawned listener lifetime.
        lock,
        socket_path.clone(),
        factory,
        lifecycle_tx,
        shutdown_rx,
    ));

    Ok(LeaderHandle {
        socket_path,
        shutdown_tx,
        task: Some(task),
    })
}

/// Resolve the two system temporary-directory aliases macOS installs at the
/// filesystem root (`/tmp -> /private/tmp` and `/var -> /private/var`). The
/// strict parent walk below must still reject every operator-controlled or
/// nested symlink; resolving only these OS-owned root aliases keeps that
/// security boundary while allowing `std::env::temp_dir()` and the documented
/// `/tmp/gents-grok.sock` fallback to work on macOS.
fn normalize_platform_temp_root(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    for root in [Path::new("/tmp"), Path::new("/var")] {
        if !path.starts_with(root) {
            continue;
        }
        let metadata = match std::fs::symlink_metadata(root) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("inspecting platform temporary root {}", root.display())
                });
            }
        };
        if !metadata.file_type().is_symlink() {
            return Ok(path.to_path_buf());
        }
        let canonical = std::fs::canonicalize(root).with_context(|| {
            format!("resolving platform temporary root alias {}", root.display())
        })?;
        let suffix = path.strip_prefix(root).with_context(|| {
            format!(
                "removing platform temporary root {} from {}",
                root.display(),
                path.display()
            )
        })?;
        return Ok(canonical.join(suffix));
    }
    Ok(path.to_path_buf())
}

// ---------------------------------------------------------------------------
// Accept loop
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn accept_loop(
    listener: UnixListener,
    lock: LeaderLock,
    socket_path: PathBuf,
    factory: Arc<dyn AcpDelegateFactory>,
    lifecycle: broadcast::Sender<ServerEnvelope>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let next_client_id = AtomicU64::new(1);
    let mut connections = JoinSet::new();

    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            changed = shutdown.changed() => {
                // `Err` means the handle is gone, which is an implicit stop.
                let _ = changed;
                break;
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _address)) => {
                        let client_id = next_client_id.fetch_add(1, Ordering::Relaxed);
                        let lifecycle_rx = lifecycle.subscribe();
                        connections.spawn(handle_connection(
                            stream,
                            client_id,
                            factory.clone(),
                            lifecycle_rx,
                        ));
                    }
                    Err(error) => {
                        tracing::warn!(
                            target: LOG_TARGET,
                            %error,
                            "grok shim leader failed to accept a pager connection"
                        );
                        // Back off so a persistent accept error cannot hot-spin.
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        }
    }

    // Announce the stop to every live connection, then stop accepting.
    let _ = lifecycle.send(ServerEnvelope::ShuttingDown {
        reason: ShutdownReason::Manual,
        delay_ms: WIRE_SHUTDOWN_DELAY_MS,
    });
    let _ = lifecycle.send(ServerEnvelope::Shutdown);
    // Close the lifecycle channel after queuing both frames. Connections
    // drain the announcements, then observe `Closed` and enter their
    // `on_disconnect` cleanup while the accept loop waits below.
    drop(lifecycle);

    // The pager-facing delay is zero, but clean server shutdown still owns
    // every connection task until its delegate has run `on_disconnect` and
    // its bounded per-connection cleanup has completed. Detaching here would
    // let `LeaderHandle::shutdown` return while pending turns were still live.
    let drain = async { while connections.join_next().await.is_some() {} };
    let cleanup_result = if tokio::time::timeout(CONNECTION_CLEANUP_GRACE, drain)
        .await
        .is_err()
    {
        tracing::warn!(
            target: LOG_TARGET,
            grace_ms = CONNECTION_CLEANUP_GRACE.as_millis(),
            "grok shim leader connection cleanup grace elapsed"
        );
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        Err(anyhow!(
            "grok shim leader connection cleanup exceeded {}ms",
            CONNECTION_CLEANUP_GRACE.as_millis()
        ))
    } else {
        Ok(())
    };

    drop(listener);

    // Clean stop: remove the published socket, then release the leader lock.
    remove_published_socket(&socket_path);
    lock.release();
    tracing::info!(
        target: LOG_TARGET,
        socket = %socket_path.display(),
        "grok shim leader stopped cleanly"
    );
    cleanup_result
}

fn remove_published_socket(socket_path: &Path) {
    match std::fs::remove_file(socket_path) {
        Ok(()) => {
            tracing::debug!(
                target: LOG_TARGET,
                socket = %socket_path.display(),
                "removed the published grok shim socket on clean stop"
            );
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            tracing::warn!(
                target: LOG_TARGET,
                %error,
                socket = %socket_path.display(),
                "failed to remove the published grok shim socket on clean stop"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Per-connection handling
// ---------------------------------------------------------------------------

async fn handle_connection(
    stream: UnixStream,
    client_id: u64,
    factory: Arc<dyn AcpDelegateFactory>,
    mut lifecycle: broadcast::Receiver<ServerEnvelope>,
) {
    let (mut reader, writer) = stream.into_split();
    let (frames_tx, mut frames_rx) = mpsc::channel::<ServerEnvelope>(OUTBOUND_FRAME_CAPACITY);
    let frames_tx = FrameSender::Bounded(frames_tx);
    let (client_frames_tx, mut client_frames_rx) =
        mpsc::channel::<std::result::Result<ClientEnvelope, ProtocolError>>(INBOUND_FRAME_CAPACITY);
    // This task is the sole owner of the read half. The connection loop only
    // selects on decoded-frame delivery, so dropping that receive future for a
    // lifecycle event never drops a partially-read length prefix or payload.
    let reader_task = tokio::spawn(async move {
        loop {
            let frame = read_client_envelope(&mut reader).await;
            let terminal = frame.is_err();
            if client_frames_tx.send(frame).await.is_err() || terminal {
                break;
            }
        }
    });
    let writer_task = tokio::spawn(async move {
        let mut writer = writer;
        while let Some(envelope) = frames_rx.recv().await {
            if let Err(error) = write_server_envelope(&mut writer, &envelope).await {
                tracing::debug!(
                    target: LOG_TARGET,
                    %error,
                    "grok shim leader stopped writing to a pager connection"
                );
                break;
            }
        }
    });

    // Phase 1: the first frame must be a valid register; `registered` is only
    // written after it validates, and the connection's delegate is only
    // constructed after the register frame validates.
    let registration =
        match register_client(&mut client_frames_rx, &frames_tx, &mut lifecycle).await {
            Ok(registration) => registration,
            Err(()) => {
                // register_client already logged the cause and, where the protocol
                // demands it, wrote the error envelope. No delegate was ever
                // constructed for this connection, so there is nothing to notify.
                drop(frames_tx);
                reader_task.abort();
                let _ = reader_task.await;
                let _ = tokio::time::timeout(MAX_SHUTDOWN_GRACE, writer_task).await;
                return;
            }
        };

    let delegate: Arc<dyn AcpDelegate> =
        match factory.create_delegate(client_id, &registration).await {
            Ok(delegate) => delegate,
            Err(error) => {
                // The factory is the seam for per-connection state; failing to
                // construct it must not leak a half-registered connection.
                tracing::error!(
                    target: LOG_TARGET,
                    client_id,
                    %error,
                    "grok shim leader could not construct an ACP delegate for a connection"
                );
                drop(frames_tx);
                reader_task.abort();
                let _ = reader_task.await;
                let _ = tokio::time::timeout(MAX_SHUTDOWN_GRACE, writer_task).await;
                return;
            }
        };

    let _ = frames_tx.try_send(ServerEnvelope::Registered {
        client_id,
        ready: true,
        leader_protocol_version: LEADER_PROTOCOL_VERSION,
        leader_binary_version: leader_binary_version(),
        leader_capabilities: leader_capabilities(),
    });
    tracing::info!(
        target: LOG_TARGET,
        client_id,
        client_type = %registration.client_type,
        mode = registration.mode.wire_name(),
        yolo_mode = registration.capabilities.yolo_mode,
        auto_mode = registration.capabilities.auto_mode,
        terminal = registration.capabilities.terminal,
        "grok shim leader registered a pager client"
    );

    // Phase 2: serve frames. Each ACP frame is dispatched on its own task so
    // a long-running prompt cannot block reading the cancel that stops it.
    let outbound = AcpOutbound {
        frames: frames_tx.clone(),
    };
    let mut acp_tasks: JoinSet<()> = JoinSet::new();
    loop {
        reap_acp_dispatches(&mut acp_tasks);
        tokio::select! {
            biased;
            lifecycle_frame = lifecycle.recv() => {
                match lifecycle_frame {
                    Ok(envelope) => {
                        // Forward `shutting_down` and `shutdown`; the closed
                        // channel below ends the loop.
                        if frames_tx.try_send(envelope).is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(missed)) => {
                        tracing::warn!(
                            target: LOG_TARGET,
                            client_id,
                            missed,
                            "grok shim leader connection missed lifecycle frames; closing it"
                        );
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            frame = client_frames_rx.recv() => {
                match frame {
                    Some(Ok(ClientEnvelope::Ping)) => {
                        if frames_tx.try_send(ServerEnvelope::Pong).is_err() {
                            break;
                        }
                    }
                    Some(Ok(ClientEnvelope::Acp { payload })) => {
                        if !acp_dispatch_has_capacity(&acp_tasks) {
                            tracing::warn!(
                                target: LOG_TARGET,
                                client_id,
                                active = acp_tasks.len(),
                                "grok shim leader rejected ACP dispatch at the per-connection limit"
                            );
                            if let Some(response) = internal_error_response(&payload) {
                                if outbound.try_send(response).is_err() {
                                    break;
                                }
                            } else {
                                // Notifications have no response channel on
                                // which overload can be reported. Closing the
                                // connection invokes `on_disconnect`, which
                                // safely cancels every pending turn instead of
                                // silently dropping a session/cancel.
                                break;
                            }
                        } else {
                            spawn_acp_dispatch(
                                &mut acp_tasks,
                                delegate.clone(),
                                outbound.clone(),
                                payload,
                            );
                        }
                    }
                    Some(Ok(ClientEnvelope::Control { request_id, command })) => {
                        tracing::warn!(
                            target: LOG_TARGET,
                            client_id,
                            request_id = %request_id,
                            command = %command,
                            "grok shim leader received an unsupported control command"
                        );
                        if frames_tx.try_send(ServerEnvelope::Error {
                            code: ENVELOPE_ERROR_METHOD_NOT_FOUND,
                            message: format!(
                                "the Gents leader shim does not implement control commands \
                                 (request {request_id:?}); leader_capabilities.control_v1 is false"
                            ),
                        }).is_err() {
                            break;
                        }
                    }
                    Some(Ok(ClientEnvelope::Register { .. })) => {
                        protocol_violation(
                            &frames_tx,
                            "register is only valid as the first frame on a connection",
                        );
                        break;
                    }
                    Some(Ok(ClientEnvelope::Disconnect)) => {
                        tracing::debug!(
                            target: LOG_TARGET,
                            client_id,
                            "grok shim leader pager client disconnected"
                        );
                        break;
                    }
                    Some(Err(error)) if error.is_connection_closed() => break,
                    Some(Err(error)) => {
                        tracing::debug!(
                            target: LOG_TARGET,
                            client_id,
                            %error,
                            "grok shim leader received an undecodable frame"
                        );
                        protocol_violation(
                            &frames_tx,
                            &format!("undecodable leader frame: {error}"),
                        );
                        break;
                    }
                    None => break,
                }
            }
        }
    }

    // Connection cleanup: drain the delegate's connection-scoped state first
    // so in-flight handlers observe the drained turn table, then let the
    // queued frames flush before the writer is dropped.
    delegate.on_disconnect().await;
    let drain = async { while acp_tasks.join_next().await.is_some() {} };
    if tokio::time::timeout(MAX_SHUTDOWN_GRACE, drain)
        .await
        .is_err()
    {
        tracing::warn!(
            target: LOG_TARGET,
            client_id,
            "grok shim leader closed a connection with ACP dispatch still running"
        );
        acp_tasks.abort_all();
        while acp_tasks.join_next().await.is_some() {}
    }
    drop(outbound);
    drop(frames_tx);
    reader_task.abort();
    let _ = reader_task.await;
    let _ = tokio::time::timeout(MAX_SHUTDOWN_GRACE, writer_task).await;
}

/// Read frames until a valid `register` arrives. `Err(())` means the
/// connection must close; the error envelope (when the protocol requires one)
/// has already been queued.
async fn register_client(
    client_frames: &mut mpsc::Receiver<std::result::Result<ClientEnvelope, ProtocolError>>,
    frames: &FrameSender,
    lifecycle: &mut broadcast::Receiver<ServerEnvelope>,
) -> std::result::Result<Registration, ()> {
    // Every branch below resolves the registration (or closes the
    // connection), so a single select suffices — clippy's `never_loop` is
    // right that the historical `loop` here never iterated.
    let frame = tokio::select! {
        lifecycle_frame = lifecycle.recv() => {
            let _ = lifecycle_frame;
            // The leader is stopping before this client registered.
            return Err(());
        }
        frame = client_frames.recv() => match frame {
            Some(frame) => frame,
            None => return Err(()),
        },
    };
    match frame {
        Ok(ClientEnvelope::Register {
            client_type,
            mode,
            capabilities,
        }) => {
            if let Err(reason) = validate_register(&client_type, &mode) {
                tracing::warn!(
                    target: LOG_TARGET,
                    client_type = %client_type,
                    mode = %mode.wire_name(),
                    reason = %format!("{reason:#}"),
                    "grok shim leader rejected an invalid register frame"
                );
                let _ = frames.try_send(ServerEnvelope::Error {
                    code: ENVELOPE_ERROR_INVALID_REQUEST,
                    message: format!("{reason:#}"),
                });
                return Err(());
            }
            Ok(Registration {
                client_type,
                mode,
                capabilities,
            })
        }
        Ok(_) => {
            protocol_violation(
                frames,
                "the first frame on a leader connection must be register",
            );
            Err(())
        }
        Err(error) if error.is_connection_closed() => {
            tracing::debug!(
                target: LOG_TARGET,
                "grok shim leader connection closed before register"
            );
            Err(())
        }
        Err(error) => {
            protocol_violation(
                frames,
                &format!("undecodable leader frame before register: {error}"),
            );
            Err(())
        }
    }
}

fn validate_register(client_type: &str, _mode: &RegisterMode) -> Result<()> {
    if client_type.trim().is_empty() {
        bail!("register requires a non-empty client_type");
    }
    Ok(())
}

fn protocol_violation(frames: &FrameSender, message: &str) {
    tracing::warn!(target: LOG_TARGET, message, "grok shim leader protocol violation");
    let _ = frames.try_send(ServerEnvelope::Error {
        code: ENVELOPE_ERROR_INVALID_REQUEST,
        message: message.to_string(),
    });
}

fn reap_acp_dispatches(tasks: &mut JoinSet<()>) {
    while let Some(result) = tasks.try_join_next() {
        if let Err(error) = result {
            tracing::warn!(
                target: LOG_TARGET,
                %error,
                "grok shim leader ACP dispatch task failed to join"
            );
        }
    }
}

fn acp_dispatch_has_capacity(tasks: &JoinSet<()>) -> bool {
    tasks.len() < MAX_ACP_DISPATCH_TASKS
}

fn spawn_acp_dispatch(
    tasks: &mut JoinSet<()>,
    delegate: Arc<dyn AcpDelegate>,
    outbound: AcpOutbound,
    payload: String,
) {
    tasks.spawn(async move {
        if let Err(error) = delegate.handle_acp(&payload, outbound.clone()).await {
            tracing::warn!(
                target: LOG_TARGET,
                %error,
                "grok shim leader ACP dispatch failed"
            );
            // A request must never hang: answer it with a JSON-RPC internal
            // error through the ACP channel so the pager can recover.
            if let Some(response) = internal_error_response(&payload) {
                if let Err(error) = outbound.send(response).await {
                    tracing::debug!(
                        target: LOG_TARGET,
                        %error,
                        "grok shim leader could not deliver an ACP failure response"
                    );
                }
            }
        }
    });
}

/// Build a JSON-RPC internal-error response for a failed request. Returns
/// `None` for notifications and undecodable payloads, which expect no answer.
fn internal_error_response(payload: &str) -> Option<String> {
    let value: Value = serde_json::from_str(payload).ok()?;
    let id = value.get("id")?;
    // Only requests are answered; notifications expect no response.
    value.get("method")?;
    Some(
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": JSONRPC_INTERNAL_ERROR,
                "message": "Gents leader shim ACP dispatch failed",
            },
        })
        .to_string(),
    )
}

fn leader_binary_version() -> String {
    format!("gents-{}", env!("CARGO_PKG_VERSION"))
}

fn leader_capabilities() -> LeaderCapabilities {
    // The shim implements none of the optional leader extensions; the pager
    // reads these flags to decide what it may send.
    LeaderCapabilities {
        control_v1: false,
        runtime_cpu_profile: false,
        profile_formats: Vec::new(),
        workspace_exposure: false,
        relaunch_v1: false,
    }
}

// ---------------------------------------------------------------------------
// Socket-parent creation
// ---------------------------------------------------------------------------

/// Ensure every component of `parent` exists as a plain directory.
///
/// This deliberately does *not* use `create_dir_all`: that primitive walks the
/// path through symlinks (`mkdir(2)` on each prefix follows symlinked
/// components), so a symlink planted anywhere in the chain would redirect
/// where the socket and its sibling lock file actually land. Instead the path
/// is walked component by component:
///
/// * an existing directory component is accepted exactly as it is — its mode
///   is left untouched, so the shim never chmods an operator-owned parent,
///   `/tmp`, `/var`, or an `XDG_RUNTIME_DIR`;
/// * an existing symlink component is rejected even when it points to a
///   directory (`symlink_metadata` never follows the link);
/// * any other existing component (a regular file, a socket, ...) is
///   rejected;
/// * a missing component is created with mode `0700` *from creation*
///   (`DirBuilderExt::mode`, which passes the mode to `mkdir(2)` so the
///   directory is never even momentarily group/world-readable), and verified
///   with `symlink_metadata` before the walk descends into it.
fn ensure_socket_parent(parent: &Path) -> Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.mode(SOCKET_PARENT_DIR_MODE);
    let mut current = PathBuf::new();
    for component in parent.components() {
        use std::path::Component;
        match component {
            Component::Prefix(_) | Component::RootDir => {
                current.push(component.as_os_str());
                continue;
            }
            _ => current.push(component.as_os_str()),
        }
        // Lstat the candidate: never follows a symlink component.
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) => {
                let file_type = metadata.file_type();
                if file_type.is_symlink() {
                    bail!(
                        "refusing the grok shim socket parent {}: {} is a symlink",
                        parent.display(),
                        current.display()
                    );
                }
                if !file_type.is_dir() {
                    bail!(
                        "refusing the grok shim socket parent {}: {} exists and is not a directory",
                        parent.display(),
                        current.display()
                    );
                }
                // An existing directory is accepted with whatever mode it has.
                continue;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "inspecting the grok shim socket parent component {}",
                        current.display()
                    )
                });
            }
        }
        // The component is missing: create it 0700 from creation.
        builder.create(&current).with_context(|| {
            format!("creating the grok shim socket parent {}", current.display())
        })?;
        let created = std::fs::symlink_metadata(&current).with_context(|| {
            format!(
                "verifying the newly created grok shim socket parent {}",
                current.display()
            )
        })?;
        if created.file_type().is_symlink() || !created.file_type().is_dir() {
            bail!(
                "the grok shim socket parent component {} was created but is not a plain directory",
                current.display()
            );
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Exclusive leader lock
// ---------------------------------------------------------------------------

/// The exclusive sibling leader lock.
///
/// The lock file is the published socket path with its extension swapped to
/// `lock`. It is opened with `O_NOFOLLOW`, forced to `0600`, and locked with a
/// nonblocking exclusive lock (`File::try_lock`, i.e. `flock(2)` with
/// `LOCK_EX | LOCK_NB` on Unix, on the open file description). The holder PID
/// is written for diagnostics. The open `File` is the guard: the lock lives
/// exactly as long as it does, which is why [`spawn_leader`] moves the guard
/// into the accept-loop future.
struct LeaderLock {
    path: PathBuf,
    file: File,
}

impl LeaderLock {
    fn acquire(socket_path: &Path) -> Result<Self> {
        let path = socket_path.with_extension("lock");
        // The lock file lives beside the published socket, and a spawn may
        // target a socket path whose parent chain does not exist yet (the
        // atomic publication step creates it). Create the parent here — with
        // the symlink-rejecting, no-chmod walk — so the election file always
        // sits next to the socket it elects for.
        if let Some(parent) = path.parent() {
            ensure_socket_parent(parent).with_context(|| {
                format!(
                    "creating the grok shim leader lock parent {}",
                    parent.display()
                )
            })?;
        }
        for _ in 0..LOCK_ACQUIRE_ATTEMPTS {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .mode(PRIVATE_FILE_MODE)
                .custom_flags(O_NOFOLLOW)
                .open(&path)
                .with_context(|| format!("opening the grok shim leader lock {}", path.display()))?;
            if let Err(error) = file.try_lock() {
                if matches!(error, std::fs::TryLockError::WouldBlock) {
                    let holder = read_holder_pid(&path);
                    return Err(anyhow!(
                        "another grok leader already holds {} (last recorded holder pid: {})",
                        path.display(),
                        holder
                            .map(|pid| pid.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                    ));
                }
                return Err(anyhow!(
                    "locking the grok shim leader lock {} failed: {error}",
                    path.display()
                ));
            }
            if file_identity(&file) != path_identity(&path) {
                drop(file);
                continue;
            }
            force_private_mode(&file, &path)?;
            let mut file = file;
            write_holder_pid(&mut file)
                .with_context(|| format!("recording the leader pid in {}", path.display()))?;
            // Revalidate after every mutation of the descriptor. A replaced
            // pathname never becomes the published election point.
            if file_identity(&file) != path_identity(&path) {
                drop(file);
                continue;
            }
            return Ok(Self { path, file });
        }
        bail!(
            "the grok shim leader lock {} changed inode during acquisition",
            path.display()
        )
    }

    /// Release the lock by dropping the descriptor while retaining the stable
    /// private inode. Removing a flock file before dropping the descriptor
    /// lets another leader create and lock a different inode; dropping first
    /// and then unlinking can delete a new leader's lock. A persistent inode is
    /// the only race-free pathname contract for advisory locks.
    fn release(self) {
        tracing::debug!(target: LOG_TARGET, lock = %self.path.display(), "released the grok shim leader lock");
        drop(self.file);
    }
}

/// Force `0600` on the open lock file and verify it through the descriptor.
///
/// The mode passed to `open(2)` is masked by the process umask and ignored
/// entirely when the file already exists, so the mode is set on the open file
/// and then re-read through the same descriptor. Verifying through the
/// descriptor (not the path) fails closed if anything swapped the path.
fn force_private_mode(file: &File, path: &Path) -> Result<()> {
    let current = file
        .metadata()
        .with_context(|| {
            format!(
                "reading the mode of the grok shim leader lock {}",
                path.display()
            )
        })?
        .permissions()
        .mode();
    if current & 0o777 == PRIVATE_FILE_MODE {
        return Ok(());
    }
    file.set_permissions(Permissions::from_mode(PRIVATE_FILE_MODE))
        .with_context(|| {
            format!(
                "forcing mode 0600 on the grok shim leader lock {}",
                path.display()
            )
        })?;
    let forced = file
        .metadata()
        .with_context(|| {
            format!(
                "re-reading the mode of the grok shim leader lock {}",
                path.display()
            )
        })?
        .permissions()
        .mode();
    if forced & 0o777 != PRIVATE_FILE_MODE {
        bail!(
            "the grok shim leader lock {} is mode {:o} instead of 0600",
            path.display(),
            forced & 0o777
        );
    }
    Ok(())
}

fn file_identity(file: &File) -> Option<(u64, u64)> {
    let metadata = file.metadata().ok()?;
    Some((metadata.dev(), metadata.ino()))
}

fn path_identity(path: &Path) -> Option<(u64, u64)> {
    let metadata = std::fs::symlink_metadata(path).ok()?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        return None;
    }
    Some((metadata.dev(), metadata.ino()))
}

/// PID recorded in the lock file, for the "another leader holds the lock"
/// diagnostic. Path-based read: it is only used in a log message.
fn read_holder_pid(lock_path: &Path) -> Option<u32> {
    std::fs::read_to_string(lock_path).ok()?.trim().parse().ok()
}

fn write_holder_pid(file: &mut File) -> Result<()> {
    file.set_len(0)
        .with_context(|| "truncating the grok shim leader lock".to_string())?;
    file.write_all(format!("{}\n", process::id()).as_bytes())
        .with_context(|| "writing the leader pid into the lock file".to_string())?;
    Ok(())
}

/// Remove a stale socket left behind by a crashed leader. Only called while
/// the exclusive lock is held, so this can never delete a live leader's
/// socket. Anything that is not a plain socket file is refused.
fn remove_stale_socket(socket_path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(socket_path) {
        Ok(metadata) => {
            let file_type = metadata.file_type();
            if file_type.is_symlink() {
                bail!(
                    "refusing to replace the symlink at {} with a grok shim leader socket",
                    socket_path.display()
                );
            }
            if !file_type.is_socket() {
                bail!(
                    "{} exists and is not a unix socket; refusing to replace it",
                    socket_path.display()
                );
            }
            std::fs::remove_file(socket_path).with_context(|| {
                format!(
                    "removing the stale grok shim socket {}",
                    socket_path.display()
                )
            })?;
            tracing::debug!(
                target: LOG_TARGET,
                socket = %socket_path.display(),
                "removed a stale grok shim leader socket under the exclusive leader lock"
            );
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "inspecting the grok shim socket path {}",
                    socket_path.display()
                )
            });
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Atomic socket publication
// ---------------------------------------------------------------------------

/// Publish the leader socket atomically.
///
/// The listener binds inside a private `0700` short same-device staging
/// ancestor, so binding never depends on the length of the published path;
/// the socket is forced to `0600` while it is still unreachable inside the
/// staging directory, and only then `rename(2)`d onto the published path.
fn publish_listener(socket_path: &Path) -> Result<UnixListener> {
    let parent = socket_path.parent().ok_or_else(|| {
        anyhow!(
            "the grok shim leader socket path {} has no parent directory",
            socket_path.display()
        )
    })?;
    // Walk the parent chain one component at a time: existing directories are
    // accepted with their own mode (never chmodmed), symlinks and non-directory
    // components are refused, and only missing components are created 0700.
    ensure_socket_parent(parent).with_context(|| {
        format!(
            "creating the grok shim leader socket parent {}",
            parent.display()
        )
    })?;
    let staging = StagingDir::create(parent)?;
    let result = bind_and_publish(&staging.path, socket_path);
    staging.cleanup();
    result
}

fn bind_and_publish(staging_dir: &Path, socket_path: &Path) -> Result<UnixListener> {
    let staged_socket = staging_dir.join(STAGED_SOCKET_NAME);
    let listener = UnixListener::bind(&staged_socket).with_context(|| {
        format!(
            "binding the grok shim socket inside the staging ancestor {}",
            staging_dir.display()
        )
    })?;
    if let Err(error) = force_socket_mode(&staged_socket) {
        let _ = std::fs::remove_file(&staged_socket);
        return Err(error);
    }
    // rename(2) publishes atomically: a pager client either sees no socket or
    // sees the finished 0600 socket, never a partially published one, and the
    // bound socket keeps serving from its new path.
    if let Err(error) = std::fs::rename(&staged_socket, socket_path) {
        let _ = std::fs::remove_file(&staged_socket);
        return Err(error).with_context(|| {
            format!(
                "publishing the grok shim socket at {}",
                socket_path.display()
            )
        });
    }
    Ok(listener)
}

fn force_socket_mode(socket: &Path) -> Result<()> {
    std::fs::set_permissions(socket, Permissions::from_mode(PRIVATE_FILE_MODE)).with_context(
        || {
            format!(
                "forcing mode 0600 on the grok shim socket {}",
                socket.display()
            )
        },
    )?;
    let mode = std::fs::metadata(socket)
        .with_context(|| {
            format!(
                "reading the mode of the grok shim socket {}",
                socket.display()
            )
        })?
        .permissions()
        .mode();
    if mode & 0o777 != PRIVATE_FILE_MODE {
        bail!(
            "the grok shim socket {} is mode {:o} instead of 0600",
            socket.display(),
            mode & 0o777
        );
    }
    Ok(())
}

/// The private short same-device staging ancestor for one publication.
struct StagingDir {
    path: PathBuf,
}

impl StagingDir {
    /// Create a fresh private staging directory for publishing a socket whose
    /// parent is `final_parent`.
    ///
    /// Candidates are the same-device ancestors of `final_parent`, shortest
    /// first, so the bind path stays short even when the published path is
    /// near the `sun_path` limit. `rename(2)` to the published path stays on
    /// one filesystem because the staging directory shares the parent's
    /// device.
    fn create(final_parent: &Path) -> Result<Self> {
        let mut last_error: Option<std::io::Error> = None;
        // Create the staging directory atomically private: the mode rides
        // the `mkdir(2)` call itself (`DirBuilderExt::mode`), so the
        // directory is never even momentarily group/world-readable — unlike
        // a create-then-chmod sequence, where a umask-derived 0755 window
        // exists between the two syscalls.
        let mut builder = std::fs::DirBuilder::new();
        builder.mode(STAGING_DIR_MODE);
        for ancestor in staging_candidate_ancestors(final_parent) {
            for _ in 0..STAGING_ATTEMPTS {
                let candidate = ancestor.join(staging_dir_name());
                match builder.create(&candidate) {
                    Ok(()) => {
                        // Verify the mode actually applied: a filesystem
                        // that ignored the request must not be published
                        // through.
                        let mode = std::fs::metadata(&candidate)
                            .with_context(|| {
                                format!(
                                    "inspecting the grok shim staging ancestor {}",
                                    candidate.display()
                                )
                            })?
                            .permissions()
                            .mode();
                        if mode & 0o777 != STAGING_DIR_MODE {
                            let _ = std::fs::remove_dir(&candidate);
                            bail!(
                                "the grok shim staging ancestor {} is mode {:o} instead of 0700",
                                candidate.display(),
                                mode & 0o777
                            );
                        }
                        tracing::debug!(
                            target: LOG_TARGET,
                            staging = %candidate.display(),
                            "created a private staging ancestor for the grok shim socket"
                        );
                        return Ok(Self { path: candidate });
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(error) => {
                        last_error = Some(error);
                        break;
                    }
                }
            }
        }
        bail!(
            "no writable same-device staging ancestor for {}; last error: {}",
            final_parent.display(),
            last_error
                .map(|error| error.to_string())
                .unwrap_or_else(|| "none".to_string())
        )
    }

    /// Remove the (now empty) staging directory. Best effort: a leftover empty
    /// directory is harmless, unlike a leftover socket.
    fn cleanup(self) {
        if let Err(error) = std::fs::remove_dir(&self.path) {
            tracing::warn!(
                target: LOG_TARGET,
                %error,
                staging = %self.path.display(),
                "failed to remove the grok shim staging ancestor"
            );
        }
    }
}

/// Same-device non-root ancestors of `final_parent`, ordered shortest
/// (shallowest) first. `/` is never a staging candidate: a privileged process
/// must not publish temporary shim directories at the filesystem root.
/// Ancestors on a different device are skipped because publishing from them
/// could not `rename(2)` onto the final path.
fn staging_candidate_ancestors(final_parent: &Path) -> Vec<PathBuf> {
    let parent_dev = match std::fs::metadata(final_parent) {
        Ok(metadata) => metadata.dev(),
        Err(_) => return Vec::new(),
    };
    let mut candidates = Vec::new();
    for ancestor in final_parent.ancestors() {
        if ancestor == Path::new("/") {
            continue;
        }
        if let Ok(metadata) = std::fs::metadata(ancestor) {
            if metadata.dev() == parent_dev {
                candidates.push(ancestor.to_path_buf());
            }
        }
    }
    candidates.sort_by_key(|path| path.as_os_str().len());
    candidates
}

fn staging_dir_name() -> String {
    format!(
        "{STAGING_DIR_PREFIX}{}",
        &Uuid::new_v4().simple().to_string()[..8]
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::Mutex;
    use tokio::time::{sleep, timeout, Instant};

    use crate::commands::grok_shim::protocol::{read_server_envelope, write_client_envelope};
    use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    /// Test double for the ACP delegate: records payloads, capabilities, and
    /// disconnects, echoes every ACP payload back, and can push one deferred
    /// payload after `handle_acp` returns. Each instance records its assigned
    /// client id, so tests can prove two registrations get two distinct
    /// delegate instances.
    struct RecordingDelegate {
        late_push: bool,
        payloads: Mutex<Vec<String>>,
        capabilities: Mutex<Option<(bool, bool, bool, Option<String>)>>,
        client_ids: Mutex<Vec<u64>>,
        disconnects: AtomicUsize,
        /// Monotonic instance id, unique per constructed delegate.
        instance_id: u64,
    }

    impl RecordingDelegate {
        fn new(late_push: bool, instance_id: u64) -> Arc<Self> {
            Arc::new(Self {
                late_push,
                payloads: Mutex::new(Vec::new()),
                capabilities: Mutex::new(None),
                client_ids: Mutex::new(Vec::new()),
                disconnects: AtomicUsize::new(0),
                instance_id,
            })
        }

        fn disconnect_count(&self) -> usize {
            self.disconnects.load(Ordering::SeqCst)
        }

        fn recorded_client_ids(&self) -> Vec<u64> {
            self.client_ids.lock().expect("client id log").clone()
        }
    }

    impl AcpDelegate for RecordingDelegate {
        fn handle_acp<'a>(
            &'a self,
            payload: &'a str,
            outbound: AcpOutbound,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                self.payloads
                    .lock()
                    .expect("payload log")
                    .push(payload.to_string());
                outbound.send(payload.to_string()).await?;
                if self.late_push {
                    let outbound = outbound.clone();
                    tokio::spawn(async move {
                        sleep(Duration::from_millis(10)).await;
                        let _ = outbound
                            .send(
                                json!({
                                    "jsonrpc": "2.0",
                                    "method": "deferred",
                                    "params": {},
                                })
                                .to_string(),
                            )
                            .await;
                    });
                }
                Ok(())
            })
        }

        fn on_disconnect(&self) -> BoxFuture<'_, ()> {
            Box::pin(async move {
                self.disconnects.fetch_add(1, Ordering::SeqCst);
            })
        }
    }

    /// Delegate whose disconnect hook parks until the test releases it. This
    /// makes the leader-vs-connection shutdown ordering observable without
    /// sleeps or scheduler assumptions.
    struct GatedDisconnectDelegate {
        started: tokio::sync::Notify,
        release: tokio::sync::Notify,
        completed: AtomicBool,
    }

    impl GatedDisconnectDelegate {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                started: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
                completed: AtomicBool::new(false),
            })
        }
    }

    impl AcpDelegate for GatedDisconnectDelegate {
        fn handle_acp<'a>(
            &'a self,
            _payload: &'a str,
            _outbound: AcpOutbound,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }

        fn on_disconnect(&self) -> BoxFuture<'_, ()> {
            Box::pin(async move {
                self.started.notify_one();
                self.release.notified().await;
                self.completed.store(true, Ordering::SeqCst);
            })
        }
    }

    /// A recording factory: constructs a fresh [`RecordingDelegate`] per
    /// registered connection and records both the construction count and the
    /// per-connection client ids / capabilities it observed. The instance id
    /// is unique per constructed delegate, which is exactly the property the
    /// leader must preserve: no two registered connections share a delegate.
    #[derive(Default)]
    struct RecordingFactory {
        /// Construct late-push delegates (one deferred ACP push after
        /// `handle_acp` returns).
        late_push: bool,
        /// Every delegate this factory constructed, in registration order.
        delegates: Mutex<Vec<Arc<RecordingDelegate>>>,
        /// The client ids the leader passed in, in registration order.
        client_ids: Mutex<Vec<u64>>,
        /// The registration capabilities the leader passed in.
        capabilities: Mutex<Vec<ClientCapabilities>>,
    }

    impl RecordingFactory {
        fn constructed_count(&self) -> usize {
            self.delegates.lock().expect("delegate log").len()
        }

        fn constructed(&self) -> Vec<Arc<RecordingDelegate>> {
            self.delegates.lock().expect("delegate log").clone()
        }

        fn observed_client_ids(&self) -> Vec<u64> {
            self.client_ids.lock().expect("client id log").clone()
        }
    }

    impl AcpDelegateFactory for RecordingFactory {
        fn create_delegate<'a>(
            &'a self,
            client_id: u64,
            registration: &'a Registration,
        ) -> BoxFuture<'a, Result<Arc<dyn AcpDelegate>>> {
            Box::pin(async move {
                self.client_ids
                    .lock()
                    .expect("client id log")
                    .push(client_id);
                self.capabilities
                    .lock()
                    .expect("capability log")
                    .push(registration.capabilities.clone());
                let delegate =
                    RecordingDelegate::new(self.late_push, (self.constructed_count() + 1) as u64);
                delegate
                    .client_ids
                    .lock()
                    .expect("client id log")
                    .push(client_id);
                *delegate.capabilities.lock().expect("capability log") = Some((
                    registration.capabilities.yolo_mode,
                    registration.capabilities.auto_mode,
                    registration.capabilities.terminal,
                    registration.capabilities.client_version.clone(),
                ));
                self.delegates
                    .lock()
                    .expect("delegate log")
                    .push(delegate.clone());
                Ok(delegate as Arc<dyn AcpDelegate>)
            })
        }
    }

    /// A factory that always fails, for the zero-delegates-on-failure test.
    #[derive(Default)]
    struct FailingFactory {
        attempts: AtomicUsize,
    }

    impl AcpDelegateFactory for FailingFactory {
        fn create_delegate<'a>(
            &'a self,
            _client_id: u64,
            _registration: &'a Registration,
        ) -> BoxFuture<'a, Result<Arc<dyn AcpDelegate>>> {
            Box::pin(async move {
                self.attempts.fetch_add(1, Ordering::SeqCst);
                Err(anyhow!("test factory refuses to construct a delegate"))
            })
        }
    }

    // -- fixtures and helpers ------------------------------------------------

    /// An explicit short root for near-limit path tests, per the slice
    /// contract. Falls back to the platform temp dir when `/tmp` is absent.
    ///
    /// The chosen root is canonicalized: on macOS `/tmp` is a symlink to
    /// `/private/tmp`, and the socket-parent walk deliberately refuses
    /// symlinked components, so the fixture resolves the platform's system
    /// temp symlink once here. Every path the tests then hand to the shim is
    /// a chain of plain directories, which is what production on a real
    /// (non-symlinked) runtime dir looks like.
    fn short_test_root() -> PathBuf {
        // 1. The contract's explicit short root when it is actually writable
        //    and genuinely short: `metadata`/`is_dir` alone accept an
        //    unwritable or over-long mount, and a canonical absolute root
        //    longer than [`MAX_SHORT_ROOT_BYTES`] leaves no room for the
        //    near-limit `sun_path` exercises below.
        let explicit = Path::new("/tmp");
        if is_short_writable_root(explicit) {
            return std::fs::canonicalize(explicit).unwrap_or_else(|_| explicit.to_path_buf());
        }
        // 2. Sandbox escape hatch: a caller-provided short root (used when the
        //    process cannot write `/tmp` at all, e.g. a seatbelt profile that
        //    scopes writes to a workspace). Same writability and length bar.
        if let Ok(override_root) = std::env::var("GENTS_TEST_ROOT") {
            if !override_root.trim().is_empty() {
                let override_root = PathBuf::from(override_root);
                if is_short_writable_root(&override_root) {
                    return std::fs::canonicalize(&override_root).unwrap_or(override_root);
                }
            }
        }
        // 3. The platform temp dir, under the same two bars. A TMPDIR pointed
        //    into a deep sandboxed workspace is writable but longer than
        //    `sun_path`, which would make every real `bind(2)` impossible.
        let platform = std::env::temp_dir();
        if is_short_writable_root(&platform) {
            return std::fs::canonicalize(&platform).unwrap_or(platform);
        }
        // 4. A short *relative* root anchored at the process cwd.
        //
        //    Sandboxed CI checkouts can make every writable absolute location
        //    longer than `sun_path` (104 bytes on macOS) — but `sun_path`
        //    bounds the path *string* passed to bind, and a relative path
        //    keeps that string short. The leader code path is identical for
        //    relative and absolute paths (the parent walk, staging,
        //    `rename(2)`, and connect all resolve against cwd), so near-limit
        //    tests still exercise genuinely near-limit bind/connect strings.
        let workspace = PathBuf::from(".tmp-test-root");
        std::fs::create_dir_all(&workspace).expect("creating the workspace-local short test root");
        workspace
    }

    /// Upper bound for an absolute short-root candidate so the near-limit
    /// fixtures keep room to build deep parents and long filenames under it.
    const MAX_SHORT_ROOT_BYTES: usize = 64;

    /// A root is usable only when it exists, is a directory, this process can
    /// actually create entries inside it, and its canonical form is short
    /// enough to leave room for the near-limit path exercises.
    fn is_short_writable_root(root: &Path) -> bool {
        let Ok(canonical) = std::fs::canonicalize(root) else {
            return false;
        };
        if canonical.as_os_str().len() > MAX_SHORT_ROOT_BYTES || !canonical.is_dir() {
            return false;
        }
        let probe = canonical.join(format!(
            ".gents-root-probe-{}",
            &Uuid::new_v4().simple().to_string()[..8]
        ));
        match std::fs::File::create(&probe) {
            Ok(_) => {
                let _ = std::fs::remove_file(&probe);
                true
            }
            Err(_) => false,
        }
    }

    fn unique_test_root(label: &str) -> PathBuf {
        let root = short_test_root().join(format!(
            "gents-grok-leader-{label}-{}",
            &Uuid::new_v4().simple().to_string()[..8]
        ));
        std::fs::create_dir_all(&root)
            .unwrap_or_else(|error| panic!("creating the test root {}: {error}", root.display()));
        root
    }

    /// A socket path whose *filename* is near the `sun_path` limit.
    ///
    /// Falls back to a plain short filename when the test root alone already
    /// reaches the requested byte target, so the helper never underflows; the
    /// near-limit property is then asserted by the caller.
    fn long_filename_socket_path(root: &Path, target_bytes: usize) -> PathBuf {
        let file_name_len = target_bytes.saturating_sub(root.as_os_str().len() + 1);
        let stem_len = file_name_len.saturating_sub(".sock".len());
        if stem_len <= 8 {
            // The root is too long to build a meaningful near-limit filename;
            // return a plain short name and let the caller's own assertion
            // decide whether the path really approached the limit.
            return root.join("s.sock");
        }
        root.join(format!("{}.sock", "f".repeat(stem_len)))
    }

    /// A socket path whose *parent chain* is near the `sun_path` limit.
    fn long_parent_socket_path(root: &Path, target_bytes: usize) -> PathBuf {
        let file_name = "s.sock";
        let component = "d".repeat(12);
        let mut parent = root.to_path_buf();
        loop {
            let candidate = parent.join(&component);
            if candidate.as_os_str().len() + 1 + file_name.len() > target_bytes {
                break;
            }
            parent = candidate;
        }
        let socket = parent.join(file_name);
        assert!(
            socket.as_os_str().len() + file_name.len() + 1 > target_bytes - 16,
            "the parent chain should really approach the limit"
        );
        socket
    }

    fn test_capabilities() -> ClientCapabilities {
        ClientCapabilities {
            yolo_mode: true,
            auto_mode: false,
            default_model: Some("GLM-5.3-NVFP4".to_string()),
            client_version: Some("grok-pager-test".to_string()),
            code_nav_enabled: false,
            terminal: false,
            fs_read: false,
            fs_write: false,
            status_line: true,
        }
    }

    fn register_envelope() -> ClientEnvelope {
        ClientEnvelope::Register {
            client_type: "grok-pager".to_string(),
            mode: RegisterMode::Stdio,
            capabilities: test_capabilities(),
        }
    }

    async fn connect(socket: &Path) -> (OwnedReadHalf, OwnedWriteHalf) {
        let stream = timeout(TEST_TIMEOUT, UnixStream::connect(socket))
            .await
            .expect("connecting to the leader should not time out")
            .expect("connecting to the leader should succeed");
        stream.into_split()
    }

    async fn write_client_frame(writer: &mut OwnedWriteHalf, envelope: &ClientEnvelope) {
        timeout(TEST_TIMEOUT, write_client_envelope(writer, envelope))
            .await
            .expect("writing a client frame should not time out")
            .expect("writing a client frame should succeed");
    }

    async fn next_server_frame(reader: &mut OwnedReadHalf) -> Option<ServerEnvelope> {
        match timeout(TEST_TIMEOUT, read_server_envelope(reader))
            .await
            .expect("reading a server frame should not time out")
        {
            Ok(envelope) => Some(envelope),
            Err(error) if error.is_connection_closed() => None,
            Err(error) => panic!("reading a server frame should succeed: {error}"),
        }
    }

    /// Connect, send a valid register, and assert the exact `registered`
    /// shape the audited wire requires.
    async fn register(socket: &Path) -> (OwnedReadHalf, OwnedWriteHalf, u64) {
        let (mut reader, mut writer) = connect(socket).await;
        write_client_frame(&mut writer, &register_envelope()).await;
        match next_server_frame(&mut reader).await {
            Some(ServerEnvelope::Registered {
                client_id,
                ready,
                leader_protocol_version,
                leader_binary_version,
                ..
            }) => {
                assert!(ready, "registered must report ready");
                assert_eq!(leader_protocol_version, LEADER_PROTOCOL_VERSION);
                assert_eq!(
                    leader_binary_version,
                    format!("gents-{}", env!("CARGO_PKG_VERSION")),
                    "registered must report the gents-prefixed package version"
                );
                assert!(client_id >= 1, "client ids start at 1");
                (reader, writer, client_id)
            }
            other => panic!("expected registered, got {other:?}"),
        }
    }

    fn assert_socket_mode_0600(socket: &Path) {
        let metadata = std::fs::metadata(socket).unwrap_or_else(|error| {
            panic!(
                "the published socket {} should exist: {error}",
                socket.display()
            )
        });
        assert!(
            metadata.file_type().is_socket(),
            "the published path should be a unix socket"
        );
        assert_eq!(
            metadata.permissions().mode() & 0o777,
            0o600,
            "the published socket must be 0600"
        );
    }

    fn assert_clean_stop(handle: &LeaderHandle) {
        assert!(
            !handle.socket_path().exists(),
            "a clean stop must remove the published socket"
        );
        let lock = std::fs::symlink_metadata(handle.lock_path())
            .expect("a clean stop retains the stable leader lock inode");
        assert!(lock.file_type().is_file());
        assert_eq!(lock.permissions().mode() & 0o777, PRIVATE_FILE_MODE);
    }

    /// Drive one full pager exchange through the production leader: register,
    /// ping/pong, ACP round trip, disconnect, and disconnect observation.
    async fn exercise_leader(socket: &Path, factory: &RecordingFactory) {
        let (mut reader, mut writer, _client_id) = register(socket).await;

        write_client_frame(&mut writer, &ClientEnvelope::Ping).await;
        match next_server_frame(&mut reader).await {
            Some(ServerEnvelope::Pong) => {}
            other => panic!("expected pong, got {other:?}"),
        }

        let payload = json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "initialize",
            "params": {},
        })
        .to_string();
        write_client_frame(
            &mut writer,
            &ClientEnvelope::Acp {
                payload: payload.clone(),
            },
        )
        .await;
        match next_server_frame(&mut reader).await {
            Some(ServerEnvelope::Acp { payload: echoed }) => assert_eq!(echoed, payload),
            other => panic!("expected an acp echo, got {other:?}"),
        }
        // The connection's delegate — the one the factory constructed for it —
        // saw exactly the dispatched payload.
        let delegates = factory.constructed();
        let delegate = delegates
            .last()
            .expect("registration must have constructed a delegate");
        let payloads = delegate.payloads.lock().expect("payload log").clone();
        assert_eq!(
            payloads,
            vec![payload],
            "the connection delegate should have seen exactly the dispatched payload"
        );

        write_client_frame(&mut writer, &ClientEnvelope::Disconnect).await;
        assert!(
            next_server_frame(&mut reader).await.is_none(),
            "the leader should close the connection after disconnect"
        );
        // The server drops the connection only after on_disconnect ran, so
        // observing EOF proves the disconnect notification was delivered to
        // the connection's own delegate.
        assert!(
            delegate.disconnect_count() >= 1,
            "the connection delegate should observe the disconnect"
        );
    }

    /// Spawn a leader, retrying while a just-aborted previous leader is still
    /// releasing its lock (task abort is asynchronous by nature).
    async fn spawn_with_retry(socket: &Path, factory: Arc<RecordingFactory>) -> LeaderHandle {
        let deadline = Instant::now() + TEST_TIMEOUT;
        loop {
            match spawn_leader(
                LeaderServerConfig::new(socket.to_path_buf()),
                factory.clone(),
            ) {
                Ok(handle) => return handle,
                Err(error) => {
                    assert!(
                        Instant::now() < deadline,
                        "a leader should be spawnable after the previous handle was dropped: {error:#}"
                    );
                    sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }

    /// The default recording factory: works with `exercise_leader` and every
    /// test that just needs a functioning per-connection delegate.
    fn recording_factory() -> Arc<RecordingFactory> {
        Arc::new(RecordingFactory::default())
    }

    // -- pure helpers --------------------------------------------------------

    #[test]
    fn leader_binary_version_is_the_prefixed_package_version() {
        assert_eq!(
            leader_binary_version(),
            format!("gents-{}", env!("CARGO_PKG_VERSION"))
        );
    }

    #[test]
    fn register_validation_rejects_blank_client_types() {
        assert!(validate_register("grok-pager", &RegisterMode::Stdio).is_ok());
        assert!(validate_register("grok-pager", &RegisterMode::Headless).is_ok());
        assert!(validate_register("", &RegisterMode::Stdio).is_err());
        assert!(validate_register("   ", &RegisterMode::Stdio).is_err());
    }

    #[test]
    fn internal_error_responses_target_requests_only() {
        assert!(
            internal_error_response(r#"{"jsonrpc":"2.0","method":"x"}"#).is_none(),
            "notifications must not be answered"
        );
        assert!(
            internal_error_response("not json").is_none(),
            "undecodable payloads must not be answered"
        );
        let response = internal_error_response(r#"{"jsonrpc":"2.0","id":7,"method":"x"}"#)
            .expect("requests must be answered");
        let value: Value =
            serde_json::from_str(&response).expect("the failure response is valid JSON-RPC");
        assert_eq!(value["id"], json!(7));
        assert_eq!(value["error"]["code"], json!(JSONRPC_INTERNAL_ERROR));
    }

    #[tokio::test]
    async fn production_outbound_queue_backpressures_until_the_writer_drains() {
        let (frames, mut receiver) = mpsc::channel(1);
        let outbound = AcpOutbound {
            frames: FrameSender::Bounded(frames),
        };
        outbound.send("first").await.expect("first frame fits");
        let blocked = tokio::spawn({
            let outbound = outbound.clone();
            async move { outbound.send("second").await }
        });
        tokio::task::yield_now().await;
        assert!(
            !blocked.is_finished(),
            "the second send must wait while the bounded queue is full"
        );
        assert!(matches!(
            receiver.try_recv(),
            Ok(ServerEnvelope::Acp { .. })
        ));
        blocked
            .await
            .expect("backpressured sender joins")
            .expect("send resumes after drain");
        assert!(matches!(
            receiver.try_recv(),
            Ok(ServerEnvelope::Acp { .. })
        ));
    }

    #[tokio::test]
    async fn acp_dispatch_capacity_is_hard_bounded_and_completed_tasks_reap() {
        let mut tasks = JoinSet::new();
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        for _ in 0..MAX_ACP_DISPATCH_TASKS {
            let release = release.clone();
            tasks.spawn(async move {
                let _permit = release.acquire().await.expect("release semaphore");
            });
        }
        assert!(!acp_dispatch_has_capacity(&tasks));
        release.add_permits(MAX_ACP_DISPATCH_TASKS);
        while tasks.join_next().await.is_some() {}
        reap_acp_dispatches(&mut tasks);
        assert!(acp_dispatch_has_capacity(&tasks));
    }

    #[test]
    fn staging_candidates_prefer_the_shortest_same_device_ancestor() {
        let root = unique_test_root("staging-order");
        let parent = root.join("a").join("b");
        std::fs::create_dir_all(&parent).expect("creating nested test dirs");
        let candidates = staging_candidate_ancestors(&parent);
        assert!(
            !candidates.is_empty(),
            "at least the parent itself qualifies"
        );
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate != Path::new("/")),
            "the filesystem root must never be used for staging"
        );
        let parent_dev = std::fs::metadata(&parent).expect("parent metadata").dev();
        for candidate in &candidates {
            assert_eq!(
                std::fs::metadata(candidate)
                    .expect("candidate metadata")
                    .dev(),
                parent_dev,
                "every candidate must share the socket parent's device"
            );
            assert!(
                parent.starts_with(candidate),
                "every candidate must be an ancestor of (or equal to) the socket parent"
            );
        }
        let lengths: Vec<usize> = candidates
            .iter()
            .map(|path| path.as_os_str().len())
            .collect();
        let mut sorted = lengths.clone();
        sorted.sort_unstable();
        assert_eq!(lengths, sorted, "candidates must be ordered shortest first");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn the_staging_dir_is_private_and_removed_on_cleanup() {
        let root = unique_test_root("staging-dir");
        let staging = StagingDir::create(&root).expect("creating the staging ancestor");
        let mode = std::fs::metadata(&staging.path)
            .expect("staging metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o700, "the staging ancestor must be private");
        let staging_path = staging.path.clone();
        staging.cleanup();
        assert!(
            !staging_path.exists(),
            "cleanup must remove the staging ancestor"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    /// The staging directory is created 0700 *from creation*: the mode rides
    /// the `mkdir(2)` call itself (`DirBuilderExt::mode`), so there is no
    /// create-then-chmod window in which a umask-derived group/world-readable
    /// directory is observable.
    #[cfg(unix)]
    #[test]
    fn the_staging_dir_is_0700_from_creation_and_rejects_unsafe_parents() {
        // 1. A staging directory created inside a normal parent is 0700 the
        //    moment it exists — no chmod-after-create sequence is involved.
        let root = unique_test_root("staging-create-mode");
        let staging = StagingDir::create(&root).expect("creating the staging ancestor");
        assert_eq!(
            std::fs::metadata(&staging.path)
                .expect("staging metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700,
            "the staging ancestor must be 0700 from creation"
        );
        staging.cleanup();
        let _ = std::fs::remove_dir_all(&root);

        // 2. A symlinked socket parent is rejected before any staging
        //    directory is created through it: publication can never be
        //    redirected through a link an attacker controls.
        let root = unique_test_root("staging-symlink-parent");
        let target = root.join("real");
        let link = root.join("link");
        std::fs::create_dir(&target).expect("creating the symlink target");
        std::os::unix::fs::symlink(&target, &link).expect("creating the parent symlink");
        let socket = link.join("leader.sock");
        assert!(
            publish_listener(&socket).is_err(),
            "publication through a symlinked parent must be refused"
        );
        assert!(
            !target.join("leader.sock").exists(),
            "nothing may be published through the symlink"
        );
        // The staging ancestors never leaked into the target directory.
        let leaked: Vec<_> = std::fs::read_dir(&target)
            .expect("target readable")
            .filter_map(std::result::Result::ok)
            .collect();
        assert!(
            leaked.is_empty(),
            "no staging directory may be created through the symlinked parent"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    // -- socket-parent creation ----------------------------------------------

    #[test]
    fn nested_missing_socket_parents_are_created_0700() {
        let root = unique_test_root("parent-nested");
        let parent = root.join("a").join("b").join("c");
        ensure_socket_parent(&parent).expect("the nested parent chain must be created");
        for component in [root.join("a"), root.join("a").join("b"), parent.clone()] {
            let mode = std::fs::metadata(&component)
                .unwrap_or_else(|error| {
                    panic!("component {} must exist: {error}", component.display())
                })
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(
                mode,
                0o700,
                "component {} must be created 0700 from creation",
                component.display()
            );
        }
        // The second call must accept the (now existing) chain without error
        // and without changing any mode.
        ensure_socket_parent(&parent).expect("existing components must be accepted");
        let mode = std::fs::metadata(&parent)
            .expect("parent metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o700);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_pre_existing_socket_parent_keeps_its_own_mode() {
        let root = unique_test_root("parent-mode");
        let parent = root.join("owned");
        std::fs::create_dir(&parent).expect("creating the operator-owned parent");
        // Deliberately different from 0700: the walk must accept the existing
        // directory and must never chmod an operator-owned parent.
        std::fs::set_permissions(&parent, Permissions::from_mode(0o755))
            .expect("loosening the parent mode");
        ensure_socket_parent(&parent).expect("an existing parent must be accepted");
        let mode = std::fs::metadata(&parent)
            .expect("parent metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            mode, 0o755,
            "the pre-existing parent mode must be left untouched"
        );
        // A deeper chain under the pre-existing parent: the existing 0755
        // component keeps its mode, the new one is created 0700.
        let deep = parent.join("created");
        ensure_socket_parent(&deep).expect("the deeper chain must be created");
        assert_eq!(
            std::fs::metadata(&parent)
                .expect("parent metadata")
                .permissions()
                .mode()
                & 0o777,
            0o755,
            "the existing component must not be chmodmed even mid-walk"
        );
        assert_eq!(
            std::fs::metadata(&deep)
                .expect("new component metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700,
            "the new component must be created 0700"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_symlink_component_is_rejected_and_its_target_untouched() {
        let root = unique_test_root("parent-symlink");
        let target = root.join("target-dir");
        std::fs::create_dir(&target).expect("creating the symlink target");
        std::fs::set_permissions(&target, Permissions::from_mode(0o755))
            .expect("setting the target mode");
        let link = root.join("link");
        std::os::unix::fs::symlink(&target, &link).expect("creating the symlink");
        // The symlink points at a directory, but it must still be refused.
        let deep = link.join("nested");
        let error = ensure_socket_parent(&deep).expect_err("a symlink component must be rejected");
        assert!(
            error.to_string().contains("is a symlink"),
            "the error should name the symlink component: {error:#}"
        );
        assert!(
            !target.join("nested").exists(),
            "the symlink target must be untouched"
        );
        assert_eq!(
            std::fs::symlink_metadata(&link)
                .expect("link metadata")
                .file_type()
                .is_symlink(),
            true,
            "the symlink itself must be left in place"
        );
        assert_eq!(
            std::fs::metadata(&target)
                .expect("target metadata")
                .permissions()
                .mode()
                & 0o777,
            0o755,
            "the target's mode must be untouched"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_regular_file_component_is_rejected() {
        let root = unique_test_root("parent-file");
        let blocker = root.join("blocker");
        std::fs::write(&blocker, "not a directory\n").expect("writing the blocking file");
        let deep = blocker.join("nested");
        let error =
            ensure_socket_parent(&deep).expect_err("a non-directory component must be rejected");
        assert!(
            error.to_string().contains("is not a directory"),
            "the error should name the non-directory component: {error:#}"
        );
        assert_eq!(
            std::fs::read_to_string(&blocker).expect("the blocking file should be readable"),
            "not a directory\n",
            "the blocking file must be untouched"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn spawn_refuses_a_symlinked_socket_parent() {
        let root = unique_test_root("parent-spawn-symlink");
        let target = root.join("target-dir");
        std::fs::create_dir(&target).expect("creating the symlink target");
        let link = root.join("link");
        std::os::unix::fs::symlink(&target, &link).expect("creating the symlink");
        let socket = link.join("leader.sock");
        let spawn = spawn_leader(LeaderServerConfig::new(socket.clone()), recording_factory());
        assert!(
            spawn.is_err(),
            "a spawn through a symlinked parent must be refused"
        );
        assert!(
            !target.join("leader.sock").exists(),
            "nothing may be published through the symlink"
        );
        assert!(
            !target.join("leader.lock").exists(),
            "no lock file may be created through the symlink"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn spawn_refuses_a_regular_file_socket_parent_component() {
        let root = unique_test_root("parent-spawn-file");
        let blocker = root.join("blocker");
        std::fs::write(&blocker, "not a directory\n").expect("writing the blocking file");
        let socket = blocker.join("leader.sock");
        let spawn = spawn_leader(LeaderServerConfig::new(socket.clone()), recording_factory());
        assert!(
            spawn.is_err(),
            "a spawn through a regular-file parent component must be refused"
        );
        assert_eq!(
            std::fs::read_to_string(&blocker).expect("the blocking file should be readable"),
            "not a directory\n",
            "the blocking file must be untouched"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    // -- publication ---------------------------------------------------------

    #[tokio::test]
    async fn over_limit_paths_still_bind_and_publish() {
        let root = unique_test_root("overlimit");
        let socket = long_parent_socket_path(&root, 200);
        assert!(
            socket.as_os_str().len() > 110,
            "the published path must exceed every sun_path limit (got {})",
            socket.as_os_str().len()
        );
        // Binding happens inside the short staging ancestor, so publication
        // never depends on the length of the published path.
        let listener = publish_listener(&socket)
            .expect("binding must not depend on the published path length");
        let metadata =
            std::fs::metadata(&socket).expect("the socket must be published at the long path");
        assert!(metadata.file_type().is_socket());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        drop(listener);
        std::fs::remove_file(&socket).expect("cleaning up the published socket");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn near_limit_long_filename_binds_and_connects() {
        let root = unique_test_root("longfile");
        let socket = long_filename_socket_path(&root, NEAR_LIMIT_PATH_BYTES);
        assert!(socket.as_os_str().len() <= NEAR_LIMIT_PATH_BYTES);
        let factory = recording_factory();
        let mut handle = spawn_leader(LeaderServerConfig::new(socket.clone()), factory.clone())
            .expect("the leader should spawn near the path-length limit");
        assert_socket_mode_0600(&socket);
        exercise_leader(&socket, &factory).await;
        handle.shutdown().await.expect("clean shutdown");
        assert_clean_stop(&handle);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn near_limit_long_parent_binds_and_connects() {
        let root = unique_test_root("longparent");
        let socket = long_parent_socket_path(&root, NEAR_LIMIT_PATH_BYTES);
        // The parent directories do not exist yet: publication must create
        // them, stage the bind in a short ancestor, and rename into place.
        let factory = recording_factory();
        let mut handle = spawn_leader(LeaderServerConfig::new(socket.clone()), factory.clone())
            .expect("the leader should spawn near the path-length limit");
        assert_socket_mode_0600(&socket);
        exercise_leader(&socket, &factory).await;
        handle.shutdown().await.expect("clean shutdown");
        assert_clean_stop(&handle);
        let _ = std::fs::remove_dir_all(&root);
    }

    // -- spawn lifetime, election, and cleanup -------------------------------

    #[tokio::test]
    async fn the_production_spawn_lifetime_publishes_serves_and_cleans_up() {
        let root = unique_test_root("lifetime");
        let socket = root.join("leader.sock");
        let lock_path = socket.with_extension("lock");
        let factory = recording_factory();
        let mut handle = spawn_leader(LeaderServerConfig::new(socket.clone()), factory.clone())
            .expect("the production leader should spawn");
        assert_eq!(handle.socket_path(), socket.as_path());
        assert_eq!(handle.lock_path(), lock_path.as_path());
        assert_socket_mode_0600(&socket);
        assert_eq!(
            std::fs::read_to_string(&lock_path)
                .expect("the lock file should be readable")
                .trim()
                .parse::<u32>()
                .expect("the lock file should hold the holder pid"),
            process::id(),
            "the lock file must record the holder pid"
        );
        assert_eq!(
            std::fs::metadata(&lock_path)
                .expect("lock metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600,
            "the lock file must be forced to 0600"
        );
        exercise_leader(&socket, &factory).await;
        handle.shutdown().await.expect("clean shutdown");
        assert_clean_stop(&handle);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_second_leader_fails_while_the_first_holds_the_lock() {
        let root = unique_test_root("exclusive");
        let socket = root.join("leader.sock");
        let lock_path = socket.with_extension("lock");
        let first_factory = recording_factory();
        let mut first = spawn_leader(
            LeaderServerConfig::new(socket.clone()),
            first_factory.clone(),
        )
        .expect("the first leader should spawn");

        let second = spawn_leader(LeaderServerConfig::new(socket.clone()), recording_factory());
        assert!(
            second.is_err(),
            "a second leader must fail while the first holds the lock"
        );
        assert!(
            socket.exists(),
            "the failed second leader must not remove the winner's socket"
        );
        assert!(
            lock_path.exists(),
            "the failed second leader must not remove the winner's lock file"
        );
        assert_eq!(
            std::fs::read_to_string(&lock_path)
                .expect("the winner's lock file should be readable")
                .trim()
                .parse::<u32>()
                .expect("the winner's lock file should hold a pid"),
            process::id(),
            "the failed leader must not overwrite the holder pid"
        );

        // The winner still serves traffic through the production path.
        exercise_leader(&socket, &first_factory).await;

        first.shutdown().await.expect("clean shutdown");
        assert_clean_stop(&first);

        // The lock is free again, so a new leader can take the socket.
        let mut second = spawn_leader(LeaderServerConfig::new(socket.clone()), recording_factory())
            .expect("a leader should spawn after the previous one stopped");
        second.shutdown().await.expect("clean shutdown");
        assert_clean_stop(&second);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_stale_lock_file_is_reclaimed_and_forced_to_0600() {
        let root = unique_test_root("stale");
        let socket = root.join("leader.sock");
        let lock_path = socket.with_extension("lock");
        std::fs::write(&lock_path, "999999\n").expect("writing a stale lock file");
        std::fs::set_permissions(&lock_path, Permissions::from_mode(0o644))
            .expect("loosening the stale lock mode");
        let mut handle = spawn_leader(LeaderServerConfig::new(socket.clone()), recording_factory())
            .expect("a stale lock file must be reclaimable");
        let metadata = std::fs::metadata(&lock_path).expect("the lock file should exist");
        assert_eq!(
            metadata.permissions().mode() & 0o777,
            0o600,
            "the reclaimed lock must be forced to 0600"
        );
        assert_eq!(
            std::fs::read_to_string(&lock_path)
                .expect("the lock file should be readable")
                .trim()
                .parse::<u32>()
                .expect("the lock file should hold a pid"),
            process::id()
        );
        handle.shutdown().await.expect("clean shutdown");
        assert_clean_stop(&handle);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn spawn_refuses_to_replace_a_non_socket_path() {
        let root = unique_test_root("occupied");
        let socket = root.join("leader.sock");
        std::fs::write(&socket, "not a socket\n").expect("writing a blocking file");
        let spawn = spawn_leader(LeaderServerConfig::new(socket.clone()), recording_factory());
        assert!(spawn.is_err(), "a non-socket path must be refused");
        assert_eq!(
            std::fs::read_to_string(&socket).expect("the blocking file should be untouched"),
            "not a socket\n"
        );
        let lock = socket.with_extension("lock");
        assert!(
            lock.exists(),
            "a failed spawn retains the stable lock inode"
        );
        let probe = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock)
            .expect("opening the released lock");
        probe
            .try_lock()
            .expect("a failed spawn must release its lock");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn dropping_the_handle_unlinks_the_socket_and_leaves_a_reclaimable_lock() {
        let root = unique_test_root("drop");
        let socket = root.join("leader.sock");
        let handle = spawn_leader(LeaderServerConfig::new(socket.clone()), recording_factory())
            .expect("the leader should spawn");
        assert!(socket.exists());
        drop(handle);
        assert!(
            !socket.exists(),
            "dropping the handle must unlink the published socket"
        );
        assert!(
            socket.with_extension("lock").exists(),
            "drop leaves the lock file for the next leader to reclaim"
        );
        let factory = recording_factory();
        let mut next = spawn_with_retry(&socket, factory.clone()).await;
        exercise_leader(&socket, &factory).await;
        next.shutdown().await.expect("clean shutdown");
        assert_clean_stop(&next);
        let _ = std::fs::remove_dir_all(&root);
    }

    // -- registration order and protocol handling ----------------------------

    #[tokio::test]
    async fn register_must_precede_registered_and_validate() {
        let root = unique_test_root("register");
        let socket = root.join("leader.sock");
        let mut handle = spawn_leader(LeaderServerConfig::new(socket.clone()), recording_factory())
            .expect("the leader should spawn");

        // ping before register is a protocol violation.
        {
            let (mut reader, mut writer) = connect(&socket).await;
            write_client_frame(&mut writer, &ClientEnvelope::Ping).await;
            match next_server_frame(&mut reader).await {
                Some(ServerEnvelope::Error { code, .. }) => {
                    assert_eq!(code, ENVELOPE_ERROR_INVALID_REQUEST)
                }
                other => panic!("expected an error envelope, got {other:?}"),
            }
            assert!(
                next_server_frame(&mut reader).await.is_none(),
                "the leader must close after a protocol violation"
            );
        }
        // acp before register is a protocol violation.
        {
            let (mut reader, mut writer) = connect(&socket).await;
            write_client_frame(
                &mut writer,
                &ClientEnvelope::Acp {
                    payload: json!({"jsonrpc": "2.0", "id": 1, "method": "initialize"}).to_string(),
                },
            )
            .await;
            match next_server_frame(&mut reader).await {
                Some(ServerEnvelope::Error { code, .. }) => {
                    assert_eq!(code, ENVELOPE_ERROR_INVALID_REQUEST)
                }
                other => panic!("expected an error envelope, got {other:?}"),
            }
            assert!(next_server_frame(&mut reader).await.is_none());
        }
        // an unknown register mode is rejected as an undecodable frame: with
        // the typed protocol the wire narrows to "stdio" | "headless".
        {
            let (mut reader, mut writer) = connect(&socket).await;
            write_client_frame(
                &mut writer,
                &ClientEnvelope::Register {
                    client_type: "grok-pager".to_string(),
                    mode: RegisterMode::Headless,
                    capabilities: test_capabilities(),
                },
            )
            .await;
            match next_server_frame(&mut reader).await {
                Some(ServerEnvelope::Registered { .. }) => {}
                other => panic!("expected registered, got {other:?}"),
            }
        }
        // a blank client_type is rejected.
        {
            let (mut reader, mut writer) = connect(&socket).await;
            write_client_frame(
                &mut writer,
                &ClientEnvelope::Register {
                    client_type: "   ".to_string(),
                    mode: RegisterMode::Stdio,
                    capabilities: test_capabilities(),
                },
            )
            .await;
            match next_server_frame(&mut reader).await {
                Some(ServerEnvelope::Error { code, .. }) => {
                    assert_eq!(code, ENVELOPE_ERROR_INVALID_REQUEST)
                }
                other => panic!("expected an error envelope, got {other:?}"),
            }
            assert!(next_server_frame(&mut reader).await.is_none());
        }
        // a valid register still works afterwards.
        {
            let (reader, writer, _client_id) = register(&socket).await;
            drop((reader, writer));
        }

        handle.shutdown().await.expect("clean shutdown");
        assert_clean_stop(&handle);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_second_register_is_rejected() {
        let root = unique_test_root("reregister");
        let socket = root.join("leader.sock");
        let mut handle = spawn_leader(LeaderServerConfig::new(socket.clone()), recording_factory())
            .expect("the leader should spawn");
        let (mut reader, mut writer, _client_id) = register(&socket).await;
        write_client_frame(&mut writer, &register_envelope()).await;
        match next_server_frame(&mut reader).await {
            Some(ServerEnvelope::Error { code, .. }) => {
                assert_eq!(code, ENVELOPE_ERROR_INVALID_REQUEST)
            }
            other => panic!("expected an error envelope, got {other:?}"),
        }
        assert!(
            next_server_frame(&mut reader).await.is_none(),
            "the leader must close after a second register"
        );
        handle.shutdown().await.expect("clean shutdown");
        assert_clean_stop(&handle);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn registration_captures_client_capabilities() {
        let root = unique_test_root("caps");
        let socket = root.join("leader.sock");
        let factory = recording_factory();
        let mut handle = spawn_leader(LeaderServerConfig::new(socket.clone()), factory.clone())
            .expect("the leader should spawn");
        let (mut reader, mut writer, _client_id) = register(&socket).await;
        // The factory runs before the server reads any further frame, so a
        // completed ping/pong proves the capabilities reached construction.
        write_client_frame(&mut writer, &ClientEnvelope::Ping).await;
        match next_server_frame(&mut reader).await {
            Some(ServerEnvelope::Pong) => {}
            other => panic!("expected pong, got {other:?}"),
        }
        let constructed = factory.constructed();
        let delegate = constructed
            .first()
            .expect("registration must have constructed a delegate");
        assert_eq!(
            delegate
                .capabilities
                .lock()
                .expect("capability log")
                .clone(),
            Some((true, false, false, Some("grok-pager-test".to_string()))),
            "yolo_mode/auto_mode/terminal and the client version must reach the \
             per-connection delegate at construction time"
        );
        handle.shutdown().await.expect("clean shutdown");
        assert_clean_stop(&handle);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn control_commands_answer_method_not_found() {
        let root = unique_test_root("control");
        let socket = root.join("leader.sock");
        let mut handle = spawn_leader(LeaderServerConfig::new(socket.clone()), recording_factory())
            .expect("the leader should spawn");
        let (mut reader, mut writer, _client_id) = register(&socket).await;
        write_client_frame(
            &mut writer,
            &ClientEnvelope::Control {
                request_id: "req-1".to_string(),
                command: json!({"type": "relaunch"}),
            },
        )
        .await;
        match next_server_frame(&mut reader).await {
            Some(ServerEnvelope::Error { code, message }) => {
                assert_eq!(code, ENVELOPE_ERROR_METHOD_NOT_FOUND);
                assert!(
                    message.contains("req-1"),
                    "the error should name the control request"
                );
            }
            other => panic!("expected an error envelope, got {other:?}"),
        }
        handle.shutdown().await.expect("clean shutdown");
        assert_clean_stop(&handle);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn deferred_acp_pushes_arrive_after_dispatch_returns() {
        let root = unique_test_root("deferred");
        let socket = root.join("leader.sock");
        let factory = Arc::new(RecordingFactory {
            late_push: true,
            ..Default::default()
        });
        let mut handle = spawn_leader(LeaderServerConfig::new(socket.clone()), factory.clone())
            .expect("the leader should spawn");
        let (mut reader, mut writer, _client_id) = register(&socket).await;
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "session/prompt",
            "params": {"sessionId": "s"},
        })
        .to_string();
        write_client_frame(
            &mut writer,
            &ClientEnvelope::Acp {
                payload: payload.clone(),
            },
        )
        .await;
        match next_server_frame(&mut reader).await {
            Some(ServerEnvelope::Acp { payload: echoed }) => assert_eq!(echoed, payload),
            other => panic!("expected an acp echo, got {other:?}"),
        }
        // The deferred push arrives after handle_acp returned, on the same
        // connection, through the cloned outbound handle.
        match next_server_frame(&mut reader).await {
            Some(ServerEnvelope::Acp { payload }) => {
                assert!(payload.contains("deferred"), "got {payload}");
            }
            other => panic!("expected the deferred push, got {other:?}"),
        }
        handle.shutdown().await.expect("clean shutdown");
        assert_clean_stop(&handle);
        let _ = std::fs::remove_dir_all(&root);
    }

    // -- per-connection delegate construction --------------------------------

    #[tokio::test]
    async fn two_registrations_receive_distinct_delegate_instances() {
        let root = unique_test_root("per-connection");
        let socket = root.join("leader.sock");
        let factory = recording_factory();
        let mut handle = spawn_leader(LeaderServerConfig::new(socket.clone()), factory.clone())
            .expect("the leader should spawn");

        let (reader_a, writer_a, client_id_a) = register(&socket).await;
        let (reader_b, writer_b, client_id_b) = register(&socket).await;
        assert_ne!(
            client_id_a, client_id_b,
            "two registrations must receive distinct client ids"
        );
        // Keep both connections open until the assertions below finish.
        let connections = (reader_a, writer_a, reader_b, writer_b);

        let delegates = factory.constructed();
        assert_eq!(
            delegates.len(),
            2,
            "each registered connection must construct exactly one delegate"
        );
        let instance_ids: Vec<u64> = delegates.iter().map(|d| d.instance_id).collect();
        assert_ne!(
            instance_ids[0], instance_ids[1],
            "two registrations must receive distinct delegate instances"
        );
        assert_eq!(
            factory.observed_client_ids(),
            vec![client_id_a, client_id_b],
            "the factory must see each connection's generated client id"
        );
        assert!(
            delegates[0].recorded_client_ids().contains(&client_id_a),
            "delegate A must carry client A's id"
        );
        assert!(
            delegates[1].recorded_client_ids().contains(&client_id_b),
            "delegate B must carry client B's id"
        );

        drop(connections);
        handle.shutdown().await.expect("clean shutdown");
        assert_clean_stop(&handle);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn failed_registration_constructs_zero_delegates() {
        let root = unique_test_root("failed-registration");
        let socket = root.join("leader.sock");
        let factory = recording_factory();
        let mut handle = spawn_leader(LeaderServerConfig::new(socket.clone()), factory.clone())
            .expect("the leader should spawn");

        // A blank client_type is rejected before any delegate is constructed.
        {
            let (mut reader, mut writer) = connect(&socket).await;
            write_client_frame(
                &mut writer,
                &ClientEnvelope::Register {
                    client_type: "   ".to_string(),
                    mode: RegisterMode::Stdio,
                    capabilities: test_capabilities(),
                },
            )
            .await;
            match next_server_frame(&mut reader).await {
                Some(ServerEnvelope::Error { code, .. }) => {
                    assert_eq!(code, ENVELOPE_ERROR_INVALID_REQUEST)
                }
                other => panic!("expected an error envelope, got {other:?}"),
            }
            assert!(next_server_frame(&mut reader).await.is_none());
        }
        // ping before register is also rejected with no delegate constructed.
        {
            let (mut reader, mut writer) = connect(&socket).await;
            write_client_frame(&mut writer, &ClientEnvelope::Ping).await;
            assert!(next_server_frame(&mut reader).await.is_some());
            assert!(next_server_frame(&mut reader).await.is_none());
        }

        assert_eq!(
            factory.constructed_count(),
            0,
            "failed registrations must construct zero delegates"
        );
        assert!(
            factory.observed_client_ids().is_empty(),
            "the factory must never be invoked for a failed registration"
        );

        // A valid registration still constructs exactly one afterwards.
        let (_reader, _writer, _client_id) = register(&socket).await;
        assert_eq!(factory.constructed_count(), 1);

        handle.shutdown().await.expect("clean shutdown");
        assert_clean_stop(&handle);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_failing_factory_closes_the_connection_and_constructs_no_delegate() {
        let root = unique_test_root("failing-factory");
        let socket = root.join("leader.sock");
        let factory = Arc::new(FailingFactory::default());
        let mut handle = spawn_leader(LeaderServerConfig::new(socket.clone()), factory.clone())
            .expect("the leader should spawn");

        // The register frame validates, but the factory refuses to construct
        // the delegate, so the leader closes the connection without ever
        // writing `registered`.
        {
            let (mut reader, mut writer) = connect(&socket).await;
            write_client_frame(&mut writer, &register_envelope()).await;
            assert!(
                next_server_frame(&mut reader).await.is_none(),
                "the leader must close the connection when the factory fails"
            );
        }
        assert_eq!(
            factory.attempts.load(Ordering::SeqCst),
            1,
            "the factory must have been invoked exactly once"
        );

        handle.shutdown().await.expect("clean shutdown");
        assert_clean_stop(&handle);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn disconnecting_b_notifies_only_b_and_a_still_serves() {
        let root = unique_test_root("isolation");
        let socket = root.join("leader.sock");
        let factory = recording_factory();
        let mut handle = spawn_leader(LeaderServerConfig::new(socket.clone()), factory.clone())
            .expect("the leader should spawn");

        // Client A registers and stays connected.
        let (mut reader_a, mut writer_a, _client_id_a) = register(&socket).await;
        // Client B registers and then disconnects.
        let (mut reader_b, mut writer_b, _client_id_b) = register(&socket).await;

        let delegates = factory.constructed();
        assert_eq!(delegates.len(), 2);
        let delegate_a = delegates[0].clone();
        let delegate_b = delegates[1].clone();

        // B disconnects; only B's delegate observes it.
        write_client_frame(&mut writer_b, &ClientEnvelope::Disconnect).await;
        assert!(
            next_server_frame(&mut reader_b).await.is_none(),
            "the leader should close connection B after its disconnect"
        );
        assert_eq!(
            delegate_b.disconnect_count(),
            1,
            "connection B's on_disconnect must run exactly once"
        );
        assert_eq!(
            delegate_a.disconnect_count(),
            0,
            "connection A's on_disconnect must not run for B's disconnect"
        );

        // A still handles a subsequent ping.
        write_client_frame(&mut writer_a, &ClientEnvelope::Ping).await;
        match next_server_frame(&mut reader_a).await {
            Some(ServerEnvelope::Pong) => {}
            other => panic!("expected pong for A after B disconnected, got {other:?}"),
        }
        // ...and a subsequent ACP dispatch, on A's own delegate.
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 11,
            "method": "initialize",
            "params": {},
        })
        .to_string();
        write_client_frame(
            &mut writer_a,
            &ClientEnvelope::Acp {
                payload: payload.clone(),
            },
        )
        .await;
        match next_server_frame(&mut reader_a).await {
            Some(ServerEnvelope::Acp { payload: echoed }) => assert_eq!(echoed, payload),
            other => panic!("expected an acp echo for A, got {other:?}"),
        }
        let payloads_b = delegate_b.payloads.lock().expect("payload log").clone();
        assert!(
            payloads_b.is_empty(),
            "B's delegate must never see A's ACP dispatch"
        );
        assert_eq!(
            delegate_a.disconnect_count(),
            0,
            "A's delegate must still be connected after serving A"
        );

        drop((reader_a, writer_a));
        handle.shutdown().await.expect("clean shutdown");
        assert_clean_stop(&handle);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn default_shutdown_waits_for_connection_on_disconnect_cleanup() {
        let root = unique_test_root("shutdown-disconnect-cleanup");
        let socket = root.join("leader.sock");
        let delegate = GatedDisconnectDelegate::new();
        let factory: Arc<dyn AcpDelegateFactory> = Arc::new({
            let delegate = delegate.clone();
            move |_client_id: u64, _registration: &Registration| {
                Ok(delegate.clone() as Arc<dyn AcpDelegate>)
            }
        });
        let handle = spawn_leader(LeaderServerConfig::new(socket.clone()), factory)
            .expect("the leader should spawn");
        let (_reader, _writer, _client_id) = register(&socket).await;

        let mut shutdown = tokio::spawn(async move {
            let mut handle = handle;
            let result = handle.shutdown().await;
            (handle, result)
        });
        timeout(TEST_TIMEOUT, delegate.started.notified())
            .await
            .expect("the connection should enter on_disconnect");
        assert!(
            timeout(Duration::from_millis(50), &mut shutdown)
                .await
                .is_err(),
            "LeaderHandle::shutdown must wait while on_disconnect is still running"
        );

        delegate.release.notify_one();
        let (handle, result) = timeout(TEST_TIMEOUT, shutdown)
            .await
            .expect("leader shutdown should finish after disconnect cleanup")
            .expect("shutdown task should join");
        result.expect("leader shutdown should succeed");
        assert!(
            delegate.completed.load(Ordering::SeqCst),
            "shutdown may return only after on_disconnect completed"
        );
        assert_clean_stop(&handle);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn shutdown_announces_the_stop_to_live_connections() {
        let root = unique_test_root("shutdown-frames");
        let socket = root.join("leader.sock");
        let mut handle = spawn_leader(LeaderServerConfig::new(socket.clone()), recording_factory())
            .expect("the leader should spawn");
        let (mut reader, _writer, _client_id) = register(&socket).await;
        handle.shutdown().await.expect("clean shutdown");
        match next_server_frame(&mut reader).await {
            Some(ServerEnvelope::ShuttingDown { reason, delay_ms }) => {
                assert_eq!(reason, ShutdownReason::Manual);
                assert_eq!(delay_ms, 0);
            }
            other => panic!("expected shutting_down, got {other:?}"),
        }
        match next_server_frame(&mut reader).await {
            Some(ServerEnvelope::Shutdown) => {}
            other => panic!("expected shutdown, got {other:?}"),
        }
        assert!(
            next_server_frame(&mut reader).await.is_none(),
            "the leader should close the connection after shutdown"
        );
        assert_clean_stop(&handle);
        let _ = std::fs::remove_dir_all(&root);
    }
}
