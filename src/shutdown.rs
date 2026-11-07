//! Graceful shutdown on SIGTERM/SIGINT (issue #118).
//!
//! In the container kube-shim runs as PID 1, and the kernel never applies
//! a default signal action to PID 1. Without a handler, SIGTERM does
//! nothing and podman resorts to SIGKILL after its stop timeout. So
//! `main.rs` installs a handler right at startup and turns the first
//! signal into a [`Shutdown`] request that every long-running part of the
//! process watches: the HTTP listeners drain their connections, and each
//! reconciliation loop stops at its next safe point instead of in the
//! middle of a cloud API call.

use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::signal::unix::{signal, Signal, SignalKind};
use tokio::sync::watch;

/// Fires a [`Shutdown`] request. Held by whoever listens for signals.
pub struct ShutdownTrigger {
    tx: watch::Sender<bool>,
}

impl ShutdownTrigger {
    /// Asks everything watching the matching [`Shutdown`] to stop.
    /// Calling it more than once is harmless.
    pub fn trigger(&self) {
        self.tx.send_replace(true);
    }
}

/// A cheap, clonable view of whether shutdown has been requested.
#[derive(Clone)]
pub struct Shutdown {
    rx: watch::Receiver<bool>,
}

impl Shutdown {
    /// A `Shutdown` that is never requested, for callers (mostly tests)
    /// that just want a loop to run forever.
    pub fn never() -> Self {
        let (_trigger, shutdown) = channel();
        shutdown
    }

    /// Whether shutdown has been requested yet. Used at safe points
    /// inside a pass, such as between two jobs.
    pub fn is_requested(&self) -> bool {
        *self.rx.borrow()
    }

    /// Resolves once shutdown is requested. Never resolves if the
    /// trigger was dropped without firing.
    pub async fn requested(&self) {
        let mut rx = self.rx.clone();
        if rx.wait_for(|requested| *requested).await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// Creates a linked trigger and shutdown view.
pub fn channel() -> (ShutdownTrigger, Shutdown) {
    let (tx, rx) = watch::channel(false);
    (ShutdownTrigger { tx }, Shutdown { rx })
}

/// How long an HTTP listener lets in-flight requests finish after
/// shutdown is requested before it closes the remaining connections.
/// Long enough for a normal API call; a `kubectl logs -f` stream is cut
/// off at this point.
pub const HTTP_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Starts a graceful shutdown of the server behind `handle` once
/// `shutdown` is requested, allowing [`HTTP_DRAIN_TIMEOUT`] for open
/// connections. Safe to call before the server starts listening.
pub fn graceful_on_shutdown(handle: axum_server::Handle<SocketAddr>, shutdown: Shutdown) {
    tokio::spawn(async move {
        shutdown.requested().await;
        handle.graceful_shutdown(Some(HTTP_DRAIN_TIMEOUT));
    });
}

/// The process's SIGTERM and SIGINT handlers. Creating this installs
/// them immediately, so it should happen as early as possible in `main`.
pub struct Signals {
    terminate: Signal,
    interrupt: Signal,
}

impl Signals {
    pub fn install() -> Result<Self> {
        Ok(Self {
            terminate: signal(SignalKind::terminate()).context("installing SIGTERM handler")?,
            interrupt: signal(SignalKind::interrupt()).context("installing SIGINT handler")?,
        })
    }

    /// Waits for the next SIGTERM or SIGINT and returns its name.
    pub async fn recv(&mut self) -> &'static str {
        tokio::select! {
            _ = self.terminate.recv() => "SIGTERM",
            _ = self.interrupt.recv() => "SIGINT",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_trigger_wakes_every_clone() {
        let (trigger, shutdown) = channel();
        let other = shutdown.clone();
        assert!(!shutdown.is_requested());

        trigger.trigger();

        assert!(shutdown.is_requested());
        assert!(other.is_requested());
        tokio::time::timeout(Duration::from_secs(1), other.requested())
            .await
            .expect("requested() should resolve once triggered");
    }

    #[tokio::test]
    async fn test_never_is_never_requested() {
        let shutdown = Shutdown::never();
        assert!(!shutdown.is_requested());
        let waited = tokio::time::timeout(Duration::from_millis(50), shutdown.requested()).await;
        assert!(waited.is_err(), "Shutdown::never() must not resolve");
    }
}
