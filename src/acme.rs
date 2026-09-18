//! ACME (Let's Encrypt) certificate issuance and renewal (Phase 4).
//!
//! Kept separate from `tls.rs`, which still owns the self-signed fallback
//! path used whenever `server.hostname` is unset (local dev, CI, or any
//! deployment that hasn't configured a real public hostname).

use crate::app::server_header_layer;
use crate::config::ServerConfig;
use anyhow::{bail, Context, Result};
use axum::Router;
use rustls_acme::axum::AxumAcceptor;
use rustls_acme::{caches::DirCache, AcmeConfig, AcmeState, EventOk, UseChallenge};
use std::fmt::Debug;
use std::time::Duration;
use tokio_stream::StreamExt;

const DIRECTORY_STAGING: &str = "staging";
const DIRECTORY_PRODUCTION: &str = "production";

/// How long `setup()` will keep retrying to obtain the very first
/// certificate before giving up and returning an error -- a cold-start
/// failure needs to be loud (the process refuses to start), per
/// docs/IMPLEMENTATION_PLAN.md Phase 4, rather than silently serving
/// broken or absent TLS. Renewal of an already-deployed certificate is
/// *not* subject to this timeout -- see `spawn_renewal_task` below, which
/// only ever logs.
const COLD_START_TIMEOUT: Duration = Duration::from_secs(5 * 60);

pub struct Acme {
    /// Hands to `axum_server::Server::acceptor()` for the `:443`-published
    /// listener; resolves whatever certificate ACME has most recently
    /// deployed.
    pub acceptor: AxumAcceptor,
    /// The minimal, GET-only-in-spirit router that answers Let's Encrypt's
    /// HTTP-01 challenge on `server.acme_challenge_port` -- nothing else is
    /// ever registered on it.
    pub challenge_router: Router,
}

/// Builds the ACME-issued TLS acceptor and HTTP-01 challenge router,
/// blocking until a certificate is actually available to serve -- either
/// freshly issued, or a still-valid one loaded from `acme_cache_dir` -- or
/// failing with an error if that doesn't happen within
/// `COLD_START_TIMEOUT`. Once that first certificate is in hand, ongoing
/// renewal is handed off to a background task that only logs.
///
/// Must only be called when `cfg.hostname` is `Some`; the self-signed
/// fallback in `tls.rs` is what handles the `None` case.
pub async fn setup(cfg: &ServerConfig) -> Result<Acme> {
    let hostname = cfg
        .hostname
        .as_ref()
        .expect("acme::setup() requires server.hostname to be set")
        .clone();

    std::fs::create_dir_all(&cfg.acme_cache_dir).with_context(|| {
        format!(
            "Failed to create ACME cache directory at {}",
            cfg.acme_cache_dir
        )
    })?;

    let mut acme_config = AcmeConfig::new([hostname.as_str()])
        .cache(DirCache::new(cfg.acme_cache_dir.clone()))
        .challenge_type(UseChallenge::Http01);

    acme_config = match cfg.acme_directory.to_lowercase().as_str() {
        DIRECTORY_STAGING => acme_config.directory_lets_encrypt(false),
        DIRECTORY_PRODUCTION => acme_config.directory_lets_encrypt(true),
        custom_directory_url => acme_config.directory(custom_directory_url),
    };

    if let Some(email) = &cfg.acme_contact_email {
        acme_config = acme_config.contact_push(format!("mailto:{email}"));
    }

    let mut state = acme_config.state();
    let acceptor = state.axum_acceptor(state.default_rustls_config());
    let challenge_service = state.http01_challenge_tower_service();

    let challenge_router = Router::new()
        .route_service("/.well-known/acme-challenge/:token", challenge_service)
        .layer(server_header_layer());

    tokio::time::timeout(COLD_START_TIMEOUT, wait_for_first_certificate(&mut state))
        .await
        .with_context(|| {
            format!(
                "Failed to obtain an ACME certificate for {hostname} within {COLD_START_TIMEOUT:?} \
                 -- check that DNS for {hostname} already resolves to this host, and that port {} \
                 is reachable from the internet for the HTTP-01 challenge",
                cfg.acme_challenge_port
            )
        })??;

    spawn_renewal_task(state, hostname);

    Ok(Acme {
        acceptor,
        challenge_router,
    })
}

/// Drives the ACME state machine's event stream until the first
/// certificate is deployed (freshly issued, or loaded from cache), logging
/// every event along the way. `.next()` already waits on the crate's own
/// internal retry/backoff timers between attempts -- this loop adds a
/// wall-clock ceiling via its caller's `tokio::time::timeout`, it doesn't
/// reimplement backoff itself.
async fn wait_for_first_certificate<EC, EA>(state: &mut AcmeState<EC, EA>) -> Result<()>
where
    EC: Debug + 'static,
    EA: Debug + 'static,
{
    loop {
        match state.next().await {
            Some(Ok(EventOk::DeployedNewCert)) => {
                tracing::info!("ACME: obtained a new certificate");
                return Ok(());
            }
            Some(Ok(EventOk::DeployedCachedCert)) => {
                tracing::info!("ACME: loaded a still-valid cached certificate");
                return Ok(());
            }
            Some(Ok(event)) => {
                tracing::debug!("ACME: {event:?}");
            }
            Some(Err(err)) => {
                tracing::warn!("ACME: attempt failed, retrying: {err:?}");
            }
            None => {
                bail!("ACME event stream ended unexpectedly before a certificate was obtained");
            }
        }
    }
}

/// Keeps driving the ACME state machine forever in the background, so
/// certificates get renewed automatically ahead of expiry. A renewal
/// failure is only ever logged, never fatal: the certificate already
/// deployed (and referenced by `Acme::acceptor`'s resolver) keeps serving
/// traffic regardless, and rustls-acme retries renewal on its own
/// schedule. This is what makes a transient Let's Encrypt outage or
/// rate-limit bump a non-event instead of a self-inflicted outage.
fn spawn_renewal_task<EC, EA>(mut state: AcmeState<EC, EA>, hostname: String)
where
    EC: Debug + Send + 'static,
    EA: Debug + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            match state.next().await {
                Some(Ok(event)) => tracing::info!("ACME ({hostname}): {event:?}"),
                Some(Err(err)) => tracing::warn!("ACME ({hostname}): {err:?}"),
                None => {
                    tracing::error!(
                        "ACME ({hostname}): event stream ended -- no further renewal attempts \
                         will happen for the rest of this process's lifetime"
                    );
                    return;
                }
            }
        }
    });
}
