use anyhow::{Context, Result};
use clap::Parser;
use kube_shim::providers::upcloud::UpCloudProvider;
use kube_shim::providers::CloudProvider;
use kube_shim::reconcile::JobContext;
use kube_shim::shutdown::{self, Shutdown, ShutdownTrigger};
use kube_shim::{acme, api, app, config, db, metadata, pricing, reconcile, ssh, tls, workload};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

/// How long `main` waits, after a shutdown signal, for the reconciliation
/// loops to reach a safe point (issue #118). A job step can make a cloud
/// API call that takes up to its 30s request timeout, so this is longer
/// than that. Must stay below `--stop-timeout` in deploy/kube-shim.container
/// so podman never has to resort to SIGKILL.
const RECONCILE_DRAIN_TIMEOUT: Duration = Duration::from_secs(45);

#[derive(Parser, Debug)]
#[command(name = "kube-shim")]
#[command(about = "Lightweight Kubernetes API server for ephemeral workloads", long_about = None)]
struct Args {
    #[arg(short, long, default_value = "config.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Installed first, before anything slow: kube-shim is PID 1 in its
    // container, where a signal without a handler does nothing at all.
    let mut signals = shutdown::Signals::install()?;
    let (trigger, shutdown) = shutdown::channel();
    let trigger = Arc::new(trigger);
    tokio::spawn({
        let trigger = trigger.clone();
        async move {
            let name = signals.recv().await;
            tracing::info!("received {name}, shutting down");
            trigger.trigger();
            let name = signals.recv().await;
            tracing::warn!("received {name} again, exiting immediately");
            std::process::exit(1);
        }
    });

    let args = Args::parse();
    tracing::info!("Loading configuration from: {}", args.config);

    // Load configuration
    let cfg = config::Config::from_file(&args.config)?;
    tracing::info!("Configuration loaded successfully");

    // Initialize database
    let pool = db::init_pool(&cfg.database.path).await?;
    tracing::info!("Database initialized at: {}", cfg.database.path);

    // Startup recovery (Phase 6): surface any job a previous crash left
    // mid-flight before the reconciliation loop starts polling normally.
    let recovered = reconcile::startup::recover(&pool).await?;
    tracing::info!("Startup recovery: {recovered} job(s) resumed");

    // Best-effort UpCloud connectivity/auth check (Phase 7) -- logged as a
    // warning on failure, never fatal. The already-deployed
    // kube-shim.brawer.ch config still has upcloud.token = "REPLACE_ME";
    // nothing in this phase actually depends on UpCloud working yet (real
    // operations don't happen until Phase 8), so a hard failure here would
    // crash-loop that instance on its next auto-update for no operational
    // reason.
    let upcloud = Arc::new(UpCloudProvider::new(cfg.upcloud.token.clone()));
    // Grabbed before `upcloud` moves into `job_context` below -- these
    // feed `CostReportConfig` (Phase 14a) so the cost report's own
    // provider-name fields come from the actual provider
    // implementation, not a hardcoded literal.
    let provider_name = upcloud.provider_name().to_string();
    let invoice_issuer_name = upcloud.invoice_issuer_name().to_string();
    let Some(connectivity) = unless_shutdown(&shutdown, upcloud.check_connectivity()).await else {
        return Ok(());
    };
    match connectivity {
        Ok(()) => tracing::info!("UpCloud connectivity check succeeded"),
        Err(err) => tracing::warn!("UpCloud connectivity check failed (continuing anyway): {err}"),
    }

    // The shim's own current public IPv4 (Phase 9), used to build worker
    // VMs' inbound-SSH-allow firewall rule -- queried once here rather
    // than per-tick, since it can't change while this process is running.
    // Best-effort like the connectivity check above: `None` just means a
    // freshly created worker VM gets no inbound SSH access at all (see
    // src/metadata.rs's own docs), never a startup failure.
    let Some(own_public_ip) = unless_shutdown(&shutdown, metadata::own_public_ipv4()).await else {
        return Ok(());
    };
    match &own_public_ip {
        Some(ip) => tracing::info!("shim's own public IPv4: {ip}"),
        None => tracing::warn!(
            "could not determine the shim's own public IPv4 -- worker VMs will have no inbound \
             SSH access until this is resolved"
        ),
    }

    // Reconciliation loop (Phase 6), running in the background for the
    // lifetime of the process. `notify` is also handed to the API router
    // below so a new CronJob can wake it immediately instead of waiting
    // for the fallback poll.
    let notify = Arc::new(Notify::new());
    let job_context = JobContext {
        provider: upcloud,
        dry_run: cfg.upcloud.dry_run,
        resource_prefix: cfg.shim.resource_prefix.clone(),
        zone: cfg.upcloud.zone.clone(),
        worker_template_uuid: cfg.upcloud.worker_template_uuid.clone(),
        worker_ssh_public_keys: cfg.upcloud.worker_ssh_public_keys.clone(),
        own_public_ip,
        worker_ssh_private_key: cfg.upcloud.worker_ssh_private_key.clone(),
        worker_ssh_port: ssh::SSH_PORT,
        main_currency: cfg.shim.main_currency.clone(),
        plan_filter: workload::PlanFilter {
            allow_prefixes: cfg.upcloud.worker_plan_allow_prefixes.clone(),
            deny_prefixes: cfg.upcloud.worker_plan_deny_prefixes.clone(),
        },
    };
    // Phase 14a: if this is genuinely the first boot ever against this
    // database (neither UpCloud pricing nor ECB exchange rates cached
    // yet), sync both once, blocking, before the server starts accepting
    // any requests below -- otherwise a job could be admitted and reach
    // `Created` before `reconcile::run`'s own background sync loop has
    // had a chance to run even once, leaving its cost permanently
    // unknowable (cost calculation never hits either API live per job).
    // On every later restart the tables already have rows in them from
    // a previous sync, so this is skipped and startup stays immediate --
    // the background loop's own eager first tick still refreshes both
    // right away, just non-blocking.
    if !reconcile::pricing::has_cached_pricing(&pool).await? {
        tracing::info!(
            "No cached pricing/exchange rates yet (first boot) -- syncing once before starting"
        );
        let synced = unless_shutdown(
            &shutdown,
            reconcile::pricing::sync_pricing_and_rates(&pool, &job_context),
        )
        .await;
        let Some(synced) = synced else {
            return Ok(());
        };
        synced?;
    }

    // Phase 14b: seeds the rolling budget guard's DB-backed settings
    // from config.toml's [shim] fields, but only on a genuine first
    // boot -- a no-op on every later restart, since PATCH /settings
    // may have since changed these live and config.toml is never
    // re-read for this (see pricing::ensure_budget_seeded's own docs).
    pricing::ensure_budget_seeded(
        &pool,
        &pricing::BudgetSettings {
            main_currency: cfg.shim.main_currency.clone(),
            budget_daily_rate: cfg.shim.budget_daily_rate,
            budget_rollover_cap_days: cfg.shim.budget_rollover_cap_days,
        },
    )
    .await?;

    // Shared with the public `/readyz` (Phase 15) so it can report a
    // real signal -- whether this loop is actually still ticking --
    // rather than a hardcoded 200.
    let tick_health = Arc::new(reconcile::TickHealth::new());
    let reconcile_loops = tokio::spawn(reconcile::run(
        pool.clone(),
        notify.clone(),
        job_context,
        tick_health.clone(),
        shutdown.clone(),
    ));

    // Build router. The worker SSH private key is handed in separately
    // (not via JobContext, which is reconcile-loop-only) -- `api::logs`
    // (Phase 10) needs it too, for live log streaming/one-shot fetches
    // while a job is still running.
    let worker_ssh = Arc::new(ssh::WorkerSshConfig {
        private_key: cfg.upcloud.worker_ssh_private_key.clone(),
        port: ssh::SSH_PORT,
    });
    let cost_report_config = Arc::new(api::cost_report::CostReportConfig {
        resource_prefix: cfg.shim.resource_prefix.clone(),
        zone: cfg.upcloud.zone.clone(),
        main_currency: cfg.shim.main_currency.clone(),
        provider_name,
        invoice_issuer_name,
    });
    let router = app::build_router(
        pool,
        Arc::new(cfg.server.api_tokens.clone()),
        notify,
        worker_ssh,
        cost_report_config,
        tick_health,
    );

    let served = serve(&cfg.server, router, shutdown).await;
    if let Err(err) = &served {
        tracing::error!("HTTP server stopped: {err:?}");
    }
    finish(trigger, reconcile_loops).await;
    served
}

/// Runs a startup step, giving up early if shutdown is requested first.
/// Only used for steps that change no cloud resources (connectivity
/// checks, the first pricing sync, waiting for a certificate), so
/// abandoning one halfway is safe.
async fn unless_shutdown<T>(shutdown: &Shutdown, step: impl Future<Output = T>) -> Option<T> {
    tokio::select! {
        biased;
        _ = shutdown.requested() => {
            tracing::info!("shutdown requested during startup; exiting");
            None
        }
        value = step => Some(value),
    }
}

/// Stops the reconciliation loops (if they aren't stopping already) and
/// waits up to `RECONCILE_DRAIN_TIMEOUT` for them to reach a safe point.
async fn finish(trigger: Arc<ShutdownTrigger>, reconcile_loops: tokio::task::JoinHandle<()>) {
    trigger.trigger();
    match tokio::time::timeout(RECONCILE_DRAIN_TIMEOUT, reconcile_loops).await {
        Ok(Ok(())) => tracing::info!("shutdown complete"),
        Ok(Err(err)) => tracing::error!("reconciliation loop ended abnormally: {err}"),
        Err(_) => tracing::warn!(
            "reconciliation loops still busy after {RECONCILE_DRAIN_TIMEOUT:?}; exiting anyway"
        ),
    }
}

/// Serves the API until `shutdown` is requested, then lets in-flight
/// requests finish (see `shutdown::HTTP_DRAIN_TIMEOUT`) and returns.
async fn serve(cfg: &config::ServerConfig, router: axum::Router, shutdown: Shutdown) -> Result<()> {
    let addr: SocketAddr = format!("{}:{}", cfg.host, cfg.port)
        .parse()
        .context("Invalid server host/port")?;

    let handle = axum_server::Handle::new();
    shutdown::graceful_on_shutdown(handle.clone(), shutdown.clone());

    match &cfg.hostname {
        Some(hostname) => {
            // Real, CA-trusted certificate via ACME (Let's Encrypt),
            // Caddy-like -- see docs/IMPLEMENTATION_PLAN.md Phase 4. Blocks
            // here until a certificate is actually available (fresh or
            // cached); a cold-start failure returns an error and this
            // process exits non-zero rather than serving broken TLS.
            tracing::info!("ACME enabled for hostname {hostname}; obtaining certificate...");
            // acme::setup() starts the :80 HTTP-01 challenge responder
            // itself (as a background task) before waiting for the first
            // certificate -- issuance depends on that responder already
            // being reachable, so starting it only *after* setup()
            // returned would deadlock.
            let Some(acceptor) =
                unless_shutdown(&shutdown, acme::setup(cfg, shutdown.clone())).await
            else {
                return Ok(());
            };
            let acceptor = acceptor?;

            tracing::info!(
                "Server listening on https://{} (ACME cert for {hostname})",
                addr
            );
            axum_server::bind(addr)
                .handle(handle)
                .acceptor(acceptor)
                .serve(router.into_make_service())
                .await
                .context("Server error")?;
        }
        None => {
            // No public hostname configured -- local dev/CI fallback:
            // the same self-signed certificate Phase 1 always used.
            tracing::info!(
                "No server.hostname configured; using self-signed certificate (local dev/CI)"
            );
            let tls_config = tls::load_tls_config(&cfg.tls_cert_path, &cfg.tls_key_path)
                .await
                .context("Failed to load TLS configuration")?;

            tracing::info!("Server listening on https://{}", addr);
            axum_server::bind_rustls(addr, tls_config)
                .handle(handle)
                .serve(router.into_make_service())
                .await
                .context("Server error")?;
        }
    }

    Ok(())
}
