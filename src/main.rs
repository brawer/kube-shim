use anyhow::{Context, Result};
use clap::Parser;
use kube_shim::providers::upcloud::UpCloudProvider;
use kube_shim::providers::CloudProvider;
use kube_shim::reconcile::JobContext;
use kube_shim::{acme, api, app, config, db, metadata, pricing, reconcile, ssh, tls};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Notify;

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
    match upcloud.check_connectivity().await {
        Ok(()) => tracing::info!("UpCloud connectivity check succeeded"),
        Err(err) => tracing::warn!("UpCloud connectivity check failed (continuing anyway): {err}"),
    }

    // The shim's own current public IPv4 (Phase 9), used to build worker
    // VMs' inbound-SSH-allow firewall rule -- queried once here rather
    // than per-tick, since it can't change while this process is running.
    // Best-effort like the connectivity check above: `None` just means a
    // freshly created worker VM gets no inbound SSH access at all (see
    // src/metadata.rs's own docs), never a startup failure.
    let own_public_ip = metadata::own_public_ipv4().await;
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
        reconcile::pricing::sync_pricing_and_rates(&pool, &job_context).await?;
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

    tokio::spawn(reconcile::run(pool.clone(), notify.clone(), job_context));

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
    );

    let addr: SocketAddr = format!("{}:{}", cfg.server.host, cfg.server.port)
        .parse()
        .context("Invalid server host/port")?;

    match &cfg.server.hostname {
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
            let acceptor = acme::setup(&cfg.server).await?;

            tracing::info!(
                "Server listening on https://{} (ACME cert for {hostname})",
                addr
            );
            axum_server::bind(addr)
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
            let tls_config =
                tls::load_tls_config(&cfg.server.tls_cert_path, &cfg.server.tls_key_path)
                    .await
                    .context("Failed to load TLS configuration")?;

            tracing::info!("Server listening on https://{}", addr);
            axum_server::bind_rustls(addr, tls_config)
                .serve(router.into_make_service())
                .await
                .context("Server error")?;
        }
    }

    Ok(())
}
