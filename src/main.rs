use anyhow::{Context, Result};
use clap::Parser;
use kube_shim::providers::upcloud::UpCloudProvider;
use kube_shim::{acme, app, config, db, reconcile, tls};
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
    let upcloud = UpCloudProvider::new(cfg.upcloud.token.clone());
    match upcloud.check_connectivity().await {
        Ok(()) => tracing::info!("UpCloud connectivity check succeeded"),
        Err(err) => tracing::warn!("UpCloud connectivity check failed (continuing anyway): {err}"),
    }

    // Reconciliation loop (Phase 6), running in the background for the
    // lifetime of the process. `notify` is also handed to the API router
    // below so a new CronJob can wake it immediately instead of waiting
    // for the fallback poll.
    let notify = Arc::new(Notify::new());
    tokio::spawn(reconcile::run(
        pool.clone(),
        notify.clone(),
        cfg.upcloud.dry_run,
    ));

    // Build router
    let router = app::build_router(pool, Arc::new(cfg.server.api_tokens.clone()), notify);

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
