use anyhow::{Context, Result};
use clap::Parser;
use kube_shim::{acme, app, config, db, tls};
use std::net::SocketAddr;
use std::sync::Arc;

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

    // Build router
    let router = app::build_router(pool, Arc::new(cfg.server.api_tokens.clone()));

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
