use anyhow::{Context, Result};
use clap::Parser;
use kube_shim::{app, config, db, tls};
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

    // Load TLS configuration
    let tls_config = tls::load_tls_config(&cfg.server.tls_cert_path, &cfg.server.tls_key_path)
        .await
        .context("Failed to load TLS configuration")?;

    let addr: SocketAddr = format!("{}:{}", cfg.server.host, cfg.server.port)
        .parse()
        .context("Invalid server host/port")?;

    tracing::info!("Server listening on https://{}", addr);

    axum_server::bind_rustls(addr, tls_config)
        .serve(router.into_make_service())
        .await
        .context("Server error")?;

    Ok(())
}
