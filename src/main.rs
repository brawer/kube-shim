use kube_shim::{api, config, db, tls};
use anyhow::{Context, Result};
use axum::{
    routing::{get, post},
    Router,
};
use clap::Parser;
use std::net::SocketAddr;
use tower_http::cors::CorsLayer;

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
    let app = build_router(pool);

    // Load TLS configuration
    let tls_config = tls::load_tls_config(&cfg.server.tls_cert_path, &cfg.server.tls_key_path)
        .await
        .context("Failed to load TLS configuration")?;

    let addr: SocketAddr = format!("{}:{}", cfg.server.host, cfg.server.port)
        .parse()
        .context("Invalid server host/port")?;

    tracing::info!("Server listening on https://{}", addr);

    axum_server::bind_rustls(addr, tls_config)
        .serve(app.into_make_service())
        .await
        .context("Server error")?;

    Ok(())
}

fn build_router(pool: sqlx::SqlitePool) -> Router {
    Router::new()
        // Discovery endpoints
        .route("/api/v1", get(api::discovery_v1))
        .route("/apis/batch/v1", get(api::discovery_batch_v1))

        // Health check
        .route("/health", get(api::health))

        // Secrets API
        .route(
            "/api/v1/namespaces/:namespace/secrets",
            post(api::secret::create_secret)
                .get(api::secret::list_secrets),
        )
        .route(
            "/api/v1/namespaces/:namespace/secrets/:name",
            get(api::secret::get_secret)
                .delete(api::secret::delete_secret),
        )

        // CronJobs API
        .route(
            "/apis/batch/v1/namespaces/:namespace/cronjobs",
            post(api::cronjob::create_cronjob)
                .get(api::cronjob::list_cronjobs),
        )
        .route(
            "/apis/batch/v1/namespaces/:namespace/cronjobs/:name",
            get(api::cronjob::get_cronjob)
                .delete(api::cronjob::delete_cronjob),
        )

        .layer(CorsLayer::permissive())
        .with_state(pool)
}
