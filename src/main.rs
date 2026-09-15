use kube_shim::{api, config, db};
use anyhow::{Context, Result};
use axum::{
    routing::{get, post},
    Router,
};
use clap::Parser;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tracing_subscriber;

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

    // Bind and listen
    let listener = TcpListener::bind(format!("{}:{}", cfg.server.host, cfg.server.port))
        .await
        .context("Failed to bind to address")?;

    tracing::info!(
        "Server listening on http://{}:{} (Note: Phase 2 will add TLS support)",
        cfg.server.host,
        cfg.server.port
    );

    axum::serve(listener, app)
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
