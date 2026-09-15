//! Builds the authenticated `:6443` API router. Kept here (rather than
//! only in `main.rs`) so integration tests exercise the exact same router
//! construction the real binary uses, instead of a hand-rolled duplicate.

use crate::{api, auth, config};
use axum::{
    middleware,
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use tower_http::cors::CorsLayer;

pub fn build_router(pool: sqlx::SqlitePool, api_tokens: Arc<Vec<config::ApiToken>>) -> Router {
    Router::new()
        // Discovery endpoints
        .route("/api/v1", get(api::discovery_v1))
        .route("/apis/batch/v1", get(api::discovery_batch_v1))
        // Health check
        .route("/health", get(api::health))
        // Secrets API
        .route(
            "/api/v1/namespaces/:namespace/secrets",
            post(api::secret::create_secret).get(api::secret::list_secrets),
        )
        .route(
            "/api/v1/namespaces/:namespace/secrets/:name",
            get(api::secret::get_secret).delete(api::secret::delete_secret),
        )
        // CronJobs API
        .route(
            "/apis/batch/v1/namespaces/:namespace/cronjobs",
            post(api::cronjob::create_cronjob).get(api::cronjob::list_cronjobs),
        )
        .route(
            "/apis/batch/v1/namespaces/:namespace/cronjobs/:name",
            get(api::cronjob::get_cronjob).delete(api::cronjob::delete_cronjob),
        )
        .layer(CorsLayer::permissive())
        // Outermost layer: runs before CORS and before any handler, so an
        // unauthenticated request never reaches application logic at all.
        .layer(middleware::from_fn_with_state(
            api_tokens,
            auth::require_bearer_token,
        ))
        .with_state(pool)
}
