//! Builds the authenticated `:6443` API router. Kept here (rather than
//! only in `main.rs`) so integration tests exercise the exact same router
//! construction the real binary uses, instead of a hand-rolled duplicate.

use crate::{api, auth, config};
use axum::{
    http::{HeaderName, HeaderValue},
    middleware,
    routing::{get, post},
    Extension, Router,
};
use std::sync::Arc;
use tokio::sync::Notify;
use tower_http::{cors::CorsLayer, set_header::SetResponseHeaderLayer};

/// A `Server: kube-shim/x.y.z` header, stamped from `Cargo.toml`'s own
/// package version at compile time -- so it's always possible to tell
/// which release is actually live on a given deployment (e.g.
/// kube-shim.brawer.ch) with nothing more than `curl -I`. Shared between
/// every router the binary serves (the authenticated API here, and the
/// ACME HTTP-01 challenge router, Phase 4), not just this one, so it's
/// factored out rather than inlined into `build_router` alone.
pub fn server_header_layer() -> SetResponseHeaderLayer<HeaderValue> {
    SetResponseHeaderLayer::overriding(
        HeaderName::from_static("server"),
        HeaderValue::from_static(concat!("kube-shim/", env!("CARGO_PKG_VERSION"))),
    )
}

/// `notify`: shared with the reconciliation loop (Phase 6) via
/// `Extension`, not axum's `State` -- only `create_cronjob` needs it (to
/// wake the loop immediately on a new `CronJob` instead of waiting up to
/// `reconcile::FALLBACK_INTERVAL`), and `Extension` lets it reach just
/// that one handler without changing every other handler's `State<...>`
/// extractor.
pub fn build_router(
    pool: sqlx::SqlitePool,
    api_tokens: Arc<Vec<config::ApiToken>>,
    notify: Arc<Notify>,
) -> Router {
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
        .layer(Extension(notify))
        .layer(CorsLayer::permissive())
        // Runs before CORS and before any handler, so an unauthenticated
        // request never reaches application logic at all.
        .layer(middleware::from_fn_with_state(
            api_tokens,
            auth::require_bearer_token,
        ))
        // Truly outermost: applied to every response, auth failures
        // included -- which version answered a request isn't sensitive,
        // and is exactly what you want to see on a 401 while debugging a
        // stale deployment.
        .layer(server_header_layer())
        .with_state(pool)
}
