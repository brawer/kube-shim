//! Builds the `:443` app: the authenticated API sub-router (Phase 2's
//! bearer-token middleware, CORS) merged with the public, unauthenticated
//! status-page sub-router (Phase 15) -- two separately-constructed
//! `Router`s, not one router with routes individually opted out of auth,
//! so a bug in route registration can't accidentally leave a new
//! authenticated-API route unprotected or a new public route behind a
//! token real uptime monitors/phone browsers won't have. Kept here
//! (rather than only in `main.rs`) so integration tests exercise the
//! exact same router construction the real binary uses, instead of a
//! hand-rolled duplicate.

use crate::reconcile::TickHealth;
use crate::{api, auth, config, ssh, status_page};
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
/// extractor. `worker_ssh` (Phase 10) is the same pattern, for
/// `api::logs`'s own need to SSH into a worker VM directly, independent
/// of the reconciliation loop's own `JobContext`. `cost_report_config`
/// (Phase 14a) is the same pattern again, for `api::cost_report`'s own
/// need for `resource_prefix`/`zone`/`main_currency`.
#[allow(clippy::too_many_arguments)]
fn authenticated_router(
    pool: sqlx::SqlitePool,
    api_tokens: Arc<Vec<config::ApiToken>>,
    notify: Arc<Notify>,
    worker_ssh: Arc<ssh::WorkerSshConfig>,
    cost_report_config: Arc<api::cost_report::CostReportConfig>,
) -> Router {
    Router::new()
        // Discovery endpoints
        .route("/api", get(api::discovery_root))
        .route("/apis", get(api::discovery_apis_root))
        .route("/api/v1", get(api::discovery_v1))
        .route("/apis/batch/v1", get(api::discovery_batch_v1))
        .route(
            "/apis/metrics.k8s.io/v1beta1",
            get(api::discovery_metrics_v1beta1),
        )
        .route(
            "/apis/cost.kube-shim.brawer.ch/v1",
            get(api::discovery_cost_v1),
        )
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
        // Pods (Phase 12's minimal synthetic GET; Phase 10's /log
        // subresource) -- one pod per job run, so "pod name" is a job's
        // own `name` directly.
        .route(
            "/api/v1/namespaces/:namespace/pods/:name",
            get(api::pods::get_pod),
        )
        .route(
            "/api/v1/namespaces/:namespace/pods/:name/log",
            get(api::logs::get_pod_log),
        )
        // Events (Phase 12) -- what `kubectl describe pod` reads.
        .route(
            "/api/v1/namespaces/:namespace/events",
            get(api::events::list_events),
        )
        // Nodes (Phase 12 follow-up) -- one per currently-allocated
        // worker VM; cluster-scoped, so no :namespace segment. Needed
        // for `kubectl top nodes` to have a real object to correlate
        // against the metrics.k8s.io data below (see api::nodes's own
        // docs).
        .route("/api/v1/nodes", get(api::nodes::list_nodes))
        // Metrics (Phase 12) -- what `kubectl top nodes`/`kubectl top
        // pods` read. "nodes" is cluster-scoped (no namespace); "pods"
        // has both an all-namespaces form (`kubectl top pods -A`) and a
        // namespaced one (plain `kubectl top pods`), matching real
        // metrics-server's own dual-route convention.
        .route(
            "/apis/metrics.k8s.io/v1beta1/nodes",
            get(api::metrics::list_node_metrics),
        )
        .route(
            "/apis/metrics.k8s.io/v1beta1/pods",
            get(api::metrics::list_pod_metrics),
        )
        .route(
            "/apis/metrics.k8s.io/v1beta1/namespaces/:namespace/pods",
            get(api::metrics::list_pod_metrics_for_namespace),
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
        // Standalone Jobs API (Phase 13) -- a job run that exists
        // directly, not only ever spawned indirectly by a CronJob.
        .route(
            "/apis/batch/v1/namespaces/:namespace/jobs",
            post(api::job::create_job).get(api::job::list_jobs),
        )
        .route(
            "/apis/batch/v1/namespaces/:namespace/jobs/:name",
            get(api::job::get_job).delete(api::job::delete_job),
        )
        // Cost report (Phase 14a) -- cluster-scoped, same as `/api/v1/nodes`.
        .route(
            "/apis/cost.kube-shim.brawer.ch/v1/report",
            get(api::cost_report::get_cost_report),
        )
        // Rolling budget guard (Phase 14b) -- `notify` (already layered
        // below for `create_cronjob`) wakes the reconciliation loop
        // immediately on either call. The budget guard rejects at
        // admission time now, not via a job sitting around waiting
        // (see `pricing::check_budget`'s own docs), so this specifically
        // speeds up a CronJob-spawned run that was skipped for
        // insufficient budget -- it gets retried right away instead of
        // waiting up to the fallback tick.
        .route(
            "/apis/cost.kube-shim.brawer.ch/v1/settings",
            get(api::budget::get_settings).patch(api::budget::patch_settings),
        )
        .route(
            "/apis/cost.kube-shim.brawer.ch/v1/budget/topup",
            post(api::budget::topup),
        )
        .layer(Extension(notify))
        .layer(Extension(worker_ssh))
        .layer(Extension(cost_report_config))
        .layer(CorsLayer::permissive())
        // Runs before CORS and before any handler, so an unauthenticated
        // request never reaches application logic at all. Scoped to
        // just this sub-router -- the public status page/health
        // endpoints below are merged in separately, outside this
        // middleware's reach entirely, not exempted route-by-route.
        .layer(middleware::from_fn_with_state(
            api_tokens,
            auth::require_bearer_token,
        ))
        .with_state(pool)
}

/// The public, unauthenticated sub-router (Phase 15) -- `/`/`/statusz`
/// plus `/healthz`/`/livez`/`/readyz`. See `status_page`'s own
/// top-level docs for why this is GET-only by construction and what
/// it does (and deliberately does not) render. Never passes through
/// `authenticated_router`'s bearer-token middleware at all, since it's
/// a separate `Router` merged in alongside it, not a route inside it
/// with an auth check skipped.
fn public_router(pool: sqlx::SqlitePool, tick_health: Arc<TickHealth>) -> Router {
    Router::new()
        .route("/", get(status_page::render))
        .route("/statusz", get(status_page::render))
        .route("/healthz", get(status_page::readyz))
        .route("/livez", get(status_page::livez))
        .route("/readyz", get(status_page::readyz))
        .layer(Extension(tick_health))
        .with_state(pool)
}

/// Builds the full `:443` app: `authenticated_router` merged with
/// `public_router`, with the `Server:` header (`server_header_layer`)
/// applied outermost to both -- the one thing every response, success
/// or failure, authenticated or not, should carry.
#[allow(clippy::too_many_arguments)]
pub fn build_router(
    pool: sqlx::SqlitePool,
    api_tokens: Arc<Vec<config::ApiToken>>,
    notify: Arc<Notify>,
    worker_ssh: Arc<ssh::WorkerSshConfig>,
    cost_report_config: Arc<api::cost_report::CostReportConfig>,
    tick_health: Arc<TickHealth>,
) -> Router {
    authenticated_router(
        pool.clone(),
        api_tokens,
        notify,
        worker_ssh,
        cost_report_config,
    )
    .merge(public_router(pool, tick_health))
    .layer(server_header_layer())
}
