//! Public, unauthenticated status page + health endpoints (Phase 15) --
//! `/` and `/statusz` (same content, the informal zPages-style alias),
//! plus `/healthz`/`/livez`/`/readyz`. Merged into the main `:443` app
//! (`app::build_router`) as a separate sub-router that never passes
//! through Phase 2's bearer-token middleware at all -- these are meant
//! to be reachable from a phone browser or an uptime monitor with no
//! token, the same way a real cluster's own health endpoints are.
//!
//! **GET-only by construction, not just by convention**: every route
//! this module's handlers are mounted on in `app.rs` only ever
//! registers a `get(...)` -- there is no write path here for a future
//! bug to accidentally expose, because none was ever wired up, not
//! because a check happens to reject one.
//!
//! **Explicit allowlist, not a generic dump**: every query below
//! selects exactly the columns rendered and nothing else -- no
//! `worker_ssh_ip`, no `volume_id`/`worker_vm_id`, no raw `spec` JSON,
//! no secret/token material of any kind. This page has no
//! authentication at all, so what it shows is a deliberate, narrow
//! choice (job name/namespace/status/elapsed time/VM size, recent
//! event reasons/messages, cost figures), not "whatever happened to be
//! convenient to query."

use crate::reconcile::job::extract_resource_requests;
use crate::reconcile::TickHealth;
use crate::{pricing, workload};
use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    Extension, Json,
};
use chrono::Utc;
use serde_json::json;
use sqlx::{Row, SqlitePool};
use std::sync::Arc;

/// Minimal HTML escaping -- job names/namespaces/event messages are
/// operator-controlled, not third-party input, but this page has no
/// auth at all, so a stray `<`/`&` in a job name shouldn't be able to
/// break the page's own markup either.
fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn format_elapsed(seconds: i64) -> String {
    let seconds = seconds.max(0);
    let h = seconds / 3600;
    let m = (seconds % 3600) / 60;
    let s = seconds % 60;
    if h > 0 {
        format!("{h}h{m:02}m")
    } else if m > 0 {
        format!("{m}m{s:02}s")
    } else {
        format!("{s}s")
    }
}

struct RunningJob {
    name: String,
    namespace: String,
    cronjob_name: Option<String>,
    status: String,
    created_at: i64,
    plan_name: Option<&'static str>,
}

async fn running_jobs(pool: &SqlitePool) -> anyhow::Result<Vec<RunningJob>> {
    let rows = sqlx::query(
        "SELECT name, namespace, cronjob_name, status, created_at, spec \
         FROM jobs WHERE status != 'Archived' ORDER BY created_at DESC",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let name: String = row.get(0);
            let namespace: String = row.get(1);
            let cronjob_name: Option<String> = row.get(2);
            let status: String = row.get(3);
            let created_at: i64 = row.get(4);
            let spec_str: String = row.get(5);
            let spec: serde_json::Value =
                serde_json::from_str(&spec_str).unwrap_or(serde_json::Value::Null);
            let (cpu_millicores, memory_mb) = extract_resource_requests(&spec);
            let plan_name = workload::plan_for_resource_requests(cpu_millicores, memory_mb)
                .map(|plan| plan.name);
            RunningJob {
                name,
                namespace,
                cronjob_name,
                status,
                created_at,
                plan_name,
            }
        })
        .collect())
}

struct RecentEvent {
    reason: String,
    message: String,
    timestamp: i64,
    event_type: String,
    job_name: Option<String>,
    job_namespace: Option<String>,
}

async fn recent_events(pool: &SqlitePool) -> anyhow::Result<Vec<RecentEvent>> {
    let rows = sqlx::query(
        "SELECT e.reason, e.message, e.timestamp, e.type, j.name, j.namespace \
         FROM events e LEFT JOIN jobs j ON e.job_id = j.id \
         ORDER BY e.timestamp DESC LIMIT 20",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RecentEvent {
            reason: row.get(0),
            message: row.get(1),
            timestamp: row.get(2),
            event_type: row.get(3),
            job_name: row.get(4),
            job_namespace: row.get(5),
        })
        .collect())
}

async fn accumulated_cost(pool: &SqlitePool) -> anyhow::Result<f64> {
    let total: Option<f64> =
        sqlx::query_scalar("SELECT SUM(actual_cost) FROM jobs WHERE actual_cost IS NOT NULL")
            .fetch_one(pool)
            .await?;
    Ok(total.unwrap_or(0.0))
}

fn render_html(
    jobs: &[RunningJob],
    events: &[RecentEvent],
    budget: Option<(f64, pricing::BudgetSettings)>,
    accumulated: f64,
) -> String {
    let now = Utc::now().timestamp();

    let jobs_rows = if jobs.is_empty() {
        "<tr><td colspan=\"5\"><em>none</em></td></tr>".to_string()
    } else {
        jobs.iter()
            .map(|j| {
                let kind = match &j.cronjob_name {
                    Some(cj) => format!("CronJob: {}", esc(cj)),
                    None => "standalone".to_string(),
                };
                format!(
                    "<tr><td>{}/{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                    esc(&j.namespace),
                    esc(&j.name),
                    kind,
                    esc(&j.status),
                    format_elapsed(now - j.created_at),
                    j.plan_name.unwrap_or("n/a"),
                )
            })
            .collect::<String>()
    };

    let events_rows = if events.is_empty() {
        "<tr><td colspan=\"4\"><em>none</em></td></tr>".to_string()
    } else {
        events
            .iter()
            .map(|e| {
                let who = match (&e.job_namespace, &e.job_name) {
                    (Some(ns), Some(name)) => format!("{}/{}", esc(ns), esc(name)),
                    _ => "(system)".to_string(),
                };
                let css_class = if e.event_type == "Warning" {
                    "warning"
                } else {
                    "normal"
                };
                format!(
                    "<tr class=\"{css_class}\"><td>{}</td><td>{who}</td><td>{}</td><td>{}</td></tr>",
                    format_elapsed(now - e.timestamp) ,
                    esc(&e.reason),
                    esc(&e.message),
                )
            })
            .collect::<String>()
    };

    let budget_html = match budget {
        Some((balance, settings)) if settings.budget_daily_rate > 0.0 => format!(
            "<p>Balance: <strong>{:.4} {}</strong> (accrues {:.4}/day, capped at {} days)</p>",
            balance,
            esc(&settings.main_currency),
            settings.budget_daily_rate,
            settings.budget_rollover_cap_days
        ),
        _ => "<p>Budget guard: <em>disabled</em></p>".to_string(),
    };

    format!(
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="15">
<title>kube-shim status</title>
<style>
  body {{ font-family: system-ui, sans-serif; margin: 1.5rem; max-width: 60rem; }}
  h1 {{ font-size: 1.3rem; }}
  h2 {{ font-size: 1.05rem; margin-top: 2rem; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 0.9rem; }}
  td, th {{ text-align: left; padding: 0.3rem 0.6rem; border-bottom: 1px solid #ddd; vertical-align: top; }}
  tr.warning {{ background: #fff3e0; }}
  footer {{ margin-top: 2rem; font-size: 0.8rem; color: #666; }}
</style>
</head>
<body>
<h1>kube-shim</h1>
{budget_html}
<p>Accumulated real cost (completed jobs): <strong>{accumulated:.4}</strong></p>

<h2>Running jobs</h2>
<table>
<tr><th>Job</th><th>Type</th><th>Status</th><th>Elapsed</th><th>VM size</th></tr>
{jobs_rows}
</table>

<h2>Recent events</h2>
<table>
<tr><th>Age</th><th>Job</th><th>Reason</th><th>Message</th></tr>
{events_rows}
</table>

<footer>kube-shim/{version} &middot; refreshes every 15s</footer>
</body>
</html>
"#,
        version = env!("CARGO_PKG_VERSION"),
    )
}

pub async fn render(State(pool): State<SqlitePool>) -> Response {
    let jobs = match running_jobs(&pool).await {
        Ok(jobs) => jobs,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    let events = match recent_events(&pool).await {
        Ok(events) => events,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    let accumulated = match accumulated_cost(&pool).await {
        Ok(cost) => cost,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    let budget = pricing::settle_accrual(&pool).await.unwrap_or(None);

    Html(render_html(&jobs, &events, budget, accumulated)).into_response()
}

/// Liveness: the process itself is up and the async runtime is
/// responding -- nothing more. Deliberately checks *no* dependency
/// (DB, reconciliation loop): a real k8s liveness probe that depends
/// on downstream health causes exactly the wrong reaction to a
/// transient DB blip (killing/restarting a process that was never
/// actually stuck), which is what `/readyz` below is for instead.
pub async fn livez() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({"status": "ok"})))
}

/// Readiness: the DB is actually reachable *and* the reconciliation
/// loop has ticked recently (`TickHealth`, `reconcile` module) --
/// real signals, not a hardcoded `200`. `/healthz` is a plain alias
/// of this, kept for older tooling that only knows the combined name.
pub async fn readyz(
    State(pool): State<SqlitePool>,
    Extension(tick_health): Extension<Arc<TickHealth>>,
) -> impl IntoResponse {
    let db_ok = sqlx::query_scalar::<_, i64>("SELECT 1")
        .fetch_one(&pool)
        .await
        .is_ok();
    let loop_ok = tick_health.is_healthy();

    if db_ok && loop_ok {
        (StatusCode::OK, Json(json!({"status": "ok"})))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "not ready", "db_ok": db_ok, "reconcile_loop_ok": loop_ok})),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reconcile::job::record_event;
    use axum::{routing::get, Router};
    use tower::ServiceExt;

    fn router(pool: SqlitePool, tick_health: Arc<TickHealth>) -> Router {
        Router::new()
            .route("/", get(render))
            .route("/readyz", get(readyz))
            .route("/livez", get(livez))
            .layer(Extension(tick_health))
            .with_state(pool)
    }

    async fn body_text(response: Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn test_render_lists_running_jobs_and_excludes_archived() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'VolumePending', ?, ?, 1)",
        )
        .bind(Utc::now().timestamp() - 90)
        .bind(Utc::now().timestamp())
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j2', 'job-done', 'default', '{}', 'Archived', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let response = router(pool, Arc::new(TickHealth::new()))
            .oneshot(
                axum::http::Request::builder()
                    .uri("/")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(body.contains("default/job-one"));
        assert!(body.contains("VolumePending"));
        assert!(body.contains("1m30s"));
        assert!(!body.contains("job-done"));
    }

    #[tokio::test]
    async fn test_render_escapes_job_names() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', '<script>evil</script>', 'default', '{}', 'Created', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let response = router(pool, Arc::new(TickHealth::new()))
            .oneshot(
                axum::http::Request::builder()
                    .uri("/")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = body_text(response).await;
        assert!(!body.contains("<script>"));
        assert!(body.contains("&lt;script&gt;"));
    }

    #[tokio::test]
    async fn test_render_never_includes_worker_ssh_ip_or_volume_id() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_ssh_ip, volume_id, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'ContainerRunning', '203.0.113.99', 'vol-secret-id', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let response = router(pool, Arc::new(TickHealth::new()))
            .oneshot(
                axum::http::Request::builder()
                    .uri("/")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = body_text(response).await;
        assert!(!body.contains("203.0.113.99"));
        assert!(!body.contains("vol-secret-id"));
    }

    #[tokio::test]
    async fn test_render_shows_recent_events_including_system_ones() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'Created', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();
        record_event(&pool, Some("j1"), "VolumePending", "Transitioned", "Normal")
            .await
            .unwrap();
        record_event(
            &pool,
            None,
            "BudgetToppedUp",
            "+20.0000, new balance 20.0000",
            "Normal",
        )
        .await
        .unwrap();

        let response = router(pool, Arc::new(TickHealth::new()))
            .oneshot(
                axum::http::Request::builder()
                    .uri("/")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = body_text(response).await;
        assert!(body.contains("default/job-one"));
        assert!(body.contains("VolumePending"));
        assert!(body.contains("(system)"));
        assert!(body.contains("BudgetToppedUp"));
    }

    #[tokio::test]
    async fn test_livez_always_ok() {
        let response = livez().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_readyz_not_ready_before_first_tick() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let response = router(pool, Arc::new(TickHealth::new()))
            .oneshot(
                axum::http::Request::builder()
                    .uri("/readyz")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_readyz_ok_once_a_tick_has_landed() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let tick_health = Arc::new(TickHealth::new());
        tick_health.record();

        let response = router(pool, tick_health)
            .oneshot(
                axum::http::Request::builder()
                    .uri("/readyz")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
