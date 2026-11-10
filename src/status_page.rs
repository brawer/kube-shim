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
//!
//! From a CronJob's spec only `schedule`, `suspend` and the container
//! image are read, and of the image only its version (tag or digest) is
//! shown, never the registry or repository path -- see `image_version`.

use crate::metrics_export::is_finished_state;
use crate::pricing;
use crate::reconcile::job::{is_pre_completion_state, RUN_FAILED_SQL};
use crate::reconcile::schedule::parse_schedule;
use crate::reconcile::TickHealth;
use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    Extension, Json,
};
use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::{Row, SqlitePool};
use std::collections::HashMap;
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

/// SQL for the image of the container at `path` in the `spec` JSON
/// column, or NULL when the spec isn't valid JSON or the image isn't a
/// string. Only the image is extracted, never the rest of the spec.
fn image_sql(path: &str) -> String {
    format!(
        "CASE WHEN json_valid(spec) AND json_type(spec, '{path}') = 'text' \
         THEN json_extract(spec, '{path}') END"
    )
}

/// A Job spec's container image; the shim runs `containers[0]` only
/// (`cloud_init`).
const JOB_IMAGE_PATH: &str = "$.template.spec.containers[0].image";
/// The image a CronJob's next run will get.
const CRONJOB_IMAGE_PATH: &str = "$.jobTemplate.spec.template.spec.containers[0].image";

/// The version part of an image reference: its tag, its digest
/// (shortened to 12 hex digits), or both. An image without either runs
/// as `latest`, so that's what is shown. The registry and repository
/// are left out on purpose: they can name private infrastructure, and
/// this page has no authentication.
fn image_version(image: &str) -> String {
    let (name, digest) = match image.split_once('@') {
        Some((name, digest)) => (name, Some(digest)),
        None => (image, None),
    };
    // A `:` before the last `/` is a registry port, not a tag.
    let last_part = name.rsplit('/').next().unwrap_or(name);
    let tag = last_part.split_once(':').map(|(_, tag)| tag);
    let digest = digest.map(|digest| match digest.split_once(':') {
        Some((algorithm, hex)) => {
            format!("{algorithm}:{}", hex.chars().take(12).collect::<String>())
        }
        None => digest.to_string(),
    });
    match (tag, digest) {
        (Some(tag), Some(digest)) => format!("{tag}@{digest}"),
        (Some(tag), None) => tag.to_string(),
        (None, Some(digest)) => digest,
        (None, None) => "latest".to_string(),
    }
}

fn render_version(image: Option<&str>) -> String {
    match image {
        Some(image) => format!("<code>{}</code>", esc(&image_version(image))),
        None => "n/a".to_string(),
    }
}

struct RunningJob {
    name: String,
    namespace: String,
    cronjob_name: Option<String>,
    status: String,
    created_at: i64,
    /// The plan the job's worker VM was created with (issue #85);
    /// `None` until the VM exists.
    plan_name: Option<String>,
    image: Option<String>,
}

async fn running_jobs(pool: &SqlitePool) -> anyhow::Result<Vec<RunningJob>> {
    let rows = sqlx::query(&format!(
        "SELECT name, namespace, cronjob_name, status, created_at, worker_plan, {} \
         FROM jobs WHERE status != 'Archived' ORDER BY created_at DESC",
        image_sql(JOB_IMAGE_PATH)
    ))
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
            let plan_name: Option<String> = row.get(5);
            let image: Option<String> = row.get(6);
            RunningJob {
                name,
                namespace,
                cronjob_name,
                status,
                created_at,
                plan_name,
                image,
            }
        })
        .collect())
}

/// One row of the CronJobs table (issue #127).
struct CronJobSummary {
    name: String,
    namespace: String,
    schedule: String,
    suspended: bool,
    /// The image the next run will get.
    image: Option<String>,
    /// The most recent finished run, `None` if it never ran.
    last_run: Option<LastRun>,
    /// Runs in progress, newest first.
    active_runs: Vec<ActiveRun>,
}

struct LastRun {
    finished_at: i64,
    /// `None` only for a run removed from history (`pruned_jobs`)
    /// before that table recorded the outcome.
    failed: Option<bool>,
    /// In `main_currency`; `None` if pricing wasn't synced yet.
    cost: Option<f64>,
    /// The image the run ran with, from its own copy of the spec.
    image: Option<String>,
}

struct ActiveRun {
    name: String,
    image: Option<String>,
}

async fn cronjob_summaries(pool: &SqlitePool) -> anyhow::Result<Vec<CronJobSummary>> {
    let cronjobs = sqlx::query(&format!(
        "SELECT name, namespace, schedule, \
                COALESCE(CASE WHEN json_valid(spec) THEN json_type(spec, '$.suspend') = 'true' END, 0), \
                {} \
         FROM cronjobs ORDER BY namespace, name",
        image_sql(CRONJOB_IMAGE_PATH)
    ))
    .fetch_all(pool)
    .await?;

    // Every run still in `jobs`, newest first. A run's finish time falls
    // back the same way `metrics_export::write_job_runs` does for rows
    // from before `completed_at` was recorded.
    let runs = sqlx::query(&format!(
        "SELECT cronjob_name, namespace, name, status, {RUN_FAILED_SQL}, \
                COALESCE(completed_at, ( \
                    SELECT MIN(e.timestamp) FROM events e \
                    WHERE e.job_id = jobs.id AND e.reason IN ('Succeeded', 'Failed') \
                ), updated_at), \
                actual_cost, {} \
         FROM jobs WHERE cronjob_name IS NOT NULL ORDER BY created_at DESC, id DESC",
        image_sql(JOB_IMAGE_PATH)
    ))
    .fetch_all(pool)
    .await?;

    // The newest removed run of each CronJob. SQLite takes the bare
    // columns from the row that has the MAX.
    let pruned = sqlx::query(&format!(
        "SELECT cronjob_name, namespace, MAX(COALESCE(completed_at, created_at)), \
                actual_cost, {}, failed \
         FROM pruned_jobs WHERE cronjob_name IS NOT NULL GROUP BY cronjob_name, namespace",
        image_sql(JOB_IMAGE_PATH)
    ))
    .fetch_all(pool)
    .await?;

    let mut last_runs: HashMap<(String, String), LastRun> = HashMap::new();
    let mut active_runs: HashMap<(String, String), Vec<ActiveRun>> = HashMap::new();
    for row in runs {
        let key: (String, String) = (row.get(1), row.get(0));
        let status: String = row.get(3);
        if is_pre_completion_state(&status) {
            active_runs.entry(key).or_default().push(ActiveRun {
                name: row.get(2),
                image: row.get(7),
            });
        } else if is_finished_state(&status) {
            let run = LastRun {
                finished_at: row.get(5),
                failed: Some(row.get(4)),
                cost: row.get(6),
                image: row.get(7),
            };
            keep_newer(&mut last_runs, key, run);
        }
    }
    for row in pruned {
        let key: (String, String) = (row.get(1), row.get(0));
        let run = LastRun {
            finished_at: row.get(2),
            failed: row.get(5),
            cost: row.get(3),
            image: row.get(4),
        };
        keep_newer(&mut last_runs, key, run);
    }

    Ok(cronjobs
        .into_iter()
        .map(|row| {
            let name: String = row.get(0);
            let namespace: String = row.get(1);
            let key = (namespace.clone(), name.clone());
            CronJobSummary {
                schedule: row.get(2),
                suspended: row.get(3),
                image: row.get(4),
                last_run: last_runs.remove(&key),
                active_runs: active_runs.remove(&key).unwrap_or_default(),
                name,
                namespace,
            }
        })
        .collect())
}

/// Keeps whichever run finished later; on a tie, the one already there,
/// so a run still in `jobs` wins over a
/// removed one.
fn keep_newer(
    last_runs: &mut HashMap<(String, String), LastRun>,
    key: (String, String),
    run: LastRun,
) {
    match last_runs.get(&key) {
        Some(existing) if existing.finished_at >= run.finished_at => {}
        _ => {
            last_runs.insert(key, run);
        }
    }
}

fn render_cronjob_rows(
    cronjobs: &[CronJobSummary],
    currency: Option<&str>,
    now: DateTime<Utc>,
) -> String {
    if cronjobs.is_empty() {
        return "<tr><td colspan=\"4\"><em>none</em></td></tr>".to_string();
    }
    cronjobs
        .iter()
        .map(|cj| {
            let mut schedule = format!("<code>{}</code>", esc(&cj.schedule));
            if cj.suspended {
                schedule.push_str(" (suspended)");
            }

            let last_run = match &cj.last_run {
                None => "never".to_string(),
                Some(run) => {
                    let result = match run.failed {
                        Some(true) => "<strong>failed</strong>",
                        Some(false) => "succeeded",
                        None => "result not kept",
                    };
                    let cost = match (run.cost, currency) {
                        (Some(cost), Some(currency)) => format!("{cost:.4} {}", esc(currency)),
                        (Some(cost), None) => format!("{cost:.4}"),
                        (None, _) => "n/a".to_string(),
                    };
                    format!(
                        "{} ago, {result}, {cost}<br>version {}",
                        format_elapsed(now.timestamp() - run.finished_at),
                        render_version(run.image.as_deref()),
                    )
                }
            };

            let next = if !cj.active_runs.is_empty() {
                cj.active_runs
                    .iter()
                    .map(|run| {
                        format!(
                            "running: {}<br>version {}",
                            esc(&run.name),
                            render_version(run.image.as_deref())
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("<br>")
            } else if cj.suspended {
                "suspended".to_string()
            } else {
                // Schedules are evaluated in UTC (`timeZone` isn't
                // supported), so the time is shown in UTC too.
                match parse_schedule(&cj.schedule)
                    .ok()
                    .and_then(|schedule| schedule.next_after(&now))
                {
                    Some(next) => format!(
                        "{} (in {})<br>version {}",
                        next.format("%Y-%m-%d %H:%M UTC"),
                        format_elapsed((next - now).num_seconds()),
                        render_version(cj.image.as_deref()),
                    ),
                    None => "n/a".to_string(),
                }
            };

            let css_class = match &cj.last_run {
                Some(LastRun { failed: Some(true), .. }) => " class=\"warning\"",
                _ => "",
            };
            format!(
                "<tr{css_class}><td>{}/{}</td><td>{schedule}</td><td>{last_run}</td><td>{next}</td></tr>",
                esc(&cj.namespace),
                esc(&cj.name),
            )
        })
        .collect()
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
    let total: Option<f64> = sqlx::query_scalar(
        "SELECT SUM(actual_cost) FROM ( \
               SELECT actual_cost FROM jobs UNION ALL SELECT actual_cost FROM pruned_jobs)",
    )
    .fetch_one(pool)
    .await?;
    Ok(total.unwrap_or(0.0))
}

fn render_html(
    jobs: &[RunningJob],
    cronjobs: &[CronJobSummary],
    events: &[RecentEvent],
    budget: Option<(f64, pricing::BudgetSettings)>,
    accumulated: f64,
) -> String {
    let now_time = Utc::now();
    let now = now_time.timestamp();

    let currency = budget
        .as_ref()
        .map(|(_, settings)| settings.main_currency.as_str());
    let cronjob_rows = render_cronjob_rows(cronjobs, currency, now_time);

    let jobs_rows = if jobs.is_empty() {
        "<tr><td colspan=\"6\"><em>none</em></td></tr>".to_string()
    } else {
        jobs.iter()
            .map(|j| {
                let kind = match &j.cronjob_name {
                    Some(cj) => format!("CronJob: {}", esc(cj)),
                    None => "standalone".to_string(),
                };
                format!(
                    "<tr><td>{}/{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                    esc(&j.namespace),
                    esc(&j.name),
                    kind,
                    esc(&j.status),
                    format_elapsed(now - j.created_at),
                    j.plan_name.as_deref().unwrap_or("n/a"),
                    render_version(j.image.as_deref()),
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
<tr><th>Job</th><th>Type</th><th>Status</th><th>Elapsed</th><th>VM size</th><th>Version</th></tr>
{jobs_rows}
</table>

<h2>CronJobs</h2>
<table>
<tr><th>CronJob</th><th>Schedule</th><th>Last run</th><th>Next</th></tr>
{cronjob_rows}
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
    let cronjobs = match cronjob_summaries(&pool).await {
        Ok(cronjobs) => cronjobs,
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

    Html(render_html(&jobs, &cronjobs, &events, budget, accumulated)).into_response()
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
    use serde_json::json;
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

    /// A `CronJob` run skipped for insufficient budget
    /// (`reconcile::schedule::create_job_run`) has no job row of its
    /// own to show in "Running jobs" -- the whole point of its event
    /// being recorded at all (see that function's own docs) is that
    /// it must still show up here, and visibly so, not blend in with
    /// routine `Normal` transitions.
    #[tokio::test]
    async fn test_render_highlights_budget_skip_events_as_warnings() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        record_event(
            &pool,
            None,
            "BudgetSkip",
            "CronJob default/nightly-import: skipping this run, estimated cost \
             (1.2000 EUR) exceeds the current budget balance -- will retry next tick",
            "Warning",
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
        assert!(body.contains("BudgetSkip"));
        assert!(body.contains("<tr class=\"warning\">"));
    }

    async fn get_page(pool: SqlitePool) -> String {
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
        body_text(response).await
    }

    fn job_spec(image: &str) -> String {
        json!({"template": {"spec": {"containers": [{"image": image}]}}}).to_string()
    }

    async fn insert_cronjob(
        pool: &SqlitePool,
        name: &str,
        schedule: &str,
        suspend: bool,
        image: &str,
    ) {
        let spec = json!({
            "schedule": schedule,
            "suspend": suspend,
            "jobTemplate": {"spec": {"template": {"spec": {"containers": [{"image": image}]}}}},
        });
        sqlx::query(
            "INSERT INTO cronjobs (id, name, namespace, spec, schedule, created_at, updated_at, version) \
             VALUES (?, ?, 'default', ?, ?, 0, 0, 1)",
        )
        .bind(format!("cj-{name}"))
        .bind(name)
        .bind(spec.to_string())
        .bind(schedule)
        .execute(pool)
        .await
        .unwrap();
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_run(
        pool: &SqlitePool,
        id: &str,
        cronjob: &str,
        status: &str,
        exit_code: Option<i64>,
        completed_at: Option<i64>,
        cost: Option<f64>,
        image: &str,
    ) {
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, cronjob_name, spec, status, exit_code, \
                               completed_at, actual_cost, created_at, updated_at, version) \
             VALUES (?, ?, 'default', ?, ?, ?, ?, ?, ?, 0, 0, 1)",
        )
        .bind(id)
        .bind(format!("{cronjob}-{id}"))
        .bind(cronjob)
        .bind(job_spec(image))
        .bind(status)
        .bind(exit_code)
        .bind(completed_at)
        .bind(cost)
        .execute(pool)
        .await
        .unwrap();
    }

    #[test]
    fn test_image_version() {
        assert_eq!(image_version("ghcr.io/acme/tool:1.2.3"), "1.2.3");
        assert_eq!(image_version("registry.example:5000/acme/tool"), "latest");
        assert_eq!(image_version("registry.example:5000/tool:v2"), "v2");
        assert_eq!(image_version("alpine"), "latest");
        assert_eq!(
            image_version("tool@sha256:0123456789abcdef0123456789abcdef"),
            "sha256:0123456789ab"
        );
        assert_eq!(
            image_version("ghcr.io/acme/tool:1.2.3@sha256:0123456789abcdef"),
            "1.2.3@sha256:0123456789ab"
        );
    }

    #[tokio::test]
    async fn test_cronjob_never_run_shows_next_time_and_version() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_cronjob(
            &pool,
            "nightly",
            "0 3 * * *",
            false,
            "ghcr.io/acme/tool:2.0.0",
        )
        .await;

        let body = get_page(pool).await;
        let expected_next = parse_schedule("0 3 * * *")
            .unwrap()
            .next_after(&Utc::now())
            .unwrap()
            .format("%Y-%m-%d %H:%M UTC")
            .to_string();
        assert!(body.contains("default/nightly"));
        assert!(body.contains("<code>0 3 * * *</code>"));
        assert!(body.contains("<td>never</td>"));
        assert!(body.contains(&expected_next));
        assert!(body.contains("version <code>2.0.0</code>"));
        // Only the version, never the registry/repository.
        assert!(!body.contains("ghcr.io"));
    }

    #[tokio::test]
    async fn test_cronjob_last_run_shows_result_cost_and_version_it_ran() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_cronjob(&pool, "nightly", "0 3 * * *", false, "acme/tool:3.0.0").await;
        let now = Utc::now().timestamp();
        insert_run(
            &pool,
            "old",
            "nightly",
            "Archived",
            Some(1),
            Some(now - 7200),
            Some(0.5),
            "acme/tool:1.0.0",
        )
        .await;
        insert_run(
            &pool,
            "new",
            "nightly",
            "Archived",
            Some(0),
            Some(now - 90),
            Some(0.0123),
            "acme/tool:2.0.0",
        )
        .await;

        let body = get_page(pool).await;
        assert!(body.contains("1m30s ago, succeeded, 0.0123<br>version <code>2.0.0</code>"));
        assert!(!body.contains("<code>1.0.0</code>"));
        // The next run gets the CronJob's current image.
        assert!(body.contains("version <code>3.0.0</code>"));
    }

    #[tokio::test]
    async fn test_cronjob_failed_last_run_is_highlighted_and_missing_cost_is_na() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_cronjob(&pool, "nightly", "0 3 * * *", false, "acme/tool:1").await;
        let now = Utc::now().timestamp();
        insert_run(
            &pool,
            "j1",
            "nightly",
            "VMTerminating",
            Some(2),
            Some(now - 30),
            None,
            "acme/tool:1",
        )
        .await;

        let body = get_page(pool).await;
        assert!(body.contains("30s ago, <strong>failed</strong>, n/a"));
        assert!(body.contains("<tr class=\"warning\"><td>default/nightly</td>"));
    }

    #[tokio::test]
    async fn test_cronjob_failed_event_counts_as_failed() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_cronjob(&pool, "nightly", "0 3 * * *", false, "acme/tool:1").await;
        let now = Utc::now().timestamp();
        insert_run(
            &pool,
            "j1",
            "nightly",
            "Archived",
            None,
            Some(now - 30),
            None,
            "acme/tool:1",
        )
        .await;
        record_event(&pool, Some("j1"), "Failed", "deadline exceeded", "Warning")
            .await
            .unwrap();

        let body = get_page(pool).await;
        assert!(body.contains("<strong>failed</strong>"));
    }

    #[tokio::test]
    async fn test_cronjob_with_run_in_progress_shows_that_run_and_its_version() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_cronjob(&pool, "nightly", "0 3 * * *", false, "acme/tool:5").await;
        insert_run(
            &pool,
            "j1",
            "nightly",
            "ContainerRunning",
            None,
            None,
            None,
            "acme/tool:4",
        )
        .await;

        let body = get_page(pool).await;
        assert!(body.contains("running: nightly-j1<br>version <code>4</code>"));
        assert!(body.contains("<td>never</td>"));
    }

    #[tokio::test]
    async fn test_suspended_cronjob() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_cronjob(&pool, "paused", "0 3 * * *", true, "acme/tool:1").await;

        let body = get_page(pool).await;
        assert!(body.contains("<code>0 3 * * *</code> (suspended)"));
        assert!(body.contains("<td>suspended</td>"));
    }

    #[tokio::test]
    async fn test_cronjob_last_run_removed_from_history_shows_its_result() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_cronjob(&pool, "nightly", "0 3 * * *", false, "acme/tool:2").await;
        let now = Utc::now().timestamp();
        insert_run(
            &pool,
            "j1",
            "nightly",
            "Archived",
            Some(1),
            Some(now - 60),
            Some(0.25),
            "acme/tool:2",
        )
        .await;
        crate::reconcile::history::prune_job(&pool, "j1", now)
            .await
            .unwrap();

        let body = get_page(pool).await;
        assert!(
            body.contains("1m00s ago, <strong>failed</strong>, 0.2500<br>version <code>2</code>")
        );
    }

    /// Runs removed before `pruned_jobs.failed` existed have no result.
    #[tokio::test]
    async fn test_cronjob_last_run_removed_before_results_were_kept() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_cronjob(&pool, "nightly", "0 3 * * *", false, "acme/tool:2").await;
        let now = Utc::now().timestamp();
        insert_run(
            &pool,
            "kept",
            "nightly",
            "Archived",
            Some(0),
            Some(now - 7200),
            Some(0.1),
            "acme/tool:1",
        )
        .await;
        sqlx::query(
            "INSERT INTO pruned_jobs (id, name, namespace, cronjob_name, spec, created_at, \
                                      completed_at, actual_cost, pruned_at) \
             VALUES ('gone', 'nightly-gone', 'default', 'nightly', ?, 0, ?, 0.25, ?)",
        )
        .bind(job_spec("acme/tool:2"))
        .bind(now - 60)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        let body = get_page(pool).await;
        assert!(body.contains("1m00s ago, result not kept, 0.2500<br>version <code>2</code>"));
    }

    #[tokio::test]
    async fn test_running_jobs_show_version() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'Created', 0, 0, 1)",
        )
        .bind(job_spec("private.registry.example/team/tool:9.9"))
        .execute(&pool)
        .await
        .unwrap();

        let body = get_page(pool).await;
        assert!(body.contains("<td><code>9.9</code></td>"));
        assert!(!body.contains("private.registry.example"));
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
