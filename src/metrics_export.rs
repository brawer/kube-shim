//! `GET /metrics` (Phase 18): Prometheus text exposition for two
//! audiences on one unauthenticated endpoint, told apart by name
//! prefix.
//!
//! 1. **`kubeshim_*`: the shim's own health.** Process start time and
//!    uptime, per-loop run counters and durations (job tick, orphan
//!    scan, metrics poll, pricing sync -- from `reconcile::TickHealth`),
//!    jobs by state, and the budget balance when the budget guard is on.
//! 2. **`node_vmstat_*`/`node_memory_*`: each worker VM's virtual
//!    memory**, from the latest `worker_metrics` sample
//!    (`reconcile::metrics_collector`). These use node-exporter's exact
//!    metric names and types, so a Grafana dashboard or alert rule built
//!    against kube-shim ports unchanged to a real cluster later. A job
//!    with no sample yet is simply absent, like `metrics.k8s.io`.
//!
//! **Hand-rolled, not a crate.** The text format is just `# HELP`/
//! `# TYPE` lines plus `name{labels} value`, and every value here is
//! read fresh per scrape (atomics, one DB query each) rather than
//! accumulated in a registry. `prometheus` or `metrics` +
//! `metrics-exporter-prometheus` would add a global registry and a
//! dependency tree for about twenty series, so this follows the
//! project's usual preference for fewer dependencies. Label values are
//! escaped per the format spec (`\`, `"`, newline).
//!
//! **Unauthenticated, like the status page.** Infra scrapers can't
//! always send a bearer token. Everything here is either process
//! health or job name/namespace/node name, which the public status page
//! (`status_page`) already shows without auth; no IPs, volume IDs or
//! secrets.
//!
//! Worker series carry `namespace`, `pod` and `node` labels: `pod` is
//! the job's own name (one pod per job run, same as `api::pods`), and
//! `node` is the worker VM name `kubectl get nodes` shows. Not `job`,
//! which Prometheus itself sets to the scrape job's name.

use crate::pricing;
use crate::reconcile::{LoopStats, TickHealth, STATE_SEQUENCE};
use axum::{
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Extension,
};
use chrono::Utc;
use sqlx::{Row, SqlitePool};
use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;

/// The content type Prometheus expects for the text format.
const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Escapes a label value per the exposition format.
fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Formats `{k="v",...}`, or nothing when there are no labels.
fn labels(pairs: &[(&str, &str)]) -> String {
    if pairs.is_empty() {
        return String::new();
    }
    let inner: Vec<String> = pairs
        .iter()
        .map(|(k, v)| format!("{k}=\"{}\"", escape_label(v)))
        .collect();
    format!("{{{}}}", inner.join(","))
}

/// Builds one exposition document. Each metric family is written whole
/// (HELP, TYPE, then every series), as the format requires.
#[derive(Default)]
struct Exposition(String);

impl Exposition {
    fn family(&mut self, name: &str, kind: &str, help: &str) {
        let _ = writeln!(self.0, "# HELP {name} {help}");
        let _ = writeln!(self.0, "# TYPE {name} {kind}");
    }

    fn sample(&mut self, name: &str, pairs: &[(&str, &str)], value: impl std::fmt::Display) {
        let _ = writeln!(self.0, "{name}{} {value}", labels(pairs));
    }

    fn single(&mut self, name: &str, kind: &str, help: &str, value: impl std::fmt::Display) {
        self.family(name, kind, help);
        self.sample(name, &[], value);
    }
}

/// `kubeshim_<loop>_runs_total{result}`, `..._duration_seconds`
/// (a summary with only `_sum`/`_count`, no quantiles) and
/// `..._last_run_timestamp_seconds` for one background loop.
fn write_loop(out: &mut Exposition, prefix: &str, what: &str, stats: &LoopStats) {
    let runs = format!("kubeshim_{prefix}_runs_total");
    out.family(
        &runs,
        "counter",
        &format!("Completed {what} passes, by result."),
    );
    out.sample(&runs, &[("result", "success")], stats.successes());
    out.sample(&runs, &[("result", "failure")], stats.failures());

    let duration = format!("kubeshim_{prefix}_duration_seconds");
    out.family(
        &duration,
        "summary",
        &format!("Time spent in {what} passes."),
    );
    out.sample(
        &format!("{duration}_sum"),
        &[],
        stats.duration_seconds_total(),
    );
    out.sample(
        &format!("{duration}_count"),
        &[],
        stats.successes() + stats.failures(),
    );

    out.single(
        &format!("kubeshim_{prefix}_last_run_timestamp_seconds"),
        "gauge",
        &format!("Unix time of the most recent {what} pass, or 0 if none yet."),
        stats.last_run_at(),
    );
}

fn write_self_metrics(out: &mut Exposition, health: &TickHealth, now: i64) {
    out.family(
        "kubeshim_build_info",
        "gauge",
        "Always 1; the version label is the running kube-shim release.",
    );
    out.sample(
        "kubeshim_build_info",
        &[("version", env!("CARGO_PKG_VERSION"))],
        1,
    );
    out.single(
        "kubeshim_process_start_time_seconds",
        "gauge",
        "Unix time the kube-shim process started.",
        health.started_at(),
    );
    out.single(
        "kubeshim_uptime_seconds",
        "gauge",
        "Seconds since the kube-shim process started.",
        (now - health.started_at()).max(0),
    );

    write_loop(out, "reconcile_tick", "reconciliation tick", &health.tick);
    write_loop(out, "orphan_scan", "orphan scan", &health.orphan_scan);
    write_loop(
        out,
        "metrics_poll",
        "worker metrics poll",
        &health.metrics_poll,
    );
    write_loop(out, "pricing_sync", "pricing sync", &health.pricing_sync);
}

/// One series per `STATE_SEQUENCE` value, zero-filled, so a state with
/// no jobs reads 0 instead of disappearing (which would break
/// `rate()`/alerts on it). A status outside the sequence, which
/// shouldn't happen, is still reported rather than dropped.
async fn write_jobs_by_state(out: &mut Exposition, pool: &SqlitePool) -> anyhow::Result<()> {
    let rows = sqlx::query("SELECT status, COUNT(*) FROM jobs GROUP BY status")
        .fetch_all(pool)
        .await?;
    let mut counts: HashMap<String, i64> = rows
        .into_iter()
        .map(|row| (row.get::<String, _>(0), row.get::<i64, _>(1)))
        .collect();

    out.family(
        "kubeshim_jobs_by_state",
        "gauge",
        "Job runs currently in each reconciliation state.",
    );
    for state in STATE_SEQUENCE {
        let count = counts.remove(*state).unwrap_or(0);
        out.sample("kubeshim_jobs_by_state", &[("state", state)], count);
    }
    let mut leftover: Vec<_> = counts.into_iter().collect();
    leftover.sort();
    for (state, count) in leftover {
        out.sample("kubeshim_jobs_by_state", &[("state", &state)], count);
    }
    Ok(())
}

/// Only when the budget guard is enabled (a daily rate above zero),
/// matching what the status page shows.
async fn write_budget(out: &mut Exposition, pool: &SqlitePool) -> anyhow::Result<()> {
    if let Some((balance, settings)) = pricing::settle_accrual(pool).await? {
        if settings.budget_daily_rate > 0.0 {
            out.family(
                "kubeshim_budget_balance",
                "gauge",
                "Current rolling budget balance, in the main currency.",
            );
            out.sample(
                "kubeshim_budget_balance",
                &[("currency", &settings.main_currency)],
                balance,
            );
        }
    }
    Ok(())
}

struct WorkerVmSample {
    namespace: String,
    pod: String,
    node: String,
    pgfault: i64,
    pgmajfault: i64,
    pswpin: i64,
    pswpout: i64,
    mapped_bytes: i64,
    cached_bytes: i64,
    anon_bytes: i64,
}

async fn worker_vm_samples(pool: &SqlitePool) -> anyhow::Result<Vec<WorkerVmSample>> {
    let rows = sqlx::query(
        "SELECT jobs.namespace, jobs.name, jobs.worker_vm_name, \
                m.pgfault, m.pgmajfault, m.pswpin, m.pswpout, \
                m.mapped_bytes, m.cached_bytes, m.anon_bytes \
         FROM worker_metrics m JOIN jobs ON m.job_id = jobs.id \
         WHERE m.pgfault IS NOT NULL AND m.pgmajfault IS NOT NULL \
           AND m.pswpin IS NOT NULL AND m.pswpout IS NOT NULL \
           AND m.mapped_bytes IS NOT NULL AND m.cached_bytes IS NOT NULL \
           AND m.anon_bytes IS NOT NULL \
         ORDER BY jobs.namespace, jobs.name",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| WorkerVmSample {
            namespace: row.get(0),
            pod: row.get(1),
            node: row.get::<Option<String>, _>(2).unwrap_or_default(),
            pgfault: row.get(3),
            pgmajfault: row.get(4),
            pswpin: row.get(5),
            pswpout: row.get(6),
            mapped_bytes: row.get(7),
            cached_bytes: row.get(8),
            anon_bytes: row.get(9),
        })
        .collect())
}

/// node-exporter's own names, types and HELP wording: `/proc/vmstat`
/// fields are `untyped`, `/proc/meminfo` fields are `gauge`.
fn write_worker_vm_metrics(out: &mut Exposition, samples: &[WorkerVmSample]) {
    if samples.is_empty() {
        return;
    }
    type Field = fn(&WorkerVmSample) -> i64;
    let families: &[(&str, &str, &str, Field)] = &[
        (
            "node_vmstat_pgfault",
            "untyped",
            "/proc/vmstat information field pgfault.",
            |s| s.pgfault,
        ),
        (
            "node_vmstat_pgmajfault",
            "untyped",
            "/proc/vmstat information field pgmajfault.",
            |s| s.pgmajfault,
        ),
        (
            "node_vmstat_pswpin",
            "untyped",
            "/proc/vmstat information field pswpin.",
            |s| s.pswpin,
        ),
        (
            "node_vmstat_pswpout",
            "untyped",
            "/proc/vmstat information field pswpout.",
            |s| s.pswpout,
        ),
        (
            "node_memory_Mapped_bytes",
            "gauge",
            "Memory information field Mapped_bytes.",
            |s| s.mapped_bytes,
        ),
        (
            "node_memory_Cached_bytes",
            "gauge",
            "Memory information field Cached_bytes.",
            |s| s.cached_bytes,
        ),
        (
            "node_memory_AnonPages_bytes",
            "gauge",
            "Memory information field AnonPages_bytes.",
            |s| s.anon_bytes,
        ),
    ];
    for (name, kind, help, field) in families {
        out.family(name, kind, help);
        for s in samples {
            out.sample(
                name,
                &[
                    ("namespace", &s.namespace),
                    ("pod", &s.pod),
                    ("node", &s.node),
                ],
                field(s),
            );
        }
    }
}

async fn render_all(pool: &SqlitePool, health: &TickHealth) -> anyhow::Result<String> {
    let mut out = Exposition::default();
    write_self_metrics(&mut out, health, Utc::now().timestamp());
    write_jobs_by_state(&mut out, pool).await?;
    write_budget(&mut out, pool).await?;
    write_worker_vm_metrics(&mut out, &worker_vm_samples(pool).await?);
    Ok(out.0)
}

pub async fn render(
    State(pool): State<SqlitePool>,
    Extension(tick_health): Extension<Arc<TickHealth>>,
) -> Response {
    match render_all(&pool, &tick_health).await {
        Ok(body) => ([(header::CONTENT_TYPE, CONTENT_TYPE)], body).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Router};
    use std::time::Duration;
    use tower::ServiceExt;

    async fn scrape(pool: SqlitePool, health: Arc<TickHealth>) -> (StatusCode, String, String) {
        let response = Router::new()
            .route("/metrics", get(render))
            .layer(Extension(health))
            .with_state(pool)
            .oneshot(
                axum::http::Request::builder()
                    .uri("/metrics")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .map(|v| v.to_str().unwrap().to_string())
            .unwrap_or_default();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            content_type,
            String::from_utf8(bytes.to_vec()).unwrap(),
        )
    }

    #[test]
    fn test_escape_label() {
        assert_eq!(escape_label("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
    }

    #[tokio::test]
    async fn test_self_metrics_and_loop_counters() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let health = Arc::new(TickHealth::new());
        health.tick.record(true, Duration::from_millis(1500));
        health.tick.record(false, Duration::from_millis(500));
        health.orphan_scan.record(true, Duration::from_millis(10));

        let (status, content_type, body) = scrape(pool, health).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(content_type, CONTENT_TYPE);
        assert!(body.contains(&format!(
            "kubeshim_build_info{{version=\"{}\"}} 1",
            env!("CARGO_PKG_VERSION")
        )));
        assert!(body.contains("# TYPE kubeshim_uptime_seconds gauge"));
        assert!(body.contains("kubeshim_reconcile_tick_runs_total{result=\"success\"} 1"));
        assert!(body.contains("kubeshim_reconcile_tick_runs_total{result=\"failure\"} 1"));
        assert!(body.contains("kubeshim_reconcile_tick_duration_seconds_sum 2\n"));
        assert!(body.contains("kubeshim_reconcile_tick_duration_seconds_count 2\n"));
        assert!(body.contains("kubeshim_orphan_scan_runs_total{result=\"success\"} 1"));
        assert!(body.contains("kubeshim_metrics_poll_last_run_timestamp_seconds 0\n"));
        assert!(body.contains("kubeshim_pricing_sync_runs_total{result=\"failure\"} 0"));
    }

    #[tokio::test]
    async fn test_jobs_by_state_zero_fills_every_state() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        for (id, status) in [
            ("j1", "ContainerRunning"),
            ("j2", "ContainerRunning"),
            ("j3", "Archived"),
        ] {
            sqlx::query(
                "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
                 VALUES (?, ?, 'default', '{}', ?, 0, 0, 1)",
            )
            .bind(id)
            .bind(id)
            .bind(status)
            .execute(&pool)
            .await
            .unwrap();
        }

        let (_, _, body) = scrape(pool, Arc::new(TickHealth::new())).await;
        assert!(body.contains("kubeshim_jobs_by_state{state=\"ContainerRunning\"} 2"));
        assert!(body.contains("kubeshim_jobs_by_state{state=\"Archived\"} 1"));
        assert!(body.contains("kubeshim_jobs_by_state{state=\"Created\"} 0"));
        let series = body
            .lines()
            .filter(|l| l.starts_with("kubeshim_jobs_by_state{"))
            .count();
        assert_eq!(series, STATE_SEQUENCE.len());
    }

    #[tokio::test]
    async fn test_budget_balance_absent_while_budget_guard_disabled() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let (_, _, body) = scrape(pool, Arc::new(TickHealth::new())).await;
        assert!(!body.contains("kubeshim_budget_balance"));
    }

    async fn insert_job(pool: &SqlitePool, id: &str, name: &str, vm: &str) {
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_vm_name, worker_ssh_ip, \
             created_at, updated_at, version) \
             VALUES (?, ?, 'batch', '{}', 'ContainerRunning', ?, '203.0.113.7', 0, 0, 1)",
        )
        .bind(id)
        .bind(name)
        .bind(vm)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_worker_vm_metrics_use_node_exporter_names() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job(&pool, "j1", "osmdiffs", "kube-shim-worker-a").await;
        sqlx::query(
            "INSERT INTO worker_metrics (job_id, node_memory_total_bytes, node_memory_used_bytes, \
             node_cpu_count, node_load1, sampled_at, pgfault, pgmajfault, pswpin, pswpout, \
             mapped_bytes, cached_bytes, anon_bytes) \
             VALUES ('j1', 2000, 1000, 1, 0.1, 0, 900, 42, 0, 0, 4096, 8192, 1024)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let (_, _, body) = scrape(pool, Arc::new(TickHealth::new())).await;
        let labels = "{namespace=\"batch\",pod=\"osmdiffs\",node=\"kube-shim-worker-a\"}";
        assert!(body.contains("# TYPE node_vmstat_pgmajfault untyped"));
        assert!(body.contains(&format!("node_vmstat_pgfault{labels} 900\n")));
        assert!(body.contains(&format!("node_vmstat_pgmajfault{labels} 42\n")));
        assert!(body.contains(&format!("node_vmstat_pswpin{labels} 0\n")));
        assert!(body.contains("# TYPE node_memory_Mapped_bytes gauge"));
        assert!(body.contains(&format!("node_memory_Mapped_bytes{labels} 4096\n")));
        assert!(body.contains(&format!("node_memory_Cached_bytes{labels} 8192\n")));
        assert!(body.contains(&format!("node_memory_AnonPages_bytes{labels} 1024\n")));
        // Never the worker's IP, even though it's in the jobs row.
        assert!(!body.contains("203.0.113.7"));
    }

    #[tokio::test]
    async fn test_worker_without_vm_stats_is_absent() {
        // A sample from before Phase 18, or whose /proc sections didn't
        // parse: node-health only, VM columns NULL.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job(&pool, "j1", "osmdiffs", "kube-shim-worker-a").await;
        sqlx::query(
            "INSERT INTO worker_metrics (job_id, node_memory_total_bytes, node_memory_used_bytes, \
             node_cpu_count, node_load1, sampled_at) VALUES ('j1', 2000, 1000, 1, 0.1, 0)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let (_, _, body) = scrape(pool, Arc::new(TickHealth::new())).await;
        assert!(!body.contains("node_vmstat"));
        assert!(!body.contains("node_memory_"));
    }
}
