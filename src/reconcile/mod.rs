//! Reconciliation loop skeleton (Phase 6): the state machine that will
//! eventually drive all real orchestration. Every transition today is
//! mocked -- no external API calls, matching this phase's own scope --
//! but the loop's own architecture (fallback poll + `Notify` wake-up,
//! startup recovery, per-job version bookkeeping) is the real thing later
//! phases build directly on top of, not a throwaway prototype.

pub mod job;
pub mod orphan_scan;
pub mod schedule;
pub mod startup;

pub use job::JobContext;

use anyhow::Result;
use sqlx::SqlitePool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

/// Fallback cadence, used alongside (not instead of) `Notify`-based
/// wake-ups. Still needed even once every event the shim causes itself
/// wakes the loop immediately: it's what catches state changes the shim
/// wouldn't otherwise hear about -- a provider-side VM failure, once
/// Phase 7+ adds real provider calls, for example.
const FALLBACK_INTERVAL: Duration = Duration::from_secs(10);

/// Orphan scanning runs on its own, much slower cadence, deliberately
/// decoupled from `FALLBACK_INTERVAL`/`Notify`. The job-tick loop can fire
/// far more often than every 10s -- any API call that creates a job wakes
/// it immediately via `Notify` -- and there is no reason to hit
/// `GET /1.3/storage/normal` at that same rate just to look for volumes
/// nothing has leaked yet. Five minutes is frequent enough that a real
/// leak doesn't sit around costing money for long, without treating every
/// job-tick wake-up as a reason to re-list the whole account's volumes.
const ORPHAN_SCAN_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Runs forever, driving the reconciliation loop: one pass immediately,
/// then one every time either the fallback interval elapses or `notify`
/// fires, whichever comes first. Callers wake it immediately by calling
/// `notify.notify_one()` after any change the shim already knows about
/// (e.g. a new `CronJob` created via the API) rather than waiting up to
/// `FALLBACK_INTERVAL` for the change to be noticed.
///
/// Also spawns a second, independent background task that scans for
/// orphaned volumes every `ORPHAN_SCAN_INTERVAL` -- see that constant's
/// doc for why this isn't just part of the job tick above.
///
/// Intended to be spawned once as a background task from `main.rs`, after
/// `startup::recover()` has already run. `ctx` is threaded straight
/// through to both `job::advance_all()` (via the job tick) and
/// `orphan_scan::scan_and_clean()` (via its own loop) -- see their own
/// docs (Phase 7/8).
pub async fn run(pool: SqlitePool, notify: Arc<Notify>, ctx: JobContext) {
    tokio::spawn(run_orphan_scan_loop(
        pool.clone(),
        ctx.clone(),
        ORPHAN_SCAN_INTERVAL,
    ));
    run_with_interval(pool, notify, FALLBACK_INTERVAL, ctx).await
}

/// One pass immediately (matching the job loop's own eager-first-tick
/// behavior -- useful for catching a leak left over from before the shim
/// last restarted), then one every `interval_duration` thereafter, with
/// no `Notify` wake-up: nothing the shim does itself needs an untracked
/// volume cleaned up sooner than the next scheduled scan.
async fn run_orphan_scan_loop(pool: SqlitePool, ctx: JobContext, interval_duration: Duration) {
    let mut interval = tokio::time::interval(interval_duration);

    loop {
        interval.tick().await;

        match orphan_scan::scan_and_clean(&pool, &ctx).await {
            Ok(cleaned) if cleaned > 0 => {
                tracing::warn!("orphan scan: cleaned up {cleaned} untracked volume(s)")
            }
            Ok(_) => {}
            Err(err) => tracing::error!("orphan scan failed: {err:?}"),
        }
    }
}

async fn run_with_interval(
    pool: SqlitePool,
    notify: Arc<Notify>,
    interval_duration: Duration,
    ctx: JobContext,
) {
    let mut interval = tokio::time::interval(interval_duration);

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = notify.notified() => {}
        }

        if let Err(err) = tick(&pool, &ctx).await {
            tracing::error!("reconciliation tick failed: {err:?}");
        }
    }
}

/// One reconciliation pass: check every `CronJob`'s schedule for a due
/// run, then advance every non-terminal job by one state. Orphan
/// scanning is *not* part of this pass -- it runs on its own slower
/// cadence, see `run_orphan_scan_loop`/`ORPHAN_SCAN_INTERVAL`.
pub async fn tick(pool: &SqlitePool, ctx: &JobContext) -> Result<()> {
    let scheduled = schedule::schedule_due_jobs(pool).await?;
    let advanced = job::advance_all(pool, ctx).await?;
    if scheduled > 0 || advanced > 0 {
        tracing::debug!(
            "reconciliation tick: scheduled {scheduled} new job(s), advanced {advanced}"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::upcloud::UpCloudProvider;
    use std::time::Instant;

    fn mock_ctx() -> JobContext {
        JobContext {
            provider: Arc::new(UpCloudProvider::new("unused-in-dry-run")),
            dry_run: true,
            resource_prefix: "kube-shim-test".to_string(),
            zone: "de-fra1".to_string(),
        }
    }

    #[tokio::test]
    async fn test_notify_wakes_the_loop_faster_than_the_fallback_interval() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'Created', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let notify = Arc::new(Notify::new());
        let loop_pool = pool.clone();
        let loop_notify = notify.clone();
        // A deliberately long fallback interval -- if the second advance
        // below happens quickly, it can only be because Notify woke the
        // loop, not because the fallback tick happened to land first.
        tokio::spawn(async move {
            run_with_interval(loop_pool, loop_notify, Duration::from_secs(30), mock_ctx()).await;
        });

        // tokio::time::interval's *first* tick fires immediately (by
        // design -- see run()'s own doc comment: "one pass immediately,
        // then..."), so wait that eager startup pass out first, rather
        // than racing it against the notify_one() below.
        wait_for_status_change(&pool, "Created", Duration::from_secs(5)).await;
        let after_startup_pass: String =
            sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();

        let start = Instant::now();
        notify.notify_one();
        wait_for_status_change(&pool, &after_startup_pass, Duration::from_secs(5)).await;

        assert!(
            start.elapsed() < Duration::from_secs(5),
            "job was not advanced within 5s of notify_one()"
        );
    }

    async fn wait_for_status_change(pool: &SqlitePool, previous: &str, timeout: Duration) {
        let start = Instant::now();
        loop {
            let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
                .fetch_one(pool)
                .await
                .unwrap();
            if status != previous {
                return;
            }
            assert!(
                start.elapsed() < timeout,
                "status did not change away from {previous:?} within {timeout:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn test_tick_schedules_and_advances_in_one_pass() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let created_at = chrono::Utc::now().timestamp() - 120;
        sqlx::query(
            "INSERT INTO cronjobs (id, name, namespace, spec, schedule, created_at, updated_at, version) \
             VALUES ('cj1', 'every-minute', 'default', '{\"jobTemplate\":{\"spec\":{}}}', '* * * * *', ?, ?, 1)",
        )
        .bind(created_at)
        .bind(created_at)
        .execute(&pool)
        .await
        .unwrap();

        tick(&pool, &mock_ctx()).await.unwrap();

        let status: String =
            sqlx::query_scalar("SELECT status FROM jobs WHERE cronjob_name = 'every-minute'")
                .fetch_one(&pool)
                .await
                .unwrap();
        // Created by schedule_due_jobs(), then immediately advanced one
        // step by job::advance_all() in the same tick.
        assert_eq!(status, "VolumePending");
    }

    #[tokio::test]
    async fn test_orphan_scan_loop_runs_on_its_own_periodic_cadence() {
        use axum::Json;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let call_count = Arc::new(AtomicUsize::new(0));
        let counter = call_count.clone();
        let app = axum::Router::new().route(
            "/1.3/storage/normal",
            axum::routing::get(move || {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Json(serde_json::json!({"storages": {"storage": []}}))
                }
            }),
        );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let ctx = JobContext {
            provider: Arc::new(provider),
            dry_run: false,
            resource_prefix: "kube-shim-test".to_string(),
            zone: "de-fra1".to_string(),
        };

        // A short interval stands in for ORPHAN_SCAN_INTERVAL here --
        // waiting out the real 5 minutes would make this test useless.
        tokio::spawn(run_orphan_scan_loop(pool, ctx, Duration::from_millis(20)));

        tokio::time::sleep(Duration::from_millis(150)).await;
        let calls = call_count.load(Ordering::SeqCst);
        assert!(
            calls >= 2,
            "expected the orphan scan loop to run more than once within 150ms \
             at a 20ms interval (proving it's periodic, not just an eager \
             first pass), got {calls}"
        );
    }
}
