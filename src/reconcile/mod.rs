//! Reconciliation loop skeleton (Phase 6): the state machine that will
//! eventually drive all real orchestration. Every transition today is
//! mocked -- no external API calls, matching this phase's own scope --
//! but the loop's own architecture (fallback poll + `Notify` wake-up,
//! startup recovery, per-job version bookkeeping) is the real thing later
//! phases build directly on top of, not a throwaway prototype.

pub mod job;
pub mod schedule;
pub mod startup;

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

/// Runs forever, driving the reconciliation loop: one pass immediately,
/// then one every time either the fallback interval elapses or `notify`
/// fires, whichever comes first. Callers wake it immediately by calling
/// `notify.notify_one()` after any change the shim already knows about
/// (e.g. a new `CronJob` created via the API) rather than waiting up to
/// `FALLBACK_INTERVAL` for the change to be noticed.
///
/// Intended to be spawned once as a background task from `main.rs`, after
/// `startup::recover()` has already run.
pub async fn run(pool: SqlitePool, notify: Arc<Notify>) {
    run_with_interval(pool, notify, FALLBACK_INTERVAL).await
}

async fn run_with_interval(pool: SqlitePool, notify: Arc<Notify>, interval_duration: Duration) {
    let mut interval = tokio::time::interval(interval_duration);

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = notify.notified() => {}
        }

        if let Err(err) = tick(&pool).await {
            tracing::error!("reconciliation tick failed: {err:?}");
        }
    }
}

/// One reconciliation pass: check every `CronJob`'s schedule for a due
/// run, then advance every non-terminal job by one state.
pub async fn tick(pool: &SqlitePool) -> Result<()> {
    let scheduled = schedule::schedule_due_jobs(pool).await?;
    let advanced = job::advance_all(pool).await?;
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
    use std::time::Instant;

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
            run_with_interval(loop_pool, loop_notify, Duration::from_secs(30)).await;
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

        tick(&pool).await.unwrap();

        let status: String =
            sqlx::query_scalar("SELECT status FROM jobs WHERE cronjob_name = 'every-minute'")
                .fetch_one(&pool)
                .await
                .unwrap();
        // Created by schedule_due_jobs(), then immediately advanced one
        // step by job::advance_all() in the same tick.
        assert_eq!(status, "VolumePending");
    }
}
