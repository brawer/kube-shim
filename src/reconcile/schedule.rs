//! Turns a `CronJob`'s schedule into actual job runs (Phase 6) -- the
//! shim-internal equivalent of real Kubernetes' cronjob controller. Pure
//! DB/cron-math, no external API calls, matching this phase's scope.

use anyhow::Result;
use chrono::{DateTime, Utc};
use cron::Schedule;
use serde_json::Value as JsonValue;
use sqlx::{Row, SqlitePool};
use std::str::FromStr;
use uuid::Uuid;

/// Kubernetes `CronJob.spec.schedule` uses standard 5-field POSIX cron
/// syntax (minute hour day-of-month month day-of-week, e.g. `"0 2 * * 0"`)
/// -- the `cron` crate wants a 6-field form with seconds first. Prepending
/// `"0 "` is the standard bridge between the two; this project doesn't
/// need sub-minute scheduling, so a fixed `0` seconds field is exact, not
/// an approximation.
fn to_six_field(schedule: &str) -> String {
    format!("0 {schedule}")
}

/// Checks every `CronJob`'s schedule and creates a new `jobs` row for any
/// that are due -- "due" meaning at least one scheduled instant falls
/// between the last time a run was created for it (or its own creation
/// time, if it has never run) and now. Only the *most recent* such instant
/// matters: if the shim was down for a while and several instants were
/// missed, this deliberately catches up with a single run rather than
/// bursting one per missed instant (mirrors real Kubernetes' own
/// "coalesce missed schedules" behavior for `Allow`-policy CronJobs,
/// though this project only ever behaves as if `concurrencyPolicy: Allow`
/// -- there's no support for `Forbid`/`Replace` here, since nothing else
/// in this plan gives a reason to need them).
pub async fn schedule_due_jobs(pool: &SqlitePool) -> Result<usize> {
    let cronjobs =
        sqlx::query("SELECT id, name, namespace, spec, schedule, created_at FROM cronjobs")
            .fetch_all(pool)
            .await?;

    let now = Utc::now();
    let mut scheduled = 0;

    for row in cronjobs {
        let cronjob_id: String = row.get(0);
        let name: String = row.get(1);
        let namespace: String = row.get(2);
        let spec_str: String = row.get(3);
        let schedule_str: String = row.get(4);
        let created_at: i64 = row.get(5);

        if schedule_str.trim().is_empty() {
            // Shouldn't happen for any CronJob created since this phase's
            // fix to create_cronjob, but don't crash on stale/bad data.
            continue;
        }

        let schedule = match Schedule::from_str(&to_six_field(&schedule_str)) {
            Ok(schedule) => schedule,
            Err(err) => {
                tracing::warn!(
                    "CronJob {namespace}/{name} has an unparseable schedule {schedule_str:?}: {err}"
                );
                continue;
            }
        };

        let last_scheduled = last_scheduled_time(pool, &name, &namespace, created_at).await?;
        let is_due = schedule
            .after(&last_scheduled)
            .next()
            .is_some_and(|next| next <= now);
        if !is_due {
            continue;
        }

        create_job_run(pool, &cronjob_id, &name, &namespace, &spec_str, now).await?;
        scheduled += 1;
    }

    Ok(scheduled)
}

/// The most recent instant a run was actually created for this CronJob,
/// derived from the `jobs` table itself rather than stored in a separate
/// `last_scheduled` column on `cronjobs`. Deliberately, on its own
/// merits -- not because adding a column would have been hard (`src/db/
/// migrations.rs` handles that safely now): a dedicated column would be
/// redundant state that only one code path (this one, right here) ever
/// writes, so it could never legitimately drift from `MAX(jobs.
/// created_at)` -- one fewer thing to keep in sync for zero benefit
/// today. It also sidesteps a real question a dedicated column would
/// force: once Phase 13's budget guard exists, a due schedule can be
/// held in `BudgetWait` *before* a `jobs` row exists for it, so "due" and
/// "a run was created" stop being the same instant -- a stored column
/// would need an explicit answer for which of those two moments it
/// tracks. Deriving from `jobs.created_at` answers that by construction:
/// it only ever reflects runs that were actually created.
async fn last_scheduled_time(
    pool: &SqlitePool,
    cronjob_name: &str,
    namespace: &str,
    cronjob_created_at: i64,
) -> Result<DateTime<Utc>> {
    let row: (Option<i64>,) =
        sqlx::query_as("SELECT MAX(created_at) FROM jobs WHERE cronjob_name = ? AND namespace = ?")
            .bind(cronjob_name)
            .bind(namespace)
            .fetch_one(pool)
            .await?;

    let timestamp = row.0.unwrap_or(cronjob_created_at);
    Ok(DateTime::from_timestamp(timestamp, 0).unwrap_or_else(Utc::now))
}

async fn create_job_run(
    pool: &SqlitePool,
    cronjob_id: &str,
    cronjob_name: &str,
    namespace: &str,
    cronjob_spec_str: &str,
    now: DateTime<Utc>,
) -> Result<()> {
    let _ = cronjob_id; // not needed yet; kept for a future FK-style reference
    let cronjob_spec: JsonValue = serde_json::from_str(cronjob_spec_str).unwrap_or(JsonValue::Null);
    let job_template_spec = cronjob_spec
        .pointer("/jobTemplate/spec")
        .cloned()
        .unwrap_or(JsonValue::Null);

    let job_id = Uuid::new_v4().to_string();
    let job_name = format!("{cronjob_name}-{}", now.timestamp());
    let job_spec_json = serde_json::to_string(&job_template_spec)?;
    let now_ts = now.timestamp();

    sqlx::query(
        r#"
        INSERT INTO jobs (id, name, namespace, cronjob_name, spec, status, created_at, updated_at, version)
        VALUES (?, ?, ?, ?, ?, 'Created', ?, ?, 1)
        "#,
    )
    .bind(&job_id)
    .bind(&job_name)
    .bind(namespace)
    .bind(cronjob_name)
    .bind(&job_spec_json)
    .bind(now_ts)
    .bind(now_ts)
    .execute(pool)
    .await?;

    tracing::info!(
        "scheduled new job run {namespace}/{job_name} for CronJob {namespace}/{cronjob_name}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    async fn insert_cronjob(pool: &SqlitePool, name: &str, schedule: &str, created_at: i64) {
        let spec = json!({
            "schedule": schedule,
            "jobTemplate": {"spec": {"activeDeadlineSeconds": 3600}}
        });
        sqlx::query(
            "INSERT INTO cronjobs (id, name, namespace, spec, schedule, created_at, updated_at, version) \
             VALUES (?, ?, 'default', ?, ?, ?, ?, 1)",
        )
        .bind(format!("cj-{name}"))
        .bind(name)
        .bind(spec.to_string())
        .bind(schedule)
        .bind(created_at)
        .bind(created_at)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_due_every_minute_schedule_creates_a_job() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        // "created" far enough in the past that an every-minute schedule
        // is guaranteed to have a due instant by now.
        let created_at = Utc::now().timestamp() - 120;
        insert_cronjob(&pool, "every-minute", "* * * * *", created_at).await;

        let scheduled = schedule_due_jobs(&pool).await.unwrap();
        assert_eq!(scheduled, 1);

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE cronjob_name = 'every-minute'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_not_due_yet_creates_nothing() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        // Just created, with a schedule that won't fire again for a year.
        insert_cronjob(&pool, "yearly", "0 0 1 1 *", Utc::now().timestamp()).await;

        let scheduled = schedule_due_jobs(&pool).await.unwrap();
        assert_eq!(scheduled, 0);
    }

    #[tokio::test]
    async fn test_already_scheduled_recently_is_not_scheduled_again() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let created_at = Utc::now().timestamp() - 120;
        insert_cronjob(&pool, "every-minute", "* * * * *", created_at).await;

        let first = schedule_due_jobs(&pool).await.unwrap();
        assert_eq!(first, 1);

        // Immediately re-running the check must not create a second job
        // for the same due instant -- last_scheduled_time() now reflects
        // the job just created.
        let second = schedule_due_jobs(&pool).await.unwrap();
        assert_eq!(second, 0);
    }

    #[tokio::test]
    async fn test_empty_schedule_is_skipped_not_fatal() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_cronjob(&pool, "no-schedule", "", Utc::now().timestamp()).await;

        let scheduled = schedule_due_jobs(&pool).await.unwrap();
        assert_eq!(scheduled, 0);
    }

    #[tokio::test]
    async fn test_unparseable_schedule_is_skipped_not_fatal() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_cronjob(
            &pool,
            "bogus",
            "not a cron expression",
            Utc::now().timestamp(),
        )
        .await;

        let scheduled = schedule_due_jobs(&pool).await.unwrap();
        assert_eq!(scheduled, 0);
    }

    #[tokio::test]
    async fn test_job_run_carries_the_job_template_spec() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let created_at = Utc::now().timestamp() - 120;
        insert_cronjob(&pool, "every-minute", "* * * * *", created_at).await;

        schedule_due_jobs(&pool).await.unwrap();

        let spec_str: String =
            sqlx::query_scalar("SELECT spec FROM jobs WHERE cronjob_name = 'every-minute'")
                .fetch_one(&pool)
                .await
                .unwrap();
        let spec: JsonValue = serde_json::from_str(&spec_str).unwrap();
        assert_eq!(spec["activeDeadlineSeconds"], 3600);
    }
}
