//! Removes finished Jobs the way Kubernetes does: a Job whose spec sets
//! `ttlSecondsAfterFinished` is deleted that many seconds after it
//! finished, and a CronJob keeps only its newest
//! `successfulJobsHistoryLimit` succeeded and `failedJobsHistoryLimit`
//! failed runs (defaults 3 and 1, as in Kubernetes).
//!
//! This only affects database rows. A job's worker VM and volume are
//! torn down as soon as the job ends, whatever these settings say; a
//! row is removed only once it reaches `Archived`, so removing it never
//! leaves a VM behind. A finished run that is still cleaning up counts
//! towards its CronJob's history limit (it is one of the newest runs)
//! but is left alone until it is archived.
//!
//! Removing a Job drops its cached logs, events and metrics. What the
//! cost report, the status page's total cost and the CronJob scheduler
//! need is first copied to `pruned_jobs`, so billing records stay
//! complete and a CronJob whose every run was removed doesn't think it
//! has never run.

use crate::reconcile::job::{is_pre_completion_state, RUN_FAILED_SQL, TERMINAL_STATE};
use anyhow::Result;
use serde_json::Value as JsonValue;
use sqlx::{Row, SqlitePool};

/// Kubernetes' defaults for a CronJob's history limits.
const DEFAULT_SUCCESSFUL_JOBS_HISTORY_LIMIT: i64 = 3;
const DEFAULT_FAILED_JOBS_HISTORY_LIMIT: i64 = 1;

/// Removes every archived Job that `ttlSecondsAfterFinished` or its
/// CronJob's history limits say should go. Returns how many were removed.
pub async fn prune_finished_jobs(pool: &SqlitePool, now: i64) -> Result<usize> {
    let mut ids = expired_ttl_jobs(pool, now).await?;
    for id in over_history_limit_jobs(pool).await? {
        if !ids.contains(&id) {
            ids.push(id);
        }
    }
    for id in &ids {
        prune_job(pool, id, now).await?;
    }
    if !ids.is_empty() {
        tracing::info!("removed {} finished job(s) from history", ids.len());
    }
    Ok(ids.len())
}

/// Archived Jobs whose `ttlSecondsAfterFinished` has passed. A job that
/// never recorded `completed_at` (it was force-failed before its
/// container finished) counts as finished when it was archived.
async fn expired_ttl_jobs(pool: &SqlitePool, now: i64) -> Result<Vec<String>> {
    let ids = sqlx::query_scalar(
        "SELECT id FROM jobs \
         WHERE status = ? \
           AND json_valid(spec) \
           AND json_type(spec, '$.ttlSecondsAfterFinished') = 'integer' \
           AND json_extract(spec, '$.ttlSecondsAfterFinished') >= 0 \
           AND COALESCE(completed_at, updated_at) \
               + json_extract(spec, '$.ttlSecondsAfterFinished') <= ?",
    )
    .bind(TERMINAL_STATE)
    .bind(now)
    .fetch_all(pool)
    .await?;
    Ok(ids)
}

/// Archived runs beyond their CronJob's history limits. Runs of a
/// CronJob that no longer exists are left alone.
async fn over_history_limit_jobs(pool: &SqlitePool) -> Result<Vec<String>> {
    let cronjobs = sqlx::query("SELECT name, namespace, spec FROM cronjobs")
        .fetch_all(pool)
        .await?;

    let mut ids = Vec::new();
    for cronjob in cronjobs {
        let name: String = cronjob.get(0);
        let namespace: String = cronjob.get(1);
        let spec_str: String = cronjob.get(2);
        let spec: JsonValue = serde_json::from_str(&spec_str).unwrap_or(JsonValue::Null);
        let successful_limit = history_limit(
            &spec,
            "successfulJobsHistoryLimit",
            DEFAULT_SUCCESSFUL_JOBS_HISTORY_LIMIT,
        );
        let failed_limit = history_limit(
            &spec,
            "failedJobsHistoryLimit",
            DEFAULT_FAILED_JOBS_HISTORY_LIMIT,
        );

        let runs = sqlx::query(&format!(
            "SELECT id, status, {RUN_FAILED_SQL} FROM jobs \
             WHERE cronjob_name = ? AND namespace = ? \
             ORDER BY created_at DESC, id DESC"
        ))
        .bind(&name)
        .bind(&namespace)
        .fetch_all(pool)
        .await?;

        let (mut kept_successful, mut kept_failed) = (0, 0);
        for run in runs {
            let id: String = run.get(0);
            let status: String = run.get(1);
            let failed: bool = run.get(2);
            if is_pre_completion_state(&status) {
                continue;
            }
            let (kept, limit) = if failed {
                (&mut kept_failed, failed_limit)
            } else {
                (&mut kept_successful, successful_limit)
            };
            if *kept < limit {
                *kept += 1;
            } else if status == TERMINAL_STATE {
                ids.push(id);
            }
        }
    }
    Ok(ids)
}

/// A history limit from a CronJob spec; missing, `null` or invalid means
/// the Kubernetes default.
fn history_limit(spec: &JsonValue, field: &str, default: i64) -> i64 {
    spec.get(field)
        .and_then(JsonValue::as_i64)
        .filter(|limit| *limit >= 0)
        .unwrap_or(default)
}

/// Removes one job's row, its events, metrics and volume records, after
/// copying what billing and scheduling need to `pruned_jobs`. Also used
/// by `kubectl delete job` on a finished job, so a deleted job's cost
/// stays in the cost report.
pub(crate) async fn prune_job(pool: &SqlitePool, job_id: &str, now: i64) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        "INSERT OR REPLACE INTO pruned_jobs \
         (id, name, namespace, cronjob_name, spec, created_at, completed_at, worker_plan, \
          actual_cost, pruned_at) \
         SELECT id, name, namespace, cronjob_name, spec, created_at, completed_at, worker_plan, \
          actual_cost, ? FROM jobs WHERE id = ?",
    )
    .bind(now)
    .bind(job_id)
    .execute(&mut *tx)
    .await?;
    for table in ["events", "worker_metrics", "job_volumes"] {
        sqlx::query(&format!("DELETE FROM {table} WHERE job_id = ?"))
            .bind(job_id)
            .execute(&mut *tx)
            .await?;
    }
    sqlx::query("DELETE FROM jobs WHERE id = ?")
        .bind(job_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    async fn insert_cronjob(pool: &SqlitePool, name: &str, extra: JsonValue) {
        let mut spec = json!({"schedule": "* * * * *", "jobTemplate": {"spec": {}}});
        for (key, value) in extra.as_object().unwrap() {
            spec[key] = value.clone();
        }
        sqlx::query(
            "INSERT INTO cronjobs (id, name, namespace, spec, schedule, created_at, updated_at, version) \
             VALUES (?, ?, 'default', ?, '* * * * *', 0, 0, 1)",
        )
        .bind(format!("cj-{name}"))
        .bind(name)
        .bind(spec.to_string())
        .execute(pool)
        .await
        .unwrap();
    }

    /// A run of `cronjob` (or a standalone job, for `None`) created at
    /// `created_at`, finished one second later with `exit_code`.
    async fn insert_job(
        pool: &SqlitePool,
        id: &str,
        cronjob: Option<&str>,
        status: &str,
        exit_code: i64,
        created_at: i64,
        spec: JsonValue,
    ) {
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, cronjob_name, spec, status, exit_code, \
             cached_logs, actual_cost, created_at, completed_at, updated_at, version) \
             VALUES (?, ?, 'default', ?, ?, ?, ?, 'some logs', 0.01, ?, ?, ?, 1)",
        )
        .bind(id)
        .bind(id)
        .bind(cronjob)
        .bind(spec.to_string())
        .bind(status)
        .bind(exit_code)
        .bind(created_at)
        .bind(created_at + 1)
        .bind(created_at + 2)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn job_ids(pool: &SqlitePool) -> Vec<String> {
        sqlx::query_scalar("SELECT id FROM jobs ORDER BY id")
            .fetch_all(pool)
            .await
            .unwrap()
    }

    async fn pruned_ids(pool: &SqlitePool) -> Vec<String> {
        sqlx::query_scalar("SELECT id FROM pruned_jobs ORDER BY id")
            .fetch_all(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_default_limits_keep_three_succeeded_and_one_failed() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_cronjob(&pool, "cj", json!({})).await;
        for i in 1..=5 {
            insert_job(
                &pool,
                &format!("ok{i}"),
                Some("cj"),
                "Archived",
                0,
                i * 100,
                json!({}),
            )
            .await;
            insert_job(
                &pool,
                &format!("bad{i}"),
                Some("cj"),
                "Archived",
                1,
                i * 100 + 50,
                json!({}),
            )
            .await;
        }

        assert_eq!(prune_finished_jobs(&pool, 10_000).await.unwrap(), 6);
        assert_eq!(job_ids(&pool).await, vec!["bad5", "ok3", "ok4", "ok5"]);
        assert_eq!(
            pruned_ids(&pool).await,
            vec!["bad1", "bad2", "bad3", "bad4", "ok1", "ok2"]
        );
    }

    #[tokio::test]
    async fn test_explicit_limits_including_zero() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_cronjob(
            &pool,
            "cj",
            json!({"successfulJobsHistoryLimit": 0, "failedJobsHistoryLimit": 2}),
        )
        .await;
        insert_job(&pool, "ok1", Some("cj"), "Archived", 0, 100, json!({})).await;
        for i in 1..=3 {
            insert_job(
                &pool,
                &format!("bad{i}"),
                Some("cj"),
                "Archived",
                1,
                i * 100 + 50,
                json!({}),
            )
            .await;
        }

        prune_finished_jobs(&pool, 10_000).await.unwrap();
        assert_eq!(job_ids(&pool).await, vec!["bad2", "bad3"]);
    }

    #[tokio::test]
    async fn test_running_and_cleaning_up_runs_are_never_removed() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_cronjob(&pool, "cj", json!({"successfulJobsHistoryLimit": 1})).await;
        insert_job(&pool, "old", Some("cj"), "Archived", 0, 100, json!({})).await;
        // Finished and still tearing down its VM: counts as the newest
        // successful run, so "old" goes, but it stays itself.
        insert_job(
            &pool,
            "cleaning",
            Some("cj"),
            "VMTerminating",
            0,
            200,
            json!({}),
        )
        .await;
        insert_job(
            &pool,
            "running",
            Some("cj"),
            "ContainerRunning",
            0,
            300,
            json!({}),
        )
        .await;

        prune_finished_jobs(&pool, 10_000).await.unwrap();
        assert_eq!(job_ids(&pool).await, vec!["cleaning", "running"]);

        // Once a newer run is archived, the one still cleaning up is over
        // the limit too, but is left until it is archived itself.
        insert_job(&pool, "newest", Some("cj"), "Archived", 0, 400, json!({})).await;
        prune_finished_jobs(&pool, 10_000).await.unwrap();
        assert_eq!(job_ids(&pool).await, vec!["cleaning", "newest", "running"]);
    }

    #[tokio::test]
    async fn test_ttl_seconds_after_finished() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        // Finished (completed_at) at 101.
        insert_job(
            &pool,
            "ttl60",
            None,
            "Archived",
            0,
            100,
            json!({"ttlSecondsAfterFinished": 60}),
        )
        .await;
        insert_job(
            &pool,
            "ttl0",
            None,
            "Archived",
            1,
            100,
            json!({"ttlSecondsAfterFinished": 0}),
        )
        .await;
        insert_job(&pool, "no-ttl", None, "Archived", 0, 100, json!({})).await;
        insert_job(
            &pool,
            "cleaning",
            None,
            "VolumeDetaching",
            0,
            100,
            json!({"ttlSecondsAfterFinished": 0}),
        )
        .await;

        prune_finished_jobs(&pool, 160).await.unwrap();
        assert_eq!(job_ids(&pool).await, vec!["cleaning", "no-ttl", "ttl60"]);

        prune_finished_jobs(&pool, 161).await.unwrap();
        assert_eq!(job_ids(&pool).await, vec!["cleaning", "no-ttl"]);
    }

    #[tokio::test]
    async fn test_ttl_applies_to_cronjob_runs_too() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_cronjob(&pool, "cj", json!({})).await;
        insert_job(
            &pool,
            "run",
            Some("cj"),
            "Archived",
            0,
            100,
            json!({"ttlSecondsAfterFinished": 10}),
        )
        .await;
        prune_finished_jobs(&pool, 1_000).await.unwrap();
        assert!(job_ids(&pool).await.is_empty());
    }

    #[tokio::test]
    async fn test_runs_of_a_deleted_cronjob_are_kept() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        for i in 1..=5 {
            insert_job(
                &pool,
                &format!("ok{i}"),
                Some("gone"),
                "Archived",
                0,
                i * 100,
                json!({}),
            )
            .await;
        }
        assert_eq!(prune_finished_jobs(&pool, 10_000).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_prune_job_removes_related_rows_and_keeps_the_billing_record() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job(&pool, "j1", Some("cj"), "Archived", 0, 100, json!({})).await;
        sqlx::query(
            "INSERT INTO events (id, job_id, reason, message, timestamp) \
             VALUES ('e1', 'j1', 'Started', 'x', 0), ('e2', NULL, 'BudgetSkip', 'y', 0)",
        )
        .execute(&pool)
        .await
        .unwrap();

        prune_job(&pool, "j1", 500).await.unwrap();

        assert!(job_ids(&pool).await.is_empty());
        let events: Vec<String> = sqlx::query_scalar("SELECT id FROM events")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(events, vec!["e2"]);
        let (name, cronjob, completed_at, cost, pruned_at): (String, String, i64, f64, i64) =
            sqlx::query_as(
                "SELECT name, cronjob_name, completed_at, actual_cost, pruned_at FROM pruned_jobs",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            (
                name.as_str(),
                cronjob.as_str(),
                completed_at,
                cost,
                pruned_at
            ),
            ("j1", "cj", 101, 0.01, 500)
        );
    }
}
