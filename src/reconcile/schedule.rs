//! Turns a `CronJob`'s schedule into actual job runs (Phase 6) -- the
//! shim-internal equivalent of real Kubernetes' cronjob controller.
//! Mostly DB/cron-math, plus one real network-adjacent check (Phase
//! 14b): the rolling budget guard, consulted before a run is created
//! at all.

use crate::pricing;
use crate::reconcile::job::{active_deadline_seconds, is_pre_completion_state, record_event};
use crate::reconcile::JobContext;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
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
/// "coalesce missed schedules" behavior).
///
/// Issue #105 adds the scheduling fields of `CronJobSpec`, with
/// Kubernetes' semantics:
///
/// - **`suspend: true`** creates no new runs; runs already started keep
///   going. Instants that pass while suspended count as missed, so on
///   unsuspending, the usual catch-up applies (subject to the deadline).
/// - **`startingDeadlineSeconds`** drops a due instant once it's more
///   than that many seconds old -- see `is_due`.
/// - **`concurrencyPolicy: Forbid`** holds a due run back while an
///   earlier run of the same CronJob is still active. Nothing is
///   recorded, so the instant stays due and is picked up once the
///   earlier run finishes, if the deadline still allows. `Replace` is
///   rejected at admission (`admission::validate_cronjob_schedule_fields`).
///
/// **A due schedule whose estimated cost the current budget can't
/// cover is skipped, not created and left to wait (Phase 14b)** --
/// see `create_job_run`'s own docs for why, and why this needs no
/// extra retry logic of its own: `last_scheduled_time` derives from
/// `MAX(jobs.created_at)`, so skipping a run leaves it unchanged, and
/// the very next tick sees the exact same instant as still due.
pub async fn schedule_due_jobs(pool: &SqlitePool, ctx: &JobContext) -> Result<usize> {
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

        let spec: JsonValue = serde_json::from_str(&spec_str).unwrap_or(JsonValue::Null);
        if spec.get("suspend").and_then(JsonValue::as_bool) == Some(true) {
            continue;
        }

        let last_scheduled = last_scheduled_time(pool, &name, &namespace, created_at).await?;
        let starting_deadline = spec
            .get("startingDeadlineSeconds")
            .and_then(JsonValue::as_i64);
        if !is_due(&schedule, last_scheduled, now, starting_deadline) {
            continue;
        }

        if spec.get("concurrencyPolicy").and_then(JsonValue::as_str) == Some("Forbid")
            && has_active_run(pool, &name, &namespace).await?
        {
            tracing::debug!(
                "CronJob {namespace}/{name} is due, but its previous run is still active \
                 (concurrencyPolicy: Forbid)"
            );
            continue;
        }

        if create_job_run(pool, ctx, &cronjob_id, &name, &namespace, &spec_str, now).await? {
            scheduled += 1;
        }
    }

    Ok(scheduled)
}

/// Whether a run is due: some scheduled instant falls after
/// `last_scheduled` and no later than `now`. With a starting deadline,
/// that instant must also be at most `starting_deadline` seconds old
/// (Kubernetes: "If the difference is higher than that limit, it will
/// skip this execution"), so the search starts at whichever is later.
/// Either way only the existence of such an instant matters, never how
/// many there were, so this never walks through a long backlog of
/// missed instants (e.g. after months of `suspend: true`).
fn is_due(
    schedule: &Schedule,
    last_scheduled: DateTime<Utc>,
    now: DateTime<Utc>,
    starting_deadline: Option<i64>,
) -> bool {
    let window_start = match starting_deadline {
        // `after` is exclusive, so start one second early to keep an
        // instant exactly `deadline` seconds old.
        Some(deadline) => Duration::try_seconds(deadline.saturating_add(1))
            .and_then(|deadline| now.checked_sub_signed(deadline))
            .map_or(last_scheduled, |earliest| last_scheduled.max(earliest)),
        None => last_scheduled,
    };
    schedule
        .after(&window_start)
        .next()
        .is_some_and(|next| next <= now)
}

/// Whether a run of this CronJob is still before completion (anything
/// up to `ContainerRunning`), for `concurrencyPolicy: Forbid`. A run
/// that has finished but is still cleaning up (detaching its volume,
/// deleting its VM) doesn't count, as a finished Job doesn't in
/// Kubernetes.
async fn has_active_run(pool: &SqlitePool, cronjob_name: &str, namespace: &str) -> Result<bool> {
    let statuses: Vec<String> =
        sqlx::query_scalar("SELECT status FROM jobs WHERE cronjob_name = ? AND namespace = ?")
            .bind(cronjob_name)
            .bind(namespace)
            .fetch_all(pool)
            .await?;
    Ok(statuses
        .iter()
        .any(|status| is_pre_completion_state(status)))
}

/// The most recent instant a run was actually created for this CronJob,
/// derived from the `jobs` table itself rather than stored in a separate
/// `last_scheduled` column on `cronjobs`. Deliberately, on its own
/// merits -- not because adding a column would have been hard (`src/db/
/// migrations.rs` handles that safely now): a dedicated column would be
/// redundant state that only one code path (this one, right here) ever
/// writes, so it could never legitimately drift from `MAX(jobs.
/// created_at)` -- one fewer thing to keep in sync for zero benefit
/// today.
///
/// It also turns out to give `create_job_run`'s budget-skip (Phase
/// 14b) a free, correct retry for nothing: a due instant whose run
/// gets skipped (insufficient budget) never creates a row, so this
/// stays unchanged, and the very next tick's `is_due` check sees that
/// exact same instant as still due -- a budget-starved CronJob
/// effectively retries every reconciliation tick until the balance
/// recovers, not just at its own next scheduled instant, with zero
/// extra code required for that.
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

/// Creates the run, unless the budget guard (Phase 14b) rejects it --
/// the same admission-time check `api::job::create_job` uses, applied
/// here instead of a `BudgetWait` state the run would otherwise sit
/// in after being created anyway. Returns whether a row was actually
/// created (`schedule_due_jobs` only counts those); a skip is logged
/// at `warn` *and* recorded as a job-independent `Warning` event (same
/// `record_event(pool, None, ...)` shape `api::budget`'s own events
/// use) -- there's no job object to attach it to, since none was
/// created, but it must still be queryable (Phase 15's public status
/// page surfaces recent events; a skip that only ever hit a log line
/// would be invisible there, indistinguishable from a healthy
/// CronJob that simply has nothing due yet). See `last_scheduled_time`'s
/// own docs for why skipping needs no extra retry logic of its own.
async fn create_job_run(
    pool: &SqlitePool,
    ctx: &JobContext,
    cronjob_id: &str,
    cronjob_name: &str,
    namespace: &str,
    cronjob_spec_str: &str,
    now: DateTime<Utc>,
) -> Result<bool> {
    let _ = cronjob_id; // not needed yet; kept for a future FK-style reference
    let cronjob_spec: JsonValue = serde_json::from_str(cronjob_spec_str).unwrap_or(JsonValue::Null);
    let job_template_spec = cronjob_spec
        .pointer("/jobTemplate/spec")
        .cloned()
        .unwrap_or(JsonValue::Null);

    let estimated_cost = if let Some(deadline) = active_deadline_seconds(&job_template_spec) {
        let main_currency = pricing::effective_main_currency(pool, &ctx.main_currency).await?;
        let (estimated_cost, sufficient) = pricing::check_budget(
            pool,
            &ctx.zone,
            &job_template_spec,
            deadline,
            &main_currency,
        )
        .await?;
        if !sufficient {
            let message = format!(
                "CronJob {namespace}/{cronjob_name}: skipping this run, estimated cost \
                 ({:.4} {main_currency}) exceeds the current budget balance -- will retry \
                 next tick",
                estimated_cost.unwrap_or(0.0)
            );
            tracing::warn!("{message}");
            record_event(pool, None, "BudgetSkip", &message, "Warning").await?;
            return Ok(false);
        }
        estimated_cost
    } else {
        None
    };

    let job_id = Uuid::new_v4().to_string();
    let job_name = format!("{cronjob_name}-{}", now.timestamp());
    let job_spec_json = serde_json::to_string(&job_template_spec)?;
    let now_ts = now.timestamp();

    sqlx::query(
        r#"
        INSERT INTO jobs (id, name, namespace, cronjob_name, spec, status, estimated_cost, created_at, updated_at, version)
        VALUES (?, ?, ?, ?, ?, 'Created', ?, ?, ?, 1)
        "#,
    )
    .bind(&job_id)
    .bind(&job_name)
    .bind(namespace)
    .bind(cronjob_name)
    .bind(&job_spec_json)
    .bind(estimated_cost)
    .bind(now_ts)
    .bind(now_ts)
    .execute(pool)
    .await?;

    tracing::info!(
        "scheduled new job run {namespace}/{job_name} for CronJob {namespace}/{cronjob_name}"
    );
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::upcloud::UpCloudProvider;
    use serde_json::json;
    use std::sync::Arc;

    fn mock_ctx() -> JobContext {
        JobContext {
            provider: Arc::new(UpCloudProvider::new("unused-in-dry-run")),
            dry_run: true,
            resource_prefix: "kube-shim-test".to_string(),
            zone: "de-fra1".to_string(),
            worker_template_uuid: "01000000-0000-4000-8000-000030240200".to_string(),
            worker_ssh_public_keys: vec![],
            own_public_ip: None,
            worker_ssh_private_key: String::new(),
            worker_ssh_port: crate::ssh::SSH_PORT,
            main_currency: "EUR".to_string(),
            plan_filter: Default::default(),
        }
    }

    async fn insert_cronjob(pool: &SqlitePool, name: &str, schedule: &str, created_at: i64) {
        insert_cronjob_with(pool, name, schedule, created_at, json!({})).await;
    }

    /// Like `insert_cronjob`, with extra top-level spec fields (`suspend`,
    /// `startingDeadlineSeconds`, `concurrencyPolicy`).
    async fn insert_cronjob_with(
        pool: &SqlitePool,
        name: &str,
        schedule: &str,
        created_at: i64,
        extra: JsonValue,
    ) {
        let mut spec = json!({
            "schedule": schedule,
            "jobTemplate": {"spec": {"activeDeadlineSeconds": 3600}}
        });
        for (key, value) in extra.as_object().unwrap() {
            spec[key] = value.clone();
        }
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

        let scheduled = schedule_due_jobs(&pool, &mock_ctx()).await.unwrap();
        assert_eq!(scheduled, 1);

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE cronjob_name = 'every-minute'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 1);
    }

    async fn seed_budget(pool: &SqlitePool, balance: f64, daily_rate: f64) {
        sqlx::query(
            "INSERT INTO budget_state \
             (id, balance, last_accrual_at, main_currency, budget_daily_rate, budget_rollover_cap_days) \
             VALUES (1, ?, ?, 'EUR', ?, 7)",
        )
        .bind(balance)
        .bind(Utc::now().timestamp())
        .bind(daily_rate)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_price(pool: &SqlitePool) {
        sqlx::query(
            "INSERT INTO provider_pricing (zone, price_key, amount, price, currency, fetched_at) \
             VALUES ('de-fra1', 'server_plan_DEV-1xCPU-1GB-10GB', 1.0, 0.4464, 'EUR', 0)",
        )
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_due_run_skipped_when_budget_insufficient() {
        // The admission-time budget check (Phase 14b) applies here too,
        // not just to api::job::create_job -- a due run that can't be
        // afforded is skipped (no row created at all), not created and
        // left in a BudgetWait-style state.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        crate::workload::seed_test_plans(&pool).await;
        insert_price(&pool).await;
        seed_budget(&pool, 0.0, 2.0).await;
        let created_at = Utc::now().timestamp() - 120;
        insert_cronjob(&pool, "every-minute", "* * * * *", created_at).await;

        let scheduled = schedule_due_jobs(&pool, &mock_ctx()).await.unwrap();
        assert_eq!(scheduled, 0);

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE cronjob_name = 'every-minute'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 0);

        // No job row exists to attach an event to, but the skip must
        // still be queryable (Phase 15's status page surfaces recent
        // events) -- not just a log line nothing else can see.
        let (reason, event_type, job_id): (String, String, Option<String>) =
            sqlx::query_as("SELECT reason, type, job_id FROM events WHERE reason = 'BudgetSkip'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(reason, "BudgetSkip");
        assert_eq!(event_type, "Warning");
        assert_eq!(job_id, None);
    }

    #[tokio::test]
    async fn test_skipped_run_retries_on_the_next_tick_once_budget_recovers() {
        // last_scheduled_time derives from MAX(jobs.created_at) -- a
        // skipped run (no row created) leaves it unchanged, so the
        // exact same due instant is retried on the very next call,
        // with no extra retry bookkeeping needed.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        crate::workload::seed_test_plans(&pool).await;
        insert_price(&pool).await;
        seed_budget(&pool, 0.0, 2.0).await;
        let created_at = Utc::now().timestamp() - 120;
        insert_cronjob(&pool, "every-minute", "* * * * *", created_at).await;

        assert_eq!(schedule_due_jobs(&pool, &mock_ctx()).await.unwrap(), 0);

        pricing::apply_topup(&pool, 10.0).await.unwrap();

        assert_eq!(schedule_due_jobs(&pool, &mock_ctx()).await.unwrap(), 1);
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE cronjob_name = 'every-minute'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_due_run_stores_estimated_cost_when_budget_covers_it() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        crate::workload::seed_test_plans(&pool).await;
        insert_price(&pool).await;
        seed_budget(&pool, 10.0, 2.0).await;
        let created_at = Utc::now().timestamp() - 120;
        insert_cronjob(&pool, "every-minute", "* * * * *", created_at).await;

        assert_eq!(schedule_due_jobs(&pool, &mock_ctx()).await.unwrap(), 1);

        let estimated_cost: Option<f64> = sqlx::query_scalar(
            "SELECT estimated_cost FROM jobs WHERE cronjob_name = 'every-minute'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        // 1 hour (the test CronJob's own activeDeadlineSeconds) at the
        // DEV plan's own cents/hour rate.
        assert!((estimated_cost.unwrap() - 0.004464).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_not_due_yet_creates_nothing() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        // Just created, with a schedule that won't fire again for a year.
        insert_cronjob(&pool, "yearly", "0 0 1 1 *", Utc::now().timestamp()).await;

        let scheduled = schedule_due_jobs(&pool, &mock_ctx()).await.unwrap();
        assert_eq!(scheduled, 0);
    }

    #[tokio::test]
    async fn test_already_scheduled_recently_is_not_scheduled_again() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let created_at = Utc::now().timestamp() - 120;
        insert_cronjob(&pool, "every-minute", "* * * * *", created_at).await;

        let first = schedule_due_jobs(&pool, &mock_ctx()).await.unwrap();
        assert_eq!(first, 1);

        // Immediately re-running the check must not create a second job
        // for the same due instant -- last_scheduled_time() now reflects
        // the job just created.
        let second = schedule_due_jobs(&pool, &mock_ctx()).await.unwrap();
        assert_eq!(second, 0);
    }

    #[tokio::test]
    async fn test_empty_schedule_is_skipped_not_fatal() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_cronjob(&pool, "no-schedule", "", Utc::now().timestamp()).await;

        let scheduled = schedule_due_jobs(&pool, &mock_ctx()).await.unwrap();
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

        let scheduled = schedule_due_jobs(&pool, &mock_ctx()).await.unwrap();
        assert_eq!(scheduled, 0);
    }

    #[tokio::test]
    async fn test_job_run_carries_the_job_template_spec() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let created_at = Utc::now().timestamp() - 120;
        insert_cronjob(&pool, "every-minute", "* * * * *", created_at).await;

        schedule_due_jobs(&pool, &mock_ctx()).await.unwrap();

        let spec_str: String =
            sqlx::query_scalar("SELECT spec FROM jobs WHERE cronjob_name = 'every-minute'")
                .fetch_one(&pool)
                .await
                .unwrap();
        let spec: JsonValue = serde_json::from_str(&spec_str).unwrap();
        assert_eq!(spec["activeDeadlineSeconds"], 3600);
    }

    async fn job_count(pool: &SqlitePool, cronjob_name: &str) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE cronjob_name = ?")
            .bind(cronjob_name)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339).unwrap().into()
    }

    fn weekly_sunday_3am() -> Schedule {
        Schedule::from_str(&to_six_field("0 3 * * SUN")).unwrap()
    }

    #[test]
    fn test_is_due_without_deadline_catches_up_on_any_missed_instant() {
        // Last run Sunday 2026-11-01 03:00; now Wednesday 2026-11-11, with
        // Sunday 2026-11-08 03:00 missed (e.g. while suspended).
        let last = at("2026-11-01T03:00:00Z");
        let now = at("2026-11-11T12:00:00Z");
        assert!(is_due(&weekly_sunday_3am(), last, now, None));
    }

    #[test]
    fn test_is_due_skips_an_instant_older_than_the_deadline() {
        // Same as above with osmdiffs' startingDeadlineSeconds: 3600 --
        // Sunday's slot is days old, so no run until the next Sunday.
        let last = at("2026-11-01T03:00:00Z");
        let now = at("2026-11-11T12:00:00Z");
        assert!(!is_due(&weekly_sunday_3am(), last, now, Some(3600)));
    }

    #[test]
    fn test_is_due_within_the_deadline() {
        let last = at("2026-11-01T03:00:00Z");
        for now in [
            "2026-11-08T03:00:00Z", // on the dot
            "2026-11-08T03:30:00Z",
            "2026-11-08T04:00:00Z", // exactly 3600 s late: still allowed
        ] {
            assert!(
                is_due(&weekly_sunday_3am(), last, at(now), Some(3600)),
                "{now}"
            );
        }
        assert!(!is_due(
            &weekly_sunday_3am(),
            last,
            at("2026-11-08T04:00:01Z"),
            Some(3600)
        ));
    }

    #[test]
    fn test_is_due_deadline_never_reaches_back_before_the_last_run() {
        // The last run was at 03:00 itself: that instant is used up, even
        // though it's within the deadline.
        let last = at("2026-11-08T03:00:00Z");
        let now = at("2026-11-08T03:10:00Z");
        assert!(!is_due(&weekly_sunday_3am(), last, now, Some(3600)));
        assert!(!is_due(&weekly_sunday_3am(), last, now, None));
    }

    #[test]
    fn test_is_due_huge_deadline_does_not_overflow() {
        let last = at("2026-11-01T03:00:00Z");
        let now = at("2026-11-11T12:00:00Z");
        assert!(is_due(&weekly_sunday_3am(), last, now, Some(i64::MAX)));
    }

    #[tokio::test]
    async fn test_suspended_cronjob_creates_no_jobs() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let created_at = Utc::now().timestamp() - 120;
        insert_cronjob_with(
            &pool,
            "suspended",
            "* * * * *",
            created_at,
            json!({"suspend": true}),
        )
        .await;

        for _ in 0..3 {
            assert_eq!(schedule_due_jobs(&pool, &mock_ctx()).await.unwrap(), 0);
        }
        assert_eq!(job_count(&pool, "suspended").await, 0);
    }

    #[tokio::test]
    async fn test_unsuspending_catches_up_with_one_run() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        // Several every-minute instants pass while suspended.
        let created_at = Utc::now().timestamp() - 300;
        insert_cronjob_with(
            &pool,
            "paused",
            "* * * * *",
            created_at,
            json!({"suspend": true}),
        )
        .await;
        assert_eq!(schedule_due_jobs(&pool, &mock_ctx()).await.unwrap(), 0);

        sqlx::query("UPDATE cronjobs SET spec = json_set(spec, '$.suspend', json('false'))")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(schedule_due_jobs(&pool, &mock_ctx()).await.unwrap(), 1);
        assert_eq!(schedule_due_jobs(&pool, &mock_ctx()).await.unwrap(), 0);
        assert_eq!(job_count(&pool, "paused").await, 1);
    }

    #[tokio::test]
    async fn test_missed_instant_past_the_starting_deadline_is_skipped() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        // Every 1 January: created three years ago, never run, so the
        // last missed instant is months old -- far past a 30 s deadline.
        let created_at = Utc::now().timestamp() - 3 * 366 * 24 * 3600;
        insert_cronjob_with(
            &pool,
            "with-deadline",
            "0 0 1 1 *",
            created_at,
            json!({"startingDeadlineSeconds": 30}),
        )
        .await;
        insert_cronjob(&pool, "without-deadline", "0 0 1 1 *", created_at).await;

        assert_eq!(schedule_due_jobs(&pool, &mock_ctx()).await.unwrap(), 1);
        assert_eq!(job_count(&pool, "with-deadline").await, 0);
        assert_eq!(job_count(&pool, "without-deadline").await, 1);
    }

    async fn insert_run(pool: &SqlitePool, cronjob_name: &str, status: &str, created_at: i64) {
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, cronjob_name, spec, status, created_at, updated_at, version) \
             VALUES (?, ?, 'default', ?, '{}', ?, ?, ?, 1)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(format!("{cronjob_name}-{created_at}"))
        .bind(cronjob_name)
        .bind(status)
        .bind(created_at)
        .bind(created_at)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_forbid_holds_a_due_run_while_the_previous_one_is_active() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = Utc::now().timestamp();
        insert_cronjob_with(
            &pool,
            "forbid",
            "* * * * *",
            now - 600,
            json!({"concurrencyPolicy": "Forbid"}),
        )
        .await;
        insert_cronjob(&pool, "allow", "* * * * *", now - 600).await;
        // Each has a run from 5 minutes ago that is still going.
        insert_run(&pool, "forbid", "ContainerRunning", now - 300).await;
        insert_run(&pool, "allow", "ContainerRunning", now - 300).await;

        assert_eq!(schedule_due_jobs(&pool, &mock_ctx()).await.unwrap(), 1);
        assert_eq!(job_count(&pool, "forbid").await, 1);
        assert_eq!(job_count(&pool, "allow").await, 2);

        // Once the run has finished (cleanup still in progress counts as
        // finished), the held-back instant is picked up.
        sqlx::query("UPDATE jobs SET status = 'VMTerminating' WHERE cronjob_name = 'forbid'")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(schedule_due_jobs(&pool, &mock_ctx()).await.unwrap(), 1);
        assert_eq!(job_count(&pool, "forbid").await, 2);
    }

    #[tokio::test]
    async fn test_suspending_leaves_a_running_job_alone() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = Utc::now().timestamp();
        insert_cronjob_with(
            &pool,
            "busy",
            "* * * * *",
            now - 600,
            json!({"suspend": true}),
        )
        .await;
        insert_run(&pool, "busy", "ContainerRunning", now - 300).await;

        assert_eq!(schedule_due_jobs(&pool, &mock_ctx()).await.unwrap(), 0);
        let status: String =
            sqlx::query_scalar("SELECT status FROM jobs WHERE cronjob_name = 'busy'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "ContainerRunning");
    }
}
