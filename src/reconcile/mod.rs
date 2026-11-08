//! Reconciliation loop skeleton (Phase 6): the state machine that will
//! eventually drive all real orchestration. Every transition today is
//! mocked -- no external API calls, matching this phase's own scope --
//! but the loop's own architecture (fallback poll + `Notify` wake-up,
//! startup recovery, per-job version bookkeeping) is the real thing later
//! phases build directly on top of, not a throwaway prototype.

pub mod history;
pub mod job;
pub mod metrics_collector;
pub mod orphan_scan;
pub mod pricing;
pub mod schedule;
pub mod startup;

pub use job::JobContext;
pub(crate) use job::STATE_SEQUENCE;

use crate::shutdown::Shutdown;
use crate::ssh;
use anyhow::Result;
use chrono::Utc;
use sqlx::SqlitePool;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
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

/// How often `reconcile::metrics_collector` SSHes into every currently
/// `ContainerRunning` worker for a real `podman stats` + machine-health
/// sample -- "real metrics, not fake numbers," per the user's own
/// request that replaced `api::metrics`'s original design (estimated
/// from `resources.requests`). 30s matches the cadence real Kubernetes
/// metrics-server itself typically samples at by default, not picked
/// arbitrarily.
const METRICS_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// How often `reconcile::pricing` re-syncs UpCloud's real pricing
/// catalog and the ECB's daily reference rates (Phase 14a) -- both
/// genuinely only change at most once a day at the source, so a daily
/// cadence matches reality rather than polling needlessly often.
const PRICING_SYNC_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

/// How long since the job-tick loop's own last pass (success *or*
/// failure -- a tick that returned an `Err` still proves the loop
/// itself is alive and responsive, not hung) before `/readyz` (Phase
/// 15) considers it unhealthy. Generous relative to `FALLBACK_INTERVAL`
/// (10s) so one slow tick (a real UpCloud call taking a few seconds)
/// doesn't flap readiness, while still catching a genuinely
/// hung/panicked loop within a minute.
const TICK_STALE_THRESHOLD_SECS: i64 = 60;

/// Shared with the public status page (Phase 15) so `/readyz` can
/// report a real signal -- whether the reconciliation loop is
/// actually still ticking -- rather than a hardcoded `200 OK`.
/// `Arc<TickHealth>` is cloned into both `run` (which records every
/// tick) and `app::build_router`'s public sub-router (which only ever
/// reads it). Plain atomics, not behind a `Mutex`: one writer per
/// field (the loop that owns it), many readers (concurrent
/// `/readyz`/`/metrics` requests), no need for anything heavier.
///
/// Phase 18 widened this from "when did the job tick last run" to the
/// per-loop counters `/metrics` (`metrics_export`) serves -- it was
/// already the one object shared between every background loop and the
/// public router, so the self-instrumentation rides along on it instead
/// of threading a second `Arc` through the same two places.
pub struct TickHealth {
    /// Unix seconds this process started (well, when this was built,
    /// which `main.rs` does right at startup) -- for
    /// `kubeshim_process_start_time_seconds`/uptime.
    started_at: i64,
    /// The job-tick loop (`run_with_interval`).
    pub(crate) tick: LoopStats,
    /// `run_orphan_scan_loop`.
    pub(crate) orphan_scan: LoopStats,
    /// `run_metrics_poll_loop`.
    pub(crate) metrics_poll: LoopStats,
    /// `run_pricing_sync_loop`.
    pub(crate) pricing_sync: LoopStats,
}

impl Default for TickHealth {
    fn default() -> Self {
        Self::new()
    }
}

impl TickHealth {
    pub fn new() -> Self {
        Self {
            started_at: Utc::now().timestamp(),
            tick: LoopStats::default(),
            orphan_scan: LoopStats::default(),
            metrics_poll: LoopStats::default(),
            pricing_sync: LoopStats::default(),
        }
    }

    /// `pub(crate)`: also called directly by `status_page`'s own tests
    /// to simulate a tick having landed, without needing to spin up
    /// the real loop. Records a successful, zero-duration tick.
    #[cfg(test)]
    pub(crate) fn record(&self) {
        self.tick.record(true, Duration::ZERO);
    }

    pub(crate) fn started_at(&self) -> i64 {
        self.started_at
    }

    /// `false` both before the very first tick has ever landed (the
    /// loop hasn't actually started doing anything yet -- a real,
    /// if brief, "not ready") and once the most recent one is older
    /// than `TICK_STALE_THRESHOLD_SECS`.
    pub fn is_healthy(&self) -> bool {
        let last = self.tick.last_run_at();
        last != 0 && Utc::now().timestamp() - last < TICK_STALE_THRESHOLD_SECS
    }
}

/// One background loop's own run counters, for `/metrics` (Phase 18).
/// Durations are kept in whole microseconds so they fit an atomic
/// integer; `/metrics` converts back to seconds.
#[derive(Default)]
pub(crate) struct LoopStats {
    last_run_at: AtomicI64,
    successes: AtomicU64,
    failures: AtomicU64,
    duration_micros_total: AtomicU64,
}

impl LoopStats {
    /// Called once per pass, success or failure -- a failed pass still
    /// proves the loop itself is alive, which is what `/readyz` needs.
    pub(crate) fn record(&self, success: bool, elapsed: Duration) {
        self.last_run_at
            .store(Utc::now().timestamp(), Ordering::Relaxed);
        if success {
            self.successes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failures.fetch_add(1, Ordering::Relaxed);
        }
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        self.duration_micros_total
            .fetch_add(micros, Ordering::Relaxed);
    }

    /// Unix seconds of the most recent pass, or `0` if none yet.
    pub(crate) fn last_run_at(&self) -> i64 {
        self.last_run_at.load(Ordering::Relaxed)
    }

    pub(crate) fn successes(&self) -> u64 {
        self.successes.load(Ordering::Relaxed)
    }

    pub(crate) fn failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }

    pub(crate) fn duration_seconds_total(&self) -> f64 {
        self.duration_micros_total.load(Ordering::Relaxed) as f64 / 1e6
    }
}

/// Runs until `shutdown` is requested, driving the reconciliation loop:
/// one pass immediately,
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
///
/// On shutdown (issue #118) no loop is cut off in the middle of a pass
/// that changes cloud resources: the job loop finishes the step it is on
/// and leaves the remaining jobs for the next start, and the orphan scan
/// finishes its current pass. The metrics poll and pricing sync only read
/// from outside, so they are dropped mid-pass. Returns once every loop
/// has stopped.
pub async fn run(
    pool: SqlitePool,
    notify: Arc<Notify>,
    ctx: JobContext,
    tick_health: Arc<TickHealth>,
    shutdown: Shutdown,
) {
    let orphan_scan = tokio::spawn(run_orphan_scan_loop(
        pool.clone(),
        ctx.clone(),
        ORPHAN_SCAN_INTERVAL,
        tick_health.clone(),
        shutdown.clone(),
    ));
    let metrics_poll = tokio::spawn(run_metrics_poll_loop(
        pool.clone(),
        ctx.clone(),
        METRICS_POLL_INTERVAL,
        tick_health.clone(),
        shutdown.clone(),
    ));
    let pricing_sync = tokio::spawn(run_pricing_sync_loop(
        pool.clone(),
        ctx.clone(),
        PRICING_SYNC_INTERVAL,
        tick_health.clone(),
        shutdown.clone(),
    ));
    run_with_interval(pool, notify, FALLBACK_INTERVAL, ctx, tick_health, shutdown).await;

    // The job loop has stopped; wait for the others to reach their own
    // safe points too, so `main` knows nothing is mid-operation.
    for (name, handle) in [
        ("orphan scan", orphan_scan),
        ("metrics poll", metrics_poll),
        ("pricing sync", pricing_sync),
    ] {
        if let Err(err) = handle.await {
            tracing::error!("{name} loop ended abnormally: {err}");
        }
    }
    tracing::info!("reconciliation loops stopped");
}

/// One pass immediately (matching the job loop's own eager-first-tick
/// behavior -- useful for catching a leak left over from before the shim
/// last restarted), then one every `interval_duration` thereafter, with
/// no `Notify` wake-up: nothing the shim does itself needs an untracked
/// volume cleaned up sooner than the next scheduled scan.
async fn run_orphan_scan_loop(
    pool: SqlitePool,
    ctx: JobContext,
    interval_duration: Duration,
    tick_health: Arc<TickHealth>,
    shutdown: Shutdown,
) {
    let mut interval = tokio::time::interval(interval_duration);

    loop {
        tokio::select! {
            biased;
            _ = shutdown.requested() => return,
            _ = interval.tick() => {}
        }

        let start = Instant::now();
        let result = orphan_scan::scan_and_clean(&pool, &ctx).await;
        tick_health
            .orphan_scan
            .record(result.is_ok(), start.elapsed());
        match result {
            Ok(cleaned) if cleaned > 0 => {
                tracing::warn!("orphan scan: cleaned up {cleaned} untracked volume(s)")
            }
            Ok(_) => {}
            Err(err) => tracing::error!("orphan scan failed: {err:?}"),
        }
    }
}

/// Same shape as `run_orphan_scan_loop`: one pass immediately, then one
/// every `interval_duration` thereafter, no `Notify` wake-up (a job
/// becoming `ContainerRunning` doesn't need its first real sample any
/// sooner than the next scheduled poll).
async fn run_metrics_poll_loop(
    pool: SqlitePool,
    ctx: JobContext,
    interval_duration: Duration,
    tick_health: Arc<TickHealth>,
    shutdown: Shutdown,
) {
    let ssh_config = ssh::WorkerSshConfig {
        private_key: ctx.worker_ssh_private_key.clone(),
        port: ctx.worker_ssh_port,
    };
    let mut interval = tokio::time::interval(interval_duration);

    loop {
        tokio::select! {
            biased;
            _ = shutdown.requested() => return,
            _ = interval.tick() => {}
        }

        let start = Instant::now();
        // Only reads from workers and records samples, so unlike the
        // other loops it may be cut off mid-pass: a lost sample is
        // harmless, and SSH timeouts could otherwise hold up shutdown.
        let result = tokio::select! {
            biased;
            _ = shutdown.requested() => return,
            result = metrics_collector::poll_once(&pool, &ssh_config) => result,
        };
        tick_health
            .metrics_poll
            .record(result.is_ok(), start.elapsed());
        if let Err(err) = result {
            tracing::error!("metrics poll failed: {err:?}");
        }
    }
}

/// Same shape as `run_orphan_scan_loop`: one pass immediately (so a
/// freshly-started shim has real pricing to estimate the very first
/// job's cost against, not an empty cache for up to a day), then one
/// every `interval_duration` thereafter.
async fn run_pricing_sync_loop(
    pool: SqlitePool,
    ctx: JobContext,
    interval_duration: Duration,
    tick_health: Arc<TickHealth>,
    shutdown: Shutdown,
) {
    let mut interval = tokio::time::interval(interval_duration);

    loop {
        tokio::select! {
            biased;
            _ = shutdown.requested() => return,
            _ = interval.tick() => {}
        }

        let start = Instant::now();
        // Only reads from UpCloud and the ECB, and writes each table in
        // one transaction, so it may be cut off mid-pass: the next start
        // syncs again right away.
        let result = tokio::select! {
            biased;
            _ = shutdown.requested() => return,
            result = pricing::sync_pricing_and_rates(&pool, &ctx) => result,
        };
        tick_health
            .pricing_sync
            .record(result.is_ok(), start.elapsed());
        if let Err(err) = result {
            tracing::error!("pricing/exchange-rate sync failed: {err:?}");
        }
    }
}

async fn run_with_interval(
    pool: SqlitePool,
    notify: Arc<Notify>,
    interval_duration: Duration,
    ctx: JobContext,
    tick_health: Arc<TickHealth>,
    shutdown: Shutdown,
) {
    let mut interval = tokio::time::interval(interval_duration);

    loop {
        tokio::select! {
            biased;
            _ = shutdown.requested() => return,
            _ = interval.tick() => {}
            _ = notify.notified() => {}
        }

        let start = Instant::now();
        let result = tick_until(&pool, &ctx, &shutdown).await;
        // Recorded regardless of success/failure -- a tick that
        // returned an Err still proves the loop itself is alive and
        // responsive, which is what /readyz (Phase 15) actually needs
        // to know. /metrics (Phase 18) tells the two apart.
        tick_health.tick.record(result.is_ok(), start.elapsed());
        if let Err(err) = result {
            tracing::error!("reconciliation tick failed: {err:?}");
        }
    }
}

/// One reconciliation pass: check every `CronJob`'s schedule for a due
/// run, advance every non-terminal job by one state, then remove
/// archived jobs that `ttlSecondsAfterFinished` or their CronJob's
/// history limits no longer keep (`history`). Orphan
/// scanning is *not* part of this pass -- it runs on its own slower
/// cadence, see `run_orphan_scan_loop`/`ORPHAN_SCAN_INTERVAL`.
pub async fn tick(pool: &SqlitePool, ctx: &JobContext) -> Result<()> {
    tick_until(pool, ctx, &Shutdown::never()).await
}

/// Like [`tick`], but stops advancing jobs once `shutdown` is requested,
/// after the job it is working on (see `job::advance_all_until`).
async fn tick_until(pool: &SqlitePool, ctx: &JobContext, shutdown: &Shutdown) -> Result<()> {
    let scheduled = schedule::schedule_due_jobs(pool, ctx).await?;
    let advanced = job::advance_all_until(pool, ctx, shutdown).await?;
    let pruned = history::prune_finished_jobs(pool, Utc::now().timestamp()).await?;
    if scheduled > 0 || advanced > 0 || pruned > 0 {
        tracing::debug!(
            "reconciliation tick: scheduled {scheduled} new job(s), advanced {advanced}, \
             removed {pruned} from history"
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
            worker_template_uuid: "01000000-0000-4000-8000-000030240200".to_string(),
            worker_ssh_public_keys: vec![],
            own_public_ip: None,
            worker_ssh_private_key: String::new(),
            worker_ssh_port: crate::ssh::SSH_PORT,
            main_currency: "EUR".to_string(),
            plan_filter: Default::default(),
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
            run_with_interval(
                loop_pool,
                loop_notify,
                Duration::from_secs(30),
                mock_ctx(),
                Arc::new(TickHealth::new()),
                Shutdown::never(),
            )
            .await;
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

    /// Issue #118: an idle loop returns as soon as shutdown is requested,
    /// rather than waiting out its interval or running forever.
    #[tokio::test]
    async fn test_loops_return_promptly_on_shutdown() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let (trigger, shutdown) = crate::shutdown::channel();
        let job_loop = tokio::spawn(run_with_interval(
            pool.clone(),
            Arc::new(Notify::new()),
            Duration::from_secs(3600),
            mock_ctx(),
            Arc::new(TickHealth::new()),
            shutdown.clone(),
        ));
        let orphan_loop = tokio::spawn(run_orphan_scan_loop(
            pool,
            mock_ctx(),
            Duration::from_secs(3600),
            Arc::new(TickHealth::new()),
            shutdown,
        ));
        // Let both eager first passes run, so the loops are idle.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let start = Instant::now();
        trigger.trigger();
        tokio::time::timeout(Duration::from_secs(1), job_loop)
            .await
            .expect("job loop did not stop within 1s of shutdown")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), orphan_loop)
            .await
            .expect("orphan scan loop did not stop within 1s of shutdown")
            .unwrap();
        assert!(start.elapsed() < Duration::from_secs(1));
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
            worker_template_uuid: "01000000-0000-4000-8000-000030240200".to_string(),
            worker_ssh_public_keys: vec![],
            own_public_ip: None,
            worker_ssh_private_key: String::new(),
            worker_ssh_port: crate::ssh::SSH_PORT,
            main_currency: "EUR".to_string(),
            plan_filter: Default::default(),
        };

        // A short interval stands in for ORPHAN_SCAN_INTERVAL here --
        // waiting out the real 5 minutes would make this test useless.
        tokio::spawn(run_orphan_scan_loop(
            pool,
            ctx,
            Duration::from_millis(20),
            Arc::new(TickHealth::new()),
            Shutdown::never(),
        ));

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
