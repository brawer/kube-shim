//! Real schema migrations for columns added to *already-existing* tables.
//!
//! `db/mod.rs`'s own schema setup only ever re-runs `CREATE TABLE IF NOT
//! EXISTS`/`CREATE INDEX IF NOT EXISTS` from `schema.sql` on every
//! startup -- which is exactly right for a brand new table, but a total
//! no-op against a table that already exists. Editing an existing
//! `CREATE TABLE` in `schema.sql` to add a column therefore does nothing
//! for a database that was created before that edit -- discovered while
//! building Phase 6 (see docs/IMPLEMENTATION_PLAN.md), which sidestepped
//! needing a new column rather than fix the underlying gap. This module
//! is that fix: a small, explicit list of columns to add to existing
//! tables, applied idempotently (checked via `PRAGMA table_info` first)
//! on every startup, so it's safe to run against both a database that
//! predates the column and a fresh one where `schema.sql` already
//! includes it.
//!
//! New columns on an existing table go here, not by editing that table's
//! `CREATE TABLE` in `schema.sql` after it's already shipped.

use anyhow::{Context, Result};
use sqlx::{Row, SqlitePool};

/// `(table, column, type-and-constraints)`. First real entry (Phase 10):
/// `jobs.cached_logs` holds a job's full `podman logs` output, captured
/// once via SSH right after real completion is detected
/// (`reconcile::job`'s `ContainerRunning` handler) -- so `kubectl logs`
/// still works once the worker VM itself is gone (`VMTerminating`
/// deletes it a few states later), the same way a real Kubernetes pod's
/// logs remain fetchable for a while after the pod exits.
const ADDED_COLUMNS: &[(&str, &str, &str)] = &[
    ("jobs", "cached_logs", "TEXT"),
    // Phase 14a: a job's worst-case cost (computed once, at creation,
    // from its activeDeadlineSeconds) and its real cost (computed once
    // real completion is detected), both in `main_currency`. Nullable:
    // either can be unset if pricing/exchange-rate caches weren't
    // synced yet when they were needed -- see `pricing::` module docs.
    ("jobs", "estimated_cost", "REAL"),
    ("jobs", "actual_cost", "REAL"),
    // The real moment completion was detected -- distinct from
    // `updated_at` (bumped again by every later cleanup-tail
    // transition) -- so `api::cost_report` (Phase 14a) has an accurate
    // `ChargePeriodEnd`, not an approximation from a column that keeps
    // moving after the job itself actually finished.
    ("jobs", "completed_at", "INTEGER"),
    // Phase 16: the real UpCloud title a job's ephemeral volume was
    // created with -- same reasoning as `jobs.worker_vm_name` (already
    // tracked since Phase 9), for the same "easy lookup without
    // re-deriving the naming convention by hand" reason, now extended
    // to volumes too.
    ("job_volumes", "provider_volume_name", "TEXT"),
    // Issue #85: the server plan a job's worker VM was really created
    // with. Plan choice now depends on the synced catalog and prices,
    // so actual cost, the cost report and the status page read this
    // instead of re-deriving a plan that might differ by then. NULL
    // until the VM is created (or for jobs from before this column).
    ("jobs", "worker_plan", "TEXT"),
    // Phase 18: a worker VM's virtual-memory stats, from /proc/vmstat
    // (cumulative counters since boot) and /proc/meminfo (bytes), for
    // `/metrics`. All NULL when that part of a sample didn't parse --
    // see `reconcile::metrics_collector`'s own docs.
    ("worker_metrics", "pgfault", "INTEGER"),
    ("worker_metrics", "pgmajfault", "INTEGER"),
    ("worker_metrics", "pswpin", "INTEGER"),
    ("worker_metrics", "pswpout", "INTEGER"),
    ("worker_metrics", "mapped_bytes", "INTEGER"),
    ("worker_metrics", "cached_bytes", "INTEGER"),
    ("worker_metrics", "anon_bytes", "INTEGER"),
    // Issue #131: whether `jobs.cached_logs` starts every line with a
    // timestamp (`podman logs --timestamps`), which `api::logs` strips
    // unless the request asks for `timestamps=true`. 0 for logs cached
    // before that change, and for a worker's start-error text.
    (
        "jobs",
        "cached_logs_timestamped",
        "INTEGER NOT NULL DEFAULT 0",
    ),
    // Whether a removed run failed (`reconcile::job::RUN_FAILED_SQL`,
    // evaluated while its events still exist), so the status page can
    // still show a CronJob's last result after its row is gone. NULL
    // for runs removed before this column existed.
    ("pruned_jobs", "failed", "INTEGER"),
];

pub async fn run(pool: &SqlitePool) -> Result<()> {
    apply_added_columns(pool, ADDED_COLUMNS).await
}

async fn apply_added_columns(
    pool: &SqlitePool,
    added_columns: &[(&str, &str, &str)],
) -> Result<()> {
    for (table, column, definition) in added_columns {
        if column_exists(pool, table, column).await? {
            continue;
        }

        let sql = format!("ALTER TABLE {table} ADD COLUMN {column} {definition}");
        sqlx::query(&sql)
            .execute(pool)
            .await
            .with_context(|| format!("Failed to run migration: {sql}"))?;
        tracing::info!("Migration: added column {table}.{column}");
    }
    Ok(())
}

async fn column_exists(pool: &SqlitePool, table: &str, column: &str) -> Result<bool> {
    // PRAGMA statements don't accept bound parameters in SQLite; `table`
    // only ever comes from the hardcoded ADDED_COLUMNS list above, never
    // from user input, so interpolating it directly is safe here.
    let rows = sqlx::query(&format!("PRAGMA table_info({table})"))
        .fetch_all(pool)
        .await
        .with_context(|| format!("Failed to inspect schema of table {table}"))?;

    Ok(rows
        .iter()
        .any(|row| row.get::<String, _>("name") == column))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_adds_a_missing_column() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        assert!(!column_exists(&pool, "jobs", "test_column").await.unwrap());

        apply_added_columns(&pool, &[("jobs", "test_column", "TEXT")])
            .await
            .unwrap();

        assert!(column_exists(&pool, "jobs", "test_column").await.unwrap());
    }

    #[tokio::test]
    async fn test_is_idempotent_when_column_already_exists() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let migration: &[(&str, &str, &str)] = &[("jobs", "test_column", "TEXT")];

        apply_added_columns(&pool, migration).await.unwrap();
        // A second run must not error (e.g. "duplicate column name") --
        // this is exactly the case a fresh database hits, where
        // schema.sql already created the column from the start.
        apply_added_columns(&pool, migration).await.unwrap();

        assert!(column_exists(&pool, "jobs", "test_column").await.unwrap());
    }

    #[tokio::test]
    async fn test_preserves_existing_data() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'Created', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        apply_added_columns(&pool, &[("jobs", "test_column", "TEXT")])
            .await
            .unwrap();

        let name: String = sqlx::query_scalar("SELECT name FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(name, "job-one");
    }

    #[tokio::test]
    async fn test_init_pool_adds_the_real_added_columns() {
        // init_pool() (src/db/mod.rs) already calls run() as part of its
        // own normal startup sequence -- this exercises that real,
        // production path end to end against the real ADDED_COLUMNS list,
        // rather than just apply_added_columns() directly against a
        // hand-picked test column as this module's other tests do.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        assert!(column_exists(&pool, "jobs", "cached_logs").await.unwrap());
    }
}
