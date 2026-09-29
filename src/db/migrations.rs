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
const ADDED_COLUMNS: &[(&str, &str, &str)] = &[("jobs", "cached_logs", "TEXT")];

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
