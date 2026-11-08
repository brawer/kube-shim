mod migrations;

use anyhow::{Context, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::path::Path;
use std::str::FromStr;

pub async fn init_pool<P: AsRef<Path>>(db_path: P) -> Result<SqlitePool> {
    let db_url = format!("sqlite://{}", db_path.as_ref().display());

    // create_if_missing is required: sqlx does not create the database file
    // by default, so a fresh deployment with no pre-existing db.sqlite would
    // otherwise fail to start.
    let options = SqliteConnectOptions::from_str(&db_url)
        .with_context(|| format!("Invalid database path: {}", db_url))?
        .create_if_missing(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await
        .context("Failed to connect to SQLite database")?;

    // Base schema: safe to re-run on every startup, since every statement
    // in schema.sql is a CREATE TABLE/INDEX IF NOT EXISTS -- a no-op
    // against a table that already exists, which is also exactly why it
    // can't handle a column added to an existing table (see
    // db/migrations.rs, run right after, for that).
    enable_incremental_auto_vacuum(&pool).await?;
    run_schema(&pool).await?;
    migrations::run(&pool).await?;

    Ok(pool)
}

/// `PRAGMA auto_vacuum`'s value for `INCREMENTAL`.
const AUTO_VACUUM_INCREMENTAL: i64 = 2;

/// Switches the database to incremental auto-vacuum, so pages freed when
/// `reconcile::history` removes old jobs can be given back to the disk
/// with `PRAGMA incremental_vacuum` (see [`release_free_pages`]) instead
/// of the file staying at its largest size forever. A new database gets
/// the setting before any table exists. An existing one needs a single
/// full `VACUUM` to switch, which runs once, here, and needs free disk
/// space about the size of the database while it runs.
async fn enable_incremental_auto_vacuum(pool: &SqlitePool) -> Result<()> {
    // The setting and the VACUUM that applies it must run on the same
    // connection.
    let mut conn = pool.acquire().await?;
    let mode: i64 = sqlx::query_scalar("PRAGMA auto_vacuum")
        .fetch_one(&mut *conn)
        .await?;
    if mode == AUTO_VACUUM_INCREMENTAL {
        return Ok(());
    }

    sqlx::query("PRAGMA auto_vacuum = INCREMENTAL")
        .execute(&mut *conn)
        .await?;
    tracing::info!("Switching database to incremental auto-vacuum (one-time VACUUM)");
    sqlx::query("VACUUM")
        .execute(&mut *conn)
        .await
        .context("Failed to VACUUM the database to enable incremental auto-vacuum")?;

    let mode: i64 = sqlx::query_scalar("PRAGMA auto_vacuum")
        .fetch_one(&mut *conn)
        .await?;
    if mode != AUTO_VACUUM_INCREMENTAL {
        tracing::warn!("Database auto_vacuum is still {mode} after VACUUM");
    }
    Ok(())
}

/// Gives every free page back to the file system, shrinking the database
/// file. Cheap when there's little to free; called after
/// `reconcile::history` removes jobs. Does nothing on a database that
/// isn't in incremental auto-vacuum mode.
pub async fn release_free_pages(pool: &SqlitePool) -> Result<()> {
    sqlx::query("PRAGMA incremental_vacuum")
        .execute(pool)
        .await?;
    Ok(())
}

async fn run_schema(pool: &SqlitePool) -> Result<()> {
    let schema = include_str!("schema.sql");

    // Execute all statements in schema
    for statement in schema.split(';') {
        let trimmed = statement.trim();
        if !trimmed.is_empty() {
            sqlx::query(trimmed)
                .execute(pool)
                .await
                .context(format!("Failed to execute SQL: {}", trimmed))?;
        }
    }

    tracing::info!("Database schema initialized successfully");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_db_init() {
        let pool = init_pool(":memory:")
            .await
            .expect("Failed to init in-memory DB");

        // Verify tables exist
        let result: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='secrets'",
        )
        .fetch_one(&pool)
        .await
        .expect("Failed to query");

        assert_eq!(result.0, 1);
    }

    async fn auto_vacuum(pool: &SqlitePool) -> i64 {
        sqlx::query_scalar("PRAGMA auto_vacuum")
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn file_pages(pool: &SqlitePool) -> (i64, i64) {
        let pages = sqlx::query_scalar("PRAGMA page_count")
            .fetch_one(pool)
            .await
            .unwrap();
        let free = sqlx::query_scalar("PRAGMA freelist_count")
            .fetch_one(pool)
            .await
            .unwrap();
        (pages, free)
    }

    #[tokio::test]
    async fn test_existing_database_is_switched_to_incremental_and_shrinks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.sqlite");

        // A database created before this change: no auto-vacuum.
        {
            let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))
                .unwrap()
                .create_if_missing(true);
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(options)
                .await
                .unwrap();
            sqlx::query("CREATE TABLE filler (data TEXT)")
                .execute(&pool)
                .await
                .unwrap();
            assert_eq!(auto_vacuum(&pool).await, 0);
            pool.close().await;
        }

        let pool = init_pool(&path).await.unwrap();
        assert_eq!(auto_vacuum(&pool).await, AUTO_VACUUM_INCREMENTAL);

        sqlx::query(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 500) \
             INSERT INTO filler SELECT printf('%.4000c', 'x') FROM n",
        )
        .execute(&pool)
        .await
        .unwrap();
        let (full, _) = file_pages(&pool).await;
        sqlx::query("DELETE FROM filler")
            .execute(&pool)
            .await
            .unwrap();
        let (_, free) = file_pages(&pool).await;
        assert!(
            free > 400,
            "deleted rows should leave free pages, got {free}"
        );

        release_free_pages(&pool).await.unwrap();
        let (pages, free) = file_pages(&pool).await;
        assert_eq!(free, 0);
        assert!(pages < full / 10, "{pages} pages left of {full}");
    }

    #[tokio::test]
    async fn test_new_database_starts_incremental() {
        let dir = tempfile::tempdir().unwrap();
        let pool = init_pool(dir.path().join("db.sqlite")).await.unwrap();
        assert_eq!(auto_vacuum(&pool).await, AUTO_VACUUM_INCREMENTAL);
    }
}
