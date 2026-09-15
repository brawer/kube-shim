use anyhow::{Context, Result};
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use std::path::Path;

pub async fn init_pool<P: AsRef<Path>>(db_path: P) -> Result<SqlitePool> {
    let db_url = format!(
        "sqlite://{}",
        db_path.as_ref().display()
    );

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .context("Failed to connect to SQLite database")?;

    // Run migrations
    run_migrations(&pool).await?;

    Ok(pool)
}

async fn run_migrations(pool: &SqlitePool) -> Result<()> {
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
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='secrets'"
        )
        .fetch_one(&pool)
        .await
        .expect("Failed to query");

        assert_eq!(result.0, 1);
    }
}
