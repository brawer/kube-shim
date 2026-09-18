//! Startup recovery (Phase 6): surface jobs a previous shim crash left
//! mid-flight. Everything in this phase is mocked (no real volumes, VMs,
//! or containers exist yet), so there's nothing to actually clean up --
//! this just makes a crash visible in the logs and confirms those jobs
//! re-enter the normal reconciliation loop on the very next tick, exactly
//! like any other in-progress job. Real crash-safety work (idempotent
//! retries against a real cloud provider, orphan detection) is Phase 10.

use crate::reconcile::job::TERMINAL_STATE;
use anyhow::Result;
use sqlx::{Row, SqlitePool};

/// Logs every job not yet in a terminal state at startup. Returns how many
/// were found, so `main.rs` can report it too.
pub async fn recover(pool: &SqlitePool) -> Result<usize> {
    let rows = sqlx::query("SELECT name, namespace, status FROM jobs WHERE status != ?")
        .bind(TERMINAL_STATE)
        .fetch_all(pool)
        .await?;

    for row in &rows {
        let name: String = row.get(0);
        let namespace: String = row.get(1);
        let status: String = row.get(2);
        tracing::info!(
            "startup recovery: job {namespace}/{name} was left in state {status} by a \
             previous run -- resuming reconciliation"
        );
    }

    if !rows.is_empty() {
        tracing::info!("startup recovery: {} job(s) resumed", rows.len());
    }

    Ok(rows.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_recover_counts_non_terminal_jobs() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) VALUES \
             ('j1', 'job-one', 'default', '{}', 'VolumePending', 0, 0, 1), \
             ('j2', 'job-two', 'default', '{}', 'Archived', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let recovered = recover(&pool).await.unwrap();
        assert_eq!(recovered, 1);
    }

    #[tokio::test]
    async fn test_recover_returns_zero_with_no_jobs() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let recovered = recover(&pool).await.unwrap();
        assert_eq!(recovered, 0);
    }
}
