//! Startup recovery (Phase 6): surface jobs a previous shim crash left
//! mid-flight. Every non-terminal job re-enters the normal reconciliation
//! loop on the very next tick automatically, exactly like any other
//! in-progress job -- `advance_all` (Phase 7-11) is what actually does
//! real cleanup/idempotent-retry/deadline work, since its own checks are
//! wall-clock- and database-timestamp-based and therefore already
//! correct whether or not the shim was down in between. This module's
//! own job is purely observational: make a crash, and specifically a
//! deadline that already passed while nobody was watching, visible in
//! the logs the moment the process comes back up, rather than silently
//! waiting for the first regular tick to notice it moments later.

use crate::reconcile::job::TERMINAL_STATE;
use anyhow::Result;
use chrono::Utc;
use serde_json::Value as JsonValue;
use sqlx::{Row, SqlitePool};

/// Logs every job not yet in a terminal state at startup -- specifically
/// calling out any whose `activeDeadlineSeconds` had *already* elapsed
/// while the shim was down, since that's the one case where "resuming
/// reconciliation" actually means "about to be force-failed on the very
/// next tick" rather than genuinely continuing. Returns how many
/// non-terminal jobs were found in total, so `main.rs` can report it too.
pub async fn recover(pool: &SqlitePool) -> Result<usize> {
    let rows =
        sqlx::query("SELECT name, namespace, status, spec, created_at FROM jobs WHERE status != ?")
            .bind(TERMINAL_STATE)
            .fetch_all(pool)
            .await?;

    let now = Utc::now().timestamp();
    for row in &rows {
        let name: String = row.get(0);
        let namespace: String = row.get(1);
        let status: String = row.get(2);
        let spec_str: String = row.get(3);
        let created_at: i64 = row.get(4);

        let spec: JsonValue = serde_json::from_str(&spec_str).unwrap_or(JsonValue::Null);
        let deadline = spec
            .get("activeDeadlineSeconds")
            .and_then(JsonValue::as_i64);

        match deadline {
            Some(deadline) if now - created_at > deadline => {
                tracing::warn!(
                    "startup recovery: job {namespace}/{name} was left in state {status} by a \
                     previous run, and its activeDeadlineSeconds ({deadline}s) already elapsed \
                     while this shim was down -- will be force-failed on the next tick"
                );
            }
            _ => {
                tracing::info!(
                    "startup recovery: job {namespace}/{name} was left in state {status} by a \
                     previous run -- resuming reconciliation"
                );
            }
        }
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
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) VALUES \
             ('j1', 'job-one', 'default', '{}', 'VolumePending', ?, ?, 1), \
             ('j2', 'job-two', 'default', '{}', 'Archived', ?, ?, 1)",
        )
        .bind(now)
        .bind(now)
        .bind(now)
        .bind(now)
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

    #[tokio::test]
    async fn test_recover_flags_a_deadline_already_missed_while_down() {
        // Doesn't assert on log output directly (this crate doesn't wire
        // up a test subscriber) -- just confirms recover() still
        // completes normally and counts the job, since the interesting
        // behavior here is the distinct warn!-level message, which is a
        // real log entry inspected in the real hands-on verification
        // (docs/IMPLEMENTATION_PLAN.md Phase 11), not unit-testable
        // without adding log-capture machinery this project doesn't
        // otherwise need.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = Utc::now().timestamp();
        let spec = serde_json::json!({"activeDeadlineSeconds": 60}).to_string();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'ContainerRunning', ?, ?, 1)",
        )
        .bind(spec)
        .bind(now - 1000)
        .bind(now - 1000)
        .execute(&pool)
        .await
        .unwrap();

        let recovered = recover(&pool).await.unwrap();
        assert_eq!(recovered, 1);
    }
}
