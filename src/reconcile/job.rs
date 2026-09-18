//! Job state machine (Phase 6). Every transition here is mocked -- no
//! external API calls, per this phase's own scope -- one state per
//! reconciliation tick, purely to prove the loop's plumbing (DB reads/
//! writes, retry/version bookkeeping, logging) works end to end. Phase 7+
//! replaces each mocked step with a real one against `CloudProvider`,
//! without needing to change this module's overall shape.

use anyhow::Result;
use chrono::Utc;
use sqlx::{Row, SqlitePool};

/// The full pipeline a job run passes through, matching "Reconciliation
/// Loop State Machine" under Key Implementation Details in
/// docs/IMPLEMENTATION_PLAN.md -- minus `BudgetWait` (Phase 13 doesn't
/// exist yet, so nothing ever waits on budget) and collapsing the real
/// `Succeeded`/`Failed` fork down to always `Succeeded` (there's no real
/// container execution yet to have an outcome to report -- Phase 8+
/// determines this for real).
const STATE_SEQUENCE: &[&str] = &[
    "Created",
    "VolumePending",
    "VolumeCreating",
    "VolumeCreated",
    "VolumeAttaching",
    "VolumeAttached",
    "VMPending",
    "VMCreating",
    "VMRunning",
    "ContainerRunning",
    "Succeeded",
    "VolumeDetaching",
    "VolumeDeleted",
    "Archived",
];

/// `Archived` is the only true terminal state in this pipeline -- even a
/// mocked "Succeeded" run still continues on to cleanup states, matching
/// how a real job run's lifecycle doesn't stop just because the workload
/// itself finished.
pub const TERMINAL_STATE: &str = "Archived";

/// The state one tick after `current`, or `None` if `current` is
/// `TERMINAL_STATE` or not a state this pipeline recognizes at all (e.g.
/// leftover data from a different schema version -- callers should leave
/// such a job alone and log a warning rather than guess).
pub fn next_state(current: &str) -> Option<&'static str> {
    let index = STATE_SEQUENCE.iter().position(|state| *state == current)?;
    STATE_SEQUENCE.get(index + 1).copied()
}

/// Advances every job not yet in `TERMINAL_STATE` by exactly one state.
/// Returns how many jobs were advanced (for logging/testing).
pub async fn advance_all(pool: &SqlitePool) -> Result<usize> {
    let rows = sqlx::query("SELECT id, name, namespace, status FROM jobs WHERE status != ?")
        .bind(TERMINAL_STATE)
        .fetch_all(pool)
        .await?;

    let mut advanced = 0;
    for row in rows {
        let id: String = row.get(0);
        let name: String = row.get(1);
        let namespace: String = row.get(2);
        let status: String = row.get(3);

        let Some(next) = next_state(&status) else {
            tracing::warn!(
                "job {namespace}/{name} is in unrecognized state {status:?}, leaving it alone"
            );
            continue;
        };

        let now = Utc::now().timestamp();
        sqlx::query(
            r#"
            UPDATE jobs
            SET status = ?, last_transition_time = ?, updated_at = ?, version = version + 1
            WHERE id = ?
            "#,
        )
        .bind(next)
        .bind(now)
        .bind(now)
        .bind(&id)
        .execute(pool)
        .await?;

        tracing::info!("job {namespace}/{name}: {status} -> {next}");
        advanced += 1;
    }

    Ok(advanced)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_state_walks_the_full_sequence() {
        let mut state = "Created";
        let mut steps = 0;
        while let Some(next) = next_state(state) {
            state = next;
            steps += 1;
            assert!(steps < 100, "state sequence should terminate");
        }
        assert_eq!(state, TERMINAL_STATE);
        assert_eq!(steps, STATE_SEQUENCE.len() - 1);
    }

    #[test]
    fn test_next_state_terminal_has_no_successor() {
        assert_eq!(next_state(TERMINAL_STATE), None);
    }

    #[test]
    fn test_next_state_unknown_state_returns_none() {
        assert_eq!(next_state("SomeStateFromAFutureSchemaVersion"), None);
    }

    #[tokio::test]
    async fn test_advance_all_moves_each_job_one_step() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'Created', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool).await.unwrap();
        assert_eq!(advanced, 1);

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "VolumePending");
    }

    #[tokio::test]
    async fn test_advance_all_ignores_terminal_jobs() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'Archived', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool).await.unwrap();
        assert_eq!(advanced, 0);
    }

    #[tokio::test]
    async fn test_advance_all_increments_version() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'Created', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        advance_all(&pool).await.unwrap();

        let version: i64 = sqlx::query_scalar("SELECT version FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(version, 2);
    }
}
