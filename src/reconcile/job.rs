//! Job state machine. Phase 6 built it fully mocked; Phase 8 makes the
//! two states that don't depend on a real worker VM existing yet
//! (`VolumePending` creates a real ephemeral volume, `VolumeDetaching`
//! deletes it again) actually real. Every other state -- everything
//! VM/container-shaped -- stays mocked until Phase 9 builds a real VM to
//! attach that real volume to.
//!
//! **Why volume attach/detach itself is still mocked, on purpose:**
//! `attach_volume`/`detach_volume` (Phase 7) both require a real server
//! UUID -- there is no worker VM to attach to until Phase 9 exists, so
//! `VolumeAttaching`/`VolumeAttached` stay exactly as mocked as they were
//! in Phase 6. This does mean a real `dry_run=false` job run today
//! creates a real volume, "mock-attaches" it to a VM that was never
//! created, then genuinely deletes the same real volume a few states
//! later -- a real create+delete round trip with a mocked no-op in the
//! middle, not the full real pipeline. That's an intentional, honest
//! reflection of what's actually implemented so far, not a bug to paper
//! over.

use crate::providers::{CloudProvider, CreateVolumeRequest};
use crate::{volumes, workload};
use anyhow::Result;
use chrono::Utc;
use serde_json::Value as JsonValue;
use sqlx::{Row, SqlitePool};
use std::sync::Arc;
use uuid::Uuid;

/// The full pipeline a job run passes through, matching "Reconciliation
/// Loop State Machine" under Key Implementation Details in
/// docs/IMPLEMENTATION_PLAN.md -- minus `BudgetWait` (Phase 13 doesn't
/// exist yet, so nothing ever waits on budget) and collapsing the real
/// `Succeeded`/`Failed` fork down to always `Succeeded` (there's no real
/// container execution yet to have an outcome to report -- Phase 9+
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

/// How long `VolumePending`/`VolumeDetaching` will keep silently retrying
/// (still every tick, just at `warn` level) before escalating to `error`
/// -- observability only, not a give-up-and-clean-up mechanism. That's
/// Phase 10's job; see this module's own top-level docs on staying in
/// scope.
const RETRY_ESCALATION_THRESHOLD: i64 = 5 * 60;

/// Everything a job-advancing tick needs beyond the database: the real
/// `CloudProvider` to call, whether to actually call it, and the naming/
/// placement defaults real calls need. Bundled into one struct (rather
/// than four+ parameters threaded through every function) since every
/// real-work handler below needs the same four things.
#[derive(Clone)]
pub struct JobContext {
    pub provider: Arc<dyn CloudProvider>,
    /// From `config.toml`'s `[upcloud] dry_run`. `VolumePending`/
    /// `VolumeDetaching` log what they *would* do instead of doing it
    /// when this is `true`; every other state stays mocked either way
    /// (see this module's own top-level docs).
    pub dry_run: bool,
    /// From `config.toml`'s `[shim] resource_prefix` (Phase 5) --
    /// threaded through real volume naming now, ahead of Phase 15's own
    /// more thorough naming-convention pass, since a real UpCloud volume
    /// needs *some* real title today.
    pub resource_prefix: String,
    /// From `config.toml`'s `[upcloud] zone` (Phase 7).
    pub zone: String,
}

/// The state one tick after `current`, or `None` if `current` is
/// `TERMINAL_STATE` or not a state this pipeline recognizes at all (e.g.
/// leftover data from a different schema version -- callers should leave
/// such a job alone and log a warning rather than guess).
pub fn next_state(current: &str) -> Option<&'static str> {
    let index = STATE_SEQUENCE.iter().position(|state| *state == current)?;
    STATE_SEQUENCE.get(index + 1).copied()
}

/// Advances every job not yet in `TERMINAL_STATE` by up to one state.
/// Most states advance unconditionally (still fully mocked); the two
/// states doing real work (`VolumePending`, `VolumeDetaching`) only
/// advance once their real `CloudProvider` call actually succeeds --
/// otherwise the job stays put and retries on the next tick. Returns how
/// many jobs were advanced (for logging/testing).
pub async fn advance_all(pool: &SqlitePool, ctx: &JobContext) -> Result<usize> {
    let rows = sqlx::query(
        "SELECT id, name, namespace, status, spec, last_transition_time FROM jobs WHERE status != ?",
    )
    .bind(TERMINAL_STATE)
    .fetch_all(pool)
    .await?;

    let mut advanced = 0;
    for row in rows {
        let id: String = row.get(0);
        let name: String = row.get(1);
        let namespace: String = row.get(2);
        let status: String = row.get(3);
        let spec_str: String = row.get(4);
        let last_transition_time: Option<i64> = row.get(5);

        let Some(next) = next_state(&status) else {
            tracing::warn!(
                "job {namespace}/{name} is in unrecognized state {status:?}, leaving it alone"
            );
            continue;
        };

        let should_advance = match status.as_str() {
            "VolumePending" => {
                handle_volume_pending(
                    pool,
                    ctx,
                    &id,
                    &namespace,
                    &name,
                    &spec_str,
                    last_transition_time,
                )
                .await?
            }
            "VolumeDetaching" => {
                handle_volume_detaching(pool, ctx, &id, &namespace, &name, last_transition_time)
                    .await?
            }
            "VMPending" => {
                log_would_create_vm(&namespace, &name, &spec_str, ctx.dry_run);
                true
            }
            _ => true,
        };

        if !should_advance {
            continue;
        }

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

/// Job `spec` (as stored by `reconcile::schedule::create_job_run`) is the
/// CronJob's `jobTemplate.spec` directly -- `template.spec.volumes[]`,
/// `template.spec.containers[]`, `activeDeadlineSeconds` all live at the
/// top level here, one JSON path segment shorter than in the CronJob's
/// own spec (see `src/admission.rs` for that longer form).
fn find_ephemeral_volume(spec: &JsonValue) -> Option<&JsonValue> {
    spec.pointer("/template/spec/volumes")
        .and_then(JsonValue::as_array)?
        .iter()
        .find_map(|v| v.pointer("/ephemeral/volumeClaimTemplate/spec"))
}

/// `VolumePending`: create the job's ephemeral volume for real (or log
/// what would be created, in dry-run mode). Returns whether the job
/// should advance to `VolumeCreating` this tick.
#[allow(clippy::too_many_arguments)]
async fn handle_volume_pending(
    pool: &SqlitePool,
    ctx: &JobContext,
    job_id: &str,
    namespace: &str,
    name: &str,
    spec_str: &str,
    last_transition_time: Option<i64>,
) -> Result<bool> {
    let spec: JsonValue = serde_json::from_str(spec_str).unwrap_or(JsonValue::Null);
    let Some(volume) = find_ephemeral_volume(&spec) else {
        tracing::debug!("job {namespace}/{name}: no ephemeral volume requested, nothing to create");
        return Ok(true);
    };

    let size_gb = volume
        .pointer("/resources/requests/storage")
        .and_then(JsonValue::as_str)
        .and_then(|q| volumes::parse_storage_quantity_gb(q).ok());
    let tier =
        volumes::StorageTier::parse(volume.get("storageClassName").and_then(JsonValue::as_str))
            .unwrap_or(volumes::StorageTier::Standard);

    let Some(size_gb) = size_gb else {
        tracing::warn!(
            "job {namespace}/{name}: no valid resources.requests.storage found, advancing \
             without creating a volume -- admission-time rejection for this isn't built yet"
        );
        return Ok(true);
    };

    if ctx.dry_run {
        tracing::info!(
            "DRY-RUN: would create volume for job {namespace}/{name}: {size_gb}GB, tier {tier:?}"
        );
        return Ok(true);
    }

    let title = format!("{}-vol-{name}", ctx.resource_prefix);
    let request = CreateVolumeRequest {
        size_gb,
        tier: tier.upcloud_tier().to_string(),
        title,
        zone: ctx.zone.clone(),
    };

    match ctx.provider.create_volume(request).await {
        Ok(volume) => {
            tracing::info!(
                "job {namespace}/{name}: created volume {} ({size_gb}GB, {tier:?})",
                volume.id
            );
            insert_job_volume(pool, job_id, size_gb, tier, &volume.id).await?;
            Ok(true)
        }
        Err(err) => {
            record_failed_attempt(
                pool,
                job_id,
                namespace,
                name,
                &err.to_string(),
                last_transition_time,
            )
            .await?;
            Ok(false)
        }
    }
}

/// `VolumeDetaching`: delete the job's real volume, if one was ever
/// created (a job that never requested one, or whose creation never
/// actually succeeded, just passes through). Returns whether the job
/// should advance to `VolumeDeleted` this tick.
async fn handle_volume_detaching(
    pool: &SqlitePool,
    ctx: &JobContext,
    job_id: &str,
    namespace: &str,
    name: &str,
    last_transition_time: Option<i64>,
) -> Result<bool> {
    let Some(provider_volume_id) = job_volume_id(pool, job_id).await? else {
        tracing::debug!(
            "job {namespace}/{name}: no volume was ever created for this run, nothing to delete"
        );
        return Ok(true);
    };

    if ctx.dry_run {
        tracing::info!(
            "DRY-RUN: would delete volume {provider_volume_id} for job {namespace}/{name}"
        );
        return Ok(true);
    }

    match ctx.provider.delete_volume(&provider_volume_id).await {
        Ok(()) => {
            tracing::info!("job {namespace}/{name}: deleted volume {provider_volume_id}");
            delete_job_volume(pool, job_id).await?;
            Ok(true)
        }
        Err(err) => {
            record_failed_attempt(
                pool,
                job_id,
                namespace,
                name,
                &err.to_string(),
                last_transition_time,
            )
            .await?;
            Ok(false)
        }
    }
}

async fn insert_job_volume(
    pool: &SqlitePool,
    job_id: &str,
    size_gb: u32,
    tier: volumes::StorageTier,
    provider_volume_id: &str,
) -> Result<()> {
    let id = Uuid::new_v4().to_string();
    let now = Utc::now().timestamp();
    sqlx::query(
        r#"
        INSERT INTO job_volumes (id, job_id, size_gb, storage_class_name, provider_volume_id, mount_point, created_at)
        VALUES (?, ?, ?, ?, ?, NULL, ?)
        "#,
    )
    .bind(id)
    .bind(job_id)
    .bind(size_gb)
    .bind(tier.class_name())
    .bind(provider_volume_id)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

async fn job_volume_id(pool: &SqlitePool, job_id: &str) -> Result<Option<String>> {
    let id: Option<String> =
        sqlx::query_scalar("SELECT provider_volume_id FROM job_volumes WHERE job_id = ?")
            .bind(job_id)
            .fetch_optional(pool)
            .await?
            .flatten();
    Ok(id)
}

async fn delete_job_volume(pool: &SqlitePool, job_id: &str) -> Result<()> {
    sqlx::query("DELETE FROM job_volumes WHERE job_id = ?")
        .bind(job_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Records a failed real-call attempt (`retry_count`/`last_error`,
/// both existing columns front-loaded in Phase 1's original schema) and
/// logs it -- at `warn` normally, escalating to `error` once it's been
/// failing for more than `RETRY_ESCALATION_THRESHOLD`. The job always
/// stays in its current state and retries next tick either way: deciding
/// when to actually give up and clean up is Phase 10's job, not this
/// one's -- this is purely about making a stuck job impossible to miss
/// in the logs.
async fn record_failed_attempt(
    pool: &SqlitePool,
    job_id: &str,
    namespace: &str,
    name: &str,
    error: &str,
    last_transition_time: Option<i64>,
) -> Result<()> {
    let now = Utc::now().timestamp();
    sqlx::query("UPDATE jobs SET retry_count = retry_count + 1, last_error = ?, updated_at = ? WHERE id = ?")
        .bind(error)
        .bind(now)
        .bind(job_id)
        .execute(pool)
        .await?;

    let retry_count: i64 = sqlx::query_scalar("SELECT retry_count FROM jobs WHERE id = ?")
        .bind(job_id)
        .fetch_one(pool)
        .await?;
    let elapsed = now - last_transition_time.unwrap_or(now);

    if elapsed > RETRY_ESCALATION_THRESHOLD {
        tracing::error!(
            "job {namespace}/{name}: still failing after {elapsed}s (retry #{retry_count}): \
             {error} -- no automatic give-up/cleanup exists yet (Phase 10)"
        );
    } else {
        tracing::warn!(
            "job {namespace}/{name}: attempt failed (retry #{retry_count}), will retry: {error}"
        );
    }
    Ok(())
}

fn log_would_create_vm(namespace: &str, name: &str, spec_str: &str, dry_run: bool) {
    let spec: JsonValue = serde_json::from_str(spec_str).unwrap_or(JsonValue::Null);
    let requests = spec.pointer("/template/spec/containers/0/resources/requests");

    let cpu_cores = requests
        .and_then(|r| r.get("cpu"))
        .and_then(JsonValue::as_str)
        .and_then(|q| workload::parse_cpu_cores(q).ok());
    let memory_gb = requests
        .and_then(|r| r.get("memory"))
        .and_then(JsonValue::as_str)
        .and_then(|q| volumes::parse_storage_quantity_gb(q).ok());

    let prefix = if dry_run { "DRY-RUN: " } else { "" };
    match (cpu_cores, memory_gb) {
        (Some(cpu_cores), Some(memory_gb)) => {
            match workload::smallest_fitting_server_plan(cpu_cores, memory_gb) {
                Some(plan) => tracing::info!(
                    "{prefix}would launch VM for job {namespace}/{name}: plan {} \
                     ({cpu_cores} CPU / {memory_gb}GB requested)",
                    plan.name
                ),
                None => tracing::warn!(
                    "{prefix}would launch VM for job {namespace}/{name}, but no known server \
                     plan is big enough for {cpu_cores} CPU / {memory_gb}GB -- see \
                     src/workload.rs's own note on this"
                ),
            }
        }
        _ => tracing::info!(
            "{prefix}would launch VM for job {namespace}/{name} with no resource requests set \
             (no requests.cpu/.memory in the pod template -- optional in real Kubernetes too)"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::upcloud::UpCloudProvider;

    fn mock_ctx(dry_run: bool) -> JobContext {
        JobContext {
            provider: Arc::new(UpCloudProvider::new("unused-in-dry-run")),
            dry_run,
            resource_prefix: "kube-shim-test".to_string(),
            zone: "de-fra1".to_string(),
        }
    }

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

        let advanced = advance_all(&pool, &mock_ctx(true)).await.unwrap();
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

        let advanced = advance_all(&pool, &mock_ctx(true)).await.unwrap();
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

        advance_all(&pool, &mock_ctx(true)).await.unwrap();

        let version: i64 = sqlx::query_scalar("SELECT version FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(version, 2);
    }

    #[tokio::test]
    async fn test_volume_pending_dry_run_advances_without_provider_call() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let spec = serde_json::json!({
            "template": {"spec": {
                "volumes": [{"ephemeral": {"volumeClaimTemplate": {"spec": {
                    "resources": {"requests": {"storage": "1Gi"}}
                }}}}]
            }}
        });
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'VolumePending', 0, 0, 1)",
        )
        .bind(spec.to_string())
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &mock_ctx(true)).await.unwrap();
        assert_eq!(advanced, 1);

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "VolumeCreating");

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_volumes WHERE job_id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0, "dry-run must never write a job_volumes row");
    }

    #[tokio::test]
    async fn test_volume_pending_with_no_ephemeral_volume_advances_without_creating_one() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{\"template\":{\"spec\":{}}}', 'VolumePending', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &mock_ctx(false)).await.unwrap();
        assert_eq!(advanced, 1);

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_volumes WHERE job_id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    fn ephemeral_volume_spec() -> String {
        serde_json::json!({
            "template": {"spec": {
                "volumes": [{"ephemeral": {"volumeClaimTemplate": {"spec": {
                    "resources": {"requests": {"storage": "1Gi"}},
                    "storageClassName": "kube-shim-fast"
                }}}}]
            }}
        })
        .to_string()
    }

    #[tokio::test]
    async fn test_volume_pending_real_success_creates_volume_and_advances() {
        let app = axum::Router::new().route(
            "/1.3/storage",
            axum::routing::post(|| async {
                axum::Json(serde_json::json!({
                    "storage": {"uuid": "vol-real-1", "size": 1, "tier": "maxiops", "title": "t", "zone": "de-fra1"}
                }))
            }),
        );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let ctx = JobContext {
            provider: Arc::new(provider),
            dry_run: false,
            resource_prefix: "kube-shim-test".to_string(),
            zone: "de-fra1".to_string(),
        };

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'VolumePending', 0, 0, 1)",
        )
        .bind(ephemeral_volume_spec())
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 1);

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "VolumeCreating");

        let (provider_volume_id, storage_class_name): (String, String) = sqlx::query_as(
            "SELECT provider_volume_id, storage_class_name FROM job_volumes WHERE job_id = 'j1'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(provider_volume_id, "vol-real-1");
        assert_eq!(storage_class_name, "kube-shim-fast");
    }

    #[tokio::test]
    async fn test_volume_pending_real_failure_retries_and_records_error() {
        let app = axum::Router::new().route(
            "/1.3/storage",
            axum::routing::post(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
        );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let ctx = JobContext {
            provider: Arc::new(provider),
            dry_run: false,
            resource_prefix: "kube-shim-test".to_string(),
            zone: "de-fra1".to_string(),
        };

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, last_transition_time, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'VolumePending', ?, ?, ?, 1)",
        )
        .bind(ephemeral_volume_spec())
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 0, "a failed real call must not advance the job");

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "VolumePending", "must stay put to retry next tick");

        let (retry_count, last_error): (i64, Option<String>) =
            sqlx::query_as("SELECT retry_count, last_error FROM jobs WHERE id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(retry_count, 1);
        assert!(!last_error.unwrap().is_empty());

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_volumes WHERE job_id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            count, 0,
            "a failed create must never leave a job_volumes row behind"
        );
    }

    #[tokio::test]
    async fn test_volume_detaching_with_no_tracked_volume_advances_immediately() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'VolumeDetaching', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &mock_ctx(true)).await.unwrap();
        assert_eq!(advanced, 1);

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "VolumeDeleted");
    }

    #[tokio::test]
    async fn test_volume_detaching_real_success_deletes_volume_and_row() {
        let app = axum::Router::new().route(
            "/1.3/storage/:uuid",
            axum::routing::delete(|| async { axum::http::StatusCode::NO_CONTENT }),
        );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let ctx = JobContext {
            provider: Arc::new(provider),
            dry_run: false,
            resource_prefix: "kube-shim-test".to_string(),
            zone: "de-fra1".to_string(),
        };

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'VolumeDetaching', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO job_volumes (id, job_id, size_gb, storage_class_name, provider_volume_id, created_at) \
             VALUES ('jv1', 'j1', 1, 'kube-shim-standard', 'vol-real-1', 0)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 1);

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "VolumeDeleted");

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_volumes WHERE job_id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            count, 0,
            "the job_volumes row must be gone once the real volume is deleted"
        );
    }

    #[tokio::test]
    async fn test_volume_detaching_dry_run_does_not_delete_tracked_row() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'VolumeDetaching', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO job_volumes (id, job_id, size_gb, storage_class_name, provider_volume_id, created_at) \
             VALUES ('jv1', 'j1', 1, 'kube-shim-standard', 'vol-abc', 0)",
        )
        .execute(&pool)
        .await
        .unwrap();

        advance_all(&pool, &mock_ctx(true)).await.unwrap();

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_volumes WHERE job_id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            count, 1,
            "dry-run must not touch a real-looking tracked row"
        );
    }
}
