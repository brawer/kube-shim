//! Orphan detection: finds UpCloud resources carrying this instance's
//! `resource_prefix` that aren't tracked in the database, and deletes
//! them. Volumes since Phase 8; servers joined in Phase 9, once real
//! worker VMs existed to leak in the first place.
//!
//! Driven by `reconcile::run_orphan_scan_loop` on its own
//! `ORPHAN_SCAN_INTERVAL` cadence (five minutes), deliberately separate
//! from the job-tick loop's own `Notify`-driven wake-ups -- see that
//! constant's doc comment for why tying this to every job tick would mean
//! listing the whole account's resources far more often than any real
//! leak could occur.

use crate::reconcile::JobContext;
use anyhow::Result;
use sqlx::SqlitePool;
use std::collections::HashSet;

/// Deletes every untracked volume and worker VM this instance owns (by
/// `title` prefix) but has no corresponding database row for. Returns how
/// many were cleaned up in total. A no-op in dry-run mode: nothing real is
/// ever created there, so there is nothing real to have leaked either.
pub async fn scan_and_clean(pool: &SqlitePool, ctx: &JobContext) -> Result<usize> {
    if ctx.dry_run {
        return Ok(0);
    }

    let volumes_cleaned = scan_and_clean_volumes(pool, ctx).await?;
    let servers_cleaned = scan_and_clean_servers(pool, ctx).await?;
    Ok(volumes_cleaned + servers_cleaned)
}

async fn scan_and_clean_volumes(pool: &SqlitePool, ctx: &JobContext) -> Result<usize> {
    let remote_volumes = ctx.provider.list_volumes(&ctx.zone).await?;
    let tracked: HashSet<String> = sqlx::query_scalar(
        "SELECT provider_volume_id FROM job_volumes WHERE provider_volume_id IS NOT NULL",
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect();

    let ours_prefix = format!("{}-vol-", ctx.resource_prefix);
    let mut cleaned = 0;

    for volume in remote_volumes {
        if !volume.title.starts_with(&ours_prefix) {
            continue;
        }
        if tracked.contains(&volume.id) {
            continue;
        }

        tracing::warn!(
            "orphan scan: deleting untracked volume {} ({}, {}GB)",
            volume.id,
            volume.title,
            volume.size_gb
        );
        match ctx.provider.delete_volume(&volume.id).await {
            Ok(()) => cleaned += 1,
            Err(err) => {
                tracing::error!("orphan scan: failed to delete volume {}: {err}", volume.id)
            }
        }
    }

    Ok(cleaned)
}

/// Matched by `{resource_prefix}-worker-` title prefix against
/// `jobs.worker_vm_id`, the same convention `scan_and_clean_volumes` uses
/// against `job_volumes.provider_volume_id`. A worker VM can leak the same
/// way a volume can -- a crash between "API call succeeded" and "DB write
/// committed" -- and a real one now exists to do so (Phase 9).
async fn scan_and_clean_servers(pool: &SqlitePool, ctx: &JobContext) -> Result<usize> {
    let remote_servers = ctx.provider.list_servers(&ctx.zone).await?;
    let tracked: HashSet<String> =
        sqlx::query_scalar("SELECT worker_vm_id FROM jobs WHERE worker_vm_id IS NOT NULL")
            .fetch_all(pool)
            .await?
            .into_iter()
            .collect();

    let ours_prefix = format!("{}-worker-", ctx.resource_prefix);
    let mut cleaned = 0;

    for server in remote_servers {
        if !server.title.starts_with(&ours_prefix) {
            continue;
        }
        if tracked.contains(&server.id) {
            continue;
        }

        // Same real constraint as reconcile::job's handle_vm_terminating:
        // UpCloud refuses to delete a server that isn't already "stopped"
        // (409 SERVER_STATE_ILLEGAL) -- an untracked worker VM found here
        // is often still genuinely running, so this can't skip straight
        // to delete_server the way it used to.
        if server.state != "stopped" {
            if server.state == "started" {
                tracing::warn!(
                    "orphan scan: stopping untracked worker VM {} ({}) before deleting it",
                    server.id,
                    server.title
                );
                if let Err(err) = ctx.provider.stop_server(&server.id).await {
                    tracing::error!("orphan scan: failed to stop server {}: {err}", server.id);
                }
            }
            // Stop is asynchronous -- deletion is left for a future scan
            // once the server has actually reached "stopped".
            continue;
        }

        tracing::warn!(
            "orphan scan: deleting untracked worker VM {} ({})",
            server.id,
            server.title
        );
        match ctx.provider.delete_server(&server.id).await {
            Ok(()) => cleaned += 1,
            Err(err) => {
                tracing::error!("orphan scan: failed to delete server {}: {err}", server.id)
            }
        }
    }

    Ok(cleaned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::upcloud::tests::mock_server;
    use crate::providers::upcloud::UpCloudProvider;
    use axum::Json;
    use serde_json::json;
    use std::sync::Arc;

    fn ctx_with(provider: UpCloudProvider) -> JobContext {
        JobContext {
            provider: Arc::new(provider),
            dry_run: false,
            resource_prefix: "kube-shim".to_string(),
            zone: "de-fra1".to_string(),
            worker_template_uuid: "01000000-0000-4000-8000-000030240200".to_string(),
            worker_ssh_public_keys: vec![],
            own_public_ip: None,
            worker_ssh_private_key: String::new(),
            worker_ssh_port: crate::ssh::SSH_PORT,
        }
    }

    #[tokio::test]
    async fn test_dry_run_never_calls_the_provider() {
        // No routes registered at all -- if scan_and_clean tried to call
        // the provider in dry-run mode, this would fail with a connection
        // or 404 error instead of returning Ok(0).
        let app = axum::Router::new();
        let provider = mock_server(app).await;
        let pool = crate::db::init_pool(":memory:").await.unwrap();

        let mut ctx = ctx_with(provider);
        ctx.dry_run = true;

        assert_eq!(scan_and_clean(&pool, &ctx).await.unwrap(), 0);
    }

    fn no_servers_route() -> axum::Router {
        axum::Router::new().route(
            "/1.3/server",
            axum::routing::get(|| async { Json(json!({"servers": {"server": []}})) }),
        )
    }

    #[tokio::test]
    async fn test_deletes_untracked_volume_with_our_prefix() {
        let app = axum::Router::new()
            .route(
                "/1.3/storage/normal",
                axum::routing::get(|| async {
                    Json(json!({"storages": {"storage": [
                        {"uuid": "orphan-1", "size": 1, "tier": "standard", "title": "kube-shim-vol-osmdiffs-weekly-123", "zone": "de-fra1"},
                        {"uuid": "not-ours", "size": 5, "tier": "standard", "title": "some-other-volume", "zone": "de-fra1"}
                    ]}}))
                }),
            )
            .route(
                "/1.3/storage/:uuid",
                axum::routing::delete(|| async { axum::http::StatusCode::NO_CONTENT }),
            )
            .merge(no_servers_route());
        let provider = mock_server(app).await;
        let pool = crate::db::init_pool(":memory:").await.unwrap();

        let cleaned = scan_and_clean(&pool, &ctx_with(provider)).await.unwrap();
        assert_eq!(cleaned, 1);
    }

    #[tokio::test]
    async fn test_tracked_volume_is_left_alone() {
        let app = axum::Router::new()
            .route(
                "/1.3/storage/normal",
                axum::routing::get(|| async {
                    Json(json!({"storages": {"storage": [
                        {"uuid": "tracked-1", "size": 1, "tier": "standard", "title": "kube-shim-vol-osmdiffs-weekly-123", "zone": "de-fra1"}
                    ]}}))
                }),
            )
            // No DELETE route registered -- if scan_and_clean tried to
            // delete the tracked volume, this test would fail with a 404.
            .merge(no_servers_route());
        let provider = mock_server(app).await;
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO job_volumes (id, job_id, size_gb, storage_class_name, provider_volume_id, created_at) \
             VALUES ('jv1', 'job1', 1, 'kube-shim-standard', 'tracked-1', 0)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let cleaned = scan_and_clean(&pool, &ctx_with(provider)).await.unwrap();
        assert_eq!(cleaned, 0);
    }

    #[tokio::test]
    async fn test_deletes_untracked_worker_vm_with_our_prefix() {
        let app = axum::Router::new()
            .route(
                "/1.3/storage/normal",
                axum::routing::get(|| async { Json(json!({"storages": {"storage": []}})) }),
            )
            .route(
                "/1.3/server",
                axum::routing::get(|| async {
                    // Already "stopped" -- e.g. a worker whose job finished
                    // and that got stopped on a previous scan (see
                    // test_stops_a_running_untracked_worker_vm_before_deleting_it
                    // for the "still started" path, which can't delete in
                    // the same scan).
                    Json(json!({"servers": {"server": [
                        {"uuid": "orphan-vm-1", "title": "kube-shim-worker-osmdiffs-weekly-123", "state": "stopped", "zone": "de-fra1"},
                        {"uuid": "not-ours", "title": "some-other-server", "state": "started", "zone": "de-fra1"}
                    ]}}))
                }),
            )
            .route(
                "/1.3/server/:uuid",
                axum::routing::delete(|| async { axum::http::StatusCode::NO_CONTENT }),
            );
        let provider = mock_server(app).await;
        let pool = crate::db::init_pool(":memory:").await.unwrap();

        let cleaned = scan_and_clean(&pool, &ctx_with(provider)).await.unwrap();
        assert_eq!(cleaned, 1);
    }

    #[tokio::test]
    async fn test_stops_a_running_untracked_worker_vm_before_deleting_it() {
        // Real Phase 11 finding: UpCloud refuses delete_server on a
        // "started" server (409 SERVER_STATE_ILLEGAL). An untracked worker
        // found still running must be stopped first, not deleted
        // immediately -- deletion is left for a later scan.
        let delete_called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let delete_called_clone = delete_called.clone();
        let stop_called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_called_clone = stop_called.clone();
        let app = axum::Router::new()
            .route(
                "/1.3/storage/normal",
                axum::routing::get(|| async { Json(json!({"storages": {"storage": []}})) }),
            )
            .route(
                "/1.3/server",
                axum::routing::get(|| async {
                    Json(json!({"servers": {"server": [
                        {"uuid": "orphan-vm-1", "title": "kube-shim-worker-osmdiffs-weekly-123", "state": "started", "zone": "de-fra1"}
                    ]}}))
                }),
            )
            .route(
                "/1.3/server/:uuid",
                axum::routing::delete(move || {
                    delete_called_clone.store(true, std::sync::atomic::Ordering::SeqCst);
                    async { axum::http::StatusCode::NO_CONTENT }
                }),
            )
            .route(
                "/1.3/server/:uuid/stop",
                axum::routing::post(move || {
                    stop_called_clone.store(true, std::sync::atomic::Ordering::SeqCst);
                    async {
                        Json(json!({"server": {"uuid": "orphan-vm-1", "title": "t", "state": "started"}}))
                    }
                }),
            );
        let provider = mock_server(app).await;
        let pool = crate::db::init_pool(":memory:").await.unwrap();

        let cleaned = scan_and_clean(&pool, &ctx_with(provider)).await.unwrap();
        assert_eq!(cleaned, 0, "deletion is deferred to a later scan");
        assert!(stop_called.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!delete_called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_tracked_worker_vm_is_left_alone() {
        let app = axum::Router::new()
            .route(
                "/1.3/storage/normal",
                axum::routing::get(|| async { Json(json!({"storages": {"storage": []}})) }),
            )
            // No DELETE route registered -- if scan_and_clean tried to
            // delete the tracked VM, this test would fail with a 404.
            .route(
                "/1.3/server",
                axum::routing::get(|| async {
                    Json(json!({"servers": {"server": [
                        {"uuid": "tracked-vm-1", "title": "kube-shim-worker-osmdiffs-weekly-123", "state": "started", "zone": "de-fra1"}
                    ]}}))
                }),
            );
        let provider = mock_server(app).await;
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_vm_id, created_at, updated_at, version) \
             VALUES ('job1', 'osmdiffs-weekly-123', 'default', '{}', 'ContainerRunning', 'tracked-vm-1', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let cleaned = scan_and_clean(&pool, &ctx_with(provider)).await.unwrap();
        assert_eq!(cleaned, 0);
    }
}
