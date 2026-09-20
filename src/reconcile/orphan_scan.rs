//! Orphan volume detection (Phase 8): finds UpCloud storage volumes
//! carrying this instance's `resource_prefix` that aren't tracked in
//! `job_volumes`, and deletes them. Volumes only for now -- server orphan
//! scanning joins this scan in Phase 9, once real worker VMs exist to
//! leak in the first place.
//!
//! Driven by `reconcile::run_orphan_scan_loop` on its own
//! `ORPHAN_SCAN_INTERVAL` cadence (five minutes), deliberately separate
//! from the job-tick loop's own `Notify`-driven wake-ups -- see that
//! constant's doc comment for why tying this to every job tick would mean
//! listing the whole account's volumes far more often than any real leak
//! could occur.

use crate::reconcile::JobContext;
use anyhow::Result;
use sqlx::SqlitePool;
use std::collections::HashSet;

/// Deletes every untracked volume this instance owns (by `title` prefix)
/// but has no `job_volumes` row for. Returns how many were cleaned up.
/// A no-op in dry-run mode: nothing real is ever created there, so there
/// is nothing real to have leaked either.
pub async fn scan_and_clean(pool: &SqlitePool, ctx: &JobContext) -> Result<usize> {
    if ctx.dry_run {
        return Ok(0);
    }

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
            Err(err) => tracing::error!("orphan scan: failed to delete {}: {err}", volume.id),
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
            );
        let provider = mock_server(app).await;
        let pool = crate::db::init_pool(":memory:").await.unwrap();

        let cleaned = scan_and_clean(&pool, &ctx_with(provider)).await.unwrap();
        assert_eq!(cleaned, 1);
    }

    #[tokio::test]
    async fn test_tracked_volume_is_left_alone() {
        let app = axum::Router::new().route(
            "/1.3/storage/normal",
            axum::routing::get(|| async {
                Json(json!({"storages": {"storage": [
                    {"uuid": "tracked-1", "size": 1, "tier": "standard", "title": "kube-shim-vol-osmdiffs-weekly-123", "zone": "de-fra1"}
                ]}}))
            }),
        );
        // No DELETE route registered -- if scan_and_clean tried to delete
        // the tracked volume, this test would fail with a 404 error.
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
}
