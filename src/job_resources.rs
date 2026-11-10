//! The billable cloud resources each job run created (issue #148) --
//! the `job_resources` table. `reconcile::job` records a resource the
//! moment the provider has created it and again the moment it's
//! deleted, since that's when the provider's billing for it starts and
//! stops. Two things read it back: `reconcile::job` computes
//! `jobs.actual_cost` from it once the job is archived, whichever way
//! the job ended (success, a container failure, a force-fail, a failed
//! provisioning step), and `api::cost_report` takes each row's
//! `ResourceId`/`ResourceName` and charge period from it.
//!
//! Rows are never deleted -- not when a job is removed from history
//! either (`reconcile::history::prune_job`) -- so the cost report keeps
//! a removed job's resources, the same way `pruned_jobs` keeps its cost.

use anyhow::Result;
use sqlx::{Row, SqlitePool};

/// A job's worker VM. Its boot disk and public IPv4 address are created
/// and deleted with it, so they're columns of its row.
pub const SERVER: &str = "server";
/// A job's ephemeral volume.
pub const VOLUME: &str = "volume";

#[derive(Debug, Clone, PartialEq)]
pub struct JobResource {
    pub kind: String,
    pub provider_id: String,
    pub name: String,
    pub created_at: i64,
    pub deleted_at: Option<i64>,
    pub boot_disk_id: Option<String>,
    pub public_ipv4: Option<String>,
}

impl JobResource {
    /// How long the resource existed, if it has been deleted.
    pub fn lifetime_seconds(&self) -> Option<i64> {
        self.deleted_at
            .map(|deleted_at| (deleted_at - self.created_at).max(0))
    }
}

/// Records that the provider created a resource for this job. A second
/// call for the same kind (a retry that found the resource already
/// there) keeps the first creation time.
pub async fn record_created(
    pool: &SqlitePool,
    job_id: &str,
    kind: &str,
    provider_id: &str,
    name: &str,
    now: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT OR IGNORE INTO job_resources (job_id, kind, provider_id, name, created_at) \
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(job_id)
    .bind(kind)
    .bind(provider_id)
    .bind(name)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

/// Records the provider's ID for the worker VM's boot disk.
pub async fn set_boot_disk_id(pool: &SqlitePool, job_id: &str, boot_disk_id: &str) -> Result<()> {
    sqlx::query("UPDATE job_resources SET boot_disk_id = ? WHERE job_id = ? AND kind = ?")
        .bind(boot_disk_id)
        .bind(job_id)
        .bind(SERVER)
        .execute(pool)
        .await?;
    Ok(())
}

/// Records the worker VM's public IPv4 address.
pub async fn set_public_ipv4(pool: &SqlitePool, job_id: &str, ip: &str) -> Result<()> {
    sqlx::query("UPDATE job_resources SET public_ipv4 = ? WHERE job_id = ? AND kind = ?")
        .bind(ip)
        .bind(job_id)
        .bind(SERVER)
        .execute(pool)
        .await?;
    Ok(())
}

/// Records that the provider deleted this job's resource of `kind`.
/// Keeps the first deletion time if called again.
pub async fn record_deleted(pool: &SqlitePool, job_id: &str, kind: &str, now: i64) -> Result<()> {
    sqlx::query(
        "UPDATE job_resources SET deleted_at = ? \
         WHERE job_id = ? AND kind = ? AND deleted_at IS NULL",
    )
    .bind(now)
    .bind(job_id)
    .bind(kind)
    .execute(pool)
    .await?;
    Ok(())
}

/// Marks every resource of this job not yet recorded as deleted as
/// deleted now -- for a job that is being archived, whose cleanup has
/// finished, so nothing of it should be left.
pub async fn record_all_deleted(pool: &SqlitePool, job_id: &str, now: i64) -> Result<()> {
    sqlx::query("UPDATE job_resources SET deleted_at = ? WHERE job_id = ? AND deleted_at IS NULL")
        .bind(now)
        .bind(job_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Every resource recorded for this job, server first.
pub async fn for_job(pool: &SqlitePool, job_id: &str) -> Result<Vec<JobResource>> {
    let rows = sqlx::query(
        "SELECT kind, provider_id, name, created_at, deleted_at, boot_disk_id, public_ipv4 \
         FROM job_resources WHERE job_id = ? ORDER BY kind = 'volume', kind",
    )
    .bind(job_id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| JobResource {
            kind: r.get(0),
            provider_id: r.get(1),
            name: r.get(2),
            created_at: r.get(3),
            deleted_at: r.get(4),
            boot_disk_id: r.get(5),
            public_ipv4: r.get(6),
        })
        .collect())
}

/// How long the job's worker VM and its volume existed, from its
/// recorded resources: `None` for one it never had or that isn't
/// deleted yet.
pub fn lifetimes(resources: &[JobResource]) -> (Option<i64>, Option<i64>) {
    let lifetime = |kind: &str| {
        resources
            .iter()
            .find(|r| r.kind == kind)
            .and_then(JobResource::lifetime_seconds)
    };
    (lifetime(SERVER), lifetime(VOLUME))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_records_a_resources_lifetime() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        record_created(&pool, "j1", SERVER, "srv-uuid", "kube-shim-worker-x", 100)
            .await
            .unwrap();
        // A retry that found the VM already there keeps the first time.
        record_created(&pool, "j1", SERVER, "srv-uuid", "kube-shim-worker-x", 150)
            .await
            .unwrap();
        set_boot_disk_id(&pool, "j1", "disk-uuid").await.unwrap();
        set_public_ipv4(&pool, "j1", "192.0.2.1").await.unwrap();
        record_created(&pool, "j1", VOLUME, "vol-uuid", "kube-shim-vol-x", 90)
            .await
            .unwrap();
        record_deleted(&pool, "j1", VOLUME, 400).await.unwrap();
        record_deleted(&pool, "j1", VOLUME, 500).await.unwrap();

        let resources = for_job(&pool, "j1").await.unwrap();
        assert_eq!(resources.len(), 2);
        assert_eq!(resources[0].kind, SERVER);
        assert_eq!(resources[0].created_at, 100);
        assert_eq!(resources[0].boot_disk_id.as_deref(), Some("disk-uuid"));
        assert_eq!(resources[0].public_ipv4.as_deref(), Some("192.0.2.1"));
        assert_eq!(resources[1].kind, VOLUME);
        assert_eq!(resources[1].deleted_at, Some(400));
        assert_eq!(lifetimes(&resources), (None, Some(310)));

        record_all_deleted(&pool, "j1", 700).await.unwrap();
        let resources = for_job(&pool, "j1").await.unwrap();
        assert_eq!(lifetimes(&resources), (Some(600), Some(310)));
    }
}
