//! `WorkloadKind` abstraction and shared per-job resource-sizing helpers
//! (Phase 5). Neither is wired into a live request path yet -- there's no
//! reconciliation loop or VM provisioning to consume them until Phase 6+
//! -- but both live here now, tested, as the single home later phases
//! build on rather than improvising their own version of each.

/// What kind of workload *owns* a job run, if any. Only `CronJob` exists
/// today. `Deployment` is real Kubernetes' next logical workload type
/// and is deliberately not built yet (see docs/IMPLEMENTATION_PLAN.md
/// "Future Work: Deployments") -- this enum exists now purely so the
/// reconciliation and VM-provisioning code built on top of it later
/// (Phase 6+) isn't written in a way that silently assumes CronJob is the
/// only possible kind.
///
/// Deliberately does *not* have a `Job` variant (Phase 13: standalone
/// `batch/v1` `Job`s, created directly rather than only ever spawned by
/// a `CronJob`'s schedule): this enum denotes the *owner*, not the job
/// run's own resource kind (which is always `Job` -- trivially, there's
/// no other option, so it wouldn't need an enum at all). A standalone
/// Job genuinely has no owner at all, the same as in real Kubernetes
/// (Jobs don't own other Jobs) -- that's `jobs.cronjob_name` being
/// absent, not a `WorkloadKind` value of its own.
use crate::providers::PlanInfo;
use anyhow::Result;
use sqlx::{Row, SqlitePool};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadKind {
    CronJob,
}

/// A cloud-provider server plan, read from the `provider_plans` cache
/// (issue #85). That table is filled daily from the provider's own
/// catalog by `reconcile::pricing`, so no plan names are hardcoded here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerPlan {
    pub name: String,
    pub cpu_cores: u32,
    pub memory_mb: u32,
    /// Storage the plan includes, in GB -- see `plan_bundles_disk_and_ip`.
    pub storage_gb: u32,
    pub storage_tier: Option<String>,
}

/// Boot disk size for every worker VM -- an OS-only disk cloned from the
/// worker template, not the job's own scratch space (that's the
/// separately attached ephemeral volume). Always on the `standard` tier.
pub const WORKER_BOOT_DISK_GB: u32 = 10;

/// Whether a plan bundles the worker's boot disk and public IPv4 into
/// its price. Plans that include at least the boot disk's size in
/// storage do (UpCloud: "the largest storage that fits within the plan
/// quota is counted as part of the plan", as long as the disk is on the
/// plan's own tier -- see `boot_disk_tier`). Plans that don't are
/// UpCloud's Cloud Native style ("Cloud Native plans unbundle storage
/// and IP addresses"): the boot disk is billed per GB at the `standard`
/// rate and the IPv4 address at `ipv4_address`, and both plan ranking
/// and cost reporting add them.
pub fn plan_bundles_disk_and_ip(plan_storage_gb: u32) -> bool {
    plan_storage_gb >= WORKER_BOOT_DISK_GB
}

/// The tier to clone the boot disk on: the plan's own storage tier when
/// it bundles the disk, so the disk counts as part of the plan, else
/// `standard`, the cheapest tier for a disk billed separately.
pub fn boot_disk_tier(plan: &ServerPlan) -> &str {
    match &plan.storage_tier {
        Some(tier) if plan_bundles_disk_and_ip(plan.storage_gb) => tier,
        _ => "standard",
    }
}

/// Which catalog plans worker VMs may use (issue #85). GPU plans are
/// never eligible, nor are plans UpCloud no longer sells
/// (`current_offering: "no"`). Otherwise a plan is eligible if `allow_prefixes` is
/// empty or one of them is a prefix of its name, and none of
/// `deny_prefixes` is. Applied when the catalog is synced, so a changed
/// list takes effect at the next sync, which runs right at startup.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlanFilter {
    pub allow_prefixes: Vec<String>,
    pub deny_prefixes: Vec<String>,
}

impl PlanFilter {
    pub fn is_eligible(&self, plan: &PlanInfo) -> bool {
        if plan.gpu_count > 0 || !plan.current_offering {
            return false;
        }
        let allowed = self.allow_prefixes.is_empty()
            || self.allow_prefixes.iter().any(|p| plan.name.starts_with(p));
        let denied = self.deny_prefixes.iter().any(|p| plan.name.starts_with(p));
        allowed && !denied
    }
}

/// Replaces the cached plan catalog with the eligible subset of
/// `plans`. Wholesale, in one transaction, like `currency::store_rates`:
/// a plan UpCloud retired disappears from the next choice, while a job
/// that already runs on it keeps its recorded `jobs.worker_plan`.
pub async fn store_plans(
    pool: &SqlitePool,
    plans: &[PlanInfo],
    filter: &PlanFilter,
    fetched_at: i64,
) -> Result<usize> {
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM provider_plans")
        .execute(&mut *tx)
        .await?;
    let mut stored = 0;
    for plan in plans.iter().filter(|p| filter.is_eligible(p)) {
        sqlx::query(
            "INSERT INTO provider_plans \
                 (name, cpu_cores, memory_mb, storage_gb, storage_tier, fetched_at) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&plan.name)
        .bind(plan.cpu_cores)
        .bind(plan.memory_mb)
        .bind(plan.storage_gb)
        .bind(&plan.storage_tier)
        .bind(fetched_at)
        .execute(&mut *tx)
        .await?;
        stored += 1;
    }
    tx.commit().await?;
    Ok(stored)
}

/// Whether the plan catalog has ever been synced.
pub async fn has_cached_plans(pool: &SqlitePool) -> Result<bool> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_plans")
        .fetch_one(pool)
        .await?;
    Ok(count > 0)
}

/// The cheapest cached plan in `zone` that meets or exceeds both
/// requested amounts, or `None` if nothing cached is big enough --
/// callers decide what "no plan fits" means for them. Picking by price
/// rather than by size matters once several families offer the same
/// shape (`6xCPU-16GB` vs `CLOUDNATIVE-6xCPU-16GB`, issue #85). A plan
/// with no cached price still qualifies, ranked after every priced one
/// (smallest first), so provisioning never depends on the price sync.
/// The price compared includes the boot disk for plans whose included
/// storage doesn't cover it, and the IPv4 address with it
/// (`plan_bundles_disk_and_ip`).
pub async fn cheapest_fitting_server_plan(
    pool: &SqlitePool,
    zone: &str,
    cpu_cores: u32,
    memory_gb: u32,
) -> Result<Option<ServerPlan>> {
    let row = sqlx::query(
        "SELECT p.name, p.cpu_cores, p.memory_mb, p.storage_gb, p.storage_tier \
         FROM provider_plans p \
         LEFT JOIN provider_pricing pr \
           ON pr.zone = ?1 AND pr.price_key = 'server_plan_' || p.name \
         LEFT JOIN provider_pricing st \
           ON st.zone = ?1 AND st.price_key = 'storage_standard' \
         LEFT JOIN provider_pricing ip \
           ON ip.zone = ?1 AND ip.price_key = 'ipv4_address' \
         WHERE p.cpu_cores >= ?2 AND p.memory_mb >= ?3 \
         ORDER BY (pr.price IS NULL), \
           pr.price / pr.amount \
             + CASE WHEN p.storage_gb < ?4 \
                 THEN COALESCE(st.price / st.amount, 0) * ?4 + COALESCE(ip.price / ip.amount, 0) \
                 ELSE 0 END, \
           p.cpu_cores, p.memory_mb, p.name \
         LIMIT 1",
    )
    .bind(zone)
    .bind(cpu_cores)
    .bind(i64::from(memory_gb) * 1024)
    .bind(WORKER_BOOT_DISK_GB)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| ServerPlan {
        name: r.get(0),
        cpu_cores: r.get(1),
        memory_mb: r.get(2),
        storage_gb: r.get(3),
        storage_tier: r.get(4),
    }))
}

/// Same defaulting convention `handle_vm_pending` uses when actually
/// sizing a worker VM -- a job with no explicit `resources.requests`
/// gets the cheapest plan with at least 1 CPU / 1GB, not an arbitrary
/// guess. Shared by `pricing` (cost estimates must size against the
/// same plan that'll really be provisioned), `admission` and
/// `handle_vm_pending`, so the rounding can't drift between them.
pub(crate) async fn plan_for_resource_requests(
    pool: &SqlitePool,
    zone: &str,
    cpu_millicores: Option<u32>,
    memory_mb: Option<u32>,
) -> Result<Option<ServerPlan>> {
    let (cpu_cores, memory_gb) = whole_units_for_requests(cpu_millicores, memory_mb);
    cheapest_fitting_server_plan(pool, zone, cpu_cores, memory_gb).await
}

/// Requests rounded *up* to whole cores and whole GB: UpCloud has no
/// fractional-core or sub-GB-memory plans.
pub(crate) fn whole_units_for_requests(
    cpu_millicores: Option<u32>,
    memory_mb: Option<u32>,
) -> (u32, u32) {
    (
        cpu_millicores.unwrap_or(1000).div_ceil(1000),
        memory_mb.unwrap_or(1024).div_ceil(1024),
    )
}

/// Test fixture: the DEV plans the hardcoded table used to hold before
/// issue #85, stored as if synced from the catalog.
#[cfg(test)]
pub(crate) async fn seed_test_plans(pool: &SqlitePool) {
    let plans: Vec<PlanInfo> = [
        ("DEV-1xCPU-1GB-10GB", 1, 1),
        ("DEV-1xCPU-2GB", 1, 2),
        ("DEV-1xCPU-4GB", 1, 4),
        ("DEV-2xCPU-4GB", 2, 4),
        ("DEV-2xCPU-8GB", 2, 8),
        ("DEV-2xCPU-16GB", 2, 16),
    ]
    .into_iter()
    .map(|(name, cpu_cores, memory_gb)| PlanInfo {
        name: name.to_string(),
        cpu_cores,
        memory_mb: memory_gb * 1024,
        // Every DEV plan includes at least the boot disk's 10 GB.
        storage_gb: 20,
        storage_tier: Some("standard".to_string()),
        gpu_count: 0,
        current_offering: true,
    })
    .collect();
    store_plans(pool, &plans, &PlanFilter::default(), 0)
        .await
        .unwrap();
}

/// Parses a Kubernetes CPU resource quantity (`resources.requests.cpu`,
/// e.g. `"2"` or `"500m"` for 500 millicores) into millicores, the same
/// integer-precision unit Kubernetes itself uses internally -- real
/// Kubernetes CPU requests are *not* restricted to whole cores (that's
/// standard k8s, not specific to Borg), and this project needs to
/// preserve that precision for accurate `kubectl top`/events reporting
/// (`api::metrics`/`api::nodes`), not just for VM sizing. `parse_cpu_cores`
/// below is a *separate, deliberately lossy* wrapper over this, used only
/// where a whole-core answer is actually required (worker-VM plan selection:
/// UpCloud's own plan catalog only offers whole-CPU-count plans, so there's
/// no such thing as provisioning "0.5 CPU" regardless of what the job asked for).
pub fn parse_cpu_millicores(quantity: &str) -> Result<u32, String> {
    let quantity = quantity.trim();
    if let Some(millicores) = quantity.strip_suffix('m') {
        let millicores: f64 = millicores
            .parse()
            .map_err(|_| format!("not a valid CPU quantity: {quantity:?}"))?;
        if millicores < 0.0 {
            return Err(format!("CPU quantity must not be negative: {quantity:?}"));
        }
        return Ok(millicores.round() as u32);
    }

    let cores: f64 = quantity
        .parse()
        .map_err(|_| format!("not a valid CPU quantity: {quantity:?}"))?;
    if cores < 0.0 {
        return Err(format!("CPU quantity must not be negative: {quantity:?}"));
    }
    Ok((cores * 1000.0).round() as u32)
}

/// Rounds a CPU request *up* to a whole number of cores -- see
/// `parse_cpu_millicores`'s own docs for why this lossy rounding exists
/// as a separate function rather than being the only way to parse this
/// field: it's correct for sizing a worker VM (UpCloud has no
/// fractional-core plans), wrong for reporting what was actually asked
/// for.
pub fn parse_cpu_cores(quantity: &str) -> Result<u32, String> {
    Ok(parse_cpu_millicores(quantity)?.div_ceil(1000))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool_with_dev_plans() -> SqlitePool {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        seed_test_plans(&pool).await;
        pool
    }

    async fn price(pool: &SqlitePool, plan: &str, price: f64) {
        sqlx::query(
            "INSERT INTO provider_pricing (zone, price_key, amount, price, currency, fetched_at) \
             VALUES ('de-fra1', ?, 1.0, ?, 'EUR', 0)",
        )
        .bind(format!("server_plan_{plan}"))
        .bind(price)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn price_key(pool: &SqlitePool, key: &str, price: f64) {
        sqlx::query(
            "INSERT INTO provider_pricing (zone, price_key, amount, price, currency, fetched_at) \
             VALUES ('de-fra1', ?, 1.0, ?, 'EUR', 0)",
        )
        .bind(key)
        .bind(price)
        .execute(pool)
        .await
        .unwrap();
    }

    fn plan_info(name: &str, cpu_cores: u32, memory_gb: u32, gpu_count: u32) -> PlanInfo {
        PlanInfo {
            name: name.to_string(),
            cpu_cores,
            memory_mb: memory_gb * 1024,
            storage_gb: 0,
            storage_tier: None,
            gpu_count,
            current_offering: true,
        }
    }

    async fn pick(pool: &SqlitePool, cpu: u32, mem: u32) -> Option<String> {
        cheapest_fitting_server_plan(pool, "de-fra1", cpu, mem)
            .await
            .unwrap()
            .map(|p| p.name)
    }

    #[tokio::test]
    async fn test_without_prices_the_smallest_fitting_plan_wins() {
        let pool = pool_with_dev_plans().await;
        assert_eq!(pick(&pool, 2, 8).await.as_deref(), Some("DEV-2xCPU-8GB"));
        // Rounds up on cores first: a 1-core request needing 3GB must not
        // pick a 2-core plan just because 2xCPU-4GB also has enough memory.
        assert_eq!(pick(&pool, 1, 3).await.as_deref(), Some("DEV-1xCPU-4GB"));
        assert_eq!(
            pick(&pool, 1, 1).await.as_deref(),
            Some("DEV-1xCPU-1GB-10GB")
        );
    }

    #[tokio::test]
    async fn test_none_when_no_cached_plan_is_big_enough() {
        let pool = pool_with_dev_plans().await;
        assert_eq!(pick(&pool, 6, 8).await, None);
    }

    #[tokio::test]
    async fn test_none_when_nothing_is_cached() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        assert_eq!(pick(&pool, 1, 1).await, None);
    }

    #[tokio::test]
    async fn test_cheapest_priced_plan_wins_over_a_smaller_one() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let plans = [
            plan_info("6xCPU-16GB", 6, 16, 0),
            plan_info("CLOUDNATIVE-6xCPU-16GB", 6, 16, 0),
            plan_info("CLOUDNATIVE-8xCPU-16GB", 8, 16, 0),
        ];
        store_plans(&pool, &plans, &PlanFilter::default(), 0)
            .await
            .unwrap();
        price(&pool, "6xCPU-16GB", 15.4761).await;
        price(&pool, "CLOUDNATIVE-6xCPU-16GB", 11.0).await;
        price(&pool, "CLOUDNATIVE-8xCPU-16GB", 10.0).await;
        assert_eq!(
            pick(&pool, 6, 8).await.as_deref(),
            Some("CLOUDNATIVE-8xCPU-16GB")
        );
    }

    #[tokio::test]
    async fn test_ranking_adds_the_boot_disk_when_the_plan_includes_no_storage() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let mut with_disk = plan_info("1xCPU-1GB", 1, 1, 0);
        with_disk.storage_gb = 25;
        with_disk.storage_tier = Some("maxiops".to_string());
        let without_disk = plan_info("CLOUDNATIVE-1xCPU-1GB", 1, 1, 0);
        store_plans(&pool, &[with_disk, without_disk], &PlanFilter::default(), 0)
            .await
            .unwrap();
        price(&pool, "1xCPU-1GB", 1.0).await;
        price(&pool, "CLOUDNATIVE-1xCPU-1GB", 0.95).await;
        // Cheaper on its own, but 10 GB of standard storage at 0.01 c/GB/h
        // adds 0.1 c/h and makes it the more expensive choice.
        assert_eq!(
            pick(&pool, 1, 1).await.as_deref(),
            Some("CLOUDNATIVE-1xCPU-1GB"),
            "without storage prices cached, only the plan price counts"
        );
        sqlx::query(
            "INSERT INTO provider_pricing (zone, price_key, amount, price, currency, fetched_at) \
             VALUES ('de-fra1', 'storage_standard', 1.0, 0.01, 'EUR', 0)",
        )
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(pick(&pool, 1, 1).await.as_deref(), Some("1xCPU-1GB"));
    }

    #[tokio::test]
    async fn test_ranking_adds_the_ipv4_address_when_the_plan_includes_no_storage() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let mut with_disk = plan_info("1xCPU-1GB", 1, 1, 0);
        with_disk.storage_gb = 25;
        let without_disk = plan_info("CLOUDNATIVE-1xCPU-1GB", 1, 1, 0);
        store_plans(&pool, &[with_disk, without_disk], &PlanFilter::default(), 0)
            .await
            .unwrap();
        price(&pool, "1xCPU-1GB", 1.0).await;
        price(&pool, "CLOUDNATIVE-1xCPU-1GB", 0.6).await;
        price_key(&pool, "ipv4_address", 0.48).await;
        assert_eq!(pick(&pool, 1, 1).await.as_deref(), Some("1xCPU-1GB"));
    }

    #[test]
    fn test_boot_disk_tier_follows_the_plan_only_when_it_bundles_storage() {
        let mut plan = ServerPlan {
            name: "1xCPU-1GB".to_string(),
            cpu_cores: 1,
            memory_mb: 1024,
            storage_gb: 25,
            storage_tier: Some("maxiops".to_string()),
        };
        assert_eq!(boot_disk_tier(&plan), "maxiops");
        plan.storage_gb = 0;
        plan.storage_tier = None;
        assert_eq!(boot_disk_tier(&plan), "standard");
    }

    #[tokio::test]
    async fn test_priced_plans_rank_before_unpriced_ones() {
        let pool = pool_with_dev_plans().await;
        price(&pool, "DEV-2xCPU-4GB", 2.0).await;
        assert_eq!(pick(&pool, 1, 1).await.as_deref(), Some("DEV-2xCPU-4GB"));
    }

    #[tokio::test]
    async fn test_price_is_normalized_by_amount() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        store_plans(
            &pool,
            &[plan_info("A", 1, 1, 0), plan_info("B", 1, 1, 0)],
            &PlanFilter::default(),
            0,
        )
        .await
        .unwrap();
        price(&pool, "A", 3.0).await;
        sqlx::query(
            "INSERT INTO provider_pricing (zone, price_key, amount, price, currency, fetched_at) \
             VALUES ('de-fra1', 'server_plan_B', 2.0, 4.0, 'EUR', 0)",
        )
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(pick(&pool, 1, 1).await.as_deref(), Some("B"));
    }

    #[tokio::test]
    async fn test_store_plans_replaces_the_catalog_and_applies_the_filter() {
        let pool = pool_with_dev_plans().await;
        let filter = PlanFilter {
            allow_prefixes: vec![],
            deny_prefixes: vec!["CLOUDNATIVE-".to_string()],
        };
        let stored = store_plans(
            &pool,
            &[
                plan_info("6xCPU-16GB", 6, 16, 0),
                plan_info("CLOUDNATIVE-6xCPU-16GB", 6, 16, 0),
                plan_info("GPU-8xCPU-64GB-1xL40S", 8, 64, 1),
            ],
            &filter,
            0,
        )
        .await
        .unwrap();
        assert_eq!(stored, 1);
        // The old DEV plans are gone, not merged with the new catalog.
        assert_eq!(pick(&pool, 1, 1).await.as_deref(), Some("6xCPU-16GB"));
        assert_eq!(pick(&pool, 8, 8).await, None);
    }

    #[test]
    fn test_plan_filter() {
        let any = PlanFilter::default();
        assert!(any.is_eligible(&plan_info("2xCPU-4GB", 2, 4, 0)));
        assert!(!any.is_eligible(&plan_info("GPU-8xCPU-64GB-1xL40S", 8, 64, 1)));
        let mut retired = plan_info("DEV-1xCPU-1GB-10GB", 1, 1, 0);
        retired.current_offering = false;
        assert!(!any.is_eligible(&retired));

        let dev_only = PlanFilter {
            allow_prefixes: vec!["DEV-".to_string(), "CLOUDNATIVE-".to_string()],
            deny_prefixes: vec!["CLOUDNATIVE-8x".to_string()],
        };
        assert!(dev_only.is_eligible(&plan_info("DEV-2xCPU-4GB", 2, 4, 0)));
        assert!(dev_only.is_eligible(&plan_info("CLOUDNATIVE-6xCPU-16GB", 6, 16, 0)));
        assert!(!dev_only.is_eligible(&plan_info("CLOUDNATIVE-8xCPU-16GB", 8, 16, 0)));
        assert!(!dev_only.is_eligible(&plan_info("2xCPU-4GB", 2, 4, 0)));
    }

    #[test]
    fn test_workload_kind_cronjob_exists() {
        let kind = WorkloadKind::CronJob;
        assert_eq!(kind, WorkloadKind::CronJob);
    }

    #[test]
    fn test_parse_cpu_cores_whole_number() {
        assert_eq!(parse_cpu_cores("2"), Ok(2));
    }

    #[test]
    fn test_parse_cpu_cores_millicores_rounds_up() {
        assert_eq!(parse_cpu_cores("500m"), Ok(1));
        assert_eq!(parse_cpu_cores("1500m"), Ok(2));
        assert_eq!(parse_cpu_cores("1000m"), Ok(1));
    }

    #[test]
    fn test_parse_cpu_cores_rejects_garbage() {
        assert!(parse_cpu_cores("not-a-number").is_err());
    }

    #[test]
    fn test_parse_cpu_cores_rejects_negative() {
        assert!(parse_cpu_cores("-2").is_err());
        assert!(parse_cpu_cores("-500m").is_err());
    }

    #[test]
    fn test_parse_cpu_millicores_preserves_fractional_precision() {
        assert_eq!(parse_cpu_millicores("500m"), Ok(500));
        assert_eq!(parse_cpu_millicores("1500m"), Ok(1500));
        assert_eq!(parse_cpu_millicores("2"), Ok(2000));
        assert_eq!(parse_cpu_millicores("1.5"), Ok(1500));
    }

    #[test]
    fn test_parse_cpu_millicores_rejects_negative() {
        assert!(parse_cpu_millicores("-500m").is_err());
    }
}
