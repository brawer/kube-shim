//! Daily background sync of UpCloud pricing + ECB exchange rates
//! (Phase 14a) -- `src/pricing.rs`'s cost calculation reads from these
//! cached tables, never hitting either API live per job.

use crate::currency;
use crate::reconcile::JobContext;
use crate::workload;
use anyhow::Result;
use chrono::Utc;
use sqlx::SqlitePool;

/// Whether a previous sync (this boot or an earlier one -- the tables
/// persist across restarts) has ever populated *both* caches. Used by
/// `main.rs` to decide whether the very first sync needs to be a
/// blocking, synchronous call before the server starts accepting
/// requests at all: a job admitted before either cache has anything in
/// it would otherwise have no way to ever learn its own cost, since
/// `src/pricing.rs` never hits either API live per job. Deliberately a
/// cumulative (AND, not OR) check -- UpCloud pricing and ECB exchange
/// rates are two independent real network dependencies, and having
/// only one of them still leaves currency conversion (or the pricing
/// itself) unable to produce a real number.
///
/// Since issue #85 the server-plan catalog (`provider_plans`) counts
/// too: without it no job can be sized at all.
pub async fn has_cached_pricing(pool: &SqlitePool) -> Result<bool> {
    let pricing: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_pricing")
        .fetch_one(pool)
        .await?;
    let rates: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM exchange_rates")
        .fetch_one(pool)
        .await?;
    Ok(pricing > 0 && rates > 0 && workload::has_cached_plans(pool).await?)
}

/// One pass: sync the plan catalog, pricing, then exchange rates. Each
/// part tolerates the others failing independently (a `?` on the first alone would
/// otherwise skip exchange-rate sync entirely just because, say,
/// UpCloud was briefly unreachable) -- logged, not fatal, same
/// resilience stance `reconcile::metrics_collector` already takes for
/// a single bad poll.
pub async fn sync_pricing_and_rates(pool: &SqlitePool, ctx: &JobContext) -> Result<()> {
    if let Err(err) = sync_plans(pool, ctx).await {
        tracing::error!("server plan sync failed: {err:?}");
    }
    if let Err(err) = sync_pricing(pool, ctx).await {
        tracing::error!("pricing sync failed: {err:?}");
    }
    if let Err(err) = sync_exchange_rates(pool).await {
        tracing::error!("exchange rate sync failed: {err:?}");
    }
    Ok(())
}

/// Replaces `provider_plans` with the eligible part of the provider's
/// catalog (issue #85). An empty catalog is treated as an error rather
/// than stored, so a bad response can't wipe out every plan.
async fn sync_plans(pool: &SqlitePool, ctx: &JobContext) -> Result<()> {
    if ctx.dry_run {
        tracing::debug!("DRY-RUN: skipping real server plan sync");
        return Ok(());
    }
    let plans = ctx.provider.list_plans().await?;
    if plans.is_empty() {
        anyhow::bail!("provider returned an empty plan catalog; keeping the cached one");
    }
    let stored =
        workload::store_plans(pool, &plans, &ctx.plan_filter, Utc::now().timestamp()).await?;
    tracing::info!(
        "server plan sync: {stored} of {} plans eligible for worker VMs",
        plans.len()
    );
    Ok(())
}

/// Stores every price-catalog entry of the configured zone -- one
/// `GET /1.3/price` per sync. The whole zone is a few hundred small
/// rows, and storing all of it means a plan that only just appeared in
/// the catalog already has its price.
async fn sync_pricing(pool: &SqlitePool, ctx: &JobContext) -> Result<()> {
    if ctx.dry_run {
        tracing::debug!("DRY-RUN: skipping real pricing sync");
        return Ok(());
    }

    let prices = ctx.provider.get_zone_prices(&ctx.zone).await?;
    let now = Utc::now().timestamp();
    let mut tx = pool.begin().await?;
    for (price_key, entry) in &prices {
        sqlx::query(
            "INSERT INTO provider_pricing (zone, price_key, amount, price, currency, fetched_at) \
             VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT(zone, price_key) DO UPDATE SET \
                 amount = excluded.amount, price = excluded.price, \
                 currency = excluded.currency, fetched_at = excluded.fetched_at",
        )
        .bind(&ctx.zone)
        .bind(price_key)
        .bind(entry.amount)
        .bind(entry.price)
        .bind(&entry.currency)
        .bind(now)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn sync_exchange_rates(pool: &SqlitePool) -> Result<()> {
    let rates = currency::fetch_ecb_rates().await?;
    currency::store_rates(pool, &rates, Utc::now().timestamp()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn insert_price(pool: &SqlitePool) {
        sqlx::query(
            "INSERT INTO provider_pricing (zone, price_key, amount, price, currency, fetched_at) \
             VALUES ('de-fra1', 'server_plan_DEV-1xCPU-1GB-10GB', 1.0, 0.4464, 'EUR', 0)",
        )
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_rate(pool: &SqlitePool) {
        sqlx::query(
            "INSERT INTO exchange_rates (currency, rate, fetched_at) VALUES ('USD', 1.08, 0)",
        )
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_has_cached_pricing_false_when_both_tables_empty() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        assert!(!has_cached_pricing(&pool).await.unwrap());
    }

    #[tokio::test]
    async fn test_has_cached_pricing_false_when_only_pricing_present() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_price(&pool).await;
        assert!(!has_cached_pricing(&pool).await.unwrap());
    }

    #[tokio::test]
    async fn test_has_cached_pricing_false_when_only_rates_present() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_rate(&pool).await;
        assert!(!has_cached_pricing(&pool).await.unwrap());
    }

    #[tokio::test]
    async fn test_has_cached_pricing_false_without_plans() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_price(&pool).await;
        insert_rate(&pool).await;
        assert!(!has_cached_pricing(&pool).await.unwrap());
    }

    #[tokio::test]
    async fn test_has_cached_pricing_true_when_all_present() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_price(&pool).await;
        insert_rate(&pool).await;
        workload::seed_test_plans(&pool).await;
        assert!(has_cached_pricing(&pool).await.unwrap());
    }

    fn ctx_with(provider: crate::providers::upcloud::UpCloudProvider) -> JobContext {
        JobContext {
            provider: std::sync::Arc::new(provider),
            dry_run: false,
            resource_prefix: "kube-shim-test".to_string(),
            zone: "de-fra1".to_string(),
            worker_template_uuid: "01000000-0000-4000-8000-000030240200".to_string(),
            worker_ssh_public_keys: vec![],
            own_public_ip: None,
            worker_ssh_private_key: String::new(),
            worker_ssh_port: crate::ssh::SSH_PORT,
            main_currency: "EUR".to_string(),
            plan_filter: workload::PlanFilter {
                allow_prefixes: vec![],
                deny_prefixes: vec!["HICPU-".to_string()],
            },
        }
    }

    fn catalog_app(plans: serde_json::Value) -> axum::Router {
        axum::Router::new()
            .route(
                "/1.3/plan",
                axum::routing::get(move || {
                    let plans = plans.clone();
                    async move { axum::Json(serde_json::json!({"plans": {"plan": plans}})) }
                }),
            )
            .route(
                "/1.3/price",
                axum::routing::get(|| async {
                    axum::Json(serde_json::json!({"prices": {"currency": "EUR", "zone": [
                        {"name": "de-fra1",
                         "server_plan_CLOUDNATIVE-6xCPU-16GB": {"amount": 1, "price": 11.0119},
                         "storage_standard": {"amount": 1, "price": 0.0118},
                         "ipv4_address": {"amount": 1, "price": 0.4812}},
                        {"name": "fi-hel1",
                         "server_plan_CLOUDNATIVE-6xCPU-16GB": {"amount": 1, "price": 99.0}}
                    ]}}))
                }),
            )
    }

    #[tokio::test]
    async fn test_sync_stores_eligible_plans_and_the_whole_zone_price_list() {
        let app = catalog_app(serde_json::json!([
            {"name": "CLOUDNATIVE-6xCPU-16GB", "core_number": 6, "memory_amount": 16384,
             "storage_size": 0, "storage_tier": null},
            {"name": "HICPU-8xCPU-16GB", "core_number": 8, "memory_amount": 16384,
             "storage_size": 200, "storage_tier": "maxiops"},
            {"name": "GPU-8xCPU-64GB-1xL40S", "core_number": 8, "memory_amount": 65536,
             "gpu_amount": 1, "storage_size": 0, "storage_tier": null}
        ]));
        let ctx = ctx_with(crate::providers::upcloud::tests::mock_server(app).await);
        let pool = crate::db::init_pool(":memory:").await.unwrap();

        sync_plans(&pool, &ctx).await.unwrap();
        sync_pricing(&pool, &ctx).await.unwrap();

        let plans: Vec<String> = sqlx::query_scalar("SELECT name FROM provider_plans")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(plans, vec!["CLOUDNATIVE-6xCPU-16GB"]);
        let prices: Vec<(String, f64)> = sqlx::query_as(
            "SELECT price_key, price FROM provider_pricing WHERE zone = 'de-fra1' ORDER BY price_key",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(prices.len(), 3);
        assert_eq!(prices[0], ("ipv4_address".to_string(), 0.4812));
        let other_zones: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM provider_pricing WHERE zone != 'de-fra1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(other_zones, 0);
    }

    #[tokio::test]
    async fn test_sync_keeps_the_cached_plans_when_the_catalog_comes_back_empty() {
        let ctx = ctx_with(
            crate::providers::upcloud::tests::mock_server(catalog_app(serde_json::json!([]))).await,
        );
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        workload::seed_test_plans(&pool).await;

        assert!(sync_plans(&pool, &ctx).await.is_err());
        assert!(workload::has_cached_plans(&pool).await.unwrap());
    }
}
