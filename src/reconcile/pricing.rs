//! Daily background sync of UpCloud pricing + ECB exchange rates
//! (Phase 14a) -- `src/pricing.rs`'s cost calculation reads from these
//! cached tables, never hitting either API live per job.

use crate::currency;
use crate::reconcile::JobContext;
use crate::volumes::StorageTier;
use crate::workload;
use anyhow::Result;
use chrono::Utc;
use sqlx::SqlitePool;

/// Every `(zone, price_key)` this project actually needs priced --
/// every known server plan (`workload::all_server_plans()`) plus both
/// `StorageTier`s -- not UpCloud's whole catalog, which also covers
/// managed databases, object storage, and other services this project
/// never provisions.
fn needed_price_keys() -> Vec<String> {
    let mut keys: Vec<String> = workload::all_server_plans()
        .iter()
        .map(|plan| format!("server_plan_{}", plan.name))
        .collect();
    for tier in [StorageTier::Standard, StorageTier::Fast] {
        keys.push(format!("storage_{}", tier.upcloud_tier()));
    }
    keys
}

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
pub async fn has_cached_pricing(pool: &SqlitePool) -> Result<bool> {
    let pricing: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_pricing")
        .fetch_one(pool)
        .await?;
    let rates: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM exchange_rates")
        .fetch_one(pool)
        .await?;
    Ok(pricing > 0 && rates > 0)
}

/// One pass: sync pricing, then exchange rates. Each half tolerates
/// the other failing independently (a `?` on the first alone would
/// otherwise skip exchange-rate sync entirely just because, say,
/// UpCloud was briefly unreachable) -- logged, not fatal, same
/// resilience stance `reconcile::metrics_collector` already takes for
/// a single bad poll.
pub async fn sync_pricing_and_rates(pool: &SqlitePool, ctx: &JobContext) -> Result<()> {
    if let Err(err) = sync_pricing(pool, ctx).await {
        tracing::error!("pricing sync failed: {err:?}");
    }
    if let Err(err) = sync_exchange_rates(pool).await {
        tracing::error!("exchange rate sync failed: {err:?}");
    }
    Ok(())
}

async fn sync_pricing(pool: &SqlitePool, ctx: &JobContext) -> Result<()> {
    if ctx.dry_run {
        tracing::debug!("DRY-RUN: skipping real pricing sync");
        return Ok(());
    }

    let now = Utc::now().timestamp();
    for price_key in needed_price_keys() {
        match ctx.provider.get_pricing(&ctx.zone, &price_key).await {
            Ok(entry) => {
                sqlx::query(
                    "INSERT INTO provider_pricing (zone, price_key, amount, price, currency, fetched_at) \
                     VALUES (?, ?, ?, ?, ?, ?) \
                     ON CONFLICT(zone, price_key) DO UPDATE SET \
                         amount = excluded.amount, price = excluded.price, \
                         currency = excluded.currency, fetched_at = excluded.fetched_at",
                )
                .bind(&ctx.zone)
                .bind(&price_key)
                .bind(entry.amount)
                .bind(entry.price)
                .bind(&entry.currency)
                .bind(now)
                .execute(pool)
                .await?;
            }
            Err(err) => {
                // One missing/unreachable price key doesn't block the
                // rest -- a job needing exactly this one simply stays
                // unestimated until the next sync, same as any other
                // transient pricing gap `src/pricing.rs` already
                // tolerates.
                tracing::warn!(
                    "pricing sync: could not fetch {price_key} for zone {}: {err}",
                    ctx.zone
                );
            }
        }
    }
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
    async fn test_has_cached_pricing_true_when_both_present() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_price(&pool).await;
        insert_rate(&pool).await;
        assert!(has_cached_pricing(&pool).await.unwrap());
    }
}
