//! Provider-agnostic cost calculation (Phase 14a) -- real VM + volume
//! cost, computed from cached pricing (`provider_pricing`, synced
//! daily by `reconcile::pricing`, never a live API call per job) and
//! converted to the operator's `main_currency` via `currency::convert`.

use crate::currency;
use crate::reconcile::job::{extract_resource_requests, find_ephemeral_volume};
use crate::volumes::{self, StorageTier};
use crate::workload;
use anyhow::Result;
use serde_json::Value as JsonValue;
use sqlx::{Row, SqlitePool};

/// UpCloud bills both server plans and block storage hourly --
/// verified hands-on against the live pricing catalog:
/// `server_plan_DEV-1xCPU-1GB-10GB`'s own price (`0.4464`) only lines
/// up with UpCloud's published "~€3/month" rate for that plan when
/// read as cents *per hour* (`0.4464 × ~730h/month ≈ 326 cents ≈
/// 3.26 EUR/month`).
const SECONDS_PER_HOUR: f64 = 3600.0;

async fn cached_price(
    pool: &SqlitePool,
    zone: &str,
    price_key: &str,
) -> Result<Option<(f64, f64, String)>> {
    let row = sqlx::query(
        "SELECT amount, price, currency FROM provider_pricing WHERE zone = ? AND price_key = ?",
    )
    .bind(zone)
    .bind(price_key)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| (r.get(0), r.get(1), r.get(2))))
}

/// One real charge -- a job produces one of these for its worker VM,
/// plus a second one if it also has an ephemeral volume. Real FOCUS
/// requirement, found by testing the actual validator rather than
/// assumed from column *names* alone: for any non-`"Correction"`
/// `"Usage"`/`"Purchase"` charge, `SkuId`/`SkuPriceId`/
/// `PricingCategory`/`PricingQuantity`/`ListUnitPrice`/
/// `ContractedUnitPrice`/`ConsumedQuantity`/`ConsumedUnit` all become
/// *mandatory*, not optional -- so a job's VM and volume costs can't
/// be blended into one row the way an earlier version of this module
/// did; FOCUS models one row per charge (per SKU), not per job.
/// `unit_price`/`total` are in the provider's own real billing
/// currency (`providers::PriceEntry`'s own docs) -- never `EUR`
/// assumed.
pub struct ChargeBreakdown {
    pub sku_id: String,
    pub sku_price_id: String,
    /// `"Standard"` always, for kube-shim's own fixed on-demand
    /// pricing -- the real FOCUS `PricingCategory` enum value for
    /// "the agreed-upon rate, no commitment discount, no dynamic
    /// pricing" (verified against the real spec text, not assumed
    /// from the name: `"On Demand"` is *not* one of the four allowed
    /// values).
    pub pricing_category: &'static str,
    /// Hours, always -- UpCloud bills both server plans and block
    /// storage hourly (verified hands-on against the live pricing
    /// catalog, see `charge_breakdown_for_duration`'s own docs).
    pub quantity: f64,
    pub unit: &'static str,
    pub unit_price: f64,
    pub total: f64,
}

/// A job's VM charge, plus a second charge if it also has an
/// ephemeral volume -- in the provider's own real billing currency,
/// for `duration_hours`. `None` if pricing for the job's own plan
/// hasn't been synced yet (the daily sync hasn't run yet, or
/// genuinely can't fit any known plan -- same as
/// `handle_vm_pending`'s own failure case). Not an error either way: a
/// job's cost simply stays unestimated until pricing catches up, the
/// same resilience stance the rest of this project already takes for
/// a transient gap rather than failing outright.
///
/// UpCloud bills both server plans and block storage hourly --
/// verified hands-on against the live pricing catalog:
/// `server_plan_DEV-1xCPU-1GB-10GB`'s own price (`0.4464`) only lines
/// up with UpCloud's published "~€3/month" rate for that plan when
/// read as cents *per hour* (`0.4464 × ~730h/month ≈ 326 cents ≈
/// 3.26 EUR/month`).
async fn charge_breakdown_for_duration(
    pool: &SqlitePool,
    zone: &str,
    spec: &JsonValue,
    duration_hours: f64,
) -> Result<Option<(Vec<ChargeBreakdown>, String)>> {
    let (cpu_millicores, memory_mb) = extract_resource_requests(spec);
    // Same rounding `handle_vm_pending` applies when actually sizing
    // the worker VM -- the cost estimate must size against the exact
    // plan that'll really be provisioned, not a differently-rounded
    // guess of its own.
    let cpu_cores = cpu_millicores.unwrap_or(1000).div_ceil(1000);
    let memory_gb = memory_mb.unwrap_or(1024).div_ceil(1024);
    let Some(plan) = workload::smallest_fitting_server_plan(cpu_cores, memory_gb) else {
        return Ok(None);
    };

    let vm_price_key = format!("server_plan_{}", plan.name);
    let Some((vm_amount, vm_price, provider_currency)) =
        cached_price(pool, zone, &vm_price_key).await?
    else {
        return Ok(None);
    };
    let vm_unit_price = (vm_price / vm_amount) / 100.0;
    let mut charges = vec![ChargeBreakdown {
        sku_id: plan.name.to_string(),
        sku_price_id: vm_price_key,
        pricing_category: "Standard",
        quantity: duration_hours,
        unit: "Hours",
        unit_price: vm_unit_price,
        total: vm_unit_price * duration_hours,
    }];

    if let Some(volume) = find_ephemeral_volume(spec) {
        if let Some(size_gb) = volume
            .pointer("/resources/requests/storage")
            .and_then(JsonValue::as_str)
            .and_then(|q| volumes::parse_storage_quantity_gb(q).ok())
        {
            let tier =
                StorageTier::parse(volume.get("storageClassName").and_then(JsonValue::as_str))
                    .unwrap_or(StorageTier::Standard);
            let storage_price_key = format!("storage_{}", tier.upcloud_tier());
            // Storage pricing not synced yet simply means the
            // volume's own (typically much smaller) charge is left
            // out -- the VM charge still stands on its own rather
            // than failing the whole calculation over a secondary
            // cost.
            if let Some((storage_amount, storage_price, _)) =
                cached_price(pool, zone, &storage_price_key).await?
            {
                // The unit price of using *this specific volume's
                // size* for one hour -- the SKU already bakes in the
                // size, the same way a real "m5.xlarge" SKU bakes in
                // a specific CPU/RAM spec rather than pricing CPU and
                // RAM as separate line items.
                let per_gb_hour = (storage_price / storage_amount) / 100.0;
                let unit_price = per_gb_hour * size_gb as f64;
                charges.push(ChargeBreakdown {
                    sku_id: format!("{}-{size_gb}GB", tier.upcloud_tier()),
                    sku_price_id: storage_price_key,
                    pricing_category: "Standard",
                    quantity: duration_hours,
                    unit: "Hours",
                    unit_price,
                    total: unit_price * duration_hours,
                });
            }
        }
    }

    Ok(Some((charges, provider_currency)))
}

/// A job's worst-case charges before it starts, from its own
/// `activeDeadlineSeconds` -- a true upper bound, not a hopeful guess:
/// Phase 5's admission check guarantees every job has this set, and
/// Phase 11 force-kills the VM if it's ever exceeded, so nothing can
/// run past the duration this estimate assumes. Used directly by
/// `api::cost_report` (one CSV row per charge); `estimate_job_cost`
/// below just sums these for the single aggregate `jobs.estimated_cost`
/// Phase 14b's budget guard needs.
pub async fn estimate_job_charges(
    pool: &SqlitePool,
    zone: &str,
    spec: &JsonValue,
    active_deadline_seconds: i64,
) -> Result<Option<(Vec<ChargeBreakdown>, String)>> {
    let duration_hours = active_deadline_seconds.max(0) as f64 / SECONDS_PER_HOUR;
    charge_breakdown_for_duration(pool, zone, spec, duration_hours).await
}

/// A job's real charges once it's actually finished, from its real
/// elapsed wall-clock duration -- not the conservative
/// `activeDeadlineSeconds` estimate `estimate_job_charges` uses. Used
/// directly by `api::cost_report`; `calculate_actual_cost` below just
/// sums these for the single aggregate `jobs.actual_cost`.
pub async fn actual_job_charges(
    pool: &SqlitePool,
    zone: &str,
    spec: &JsonValue,
    duration_seconds: i64,
) -> Result<Option<(Vec<ChargeBreakdown>, String)>> {
    let duration_hours = duration_seconds.max(0) as f64 / SECONDS_PER_HOUR;
    charge_breakdown_for_duration(pool, zone, spec, duration_hours).await
}

/// A job's worst-case *total* cost before it starts, converted to
/// `main_currency` -- the single aggregate Phase 14b's budget guard
/// compares against the balance; it doesn't need the per-charge detail
/// `estimate_job_charges` provides for the FOCUS report.
pub async fn estimate_job_cost(
    pool: &SqlitePool,
    zone: &str,
    spec: &JsonValue,
    active_deadline_seconds: i64,
    main_currency: &str,
) -> Result<Option<f64>> {
    let Some((charges, provider_currency)) =
        estimate_job_charges(pool, zone, spec, active_deadline_seconds).await?
    else {
        return Ok(None);
    };
    let total: f64 = charges.iter().map(|c| c.total).sum();
    currency::convert(pool, total, &provider_currency, main_currency).await
}

/// A job's real *total* cost once it's actually finished, converted to
/// `main_currency` -- see `estimate_job_cost`'s own docs for why this
/// is just a sum over `actual_job_charges`.
pub async fn calculate_actual_cost(
    pool: &SqlitePool,
    zone: &str,
    spec: &JsonValue,
    duration_seconds: i64,
    main_currency: &str,
) -> Result<Option<f64>> {
    let Some((charges, provider_currency)) =
        actual_job_charges(pool, zone, spec, duration_seconds).await?
    else {
        return Ok(None);
    };
    let total: f64 = charges.iter().map(|c| c.total).sum();
    currency::convert(pool, total, &provider_currency, main_currency).await
}

/// The provider's own real billing currency, read back from whichever
/// cached price was synced most recently -- every cached entry shares
/// the same currency (it's an account-wide setting, not a per-plan
/// one), so any one row answers this. `None` means pricing hasn't
/// synced at all yet. Used by `api::cost_report` (Phase 14a) to show
/// `BilledCost`/`EffectiveCost`/etc. in the provider's real currency
/// alongside their `main_currency`-converted counterparts, without
/// needing to store the provider-currency amount on every job row.
pub async fn provider_currency(pool: &SqlitePool) -> Result<Option<String>> {
    let row = sqlx::query("SELECT currency FROM provider_pricing LIMIT 1")
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| r.get(0)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    async fn insert_price(
        pool: &SqlitePool,
        zone: &str,
        key: &str,
        amount: f64,
        price: f64,
        currency: &str,
    ) {
        sqlx::query(
            "INSERT INTO provider_pricing (zone, price_key, amount, price, currency, fetched_at) \
             VALUES (?, ?, ?, ?, ?, 0)",
        )
        .bind(zone)
        .bind(key)
        .bind(amount)
        .bind(price)
        .bind(currency)
        .execute(pool)
        .await
        .unwrap();
    }

    fn vm_only_spec() -> JsonValue {
        json!({
            "template": {"spec": {
                "containers": [{"resources": {"requests": {"cpu": "1", "memory": "1Gi"}}}]
            }}
        })
    }

    fn vm_and_volume_spec() -> JsonValue {
        json!({
            "template": {"spec": {
                "containers": [{"resources": {"requests": {"cpu": "1", "memory": "1Gi"}}}],
                "volumes": [{"ephemeral": {"volumeClaimTemplate": {"spec": {
                    "resources": {"requests": {"storage": "10Gi"}},
                    "storageClassName": "kube-shim-standard"
                }}}}]
            }}
        })
    }

    #[tokio::test]
    async fn test_estimate_vm_only_job() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_price(
            &pool,
            "de-fra1",
            "server_plan_DEV-1xCPU-1GB-10GB",
            1.0,
            0.4464,
            "EUR",
        )
        .await;

        // 3600s = 1 hour -> exactly one hour of the VM's own cents/hour price.
        let cost = estimate_job_cost(&pool, "de-fra1", &vm_only_spec(), 3600, "EUR")
            .await
            .unwrap()
            .unwrap();
        assert!((cost - 0.004464).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_estimate_vm_and_volume_job() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_price(
            &pool,
            "de-fra1",
            "server_plan_DEV-1xCPU-1GB-10GB",
            1.0,
            0.4464,
            "EUR",
        )
        .await;
        insert_price(&pool, "de-fra1", "storage_standard", 1.0, 0.0118, "EUR").await;

        // 1 hour: VM 0.4464 cents + 10GB * 0.0118 cents/GB = 0.4464 + 0.118 = 0.5644 cents
        let cost = estimate_job_cost(&pool, "de-fra1", &vm_and_volume_spec(), 3600, "EUR")
            .await
            .unwrap()
            .unwrap();
        assert!((cost - 0.005644).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_estimate_without_volume_pricing_still_returns_vm_cost_alone() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_price(
            &pool,
            "de-fra1",
            "server_plan_DEV-1xCPU-1GB-10GB",
            1.0,
            0.4464,
            "EUR",
        )
        .await;
        // storage_standard deliberately not inserted.

        let cost = estimate_job_cost(&pool, "de-fra1", &vm_and_volume_spec(), 3600, "EUR")
            .await
            .unwrap()
            .unwrap();
        assert!((cost - 0.004464).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_estimate_with_no_pricing_synced_yet_is_none() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let cost = estimate_job_cost(&pool, "de-fra1", &vm_only_spec(), 3600, "EUR")
            .await
            .unwrap();
        assert_eq!(cost, None);
    }

    #[tokio::test]
    async fn test_estimate_with_no_fitting_plan_is_none() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_price(
            &pool,
            "de-fra1",
            "server_plan_DEV-1xCPU-1GB-10GB",
            1.0,
            0.4464,
            "EUR",
        )
        .await;
        let huge_spec = json!({
            "template": {"spec": {
                "containers": [{"resources": {"requests": {"cpu": "999", "memory": "999Gi"}}}]
            }}
        });
        let cost = estimate_job_cost(&pool, "de-fra1", &huge_spec, 3600, "EUR")
            .await
            .unwrap();
        assert_eq!(cost, None);
    }

    #[tokio::test]
    async fn test_estimate_converts_to_a_different_main_currency() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_price(
            &pool,
            "de-fra1",
            "server_plan_DEV-1xCPU-1GB-10GB",
            1.0,
            0.4464,
            "EUR",
        )
        .await;
        currency::store_rates(
            &pool,
            &[currency::ExchangeRate {
                currency: "CHF".to_string(),
                rate: 0.9359,
            }],
            0,
        )
        .await
        .unwrap();

        let cost_eur = estimate_job_cost(&pool, "de-fra1", &vm_only_spec(), 3600, "EUR")
            .await
            .unwrap()
            .unwrap();
        let cost_chf = estimate_job_cost(&pool, "de-fra1", &vm_only_spec(), 3600, "CHF")
            .await
            .unwrap()
            .unwrap();
        assert!((cost_chf - cost_eur * 0.9359).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_estimate_in_a_different_main_currency_with_no_rate_synced_is_none() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_price(
            &pool,
            "de-fra1",
            "server_plan_DEV-1xCPU-1GB-10GB",
            1.0,
            0.4464,
            "EUR",
        )
        .await;
        // No CHF rate stored.
        let cost = estimate_job_cost(&pool, "de-fra1", &vm_only_spec(), 3600, "CHF")
            .await
            .unwrap();
        assert_eq!(cost, None);
    }

    #[tokio::test]
    async fn test_actual_cost_uses_real_duration_not_the_deadline() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_price(
            &pool,
            "de-fra1",
            "server_plan_DEV-1xCPU-1GB-10GB",
            1.0,
            0.4464,
            "EUR",
        )
        .await;

        // Real run was only 30 minutes (1800s), half of the 1-hour cost.
        let cost = calculate_actual_cost(&pool, "de-fra1", &vm_only_spec(), 1800, "EUR")
            .await
            .unwrap()
            .unwrap();
        assert!((cost - 0.002232).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_a_provider_billing_in_usd_is_handled_without_hardcoding_eur() {
        // The exact real-world case that prompted this: UpCloud bills
        // some accounts in USD, not just EUR.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_price(
            &pool,
            "us-chi1",
            "server_plan_DEV-1xCPU-1GB-10GB",
            1.0,
            0.5,
            "USD",
        )
        .await;
        currency::store_rates(
            &pool,
            &[currency::ExchangeRate {
                currency: "USD".to_string(),
                rate: 1.1269,
            }],
            0,
        )
        .await
        .unwrap();

        let cost_usd = estimate_job_cost(&pool, "us-chi1", &vm_only_spec(), 3600, "USD")
            .await
            .unwrap()
            .unwrap();
        assert!((cost_usd - 0.005).abs() < 1e-9);

        let cost_eur = estimate_job_cost(&pool, "us-chi1", &vm_only_spec(), 3600, "EUR")
            .await
            .unwrap()
            .unwrap();
        assert!((cost_eur - cost_usd / 1.1269).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_charge_breakdown_vm_only_is_a_single_charge() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_price(
            &pool,
            "de-fra1",
            "server_plan_DEV-1xCPU-1GB-10GB",
            1.0,
            0.4464,
            "EUR",
        )
        .await;

        let (charges, currency) = estimate_job_charges(&pool, "de-fra1", &vm_only_spec(), 3600)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(currency, "EUR");
        assert_eq!(charges.len(), 1);
        assert_eq!(charges[0].sku_id, "DEV-1xCPU-1GB-10GB");
        assert_eq!(charges[0].sku_price_id, "server_plan_DEV-1xCPU-1GB-10GB");
        assert_eq!(charges[0].pricing_category, "Standard");
        assert_eq!(charges[0].quantity, 1.0); // 1 hour
        assert_eq!(charges[0].unit, "Hours");
        assert!((charges[0].unit_price - 0.004464).abs() < 1e-9);
        assert!((charges[0].total - 0.004464).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_charge_breakdown_with_volume_is_two_separate_charges() {
        // The real finding that drove this redesign: FOCUS requires
        // real per-SKU detail for every non-Correction Usage charge,
        // so a job's VM and volume costs can't be blended into one
        // row -- they're two distinct charges, each with their own
        // SkuId/quantity/unit price.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_price(
            &pool,
            "de-fra1",
            "server_plan_DEV-1xCPU-1GB-10GB",
            1.0,
            0.4464,
            "EUR",
        )
        .await;
        insert_price(&pool, "de-fra1", "storage_standard", 1.0, 0.0118, "EUR").await;

        let (charges, _) = estimate_job_charges(&pool, "de-fra1", &vm_and_volume_spec(), 3600)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(charges.len(), 2);
        assert_eq!(charges[0].sku_id, "DEV-1xCPU-1GB-10GB");
        assert!((charges[0].total - 0.004464).abs() < 1e-9);
        assert_eq!(charges[1].sku_id, "standard-10GB");
        assert_eq!(charges[1].sku_price_id, "storage_standard");
        // 10GB * 0.0118 cents/GB/hour / 100 = 0.00118 per hour, for 1 hour.
        assert!((charges[1].total - 0.00118).abs() < 1e-9);

        let total: f64 = charges.iter().map(|c| c.total).sum();
        assert!((total - 0.005644).abs() < 1e-9); // matches the old blended total
    }
}
