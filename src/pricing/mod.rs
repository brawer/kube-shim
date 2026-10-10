//! Provider-agnostic cost calculation (Phase 14a) -- real VM + volume
//! cost, computed from cached pricing (`provider_pricing`, synced
//! daily by `reconcile::pricing`, never a live API call per job) and
//! converted to the operator's `main_currency` via `currency::convert`.

use crate::currency;
use crate::reconcile::job::{extract_resource_requests, find_ephemeral_volume, is_committed_state};
use crate::volumes::{self, StorageTier};
use crate::workload;
use anyhow::Result;
use chrono::Utc;
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

/// Rolling budget guard (Phase 14b): the three settings `PATCH
/// /settings` (`api::budget`) can change live, bundled together since
/// they're always read and written as one row (`budget_state`).
#[derive(Debug, Clone, PartialEq)]
pub struct BudgetSettings {
    pub main_currency: String,
    pub budget_daily_rate: f64,
    pub budget_rollover_cap_days: u32,
}

/// A subset of `BudgetSettings`' own fields a `PATCH /settings` call
/// wants to change -- `None` on a field means "leave it alone", the
/// real PATCH semantics (not "set to a default"), so a caller only
/// ever sends the fields it actually wants to update.
#[derive(Debug, Default)]
pub struct SettingsPatch {
    pub budget_daily_rate: Option<f64>,
    pub budget_rollover_cap_days: Option<u32>,
    pub main_currency: Option<String>,
}

/// The real outcome of a `PATCH /settings` call -- the settled
/// balance and final settings, plus the real before/after numbers if
/// `main_currency` actually changed, for `api::budget` to record as
/// an event.
#[derive(Debug)]
pub struct SettingsPatchOutcome {
    pub settings: BudgetSettings,
    pub balance: f64,
    /// `(old_currency, old_balance, new_currency, new_balance)`.
    pub currency_conversion: Option<(String, f64, String, f64)>,
}

#[derive(Debug, thiserror::Error)]
pub enum BudgetError {
    #[error("budget settings have not been initialized")]
    NotSeeded,
    #[error("cannot convert {from} to {to}: no exchange rate cached for one of them")]
    CurrencyConversionUnavailable { from: String, to: String },
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

struct BudgetRow {
    balance: f64,
    last_accrual_at: i64,
    main_currency: String,
    budget_daily_rate: f64,
    budget_rollover_cap_days: u32,
}

async fn fetch_budget_row(pool: &SqlitePool) -> Result<Option<BudgetRow>> {
    let row = sqlx::query(
        "SELECT balance, last_accrual_at, main_currency, budget_daily_rate, \
         budget_rollover_cap_days FROM budget_state WHERE id = 1",
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| BudgetRow {
        balance: r.get(0),
        last_accrual_at: r.get(1),
        main_currency: r.get(2),
        budget_daily_rate: r.get(3),
        budget_rollover_cap_days: r.get(4),
    }))
}

/// Seeds the single `budget_state` row from `config.toml`'s `[shim]`
/// fields -- but only on a genuine first boot (no row yet); every
/// later boot leaves it alone, since `PATCH /settings` may have since
/// changed these live and `config.toml` is never re-read for this
/// (same bootstrap-once convention `server.api_tokens` already uses).
/// A no-op if a row already exists.
pub async fn ensure_budget_seeded(pool: &SqlitePool, seed: &BudgetSettings) -> Result<()> {
    if fetch_budget_row(pool).await?.is_some() {
        return Ok(());
    }
    let now = Utc::now().timestamp();
    sqlx::query(
        "INSERT INTO budget_state \
         (id, balance, last_accrual_at, main_currency, budget_daily_rate, budget_rollover_cap_days) \
         VALUES (1, 0.0, ?, ?, ?, ?)",
    )
    .bind(now)
    .bind(&seed.main_currency)
    .bind(seed.budget_daily_rate)
    .bind(seed.budget_rollover_cap_days)
    .execute(pool)
    .await?;
    Ok(())
}

/// The live settings, or `None` if `budget_state` has never been
/// seeded (e.g. a bare test pool, or a process that hasn't called
/// `ensure_budget_seeded` yet).
pub async fn current_settings(pool: &SqlitePool) -> Result<Option<BudgetSettings>> {
    Ok(fetch_budget_row(pool).await?.map(|r| BudgetSettings {
        main_currency: r.main_currency,
        budget_daily_rate: r.budget_daily_rate,
        budget_rollover_cap_days: r.budget_rollover_cap_days,
    }))
}

/// The live `main_currency` setting if `budget_state` has been seeded,
/// else `fallback` -- the `config.toml`-sourced value every cost
/// calculation call site (`reconcile::job`, `api::cost_report`) had
/// before this table existed. Lets a `PATCH /settings` currency change
/// take effect immediately everywhere, without needing `JobContext`/
/// `CostReportConfig` to carry a mutable copy of their own.
pub async fn effective_main_currency(pool: &SqlitePool, fallback: &str) -> Result<String> {
    Ok(current_settings(pool)
        .await?
        .map(|s| s.main_currency)
        .unwrap_or_else(|| fallback.to_string()))
}

/// Settles time-based accrual into the stored balance -- the real
/// elapsed time since `last_accrual_at`, at `budget_daily_rate` per
/// day, capped so *accrual alone* never pushes the balance past
/// `budget_rollover_cap_days` worth -- several quiet days build up
/// headroom for one bigger job, without growing unbounded if jobs
/// never run. Deliberately a cap on accrual, not a hard ceiling on the
/// balance itself: a manual top-up can legitimately push the balance
/// above that cap on purpose (that's the whole point of being able to
/// top up at all), and this must never claw it back down again on the
/// next settle just because accrual's own cap says so -- only the
/// *accrued* portion is ever limited by how much headroom is left
/// below the cap, never applied to money that arrived another way.
/// The only place that both computes and persists this, so every real
/// caller (the budget-admission check, `GET`/`PATCH /settings`, a
/// top-up) sees the same real-time-correct number instead of a stale
/// tick's worth -- no separate periodic "accrual tick" exists for
/// this reason; settling lazily on read is simpler and always
/// correct-as-of-now, not just correct-as-of-the-last-tick.
///
/// `None` if `budget_state` has never been seeded. A no-op (returns
/// the stored balance unchanged, doesn't touch `last_accrual_at`)
/// when `budget_daily_rate <= 0.0` -- this project's own "budgeting
/// disabled" convention (also `ShimConfig::default()`'s own value, so
/// an already-deployed instance that's never configured a budget
/// keeps behaving exactly as before this phase existed).
pub async fn settle_accrual(pool: &SqlitePool) -> Result<Option<(f64, BudgetSettings)>> {
    let Some(row) = fetch_budget_row(pool).await? else {
        return Ok(None);
    };
    let settings = BudgetSettings {
        main_currency: row.main_currency.clone(),
        budget_daily_rate: row.budget_daily_rate,
        budget_rollover_cap_days: row.budget_rollover_cap_days,
    };
    if row.budget_daily_rate <= 0.0 {
        return Ok(Some((row.balance, settings)));
    }
    let now = Utc::now().timestamp();
    let elapsed_days = (now - row.last_accrual_at).max(0) as f64 / 86400.0;
    let cap = row.budget_daily_rate * row.budget_rollover_cap_days as f64;
    let room_left_below_cap = (cap - row.balance).max(0.0);
    let accrued = (row.budget_daily_rate * elapsed_days).min(room_left_below_cap);
    let new_balance = row.balance + accrued;
    sqlx::query("UPDATE budget_state SET balance = ?, last_accrual_at = ? WHERE id = 1")
        .bind(new_balance)
        .bind(now)
        .execute(pool)
        .await?;
    Ok(Some((new_balance, settings)))
}

/// Every other currently in-flight job's `estimated_cost` that's
/// already started real provisioning (`reconcile::job::
/// is_committed_state`) -- a job still sitting in `Created`/
/// `BudgetWait` hasn't consumed anything yet, so it doesn't count
/// here; it's the thing being checked against this sum, not a
/// contributor to it. Reuses `reconcile::job`'s own state ordering
/// rather than hardcoding a second copy of "which states count" here.
async fn committed_budget(pool: &SqlitePool) -> Result<f64> {
    let rows: Vec<(String, f64)> = sqlx::query_as(
        "SELECT status, estimated_cost FROM jobs WHERE estimated_cost IS NOT NULL AND status != ?",
    )
    .bind(crate::reconcile::job::TERMINAL_STATE)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .filter(|(status, _)| is_committed_state(status))
        .map(|(_, cost)| cost)
        .sum())
}

/// `None` means budgeting is disabled (no settings row, or
/// `budget_daily_rate <= 0`) -- callers should treat every job as
/// always permitted to launch. `Some(available)` is the real,
/// accrual-settled balance minus every other already-launched job's
/// estimate (`committed_budget`) -- can go negative if committed
/// spend already exceeds the balance (e.g. right after a currency
/// conversion), in which case nothing new should launch until it
/// recovers.
pub async fn available_budget(pool: &SqlitePool) -> Result<Option<f64>> {
    let Some((balance, settings)) = settle_accrual(pool).await? else {
        return Ok(None);
    };
    if settings.budget_daily_rate <= 0.0 {
        return Ok(None);
    }
    let committed = committed_budget(pool).await?;
    Ok(Some(balance - committed))
}

/// Adds `amount` directly to the current balance (after settling
/// accrual first, so a pending-but-unsettled accrual isn't lost under
/// the top-up). Deliberately additive, not a "set to X" call: the
/// caller never needs to know the current balance to compute the
/// right number, and an additive update can't race against accrual
/// settling the way a "set absolute" one would (no read-then-write
/// gap for a concurrent accrual to land in).
pub async fn apply_topup(pool: &SqlitePool, amount: f64) -> Result<f64, BudgetError> {
    let Some((balance, _)) = settle_accrual(pool).await? else {
        return Err(BudgetError::NotSeeded);
    };
    let new_balance = balance + amount;
    sqlx::query("UPDATE budget_state SET balance = ? WHERE id = 1")
        .bind(new_balance)
        .execute(pool)
        .await
        .map_err(|e| BudgetError::Internal(e.into()))?;
    Ok(new_balance)
}

/// Applies a `PATCH /settings` request. A `main_currency` change
/// settles accrual first (in the *old* currency, so the balance being
/// converted is genuinely up to date), then converts both the balance
/// and `budget_daily_rate` through the day's ECB rate -- not a silent
/// relabeling: a balance of `100` doesn't mean the same thing as `100
/// CHF` once `main_currency` becomes `EUR`, and `budget_daily_rate` is
/// denominated in `main_currency` too (`config.rs`'s own docs), so
/// leaving it as a bare number under the new currency would silently
/// change the real accrual rate. `budget_daily_rate` is only
/// auto-converted this way when the same request isn't *also*
/// explicitly overriding it -- an explicit value always wins over an
/// implicit conversion of the old one.
pub async fn patch_settings(
    pool: &SqlitePool,
    patch: SettingsPatch,
) -> Result<SettingsPatchOutcome, BudgetError> {
    let Some((settled_balance, mut settings)) = settle_accrual(pool).await? else {
        return Err(BudgetError::NotSeeded);
    };
    let mut balance = settled_balance;
    let mut currency_conversion = None;

    if let Some(target_currency) = &patch.main_currency {
        if *target_currency != settings.main_currency {
            let converted_balance =
                currency::convert(pool, balance, &settings.main_currency, target_currency)
                    .await?
                    .ok_or_else(|| BudgetError::CurrencyConversionUnavailable {
                        from: settings.main_currency.clone(),
                        to: target_currency.clone(),
                    })?;
            let new_daily_rate = if patch.budget_daily_rate.is_none() {
                Some(
                    currency::convert(
                        pool,
                        settings.budget_daily_rate,
                        &settings.main_currency,
                        target_currency,
                    )
                    .await?
                    .ok_or_else(|| {
                        BudgetError::CurrencyConversionUnavailable {
                            from: settings.main_currency.clone(),
                            to: target_currency.clone(),
                        }
                    })?,
                )
            } else {
                None
            };
            currency_conversion = Some((
                settings.main_currency.clone(),
                balance,
                target_currency.clone(),
                converted_balance,
            ));
            balance = converted_balance;
            if let Some(rate) = new_daily_rate {
                settings.budget_daily_rate = rate;
            }
            settings.main_currency = target_currency.clone();
        }
    }
    if let Some(rate) = patch.budget_daily_rate {
        settings.budget_daily_rate = rate;
    }
    if let Some(days) = patch.budget_rollover_cap_days {
        settings.budget_rollover_cap_days = days;
    }

    sqlx::query(
        "UPDATE budget_state SET balance = ?, main_currency = ?, budget_daily_rate = ?, \
         budget_rollover_cap_days = ? WHERE id = 1",
    )
    .bind(balance)
    .bind(&settings.main_currency)
    .bind(settings.budget_daily_rate)
    .bind(settings.budget_rollover_cap_days)
    .execute(pool)
    .await
    .map_err(|e| BudgetError::Internal(e.into()))?;

    Ok(SettingsPatchOutcome {
        settings,
        balance,
        currency_conversion,
    })
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

    // --- Phase 14b: rolling budget guard ---

    async fn seed_budget_row(
        pool: &SqlitePool,
        balance: f64,
        last_accrual_at: i64,
        currency: &str,
        daily_rate: f64,
        rollover_cap_days: u32,
    ) {
        sqlx::query(
            "INSERT INTO budget_state \
             (id, balance, last_accrual_at, main_currency, budget_daily_rate, budget_rollover_cap_days) \
             VALUES (1, ?, ?, ?, ?, ?)",
        )
        .bind(balance)
        .bind(last_accrual_at)
        .bind(currency)
        .bind(daily_rate)
        .bind(rollover_cap_days)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_job_with_estimate(pool: &SqlitePool, id: &str, status: &str, estimate: f64) {
        let now = chrono::Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, estimated_cost, created_at, updated_at, version) \
             VALUES (?, ?, 'default', '{}', ?, ?, ?, ?, 1)",
        )
        .bind(id)
        .bind(id)
        .bind(status)
        .bind(estimate)
        .bind(now)
        .bind(now)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_ensure_budget_seeded_seeds_once_from_config_defaults() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let seed = BudgetSettings {
            main_currency: "CHF".to_string(),
            budget_daily_rate: 2.0,
            budget_rollover_cap_days: 7,
        };
        ensure_budget_seeded(&pool, &seed).await.unwrap();

        let settings = current_settings(&pool).await.unwrap().unwrap();
        assert_eq!(settings, seed);
        let (balance, _) = settle_accrual(&pool).await.unwrap().unwrap();
        assert_eq!(balance, 0.0);
    }

    #[tokio::test]
    async fn test_ensure_budget_seeded_is_a_noop_if_already_seeded() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        ensure_budget_seeded(
            &pool,
            &BudgetSettings {
                main_currency: "EUR".to_string(),
                budget_daily_rate: 2.0,
                budget_rollover_cap_days: 7,
            },
        )
        .await
        .unwrap();
        // A later boot's config.toml seed (e.g. after PATCH /settings
        // already changed the live value) must not overwrite it.
        ensure_budget_seeded(
            &pool,
            &BudgetSettings {
                main_currency: "USD".to_string(),
                budget_daily_rate: 99.0,
                budget_rollover_cap_days: 1,
            },
        )
        .await
        .unwrap();

        let settings = current_settings(&pool).await.unwrap().unwrap();
        assert_eq!(settings.main_currency, "EUR");
        assert_eq!(settings.budget_daily_rate, 2.0);
    }

    #[tokio::test]
    async fn test_settle_accrual_accrues_at_the_daily_rate() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = chrono::Utc::now().timestamp();
        let half_day_ago = now - 12 * 3600;
        seed_budget_row(&pool, 0.0, half_day_ago, "EUR", 2.0, 7).await;

        let (balance, _) = settle_accrual(&pool).await.unwrap().unwrap();
        // Half a day at 2.0/day.
        assert!((balance - 1.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_settle_accrual_caps_at_the_rollover_limit() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = chrono::Utc::now().timestamp();
        let ten_days_ago = now - 10 * 86400;
        // 10 days at 2.0/day would be 20, but the cap is 7 days (14.0).
        seed_budget_row(&pool, 0.0, ten_days_ago, "EUR", 2.0, 7).await;

        let (balance, _) = settle_accrual(&pool).await.unwrap().unwrap();
        assert!((balance - 14.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_settle_accrual_caps_an_already_saturated_balance_too() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = chrono::Utc::now().timestamp();
        let one_day_ago = now - 86400;
        // Already at the cap; one more day must not push it past it.
        seed_budget_row(&pool, 14.0, one_day_ago, "EUR", 2.0, 7).await;

        let (balance, _) = settle_accrual(&pool).await.unwrap().unwrap();
        assert!((balance - 14.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_settle_accrual_never_claws_back_a_balance_above_the_cap_from_a_topup() {
        // A manual top-up can legitimately push the balance above the
        // accrual cap on purpose -- a later settle must not reduce it
        // back down to the cap, which would silently destroy the
        // top-up. This is exactly the real bug `apply_topup`'s own test
        // caught: a top-up immediately followed by a settle used to
        // clamp 25.0 back down to the 14.0 accrual cap.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = chrono::Utc::now().timestamp();
        // Cap is 2.0 * 7 = 14.0; balance of 25.0 is already above it.
        seed_budget_row(&pool, 25.0, now, "EUR", 2.0, 7).await;

        let (balance, _) = settle_accrual(&pool).await.unwrap().unwrap();
        assert!((balance - 25.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_settle_accrual_is_a_noop_when_disabled() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = chrono::Utc::now().timestamp();
        let long_ago = now - 30 * 86400;
        seed_budget_row(&pool, 5.0, long_ago, "EUR", 0.0, 7).await;

        let (balance, _) = settle_accrual(&pool).await.unwrap().unwrap();
        assert_eq!(balance, 5.0);
        // last_accrual_at must also be left untouched.
        let row = fetch_budget_row(&pool).await.unwrap().unwrap();
        assert_eq!(row.last_accrual_at, long_ago);
    }

    #[tokio::test]
    async fn test_settle_accrual_none_when_not_seeded() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        assert_eq!(settle_accrual(&pool).await.unwrap(), None);
        assert_eq!(current_settings(&pool).await.unwrap(), None);
        assert_eq!(available_budget(&pool).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_available_budget_none_when_disabled() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        seed_budget_row(&pool, 100.0, chrono::Utc::now().timestamp(), "EUR", 0.0, 7).await;
        assert_eq!(available_budget(&pool).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_available_budget_subtracts_only_already_launched_jobs() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        seed_budget_row(&pool, 10.0, chrono::Utc::now().timestamp(), "EUR", 2.0, 7).await;
        // Already launched -- counts against the balance.
        insert_job_with_estimate(&pool, "launched", "VolumePending", 3.0).await;
        // Still waiting its turn -- must not count twice (it's the one
        // being checked, not a contributor), and Created hasn't
        // launched either.
        insert_job_with_estimate(&pool, "waiting", "BudgetWait", 4.0).await;
        insert_job_with_estimate(&pool, "fresh", "Created", 1.0).await;
        // Finished -- long done consuming anything.
        insert_job_with_estimate(&pool, "done", "Succeeded", 2.0).await;

        let available = available_budget(&pool).await.unwrap().unwrap();
        assert!((available - 7.0).abs() < 1e-6); // 10 - 3 (launched only)
    }

    #[tokio::test]
    async fn test_apply_topup_adds_to_the_settled_balance() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        seed_budget_row(&pool, 5.0, chrono::Utc::now().timestamp(), "EUR", 2.0, 7).await;

        let balance = apply_topup(&pool, 20.0).await.unwrap();
        assert!((balance - 25.0).abs() < 1e-6);
        let (settled, _) = settle_accrual(&pool).await.unwrap().unwrap();
        assert!((settled - 25.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_apply_topup_not_seeded_is_an_error() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let err = apply_topup(&pool, 20.0).await.unwrap_err();
        assert!(matches!(err, BudgetError::NotSeeded));
    }

    #[tokio::test]
    async fn test_patch_settings_updates_rate_and_cap_days_without_touching_currency() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        seed_budget_row(&pool, 10.0, chrono::Utc::now().timestamp(), "EUR", 2.0, 7).await;

        let outcome = patch_settings(
            &pool,
            SettingsPatch {
                budget_daily_rate: Some(3.0),
                budget_rollover_cap_days: Some(14),
                main_currency: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(outcome.settings.budget_daily_rate, 3.0);
        assert_eq!(outcome.settings.budget_rollover_cap_days, 14);
        assert_eq!(outcome.settings.main_currency, "EUR");
        assert!(outcome.currency_conversion.is_none());
        assert!((outcome.balance - 10.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_patch_settings_converts_balance_and_daily_rate_on_currency_change() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        seed_budget_row(&pool, 10.0, chrono::Utc::now().timestamp(), "EUR", 2.0, 7).await;
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

        let outcome = patch_settings(
            &pool,
            SettingsPatch {
                budget_daily_rate: None,
                budget_rollover_cap_days: None,
                main_currency: Some("CHF".to_string()),
            },
        )
        .await
        .unwrap();

        assert_eq!(outcome.settings.main_currency, "CHF");
        // Both the balance and the daily rate convert by the same rate.
        assert!((outcome.balance - 10.0 * 0.9359).abs() < 1e-6);
        assert!((outcome.settings.budget_daily_rate - 2.0 * 0.9359).abs() < 1e-6);
        let (old_currency, old_balance, new_currency, new_balance) =
            outcome.currency_conversion.unwrap();
        assert_eq!(old_currency, "EUR");
        assert_eq!(new_currency, "CHF");
        assert!((old_balance - 10.0).abs() < 1e-6);
        assert!((new_balance - 10.0 * 0.9359).abs() < 1e-6);

        // Persisted, not just returned.
        let settings = current_settings(&pool).await.unwrap().unwrap();
        assert_eq!(settings.main_currency, "CHF");
    }

    #[tokio::test]
    async fn test_patch_settings_explicit_daily_rate_overrides_auto_conversion() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        seed_budget_row(&pool, 10.0, chrono::Utc::now().timestamp(), "EUR", 2.0, 7).await;
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

        let outcome = patch_settings(
            &pool,
            SettingsPatch {
                budget_daily_rate: Some(5.0),
                budget_rollover_cap_days: None,
                main_currency: Some("CHF".to_string()),
            },
        )
        .await
        .unwrap();

        // The explicit value wins outright -- not also converted.
        assert_eq!(outcome.settings.budget_daily_rate, 5.0);
        // The balance (which has no explicit-override alternative) still
        // converts regardless.
        assert!((outcome.balance - 10.0 * 0.9359).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_patch_settings_currency_change_without_cached_rate_is_an_error() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        seed_budget_row(&pool, 10.0, chrono::Utc::now().timestamp(), "EUR", 2.0, 7).await;
        // No CHF rate stored.

        let err = patch_settings(
            &pool,
            SettingsPatch {
                budget_daily_rate: None,
                budget_rollover_cap_days: None,
                main_currency: Some("CHF".to_string()),
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            BudgetError::CurrencyConversionUnavailable { .. }
        ));

        // Nothing was persisted on failure.
        let settings = current_settings(&pool).await.unwrap().unwrap();
        assert_eq!(settings.main_currency, "EUR");
    }

    #[tokio::test]
    async fn test_patch_settings_not_seeded_is_an_error() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let err = patch_settings(&pool, SettingsPatch::default())
            .await
            .unwrap_err();
        assert!(matches!(err, BudgetError::NotSeeded));
    }

    #[tokio::test]
    async fn test_effective_main_currency_falls_back_when_not_seeded() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        assert_eq!(effective_main_currency(&pool, "EUR").await.unwrap(), "EUR");
    }

    #[tokio::test]
    async fn test_effective_main_currency_uses_the_live_value_once_seeded() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        seed_budget_row(&pool, 0.0, chrono::Utc::now().timestamp(), "CHF", 0.0, 7).await;
        // The fallback ("EUR") is ignored once a live settings row exists.
        assert_eq!(effective_main_currency(&pool, "EUR").await.unwrap(), "CHF");
    }
}
