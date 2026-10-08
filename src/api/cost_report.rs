//! Real FOCUS v1.4-subset CSV cost report (Phase 14a) -- `GET
//! /apis/cost.kube-shim.brawer.ch/v1/report?from=...&to=...`. One row
//! per real *charge* (`reconcile::pricing::ChargeBreakdown`), not one
//! row per job -- a job with an ephemeral volume produces two charges
//! (its worker VM, and its volume), each with its own real SKU/
//! quantity/unit-price detail.
//!
//! **Why per-charge, not per-job:** found by testing the actual
//! `focus-validator` tool directly, not by reading column *names*
//! alone -- for any non-`"Correction"` `"Usage"`/`"Purchase"` charge,
//! FOCUS actually requires `SkuId`/`SkuPriceId`/`PricingCategory`/
//! `PricingQuantity`/`ListUnitPrice`/`ContractedUnitPrice`/
//! `ConsumedQuantity`/`ConsumedUnit`/`PricingCurrencyListUnitPrice` to
//! all be non-null. A job's VM and volume have genuinely different
//! values for every one of those, so blending them into a single row
//! (an earlier version of this module did exactly that) can't be made
//! to satisfy this cluster of requirements at all -- FOCUS models one
//! row per charge because real cloud bills do too.
//!
//! See docs/IMPLEMENTATION_PLAN.md Phase 14a for the full research
//! writeup, including the other two real findings this module's own
//! shape follows: (1) `focus-validator`'s own v1.4 rule set currently
//! fails to even load -- a circular dependency between
//! `CommitmentDiscountQuantity`/`CommitmentDiscountUnit` rules,
//! independent of any input content, already filed upstream at
//! <https://github.com/FinOps-Open-Cost-and-Usage-Spec/FOCUS_Spec/pull/2609>
//! -- so this is validated against the older, working `v1.3.0.1` rule
//! set instead (tracked on our own side at
//! <https://github.com/brawer/kube-shim/issues/62>, to re-run a real
//! `v1.4` pass once the upstream fix ships); (2) the validator expects
//! *every* column FOCUS v1.4 defines for the Cost and Usage dataset to
//! be present in the header -- genuinely null for whichever ones don't
//! apply, not a sparse subset -- confirmed by running the real tool
//! against real generated output, not assumed from a schema reading.

use crate::currency;
use crate::pricing::{self, ChargeBreakdown};
use axum::{
    extract::{Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Extension,
};
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use serde_json::Value as JsonValue;
use sqlx::{Row, SqlitePool};
use std::collections::HashMap;
use std::sync::Arc;

/// Threaded in via `Extension`, the same pattern `api::logs`'s
/// `worker_ssh` already uses -- `zone`/`main_currency` are config-level,
/// not per-request state, so `axum::State` (shared with every other
/// handler's `SqlitePool`) isn't the right fit.
pub struct CostReportConfig {
    pub resource_prefix: String,
    pub zone: String,
    pub main_currency: String,
    /// From `CloudProvider::provider_name()` -- `HostProviderName`/
    /// `ServiceProviderName` come from the actual provider
    /// implementation rather than a hardcoded `"UpCloud"` literal
    /// here, so a future second provider doesn't require editing this
    /// module at all.
    pub provider_name: String,
    /// From `CloudProvider::invoice_issuer_name()` -- see that
    /// method's own docs for why it's kept separate from
    /// `provider_name`.
    pub invoice_issuer_name: String,
}

/// Every column FOCUS v1.4 defines for the Cost and Usage dataset, in
/// the real spec's own Column ID order -- not a sparse subset. Real
/// finding, verified hands-on against `focus-validator` directly: the
/// tool expects the *entire* defined column set to be present in the
/// header, genuinely null for whichever ones don't apply, not just the
/// columns a given producer happens to populate -- matching how real
/// provider FOCUS exports (AWS CUR, GCP billing export) already
/// behave. See this module's own top-level docs for the other two
/// real findings this shape follows (per-charge rows, and the
/// `v1.4`-rule-loading bug upstream).
///
/// Columns kube-shim genuinely has no data for (no sub-accounts, no
/// resource tags, no commitment discounts, no contracts, no capacity
/// reservations, no multi-currency-aware SKU metering) are always
/// null -- `charge_row` below marks each one explicitly, rather than
/// leaving a silent gap between this list and what's actually
/// written.
const COLUMNS: &[&str] = &[
    "AllocatedMethodDetails",
    "AllocatedMethodId",
    "AllocatedResourceId",
    "AllocatedResourceName",
    "AllocatedTags",
    "AvailabilityZone",
    "BilledCost",
    "BillingAccountId",
    "BillingAccountName",
    "BillingAccountType",
    "BillingCurrency",
    "BillingPeriodEnd",
    "BillingPeriodStart",
    "CapacityReservationId",
    "CapacityReservationStatus",
    "ChargeCategory",
    "ChargeClass",
    "ChargeDescription",
    "ChargeFrequency",
    "ChargePeriodEnd",
    "ChargePeriodStart",
    "CommitmentDiscountCategory",
    "CommitmentDiscountId",
    "CommitmentDiscountName",
    "CommitmentDiscountQuantity",
    "CommitmentDiscountStatus",
    "CommitmentDiscountType",
    "CommitmentDiscountUnit",
    "CommitmentProgramEligibilityDetails",
    "ConsumedQuantity",
    "ConsumedUnit",
    "ContractApplied",
    "ContractedCost",
    "ContractedUnitPrice",
    "EffectiveCost",
    "HostProviderName",
    "InvoiceDetailId",
    "InvoiceId",
    "InvoiceIssuerName",
    "ListCost",
    "ListUnitPrice",
    "PricingCategory",
    "PricingCurrency",
    "PricingCurrencyContractedUnitPrice",
    "PricingCurrencyEffectiveCost",
    "PricingCurrencyListUnitPrice",
    "PricingQuantity",
    "PricingUnit",
    "RegionId",
    "RegionName",
    "ResourceId",
    "ResourceName",
    "ResourceType",
    "ServiceCategory",
    "ServiceName",
    "ServiceProviderName",
    "ServiceSubcategory",
    "SkuId",
    "SkuMeter",
    "SkuPriceDetails",
    "SkuPriceId",
    "SubAccountId",
    "SubAccountName",
    "SubAccountType",
    "Tags",
];

struct CompletedJob {
    name: String,
    namespace: String,
    spec: JsonValue,
    created_at: i64,
    completed_at: i64,
}

fn parse_date_param(
    params: &HashMap<String, String>,
    key: &str,
) -> Result<NaiveDate, (StatusCode, String)> {
    let raw = params.get(key).ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            format!("missing required query parameter {key:?}"),
        )
    })?;
    NaiveDate::parse_from_str(raw, "%Y-%m-%d").map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            format!("{key} must be an ISO date (YYYY-MM-DD), got {raw:?}"),
        )
    })
}

/// RFC 3339, matching every other timestamp this project's own APIs
/// already emit (`api::metrics`/`api::events`).
fn rfc3339(unix_seconds: i64) -> String {
    DateTime::from_timestamp(unix_seconds, 0)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_default()
}

/// The calendar month containing `unix_seconds`, as `(start, end)` --
/// `BillingPeriodStart`/`End` are the invoice period, not the
/// per-charge period (`ChargePeriodStart`/`End`): kube-shim doesn't
/// track UpCloud's own real invoice boundaries, so the containing
/// calendar month is the closest honest approximation.
fn billing_period_for(unix_seconds: i64) -> (String, String) {
    let dt = DateTime::from_timestamp(unix_seconds, 0).unwrap_or_else(Utc::now);
    let start = Utc
        .with_ymd_and_hms(dt.year(), dt.month(), 1, 0, 0, 0)
        .single()
        .unwrap_or(dt);
    let (next_year, next_month) = if dt.month() == 12 {
        (dt.year() + 1, 1)
    } else {
        (dt.year(), dt.month() + 1)
    };
    let end = Utc
        .with_ymd_and_hms(next_year, next_month, 1, 0, 0, 0)
        .single()
        .unwrap_or(start);
    (start.to_rfc3339(), end.to_rfc3339())
}

/// Quotes a field only if it actually needs it (contains a comma,
/// quote, or newline) -- real provider FOCUS exports leave plain
/// numbers/identifiers unquoted, and unnecessary quoting on a Decimal
/// column risks a stricter CSV reader inferring it as a string column
/// instead of numeric.
fn csv_field(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

/// 6 decimal places, not UpCloud's own 4 -- a single hour of this
/// project's cheapest real plan already costs a provider-currency
/// amount like `0.004464` (0.4464 cents/hour), which 4 decimal places
/// would round to `0.0045`, a ~0.8% error that then compounds across
/// every aggregation. 6 places keeps that real precision while still
/// avoiding Rust's default float formatting's long, non-currency-like
/// tails for an inexact value.
fn format_decimal(value: f64) -> String {
    format!("{value:.6}")
}

/// One CSV row's worth of fields, in `COLUMNS`' own order -- a job
/// produces one of these per real charge (`ChargeBreakdown`).
#[allow(clippy::too_many_arguments)]
/// Builds one CSV row's fields, keyed by column name rather than a
/// fixed position -- with 65 real columns, a manually-aligned
/// positional array (an earlier version of this function used one) is
/// exactly the kind of thing that silently drifts out of sync with
/// `COLUMNS` after the next edit. Every column not explicitly set here
/// defaults to empty (genuinely null -- kube-shim has no sub-accounts,
/// resource tags, commitment discounts, contracts, capacity
/// reservations, or multi-currency-aware SKU metering), so adding a
/// new real value later only ever means adding one more `insert` call,
/// never re-counting positions.
async fn charge_row(
    pool: &SqlitePool,
    config: &CostReportConfig,
    job: &CompletedJob,
    charge: &ChargeBreakdown,
    provider_currency: &str,
) -> anyhow::Result<Option<Vec<String>>> {
    let Some(pricing_currency_unit_price) = currency::convert(
        pool,
        charge.unit_price,
        provider_currency,
        &config.main_currency,
    )
    .await?
    else {
        return Ok(None);
    };
    let Some(pricing_currency_total) =
        currency::convert(pool, charge.total, provider_currency, &config.main_currency).await?
    else {
        return Ok(None);
    };

    let is_volume = charge.sku_price_id.starts_with("storage_");
    let (billing_period_start, billing_period_end) = billing_period_for(job.created_at);
    let charge_description = format!(
        "{}/{} {}",
        job.namespace,
        job.name,
        if is_volume {
            "ephemeral volume"
        } else {
            "worker VM"
        }
    );
    let billed_cost = format_decimal(charge.total);
    let unit_price = format_decimal(charge.unit_price);
    let quantity = format_decimal(charge.quantity);

    let mut fields: HashMap<&str, String> = HashMap::new();
    fields.insert("BilledCost", billed_cost.clone());
    fields.insert("BillingAccountId", config.resource_prefix.clone());
    fields.insert("BillingAccountName", config.resource_prefix.clone());
    fields.insert("BillingAccountType", "Standard".to_string());
    fields.insert("BillingCurrency", provider_currency.to_string());
    fields.insert("BillingPeriodStart", billing_period_start);
    fields.insert("BillingPeriodEnd", billing_period_end);
    fields.insert("ChargeCategory", "Usage".to_string());
    // ChargeClass stays unset (null): kube-shim never issues corrections.
    // Every charge is metered hourly for the duration the resource actually
    // ran, i.e. it only recurs while the job is using the resource.
    fields.insert("ChargeFrequency", "Usage-Based".to_string());
    fields.insert("ChargeDescription", charge_description);
    fields.insert("ChargePeriodStart", rfc3339(job.created_at));
    fields.insert("ChargePeriodEnd", rfc3339(job.completed_at));
    fields.insert("ConsumedQuantity", quantity.clone());
    fields.insert("ConsumedUnit", charge.unit.to_string());
    fields.insert("ContractedCost", billed_cost.clone());
    fields.insert("ContractedUnitPrice", unit_price.clone());
    fields.insert("EffectiveCost", billed_cost.clone());
    fields.insert("HostProviderName", config.provider_name.clone());
    fields.insert("InvoiceIssuerName", config.invoice_issuer_name.clone());
    fields.insert("ListCost", billed_cost.clone());
    fields.insert("ListUnitPrice", unit_price.clone());
    fields.insert("PricingCategory", charge.pricing_category.to_string());
    fields.insert("PricingCurrency", config.main_currency.clone());
    fields.insert(
        "PricingCurrencyContractedUnitPrice",
        format_decimal(pricing_currency_unit_price),
    );
    fields.insert(
        "PricingCurrencyEffectiveCost",
        format_decimal(pricing_currency_total),
    );
    fields.insert(
        "PricingCurrencyListUnitPrice",
        format_decimal(pricing_currency_unit_price),
    );
    fields.insert("PricingQuantity", quantity);
    fields.insert("PricingUnit", charge.unit.to_string());
    // Real FOCUS enum values (verified against the spec's own "Allowed
    // Values" table for both ServiceCategory and ServiceSubcategory) --
    // a volume charge is genuinely "Storage"/"Block Storage", not
    // "Compute"/"Virtual Machines" like its job's VM charge.
    fields.insert(
        "ServiceCategory",
        if is_volume { "Storage" } else { "Compute" }.to_string(),
    );
    fields.insert(
        "ServiceName",
        format!(
            "{} {}",
            config.provider_name,
            if is_volume { "Storage" } else { "Server" }
        ),
    );
    fields.insert("ServiceProviderName", config.provider_name.clone());
    fields.insert(
        "ServiceSubcategory",
        if is_volume {
            "Block Storage"
        } else {
            "Virtual Machines"
        }
        .to_string(),
    );
    fields.insert("SkuId", charge.sku_id.clone());
    fields.insert(
        "SkuMeter",
        if is_volume {
            "Block Volume Usage".to_string()
        } else {
            "Compute Usage".to_string()
        },
    );
    fields.insert("SkuPriceId", charge.sku_price_id.clone());

    Ok(Some(
        COLUMNS
            .iter()
            .map(|col| fields.get(col).cloned().unwrap_or_default())
            .collect(),
    ))
}

pub async fn get_cost_report(
    State(pool): State<SqlitePool>,
    Extension(config): Extension<Arc<CostReportConfig>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response, (StatusCode, String)> {
    let from = parse_date_param(&params, "from")?;
    let to = parse_date_param(&params, "to")?;

    let from_ts = from.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp();
    // Inclusive of the whole "to" day.
    let to_ts = (to + chrono::Duration::days(1))
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp();

    let rows = sqlx::query(
        "SELECT name, namespace, spec, created_at, completed_at FROM jobs \
         WHERE actual_cost IS NOT NULL AND created_at >= ? AND created_at < ? \
         ORDER BY created_at",
    )
    .bind(from_ts)
    .bind(to_ts)
    .fetch_all(&pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let jobs: Vec<CompletedJob> = rows
        .into_iter()
        .map(|r| {
            let spec_str: String = r.get(2);
            CompletedJob {
                name: r.get(0),
                namespace: r.get(1),
                spec: serde_json::from_str(&spec_str).unwrap_or(JsonValue::Null),
                created_at: r.get(3),
                completed_at: r.get::<Option<i64>, _>(4).unwrap_or_else(|| r.get(3)),
            }
        })
        .collect();

    let mut csv = COLUMNS.join(",");
    csv.push('\n');

    for completed_job in &jobs {
        let duration_seconds = completed_job.completed_at - completed_job.created_at;
        let Some((charges, provider_currency)) =
            pricing::actual_job_charges(&pool, &config.zone, &completed_job.spec, duration_seconds)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        else {
            continue;
        };

        for charge in &charges {
            let Some(fields) =
                charge_row(&pool, &config, completed_job, charge, &provider_currency)
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            else {
                continue;
            };
            csv.push_str(
                &fields
                    .iter()
                    .map(|f| csv_field(f))
                    .collect::<Vec<_>>()
                    .join(","),
            );
            csv.push('\n');
        }
    }

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/csv; charset=utf-8")],
        csv,
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Router};
    use tower::ServiceExt;

    fn router(pool: SqlitePool) -> Router {
        Router::new()
            .route("/report", get(get_cost_report))
            .layer(Extension(Arc::new(CostReportConfig {
                resource_prefix: "kube-shim-test".to_string(),
                zone: "de-fra1".to_string(),
                main_currency: "EUR".to_string(),
                provider_name: "UpCloud".to_string(),
                invoice_issuer_name: "UpCloud Ltd".to_string(),
            })))
            .with_state(pool)
    }

    async fn insert_price(pool: &SqlitePool, key: &str, amount: f64, price: f64) {
        sqlx::query(
            "INSERT INTO provider_pricing (zone, price_key, amount, price, currency, fetched_at) \
             VALUES ('de-fra1', ?, ?, ?, 'EUR', 0)",
        )
        .bind(key)
        .bind(amount)
        .bind(price)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_completed_job(
        pool: &SqlitePool,
        name: &str,
        spec: &str,
        created_at: i64,
        completed_at: i64,
        actual_cost: f64,
    ) {
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, completed_at, \
             updated_at, actual_cost, version) \
             VALUES (?, ?, 'default', ?, 'Archived', ?, ?, ?, ?, 1)",
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(name)
        .bind(spec)
        .bind(created_at)
        .bind(completed_at)
        .bind(completed_at)
        .bind(actual_cost)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn get_report(pool: SqlitePool, query: &str) -> axum::response::Response {
        router(pool)
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/report{query}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    async fn body_text(response: axum::response::Response) -> String {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(body.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn test_missing_query_params_is_400() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let response = get_report(pool, "").await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_malformed_date_is_400() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let response = get_report(pool, "?from=not-a-date&to=2026-09-30").await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_no_completed_jobs_is_header_only() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let response = get_report(pool, "?from=2026-09-01&to=2026-09-30").await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0], COLUMNS.join(","));
    }

    #[tokio::test]
    async fn test_vm_only_job_produces_one_real_focus_row() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_price(&pool, "server_plan_DEV-1xCPU-1GB-10GB", 1.0, 0.4464).await;
        let created = chrono::NaiveDate::from_ymd_opt(2026, 9, 15)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp();
        let completed = created + 3600; // 1 hour
        let spec = r#"{"template": {"spec": {"containers": [{"resources": {"requests": {"cpu": "1", "memory": "1Gi"}}}]}}}"#;
        insert_completed_job(&pool, "job-one", spec, created, completed, 0.004464).await;

        let response = get_report(pool, "?from=2026-09-01&to=2026-09-30").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/csv; charset=utf-8"
        );
        let body = body_text(response).await;
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2); // header + 1 charge row

        let fields: Vec<&str> = lines[1].split(',').collect();
        let col = |name: &str| fields[COLUMNS.iter().position(|c| *c == name).unwrap()];
        assert_eq!(col("BilledCost"), "0.004464");
        assert_eq!(col("BillingCurrency"), "EUR");
        assert_eq!(col("ChargeCategory"), "Usage");
        assert_eq!(col("ChargeClass"), "");
        assert_eq!(col("SkuId"), "DEV-1xCPU-1GB-10GB");
        assert_eq!(col("SkuPriceId"), "server_plan_DEV-1xCPU-1GB-10GB");
        assert_eq!(col("ConsumedQuantity"), "1.000000");
        assert_eq!(col("ConsumedUnit"), "Hours");
        assert_eq!(col("PricingCategory"), "Standard");
        assert_eq!(col("PricingCurrency"), "EUR");
        assert_eq!(col("PricingCurrencyEffectiveCost"), "0.004464");
        // Sourced from CostReportConfig (ultimately CloudProvider::
        // provider_name()/invoice_issuer_name()), not a literal in this
        // module.
        assert_eq!(col("HostProviderName"), "UpCloud");
        assert_eq!(col("ServiceProviderName"), "UpCloud");
        assert_eq!(col("InvoiceIssuerName"), "UpCloud Ltd");
        assert_eq!(col("ServiceCategory"), "Compute");
        assert_eq!(col("ServiceSubcategory"), "Virtual Machines");
    }

    #[tokio::test]
    async fn test_job_with_volume_produces_two_charge_rows() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_price(&pool, "server_plan_DEV-1xCPU-1GB-10GB", 1.0, 0.4464).await;
        insert_price(&pool, "storage_standard", 1.0, 0.0118).await;
        let created = chrono::NaiveDate::from_ymd_opt(2026, 9, 15)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp();
        let completed = created + 3600;
        let spec = r#"{"template": {"spec": {
            "containers": [{"resources": {"requests": {"cpu": "1", "memory": "1Gi"}}}],
            "volumes": [{"ephemeral": {"volumeClaimTemplate": {"spec": {
                "resources": {"requests": {"storage": "10Gi"}},
                "storageClassName": "kube-shim-standard"
            }}}}]
        }}}"#;
        insert_completed_job(&pool, "job-one", spec, created, completed, 0.005644).await;

        let response = get_report(pool, "?from=2026-09-01&to=2026-09-30").await;
        let body = body_text(response).await;
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 3); // header + 2 charge rows (VM, volume)

        let sku_id_idx = COLUMNS.iter().position(|c| *c == "SkuId").unwrap();
        let sku_ids: Vec<&str> = lines[1..]
            .iter()
            .map(|line| line.split(',').nth(sku_id_idx).unwrap())
            .collect();
        assert_eq!(sku_ids, vec!["DEV-1xCPU-1GB-10GB", "standard-10GB"]);

        // The volume charge is genuinely "Storage"/"Block Storage", not
        // "Compute"/"Virtual Machines" like its job's VM charge.
        let category_idx = COLUMNS
            .iter()
            .position(|c| *c == "ServiceCategory")
            .unwrap();
        let subcategory_idx = COLUMNS
            .iter()
            .position(|c| *c == "ServiceSubcategory")
            .unwrap();
        let categories: Vec<&str> = lines[1..]
            .iter()
            .map(|line| line.split(',').nth(category_idx).unwrap())
            .collect();
        let subcategories: Vec<&str> = lines[1..]
            .iter()
            .map(|line| line.split(',').nth(subcategory_idx).unwrap())
            .collect();
        assert_eq!(categories, vec!["Compute", "Storage"]);
        assert_eq!(subcategories, vec!["Virtual Machines", "Block Storage"]);
    }

    #[tokio::test]
    async fn test_job_outside_date_range_is_excluded() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_price(&pool, "server_plan_DEV-1xCPU-1GB-10GB", 1.0, 0.4464).await;
        let october = chrono::NaiveDate::from_ymd_opt(2026, 10, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp();
        let spec = r#"{"template": {"spec": {"containers": [{"resources": {"requests": {"cpu": "1", "memory": "1Gi"}}}]}}}"#;
        insert_completed_job(&pool, "job-one", spec, october, october + 3600, 0.004464).await;

        // Report asked for September -- the October job must not appear.
        let response = get_report(pool, "?from=2026-09-01&to=2026-09-30").await;
        let body = body_text(response).await;
        assert_eq!(body.lines().count(), 1); // header only
    }
}
