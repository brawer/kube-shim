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
//! independent of any input content. The *spec repo's* rule model
//! already fixed this
//! (<https://github.com/FinOps-Open-Cost-and-Usage-Spec/FOCUS_Spec/pull/2609>,
//! merged 2026-09-08), but `focus-validator` is a separate PyPI
//! package that bundles its own vendored copy of the rule JSON --
//! its latest release (2.2.1, published 2026-08-05, before that fix)
//! still crashes identically on `--validate-version 1.4` (confirmed
//! hands-on, not assumed from the merged PR alone). So this is
//! validated against the older, working `v1.3.0.1` rule set instead
//! (tracked on our own side at
//! <https://github.com/brawer/kube-shim/issues/62>, to re-run a real
//! `v1.4` pass once a `focus-validator` release actually picks up the
//! fix); (2) the validator expects *every* column FOCUS v1.4 defines
//! for the Cost and Usage dataset to be present in the header --
//! genuinely null for whichever ones don't
//! apply, not a sparse subset -- confirmed by running the real tool
//! against real generated output, not assumed from a schema reading.

use crate::currency;
use crate::job_resources::{self, JobResource};
use crate::naming;
use crate::pricing::{self, ChargeBreakdown, ChargeKind};
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
/// commitment discounts, no contracts, no capacity
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
    cronjob_name: Option<String>,
    spec: JsonValue,
    created_at: i64,
    completed_at: i64,
    worker_plan: Option<String>,
    /// From `jobs`; `None` once the job is removed from history.
    worker_vm_id: Option<String>,
    worker_vm_name: Option<String>,
    /// The job's billable resources (issue #148); empty for a job
    /// provisioned before `job_resources` existed.
    resources: Vec<JobResource>,
}

/// The resource one charge is for, and the period it was billed:
/// FOCUS's `ResourceId`/`ResourceName`/`ResourceType` and
/// `ChargePeriodStart`/`ChargePeriodEnd`.
struct ChargedResource {
    id: String,
    name: String,
    resource_type: &'static str,
    period_start: i64,
    period_end: i64,
}

impl CompletedJob {
    fn resource(&self, kind: &str) -> Option<&JobResource> {
        self.resources.iter().find(|r| r.kind == kind)
    }

    /// The worker VM's provider ID, title and lifetime. For a job from
    /// before `job_resources` existed: the ID and title `jobs` still
    /// has (else the title the VM was created with, also as its ID),
    /// and the job's own lifetime, as before issue #148.
    fn server(&self, config: &CostReportConfig) -> (String, String, i64, i64) {
        if let Some(r) = self.resource(job_resources::SERVER) {
            return (
                r.provider_id.clone(),
                r.name.clone(),
                r.created_at,
                r.deleted_at.unwrap_or(r.created_at),
            );
        }
        let name = self.worker_vm_name.clone().unwrap_or_else(|| {
            naming::worker_title(&config.resource_prefix, &self.namespace, &self.name)
        });
        let id = self.worker_vm_id.clone().unwrap_or_else(|| name.clone());
        (id, name, self.created_at, self.completed_at)
    }

    fn charged_resource(&self, config: &CostReportConfig, kind: ChargeKind) -> ChargedResource {
        if kind == ChargeKind::Volume {
            let (id, name, start, end) = match self.resource(job_resources::VOLUME) {
                Some(r) => (
                    r.provider_id.clone(),
                    r.name.clone(),
                    r.created_at,
                    r.deleted_at.unwrap_or(r.created_at),
                ),
                None => {
                    let name =
                        naming::volume_title(&config.resource_prefix, &self.namespace, &self.name);
                    (name.clone(), name, self.created_at, self.completed_at)
                }
            };
            return ChargedResource {
                id,
                name,
                resource_type: "Block Storage",
                period_start: start,
                period_end: end,
            };
        }

        // The boot disk and IPv4 address live and die with the VM.
        let (server_id, server_name, start, end) = self.server(config);
        let server_row = self.resource(job_resources::SERVER);
        let (id, name, resource_type) = match kind {
            ChargeKind::BootDisk => (
                server_row
                    .and_then(|r| r.boot_disk_id.clone())
                    .unwrap_or_else(|| server_id.clone()),
                // The title `create_server` gives the boot disk.
                format!("{server_name}-os"),
                "Block Storage",
            ),
            ChargeKind::Ipv4 => match server_row.and_then(|r| r.public_ipv4.clone()) {
                Some(ip) => (ip.clone(), ip, "IP Address"),
                None => (server_id, format!("{server_name} IPv4"), "IP Address"),
            },
            _ => (server_id, server_name, "Virtual Machine"),
        };
        ChargedResource {
            id,
            name,
            resource_type,
            period_start: start,
            period_end: end,
        }
    }

    /// FOCUS `Tags`: a JSON object, so the report can be grouped per
    /// run or per CronJob. The CronJob is empty for a one-off Job, like
    /// `/metrics`' own `cronjob` label.
    fn tags(&self) -> String {
        serde_json::json!({
            "kube-shim/namespace": self.namespace,
            "kube-shim/job": self.name,
            "kube-shim/cronjob": self.cronjob_name.clone().unwrap_or_default(),
        })
        .to_string()
    }
}

/// The FOCUS service columns for one kind of charge:
/// `(ServiceCategory, ServiceSubcategory, ServiceName suffix, SkuMeter,
/// ChargeDescription suffix)`. Category and subcategory are values from
/// the spec's own "Allowed Values" tables.
fn service_columns(
    kind: ChargeKind,
) -> (
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
) {
    match kind {
        ChargeKind::Server => (
            "Compute",
            "Virtual Machines",
            "Server",
            "Compute Usage",
            "worker VM",
        ),
        ChargeKind::BootDisk => (
            "Storage",
            "Block Storage",
            "Storage",
            "Block Volume Usage",
            "worker boot disk",
        ),
        ChargeKind::Ipv4 => (
            "Networking",
            "Network Infrastructure",
            "Networking",
            "IP Address Usage",
            "worker IPv4 address",
        ),
        ChargeKind::Volume => (
            "Storage",
            "Block Storage",
            "Storage",
            "Block Volume Usage",
            "ephemeral volume",
        ),
    }
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

    let resource = job.charged_resource(config, charge.kind);
    let (service_category, service_subcategory, service_name, sku_meter, description) =
        service_columns(charge.kind);
    let (billing_period_start, billing_period_end) = billing_period_for(resource.period_start);
    let charge_description = format!("{}/{} {description}", job.namespace, job.name);
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
    fields.insert("ChargePeriodStart", rfc3339(resource.period_start));
    fields.insert("ChargePeriodEnd", rfc3339(resource.period_end));
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
    fields.insert("ResourceId", resource.id);
    fields.insert("ResourceName", resource.name);
    fields.insert("ResourceType", resource.resource_type.to_string());
    fields.insert("ServiceCategory", service_category.to_string());
    fields.insert(
        "ServiceName",
        format!("{} {service_name}", config.provider_name),
    );
    fields.insert("ServiceProviderName", config.provider_name.clone());
    fields.insert("ServiceSubcategory", service_subcategory.to_string());
    fields.insert("SkuId", charge.sku_id.clone());
    fields.insert("SkuMeter", sku_meter.to_string());
    fields.insert("SkuPriceId", charge.sku_price_id.clone());
    fields.insert("Tags", job.tags());

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

    // Every job that created a billable resource has an `actual_cost`
    // once it's archived (issue #148); a deleted resource of a job still
    // being cleaned up is already billed, so it's listed too.
    let rows = sqlx::query(
        "SELECT id, name, namespace, cronjob_name, spec, created_at, completed_at, worker_plan, \
                worker_vm_id, worker_vm_name FROM ( \
           SELECT id, name, namespace, cronjob_name, spec, created_at, completed_at, worker_plan, \
                  worker_vm_id, worker_vm_name, actual_cost \
           FROM jobs \
           UNION ALL \
           SELECT id, name, namespace, cronjob_name, spec, created_at, completed_at, worker_plan, \
                  NULL, NULL, actual_cost \
           FROM pruned_jobs) \
         WHERE (actual_cost IS NOT NULL \
                OR id IN (SELECT job_id FROM job_resources WHERE deleted_at IS NOT NULL)) \
           AND created_at >= ? AND created_at < ? \
         ORDER BY created_at",
    )
    .bind(from_ts)
    .bind(to_ts)
    .fetch_all(&pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let mut jobs: Vec<CompletedJob> = Vec::with_capacity(rows.len());
    for r in rows {
        let id: String = r.get(0);
        let spec_str: String = r.get(4);
        let resources = job_resources::for_job(&pool, &id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        jobs.push(CompletedJob {
            name: r.get(1),
            namespace: r.get(2),
            cronjob_name: r.get(3),
            spec: serde_json::from_str(&spec_str).unwrap_or(JsonValue::Null),
            created_at: r.get(5),
            completed_at: r.get::<Option<i64>, _>(6).unwrap_or_else(|| r.get(5)),
            worker_plan: r.get(7),
            worker_vm_id: r.get(8),
            worker_vm_name: r.get(9),
            resources,
        });
    }

    let mut csv = COLUMNS.join(",");
    csv.push('\n');

    for completed_job in &jobs {
        // Each resource is charged for as long as it existed. A job from
        // before `job_resources` existed is charged for its own lifetime.
        let (server_seconds, volume_seconds) = if completed_job.resources.is_empty() {
            let duration_seconds = completed_job.completed_at - completed_job.created_at;
            (Some(duration_seconds), Some(duration_seconds))
        } else {
            job_resources::lifetimes(&completed_job.resources)
        };
        let Some((charges, provider_currency)) = pricing::actual_job_charges(
            &pool,
            &config.zone,
            &completed_job.spec,
            completed_job.worker_plan.as_deref(),
            server_seconds,
            volume_seconds,
        )
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
        crate::workload::seed_test_plans(&pool).await;
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
        crate::workload::seed_test_plans(&pool).await;
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

    /// Splits one CSV line, honoring `csv_field`'s quoting.
    fn parse_csv_line(line: &str) -> Vec<String> {
        let mut fields = vec![String::new()];
        let mut quoted = false;
        let mut chars = line.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '"' if quoted && chars.peek() == Some(&'"') => {
                    chars.next();
                    fields.last_mut().unwrap().push('"');
                }
                '"' => quoted = !quoted,
                ',' if !quoted => fields.push(String::new()),
                _ => fields.last_mut().unwrap().push(c),
            }
        }
        fields
    }

    #[tokio::test]
    async fn test_force_failed_job_reports_every_resource_it_created() {
        // Issue #148: a job force-failed after its VM (on a plan that
        // doesn't bundle disk and IP) and its volume were created. Each
        // row names its resource and is charged for that resource's own
        // lifetime, not the job's.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        crate::workload::store_plans(
            &pool,
            &[crate::providers::PlanInfo {
                name: "CLOUDNATIVE-1xCPU-2GB".to_string(),
                cpu_cores: 1,
                memory_mb: 2048,
                storage_gb: 0,
                storage_tier: None,
                gpu_count: 0,
                current_offering: true,
            }],
            &Default::default(),
            0,
        )
        .await
        .unwrap();
        insert_price(&pool, "server_plan_CLOUDNATIVE-1xCPU-2GB", 1.0, 0.5).await;
        insert_price(&pool, "storage_standard", 1.0, 0.0118).await;
        insert_price(&pool, "ipv4_address", 1.0, 0.4812).await;
        let created = chrono::NaiveDate::from_ymd_opt(2026, 11, 7)
            .unwrap()
            .and_hms_opt(10, 55, 0)
            .unwrap()
            .and_utc()
            .timestamp();
        let spec = r#"{"template": {"spec": {
            "containers": [{"resources": {"requests": {"cpu": "1", "memory": "2Gi"}}}],
            "volumes": [{"ephemeral": {"volumeClaimTemplate": {"spec": {
                "resources": {"requests": {"storage": "10Gi"}},
                "storageClassName": "kube-shim-standard"
            }}}}]
        }}}"#;
        // Force-failed: no completed_at, no exit code.
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, cronjob_name, spec, status, created_at, \
             updated_at, worker_plan, actual_cost, version) \
             VALUES ('j1', 'dns-debug-1', 'default', 'dns-debug', ?, 'Archived', ?, ?, \
             'CLOUDNATIVE-1xCPU-2GB', 0.01, 1)",
        )
        .bind(spec)
        .bind(created)
        .bind(created + 3600)
        .execute(&pool)
        .await
        .unwrap();
        let worker = "kube-shim-test-worker-default-dns-debug-1";
        job_resources::record_created(
            &pool,
            "j1",
            job_resources::VOLUME,
            "vol-uuid",
            "v",
            created + 10,
        )
        .await
        .unwrap();
        job_resources::record_created(
            &pool,
            "j1",
            job_resources::SERVER,
            "srv-uuid",
            worker,
            created + 30,
        )
        .await
        .unwrap();
        job_resources::set_boot_disk_id(&pool, "j1", "disk-uuid")
            .await
            .unwrap();
        job_resources::set_public_ipv4(&pool, "j1", "192.0.2.7")
            .await
            .unwrap();
        job_resources::record_deleted(&pool, "j1", job_resources::VOLUME, created + 1810)
            .await
            .unwrap();
        job_resources::record_deleted(&pool, "j1", job_resources::SERVER, created + 3630)
            .await
            .unwrap();

        let response = get_report(pool, "?from=2026-11-06&to=2026-11-09").await;
        let body = body_text(response).await;
        let rows: Vec<Vec<String>> = body.lines().skip(1).map(parse_csv_line).collect();
        assert_eq!(rows.len(), 4); // VM, boot disk, IPv4, volume
        let col = |row: &Vec<String>, name: &str| {
            row[COLUMNS.iter().position(|c| *c == name).unwrap()].clone()
        };
        for row in &rows {
            assert_eq!(row.len(), COLUMNS.len());
            assert!(col(row, "BilledCost").parse::<f64>().unwrap() > 0.0);
            let tags: JsonValue = serde_json::from_str(&col(row, "Tags")).unwrap();
            assert_eq!(
                tags,
                serde_json::json!({
                    "kube-shim/namespace": "default",
                    "kube-shim/job": "dns-debug-1",
                    "kube-shim/cronjob": "dns-debug",
                })
            );
        }
        let resources: Vec<(String, String, String)> = rows
            .iter()
            .map(|r| {
                (
                    col(r, "ResourceId"),
                    col(r, "ResourceName"),
                    col(r, "ResourceType"),
                )
            })
            .collect();
        assert_eq!(
            resources,
            vec![
                ("srv-uuid".into(), worker.into(), "Virtual Machine".into()),
                (
                    "disk-uuid".into(),
                    format!("{worker}-os"),
                    "Block Storage".into()
                ),
                ("192.0.2.7".into(), "192.0.2.7".into(), "IP Address".into()),
                ("vol-uuid".into(), "v".into(), "Block Storage".into()),
            ]
        );
        // The VM's rows run from its creation to its deletion (1 hour),
        // the volume's from its own (half an hour).
        assert_eq!(col(&rows[0], "ChargePeriodStart"), rfc3339(created + 30));
        assert_eq!(col(&rows[0], "ChargePeriodEnd"), rfc3339(created + 3630));
        assert_eq!(col(&rows[0], "ConsumedQuantity"), "1.000000");
        assert_eq!(col(&rows[3], "ChargePeriodStart"), rfc3339(created + 10));
        assert_eq!(col(&rows[3], "ConsumedQuantity"), "0.500000");
        assert_eq!(col(&rows[2], "ServiceCategory"), "Networking");
        assert_eq!(col(&rows[1], "ServiceCategory"), "Storage");
    }

    #[tokio::test]
    async fn test_job_from_before_resources_were_recorded_still_names_its_vm() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        crate::workload::seed_test_plans(&pool).await;
        insert_price(&pool, "server_plan_DEV-1xCPU-1GB-10GB", 1.0, 0.4464).await;
        let created = chrono::NaiveDate::from_ymd_opt(2026, 9, 15)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp();
        let spec = r#"{"template": {"spec": {"containers": [{"resources": {"requests": {"cpu": "1", "memory": "1Gi"}}}]}}}"#;
        insert_completed_job(&pool, "job-one", spec, created, created + 3600, 0.004464).await;
        sqlx::query("UPDATE jobs SET worker_vm_id = 'srv-old', worker_vm_name = 'w-old'")
            .execute(&pool)
            .await
            .unwrap();

        let response = get_report(pool, "?from=2026-09-01&to=2026-09-30").await;
        let body = body_text(response).await;
        let rows: Vec<Vec<String>> = body.lines().skip(1).map(parse_csv_line).collect();
        assert_eq!(rows.len(), 1);
        let col = |name: &str| rows[0][COLUMNS.iter().position(|c| *c == name).unwrap()].clone();
        assert_eq!(col("ResourceId"), "srv-old");
        assert_eq!(col("ResourceName"), "w-old");
        assert_eq!(col("ResourceType"), "Virtual Machine");
        assert_eq!(col("ChargePeriodStart"), rfc3339(created));
        let tags: JsonValue = serde_json::from_str(&col("Tags")).unwrap();
        assert_eq!(tags["kube-shim/cronjob"], "");
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

    #[tokio::test]
    async fn test_removed_job_stays_in_the_report() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        crate::workload::seed_test_plans(&pool).await;
        insert_price(&pool, "server_plan_DEV-1xCPU-1GB-10GB", 1.0, 0.4464).await;
        let created = chrono::NaiveDate::from_ymd_opt(2026, 9, 15)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp();
        let spec = r#"{"template": {"spec": {"containers": [{"resources": {"requests": {"cpu": "1", "memory": "1Gi"}}}]}}}"#;
        insert_completed_job(&pool, "job-one", spec, created, created + 3600, 0.004464).await;
        let id: String = sqlx::query_scalar("SELECT id FROM jobs")
            .fetch_one(&pool)
            .await
            .unwrap();
        crate::reconcile::history::prune_job(&pool, &id, created + 7200)
            .await
            .unwrap();

        let response = get_report(pool, "?from=2026-09-01&to=2026-09-30").await;
        let body = body_text(response).await;
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2); // header + 1 charge row
        let fields: Vec<&str> = lines[1].split(',').collect();
        let col = |name: &str| fields[COLUMNS.iter().position(|c| *c == name).unwrap()];
        assert_eq!(col("BilledCost"), "0.004464");
    }
}
