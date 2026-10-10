//! Rolling budget guard's HTTP surface (Phase 14b) -- `GET`/`PATCH
//! /apis/cost.kube-shim.brawer.ch/v1/settings` and `POST
//! /apis/cost.kube-shim.brawer.ch/v1/budget/topup`. All the real
//! accrual/currency-conversion logic lives in `pricing` (`BudgetError`,
//! `settle_accrual`, `apply_topup`, `patch_settings`); this module is
//! just the thin request/response + event-recording + wake-the-loop
//! layer around it, the same split `api::cost_report` already uses
//! between itself and `pricing`'s cost-calculation functions.
//!
//! **Deliberately additive top-up, not a "set balance to X" endpoint**
//! -- discussed explicitly before writing any of this: a "set
//! absolute" call would need the caller to `GET` the current balance
//! first to compute the right number, racing against
//! `pricing::settle_accrual`'s own accrual (which can change the
//! balance between that `GET` and the `PATCH`/`POST` that follows it)
//! -- an additive top-up needs no such read-first step at all, so
//! there's no race window to begin with.
//!
//! `amount` can be negative, to correct an over-generous previous
//! top-up -- the same race-free reasoning applies either way. The
//! result is clamped so the balance never goes below zero
//! (`pricing::apply_topup`'s own docs); when a correction overshoots
//! (e.g. `-1000` against a balance of `10`), the event recorded below
//! says so explicitly (what was requested vs. what actually landed),
//! and the response's `balance` is always the real post-clamp value
//! -- never a number that silently wasn't honored.

use crate::pricing::{self, BudgetError, SettingsPatch};
use crate::reconcile::job::record_event;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Debug, Serialize)]
pub struct SettingsResponse {
    pub budget_daily_rate: f64,
    pub budget_rollover_cap_days: u32,
    pub main_currency: String,
    pub balance: f64,
}

#[derive(Debug, Default, Deserialize)]
pub struct PatchSettingsRequest {
    pub budget_daily_rate: Option<f64>,
    pub budget_rollover_cap_days: Option<u32>,
    pub main_currency: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TopUpRequest {
    /// Positive to add funds; negative to correct an over-generous
    /// previous top-up (clamped so the balance never goes below
    /// zero -- see `pricing::apply_topup`'s own docs). Must be
    /// nonzero and finite.
    pub amount: f64,
}

#[derive(Debug, Serialize)]
pub struct TopUpResponse {
    pub balance: f64,
}

fn budget_error_response(err: BudgetError) -> (StatusCode, String) {
    match err {
        // Shouldn't happen in production (main.rs always seeds this
        // before the server starts accepting requests) -- a genuine
        // server-side misconfiguration if it ever does, not something
        // the caller's own request can fix.
        BudgetError::NotSeeded => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        // The caller's chosen currency can't be honored right now (no
        // cached ECB rate for it yet) -- a real, recoverable condition
        // worth a distinct status from "something broke".
        BudgetError::CurrencyConversionUnavailable { .. } => {
            (StatusCode::CONFLICT, err.to_string())
        }
        BudgetError::Internal(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// `GET /settings` -- always settles accrual first, so the balance
/// shown is correct as of right now, not as of whenever a job last
/// happened to check it.
pub async fn get_settings(
    State(pool): State<SqlitePool>,
) -> Result<Response, (StatusCode, String)> {
    let (balance, settings) = pricing::settle_accrual(&pool)
        .await
        .map_err(BudgetError::Internal)
        .map_err(budget_error_response)?
        .ok_or_else(|| budget_error_response(BudgetError::NotSeeded))?;
    Ok(Json(SettingsResponse {
        budget_daily_rate: settings.budget_daily_rate,
        budget_rollover_cap_days: settings.budget_rollover_cap_days,
        main_currency: settings.main_currency,
        balance,
    })
    .into_response())
}

/// `PATCH /settings` -- any subset of the three fields; omitted means
/// "leave it alone". See `pricing::patch_settings`'s own docs for the
/// currency-conversion behavior when `main_currency` changes. Wakes
/// the reconciliation loop immediately (any `BudgetWait` job should
/// see the new numbers right away, not wait for the fallback tick) and
/// records one `SettingsChanged` event summarizing what changed.
pub async fn patch_settings(
    State(pool): State<SqlitePool>,
    Extension(notify): Extension<Arc<Notify>>,
    Json(req): Json<PatchSettingsRequest>,
) -> Result<Response, (StatusCode, String)> {
    let outcome = pricing::patch_settings(
        &pool,
        SettingsPatch {
            budget_daily_rate: req.budget_daily_rate,
            budget_rollover_cap_days: req.budget_rollover_cap_days,
            main_currency: req.main_currency,
        },
    )
    .await
    .map_err(budget_error_response)?;

    let mut message = format!(
        "budget_daily_rate={}, budget_rollover_cap_days={}, main_currency={}",
        outcome.settings.budget_daily_rate,
        outcome.settings.budget_rollover_cap_days,
        outcome.settings.main_currency
    );
    if let Some((old_currency, old_balance, new_currency, new_balance)) =
        &outcome.currency_conversion
    {
        message = format!(
            "{message} (balance converted {old_balance:.4} {old_currency} -> \
             {new_balance:.4} {new_currency})"
        );
    }
    record_event(&pool, None, "SettingsChanged", &message, "Normal")
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    notify.notify_one();

    Ok(Json(SettingsResponse {
        budget_daily_rate: outcome.settings.budget_daily_rate,
        budget_rollover_cap_days: outcome.settings.budget_rollover_cap_days,
        main_currency: outcome.settings.main_currency,
        balance: outcome.balance,
    })
    .into_response())
}

/// `POST /budget/topup` -- adds `amount` (in `main_currency`) directly
/// to the balance; see this module's own top-level docs for why this
/// is additive, not a "set to X" call. Wakes the reconciliation loop
/// immediately and records a `BudgetToppedUp` event.
pub async fn topup(
    State(pool): State<SqlitePool>,
    Extension(notify): Extension<Arc<Notify>>,
    Json(req): Json<TopUpRequest>,
) -> Result<Response, (StatusCode, String)> {
    // Written as the condition to accept, not the one to reject: a
    // negated `>` comparison on a float is a real correctness trap
    // (NaN compares false either way around), not just a style nit.
    // Zero is rejected too -- a no-op top-up is almost certainly a
    // client bug, not a real request.
    let is_valid_amount = req.amount.is_finite() && req.amount != 0.0;
    if !is_valid_amount {
        return Err((
            StatusCode::BAD_REQUEST,
            "amount must be a nonzero finite number".to_string(),
        ));
    }

    let (balance, applied_delta) = pricing::apply_topup(&pool, req.amount)
        .await
        .map_err(budget_error_response)?;

    // `applied_delta` only ever differs from the requested `amount`
    // when clamping kicked in (a correction that overshot zero) --
    // the event must say so explicitly rather than silently reporting
    // a number that wasn't actually honored.
    let message = if (applied_delta - req.amount).abs() > 1e-9 {
        format!(
            "{:+.4} requested, clamped to {applied_delta:+.4} (balance cannot go below \
             zero), new balance {balance:.4}",
            req.amount
        )
    } else {
        format!("{:+.4}, new balance {balance:.4}", req.amount)
    };
    record_event(&pool, None, "BudgetToppedUp", &message, "Normal")
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    notify.notify_one();

    Ok(Json(TopUpResponse { balance }).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::Request,
        routing::{get, post},
        Router,
    };
    use tower::ServiceExt;

    fn router(pool: SqlitePool) -> Router {
        Router::new()
            .route("/settings", get(get_settings).patch(patch_settings))
            .route("/budget/topup", post(topup))
            .layer(Extension(Arc::new(Notify::new())))
            .with_state(pool)
    }

    async fn body_json(response: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn seed(pool: &SqlitePool, balance: f64, daily_rate: f64, cap_days: u32) {
        pricing::ensure_budget_seeded(
            pool,
            &pricing::BudgetSettings {
                main_currency: "EUR".to_string(),
                budget_daily_rate: daily_rate,
                budget_rollover_cap_days: cap_days,
            },
        )
        .await
        .unwrap();
        if balance != 0.0 {
            pricing::apply_topup(pool, balance).await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_get_settings_returns_the_live_balance_and_settings() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        seed(&pool, 10.0, 2.0, 7).await;

        let response = router(pool)
            .oneshot(
                Request::builder()
                    .uri("/settings")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["main_currency"], "EUR");
        assert_eq!(body["budget_daily_rate"], 2.0);
        assert_eq!(body["budget_rollover_cap_days"], 7);
        assert!((body["balance"].as_f64().unwrap() - 10.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_get_settings_not_seeded_is_500() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let response = router(pool)
            .oneshot(
                Request::builder()
                    .uri("/settings")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn test_patch_settings_updates_and_records_an_event() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        seed(&pool, 10.0, 2.0, 7).await;

        let response = router(pool.clone())
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/settings")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"budget_daily_rate": 3.0}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["budget_daily_rate"], 3.0);

        let (reason, message): (String, String) =
            sqlx::query_as("SELECT reason, message FROM events WHERE job_id IS NULL")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(reason, "SettingsChanged");
        assert!(message.contains("budget_daily_rate=3"));
    }

    #[tokio::test]
    async fn test_patch_settings_currency_conversion_unavailable_is_409() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        seed(&pool, 10.0, 2.0, 7).await;
        // No CHF rate cached.

        let response = router(pool)
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/settings")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"main_currency": "CHF"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_topup_adds_to_balance_and_records_an_event() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        seed(&pool, 5.0, 2.0, 7).await;

        let response = router(pool.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/budget/topup")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"amount": 20}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert!((body["balance"].as_f64().unwrap() - 25.0).abs() < 1e-6);

        let (reason, message): (String, String) =
            sqlx::query_as("SELECT reason, message FROM events WHERE job_id IS NULL")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(reason, "BudgetToppedUp");
        assert!(message.contains("+20.0000"));
        assert!(message.contains("25.0000"));
    }

    #[tokio::test]
    async fn test_topup_rejects_zero_amount() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        seed(&pool, 5.0, 2.0, 7).await;

        let response = router(pool)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/budget/topup")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"amount": 0}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_topup_negative_corrects_an_over_generous_previous_topup() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        seed(&pool, 25.0, 0.0, 7).await;

        let response = router(pool.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/budget/topup")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"amount": -15}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert!((body["balance"].as_f64().unwrap() - 10.0).abs() < 1e-6);

        let message: String = sqlx::query_scalar("SELECT message FROM events WHERE job_id IS NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(message.contains("-15.0000"));
        assert!(!message.contains("clamped"));
    }

    #[tokio::test]
    async fn test_topup_negative_clamps_at_zero_and_says_so_in_the_event() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        seed(&pool, 10.0, 0.0, 7).await;

        let response = router(pool.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/budget/topup")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"amount": -1000}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        // The response's balance is always the real, honest post-clamp
        // value -- never the unclamped number the caller asked for.
        assert_eq!(body["balance"].as_f64().unwrap(), 0.0);

        let message: String = sqlx::query_scalar("SELECT message FROM events WHERE job_id IS NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
        // States both what was requested and what actually landed.
        assert!(message.contains("-1000.0000 requested"));
        assert!(message.contains("clamped to -10.0000"));
    }

    #[tokio::test]
    async fn test_topup_not_seeded_is_500() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let response = router(pool)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/budget/topup")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"amount": 20}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
