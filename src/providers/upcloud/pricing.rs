//! Pricing lookup. `GET /1.3/price` returns a large catalog (every zone,
//! every plan/service) -- verified for real against the live API while
//! researching UpCloud (docs/IMPLEMENTATION_PLAN.md Phase 7); this module
//! only ever returns the one zone a caller actually asked for, not every
//! zone in the catalog. See `PriceEntry`'s own docs for why
//! the return type stays a raw passthrough rather than a typed cost
//! model -- that's Phase 14a's job, once currency conversion exists to
//! design it for.
//!
//! The response's top-level `prices.currency` field (verified hands-on
//! against the live account: `"EUR"`) is the account's own *real*
//! billing currency -- not a fixed UpCloud-wide constant, since UpCloud
//! bills other accounts in `USD`. Phase 14a threads this straight
//! through to `PriceEntry::currency` rather than assuming anything.

use super::UpCloudProvider;
use crate::providers::{PriceEntry, ProviderError};
use reqwest::Method;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize)]
struct PriceListEnvelope {
    prices: PriceList,
}

#[derive(Deserialize)]
struct PriceList {
    currency: String,
    zone: Vec<ZonePrices>,
}

#[derive(Deserialize)]
struct ZonePrices {
    name: String,
    #[serde(flatten)]
    entries: HashMap<String, RawPriceEntry>,
}

#[derive(Deserialize)]
struct RawPriceEntry {
    amount: f64,
    price: f64,
}

pub(super) async fn get_zone_prices(
    provider: &UpCloudProvider,
    zone: &str,
) -> Result<HashMap<String, PriceEntry>, ProviderError> {
    let response: PriceListEnvelope = provider
        .send_json(provider.request(Method::GET, "/price"))
        .await?;

    let currency = response.prices.currency;
    let zone_prices = response
        .prices
        .zone
        .into_iter()
        .find(|z| z.name == zone)
        .ok_or_else(|| ProviderError::NotFound(format!("zone {zone} not present in price list")))?;

    Ok(zone_prices
        .entries
        .into_iter()
        .map(|(key, entry)| {
            (
                key,
                PriceEntry {
                    amount: entry.amount,
                    price: entry.price,
                    currency: currency.clone(),
                },
            )
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::super::tests::mock_server;
    use crate::providers::CloudProvider;
    use axum::{routing::get, Json};
    use serde_json::json;

    fn sample_price_response() -> serde_json::Value {
        json!({
            "prices": {
                "currency": "EUR",
                "zone": [
                    {
                        "name": "de-fra1",
                        "server_plan_DEV-1xCPU-1GB-10GB": {"amount": 1, "price": 0.4464},
                        "ipv4_address": {"amount": 1, "price": 0.4812}
                    },
                    {
                        "name": "fi-hel1",
                        "server_plan_DEV-1xCPU-1GB-10GB": {"amount": 1, "price": 0.4464}
                    }
                ]
            }
        })
    }

    #[tokio::test]
    async fn test_get_zone_prices_finds_the_right_zone() {
        let app = axum::Router::new().route(
            "/1.3/price",
            get(move || async move { Json(sample_price_response()) }),
        );
        let provider = mock_server(app).await;

        let prices = provider.get_zone_prices("de-fra1").await.unwrap();
        assert_eq!(prices.len(), 2);
        let entry = &prices["server_plan_DEV-1xCPU-1GB-10GB"];

        assert_eq!(entry.amount, 1.0);
        assert_eq!(entry.price, 0.4464);
        assert_eq!(entry.currency, "EUR");
    }

    #[tokio::test]
    async fn test_get_zone_prices_captures_a_non_eur_currency() {
        // UpCloud bills some accounts in USD, not just EUR -- this must
        // come from the real response, not a hardcoded assumption.
        let app = axum::Router::new().route(
            "/1.3/price",
            get(move || async move {
                Json(json!({
                    "prices": {
                        "currency": "USD",
                        "zone": [{
                            "name": "us-chi1",
                            "server_plan_DEV-1xCPU-1GB-10GB": {"amount": 1, "price": 0.5}
                        }]
                    }
                }))
            }),
        );
        let provider = mock_server(app).await;

        let prices = provider.get_zone_prices("us-chi1").await.unwrap();
        assert_eq!(prices["server_plan_DEV-1xCPU-1GB-10GB"].currency, "USD");
    }

    #[tokio::test]
    async fn test_get_zone_prices_unknown_zone_is_not_found() {
        let app = axum::Router::new().route(
            "/1.3/price",
            get(move || async move { Json(sample_price_response()) }),
        );
        let provider = mock_server(app).await;

        let err = provider
            .get_zone_prices("us-nonexistent1")
            .await
            .unwrap_err();
        assert!(matches!(err, crate::providers::ProviderError::NotFound(_)));
    }
}
