//! ECB daily reference exchange rates (Phase 14a) -- EUR-anchored
//! (every entry is "1 EUR = X <currency>"), used to convert the cloud
//! provider's own real billing currency (never assumed to be `EUR` --
//! see `providers::PriceEntry`'s own docs) to the operator's
//! `main_currency`.
//!
//! The feed only ever gives EUR->X rates, never a direct X->Y rate
//! between two non-EUR currencies -- so converting, say, `USD` to
//! `CHF` has to pivot through EUR explicitly (`amount ÷ rate[USD] ×
//! rate[CHF]`), not look up a `USD`->`CHF` rate that doesn't exist in
//! this feed at all. `convert` below does exactly that pivot, and
//! degrades to a plain pass-through whenever either side of the
//! conversion already *is* EUR.

use anyhow::Result;
use serde::Deserialize;
use sqlx::{Row, SqlitePool};

const ECB_DAILY_RATES_URL: &str = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml";

/// Real shape, verified hands-on against the live feed -- nested `Cube`
/// elements (an outer wrapper, then one dated `Cube`, then one `Cube`
/// per currency), not a flat list; the root element's own
/// `gesmes:Envelope` tag keeps its namespace prefix literally, since
/// `quick-xml` matches raw tag text rather than resolving XML
/// namespaces.
#[derive(Debug, Deserialize)]
#[serde(rename = "gesmes:Envelope")]
struct Envelope {
    #[serde(rename = "Cube")]
    outer: OuterCube,
}

#[derive(Debug, Deserialize)]
struct OuterCube {
    #[serde(rename = "Cube")]
    dated: DatedCube,
}

#[derive(Debug, Deserialize)]
struct DatedCube {
    #[serde(rename = "Cube", default)]
    rates: Vec<RateCube>,
}

#[derive(Debug, Deserialize)]
struct RateCube {
    #[serde(rename = "@currency")]
    currency: String,
    #[serde(rename = "@rate")]
    rate: f64,
}

/// One currency's EUR-anchored rate, as actually stored/looked up --
/// `("USD", 1.1269)` means "1 EUR = 1.1269 USD".
pub struct ExchangeRate {
    pub currency: String,
    pub rate: f64,
}

/// Fetches and parses the ECB's real daily reference-rate feed.
pub async fn fetch_ecb_rates() -> Result<Vec<ExchangeRate>> {
    let xml = reqwest::get(ECB_DAILY_RATES_URL).await?.text().await?;
    parse_ecb_rates(&xml)
}

fn parse_ecb_rates(xml: &str) -> Result<Vec<ExchangeRate>> {
    let envelope: Envelope = quick_xml::de::from_str(xml)?;
    Ok(envelope
        .outer
        .dated
        .rates
        .into_iter()
        .map(|c| ExchangeRate {
            currency: c.currency,
            rate: c.rate,
        })
        .collect())
}

/// Replaces the whole cached rate table -- the ECB publishes one
/// complete daily snapshot, not incremental updates, so a stale
/// currency that drops out of a future day's feed (none have so far,
/// but nothing guarantees it never will) shouldn't linger forever
/// either.
pub async fn store_rates(pool: &SqlitePool, rates: &[ExchangeRate], fetched_at: i64) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM exchange_rates")
        .execute(&mut *tx)
        .await?;
    for rate in rates {
        sqlx::query("INSERT INTO exchange_rates (currency, rate, fetched_at) VALUES (?, ?, ?)")
            .bind(&rate.currency)
            .bind(rate.rate)
            .bind(fetched_at)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn cached_rate(pool: &SqlitePool, currency: &str) -> Result<Option<f64>> {
    let row = sqlx::query("SELECT rate FROM exchange_rates WHERE currency = ?")
        .bind(currency)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| r.get(0)))
}

/// Converts `amount` in `from_currency` to `to_currency`, pivoting
/// through EUR (the feed's own anchor) when neither side already is
/// EUR. `Ok(None)` means a needed rate isn't cached yet (the daily sync
/// hasn't run, or a genuinely unknown currency) -- callers treat this
/// the same as "pricing not available yet" rather than erroring, since
/// it's expected to self-resolve on the next sync.
pub async fn convert(
    pool: &SqlitePool,
    amount: f64,
    from_currency: &str,
    to_currency: &str,
) -> Result<Option<f64>> {
    if from_currency == to_currency {
        return Ok(Some(amount));
    }

    let eur_amount = if from_currency == "EUR" {
        amount
    } else {
        let Some(rate) = cached_rate(pool, from_currency).await? else {
            return Ok(None);
        };
        amount / rate
    };

    if to_currency == "EUR" {
        return Ok(Some(eur_amount));
    }

    let Some(rate) = cached_rate(pool, to_currency).await? else {
        return Ok(None);
    };
    Ok(Some(eur_amount * rate))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Trimmed but otherwise verbatim capture of the real ECB feed
    /// (`curl https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml`),
    /// not a hand-constructed fixture -- the exact nesting/namespace
    /// shape this module's structs need to actually match.
    const REAL_CAPTURED_FEED: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01" xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
	<gesmes:subject>Reference rates</gesmes:subject>
	<gesmes:Sender>
		<gesmes:name>European Central Bank</gesmes:name>
	</gesmes:Sender>
	<Cube>
		<Cube time='2026-10-06'>
			<Cube currency='USD' rate='1.1269'/>
			<Cube currency='JPY' rate='178.15'/>
			<Cube currency='CHF' rate='0.9359'/>
		</Cube>
	</Cube>
</gesmes:Envelope>"#;

    #[test]
    fn test_parse_real_captured_feed() {
        let rates = parse_ecb_rates(REAL_CAPTURED_FEED).unwrap();
        assert_eq!(rates.len(), 3);
        let usd = rates.iter().find(|r| r.currency == "USD").unwrap();
        assert_eq!(usd.rate, 1.1269);
        let chf = rates.iter().find(|r| r.currency == "CHF").unwrap();
        assert_eq!(chf.rate, 0.9359);
    }

    #[test]
    fn test_parse_garbage_is_an_error() {
        assert!(parse_ecb_rates("not xml at all").is_err());
    }

    async fn pool_with_rates(rates: &[(&str, f64)]) -> SqlitePool {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let entries: Vec<ExchangeRate> = rates
            .iter()
            .map(|(c, r)| ExchangeRate {
                currency: c.to_string(),
                rate: *r,
            })
            .collect();
        store_rates(&pool, &entries, 0).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn test_convert_same_currency_is_a_no_op() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let result = convert(&pool, 42.0, "EUR", "EUR").await.unwrap();
        assert_eq!(result, Some(42.0));
    }

    #[tokio::test]
    async fn test_convert_eur_to_other() {
        let pool = pool_with_rates(&[("CHF", 0.9359)]).await;
        let result = convert(&pool, 10.0, "EUR", "CHF").await.unwrap();
        assert_eq!(result, Some(9.359));
    }

    #[tokio::test]
    async fn test_convert_other_to_eur() {
        let pool = pool_with_rates(&[("USD", 1.1269)]).await;
        let result = convert(&pool, 11.269, "USD", "EUR").await.unwrap();
        assert!((result.unwrap() - 10.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_convert_pivots_through_eur_for_two_non_eur_currencies() {
        // The real case the ECB's own EUR-anchored feed forces: USD ->
        // CHF has no direct rate in the feed at all.
        let pool = pool_with_rates(&[("USD", 1.1269), ("CHF", 0.9359)]).await;
        let result = convert(&pool, 112.69, "USD", "CHF").await.unwrap().unwrap();
        // 112.69 USD -> 100 EUR -> 93.59 CHF
        assert!((result - 93.59).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_convert_missing_rate_is_none_not_an_error() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let result = convert(&pool, 10.0, "USD", "CHF").await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_store_rates_replaces_the_whole_table() {
        let pool = pool_with_rates(&[("USD", 1.1), ("JPY", 180.0)]).await;
        store_rates(
            &pool,
            &[ExchangeRate {
                currency: "CHF".to_string(),
                rate: 0.93,
            }],
            1,
        )
        .await
        .unwrap();

        assert_eq!(cached_rate(&pool, "USD").await.unwrap(), None);
        assert_eq!(cached_rate(&pool, "CHF").await.unwrap(), Some(0.93));
    }
}
