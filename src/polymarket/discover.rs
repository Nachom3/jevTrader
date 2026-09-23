use std::collections::HashMap;

use anyhow::{Context, Result, anyhow};
use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, FixedOffset, NaiveDate, Timelike, Utc, Weekday,
};
use polymarket_client_sdk_v2::gamma::{Client as GammaClient, types::response::Market};
use reqwest::{StatusCode, Url};
use serde_json::Value;

use crate::{
    domain::{Asset, Horizon},
    market_spec::MarketSpec,
};

const EPOCH_HORIZONS: [(&str, i64); 4] =
    [("5m", 300), ("15m", 900), ("4h", 14_400), ("daily", 86_400)];
const PROBE_ASSETS: [(&str, &str); 2] = [("btc", "bitcoin"), ("eth", "ethereum")];

/// Finds open BTC/ETH up-down markets from a deterministic set of live slugs,
/// then keeps the longest open contract in each asset/horizon group.
pub async fn discover_live_specs(gamma: &GammaClient) -> Result<Vec<MarketSpec>> {
    let slugs = probe_slugs(Utc::now());
    let probed = slugs.len();
    let client = reqwest::Client::new();
    let mut skipped = SkipCounts::default();
    let mut market_hits = 0;
    let mut selected: HashMap<(Asset, Horizon), Candidate> = HashMap::new();

    for probe_slug in slugs {
        let Some(value) = fetch_market_value_by_slug(gamma, &client, &probe_slug).await? else {
            skipped.slug_miss += 1;
            continue;
        };
        market_hits += 1;

        let market: Market = match serde_json::from_value(value.clone()) {
            Ok(market) => market,
            Err(_) => {
                skipped.unreadable_market += 1;
                continue;
            }
        };
        let slug = market.slug.as_deref().unwrap_or_default();
        if !is_supported_up_down_slug(slug) {
            skipped.other_slug += 1;
            continue;
        }
        if market.active != Some(true) {
            skipped.inactive += 1;
            continue;
        }
        if market.closed != Some(false) {
            skipped.closed += 1;
            continue;
        }
        if market.accepting_orders != Some(true) {
            skipped.not_accepting_orders += 1;
            continue;
        }
        if market.enable_order_book != Some(true) {
            skipped.order_book_disabled += 1;
            continue;
        }

        let Some(target) = event_price_to_beat(&value) else {
            skipped.missing_target += 1;
            tracing::warn!(
                market = slug,
                "skipping live market with missing or invalid eventMetadata.priceToBeat"
            );
            continue;
        };
        let Some(asset) = Asset::from_slug(slug) else {
            skipped.unknown_asset += 1;
            continue;
        };
        if is_daily_up_down_slug(slug) {
            skipped.unsupported_daily_horizon += 1;
            tracing::warn!(
                market = slug,
                horizon = "daily",
                "skipping daily market because domain Horizon has no daily variant"
            );
            continue;
        }
        let Some(horizon) = horizon_from_slug(slug) else {
            skipped.unknown_horizon += 1;
            continue;
        };
        let Some(end_date) = market.end_date else {
            skipped.missing_end_date += 1;
            continue;
        };
        let end_ms = end_date.timestamp_millis();
        let Some(resolution_source) = market
            .resolution_source
            .filter(|source| !source.trim().is_empty())
        else {
            skipped.missing_resolution_source += 1;
            continue;
        };
        let Some(question) = market
            .question
            .filter(|question| !question.trim().is_empty())
        else {
            skipped.missing_question += 1;
            continue;
        };

        let spec = MarketSpec {
            slug: slug.to_owned(),
            question,
            resolution_source: resolution_source.clone(),
            resolution_rules: market.description.unwrap_or_default(),
            target,
            resolution_at_ms: end_ms,
            asset: Some(asset),
            horizon: Some(horizon),
            reference_source: Some(resolution_source),
            window_secs: Some(horizon.seconds()),
            start_ms: Some(end_ms.saturating_sub((horizon.seconds() * 1_000) as i64)),
        };
        selected
            .entry((asset, horizon))
            .and_modify(|current| {
                if end_ms > current.end_ms {
                    *current = Candidate {
                        spec: spec.clone(),
                        end_ms,
                    };
                }
            })
            .or_insert(Candidate { spec, end_ms });
    }

    tracing::info!(
        slugs_probed = probed,
        market_hits,
        skip_counts = %skipped.summary(),
        "completed live-market slug probing"
    );

    let mut candidates = selected.into_values().collect::<Vec<_>>();
    candidates.sort_by_key(|candidate| {
        let asset_rank = match candidate.spec.asset {
            Some(Asset::Btc) => 0,
            Some(Asset::Eth) => 1,
            None => 2,
        };
        let horizon_rank = match candidate.spec.horizon {
            Some(Horizon::M5) => 0,
            Some(Horizon::M15) => 1,
            Some(Horizon::H1) => 2,
            Some(Horizon::H4) => 3,
            Some(Horizon::Daily) => 4,
            None => 4,
        };
        (asset_rank, horizon_rank)
    });
    candidates.truncate(8);

    if candidates.is_empty() {
        return Err(anyhow!(
            "no live BTC/ETH up-down markets qualified (probed {probed} slugs, {market_hits} hits; skip counts: {})",
            skipped.summary()
        ));
    }

    Ok(candidates
        .into_iter()
        .map(|candidate| candidate.spec)
        .collect())
}

/// Fetches the raw response from the same `/markets/slug/{slug}` Gamma
/// endpoint used by the SDK helper. Raw JSON is needed for eventMetadata, which
/// is not represented by the SDK's typed `Market` response.
async fn fetch_market_value_by_slug(
    gamma: &GammaClient,
    client: &reqwest::Client,
    slug: &str,
) -> Result<Option<Value>> {
    let url: Url = gamma
        .host()
        .join(&format!("markets/slug/{slug}"))
        .context("building Gamma market-by-slug URL")?;
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("requesting Gamma market slug `{slug}`"))?;

    if matches!(
        response.status(),
        StatusCode::NOT_FOUND | StatusCode::NO_CONTENT
    ) {
        return Ok(None);
    }
    let response = response
        .error_for_status()
        .with_context(|| format!("Gamma market-by-slug request failed for `{slug}`"))?;
    let value: Value = response
        .json()
        .await
        .with_context(|| format!("decoding Gamma market slug `{slug}`"))?;
    if value.is_null()
        || value.as_array().is_some_and(Vec::is_empty)
        || value.as_object().is_some_and(serde_json::Map::is_empty)
    {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

fn probe_slugs(now: DateTime<Utc>) -> Vec<String> {
    let mut slugs = Vec::with_capacity(20);
    for (asset_slug, _) in PROBE_ASSETS {
        for (horizon_slug, horizon_secs) in EPOCH_HORIZONS {
            let boundary = now.timestamp().div_euclid(horizon_secs) * horizon_secs;
            for epoch in [boundary, boundary - horizon_secs] {
                slugs.push(format!("{asset_slug}-updown-{horizon_slug}-{epoch}"));
            }
        }
    }

    let next_hour_utc = DateTime::from_timestamp(now.timestamp() + 3_600, 0)
        .expect("next hourly probe timestamp is representable");
    for (_, named_asset) in PROBE_ASSETS {
        let current_hour = named_hour_slug(named_asset, &now);
        let next_hour = named_hour_slug(named_asset, &next_hour_utc);
        slugs.push(current_hour);
        if !slugs
            .last()
            .is_some_and(|current_hour| current_hour == &next_hour)
        {
            slugs.push(next_hour);
        }
    }
    slugs
}

fn named_hour_slug(asset: &str, utc: &DateTime<Utc>) -> String {
    const MONTHS: [&str; 12] = [
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    ];
    let eastern = utc.with_timezone(&new_york_offset(utc));
    let hour = match eastern.hour() % 12 {
        0 => 12,
        hour => hour,
    };
    let meridiem = if eastern.hour() < 12 { "am" } else { "pm" };
    format!(
        "{asset}-up-or-down-{}-{}-{}-{hour}{meridiem}-et",
        MONTHS[eastern.month0() as usize],
        eastern.day(),
        eastern.year()
    )
}

fn new_york_offset(utc: &DateTime<Utc>) -> FixedOffset {
    // chrono is already a dependency, but chrono-tz is not. Apply New York's
    // current US DST rule using its UTC transition instants: 07:00 on the
    // second Sunday in March through 06:00 on the first Sunday in November.
    let year = utc.year();
    let daylight_start = nth_weekday_of_month(year, 3, Weekday::Sun, 2)
        .and_hms_opt(7, 0, 0)
        .expect("valid daylight-saving start time")
        .and_utc();
    let daylight_end = nth_weekday_of_month(year, 11, Weekday::Sun, 1)
        .and_hms_opt(6, 0, 0)
        .expect("valid daylight-saving end time")
        .and_utc();
    let timestamp = utc.timestamp();
    let offset_hours =
        if timestamp >= daylight_start.timestamp() && timestamp < daylight_end.timestamp() {
            4
        } else {
            5
        };
    FixedOffset::west_opt(offset_hours * 60 * 60).expect("valid New York UTC offset")
}

fn nth_weekday_of_month(year: i32, month: u32, weekday: Weekday, nth: u32) -> NaiveDate {
    let first_day = NaiveDate::from_ymd_opt(year, month, 1).expect("valid month start");
    let days_until_weekday = (7 + weekday.num_days_from_sunday() as i64
        - first_day.weekday().num_days_from_sunday() as i64)
        % 7;
    first_day + ChronoDuration::days(days_until_weekday + 7 * i64::from(nth - 1))
}

#[derive(Default)]
struct SkipCounts {
    slug_miss: usize,
    unreadable_market: usize,
    other_slug: usize,
    inactive: usize,
    closed: usize,
    not_accepting_orders: usize,
    order_book_disabled: usize,
    unknown_asset: usize,
    unknown_horizon: usize,
    missing_end_date: usize,
    missing_target: usize,
    missing_resolution_source: usize,
    missing_question: usize,
    unsupported_daily_horizon: usize,
}

impl SkipCounts {
    fn summary(&self) -> String {
        format!(
            "slug_miss={}, unreadable_market={}, other_slug={}, inactive={}, closed={}, not_accepting_orders={}, order_book_disabled={}, unknown_asset={}, unknown_horizon={}, missing_end_date={}, missing_target={}, missing_resolution_source={}, missing_question={}, unsupported_daily_horizon={}",
            self.slug_miss,
            self.unreadable_market,
            self.other_slug,
            self.inactive,
            self.closed,
            self.not_accepting_orders,
            self.order_book_disabled,
            self.unknown_asset,
            self.unknown_horizon,
            self.missing_end_date,
            self.missing_target,
            self.missing_resolution_source,
            self.missing_question,
            self.unsupported_daily_horizon,
        )
    }
}

struct Candidate {
    spec: MarketSpec,
    end_ms: i64,
}

fn is_supported_up_down_slug(slug: &str) -> bool {
    let slug = slug.to_ascii_lowercase();
    slug.contains("btc-updown")
        || slug.contains("eth-updown")
        || slug.contains("bitcoin-up-or-down")
        || slug.contains("ethereum-up-or-down")
}

fn is_daily_up_down_slug(slug: &str) -> bool {
    let slug = slug.to_ascii_lowercase();
    slug.starts_with("btc-updown-daily-") || slug.starts_with("eth-updown-daily-")
}

fn horizon_from_slug(slug: &str) -> Option<Horizon> {
    Horizon::from_slug(slug).or_else(|| has_named_hourly_suffix(slug).then_some(Horizon::H1))
}

fn has_named_hourly_suffix(slug: &str) -> bool {
    let tokens = slug
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();
    if tokens.len() < 2 || !tokens[tokens.len() - 1].eq_ignore_ascii_case("et") {
        return false;
    }
    let hour = tokens[tokens.len() - 2].to_ascii_lowercase();
    let Some(hour_digits) = hour.strip_suffix("am").or_else(|| hour.strip_suffix("pm")) else {
        return false;
    };
    hour_digits
        .parse::<u8>()
        .is_ok_and(|hour| (1..=12).contains(&hour))
}

fn event_price_to_beat(value: &Value) -> Option<f64> {
    let direct = value.get("eventMetadata").and_then(parse_price_to_beat);
    let nested = value
        .get("events")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|event| event.get("eventMetadata"))
        .find_map(parse_price_to_beat);
    direct
        .or(nested)
        .filter(|price| price.is_finite() && *price > 0.0)
}

fn parse_price_to_beat(metadata: &Value) -> Option<f64> {
    let parsed_metadata;
    let metadata = if let Some(encoded) = metadata.as_str() {
        parsed_metadata = serde_json::from_str::<Value>(encoded).ok()?;
        &parsed_metadata
    } else {
        metadata
    };
    let price = metadata.get("priceToBeat")?;
    price
        .as_f64()
        .or_else(|| price.as_str()?.parse::<f64>().ok())
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use super::{
        has_named_hourly_suffix, horizon_from_slug, is_daily_up_down_slug,
        is_supported_up_down_slug, probe_slugs,
    };
    use crate::domain::Horizon;

    #[test]
    fn identifies_supported_market_families_and_hourly_suffixes() {
        assert!(is_supported_up_down_slug("btc-updown-5m-june-20"));
        assert!(is_supported_up_down_slug("btc-updown-daily-1790116200"));
        assert!(is_daily_up_down_slug("btc-updown-daily-1790116200"));
        assert!(is_daily_up_down_slug("eth-updown-daily-1790116200"));
        assert_eq!(horizon_from_slug("btc-updown-daily-1790116200"), None);
        assert!(!is_daily_up_down_slug("btc-updown-4h-1790116200"));
        assert!(is_supported_up_down_slug("ethereum-up-or-down-10pm-et"));
        assert!(!is_supported_up_down_slug("bitcoin-price-above-100k"));
        assert!(has_named_hourly_suffix("bitcoin-up-or-down-july-8-10am-et"));
        assert!(has_named_hourly_suffix("bitcoin-up-or-down-july-8-10pm-et"));
        assert!(!has_named_hourly_suffix(
            "bitcoin-up-or-down-july-8-13pm-et"
        ));
        assert_eq!(
            horizon_from_slug("ethereum-up-or-down-july-8-10pm-et"),
            Some(Horizon::H1)
        );
        assert_eq!(
            horizon_from_slug("btc-updown-15m-june-20"),
            Some(Horizon::M15)
        );
    }

    #[test]
    fn probes_epoch_boundaries_and_named_new_york_hours() {
        let now = "2026-09-22T22:34:50Z"
            .parse::<DateTime<Utc>>()
            .expect("parse test instant");
        let slugs = probe_slugs(now);

        assert_eq!(slugs.len(), 20);
        assert!(slugs.contains(&"btc-updown-5m-1790116200".to_owned()));
        assert!(slugs.contains(&"eth-updown-4h-1790107200".to_owned()));
        let daily_epoch = now.timestamp().div_euclid(86_400) * 86_400;
        assert!(slugs.contains(&format!("btc-updown-daily-{daily_epoch}")));
        assert!(slugs.contains(&format!("btc-updown-daily-{}", daily_epoch - 86_400)));
        assert!(slugs.contains(&format!("eth-updown-daily-{daily_epoch}")));
        assert!(slugs.contains(&format!("eth-updown-daily-{}", daily_epoch - 86_400)));
        assert!(slugs.contains(&"bitcoin-up-or-down-september-22-2026-6pm-et".to_owned()));
        assert!(slugs.contains(&"bitcoin-up-or-down-september-22-2026-7pm-et".to_owned()));
        assert!(slugs.contains(&"ethereum-up-or-down-september-22-2026-6pm-et".to_owned()));
        assert!(slugs.contains(&"ethereum-up-or-down-september-22-2026-7pm-et".to_owned()));
    }

    #[test]
    fn named_hour_probes_follow_new_york_dst_transitions() {
        let before_spring_forward = "2026-03-08T06:30:00Z"
            .parse::<DateTime<Utc>>()
            .expect("parse test instant");
        let slugs = probe_slugs(before_spring_forward);
        assert!(slugs.contains(&"bitcoin-up-or-down-march-8-2026-1am-et".to_owned()));
        assert!(slugs.contains(&"bitcoin-up-or-down-march-8-2026-3am-et".to_owned()));
    }
}
