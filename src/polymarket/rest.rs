use thiserror::Error;

use polymarket_client_sdk_v2::clob::{
    Client as ClobClient, types::request::OrderBookSummaryRequest,
};
use polymarket_client_sdk_v2::gamma::{Client as GammaClient, types::request::MarketBySlugRequest};
use polymarket_client_sdk_v2::types::{Decimal, U256};

use crate::domain::{ConditionId, MarketId, PriceTicks, TickSize, TokenId};

use super::book::BASE_UNITS_PER_TOKEN;

/// Market identifiers needed to initialize a local YES-side book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketMetadata {
    pub market_id: MarketId,
    pub condition_id: ConditionId,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
}

/// Executable top-of-book values returned by the CLOB discovery call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopOfBookSnapshot {
    pub best_bid: Option<PriceTicks>,
    pub best_ask: Option<PriceTicks>,
    pub tick_size: TickSize,
    pub min_order_size: u64,
}

/// Errors raised while translating SDK responses into provider-free domain data.
#[derive(Debug, Error)]
pub enum RestError {
    #[error("Polymarket SDK request failed: {0}")]
    Sdk(#[from] polymarket_client_sdk_v2::error::Error),
    #[error("market slug must not be empty")]
    EmptySlug,
    #[error("market `{market}` is missing `{field}`")]
    MissingField { market: String, field: &'static str },
    #[error("market `{market}` has {count} outcome tokens; a binary market is required")]
    UnsupportedOutcomeCount { market: String, count: usize },
    #[error("market `{market}` does not identify both YES and NO outcomes")]
    MissingOutcomeLabels { market: String },
    #[error("invalid token id `{0}`")]
    InvalidTokenId(String),
    #[error("invalid decimal `{value}` for `{field}`")]
    InvalidDecimal { field: &'static str, value: String },
    #[error("price `{value}` for `{field}` is outside the executable 0..=1 range")]
    PriceOutOfRange { field: &'static str, value: String },
    #[error("tick size `{value}` is outside the executable (0, 1] range")]
    TickSizeOutOfRange { value: String },
    #[error("quantity `{value}` for `{field}` cannot be represented in u64 base units")]
    QuantityOutOfRange { field: &'static str, value: String },
}

/// Fetches Gamma market metadata by slug and translates it to domain IDs.
///
/// This helper is read-only. It intentionally does not retain provider SDK
/// response types beyond this conversion boundary.
pub async fn fetch_market_by_slug(
    client: &GammaClient,
    slug: &str,
) -> Result<MarketMetadata, RestError> {
    if slug.is_empty() {
        return Err(RestError::EmptySlug);
    }

    let request = MarketBySlugRequest::builder().slug(slug.to_owned()).build();
    let market = client.market_by_slug(&request).await?;
    let market_name = if market.id.is_empty() {
        slug.to_owned()
    } else {
        market.id.clone()
    };

    let condition_id = market.condition_id.ok_or_else(|| RestError::MissingField {
        market: market_name.clone(),
        field: "condition_id",
    })?;
    let token_ids = market
        .clob_token_ids
        .ok_or_else(|| RestError::MissingField {
            market: market_name.clone(),
            field: "clob_token_ids",
        })?;

    if token_ids.len() != 2 {
        return Err(RestError::UnsupportedOutcomeCount {
            market: market_name.clone(),
            count: token_ids.len(),
        });
    }

    let outcomes = market.outcomes.ok_or_else(|| RestError::MissingField {
        market: market_name.clone(),
        field: "outcomes",
    })?;
    let yes_index = outcomes.iter().position(|outcome| {
        outcome.eq_ignore_ascii_case("yes") || outcome.eq_ignore_ascii_case("up")
    });
    let no_index = outcomes.iter().position(|outcome| {
        outcome.eq_ignore_ascii_case("no") || outcome.eq_ignore_ascii_case("down")
    });
    let (yes_index, no_index) = match (yes_index, no_index) {
        (Some(yes), Some(no)) if yes != no => (yes, no),
        _ => {
            return Err(RestError::MissingOutcomeLabels {
                market: market_name,
            });
        }
    };

    Ok(MarketMetadata {
        market_id: MarketId(market.id),
        condition_id: ConditionId(condition_id.to_string()),
        yes_token_id: TokenId(token_ids[yes_index].to_string()),
        no_token_id: TokenId(token_ids[no_index].to_string()),
    })
}

/// Fetches the CLOB order-book summary and returns executable top-of-book data.
///
/// Prices are quantized into [`PriceTicks`]. The SDK's decimal quantities are
/// converted to [`BASE_UNITS_PER_TOKEN`] integer base units so callers never
/// execute using floating-point quantities.
pub async fn fetch_top_of_book(
    client: &ClobClient,
    token_id: &TokenId,
) -> Result<TopOfBookSnapshot, RestError> {
    let asset_id = token_id
        .0
        .parse::<U256>()
        .map_err(|_| RestError::InvalidTokenId(token_id.0.clone()))?;
    let request = OrderBookSummaryRequest::builder()
        .token_id(asset_id)
        .build();
    let book = client.order_book(&request).await?;

    let best_bid = book
        .bids
        .iter()
        .filter(|level| level.size > Decimal::ZERO)
        .max_by(|left, right| left.price.cmp(&right.price))
        .map(|level| decimal_to_price_ticks(level.price, "best_bid"))
        .transpose()?;
    let best_ask = book
        .asks
        .iter()
        .filter(|level| level.size > Decimal::ZERO)
        .min_by(|left, right| left.price.cmp(&right.price))
        .map(|level| decimal_to_price_ticks(level.price, "best_ask"))
        .transpose()?;

    let tick_size = decimal_to_f64(book.tick_size.into(), "tick_size")?;
    if tick_size <= 0.0 || tick_size > 1.0 {
        return Err(RestError::TickSizeOutOfRange {
            value: tick_size.to_string(),
        });
    }

    Ok(TopOfBookSnapshot {
        best_bid,
        best_ask,
        tick_size: TickSize::from_f64(tick_size),
        min_order_size: decimal_to_base_units(book.min_order_size, "min_order_size")?,
    })
}

fn decimal_to_price_ticks(value: Decimal, field: &'static str) -> Result<PriceTicks, RestError> {
    let number = decimal_to_f64(value, field)?;
    if !(0.0..=1.0).contains(&number) {
        return Err(RestError::PriceOutOfRange {
            field,
            value: value.to_string(),
        });
    }
    Ok(PriceTicks::from_f64(number))
}

fn decimal_to_f64(value: Decimal, field: &'static str) -> Result<f64, RestError> {
    let text = value.to_string();
    let number = text.parse::<f64>().map_err(|_| RestError::InvalidDecimal {
        field,
        value: text.clone(),
    })?;
    if number.is_finite() {
        Ok(number)
    } else {
        Err(RestError::InvalidDecimal { field, value: text })
    }
}

fn decimal_to_base_units(value: Decimal, field: &'static str) -> Result<u64, RestError> {
    let text = value.to_string();
    if value.is_sign_negative() {
        return Err(RestError::QuantityOutOfRange { field, value: text });
    }

    let scale = value.scale();
    let mantissa = u128::try_from(value.mantissa()).map_err(|_| RestError::QuantityOutOfRange {
        field,
        value: text.clone(),
    })?;
    let base_scale = BASE_UNITS_PER_TOKEN.ilog10();
    let scaled = if scale <= base_scale {
        mantissa.checked_mul(10_u128.pow(base_scale - scale))
    } else {
        let divisor = 10_u128.pow(scale - base_scale);
        let (quotient, remainder) = (mantissa / divisor, mantissa % divisor);
        (remainder == 0).then_some(quotient)
    }
    .ok_or_else(|| RestError::QuantityOutOfRange {
        field,
        value: text.clone(),
    })?;

    u64::try_from(scaled).map_err(|_| RestError::QuantityOutOfRange { field, value: text })
}
