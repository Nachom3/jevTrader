//! Provider-free types shared by external exchange adapters.

use thiserror::Error;

/// External venue represented by a normalized feed tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Venue {
    Binance,
    Coinbase,
    Deribit,
}

/// A normalized statistical observation from an external venue.
///
/// All `f64` values in this type are statistical only: they are suitable for
/// returns, volatility, and other feature calculations. They are **not
/// executable prices** and must never flow into an executable `PriceTicks`
/// without explicit quantization at that boundary. A feed that does not carry
/// a value for a field uses `f64::NAN` for that field.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VenueTick {
    pub venue: Venue,
    pub symbol: &'static str,
    pub price_f64: f64,
    pub best_bid_f64: f64,
    pub best_ask_f64: f64,
    pub trade_size_f64: f64,
    pub trade_side_buy: bool,
    pub ts_exchange_ms: i64,
    pub ts_local_ms: i64,
}

/// Errors raised while connecting to, decoding, or exhausting an external
/// venue feed.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum FeedError {
    #[error("feed connection failed: {0}")]
    Connect(String),
    #[error("feed payload parse failed: {0}")]
    Parse(String),
    #[error("feed reconnect budget exhausted after {attempts} retries")]
    Exhausted { attempts: u32 },
}

/// Parses the RFC 3339 UTC timestamps used by Coinbase without adding a time
/// crate just for feed normalization. Sub-millisecond precision is truncated
/// because the common type stores milliseconds.
#[allow(dead_code)]
pub(crate) fn parse_rfc3339_millis(value: &str) -> Result<i64, FeedError> {
    let (date, time) = value
        .split_once('T')
        .ok_or_else(|| FeedError::Parse(format!("invalid RFC 3339 timestamp `{value}`")))?;
    let time = time
        .strip_suffix('Z')
        .ok_or_else(|| FeedError::Parse(format!("timestamp is not UTC `{value}`")))?;
    let mut date_parts = date.split('-');
    let year = parse_component(date_parts.next(), "year", value)? as i64;
    let month = parse_component(date_parts.next(), "month", value)? as i64;
    let day = parse_component(date_parts.next(), "day", value)? as i64;
    if date_parts.next().is_some() || !(1..=12).contains(&month) {
        return Err(FeedError::Parse(format!(
            "invalid date in timestamp `{value}`"
        )));
    }

    let (clock, fraction) = time.split_once('.').unwrap_or((time, ""));
    let mut clock_parts = clock.split(':');
    let hour = parse_component(clock_parts.next(), "hour", value)? as i64;
    let minute = parse_component(clock_parts.next(), "minute", value)? as i64;
    let second = parse_component(clock_parts.next(), "second", value)? as i64;
    if clock_parts.next().is_some()
        || hour > 23
        || minute > 59
        || second > 59
        || fraction
            .chars()
            .any(|character| !character.is_ascii_digit())
    {
        return Err(FeedError::Parse(format!(
            "invalid time in timestamp `{value}`"
        )));
    }

    let leap_year = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_month = match month {
        2 if leap_year => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if day == 0 || day > days_in_month {
        return Err(FeedError::Parse(format!(
            "invalid day in timestamp `{value}`"
        )));
    }

    let fraction_millis = fraction
        .chars()
        .take(3)
        .enumerate()
        .try_fold(0_i64, |total, (index, character)| {
            let digit = i64::from(character.to_digit(10)?);
            let multiplier = match index {
                0 => 100,
                1 => 10,
                _ => 1,
            };
            Some(total + digit * multiplier)
        })
        .unwrap_or(0);
    let days = days_from_civil(year, month, day);
    days.checked_mul(86_400_000)
        .and_then(|millis| millis.checked_add(hour * 3_600_000))
        .and_then(|millis| millis.checked_add(minute * 60_000))
        .and_then(|millis| millis.checked_add(second * 1_000))
        .and_then(|millis| millis.checked_add(fraction_millis))
        .ok_or_else(|| FeedError::Parse(format!("timestamp out of range `{value}`")))
}

fn parse_component(component: Option<&str>, field: &str, original: &str) -> Result<u32, FeedError> {
    component
        .ok_or_else(|| FeedError::Parse(format!("missing {field} in timestamp `{original}`")))?
        .parse::<u32>()
        .map_err(|_| FeedError::Parse(format!("invalid {field} in timestamp `{original}`")))
}

// Howard Hinnant's proleptic Gregorian conversion, expressed with integer
// arithmetic. The returned value is the number of days since 1970-01-01.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let adjusted_year = year - i64::from(month <= 2);
    let era = if adjusted_year >= 0 {
        adjusted_year / 400
    } else {
        (adjusted_year - 399) / 400
    };
    let year_of_era = adjusted_year - era * 400;
    let month_prime = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod tests {
    use super::parse_rfc3339_millis;

    #[test]
    fn parses_utc_timestamp_to_milliseconds() {
        assert_eq!(
            parse_rfc3339_millis("2024-01-02T03:04:05.678901Z").unwrap(),
            1_704_164_645_678
        );
    }
}
