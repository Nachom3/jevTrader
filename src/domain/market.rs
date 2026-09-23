use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

/// The underlying asset traded by the market family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Asset {
    Btc,
    Eth,
}

impl Asset {
    pub const BTC: Self = Self::Btc;
    pub const ETH: Self = Self::Eth;

    pub const ALL: [Self; 2] = [Self::Btc, Self::Eth];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Btc => "BTC",
            Self::Eth => "ETH",
        }
    }

    /// Finds a supported asset token in a market slug.
    #[must_use]
    pub fn from_slug(slug: &str) -> Option<Self> {
        slug.split(|character: char| !character.is_ascii_alphanumeric())
            .find_map(|token| match token.to_ascii_lowercase().as_str() {
                "btc" | "bitcoin" => Some(Self::Btc),
                "eth" | "ethereum" => Some(Self::Eth),
                _ => None,
            })
    }
}

impl fmt::Display for Asset {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for Asset {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "btc" | "bitcoin" => Ok(Self::Btc),
            "eth" | "ethereum" => Ok(Self::Eth),
            _ => Err("asset must be BTC or ETH"),
        }
    }
}

/// The resolution horizon represented by a market.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Horizon {
    M5,
    M15,
    H1,
    H4,
    Daily,
}

impl Horizon {
    pub const FIVE_MINUTES: Self = Self::M5;
    pub const FIFTEEN_MINUTES: Self = Self::M15;
    pub const ONE_HOUR: Self = Self::H1;
    pub const FOUR_HOURS: Self = Self::H4;
    pub const DAILY: Self = Self::Daily;

    pub const ALL: [Self; 5] = [Self::M5, Self::M15, Self::H1, Self::H4, Self::Daily];

    #[must_use]
    pub const fn seconds(self) -> u64 {
        match self {
            Self::M5 => 5 * 60,
            Self::M15 => 15 * 60,
            Self::H1 => 60 * 60,
            Self::H4 => 4 * 60 * 60,
            Self::Daily => 24 * 60 * 60,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::M5 => "5m",
            Self::M15 => "15m",
            Self::H1 => "1h",
            Self::H4 => "4h",
            Self::Daily => "daily",
        }
    }

    /// Finds a supported horizon token in a market slug.
    #[must_use]
    pub fn from_slug(slug: &str) -> Option<Self> {
        let tokens = slug
            .split(|character: char| !character.is_ascii_alphanumeric())
            .filter(|token| !token.is_empty())
            .map(str::to_ascii_lowercase)
            .collect::<Vec<_>>();

        tokens
            .iter()
            .find_map(|token| Self::from_str(token).ok())
            .or_else(|| {
                tokens.windows(2).find_map(|pair| {
                    let value = format!("{}{}", pair[0], pair[1]);
                    Self::from_str(&value).ok()
                })
            })
    }
}

impl fmt::Display for Horizon {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Daily => formatter.write_str("1d"),
            _ => formatter.write_str(self.as_str()),
        }
    }
}

impl FromStr for Horizon {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "5m" | "5min" | "5mins" | "5minute" | "5minutes" => Ok(Self::M5),
            "15m" | "15min" | "15mins" | "15minute" | "15minutes" => Ok(Self::M15),
            "1h" | "1hr" | "1hour" | "1hours" => Ok(Self::H1),
            "4h" | "4hr" | "4hour" | "4hours" => Ok(Self::H4),
            "daily" | "1d" | "day" => Ok(Self::Daily),
            _ => Err("horizon must be 5m, 15m, 1h, 4h, or daily"),
        }
    }
}

/// A stable identifier for one asset and resolution horizon pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MarketKey {
    pub asset: Asset,
    pub horizon: Horizon,
}

impl MarketKey {
    #[must_use]
    pub const fn new(asset: Asset, horizon: Horizon) -> Self {
        Self { asset, horizon }
    }

    #[must_use]
    pub const fn all() -> [Self; 8] {
        [
            Self::new(Asset::Btc, Horizon::M5),
            Self::new(Asset::Btc, Horizon::M15),
            Self::new(Asset::Btc, Horizon::H1),
            Self::new(Asset::Btc, Horizon::H4),
            Self::new(Asset::Eth, Horizon::M5),
            Self::new(Asset::Eth, Horizon::M15),
            Self::new(Asset::Eth, Horizon::H1),
            Self::new(Asset::Eth, Horizon::H4),
        ]
    }

    /// Canonical tag used as `market_id` in storage and reports
    /// (`BTC-5m`, `ETH-1h`, ...).
    #[must_use]
    pub fn market_id(self) -> String {
        format!("{}-{}", self.asset.as_str(), self.horizon.as_str())
    }
}

impl fmt::Display for MarketKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}-{}",
            self.asset.to_string().to_ascii_lowercase(),
            self.horizon
        )
    }
}

impl FromStr for MarketKey {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (asset, horizon) = value
            .split_once('-')
            .ok_or("market key must be formatted as ASSET-HORIZON")?;
        Ok(Self::new(asset.parse()?, horizon.parse()?))
    }
}

/// An underlying price used to calculate distance from a market target.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ReferencePrice {
    pub value: f64,
}

impl ReferencePrice {
    #[must_use]
    pub const fn new(value: f64) -> Self {
        Self { value }
    }

    #[must_use]
    pub fn is_valid(self) -> bool {
        self.value.is_finite() && self.value > 0.0
    }

    /// Returns percentage points: `(reference / target - 1) * 100`.
    #[must_use]
    pub fn distance_to_target_pct(self, target: f64) -> f64 {
        if self.is_valid() && target.is_finite() && target > 0.0 {
            (self.value / target - 1.0) * 100.0
        } else {
            0.0
        }
    }
}

impl From<f64> for ReferencePrice {
    fn from(value: f64) -> Self {
        Self::new(value)
    }
}

/// How a target price resolves the YES outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResolutionMechanism {
    Above,
    Below,
}

impl ResolutionMechanism {
    pub const ABOVE_TARGET: Self = Self::Above;
    pub const BELOW_TARGET: Self = Self::Below;

    #[must_use]
    pub fn from_question(question: &str) -> Self {
        let question = question.to_ascii_lowercase();
        if question.contains("below")
            || question.contains("under")
            || question.contains("less than")
        {
            Self::Below
        } else {
            Self::Above
        }
    }

    /// Returns distance in the direction that makes a positive value favorable
    /// to the YES outcome.
    #[must_use]
    pub fn distance_to_target_pct(self, reference: ReferencePrice, target: f64) -> f64 {
        let distance = reference.distance_to_target_pct(target);
        match self {
            Self::Above => distance,
            Self::Below => -distance,
        }
    }

    #[must_use]
    pub fn resolves_yes(self, reference: ReferencePrice, target: f64) -> bool {
        if !reference.is_valid() || !target.is_finite() || target <= 0.0 {
            return false;
        }
        match self {
            Self::Above => reference.value >= target,
            Self::Below => reference.value <= target,
        }
    }
}

#[cfg(test)]
mod horizon_tests {
    use super::Horizon;

    #[test]
    fn daily_horizon_has_market_slug_and_display_representations() {
        for alias in ["daily", "1d", "day"] {
            assert_eq!(alias.parse::<Horizon>(), Ok(Horizon::Daily));
        }
        assert_eq!(Horizon::Daily.as_str(), "daily");
        assert_eq!(Horizon::Daily.to_string(), "1d");
        assert_eq!(Horizon::Daily.seconds(), 86_400);
        assert_eq!(
            Horizon::from_slug("btc-updown-daily-1790116200"),
            Some(Horizon::Daily)
        );
    }
}

impl FromStr for ResolutionMechanism {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "above" | "over" | "up" => Ok(Self::Above),
            "below" | "under" | "down" => Ok(Self::Below),
            _ => Err("resolution mechanism must be above or below"),
        }
    }
}

/// The initial/reference price a resolution window is measured against.
///
/// The venue is never hardcoded: `source` names the series the market
/// metadata declares (e.g. `Binance BTC/USDT 1m close`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReferencePoint {
    pub value: f64,
    pub source: String,
    pub observed_at_ms: i64,
    pub window_secs: u64,
}

impl ReferencePoint {
    /// Creates a reference point without validating the venue.
    #[must_use]
    pub fn new(
        value: f64,
        source: impl Into<String>,
        observed_at_ms: i64,
        window_secs: u64,
    ) -> Self {
        Self {
            value,
            source: source.into(),
            observed_at_ms,
            window_secs,
        }
    }

    /// `spot - reference` in price units. Non-finite inputs yield `0.0`.
    #[must_use]
    pub fn raw_distance(&self, spot: f64) -> f64 {
        if spot.is_finite() && self.value.is_finite() {
            spot - self.value
        } else {
            0.0
        }
    }

    /// `(spot / reference - 1) * 100` in percentage points.
    #[must_use]
    pub fn distance_pct(&self, spot: f64) -> f64 {
        if spot.is_finite() && spot > 0.0 && self.value.is_finite() && self.value > 0.0 {
            (spot / self.value - 1.0) * 100.0
        } else {
            0.0
        }
    }

    /// Distance in sigma units, optionally scaled by `sqrt(time_remaining)`.
    #[must_use]
    pub fn distance_sigma(
        &self,
        spot: f64,
        sigma_pct: f64,
        time_remaining_secs: u64,
        horizon_scaling: bool,
    ) -> f64 {
        let distance_pct = self.distance_pct(spot);
        if !distance_pct.is_finite() {
            return 0.0;
        }
        if !(sigma_pct.is_finite() && sigma_pct > 0.0) {
            return 0.0;
        }
        let denominator = if horizon_scaling {
            if time_remaining_secs == 0 {
                return 0.0;
            }
            sigma_pct * (time_remaining_secs as f64).sqrt()
        } else {
            sigma_pct
        };
        let value = distance_pct / denominator;
        if value.is_finite() { value } else { 0.0 }
    }
}

/// How a contract resolves: source, window, start/end, and reference.
///
/// The engine compares `current_resolution_price` against `reference`
/// through this type; the strategy only sees the normalized distances and
/// the remaining time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolutionWindow {
    pub source: String,
    pub window: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub reference: ReferencePoint,
}

impl ResolutionWindow {
    /// Creates a resolution window. Malformed metadata (end <= start) is
    /// represented, not panicked on, so the hot path can report it.
    #[must_use]
    pub fn new(
        source: impl Into<String>,
        window: impl Into<String>,
        start_ms: i64,
        end_ms: i64,
        reference: ReferencePoint,
    ) -> Self {
        Self {
            source: source.into(),
            window: window.into(),
            start_ms,
            end_ms,
            reference,
        }
    }

    /// Seconds from `observed_at_ms` to resolution, saturating at zero.
    #[must_use]
    pub fn time_remaining_secs(&self, observed_at_ms: i64) -> u64 {
        self.end_ms.saturating_sub(observed_at_ms).max(0) as u64 / 1_000
    }

    /// Whether the contract is inside its resolution window.
    #[must_use]
    pub fn is_active_at(&self, observed_at_ms: i64) -> bool {
        observed_at_ms >= self.start_ms && observed_at_ms < self.end_ms
    }

    /// Raw price distance of `spot` to the reference.
    #[must_use]
    pub fn raw_distance(&self, spot: f64) -> f64 {
        self.reference.raw_distance(spot)
    }

    /// Percentage distance of `spot` to the reference.
    #[must_use]
    pub fn distance_pct(&self, spot: f64) -> f64 {
        self.reference.distance_pct(spot)
    }
}
