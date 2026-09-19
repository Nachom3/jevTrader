//! Isolated paper portfolios: CONTROL and QUANT_V1 never contaminate.
//!
//! Portfolios are keyed by (variant, market). All PnL is in percentage
//! points per share for buys: realized on exits, unrealized on open
//! inventory vs the latest mid, total = realized + unrealized.

use std::collections::BTreeMap;

/// One fill applied to a portfolio.
#[derive(Debug, Clone, Copy)]
pub struct FillEvent {
    pub price: f64,
    pub size: f64,
    pub ts_ms: i64,
    pub toxic: bool,
}

/// Isolated portfolio for one (variant, market) pair.
#[derive(Debug, Clone, Default)]
pub struct Portfolio {
    pub fills: Vec<FillEvent>,
    pub exits: Vec<(f64, f64, i64)>,
    pub last_mid: Option<f64>,
    pub peak_total: f64,
    pub max_drawdown_pp: f64,
}

impl Portfolio {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply_fill(&mut self, fill: FillEvent) {
        self.fills.push(fill);
    }

    pub fn apply_exit(&mut self, price: f64, size: f64, ts_ms: i64) {
        self.exits.push((price, size, ts_ms));
        let _ = ts_ms;
    }

    pub fn observe_mid(&mut self, mid: f64) {
        self.last_mid = Some(mid);
        let total = self.total_pnl_pp();
        if total > self.peak_total {
            self.peak_total = total;
        }
        let dd = self.peak_total - total;
        if dd > self.max_drawdown_pp {
            self.max_drawdown_pp = dd;
        }
    }

    fn bought_notional(&self) -> f64 {
        self.fills.iter().map(|f| f.price * f.size).sum()
    }

    fn bought_size(&self) -> f64 {
        self.fills.iter().map(|f| f.size).sum()
    }

    fn exited_size(&self) -> f64 {
        self.exits.iter().map(|e| e.1).sum()
    }

    /// Realized PnL in pp: exits vs average buy price (FIFO-approx avg).
    #[must_use]
    pub fn realized_pnl_pp(&self) -> f64 {
        let bs = self.bought_size();
        if bs <= 0.0 {
            return 0.0;
        }
        let avg_buy = self.bought_notional() / bs;
        self.exits
            .iter()
            .map(|(p, s, _)| (p - avg_buy) * 100.0 * s)
            .sum()
    }

    /// Unrealized PnL on open inventory vs the latest mid.
    #[must_use]
    pub fn unrealized_pnl_pp(&self) -> f64 {
        let open = self.bought_size() - self.exited_size();
        if open <= 0.0 {
            return 0.0;
        }
        let bs = self.bought_size();
        let avg_buy = if bs > 0.0 {
            self.bought_notional() / bs
        } else {
            0.0
        };
        match self.last_mid {
            Some(mid) => (mid - avg_buy) * 100.0 * open,
            None => 0.0,
        }
    }

    #[must_use]
    pub fn total_pnl_pp(&self) -> f64 {
        self.realized_pnl_pp() + self.unrealized_pnl_pp()
    }

    #[must_use]
    pub fn inventory(&self) -> f64 {
        self.bought_size() - self.exited_size()
    }

    #[must_use]
    pub fn turnover(&self) -> f64 {
        self.bought_size() + self.exited_size()
    }

    #[must_use]
    pub fn fill_count(&self) -> usize {
        self.fills.len()
    }

    #[must_use]
    pub fn toxic_fill_rate(&self) -> f64 {
        if self.fills.is_empty() {
            return 0.0;
        }
        self.fills.iter().filter(|f| f.toxic).count() as f64 / self.fills.len() as f64
    }
}

/// Aggregate stats over one portfolio.
#[derive(Debug, Clone, Default)]
pub struct PortfolioStats {
    pub realized_pp: f64,
    pub unrealized_pp: f64,
    pub total_pp: f64,
    pub max_drawdown_pp: f64,
    pub inventory: f64,
    pub turnover: f64,
    pub fills: usize,
    pub toxic_fill_rate: f64,
}

impl PortfolioStats {
    #[must_use]
    pub fn of(p: &Portfolio) -> Self {
        Self {
            realized_pp: p.realized_pnl_pp(),
            unrealized_pp: p.unrealized_pnl_pp(),
            total_pp: p.total_pnl_pp(),
            max_drawdown_pp: p.max_drawdown_pp,
            inventory: p.inventory(),
            turnover: p.turnover(),
            fills: p.fill_count(),
            toxic_fill_rate: p.toxic_fill_rate(),
        }
    }
}

/// Registry of isolated portfolios keyed by (variant, market).
#[derive(Debug, Default)]
pub struct PortfolioRegistry {
    inner: BTreeMap<(String, String), Portfolio>,
}

impl PortfolioRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn portfolio(&mut self, variant: &str, market: &str) -> &mut Portfolio {
        self.inner
            .entry((variant.to_owned(), market.to_owned()))
            .or_default()
    }

    #[must_use]
    pub fn get(&self, variant: &str, market: &str) -> Option<&Portfolio> {
        self.inner.get(&(variant.to_owned(), market.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn realized_unrealized_and_drawdown() {
        let mut p = Portfolio::new();
        p.apply_fill(FillEvent {
            price: 0.40,
            size: 10.0,
            ts_ms: 0,
            toxic: false,
        });
        p.observe_mid(0.42);
        assert!((p.unrealized_pnl_pp() - 20.0).abs() < 1e-9);
        p.apply_exit(0.43, 10.0, 60_000);
        assert!((p.realized_pnl_pp() - 30.0).abs() < 1e-9);
        assert_eq!(p.inventory(), 0.0);
        p.observe_mid(0.39);
        assert!(p.max_drawdown_pp >= 0.0);
    }

    #[test]
    fn held_to_resolution_uses_real_outcome() {
        // YES winner -> 1.0; loser -> 0.0. Resolution exit at outcome.
        let mut winner = Portfolio::new();
        winner.apply_fill(FillEvent {
            price: 0.43,
            size: 10.0,
            ts_ms: 0,
            toxic: false,
        });
        winner.apply_exit(1.0, 10.0, 300_000);
        assert!((winner.realized_pnl_pp() - 570.0).abs() < 1e-9);
        let mut loser = Portfolio::new();
        loser.apply_fill(FillEvent {
            price: 0.43,
            size: 10.0,
            ts_ms: 0,
            toxic: false,
        });
        loser.apply_exit(0.0, 10.0, 300_000);
        assert!(loser.realized_pnl_pp() < 0.0);
    }

    #[test]
    fn registry_isolates_variants_and_markets() {
        let mut r = PortfolioRegistry::new();
        r.portfolio("CONTROL", "BTC-5m").apply_fill(FillEvent {
            price: 0.4,
            size: 1.0,
            ts_ms: 0,
            toxic: false,
        });
        assert!(r.get("QUANT_V1", "BTC-5m").is_none());
        assert!(r.get("CONTROL", "ETH-5m").is_none());
        assert_eq!(r.get("CONTROL", "BTC-5m").unwrap().fill_count(), 1);
    }
}
