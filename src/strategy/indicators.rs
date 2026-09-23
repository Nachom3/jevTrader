//! Deterministic technical indicators over closed candles, oldest first.

/// Mean of the last `period` closes.
///
/// Worst-case time complexity is O(period); auxiliary space is O(1).
pub fn sma_last(closes: &[f64], period: usize) -> Option<f64> {
    if period == 0 || closes.len() < period {
        return None;
    }

    let window = &closes[closes.len() - period..];
    if window.iter().any(|value| !value.is_finite()) {
        return None;
    }

    mean(window)
}

/// Classic EMA seeded with the SMA of the first `period` closes.
///
/// Worst-case time complexity is O(closes.len()); auxiliary space is O(1).
pub fn ema_last(closes: &[f64], period: usize) -> Option<f64> {
    if period == 0 || closes.len() < period {
        return None;
    }
    if closes.iter().any(|value| !value.is_finite()) {
        return None;
    }

    let mut ema = mean(&closes[..period])?;
    let k = 2.0 / (period as f64 + 1.0);
    for &close in &closes[period..] {
        ema = weighted_average(ema, close, k)?;
    }

    ema.is_finite().then_some(ema)
}

/// Wilder RSI of the closes, clamped to the range 0–100.
///
/// Worst-case time complexity is O(closes.len()); auxiliary space is O(1).
pub fn rsi_last(closes: &[f64], period: usize) -> Option<f64> {
    if period == 0 || closes.len() <= period {
        return None;
    }
    if closes.iter().any(|value| !value.is_finite()) {
        return None;
    }

    let mut average_gain = 0.0;
    let mut average_loss = 0.0;
    for index in 1..=period {
        let change = closes[index] - closes[index - 1];
        if !change.is_finite() {
            return None;
        }
        let gain = change.max(0.0);
        let loss = (-change).max(0.0);
        let count = index as f64;
        let weight = 1.0 / count;
        average_gain = weighted_average(average_gain, gain, weight)?;
        average_loss = weighted_average(average_loss, loss, weight)?;
    }

    let weight = 1.0 / period as f64;
    for index in (period + 1)..closes.len() {
        let change = closes[index] - closes[index - 1];
        if !change.is_finite() {
            return None;
        }
        let gain = change.max(0.0);
        let loss = (-change).max(0.0);
        average_gain = weighted_average(average_gain, gain, weight)?;
        average_loss = weighted_average(average_loss, loss, weight)?;
    }

    // No losses is conventionally treated as the upper RSI bound, including
    // a completely unchanged series where both smoothed values are zero.
    if average_loss == 0.0 {
        return Some(100.0);
    }

    let scale = average_gain.max(average_loss);
    let normalized_gain = average_gain / scale;
    let normalized_loss = average_loss / scale;
    let rsi = 100.0 * normalized_gain / (normalized_gain + normalized_loss);
    rsi.is_finite().then_some(rsi.clamp(0.0, 100.0))
}

/// Wilder ADX from high, low, and close candles, clamped to the range 0–100.
///
/// The first `period` directional movements seed the smoothed components; the
/// first DX seeds ADX, so `period + 1` candles are sufficient. Worst-case time
/// complexity is O(highs.len()); auxiliary space is O(1).
pub fn adx_last(highs: &[f64], lows: &[f64], closes: &[f64], period: usize) -> Option<f64> {
    if period == 0
        || highs.len() <= period
        || lows.len() != highs.len()
        || closes.len() != highs.len()
    {
        return None;
    }
    if highs
        .iter()
        .chain(lows)
        .chain(closes)
        .any(|value| !value.is_finite())
    {
        return None;
    }

    let mut smooth_tr = 0.0;
    let mut smooth_plus_dm = 0.0;
    let mut smooth_minus_dm = 0.0;
    for index in 1..=period {
        let (tr, plus_dm, minus_dm) = directional_movement(highs, lows, closes, index)?;
        let weight = 1.0 / index as f64;
        smooth_tr = weighted_average(smooth_tr, tr, weight)?;
        smooth_plus_dm = weighted_average(smooth_plus_dm, plus_dm, weight)?;
        smooth_minus_dm = weighted_average(smooth_minus_dm, minus_dm, weight)?;
    }

    let mut adx = directional_index(smooth_tr, smooth_plus_dm, smooth_minus_dm)?;
    let weight = 1.0 / period as f64;
    for index in (period + 1)..highs.len() {
        let (tr, plus_dm, minus_dm) = directional_movement(highs, lows, closes, index)?;
        smooth_tr = wilder_average(smooth_tr, tr, period)?;
        smooth_plus_dm = wilder_average(smooth_plus_dm, plus_dm, period)?;
        smooth_minus_dm = wilder_average(smooth_minus_dm, minus_dm, period)?;
        let dx = directional_index(smooth_tr, smooth_plus_dm, smooth_minus_dm)?;
        adx = weighted_average(adx, dx, weight)?;
    }

    adx.is_finite().then_some(adx.clamp(0.0, 100.0))
}

fn mean(values: &[f64]) -> Option<f64> {
    let mut result = 0.0;
    for (index, &value) in values.iter().enumerate() {
        let count = (index + 1) as f64;
        result = weighted_average(result, value, 1.0 / count)?;
    }
    Some(result)
}

fn weighted_average(previous: f64, next: f64, next_weight: f64) -> Option<f64> {
    let value = previous * (1.0 - next_weight) + next * next_weight;
    value.is_finite().then_some(value)
}

fn wilder_average(previous: f64, next: f64, period: usize) -> Option<f64> {
    let weight = 1.0 / period as f64;
    weighted_average(previous, next, weight)
}

fn directional_movement(
    highs: &[f64],
    lows: &[f64],
    closes: &[f64],
    index: usize,
) -> Option<(f64, f64, f64)> {
    let up_move = highs[index] - highs[index - 1];
    let down_move = lows[index - 1] - lows[index];
    let high_low = highs[index] - lows[index];
    let high_previous_close = (highs[index] - closes[index - 1]).abs();
    let low_previous_close = (lows[index] - closes[index - 1]).abs();
    if !up_move.is_finite()
        || !down_move.is_finite()
        || !high_low.is_finite()
        || !high_previous_close.is_finite()
        || !low_previous_close.is_finite()
    {
        return None;
    }

    let tr = high_low.max(high_previous_close).max(low_previous_close);
    let plus_dm = if up_move > down_move && up_move > 0.0 {
        up_move
    } else {
        0.0
    };
    let minus_dm = if down_move > up_move && down_move > 0.0 {
        down_move
    } else {
        0.0
    };
    Some((tr, plus_dm, minus_dm))
}

fn directional_index(smooth_tr: f64, smooth_plus_dm: f64, smooth_minus_dm: f64) -> Option<f64> {
    if smooth_tr == 0.0 {
        return None;
    }

    let plus_di = 100.0 * (smooth_plus_dm / smooth_tr);
    let minus_di = 100.0 * (smooth_minus_dm / smooth_tr);
    let di_sum = plus_di + minus_di;
    if !plus_di.is_finite() || !minus_di.is_finite() || di_sum == 0.0 || !di_sum.is_finite() {
        return None;
    }

    let dx = 100.0 * ((plus_di - minus_di).abs() / di_sum);
    dx.is_finite().then_some(dx.clamp(0.0, 100.0))
}

#[cfg(test)]
mod tests {
    use super::{adx_last, ema_last, rsi_last, sma_last};

    fn assert_close(actual: Option<f64>, expected: f64) {
        let actual = actual.expect("indicator should have a value");
        assert!((actual - expected).abs() < 1e-9, "{actual} != {expected}");
    }

    #[test]
    fn sma_uses_the_requested_trailing_window() {
        let closes = [1.0, 2.0, 3.0, 4.0, 5.0];
        assert_close(sma_last(&closes, 5), 3.0);
        assert_close(sma_last(&closes, 3), 4.0);
    }

    #[test]
    fn ema_period_one_is_the_last_close() {
        assert_eq!(ema_last(&[1.0, 2.0, 7.0], 1), Some(7.0));
    }

    #[test]
    fn rsi_handles_unidirectional_and_mixed_moves() {
        assert_eq!(rsi_last(&[1.0, 2.0, 3.0, 4.0], 2), Some(100.0));
        assert_eq!(rsi_last(&[4.0, 3.0, 2.0, 1.0], 2), Some(0.0));

        // For [10, 12, 11, 14] with period 2, the seeded averages are
        // gain=(2+0)/2=1 and loss=(0+1)/2=0.5. After the final +3,
        // Wilder smoothing gives gain=(1+3)/2=2 and loss=0.25;
        // RSI=100*2/(2+0.25)=800/9.
        assert_close(rsi_last(&[10.0, 12.0, 11.0, 14.0], 2), 800.0 / 9.0);
    }

    #[test]
    fn adx_reports_strong_trends_and_handles_flat_data() {
        let highs = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0];
        let lows = [9.0, 10.0, 11.0, 12.0, 13.0, 14.0];
        let closes = [9.5, 10.5, 11.5, 12.5, 13.5, 14.5];
        assert!(adx_last(&highs, &lows, &closes, 2).is_some_and(|adx| adx > 25.0));

        let flat_highs = [10.5; 4];
        let flat_lows = [9.5; 4];
        let flat_closes = [10.0; 4];
        assert!(adx_last(&flat_highs, &flat_lows, &flat_closes, 2).is_none_or(|adx| adx < 15.0));
    }

    #[test]
    fn insufficient_data_returns_none() {
        assert_eq!(sma_last(&[1.0, 2.0], 3), None);
        assert_eq!(ema_last(&[1.0, 2.0], 3), None);
        assert_eq!(rsi_last(&[1.0, 2.0], 2), None);
        assert_eq!(adx_last(&[2.0, 3.0], &[1.0, 2.0], &[1.5, 2.5], 2), None);
    }

    #[test]
    fn non_finite_inputs_return_none() {
        let closes = [1.0, f64::NAN, 3.0];
        assert_eq!(sma_last(&closes, 2), None);
        assert_eq!(ema_last(&closes, 2), None);
        assert_eq!(rsi_last(&closes, 2), None);
        assert_eq!(
            adx_last(&[2.0, f64::NAN], &[1.0, 2.0], &[1.5, 2.5], 1),
            None
        );
        assert_eq!(adx_last(&[2.0, 3.0], &[1.0, 2.0], &closes[..2], 1), None);
    }

    #[test]
    fn zero_period_returns_none() {
        assert_eq!(sma_last(&[1.0], 0), None);
        assert_eq!(ema_last(&[1.0], 0), None);
        assert_eq!(rsi_last(&[1.0], 0), None);
        assert_eq!(adx_last(&[2.0], &[1.0], &[1.5], 0), None);
    }
}
