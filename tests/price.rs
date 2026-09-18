use jevtrader::domain::{PriceTicks, TickSize};
use proptest::prelude::*;

proptest! {
    #[test]
    fn from_f64_to_f64_roundtrips_within_one_micro(price in 0.0f64..=1.0) {
        let roundtrip = PriceTicks::from_f64(price).to_f64();
        prop_assert!((roundtrip - price).abs() <= 1e-6);
    }

    #[test]
    fn exact_tick_multiples_are_detected(
        k in 0u32..=400,
        tick in prop::sample::select(vec![0.01f64, 0.001, 0.0025]),
    ) {
        let price = f64::from(k) * tick;
        prop_assume!(price <= 1.0);
        prop_assert!(
            PriceTicks::from_f64(price).is_multiple_of(TickSize::from_f64(tick))
        );
    }

    #[test]
    fn half_tick_offsets_are_rejected(
        k in 0u32..=400,
        tick in prop::sample::select(vec![0.01f64, 0.001, 0.0025]),
    ) {
        // Half-tick offsets (>= 500 micros) dwarf quantization error (<= 1 micro).
        let price = f64::from(k) * tick + tick / 2.0;
        prop_assume!(price <= 1.0);
        prop_assert!(
            !PriceTicks::from_f64(price).is_multiple_of(TickSize::from_f64(tick))
        );
    }

    #[test]
    fn from_f64_ordering_is_monotonic(left in 0.0f64..=1.0, right in 0.0f64..=1.0) {
        let left_ticks = PriceTicks::from_f64(left);
        let right_ticks = PriceTicks::from_f64(right);
        if left <= right {
            prop_assert!(left_ticks <= right_ticks);
        } else {
            prop_assert!(left_ticks >= right_ticks);
        }
    }
}
