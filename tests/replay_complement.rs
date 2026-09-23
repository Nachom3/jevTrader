use jevtrader::replay::{
    ComplementStats, Fidelity, NoTopOfBook, SYNTHETIC_COMPLEMENT_SOURCE, SyntheticComplement,
    complement_no_to_yes,
};

fn no_book(no_bid: f64, no_ask: f64) -> NoTopOfBook {
    NoTopOfBook {
        no_bid,
        no_ask,
        no_bid_size: 7.0,
        no_ask_size: 11.0,
        ts_ms: 42_000,
        market_id: "market-1".to_owned(),
    }
}

#[test]
fn complement_mirrors_no_prices_with_exact_arithmetic() {
    let input = no_book(0.41, 0.44);
    let output = complement_no_to_yes(&input).expect("valid NO book should complement");

    assert_eq!(output.yes_bid, 1.0 - input.no_ask);
    assert_eq!(output.yes_ask, 1.0 - input.no_bid);
    assert_eq!(output.yes_mid, 1.0 - input.no_mid());
    assert_eq!(output.ts_ms, input.ts_ms);
    assert_eq!(output.market_id, input.market_id);
}

#[test]
fn complement_carries_sizes_to_the_mirrored_yes_levels() {
    let output = complement_no_to_yes(&no_book(0.25, 0.30)).expect("valid NO book");

    assert_eq!(output.yes_bid_size, 11.0);
    assert_eq!(output.yes_ask_size, 7.0);
}

#[test]
fn crossed_no_spread_is_rejected() {
    let input = no_book(0.60, 0.40);

    assert!(complement_no_to_yes(&input).is_none());
}

#[test]
fn non_finite_and_out_of_range_prices_are_rejected_as_range_errors() {
    let mut non_finite = no_book(0.40, 0.45);
    non_finite.no_bid = f64::NAN;
    assert!(complement_no_to_yes(&non_finite).is_none());

    let mut out_of_range = no_book(0.40, 0.45);
    out_of_range.no_ask = 1.01;
    assert!(complement_no_to_yes(&out_of_range).is_none());
}

#[test]
fn stats_count_inputs_and_each_rejection_reason_honestly() {
    let mut stats = ComplementStats::default();
    let valid = no_book(0.40, 0.45);
    let bad_spread = no_book(0.60, 0.40);
    let mut bad_range = no_book(0.40, 0.45);
    bad_range.no_bid = -0.01;

    assert!(stats.observe(&valid).is_some());
    assert!(stats.observe(&bad_spread).is_none());
    assert!(stats.observe(&bad_range).is_none());

    assert_eq!(stats.n_input, 3);
    assert_eq!(stats.n_ok, 1);
    assert_eq!(stats.n_rejected_bad_spread, 1);
    assert_eq!(stats.n_rejected_range, 1);
}

#[test]
fn output_is_explicitly_labelled_as_synthetic() {
    let output = complement_no_to_yes(&no_book(0.40, 0.45)).expect("valid NO book");

    assert_eq!(output.fidelity, Fidelity::SyntheticComplement);
    assert_eq!(output.fidelity.as_str(), SYNTHETIC_COMPLEMENT_SOURCE);
    assert_eq!(output.source, SYNTHETIC_COMPLEMENT_SOURCE);

    let _: SyntheticComplement = output;
}
