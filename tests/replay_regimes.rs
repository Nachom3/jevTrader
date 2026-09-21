use jevtrader::replay::{
    BasisBucket, FlowBucket, RegimeFeatures, RegimeLabel, RollingRegimeClassifier, SigmaBucket,
    TimeBucket, Trend, Volatility,
};

fn features(
    volatility: f64,
    trend_score: f64,
    minutes_to_resolution: u64,
    distance_sigma: f64,
    ofi: f64,
    basis: f64,
) -> RegimeFeatures {
    RegimeFeatures {
        trend_score,
        volatility,
        minutes_to_resolution,
        distance_sigma,
        ofi,
        basis,
        asset: "BTC".to_owned(),
        horizon: "5m".to_owned(),
    }
}

#[test]
fn empty_history_defaults_volatility_to_normal() {
    let mut classifier = RollingRegimeClassifier::new(4);

    let label = classifier.classify(&features(0.42, 0.0, 100, 0.0, 0.0, 0.0));

    assert_eq!(label.volatility, Volatility::Normal);
}

#[test]
fn rolling_percentiles_progress_and_future_does_not_rewrite_past() {
    let mut classifier = RollingRegimeClassifier::new(4);
    let expected = [
        Volatility::Low,
        Volatility::Normal,
        Volatility::High,
        Volatility::Extreme,
    ];
    for volatility in [1.0, 2.0, 3.0, 4.0] {
        classifier.observe("BTC", "5m", volatility, 0.0);
    }

    let first_label = classifier.classify(&features(1.0, 0.0, 100, 0.0, 0.0, 0.0));
    for (volatility, expected_bucket) in [1.0, 2.0, 3.0, 4.0].into_iter().zip(expected) {
        let label = classifier.classify(&features(volatility, 0.0, 100, 0.0, 0.0, 0.0));
        assert_eq!(label.volatility, expected_bucket);
    }

    classifier.observe("BTC", "5m", 100.0, 0.0);
    let reclassified_first = classifier.classify(&features(1.0, 0.0, 100, 0.0, 0.0, 0.0));

    assert_eq!(reclassified_first, first_label);
}

#[test]
fn trend_uses_fixed_thresholds_including_borders() {
    let mut classifier = RollingRegimeClassifier::new(0);

    assert_eq!(
        classifier
            .classify(&features(0.0, -0.100_001, 100, 0.0, 0.0, 0.0))
            .trend,
        Trend::Bear
    );
    assert_eq!(
        classifier
            .classify(&features(0.0, -0.1, 100, 0.0, 0.0, 0.0))
            .trend,
        Trend::Sideways
    );
    assert_eq!(
        classifier
            .classify(&features(0.0, 0.1, 100, 0.0, 0.0, 0.0))
            .trend,
        Trend::Sideways
    );
    assert_eq!(
        classifier
            .classify(&features(0.0, 0.100_001, 100, 0.0, 0.0, 0.0))
            .trend,
        Trend::Bull
    );
}

#[test]
fn sigma_buckets_cover_six_fixed_levels() {
    let mut classifier = RollingRegimeClassifier::new(0);
    let cases = [
        (-2.01, SigmaBucket::NegBeyond2),
        (-1.5, SigmaBucket::Neg2To1),
        (-0.5, SigmaBucket::Neg1To0),
        (0.0, SigmaBucket::Pos0To1),
        (0.5, SigmaBucket::Pos0To1),
        (1.5, SigmaBucket::Pos1To2),
        (2.01, SigmaBucket::PosBeyond2),
    ];

    for (distance_sigma, expected) in cases {
        let label = classifier.classify(&features(0.0, 0.0, 100, distance_sigma, 0.0, 0.0));
        assert_eq!(label.sigma, expected, "distance_sigma={distance_sigma}");
    }
}

#[test]
fn time_buckets_use_fixed_resolution_windows() {
    let mut classifier = RollingRegimeClassifier::new(0);
    let cases = [
        (300, TimeBucket::Far),
        (100, TimeBucket::Mid),
        (30, TimeBucket::Near),
        (5, TimeBucket::VeryNear),
    ];

    for (minutes, expected) in cases {
        let label = classifier.classify(&features(0.0, 0.0, minutes, 0.0, 0.0, 0.0));
        assert_eq!(label.time, expected, "minutes_to_resolution={minutes}");
    }
}

#[test]
fn label_roundtrip_preserves_numeric_features_and_dimensions() {
    let mut classifier = RollingRegimeClassifier::new(0);
    let input = RegimeFeatures {
        trend_score: 0.123,
        volatility: 0.456,
        minutes_to_resolution: 37,
        distance_sigma: -1.25,
        ofi: 0.4,
        basis: -0.0007,
        asset: "BTC".to_owned(),
        horizon: "5m".to_owned(),
    };

    let label = classifier.classify(&input);
    assert_eq!(label.trend, Trend::Bull);
    assert_eq!(label.flow, FlowBucket::StrongBuy);
    assert_eq!(label.basis, BasisBucket::Negative);

    let encoded = serde_json::to_string(&label).expect("regime label should serialize");
    let decoded: RegimeLabel =
        serde_json::from_str(&encoded).expect("regime label should deserialize");

    assert_eq!(decoded, label);
    assert_eq!(decoded.features.trend_score, 0.123);
    assert_eq!(decoded.features.volatility, 0.456);
    assert_eq!(decoded.features.minutes_to_resolution, 37);
    assert_eq!(decoded.features.distance_sigma, -1.25);
    assert_eq!(decoded.features.ofi, 0.4);
    assert_eq!(decoded.features.basis, -0.0007);
}

#[test]
fn asset_and_horizon_are_propagated_without_silent_normalization() {
    let mut classifier = RollingRegimeClassifier::new(0);
    let mut input = features(0.0, 0.0, 100, 0.0, 0.0, 0.0);
    input.asset = "BTC".to_owned();
    input.horizon = "5m".to_owned();

    let label = classifier.classify(&input);

    assert_eq!(label.asset, "BTC");
    assert_eq!(label.horizon, "5m");
    assert_eq!(label.features.asset, "BTC");
    assert_eq!(label.features.horizon, "5m");
}
