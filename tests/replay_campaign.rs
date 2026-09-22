use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use jevtrader::replay::resolution::resolve_market_split;
use jevtrader::replay::{
    Arm, CampaignConfig, FillProfile, HistoricalEvent, JevCaller, Provenance, ResolutionOutcome,
    ResolutionSpec, ResolvedMarket, run_episode_campaign_with_caller,
};

const CONDITION: &str = "condition-1";
const MARKET: &str = "market-1";

fn campaign_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "jevtrader-replay-campaign-{label}-{}",
        std::process::id()
    ))
}

fn resolved_market() -> ResolvedMarket {
    let spec = ResolutionSpec {
        condition_id: CONDITION.to_owned(),
        market_id: MARKET.to_owned(),
        asset: "BTC".to_owned(),
        horizon: "5m".to_owned(),
        resolution_source: "official source".to_owned(),
        resolution_rule_excerpt: "resolves YES when the observed target is reached".to_owned(),
        fidelity: jevtrader::replay::Fidelity::Exact,
        resolution_at_ms: 10_000,
        reference: None,
        strike: None,
        start_at: None,
        end_at: None,
    };
    resolve_market_split(
        &spec,
        Some(ResolutionOutcome::Yes),
        Provenance::Exact,
        Provenance::Exact,
        false,
        false,
    )
    .expect("fixture resolution is valid")
}

fn fixture_events() -> Vec<HistoricalEvent> {
    vec![
        HistoricalEvent::PolyTop {
            ts_ms: 1_000,
            condition_id: CONDITION.to_owned(),
            best_bid: 0.40,
            best_ask: 0.50,
            source: "fixture".to_owned(),
        },
        HistoricalEvent::PolyTrade {
            ts_ms: 2_000,
            condition_id: CONDITION.to_owned(),
            price: 0.45,
            size: 1.0,
            aggressor: Some("BUY".to_owned()),
            direction_quality: "EXACT".to_owned(),
            source: "fixture".to_owned(),
        },
        HistoricalEvent::PolyTrade {
            ts_ms: 2_500,
            condition_id: CONDITION.to_owned(),
            price: 0.40,
            size: 100.0,
            aggressor: Some("SELL".to_owned()),
            direction_quality: "EXACT".to_owned(),
            source: "fixture".to_owned(),
        },
        HistoricalEvent::PolyTrade {
            ts_ms: 3_000,
            condition_id: CONDITION.to_owned(),
            price: 0.39,
            size: 100.0,
            aggressor: Some("SELL".to_owned()),
            direction_quality: "EXACT".to_owned(),
            source: "fixture".to_owned(),
        },
    ]
}

fn questions() -> HashMap<String, (String, String)> {
    HashMap::from([(
        MARKET.to_owned(),
        (
            "Will BTC reach the target?".to_owned(),
            "The official source determines the outcome.".to_owned(),
        ),
    )])
}

fn v1_envelope() -> String {
    serde_json::json!({
        "answers": {
            "yes_pressure_5s": {"type": "noul", "noul": 0.95},
            "no_pressure_5s": {"type": "noul", "noul": 0.05},
            "move_persists": {"type": "noul", "noul": 0.95},
            "underreact_up": {"type": "noul", "noul": 0.95},
            "underreact_down": {"type": "noul", "noul": 0.05},
            "repricing_ticks": {
                "type": "choice",
                "probabilities": {
                    "UP_3_PLUS_TICKS": 0.10,
                    "UP_2_TICKS": 0.15,
                    "UP_1_TICK": 0.60,
                    "FLAT": 0.05,
                    "DOWN_1_TICK": 0.04,
                    "DOWN_2_TICKS": 0.03,
                    "DOWN_3_PLUS_TICKS": 0.03
                },
                "confidence": 0.90
            },
            "fill_before_decay": {"type": "noul", "noul": 0.95},
            "fill_toxic": {"type": "noul", "noul": 0.05}
        }
    })
    .to_string()
}

#[derive(Clone)]
struct LocalStub {
    calls: Arc<AtomicUsize>,
}

impl LocalStub {
    fn new() -> (Self, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Self {
                calls: Arc::clone(&calls),
            },
            calls,
        )
    }
}

impl JevCaller for LocalStub {
    fn call(
        &mut self,
        _state_json: &str,
        _questions_json: &str,
    ) -> Result<(String, u64, bool), String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok((v1_envelope(), 7, false))
    }
}

fn base_config(dir: &str) -> CampaignConfig {
    let mut config =
        CampaignConfig::smoke("campaign-test", campaign_dir(dir).display().to_string());
    config.arms = vec![Arm::JevOnly];
    config.fill_profile = FillProfile::Base;
    config.per_condition_signals = 1;
    config.max_jev_calls = 10;
    config
}

#[test]
fn campaign_is_byte_deterministic_and_persistent_cache_hits() {
    let config = base_config("cache");
    let resolutions = HashMap::from([(CONDITION.to_owned(), resolved_market())]);
    let questions = questions();

    let (stub, calls) = LocalStub::new();
    let first = run_episode_campaign_with_caller(
        &config,
        &fixture_events(),
        &resolutions,
        &questions,
        stub,
    )
    .expect("first campaign succeeds");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(first.jev_calls, 1);
    assert_eq!(first.jev_misses, 1);

    let (stub, calls) = LocalStub::new();
    let second = run_episode_campaign_with_caller(
        &config,
        &fixture_events(),
        &resolutions,
        &questions,
        stub,
    )
    .expect("cached campaign succeeds");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(second.jev_calls, 0);
    assert!(second.jev_hits > 0);
    assert_eq!(second.jev_misses, 0);
    assert_eq!(
        serde_json::to_vec(&first.episodes).expect("first episodes serialize"),
        serde_json::to_vec(&second.episodes).expect("second episodes serialize")
    );
}

#[test]
fn quant_only_never_calls_jev() {
    let mut config = base_config("quant-only");
    config.arms = vec![Arm::QuantOnly];
    let (stub, calls) = LocalStub::new();
    let output = run_episode_campaign_with_caller(
        &config,
        &fixture_events(),
        &HashMap::from([(CONDITION.to_owned(), resolved_market())]),
        &questions(),
        stub,
    )
    .expect("quant-only campaign succeeds");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(output.jev_calls, 0);
    assert_eq!(output.jev_hits, 0);
    assert_eq!(output.jev_misses, 0);
}

#[test]
fn no_prints_never_calls_jev() {
    let config = base_config("no-prints");
    let stream: Vec<HistoricalEvent> = Vec::new();
    let (stub, calls) = LocalStub::new();
    let output = run_episode_campaign_with_caller(
        &config,
        &stream,
        &HashMap::from([(CONDITION.to_owned(), resolved_market())]),
        &questions(),
        stub,
    )
    .expect("empty campaign succeeds");

    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(output.jev_calls, 0);
    assert_eq!(output.jev_hits, 0);
    assert_eq!(output.jev_misses, 0);
    assert_eq!(output.episodes.len(), 0);

    let cache_path = PathBuf::from(&config.cache_dir).join("jev_cache.json");
    let cache: Vec<serde_json::Value> = serde_json::from_str(
        &std::fs::read_to_string(cache_path).expect("campaign writes an empty cache"),
    )
    .expect("empty cache is valid JSON");
    assert_eq!(cache.len(), 0);
}

#[test]
fn print_touch_quotes_without_book() {
    let mut config = base_config("print-touch");
    config.per_condition_signals = 2;
    let stream = vec![
        HistoricalEvent::PolyTrade {
            ts_ms: 1_000,
            condition_id: CONDITION.to_owned(),
            price: 0.45,
            size: 1.0,
            aggressor: Some("BUY".to_owned()),
            direction_quality: "EXACT".to_owned(),
            source: "fixture".to_owned(),
        },
        HistoricalEvent::PolyTrade {
            ts_ms: 1_500,
            condition_id: CONDITION.to_owned(),
            price: 0.40,
            size: 1.0,
            aggressor: Some("SELL".to_owned()),
            direction_quality: "EXACT".to_owned(),
            source: "fixture".to_owned(),
        },
        HistoricalEvent::PolyTrade {
            ts_ms: 2_000,
            condition_id: CONDITION.to_owned(),
            price: 0.39,
            size: 100.0,
            aggressor: Some("SELL".to_owned()),
            direction_quality: "EXACT".to_owned(),
            source: "fixture".to_owned(),
        },
    ];
    let resolutions = HashMap::from([(CONDITION.to_owned(), resolved_market())]);
    let questions = questions();
    let (stub, calls) = LocalStub::new();
    let output = run_episode_campaign_with_caller(&config, &stream, &resolutions, &questions, stub)
        .expect("print-touch campaign succeeds");

    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(output.jev_calls, 2);
    assert_eq!(output.episodes.len(), 1);
    let episode = &output.episodes[0];
    assert!((episode.limit_price - 0.41).abs() < 1e-9);
    assert!(episode.order_arrival_ts_ms >= 1_550);
    if let Some(fill_ts_ms) = episode.fill_ts_ms {
        assert!(fill_ts_ms >= episode.order_arrival_ts_ms);
        assert!(fill_ts_ms <= resolved_market().resolved_at_ms);
        assert!(episode.fill_qty.is_some_and(|quantity| quantity > 0.0));
    }

    let (stub, second_calls) = LocalStub::new();
    let second = run_episode_campaign_with_caller(&config, &stream, &resolutions, &questions, stub)
        .expect("cached print-touch campaign succeeds");
    assert_eq!(second_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        serde_json::to_vec(&output.episodes).expect("first episodes serialize"),
        serde_json::to_vec(&second.episodes).expect("second episodes serialize")
    );
}

#[test]
fn exhausted_budget_skips_without_panicking() {
    let mut config = base_config("budget");
    config.arms = vec![Arm::JevOnly, Arm::QuantPlusJev];
    config.max_jev_calls = 1;
    let (stub, calls) = LocalStub::new();
    let output = run_episode_campaign_with_caller(
        &config,
        &fixture_events(),
        &HashMap::from([(CONDITION.to_owned(), resolved_market())]),
        &questions(),
        stub,
    )
    .expect("budget exhaustion is a conservative skip");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(output.jev_calls, 1);
    assert!(output.skipped > 0);
}
