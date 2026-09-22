use jevtrader::replay::{
    BlockBootstrap, EconomySummary, MarketSpan, OosReport, SplitAssign, TemporalWindow,
    embargo_flags, oos_report_with_draws, plan_windows, purge_train, requires_gap_ms,
    summarize_distribution,
};

#[test]
fn planner_emits_contiguous_named_windows_and_rejects_invalid_cuts() {
    let windows = plan_windows(1_000, 1_300, 100, 50, 150);
    assert_eq!(windows.len(), 2);
    assert_eq!(windows[0].name, "w00");
    assert_eq!(windows[1].name, "w01");
    assert_eq!(
        (windows[0].train_start_ms, windows[0].train_end_ms),
        (1_000, 1_100)
    );
    assert_eq!(
        (windows[0].test_start_ms, windows[0].test_end_ms),
        (1_100, 1_150)
    );
    assert_eq!(
        (windows[1].train_start_ms, windows[1].train_end_ms),
        (1_150, 1_250)
    );
    assert_eq!(
        (windows[1].test_start_ms, windows[1].test_end_ms),
        (1_250, 1_300)
    );
    assert_eq!(windows[0].assign(1_050), Some(SplitAssign::InSample));
    assert_eq!(windows[0].assign(1_125), Some(SplitAssign::OutOfSample));

    assert!(plan_windows(1_000, 1_300, 0, 50, 50).is_empty());
    assert!(TemporalWindow::new("bad", 1_100, 1_000, 1_200, 1_300).is_err());
}

#[test]
fn purge_uses_the_pre_cut_train_zone() {
    let markets = vec![
        MarketSpan::new("late", 700, 950).unwrap(),
        MarketSpan::new("boundary", 600, 900).unwrap(),
        MarketSpan::new("clean", 400, 800).unwrap(),
    ];
    // Docstring example: train_end=1_000, purge=100 => cutoff=900.
    let (kept, dropped) = purge_train(&markets, 1_000, 100);
    assert_eq!(
        kept.iter()
            .map(|m| m.condition_id.as_str())
            .collect::<Vec<_>>(),
        ["boundary", "clean"]
    );
    assert_eq!(dropped[0].condition_id, "late");
}

#[test]
fn consecutive_windows_mark_a_short_embargo_gap() {
    let windows = plan_windows(1_000, 1_400, 100, 100, 200);
    assert_eq!(windows.len(), 2);
    assert!(requires_gap_ms(
        windows[0].test_end_ms,
        windows[1].train_start_ms,
        1
    ));
    assert_eq!(embargo_flags(&windows, 1), vec![true]);
}

#[test]
fn block_bootstrap_is_seeded_and_keeps_block_indexes_whole() {
    let blocks = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
    let first = BlockBootstrap::new(blocks.clone(), 7).resample(8);
    assert_eq!(first, BlockBootstrap::new(blocks.clone(), 7).resample(8));
    assert_ne!(first, BlockBootstrap::new(blocks, 8).resample(8));
    assert!(first.iter().flatten().all(|block_index| *block_index < 3));

    let summary = summarize_distribution(&[-2.0, -1.0, 1.0, 2.0]);
    assert!(summary.ci95_low <= summary.mean);
    assert!(summary.ci95_high >= summary.mean);
    assert!((0.0..=1.0).contains(&summary.p_positive));
    assert_eq!(summary.median, 0.0);
}

#[test]
fn oos_headline_reports_net_metrics_and_keeps_namespaces_separate() {
    let episodes = vec![("market-a".to_owned(), 2.0), ("market-b".to_owned(), -1.0)];
    let equity = [0.0, 2.0, 1.0, -1.0];
    let headline = oos_report_with_draws(
        &episodes,
        2,
        1,
        Some(10.0),
        Some(&equity),
        Some(3.0),
        Some(12.0),
        100,
        99,
    );
    assert_eq!(headline.net_pnl_usd, 1.0);
    assert_eq!(headline.pnl_per_trade, 0.5);
    assert_eq!(headline.roi, Some(0.1));
    assert_eq!(headline.max_drawdown_usd, Some(3.0));
    assert_eq!(headline.profit_factor, Some(2.0));
    assert_eq!(headline.fills, 2);
    assert_eq!(headline.nofills, 1);
    assert!((0.0..=1.0).contains(&headline.p_positive));

    let in_sample = EconomySummary {
        n_episodes: 10,
        ..EconomySummary::default()
    };
    let out_of_sample = EconomySummary {
        n_episodes: 2,
        ..EconomySummary::default()
    };
    let report = OosReport::new(in_sample, out_of_sample, headline);
    assert_eq!(report.in_sample.n_episodes, 10);
    assert_eq!(report.out_of_sample.n_episodes, 2);
    assert_ne!(report.in_sample.n_episodes, report.out_of_sample.n_episodes);
}
