use std::{net::SocketAddr, time::Duration};

use chrono::{DateTime, Utc};
use jevtrader::{
    domain::{Asset, Horizon},
    polymarket::discover_live_specs,
};
use polymarket_client_sdk_v2::gamma::Client as GammaClient;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
    time::timeout,
};

async fn gamma_server<F>(handler: F) -> (GammaClient, JoinHandle<Vec<String>>)
where
    F: Fn(&str) -> Option<Value> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind local Gamma fixture server");
    let address = listener.local_addr().expect("read fixture server address");
    let server = tokio::spawn(async move {
        let mut requested_slugs = Vec::new();
        loop {
            let Ok(Ok((mut stream, _))) =
                timeout(Duration::from_millis(150), listener.accept()).await
            else {
                break;
            };
            let mut request = [0_u8; 4096];
            let read = stream.read(&mut request).await.expect("read Gamma request");
            let request = String::from_utf8_lossy(&request[..read]);
            let path = request
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .unwrap_or_default();
            let slug = path.rsplit('/').next().unwrap_or_default().to_owned();
            requested_slugs.push(slug.clone());

            let (status, body) = match handler(&slug) {
                Some(body) => (
                    "200 OK",
                    serde_json::to_vec(&body).expect("serialize Gamma fixture"),
                ),
                None => ("404 Not Found", Vec::new()),
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write Gamma response headers");
            stream
                .write_all(&body)
                .await
                .expect("write Gamma response body");
        }
        requested_slugs
    });
    (gamma_client(address), server)
}

fn gamma_client(address: SocketAddr) -> GammaClient {
    GammaClient::new(&format!("http://{address}/")).expect("create local Gamma client")
}

fn epoch_market(slug: &str) -> Option<Value> {
    let parts = slug.split('-').collect::<Vec<_>>();
    if parts.len() != 4 || parts[1] != "updown" {
        return None;
    }
    let horizon_secs = match parts[2] {
        "5m" => 300,
        "15m" => 900,
        "4h" => 14_400,
        _ => return None,
    };
    let boundary = parts[3].parse::<i64>().ok()?;
    let end_date = DateTime::<Utc>::from_timestamp(boundary + horizon_secs, 0)?.to_rfc3339();
    Some(live_market(slug, &end_date, json!({"priceToBeat": 66_000})))
}

fn live_market(slug: &str, end_date: &str, event_metadata: Value) -> Value {
    json!({
        "id": slug,
        "slug": slug,
        "question": format!("Will {slug} resolve up?"),
        "description": format!("Resolution rules for {slug}"),
        "resolutionSource": "https://data.binance.com/api/v3/klines",
        "endDate": end_date,
        "active": true,
        "closed": false,
        "acceptingOrders": true,
        "enableOrderBook": true,
        "eventMetadata": event_metadata
    })
}

#[tokio::test]
async fn selects_latest_open_market_per_epoch_group_and_maps_fields() {
    let (gamma, server) = gamma_server(epoch_market).await;

    let specs = discover_live_specs(&gamma)
        .await
        .expect("discover valid BTC and ETH markets");
    let requested_slugs = server.await.expect("finish Gamma slug probes");

    assert!((14..=16).contains(&requested_slugs.len()));
    assert_eq!(
        requested_slugs
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        requested_slugs.len()
    );
    assert_eq!(specs.len(), 6);
    for spec in &specs {
        assert!(matches!(spec.asset, Some(Asset::Btc | Asset::Eth)));
        assert!(matches!(
            spec.horizon,
            Some(Horizon::M5 | Horizon::M15 | Horizon::H4)
        ));
        assert_eq!(spec.target, 66_000.0);
        assert!(
            spec.slug
                .rsplit('-')
                .next()
                .is_some_and(|epoch| epoch.parse::<i64>().is_ok())
        );
        assert_eq!(
            spec.reference_source.as_deref(),
            Some(spec.resolution_source.as_str())
        );
        assert_eq!(
            spec.window_secs,
            Some(spec.horizon.expect("explicit horizon").seconds())
        );
    }
    assert!(
        requested_slugs
            .iter()
            .any(|slug| slug.contains("bitcoin-up-or-down-"))
    );
    assert!(
        requested_slugs
            .iter()
            .any(|slug| slug.contains("ethereum-up-or-down-"))
    );

    for asset_prefix in ["btc", "eth"] {
        for horizon in ["5m", "15m", "4h"] {
            let selected = specs
                .iter()
                .find(|spec| {
                    spec.slug
                        .starts_with(&format!("{asset_prefix}-updown-{horizon}-"))
                })
                .expect("one latest market per asset/horizon group");
            let expected_latest = requested_slugs
                .iter()
                .filter(|slug| slug.starts_with(&format!("{asset_prefix}-updown-{horizon}-")))
                .filter_map(|slug| slug.rsplit('-').next()?.parse::<i64>().ok())
                .max()
                .expect("two epoch variants were probed");
            let selected_epoch = selected
                .slug
                .rsplit('-')
                .next()
                .expect("selected epoch suffix")
                .parse::<i64>()
                .expect("numeric selected epoch");
            assert_eq!(selected_epoch, expected_latest);
        }
    }
}

#[tokio::test]
async fn applies_market_filters_and_skips_missing_targets() {
    let (gamma, server) = gamma_server(|slug| {
        let mut market = epoch_market(slug)?;
        if slug.starts_with("btc-updown-5m-") {
            market["active"] = json!(false);
        } else if slug.starts_with("eth-updown-5m-") {
            market["closed"] = json!(true);
        } else if slug.starts_with("btc-updown-15m-") {
            market["acceptingOrders"] = json!(false);
        } else if slug.starts_with("eth-updown-15m-") {
            market["enableOrderBook"] = json!(false);
        } else if slug.starts_with("btc-updown-4h-") {
            market["eventMetadata"] = json!({});
        } else {
            return Some(market);
        }
        Some(market)
    })
    .await;

    let specs = discover_live_specs(&gamma)
        .await
        .expect("ETH 4h markets should qualify");
    let _requested_slugs = server.await.expect("finish Gamma slug probes");

    assert_eq!(specs.len(), 1);
    assert!(specs[0].slug.starts_with("eth-updown-4h-"));
    assert_eq!(specs[0].asset, Some(Asset::Eth));
    assert_eq!(specs[0].horizon, Some(Horizon::H4));
}

#[tokio::test]
async fn zero_qualifying_markets_reports_probe_misses_and_skip_counts() {
    let (gamma, server) = gamma_server(|_| None).await;

    let error = discover_live_specs(&gamma)
        .await
        .expect_err("all missing slugs should fail discovery");
    let requested_slugs = server.await.expect("finish Gamma slug probes");

    let message = error.to_string();
    assert!((14..=16).contains(&requested_slugs.len()));
    assert!(message.contains(&format!("probed {} slugs", requested_slugs.len())));
    assert!(message.contains(&format!("slug_miss={}", requested_slugs.len())));
    assert!(message.contains("no live BTC/ETH up-down markets qualified"));
}
