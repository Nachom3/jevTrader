use jevtrader::polymarket::{MarketMetadata, fetch_market_by_slug};
use polymarket_client_sdk_v2::gamma::Client as GammaClient;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const CONDITION_ID: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";

async fn fetch_market_with_outcomes(outcomes: &[&str]) -> MarketMetadata {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock Gamma server");
    let address = listener.local_addr().expect("read mock Gamma address");
    let outcomes_json = serde_json::to_string(outcomes).expect("serialize outcomes");
    let token_ids_json = serde_json::to_string(&["101", "202"]).expect("serialize token IDs");
    let body = serde_json::json!({
        "id": "market-1",
        "conditionId": CONDITION_ID,
        "outcomes": outcomes_json,
        "clobTokenIds": token_ids_json,
    })
    .to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept mock Gamma request");
        let mut request = [0_u8; 4096];
        let _bytes_read = stream
            .read(&mut request)
            .await
            .expect("read mock Gamma request");
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write mock Gamma response");
    });

    let client = GammaClient::new(&format!("http://{address}")).expect("create mock Gamma client");
    let metadata = fetch_market_by_slug(&client, "market-1")
        .await
        .expect("fetch market metadata");
    server.await.expect("join mock Gamma server");
    metadata
}

#[tokio::test]
async fn fetch_market_by_slug_maps_up_down_outcomes_to_yes_and_no_tokens() {
    let metadata = fetch_market_with_outcomes(&["Up", "Down"]).await;

    assert_eq!(metadata.yes_token_id.0, "101");
    assert_eq!(metadata.no_token_id.0, "202");
}

#[tokio::test]
async fn fetch_market_by_slug_keeps_yes_no_outcome_mapping_case_insensitive() {
    let metadata = fetch_market_with_outcomes(&["YES", "No"]).await;

    assert_eq!(metadata.yes_token_id.0, "101");
    assert_eq!(metadata.no_token_id.0, "202");
}
