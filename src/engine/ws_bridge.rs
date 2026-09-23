//! Task adapter for running Polymarket's public market stream on Tokio.

use tracing::{info, warn};

use crate::domain::TokenId;
use crate::engine::market_actor::MarketMessage;
use crate::polymarket::ws::{MarketStreamClient, WsError};

/// Runs the configured market stream into the engine's message channel.
///
/// Returns promptly if the receiver is dropped, even before the first market
/// event arrives. Reconnection and retry limits remain owned by
/// [`MarketStreamClient`].
pub async fn run_market_ws(
    client: MarketStreamClient,
    sender: tokio::sync::mpsc::Sender<MarketMessage>,
) -> Result<(), WsError> {
    info!(
        yes_token_id = %redacted_token_id(client.yes_token_id()),
        no_token_id = %redacted_token_id(client.no_token_id()),
        "starting Polymarket market WebSocket bridge"
    );

    let result = if sender.is_closed() {
        Err(WsError::ConsumerDropped)
    } else {
        tokio::select! {
            result = client.run(sender.clone()) => result,
            () = sender.closed() => Err(WsError::ConsumerDropped),
        }
    };
    if let Err(error) = &result {
        warn!(error = %error, "Polymarket market WebSocket bridge exited with an error");
    }
    result
}

/// Spawns [`run_market_ws`] on the current Tokio runtime.
pub fn spawn_market_ws(
    client: MarketStreamClient,
    sender: tokio::sync::mpsc::Sender<MarketMessage>,
) -> tokio::task::JoinHandle<Result<(), WsError>> {
    tokio::spawn(run_market_ws(client, sender))
}

fn redacted_token_id(token_id: &TokenId) -> String {
    let chars: Vec<char> = token_id.0.chars().collect();
    let suffix: String = chars[chars.len().saturating_sub(6)..].iter().collect();
    format!("***{suffix}")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;

    use crate::domain::TokenId;
    use crate::polymarket::ws::{MarketStreamClient, WsError};

    use super::spawn_market_ws;

    const YES_TOKEN_ID: &str = "12345678901234567890";
    const NO_TOKEN_ID: &str = "98765432109876543210";

    fn client() -> MarketStreamClient {
        MarketStreamClient::new(
            TokenId(YES_TOKEN_ID.to_owned()),
            TokenId(NO_TOKEN_ID.to_owned()),
        )
        .expect("dummy numeric token IDs should construct without network access")
    }

    #[test]
    fn market_stream_client_exposes_yes_and_no_token_ids() {
        let client = client();

        assert_eq!(client.yes_token_id().0, YES_TOKEN_ID);
        assert_eq!(client.no_token_id().0, NO_TOKEN_ID);
    }

    #[tokio::test]
    async fn spawned_bridge_returns_error_after_receiver_is_dropped() {
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);

        let task = spawn_market_ws(client(), sender);
        let task_result = tokio::time::timeout(Duration::from_secs(15), task)
            .await
            .expect("market WebSocket bridge did not terminate within 15 seconds")
            .expect("market WebSocket bridge task panicked");

        let termination_path = match &task_result {
            Err(WsError::ConsumerDropped) => "consumer dropped",
            Err(WsError::Sdk(_)) => "SDK subscription or connection error",
            Err(WsError::ConnectionClosed) => "connection closed",
            Err(WsError::ReconnectExhausted { .. }) => "reconnect budget exhausted",
            Err(_) => "other WebSocket error",
            Ok(()) => "unexpected successful completion",
        };
        let assertion_message =
            format!("receiver-drop termination path: {termination_path}; result={task_result:?}");
        println!("{assertion_message}");
        assert!(task_result.is_err(), "{assertion_message}");
    }
}
