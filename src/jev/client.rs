//! Single-flight Jev/System One HTTP client.

use super::request::{SystemOneRequest, V1State};
use super::response::{JevEvaluation, JevParseError, parse_evaluation_json};
use reqwest::Client;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

const SYSTEM_ONE_URL: &str = "https://api.typesafe.ai/v1/systemone";

static HTTP_CLIENT: OnceLock<Client> = OnceLock::new();

fn http_client() -> &'static Client {
    HTTP_CLIENT.get_or_init(|| {
        Client::builder()
            .build()
            .expect("the shared Jev HTTP client must be constructible")
    })
}

/// Errors returned by a single Jev evaluation.
#[derive(Debug, Error)]
pub enum JevError {
    #[error("Jev request transport error: {0}")]
    Transport(#[source] reqwest::Error),
    #[error("Jev request exceeded its deadline")]
    Deadline,
    #[error("Jev returned HTTP status {status}: {body}")]
    Status {
        status: reqwest::StatusCode,
        body: String,
    },
    #[error("Jev response parse error: {0}")]
    Parse(#[source] JevParseError),
}

/// Evaluate one state with one request and no signal retry.
///
/// The timeout is applied to the complete request, including response-body
/// consumption. A timed-out signal is deliberately lost rather than retried:
/// a late signal is worse than a lost one because its originating market state
/// may no longer be current when it reaches the strategy.
pub async fn evaluate(
    state: &V1State,
    state_seq: u64,
    market_id: &str,
    api_key: &str,
    deadline: Duration,
) -> Result<JevEvaluation, JevError> {
    let candidate_buy_price = state.candidate_order.price;
    let questions = crate::jev::request::QuestionSet::V1.build(candidate_buy_price);
    let (body, sent_at_ms, received_at_ms) =
        post(state, &questions, api_key, deadline).await?;

    parse_evaluation_json(&body, market_id, state_seq, sent_at_ms, received_at_ms)
        .map_err(JevError::Parse)
}

/// Transport-only POST: sends one state with an arm-specific question set
/// and returns the raw body plus timestamps. Parsing is the caller's job,
/// so V1 and V3 arms share transport without sharing validation.
pub async fn post(
    state: &V1State,
    questions: &serde_json::Value,
    api_key: &str,
    deadline: Duration,
) -> Result<(Vec<u8>, i64, i64), JevError> {
    let request = SystemOneRequest::with_questions(state.clone(), questions.clone());
    let sent_at_ms = unix_time_ms();

    let response = http_client()
        .post(SYSTEM_ONE_URL)
        .bearer_auth(api_key)
        .timeout(deadline)
        .json(&request)
        .send()
        .await
        .map_err(classify_request_error)?;

    let status = response.status();
    let body = response.bytes().await.map_err(classify_request_error)?;
    let received_at_ms = unix_time_ms();

    if !status.is_success() {
        return Err(JevError::Status {
            status,
            body: String::from_utf8_lossy(&body).into_owned(),
        });
    }

    Ok((body.to_vec(), sent_at_ms, received_at_ms))
}

fn classify_request_error(error: reqwest::Error) -> JevError {
    if error.is_timeout() {
        JevError::Deadline
    } else {
        JevError::Transport(error)
    }
}

fn unix_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
