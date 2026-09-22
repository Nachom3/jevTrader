//! Single-flight Jev/System One HTTP client.

use super::request::{SystemOneRequest, V1State};
use super::response::{JevEvaluation, JevParseError, parse_evaluation_json};
use reqwest::Client;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Maximum number of HTTP attempts allowed by the offline precompute path.
pub const PRECOMPUTE_MAX_ATTEMPTS: u32 = 4;
const PRECOMPUTE_BACKOFF_BASE: Duration = Duration::from_millis(50);
const PRECOMPUTE_BACKOFF_MAX: Duration = Duration::from_secs(2);
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
    let (body, sent_at_ms, received_at_ms) = post(state, &questions, api_key, deadline).await?;

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

/// One attempt made by the precompute-only retry path.
#[derive(Debug, Clone)]
pub struct PrecomputeAttempt {
    pub duration_ms: u64,
    pub error: Option<String>,
}

/// Result of a precompute request, including every retry's audit metadata.
#[derive(Debug, Clone)]
pub struct PrecomputeOutcome {
    pub body: Option<Vec<u8>>,
    pub sent_at_ms: i64,
    pub received_at_ms: i64,
    pub attempt_count: u32,
    pub attempts: Vec<PrecomputeAttempt>,
    pub observed_latency_ms: u64,
    pub total_duration_ms: u64,
    pub final_error: Option<String>,
}

impl PrecomputeOutcome {
    fn failure(
        attempt_count: u32,
        attempts: Vec<PrecomputeAttempt>,
        total_duration_ms: u64,
        error: String,
    ) -> Self {
        Self {
            body: None,
            sent_at_ms: 0,
            received_at_ms: 0,
            attempt_count,
            attempts,
            observed_latency_ms: total_duration_ms,
            total_duration_ms,
            final_error: Some(error),
        }
    }
}

/// Precompute-only Jev POST with bounded retry for HTTP 429 and 5xx.
///
/// This helper is intentionally separate from [`evaluate`] and [`post`]. The
/// live strategy path keeps its deadline/no-retry semantics; offline phase 1
/// may spend a bounded amount of time recovering transient provider pressure.
/// Every attempt is timed and the returned object is suitable for cache and
/// parquet audit fields.
pub async fn evaluate_precompute(
    state: &V1State,
    questions: &serde_json::Value,
    api_key: &str,
    deadline: Duration,
    max_attempts: u32,
) -> PrecomputeOutcome {
    let attempts_limit = max_attempts.clamp(1, PRECOMPUTE_MAX_ATTEMPTS);
    let started = Instant::now();
    let mut attempts = Vec::with_capacity(attempts_limit as usize);

    for attempt in 1..=attempts_limit {
        let attempt_started = Instant::now();
        let result = post(state, questions, api_key, deadline).await;
        let duration_ms = attempt_started.elapsed().as_millis().min(u64::MAX as u128) as u64;
        match result {
            Ok((body, sent_at_ms, received_at_ms)) => {
                attempts.push(PrecomputeAttempt {
                    duration_ms,
                    error: None,
                });
                return PrecomputeOutcome {
                    body: Some(body),
                    sent_at_ms,
                    received_at_ms,
                    attempt_count: attempt,
                    attempts,
                    observed_latency_ms: received_at_ms
                        .saturating_sub(sent_at_ms)
                        .max(0) as u64,
                    total_duration_ms: started.elapsed().as_millis().min(u64::MAX as u128)
                        as u64,
                    final_error: None,
                };
            }
            Err(error) => {
                let retryable = is_precompute_retryable(&error);
                let error_text = error.to_string();
                attempts.push(PrecomputeAttempt {
                    duration_ms,
                    error: Some(error_text.clone()),
                });
                if !retryable || attempt == attempts_limit {
                    return PrecomputeOutcome::failure(
                        attempt,
                        attempts,
                        started.elapsed().as_millis().min(u64::MAX as u128) as u64,
                        error_text,
                    );
                }
                tokio::time::sleep(precompute_retry_delay(attempt)).await;
            }
        }
    }

    unreachable!("the bounded precompute attempt loop always returns")
}

/// Returns whether an error is eligible for the offline retry policy.
#[must_use]
pub fn is_precompute_retryable(error: &JevError) -> bool {
    matches!(
        error,
        JevError::Status { status, .. } if status.as_u16() == 429 || status.is_server_error()
    )
}

/// Exponential backoff with a bounded, time-derived jitter component.
#[must_use]
pub fn precompute_retry_delay(attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(10);
    let multiplier = 1_u32 << exponent;
    let base_ms = PRECOMPUTE_BACKOFF_BASE
        .as_millis()
        .saturating_mul(multiplier as u128)
        .min(PRECOMPUTE_BACKOFF_MAX.as_millis()) as u64;
    let jitter_window = (base_ms / 4).max(1);
    let jitter = (unix_time_ms().unsigned_abs() % (jitter_window + 1)) as u64;
    Duration::from_millis(
        base_ms
            .saturating_add(jitter)
            .min(PRECOMPUTE_BACKOFF_MAX.as_millis() as u64),
    )
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
