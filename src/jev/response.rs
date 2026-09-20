//! Typed response contracts and validation for Jev/System One V1 answers.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The seven possible `repricing_ticks` Choice buckets.
pub const REPRICING_BUCKETS: [&str; 7] = [
    "UP_3_PLUS_TICKS",
    "UP_2_TICKS",
    "UP_1_TICK",
    "FLAT",
    "DOWN_1_TICK",
    "DOWN_2_TICKS",
    "DOWN_3_PLUS_TICKS",
];

/// Raw System One response envelope.
#[derive(Debug, Clone, Deserialize)]
pub struct SystemOneResponse {
    pub answers: std::collections::BTreeMap<String, RawAnswer>,
    /// Token usage reported by the API. Absent on older payloads and in
    /// synthetic fixtures; defaults to zero and never fails validation.
    #[serde(default)]
    pub usage: Option<ResponseUsage>,
}

/// Token usage accompanying a System One response.
///
/// Field aliases cover the documented `input_tokens`/`output_tokens` shape
/// plus common provider spellings; unknown shapes keep the zero default
/// rather than rejecting an otherwise valid evaluation.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ResponseUsage {
    #[serde(default, alias = "prompt_tokens", alias = "inputTokens")]
    pub input_tokens: u64,
    #[serde(default, alias = "completion_tokens", alias = "outputTokens")]
    pub output_tokens: u64,
}

/// Raw answer fields returned by the System One API.
///
/// The API includes `type`; keeping it optional also accepts the compact answer
/// representation while validation still checks every field needed by V1.
#[derive(Debug, Clone, Deserialize)]
pub struct RawAnswer {
    #[serde(rename = "type")]
    pub answer_type: Option<String>,
    pub noul: Option<f64>,
    pub choice: Option<String>,
    pub probabilities: Option<std::collections::BTreeMap<String, f64>>,
    pub confidence: Option<f64>,
}

/// A validated probability distribution over repricing buckets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickDistribution {
    pub up_3_plus: f64,
    pub up_2: f64,
    pub up_1: f64,
    pub flat: f64,
    pub down_1: f64,
    pub down_2: f64,
    pub down_3_plus: f64,
}

/// A validated set of the eight V1 answers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V1Signal {
    pub yes_pressure_5s: f64,
    pub no_pressure_5s: f64,
    pub move_persists: f64,
    pub underreact_up: f64,
    pub underreact_down: f64,
    pub repricing: TickDistribution,
    pub repricing_confidence: f64,
    pub fill_before_decay: f64,
    pub fill_toxic: f64,
}

impl V1Signal {
    /// P(YES rises by >= 1 tick in 5s), straight from the distribution.
    pub fn p_up_ge_1_tick(&self) -> f64 {
        self.repricing.up_1 + self.repricing.up_2 + self.repricing.up_3_plus
    }
}

/// A validated signal together with the identity and timing of its source state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JevEvaluation {
    pub market_id: String,
    pub state_seq: u64,
    pub sent_at_ms: i64,
    pub received_at_ms: i64,
    pub latency_ms: u64,
    pub signal: V1Signal,
    /// Measured API token usage; zero when the payload omits `usage`
    /// (older payloads, synthetic evaluations in tests).
    pub tokens_in: u64,
    pub tokens_out: u64,
}

/// Any malformed or semantically invalid System One answer.
#[derive(Debug, Error)]
pub enum JevParseError {
    #[error("invalid JSON response: {0}")]
    Json(#[from] serde_json::Error),
    #[error("missing answer `{0}`")]
    MissingAnswer(&'static str),
    #[error("answer `{answer}` has type `{actual}`, expected `{expected}`")]
    UnexpectedAnswerType {
        answer: &'static str,
        expected: &'static str,
        actual: String,
    },
    #[error("answer `{answer}` is missing field `{field}`")]
    MissingField {
        answer: &'static str,
        field: &'static str,
    },
    #[error("probability `{answer}` must be finite and in [0, 1], got {value}")]
    InvalidProbability { answer: &'static str, value: f64 },
    #[error("repricing probability `{bucket}` must be finite and in [0, 1], got {value}")]
    InvalidBucketProbability { bucket: String, value: f64 },
    #[error("repricing distribution is missing bucket `{0}`")]
    MissingBucket(&'static str),
    #[error("repricing distribution contains unexpected bucket `{0}`")]
    UnexpectedBucket(String),
    #[error("repricing distribution sums to {0}, expected 1 ± 0.02")]
    InvalidDistributionSum(f64),
    #[error("repricing confidence must be finite and in [0, 1], got {0}")]
    InvalidConfidence(f64),
    #[error("invalid request state: {0}")]
    InvalidRequestState(String),
}

/// Tolerance for the 7-bucket repricing distribution sum. Jev rounds bucket
/// probabilities to 2 decimals, so live sums of 0.99/1.01 are rounding
/// artifacts, not malformed judgments; values are kept verbatim and never
/// renormalized. Genuinely broken distributions (e.g. 0.9/1.1) still fail.
const DISTRIBUTION_TOLERANCE: f64 = 2e-2;

/// Parse and validate the eight V1 answers from a decoded response envelope.
pub fn parse_v1_signal(response: &SystemOneResponse) -> Result<V1Signal, JevParseError> {
    let (repricing, repricing_confidence) = choice_distribution(response, "repricing_ticks")?;

    Ok(V1Signal {
        yes_pressure_5s: noul(response, "yes_pressure_5s")?,
        no_pressure_5s: noul(response, "no_pressure_5s")?,
        move_persists: noul(response, "move_persists")?,
        underreact_up: noul(response, "underreact_up")?,
        underreact_down: noul(response, "underreact_down")?,
        repricing,
        repricing_confidence,
        fill_before_decay: noul(response, "fill_before_decay")?,
        fill_toxic: noul(response, "fill_toxic")?,
    })
}

/// Parse JSON response bytes and validate all eight V1 answers.
pub fn parse_v1_signal_json(payload: &[u8]) -> Result<V1Signal, JevParseError> {
    let response = serde_json::from_slice::<SystemOneResponse>(payload)?;
    parse_v1_signal(&response)
}

/// Parse a validated response while preserving the originating state identity.
pub fn parse_evaluation(
    response: &SystemOneResponse,
    market_id: &str,
    state_seq: u64,
    sent_at_ms: i64,
    received_at_ms: i64,
) -> Result<JevEvaluation, JevParseError> {
    let usage = response.usage.clone().unwrap_or_default();
    Ok(JevEvaluation {
        market_id: market_id.to_owned(),
        state_seq,
        sent_at_ms,
        received_at_ms,
        latency_ms: received_at_ms.saturating_sub(sent_at_ms).max(0) as u64,
        signal: parse_v1_signal(response)?,
        tokens_in: usage.input_tokens,
        tokens_out: usage.output_tokens,
    })
}

/// Parse JSON response bytes while preserving the originating state identity.
pub fn parse_evaluation_json(
    payload: &[u8],
    market_id: &str,
    state_seq: u64,
    sent_at_ms: i64,
    received_at_ms: i64,
) -> Result<JevEvaluation, JevParseError> {
    let response = serde_json::from_slice::<SystemOneResponse>(payload)?;
    parse_evaluation(&response, market_id, state_seq, sent_at_ms, received_at_ms)
}

fn noul(response: &SystemOneResponse, answer_id: &'static str) -> Result<f64, JevParseError> {
    let answer = response
        .answers
        .get(answer_id)
        .ok_or(JevParseError::MissingAnswer(answer_id))?;
    validate_type(answer, answer_id, "noul")?;
    let value = answer.noul.ok_or(JevParseError::MissingField {
        answer: answer_id,
        field: "noul",
    })?;
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(JevParseError::InvalidProbability {
            answer: answer_id,
            value,
        });
    }
    Ok(value)
}

fn choice_distribution(
    response: &SystemOneResponse,
    answer_id: &'static str,
) -> Result<(TickDistribution, f64), JevParseError> {
    let answer = response
        .answers
        .get(answer_id)
        .ok_or(JevParseError::MissingAnswer(answer_id))?;
    validate_type(answer, answer_id, "choice")?;
    let probabilities = answer
        .probabilities
        .as_ref()
        .ok_or(JevParseError::MissingField {
            answer: answer_id,
            field: "probabilities",
        })?;

    for bucket in probabilities.keys() {
        if !REPRICING_BUCKETS.contains(&bucket.as_str()) {
            return Err(JevParseError::UnexpectedBucket(bucket.clone()));
        }
    }
    for bucket in REPRICING_BUCKETS {
        if !probabilities.contains_key(bucket) {
            return Err(JevParseError::MissingBucket(bucket));
        }
    }

    for (bucket, value) in probabilities {
        if !value.is_finite() || !(0.0..=1.0).contains(value) {
            return Err(JevParseError::InvalidBucketProbability {
                bucket: bucket.clone(),
                value: *value,
            });
        }
    }
    let sum: f64 = REPRICING_BUCKETS
        .iter()
        .map(|bucket| probabilities[*bucket])
        .sum();
    if (sum - 1.0).abs() > DISTRIBUTION_TOLERANCE {
        return Err(JevParseError::InvalidDistributionSum(sum));
    }

    let confidence = answer.confidence.ok_or(JevParseError::MissingField {
        answer: answer_id,
        field: "confidence",
    })?;
    if !confidence.is_finite() || !(0.0..=1.0).contains(&confidence) {
        return Err(JevParseError::InvalidConfidence(confidence));
    }

    Ok((
        TickDistribution {
            up_3_plus: probabilities["UP_3_PLUS_TICKS"],
            up_2: probabilities["UP_2_TICKS"],
            up_1: probabilities["UP_1_TICK"],
            flat: probabilities["FLAT"],
            down_1: probabilities["DOWN_1_TICK"],
            down_2: probabilities["DOWN_2_TICKS"],
            down_3_plus: probabilities["DOWN_3_PLUS_TICKS"],
        },
        confidence,
    ))
}

fn validate_type(
    answer: &RawAnswer,
    answer_id: &'static str,
    expected: &'static str,
) -> Result<(), JevParseError> {
    if let Some(actual) = answer.answer_type.as_deref()
        && actual != expected
    {
        return Err(JevParseError::UnexpectedAnswerType {
            answer: answer_id,
            expected,
            actual: actual.to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_payload() -> SystemOneResponse {
        serde_json::from_str(
            r#"
            {
              "model": "jev-latest",
              "answers": {
                "yes_pressure_5s": {"type": "noul", "noul": 0.81},
                "no_pressure_5s": {"type": "noul", "noul": 0.12},
                "move_persists": {"type": "noul", "noul": 0.73},
                "underreact_up": {"type": "noul", "noul": 0.88},
                "underreact_down": {"type": "noul", "noul": 0.18},
                "repricing_ticks": {
                  "type": "choice",
                  "choice": "UP_1_TICK",
                  "probabilities": {
                    "UP_3_PLUS_TICKS": 0.10,
                    "UP_2_TICKS": 0.20,
                    "UP_1_TICK": 0.44,
                    "FLAT": 0.12,
                    "DOWN_1_TICK": 0.07,
                    "DOWN_2_TICKS": 0.04,
                    "DOWN_3_PLUS_TICKS": 0.03
                  },
                  "confidence": 0.79
                },
                "fill_before_decay": {"type": "noul", "noul": 0.66},
                "fill_toxic": {"type": "noul", "noul": 0.21}
              }
            }
            "#,
        )
        .expect("fixture should decode")
    }

    #[test]
    fn valid_eight_answer_payload_parses_to_expected_signal() {
        let signal = parse_v1_signal(&valid_payload()).expect("fixture should validate");

        assert_eq!(signal.yes_pressure_5s, 0.81);
        assert_eq!(signal.no_pressure_5s, 0.12);
        assert_eq!(signal.move_persists, 0.73);
        assert_eq!(signal.underreact_up, 0.88);
        assert_eq!(signal.underreact_down, 0.18);
        assert_eq!(signal.repricing.up_3_plus, 0.10);
        assert_eq!(signal.repricing.up_2, 0.20);
        assert_eq!(signal.repricing.up_1, 0.44);
        assert_eq!(signal.repricing.flat, 0.12);
        assert_eq!(signal.repricing.down_1, 0.07);
        assert_eq!(signal.repricing.down_2, 0.04);
        assert_eq!(signal.repricing.down_3_plus, 0.03);
        assert_eq!(signal.repricing_confidence, 0.79);
        assert_eq!(signal.fill_before_decay, 0.66);
        assert_eq!(signal.fill_toxic, 0.21);
    }

    #[test]
    fn rejects_out_of_range_noul() {
        let mut response = valid_payload();
        response.answers.get_mut("underreact_up").unwrap().noul = Some(1.01);

        assert!(matches!(
            parse_v1_signal(&response),
            Err(JevParseError::InvalidProbability {
                answer: "underreact_up",
                ..
            })
        ));
    }

    #[test]
    fn rejects_distribution_that_does_not_sum_to_one() {
        let mut response = valid_payload();
        response
            .answers
            .get_mut("repricing_ticks")
            .unwrap()
            .probabilities
            .as_mut()
            .unwrap()
            .insert("FLAT".to_owned(), 0.20);

        assert!(matches!(
            parse_v1_signal(&response),
            Err(JevParseError::InvalidDistributionSum(_))
        ));
    }

    #[test]
    fn accepts_two_decimal_rounding_sums_verbatim() {
        // Live Jev rounds buckets to 2 decimals (observed sums 0.99/1.01).
        // Accepted without renormalization: stored values stay verbatim.
        let mut response = valid_payload();
        response
            .answers
            .get_mut("repricing_ticks")
            .unwrap()
            .probabilities
            .as_mut()
            .unwrap()
            .insert("FLAT".to_owned(), 0.11);
        let signal = parse_v1_signal(&response).expect("0.99 rounding is valid");
        assert_eq!(signal.repricing.flat, 0.11);
        assert_eq!(signal.repricing.up_1, 0.44);
    }

    #[test]
    fn rejects_missing_distribution_bucket() {
        let mut response = valid_payload();
        response
            .answers
            .get_mut("repricing_ticks")
            .unwrap()
            .probabilities
            .as_mut()
            .unwrap()
            .remove("DOWN_3_PLUS_TICKS");

        assert!(matches!(
            parse_v1_signal(&response),
            Err(JevParseError::MissingBucket("DOWN_3_PLUS_TICKS"))
        ));
    }

    #[test]
    fn usage_is_carried_into_the_evaluation() {
        let mut response = valid_payload();
        response.usage = Some(ResponseUsage {
            input_tokens: 1_234,
            output_tokens: 567,
        });
        let evaluation = parse_evaluation(&response, "market-1", 7, 1_000, 1_100)
            .expect("fixture should validate");

        assert_eq!(evaluation.tokens_in, 1_234);
        assert_eq!(evaluation.tokens_out, 567);
        assert_eq!(evaluation.state_seq, 7);
    }

    #[test]
    fn missing_usage_defaults_to_zero_without_failing() {
        let evaluation = parse_evaluation(&valid_payload(), "market-1", 7, 1_000, 1_100)
            .expect("fixture should validate");

        assert_eq!(evaluation.tokens_in, 0);
        assert_eq!(evaluation.tokens_out, 0);
    }

    #[test]
    fn usage_aliases_cover_provider_spellings() {
        let usage: ResponseUsage =
            serde_json::from_str(r#"{"prompt_tokens": 10, "completion_tokens": 20}"#)
                .expect("aliases should decode");
        assert_eq!(usage.input_tokens, 10);
        assert_eq!(usage.output_tokens, 20);

        let camel: ResponseUsage =
            serde_json::from_str(r#"{"inputTokens": 30, "outputTokens": 40}"#)
                .expect("camelCase should decode");
        assert_eq!(camel.input_tokens, 30);
        assert_eq!(camel.output_tokens, 40);
    }
}
