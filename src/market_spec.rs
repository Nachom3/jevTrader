//! Versioned market metadata loaded from a human-reviewable Markdown file.

use std::{collections::BTreeMap, fs, num::ParseFloatError, path::Path};

use thiserror::Error;

const REQUIRED_FIELDS: [&str; 6] = [
    "slug",
    "question",
    "resolution_source",
    "resolution_rules",
    "target",
    "resolution_at_ms",
];

/// The market metadata needed by the runtime decision pipeline.
#[derive(Debug, Clone, PartialEq)]
pub struct MarketSpec {
    pub slug: String,
    pub question: String,
    pub resolution_source: String,
    pub resolution_rules: String,
    pub target: f64,
    pub resolution_at_ms: i64,
}

impl MarketSpec {
    /// Load and validate one market specification from a Markdown file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, MarketSpecError> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path).map_err(|source| MarketSpecError::Io {
            path: path.display().to_string(),
            source,
        })?;
        Self::parse(&contents)
    }

    /// Parse the small line-oriented Markdown format used by versioned specs.
    pub fn parse(contents: &str) -> Result<Self, MarketSpecError> {
        let fields = parse_fields(contents)?;
        for field in REQUIRED_FIELDS {
            if !fields.contains_key(field) {
                return Err(MarketSpecError::MissingField { field });
            }
        }

        let value = |field: &'static str| {
            fields
                .get(field)
                .expect("required fields were checked above")
                .clone()
        };
        let target_text = value("target");
        let target =
            target_text
                .parse::<f64>()
                .map_err(|source| MarketSpecError::InvalidTarget {
                    value: target_text,
                    source,
                })?;
        if !target.is_finite() || target <= 0.0 {
            return Err(MarketSpecError::InvalidValue {
                field: "target",
                value: target.to_string(),
                reason: "must be finite and greater than zero",
            });
        }

        let resolution_at_text = value("resolution_at_ms");
        let resolution_at_ms = resolution_at_text.parse::<i64>().map_err(|source| {
            MarketSpecError::InvalidResolutionAtMs {
                value: resolution_at_text,
                source,
            }
        })?;

        Ok(Self {
            slug: value("slug"),
            question: value("question"),
            resolution_source: value("resolution_source"),
            resolution_rules: value("resolution_rules"),
            target,
            resolution_at_ms,
        })
    }
}

#[derive(Debug, Error)]
pub enum MarketSpecError {
    #[error("could not read market spec `{path}`: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("market spec field `{field}` is missing")]
    MissingField { field: &'static str },
    #[error("market spec field `{field}` is duplicated")]
    DuplicateField { field: String },
    #[error("unknown market spec field `{field}`")]
    UnknownField { field: String },
    #[error("invalid market spec syntax on line {line}: {content}")]
    InvalidSyntax { line: usize, content: String },
    #[error("invalid market spec value for `{field}`: {value} ({reason})")]
    InvalidValue {
        field: &'static str,
        value: String,
        reason: &'static str,
    },
    #[error("invalid target `{value}`: {source}")]
    InvalidTarget {
        value: String,
        #[source]
        source: ParseFloatError,
    },
    #[error("invalid resolution_at_ms `{value}`: {source}")]
    InvalidResolutionAtMs {
        value: String,
        #[source]
        source: std::num::ParseIntError,
    },
}

fn parse_fields(contents: &str) -> Result<BTreeMap<String, String>, MarketSpecError> {
    let mut fields = BTreeMap::new();
    let mut lines = contents.split('\n').enumerate();

    while let Some((line_number, raw_line)) = lines.next() {
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some(rest) = line.strip_prefix("resolution_rules:") {
            if !rest.trim().is_empty() && rest.trim() != "|" {
                return Err(MarketSpecError::InvalidSyntax {
                    line: line_number + 1,
                    content: line.to_owned(),
                });
            }
            if fields.contains_key("resolution_rules") {
                return Err(MarketSpecError::DuplicateField {
                    field: "resolution_rules".to_owned(),
                });
            }

            let mut rules = String::new();
            for (rules_line_number, raw_rules_line) in lines {
                let rules_line = raw_rules_line.strip_suffix('\r').unwrap_or(raw_rules_line);
                if rules_line.is_empty() {
                    rules.push('\n');
                } else if let Some(text) = rules_line.strip_prefix("  ") {
                    rules.push_str(text);
                    rules.push('\n');
                } else {
                    return Err(MarketSpecError::InvalidSyntax {
                        line: rules_line_number + 1,
                        content: rules_line.to_owned(),
                    });
                }
            }
            while rules.ends_with('\n') {
                rules.pop();
            }
            fields.insert("resolution_rules".to_owned(), rules);
            break;
        }

        let Some((key, raw_value)) = line.split_once(':') else {
            return Err(MarketSpecError::InvalidSyntax {
                line: line_number + 1,
                content: line.to_owned(),
            });
        };
        let key = key.trim();
        if !REQUIRED_FIELDS.contains(&key) {
            return Err(MarketSpecError::UnknownField {
                field: key.to_owned(),
            });
        }
        if fields.contains_key(key) {
            return Err(MarketSpecError::DuplicateField {
                field: key.to_owned(),
            });
        }
        let value = raw_value.strip_prefix(' ').unwrap_or(raw_value);
        if value.is_empty() {
            return Err(MarketSpecError::InvalidValue {
                field: field_name(key),
                value: value.to_owned(),
                reason: "must not be empty",
            });
        }
        fields.insert(key.to_owned(), value.to_owned());
    }

    Ok(fields)
}

fn field_name(field: &str) -> &'static str {
    match field {
        "slug" => "slug",
        "question" => "question",
        "resolution_source" => "resolution_source",
        "resolution_rules" => "resolution_rules",
        "target" => "target",
        "resolution_at_ms" => "resolution_at_ms",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn real_spec_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("markets/bitcoin-above-82k-on-september-19-2026.md")
    }

    #[test]
    fn parses_the_versioned_bitcoin_market_spec() {
        let spec = MarketSpec::load(real_spec_path()).expect("real market spec should parse");

        assert_eq!(spec.slug, "bitcoin-above-82k-on-september-19-2026");
        assert_eq!(
            spec.question,
            "Will the price of Bitcoin be above $82,000 on September 19?"
        );
        assert_eq!(spec.resolution_source, "Binance BTC/USDT 1m close");
        assert_eq!(spec.target, 82_000.0);
        assert_eq!(spec.resolution_at_ms, 1_789_833_600_000);
        assert_eq!(
            spec.resolution_rules,
            "This market will resolve to \"Yes\" if the Binance 1 minute candle for BTC/USDT 12:00 in the ET timezone (noon) on the date specified in the title has a final \"Close\" price higher than the price specified in the title. Otherwise, this market will resolve to \"No\".\n\nThe resolution source for this market is Binance, specifically the BTC/USDT \"Close\" prices currently available at https://www.binance.com/en/trade/BTC_USDT with \"1m\" and \"Candles\" selected on the top bar.\n\nPlease note that this market is about the price according to Binance BTC/USDT, not according to other exchanges or trading pairs.\n\nPrice precision is determined by the number of decimal places in the source."
        );
    }
}
