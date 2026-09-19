//! Compile-time loader for the versioned Jev Lead-Lag V1 question document.

use std::collections::HashSet;

use thiserror::Error;

use crate::domain::PriceTicks;

use super::{ChoiceQuestion, NoulQuestion, RepricingCriteria, V1Questions};

const DOCUMENT: &str = include_str!("../../docs/jev-questions-v1.md");

/// The exact question IDs consumed by the V1 response parser.
pub const QUESTION_IDS: [&str; 8] = [
    "yes_pressure_5s",
    "no_pressure_5s",
    "move_persists",
    "underreact_up",
    "underreact_down",
    "repricing_ticks",
    "fill_before_decay",
    "fill_toxic",
];

const CRITERIA_IDS: [&str; 7] = [
    "UP_3_PLUS_TICKS",
    "UP_2_TICKS",
    "UP_1_TICK",
    "FLAT",
    "DOWN_1_TICK",
    "DOWN_2_TICKS",
    "DOWN_3_PLUS_TICKS",
];

/// The only question kinds accepted by the V1 document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuestionType {
    Noul,
    Choice,
}

/// Parsed, validated question data before candidate-price substitution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuestionDefinition {
    pub id: String,
    pub question_type: QuestionType,
    pub instructions: String,
    pub criteria: Vec<(String, String)>,
}

/// Errors found while parsing or validating the versioned question document.
#[derive(Debug, Error)]
pub enum QuestionsMdError {
    #[error("question document has invalid syntax on line {line}: {content}")]
    InvalidSyntax { line: usize, content: String },
    #[error("question `{id}` is missing field `{field}`")]
    MissingField { id: String, field: &'static str },
    #[error("question ID `{id}` is not part of the V1 contract")]
    UnknownQuestionId { id: String },
    #[error("question ID `{id}` is duplicated")]
    DuplicateQuestionId { id: String },
    #[error("question `{id}` has unsupported type `{kind}`")]
    InvalidType { id: String, kind: String },
    #[error("question `{id}` has criteria but is not a choice question")]
    UnexpectedCriteria { id: String },
    #[error("choice question `{id}` is missing criteria `{criterion}`")]
    MissingCriterion { id: String, criterion: &'static str },
    #[error("choice question `{id}` has unexpected criteria `{criterion}`")]
    UnexpectedCriterion { id: String, criterion: String },
    #[error("choice question `{id}` duplicates criteria `{criterion}`")]
    DuplicateCriterion { id: String, criterion: String },
    #[error("question `{id}` must contain the candidate price placeholder")]
    MissingCandidatePricePlaceholder { id: String },
    #[error("question `{id}` contains the candidate price placeholder unexpectedly")]
    UnexpectedCandidatePricePlaceholder { id: String },
}

/// Parse and validate the embedded Markdown document without doing I/O.
pub fn parse_document(document: &str) -> Result<Vec<QuestionDefinition>, QuestionsMdError> {
    let mut sections = Vec::new();
    let mut current: Option<RawQuestion> = None;
    let mut lines = document.lines().enumerate().peekable();

    while let Some((line_number, line)) = lines.next() {
        if let Some(id) = line.strip_prefix("## ") {
            if let Some(previous) = current.take() {
                sections.push(finish(previous)?);
            }
            let id = id.to_owned();
            if id.is_empty() {
                return Err(QuestionsMdError::InvalidSyntax {
                    line: line_number + 1,
                    content: line.to_owned(),
                });
            }
            current = Some(RawQuestion {
                id,
                question_type: None,
                instructions: None,
                criteria: Vec::new(),
                criteria_declared: false,
            });
            continue;
        }

        let Some(question) = current.as_mut() else {
            continue;
        };
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some(kind) = line.strip_prefix("type: ") {
            if question.question_type.is_some() {
                return Err(QuestionsMdError::InvalidSyntax {
                    line: line_number + 1,
                    content: line.to_owned(),
                });
            }
            question.question_type = Some(match kind {
                "noul" => QuestionType::Noul,
                "choice" => QuestionType::Choice,
                _ => {
                    return Err(QuestionsMdError::InvalidType {
                        id: question.id.clone(),
                        kind: kind.to_owned(),
                    });
                }
            });
            continue;
        }

        if line == "instructions: |" {
            if question.instructions.is_some() {
                return Err(QuestionsMdError::InvalidSyntax {
                    line: line_number + 1,
                    content: line.to_owned(),
                });
            }
            let mut instructions = Vec::new();
            while let Some((_, next_line)) = lines.peek().copied() {
                if next_line.is_empty() {
                    break;
                }
                let Some(text) = next_line.strip_prefix("  ") else {
                    break;
                };
                let _ = lines.next();
                instructions.push(text);
            }
            if instructions.is_empty() {
                return Err(QuestionsMdError::MissingField {
                    id: question.id.clone(),
                    field: "instructions",
                });
            }
            question.instructions = Some(instructions.join("\n"));
            continue;
        }

        if line == "criteria:" {
            if question.criteria_declared {
                return Err(QuestionsMdError::InvalidSyntax {
                    line: line_number + 1,
                    content: line.to_owned(),
                });
            }
            question.criteria_declared = true;
            while let Some((criteria_line_number, next_line)) = lines.peek().copied() {
                if next_line.is_empty() {
                    break;
                }
                let Some(entry) = next_line.strip_prefix("  ") else {
                    break;
                };
                let _ = lines.next();
                let Some((criterion, value)) = entry.split_once(": ") else {
                    return Err(QuestionsMdError::InvalidSyntax {
                        line: criteria_line_number + 1,
                        content: next_line.to_owned(),
                    });
                };
                if criterion.is_empty() || value.is_empty() {
                    return Err(QuestionsMdError::InvalidSyntax {
                        line: criteria_line_number + 1,
                        content: next_line.to_owned(),
                    });
                }
                question
                    .criteria
                    .push((criterion.to_owned(), value.to_owned()));
            }
            continue;
        }

        return Err(QuestionsMdError::InvalidSyntax {
            line: line_number + 1,
            content: line.to_owned(),
        });
    }

    if let Some(last) = current {
        sections.push(finish(last)?);
    }
    validate_sections(sections)
}

/// Load the compile-time document and substitute the candidate maker price.
pub fn load(candidate_buy_price: PriceTicks) -> Result<V1Questions, QuestionsMdError> {
    let definitions = parse_document(DOCUMENT)?;
    build_questions(&definitions, candidate_buy_price)
}

fn build_questions(
    definitions: &[QuestionDefinition],
    candidate_buy_price: PriceTicks,
) -> Result<V1Questions, QuestionsMdError> {
    let instruction = |id: &'static str| -> Result<String, QuestionsMdError> {
        let Some(definition) = definitions.iter().find(|definition| definition.id == id) else {
            return Err(QuestionsMdError::UnknownQuestionId { id: id.to_owned() });
        };
        Ok(render_instructions(definition, candidate_buy_price))
    };
    let repricing = definitions
        .iter()
        .find(|definition| definition.id == "repricing_ticks")
        .ok_or_else(|| QuestionsMdError::UnknownQuestionId {
            id: "repricing_ticks".to_owned(),
        })?;

    let criteria = |id: &'static str| -> Result<String, QuestionsMdError> {
        repricing
            .criteria
            .iter()
            .find(|(criterion, _)| criterion == id)
            .map(|(_, value)| value.clone())
            .ok_or_else(|| QuestionsMdError::MissingCriterion {
                id: repricing.id.clone(),
                criterion: id,
            })
    };

    Ok(V1Questions::from_parts(
        NoulQuestion::new(instruction("yes_pressure_5s")?),
        NoulQuestion::new(instruction("no_pressure_5s")?),
        NoulQuestion::new(instruction("move_persists")?),
        NoulQuestion::new(instruction("underreact_up")?),
        NoulQuestion::new(instruction("underreact_down")?),
        ChoiceQuestion::with_criteria(
            instruction("repricing_ticks")?,
            RepricingCriteria {
                up_3_plus_ticks: criteria("UP_3_PLUS_TICKS")?,
                up_2_ticks: criteria("UP_2_TICKS")?,
                up_1_tick: criteria("UP_1_TICK")?,
                flat: criteria("FLAT")?,
                down_1_tick: criteria("DOWN_1_TICK")?,
                down_2_ticks: criteria("DOWN_2_TICKS")?,
                down_3_plus_ticks: criteria("DOWN_3_PLUS_TICKS")?,
            },
        ),
        NoulQuestion::new(instruction("fill_before_decay")?),
        NoulQuestion::new(instruction("fill_toxic")?),
    ))
}

fn render_instructions(definition: &QuestionDefinition, candidate_buy_price: PriceTicks) -> String {
    definition.instructions.replace(
        "{candidate_price}",
        &candidate_buy_price.to_f64().to_string(),
    )
}

fn finish(raw: RawQuestion) -> Result<QuestionDefinition, QuestionsMdError> {
    let question_type = raw
        .question_type
        .ok_or_else(|| QuestionsMdError::MissingField {
            id: raw.id.clone(),
            field: "type",
        })?;
    let instructions = raw
        .instructions
        .ok_or_else(|| QuestionsMdError::MissingField {
            id: raw.id.clone(),
            field: "instructions",
        })?;

    match question_type {
        QuestionType::Noul if raw.criteria_declared => {
            return Err(QuestionsMdError::UnexpectedCriteria { id: raw.id });
        }
        QuestionType::Choice if !raw.criteria_declared => {
            return Err(QuestionsMdError::MissingField {
                id: raw.id,
                field: "criteria",
            });
        }
        QuestionType::Choice => validate_criteria(&raw.id, &raw.criteria)?,
        QuestionType::Noul => {}
    }

    if matches!(raw.id.as_str(), "fill_before_decay" | "fill_toxic")
        && !instructions.contains("{candidate_price}")
    {
        return Err(QuestionsMdError::MissingCandidatePricePlaceholder { id: raw.id });
    }
    if !matches!(raw.id.as_str(), "fill_before_decay" | "fill_toxic")
        && instructions.contains("{candidate_price}")
    {
        return Err(QuestionsMdError::UnexpectedCandidatePricePlaceholder { id: raw.id });
    }

    Ok(QuestionDefinition {
        id: raw.id,
        question_type,
        instructions,
        criteria: raw.criteria,
    })
}

fn validate_sections(
    sections: Vec<QuestionDefinition>,
) -> Result<Vec<QuestionDefinition>, QuestionsMdError> {
    let mut seen = HashSet::new();
    for section in &sections {
        if !QUESTION_IDS.contains(&section.id.as_str()) {
            return Err(QuestionsMdError::UnknownQuestionId {
                id: section.id.clone(),
            });
        }
        if !seen.insert(section.id.as_str()) {
            return Err(QuestionsMdError::DuplicateQuestionId {
                id: section.id.clone(),
            });
        }
    }
    for id in QUESTION_IDS {
        if !seen.contains(id) {
            return Err(QuestionsMdError::MissingField {
                id: id.to_owned(),
                field: "section",
            });
        }
    }
    if sections.len() != QUESTION_IDS.len() {
        return Err(QuestionsMdError::InvalidSyntax {
            line: 0,
            content: "expected exactly eight question sections".to_owned(),
        });
    }
    Ok(sections)
}

fn validate_criteria(id: &str, criteria: &[(String, String)]) -> Result<(), QuestionsMdError> {
    let mut seen = HashSet::new();
    for (criterion, _) in criteria {
        if !CRITERIA_IDS.contains(&criterion.as_str()) {
            return Err(QuestionsMdError::UnexpectedCriterion {
                id: id.to_owned(),
                criterion: criterion.clone(),
            });
        }
        if !seen.insert(criterion.as_str()) {
            return Err(QuestionsMdError::DuplicateCriterion {
                id: id.to_owned(),
                criterion: criterion.clone(),
            });
        }
    }
    for criterion in CRITERIA_IDS {
        if !seen.contains(criterion) {
            return Err(QuestionsMdError::MissingCriterion {
                id: id.to_owned(),
                criterion,
            });
        }
    }
    Ok(())
}

struct RawQuestion {
    id: String,
    question_type: Option<QuestionType>,
    instructions: Option<String>,
    criteria: Vec<(String, String)>,
    criteria_declared: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn golden_document_has_exact_v1_ids_types_and_buckets() {
        let definitions = parse_document(DOCUMENT).expect("question document should parse");
        let ids: Vec<&str> = definitions
            .iter()
            .map(|definition| definition.id.as_str())
            .collect();
        assert_eq!(ids, QUESTION_IDS);
        assert_eq!(
            definitions
                .iter()
                .map(|definition| definition.question_type)
                .collect::<Vec<_>>(),
            vec![
                QuestionType::Noul,
                QuestionType::Noul,
                QuestionType::Noul,
                QuestionType::Noul,
                QuestionType::Noul,
                QuestionType::Choice,
                QuestionType::Noul,
                QuestionType::Noul,
            ]
        );

        let questions = load(PriceTicks::from_f64(0.44)).expect("questions should load");
        let value = serde_json::to_value(questions).expect("questions should serialize");
        assert_eq!(value.as_object().map(|object| object.len()), Some(8));
        assert_eq!(value["repricing_ticks"]["type"], json!("choice"));
        assert_eq!(
            value["repricing_ticks"]["criteria"],
            json!({
                "UP_3_PLUS_TICKS": "YES rises by 3 or more minimum price increments",
                "UP_2_TICKS": "YES rises by 2 minimum price increments",
                "UP_1_TICK": "YES rises by 1 minimum price increment",
                "FLAT": "YES stays within the current tick",
                "DOWN_1_TICK": "YES falls by 1 minimum price increment",
                "DOWN_2_TICKS": "YES falls by 2 minimum price increments",
                "DOWN_3_PLUS_TICKS": "YES falls by 3 or more minimum price increments"
            })
        );
    }

    #[test]
    fn substitutes_candidate_price_without_changing_other_wording() {
        let questions = load(PriceTicks::from_f64(0.44)).expect("questions should load");
        let value = serde_json::to_value(questions).expect("questions should serialize");

        assert_eq!(
            value["yes_pressure_5s"]["instructions"],
            "Does the current external market state in `underlying` imply an increase in the probability of YES over the next 5 seconds?"
        );
        assert_eq!(
            value["fill_before_decay"]["instructions"],
            "Is the maker order in `candidate_order` (BUY YES at 0.44) likely to be filled before the current informational advantage disappears?"
        );
        assert_eq!(
            value["fill_toxic"]["instructions"],
            "If the maker order in `candidate_order` (BUY YES at 0.44) gets filled, is the fill likely to occur because the market is moving adversely against that quote?"
        );
    }
}
