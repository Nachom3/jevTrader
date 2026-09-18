//! System One client.

pub mod client;
pub mod request;
pub mod response;

pub use client::{JevError, evaluate};
pub use request::{
    CandidateOrder, ChoiceQuestion, MarketContext, NoulQuestion, OrderSide, RepricingCriteria,
    SystemOneRequest, TimeInForce, V1Questions, V1Request, V1State, v1_questions, v1_state,
};
pub use response::{
    JevEvaluation, JevParseError, RawAnswer, SystemOneResponse, TickDistribution, V1Signal,
    parse_evaluation, parse_evaluation_json, parse_v1_signal, parse_v1_signal_json,
};
