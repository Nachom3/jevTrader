//! Paper/live boundary.

#[allow(dead_code)]
pub mod paper;

pub use paper::{
    DEFAULT_FILL_RATIO_PER_TOUCH, PaperBook, PaperError, PaperFill, PaperOrder, TopOfBookUpdate,
    markout,
};
