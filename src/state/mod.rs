// Scaffold: state helpers are not wired into the engine yet; keep them public
// for the pure builder and bench targets without requiring engine integration.
#[allow(dead_code)]
pub mod feature_builder;
#[allow(dead_code)]
pub mod market_state;
#[allow(dead_code)]
pub mod poly_history;
#[allow(dead_code)]
pub mod quant_features;
#[allow(dead_code)]
pub mod rolling;

#[allow(dead_code, unused_imports)]
pub use feature_builder::*;
#[allow(dead_code, unused_imports)]
pub use market_state::*;
#[allow(dead_code, unused_imports)]
pub use poly_history::*;
#[allow(dead_code, unused_imports)]
pub use quant_features::*;
#[allow(dead_code, unused_imports)]
pub use rolling::*;
