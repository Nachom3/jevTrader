mod state;
mod strategy;

use anyhow::Result;
use jevtrader::config::AppConfig;

fn main() -> Result<()> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(startup_check())
}

async fn startup_check() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    eprintln!("INFO jevtrader online: market engine scaffold ready!");
    let config = AppConfig::load().map_err(anyhow::Error::new)?;
    eprintln!(
        "INFO configuration validated and quote thresholds loaded: {:?}",
        config.quote_thresholds
    );
    eprintln!(
        "INFO Tokio runtime startup check complete; actor loops and live wiring are deferred"
    );
    Ok(())
}
