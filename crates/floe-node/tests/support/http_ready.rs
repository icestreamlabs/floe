use std::time::Duration;

use anyhow::{Result, bail};
use reqwest::StatusCode;
use tokio::time::{Instant, interval};

pub(crate) async fn wait_for_healthz(addr: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(6);
    let mut poll = interval(Duration::from_millis(100));
    loop {
        match client.get(format!("{addr}/healthz")).send().await {
            Ok(response) if response.status() == StatusCode::OK => return Ok(()),
            Ok(response) if Instant::now() >= deadline => {
                bail!("healthz returned {}", response.status())
            }
            Err(err) if Instant::now() >= deadline => bail!("healthz never became ready: {err}"),
            Ok(_) | Err(_) => {
                poll.tick().await;
            }
        }
    }
}
