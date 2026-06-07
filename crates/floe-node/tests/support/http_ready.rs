use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::StatusCode;

use crate::wait::wait_until;

pub(crate) async fn wait_for_healthz(addr: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let addr = addr.to_owned();
    wait_until(
        "healthz ready",
        Duration::from_secs(6),
        Duration::from_millis(100),
        || {
            let client = client.clone();
            let addr = addr.clone();
            async move {
                let response = client
                    .get(format!("{addr}/healthz"))
                    .send()
                    .await
                    .context("request healthz")?;
                if response.status() == StatusCode::OK {
                    Ok(Some(()))
                } else {
                    bail!("healthz returned {}", response.status())
                }
            }
        },
    )
    .await
}
