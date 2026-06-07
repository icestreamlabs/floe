use std::future::Future;
use std::time::Duration;

use anyhow::{Result, bail};
use tokio::time::{Instant, interval};

pub(crate) async fn wait_until<T, F, Fut>(
    label: impl AsRef<str>,
    timeout: Duration,
    poll_interval: Duration,
    mut attempt: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<T>>>,
{
    let label = label.as_ref();
    let deadline = Instant::now() + timeout;
    let mut poll = interval(poll_interval);
    let mut last_error = None;

    loop {
        match attempt().await {
            Ok(Some(value)) => return Ok(value),
            Ok(None) => {}
            Err(err) => last_error = Some(err),
        }
        if Instant::now() >= deadline {
            if let Some(err) = last_error {
                bail!("timed out waiting for {label}: {err}");
            }
            bail!("timed out waiting for {label}");
        }
        poll.tick().await;
    }
}
