use std::time::Duration;

use anyhow::{Context, Result, ensure};
use nexmark::config::NexmarkConfig;
use nexmark::EventGenerator;
use nexmark::event::Event;
use serde::Serialize;
use serde_json::json;
use tokio::time::sleep;

#[derive(Debug, Clone)]
pub struct Config {
    pub events_per_second: f64,
    pub max_events: Option<u64>,
}

pub async fn run(config: Config) -> Result<()> {
    ensure!(
        config.events_per_second.is_finite() && config.events_per_second > 0.0,
        "events-per-second must be a positive finite value"
    );

    if let Some(limit) = config.max_events {
        if limit == 0 {
            return Ok(());
        }
    }

    let mut generator = EventGenerator::new(NexmarkConfig::default());
    let interval = Duration::from_secs_f64(1.0 / config.events_per_second);
    let mut emitted: u64 = 0;

    loop {
        let event = generator
            .next()
            .context("nexmark generator produced no event")?;
        emit_event(&event)?;
        emitted = emitted.saturating_add(1);

        if let Some(limit) = config.max_events {
            if emitted >= limit {
                break;
            }
        }

        if !interval.is_zero() {
            sleep(interval).await;
        } else {
            tokio::task::yield_now().await;
        }
    }

    Ok(())
}

fn emit_event(event: &Event) -> Result<()> {
    match event {
        Event::Person(person) => print_payload("person", person),
        Event::Auction(auction) => print_payload("auction", auction),
        Event::Bid(bid) => print_payload("bid", bid),
    }
}

fn print_payload<T>(event_type: &str, payload: &T) -> Result<()>
where
    T: Serialize,
{
    let value = json!({
        "type": event_type,
        "data": payload,
    });
    let line = serde_json::to_string(&value)?;
    println!("{}", line);
    Ok(())
}
