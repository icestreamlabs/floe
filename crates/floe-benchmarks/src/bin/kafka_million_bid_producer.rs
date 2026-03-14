use std::env;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rdkafka::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

const DEFAULT_ROWS: usize = 1_000_000;
const DEFAULT_PROGRESS_EVERY: usize = 100_000;
const BASE_TS_MS: i64 = 1_700_000_000_000;

#[derive(Debug)]
struct Config {
    brokers: String,
    topic: String,
    rows: usize,
    progress_every: usize,
}

#[derive(Debug)]
struct BidInput {
    auction: i64,
    bidder: i64,
    price: i64,
    channel: &'static str,
    url: String,
    date_time_ms: i64,
}

impl BidInput {
    fn from_bid_idx(bid_idx: usize) -> Self {
        let bid_idx_i64 = i64::try_from(bid_idx).unwrap_or_default();
        let auction = i64::try_from((bid_idx - 1) % 10_000 + 1).unwrap_or_default();
        let bidder = 10_000_i64 + bid_idx_i64;
        let price = 1_000_i64 + (bid_idx_i64 % 50_000);
        let channel = match bid_idx % 5 {
            0 => "web",
            1 => "apple",
            2 => "google",
            3 => "facebook",
            _ => "baidu",
        };
        let dir1 = format!("dir{}", auction % 11);
        let url = if channel == "web" {
            format!(
                "https://example.com/{dir1}/item/{bid_idx}?q=1&channel_id={}",
                bid_idx % 97
            )
        } else {
            format!("https://example.com/{dir1}/item/{bid_idx}?q=1")
        };
        Self {
            auction,
            bidder,
            price,
            channel,
            url,
            date_time_ms: BASE_TS_MS + bid_idx_i64,
        }
    }

    fn to_json(&self, bid_idx: usize) -> String {
        format!(
            "{{\"auction\":{},\"bidder\":{},\"price\":{},\"channel\":\"{}\",\"url\":\"{}\",\"date_time\":{},\"extra\":\"bid_extra_{}\"}}",
            self.auction,
            self.bidder,
            self.price,
            self.channel,
            self.url,
            self.date_time_ms,
            bid_idx,
        )
    }
}

fn parse_args() -> Result<Config> {
    let mut brokers = None;
    let mut topic = None;
    let mut rows = DEFAULT_ROWS;
    let mut progress_every = DEFAULT_PROGRESS_EVERY;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--brokers" => brokers = args.next(),
            "--topic" => topic = args.next(),
            "--rows" => {
                rows = args
                    .next()
                    .context("missing value for --rows")?
                    .parse()
                    .context("parse --rows")?;
            }
            "--progress-every" => {
                progress_every = args
                    .next()
                    .context("missing value for --progress-every")?
                    .parse()
                    .context("parse --progress-every")?;
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    let brokers = brokers.context("missing required --brokers")?;
    let topic = topic.context("missing required --topic")?;

    Ok(Config {
        brokers,
        topic,
        rows,
        progress_every,
    })
}

fn print_usage() {
    println!(
        "Usage: kafka_million_bid_producer --brokers HOST:PORT --topic TOPIC [--rows N] [--progress-every N]"
    );
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let config = parse_args()?;

    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &config.brokers)
        .create()
        .context("create kafka producer")?;

    for bid_idx in 1..=config.rows {
        let payload = BidInput::from_bid_idx(bid_idx).to_json(bid_idx);

        loop {
            let record: BaseRecord<'_, (), _> = BaseRecord::to(&config.topic).payload(&payload);
            match producer.send(record) {
                Ok(()) => break,
                Err((error, _record)) => {
                    if producer.in_flight_count() > 0 {
                        producer.poll(Duration::from_millis(10));
                        continue;
                    }
                    return Err(error).context("send kafka record");
                }
            }
        }

        producer.poll(Duration::ZERO);

        if config.progress_every > 0 && bid_idx % config.progress_every == 0 {
            println!("produced {bid_idx} rows to topic={}", config.topic);
        }
    }

    producer
        .flush(Duration::from_secs(60))
        .context("flush kafka producer")?;

    println!(
        "finished producing rows={} topic={} brokers={}",
        config.rows, config.topic, config.brokers
    );

    Ok(())
}
