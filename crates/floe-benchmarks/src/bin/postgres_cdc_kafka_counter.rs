use std::collections::BTreeMap;
use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::{Message, Offset, TopicPartitionList};

fn main() -> Result<()> {
    let args = Args::parse(env::args().skip(1).collect())?;
    let group_id = format!(
        "floe-cdc-bench-counter-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    );
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &args.brokers)
        .set("group.id", &group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("fetch.wait.max.ms", "1")
        .set("fetch.min.bytes", "1")
        .create()
        .context("create Kafka CDC benchmark consumer")?;
    let mut assignment = TopicPartitionList::new();
    for topic in &args.topics {
        assignment
            .add_partition_offset(topic, 0, Offset::Beginning)
            .with_context(|| {
                format!("build direct assignment for CDC benchmark topic '{topic}'")
            })?;
    }
    consumer
        .assign(&assignment)
        .with_context(|| format!("assign CDC benchmark topics {:?}", args.topics))?;

    let started_at = Instant::now();
    let deadline = started_at + args.timeout;
    let mut first_message_at = None;
    let mut last_message_at = None;
    let mut messages = 0_u64;
    let mut key_bytes = 0_u64;
    let mut value_bytes = 0_u64;
    let mut messages_by_topic = BTreeMap::<String, u64>::new();

    while Instant::now() < deadline && messages < args.expected {
        match consumer.poll(Duration::from_millis(50)) {
            Some(Ok(message)) => {
                let now = Instant::now();
                first_message_at.get_or_insert(now);
                last_message_at = Some(now);
                messages = messages.saturating_add(1);
                *messages_by_topic
                    .entry(message.topic().to_string())
                    .or_insert(0) += 1;
                key_bytes = key_bytes.saturating_add(
                    message
                        .key()
                        .map(|key| u64::try_from(key.len()).unwrap_or(u64::MAX))
                        .unwrap_or(0),
                );
                value_bytes = value_bytes.saturating_add(
                    message
                        .payload()
                        .map(|payload| u64::try_from(payload.len()).unwrap_or(u64::MAX))
                        .unwrap_or(0),
                );
                if messages.is_multiple_of(100_000) {
                    eprintln!("cdc_counter.messages={messages}");
                }
            }
            Some(Err(err)) if is_retryable_poll_error(&err) => {}
            Some(Err(err)) => return Err(anyhow!(err)).context("poll CDC benchmark topic"),
            None => {}
        }
    }

    if messages != args.expected {
        bail!(
            "CDC benchmark observed {messages} Kafka records, expected {} before timeout",
            args.expected
        );
    }

    let wall_s = started_at.elapsed().as_secs_f64();
    let stream_s = first_message_at
        .zip(last_message_at)
        .map(|(first, last)| (last - first).as_secs_f64().max(0.001))
        .unwrap_or(wall_s.max(0.001));
    let total_bytes = key_bytes.saturating_add(value_bytes);
    let wall_rows_per_s = messages as f64 / wall_s.max(0.001);
    let stream_rows_per_s = messages as f64 / stream_s;
    let wall_mb_per_s = total_bytes as f64 / 1_000_000.0 / wall_s.max(0.001);
    let stream_mb_per_s = total_bytes as f64 / 1_000_000.0 / stream_s;

    println!("cdc_counter.expected_messages={}", args.expected);
    println!("cdc_counter.observed_messages={messages}");
    for topic in &args.topics {
        let topic_messages = messages_by_topic.get(topic).copied().unwrap_or(0);
        println!(
            "cdc_counter.topic.{}.observed_messages={topic_messages}",
            topic_env_key(topic)
        );
    }
    println!("cdc_counter.key_bytes={key_bytes}");
    println!("cdc_counter.value_bytes={value_bytes}");
    println!("cdc_counter.total_bytes={total_bytes}");
    println!("cdc_counter.wall_seconds={wall_s:.3}");
    println!("cdc_counter.stream_seconds={stream_s:.3}");
    println!("cdc_counter.wall_rows_per_second={wall_rows_per_s:.0}");
    println!("cdc_counter.stream_rows_per_second={stream_rows_per_s:.0}");
    println!("cdc_counter.wall_mb_per_second={wall_mb_per_s:.3}");
    println!("cdc_counter.stream_mb_per_second={stream_mb_per_s:.3}");

    Ok(())
}

fn is_retryable_poll_error(err: &KafkaError) -> bool {
    let message = err.to_string();
    message.contains("UnknownTopicOrPartition")
        || message.contains("Broker: Unknown topic or partition")
}

struct Args {
    brokers: String,
    topics: Vec<String>,
    expected: u64,
    timeout: Duration,
}

impl Args {
    fn parse(args: Vec<String>) -> Result<Self> {
        let brokers = arg_value(&args, "--brokers")?;
        let topic_arg = arg_value(&args, "--topics").or_else(|_| arg_value(&args, "--topic"))?;
        let topics = topic_arg
            .split(',')
            .map(str::trim)
            .filter(|topic| !topic.is_empty())
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        if topics.is_empty() {
            bail!("--topic/--topics must contain at least one topic");
        }
        let expected = arg_value(&args, "--expected")?
            .parse::<u64>()
            .context("--expected must be a positive integer")?;
        if expected == 0 {
            bail!("--expected must be greater than zero");
        }
        let timeout_secs = arg_value(&args, "--timeout-secs")
            .unwrap_or_else(|_| "600".to_string())
            .parse::<u64>()
            .context("--timeout-secs must be a positive integer")?;
        if timeout_secs == 0 {
            bail!("--timeout-secs must be greater than zero");
        }
        Ok(Self {
            brokers,
            topics,
            expected,
            timeout: Duration::from_secs(timeout_secs),
        })
    }
}

fn topic_env_key(topic: &str) -> String {
    topic
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn arg_value(args: &[String], name: &str) -> Result<String> {
    let Some(idx) = args.iter().position(|arg| arg == name) else {
        bail!("missing required argument {name}");
    };
    args.get(idx + 1)
        .cloned()
        .ok_or_else(|| anyhow!("{name} requires a value"))
}
