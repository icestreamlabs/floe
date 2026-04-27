use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use floe_node_core::connector::{Connector, ConnectorContext, ConnectorTick};
use floe_node_core::kafka_connector::{KafkaConnector, KafkaConnectorConfig};
use floe_node_core::source;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::json;

#[tokio::test]
#[ignore]
async fn kafka_connector_ingests_messages() {
    let brokers = match std::env::var("KAFKA_BROKERS") {
        Ok(value) => value,
        Err(_) => {
            eprintln!("set KAFKA_BROKERS to run kafka connector test");
            return;
        }
    };
    let topic = match std::env::var("KAFKA_TOPIC") {
        Ok(value) => value,
        Err(_) => {
            eprintln!("set KAFKA_TOPIC to run kafka connector test");
            return;
        }
    };

    let group_id = format!(
        "floe-test-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_millis()
    );

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("create kafka producer");

    let payload = json!({"source": "nexmark_bid", "data": {"auction": 1}}).to_string();
    let record = FutureRecord::<(), _>::to(&topic).payload(&payload);
    match producer.send(record, Duration::from_secs(5)).await {
        Ok(_) => {}
        Err((err, _)) => panic!("failed to produce test message: {err}"),
    }

    let (tx, mut rx) = source::channel(8);
    let config = KafkaConnectorConfig {
        brokers,
        topics: vec![topic],
        group_id,
        default_source: None,
        default_source_id: None,
        poll_timeout: Duration::from_millis(200),
        max_messages_per_tick: 16,
        message_format: None,
        commit_offsets_rx: None,
        resume_from_offsets: Vec::new(),
    };
    let mut connector =
        KafkaConnector::new(config, Vec::new(), HashMap::new()).expect("connector config");
    let ctx = ConnectorContext::new(tx);
    connector.init(&ctx).await.expect("connector init");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut emitted = 0usize;
    while tokio::time::Instant::now() < deadline {
        match connector.tick(&ctx).await.expect("connector tick") {
            ConnectorTick::Emitted(count) => emitted = emitted.saturating_add(count),
            ConnectorTick::Idle | ConnectorTick::Finished => {}
        }
        if emitted > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let batch = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("receive event")
        .expect("missing event");
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].source(), "nexmark_bid");

    connector.shutdown().await.expect("connector shutdown");
}
