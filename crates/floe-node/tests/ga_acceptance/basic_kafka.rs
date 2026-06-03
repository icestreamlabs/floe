use super::*;

#[tokio::test]
async fn http_ingest_to_mv_to_http_sink_acceptance() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;
    let ingest_port = find_unused_port()?;
    let sink_port = find_unused_port()?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("http_http_sink.json");
    let ingest_addr = format!("http://127.0.0.1:{ingest_port}");
    let sink_url = format!("http://127.0.0.1:{sink_port}/collect");

    let (sink_tx, mut sink_rx) = mpsc::channel::<Value>(16);
    let sink_server = spawn_sink_server(sink_port, sink_tx).await?;

    let config = json!({
        "connectors": [
            {
                "type": "http",
                "host": "127.0.0.1",
                "port": ingest_port,
                "default_source": "nexmark_bid"
            }
        ],
        "sinks": [
            {
                "type": "http",
                "name": "http_acceptance",
                "mv": "mv_acceptance_bid",
                "url": sink_url,
                "with_snapshot": true,
                "batch_rows": 1,
                "batch_bytes": 1048576,
                "queue_capacity": 64
            }
        ]
    });
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)
        .context("write acceptance config")?;

    let mut child = spawn_node_with_args(&config_path, &data_dir, 0, Some(BID_MV_SQL), &[]).await?;

    let test_result = async {
        wait_for_healthz(&ingest_addr).await?;
        post_bid(&ingest_addr, 101, 7001, 999).await?;

        let payload = timeout(Duration::from_secs(10), sink_rx.recv())
            .await
            .context("timed out waiting for sink payload")?
            .context("sink receiver closed")?;
        let rows = payload_to_rows(payload);
        let matched = rows.iter().any(|row| {
            row.get("auction").and_then(Value::as_i64) == Some(101)
                && row.get("bidder").and_then(Value::as_i64) == Some(7001)
                && row.get("price").and_then(Value::as_i64) == Some(999)
        });
        if !matched {
            bail!("http sink payload did not include expected row");
        }
        Ok(())
    }
    .await;

    stop_child(&mut child, "INT").await;
    sink_server.abort();
    let _ = sink_server.await;
    test_result
}

#[tokio::test]
#[ignore = "requires Kafka broker; set FLOE_ACCEPTANCE_KAFKA_BROKERS (and optionally FLOE_ACCEPTANCE_KAFKA_TOPIC_PREFIX)"]
async fn kafka_to_mv_to_pgwire_acceptance() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;
    let pg_port = find_unused_port()?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("kafka_pgwire_acceptance.json");
    let brokers = std::env::var("FLOE_ACCEPTANCE_KAFKA_BROKERS")
        .context("set FLOE_ACCEPTANCE_KAFKA_BROKERS for kafka acceptance")?;
    let topic_prefix = std::env::var("FLOE_ACCEPTANCE_KAFKA_TOPIC_PREFIX")
        .unwrap_or_else(|_| "floe_acceptance".to_string());
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let topic = format!("{topic_prefix}_{run_id}");
    let group_id = format!("floe-acceptance-{run_id}");

    let config = json!({
        "connectors": [
            {
                "type": "kafka",
                "brokers": brokers,
                "topics": [topic],
                "group_id": group_id,
                "default_source": "nexmark_bid",
                "poll_ms": 100,
                "max_messages_per_tick": 64
            }
        ]
    });
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)
        .context("write kafka acceptance config")?;

    let mut child = spawn_node(&config_path, &data_dir, pg_port, Some(BID_MV_SQL)).await?;

    let test_result = async {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .context("create kafka producer")?;
        let payload = json!({
            "source": "nexmark_bid",
            "data": {
                "auction": 202,
                "bidder": 7002,
                "price": 1999,
                "channel": "web",
                "url": "http://example.com",
                "date_time": 1_700_000_202_i64,
                "extra": "kafka_acceptance"
            }
        })
        .to_string();
        let record = FutureRecord::<(), _>::to(&topic).payload(&payload);
        producer
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(err, _)| err)
            .context("produce kafka acceptance message")?;

        wait_for_auction_count_at_least(pg_port, 202, 1).await?;
        Ok(())
    }
    .await;

    stop_child(&mut child, "INT").await;
    test_result
}

#[tokio::test]
#[ignore = "requires Kafka broker; set FLOE_ACCEPTANCE_KAFKA_BROKERS (and optionally FLOE_ACCEPTANCE_KAFKA_TOPIC_PREFIX)"]
async fn kafka_restart_rebuilds_transient_join_from_replayable_topic() -> Result<()> {
    let temp_dir = TempDir::new().context("create temp dir")?;
    let pg_port = find_unused_port()?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("kafka_restart_join.json");
    let brokers = std::env::var("FLOE_ACCEPTANCE_KAFKA_BROKERS")
        .context("set FLOE_ACCEPTANCE_KAFKA_BROKERS for kafka acceptance")?;
    let topic_prefix = std::env::var("FLOE_ACCEPTANCE_KAFKA_TOPIC_PREFIX")
        .unwrap_or_else(|_| "floe_acceptance".to_string());
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let topic = format!("{topic_prefix}_restart_{run_id}");
    let group_id = format!("floe-acceptance-restart-{run_id}");

    create_kafka_topic(&brokers, &topic).await?;

    let config = json!({
        "connectors": [
            {
                "type": "kafka",
                "brokers": brokers,
                "topics": [topic],
                "group_id": group_id,
                "poll_ms": 25,
                "max_messages_per_tick": 64
            }
        ],
        "runtime": {
            "mv_snapshot": {
                "max_pending_rows": 1,
                "max_pending_batches": 1,
                "max_delay_ms": 100
            }
        }
    });
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)
        .context("write kafka restart config")?;

    let mut first = spawn_node(&config_path, &data_dir, pg_port, Some(JOIN_MV_SQL)).await?;
    let test_result = async {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .context("create kafka producer")?;
        produce_auction(&producer, &topic, 501, 9001).await?;
        produce_bid(&producer, &topic, 501, 8001, 1234).await?;
        wait_for_join_count_at_least(pg_port, 501, 1).await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    stop_child(&mut first, "INT").await;
    test_result?;

    let mut restarted = spawn_node(&config_path, &data_dir, pg_port, Some(JOIN_MV_SQL)).await?;
    let restart_result = async {
        wait_for_join_count_at_least(pg_port, 501, 1).await?;
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .context("create kafka producer")?;
        produce_bid(&producer, &topic, 501, 8002, 4321).await?;
        wait_for_join_count_at_least(pg_port, 501, 2).await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    stop_child(&mut restarted, "INT").await;
    restart_result?;

    Ok(())
}
