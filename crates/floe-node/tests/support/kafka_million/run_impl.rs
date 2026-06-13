use super::*;

pub(super) async fn run_redpanda_kafka_million_test_impl(
    spec: MillionQuerySpec,
    sink_mode: SinkMode,
    no_sink_verify_mode_override: Option<NoSinkVerifyMode>,
) -> Result<()> {
    let brokers =
        std::env::var("FLOE_REDPANDA_BROKERS").unwrap_or_else(|_| "127.0.0.1:9092".to_string());
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let artifacts_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/e2e_artifacts")
        .join(format!("{}_{}", spec.mv_name, run_id));
    std::fs::create_dir_all(&artifacts_dir).context("create artifact dir")?;

    let dataset_path = artifacts_dir.join("dataset.jsonl");
    let config_path = artifacts_dir.join("node_config.json");
    let stdout_log_path = artifacts_dir.join("floe-node.stdout.log");
    let stderr_log_path = artifacts_dir.join("floe-node.stderr.log");
    let pg_port = find_unused_port()?;
    let input_topic = format!("floe_redpanda_in_{run_id}");
    let output_topic = format!("floe_redpanda_out_{run_id}");
    let group_id = format!("floe-redpanda-e2e-{run_id}");
    let mv_max_pending_deltas = std::env::var("FLOE_E2E_MV_MAX_PENDING_DELTAS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0);
    let mv_max_delay_ms = std::env::var("FLOE_E2E_MV_MAX_DELAY_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0);
    let mv_flush_enabled = mv_max_pending_deltas.is_some() || mv_max_delay_ms.is_some();
    let connector_max_messages_per_tick = std::env::var("FLOE_E2E_CONNECTOR_MAX_MESSAGES_PER_TICK")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(16_384);
    let sink_batch_rows = std::env::var("FLOE_E2E_SINK_BATCH_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(16_384);
    let ingest_batch_size = std::env::var("FLOE_E2E_INGEST_BATCH_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(16_384);
    let ingest_batch_per_source = std::env::var("FLOE_E2E_INGEST_BATCH_PER_SOURCE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(16_384);
    let ingest_batch_per_connector = std::env::var("FLOE_E2E_INGEST_BATCH_PER_CONNECTOR")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(16_384);
    let slatedb_flush_interval_ms = std::env::var("FLOE_E2E_SLATEDB_FLUSH_INTERVAL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok());
    let no_sink_verify_mode =
        no_sink_verify_mode_override.unwrap_or_else(NoSinkVerifyMode::from_env);
    let no_sink_end_count_settle_ms = std::env::var("FLOE_E2E_NO_SINK_END_COUNT_SETTLE_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_NO_SINK_END_COUNT_SETTLE_MS);
    let no_sink_end_count_poll_ms = std::env::var("FLOE_E2E_NO_SINK_END_COUNT_POLL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_NO_SINK_END_COUNT_POLL_MS);
    let build_samples = !matches!(
        (sink_mode, no_sink_verify_mode),
        (
            SinkMode::NoSink,
            NoSinkVerifyMode::CountOnly | NoSinkVerifyMode::CountAtEndOnly
        )
    );

    eprintln!("artifacts_dir={}", artifacts_dir.display());
    eprintln!("dataset_path={}", dataset_path.display());
    eprintln!(
        "brokers={brokers} input_topic={input_topic} output_topic={output_topic} sink_mode={sink_mode:?}"
    );

    if matches!(sink_mode, SinkMode::NoSink) {
        eprintln!("verify.no_sink_mode={no_sink_verify_mode:?}");
        if matches!(no_sink_verify_mode, NoSinkVerifyMode::CountAtEndOnly) {
            eprintln!("verify.no_sink_end_count_settle_ms={no_sink_end_count_settle_ms}");
            eprintln!("verify.no_sink_end_count_poll_ms={no_sink_end_count_poll_ms}");
        }
    }

    let expected = {
        let dataset_generation_started = Instant::now();
        let dataset_path = dataset_path.clone();
        let expected = tokio::task::spawn_blocking(move || {
            generate_dataset_file(&dataset_path, spec, build_samples)
        })
        .await
        .context("join dataset generation task")??;
        eprintln!(
            "timing.dataset_generation_s={:.3}",
            dataset_generation_started.elapsed().as_secs_f64()
        );
        expected
    };
    if expected.generated_rows != spec.input_row_count {
        bail!(
            "dataset generator wrote {} rows, expected {}",
            expected.generated_rows,
            spec.input_row_count
        );
    }

    let use_sql_source = matches!(spec.dataset, MillionDatasetKind::BidOnly { .. });
    let connectors = if use_sql_source {
        Vec::new()
    } else {
        vec![serde_json::json!({
            "type": "kafka",
            "brokers": brokers,
            "topics": [input_topic],
            "group_id": group_id,
            "poll_ms": 10,
            "max_messages_per_tick": connector_max_messages_per_tick
        })]
    };

    let config = serde_json::json!({
        "connectors": connectors,
        "storage": {
            "await_durable": false
        },
        "runtime": {
            "mv_flush": {
                "enabled": mv_flush_enabled,
                "max_pending_deltas": mv_max_pending_deltas,
                "max_delay_ms": mv_max_delay_ms
            }
        }
    });
    eprintln!("runtime.mv_flush.enabled={mv_flush_enabled}");
    if let Some(max_pending_deltas) = mv_max_pending_deltas {
        eprintln!("runtime.mv_flush.max_pending_deltas={max_pending_deltas}");
    }
    if let Some(max_delay_ms) = mv_max_delay_ms {
        eprintln!("runtime.mv_flush.max_delay_ms={max_delay_ms}");
    }
    eprintln!("connector.max_messages_per_tick={connector_max_messages_per_tick}");
    if matches!(sink_mode, SinkMode::WithKafkaSink) {
        eprintln!("sink.batch_rows={sink_batch_rows}");
    }
    eprintln!("run.ingest_batch_size={ingest_batch_size}");
    eprintln!("run.ingest_batch_per_source={ingest_batch_per_source}");
    eprintln!("run.ingest_batch_per_connector={ingest_batch_per_connector}");
    if let Some(flush_interval_ms) = slatedb_flush_interval_ms {
        eprintln!("run.slatedb_flush_interval_ms={flush_interval_ms}");
    }
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)
        .context("write node config")?;

    ensure_topic_exists(&brokers, &input_topic).await?;
    if matches!(sink_mode, SinkMode::WithKafkaSink) {
        ensure_topic_exists(&brokers, &output_topic).await?;
    }

    let node_spawn_started = Instant::now();
    let mv_sql = million_sql_program(MillionSqlProgram {
        spec,
        sink_mode,
        use_sql_source,
        brokers: &brokers,
        input_topic: &input_topic,
        group_id: &group_id,
        output_topic: &output_topic,
        connector_max_messages_per_tick,
        sink_batch_rows,
    });
    let mut child = spawn_node(NodeSpawnConfig {
        config_path: &config_path,
        pg_port,
        mv_sql: &mv_sql,
        stdout_log_path: &stdout_log_path,
        stderr_log_path: &stderr_log_path,
        ingest_batch_size,
        ingest_batch_per_source,
        ingest_batch_per_connector,
        slatedb_flush_interval_ms,
    })
    .await?;
    eprintln!(
        "timing.node.spawn_s={:.3}",
        node_spawn_started.elapsed().as_secs_f64()
    );

    let test_result = async {
        let pgwire_ready_started = Instant::now();
        wait_for_pgwire(pg_port, &mut child, &stderr_log_path).await?;
        eprintln!(
            "timing.node.pgwire_ready_s={:.3} (post_spawn_wait_s={:.3})",
            node_spawn_started.elapsed().as_secs_f64(),
            pgwire_ready_started.elapsed().as_secs_f64()
        );
        let execution_started = Instant::now();

        match sink_mode {
            SinkMode::WithKafkaSink => {
                let (pgwire_ready_tx, pgwire_ready_rx) = oneshot::channel();
                let expected_for_pgwire = expected.clone();
                let pgwire_task = tokio::spawn(async move {
                    verify_pgwire_subscribe(SubscribeVerification {
                        pg_port,
                        mv_name: spec.mv_name,
                        output_fields: spec.output_fields,
                        sample_match_field: spec.sample_match_field,
                        expected: expected_for_pgwire,
                        verify_mode: SubscribeVerifyMode::SamplesOnly,
                        timeout: Duration::from_secs(1800),
                        ready_tx: pgwire_ready_tx,
                    })
                    .await
                });
                pgwire_ready_rx
                    .await
                    .context("wait for pgwire subscribe consumer readiness")?;

                let produce_started = Instant::now();
                {
                    let dataset_path = dataset_path.clone();
                    let brokers = brokers.clone();
                    let input_topic = input_topic.clone();
                    tokio::task::spawn_blocking(move || {
                        produce_dataset_file(
                            &dataset_path,
                            &brokers,
                            &input_topic,
                            spec.input_row_count,
                        )
                    })
                    .await
                    .context("join kafka producer task")??;
                }
                eprintln!(
                    "kafka production completed in {:?}",
                    produce_started.elapsed()
                );

                let observed_sink = {
                    let brokers = brokers.clone();
                    let output_topic = output_topic.clone();
                    let expected_row_count = expected.metrics.row_count;
                    let output_fields = spec.output_fields;
                    tokio::task::spawn_blocking(move || {
                        consume_sink_metrics(
                            &brokers,
                            &output_topic,
                            output_fields,
                            expected_row_count,
                            Duration::from_secs(1800),
                        )
                    })
                    .await
                    .context("join sink consumer task")??
                };

                assert_eq!(
                    observed_sink.row_count, expected.metrics.row_count,
                    "sink row count mismatch"
                );
                assert_eq!(
                    observed_sink.checksum, expected.metrics.checksum,
                    "sink checksum mismatch"
                );

                pgwire_task
                    .await
                    .context("join pgwire subscribe consumer task")??;
            }
            SinkMode::NoSink => {
                let produce_started = Instant::now();
                {
                    let dataset_path = dataset_path.clone();
                    let brokers = brokers.clone();
                    let input_topic = input_topic.clone();
                    tokio::task::spawn_blocking(move || {
                        produce_dataset_file(
                            &dataset_path,
                            &brokers,
                            &input_topic,
                            spec.input_row_count,
                        )
                    })
                    .await
                    .context("join kafka producer task")??;
                }
                let produce_elapsed = produce_started.elapsed();
                eprintln!(
                    "kafka production completed in {:?}",
                    produce_elapsed
                );

                let verify_timing = verify_mv_snapshot_count_and_samples(NoSinkVerification {
                    pg_port,
                    mv_name: spec.mv_name,
                    output_fields: spec.output_fields,
                    sample_match_field: spec.sample_match_field,
                    expected: expected.clone(),
                    timeout: Duration::from_secs(1800),
                    verify_mode: no_sink_verify_mode,
                    end_count_settle: Duration::from_millis(no_sink_end_count_settle_ms),
                    end_count_poll: Duration::from_millis(no_sink_end_count_poll_ms),
                })
                .await?;

                let ingest_completion =
                    produce_elapsed + verify_timing.wait_for_count_for_throughput;
                let input_rows_per_sec =
                    safe_rows_per_sec(spec.input_row_count as f64, ingest_completion.as_secs_f64());
                let output_rows_per_sec = safe_rows_per_sec(
                    expected.metrics.row_count.max(0) as f64,
                    ingest_completion.as_secs_f64(),
                );
                eprintln!(
                    "timing.no_sink.ingest_complete_s={:.3} (produce_s={:.3}, post_produce_wait_s={:.3}, post_produce_wait_for_throughput_s={:.3})",
                    ingest_completion.as_secs_f64(),
                    produce_elapsed.as_secs_f64(),
                    verify_timing.wait_for_count.as_secs_f64(),
                    verify_timing.wait_for_count_for_throughput.as_secs_f64()
                );
                eprintln!(
                    "timing.no_sink.pgwire_connect_s={:.3}",
                    verify_timing.pgwire_connect.as_secs_f64()
                );
                eprintln!(
                    "timing.no_sink.verification_s={:.3} (sample_query_s={:.3})",
                    verify_timing.total.as_secs_f64(),
                    verify_timing.sample_query.as_secs_f64()
                );
                eprintln!(
                    "throughput.no_sink.input_rows_per_sec={:.0} output_rows_per_sec={:.0}",
                    input_rows_per_sec, output_rows_per_sec
                );
            }
        }

        eprintln!(
            "timing.execution.total_s={:.3}",
            execution_started.elapsed().as_secs_f64()
        );
        eprintln!(
            "verified rows={} checksum={}",
            expected.metrics.row_count, expected.metrics.checksum
        );

        Ok(())
    }
    .await;

    stop_child(&mut child, "INT").await;
    test_result
}

struct MillionSqlProgram<'a> {
    spec: MillionQuerySpec,
    sink_mode: SinkMode,
    use_sql_source: bool,
    brokers: &'a str,
    input_topic: &'a str,
    group_id: &'a str,
    output_topic: &'a str,
    connector_max_messages_per_tick: usize,
    sink_batch_rows: usize,
}

fn million_sql_program(program: MillionSqlProgram<'_>) -> String {
    let mut sql = String::new();
    if program.use_sql_source {
        sql.push_str(&format!(
            r#"CREATE SOURCE nexmark_bid (
  auction BIGINT,
  bidder BIGINT,
  price BIGINT,
  channel TEXT,
  url TEXT,
  date_time TIMESTAMP,
  extra TEXT,
  PRIMARY KEY (auction, bidder, date_time, price)
)
WITH (
  connector = 'kafka',
  brokers = '{}',
  topic = '{}',
  group_id = '{}',
  poll_ms = 10,
  max_messages_per_tick = {}
)
FORMAT PLAIN ENCODE JSON;
"#,
            program.brokers,
            program.input_topic,
            program.group_id,
            program.connector_max_messages_per_tick
        ));
    }
    sql.push_str(program.spec.mv_sql.trim().trim_end_matches(';'));
    sql.push_str(";\n");
    if matches!(program.sink_mode, SinkMode::WithKafkaSink) {
        sql.push_str(&format!(
            r#"CREATE SINK kafka_sink_million FROM {} WITH (
  connector = 'kafka',
  brokers = '{}',
  topic = '{}',
  with_snapshot = false,
  batch_rows = {},
  batch_bytes = 16777216,
  queue_capacity = 65536,
  retry_max_attempts = 8,
  retry_base_ms = 50,
  retry_max_backoff_ms = 1000
);
"#,
            program.spec.mv_name, program.brokers, program.output_topic, program.sink_batch_rows
        ));
    }
    sql
}

pub(super) async fn ensure_topic_exists(brokers: &str, topic: &str) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .context("create kafka admin client")?;
    let results = admin
        .create_topics(
            &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(5))),
        )
        .await
        .with_context(|| format!("create topic {topic}"))?;
    for result in results {
        match result {
            Ok(created_topic) if created_topic == topic => {}
            Ok(created_topic) => {
                bail!("unexpected topic creation result for '{created_topic}', expected '{topic}'")
            }
            Err((existing_topic, RDKafkaErrorCode::TopicAlreadyExists))
                if existing_topic == topic => {}
            Err((failed_topic, code)) => {
                bail!("failed to create topic '{failed_topic}': {code}")
            }
        }
    }
    Ok(())
}
