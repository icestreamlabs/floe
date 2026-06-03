use super::super::*;

pub(in crate::node_runtime) fn source_journal_required_sources(
    registry: &SourceRegistry,
    candidate_sources: &BTreeSet<String>,
    mode: SourceJournalConfig,
) -> BTreeSet<String> {
    match mode {
        SourceJournalConfig::Full => candidate_sources.clone(),
        SourceJournalConfig::None => BTreeSet::new(),
        SourceJournalConfig::Auto => candidate_sources
            .iter()
            .filter(|source| {
                registry
                    .get(source.as_str())
                    .is_none_or(|definition| !source_is_replayable_from_connector(definition))
            })
            .cloned()
            .collect(),
    }
}

pub(in crate::node_runtime) fn kafka_metadata_journal_required_sources(
    registry: &SourceRegistry,
    candidate_sources: &BTreeSet<String>,
    mode: SourceJournalConfig,
) -> BTreeSet<String> {
    if mode != SourceJournalConfig::Auto {
        return BTreeSet::new();
    }
    candidate_sources
        .iter()
        .filter(|source| {
            registry
                .get(source.as_str())
                .is_some_and(source_is_replayable_from_connector)
        })
        .cloned()
        .collect()
}

pub(super) fn source_is_replayable_from_connector(definition: &SourceDefinition) -> bool {
    definition.properties().iter().any(|(key, value)| {
        key.starts_with("connector.") && key.ends_with(".type") && value.as_str() == "kafka"
    })
}

pub(super) async fn replay_committed_vectorized_source_journal_entries(
    config: ReplayCommittedVectorizedSourceJournalConfig<'_>,
) -> anyhow::Result<(usize, usize)> {
    let ReplayCommittedVectorizedSourceJournalConfig {
        source_batch_journal,
        kafka_journal,
        vectorized_runtime,
        max_tick_id,
        raw_journal_sources,
        kafka_metadata_sources,
        connector_specs,
        run_args,
        definitions,
        source_id_by_name,
    } = config;
    let raw_entries = if raw_journal_sources.is_empty() {
        Vec::new()
    } else {
        source_batch_journal
            .load_committed_entries_up_to(max_tick_id, raw_journal_sources)
            .await?
    };
    let kafka_entries = if kafka_metadata_sources.is_empty() {
        Vec::new()
    } else {
        kafka_journal
            .load_committed_entries_up_to(max_tick_id, kafka_metadata_sources)
            .await?
    };

    let mut entries_by_tick = BTreeMap::<u64, ReplayTickEntries>::new();
    for entry in raw_entries {
        entries_by_tick
            .entry(entry.tick_id)
            .or_default()
            .raw
            .push(entry);
    }
    for entry in kafka_entries {
        entries_by_tick
            .entry(entry.tick_id)
            .or_default()
            .kafka
            .push(entry);
    }
    for entries in entries_by_tick.values_mut() {
        entries
            .raw
            .sort_by(|left, right| left.source.cmp(&right.source));
        entries
            .kafka
            .sort_by(|left, right| left.source.cmp(&right.source));
    }
    let shared_definitions: Arc<[SourceDefinition]> = Arc::from(definitions.to_vec());

    let mut replayed_raw = 0usize;
    let mut replayed_kafka = 0usize;
    for (tick_id, entries) in entries_by_tick {
        let mut tick_changed = false;
        for entry in entries.raw {
            replayed_raw = replayed_raw.saturating_add(1);
            for batch in entry.batches {
                vectorized_runtime
                    .apply_weighted_source_delta(&entry.source, batch)
                    .await
                    .with_context(|| {
                        format!(
                            "replay vectorized source journal for '{}' at tick {}",
                            entry.source, tick_id
                        )
                    })?;
                tick_changed = true;
            }
        }
        for entry in entries.kafka {
            replayed_kafka = replayed_kafka.saturating_add(1);
            let replayed_batches =
                replay_kafka_source_journal_entry_as_arrow(KafkaSourceJournalReplayConfig {
                    entry,
                    connector_specs,
                    run_args,
                    definitions: Arc::clone(&shared_definitions),
                    source_id_by_name,
                })
                .await?;
            for (source_name, batch) in replayed_batches {
                vectorized_runtime
                    .apply_weighted_source_delta(&source_name, batch)
                    .await
                    .with_context(|| {
                        format!(
                            "replay kafka metadata journal into vectorized source '{}' at tick {}",
                            source_name, tick_id
                        )
                    })?;
                tick_changed = true;
            }
        }
        if tick_changed {
            vectorized_runtime
                .run_tick(i64::try_from(tick_id).unwrap_or(i64::MAX))
                .await
                .with_context(|| format!("run vectorized replay tick {tick_id}"))?;
        }
    }
    Ok((replayed_raw, replayed_kafka))
}

#[derive(Default)]
struct ReplayTickEntries {
    raw: Vec<floe_executor::source_journal::VectorizedSourceBatchJournalEntry>,
    kafka: Vec<floe_executor::source_journal::KafkaSourceJournalEntry>,
}

pub(super) struct ReplayCommittedVectorizedSourceJournalConfig<'a> {
    pub(super) source_batch_journal: &'a VectorizedSourceBatchJournal,
    pub(super) kafka_journal: &'a KafkaSourceJournal,
    pub(super) vectorized_runtime: &'a mut VectorizedExecutionRuntime,
    pub(super) max_tick_id: u64,
    pub(super) raw_journal_sources: &'a BTreeSet<String>,
    pub(super) kafka_metadata_sources: &'a BTreeSet<String>,
    pub(super) connector_specs: &'a [config::ConnectorSpec],
    pub(super) run_args: &'a cli::RunArgs,
    pub(super) definitions: &'a [SourceDefinition],
    pub(super) source_id_by_name: &'a HashMap<String, usize>,
}

struct KafkaSourceJournalReplayConfig<'a> {
    entry: floe_executor::source_journal::KafkaSourceJournalEntry,
    connector_specs: &'a [config::ConnectorSpec],
    run_args: &'a cli::RunArgs,
    definitions: Arc<[SourceDefinition]>,
    source_id_by_name: &'a HashMap<String, usize>,
}

async fn replay_kafka_source_journal_entry_as_arrow(
    config: KafkaSourceJournalReplayConfig<'_>,
) -> anyhow::Result<Vec<(String, RecordBatch)>> {
    let KafkaSourceJournalReplayConfig {
        entry,
        connector_specs,
        run_args,
        definitions,
        source_id_by_name,
    } = config;
    let mut batches = Vec::new();
    for range in entry.ranges {
        let config = kafka_replay_connector_config(connector_specs, run_args, &range.topic)?;
        let replay_range = KafkaReplayRange {
            source: entry.source.clone(),
            tick_id: entry.tick_id,
            max_event_time_ms: entry.max_event_time_ms,
            topic: range.topic.clone(),
            partition: range.partition,
            start_offset: range.start_offset,
            end_offset: range.end_offset,
        };
        let replayed = KafkaConnector::replay_range(config, Arc::clone(&definitions), replay_range)
            .await
            .with_context(|| {
                format!(
                    "replay kafka range {}[{}] {}..{} for source '{}' tick {}",
                    range.topic,
                    range.partition,
                    range.start_offset,
                    range.end_offset,
                    entry.source,
                    entry.tick_id
                )
            })?;

        let source_id = source_id_by_name
            .get(entry.source.as_str())
            .copied()
            .ok_or_else(|| anyhow!("missing source id for '{}'", entry.source))?;
        let definition = definitions
            .get(source_id)
            .cloned()
            .ok_or_else(|| anyhow!("missing source definition for '{}'", entry.source))?;
        let mut builder =
            SourceArrowBatchBuilder::new(definition.clone(), replayed.events.len().max(1));
        let mut row_count = 0u64;
        let mut checksum = kafka_source_journal_initial_checksum();
        for event in replayed.events {
            let Some(event_source_id) = source_id_by_name.get(event.source()).copied() else {
                continue;
            };
            if event_source_id != source_id {
                continue;
            }
            let Some((topic, partition, offset)) = event_fast_kafka_offset(&event)
                .or_else(|| event_kafka_offset(event.resume_token()))
            else {
                return Err(anyhow!(
                    "replayed kafka event for source '{}' missing kafka position",
                    entry.source
                ));
            };
            if topic.as_ref() != range.topic.as_str()
                || partition != range.partition
                || offset < range.start_offset
                || offset > range.end_offset
            {
                return Err(anyhow!(
                    "replayed kafka event for source '{}' had unexpected position {}[{}] {}; expected {}[{}] {}..{}",
                    entry.source,
                    topic.as_ref(),
                    partition,
                    offset,
                    range.topic,
                    range.partition,
                    range.start_offset,
                    range.end_offset
                ));
            }
            let checksum_bytes = kafka_source_journal_event_checksum_bytes(&event);
            update_kafka_source_journal_checksum(&mut checksum, offset, &checksum_bytes);
            builder
                .append_event(&event)
                .with_context(|| format!("decode replayed kafka event for '{}'", entry.source))?;
            row_count = row_count.saturating_add(1);
        }
        if row_count != range.row_count || checksum != range.checksum {
            return Err(anyhow!(
                "kafka replay validation failed for source '{}' tick {} range {}[{}] {}..{}: expected rows/checksum {}/{:016x}, got {}/{:016x}",
                entry.source,
                entry.tick_id,
                range.topic,
                range.partition,
                range.start_offset,
                range.end_offset,
                range.row_count,
                range.checksum,
                row_count,
                checksum
            ));
        }
        let Some(batch) = builder.finish()? else {
            continue;
        };
        let weighted_schema = floe_executor::delta_consolidation::weighted_snapshot_schema(
            &definition.to_arrow_schema(),
        )?;
        let weighted =
            floe_executor::delta_consolidation::add_weight_column(&batch, &weighted_schema, 1)?;
        batches.push((entry.source.clone(), weighted));
    }
    Ok(batches)
}

fn kafka_replay_connector_config(
    connector_specs: &[config::ConnectorSpec],
    run_args: &cli::RunArgs,
    topic: &str,
) -> anyhow::Result<KafkaConnectorConfig> {
    for connector in connector_specs {
        let ConnectorConfig::Kafka {
            brokers,
            topics,
            group_id,
            default_source,
            poll_ms,
            max_messages_per_tick,
            format,
            ..
        } = &connector.config
        else {
            continue;
        };
        if !topics.iter().any(|candidate| candidate == topic) {
            continue;
        }
        let group_id = group_id
            .clone()
            .unwrap_or_else(|| run_args.kafka_group_id.clone());
        let poll_timeout = Duration::from_millis(poll_ms.unwrap_or(run_args.kafka_poll_ms));
        return Ok(KafkaConnectorConfig {
            brokers: brokers.clone(),
            topics: topics.clone(),
            group_id,
            default_source: default_source.clone(),
            poll_timeout,
            replay_idle_timeout: KafkaConnectorConfig::default_replay_idle_timeout(poll_timeout),
            max_messages_per_tick: max_messages_per_tick.unwrap_or(run_args.kafka_max_messages),
            message_format: format.clone(),
            commit_offsets_rx: None,
            resume_from_offsets: Vec::new(),
        });
    }
    Err(anyhow!(
        "no kafka connector configured for replay topic '{topic}'"
    ))
}
