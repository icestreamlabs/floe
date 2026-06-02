use super::super::*;

pub(in crate::node_runtime) fn source_journal_required_sources(
    registry: &SourceRegistry,
    transient_only_sources: &BTreeSet<String>,
    mode: SourceJournalConfig,
) -> BTreeSet<String> {
    match mode {
        SourceJournalConfig::Full => transient_only_sources.clone(),
        SourceJournalConfig::None => BTreeSet::new(),
        SourceJournalConfig::Auto => transient_only_sources
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
    transient_only_sources: &BTreeSet<String>,
    mode: SourceJournalConfig,
) -> BTreeSet<String> {
    if mode != SourceJournalConfig::Auto {
        return BTreeSet::new();
    }
    transient_only_sources
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

#[allow(clippy::too_many_arguments)]
pub(super) async fn replay_committed_vectorized_source_journal_entries(
    source_batch_journal: &VectorizedSourceBatchJournal,
    kafka_journal: &KafkaSourceJournal,
    vectorized_runtime: &mut VectorizedExecutionRuntime,
    max_tick_id: u64,
    raw_journal_sources: &BTreeSet<String>,
    kafka_metadata_sources: &BTreeSet<String>,
    connector_specs: &[config::ConnectorSpec],
    run_args: &cli::RunArgs,
    definitions: &[SourceDefinition],
    source_id_by_name: &HashMap<String, usize>,
) -> anyhow::Result<(usize, usize)> {
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

    let mut raw_entry_by_tick_and_source = BTreeMap::new();
    for entry in raw_entries {
        raw_entry_by_tick_and_source.insert((entry.tick_id, entry.source.clone()), entry);
    }
    let mut kafka_entry_by_tick_and_source = BTreeMap::new();
    for entry in kafka_entries {
        kafka_entry_by_tick_and_source.insert((entry.tick_id, entry.source.clone()), entry);
    }

    let replay_sources: BTreeSet<String> = raw_journal_sources
        .union(kafka_metadata_sources)
        .cloned()
        .collect();
    let mut replayed_raw = 0usize;
    let mut replayed_kafka = 0usize;
    for tick_id in 1..=max_tick_id {
        let mut tick_changed = false;
        for source in &replay_sources {
            if raw_journal_sources.contains(source) {
                let Some(entry) = raw_entry_by_tick_and_source.remove(&(tick_id, source.clone()))
                else {
                    continue;
                };
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
            } else if kafka_metadata_sources.contains(source) {
                let Some(entry) = kafka_entry_by_tick_and_source.remove(&(tick_id, source.clone()))
                else {
                    continue;
                };
                replayed_kafka = replayed_kafka.saturating_add(1);
                let replayed_batches = replay_kafka_source_journal_entry_as_arrow(
                    entry,
                    connector_specs,
                    run_args,
                    definitions,
                    source_id_by_name,
                )
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

#[allow(clippy::too_many_arguments)]
async fn replay_kafka_source_journal_entry_as_arrow(
    entry: floe_executor::source_journal::KafkaSourceJournalEntry,
    connector_specs: &[config::ConnectorSpec],
    run_args: &cli::RunArgs,
    definitions: &[SourceDefinition],
    source_id_by_name: &HashMap<String, usize>,
) -> anyhow::Result<Vec<(String, RecordBatch)>> {
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
        let replayed = KafkaConnector::replay_range(
            config,
            definitions.to_vec(),
            HashMap::new(),
            replay_range,
        )
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
        return Ok(KafkaConnectorConfig {
            brokers: brokers.clone(),
            topics: topics.clone(),
            group_id,
            default_source: default_source.clone(),
            poll_timeout: Duration::from_millis(poll_ms.unwrap_or(run_args.kafka_poll_ms)),
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
