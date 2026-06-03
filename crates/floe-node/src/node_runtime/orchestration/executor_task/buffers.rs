use super::*;

type SourceOffsetsByPartition = Option<HashMap<u32, u64>>;
type KafkaSourceRangesByPartition =
    Option<HashMap<(Arc<str>, i32), KafkaSourceJournalRangeAccumulator>>;

pub(super) struct ExecutorTickBuffers {
    pub(super) decoded_counts: Vec<usize>,
    pub(super) tick_source_offsets: Vec<SourceOffsetsByPartition>,
    pub(super) tick_kafka_offsets: HashMap<(Arc<str>, i32), i64>,
    pub(super) tick_kafka_source_ranges: Vec<KafkaSourceRangesByPartition>,
    pub(super) tick_postgres_lsns: HashMap<String, (u64, String)>,
    pub(super) tick_postgres_sources: HashMap<String, String>,
    pub(super) tick_postgres_table_lsns: Vec<(String, String, String, u64)>,
    pub(super) tick_source_max_event_ts: Vec<Option<i64>>,
    pub(super) arrow_batches_by_source: Vec<Vec<RecordBatch>>,
    pub(super) execution_arrow_batches_by_source: Vec<Vec<RecordBatch>>,
    pub(super) weighted_arrow_batches_by_source: Vec<Vec<RecordBatch>>,
    pub(super) vectorized_source_journal_batches: Vec<VectorizedSourceJournalTransientBatch>,
    pub(super) arrow_builders_by_source: Vec<Option<SourceArrowBatchBuilder>>,
    pub(super) commit_acks_by_source: Vec<Vec<core_source::CommitAck>>,
    pub(super) tick_commit_acks: Vec<core_source::CommitAck>,
    pub(super) per_connector_counts: Vec<usize>,
}

impl ExecutorTickBuffers {
    pub(super) fn new(
        active_source_definitions_by_id: &[Option<SourceDefinition>],
        required_columns_by_source_id: &[Option<Arc<[bool]>>],
        max_batch_per_source: usize,
        connector_count: usize,
    ) -> Self {
        let source_count = active_source_definitions_by_id.len();
        Self {
            decoded_counts: vec![0; source_count],
            tick_source_offsets: (0..source_count).map(|_| None).collect(),
            tick_kafka_offsets: HashMap::new(),
            tick_kafka_source_ranges: (0..source_count).map(|_| None).collect(),
            tick_postgres_lsns: HashMap::new(),
            tick_postgres_sources: HashMap::new(),
            tick_postgres_table_lsns: Vec::new(),
            tick_source_max_event_ts: vec![None; source_count],
            arrow_batches_by_source: (0..source_count).map(|_| Vec::new()).collect(),
            execution_arrow_batches_by_source: (0..source_count).map(|_| Vec::new()).collect(),
            weighted_arrow_batches_by_source: (0..source_count).map(|_| Vec::new()).collect(),
            vectorized_source_journal_batches: Vec::new(),
            arrow_builders_by_source: active_source_definitions_by_id
                .iter()
                .enumerate()
                .map(|(source_id, definition)| {
                    definition.as_ref().map(|definition| {
                        SourceArrowBatchBuilder::new_with_required_columns(
                            definition.clone(),
                            max_batch_per_source,
                            required_columns_by_source_id
                                .get(source_id)
                                .and_then(Clone::clone),
                        )
                    })
                })
                .collect(),
            commit_acks_by_source: (0..source_count).map(|_| Vec::new()).collect(),
            tick_commit_acks: Vec::new(),
            per_connector_counts: vec![0; connector_count],
        }
    }

    pub(super) fn reset_for_tick(&mut self) {
        self.decoded_counts.fill(0);
        for offsets in &mut self.tick_source_offsets {
            *offsets = None;
        }
        self.tick_kafka_offsets.clear();
        for ranges in &mut self.tick_kafka_source_ranges {
            *ranges = None;
        }
        self.tick_postgres_lsns.clear();
        self.tick_postgres_sources.clear();
        self.tick_postgres_table_lsns.clear();
        self.tick_source_max_event_ts.fill(None);
        for batches in &mut self.arrow_batches_by_source {
            batches.clear();
        }
        for batches in &mut self.execution_arrow_batches_by_source {
            batches.clear();
        }
        for batches in &mut self.weighted_arrow_batches_by_source {
            batches.clear();
        }
        self.vectorized_source_journal_batches.clear();
        for acks in &mut self.commit_acks_by_source {
            acks.clear();
        }
        self.tick_commit_acks.clear();
        self.per_connector_counts.fill(0);
    }
}
