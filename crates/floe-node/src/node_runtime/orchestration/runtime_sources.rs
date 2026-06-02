use super::*;

pub(super) struct RuntimeSourceIndexes {
    pub(super) source_names_by_id: Arc<Vec<String>>,
    pub(super) active_source_definitions_by_id: Arc<Vec<Option<SourceDefinition>>>,
    pub(super) materialized_source_ids: Arc<Vec<bool>>,
    pub(super) kafka_metadata_journal_source_ids: Arc<Vec<usize>>,
    pub(super) source_journal_required_sources_for_task: Arc<BTreeSet<String>>,
    pub(super) cdc_schemas_by_source_id:
        Arc<HashMap<CdcSourceId, HashMap<CdcTableId, CdcTableSchema>>>,
    pub(super) cdc_stateful_table_ids_by_source_id: Arc<HashMap<CdcSourceId, HashSet<CdcTableId>>>,
}

pub(super) fn build_runtime_source_indexes(
    definitions: &[SourceDefinition],
    all_required_sources: &BTreeSet<String>,
    kafka_metadata_journal_required_sources: &BTreeSet<String>,
    source_journal_required_sources: &BTreeSet<String>,
    postgres_cdc_runtime_plans_by_connector: &HashMap<String, PostgresCdcRuntimePlan>,
) -> RuntimeSourceIndexes {
    RuntimeSourceIndexes {
        source_names_by_id: Arc::new(
            definitions
                .iter()
                .map(|definition| definition.name().to_string())
                .collect(),
        ),
        active_source_definitions_by_id: Arc::new(
            definitions
                .iter()
                .map(|definition| {
                    all_required_sources
                        .contains(definition.name())
                        .then_some(definition.clone())
                })
                .collect(),
        ),
        materialized_source_ids: Arc::new(
            definitions
                .iter()
                .map(|definition| all_required_sources.contains(definition.name()))
                .collect(),
        ),
        kafka_metadata_journal_source_ids: Arc::new(
            definitions
                .iter()
                .enumerate()
                .filter_map(|(idx, definition)| {
                    kafka_metadata_journal_required_sources
                        .contains(definition.name())
                        .then_some(idx)
                })
                .collect(),
        ),
        source_journal_required_sources_for_task: Arc::new(source_journal_required_sources.clone()),
        cdc_schemas_by_source_id: Arc::new(
            postgres_cdc_runtime_plans_by_connector
                .values()
                .map(|plan| (plan.source_id.clone(), plan.schemas.clone()))
                .collect(),
        ),
        cdc_stateful_table_ids_by_source_id: Arc::new(
            postgres_cdc_runtime_plans_by_connector
                .values()
                .map(|plan| {
                    (
                        plan.source_id.clone(),
                        plan.schemas.keys().cloned().collect(),
                    )
                })
                .collect(),
        ),
    }
}
