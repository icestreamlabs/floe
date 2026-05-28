use std::path::PathBuf;

fn repo_file(path: &str) -> String {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root")
        .to_path_buf();
    std::fs::read_to_string(root.join(path)).expect("read source file")
}

#[test]
fn source_filter_project_compilation_uses_async_batch_evaluators() {
    let source_map =
        repo_file("crates/floe-executor/src/dbsp_graph_builder/compile/source_map_phase.rs");

    assert!(!source_map.contains("DbspFilterMap::new_batch"));
    assert!(!source_map.contains(".transform_delta("));
    assert_eq!(
        source_map.matches("DbspFilterMap::new_async_batch").count(),
        3
    );
    assert_eq!(source_map.matches("transform_delta_arrow").count(), 3);
}

#[test]
fn joins_use_batch_key_paths_and_vectorized_key_extraction() {
    let join_phase = repo_file("crates/floe-executor/src/dbsp_graph_builder/compile/join_phase.rs");

    assert!(!join_phase.contains("DbspJoin::new_with_state_namespace"));
    assert!(!join_phase.contains("extract_encoded_row_columns"));
    assert!(join_phase.contains("DbspJoin::new_batch_with_state_namespace"));
    assert!(join_phase.contains("DbspJoin::spawn_transient_with_inputs_and_retention"));
    assert!(join_phase.contains("VectorizedEncodedKeyExtractor"));
    assert!(join_phase.matches("extract_keyed_deltas").count() >= 6);
}

#[test]
fn topn_compilation_uses_batch_key_paths() {
    let topn_phase =
        repo_file("crates/floe-executor/src/dbsp_graph_builder/compile/set_ops_topn_phase.rs");
    let transient_topn =
        repo_file("crates/floe-executor/src/dbsp_graph_builder/builder/transient_topn.rs");

    assert!(!topn_phase.contains("new_with_key_extractor::<"));
    assert!(!topn_phase.contains("extract_encoded_row_columns"));
    assert!(!topn_phase.contains("extract_encoded_row_scalar"));
    assert!(topn_phase.contains("DbspTopN::new_with_batch_key_extractor"));
    assert!(topn_phase.contains("DbspPartitionedTop1::new_with_batch_key_extractor"));
    assert!(topn_phase.contains("VectorizedTopNKeyParts"));
    assert!(topn_phase.contains("DeltaBatchBuffer::new"));
    assert!(!transient_topn.contains("extract_encoded_row_columns"));
    assert!(!transient_topn.contains("extract_encoded_row_scalar"));
    assert!(transient_topn.contains("TransientTopNKeyExtractor"));
    assert!(transient_topn.contains("DeltaBatchBuffer::new_projected"));
}

#[test]
fn aggregates_and_windows_use_batch_key_paths() {
    let aggregate_phase =
        repo_file("crates/floe-executor/src/dbsp_graph_builder/compile/aggregate_window_phase.rs");
    let vectorized_keys = repo_file("crates/floe-executor/src/vectorized_keys.rs");

    assert!(!aggregate_phase.contains("DbspAggregate::new::<"));
    assert!(!aggregate_phase.contains("DbspWindowAggregate::new::<"));
    assert!(aggregate_phase.contains("DbspAggregate::new_batch"));
    assert!(aggregate_phase.contains("DbspWindowAggregate::new_with_batch_extractor"));
    assert!(aggregate_phase.contains("VectorizedEncodedKeyExtractor"));
    assert!(aggregate_phase.contains("evaluate_count_batch_row_values"));
    assert!(aggregate_phase.contains("evaluate_incremental_aggregate_batch_row_values"));
    assert!(aggregate_phase.contains("build_window_count_batch_row_evaluator"));
    assert!(aggregate_phase.contains("build_window_incremental_aggregate_batch_row_evaluator"));
    assert!(aggregate_phase.contains("build_prekeyed_incremental_aggregate_batch_row_evaluator"));
    assert!(aggregate_phase.contains("count_eval_record_batch("));
    assert!(aggregate_phase.contains("encoded_scalar_from_arrow_array"));
    assert!(aggregate_phase.contains("#[cfg(test)]\npub(crate) fn build_count_row_evaluator"));
    assert!(
        aggregate_phase
            .contains("#[cfg(test)]\npub(crate) fn build_incremental_aggregate_row_evaluator")
    );
    assert!(aggregate_phase.matches("extract_keyed_time_deltas").count() >= 5);
    assert!(aggregate_phase.matches("extract_keyed_deltas").count() >= 2);
    assert!(vectorized_keys.contains("DeltaBatchBuffer::new_projected"));
    assert!(vectorized_keys.contains("DeltaBatchBuffer::new_keyed"));
    assert!(!vectorized_keys.contains("extract_encoded_row_columns"));
    assert!(!vectorized_keys.contains("encode_primary_key(&row"));
    assert!(vectorized_keys.contains("projected_arrow_schema"));
}

#[test]
fn scalar_filter_join_and_semijoin_constructors_are_not_production_apis() {
    let filter_map = repo_file("crates/dbsp-runtime/src/filter_map.rs");
    let join = repo_file("crates/dbsp-runtime/src/join.rs");
    let join_op = repo_file("crates/dbsp-runtime/src/operators/join/op.rs");
    let semijoin = repo_file("crates/dbsp-runtime/src/semijoin.rs");
    let semijoin_op = repo_file("crates/dbsp-runtime/src/operators/semijoin.rs");
    let topn = repo_file("crates/dbsp-runtime/src/topn.rs");
    let top1 = repo_file("crates/dbsp-runtime/src/top1.rs");
    let aggregate = repo_file("crates/dbsp-runtime/src/aggregate.rs");
    let aggregate_op = repo_file("crates/dbsp-runtime/src/operators/aggregate.rs");
    let count_aggregate = repo_file("crates/dbsp-runtime/src/count_aggregate.rs");
    let incremental_aggregate = repo_file("crates/dbsp-runtime/src/incremental_aggregate.rs");
    let window = repo_file("crates/dbsp-runtime/src/window.rs");
    let window_op = repo_file("crates/dbsp-runtime/src/operators/window.rs");
    let window_count = repo_file("crates/dbsp-runtime/src/window_count_aggregate.rs");
    let window_count_star = repo_file("crates/dbsp-runtime/src/window_count_star_aggregate.rs");

    assert!(filter_map.contains("#[cfg(test)]\n    pub async fn new"));
    assert!(join.contains("#[cfg(test)]\n    pub async fn new<"));
    assert!(join.contains("#[cfg(test)]\n    pub async fn new_with_state_namespace"));
    assert!(join_op.contains("#[cfg(test)]\n    pub fn new("));
    assert!(semijoin.contains("#[cfg(test)]\n    pub async fn new<"));
    assert!(semijoin_op.contains("#[cfg(test)]\n    pub fn new("));
    assert!(topn.contains("#[cfg(test)]\n    pub async fn new<"));
    assert!(topn.contains("#[cfg(test)]\n    pub async fn new_with_key_extractor"));
    assert!(top1.contains("#[cfg(test)]\n    pub async fn new_with_key_extractor"));
    assert!(aggregate.contains("#[cfg(test)]\n    pub async fn new<"));
    assert!(aggregate_op.contains("#[cfg(test)]\n    pub fn new("));
    assert!(count_aggregate.contains("#[cfg(test)]\n    pub async fn new<"));
    assert!(incremental_aggregate.contains("#[cfg(test)]\n    pub async fn new<"));
    assert!(window.contains("#[cfg(test)]\n    pub async fn new<"));
    assert!(window_op.contains("#[cfg(test)]\n    pub fn new("));
    assert!(window_count.contains("#[cfg(test)]\n    pub async fn new<"));
    assert!(window_count_star.contains("#[cfg(test)]\n    pub async fn new<"));
}

#[test]
fn production_transient_delta_transforms_are_async() {
    let materialize = repo_file("crates/floe-executor/src/dbsp_graph_builder/materialize.rs");
    let builder = repo_file("crates/floe-executor/src/dbsp_graph_builder/builder.rs");
    let transient_segment =
        repo_file("crates/floe-executor/src/dbsp_graph_builder/builder/transient_segment.rs");

    assert!(materialize.contains("BoxFuture<'static, Result<Vec<(Vec<u8>, i64)>>>"));
    assert!(builder.contains("identity_delta_transform"));
    assert!(transient_segment.contains("transform_delta_arrow"));
    assert!(!transient_segment.contains(".transform_delta("));
}

#[test]
fn mv_scan_and_storage_materialization_use_batch_arrow_boundaries() {
    let encoded_batch = repo_file("crates/floe-executor/src/encoded_batch.rs");
    let table_helpers = repo_file("crates/floe-executor/src/table_provider/helpers.rs");
    let mv_changelog = repo_file("crates/floe-executor/src/mv_changelog.rs");
    let zset_storage = repo_file("crates/dbsp-runtime/src/collections/zset/versioned/storage.rs");

    assert!(encoded_batch.contains("build_expanded_batches_from_encoded_rows"));
    assert!(encoded_batch.contains("DeltaBatchBuffer::new_projected"));
    assert!(encoded_batch.contains("take(array.as_ref(), &take_indices, None)"));
    assert!(table_helpers.contains("build_expanded_batches_from_encoded_rows"));
    assert!(!table_helpers.contains("extract_encoded_row_scalars"));
    assert!(!mv_changelog.contains("decode_all_encoded_row_scalars_into"));
    assert!(mv_changelog.contains("EncodedRowBatchMode::Snapshot"));
    assert!(mv_changelog.contains("EncodedRowBatchMode::Delta"));
    assert!(zset_storage.contains("resolve_many(&missing_ids)"));
    assert!(zset_storage.contains("apply_id_deltas_to_aggregate"));
}

#[test]
fn source_journal_uses_arrow_batch_payloads() {
    let source_journal = repo_file("crates/floe-executor/src/source_journal.rs");

    assert!(source_journal.contains("SOURCE_BATCH_JOURNAL_ARROW_MAGIC"));
    assert!(source_journal.contains("BinaryArray::from_iter_values"));
    assert!(source_journal.contains("Int64Array::from_iter_values"));
    assert!(source_journal.contains("StreamWriter::try_new"));
    assert!(source_journal.contains("StreamReader::try_new"));
    assert!(source_journal.contains("decode_legacy_entry"));
}
