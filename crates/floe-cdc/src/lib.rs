mod apply;
mod codec;
mod deltas;
mod json;
mod keys;
mod metadata;

pub use apply::CdcTableStore;
pub use deltas::{CdcApplyResult, CdcRowDelta, CdcTableDeltas};
pub use metadata::CdcMetadataStore;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::codec::{CDC_ROW_STATE_MAGIC, decode_cdc_row_state, encode_cdc_row_state};
    use crate::keys::row_key_bytes;
    use dbsp_storage::storage::{KeyValueTable, SlateTable};
    use floe_cdc_core::{
        CdcChange, CdcCheckpoint, CdcColumn, CdcColumnarColumn, CdcColumnarRowBatch, CdcPrimaryKey,
        CdcRow, CdcRowKey, CdcSourceDefinition, CdcSourceId, CdcSourcePosition, CdcTableDefinition,
        CdcTableId, CdcTableSchema, CdcTransactionId, ChangeBatch, TransactionBatch,
        UpstreamTableRef,
    };
    use floe_core::RowValue;
    use floe_core::catalog::ColumnType;
    use object_store::memory::InMemory;
    use slatedb::Db;
    use slatedb::WriteBatch;

    async fn test_table(name: &str) -> Arc<dyn KeyValueTable> {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(name, object_store).await.expect("open SlateDB"));
        Arc::new(SlateTable::new(db))
    }

    async fn test_store(name: &str) -> CdcTableStore {
        CdcTableStore::new(test_table(name).await)
    }

    fn orders_schema() -> CdcTableSchema {
        CdcTableSchema::new(
            CdcTableId::new("orders").expect("table id"),
            UpstreamTableRef::new("public", "orders").expect("upstream"),
            vec![
                CdcColumn::new("id", ColumnType::Int64, false).expect("id"),
                CdcColumn::new("amount", ColumnType::Int64, true).expect("amount"),
                CdcColumn::new("status", ColumnType::Utf8, true).expect("status"),
            ],
            CdcPrimaryKey::new(["id"]).expect("primary key"),
        )
        .expect("schema")
    }

    fn schemas(schema: CdcTableSchema) -> HashMap<CdcTableId, CdcTableSchema> {
        HashMap::from([(schema.table_id().clone(), schema)])
    }

    fn row(id: i64, amount: Option<i64>, status: Option<&str>) -> CdcRow {
        CdcRow::new([
            Some(RowValue::Int64(id)),
            amount.map(RowValue::Int64),
            status.map(|value| RowValue::Utf8(value.to_string())),
        ])
        .expect("row")
    }

    fn key(id: i64) -> CdcRowKey {
        CdcRowKey::new([RowValue::Int64(id)]).expect("row key")
    }

    fn tx(position: &str, batches: Vec<ChangeBatch>) -> TransactionBatch {
        TransactionBatch::new(
            CdcSourceId::new("pg_main").expect("source id"),
            Some(CdcTransactionId::new(format!("tx-{position}")).expect("txid")),
            None,
            CdcSourcePosition::postgres(position, None).expect("position"),
            batches,
        )
        .expect("transaction")
    }

    #[test]
    fn binary_row_state_codec_round_trips_all_value_types() {
        let source = CdcRow::new([
            Some(RowValue::Int64(7)),
            Some(RowValue::Bool(true)),
            Some(RowValue::Utf8("paid".to_string())),
            Some(RowValue::TimestampMillis(1_700_000_000_000)),
            None,
        ])
        .expect("row");

        let encoded = encode_cdc_row_state(&source).expect("encode row state");
        assert!(encoded.starts_with(CDC_ROW_STATE_MAGIC));
        assert_eq!(
            decode_cdc_row_state(&encoded).expect("decode row state"),
            source
        );
    }

    #[tokio::test]
    async fn applies_columnar_snapshot_insert_batch() {
        let store = test_store("cdc-table-columnar-snapshot").await;
        let schema = orders_schema();
        let rows = CdcColumnarRowBatch::new(vec![
            CdcColumnarColumn::Int64(vec![Some(1), Some(2)]),
            CdcColumnarColumn::Int64(vec![Some(10), None]),
            CdcColumnarColumn::Utf8(vec![Some("open".to_string()), Some("closed".to_string())]),
        ])
        .expect("columnar rows");
        let transaction = tx(
            "0/10",
            vec![
                ChangeBatch::new_snapshot_insert(schema.table_id().clone(), rows)
                    .expect("snapshot batch"),
            ],
        );

        let apply_result = store
            .apply_transaction(&schemas(schema.clone()), &transaction)
            .await
            .expect("apply columnar snapshot");
        assert_eq!(apply_result.table_deltas().len(), 1);
        assert_eq!(apply_result.table_deltas()[0].row_count(), 2);
        assert_eq!(
            apply_result.table_deltas()[0]
                .snapshot_insert_rows()
                .expect("snapshot rows")
                .row_count(),
            2
        );
        assert!(apply_result.table_deltas()[0].deltas().is_empty());

        assert_eq!(
            store
                .load_row(schema.table_id(), &key(1))
                .await
                .expect("load first row")
                .expect("first row exists")
                .values(),
            &[
                Some(RowValue::Int64(1)),
                Some(RowValue::Int64(10)),
                Some(RowValue::Utf8("open".to_string()))
            ]
        );
        assert_eq!(
            store
                .load_row(schema.table_id(), &key(2))
                .await
                .expect("load second row")
                .expect("second row exists")
                .values(),
            &[
                Some(RowValue::Int64(2)),
                None,
                Some(RowValue::Utf8("closed".to_string()))
            ]
        );
    }

    #[tokio::test]
    async fn metadata_round_trips_sources_tables_and_checkpoints() {
        let table = test_table("cdc-metadata-round-trip").await;
        let metadata = CdcMetadataStore::new(Arc::clone(&table));
        let apply_store = CdcTableStore::new(Arc::clone(&table));
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let source = CdcSourceDefinition::postgres(source_id.clone())
            .expect("source")
            .with_property("slot.name", "floe_slot")
            .expect("slot property")
            .with_property("publication.name", "floe_publication")
            .expect("publication property");
        metadata
            .upsert_source(&source)
            .await
            .expect("persist source");

        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let table_definition = CdcTableDefinition::new(source_id.clone(), schema.clone());
        metadata
            .upsert_table(&table_definition)
            .await
            .expect("persist table");

        let transaction = tx(
            "0/50",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![CdcChange::Insert {
                        row: row(50, Some(5000), Some("open")),
                    }],
                )
                .expect("batch"),
            ],
        );
        let checkpoint = apply_store
            .apply_transaction(&schemas(schema), &transaction)
            .await
            .expect("apply transaction")
            .checkpoint()
            .clone();

        let reloaded_metadata = CdcMetadataStore::new(Arc::clone(&table));
        let reloaded_apply_store = CdcTableStore::new(table);
        assert_eq!(
            reloaded_metadata
                .load_source(&source_id)
                .await
                .expect("load source"),
            Some(source.clone())
        );
        assert_eq!(
            reloaded_metadata.sources().await.expect("load sources"),
            vec![source]
        );
        assert_eq!(
            reloaded_metadata
                .load_table(&table_id)
                .await
                .expect("load table"),
            Some(table_definition.clone())
        );
        assert_eq!(
            reloaded_metadata
                .tables_for_source(&source_id)
                .await
                .expect("load source tables"),
            vec![table_definition]
        );
        assert_eq!(
            reloaded_apply_store
                .load_checkpoint(&source_id)
                .await
                .expect("load checkpoint"),
            Some(checkpoint)
        );
    }

    #[tokio::test]
    async fn explicit_checkpoint_commit_round_trips_without_rows() {
        let store = test_store("cdc-explicit-checkpoint").await;
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let checkpoint = CdcCheckpoint::new(
            source_id.clone(),
            CdcSourcePosition::postgres("0/70", None).expect("position"),
            Some(CdcTransactionId::new("snapshot-0-70").expect("transaction id")),
        );

        store
            .commit_checkpoint(&checkpoint)
            .await
            .expect("commit checkpoint");

        assert_eq!(
            store
                .load_checkpoint(&source_id)
                .await
                .expect("load checkpoint"),
            Some(checkpoint)
        );
    }

    #[tokio::test]
    async fn table_metadata_rejects_missing_source_and_moves_source_index() {
        let table = test_table("cdc-metadata-index").await;
        let metadata = CdcMetadataStore::new(table);
        let pg_main = CdcSourceId::new("pg_main").expect("source id");
        let pg_other = CdcSourceId::new("pg_other").expect("source id");
        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let table_definition = CdcTableDefinition::new(pg_main.clone(), schema.clone());

        let err = metadata
            .upsert_table(&table_definition)
            .await
            .expect_err("table should require source metadata first");
        assert!(format!("{err:#}").contains("does not exist"));

        metadata
            .upsert_source(&CdcSourceDefinition::postgres(pg_main.clone()).expect("source"))
            .await
            .expect("persist main source");
        metadata
            .upsert_source(&CdcSourceDefinition::postgres(pg_other.clone()).expect("source"))
            .await
            .expect("persist other source");
        metadata
            .upsert_table(&table_definition)
            .await
            .expect("persist table on main source");
        assert_eq!(
            metadata
                .tables_for_source(&pg_main)
                .await
                .expect("main tables")
                .len(),
            1
        );

        let moved = CdcTableDefinition::new(pg_other.clone(), schema);
        metadata
            .upsert_table(&moved)
            .await
            .expect("move table to other source");
        assert!(
            metadata
                .tables_for_source(&pg_main)
                .await
                .expect("main tables")
                .is_empty()
        );
        assert_eq!(
            metadata
                .tables_for_source(&pg_other)
                .await
                .expect("other tables"),
            vec![moved.clone()]
        );
        assert_eq!(
            metadata.load_table(&table_id).await.expect("load table"),
            Some(moved)
        );
    }

    #[tokio::test]
    async fn applies_insert_update_and_delete_with_atomic_checkpoint() {
        let store = test_store("cdc-apply-insert-update-delete").await;
        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let insert_row = row(1, Some(100), Some("open"));
        let update_row = row(1, Some(150), Some("paid"));

        let insert = tx(
            "0/1",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![CdcChange::Insert {
                        row: insert_row.clone(),
                    }],
                )
                .expect("insert batch"),
            ],
        );
        let result = store
            .apply_transaction(&schemas(schema.clone()), &insert)
            .await
            .expect("apply insert");
        assert!(!result.already_committed());
        assert_eq!(result.table_deltas()[0].deltas()[0].diff(), 1);
        assert_eq!(
            store.load_row(&table_id, &key(1)).await.expect("load row"),
            Some(insert_row.clone())
        );
        assert_eq!(
            store
                .load_checkpoint(insert.source_id())
                .await
                .expect("load checkpoint"),
            Some(result.checkpoint().clone())
        );

        let update = tx(
            "0/2",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![CdcChange::Update {
                        key: Some(key(1)),
                        before: None,
                        after: update_row.clone(),
                    }],
                )
                .expect("update batch"),
            ],
        );
        let result = store
            .apply_transaction(&schemas(schema.clone()), &update)
            .await
            .expect("apply update");
        assert_eq!(result.table_deltas()[0].deltas().len(), 2);
        assert_eq!(result.table_deltas()[0].deltas()[0].diff(), -1);
        assert_eq!(result.table_deltas()[0].deltas()[1].diff(), 1);
        assert_eq!(
            store
                .load_row(&table_id, &key(1))
                .await
                .expect("load updated row"),
            Some(update_row)
        );

        let delete = tx(
            "0/3",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![CdcChange::Delete {
                        key: Some(key(1)),
                        before: None,
                    }],
                )
                .expect("delete batch"),
            ],
        );
        let result = store
            .apply_transaction(&schemas(schema), &delete)
            .await
            .expect("apply delete");
        assert_eq!(result.table_deltas()[0].deltas()[0].diff(), -1);
        assert_eq!(
            store
                .load_row(&table_id, &key(1))
                .await
                .expect("load deleted row"),
            None
        );
    }

    #[tokio::test]
    async fn resolves_unchanged_toast_columns_from_previous_row() {
        let store = test_store("cdc-resolve-unchanged-toast").await;
        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let original = row(1, Some(100), Some("large-note"));
        let insert = tx(
            "0/1",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![CdcChange::Insert {
                        row: original.clone(),
                    }],
                )
                .expect("insert batch"),
            ],
        );
        store
            .apply_transaction(&schemas(schema.clone()), &insert)
            .await
            .expect("apply insert");

        let unresolved_after = CdcRow::with_unchanged_toast_indices(
            [Some(RowValue::Int64(1)), Some(RowValue::Int64(150)), None],
            [2],
        )
        .expect("unresolved toast row");
        let update = tx(
            "0/2",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![CdcChange::Update {
                        key: Some(key(1)),
                        before: None,
                        after: unresolved_after,
                    }],
                )
                .expect("update batch"),
            ],
        );
        let result = store
            .apply_transaction(&schemas(schema), &update)
            .await
            .expect("apply toast update");
        let expected = row(1, Some(150), Some("large-note"));

        assert_eq!(
            store
                .load_row(&table_id, &key(1))
                .await
                .expect("load resolved row"),
            Some(expected.clone())
        );
        assert_eq!(result.table_deltas()[0].deltas()[0].row(), &original);
        assert_eq!(result.table_deltas()[0].deltas()[1].row(), &expected);
    }

    #[tokio::test]
    async fn row_state_uses_binary_codec_and_reads_legacy_json() {
        let table = test_table("cdc-row-state-binary").await;
        let store = CdcTableStore::new(Arc::clone(&table));
        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let binary_row = row(1, Some(100), Some("open"));
        let transaction = tx(
            "0/5",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![CdcChange::Insert {
                        row: binary_row.clone(),
                    }],
                )
                .expect("batch"),
            ],
        );
        store
            .apply_transaction(&schemas(schema), &transaction)
            .await
            .expect("apply binary row");

        let binary_key = row_key_bytes(&table_id, &key(1)).expect("row key");
        let binary_bytes = table
            .get(&binary_key)
            .await
            .expect("load raw binary row")
            .expect("binary row should exist");
        assert!(binary_bytes.starts_with(CDC_ROW_STATE_MAGIC));
        assert_ne!(binary_bytes.first(), Some(&b'{'));
        assert_eq!(
            store.load_row(&table_id, &key(1)).await.expect("load row"),
            Some(binary_row)
        );

        let legacy_row = row(88, None, Some("legacy"));
        let legacy_key = row_key_bytes(&table_id, &key(88)).expect("legacy row key");
        let mut batch = WriteBatch::new();
        batch.put(
            legacy_key,
            serde_json::to_vec(&legacy_row).expect("legacy JSON row"),
        );
        table.write_batch(batch).await.expect("write legacy row");
        assert_eq!(
            store
                .load_row(&table_id, &key(88))
                .await
                .expect("load legacy row"),
            Some(legacy_row)
        );
    }

    #[tokio::test]
    async fn overlay_handles_multiple_changes_for_same_key_in_one_transaction() {
        let store = test_store("cdc-apply-overlay").await;
        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let transaction = tx(
            "0/10",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![
                        CdcChange::Insert {
                            row: row(5, Some(10), Some("open")),
                        },
                        CdcChange::Update {
                            key: Some(key(5)),
                            before: None,
                            after: row(5, Some(20), Some("paid")),
                        },
                        CdcChange::Delete {
                            key: Some(key(5)),
                            before: None,
                        },
                    ],
                )
                .expect("batch"),
            ],
        );

        let result = store
            .apply_transaction(&schemas(schema), &transaction)
            .await
            .expect("apply transaction");
        let diffs: Vec<i64> = result.table_deltas()[0]
            .deltas()
            .iter()
            .map(CdcRowDelta::diff)
            .collect();
        assert_eq!(diffs, vec![1, -1, 1, -1]);
        assert_eq!(
            store.load_row(&table_id, &key(5)).await.expect("load row"),
            None
        );
    }

    #[tokio::test]
    async fn exact_checkpoint_reapply_is_idempotent() {
        let store = test_store("cdc-apply-idempotent").await;
        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let transaction = tx(
            "0/20",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![CdcChange::Insert {
                        row: row(9, Some(90), Some("open")),
                    }],
                )
                .expect("batch"),
            ],
        );

        store
            .apply_transaction(&schemas(schema.clone()), &transaction)
            .await
            .expect("first apply");
        let replay = store
            .apply_transaction(&schemas(schema), &transaction)
            .await
            .expect("reapply");
        assert!(replay.already_committed());
        assert!(replay.table_deltas().is_empty());
        assert_eq!(
            store.load_row(&table_id, &key(9)).await.expect("load row"),
            Some(row(9, Some(90), Some("open")))
        );
    }

    #[tokio::test]
    async fn stale_checkpoint_replay_is_ignored_without_rewinding_state() {
        let store = test_store("cdc-apply-stale-replay").await;
        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let newer = tx(
            "0/20",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![CdcChange::Insert {
                        row: row(20, Some(200), Some("newer")),
                    }],
                )
                .expect("batch"),
            ],
        );
        let newer_checkpoint = store
            .apply_transaction(&schemas(schema.clone()), &newer)
            .await
            .expect("apply newer")
            .checkpoint()
            .clone();

        let stale = tx(
            "0/10",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![CdcChange::Insert {
                        row: row(10, Some(100), Some("stale")),
                    }],
                )
                .expect("batch"),
            ],
        );
        let replay = store
            .apply_transaction(&schemas(schema), &stale)
            .await
            .expect("ignore stale replay");
        assert!(replay.already_committed());
        assert_eq!(replay.checkpoint(), &newer_checkpoint);
        assert!(replay.table_deltas().is_empty());
        assert_eq!(
            store.load_row(&table_id, &key(10)).await.expect("load row"),
            None
        );
        assert_eq!(
            store
                .load_checkpoint(stale.source_id())
                .await
                .expect("load checkpoint"),
            Some(newer_checkpoint)
        );
    }

    #[tokio::test]
    async fn fresh_store_reloads_checkpoint_and_rows_from_slate_table() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(
            Db::open("cdc-apply-reload", object_store)
                .await
                .expect("open SlateDB"),
        );
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
        let store = CdcTableStore::new(Arc::clone(&table));
        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let transaction = tx(
            "0/25",
            vec![
                ChangeBatch::new(
                    table_id.clone(),
                    vec![CdcChange::Insert {
                        row: row(10, Some(1000), Some("open")),
                    }],
                )
                .expect("batch"),
            ],
        );
        let checkpoint = store
            .apply_transaction(&schemas(schema), &transaction)
            .await
            .expect("apply")
            .checkpoint()
            .clone();

        let reloaded = CdcTableStore::new(table);
        assert_eq!(
            reloaded
                .load_checkpoint(transaction.source_id())
                .await
                .expect("load checkpoint"),
            Some(checkpoint)
        );
        assert_eq!(
            reloaded
                .load_row(&table_id, &key(10))
                .await
                .expect("load row"),
            Some(row(10, Some(1000), Some("open")))
        );
    }

    #[tokio::test]
    async fn missing_previous_row_for_key_only_delete_is_rejected() {
        let store = test_store("cdc-apply-missing-delete").await;
        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let transaction = tx(
            "0/30",
            vec![
                ChangeBatch::new(
                    table_id,
                    vec![CdcChange::Delete {
                        key: Some(key(404)),
                        before: None,
                    }],
                )
                .expect("batch"),
            ],
        );

        let err = store
            .apply_transaction(&schemas(schema), &transaction)
            .await
            .expect_err("delete should fail");
        assert!(format!("{err:#}").contains("could not find previous row"));
    }

    #[tokio::test]
    async fn truncate_is_rejected_without_mutating_checkpoint() {
        let store = test_store("cdc-apply-truncate").await;
        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let transaction = tx(
            "0/40",
            vec![ChangeBatch::new(table_id, vec![CdcChange::Truncate]).expect("batch")],
        );

        let err = store
            .apply_transaction(&schemas(schema), &transaction)
            .await
            .expect_err("truncate should fail");
        assert!(format!("{err:#}").contains("truncate"));
        assert_eq!(
            store
                .load_checkpoint(transaction.source_id())
                .await
                .expect("load checkpoint"),
            None
        );
    }
}
