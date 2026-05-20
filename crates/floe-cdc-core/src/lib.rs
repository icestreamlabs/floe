mod change;
mod checkpoint;
mod ids;
mod position;
mod row;
mod schema;
mod source;

pub use change::{CdcChange, CdcOperation, CdcSchemaVersionMap, ChangeBatch, TransactionBatch};
pub use checkpoint::CdcCheckpoint;
pub use ids::{CdcSourceId, CdcTableId, CdcTransactionId, UpstreamTableRef};
pub use position::CdcSourcePosition;
pub use row::{CdcColumnarColumn, CdcColumnarRowBatch, CdcRow, CdcRowKey};
pub use schema::{CdcColumn, CdcPrimaryKey, CdcTableDefinition, CdcTableSchema};
pub use source::{
    CdcSourceCategory, CdcSourceDefinition, CdcSourceSemantics, DirectQuerySupport,
    TableMaterializationRequirement,
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use floe_core::RowValue;
    use floe_core::catalog::ColumnType;

    fn id_column(nullable: bool) -> CdcColumn {
        CdcColumn::new("id", ColumnType::Int64, nullable).expect("id column")
    }

    fn amount_column() -> CdcColumn {
        CdcColumn::new("amount", ColumnType::Int64, true).expect("amount column")
    }

    fn orders_schema() -> CdcTableSchema {
        CdcTableSchema::new(
            CdcTableId::new("orders").expect("table id"),
            UpstreamTableRef::new("public", "orders").expect("upstream"),
            vec![
                CdcColumn::new("tenant_id", ColumnType::Int64, false).expect("tenant column"),
                id_column(false),
                amount_column(),
                CdcColumn::new("status", ColumnType::Utf8, true).expect("status column"),
            ],
            CdcPrimaryKey::new(["tenant_id", "id"]).expect("primary key"),
        )
        .expect("orders schema")
    }

    fn orders_row(tenant_id: i64, id: i64, amount: Option<i64>, status: Option<&str>) -> CdcRow {
        CdcRow::new([
            Some(RowValue::Int64(tenant_id)),
            Some(RowValue::Int64(id)),
            amount.map(RowValue::Int64),
            status.map(|value| RowValue::Utf8(value.to_string())),
        ])
        .expect("orders row")
    }

    #[test]
    fn native_database_cdc_requires_table_materialization_and_primary_key() {
        let semantics = CdcSourceSemantics::for_category(CdcSourceCategory::NativeDatabaseCdc);
        assert_eq!(semantics.direct_query_support(), DirectQuerySupport::None);
        assert_eq!(
            semantics.table_materialization(),
            TableMaterializationRequirement::AlwaysRequired
        );
        assert!(semantics.primary_key_required_for_table());
        assert!(semantics.validate_direct_query(false).is_err());
        assert!(semantics.validate_table_primary_key(None).is_err());

        let key = CdcPrimaryKey::new(["id"]).expect("primary key");
        semantics
            .validate_table_primary_key(Some(&key))
            .expect("primary key should satisfy CDC table contract");
    }

    #[test]
    fn append_only_sources_allow_direct_queries_without_primary_keys() {
        let semantics = CdcSourceSemantics::for_category(CdcSourceCategory::AppendOnly);
        assert_eq!(semantics.direct_query_support(), DirectQuerySupport::Full);
        assert_eq!(
            semantics.table_materialization(),
            TableMaterializationRequirement::Optional
        );
        assert!(!semantics.primary_key_required_for_table());
        semantics
            .validate_direct_query(true)
            .expect("append-only sources can be directly queried");
        semantics
            .validate_table_primary_key(None)
            .expect("append-only sources do not require a table primary key");
    }

    #[test]
    fn upsert_sources_allow_only_stateless_direct_queries() {
        let semantics = CdcSourceSemantics::for_category(CdcSourceCategory::Upsert);
        assert_eq!(
            semantics.direct_query_support(),
            DirectQuerySupport::StatelessOnly
        );
        semantics
            .validate_direct_query(false)
            .expect("stateless direct query should be allowed");
        assert!(semantics.validate_direct_query(true).is_err());
        assert!(semantics.validate_table_primary_key(None).is_err());
    }

    #[test]
    fn source_definitions_record_connector_semantics_and_properties() {
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let source = CdcSourceDefinition::postgres(source_id.clone())
            .expect("source")
            .with_property("slot.name", "floe_slot")
            .expect("property")
            .with_property("publication.name", "floe_pub")
            .expect("property");

        assert_eq!(source.source_id(), &source_id);
        assert_eq!(source.connector(), "postgres-cdc");
        assert_eq!(
            source.semantics().category(),
            CdcSourceCategory::NativeDatabaseCdc
        );
        assert_eq!(source.property("slot.name"), Some("floe_slot"));
        assert!(
            CdcSourceDefinition::new(
                source_id,
                "",
                CdcSourceSemantics::for_category(CdcSourceCategory::NativeDatabaseCdc)
            )
            .is_err()
        );
    }

    #[test]
    fn source_definitions_validate_owned_table_definitions() {
        let source = CdcSourceDefinition::postgres(CdcSourceId::new("pg_main").expect("source id"))
            .expect("source");
        let table = CdcTableDefinition::new(source.source_id().clone(), orders_schema());
        source
            .validate_table_definition(&table)
            .expect("table should match source semantics");

        let other_source_table = CdcTableDefinition::new(
            CdcSourceId::new("pg_other").expect("source id"),
            table.schema().clone(),
        );
        assert!(
            source
                .validate_table_definition(&other_source_table)
                .is_err()
        );
    }

    #[test]
    fn primary_key_rejects_empty_and_duplicate_columns() {
        assert!(CdcPrimaryKey::new(Vec::<String>::new()).is_err());
        assert!(CdcPrimaryKey::new(["id", ""]).is_err());
        assert!(CdcPrimaryKey::new(["id", "id"]).is_err());

        let key = CdcPrimaryKey::new(["tenant_id", "id"]).expect("composite primary key");
        assert_eq!(key.columns(), &["tenant_id".to_string(), "id".to_string()]);
        assert!(key.contains_column("tenant_id"));
    }

    #[test]
    fn table_schema_validates_primary_key_columns() {
        let table_id = CdcTableId::new("orders").expect("table id");
        let upstream = UpstreamTableRef::new("public", "orders").expect("upstream");
        let key = CdcPrimaryKey::new(["id"]).expect("primary key");

        CdcTableSchema::new(
            table_id.clone(),
            upstream.clone(),
            vec![id_column(false), amount_column()],
            key.clone(),
        )
        .expect("valid schema");

        let missing_key = CdcTableSchema::new(
            table_id.clone(),
            upstream.clone(),
            vec![amount_column()],
            key.clone(),
        )
        .expect_err("missing primary key column should fail");
        assert!(missing_key.to_string().contains("is not in table schema"));

        let nullable_key = CdcTableSchema::new(
            table_id,
            upstream,
            vec![id_column(true), amount_column()],
            key,
        )
        .expect_err("nullable primary key column should fail");
        assert!(nullable_key.to_string().contains("cannot be nullable"));
    }

    #[test]
    fn table_schema_with_composite_primary_key_serializes() {
        let schema = orders_schema();
        let encoded = serde_json::to_vec(&schema).expect("serialize schema");
        let decoded: CdcTableSchema = serde_json::from_slice(&encoded).expect("decode schema");

        assert_eq!(decoded, schema);
        assert_eq!(
            decoded.primary_key().columns(),
            &["tenant_id".to_string(), "id".to_string()]
        );
    }

    #[test]
    fn source_positions_reject_empty_values() {
        assert!(CdcSourcePosition::postgres("", None).is_err());
        assert!(CdcSourcePosition::postgres("0/16B6C50", Some(String::new())).is_err());
        assert!(CdcSourcePosition::opaque("").is_err());

        let position = CdcSourcePosition::postgres("0/16B6C50", Some("0/16B6C20".to_string()))
            .expect("postgres position");
        assert_eq!(
            position,
            CdcSourcePosition::Postgres {
                commit_lsn: "0/16B6C50".to_string(),
                event_lsn: Some("0/16B6C20".to_string())
            }
        );
    }

    #[test]
    fn source_positions_compare_postgres_frontiers() {
        let commit_20 = CdcSourcePosition::postgres("0/20", None).expect("position");
        let commit_10 = CdcSourcePosition::postgres("0/10", None).expect("position");
        assert!(commit_20.covers(&commit_10).expect("compare"));
        assert!(commit_20.covers(&commit_20).expect("compare"));
        assert!(!commit_10.covers(&commit_20).expect("compare"));

        let event_21 =
            CdcSourcePosition::postgres("0/20", Some("0/21".to_string())).expect("event position");
        let event_22 =
            CdcSourcePosition::postgres("0/20", Some("0/22".to_string())).expect("event position");
        assert!(commit_20.covers(&event_22).expect("compare"));
        assert!(!event_22.covers(&commit_20).expect("compare"));
        assert!(event_22.covers(&event_21).expect("compare"));
        assert!(!event_21.covers(&event_22).expect("compare"));

        let opaque = CdcSourcePosition::opaque("same").expect("opaque");
        assert!(opaque.covers(&opaque).expect("compare"));
        assert!(
            opaque
                .covers(&CdcSourcePosition::opaque("other").expect("opaque"))
                .is_ok_and(|covers| !covers)
        );
        assert!(opaque.covers(&commit_20).is_err());
    }

    #[test]
    fn checkpoints_compare_source_and_position() {
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let checkpoint = CdcCheckpoint::new(
            source_id.clone(),
            CdcSourcePosition::postgres("0/20", None).expect("position"),
            None,
        );
        let older = CdcCheckpoint::new(
            source_id,
            CdcSourcePosition::postgres("0/10", None).expect("position"),
            None,
        );
        assert!(checkpoint.covers(&older).expect("checkpoint covers"));

        let different_source = CdcCheckpoint::new(
            CdcSourceId::new("pg_other").expect("source id"),
            CdcSourcePosition::postgres("0/10", None).expect("position"),
            None,
        );
        assert!(checkpoint.covers(&different_source).is_err());
    }

    #[test]
    fn rows_validate_against_schema_and_extract_composite_keys() {
        let schema = orders_schema();
        let row = orders_row(7, 42, Some(100), Some("open"));
        schema.validate_row(&row).expect("valid row");

        let key = schema.primary_key_from_row(&row).expect("primary key");
        assert_eq!(key.values(), &[RowValue::Int64(7), RowValue::Int64(42)]);
        key.validate_against_schema(&schema).expect("valid key");

        let wrong_width = CdcRow::new([Some(RowValue::Int64(7))]).expect("row");
        assert!(schema.validate_row(&wrong_width).is_err());

        let null_pk = CdcRow::new([
            Some(RowValue::Int64(7)),
            None,
            Some(RowValue::Int64(100)),
            None,
        ])
        .expect("row");
        assert!(schema.validate_row(&null_pk).is_err());

        let wrong_type = CdcRow::new([
            Some(RowValue::Int64(7)),
            Some(RowValue::Utf8("not-an-id".to_string())),
            Some(RowValue::Int64(100)),
            None,
        ])
        .expect("row");
        assert!(schema.validate_row(&wrong_type).is_err());
    }

    #[test]
    fn change_batches_validate_change_shapes() {
        let schema = orders_schema();
        let table_id = schema.table_id().clone();
        let before = orders_row(7, 42, Some(100), Some("open"));
        let after = orders_row(7, 42, Some(150), Some("paid"));
        let key = schema.primary_key_from_row(&before).expect("primary key");

        let batch = ChangeBatch::new(
            table_id.clone(),
            vec![
                CdcChange::Insert {
                    row: before.clone(),
                },
                CdcChange::Update {
                    key: Some(key.clone()),
                    before: Some(before),
                    after,
                },
                CdcChange::Delete {
                    key: Some(key),
                    before: None,
                },
            ],
        )
        .expect("change batch");
        batch
            .validate_against_schema(&schema)
            .expect("valid change batch");

        let invalid_delete = ChangeBatch::new(
            table_id,
            vec![CdcChange::Delete {
                key: None,
                before: None,
            }],
        )
        .expect("invalid delete batch");
        assert!(invalid_delete.validate_against_schema(&schema).is_err());
    }

    #[test]
    fn transaction_batches_validate_table_schemas_and_checkpoint_frontier() {
        let schema = orders_schema();
        let batch = ChangeBatch::new(
            schema.table_id().clone(),
            vec![CdcChange::Insert {
                row: orders_row(7, 42, Some(100), Some("open")),
            }],
        )
        .expect("change batch");
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let txid = CdcTransactionId::new("tx-1").expect("txid");
        let commit_position =
            CdcSourcePosition::postgres("0/16B6C50", Some("0/16B6C20".to_string()))
                .expect("position");
        let transaction = TransactionBatch::new(
            source_id.clone(),
            Some(txid.clone()),
            None,
            commit_position.clone(),
            vec![batch],
        )
        .expect("transaction batch");

        let schemas = HashMap::from([(schema.table_id().clone(), schema)]);
        transaction
            .validate_against_schemas(&schemas)
            .expect("valid transaction");

        let checkpoint = CdcCheckpoint::from_transaction(&transaction);
        assert_eq!(checkpoint.source_id(), &source_id);
        assert_eq!(checkpoint.transaction_id(), Some(&txid));
        assert_eq!(checkpoint.position(), &commit_position);

        let missing_schemas = HashMap::new();
        assert!(
            transaction
                .validate_against_schemas(&missing_schemas)
                .is_err()
        );
    }

    #[test]
    fn empty_batches_and_transactions_are_rejected() {
        let table_id = CdcTableId::new("orders").expect("table id");
        assert!(ChangeBatch::new(table_id, Vec::new()).is_err());

        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let commit_position = CdcSourcePosition::opaque("frontier-1").expect("position");
        assert!(TransactionBatch::new(source_id, None, None, commit_position, Vec::new()).is_err());
        assert!(CdcTransactionId::new("").is_err());
    }
}
