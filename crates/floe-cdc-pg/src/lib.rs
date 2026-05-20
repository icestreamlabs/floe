mod client;
mod config;
mod lsn;
mod pgoutput;
#[cfg(test)]
mod pgoutput_test_messages;
mod snapshot;
mod transaction;

pub use client::*;
pub use config::*;
pub use lsn::*;
pub use pgoutput::*;
pub use snapshot::*;
pub use transaction::*;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use dbsp_storage::storage::{KeyValueTable, SlateTable};
    use floe_cdc::CdcTableStore;
    use floe_cdc_core::{
        CdcChange, CdcCheckpoint, CdcColumn, CdcPrimaryKey, CdcRow, CdcSourceId, CdcSourcePosition,
        CdcTableId, CdcTableSchema, CdcTransactionId, ChangeBatch, TransactionBatch,
        UpstreamTableRef,
    };
    use floe_core::RowValue;
    use floe_core::catalog::ColumnType;
    use object_store::memory::InMemory;
    use pgwire_replication::{Lsn as PgWireLsn, ReplicationEvent as PgWireReplicationEvent};
    use slatedb::Db;

    use crate::snapshot::{parse_simple_data_row, validate_replication_slot_name};

    async fn test_store(name: &str) -> CdcTableStore {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(name, object_store).await.expect("open SlateDB"));
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
        CdcTableStore::new(table)
    }

    fn orders_schema() -> CdcTableSchema {
        CdcTableSchema::new(
            CdcTableId::new("orders").expect("table id"),
            UpstreamTableRef::new("public", "orders").expect("upstream"),
            vec![
                CdcColumn::new("id", ColumnType::Int64, false).expect("id column"),
                CdcColumn::new("status", ColumnType::Utf8, true).expect("status column"),
            ],
            CdcPrimaryKey::new(["id"]).expect("primary key"),
        )
        .expect("schema")
    }

    fn checkpoint_transaction(source_id: CdcSourceId, position: &str) -> TransactionBatch {
        let schema = orders_schema();
        TransactionBatch::new(
            source_id,
            Some(CdcTransactionId::new(format!("tx-{position}")).expect("txid")),
            None,
            CdcSourcePosition::postgres(position, None).expect("position"),
            vec![
                ChangeBatch::new(
                    schema.table_id().clone(),
                    vec![CdcChange::Insert {
                        row: CdcRow::new([
                            Some(RowValue::Int64(1)),
                            Some(RowValue::Utf8("open".to_string())),
                        ])
                        .expect("row"),
                    }],
                )
                .expect("change batch"),
            ],
        )
        .expect("transaction")
    }

    #[test]
    fn postgres_lsn_parses_formats_and_serializes_as_pg_lsn() {
        let lsn = PostgresLsn::parse("16/B374D848").expect("parse lsn");
        assert_eq!(lsn.to_string(), "16/B374D848");
        assert_eq!(PostgresLsn::from_u64(lsn.as_u64()), lsn);
        assert!(!lsn.is_zero());

        let encoded = serde_json::to_string(&lsn).expect("serialize lsn");
        assert_eq!(encoded, r#""16/B374D848""#);
        let decoded: PostgresLsn = serde_json::from_str(&encoded).expect("decode lsn");
        assert_eq!(decoded, lsn);
        assert!(PostgresLsn::parse("not-a-lsn").is_err());
    }

    #[test]
    fn postgres_lsn_converts_to_cdc_source_position() {
        let position = PostgresLsn::parse("0/16B6C50")
            .expect("parse lsn")
            .to_source_position()
            .expect("source position");
        assert_eq!(
            position,
            CdcSourcePosition::Postgres {
                commit_lsn: "0/16B6C50".to_string(),
                event_lsn: None
            }
        );
    }

    #[test]
    fn config_validates_required_fields_and_maps_to_pgwire_config() {
        assert!(PostgresCdcConfig::new("", "floe", "", "app", "slot", "pub").is_err());
        assert!(PostgresCdcConfig::new("localhost", "", "", "app", "slot", "pub").is_err());
        assert!(PostgresCdcConfig::new("localhost", "floe", "", "", "slot", "pub").is_err());
        assert!(PostgresCdcConfig::new("localhost", "floe", "", "app", "", "pub").is_err());
        assert!(PostgresCdcConfig::new("localhost", "floe", "", "app", "slot", "").is_err());

        let start = PostgresLsn::parse("0/10").expect("start lsn");
        let stop = PostgresLsn::parse("0/20").expect("stop lsn");
        let config = PostgresCdcConfig::new("localhost", "floe", "secret", "app", "slot", "pub")
            .expect("config")
            .with_port(15432)
            .expect("port")
            .with_start_lsn(start)
            .with_stop_lsn(stop)
            .with_status_interval(Duration::from_millis(250))
            .expect("status interval")
            .with_idle_wakeup_interval(Duration::from_millis(500))
            .expect("idle interval")
            .with_buffer_events(64)
            .expect("buffer size");

        let pgwire = config.to_replication_config().expect("pgwire config");
        assert_eq!(pgwire.host, "localhost");
        assert_eq!(pgwire.port, 15432);
        assert_eq!(pgwire.user, "floe");
        assert_eq!(pgwire.password, "secret");
        assert_eq!(pgwire.database, "app");
        assert_eq!(pgwire.slot, "slot");
        assert_eq!(pgwire.publication, "pub");
        assert_eq!(PostgresLsn::from(pgwire.start_lsn), start);
        assert_eq!(pgwire.stop_at_lsn.map(PostgresLsn::from), Some(stop));
        assert_eq!(pgwire.status_interval, Duration::from_millis(250));
        assert_eq!(pgwire.idle_wakeup_interval, Duration::from_millis(500));
        assert_eq!(pgwire.buffer_events, 64);
    }

    #[test]
    fn exported_slot_response_helpers_parse_data_row_and_validate_slot_name() {
        let mut row = Vec::new();
        row.extend_from_slice(&4_i16.to_be_bytes());
        put_data_row_text(&mut row, "slot_a");
        put_data_row_text(&mut row, "0/16B6C50");
        put_data_row_text(&mut row, "00000003-00000010-1");
        put_data_row_text(&mut row, "pgoutput");

        let values = parse_simple_data_row(&row).expect("parse data row");
        assert_eq!(
            values,
            vec![
                Some("slot_a".to_string()),
                Some("0/16B6C50".to_string()),
                Some("00000003-00000010-1".to_string()),
                Some("pgoutput".to_string())
            ]
        );
        validate_replication_slot_name("slot_a_123").expect("valid slot");
        assert!(validate_replication_slot_name("Slot-A").is_err());
    }

    #[test]
    fn config_can_resume_from_cdc_checkpoint() {
        let checkpoint = CdcCheckpoint::new(
            CdcSourceId::new("pg_main").expect("source id"),
            CdcSourcePosition::postgres("0/80", None).expect("position"),
            None,
        );
        let config = PostgresCdcConfig::new("localhost", "floe", "secret", "app", "slot", "pub")
            .expect("config")
            .with_start_checkpoint(&checkpoint)
            .expect("resume from checkpoint");
        assert_eq!(config.start_lsn(), Some(PostgresLsn::from_u64(0x80)));
        assert_eq!(
            PostgresLsn::from(config.to_replication_config().expect("pgwire").start_lsn),
            PostgresLsn::from_u64(0x80)
        );
    }

    #[tokio::test]
    async fn stored_checkpoint_configures_start_lsn() {
        let source_id = CdcSourceId::new("pg_main").expect("source id");
        let table_store = test_store("pg-cdc-config-checkpoint").await;
        let schema = orders_schema();
        table_store
            .apply_transaction(
                &HashMap::from([(schema.table_id().clone(), schema)]),
                &checkpoint_transaction(source_id.clone(), "0/90"),
            )
            .await
            .expect("apply checkpoint transaction");

        let config = PostgresCdcConfig::new("localhost", "floe", "secret", "app", "slot", "pub")
            .expect("config");
        let resumed = config_with_stored_cdc_checkpoint(config, &table_store, &source_id)
            .await
            .expect("resume config");
        assert_eq!(resumed.start_lsn(), Some(PostgresLsn::from_u64(0x90)));

        let no_checkpoint_source = CdcSourceId::new("pg_other").expect("source id");
        let unchanged = config_with_stored_cdc_checkpoint(
            PostgresCdcConfig::new("localhost", "floe", "secret", "app", "slot", "pub")
                .expect("config"),
            &table_store,
            &no_checkpoint_source,
        )
        .await
        .expect("no checkpoint config");
        assert_eq!(unchanged.start_lsn(), None);
    }

    fn put_data_row_text(out: &mut Vec<u8>, value: &str) {
        out.extend_from_slice(&(value.len() as i32).to_be_bytes());
        out.extend_from_slice(value.as_bytes());
    }

    #[test]
    fn config_rejects_invalid_replay_bounds_and_tunables() {
        let start = PostgresLsn::parse("0/20").expect("start lsn");
        let stop = PostgresLsn::parse("0/10").expect("stop lsn");
        assert!(
            PostgresCdcConfig::new("localhost", "floe", "", "app", "slot", "pub")
                .expect("config")
                .with_start_lsn(start)
                .with_stop_lsn(stop)
                .validate()
                .is_err()
        );
        assert!(
            PostgresCdcConfig::new("localhost", "floe", "", "app", "slot", "pub")
                .expect("config")
                .with_status_interval(Duration::ZERO)
                .is_err()
        );
        assert!(
            PostgresCdcConfig::new("localhost", "floe", "", "app", "slot", "pub")
                .expect("config")
                .with_buffer_events(0)
                .is_err()
        );
    }

    #[test]
    fn events_map_from_pgwire_without_copying_bytes() {
        let data = Bytes::from_static(b"pgoutput");
        let event = PgWireReplicationEvent::XLogData {
            wal_start: PgWireLsn::from_u64(1),
            wal_end: PgWireLsn::from_u64(2),
            server_time_micros: 3,
            data: data.clone(),
        };
        assert_eq!(
            PostgresReplicationEvent::from(event),
            PostgresReplicationEvent::XLogData {
                wal_start: PostgresLsn::from_u64(1),
                wal_end: PostgresLsn::from_u64(2),
                server_time_micros: 3,
                data
            }
        );
    }
}
