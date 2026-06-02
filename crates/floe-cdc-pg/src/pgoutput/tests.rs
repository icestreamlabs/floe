use super::*;
use crate::pgoutput_test_messages::{
    insert_message as insert_relation_message, orders_relation_message,
    orders_relation_message_for, origin_message, put_text_value, put_u8, put_u16, put_u32,
    put_unchanged_toast_value, relation_message_with_identity_and_column_specs, truncate_message,
    tuple, tuple_with_unchanged_toast,
};
use floe_core::RowValue;

const PG_FLOAT4_TEST_OID: u32 = 700;
const PG_FLOAT8_TEST_OID: u32 = 701;
const PG_TIMESTAMP_TEST_OID: u32 = 1114;
const PG_TIMESTAMPTZ_TEST_OID: u32 = 1184;
const PG_JSON_TEST_OID: u32 = 114;
const PG_JSONB_TEST_OID: u32 = 3802;
const PG_UUID_TEST_OID: u32 = 2950;
const PG_BYTEA_TEST_OID: u32 = 17;

fn relation_message() -> Bytes {
    orders_relation_message()
}

fn relation_message_for(relation_id: u32, table: &str) -> Bytes {
    orders_relation_message_for(relation_id, table)
}

fn insert_message(values: impl IntoIterator<Item = Option<&'static str>>) -> Bytes {
    insert_relation_message(42, values)
}

fn decoder_with_relation() -> PgOutputDecoder {
    let mut decoder = PgOutputDecoder::new();
    decoder
        .decode_message(relation_message())
        .expect("decode relation");
    decoder
}

#[test]
fn decodes_relation_metadata_and_builds_cdc_schema() {
    let mut decoder = PgOutputDecoder::new();
    let message = decoder
        .decode_message(relation_message())
        .expect("decode relation");
    let PgOutputMessage::Relation(relation) = message else {
        panic!("expected relation message");
    };

    assert_eq!(relation.relation_id(), PostgresRelationId::new(42));
    assert_eq!(relation.namespace(), "public");
    assert_eq!(relation.name(), "orders");
    assert_eq!(
        relation.replica_identity(),
        PostgresReplicaIdentity::Default
    );
    assert_eq!(relation.columns().len(), 4);
    assert!(relation.columns()[0].is_key());
    assert_eq!(
        decoder
            .relation(PostgresRelationId::new(42))
            .expect("cached relation"),
        &relation
    );

    let schema = relation
        .to_cdc_schema(CdcTableId::new("orders").expect("table id"))
        .expect("cdc schema");
    assert_eq!(schema.upstream_table().schema(), "public");
    assert_eq!(schema.upstream_table().table(), "orders");
    assert_eq!(schema.primary_key().columns(), &["id".to_string()]);
    assert!(!schema.columns()[0].nullable());
    assert!(schema.columns()[1].nullable());
    assert_eq!(schema.columns()[1].data_type(), &ColumnType::Int64);
}

#[test]
fn decodes_insert_to_cdc_change() {
    let mut decoder = decoder_with_relation();
    let change = decoder
        .decode_cdc_change(insert_message([
            Some("7"),
            Some("100"),
            Some("open"),
            Some("t"),
        ]))
        .expect("decode insert")
        .expect("change")
        .into_change();

    assert_eq!(
        change,
        CdcChange::Insert {
            row: CdcRow::new([
                Some(RowValue::Int64(7)),
                Some(RowValue::Int64(100)),
                Some(RowValue::Utf8("open".to_string())),
                Some(RowValue::Bool(true)),
            ])
            .expect("row")
        }
    );
}

#[test]
fn parses_date_and_numeric_text_values() {
    let date_column = PgOutputColumn {
        flags: 0,
        name: "shipdate".to_string(),
        type_oid: PG_DATE_OID,
        type_modifier: -1,
    };
    let numeric_column = PgOutputColumn {
        flags: 0,
        name: "price".to_string(),
        type_oid: PG_NUMERIC_OID,
        type_modifier: -1,
    };
    let decimal_column = PgOutputColumn {
        flags: 0,
        name: "discount".to_string(),
        type_oid: PG_NUMERIC_OID,
        type_modifier: 4 + (15 << 16) + 2,
    };

    assert_eq!(
        parse_text_row_value(&date_column, "1970-01-01").expect("date"),
        RowValue::DateDays(0)
    );
    assert_eq!(
        parse_text_row_value(&date_column, "1969-12-31").expect("date"),
        RowValue::DateDays(-1)
    );
    assert_eq!(
        parse_text_row_value(&numeric_column, "123.45").expect("numeric"),
        RowValue::Numeric("123.45".to_string())
    );
    assert_eq!(
        column_type_for_oid(PG_NUMERIC_OID, decimal_column.type_modifier).expect("type"),
        ColumnType::Decimal128 {
            precision: 15,
            scale: 2
        }
    );
    assert_eq!(
        parse_text_row_value(&decimal_column, "123.45").expect("decimal"),
        RowValue::Decimal128(12_345)
    );
}

#[test]
fn pgoutput_type_compatibility_matrix_is_explicit() {
    #[derive(Debug)]
    struct SupportedTypeCase {
        name: &'static str,
        type_oid: u32,
        type_modifier: i32,
        sample: &'static str,
        expected_type: ColumnType,
        expected_value: RowValue,
    }

    for case in [
        SupportedTypeCase {
            name: "bool",
            type_oid: PG_BOOL_OID,
            type_modifier: -1,
            sample: "t",
            expected_type: ColumnType::Bool,
            expected_value: RowValue::Bool(true),
        },
        SupportedTypeCase {
            name: "int2",
            type_oid: PG_INT2_OID,
            type_modifier: -1,
            sample: "-2",
            expected_type: ColumnType::Int64,
            expected_value: RowValue::Int64(-2),
        },
        SupportedTypeCase {
            name: "int4",
            type_oid: PG_INT4_OID,
            type_modifier: -1,
            sample: "123",
            expected_type: ColumnType::Int64,
            expected_value: RowValue::Int64(123),
        },
        SupportedTypeCase {
            name: "int8",
            type_oid: PG_INT8_OID,
            type_modifier: -1,
            sample: "1234567890123",
            expected_type: ColumnType::Int64,
            expected_value: RowValue::Int64(1_234_567_890_123),
        },
        SupportedTypeCase {
            name: "text",
            type_oid: PG_TEXT_OID,
            type_modifier: -1,
            sample: "open",
            expected_type: ColumnType::Utf8,
            expected_value: RowValue::Utf8("open".to_string()),
        },
        SupportedTypeCase {
            name: "varchar",
            type_oid: PG_VARCHAR_OID,
            type_modifier: -1,
            sample: "paid",
            expected_type: ColumnType::Utf8,
            expected_value: RowValue::Utf8("paid".to_string()),
        },
        SupportedTypeCase {
            name: "date",
            type_oid: PG_DATE_OID,
            type_modifier: -1,
            sample: "1970-01-02",
            expected_type: ColumnType::DateDays,
            expected_value: RowValue::DateDays(1),
        },
        SupportedTypeCase {
            name: "timestamp",
            type_oid: PG_TIMESTAMP_TEST_OID,
            type_modifier: -1,
            sample: "2024-01-02 03:04:05.678",
            expected_type: ColumnType::TimestampMillis,
            expected_value: RowValue::TimestampMillis(1_704_164_645_678),
        },
        SupportedTypeCase {
            name: "timestamptz",
            type_oid: PG_TIMESTAMPTZ_TEST_OID,
            type_modifier: -1,
            sample: "2024-01-02 03:04:05.678+00",
            expected_type: ColumnType::TimestampMillis,
            expected_value: RowValue::TimestampMillis(1_704_164_645_678),
        },
        SupportedTypeCase {
            name: "numeric",
            type_oid: PG_NUMERIC_OID,
            type_modifier: -1,
            sample: "123.45",
            expected_type: ColumnType::Numeric,
            expected_value: RowValue::Numeric("123.45".to_string()),
        },
        SupportedTypeCase {
            name: "decimal",
            type_oid: PG_NUMERIC_OID,
            type_modifier: 4 + (12 << 16) + 2,
            sample: "123.45",
            expected_type: ColumnType::Decimal128 {
                precision: 12,
                scale: 2,
            },
            expected_value: RowValue::Decimal128(12_345),
        },
        SupportedTypeCase {
            name: "uuid",
            type_oid: PG_UUID_TEST_OID,
            type_modifier: -1,
            sample: "550e8400-e29b-41d4-a716-446655440000",
            expected_type: ColumnType::Utf8,
            expected_value: RowValue::Utf8("550e8400-e29b-41d4-a716-446655440000".to_string()),
        },
        SupportedTypeCase {
            name: "json",
            type_oid: PG_JSON_TEST_OID,
            type_modifier: -1,
            sample: r#"{"state":"paid"}"#,
            expected_type: ColumnType::Utf8,
            expected_value: RowValue::Utf8(r#"{"state":"paid"}"#.to_string()),
        },
        SupportedTypeCase {
            name: "jsonb",
            type_oid: PG_JSONB_TEST_OID,
            type_modifier: -1,
            sample: r#"{"state": "paid"}"#,
            expected_type: ColumnType::Utf8,
            expected_value: RowValue::Utf8(r#"{"state": "paid"}"#.to_string()),
        },
        SupportedTypeCase {
            name: "bytea",
            type_oid: PG_BYTEA_TEST_OID,
            type_modifier: -1,
            sample: r#"\xdeadbeef"#,
            expected_type: ColumnType::Utf8,
            expected_value: RowValue::Utf8(r#"\xdeadbeef"#.to_string()),
        },
    ] {
        let column = PgOutputColumn {
            flags: 0,
            name: case.name.to_string(),
            type_oid: case.type_oid,
            type_modifier: case.type_modifier,
        };
        assert_eq!(
            column_type_for_oid(case.type_oid, case.type_modifier).expect(case.name),
            case.expected_type
        );
        assert_eq!(
            parse_text_row_value(&column, case.sample).expect(case.name),
            case.expected_value
        );
    }

    for (name, type_oid) in [
        ("float4", PG_FLOAT4_TEST_OID),
        ("float8", PG_FLOAT8_TEST_OID),
    ] {
        let err = column_type_for_oid(type_oid, -1).expect_err(name);
        assert!(
            format!("{err:#}").contains("unsupported Postgres type OID"),
            "{name} should fail with an explicit unsupported-type error: {err:#}"
        );
    }

    let text_column = PgOutputColumn {
        flags: 0,
        name: "payload".to_string(),
        type_oid: PG_TEXT_OID,
        type_modifier: -1,
    };
    assert_eq!(
        tuple_value_to_row_value(&text_column, &PgOutputTupleValue::Null).expect("nullable text"),
        None
    );
    let err = tuple_value_to_row_value(
        &text_column,
        &PgOutputTupleValue::Binary(Bytes::from_static(b"not-text")),
    )
    .expect_err("binary pgoutput values are not supported");
    assert!(format!("{err:#}").contains("binary pgoutput value"));
}

#[test]
fn decodes_replica_identity_modes_and_reports_unsupported_identity() {
    for (wire, expected) in [
        (b'd', PostgresReplicaIdentity::Default),
        (b'n', PostgresReplicaIdentity::Nothing),
        (b'f', PostgresReplicaIdentity::Full),
        (b'i', PostgresReplicaIdentity::Index),
        (b'x', PostgresReplicaIdentity::Unknown(b'x')),
    ] {
        let PgOutputMessage::Relation(relation) =
            decode_pgoutput_message(relation_message_with_identity_and_column_specs(
                42,
                "orders",
                wire,
                &[("id", PG_INT8_OID, true), ("status", PG_TEXT_OID, false)],
            ))
            .expect("decode relation")
        else {
            panic!("expected relation");
        };
        assert_eq!(relation.replica_identity(), expected);
    }

    let PgOutputMessage::Relation(relation) =
        decode_pgoutput_message(relation_message_with_identity_and_column_specs(
            43,
            "orders_without_identity",
            b'n',
            &[("id", PG_INT8_OID, false), ("status", PG_TEXT_OID, false)],
        ))
        .expect("decode no-key relation")
    else {
        panic!("expected relation");
    };
    let err = relation
        .to_cdc_schema(CdcTableId::new("orders_without_identity").expect("table id"))
        .expect_err("relation without replica identity key should be unsupported");
    let message = format!("{err:#}");
    assert!(message.contains("replica identity Nothing"));
    assert!(message.contains("no key columns"));
}

#[test]
fn decodes_replica_identity_full_update_with_before_image() {
    let mut decoder = PgOutputDecoder::new();
    decoder
        .decode_message(relation_message_with_identity_and_column_specs(
            42,
            "orders",
            b'f',
            &[("id", PG_INT8_OID, true), ("status", PG_TEXT_OID, false)],
        ))
        .expect("decode relation");
    let mut out = Vec::new();
    put_u8(&mut out, b'U');
    put_u32(&mut out, 42);
    put_u8(&mut out, b'O');
    out.extend_from_slice(&tuple([Some("7"), Some("open")]));
    put_u8(&mut out, b'N');
    out.extend_from_slice(&tuple([Some("7"), Some("paid")]));

    let change = decoder
        .decode_cdc_change(Bytes::from(out))
        .expect("decode update")
        .expect("change")
        .into_change();

    assert_eq!(
        change,
        CdcChange::Update {
            key: None,
            before: Some(
                CdcRow::new([
                    Some(RowValue::Int64(7)),
                    Some(RowValue::Utf8("open".to_string())),
                ])
                .expect("row")
            ),
            after: CdcRow::new([
                Some(RowValue::Int64(7)),
                Some(RowValue::Utf8("paid".to_string())),
            ])
            .expect("row")
        }
    );
}

#[test]
fn decodes_update_with_key_to_cdc_change() {
    let mut decoder = decoder_with_relation();
    let mut out = Vec::new();
    put_u8(&mut out, b'U');
    put_u32(&mut out, 42);
    put_u8(&mut out, b'K');
    out.extend_from_slice(&tuple([Some("7"), None, None, None]));
    put_u8(&mut out, b'N');
    out.extend_from_slice(&tuple([
        Some("7"),
        Some("150"),
        Some("paid"),
        Some("false"),
    ]));

    let change = decoder
        .decode_cdc_change(Bytes::from(out))
        .expect("decode update")
        .expect("change")
        .into_change();
    assert_eq!(
        change,
        CdcChange::Update {
            key: Some(CdcRowKey::new([RowValue::Int64(7)]).expect("key")),
            before: None,
            after: CdcRow::new([
                Some(RowValue::Int64(7)),
                Some(RowValue::Int64(150)),
                Some(RowValue::Utf8("paid".to_string())),
                Some(RowValue::Bool(false)),
            ])
            .expect("row")
        }
    );
}

#[test]
fn decodes_update_with_unchanged_toast_marker() {
    let mut decoder = decoder_with_relation();
    let mut out = Vec::new();
    put_u8(&mut out, b'U');
    put_u32(&mut out, 42);
    put_u8(&mut out, b'K');
    out.extend_from_slice(&tuple([Some("7"), None, None, None]));
    put_u8(&mut out, b'N');
    out.extend_from_slice(&tuple_with_unchanged_toast(
        [Some("7"), Some("150"), None, Some("false")],
        [2],
    ));

    let change = decoder
        .decode_cdc_change(Bytes::from(out))
        .expect("decode update")
        .expect("change")
        .into_change();
    let expected_after = CdcRow::with_unchanged_toast_indices(
        [
            Some(RowValue::Int64(7)),
            Some(RowValue::Int64(150)),
            None,
            Some(RowValue::Bool(false)),
        ],
        [2],
    )
    .expect("row");

    assert_eq!(
        change,
        CdcChange::Update {
            key: Some(CdcRowKey::new([RowValue::Int64(7)]).expect("key")),
            before: None,
            after: expected_after,
        }
    );
}

#[test]
fn decodes_delete_with_old_tuple_to_cdc_change() {
    let mut decoder = decoder_with_relation();
    let mut out = Vec::new();
    put_u8(&mut out, b'D');
    put_u32(&mut out, 42);
    put_u8(&mut out, b'O');
    out.extend_from_slice(&tuple([Some("7"), Some("100"), Some("open"), Some("t")]));

    let change = decoder
        .decode_cdc_change(Bytes::from(out))
        .expect("decode delete")
        .expect("change")
        .into_change();
    assert_eq!(
        change,
        CdcChange::Delete {
            key: None,
            before: Some(
                CdcRow::new([
                    Some(RowValue::Int64(7)),
                    Some(RowValue::Int64(100)),
                    Some(RowValue::Utf8("open".to_string())),
                    Some(RowValue::Bool(true)),
                ])
                .expect("row")
            )
        }
    );
}

#[test]
fn decodes_truncate_and_origin_messages() {
    let mut truncate = Vec::new();
    put_u8(&mut truncate, b'T');
    put_u32(&mut truncate, 2);
    put_u8(&mut truncate, 3);
    put_u32(&mut truncate, 42);
    put_u32(&mut truncate, 43);
    assert_eq!(
        decode_pgoutput_message(Bytes::from(truncate)).expect("truncate"),
        PgOutputMessage::Truncate {
            relation_ids: vec![PostgresRelationId::new(42), PostgresRelationId::new(43)],
            cascade: true,
            restart_identity: true,
        }
    );

    assert_eq!(
        decode_pgoutput_message(origin_message(0x16B6C50, "upstream")).expect("origin"),
        PgOutputMessage::Origin {
            commit_lsn: PostgresLsn::from_u64(0x16B6C50),
            name: "upstream".to_string(),
        }
    );
}

#[test]
fn decodes_multi_relation_truncate_to_cdc_changes() {
    let mut decoder = PgOutputDecoder::new();
    decoder
        .decode_message(relation_message_for(42, "orders"))
        .expect("orders relation");
    decoder
        .decode_message(relation_message_for(43, "customers"))
        .expect("customers relation");

    let changes = decoder
        .decode_cdc_changes(truncate_message([42, 43]))
        .expect("decode truncate changes");
    let observed: Vec<(String, CdcChange)> = changes
        .into_iter()
        .map(|change| (change.relation().name().to_string(), change.into_change()))
        .collect();
    assert_eq!(
        observed,
        vec![
            ("orders".to_string(), CdcChange::Truncate),
            ("customers".to_string(), CdcChange::Truncate)
        ]
    );
}

#[test]
fn rejects_changes_before_relation_metadata() {
    let mut decoder = PgOutputDecoder::new();
    let err = decoder
        .decode_cdc_change(insert_message([Some("7"), Some("100"), None, Some("t")]))
        .expect_err("missing relation should fail");
    assert!(format!("{err:#}").contains("unknown relation id 42"));
}

#[test]
fn rejects_unchanged_toast_for_full_rows() {
    let mut decoder = decoder_with_relation();
    let mut out = Vec::new();
    put_u8(&mut out, b'I');
    put_u32(&mut out, 42);
    put_u8(&mut out, b'N');
    put_u16(&mut out, 4);
    put_text_value(&mut out, "7");
    put_text_value(&mut out, "100");
    put_unchanged_toast_value(&mut out);
    put_text_value(&mut out, "t");

    let err = decoder
        .decode_cdc_change(Bytes::from(out))
        .expect_err("unchanged toast should fail");
    assert!(format!("{err:#}").contains("unchanged TOAST"));
}
