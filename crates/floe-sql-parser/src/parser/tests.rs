use super::*;

#[test]
fn parses_kafka_sink_debezium_options() {
    let statement = parse_floe_statement(
        "CREATE SINK out_orders FROM mv_orders WITH (
            type = 'kafka',
            brokers = 'localhost:9092',
            topic = 'orders',
            format = 'debezium-json',
            key.columns = 'tenant_id,id',
            with_snapshot = true
        )",
    )
    .expect("parse sink");

    let FloeStatement::CreateSink(definition) = statement else {
        panic!("expected CREATE SINK statement");
    };
    assert_eq!(definition.name(), "out_orders");
    assert_eq!(definition.mv_name(), "mv_orders");
    assert!(definition.with_snapshot());
    match definition.connector() {
        SinkConnector::Kafka {
            brokers,
            topic,
            format,
            key_columns,
        } => {
            assert_eq!(brokers, "localhost:9092");
            assert_eq!(topic, "orders");
            assert_eq!(format.as_deref(), Some("debezium_json"));
            assert_eq!(
                key_columns,
                &vec!["tenant_id".to_string(), "id".to_string()]
            );
        }
        other => panic!("expected Kafka sink, got {other:?}"),
    }
}
