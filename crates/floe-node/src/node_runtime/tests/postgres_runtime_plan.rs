use super::*;

#[tokio::test]
async fn postgres_cdc_runtime_plan_builds_cdc_schema_from_source_primary_key() {
    let statement = parse_floe_statement(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, amount BIGINT NOT NULL, note TEXT)",
    )
    .expect("parse create table");
    let FloeStatement::CreateTable(definition) = statement else {
        panic!("expected create table statement");
    };
    let table = table_definition_from_sql(&definition).expect("table definition");
    let source = source_definition_from_table(&table).expect("source definition");
    let mut registry = SourceRegistry::new();
    registry.register(source);
    let include_tables = vec!["public.orders".to_string()];

    let plan = postgres_cdc_runtime_plan(
        "pg_main",
        "postgres://postgres:postgres@localhost/postgres",
        PostgresSchemaEvolutionPolicy::IgnoreCompatible,
        Some(&include_tables),
        &registry,
        &HashMap::new(),
        &HashMap::new(),
    )
    .await
    .expect("runtime plan")
    .expect("native runtime plan");

    assert_eq!(plan.source_id.as_str(), "pg_main");
    let schema = plan
        .schemas
        .get(&CdcTableId::new("orders").expect("table id"))
        .expect("orders schema");
    assert_eq!(schema.upstream_table().schema(), "public");
    assert_eq!(schema.upstream_table().table(), "orders");
    assert_eq!(schema.primary_key().columns(), &["id".to_string()]);
    assert_eq!(schema.columns().len(), 3);
    assert!(!schema.columns()[0].nullable());
    assert_eq!(
        plan.schema_evolution_policy,
        PostgresSchemaEvolutionPolicy::IgnoreCompatible
    );
}

#[tokio::test]
async fn postgres_cdc_runtime_plan_accepts_postgres_replication_target() {
    let statement = parse_floe_statement(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, amount BIGINT NOT NULL, note TEXT)",
    )
    .expect("parse create table");
    let FloeStatement::CreateTable(definition) = statement else {
        panic!("expected create table statement");
    };
    let table = table_definition_from_sql(&definition).expect("table definition");
    let source = source_definition_from_table(&table).expect("source definition");
    let mut registry = SourceRegistry::new();
    registry.register(source);
    let include_tables = vec!["public.orders".to_string()];
    let pipeline = CatalogReplicationPipelineDefinition::new(
        "pg_orders_to_postgres",
        "pg_main",
        "public.orders",
        CatalogReplicationPipelineTarget::Postgres {
            connection: "postgres://postgres:postgres@localhost/postgres".to_string(),
            table: "public.orders_copy".to_string(),
        },
        CatalogReplicationPipelineFormat::FloeJson,
        CatalogReplicationBufferMode::Durable,
        CatalogReplicationBufferPolicy::default(),
        false,
        false,
        CatalogReplicationErrorPolicy::default(),
    )
    .expect("pipeline");

    let plan = postgres_cdc_runtime_plan(
        "pg_main",
        "postgres://postgres:postgres@localhost/postgres",
        PostgresSchemaEvolutionPolicy::FailFast,
        Some(&include_tables),
        &registry,
        &HashMap::new(),
        &HashMap::from([(pipeline.name().to_string(), pipeline)]),
    )
    .await
    .expect("runtime plan")
    .expect("native runtime plan");

    assert_eq!(plan.replication_pipelines.len(), 1);
    let target = &plan.replication_pipelines[0].target;
    assert!(matches!(
        target,
        ReplicationPipelineRuntimeTarget::Postgres { connection, table }
            if connection == "postgres://postgres:postgres@localhost/postgres"
                && table == "public.orders_copy"
    ));
}

#[tokio::test]
async fn postgres_cdc_runtime_plan_keeps_pipeline_only_table_unmaterialized() {
    let statement = parse_floe_statement(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, amount BIGINT NOT NULL, note TEXT)",
    )
    .expect("parse create table");
    let FloeStatement::CreateTable(definition) = statement else {
        panic!("expected create table statement");
    };
    let table = table_definition_from_sql(&definition).expect("table definition");
    let source = source_definition_from_table(&table).expect("source definition");
    let mut registry = SourceRegistry::new();
    registry.register(source);
    let include_tables = vec!["public.orders".to_string()];
    let pipeline = CatalogReplicationPipelineDefinition::new(
        "pg_orders_to_kafka",
        "pg_main",
        "public.orders",
        CatalogReplicationPipelineTarget::Kafka {
            brokers: "localhost:9092".to_string(),
            topic: "orders_cdc".to_string(),
        },
        CatalogReplicationPipelineFormat::DebeziumJson,
        CatalogReplicationBufferMode::Durable,
        CatalogReplicationBufferPolicy::default(),
        false,
        false,
        CatalogReplicationErrorPolicy::default(),
    )
    .expect("pipeline");

    let plan = postgres_cdc_runtime_plan(
        "pg_main",
        "postgres://postgres:postgres@localhost/postgres",
        PostgresSchemaEvolutionPolicy::FailFast,
        Some(&include_tables),
        &registry,
        &HashMap::new(),
        &HashMap::from([(pipeline.name().to_string(), pipeline)]),
    )
    .await
    .expect("runtime plan")
    .expect("native runtime plan");

    assert_eq!(plan.replication_pipelines.len(), 1);
    let pipeline_plan = &plan.replication_pipelines[0];
    assert_eq!(pipeline_plan.table_id.as_str(), "orders");
    assert_eq!(
        pipeline_plan.format,
        ReplicationPipelineRuntimeFormat::DebeziumJson
    );
    assert!(matches!(
        pipeline_plan.target,
        ReplicationPipelineRuntimeTarget::Kafka { .. }
    ));
    assert!(plan.schemas.contains_key(&pipeline_plan.table_id));
}

#[tokio::test]
async fn postgres_cdc_runtime_plan_rejects_include_table_without_primary_key() {
    let source = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new("id", SourceDataType::Int64)],
    )
    .expect("source");
    let mut registry = SourceRegistry::new();
    registry.register(source);
    let include_tables = vec!["orders".to_string()];

    let err = match postgres_cdc_runtime_plan(
        "pg_main",
        "postgres://postgres:postgres@localhost/postgres",
        PostgresSchemaEvolutionPolicy::FailFast,
        Some(&include_tables),
        &registry,
        &HashMap::new(),
        &HashMap::new(),
    )
    .await
    {
        Ok(_) => panic!("include-table CDC source without a primary key should fail"),
        Err(err) => err,
    };

    assert!(
        err.to_string()
            .contains("source 'orders' without a primary key")
    );
}

#[tokio::test]
async fn postgres_cdc_runtime_plan_rejects_unbound_include_table() {
    let registry = SourceRegistry::new();
    let include_tables = vec!["public.orders".to_string()];

    let err = match postgres_cdc_runtime_plan(
        "pg_main",
        "postgres://postgres:postgres@localhost/postgres",
        PostgresSchemaEvolutionPolicy::FailFast,
        Some(&include_tables),
        &registry,
        &HashMap::new(),
        &HashMap::new(),
    )
    .await
    {
        Ok(_) => panic!("unbound include-table CDC source should fail"),
        Err(err) => err,
    };

    assert!(err.to_string().contains("not bound to a Floe CDC table"));
}
