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

    let source_tables = HashMap::new();
    let replication_pipelines = HashMap::new();
    let plan = postgres_cdc_runtime_plan(PostgresCdcRuntimePlanRequest {
        connector_name: "pg_main",
        connection_string: "postgres://postgres:postgres@localhost/postgres",
        schema_evolution_policy: PostgresSchemaEvolutionPolicy::IgnoreCompatible,
        include_tables: Some(&include_tables),
        registry: &registry,
        source_tables: &source_tables,
        replication_pipelines: &replication_pipelines,
    })
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
    let pipeline =
        CatalogReplicationPipelineDefinition::new(CatalogReplicationPipelineDefinitionParts {
            name: "pg_orders_to_postgres".to_string(),
            source_name: "pg_main".to_string(),
            upstream_table: "public.orders".to_string(),
            target: CatalogReplicationPipelineTarget::Postgres {
                connection: "postgres://postgres:postgres@localhost/postgres".to_string(),
                table: "public.orders_copy".to_string(),
            },
            format: CatalogReplicationPipelineFormat::FloeJson,
            buffer_mode: CatalogReplicationBufferMode::Durable,
            buffer_policy: CatalogReplicationBufferPolicy::default(),
            emit_tombstones: false,
            include_transaction_metadata: false,
            error_policy: CatalogReplicationErrorPolicy::default(),
        })
        .expect("pipeline");

    let source_tables = HashMap::new();
    let replication_pipelines = HashMap::from([(pipeline.name().to_string(), pipeline)]);
    let plan = postgres_cdc_runtime_plan(PostgresCdcRuntimePlanRequest {
        connector_name: "pg_main",
        connection_string: "postgres://postgres:postgres@localhost/postgres",
        schema_evolution_policy: PostgresSchemaEvolutionPolicy::FailFast,
        include_tables: Some(&include_tables),
        registry: &registry,
        source_tables: &source_tables,
        replication_pipelines: &replication_pipelines,
    })
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
    let pipeline =
        CatalogReplicationPipelineDefinition::new(CatalogReplicationPipelineDefinitionParts {
            name: "pg_orders_to_kafka".to_string(),
            source_name: "pg_main".to_string(),
            upstream_table: "public.orders".to_string(),
            target: CatalogReplicationPipelineTarget::Kafka {
                brokers: "localhost:9092".to_string(),
                topic: "orders_cdc".to_string(),
            },
            format: CatalogReplicationPipelineFormat::DebeziumJson,
            buffer_mode: CatalogReplicationBufferMode::Durable,
            buffer_policy: CatalogReplicationBufferPolicy::default(),
            emit_tombstones: false,
            include_transaction_metadata: false,
            error_policy: CatalogReplicationErrorPolicy::default(),
        })
        .expect("pipeline");

    let source_tables = HashMap::new();
    let replication_pipelines = HashMap::from([(pipeline.name().to_string(), pipeline)]);
    let plan = postgres_cdc_runtime_plan(PostgresCdcRuntimePlanRequest {
        connector_name: "pg_main",
        connection_string: "postgres://postgres:postgres@localhost/postgres",
        schema_evolution_policy: PostgresSchemaEvolutionPolicy::FailFast,
        include_tables: Some(&include_tables),
        registry: &registry,
        source_tables: &source_tables,
        replication_pipelines: &replication_pipelines,
    })
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

    let source_tables = HashMap::new();
    let replication_pipelines = HashMap::new();
    let err = match postgres_cdc_runtime_plan(PostgresCdcRuntimePlanRequest {
        connector_name: "pg_main",
        connection_string: "postgres://postgres:postgres@localhost/postgres",
        schema_evolution_policy: PostgresSchemaEvolutionPolicy::FailFast,
        include_tables: Some(&include_tables),
        registry: &registry,
        source_tables: &source_tables,
        replication_pipelines: &replication_pipelines,
    })
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

    let source_tables = HashMap::new();
    let replication_pipelines = HashMap::new();
    let err = match postgres_cdc_runtime_plan(PostgresCdcRuntimePlanRequest {
        connector_name: "pg_main",
        connection_string: "postgres://postgres:postgres@localhost/postgres",
        schema_evolution_policy: PostgresSchemaEvolutionPolicy::FailFast,
        include_tables: Some(&include_tables),
        registry: &registry,
        source_tables: &source_tables,
        replication_pipelines: &replication_pipelines,
    })
    .await
    {
        Ok(_) => panic!("unbound include-table CDC source should fail"),
        Err(err) => err,
    };

    assert!(err.to_string().contains("not bound to a Floe CDC table"));
}
