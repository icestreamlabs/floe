use super::*;

#[test]
fn table_definition_from_sql_preserves_primary_key_and_nullability() {
    let statement = parse_floe_statement(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, note TEXT, enabled BOOL NOT NULL, created_at TIMESTAMP)",
    )
    .expect("parse create table");
    let FloeStatement::CreateTable(definition) = statement else {
        panic!("expected create table statement");
    };
    let table = table_definition_from_sql(&definition).expect("table definition");
    assert_eq!(table.name(), "orders");
    assert_eq!(table.columns().len(), 4);
    assert_eq!(table.primary_key_index(), Some(0));
    assert!(!table.columns()[0].nullable());
    assert!(table.columns()[1].nullable());
    assert!(!table.columns()[2].nullable());
    assert!(table.columns()[3].nullable());
}

#[test]
fn source_definition_from_table_sets_pk_property() {
    let statement =
        parse_floe_statement("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT, active BOOL)")
            .expect("parse create table");
    let FloeStatement::CreateTable(definition) = statement else {
        panic!("expected create table statement");
    };
    let table = table_definition_from_sql(&definition).expect("table definition");
    let source = source_definition_from_table(&table).expect("source definition");
    assert_eq!(source.name(), "users");
    assert_eq!(source.property(SOURCE_PRIMARY_KEY_PROPERTY), Some("id"));
    assert!(source_definition_has_primary_key(&source));
    assert!(!source.columns()[0].nullable());
    assert!(source.columns()[1].nullable());
}

#[test]
fn catalog_source_definition_from_sql_preserves_postgres_cdc_options() {
    let statement = parse_floe_statement(
        "CREATE SOURCE pg_main WITH (
            connector = 'postgres-cdc',
            connection = 'postgres://postgres:postgres@localhost/postgres',
            slot.name = 'floe_slot',
            publication.name = 'floe_pub',
            schema.evolution = 'apply-compatible-additions',
            slot.create = false,
            publication.create = true
        )",
    )
    .expect("parse create source");
    let FloeStatement::CreateSource(definition) = statement else {
        panic!("expected create source statement");
    };
    let source = catalog_source_definition_from_sql(&definition).expect("catalog source");
    assert_eq!(source.name(), "pg_main");
    let CatalogSourceConnector::PostgresCdc(postgres) = source.connector();
    assert_eq!(
        postgres.connection(),
        "postgres://postgres:postgres@localhost/postgres"
    );
    assert_eq!(postgres.slot(), "floe_slot");
    assert_eq!(postgres.publication(), Some("floe_pub"));
    assert_eq!(
        postgres.schema_evolution_policy(),
        CatalogPostgresCdcSchemaEvolutionPolicy::ApplyCompatibleAdditions
    );
    assert!(!postgres.auto_create_slot());
    assert!(postgres.auto_create_publication());
}

#[test]
fn materialized_view_validation_rejects_raw_cdc_source_references() {
    let source = CatalogSourceDefinition::new(
        "pg_main",
        CatalogSourceConnector::PostgresCdc(
            PostgresCdcSourceDefinition::new_with_setup_policy(
                "postgres://postgres:postgres@localhost/postgres",
                "floe_slot",
                Some("floe_pub".to_string()),
                Some(false),
                CatalogPostgresCdcSchemaEvolutionPolicy::IgnoreCompatible,
                false,
                true,
            )
            .expect("postgres source"),
        ),
    )
    .expect("source");
    let views = vec![
        MaterializedViewDefinition::new("mv_orders", "SELECT * FROM pg_main", false),
        MaterializedViewDefinition::new("mv_join", "SELECT * FROM orders", false),
    ];

    let err = validate_materialized_views_do_not_query_raw_cdc_sources(
        &HashMap::from([(source.name().to_string(), source)]),
        &views,
    )
    .expect_err("raw CDC source should be rejected");

    assert!(err.to_string().contains("reads raw CDC source 'pg_main'"));
    assert!(
        err.to_string()
            .contains("CREATE TABLE ... FROM pg_main TABLE")
    );
}

#[test]
fn materialized_view_validation_accepts_cdc_table_references() {
    let source = CatalogSourceDefinition::new(
        "pg_main",
        CatalogSourceConnector::PostgresCdc(
            PostgresCdcSourceDefinition::new_with_setup_policy(
                "postgres://postgres:postgres@localhost/postgres",
                "floe_slot",
                Some("floe_pub".to_string()),
                Some(false),
                CatalogPostgresCdcSchemaEvolutionPolicy::IgnoreCompatible,
                false,
                true,
            )
            .expect("postgres source"),
        ),
    )
    .expect("source");
    let views = vec![MaterializedViewDefinition::new(
        "mv_orders",
        "SELECT * FROM orders",
        false,
    )];

    validate_materialized_views_do_not_query_raw_cdc_sources(
        &HashMap::from([(source.name().to_string(), source)]),
        &views,
    )
    .expect("CDC table reference should be allowed");
}

#[test]
fn source_backed_table_definition_from_sql_preserves_binding() {
    let statement = parse_floe_statement(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, amount BIGINT)
         FROM pg_main TABLE 'public.orders'",
    )
    .expect("parse source-backed table");
    let FloeStatement::CreateTable(definition) = statement else {
        panic!("expected create table statement");
    };
    let binding = source_backed_table_definition_from_sql(&definition)
        .expect("source-backed table")
        .expect("binding");
    assert_eq!(binding.table_name(), "orders");
    assert_eq!(binding.source_name(), "pg_main");
    assert_eq!(binding.upstream_table(), "public.orders");
}

#[test]
fn replication_pipeline_definition_from_sql_preserves_postgres_target() {
    let statement = parse_floe_statement(
        "CREATE REPLICATION PIPELINE pg_orders_to_postgres
         FROM pg_main TABLE public.orders
         INTO POSTGRES WITH (
            connection = 'postgres://postgres:postgres@localhost/postgres',
            table = 'public.orders_copy',
            error.policy = 'dead_letter_and_continue',
            error.max_retries = 5
         )",
    )
    .expect("parse pipeline");
    let FloeStatement::CreateReplicationPipeline(definition) = statement else {
        panic!("expected replication pipeline statement");
    };

    let pipeline = replication_pipeline_definition_from_sql(&definition).expect("catalog pipeline");

    assert_eq!(pipeline.name(), "pg_orders_to_postgres");
    assert_eq!(pipeline.source_name(), "pg_main");
    assert_eq!(pipeline.upstream_table(), "public.orders");
    assert_eq!(
        pipeline.format(),
        CatalogReplicationPipelineFormat::FloeJson
    );
    assert_eq!(
        pipeline.target(),
        &CatalogReplicationPipelineTarget::Postgres {
            connection: "postgres://postgres:postgres@localhost/postgres".to_string(),
            table: "public.orders_copy".to_string(),
        }
    );
    assert_eq!(
        pipeline.error_policy().mode(),
        CatalogReplicationErrorPolicyMode::DeadLetterAndContinue
    );
    assert_eq!(pipeline.error_policy().max_retries(), Some(5));
}

#[test]
fn catalog_postgres_source_connector_merges_include_tables() {
    let mut connector_specs = Vec::new();
    let mut catalog_sources = HashMap::new();
    let source = CatalogSourceDefinition::new(
        "pg_main",
        CatalogSourceConnector::PostgresCdc(
            PostgresCdcSourceDefinition::new_with_setup_policy(
                "postgres://postgres:postgres@localhost/postgres",
                "floe_slot",
                Some("floe_pub".to_string()),
                Some(false),
                CatalogPostgresCdcSchemaEvolutionPolicy::IgnoreCompatible,
                false,
                true,
            )
            .expect("postgres source"),
        ),
    )
    .expect("source");
    catalog_sources.insert(source.name().to_string(), source);
    let mut source_tables = HashMap::new();
    let binding =
        SourceBackedTableDefinition::new("orders", "pg_main", "public.orders").expect("binding");
    source_tables.insert(binding.table_name().to_string(), binding);

    merge_catalog_source_connectors(
        &mut connector_specs,
        &catalog_sources,
        &source_tables,
        &HashMap::new(),
    )
    .expect("merge connector");

    assert_eq!(connector_specs.len(), 1);
    assert_eq!(connector_specs[0].name, "pg_main");
    let ConnectorConfig::PostgresCdc {
        connection,
        slot,
        publication,
        include_tables,
        include_schema_in_source,
        schema_evolution_policy,
        auto_create_slot,
        auto_create_publication,
        ..
    } = &connector_specs[0].config
    else {
        panic!("expected postgres cdc connector");
    };
    assert_eq!(
        connection,
        "postgres://postgres:postgres@localhost/postgres"
    );
    assert_eq!(slot, "floe_slot");
    assert_eq!(publication.as_deref(), Some("floe_pub"));
    assert_eq!(
        include_tables.as_deref(),
        Some(&["public.orders".to_string()][..])
    );
    assert_eq!(include_schema_in_source.as_ref().copied(), Some(false));
    assert_eq!(
        schema_evolution_policy.as_ref().copied(),
        Some(CatalogPostgresCdcSchemaEvolutionPolicy::IgnoreCompatible)
    );
    assert_eq!(auto_create_slot.as_ref().copied(), Some(false));
    assert_eq!(auto_create_publication.as_ref().copied(), Some(true));
}

#[test]
fn catalog_postgres_source_connector_merges_pipeline_tables() {
    let mut connector_specs = Vec::new();
    let mut catalog_sources = HashMap::new();
    let source = CatalogSourceDefinition::new(
        "pg_main",
        CatalogSourceConnector::PostgresCdc(
            PostgresCdcSourceDefinition::new(
                "postgres://postgres:postgres@localhost/postgres",
                "floe_slot",
                Some("floe_pub".to_string()),
                Some(false),
            )
            .expect("postgres source"),
        ),
    )
    .expect("source");
    catalog_sources.insert(source.name().to_string(), source);
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
    let mut pipelines = HashMap::new();
    pipelines.insert(pipeline.name().to_string(), pipeline);

    merge_catalog_source_connectors(
        &mut connector_specs,
        &catalog_sources,
        &HashMap::new(),
        &pipelines,
    )
    .expect("merge connector");

    let ConnectorConfig::PostgresCdc { include_tables, .. } = &connector_specs[0].config else {
        panic!("expected postgres cdc connector");
    };
    assert_eq!(
        include_tables.as_deref(),
        Some(&["public.orders".to_string()][..])
    );
}
