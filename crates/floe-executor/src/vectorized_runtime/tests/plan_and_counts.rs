#[tokio::test]
async fn unsupported_shapes_are_rejected_without_incremental_operators() {
    let mut sources = SourceRegistry::new();
    sources.register(
        SourceDefinition::new(
            "orders",
            vec![
                SourceColumn::new_nullable("id", SourceDataType::Int64, false),
                SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
            ],
        )
        .expect("orders source definition"),
    );
    sources.register(
        SourceDefinition::new(
            "bids",
            vec![
                SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
                SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
                SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            ],
        )
        .expect("bids source definition"),
    );
    sources.register(
        SourceDefinition::new(
            "auctions",
            vec![
                SourceColumn::new_nullable("id", SourceDataType::Int64, false),
                SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
                SourceColumn::new_nullable("initial_bid", SourceDataType::Int64, false),
            ],
        )
        .expect("auctions source definition"),
    );
    sources.register(
        SourceDefinition::new(
            "people",
            vec![
                SourceColumn::new_nullable("id", SourceDataType::Int64, false),
                SourceColumn::new_nullable("name", SourceDataType::Utf8, false),
            ],
        )
        .expect("people source definition"),
    );
    sources.register(
        SourceDefinition::new(
            "auction",
            vec![
                SourceColumn::new_nullable("id", SourceDataType::Int64, false),
                SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, false),
            ],
        )
        .expect("auction source definition"),
    );
    sources.register(
        SourceDefinition::new(
            "bid",
            vec![
                SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
                SourceColumn::new_nullable("price", SourceDataType::Int64, false),
                SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, false),
            ],
        )
        .expect("bid source definition"),
    );
    sources.register(
        SourceDefinition::new(
            "auction_sellers",
            vec![SourceColumn::new_nullable(
                "seller",
                SourceDataType::Int64,
                false,
            )],
        )
        .expect("auction sellers source definition"),
    );

    let id_note_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("note", DataType::Utf8, false),
    ]));
    let count_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, false)]));
    let person_price_schema = Arc::new(Schema::new(vec![
        Field::new("person_id", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let asof_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("price", DataType::Int64, true),
    ]));
    let auction_seller_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("seller", DataType::Int64, false),
    ]));
    let auction_price_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let auction_schema = Arc::new(Schema::new(vec![Field::new(
        "auction",
        DataType::Int64,
        false,
    )]));
    let key_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let cases: Vec<(&str, &str, SchemaRef)> = vec![
        (
            "mv_values_join",
            "SELECT o.id, v.note FROM orders AS o JOIN \
             (VALUES (1, 'one'), (3, 'three')) AS v(id, note) ON o.id = v.id",
            Arc::clone(&id_note_schema),
        ),
        (
            "mv_distinct_aggregate",
            "SELECT COUNT(auction) AS c FROM (SELECT DISTINCT auction, bidder FROM bids) d",
            Arc::clone(&count_schema),
        ),
        (
            "mv_three_way_join",
            "SELECT p.id AS person_id, b.price \
             FROM people p \
             JOIN auctions a ON p.id = a.seller \
             JOIN bids b ON a.id = b.auction",
            Arc::clone(&person_price_schema),
        ),
        (
            "mv_three_way_aggregate",
            "SELECT p.id AS person_id, COUNT(b.price) AS price \
             FROM people p \
             JOIN auctions a ON p.id = a.seller \
             JOIN bids b ON a.id = b.auction \
             GROUP BY p.id",
            Arc::clone(&person_price_schema),
        ),
        (
            "mv_asof_join",
            "SELECT a.id, b.price \
             FROM auction a ASOF JOIN bid b \
             MATCH_CONDITION (b.\"dateTime\" <= a.\"dateTime\") \
             ON a.id = b.auction",
            Arc::clone(&asof_schema),
        ),
        (
            "mv_distinct_join",
            "SELECT d.auction, a.seller \
             FROM (SELECT DISTINCT auction FROM bids) d \
             JOIN auctions a ON d.auction = a.id",
            Arc::clone(&auction_seller_schema),
        ),
        (
            "mv_subquery",
            "SELECT auction, price FROM bids WHERE auction IN (SELECT id FROM auctions)",
            Arc::clone(&auction_price_schema),
        ),
        (
            "mv_self_join",
            "SELECT l.id AS auction, r.amount AS price \
             FROM orders l JOIN orders r ON l.id = r.id",
            Arc::clone(&auction_price_schema),
        ),
        (
            "mv_range_join",
            "SELECT b.auction, a.seller \
             FROM bids b JOIN auctions a ON b.price >= a.initial_bid",
            Arc::clone(&auction_seller_schema),
        ),
        (
            "mv_distinct_topn",
            "SELECT DISTINCT auction FROM bids ORDER BY auction DESC LIMIT 2",
            Arc::clone(&auction_schema),
        ),
        (
            "mv_global_join_topn",
            "SELECT b.auction, a.seller \
             FROM bids b JOIN auctions a ON b.auction = a.id \
             ORDER BY b.price DESC LIMIT 2",
            Arc::clone(&auction_seller_schema),
        ),
        (
            "mv_aggregate_over_join_topn",
            "SELECT auction, COUNT(*) AS price \
             FROM (SELECT b.auction, b.price \
                   FROM bids b JOIN auctions a ON b.auction = a.id \
                   ORDER BY b.price DESC LIMIT 2) t \
             GROUP BY auction",
            Arc::clone(&auction_price_schema),
        ),
        (
            "mv_union_topn",
            "SELECT key \
             FROM (SELECT auction AS key FROM bids UNION ALL SELECT id AS key FROM auctions) u \
             ORDER BY key DESC LIMIT 2",
            Arc::clone(&key_schema),
        ),
        (
            "mv_union_over_distinct",
            "SELECT key \
             FROM (SELECT DISTINCT auction AS key FROM bids \
             UNION ALL SELECT id AS key FROM auctions) u",
            Arc::clone(&key_schema),
        ),
        (
            "mv_join_over_join",
            "SELECT j.auction, a.seller \
             FROM (SELECT l.auction, r.price FROM bids l JOIN bids r ON l.auction = r.auction WHERE l.price < r.price) j \
             JOIN auctions a ON j.auction = a.id",
            Arc::clone(&auction_seller_schema),
        ),
    ];

    for (view_name, query, schema) in cases {
        assert_incremental_plan_rejected(&sources, view_name, query, schema).await;
    }
}
#[tokio::test]
async fn pruned_execution_batches_do_not_prune_query_provider() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new("id", SourceDataType::Int64),
            SourceColumn::new("note", SourceDataType::Utf8),
        ],
    )
    .expect("source definition");
    let required_columns = Some(Arc::<[bool]>::from(vec![true, false]));
    let mut builder = SourceArrowBatchBuilder::new_with_execution_required_columns(
        definition.clone(),
        1,
        required_columns,
    );
    builder
        .append_event(&AppendIngestEvent::new(
            "orders",
            json!({"id": 1, "note": "kept"}),
        ))
        .expect("append source event");
    let batches = builder
        .finish()
        .expect("finish source batches")
        .expect("source batches");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-query-provider-pruned").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_orders",
            "SELECT id FROM orders",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default()
            .with_source_query_tables()
            .with_operator_state_table(table),
    )
    .await
    .expect("runtime");

    let SourceArrowBatches::ExecutionAndQuery { execution, query } = batches else {
        panic!("expected execution and query batches");
    };
    runtime
        .append_source_batches_for_execution_and_query("orders", vec![execution], vec![query])
        .await
        .expect("append source batches");
    runtime.run_tick(1).await.expect("run vectorized tick");

    let snapshot = scan_materialized_view_table(
        Arc::clone(&registry),
        "mv_orders",
        Arc::clone(&output_schema),
        "SELECT id FROM mv_orders",
    )
    .await;
    let id = snapshot[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("mv id column")
        .value(0);
    assert_eq!(id, 1);

    let provider = runtime
        .table_providers()
        .into_iter()
        .find_map(|(name, provider)| (name == "orders").then_some(provider))
        .expect("orders query provider");
    let ctx = SessionContext::new();
    ctx.register_table("orders", provider)
        .expect("register query provider");
    let batches = ctx
        .sql("SELECT note FROM orders")
        .await
        .expect("query provider sql")
        .collect()
        .await
        .expect("collect query provider rows");
    let note = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("note column")
        .value(0);
    assert_eq!(note, "kept");
}

#[tokio::test]
async fn primary_key_cdc_delta_updates_filter_project_mv_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new("id", SourceDataType::Int64),
            SourceColumn::new("amount", SourceDataType::Int64),
        ],
    )
    .expect("source definition")
    .with_property(SOURCE_PRIMARY_KEY_PROPERTY, "id");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 30])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-primary-key-cdc-stateless").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_orders",
            "SELECT id, amount FROM orders WHERE amount >= 20",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default()
            .with_source_query_tables()
            .with_operator_state_table(table),
    )
    .await
    .expect("runtime");

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let update_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 40, 30])),
        ],
    )
    .expect("cdc source rows");
    let weighted = weighted_batch_from_diffs(&update_rows, &weighted_schema, &[-1, 1, -1])
        .expect("weighted cdc rows");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply cdc delta");
    runtime.run_tick(2).await.expect("cdc tick");

    let handle = registry.get("mv_orders").expect("materialized view");
    let snapshot = scan_materialized_view_table(
        Arc::clone(&registry),
        "mv_orders",
        Arc::clone(&output_schema),
        "SELECT id, amount FROM mv_orders",
    )
    .await;
    assert_eq!(id_count_rows(&snapshot), vec![(1, 40)]);

    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(1, 40, 1), (2, 30, -1)]
    );

    let provider = runtime
        .table_providers()
        .into_iter()
        .find_map(|(name, provider)| (name == "orders").then_some(provider))
        .expect("orders query provider");
    let ctx = SessionContext::new();
    ctx.register_table("orders", provider)
        .expect("register orders provider");
    let source_rows = ctx
        .sql("SELECT id, amount FROM orders ORDER BY id")
        .await
        .expect("source query")
        .collect()
        .await
        .expect("collect source rows");
    assert_eq!(source_rows.len(), 1);
    assert_eq!(int64_values(&source_rows[0], 0), vec![1]);
    assert_eq!(int64_values(&source_rows[0], 1), vec![40]);
}

#[tokio::test]
async fn filter_project_uses_slate_backed_columnar_stateless_operator_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new("id", SourceDataType::Int64),
            SourceColumn::new("note", SourceDataType::Utf8),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 4])),
            Arc::new(StringArray::from(vec!["a", "b", "d"])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-stateless").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::clone(&schema);
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_orders",
            "SELECT id * 2 AS id, note FROM orders WHERE id * 2 >= 4",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarStateless
    );
    assert!(
        registry.get("mv_orders").is_some(),
        "materialized view handle must exist before the first tick"
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");
    assert_stateless_source_namespace_empty(&table, "mv_orders").await;

    let handle = registry.get("mv_orders").expect("materialized view");
    assert!(handle.arrow_snapshot_for(1).is_none());
    let snapshot = scan_materialized_view_table(
        Arc::clone(&registry),
        "mv_orders",
        Arc::clone(&output_schema),
        "SELECT id, note FROM mv_orders",
    )
    .await;
    assert_eq!(
        id_note_rows(&snapshot),
        vec![(4, "b".to_string()), (8, "d".to_string())]
    );

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let source_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .expect("source delta rows");
    let weighted = weighted_batch_from_diffs(&source_rows, &weighted_schema, &[-1, -1, 1])
        .expect("weighted source rows");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted delta");
    runtime.run_tick(2).await.expect("weighted tick");
    assert_stateless_source_namespace_empty(&table, "mv_orders").await;

    assert!(handle.arrow_snapshot_for(2).is_none());
    let snapshot = scan_materialized_view_table(
        Arc::clone(&registry),
        "mv_orders",
        Arc::clone(&output_schema),
        "SELECT id, note FROM mv_orders",
    )
    .await;
    assert_eq!(
        id_note_rows(&snapshot),
        vec![(6, "c".to_string()), (8, "d".to_string())]
    );
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_note_rows(&delta),
        vec![(4, "b".to_string(), -1), (6, "c".to_string(), 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_orders",
            "SELECT id * 2 AS id, note FROM orders WHERE id * 2 >= 4",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarStateless
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_orders")
        .expect("recovered materialized view");
    assert!(recovered_handle.arrow_snapshot_for(3).is_none());
    let recovered_snapshot = scan_materialized_view_table(
        Arc::clone(&recovery_registry),
        "mv_orders",
        Arc::clone(&output_schema),
        "SELECT id, note FROM mv_orders",
    )
    .await;
    assert_eq!(
        id_note_rows(&recovered_snapshot),
        vec![(6, "c".to_string()), (8, "d".to_string())]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn values_relation_uses_slate_backed_columnar_constant_operator() {
    let sources = SourceRegistry::new();
    let table = build_operator_state_table("vectorized-columnar-constant-values").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("note", DataType::Utf8, false),
    ]));
    let query = "SELECT id, note FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, note)";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_values",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarConstant
    );

    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_values").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("initial snapshot");
    assert_eq!(
        id_note_rows(&snapshot),
        vec![(1, "a".to_string()), (2, "b".to_string())]
    );
    let delta = handle.arrow_delta_for(1).expect("initial delta");
    assert_eq!(
        weighted_id_note_rows(&delta),
        vec![(1, "a".to_string(), 1), (2, "b".to_string(), 1)]
    );

    runtime.run_tick(2).await.expect("stable tick");
    let snapshot = handle.arrow_snapshot_for(2).expect("stable snapshot");
    assert_eq!(
        id_note_rows(&snapshot),
        vec![(1, "a".to_string()), (2, "b".to_string())]
    );
    let delta = handle.arrow_delta_for(2).expect("stable empty delta");
    assert!(delta.iter().all(|batch| batch.num_rows() == 0));

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_values",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarConstant
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_values")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        id_note_rows(&recovered_snapshot),
        vec![(1, "a".to_string()), (2, "b".to_string())]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn empty_values_relation_persists_columnar_constant_state() {
    let sources = SourceRegistry::new();
    let table = build_operator_state_table("vectorized-columnar-constant-empty-values").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("note", DataType::Utf8, false),
    ]));
    let query = "SELECT id, note FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, note) WHERE id > 10";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_empty_values",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarConstant
    );

    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_empty_values").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("empty snapshot");
    assert!(id_note_rows(&snapshot).is_empty());
    let delta = handle.arrow_delta_for(1).expect("empty delta");
    assert!(delta.iter().all(|batch| batch.num_rows() == 0));
    assert!(
        table
            .get_bytes(b"mv/mv_empty_values/columnar/constant/state/initialized")
            .await
            .expect("read initialized marker")
            .is_some()
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_empty_values",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarConstant
    );
    recovered.run_tick(2).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_empty_values")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(2)
        .expect("recovered empty snapshot");
    assert!(id_note_rows(&recovered_snapshot).is_empty());
    let recovered_delta = recovered_handle
        .arrow_delta_for(2)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn sort_passthrough_uses_slate_backed_columnar_stateless_operator_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![3, 1, 2]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-sort-stateless").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::clone(&schema);
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_orders",
            "SELECT id FROM orders ORDER BY id DESC",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarStateless
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_orders").expect("materialized view");
    assert!(handle.arrow_snapshot_for(1).is_none());
    let snapshot = scan_materialized_view_table(
        Arc::clone(&registry),
        "mv_orders",
        Arc::clone(&output_schema),
        "SELECT id FROM mv_orders",
    )
    .await;
    assert_eq!(single_int_rows(&snapshot), vec![1, 2, 3]);

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let source_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2, 4]))],
    )
    .expect("source delta rows");
    let weighted = weighted_batch_from_diffs(&source_rows, &weighted_schema, &[-1, 1])
        .expect("weighted source rows");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted delta");
    runtime.run_tick(2).await.expect("weighted tick");

    assert!(handle.arrow_snapshot_for(2).is_none());
    let snapshot = scan_materialized_view_table(
        Arc::clone(&registry),
        "mv_orders",
        Arc::clone(&output_schema),
        "SELECT id FROM mv_orders",
    )
    .await;
    assert_eq!(single_int_rows(&snapshot), vec![1, 3, 4]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, -1), (4, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_orders",
            "SELECT id FROM orders ORDER BY id DESC",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarStateless
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_orders")
        .expect("recovered materialized view");
    assert!(recovered_handle.arrow_snapshot_for(3).is_none());
    let recovered_snapshot = scan_materialized_view_table(
        Arc::clone(&recovery_registry),
        "mv_orders",
        Arc::clone(&output_schema),
        "SELECT id FROM mv_orders",
    )
    .await;
    assert_eq!(single_int_rows(&recovered_snapshot), vec![1, 3, 4]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn union_all_uses_slate_backed_columnar_operator_incrementally() {
    let orders = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("orders source definition");
    let shipments = SourceDefinition::new(
        "shipments",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("shipments source definition");
    let schema = orders.to_arrow_schema();
    let initial_orders = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .expect("initial orders");
    let initial_shipments = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2, 4]))],
    )
    .expect("initial shipments");

    let mut sources = SourceRegistry::new();
    sources.register(orders);
    sources.register(shipments);
    let table = build_operator_state_table("vectorized-columnar-union").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_union_ids",
            "SELECT id FROM orders WHERE id <= 2 UNION ALL SELECT id FROM shipments WHERE id >= 2",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarUnion
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial_orders.clone()],
            vec![initial_orders],
        )
        .await
        .expect("append initial orders");
    runtime
        .append_source_batches_for_execution_and_query(
            "shipments",
            vec![initial_shipments.clone()],
            vec![initial_shipments],
        )
        .await
        .expect("append initial shipments");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_union_ids").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 2, 2, 4]);

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let order_retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2]))],
    )
    .expect("order retract");
    let weighted = weighted_batch_from_diffs(&order_retract, &weighted_schema, &[-1])
        .expect("weighted order retract");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply order retract");
    let shipment_insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![5]))],
    )
    .expect("shipment insert");
    let weighted = weighted_batch_from_diffs(&shipment_insert, &weighted_schema, &[1])
        .expect("weighted shipment insert");
    runtime
        .apply_weighted_source_delta("shipments", weighted)
        .await
        .expect("apply shipment insert");
    runtime.run_tick(2).await.expect("weighted tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(single_int_rows(&snapshot), vec![1, 2, 4, 5]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, -1), (5, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_union_ids",
            "SELECT id FROM orders WHERE id <= 2 UNION ALL SELECT id FROM shipments WHERE id >= 2",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarUnion
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_union_ids")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![1, 2, 4, 5]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let shipment_retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2]))],
    )
    .expect("shipment retract");
    let weighted = weighted_batch_from_diffs(&shipment_retract, &weighted_schema, &[-1])
        .expect("weighted shipment retract");
    recovered
        .apply_weighted_source_delta("shipments", weighted)
        .await
        .expect("apply shipment retract");
    recovered.run_tick(4).await.expect("post-recovery tick");

    let snapshot = recovered_handle.arrow_snapshot_for(4).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 4, 5]);
    let delta = recovered_handle.arrow_delta_for(4).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, -1)]);
}

#[tokio::test]
async fn source_union_values_relation_uses_slate_backed_columnar_union_operator() {
    let orders = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("orders source definition");
    let schema = orders.to_arrow_schema();
    let initial_orders = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .expect("initial orders");

    let mut sources = SourceRegistry::new();
    sources.register(orders);
    let table = build_operator_state_table("vectorized-columnar-union-values").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let query = "SELECT id FROM orders UNION ALL \
                 SELECT id FROM (VALUES (2), (4)) AS v(id)";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_union_values",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarUnion
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial_orders.clone()],
            vec![initial_orders],
        )
        .await
        .expect("append initial orders");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_union_values").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("initial snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 2, 2, 4]);
    let delta = handle.arrow_delta_for(1).expect("initial delta");
    assert_eq!(
        weighted_single_int_rows(&delta),
        vec![(1, 1), (2, 1), (2, 1), (4, 1)]
    );

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let source_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2, 5]))],
    )
    .expect("source delta rows");
    let weighted = weighted_batch_from_diffs(&source_rows, &weighted_schema, &[-1, 1])
        .expect("weighted source rows");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted source delta");
    runtime.run_tick(2).await.expect("weighted tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("updated snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 2, 4, 5]);
    let delta = handle.arrow_delta_for(2).expect("updated delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, -1), (5, 1)]);

    table
        .delete(b"mv/mv_union_values/columnar/union/constant_1/state/initialized")
        .await
        .expect("delete initialized marker");

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_union_values",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarUnion
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_union_values")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![1, 2, 4, 5]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn union_distinct_uses_slate_backed_columnar_operator_incrementally() {
    let orders = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("orders source definition");
    let shipments = SourceDefinition::new(
        "shipments",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("shipments source definition");
    let schema = orders.to_arrow_schema();
    let initial_orders = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .expect("initial orders");
    let initial_shipments = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2, 4]))],
    )
    .expect("initial shipments");

    let mut sources = SourceRegistry::new();
    sources.register(orders);
    sources.register(shipments);
    let table = build_operator_state_table("vectorized-columnar-union-distinct").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_union_ids",
            "SELECT id FROM orders WHERE id <= 2 UNION SELECT id FROM shipments WHERE id >= 2",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarUnion
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial_orders.clone()],
            vec![initial_orders],
        )
        .await
        .expect("append initial orders");
    runtime
        .append_source_batches_for_execution_and_query(
            "shipments",
            vec![initial_shipments.clone()],
            vec![initial_shipments],
        )
        .await
        .expect("append initial shipments");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_union_ids").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 2, 4]);

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let order_retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2]))],
    )
    .expect("order retract");
    let weighted = weighted_batch_from_diffs(&order_retract, &weighted_schema, &[-1])
        .expect("weighted order retract");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply order retract");
    runtime.run_tick(2).await.expect("duplicate retract tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(single_int_rows(&snapshot), vec![1, 2, 4]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert!(delta.iter().all(|batch| batch.num_rows() == 0));

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_union_ids",
            "SELECT id FROM orders WHERE id <= 2 UNION SELECT id FROM shipments WHERE id >= 2",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarUnion
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_union_ids")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(single_int_rows(&recovered_snapshot), vec![1, 2, 4]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let shipment_retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2]))],
    )
    .expect("shipment retract");
    let weighted = weighted_batch_from_diffs(&shipment_retract, &weighted_schema, &[-1])
        .expect("weighted shipment retract");
    recovered
        .apply_weighted_source_delta("shipments", weighted)
        .await
        .expect("apply shipment retract");
    recovered.run_tick(4).await.expect("post-recovery tick");

    let snapshot = recovered_handle.arrow_snapshot_for(4).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 4]);
    let delta = recovered_handle.arrow_delta_for(4).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, -1)]);
}

#[tokio::test]
async fn ordered_union_distinct_uses_slate_backed_columnar_operator_incrementally() {
    let orders = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("orders source definition");
    let shipments = SourceDefinition::new(
        "shipments",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("shipments source definition");
    let schema = orders.to_arrow_schema();
    let initial_orders = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .expect("initial orders");
    let initial_shipments = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2, 4]))],
    )
    .expect("initial shipments");

    let mut sources = SourceRegistry::new();
    sources.register(orders);
    sources.register(shipments);
    let table = build_operator_state_table("vectorized-columnar-ordered-union-distinct").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let query = "SELECT id FROM orders WHERE id <= 2 UNION SELECT id FROM shipments WHERE id >= 2 ORDER BY id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_ordered_union_ids",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarUnion
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial_orders.clone()],
            vec![initial_orders],
        )
        .await
        .expect("append initial orders");
    runtime
        .append_source_batches_for_execution_and_query(
            "shipments",
            vec![initial_shipments.clone()],
            vec![initial_shipments],
        )
        .await
        .expect("append initial shipments");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_ordered_union_ids")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 2, 4]);

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let order_retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2]))],
    )
    .expect("order retract");
    let weighted = weighted_batch_from_diffs(&order_retract, &weighted_schema, &[-1])
        .expect("weighted order retract");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply order retract");
    runtime.run_tick(2).await.expect("duplicate retract tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(single_int_rows(&snapshot), vec![1, 2, 4]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert!(delta.iter().all(|batch| batch.num_rows() == 0));

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_ordered_union_ids",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarUnion
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_ordered_union_ids")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![1, 2, 4]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let shipment_retract = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2]))],
    )
    .expect("shipment retract");
    let weighted = weighted_batch_from_diffs(&shipment_retract, &weighted_schema, &[-1])
        .expect("weighted shipment retract");
    recovered
        .apply_weighted_source_delta("shipments", weighted)
        .await
        .expect("apply shipment retract");
    recovered.run_tick(4).await.expect("post-recovery tick");

    let snapshot = recovered_handle.arrow_snapshot_for(4).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![1, 4]);
    let delta = recovered_handle.arrow_delta_for(4).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(2, -1)]);
}

#[tokio::test]
async fn count_group_by_uses_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 1, 2]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-count").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("count", DataType::Int64, false),
    ]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_counts",
            "SELECT id, COUNT(*) AS count FROM orders GROUP BY id",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_order_counts").expect("materialized view");
    let version = handle.latest_version().expect("mv version");
    let snapshot = handle.arrow_snapshot_for(version).expect("mv snapshot");
    assert_eq!(snapshot.len(), 1);
    assert_eq!(int64_values(&snapshot[0], 0), vec![1, 2]);
    assert_eq!(int64_values(&snapshot[0], 1), vec![2, 1]);

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let source_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .expect("source delta rows");
    let weighted = weighted_batch_from_diffs(&source_rows, &weighted_schema, &[-1, 1, 1])
        .expect("weighted source rows");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted delta");
    runtime.run_tick(2).await.expect("weighted tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(snapshot.len(), 1);
    assert_eq!(int64_values(&snapshot[0], 0), vec![1, 2, 3]);
    assert_eq!(int64_values(&snapshot[0], 1), vec![1, 2, 1]);

    let delta = handle.arrow_delta_for(2).expect("mv delta");
    let delta = delta
        .iter()
        .filter(|batch| batch.num_rows() > 0)
        .collect::<Vec<_>>();
    assert_eq!(delta.len(), 1);
    let weight_idx = delta[0]
        .schema()
        .index_of(WEIGHT_COLUMN_NAME)
        .expect("weight column");
    assert_eq!(int64_values(delta[0], 0), vec![1, 1, 2, 2, 3]);
    assert_eq!(int64_values(delta[0], 1), vec![2, 1, 1, 2, 1]);
    assert_eq!(int64_values(delta[0], weight_idx), vec![-1, 1, -1, 1, 1]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_counts",
            "SELECT id, COUNT(*) AS count FROM orders GROUP BY id",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_counts")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(recovered_snapshot.len(), 1);
    assert_eq!(int64_values(&recovered_snapshot[0], 0), vec![1, 2, 3]);
    assert_eq!(int64_values(&recovered_snapshot[0], 1), vec![1, 2, 1]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn distinct_uses_slate_backed_grouped_count_state_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 1, 2]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-distinct").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_ids",
            "SELECT DISTINCT id FROM orders",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedCount
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_order_ids").expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(single_int_rows(&snapshot), vec![1, 2]);

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let retract_one = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .expect("source retract row");
    let weighted = weighted_batch_from_diffs(&retract_one, &weighted_schema, &[-1])
        .expect("weighted source row");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    runtime.run_tick(2).await.expect("duplicate retract tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(single_int_rows(&snapshot), vec![1, 2]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert!(delta.iter().all(|batch| batch.num_rows() == 0));

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_ids",
            "SELECT DISTINCT id FROM orders",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedCount
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_ids")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(single_int_rows(&recovered_snapshot), vec![1, 2]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract_last = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .expect("source retract row");
    let weighted = weighted_batch_from_diffs(&retract_last, &weighted_schema, &[-1])
        .expect("weighted source row");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(4).await.expect("last retract tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 4)
            .await;
    assert_eq!(single_int_rows(&snapshot), vec![2]);
    let delta = recovered_handle.arrow_delta_for(4).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(1, -1)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![3]))],
    )
    .expect("source insert row");
    let weighted =
        weighted_batch_from_diffs(&insert, &weighted_schema, &[1]).expect("weighted source row");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted insert");
    recovered.run_tick(5).await.expect("insert tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 5)
            .await;
    assert_eq!(single_int_rows(&snapshot), vec![2, 3]);
    let delta = recovered_handle.arrow_delta_for(5).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(3, 1)]);
}

#[tokio::test]
async fn ordered_distinct_uses_slate_backed_grouped_count_state_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 1, 2]))],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-ordered-distinct").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let query = "SELECT DISTINCT id FROM orders ORDER BY id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_ids_ordered",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedCount
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_order_ids_ordered")
        .expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(single_int_rows(&snapshot), vec![1, 2]);

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let retract_one = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .expect("source retract row");
    let weighted = weighted_batch_from_diffs(&retract_one, &weighted_schema, &[-1])
        .expect("weighted source row");
    runtime
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    runtime.run_tick(2).await.expect("duplicate retract tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(single_int_rows(&snapshot), vec![1, 2]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert!(delta.iter().all(|batch| batch.num_rows() == 0));

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_ids_ordered",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedCount
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_ids_ordered")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(single_int_rows(&recovered_snapshot), vec![1, 2]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract_last = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .expect("source retract row");
    let weighted = weighted_batch_from_diffs(&retract_last, &weighted_schema, &[-1])
        .expect("weighted source row");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply last duplicate retract");
    recovered
        .run_tick(4)
        .await
        .expect("last duplicate retract tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 4)
            .await;
    assert_eq!(single_int_rows(&snapshot), vec![2]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(1, -1)]);
}

#[tokio::test]
async fn grouped_count_with_hidden_key_uses_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("ts", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 10])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-count-hidden").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("count", DataType::Int64, false),
    ]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_counts",
            "SELECT id, COUNT(*) AS count FROM orders GROUP BY id, ts",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedCount
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_order_counts").expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(id_count_rows(&snapshot), vec![(1, 1), (1, 1), (2, 1)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("orders", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(id_count_rows(&snapshot), vec![(1, 1), (1, 2), (2, 1)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 1, -1), (1, 2, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_counts",
            "SELECT id, COUNT(*) AS count FROM orders GROUP BY id, ts",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedCount
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_counts")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(
        id_count_rows(&recovered_snapshot),
        vec![(1, 1), (1, 2), (2, 1)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let hidden_key_insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![20])),
        ],
    )
    .expect("hidden key insert rows");
    recovered
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![hidden_key_insert.clone()],
            vec![hidden_key_insert],
        )
        .await
        .expect("append hidden key source rows");
    recovered.run_tick(4).await.expect("post-recovery tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 4)
            .await;
    assert_eq!(id_count_rows(&snapshot), vec![(1, 2), (1, 2), (2, 1)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-recovery delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 1, -1), (1, 2, 1)]);
}

#[tokio::test]
async fn append_only_hop_grouped_count_recovers_compact_state() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, false),
        ],
    )
    .expect("source definition")
    .with_property("append_only", "true");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1])),
            Arc::new(TimestampMillisecondArray::from(vec![1000, 2000])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-count-hop-append").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("count", DataType::Int64, false),
    ]));
    let mut runtime = VectorizedExecutionRuntime::new_with_udfs_and_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_bid_counts",
            r#"SELECT auction, COUNT(*) AS count FROM bids GROUP BY auction, HOP("dateTime", 1000, 3000)"#,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        vec![test_hop_udf()],
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedCount
    );

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_bid_counts").expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(
        id_count_rows(&snapshot),
        vec![(1, 1), (1, 1), (1, 2), (1, 2)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_udfs_and_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_bid_counts",
            r#"SELECT auction, COUNT(*) AS count FROM bids GROUP BY auction, HOP("dateTime", 1000, 3000)"#,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        vec![test_hop_udf()],
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    recovered.run_tick(2).await.expect("recovered empty tick");

    let duplicate = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(TimestampMillisecondArray::from(vec![1000])),
        ],
    )
    .expect("duplicate source batch");
    recovered
        .append_source_batches_for_execution_and_query(
            "bids",
            vec![duplicate.clone()],
            vec![duplicate],
        )
        .await
        .expect("append duplicate source row");
    recovered.run_tick(3).await.expect("post-recovery tick");

    let recovered_handle = recovery_registry
        .get("mv_bid_counts")
        .expect("recovered materialized view");
    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(
        id_count_rows(&snapshot),
        vec![(1, 1), (1, 2), (1, 3), (1, 3)]
    );
    let delta = recovered_handle
        .arrow_delta_for(3)
        .expect("post-recovery delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![
            (1, 1, -1),
            (1, 2, -1),
            (1, 2, -1),
            (1, 2, 1),
            (1, 3, 1),
            (1, 3, 1),
        ]
    );
}

#[tokio::test]
async fn append_only_tumble_grouped_count_uses_compact_state() {
    let definition = SourceDefinition::new(
        "bids",
        vec![
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, false),
        ],
    )
    .expect("source definition")
    .with_property("append_only", "true");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(TimestampMillisecondArray::from(vec![1000, 2000, 11_000])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-count-tumble-append").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("bidder", DataType::Int64, false),
        Field::new("count", DataType::Int64, false),
    ]));
    let mut runtime = VectorizedExecutionRuntime::new_with_udfs_and_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_bid_counts",
            r#"SELECT bidder, COUNT(*) AS count FROM bids GROUP BY bidder, TUMBLE("dateTime", 10000)"#,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        vec![test_tumble_udf()],
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");

    runtime
        .append_source_batches_for_execution_and_query("bids", vec![initial.clone()], vec![initial])
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_bid_counts").expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(id_count_rows(&snapshot), vec![(1, 2), (2, 1)]);

    let mv_namespace = namespaces::materialized_view("mv_bid_counts").expect("MV namespace");
    let fast_log_namespace =
        format!("{mv_namespace}/columnar/grouped_count/state__append_only_single_hop_count_log");
    let fast_log_prefix = keyspace::namespace_prefix(keyspace::prefix::INDEX, &fast_log_namespace);
    assert!(
        !table
            .scan_prefix(&fast_log_prefix, &ScanOptions::default())
            .await
            .expect("scan compact TUMBLE state log")
            .is_empty(),
        "TUMBLE count should use the typed append-only fixed-window state"
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_udfs_and_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_bid_counts",
            r#"SELECT bidder, COUNT(*) AS count FROM bids GROUP BY bidder, TUMBLE("dateTime", 10000)"#,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        vec![test_tumble_udf()],
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    recovered.run_tick(2).await.expect("recovered empty tick");

    let duplicate = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(TimestampMillisecondArray::from(vec![1000])),
        ],
    )
    .expect("duplicate source batch");
    recovered
        .append_source_batches_for_execution_and_query(
            "bids",
            vec![duplicate.clone()],
            vec![duplicate],
        )
        .await
        .expect("append duplicate source row");
    recovered.run_tick(3).await.expect("post-recovery tick");

    let recovered_handle = recovery_registry
        .get("mv_bid_counts")
        .expect("recovered materialized view");
    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(id_count_rows(&snapshot), vec![(1, 3), (2, 1)]);
    let delta = recovered_handle
        .arrow_delta_for(3)
        .expect("post-recovery delta");
    assert_eq!(weighted_id_count_rows(&delta), vec![(1, 2, -1), (1, 3, 1)]);
}

#[tokio::test]
async fn grouped_count_supports_boolean_group_key_incrementally() {
    let definition = SourceDefinition::new(
        "events",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("active", SourceDataType::Bool, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(BooleanArray::from(vec![true, false, true])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-count-bool-key").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("active", DataType::Boolean, false),
        Field::new("count", DataType::Int64, false),
    ]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_active_counts",
            "SELECT active, COUNT(*) AS count FROM events GROUP BY active",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedCount
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "events",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_active_counts").expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(bool_count_rows(&snapshot), vec![(false, 1), (true, 2)]);

    let insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(BooleanArray::from(vec![false])),
        ],
    )
    .expect("source insert rows");
    runtime
        .append_source_batches_for_execution_and_query("events", vec![insert.clone()], vec![insert])
        .await
        .expect("append source rows");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(bool_count_rows(&snapshot), vec![(false, 2), (true, 2)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_bool_count_rows(&delta),
        vec![(false, 1, -1), (false, 2, 1)]
    );
}

#[tokio::test]
async fn grouped_max_with_hidden_key_uses_slate_backed_columnar_operator_incrementally() {
    let definition = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("ts", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("source definition");
    let schema = definition.to_arrow_schema();
    let initial = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 10])),
            Arc::new(Int64Array::from(vec![50, 40, 60])),
        ],
    )
    .expect("initial source batch");

    let mut sources = SourceRegistry::new();
    sources.register(definition);
    let table = build_operator_state_table("vectorized-columnar-grouped-max-hidden").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "max_price",
        DataType::Int64,
        false,
    )]));
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_max",
            "SELECT MAX(price) AS max_price FROM orders GROUP BY id, ts",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedMax
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![initial.clone()],
            vec![initial],
        )
        .await
        .expect("append initial source rows");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_order_max").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![40, 50, 60]);

    let lower_insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(Int64Array::from(vec![30])),
        ],
    )
    .expect("lower source insert rows");
    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![lower_insert.clone()],
            vec![lower_insert],
        )
        .await
        .expect("append lower source rows");
    runtime.run_tick(2).await.expect("lower insert tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![40, 50, 60]);
    let delta = handle.arrow_delta_for(2).expect("unchanged max delta");
    assert!(delta.iter().all(|batch| batch.num_rows() == 0));

    let higher_insert = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(Int64Array::from(vec![70])),
        ],
    )
    .expect("higher source insert rows");
    runtime
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![higher_insert.clone()],
            vec![higher_insert],
        )
        .await
        .expect("append higher source rows");
    runtime.run_tick(3).await.expect("higher insert tick");

    let snapshot = handle.arrow_snapshot_for(3).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![40, 60, 70]);
    let delta = handle.arrow_delta_for(3).expect("higher max delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(50, -1), (70, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![VectorizedMaterializedViewPlan::from_sql(
            "mv_order_max",
            "SELECT MAX(price) AS max_price FROM orders GROUP BY id, ts",
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(table),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedMax
    );
    recovered.run_tick(4).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_order_max")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![40, 60, 70]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(4)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let retract_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(Int64Array::from(vec![70])),
        ],
    )
    .expect("retract source rows");
    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&retract_rows, &weighted_schema, &[-1])
        .expect("weighted retract rows");
    recovered
        .apply_weighted_source_delta("orders", weighted)
        .await
        .expect("apply weighted retract");
    recovered.run_tick(5).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(5)
        .expect("post-retract snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![40, 50, 60]);
    let delta = recovered_handle
        .arrow_delta_for(5)
        .expect("post-retract delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(50, 1), (70, -1)]);
}
