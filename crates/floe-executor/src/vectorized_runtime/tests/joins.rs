#[tokio::test]
async fn join_uses_slate_backed_columnar_operator_incrementally() {
    let orders = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("customer_id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
        ],
    )
    .expect("orders source definition");
    let customers = SourceDefinition::new(
        "customers",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("region", SourceDataType::Utf8, false),
        ],
    )
    .expect("customers source definition");
    let orders_schema = orders.to_arrow_schema();
    let customers_schema = customers.to_arrow_schema();
    let initial_orders = RecordBatch::try_new(
        Arc::clone(&orders_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 11, 12])),
            Arc::new(Int64Array::from(vec![50, 60, 70])),
        ],
    )
    .expect("initial orders batch");
    let initial_customers = RecordBatch::try_new(
        Arc::clone(&customers_schema),
        vec![
            Arc::new(Int64Array::from(vec![10, 11])),
            Arc::new(StringArray::from(vec!["west", "east"])),
        ],
    )
    .expect("initial customers batch");

    let mut sources = SourceRegistry::new();
    sources.register(orders);
    sources.register(customers);
    let table = build_operator_state_table("vectorized-columnar-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let query = "SELECT o.id AS order_id, c.region, o.amount \
        FROM orders o JOIN customers c ON o.customer_id = c.id \
        WHERE c.region = 'west'";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_west_orders",
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
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&runtime, "incremental_inner");

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
            "customers",
            vec![initial_customers.clone()],
            vec![initial_customers],
        )
        .await
        .expect("append initial customers");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_west_orders").expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(join_rows(&snapshot), vec![(1, "west".to_string(), 50)]);

    let customer_insert = RecordBatch::try_new(
        Arc::clone(&customers_schema),
        vec![
            Arc::new(Int64Array::from(vec![12])),
            Arc::new(StringArray::from(vec!["west"])),
        ],
    )
    .expect("customer insert batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "customers",
            vec![customer_insert.clone()],
            vec![customer_insert],
        )
        .await
        .expect("append customer insert");
    runtime.run_tick(2).await.expect("right delta tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(
        join_rows(&snapshot),
        vec![(1, "west".to_string(), 50), (3, "west".to_string(), 70)]
    );
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_join_rows(&delta),
        vec![(3, "west".to_string(), 70, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_west_orders",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarJoin
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_west_orders")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(
        join_rows(&recovered_snapshot),
        vec![(1, "west".to_string(), 50), (3, "west".to_string(), 70)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let order_insert = RecordBatch::try_new(
        Arc::clone(&orders_schema),
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(Int64Array::from(vec![12])),
            Arc::new(Int64Array::from(vec![80])),
        ],
    )
    .expect("order insert batch");
    recovered
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![order_insert.clone()],
            vec![order_insert],
        )
        .await
        .expect("append order insert");
    recovered.run_tick(4).await.expect("left delta tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 4)
            .await;
    assert_eq!(
        join_rows(&snapshot),
        vec![
            (1, "west".to_string(), 50),
            (3, "west".to_string(), 70),
            (4, "west".to_string(), 80),
        ]
    );
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-insert delta");
    assert_eq!(
        weighted_join_rows(&delta),
        vec![(4, "west".to_string(), 80, 1)]
    );

    let customer_retract = RecordBatch::try_new(
        Arc::clone(&customers_schema),
        vec![
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(StringArray::from(vec!["west"])),
        ],
    )
    .expect("customer retract batch");
    let weighted_schema = crate::delta_consolidation::weighted_snapshot_schema(&customers_schema)
        .expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&customer_retract, &weighted_schema, &[-1])
        .expect("weighted customer retract");
    recovered
        .apply_weighted_source_delta("customers", weighted)
        .await
        .expect("apply customer retract");
    recovered.run_tick(5).await.expect("right retract tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 5)
            .await;
    assert_eq!(
        join_rows(&snapshot),
        vec![(3, "west".to_string(), 70), (4, "west".to_string(), 80)]
    );
    let delta = recovered_handle
        .arrow_delta_for(5)
        .expect("post-retract delta");
    assert_eq!(
        weighted_join_rows(&delta),
        vec![(1, "west".to_string(), 50, -1)]
    );
}
#[tokio::test]
async fn ordered_join_uses_slate_backed_columnar_operator_incrementally() {
    let orders = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("customer_id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
        ],
    )
    .expect("orders source definition");
    let customers = SourceDefinition::new(
        "customers",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("region", SourceDataType::Utf8, false),
        ],
    )
    .expect("customers source definition");
    let orders_schema = orders.to_arrow_schema();
    let customers_schema = customers.to_arrow_schema();
    let initial_orders = RecordBatch::try_new(
        Arc::clone(&orders_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 11, 12])),
            Arc::new(Int64Array::from(vec![50, 60, 70])),
        ],
    )
    .expect("initial orders batch");
    let initial_customers = RecordBatch::try_new(
        Arc::clone(&customers_schema),
        vec![
            Arc::new(Int64Array::from(vec![10, 11])),
            Arc::new(StringArray::from(vec!["west", "east"])),
        ],
    )
    .expect("initial customers batch");

    let mut sources = SourceRegistry::new();
    sources.register(orders);
    sources.register(customers);
    let table = build_operator_state_table("vectorized-columnar-ordered-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let query = "SELECT o.id AS order_id, c.region, o.amount \
        FROM orders o JOIN customers c ON o.customer_id = c.id \
        WHERE c.region = 'west' \
        ORDER BY order_id";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_ordered_west_orders",
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
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&runtime, "incremental_inner");

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
            "customers",
            vec![initial_customers.clone()],
            vec![initial_customers],
        )
        .await
        .expect("append initial customers");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_ordered_west_orders")
        .expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(join_rows(&snapshot), vec![(1, "west".to_string(), 50)]);

    let customer_insert = RecordBatch::try_new(
        Arc::clone(&customers_schema),
        vec![
            Arc::new(Int64Array::from(vec![12])),
            Arc::new(StringArray::from(vec!["west"])),
        ],
    )
    .expect("customer insert batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "customers",
            vec![customer_insert.clone()],
            vec![customer_insert],
        )
        .await
        .expect("append customer insert");
    runtime.run_tick(2).await.expect("right delta tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(
        join_rows(&snapshot),
        vec![(1, "west".to_string(), 50), (3, "west".to_string(), 70)]
    );
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_join_rows(&delta),
        vec![(3, "west".to_string(), 70, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_ordered_west_orders",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&recovered, "incremental_inner");
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_ordered_west_orders")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(
        join_rows(&recovered_snapshot),
        vec![(1, "west".to_string(), 50), (3, "west".to_string(), 70)]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let order_insert = RecordBatch::try_new(
        Arc::clone(&orders_schema),
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(Int64Array::from(vec![12])),
            Arc::new(Int64Array::from(vec![80])),
        ],
    )
    .expect("order insert batch");
    recovered
        .append_source_batches_for_execution_and_query(
            "orders",
            vec![order_insert.clone()],
            vec![order_insert],
        )
        .await
        .expect("append order insert");
    recovered.run_tick(4).await.expect("left delta tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 4)
            .await;
    assert_eq!(
        join_rows(&snapshot),
        vec![
            (1, "west".to_string(), 50),
            (3, "west".to_string(), 70),
            (4, "west".to_string(), 80),
        ]
    );
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-insert delta");
    assert_eq!(
        weighted_join_rows(&delta),
        vec![(4, "west".to_string(), 80, 1)]
    );
}

#[tokio::test]
async fn multi_column_join_uses_slate_backed_columnar_operator_semantics() {
    let orders = SourceDefinition::new(
        "orders",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("customer_id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
        ],
    )
    .expect("orders source definition");
    let customers = SourceDefinition::new(
        "customers",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("amount", SourceDataType::Int64, false),
            SourceColumn::new_nullable("region", SourceDataType::Utf8, false),
        ],
    )
    .expect("customers source definition");
    let orders_schema = orders.to_arrow_schema();
    let customers_schema = customers.to_arrow_schema();
    let initial_orders = RecordBatch::try_new(
        Arc::clone(&orders_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 10, 11])),
            Arc::new(Int64Array::from(vec![50, 60, 70])),
        ],
    )
    .expect("initial orders batch");
    let initial_customers = RecordBatch::try_new(
        Arc::clone(&customers_schema),
        vec![
            Arc::new(Int64Array::from(vec![10, 10, 11])),
            Arc::new(Int64Array::from(vec![50, 60, 80])),
            Arc::new(StringArray::from(vec!["west", "east", "north"])),
        ],
    )
    .expect("initial customers batch");

    let mut sources = SourceRegistry::new();
    sources.register(orders);
    sources.register(customers);
    let table = build_operator_state_table("vectorized-columnar-multi-column-join").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let query = "SELECT o.id AS order_id, c.region, o.amount \
        FROM orders o JOIN customers c \
        ON o.customer_id = c.id AND o.amount = c.amount";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_customer_amount_orders",
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
        MaterializedViewExecutionMode::ColumnarJoin
    );
    assert_columnar_join_strategy(&runtime, "incremental_inner");

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
            "customers",
            vec![initial_customers.clone()],
            vec![initial_customers],
        )
        .await
        .expect("append initial customers");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry
        .get("mv_customer_amount_orders")
        .expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(
        join_rows(&snapshot),
        vec![(1, "west".to_string(), 50), (2, "east".to_string(), 60)]
    );

    let customer_insert = RecordBatch::try_new(
        Arc::clone(&customers_schema),
        vec![
            Arc::new(Int64Array::from(vec![11])),
            Arc::new(Int64Array::from(vec![70])),
            Arc::new(StringArray::from(vec!["south"])),
        ],
    )
    .expect("customer insert batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "customers",
            vec![customer_insert.clone()],
            vec![customer_insert],
        )
        .await
        .expect("append customer insert");
    runtime.run_tick(2).await.expect("insert tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(
        join_rows(&snapshot),
        vec![
            (1, "west".to_string(), 50),
            (2, "east".to_string(), 60),
            (3, "south".to_string(), 70),
        ]
    );
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_join_rows(&delta),
        vec![(3, "south".to_string(), 70, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_customer_amount_orders",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarJoin
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_customer_amount_orders")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(
        join_rows(&recovered_snapshot),
        vec![
            (1, "west".to_string(), 50),
            (2, "east".to_string(), 60),
            (3, "south".to_string(), 70),
        ]
    );
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));
}

#[tokio::test]
async fn join_topn_uses_slate_backed_columnar_operator_incrementally() {
    let auctions = SourceDefinition::new(
        "auction",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("itemName", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("description", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("initialBid", SourceDataType::Int64, false),
            SourceColumn::new_nullable("reserve", SourceDataType::Int64, false),
            SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("expires", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
            SourceColumn::new_nullable("category", SourceDataType::Int64, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("auction source definition")
    .with_property(SOURCE_PRIMARY_KEY_PROPERTY, "id");
    let bids = SourceDefinition::new(
        "bid",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("bid source definition");
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auction_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["item-1", "item-2"])),
            Arc::new(StringArray::from(vec!["description-1", "description-2"])),
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(TimestampMillisecondArray::from(vec![10, 10])),
            Arc::new(TimestampMillisecondArray::from(vec![100, 100])),
            Arc::new(Int64Array::from(vec![101, 102])),
            Arc::new(Int64Array::from(vec![7, 8])),
            Arc::new(StringArray::from(vec![
                "auction-extra-1",
                "auction-extra-2",
            ])),
        ],
    )
    .expect("initial auction batch");
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 11, 9, 12])),
            Arc::new(Int64Array::from(vec![100, 200, 200, 50])),
            Arc::new(TimestampMillisecondArray::from(vec![20, 15, 15, 25])),
            Arc::new(StringArray::from(vec![
                "bid-extra-10",
                "bid-extra-11",
                "bid-extra-09",
                "bid-extra-12",
            ])),
        ],
    )
    .expect("initial bid batch");

    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-join-topn").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("itemName", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("initialBid", DataType::Int64, false),
        Field::new("reserve", DataType::Int64, false),
        Field::new(
            "dateTime",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(
            "expires",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("seller", DataType::Int64, false),
        Field::new("category", DataType::Int64, false),
        Field::new("extra", DataType::Utf8, false),
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new(
            "bidTime",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("bidExtra", DataType::Utf8, false),
    ]));
    let query = "SELECT id, \"itemName\", description, \"initialBid\", reserve, \"dateTime\", \
        expires, seller, category, extra, auction, bidder, price, \"bidTime\", \"bidExtra\" \
        FROM (SELECT a.id, a.\"itemName\", a.description, a.\"initialBid\", a.reserve, \
        a.\"dateTime\", a.expires, a.seller, a.category, a.extra, b.auction, b.bidder, \
        b.price, b.\"dateTime\" AS \"bidTime\", b.extra AS \"bidExtra\", \
        ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY b.price DESC, b.\"dateTime\" ASC, \
        b.bidder ASC, b.extra ASC) AS rownum \
        FROM auction a JOIN bid b ON a.id = b.auction \
        WHERE b.\"dateTime\" BETWEEN a.\"dateTime\" AND a.expires) ranked \
        WHERE rownum <= 1";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_top_bid",
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
        MaterializedViewExecutionMode::ColumnarJoinTopN
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "auction",
            vec![initial_auctions.clone()],
            vec![initial_auctions],
        )
        .await
        .expect("append initial auctions");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![initial_bids.clone()],
            vec![initial_bids],
        )
        .await
        .expect("append initial bids");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_top_bid").expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(join_topn_rows(&snapshot), vec![(1, 9, 200), (2, 12, 50)]);
    assert_eq!(
        join_topn_rows_with_extra(&snapshot),
        vec![
            (1, 9, 200, "bid-extra-09".to_string()),
            (2, 12, 50, "bid-extra-12".to_string())
        ]
    );

    let better_bid = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![13])),
            Arc::new(Int64Array::from(vec![300])),
            Arc::new(TimestampMillisecondArray::from(vec![30])),
            Arc::new(StringArray::from(vec!["bid-extra-13"])),
        ],
    )
    .expect("better bid batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![better_bid.clone()],
            vec![better_bid],
        )
        .await
        .expect("append better bid");
    runtime.run_tick(2).await.expect("better bid tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 2).await;
    assert_eq!(join_topn_rows(&snapshot), vec![(1, 13, 300), (2, 12, 50)]);
    let delta = handle.arrow_delta_for(2).expect("better bid delta");
    assert_eq!(
        weighted_join_topn_rows(&delta),
        vec![(1, 9, 200, -1), (1, 13, 300, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_top_bid",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarJoinTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_top_bid")
        .expect("recovered materialized view");
    let recovered_snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 3)
            .await;
    assert_eq!(
        join_topn_rows(&recovered_snapshot),
        vec![(1, 13, 300), (2, 12, 50)]
    );

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&bid_schema).expect("weighted schema");
    let retract = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![13])),
            Arc::new(Int64Array::from(vec![300])),
            Arc::new(TimestampMillisecondArray::from(vec![30])),
            Arc::new(StringArray::from(vec!["bid-extra-13"])),
        ],
    )
    .expect("retract bid batch");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract bid");
    recovered
        .apply_weighted_source_delta("bid", weighted)
        .await
        .expect("apply weighted bid retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot =
        materialized_view_snapshot_for(recovered_handle.as_ref(), Arc::clone(&output_schema), 4)
            .await;
    assert_eq!(join_topn_rows(&snapshot), vec![(1, 9, 200), (2, 12, 50)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_join_topn_rows(&delta),
        vec![(1, 9, 200, 1), (1, 13, 300, -1)]
    );
    assert_eq!(
        join_topn_rows_with_extra(&snapshot),
        vec![
            (1, 9, 200, "bid-extra-09".to_string()),
            (2, 12, 50, "bid-extra-12".to_string())
        ]
    );
}

#[tokio::test]
async fn q6_shape_uses_grouped_stats_over_grouped_max_join_semantics() {
    let auctions = SourceDefinition::new(
        "auction",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("itemName", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("description", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("initialBid", SourceDataType::Int64, false),
            SourceColumn::new_nullable("reserve", SourceDataType::Int64, false),
            SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("expires", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
            SourceColumn::new_nullable("category", SourceDataType::Int64, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("auction source definition")
    .with_property(SOURCE_PRIMARY_KEY_PROPERTY, "id");
    let bids = SourceDefinition::new(
        "bid",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            SourceColumn::new_nullable("dateTime", SourceDataType::TimestampMillis, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("bid source definition");
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auction_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["item-1", "item-2", "item-3"])),
            Arc::new(StringArray::from(vec![
                "description-1",
                "description-2",
                "description-3",
            ])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
            Arc::new(TimestampMillisecondArray::from(vec![10, 10, 10])),
            Arc::new(TimestampMillisecondArray::from(vec![100, 100, 100])),
            Arc::new(Int64Array::from(vec![10, 10, 20])),
            Arc::new(Int64Array::from(vec![7, 7, 8])),
            Arc::new(StringArray::from(vec![
                "auction-extra-1",
                "auction-extra-2",
                "auction-extra-3",
            ])),
        ],
    )
    .expect("initial auction batch");
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
            Arc::new(Int64Array::from(vec![101, 102, 201, 301])),
            Arc::new(Int64Array::from(vec![100, 120, 110, 300])),
            Arc::new(TimestampMillisecondArray::from(vec![20, 25, 30, 40])),
            Arc::new(StringArray::from(vec![
                "bid-extra-101",
                "bid-extra-102",
                "bid-extra-201",
                "bid-extra-301",
            ])),
        ],
    )
    .expect("initial bid batch");

    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-q6-grouped-max-rewrite").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("seller", DataType::Int64, false),
        Field::new("moving_avg_price", DataType::Float64, true),
    ]));
    let query = "SELECT seller, AVG(price) AS moving_avg_price FROM (\
        SELECT a.seller, b.price, b.\"dateTime\", \
        ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC, \
        b.\"dateTime\" ASC, b.bidder ASC, b.extra ASC) AS rownum \
        FROM auction a JOIN bid b ON a.id = b.auction \
        WHERE b.\"dateTime\" BETWEEN a.\"dateTime\" AND a.expires) ranked \
        WHERE rownum <= 1 GROUP BY seller";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_q6",
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
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "auction",
            vec![initial_auctions.clone()],
            vec![initial_auctions],
        )
        .await
        .expect("append initial auctions");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![initial_bids.clone()],
            vec![initial_bids],
        )
        .await
        .expect("append initial bids");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_q6").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(category_avg_rows(&snapshot), vec![(10, 115.0), (20, 300.0)]);

    let better_bid = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![202])),
            Arc::new(Int64Array::from(vec![200])),
            Arc::new(TimestampMillisecondArray::from(vec![35])),
            Arc::new(StringArray::from(vec!["bid-extra-202"])),
        ],
    )
    .expect("better bid batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![better_bid.clone()],
            vec![better_bid],
        )
        .await
        .expect("append better bid");
    runtime.run_tick(2).await.expect("better bid tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(category_avg_rows(&snapshot), vec![(10, 160.0), (20, 300.0)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_category_avg_rows(&delta),
        vec![(10, 115.0, -1), (10, 160.0, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_q6",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_q6")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(
        category_avg_rows(&recovered_snapshot),
        vec![(10, 160.0), (20, 300.0)]
    );

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&bid_schema).expect("weighted schema");
    let retract = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![202])),
            Arc::new(Int64Array::from(vec![200])),
            Arc::new(TimestampMillisecondArray::from(vec![35])),
            Arc::new(StringArray::from(vec!["bid-extra-202"])),
        ],
    )
    .expect("retract bid batch");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract bid");
    recovered
        .apply_weighted_source_delta("bid", weighted)
        .await
        .expect("apply weighted bid retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(category_avg_rows(&snapshot), vec![(10, 115.0), (20, 300.0)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_category_avg_rows(&delta),
        vec![(10, 160.0, -1), (10, 115.0, 1)]
    );
}

#[tokio::test]
async fn cdc_q9_shape_uses_incremental_join_topn_semantics() {
    let auctions = SourceDefinition::new(
        "nexmark_auction",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("item_name", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("description", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("initial_bid", SourceDataType::Int64, false),
            SourceColumn::new_nullable("reserve", SourceDataType::Int64, false),
            SourceColumn::new_nullable("date_time", SourceDataType::Int64, false),
            SourceColumn::new_nullable("expires", SourceDataType::Int64, false),
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
            SourceColumn::new_nullable("category", SourceDataType::Int64, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("auction source definition");
    let bids = SourceDefinition::new(
        "nexmark_bid",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            SourceColumn::new_nullable("channel", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("url", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("date_time", SourceDataType::Int64, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("bid source definition");
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auction_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["item-1", "item-2"])),
            Arc::new(StringArray::from(vec!["description-1", "description-2"])),
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(Int64Array::from(vec![10, 10])),
            Arc::new(Int64Array::from(vec![100, 100])),
            Arc::new(Int64Array::from(vec![101, 102])),
            Arc::new(Int64Array::from(vec![7, 8])),
            Arc::new(StringArray::from(vec![
                "auction-extra-1",
                "auction-extra-2",
            ])),
        ],
    )
    .expect("initial auction batch");
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![100, 101, 102, 103])),
            Arc::new(Int64Array::from(vec![1, 1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 11, 9, 12])),
            Arc::new(Int64Array::from(vec![100, 200, 200, 50])),
            Arc::new(StringArray::from(vec!["web", "web", "web", "web"])),
            Arc::new(StringArray::from(vec!["/10", "/11", "/09", "/12"])),
            Arc::new(Int64Array::from(vec![20, 15, 15, 25])),
            Arc::new(StringArray::from(vec![
                "bid-extra-10",
                "bid-extra-11",
                "bid-extra-09",
                "bid-extra-12",
            ])),
        ],
    )
    .expect("initial bid batch");

    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-cdc-q9-shape").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("itemName", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("initialBid", DataType::Int64, false),
        Field::new("reserve", DataType::Int64, false),
        Field::new("dateTime", DataType::Int64, false),
        Field::new("expires", DataType::Int64, false),
        Field::new("seller", DataType::Int64, false),
        Field::new("category", DataType::Int64, false),
        Field::new("extra", DataType::Utf8, false),
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new("bidTime", DataType::Int64, false),
        Field::new("bidExtra", DataType::Utf8, false),
    ]));
    let query = "SELECT id, \"itemName\", description, \"initialBid\", reserve, \"dateTime\", \
        expires, seller, category, extra, auction, bidder, price, \"bidTime\", \"bidExtra\" \
        FROM (SELECT a.id, a.item_name AS \"itemName\", a.description, \
        a.initial_bid AS \"initialBid\", a.reserve, a.auction_time AS \"dateTime\", \
        a.expires, a.seller, a.category, a.auction_extra AS extra, b.auction, b.bidder, \
        b.price, b.bid_time AS \"bidTime\", b.bid_extra AS \"bidExtra\", \
        ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY b.price DESC, b.bid_time ASC, \
        b.bidder ASC, b.bid_extra ASC) AS rownum \
        FROM (SELECT id, item_name, description, initial_bid, reserve, \
        date_time AS auction_time, expires, seller, category, extra AS auction_extra \
        FROM nexmark_auction) a JOIN (SELECT auction, bidder, price, date_time AS bid_time, \
        extra AS bid_extra FROM nexmark_bid) b ON a.id = b.auction \
        WHERE b.bid_time BETWEEN a.auction_time AND a.expires) ranked \
        WHERE rownum <= 1";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_cdc_q9",
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
        MaterializedViewExecutionMode::ColumnarJoinTopN
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_auction",
            vec![initial_auctions.clone()],
            vec![initial_auctions],
        )
        .await
        .expect("append initial auctions");
    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_bid",
            vec![initial_bids.clone()],
            vec![initial_bids],
        )
        .await
        .expect("append initial bids");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_cdc_q9").expect("materialized view");
    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 1).await;
    assert_eq!(join_topn_rows(&snapshot), vec![(1, 9, 200), (2, 12, 50)]);
    assert_eq!(
        join_topn_rows_with_extra(&snapshot),
        vec![
            (1, 9, 200, "bid-extra-09".to_string()),
            (2, 12, 50, "bid-extra-12".to_string())
        ]
    );

    let better_bid = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![104])),
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![13])),
            Arc::new(Int64Array::from(vec![300])),
            Arc::new(StringArray::from(vec!["web"])),
            Arc::new(StringArray::from(vec!["/13"])),
            Arc::new(Int64Array::from(vec![30])),
            Arc::new(StringArray::from(vec!["bid-extra-13"])),
        ],
    )
    .expect("better bid batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_bid",
            vec![better_bid.clone()],
            vec![better_bid.clone()],
        )
        .await
        .expect("append better bid");
    runtime.run_tick(2).await.expect("better bid tick");

    let delta = handle.arrow_delta_for(2).expect("better bid delta");
    assert_eq!(
        weighted_join_topn_rows(&delta),
        vec![(1, 9, 200, -1), (1, 13, 300, 1)]
    );

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&bid_schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&better_bid, &weighted_schema, &[-1])
        .expect("weighted retract better bid");
    runtime
        .apply_weighted_source_delta("nexmark_bid", weighted)
        .await
        .expect("apply weighted bid retract");
    runtime.run_tick(3).await.expect("retract tick");

    let snapshot =
        materialized_view_snapshot_for(handle.as_ref(), Arc::clone(&output_schema), 3).await;
    assert_eq!(join_topn_rows(&snapshot), vec![(1, 9, 200), (2, 12, 50)]);
    let delta = handle.arrow_delta_for(3).expect("post-retract delta");
    assert_eq!(
        weighted_join_topn_rows(&delta),
        vec![(1, 9, 200, 1), (1, 13, 300, -1)]
    );
}

#[tokio::test]
async fn cdc_q6_shape_uses_incremental_top_bid_grouped_avg_semantics() {
    let auctions = SourceDefinition::new(
        "nexmark_auction",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("item_name", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("description", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("initial_bid", SourceDataType::Int64, false),
            SourceColumn::new_nullable("reserve", SourceDataType::Int64, false),
            SourceColumn::new_nullable("date_time", SourceDataType::Int64, false),
            SourceColumn::new_nullable("expires", SourceDataType::Int64, false),
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
            SourceColumn::new_nullable("category", SourceDataType::Int64, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("auction source definition");
    let bids = SourceDefinition::new(
        "nexmark_bid",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            SourceColumn::new_nullable("channel", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("url", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("date_time", SourceDataType::Int64, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("bid source definition");
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auction_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["item-1", "item-2", "item-3"])),
            Arc::new(StringArray::from(vec![
                "description-1",
                "description-2",
                "description-3",
            ])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
            Arc::new(Int64Array::from(vec![10, 10, 10])),
            Arc::new(Int64Array::from(vec![100, 100, 100])),
            Arc::new(Int64Array::from(vec![10, 10, 20])),
            Arc::new(Int64Array::from(vec![7, 7, 8])),
            Arc::new(StringArray::from(vec![
                "auction-extra-1",
                "auction-extra-2",
                "auction-extra-3",
            ])),
        ],
    )
    .expect("initial auction batch");
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![101, 102, 201, 301])),
            Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
            Arc::new(Int64Array::from(vec![101, 102, 201, 301])),
            Arc::new(Int64Array::from(vec![100, 120, 110, 300])),
            Arc::new(StringArray::from(vec!["web", "web", "web", "web"])),
            Arc::new(StringArray::from(vec!["/101", "/102", "/201", "/301"])),
            Arc::new(Int64Array::from(vec![20, 25, 30, 40])),
            Arc::new(StringArray::from(vec![
                "bid-extra-101",
                "bid-extra-102",
                "bid-extra-201",
                "bid-extra-301",
            ])),
        ],
    )
    .expect("initial bid batch");

    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-cdc-q6-shape").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("seller", DataType::Int64, false),
        Field::new("moving_avg_price", DataType::Int64, true),
    ]));
    let query = "SELECT seller, CAST(AVG(price) AS BIGINT) AS moving_avg_price \
        FROM (SELECT a.seller, b.price, b.date_time, \
        ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC, \
        b.date_time ASC, b.bidder ASC, b.channel ASC, b.url ASC, b.extra ASC) AS rownum \
        FROM nexmark_auction a JOIN nexmark_bid b ON a.id = b.auction \
        WHERE b.date_time BETWEEN a.date_time AND a.expires) ranked \
        WHERE rownum <= 1 GROUP BY seller";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_cdc_q6",
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
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_auction",
            vec![initial_auctions.clone()],
            vec![initial_auctions],
        )
        .await
        .expect("append initial auctions");
    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_bid",
            vec![initial_bids.clone()],
            vec![initial_bids],
        )
        .await
        .expect("append initial bids");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_cdc_q6").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(10, 115), (20, 300)]);

    let better_bid = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![202])),
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![202])),
            Arc::new(Int64Array::from(vec![200])),
            Arc::new(StringArray::from(vec!["web"])),
            Arc::new(StringArray::from(vec!["/202"])),
            Arc::new(Int64Array::from(vec![35])),
            Arc::new(StringArray::from(vec!["bid-extra-202"])),
        ],
    )
    .expect("better bid batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_bid",
            vec![better_bid.clone()],
            vec![better_bid.clone()],
        )
        .await
        .expect("append better bid");
    runtime.run_tick(2).await.expect("better bid tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(10, 160), (20, 300)]);

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&bid_schema).expect("weighted schema");
    let weighted = weighted_batch_from_diffs(&better_bid, &weighted_schema, &[-1])
        .expect("weighted retract better bid");
    runtime
        .apply_weighted_source_delta("nexmark_bid", weighted)
        .await
        .expect("apply weighted bid retract");
    runtime.run_tick(3).await.expect("retract tick");

    let snapshot = handle.arrow_snapshot_for(3).expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(10, 115), (20, 300)]);

    let deleted_auction = RecordBatch::try_new(
        Arc::clone(&auction_schema),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(StringArray::from(vec!["item-3"])),
            Arc::new(StringArray::from(vec!["description-3"])),
            Arc::new(Int64Array::from(vec![30])),
            Arc::new(Int64Array::from(vec![300])),
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(Int64Array::from(vec![100])),
            Arc::new(Int64Array::from(vec![20])),
            Arc::new(Int64Array::from(vec![8])),
            Arc::new(StringArray::from(vec!["auction-extra-3"])),
        ],
    )
    .expect("deleted auction batch");
    let weighted_auction_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&auction_schema)
            .expect("weighted auction schema");
    let weighted_auction =
        weighted_batch_from_diffs(&deleted_auction, &weighted_auction_schema, &[-1])
            .expect("weighted auction delete");
    runtime
        .apply_weighted_source_delta("nexmark_auction", weighted_auction)
        .await
        .expect("apply weighted auction delete");
    runtime.run_tick(4).await.expect("auction delete tick");

    let snapshot = handle
        .arrow_snapshot_for(4)
        .expect("post-auction-delete snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(10, 115)]);
}

#[tokio::test]
async fn cdc_q6_generated_mutations_match_query_provider_semantics() {
    const BASE_TS: i64 = 1_700_000_000_000;
    const BID_INITIAL_ROWS: i64 = 10_000;
    const AUCTION_INITIAL_ROWS: i64 = 1_112;
    const PERSON_KEYSPACE: i64 = 1_112;
    const BID_UPDATES: i64 = 4_444;
    const BID_DELETES: i64 = 2_222;
    const BID_INSERTS: i64 = 2_222;
    const AUCTION_UPDATES: i64 = 556;
    const AUCTION_DELETES: i64 = 278;
    const AUCTION_INSERTS: i64 = 278;
    const LIVE_AUCTION_KEYSPACE: i64 = AUCTION_INITIAL_ROWS + AUCTION_INSERTS;

    fn auction_batch(
        schema: &SchemaRef,
        ids: impl IntoIterator<Item = (i64, bool)>,
    ) -> RecordBatch {
        let mut id_values = Vec::new();
        let mut item_names = Vec::new();
        let mut descriptions = Vec::new();
        let mut initial_bids = Vec::new();
        let mut reserves = Vec::new();
        let mut date_times = Vec::new();
        let mut expires = Vec::new();
        let mut sellers = Vec::new();
        let mut categories = Vec::new();
        let mut extras = Vec::new();
        for (id, updated) in ids {
            id_values.push(id);
            item_names.push(format!("item_{id}"));
            descriptions.push(format!("auction description {id}"));
            initial_bids.push(100 + (id % 10_000));
            reserves.push(1000 + (id % 100_000) + if updated { 31 } else { 0 });
            date_times.push(BASE_TS + id);
            expires.push(BASE_TS + id + 86_400_000 + if updated { 1000 } else { 0 });
            sellers.push(((id - 1) % PERSON_KEYSPACE) + 1);
            let category = ((id - 1) % 20) + 1;
            categories.push(if updated {
                if category == 20 { 1 } else { category + 1 }
            } else {
                category
            });
            extras.push(if updated {
                format!("auction_extra_{id}_updated")
            } else {
                format!("auction_extra_{id}")
            });
        }
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(id_values)),
                Arc::new(StringArray::from(item_names)),
                Arc::new(StringArray::from(descriptions)),
                Arc::new(Int64Array::from(initial_bids)),
                Arc::new(Int64Array::from(reserves)),
                Arc::new(Int64Array::from(date_times)),
                Arc::new(Int64Array::from(expires)),
                Arc::new(Int64Array::from(sellers)),
                Arc::new(Int64Array::from(categories)),
                Arc::new(StringArray::from(extras)),
            ],
        )
        .expect("auction batch")
    }

    fn bid_batch(
        schema: &SchemaRef,
        ids: impl IntoIterator<Item = (i64, bool)>,
        auction_keyspace: i64,
    ) -> RecordBatch {
        let mut id_values = Vec::new();
        let mut auctions = Vec::new();
        let mut bidders = Vec::new();
        let mut prices = Vec::new();
        let mut channels = Vec::new();
        let mut urls = Vec::new();
        let mut date_times = Vec::new();
        let mut extras = Vec::new();
        for (id, updated) in ids {
            id_values.push(id);
            let auction = ((id - 1) % auction_keyspace) + 1;
            auctions.push(auction);
            bidders.push(((id - 1) % PERSON_KEYSPACE) + 1);
            prices.push(1000 + ((id * 17) % 2_000_000) + if updated { 17 } else { 0 });
            let channel = if updated {
                match id % 4 {
                    0 => "apple",
                    1 => "google",
                    2 => "facebook",
                    _ => "baidu",
                }
            } else {
                match id % 5 {
                    0 => "apple",
                    1 => "google",
                    2 => "facebook",
                    3 => "baidu",
                    _ => "web",
                }
            };
            channels.push(channel.to_string());
            urls.push(if updated {
                format!(
                    "https://cdc.example.com/watch/channel_id={}/u/{id}",
                    (id + 7) % 100
                )
            } else {
                format!(
                    "https://nexmark.example.com/auction/{auction}/bid/{id}?channel_id={}",
                    id % 100
                )
            });
            date_times.push(BASE_TS + id + if updated { 1000 } else { 0 });
            extras.push(if updated {
                format!("bid_extra_ccc_{id}_updated")
            } else {
                format!("bid_extra_ccc_{id}")
            });
        }
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(id_values)),
                Arc::new(Int64Array::from(auctions)),
                Arc::new(Int64Array::from(bidders)),
                Arc::new(Int64Array::from(prices)),
                Arc::new(StringArray::from(channels)),
                Arc::new(StringArray::from(urls)),
                Arc::new(Int64Array::from(date_times)),
                Arc::new(StringArray::from(extras)),
            ],
        )
        .expect("bid batch")
    }

    async fn apply_weighted(
        runtime: &mut VectorizedExecutionRuntime,
        source_name: &str,
        schema: &SchemaRef,
        batch: RecordBatch,
        diffs: &[i64],
    ) {
        let weighted_schema =
            crate::delta_consolidation::weighted_snapshot_schema(schema).expect("weighted schema");
        let weighted =
            weighted_batch_from_diffs(&batch, &weighted_schema, diffs).expect("weighted delta");
        runtime
            .apply_weighted_source_delta(source_name, weighted)
            .await
            .expect("apply weighted delta");
    }

    let auctions = SourceDefinition::new(
        "nexmark_auction",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("item_name", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("description", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("initial_bid", SourceDataType::Int64, false),
            SourceColumn::new_nullable("reserve", SourceDataType::Int64, false),
            SourceColumn::new_nullable("date_time", SourceDataType::Int64, false),
            SourceColumn::new_nullable("expires", SourceDataType::Int64, false),
            SourceColumn::new_nullable("seller", SourceDataType::Int64, false),
            SourceColumn::new_nullable("category", SourceDataType::Int64, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("auction source definition")
    .with_property(SOURCE_PRIMARY_KEY_PROPERTY, "id");
    let bids = SourceDefinition::new(
        "nexmark_bid",
        vec![
            SourceColumn::new_nullable("id", SourceDataType::Int64, false),
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("bidder", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
            SourceColumn::new_nullable("channel", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("url", SourceDataType::Utf8, false),
            SourceColumn::new_nullable("date_time", SourceDataType::Int64, false),
            SourceColumn::new_nullable("extra", SourceDataType::Utf8, false),
        ],
    )
    .expect("bid source definition")
    .with_property(SOURCE_PRIMARY_KEY_PROPERTY, "id");
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);

    let table = build_operator_state_table("vectorized-columnar-cdc-q6-generated-mutations").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("seller", DataType::Int64, false),
        Field::new("moving_avg_price", DataType::Int64, true),
    ]));
    let query = "SELECT seller, CAST(AVG(price) AS BIGINT) AS moving_avg_price \
        FROM (SELECT a.seller, b.price, b.date_time, \
        ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC, \
        b.date_time ASC, b.bidder ASC, b.channel ASC, b.url ASC, b.extra ASC) AS rownum \
        FROM nexmark_auction a JOIN nexmark_bid b ON a.id = b.auction \
        WHERE b.date_time BETWEEN a.date_time AND a.expires) ranked \
        WHERE rownum <= 1 GROUP BY seller";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_cdc_q6_generated",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&registry),
        VectorizedExecutionRuntimeOptions::default()
            .with_operator_state_table(Arc::clone(&table))
            .with_source_query_tables(),
    )
    .await
    .expect("runtime");
    assert_eq!(
        runtime.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    let initial_auctions = auction_batch(
        &auction_schema,
        (1..=AUCTION_INITIAL_ROWS).map(|id| (id, false)),
    );
    let initial_bids = bid_batch(
        &bid_schema,
        (1..=BID_INITIAL_ROWS).map(|id| (id, false)),
        AUCTION_INITIAL_ROWS,
    );
    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_auction",
            vec![initial_auctions.clone()],
            vec![initial_auctions],
        )
        .await
        .expect("append initial auctions");
    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_bid",
            vec![initial_bids.clone()],
            vec![initial_bids],
        )
        .await
        .expect("append initial bids");
    runtime.run_tick(1).await.expect("initial tick");

    let bid_update_batch = bid_batch(
        &bid_schema,
        (1..=BID_UPDATES)
            .map(|id| (id, false))
            .chain((1..=BID_UPDATES).map(|id| (id, true))),
        AUCTION_INITIAL_ROWS,
    );
    let bid_update_diffs = std::iter::repeat_n(-1, BID_UPDATES as usize)
        .chain(std::iter::repeat_n(1, BID_UPDATES as usize))
        .collect::<Vec<_>>();
    apply_weighted(
        &mut runtime,
        "nexmark_bid",
        &bid_schema,
        bid_update_batch,
        &bid_update_diffs,
    )
    .await;
    runtime.run_tick(2).await.expect("bid update tick");

    let bid_delete_start = BID_UPDATES + 1;
    let bid_delete_batch = bid_batch(
        &bid_schema,
        (bid_delete_start..bid_delete_start + BID_DELETES).map(|id| (id, false)),
        AUCTION_INITIAL_ROWS,
    );
    let bid_delete_diffs = vec![-1; BID_DELETES as usize];
    apply_weighted(
        &mut runtime,
        "nexmark_bid",
        &bid_schema,
        bid_delete_batch,
        &bid_delete_diffs,
    )
    .await;
    runtime.run_tick(3).await.expect("bid delete tick");

    let bid_insert_batch = bid_batch(
        &bid_schema,
        (BID_INITIAL_ROWS + 1..=BID_INITIAL_ROWS + BID_INSERTS).map(|id| (id, false)),
        LIVE_AUCTION_KEYSPACE,
    );
    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_bid",
            vec![bid_insert_batch.clone()],
            vec![bid_insert_batch],
        )
        .await
        .expect("append bid inserts");
    runtime.run_tick(4).await.expect("bid insert tick");

    let auction_update_batch = auction_batch(
        &auction_schema,
        (1..=AUCTION_UPDATES)
            .map(|id| (id, false))
            .chain((1..=AUCTION_UPDATES).map(|id| (id, true))),
    );
    let auction_update_diffs = std::iter::repeat_n(-1, AUCTION_UPDATES as usize)
        .chain(std::iter::repeat_n(1, AUCTION_UPDATES as usize))
        .collect::<Vec<_>>();
    apply_weighted(
        &mut runtime,
        "nexmark_auction",
        &auction_schema,
        auction_update_batch,
        &auction_update_diffs,
    )
    .await;
    runtime.run_tick(5).await.expect("auction update tick");

    let auction_delete_start = AUCTION_UPDATES + 1;
    let auction_delete_batch = auction_batch(
        &auction_schema,
        (auction_delete_start..auction_delete_start + AUCTION_DELETES).map(|id| (id, false)),
    );
    let auction_delete_diffs = vec![-1; AUCTION_DELETES as usize];
    apply_weighted(
        &mut runtime,
        "nexmark_auction",
        &auction_schema,
        auction_delete_batch,
        &auction_delete_diffs,
    )
    .await;
    runtime.run_tick(6).await.expect("auction delete tick");

    let auction_insert_batch = auction_batch(
        &auction_schema,
        (AUCTION_INITIAL_ROWS + 1..=AUCTION_INITIAL_ROWS + AUCTION_INSERTS).map(|id| (id, false)),
    );
    runtime
        .append_source_batches_for_execution_and_query(
            "nexmark_auction",
            vec![auction_insert_batch.clone()],
            vec![auction_insert_batch],
        )
        .await
        .expect("append auction inserts");
    runtime.run_tick(7).await.expect("auction insert tick");

    let handle = registry
        .get("mv_cdc_q6_generated")
        .expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(7).expect("mv snapshot");
    let actual = id_count_rows(&snapshot);

    let ctx = SessionContext::new();
    for (name, provider) in runtime.table_providers() {
        ctx.register_table(&name, provider)
            .expect("register source table");
    }
    let expected = ctx
        .sql(query)
        .await
        .expect("plan expected q6")
        .collect()
        .await
        .expect("collect expected q6");
    assert_eq!(actual, id_count_rows(&expected));
}

#[tokio::test]
async fn topn_over_join_avg_uses_grouped_stats_input_semantics() {
    let auctions = SourceDefinition::new(
        "auction",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("auction source definition");
    let bids = SourceDefinition::new(
        "bid",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("bid source definition");
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auction_schema),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .expect("initial auction batch");
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![100, 200, 50])),
        ],
    )
    .expect("initial bid batch");

    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table = build_operator_state_table("vectorized-columnar-topn-over-join-avg").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let query = "SELECT key, value FROM (\
        SELECT auction AS key, CAST(avg_price AS BIGINT) AS value \
        FROM (SELECT b.auction, AVG(b.price) AS avg_price \
            FROM bid b JOIN auction a ON b.auction = a.id GROUP BY b.auction) j \
        ORDER BY avg_price DESC LIMIT 2) s";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_top_avg_bid",
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
        MaterializedViewExecutionMode::ColumnarTopN
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "auction",
            vec![initial_auctions.clone()],
            vec![initial_auctions],
        )
        .await
        .expect("append initial auctions");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![initial_bids.clone()],
            vec![initial_bids],
        )
        .await
        .expect("append initial bids");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_top_avg_bid").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 150), (2, 50)]);

    let better_bid = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![550])),
        ],
    )
    .expect("better bid batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![better_bid.clone()],
            vec![better_bid],
        )
        .await
        .expect("append better bid");
    runtime.run_tick(2).await.expect("better bid tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 150), (2, 300)]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(2, 50, -1), (2, 300, 1)]
    );

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_top_avg_bid",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarTopN
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_top_avg_bid")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(id_count_rows(&recovered_snapshot), vec![(1, 150), (2, 300)]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&bid_schema).expect("weighted schema");
    let retract = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![550])),
        ],
    )
    .expect("retract bid batch");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract bid");
    recovered
        .apply_weighted_source_delta("bid", weighted)
        .await
        .expect("apply weighted bid retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(id_count_rows(&snapshot), vec![(1, 150), (2, 50)]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(
        weighted_id_count_rows(&delta),
        vec![(2, 50, 1), (2, 300, -1)]
    );
}

#[tokio::test]
async fn global_aggregate_over_join_avg_topn_uses_grouped_stats_topn_input_semantics() {
    let auctions = SourceDefinition::new(
        "auction",
        vec![SourceColumn::new_nullable(
            "id",
            SourceDataType::Int64,
            false,
        )],
    )
    .expect("auction source definition");
    let bids = SourceDefinition::new(
        "bid",
        vec![
            SourceColumn::new_nullable("auction", SourceDataType::Int64, false),
            SourceColumn::new_nullable("price", SourceDataType::Int64, false),
        ],
    )
    .expect("bid source definition");
    let auction_schema = auctions.to_arrow_schema();
    let bid_schema = bids.to_arrow_schema();
    let initial_auctions = RecordBatch::try_new(
        Arc::clone(&auction_schema),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .expect("initial auction batch");
    let initial_bids = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![100, 200, 50])),
        ],
    )
    .expect("initial bid batch");

    let mut sources = SourceRegistry::new();
    sources.register(auctions);
    sources.register(bids);
    let table =
        build_operator_state_table("vectorized-columnar-aggregate-over-join-avg-topn").await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "total",
        DataType::Int64,
        true,
    )]));
    let query = "SELECT SUM(value) AS total FROM (\
        SELECT auction AS key, CAST(avg_price AS BIGINT) AS value \
        FROM (SELECT b.auction, AVG(b.price) AS avg_price \
            FROM bid b JOIN auction a ON b.auction = a.id GROUP BY b.auction) j \
        ORDER BY avg_price DESC LIMIT 2) s";
    let mut runtime = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_top_avg_total",
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
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );

    runtime
        .append_source_batches_for_execution_and_query(
            "auction",
            vec![initial_auctions.clone()],
            vec![initial_auctions],
        )
        .await
        .expect("append initial auctions");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![initial_bids.clone()],
            vec![initial_bids],
        )
        .await
        .expect("append initial bids");
    runtime.run_tick(1).await.expect("initial tick");

    let handle = registry.get("mv_top_avg_total").expect("materialized view");
    let snapshot = handle.arrow_snapshot_for(1).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![200]);

    let better_bid = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![550])),
        ],
    )
    .expect("better bid batch");
    runtime
        .append_source_batches_for_execution_and_query(
            "bid",
            vec![better_bid.clone()],
            vec![better_bid],
        )
        .await
        .expect("append better bid");
    runtime.run_tick(2).await.expect("better bid tick");

    let snapshot = handle.arrow_snapshot_for(2).expect("mv snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![450]);
    let delta = handle.arrow_delta_for(2).expect("mv delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(200, -1), (450, 1)]);

    let recovery_registry = Arc::new(MaterializedViewRegistry::new());
    let mut recovered = VectorizedExecutionRuntime::new_with_options(
        &sources,
        vec![SqlMaterializedViewPlan::from_sql(
            "mv_top_avg_total",
            query,
            Arc::clone(&output_schema),
        )],
        Arc::clone(&recovery_registry),
        VectorizedExecutionRuntimeOptions::default().with_operator_state_table(Arc::clone(&table)),
    )
    .await
    .expect("recovered runtime");
    assert_eq!(
        recovered.materialized_views[0].operator.mode(),
        MaterializedViewExecutionMode::ColumnarGroupedStats
    );
    recovered.run_tick(3).await.expect("recovered tick");

    let recovered_handle = recovery_registry
        .get("mv_top_avg_total")
        .expect("recovered materialized view");
    let recovered_snapshot = recovered_handle
        .arrow_snapshot_for(3)
        .expect("recovered snapshot");
    assert_eq!(single_int_rows(&recovered_snapshot), vec![450]);
    let recovered_delta = recovered_handle
        .arrow_delta_for(3)
        .expect("recovered empty delta");
    assert!(recovered_delta.iter().all(|batch| batch.num_rows() == 0));

    let weighted_schema =
        crate::delta_consolidation::weighted_snapshot_schema(&bid_schema).expect("weighted schema");
    let retract = RecordBatch::try_new(
        Arc::clone(&bid_schema),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![550])),
        ],
    )
    .expect("retract bid batch");
    let weighted =
        weighted_batch_from_diffs(&retract, &weighted_schema, &[-1]).expect("weighted retract bid");
    recovered
        .apply_weighted_source_delta("bid", weighted)
        .await
        .expect("apply weighted bid retract");
    recovered.run_tick(4).await.expect("retract tick");

    let snapshot = recovered_handle
        .arrow_snapshot_for(4)
        .expect("post-retract snapshot");
    assert_eq!(single_int_rows(&snapshot), vec![200]);
    let delta = recovered_handle
        .arrow_delta_for(4)
        .expect("post-retract delta");
    assert_eq!(weighted_single_int_rows(&delta), vec![(200, 1), (450, -1)]);
}
