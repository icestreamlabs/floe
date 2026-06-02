use super::*;

pub(super) fn arrow_schema(fields: Vec<Field>) -> Arc<Schema> {
    Arc::new(Schema::new(fields))
}

pub(super) fn udf_batch_len(args: &[ColumnarValue]) -> usize {
    args.iter()
        .find_map(|arg| match arg {
            ColumnarValue::Array(array) => Some(array.len()),
            ColumnarValue::Scalar(_) => None,
        })
        .unwrap_or(1)
}

pub(super) fn split_index_value(text: &str, delimiter: &str, index: i64) -> Option<String> {
    if index < 0 || delimiter.is_empty() {
        return None;
    }
    text.split(delimiter)
        .nth(index as usize)
        .map(str::to_string)
}

pub(super) async fn sql_plan(sql: &str) -> datafusion::logical_expr::LogicalPlan {
    let ctx = SessionContext::new();
    let bid_provider: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(nexmark_bid_schema()));
    let auction_provider: Arc<dyn TableProvider> =
        Arc::new(EmptyTable::new(nexmark_auction_schema()));
    let person_provider: Arc<dyn TableProvider> =
        Arc::new(EmptyTable::new(nexmark_person_schema()));
    ctx.register_table("nexmark_bid", Arc::clone(&bid_provider))
        .expect("register nexmark_bid");
    ctx.register_table("bid", bid_provider)
        .expect("register bid");
    ctx.register_table("nexmark_auction", Arc::clone(&auction_provider))
        .expect("register nexmark_auction");
    ctx.register_table("auction", auction_provider)
        .expect("register auction");
    ctx.register_table("nexmark_person", Arc::clone(&person_provider))
        .expect("register nexmark_person");
    ctx.register_table("person", person_provider)
        .expect("register person");
    register_planner_test_udfs(&ctx);
    let state = ctx.state();
    let plan = dbsp::circuit::create_logical_plan_with_asof_preplanner(&state, sql)
        .await
        .expect("build logical plan");
    let optimized = state.optimize(&plan).expect("optimize logical plan");
    if logical_plan_uses_only_dbsp_supported_types(&optimized) {
        optimized
    } else {
        plan
    }
}

pub(super) fn logical_plan_uses_only_dbsp_supported_types(
    plan: &datafusion::logical_expr::LogicalPlan,
) -> bool {
    logical_plan_node_supported(plan)
        && plan
            .inputs()
            .into_iter()
            .all(logical_plan_uses_only_dbsp_supported_types)
}

pub(super) fn logical_plan_node_supported(plan: &datafusion::logical_expr::LogicalPlan) -> bool {
    plan.schema()
        .fields()
        .iter()
        .all(|field| dbsp_supported_arrow_type(field.data_type()))
}

pub(super) fn dbsp_supported_arrow_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int64
            | DataType::Utf8
            | DataType::Boolean
            | DataType::Timestamp(TimeUnit::Millisecond, None)
    )
}

pub(super) fn register_planner_test_udfs(ctx: &SessionContext) {
    let passthrough_int64: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = udf_batch_len(args);
            let array: ArrayRef = Arc::new(Int64Array::from(vec![None::<i64>; len]));
            Ok(ColumnarValue::Array(array))
        },
    );
    let date_format_udf: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = udf_batch_len(args);
            let ts = args
                .first()
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(TimestampMillisecondArray::from(vec![
                        None::<i64>;
                        len
                    ])))
                })
                .into_array(len)?;
            let fmt = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(StringArray::from(vec![None::<&str>; len])))
                })
                .into_array(len)?;
            let (Some(ts), Some(fmt)) = (
                ts.as_any().downcast_ref::<TimestampMillisecondArray>(),
                fmt.as_any().downcast_ref::<StringArray>(),
            ) else {
                let array: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>; len]));
                return Ok(ColumnarValue::Array(array));
            };

            let values = (0..len)
                .map(|row_idx| {
                    if ts.is_null(row_idx) || fmt.is_null(row_idx) {
                        return None;
                    }
                    let dt = chrono::DateTime::<Utc>::from_timestamp_millis(ts.value(row_idx))?;
                    let pattern = fmt
                        .value(row_idx)
                        .replace("yyyy", "%Y")
                        .replace("MM", "%m")
                        .replace("dd", "%d")
                        .replace("HH", "%H")
                        .replace("mm", "%M")
                        .replace("ss", "%S");
                    Some(dt.format(&pattern).to_string())
                })
                .collect::<Vec<_>>();
            Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
        },
    );
    let regexp_extract_udf: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = udf_batch_len(args);
            let text = args
                .first()
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(StringArray::from(vec![None::<&str>; len])))
                })
                .into_array(len)?;
            let pattern = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(StringArray::from(vec![None::<&str>; len])))
                })
                .into_array(len)?;
            let group = args
                .get(2)
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(Int64Array::from(vec![None::<i64>; len])))
                })
                .into_array(len)?;
            let (Some(text), Some(pattern), Some(group)) = (
                text.as_any().downcast_ref::<StringArray>(),
                pattern.as_any().downcast_ref::<StringArray>(),
                group.as_any().downcast_ref::<Int64Array>(),
            ) else {
                let array: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>; len]));
                return Ok(ColumnarValue::Array(array));
            };

            let mut cache: HashMap<String, Option<Regex>> = HashMap::new();
            let values = (0..len)
                .map(|row_idx| {
                    if text.is_null(row_idx) || pattern.is_null(row_idx) || group.is_null(row_idx) {
                        return None;
                    }
                    let group_idx = group.value(row_idx);
                    if group_idx < 0 {
                        return None;
                    }
                    let pattern_text = pattern.value(row_idx);
                    let regex = cache
                        .entry(pattern_text.to_string())
                        .or_insert_with(|| Regex::new(pattern_text).ok());
                    let regex = regex.as_ref()?;
                    let captures = regex.captures(text.value(row_idx))?;
                    let matched = captures.get(group_idx as usize)?;
                    Some(matched.as_str().to_string())
                })
                .collect::<Vec<_>>();
            Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
        },
    );
    let split_index_udf: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = udf_batch_len(args);
            let text = args
                .first()
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(StringArray::from(vec![None::<&str>; len])))
                })
                .into_array(len)?;
            let delimiter = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(StringArray::from(vec![None::<&str>; len])))
                })
                .into_array(len)?;
            let index = args
                .get(2)
                .cloned()
                .unwrap_or_else(|| {
                    ColumnarValue::Array(Arc::new(Int64Array::from(vec![None::<i64>; len])))
                })
                .into_array(len)?;
            let (Some(text), Some(delimiter), Some(index)) = (
                text.as_any().downcast_ref::<StringArray>(),
                delimiter.as_any().downcast_ref::<StringArray>(),
                index.as_any().downcast_ref::<Int64Array>(),
            ) else {
                let array: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>; len]));
                return Ok(ColumnarValue::Array(array));
            };

            let values = (0..len)
                .map(|row_idx| {
                    if text.is_null(row_idx) || delimiter.is_null(row_idx) || index.is_null(row_idx)
                    {
                        return None;
                    }
                    split_index_value(
                        text.value(row_idx),
                        delimiter.value(row_idx),
                        index.value(row_idx),
                    )
                })
                .collect::<Vec<_>>();
            Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
        },
    );
    let proctime: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = udf_batch_len(args);
            let array: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![None::<i64>; len]));
            Ok(ColumnarValue::Array(array))
        },
    );
    let ts = DataType::Timestamp(TimeUnit::Millisecond, None);
    ctx.register_udf(create_udf(
        "proctime",
        vec![],
        ts,
        Volatility::Volatile,
        proctime,
    ));
    ctx.register_udf(create_udf(
        "hour",
        vec![DataType::Timestamp(TimeUnit::Millisecond, None)],
        DataType::Int64,
        Volatility::Immutable,
        Arc::clone(&passthrough_int64),
    ));
    ctx.register_udf(create_udf(
        "date_format",
        vec![
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Utf8,
        ],
        DataType::Utf8,
        Volatility::Immutable,
        date_format_udf,
    ));
    ctx.register_udf(create_udf(
        "regexp_extract",
        vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
        DataType::Utf8,
        Volatility::Immutable,
        regexp_extract_udf,
    ));
    ctx.register_udf(create_udf(
        "split_index",
        vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
        DataType::Utf8,
        Volatility::Immutable,
        split_index_udf,
    ));
    ctx.register_udf(create_udf(
        "count_char",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Int64,
        Volatility::Immutable,
        passthrough_int64,
    ));
}
