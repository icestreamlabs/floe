use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::Utc;
use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, Int64Builder, StringArray, StringBuilder,
    TimestampMillisecondArray, TimestampMillisecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::{TableProvider, empty::EmptyTable};
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::expr_fn::{SimpleScalarUDF, create_udf};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionImplementation, ScalarUDF, Signature, TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;
use dbsp_planner::create_logical_plan_with_asof_preplanner;
use floe_sql_parser::MaterializedViewDefinition;
use regex::Regex;

use crate::source::SourceRegistry;

#[derive(Debug, Clone)]
pub struct PlannedMaterializedView {
    definition: MaterializedViewDefinition,
    logical_plan: LogicalPlan,
}

impl PlannedMaterializedView {
    pub fn definition(&self) -> &MaterializedViewDefinition {
        &self.definition
    }

    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.logical_plan
    }
}

pub async fn plan_materialized_views(
    sources: &SourceRegistry,
    definitions: &[MaterializedViewDefinition],
) -> Result<Vec<PlannedMaterializedView>> {
    if definitions.is_empty() {
        return Ok(Vec::new());
    }

    let ctx = SessionContext::new();

    register_sources(&ctx, sources).await?;
    register_nexmark_udfs(&ctx);

    let mut plans = Vec::with_capacity(definitions.len());
    for definition in definitions {
        let state = ctx.state();
        let plan = create_logical_plan_with_asof_preplanner(&state, definition.query())
            .await
            .with_context(|| {
                format!(
                    "failed to create logical plan for materialized view {}",
                    definition.name()
                )
            })?;
        let optimized = state.optimize(&plan).with_context(|| {
            format!(
                "failed to optimize logical plan for materialized view {}",
                definition.name()
            )
        })?;
        let plan = if logical_plan_uses_only_dbsp_supported_types(&optimized) {
            optimized
        } else {
            tracing::debug!(
                view = %definition.name(),
                "falling back to unoptimized logical plan because optimized plan uses unsupported DBSP types"
            );
            plan
        };
        plans.push(PlannedMaterializedView {
            definition: definition.clone(),
            logical_plan: plan,
        });
    }

    Ok(plans)
}

async fn register_sources(ctx: &SessionContext, sources: &SourceRegistry) -> Result<()> {
    for definition in sources.definitions() {
        let schema = definition.to_arrow_schema();
        let base_table: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(Arc::clone(&schema)));
        ctx.register_table(definition.name(), Arc::clone(&base_table))
            .with_context(|| format!("failed to register source {}", definition.name()))?;

        if let Some(short_name) = definition.name().strip_prefix("nexmark_") {
            let alias_schema = camel_case_schema(definition);
            let alias_table: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(alias_schema));
            ctx.register_table(short_name, Arc::clone(&alias_table))
                .with_context(|| {
                    format!(
                        "failed to register alias {short_name} for source {}",
                        definition.name()
                    )
                })?;
        }
    }

    Ok(())
}

fn register_nexmark_udfs(ctx: &SessionContext) {
    for udf in planner_udfs() {
        ctx.register_udf(udf);
    }
}

fn logical_plan_uses_only_dbsp_supported_types(plan: &LogicalPlan) -> bool {
    logical_plan_node_supported(plan)
        && plan
            .inputs()
            .into_iter()
            .all(logical_plan_uses_only_dbsp_supported_types)
}

fn logical_plan_node_supported(plan: &LogicalPlan) -> bool {
    plan.schema()
        .fields()
        .iter()
        .all(|field| dbsp_supported_arrow_type(field.data_type()))
}

fn dbsp_supported_arrow_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int64
            | DataType::Utf8
            | DataType::Boolean
            | DataType::Timestamp(TimeUnit::Millisecond, None)
            | DataType::Date32
            | DataType::Decimal128(_, _)
    )
}

fn passthrough_window_udf(
    name: &str,
    signatures: Vec<Vec<DataType>>,
    return_type: DataType,
    fun: ScalarFunctionImplementation,
) -> ScalarUDF {
    let signature = Signature::one_of(
        signatures.into_iter().map(TypeSignature::Exact).collect(),
        Volatility::Immutable,
    );
    ScalarUDF::from(SimpleScalarUDF::new_with_signature(
        name,
        signature,
        return_type,
        fun,
    ))
}

fn udf_batch_len(args: &[ColumnarValue]) -> usize {
    args.iter()
        .find_map(|arg| match arg {
            ColumnarValue::Array(array) => Some(array.len()),
            ColumnarValue::Scalar(_) => None,
        })
        .unwrap_or(1)
}

fn null_ts_value(len: usize) -> ColumnarValue {
    let array: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![None::<i64>; len]));
    ColumnarValue::Array(array)
}

fn null_utf8_value(len: usize) -> ColumnarValue {
    let array: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>; len]));
    ColumnarValue::Array(array)
}

fn null_i64_value(len: usize) -> ColumnarValue {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![None::<i64>; len]));
    ColumnarValue::Array(array)
}

fn translate_date_format_pattern(pattern: &str) -> String {
    pattern
        .replace("yyyy", "%Y")
        .replace("MM", "%m")
        .replace("dd", "%d")
        .replace("HH", "%H")
        .replace("mm", "%M")
        .replace("ss", "%S")
}

fn split_index_value(text: &str, delimiter: &str, index: i64) -> Option<String> {
    if index < 0 || delimiter.is_empty() {
        return None;
    }
    text.split(delimiter)
        .nth(index as usize)
        .map(str::to_string)
}

pub fn planner_udfs() -> Vec<ScalarUDF> {
    let passthrough_ts: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            Ok(args
                .first()
                .cloned()
                .unwrap_or_else(|| null_ts_value(udf_batch_len(args))))
        },
    );
    let tumble_udf: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = udf_batch_len(args);
            let ts = args
                .first()
                .cloned()
                .unwrap_or_else(|| null_ts_value(len))
                .into_array(len)?;
            let size = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| null_i64_value(len))
                .into_array(len)?;
            let (Some(ts), Some(size)) = (
                ts.as_any().downcast_ref::<TimestampMillisecondArray>(),
                size.as_any().downcast_ref::<Int64Array>(),
            ) else {
                return Ok(null_ts_value(len));
            };

            let mut out = TimestampMillisecondBuilder::with_capacity(len)
                .with_data_type(DataType::Timestamp(TimeUnit::Millisecond, None));
            for row_idx in 0..len {
                if ts.is_null(row_idx) || size.is_null(row_idx) {
                    out.append_null();
                    continue;
                }
                let size_ms = size.value(row_idx);
                if size_ms <= 0 {
                    out.append_null();
                    continue;
                }
                let millis = ts.value(row_idx);
                out.append_value(millis.div_euclid(size_ms) * size_ms);
            }
            Ok(ColumnarValue::Array(Arc::new(out.finish())))
        },
    );
    let date_format_udf: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = udf_batch_len(args);
            let ts = args
                .first()
                .cloned()
                .unwrap_or_else(|| null_ts_value(len))
                .into_array(len)?;
            let fmt = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| null_utf8_value(len))
                .into_array(len)?;
            let (Some(ts), Some(fmt)) = (
                ts.as_any().downcast_ref::<TimestampMillisecondArray>(),
                fmt.as_any().downcast_ref::<StringArray>(),
            ) else {
                return Ok(null_utf8_value(len));
            };

            let mut out = StringBuilder::new();
            for row_idx in 0..len {
                if ts.is_null(row_idx) || fmt.is_null(row_idx) {
                    out.append_null();
                    continue;
                }

                let Some(dt) = chrono::DateTime::<Utc>::from_timestamp_millis(ts.value(row_idx))
                else {
                    out.append_null();
                    continue;
                };
                let pattern = translate_date_format_pattern(fmt.value(row_idx));
                out.append_value(dt.format(&pattern).to_string());
            }
            Ok(ColumnarValue::Array(Arc::new(out.finish())))
        },
    );
    let regexp_extract_udf: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = udf_batch_len(args);
            let text = args
                .first()
                .cloned()
                .unwrap_or_else(|| null_utf8_value(len))
                .into_array(len)?;
            let pattern = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| null_utf8_value(len))
                .into_array(len)?;
            let group = args
                .get(2)
                .cloned()
                .unwrap_or_else(|| null_i64_value(len))
                .into_array(len)?;
            let (Some(text), Some(pattern), Some(group)) = (
                text.as_any().downcast_ref::<StringArray>(),
                pattern.as_any().downcast_ref::<StringArray>(),
                group.as_any().downcast_ref::<Int64Array>(),
            ) else {
                return Ok(null_utf8_value(len));
            };

            let mut cache: HashMap<String, Option<Regex>> = HashMap::new();
            let mut out = StringBuilder::new();
            for row_idx in 0..len {
                if text.is_null(row_idx) || pattern.is_null(row_idx) || group.is_null(row_idx) {
                    out.append_null();
                    continue;
                }

                let group_idx = group.value(row_idx);
                if group_idx < 0 {
                    out.append_null();
                    continue;
                }

                let pattern_text = pattern.value(row_idx);
                let regex = cache
                    .entry(pattern_text.to_string())
                    .or_insert_with(|| Regex::new(pattern_text).ok());
                let Some(regex) = regex.as_ref() else {
                    out.append_null();
                    continue;
                };
                let Some(captures) = regex.captures(text.value(row_idx)) else {
                    out.append_null();
                    continue;
                };
                let Some(matched) = captures.get(group_idx as usize) else {
                    out.append_null();
                    continue;
                };
                out.append_value(matched.as_str());
            }
            Ok(ColumnarValue::Array(Arc::new(out.finish())))
        },
    );
    let split_index_udf: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = udf_batch_len(args);
            let text = args
                .first()
                .cloned()
                .unwrap_or_else(|| null_utf8_value(len))
                .into_array(len)?;
            let delimiter = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| null_utf8_value(len))
                .into_array(len)?;
            let index = args
                .get(2)
                .cloned()
                .unwrap_or_else(|| null_i64_value(len))
                .into_array(len)?;
            let (Some(text), Some(delimiter), Some(index)) = (
                text.as_any().downcast_ref::<StringArray>(),
                delimiter.as_any().downcast_ref::<StringArray>(),
                index.as_any().downcast_ref::<Int64Array>(),
            ) else {
                return Ok(null_utf8_value(len));
            };

            let mut out = StringBuilder::new();
            for row_idx in 0..len {
                if text.is_null(row_idx) || delimiter.is_null(row_idx) || index.is_null(row_idx) {
                    out.append_null();
                    continue;
                }

                match split_index_value(
                    text.value(row_idx),
                    delimiter.value(row_idx),
                    index.value(row_idx),
                ) {
                    Some(value) => out.append_value(value),
                    None => out.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(out.finish())))
        },
    );
    let hour_udf: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = udf_batch_len(args);
            let ts = args
                .first()
                .cloned()
                .unwrap_or_else(|| null_ts_value(len))
                .into_array(len)?;
            let Some(ts) = ts.as_any().downcast_ref::<TimestampMillisecondArray>() else {
                return Ok(null_i64_value(len));
            };
            let mut out = Int64Builder::with_capacity(len);
            for row_idx in 0..len {
                if ts.is_null(row_idx) {
                    out.append_null();
                } else {
                    // Floor-division based UTC hour extraction from epoch millis.
                    let millis = ts.value(row_idx);
                    let hour = millis.div_euclid(3_600_000).rem_euclid(24);
                    out.append_value(hour);
                }
            }
            Ok(ColumnarValue::Array(Arc::new(out.finish())))
        },
    );
    let count_char_udf: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            let len = udf_batch_len(args);
            let text = args
                .first()
                .cloned()
                .unwrap_or_else(|| null_utf8_value(len))
                .into_array(len)?;
            let needle = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| null_utf8_value(len))
                .into_array(len)?;
            let (Some(text), Some(needle)) = (
                text.as_any().downcast_ref::<StringArray>(),
                needle.as_any().downcast_ref::<StringArray>(),
            ) else {
                return Ok(null_i64_value(len));
            };

            let mut out = Int64Builder::with_capacity(len);
            for row_idx in 0..len {
                if text.is_null(row_idx) || needle.is_null(row_idx) {
                    out.append_null();
                    continue;
                }
                let haystack = text.value(row_idx);
                let token = needle.value(row_idx);
                let count = if token.is_empty() {
                    0_i64
                } else {
                    i64::try_from(haystack.matches(token).count()).unwrap_or(i64::MAX)
                };
                out.append_value(count);
            }
            Ok(ColumnarValue::Array(Arc::new(out.finish())))
        },
    );
    let proctime: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            Ok(null_ts_value(udf_batch_len(args)))
        },
    );

    let ts = DataType::Timestamp(TimeUnit::Millisecond, None);
    vec![
        passthrough_window_udf(
            "tumble",
            vec![
                vec![ts.clone(), DataType::Int64],
                vec![ts.clone(), DataType::Int64, DataType::Int64],
            ],
            ts.clone(),
            Arc::clone(&tumble_udf),
        ),
        passthrough_window_udf(
            "hop",
            vec![
                vec![ts.clone(), DataType::Int64, DataType::Int64],
                vec![
                    ts.clone(),
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                ],
            ],
            ts.clone(),
            Arc::clone(&passthrough_ts),
        ),
        passthrough_window_udf(
            "session",
            vec![
                vec![ts.clone(), DataType::Int64],
                vec![ts.clone(), DataType::Int64, DataType::Int64],
            ],
            ts.clone(),
            Arc::clone(&passthrough_ts),
        ),
        passthrough_window_udf(
            "tumble_start",
            vec![
                vec![ts.clone(), DataType::Int64],
                vec![ts.clone(), DataType::Int64, DataType::Int64],
            ],
            ts.clone(),
            Arc::clone(&passthrough_ts),
        ),
        passthrough_window_udf(
            "tumble_end",
            vec![
                vec![ts.clone(), DataType::Int64],
                vec![ts.clone(), DataType::Int64, DataType::Int64],
            ],
            ts.clone(),
            Arc::clone(&passthrough_ts),
        ),
        passthrough_window_udf(
            "hop_start",
            vec![
                vec![ts.clone(), DataType::Int64, DataType::Int64],
                vec![
                    ts.clone(),
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                ],
            ],
            ts.clone(),
            Arc::clone(&passthrough_ts),
        ),
        passthrough_window_udf(
            "hop_end",
            vec![
                vec![ts.clone(), DataType::Int64, DataType::Int64],
                vec![
                    ts.clone(),
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                ],
            ],
            ts.clone(),
            Arc::clone(&passthrough_ts),
        ),
        passthrough_window_udf(
            "tumble_rowtime",
            vec![
                vec![ts.clone(), DataType::Int64],
                vec![ts.clone(), DataType::Int64, DataType::Int64],
            ],
            ts.clone(),
            Arc::clone(&passthrough_ts),
        ),
        create_udf(
            "proctime",
            vec![],
            ts.clone(),
            Volatility::Volatile,
            proctime,
        ),
        create_udf(
            "regexp_extract",
            vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
            DataType::Utf8,
            Volatility::Immutable,
            regexp_extract_udf,
        ),
        create_udf(
            "split_index",
            vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
            DataType::Utf8,
            Volatility::Immutable,
            split_index_udf,
        ),
        create_udf(
            "date_format",
            vec![ts.clone(), DataType::Utf8],
            DataType::Utf8,
            Volatility::Immutable,
            date_format_udf,
        ),
        create_udf(
            "hour",
            vec![ts],
            DataType::Int64,
            Volatility::Immutable,
            Arc::clone(&hour_udf),
        ),
        create_udf(
            "count_char",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Int64,
            Volatility::Immutable,
            count_char_udf,
        ),
    ]
}

pub fn camel_case_schema(definition: &floe_core::source::SourceDefinition) -> Arc<Schema> {
    let fields: Vec<Field> = definition
        .columns()
        .iter()
        .map(|column| {
            Field::new(
                to_camel_case(column.name()),
                column.data_type().arrow_type(),
                column.nullable(),
            )
        })
        .collect();
    Arc::new(Schema::new(fields))
}

pub(crate) fn to_camel_case(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut uppercase_next = false;

    for (idx, ch) in input.chars().enumerate() {
        if ch == '_' {
            uppercase_next = true;
            continue;
        }

        if idx == 0 {
            output.push(ch.to_ascii_lowercase());
        } else if uppercase_next {
            output.push(ch.to_ascii_uppercase());
            uppercase_next = false;
        } else {
            output.push(ch);
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use anyhow::Context;
    use datafusion::arrow::array::{Array, StringArray};
    use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition};
    use floe_sql_parser::parse_materialized_view;

    use super::*;

    #[tokio::test]
    async fn plans_simple_select() {
        let mut registry = SourceRegistry::new();
        registry.extend(crate::generator::definitions().expect("definitions"));

        let definition =
            parse_materialized_view("CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_person")
                .expect("parse mv");

        let planned = plan_materialized_views(&registry, &[definition])
            .await
            .expect("plan mv");

        assert_eq!(planned.len(), 1);
        let logical_plan = planned[0].logical_plan();
        assert!(
            logical_plan
                .display_indent()
                .to_string()
                .contains("nexmark_person")
        );
        assert_eq!(planned[0].definition().name(), "mv");
    }

    #[tokio::test]
    async fn plans_canonical_bid_select() {
        let mut registry = SourceRegistry::new();
        registry.extend(crate::generator::definitions().expect("definitions"));

        let definition = parse_materialized_view(
            "CREATE MATERIALIZED VIEW mv AS SELECT auction, \"dateTime\" FROM bid",
        )
        .expect("parse mv");

        let planned = plan_materialized_views(&registry, &[definition])
            .await
            .expect("plan mv");

        assert_eq!(planned.len(), 1);
        let logical_plan = planned[0].logical_plan().display_indent().to_string();
        assert!(
            logical_plan.contains("bid")
                && (logical_plan.contains("Projection")
                    || logical_plan.contains("TableScan: bid projection=[auction, dateTime]")),
            "logical plan was: {logical_plan}"
        );
        assert_eq!(planned[0].definition().name(), "mv");
    }

    #[tokio::test]
    async fn plans_reference_nexmark_queries() {
        let mut registry = SourceRegistry::new();
        registry.extend(crate::generator::definitions().expect("definitions"));

        let queries: Vec<(&str, &str)> = vec![
            (
                "q0",
                "SELECT auction, bidder, price, \"dateTime\", extra FROM bid",
            ),
            (
                "q1",
                "SELECT auction, bidder, price * 0.908 AS converted_price, \"dateTime\", extra FROM bid",
            ),
            (
                "q2",
                "SELECT auction, price FROM bid WHERE auction % 123 = 0",
            ),
            (
                "q3",
                "SELECT p.name, p.city, p.state, a.id FROM auction AS a JOIN person AS p ON a.seller = p.id WHERE a.category = 10 AND p.state IN ('or', 'id', 'ca')",
            ),
            (
                "q4",
                "SELECT category, AVG(final_price) FROM (SELECT MAX(b.price) AS final_price, a.category FROM auction a JOIN bid b ON a.id = b.auction WHERE b.\"dateTime\" BETWEEN a.\"dateTime\" AND a.expires GROUP BY a.id, a.category) perAuction GROUP BY category",
            ),
            (
                "q6",
                "SELECT seller, AVG(price) OVER (PARTITION BY seller ORDER BY \"dateTime\" ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS moving_avg_price FROM (SELECT a.seller, b.price, b.\"dateTime\", ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC) AS rownum FROM auction a JOIN bid b ON a.id = b.auction WHERE b.\"dateTime\" BETWEEN a.\"dateTime\" AND a.expires) ranked WHERE rownum <= 1",
            ),
            (
                "q9",
                "SELECT id, \"itemName\", description, \"initialBid\", reserve, \"dateTime\", expires, seller, category, extra, auction, bidder, price, \"bidTime\", \"bidExtra\" FROM (SELECT a.id, a.\"itemName\", a.description, a.\"initialBid\", a.reserve, a.\"dateTime\", a.expires, a.seller, a.category, a.extra, b.auction, b.bidder, b.price, b.\"dateTime\" AS \"bidTime\", b.extra AS \"bidExtra\", ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY b.price DESC, b.\"dateTime\" ASC) AS rownum FROM auction a JOIN bid b ON a.id = b.auction WHERE b.\"dateTime\" BETWEEN a.\"dateTime\" AND a.expires) ranked WHERE rownum <= 1",
            ),
            (
                "q18",
                "SELECT auction, bidder, price, channel, url, \"dateTime\", extra FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY \"dateTime\" DESC) AS rank_number FROM bid) dedup WHERE rank_number <= 1",
            ),
            (
                "q19",
                "SELECT auction, bidder, price, channel, url, \"dateTime\", extra FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number FROM bid) ranked WHERE rank_number <= 10",
            ),
            (
                "q20",
                "SELECT b.auction, b.bidder, b.price, b.channel, b.url, b.\"dateTime\", b.extra, a.\"itemName\", a.description, a.\"initialBid\", a.reserve, a.\"dateTime\" AS auction_time, a.expires, a.seller, a.category, a.extra AS auction_extra FROM bid AS b JOIN auction AS a ON b.auction = a.id WHERE a.category = 10",
            ),
        ];

        let materialized_views: Vec<_> = queries
            .iter()
            .map(|(name, query)| {
                parse_materialized_view(&format!("CREATE MATERIALIZED VIEW {name} AS {query}"))
                    .with_context(|| format!("failed to parse query {name}"))
                    .expect("parse mv")
            })
            .collect();

        let planned = plan_materialized_views(&registry, &materialized_views)
            .await
            .expect("plan mv");

        assert_eq!(planned.len(), materialized_views.len());
        for (plan, (name, _)) in planned.iter().zip(queries.iter()) {
            let display = plan.logical_plan().display_indent().to_string();
            assert!(
                display.contains("TableScan"),
                "logical plan for {name} did not render expected output: {display}"
            );
            assert_eq!(plan.definition().name(), *name);
        }
    }

    #[tokio::test]
    async fn plans_catalog_table_from_registry() {
        let mut registry = SourceRegistry::new();
        registry.register(
            SourceDefinition::new(
                "orders",
                vec![
                    SourceColumn::new_nullable("id", SourceDataType::Int64, false),
                    SourceColumn::new_nullable("note", SourceDataType::Utf8, true),
                ],
            )
            .expect("source definition"),
        );

        let definition =
            parse_materialized_view("CREATE MATERIALIZED VIEW mv_orders AS SELECT id FROM orders")
                .expect("parse mv");

        let planned = plan_materialized_views(&registry, &[definition])
            .await
            .expect("plan mv");

        assert_eq!(planned.len(), 1);
        let logical_plan = planned[0].logical_plan().display_indent().to_string();
        assert!(
            logical_plan.contains("orders"),
            "logical plan was: {logical_plan}"
        );
    }

    #[test]
    fn supported_arrow_type_guard_covers_dbsp_scalar_types() {
        assert!(dbsp_supported_arrow_type(&DataType::Int64));
        assert!(dbsp_supported_arrow_type(&DataType::Utf8));
        assert!(dbsp_supported_arrow_type(&DataType::Boolean));
        assert!(dbsp_supported_arrow_type(&DataType::Timestamp(
            TimeUnit::Millisecond,
            None
        )));
        assert!(dbsp_supported_arrow_type(&DataType::Date32));
        assert!(dbsp_supported_arrow_type(&DataType::Decimal128(38, 9)));
    }

    #[test]
    fn camel_case_schema_preserves_source_nullability() {
        let definition = SourceDefinition::new(
            "nexmark_order",
            vec![
                SourceColumn::new_nullable("order_id", SourceDataType::Int64, false),
                SourceColumn::new_nullable("note_text", SourceDataType::Utf8, true),
            ],
        )
        .expect("source definition");

        let schema = camel_case_schema(&definition);

        assert_eq!(schema.field(0).name(), "orderId");
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "noteText");
        assert!(schema.field(1).is_nullable());
    }

    #[tokio::test]
    async fn plans_session_window_grouping_query() {
        let mut registry = SourceRegistry::new();
        registry.extend(crate::generator::definitions().expect("definitions"));

        let definition = parse_materialized_view(
            "CREATE MATERIALIZED VIEW mv_session AS \
             SELECT bidder, COUNT(*) AS bid_count \
             FROM bid \
             GROUP BY bidder, SESSION(\"dateTime\", 5000, 1000)",
        )
        .expect("parse mv");

        let planned = plan_materialized_views(&registry, &[definition])
            .await
            .expect("plan mv");

        assert_eq!(planned.len(), 1);
        let logical_plan = planned[0].logical_plan().display_indent().to_string();
        assert!(
            logical_plan.contains("Aggregate"),
            "logical plan was: {logical_plan}"
        );
    }

    #[tokio::test]
    async fn regexp_extract_returns_capture_and_null_for_invalid_pattern() {
        let ctx = SessionContext::new();
        register_nexmark_udfs(&ctx);

        let batches = ctx
            .sql(
                "SELECT \
                 REGEXP_EXTRACT('x&channel_id=abc123&y=1', '(&|^)channel_id=([^&]*)', 2) AS capture, \
                 REGEXP_EXTRACT('abc', '(', 1) AS invalid_pattern, \
                 REGEXP_EXTRACT('abc', '(a)', 9) AS missing_group",
            )
            .await
            .expect("build udf query")
            .collect()
            .await
            .expect("collect udf query");

        let batch = batches.first().expect("single batch");
        let capture = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("capture string array");
        let invalid_pattern = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("invalid_pattern string array");
        let missing_group = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("missing_group string array");

        assert_eq!(capture.value(0), "abc123");
        assert!(invalid_pattern.is_null(0));
        assert!(missing_group.is_null(0));
    }

    #[tokio::test]
    async fn split_index_returns_segments_and_null_for_invalid_inputs() {
        let ctx = SessionContext::new();
        register_nexmark_udfs(&ctx);

        let batches = ctx
            .sql(
                "SELECT \
                 SPLIT_INDEX('https://example.com/dir/item/42', '/', 3) AS dir1, \
                 SPLIT_INDEX('https://example.com/dir/item/42', '/', 4) AS dir2, \
                 SPLIT_INDEX('https://example.com/dir/item/42', '/', 5) AS dir3, \
                 SPLIT_INDEX('abc', '', 0) AS empty_delimiter, \
                 SPLIT_INDEX('abc', '/', -1) AS negative_index, \
                 SPLIT_INDEX('abc', '/', 5) AS out_of_range",
            )
            .await
            .expect("build split_index query")
            .collect()
            .await
            .expect("collect split_index query");

        let batch = batches.first().expect("single batch");
        let dir1 = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("dir1 string array");
        let dir2 = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("dir2 string array");
        let dir3 = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("dir3 string array");
        let empty_delimiter = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("empty_delimiter string array");
        let negative_index = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("negative_index string array");
        let out_of_range = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("out_of_range string array");

        assert_eq!(dir1.value(0), "dir");
        assert_eq!(dir2.value(0), "item");
        assert_eq!(dir3.value(0), "42");
        assert!(empty_delimiter.is_null(0));
        assert!(negative_index.is_null(0));
        assert!(out_of_range.is_null(0));
    }
}
