use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::{TableProvider, empty::EmptyTable};
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::expr_fn::{SimpleScalarUDF, create_udf};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionImplementation, ScalarUDF, Signature, TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;
use floe_sql_parser::MaterializedViewDefinition;

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
        let plan = ctx
            .state()
            .create_logical_plan(definition.query())
            .await
            .with_context(|| {
                format!(
                    "failed to create logical plan for materialized view {}",
                    definition.name()
                )
            })?;
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

fn planner_udfs() -> Vec<ScalarUDF> {

    let passthrough_ts: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            Ok(args
                .first()
                .cloned()
                .unwrap_or_else(|| null_ts_value(udf_batch_len(args))))
        },
    );
    let scalar_utf8: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            Ok(args
                .first()
                .cloned()
                .unwrap_or_else(|| null_utf8_value(udf_batch_len(args))))
        },
    );
    let scalar_int: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            Ok(args
                .first()
                .cloned()
                .unwrap_or_else(|| null_i64_value(udf_batch_len(args))))
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
            Arc::clone(&passthrough_ts),
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
            Arc::clone(&scalar_utf8),
        ),
        create_udf(
            "split_index",
            vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::clone(&scalar_utf8),
        ),
        create_udf(
            "date_format",
            vec![ts.clone(), DataType::Utf8],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::clone(&scalar_utf8),
        ),
        create_udf(
            "hour",
            vec![ts],
            DataType::Int64,
            Volatility::Immutable,
            Arc::clone(&scalar_int),
        ),
        create_udf(
            "count_char",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Int64,
            Volatility::Immutable,
            scalar_int,
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
                true,
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
            logical_plan.contains("Projection") && logical_plan.contains("bid"),
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
}
