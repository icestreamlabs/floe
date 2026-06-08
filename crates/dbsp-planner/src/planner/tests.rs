use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{Column, Result as DataFusionResult};
use datafusion::datasource::{TableProvider, empty::EmptyTable};
use datafusion::functions_aggregate::expr_fn::{avg, count, sum};
use datafusion::logical_expr::expr::WildcardOptions;
use datafusion::logical_expr::expr_fn::SimpleScalarUDF;
use datafusion::logical_expr::logical_plan::Sort as LogicalSort;
use datafusion::logical_expr::logical_plan::builder::LogicalTableSource;
use datafusion::logical_expr::{
    ColumnarValue, Expr, JoinType, LogicalPlanBuilder, ScalarFunctionImplementation, ScalarUDF,
    Signature, TableSource, TypeSignature, Volatility, col, lit,
};
use datafusion::prelude::SessionContext;

use dbsp_circuit::circuit::plan::{
    DbspAggregateFunction, DbspJoinType, DbspNodeKind, DbspWindowPolicy,
};
use dbsp_circuit::circuit::schema::Field;
use dbsp_circuit::circuit::tables as nexmark_tables;
use dbsp_circuit::circuit::tables::TableDescriptor;
use dbsp_circuit::circuit::types::DbspScalarType;

use super::expr::map_aggregate_expr;
use super::{CircuitPlanner, PlannerConfig};

fn planner_config() -> PlannerConfig {
    let mut config = PlannerConfig::new();
    config.register_table(nexmark_person_table());
    config.register_table(nexmark_person_alias_table());
    config.register_table(nexmark_auction_table());
    config.register_table(nexmark_auction_alias_table());
    config.register_table(nexmark_bid_table());
    config.register_table(nexmark_bid_alias_table());
    config
}

fn static_table(
    table: anyhow::Result<&'static TableDescriptor>,
    label: &str,
) -> &'static TableDescriptor {
    match table {
        Ok(table) => table,
        Err(error) => panic!("invalid {label} table descriptor: {error}"),
    }
}

fn nexmark_person_table() -> &'static TableDescriptor {
    static_table(nexmark_tables::nexmark_person_table(), "nexmark_person")
}

fn nexmark_person_alias_table() -> &'static TableDescriptor {
    static_table(nexmark_tables::nexmark_person_alias_table(), "person")
}

fn nexmark_auction_table() -> &'static TableDescriptor {
    static_table(nexmark_tables::nexmark_auction_table(), "nexmark_auction")
}

fn nexmark_auction_alias_table() -> &'static TableDescriptor {
    static_table(nexmark_tables::nexmark_auction_alias_table(), "auction")
}

fn nexmark_bid_table() -> &'static TableDescriptor {
    static_table(nexmark_tables::nexmark_bid_table(), "nexmark_bid")
}

fn nexmark_bid_alias_table() -> &'static TableDescriptor {
    static_table(nexmark_tables::nexmark_bid_alias_table(), "bid")
}

fn table_source(table: &'static TableDescriptor) -> Arc<dyn TableSource> {
    Arc::new(LogicalTableSource::new(table.schema().to_arrow_schema()))
}

fn table_source_owned(table: &TableDescriptor) -> Arc<dyn TableSource> {
    Arc::new(LogicalTableSource::new(table.schema().to_arrow_schema()))
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
    let array: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![None; len]));
    ColumnarValue::Array(array)
}

async fn sql_plan(sql: &str) -> datafusion::logical_expr::LogicalPlan {
    let ctx = SessionContext::new();
    for table in [
        nexmark_person_table(),
        nexmark_person_alias_table(),
        nexmark_auction_table(),
        nexmark_auction_alias_table(),
        nexmark_bid_table(),
        nexmark_bid_alias_table(),
    ] {
        let provider: Arc<dyn TableProvider> =
            Arc::new(EmptyTable::new(table.schema().to_arrow_schema()));
        ctx.register_table(table.name(), provider)
            .expect("register nexmark table");
    }
    let passthrough_ts: ScalarFunctionImplementation = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            Ok(args
                .first()
                .cloned()
                .unwrap_or_else(|| null_ts_value(udf_batch_len(args))))
        },
    );
    let ts = DataType::Timestamp(TimeUnit::Millisecond, None);
    let tumble_sig = Signature::one_of(
        vec![
            TypeSignature::Exact(vec![ts.clone(), DataType::Int64]),
            TypeSignature::Exact(vec![ts.clone(), DataType::Int64, DataType::Int64]),
        ],
        Volatility::Immutable,
    );
    let hop_sig = Signature::one_of(
        vec![
            TypeSignature::Exact(vec![ts.clone(), DataType::Int64, DataType::Int64]),
            TypeSignature::Exact(vec![
                ts.clone(),
                DataType::Int64,
                DataType::Int64,
                DataType::Int64,
            ]),
        ],
        Volatility::Immutable,
    );
    let session_sig = Signature::one_of(
        vec![
            TypeSignature::Exact(vec![ts.clone(), DataType::Int64]),
            TypeSignature::Exact(vec![ts.clone(), DataType::Int64, DataType::Int64]),
        ],
        Volatility::Immutable,
    );
    ctx.register_udf(ScalarUDF::from(SimpleScalarUDF::new_with_signature(
        "tumble",
        tumble_sig,
        ts.clone(),
        Arc::clone(&passthrough_ts),
    )));
    ctx.register_udf(ScalarUDF::from(SimpleScalarUDF::new_with_signature(
        "hop",
        hop_sig,
        ts.clone(),
        Arc::clone(&passthrough_ts),
    )));
    ctx.register_udf(ScalarUDF::from(SimpleScalarUDF::new_with_signature(
        "session",
        session_sig,
        ts,
        passthrough_ts,
    )));

    let state = ctx.state();
    super::create_logical_plan_with_asof_preplanner(&state, sql)
        .await
        .expect("build SQL logical plan")
}

fn qualified(table: &'static TableDescriptor, column: &str) -> String {
    format!("{}.{}", table.name(), column)
}

fn select_predicate_in_unary_chain(
    circuit_plan: &super::CircuitPlan,
    mut node_id: usize,
) -> Option<String> {
    loop {
        let node = circuit_plan.node(node_id)?;
        if let DbspNodeKind::Select(select) = &node.kind {
            return Some(format!("{:?}", select.predicate().expression().expr()));
        }
        if node.inputs.len() != 1 {
            return None;
        }
        node_id = node.inputs[0];
    }
}

mod aggregate_distinct;
mod joins;
mod optimizer_scan;
mod topn_windows;
