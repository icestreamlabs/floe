use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::logical_expr::{Expr, LogicalPlan, TableScan};

use super::VectorizedSourceState;
use super::source_state::resolve_source_table;

pub(super) fn source_set_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> BTreeSet<String> {
    let mut source_names = BTreeSet::new();
    collect_sources(plan, sources, &mut source_names);
    source_names
}

fn collect_sources(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
    source_names: &mut BTreeSet<String>,
) {
    if let LogicalPlan::TableScan(scan) = plan
        && let Some(source_name) = table_scan_source(scan, sources)
    {
        source_names.insert(source_name);
    }
    for input in plan.inputs() {
        collect_sources(input, sources, source_names);
    }
}

pub(super) fn table_scan_source(
    scan: &TableScan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Option<String> {
    resolve_source_table(scan.table_name.table().to_string(), sources)
}

pub(super) fn strip_alias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => strip_alias(alias.expr.as_ref()),
        _ => expr,
    }
}

pub(super) fn derived_relation_name(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Projection(projection) => derived_relation_name(projection.input.as_ref()),
        LogicalPlan::Filter(filter) => derived_relation_name(filter.input.as_ref()),
        LogicalPlan::SubqueryAlias(alias) => Some(alias.alias.to_string()),
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => {
            derived_relation_name(sort.input.as_ref())
        }
        _ => None,
    }
}

pub(super) fn df_schema_to_arrow(schema: &datafusion::common::DFSchemaRef) -> Result<SchemaRef> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| Field::new(field.name(), field.data_type().clone(), field.is_nullable()))
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new(fields)))
}

pub(super) fn row_converter_for_schema(schema: &SchemaRef) -> Result<RowConverter> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| SortField::new(field.data_type().clone()))
        .collect::<Vec<_>>();
    RowConverter::new(fields).context("build Arrow row converter")
}

pub(super) fn rewrap_record_batches_with_schema(
    batches: &[RecordBatch],
    schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    batches
        .iter()
        .map(|batch| {
            if batch.num_columns() != schema.fields().len() {
                bail!(
                    "record batch width {} does not match schema width {}",
                    batch.num_columns(),
                    schema.fields().len()
                );
            }
            for (idx, field) in schema.fields().iter().enumerate() {
                let actual_type = batch.column(idx).data_type();
                if actual_type != field.data_type() {
                    bail!(
                        "record batch column {idx} type {actual_type:?} does not match expected {:?}",
                        field.data_type()
                    );
                }
            }
            Ok(RecordBatch::try_new(
                Arc::clone(schema),
                batch.columns().to_vec(),
            )?)
        })
        .collect()
}
