use super::*;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::Expr;

pub(super) fn rename_batches(
    batches: &[RecordBatch],
    schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    batches
        .iter()
        .map(|batch| {
            if batch.num_columns() != schema.fields().len() {
                bail!("alias schema column count does not match source batch");
            }
            Ok(RecordBatch::try_new(
                Arc::clone(schema),
                batch.columns().to_vec(),
            )?)
        })
        .collect()
}

pub(super) fn dynamic_state_provider(
    schema: SchemaRef,
    key_indices: Option<&[usize]>,
) -> Result<DynamicStateTableProvider> {
    match key_indices {
        Some(indices) if !indices.is_empty() => {
            DynamicStateTableProvider::new_with_key_indices(schema, indices.to_vec())
        }
        _ => Ok(DynamicStateTableProvider::new(schema)),
    }
}

pub(super) fn source_key_indices(
    schema: &SchemaRef,
    primary_key_columns: &[String],
) -> Result<Option<Vec<usize>>> {
    if primary_key_columns.is_empty() {
        return Ok(None);
    }
    primary_key_columns
        .iter()
        .map(|column| {
            schema.index_of(column).with_context(|| {
                format!("source primary key column '{column}' missing from schema")
            })
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

pub(super) fn incremental_source_for_plan(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Option<String> {
    if plan_contains_expression_subquery(plan) {
        return None;
    }
    incremental_source_for_plan_inner(plan, sources)
}

fn incremental_source_for_plan_inner(
    plan: &LogicalPlan,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Option<String> {
    match plan {
        LogicalPlan::Projection(projection) => {
            incremental_source_for_plan_inner(projection.input.as_ref(), sources)
        }
        LogicalPlan::Filter(filter) => {
            incremental_source_for_plan_inner(filter.input.as_ref(), sources)
        }
        LogicalPlan::Sort(sort) => incremental_source_for_plan_inner(sort.input.as_ref(), sources),
        LogicalPlan::SubqueryAlias(alias) => {
            incremental_source_for_plan_inner(alias.input.as_ref(), sources)
        }
        LogicalPlan::TableScan(scan) => resolve_source_table(scan.table_name.to_string(), sources),
        _ => None,
    }
}

pub(super) fn plan_contains_expression_subquery(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        for expr in node.expressions() {
            if expr_contains_subquery(&expr) {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

fn expr_contains_subquery(expr: &Expr) -> bool {
    expr.exists(|expr| {
        Ok(matches!(
            expr,
            Expr::Exists(_) | Expr::InSubquery(_) | Expr::ScalarSubquery(_)
        ))
    })
    .unwrap_or(true)
}

pub(super) fn resolve_source_table(
    table_name: String,
    sources: &HashMap<String, VectorizedSourceState>,
) -> Option<String> {
    if sources.contains_key(&table_name) {
        return Some(table_name);
    }
    sources
        .keys()
        .find(|source_name| source_name.strip_prefix("nexmark_") == Some(table_name.as_str()))
        .cloned()
}

pub(super) fn source_primary_key_columns(definition: &SourceDefinition) -> Vec<String> {
    definition
        .property(SOURCE_PRIMARY_KEY_PROPERTY)
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|column| !column.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn camel_case_schema(definition: &SourceDefinition) -> SchemaRef {
    let fields = definition
        .columns()
        .iter()
        .map(|column| {
            Field::new(
                to_camel_case(column.name()),
                column.data_type().arrow_type(),
                column.nullable(),
            )
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

fn to_camel_case(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut uppercase_next = false;
    for ch in input.chars() {
        if ch == '_' {
            uppercase_next = true;
            continue;
        }
        if uppercase_next {
            for upper in ch.to_uppercase() {
                out.push(upper);
            }
            uppercase_next = false;
        } else {
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use floe_core::source::{SourceColumn, SourceDataType, SourceDefinition};

    use super::*;

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
}
