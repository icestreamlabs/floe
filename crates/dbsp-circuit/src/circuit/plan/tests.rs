use std::sync::Arc;

use super::*;
use arrow_schema::{DataType, TimeUnit};
use datafusion::logical_expr::expr_fn::cast;
use datafusion::logical_expr::{Expr, Like, col, lit};

use crate::circuit::schema::{Field, RowSchema};
use crate::circuit::types::DbspScalarType;

fn base_schema() -> Arc<RowSchema> {
    RowSchema::try_new(vec![
        Field::new("id", DbspScalarType::Int64, false),
        Field::new("value", DbspScalarType::Int64, false),
        Field::new("text", DbspScalarType::Utf8, false),
        Field::new("flag", DbspScalarType::Bool, false),
    ])
    .expect("schema")
}

#[test]
fn select_requires_boolean_predicate() {
    let schema = base_schema();
    assert!(DbspSelectNode::try_new(schema.clone(), col("flag")).is_ok());
    let err = DbspSelectNode::try_new(schema.clone(), col("value")).unwrap_err();
    assert!(err.to_string().contains("predicate must return Bool"));
}

#[test]
fn project_builds_output_schema() {
    let schema = base_schema();
    let project = DbspProjectNode::try_new(
        schema.clone(),
        vec![
            ProjectItem {
                expr: col("id"),
                alias: Some("pid".to_string()),
            },
            ProjectItem {
                expr: (col("value") + lit(1i64)).alias("value_plus_one"),
                alias: None,
            },
        ],
    )
    .expect("project");

    assert_eq!(project.output_schema().len(), 2);
}

#[test]
fn join_requires_matching_key_types() {
    let left = base_schema();
    let right = RowSchema::try_new(vec![Field::new("other_id", DbspScalarType::Utf8, false)])
        .expect("right");

    let err = DbspJoinNode::try_new(
        DbspJoinType::Inner,
        left.clone(),
        right.clone(),
        vec![(col("id"), col("other_id"))],
        None,
    )
    .unwrap_err();
    assert!(err.to_string().contains("join key type mismatch"));
}

#[test]
fn aggregate_output_schema_combines_keys_and_aggs() {
    let schema = base_schema();
    let agg = DbspAggregateNode::try_new(
        schema.clone(),
        vec![(col("id"), Some("id".to_string()))],
        vec![(
            DbspAggregateFunction::Sum,
            Some(col("value")),
            Some("total".to_string()),
        )],
    )
    .expect("aggregate");
    assert_eq!(agg.output_schema().len(), 2);
}

#[test]
fn arithmetic_requires_int64_operands() {
    let schema = base_schema();
    assert!(DbspExpression::analyze(col("value") + lit(5i64), schema.clone()).is_ok());

    assert!(DbspExpression::analyze(col("text") + lit("foo"), schema.clone()).is_err());
}

#[test]
fn boolean_operations_require_boolean_inputs() {
    let schema = base_schema();
    assert!(DbspExpression::analyze(col("flag").and(col("flag")), schema.clone()).is_ok());

    assert!(DbspExpression::analyze(col("value").and(col("flag")), schema.clone()).is_err());
}

#[test]
fn like_only_allows_prefix_or_suffix_wildcards() {
    let schema = base_schema();
    let valid = Expr::Like(Like::new(
        false,
        Box::new(col("text")),
        Box::new(lit("foo%")),
        None,
        false,
    ));
    assert!(DbspExpression::analyze(valid, schema.clone()).is_ok());

    let invalid = Expr::Like(Like::new(
        false,
        Box::new(col("text")),
        Box::new(lit("%foo%")),
        None,
        false,
    ));
    assert!(DbspExpression::analyze(invalid, schema.clone()).is_err());

    let escaped = Expr::Like(Like::new(
        false,
        Box::new(col("text")),
        Box::new(lit("foo%")),
        Some('\\'),
        false,
    ));
    assert!(DbspExpression::analyze(escaped, schema.clone()).is_err());
}

#[test]
fn cast_matrix_rejects_unsupported_types() {
    let schema = base_schema();
    let to_timestamp = cast(
        col("value"),
        DataType::Timestamp(TimeUnit::Millisecond, None),
    );
    assert!(DbspExpression::analyze(to_timestamp, schema.clone()).is_ok());

    let err = DbspExpression::analyze(cast(col("text"), DataType::Boolean), schema).unwrap_err();
    assert!(
        err.to_string()
            .contains("casts are restricted to Int64/Utf8/TimestampMillis conversions")
    );
}
