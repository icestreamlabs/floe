use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Result, bail};
use datafusion::logical_expr::Expr;

use crate::circuit::schema::{Field, RowSchema};
use crate::circuit::tables::TableDescriptor;
use crate::circuit::types::DbspScalarType;

use super::expression::DbspExpression;

#[derive(Clone, Debug)]
pub struct DbspPredicate {
    expression: DbspExpression,
}

impl DbspPredicate {
    pub fn try_new(expr: Expr, input_schema: Arc<RowSchema>) -> Result<Self> {
        let expression = DbspExpression::analyze(expr, input_schema)?;
        if expression.data_type() != &DbspScalarType::Bool {
            bail!(
                "predicate must return Bool, found {}",
                expression.data_type().name()
            );
        }
        Ok(Self { expression })
    }

    pub fn expression(&self) -> &DbspExpression {
        &self.expression
    }
}

#[derive(Clone, Debug)]
pub struct DbspSourceNode {
    pub table: Arc<TableDescriptor>,
}

impl DbspSourceNode {
    pub fn output_schema(&self) -> Arc<RowSchema> {
        self.table.schema().clone()
    }
}

#[derive(Clone, Debug)]
pub struct ProjectItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Clone, Debug)]
pub struct DbspProjectExpr {
    expression: DbspExpression,
    alias: String,
}

impl DbspProjectExpr {
    fn try_new(item: ProjectItem, input_schema: Arc<RowSchema>) -> Result<Self> {
        let expression = DbspExpression::analyze(item.expr, input_schema.clone())?;
        let alias = item
            .alias
            .unwrap_or_else(|| expression.expr().schema_name().to_string());
        Ok(Self { expression, alias })
    }

    fn field(&self) -> Field {
        Field::new(
            self.alias.clone(),
            self.expression.data_type().clone(),
            self.expression.nullable(),
        )
    }

    pub fn expression(&self) -> &DbspExpression {
        &self.expression
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }
}

#[derive(Clone, Debug)]
pub struct DbspProjectNode {
    input_schema: Arc<RowSchema>,
    expressions: Vec<DbspProjectExpr>,
    output_schema: Arc<RowSchema>,
}

impl DbspProjectNode {
    pub fn try_new(input_schema: Arc<RowSchema>, items: Vec<ProjectItem>) -> Result<Self> {
        if items.is_empty() {
            bail!("project requires at least one expression");
        }

        let mut expressions = Vec::with_capacity(items.len());
        let mut fields = Vec::with_capacity(items.len());
        let mut aliases = HashSet::new();

        for item in items {
            let expr = DbspProjectExpr::try_new(item, input_schema.clone())?;
            if !aliases.insert(expr.alias.clone()) {
                bail!("duplicate projection alias {}", expr.alias);
            }
            fields.push(expr.field());
            expressions.push(expr);
        }

        let output_schema = RowSchema::try_new(fields)?;
        Ok(Self {
            input_schema,
            expressions,
            output_schema,
        })
    }

    pub fn input_schema(&self) -> &Arc<RowSchema> {
        &self.input_schema
    }

    pub fn output_schema(&self) -> &Arc<RowSchema> {
        &self.output_schema
    }

    pub fn expressions(&self) -> &[DbspProjectExpr] {
        &self.expressions
    }
}

#[derive(Clone, Debug)]
pub struct DbspSelectNode {
    input_schema: Arc<RowSchema>,
    predicate: DbspPredicate,
}

impl DbspSelectNode {
    pub fn try_new(input_schema: Arc<RowSchema>, predicate: Expr) -> Result<Self> {
        let predicate = DbspPredicate::try_new(predicate, input_schema.clone())?;
        Ok(Self {
            input_schema,
            predicate,
        })
    }

    pub fn output_schema(&self) -> &Arc<RowSchema> {
        &self.input_schema
    }

    pub fn predicate(&self) -> &DbspPredicate {
        &self.predicate
    }
}

#[derive(Clone, Debug)]
pub enum DbspJoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
}

#[derive(Clone, Debug)]
pub struct DbspJoinKey {
    left: DbspExpression,
    right: DbspExpression,
}

impl DbspJoinKey {
    fn try_new(
        left_expr: Expr,
        left_schema: Arc<RowSchema>,
        right_expr: Expr,
        right_schema: Arc<RowSchema>,
    ) -> Result<Self> {
        let left = DbspExpression::analyze(left_expr, left_schema)?;
        let right = DbspExpression::analyze(right_expr, right_schema)?;
        if left.data_type() != right.data_type() {
            bail!(
                "join key type mismatch: left {} vs right {}",
                left.data_type().name(),
                right.data_type().name()
            );
        }
        if left.data_type() == &DbspScalarType::Bool {
            bail!("boolean join keys are not supported");
        }
        Ok(Self { left, right })
    }

    pub fn data_type(&self) -> &DbspScalarType {
        self.left.data_type()
    }

    pub fn left_expression(&self) -> &DbspExpression {
        &self.left
    }

    pub fn right_expression(&self) -> &DbspExpression {
        &self.right
    }
}

#[derive(Clone, Debug)]
pub struct DbspRangeJoinSpec {
    right_key: DbspExpression,
    left_lower: DbspExpression,
    left_upper: DbspExpression,
}

impl DbspRangeJoinSpec {
    fn try_new(
        right_key_expr: Expr,
        right_schema: Arc<RowSchema>,
        left_lower_expr: Expr,
        left_upper_expr: Expr,
        left_schema: Arc<RowSchema>,
    ) -> Result<Self> {
        let right_key = DbspExpression::analyze(right_key_expr, right_schema)?;
        let left_lower = DbspExpression::analyze(left_lower_expr, left_schema.clone())?;
        let left_upper = DbspExpression::analyze(left_upper_expr, left_schema)?;

        if left_lower.data_type() != right_key.data_type()
            || left_upper.data_type() != right_key.data_type()
        {
            bail!(
                "range join bound type mismatch: right key {}, lower {}, upper {}",
                right_key.data_type().name(),
                left_lower.data_type().name(),
                left_upper.data_type().name()
            );
        }

        if !matches!(
            right_key.data_type(),
            DbspScalarType::Int64 | DbspScalarType::TimestampMillis
        ) {
            bail!(
                "range joins currently require Int64 or TimestampMillis bounds, found {}",
                right_key.data_type().name()
            );
        }

        Ok(Self {
            right_key,
            left_lower,
            left_upper,
        })
    }

    pub fn right_key_expression(&self) -> &DbspExpression {
        &self.right_key
    }

    pub fn left_lower_expression(&self) -> &DbspExpression {
        &self.left_lower
    }

    pub fn left_upper_expression(&self) -> &DbspExpression {
        &self.left_upper
    }
}

#[derive(Clone, Debug)]
pub struct DbspAsofJoinSpec {
    left_timestamp: DbspExpression,
    right_timestamp: DbspExpression,
}

impl DbspAsofJoinSpec {
    fn try_new(
        left_timestamp_expr: Expr,
        left_schema: Arc<RowSchema>,
        right_timestamp_expr: Expr,
        right_schema: Arc<RowSchema>,
    ) -> Result<Self> {
        let left_timestamp = DbspExpression::analyze(left_timestamp_expr, left_schema)?;
        let right_timestamp = DbspExpression::analyze(right_timestamp_expr, right_schema)?;

        if left_timestamp.data_type() != right_timestamp.data_type() {
            bail!(
                "ASOF join timestamp type mismatch: left {}, right {}",
                left_timestamp.data_type().name(),
                right_timestamp.data_type().name()
            );
        }
        if !matches!(
            left_timestamp.data_type(),
            DbspScalarType::Int64 | DbspScalarType::TimestampMillis
        ) {
            bail!(
                "ASOF joins currently require Int64 or TimestampMillis timestamps, found {}",
                left_timestamp.data_type().name()
            );
        }

        Ok(Self {
            left_timestamp,
            right_timestamp,
        })
    }

    pub fn left_timestamp_expression(&self) -> &DbspExpression {
        &self.left_timestamp
    }

    pub fn right_timestamp_expression(&self) -> &DbspExpression {
        &self.right_timestamp
    }
}

#[derive(Clone, Debug)]
pub struct DbspJoinNode {
    pub join_type: DbspJoinType,
    pub left_schema: Arc<RowSchema>,
    pub right_schema: Arc<RowSchema>,
    pub output_schema: Arc<RowSchema>,
    pub keys: Vec<DbspJoinKey>,
    pub residual: Option<DbspExpression>,
    pub range: Option<DbspRangeJoinSpec>,
    pub asof: Option<DbspAsofJoinSpec>,
}

impl DbspJoinNode {
    pub fn try_new(
        join_type: DbspJoinType,
        left_schema: Arc<RowSchema>,
        right_schema: Arc<RowSchema>,
        key_pairs: Vec<(Expr, Expr)>,
        residual: Option<Expr>,
    ) -> Result<Self> {
        if key_pairs.is_empty() {
            bail!("join requires at least one key pair");
        }

        let mut keys = Vec::with_capacity(key_pairs.len());
        for (left, right) in key_pairs {
            keys.push(DbspJoinKey::try_new(
                left,
                left_schema.clone(),
                right,
                right_schema.clone(),
            )?);
        }

        let residual = if let Some(expr) = residual {
            let combined_schema = Self::matched_schema(left_schema.clone(), right_schema.clone())?;
            Some(DbspExpression::analyze(expr, combined_schema)?)
        } else {
            None
        };

        let output_schema =
            Self::combined_schema(left_schema.clone(), right_schema.clone(), &join_type)?;

        Ok(Self {
            join_type,
            left_schema,
            right_schema,
            output_schema,
            keys,
            residual,
            range: None,
            asof: None,
        })
    }

    pub fn try_new_range(
        left_schema: Arc<RowSchema>,
        right_schema: Arc<RowSchema>,
        right_key_expr: Expr,
        left_lower_expr: Expr,
        left_upper_expr: Expr,
        residual: Option<Expr>,
    ) -> Result<Self> {
        let range = DbspRangeJoinSpec::try_new(
            right_key_expr,
            right_schema.clone(),
            left_lower_expr,
            left_upper_expr,
            left_schema.clone(),
        )?;
        let residual = if let Some(expr) = residual {
            let combined_schema = Self::matched_schema(left_schema.clone(), right_schema.clone())?;
            Some(DbspExpression::analyze(expr, combined_schema)?)
        } else {
            None
        };
        let output_schema = Self::combined_schema(
            left_schema.clone(),
            right_schema.clone(),
            &DbspJoinType::Inner,
        )?;

        Ok(Self {
            join_type: DbspJoinType::Inner,
            left_schema,
            right_schema,
            output_schema,
            keys: Vec::new(),
            residual,
            range: Some(range),
            asof: None,
        })
    }

    pub fn try_new_asof(
        join_type: DbspJoinType,
        left_schema: Arc<RowSchema>,
        right_schema: Arc<RowSchema>,
        key_pairs: Vec<(Expr, Expr)>,
        left_timestamp_expr: Expr,
        right_timestamp_expr: Expr,
        residual: Option<Expr>,
    ) -> Result<Self> {
        if !matches!(join_type, DbspJoinType::Inner | DbspJoinType::LeftOuter) {
            bail!("ASOF joins currently support INNER or LEFT OUTER semantics");
        }

        let mut keys = Vec::with_capacity(key_pairs.len());
        for (left, right) in key_pairs {
            keys.push(DbspJoinKey::try_new(
                left,
                left_schema.clone(),
                right,
                right_schema.clone(),
            )?);
        }

        let asof = DbspAsofJoinSpec::try_new(
            left_timestamp_expr,
            left_schema.clone(),
            right_timestamp_expr,
            right_schema.clone(),
        )?;
        let residual = if let Some(expr) = residual {
            let combined_schema = Self::matched_schema(left_schema.clone(), right_schema.clone())?;
            Some(DbspExpression::analyze(expr, combined_schema)?)
        } else {
            None
        };
        let output_schema =
            Self::combined_schema(left_schema.clone(), right_schema.clone(), &join_type)?;

        Ok(Self {
            join_type,
            left_schema,
            right_schema,
            output_schema,
            keys,
            residual,
            range: None,
            asof: Some(asof),
        })
    }

    fn combined_schema(
        left: Arc<RowSchema>,
        right: Arc<RowSchema>,
        join_type: &DbspJoinType,
    ) -> Result<Arc<RowSchema>> {
        match join_type {
            DbspJoinType::LeftSemi | DbspJoinType::LeftAnti => return Ok(left),
            DbspJoinType::RightSemi | DbspJoinType::RightAnti => return Ok(right),
            DbspJoinType::Inner
            | DbspJoinType::LeftOuter
            | DbspJoinType::RightOuter
            | DbspJoinType::FullOuter => {}
        }

        let mut fields = Vec::with_capacity(left.len() + right.len());
        let mut existing = HashSet::new();

        for field in left.fields().iter() {
            existing.insert(field.name.clone());
            let nullable = match join_type {
                DbspJoinType::Inner | DbspJoinType::LeftOuter => field.nullable,
                DbspJoinType::RightOuter | DbspJoinType::FullOuter => true,
                DbspJoinType::LeftSemi
                | DbspJoinType::RightSemi
                | DbspJoinType::LeftAnti
                | DbspJoinType::RightAnti => unreachable!("semi/anti joins returned above"),
            };
            fields.push(Field::new(
                field.name.clone(),
                field.data_type.clone(),
                nullable,
            ));
        }

        for field in right.fields().iter() {
            let mut name = field.name.clone();
            let mut suffix = 1;
            while existing.contains(&name) {
                name = format!("{}_{}", field.name, suffix);
                suffix += 1;
            }
            existing.insert(name.clone());
            let nullable = match join_type {
                DbspJoinType::Inner => field.nullable,
                DbspJoinType::LeftOuter | DbspJoinType::FullOuter => true,
                DbspJoinType::RightOuter => field.nullable,
                DbspJoinType::LeftSemi
                | DbspJoinType::RightSemi
                | DbspJoinType::LeftAnti
                | DbspJoinType::RightAnti => unreachable!("semi/anti joins returned above"),
            };
            fields.push(Field::new(name, field.data_type.clone(), nullable));
        }

        RowSchema::try_new(fields)
    }

    fn matched_schema(left: Arc<RowSchema>, right: Arc<RowSchema>) -> Result<Arc<RowSchema>> {
        let mut fields = Vec::with_capacity(left.len() + right.len());
        let mut existing = HashSet::new();

        for field in left.fields().iter() {
            existing.insert(field.name.clone());
            fields.push(field.clone());
        }

        for field in right.fields().iter() {
            let mut field = field.clone();
            let base_name = field.name.clone();
            let mut suffix = 1;
            while existing.contains(&field.name) {
                field.name = format!("{base_name}_{suffix}");
                suffix += 1;
            }
            existing.insert(field.name.clone());
            fields.push(field);
        }

        RowSchema::try_new(fields)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DbspAggregateFunction {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

impl DbspAggregateFunction {
    pub fn result_type(&self, input_type: &DbspScalarType) -> Result<DbspScalarType> {
        match self {
            DbspAggregateFunction::Count => Ok(DbspScalarType::Int64),
            DbspAggregateFunction::Sum => {
                if matches!(
                    input_type,
                    DbspScalarType::Int64
                        | DbspScalarType::TimestampMillis
                        | DbspScalarType::Decimal128 { .. }
                ) {
                    Ok(input_type.clone())
                } else {
                    bail!(
                        "aggregate {:?} not supported for {}",
                        self,
                        input_type.name()
                    );
                }
            }
            DbspAggregateFunction::Min | DbspAggregateFunction::Max => {
                if matches!(
                    input_type,
                    DbspScalarType::Int64
                        | DbspScalarType::TimestampMillis
                        | DbspScalarType::Utf8
                        | DbspScalarType::DateDays
                        | DbspScalarType::Decimal128 { .. }
                ) {
                    Ok(input_type.clone())
                } else {
                    bail!(
                        "aggregate {:?} not supported for {}",
                        self,
                        input_type.name()
                    );
                }
            }
            DbspAggregateFunction::Avg => {
                if input_type == &DbspScalarType::Int64 {
                    Ok(DbspScalarType::Int64)
                } else {
                    bail!("AVG only supported for Int64 inputs")
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct DbspAggregateExpr {
    function: DbspAggregateFunction,
    expression: Option<DbspExpression>,
    filter: Option<DbspExpression>,
    distinct: bool,
    alias: String,
    output_type: DbspScalarType,
}

impl DbspAggregateExpr {
    fn try_new(
        function: DbspAggregateFunction,
        expr: Option<Expr>,
        filter: Option<Expr>,
        distinct: bool,
        alias: Option<String>,
        input_schema: Arc<RowSchema>,
    ) -> Result<Self> {
        let (expression, input_type) = if let Some(expr) = expr {
            let typed = DbspExpression::analyze(expr, input_schema.clone())?;
            (Some(typed.clone()), Some(typed.data_type().clone()))
        } else {
            (None, None)
        };
        let filter = if let Some(filter) = filter {
            let typed = DbspExpression::analyze(filter, input_schema)?;
            if typed.data_type() != &DbspScalarType::Bool {
                bail!(
                    "aggregate FILTER expression must return Bool, found {}",
                    typed.data_type().name()
                );
            }
            Some(typed)
        } else {
            None
        };

        let resolved_input_type = match (&function, input_type) {
            (DbspAggregateFunction::Count, _) => DbspScalarType::Int64,
            (_, Some(ty)) => ty,
            _ => bail!("aggregate {:?} requires an input expression", function),
        };
        if distinct {
            if function != DbspAggregateFunction::Count {
                bail!("DISTINCT is only supported for COUNT aggregates");
            }
            if expression.is_none() {
                bail!("COUNT(DISTINCT ...) requires an argument expression");
            }
        }

        let output_type = function.result_type(&resolved_input_type)?;
        let alias = alias.unwrap_or_else(|| match &function {
            DbspAggregateFunction::Count => "count".to_string(),
            DbspAggregateFunction::Sum => "sum".to_string(),
            DbspAggregateFunction::Min => "min".to_string(),
            DbspAggregateFunction::Max => "max".to_string(),
            DbspAggregateFunction::Avg => "avg".to_string(),
        });

        Ok(Self {
            function,
            expression,
            filter,
            distinct,
            alias,
            output_type,
        })
    }

    fn field(&self) -> Field {
        Field::new(self.alias.clone(), self.output_type.clone(), true)
    }

    pub fn function(&self) -> &DbspAggregateFunction {
        &self.function
    }

    pub fn expression(&self) -> Option<&DbspExpression> {
        self.expression.as_ref()
    }

    pub fn filter(&self) -> Option<&DbspExpression> {
        self.filter.as_ref()
    }

    pub fn distinct(&self) -> bool {
        self.distinct
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }

    pub fn output_type(&self) -> &DbspScalarType {
        &self.output_type
    }
}

#[derive(Clone, Debug)]
pub struct GroupKeyExpr {
    expression: DbspExpression,
    alias: String,
}

impl GroupKeyExpr {
    fn try_new(expr: Expr, input_schema: Arc<RowSchema>, alias: Option<String>) -> Result<Self> {
        let expression = DbspExpression::analyze(expr, input_schema)?;
        let alias = alias.unwrap_or_else(|| expression.expr().schema_name().to_string());
        if expression.data_type() == &DbspScalarType::Bool {
            bail!("boolean values are not supported as group keys");
        }
        Ok(Self { expression, alias })
    }

    fn field(&self) -> Field {
        Field::new(
            self.alias.clone(),
            self.expression.data_type().clone(),
            self.expression.nullable(),
        )
    }

    pub fn expression(&self) -> &DbspExpression {
        &self.expression
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }
}

type AggregateSpec = (
    DbspAggregateFunction,
    Option<Expr>,
    Option<Expr>,
    bool,
    Option<String>,
);

#[derive(Clone, Debug)]
pub struct DbspAggregateNode {
    input_schema: Arc<RowSchema>,
    group_keys: Vec<GroupKeyExpr>,
    aggregates: Vec<DbspAggregateExpr>,
    output_schema: Arc<RowSchema>,
}

impl DbspAggregateNode {
    pub fn try_new(
        input_schema: Arc<RowSchema>,
        group_keys: Vec<(Expr, Option<String>)>,
        aggregates: Vec<AggregateSpec>,
    ) -> Result<Self> {
        let mut group_exprs = Vec::with_capacity(group_keys.len());
        let mut fields = Vec::new();
        for (expr, alias) in group_keys {
            let key = GroupKeyExpr::try_new(expr, input_schema.clone(), alias)?;
            fields.push(key.field());
            group_exprs.push(key);
        }

        let mut agg_exprs = Vec::with_capacity(aggregates.len());
        for (func, expr, filter, distinct, alias) in aggregates {
            let agg = DbspAggregateExpr::try_new(
                func,
                expr,
                filter,
                distinct,
                alias,
                input_schema.clone(),
            )?;
            fields.push(agg.field());
            agg_exprs.push(agg);
        }

        let output_schema = RowSchema::try_new(fields)?;
        Ok(Self {
            input_schema,
            group_keys: group_exprs,
            aggregates: agg_exprs,
            output_schema,
        })
    }

    pub fn output_schema(&self) -> &Arc<RowSchema> {
        &self.output_schema
    }

    pub fn input_schema(&self) -> &Arc<RowSchema> {
        &self.input_schema
    }

    pub fn group_keys(&self) -> &[GroupKeyExpr] {
        &self.group_keys
    }

    pub fn aggregates(&self) -> &[DbspAggregateExpr] {
        &self.aggregates
    }
}

#[derive(Clone, Debug)]
pub enum DbspWindowPolicy {
    Tumbling { size_ms: i64 },
    Hopping { size_ms: i64, slide_ms: i64 },
    Session { gap_ms: i64 },
}

impl DbspWindowPolicy {
    pub fn validate(&self) -> Result<()> {
        match self {
            DbspWindowPolicy::Tumbling { size_ms } => {
                if *size_ms <= 0 {
                    bail!("tumbling window size must be positive")
                }
            }
            DbspWindowPolicy::Hopping { size_ms, slide_ms } => {
                if *size_ms <= 0 || *slide_ms <= 0 {
                    bail!("window size and slide must be positive")
                }
                if size_ms % slide_ms != 0 {
                    bail!("window size must be a multiple of slide")
                }
            }
            DbspWindowPolicy::Session { gap_ms } => {
                if *gap_ms <= 0 {
                    bail!("session window gap must be positive")
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct DbspWindowSpec {
    pub policy: DbspWindowPolicy,
    pub time_expression: DbspExpression,
    pub allowed_lateness_ms: i64,
}

impl DbspWindowSpec {
    pub fn try_new(
        policy: DbspWindowPolicy,
        time_expr: Expr,
        input_schema: Arc<RowSchema>,
        allowed_lateness_ms: i64,
    ) -> Result<Self> {
        policy.validate()?;
        if allowed_lateness_ms < 0 {
            bail!("allowed lateness must be non-negative");
        }
        let expr = DbspExpression::analyze(time_expr, input_schema)?;
        if expr.data_type() != &DbspScalarType::TimestampMillis {
            bail!("window time expression must be TimestampMillis");
        }
        Ok(Self {
            policy,
            time_expression: expr,
            allowed_lateness_ms,
        })
    }
}

#[derive(Clone, Debug)]
pub struct DbspWindowAggregateNode {
    pub aggregate: DbspAggregateNode,
    pub window: DbspWindowSpec,
}

#[derive(Clone, Debug)]
pub struct OrderExpr {
    expression: DbspExpression,
    ascending: bool,
    nulls_first: bool,
}

impl OrderExpr {
    pub fn try_new(
        expr: Expr,
        input_schema: Arc<RowSchema>,
        ascending: bool,
        nulls_first: bool,
    ) -> Result<Self> {
        let expression = DbspExpression::analyze(expr, input_schema)?;
        if expression.data_type() == &DbspScalarType::Bool {
            bail!("boolean ordering is not supported");
        }
        Ok(Self {
            expression,
            ascending,
            nulls_first,
        })
    }

    pub fn expression(&self) -> &DbspExpression {
        &self.expression
    }

    pub fn ascending(&self) -> bool {
        self.ascending
    }

    pub fn nulls_first(&self) -> bool {
        self.nulls_first
    }
}

#[derive(Clone, Debug)]
pub struct DbspTopNNode {
    input_schema: Arc<RowSchema>,
    partition_by: Vec<DbspExpression>,
    order_by: Vec<OrderExpr>,
    limit: usize,
    offset: usize,
}

impl DbspTopNNode {
    pub fn try_new(
        input_schema: Arc<RowSchema>,
        partition_by: Vec<Expr>,
        order_by: Vec<OrderExpr>,
        limit: usize,
        offset: usize,
    ) -> Result<Self> {
        if limit == 0 {
            bail!("limit must be greater than zero");
        }
        if order_by.is_empty() {
            bail!("TopN requires at least one ORDER BY expression");
        }
        let mut partition_exprs = Vec::with_capacity(partition_by.len());
        for expr in partition_by {
            let analyzed = DbspExpression::analyze(expr, input_schema.clone())?;
            if analyzed.data_type() == &DbspScalarType::Bool {
                bail!("boolean partition keys are not supported");
            }
            partition_exprs.push(analyzed);
        }
        Ok(Self {
            input_schema,
            partition_by: partition_exprs,
            order_by,
            limit,
            offset,
        })
    }

    pub fn output_schema(&self) -> &Arc<RowSchema> {
        &self.input_schema
    }

    pub fn order_by(&self) -> &[OrderExpr] {
        &self.order_by
    }

    pub fn partition_by(&self) -> &[DbspExpression] {
        &self.partition_by
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn offset(&self) -> usize {
        self.offset
    }
}

#[derive(Clone, Debug)]
pub struct DbspUnionNode {
    output_schema: Arc<RowSchema>,
}

#[path = "nodes/set_sink.rs"]
mod set_sink;

pub use set_sink::*;
