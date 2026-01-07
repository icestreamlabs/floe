use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Result, bail};
use datafusion::logical_expr::{Expr, ExprSchemable, Operator};
use datafusion::scalar::ScalarValue as DfScalarValue;
use datafusion_common::DFSchema;

use crate::circuit::schema::{Field, RowSchema};
use crate::circuit::tables::TableDescriptor;
use crate::circuit::types::DbspScalarType;

#[derive(Clone, Debug)]
pub struct DbspExpression {
    expr: Expr,
    data_type: DbspScalarType,
    nullable: bool,
}

impl DbspExpression {
    pub fn analyze(expr: Expr, input_schema: Arc<RowSchema>) -> Result<Self> {
        let df_schema = input_schema.to_dfschema()?;
        let data_type = DbspScalarType::try_from_arrow(&expr.get_type(df_schema.as_ref())?)?;
        let nullable = expr.nullable(df_schema.as_ref())?;
        let expression = Self {
            expr,
            data_type,
            nullable,
        };

        Self::validate_supported(&expression.expr, df_schema.as_ref())?;

        Ok(expression)
    }

    pub fn expr(&self) -> &Expr {
        &self.expr
    }

    #[allow(deprecated)]
    fn validate_supported(expr: &Expr, df_schema: &DFSchema) -> Result<()> {
        match expr {
            Expr::Alias(alias) => Self::validate_supported(alias.expr.as_ref(), df_schema),
            Expr::Column(_) | Expr::ScalarVariable(_, _) | Expr::OuterReferenceColumn(_, _) => {
                Ok(())
            }
            Expr::Literal(_, _) => Ok(()),
            Expr::BinaryExpr(binary) => {
                Self::validate_supported(binary.left.as_ref(), df_schema)?;
                Self::validate_supported(binary.right.as_ref(), df_schema)?;

                let left_type = Self::infer_type(binary.left.as_ref(), df_schema)?;
                let right_type = Self::infer_type(binary.right.as_ref(), df_schema)?;

                match binary.op {
                    Operator::Plus
                    | Operator::Minus
                    | Operator::Multiply
                    | Operator::Divide
                    | Operator::Modulo => {
                        Self::ensure_int64(
                            &left_type,
                            "arithmetic expressions require Int64 operands",
                        )?;
                        Self::ensure_int64(
                            &right_type,
                            "arithmetic expressions require Int64 operands",
                        )?
                    }
                    Operator::Eq
                    | Operator::NotEq
                    | Operator::IsDistinctFrom
                    | Operator::IsNotDistinctFrom
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq => {
                        Self::ensure_comparable(&left_type, &right_type)?;
                    }
                    Operator::And | Operator::Or => {
                        Self::ensure_bool(
                            &left_type,
                            "logical operator requires boolean operands",
                        )?;
                        Self::ensure_bool(
                            &right_type,
                            "logical operator requires boolean operands",
                        )?
                    }
                    Operator::StringConcat => {
                        Self::ensure_string(
                            &left_type,
                            "string concatenation requires string operands",
                        )?;
                        Self::ensure_string(
                            &right_type,
                            "string concatenation requires string operands",
                        )?
                    }
                    Operator::LikeMatch
                    | Operator::ILikeMatch
                    | Operator::NotLikeMatch
                    | Operator::NotILikeMatch => {
                        Self::ensure_string(&left_type, "LIKE requires text operands")?;
                        Self::ensure_string(&right_type, "LIKE requires text operands")?;
                        Self::validate_like_operand(binary.right.as_ref())?;
                    }
                    _ => bail!("operator {:?} is not supported in DBSP circuits", binary.op),
                }

                Ok(())
            }
            Expr::Like(like) => {
                Self::validate_supported(like.expr.as_ref(), df_schema)?;
                Self::validate_supported(like.pattern.as_ref(), df_schema)?;

                if like.escape_char.is_some() {
                    bail!("LIKE escape characters are not supported yet");
                }

                let left_type = Self::infer_type(like.expr.as_ref(), df_schema)?;
                Self::ensure_string(&left_type, "LIKE requires text operands")?;

                Self::validate_like_operand(like.pattern.as_ref())?;
                Ok(())
            }
            Expr::SimilarTo(_) => bail!("SIMILAR TO expressions are not supported yet"),
            Expr::Not(inner) => {
                Self::validate_supported(inner.as_ref(), df_schema)?;
                let ty = Self::infer_type(inner.as_ref(), df_schema)?;
                Self::ensure_bool(&ty, "NOT requires a boolean expression")
            }
            Expr::IsNull(inner)
            | Expr::IsNotNull(inner)
            | Expr::IsTrue(inner)
            | Expr::IsFalse(inner)
            | Expr::IsUnknown(inner)
            | Expr::IsNotTrue(inner)
            | Expr::IsNotFalse(inner)
            | Expr::IsNotUnknown(inner) => Self::validate_supported(inner.as_ref(), df_schema),
            Expr::Negative(inner) => {
                Self::validate_supported(inner.as_ref(), df_schema)?;
                let ty = Self::infer_type(inner.as_ref(), df_schema)?;
                Self::ensure_int64(&ty, "numeric negation requires Int64 operands")
            }
            Expr::Case(case) => {
                if let Some(expr) = &case.expr {
                    Self::validate_supported(expr.as_ref(), df_schema)?;
                }
                for (when, then) in &case.when_then_expr {
                    Self::validate_supported(when.as_ref(), df_schema)?;
                    let when_ty = Self::infer_type(when.as_ref(), df_schema)?;
                    Self::ensure_bool(&when_ty, "WHEN clauses must return boolean")?;
                    Self::validate_supported(then.as_ref(), df_schema)?;
                }
                if let Some(else_expr) = &case.else_expr {
                    Self::validate_supported(else_expr.as_ref(), df_schema)?;
                }
                Ok(())
            }
            Expr::Cast(cast) => {
                Self::validate_supported(cast.expr.as_ref(), df_schema)?;
                let from_type = Self::infer_type(cast.expr.as_ref(), df_schema)?;
                let to_type = DbspScalarType::try_from_arrow(&cast.data_type)?;
                Self::validate_cast(&from_type, &to_type)
            }
            Expr::TryCast(_) => bail!("TRY_CAST is not supported"),
            Expr::Between(_) => bail!("BETWEEN expressions are not supported yet"),
            Expr::ScalarFunction(_) => bail!("scalar functions are not supported yet"),
            Expr::AggregateFunction(_) => {
                bail!("aggregate functions are not expected in this context")
            }
            Expr::WindowFunction(_) => bail!("window functions are not supported in expressions"),
            Expr::InList(_) => bail!("IN lists are not supported"),
            Expr::Exists(_) | Expr::InSubquery(_) | Expr::ScalarSubquery(_) => {
                bail!("subqueries are not supported in DBSP expressions")
            }
            Expr::GroupingSet(_) | Expr::Wildcard { .. } => {
                bail!("wildcards and grouping sets must be expanded before circuit planning")
            }
            Expr::Placeholder(_) => Ok(()),
            Expr::Unnest(_) => bail!("UNNEST is not supported in DBSP expressions"),
        }
    }

    fn infer_type(expr: &Expr, df_schema: &DFSchema) -> Result<DbspScalarType> {
        let arrow_type = expr.get_type(df_schema)?;
        DbspScalarType::try_from_arrow(&arrow_type)
    }

    fn ensure_int64(ty: &DbspScalarType, msg: &str) -> Result<()> {
        if ty == &DbspScalarType::Int64 {
            Ok(())
        } else {
            bail!("{msg}: found {}", ty.name())
        }
    }

    fn ensure_bool(ty: &DbspScalarType, msg: &str) -> Result<()> {
        if ty == &DbspScalarType::Bool {
            Ok(())
        } else {
            bail!("{msg}: found {}", ty.name())
        }
    }

    fn ensure_string(ty: &DbspScalarType, msg: &str) -> Result<()> {
        if ty == &DbspScalarType::Utf8 {
            Ok(())
        } else {
            bail!("{msg}: found {}", ty.name())
        }
    }

    fn ensure_comparable(left: &DbspScalarType, right: &DbspScalarType) -> Result<()> {
        if left == right {
            match left {
                DbspScalarType::Int64
                | DbspScalarType::Utf8
                | DbspScalarType::TimestampMillis
                | DbspScalarType::Bool => Ok(()),
            }
        } else {
            bail!(
                "comparison operands must have matching types, found {} and {}",
                left.name(),
                right.name()
            )
        }
    }

    fn validate_cast(from: &DbspScalarType, to: &DbspScalarType) -> Result<()> {
        if from == to {
            return Ok(());
        }

        match (from, to) {
            (DbspScalarType::Int64, DbspScalarType::TimestampMillis)
            | (DbspScalarType::TimestampMillis, DbspScalarType::Int64) => Ok(()),
            _ => bail!(
                "casts are restricted to Int64 ↔ TimestampMillis conversions (attempted {} -> {})",
                from.name(),
                to.name()
            ),
        }
    }

    fn validate_like_operand(pattern_expr: &Expr) -> Result<()> {
        match pattern_expr {
            Expr::Literal(DfScalarValue::Utf8(Some(pattern)), _) => {
                Self::validate_like_pattern(pattern)
            }
            Expr::Literal(_, _) => bail!("LIKE patterns must be UTF-8 strings"),
            _ => bail!("LIKE pattern must be a literal string"),
        }
    }

    fn validate_like_pattern(pattern: &str) -> Result<()> {
        if pattern.contains('_') {
            bail!("LIKE patterns may not use '_' wildcards yet");
        }

        let percent_count = pattern.matches('%').count();
        if percent_count == 0 {
            return Ok(());
        }
        if percent_count == 1 && (pattern.starts_with('%') || pattern.ends_with('%')) {
            return Ok(());
        }
        bail!(
            "LIKE only supports prefix or suffix wildcards (pattern '{}')",
            pattern
        )
    }

    pub fn data_type(&self) -> &DbspScalarType {
        &self.data_type
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }
}

#[derive(Clone, Debug)]
pub struct DbspPredicate {
    expression: DbspExpression,
}

impl DbspPredicate {
    pub fn try_new(expr: Expr, input_schema: Arc<RowSchema>) -> Result<Self> {
        let expression = DbspExpression::analyze(expr, input_schema)?;
        if expression.data_type != DbspScalarType::Bool {
            bail!(
                "predicate must return Bool, found {}",
                expression.data_type.name()
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
    pub table: &'static TableDescriptor,
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
            self.expression.data_type.clone(),
            self.expression.nullable,
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
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
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
        if left.data_type != right.data_type {
            bail!(
                "join key type mismatch: left {} vs right {}",
                left.data_type.name(),
                right.data_type.name()
            );
        }
        if left.data_type == DbspScalarType::Bool {
            bail!("boolean join keys are not supported");
        }
        Ok(Self { left, right })
    }

    pub fn data_type(&self) -> &DbspScalarType {
        &self.left.data_type
    }

    pub fn left_expression(&self) -> &DbspExpression {
        &self.left
    }

    pub fn right_expression(&self) -> &DbspExpression {
        &self.right
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
            let combined_schema = Self::combined_schema(left_schema.clone(), right_schema.clone())?;
            Some(DbspExpression::analyze(expr, combined_schema)?)
        } else {
            None
        };

        let output_schema = Self::combined_schema(left_schema.clone(), right_schema.clone())?;

        Ok(Self {
            join_type,
            left_schema,
            right_schema,
            output_schema,
            keys,
            residual,
        })
    }

    fn combined_schema(left: Arc<RowSchema>, right: Arc<RowSchema>) -> Result<Arc<RowSchema>> {
        let mut fields = Vec::with_capacity(left.len() + right.len());
        let mut existing = HashSet::new();

        for field in left.fields().iter() {
            existing.insert(field.name.clone());
            fields.push(field.clone());
        }

        for field in right.fields().iter() {
            let mut name = field.name.clone();
            let mut suffix = 1;
            while existing.contains(&name) {
                name = format!("{}_{}", field.name, suffix);
                suffix += 1;
            }
            existing.insert(name.clone());
            fields.push(Field::new(name, field.data_type.clone(), field.nullable));
        }

        RowSchema::try_new(fields)
    }
}

#[derive(Clone, Debug)]
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
            DbspAggregateFunction::Sum
            | DbspAggregateFunction::Min
            | DbspAggregateFunction::Max => {
                if matches!(
                    input_type,
                    DbspScalarType::Int64 | DbspScalarType::TimestampMillis
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
#[allow(dead_code)]
pub struct DbspAggregateExpr {
    function: DbspAggregateFunction,
    expression: Option<DbspExpression>,
    alias: String,
    output_type: DbspScalarType,
}

impl DbspAggregateExpr {
    fn try_new(
        function: DbspAggregateFunction,
        expr: Option<Expr>,
        alias: Option<String>,
        input_schema: Arc<RowSchema>,
    ) -> Result<Self> {
        let (expression, input_type) = if let Some(expr) = expr {
            let typed = DbspExpression::analyze(expr, input_schema)?;
            (Some(typed.clone()), Some(typed.data_type.clone()))
        } else {
            (None, None)
        };

        let resolved_input_type = match (&function, input_type) {
            (DbspAggregateFunction::Count, _) => DbspScalarType::Int64,
            (_, Some(ty)) => ty,
            _ => bail!("aggregate {:?} requires an input expression", function),
        };

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
            alias,
            output_type,
        })
    }

    fn field(&self) -> Field {
        Field::new(self.alias.clone(), self.output_type.clone(), true)
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
        if expression.data_type == DbspScalarType::Bool {
            bail!("boolean values are not supported as group keys");
        }
        Ok(Self { expression, alias })
    }

    fn field(&self) -> Field {
        Field::new(
            self.alias.clone(),
            self.expression.data_type.clone(),
            self.expression.nullable,
        )
    }
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
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
        aggregates: Vec<(DbspAggregateFunction, Option<Expr>, Option<String>)>,
    ) -> Result<Self> {
        let mut group_exprs = Vec::with_capacity(group_keys.len());
        let mut fields = Vec::new();
        for (expr, alias) in group_keys {
            let key = GroupKeyExpr::try_new(expr, input_schema.clone(), alias)?;
            fields.push(key.field());
            group_exprs.push(key);
        }

        let mut agg_exprs = Vec::with_capacity(aggregates.len());
        for (func, expr, alias) in aggregates {
            let agg = DbspAggregateExpr::try_new(func, expr, alias, input_schema.clone())?;
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
}

#[derive(Clone, Debug)]
pub enum DbspWindowPolicy {
    Tumbling { size_ms: i64 },
    Hopping { size_ms: i64, slide_ms: i64 },
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
        if expr.data_type != DbspScalarType::TimestampMillis {
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
#[allow(dead_code)]
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
        if expression.data_type == DbspScalarType::Bool {
            bail!("boolean ordering is not supported");
        }
        Ok(Self {
            expression,
            ascending,
            nulls_first,
        })
    }
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct DbspTopNNode {
    input_schema: Arc<RowSchema>,
    order_by: Vec<OrderExpr>,
    limit: usize,
    offset: usize,
}

impl DbspTopNNode {
    pub fn try_new(
        input_schema: Arc<RowSchema>,
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
        Ok(Self {
            input_schema,
            order_by,
            limit,
            offset,
        })
    }

    pub fn output_schema(&self) -> &Arc<RowSchema> {
        &self.input_schema
    }
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct DbspUnionNode {
    input_schemas: Vec<Arc<RowSchema>>,
    output_schema: Arc<RowSchema>,
}

impl DbspUnionNode {
    pub fn try_new(input_schemas: Vec<Arc<RowSchema>>) -> Result<Self> {
        if input_schemas.is_empty() {
            bail!("union requires at least one input");
        }
        let first = input_schemas[0].clone();
        for schema in &input_schemas[1..] {
            if schema.fields() != first.fields() {
                bail!("all union inputs must share the same schema");
            }
        }
        Ok(Self {
            input_schemas,
            output_schema: first,
        })
    }

    pub fn output_schema(&self) -> &Arc<RowSchema> {
        &self.output_schema
    }
}

#[derive(Clone, Debug)]
pub struct DbspSinkNode {
    pub name: String,
    input_schema: Arc<RowSchema>,
}

impl DbspSinkNode {
    pub fn new(name: impl Into<String>, input_schema: Arc<RowSchema>) -> Self {
        Self {
            name: name.into(),
            input_schema,
        }
    }

    pub fn input_schema(&self) -> &Arc<RowSchema> {
        &self.input_schema
    }
}

#[derive(Clone, Debug)]
pub enum DbspNodeKind {
    Source(DbspSourceNode),
    Select(DbspSelectNode),
    Project(DbspProjectNode),
    Join(DbspJoinNode),
    Aggregate(DbspAggregateNode),
    WindowAggregate(DbspWindowAggregateNode),
    TopN(DbspTopNNode),
    Union(DbspUnionNode),
    Passthrough,
    Sink(DbspSinkNode),
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, TimeUnit};
    use datafusion::logical_expr::expr_fn::cast;
    use datafusion::logical_expr::{Like, col, lit};

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
    fn cast_matrix_is_restricted_to_int_timestamp() {
        let schema = base_schema();
        let to_timestamp = cast(
            col("value"),
            DataType::Timestamp(TimeUnit::Millisecond, None),
        );
        assert!(DbspExpression::analyze(to_timestamp, schema.clone()).is_ok());

        let err = DbspExpression::analyze(cast(col("text"), DataType::Int64), schema).unwrap_err();
        assert!(
            err.to_string()
                .contains("casts are restricted to Int64 ↔ TimestampMillis")
        );
    }
}
