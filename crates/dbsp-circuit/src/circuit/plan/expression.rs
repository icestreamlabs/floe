use std::sync::Arc;

use anyhow::{Result, bail};
use datafusion::logical_expr::{Expr, ExprSchemable, Operator};
use datafusion::scalar::ScalarValue as DfScalarValue;
use datafusion_common::DFSchema;

use crate::circuit::schema::RowSchema;
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
