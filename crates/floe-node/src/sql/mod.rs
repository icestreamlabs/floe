use anyhow::{anyhow, Result};
use floe_core::catalog::{ColumnDefinition, TableDefinition};
use sqlparser::ast::{
    ColumnOption, CreateTable, DataType, Expr, Ident, IndexColumn, Insert, ObjectName,
    ObjectNamePart, TableConstraint, TableObject, UnaryOperator, Value as SqlValue, ValueWithSpan,
};

pub fn build_table_definition(create: &CreateTable) -> Result<TableDefinition> {
    let table_name = object_name_to_string(&create.name)?;
    let mut defs = Vec::with_capacity(create.columns.len());
    for col in &create.columns {
        ensure_integer_column(&col.data_type, &col.name.value)?;
        let is_primary = col.options.iter().any(|opt| {
            matches!(
                opt.option,
                ColumnOption::Unique {
                    is_primary: true,
                    ..
                }
            )
        });
        defs.push((col.name.value.clone(), is_primary));
    }

    for constraint in &create.constraints {
        if let TableConstraint::PrimaryKey { columns, .. } = constraint {
            for column in columns {
                let column_name = index_column_name(column)?;
                mark_primary_key(&mut defs, &column_name)?;
            }
        }
    }

    let columns = defs
        .into_iter()
        .map(|(name, primary)| ColumnDefinition::new(name, primary))
        .collect::<Vec<_>>();

    TableDefinition::new(table_name, columns)
}

pub fn table_name_from_object(table: &TableObject) -> Result<String> {
    match table {
        TableObject::TableName(name) => object_name_to_string(name),
        TableObject::TableFunction(_) => {
            Err(anyhow!("INSERT INTO TABLE FUNCTION is not supported"))
        }
    }
}

pub fn extract_insert_rows(definition: &TableDefinition, insert: &Insert) -> Result<Vec<Vec<i64>>> {
    use sqlparser::ast::{SetExpr, Values};

    let source = insert
        .source
        .as_ref()
        .ok_or_else(|| anyhow!("INSERT requires a VALUES clause"))?;
    let values = match source.body.as_ref() {
        SetExpr::Values(Values { rows, .. }) => rows,
        _ => return Err(anyhow!("only VALUES inserts are supported")),
    };

    let column_order = if insert.columns.is_empty() {
        definition
            .columns()
            .iter()
            .map(|column| column.name().to_string())
            .collect::<Vec<_>>()
    } else {
        insert
            .columns
            .iter()
            .map(|ident| ident.value.clone())
            .collect::<Vec<_>>()
    };

    if column_order.len() != definition.columns().len() {
        return Err(anyhow!(
            "insert specifies {} columns but table has {}",
            column_order.len(),
            definition.columns().len()
        ));
    }

    let mut rows_out = Vec::with_capacity(values.len());
    for row in values {
        if row.len() != column_order.len() {
            return Err(anyhow!(
                "expected {} values in row, found {}",
                column_order.len(),
                row.len()
            ));
        }

        let mut materialized = vec![0_i64; definition.columns().len()];
        for (expr, column_name) in row.iter().zip(column_order.iter()) {
            let idx = definition
                .column_index(column_name)
                .ok_or_else(|| anyhow!("unknown column {column_name} in INSERT"))?;
            let value = parse_int_literal(expr)?;
            materialized[idx] = value;
        }
        definition.validate_row(&materialized)?;
        rows_out.push(materialized);
    }
    Ok(rows_out)
}

fn ensure_integer_column(data_type: &DataType, column: &str) -> Result<()> {
    let is_integer = matches!(
        data_type,
        DataType::Int(_)
            | DataType::Integer(_)
            | DataType::BigInt(_)
            | DataType::SmallInt(_)
            | DataType::Int2(_)
            | DataType::Int4(_)
            | DataType::Int8(_)
            | DataType::Unsigned
            | DataType::UnsignedInteger
            | DataType::Int2Unsigned(_)
            | DataType::Int4Unsigned(_)
            | DataType::Int8Unsigned(_)
            | DataType::IntegerUnsigned(_)
            | DataType::BigIntUnsigned(_)
            | DataType::SmallIntUnsigned(_)
            | DataType::IntUnsigned(_)
    );
    if is_integer {
        Ok(())
    } else {
        Err(anyhow!(
            "column {column} must be declared as an integer type"
        ))
    }
}

fn mark_primary_key(defs: &mut [(String, bool)], name: &str) -> Result<()> {
    let Some((_, flag)) = defs.iter_mut().find(|(col, _)| col == name) else {
        return Err(anyhow!("primary key references unknown column {name}"));
    };
    *flag = true;
    Ok(())
}

fn index_column_name(column: &IndexColumn) -> Result<String> {
    match &column.column.expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|ident| ident.value.clone())
            .ok_or_else(|| anyhow!("empty compound identifier")),
        other => Err(anyhow!(
            "unsupported expression in PRIMARY KEY declaration: {other}"
        )),
    }
}

fn parse_int_literal(expr: &Expr) -> Result<i64> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: SqlValue::Number(num, _),
            ..
        }) => {
            let num_str = num.to_string();
            num_str
                .parse::<i64>()
                .map_err(|err| anyhow!("failed to parse integer literal {num_str}: {err}"))
        }
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => {
            let value = parse_int_literal(expr)?;
            Ok(-value)
        }
        other => Err(anyhow!("unsupported expression in VALUES clause: {other}")),
    }
}

fn object_name_to_string(name: &ObjectName) -> Result<String> {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|Ident { value, .. }| value.clone())
        .ok_or_else(|| anyhow!("invalid identifier"))
}
