use anyhow::{Result, bail};

use crate::catalog::ColumnType;

pub fn postgres_column_type(
    udt_name: &str,
    data_type: &str,
    numeric_precision: Option<i32>,
    numeric_scale: Option<i32>,
) -> Result<ColumnType> {
    let udt_name = normalize_postgres_type(udt_name);
    let data_type = normalize_postgres_type(data_type);
    match udt_name.as_str() {
        "int8" | "int4" | "int2" | "bigint" | "integer" | "smallint" => Ok(ColumnType::Int64),
        "bool" | "boolean" => Ok(ColumnType::Bool),
        "text" | "varchar" | "character varying" | "bpchar" | "character" | "name" | "uuid"
        | "json" | "jsonb" | "bytea" => Ok(ColumnType::Utf8),
        "timestamp"
        | "timestamptz"
        | "timestamp without time zone"
        | "timestamp with time zone" => Ok(ColumnType::TimestampMillis),
        "date" => Ok(ColumnType::DateDays),
        "numeric" | "decimal" => {
            decimal128_type_from_precision_scale(numeric_precision, numeric_scale)
                .unwrap_or(Ok(ColumnType::Numeric))
        }
        _ if matches!(
            data_type.as_str(),
            "timestamp without time zone" | "timestamp with time zone"
        ) =>
        {
            Ok(ColumnType::TimestampMillis)
        }
        _ => bail!(
            "unsupported Postgres column type '{}' ({})",
            udt_name,
            data_type
        ),
    }
}

pub fn postgres_type_compatible(
    expected: &ColumnType,
    udt_name: &str,
    data_type: &str,
    numeric_precision: Option<i32>,
    numeric_scale: Option<i32>,
) -> bool {
    let udt_name = normalize_postgres_type(udt_name);
    let data_type = normalize_postgres_type(data_type);
    match expected {
        ColumnType::Int64 => matches!(
            udt_name.as_str(),
            "int8" | "int4" | "int2" | "bigint" | "integer" | "smallint"
        ),
        ColumnType::Bool => matches!(udt_name.as_str(), "bool" | "boolean"),
        ColumnType::Utf8 => matches!(
            udt_name.as_str(),
            "text"
                | "varchar"
                | "character varying"
                | "bpchar"
                | "character"
                | "name"
                | "uuid"
                | "json"
                | "jsonb"
                | "bytea"
        ),
        ColumnType::TimestampMillis => {
            matches!(
                udt_name.as_str(),
                "timestamp"
                    | "timestamptz"
                    | "timestamp without time zone"
                    | "timestamp with time zone"
            ) || matches!(
                data_type.as_str(),
                "timestamp without time zone" | "timestamp with time zone"
            )
        }
        ColumnType::DateDays => udt_name == "date" || data_type == "date",
        ColumnType::Decimal128 { precision, scale } => {
            let numeric = matches!(udt_name.as_str(), "numeric" | "decimal")
                || matches!(data_type.as_str(), "numeric" | "decimal");
            numeric
                && match (numeric_precision, numeric_scale) {
                    (Some(actual_precision), Some(actual_scale)) => {
                        actual_precision == i32::from(*precision)
                            && actual_scale == i32::from(*scale)
                    }
                    (None, None) => true,
                    _ => false,
                }
        }
        ColumnType::Numeric => {
            matches!(udt_name.as_str(), "numeric" | "decimal")
                || matches!(data_type.as_str(), "numeric" | "decimal")
        }
    }
}

pub fn decimal128_type_from_precision_scale(
    precision: Option<i32>,
    scale: Option<i32>,
) -> Option<Result<ColumnType>> {
    let (Some(precision), Some(scale)) = (precision, scale) else {
        return None;
    };
    if !(1..=38).contains(&precision) || !(0..=precision).contains(&scale) {
        return None;
    }
    Some(ColumnType::decimal128(precision as u8, scale as i8))
}

pub fn normalize_postgres_type(postgres_type: &str) -> String {
    postgres_type
        .trim_start_matches("pg_catalog.")
        .to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constrained_numeric_requires_matching_shape() {
        let expected = ColumnType::decimal128(12, 2).expect("decimal type");
        assert!(postgres_type_compatible(
            &expected,
            "numeric",
            "numeric",
            Some(12),
            Some(2)
        ));
        assert!(!postgres_type_compatible(
            &expected,
            "numeric",
            "numeric",
            Some(5),
            Some(2)
        ));
    }
}
