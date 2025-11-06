use std::fmt;

use anyhow::{Result, bail};

/// Scalar types supported by the DBSP circuit runtime.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum DbspScalarType {
    Int64,
    Utf8,
    TimestampMillis,
    Bool,
}

impl DbspScalarType {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Int64 => "Int64",
            Self::Utf8 => "Utf8",
            Self::TimestampMillis => "TimestampMillis",
            Self::Bool => "Bool",
        }
    }

    pub fn to_arrow(&self) -> arrow_schema::DataType {
        match self {
            Self::Int64 => arrow_schema::DataType::Int64,
            Self::Utf8 => arrow_schema::DataType::Utf8,
            Self::TimestampMillis => {
                arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None)
            }
            Self::Bool => arrow_schema::DataType::Boolean,
        }
    }

    pub fn try_from_arrow(data_type: &arrow_schema::DataType) -> Result<Self> {
        use arrow_schema::{DataType, TimeUnit};
        match data_type {
            DataType::Int64 => Ok(Self::Int64),
            DataType::Utf8 => Ok(Self::Utf8),
            DataType::Boolean => Ok(Self::Bool),
            DataType::Timestamp(TimeUnit::Millisecond, None) => Ok(Self::TimestampMillis),
            other => bail!("unsupported DataFusion type {:?}", other),
        }
    }
}

/// Typed scalar value used by circuit rows.
#[derive(Clone, Debug, PartialEq)]
pub enum ScalarValue {
    Int64(i64),
    Utf8(String),
    TimestampMillis(i64),
    Bool(bool),
    Null(DbspScalarType),
}

impl ScalarValue {
    pub fn data_type(&self) -> DbspScalarType {
        match self {
            Self::Int64(_) => DbspScalarType::Int64,
            Self::Utf8(_) => DbspScalarType::Utf8,
            Self::TimestampMillis(_) => DbspScalarType::TimestampMillis,
            Self::Bool(_) => DbspScalarType::Bool,
            Self::Null(ty) => ty.clone(),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null(_))
    }

    pub fn null(ty: DbspScalarType) -> Self {
        Self::Null(ty)
    }

    pub fn timestamp_millis(value: i64) -> Self {
        Self::TimestampMillis(value)
    }
}

impl From<i64> for ScalarValue {
    fn from(value: i64) -> ScalarValue {
        ScalarValue::Int64(value)
    }
}

impl From<String> for ScalarValue {
    fn from(value: String) -> ScalarValue {
        ScalarValue::Utf8(value)
    }
}

impl From<&str> for ScalarValue {
    fn from(value: &str) -> ScalarValue {
        ScalarValue::Utf8(value.to_string())
    }
}

impl From<bool> for ScalarValue {
    fn from(value: bool) -> ScalarValue {
        ScalarValue::Bool(value)
    }
}

impl fmt::Display for DbspScalarType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, TimeUnit};

    #[test]
    fn scalar_value_type_round_trip() {
        let values = vec![
            ScalarValue::Int64(42),
            ScalarValue::Utf8("hello".to_string()),
            ScalarValue::TimestampMillis(1_700_000_000_000),
            ScalarValue::Bool(true),
            ScalarValue::Null(DbspScalarType::Utf8),
        ];

        for value in values {
            let ty = value.data_type();
            assert_eq!(ty.name().len() > 0, true);
            if let ScalarValue::Null(_) = value {
                assert!(value.is_null());
            } else {
                assert!(!value.is_null());
            }
        }
    }

    #[test]
    fn arrow_conversion() {
        assert_eq!(DbspScalarType::Int64.to_arrow(), DataType::Int64);
        assert_eq!(
            DbspScalarType::TimestampMillis.to_arrow(),
            DataType::Timestamp(TimeUnit::Millisecond, None)
        );

        let ty = DbspScalarType::try_from_arrow(&DataType::Boolean).unwrap();
        assert_eq!(ty, DbspScalarType::Bool);

        assert!(DbspScalarType::try_from_arrow(&DataType::Float64).is_err());
    }
}
