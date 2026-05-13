use std::fmt;

use anyhow::{Result, bail};

/// Scalar types supported by the DBSP circuit runtime.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum DbspScalarType {
    Int64,
    Utf8,
    TimestampMillis,
    Bool,
    DateDays,
}

impl DbspScalarType {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Int64 => "Int64",
            Self::Utf8 => "Utf8",
            Self::TimestampMillis => "TimestampMillis",
            Self::Bool => "Bool",
            Self::DateDays => "DateDays",
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
            Self::DateDays => arrow_schema::DataType::Date32,
        }
    }

    pub fn try_from_arrow(data_type: &arrow_schema::DataType) -> Result<Self> {
        use arrow_schema::{DataType, TimeUnit};
        match data_type {
            DataType::Int64 => Ok(Self::Int64),
            DataType::Utf8 => Ok(Self::Utf8),
            DataType::Boolean => Ok(Self::Bool),
            DataType::Timestamp(TimeUnit::Millisecond, None) => Ok(Self::TimestampMillis),
            DataType::Date32 => Ok(Self::DateDays),
            other => bail!("unsupported DataFusion type {:?}", other),
        }
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
    fn arrow_conversion() {
        assert_eq!(DbspScalarType::Int64.to_arrow(), DataType::Int64);
        assert_eq!(
            DbspScalarType::TimestampMillis.to_arrow(),
            DataType::Timestamp(TimeUnit::Millisecond, None)
        );

        let ty = DbspScalarType::try_from_arrow(&DataType::Boolean).unwrap();
        assert_eq!(ty, DbspScalarType::Bool);
        let ty = DbspScalarType::try_from_arrow(&DataType::Date32).unwrap();
        assert_eq!(ty, DbspScalarType::DateDays);

        assert!(DbspScalarType::try_from_arrow(&DataType::Float64).is_err());
    }
}
