pub type RowValues = Vec<i64>;

pub mod catalog {
    use super::RowValues;
    use anyhow::{Result, anyhow, ensure};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub enum ColumnType {
        Int64,
    }

    impl ColumnType {
        pub fn arrow_type(&self) -> arrow_schema::DataType {
            match self {
                ColumnType::Int64 => arrow_schema::DataType::Int64,
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct ColumnDefinition {
        name: String,
        data_type: ColumnType,
        primary_key: bool,
    }

    impl ColumnDefinition {
        pub fn new(name: impl Into<String>, primary_key: bool) -> Self {
            Self {
                name: name.into(),
                data_type: ColumnType::Int64,
                primary_key,
            }
        }

        pub fn name(&self) -> &str {
            &self.name
        }

        pub fn data_type(&self) -> &ColumnType {
            &self.data_type
        }

        pub fn is_primary_key(&self) -> bool {
            self.primary_key
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TableDefinition {
        name: String,
        columns: Vec<ColumnDefinition>,
    }

    impl TableDefinition {
        pub fn new(name: impl Into<String>, columns: Vec<ColumnDefinition>) -> Result<Self> {
            let name = name.into();
            ensure!(!name.trim().is_empty(), "table name cannot be empty");
            ensure!(
                !columns.is_empty(),
                "table {} must have at least one column",
                name
            );
            let pk_count = columns.iter().filter(|c| c.is_primary_key()).count();
            ensure!(
                pk_count == 1,
                "table {} must define exactly one primary key column",
                name
            );
            Ok(Self { name, columns })
        }

        pub fn name(&self) -> &str {
            &self.name
        }

        pub fn columns(&self) -> &[ColumnDefinition] {
            &self.columns
        }

        pub fn primary_key_index(&self) -> usize {
            self.columns
                .iter()
                .position(|column| column.is_primary_key())
                .expect("table definition validated to contain a primary key")
        }

        pub fn column_index(&self, name: &str) -> Option<usize> {
            self.columns.iter().position(|column| column.name() == name)
        }

        pub fn to_arrow_schema(&self) -> arrow_schema::SchemaRef {
            let fields = self
                .columns
                .iter()
                .map(|column| {
                    arrow_schema::Field::new(column.name(), column.data_type().arrow_type(), false)
                })
                .collect::<Vec<_>>();
            std::sync::Arc::new(arrow_schema::Schema::new(fields))
        }

        pub fn validate_row(&self, row: &RowValues) -> Result<()> {
            if row.len() != self.columns.len() {
                return Err(anyhow!(
                    "row length {} does not match column count {}",
                    row.len(),
                    self.columns.len()
                ));
            }
            Ok(())
        }
    }
}

pub mod encoding {
    use super::RowValues;
    use anyhow::{Result, anyhow};

    #[derive(Debug, Clone, PartialEq, Eq, Default)]
    pub struct ArchivedRow {
        bytes: Vec<u8>,
    }

    impl ArchivedRow {
        pub fn new(bytes: Vec<u8>) -> Self {
            Self { bytes }
        }

        pub fn bytes(&self) -> &[u8] {
            &self.bytes
        }
    }

    pub fn encode(values: &RowValues) -> Result<ArchivedRow> {
        let aligned = rkyv::api::high::to_bytes::<rkyv::rancor::Error>(&values.clone())
            .map_err(|err| anyhow!("failed to encode row using rkyv: {err}"))?;
        Ok(ArchivedRow::new(aligned.into_vec()))
    }

    pub fn decode(row: &ArchivedRow) -> Result<RowValues> {
        let values = rkyv::from_bytes::<RowValues, rkyv::rancor::Error>(row.bytes())
            .map_err(|err| anyhow!("failed to decode row using rkyv: {err}"))?;
        Ok(values)
    }
}
