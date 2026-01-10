use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use arrow_schema::{Field as ArrowField, Schema as ArrowSchema};
use datafusion_common::{DFSchema, DFSchemaRef};

use crate::circuit::types::{DbspScalarType, ScalarValue};

pub type FieldRef = usize;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub data_type: DbspScalarType,
    pub nullable: bool,
}

impl Field {
    pub fn new(name: impl Into<String>, data_type: DbspScalarType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RowSchema {
    fields: Vec<Field>,
    index_by_name: HashMap<String, usize>,
}

impl RowSchema {
    pub fn try_new(fields: Vec<Field>) -> Result<Arc<Self>> {
        let mut index_by_name = HashMap::new();
        for (idx, field) in fields.iter().enumerate() {
            if index_by_name.insert(field.name.clone(), idx).is_some() {
                return Err(anyhow!("duplicate field name: {}", field.name));
            }
        }

        Ok(Arc::new(Self {
            fields,
            index_by_name,
        }))
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn field(&self, index: usize) -> Option<&Field> {
        self.fields.get(index)
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    pub fn field_index(&self, name: &str) -> Option<FieldRef> {
        self.index_by_name.get(name).copied()
    }

    pub fn validate_row(&self, values: &[ScalarValue]) -> Result<()> {
        if values.len() != self.fields.len() {
            return Err(anyhow!(
                "row length mismatch: expected {}, found {}",
                self.fields.len(),
                values.len()
            ));
        }

        for (field, value) in self.fields.iter().zip(values.iter()) {
            if value.is_null() {
                if !field.nullable {
                    return Err(anyhow!("field {} is not nullable", field.name));
                }
                continue;
            }

            if value.data_type() != field.data_type {
                return Err(anyhow!(
                    "type mismatch for field {}: expected {}, found {}",
                    field.name,
                    field.data_type.name(),
                    value.data_type().name()
                ));
            }
        }

        Ok(())
    }

    pub fn to_arrow_schema(&self) -> Arc<ArrowSchema> {
        let fields: Vec<ArrowField> = self
            .fields
            .iter()
            .map(|field| ArrowField::new(&field.name, field.data_type.to_arrow(), field.nullable))
            .collect();
        Arc::new(ArrowSchema::new(fields))
    }

    pub fn to_dfschema(&self) -> Result<DFSchemaRef> {
        let arrow_schema = self.to_arrow_schema();
        let df_schema = DFSchema::try_from((*arrow_schema.as_ref()).clone())
            .context("convert Arrow schema to DataFusion schema")?;
        Ok(Arc::new(df_schema))
    }
}

#[derive(Clone, Debug)]
pub struct PrimaryKey {
    schema: Arc<RowSchema>,
    columns: Vec<FieldRef>,
}

impl PrimaryKey {
    pub fn new(schema: Arc<RowSchema>, column_names: &[&str]) -> Result<Self> {
        let mut columns = Vec::with_capacity(column_names.len());
        for name in column_names {
            let index = schema
                .field_index(name)
                .ok_or_else(|| anyhow!("unknown primary key column {name}"))?;
            columns.push(index);
        }
        Ok(Self { schema, columns })
    }

    pub fn schema(&self) -> &Arc<RowSchema> {
        &self.schema
    }

    pub fn columns(&self) -> &[FieldRef] {
        &self.columns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_schema() -> Arc<RowSchema> {
        RowSchema::try_new(vec![
            Field::new("id", DbspScalarType::Int64, false),
            Field::new("name", DbspScalarType::Utf8, false),
            Field::new("active", DbspScalarType::Bool, true),
        ])
        .expect("build schema")
    }

    #[test]
    fn rejects_duplicate_fields() {
        let result = RowSchema::try_new(vec![
            Field::new("id", DbspScalarType::Int64, false),
            Field::new("id", DbspScalarType::Utf8, false),
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn validates_row_values() {
        let schema = sample_schema();
        let ok = vec![
            ScalarValue::Int64(1),
            ScalarValue::Utf8("alice".to_string()),
            ScalarValue::Null(DbspScalarType::Bool),
        ];
        assert!(schema.validate_row(&ok).is_ok());

        let wrong_type = vec![
            ScalarValue::Utf8("oops".to_string()),
            ScalarValue::Utf8("alice".to_string()),
            ScalarValue::Null(DbspScalarType::Bool),
        ];
        assert!(schema.validate_row(&wrong_type).is_err());

        let not_nullable = vec![
            ScalarValue::Null(DbspScalarType::Int64),
            ScalarValue::Utf8("alice".to_string()),
            ScalarValue::Null(DbspScalarType::Bool),
        ];
        assert!(schema.validate_row(&not_nullable).is_err());
    }

    #[test]
    fn primary_key_uses_indices() {
        let schema = sample_schema();
        let pk = PrimaryKey::new(schema.clone(), &["id"]).expect("primary key");
        assert_eq!(pk.columns(), &[0]);
        assert!(PrimaryKey::new(schema, &["missing"]).is_err());
    }
}
