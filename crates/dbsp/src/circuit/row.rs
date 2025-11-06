use std::sync::Arc;

use anyhow::{Result, bail};

use crate::circuit::schema::RowSchema;
use crate::circuit::types::ScalarValue;

#[derive(Clone, Debug)]
pub struct Row {
    schema: Arc<RowSchema>,
    values: Vec<ScalarValue>,
}

impl Row {
    pub fn new(schema: Arc<RowSchema>, values: Vec<ScalarValue>) -> Result<Self> {
        schema.validate_row(&values)?;
        Ok(Self { schema, values })
    }

    pub fn schema(&self) -> &Arc<RowSchema> {
        &self.schema
    }

    pub fn values(&self) -> &[ScalarValue] {
        &self.values
    }

    pub fn value(&self, index: usize) -> Option<&ScalarValue> {
        self.values.get(index)
    }
}

pub struct RowBuilder {
    schema: Arc<RowSchema>,
    values: Vec<ScalarValue>,
}

impl RowBuilder {
    pub fn new(schema: Arc<RowSchema>) -> Self {
        let capacity = schema.len();
        Self {
            schema,
            values: Vec::with_capacity(capacity),
        }
    }

    pub fn push(mut self, value: ScalarValue) -> Result<Self> {
        let field_index = self.values.len();
        if let Some(field) = self.schema.field(field_index) {
            if value.is_null() {
                if !field.nullable {
                    bail!("field {} is not nullable", field.name);
                }
            } else if value.data_type() != field.data_type {
                bail!(
                    "type mismatch for field {}: expected {}, found {}",
                    field.name,
                    field.data_type.name(),
                    value.data_type().name()
                );
            }
        } else {
            bail!(
                "too many values for schema (expected {})",
                self.schema.len()
            );
        }

        self.values.push(value);
        Ok(self)
    }

    pub fn finish(mut self) -> Result<Row> {
        if self.values.len() != self.schema.len() {
            bail!(
                "row length mismatch: expected {}, found {}",
                self.schema.len(),
                self.values.len()
            );
        }
        let values = std::mem::take(&mut self.values);
        Row::new(self.schema, values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::circuit::schema::Field;
    use crate::circuit::types::{DbspScalarType, ScalarValue};

    fn schema() -> Arc<RowSchema> {
        RowSchema::try_new(vec![
            Field::new("id", DbspScalarType::Int64, false),
            Field::new("active", DbspScalarType::Bool, true),
        ])
        .expect("schema")
    }

    #[test]
    fn builder_respects_schema() {
        let schema = schema();
        let row = RowBuilder::new(schema.clone())
            .push(ScalarValue::Int64(10))
            .unwrap()
            .push(ScalarValue::Bool(true))
            .unwrap()
            .finish()
            .expect("row");

        assert_eq!(row.value(0), Some(&ScalarValue::Int64(10)));
        assert_eq!(row.value(1), Some(&ScalarValue::Bool(true)));

        match RowBuilder::new(schema.clone()).push(ScalarValue::Bool(true)) {
            Ok(_) => panic!("type mismatch accepted"),
            Err(err) => assert!(err.to_string().contains("expected Int64")),
        }

        let err = RowBuilder::new(schema)
            .push(ScalarValue::Int64(1))
            .unwrap()
            .finish();
        assert!(err.unwrap_err().to_string().contains("row length mismatch"));
    }
}
