use std::sync::Arc;

use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};

use crate::circuit::schema::RowSchema;

pub const WEIGHT_COLUMN_NAME: &str = "__weight";
pub const KEY_COLUMN_NAME: &str = "__key";

pub fn delta_arrow_schema(row_schema: &RowSchema, include_key: bool) -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(delta_arrow_fields(
        row_schema,
        include_key,
    )))
}

pub fn delta_arrow_fields(row_schema: &RowSchema, include_key: bool) -> Vec<ArrowField> {
    let mut fields: Vec<ArrowField> = row_schema
        .fields()
        .iter()
        .map(|field| ArrowField::new(&field.name, field.data_type.to_arrow(), field.nullable))
        .collect();

    if include_key {
        fields.push(ArrowField::new(KEY_COLUMN_NAME, DataType::Binary, false));
    }

    fields.push(ArrowField::new(WEIGHT_COLUMN_NAME, DataType::Int64, false));
    fields
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::circuit::schema::{Field, RowSchema};
    use crate::circuit::types::DbspScalarType;

    #[test]
    fn delta_schema_appends_weight_and_optional_key() {
        let schema = RowSchema::try_new(vec![
            Field::new("id", DbspScalarType::Int64, false),
            Field::new("name", DbspScalarType::Utf8, true),
        ])
        .expect("schema");

        let base = delta_arrow_fields(&schema, false);
        assert_eq!(base.len(), 3);
        assert_eq!(base[0].name(), "id");
        assert_eq!(base[1].name(), "name");
        assert_eq!(base[2].name(), WEIGHT_COLUMN_NAME);
        assert_eq!(base[2].data_type(), &DataType::Int64);
        assert!(!base[2].is_nullable());

        let with_key = delta_arrow_fields(&schema, true);
        assert_eq!(with_key.len(), 4);
        assert_eq!(with_key[2].name(), KEY_COLUMN_NAME);
        assert_eq!(with_key[2].data_type(), &DataType::Binary);
        assert!(!with_key[2].is_nullable());
        assert_eq!(with_key[3].name(), WEIGHT_COLUMN_NAME);
    }
}
