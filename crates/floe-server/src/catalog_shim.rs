use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_pg::datatypes::field_into_pg_type;
use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{SchemaProvider, memory::MemorySchemaProvider};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use pgwire::error::PgWireResult;

use crate::execution::FloeServerState;
use crate::internal_error;

#[derive(Clone)]
struct CatalogColumn {
    name: String,
    data_type: DataType,
    nullable: bool,
}

#[derive(Clone, Copy)]
enum RelationKind {
    BaseTable,
    MaterializedView,
}

impl RelationKind {
    fn relkind(self) -> &'static str {
        match self {
            RelationKind::BaseTable => "r",
            RelationKind::MaterializedView => "m",
        }
    }

    fn table_type(self) -> &'static str {
        match self {
            RelationKind::BaseTable => "BASE TABLE",
            RelationKind::MaterializedView => "MATERIALIZED VIEW",
        }
    }
}

#[derive(Clone)]
struct CatalogRelation {
    schema: String,
    name: String,
    kind: RelationKind,
    columns: Vec<CatalogColumn>,
    definition: Option<String>,
    oid: i32,
}

pub(crate) async fn refresh_catalog_shim(state: &FloeServerState) -> PgWireResult<()> {
    let storage = state.query.storage();
    let mut relations = Vec::new();

    let tables = storage
        .tables()
        .await
        .map_err(|err| internal_error(format!("failed to load table metadata: {err}")))?;
    for table in tables {
        let (schema_name, relation_name) = split_relation_name(table.name());
        let columns = table
            .to_arrow_schema()
            .fields()
            .iter()
            .map(|field| CatalogColumn {
                name: field.name().to_string(),
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
            })
            .collect::<Vec<_>>();
        relations.push(CatalogRelation {
            schema: schema_name,
            name: relation_name,
            kind: RelationKind::BaseTable,
            columns,
            definition: None,
            oid: 0,
        });
    }

    let mut materialized_views = storage
        .materialized_views()
        .await
        .map_err(|err| internal_error(format!("failed to load materialized views: {err}")))?;
    materialized_views.sort_by(|left, right| left.name().cmp(right.name()));
    for mv in materialized_views {
        let (schema_name, relation_name) = split_relation_name(mv.name());
        let schema = if let Some(schema) = state.materialized_views.schema(mv.name()) {
            Some(schema)
        } else {
            storage
                .materialized_view_schema(mv.name())
                .await
                .map_err(|err| {
                    internal_error(format!(
                        "failed to load materialized view schema for '{}': {err}",
                        mv.name()
                    ))
                })?
        };
        let Some(schema) = schema else {
            continue;
        };
        let columns = schema
            .fields()
            .iter()
            .map(|field| CatalogColumn {
                name: field.name().to_string(),
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
            })
            .collect::<Vec<_>>();
        relations.push(CatalogRelation {
            schema: schema_name,
            name: relation_name,
            kind: RelationKind::MaterializedView,
            columns,
            definition: Some(mv.query().to_string()),
            oid: 0,
        });
    }

    relations.sort_by(|left, right| {
        left.schema
            .cmp(&right.schema)
            .then(left.name.cmp(&right.name))
            .then(left.kind.relkind().cmp(right.kind.relkind()))
    });
    for (idx, relation) in relations.iter_mut().enumerate() {
        relation.oid = 10_000 + i32::try_from(idx).unwrap_or(i32::MAX);
    }

    let mut namespaces: BTreeMap<String, i32> = BTreeMap::from([
        ("information_schema".to_string(), 12),
        ("pg_catalog".to_string(), 11),
        ("public".to_string(), 2_200),
    ]);
    let mut next_namespace_oid = 3_000 + i32::try_from(namespaces.len()).unwrap_or(i32::MAX);
    for relation in &relations {
        if !namespaces.contains_key(&relation.schema) {
            namespaces.insert(relation.schema.clone(), next_namespace_oid);
            next_namespace_oid = next_namespace_oid.saturating_add(1);
        }
    }

    let session = state.query.session();
    ensure_schema(&session, "pg_catalog")?;
    ensure_schema(&session, "information_schema")?;

    register_mem_table(
        &session,
        "pg_catalog.pg_namespace",
        build_pg_namespace_batch(&namespaces)?,
    )?;
    register_mem_table(
        &session,
        "pg_catalog.pg_class",
        build_pg_class_batch(&relations, &namespaces)?,
    )?;
    register_mem_table(
        &session,
        "pg_catalog.pg_attribute",
        build_pg_attribute_batch(&relations)?,
    )?;
    register_mem_table(
        &session,
        "pg_catalog.pg_matviews",
        build_pg_matviews_batch(&relations)?,
    )?;
    register_mem_table(
        &session,
        "information_schema.tables",
        build_information_schema_tables_batch(&relations)?,
    )?;
    register_mem_table(
        &session,
        "information_schema.columns",
        build_information_schema_columns_batch(&relations)?,
    )?;
    Ok(())
}

fn split_relation_name(name: &str) -> (String, String) {
    let trimmed = name.trim();
    if let Some((schema, relation)) = trimmed.split_once('.')
        && !schema.is_empty()
        && !relation.is_empty()
    {
        return (schema.to_string(), relation.to_string());
    }
    ("public".to_string(), trimmed.to_string())
}

fn ensure_schema(
    session: &SessionContext,
    schema_name: &str,
) -> PgWireResult<Arc<dyn SchemaProvider>> {
    let Some(catalog) = session.catalog("datafusion") else {
        return Err(internal_error(
            "default catalog 'datafusion' is not registered",
        ));
    };
    if let Some(schema) = catalog.schema(schema_name) {
        return Ok(schema);
    }
    let schema: Arc<dyn SchemaProvider> = Arc::new(MemorySchemaProvider::new());
    catalog
        .register_schema(schema_name, Arc::clone(&schema))
        .map_err(|err| {
            internal_error(format!("failed to register schema '{schema_name}': {err}"))
        })?;
    Ok(schema)
}

fn register_mem_table(
    session: &SessionContext,
    table_ref: &str,
    batch: RecordBatch,
) -> PgWireResult<()> {
    let schema = batch.schema();
    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).map_err(|err| {
        internal_error(format!(
            "failed to build catalog table '{table_ref}': {err}"
        ))
    })?;
    let _ = session.deregister_table(table_ref);
    session
        .register_table(table_ref, Arc::new(mem_table))
        .map_err(|err| {
            internal_error(format!(
                "failed to register catalog table '{table_ref}': {err}"
            ))
        })?;
    Ok(())
}

fn build_pg_namespace_batch(namespaces: &BTreeMap<String, i32>) -> PgWireResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", DataType::Int32, false),
        Field::new("nspname", DataType::Utf8, false),
    ]));
    let oids = namespaces.values().copied().collect::<Vec<_>>();
    let names = namespaces.keys().cloned().collect::<Vec<_>>();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(oids)),
        Arc::new(StringArray::from(names)),
    ];
    RecordBatch::try_new(schema, arrays)
        .map_err(|err| internal_error(format!("failed to build pg_namespace batch: {err}")))
}

fn build_pg_class_batch(
    relations: &[CatalogRelation],
    namespaces: &BTreeMap<String, i32>,
) -> PgWireResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("oid", DataType::Int32, false),
        Field::new("relname", DataType::Utf8, false),
        Field::new("relnamespace", DataType::Int32, false),
        Field::new("relkind", DataType::Utf8, false),
    ]));
    let mut oids = Vec::with_capacity(relations.len());
    let mut names = Vec::with_capacity(relations.len());
    let mut namespace_oids = Vec::with_capacity(relations.len());
    let mut kinds = Vec::with_capacity(relations.len());
    for relation in relations {
        oids.push(relation.oid);
        names.push(relation.name.clone());
        namespace_oids.push(*namespaces.get(&relation.schema).unwrap_or(&2_200));
        kinds.push(relation.kind.relkind().to_string());
    }
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(oids)),
        Arc::new(StringArray::from(names)),
        Arc::new(Int32Array::from(namespace_oids)),
        Arc::new(StringArray::from(kinds)),
    ];
    RecordBatch::try_new(schema, arrays)
        .map_err(|err| internal_error(format!("failed to build pg_class batch: {err}")))
}

fn build_pg_attribute_batch(relations: &[CatalogRelation]) -> PgWireResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("attrelid", DataType::Int32, false),
        Field::new("attname", DataType::Utf8, false),
        Field::new("atttypid", DataType::Int32, false),
        Field::new("attnum", DataType::Int32, false),
        Field::new("attnotnull", DataType::Boolean, false),
    ]));
    let mut relids = Vec::new();
    let mut names = Vec::new();
    let mut type_oids = Vec::new();
    let mut ordinals = Vec::new();
    let mut not_null = Vec::new();
    for relation in relations {
        for (idx, column) in relation.columns.iter().enumerate() {
            let field = Arc::new(Field::new(
                &column.name,
                column.data_type.clone(),
                column.nullable,
            ));
            let oid = field_into_pg_type(&field)
                .map(|ty| i32::try_from(ty.oid()).unwrap_or(i32::MAX))
                .unwrap_or(0);
            relids.push(relation.oid);
            names.push(column.name.clone());
            type_oids.push(oid);
            ordinals.push(i32::try_from(idx + 1).unwrap_or(i32::MAX));
            not_null.push(!column.nullable);
        }
    }
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(relids)),
        Arc::new(StringArray::from(names)),
        Arc::new(Int32Array::from(type_oids)),
        Arc::new(Int32Array::from(ordinals)),
        Arc::new(datafusion::arrow::array::BooleanArray::from(not_null)),
    ];
    RecordBatch::try_new(schema, arrays)
        .map_err(|err| internal_error(format!("failed to build pg_attribute batch: {err}")))
}

fn build_pg_matviews_batch(relations: &[CatalogRelation]) -> PgWireResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("schemaname", DataType::Utf8, false),
        Field::new("matviewname", DataType::Utf8, false),
        Field::new("definition", DataType::Utf8, true),
    ]));
    let mut schema_names = Vec::new();
    let mut names = Vec::new();
    let mut definitions = Vec::new();
    for relation in relations
        .iter()
        .filter(|r| matches!(r.kind, RelationKind::MaterializedView))
    {
        schema_names.push(relation.schema.clone());
        names.push(relation.name.clone());
        definitions.push(relation.definition.clone());
    }
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(schema_names)),
        Arc::new(StringArray::from(names)),
        Arc::new(StringArray::from(definitions)),
    ];
    RecordBatch::try_new(schema, arrays)
        .map_err(|err| internal_error(format!("failed to build pg_matviews batch: {err}")))
}

fn build_information_schema_tables_batch(
    relations: &[CatalogRelation],
) -> PgWireResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("table_schema", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_type", DataType::Utf8, false),
    ]));
    let mut table_schemas = Vec::with_capacity(relations.len());
    let mut table_names = Vec::with_capacity(relations.len());
    let mut table_types = Vec::with_capacity(relations.len());
    for relation in relations {
        table_schemas.push(relation.schema.clone());
        table_names.push(relation.name.clone());
        table_types.push(relation.kind.table_type().to_string());
    }
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(table_schemas)),
        Arc::new(StringArray::from(table_names)),
        Arc::new(StringArray::from(table_types)),
    ];
    RecordBatch::try_new(schema, arrays).map_err(|err| {
        internal_error(format!(
            "failed to build information_schema.tables batch: {err}"
        ))
    })
}

fn build_information_schema_columns_batch(
    relations: &[CatalogRelation],
) -> PgWireResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("table_schema", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("ordinal_position", DataType::Int32, false),
        Field::new("is_nullable", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
    ]));
    let mut table_schemas = Vec::new();
    let mut table_names = Vec::new();
    let mut column_names = Vec::new();
    let mut ordinals = Vec::new();
    let mut nullable = Vec::new();
    let mut data_types = Vec::new();
    for relation in relations {
        for (idx, column) in relation.columns.iter().enumerate() {
            table_schemas.push(relation.schema.clone());
            table_names.push(relation.name.clone());
            column_names.push(column.name.clone());
            ordinals.push(i32::try_from(idx + 1).unwrap_or(i32::MAX));
            nullable.push(if column.nullable { "YES" } else { "NO" }.to_string());
            data_types.push(column.data_type.to_string().to_ascii_lowercase());
        }
    }
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(table_schemas)),
        Arc::new(StringArray::from(table_names)),
        Arc::new(StringArray::from(column_names)),
        Arc::new(Int32Array::from(ordinals)),
        Arc::new(StringArray::from(nullable)),
        Arc::new(StringArray::from(data_types)),
    ];
    RecordBatch::try_new(schema, arrays).map_err(|err| {
        internal_error(format!(
            "failed to build information_schema.columns batch: {err}"
        ))
    })
}
