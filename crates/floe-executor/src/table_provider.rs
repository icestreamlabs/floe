use std::any::Any;
use std::fmt;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::memory::MemTable;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use floe_core::catalog::TableDefinition;
use floe_storage::SlateCatalog;

use crate::encoding::decode_projected_row_key;
use crate::materialized_view::{DbspPersistedState, MaterializedViewRegistry};
use crate::stream_types::{Diff, Row};
use dbsp::handles::ZSetHandleView;

const MV_VERSION_COLUMN: &str = "__mv_version";

pub struct SlateTableProvider {
    storage: Arc<SlateCatalog>,
    table: TableDefinition,
    schema: datafusion::arrow::datatypes::SchemaRef,
}

impl fmt::Debug for SlateTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlateTableProvider")
            .field("table", &self.table.name())
            .finish()
    }
}

impl SlateTableProvider {
    pub fn new(storage: Arc<SlateCatalog>, table: TableDefinition) -> Self {
        let schema = table.to_arrow_schema();
        Self {
            storage,
            table,
            schema,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for SlateTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|expr| {
                if parse_mv_version_expr(expr).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut rows = self
            .storage
            .read_rows(&self.table)
            .await
            .map_err(to_datafusion_error)?;

        if let Some(limit) = limit {
            if rows.len() > limit {
                rows.truncate(limit);
            }
        }

        let batches = build_i64_batches(rows, self.schema.clone()).map_err(to_datafusion_error)?;
        let mem_table = MemTable::try_new(self.schema.clone(), vec![batches])?;
        mem_table.scan(state, projection, filters, limit).await
    }
}

fn build_i64_batches(
    rows: Vec<Vec<i64>>,
    schema: datafusion::arrow::datatypes::SchemaRef,
) -> Result<Vec<RecordBatch>> {
    if rows.is_empty() {
        return Ok(vec![RecordBatch::new_empty(schema)]);
    }

    let column_count = schema.fields().len();
    let mut columns: Vec<Vec<i64>> = vec![Vec::with_capacity(rows.len()); column_count];

    for row in rows {
        for (idx, value) in row.into_iter().enumerate() {
            if let Some(column) = columns.get_mut(idx) {
                column.push(value);
            } else {
                return Err(anyhow!("row contains unexpected column index {idx}"));
            }
        }
    }

    let arrays: Vec<ArrayRef> = columns
        .into_iter()
        .map(|col| Arc::new(Int64Array::from(col)) as ArrayRef)
        .collect();

    let batch = RecordBatch::try_new(schema, arrays).map_err(anyhow::Error::from)?;
    Ok(vec![batch])
}

fn to_datafusion_error(err: anyhow::Error) -> DataFusionError {
    DataFusionError::Execution(err.to_string())
}

pub struct MaterializedViewTableProvider {
    registry: Arc<MaterializedViewRegistry>,
    view_name: String,
    schema: datafusion::arrow::datatypes::SchemaRef,
}

impl MaterializedViewTableProvider {
    pub fn new(
        registry: Arc<MaterializedViewRegistry>,
        view_name: impl Into<String>,
        schema: datafusion::arrow::datatypes::SchemaRef,
    ) -> Self {
        let include_mv_version = !schema
            .fields()
            .iter()
            .any(|field| field.name() == MV_VERSION_COLUMN);
        let schema_with_meta = if include_mv_version {
            append_mv_version_field(&schema)
        } else {
            Arc::clone(&schema)
        };
        Self {
            registry,
            view_name: view_name.into(),
            schema: schema_with_meta,
        }
    }

    async fn build_batches(&self, as_of_version: Option<u64>) -> DFResult<Vec<RecordBatch>> {
        let (rows, version) = self.load_rows(as_of_version).await?;
        let rows = self.attach_version_column(rows, version);
        build_scalar_batches(rows, self.schema.clone())
            .map_err(|err| DataFusionError::Execution(err.to_string()))
    }

    #[cfg(test)]
    pub async fn build_batches_for_test(&self) -> DFResult<Vec<RecordBatch>> {
        self.build_batches(None).await
    }

    #[cfg(test)]
    pub async fn build_batches_at_version(&self, version: u64) -> DFResult<Vec<RecordBatch>> {
        self.build_batches(Some(version)).await
    }

    async fn load_rows(&self, as_of_version: Option<u64>) -> DFResult<(Vec<Row>, u64)> {
        let view = self.registry.get(&self.view_name).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "materialized view '{}' is not registered",
                self.view_name
            ))
        })?;

        let Some(state) = view.dbsp_state() else {
            return Ok((Vec::new(), 0));
        };
        let target_version = as_of_version.unwrap_or(state.version());
        let rows = self
            .materialize_dbsp_rows(state, Some(target_version))
            .await?;
        Ok((rows, target_version))
    }

    async fn materialize_dbsp_rows(
        &self,
        state: DbspPersistedState,
        as_of_version: Option<u64>,
    ) -> DFResult<Vec<Row>> {
        let target_version = as_of_version.unwrap_or(state.version());
        let handle_view = ZSetHandleView::new(
            state.dictionary(),
            state.table(),
            state.namespace().to_string(),
            target_version,
        );
        let snapshot = handle_view
            .materialize()
            .await
            .map_err(|err| DataFusionError::Execution(err.to_string()))?;
        #[cfg(test)]
        eprintln!(
            "materialize_dbsp_rows view={} version {} snapshot len {}",
            self.view_name,
            target_version,
            snapshot.len()
        );
        let mut rows = Vec::new();
        for (key, diff) in snapshot {
            let decoded = decode_projected_row_key(&key)
                .map_err(|err| DataFusionError::Execution(err.to_string()))?;
            append_row_with_diff(&mut rows, decoded, diff)?;
        }
        Ok(rows)
    }

    fn attach_version_column(&self, rows: Vec<Row>, version: u64) -> Vec<Row> {
        let schema_has_mv_version = self
            .schema
            .fields()
            .iter()
            .any(|field| field.name() == MV_VERSION_COLUMN);
        if !schema_has_mv_version {
            return rows;
        }
        rows.into_iter()
            .map(|mut row| {
                row.push(ScalarValue::UInt64(Some(version)));
                row
            })
            .collect()
    }
}

impl fmt::Debug for MaterializedViewTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaterializedViewTableProvider")
            .field("view", &self.view_name)
            .finish()
    }
}

#[async_trait::async_trait]
impl TableProvider for MaterializedViewTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|expr| {
                if parse_mv_version_expr(expr).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let (as_of_version, passthrough_filters) = extract_mv_version_filter(filters);
        let batches = self.build_batches(as_of_version).await?;
        let mem_table = MemTable::try_new(self.schema.clone(), vec![batches])?;
        // Always expose full schema (including __mv_version) regardless of projection pushdown.
        mem_table
            .scan(state, projection, &passthrough_filters, limit)
            .await
    }
}

fn extract_mv_version_filter(filters: &[Expr]) -> (Option<u64>, Vec<Expr>) {
    let mut as_of_version = None;
    let mut retained = Vec::with_capacity(filters.len());
    for expr in filters {
        if let Some(version) = parse_mv_version_expr(expr) {
            if as_of_version.is_none() {
                as_of_version = Some(version);
            }
            continue;
        }
        retained.push(expr.clone());
    }
    (as_of_version, retained)
}

fn parse_mv_version_expr(expr: &Expr) -> Option<u64> {
    if let Expr::BinaryExpr(binary) = expr {
        if binary.op != Operator::Eq {
            return None;
        }
        if is_mv_version_column(binary.left.as_ref()) {
            return literal_to_u64(binary.right.as_ref());
        }
        if is_mv_version_column(binary.right.as_ref()) {
            return literal_to_u64(binary.left.as_ref());
        }
    }
    None
}

fn is_mv_version_column(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(col) if col.name == MV_VERSION_COLUMN)
}

fn literal_to_u64(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Literal(ScalarValue::UInt64(Some(value)), _) => Some(*value),
        Expr::Literal(ScalarValue::Int64(Some(value)), _) if *value >= 0 => Some(*value as u64),
        _ => None,
    }
}

fn append_row_with_diff(rows: &mut Vec<Row>, row: Row, diff: Diff) -> DFResult<()> {
    if diff < 0 {
        return Err(DataFusionError::Execution(format!(
            "materialized view snapshot contains negative diff {diff}"
        )));
    }
    for _ in 0..diff {
        rows.push(row.clone());
    }
    Ok(())
}

fn build_scalar_batches(
    rows: Vec<Row>,
    schema: datafusion::arrow::datatypes::SchemaRef,
) -> Result<Vec<RecordBatch>> {
    if rows.is_empty() {
        return Ok(vec![RecordBatch::new_empty(schema)]);
    }

    let column_count = schema.fields().len();
    let mut columns: Vec<Vec<ScalarValue>> = vec![Vec::with_capacity(rows.len()); column_count];

    for row in rows {
        if row.len() != column_count {
            return Err(anyhow!(
                "row has {} columns but schema has {}",
                row.len(),
                column_count
            ));
        }
        for (idx, value) in row.into_iter().enumerate() {
            columns[idx].push(value);
        }
    }

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(column_count);
    for column in columns {
        let array = ScalarValue::iter_to_array(column.into_iter())
            .map_err(|err| anyhow!(err.to_string()))?;
        arrays.push(array);
    }

    let batch = RecordBatch::try_new(schema, arrays).map_err(anyhow::Error::from)?;
    Ok(vec![batch])
}

fn append_mv_version_field(schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| (**field).clone())
        .collect();
    fields.push(Field::new(MV_VERSION_COLUMN, DataType::UInt64, false));
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dbsp_bridge::DbspBridge;
    use crate::encoding::encode_projected_row_key;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Column;
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
    use datafusion::scalar::ScalarValue;
    use object_store::{ObjectStore, memory::InMemory};
    use slatedb::Db;

    #[tokio::test]
    async fn materialized_view_provider_emits_rows() {
        let registry = Arc::new(MaterializedViewRegistry::new());
        let view = registry.register("mv_test");

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("mv-provider", store).await.expect("open SlateDB"));
        let mut bridge = DbspBridge::new(db).await.expect("bridge");
        let mut dbsp_view = bridge.new_view("mv_test").await.expect("dbsp view");
        let row_one = vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Utf8(Some("one".into())),
        ];
        dbsp_view.add_delta(encode_projected_row_key(&row_one).expect("encode"), 1);
        let version_one = dbsp_view
            .flush()
            .await
            .expect("flush first version")
            .version;
        let row_two = vec![
            ScalarValue::Int64(Some(2)),
            ScalarValue::Utf8(Some("two".into())),
        ];
        dbsp_view.add_delta(encode_projected_row_key(&row_two).expect("encode"), 1);
        dbsp_view.flush().await.expect("flush second version");
        let handle_view = dbsp_view.latest_handle_view();
        let (dict, table, namespace, version) = handle_view.into_parts();
        view.set_dbsp_state(DbspPersistedState::new(dict, table, namespace, version));

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("label", DataType::Utf8, true),
        ]));

        let provider = MaterializedViewTableProvider::new(registry.clone(), "mv_test", schema);
        let latest = provider
            .build_batches_for_test()
            .await
            .expect("build latest");
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].num_rows(), 2);
        assert_eq!(latest[0].num_columns(), 3);

        let as_of = provider
            .build_batches_at_version(version_one)
            .await
            .expect("build as of version");
        assert_eq!(as_of.len(), 1);
        assert_eq!(as_of[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn materialized_view_provider_empty_then_populated() {
        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.register("mv_empty");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "auction",
            DataType::Int64,
            true,
        )]));
        let provider =
            MaterializedViewTableProvider::new(Arc::clone(&registry), "mv_empty", schema.clone());
        let batches = provider
            .build_batches_for_test()
            .await
            .expect("build empty batches");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("mv-empty", store).await.expect("open SlateDB"));
        let mut bridge = DbspBridge::new(db).await.expect("bridge");
        let mut dbsp_view = bridge.new_view("mv_empty").await.expect("view");
        let row = vec![ScalarValue::Int64(Some(5))];
        dbsp_view.add_delta(encode_projected_row_key(&row).expect("encode"), 1);
        dbsp_view.flush().await.expect("flush view");
        let handle_view = dbsp_view.latest_handle_view();
        let (dict, table, namespace, version) = handle_view.into_parts();
        registry
            .get("mv_empty")
            .expect("view registered")
            .set_dbsp_state(DbspPersistedState::new(dict, table, namespace, version));

        let populated = provider
            .build_batches_for_test()
            .await
            .expect("build populated batches");
        assert_eq!(populated.len(), 1);
        assert_eq!(populated[0].num_rows(), 1);
    }

    #[test]
    fn mv_version_filter_is_extracted() {
        let mv_filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name(MV_VERSION_COLUMN))),
            Operator::Eq,
            Box::new(Expr::Literal(ScalarValue::UInt64(Some(7)), None)),
        ));
        let other_filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name("auction"))),
            Operator::Eq,
            Box::new(Expr::Literal(ScalarValue::Int64(Some(42)), None)),
        ));
        let filters = vec![mv_filter.clone(), other_filter.clone()];
        let (version, retained) = super::extract_mv_version_filter(&filters);
        assert_eq!(version, Some(7));
        assert_eq!(retained, vec![other_filter.clone()]);

        let (none_version, unchanged) = super::extract_mv_version_filter(&[other_filter.clone()]);
        assert!(none_version.is_none());
        assert_eq!(unchanged, vec![other_filter.clone()]);

        let (first_version, _) =
            super::extract_mv_version_filter(&[mv_filter.clone(), mv_filter.clone()]);
        assert_eq!(first_version, Some(7));
    }
}
