use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow_array::builder::Int64Builder;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow_ord::sort::{SortColumn, lexsort_to_indices};
use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions};
use arrow_select::concat::concat_batches;
use arrow_select::take::take;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use slatedb::WriteBatch;

use crate::handles::ZSetHandle;
use crate::storage::KeyValueTable;
use crate::storage::encoding;
use crate::storage::keyspace;
use crate::storage::segment::{ArrowSegmentStore, SegmentWriteStats, encode_segment_envelope};

pub const COLUMNAR_WEIGHT_COLUMN: &str = "__weight";

#[derive(Clone, Debug)]
pub struct ColumnarI64ZSet {
    schema: SchemaRef,
    value_column_count: usize,
    batches: Vec<RecordBatch>,
}

impl ColumnarI64ZSet {
    pub fn empty(value_column_names: &[&str]) -> Self {
        let schema = schema_for_columns(value_column_names);
        Self {
            schema,
            value_column_count: value_column_names.len(),
            batches: Vec::new(),
        }
    }

    pub fn from_i64_columns(
        value_column_names: &[&str],
        value_columns: &[Vec<i64>],
        weights: Vec<i64>,
    ) -> Result<Self> {
        if value_column_names.len() != value_columns.len() {
            bail!(
                "column name count {} does not match value column count {}",
                value_column_names.len(),
                value_columns.len()
            );
        }
        for column in value_columns {
            if column.len() != weights.len() {
                bail!(
                    "value column length {} does not match weight length {}",
                    column.len(),
                    weights.len()
                );
            }
        }

        let schema = schema_for_columns(value_column_names);
        let mut arrays = Vec::with_capacity(value_columns.len() + 1);
        for column in value_columns {
            arrays.push(Arc::new(Int64Array::from(column.clone())) as ArrayRef);
        }
        arrays.push(Arc::new(Int64Array::from(weights)) as ArrayRef);
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)
            .context("build columnar i64 zset record batch")?;
        Self::try_new(schema, value_column_names.len(), vec![batch])
    }

    pub fn from_i64_arrays(
        value_column_names: &[&str],
        value_columns: Vec<Int64Array>,
        weights: Int64Array,
    ) -> Result<Self> {
        if value_column_names.len() != value_columns.len() {
            bail!(
                "column name count {} does not match value column count {}",
                value_column_names.len(),
                value_columns.len()
            );
        }
        for column in &value_columns {
            if column.len() != weights.len() {
                bail!(
                    "value column length {} does not match weight length {}",
                    column.len(),
                    weights.len()
                );
            }
        }

        let schema = schema_for_columns(value_column_names);
        let mut arrays = Vec::with_capacity(value_columns.len() + 1);
        arrays.extend(
            value_columns
                .into_iter()
                .map(|column| Arc::new(column) as ArrayRef),
        );
        arrays.push(Arc::new(weights) as ArrayRef);
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)
            .context("build columnar i64 zset record batch")?;
        Self::try_new(schema, value_column_names.len(), vec![batch])
    }

    pub fn try_new(
        schema: SchemaRef,
        value_column_count: usize,
        batches: Vec<RecordBatch>,
    ) -> Result<Self> {
        validate_schema(&schema, value_column_count)?;
        for batch in &batches {
            validate_batch(&schema, batch)?;
        }
        Ok(Self {
            schema,
            value_column_count,
            batches,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub fn value_column_count(&self) -> usize {
        self.value_column_count
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub fn is_empty(&self) -> bool {
        self.batches.iter().all(|batch| batch.num_rows() == 0)
    }

    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    pub fn push_batch(&mut self, batch: RecordBatch) -> Result<()> {
        validate_batch(&self.schema, &batch)?;
        self.batches.push(batch);
        Ok(())
    }

    pub fn extend(&mut self, other: ColumnarI64ZSet) -> Result<()> {
        if other.schema.as_ref() != self.schema.as_ref() {
            bail!("columnar zset schema mismatch");
        }
        self.batches.extend(other.batches);
        Ok(())
    }

    pub fn materialize(&self) -> Result<HashMap<Vec<i64>, i64>> {
        let mut aggregate = HashMap::new();
        for batch in &self.batches {
            let value_columns = value_columns(batch, self.value_column_count)?;
            let weights = weight_column(batch, self.value_column_count)?;
            for row_idx in 0..batch.num_rows() {
                let mut key = Vec::with_capacity(self.value_column_count);
                for column in &value_columns {
                    key.push(column.value(row_idx));
                }
                let weight = weights.value(row_idx);
                if weight == 0 {
                    continue;
                }
                let next = aggregate
                    .get(&key)
                    .copied()
                    .unwrap_or(0_i64)
                    .saturating_add(weight);
                if next == 0 {
                    aggregate.remove(&key);
                } else {
                    aggregate.insert(key, next);
                }
            }
        }
        Ok(aggregate)
    }

    pub fn grouped_weights_by_first_column(&self) -> Result<HashMap<i64, i64>> {
        if self.value_column_count != 1 {
            bail!(
                "expected one value column for keyed weight grouping, found {}",
                self.value_column_count
            );
        }
        let mut aggregate = HashMap::new();
        for batch in &self.batches {
            let keys = i64_column(batch, 0)?;
            let weights = weight_column(batch, self.value_column_count)?;
            for row_idx in 0..batch.num_rows() {
                let weight = weights.value(row_idx);
                if weight == 0 {
                    continue;
                }
                let key = keys.value(row_idx);
                let next = aggregate
                    .get(&key)
                    .copied()
                    .unwrap_or(0_i64)
                    .saturating_add(weight);
                if next == 0 {
                    aggregate.remove(&key);
                } else {
                    aggregate.insert(key, next);
                }
            }
        }
        Ok(aggregate)
    }
}

pub struct SlateBackedColumnarI64ZSet {
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    schema: SchemaRef,
    value_column_count: usize,
    segment_store: ArrowSegmentStore,
    manifest_prefix: Vec<u8>,
    state_key: Vec<u8>,
    current_version: u64,
    persisted_version: u64,
    manifest: Option<ColumnarVersionManifest>,
    next_segment_id: u64,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug)]
struct ColumnarVersionManifest {
    base: Option<u64>,
    segments: Vec<u64>,
    reference_count: u64,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Copy, Debug)]
struct ColumnarVersionState {
    persisted_version: u64,
    next_segment_id: u64,
}

impl SlateBackedColumnarI64ZSet {
    pub async fn new(
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        value_column_names: &[&str],
    ) -> Result<Self> {
        let namespace = namespace.into();
        let segment_store = ArrowSegmentStore::new(Arc::clone(&table), namespace.clone());
        let mut manifest_prefix = keyspace::namespace_prefix(keyspace::prefix::ZSET, &namespace);
        manifest_prefix.extend_from_slice(b"manifest/columnar/");
        let mut state_key = keyspace::namespace_prefix(keyspace::prefix::ZSET, &namespace);
        state_key.extend_from_slice(b"version_state/current");
        let mut zset = Self {
            table,
            namespace,
            schema: schema_for_columns(value_column_names),
            value_column_count: value_column_names.len(),
            segment_store,
            manifest_prefix,
            state_key,
            current_version: 0,
            persisted_version: 0,
            manifest: None,
            next_segment_id: 1,
        };
        zset.refresh_state().await?;
        Ok(zset)
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn current_handle(&self) -> Option<ZSetHandle> {
        (self.current_version != 0).then(|| self.handle_for_version(self.current_version))
    }

    pub fn handle_for_version(&self, version: u64) -> ZSetHandle {
        ZSetHandle {
            ns: self.namespace.clone(),
            version,
        }
    }

    pub async fn create_version(
        &mut self,
        delta: &ColumnarI64ZSet,
        base: Option<u64>,
    ) -> Result<Option<ZSetHandle>> {
        self.validate_delta(delta)?;
        if delta.is_empty() {
            return Ok(None);
        }

        let segment_id = self.next_segment_id;
        self.next_segment_id = self.next_segment_id.saturating_add(1);
        let stats = segment_stats(delta)?;
        let (segment_bytes, _) =
            encode_segment_envelope(Arc::clone(&self.schema), delta.batches(), stats)
                .context("encode columnar zset segment")?;

        let mut batch = WriteBatch::new();
        batch.put(
            self.segment_store.key_for_segment(segment_id),
            segment_bytes,
        );

        if let Some(base_version) = base {
            let mut base_manifest = self.load_manifest_record(base_version).await?;
            base_manifest.reference_count = base_manifest.reference_count.saturating_add(1);
            batch.put(
                self.manifest_key(base_version),
                encoding::encode(&base_manifest).context("encode columnar base manifest")?,
            );
        }

        let next_version = self.current_version.saturating_add(1);
        let manifest = ColumnarVersionManifest {
            base,
            segments: vec![segment_id],
            reference_count: 1,
        };
        batch.put(
            self.manifest_key(next_version),
            encoding::encode(&manifest).context("encode columnar manifest")?,
        );
        let state = ColumnarVersionState {
            persisted_version: next_version,
            next_segment_id: self.next_segment_id,
        };
        batch.put(
            self.state_key.clone(),
            encoding::encode(&state).context("encode columnar version state")?,
        );
        self.table
            .write_batch(batch)
            .await
            .context("write columnar zset version")?;
        self.current_version = next_version;
        self.persisted_version = next_version;
        self.manifest = Some(manifest);
        Ok(Some(self.handle_for_version(next_version)))
    }

    pub async fn read_delta(&self, handle: &ZSetHandle) -> Result<ColumnarI64ZSet> {
        if handle.ns != self.namespace {
            bail!(
                "columnar zset handle namespace '{}' does not match '{}'",
                handle.ns,
                self.namespace
            );
        }
        if handle.version == 0 {
            return Ok(self.empty_like());
        }
        let manifest = self.load_manifest_record(handle.version).await?;
        self.read_manifest_delta(&manifest).await
    }

    async fn read_segment_delta(&self, segment_id: u64) -> Result<ColumnarI64ZSet> {
        let segment = self
            .segment_store
            .read_segment(segment_id)
            .await
            .with_context(|| format!("read columnar zset segment {segment_id}"))?
            .ok_or_else(|| anyhow::anyhow!("missing columnar zset segment {segment_id}"))?;
        ColumnarI64ZSet::try_new(segment.schema, self.value_column_count, segment.batches)
            .with_context(|| format!("decode columnar zset segment {segment_id}"))
    }

    pub async fn materialize(&self) -> Result<HashMap<Vec<i64>, i64>> {
        self.materialize_version(self.current_version).await
    }

    pub async fn materialize_columnar(&self) -> Result<ColumnarI64ZSet> {
        self.materialize_columnar_version(self.current_version)
            .await
    }

    pub async fn materialize_version(&self, version: u64) -> Result<HashMap<Vec<i64>, i64>> {
        if version == 0 {
            return Ok(HashMap::new());
        }
        let manifest = self.load_manifest_record(version).await?;
        let mut aggregate = if let Some(base) = manifest.base {
            Box::pin(self.materialize_version(base)).await?
        } else {
            HashMap::new()
        };
        apply_delta_to_map(&mut aggregate, self.read_manifest_delta(&manifest).await?)?;
        Ok(aggregate)
    }

    pub async fn materialize_columnar_version(&self, version: u64) -> Result<ColumnarI64ZSet> {
        if version == 0 {
            return Ok(self.empty_like());
        }

        let mut chain = Vec::new();
        let mut current = Some(version);
        while let Some(version) = current {
            let manifest = self.load_manifest_record(version).await?;
            current = manifest.base;
            chain.push(manifest);
        }

        let mut delta = self.empty_like();
        for manifest in chain.into_iter().rev() {
            delta.extend(self.read_manifest_delta(&manifest).await?)?;
        }
        consolidate_columnar_i64_zset(delta)
    }

    async fn read_manifest_delta(
        &self,
        manifest: &ColumnarVersionManifest,
    ) -> Result<ColumnarI64ZSet> {
        let mut delta = self.empty_like();
        for segment_id in &manifest.segments {
            delta.extend(self.read_segment_delta(*segment_id).await?)?;
        }
        Ok(delta)
    }

    async fn refresh_state(&mut self) -> Result<()> {
        let Some(bytes) = self
            .table
            .get_bytes(&self.state_key)
            .await
            .context("read columnar zset version state")?
        else {
            return Ok(());
        };
        let state: ColumnarVersionState =
            encoding::decode(bytes.as_ref()).context("decode columnar zset version state")?;
        self.persisted_version = state.persisted_version;
        self.current_version = state.persisted_version;
        self.next_segment_id = state.next_segment_id.max(1);
        self.manifest = if state.persisted_version == 0 {
            None
        } else {
            Some(self.load_manifest_record(state.persisted_version).await?)
        };
        Ok(())
    }

    fn manifest_key(&self, version: u64) -> Vec<u8> {
        keyspace::key_with_u64(&self.manifest_prefix, version)
    }

    async fn load_manifest_record(&self, version: u64) -> Result<ColumnarVersionManifest> {
        let bytes = self
            .table
            .get_bytes(&self.manifest_key(version))
            .await
            .with_context(|| format!("read columnar zset manifest {version}"))?
            .ok_or_else(|| anyhow::anyhow!("missing columnar zset manifest {version}"))?;
        encoding::decode(bytes.as_ref()).context("decode columnar zset manifest")
    }

    #[allow(dead_code)]
    pub async fn materialize_all_segments_for_debug(&self) -> Result<HashMap<Vec<i64>, i64>> {
        let mut aggregate = HashMap::new();
        let mut segment_ids = self
            .segment_store
            .list_segment_ids()
            .await
            .context("list columnar zset segments")?;
        segment_ids.sort_unstable();
        for segment_id in segment_ids {
            apply_delta_to_map(&mut aggregate, self.read_segment_delta(segment_id).await?)?;
        }
        Ok(aggregate)
    }

    fn validate_delta(&self, delta: &ColumnarI64ZSet) -> Result<()> {
        if delta.schema.as_ref() != self.schema.as_ref() {
            bail!("columnar zset delta schema mismatch");
        }
        Ok(())
    }

    fn empty_like(&self) -> ColumnarI64ZSet {
        ColumnarI64ZSet {
            schema: Arc::clone(&self.schema),
            value_column_count: self.value_column_count,
            batches: Vec::new(),
        }
    }
}

pub(crate) fn i64_column(batch: &RecordBatch, index: usize) -> Result<&Int64Array> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("column {index} is not Int64"))
}

fn value_columns(batch: &RecordBatch, value_column_count: usize) -> Result<Vec<&Int64Array>> {
    (0..value_column_count)
        .map(|index| i64_column(batch, index))
        .collect()
}

fn weight_column(batch: &RecordBatch, value_column_count: usize) -> Result<&Int64Array> {
    i64_column(batch, value_column_count)
}

fn schema_for_columns(value_column_names: &[&str]) -> SchemaRef {
    let mut fields = value_column_names
        .iter()
        .map(|name| Field::new(*name, DataType::Int64, false))
        .collect::<Vec<_>>();
    fields.push(Field::new(COLUMNAR_WEIGHT_COLUMN, DataType::Int64, false));
    Arc::new(Schema::new(fields))
}

fn validate_schema(schema: &Schema, value_column_count: usize) -> Result<()> {
    if schema.fields().len() != value_column_count + 1 {
        bail!(
            "expected {} fields for columnar zset, found {}",
            value_column_count + 1,
            schema.fields().len()
        );
    }
    for field in schema.fields().iter().take(value_column_count) {
        if field.data_type() != &DataType::Int64 {
            bail!("columnar zset value column '{}' is not Int64", field.name());
        }
        if field.is_nullable() {
            bail!("columnar zset value column '{}' is nullable", field.name());
        }
    }
    let weight = schema.field(value_column_count);
    if weight.name() != COLUMNAR_WEIGHT_COLUMN {
        bail!(
            "expected weight column '{}' at index {}, found '{}'",
            COLUMNAR_WEIGHT_COLUMN,
            value_column_count,
            weight.name()
        );
    }
    if weight.data_type() != &DataType::Int64 {
        bail!("columnar zset weight column is not Int64");
    }
    if weight.is_nullable() {
        bail!("columnar zset weight column is nullable");
    }
    Ok(())
}

fn validate_batch(schema: &SchemaRef, batch: &RecordBatch) -> Result<()> {
    if batch.schema().as_ref() != schema.as_ref() {
        bail!("columnar zset batch schema mismatch");
    }
    for column_idx in 0..batch.num_columns() {
        if batch.column(column_idx).null_count() != 0 {
            bail!("columnar zset column {column_idx} contains NULL values");
        }
    }
    Ok(())
}

fn segment_stats(delta: &ColumnarI64ZSet) -> Result<SegmentWriteStats> {
    let mut min_key_hash = u64::MAX;
    let mut max_key_hash = 0_u64;
    let mut tombstones = 0_usize;
    let mut rows = 0_usize;

    for batch in delta.batches() {
        let weights = weight_column(batch, delta.value_column_count)?;
        let value_columns = value_columns(batch, delta.value_column_count)?;
        for row_idx in 0..batch.num_rows() {
            rows = rows.saturating_add(1);
            let mut hasher = ahash::AHasher::default();
            for column in &value_columns {
                hasher.write_i64(column.value(row_idx));
            }
            let hash = hasher.finish();
            min_key_hash = min_key_hash.min(hash);
            max_key_hash = max_key_hash.max(hash);
            if weights.value(row_idx) < 0 {
                tombstones = tombstones.saturating_add(1);
            }
        }
    }

    if rows == 0 {
        return SegmentWriteStats::new(0, 0, 0.0);
    }
    SegmentWriteStats::new(min_key_hash, max_key_hash, tombstones as f64 / rows as f64)
}

fn consolidate_columnar_i64_zset(delta: ColumnarI64ZSet) -> Result<ColumnarI64ZSet> {
    if delta.is_empty() {
        return Ok(delta);
    }

    let schema = delta.schema();
    let value_column_count = delta.value_column_count();
    let batch = concat_batches(&schema, delta.batches())
        .context("concat columnar zset batches for consolidation")?;
    if batch.num_rows() == 0 {
        return Ok(ColumnarI64ZSet {
            schema,
            value_column_count,
            batches: Vec::new(),
        });
    }

    let sort_columns = (0..value_column_count)
        .map(|index| SortColumn {
            values: Arc::clone(batch.column(index)),
            options: Some(SortOptions::new(false, false)),
        })
        .collect::<Vec<_>>();
    let indices = lexsort_to_indices(&sort_columns, None).context("sort columnar zset keys")?;

    let mut sorted_values = Vec::with_capacity(value_column_count);
    for column_idx in 0..value_column_count {
        let column = take(batch.column(column_idx).as_ref(), &indices, None)
            .with_context(|| format!("take sorted value column {column_idx}"))?;
        sorted_values.push(
            column
                .as_any()
                .downcast_ref::<Int64Array>()
                .with_context(|| format!("sorted value column {column_idx} is not Int64"))?
                .clone(),
        );
    }
    let sorted_weights_ref = take(batch.column(value_column_count).as_ref(), &indices, None)
        .context("take sorted zset weights")?;
    let sorted_weights = sorted_weights_ref
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("sorted zset weights are not Int64")?;

    let mut value_builders = (0..value_column_count)
        .map(|_| Int64Builder::with_capacity(sorted_weights.len()))
        .collect::<Vec<_>>();
    let mut weight_builder = Int64Builder::with_capacity(sorted_weights.len());
    let mut group_start = 0_usize;
    let mut group_weight = 0_i64;

    for row_idx in 0..sorted_weights.len() {
        if row_idx != group_start && !same_value_row(&sorted_values, group_start, row_idx) {
            append_consolidated_row(
                &sorted_values,
                &mut value_builders,
                &mut weight_builder,
                group_start,
                group_weight,
            );
            group_start = row_idx;
            group_weight = 0;
        }
        group_weight = group_weight.saturating_add(sorted_weights.value(row_idx));
    }
    append_consolidated_row(
        &sorted_values,
        &mut value_builders,
        &mut weight_builder,
        group_start,
        group_weight,
    );

    let mut arrays = value_builders
        .into_iter()
        .map(|mut builder| Arc::new(builder.finish()) as ArrayRef)
        .collect::<Vec<_>>();
    arrays.push(Arc::new(weight_builder.finish()) as ArrayRef);
    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)
        .context("build consolidated zset batch")?;
    ColumnarI64ZSet::try_new(schema, value_column_count, vec![batch])
}

fn same_value_row(values: &[Int64Array], left: usize, right: usize) -> bool {
    values
        .iter()
        .all(|column| column.value(left) == column.value(right))
}

fn append_consolidated_row(
    values: &[Int64Array],
    value_builders: &mut [Int64Builder],
    weight_builder: &mut Int64Builder,
    row_idx: usize,
    weight: i64,
) {
    if weight == 0 {
        return;
    }
    for (column, builder) in values.iter().zip(value_builders.iter_mut()) {
        builder.append_value(column.value(row_idx));
    }
    weight_builder.append_value(weight);
}

fn apply_delta_to_map(
    aggregate: &mut HashMap<Vec<i64>, i64>,
    delta: ColumnarI64ZSet,
) -> Result<()> {
    for (key, weight) in delta.materialize()? {
        let next = aggregate
            .get(&key)
            .copied()
            .unwrap_or(0_i64)
            .saturating_add(weight);
        if next == 0 {
            aggregate.remove(&key);
        } else {
            aggregate.insert(key, next);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use slatedb::Db;

    use crate::storage::SlateTable;

    #[test]
    fn materializes_signed_rows() {
        let zset = ColumnarI64ZSet::from_i64_columns(
            &["key", "value"],
            &[vec![1, 1, 2], vec![10, 10, 20]],
            vec![1, -1, 3],
        )
        .expect("zset");

        let materialized = zset.materialize().expect("materialize");
        assert_eq!(materialized.len(), 1);
        assert_eq!(materialized.get(&vec![2, 20]), Some(&3));
    }

    #[test]
    fn groups_weights_by_single_key_column() {
        let zset =
            ColumnarI64ZSet::from_i64_columns(&["key"], &[vec![1, 1, 2, 2]], vec![1, 2, 3, -3])
                .expect("zset");

        let grouped = zset.grouped_weights_by_first_column().expect("group");
        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped.get(&1), Some(&3));
    }

    async fn build_table(name: &str) -> Arc<dyn KeyValueTable> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(name, store).await.expect("open SlateDB"));
        Arc::new(SlateTable::new(db))
    }

    #[tokio::test]
    async fn slate_backed_zset_materializes_persisted_segments() {
        let table = build_table("columnar-zset-slate-backed").await;
        let mut zset = SlateBackedColumnarI64ZSet::new(table, "columnar_state", &["key"])
            .await
            .expect("zset");
        let first = ColumnarI64ZSet::from_i64_columns(&["key"], &[vec![1, 1, 2]], vec![1, 2, 3])
            .expect("first");
        let second = ColumnarI64ZSet::from_i64_columns(&["key"], &[vec![1, 2]], vec![-3, 1])
            .expect("second");

        let first_handle = zset
            .create_version(&first, None)
            .await
            .expect("persist first")
            .expect("first handle");
        let base = zset.current_handle().map(|handle| handle.version);
        zset.create_version(&second, base)
            .await
            .expect("persist second")
            .expect("second handle");

        let first_read = zset.read_delta(&first_handle).await.expect("read first");
        assert_eq!(
            first_read.materialize().expect("first materialized").len(),
            2
        );

        let materialized = zset.materialize().await.expect("materialize");
        assert_eq!(materialized.len(), 1);
        assert_eq!(materialized.get(&vec![2]), Some(&4));
    }
}
