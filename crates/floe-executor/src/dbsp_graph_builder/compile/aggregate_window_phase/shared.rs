use super::*;
use crate::delta_batch::{DeltaBatchBuffer, DeltaBatchConfig};
use crate::encoding::EncodedRowScalar;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;

pub(super) type ExpressionColumnMap = HashMap<String, usize>;

pub(super) fn project_encoded_delta_batch<K>(
    delta_values: &[(K, i64)],
    projector: impl Fn(&K) -> Vec<u8>,
) -> Vec<(Vec<u8>, i64)> {
    let mut projected = HashMap::<Vec<u8>, i64>::new();
    for (key, weight) in delta_values {
        if *weight == 0 {
            continue;
        }
        let encoded = projector(key);
        if encoded.is_empty() {
            continue;
        }
        let entry = projected.entry(encoded.clone()).or_insert(0);
        *entry += *weight;
        if *entry == 0 {
            projected.remove(&encoded);
        }
    }
    projected.into_iter().collect()
}

#[derive(Clone)]
pub(super) struct CountEvalLayout {
    pub(super) filters: Vec<dbsp::DbspExpression>,
    pub(super) filter_direct_columns: Vec<Option<usize>>,
    pub(super) expressions: Vec<dbsp::DbspExpression>,
    pub(super) expression_direct_columns: Vec<Option<usize>>,
    pub(super) required_input_columns: Vec<usize>,
    pub(super) required_input_positions: HashMap<usize, usize>,
    pub(super) plans: Vec<CountEvalPlan>,
}

#[derive(Clone, Copy)]
pub(super) struct CountEvalPlan {
    pub(super) filter_index: Option<usize>,
    pub(super) expr_index: Option<usize>,
}

pub(super) fn count_eval_record_batch(
    layout: &CountEvalLayout,
    input_schema: &RowSchema,
    rows: impl IntoIterator<Item = (Vec<u8>, i64)>,
) -> Result<Option<RecordBatch>> {
    let arrow_schema = input_schema.to_arrow_schema();
    let eval_schema = projected_arrow_schema(&arrow_schema, &layout.required_input_columns)?;
    let input_columns = Arc::<[usize]>::from(layout.required_input_columns.clone());
    let mut buffer = DeltaBatchBuffer::new_projected(
        eval_schema,
        input_columns,
        false,
        DeltaBatchConfig {
            max_rows: usize::MAX,
            max_bytes: usize::MAX,
        },
    )
    .context("create vectorized aggregate input buffer")?;
    for (row, weight) in rows {
        let _ = buffer
            .push(row, weight, None)
            .context("decode vectorized aggregate input row")?;
    }
    buffer
        .flush_manual()
        .context("flush vectorized aggregate input batch")
}

pub(super) fn projected_arrow_schema(
    input_schema: &datafusion::arrow::datatypes::SchemaRef,
    columns: &[usize],
) -> Result<datafusion::arrow::datatypes::SchemaRef> {
    let fields = columns
        .iter()
        .map(|idx| {
            input_schema
                .fields()
                .get(*idx)
                .map(|field| (**field).clone())
                .ok_or_else(|| {
                    anyhow!(
                        "aggregate input column {idx} is out of bounds for schema width {}",
                        input_schema.fields().len()
                    )
                })
        })
        .collect::<Result<Vec<Field>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

pub(super) enum EncodedAggregateAccumulator {
    Count {
        count: i64,
    },
    CountDistinct {
        weights: HashMap<EncodedRowScalar, i64>,
    },
    Sum {
        sum: i128,
        has_value: bool,
    },
    Avg {
        sum: i64,
        count: i64,
    },
    Min {
        current: Option<EncodedRowScalar>,
    },
    Max {
        current: Option<EncodedRowScalar>,
    },
}
