use std::sync::Arc;

use floe_executor::dbsp_bridge::DbspBridge;
use floe_executor::{FloeQueryContext, MaterializedViewRegistry, load_or_register_mv};
use futures::{StreamExt, stream};
use pgwire::api::results::{FieldInfo, QueryResponse};
use pgwire::error::PgWireResult;
use tokio::sync::Mutex;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;

use super::sql::mv_identifiers_in_sql;
use super::types::{arrow_schema_to_field_info, encode_stream_row};
use super::user_error;

pub(crate) struct FloeServerState {
    pub(crate) query: FloeQueryContext,
    pub(crate) materialized_views: Arc<MaterializedViewRegistry>,
    bridge: Arc<Mutex<DbspBridge>>,
}

impl FloeServerState {
    pub(crate) fn new(
        query: FloeQueryContext,
        materialized_views: Arc<MaterializedViewRegistry>,
        bridge: DbspBridge,
    ) -> Self {
        Self {
            query,
            materialized_views,
            bridge: Arc::new(Mutex::new(bridge)),
        }
    }

    pub(crate) async fn ensure_materialized_view_registered(&self, name: &str) -> PgWireResult<()> {
        let session = self.query.session();
        let mut bridge = self.bridge.lock().await;
        load_or_register_mv(
            &session,
            Arc::clone(&self.materialized_views),
            &mut bridge,
            name,
        )
        .await
        .map_err(|err| {
            user_error(format!(
                "materialized view '{name}' is not available: {err}"
            ))
        })
    }

    pub(crate) async fn ensure_materialized_views_in_sql(&self, sql: &str) -> PgWireResult<()> {
        for view in mv_identifiers_in_sql(sql) {
            self.ensure_materialized_view_registered(&view).await?;
        }
        Ok(())
    }
}

pub(crate) fn build_query_response(batches: Vec<RecordBatch>) -> PgWireResult<QueryResponse> {
    if batches.is_empty() {
        let schema = Arc::new(Vec::new());
        let rows = stream::iter(Vec::<PgWireResult<_>>::new());
        return Ok(QueryResponse::new(schema, rows));
    }

    let info = Arc::new(arrow_schema_to_field_info(&batches[0].schema())?);
    let schema_ref = info.clone();
    let row_stream = stream::iter(batches.into_iter().flat_map(move |batch| {
        let schema = Arc::clone(&schema_ref);
        (0..batch.num_rows())
            .map(move |row_idx| encode_stream_row(&batch, row_idx, Arc::clone(&schema)))
    }));

    Ok(QueryResponse::new(info, row_stream))
}

pub(crate) async fn build_query_response_stream(
    mut batch_stream: SendableRecordBatchStream,
) -> PgWireResult<QueryResponse> {
    let Some(first_batch_result) = batch_stream.next().await else {
        let schema = Arc::new(Vec::new());
        let rows = stream::iter(Vec::<PgWireResult<_>>::new());
        return Ok(QueryResponse::new(schema, rows));
    };
    let first_batch = first_batch_result
        .map_err(|err| user_error(format!("DataFusion execution error: {err}")))?;
    let info = Arc::new(arrow_schema_to_field_info(&first_batch.schema())?);
    let row_schema = Arc::clone(&info);

    struct StreamState {
        stream: SendableRecordBatchStream,
        current_batch: Option<RecordBatch>,
        next_row: usize,
        schema: Arc<Vec<FieldInfo>>,
    }

    let initial_state = StreamState {
        stream: batch_stream,
        current_batch: Some(first_batch),
        next_row: 0,
        schema: row_schema,
    };

    let rows = stream::try_unfold(initial_state, move |mut state| async move {
        loop {
            if let Some(batch) = state.current_batch.as_ref() {
                if state.next_row < batch.num_rows() {
                    let schema = Arc::clone(&state.schema);
                    let row = encode_stream_row(batch, state.next_row, schema)?;
                    state.next_row += 1;
                    return Ok(Some((row, state)));
                }
                state.current_batch = None;
                state.next_row = 0;
            }

            match state.stream.next().await {
                Some(Ok(batch)) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    state.current_batch = Some(batch);
                    state.next_row = 0;
                }
                Some(Err(err)) => {
                    return Err(user_error(format!("DataFusion execution error: {err}")));
                }
                None => return Ok(None),
            }
        }
    });

    Ok(QueryResponse::new(info, rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::physical_plan::RecordBatchStream;
    use futures::Stream;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    #[tokio::test]
    async fn build_query_response_stream_yields_batches_incrementally() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch_one = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )
        .expect("record batch");
        let batch_two = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![3])) as ArrayRef],
        )
        .expect("record batch");
        let poll_counter = Arc::new(AtomicUsize::new(0));
        let stream: SendableRecordBatchStream = Box::pin(TestBatchStream::new(
            vec![batch_one, batch_two],
            poll_counter.clone(),
        ));

        let mut response = build_query_response_stream(stream)
            .await
            .expect("stream response");
        let schema = response.row_schema();
        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].name(), "value");

        let rows = response.data_rows();
        rows.next().await.expect("row").expect("ok row");
        assert_eq!(poll_counter.load(Ordering::SeqCst), 1);

        rows.next().await.expect("row").expect("ok row");
        assert_eq!(
            poll_counter.load(Ordering::SeqCst),
            1,
            "second batch should not be polled yet"
        );

        rows.next().await.expect("row").expect("ok row");
        assert_eq!(
            poll_counter.load(Ordering::SeqCst),
            2,
            "second batch should be polled after draining the first"
        );
        assert!(rows.next().await.is_none());
    }

    struct TestBatchStream {
        batches: Vec<RecordBatch>,
        next_index: usize,
        polled: Arc<AtomicUsize>,
    }

    impl TestBatchStream {
        fn new(batches: Vec<RecordBatch>, polled: Arc<AtomicUsize>) -> Self {
            Self {
                batches,
                next_index: 0,
                polled,
            }
        }
    }

    impl Stream for TestBatchStream {
        type Item = datafusion::error::Result<RecordBatch>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = self.get_mut();
            if this.next_index >= this.batches.len() {
                return Poll::Ready(None);
            }
            let batch = this.batches[this.next_index].clone();
            this.next_index += 1;
            this.polled.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Some(Ok(batch)))
        }
    }

    impl RecordBatchStream for TestBatchStream {
        fn schema(&self) -> SchemaRef {
            self.batches
                .first()
                .map(|batch| batch.schema())
                .unwrap_or_else(|| Arc::new(Schema::new(Vec::<Field>::new())))
        }
    }
}
