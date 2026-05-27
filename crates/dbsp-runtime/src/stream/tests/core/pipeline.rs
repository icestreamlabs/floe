use std::sync::Arc;

use crate::LogicalWorkSnapshot;
use crate::handles::ZSetHandle;
use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::runtime::{DeltaOperator, PipelineBuilder, single_input_pipeline};
use crate::stream::tests::common::build_db;
use crate::stream::{StreamRetention, ZSetStream};
use async_trait::async_trait;

type ObservedHandles = Arc<tokio::sync::Mutex<Vec<(i64, Vec<ZSetHandle>)>>>;

struct RecordingOp {
    observed: ObservedHandles,
}

#[async_trait]
impl DeltaOperator for RecordingOp {
    async fn on_step(
        &mut self,
        ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        let snapshot = inputs.to_vec();
        let mut guard = self.observed.lock().await;
        guard.push((ts, snapshot));
        Ok(None)
    }
}

struct PassthroughOp;

#[async_trait]
impl DeltaOperator for PassthroughOp {
    async fn on_step(
        &mut self,
        _ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        Ok(inputs.first().cloned())
    }
}

struct WorkReportingOp {
    work: LogicalWorkSnapshot,
}

#[async_trait]
impl DeltaOperator for WorkReportingOp {
    async fn on_step(
        &mut self,
        _ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        self.work.input_delta_rows = inputs.len() as u64;
        Ok(inputs.first().cloned())
    }

    fn logical_work(&self) -> Option<LogicalWorkSnapshot> {
        Some(self.work)
    }
}

struct EmitEmptyHandleOp;

#[async_trait]
impl DeltaOperator for EmitEmptyHandleOp {
    async fn on_step(
        &mut self,
        _ts: i64,
        _inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        Ok(Some(ZSetHandle {
            ns: "pipeline_empty".to_string(),
            version: 0,
        }))
    }
}

#[tokio::test]
async fn pipeline_invokes_operator_with_aligned_inputs() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));

    let dict_left = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), "pipe_left", None)
            .await
            .expect("left dict"),
    );
    let dict_right = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), "pipe_right", None)
            .await
            .expect("right dict"),
    );

    let mut left = ZSetStream::new(
        dict_left,
        table.clone(),
        "pipe_left".to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("left stream");
    let mut right = ZSetStream::new(
        dict_right,
        table.clone(),
        "pipe_right".to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("right stream");

    let observed: ObservedHandles = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    let mut pipeline = PipelineBuilder::new(vec![
        left.handle_stream().stream(),
        right.handle_stream().stream(),
    ])
    .push_op(RecordingOp {
        observed: Arc::clone(&observed),
    })
    .build();

    left.add_delta(vec![1], 1);
    left.flush().await.expect("flush left t1");
    right.add_delta(vec![2], 1);
    right.flush().await.expect("flush right t1");
    pipeline.step_once().await.expect("process t1");

    left.add_delta(vec![3], 1);
    left.flush().await.expect("flush left t2");
    right.add_delta(vec![4], 1);
    right.flush().await.expect("flush right t2");
    pipeline.step_once().await.expect("process t2");

    let collected = observed.lock().await;
    assert_eq!(collected.len(), 2);
    assert_eq!(collected[0].0, 1);
    assert_eq!(collected[0].1[0].version, 1);
    assert_eq!(collected[0].1[1].version, 1);
    assert_eq!(collected[1].0, 2);
    assert_eq!(collected[1].1[0].version, 2);
    assert_eq!(collected[1].1[1].version, 2);
}

#[tokio::test]
async fn single_input_pipeline_passes_through_operator() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));

    let dict = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), "single_pipe", None)
            .await
            .expect("dict"),
    );

    let mut stream = ZSetStream::new(
        dict,
        table.clone(),
        "single_pipe".to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("stream");

    let observed: ObservedHandles = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    let mut pipeline = single_input_pipeline(
        stream.handle_stream().stream(),
        vec![
            Box::new(PassthroughOp),
            Box::new(RecordingOp {
                observed: Arc::clone(&observed),
            }),
        ],
    );

    stream.add_delta(vec![1], 1);
    stream.flush().await.expect("flush t1");
    pipeline.step_once().await.expect("process t1");

    stream.add_delta(vec![2], 1);
    stream.flush().await.expect("flush t2");
    pipeline.step_once().await.expect("process t2");

    let collected = observed.lock().await;
    assert_eq!(collected.len(), 2);
    assert_eq!(collected[0].0, 1);
    assert_eq!(collected[0].1[0].version, 1);
    assert_eq!(collected[1].0, 2);
    assert_eq!(collected[1].1[0].version, 2);
}

#[tokio::test]
async fn pipeline_propagates_explicit_empty_handles() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));

    let dict = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), "empty_pipe", None)
            .await
            .expect("dict"),
    );

    let mut stream = ZSetStream::new(
        dict,
        table.clone(),
        "empty_pipe".to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("stream");

    let observed: ObservedHandles = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    let mut pipeline = single_input_pipeline(
        stream.handle_stream().stream(),
        vec![
            Box::new(EmitEmptyHandleOp),
            Box::new(RecordingOp {
                observed: Arc::clone(&observed),
            }),
        ],
    );

    stream.add_delta(vec![1], 1);
    stream.flush().await.expect("flush t1");
    pipeline.step_once().await.expect("process t1");

    stream.add_delta(vec![2], 1);
    stream.flush().await.expect("flush t2");
    pipeline.step_once().await.expect("process t2");

    let collected = observed.lock().await;
    assert_eq!(collected.len(), 2);
    assert_eq!(collected[0].0, 1);
    assert_eq!(collected[0].1[0].version, 0);
    assert_eq!(collected[1].0, 2);
    assert_eq!(collected[1].1[0].version, 0);
}

#[tokio::test]
async fn pipeline_exposes_operator_logical_work() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));

    let dict = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), "work_pipe", None)
            .await
            .expect("dict"),
    );

    let mut stream = ZSetStream::new(
        dict,
        table.clone(),
        "work_pipe".to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("stream");

    let mut pipeline = single_input_pipeline(
        stream.handle_stream().stream(),
        vec![Box::new(WorkReportingOp {
            work: LogicalWorkSnapshot::default(),
        })],
    );

    stream.add_delta(vec![1], 1);
    stream.flush().await.expect("flush t1");
    pipeline.step_once().await.expect("process t1");

    let work = pipeline.operator_logical_work();
    assert_eq!(work.len(), 1);
    assert_eq!(work[0].operator_index, 0);
    assert_eq!(work[0].work.input_delta_rows, 1);
}
