use std::sync::Arc;

use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::runtime::HandleOperatorRuntime;
use crate::stream::tests::common::build_db;
use crate::stream::{StreamRetention, ZSetStream};

#[tokio::test]
async fn handle_operator_runtime_waits_for_alignment() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(Arc::clone(&db)));

    let dict_left = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), "runtime_left", None)
            .await
            .expect("left dict"),
    );
    let dict_right = Arc::new(
        Dictionary::<Vec<u8>>::with_table(table.clone(), "runtime_right", None)
            .await
            .expect("right dict"),
    );

    let mut left = ZSetStream::new(
        dict_left,
        table.clone(),
        "runtime_left".to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("left stream");
    let mut right = ZSetStream::new(
        dict_right,
        table.clone(),
        "runtime_right".to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("right stream");

    let records: Arc<tokio::sync::Mutex<Vec<(i64, u64, u64)>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let records_clone = Arc::clone(&records);

    let mut runtime = HandleOperatorRuntime::new(
        vec![left.handle_stream().stream(), right.handle_stream().stream()],
        move |ts, handles| {
            let records = Arc::clone(&records_clone);
            let snapshot = handles.to_vec();
            async move {
                let mut guard = records.lock().await;
                guard.push((ts, snapshot[0].version, snapshot[1].version));
                Ok(())
            }
        },
    );

    left.add_delta(vec![1], 1);
    left.flush().await.expect("flush left t1");
    right.add_delta(vec![2], 1);
    right.flush().await.expect("flush right t1");
    runtime.step().await.expect("process t1");

    left.add_delta(vec![3], 1);
    left.flush().await.expect("flush left t2");
    right.add_delta(vec![4], 1);
    right.flush().await.expect("flush right t2");
    runtime.step().await.expect("process t2");

    let collected = records.lock().await;
    assert_eq!(collected.len(), 2);
    assert_eq!(collected[0], (1, 1, 1));
    assert_eq!(collected[1], (2, 2, 2));
}
