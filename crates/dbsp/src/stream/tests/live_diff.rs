use std::collections::HashMap;
use std::sync::Arc;

use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::operations::basic::differentiate_zset_stream_live;
use crate::stream::tests::common::build_db;
use crate::stream::util::{collect_values, materialize_zset_handle};
use crate::stream::zset_stream::{StreamRetention, ZSetStream};

#[tokio::test]
async fn live_diff_emits_empty_delta_for_noop_ticks() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "live_diff_input", None)
            .await
            .expect("build dictionary"),
    );

    let mut zset = ZSetStream::new(
        dict,
        table.clone(),
        "live_diff_input".to_string(),
        StreamRetention::KeepLast { keep_last: 4 },
    )
    .await
    .expect("create zset stream");

    // t1: +1 on "a"
    zset.add_delta(b"a".to_vec(), 1);
    zset.flush().await.expect("flush t1");
    // t2: +1 on "b"
    zset.add_delta(b"b".to_vec(), 1);
    zset.flush().await.expect("flush t2");
    // t3: no change
    zset.flush().await.expect("flush t3 noop");

    let diff_stream = differentiate_zset_stream_live::<Vec<u8>>(&zset.handle_stream())
        .await
        .expect("build live diff stream");
    let diff_handles = collect_values(&diff_stream, 3)
        .await
        .expect("collect diff handles");

    assert!(
        diff_handles.len() >= 3,
        "expected at least one handle per tick"
    );
    // Use the last three emitted handles to align with the three ticks we produced.
    let start = diff_handles.len() - 3;

    let mut cache = HashMap::new();
    let h1 = materialize_zset_handle::<Vec<u8>>(table.clone(), &mut cache, &diff_handles[start])
        .await
        .expect("mat diff t1");
    let h2 =
        materialize_zset_handle::<Vec<u8>>(table.clone(), &mut cache, &diff_handles[start + 1])
            .await
            .expect("mat diff t2");
    let h3 =
        materialize_zset_handle::<Vec<u8>>(table.clone(), &mut cache, &diff_handles[start + 2])
            .await
            .expect("mat diff t3");

    assert_eq!(h1, HashMap::from([(b"a".to_vec(), 1)]));
    assert_eq!(h2, HashMap::from([(b"b".to_vec(), 1)]));
    assert!(h3.is_empty(), "noop tick should emit empty delta");
}
