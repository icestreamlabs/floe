use std::collections::HashMap;
use std::sync::Arc;

use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::operations::basic::differentiate_zset_stream;
use crate::stream::tests::common::build_db;
use crate::stream::util::{collect_values, materialize_zset_handle};
use crate::stream::zset_stream::{StreamRetention, ZSetStream};

#[tokio::test]
async fn differentiate_zset_stream_emits_deltas_per_step() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "zset_differentiation_input", None)
            .await
            .expect("build dictionary"),
    );

    let mut zset = ZSetStream::new(
        dict,
        table.clone(),
        "zset_differentiation_input".to_string(),
        StreamRetention::KeepLast { keep_last: 4 },
    )
    .await
    .expect("create zset stream");

    zset.add_delta(b"a".to_vec(), 1);
    zset.flush().await.expect("flush t1");
    zset.add_delta(b"b".to_vec(), 1);
    zset.flush().await.expect("flush t2");
    zset.add_delta(b"a".to_vec(), -1);
    zset.flush().await.expect("flush t3");

    let handles = zset.handle_stream();
    let diff_stream = differentiate_zset_stream::<Vec<u8>>(&handles)
        .await
        .expect("build diff stream");

    let diff_handles = collect_values(&diff_stream, diff_stream.current_time())
        .await
        .expect("collect diff handles");

    let mut dict_cache = HashMap::new();
    let mut observed = Vec::new();
    for handle in diff_handles {
        observed.push(
            materialize_zset_handle::<Vec<u8>>(table.clone(), &mut dict_cache, &handle)
                .await
                .expect("materialize diff handle"),
        );
    }

    let mut t1 = HashMap::new();
    t1.insert(b"a".to_vec(), 1);
    let mut t2 = HashMap::new();
    t2.insert(b"b".to_vec(), 1);
    let mut t3 = HashMap::new();
    t3.insert(b"a".to_vec(), -1);

    let expected = vec![HashMap::new(), t1, t2, t3];
    assert_eq!(observed, expected);
}
