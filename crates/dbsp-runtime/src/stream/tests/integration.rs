use std::collections::HashMap;
use std::sync::Arc;

use crate::collections::CompactionPolicy;
use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::operations::basic::integrate_zset_stream;
use crate::stream::tests::common::build_db;
use crate::stream::util::{collect_values, materialize_zset_handle};
use crate::stream::zset_stream::{StreamRetention, ZSetStream};

#[tokio::test]
async fn integrate_zset_stream_handles_compaction_noop_ticks_incrementally() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "zset_integrate_compaction", None)
            .await
            .expect("build dictionary"),
    );

    let mut source = ZSetStream::new(
        dict,
        table.clone(),
        "zset_integrate_compaction".to_string(),
        StreamRetention::KeepLast { keep_last: 4 },
    )
    .await
    .expect("create source zset stream");
    source.set_compaction_policy(CompactionPolicy {
        max_chain_len: 1,
        max_segments: 1,
        max_bucket_segments: 1,
    });

    source.add_delta(b"a".to_vec(), 1);
    source.flush().await.expect("flush t1");
    let _ = source
        .wait_for_background_compaction()
        .await
        .expect("wait for compaction");
    source.flush().await.expect("flush t2 noop");
    source.add_delta(b"a".to_vec(), 1);
    source.flush().await.expect("flush t3");

    let integrated = integrate_zset_stream::<Vec<u8>>(&source.handle_stream())
        .await
        .expect("build integrated stream");
    let handles = collect_values(&integrated, integrated.current_time())
        .await
        .expect("collect integrated handles");

    let mut cache = HashMap::new();
    let mut observed = Vec::new();
    for handle in handles {
        observed.push(
            materialize_zset_handle::<Vec<u8>>(table.clone(), &mut cache, &handle)
                .await
                .expect("materialize integrated handle"),
        );
    }

    assert!(observed.len() >= 4, "expected default plus three ticks");
    assert_eq!(observed[1], HashMap::from([(b"a".to_vec(), 1)]));
    assert_eq!(observed[2], HashMap::from([(b"a".to_vec(), 1)]));
    assert_eq!(observed[3], HashMap::from([(b"a".to_vec(), 2)]));
}
