use std::sync::Arc;

use crate::collections::CompactionPolicy;
use crate::storage::dictionary::Dictionary;
use crate::storage::{KeyValueTable, SlateTable};
use crate::stream::tests::common::build_db;
use crate::stream::{StreamRetention, ZSetStream};
use slatedb::config::ScanOptions;

#[tokio::test]
async fn zset_stream_compacts_and_releases_versions() {
    let db = build_db().await;
    let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
    let dict = Arc::new(
        Dictionary::with_table(table.clone(), "zset_compact_stream", None)
            .await
            .expect("build dictionary"),
    );

    let mut zset = ZSetStream::new(
        dict,
        table.clone(),
        "zset_compact_stream".to_string(),
        StreamRetention::KeepLast { keep_last: 1 },
    )
    .await
    .expect("create zset stream");
    zset.set_compaction_policy(CompactionPolicy {
        max_chain_len: 2,
        max_segments: 4,
    });

    zset.add_delta(b"a".to_vec(), 1);
    zset.flush().await.expect("flush t1");
    zset.add_delta(b"b".to_vec(), 1);
    zset.flush().await.expect("flush t2");
    zset.add_delta(b"c".to_vec(), 1);
    zset.flush().await.expect("flush t3");

    let stats = zset.versioned().chain_stats().await.expect("chain stats");
    assert_eq!(stats.version_count, 1);

    let manifests = table
        .scan_prefix(
            zset.versioned().manifest_prefix_bytes(),
            &ScanOptions::default(),
        )
        .await
        .expect("scan manifests");
    assert_eq!(manifests.len(), 1);

    let view = zset
        .latest_view()
        .materialize()
        .await
        .expect("materialize compacted view");
    assert_eq!(view.get(b"a".as_ref()), Some(&1));
    assert_eq!(view.get(b"b".as_ref()), Some(&1));
    assert_eq!(view.get(b"c".as_ref()), Some(&1));
}
