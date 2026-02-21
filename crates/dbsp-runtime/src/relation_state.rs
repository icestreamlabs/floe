use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::collections::zset::VersionedZSet;
use crate::handles::ZSetHandle;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use anyhow::Context;
use std::hash::Hash;
use std::sync::Arc;

/// Canonical relation state at a logical time boundary.
pub struct RelationState<K>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    /// Integrated `R_t` for the relation.
    pub integrated: VersionedZSet<K>,
    /// Handle pointing at the integrated version.
    pub latest_handle: ZSetHandle,
}

impl<K> RelationState<K>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub fn update(&mut self, integrated: VersionedZSet<K>, handle: ZSetHandle) {
        self.integrated = integrated;
        self.latest_handle = handle;
    }

    pub fn update_handle(&mut self, handle: ZSetHandle) {
        self.latest_handle = handle;
    }

    pub fn dictionary(&self) -> Arc<Dictionary<K>> {
        self.integrated.dictionary()
    }

    pub async fn empty(table: Arc<dyn KeyValueTable>, namespace: String) -> anyhow::Result<Self> {
        let dict = Arc::new(
            Dictionary::<K>::with_table(table.clone(), namespace.clone(), None)
                .await
                .context("create relation state dictionary")?,
        );
        let integrated = VersionedZSet::new(dict, table, namespace.clone())
            .await
            .context("create relation state zset")?;
        let latest_handle = ZSetHandle {
            ns: namespace,
            version: 0,
        };
        Ok(RelationState {
            integrated,
            latest_handle,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use slatedb::Db;
    use slatedb::config::ScanOptions;

    use crate::collections::zset::SegmentRecord;
    use crate::storage::SlateTable;
    use crate::storage::dictionary::KeyIntern;

    use super::RelationState;

    async fn build_table(name: &str) -> Arc<dyn crate::storage::KeyValueTable> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open(name, store).await.expect("open SlateDB"));
        Arc::new(SlateTable::new(db))
    }

    #[tokio::test]
    async fn empty_state_starts_at_version_zero() {
        let table = build_table("relation-state-empty").await;
        let state = RelationState::<i64>::empty(table, "relation-state-empty".to_string())
            .await
            .expect("create empty relation state");
        assert_eq!(state.latest_handle.version, 0);
        assert!(
            state
                .integrated
                .materialize()
                .await
                .expect("materialize empty state")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn update_handle_preserves_versioned_state_semantics() {
        let table = build_table("relation-state-update").await;
        let mut state = RelationState::<i64>::empty(table, "relation-state-update".to_string())
            .await
            .expect("create relation state");
        let key_id = state
            .dictionary()
            .intern(&42)
            .await
            .expect("intern state key");
        let version = state
            .integrated
            .create_version(vec![SegmentRecord {
                id: 0,
                bucket: 0,
                deltas: vec![(key_id, 1)],
            }])
            .await
            .expect("create version");
        let handle = state.integrated.handle_for_version(version);
        state.update_handle(handle.clone());
        assert_eq!(state.latest_handle.version, handle.version);

        let materialized = state
            .integrated
            .materialize()
            .await
            .expect("materialize integrated state");
        assert_eq!(materialized.get(&42).copied(), Some(1));

        let dict_keys = state
            .integrated
            .table()
            .scan_prefix(b"dict/relation-state-update/", &ScanOptions::default())
            .await
            .expect("scan dictionary keys");
        assert!(
            !dict_keys.is_empty(),
            "dictionary state should stay on KV dictionary prefixes"
        );

        let manifest_keys = state
            .integrated
            .table()
            .scan_prefix(
                b"zset/relation-state-update/manifest/",
                &ScanOptions::default(),
            )
            .await
            .expect("scan manifest keys");
        assert!(
            !manifest_keys.is_empty(),
            "versioned stream state should stay on KV manifest prefixes"
        );
    }
}
