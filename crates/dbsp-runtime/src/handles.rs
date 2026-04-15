use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use anyhow::Result;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::collections::zset::VersionedZSet;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::util::transient_zset_batch;

/// Lightweight reference to a materialized version of a versioned ZSet.
///
/// Stream rows store instances of this handle rather than embedding full collections.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ZSetHandle {
    /// Logical namespace for the referenced versioned ZSet.
    pub ns: String,
    /// Concrete version number within the namespace.
    pub version: u64,
}

/// Handle that references another stream at a particular committed frontier.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct StreamHandle {
    /// Namespace of the nested stream.
    pub ns: String,
    /// Committed frontier to read from (inclusive).
    pub frontier: i64,
}

/// Lazy view onto the contents of a handle-backed ZSet.
pub struct ZSetHandleView<K>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    dict: Arc<Dictionary<K>>,
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    version: u64,
}

impl<K> ZSetHandleView<K>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'rk> RkyvSerialize<RkyvSerializer<'rk>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub fn new(
        dict: Arc<Dictionary<K>>,
        table: Arc<dyn KeyValueTable>,
        namespace: impl Into<String>,
        version: u64,
    ) -> Self {
        Self {
            dict,
            table,
            namespace: namespace.into(),
            version,
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn into_parts(self) -> (Arc<Dictionary<K>>, Arc<dyn KeyValueTable>, String, u64) {
        (self.dict, self.table, self.namespace, self.version)
    }

    pub async fn materialize(&self) -> Result<HashMap<K, i64>> {
        let handle = ZSetHandle {
            ns: self.namespace.clone(),
            version: self.version,
        };
        if let Some(batch) = transient_zset_batch::<K>(&handle) {
            let mut materialized: HashMap<K, i64> = HashMap::with_capacity(batch.len());
            for (key, delta) in batch.as_ref() {
                let next = materialized
                    .get(key)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(*delta);
                if next == 0 {
                    materialized.remove(key);
                } else {
                    materialized.insert(key.clone(), next);
                }
            }
            return Ok(materialized);
        }

        let span = tracing::debug_span!(
            "materialize",
            namespace = %self.namespace,
            version = self.version
        );
        let _enter = span.enter();
        let versioned = VersionedZSet::open_for_handle(
            self.dict.clone(),
            self.table.clone(),
            self.namespace.clone(),
            self.version,
        )
        .await?;
        versioned.load_existing_version(self.version).await
    }

    pub async fn delta_iter(&self) -> Result<Vec<(K, i64)>> {
        let handle = ZSetHandle {
            ns: self.namespace.clone(),
            version: self.version,
        };
        if let Some(batch) = transient_zset_batch::<K>(&handle) {
            return Ok(batch.as_ref().clone());
        }

        let span = tracing::debug_span!(
            "delta_iter",
            namespace = %self.namespace,
            version = self.version
        );
        let _enter = span.enter();
        let versioned = VersionedZSet::open_for_handle(
            self.dict.clone(),
            self.table.clone(),
            self.namespace.clone(),
            self.version,
        )
        .await?;
        versioned.delta_iter_with_dict(self.version).await
    }
}
