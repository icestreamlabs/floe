use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;
use slatedb::config::ScanOptions;

use crate::storage::encoding::{self, RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::storage::timestamps;

use super::{StreamCore, StreamState};

impl<T> StreamState<T>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub(super) fn new(default: T) -> Self {
        Self {
            logical_timestamp: 0,
            identity: true,
            default,
            pending_data: BTreeMap::new(),
            pending_defaults: BTreeMap::new(),
            pending_state: false,
            data_cache: HashMap::new(),
            default_changes: BTreeMap::new(),
            last_default_ts: 0,
        }
    }

    pub(super) fn default_at(&self, timestamp: i64) -> T {
        if let Some((_, value)) = self.pending_defaults.range(..=timestamp).next_back() {
            return value.clone();
        }

        if let Some((_, value)) = self.default_changes.range(..=timestamp).next_back() {
            return value.clone();
        }

        self.default.clone()
    }
}

impl<T> StreamCore<T>
where
    T: Archive
        + Clone
        + PartialEq
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub(super) fn encode_data_key(&self, timestamp: i64) -> Result<Vec<u8>> {
        timestamps::append(self.data_prefix.as_slice(), timestamp)
    }

    pub(super) fn encode_default_key(&self, timestamp: i64) -> Result<Vec<u8>> {
        timestamps::append(self.default_prefix.as_slice(), timestamp)
    }

    pub(super) fn encode_intent_key(&self) -> Vec<u8> {
        let mut key = self.state_key.clone();
        key.extend_from_slice(b"/intent");
        key
    }

    pub(super) async fn clear_intent(&self) -> Result<()> {
        let intent_key = self.encode_intent_key();
        if self.table.get_bytes(&intent_key).await?.is_some() {
            let mut batch = WriteBatch::new();
            batch.delete(intent_key);
            self.table.write_batch(batch).await?;
        }
        Ok(())
    }

    pub(super) async fn load_default_changes(&self) -> Result<BTreeMap<i64, T>> {
        let entries = self
            .table
            .scan_prefix_bytes(self.default_prefix.as_slice(), &ScanOptions::default())
            .await?;

        let mut changes = BTreeMap::new();
        for (key_bytes, value_bytes) in entries {
            let timestamp =
                timestamps::extract(self.default_prefix.as_slice(), key_bytes.as_ref())?;
            let value: T = encoding::decode(value_bytes.as_ref())
                .context("unable to decode default change")?;
            changes.insert(timestamp, value);
        }
        Ok(changes)
    }
}
