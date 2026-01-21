use std::collections::BTreeMap;

use anyhow::{Context, Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use slatedb::WriteBatch;

use crate::storage::encoding;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::Stream;

impl<T> Stream<T>
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
    pub async fn flush(&mut self) -> Result<()> {
        let mut batch = WriteBatch::new();
        let mut dirty = false;
        if self.flush_defaults_into(&mut batch)? {
            dirty = true;
        }
        if self.flush_data_into(&mut batch)? {
            dirty = true;
        }
        let committed_ts = self.flush_state_into(&mut batch)?;
        if committed_ts.is_some() {
            dirty = true;
        }

        if dirty {
            let committed_ts =
                committed_ts.ok_or_else(|| anyhow!("stream flush missing committed timestamp"))?;
            let intent_key = self.encode_intent_key();
            batch.put(intent_key.clone(), vec![1]);
            self.core.table.write_batch(batch).await?;

            let mut cleanup = WriteBatch::new();
            cleanup.delete(intent_key);
            self.core.table.write_batch(cleanup).await?;

            self.notify_committed_frontier(committed_ts);
        }

        {
            let mut state = self.write_state();
            state.pending_state = false;
        }
        Ok(())
    }

    pub(crate) fn flush_data_into(&mut self, batch: &mut WriteBatch) -> Result<bool> {
        let pending = {
            let mut state = self.write_state();
            if state.pending_data.is_empty() {
                return Ok(false);
            }
            let mut pending_map = BTreeMap::new();
            std::mem::swap(&mut pending_map, &mut state.pending_data);
            pending_map
        };

        for (timestamp, value) in pending {
            let key = self.core.encode_data_key(timestamp)?;
            let encoded = encoding::encode(&value).context("unable to encode stream value")?;
            batch.put(key, encoded);
        }

        Ok(true)
    }

    pub(crate) fn flush_defaults_into(&mut self, batch: &mut WriteBatch) -> Result<bool> {
        let pending = {
            let mut state = self.write_state();
            if state.pending_defaults.is_empty() {
                return Ok(false);
            }
            let mut pending_map = BTreeMap::new();
            std::mem::swap(&mut pending_map, &mut state.pending_defaults);
            pending_map
        };

        for (timestamp, value) in pending {
            let key = self.core.encode_default_key(timestamp)?;
            let encoded = encoding::encode(&value).context("unable to encode default change")?;
            batch.put(key, encoded);
            let mut state = self.write_state();
            state.default_changes.insert(timestamp, value);
            state.last_default_ts = state.last_default_ts.max(timestamp);
        }

        Ok(true)
    }

    pub(crate) fn flush_state_into(&mut self, batch: &mut WriteBatch) -> Result<Option<i64>> {
        let snapshot = {
            let state = self.read_state();
            if !state.pending_state {
                return Ok(None);
            }
            (
                state.logical_timestamp,
                state.identity,
                state.default.clone(),
                state.last_default_ts,
            )
        };
        let encoded = encoding::encode(&snapshot).context("unable to encode stream state")?;
        batch.put(self.core.state_key.clone(), encoded);
        Ok(Some(snapshot.0))
    }
}
