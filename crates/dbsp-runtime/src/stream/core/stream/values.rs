use anyhow::{Context, Result, anyhow};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

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
    pub async fn send(&mut self, element: T) -> Result<i64> {
        let next_timestamp = {
            let mut state = self.write_state();
            let next_timestamp = state.logical_timestamp + 1;
            if element != state.default {
                state.pending_data.insert(next_timestamp, element.clone());
                state.data_cache.insert(next_timestamp, element);
                state.identity = false;
            }
            state.logical_timestamp = next_timestamp;
            state.pending_state = true;
            next_timestamp
        };
        Ok(next_timestamp)
    }

    pub async fn set_default(&mut self, new_default: T) -> Result<()> {
        let mut state = self.write_state();
        let current_ts = state.logical_timestamp;
        state.default = new_default.clone();
        state.pending_defaults.insert(current_ts, new_default);
        state.pending_state = true;
        Ok(())
    }

    pub async fn get(&mut self, timestamp: i64) -> Result<T> {
        if timestamp < 0 {
            return Err(anyhow!("timestamp cannot be negative"));
        }

        loop {
            let mut fetch_key: Option<Vec<u8>> = None;
            let mut fallback_value: Option<T> = None;
            let mut needs_advance = false;

            {
                let state = self.read_state();
                if timestamp > state.logical_timestamp {
                    needs_advance = true;
                } else if let Some(value) = state.pending_data.get(&timestamp) {
                    return Ok(value.clone());
                } else if let Some(value) = state.data_cache.get(&timestamp) {
                    return Ok(value.clone());
                } else {
                    fetch_key = Some(self.core.encode_data_key(timestamp)?);
                    fallback_value = Some(state.default_at(timestamp));
                }
            }

            if needs_advance {
                self.advance_to(timestamp).await?;
                continue;
            }

            if let Some(key) = fetch_key {
                if let Some(bytes) = self.core.table.get(&key).await? {
                    let value: T =
                        encoding::decode(&bytes).context("unable to decode stream value")?;
                    {
                        let mut state = self.write_state();
                        state.data_cache.insert(timestamp, value.clone());
                    }
                    return Ok(value);
                } else if let Some(default_value) = fallback_value {
                    return Ok(default_value);
                }
            }
        }
    }

    pub async fn latest(&mut self) -> Result<T> {
        self.get(self.current_time()).await
    }

    pub async fn latest_with_ts(&mut self) -> Result<(i64, T)> {
        let ts = self.current_time();
        let value = self.get(ts).await?;
        Ok((ts, value))
    }

    pub async fn to_vec(&mut self) -> Result<Vec<T>> {
        let frontier = self.current_time();
        let mut values = Vec::with_capacity((frontier + 1) as usize);
        for t in 0..=frontier {
            values.push(self.get(t).await?);
        }
        Ok(values)
    }

    pub async fn advance_to(&mut self, timestamp: i64) -> Result<()> {
        loop {
            let current = self.current_time();
            if current >= timestamp {
                break;
            }
            let default = { self.read_state().default.clone() };
            self.send(default).await?;
        }
        Ok(())
    }

    pub(crate) fn set_default_in_place(&self, value: T) {
        let mut state = self.write_state();
        let current_ts = state.logical_timestamp;
        state.default = value.clone();
        state.pending_defaults.insert(current_ts, value);
        state.pending_state = true;
    }

    pub(crate) fn push_value_in_place(&self, value: T) {
        let mut state = self.write_state();
        let next_timestamp = state.logical_timestamp + 1;
        if value != state.default {
            state.pending_data.insert(next_timestamp, value.clone());
            state.data_cache.insert(next_timestamp, value);
            state.identity = false;
        }
        state.logical_timestamp = next_timestamp;
        state.pending_state = true;
    }
}
