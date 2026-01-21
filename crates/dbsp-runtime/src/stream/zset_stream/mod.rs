mod core;
mod handles;

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::collections::zset::{CompactionPolicy, VersionedZSet};
use crate::handles::ZSetHandle;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::core::stream::Stream;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamRetention {
    None,
    KeepLast { keep_last: usize },
    AllButLatest,
}

impl StreamRetention {
    fn window_size(self) -> Option<usize> {
        match self {
            StreamRetention::None => None,
            StreamRetention::KeepLast { keep_last } if keep_last > 0 => Some(keep_last),
            StreamRetention::KeepLast { .. } => None,
            StreamRetention::AllButLatest => Some(1),
        }
    }
}

pub struct ZSetStream<K>
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
    pub(crate) stream: Stream<ZSetHandle>,
    delta_stream: Stream<ZSetHandle>,
    versioned: VersionedZSet<K>,
    delta_versioned: VersionedZSet<K>,
    overlay: HashMap<K, i64>,
    retention: StreamRetention,
    compaction: CompactionPolicy,
    retention_window: VecDeque<ZSetHandle>,
    retention_counts: HashMap<u64, usize>,
    current_handle: ZSetHandle,
    delta_retention_window: VecDeque<ZSetHandle>,
    delta_retention_counts: HashMap<u64, usize>,
    delta_current_handle: ZSetHandle,
}
