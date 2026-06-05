mod compaction;
mod state;
mod storage;

use std::collections::BTreeMap;
use std::hash::Hash;
use std::sync::Arc;

use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

#[derive(Clone, Copy, Debug)]
pub struct CompactionPolicy {
    pub max_chain_len: usize,
    pub max_segments: usize,
    pub max_bucket_segments: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct VersionChainStats {
    pub version_count: usize,
    pub segment_count: usize,
    pub max_bucket_segment_count: usize,
}

pub type SegmentId = u64;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone)]
pub struct SegmentRecord {
    pub id: SegmentId,
    pub bucket: u16,
    pub deltas: Vec<(u64, i64)>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone)]
pub struct ZSetVersionManifest {
    pub base: Option<u64>,
    pub buckets: BTreeMap<u16, Vec<SegmentId>>,
    pub reference_count: u64,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy)]
pub(crate) struct ZSetVersionState {
    pub(crate) persisted_version: u64,
    pub(crate) next_segment_id: SegmentId,
}

#[derive(Clone)]
pub(crate) struct VersionWritePlan {
    pub(crate) version: u64,
    pub(crate) manifest: ZSetVersionManifest,
}

pub struct VersionedZSet<K>
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
    dict: Arc<Dictionary<K>>,
    table: Arc<dyn KeyValueTable>,
    namespace: String,
    manifest_prefix: Vec<u8>,
    segment_prefix: Vec<u8>,
    state_key: Vec<u8>,
    current_version: u64,
    persisted_version: u64,
    intent_key: Vec<u8>,
    manifest: Option<ZSetVersionManifest>,
    next_segment_id: SegmentId,
}
