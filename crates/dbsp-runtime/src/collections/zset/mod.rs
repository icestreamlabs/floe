mod base;
mod versioned;

pub use base::ZSet;
pub(crate) use versioned::VersionWritePlan;
pub use versioned::{
    CompactionPolicy, SegmentId, SegmentRecord, VersionChainStats, VersionedZSet,
    ZSetVersionManifest,
};

const ZSET_PREFIX: &str = "zset/";

#[cfg(test)]
fn prefix_bounds(prefix: &[u8]) -> Range<Vec<u8>> {
    let mut end = prefix.to_vec();
    end.push(0xFF);
    prefix.to_vec()..end
}

#[cfg(test)]
use std::ops::Range;
