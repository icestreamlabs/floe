mod base;
mod derived;
mod versioned;

use std::ops::Range;

pub use base::ZSet;
pub use derived::{h, join, project, select};
pub use versioned::{
    CompactionPolicy, SegmentId, SegmentRecord, VersionChainStats, VersionedZSet,
    ZSetVersionManifest,
};

const ZSET_PREFIX: &str = "zset/";

fn prefix_bounds(prefix: &[u8]) -> Range<Vec<u8>> {
    let mut end = prefix.to_vec();
    end.push(0xFF);
    prefix.to_vec()..end
}
