pub mod dictionary;
pub mod encoding;
pub mod gc;
pub mod keyspace;
pub mod manifest;
pub mod segment;
pub mod segment_compaction;
mod table;
pub mod timestamps;
pub use table::{KeyValueTable, SlateTable, prefix_bounds};

use slatedb::Error as SlateError;

pub(crate) fn map_slate_err(err: SlateError) -> anyhow::Error {
    anyhow::Error::new(err)
}
