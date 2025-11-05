pub mod dictionary;
pub mod encoding;
pub mod keyspace;
mod table;
pub mod timestamps;
pub use table::{KeyValueTable, SlateTable};

use slatedb::Error as SlateError;

pub(crate) fn map_slate_err(err: SlateError) -> anyhow::Error {
    anyhow::Error::new(err)
}
