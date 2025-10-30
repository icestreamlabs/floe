pub mod encoding;
mod table;
pub use table::{KeyValueTable, SlateTable};

use slatedb::Error as SlateError;

pub(crate) fn map_slate_err(err: SlateError) -> anyhow::Error {
    anyhow::Error::new(err)
}
