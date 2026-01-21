mod filters;
mod helpers;
mod materialized_view;
mod slate;
mod source;

#[cfg(test)]
mod tests;

const MV_VERSION_COLUMN: &str = "__mv_version";

pub use materialized_view::MaterializedViewTableProvider;
pub use slate::SlateTableProvider;
pub use source::SourceTableProvider;
