#![allow(
    clippy::collapsible_if,
    clippy::collapsible_else_if,
    clippy::collapsible_match,
    clippy::match_like_matches_macro,
    clippy::needless_borrow,
    clippy::nonminimal_bool,
    clippy::overly_complex_bool_expr,
    clippy::ptr_arg,
    clippy::type_complexity,
    clippy::unused_enumerate_index
)]

pub mod checkpoint;
pub mod codec;
pub mod context;
pub mod dbsp_bridge;
pub mod dbsp_graph_builder;
pub mod dbsp_plan;
#[cfg(test)]
mod dbsp_table_environment;
pub mod delta_batch;
pub mod delta_consolidation;
pub mod encoding;
mod metrics;
pub mod mv;
pub mod mv_changelog;
pub mod mv_loader;
pub mod namespaces;
pub mod operator_state;
pub mod outer_stream;
mod scalar_array_builder;
pub mod source_decoder;
pub mod source_journal;
pub mod stream_types;
pub mod subscribe;
pub mod table_provider;
pub mod task_events;
pub mod vectorized_keys;
pub mod vectorized_runtime;

pub use context::FloeQueryContext;
pub use dbsp_bridge::{DbspBridge, NamespaceStorageSummary};
pub use dbsp_graph_builder::{
    BuildInputs, BuildOutputs, DbspGraphBuilder, MvFlushCoalescingConfig, OverlaySnapshotConfig,
    PersistencePolicyConfig, PlanSourceRequirements, plan_source_requirements,
    source_batch_journal_root_source_name, source_batch_journal_root_sources,
    source_batch_journal_root_sources_with_config, transient_source_root_requirements,
};
pub use dbsp_plan::{DbspPlanBuilder, ValidatedPlan, nexmark_config, validate_dbsp_plan};
pub use delta_batch::{DeltaBatchBuffer, DeltaBatchConfig, FlushReason};
pub use floe_core::source::SourceRegistry;
pub use mv::registry::{MaterializedViewHandle, MaterializedViewRegistry};
pub use mv::runtime::MaterializedView;
pub use mv_loader::load_or_register_mv;
pub use operator_state::OperatorStateHandle;
pub use outer_stream::OuterStreamRegistry;
pub use source_decoder::{SourceArrowBatchBuilder, SourceRowDecoder};
pub use stream_types::{Diff, Timestamp};
pub use subscribe::SubscribeExecutionConfig;
pub use table_provider::{MaterializedViewTableProvider, SlateTableProvider};
pub use task_events::{
    GRAPH_TASK_EVENT_CHANNEL_CAPACITY, GraphTaskError, GraphTaskReceiver, GraphTaskSender,
};
pub use vectorized_runtime::{
    VectorizedExecutionRuntime, VectorizedMaterializedViewPlan, weighted_batch_from_diffs,
};
