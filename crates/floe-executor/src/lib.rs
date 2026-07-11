pub mod checkpoint;
mod columnar_snapshot;
mod context;
pub mod dbsp_bridge;
pub mod dbsp_plan;
pub mod delta_consolidation;
pub mod encoding;
mod maintenance;
mod metrics;
pub mod mv;
pub mod mv_changelog;
pub mod mv_loader;
pub mod namespaces;
mod operator_state;
mod scalar_array_builder;
pub mod source_decoder;
pub mod source_journal;
mod source_requirements;
pub mod stream_types;
pub mod subscribe;
mod table_provider;
mod vectorized_runtime;
mod vectorized_source_delta;

pub use context::FloeQueryContext;
pub use dbsp_bridge::{DbspBridge, NamespaceStorageSummary};
pub use dbsp_plan::{DbspPlanBuilder, ValidatedPlan, nexmark_config, validate_dbsp_plan};
pub use floe_core::source::SourceRegistry;
pub use maintenance::DbspMaintenance;
pub use mv::registry::{MaterializedViewHandle, MaterializedViewRegistry};
pub use mv::runtime::MaterializedView;
pub use mv_loader::load_or_register_mv;
pub use operator_state::OperatorStateHandle;
pub use source_decoder::{SourceArrowBatchBuilder, SourceArrowBatchMode, SourceArrowBatches};
pub use source_requirements::{PlanSourceRequirements, plan_source_requirements};
pub use stream_types::{Diff, Timestamp};
pub use subscribe::SubscribeExecutionConfig;
pub use table_provider::{MaterializedViewTableProvider, SlateTableProvider};
pub use vectorized_runtime::{
    VectorizedExecutionRuntime, VectorizedExecutionRuntimeOptions,
    VectorizedMaterializedViewExecutionPolicy, VectorizedMaterializedViewPlan,
    print_columnar_phase_profile, reset_columnar_phase_profile, weighted_batch_from_diffs,
};
