pub mod circuit_builder;
pub mod context;
pub mod dataflow_plan;
pub mod dbsp_bridge;
pub mod encoding;
pub mod execution_loop;
pub mod expr_eval;
pub mod materialized_view;
pub mod operators;
pub mod query_planner;
pub mod source_decoder;
pub mod stream_types;
pub mod table_provider;

pub use circuit_builder::{
    Circuit, CircuitContext, ConnectedDetail, ConnectedOperator, RowStreamHandle, SourceRegistry,
};
pub use context::FloeQueryContext;
pub use dataflow_plan::{DataflowPlan, Expr, OperatorNode};
pub use execution_loop::{
    BuiltGraph, IngestedRow, ScanRuntime, TickLoop, build_graph, instantiate_tick_loop,
};
pub use materialized_view::{MaterializedViewHandle, MaterializedViewRegistry};
pub use operators::{
    FilterOperator, JoinOperator, MapOperator, MaterializeOperator, RowSink, ScanOperator,
};
pub use query_planner::QueryPlanner;
pub use source_decoder::SourceRowDecoder;
pub use stream_types::{Diff, InputPort, OperatorId, OutputPort, Row, StreamOperator, Timestamp};
pub use table_provider::{MaterializedViewTableProvider, SlateTableProvider};
