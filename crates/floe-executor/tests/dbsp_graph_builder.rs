use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::Utc;
use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, StringArray, TimestampMillisecondArray,
};
use datafusion::common::Column;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::{TableProvider, empty::EmptyTable};
use datafusion::functions_aggregate::expr_fn::{avg, count, count_distinct, max, min, sum};
use datafusion::logical_expr::expr_fn::ExprFunctionExt;
use datafusion::logical_expr::expr_fn::create_udf;
use datafusion::logical_expr::{
    ColumnarValue, Expr, JoinType, ScalarFunctionImplementation, Volatility, col, lit, table_scan,
};
use datafusion::prelude::SessionContext;
use dbsp::StreamRetention;
use dbsp::handles::ZSetHandle;
use floe_executor::GraphTaskError;
use floe_executor::MaterializedViewRegistry;
use floe_executor::dbsp_bridge::DbspBridge;
use floe_executor::dbsp_graph_builder::{
    BuildInputs, DbspGraphBuilder, source_batch_journal_root_sources,
};
use floe_executor::dbsp_plan::{
    DbspPlanBuilder, nexmark_auction_table, nexmark_bid_table, nexmark_config,
    nexmark_person_table, validate_dbsp_plan,
};
use floe_executor::encoding::{EncodedRowScalar, decode_all_encoded_row_scalars};
use floe_executor::outer_stream::OuterStreamRegistry;
use object_store::memory::InMemory;
use regex::Regex;
use slatedb::Db;
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;

#[path = "dbsp_graph_builder/row_support.rs"]
mod row_support;
#[path = "dbsp_graph_builder/support.rs"]
mod support;
#[path = "dbsp_graph_builder/window_support.rs"]
mod window_support;

use row_support::*;
use support::*;
use window_support::*;

#[path = "dbsp_graph_builder/basic_cases.rs"]
mod basic_cases;
#[path = "dbsp_graph_builder/distinct_cases.rs"]
mod distinct_cases;
#[path = "dbsp_graph_builder/join_cases.rs"]
mod join_cases;
#[path = "dbsp_graph_builder/join_filter_cases.rs"]
mod join_filter_cases;
#[path = "dbsp_graph_builder/projection_cases.rs"]
mod projection_cases;
#[path = "dbsp_graph_builder/recovery_failure_cases.rs"]
mod recovery_failure_cases;
#[path = "dbsp_graph_builder/topn_cases.rs"]
mod topn_cases;
#[path = "dbsp_graph_builder/transient_join_cases.rs"]
mod transient_join_cases;
#[path = "dbsp_graph_builder/window_cases.rs"]
mod window_cases;
