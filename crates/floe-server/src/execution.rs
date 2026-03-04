use std::sync::Arc;

use arrow_pg::datatypes::{arrow_schema_to_pg_fields, encode_recordbatch};
use floe_executor::dbsp_bridge::DbspBridge;
use floe_executor::{FloeQueryContext, MaterializedViewRegistry, load_or_register_mv};
use futures::stream;
use pgwire::api::portal::Format;
use pgwire::api::results::QueryResponse;
use pgwire::error::PgWireResult;
use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio::sync::Mutex;

use datafusion::arrow::record_batch::RecordBatch;

use super::sql::{extract_tables_from_query, is_system_catalog_relation, unqualified_table_name};
use super::{parse_error, undefined_table_error};
use crate::catalog_shim::refresh_catalog_shim;

pub(crate) struct FloeServerState {
    pub(crate) query: FloeQueryContext,
    pub(crate) materialized_views: Arc<MaterializedViewRegistry>,
    bridge: Arc<Mutex<DbspBridge>>,
}

impl FloeServerState {
    pub(crate) fn new(
        query: FloeQueryContext,
        materialized_views: Arc<MaterializedViewRegistry>,
        bridge: DbspBridge,
    ) -> Self {
        Self {
            query,
            materialized_views,
            bridge: Arc::new(Mutex::new(bridge)),
        }
    }

    pub(crate) async fn ensure_materialized_view_registered(&self, name: &str) -> PgWireResult<()> {
        let session = self.query.session();
        let mut bridge = self.bridge.lock().await;
        load_or_register_mv(
            &session,
            Arc::clone(&self.materialized_views),
            &mut bridge,
            name,
        )
        .await
        .map_err(|err| {
            undefined_table_error(format!(
                "materialized view '{name}' is not available: {err}"
            ))
        })
    }

    pub(crate) async fn ensure_materialized_views_in_sql(&self, sql: &str) -> PgWireResult<()> {
        let dialect = PostgreSqlDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .map_err(|err| parse_error(format!("SQL parse error: {err}")))?;
        for statement in statements {
            if let Statement::Query(query) = statement {
                let mut names = Vec::new();
                extract_tables_from_query(&query, &mut names);
                for view in names {
                    if is_system_catalog_relation(&view) {
                        continue;
                    }
                    self.ensure_materialized_view_registered(unqualified_table_name(&view))
                        .await?;
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn refresh_catalog_shims(&self) -> PgWireResult<()> {
        refresh_catalog_shim(self).await
    }
}

pub(crate) fn build_query_response(batches: Vec<RecordBatch>) -> PgWireResult<QueryResponse> {
    if batches.is_empty() {
        let schema = Arc::new(Vec::new());
        let rows = stream::iter(Vec::<PgWireResult<_>>::new());
        return Ok(QueryResponse::new(schema, rows));
    }

    let info = Arc::new(arrow_schema_to_pg_fields(
        batches[0].schema().as_ref(),
        &Format::UnifiedText,
        None,
    )?);
    let fields = Arc::clone(&info);
    let row_stream = stream::iter(
        batches
            .into_iter()
            .flat_map(move |batch| encode_recordbatch(Arc::clone(&fields), batch)),
    );

    Ok(QueryResponse::new(info, row_stream))
}
