use std::sync::Arc;

use arrow_pg::datatypes::{arrow_schema_to_pg_fields, encode_recordbatch};
use floe_executor::dbsp_bridge::DbspBridge;
use floe_executor::{FloeQueryContext, MaterializedViewRegistry, load_or_register_mv};
use futures::stream;
use pgwire::api::portal::Format;
use pgwire::api::results::QueryResponse;
use pgwire::error::PgWireResult;
use tokio::sync::Mutex;

use datafusion::arrow::record_batch::RecordBatch;

use super::sql::mv_identifiers_in_sql;
use super::user_error;

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
            user_error(format!(
                "materialized view '{name}' is not available: {err}"
            ))
        })
    }

    pub(crate) async fn ensure_materialized_views_in_sql(&self, sql: &str) -> PgWireResult<()> {
        for view in mv_identifiers_in_sql(sql) {
            self.ensure_materialized_view_registered(&view).await?;
        }
        Ok(())
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
