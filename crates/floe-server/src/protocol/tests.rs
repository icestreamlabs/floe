use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::{Buf, Bytes};
use datafusion::scalar::ScalarValue;
use floe_executor::dbsp_bridge::DbspBridge;
use floe_executor::encoding::encode_projected_row_key;
use floe_executor::materialized_view::DbspPersistedState;
use floe_executor::{
    FloeQueryContext, MaterializedViewRegistry, MaterializedViewTableProvider,
};
use floe_storage::SlateCatalog;
use futures::StreamExt;
use pgwire::api::portal::Portal;
use pgwire::api::results::Response;
use pgwire::api::stmt::StoredStatement;
use pgwire::messages::data::DataRow;
use pgwire::messages::extendedquery::Bind;
use postgres_types::Type as PgType;
use slatedb::Db;
use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use super::extended::{FloeExtendedHandler, FloeExtendedQueryParser};
use super::simple::FloeQueryHandler;
use crate::execution::FloeServerState;

async fn state_with_single_mv() -> Arc<FloeServerState> {
    let catalog = Arc::new(SlateCatalog::in_memory().await.expect("catalog"));
    let query = FloeQueryContext::new(Arc::clone(&catalog));
    let registry = Arc::new(MaterializedViewRegistry::new());
    registry.register("mv_test");
    registry.set_schema(
        "mv_test",
        Arc::new(Schema::new(vec![Field::new(
            "auction",
            DataType::Int64,
            true,
        )])),
    );
    let bridge = DbspBridge::new(catalog.db()).await.expect("bridge");
    Arc::new(FloeServerState::new(query, registry, bridge))
}

#[tokio::test]
async fn registers_materialized_view_on_select() {
    let catalog = Arc::new(SlateCatalog::in_memory().await.expect("catalog"));
    let query = FloeQueryContext::new(Arc::clone(&catalog));
    let registry = Arc::new(MaterializedViewRegistry::new());
    registry.register("mv_test");
    registry.set_schema(
        "mv_test",
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
    );

    let bridge = DbspBridge::new(catalog.db()).await.expect("bridge");
    let state = Arc::new(FloeServerState::new(
        query.clone(),
        Arc::clone(&registry),
        bridge,
    ));
    let handler = FloeQueryHandler::new(state);

    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, "SELECT * FROM mv_test").expect("parse");
    let Statement::Query(query_stmt) = &statements[0] else {
        panic!("expected query");
    };
    handler
        .ensure_materialized_views_in_query(query_stmt)
        .await
        .expect("ensure mv");

    assert!(query.session().table("mv_test").await.is_ok());
}

#[tokio::test]
async fn rejects_unknown_table_in_select() {
    let catalog = Arc::new(SlateCatalog::in_memory().await.expect("catalog"));
    let query = FloeQueryContext::new(Arc::clone(&catalog));
    let registry = Arc::new(MaterializedViewRegistry::new());
    let bridge = DbspBridge::new(catalog.db()).await.expect("bridge");
    let state = Arc::new(FloeServerState::new(query, Arc::clone(&registry), bridge));
    let handler = FloeQueryHandler::new(state);

    let dialect = PostgreSqlDialect {};
    let statements =
        Parser::parse_sql(&dialect, "SELECT * FROM missing_mv").expect("parse statement");
    let Statement::Query(query_stmt) = &statements[0] else {
        panic!("expected query");
    };
    let err = handler
        .ensure_materialized_views_in_query(query_stmt)
        .await
        .expect_err("expected error");
    assert!(
        err.to_string()
            .contains("materialized view 'missing_mv' is not available")
    );
}

#[tokio::test]
async fn rejects_create_table_over_pgwire() {
    let catalog = Arc::new(SlateCatalog::in_memory().await.expect("catalog"));
    let query = FloeQueryContext::new(Arc::clone(&catalog));
    let registry = Arc::new(MaterializedViewRegistry::new());
    let bridge = DbspBridge::new(catalog.db()).await.expect("bridge");
    let state = Arc::new(FloeServerState::new(query, registry, bridge));
    let handler = FloeQueryHandler::new(state);

    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, "CREATE TABLE t (id INT)").expect("parse");
    let statement = statements.pop().expect("statement");
    let err = match handler.execute_statement(statement).await {
        Ok(_) => panic!("expected CREATE TABLE to be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("CREATE TABLE is not supported via the Floe pgwire endpoint")
    );
}

#[tokio::test]
async fn rejects_insert_over_pgwire() {
    let catalog = Arc::new(SlateCatalog::in_memory().await.expect("catalog"));
    let query = FloeQueryContext::new(Arc::clone(&catalog));
    let registry = Arc::new(MaterializedViewRegistry::new());
    let bridge = DbspBridge::new(catalog.db()).await.expect("bridge");
    let state = Arc::new(FloeServerState::new(query, registry, bridge));
    let handler = FloeQueryHandler::new(state);

    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, "INSERT INTO t VALUES (1)").expect("parse");
    let statement = statements.pop().expect("statement");
    let err = match handler.execute_statement(statement).await {
        Ok(_) => panic!("expected INSERT to be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("INSERT is not supported; materialized views are read-only")
    );
}

#[tokio::test]
async fn extended_parser_rejects_non_select() {
    let state = state_with_single_mv().await;
    let parser = FloeExtendedQueryParser::new(state);
    let err = parser
        .prepare_statement("INSERT INTO mv_test VALUES (1)", &[])
        .await
        .expect_err("expected rejection");
    assert!(
        err.to_string()
            .contains("only SELECT statements are supported")
    );
}

#[tokio::test]
async fn extended_parser_tracks_referenced_mvs_and_parameters() {
    let state = state_with_single_mv().await;
    let parser = FloeExtendedQueryParser::new(Arc::clone(&state));
    let prepared = parser
        .prepare_statement("SELECT * FROM mv_test WHERE auction > $1", &[])
        .await
        .expect("prepared");
    assert_eq!(prepared.parameter_count(), 1);
    assert_eq!(prepared.referenced_views(), &["mv_test"]);
}

#[tokio::test]
async fn extended_handler_renders_bound_sql() {
    let state = state_with_single_mv().await;
    let parser = FloeExtendedQueryParser::new(Arc::clone(&state));
    let prepared = parser
        .prepare_statement("SELECT auction FROM mv_test WHERE auction > $1", &[])
        .await
        .expect("prepared");
    let stored = Arc::new(StoredStatement::new(
        "stmt".into(),
        prepared.clone(),
        vec![PgType::INT8],
    ));
    let bind = Bind::new(
        Some("portal".into()),
        Some("stmt".into()),
        vec![0],
        vec![Some(Bytes::from("100"))],
        vec![0],
    );
    let portal = Portal::try_new(&bind, Arc::clone(&stored)).expect("portal");
    let handler = FloeExtendedHandler::new(state);
    let sql = handler.render_portal_sql(&portal).expect("rendered SQL");
    assert!(
        sql.contains("WHERE auction > 100"),
        "unexpected rendered SQL: {sql}"
    );
}

#[tokio::test]
async fn streaming_execute_respects_mv_version_filter() {
    let (state, versions) = streaming_state_with_rows(&[10, 20]).await;
    let handler = FloeQueryHandler::new(state);
    let version_literal = versions[0];
    let sql = format!(
        "SELECT value, __mv_version FROM {view} WHERE __mv_version = {version} ORDER BY value",
        view = STREAM_VIEW_NAME,
        version = version_literal
    );
    let response = handler
        .execute_sql_streaming(&sql)
        .await
        .expect("streaming query");
    let Response::Query(mut query) = response else {
        panic!("expected query response");
    };

    let schema = query.row_schema();
    let field_names: Vec<_> = schema
        .iter()
        .map(|field| field.name().to_string())
        .collect();
    assert_eq!(
        field_names,
        vec!["value".to_string(), "__mv_version".to_string()]
    );

    let rows_stream = query.data_rows();
    let mut rows = Vec::new();
    while let Some(row) = rows_stream.next().await {
        let row = row.expect("data row");
        rows.push(decode_text_row(row));
    }
    assert_eq!(
        rows.len(),
        1,
        "only the requested version should be streamed"
    );
    assert_eq!(rows[0][0].as_deref(), Some("10"));
    assert_eq!(
        rows[0][1].as_deref(),
        Some(version_literal.to_string().as_str())
    );
}

const STREAM_VIEW_NAME: &str = "mv_stream_filter";

async fn streaming_state_with_rows(rows: &[i64]) -> (Arc<FloeServerState>, Vec<u64>) {
    let catalog = Arc::new(SlateCatalog::in_memory().await.expect("catalog"));
    let query = FloeQueryContext::new(Arc::clone(&catalog));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let db = catalog.db();
    let (dbsp_state, versions) =
        seed_mv_state(Arc::clone(&db), rows, Arc::clone(&schema)).await;
    let registry = Arc::new(MaterializedViewRegistry::new());
    registry.set_schema(STREAM_VIEW_NAME.to_string(), Arc::clone(&schema));
    let handle = registry.register(STREAM_VIEW_NAME.to_string());
    handle.set_dbsp_state(dbsp_state);
    let provider = MaterializedViewTableProvider::new(
        Arc::clone(&registry),
        STREAM_VIEW_NAME.to_string(),
        schema,
    );
    query
        .session()
        .register_table(STREAM_VIEW_NAME, Arc::new(provider))
        .expect("register mv provider");
    let bridge = DbspBridge::new(db).await.expect("bridge");
    let state = FloeServerState::new(query, registry, bridge);
    (Arc::new(state), versions)
}

async fn seed_mv_state(
    db: Arc<Db>,
    rows: &[i64],
    schema: SchemaRef,
) -> (DbspPersistedState, Vec<u64>) {
    let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");
    let mut view = bridge
        .new_view(STREAM_VIEW_NAME)
        .await
        .expect("create view");
    let mut versions = Vec::new();
    for value in rows {
        let key = encode_projected_row_key(&[ScalarValue::Int64(Some(*value))])
            .expect("encode row key");
        view.add_delta(key, 1);
        let handle = view.flush().await.expect("flush view");
        versions.push(handle.version);
    }
    bridge
        .save_mv_schema(STREAM_VIEW_NAME, Arc::clone(&schema))
        .await
        .expect("persist schema");
    let handle_view = view.latest_handle_view();
    let (dict, table, namespace, version) = handle_view.into_parts();
    (
        DbspPersistedState::new(dict, table, namespace, version),
        versions,
    )
}

fn decode_text_row(mut row: DataRow) -> Vec<Option<String>> {
    let mut values = Vec::with_capacity(row.field_count as usize);
    for _ in 0..row.field_count {
        let len = row.data.get_i32();
        if len < 0 {
            values.push(None);
        } else {
            let bytes = row.data.split_to(len as usize);
            values.push(Some(String::from_utf8(bytes.to_vec()).expect("utf8 value")));
        }
    }
    values
}
