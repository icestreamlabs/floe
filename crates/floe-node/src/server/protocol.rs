use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::dataframe::DataFrame;
use floe_executor::tail::{execute_tail, parse_tail_sql, tail_output_schema};
use futures::Sink;
use pgwire::api::auth::StartupHandler;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, FieldFormat, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use postgres_types::Type as PgType;
use sqlparser::ast::{Query, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio_util::sync::CancellationToken;

use super::execution::{FloeServerState, build_query_response, build_query_response_stream};
use super::sql::{
    collect_placeholder_indices, decode_parameter_value, ensure_select_statement,
    extract_tables_from_query, substitute_placeholders,
};
use super::tail::{TailResponseStream, detect_single_tail_statement};
use super::types::arrow_schema_to_field_info;
use super::user_error;

#[derive(Clone, Debug)]
struct PreparedStatement {
    statement: Statement,
    result_fields: Arc<Vec<pgwire::api::results::FieldInfo>>,
    referenced_views: Vec<String>,
    parameter_count: usize,
    param_types: Vec<PgType>,
}

impl PreparedStatement {
    fn result_fields(&self) -> Arc<Vec<pgwire::api::results::FieldInfo>> {
        Arc::clone(&self.result_fields)
    }

    fn referenced_views(&self) -> &[String] {
        &self.referenced_views
    }

    fn parameter_count(&self) -> usize {
        self.parameter_count
    }

    fn parameter_types(&self) -> Vec<PgType> {
        self.param_types.clone()
    }
}

struct FloeExtendedQueryParser {
    state: Arc<FloeServerState>,
    dialect: PostgreSqlDialect,
}

impl FloeExtendedQueryParser {
    fn new(state: Arc<FloeServerState>) -> Self {
        Self {
            state,
            dialect: PostgreSqlDialect {},
        }
    }

    async fn prepare_statement(
        &self,
        sql: &str,
        parameter_types: &[PgType],
    ) -> PgWireResult<PreparedStatement> {
        let mut statements = Parser::parse_sql(&self.dialect, sql)
            .map_err(|err| user_error(format!("SQL parse error: {err}")))?;
        if statements.len() != 1 {
            return Err(user_error(
                "extended protocol supports a single statement per Parse",
            ));
        }
        let statement = statements
            .pop()
            .ok_or_else(|| user_error("expected statement"))?;
        ensure_select_statement(&statement)?;

        let mut referenced = Vec::new();
        if let Statement::Query(query) = &statement {
            extract_tables_from_query(query, &mut referenced);
        }
        let mut deduped = Vec::new();
        let mut seen = HashSet::new();
        for name in referenced {
            if seen.insert(name.clone()) {
                deduped.push(name);
            }
        }

        for view in &deduped {
            self.state.ensure_materialized_view_registered(view).await?;
        }

        self.state.ensure_materialized_views_in_sql(sql).await?;
        let dataframe = self
            .state
            .query
            .session()
            .sql(sql)
            .await
            .map_err(|err| user_error(format!("DataFusion planning error: {err}")))?;
        let df_schema_ref = dataframe.schema();
        let df_schema_owned = (*df_schema_ref).clone();
        let arrow_schema: Schema = df_schema_owned.into();
        let schema_ref: SchemaRef = Arc::new(arrow_schema);
        let fields = Arc::new(arrow_schema_to_field_info(&schema_ref)?);

        let placeholder_indices = collect_placeholder_indices(&statement)?;
        let parameter_count = placeholder_indices.iter().copied().max().unwrap_or(0);
        let mut bound_param_types = vec![PgType::UNKNOWN; parameter_count];
        for (idx, ty) in parameter_types.iter().enumerate() {
            if idx < bound_param_types.len() {
                bound_param_types[idx] = ty.clone();
            }
        }

        Ok(PreparedStatement {
            statement,
            result_fields: fields,
            referenced_views: deduped,
            parameter_count,
            param_types: bound_param_types,
        })
    }
}

#[async_trait]
impl QueryParser for FloeExtendedQueryParser {
    type Statement = PreparedStatement;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        parameter_types: &[PgType],
    ) -> PgWireResult<PreparedStatement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        self.prepare_statement(sql, parameter_types).await
    }
}

struct FloeExtendedHandler {
    state: Arc<FloeServerState>,
    parser: Arc<FloeExtendedQueryParser>,
}

impl FloeExtendedHandler {
    fn new(state: Arc<FloeServerState>) -> Self {
        Self {
            parser: Arc::new(FloeExtendedQueryParser::new(Arc::clone(&state))),
            state,
        }
    }

    fn render_portal_sql(&self, portal: &Portal<PreparedStatement>) -> PgWireResult<String> {
        render_sql_with_params(&portal.statement.statement, portal)
    }
}

#[async_trait]
impl ExtendedQueryHandler for FloeExtendedHandler {
    type Statement = PreparedStatement;
    type QueryParser = FloeExtendedQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::clone(&self.parser)
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let stmt = &target.statement;
        Ok(DescribeStatementResponse::new(
            stmt.parameter_types(),
            stmt.result_fields().as_ref().clone(),
        ))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(DescribePortalResponse::new(
            target.statement.statement.result_fields().as_ref().clone(),
        ))
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        for view in portal.statement.statement.referenced_views() {
            self.state.ensure_materialized_view_registered(view).await?;
        }

        let sql = self.render_portal_sql(portal)?;
        self.state.ensure_materialized_views_in_sql(&sql).await?;
        let dataframe = self
            .state
            .query
            .session()
            .sql(&sql)
            .await
            .map_err(|err| user_error(format!("DataFusion planning error: {err}")))?;
        let batches = dataframe
            .collect()
            .await
            .map_err(|err| user_error(format!("DataFusion execution error: {err}")))?;
        let response = build_query_response(batches)?;
        Ok(Response::Query(response))
    }
}

pub(super) struct FloeServerFactory {
    simple_handler: Arc<FloeQueryHandler>,
    extended_handler: Arc<FloeExtendedHandler>,
}

impl FloeServerFactory {
    pub(super) fn new(state: Arc<FloeServerState>) -> Self {
        let simple_state = Arc::clone(&state);
        Self {
            simple_handler: Arc::new(FloeQueryHandler::new(simple_state)),
            extended_handler: Arc::new(FloeExtendedHandler::new(state)),
        }
    }
}

impl PgWireServerHandlers for FloeServerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.simple_handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.extended_handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.simple_handler.clone()
    }
}

pub(super) struct FloeQueryHandler {
    state: Arc<FloeServerState>,
}

impl FloeQueryHandler {
    fn new(state: Arc<FloeServerState>) -> Self {
        Self { state }
    }

    async fn handle_select(&self, query: &Query) -> PgWireResult<Response> {
        self.ensure_materialized_views_in_query(query).await?;
        self.execute_sql_streaming(&query.to_string()).await
    }

    async fn execute_statement(&self, statement: Statement) -> PgWireResult<Response> {
        match statement {
            Statement::CreateTable(_) => Err(user_error(
                "CREATE TABLE is not supported via the Floe pgwire endpoint",
            )),
            Statement::Insert(_) => Err(user_error(
                "INSERT is not supported; materialized views are read-only",
            )),
            Statement::Query(query) => self.handle_select(&query).await,
            other => Err(user_error(format!("unsupported statement: {other}"))),
        }
    }

    async fn execute_tail_statement(&self, sql: &str) -> PgWireResult<Response> {
        let params =
            parse_tail_sql(sql).map_err(|err| user_error(format!("TAIL parse error: {err}")))?;
        self.state
            .ensure_materialized_view_registered(&params.mv_name)
            .await?;
        let schema = tail_output_schema(self.state.materialized_views.as_ref(), &params.mv_name)
            .map_err(|err| user_error(format!("TAIL schema error: {err}")))?;
        let fields = Arc::new(arrow_schema_to_field_info(&schema)?);
        let cancel = CancellationToken::new();
        let session = self.state.query.session();
        let tail_stream = execute_tail(
            &session,
            self.state.materialized_views.as_ref(),
            params,
            cancel.clone(),
        )
        .await
        .map_err(|err| user_error(format!("TAIL execution error: {err}")))?;
        let rows = TailResponseStream::new(fields.clone(), tail_stream, cancel);
        Ok(Response::Query(QueryResponse::new(fields, rows)))
    }

    async fn ensure_materialized_views_in_query(&self, query: &Query) -> PgWireResult<()> {
        let mut names = Vec::new();
        extract_tables_from_query(query, &mut names);
        for table in names {
            self.state
                .ensure_materialized_view_registered(&table)
                .await?;
        }
        Ok(())
    }

    async fn plan_sql(&self, sql: &str) -> PgWireResult<DataFrame> {
        self.state
            .query
            .session()
            .sql(sql)
            .await
            .map_err(|err| user_error(format!("DataFusion planning error: {err}")))
    }

    async fn execute_sql_streaming(&self, sql: &str) -> PgWireResult<Response> {
        self.state.ensure_materialized_views_in_sql(sql).await?;
        let df = self.plan_sql(sql).await?;
        let stream = df
            .execute_stream()
            .await
            .map_err(|err| user_error(format!("DataFusion execution error: {err}")))?;
        let response = build_query_response_stream(stream).await?;
        Ok(Response::Query(response))
    }
}

#[async_trait]
impl NoopStartupHandler for FloeQueryHandler {
    async fn post_startup<C>(
        &self,
        _client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for FloeQueryHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        if let Some(tail_sql) = detect_single_tail_statement(query) {
            let response = self.execute_tail_statement(tail_sql).await?;
            return Ok(vec![response]);
        }

        let dialect = PostgreSqlDialect {};
        let statements = Parser::parse_sql(&dialect, query)
            .map_err(|err| user_error(format!("SQL parse error: {err}")))?;

        let mut responses = Vec::with_capacity(statements.len());
        for statement in statements {
            responses.push(self.execute_statement(statement).await?);
        }

        if responses.is_empty() {
            responses.push(Response::Execution(Tag::new("EMPTY")));
        }
        Ok(responses)
    }
}

fn render_sql_with_params(
    prepared: &PreparedStatement,
    portal: &Portal<PreparedStatement>,
) -> PgWireResult<String> {
    let expected = prepared.parameter_count();
    if portal.parameter_len() != expected {
        return Err(user_error(format!(
            "expected {expected} parameter(s) but received {}",
            portal.parameter_len()
        )));
    }

    let mut decoded = Vec::with_capacity(expected);
    for idx in 0..expected {
        let format = portal.parameter_format.format_for(idx);
        if matches!(format, FieldFormat::Binary) {
            return Err(user_error("binary parameters are not supported"));
        }
        let raw_value = portal
            .parameters
            .get(idx)
            .ok_or_else(|| user_error("missing parameter value"))?;
        let value = decode_parameter_value(raw_value.as_ref(), format)?;
        decoded.push(value);
    }

    let mut statement = prepared.statement.clone();
    substitute_placeholders(&mut statement, &decoded)?;
    Ok(statement.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use bytes::Bytes;
    use bytes::Buf;
    use datafusion::scalar::ScalarValue;
    use floe_executor::dbsp_bridge::DbspBridge;
    use floe_executor::encoding::encode_projected_row_key;
    use floe_executor::materialized_view::DbspPersistedState;
    use floe_executor::{
        FloeQueryContext, MaterializedViewRegistry, MaterializedViewTableProvider,
    };
    use futures::StreamExt;
    use floe_storage::SlateCatalog;
    use pgwire::messages::data::DataRow;
    use pgwire::messages::extendedquery::Bind;
    use slatedb::Db;
    use sqlparser::parser::Parser;

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
        let mut statements =
            Parser::parse_sql(&dialect, "INSERT INTO t VALUES (1)").expect("parse");
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
}
