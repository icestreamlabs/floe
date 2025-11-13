use std::collections::HashSet;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use core::ops::ControlFlow;
use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use floe_executor::dbsp_bridge::DbspBridge;
use floe_executor::{FloeQueryContext, MaterializedViewRegistry, load_or_register_mv};
use floe_storage::SlateCatalog;
use futures::Sink;
use futures::stream;
use pgwire::api::auth::StartupHandler;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;
use postgres_types::Type as PgType;
use sqlparser::ast::{
    Expr, Ident, ObjectName, ObjectNamePart, Query, SetExpr, Statement, TableFactor, Value,
    ValueWithSpan, visit_expressions, visit_expressions_mut,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::Mutex;

const LISTEN_ENV: &str = "FLOE_PG_ADDR";
const DATA_ENV: &str = "FLOE_DATA_DIR";

pub async fn init_storage() -> Result<Arc<SlateCatalog>> {
    match std::env::var(DATA_ENV) {
        Ok(dir) => {
            let path = PathBuf::from(dir);
            SlateCatalog::with_filesystem(path)
                .await
                .map(Arc::new)
                .context("failed to initialise SlateDB filesystem catalog")
        }
        Err(_) => SlateCatalog::in_memory()
            .await
            .map(Arc::new)
            .context("failed to initialise SlateDB in-memory catalog"),
    }
}

pub async fn run(
    query: FloeQueryContext,
    materialized_views: Arc<MaterializedViewRegistry>,
) -> Result<()> {
    let db = query.storage().db();
    let bridge = DbspBridge::new(db).await?;
    let state = Arc::new(FloeServerState::new(query, materialized_views, bridge));
    let factory = Arc::new(FloeServerFactory::new(state));

    let address = std::env::var(LISTEN_ENV).unwrap_or_else(|_| "127.0.0.1:6432".to_string());
    let listener = TcpListener::bind(&address)
        .await
        .with_context(|| format!("failed to bind pgwire listener at {address}"))?;
    println!("Floe pgwire endpoint listening on {address}");

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (socket, peer) = accept_result?;
                let handlers = factory.clone();
                tokio::spawn(async move {
                    if let Err(err) = process_socket(socket, None, handlers).await {
                        eprintln!("connection {peer:?} terminated with error: {err}");
                    }
                });
            }
            signal = signal::ctrl_c() => {
                match signal {
                    Ok(()) => {
                        println!("Shutdown signal received, closing pgwire listener");
                    }
                    Err(err) => {
                        eprintln!("Failed to listen for shutdown signal: {err}");
                    }
                }
                break;
            }
        }
    }

    Ok(())
}

#[derive(Clone)]
struct FloeServerState {
    query: FloeQueryContext,
    materialized_views: Arc<MaterializedViewRegistry>,
    bridge: Arc<Mutex<DbspBridge>>,
}

impl FloeServerState {
    fn new(
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

    async fn ensure_materialized_view_registered(&self, name: &str) -> PgWireResult<()> {
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

    async fn ensure_materialized_views_in_sql(&self, sql: &str) -> PgWireResult<()> {
        for view in mv_identifiers_in_sql(sql) {
            self.ensure_materialized_view_registered(&view).await?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct PreparedStatement {
    statement: Statement,
    result_fields: Arc<Vec<FieldInfo>>,
    referenced_views: Vec<String>,
    parameter_count: usize,
    param_types: Vec<PgType>,
}

impl PreparedStatement {
    fn result_fields(&self) -> Arc<Vec<FieldInfo>> {
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
        let statement = statements.pop().expect("statement present");
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

struct FloeServerFactory {
    simple_handler: Arc<FloeQueryHandler>,
    extended_handler: Arc<FloeExtendedHandler>,
}

impl FloeServerFactory {
    fn new(state: Arc<FloeServerState>) -> Self {
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

struct FloeQueryHandler {
    state: Arc<FloeServerState>,
}

impl FloeQueryHandler {
    fn new(state: Arc<FloeServerState>) -> Self {
        Self { state }
    }

    async fn handle_select(&self, query: &Query) -> PgWireResult<Response> {
        self.ensure_materialized_views_in_query(query).await?;
        self.execute_sql(&query.to_string()).await
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
            other => Err(user_error(format!(
                "unsupported statement: {}",
                other.to_string()
            ))),
        }
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

    async fn execute_sql(&self, sql: &str) -> PgWireResult<Response> {
        self.state.ensure_materialized_views_in_sql(sql).await?;
        let df = self
            .state
            .query
            .session()
            .sql(sql)
            .await
            .map_err(|err| user_error(format!("DataFusion planning error: {err}")))?;
        let batches = df
            .collect()
            .await
            .map_err(|err| user_error(format!("DataFusion execution error: {err}")))?;
        let response = build_query_response(batches)?;
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

fn build_query_response(batches: Vec<RecordBatch>) -> PgWireResult<QueryResponse> {
    if batches.is_empty() {
        let schema = Arc::new(Vec::new());
        let rows = stream::iter(Vec::<PgWireResult<_>>::new());
        return Ok(QueryResponse::new(schema, rows));
    }

    let schema = batches[0].schema();
    let info = Arc::new(arrow_schema_to_field_info(&schema)?);
    let column_count = info.len();

    let mut row_buffer = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(column_count);
            for col_idx in 0..column_count {
                let array = batch.column(col_idx);
                let int_array = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    user_error(format!(
                        "expected INT8 column at position {} but found {}",
                        col_idx,
                        array.data_type()
                    ))
                })?;
                row.push(if int_array.is_null(row_idx) {
                    None
                } else {
                    Some(int_array.value(row_idx))
                });
            }
            row_buffer.push(row);
        }
    }

    let schema_ref = info.clone();
    let row_stream = stream::iter(row_buffer.into_iter().map(move |values| {
        let mut encoder = DataRowEncoder::new(schema_ref.clone());
        for value in values {
            encoder.encode_field(&value)?;
        }
        encoder.finish()
    }));

    Ok(QueryResponse::new(info, row_stream))
}

fn arrow_schema_to_field_info(schema: &SchemaRef) -> PgWireResult<Vec<FieldInfo>> {
    let mut fields = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        if field.data_type() != &datafusion::arrow::datatypes::DataType::Int64 {
            return Err(user_error(format!(
                "unsupported column type {} in result set",
                field.data_type()
            )));
        }
        fields.push(FieldInfo::new(
            field.name().clone(),
            None,
            None,
            Type::INT8,
            FieldFormat::Text,
        ));
    }
    Ok(fields)
}

fn ensure_select_statement(statement: &Statement) -> PgWireResult<()> {
    match statement {
        Statement::Query(query) if is_select_expr(&query.body) => Ok(()),
        _ => Err(user_error("only SELECT statements are supported")),
    }
}

fn is_select_expr(expr: &SetExpr) -> bool {
    match expr {
        SetExpr::Select(_) => true,
        SetExpr::SetOperation { left, right, .. } => is_select_expr(left) && is_select_expr(right),
        SetExpr::Query(query) => is_select_expr(&query.body),
        _ => false,
    }
}

fn collect_placeholder_indices(statement: &Statement) -> PgWireResult<Vec<usize>> {
    let mut indices = Vec::new();
    let result = visit_expressions(statement, |expr| {
        if let Expr::Value(ValueWithSpan {
            value: Value::Placeholder(name),
            ..
        }) = expr
        {
            match parse_placeholder_index(name) {
                Ok(idx) => indices.push(idx),
                Err(err) => return ControlFlow::Break(err),
            }
        }
        ControlFlow::Continue(())
    });
    match result {
        ControlFlow::Continue(_) => Ok(indices),
        ControlFlow::Break(err) => Err(err),
    }
}

fn parse_placeholder_index(name: &str) -> PgWireResult<usize> {
    let trimmed = name.trim_start_matches(|c| c == '$' || c == '?');
    if trimmed.is_empty() {
        return Err(user_error(format!("invalid placeholder '{name}'")));
    }
    let idx = trimmed
        .parse::<usize>()
        .map_err(|_| user_error(format!("invalid placeholder '{name}'")))?;
    if idx == 0 {
        return Err(user_error(format!("invalid placeholder '{name}'")));
    }
    Ok(idx)
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

fn decode_parameter_value(
    raw: Option<&Bytes>,
    _format: FieldFormat,
) -> PgWireResult<ValueWithSpan> {
    match raw {
        None => Ok(Value::Null.with_empty_span()),
        Some(bytes) => {
            let text = std::str::from_utf8(bytes.as_ref())
                .map_err(|_| user_error("parameter values must be valid UTF-8"))?;
            Ok(string_to_value(text).with_empty_span())
        }
    }
}

fn string_to_value(input: &str) -> Value {
    if input.parse::<i64>().is_ok() {
        Value::Number(input.to_string(), false)
    } else if input.parse::<f64>().is_ok() {
        Value::Number(input.to_string(), false)
    } else if input.eq_ignore_ascii_case("true") {
        Value::Boolean(true)
    } else if input.eq_ignore_ascii_case("false") {
        Value::Boolean(false)
    } else {
        Value::SingleQuotedString(input.to_string())
    }
}

fn substitute_placeholders(
    statement: &mut Statement,
    values: &[ValueWithSpan],
) -> PgWireResult<()> {
    let result = visit_expressions_mut(statement, |expr| {
        if let Expr::Value(ValueWithSpan {
            value: Value::Placeholder(name),
            ..
        }) = expr
        {
            let idx = match parse_placeholder_index(name) {
                Ok(idx) => idx,
                Err(err) => return ControlFlow::Break(err),
            };
            if idx == 0 || idx > values.len() {
                return ControlFlow::Break(user_error(format!(
                    "placeholder {name} has no bound value"
                )));
            }
            *expr = Expr::Value(values[idx - 1].clone());
        }
        ControlFlow::Continue(())
    });
    match result {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(err) => Err(err),
    }
}

fn extract_tables_from_query(query: &Query, names: &mut Vec<String>) {
    extract_tables_from_setexpr(&query.body, names);
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            extract_tables_from_query(&cte.query, names);
        }
    }
}

fn extract_tables_from_setexpr(expr: &SetExpr, names: &mut Vec<String>) {
    match expr {
        SetExpr::Select(select) => {
            for table in &select.from {
                extract_tables_from_table_factor(&table.relation, names);
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            extract_tables_from_setexpr(left, names);
            extract_tables_from_setexpr(right, names);
        }
        SetExpr::Query(query) => extract_tables_from_query(query, names),
        _ => {}
    }
}

fn extract_tables_from_table_factor(factor: &TableFactor, names: &mut Vec<String>) {
    match factor {
        TableFactor::Table { name, .. } => {
            if let Some(table) = normalize_object_name(name) {
                names.push(table);
            }
        }
        TableFactor::Derived { subquery, .. } => extract_tables_from_query(subquery, names),
        _ => {}
    }
}

fn normalize_object_name(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|Ident { value, .. }| value.clone())
}

fn mv_identifiers_in_sql(sql: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut seen = HashSet::new();
    for raw in sql.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '"')) {
        if raw.is_empty() {
            continue;
        }
        if let Some(name) = normalize_identifier(raw) {
            if seen.insert(name.clone()) {
                names.push(name);
            }
        }
    }
    names
}

fn normalize_identifier(raw: &str) -> Option<String> {
    let quoted = raw.starts_with('"') && raw.ends_with('"') && raw.len() >= 2;
    let inner = if quoted { &raw[1..raw.len() - 1] } else { raw };
    if inner.is_empty() {
        return None;
    }
    let normalized = if quoted {
        inner.to_string()
    } else {
        inner.to_ascii_lowercase()
    };
    if normalized.starts_with("mv_") {
        Some(normalized)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use floe_executor::MaterializedViewRegistry;
    use pgwire::messages::extendedquery::Bind;
    use sqlparser::dialect::PostgreSqlDialect;
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

    #[test]
    fn detects_mv_identifiers_in_sql() {
        let sql = r#"SELECT * FROM mv_orders JOIN "mv_Sales" ON mv_orders.id = "mv_Sales".id"#;
        let mut names = mv_identifiers_in_sql(sql);
        names.sort();
        let mut expected = vec!["mv_orders".to_string(), "mv_Sales".to_string()];
        expected.sort();
        assert_eq!(names, expected);
    }

    // No client calls are required in tests as routing is validated directly
    // against parsed statements.
}

fn user_error(message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        "XX000".into(),
        message.into(),
    )))
}
