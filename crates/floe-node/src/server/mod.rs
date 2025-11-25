use std::collections::HashSet;
use std::fmt::Debug;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use core::ops::ControlFlow;
use datafusion::arrow::array::{
    Array, Decimal128Array, Decimal256Array, Int16Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::physical_plan::SendableRecordBatchStream;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use floe_executor::dbsp_bridge::DbspBridge;
use floe_executor::pgwire::tail::{
    TailBatch, TailStream, execute_tail, parse_tail_sql, tail_output_schema,
};
use floe_executor::{FloeQueryContext, MaterializedViewRegistry, load_or_register_mv, namespaces};
use floe_storage::SlateCatalog;
use futures::{Sink, Stream, StreamExt, stream};
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
use pgwire::messages::data::DataRow;
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
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use datafusion::arrow::array::ArrayRef;

const LISTEN_ENV: &str = "FLOE_PG_ADDR";
const DATA_ENV: &str = "FLOE_DATA_DIR";
const TAIL_OP_VALUE: i16 = 1;

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
            other => Err(user_error(format!(
                "unsupported statement: {}",
                other.to_string()
            ))),
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

fn build_query_response(batches: Vec<RecordBatch>) -> PgWireResult<QueryResponse> {
    if batches.is_empty() {
        let schema = Arc::new(Vec::new());
        let rows = stream::iter(Vec::<PgWireResult<_>>::new());
        return Ok(QueryResponse::new(schema, rows));
    }

    let info = Arc::new(arrow_schema_to_field_info(&batches[0].schema())?);
    let schema_ref = info.clone();
    let row_stream = stream::iter(batches.into_iter().flat_map(move |batch| {
        let schema = Arc::clone(&schema_ref);
        (0..batch.num_rows()).map(move |row_idx| {
            encode_stream_row(&batch, row_idx, Arc::clone(&schema))
        })
    }));

    Ok(QueryResponse::new(info, row_stream))
}

async fn build_query_response_stream(
    mut batch_stream: SendableRecordBatchStream,
) -> PgWireResult<QueryResponse> {
    let Some(first_batch_result) = batch_stream.next().await else {
        let schema = Arc::new(Vec::new());
        let rows = stream::iter(Vec::<PgWireResult<_>>::new());
        return Ok(QueryResponse::new(schema, rows));
    };
    let first_batch = first_batch_result
        .map_err(|err| user_error(format!("DataFusion execution error: {err}")))?;
    let info = Arc::new(arrow_schema_to_field_info(&first_batch.schema())?);
    let row_schema = Arc::clone(&info);

    struct StreamState {
        stream: SendableRecordBatchStream,
        current_batch: Option<RecordBatch>,
        next_row: usize,
        schema: Arc<Vec<FieldInfo>>,
    }

    let initial_state = StreamState {
        stream: batch_stream,
        current_batch: Some(first_batch),
        next_row: 0,
        schema: row_schema,
    };

    let rows = stream::try_unfold(initial_state, move |mut state| async move {
        loop {
            if let Some(batch) = state.current_batch.as_ref() {
                if state.next_row < batch.num_rows() {
                    let schema = Arc::clone(&state.schema);
                    let row = encode_stream_row(batch, state.next_row, schema)?;
                    state.next_row += 1;
                    return Ok(Some((row, state)));
                }
                state.current_batch = None;
                state.next_row = 0;
            }

            match state.stream.next().await {
                Some(Ok(batch)) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    state.current_batch = Some(batch);
                    state.next_row = 0;
                }
                Some(Err(err)) => {
                    return Err(user_error(format!("DataFusion execution error: {err}")));
                }
                None => return Ok(None),
            }
        }
    });

    Ok(QueryResponse::new(info, rows))
}

struct TailResponseStream {
    schema: Arc<Vec<FieldInfo>>,
    stream: TailStream,
    cancel: CancellationToken,
    current_batch: Option<TailBatch>,
    next_row: usize,
}

impl TailResponseStream {
    fn new(schema: Arc<Vec<FieldInfo>>, stream: TailStream, cancel: CancellationToken) -> Self {
        Self {
            schema,
            stream,
            cancel,
            current_batch: None,
            next_row: 0,
        }
    }
}

impl Drop for TailResponseStream {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl Stream for TailResponseStream {
    type Item = PgWireResult<DataRow>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(batch) = self.current_batch.as_ref() {
                if self.next_row < batch.batch.num_rows() {
                    let schema = Arc::clone(&self.schema);
                    let row = encode_tail_row(schema, batch.version, &batch.batch, self.next_row);
                    self.next_row += 1;
                    return Poll::Ready(Some(row));
                }
                self.current_batch = None;
                self.next_row = 0;
            }

            match Pin::new(&mut self.stream).poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    if batch.batch.num_rows() == 0 {
                        continue;
                    }
                    self.current_batch = Some(batch);
                    self.next_row = 0;
                }
                Poll::Ready(Some(Err(err))) => {
                    return Poll::Ready(Some(Err(user_error(format!(
                        "TAIL execution error: {err}"
                    )))));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn encode_stream_row(
    batch: &RecordBatch,
    row_idx: usize,
    schema: Arc<Vec<FieldInfo>>,
) -> PgWireResult<DataRow> {
    let mut encoder = DataRowEncoder::new(schema);
    for col_idx in 0..batch.num_columns() {
        let array = batch.column(col_idx);
        let data_type = batch.schema().field(col_idx).data_type().clone();
        encode_arrow_value(array.as_ref(), row_idx, &data_type, &mut encoder)?;
    }
    encoder.finish()
}

fn encode_tail_row(
    schema: Arc<Vec<FieldInfo>>,
    version: i64,
    batch: &RecordBatch,
    row_idx: usize,
) -> PgWireResult<DataRow> {
    let mut encoder = DataRowEncoder::new(schema);
    encoder.encode_field(&Some(version))?;
    encoder.encode_field(&Some(i64::from(TAIL_OP_VALUE)))?;
    encoder.encode_field(&Option::<i64>::None)?;
    for col_idx in 0..batch.num_columns() {
        let array = batch.column(col_idx);
        let data_type = batch.schema().field(col_idx).data_type().clone();
        encode_arrow_value(array.as_ref(), row_idx, &data_type, &mut encoder)?;
    }
    encoder.finish()
}

fn encode_arrow_value(
    array: &dyn Array,
    row_idx: usize,
    data_type: &DataType,
    encoder: &mut DataRowEncoder,
) -> PgWireResult<()> {
    match data_type {
        DataType::Int16 => {
            let array = array.as_any().downcast_ref::<Int16Array>().unwrap();
            let value = if array.is_null(row_idx) {
                None
            } else {
                Some(array.value(row_idx))
            };
            encoder.encode_field(&value)
        }
        DataType::UInt16 => {
            let array = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            let value = if array.is_null(row_idx) {
                None
            } else {
                Some(array.value(row_idx) as i64)
            };
            encoder.encode_field(&value)
        }
        DataType::Int32 => {
            let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            let value = if array.is_null(row_idx) {
                None
            } else {
                Some(array.value(row_idx))
            };
            encoder.encode_field(&value)
        }
        DataType::UInt32 => {
            let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            let value = if array.is_null(row_idx) {
                None
            } else {
                Some(array.value(row_idx) as i64)
            };
            encoder.encode_field(&value)
        }
        DataType::Int64 => {
            let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            let value = if array.is_null(row_idx) {
                None
            } else {
                Some(array.value(row_idx))
            };
            encoder.encode_field(&value)
        }
        DataType::UInt64 => {
            let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            let value = if array.is_null(row_idx) {
                None
            } else {
                Some(array.value(row_idx) as i64)
            };
            encoder.encode_field(&value)
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            if array.is_null(row_idx) {
                return encoder.encode_field::<Option<NaiveDateTime>>(&None);
            }
            let micros = array.value(row_idx);
            let naive = micros_to_naive_datetime(micros)
                .ok_or_else(|| user_error(format!("timestamp micros {micros} out of range")))?;
            if tz.is_some() {
                let utc: DateTime<Utc> = Utc.from_utc_datetime(&naive);
                encoder.encode_field(&Some(utc))
            } else {
                encoder.encode_field(&Some(naive))
            }
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            if array.is_null(row_idx) {
                return encoder.encode_field::<Option<NaiveDateTime>>(&None);
            }
            let micros = array.value(row_idx).saturating_mul(1000);
            let naive = micros_to_naive_datetime(micros)
                .ok_or_else(|| user_error(format!("timestamp micros {micros} out of range")))?;
            if tz.is_some() {
                let utc: DateTime<Utc> = Utc.from_utc_datetime(&naive);
                encoder.encode_field(&Some(utc))
            } else {
                encoder.encode_field(&Some(naive))
            }
        }
        DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<StringArray>().unwrap();
            let value: Option<&str> = if array.is_null(row_idx) {
                None
            } else {
                Some(array.value(row_idx))
            };
            encoder.encode_field(&value)
        }
        DataType::Decimal128(_, _) => {
            let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            if array.is_null(row_idx) {
                encoder.encode_field::<Option<String>>(&None)
            } else {
                let value = array.value_as_string(row_idx);
                encoder.encode_field(&Some(value))
            }
        }
        DataType::Decimal256(_, _) => {
            let array = array.as_any().downcast_ref::<Decimal256Array>().unwrap();
            if array.is_null(row_idx) {
                encoder.encode_field::<Option<String>>(&None)
            } else {
                let value = array.value_as_string(row_idx);
                encoder.encode_field(&Some(value))
            }
        }
        other => Err(user_error(format!(
            "unsupported column type {} in result set",
            other
        ))),
    }
}

fn micros_to_naive_datetime(micros: i64) -> Option<NaiveDateTime> {
    DateTime::<Utc>::from_timestamp_micros(micros).map(|dt| dt.naive_utc())
}

fn arrow_schema_to_field_info(schema: &SchemaRef) -> PgWireResult<Vec<FieldInfo>> {
    let mut fields = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        match field.data_type() {
            DataType::Int16
            | DataType::UInt16
            | DataType::Int32
            | DataType::UInt32
            | DataType::Int64
            | DataType::UInt64
            | DataType::Utf8
            | DataType::Timestamp(TimeUnit::Microsecond, _)
            | DataType::Timestamp(TimeUnit::Millisecond, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => {}
            other => {
                return Err(user_error(format!(
                    "unsupported column type {} in result set",
                    other
                )));
            }
        }
        let pg_type = match field.data_type() {
            DataType::Timestamp(_, Some(_)) => Type::TIMESTAMPTZ,
            DataType::Timestamp(_, None) => Type::TIMESTAMP,
            DataType::Utf8 => Type::TEXT,
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => Type::NUMERIC,
            _ => Type::INT8,
        };
        fields.push(FieldInfo::new(
            field.name().clone(),
            None,
            None,
            pg_type,
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
    if normalized.starts_with("mv_") && namespaces::materialized_view(&normalized).is_ok() {
        Some(normalized)
    } else {
        None
    }
}

fn detect_single_tail_statement(query: &str) -> Option<&str> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return None;
    }
    let statement = trimmed.trim_end_matches(|c: char| c.is_whitespace() || c == ';');
    if statement.is_empty() {
        return None;
    }
    if statement.contains(';') {
        return None;
    }
    if is_tail_statement(statement) {
        Some(statement)
    } else {
        None
    }
}

fn is_tail_statement(sql: &str) -> bool {
    let trimmed = sql.trim_start_matches(|c: char| c.is_ascii_control() || c.is_whitespace());
    if trimmed.len() < 4 {
        return false;
    }
    if !trimmed[..4].eq_ignore_ascii_case("TAIL") {
        return false;
    }
    trimmed[4..]
        .chars()
        .next()
        .map_or(false, |ch| ch.is_whitespace())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use bytes::Buf;
    use datafusion::physical_plan::RecordBatchStream;
    use datafusion::scalar::ScalarValue;
    use floe_executor::encoding::encode_projected_row_key;
    use floe_executor::materialized_view::DbspPersistedState;
    use floe_executor::{
        FloeQueryContext, MaterializedViewRegistry, MaterializedViewTableProvider,
    };
    use futures::Stream;
    use pgwire::messages::extendedquery::Bind;
    use pgwire::messages::data::DataRow;
    use slatedb::Db;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

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

    #[test]
    fn arrow_schema_maps_timestamp_types() {
        let schema = SchemaRef::from(Schema::new(vec![
            Field::new("ts_micros", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new(
                "ts_millis",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
            Field::new("label", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
        ]));

        let fields = arrow_schema_to_field_info(&schema).expect("map schema");
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[0].datatype(), &Type::TIMESTAMP);
        assert_eq!(fields[1].datatype(), &Type::TIMESTAMPTZ);
        assert_eq!(fields[2].datatype(), &Type::TEXT);
        assert_eq!(fields[3].datatype(), &Type::NUMERIC);
    }

    #[test]
    fn encode_timestamp_values() {
        // 2024-01-01T00:00:01Z
        let micros = 1_704_067_201_000_000i64;
        let millis = micros / 1000;

        let micros_array = TimestampMicrosecondArray::from(vec![Some(micros), None]);
        let millis_array = {
            use arrow_data::ArrayData;
            use arrow_buffer::{Buffer, NullBuffer};

            let values = Buffer::from_slice_ref(&[millis, 0]);
            let nulls = NullBuffer::from(vec![true, false]);
            let data = ArrayData::builder(DataType::Timestamp(
                TimeUnit::Millisecond,
                Some("UTC".into()),
            ))
            .len(2)
            .add_buffer(values)
            .null_bit_buffer(Some(nulls.into_inner().into_inner()))
            .build()
            .expect("array data");
            TimestampMillisecondArray::from(data)
        };
        let utf8_array = StringArray::from(vec![Some("hello"), None]);
        let decimal_array = Decimal128Array::from(vec![Some(12_345i128), None]).with_precision_and_scale(10, 2).expect("decimal array");

        let schema = SchemaRef::from(Schema::new(vec![
            Field::new("ts_micros", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new(
                "ts_millis",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
            Field::new("label", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(micros_array) as ArrayRef,
                Arc::new(millis_array) as ArrayRef,
                Arc::new(utf8_array) as ArrayRef,
                Arc::new(decimal_array) as ArrayRef,
            ],
        )
        .expect("batch");

        let field_info = Arc::new(arrow_schema_to_field_info(&batch.schema()).expect("schema"));
        let row = encode_stream_row(&batch, 0, Arc::clone(&field_info)).expect("encode row");

        // Decode the row buffer to confirm both fields are non-null and encoded.
        let mut buf = row.data.clone();
        let first_len = buf.get_i32();
        assert!(first_len > 0);
        let _ = buf.split_to(first_len as usize);
        let second_len = buf.get_i32();
        assert!(second_len > 0);
        let _ = buf.split_to(second_len as usize);
        let third_len = buf.get_i32();
        assert!(third_len > 0);
        let _ = buf.split_to(third_len as usize);
        let fourth_len = buf.get_i32();
        assert!(fourth_len > 0);

        // Null row should encode null markers.
        let null_row = encode_stream_row(&batch, 1, field_info).expect("encode null row");
        let mut buf_null = null_row.data.clone();
        assert_eq!(buf_null.get_i32(), -1);
        assert_eq!(buf_null.get_i32(), -1);
        assert_eq!(buf_null.get_i32(), -1);
        assert_eq!(buf_null.get_i32(), -1);
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

    #[test]
    fn detects_tail_statement_in_simple_query() {
        let query = "  TAIL mv_orders WITH SNAPSHOT;;\n";
        assert_eq!(
            detect_single_tail_statement(query),
            Some("TAIL mv_orders WITH SNAPSHOT")
        );
        assert!(detect_single_tail_statement("SELECT 1;").is_none());
        assert!(detect_single_tail_statement("TAIL mv_orders; SELECT 1").is_none());
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

    #[tokio::test]
    async fn build_query_response_stream_yields_batches_incrementally() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch_one = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )
        .expect("record batch");
        let batch_two = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![3])) as ArrayRef],
        )
        .expect("record batch");
        let poll_counter = Arc::new(AtomicUsize::new(0));
        let stream: SendableRecordBatchStream = Box::pin(TestBatchStream::new(
            vec![batch_one, batch_two],
            poll_counter.clone(),
        ));

        let mut response = build_query_response_stream(stream)
            .await
            .expect("stream response");
        let schema = response.row_schema();
        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].name(), "value");

        let rows = response.data_rows();
        rows.next().await.expect("row").expect("ok row");
        assert_eq!(poll_counter.load(Ordering::SeqCst), 1);

        rows.next().await.expect("row").expect("ok row");
        assert_eq!(
            poll_counter.load(Ordering::SeqCst),
            1,
            "second batch should not be polled yet"
        );

        rows.next().await.expect("row").expect("ok row");
        assert_eq!(
            poll_counter.load(Ordering::SeqCst),
            2,
            "second batch should be polled after draining the first"
        );
        assert!(rows.next().await.is_none());
    }

    struct TestBatchStream {
        batches: Vec<RecordBatch>,
        next_index: usize,
        polled: Arc<AtomicUsize>,
    }

    impl TestBatchStream {
        fn new(batches: Vec<RecordBatch>, polled: Arc<AtomicUsize>) -> Self {
            Self {
                batches,
                next_index: 0,
                polled,
            }
        }
    }

    impl Stream for TestBatchStream {
        type Item = datafusion::error::Result<RecordBatch>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = self.get_mut();
            if this.next_index >= this.batches.len() {
                return Poll::Ready(None);
            }
            let batch = this.batches[this.next_index].clone();
            this.next_index += 1;
            this.polled.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Some(Ok(batch)))
        }
    }

    impl RecordBatchStream for TestBatchStream {
        fn schema(&self) -> SchemaRef {
            self.batches
                .first()
                .map(|batch| batch.schema())
                .unwrap_or_else(|| Arc::new(Schema::new(Vec::<Field>::new())))
        }
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
