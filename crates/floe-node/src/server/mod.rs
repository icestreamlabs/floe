use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use floe_executor::FloeQueryContext;
use floe_storage::SlateCatalog;
use futures::Sink;
use futures::stream;
use pgwire::api::auth::StartupHandler;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;
use sqlparser::ast::{CreateTable, Insert, Query, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio::net::TcpListener;
use tokio::signal;

use crate::sql;

const LISTEN_ENV: &str = "FLOE_PG_ADDR";
const DATA_ENV: &str = "FLOE_DATA_DIR";

pub async fn run() -> Result<()> {
    let storage = match std::env::var(DATA_ENV) {
        Ok(dir) => {
            let path = PathBuf::from(dir);
            SlateCatalog::with_filesystem(path)
                .await
                .context("failed to initialise SlateDB filesystem catalog")?
        }
        Err(_) => SlateCatalog::in_memory()
            .await
            .context("failed to initialise SlateDB in-memory catalog")?,
    };

    let query = FloeQueryContext::new(Arc::new(storage));
    query
        .preload_tables()
        .await
        .context("failed to register tables with DataFusion")?;

    let state = Arc::new(FloeServerState::new(query));
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
}

impl FloeServerState {
    fn new(query: FloeQueryContext) -> Self {
        Self { query }
    }
}

struct FloeServerFactory {
    handler: Arc<FloeQueryHandler>,
}

impl FloeServerFactory {
    fn new(state: Arc<FloeServerState>) -> Self {
        Self {
            handler: Arc::new(FloeQueryHandler::new(state)),
        }
    }
}

impl PgWireServerHandlers for FloeServerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.handler.clone()
    }
}

struct FloeQueryHandler {
    state: Arc<FloeServerState>,
}

impl FloeQueryHandler {
    fn new(state: Arc<FloeServerState>) -> Self {
        Self { state }
    }

    async fn handle_create_table(&self, create: &CreateTable) -> PgWireResult<Response> {
        let definition = sql::build_table_definition(create)
            .map_err(|err| user_error(format!("invalid CREATE TABLE: {err}")))?;
        self.state
            .query
            .register_table(definition)
            .await
            .map_err(|err| user_error(err.to_string()))?;
        Ok(Response::Execution(Tag::new("CREATE TABLE")))
    }

    async fn handle_insert(&self, insert: &Insert) -> PgWireResult<Response> {
        let table_name = sql::table_name_from_object(&insert.table)
            .map_err(|err| user_error(err.to_string()))?;
        let definition = self
            .state
            .query
            .storage()
            .table(&table_name)
            .await
            .map_err(|err| user_error(err.to_string()))?
            .ok_or_else(|| user_error(format!("unknown table {table_name}")))?;

        let rows = sql::extract_insert_rows(&definition, insert)
            .map_err(|err| user_error(format!("invalid INSERT: {err}")))?;
        for row in &rows {
            self.state
                .query
                .storage()
                .insert_row(&definition, row)
                .await
                .map_err(|err| user_error(err.to_string()))?;
        }

        Ok(Response::Execution(
            Tag::new("INSERT").with_rows(rows.len()),
        ))
    }

    async fn handle_select(&self, query: &Query) -> PgWireResult<Response> {
        self.execute_sql(&query.to_string()).await
    }

    async fn execute_sql(&self, sql: &str) -> PgWireResult<Response> {
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
            match statement {
                Statement::CreateTable(create) => {
                    responses.push(self.handle_create_table(&create).await?);
                }
                Statement::Insert(insert) => {
                    responses.push(self.handle_insert(&insert).await?);
                }
                Statement::Query(query) => {
                    responses.push(self.handle_select(&query).await?);
                }
                other => {
                    return Err(user_error(format!(
                        "unsupported statement: {}",
                        other.to_string()
                    )));
                }
            }
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

    let info = Arc::new(fields);
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

fn user_error(message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        "XX000".into(),
        message.into(),
    )))
}
