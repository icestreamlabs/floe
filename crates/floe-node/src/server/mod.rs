use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use floe_executor::{FloeQueryContext, MaterializedViewRegistry, MaterializedViewTableProvider};
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
use sqlparser::ast::{CreateTable, Insert, Query, SetExpr, Statement, TableFactor};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio::net::TcpListener;
use tokio::signal;

use crate::sql;

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
    let state = Arc::new(FloeServerState::new(query, materialized_views));
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
}

impl FloeServerState {
    fn new(query: FloeQueryContext, materialized_views: Arc<MaterializedViewRegistry>) -> Self {
        Self {
            query,
            materialized_views,
        }
    }

    async fn ensure_materialized_view_registered(&self, name: &str) -> PgWireResult<()> {
        if self.materialized_views.get(name).is_none() {
            return Ok(());
        }
        let schema = match self.materialized_views.schema(name) {
            Some(schema) => schema,
            None => return Ok(()),
        };

        let session = self.query.session();
        if session.table(name).await.is_ok() {
            return Ok(());
        }

        let provider = MaterializedViewTableProvider::new(
            Arc::clone(&self.materialized_views),
            name.to_string(),
            schema,
        );
        session
            .register_table(name, Arc::new(provider))
            .map_err(|err| {
                user_error(format!(
                    "failed to register materialized view {name}: {err}"
                ))
            })?;
        Ok(())
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
        let _views = Arc::clone(&self.state.materialized_views);
        self.execute_sql(&query.to_string()).await
    }

    async fn execute_sql(&self, sql: &str) -> PgWireResult<Response> {
        self.ensure_materialized_views_in_sql(sql).await?;
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

    async fn ensure_materialized_views_in_sql(&self, sql: &str) -> PgWireResult<()> {
        let dialect = PostgreSqlDialect {};
        if let Ok(statements) = Parser::parse_sql(&dialect, sql) {
            for statement in statements {
                for table in extract_tables_from_statement(&statement) {
                    self.state
                        .ensure_materialized_view_registered(&table)
                        .await?;
                }
            }
        }
        Ok(())
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

fn extract_tables_from_statement(statement: &Statement) -> Vec<String> {
    let mut names = Vec::new();
    match statement {
        Statement::Query(query) => extract_tables_from_query(query, &mut names),
        _ => {}
    }
    names
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
        TableFactor::Table { name, .. } => names.push(name.to_string()),
        TableFactor::Derived { subquery, .. } => extract_tables_from_query(subquery, names),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use floe_executor::MaterializedViewRegistry;

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

        let state = Arc::new(FloeServerState::new(query.clone(), Arc::clone(&registry)));
        let handler = FloeQueryHandler::new(state);

        handler
            .ensure_materialized_views_in_sql("SELECT * FROM mv_test")
            .await
            .expect("ensure mv");

        assert!(query.session().table("mv_test").await.is_ok());
    }
}

fn user_error(message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        "XX000".into(),
        message.into(),
    )))
}
