use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use floe_core::catalog::{ColumnDefinition, TableDefinition};
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
use sqlparser::ast::{
    ColumnOption, CreateTable, DataType, Expr as SqlExpr, IndexColumn, Insert, ObjectName,
    ObjectNamePart, Query, Statement, TableConstraint, TableObject, UnaryOperator,
    Value as SqlValue, ValueWithSpan,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let data_dir = data_directory();
    let storage = Arc::new(
        SlateCatalog::open_or_create(&data_dir)
            .await
            .with_context(|| format!("failed to open SlateDB catalog at {}", data_dir.display()))?,
    );

    let query = FloeQueryContext::new(storage.clone());
    query
        .preload_tables()
        .await
        .context("failed to register tables with DataFusion")?;

    let state = Arc::new(FloeServerState::new(query));
    let factory = Arc::new(FloeServerFactory::new(state));

    let address = std::env::var("FLOE_PG_ADDR").unwrap_or_else(|_| "127.0.0.1:6432".to_string());
    let listener = TcpListener::bind(&address)
        .await
        .with_context(|| format!("failed to bind pgwire listener at {address}"))?;
    println!("Floe pgwire endpoint listening on {address}");

    loop {
        let (socket, peer) = listener.accept().await?;
        let handlers = factory.clone();
        tokio::spawn(async move {
            if let Err(err) = process_socket(socket, None, handlers).await {
                eprintln!("connection {peer:?} terminated with error: {err}");
            }
        });
    }
}

fn data_directory() -> PathBuf {
    std::env::var("FLOE_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(".floe/db"))
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
        let definition = build_table_definition(create)
            .map_err(|err| user_error(format!("invalid CREATE TABLE: {err}")))?;
        self.state
            .query
            .register_table(definition)
            .await
            .map_err(|err| user_error(err.to_string()))?;
        Ok(Response::Execution(Tag::new("CREATE TABLE")))
    }

    async fn handle_insert(&self, insert: &Insert) -> PgWireResult<Response> {
        let table_name = table_name_from_object(&insert.table)?;
        let definition = self
            .state
            .query
            .storage()
            .table(&table_name)
            .await
            .map_err(|err| user_error(err.to_string()))?
            .ok_or_else(|| user_error(format!("unknown table {table_name}")))?;

        let rows = extract_insert_rows(&definition, insert)
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

fn build_table_definition(create: &CreateTable) -> Result<TableDefinition> {
    let table_name = object_name_to_string(&create.name)?;
    let mut defs = Vec::with_capacity(create.columns.len());
    for col in &create.columns {
        ensure_integer_column(&col.data_type, &col.name.value)?;
        let is_primary = col.options.iter().any(|opt| {
            matches!(
                opt.option,
                ColumnOption::Unique {
                    is_primary: true,
                    ..
                }
            )
        });
        defs.push((col.name.value.clone(), is_primary));
    }

    for constraint in &create.constraints {
        if let TableConstraint::PrimaryKey { columns, .. } = constraint {
            for column in columns {
                let column_name = index_column_name(column)?;
                mark_primary_key(&mut defs, &column_name)?;
            }
        }
    }

    let columns = defs
        .into_iter()
        .map(|(name, primary)| ColumnDefinition::new(name, primary))
        .collect::<Vec<_>>();

    TableDefinition::new(table_name, columns)
}

fn ensure_integer_column(data_type: &DataType, column: &str) -> Result<()> {
    let is_integer = matches!(
        data_type,
        DataType::Int(_)
            | DataType::Integer(_)
            | DataType::BigInt(_)
            | DataType::SmallInt(_)
            | DataType::Unsigned
            | DataType::UnsignedInteger
            | DataType::IntUnsigned(_)
            | DataType::IntegerUnsigned(_)
            | DataType::BigIntUnsigned(_)
            | DataType::SmallIntUnsigned(_)
            | DataType::Int2(_)
            | DataType::Int2Unsigned(_)
            | DataType::Int4(_)
            | DataType::Int4Unsigned(_)
            | DataType::Int8(_)
            | DataType::Int8Unsigned(_)
    );
    if is_integer {
        Ok(())
    } else {
        Err(anyhow!(
            "column {column} must be declared as an integer type"
        ))
    }
}

fn mark_primary_key(defs: &mut [(String, bool)], name: &str) -> Result<()> {
    let Some((_, flag)) = defs.iter_mut().find(|(col, _)| col == name) else {
        return Err(anyhow!("primary key references unknown column {name}"));
    };
    *flag = true;
    Ok(())
}

fn object_name_to_string(name: &ObjectName) -> Result<String> {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.clone())
        .ok_or_else(|| anyhow!("invalid identifier"))
}

fn table_name_from_object(table: &TableObject) -> PgWireResult<String> {
    match table {
        TableObject::TableName(name) => {
            object_name_to_string(name).map_err(|err| user_error(err.to_string()))
        }
        TableObject::TableFunction(_) => {
            Err(user_error("INSERT INTO TABLE FUNCTION is not supported"))
        }
    }
}

fn index_column_name(column: &IndexColumn) -> Result<String> {
    match &column.column.expr {
        SqlExpr::Identifier(ident) => Ok(ident.value.clone()),
        SqlExpr::CompoundIdentifier(parts) => parts
            .last()
            .map(|ident| ident.value.clone())
            .ok_or_else(|| anyhow!("empty compound identifier")),
        other => Err(anyhow!(
            "unsupported expression in PRIMARY KEY declaration: {other}"
        )),
    }
}

fn extract_insert_rows(definition: &TableDefinition, insert: &Insert) -> Result<Vec<Vec<i64>>> {
    use sqlparser::ast::{SetExpr, Values};

    let source = insert
        .source
        .as_ref()
        .ok_or_else(|| anyhow!("INSERT requires a VALUES clause"))?;
    let values = match source.body.as_ref() {
        SetExpr::Values(Values { rows, .. }) => rows,
        _ => return Err(anyhow!("only VALUES inserts are supported")),
    };

    let column_order = if insert.columns.is_empty() {
        definition
            .columns()
            .iter()
            .map(|column| column.name().to_string())
            .collect::<Vec<_>>()
    } else {
        insert
            .columns
            .iter()
            .map(|ident| ident.value.clone())
            .collect::<Vec<_>>()
    };

    if column_order.len() != definition.columns().len() {
        return Err(anyhow!(
            "insert specifies {} columns but table has {}",
            column_order.len(),
            definition.columns().len()
        ));
    }

    let mut rows_out = Vec::with_capacity(values.len());
    for row in values {
        if row.len() != column_order.len() {
            return Err(anyhow!(
                "expected {} values in row, found {}",
                column_order.len(),
                row.len()
            ));
        }

        let mut materialized = vec![0_i64; definition.columns().len()];
        for (expr, column_name) in row.iter().zip(column_order.iter()) {
            let idx = definition
                .column_index(column_name)
                .ok_or_else(|| anyhow!("unknown column {column_name} in INSERT"))?;
            let value = parse_int_literal(expr)?;
            materialized[idx] = value;
        }
        definition.validate_row(&materialized)?;
        rows_out.push(materialized);
    }
    Ok(rows_out)
}

fn parse_int_literal(expr: &SqlExpr) -> Result<i64> {
    match expr {
        SqlExpr::Value(ValueWithSpan {
            value: SqlValue::Number(num, _),
            ..
        }) => {
            let num_str = num.to_string();
            num_str
                .parse::<i64>()
                .map_err(|err| anyhow!("failed to parse integer literal {num_str}: {err}"))
        }
        SqlExpr::UnaryOp { op, expr } if matches!(op, UnaryOperator::Minus) => {
            let value = parse_int_literal(expr)?;
            Ok(-value)
        }
        other => Err(anyhow!("unsupported expression in VALUES clause: {other}")),
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
