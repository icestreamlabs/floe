use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use futures::Sink;
use pgwire::api::portal::Portal;
use pgwire::api::query::ExtendedQueryHandler;
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, FieldFormat, Response,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use postgres_types::Type as PgType;
use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::execution::{FloeServerState, build_query_response};
use crate::management::{
    ManagementStatement, handle_management_statement, management_result_schema,
    parse_management_statement,
};
use crate::sql::{
    collect_placeholder_indices, decode_parameter_value, ensure_select_statement,
    extract_tables_from_query, substitute_placeholders,
};
use crate::types::arrow_schema_to_field_info;
use crate::user_error;

#[derive(Clone, Debug)]
pub(super) struct PreparedStatement {
    kind: PreparedStatementKind,
    result_fields: Arc<Vec<pgwire::api::results::FieldInfo>>,
    referenced_views: Vec<String>,
    parameter_count: usize,
    param_types: Vec<PgType>,
}

#[derive(Clone, Debug)]
enum PreparedStatementKind {
    Query(Statement),
    Management(ManagementStatement),
}

impl PreparedStatement {
    pub(super) fn result_fields(&self) -> Arc<Vec<pgwire::api::results::FieldInfo>> {
        Arc::clone(&self.result_fields)
    }

    pub(super) fn referenced_views(&self) -> &[String] {
        &self.referenced_views
    }

    pub(super) fn parameter_count(&self) -> usize {
        self.parameter_count
    }

    pub(super) fn parameter_types(&self) -> Vec<PgType> {
        self.param_types.clone()
    }

    fn kind(&self) -> &PreparedStatementKind {
        &self.kind
    }
}

pub(super) struct FloeExtendedQueryParser {
    state: Arc<FloeServerState>,
    dialect: PostgreSqlDialect,
}

impl FloeExtendedQueryParser {
    pub(super) fn new(state: Arc<FloeServerState>) -> Self {
        Self {
            state,
            dialect: PostgreSqlDialect {},
        }
    }

    pub(super) async fn prepare_statement(
        &self,
        sql: &str,
        parameter_types: &[PgType],
    ) -> PgWireResult<PreparedStatement> {
        if let Some(statement) = parse_management_statement(sql) {
            let schema = management_result_schema(&statement);
            let fields = Arc::new(arrow_schema_to_field_info(&schema)?);
            return Ok(PreparedStatement {
                kind: PreparedStatementKind::Management(statement),
                result_fields: fields,
                referenced_views: Vec::new(),
                parameter_count: 0,
                param_types: Vec::new(),
            });
        }

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
            kind: PreparedStatementKind::Query(statement),
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

pub(super) struct FloeExtendedHandler {
    state: Arc<FloeServerState>,
    parser: Arc<FloeExtendedQueryParser>,
}

impl FloeExtendedHandler {
    pub(super) fn new(state: Arc<FloeServerState>) -> Self {
        Self {
            parser: Arc::new(FloeExtendedQueryParser::new(Arc::clone(&state))),
            state,
        }
    }

    pub(super) fn render_portal_sql(
        &self,
        portal: &Portal<PreparedStatement>,
    ) -> PgWireResult<String> {
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
        if let PreparedStatementKind::Management(statement) =
            portal.statement.statement.kind()
        {
            return handle_management_statement(self.state.as_ref(), statement).await;
        }

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

fn render_sql_with_params(
    prepared: &PreparedStatement,
    portal: &Portal<PreparedStatement>,
) -> PgWireResult<String> {
    let PreparedStatementKind::Query(statement) = prepared.kind() else {
        return Err(user_error("management statements do not support parameters"));
    };
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

    let mut statement = statement.clone();
    substitute_placeholders(&mut statement, &decoded)?;
    Ok(statement.to_string())
}
