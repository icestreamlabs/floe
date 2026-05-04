use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

use arrow_pg::datatypes::df::{deserialize_parameters, encode_dataframe};
use arrow_pg::datatypes::into_pg_type;
use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::LogicalPlan;
use futures::Sink;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::ExtendedQueryHandler;
use pgwire::api::results::{DescribePortalResponse, DescribeStatementResponse, Response};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use postgres_types::Type as PgType;
use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::execution::FloeServerState;
use crate::management::{
    ManagementStatement, handle_management_statement, management_result_schema,
    parse_management_statement,
};
use crate::protocol::bootstrap::{detect_noop_session_command, rewrite_bootstrap_sql};
use crate::sql::{
    ensure_select_statement, extract_tables_from_query, is_system_catalog_relation,
    unqualified_table_name,
};
use crate::types::arrow_schema_to_field_info;
use crate::{parse_error, planner_error, user_error};

#[derive(Clone, Debug)]
pub(super) struct PreparedStatement {
    kind: PreparedStatementKind,
    result_fields: Arc<Vec<pgwire::api::results::FieldInfo>>,
    referenced_views: Vec<String>,
    param_types: Vec<PgType>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
enum PreparedStatementKind {
    Query { plan: LogicalPlan },
    Management(ManagementStatement),
    Noop { tag: String },
}

impl PreparedStatement {
    pub(super) fn result_fields(&self) -> Arc<Vec<pgwire::api::results::FieldInfo>> {
        Arc::clone(&self.result_fields)
    }

    pub(super) fn referenced_views(&self) -> &[String] {
        &self.referenced_views
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
        parameter_types: &[Option<PgType>],
    ) -> PgWireResult<PreparedStatement> {
        if let Some(tag) = detect_noop_session_command(sql) {
            return Ok(PreparedStatement {
                kind: PreparedStatementKind::Noop {
                    tag: tag.to_string(),
                },
                result_fields: Arc::new(Vec::new()),
                referenced_views: Vec::new(),
                param_types: Vec::new(),
            });
        }
        if let Some(statement) = parse_management_statement(sql) {
            let schema = management_result_schema(&statement);
            let fields = Arc::new(arrow_schema_to_field_info(&schema)?);
            return Ok(PreparedStatement {
                kind: PreparedStatementKind::Management(statement),
                result_fields: fields,
                referenced_views: Vec::new(),
                param_types: Vec::new(),
            });
        }
        let rewritten_sql = rewrite_bootstrap_sql(sql).unwrap_or_else(|| sql.trim().to_string());

        let mut statements = Parser::parse_sql(&self.dialect, &rewritten_sql)
            .map_err(|err| parse_error(format!("SQL parse error: {err}")))?;
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
            if is_system_catalog_relation(&name) {
                continue;
            }
            let unqualified = unqualified_table_name(&name).to_string();
            if seen.insert(unqualified.clone()) {
                deduped.push(unqualified);
            }
        }

        for view in &deduped {
            self.state.ensure_materialized_view_registered(view).await?;
        }

        self.state.refresh_catalog_shims().await?;
        self.state
            .ensure_materialized_views_in_sql(&rewritten_sql)
            .await?;
        let dataframe = self
            .state
            .query
            .session()
            .sql(&rewritten_sql)
            .await
            .map_err(|err| planner_error(format!("DataFusion planning error: {err}")))?;
        let schema_ref = Arc::new(dataframe.schema().as_arrow().clone());
        let fields = Arc::new(arrow_schema_to_field_info(&schema_ref)?);
        let logical_plan = dataframe.into_unoptimized_plan();
        let inferred_parameter_types = logical_plan
            .get_parameter_types()
            .map_err(|err| user_error(format!("DataFusion parameter inference error: {err}")))?;
        let bound_param_types = infer_parameter_types(&inferred_parameter_types, parameter_types)?;

        Ok(PreparedStatement {
            kind: PreparedStatementKind::Query { plan: logical_plan },
            result_fields: fields,
            referenced_views: deduped,
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
        parameter_types: &[Option<PgType>],
    ) -> PgWireResult<PreparedStatement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        self.prepare_statement(sql, parameter_types).await
    }

    fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<PgType>> {
        Ok(stmt.parameter_types())
    }

    fn get_result_schema(
        &self,
        stmt: &Self::Statement,
        _column_format: Option<&Format>,
    ) -> PgWireResult<Vec<pgwire::api::results::FieldInfo>> {
        Ok(stmt.result_fields().as_ref().clone())
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

    pub(super) async fn execute_portal_query(
        &self,
        portal: &Portal<PreparedStatement>,
    ) -> PgWireResult<Response> {
        let PreparedStatementKind::Query { plan } = portal.statement.statement.kind() else {
            return Err(user_error("expected query statement"));
        };
        let inferred_types = plan
            .get_parameter_types()
            .map_err(|err| user_error(format!("DataFusion parameter inference error: {err}")))?;
        let param_values = deserialize_parameters(portal, &ordered_param_types(&inferred_types))?;
        let plan_with_values = plan
            .clone()
            .replace_params_with_values(&param_values)
            .map_err(|err| user_error(format!("DataFusion parameter binding error: {err}")))?;
        let optimized = self
            .state
            .query
            .session()
            .state()
            .optimize(&plan_with_values)
            .map_err(|err| user_error(format!("DataFusion optimization error: {err}")))?;
        let dataframe = self
            .state
            .query
            .session()
            .execute_logical_plan(optimized)
            .await
            .map_err(|err| user_error(format!("DataFusion execution error: {err}")))?;
        let response = encode_dataframe(dataframe, &portal.result_column_format, None).await?;
        Ok(Response::Query(response))
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
        if let PreparedStatementKind::Management(statement) = portal.statement.statement.kind() {
            return handle_management_statement(self.state.as_ref(), statement).await;
        }
        if let PreparedStatementKind::Noop { tag } = portal.statement.statement.kind() {
            return Ok(Response::Execution(pgwire::api::results::Tag::new(
                tag.as_str(),
            )));
        }

        self.state.refresh_catalog_shims().await?;
        for view in portal.statement.statement.referenced_views() {
            self.state.ensure_materialized_view_registered(view).await?;
        }
        self.execute_portal_query(portal).await
    }
}

fn infer_parameter_types(
    inferred_types: &HashMap<String, Option<DataType>>,
    client_types: &[Option<PgType>],
) -> PgWireResult<Vec<PgType>> {
    let mut result = Vec::new();
    for inferred in ordered_param_types(inferred_types) {
        let ty = match inferred {
            Some(data_type) => into_pg_type(data_type)?,
            None => PgType::UNKNOWN,
        };
        result.push(ty);
    }
    for (idx, ty) in client_types.iter().enumerate() {
        if idx < result.len()
            && let Some(ty) = ty
        {
            result[idx] = ty.clone();
        }
    }
    Ok(result)
}

fn ordered_param_types(types: &HashMap<String, Option<DataType>>) -> Vec<Option<&DataType>> {
    let mut ordered = types.iter().collect::<Vec<_>>();
    ordered.sort_by(|a, b| a.0.cmp(b.0));
    ordered.into_iter().map(|(_, ty)| ty.as_ref()).collect()
}
