use std::fmt::Debug;
use std::sync::Arc;

use arrow_pg::datatypes::df::encode_dataframe;
use async_trait::async_trait;
use floe_executor::tail::{execute_tail, parse_tail_sql, tail_output_schema};
use futures::Sink;
use pgwire::api::ClientInfo;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::portal::Format;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{QueryResponse, Response, Tag};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use sqlparser::ast::{Query, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio_util::sync::CancellationToken;

use crate::execution::FloeServerState;
use crate::management::{detect_single_management_statement, handle_management_statement};
use crate::protocol::bootstrap::{detect_noop_session_command, rewrite_bootstrap_sql};
use crate::sql::{extract_tables_from_query, is_system_catalog_relation, unqualified_table_name};
use crate::tail::{TailResponseStream, detect_single_tail_statement};
use crate::types::arrow_schema_to_field_info;
use crate::{feature_not_supported_error, parse_error, planner_error, user_error};

pub(super) struct FloeQueryHandler {
    state: Arc<FloeServerState>,
}

impl FloeQueryHandler {
    pub(super) fn new(state: Arc<FloeServerState>) -> Self {
        Self { state }
    }

    async fn handle_select(&self, query: &Query) -> PgWireResult<Response> {
        self.ensure_materialized_views_in_query(query).await?;
        self.execute_sql_streaming(&query.to_string()).await
    }

    pub(super) async fn execute_statement(&self, statement: Statement) -> PgWireResult<Response> {
        match statement {
            Statement::CreateTable(_) => Err(feature_not_supported_error(
                "CREATE TABLE is not supported via the Floe pgwire endpoint",
            )),
            Statement::Insert(_) => Err(feature_not_supported_error(
                "INSERT is not supported; materialized views are read-only",
            )),
            Statement::Set(_) => Ok(Response::Execution(Tag::new("SET"))),
            Statement::StartTransaction { .. } => Ok(Response::Execution(Tag::new("BEGIN"))),
            Statement::Commit { .. } => Ok(Response::Execution(Tag::new("COMMIT"))),
            Statement::Rollback { .. } => Ok(Response::Execution(Tag::new("ROLLBACK"))),
            Statement::Query(query) => self.handle_select(&query).await,
            other => Err(feature_not_supported_error(format!(
                "unsupported statement: {other}"
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

    pub(super) async fn ensure_materialized_views_in_query(
        &self,
        query: &Query,
    ) -> PgWireResult<()> {
        let mut names = Vec::new();
        extract_tables_from_query(query, &mut names);
        for table in names {
            if is_system_catalog_relation(&table) {
                continue;
            }
            self.state
                .ensure_materialized_view_registered(unqualified_table_name(&table))
                .await?;
        }
        Ok(())
    }

    async fn plan_sql(&self, sql: &str) -> PgWireResult<datafusion::dataframe::DataFrame> {
        self.state
            .query
            .session()
            .sql(sql)
            .await
            .map_err(|err| planner_error(format!("DataFusion planning error: {err}")))
    }

    pub(super) async fn execute_sql_streaming(&self, sql: &str) -> PgWireResult<Response> {
        self.state.refresh_catalog_shims().await?;
        self.state.ensure_materialized_views_in_sql(sql).await?;
        let df = self.plan_sql(sql).await?;
        let response = encode_dataframe(df, &Format::UnifiedText, None).await?;
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
        if let Some(statement) = detect_single_management_statement(query) {
            let response = handle_management_statement(self.state.as_ref(), &statement).await?;
            return Ok(vec![response]);
        }
        if let Some(tag) = detect_noop_session_command(query) {
            return Ok(vec![Response::Execution(Tag::new(tag))]);
        }
        if let Some(rewritten) = rewrite_bootstrap_sql(query) {
            let response = self.execute_sql_streaming(&rewritten).await?;
            return Ok(vec![response]);
        }

        let dialect = PostgreSqlDialect {};
        let statements = Parser::parse_sql(&dialect, query)
            .map_err(|err| parse_error(format!("SQL parse error: {err}")))?;

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
