use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::execution::{FloeServerState, build_query_response};
use crate::user_error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ManagementStatement {
    ShowMaterializedViews,
    DescribeMaterializedView { name: String },
}

pub(crate) fn parse_management_statement(sql: &str) -> Option<ManagementStatement> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return None;
    }
    let statement = trimmed.trim_end_matches(|c: char| c.is_whitespace() || c == ';');
    if statement.is_empty() {
        return None;
    }
    let tokens = tokenize(statement);
    if tokens.len() == 3
        && tokens[0].eq_ignore_ascii_case("SHOW")
        && tokens[1].eq_ignore_ascii_case("MATERIALIZED")
        && (tokens[2].eq_ignore_ascii_case("VIEWS") || tokens[2].eq_ignore_ascii_case("VIEW"))
    {
        return Some(ManagementStatement::ShowMaterializedViews);
    }
    if tokens.len() == 4
        && tokens[0].eq_ignore_ascii_case("DESCRIBE")
        && tokens[1].eq_ignore_ascii_case("MATERIALIZED")
        && tokens[2].eq_ignore_ascii_case("VIEW")
    {
        let name = parse_identifier_token(&tokens[3])?;
        return Some(ManagementStatement::DescribeMaterializedView { name });
    }
    None
}

pub(crate) fn detect_single_management_statement(
    query: &str,
) -> Option<ManagementStatement> {
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
    parse_management_statement(statement)
}

pub(crate) fn management_result_schema(statement: &ManagementStatement) -> SchemaRef {
    match statement {
        ManagementStatement::ShowMaterializedViews => show_schema(),
        ManagementStatement::DescribeMaterializedView { .. } => describe_schema(),
    }
}

pub(crate) async fn handle_management_statement(
    state: &FloeServerState,
    statement: &ManagementStatement,
) -> PgWireResult<Response> {
    let batch = match statement {
        ManagementStatement::ShowMaterializedViews => {
            show_materialized_views_batch(state).await?
        }
        ManagementStatement::DescribeMaterializedView { name } => {
            describe_materialized_view_batch(state, name).await?
        }
    };
    let response = build_query_response(vec![batch])?;
    Ok(Response::Query(response))
}

fn show_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("query", DataType::Utf8, false),
        Field::new("if_not_exists", DataType::Boolean, false),
    ]))
}

fn describe_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("view_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("nullable", DataType::Boolean, false),
        Field::new("query", DataType::Utf8, false),
        Field::new("if_not_exists", DataType::Boolean, false),
    ]))
}

async fn show_materialized_views_batch(
    state: &FloeServerState,
) -> PgWireResult<RecordBatch> {
    let storage = state.query.storage();
    let mut views = storage
        .materialized_views()
        .await
        .map_err(|err| user_error(format!("failed to load materialized views: {err}")))?;
    views.sort_by(|a, b| a.name().cmp(b.name()));

    let names: Vec<String> = views.iter().map(|view| view.name().to_string()).collect();
    let queries: Vec<String> = views.iter().map(|view| view.query().to_string()).collect();
    let if_not_exists: Vec<bool> = views.iter().map(|view| view.if_not_exists()).collect();

    let schema = show_schema();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(names)),
        Arc::new(StringArray::from(queries)),
        Arc::new(BooleanArray::from(if_not_exists)),
    ];
    RecordBatch::try_new(schema, arrays)
        .map_err(|err| user_error(format!("failed to build SHOW results: {err}")))
}

async fn describe_materialized_view_batch(
    state: &FloeServerState,
    name: &str,
) -> PgWireResult<RecordBatch> {
    let storage = state.query.storage();
    let metadata = storage
        .materialized_view(name)
        .await
        .map_err(|err| user_error(format!("failed to load materialized view metadata: {err}")))?;
    let Some(metadata) = metadata else {
        return Err(user_error(format!(
            "materialized view '{name}' does not exist"
        )));
    };

    let schema = load_view_schema(state, name).await?;
    let fields = schema.fields();
    let row_count = fields.len();

    let view_names = vec![name.to_string(); row_count];
    let column_names: Vec<String> = fields.iter().map(|field| field.name().to_string()).collect();
    let data_types: Vec<String> = fields
        .iter()
        .map(|field| field.data_type().to_string())
        .collect();
    let nullable: Vec<bool> = fields.iter().map(|field| field.is_nullable()).collect();
    let queries = vec![metadata.query().to_string(); row_count];
    let if_not_exists = vec![metadata.if_not_exists(); row_count];

    let schema = describe_schema();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(view_names)),
        Arc::new(StringArray::from(column_names)),
        Arc::new(StringArray::from(data_types)),
        Arc::new(BooleanArray::from(nullable)),
        Arc::new(StringArray::from(queries)),
        Arc::new(BooleanArray::from(if_not_exists)),
    ];
    RecordBatch::try_new(schema, arrays)
        .map_err(|err| user_error(format!("failed to build DESCRIBE results: {err}")))
}

async fn load_view_schema(state: &FloeServerState, name: &str) -> PgWireResult<SchemaRef> {
    if let Some(schema) = state.materialized_views.schema(name) {
        return Ok(schema);
    }
    let storage = state.query.storage();
    let schema = storage
        .materialized_view_schema(name)
        .await
        .map_err(|err| user_error(format!("failed to load materialized view schema: {err}")))?;
    match schema {
        Some(schema) => {
            state.materialized_views.set_schema(name.to_string(), Arc::clone(&schema));
            Ok(schema)
        }
        None => Err(user_error(format!(
            "materialized view '{name}' is missing schema metadata"
        ))),
    }
}

fn tokenize(sql: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    for ch in sql.chars() {
        match ch {
            '"' => {
                in_quotes = !in_quotes;
                current.push(ch);
            }
            ch if ch.is_whitespace() && !in_quotes => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
            }
            _ => current.push(ch),
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

fn parse_identifier_token(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        let inner = &trimmed[1..trimmed.len() - 1];
        Some(inner.replace("\"\"", "\""))
    } else {
        Some(trimmed.to_ascii_lowercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_show_materialized_views() {
        let stmt = parse_management_statement("SHOW MATERIALIZED VIEWS");
        assert_eq!(stmt, Some(ManagementStatement::ShowMaterializedViews));
        let stmt = parse_management_statement("show materialized view");
        assert_eq!(stmt, Some(ManagementStatement::ShowMaterializedViews));
    }

    #[test]
    fn parses_describe_materialized_view() {
        let stmt = parse_management_statement("DESCRIBE MATERIALIZED VIEW mv_test");
        assert_eq!(
            stmt,
            Some(ManagementStatement::DescribeMaterializedView {
                name: "mv_test".to_string()
            })
        );
        let stmt = parse_management_statement("DESCRIBE MATERIALIZED VIEW \"Mv_Case\"");
        assert_eq!(
            stmt,
            Some(ManagementStatement::DescribeMaterializedView {
                name: "Mv_Case".to_string()
            })
        );
    }
}
