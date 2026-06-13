use std::collections::BTreeSet;

use anyhow::{Result, anyhow};
use sqlparser::ast::{
    ColumnOption, DataType, ExactNumberInfo, Expr, ObjectName, OrderByExpr, Query, Select, SetExpr,
    Statement, TableConstraint, TableFactor, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::definitions::{
    CreateSourceDefinition, CreateTableColumnDefinition, CreateTableDefinition,
    CreateTableSourceDefinition, FileSourceOptions, FloeStatement, GeneratorSourceOptions,
    HttpSourceOptions, KafkaSourceOptions, MaterializedViewDefinition, ObjectStoreSourceOptions,
    PostgresCdcSchemaEvolutionPolicy, PostgresCdcSourceOptions, ReplicationBufferMode,
    ReplicationBufferPolicy, ReplicationErrorPolicy, ReplicationErrorPolicyMode,
    ReplicationPipelineDefinition, ReplicationPipelineFormat, ReplicationPipelineTarget,
    SinkConnector, SinkDefinition, SinkOptions, SourceConnector, SqlColumnType,
};

#[path = "parser/options.rs"]
mod options;
#[path = "parser/statements.rs"]
mod statements;
#[cfg(test)]
#[path = "parser/tests.rs"]
mod tests;

use options::*;
pub use statements::parse_create_table;
use statements::*;
pub fn parse_floe_statement(sql: &str) -> Result<FloeStatement> {
    let mut statements = parse_floe_program(sql)?;
    if statements.len() != 1 {
        return Err(anyhow!("SQL text must contain exactly one statement"));
    }
    Ok(statements.remove(0))
}

pub fn parse_floe_program(sql: &str) -> Result<Vec<FloeStatement>> {
    let mut statements = Vec::new();
    for statement in split_sql_statements(sql)? {
        let normalized = normalize_sql(&statement)?;
        if starts_with_keyword(normalized, "CREATE SOURCE") {
            let definition = parse_create_source(normalized)?;
            statements.push(FloeStatement::CreateSource(definition));
            continue;
        }
        if starts_with_keyword(normalized, "CREATE SINK") {
            let definition = parse_sink_statement(normalized)?;
            statements.push(FloeStatement::CreateSink(definition));
            continue;
        }
        if starts_with_keyword(normalized, "CREATE REPLICATION PIPELINE") {
            let definition = parse_replication_pipeline_statement(normalized)?;
            statements.push(FloeStatement::CreateReplicationPipeline(definition));
            continue;
        }
        if starts_with_keyword(normalized, "CREATE TABLE") {
            let definition = parse_create_table(normalized)?;
            statements.push(FloeStatement::CreateTable(definition));
            continue;
        }
        if starts_with_keyword(normalized, "CREATE") {
            let definition = parse_materialized_view(normalized)?;
            statements.push(FloeStatement::CreateMaterializedView(definition));
            continue;
        }
        if starts_with_keyword(normalized, "SUBSCRIBE") {
            statements.push(parse_subscribe_statement(normalized)?);
            continue;
        }
        return Err(anyhow!("unsupported SQL statement: {normalized}"));
    }
    if statements.is_empty() {
        return Err(anyhow!("SQL program cannot be empty"));
    }
    Ok(statements)
}

pub fn parse_create_source(sql: &str) -> Result<CreateSourceDefinition> {
    let normalized = normalize_sql(sql)?;
    let mut rest = consume_keyword(normalized, "CREATE")
        .ok_or_else(|| anyhow!("expected CREATE at start of source statement"))?;
    rest = consume_keyword(rest, "SOURCE")
        .ok_or_else(|| anyhow!("expected SOURCE after CREATE in source statement"))?;
    let if_not_exists = if let Some(after_if) = consume_keyword(rest, "IF") {
        let after_not = consume_keyword(after_if, "NOT")
            .ok_or_else(|| anyhow!("expected NOT after IF in CREATE SOURCE IF NOT EXISTS"))?;
        let after_exists = consume_keyword(after_not, "EXISTS")
            .ok_or_else(|| anyhow!("expected EXISTS after IF NOT in CREATE SOURCE"))?;
        rest = after_exists;
        true
    } else {
        false
    };
    let (next, source_name) = parse_identifier(rest)?;
    rest = next;
    let (next, columns) = parse_optional_source_schema(rest, &source_name)?;
    rest = next;
    reject_unsupported_source_clauses_before_with(rest)?;
    rest = consume_keyword(rest, "WITH")
        .ok_or_else(|| anyhow!("expected WITH (...) in CREATE SOURCE statement"))?;
    let (next, options) = parse_parenthesized_options(rest)?;
    rest = next;
    let (next, format_clause) = parse_optional_source_format(rest)?;
    rest = next;

    if !rest.trim().is_empty() {
        return Err(anyhow!(
            "unexpected tokens after CREATE SOURCE statement: {}",
            rest.trim()
        ));
    }

    let connector = option_any(&options, &["connector", "type"])
        .ok_or_else(|| anyhow!("CREATE SOURCE requires connector/type option"))?
        .to_ascii_lowercase()
        .replace('-', "_");
    let connector = match connector.as_str() {
        "kafka" => {
            validate_risingwave_kafka_options(&options)?;
            SourceConnector::Kafka(KafkaSourceOptions::new(
                option_any(
                    &options,
                    &[
                        "brokers",
                        "properties.bootstrap.server",
                        "bootstrap.servers",
                        "bootstrap.server",
                    ],
                )
                .ok_or_else(|| {
                    anyhow!(
                        "CREATE SOURCE kafka requires brokers/properties.bootstrap.server option"
                    )
                })?
                .to_string(),
                option_any(&options, &["topics", "topic"])
                    .map(|value| parse_string_list_option("topics", value))
                    .transpose()?
                    .ok_or_else(|| anyhow!("CREATE SOURCE kafka requires topics/topic"))?,
                option_any(&options, &["group_id", "group.id"]).map(ToString::to_string),
                option_any(&options, &["default_source", "source"]).map(ToString::to_string),
                option_any(&options, &["poll_ms", "poll.ms"])
                    .map(|value| parse_positive_u64_option("poll_ms", value))
                    .transpose()?,
                option_any(&options, &["max_messages_per_tick", "max_messages"])
                    .map(|value| parse_positive_usize_option("max_messages_per_tick", value))
                    .transpose()?,
                merge_source_format_option(
                    option_any(&options, &["format"]).map(normalize_sink_format),
                    format_clause.as_ref(),
                )?,
            )?)
        }
        "file" => {
            ensure_source_format_is_plain_json(format_clause.as_ref(), "file")?;
            SourceConnector::File(FileSourceOptions::new(
                required_source_option(&options, "path")?.to_string(),
                option_any(&options, &["default_source", "source"]).map(ToString::to_string),
            )?)
        }
        "http" => {
            ensure_source_format_is_plain_json(format_clause.as_ref(), "http")?;
            SourceConnector::Http(HttpSourceOptions::new(
                option_any(&options, &["host", "hostname"]).map(ToString::to_string),
                option_any(&options, &["port"])
                    .map(|value| parse_u16_option("port", value))
                    .transpose()?
                    .ok_or_else(|| anyhow!("CREATE SOURCE http requires port"))?,
                option_any(&options, &["default_source", "source"]).map(ToString::to_string),
            )?)
        }
        "generator" | "nexmark" => {
            if format_clause.is_some() {
                return Err(anyhow!(
                    "CREATE SOURCE generator uses built-in Nexmark encoding; omit FORMAT/ENCODE"
                ));
            }
            if !columns.is_empty() {
                return Err(anyhow!(
                    "CREATE SOURCE generator uses built-in Nexmark schemas; omit inline schema"
                ));
            }
            SourceConnector::Generator(GeneratorSourceOptions::new(
                option_any(&options, &["events_per_second", "events.per_second"])
                    .map(|value| parse_f64_option("events_per_second", value))
                    .transpose()?,
                option_any(&options, &["max_events", "max.events"])
                    .map(|value| parse_u64_option("max_events", value))
                    .transpose()?,
            )?)
        }
        "object_store" | "objectstore" => {
            ensure_source_format_is_plain_json(format_clause.as_ref(), "object_store")?;
            SourceConnector::ObjectStore(ObjectStoreSourceOptions::new(
                required_source_option(&options, "url")?.to_string(),
                option_any(&options, &["default_source", "source"]).map(ToString::to_string),
            )?)
        }
        "postgres_cdc" => {
            if format_clause.is_some() {
                return Err(anyhow!(
                    "CREATE SOURCE postgres-cdc uses native CDC encoding; omit FORMAT/ENCODE"
                ));
            }
            if !columns.is_empty() {
                return Err(anyhow!(
                    "CREATE SOURCE postgres-cdc does not accept inline schema; use CREATE TABLE ... FROM for CDC tables"
                ));
            }
            SourceConnector::PostgresCdc(PostgresCdcSourceOptions::new_with_setup_policy(
                postgres_connection_string_from_options(&options)?,
                option_any(&options, &["slot.name", "slot"])
                    .ok_or_else(|| anyhow!("CREATE SOURCE postgres-cdc requires slot.name/slot"))?
                    .to_string(),
                option_any(&options, &["publication.name", "publication"]).map(ToString::to_string),
                options
                    .get("include_schema_in_source")
                    .map(|value| parse_bool_option("include_schema_in_source", value))
                    .transpose()?,
                postgres_schema_evolution_policy_from_options(&options)?,
                option_any(
                    &options,
                    &["slot.create", "slot.auto_create", "auto_create_slot"],
                )
                .map(|value| parse_bool_option("slot.create", value))
                .transpose()?
                .unwrap_or(true),
                option_any(
                    &options,
                    &[
                        "publication.create",
                        "publication.auto_create",
                        "auto_create_publication",
                    ],
                )
                .map(|value| parse_bool_option("publication.create", value))
                .transpose()?
                .unwrap_or(true),
            )?)
        }
        other => return Err(anyhow!("unsupported source connector type '{other}'")),
    };

    CreateSourceDefinition::new_with_columns_and_if_not_exists(
        source_name,
        connector,
        columns,
        if_not_exists,
    )
}

fn reject_unsupported_source_clauses_before_with(rest: &str) -> Result<()> {
    let trimmed = rest.trim_start();
    if starts_with_keyword(trimmed, "INCLUDE") {
        return Err(anyhow!(
            "CREATE SOURCE INCLUDE clauses are not supported by Floe"
        ));
    }
    Ok(())
}

fn validate_risingwave_kafka_options(
    options: &std::collections::HashMap<String, String>,
) -> Result<()> {
    if let Some(mode) = option_any(options, &["scan.startup.mode"]) {
        let normalized = mode.to_ascii_lowercase().replace('-', "_");
        if normalized != "earliest" {
            return Err(anyhow!(
                "CREATE SOURCE kafka supports only scan.startup.mode = 'earliest'"
            ));
        }
    }
    for (key, expected) in [
        ("properties.fetch.wait.max.ms", "1"),
        ("properties.fetch.queue.backoff.ms", "1"),
        ("properties.fetch.min.bytes", "1"),
    ] {
        if let Some(value) = options.get(key)
            && value != expected
        {
            return Err(anyhow!(
                "CREATE SOURCE kafka option {key} is fixed to {expected} in Floe"
            ));
        }
    }
    Ok(())
}

fn ensure_source_format_is_plain_json(
    format: Option<&SourceFormatClause>,
    connector: &str,
) -> Result<()> {
    if let Some(format) = format
        && !format.is_plain_json()
    {
        return Err(anyhow!(
            "CREATE SOURCE {connector} currently supports only FORMAT PLAIN ENCODE JSON"
        ));
    }
    Ok(())
}

pub fn parse_materialized_view(sql: &str) -> Result<MaterializedViewDefinition> {
    let normalized = normalize_sql(sql)?;
    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, normalized)
        .map_err(|err| anyhow!("failed to parse materialized view statement: {err}"))?;

    if statements.is_empty() {
        return Err(anyhow!("materialized view definition cannot be empty"));
    }
    if statements.len() != 1 {
        return Err(anyhow!(
            "materialized view definition cannot contain multiple statements"
        ));
    }

    let statement = statements.remove(0);
    match statement {
        Statement::CreateView {
            or_alter,
            or_replace,
            materialized,
            name,
            columns,
            query,
            options: _,
            cluster_by,
            comment,
            with_no_schema_binding,
            if_not_exists,
            temporary,
            to,
            params,
            secure,
            name_before_not_exists,
        } => {
            if !materialized {
                return Err(anyhow!("expected CREATE MATERIALIZED VIEW statement"));
            }
            if or_alter || or_replace || temporary {
                return Err(anyhow!(
                    "CREATE MATERIALIZED VIEW does not support OR ALTER/REPLACE or TEMPORARY"
                ));
            }
            if !columns.is_empty() {
                return Err(anyhow!(
                    "column lists are not supported in materialized view definitions"
                ));
            }
            if !cluster_by.is_empty()
                || comment.is_some()
                || with_no_schema_binding
                || to.is_some()
                || params.is_some()
                || secure
                || name_before_not_exists
            {
                return Err(anyhow!(
                    "unsupported CREATE MATERIALIZED VIEW options are present"
                ));
            }

            let name = object_name_to_string(&name)?;
            let query = query.to_string();
            if query.trim().is_empty() {
                return Err(anyhow!("materialized view requires a SELECT query"));
            }

            Ok(MaterializedViewDefinition::new(name, query, if_not_exists))
        }
        _ => Err(anyhow!("expected CREATE MATERIALIZED VIEW statement")),
    }
}

pub fn referenced_table_names_in_query(query_sql: &str) -> Result<BTreeSet<String>> {
    let normalized = normalize_sql(query_sql)?;
    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, normalized)
        .map_err(|err| anyhow!("failed to parse query table references: {err}"))?;
    if statements.len() != 1 {
        return Err(anyhow!("query must contain exactly one statement"));
    }
    let Statement::Query(query) = statements.remove(0) else {
        return Err(anyhow!("expected SELECT query"));
    };

    let mut references = BTreeSet::new();
    collect_query_table_references(&query, &mut references)?;
    Ok(references)
}

fn collect_query_table_references(query: &Query, references: &mut BTreeSet<String>) -> Result<()> {
    let mut cte_names = BTreeSet::new();
    if let Some(with) = query.with.as_ref() {
        for cte in &with.cte_tables {
            collect_query_table_references(&cte.query, references)?;
            cte_names.insert(cte.alias.name.value.clone());
        }
    }
    collect_set_expr_table_references(&query.body, references, &cte_names)
}

fn collect_set_expr_table_references(
    expr: &SetExpr,
    references: &mut BTreeSet<String>,
    cte_names: &BTreeSet<String>,
) -> Result<()> {
    match expr {
        SetExpr::Select(select) => collect_select_table_references(select, references, cte_names),
        SetExpr::Query(query) => collect_query_table_references(query, references),
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_expr_table_references(left, references, cte_names)?;
            collect_set_expr_table_references(right, references, cte_names)
        }
        SetExpr::Table(table) => {
            let Some(table_name) = table.table_name.as_ref() else {
                return Ok(());
            };
            let reference = table
                .schema_name
                .as_ref()
                .map(|schema| format!("{schema}.{table_name}"))
                .unwrap_or_else(|| table_name.clone());
            if !cte_names.contains(&reference) {
                references.insert(reference);
            }
            Ok(())
        }
        SetExpr::Values(_)
        | SetExpr::Insert(_)
        | SetExpr::Update(_)
        | SetExpr::Delete(_)
        | SetExpr::Merge(_) => Ok(()),
    }
}

fn collect_select_table_references(
    select: &Select,
    references: &mut BTreeSet<String>,
    cte_names: &BTreeSet<String>,
) -> Result<()> {
    for table in &select.from {
        collect_table_with_joins_references(table, references, cte_names)?;
    }
    Ok(())
}

fn collect_table_with_joins_references(
    table: &TableWithJoins,
    references: &mut BTreeSet<String>,
    cte_names: &BTreeSet<String>,
) -> Result<()> {
    collect_table_factor_references(&table.relation, references, cte_names)?;
    for join in &table.joins {
        collect_table_factor_references(&join.relation, references, cte_names)?;
    }
    Ok(())
}

fn collect_table_factor_references(
    table: &TableFactor,
    references: &mut BTreeSet<String>,
    cte_names: &BTreeSet<String>,
) -> Result<()> {
    match table {
        TableFactor::Table { name, args, .. } => {
            if args.is_none() {
                let reference = object_name_to_string(name)?;
                if !cte_names.contains(&reference) {
                    references.insert(reference);
                }
            }
        }
        TableFactor::Derived { subquery, .. } => {
            collect_query_table_references(subquery, references)?;
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_table_with_joins_references(table_with_joins, references, cte_names)?;
        }
        TableFactor::Pivot { table, .. } | TableFactor::Unpivot { table, .. } => {
            collect_table_factor_references(table, references, cte_names)?;
        }
        TableFactor::TableFunction { .. }
        | TableFactor::Function { .. }
        | TableFactor::UNNEST { .. }
        | TableFactor::JsonTable { .. }
        | TableFactor::OpenJsonTable { .. }
        | TableFactor::MatchRecognize { .. }
        | TableFactor::XmlTable { .. }
        | TableFactor::SemanticView { .. } => {}
    }
    Ok(())
}
