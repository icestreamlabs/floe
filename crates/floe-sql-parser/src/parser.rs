use std::collections::BTreeSet;

use anyhow::{Result, anyhow};
use sqlparser::ast::{
    ColumnOption, DataType, Expr, ObjectName, OrderByExpr, Query, Select, SetExpr, Statement,
    TableConstraint, TableFactor, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::definitions::{
    CreateSourceDefinition, CreateTableColumnDefinition, CreateTableDefinition,
    CreateTableSourceDefinition, FloeStatement, MaterializedViewDefinition,
    PostgresCdcSchemaEvolutionPolicy, PostgresCdcSourceOptions, ReplicationBufferMode,
    ReplicationBufferPolicy, ReplicationPipelineDefinition, ReplicationPipelineFormat,
    ReplicationPipelineTarget, SinkConnector, SinkDefinition, SourceConnector, SqlColumnType,
};

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
        if starts_with_keyword(normalized, "TAIL") {
            statements.push(parse_tail_statement(normalized)?);
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
    let (next, source_name) = parse_identifier(rest)?;
    rest = next;
    rest = consume_keyword(rest, "WITH")
        .ok_or_else(|| anyhow!("expected WITH (...) in CREATE SOURCE statement"))?;
    let (next, options) = parse_parenthesized_options(rest)?;
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
        "postgres_cdc" => SourceConnector::PostgresCdc(
            PostgresCdcSourceOptions::new_with_schema_evolution_policy(
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
            )?,
        ),
        other => return Err(anyhow!("unsupported source connector type '{other}'")),
    };

    CreateSourceDefinition::new(source_name, connector)
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

pub fn parse_create_table(sql: &str) -> Result<CreateTableDefinition> {
    let normalized = normalize_sql(sql)?;
    let (table_sql, source) = split_create_table_source_clause(normalized)?;
    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, &table_sql)
        .map_err(|err| anyhow!("failed to parse create table statement: {err}"))?;
    if statements.len() != 1 {
        return Err(anyhow!(
            "CREATE TABLE definition cannot contain multiple statements"
        ));
    }
    let statement = statements.remove(0);
    let Statement::CreateTable(create_table) = statement else {
        return Err(anyhow!("expected CREATE TABLE statement"));
    };
    if create_table.or_replace
        || create_table.temporary
        || create_table.external
        || create_table.dynamic
        || create_table.global.is_some()
        || create_table.transient
        || create_table.volatile
        || create_table.iceberg
    {
        return Err(anyhow!("unsupported CREATE TABLE modifiers are present"));
    }
    if create_table.query.is_some() || create_table.like.is_some() || create_table.clone.is_some() {
        return Err(anyhow!(
            "CREATE TABLE AS/LIKE/CLONE forms are not supported by Floe"
        ));
    }
    if create_table.columns.is_empty() {
        return Err(anyhow!("CREATE TABLE must declare at least one column"));
    }
    if create_table.if_not_exists {
        return Err(anyhow!(
            "CREATE TABLE IF NOT EXISTS is not yet supported in Floe SQL programs"
        ));
    }

    let table_name = object_name_to_string(&create_table.name)?;
    let mut pk_names = primary_key_columns(&create_table.constraints)?;
    let mut columns = Vec::with_capacity(create_table.columns.len());
    for column in &create_table.columns {
        let name = column.name.value.clone();
        let mut nullable = true;
        let mut primary_key = false;
        for option in &column.options {
            match &option.option {
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Null => nullable = true,
                ColumnOption::Unique { is_primary, .. } if *is_primary => {
                    primary_key = true;
                    nullable = false;
                }
                _ => {}
            }
        }
        if pk_names.contains(&name) {
            primary_key = true;
            nullable = false;
        }
        let data_type = parse_table_column_type(&column.data_type, &table_name, &name)?;
        columns.push(CreateTableColumnDefinition::new(
            name,
            data_type,
            nullable,
            primary_key,
        ));
    }
    if !pk_names.is_empty() {
        pk_names.retain(|pk_name| columns.iter().all(|column| column.name() != pk_name));
        if !pk_names.is_empty() {
            return Err(anyhow!(
                "primary key columns not declared in table {}: {}",
                table_name,
                pk_names.join(", ")
            ));
        }
    }
    CreateTableDefinition::new_with_source(table_name, columns, source)
}

fn split_create_table_source_clause(
    sql: &str,
) -> Result<(String, Option<CreateTableSourceDefinition>)> {
    let Some(from_idx) = find_top_level_keyword(sql, "FROM") else {
        return Ok((sql.to_string(), None));
    };
    let table_sql = sql[..from_idx].trim().to_string();
    let mut rest = &sql[from_idx..];
    rest = consume_keyword(rest, "FROM")
        .ok_or_else(|| anyhow!("expected FROM in source-backed CREATE TABLE statement"))?;
    let (next, source_name) = parse_identifier(rest)?;
    rest = next;
    rest = consume_keyword(rest, "TABLE")
        .ok_or_else(|| anyhow!("expected TABLE after source name in CREATE TABLE FROM clause"))?;
    let (next, upstream_table) = parse_table_reference_literal(rest)?;
    rest = next;
    if !rest.trim().is_empty() {
        return Err(anyhow!(
            "unexpected tokens after CREATE TABLE FROM clause: {}",
            rest.trim()
        ));
    }
    Ok((
        table_sql,
        Some(CreateTableSourceDefinition::new(
            source_name,
            upstream_table,
        )?),
    ))
}

fn parse_table_reference_literal(input: &str) -> Result<(&str, String)> {
    let trimmed = input.trim_start();
    if trimmed.starts_with('\'') {
        parse_single_quoted_literal(trimmed)
    } else {
        parse_identifier(trimmed)
    }
}

fn primary_key_columns(constraints: &[TableConstraint]) -> Result<Vec<String>> {
    let mut primary_key_columns = Vec::new();
    for constraint in constraints {
        if let TableConstraint::PrimaryKey { columns, .. } = constraint {
            for column in columns {
                primary_key_columns.push(primary_key_column_name(&column.column)?);
            }
        }
    }
    if primary_key_columns.len() > 1 {
        return Err(anyhow!(
            "Floe currently supports exactly one primary key column"
        ));
    }
    Ok(primary_key_columns)
}

fn primary_key_column_name(column: &OrderByExpr) -> Result<String> {
    match &column.expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|part| part.value.clone())
            .ok_or_else(|| anyhow!("primary key identifier cannot be empty")),
        other => Err(anyhow!(
            "unsupported primary key expression '{other}'; expected a column identifier"
        )),
    }
}

fn parse_table_column_type(
    data_type: &DataType,
    table_name: &str,
    column_name: &str,
) -> Result<SqlColumnType> {
    let parsed = match data_type {
        DataType::Int(_)
        | DataType::Integer(_)
        | DataType::BigInt(_)
        | DataType::Int8(_)
        | DataType::Int64 => SqlColumnType::Int64,
        DataType::Boolean | DataType::Bool => SqlColumnType::Bool,
        DataType::Varchar(_)
        | DataType::Char(_)
        | DataType::Character(_)
        | DataType::Text
        | DataType::String(_) => SqlColumnType::Utf8,
        DataType::Timestamp(_, _) | DataType::Datetime(_) | DataType::TimestampNtz => {
            SqlColumnType::TimestampMillis
        }
        DataType::Date | DataType::Date32 => SqlColumnType::DateDays,
        DataType::Numeric(_)
        | DataType::Decimal(_)
        | DataType::DecimalUnsigned(_)
        | DataType::BigNumeric(_)
        | DataType::BigDecimal(_)
        | DataType::Dec(_) => SqlColumnType::Numeric,
        other => {
            return Err(anyhow!(
                "unsupported type '{other}' for column '{}' in table '{}'; supported: INT64, BOOL, UTF8/TEXT, TIMESTAMP, DATE, NUMERIC",
                column_name,
                table_name
            ));
        }
    };
    Ok(parsed)
}

fn parse_sink_statement(sql: &str) -> Result<SinkDefinition> {
    let mut rest = consume_keyword(sql, "CREATE")
        .ok_or_else(|| anyhow!("expected CREATE at start of sink statement"))?;
    rest = consume_keyword(rest, "SINK")
        .ok_or_else(|| anyhow!("expected SINK after CREATE in sink statement"))?;
    let (next, sink_name) = parse_identifier(rest)?;
    rest = next;
    rest = consume_keyword(rest, "FROM")
        .ok_or_else(|| anyhow!("expected FROM in CREATE SINK statement"))?;
    let (next, mv_name) = parse_identifier(rest)?;
    rest = next;
    rest = consume_keyword(rest, "WITH")
        .ok_or_else(|| anyhow!("expected WITH (...) in CREATE SINK statement"))?;
    let (next, options) = parse_parenthesized_options(rest)?;
    rest = next;

    if !rest.trim().is_empty() {
        return Err(anyhow!(
            "unexpected tokens after CREATE SINK statement: {}",
            rest.trim()
        ));
    }

    let connector = options
        .get("connector")
        .or_else(|| options.get("type"))
        .ok_or_else(|| anyhow!("CREATE SINK requires connector/type option"))?
        .to_ascii_lowercase();

    let with_snapshot = if let Some(value) = options.get("with_snapshot") {
        parse_bool_option("with_snapshot", value)?
    } else {
        false
    };
    let as_of = if let Some(value) = options.get("as_of") {
        Some(parse_i64_option("as_of", value)?)
    } else {
        None
    };

    let connector = match connector.as_str() {
        "kafka" => {
            let brokers = required_option(&options, "brokers")?.to_string();
            let topic = required_option(&options, "topic")?.to_string();
            let format = option_any(&options, &["format"]).map(normalize_sink_format);
            let key_columns = option_any(
                &options,
                &["key_columns", "key.columns", "primary_key", "primary.key"],
            )
            .map(parse_column_list_option)
            .transpose()?
            .unwrap_or_default();
            SinkConnector::Kafka {
                brokers,
                topic,
                format,
                key_columns,
            }
        }
        "file" => {
            let path = required_option(&options, "path")?.to_string();
            let append = options
                .get("append")
                .map(|value| parse_bool_option("append", value))
                .transpose()?;
            SinkConnector::File { path, append }
        }
        "http" => {
            let url = required_option(&options, "url")?.to_string();
            let batch_size = options
                .get("batch_size")
                .map(|value| parse_usize_option("batch_size", value))
                .transpose()?;
            SinkConnector::Http { url, batch_size }
        }
        other => return Err(anyhow!("unsupported sink connector type '{other}'")),
    };

    Ok(SinkDefinition::new(
        sink_name,
        mv_name,
        connector,
        with_snapshot,
        as_of,
    ))
}

fn parse_replication_pipeline_statement(sql: &str) -> Result<ReplicationPipelineDefinition> {
    let mut rest = consume_keyword(sql, "CREATE")
        .ok_or_else(|| anyhow!("expected CREATE at start of replication pipeline statement"))?;
    rest = consume_keyword(rest, "REPLICATION")
        .ok_or_else(|| anyhow!("expected REPLICATION after CREATE in pipeline statement"))?;
    rest = consume_keyword(rest, "PIPELINE")
        .ok_or_else(|| anyhow!("expected PIPELINE after CREATE REPLICATION"))?;
    let (next, pipeline_name) = parse_identifier(rest)?;
    rest = next;
    rest = consume_keyword(rest, "FROM")
        .ok_or_else(|| anyhow!("expected FROM in CREATE REPLICATION PIPELINE statement"))?;
    let (next, source_name) = parse_identifier(rest)?;
    rest = next;
    rest = consume_keyword(rest, "TABLE")
        .ok_or_else(|| anyhow!("expected TABLE after source name in replication pipeline"))?;
    let (next, upstream_table) = parse_table_reference_literal(rest)?;
    rest = next;
    rest = consume_keyword(rest, "INTO")
        .ok_or_else(|| anyhow!("expected INTO in CREATE REPLICATION PIPELINE statement"))?;
    let (next, target_name) = parse_identifier(rest)?;
    rest = next;
    rest = consume_keyword(rest, "WITH")
        .ok_or_else(|| anyhow!("expected WITH (...) in CREATE REPLICATION PIPELINE statement"))?;
    let (next, options) = parse_parenthesized_options(rest)?;
    rest = next;

    if !rest.trim().is_empty() {
        return Err(anyhow!(
            "unexpected tokens after CREATE REPLICATION PIPELINE statement: {}",
            rest.trim()
        ));
    }

    let format = match option_any(&options, &["format"])
        .unwrap_or("floe_json")
        .to_ascii_lowercase()
        .replace('-', "_")
        .as_str()
    {
        "floe_json" | "compact_json" => ReplicationPipelineFormat::FloeJson,
        "debezium_json" => ReplicationPipelineFormat::DebeziumJson,
        "arrow_ipc" => ReplicationPipelineFormat::ArrowIpc,
        other => return Err(anyhow!("unsupported replication pipeline format '{other}'")),
    };
    let buffer_mode = if option_any(&options, &["durable_buffer"])
        .map(|value| parse_bool_option("durable_buffer", value))
        .transpose()?
        .unwrap_or(true)
    {
        ReplicationBufferMode::Durable
    } else {
        ReplicationBufferMode::NoBuffer
    };
    let buffer_policy = ReplicationBufferPolicy::new(
        option_any(
            &options,
            &["buffer.max_pending_bytes", "buffer_max_pending_bytes"],
        )
        .map(|value| parse_usize_option("buffer.max_pending_bytes", value))
        .transpose()?,
        option_any(
            &options,
            &["buffer.max_pending_records", "buffer_max_pending_records"],
        )
        .map(|value| parse_usize_option("buffer.max_pending_records", value))
        .transpose()?,
        option_any(
            &options,
            &[
                "buffer.max_pending_transactions",
                "buffer.max_pending_objects",
                "buffer_max_pending_transactions",
                "buffer_max_pending_objects",
            ],
        )
        .map(|value| parse_usize_option("buffer.max_pending_transactions", value))
        .transpose()?,
        option_any(
            &options,
            &["buffer.max_pending_age_ms", "buffer_max_pending_age_ms"],
        )
        .map(|value| parse_u64_option("buffer.max_pending_age_ms", value))
        .transpose()?,
    );
    let emit_tombstones = option_any(
        &options,
        &["emit_tombstones", "tombstones", "delete.tombstones"],
    )
    .map(|value| parse_bool_option("emit_tombstones", value))
    .transpose()?
    .unwrap_or(false);
    let include_transaction_metadata = option_any(
        &options,
        &["include_transaction_metadata", "transaction_metadata"],
    )
    .map(|value| parse_bool_option("include_transaction_metadata", value))
    .transpose()?
    .unwrap_or(false);

    let target = match target_name.to_ascii_lowercase().replace('-', "_").as_str() {
        "kafka" => ReplicationPipelineTarget::Kafka {
            brokers: required_replication_option(&options, "brokers")?.to_string(),
            topic: required_replication_option(&options, "topic")?.to_string(),
        },
        "postgres" | "postgresql" => ReplicationPipelineTarget::Postgres {
            connection: option_any(&options, &["connection", "connection_string", "url"])
                .ok_or_else(|| {
                    anyhow!(
                        "CREATE REPLICATION PIPELINE requires 'connection' option for Postgres target"
                    )
                })?
                .to_string(),
            table: option_any(&options, &["table", "target.table", "target_table"])
                .ok_or_else(|| {
                    anyhow!(
                        "CREATE REPLICATION PIPELINE requires 'table' option for Postgres target"
                    )
                })?
                .to_string(),
        },
        other => return Err(anyhow!("unsupported replication pipeline target '{other}'")),
    };

    ReplicationPipelineDefinition::new(
        pipeline_name,
        source_name,
        upstream_table,
        target,
        format,
        buffer_mode,
        buffer_policy,
        emit_tombstones,
        include_transaction_metadata,
    )
}

fn postgres_connection_string_from_options(
    options: &std::collections::HashMap<String, String>,
) -> Result<String> {
    if let Some(connection) =
        option_any(options, &["connection", "connection_string", "dsn", "url"])
    {
        return Ok(connection.to_string());
    }

    let host = option_any(options, &["hostname", "host"])
        .ok_or_else(|| anyhow!("CREATE SOURCE postgres-cdc requires connection or hostname"))?;
    let port = option_any(options, &["port"]).unwrap_or("5432");
    let user = option_any(options, &["username", "user"])
        .ok_or_else(|| anyhow!("CREATE SOURCE postgres-cdc requires connection or username"))?;
    let database =
        option_any(options, &["database.name", "database", "dbname"]).ok_or_else(|| {
            anyhow!("CREATE SOURCE postgres-cdc requires connection or database.name")
        })?;
    let mut parts = vec![
        format!("host={host}"),
        format!("port={port}"),
        format!("user={user}"),
        format!("dbname={database}"),
    ];
    if let Some(password) = option_any(options, &["password"]) {
        parts.push(format!("password={password}"));
    }
    Ok(parts.join(" "))
}

fn postgres_schema_evolution_policy_from_options(
    options: &std::collections::HashMap<String, String>,
) -> Result<PostgresCdcSchemaEvolutionPolicy> {
    let Some(value) = option_any(
        options,
        &[
            "schema.evolution",
            "schema_evolution",
            "schema.evolution.policy",
            "schema_evolution_policy",
        ],
    ) else {
        return Ok(PostgresCdcSchemaEvolutionPolicy::FailFast);
    };

    let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "fail_fast" | "failfast" => Ok(PostgresCdcSchemaEvolutionPolicy::FailFast),
        "ignore_compatible" | "project_compatible" => {
            Ok(PostgresCdcSchemaEvolutionPolicy::IgnoreCompatible)
        }
        "apply_compatible_additions" | "apply_compatible" => {
            Ok(PostgresCdcSchemaEvolutionPolicy::ApplyCompatibleAdditions)
        }
        other => Err(anyhow!(
            "unsupported Postgres CDC schema evolution policy '{other}'; expected fail_fast, ignore_compatible, or apply_compatible_additions"
        )),
    }
}

fn parse_tail_statement(sql: &str) -> Result<FloeStatement> {
    let mut rest = consume_keyword(sql, "TAIL")
        .ok_or_else(|| anyhow!("expected TAIL at start of statement"))?;
    let (next, mv_name) = parse_identifier(rest)?;
    rest = next;

    let mut with_snapshot = false;
    if let Some(next) = consume_sequence(rest, &["WITH", "SNAPSHOT"]) {
        with_snapshot = true;
        rest = next;
    }

    let mut as_of = None;
    rest = rest.trim_start();
    if !rest.is_empty() {
        let Some(after_as) = consume_keyword(rest, "AS") else {
            return Err(anyhow!(
                "unexpected tokens after TAIL statement: {}",
                rest.trim()
            ));
        };
        let after_of = consume_keyword(after_as, "OF")
            .ok_or_else(|| anyhow!("expected OF after AS in TAIL statement"))?;
        let (next, version) = parse_integer_literal(after_of)?;
        as_of = Some(version);
        rest = next;
    }

    if !rest.trim().is_empty() {
        return Err(anyhow!(
            "unexpected tokens after TAIL statement: {}",
            rest.trim()
        ));
    }

    Ok(FloeStatement::Tail {
        mv_name,
        with_snapshot,
        as_of,
    })
}

fn parse_parenthesized_options(
    input: &str,
) -> Result<(&str, std::collections::HashMap<String, String>)> {
    let mut rest = input.trim_start();
    if !rest.starts_with('(') {
        return Err(anyhow!("expected '(' to start option list"));
    }
    rest = &rest[1..];

    let mut options = std::collections::HashMap::new();
    loop {
        rest = rest.trim_start();
        if let Some(after) = rest.strip_prefix(')') {
            return Ok((after, options));
        }
        let (next, key) = parse_option_key(rest)?;
        rest = next;
        rest = rest.trim_start();
        if !rest.starts_with('=') {
            return Err(anyhow!("expected '=' after option key '{key}'"));
        }
        rest = &rest[1..];
        let (next, value) = parse_option_value(rest)?;
        rest = next;
        let canonical = key.to_ascii_lowercase();
        if options.insert(canonical.clone(), value).is_some() {
            return Err(anyhow!("duplicate option '{canonical}' in WITH clause"));
        }

        rest = rest.trim_start();
        if let Some(after) = rest.strip_prefix(',') {
            rest = after;
            continue;
        }
        if let Some(after) = rest.strip_prefix(')') {
            return Ok((after, options));
        }
        return Err(anyhow!(
            "expected ',' or ')' after option assignment, found '{}'",
            rest
        ));
    }
}

fn consume_keyword<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = input.trim_start();
    if trimmed.len() < keyword.len() {
        return None;
    }
    let (candidate, rest) = trimmed.split_at(keyword.len());
    if !candidate.eq_ignore_ascii_case(keyword) {
        return None;
    }
    if !is_keyword_boundary(rest.chars().next()) {
        return None;
    }
    Some(rest)
}

fn consume_sequence<'a>(mut input: &'a str, keywords: &[&str]) -> Option<&'a str> {
    for keyword in keywords {
        input = consume_keyword(input, keyword)?;
    }
    Some(input)
}

fn is_keyword_boundary(ch: Option<char>) -> bool {
    match ch {
        None => true,
        Some(c) => c.is_whitespace() || matches!(c, '(' | ')' | ';' | ','),
    }
}

fn parse_identifier(input: &str) -> Result<(&str, String)> {
    let trimmed = input.trim_start();
    if trimmed.is_empty() {
        return Err(anyhow!("expected identifier"));
    }

    if trimmed.starts_with('"') {
        parse_quoted_identifier(trimmed)
    } else {
        parse_unquoted_identifier(trimmed)
    }
}

fn parse_quoted_identifier(input: &str) -> Result<(&str, String)> {
    let bytes = input.as_bytes();
    let mut value = String::new();
    let mut i = 1; // Skip opening quote

    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    value.push('"');
                    i += 2;
                } else {
                    let rest = &input[i + 1..];
                    return Ok((rest, value));
                }
            }
            ch => {
                value.push(ch as char);
                i += 1;
            }
        }
    }

    Err(anyhow!("unterminated quoted identifier"))
}

fn parse_unquoted_identifier(input: &str) -> Result<(&str, String)> {
    let mut end = 0;
    for byte in input.as_bytes() {
        let ch = *byte as char;
        if ch.is_alphanumeric() || ch == '_' || ch == '.' {
            end += 1;
        } else {
            break;
        }
    }

    if end == 0 {
        return Err(anyhow!("expected identifier"));
    }

    let name = &input[..end];
    let rest = &input[end..];
    Ok((rest, name.to_string()))
}

fn starts_with_keyword(input: &str, keyword: &str) -> bool {
    consume_keyword(input, keyword).is_some()
}

fn option_any<'a>(
    options: &'a std::collections::HashMap<String, String>,
    keys: &[&str],
) -> Option<&'a str> {
    keys.iter()
        .find_map(|key| options.get(*key).map(String::as_str))
}

fn find_top_level_keyword(input: &str, keyword: &str) -> Option<usize> {
    let chars: Vec<(usize, char)> = input.char_indices().collect();
    let mut depth = 0_i32;
    let mut in_single = false;
    let mut in_double = false;
    let mut idx = 0;

    while idx < chars.len() {
        let (byte_idx, ch) = chars[idx];
        if in_single {
            if ch == '\'' {
                if idx + 1 < chars.len() && chars[idx + 1].1 == '\'' {
                    idx += 1;
                } else {
                    in_single = false;
                }
            }
        } else if in_double {
            if ch == '"' {
                if idx + 1 < chars.len() && chars[idx + 1].1 == '"' {
                    idx += 1;
                } else {
                    in_double = false;
                }
            }
        } else {
            match ch {
                '\'' => in_single = true,
                '"' => in_double = true,
                '(' => depth += 1,
                ')' => depth -= 1,
                _ if depth == 0 => {
                    let rest = &input[byte_idx..];
                    if consume_keyword(rest, keyword).is_some() {
                        return Some(byte_idx);
                    }
                }
                _ => {}
            }
        }
        idx += 1;
    }

    None
}

fn object_name_to_string(name: &ObjectName) -> Result<String> {
    let mut parts = Vec::with_capacity(name.0.len());
    for part in &name.0 {
        let ident = part.as_ident().ok_or_else(|| {
            anyhow!("materialized view name contains unsupported identifier syntax")
        })?;
        parts.push(ident.value.as_str());
    }
    Ok(parts.join("."))
}

fn normalize_sql(sql: &str) -> Result<&str> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("SQL statement cannot be empty"));
    }
    let trimmed = trimmed.trim_start_matches(|c: char| c.is_ascii_control());
    let trimmed = trimmed.trim_end_matches(|c: char| c.is_whitespace() || c == ';');
    let trimmed = trimmed.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("SQL statement cannot be empty"));
    }
    Ok(trimmed)
}

fn parse_integer_literal(input: &str) -> Result<(&str, i64)> {
    let trimmed = input.trim_start();
    if trimmed.is_empty() {
        return Err(anyhow!("expected integer literal"));
    }
    let bytes = trimmed.as_bytes();
    let mut end = 0;
    if matches!(bytes[0], b'+' | b'-') {
        end += 1;
    }
    while end < bytes.len() && bytes[end].is_ascii_digit() {
        end += 1;
    }
    if end == 0 || (end == 1 && matches!(bytes[0], b'+' | b'-')) {
        return Err(anyhow!("expected integer literal"));
    }
    let literal = &trimmed[..end];
    let value = literal
        .parse::<i64>()
        .map_err(|_| anyhow!("integer literal '{literal}' is out of range"))?;
    Ok((&trimmed[end..], value))
}

fn parse_option_key(input: &str) -> Result<(&str, String)> {
    let trimmed = input.trim_start();
    if trimmed.starts_with('\'') {
        parse_single_quoted_literal(trimmed)
    } else {
        parse_identifier(trimmed)
    }
}

fn parse_option_value(input: &str) -> Result<(&str, String)> {
    let trimmed = input.trim_start();
    if trimmed.is_empty() {
        return Err(anyhow!("expected option value"));
    }
    if trimmed.starts_with('\'') {
        return parse_single_quoted_literal(trimmed);
    }
    if trimmed.starts_with('"') {
        return parse_quoted_identifier(trimmed);
    }

    let mut end = 0;
    for ch in trimmed.chars() {
        if ch == ',' || ch == ')' || ch.is_whitespace() {
            break;
        }
        end += ch.len_utf8();
    }
    if end == 0 {
        return Err(anyhow!("expected option value"));
    }
    Ok((&trimmed[end..], trimmed[..end].to_string()))
}

fn parse_single_quoted_literal(input: &str) -> Result<(&str, String)> {
    let bytes = input.as_bytes();
    if !matches!(bytes.first(), Some(b'\'')) {
        return Err(anyhow!("expected single-quoted literal"));
    }

    let mut value = String::new();
    let mut i = 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    value.push('\'');
                    i += 2;
                } else {
                    let rest = &input[i + 1..];
                    return Ok((rest, value));
                }
            }
            b => {
                value.push(b as char);
                i += 1;
            }
        }
    }

    Err(anyhow!("unterminated single-quoted literal"))
}

fn parse_bool_option(name: &str, value: &str) -> Result<bool> {
    if value.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if value.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        Err(anyhow!("option '{name}' must be true or false"))
    }
}

fn parse_i64_option(name: &str, value: &str) -> Result<i64> {
    value
        .parse::<i64>()
        .map_err(|_| anyhow!("option '{name}' must be a valid Int64"))
}

fn parse_usize_option(name: &str, value: &str) -> Result<usize> {
    value
        .parse::<usize>()
        .map_err(|_| anyhow!("option '{name}' must be a non-negative integer"))
}

fn parse_u64_option(name: &str, value: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .map_err(|_| anyhow!("option '{name}' must be a non-negative integer"))
}

fn normalize_sink_format(value: &str) -> String {
    value.to_ascii_lowercase().replace('-', "_")
}

fn parse_column_list_option(value: &str) -> Result<Vec<String>> {
    let mut columns = Vec::new();
    for raw in value.split(',') {
        let column = raw.trim();
        if column.is_empty() {
            return Err(anyhow!("key column list cannot contain empty columns"));
        }
        columns.push(column.to_string());
    }
    if columns.is_empty() {
        return Err(anyhow!("key column list cannot be empty"));
    }
    Ok(columns)
}

fn required_option<'a>(
    options: &'a std::collections::HashMap<String, String>,
    key: &str,
) -> Result<&'a str> {
    options
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| anyhow!("CREATE SINK requires '{key}' option"))
}

fn required_replication_option<'a>(
    options: &'a std::collections::HashMap<String, String>,
    key: &str,
) -> Result<&'a str> {
    options
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| anyhow!("CREATE REPLICATION PIPELINE requires '{key}' option"))
}

fn split_sql_statements(sql: &str) -> Result<Vec<String>> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let chars: Vec<char> = sql.chars().collect();
    let mut idx = 0;
    let mut in_single = false;
    let mut in_double = false;

    while idx < chars.len() {
        let ch = chars[idx];
        if in_single {
            current.push(ch);
            if ch == '\'' {
                if idx + 1 < chars.len() && chars[idx + 1] == '\'' {
                    current.push(chars[idx + 1]);
                    idx += 1;
                } else {
                    in_single = false;
                }
            }
        } else if in_double {
            current.push(ch);
            if ch == '"' {
                if idx + 1 < chars.len() && chars[idx + 1] == '"' {
                    current.push(chars[idx + 1]);
                    idx += 1;
                } else {
                    in_double = false;
                }
            }
        } else {
            match ch {
                '\'' => {
                    in_single = true;
                    current.push(ch);
                }
                '"' => {
                    in_double = true;
                    current.push(ch);
                }
                ';' => {
                    let statement = current.trim();
                    if !statement.is_empty() {
                        statements.push(statement.to_string());
                    }
                    current.clear();
                }
                _ => current.push(ch),
            }
        }
        idx += 1;
    }

    if in_single || in_double {
        return Err(anyhow!("unterminated quoted string in SQL program"));
    }

    let statement = current.trim();
    if !statement.is_empty() {
        statements.push(statement.to_string());
    }
    Ok(statements)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_kafka_sink_debezium_options() {
        let statement = parse_floe_statement(
            "CREATE SINK out_orders FROM mv_orders WITH (
                type = 'kafka',
                brokers = 'localhost:9092',
                topic = 'orders',
                format = 'debezium-json',
                key.columns = 'tenant_id,id',
                with_snapshot = true
            )",
        )
        .expect("parse sink");

        let FloeStatement::CreateSink(definition) = statement else {
            panic!("expected CREATE SINK statement");
        };
        assert_eq!(definition.name(), "out_orders");
        assert_eq!(definition.mv_name(), "mv_orders");
        assert!(definition.with_snapshot());
        match definition.connector() {
            SinkConnector::Kafka {
                brokers,
                topic,
                format,
                key_columns,
            } => {
                assert_eq!(brokers, "localhost:9092");
                assert_eq!(topic, "orders");
                assert_eq!(format.as_deref(), Some("debezium_json"));
                assert_eq!(
                    key_columns,
                    &vec!["tenant_id".to_string(), "id".to_string()]
                );
            }
            other => panic!("expected Kafka sink, got {other:?}"),
        }
    }
}
