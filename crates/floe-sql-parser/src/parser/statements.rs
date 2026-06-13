use super::*;
use crate::definitions::ReplicationPipelineDefinitionParts;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SourceFormatClause {
    data_format: String,
    data_encode: String,
}

impl SourceFormatClause {
    pub(super) fn new(data_format: impl Into<String>, data_encode: impl Into<String>) -> Self {
        Self {
            data_format: normalize_sink_format(&data_format.into()),
            data_encode: normalize_sink_format(&data_encode.into()),
        }
    }

    pub(super) fn message_format(&self) -> Result<String> {
        if self.data_encode != "json" {
            return Err(anyhow!(
                "unsupported source ENCODE '{}'; Floe currently supports ENCODE JSON",
                self.data_encode
            ));
        }
        match self.data_format.as_str() {
            "plain" => Ok("floe_json".to_string()),
            "debezium" => Ok("debezium_json".to_string()),
            other => Err(anyhow!(
                "unsupported source FORMAT '{other}'; Floe currently supports FORMAT PLAIN ENCODE JSON and FORMAT DEBEZIUM ENCODE JSON"
            )),
        }
    }

    pub(super) fn is_plain_json(&self) -> bool {
        self.data_format == "plain" && self.data_encode == "json"
    }
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

pub(super) fn parse_optional_source_schema<'a>(
    input: &'a str,
    source_name: &str,
) -> Result<(&'a str, Vec<CreateTableColumnDefinition>)> {
    let trimmed = input.trim_start();
    if !trimmed.starts_with('(') {
        return Ok((input, Vec::new()));
    }
    let (rest, schema_clause) = parse_balanced_parenthesized_clause(trimmed)?;
    reject_unsupported_source_schema_tokens(&schema_clause)?;
    let table_sql = format!("CREATE TABLE __floe_source {schema_clause}");
    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, &table_sql)
        .map_err(|err| anyhow!("failed to parse CREATE SOURCE schema: {err}"))?;
    if statements.len() != 1 {
        return Err(anyhow!("CREATE SOURCE schema must contain one column list"));
    }
    let statement = statements.remove(0);
    let Statement::CreateTable(create_table) = statement else {
        return Err(anyhow!("expected CREATE SOURCE schema column list"));
    };
    if create_table.columns.is_empty() {
        return Err(anyhow!(
            "CREATE SOURCE {source_name} must declare at least one column when schema is provided"
        ));
    }

    let pk_names = source_primary_key_columns(&create_table.constraints)?;
    let mut columns = Vec::with_capacity(create_table.columns.len());
    for column in &create_table.columns {
        let name = column.name.value.clone();
        let mut primary_key = pk_names.iter().any(|pk_name| pk_name == &name);
        for option in &column.options {
            match &option.option {
                ColumnOption::NotNull => {
                    return Err(anyhow!(
                        "CREATE SOURCE schemas do not support NOT NULL constraints; use CREATE TABLE for enforced nullability"
                    ));
                }
                ColumnOption::Unique { is_primary, .. } if *is_primary => {
                    primary_key = true;
                }
                ColumnOption::Null => {}
                ColumnOption::Generated { .. }
                | ColumnOption::Default(_)
                | ColumnOption::Materialized(_)
                | ColumnOption::Ephemeral(_)
                | ColumnOption::Alias(_) => {
                    return Err(anyhow!(
                        "CREATE SOURCE generated/default columns are not supported by Floe"
                    ));
                }
                ColumnOption::Check(_) => {
                    return Err(anyhow!(
                        "CREATE SOURCE CHECK constraints are not supported by Floe"
                    ));
                }
                ColumnOption::ForeignKey { .. } => {
                    return Err(anyhow!(
                        "CREATE SOURCE foreign keys are not supported by Floe"
                    ));
                }
                other => {
                    return Err(anyhow!("unsupported CREATE SOURCE column option '{other}'"));
                }
            }
        }
        let data_type = parse_table_column_type(&column.data_type, source_name, &name)?;
        columns.push(CreateTableColumnDefinition::new(
            name,
            data_type,
            !primary_key,
            primary_key,
        ));
    }
    for pk_name in &pk_names {
        if columns.iter().all(|column| column.name() != pk_name) {
            return Err(anyhow!(
                "primary key columns not declared in source {}: {}",
                source_name,
                pk_name
            ));
        }
    }
    Ok((rest, columns))
}

fn reject_unsupported_source_schema_tokens(schema_clause: &str) -> Result<()> {
    if contains_keyword_token(schema_clause, "WATERMARK") {
        return Err(anyhow!(
            "CREATE SOURCE WATERMARK clauses are not supported by Floe"
        ));
    }
    if contains_keyword_token(schema_clause, "INCLUDE") {
        return Err(anyhow!(
            "CREATE SOURCE INCLUDE clauses are not supported by Floe"
        ));
    }
    if contains_top_level_star(schema_clause) {
        return Err(anyhow!(
            "CREATE SOURCE '*' schemas require external schema discovery, which Floe does not support"
        ));
    }
    if contains_keyword_token(schema_clause, "AS") {
        return Err(anyhow!(
            "CREATE SOURCE generated/default columns are not supported by Floe"
        ));
    }
    Ok(())
}

fn contains_keyword_token(input: &str, keyword: &str) -> bool {
    let chars: Vec<(usize, char)> = input.char_indices().collect();
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
                _ if is_keyword_boundary(input[..byte_idx].chars().next_back())
                    && consume_keyword(&input[byte_idx..], keyword).is_some() =>
                {
                    return true;
                }
                _ => {}
            }
        }
        idx += 1;
    }
    false
}

fn contains_top_level_star(input: &str) -> bool {
    let chars: Vec<char> = input.chars().collect();
    let mut depth = 0_i32;
    let mut in_single = false;
    let mut in_double = false;
    let mut idx = 0;
    while idx < chars.len() {
        let ch = chars[idx];
        if in_single {
            if ch == '\'' {
                if idx + 1 < chars.len() && chars[idx + 1] == '\'' {
                    idx += 1;
                } else {
                    in_single = false;
                }
            }
        } else if in_double {
            if ch == '"' {
                if idx + 1 < chars.len() && chars[idx + 1] == '"' {
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
                '*' if depth == 1 => return true,
                _ => {}
            }
        }
        idx += 1;
    }
    false
}

pub(super) fn parse_optional_source_format(
    input: &str,
) -> Result<(&str, Option<SourceFormatClause>)> {
    let Some(rest) = consume_keyword(input, "FORMAT") else {
        return Ok((input, None));
    };
    let (next, data_format) = parse_identifier(rest)?;
    let rest = consume_keyword(next, "ENCODE")
        .ok_or_else(|| anyhow!("expected ENCODE after source FORMAT"))?;
    let (next, data_encode) = parse_identifier(rest)?;
    let rest = next;
    if rest.trim_start().starts_with('(') {
        let (_next, _options) = parse_parenthesized_options(rest)?;
        return Err(anyhow!(
            "source ENCODE option lists are not yet supported in Floe"
        ));
    }
    Ok((
        rest,
        Some(SourceFormatClause::new(data_format, data_encode)),
    ))
}

pub(super) fn merge_source_format_option(
    with_format: Option<String>,
    clause: Option<&SourceFormatClause>,
) -> Result<Option<String>> {
    let clause_format = clause.map(SourceFormatClause::message_format).transpose()?;
    match (with_format, clause_format) {
        (Some(left), Some(right)) if left != right => Err(anyhow!(
            "source format option '{left}' conflicts with FORMAT/ENCODE clause '{right}'"
        )),
        (Some(value), _) | (_, Some(value)) => Ok(Some(value)),
        (None, None) => Ok(None),
    }
}

fn source_primary_key_columns(constraints: &[TableConstraint]) -> Result<Vec<String>> {
    let mut primary_key_columns = Vec::new();
    for constraint in constraints {
        if let TableConstraint::PrimaryKey { columns, .. } = constraint {
            for column in columns {
                primary_key_columns.push(primary_key_column_name(&column.column)?);
            }
        }
    }
    Ok(primary_key_columns)
}

fn parse_balanced_parenthesized_clause(input: &str) -> Result<(&str, String)> {
    let trimmed = input.trim_start();
    if !trimmed.starts_with('(') {
        return Err(anyhow!("expected '(' to start source schema"));
    }
    let chars: Vec<(usize, char)> = trimmed.char_indices().collect();
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
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        let end = byte_idx + ch.len_utf8();
                        return Ok((&trimmed[end..], trimmed[..end].to_string()));
                    }
                    if depth < 0 {
                        return Err(anyhow!("unbalanced ')' in source schema"));
                    }
                }
                _ => {}
            }
        }
        idx += 1;
    }
    Err(anyhow!("unterminated source schema"))
}

pub(super) fn split_create_table_source_clause(
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

pub(super) fn parse_table_reference_literal(input: &str) -> Result<(&str, String)> {
    let trimmed = input.trim_start();
    if trimmed.starts_with('\'') {
        parse_single_quoted_literal(trimmed)
    } else {
        parse_identifier(trimmed)
    }
}

pub(super) fn primary_key_columns(constraints: &[TableConstraint]) -> Result<Vec<String>> {
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

pub(super) fn primary_key_column_name(column: &OrderByExpr) -> Result<String> {
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

pub(super) fn parse_table_column_type(
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
        DataType::Numeric(info)
        | DataType::Decimal(info)
        | DataType::DecimalUnsigned(info)
        | DataType::BigNumeric(info)
        | DataType::BigDecimal(info)
        | DataType::Dec(info) => parse_exact_numeric_type(info, table_name, column_name)?,
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

pub(super) fn parse_exact_numeric_type(
    info: &ExactNumberInfo,
    table_name: &str,
    column_name: &str,
) -> Result<SqlColumnType> {
    match info {
        ExactNumberInfo::None => Ok(SqlColumnType::Numeric),
        ExactNumberInfo::Precision(precision) => {
            decimal128_type_from_exact_number(*precision, 0, table_name, column_name)
        }
        ExactNumberInfo::PrecisionAndScale(precision, scale) => {
            if *scale < 0 {
                return Err(anyhow!(
                    "unsupported negative NUMERIC scale {scale} for column '{}' in table '{}'",
                    column_name,
                    table_name
                ));
            }
            decimal128_type_from_exact_number(*precision, *scale as u64, table_name, column_name)
        }
    }
}

pub(super) fn decimal128_type_from_exact_number(
    precision: u64,
    scale: u64,
    table_name: &str,
    column_name: &str,
) -> Result<SqlColumnType> {
    let precision = u8::try_from(precision).map_err(|_| {
        anyhow!(
            "unsupported NUMERIC precision {precision} for column '{}' in table '{}'; Decimal128 supports precision 1..=38",
            column_name,
            table_name
        )
    })?;
    let scale = i8::try_from(scale).map_err(|_| {
        anyhow!(
            "unsupported NUMERIC scale {scale} for column '{}' in table '{}'; Decimal128 supports scale 0..=38",
            column_name,
            table_name
        )
    })?;
    SqlColumnType::decimal128(precision, scale).map_err(|err| {
        anyhow!(
            "unsupported NUMERIC({precision},{scale}) for column '{}' in table '{}': {err}",
            column_name,
            table_name
        )
    })
}

pub(super) fn parse_sink_statement(sql: &str) -> Result<SinkDefinition> {
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

    let connector_from_connector_key = options.contains_key("connector");
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
    let checkpoint_partition =
        option_any(&options, &["checkpoint_partition", "checkpoint.partition"])
            .map(|value| parse_i32_option("checkpoint_partition", value))
            .transpose()?;
    if checkpoint_partition.is_some_and(|partition| partition < 0) {
        return Err(anyhow!("option 'checkpoint_partition' must be >= 0"));
    }
    let sink_options = SinkOptions::new(
        option_any(&options, &["batch_rows", "batch.rows"])
            .map(|value| parse_positive_usize_option("batch_rows", value))
            .transpose()?,
        option_any(&options, &["batch_bytes", "batch.bytes"])
            .map(|value| parse_positive_usize_option("batch_bytes", value))
            .transpose()?,
        option_any(&options, &["queue_capacity", "queue.capacity"])
            .map(|value| parse_positive_usize_option("queue_capacity", value))
            .transpose()?,
        option_any(&options, &["retry_max_attempts", "retry.max_attempts"])
            .map(|value| parse_positive_usize_option("retry_max_attempts", value))
            .transpose()?,
        option_any(&options, &["retry_base_ms", "retry.base_ms"])
            .map(|value| parse_positive_u64_option("retry_base_ms", value))
            .transpose()?,
        option_any(&options, &["retry_max_backoff_ms", "retry.max_backoff_ms"])
            .map(|value| parse_positive_u64_option("retry_max_backoff_ms", value))
            .transpose()?,
        option_any(&options, &["transactional_id", "transactional.id"]).map(ToString::to_string),
        option_any(&options, &["checkpoint_topic", "checkpoint.topic"]).map(ToString::to_string),
        checkpoint_partition,
    );

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
                .map(|value| parse_positive_usize_option("batch_size", value))
                .transpose()?;
            SinkConnector::Http { url, batch_size }
        }
        "postgres" | "postgresql" => {
            let connection =
                option_any(&options, &["connection", "connection_string", "dsn", "url"])
                    .ok_or_else(|| {
                        anyhow!("Postgres sink requires connection/connection_string/url")
                    })?
                    .to_string();
            let table = option_any(&options, &["table", "target.table", "target_table"])
                .ok_or_else(|| anyhow!("Postgres sink requires table/target_table"))?
                .to_string();
            let mode = option_any(&options, &["mode", "sink_type", "sink.type"])
                .or_else(|| {
                    connector_from_connector_key
                        .then(|| options.get("type").map(String::as_str))
                        .flatten()
                })
                .map(normalize_sink_format);
            let primary_key = option_any(
                &options,
                &["primary_key", "primary.key", "key_columns", "key.columns"],
            )
            .map(parse_column_list_option)
            .transpose()?
            .unwrap_or_default();
            SinkConnector::Postgres {
                connection,
                table,
                mode,
                primary_key,
            }
        }
        other => return Err(anyhow!("unsupported sink connector type '{other}'")),
    };

    Ok(SinkDefinition::new_with_options(
        sink_name,
        mv_name,
        connector,
        with_snapshot,
        as_of,
        sink_options,
    ))
}

pub(super) fn parse_replication_pipeline_statement(
    sql: &str,
) -> Result<ReplicationPipelineDefinition> {
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
    let error_policy = ReplicationErrorPolicy::new(
        option_any(&options, &["error.policy", "error_policy"])
            .map(parse_replication_error_policy_mode)
            .transpose()?
            .unwrap_or_default(),
        option_any(&options, &["error.max_retries", "error_max_retries"])
            .map(|value| parse_u32_option("error.max_retries", value))
            .transpose()?,
    );

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

    ReplicationPipelineDefinition::new(ReplicationPipelineDefinitionParts {
        name: pipeline_name.to_string(),
        source_name: source_name.to_string(),
        upstream_table: upstream_table.to_string(),
        target,
        format,
        buffer_mode,
        buffer_policy,
        emit_tombstones,
        include_transaction_metadata,
        error_policy,
    })
}

pub(super) fn postgres_connection_string_from_options(
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

pub(super) fn postgres_schema_evolution_policy_from_options(
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

pub(super) fn parse_subscribe_statement(sql: &str) -> Result<FloeStatement> {
    let (mv_name, with_snapshot, as_of) = parse_stream_statement_parts(sql, "SUBSCRIBE")?;
    Ok(FloeStatement::Subscribe {
        mv_name,
        with_snapshot,
        as_of,
    })
}

pub(super) fn parse_stream_statement_parts(
    sql: &str,
    keyword: &str,
) -> Result<(String, bool, Option<i64>)> {
    let mut rest = consume_keyword(sql, keyword)
        .ok_or_else(|| anyhow!("expected {keyword} at start of statement"))?;
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
                "unexpected tokens after {keyword} statement: {}",
                rest.trim()
            ));
        };
        let after_of = consume_keyword(after_as, "OF")
            .ok_or_else(|| anyhow!("expected OF after AS in {keyword} statement"))?;
        let (next, version) = parse_integer_literal(after_of)?;
        as_of = Some(version);
        rest = next;
    }

    if !rest.trim().is_empty() {
        return Err(anyhow!(
            "unexpected tokens after {keyword} statement: {}",
            rest.trim()
        ));
    }

    Ok((mv_name, with_snapshot, as_of))
}
