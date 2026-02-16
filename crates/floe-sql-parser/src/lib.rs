use anyhow::{Result, anyhow};
use sqlparser::ast::{ObjectName, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FloeStatement {
    CreateMaterializedView(MaterializedViewDefinition),
    CreateSink(SinkDefinition),
    Tail {
        mv_name: String,
        with_snapshot: bool,
        as_of: Option<i64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkDefinition {
    name: String,
    mv_name: String,
    connector: SinkConnector,
    with_snapshot: bool,
    as_of: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkConnector {
    Kafka {
        brokers: String,
        topic: String,
    },
    File {
        path: String,
        append: Option<bool>,
    },
    Http {
        url: String,
        batch_size: Option<usize>,
    },
}

impl SinkDefinition {
    pub fn new(
        name: impl Into<String>,
        mv_name: impl Into<String>,
        connector: SinkConnector,
        with_snapshot: bool,
        as_of: Option<i64>,
    ) -> Self {
        Self {
            name: name.into(),
            mv_name: mv_name.into(),
            connector,
            with_snapshot,
            as_of,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn mv_name(&self) -> &str {
        &self.mv_name
    }

    pub fn connector(&self) -> &SinkConnector {
        &self.connector
    }

    pub fn with_snapshot(&self) -> bool {
        self.with_snapshot
    }

    pub fn as_of(&self) -> Option<i64> {
        self.as_of
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedViewDefinition {
    name: String,
    query: String,
    if_not_exists: bool,
}

impl MaterializedViewDefinition {
    pub fn new(name: impl Into<String>, query: impl Into<String>, if_not_exists: bool) -> Self {
        Self {
            name: name.into(),
            query: query.into(),
            if_not_exists,
        }
    }

    #[allow(dead_code)]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[allow(dead_code)]
    pub fn query(&self) -> &str {
        &self.query
    }

    #[allow(dead_code)]
    pub fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }
}

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
        if starts_with_keyword(normalized, "CREATE SINK") {
            let definition = parse_sink_statement(normalized)?;
            statements.push(FloeStatement::CreateSink(definition));
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

            Ok(MaterializedViewDefinition {
                name,
                query,
                if_not_exists,
            })
        }
        _ => Err(anyhow!("expected CREATE MATERIALIZED VIEW statement")),
    }
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
            SinkConnector::Kafka { brokers, topic }
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

fn required_option<'a>(
    options: &'a std::collections::HashMap<String, String>,
    key: &str,
) -> Result<&'a str> {
    options
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| anyhow!("CREATE SINK requires '{key}' option"))
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
    fn parse_basic() {
        let sql = "CREATE MATERIALIZED VIEW mv AS SELECT * FROM nexmark_person";
        let mv = parse_materialized_view(sql).expect("parse mv");
        assert_eq!(mv.name, "mv");
        assert_eq!(mv.query, "SELECT * FROM nexmark_person");
        assert!(!mv.if_not_exists);
    }

    #[test]
    fn parse_if_not_exists() {
        let sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS SELECT 1";
        let mv = parse_materialized_view(sql).expect("parse mv");
        assert!(mv.if_not_exists);
        assert_eq!(mv.query, "SELECT 1");
    }

    #[test]
    fn parse_with_clause() {
        let sql = "CREATE MATERIALIZED VIEW mv WITH (foo = 'bar') AS SELECT 1";
        let mv = parse_materialized_view(sql).expect("parse mv");
        assert_eq!(mv.name, "mv");
        assert_eq!(mv.query, "SELECT 1");
    }

    #[test]
    fn reject_missing_as() {
        let sql = "CREATE MATERIALIZED VIEW mv SELECT 1";
        let err = parse_materialized_view(sql).unwrap_err();
        assert!(
            err.to_string()
                .contains("failed to parse materialized view statement")
        );
    }

    #[test]
    fn reject_empty_query() {
        let sql = "CREATE MATERIALIZED VIEW mv AS";
        let err = parse_materialized_view(sql).unwrap_err();
        assert!(
            err.to_string()
                .contains("failed to parse materialized view statement")
        );
    }

    #[test]
    fn reject_multiple_statements() {
        let sql =
            "CREATE MATERIALIZED VIEW mv AS SELECT 1; CREATE MATERIALIZED VIEW mv2 AS SELECT 2";
        let err = parse_materialized_view(sql).unwrap_err();
        assert!(err.to_string().contains("multiple statements"));
    }

    #[test]
    fn parse_quoted_identifier() {
        let sql = "CREATE MATERIALIZED VIEW \"MyView\" AS SELECT 1";
        let mv = parse_materialized_view(sql).expect("parse mv");
        assert_eq!(mv.name, "MyView");
    }

    #[test]
    fn parse_create_sink_statement() {
        let stmt = parse_floe_statement(
            "CREATE SINK out_bid FROM mv_bid WITH (type = 'http', url = 'http://localhost:8080', batch_size = 32, with_snapshot = true, as_of = 42)",
        )
        .expect("parse sink");
        match stmt {
            FloeStatement::CreateSink(definition) => {
                assert_eq!(definition.name(), "out_bid");
                assert_eq!(definition.mv_name(), "mv_bid");
                assert!(definition.with_snapshot());
                assert_eq!(definition.as_of(), Some(42));
                assert_eq!(
                    definition.connector(),
                    &SinkConnector::Http {
                        url: "http://localhost:8080".to_string(),
                        batch_size: Some(32),
                    }
                );
            }
            other => panic!("expected CREATE SINK statement, got {other:?}"),
        }
    }

    #[test]
    fn parse_floe_program_preserves_statement_order() {
        let program = r#"
            CREATE MATERIALIZED VIEW mv_bid AS SELECT auction FROM bid;
            CREATE SINK sink_bid FROM mv_bid WITH (connector = 'file', path = '/tmp/out.jsonl', append = true);
            TAIL mv_bid WITH SNAPSHOT;
        "#;
        let statements = parse_floe_program(program).expect("parse program");
        assert_eq!(statements.len(), 3);
        assert!(matches!(
            statements.first(),
            Some(FloeStatement::CreateMaterializedView(_))
        ));
        assert!(matches!(
            statements.get(1),
            Some(FloeStatement::CreateSink(_))
        ));
        assert!(matches!(
            statements.last(),
            Some(FloeStatement::Tail { .. })
        ));
    }

    #[test]
    fn parse_floe_statement_rejects_multi_statement_input() {
        let err = parse_floe_statement("TAIL mv; TAIL mv2").unwrap_err();
        assert!(err.to_string().contains("exactly one statement"));
    }

    #[test]
    fn parse_tail_variants() {
        let stmt = parse_floe_statement("TAIL mv_orders").expect("parse tail");
        assert_eq!(
            stmt,
            FloeStatement::Tail {
                mv_name: "mv_orders".to_string(),
                with_snapshot: false,
                as_of: None,
            }
        );

        let stmt =
            parse_floe_statement("TAIL mv_orders WITH SNAPSHOT").expect("parse tail snapshot");
        assert_eq!(
            stmt,
            FloeStatement::Tail {
                mv_name: "mv_orders".to_string(),
                with_snapshot: true,
                as_of: None,
            }
        );

        let stmt = parse_floe_statement("TAIL mv_orders AS OF 42").expect("parse tail as of");
        assert_eq!(
            stmt,
            FloeStatement::Tail {
                mv_name: "mv_orders".to_string(),
                with_snapshot: false,
                as_of: Some(42),
            }
        );

        let stmt = parse_floe_statement("TAIL mv_orders WITH SNAPSHOT AS OF 42")
            .expect("parse tail snapshot as of");
        assert_eq!(
            stmt,
            FloeStatement::Tail {
                mv_name: "mv_orders".to_string(),
                with_snapshot: true,
                as_of: Some(42),
            }
        );
    }
}
