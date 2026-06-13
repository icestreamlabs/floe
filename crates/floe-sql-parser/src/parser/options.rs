use super::*;

pub(super) fn parse_parenthesized_options(
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

pub(super) fn consume_keyword<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
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

pub(super) fn consume_sequence<'a>(mut input: &'a str, keywords: &[&str]) -> Option<&'a str> {
    for keyword in keywords {
        input = consume_keyword(input, keyword)?;
    }
    Some(input)
}

pub(super) fn is_keyword_boundary(ch: Option<char>) -> bool {
    match ch {
        None => true,
        Some(c) => c.is_whitespace() || matches!(c, '(' | ')' | ';' | ','),
    }
}

pub(super) fn parse_identifier(input: &str) -> Result<(&str, String)> {
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

pub(super) fn parse_quoted_identifier(input: &str) -> Result<(&str, String)> {
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

pub(super) fn parse_unquoted_identifier(input: &str) -> Result<(&str, String)> {
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

pub(super) fn starts_with_keyword(input: &str, keyword: &str) -> bool {
    consume_keyword(input, keyword).is_some()
}

pub(super) fn option_any<'a>(
    options: &'a std::collections::HashMap<String, String>,
    keys: &[&str],
) -> Option<&'a str> {
    keys.iter()
        .find_map(|key| options.get(*key).map(String::as_str))
}

pub(super) fn find_top_level_keyword(input: &str, keyword: &str) -> Option<usize> {
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

pub(super) fn object_name_to_string(name: &ObjectName) -> Result<String> {
    let mut parts = Vec::with_capacity(name.0.len());
    for part in &name.0 {
        let ident = part.as_ident().ok_or_else(|| {
            anyhow!("materialized view name contains unsupported identifier syntax")
        })?;
        parts.push(ident.value.as_str());
    }
    Ok(parts.join("."))
}

pub(super) fn normalize_sql(sql: &str) -> Result<&str> {
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

pub(super) fn parse_integer_literal(input: &str) -> Result<(&str, i64)> {
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

pub(super) fn parse_option_key(input: &str) -> Result<(&str, String)> {
    let trimmed = input.trim_start();
    if trimmed.starts_with('\'') {
        parse_single_quoted_literal(trimmed)
    } else {
        parse_identifier(trimmed)
    }
}

pub(super) fn parse_option_value(input: &str) -> Result<(&str, String)> {
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

pub(super) fn parse_single_quoted_literal(input: &str) -> Result<(&str, String)> {
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

pub(super) fn parse_bool_option(name: &str, value: &str) -> Result<bool> {
    if value.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if value.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        Err(anyhow!("option '{name}' must be true or false"))
    }
}

pub(super) fn parse_i64_option(name: &str, value: &str) -> Result<i64> {
    value
        .parse::<i64>()
        .map_err(|_| anyhow!("option '{name}' must be a valid Int64"))
}

pub(super) fn parse_usize_option(name: &str, value: &str) -> Result<usize> {
    value
        .parse::<usize>()
        .map_err(|_| anyhow!("option '{name}' must be a non-negative integer"))
}

pub(super) fn parse_positive_usize_option(name: &str, value: &str) -> Result<usize> {
    let value = parse_usize_option(name, value)?;
    if value == 0 {
        return Err(anyhow!("option '{name}' must be greater than zero"));
    }
    Ok(value)
}

pub(super) fn parse_u64_option(name: &str, value: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .map_err(|_| anyhow!("option '{name}' must be a non-negative integer"))
}

pub(super) fn parse_positive_u64_option(name: &str, value: &str) -> Result<u64> {
    let value = parse_u64_option(name, value)?;
    if value == 0 {
        return Err(anyhow!("option '{name}' must be greater than zero"));
    }
    Ok(value)
}

pub(super) fn parse_u32_option(name: &str, value: &str) -> Result<u32> {
    value
        .parse::<u32>()
        .map_err(|_| anyhow!("option '{name}' must be a non-negative integer"))
}

pub(super) fn parse_u16_option(name: &str, value: &str) -> Result<u16> {
    value
        .parse::<u16>()
        .map_err(|_| anyhow!("option '{name}' must be a valid port number"))
}

pub(super) fn parse_i32_option(name: &str, value: &str) -> Result<i32> {
    value
        .parse::<i32>()
        .map_err(|_| anyhow!("option '{name}' must be a valid Int32"))
}

pub(super) fn parse_f64_option(name: &str, value: &str) -> Result<f64> {
    value
        .parse::<f64>()
        .map_err(|_| anyhow!("option '{name}' must be a valid number"))
}

pub(super) fn parse_replication_error_policy_mode(
    value: &str,
) -> Result<ReplicationErrorPolicyMode> {
    match value.to_ascii_lowercase().replace('-', "_").as_str() {
        "fail_fast" | "failfast" => Ok(ReplicationErrorPolicyMode::FailFast),
        "retry" | "retry_with_backoff" => Ok(ReplicationErrorPolicyMode::RetryWithBackoff),
        "dlq" | "dead_letter" | "dead_letter_and_continue" => {
            Ok(ReplicationErrorPolicyMode::DeadLetterAndContinue)
        }
        other => Err(anyhow!(
            "unsupported replication pipeline error policy '{other}'"
        )),
    }
}

pub(super) fn normalize_sink_format(value: &str) -> String {
    value.to_ascii_lowercase().replace('-', "_")
}

pub(super) fn parse_column_list_option(value: &str) -> Result<Vec<String>> {
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

pub(super) fn parse_string_list_option(name: &str, value: &str) -> Result<Vec<String>> {
    let mut values = Vec::new();
    for raw in value.split(',') {
        let item = raw.trim();
        if item.is_empty() {
            return Err(anyhow!("option '{name}' cannot contain empty values"));
        }
        values.push(item.to_string());
    }
    if values.is_empty() {
        return Err(anyhow!("option '{name}' cannot be empty"));
    }
    Ok(values)
}

pub(super) fn required_option<'a>(
    options: &'a std::collections::HashMap<String, String>,
    key: &str,
) -> Result<&'a str> {
    options
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| anyhow!("CREATE SINK requires '{key}' option"))
}

pub(super) fn required_source_option<'a>(
    options: &'a std::collections::HashMap<String, String>,
    key: &str,
) -> Result<&'a str> {
    options
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| anyhow!("CREATE SOURCE requires '{key}' option"))
}

pub(super) fn required_replication_option<'a>(
    options: &'a std::collections::HashMap<String, String>,
    key: &str,
) -> Result<&'a str> {
    options
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| anyhow!("CREATE REPLICATION PIPELINE requires '{key}' option"))
}

pub(super) fn split_sql_statements(sql: &str) -> Result<Vec<String>> {
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
