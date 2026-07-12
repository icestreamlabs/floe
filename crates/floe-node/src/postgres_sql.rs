use anyhow::{Result, ensure};

pub(crate) fn quote_postgres_qualified_name(name: &str) -> Result<String> {
    let parts = name
        .split('.')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(quote_postgres_ident)
        .collect::<Vec<_>>();
    ensure!(!parts.is_empty(), "Postgres qualified name cannot be empty");
    Ok(parts.join("."))
}

pub(crate) fn quote_postgres_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
