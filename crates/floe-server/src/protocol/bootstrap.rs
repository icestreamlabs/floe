pub(super) fn rewrite_bootstrap_sql(sql: &str) -> Option<String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return None;
    }
    let normalized = trimmed
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();

    match normalized.as_str() {
        "select version()" => Some("SELECT 'PostgreSQL 16.0 (Floe)' AS version".to_string()),
        "select current_schema()" => Some("SELECT 'public' AS current_schema".to_string()),
        "show transaction_isolation" => {
            Some("SELECT 'read committed' AS transaction_isolation".to_string())
        }
        "show standard_conforming_strings" => {
            Some("SELECT 'on' AS standard_conforming_strings".to_string())
        }
        "show server_version" => Some("SELECT '16.0' AS server_version".to_string()),
        "show server_encoding" => Some("SELECT 'UTF8' AS server_encoding".to_string()),
        "show client_encoding" => Some("SELECT 'UTF8' AS client_encoding".to_string()),
        "show application_name" => Some("SELECT '' AS application_name".to_string()),
        "show search_path" => Some("SELECT 'public' AS search_path".to_string()),
        _ => rewrite_select_current_setting(&normalized),
    }
}

pub(super) fn detect_noop_session_command(sql: &str) -> Option<&'static str> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() || trimmed.contains(';') {
        return None;
    }
    let normalized = trimmed
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    if normalized.starts_with("set ") {
        return Some("SET");
    }
    if normalized.starts_with("reset ") {
        return Some("RESET");
    }
    if normalized == "begin"
        || normalized == "begin transaction"
        || normalized == "start transaction"
        || normalized == "start transaction read write"
        || normalized == "start transaction read only"
    {
        return Some("BEGIN");
    }
    if normalized.starts_with("commit") {
        return Some("COMMIT");
    }
    if normalized.starts_with("rollback") {
        return Some("ROLLBACK");
    }
    None
}

fn rewrite_select_current_setting(normalized_sql: &str) -> Option<String> {
    if normalized_sql == "select current_setting('server_version_num')" {
        return Some("SELECT '160000' AS current_setting".to_string());
    }
    if normalized_sql == "select current_setting('standard_conforming_strings')" {
        return Some("SELECT 'on' AS current_setting".to_string());
    }
    if normalized_sql.starts_with("select pg_catalog.set_config(") {
        return Some("SELECT '' AS set_config".to_string());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_common_bootstrap_queries() {
        assert!(rewrite_bootstrap_sql("SELECT version();").is_some());
        assert!(rewrite_bootstrap_sql("show transaction_isolation").is_some());
        assert!(rewrite_bootstrap_sql("SHOW standard_conforming_strings;").is_some());
        assert!(rewrite_bootstrap_sql("SELECT current_setting('server_version_num')").is_some());
        assert!(rewrite_bootstrap_sql("SHOW search_path").is_some());
        assert!(
            rewrite_bootstrap_sql("SELECT pg_catalog.set_config('search_path', '', false)")
                .is_some()
        );
    }

    #[test]
    fn leaves_unknown_queries_untouched() {
        assert!(rewrite_bootstrap_sql("SELECT * FROM mv_test").is_none());
        assert!(rewrite_bootstrap_sql("SHOW materialized_views").is_none());
    }

    #[test]
    fn detects_noop_session_commands() {
        assert_eq!(
            detect_noop_session_command("SET search_path TO public"),
            Some("SET")
        );
        assert_eq!(
            detect_noop_session_command("RESET search_path"),
            Some("RESET")
        );
        assert_eq!(detect_noop_session_command("BEGIN"), Some("BEGIN"));
        assert_eq!(detect_noop_session_command("COMMIT"), Some("COMMIT"));
        assert_eq!(detect_noop_session_command("ROLLBACK"), Some("ROLLBACK"));
        assert_eq!(
            detect_noop_session_command("SELECT 1; SET search_path TO public"),
            None
        );
    }
}
