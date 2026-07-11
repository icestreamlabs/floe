use super::*;

pub(super) fn source_labels(sources: &[Source]) -> String {
    sources
        .iter()
        .map(|source| source.label())
        .collect::<Vec<_>>()
        .join(" ")
}

pub(super) fn log(message: impl AsRef<str>) {
    println!("[nexmark-cross-engine] {}", message.as_ref());
}

pub(super) fn token_value(line: &str, prefix: &str) -> Option<String> {
    for token in line.split_whitespace() {
        if let Some(value) = token.strip_prefix(prefix) {
            return Some(
                value
                    .trim_matches(|ch: char| {
                        !(ch.is_ascii_alphanumeric()
                            || ch == '_'
                            || ch == '.'
                            || ch == ':'
                            || ch == '-')
                    })
                    .to_string(),
            );
        }
    }
    None
}

pub(super) fn print_usage() {
    println!(
        "Usage: nexmark_cross_engine_compare [floe|materialize|risingwave|feldera|floe,risingwave|all] [all|nexmark_all|q0..q22]"
    );
}
