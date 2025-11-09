use anyhow::{Result, bail};

const SEPARATOR: char = '/';

/// Namespace helpers for DBSP-backed resources.
///
/// All namespaces follow `<scope>/<identifier>` semantics so they can be
/// discovered and reasoned about uniformly inside SlateDB.
pub fn materialized_view(view_name: &str) -> Result<String> {
    let trimmed = view_name.trim();
    validate_component(trimmed, "materialized view name")?;
    Ok(format!("mv/{trimmed}"))
}

pub fn source(source_name: &str) -> Result<String> {
    let trimmed = source_name.trim();
    validate_component(trimmed, "source name")?;
    Ok(format!("src/{trimmed}"))
}

pub fn operator_state(graph_id: &str, operator_index: usize, side: &str) -> Result<String> {
    let graph = graph_id.trim();
    let side_trimmed = side.trim();
    validate_component(graph, "graph id")?;
    validate_component(side_trimmed, "operator side")?;
    Ok(format!(
        "op/{graph}/{op}/{side}",
        graph = graph,
        op = operator_index,
        side = side_trimmed
    ))
}

fn validate_component(component: &str, label: &str) -> Result<()> {
    if component.is_empty() {
        bail!("{label} cannot be empty or whitespace-only");
    }
    if component.contains(SEPARATOR) {
        bail!("{label} cannot contain '{SEPARATOR}'");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_materialized_view_namespace() {
        assert_eq!(materialized_view("mv_q1").unwrap(), "mv/mv_q1");
    }

    #[test]
    fn builds_source_namespace() {
        assert_eq!(source("bids").unwrap(), "src/bids");
    }

    #[test]
    fn builds_operator_namespace() {
        assert_eq!(
            operator_state("planA", 2, "left").unwrap(),
            "op/planA/2/left"
        );
    }

    #[test]
    fn rejects_slashes() {
        assert!(operator_state("plan/A", 0, "left").is_err());
        assert!(materialized_view("mv/q3").is_err());
    }

    #[test]
    fn rejects_empty() {
        assert!(source("   ").is_err());
    }
}
