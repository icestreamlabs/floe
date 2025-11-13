use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use tokio::sync::Mutex;

use crate::dbsp_bridge::DbspBridge;
use crate::load_or_register_mv;
use crate::{FloeQueryContext, MaterializedViewRegistry};

pub struct QueryResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

pub struct PgwireServer {
    ctx: FloeQueryContext,
    registry: Arc<MaterializedViewRegistry>,
    bridge: Mutex<DbspBridge>,
}

impl PgwireServer {
    pub fn new(
        ctx: FloeQueryContext,
        registry: Arc<MaterializedViewRegistry>,
        bridge: DbspBridge,
    ) -> Self {
        Self {
            ctx,
            registry,
            bridge: Mutex::new(bridge),
        }
    }

    pub async fn handle_query(&self, sql: &str) -> Result<QueryResult> {
        let mv_names = find_mv_names(sql);
        if !mv_names.is_empty() {
            let session = self.ctx.session();
            let mut bridge = self.bridge.lock().await;
            for mv in mv_names {
                load_or_register_mv(&session, Arc::clone(&self.registry), &mut bridge, &mv)
                    .await
                    .with_context(|| format!("ensure materialized view '{mv}' is registered"))?;
            }
        }

        let df = self
            .ctx
            .session()
            .sql(sql)
            .await
            .context("plan SQL via DataFusion")?;
        let batches = df.collect().await.context("execute query plan")?;
        Ok(to_query_result(batches))
    }
}

fn to_query_result(batches: Vec<RecordBatch>) -> QueryResult {
    let schema = batches
        .get(0)
        .map(|batch| batch.schema())
        .unwrap_or_else(|| Arc::new(Schema::new(Vec::<Field>::new())));
    QueryResult { schema, batches }
}

fn find_mv_names(sql: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut seen = HashSet::new();
    for raw in sql.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '"')) {
        if raw.is_empty() {
            continue;
        }
        if let Some(name) = normalize_identifier(raw) {
            if seen.insert(name.clone()) {
                names.push(name);
            }
        }
    }
    names
}

fn normalize_identifier(raw: &str) -> Option<String> {
    let quoted = raw.starts_with('"') && raw.ends_with('"') && raw.len() >= 2;
    let inner = if quoted { &raw[1..raw.len() - 1] } else { raw };
    if inner.is_empty() {
        return None;
    }
    let normalized = if quoted {
        inner.to_string()
    } else {
        inner.to_ascii_lowercase()
    };
    if normalized.starts_with("mv_") {
        Some(normalized)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use datafusion::arrow::array::{ArrayRef, Int64Array};

    #[test]
    fn detects_mv_names() {
        let sql = r#"SELECT * FROM mv_orders JOIN "mv_Sales" ON mv_orders.id = "mv_Sales".id"#;
        let mut names = find_mv_names(sql);
        names.sort();
        assert_eq!(names, vec!["mv_orders", "mv_Sales"]);
    }

    #[test]
    fn query_result_wraps_batches() {
        let schema = SchemaRef::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )
        .expect("batch");
        let result = to_query_result(vec![batch]);
        assert_eq!(result.schema.fields().len(), 1);
        assert_eq!(result.batches.len(), 1);
    }
}
