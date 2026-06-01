use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use datafusion::execution::context::SessionContext;

use crate::dbsp_bridge::DbspBridge;
use crate::materialized_view::MaterializedViewRegistry;
use crate::table_provider::MaterializedViewTableProvider;

pub async fn load_or_register_mv(
    session: &SessionContext,
    registry: Arc<MaterializedViewRegistry>,
    bridge: &mut DbspBridge,
    view_name: &str,
) -> Result<()> {
    tracing::info!(view = %view_name, "load or register materialized view");
    if session.table(view_name).await.is_ok() {
        tracing::debug!(view = %view_name, "materialized view already registered");
        return Ok(());
    }

    let schema = match registry.schema(view_name) {
        Some(schema) => schema,
        None => {
            let recovered = bridge
                .load_mv_schema(view_name)
                .await
                .with_context(|| format!("load schema metadata for materialized view '{view_name}'"))?
                .ok_or_else(|| {
                    anyhow!(
                        "materialized view '{view}' is missing schema; ensure it was planned before loading",
                        view = view_name
                    )
                })?;
            registry.set_schema(view_name.to_string(), Arc::clone(&recovered));
            recovered
        }
    };

    registry.register(view_name.to_string());

    let provider = MaterializedViewTableProvider::new(
        Arc::clone(&registry),
        view_name.to_string(),
        schema.clone(),
    );

    if session.table(view_name).await.is_ok() {
        return Ok(());
    }
    session
        .register_table(view_name, Arc::new(provider))
        .with_context(|| format!("register materialized view '{view_name}'"))?;
    tracing::info!(
        view = %view_name,
        "materialized view registered with DataFusion session"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use dbsp::StreamRetention;
    use object_store::memory::InMemory;
    use slatedb::Db;

    const VIEW_NAME: &str = "mv_loader_test";

    #[tokio::test]
    async fn registers_provider_when_state_warm() -> Result<()> {
        let db = test_db("mv-loader-warm").await;
        let schema = test_schema();
        seed_view(
            Arc::clone(&db),
            &[encoded_i64_row(1)],
            Arc::clone(&schema),
            false,
        )
        .await?;

        let registry = Arc::new(MaterializedViewRegistry::new());
        let handle = registry.register(VIEW_NAME.to_string());
        registry.set_schema(VIEW_NAME.to_string(), Arc::clone(&schema));
        handle.publish_arrow_version(
            1,
            vec![RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from_iter_values([1_i64]))],
            )?],
            Vec::new(),
        );

        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let session = SessionContext::new();
        load_or_register_mv(&session, Arc::clone(&registry), &mut bridge, VIEW_NAME).await?;

        assert_eq!(query_values(&session).await?, vec![1]);
        Ok(())
    }

    #[tokio::test]
    async fn does_not_load_legacy_state_from_slate_when_missing_in_registry() -> Result<()> {
        let db = test_db("mv-loader-cold").await;
        let schema = test_schema();
        seed_view(
            Arc::clone(&db),
            &[encoded_i64_row(2), encoded_i64_row(3)],
            Arc::clone(&schema),
            false,
        )
        .await?;

        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.set_schema(VIEW_NAME.to_string(), Arc::clone(&schema));

        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let session = SessionContext::new();
        load_or_register_mv(&session, Arc::clone(&registry), &mut bridge, VIEW_NAME).await?;

        let handle = registry.get(VIEW_NAME).expect("view registered");
        assert!(
            handle.dbsp_state().is_none(),
            "legacy DBSP state should not be recovered from SlateDB"
        );
        assert_eq!(query_values(&session).await?, Vec::<i64>::new());
        Ok(())
    }

    #[tokio::test]
    async fn recovers_schema_from_persisted_metadata() -> Result<()> {
        let db = test_db("mv-loader-schema").await;
        let schema = test_schema();
        seed_view(
            Arc::clone(&db),
            &[encoded_i64_row(4)],
            Arc::clone(&schema),
            true,
        )
        .await?;

        let registry = Arc::new(MaterializedViewRegistry::new());
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let session = SessionContext::new();
        load_or_register_mv(&session, Arc::clone(&registry), &mut bridge, VIEW_NAME).await?;

        assert!(
            registry.schema(VIEW_NAME).is_some(),
            "schema should be recovered from SlateDB"
        );
        assert_eq!(query_values(&session).await?, Vec::<i64>::new());
        Ok(())
    }

    #[tokio::test]
    async fn errors_when_schema_unavailable() {
        let db = test_db("mv-loader-error").await;
        let schema = test_schema();
        let _ = seed_view(
            Arc::clone(&db),
            &[encoded_i64_row(5)],
            Arc::clone(&schema),
            false,
        )
        .await
        .expect("seed view");

        let registry = Arc::new(MaterializedViewRegistry::new());
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await.expect("bridge");
        let session = SessionContext::new();
        let err = load_or_register_mv(&session, Arc::clone(&registry), &mut bridge, VIEW_NAME)
            .await
            .expect_err("schema lookup should fail");
        assert!(err.to_string().contains("missing schema"));
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]))
    }

    fn encoded_i64_row(value: i64) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(4 + 1 + 8);
        encoded.extend_from_slice(&(1_u32).to_le_bytes());
        encoded.push(0x01);
        encoded.extend_from_slice(&value.to_le_bytes());
        encoded
    }

    async fn seed_view(
        db: Arc<Db>,
        rows: &[Vec<u8>],
        schema: SchemaRef,
        persist_schema: bool,
    ) -> Result<()> {
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let mut view = bridge
            .new_view(VIEW_NAME, StreamRetention::KeepLast { keep_last: 1 })
            .await?;
        view.add_deltas(rows.iter().cloned().map(|row| (row, 1)));
        view.flush().await?;
        if persist_schema {
            bridge
                .save_mv_schema(VIEW_NAME, Arc::clone(&schema))
                .await?;
        }
        Ok(())
    }

    async fn test_db(name: &str) -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(Db::open(name, store).await.expect("open SlateDB"))
    }

    async fn query_values(session: &SessionContext) -> Result<Vec<i64>> {
        let df = session
            .sql(&format!("SELECT value FROM {VIEW_NAME} ORDER BY value"))
            .await?;
        let batches = df.collect().await?;
        Ok(extract_values(&batches))
    }

    fn extract_values(batches: &[RecordBatch]) -> Vec<i64> {
        if batches.is_empty() {
            return Vec::new();
        }
        let mut rows = Vec::new();
        for batch in batches {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 column");
            for idx in 0..batch.num_rows() {
                rows.push(array.value(idx));
            }
        }
        rows
    }
}
