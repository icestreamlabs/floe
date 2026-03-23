use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use datafusion::execution::context::SessionContext;

use crate::dbsp_bridge::DbspBridge;
use crate::materialized_view::{DbspPersistedState, MaterializedViewRegistry};
use crate::namespaces;
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

    let handle = registry.register(view_name.to_string());
    if handle.dbsp_state().is_none() && !handle.has_encoded_overlay() {
        tracing::info!(
            view = %view_name,
            "materialized view missing DBSP state, loading from SlateDB"
        );
        let namespace = namespaces::materialized_view(view_name)
            .context("derive namespace for materialized view")?;
        let latest_handle = bridge
            .latest_view_handle(&namespace)
            .await
            .with_context(|| format!("load latest handle for materialized view '{view_name}'"))?;
        let handle_view = bridge
            .handle_view_for(&latest_handle.ns, latest_handle.version)
            .await
            .with_context(|| {
                format!(
                    "open handle view for materialized view '{view}' (version {})",
                    latest_handle.version,
                    view = view_name
                )
            })?;
        let (dict, table, ns, version) = handle_view.into_parts();
        let logical_version = bridge
            .load_mv_logical_version(view_name)
            .await?
            .unwrap_or(version);
        let state =
            DbspPersistedState::new(dict, table, ns, version).with_logical_version(logical_version);
        handle.set_dbsp_state(state);
        handle.mark_state_non_authoritative();
        handle.publish_version(
            i64::try_from(logical_version).unwrap_or(i64::MAX),
            latest_handle,
        );
    }

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
    use crate::encoding::encode_projected_row_key;
    use anyhow::Result;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::scalar::ScalarValue;
    use dbsp::StreamRetention;
    use object_store::memory::InMemory;
    use slatedb::Db;

    const VIEW_NAME: &str = "mv_loader_test";

    #[tokio::test]
    async fn registers_provider_when_state_warm() -> Result<()> {
        let db = test_db("mv-loader-warm").await;
        let schema = test_schema();
        let state = seed_view(Arc::clone(&db), &[row(1)], Arc::clone(&schema), false).await?;

        let registry = Arc::new(MaterializedViewRegistry::new());
        let handle = registry.register(VIEW_NAME.to_string());
        registry.set_schema(VIEW_NAME.to_string(), Arc::clone(&schema));
        handle.set_dbsp_state(state.clone());
        handle.publish_logical_version(
            i64::try_from(state.logical_version()).expect("logical version"),
        );

        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let session = SessionContext::new();
        load_or_register_mv(&session, Arc::clone(&registry), &mut bridge, VIEW_NAME).await?;

        assert_eq!(query_values(&session).await?, vec![1]);
        Ok(())
    }

    #[tokio::test]
    async fn loads_state_from_slate_when_missing_in_registry() -> Result<()> {
        let db = test_db("mv-loader-cold").await;
        let schema = test_schema();
        let _ = seed_view(
            Arc::clone(&db),
            &[row(2), row(3)],
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
            handle.dbsp_state().is_some(),
            "state not recovered from SlateDB"
        );
        assert_eq!(query_values(&session).await?, vec![2, 3]);
        Ok(())
    }

    #[tokio::test]
    async fn recovers_schema_from_persisted_metadata() -> Result<()> {
        let db = test_db("mv-loader-schema").await;
        let schema = test_schema();
        let _ = seed_view(Arc::clone(&db), &[row(4)], Arc::clone(&schema), true).await?;

        let registry = Arc::new(MaterializedViewRegistry::new());
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let session = SessionContext::new();
        load_or_register_mv(&session, Arc::clone(&registry), &mut bridge, VIEW_NAME).await?;

        assert!(
            registry.schema(VIEW_NAME).is_some(),
            "schema should be recovered from SlateDB"
        );
        assert_eq!(query_values(&session).await?, vec![4]);
        Ok(())
    }

    #[tokio::test]
    async fn errors_when_schema_unavailable() {
        let db = test_db("mv-loader-error").await;
        let schema = test_schema();
        let _ = seed_view(Arc::clone(&db), &[row(5)], Arc::clone(&schema), false)
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

    fn row(value: i64) -> Vec<ScalarValue> {
        vec![ScalarValue::Int64(Some(value))]
    }

    async fn seed_view(
        db: Arc<Db>,
        rows: &[Vec<ScalarValue>],
        schema: SchemaRef,
        persist_schema: bool,
    ) -> Result<DbspPersistedState> {
        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
        let mut view = bridge
            .new_view(VIEW_NAME, StreamRetention::KeepLast { keep_last: 1 })
            .await?;
        for row in rows {
            let key = encode_projected_row_key(row)?;
            view.add_delta(key, 1);
        }
        view.flush().await?;
        if persist_schema {
            bridge
                .save_mv_schema(VIEW_NAME, Arc::clone(&schema))
                .await?;
        }
        let handle_view = view.latest_handle_view();
        let (dict, table, namespace, version) = handle_view.into_parts();
        Ok(DbspPersistedState::new(dict, table, namespace, version))
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
