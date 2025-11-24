use std::sync::Arc;

use anyhow::{Context, Result};
use dbsp::circuit::{CircuitNode, CircuitPlan, DbspNodeKind, DbspSinkNode, DbspSourceNode};
use dbsp::relation_state::RelationState;
use dbsp::storage::dictionary::Dictionary;
use dbsp::storage::KeyValueTable;
use dbsp::stream::runtime::{Pipeline, PipelineBuilder};
use dbsp::stream::operations::basic::differentiate_zset_stream_live;
use dbsp::collections::zset::VersionedZSet;
use dbsp::{DistinctOp, MapOp};

use crate::dbsp_table_environment::DbspTableEnvironment;
use crate::materialized_view::MaterializedViewRegistry;
use crate::operators::MvSinkOp;
use crate::namespaces;

pub struct PipelineFromCircuit<'a> {
    pub plan: &'a CircuitPlan,
    pub tables: &'a DbspTableEnvironment,
    pub registry: Arc<MaterializedViewRegistry>,
}

impl<'a> PipelineFromCircuit<'a> {
    // MV pipeline shape today (keep in sync with operator semantics):
    //
    // - `source_stream` is a `Stream<ZSetHandle>` that yields snapshots from the
    //   base table handle (not incremental deltas).
    // - `DistinctOp` runs DBSP-style distinct over a delta stream to drop repeated
    //   records.
    // - `MapOp` currently uses an identity projector over the row bytes.
    // - `MvSinkOp` applies the incoming deltas to the MV's backing `VersionedZSet`.
    //
    // Note: although every operator implements `DeltaOperator` and expects deltas,
    // the `source_stream` feed is snapshot-based today, so the pipeline processes
    // full snapshots through delta-oriented operators.
    pub async fn build_mv_pipeline(&self, view_name: &str) -> Result<Pipeline> {
        let sink = self
            .find_sink(view_name)
            .context("unable to resolve sink in plan")?;
        let source_node = self.walk_to_source(sink)?;

        let snapshot_stream = self
            .tables
            .handle_stream_for(source_node.table)
            .context("no handle stream for source table")?;

        // Turn snapshot Stream<ZSetHandle> into per-step delta Stream<ZSetHandle>.
        // Keys are encoded rows, so K = Vec<u8>.
        let source_stream = differentiate_zset_stream_live::<Vec<u8>>(&snapshot_stream)
            .await
            .context("compute per-step deltas for source stream")?;

        let table = self.tables.table();
        let distinct_state = self.build_relation_state("distinct_state", table.clone()).await?;
        let distinct_output =
            self.build_versioned("distinct_output", table.clone()).await?;
        let distinct = DistinctOp::new(distinct_state, table.clone(), distinct_output);

        let map_state = self.build_relation_state("map_state", table.clone()).await?;
        let map_output = self.build_versioned("map_output", table.clone()).await?;
        let projector: Arc<dyn Fn(&Vec<u8>) -> Vec<u8> + Send + Sync> =
            Arc::new(|bytes: &Vec<u8>| bytes.clone());
        let map = MapOp::new(projector, map_state, table.clone(), map_output);

        let sink_state = self
            .build_relation_state(&namespaces::materialized_view(view_name)?, table.clone())
            .await?;
        let sink = MvSinkOp::new(
            sink_state,
            view_name.to_string(),
            self.registry.clone(),
            table,
        );

        Ok(PipelineBuilder::new(vec![source_stream])
            .push_op(distinct)
            .push_op(map)
            .push_op(sink)
            .build())
    }

    fn find_sink(&self, view_name: &str) -> Option<&CircuitNode> {
        self.plan.nodes.iter().find(|node| {
            if let DbspNodeKind::Sink(DbspSinkNode { name, .. }) = &node.kind {
                name == view_name
            } else {
                false
            }
        })
    }

    fn walk_to_source<'b>(&'b self, sink: &'b CircuitNode) -> Result<&'b DbspSourceNode> {
        let mut current = sink;
        loop {
            if let DbspNodeKind::Source(src) = &current.kind {
                return Ok(src);
            }
            let input_id = current
                .inputs
                .first()
                .copied()
                .context("expected unary pipeline")?;
            current = self
                .plan
                .node(input_id)
                .context("missing node referenced by input")?;
        }
    }

    async fn build_relation_state(
        &self,
        namespace: &str,
        table: Arc<dyn KeyValueTable>,
    ) -> Result<RelationState<Vec<u8>>> {
        let dict = self.build_dictionary(namespace, table.clone()).await?;
        let integrated = VersionedZSet::new(
            dict,
            table.clone(),
            namespace.to_string(),
        )
        .await
        .context("build relation state")?;
        let latest_handle = integrated
            .current_handle()
            .unwrap_or_else(|| integrated.handle_for_version(0));
        Ok(RelationState {
            integrated,
            latest_handle,
        })
    }

    async fn build_dictionary(
        &self,
        namespace: &str,
        table: Arc<dyn KeyValueTable>,
    ) -> Result<Arc<Dictionary<Vec<u8>>>> {
        let dict = Dictionary::with_table(table, namespace.to_string(), None)
            .await
            .context("build dictionary")?;
        Ok(Arc::new(dict))
    }

    async fn build_versioned(
        &self,
        namespace: &str,
        table: Arc<dyn KeyValueTable>,
    ) -> Result<VersionedZSet<Vec<u8>>> {
        let dict = self.build_dictionary(namespace, table.clone()).await?;
        VersionedZSet::new(dict, table, namespace.to_string())
            .await
            .context("create versioned zset")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbsp::storage::SlateTable;
    use dbsp::circuit::tables::nexmark_bid_table;
    use nexmark::event::Bid;
    use object_store::memory::InMemory;
    use slatedb::Db;
    use crate::table_provider::MaterializedViewTableProvider;
    use datafusion::arrow::array::{Int64Array, StringArray};

    async fn build_db() -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(Db::open("plan_to_pipeline", store).await.expect("open SlateDB"))
    }

    #[tokio::test]
    async fn builds_pipeline_for_bid_sink() {
        let db = build_db().await;
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db.clone()));
        let mut env = DbspTableEnvironment::with_table(table.clone())
            .await
            .expect("env");

        let source_node = CircuitNode {
            id: 0,
            kind: DbspNodeKind::Source(DbspSourceNode {
                table: nexmark_bid_table(),
            }),
            inputs: Vec::new(),
            output_schema: nexmark_bid_table().schema().clone(),
        };
        let sink_node = CircuitNode {
            id: 1,
            kind: DbspNodeKind::Sink(DbspSinkNode::new(
                "nexmark_bid_mv",
                nexmark_bid_table().schema().clone(),
            )),
            inputs: vec![0],
            output_schema: nexmark_bid_table().schema().clone(),
        };
        let plan = CircuitPlan {
            root: 1,
            nodes: vec![source_node, sink_node],
        };

        let registry = Arc::new(MaterializedViewRegistry::new());
        registry.register("nexmark_bid_mv");
        let builder = PipelineFromCircuit {
            plan: &plan,
            tables: &env,
            registry: registry.clone(),
        };
        let mut pipeline = builder
            .build_mv_pipeline("nexmark_bid_mv")
            .await
            .expect("build pipeline");

        let bid = Bid {
            auction: 1,
            bidder: 2,
            price: 3,
            channel: "chan".to_string(),
            url: "url".to_string(),
            date_time: 4,
            extra: "x".to_string(),
        };
        let key = crate::dbsp_table_environment::encode_bid_row(&bid).expect("encode bid");
        env.bid.add_delta(key, 1);
        env.bid.flush().await.expect("flush bid stream");

        pipeline.step_once().await.expect("step");

        let view = registry
            .get("nexmark_bid_mv")
            .expect("view registered");
        let persisted = view.dbsp_state().expect("persisted state");
        assert_eq!(
            persisted.namespace(),
            namespaces::materialized_view("nexmark_bid_mv").expect("namespace")
        );
        assert_eq!(persisted.version(), 1);

        let provider = MaterializedViewTableProvider::new(
            registry,
            "nexmark_bid_mv",
            nexmark_bid_table().schema().to_arrow_schema(),
        );
        let batches = provider
            .build_batches_for_test()
            .await
            .expect("build batches");
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        let auction_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("auction col");
        let bidder_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("bidder col");
        let price_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("price col");
        let channel_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("channel col");
        let url_col = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("url col");
        assert_eq!(auction_col.value(0), 1);
        assert_eq!(bidder_col.value(0), 2);
        assert_eq!(price_col.value(0), 3);
        assert_eq!(channel_col.value(0), "chan");
        assert_eq!(url_col.value(0), "url");
    }
}
