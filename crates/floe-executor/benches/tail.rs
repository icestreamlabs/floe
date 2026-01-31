use std::sync::Arc;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::context::SessionContext;
use datafusion::scalar::ScalarValue;
use dbsp::StreamRetention;
use futures::StreamExt;
use object_store::memory::InMemory;
use slatedb::Db;
use tokio::runtime::Runtime;

use floe_executor::dbsp_bridge::DbspBridge;
use floe_executor::encoding::encode_projected_row_key;
use floe_executor::materialized_view::DbspPersistedState;
use floe_executor::mv::registry::MaterializedViewRegistry;
use floe_executor::tail::{TailParams, execute_tail};

fn build_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

fn scalar_row(value: i64) -> Vec<ScalarValue> {
    vec![ScalarValue::Int64(Some(value))]
}

async fn append_values(
    view: &mut floe_executor::dbsp_bridge::DbspView,
    values: &[i64],
) -> anyhow::Result<dbsp::handles::ZSetHandle> {
    for value in values {
        let row = scalar_row(*value);
        let encoded = encode_projected_row_key(&row)?;
        view.add_delta(encoded, 1);
    }
    Ok(view.flush().await?)
}

fn bench_tail_delta(c: &mut Criterion) {
    let mut group = c.benchmark_group("tail_delta_throughput");
    for rows in [1_000usize, 10_000usize] {
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_function(format!("rows_{rows}"), |b| {
            let rt = Runtime::new().expect("runtime");
            b.iter_batched(
                || rows,
                |rows| {
                    rt.block_on(async move {
                        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
                        let db = Arc::new(Db::open("tail-bench", store).await.expect("db"));
                        let mut bridge = DbspBridge::new(Arc::clone(&db)).await?;
                        let mut dbsp_view = bridge
                            .new_view("mv_tail_bench", StreamRetention::KeepLast { keep_last: 1 })
                            .await?;

                        let registry = Arc::new(MaterializedViewRegistry::new());
                        registry.set_schema("mv_tail_bench", build_schema());
                        let handle = registry.register("mv_tail_bench");

                        let values: Vec<i64> = (0..rows as i64).collect();
                        let handle1 = append_values(&mut dbsp_view, &values).await?;
                        let latest_view = dbsp_view.latest_handle_view();
                        let (dict, table, ns, version) = latest_view.into_parts();
                        let state = DbspPersistedState::new(dict, table, ns, version);
                        handle.set_dbsp_state(state);
                        let handle1_version = handle1.version as i64;
                        handle.publish_version(handle1_version, handle1);

                        let ctx = SessionContext::new();
                        let params = TailParams {
                            mv_name: "mv_tail_bench".to_string(),
                            with_snapshot: false,
                            as_of: Some(handle1_version),
                        };
                        let cancel = tokio_util::sync::CancellationToken::new();
                        let mut stream =
                            execute_tail(&ctx, registry.as_ref(), params, cancel.clone()).await?;

                        let handle2 = append_values(&mut dbsp_view, &values).await?;
                        let handle2_version = handle2.version as i64;
                        handle.publish_version(handle2_version, handle2);

                        let mut seen = 0usize;
                        while let Some(batch) = stream.next().await {
                            let batch = batch?;
                            seen += batch.batch.num_rows();
                            if seen >= rows {
                                break;
                            }
                        }
                        cancel.cancel();
                        Ok::<(), anyhow::Error>(())
                    })
                    .expect("bench iteration")
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_tail_delta);
criterion_main!(benches);
