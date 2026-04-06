use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use floe_executor::SourceRowDecoder;
use floe_node::connector::{ConnectorContext, run_connector};
use floe_node::file_connector::{FileConnector, FileConnectorConfig};
use floe_node::generator;
use floe_node::source;
use serde_json::json;
use tokio_util::sync::CancellationToken;

use crate::harness::MvTestHarness;
use crate::helpers::wait_for_version;
use crate::rows::int_rows;

#[tokio::test]
async fn file_connector_ingests_and_queries() -> Result<()> {
    let mut harness = MvTestHarness::new(
        "mv_file",
        "CREATE MATERIALIZED VIEW mv_file AS \
         SELECT auction, bidder, price * 2 AS price \
         FROM nexmark_bid WHERE bidder = 42",
    )
    .await?;

    let path = write_temp_events(&[
        json!({"source": "nexmark_bid", "data": {
            "auction": 1, "bidder": 42, "price": 100,
            "channel": "web", "url": "http://a", "date_time": 10, "extra": ""
        }}),
        json!({"source": "nexmark_bid", "data": {
            "auction": 2, "bidder": 10, "price": 50,
            "channel": "web", "url": "http://b", "date_time": 11, "extra": ""
        }}),
        json!({"source": "nexmark_bid", "data": {
            "auction": 3, "bidder": 42, "price": 75,
            "channel": "web", "url": "http://c", "date_time": 12, "extra": ""
        }}),
    ])?;

    let (tx, mut rx) = source::channel(16);
    let config = FileConnectorConfig {
        path: path.clone(),
        default_source: None,
    };
    let mut connector = FileConnector::new(config, Vec::new());
    let ctx = ConnectorContext::new(tx);
    run_connector(&mut connector, &ctx, CancellationToken::new()).await?;
    drop(ctx);

    let definitions = generator::definitions()?;
    let decoders: HashMap<String, SourceRowDecoder> = definitions
        .into_iter()
        .map(|definition| {
            (
                definition.name().to_string(),
                SourceRowDecoder::new(definition),
            )
        })
        .collect();

    while let Some(batch) = rx.recv().await {
        for event in batch {
            let Some(decoder) = decoders.get(event.source()) else {
                continue;
            };
            let (encoded, _ts) = decoder.encode_row_key(&event)?;
            let Some(writer) = harness.outer.writer_mut(event.source()) else {
                continue;
            };
            writer.append_encoded(encoded, 1)?;
        }
    }
    harness.outer.tick_all().await?;

    wait_for_version(&harness.mv_registry, &harness.view_name, 1).await?;

    let (session, _bridge) = harness.session_with_view().await?;
    let df = session
        .sql("SELECT auction, bidder, price FROM mv_file ORDER BY auction")
        .await?;
    let batches = df.collect().await?;
    let rows = int_rows(&batches);
    assert_eq!(rows, vec![vec![1, 42, 200], vec![3, 42, 150]]);

    std::fs::remove_file(path)?;
    Ok(())
}

fn write_temp_events(events: &[serde_json::Value]) -> Result<PathBuf> {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    path.push(format!("floe-file-connector-{nanos}.jsonl"));

    let contents = events
        .iter()
        .map(|event| serde_json::to_string(event))
        .collect::<Result<Vec<_>, _>>()?
        .join("\n");
    std::fs::write(&path, contents)?;
    Ok(path)
}
