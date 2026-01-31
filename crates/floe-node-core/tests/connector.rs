use std::collections::BTreeSet;

use floe_node_core::connector::{ConnectorContext, run_connector};
use floe_node_core::generator::{Config, NexmarkConnector};
use floe_node_core::source;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn nexmark_connector_emits_events() {
    let (tx, mut rx) = source::channel(16);
    let config = Config {
        events_per_second: 1000.0,
        max_events: Some(3),
    };
    let mut connector = NexmarkConnector::new(config).expect("connector");
    let ctx = ConnectorContext::new(tx);

    run_connector(&mut connector, &ctx, CancellationToken::new())
        .await
        .expect("run connector");
    drop(ctx);

    let mut events = Vec::new();
    while let Some(event) = rx.recv().await {
        events.push(event);
    }

    assert_eq!(events.len(), 3);
    let names: BTreeSet<_> = events.iter().map(|event| event.source()).collect();
    let expected: BTreeSet<_> = vec!["nexmark_person", "nexmark_auction", "nexmark_bid"]
        .into_iter()
        .collect();
    assert!(names.is_subset(&expected));
}
