use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[path = "support/node_process.rs"]
mod node_process;
#[path = "support/ports.rs"]
mod ports;
#[path = "support/wait.rs"]
mod wait;

use anyhow::{Context, Result, anyhow, bail, ensure};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use node_process::{
    post_bid_with_extra, spawn_node, spawn_node_with_args, stop_child, wait_for_healthz,
};
use ports::find_unused_port;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_postgres::NoTls;
use wait::wait_until;

const BID_MV_SQL: &str = "CREATE MATERIALIZED VIEW mv_acceptance_bid AS \
     SELECT auction, bidder, price FROM nexmark_bid";
const JOIN_MV_SQL: &str = "CREATE MATERIALIZED VIEW mv_acceptance_join AS \
     SELECT b.auction, b.bidder, b.price, a.seller \
     FROM nexmark_bid b JOIN nexmark_auction a ON b.auction = a.id";

#[path = "ga_acceptance/basic_kafka.rs"]
mod basic_kafka;
#[path = "ga_acceptance/postgres_restart_shared.rs"]
mod postgres_restart_shared;
#[path = "ga_acceptance/postgres_sinks.rs"]
mod postgres_sinks;
#[path = "ga_acceptance/postgres_sql_snapshot.rs"]
mod postgres_sql_snapshot;

#[derive(Clone)]
struct SinkCaptureState {
    sender: mpsc::Sender<Value>,
}
async fn sink_collect(
    State(state): State<SinkCaptureState>,
    Json(payload): Json<Value>,
) -> StatusCode {
    if state.sender.send(payload).await.is_err() {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    StatusCode::OK
}

async fn spawn_sink_server(
    port: u16,
    sender: mpsc::Sender<Value>,
) -> Result<tokio::task::JoinHandle<()>> {
    let state = SinkCaptureState { sender };
    let app = Router::new()
        .route("/collect", post(sink_collect))
        .with_state(state);
    let listener = TokioTcpListener::bind(("127.0.0.1", port))
        .await
        .with_context(|| format!("bind sink receiver on port {port}"))?;
    Ok(tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            tracing::error!(error = %err, "sink receiver server exited");
        }
    }))
}

fn payload_to_rows(payload: Value) -> Vec<Value> {
    match payload {
        Value::Array(rows) => rows,
        row => vec![row],
    }
}

async fn wait_for_admin_metrics_contains(admin_port: u16, needle: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{admin_port}/metrics");
    wait_until(
        format!("admin metrics to contain {needle}"),
        Duration::from_secs(12),
        Duration::from_millis(100),
        || async {
            match client.get(&url).send().await {
                Ok(response) if response.status() == StatusCode::OK => {
                    let body = response.text().await.context("read admin metrics body")?;
                    Ok(body.contains(needle).then_some(()))
                }
                Ok(_) => Ok(None),
                Err(err) => Err(err.into()),
            }
        },
    )
    .await
}

async fn post_bid(addr: &str, auction: i64, bidder: i64, price: i64) -> Result<()> {
    post_bid_with_extra(addr, auction, bidder, price, "acceptance").await
}

async fn create_kafka_topic(brokers: &str, topic: &str) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .context("create kafka admin client")?;
    let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
    let results = admin
        .create_topics(&[new_topic], &AdminOptions::new())
        .await
        .context("create kafka restart topic")?;
    for result in results {
        result
            .map(|_| ())
            .map_err(|(topic, err)| anyhow::anyhow!("create kafka topic {topic}: {err}"))?;
    }
    Ok(())
}

async fn produce_auction(
    producer: &FutureProducer,
    topic: &str,
    id: i64,
    seller: i64,
) -> Result<()> {
    let payload = json!({
        "source": "nexmark_auction",
        "data": {
            "id": id,
            "seller": seller,
            "category": 17,
            "initial_bid": 100,
            "reserve": 500,
            "item_name": "restart-test",
            "description": "restart-test",
            "expires": 1_700_100_000_i64 + id,
            "date_time": 1_700_000_000_i64 + id,
            "extra": "kafka_restart"
        }
    })
    .to_string();
    let record = FutureRecord::<(), _>::to(topic).payload(&payload);
    producer
        .send(record, Duration::from_secs(5))
        .await
        .map_err(|(err, _)| err)
        .context("produce kafka auction")?;
    Ok(())
}

async fn produce_bid(
    producer: &FutureProducer,
    topic: &str,
    auction: i64,
    bidder: i64,
    price: i64,
) -> Result<()> {
    let payload = json!({
        "source": "nexmark_bid",
        "data": {
            "auction": auction,
            "bidder": bidder,
            "price": price,
            "channel": "web",
            "url": "http://example.com",
            "date_time": 1_700_000_000_i64 + auction,
            "extra": "kafka_restart"
        }
    })
    .to_string();
    let record = FutureRecord::<(), _>::to(topic).payload(&payload);
    producer
        .send(record, Duration::from_secs(5))
        .await
        .map_err(|(err, _)| err)
        .context("produce kafka bid")?;
    Ok(())
}

async fn wait_for_auction_count_at_least(
    pg_port: u16,
    auction: i64,
    min_count: i64,
) -> Result<i64> {
    node_process::wait_for_count_at_least(
        format!("mv_acceptance_bid auction={auction}"),
        min_count,
        80,
        || query_auction_count(pg_port, auction),
    )
    .await
}

async fn wait_for_join_count_at_least(pg_port: u16, auction: i64, min_count: i64) -> Result<i64> {
    node_process::wait_for_count_at_least(
        format!("mv_acceptance_join auction={auction}"),
        min_count,
        120,
        || query_join_count(pg_port, auction),
    )
    .await
}

async fn query_auction_count(pg_port: u16, auction: i64) -> Result<i64> {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={pg_port} user=postgres"),
        NoTls,
    )
    .await
    .context("connect to pgwire")?;
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    let row = client
        .query_one(
            "SELECT COUNT(*) FROM mv_acceptance_bid WHERE auction = $1",
            &[&auction],
        )
        .await
        .context("query acceptance mv count")?;
    connection_handle.abort();
    let _ = connection_handle.await;
    Ok(row.get::<_, i64>(0))
}

async fn query_join_count(pg_port: u16, auction: i64) -> Result<i64> {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={pg_port} user=postgres"),
        NoTls,
    )
    .await
    .context("connect to pgwire")?;
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    let row = client
        .query_one(
            "SELECT COUNT(*) FROM mv_acceptance_join WHERE auction = $1",
            &[&auction],
        )
        .await
        .context("query acceptance join mv count")?;
    connection_handle.abort();
    let _ = connection_handle.await;
    Ok(row.get::<_, i64>(0))
}

async fn wait_for_mv_price_count_at_least(
    pg_port: u16,
    mv_name: &str,
    auction: i64,
    price: i64,
    min_count: i64,
) -> Result<i64> {
    node_process::wait_for_count_at_least(
        format!("{mv_name} auction={auction}, price={price}"),
        min_count,
        120,
        || query_mv_price_count(pg_port, mv_name, auction, price),
    )
    .await
}

async fn query_mv_price_count(
    pg_port: u16,
    mv_name: &str,
    auction: i64,
    price: i64,
) -> Result<i64> {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={pg_port} user=postgres"),
        NoTls,
    )
    .await
    .context("connect to pgwire")?;
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    let query = format!("SELECT COUNT(*) FROM {mv_name} WHERE auction = $1 AND price = $2");
    let row = client
        .query_one(&query, &[&auction, &price])
        .await
        .with_context(|| format!("query {mv_name} count"))?;
    connection_handle.abort();
    let _ = connection_handle.await;
    Ok(row.get::<_, i64>(0))
}

async fn wait_for_auction_seller_count_at_least(
    pg_port: u16,
    mv_name: &str,
    id: i64,
    seller: i64,
    min_count: i64,
) -> Result<i64> {
    node_process::wait_for_count_at_least(
        format!("{mv_name} id={id}, seller={seller}"),
        min_count,
        120,
        || query_auction_seller_count(pg_port, mv_name, id, seller),
    )
    .await
}

async fn query_auction_seller_count(
    pg_port: u16,
    mv_name: &str,
    id: i64,
    seller: i64,
) -> Result<i64> {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={pg_port} user=postgres"),
        NoTls,
    )
    .await
    .context("connect to pgwire")?;
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    let query = format!("SELECT COUNT(*) FROM {mv_name} WHERE id = $1 AND seller = $2");
    let row = client
        .query_one(&query, &[&id, &seller])
        .await
        .with_context(|| format!("query {mv_name} auction count"))?;
    connection_handle.abort();
    let _ = connection_handle.await;
    Ok(row.get::<_, i64>(0))
}

async fn wait_for_join_mv_count_at_least(
    pg_port: u16,
    mv_name: &str,
    auction: i64,
    bidder: i64,
    seller: i64,
    min_count: i64,
) -> Result<i64> {
    node_process::wait_for_count_at_least(
        format!("{mv_name} auction={auction}, bidder={bidder}, seller={seller}"),
        min_count,
        120,
        || query_join_mv_count(pg_port, mv_name, auction, bidder, seller),
    )
    .await
}

async fn query_join_mv_count(
    pg_port: u16,
    mv_name: &str,
    auction: i64,
    bidder: i64,
    seller: i64,
) -> Result<i64> {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={pg_port} user=postgres"),
        NoTls,
    )
    .await
    .context("connect to pgwire")?;
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    let query = format!(
        "SELECT COUNT(*) FROM {mv_name} WHERE auction = $1 AND bidder = $2 AND seller = $3"
    );
    let row = client
        .query_one(&query, &[&auction, &bidder, &seller])
        .await
        .with_context(|| format!("query {mv_name} join count"))?;
    connection_handle.abort();
    let _ = connection_handle.await;
    Ok(row.get::<_, i64>(0))
}

async fn wait_for_bidder_aggregate(
    pg_port: u16,
    mv_name: &str,
    bidder: i64,
    expected_count: i64,
    expected_total: i64,
) -> Result<(i64, i64)> {
    wait_until(
        format!(
            "{mv_name} aggregate bidder={bidder} count={expected_count} total={expected_total}"
        ),
        Duration::from_secs(12),
        Duration::from_millis(100),
        || async {
            match query_bidder_aggregate(pg_port, mv_name, bidder).await {
                Ok(Some((bid_count, total_price)))
                    if bid_count == expected_count && total_price == expected_total =>
                {
                    Ok(Some((bid_count, total_price)))
                }
                Ok(_) => Ok(None),
                Err(err) => Err(err),
            }
        },
    )
    .await
}

async fn query_bidder_aggregate(
    pg_port: u16,
    mv_name: &str,
    bidder: i64,
) -> Result<Option<(i64, i64)>> {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={pg_port} user=postgres"),
        NoTls,
    )
    .await
    .context("connect to pgwire")?;
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    let query = format!("SELECT bid_count, total_price FROM {mv_name} WHERE bidder = $1");
    let rows = client
        .query(&query, &[&bidder])
        .await
        .with_context(|| format!("query {mv_name} aggregate"))?;
    let aggregate = match rows.as_slice() {
        [] => None,
        [row] => {
            let bid_count = row.try_get::<_, i64>(0).context("decode bid_count")?;
            let total_price = row.try_get::<_, i64>(1).context("decode total_price")?;
            Some((bid_count, total_price))
        }
        rows => bail!(
            "{mv_name} returned {} aggregate rows for bidder={bidder}",
            rows.len()
        ),
    };
    drop(client);
    connection_handle.abort();
    let _ = connection_handle.await;
    Ok(aggregate)
}

fn sql_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

async fn cleanup_postgres_sink_acceptance(
    client: &tokio_postgres::Client,
    publication: &str,
    slot: &str,
    source_table: &str,
    target_table: &str,
) {
    let _ = client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {publication};"))
        .await;
    let _ = client
        .execute(
            "SELECT pg_drop_replication_slot($1)
             WHERE EXISTS (
               SELECT 1
               FROM pg_replication_slots
               WHERE slot_name = $1
             )",
            &[&slot],
        )
        .await;
    let _ = client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {target_table};
             DROP TABLE IF EXISTS {source_table};"
        ))
        .await;
}

async fn wait_for_postgres_sink_row(
    client: &tokio_postgres::Client,
    table: &str,
    id: i64,
    amount: i64,
    note: Option<&str>,
) -> Result<()> {
    wait_until(
        format!("Postgres sink table {table} id={id} amount={amount} note={note:?}"),
        Duration::from_secs(12),
        Duration::from_millis(100),
        || async {
            let rows = client
                .query(
                    &format!("SELECT amount, note FROM {table} WHERE id = $1"),
                    &[&id],
                )
                .await
                .with_context(|| format!("query Postgres sink table {table}"))?;
            match rows.as_slice() {
                [row] => {
                    let row_amount = row.try_get::<_, i64>(0).context("decode sink amount")?;
                    let row_note = row
                        .try_get::<_, Option<String>>(1)
                        .context("decode sink note")?;
                    if row_amount == amount && row_note.as_deref() == note {
                        Ok(Some(()))
                    } else {
                        Err(anyhow!("last seen amount={row_amount}, note={row_note:?}"))
                    }
                }
                rows => Err(anyhow!("last seen {} rows", rows.len())),
            }
        },
    )
    .await
}

async fn wait_for_postgres_sink_typed_row(
    client: &tokio_postgres::Client,
    table: &str,
    id: i64,
    active: bool,
    order_date: &str,
    amount: &str,
    note: Option<&str>,
) -> Result<()> {
    wait_until(
        format!(
            "Postgres typed sink table {table} id={id} active={active} order_date={order_date} amount={amount} note={note:?}"
        ),
        Duration::from_secs(12),
        Duration::from_millis(100),
        || async {
            let rows = client
                .query(
                    &format!(
                        "SELECT active, order_date::text, amount::text, note FROM {table} WHERE id = $1"
                    ),
                    &[&id],
                )
                .await
                .with_context(|| format!("query Postgres typed sink table {table}"))?;
            match rows.as_slice() {
                [row] => {
                    let row_active = row.try_get::<_, bool>(0).context("decode sink active")?;
                    let row_order_date = row
                        .try_get::<_, String>(1)
                        .context("decode sink order_date")?;
                    let row_amount = row.try_get::<_, String>(2).context("decode sink amount")?;
                    let row_note = row
                        .try_get::<_, Option<String>>(3)
                        .context("decode sink note")?;
                    if row_active == active
                        && row_order_date == order_date
                        && row_amount == amount
                        && row_note.as_deref() == note
                    {
                        Ok(Some(()))
                    } else {
                        Err(anyhow!(
                            "last seen active={row_active}, order_date={row_order_date}, amount={row_amount}, note={row_note:?}"
                        ))
                    }
                }
                rows => Err(anyhow!("last seen {} rows", rows.len())),
            }
        },
    )
    .await
}

async fn wait_for_postgres_sink_absent(
    client: &tokio_postgres::Client,
    table: &str,
    id: i64,
) -> Result<()> {
    wait_until(
        format!("Postgres sink table {table} id={id} to be absent"),
        Duration::from_secs(12),
        Duration::from_millis(100),
        || async {
            let row = client
                .query_one(
                    &format!("SELECT COUNT(*)::BIGINT FROM {table} WHERE id = $1"),
                    &[&id],
                )
                .await
                .with_context(|| format!("query Postgres sink table {table} absence"))?;
            let count = row.try_get::<_, i64>(0).context("decode sink count")?;
            if count == 0 {
                Ok(Some(()))
            } else {
                Err(anyhow!("last seen count={count}"))
            }
        },
    )
    .await
}

async fn wait_for_rows_matching(
    path: &Path,
    predicate: impl Fn(&Value) -> bool,
) -> Result<Vec<Value>> {
    node_process::wait_for_jsonl_rows_matching(path, 120, predicate).await
}
