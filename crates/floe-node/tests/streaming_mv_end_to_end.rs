use std::process::Stdio;
use std::time::Duration;

#[path = "support/ports.rs"]
mod ports;

use anyhow::{Context, Result, anyhow, bail};
use ports::find_unused_port;
use tokio::net::TcpStream;
use tokio::process::Command;
use tokio::time::{Instant, interval};
use tokio_postgres::NoTls;

const MV_SQL: &str = "CREATE MATERIALIZED VIEW mv_bid_passthrough AS \
     SELECT auction, bidder, price FROM nexmark_bid";

#[tokio::test]
#[ignore = "requires TCP sockets; run with cargo test -p floe-node --test streaming_mv_end_to_end -- --ignored"]
async fn floe_node_streams_mv_rows_over_pgwire() -> Result<()> {
    let port = find_unused_port()?;
    let addr = format!("127.0.0.1:{port}");
    // let data_dir = tempdir().context("create temp SlateDB directory")?;

    let binary = env!("CARGO_BIN_EXE_floe-node");
    let mut child = Command::new(binary)
        .arg("run")
        .arg("--pgwire-addr")
        .arg(&addr)
        .arg("--admin-port")
        .arg("0")
        .arg("--events-per-second")
        .arg("1000")
        .arg("--max-events")
        .arg("5000")
        .arg("--mv-query")
        .arg(MV_SQL)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("spawn floe-node binary")?;

    let test_result = async {
        wait_for_pgwire(&addr).await?;

        let (client, connection) =
            tokio_postgres::connect(&format!("host=127.0.0.1 port={port} user=postgres"), NoTls)
                .await
                .context("connect to floe-node pgwire endpoint")?;
        let connection_handle = tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::warn!(error = %err, "pgwire connection closed");
            }
        });

        let rows_result = wait_for_bid_rows(&client).await;
        connection_handle.abort();
        let _ = connection_handle.await;

        let rows = rows_result.context("materialized view never produced rows")?;

        if rows.is_empty() {
            bail!("expected mv_bid_passthrough to contain at least one row");
        }

        Ok(())
    }
    .await;

    child.start_kill().ok();
    let _ = child.wait().await;

    test_result
}

async fn wait_for_pgwire(addr: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut poll = interval(Duration::from_millis(100));
    let mut attempts = 0usize;
    loop {
        match TcpStream::connect(addr).await {
            Ok(stream) => {
                drop(stream);
                return Ok(());
            }
            Err(err) if Instant::now() < deadline => {
                attempts = attempts.saturating_add(1);
                if attempts == 25 {
                    tracing::warn!(error = %err, "waiting for pgwire listener");
                }
                poll.tick().await;
            }
            Err(err) => bail!("pgwire listener never became ready: {err}"),
        }
    }
}

async fn wait_for_bid_rows(client: &tokio_postgres::Client) -> Result<Vec<tokio_postgres::Row>> {
    let sql = "SELECT auction, bidder, price FROM mv_bid_passthrough LIMIT 5";
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut poll = interval(Duration::from_millis(100));
    loop {
        match client.query(sql, &[]).await {
            Ok(rows) if !rows.is_empty() => return Ok(rows),
            Ok(_) if Instant::now() < deadline => {
                poll.tick().await;
            }
            Err(err) => {
                // Connection is ready but the mv may not be registered yet.
                tracing::debug!(error = %err, "query attempt failed");
                if Instant::now() >= deadline {
                    return Err(anyhow!("timed out waiting for rows from {sql}: {err}"));
                }
                poll.tick().await;
            }
            Ok(_) => return Err(anyhow!("timed out waiting for rows from {sql}")),
        }
    }
}
