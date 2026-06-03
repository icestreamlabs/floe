use super::*;

#[tokio::test]
#[ignore = "requires native logical-replication Postgres; set FLOE_ACCEPTANCE_PG_DSN"]
#[serial_test::serial(postgres_cdc_acceptance)]
async fn postgres_cdc_table_restart_resumes_from_committed_lsn() -> Result<()> {
    let dsn = std::env::var("FLOE_ACCEPTANCE_PG_DSN")
        .context("set FLOE_ACCEPTANCE_PG_DSN for CDC acceptance")?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let pg_port = find_unused_port()?;
    let table = "nexmark_bid".to_string();
    let mv_name = format!("mv_floe_cdc_restart_{run_id}");
    let slot = format!("floe_acceptance_restart_{run_id}");
    let publication = format!("floe_acceptance_restart_pub_{run_id}");
    let temp_dir = TempDir::new().context("create temp dir")?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("cdc_restart_acceptance.json");

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to postgres for native cdc restart setup")?;
    let _connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres native cdc restart setup connection closed");
        }
    });

    client
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {publication};
             DROP TABLE IF EXISTS {table};
             CREATE TABLE {table} (
               auction BIGINT PRIMARY KEY,
               bidder BIGINT NOT NULL,
               price BIGINT NOT NULL,
               channel TEXT,
               url TEXT,
               date_time BIGINT NOT NULL,
               extra TEXT
             );
             CREATE PUBLICATION {publication} FOR TABLE {table};"
        ))
        .await
        .context("prepare native cdc restart table")?;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;

    let config = json!({
        "connectors": [
            {
                "type": "postgres_cdc",
                "connection": dsn,
                "slot": slot,
                "publication": publication,
                "include_tables": [table]
            }
        ]
    });
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)
        .context("write native cdc restart config")?;

    let sql = format!(
        "CREATE TABLE {table} (
            auction BIGINT PRIMARY KEY,
            bidder BIGINT NOT NULL,
            price BIGINT NOT NULL,
            channel TEXT,
            url TEXT,
            date_time BIGINT NOT NULL,
            extra TEXT
         );
         CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name} AS
         SELECT auction, bidder, price FROM {table}"
    );
    let mut first = spawn_node(&config_path, &data_dir, pg_port, Some(&sql)).await?;

    let first_result = async {
        client
            .execute(
                &format!(
                    "INSERT INTO {table} \
                     (auction, bidder, price, channel, url, date_time, extra) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7)"
                ),
                &[
                    &11_i64,
                    &20_i64,
                    &100_i64,
                    &"web",
                    &"http://example.com",
                    &1_700_000_011_i64,
                    &"before_restart",
                ],
            )
            .await
            .context("insert native cdc restart row")?;
        wait_for_mv_price_count_at_least(pg_port, &mv_name, 11, 100, 1).await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    stop_child(&mut first, "INT").await;
    first_result?;

    let mut restarted = spawn_node(&config_path, &data_dir, pg_port, Some(&sql)).await?;
    let restart_result = async {
        wait_for_mv_price_count_at_least(pg_port, &mv_name, 11, 100, 1).await?;
        assert_eq!(
            query_mv_price_count(pg_port, &mv_name, 11, 100).await?,
            1,
            "restarted CDC MV should expose the committed pre-restart row once"
        );

        client
            .execute(
                &format!("UPDATE {table} SET price = $1, extra = $2 WHERE auction = $3"),
                &[&175_i64, &"after_restart", &11_i64],
            )
            .await
            .context("update native cdc row after restart")?;
        wait_for_mv_price_count_at_least(pg_port, &mv_name, 11, 175, 1).await?;
        assert_eq!(
            query_mv_price_count(pg_port, &mv_name, 11, 100).await?,
            0,
            "post-restart CDC update should retract the old MV row"
        );
        assert_eq!(
            query_mv_price_count(pg_port, &mv_name, 11, 175).await?,
            1,
            "post-restart CDC update should insert the new MV row once"
        );
        Ok::<(), anyhow::Error>(())
    }
    .await;
    stop_child(&mut restarted, "INT").await;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;
    let _ = client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {publication};"))
        .await;
    let _ = client
        .batch_execute(&format!("DROP TABLE IF EXISTS {table};"))
        .await;
    restart_result
}

#[tokio::test]
#[ignore = "requires native logical-replication Postgres; set FLOE_ACCEPTANCE_PG_DSN"]
#[serial_test::serial(postgres_cdc_acceptance)]
async fn postgres_cdc_shared_source_transaction_feeds_join_mv() -> Result<()> {
    let dsn = std::env::var("FLOE_ACCEPTANCE_PG_DSN")
        .context("set FLOE_ACCEPTANCE_PG_DSN for CDC acceptance")?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let pg_port = find_unused_port()?;
    let bid_table = "nexmark_bid".to_string();
    let auction_table = "nexmark_auction".to_string();
    let bid_mv = format!("mv_floe_cdc_shared_bid_{run_id}");
    let auction_mv = format!("mv_floe_cdc_shared_auction_{run_id}");
    let join_mv = format!("mv_floe_cdc_shared_join_{run_id}");
    let slot = format!("floe_acceptance_join_{run_id}");
    let publication = format!("floe_acceptance_join_pub_{run_id}");
    let temp_dir = TempDir::new().context("create temp dir")?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("cdc_join_acceptance.json");

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to postgres for shared-source cdc setup")?;
    let _connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres shared-source cdc setup connection closed");
        }
    });

    client
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {publication};
             DROP TABLE IF EXISTS {bid_table};
             DROP TABLE IF EXISTS {auction_table};
             CREATE TABLE {auction_table} (
               id BIGINT PRIMARY KEY,
               seller BIGINT NOT NULL,
               category BIGINT NOT NULL,
               initial_bid BIGINT NOT NULL,
               reserve BIGINT NOT NULL,
               item_name TEXT,
               description TEXT,
               expires BIGINT NOT NULL,
               date_time BIGINT NOT NULL,
               extra TEXT
             );
             CREATE TABLE {bid_table} (
               auction BIGINT PRIMARY KEY,
               bidder BIGINT NOT NULL,
               price BIGINT NOT NULL,
               channel TEXT,
               url TEXT,
               date_time BIGINT NOT NULL,
               extra TEXT
             );
             CREATE PUBLICATION {publication} FOR TABLE {auction_table}, {bid_table};"
        ))
        .await
        .context("prepare shared-source cdc tables")?;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;

    let config = json!({
        "connectors": [
            {
                "type": "postgres_cdc",
                "connection": dsn,
                "slot": slot,
                "publication": publication,
                "include_tables": [auction_table, bid_table]
            }
        ]
    });
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)
        .context("write shared-source cdc config")?;

    let sql = format!(
        "CREATE TABLE {auction_table} (
            id BIGINT PRIMARY KEY,
            seller BIGINT NOT NULL,
            category BIGINT NOT NULL,
            initial_bid BIGINT NOT NULL,
            reserve BIGINT NOT NULL,
            item_name TEXT,
            description TEXT,
            expires BIGINT NOT NULL,
            date_time BIGINT NOT NULL,
            extra TEXT
         );
         CREATE TABLE {bid_table} (
            auction BIGINT PRIMARY KEY,
            bidder BIGINT NOT NULL,
            price BIGINT NOT NULL,
            channel TEXT,
            url TEXT,
            date_time BIGINT NOT NULL,
            extra TEXT
         );
         CREATE MATERIALIZED VIEW IF NOT EXISTS {bid_mv} AS
         SELECT auction, bidder, price FROM {bid_table};
         CREATE MATERIALIZED VIEW IF NOT EXISTS {auction_mv} AS
         SELECT id, seller FROM {auction_table};
         CREATE MATERIALIZED VIEW IF NOT EXISTS {join_mv} AS
         SELECT b.auction, b.bidder, b.price, a.seller
         FROM {bid_table} AS b JOIN {auction_table} AS a ON b.auction = a.id"
    );
    let mut child = spawn_node(&config_path, &data_dir, pg_port, Some(&sql)).await?;

    let test_result = async {
        client
            .batch_execute(&format!(
                "BEGIN;
                 INSERT INTO {auction_table}
                   (id, seller, category, initial_bid, reserve, item_name, description, expires, date_time, extra)
                   VALUES (21, 9001, 17, 100, 500, 'item', 'description', 1700100021, 1700000021, 'auction');
                 INSERT INTO {bid_table}
                   (auction, bidder, price, channel, url, date_time, extra)
                   VALUES (21, 42, 650, 'web', 'http://example.com', 1700000021, 'bid');
                 COMMIT;"
            ))
            .await
            .context("commit shared-source cdc transaction")?;
        wait_for_mv_price_count_at_least(pg_port, &bid_mv, 21, 650, 1).await?;
        wait_for_auction_seller_count_at_least(pg_port, &auction_mv, 21, 9001, 1).await?;
        wait_for_join_mv_count_at_least(pg_port, &join_mv, 21, 42, 9001, 1).await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    stop_child(&mut child, "INT").await;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;
    let _ = client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {publication};"))
        .await;
    let _ = client
        .batch_execute(&format!("DROP TABLE IF EXISTS {bid_table};"))
        .await;
    let _ = client
        .batch_execute(&format!("DROP TABLE IF EXISTS {auction_table};"))
        .await;
    test_result
}

#[tokio::test]
#[ignore = "requires native logical-replication Postgres; set FLOE_ACCEPTANCE_PG_DSN"]
#[serial_test::serial(postgres_cdc_acceptance)]
async fn postgres_cdc_shared_source_snapshot_converges_to_wal_stream() -> Result<()> {
    let dsn = std::env::var("FLOE_ACCEPTANCE_PG_DSN")
        .context("set FLOE_ACCEPTANCE_PG_DSN for CDC acceptance")?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let pg_port = find_unused_port()?;
    let admin_port = find_unused_port()?;
    let bid_table = format!("nexmark_bid_shared_snapshot_{run_id}");
    let auction_table = format!("nexmark_auction_shared_snapshot_{run_id}");
    let source_name = format!("pg_shared_snapshot_{run_id}");
    let join_mv = format!("mv_floe_cdc_shared_snapshot_join_{run_id}");
    let slot = format!("floe_acceptance_shared_snapshot_{run_id}");
    let publication = format!("floe_acceptance_shared_snapshot_pub_{run_id}");
    let temp_dir = TempDir::new().context("create temp dir")?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("empty.json");
    std::fs::write(&config_path, "{}").context("write empty config")?;

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to postgres for shared-source snapshot setup")?;
    let _connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres shared-source snapshot setup connection closed");
        }
    });

    client
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {publication};
             DROP TABLE IF EXISTS {bid_table};
             DROP TABLE IF EXISTS {auction_table};
             CREATE TABLE {auction_table} (
               id BIGINT PRIMARY KEY,
               seller BIGINT NOT NULL,
               category BIGINT NOT NULL,
               initial_bid BIGINT NOT NULL,
               reserve BIGINT NOT NULL,
               item_name TEXT,
               description TEXT,
               expires BIGINT NOT NULL,
               date_time BIGINT NOT NULL,
               extra TEXT
             );
             CREATE TABLE {bid_table} (
               auction BIGINT PRIMARY KEY,
               bidder BIGINT NOT NULL,
               price BIGINT NOT NULL,
               channel TEXT,
               url TEXT,
               date_time BIGINT NOT NULL,
               extra TEXT
             );
             INSERT INTO {auction_table}
               (id, seller, category, initial_bid, reserve, item_name, description, expires, date_time, extra)
               VALUES (81, 9801, 17, 100, 500, 'snapshot item', 'description', 1700100081, 1700000081, 'auction_snapshot');
             INSERT INTO {bid_table}
               (auction, bidder, price, channel, url, date_time, extra)
               VALUES (81, 781, 881, 'web', 'http://example.com', 1700000081, 'bid_snapshot');"
        ))
        .await
        .context("prepare shared-source snapshot cdc tables")?;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;

    let sql = format!(
        "CREATE SOURCE {source_name} WITH (
            connector = 'postgres-cdc',
            connection = '{dsn}',
            slot.name = '{slot}',
            publication.name = '{publication}'
         );
         CREATE TABLE {auction_table} (
            id BIGINT PRIMARY KEY,
            seller BIGINT NOT NULL,
            category BIGINT NOT NULL,
            initial_bid BIGINT NOT NULL,
            reserve BIGINT NOT NULL,
            item_name TEXT,
            description TEXT,
            expires BIGINT NOT NULL,
            date_time BIGINT NOT NULL,
            extra TEXT
         ) FROM {source_name} TABLE 'public.{auction_table}';
         CREATE TABLE {bid_table} (
            auction BIGINT PRIMARY KEY,
            bidder BIGINT NOT NULL,
            price BIGINT NOT NULL,
            channel TEXT,
            url TEXT,
            date_time BIGINT NOT NULL,
            extra TEXT
         ) FROM {source_name} TABLE 'public.{bid_table}';
         CREATE MATERIALIZED VIEW IF NOT EXISTS {join_mv} AS
         SELECT b.auction, b.bidder, b.price, a.seller
         FROM {bid_table} AS b JOIN {auction_table} AS a ON b.auction = a.id"
    );
    let admin_port_arg = admin_port.to_string();
    let admin_args = ["--admin-port", admin_port_arg.as_str()];
    let mut child =
        spawn_node_with_args(&config_path, &data_dir, pg_port, Some(&sql), &admin_args).await?;

    let test_result = async {
        wait_for_join_mv_count_at_least(pg_port, &join_mv, 81, 781, 9801, 1).await?;
        wait_for_admin_metrics_contains(
            admin_port,
            &format!(
                "floe_postgres_cdc_source_lag_bytes{{slot=\"{slot}\",source=\"{source_name}\""
            ),
        )
        .await?;
        wait_for_admin_metrics_contains(
            admin_port,
            &format!(
                "floe_postgres_cdc_table_last_applied_lsn{{slot=\"{slot}\",source=\"{source_name}\",table=\"{bid_table}\""
            ),
        )
        .await?;

        client
            .batch_execute(&format!(
                "BEGIN;
                 INSERT INTO {auction_table}
                   (id, seller, category, initial_bid, reserve, item_name, description, expires, date_time, extra)
                   VALUES (82, 9802, 17, 100, 500, 'wal item', 'description', 1700100082, 1700000082, 'auction_wal');
                 INSERT INTO {bid_table}
                   (auction, bidder, price, channel, url, date_time, extra)
                   VALUES (82, 782, 882, 'web', 'http://example.com', 1700000082, 'bid_wal');
                 COMMIT;"
            ))
            .await
            .context("commit shared-source cdc transaction after snapshot")?;
        wait_for_join_mv_count_at_least(pg_port, &join_mv, 82, 782, 9802, 1).await?;
        wait_for_admin_metrics_contains(
            admin_port,
            &format!(
                "floe_postgres_cdc_table_lag_bytes{{slot=\"{slot}\",source=\"{source_name}\",table=\"{auction_table}\""
            ),
        )
        .await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    stop_child(&mut child, "INT").await;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;
    let _ = client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {publication};"))
        .await;
    let _ = client
        .batch_execute(&format!("DROP TABLE IF EXISTS {bid_table};"))
        .await;
    let _ = client
        .batch_execute(&format!("DROP TABLE IF EXISTS {auction_table};"))
        .await;
    test_result
}
