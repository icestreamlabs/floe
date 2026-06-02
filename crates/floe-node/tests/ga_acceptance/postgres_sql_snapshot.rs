use super::*;

#[tokio::test]
#[ignore = "requires native logical-replication Postgres; set FLOE_ACCEPTANCE_PG_DSN"]
#[serial_test::serial(postgres_cdc_acceptance)]
async fn postgres_cdc_sql_source_table_mv_acceptance() -> Result<()> {
    let dsn = std::env::var("FLOE_ACCEPTANCE_PG_DSN")
        .context("set FLOE_ACCEPTANCE_PG_DSN for CDC acceptance")?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let pg_port = find_unused_port()?;
    let table = "nexmark_bid".to_string();
    let source_name = format!("pg_sql_{run_id}");
    let mv_name = format!("mv_floe_cdc_sql_{run_id}");
    let slot = format!("floe_acceptance_sql_{run_id}");
    let publication = format!("floe_acceptance_sql_pub_{run_id}");
    let temp_dir = TempDir::new().context("create temp dir")?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("empty.json");
    std::fs::write(&config_path, "{}").context("write empty config")?;

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to postgres for SQL CDC acceptance setup")?;
    let _connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres SQL CDC acceptance setup connection closed");
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
        .context("prepare SQL CDC acceptance table")?;
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
         CREATE TABLE {table} (
            auction BIGINT PRIMARY KEY,
            bidder BIGINT NOT NULL,
            price BIGINT NOT NULL,
            channel TEXT,
            url TEXT,
            date_time BIGINT NOT NULL,
            extra TEXT
         ) FROM {source_name} TABLE 'public.{table}';
         CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name} AS
         SELECT auction, bidder, price FROM {table}"
    );
    let mut child = spawn_node(&config_path, &data_dir, pg_port, Some(&sql)).await?;

    let test_result = async {
        sleep(Duration::from_millis(500)).await;
        client
            .execute(
                &format!(
                    "INSERT INTO {table} \
                     (auction, bidder, price, channel, url, date_time, extra) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7)"
                ),
                &[
                    &41_i64,
                    &910_i64,
                    &123_i64,
                    &"web",
                    &"http://example.com",
                    &1_700_000_041_i64,
                    &"sql_surface",
                ],
            )
            .await
            .context("insert SQL CDC row")?;
        wait_for_mv_price_count_at_least(pg_port, &mv_name, 41, 123, 1).await?;
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
        .batch_execute(&format!("DROP TABLE IF EXISTS {table};"))
        .await;
    test_result
}

#[tokio::test]
#[ignore = "requires native logical-replication Postgres; set FLOE_ACCEPTANCE_PG_DSN"]
#[serial_test::serial(postgres_cdc_acceptance)]
async fn postgres_cdc_auto_creates_publication_and_slot_acceptance() -> Result<()> {
    let dsn = std::env::var("FLOE_ACCEPTANCE_PG_DSN")
        .context("set FLOE_ACCEPTANCE_PG_DSN for CDC acceptance")?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let pg_port = find_unused_port()?;
    let table = format!("nexmark_bid_auto_setup_{run_id}");
    let source_name = format!("pg_auto_setup_{run_id}");
    let mv_name = format!("mv_floe_cdc_auto_setup_{run_id}");
    let slot = format!("floe_acceptance_auto_setup_{run_id}");
    let publication = format!("floe_acceptance_auto_setup_pub_{run_id}");
    let temp_dir = TempDir::new().context("create temp dir")?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("empty.json");
    std::fs::write(&config_path, "{}").context("write empty config")?;

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to postgres for SQL CDC auto setup acceptance")?;
    let _connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres SQL CDC auto setup connection closed");
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
             );"
        ))
        .await
        .context("prepare SQL CDC auto setup table")?;
    let _ = client
        .query("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;

    let sql = format!(
        "CREATE SOURCE {source_name} WITH (
            connector = 'postgres-cdc',
            connection = '{dsn}',
            slot.name = '{slot}',
            publication.name = '{publication}',
            slot.create = true,
            publication.create = true
         );
         CREATE TABLE {table} (
            auction BIGINT PRIMARY KEY,
            bidder BIGINT NOT NULL,
            price BIGINT NOT NULL,
            channel TEXT,
            url TEXT,
            date_time BIGINT NOT NULL,
            extra TEXT
         ) FROM {source_name} TABLE 'public.{table}';
         CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name} AS
         SELECT auction, bidder, price FROM {table}"
    );
    let mut child = spawn_node(&config_path, &data_dir, pg_port, Some(&sql)).await?;

    let test_result = async {
        sleep(Duration::from_millis(500)).await;
        client
            .execute(
                &format!(
                    "INSERT INTO {table} \
                     (auction, bidder, price, channel, url, date_time, extra) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7)"
                ),
                &[
                    &51_i64,
                    &951_i64,
                    &151_i64,
                    &"web",
                    &"http://example.com",
                    &1_700_000_051_i64,
                    &"auto_setup",
                ],
            )
            .await
            .context("insert SQL CDC auto setup row")?;
        wait_for_mv_price_count_at_least(pg_port, &mv_name, 51, 151, 1).await?;

        let publication_exists: bool = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
                &[&publication],
            )
            .await
            .context("check auto-created publication")?
            .get(0);
        let slot_plugin = client
            .query_opt(
                "SELECT plugin FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .context("check auto-created slot")?
            .and_then(|row| row.get::<_, Option<String>>(0));

        ensure!(publication_exists, "publication was not auto-created");
        ensure!(
            slot_plugin.as_deref() == Some("pgoutput"),
            "slot was not auto-created with pgoutput"
        );
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
        .batch_execute(&format!("DROP TABLE IF EXISTS {table};"))
        .await;
    test_result
}

#[tokio::test]
#[ignore = "requires native logical-replication Postgres; set FLOE_ACCEPTANCE_PG_DSN"]
#[serial_test::serial(postgres_cdc_acceptance)]
async fn postgres_cdc_sql_source_table_snapshot_backfill_acceptance() -> Result<()> {
    let dsn = std::env::var("FLOE_ACCEPTANCE_PG_DSN")
        .context("set FLOE_ACCEPTANCE_PG_DSN for CDC acceptance")?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let pg_port = find_unused_port()?;
    let table = format!("nexmark_bid_snapshot_{run_id}");
    let source_name = format!("pg_sql_snapshot_{run_id}");
    let mv_name = format!("mv_floe_cdc_sql_snapshot_{run_id}");
    let slot = format!("floe_acceptance_snapshot_{run_id}");
    let publication = format!("floe_acceptance_snapshot_pub_{run_id}");
    let temp_dir = TempDir::new().context("create temp dir")?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("empty.json");
    std::fs::write(&config_path, "{}").context("write empty config")?;

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to postgres for SQL CDC snapshot setup")?;
    let _connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres SQL CDC snapshot setup connection closed");
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
             INSERT INTO {table}
               (auction, bidder, price, channel, url, date_time, extra)
               VALUES (71, 971, 701, 'web', 'http://example.com', 1700000071, 'snapshot');"
        ))
        .await
        .context("prepare SQL CDC snapshot acceptance table")?;
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
         CREATE TABLE {table} (
            auction BIGINT PRIMARY KEY,
            bidder BIGINT NOT NULL,
            price BIGINT NOT NULL,
            channel TEXT,
            url TEXT,
            date_time BIGINT NOT NULL,
            extra TEXT
         ) FROM {source_name} TABLE 'public.{table}';
         CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name} AS
         SELECT auction, bidder, price FROM {table}"
    );
    let mut child = spawn_node(&config_path, &data_dir, pg_port, Some(&sql)).await?;

    let test_result = async {
        wait_for_mv_price_count_at_least(pg_port, &mv_name, 71, 701, 1).await?;

        client
            .execute(
                &format!(
                    "INSERT INTO {table} \
                     (auction, bidder, price, channel, url, date_time, extra) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7)"
                ),
                &[
                    &72_i64,
                    &972_i64,
                    &702_i64,
                    &"web",
                    &"http://example.com",
                    &1_700_000_072_i64,
                    &"wal_after_snapshot",
                ],
            )
            .await
            .context("insert SQL CDC row after snapshot")?;
        wait_for_mv_price_count_at_least(pg_port, &mv_name, 72, 702, 1).await?;

        client
            .execute(
                &format!("UPDATE {table} SET price = $1, extra = $2 WHERE auction = $3"),
                &[&731_i64, &"snapshot_updated", &71_i64],
            )
            .await
            .context("update SQL CDC snapshot row")?;
        wait_for_mv_price_count_at_least(pg_port, &mv_name, 71, 731, 1).await?;
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
        .batch_execute(&format!("DROP TABLE IF EXISTS {table};"))
        .await;
    test_result
}

#[tokio::test]
#[ignore = "requires native logical-replication Postgres; set FLOE_ACCEPTANCE_PG_DSN"]
#[serial_test::serial(postgres_cdc_acceptance)]
async fn postgres_cdc_table_aggregate_update_delete_acceptance() -> Result<()> {
    let dsn = std::env::var("FLOE_ACCEPTANCE_PG_DSN")
        .context("set FLOE_ACCEPTANCE_PG_DSN for CDC acceptance")?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let pg_port = find_unused_port()?;
    let table = "nexmark_bid".to_string();
    let mv_name = format!("mv_floe_cdc_aggregate_{run_id}");
    let slot = format!("floe_acceptance_aggregate_{run_id}");
    let publication = format!("floe_acceptance_aggregate_pub_{run_id}");
    let temp_dir = TempDir::new().context("create temp dir")?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("cdc_aggregate_acceptance.json");

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to postgres for aggregate cdc setup")?;
    let _connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres aggregate cdc setup connection closed");
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
        .context("prepare aggregate cdc table")?;
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
        .context("write aggregate cdc config")?;

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
         SELECT bidder, COUNT(*) AS bid_count, SUM(price) AS total_price
         FROM {table}
         GROUP BY bidder"
    );
    let mut child = spawn_node(&config_path, &data_dir, pg_port, Some(&sql)).await?;

    let test_result = async {
        sleep(Duration::from_millis(500)).await;
        client
            .batch_execute(&format!(
                "BEGIN;
                 INSERT INTO {table}
                   (auction, bidder, price, channel, url, date_time, extra)
                   VALUES (31, 900, 100, 'web', 'http://example.com', 1700000031, 'first');
                 INSERT INTO {table}
                   (auction, bidder, price, channel, url, date_time, extra)
                   VALUES (32, 900, 200, 'web', 'http://example.com', 1700000032, 'second');
                 COMMIT;"
            ))
            .await
            .context("commit aggregate cdc inserts")?;
        wait_for_bidder_aggregate(pg_port, &mv_name, 900, 2, 300).await?;

        client
            .execute(
                &format!("UPDATE {table} SET price = $1, extra = $2 WHERE auction = $3"),
                &[&150_i64, &"updated", &31_i64],
            )
            .await
            .context("update aggregate cdc row")?;
        wait_for_bidder_aggregate(pg_port, &mv_name, 900, 2, 350).await?;

        client
            .execute(
                &format!("DELETE FROM {table} WHERE auction = $1"),
                &[&32_i64],
            )
            .await
            .context("delete aggregate cdc row")?;
        wait_for_bidder_aggregate(pg_port, &mv_name, 900, 1, 150).await?;
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
        .batch_execute(&format!("DROP TABLE IF EXISTS {table};"))
        .await;
    test_result
}
