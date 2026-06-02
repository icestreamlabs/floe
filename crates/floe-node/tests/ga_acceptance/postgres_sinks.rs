use super::*;

#[tokio::test]
#[ignore = "requires native logical-replication Postgres; set FLOE_ACCEPTANCE_PG_DSN"]
#[serial_test::serial(postgres_cdc_acceptance)]
async fn postgres_cdc_table_mv_update_delete_acceptance() -> Result<()> {
    let dsn = std::env::var("FLOE_ACCEPTANCE_PG_DSN")
        .context("set FLOE_ACCEPTANCE_PG_DSN for CDC acceptance")?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let table = "nexmark_bid".to_string();
    let mv_name = format!("mv_floe_cdc_bid_{run_id}");
    let slot = format!("floe_acceptance_native_{run_id}");
    let publication = format!("floe_acceptance_native_pub_{run_id}");
    let temp_dir = TempDir::new().context("create temp dir")?;
    let data_dir = temp_dir.path().join("data");
    let sink_path = temp_dir.path().join("cdc_native_sink.jsonl");
    let config_path = temp_dir.path().join("cdc_native_file_sink_acceptance.json");

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to postgres for native cdc acceptance setup")?;
    let _connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres native cdc acceptance setup connection closed");
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
        .context("prepare native cdc acceptance table")?;
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
        ],
        "sinks": [
            {
                "type": "file",
                "mv": mv_name,
                "path": sink_path,
                "with_snapshot": true,
                "append": true
            }
        ]
    });
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)
        .context("write native cdc acceptance config")?;

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
         CREATE MATERIALIZED VIEW {mv_name} AS SELECT auction, bidder, price FROM {table}"
    );
    let mut child = spawn_node_with_args(&config_path, &data_dir, 0, Some(&sql), &[]).await?;

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
                    &1_i64,
                    &10_i64,
                    &100_i64,
                    &"web",
                    &"http://example.com",
                    &1_700_000_001_i64,
                    &"open",
                ],
            )
            .await
            .context("insert native cdc row")?;
        wait_for_rows_matching(&sink_path, |value| {
            value.get("__op").and_then(Value::as_i64) == Some(1)
                && value.get("auction").and_then(Value::as_i64) == Some(1)
                && value.get("bidder").and_then(Value::as_i64) == Some(10)
                && value.get("price").and_then(Value::as_i64) == Some(100)
        })
        .await?;

        client
            .execute(
                &format!("UPDATE {table} SET price = $1, extra = $2 WHERE auction = $3"),
                &[&150_i64, &"paid", &1_i64],
            )
            .await
            .context("update native cdc row")?;
        wait_for_rows_matching(&sink_path, |value| {
            value.get("__op").and_then(Value::as_i64) == Some(-1)
                && value.get("auction").and_then(Value::as_i64) == Some(1)
                && value.get("price").and_then(Value::as_i64) == Some(100)
        })
        .await?;
        wait_for_rows_matching(&sink_path, |value| {
            value.get("__op").and_then(Value::as_i64) == Some(1)
                && value.get("auction").and_then(Value::as_i64) == Some(1)
                && value.get("price").and_then(Value::as_i64) == Some(150)
        })
        .await?;

        client
            .execute(
                &format!("DELETE FROM {table} WHERE auction = $1"),
                &[&1_i64],
            )
            .await
            .context("delete native cdc row")?;
        wait_for_rows_matching(&sink_path, |value| {
            value.get("__op").and_then(Value::as_i64) == Some(-1)
                && value.get("auction").and_then(Value::as_i64) == Some(1)
                && value.get("price").and_then(Value::as_i64) == Some(150)
        })
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
        .batch_execute(&format!("DROP TABLE IF EXISTS {table};"))
        .await;
    test_result
}

#[tokio::test]
#[ignore = "requires native logical-replication Postgres; set FLOE_ACCEPTANCE_PG_DSN"]
#[serial_test::serial(postgres_cdc_acceptance)]
async fn postgres_cdc_mv_to_postgres_sink_acceptance() -> Result<()> {
    let dsn = std::env::var("FLOE_ACCEPTANCE_PG_DSN")
        .context("set FLOE_ACCEPTANCE_PG_DSN for CDC acceptance")?;
    let escaped_dsn = sql_string_literal(&dsn);
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let source_name = format!("pg_sink_source_{run_id}");
    let source_table = format!("floe_mv_sink_orders_{run_id}");
    let target_table = format!("floe_mv_sink_target_{run_id}");
    let mv_name = format!("mv_pg_sink_{run_id}");
    let sink_name = format!("pg_sink_{run_id}");
    let slot = format!("floe_mv_sink_{run_id}");
    let publication = format!("floe_mv_sink_pub_{run_id}");
    let temp_dir = TempDir::new().context("create temp dir")?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir.path().join("postgres_mv_sink_acceptance.json");
    std::fs::write(&config_path, "{}").context("write empty acceptance config")?;

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to postgres for MV sink acceptance setup")?;
    let _connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres MV sink acceptance setup connection closed");
        }
    });

    cleanup_postgres_sink_acceptance(&client, &publication, &slot, &source_table, &target_table)
        .await;
    client
        .batch_execute(&format!(
            "CREATE TABLE {source_table} (
               id BIGINT PRIMARY KEY,
               amount BIGINT NOT NULL,
               note TEXT
             );
             CREATE TABLE {target_table} (
               id BIGINT PRIMARY KEY,
               amount BIGINT NOT NULL,
               note TEXT
             );
             CREATE PUBLICATION {publication} FOR TABLE {source_table};"
        ))
        .await
        .context("prepare Postgres MV sink acceptance tables")?;

    let sql = format!(
        "CREATE SOURCE {source_name} WITH (
            connector = 'postgres-cdc',
            connection = '{escaped_dsn}',
            slot.name = '{slot}',
            publication.name = '{publication}'
         );
         CREATE TABLE {source_table} (
            id BIGINT PRIMARY KEY,
            amount BIGINT NOT NULL,
            note TEXT
         ) FROM {source_name} TABLE 'public.{source_table}';
         CREATE MATERIALIZED VIEW {mv_name} AS
         SELECT id, amount, note FROM {source_table};
         CREATE SINK {sink_name} FROM {mv_name} WITH (
            connector = 'postgres',
            connection = '{escaped_dsn}',
            table = 'public.{target_table}',
            mode = 'upsert',
            primary_key = 'id',
            with_snapshot = true
         );"
    );
    let mut child = spawn_node_with_args(&config_path, &data_dir, 0, Some(&sql), &[]).await?;

    let test_result = async {
        sleep(Duration::from_millis(500)).await;
        client
            .execute(
                &format!("INSERT INTO {source_table} (id, amount, note) VALUES ($1, $2, $3)"),
                &[&1_i64, &100_i64, &"open"],
            )
            .await
            .context("insert source row for Postgres MV sink")?;
        wait_for_postgres_sink_row(&client, &target_table, 1, 100, Some("open")).await?;

        client
            .execute(
                &format!("UPDATE {source_table} SET amount = $1, note = $2 WHERE id = $3"),
                &[&175_i64, &"paid", &1_i64],
            )
            .await
            .context("update source row for Postgres MV sink")?;
        wait_for_postgres_sink_row(&client, &target_table, 1, 175, Some("paid")).await?;

        client
            .execute(
                &format!("DELETE FROM {source_table} WHERE id = $1"),
                &[&1_i64],
            )
            .await
            .context("delete source row for Postgres MV sink")?;
        wait_for_postgres_sink_absent(&client, &target_table, 1).await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    stop_child(&mut child, "INT").await;
    cleanup_postgres_sink_acceptance(&client, &publication, &slot, &source_table, &target_table)
        .await;
    test_result
}

#[tokio::test]
#[ignore = "requires native logical-replication Postgres; set FLOE_ACCEPTANCE_PG_DSN"]
#[serial_test::serial(postgres_cdc_acceptance)]
async fn postgres_cdc_replication_pipeline_to_postgres_acceptance() -> Result<()> {
    let dsn = std::env::var("FLOE_ACCEPTANCE_PG_DSN")
        .context("set FLOE_ACCEPTANCE_PG_DSN for CDC acceptance")?;
    let escaped_dsn = sql_string_literal(&dsn);
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let source_name = format!("pg_repl_source_{run_id}");
    let source_table = format!("floe_repl_orders_{run_id}");
    let target_table = format!("floe_repl_target_{run_id}");
    let pipeline_name = format!("pg_repl_to_pg_{run_id}");
    let slot = format!("floe_repl_pg_{run_id}");
    let publication = format!("floe_repl_pg_pub_{run_id}");
    let temp_dir = TempDir::new().context("create temp dir")?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir
        .path()
        .join("postgres_replication_pipeline_acceptance.json");
    std::fs::write(&config_path, "{}").context("write empty acceptance config")?;

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to postgres for replication pipeline target setup")?;
    let _connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(
                error = %err,
                "postgres replication pipeline target setup connection closed"
            );
        }
    });

    cleanup_postgres_sink_acceptance(&client, &publication, &slot, &source_table, &target_table)
        .await;
    client
        .batch_execute(&format!(
            "CREATE TABLE {source_table} (
               id BIGINT PRIMARY KEY,
               amount BIGINT NOT NULL,
               note TEXT
             );
             CREATE TABLE {target_table} (
               id BIGINT PRIMARY KEY,
               amount BIGINT NOT NULL,
               note TEXT
             );
             CREATE PUBLICATION {publication} FOR TABLE {source_table};"
        ))
        .await
        .context("prepare Postgres replication pipeline target tables")?;

    let sql = format!(
        "CREATE SOURCE {source_name} WITH (
            connector = 'postgres-cdc',
            connection = '{escaped_dsn}',
            slot.name = '{slot}',
            publication.name = '{publication}'
         );
         CREATE REPLICATION PIPELINE {pipeline_name}
         FROM {source_name} TABLE public.{source_table}
         INTO POSTGRES WITH (
            connection = '{escaped_dsn}',
            table = 'public.{target_table}',
            format = 'floe-json',
            durable_buffer = true
         );"
    );
    let mut child = spawn_node_with_args(&config_path, &data_dir, 0, Some(&sql), &[]).await?;

    let test_result = async {
        sleep(Duration::from_millis(500)).await;
        client
            .execute(
                &format!("INSERT INTO {source_table} (id, amount, note) VALUES ($1, $2, $3)"),
                &[&1_i64, &100_i64, &"open"],
            )
            .await
            .context("insert source row for Postgres replication pipeline")?;
        wait_for_postgres_sink_row(&client, &target_table, 1, 100, Some("open")).await?;

        client
            .execute(
                &format!("UPDATE {source_table} SET amount = $1, note = $2 WHERE id = $3"),
                &[&175_i64, &Option::<&str>::None, &1_i64],
            )
            .await
            .context("update source row for Postgres replication pipeline")?;
        wait_for_postgres_sink_row(&client, &target_table, 1, 175, None).await?;

        client
            .execute(
                &format!("DELETE FROM {source_table} WHERE id = $1"),
                &[&1_i64],
            )
            .await
            .context("delete source row for Postgres replication pipeline")?;
        wait_for_postgres_sink_absent(&client, &target_table, 1).await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    stop_child(&mut child, "INT").await;
    cleanup_postgres_sink_acceptance(&client, &publication, &slot, &source_table, &target_table)
        .await;
    test_result
}

#[tokio::test]
#[ignore = "requires native logical-replication Postgres; set FLOE_ACCEPTANCE_PG_DSN"]
#[serial_test::serial(postgres_cdc_acceptance)]
async fn postgres_cdc_type_coverage_to_postgres_sink_acceptance() -> Result<()> {
    let dsn = std::env::var("FLOE_ACCEPTANCE_PG_DSN")
        .context("set FLOE_ACCEPTANCE_PG_DSN for CDC acceptance")?;
    let escaped_dsn = sql_string_literal(&dsn);
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let source_name = format!("pg_types_source_{run_id}");
    let source_table = format!("floe_mv_sink_types_source_{run_id}");
    let target_table = format!("floe_mv_sink_types_target_{run_id}");
    let mv_name = format!("mv_pg_sink_types_{run_id}");
    let sink_name = format!("pg_sink_types_{run_id}");
    let slot = format!("floe_mv_sink_types_{run_id}");
    let publication = format!("floe_mv_sink_types_pub_{run_id}");
    let temp_dir = TempDir::new().context("create temp dir")?;
    let data_dir = temp_dir.path().join("data");
    let config_path = temp_dir
        .path()
        .join("postgres_mv_sink_types_acceptance.json");
    std::fs::write(&config_path, "{}").context("write empty acceptance config")?;

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect to postgres for MV sink type acceptance setup")?;
    let _connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres MV sink type acceptance setup connection closed");
        }
    });

    cleanup_postgres_sink_acceptance(&client, &publication, &slot, &source_table, &target_table)
        .await;
    client
        .batch_execute(&format!(
            "CREATE TABLE {source_table} (
               id BIGINT PRIMARY KEY,
               active BOOLEAN NOT NULL,
               order_date DATE NOT NULL,
               amount NUMERIC(12,2) NOT NULL,
               note TEXT
             );
             CREATE TABLE {target_table} (
               id BIGINT PRIMARY KEY,
               active BOOLEAN NOT NULL,
               order_date DATE NOT NULL,
               amount NUMERIC(12,2) NOT NULL,
               note TEXT
             );
             INSERT INTO {source_table}
               (id, active, order_date, amount, note)
             VALUES
               (1, true, DATE '2024-01-02', 123.45, 'snapshot');"
        ))
        .await
        .context("prepare Postgres MV sink type acceptance tables")?;

    let sql = format!(
        "CREATE SOURCE {source_name} WITH (
            connector = 'postgres-cdc',
            connection = '{escaped_dsn}',
            slot.name = '{slot}',
            publication.name = '{publication}'
         );
         CREATE TABLE {source_table} (
            id BIGINT PRIMARY KEY,
            active BOOLEAN NOT NULL,
            order_date DATE NOT NULL,
            amount NUMERIC(12,2) NOT NULL,
            note TEXT
         ) FROM {source_name} TABLE 'public.{source_table}';
         CREATE MATERIALIZED VIEW {mv_name} AS
         SELECT id, active, order_date, amount, note FROM {source_table};
         CREATE SINK {sink_name} FROM {mv_name} WITH (
            connector = 'postgres',
            connection = '{escaped_dsn}',
            table = 'public.{target_table}',
            mode = 'upsert',
            primary_key = 'id',
            with_snapshot = true
         );"
    );
    let mut child = spawn_node_with_args(&config_path, &data_dir, 0, Some(&sql), &[]).await?;

    let test_result = async {
        wait_for_postgres_sink_typed_row(
            &client,
            &target_table,
            1,
            true,
            "2024-01-02",
            "123.45",
            Some("snapshot"),
        )
        .await?;

        client
            .batch_execute(&format!(
                "UPDATE {source_table}
                 SET active = false,
                     order_date = DATE '2024-02-03',
                     amount = 987.65,
                     note = 'live_update'
                 WHERE id = 1;"
            ))
            .await
            .context("update source row for Postgres MV sink type coverage")?;
        wait_for_postgres_sink_typed_row(
            &client,
            &target_table,
            1,
            false,
            "2024-02-03",
            "987.65",
            Some("live_update"),
        )
        .await?;

        client
            .execute(
                &format!("DELETE FROM {source_table} WHERE id = $1"),
                &[&1_i64],
            )
            .await
            .context("delete source row for Postgres MV sink type coverage")?;
        wait_for_postgres_sink_absent(&client, &target_table, 1).await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    stop_child(&mut child, "INT").await;
    cleanup_postgres_sink_acceptance(&client, &publication, &slot, &source_table, &target_table)
        .await;
    test_result
}
