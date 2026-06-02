use super::*;

pub(in crate::node_runtime) async fn ensure_postgres_cdc_publication_and_slot(
    connection_string: &str,
    slot: &str,
    publication: &str,
    runtime_plan: &PostgresCdcRuntimePlan,
    auto_create_slot: bool,
    auto_create_publication: bool,
) -> Result<()> {
    let (client, connection) = tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
        .await
        .context("connect Postgres control plane for CDC setup")?;
    let connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::debug!(error = %err, "Postgres CDC setup connection closed");
        }
    });

    let setup_result = ensure_postgres_cdc_publication_and_slot_with_client(
        &client,
        slot,
        publication,
        runtime_plan,
        auto_create_slot,
        auto_create_publication,
    )
    .await;
    drop(client);
    connection_task.abort();
    setup_result
}

pub(super) async fn ensure_postgres_cdc_publication_and_slot_with_client(
    client: &tokio_postgres::Client,
    slot: &str,
    publication: &str,
    runtime_plan: &PostgresCdcRuntimePlan,
    auto_create_slot: bool,
    auto_create_publication: bool,
) -> Result<()> {
    validate_postgres_cdc_prerequisites(client).await?;
    validate_postgres_cdc_table_read_privileges(client, runtime_plan).await?;

    let publication_exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[&publication],
        )
        .await
        .with_context(|| format!("check Postgres CDC publication '{publication}'"))?
        .get(0);
    if !publication_exists {
        ensure!(
            auto_create_publication,
            "Postgres CDC publication '{publication}' does not exist; create it manually or set publication.create=true / auto_create_publication=true"
        );
        let schemas = sorted_snapshot_schemas(&runtime_plan.schemas);
        ensure!(
            !schemas.is_empty(),
            "cannot create Postgres CDC publication '{publication}' without tables"
        );
        let mut tables = schemas
            .iter()
            .map(|schema| qualified_table_name(schema.upstream_table()))
            .collect::<Vec<_>>();
        tables.sort();
        tables.dedup();
        client
            .batch_execute(&format!(
                "CREATE PUBLICATION {} FOR TABLE {}",
                quote_pg_ident(publication),
                tables.join(", ")
            ))
            .await
            .with_context(|| format!("create Postgres CDC publication '{publication}'"))?;
        tracing::info!(
            source = %runtime_plan.source_id.as_str(),
            publication = %publication,
            tables = tables.len(),
            "created Postgres CDC publication"
        );
    }

    match postgres_replication_slot_plugin(client, slot).await? {
        Some(plugin) => ensure!(
            plugin.as_deref() == Some("pgoutput"),
            "Postgres CDC logical replication slot '{slot}' must use pgoutput, got {:?}",
            plugin
        ),
        None => {
            ensure!(
                auto_create_slot,
                "Postgres CDC logical replication slot '{slot}' does not exist; create it manually or set slot.create=true / auto_create_slot=true"
            );
            tracing::debug!(
                source = %runtime_plan.source_id.as_str(),
                slot = %slot,
                "Postgres CDC logical replication slot is missing; initial snapshot will create it with an exported snapshot"
            );
        }
    }

    Ok(())
}

pub(super) async fn validate_postgres_cdc_prerequisites(
    client: &tokio_postgres::Client,
) -> Result<()> {
    let wal_level: String = client
        .query_one("SHOW wal_level", &[])
        .await
        .context("check Postgres wal_level for CDC setup")?
        .get(0);
    ensure!(
        wal_level.eq_ignore_ascii_case("logical"),
        "Postgres CDC requires wal_level=logical; current wal_level is '{wal_level}'"
    );

    let can_replicate: bool = client
        .query_one(
            "SELECT rolsuper OR rolreplication
             FROM pg_roles
             WHERE rolname = current_user",
            &[],
        )
        .await
        .context("check Postgres CDC user replication privilege")?
        .get(0);
    ensure!(
        can_replicate,
        "Postgres CDC user must have REPLICATION privilege or be a superuser to use logical replication slots"
    );
    Ok(())
}

pub(super) async fn validate_postgres_cdc_table_read_privileges(
    client: &tokio_postgres::Client,
    runtime_plan: &PostgresCdcRuntimePlan,
) -> Result<()> {
    for schema in sorted_snapshot_schemas(&runtime_plan.schemas) {
        let upstream = schema.upstream_table();
        let table_name = qualified_table_name(upstream);
        let can_select: bool = client
            .query_one(
                "SELECT COALESCE(has_table_privilege(to_regclass($1), 'SELECT'), false)",
                &[&table_name],
            )
            .await
            .with_context(|| {
                format!(
                    "check Postgres CDC SELECT privilege on '{}.{}'",
                    upstream.schema(),
                    upstream.table()
                )
            })?
            .get(0);
        ensure!(
            can_select,
            "Postgres CDC user must have SELECT privilege on table '{}.{}'",
            upstream.schema(),
            upstream.table()
        );
    }
    Ok(())
}
