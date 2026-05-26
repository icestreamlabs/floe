use std::str;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail, ensure};
use floe_cdc_pg::{PostgresCdcConfig, PostgresLsn};
use tokio::sync::watch;
use tokio_postgres::config::Host;

const DEFAULT_POSTGRES_PUBLICATION: &str = "floe_publication";

#[derive(Debug, Clone)]
pub struct PostgresCdcSourceConfig {
    pub connection_string: String,
    pub slot: String,
    pub publication: String,
    pub auto_create_slot: bool,
    pub auto_create_publication: bool,
    pub commit_lsn_rx: Option<watch::Receiver<PostgresCdcCommit>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresSlotCommit {
    pub slot: String,
    pub lsn: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PostgresCdcCommit {
    pub tick_id: u64,
    pub slots: Vec<PostgresSlotCommit>,
}

impl PostgresCdcSourceConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.connection_string.trim().is_empty(),
            "postgres connection string must not be empty"
        );
        ensure!(
            !self.slot.trim().is_empty(),
            "postgres slot must not be empty"
        );
        ensure!(
            !self.publication.trim().is_empty(),
            "postgres publication must not be empty"
        );
        Ok(())
    }
}

pub async fn stored_slot_start_lsn(connection_string: &str, slot: &str) -> Result<PostgresLsn> {
    let (client, connection) = tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
        .await
        .context("connect Postgres control plane for native CDC")?;
    let connection_task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::debug!(error = %err, "Postgres native CDC control connection closed");
        }
    });

    let row = client
        .query_opt(
            "SELECT confirmed_flush_lsn::text, restart_lsn::text
             FROM pg_replication_slots
             WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .context("query pg_replication_slots for native CDC start LSN")?
        .ok_or_else(|| {
            anyhow!(
                "Postgres logical replication slot '{slot}' does not exist; create it with pg_create_logical_replication_slot(..., 'pgoutput')"
            )
        })?;
    let confirmed: Option<String> = row.get(0);
    let restart: Option<String> = row.get(1);
    drop(client);
    connection_task.abort();

    let lsn = confirmed
        .or(restart)
        .ok_or_else(|| anyhow!("Postgres logical replication slot '{slot}' has no start LSN"))?;
    PostgresLsn::parse(&lsn)
}

pub fn replication_config_from_connection_string(
    connection_string: &str,
    slot: &str,
    publication: &str,
    start_lsn: PostgresLsn,
) -> Result<PostgresCdcConfig> {
    let config = connection_string
        .parse::<tokio_postgres::Config>()
        .with_context(|| format!("parse Postgres connection string '{connection_string}'"))?;
    let host = match config.get_hosts().first() {
        Some(Host::Tcp(host)) => host.clone(),
        Some(Host::Unix(_)) => bail!("native Postgres CDC requires a TCP host"),
        None => "localhost".to_string(),
    };
    let port = config.get_ports().first().copied().unwrap_or(5432);
    let user = config
        .get_user()
        .ok_or_else(|| anyhow!("native Postgres CDC connection string must include user"))?
        .to_string();
    let database = config
        .get_dbname()
        .map(str::to_string)
        .unwrap_or_else(|| user.clone());
    let password = config
        .get_password()
        .map(str::from_utf8)
        .transpose()
        .context("native Postgres CDC password must be valid UTF-8")?
        .unwrap_or_default()
        .to_string();

    PostgresCdcConfig::new(host, user, password, database, slot, publication)?
        .with_port(port)?
        .with_start_lsn(start_lsn)
        .with_status_interval(Duration::from_millis(100))
}

pub fn default_postgres_publication() -> String {
    DEFAULT_POSTGRES_PUBLICATION.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> PostgresCdcSourceConfig {
        PostgresCdcSourceConfig {
            connection_string: "postgres://floe:secret@localhost:5432/postgres".to_string(),
            slot: "floe_slot".to_string(),
            publication: DEFAULT_POSTGRES_PUBLICATION.to_string(),
            auto_create_slot: true,
            auto_create_publication: true,
            commit_lsn_rx: None,
        }
    }

    #[test]
    fn source_config_validates_required_fields() {
        let mut config = base_config();
        config.connection_string = " ".to_string();
        assert!(config.validate().is_err());

        config = base_config();
        config.slot = " ".to_string();
        assert!(config.validate().is_err());

        config = base_config();
        config.publication = " ".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn parses_replication_config_from_postgres_url() {
        let config = replication_config_from_connection_string(
            "postgres://floe:secret@127.0.0.1:55432/app",
            "slot",
            "publication",
            PostgresLsn::from_u64(0x50),
        )
        .expect("parse config");

        assert_eq!(config.host(), "127.0.0.1");
        assert_eq!(config.port(), 55432);
        assert_eq!(config.user(), "floe");
        assert_eq!(config.password(), "secret");
        assert_eq!(config.database(), "app");
        assert_eq!(config.slot(), "slot");
        assert_eq!(config.publication(), "publication");
        assert_eq!(config.start_lsn(), Some(PostgresLsn::from_u64(0x50)));
    }
}
