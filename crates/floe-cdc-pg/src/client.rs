use anyhow::{Context, Result};
use bytes::Bytes;
use pgwire_replication::{ReplicationClient, ReplicationEvent as PgWireReplicationEvent};

use crate::config::PostgresCdcConfig;
use crate::lsn::PostgresLsn;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostgresReplicationEvent {
    KeepAlive {
        wal_end: PostgresLsn,
        reply_requested: bool,
        server_time_micros: i64,
    },
    Begin {
        final_lsn: PostgresLsn,
        xid: u32,
        commit_time_micros: i64,
    },
    XLogData {
        wal_start: PostgresLsn,
        wal_end: PostgresLsn,
        server_time_micros: i64,
        data: Bytes,
    },
    Commit {
        lsn: PostgresLsn,
        end_lsn: PostgresLsn,
        commit_time_micros: i64,
    },
    Message {
        transactional: bool,
        lsn: PostgresLsn,
        prefix: String,
        content: Bytes,
    },
    StoppedAt {
        reached: PostgresLsn,
    },
}

impl From<PgWireReplicationEvent> for PostgresReplicationEvent {
    fn from(event: PgWireReplicationEvent) -> Self {
        match event {
            PgWireReplicationEvent::KeepAlive {
                wal_end,
                reply_requested,
                server_time_micros,
            } => Self::KeepAlive {
                wal_end: wal_end.into(),
                reply_requested,
                server_time_micros,
            },
            PgWireReplicationEvent::Begin {
                final_lsn,
                xid,
                commit_time_micros,
            } => Self::Begin {
                final_lsn: final_lsn.into(),
                xid,
                commit_time_micros,
            },
            PgWireReplicationEvent::XLogData {
                wal_start,
                wal_end,
                server_time_micros,
                data,
            } => Self::XLogData {
                wal_start: wal_start.into(),
                wal_end: wal_end.into(),
                server_time_micros,
                data,
            },
            PgWireReplicationEvent::Commit {
                lsn,
                end_lsn,
                commit_time_micros,
            } => Self::Commit {
                lsn: lsn.into(),
                end_lsn: end_lsn.into(),
                commit_time_micros,
            },
            PgWireReplicationEvent::Message {
                transactional,
                lsn,
                prefix,
                content,
            } => Self::Message {
                transactional,
                lsn: lsn.into(),
                prefix,
                content,
            },
            PgWireReplicationEvent::StoppedAt { reached } => Self::StoppedAt {
                reached: reached.into(),
            },
        }
    }
}

pub struct PostgresReplicationClient {
    inner: ReplicationClient,
}

impl PostgresReplicationClient {
    pub async fn connect(config: &PostgresCdcConfig) -> Result<Self> {
        let config = config.to_replication_config()?;
        let inner = ReplicationClient::connect(config)
            .await
            .context("connect Postgres logical replication client")?;
        Ok(Self { inner })
    }

    pub async fn recv(&mut self) -> Result<Option<PostgresReplicationEvent>> {
        self.inner
            .recv()
            .await
            .context("receive Postgres logical replication event")
            .map(|event| event.map(Into::into))
    }

    pub fn update_applied_lsn(&self, lsn: PostgresLsn) {
        self.inner.update_applied_lsn(lsn.into());
    }

    pub fn stop(&self) {
        self.inner.stop();
    }

    pub fn is_running(&self) -> bool {
        self.inner.is_running()
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        self.inner
            .shutdown()
            .await
            .context("shutdown Postgres logical replication client")
    }
}
