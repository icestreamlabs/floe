use std::fmt;
use std::str::FromStr;

use anyhow::{Context, Result, bail};
use floe_cdc_core::CdcSourcePosition;
use pgwire_replication::Lsn as PgWireLsn;
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize, Serializer};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PostgresLsn(u64);

impl PostgresLsn {
    pub const ZERO: Self = Self(0);

    pub fn parse(value: &str) -> Result<Self> {
        PgWireLsn::parse(value)
            .map(Self::from)
            .with_context(|| format!("parse Postgres LSN '{value}'"))
    }

    pub fn from_u64(value: u64) -> Self {
        Self(value)
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub fn to_pg_string(self) -> String {
        PgWireLsn::from(self).to_pg_string()
    }

    pub fn to_source_position(self) -> Result<CdcSourcePosition> {
        CdcSourcePosition::postgres(self.to_pg_string(), None)
    }

    pub fn from_source_position(position: &CdcSourcePosition) -> Result<Self> {
        let CdcSourcePosition::Postgres { commit_lsn, .. } = position else {
            bail!("expected Postgres CDC source position, got {position:?}");
        };
        Self::parse(commit_lsn)
    }
}

impl fmt::Display for PostgresLsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        PgWireLsn::from(*self).fmt(f)
    }
}

impl FromStr for PostgresLsn {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl From<PgWireLsn> for PostgresLsn {
    fn from(value: PgWireLsn) -> Self {
        Self(value.as_u64())
    }
}

impl From<PostgresLsn> for PgWireLsn {
    fn from(value: PostgresLsn) -> Self {
        PgWireLsn::from_u64(value.as_u64())
    }
}

impl Serialize for PostgresLsn {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_pg_string())
    }
}

impl<'de> Deserialize<'de> for PostgresLsn {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(de::Error::custom)
    }
}
