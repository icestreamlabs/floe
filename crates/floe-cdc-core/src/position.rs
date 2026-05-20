use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CdcSourcePosition {
    Postgres {
        commit_lsn: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_lsn: Option<String>,
    },
    Opaque {
        value: String,
    },
}

impl CdcSourcePosition {
    pub fn postgres(commit_lsn: impl Into<String>, event_lsn: Option<String>) -> Result<Self> {
        let commit_lsn = commit_lsn.into();
        ensure!(
            !commit_lsn.trim().is_empty(),
            "Postgres CDC commit LSN cannot be empty"
        );
        if let Some(event_lsn) = event_lsn.as_deref() {
            ensure!(
                !event_lsn.trim().is_empty(),
                "Postgres CDC event LSN cannot be empty"
            );
        }
        Ok(Self::Postgres {
            commit_lsn,
            event_lsn,
        })
    }

    pub fn opaque(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        ensure!(
            !value.trim().is_empty(),
            "CDC source position cannot be empty"
        );
        Ok(Self::Opaque { value })
    }

    pub fn covers(&self, other: &Self) -> Result<bool> {
        match (self, other) {
            (
                Self::Postgres {
                    commit_lsn,
                    event_lsn,
                },
                Self::Postgres {
                    commit_lsn: other_commit_lsn,
                    event_lsn: other_event_lsn,
                },
            ) => postgres_position_covers(
                commit_lsn,
                event_lsn.as_deref(),
                other_commit_lsn,
                other_event_lsn.as_deref(),
            ),
            (Self::Opaque { value }, Self::Opaque { value: other }) => Ok(value == other),
            _ => bail!(
                "cannot compare CDC source positions from different position kinds: {:?} and {:?}",
                self,
                other
            ),
        }
    }
}

fn postgres_position_covers(
    commit_lsn: &str,
    event_lsn: Option<&str>,
    other_commit_lsn: &str,
    other_event_lsn: Option<&str>,
) -> Result<bool> {
    let commit_lsn = parse_postgres_lsn(commit_lsn)?;
    let other_commit_lsn = parse_postgres_lsn(other_commit_lsn)?;
    if commit_lsn != other_commit_lsn {
        return Ok(commit_lsn > other_commit_lsn);
    }

    match (event_lsn, other_event_lsn) {
        (None, _) => Ok(true),
        (Some(_), None) => Ok(false),
        (Some(event_lsn), Some(other_event_lsn)) => {
            Ok(parse_postgres_lsn(event_lsn)? >= parse_postgres_lsn(other_event_lsn)?)
        }
    }
}

fn parse_postgres_lsn(value: &str) -> Result<u64> {
    let (high, low) = value
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("invalid Postgres LSN '{value}'"))?;
    let high = u64::from_str_radix(high, 16)
        .with_context(|| format!("parse high half of Postgres LSN '{value}'"))?;
    let low = u64::from_str_radix(low, 16)
        .with_context(|| format!("parse low half of Postgres LSN '{value}'"))?;
    Ok((high << 32) | low)
}
