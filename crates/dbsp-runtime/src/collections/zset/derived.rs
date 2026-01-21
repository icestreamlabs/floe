use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};

use super::base::ZSet;

const SELECT_PREFIX: &str = "zset_select/";
const PROJECT_PREFIX: &str = "zset_project/";
const JOIN_PREFIX: &str = "zset_join/";
const H_PREFIX: &str = "zset_h/";

static SELECT_COUNTER: AtomicU64 = AtomicU64::new(0);
static PROJECT_COUNTER: AtomicU64 = AtomicU64::new(0);
static JOIN_COUNTER: AtomicU64 = AtomicU64::new(0);
static H_COUNTER: AtomicU64 = AtomicU64::new(0);

pub async fn select<K, P>(zset: &ZSet<K>, predicate: &P) -> Result<ZSet<K>>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    P: Fn(&K) -> bool + Send + Sync,
{
    let entries = collect_entries(zset).await?;
    let namespace = derived_namespace(SELECT_PREFIX, &SELECT_COUNTER);
    let mut result = ZSet::with_table(zset.table(), namespace)
        .await
        .context("build derived ZSet for select")?;

    for (key, weight) in entries {
        if predicate(&key) {
            result.set_weight(key, weight);
        }
    }

    Ok(result)
}

pub async fn project<K, R, F>(zset: &ZSet<K>, projector: &F) -> Result<ZSet<R>>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    F: Fn(&K) -> R + Send + Sync,
{
    let mut aggregated: HashMap<R, i64> = HashMap::new();
    for (key, weight) in collect_entries(zset).await? {
        let projected = projector(&key);
        *aggregated.entry(projected).or_insert(0) += weight;
    }

    aggregated.retain(|_, weight| *weight != 0);

    let namespace = derived_namespace(PROJECT_PREFIX, &PROJECT_COUNTER);
    let mut result = ZSet::with_table(zset.table(), namespace)
        .await
        .context("build derived ZSet for project")?;
    for (key, weight) in aggregated {
        result.set_weight(key, weight);
    }

    Ok(result)
}

pub async fn join<L, R, O, P, F>(
    left: &ZSet<L>,
    right: &ZSet<R>,
    predicate: &P,
    projector: &F,
) -> Result<ZSet<O>>
where
    L: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    P: Fn(&L, &R) -> bool + Send + Sync,
    F: Fn(&L, &R) -> O + Send + Sync,
{
    let left_entries = collect_entries(left).await?;
    let right_entries = collect_entries(right).await?;

    let namespace = derived_namespace(JOIN_PREFIX, &JOIN_COUNTER);
    let mut result = ZSet::with_table(left.table(), namespace)
        .await
        .context("build derived ZSet for join")?;

    for (left_key, left_weight) in left_entries {
        for (right_key, right_weight) in &right_entries {
            if predicate(&left_key, right_key) {
                let projected = projector(&left_key, right_key);
                let combined = left_weight * *right_weight;
                result.set_weight(projected, combined);
            }
        }
    }

    result.flush().await?;
    Ok(result)
}

pub async fn h<K>(diff: &ZSet<K>, integrated_state: &ZSet<K>) -> Result<ZSet<K>>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let diff_entries = collect_entries(diff).await?;
    let integrated_entries = collect_entries(integrated_state).await?;

    let namespace = derived_namespace(H_PREFIX, &H_COUNTER);
    let mut result = ZSet::with_table(diff.table(), namespace)
        .await
        .context("build derived ZSet for H operator")?;

    for (key, diff_weight) in diff_entries {
        let state_weight = integrated_entries.get(&key).copied().unwrap_or(0);
        let coalesced = diff_weight + state_weight;

        if state_weight > 0 && coalesced <= 0 {
            result.set_weight(key, -1);
        } else if state_weight <= 0 && coalesced > 0 {
            result.set_weight(key, 1);
        }
    }

    Ok(result)
}

async fn collect_entries<K>(zset: &ZSet<K>) -> Result<HashMap<K, i64>>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let mut clone = zset.clone();
    let items = clone.items().await?;

    let mut map = HashMap::new();
    for (key, weight) in items {
        if weight != 0 {
            map.insert(key, weight);
        }
    }
    Ok(map)
}

fn derived_namespace(prefix: &str, counter: &AtomicU64) -> String {
    let id = counter.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}{id}")
}
