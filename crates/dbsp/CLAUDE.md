# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## DBSP Crate Overview

This crate implements DBSP (Database Stream Processing) primitives in Rust with SlateDB persistence. It provides the foundational layer for incremental computation through differential dataflow patterns, ported from the Python `pydbsp` implementation.

## Core Principles

### Everything is an Abelian Group
- All stream values live in a group `⟨T, add, neg, identity⟩`
- Operators **never** use `+` directly—always call the group's `add/neg` methods
- This keeps all code generic (works with ZSets, LazyZSets, Streams of anything)
- The trait: `AbelianGroup<T>` in `algebra/mod.rs`

### Streams are Sparse, Piecewise-Constant Timelines
- Store explicit **non-default events** by timestamp
- Everything else is implied by a **piecewise-constant default** (tracked as "default change" breakpoints)
- Reading at time `t` returns:
  1. The event at `t` if present, else
  2. The most recent default ≤ `t`

### Freeze Tails After Lifted Ops
- After operators like lifted `Delay`/`Integrate`, "freeze" the substream by setting its default to its latest value
- Out-of-frontier reads remain causal without recomputation

### Persist What Matters for Incremental Resumption
- Persist **logical** state (e.g., ZSet weights, join indexes, accumulators)
- Circuit can resume without recomputing from scratch
- Persist full timelines only if you need replay/materialization

## Storage Model (SlateDB)

### Single DB, Namespaced Prefixes
```
zset/<ns>/<key>                                 -> i64_be_weight
stream/<ns>/data/<be_i64_ts>                    -> value
stream/<ns>/default/<be_i64_ts>                 -> default_value
stream/<ns>/meta/state                          -> { timestamp, identity, default }
index/<ns>/…                                    -> (join indexes)
```

### Key Storage Conventions
- Use **big-endian i64** for timestamps to enable ordered scans
- One `Arc<Db>` per runtime/circuit
- Namespace prefixes create logical tables

### Flushing & Crash Safety
- Prefer **batched writes** (one atomic flush for `{defaults, data, state}`)
- If no transactions: write `{defaults, data}` first, then `state` as summary
- Optional: write an **intent** record before data and clear it after `state` for robust recovery

### Separation of Concerns
- Wrap SlateDB with a small **KV/storage trait** (e.g., `get/put/delete/scan_prefix/write_batch`)
- `ZSet`/`Stream` hold a lightweight **Table handle** (`store + prefix`), not their own DB
- See `storage/mod.rs` for the trait and `storage/encoding.rs` for serialization helpers

## Data Structures

### ZSet<K> (Zero-Set)
**File:** `collections/zset.rs`

**Semantics:** Integer-weighted multiset with Abelian group operations
- `add` merges weights by key
- `neg` negates all weights
- `identity` is empty set
- Remove entries whose weight becomes **zero**

**API:**
- `get_weight(key)` - Get weight for a key
- `set_weight(key, weight)` - Set absolute weight (0 to delete)
- `add_weight(key, delta)` - Incremental update
- `contains(key)` - Check presence
- `items()` - Iterate all non-zero entries
- `is_identity()` - Check if empty
- `flush()` - Persist to SlateDB

**Storage:**
- `zset/<ns>/<key> → i64_be`
- Keep a **pending overlay** (in-memory) to read unflushed writes
- Optional: track a **nonzero-key counter** in metadata for O(1) `is_identity()`

### Stream<T> (Temporal Stream)
**File:** `stream/mod.rs`

**Fields:**
- `timestamp` (frontier)
- `identity` (true until a non-default event appears)
- `default` (current default value)
- **Pending:** `pending_data`, `pending_defaults`, `pending_state`
- **Caches:** `data_cache` (bounded/LRU), `default_changes` (breakpoints)

**API:**
- `send(T)` - Append value at new timestamp
- `set_default(T)` - Change default for future timestamps
- `get(timestamp)` - Retrieve value at specific time
- `latest()` - Get most recent value
- `to_vec()` - Export full timeline
- `flush()` - Persist pending changes
- `current_time()` - Get current timestamp
- `is_identity()` - Check if stream is empty

**Semantics:**
- `send(x)` advances time; persist only if `x != default`
- `set_default(d)` adds a breakpoint at **current** timestamp
- `get(t)` extends to `t` by sending defaults, returns event-or-default-at-`t`

**Storage Namespaces:**
- `stream/<ns>/data/<ts> → T` (non-default events)
- `stream/<ns>/default/<ts> → T` (default change breakpoints)
- `stream/<ns>/meta/state → { timestamp, identity, default }`
- Optional: `last_default_ts` in state to avoid scanning all defaults on cold start

## Operators (Shapes & Patterns)

### Linear Operators on Streams
- **Delay:** `out[t] = in[t-1]` (seed at 0)
- **Differentiate:** `out[t] = add(in[t], neg(in[t-1]))`
- **Integrate:** Running sum via feedback delay. **Lifted integrate:** compute to fixpoint, then **set default to latest** (freeze tail)

### Lifted Operators
- **Lift1(f) / Lift2(f):** Apply `f` over latest inputs once per tick
- Track input/output **frontiers**
- Compute only when a new tick is available

### Incrementalization
**Incrementalize2(f)** for **bilinear** `f`:
```
out[t] = f(a[t], b[t])
      ⊕ f(z¹(I(a))[t], b[t])
      ⊕ f(a[t], z¹(I(b))[t])
```
where `⊕` is group add, `I` is integrate, `z¹` is one-tick delay

### ZSet Joins
- **Join:** Nested-loop to start; multiply weights, sum by output key, prune zeros
- **LazyZSet:** Represent as `Vec<ZSet<T>>`; group `add` concatenates; coalesce/equality intentionally expensive—use to avoid materializing big unions prematurely

## What to Persist vs. Recompute

### Must Persist (for incremental resumption)
- ZSet weights (durable materialized state)
- Operator state needed for the next tick (e.g., integrator's accumulator, indexes)
- Minimal stream metadata (`timestamp`, `default`, optional `last_default_ts`)

### Optional (persist if you need replay/materialization/serving)
- Full stream timelines (non-default events + default breakpoints)
- Materialized outputs for external queries

## Low Write-Amplification Design

The key to avoiding copies across `ZSet`, `Stream<ZSet>`, and `Stream<Stream<ZSet>>` is **referential storage**:

### ZSet as Layered Versions

**Segments (immutable deltas):**
```
zset/<zvid>/seg/<bucket>/<segid> -> bincode(Vec<(KeyID, i64_delta)>)
```

**Manifest (a version definition):**
```
zset/<zvid>/manifest -> bincode({
  base: Option<ZVid>,                 // parent version to layer on
  buckets: BTreeMap<u16, Vec<SegId>>, // per-bucket segment chain, newest last
  stats: Option<...>                  // optional (keys, nonzero, bytes, etc.)
})
```

**Optional materialized cache (per bucket):**
```
zset/<zvid>/mat/<bucket>/<KeyID> -> i64_weight   // only nonzero
```

**Write Path:**
1. Partition changed keys into **buckets** (e.g., `xxhash64(key) % 256`)
2. For each non-empty bucket, `PUT seg` with just those **deltas**
3. `PUT manifest` with `base = Some(old_zvid)` and the appended `segid`s
4. Return `ZSetHandle { zvid, view_ts: None }`

**Effect:** O(Δ) bytes where Δ = #changed keys. No copying of entire ZSets.

### Streams Carry Handles (Tiny)

**Stream of ZSets:**
```
stream/<ns>/data/<ts>    -> bincode(ZSetHandle { zvid: ZVid, view_ts: Option<i64> })
stream/<ns>/default/<ts> -> bincode(ZSetHandle)
stream/<ns>/meta/state   -> bincode({ timestamp, identity, default: ZSetHandle })
```

If the ZSet didn't change at time `t`, reuse the same `zvid`. That's **one small write** for the stream row and **zero** writes for the ZSet.

**Stream of Streams of ZSets:**
```
stream/<outer_ns>/data/<ts_outer> -> bincode(StreamHandle { ns: "<inner-ns>", frontier: i64 })
```

### Optional Key Interning
Shrink segments by mapping keys to `u64` IDs:
```
dict/<ns>/k2id/<encoded-K> -> u64_be_key_id
dict/<ns>/id2k/<u64_be_id> -> <encoded-K>
dict/<ns>/meta             -> { next_id: u64 }
```

### Compaction & GC
- **Compaction:** Merge long per-bucket segment chains into fewer segments
- **Reference counting:** Keep liveness for each `zvid`:
  ```
  ref/<zvid> -> u64_be_refcount
  ```
- Increment when referenced; decrement when removed
- Delete `zset/<zvid>/*` when count hits zero

### Recursion / Fixpoint with Minimal Writes
```
recur/<ns>/<epoch>/state/materialized -> (alias to a zvid or a small manifest)
recur/<ns>/<epoch>/delta/<K>          -> i64_be_weight
recur/<ns>/<epoch>/meta               -> { iter: u32, stable: bool, zvid: ZVid }
```

## SlateDB API Reference (v0.8.2)

### Opening & Configuration
- `Db::open(path, Arc<dyn ObjectStore>)` - Default settings
- `Db::builder(..).with_settings(Settings)` - Tune flush cadence, TTLs, cache, compaction
- `Db::resolve_object_store(url)` - Supports s3://, gs://, filesystem, etc.

### Write Path
- `put`, `put_with_options`, `delete`, `write(WriteBatch)`
- `WriteOptions.await_durable = true` - Block until durable (default)
- `PutOptions` - Per-row TTL override
- `Db::flush()` - Force memtables/WAL to object storage

### Durability & Checkpoints
- `create_checkpoint(scope, &CheckpointOptions)` - Snapshot manifest
- `CheckpointScope::All` - Include pending writes
- `CheckpointScope::Durable` - Only flushed data
- `snapshot()` - Consistent reads without full checkpoint

### Read Path
- `DbReader::open(path, object_store, checkpoint_id, DbReaderOptions)` - Read-only workers
- `get` - Returns `Option<Bytes>`
- `scan`/`scan_with_options` - Range queries, returns `DbIterator`
- `ReadOptions` / `ScanOptions` - Control durability filter, dirty reads, read-ahead

### Merge Operations
- Implement `MergeOperator` for associative combine operations
- Useful for counters or append-only buffers

## Performance Hygiene

- **Batch flushes** when possible (transactional if available)
- Use bounded caches (LRU/sliding window) for stream events
- Avoid full-prefix scans on hot paths
- Use big-endian timestamp keys for ordered scans
- Keep default-change lists compact (store latest breakpoint in state)
- Track metadata like nonzero-key counter, `last_default_ts` to avoid expensive scans

## Testing Checklist

### Group Laws
- Associativity, commutativity, identity/inverse for each `AbelianGroup<T>` impl (ZSet, Stream)

### Stream Parity
- Run same sequence of `send`/`set_default` as Python reference
- Compare `get(t)` for all `t`

### Operator Parity
- Delay/Differentiate/Integrate/Lift1/Lift2/Incrementalize2 produce same outputs step-by-step

### Crash/Restart
- Write/flush/reopen ZSets and Streams
- Verify `current_time`, `latest`, default floor logic, zero-pruning
- If using intents, verify idempotent recovery

## Module Layout

```
core/
  abelian_group.rs

store/
  kv.rs
  slate.rs
  table.rs

zset/
  zset.rs              // materialized API
  versioned.rs         // segments, manifests, compaction, refcount
  addition.rs

stream/
  stream.rs            // sparse events + default breakpoints + meta/state
  handle.rs            // ZSetHandle, StreamHandle
  group.rs
  ops_linear.rs
  lift.rs

operators/
  incrementalize2.rs
  joins.rs
  indexes.rs           // optional: posting lists for equi-joins

dict/
  interning.rs         // optional key-id dictionary
```

## Current Implementation (crates/dbsp/src/)

### Existing Files
- `algebra/mod.rs` - `AbelianGroup<T>` trait
- `collections/zset.rs` - `ZSet<K>` implementation with SlateDB persistence
- `stream/mod.rs` - `Stream<T>` implementation with sparse storage
- `storage/mod.rs` - `KeyValueTable` trait
- `storage/encoding.rs` - rkyv serialization helpers

### Key Types (Rust Sketches)

```rust
#[derive(Serialize, Deserialize, Clone)]
pub struct ZSetHandle {
    pub zvid: [u8; 16],          // ULID/UUID
    pub view_ts: Option<i64>,    // optional read fence
}

#[derive(Serialize, Deserialize)]
pub struct ZSetManifest {
    pub base: Option<[u8; 16]>,
    pub buckets: BTreeMap<u16, Vec<[u8; 16]>>, // segids, newest last
    pub stats: Option<ManifestStats>,
}

pub trait ZSetVersioned<K> {
    fn commit_delta(&mut self, deltas: impl Iterator<Item=(K, i64)>) -> anyhow::Result<ZSetHandle>;
    fn compact(&mut self, zvid: [u8; 16], buckets: &[u16]) -> anyhow::Result<[u8; 16]>;
}

#[derive(Serialize, Deserialize, Clone)]
pub struct StreamHandle {
    pub ns: String,     // inner stream namespace
    pub frontier: i64,  // committed timestamp
}
```

## Do's & Don'ts (Amplification Edition)

### Do
- Store **handles** in streams; store **segments + manifest** for ZSet versions
- Use **bucket sharding** for ZSet deltas; **batch** per bucket
- **Version values** with a 1-byte codec tag (future-proof encodings)
- **Compact** in the background and keep **refcounts** for safe GC
- For recursion, persist `(zvid, iter, stable)` and write only the **delta** each iteration

### Don't
- Don't copy a full ZSet into a stream row
- Don't rewrite a whole ZSet on small updates
- Don't persist LazyZSet internals—persist the **coalesced** result (or indexes) as needed

## Why This Design Stays Fast

Let `Δ` be the number of changed keys at a tick and `B` the number of touched buckets.

- **Bytes written per update:** `≈ Σ size(seg_b) + |manifest| + |stream_row| = O(Δ) + O(1)`
- **Nested streams:** Outer/inner stream rows are **tiny handles** (tens of bytes), independent of ZSet size
- **Recursion:** Each iteration is one small delta segment + one manifest → **linear in change**, not in full state

## Development Commands

Since this is a library crate, testing is the primary development activity:

```bash
# Run all tests in this crate
cargo test -p dbsp

# Run with output
cargo test -p dbsp -- --nocapture

# Run a specific test
cargo test -p dbsp test_name
```
