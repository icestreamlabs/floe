# AGENTS.md

A practical guide for building a Rust/SlateDB port of **pydbsp** with correct DBSP semantics **and low write amplification**.

---

## Core Principles

* **Everything is an Abelian group.**
  All stream values live in a group `⟨T, add, neg, identity⟩`. Operators **never** use `+` directly—always call the group’s `add/neg`. This keeps all code generic (ZSets, LazyZSets, Streams of anything).

* **Streams are sparse, piecewise-constant timelines.**
  Store explicit **non-default events** by timestamp; everything else is implied by a **piecewise-constant default** (tracked as “default change” breakpoints). Reading at time `t` returns:

  1. the event at `t` if present, else
  2. the most recent default ≤ `t`.

* **Freeze tails after lifted ops.**
  After operators like lifted `Delay`/`Integrate`, “freeze” the substream by setting its default to its latest value. Out-of-frontier reads remain causal without recomputation.

* **Persist what matters for incremental resumption.**
  Persist *logical* state (e.g., ZSet weights, join indexes, accumulators) so the circuit can resume without recomputing from scratch. Persist full timelines only if you need replay/materialization.

---

## Storage Model (SlateDB)

### One DB, many logical tables

* Use a **single** `Arc<Db>` per runtime/circuit.

* Create **namespaced prefixes** for logical tables:

  ```
  zset/<ns>/<key>                                 -> i64_be_weight
  stream/<ns>/data/<be_i64_ts>                    -> value
  stream/<ns>/default/<be_i64_ts>                 -> default_value
  stream/<ns>/meta/state                          -> { timestamp, identity, default, ... }
  index/<ns>/…                                    -> (join indexes when you add them)
  ```

* Encode timestamps as **big-endian i64** for ordered scans.

### Flushing & crash safety

* Prefer **batched writes** (one atomic flush for `{defaults, data, state}`).
* If no transactions: write `{defaults, data}` first, then `state` as a summary.
  Optional: write an **intent** record before data and clear it after `state` for robust recovery.

### Separate algebra from storage

* Wrap SlateDB with a small **KV/storage trait** (e.g., `get/put/delete/scan_prefix/write_batch`).
* `ZSet`/`Stream` hold a lightweight **Table handle** (`store + prefix`), not their own DB.

---

## SlateDB API reference (docs.rs v0.8.2)

* **Opening & configuration**

  * `Db::open(path, Arc<dyn ObjectStore>)` gives a ready-to-use handle with default `Settings`.
  * Prefer `Db::builder(..).with_settings(Settings)` when you need to tune flush cadence, TTLs, cache sizing, or compaction—`Settings` exposes knobs such as `flush_interval`, `max_unflushed_bytes`, `l0_sst_size_bytes`, `compression_codec`, and `default_ttl`.
  * Builder helpers wire in infrastructure pieces: `with_wal_object_store` (dedicated WAL bucket), `with_memory_cache` (custom `DbCache`, default `SplitCache`), `with_logical_clock` / `with_system_clock`, `with_gc_runtime`, `with_compaction_runtime`, `with_compaction_scheduler_supplier`, and `with_sst_block_size`.
  * `Db::resolve_object_store(url)` understands `s3://`, `gs://`, filesystem, and other backends shipped with SlateDB—use it before handing the handle to the builder.

* **Write path**

  * Core mutations live on `Db`: `put`, `put_with_options`, `delete`, and `write(WriteBatch)`. `WriteBatch::put/delete` accept any `AsRef<[u8]>` key/value and apply atomically; there is no batch size guard, so chunk very large flushes to avoid oversized WAL/SST artifacts.
  * `WriteOptions` defaults to `await_durable = true`, meaning `put` blocks until data has reached object storage (or WAL). Set it to `false` only when your operator tolerates replay after a crash.
  * `PutOptions` lets you override per-row TTL; the effective TTL is whatever the most recent write sets (fallback is `Settings::default_ttl`).
  * `Db::flush` / `flush_with_options` force memtables/WAL to object storage; they block until durable. Use when coordinating with DBSP frontiers or before durable checkpoints.

* **Durability windows & checkpoints**

  * `Db::create_checkpoint(scope, &CheckpointOptions)` emits a new manifest entry and returns IDs for bookkeeping. `CheckpointScope::All` optionally forces a flush to include pending writes, while `CheckpointScope::Durable` snapshots only already-durable bytes.
  * `CheckpointOptions::lifetime` adds an expiry window; `source` clones an existing checkpoint with fresh lifecycle metadata.
  * `Db::snapshot()` returns an `Arc<DbSnapshot>` for consistent reads without full checkpoint materialization.

* **Read path**

  * Use `DbReader::open(path, object_store, checkpoint_id, DbReaderOptions)` for read-only workers. Without a `checkpoint_id`, the reader maintains its own leased checkpoint; tune `manifest_poll_interval`, `checkpoint_lifetime`, and `max_memtable_bytes` to balance lag vs. cost.
  * `DbReader::get` returns `Option<Bytes>` backed by an entire 4 KiB block; clone or copy if you need to hold onto the value long term because the cache retains the block while references exist.
  * Range access: `scan`/`scan_with_options` return a `DbIterator`. Respect forward-only semantics when calling `next`/`seek`; an invalidated iterator yields `SlateDBError::InvalidatedIterator`.
  * `ReadOptions` / `ScanOptions` expose `durability_filter: DurabilityLevel` (`Remote` for flushed data only, `Memory` to allow unflushed bytes), `dirty` (visibility of uncommitted WAL), and streaming knobs (`read_ahead_bytes`, `cache_blocks`, `max_fetch_tasks`).

* **Merge operations**

  * Implement `MergeOperator` when you can express updates as an associative combine. SlateDB will compose merge operands during reads and compactions, avoiding explicit read-modify-write. Use this for counters or append-only buffers while keeping payloads in canonical little-endian/flatbuffer form.

---

## Data Structures

### `ZSet<K>`

* **Semantics**: integer-weighted multiset. `add` merges weights by key; `neg` negates; `identity` is empty set. Remove entries whose weight becomes **zero**.
* **API**: `get_weight`, `set_weight`, `add_weight`, `contains`, `items`, `is_identity`, `flush`.
* **Storage**: `zset/<ns>/<key> → i64_be`. Keep a **pending overlay** (in-memory) to read your unflushed writes.
* **Perf tips**:

  * Batch flush upserts/deletes.
  * Optionally track a **nonzero-key counter** in metadata to make `is_identity()` O(1).

### `Stream<T>`

* **Fields**:

  * `timestamp` (frontier), `identity` (true until a non-default event appears), `default` (current default value).
  * **Pending**: `pending_data: BTreeMap<ts→T>`, `pending_defaults: BTreeMap<ts→T>`, `pending_state: bool`.
  * **Caches**: `data_cache` (bounded/LRU recommended), `default_changes: BTreeMap<ts→T>` (breakpoints).
* **API**: `send(T)`, `set_default(T)`, `get(ts)`, `latest()`, `to_vec()`, `flush()`, `current_time()`, `is_identity()`.
* **Semantics**:

  * `send(x)` advances time; persist only if `x != default`.
  * `set_default(d)` adds a breakpoint at *current* `timestamp`.
  * `get(t)` extends to `t` by sending defaults, returns event-or-default-at-`t`.
* **Storage**:

  * `stream/<ns>/data/<ts> → T`
  * `stream/<ns>/default/<ts> → T`
  * `stream/<ns>/meta/state → { timestamp, identity, default }`
  * Optional `last_default_ts` in state to avoid scanning all defaults on cold start.

---

## Operators (Shapes & Patterns)

### Linear operators on streams

* **Delay**: `out[t] = in[t-1]` (seed at 0).
* **Differentiate**: `out[t] = add(in[t], neg(in[t-1]))`.
* **Integrate**: running sum via feedback delay. **Lifted integrate**: compute to fixpoint, then **set default to latest** (freeze tail).

### Lifted operators

* **Lift1(f)** / **Lift2(f)**: apply `f` over latest inputs once per tick. Track input/output **frontiers**; compute only when a new tick is available.

### Incrementalization

* **Incrementalize2(f)** for **bilinear** `f`:

  ```
  out[t] = f(a[t], b[t])
        ⊕ f(z¹(I(a))[t], b[t])
        ⊕ f(a[t], z¹(I(b))[t])
  ```

  where `⊕` is group add, `I` is integrate, `z¹` is one-tick delay.

### ZSet joins

* **Join**: nested-loop to start; multiply weights, sum by output key, prune zeros.
* **LazyZSet**: represent as `Vec<ZSet<T>>`; group `add` concatenates; coalesce/equality intentionally expensive—use to avoid materializing big unions prematurely.

---

## What to Persist vs. Recompute

* **Must persist** (for incremental resumption):

  * ZSet weights (durable materialized state).
  * Operator state needed for the next tick (e.g., integrator’s accumulator, indexes).
  * Minimal stream metadata (`timestamp`, `default`, optional `last_default_ts`).

* **Optional** (persist if you need replay/materialization/serving):

  * Full stream timelines (non-default events + default breakpoints).
  * Materialized outputs for external queries.

---

## Performance Hygiene

* **Batch flushes**; if possible, transactional.
* Use bounded caches (LRU/sliding window) for stream events.
* Avoid full-prefix scans on hot paths. Prefer metadata (e.g., nonzero-key counter, `last_default_ts`).
* Use big-endian timestamp keys for ordered scans.
* Keep default-change lists compact (store latest breakpoint in state; lazily load older ones if needed).

---

## Testing Checklist

* **Group laws**: associativity, commutativity, identity/inverse for each `AbelianGroup<T>` impl (ZSet, Stream).
* **Stream parity**: run the same sequence of `send`/`set_default` as the Python reference, compare `get(t)` for all `t`.
* **Operator parity**: Delay/Differentiate/Integrate/Lift1/Lift2/Incrementalize2 produce the same outputs step-by-step.
* **Crash/restart**:

  * Write/flush/reopen ZSets and Streams; verify `current_time`, `latest`, default floor logic, zero-pruning.
  * If using intents, verify idempotent recovery.

---

# ✨ Low-Write-Amplification Design (Handles, Layers, and Deltas)

The key to avoiding copies across `ZSet`, `Stream<ZSet>`, and `Stream<Stream<ZSet>>` is **referential storage**:

* Streams store **handles to versions** of ZSets/streams, **not copies**.
* ZSet versions are **manifests** that layer small **delta segments** over a prior version (copy-on-write).
* Optional **key interning** shrinks payloads; optional **bucket sharding** caps segment size.
* Background **compaction** rewrites manifests to shorter chains; **refcounts** enable safe GC.

## 1) ZSet as layered versions

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

**Write path (update):**

1. Partition changed keys into **buckets** (e.g., `xxhash64(key) % 256`).
2. For each non-empty bucket, `PUT seg` with just those **deltas**.
3. `PUT manifest` with `base = Some(old_zvid)` and the appended `segid`s.
4. Return `ZSetHandle { zvid, view_ts: None }`.

**Effect:** O(Δ) bytes where Δ = #changed keys. No copying of entire ZSets.

## 2) Streams carry handles (tiny)

**Stream of ZSets:**

```
stream/<ns>/data/<ts>    -> bincode(ZSetHandle { zvid: ZVid, view_ts: Option<i64> })
stream/<ns>/default/<ts> -> bincode(ZSetHandle)
stream/<ns>/meta/state   -> bincode({ timestamp, identity, default: ZSetHandle })
```

If the ZSet didn’t change at time `t`, reuse the same `zvid`. That’s **one small write** for the stream row and **zero** writes for the ZSet.

**Stream of Streams of ZSets:**

```
stream/<outer_ns>/data/<ts_outer> -> bincode(StreamHandle { ns: "<inner-ns>", frontier: i64 })
```

An outer row references an inner stream by **namespace + frontier**, not by copying inner payloads.

## 3) Optional key interning

Shrink segments by mapping keys to `u64` IDs:

```
dict/<ns>/k2id/<encoded-K> -> u64_be_key_id
dict/<ns>/id2k/<u64_be_id> -> <encoded-K>
dict/<ns>/meta             -> { next_id: u64 }
```

Segments then use compact `(KeyID, i64_delta)` tuples.

## 4) Compaction & GC

**Compaction:** Merge long per-bucket segment chains into fewer segments (or into `mat/`). Publish a **new `zvid_compacted`** (cheap manifest write) and migrate references gradually.

**Reference counting:** Keep liveness for each `zvid`:

```
ref/<zvid> -> u64_be_refcount
```

Increment when referenced by a stream row or manifest; decrement when references are removed or compacted away. Delete `zset/<zvid>/*` when the count hits zero.

## 5) Recursion / fixpoint with minimal writes

Represent the evolving state `X_k` as a chain of **ZSet versions**:

```
recur/<ns>/<epoch>/state/materialized -> (alias to a zvid or a small manifest)
recur/<ns>/<epoch>/delta/<K>          -> i64_be_weight         // optional “live delta”
recur/<ns>/<epoch>/meta               -> { iter: u32, stable: bool, zvid: ZVid }
```

Each iteration writes a **small segment + manifest** to create the next `zvid`; update `meta` last. When the delta empties, set `stable=true` and publish the final `zvid` for that epoch. No wholesale rewrites.

## 6) Restart algorithms (quick)

* **ZSet:** load a requested `zvid` → fold `base` + segments (use `mat/` if present).
* **Stream:** read `meta/state`, then fetch default breakpoints and per-`ts` handles lazily.
* **Recursive epoch:** read `recur/.../meta`; if `stable==false`, resume from the recorded `zvid` (and optional `delta`) and continue iterations until fixpoint.

---

# Practical APIs & Types (Rust sketches)

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

*Your existing `Stream<T>` works with `T = ZSetHandle` or `T = StreamHandle` unchanged.*

---

# Module layout with the low-amplification pieces

```
core/
  abelian_group.rs

store/
  kv.rs
  slate.rs
  table.rs

zset/
  zset.rs              // materialized API (as today)
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

---

## Do’s & Don’ts (amplification edition)

**Do**

* Store **handles** in streams; store **segments + manifest** for ZSet versions.
* Use **bucket sharding** for ZSet deltas; **batch** per bucket.
* **Version values** with a 1-byte codec tag (future-proof encodings).
* **Compact** in the background and keep **refcounts** for safe GC.
* For recursion, persist `(zvid, iter, stable)` and write only the **delta** each iteration.

**Don’t**

* Don’t copy a full ZSet into a stream row.
* Don’t rewrite a whole ZSet on small updates.
* Don’t persist LazyZSet internals—persist the **coalesced** result (or indexes) as needed.

---

## Why this design stays fast (intuition)

Let `Δ` be the number of changed keys at a tick and `B` the number of touched buckets.

* **Bytes written per update:** `≈ Σ size(seg_b) + |manifest| + |stream_row| = O(Δ) + O(1)`
* **Nested streams:** outer/inner stream rows are **tiny handles** (tens of bytes), independent of ZSet size.
* **Recursion:** each iteration is one small delta segment + one manifest → **linear in change**, not in full state.

---

Follow these rules and patterns and your agent will produce a faithful, **efficient** Rust/SlateDB port of `pydbsp` that preserves DBSP’s algebraic and incremental semantics—without drowning in write amplification.
