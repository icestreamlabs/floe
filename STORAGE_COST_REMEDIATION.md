# Storage Cost Remediation Tracker

This tracker is for DBSP storage changes that must replace existing code with a
provably lower storage-operation cost model. Cache-only changes do not count as
remediation unless the cold or persisted-path storage operation count also
improves.

## Acceptance Rule

Every remediation should document:

- The previous storage-operation formula.
- The new storage-operation formula.
- Whether the improvement is worst-case, amortized, or workload-conditional.
- A focused test or metric that proves the relevant storage operation count.

Storage work includes point reads, range scans, entries examined by scans,
segment reads, manifest reads/scans, dictionary reads/writes/resolves, bytes
decoded/decompressed, and write/tombstone amplification where relevant.

## Points

### 1. Versioned ZSet Manifest Discovery

Status: implemented and validated in `codex/storage-cost-remediation`.

Problem: `VersionedZSet::refresh_state` scans the entire manifest prefix to find
the latest version and next segment id. Reopen/release cost is therefore
proportional to retained manifest count.

Target: maintain a compact per-namespace version-state record outside the
manifest prefix. New opens should use point reads for state and current manifest.

Proof target:

- Before: `1` manifest-prefix range scan returning `O(manifest_count)` entries.
- After: `1` state point read plus `1` current-manifest point read.
- Release of the persisted head updates the state record directly instead of
  refreshing by manifest-prefix scan.
- Compatibility fallback may scan once when state metadata is missing, then
  repair the metadata.
- Write-side tradeoff: each persisted version update writes one additional
  state key. This is accepted here because the targeted operation is
  reopen/release discovery, where an unbounded manifest scan is replaced by
  bounded point reads.

Validation:

- `versioned_zset_reopen_uses_state_metadata_without_manifest_scan` proves reopen
  uses point metadata instead of a manifest-prefix scan.
- `versioned_zset_missing_state_metadata_scans_once_and_repairs` proves legacy
  compatibility does one fallback scan and repairs the state key.
- `versioned_zset_release_updates_state_without_manifest_scan` proves head
  release updates metadata without rediscovery scans.
- `cargo test -p dbsp-runtime collections::zset::base::tests::`
- `cargo test -p dbsp-runtime stream::tests::core::`
- `cargo test -p dbsp-runtime operators::join::tests::`
- `cargo check -p dbsp-runtime`
- Nexmark affected checks:
  - `q0`, run `1780295254191`: ok, 1,000,000 input rows, 1,000,000 result rows.
  - `q5`, run `1780296295352`: ok, 1,000,000 input rows, 5,000,000 result rows.
  - `q7`, run `1780296318072`: ok, 1,000,000 input rows, 101 result rows.
  - `q17`, run `1780296257045`: ok, 1,000,000 input rows, 10,000 result rows.
  - `q13`, run `1780299035096`: ok, 1,010,000 input rows, 1,000,000 result rows.
  - `q20`, run `1780299074135`: ok, 1,010,000 input rows, 100,000 result rows.

Benchmark investigation note: two-source `q13`/`q20` initially timed out after
the MV visibility barrier waited for a version that a no-output transient join
tick never published. This was reproduced on `main` and bisected to
`7eb5781`, not to this point's manifest-discovery change. The branch now emits
empty progress batches from the live transient join driver so overlay
materialization can publish no-op logical versions.

### 2. Ordered Extrema/Top1 Indexes

Status: pending.

Problem: min/max/top1-style deletion or repair can reload an entire affected
group from an unordered key-group index.

Target: store exact ordered per-group state keyed by `(group, aggregate_value,
row_id)` or equivalent so next extrema/top1 lookup is a bounded seek rather than
a full group scan.

Proof target:

- Before: `O(group_size)` index entries and segment values on affected extrema
  deletion.
- After: `O(1)` or `O(log group_size + output_ties)` ordered reads, subject to
  SlateDB seek/scan primitive support.

### 3. Native ASOF/Range Trace Access

Status: pending.

Problem: current range/asof decomposition can use broad right-side range scans
and scan the left in-memory range cache for right-side changes.

Target: implement native ordered trace access for ASOF/range workloads over
`(join_key, timestamp)` or equivalent, so updates touch changed keys and affected
timestamp neighborhoods.

Proof target:

- Before: right-side change can examine `O(left_state_size)` cached left ranges;
  left-side change can materialize all range postings for its interval.
- After: right-side/left-side changes read only ordered trace neighborhoods plus
  matching output rows.

### 4. Range Index Cursor/Filter Layout

Status: pending.

Problem: `IndexedBatchZSet::values_for_key_range` materializes all SlateDB range
postings into a vector before loading segments.

Target: expose storage access that supports ordered cursor reads, bounded scans,
and block/key filters. Streaming alone is not sufficient unless callers can skip
or stop before reading the whole range.

Proof target:

- Before: one range scan returning `O(range_postings)` entries and then segment
  loads for all referenced segments.
- After: bounded cursor/filter reads where entries examined are proportional to
  matching blocks/rows rather than the entire posting range.

### 5. Dictionary Round-Trip Reduction

Status: pending.

Problem: versioned ZSet reads resolve dictionary IDs back to keys, adding point
or range reads plus decompression on cold paths.

Target: avoid global dictionary resolution where operator-private segments
already carry enough encoded row data, or replace global resolution with a
storage layout that proves fewer cold reads.

Proof target:

- Before: `O(unique_key_ids)` dictionary resolution work per cold delta/version
  read, with point/range reads depending on ID locality.
- After: fewer dictionary storage reads on the same cold path, with any byte-size
  tradeoff explicitly accounted for.
