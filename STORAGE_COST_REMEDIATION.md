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

Status: implemented and validated in `codex/storage-cost-remediation`.

Problem: min/max/top1-style deletion or repair can reload an entire affected
group from an unordered key-group index.

Implemented:

- Persistent incremental MIN/MAX repair now uses an ordered range-only extrema
  index keyed by `(group, slot, ordered_aggregate_value)`, with the original row
  stored as the index value. This is enough to distinguish tied rows without
  duplicating the full row bytes in the ordered key.
- The extrema index uses range-only postings. It does not write point-lookup
  postings that the extrema repair path never reads.
- SQL partitioned `LIMIT 1` / `ROW_NUMBER() <= 1` plans now pass an ordered
  key extractor into `DbspPartitionedTop1`, which maintains an ordered
  range-only index per partition.
- The no-ordered-index aggregate hot path still moves rows into the legacy
  transient input index instead of cloning them. This protects source-batch
  Nexmark paths that do not use the new persistent ordered extrema index.

Proof target:

- Before: `O(group_size)` index entries and segment values on affected extrema
  deletion.
- After: one bounded ordered range cursor over the first live ordered key,
  reading the first posting group plus one boundary entry. Returned rows are
  `O(output_ties)`.
- Write-side replacement: optimized ordered repair paths write range-only
  postings instead of both lookup and range postings. For persistent MIN/MAX,
  this replaces the old unordered repair index on the retractable path; for
  ordered Top1 it replaces the old unordered partition repair index.

Validation:

- `incremental_aggregate_extrema_delete_uses_ordered_index` proves deleting the
  current extrema examines one replacement row rather than the whole group.
- `partitioned_top1_ordered_index_bounds_delete_repair` proves deleting the
  partition winner repairs from the ordered index without scanning the whole
  partition.
- `arrow_indexed_range_only_writes_no_lookup_postings` proves the range-only
  index layout does not write unused point-lookup postings.

### 3. Native ASOF/Range Trace Access

Status: partially implemented; one generic interval-stabbing gap remains.

Problem: current range/asof decomposition can use broad right-side range scans
and scan the left in-memory range cache for right-side changes.

Implemented:

- ASOF right keys are now encoded as `(join_key, descending timestamp)`.
- ASOF left deltas use `RangeLookupMode::First`, so a left row reads only the
  latest qualifying right row instead of materializing every prior right row in
  the range.
- Generic range joins still use the original `RangeLookupMode::All` path.
  The mode branch is outside the hot loop so existing full-range joins keep the
  old tight scan path.

Proof target:

- Before: right-side change can examine `O(left_state_size)` cached left ranges;
  left-side change can materialize all range postings for its interval.
- After for ASOF left deltas: `O(first_live_right_key_posting_group + boundary)`
  range-index entries plus the output ties for that timestamp.
- Remaining gap: right-side changes still use the existing left range cache
  scan. A fully generic fix needs a second interval index for left ranges; a
  lower-bound-only index is not enough to prove fewer operations for arbitrary
  intervals because it can still scan all intervals with `lower <= point`.

### 4. Range Index Cursor/Filter Layout

Status: implemented and validated in `codex/storage-cost-remediation`.

Problem: `IndexedBatchZSet::values_for_key_range` materializes all SlateDB range
postings into a vector before loading segments.

Implemented:

- `KeyValueTable::scan_range_bytes_until` allows a caller to stop a SlateDB
  range scan as soon as the next posting group is outside the requested cursor
  target.
- `IndexedBatchZSet::first_values_for_key_range(_with_metrics)` uses that cursor
  to return only the first live logical key in a range.
- The full `values_for_key_range` decoder keeps its no-extra-allocation decode
  path; the richer decoder that materializes the range-key bytes is used only by
  bounded first-key lookups.
- Range-only index writes are exposed for callers that only need range cursors
  and never need point lookup by index key.

Proof target:

- Before: one range scan returning `O(range_postings)` entries and then segment
  loads for all referenced segments.
- After: first-key callers examine one posting group plus one boundary posting,
  then load only the referenced segments for that first live key.

Validation:

- `arrow_indexed_first_range_lookup_stops_after_first_posting_group` proves a
  first-key lookup reads two range entries in the fixture where the full range
  reads four.
- `arrow_indexed_range_scan_filters_keys` and
  `arrow_indexed_range_scan_rejects_legacy_layout` cover the unchanged full
  range path and legacy-layout guard.

### 5. Dictionary Round-Trip Reduction

Status: implemented for adjacent cold ID resolution.

Problem: versioned ZSet reads resolve dictionary IDs back to keys, adding point
or range reads plus decompression on cold paths.

Implemented:

- `Dictionary::resolve_many` now switches to the existing range-scan resolver
  for adjacent cold IDs at a threshold of two IDs rather than waiting for a large
  batch. This targets the common ZSet segment read pattern where dictionary IDs
  are close together.

Proof target:

- Before: `O(unique_key_ids)` dictionary resolution work per cold delta/version
  read, with point/range reads depending on ID locality.
- After for adjacent IDs: one bounded ID range scan instead of one point read per
  ID. The improvement is workload-conditional on ID locality, which is exactly
  the cold segment pattern this path sees.

Validation:

- `resolve_many_uses_one_range_scan_for_adjacent_ids` proves two adjacent cold
  IDs use one range scan and zero point reads.

## Final Validation

Focused commands:

- `cargo check -p dbsp-runtime -p dbsp-storage -p floe-executor`
- `cargo test -p dbsp-storage storage::dictionary::tests::`
- `cargo test -p dbsp-runtime collections::arrow_indexed_batch_zset::tests::`
- `cargo test -p dbsp-runtime operators::incremental_aggregate::tests::`
- `cargo test -p dbsp-runtime operators::top1::tests::`
- `cargo test -p dbsp-runtime operators::range_join::tests::`
- `cargo test -p dbsp-planner asof`
- `cargo test -p floe-executor --test dbsp_graph_builder asof`

Same-condition Nexmark regression checks versus `fc07b6b`:

| Query | Baseline run | Baseline result-ready | Current run | Current result-ready | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| q4 | `1780303342783` | 14.056s | `1780304094303` | 13.988s | no regression |
| q6 | `1780303136932` | 7.171s | `1780304297449` | 7.170s | no regression |
| q7 | `1780302385671` | 2.168s | `1780304279194` | 2.188s | within noise |
| q9 | `1780303166404` | 6.819s | `1780304330458` | 6.846s | within noise |
| q17 | `1780304580013` | 21.761s | `1780304538595` | 21.451s | no regression |
| q18 | `1780303194557` | 4.772s | `1780304360169` | 4.785s | within noise |

Additional current-branch correctness/performance sanity:

- q13 run `1780302185014`: ok, result-ready 3.248s, exact 1,000,000
  result rows.
- q20 run `1780302224626`: ok, result-ready 2.627s, exact 100,000 result
  rows.
