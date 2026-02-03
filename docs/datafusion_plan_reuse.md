# DataFusion Plan Reuse and Build/Probe Selection

This note captures how Floe keeps DataFusion plans reusable while updating
state between ticks, and how we keep the state side on the hash-join build
side.

## Plan reuse with DynamicStateTableProvider

`DynamicStateTableProvider` exposes a stable `DynamicStateExec` backed by an
`ArcSwap<Vec<RecordBatch>>` snapshot. The physical plan is built once and then
reused across ticks. Each execution takes a fresh snapshot of the current
state batches so the plan sees updated state without re-planning.

Typical flow:

1. Build a physical plan once (SQL -> logical -> physical).
2. Call `set_batches` or `set_snapshot` on the provider each tick.
3. Execute the same physical plan; `DynamicStateExec` snapshots at execution
   time.

## Build/probe side selection

DataFusion's `JoinSelection` rule may swap join sides based on statistics. It
uses `total_byte_size` when available, falling back to `num_rows`. The goal is
for the dynamic state to stay on the build side (left) of the hash join, with
the stream on the probe side.

Guardrails:

- `DynamicStateExec::partition_statistics` reports exact row and byte counts.
- Keep the state input on the left in SQL (e.g. `state JOIN stream`).
- Keep the state table smaller than the streaming side so the cost model does
  not swap.

The integration test `crates/floe-executor/tests/dynamic_state_join.rs` asserts
that the build side contains `DynamicStateExec` and fails fast if the planner
flips the join.

## Forcing the build side (if needed)

If the planner still swaps unexpectedly:

- Construct a `HashJoinExec` explicitly with the state on the left, and skip
  DataFusion's join-selection optimizer for that plan.
- Or build a `SessionState` with custom physical optimizer rules that omit the
  `JoinSelection` rule via `SessionStateBuilder::with_physical_optimizer_rules`.

These are advanced escape hatches; the default behavior should be stable when
state statistics are accurate and the state side is smaller.
