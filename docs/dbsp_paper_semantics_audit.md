# DBSP Paper Semantics Audit

This audit tracks Floe against the DBSP semantics from the VLDB paper:
https://www.vldb.org/pvldb/vol16/p1601-budiu.pdf.

The scope here is denotational DBSP behavior, not restart durability. Runtime
recovery and descriptor persistence are operational concerns and are tracked
separately in `docs/dbsp_runtime_semantics.md`.

## Core Model

| Paper concept | Floe status | Evidence |
| --- | --- | --- |
| Streams are total logical-time sequences | Mostly covered | `dbsp-semantic` uses total streams; runtime `Stream<T>` has evaluator-backed future values and semantic horizon tests. |
| Values form abelian groups where `D` and `I` are used | Covered for scalar and runtime stream groups | `crates/dbsp-runtime/src/algebra/mod.rs`; stream addition tests; semantic scalar laws. |
| Strict delay has identity at `t=0` and shifts by one tick | Covered | `scalar_paper_operator_laws_hold`; runtime `delay_*` tests. |
| Differentiation is `x - z^-1 x` | Covered | `scalar_paper_operator_laws_hold`; runtime `differentiate_is_input_minus_delay`. |
| Integration is cumulative sum | Covered | `scalar_paper_operator_laws_hold`; runtime `integrate_*` tests. |
| `D(I(x)) = x` | Covered | `dbsp-semantic` scalar and collection tests; runtime roundtrip test. |
| `I(D(x)) = x` for zero-initial streams | Covered | `dbsp-semantic` scalar law; runtime zero-initial roundtrip test. |

## Incrementalization

| Paper concept | Floe status | Evidence |
| --- | --- | --- |
| Query incrementalization denotes `D o up-arrow(Q) o I` | Covered in semantic layer; partially covered in runtime | `incrementalization_matches_reference_for_scalar_and_collection_circuits`; runtime `incrementalize2` law test. |
| Incrementalization composes as `(Q2 o Q1)Delta = Q2Delta o Q1Delta` | Covered in semantic layer | `incremental_composition_matches_paper_equation`; `incremental_composition_covers_non_zero_preserving_queries`. |
| Planner-generated materialized views are equivalent to the DBSP incrementalization equation | Partially covered | `dbsp_semantic_reference` compares vectorized filter/project and join/filter/project execution against `dbsp-semantic::ZSet` normalization. Full MV graph tests still need direct semantic oracle coverage. |

## Collections And Relational Operators

| Paper concept | Floe status | Evidence |
| --- | --- | --- |
| ZSets preserve signed multiplicities and cancel zero weights | Covered at semantic and storage layers | `relational_collection_laws_cover_negative_weights`; ZSet base tests. |
| Map/filter/project preserve bag semantics | Covered in semantic and runtime operator recompute tests | `semantic_queries_cover_sets_bags_and_indexes`; runtime filter/map full recompute tests; executor `vectorized_filter_project_matches_zset_reference`. |
| Joins multiply multiplicities and handle negative weights | Covered | `join_operator_handles_negative_deltas`; `join_operator_matches_full_recompute`; indexed semantic tests; executor `vectorized_join_filter_project_matches_zset_reference`. |
| Distinct/consolidate normalize signed bags consistently | Covered | semantic distinct tests; runtime distinct/consolidate recompute tests. |
| Aggregates match snapshot semantics over integrated inputs | Partially covered | semantic aggregate tests now cover additive, non-additive, duplicate, retraction, and empty-group cases; runtime aggregate recompute tests cover representative operators; SQL/planner lowering still needs reference comparisons. |
| Windows match paper-style snapshot/window semantics | Partially covered | `semantic_windows_cover_overlaps_empty_windows_and_changing_prefixes`; `semantic_windows_cover_duplicates_and_retractions`; runtime window tests exist, but planner-level window SQL needs reference comparison. |

## Recursion

| Paper concept | Floe status | Evidence |
| --- | --- | --- |
| Feedback cycles must be guarded by delay | Covered in semantic layer | `recursive_semantics_require_guarded_feedback`. |
| Runtime/planner reject or rewrite unguarded recursive DBSP plans | Needs audit | The semantic reference rejects unguarded feedback; planner admission rules need explicit tests if/when recursive SQL plans are exposed. |
| Guarded recursive collection programs match the reference model | Covered in semantic lowering tests | guarded monotonic and non-monotonic recursion lowering tests. |

## Current Gaps

1. Planner and executor SQL plans need more direct reference tests against
   `dbsp-semantic` for aggregations, windows, distinct, and full MV graph
   execution. Filter/project and join/filter/project vectorized executor
   coverage exists.
2. Runtime operator recompute tests are broad, but they are scattered. Keep this
   audit updated as the index grows so paper-semantics coverage remains visible.
3. Recursive SQL/planner admission rules need explicit guarded-delay checks once
   recursion is part of the production planning surface.
4. Stateful aggregate and window planner tests should include negative-weight
   and duplicate-heavy cases, especially when planner rewrites introduce indexes
   or transient state.
