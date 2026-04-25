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
| Planner-generated materialized views are equivalent to the DBSP incrementalization equation | Covered for representative production shapes | `dbsp_semantic_reference` compares vectorized filter/project and join/filter/project execution against `dbsp-semantic::ZSet` normalization. Full MV graph tests compare filter/project, join, aggregate, distinct, and tumbling window outputs against normalized ZSet expectations. |

## Collections And Relational Operators

| Paper concept | Floe status | Evidence |
| --- | --- | --- |
| ZSets preserve signed multiplicities and cancel zero weights | Covered at semantic and storage layers | `relational_collection_laws_cover_negative_weights`; ZSet base tests. |
| Map/filter/project preserve bag semantics | Covered in semantic and runtime operator recompute tests | `semantic_queries_cover_sets_bags_and_indexes`; runtime filter/map full recompute tests; executor `vectorized_filter_project_matches_zset_reference`. |
| Joins multiply multiplicities and handle negative weights | Covered | `join_operator_handles_negative_deltas`; `join_operator_matches_full_recompute`; indexed semantic tests; executor `vectorized_join_filter_project_matches_zset_reference`. |
| Distinct/consolidate normalize signed bags consistently | Covered | semantic distinct tests; runtime distinct/consolidate recompute tests. |
| Aggregates match snapshot semantics over integrated inputs | Covered for representative shapes | semantic aggregate tests now cover additive, non-additive, duplicate, retraction, and empty-group cases; runtime aggregate recompute tests cover representative operators; full MV aggregate tests compare normalized outputs after insertions and retractions. |
| Windows match paper-style snapshot/window semantics | Covered for representative shapes | `semantic_windows_cover_overlaps_empty_windows_and_changing_prefixes`; `semantic_windows_cover_duplicates_and_retractions`; full MV tumbling window tests compare normalized outputs for max and grouped count. |

## Recursion

| Paper concept | Floe status | Evidence |
| --- | --- | --- |
| Feedback cycles must be guarded by delay | Covered in semantic layer | `recursive_semantics_require_guarded_feedback`. |
| Runtime/planner reject or rewrite unguarded recursive DBSP plans | Needs audit | The semantic reference rejects unguarded feedback; planner admission rules need explicit tests if/when recursive SQL plans are exposed. |
| Guarded recursive collection programs match the reference model | Covered in semantic lowering tests | guarded monotonic and non-monotonic recursion lowering tests. |

## Current Gaps

1. Runtime operator recompute tests are broad, but they are scattered. Keep this
   audit updated as the index grows so paper-semantics coverage remains visible.
2. Recursive SQL/planner admission is currently explicit rejection of
   DataFusion recursive query plans. If recursive SQL becomes production
   surface, guarded-delay acceptance tests must be added before enabling it.
3. The audit uses representative cases, not a formal proof. New planner
   rewrites, aggregate functions, window policies, or index optimizations should
   add semantic-oracle tests before being considered paper-semantics covered.
