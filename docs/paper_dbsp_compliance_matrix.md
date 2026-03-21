# Paper DBSP Compliance Matrix

| Paper Construct / Query Family | Semantic API | Lowering Path | Validation Tests | Notes / Exclusions |
| --- | --- | --- | --- | --- |
| Total semantic streams | `dbsp::semantic::Stream` | `lower_scalar_prefix`, `lower_zset_prefix` | `paper_operators_are_total_on_infinite_scalar_streams`, `lowered_scalar_prefix_matches_reference_and_survives_reopen` | Runtime lowering is validated over the requested observational prefix |
| Pointwise lift `↑f` | `Stream::lift`, `pointwise` | `lower_scalar_prefix`, `lower_zset_prefix` | `semantic_queries_cover_sets_bags_and_indexes`, `incrementalization_matches_reference_for_collection_circuit` | Extensional, semantic-first API |
| Composition / product | `Circuit::compose`, `Circuit::fanout`, `pair` | Apply circuit, then lower output stream | `incrementalization_matches_reference_for_collection_circuit` | Multi-input circuits are represented through tuples |
| Strict delay `z^-1` | `delay`, `strict_delay` | `lower_scalar_prefix`, `lower_zset_prefix` | `paper_operators_are_total_on_infinite_scalar_streams` | Semantic delay returns zero at `t=0` |
| Differentiation | `differentiate`, `circuit_d` | `lower_scalar_prefix`, lowered delta handle stream | `paper_operators_are_total_on_infinite_scalar_streams`, `lowered_zset_prefix_matches_reference_delta_and_reopen` | Delta lowering uses per-tick delta Z-set versions |
| Integration | `integrate`, `circuit_i` | `lower_scalar_prefix`, `lower_zset_prefix` | `paper_operators_are_total_on_infinite_scalar_streams`, `incrementalization_matches_reference_for_collection_circuit` | No eventual-identity restriction in semantic layer |
| Circuit incrementalization `QΔ = D ∘ ↑Q ∘ I` | `incrementalize` | Lower the resulting semantic output stream | `incrementalization_matches_reference_for_collection_circuit` | Tested on collection-valued query circuits, not just scalars |
| Sets | `Set`, `map_set`, `filter_set`, `union_set`, `join_set` | `lower_set_prefix` | `semantic_queries_cover_sets_bags_and_indexes`, `lowered_set_and_indexed_prefixes_match_reference` | Lowered as distinct-normalized Z-sets |
| Bags / Z-sets | `ZSet`, `map_zset`, `filter_zset`, `union_zset`, `join_zset`, `distinct_zset` | `lower_zset_prefix` | `semantic_queries_cover_sets_bags_and_indexes`, `lowered_zset_prefix_matches_reference_delta_and_reopen` | Core collection domain for DBSP-style relational algebra |
| Indexed collections | `IndexedZSet`, `arrange_by`, `lookup_index`, `join_indexed` | `lower_indexed_prefix` | `semantic_queries_cover_sets_bags_and_indexes`, `lowered_set_and_indexed_prefixes_match_reference` | Lowered as pair-encoded Z-sets |
| Aggregation | `aggregate_zset`, `count_by_zset` | `lower_zset_prefix` | `semantic_aggregation_nesting_and_windows_match_expected_values`, `lowered_zset_prefix_matches_reference_delta_and_reopen` | Aggregator is semantic-first; lowering checks observable results |
| Nested relations / unnest | nested `ZSet` values, `unnest_zset` | `lower_zset_prefix` | `semantic_aggregation_nesting_and_windows_match_expected_values` | Nested collections are represented compositionally as values |
| Monotonic recursion | `feedback` + semantic operators | Lower final semantic output stream | `feedback_supports_monotonic_and_non_monotonic_recursion` | Guarded by semantic delay |
| Non-monotonic recursion | `feedback` + `subtract` / negative weights | Lower final semantic output stream | `feedback_supports_monotonic_and_non_monotonic_recursion` | Guarded feedback only; unguarded cycles panic during evaluation |
| Streaming / windowed operators | `sliding_window_aggregate`, `tumbling_window_aggregate` | `lower_zset_prefix` | `semantic_aggregation_nesting_and_windows_match_expected_values` | Semantic contract is denotational; current lowering is observational-prefix based |
| Snapshot/runtime restart continuity | lowered runtime stream reopen | Reopen runtime stream namespaces in `dbsp-runtime` | `lowered_scalar_prefix_matches_reference_and_survives_reopen`, `lowered_zset_prefix_matches_reference_delta_and_reopen` | Validates logical continuity across reopen for lowered prefixes |

## Blocking Rule

Any unchecked or undocumented construct in this matrix blocks a full paper-compliance claim.
