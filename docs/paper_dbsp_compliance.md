# Paper DBSP Compliance In Floe

## Meaning

In Floe, "paper DBSP compliant" now refers to the semantic API exposed by `dbsp-semantic` and re-exported as `dbsp::semantic`.

That semantic layer provides:
- total streams from logical time `t in N` to values,
- semantic value families for scalar groups, sets, bags/Z-sets, indexed collections, and nested relations,
- pointwise lifting, composition, strict delay, and guarded feedback,
- semantic `differentiate(x) = x - delay(x)`,
- semantic `integrate(x)` defined by feedback instead of eventual-identity runtime restrictions,
- circuit-level `D`, `I`, and `QΔ = D ∘ ↑Q ∘ I`,
- semantic collection operators for relational algebra, aggregation, nested unnest/flatmap, recursion, and windowed operators.

## Runtime Split

`dbsp-runtime` remains the operational substrate.

`dbsp-runtime::stream::Stream<T>` is not the paper stream object. It is a storage-backed execution abstraction that exposes:
- current logical time,
- committed frontier,
- semantic horizon,
- default-tail state.

Those runtime observations are useful operationally and intentionally remain separate from the semantic API.

## Lowering Contract

Lowering from `dbsp-semantic` targets the existing handle/Z-set runtime substrate.

The current lowering contract is:
- semantic streams remain denotational and total,
- lowering materializes an exact requested observational prefix into runtime streams and versioned Z-sets,
- collection-valued outputs are lowered into handle-backed runtime Z-set streams,
- set outputs are lowered as distinct-normalized Z-sets,
- indexed outputs are lowered as pair-encoded Z-sets,
- lowered snapshot and delta prefixes are checked against the denotational reference evaluator.

This keeps semantic guarantees and runtime guarantees distinct while reusing the current storage and handle substrate.

## What This Sprint Does Not Claim

This sprint does not claim that:
- `dbsp-runtime::stream::Stream<T>` is the paper denotational stream model,
- SQL runtime correctness alone implies generic paper-stream compliance,
- the semantic layer replaces the existing runtime or planner,
- runtime execution semantics beyond the requested observational lowering prefix are a new semantic contract.
