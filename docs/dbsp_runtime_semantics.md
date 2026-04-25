# DBSP Runtime Semantics

This document records the semantic contract for the runtime stream layer.

## Streams

`Stream<T>` is a DBSP logical-time stream: semantically, it is a total function
from logical time to values in an Abelian group.

The persisted data/default keys are a materialization and compaction strategy,
not the stream semantics. `semantic_horizon()` is the last timestamp currently
materialized or scheduled in storage. It is not a proof that the stream has
settled semantically.

Evaluator-backed derived streams compute values pointwise from their input
streams. Generic stream operators such as delay, differentiation, integration,
lifting, addition, and negation must use this evaluator path rather than
deriving future values from storage defaults.

## Evaluator Persistence

Evaluator-backed streams persist an `meta/evaluator` marker. Reconstructible
operators persist a serializable descriptor; the runtime currently supports
descriptors for the built-in time operators `delay`, `differentiate`, and
`integrate`, plus built-in stream `add` and `neg`.

For descriptor-backed streams, reopening rebuilds the evaluator from the
descriptor and recursively reopens the input stream. This applies to scalar
streams and handle-valued streams alike, so built-in time operators over derived
ZSet handles remain semantically evaluable after process restart.

Closure-backed evaluator streams still register their evaluator graph in-process
by namespace. Reopening them in the same process restores the evaluator from the
registry. Reopening them without the evaluator graph fails explicitly. This
avoids silently interpreting a derived stream as a finite prefix plus compacted
default tail.

The remaining durable graph recovery work is to replace closure-backed runtime
evaluators with typed operator descriptors wherever the operator is part of the
production DBSP surface. Until then, non-descriptor evaluator streams are
intentionally restart-bounded.

## Lifted Handle Streams

Lifted scalar handle operators and lifted-lifted select, project, join, and H
operators are evaluator-backed at the outer stream level. For each logical time
they resolve the input handles, apply the corresponding inner operator, flush the
derived inner stream, and return its handle.

`lifted_stream_introduction` remains a finite materialization wrapper. It is
used by lifted join laws where handle namespace identity is part of the current
runtime contract; changing it to a naive evaluator-backed form regressed
`lifted_join_covers_each_delta_term`.

## ZSet Operators

The lifted ZSet select, project, join, integral, and H operators use stateful
ZSet runtime machinery. They materialize an initial prefix and then publish
future handles through live cursor tasks. Those operators should be validated by
future-tick tests and against the semantic reference model when their behavior
changes.

Reference recompute tests currently cover the stateful operator cores for
filter, map, distinct, consolidate, aggregate, group-by, top-N, joins,
semijoins, antijoins, range joins, and as-of joins. `dbsp-semantic` lowering
tests cover reference execution and reopen behavior for scalar, ZSet, set,
indexed, nested, recursive, and windowed streams.

## Partial Operators

`stream_elimination` is only exact for eventually-identity streams. It rejects
non-eventually-identity inputs instead of returning an approximate value.
