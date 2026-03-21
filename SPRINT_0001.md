# SPRINT_0001 Explicit Paper DBSP Compliance Plan
This sprint establishes an explicit paper-DBSP-compliant semantic API layer in
Floe while preserving the current handle/ZSet runtime as the execution and
persistence substrate.

The goal is not to "improve" the current storage-backed runtime `Stream<T>`
until it resembles the paper. The goal is to separate the semantic stream model
from the operational runtime model and make the semantic layer explicitly match
the DBSP paper.

## Explicit Compliance Target

For this sprint, "paper DBSP compliant" means the following at the semantic API
layer:

- semantic streams denote total functions from logical time `t ∈ N` to values,
- semantic values cover the collection domains required by DBSP, including
  group-valued streams, sets, bags/Z-sets, indexed collections, and nested
  relations,
- semantic circuits support composition, strict time operators, and feedback as
  first-class semantic constructs,
- semantic stream operators are extensional and do not expose runtime storage or
  frontier observations,
- semantic `delay` implements paper `z^-1`,
- semantic `differentiate(x)` implements `x - z^-1(x)`,
- semantic `integrate` implements the paper stream integral via feedback and is
  not restricted to eventually-identity inputs,
- the semantic layer supports pointwise lifting `↑f` and the DBSP
  incrementalization construction `QΔ = D ∘ ↑Q ∘ I` for arbitrary semantic
  DBSP circuits,
- the semantic layer can encode the major query families the paper claims DBSP
  can express, including relational algebra over sets and bags, nested
  relations, aggregation, flatmap/unnest, monotonic and non-monotonic
  recursion, streaming/windowed operators, and arbitrary compositions of these,
- the public API no longer presents the current operational runtime
  `Stream<T>` algebra as if it were the paper stream model.

This sprint is successful if Floe has a semantic stream core that satisfies the
above contract across the full semantic DBSP surface Floe claims to implement,
and if the public runtime surface is narrowed so it no longer over-claims paper
semantics.

## High-level Context

Floe's current generic `Stream<T>` in
`crates/dbsp-runtime/src/stream/core/stream/mod.rs` is an operational
prefix-plus-tail abstraction. It explicitly exposes:

- `current_time()`,
- `committed_frontier()`,
- `semantic_horizon()`,
- `default_value()`.

That is not the same semantic object as the DBSP paper's stream model, which is
a total function from logical time to values.

Today this mismatch is visible in the generic runtime algebra:

- `integrate` is only exact for eventually-identity inputs,
- `stream_elimination` is similarly restricted,
- the generic stream algebra is still re-exported from both `dbsp-runtime` and
  the public `dbsp` facade crate.

The audited SQL/runtime layer is in much better shape now, but that is not the
same as saying the public runtime stream API implements the paper's stream
semantics.

The explicit goal of this sprint is:

- make Floe explicitly paper-DBSP-compliant at the semantic stream/circuit API
  layer.

This sprint must not attempt to turn the current storage-backed
`dbsp-runtime::Stream<T>` into the paper object in place.

Reference implementation for abstraction shape and operator semantics:

- Feldera repo: `/home/jlerche/programming_projects/github.com/feldera/feldera`

---

## Epic 1: Public API Contraction

### Task 1.1: [x] Stop exporting the current generic runtime stream algebra as if it were the semantic DBSP API
- [x] Remove or deprecate re-exports of generic `Stream`, `delay`,
  `differentiate`, `integrate`, `incrementalize2`, `stream_elimination`, and
  related generic combinators from
  `crates/dbsp-runtime/src/lib.rs`.
- [x] Remove or deprecate the corresponding re-exports from
  `crates/dbsp/src/lib.rs`.
- [x] Keep exported runtime types focused on the execution substrate:
  handle/ZSet streams, runtime operators, and SQL-facing execution components.
- [x] Audit downstream crate usage to identify call sites that depend on the
  current public generic runtime algebra.
- [x] Add a short public note explaining that the current runtime `Stream<T>`
  is operational and not the paper-denotational stream model.

### Task 1.2: [x] Reduce semantic ambiguity in the current runtime stream surface
- [x] Decide whether `dbsp-runtime::Stream<T>` becomes internal or remains
  public with explicitly operational naming and docs.
- [x] If it remains public temporarily, document it as an execution/storage
  abstraction rather than the paper stream object.
- [x] Ensure no new docs, examples, or APIs describe the current runtime
  stream algebra as "the DBSP stream model".
- [x] Ensure the runtime docs explicitly distinguish:
  - current logical time,
  - committed frontier,
  - semantic horizon,
  - default tail semantics.

---

## Epic 2: Semantic Stream Layer Above Runtime

### Task 2.1: [x] Introduce a new semantic stream/circuit crate above `dbsp-runtime`
- [x] Create a new crate `crates/dbsp-semantic`.
- [x] Make `dbsp-semantic` depend on the semantic/circuit abstraction and lower
  into `dbsp-runtime`; do not make `dbsp-runtime` the owner of paper semantics.
- [x] Define semantic `Stream` in `dbsp-semantic` as an opaque circuit-edge
  abstraction rather than a persisted prefix container.
- [x] Do not expose runtime observations such as `current_time()`,
  `committed_frontier()`, `semantic_horizon()`, or `default_value()` on the
  semantic stream type.

### Task 2.2: [x] Define the compliance contract precisely
- [x] Write a short semantic contract for `dbsp-semantic` based on the DBSP
  paper's total-stream model.
- [x] State which algebraic constraints on values are required for each
  semantic operator.
- [x] State which laws the semantic layer is expected to satisfy for the full
  paper-compliance target.
- [x] Separate semantic guarantees from runtime/execution guarantees.
- [x] Explicitly define non-goals for this sprint:
  - not replacing the current handle/ZSet runtime,
  - not rewriting SQL execution around a new engine,
  - not broad runtime churn before the semantic layer exists.

### Task 2.3: [x] Define semantic value domains and collection semantics
- [x] Define the semantic value families used by DBSP, including:
  - scalar group-valued streams,
  - sets and bags/Z-sets,
  - indexed collections,
  - nested relations and nested collection values.
- [x] Define the semantic laws and equality notion for these value domains.
- [x] Ensure the semantic layer can represent paper-valid infinite streams over
  these domains without resorting to prefix-plus-tail restrictions.

### Task 2.4: [x] Define the semantic circuit model deliberately
- [x] Define semantic `Stream` as an opaque circuit edge.
- [x] Define circuit composition, tupling/product structure, strict operators,
  and feedback as semantic building blocks.
- [x] Define whatever nested or stratified circuit structure is required to
  support recursion and streaming operators in the paper model.
- [x] Keep the semantic API opaque enough that implementation details do not
  leak into user-visible laws.

### Task 2.5: [x] Build a denotational reference evaluator for semantic circuits
- [x] Add a reference evaluator for `dbsp-semantic` that computes observable
  finite prefixes from the denotational semantics.
- [x] Use it as the ground truth for law tests and lowering-equivalence tests.
- [x] Keep this evaluator separate from the lowering/execution runtime so it
  can catch semantic drift rather than reproducing it.

---

## Epic 3: Core Paper Operators

### Task 3.1: [x] Implement pointwise lifting and structural semantic combinators
- [x] Define semantic pointwise lift `↑f` for pure functions on values.
- [x] Implement the structural stream combinators required for DBSP circuit
  composition.
- [x] Ensure lifted operators are extensional and total over their intended
  paper domain.
- [x] Add direct semantic tests for lifted query construction.

### Task 3.2: [x] Implement semantic `delay`
- [x] Add a semantic `delay` operator with paper DBSP behavior.
- [x] Model it as a strict operator / `z^-1`-style primitive, taking
  inspiration from Feldera's
  `crates/dbsp/src/operator/z1.rs` in
  `/home/jlerche/programming_projects/github.com/feldera/feldera`.
- [x] Ensure the operator is total over its intended semantic domain.
- [x] Add law and behavior tests against paper examples.

### Task 3.3: [x] Implement semantic `differentiate`
- [x] Define `differentiate(x) = x - delay(x)` over the semantic stream model.
- [x] Ensure it is total over its intended semantic domain.
- [x] Add tests for standard DBSP identities and representative generated
  streams.
- [x] Confirm it does not depend on runtime concepts like frontiers, horizons,
  or tails.

### Task 3.4: [x] Implement semantic `integrate`
- [x] Implement `integrate` as a semantic feedback construction, not as a
  partial prefix operator.
- [x] Take abstraction inspiration from Feldera's
  `crates/dbsp/src/operator/integrate.rs` in
  `/home/jlerche/programming_projects/github.com/feldera/feldera`.
- [x] Ensure streams such as `1,1,1,1,... -> 1,2,3,4,...` are valid and
  supported at the semantic layer.
- [x] Remove eventual-identity restrictions from the semantic operator.

### Task 3.5: [x] Implement semantic differentiation/integration transforms over DBSP circuits
- [x] Define semantic `D` and `I` over the semantic circuit language, not just
  hand-written stream expressions.
- [x] Support the DBSP incremental construction `QΔ = D ∘ ↑Q ∘ I` for
  arbitrary semantic circuits in the supported language.
- [x] Add tests showing that the transform works beyond toy scalar examples.
- [x] Add a compliance matrix mapping each implemented semantic operator to its
  corresponding paper notion.

---

## Epic 4: Query And Collection Semantics

### Task 4.1: [x] Implement semantic collection operators for sets, bags, and indexed collections
- [x] Define semantic collection operators needed for relational algebra over
  sets and bags/Z-sets.
- [x] Cover at least:
  - map/project,
  - filter/select,
  - union/addition,
  - join,
  - distinct,
  - indexed lookup and indexed join-style composition where required.
- [x] Ensure these operators are specified semantically first and lowered
  second.

### Task 4.2: [x] Implement semantic aggregation
- [x] Define semantic group-by and aggregation operators over collection-valued
  streams.
- [x] Cover the aggregation families needed by the paper-level DBSP claim.
- [x] Add tests for aggregation laws and incrementalization behavior.

### Task 4.3: [x] Implement semantic nested relations and flatmap/unnest
- [x] Define nested relation semantics in the semantic layer.
- [x] Implement flatmap/unnest-style operators over nested collections.
- [x] Add reference examples showing denotational and lowered behavior agree.

### Task 4.4: [x] Implement semantic recursion support
- [x] Add the semantic circuit structure needed for monotonic recursion.
- [x] Add the semantic structure needed for non-monotonic or stratified
  recursion where DBSP claims to support it.
- [x] Define fixed-point/feedback behavior semantically first.
- [x] Add reference tests for recursive examples.

### Task 4.5: [x] Implement semantic streaming and windowed operators required by the paper claim
- [x] Define semantic support for the streaming/windowed operators Floe needs
  in order to claim paper DBSP compliance.
- [x] Add semantic examples and lowered execution tests for these operators.
- [x] Ensure the semantics remain denotational at the API layer even if the
  runtime implementation is operational.

---

## Epic 5: Lowering Into The Existing Runtime

### Task 5.1: [x] Lower semantic operators into the current handle/ZSet execution substrate
- [x] Define a lowering path from `dbsp-semantic` to the existing
  `dbsp-runtime` execution model.
- [x] Reuse the current delta-handle/ZSet runtime for execution and
  persistence.
- [x] Preserve the audited logical-tick semantics already restored in the SQL
  runtime path.
- [x] Avoid reintroducing frontier-collapse or sparse-tick bugs during
  lowering.

### Task 5.2: [x] Lower the full semantic DBSP surface, not only the starter operators
- [x] Ensure lowering covers the semantic stream/circuit primitives.
- [x] Ensure lowering covers collection-valued operators, aggregation, nested
  relations, recursion, and windowed/streaming constructs that are part of the
  paper-compliance claim.
- [x] Add a coverage checklist so unsupported lowered constructs block the
  compliance claim.

### Task 5.3: [x] Keep semantic/runtime concerns separated in code structure
- [x] Ensure runtime operators remain execution-oriented.
- [x] Ensure semantic operators do not expose runtime storage/frontier details.
- [x] Avoid adding new paper-semantic claims to runtime-only modules.
- [x] Ensure the `dbsp` facade distinguishes semantic exports from runtime
  exports explicitly.

---

## Epic 6: Explicit Compliance Validation

### Task 6.1: [x] Add semantic reference tests for paper operators
- [x] Add tests for canonical paper examples using finite observational prefixes
  of generated infinite streams.
- [x] Verify `delay`, `differentiate`, and `integrate` on examples that are not
  eventually constant or eventually identity.
- [x] Add identity/property tests for the semantic layer where practical.
- [x] Confirm the semantic layer is total wherever the paper operator is total
  over the stated domain.

### Task 6.2: [x] Add semantic reference tests for collection/query families
- [x] Add semantic test suites for:
  - relational algebra over sets and bags,
  - aggregation,
  - nested relations,
  - flatmap/unnest,
  - recursion,
  - streaming/windowed operators.
- [x] Ensure these tests are written against the semantic layer first, not only
  against the lowered runtime.

### Task 6.3: [x] Add semantic-vs-lowered equivalence tests
- [x] For the full claimed semantic DBSP surface, verify that semantic-layer
  lowering and the runtime execution substrate produce the same observable
  results over long finite prefixes.
- [x] Include cases with empty logical ticks and long no-op stretches.
- [x] Include restart/recovery-sensitive cases where logical version continuity
  matters downstream.

### Task 6.4: [x] Add explicit compliance documentation
- [x] Document exactly what "paper DBSP compliant" means in Floe after this
  sprint.
- [x] Distinguish the semantic API from the runtime substrate.
- [x] Record which areas are paper-compliant after this sprint and which remain
  follow-up work.
- [x] Include a short statement of what is still not claimed:
  runtime `Stream<T>` is operational, SQL runtime correctness does not by
  itself imply generic paper-stream compliance, and any remaining non-paper
  surfaces are documented explicitly.

### Task 6.5: [x] Publish a paper-compliance matrix
- [x] Create a checklist that maps each paper-level semantic construct and
  claimed query family to:
  - the semantic API entrypoint,
  - the lowering path,
  - the validation tests,
  - any known exclusions.
- [x] Treat any unchecked item as a blocker for the full paper-compliance
  claim.

---

## Definition Of Done

This sprint is done only when all of the following are true:

1. The public `dbsp-runtime` and `dbsp` surfaces no longer present the current
   operational generic stream algebra as the semantic DBSP API.
2. A new `dbsp-semantic` layer exists with an opaque semantic `Stream` type.
3. A denotational reference evaluator exists for the semantic layer.
4. Semantic value domains cover the collection families Floe needs in order to
   claim paper DBSP compliance.
5. Semantic circuit primitives include composition, strict delay, and feedback.
6. Semantic `delay`, `differentiate`, `integrate`, pointwise lifting `↑f`, and
   circuit-level `D`/`I` exist and satisfy the stated paper-level contract.
7. The semantic layer can express `QΔ = D ∘ ↑Q ∘ I` for arbitrary supported
   semantic DBSP circuits.
8. The semantic layer supports the claimed paper-level query families,
   including relational algebra, aggregation, nested relations, flatmap/unnest,
   recursion, and streaming/windowed operators.
9. Lowering from the semantic layer into the existing runtime exists for the
   full claimed semantic surface.
10. Semantic reference tests, lowering-equivalence tests, and the paper
    compliance matrix all pass.
11. The documentation explicitly states what is and is not
    paper-DBSP-compliant.

## Notes on Implementation Order

Suggested execution order to reduce churn:

1. Contract the current public `dbsp-runtime` API.
2. Contract the current public `dbsp` facade API.
3. Introduce the new `dbsp-semantic` crate above runtime.
4. Define semantic value domains and build the denotational reference
   evaluator.
5. Implement semantic circuit primitives and core paper operators.
6. Implement semantic collection/query operators, aggregation, nested
   relations, recursion, and streaming/windowed support.
7. Implement circuit-level `D`/`I` and the DBSP incrementalization transform.
8. Lower the semantic layer into the existing handle/ZSet runtime.
9. Add semantic compliance tests, lowering-equivalence tests, and the paper
   compliance matrix.
10. Publish explicit compliance documentation.

## Non-Goals For This Sprint

- Do not mutate the current storage-backed `dbsp-runtime::Stream<T>` into the
  paper object in place.
- Do not replace the current handle/ZSet execution substrate.
- Do not broaden runtime churn before the public API and semantic-layer split
  are in place.
- Do not claim paper compliance for any construct that is missing from the
  compliance matrix, even if adjacent semantic operators have landed.
