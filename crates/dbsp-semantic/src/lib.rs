//! Semantic DBSP streams and circuits.
//!
//! # Contract
//!
//! `dbsp-semantic` defines Floe's paper-facing semantic layer.
//! Semantic streams denote total functions from logical time `t in N` to values.
//! The public API is intentionally opaque: it does not expose runtime frontiers,
//! semantic horizons, storage-backed tails, or any other operational state.
//!
//! The claimed semantic value domains are:
//! - scalar abelian-group values,
//! - finite set values with extensional equality,
//! - finite-support bag / Z-set values with normalized weight-map equality,
//! - indexed collections with normalized `K -> ZSet<V>` equality,
//! - nested values formed compositionally from the collection domains.
//!
//! The semantic circuit model supports composition, pointwise lifting,
//! strict delay, guarded feedback, circuit transforms `D` and `I`, and
//! incrementalization `QΔ = D ∘ ↑Q ∘ I`.
//!
//! # Laws And Admissibility
//!
//! For values in the stated domain, the semantic layer satisfies the tested laws:
//! - `delay(x)(0) = 0` and `delay(x)(t + 1) = x(t)`,
//! - `differentiate(x) = x - delay(x)`,
//! - `integrate(x) = x + delay(integrate(x))`,
//! - `D(I(x)) = x`,
//! - `I(D(x)) = x` under the documented zero-initial assumption,
//! - `QΔ = D ∘ ↑Q ∘ I` for the covered circuit families.
//!
//! Recursive semantics are admitted only for guarded feedback. Every cycle in the
//! claimed domain must pass through `delay`; unguarded feedback is rejected by the
//! evaluator as outside the semantic domain.
//!
//! Window semantics are event-time snapshot semantics over integer timestamps.
//! Windows use half-open intervals `[start, end)`. Negative timestamps are
//! unassigned. Watermark and lateness semantics are intentionally out of scope.
//!
//! # Runtime Separation
//!
//! Lowering lives in this crate, but runtime semantics do not. Lowering targets
//! the existing `dbsp-runtime` stream, handle, and versioned Z-set substrate by
//! advancing committed logical-time prefixes one tick at a time.
//! The execution claim is that those committed prefixes, plus reopened prefixes
//! for the covered rows, match the denotational reference model.
//! This crate still does not claim that `dbsp-runtime::stream::Stream<T>` is the
//! denotational DBSP paper object or that provisional future defaults beyond the
//! committed frontier are themselves the semantic infinite tail.
//!
//! # Non-goals
//!
//! - replacing the current handle/Z-set runtime,
//! - rewriting SQL execution around a new engine,
//! - exposing runtime storage/frontier details through the semantic API.

mod circuit;
mod lowering;
mod operators;
mod stream;
mod values;

pub use circuit::{
    Circuit, add_circuit, circuit_d, circuit_i, identity, incrementalize, pointwise, strict_delay,
};
pub use lowering::{
    LoweredScalarStream, LoweredZSetStream, RuntimeValueBounds, collect_runtime_scalar_prefix,
    collect_runtime_zset_prefix, lower_indexed, lower_scalar, lower_set, lower_zset,
};
pub use operators::{
    aggregate_zset, arrange_by, count_by_zset, distinct_zset, filter_set, filter_zset,
    flat_map_zset, join_indexed, join_set, join_zset, lookup_index, map_set, map_zset,
    sliding_window_aggregate, tumbling_window_aggregate, union_set, union_zset, unnest_zset,
};
pub use stream::{
    ReferenceEvaluator, Stream, add, delay, differentiate, feedback, integrate, negate, pair,
    subtract, zip_with,
};
pub use values::{GroupValue, IndexedZSet, RuntimeKeyBounds, Set, Window, ZSet, ZeroValue};

#[cfg(test)]
mod tests;
