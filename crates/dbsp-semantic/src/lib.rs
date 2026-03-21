//! Semantic DBSP streams and circuits.
//!
//! # Contract
//!
//! `dbsp-semantic` defines the paper-facing semantic layer for Floe.
//! Semantic streams denote total functions from logical time `t in N` to
//! values. The semantic API is intentionally opaque: it does not expose
//! runtime observations such as committed frontiers, semantic horizons, or
//! storage-backed tails.
//!
//! The semantic value domain covers:
//! - scalar group-valued streams,
//! - finite set values,
//! - bag/Z-set values,
//! - indexed collections,
//! - nested relations formed compositionally from the collection types.
//!
//! The semantic circuit model supports composition, pointwise lifting,
//! strict delay, and guarded feedback. `differentiate(x)` is defined as
//! `x - delay(x)`. `integrate(x)` is defined through semantic feedback and is
//! total for all `GroupValue` streams, including streams that are not
//! eventually identity.
//!
//! # Laws
//!
//! For values in the stated domain, the semantic layer is expected to satisfy:
//! - extensional equality over finite observations,
//! - `delay(x)(0) = 0` and `delay(x)(t + 1) = x(t)`,
//! - `differentiate(x) = x - delay(x)`,
//! - `integrate(x) = x + delay(integrate(x))`,
//! - circuit incrementalization `QΔ = D ∘ ↑Q ∘ I` for supported circuits.
//!
//! # Runtime Separation
//!
//! Lowering lives in this crate, but runtime semantics do not. Lowering targets
//! the existing `dbsp-runtime` handle and Z-set substrate by materializing the
//! requested observational prefix into runtime streams and versioned Z-sets.
//! This crate does not claim that `dbsp-runtime::stream::Stream<T>` is the
//! denotational DBSP paper object.
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
    LoweredZSetStream, RuntimeValueBounds, collect_runtime_scalar_prefix,
    collect_runtime_zset_prefix, lower_indexed_prefix, lower_scalar_prefix, lower_set_prefix,
    lower_zset_prefix,
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
