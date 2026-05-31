#!/usr/bin/env bash
set -euo pipefail

cargo test -p dbsp-runtime operators::
cargo test -p dbsp-runtime stream::tests::core::compaction::
cargo test -p dbsp-runtime collections::arrow_indexed_batch_zset::tests::
cargo test -p floe-executor operators::mv_sink::tests::
cargo test -p floe-executor mv_changelog::tests::
cargo test -p floe-executor --test dbsp_graph_builder
cargo test -p floe-executor --test plan_validation
cargo bench -p dbsp --bench incrementality_evidence --no-run
