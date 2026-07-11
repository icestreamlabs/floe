#!/usr/bin/env bash
set -euo pipefail

cargo test -p dbsp-runtime stream::tests::core::compaction::
cargo test -p dbsp-runtime collections::arrow_indexed_batch_zset::tests::
cargo test -p dbsp-runtime collections::columnar_indexed_zset::tests::
cargo test -p floe-executor mv_changelog::tests::
cargo test -p floe-executor --test plan_validation
