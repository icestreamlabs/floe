#!/usr/bin/env bash
set -euo pipefail

cargo fmt --all
cargo check --workspace
cargo clippy --workspace -- -D warnings
cargo test --workspace --no-run
cargo test --workspace
