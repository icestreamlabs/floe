#!/usr/bin/env bash
set -euo pipefail

WARNING_MIN=1001
WARNING_MAX=1200
HARD_CAP=1300

repo_root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$repo_root"

if ! command -v rg >/dev/null 2>&1; then
  echo "error: ripgrep (rg) is required for this checker" >&2
  exit 1
fi

# Exclude non-production Rust paths for LOC policy checks.
exclude_re='(^|/)(tests?|benches|examples|target|generated|gen|out)($|/)'

warning_lines=()
hard_lines=()

while IFS= read -r rust_file; do
  if [[ "$rust_file" =~ $exclude_re ]]; then
    continue
  fi

  loc="$(wc -l < "$rust_file" | tr -d ' ')"
  if (( loc > HARD_CAP )); then
    hard_lines+=("  - ${rust_file} (${loc})")
  elif (( loc >= WARNING_MIN && loc <= WARNING_MAX )); then
    warning_lines+=("  - ${rust_file} (${loc})")
  fi
done < <(rg --files -g '*.rs')

echo "Repo Hygiene Report"
echo "Policy:"
echo "  warning band: ${WARNING_MIN}-${WARNING_MAX} LOC"
echo "  hard cap: >${HARD_CAP} LOC"
echo

echo "Warning band files:"
if ((${#warning_lines[@]} == 0)); then
  echo "  - none"
else
  printf "%s\n" "${warning_lines[@]}"
fi
echo

echo "Hard-cap violations:"
if ((${#hard_lines[@]} == 0)); then
  echo "  - none"
else
  printf "%s\n" "${hard_lines[@]}"
fi
