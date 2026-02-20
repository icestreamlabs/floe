#!/usr/bin/env python3
import argparse
import json
import re
from pathlib import Path


BENCH_GROUPS = {
    "update_model_indexed": [
        "indexed_zset_apply_toggle",
        "arrow_overlay_append_toggle",
        "indexed_zset_apply_lookup_hot_key",
        "arrow_overlay_append_lookup_hot_key",
    ],
    "update_model_versioned": [
        "versioned_zset_write_materialize_toggle",
        "arrow_ledger_write_materialize_toggle",
    ],
    "update_model_dictionary": [
        "dictionary_intern_existing_rows",
        "dictionary_intern_new_rows",
    ],
}


def read_estimate_ns(path: Path) -> float:
    payload = json.loads(path.read_text())
    return float(payload["mean"]["point_estimate"])


def load_group_results(criterion_root: Path, group_name: str, bench_names: list[str]) -> dict[int, dict[str, float]]:
    group_dir = criterion_root / group_name
    results: dict[int, dict[str, float]] = {}
    if not group_dir.exists():
        return results

    for bench_name in bench_names:
        bench_dir = group_dir / bench_name
        if not bench_dir.exists():
            continue
        for size_dir in bench_dir.iterdir():
            if not size_dir.is_dir():
                continue
            try:
                batch_size = int(size_dir.name)
            except ValueError:
                continue
            estimate = size_dir / "new" / "estimates.json"
            if not estimate.exists():
                continue
            results.setdefault(batch_size, {})[bench_name] = read_estimate_ns(estimate)
    return results


def parse_size_reports(log_path: Path) -> dict[int, dict[str, float]]:
    reports: dict[int, dict[str, float]] = {}
    pattern = re.compile(r"^update_model_size_report,(.*)$")
    for line in log_path.read_text().splitlines():
        match = pattern.match(line.strip())
        if not match:
            continue
        kv = {}
        for part in match.group(1).split(","):
            key, value = part.split("=", 1)
            kv[key] = value
        batch_size = int(kv["batch_size"])
        reports[batch_size] = {
            "rkyv_delta_total_bytes": float(kv["rkyv_delta_total_bytes"]),
            "arrow_delta_total_bytes": float(kv["arrow_delta_total_bytes"]),
            "arrow_over_rkyv": float(kv["arrow_over_rkyv"]),
        }
    return reports


def us(ns: float) -> float:
    return ns / 1_000.0


def ms(ns: float) -> float:
    return ns / 1_000_000.0


def fmt(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}"


def print_indexed(results: dict[int, dict[str, float]]) -> None:
    print("INDEXED (lower is better)")
    print(
        "batch,zset_toggle_us,overlay_toggle_us,overlay_speedup,"
        "zset_lookup_us,overlay_lookup_us,overlay_speedup_lookup"
    )
    for batch_size in sorted(results.keys()):
        row = results[batch_size]
        zset_toggle = row.get("indexed_zset_apply_toggle")
        overlay_toggle = row.get("arrow_overlay_append_toggle")
        zset_lookup = row.get("indexed_zset_apply_lookup_hot_key")
        overlay_lookup = row.get("arrow_overlay_append_lookup_hot_key")
        if None in (zset_toggle, overlay_toggle, zset_lookup, overlay_lookup):
            continue
        toggle_speedup = zset_toggle / overlay_toggle
        lookup_speedup = zset_lookup / overlay_lookup
        print(
            f"{batch_size},"
            f"{fmt(us(zset_toggle))},"
            f"{fmt(us(overlay_toggle))},"
            f"{fmt(toggle_speedup)},"
            f"{fmt(us(zset_lookup))},"
            f"{fmt(us(overlay_lookup))},"
            f"{fmt(lookup_speedup)}"
        )
    print()


def print_versioned(results: dict[int, dict[str, float]]) -> None:
    print("VERSIONED (lower is better)")
    print("batch,versioned_zset_us,arrow_ledger_us,arrow_speedup")
    for batch_size in sorted(results.keys()):
        row = results[batch_size]
        versioned = row.get("versioned_zset_write_materialize_toggle")
        ledger = row.get("arrow_ledger_write_materialize_toggle")
        if None in (versioned, ledger):
            continue
        speedup = versioned / ledger
        print(
            f"{batch_size},"
            f"{fmt(us(versioned))},"
            f"{fmt(us(ledger))},"
            f"{fmt(speedup)}"
        )
    print()


def print_dictionary(results: dict[int, dict[str, float]]) -> None:
    print("DICTIONARY (intern)")
    print("batch,existing_us,new_ms,new_over_existing")
    for batch_size in sorted(results.keys()):
        row = results[batch_size]
        existing = row.get("dictionary_intern_existing_rows")
        new = row.get("dictionary_intern_new_rows")
        if None in (existing, new):
            continue
        ratio = new / existing
        print(
            f"{batch_size},"
            f"{fmt(us(existing))},"
            f"{fmt(ms(new), 3)},"
            f"{fmt(ratio)}"
        )
    print()


def print_sizes(reports: dict[int, dict[str, float]]) -> None:
    if not reports:
        return
    print("DELTA PAYLOAD SIZE")
    print("batch,rkyv_total_bytes,arrow_total_bytes,arrow_over_rkyv")
    for batch_size in sorted(reports.keys()):
        row = reports[batch_size]
        print(
            f"{batch_size},"
            f"{fmt(row['rkyv_delta_total_bytes'], 0)},"
            f"{fmt(row['arrow_delta_total_bytes'], 0)},"
            f"{fmt(row['arrow_over_rkyv'])}"
        )
    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Summarize update_model_storage Criterion results."
    )
    parser.add_argument(
        "--criterion-root",
        type=Path,
        default=Path(__file__).resolve().parents[3] / "target" / "criterion",
        help="Path to Criterion results root (default: <repo>/target/criterion).",
    )
    parser.add_argument(
        "--log",
        type=Path,
        default=None,
        help="Optional path to raw cargo bench output (for size_report rows).",
    )
    args = parser.parse_args()

    indexed = load_group_results(
        args.criterion_root, "update_model_indexed", BENCH_GROUPS["update_model_indexed"]
    )
    versioned = load_group_results(
        args.criterion_root, "update_model_versioned", BENCH_GROUPS["update_model_versioned"]
    )
    dictionary = load_group_results(
        args.criterion_root, "update_model_dictionary", BENCH_GROUPS["update_model_dictionary"]
    )

    if not indexed and not versioned and not dictionary:
        print("No update_model_storage Criterion results found. Run the benchmark first.")
        return

    size_reports = {}
    if args.log is not None:
        if not args.log.exists():
            raise SystemExit(f"log file not found: {args.log}")
        size_reports = parse_size_reports(args.log)

    print_sizes(size_reports)
    print_indexed(indexed)
    print_versioned(versioned)
    print_dictionary(dictionary)


if __name__ == "__main__":
    main()
