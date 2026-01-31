#!/usr/bin/env python3
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CRITERION_ROOT = ROOT / "target" / "criterion" / "rkyv_arrow_datafusion"

BENCHES = [
    "decode_to_arrow",
    "decode_to_scalar",
    "scalar_eval",
    "datafusion_eval",
    "vectorized_reuse_plan",
    "end_to_end",
    "scalar_end_to_end",
]


def read_estimate(path: Path) -> float:
    data = json.loads(path.read_text())
    return float(data["mean"]["point_estimate"])


def load_results():
    results = {}
    for bench in BENCHES:
        bench_dir = CRITERION_ROOT / bench
        if not bench_dir.exists():
            continue
        for size_dir in bench_dir.iterdir():
            if not size_dir.is_dir():
                continue
            try:
                size = int(size_dir.name)
            except ValueError:
                continue
            estimate_path = size_dir / "new" / "estimates.json"
            if not estimate_path.exists():
                continue
            results.setdefault(size, {})[bench] = read_estimate(estimate_path)
    return results


def ns(value_s: float) -> float:
    return value_s * 1e9


def fmt(value: float) -> str:
    return f"{value:,.2f}"


def main():
    results = load_results()
    if not results:
        print("No Criterion results found. Run the bench first.")
        return

    sizes = sorted(results.keys())
    print("Batch, ns/row decode->arrow, ns/row decode->scalar, ns/row scalar_eval, ns/row df_eval, ns/row vec_reuse, ns/row scalar_e2e, ratio vec_reuse/scalar_e2e, ratio df_eval/scalar_eval")

    for size in sizes:
        row = results[size]
        decode_arrow = row.get("decode_to_arrow")
        decode_scalar = row.get("decode_to_scalar")
        scalar_eval = row.get("scalar_eval")
        df_eval = row.get("datafusion_eval")
        vec_reuse = row.get("vectorized_reuse_plan")
        scalar_e2e = row.get("scalar_end_to_end")

        def per_row(value):
            return ns(value) / size if value is not None else None

        decode_arrow_pr = per_row(decode_arrow)
        decode_scalar_pr = per_row(decode_scalar)
        scalar_eval_pr = per_row(scalar_eval)
        df_eval_pr = per_row(df_eval)
        vec_reuse_pr = per_row(vec_reuse)
        scalar_e2e_pr = per_row(scalar_e2e)

        ratio_vec = vec_reuse / scalar_e2e if vec_reuse and scalar_e2e else None
        ratio_eval = df_eval / scalar_eval if df_eval and scalar_eval else None

        print(
            f"{size},"
            f" {fmt(decode_arrow_pr) if decode_arrow_pr is not None else 'NA'},"
            f" {fmt(decode_scalar_pr) if decode_scalar_pr is not None else 'NA'},"
            f" {fmt(scalar_eval_pr) if scalar_eval_pr is not None else 'NA'},"
            f" {fmt(df_eval_pr) if df_eval_pr is not None else 'NA'},"
            f" {fmt(vec_reuse_pr) if vec_reuse_pr is not None else 'NA'},"
            f" {fmt(scalar_e2e_pr) if scalar_e2e_pr is not None else 'NA'},"
            f" {fmt(ratio_vec) if ratio_vec is not None else 'NA'},"
            f" {fmt(ratio_eval) if ratio_eval is not None else 'NA'}"
        )


if __name__ == "__main__":
    main()
