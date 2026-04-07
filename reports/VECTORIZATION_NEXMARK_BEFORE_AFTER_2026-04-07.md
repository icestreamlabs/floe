# Vectorization Refactor Nexmark Before/After (2026-04-07)

## Comparison Setup

- Baseline run ID: `1774816107830`
  - branch/commit: `main` / `cc52b35e461e8b7362f3e773c0eca3bc45ace084`
  - artifact: `target/third_party_engine_benchmarks_nexmark/1774816107830/summary.md`
- Current run ID: `1775581340198`
  - branch/commit: `vectorize-pass-1` / `088abc747f398a1965f4138b6615da53305af275`
  - artifact: `target/third_party_engine_benchmarks_nexmark/1775581340198/summary.md`
- Scope: Floe only, Nexmark `all/all`, `bid=1,000,000`.

## Headline

- Improved queries: `9`
- Regressed queries: `11`
- Unchanged queries: `1`
- Mean input rows/s: `223550 -> 241464` (`+8.01%`)

## Queries To Work Toward (Regressions)

### Higher-priority regressions (worse than -5%)

| Query | Before Rows/s | After Rows/s | Delta | Delta % |
| --- | ---: | ---: | ---: | ---: |
| q12 | 138045 | 97030 | -41015 | -29.71% |
| q5 | 136407 | 100351 | -36056 | -26.43% |
| q4 | 333773 | 301222 | -32551 | -9.75% |
| q20 | 330173 | 301312 | -28861 | -8.74% |
| q3 | 100000 | 93896 | -6104 | -6.10% |

### Lower-priority regressions (-5% to 0%)

| Query | Before Rows/s | After Rows/s | Delta | Delta % |
| --- | ---: | ---: | ---: | ---: |
| q9 | 333663 | 324445 | -9218 | -2.76% |
| q17 | 274423 | 270855 | -3568 | -1.30% |
| q14 | 343524 | 339443 | -4081 | -1.19% |
| q1 | 341296 | 338983 | -2313 | -0.68% |
| q0 | 305716 | 304692 | -1024 | -0.34% |
| q2 | 339558 | 338868 | -690 | -0.20% |

## Full Per-Query Delta

| Query | Before Rows/s | After Rows/s | Delta | Delta % |
| --- | ---: | ---: | ---: | ---: |
| q0 | 305716 | 304692 | -1024 | -0.34% |
| q1 | 341296 | 338983 | -2313 | -0.68% |
| q2 | 339558 | 338868 | -690 | -0.20% |
| q3 | 100000 | 93896 | -6104 | -6.10% |
| q4 | 333773 | 301222 | -32551 | -9.75% |
| q5 | 136407 | 100351 | -36056 | -26.43% |
| q6 | 331800 | 333223 | +1423 | +0.43% |
| q7 | 137230 | 137684 | +454 | +0.33% |
| q8 | 21276 | 22421 | +1145 | +5.38% |
| q9 | 333663 | 324445 | -9218 | -2.76% |
| q12 | 138045 | 97030 | -41015 | -29.71% |
| q13 | 80645 | 330497 | +249852 | +309.82% |
| q14 | 343524 | 339443 | -4081 | -1.19% |
| q15 | 25164 | 123731 | +98567 | +391.70% |
| q16 | 271591 | 275103 | +3512 | +1.29% |
| q17 | 274423 | 270855 | -3568 | -1.30% |
| q18 | 123900 | 260552 | +136652 | +110.29% |
| q19 | 138370 | 163692 | +25322 | +18.30% |
| q20 | 330173 | 301312 | -28861 | -8.74% |
| q21 | 280583 | 305343 | +24760 | +8.82% |
| q22 | 307408 | 307408 | +0 | +0.00% |
