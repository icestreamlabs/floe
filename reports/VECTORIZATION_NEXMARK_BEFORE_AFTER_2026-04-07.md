# Vectorization Refactor Nexmark Before/After (2026-04-07)

## Comparison Setup

- Baseline run ID: `1774816107830`
  - branch/commit: `main` / `cc52b35e461e8b7362f3e773c0eca3bc45ace084`
  - artifact: `target/third_party_engine_benchmarks_nexmark/1774816107830/summary.md`
- Current run ID: `1775657330044`
  - branch/commit: `main` / `6a5d48863758f765d53040a1769b66c5c4408246` + local working tree changes
  - artifact: `target/third_party_engine_benchmarks_nexmark/1775657330044/summary.md`
- Scope: Floe only, Nexmark `all/all`, `bid=1,000,000`.

## Headline

- Improved queries: `8`
- Regressed queries: `13`
- Unchanged queries: `0`
- Mean input rows/s: `223550 -> 240242` (`+7.47%`)

## Queries To Work Toward (Regressions)

### Higher-priority regressions (worse than -5%)

| Query | Before Rows/s | After Rows/s | Delta | Delta % |
| --- | ---: | ---: | ---: | ---: |
| q14 | 343524 | 299311 | -44213 | -12.87% |
| q6 | 331800 | 289564 | -42236 | -12.73% |
| q20 | 330173 | 290229 | -39944 | -12.10% |
| q9 | 333663 | 294632 | -39031 | -11.70% |
| q12 | 138045 | 123441 | -14604 | -10.58% |
| q7 | 137230 | 128650 | -8580 | -6.25% |
| q5 | 136407 | 128369 | -8038 | -5.89% |

### Lower-priority regressions (-5% to 0%)

| Query | Before Rows/s | After Rows/s | Delta | Delta % |
| --- | ---: | ---: | ---: | ---: |
| q2 | 339558 | 327761 | -11797 | -3.47% |
| q3 | 100000 | 97560 | -2440 | -2.44% |
| q0 | 305716 | 298685 | -7031 | -2.30% |
| q1 | 341296 | 334448 | -6848 | -2.01% |
| q22 | 307408 | 301659 | -5749 | -1.87% |
| q4 | 333773 | 331039 | -2734 | -0.82% |

## Full Per-Query Delta

| Query | Before Rows/s | After Rows/s | Delta | Delta % |
| --- | ---: | ---: | ---: | ---: |
| q0 | 305716 | 298685 | -7031 | -2.30% |
| q1 | 341296 | 334448 | -6848 | -2.01% |
| q2 | 339558 | 327761 | -11797 | -3.47% |
| q3 | 100000 | 97560 | -2440 | -2.44% |
| q4 | 333773 | 331039 | -2734 | -0.82% |
| q5 | 136407 | 128369 | -8038 | -5.89% |
| q6 | 331800 | 289564 | -42236 | -12.73% |
| q7 | 137230 | 128650 | -8580 | -6.25% |
| q8 | 21276 | 22421 | +1145 | +5.38% |
| q9 | 333663 | 294632 | -39031 | -11.70% |
| q12 | 138045 | 123441 | -14604 | -10.58% |
| q13 | 80645 | 328348 | +247703 | +307.15% |
| q14 | 343524 | 299311 | -44213 | -12.87% |
| q15 | 25164 | 178507 | +153343 | +609.37% |
| q16 | 271591 | 295945 | +24354 | +8.97% |
| q17 | 274423 | 294550 | +20127 | +7.33% |
| q18 | 123900 | 231696 | +107796 | +87.00% |
| q19 | 138370 | 148500 | +10130 | +7.32% |
| q20 | 330173 | 290229 | -39944 | -12.10% |
| q21 | 280583 | 299760 | +19177 | +6.83% |
| q22 | 307408 | 301659 | -5749 | -1.87% |
