# Benchmark Baselines

This file records the current benchmark baselines used for performance work.
The comparison metric is `Result Ready Rows/s` from `nexmark_cross_engine_compare`.
A query is considered acceptable when it is at least 95% of the listed baseline.

## Kafka Append-Only Nexmark

- Baseline run: `1779915284118`
- Baseline commit: `b4c8036` on `main`
- Current comparison run: `1781470811475`
- Harness: `target/release/nexmark_cross_engine_compare floe nexmark_all`

| Query | Baseline | 95% target | Current | Status |
| --- | ---: | ---: | ---: | --- |
| q0 | 646412 | 614092 | 656598 | pass |
| q1 | 786163 | 746855 | 802568 | pass |
| q2 | 823045 | 781893 | 821018 | pass |
| q3 | 96153 | 91346 | 94339 | pass |
| q4 | 749814 | 712324 | 593768 | fail |
| q5 | 495294 | 470530 | 630914 | pass |
| q6 | 417010 | 396160 | 509072 | pass |
| q7 | 807754 | 767367 | 814332 | pass |
| q8 | 92592 | 87963 | 84033 | fail |
| q9 | 469986 | 446487 | 392080 | fail |
| q12 | 813008 | 772358 | 816993 | pass |
| q13 | 420307 | 399292 | 759398 | pass |
| q14 | 814332 | 773616 | 793650 | pass |
| q15 | 383435 | 364264 | 821018 | pass |
| q16 | 819000 | 778050 | 807754 | pass |
| q17 | 800640 | 760608 | 443458 | fail |
| q18 | 253678 | 240995 | 311138 | pass |
| q19 | 149611 | 142131 | 345781 | pass |
| q20 | 535241 | 508479 | 591334 | pass |
| q21 | 434593 | 412864 | 823723 | pass |
| q22 | 665335 | 632069 | 820344 | pass |

Current append-only gaps to close before moving to CDC: q17, q4, q9, and q8.
q8 is a small 10k-row query, so treat it as lower priority than the 1M-row gaps.

## Postgres CDC Nexmark

- Baseline run: `1781454251977`
- Baseline commit: `301e091`
- Current comparison run: `1781467016483`
- Harness: Postgres CDC Nexmark mode in `nexmark_cross_engine_compare`

The current CDC run had harness setup failures for q1, q7, q8, q9, q12, and q13.
Those queries need a clean rerun before judging them.

| Query | Baseline | 95% target | Current | Status |
| --- | ---: | ---: | ---: | --- |
| q0 | 82603 | 78473 | 73389 | fail |
| q1 | 96116 | 91311 | n/a | rerun |
| q2 | 111433 | 105862 | 108026 | pass |
| q3 | 102396 | 97277 | 95229 | fail |
| q4 | 62782 | 59643 | 58194 | fail |
| q5 | 46513 | 44188 | 42398 | fail |
| q6 | 64004 | 60804 | 58654 | fail |
| q7 | 105977 | 100679 | n/a | rerun |
| q8 | 96237 | 91426 | n/a | rerun |
| q9 | 54758 | 52021 | n/a | rerun |
| q12 | 95877 | 91084 | n/a | rerun |
| q13 | 60745 | 57708 | n/a | rerun |
| q14 | 112082 | 106478 | 95356 | fail |
| q15 | 81967 | 77869 | 71429 | fail |
| q16 | 60390 | 57371 | 53871 | fail |
| q17 | 53245 | 50583 | 49868 | fail |
| q18 | 58965 | 56017 | 56363 | pass |
| q19 | 5979 | 5681 | 5551 | fail |
| q20 | 75993 | 72194 | 67741 | fail |
| q21 | 80932 | 76886 | 71423 | fail |
| q22 | 97895 | 93001 | 89397 | fail |

CDC status is provisional until a clean sweep completes. After Kafka append-only is within 5%,
rerun the CDC sweep and update the `Current` column before optimizing from these numbers.
