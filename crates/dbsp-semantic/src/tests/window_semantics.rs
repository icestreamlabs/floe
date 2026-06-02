use super::*;

#[test]
fn semantic_windows_cover_overlaps_empty_windows_and_changing_prefixes() {
    let snapshots = Stream::from_prefix(
        vec![
            zset::<Event>([]),
            zset([(event(1, 0, 5), 1)]),
            zset([(event(1, 0, 5), 1), (event(1, 4, 7), 1)]),
            zset([
                (event(1, 0, 5), 1),
                (event(1, 4, 7), 1),
                (event(1, 10, 9), 1),
                (event(1, -1, 99), 1),
            ]),
            zset([
                (event(1, 0, 5), 1),
                (event(1, 4, 7), 1),
                (event(1, 10, 9), 1),
                (event(1, -1, 99), 1),
            ]),
        ],
        zset([
            (event(1, 0, 5), 1),
            (event(1, 4, 7), 1),
            (event(1, 10, 9), 1),
            (event(1, -1, 99), 1),
        ]),
    );
    let sliding = sliding_window_aggregate(
        &snapshots,
        |event| Some(event.user),
        |event| Some(event.ts),
        |_window, rows| Some(rows.iter().map(|(_, weight)| *weight).sum::<i64>()),
        10,
        5,
    );
    let tumbling = tumbling_window_aggregate(
        &snapshots,
        |event| Some(event.user),
        |event| Some(event.ts),
        |_window, rows| Some(rows.iter().map(|(_, weight)| *weight).sum::<i64>()),
        10,
    );

    assert_eq!(
        observe(&sliding, 5),
        vec![
            zset::<(Window<i64>, i64)>([]),
            zset([window_count(
                Window {
                    key: 1_i64,
                    start: 0,
                    end: 10,
                },
                1,
            )]),
            zset([window_count(
                Window {
                    key: 1_i64,
                    start: 0,
                    end: 10,
                },
                2,
            )]),
            zset([
                window_count(
                    Window {
                        key: 1_i64,
                        start: 0,
                        end: 10,
                    },
                    2,
                ),
                window_count(
                    Window {
                        key: 1_i64,
                        start: 5,
                        end: 15,
                    },
                    1,
                ),
                window_count(
                    Window {
                        key: 1_i64,
                        start: 10,
                        end: 20,
                    },
                    1,
                ),
            ]),
            zset([
                window_count(
                    Window {
                        key: 1_i64,
                        start: 0,
                        end: 10,
                    },
                    2,
                ),
                window_count(
                    Window {
                        key: 1_i64,
                        start: 5,
                        end: 15,
                    },
                    1,
                ),
                window_count(
                    Window {
                        key: 1_i64,
                        start: 10,
                        end: 20,
                    },
                    1,
                ),
            ]),
        ]
    );
    assert_eq!(
        observe(&tumbling, 5),
        vec![
            zset::<(Window<i64>, i64)>([]),
            zset([window_count(
                Window {
                    key: 1_i64,
                    start: 0,
                    end: 10,
                },
                1,
            )]),
            zset([window_count(
                Window {
                    key: 1_i64,
                    start: 0,
                    end: 10,
                },
                2,
            )]),
            zset([
                window_count(
                    Window {
                        key: 1_i64,
                        start: 0,
                        end: 10,
                    },
                    2,
                ),
                window_count(
                    Window {
                        key: 1_i64,
                        start: 10,
                        end: 20,
                    },
                    1,
                ),
            ]),
            zset([
                window_count(
                    Window {
                        key: 1_i64,
                        start: 0,
                        end: 10,
                    },
                    2,
                ),
                window_count(
                    Window {
                        key: 1_i64,
                        start: 10,
                        end: 20,
                    },
                    1,
                ),
            ]),
        ]
    );
}

#[test]
fn semantic_windows_cover_duplicates_and_retractions() {
    let snapshots = Stream::from_prefix(
        vec![
            zset([(event(1, 1, 5), 2), (event(1, 6, 7), 1)]),
            zset([(event(1, 1, 5), 1), (event(1, 6, 7), 1)]),
            zset([(event(1, 1, 5), -1), (event(1, 6, 7), 1)]),
        ],
        zset([(event(1, 1, 5), -1), (event(1, 6, 7), 1)]),
    );
    let tumbling = tumbling_window_aggregate(
        &snapshots,
        |event| Some(event.user),
        |event| Some(event.ts),
        |_window, rows| Some(rows.iter().map(|(_, weight)| *weight).sum::<i64>()),
        5,
    );

    assert_eq!(
        observe(&tumbling, 3),
        vec![
            zset([
                window_count(
                    Window {
                        key: 1_i64,
                        start: 0,
                        end: 5,
                    },
                    2,
                ),
                window_count(
                    Window {
                        key: 1_i64,
                        start: 5,
                        end: 10,
                    },
                    1,
                ),
            ]),
            zset([
                window_count(
                    Window {
                        key: 1_i64,
                        start: 0,
                        end: 5,
                    },
                    1,
                ),
                window_count(
                    Window {
                        key: 1_i64,
                        start: 5,
                        end: 10,
                    },
                    1,
                ),
            ]),
            zset([
                window_count(
                    Window {
                        key: 1_i64,
                        start: 0,
                        end: 5,
                    },
                    -1,
                ),
                window_count(
                    Window {
                        key: 1_i64,
                        start: 5,
                        end: 10,
                    },
                    1,
                ),
            ]),
        ]
    );
}
