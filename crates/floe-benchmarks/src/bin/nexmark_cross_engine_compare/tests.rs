use super::*;

fn explicit_q5_fingerprint(bid_rows: u64) -> ContentFingerprint {
    let mut lines = Vec::new();
    for bid_idx in 1..=bid_rows {
        let auction = (bid_idx - 1) % NEXMARK_BID_AUCTION_CARDINALITY + 1;
        for _ in 0..5 {
            lines.push(format!("{auction}\t1"));
        }
    }
    fingerprint_lines(lines)
}

#[test]
fn deterministic_q5_fingerprint_matches_explicit_rows() {
    for bid_rows in [0, 1, 2, 10_000, 10_001] {
        assert_eq!(
            deterministic_nexmark_q5_fingerprint(bid_rows),
            explicit_q5_fingerprint(bid_rows)
        );
    }
}

#[test]
fn floe_validation_queries_use_supported_floe_surface_for_string_queries() {
    for query_id in ["q14", "q15", "q16", "q17", "q21", "q22"] {
        let query = floe_expected_query_text_for_source_tables(query_id, &[Source::Bid])
            .expect("validation query");
        let lower = query.to_ascii_lowercase();
        assert!(!lower.contains("substr("), "{query_id}: {query}");
        assert!(!lower.contains("split_part("), "{query_id}: {query}");
    }
}
