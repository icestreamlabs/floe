use super::*;

#[test]
fn build_postgres_cdc_commit_orders_slots() {
    let mut slots = HashMap::new();
    slots.insert("z_slot".to_string(), (10_u64, "0/0000000A".to_string()));
    slots.insert("a_slot".to_string(), (3_u64, "0/00000003".to_string()));
    let commit = build_postgres_cdc_commit(7, &slots);
    assert_eq!(commit.tick_id, 7);
    assert_eq!(commit.slots.len(), 2);
    assert_eq!(commit.slots[0].slot, "a_slot");
    assert_eq!(commit.slots[1].slot, "z_slot");
}

#[test]
fn compute_global_watermark_uses_min_of_active_sources() {
    let now = Instant::now();
    let mut source_watermarks = HashMap::new();
    source_watermarks.insert("s1".to_string(), 5_000);
    source_watermarks.insert("s2".to_string(), 3_000);

    let mut source_last_seen = HashMap::new();
    source_last_seen.insert("s1".to_string(), now);
    source_last_seen.insert("s2".to_string(), now);

    assert_eq!(
        compute_global_watermark(
            &source_watermarks,
            &source_last_seen,
            now,
            Duration::from_secs(30),
        ),
        Some(3_000)
    );
}

#[test]
fn compute_global_watermark_skips_idle_sources() {
    let now = Instant::now();
    let mut source_watermarks = HashMap::new();
    source_watermarks.insert("active".to_string(), 9_000);
    source_watermarks.insert("idle".to_string(), 1_000);

    let mut source_last_seen = HashMap::new();
    source_last_seen.insert("active".to_string(), now);
    source_last_seen.insert("idle".to_string(), now - Duration::from_secs(60));

    assert_eq!(
        compute_global_watermark(
            &source_watermarks,
            &source_last_seen,
            now,
            Duration::from_secs(30),
        ),
        Some(9_000)
    );
}

#[test]
fn advance_global_watermark_is_monotonic() {
    assert_eq!(advance_global_watermark(5_000, Some(4_000)), 5_000);
    assert_eq!(advance_global_watermark(5_000, Some(7_000)), 7_000);
    assert_eq!(advance_global_watermark(5_000, None), 5_000);
}
