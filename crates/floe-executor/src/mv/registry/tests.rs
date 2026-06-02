use dbsp::handles::ZSetHandle;

use super::*;

fn encoded_i64_row(value: i64) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(4 + 1 + 8);
    encoded.extend_from_slice(&(1_u32).to_le_bytes());
    encoded.push(0x01);
    encoded.extend_from_slice(&value.to_le_bytes());
    encoded
}

#[test]
fn registers_and_updates_view_state() {
    let registry = MaterializedViewRegistry::new();
    let view = registry.register("mv_test");
    let row = encoded_i64_row(1);
    view.apply_encoded_row(&row, 1);
    assert_eq!(view.snapshot_encoded().get(&row), Some(&1));
    view.apply_encoded_row(&row, -1);
    assert!(view.snapshot_encoded().is_empty());
    view.update_watermark(42);
    assert_eq!(view.watermark(), Some(42));
}

#[test]
fn registry_returns_same_handle() {
    let registry = MaterializedViewRegistry::new();
    let view_a = registry.register("mv");
    let view_b = registry.get("mv").expect("view registered");
    assert!(Arc::ptr_eq(&view_a, &view_b));
}

#[test]
fn retention_prunes_old_versions() {
    let registry = MaterializedViewRegistry::new_with_retention(Some(2));
    let view = registry.register("mv_retained");

    view.publish_version(
        1,
        ZSetHandle {
            ns: "mv_retained".to_string(),
            version: 1,
        },
    );
    view.publish_version(
        2,
        ZSetHandle {
            ns: "mv_retained".to_string(),
            version: 2,
        },
    );
    view.publish_version(
        3,
        ZSetHandle {
            ns: "mv_retained".to_string(),
            version: 3,
        },
    );

    assert!(view.handle_for_version(1).is_none());
    assert!(view.handle_for_version(2).is_some());
    assert!(view.handle_for_version(3).is_some());
}

#[test]
fn resolves_latest_handle_at_or_before_published_version() {
    let registry = MaterializedViewRegistry::new();
    let view = registry.register("mv_version_lookup");

    view.publish_version(
        1,
        ZSetHandle {
            ns: "mv_version_lookup".to_string(),
            version: 10,
        },
    );
    view.publish_logical_version(2);
    view.publish_logical_version(3);

    assert!(view.is_version_published(2));
    assert_eq!(
        view.handle_at_or_before_version(3),
        Some(ZSetHandle {
            ns: "mv_version_lookup".to_string(),
            version: 10,
        })
    );
}

#[test]
fn compact_overlay_batches_advances_base_version() {
    let registry = MaterializedViewRegistry::new();
    let view = registry.register("mv_overlay");

    view.append_encoded_overlay_batch(1, vec![(b"k1".to_vec(), 1)]);
    view.append_encoded_overlay_batch(2, vec![(b"k2".to_vec(), 1)]);
    view.append_encoded_overlay_batch(3, vec![(b"k3".to_vec(), 1)]);

    let stats = view.compact_encoded_overlay_up_to(2);
    assert_eq!(stats.removed_batches, 2);
    assert_eq!(stats.remaining_batches, 1);
    assert_eq!(stats.remaining_rows, 1);

    let (base_version, target_version, overlay) = view
        .encoded_overlay_batches(None)
        .expect("remaining overlay");
    assert_eq!(base_version, 2);
    assert_eq!(target_version, 3);
    assert_eq!(overlay, vec![(b"k3".to_vec(), 1)]);
}

#[test]
fn authoritative_row_count_tracks_encoded_state_batches() {
    let registry = MaterializedViewRegistry::new();
    let view = registry.register("mv_count");
    view.mark_state_authoritative();
    view.publish_logical_version(1);

    let key = encoded_i64_row(1);
    view.apply_encoded_state_batch(1, &[(key.clone(), 1)])
        .expect("apply first delta");
    assert_eq!(view.authoritative_row_count(), Some(1));
    assert_eq!(view.authoritative_row_count_for(1), Some(1));

    view.apply_encoded_state_batch(2, &[(key, -1)])
        .expect("apply delete delta");
    assert_eq!(view.authoritative_row_count(), Some(0));
    assert_eq!(view.authoritative_row_count_for(1), Some(1));

    view.publish_logical_version(2);
    assert_eq!(view.authoritative_row_count_for(1), None);
    assert_eq!(view.authoritative_row_count_for(2), Some(0));
}

#[test]
fn authoritative_row_count_tracks_consolidated_encoded_state_batches() {
    let registry = MaterializedViewRegistry::new();
    let view = registry.register("mv_consolidated_count");
    view.mark_state_authoritative();
    view.publish_logical_version(1);

    let first = encoded_i64_row(1);
    let second = encoded_i64_row(2);
    view.apply_consolidated_encoded_state_batch(1, &[(first.clone(), 1), (second, 2)])
        .expect("apply consolidated batch");
    assert_eq!(view.authoritative_row_count(), Some(3));
    assert_eq!(view.snapshot_encoded().get(&first), Some(&1));

    view.apply_consolidated_encoded_state_batch(2, &[(first.clone(), -1)])
        .expect("apply consolidated delete");
    assert_eq!(view.authoritative_row_count(), Some(2));
    assert!(!view.snapshot_encoded().contains_key(&first));
}

#[test]
fn seeds_authoritative_row_count_only_for_latest_version() {
    let registry = MaterializedViewRegistry::new();
    let view = registry.register("mv_seed_count");
    view.publish_logical_version(7);

    assert!(view.seed_authoritative_row_count_if_latest(7, 3));
    assert_eq!(view.authoritative_row_count(), Some(3));
    assert_eq!(view.authoritative_row_count_for(7), Some(3));

    view.mark_state_non_authoritative();
    assert!(!view.seed_authoritative_row_count_if_latest(6, 2));
    assert_eq!(view.authoritative_row_count(), None);
}

#[test]
fn cached_row_count_does_not_mark_state_authoritative() {
    let registry = MaterializedViewRegistry::new();
    let view = registry.register("mv_cached_count");
    view.publish_logical_version(3);

    assert!(view.seed_cached_row_count_if_latest(3, 4));
    assert_eq!(view.authoritative_row_count_for(3), Some(4));
    assert_eq!(view.authoritative_row_count(), None);
    assert!(!view.seed_cached_row_count_if_latest(2, 1));
}

#[test]
fn authoritative_row_count_advances_for_empty_versions() {
    let registry = MaterializedViewRegistry::new();
    let view = registry.register("mv_advance_count");
    view.publish_logical_version(4);
    assert!(view.seed_authoritative_row_count_if_latest(4, 2));

    view.publish_logical_version(5);
    view.advance_authoritative_row_count_version(5);

    assert_eq!(view.authoritative_row_count_for(4), None);
    assert_eq!(view.authoritative_row_count_for(5), Some(2));
}

#[test]
fn authoritative_row_count_preserves_visible_version_while_next_version_is_staged() {
    let registry = MaterializedViewRegistry::new();
    let view = registry.register("mv_visible_count");
    view.mark_state_authoritative();
    view.publish_logical_version(1);

    let key = encoded_i64_row(1);
    view.apply_encoded_state_batch(1, &[(key.clone(), 1)])
        .expect("apply visible delta");
    assert_eq!(view.authoritative_row_count_for(1), Some(1));

    view.apply_encoded_state_batch(2, &[(key, 1)])
        .expect("apply staged delta");
    assert_eq!(view.authoritative_row_count(), Some(2));
    assert_eq!(view.authoritative_row_count_for(1), Some(1));
    assert_eq!(view.authoritative_row_count_for(2), None);

    view.publish_logical_version(2);
    assert_eq!(view.authoritative_row_count_for(2), Some(2));
}
