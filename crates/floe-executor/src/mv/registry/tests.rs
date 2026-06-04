use dbsp::handles::ZSetHandle;

use super::*;

#[test]
fn registers_and_updates_view_state() {
    let registry = MaterializedViewRegistry::new();
    let view = registry.register("mv_test");
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
    assert!(!view.is_version_published(1));
    assert!(view.is_version_published(2));
    assert!(view.is_version_published(3));
    assert_eq!(view.next_version_after(0), Some(2));
    assert_eq!(view.version_time(1), None);
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
fn seeds_authoritative_row_count_only_for_latest_version() {
    let registry = MaterializedViewRegistry::new();
    let view = registry.register("mv_seed_count");
    view.publish_logical_version(7);

    assert!(view.seed_authoritative_row_count_if_latest(7, 3));
    assert_eq!(view.authoritative_row_count(), Some(3));
    assert_eq!(view.authoritative_row_count_for(7), Some(3));

    let stale_view = registry.register("mv_seed_count_stale");
    stale_view.publish_logical_version(7);
    assert!(!stale_view.seed_authoritative_row_count_if_latest(6, 2));
    assert_eq!(stale_view.authoritative_row_count(), None);
}
