use super::shared::{build_join_state_projection, remap_join_output_projection};
use super::*;
use crate::encoding::{EncodedRowProjectionColumn, EncodedRowProjectionSource};
use dbsp::circuit::schema::Field;
use dbsp::circuit::types::DbspScalarType;
use std::collections::{BTreeSet, HashMap};

fn test_schema() -> Arc<RowSchema> {
    RowSchema::try_new(vec![
        Field::new("auction", DbspScalarType::Int64, false),
        Field::new("bidder", DbspScalarType::Int64, false),
        Field::new("price", DbspScalarType::Int64, false),
        Field::new("extra", DbspScalarType::Utf8, true),
    ])
    .expect("schema")
}

#[test]
fn join_state_projection_drops_unneeded_columns_and_remaps_outputs() {
    let schema = test_schema();
    let required = BTreeSet::from([0usize, 2usize]);

    let (projection, remap) =
        build_join_state_projection(schema.as_ref(), &required).expect("projection");
    let projection = projection.expect("non-identity projection");

    assert_eq!(projection.output_schema().len(), 2);
    assert_eq!(projection.output_schema().field(0).unwrap().name, "auction");
    assert_eq!(projection.output_schema().field(1).unwrap().name, "price");
    assert_eq!(remap.get(&0), Some(&0));
    assert_eq!(remap.get(&2), Some(&1));
    assert!(!remap.contains_key(&1));
    assert!(!remap.contains_key(&3));

    let columns = vec![EncodedRowProjectionColumn {
        source: EncodedRowProjectionSource::Left,
        index: 2,
    }];
    let right_remap = HashMap::new();
    let remapped =
        remap_join_output_projection(&columns, &remap, &right_remap).expect("remap output");
    assert_eq!(remapped[0].index, 1);
}

#[test]
fn join_state_projection_elides_identity_projection() {
    let schema = test_schema();
    let required = BTreeSet::from([0usize, 1usize, 2usize, 3usize]);

    let (projection, remap) =
        build_join_state_projection(schema.as_ref(), &required).expect("projection");

    assert!(projection.is_none());
    assert_eq!(remap.get(&0), Some(&0));
    assert_eq!(remap.get(&3), Some(&3));
}
