use std::collections::HashMap;

use anyhow::{Result, anyhow, bail};

pub const ROW_REFERENCE_V1: u8 = 1;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct RowReferenceV1 {
    pub segment_id: u64,
    pub row_offset: u32,
    pub generation: u32,
}

impl RowReferenceV1 {
    pub fn new(segment_id: u64, row_offset: u32, generation: u32) -> Self {
        Self {
            segment_id,
            row_offset,
            generation,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowReference {
    V1(RowReferenceV1),
    Future { version: u8, payload: Vec<u8> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForwardCompatPolicy {
    PreserveUnknown,
    RejectUnknown,
}

pub fn encode_row_reference_v1(reference: RowReferenceV1) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(1 + 8 + 4 + 4);
    bytes.push(ROW_REFERENCE_V1);
    bytes.extend_from_slice(&reference.segment_id.to_be_bytes());
    bytes.extend_from_slice(&reference.row_offset.to_be_bytes());
    bytes.extend_from_slice(&reference.generation.to_be_bytes());
    bytes
}

pub fn decode_row_reference(bytes: &[u8]) -> Result<RowReference> {
    decode_row_reference_with_policy(bytes, ForwardCompatPolicy::PreserveUnknown)
}

pub fn decode_row_reference_with_policy(
    bytes: &[u8],
    policy: ForwardCompatPolicy,
) -> Result<RowReference> {
    let (version, payload) = bytes
        .split_first()
        .ok_or_else(|| anyhow!("missing row-reference version byte"))?;

    match *version {
        ROW_REFERENCE_V1 => {
            if payload.len() < 16 {
                bail!("row-reference v1 payload must contain 16 bytes");
            }
            let segment_id =
                u64::from_be_bytes(payload[0..8].try_into().expect("segment bytes width"));
            let row_offset =
                u32::from_be_bytes(payload[8..12].try_into().expect("offset bytes width"));
            let generation =
                u32::from_be_bytes(payload[12..16].try_into().expect("generation bytes width"));
            Ok(RowReference::V1(RowReferenceV1 {
                segment_id,
                row_offset,
                generation,
            }))
        }
        unknown => match policy {
            ForwardCompatPolicy::PreserveUnknown => Ok(RowReference::Future {
                version: unknown,
                payload: payload.to_vec(),
            }),
            ForwardCompatPolicy::RejectUnknown => {
                bail!("unsupported row-reference version: {unknown}")
            }
        },
    }
}

pub fn apply_reference_deltas<I>(state: &mut HashMap<RowReferenceV1, i64>, deltas: I)
where
    I: IntoIterator<Item = (RowReferenceV1, i64)>,
{
    for (row_ref, delta) in deltas {
        if delta == 0 {
            continue;
        }
        let next = state.get(&row_ref).copied().unwrap_or(0) + delta;
        if next == 0 {
            state.remove(&row_ref);
        } else {
            state.insert(row_ref, next);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_unknown_versions_with_forward_compatibility() {
        let payload = vec![9_u8, 1, 2, 3, 4];
        let decoded = decode_row_reference(&payload).expect("decode unknown future row-reference");
        assert_eq!(
            decoded,
            RowReference::Future {
                version: 9,
                payload: vec![1, 2, 3, 4],
            }
        );
    }

    #[test]
    fn rejects_unknown_versions_when_policy_requires() {
        let payload = vec![9_u8, 1, 2, 3];
        let result = decode_row_reference_with_policy(&payload, ForwardCompatPolicy::RejectUnknown);
        assert!(result.is_err());
    }

    #[test]
    fn v1_round_trip_encodes_and_decodes() {
        let reference = RowReferenceV1::new(12, 99, 3);
        let encoded = encode_row_reference_v1(reference);
        let decoded = decode_row_reference(&encoded).expect("decode row-reference v1");
        assert_eq!(decoded, RowReference::V1(reference));
    }

    #[test]
    fn duplicate_workload_accumulates_weight() {
        let row_ref = RowReferenceV1::new(1, 7, 0);
        let mut state = HashMap::new();
        apply_reference_deltas(&mut state, vec![(row_ref, 1), (row_ref, 1), (row_ref, 2)]);
        assert_eq!(state.get(&row_ref), Some(&4));
    }

    #[test]
    fn retraction_workload_removes_zero_weight() {
        let row_ref = RowReferenceV1::new(2, 11, 0);
        let mut state = HashMap::new();
        apply_reference_deltas(&mut state, vec![(row_ref, 3), (row_ref, -3)]);
        assert!(!state.contains_key(&row_ref));
    }

    #[test]
    fn toggle_workload_tracks_latest_state() {
        let row_ref = RowReferenceV1::new(3, 19, 1);
        let mut state = HashMap::new();
        apply_reference_deltas(
            &mut state,
            vec![
                (row_ref, 1),
                (row_ref, -1),
                (row_ref, 1),
                (row_ref, -1),
                (row_ref, 1),
            ],
        );
        assert_eq!(state.get(&row_ref), Some(&1));
    }
}
