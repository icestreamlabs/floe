use anyhow::{Result, anyhow};
use rkyv::api::high::{HighDeserializer, HighSerializer, HighValidator, to_bytes};
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::Error as RkyvError;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

pub type RkyvSerializer<'a> = HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>;
pub type RkyvDeserializer = HighDeserializer<RkyvError>;
pub type RkyvValidator<'a> = HighValidator<'a, RkyvError>;

pub const CODEC_TAG_RKYV: u8 = 0x00;

pub fn encode<T>(value: &T) -> Result<Vec<u8>>
where
    T: Clone + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
{
    let aligned = to_bytes::<RkyvError>(&value.clone())
        .map_err(|err| anyhow!("failed to encode value with rkyv: {err}"))?;
    let mut encoded = Vec::with_capacity(1 + aligned.len());
    encoded.push(CODEC_TAG_RKYV);
    encoded.extend_from_slice(&aligned);
    Ok(encoded)
}

pub fn decode<T>(bytes: &[u8]) -> Result<T>
where
    T: Archive,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let (tag, payload) = bytes
        .split_first()
        .ok_or_else(|| anyhow!("missing codec tag in encoded payload"))?;

    match tag {
        &CODEC_TAG_RKYV => {
            let mut aligned = AlignedVec::<16>::with_capacity(payload.len());
            aligned.extend_from_slice(payload);
            rkyv::from_bytes::<T, RkyvError>(&aligned)
                .map_err(|err| anyhow!("failed to decode value with rkyv: {err}"))
        }
        other => Err(anyhow!("unsupported codec tag: {other:#04x}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rkyv::{Archive, Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
    struct Composite {
        id: i64,
        flag: bool,
        label: String,
    }

    #[test]
    fn round_trip_timestamp() {
        let ts: i64 = 1_723_456_789;
        let encoded = encode(&ts).expect("encode timestamp");
        assert_eq!(encoded[0], CODEC_TAG_RKYV);
        let decoded: i64 = decode(&encoded).expect("decode timestamp");
        assert_eq!(decoded, ts);
    }

    #[test]
    fn round_trip_weight() {
        let weight: i64 = -42;
        let encoded = encode(&weight).expect("encode weight");
        let decoded: i64 = decode(&encoded).expect("decode weight");
        assert_eq!(decoded, weight);
    }

    #[test]
    fn round_trip_struct() {
        let composite = Composite {
            id: 7,
            flag: true,
            label: "dbsp".to_string(),
        };

        let encoded = encode(&composite).expect("encode struct");
        let decoded: Composite = decode(&encoded).expect("decode struct");
        assert_eq!(decoded, composite);
    }

    #[test]
    fn fails_on_unknown_tag() {
        let bytes = [0xFFu8, 0xAA, 0xBB];
        let result: Result<i64> = decode(&bytes);
        assert!(result.is_err());
    }
}
