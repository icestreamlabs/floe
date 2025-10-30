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

pub fn encode<T>(value: &T) -> Result<Vec<u8>>
where
    T: Clone + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
{
    let aligned = to_bytes::<RkyvError>(&value.clone())
        .map_err(|err| anyhow!("failed to encode value with rkyv: {err}"))?;
    Ok(aligned.into_vec())
}

pub fn decode<T>(bytes: &[u8]) -> Result<T>
where
    T: Archive,
    T::Archived: RkyvDeserialize<T, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    rkyv::from_bytes::<T, RkyvError>(bytes)
        .map_err(|err| anyhow!("failed to decode value with rkyv: {err}"))
}
