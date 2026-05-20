use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::path::Path as ObjectPath;
use object_store::{Error as ObjectStoreError, ObjectStore};

pub(crate) fn hex_component(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0F) as usize] as char);
    }
    out
}

pub(crate) fn push_length_prefixed_component(out: &mut Vec<u8>, component: &[u8]) {
    let len = u32::try_from(component.len()).expect("storage key component length exceeds u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(component);
}

pub(crate) async fn put_payload_object(
    object_store: &Arc<dyn ObjectStore>,
    object_key: &str,
    payload: Vec<u8>,
    label: &str,
) -> Result<()> {
    object_store
        .put(&ObjectPath::from(object_key.to_string()), payload.into())
        .await
        .with_context(|| format!("write {label} payload object '{object_key}'"))?;
    Ok(())
}

pub(crate) async fn load_payload_object(
    object_store: &Arc<dyn ObjectStore>,
    object_key: &str,
    label: &str,
) -> Result<Vec<u8>> {
    let payload = object_store
        .get(&ObjectPath::from(object_key.to_string()))
        .await
        .with_context(|| format!("load {label} payload object '{object_key}'"))?
        .bytes()
        .await
        .with_context(|| format!("read {label} payload object '{object_key}'"))?;
    Ok(payload.to_vec())
}

pub(crate) async fn delete_payload_object_if_exists(
    object_store: &Arc<dyn ObjectStore>,
    object_key: &str,
    label: &str,
) -> Result<()> {
    match object_store
        .delete(&ObjectPath::from(object_key.to_string()))
        .await
    {
        Ok(()) => Ok(()),
        Err(ObjectStoreError::NotFound { .. }) => Ok(()),
        Err(err) => {
            Err(err).with_context(|| format!("delete {label} payload object '{object_key}'"))
        }
    }
}
