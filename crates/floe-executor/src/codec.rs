use std::sync::Arc;

use anyhow::{Context, Result, bail};
use dbsp::storage::KeyValueTable;

const OUTER_STREAM_CODEC_PREFIX: &str = "codec/outer/";
pub const OUTER_STREAM_CODEC_VERSION: u32 = 1;

pub async fn ensure_outer_stream_codec(
    table: Arc<dyn KeyValueTable>,
    namespace: &str,
) -> Result<()> {
    let key = codec_key(namespace);
    if let Some(bytes) = table.get(&key).await? {
        let stored = decode_version(&bytes).context("decode stored outer stream codec version")?;
        if stored != OUTER_STREAM_CODEC_VERSION {
            bail!(
                "outer stream codec mismatch for {namespace}: stored v{stored}, expected v{}",
                OUTER_STREAM_CODEC_VERSION
            );
        }
        return Ok(());
    }
    table
        .put(&key, &OUTER_STREAM_CODEC_VERSION.to_le_bytes())
        .await
        .context("persist outer stream codec version")
}

fn decode_version(bytes: &[u8]) -> Result<u32> {
    if bytes.len() != 4 {
        bail!(
            "outer stream codec version must be 4 bytes, found {}",
            bytes.len()
        );
    }
    let mut buf = [0u8; 4];
    buf.copy_from_slice(bytes);
    Ok(u32::from_le_bytes(buf))
}

fn codec_key(namespace: &str) -> Vec<u8> {
    format!("{OUTER_STREAM_CODEC_PREFIX}{namespace}").into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbsp::storage::SlateTable;
    use object_store::memory::InMemory;
    use slatedb::Db;
    use std::sync::Arc;

    #[tokio::test]
    async fn writes_and_verifies_codec_version() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("codec-table", store).await.expect("open db"));
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
        ensure_outer_stream_codec(table.clone(), "src/bid")
            .await
            .expect("write codec");
        ensure_outer_stream_codec(table.clone(), "src/bid")
            .await
            .expect("verify codec");
    }
}
