use anyhow::Result;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use crate::handles::ZSetHandle;
use crate::stream::Stream;
use crate::stream::operations::lifted_select_zset_stream;

/// Convenience wrapper over the dbsp lifted filter pipeline.
///
/// Consumes a stream of [`ZSetHandle`]s and emits a derived stream where each
/// handle only contains keys that satisfy the provided predicate.
pub struct DbspFilter {
    stream: Stream<ZSetHandle>,
}

impl DbspFilter {
    pub async fn new<K, P>(input: &Stream<ZSetHandle>, predicate: P) -> Result<Self>
    where
        K: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<crate::storage::encoding::RkyvSerializer<'a>>,
        K::Archived: RkyvDeserialize<K, crate::storage::encoding::RkyvDeserializer>
            + for<'a> CheckBytes<crate::storage::encoding::RkyvValidator<'a>>,
        P: Fn(&K) -> bool + Send + Sync + Clone + 'static,
    {
        let stream = lifted_select_zset_stream::<K, _>(input, predicate).await?;
        Ok(Self { stream })
    }

    pub fn stream(&self) -> Stream<ZSetHandle> {
        self.stream.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::dictionary::Dictionary;
    use crate::storage::{KeyValueTable, SlateTable};
    use crate::{StreamRetention, ZSetStream};
    use object_store::{ObjectStore, memory::InMemory};
    use slatedb::Db;
    use std::sync::Arc;

    fn encode_row(values: &[i64]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(values.len() * 8);
        for value in values {
            buf.extend_from_slice(&value.to_le_bytes());
        }
        buf
    }

    async fn build_zset_stream(
        table: Arc<dyn KeyValueTable>,
        namespace: &str,
    ) -> ZSetStream<Vec<u8>> {
        let dict = Arc::new(
            Dictionary::with_table(table.clone(), namespace.to_string(), None)
                .await
                .expect("dictionary"),
        );
        ZSetStream::new(
            dict,
            table,
            namespace.to_string(),
            StreamRetention::KeepLast { keep_last: 1 },
        )
        .await
        .expect("zset stream")
    }

    #[tokio::test]
    async fn filters_rows_via_dbsp_streams() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("filter-db", store).await.expect("open db"));
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));
        let mut stream = build_zset_stream(table.clone(), "filter_input").await;
        stream.add_delta(encode_row(&[1, 10]), 1);
        stream.add_delta(encode_row(&[2, 20]), 1);
        stream.flush().await.expect("flush step 1");
        stream.add_delta(encode_row(&[1, 30]), 1);
        stream.flush().await.expect("flush step 2");

        let input_stream = stream.handle_stream();
        let predicate = |row: &Vec<u8>| row[0] == 1;

        let filter = DbspFilter::new::<Vec<u8>, _>(&input_stream, predicate)
            .await
            .expect("dbsp filter");
        let mut derived = filter.stream();
        derived.latest().await.expect("latest handle");
    }
}
