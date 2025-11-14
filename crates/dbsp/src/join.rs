use std::sync::Arc;

use anyhow::Result;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;

use async_trait::async_trait;

use crate::algebra::AbelianGroup;
use crate::handles::ZSetHandle;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::Stream;
use crate::stream::operations::{
    delta_lifted_delta_lifted_join, lifted_integrate_zset, lifted_stream_elimination,
    lifted_stream_introduction,
};

/// Convenience wrapper around the dbsp lifted join pipeline.
///
/// Accepts streams of `ZSetHandle`s (e.g., produced by `ZSetStream::handle_stream`) and
/// returns a derived stream of joined handles.
pub struct DbspJoin {
    stream: Stream<ZSetHandle>,
}

impl DbspJoin {
    pub async fn new<L, R, O, P, F>(
        left: &Stream<ZSetHandle>,
        right: &Stream<ZSetHandle>,
        predicate: P,
        projector: F,
    ) -> Result<Self>
    where
        L: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        R: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        O: Archive
            + Clone
            + Eq
            + std::hash::Hash
            + Send
            + Sync
            + 'static
            + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
        O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
        P: Fn(&L, &R) -> bool + Send + Sync + Clone + 'static,
        F: Fn(&L, &R) -> O + Send + Sync + Clone + 'static,
    {
        let left_intro = lifted_stream_introduction(left).await?;
        let right_intro = lifted_stream_introduction(right).await?;
        let nested = delta_lifted_delta_lifted_join::<L, R, O, _, _>(
            &left_intro,
            &right_intro,
            predicate,
            projector,
        )
        .await?;
        let inner_group: Arc<dyn AbelianGroup<ZSetHandle>> =
            Arc::new(ZSetHandleGroup::new(ZSetHandle {
                ns: String::new(),
                version: 0,
            }));
        let integrated = lifted_integrate_zset::<O>(&nested, inner_group.clone()).await?;
        let mut stream = lifted_stream_elimination(&integrated, inner_group).await?;
        stream.flush().await?;
        Ok(Self { stream })
    }

    pub fn stream(&self) -> Stream<ZSetHandle> {
        self.stream.clone()
    }
}

#[derive(Clone)]
struct ZSetHandleGroup {
    default: ZSetHandle,
}

impl ZSetHandleGroup {
    fn new(default: ZSetHandle) -> Self {
        Self { default }
    }
}

#[async_trait]
impl AbelianGroup<ZSetHandle> for ZSetHandleGroup {
    async fn add(&self, a: &ZSetHandle, _b: &ZSetHandle) -> ZSetHandle {
        a.clone()
    }

    async fn neg(&self, a: &ZSetHandle) -> ZSetHandle {
        a.clone()
    }

    async fn identity(&self) -> ZSetHandle {
        self.default.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::dictionary::Dictionary;
    use crate::storage::{KeyValueTable, SlateTable};
    use crate::{StreamRetention, ZSetStream};
    use object_store::memory::InMemory;
    use slatedb::Db;

    fn encode_row(values: &[i64]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(values.len() * 8);
        for value in values {
            buf.extend_from_slice(&value.to_le_bytes());
        }
        buf
    }

    fn decode_pair(bytes: &[u8]) -> (i64, i64) {
        let mut first = [0u8; 8];
        first.copy_from_slice(&bytes[0..8]);
        let mut second = [0u8; 8];
        second.copy_from_slice(&bytes[8..16]);
        (i64::from_le_bytes(first), i64::from_le_bytes(second))
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
    async fn joins_rows_via_dbsp_streams() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("join-db", store).await.expect("open db"));
        let table: Arc<dyn KeyValueTable> = Arc::new(SlateTable::new(db));

        let mut left = build_zset_stream(table.clone(), "join_left").await;
        let mut right = build_zset_stream(table.clone(), "join_right").await;

        left.add_delta(encode_row(&[1, 10]), 1);
        left.flush().await.expect("flush left step1");
        right.add_delta(encode_row(&[1, 20]), 1);
        right.flush().await.expect("flush right step1");

        left.add_delta(encode_row(&[2, 30]), 1);
        left.flush().await.expect("flush left step2");
        right.add_delta(encode_row(&[2, 40]), 1);
        right.flush().await.expect("flush right step2");

        let left_stream = left.handle_stream();
        let right_stream = right.handle_stream();
        let predicate = |l: &Vec<u8>, r: &Vec<u8>| decode_pair(l).0 == decode_pair(r).0;
        let projector = |l: &Vec<u8>, r: &Vec<u8>| {
            let mut combined = Vec::with_capacity(l.len() + r.len());
            combined.extend_from_slice(l);
            combined.extend_from_slice(r);
            combined
        };

        let join = DbspJoin::new::<Vec<u8>, Vec<u8>, Vec<u8>, _, _>(
            &left_stream,
            &right_stream,
            predicate,
            projector,
        )
        .await
        .expect("dbsp join");

        let mut joined_stream = join.stream();
        joined_stream.latest().await.expect("latest handle");
    }
}
