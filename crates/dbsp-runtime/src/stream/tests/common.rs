use std::sync::Arc;

use async_trait::async_trait;
use object_store::memory::InMemory;
use slatedb::Db;

use crate::algebra::AbelianGroup;

pub struct IntegerGroup;

#[async_trait]
impl AbelianGroup<i64> for IntegerGroup {
    async fn add(&self, a: &i64, b: &i64) -> i64 {
        a + b
    }

    async fn neg(&self, a: &i64) -> i64 {
        -a
    }

    async fn identity(&self) -> i64 {
        0
    }
}

pub async fn build_db() -> Arc<Db> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    Arc::new(Db::open("stream-test", store).await.expect("open SlateDB"))
}
