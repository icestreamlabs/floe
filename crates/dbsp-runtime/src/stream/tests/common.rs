use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use object_store::memory::InMemory;
use slatedb::Db;

use crate::algebra::AbelianGroup;

pub struct IntegerGroup;

#[async_trait]
impl AbelianGroup<i64> for IntegerGroup {
    async fn add(&self, a: &i64, b: &i64) -> Result<i64> {
        Ok(a + b)
    }

    async fn neg(&self, a: &i64) -> Result<i64> {
        Ok(-a)
    }

    async fn identity(&self) -> Result<i64> {
        Ok(0)
    }
}

pub async fn build_db() -> Arc<Db> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    Arc::new(Db::open("stream-test", store).await.expect("open SlateDB"))
}
