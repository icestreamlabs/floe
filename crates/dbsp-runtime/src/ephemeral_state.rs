use std::sync::Arc;

use anyhow::Context;
use slatedb::Db;
use slatedb::object_store::ObjectStore;
use slatedb::object_store::memory::InMemory;

use crate::storage::{KeyValueTable, SlateTable};

pub(crate) async fn build_ephemeral_state_table(
    namespace: &str,
) -> anyhow::Result<Arc<dyn KeyValueTable>> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::open(namespace, store)
            .await
            .with_context(|| format!("open in-memory SlateDB for operator state '{namespace}'"))?,
    );
    Ok(Arc::new(SlateTable::new(db)))
}
