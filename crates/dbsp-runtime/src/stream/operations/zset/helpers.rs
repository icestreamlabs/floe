use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use rkyv::Archive;
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::Serialize as RkyvSerialize;
use rkyv::bytecheck::CheckBytes;
use tokio::time::sleep;

use crate::handles::ZSetHandle;
use crate::storage::KeyValueTable;
use crate::storage::dictionary::Dictionary;
use crate::storage::encoding::{RkyvDeserializer, RkyvSerializer, RkyvValidator};
use crate::stream::Stream;
use crate::stream::util::{materialize_zset_handle, push_value_in_place, set_default_in_place};

pub(super) async fn publish_handle(
    stream: &mut Stream<ZSetHandle>,
    handle: ZSetHandle,
    initialized: &mut bool,
) -> Result<()> {
    if !*initialized {
        set_default_in_place(stream, handle.clone());
        *initialized = true;
    } else {
        push_value_in_place(stream, handle.clone());
    }
    set_default_in_place(stream, handle.clone());
    stream.flush().await?;
    Ok(())
}

pub(super) async fn materialize_zset_with_retry<K>(
    table: Arc<dyn KeyValueTable>,
    cache: &mut HashMap<String, Arc<Dictionary<K>>>,
    handle: &ZSetHandle,
) -> Result<HashMap<K, i64>>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    let mut last_err = None;
    for _ in 0..80 {
        match materialize_zset_handle::<K>(table.clone(), cache, handle).await {
            Ok(map) => return Ok(map),
            Err(err) => {
                last_err = Some(err);
                sleep(Duration::from_millis(25)).await;
            }
        }
    }
    Err(last_err.expect("at least one materialize attempt"))
}
