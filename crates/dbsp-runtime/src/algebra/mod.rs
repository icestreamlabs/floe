use std::sync::Arc;

use async_trait::async_trait;

/// Basic operations required for an Abelian group.
///
/// This mirrors the `AbelianGroupOperation` protocol from `pydbsp.core`.
#[async_trait]
pub trait AbelianGroup<T>: Send + Sync
where
    T: Send + Sync,
{
    async fn add(&self, a: &T, b: &T) -> T;
    async fn neg(&self, a: &T) -> T;
    async fn identity(&self) -> T;
}

#[async_trait]
impl<T, G> AbelianGroup<T> for Arc<G>
where
    T: Send + Sync,
    G: AbelianGroup<T> + ?Sized + Send + Sync,
{
    async fn add(&self, a: &T, b: &T) -> T {
        (**self).add(a, b).await
    }

    async fn neg(&self, a: &T) -> T {
        (**self).neg(a).await
    }

    async fn identity(&self) -> T {
        (**self).identity().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use object_store::memory::InMemory;
    use slatedb::Db;

    use super::AbelianGroup;
    use crate::stream::Stream;
    use crate::stream::StreamAddition;
    use crate::stream::util::collect_values;

    struct IntegerGroup;

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

    async fn build_db() -> Arc<Db> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        Arc::new(
            Db::open("algebra-tests", store)
                .await
                .expect("open SlateDB"),
        )
    }

    async fn assert_group_laws<T, G>(group: &G, a: &T, b: &T, c: &T)
    where
        T: Clone + PartialEq + std::fmt::Debug + Send + Sync,
        G: AbelianGroup<T> + ?Sized,
    {
        let ab = group.add(a, b).await;
        let bc = group.add(b, c).await;
        let ab_c = group.add(&ab, c).await;
        let a_bc = group.add(a, &bc).await;
        assert_eq!(ab_c, a_bc, "associativity failed");

        let ba = group.add(b, a).await;
        assert_eq!(ab, ba, "commutativity failed");

        let identity = group.identity().await;
        let a_id = group.add(a, &identity).await;
        assert_eq!(a_id, a.clone(), "identity failed");

        let neg_a = group.neg(a).await;
        let a_neg = group.add(a, &neg_a).await;
        assert_eq!(a_neg, identity, "inverse failed");
    }

    async fn build_stream(
        db: Arc<Db>,
        namespace: &str,
        group: Arc<dyn AbelianGroup<i64>>,
        values: &[i64],
        default_after: Option<i64>,
    ) -> Stream<i64> {
        let mut stream = Stream::new(db, namespace, group)
            .await
            .expect("create stream");
        for value in values {
            stream.send(*value).await.expect("send value");
        }
        if let Some(default) = default_after {
            stream.set_default(default).await.expect("set default");
        }
        stream.flush().await.expect("flush stream");
        stream
    }

    async fn assert_stream_eq(left: &Stream<i64>, right: &Stream<i64>) {
        let max_ts = left
            .semantic_horizon()
            .max(right.semantic_horizon())
            .saturating_add(1);
        let left_values = collect_values(left, max_ts)
            .await
            .expect("collect left stream values");
        let right_values = collect_values(right, max_ts)
            .await
            .expect("collect right stream values");
        assert_eq!(left_values, right_values);
    }

    #[tokio::test]
    async fn integer_group_obeys_abelian_laws() {
        let group = IntegerGroup;
        assert_group_laws(&group, &3, &-2, &7).await;
    }

    #[tokio::test]
    async fn stream_addition_obeys_abelian_laws() {
        let db = build_db().await;
        let value_group: Arc<dyn AbelianGroup<i64>> = Arc::new(IntegerGroup);

        let stream_a =
            build_stream(db.clone(), "group_a", value_group.clone(), &[2], Some(5)).await;
        let stream_b =
            build_stream(db.clone(), "group_b", value_group.clone(), &[1, 3], None).await;
        let stream_c = build_stream(db.clone(), "group_c", value_group.clone(), &[-2], None).await;

        let group = StreamAddition::from_stream(&stream_a);

        let ab = group.add(&stream_a, &stream_b).await;
        let ba = group.add(&stream_b, &stream_a).await;
        assert_stream_eq(&ab, &ba).await;

        let bc = group.add(&stream_b, &stream_c).await;
        let ab_c = group.add(&ab, &stream_c).await;
        let a_bc = group.add(&stream_a, &bc).await;
        assert_stream_eq(&ab_c, &a_bc).await;

        let identity = group.identity().await;
        let a_id = group.add(&stream_a, &identity).await;
        assert_stream_eq(&stream_a, &a_id).await;

        let neg_a = group.neg(&stream_a).await;
        let a_neg = group.add(&stream_a, &neg_a).await;
        let identity = group.identity().await;
        assert_stream_eq(&a_neg, &identity).await;
    }
}
