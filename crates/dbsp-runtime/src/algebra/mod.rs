use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

/// Basic operations required for an Abelian group.
///
/// This mirrors the `AbelianGroupOperation` protocol from `pydbsp.core`.
#[async_trait]
pub trait AbelianGroup<T>: Send + Sync
where
    T: Send + Sync,
{
    async fn add(&self, a: &T, b: &T) -> Result<T>;
    async fn neg(&self, a: &T) -> Result<T>;
    async fn identity(&self) -> Result<T>;
}

#[async_trait]
impl<T, G> AbelianGroup<T> for Arc<G>
where
    T: Send + Sync,
    G: AbelianGroup<T> + ?Sized + Send + Sync,
{
    async fn add(&self, a: &T, b: &T) -> Result<T> {
        (**self).add(a, b).await
    }

    async fn neg(&self, a: &T) -> Result<T> {
        (**self).neg(a).await
    }

    async fn identity(&self) -> Result<T> {
        (**self).identity().await
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use async_trait::async_trait;

    use super::AbelianGroup;

    struct IntegerGroup;

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

    async fn assert_group_laws<T, G>(group: &G, a: &T, b: &T, c: &T) -> Result<()>
    where
        T: Clone + PartialEq + std::fmt::Debug + Send + Sync,
        G: AbelianGroup<T> + ?Sized,
    {
        let ab = group.add(a, b).await?;
        let bc = group.add(b, c).await?;
        let ab_c = group.add(&ab, c).await?;
        let a_bc = group.add(a, &bc).await?;
        assert_eq!(ab_c, a_bc, "associativity failed");

        let ba = group.add(b, a).await?;
        assert_eq!(ab, ba, "commutativity failed");

        let identity = group.identity().await?;
        let a_id = group.add(a, &identity).await?;
        assert_eq!(a_id, a.clone(), "identity failed");

        let neg_a = group.neg(a).await?;
        let a_neg = group.add(a, &neg_a).await?;
        assert_eq!(a_neg, identity, "inverse failed");
        Ok(())
    }

    #[tokio::test]
    async fn integer_group_obeys_abelian_laws() -> Result<()> {
        let group = IntegerGroup;
        assert_group_laws(&group, &3, &-2, &7).await
    }
}
