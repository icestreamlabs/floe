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
